"""Execute the real backup/preflight body; no network or production access."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy'))
import build_tushare_direct_prepare as tool


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.runtime = self.base / 'runtime'
        generation_id = '20aa2215a7904e0d8d6601b14b8a6c90'
        self.generation = self.runtime / 'tushare-history-generations' / generation_id
        self.generation.mkdir(parents=True)
        for relative in ['tushare-history-batch-v1/admission.lock', 'tushare-egress/STOP']:
            p = self.runtime / relative
            p.parent.mkdir(exist_ok=True)
            p.touch()
        for name in ['gate.lock', 'STOP']:
            (self.generation / name).touch()
        policy = dict(schema=2, write_enabled=True, codes=[str(n) for n in range(5577)],
                      min_date='2026-09-30', max_date='2026-11-06', expires_at=tool.EXPIRY,
                      minute_requests=40, daily_requests=7500, max_requests=40000)
        self.write(self.generation / 'policy.json', policy)
        self.write(self.generation / 'state.json', dict(phase='blocked', reason='network_failed', used=6425,
                   policy_sha256=sha(policy), plan=dict(plan_sha256='d97ddaa79fea5efd6d1fd481c7623d3124a38e5c50fda3f27a0c47a4815e4588'),
                   active=dict(code='002309.SZ', start='2026-10-29', end='2026-11-02',
                               id='289f124728af45e8a27475e5bdd908e9', sent=['daily'])))
        self.write(self.generation / 'retirement.json', {'fixture': True})
        self.write(self.generation.parent / 'active.json', dict(generation=generation_id, policy_sha256=sha(policy),
                   retirement_sha256=hashlib.sha256((self.generation / 'retirement.json').read_bytes()).hexdigest()))
        self.write(self.runtime / 'tushare-history-schedule.json', dict(
                   generation='/app/runtime/tushare-history-generations/' + generation_id, policy_sha256=sha(policy),
                   expires_at=tool.EXPIRY, cron='30 18 * * mon-fri', timezone='Asia/Shanghai'))
        self.unitdir = self.base / 'systemd'
        self.unitdir.mkdir()
        (self.unitdir / tool.UNIT).write_text('[Service]\nExecStart=/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 127.0.0.1:32879\n')
        (self.unitdir / tool.SOCKET).write_text('[Socket]\nListenStream=/run/ta-tushare-egress/upstream.sock\n')
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(self.sock.close)
        self.sockpath = self.base / 'upstream.sock'
        self.sock.bind(str(self.sockpath))
        self.r = tool.request()
        self.r.update(root=str(self.base / 'prepared'), unit_directory=str(self.unitdir),
                      socket_path=str(self.sockpath), dropin=str(self.unitdir / (tool.UNIT + '.d') / '90-direct-route-20261103.conf'))
        self.backend = dict(Id=tool.BACKEND_ID, Image=tool.IMAGE, State=dict(Running=True, Health=dict(Status='healthy')),
                            Mounts=[dict(Source=str(self.runtime), Destination='/app/runtime')] + [{}] * 43)
        self.commands = []

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value))

    def external(self, args, **kwargs):
        self.commands.append(args)
        if args[0] == 'docker':
            output = json.dumps([self.backend])
        elif args[0] == 'systemctl':
            self.assertEqual(args[1], 'show')
            unit = args[2]
            p = dict(FragmentPath=str(self.unitdir / unit), DropInPaths='', ActiveState='active', ExecCondition='')
            if unit == tool.UNIT:
                p.update(User='ta-tushare-proxy', Group='ta-tushare-proxy', PrivateNetwork='no',
                         ExecStart='{ argv[]=/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 127.0.0.1:32879 ; pid=1 }')
            else:
                p['Listen'] = str(self.sockpath) + ' (Stream)'
            output = '\n'.join(k + '=' + v for k, v in p.items())
        elif args[0] == 'systemd-analyze':
            self.assertEqual(args[1], 'verify')
            self.assertTrue(all(Path(p).is_file() for p in args[2:]))
            output = ''
        elif args[0] == 'runuser':
            self.assertEqual(args[1:4], ['-u', 'ta-tushare-proxy', '--'])
            output = json.dumps([dict(verified=True, tls='TLSv1.2', elapsed_s=1)] * 3)
        else:
            self.fail('unexpected command: ' + str(args))
        return subprocess.CompletedProcess(args, 0, stdout=output, stderr='')

    def execute(self):
        stdout = io.StringIO()
        with patch('subprocess.run', self.external), patch('os.geteuid', return_value=0), contextlib.redirect_stdout(stdout):
            exec(compile(tool.PREPARE, 'prepare', 'exec'), dict(request=self.r))
        return json.loads(stdout.getvalue().splitlines()[-1])

    def snapshot(self):
        return {str(p): (p.read_bytes(), p.stat().st_mode, p.stat().st_ino)
                for directory in [self.runtime, self.unitdir] for p in directory.rglob('*') if p.is_file()}

    def test_prepares_verified_backups_without_route_or_control_mutation(self):
        before = self.snapshot()
        inode = self.sockpath.stat().st_ino
        result = self.execute()
        self.assertTrue(result['prepared'])
        self.assertEqual(result['manifest.json']['http_requests'], 0)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.sockpath.stat().st_ino, inode)
        root = Path(self.r['root'])
        manifest = result['manifest.json']
        for name, source in manifest['backup_files'].items():
            self.assertEqual((root / 'backup' / name).read_bytes(), Path(source).read_bytes())
        for destination, candidate in manifest['candidates'].items():
            self.assertFalse(Path(destination).exists())
            self.assertEqual(hashlib.sha256((root / 'candidate' / candidate['file']).read_bytes()).hexdigest(), candidate['sha256'])
        self.assertEqual(root.stat().st_mode & 0o777, 0o700)
        self.assertTrue(all(p.stat().st_mode & 0o777 == 0o600 for p in root.rglob('*') if p.is_file()))

    def test_wrong_backend_cannot_create_preparation_directory(self):
        self.backend['Id'] = 'wrong'
        with self.assertRaisesRegex(RuntimeError, 'wrong_host'):
            self.execute()
        self.assertFalse(Path(self.r['root']).exists())

    def test_changed_attempt_stops_before_any_backup_or_probe(self):
        path = self.generation / 'state.json'
        state = json.loads(path.read_text())
        state['used'] += 1
        self.write(path, state)
        before = self.snapshot()
        with self.assertRaises(SystemExit):
            self.execute()
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(Path(self.r['root']).exists())
        self.assertEqual([c[0] for c in self.commands], ['docker'])

    def test_tls_failure_preserves_route_and_records_failure(self):
        original = self.external
        def fail_tls(args, **kwargs):
            if args[0] == 'runuser':
                return subprocess.CompletedProcess(args, 1, stdout='', stderr='TLS failed')
            return original(args, **kwargs)
        before = self.snapshot()
        with patch.object(self, 'external', fail_tls), self.assertRaises(SystemExit):
            self.execute()
        self.assertEqual(self.snapshot(), before)
        status = json.loads((Path(self.r['root']) / 'status.json').read_text())
        self.assertEqual(status['stage'], 'failed_no_route_change')


if __name__ == '__main__':
    unittest.main()
