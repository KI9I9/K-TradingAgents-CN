"""Real file installation/rollback and lock behavior; system/network tools replaced."""
import json
import contextlib
import io
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy'))
import test_tushare_direct_prepare as preparation
import tushare_direct_switch as core


class ClientProbeTests(unittest.TestCase):
    def test_tls_body_connects_and_closes_without_http_or_package_import(self):
        source = b'bound transport source'
        connections = []
        class Connection:
            def __init__(self, timeout):
                self.sock = self
                self.connected = self.closed = False
                connections.append(self)
            def connect(self):
                self.connected = True
            def version(self):
                return 'TLSv1.2'
            def close(self):
                self.closed = True
        output = io.StringIO()
        with patch.object(sys, 'argv', ['-c', core.sha(source)]), patch.object(Path, 'read_bytes', return_value=source), \
             patch('runpy.run_path', return_value={'UnixTLSConnection': Connection}), contextlib.redirect_stdout(output):
            exec(compile(core.TLS_CHECK, 'tls_probe', 'exec'), {})
        result = json.loads(output.getvalue().split(':', 1)[1])
        self.assertEqual(result['http_requests'], 0)
        self.assertEqual(len(connections), 3)
        self.assertTrue(all(c.connected and c.closed for c in connections))

    def test_tls_body_rejects_transport_drift_before_loading_source(self):
        with patch.object(sys, 'argv', ['-c', 'wrong-hash']), patch.object(Path, 'read_bytes', return_value=b'changed'), \
             patch('runpy.run_path') as load:
            with self.assertRaisesRegex(SystemExit, 'transport_source_changed'):
                exec(compile(core.TLS_CHECK, 'tls_probe', 'exec'), {})
        load.assert_not_called()


class SwitchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = preparation.PreparationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        result = self.fixture.execute()
        self.m = result['manifest.json']
        self.root = Path(self.fixture.r['root'])
        self.work = self.root / 'switch-v1'
        self.work.mkdir(mode=0o700)
        (self.work / 'operator.lock').touch(mode=0o600)
        (self.work / 'status.json').write_text(json.dumps(dict(stage='reserved')))
        approved = self.fixture.base / 'APPROVED'
        approved.touch()
        candidates = {path: (self.root / 'candidate' / info['file']).read_text()
                      for path, info in self.m['candidates'].items()}
        self.r = dict(root=str(self.root), manifest_sha256=core.sha((self.root / 'manifest.json').read_bytes()),
                      expiry=self.m['expiry'], unit=self.fixture.r['unit'], socket=self.fixture.r['socket'],
                      socket_path=str(self.fixture.sockpath), unit_directory=str(self.fixture.unitdir),
                      expiry_unit=self.fixture.r['expiry_unit'], dropin=self.fixture.r['dropin'],
                      candidate_sources=candidates, approved_path=str(approved),
                      worker_unit='test-direct-worker.service', guard_unit='test-direct-rollback',
                      transport_sha256='expected-transport-source',
                      old_command='/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 127.0.0.1:32879',
                      new_command='/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 api.waditu.com:443')
        self.subject = core.Switch(self.r)
        self.commands = []
        self.direct = False
        self.fail_tls = False
        self.bad_timer = False
        self.foreign_on_tls = False
        self.restarts = []
        self.worker_collected = False

    def external(self, args, **kwargs):
        self.commands.append(args)
        if args[0] == 'systemctl':
            if args[1] == 'show':
                unit = args[2]
                if unit in (self.subject.timer, self.r['guard_unit'] + '.timer'):
                    output = 'ActiveState=active\nSubState=waiting'
                elif unit == self.r['worker_unit']:
                    output = ('LoadState=not-found\nActiveState=inactive' if self.worker_collected
                              else 'LoadState=loaded\nActiveState=active')
                elif unit == self.r['socket']:
                    output = '\n'.join(k + '=' + v for k, v in self.m['socket'].items())
                elif unit == self.r['unit']:
                    p = dict(self.m['service'])
                    if self.direct:
                        p.update(ExecStart='{ argv[]=' + self.r['new_command'] + ' ; pid=2 }',
                                 ExecCondition='/usr/bin/python3 time.time() < ' + str(self.r['expiry']),
                                 DropInPaths=self.r['dropin'])
                    output = '\n'.join(k + '=' + v for k, v in p.items())
                else:
                    self.fail('unexpected unit: ' + unit)
            else:
                self.assertIn(args[1], ('start', 'stop', 'restart', 'daemon-reload'))
                if args[1] == 'restart':
                    self.assertEqual(args[2], self.r['unit'])
                    self.direct = Path(self.r['dropin']).exists()
                    self.restarts.append(self.direct)
                output = ''
        elif args[0] == 'systemd-run':
            self.assertIn('--on-active=180s', args)
            self.assertEqual(args[-1], '--rollback')
            output = ''
        elif args[0] == 'busctl':
            self.assertEqual(args[-1], 'NextElapseUSecRealtime')
            output = 't ' + str((self.r['expiry'] + (1 if self.bad_timer else 0)) * 1000000)
        elif args[:2] == ['docker', 'inspect']:
            output = json.dumps([self.fixture.backend])
        elif args[:2] == ['docker', 'exec']:
            self.assertEqual(args[2:6], [self.m['backend_id'], 'python3', '-B', '-c'])
            self.assertEqual(args[6], core.TLS_CHECK)
            self.assertEqual(args[7], self.r['transport_sha256'])
            if self.foreign_on_tls:
                Path(self.r['dropin']).write_text('foreign content\n')
            if self.fail_tls:
                return subprocess.CompletedProcess(args, 1, stdout='', stderr='TLS failure')
            output = 'non-receipt log line\nDIRECT_ROUTE_TLS_JSON:' + json.dumps(dict(http_requests=0, observations=[dict(verified=True, tls='TLSv1.2')] * 3))
        else:
            self.fail('unexpected external operation: ' + str(args))
        return subprocess.CompletedProcess(args, 0, stdout=output, stderr='')

    def run_switch(self):
        with patch('subprocess.run', self.external):
            self.subject.execute()

    def status(self):
        return json.loads((self.work / 'status.json').read_text())

    def test_success_preserves_control_files_backend_and_socket(self):
        self.run_switch()
        self.assertEqual(self.status()['stage'], 'passed')
        committed = json.loads((self.work / 'committed.json').read_text())
        self.assertEqual(committed['http_requests'], 0)
        self.assertTrue(committed['history_still_stopped'])
        self.assertTrue(self.subject.unchanged())
        self.assertTrue(self.subject.socket_unchanged())
        self.assertEqual(self.restarts, [True])
        self.assertTrue(self.subject.link.is_symlink())
        self.assertFalse(any(cmd[:2] == ['systemctl', 'restart'] and cmd[2] != self.r['unit'] for cmd in self.commands))

    def test_failed_backend_tls_restores_old_route_without_data_restore(self):
        self.fail_tls = True
        with self.assertRaisesRegex(RuntimeError, 'command_failed:docker'):
            self.run_switch()
        self.assertEqual(self.status()['stage'], 'rolled_back')
        self.assertEqual(self.restarts, [True, False])
        self.assertTrue(self.subject.unchanged())
        self.assertTrue(self.subject.socket_unchanged())
        self.assertFalse((self.work / 'committed.json').exists())
        self.assertFalse(self.subject.link.exists())
        self.assertTrue(all(not Path(p).exists() for p in self.m['candidates']))

    def test_wrong_timer_deadline_prevents_direct_restart(self):
        self.bad_timer = True
        with self.assertRaisesRegex(RuntimeError, 'expiry_deadline_mismatch'):
            self.run_switch()
        self.assertEqual(self.status()['stage'], 'rolled_back')
        self.assertNotIn(True, self.restarts)

    def test_control_drift_prevents_installation_and_guard(self):
        control = next(Path(p) for p in self.m['protected'] if p.endswith('/state.json'))
        control.write_text(control.read_text() + '\n')
        with self.assertRaisesRegex(RuntimeError, 'original_or_control_changed'):
            self.run_switch()
        self.assertFalse(any(cmd[0] == 'systemd-run' for cmd in self.commands))
        self.assertFalse(any(Path(p).exists() for p in self.m['candidates']))

    def test_watchdog_recovers_interrupted_uncommitted_switch(self):
        with patch('subprocess.run', self.external):
            self.subject.preflight()
            self.subject.stage('installing_expiry_and_dropin')
            self.subject.install()
            self.external(['systemctl', 'restart', self.r['unit']])
            self.subject.watchdog()
        self.assertEqual(self.status()['stage'], 'rolled_back')
        self.assertEqual(self.restarts, [True, False])
        self.assertTrue(self.subject.unchanged())
        self.assertTrue(self.subject.socket_unchanged())

    def test_late_watchdog_cannot_undo_committed_route(self):
        self.run_switch()
        before = len(self.commands)
        with patch('subprocess.run', self.external):
            self.subject.watchdog()
        self.assertEqual(len(self.commands), before)
        self.assertEqual(self.status()['stage'], 'passed')

    def test_watchdog_recovers_after_worker_is_already_collected(self):
        self.worker_collected = True
        with patch('subprocess.run', self.external):
            self.subject.preflight()
            self.subject.stage('installing_expiry_and_dropin')
            self.subject.install()
            self.external(['systemctl', 'restart', self.r['unit']])
            self.subject.watchdog()
        self.assertEqual(self.status()['stage'], 'rolled_back')
        self.assertEqual(self.restarts, [True, False])
        self.assertNotIn(['systemctl', 'stop', self.r['worker_unit']], self.commands)

    def test_foreign_file_is_retained_and_transport_closed(self):
        self.fail_tls = self.foreign_on_tls = True
        with self.assertRaisesRegex(RuntimeError, 'foreign_candidate_file'):
            self.run_switch()
        self.assertEqual(self.status()['stage'], 'rollback_incomplete_transport_closed')
        self.assertEqual(Path(self.r['dropin']).read_text(), 'foreign content\n')
        self.assertIn(['systemctl', 'stop', self.r['socket'], self.r['unit']], self.commands)
        self.assertTrue(self.subject.unchanged())


if __name__ == '__main__':
    unittest.main()
