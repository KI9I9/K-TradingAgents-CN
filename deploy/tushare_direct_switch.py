"""Finite host relay switch. No HTTP, application gate or database operations.

Delivered with an exact prepared manifest and a detached systemd worker. The
independent rollback timer outlives both the management connection and worker.
"""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time


TLS_CHECK = """import hashlib,json,pathlib,runpy,sys,time
path=pathlib.Path('/app/tradingagents/utils/tushare_egress.py')
if hashlib.sha256(path.read_bytes()).hexdigest()!=sys.argv[1]:raise SystemExit('transport_source_changed')
UnixTLSConnection=runpy.run_path(str(path))['UnixTLSConnection']
rows=[]
for index in range(3):
 started=time.monotonic();connection=UnixTLSConnection(timeout=20)
 try:
  connection.connect()
  rows.append(dict(index=index+1,verified=True,tls=connection.sock.version(),elapsed_s=round(time.monotonic()-started,3)))
 finally:connection.close()
print('DIRECT_ROUTE_TLS_JSON:'+json.dumps(dict(http_requests=0,observations=rows)))
"""


def require(ok, reason):
    if not ok:
        raise RuntimeError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def write_new(path, raw, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def entry(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode), 'not_regular:' + str(path))
    return dict(sha256=sha(path.read_bytes()), mode=stat.S_IMODE(info.st_mode), uid=info.st_uid, gid=info.st_gid)


class Switch:
    def __init__(self, request):
        self.r = request
        self.root = Path(request['root'])
        require(self.root.resolve() == self.root and self.root.stat().st_uid == os.geteuid()
                and stat.S_IMODE(self.root.stat().st_mode) == 0o700, 'unsafe_preparation_directory')
        raw = (self.root / 'manifest.json').read_bytes()
        require(sha(raw) == request['manifest_sha256'], 'manifest_changed')
        self.m = json.loads(raw)
        self.work = self.root / 'switch-v1'
        self.timer = request['expiry_unit'] + '.timer'
        self.link = Path(request['unit_directory']) / 'timers.target.wants' / self.timer

    def run(self, args, timeout=15):
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            self.save('command-error-' + str(time.time_ns()) + '.json',
                      dict(program=args[0], returncode=result.returncode, stderr=result.stderr))
        require(result.returncode == 0, 'command_failed:' + args[0])
        return result.stdout.strip()

    def save(self, name, value):
        target = self.work / name
        pending = self.work / (name + '.pending-' + str(time.time_ns()))
        write_new(pending, canonical(value))
        os.replace(pending, target)
        sync_directory(self.work)

    def stage(self, name, **fields):
        self.save('status.json', dict(stage=name, at=time.time(), **fields))

    def properties(self, unit):
        fields = ['FragmentPath', 'DropInPaths', 'ExecStart', 'ExecCondition', 'User', 'Group',
                  'DynamicUser', 'ActiveState', 'SubState', 'IPAddressDeny', 'IPAddressAllow',
                  'PrivateNetwork', 'RestrictAddressFamilies', 'Listen']
        raw = self.run(['systemctl', 'show', unit] + sum([['-p', key] for key in fields], []))
        return dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)

    @contextlib.contextmanager
    def locks(self):
        runtime = Path(self.m['runtime_source'])
        paths = [self.work / 'operator.lock', runtime / 'tushare-history-batch-v1/admission.lock',
                 runtime / 'tushare-history-generations/20aa2215a7904e0d8d6601b14b8a6c90/gate.lock']
        descriptors = []
        try:
            for path in paths:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                descriptors.append(fd)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            for fd in reversed(descriptors):
                os.close(fd)

    def unchanged(self):
        return all(entry(Path(path)) == expected for path, expected
                   in (self.m['originals'] | self.m['protected']).items())

    def socket_unchanged(self):
        info = Path(self.r['socket_path']).stat()
        return stat.S_ISSOCK(info.st_mode) and [info.st_dev, info.st_ino] == self.m['socket_inode']

    def backend(self):
        backend = json.loads(self.run(['docker', 'inspect', 'ta-live-088a60b962bf-backend-1']))[0]
        require(backend['Id'] == self.m['backend_id'] and backend['Image'] == self.m['image']
                and backend['State']['Running'] and backend['State'].get('Health', {}).get('Status') == 'healthy'
                and len(backend['Mounts']) == 44, 'backend_changed')
        require(any(mount.get('Destination') == '/app/runtime' and mount.get('Source') == self.m['runtime_source']
                    for mount in backend['Mounts']), 'runtime_mount_changed')

    def preflight(self):
        require(time.time() + 300 < self.m['expiry'] == self.r['expiry'], 'expired_or_too_close')
        self.backend()
        require(json.loads((self.root / 'status.json').read_text())['stage'] == 'prepared_no_route_change', 'not_prepared')
        require(self.unchanged(), 'original_or_control_changed')
        require(self.socket_unchanged(), 'socket_inode_changed')
        require(self.properties(self.r['unit']) == self.m['service']
                and self.properties(self.r['socket']) == self.m['socket'], 'effective_unit_changed')
        require(Path(self.r['approved_path']).is_file(), 'transport_approval_missing')
        require(not os.path.lexists(self.link), 'existing_timer_link')
        require(set(self.m['candidates']) == set(self.r['candidate_sources']), 'candidate_set_changed')
        for destination, candidate in self.m['candidates'].items():
            require(not os.path.lexists(destination), 'destination_exists')
            source = self.root / 'candidate' / candidate['file']
            require(entry(source)['sha256'] == candidate['sha256'] == sha(self.r['candidate_sources'][destination].encode()),
                    'candidate_changed')
        all_files = self.m['originals'] | self.m['protected']
        for name, source in self.m['backup_files'].items():
            require(entry(self.root / 'backup' / name)['sha256'] == all_files[source]['sha256'], 'backup_changed')

    def ensure_directory(self, path):
        if not path.exists():
            path.mkdir(mode=0o755)
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and path.resolve() == path and info.st_uid == os.geteuid()
                and not stat.S_IMODE(info.st_mode) & 0o022, 'unsafe_install_directory')

    def install(self):
        # Install and verify the expiry before the live proxy is restarted.
        for destination, raw in self.r['candidate_sources'].items():
            path = Path(destination)
            self.ensure_directory(path.parent)
            write_new(path, raw.encode(), 0o644)
            sync_directory(path.parent)
            require(entry(path)['sha256'] == self.m['candidates'][destination]['sha256'], 'installed_hash_mismatch')
        self.ensure_directory(self.link.parent)
        os.symlink(str(Path(self.r['unit_directory']) / self.timer), self.link)
        sync_directory(self.link.parent)
        self.run(['systemctl', 'daemon-reload'])
        self.run(['systemctl', 'start', self.timer])
        timer = self.run(['systemctl', 'show', self.timer, '-p', 'ActiveState', '-p', 'SubState'])
        require('ActiveState=active' in timer and 'SubState=waiting' in timer, 'expiry_not_armed')
        escaped = ''.join(char if char.isascii() and char.isalnum() else '_' + format(ord(char), '02x') for char in self.timer)
        deadline = self.run(['busctl', 'get-property', 'org.freedesktop.systemd1',
                             '/org/freedesktop/systemd1/unit/' + escaped, 'org.freedesktop.systemd1.Timer',
                             'NextElapseUSecRealtime']).split()
        require(deadline == ['t', str(self.r['expiry'] * 1000000)], 'expiry_deadline_mismatch')

    def rollback(self):
        # Never restore application files or business data. Remove only our exact
        # added files; a collision causes a closed transport for inspection.
        self.stage('rolling_back')
        try:
            require(self.unchanged(), 'original_or_control_changed')
            for destination, candidate in self.m['candidates'].items():
                if os.path.lexists(destination):
                    require(entry(Path(destination))['sha256'] == candidate['sha256'], 'foreign_candidate_file')
            if os.path.lexists(self.link):
                require(self.link.is_symlink() and os.readlink(self.link) == str(Path(self.r['unit_directory']) / self.timer),
                        'foreign_timer_link')
            if Path(self.r['unit_directory'], self.timer).exists():
                self.run(['systemctl', 'stop', self.timer])
            if os.path.lexists(self.link):
                self.link.unlink()
                sync_directory(self.link.parent)
            for destination in self.m['candidates']:
                path = Path(destination)
                if path.exists():
                    path.unlink()
                    sync_directory(path.parent)
            self.run(['systemctl', 'daemon-reload'])
            if time.time() >= self.r['expiry']:
                self.run(['systemctl', 'stop', self.r['socket'], self.r['unit']])
                self.stage('rolled_back_expired_transport_closed')
                return
            self.run(['systemctl', 'restart', self.r['unit']])
            current = self.properties(self.r['unit'])
            require(current.get('ActiveState') == 'active' and current.get('SubState') == 'running'
                    and 'argv[]=' + self.r['old_command'] + ' ;' in current['ExecStart']
                    and not current.get('DropInPaths') and not current.get('ExecCondition'), 'rollback_not_verified')
            require(self.unchanged() and self.socket_unchanged() and self.properties(self.r['socket']) == self.m['socket'],
                    'rollback_state_changed')
            self.stage('rolled_back', protected_unchanged=True, socket_inode_unchanged=True, http_requests=0)
        except BaseException as error:
            self.run(['systemctl', 'stop', self.r['socket'], self.r['unit']])
            self.stage('rollback_incomplete_transport_closed', error=type(error).__name__, reason=str(error))
            raise

    def execute(self):
        with self.locks():
            self.stage('preflight')
            self.preflight()
            self.run(['systemd-run', '--quiet', '--unit=' + self.r['guard_unit'], '--on-active=180s',
                      '--timer-property=AccuracySec=1s', '--property=TimeoutStartSec=60',
                      '/usr/bin/python3', '-B', str(self.work / 'worker.py'), str(self.work / 'request.json'), '--rollback'])
            guard = self.run(['systemctl', 'show', self.r['guard_unit'] + '.timer', '-p', 'ActiveState', '-p', 'SubState'])
            require('ActiveState=active' in guard and 'SubState=waiting' in guard, 'rollback_guard_not_armed')
            try:
                self.stage('installing_expiry_and_dropin')
                self.install()
                self.stage('restarting_relay')
                self.run(['systemctl', 'restart', self.r['unit']])
                service = self.properties(self.r['unit'])
                require(service.get('ActiveState') == 'active' and service.get('SubState') == 'running', 'relay_not_running')
                require('argv[]=' + self.r['new_command'] + ' ;' in service['ExecStart']
                        and service.get('DropInPaths') == self.r['dropin']
                        and str(self.r['expiry']) in service.get('ExecCondition', ''), 'direct_command_not_loaded')
                altered = {'ExecStart', 'ExecCondition', 'DropInPaths', 'ActiveState', 'SubState'}
                require({k: v for k, v in service.items() if k not in altered} ==
                        {k: v for k, v in self.m['service'].items() if k not in altered}, 'sandbox_changed')
                self.stage('checking_backend_unix_tls')
                output = self.run(['docker', 'exec', self.m['backend_id'], 'python3', '-B', '-c', TLS_CHECK,
                                   self.r['transport_sha256']], timeout=70)
                rows = [line.split(':', 1)[1] for line in output.splitlines() if line.startswith('DIRECT_ROUTE_TLS_JSON:')]
                require(len(rows) == 1, 'backend_tls_receipt_missing')
                result = json.loads(rows[0])
                require(result['http_requests'] == 0 and len(result['observations']) == 3
                        and all(row['verified'] and row['tls'] in ('TLSv1.2', 'TLSv1.3') for row in result['observations']),
                        'backend_tls_unverified')
                self.backend()
                require(self.unchanged() and self.socket_unchanged()
                        and self.properties(self.r['socket']) == self.m['socket'], 'final_state_changed')
                require(time.time() < self.r['expiry'], 'expired_during_switch')
                result.update(route='production_direct', expires_at=self.r['expiry'], protected_unchanged=True,
                              socket_inode_unchanged=True, backend_unchanged=True, history_still_stopped=True)
                self.save('committed.json', result)
                self.stage('passed', **result)
            except BaseException as error:
                self.save('switch-error.json', dict(error=type(error).__name__, reason=str(error)))
                if not (self.work / 'committed.json').exists():
                    self.rollback()
                raise
            finally:
                # A late guard also checks the durable commit marker. Failure
                # to stop the guard must not roll back an accepted route.
                try:
                    final_stage = json.loads((self.work / 'status.json').read_text())['stage']
                    if (self.work / 'committed.json').exists() or final_stage in (
                            'rolled_back', 'rolled_back_expired_transport_closed', 'rollback_incomplete_transport_closed'):
                        self.run(['systemctl', 'stop', self.r['guard_unit'] + '.timer'])
                except Exception:
                    pass

    def watchdog(self):
        if (self.work / 'committed.json').exists():
            return
        # --collect may already have unloaded a worker killed at RuntimeMaxSec.
        # "not loaded" is not a reason to skip the independent rollback.
        state = subprocess.run(['systemctl', 'show', self.r['worker_unit'], '-p', 'LoadState', '-p', 'ActiveState'],
                               capture_output=True, text=True, timeout=15)
        fields = dict(line.split('=', 1) for line in state.stdout.splitlines() if '=' in line)
        if not (fields.get('LoadState') == 'not-found' and fields.get('ActiveState') == 'inactive'):
            self.run(['systemctl', 'stop', self.r['worker_unit']])
        with self.locks():
            if (self.work / 'committed.json').exists():
                return
            current = json.loads((self.work / 'status.json').read_text())
            if current['stage'] in ('reserved', 'preflight'):
                self.stage('failed_before_route_change', http_requests=0)
                return
            if current['stage'] in ('rolled_back', 'rolled_back_expired_transport_closed'):
                return
            self.rollback()


def main():
    os.umask(0o077)
    require(os.geteuid() == 0, 'root_required')
    path = Path(sys.argv[1])
    require(path.resolve() == path and entry(path)['mode'] == 0o600 and entry(path)['uid'] == 0, 'unsafe_request')
    request = json.loads(path.read_text())
    require(sha(Path(__file__).read_bytes()) == request['worker_sha256'], 'worker_changed')
    switch = Switch(request)
    if sys.argv[2:] == ['--rollback']:
        switch.watchdog()
    else:
        require(len(sys.argv) == 2, 'invalid_arguments')
        try:
            switch.execute()
        except BaseException as error:
            switch.save('worker-error.json', dict(error=type(error).__name__, reason=str(error)))
            raise


if __name__ == '__main__':
    main()
