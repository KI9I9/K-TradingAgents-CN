"""One reader update plus a separately checked finite permit renewal.

No Compose mutation, new mount, upstream Tushare request or order submission.
The existing bind inode is preserved. Backups and full container configuration
remain on production. Run with the frozen detached-controller launcher.
"""
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import types

ROOT = Path('/home/ubuntu/k-tradingagents-paper-continuity-20261103')
BACKEND = 'ta-live-088a60b962bf-backend-1'
TARGET = '/app/app/services/paper_quote_supply.py'
SOURCE = Path('/home/ubuntu/k-tradingagents-paper-supply-20261102/staged/app/services/paper_quote_supply.py')
PERMIT = '/app/runtime/paper-quote-supply.json'


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def save(path, value):
    raw = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True).encode()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix('.tmp')
    with temporary.open('wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    temporary.replace(path)


def event(stage):
    STATE.update(stage=stage, at=datetime.now(timezone.utc).isoformat())
    save(ATTEMPT/'state.json', STATE)


def run(argv, data=None, timeout=60):
    result = subprocess.run(argv, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        save(ATTEMPT/'command-failure.private.json', dict(returncode=result.returncode,
             stdout=result.stdout, stderr=result.stderr))
        raise RuntimeError('command_failed_private_receipt')
    return result.stdout


def info():
    return json.loads(run(['docker', 'inspect', BACKEND]))[0]


def running():
    value = info()
    require(value['Id'] == REQUEST['backend_id'] and value['Image'] == REQUEST['image']
            and len(value['Mounts']) == 45, 'backend_identity_changed')
    require(value['State']['Running'] and value['State'].get('Health', {}).get('Status') == 'healthy',
            'backend_unhealthy')
    mounts = [m for m in value['Mounts'] if m['Destination'] == TARGET]
    require(len(mounts) == 1 and mounts[0]['Source'] == str(SOURCE)
            and mounts[0]['Type'] == 'bind' and mounts[0]['RW'] is False, 'quote_bind_changed')
    config = json.loads(run(REQUEST['chain']+['config', '--format', 'json']))
    image = json.loads(run(['docker', 'image', 'inspect', config['services']['backend']['image']]))
    require(not VERIFY.compare_running('backend', value, image, config), 'running_config_drift')
    return value, config


def snapshot(new=False, backup=None):
    modules = dict(REQUEST['modules'])
    if new:
        modules[TARGET] = REQUEST['candidate_sha256']
    data = dict(runtime=REQUEST['runtime'], modules=modules, backup=str(backup) if backup else None)
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', REQUEST['snapshot_code']],
                 json.dumps(data))
    rows = [json.loads(s.split(':', 1)[1]) for s in output.splitlines() if s.startswith('ETF_SNAPSHOT:')]
    require(len(rows) == 1, 'snapshot_missing')
    return rows[0]


def protected(value):
    value = dict(value); value.pop('modules')
    value['collections'] = {k: {x: v for x, v in row.items() if x != 'raw_sha256'}
                            for k, row in value['collections'].items()}
    return value


def jobs():
    closed = ['akshare_quotes_sync', 'etf_sync', 'tushare_realtime_quotes_hourly']
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', REQUEST['query_code']],
        json.dumps(dict(auth=PAYLOAD['auth'], jobs=closed+['tushare_historical_sync', 'akshare_historical_sync'])))
    value = json.loads(output)
    require(value.get('success') is True and not value.get('missing_jobs'), 'job_query_failed')
    for row in value['data']:
        require(not row.get('has_running_execution'), 'job_running')
        if row['id'] in closed:
            require(row.get('paused') is True and row.get('next_run_time') is None, 'closed_job_enabled')
        else:
            require(row.get('paused') is False and row.get('next_run_time'), 'history_schedule_disabled')
            due = datetime.fromisoformat(row['next_run_time'].replace('Z', '+00:00'))
            require((due-datetime.now(timezone.utc)).total_seconds() > 1800, 'schedule_due_soon')


def renewal_request(check_only):
    modules = dict(REQUEST['modules'])
    if not check_only:
        modules[TARGET] = REQUEST['candidate_sha256']
    return dict(previous_sha256=REQUEST['previous_permit_sha256'], expires_at=REQUEST['expires_at'],
        modules=modules, runtime=REQUEST['runtime'], snapshot_code=REQUEST['snapshot_code'],
        query_code=REQUEST['query_code'], validate_code=REQUEST['validate_code'], auth=PAYLOAD['auth'],
        backup='/app/runtime/paper-continuity-20261103/'+PAYLOAD['attempt']+'/renewal', check_only=check_only)


def call_renewal(check_only):
    # Preflight loads the candidate reader only in this separate interpreter.
    # The running app and its current permit are untouched.
    reader = ('import sys,types\nm=types.ModuleType("app.services.paper_quote_supply");'
              'sys.modules[m.__name__]=m\nexec('+repr(REQUEST['candidate'])+',m.__dict__)\n') if check_only else ''
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', reader+REQUEST['renewal_code']],
                 json.dumps(renewal_request(check_only)), timeout=140)
    rows = [json.loads(s.split(':', 1)[1]) for s in output.splitlines()
            if s.startswith('PAPER_RENEWAL_RESULT:')]
    require(len(rows) == 1 and rows[0]['ok'], 'renewal_not_accepted_inspect_private_receipt')
    save(ATTEMPT/('renewal-check.json' if check_only else 'renewal.json'), rows[0])
    return rows[0]


def overwrite(before, after):
    require(not SOURCE.is_symlink(), 'source_symlink')
    fd = os.open(SOURCE, os.O_RDWR | os.O_NOFOLLOW)
    with os.fdopen(fd, 'r+b') as stream:
        metadata = os.fstat(stream.fileno())
        require(stat.S_ISREG(metadata.st_mode) and stream.read() == before, 'source_changed')
        require(digest(after) in (REQUEST['modules'][TARGET], REQUEST['candidate_sha256']), 'unexpected_source_bytes')
        stream.seek(0); stream.write(after); stream.truncate(); stream.flush(); os.fsync(stream.fileno())
        require(os.stat(SOURCE).st_ino == metadata.st_ino and os.stat(SOURCE).st_mode == metadata.st_mode,
                'source_inode_or_mode_changed')
    require(SOURCE.read_bytes() == after, 'source_readback_failed')


def permit_on_host(container):
    """Read permit even if an updated application fails before docker exec works."""
    mounts = [m for m in container['Mounts']
              if Path(m['Destination']) == Path(PERMIT) or Path(m['Destination']) in Path(PERMIT).parents]
    require(bool(mounts), 'runtime_mount_missing')
    mount = max(mounts, key=lambda m: len(Path(m['Destination']).parts))
    require(mount['Type'] in ('bind', 'volume'), 'runtime_mount_unsupported')
    path = Path(mount['Source'])/Path(PERMIT).relative_to(mount['Destination'])
    require(not path.is_symlink() and path.is_file(), 'host_permit_invalid')
    return path


def stop():
    require(info()['Id'] == REQUEST['backend_id'], 'container_changed_do_not_stop')
    run(['docker', 'stop', '--time', '30', BACKEND], timeout=45)
    require(not info()['State']['Running'], 'backend_still_running')


def start():
    run(['docker', 'start', BACKEND], timeout=40)
    for _ in range(36):
        value = info(); require(value['State']['Running'], 'backend_exited')
        if value['State'].get('Health', {}).get('Status') == 'healthy':
            return
        time.sleep(5)
    raise RuntimeError('backend_health_timeout')


def main():
    attempted = False; before = None; original = None; host_permit = None
    try:
        event('preflight_and_backup')
        value, config = running()
        host_permit = permit_on_host(value)
        require(digest(host_permit.read_bytes()) == REQUEST['previous_permit_sha256'], 'host_permit_changed')
        require(not SOURCE.is_symlink(), 'source_symlink')
        original = SOURCE.read_bytes()
        require(digest(original) == REQUEST['modules'][TARGET], 'original_reader_changed')
        require(digest(REQUEST['candidate'].encode()) == REQUEST['candidate_sha256'], 'candidate_changed')
        stage = ROOT/'staged'; stage.mkdir(exist_ok=True, mode=0o755); stage.chmod(0o755)
        candidate = stage/'paper_quote_supply.py'
        if candidate.exists():
            require(candidate.read_text() == REQUEST['candidate'], 'stage_changed')
        else:
            candidate.write_text(REQUEST['candidate']); candidate.chmod(0o644)
        save(ATTEMPT/'backup/reader.py', original)
        save(ATTEMPT/'backup/backend-inspect.private.json', value)
        save(ATTEMPT/'backup/config.private.json', config)
        jobs()
        before = snapshot(backup=Path('/app/runtime/paper-continuity-20261103')/PAYLOAD['attempt']/'baseline')
        save(ATTEMPT/'before.json', before)
        require(before['runtime'].get(PERMIT) == REQUEST['previous_permit_sha256'], 'permit_changed')
        argv = HELPER.native_import_argv(value, {TARGET: str(candidate)}, {TARGET: REQUEST['candidate_sha256']})
        require('PAPER_IMPORT_OK:' in run(argv, timeout=65), 'full_app_import_failed')
        # Candidate source hash is checked separately; only this child sees it here.
        call_renewal(True)
        require(protected(snapshot()) == protected(before), 'preflight_state_changed')
        jobs()
        if PAYLOAD['check_only']:
            event('ready_no_switch_or_renewal'); return 0
        # Refuse to interrupt service for a renewal that cannot be accepted yet.
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo('Asia/Shanghai')); minute = now.hour*60+now.minute
        require(now.weekday() < 5 and (570 <= minute < 680 or 780 <= minute < 890),
                'outside_safe_renewal_window')
        event('stopping_backend'); attempted = True; stop()
        overwrite(original, REQUEST['candidate'].encode())
        event('starting_backend'); start(); running()
        require(protected(snapshot(True)) == protected(before), 'deployment_state_changed')
        jobs()
        event('accepting_finite_renewal'); result = call_renewal(False)
        after = snapshot(True)
        old_protected, new_protected = protected(before), protected(after)
        old_protected['runtime'] = dict(old_protected['runtime']); new_protected['runtime'] = dict(new_protected['runtime'])
        old_protected['runtime'].pop(PERMIT); new_protected['runtime'].pop(PERMIT)
        require(old_protected == new_protected and after['runtime'][PERMIT] == result['permit_sha256'],
                'post_renewal_state_changed')
        save(ATTEMPT/'completion.json', dict(status='reader_deployed_and_permit_renewed',
            expires_at=result['expires_at'], permit_sha256=result['permit_sha256'],
            backend_mounts=45, orders_created=0, source_head=REQUEST['source_head']))
        event('complete'); return 0
    except BaseException as error:
        STATE['reason'] = str(error) if type(error) is RuntimeError else type(error).__name__
        event('failed')
        if attempted:
            try:
                # Do not restore v1 code over a v2 permit (e.g. uncertain exec result).
                require(not host_permit.is_symlink() and
                        digest(host_permit.read_bytes()) == REQUEST['previous_permit_sha256'],
                        'renewal_outcome_requires_attention')
                stop()
                actual = SOURCE.read_bytes()
                require(actual in (original, REQUEST['candidate'].encode()), 'source_changed_preserve')
                if actual != original:
                    overwrite(actual, original)
                start(); running()
                require(protected(snapshot()) == protected(before), 'rollback_state_changed')
                jobs(); event('rolled_back')
            except BaseException as rollback_error:
                STATE['rollback_reason'] = str(rollback_error) if type(rollback_error) is RuntimeError else type(rollback_error).__name__
                event('rollback_requires_attention')
        return 1


if __name__ == '__main__':
    os.umask(0o077); PAYLOAD = json.load(sys.stdin); REQUEST = PAYLOAD['request']
    ROOT.mkdir(exist_ok=True, mode=0o711); ROOT.chmod(0o711)
    ATTEMPT = ROOT/'attempts'/PAYLOAD['attempt']
    require(ATTEMPT.parent == ROOT/'attempts' and ATTEMPT.is_dir(), 'invalid_attempt')
    lock = (ROOT/'switch.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    VERIFY = types.ModuleType('verify'); exec(REQUEST['verify_code'], VERIFY.__dict__)
    HELPER = types.ModuleType('prepare'); HELPER.__file__ = str(ROOT/'prepare.py')
    exec(REQUEST['prepare_code'], HELPER.__dict__)
    STATE = {}; raise SystemExit(main())
