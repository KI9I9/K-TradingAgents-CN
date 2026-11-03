"""Deliver paper valuation/error presentation through the two existing binds.

Frozen stdin package; no Compose, permit, schedule or business-data mutation.
Preflight and execution have separate receipts. An ambiguous SSH response must
be resolved through status, never by launching another execution.
"""
from datetime import datetime, timezone
import base64
import copy
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

ROOT = Path('/home/ubuntu/k-tradingagents-paper-display-20261103')
BACKEND = 'ta-live-088a60b962bf-backend-1'
FRONTEND = 'ta-live-088a60b962bf-frontend-1'
TARGETS = {'/app/app/routers/paper.py', '/app/app/services/paper_quote_supply.py'}


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


def info(name=BACKEND):
    return json.loads(run(['docker', 'inspect', name]))[0]


def running():
    value = info()
    require(value['Id'] == REQUEST['backend_id'] and value['Image'] == REQUEST['image']
            and len(value['Mounts']) == 45, 'backend_identity_changed')
    require(value['State']['Running'] and value['State'].get('Health', {}).get('Status') == 'healthy',
            'backend_unhealthy')
    require(set(REQUEST['sources']) == TARGETS, 'unexpected_source_targets')
    for target, item in REQUEST['sources'].items():
        mounts = [m for m in value['Mounts'] if m['Destination'] == target]
        require(len(mounts) == 1 and mounts[0]['Source'] == item['host_source']
                and mounts[0]['Type'] == 'bind' and mounts[0]['RW'] is False, 'source_bind_changed')
    config = json.loads(run(REQUEST['chain']+['config', '--format', 'json']))
    image = json.loads(run(['docker', 'image', 'inspect', config['services']['backend']['image']]))
    require(not VERIFY.compare_running('backend', value, image, config), 'running_config_drift')
    return value, config


def snapshot(new=False, backup=False):
    modules = dict(REQUEST['modules'])
    if new:
        modules.update({target: item['sha256'] for target, item in REQUEST['sources'].items()})
    data = dict(runtime=REQUEST['runtime'], modules=modules,
        backup='/app/runtime/paper-display-20261103/'+PAYLOAD['attempt'] if backup else None)
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', REQUEST['snapshot_code']], json.dumps(data))
    rows = [json.loads(s.split(':', 1)[1]) for s in output.splitlines() if s.startswith('ETF_SNAPSHOT:')]
    require(len(rows) == 1, 'snapshot_missing')
    return rows[0]


def protected(value):
    value = copy.deepcopy(value); value.pop('modules')
    for row in value['collections'].values():
        row.pop('raw_sha256')
    return value


def jobs():
    closed = ['akshare_quotes_sync', 'etf_sync', 'tushare_realtime_quotes_hourly']
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', REQUEST['query_code']],
        json.dumps(dict(auth=PAYLOAD['auth'], jobs=closed+['tushare_historical_sync', 'akshare_historical_sync'])))
    value = json.loads(output)
    require(value.get('success') is True and not value.get('missing_jobs'), 'job_query_failed')
    for row in value['data']:
        require(row.get('has_running_execution') is False, 'job_running')
        if row['id'] in closed:
            require(row.get('paused') is True and row.get('next_run_time') is None, 'closed_job_enabled')
        else:
            require(row.get('paused') is False and row.get('next_run_time'), 'history_schedule_disabled')
            due = datetime.fromisoformat(row['next_run_time'].replace('Z', '+00:00'))
            require((due-datetime.now(timezone.utc)).total_seconds() > 1800, 'schedule_due_soon')
    save(ATTEMPT/'jobs.json', value)


def frontend_root():
    value = info(FRONTEND)
    require(value['Id'] == REQUEST['frontend_id'], 'frontend_identity_changed')
    mounts = [m for m in value['Mounts'] if m['Destination'] == '/usr/share/nginx/html']
    require(len(mounts) == 1 and mounts[0]['Source'] == REQUEST['frontend_root'], 'frontend_bind_changed')
    return Path(mounts[0]['Source'])


def frontend_probe(checks):
    import ssl
    value = info(FRONTEND)
    cert = Path(next(m['Source'] for m in value['Mounts'] if m['Destination'] == '/run/secrets/tls-cert.pem')).read_text()
    require(digest(ssl.PEM_cert_to_DER_cert(cert)) == REQUEST['frontend_cert_sha256'], 'frontend_cert_drift')
    code = REQUEST['http_probe']+'\nimport json\nprint(json.dumps(verify_frontend_resources('+repr(checks)+','+repr(cert)+',timeout=60)))'
    return json.loads(run(['docker', 'exec', BACKEND, 'python3', '-B', '-c', code], timeout=70))


def stage():
    for target, item in REQUEST['sources'].items():
        require(target in TARGETS and digest(item['source'].encode()) == item['sha256'], 'candidate_changed')
        path = ROOT/'staged'/target.removeprefix('/app/')
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        if path.exists(): require(path.read_bytes() == item['source'].encode(), 'stage_changed')
        else: path.write_text(item['source'])
        path.chmod(0o644)
    for name, item in REQUEST['frontend_delta'].items():
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe_asset_path')
        raw = base64.b64decode(item['base64'], validate=True)
        require(digest(raw) == item['sha256'], 'candidate_asset_changed')
        path = ROOT/'staged/frontend'/name
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        if path.exists(): require(path.read_bytes() == raw, 'stage_asset_changed')
        else: path.write_bytes(raw)
        path.chmod(0o644)
    (ROOT/'staged').chmod(0o755)
    for path in (ROOT/'staged').rglob('*'):
        if path.is_dir(): path.chmod(0o755)


def preflight():
    stage(); value, config = running()
    save(ATTEMPT/'backup/backend-inspect.private.json', value)
    save(ATTEMPT/'backup/config.private.json', config)
    for i, path in enumerate(REQUEST['chain_files']):
        save(ATTEMPT/('backup/compose/'+str(i)), Path(path).read_bytes())
    front = frontend_root()
    actual = {str(p.relative_to(front)): digest(p.read_bytes()) for p in front.rglob('*') if p.is_file()}
    require(actual == REQUEST['frontend_before'], 'frontend_files_drift')
    save(ATTEMPT/'backup/index.html', (front/'index.html').read_bytes())
    for name in REQUEST['frontend_delta']:
        require(name == 'index.html' or name not in actual, 'asset_collision')
    for target, item in REQUEST['sources'].items():
        source = Path(item['host_source'])
        require(not source.is_symlink() and digest(source.read_bytes()) == REQUEST['modules'][target], 'original_source_changed')
        save(ATTEMPT/'backup/sources'/target.removeprefix('/app/'), source.read_bytes())
    jobs(); before = snapshot(backup=True)
    save(ATTEMPT/'before.json', before)
    require(before['runtime'].get('/app/runtime/paper-quote-supply.json') == REQUEST['permit_sha256'], 'permit_changed')
    argv = HELPER.native_import_argv(value,
        {t: str(ROOT/'staged'/t.removeprefix('/app/')) for t in REQUEST['sources']},
        {t: i['sha256'] for t, i in REQUEST['sources'].items()})
    require('PAPER_IMPORT_OK:' in run(argv, timeout=65), 'full_app_import_failed')
    require(protected(snapshot()) == protected(before), 'preflight_state_changed')
    jobs()
    save(ATTEMPT/'backup/manifest.json', {str(p.relative_to(ATTEMPT/'backup')): digest(p.read_bytes())
         for p in (ATTEMPT/'backup').rglob('*') if p.is_file()})
    return before


def overwrite(target, after):
    source = Path(REQUEST['sources'][target]['host_source'])
    require(not source.is_symlink(), 'source_symlink')
    original = (ATTEMPT/'backup/sources'/target.removeprefix('/app/')).read_bytes()
    candidate = REQUEST['sources'][target]['source'].encode()
    require(after in (original, candidate), 'unexpected_replacement')
    with os.fdopen(os.open(source, os.O_RDWR | os.O_NOFOLLOW), 'r+b') as stream:
        metadata = os.fstat(stream.fileno())
        require(stat.S_ISREG(metadata.st_mode) and stream.read() in (original, candidate), 'source_changed')
        stream.seek(0); stream.write(after); stream.truncate(); stream.flush(); os.fsync(stream.fileno())
        current = source.stat()
        require((current.st_ino, current.st_mode, current.st_uid, current.st_gid) ==
                (metadata.st_ino, metadata.st_mode, metadata.st_uid, metadata.st_gid), 'source_metadata_changed')
    require(source.read_bytes() == after, 'source_readback_failed')


def stop():
    require(info()['Id'] == REQUEST['backend_id'], 'container_changed_do_not_stop')
    run(['docker', 'stop', '--time', '30', BACKEND], timeout=45)
    require(not info()['State']['Running'], 'backend_still_running')


def start():
    run(['docker', 'start', BACKEND], timeout=40)
    for _ in range(36):
        value = info(); require(value['State']['Running'], 'backend_exited')
        if value['State'].get('Health', {}).get('Status') == 'healthy': return
        time.sleep(5)
    raise RuntimeError('backend_health_timeout')


def publish_frontend():
    root = frontend_root()
    for name, item in REQUEST['frontend_delta'].items():
        if name == 'index.html': continue
        path = root/name; path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        require(not path.exists(), 'asset_collision_after_preflight')
        raw = (ROOT/'staged/frontend'/name).read_bytes()
        require(digest(raw) == item['sha256'], 'staged_asset_changed')
        with path.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        path.chmod(0o644)
    save(root/'index.html', (ROOT/'staged/frontend/index.html').read_bytes())
    (root/'index.html').chmod(0o644)


def accept_api():
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', REQUEST['api_probe']],
                 json.dumps(dict(auth=PAYLOAD['auth'], runtime=REQUEST['runtime'])))
    rows = [json.loads(s.split(':', 1)[1]) for s in output.splitlines() if s.startswith('PAPER_DISPLAY_ACCEPT:')]
    require(len(rows) == 1 and rows[0]['ok'], 'api_acceptance_failed')
    save(ATTEMPT/'api.json', rows[0])


def main():
    attempted = False; front_attempted = False; before = None
    try:
        event('preflight_and_backup'); before = preflight()
        if PAYLOAD['check_only']:
            event('ready_no_switch'); return 0
        event('stopping_backend'); attempted = True; stop()
        for target, item in REQUEST['sources'].items(): overwrite(target, item['source'].encode())
        event('starting_backend'); start(); running()
        require(protected(snapshot(True)) == protected(before), 'deployment_state_changed')
        jobs(); accept_api()
        event('publishing_frontend'); front_attempted = True; publish_frontend()
        save(ATTEMPT/'frontend-http.json', frontend_probe({p: i['sha256'] for p, i in REQUEST['frontend_delta'].items()}))
        after = snapshot(True)
        require(protected(after) == protected(before), 'acceptance_state_changed')
        save(ATTEMPT/'after.json', after)
        save(ATTEMPT/'completion.json', dict(status='deployed', source_head=REQUEST['source_head'],
             backend_mounts=45, frontend_resources=len(REQUEST['frontend_delta']),
             permit_sha256=REQUEST['permit_sha256'], business_data_unchanged=True, orders_created=0))
        event('complete'); return 0
    except BaseException as error:
        STATE['reason'] = str(error) if type(error) is RuntimeError else type(error).__name__
        event('failed')
        if attempted:
            try:
                event('rolling_back')
                if front_attempted:
                    root = frontend_root(); save(root/'index.html', (ATTEMPT/'backup/index.html').read_bytes())
                    (root/'index.html').chmod(0o644)
                stop()
                for target in REQUEST['sources']:
                    overwrite(target, (ATTEMPT/'backup/sources'/target.removeprefix('/app/')).read_bytes())
                start(); running()
                require(protected(snapshot()) == protected(before), 'rollback_state_changed_preserve_database')
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
