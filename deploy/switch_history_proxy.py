"""Bounded history button release on top of the deployed history release; never changes gates or business data.

Run detached by run_history_button_switch.py. Credentials arrive on stdin only.
Every attempt has a private evidence/backup directory; a host lock prevents overlap.
"""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import types

ROOT = Path('/home/ubuntu/k-tradingagents-history-proxy-v1-20261029')
STAGED = ROOT / 'staged'
BACKEND = 'ta-live-088a60b962bf-backend-1'
FRONTEND = 'ta-live-088a60b962bf-frontend-1'
MONGO = 'ta-live-088a60b962bf-mongodb-1'
JOBS = ['akshare_quotes_sync', 'quotes_ingestion_service', 'etf_sync',
        'tushare_realtime_quotes_hourly', 'tushare_historical_sync', 'akshare_historical_sync']
DISABLED = set(JOBS) - {'akshare_historical_sync'}
ENTRY_HASH = 'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b'
CERT_HASH = 'd8245a033b6e0ed360a5b7b64a14b8757b15fcec9a90a69cd0300e0dd2935be4'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()


def require(value, code):
    if not value:
        raise RuntimeError(code)


def save(name, data):
    path = ATTEMPT / name
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    raw = data if isinstance(data, bytes) else json.dumps(data, indent=2).encode()
    tmp = path.with_name(path.name + '.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    tmp.replace(path)


def event(stage):
    STATE['stage'] = stage
    STATE['events'].append({'stage': stage, 'utc': datetime.now(timezone.utc).isoformat()})
    save('state.json', STATE)
    print(stage, flush=True)


def run(args, data=None, timeout=60):
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        save('last-command-failure.json', {'command': args[:2], 'returncode': result.returncode,
             'stdout': result.stdout, 'stderr': result.stderr})
        raise RuntimeError('command_failed:' + args[0])
    return result.stdout


def inspect(service='backend'):
    return json.loads(run(['docker', 'inspect', BACKEND if service == 'backend' else FRONTEND]))[0]


def in_backend(code):
    return run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-'], code)


def config(new):
    return json.loads(run(chain(new) + ['config', '--format', 'json']))


def chain(new):
    return CHAIN + (['-f', str(STAGED / 'compose.history-proxy.json')] if new else [])


def module_hashes(new, backup=False):
    expected = dict(PAYLOAD['old_modules'])
    expected.update({f['target']: f['sha256'] if new else f.get('baseline_sha256')
                     for f in MANIFEST['files']})
    code = 'import pathlib,hashlib,json,base64\npaths=' + repr(list(expected)) + '\nout={}\n'
    code += "for p in paths:\n d=pathlib.Path(p).read_bytes() if pathlib.Path(p).exists() else None\n out[p]={'sha256':hashlib.sha256(d).hexdigest() if d is not None else None}\n"
    if backup:
        code += " if d is not None:out[p]['bytes']=base64.b64encode(d).decode()\n"
    code += 'print(json.dumps(out))\n'
    values = json.loads(in_backend(code))
    for path, sha in expected.items():
        require(values[path]['sha256'] == sha, 'module_mismatch:' + path)
        if backup and 'bytes' in values[path]:
            save('backup/modules/' + path.removeprefix('/app/'), base64.b64decode(values[path]['bytes']))
    return {p: v['sha256'] for p, v in values.items()}


def check_running(new):
    cfg = config(new)
    require(digest(canonical(cfg)) == PAYLOAD['config_hashes']['after' if new else 'before'], 'config_drift')
    image = json.loads(run(['docker', 'image', 'inspect', cfg['services']['backend']['image']]))
    current = inspect()
    require(current['Image'] == PAYLOAD['image_id'], 'image_drift')
    require(not VERIFY.compare_running('backend', current, image, cfg), 'running_config_mismatch')
    require(len(current['Mounts']) == 39, 'mount_count_mismatch')
    return current


def seal_check(backup=False):
    for name, path in SEALS.items():
        raw = Path(path).read_bytes()
        require(digest(raw) == PAYLOAD['seal_hashes'][name], 'seal_changed:' + name)
        if backup:
            save('backup/seals/' + name, raw)


def audit(name):
    code = 'import types,sys\nm=types.ModuleType("quote_guard_runtime")\n'
    code += 'exec(' + repr(PAYLOAD['runtime_tool']) + ',m.__dict__)\nsys.modules["quote_guard_runtime"]=m\n'
    code += 'sys.argv=' + repr(['audit', '--expected-entrypoint-sha256', ENTRY_HASH]) + '\n'
    code += 'exec(' + repr(PAYLOAD['audit_tool']) + ',{"__name__":"__main__"})\n'
    result = json.loads(in_backend(code))
    require(result['gate']['stop_exists'] is True, 'STOP_missing')
    require(result['gate'] == PAYLOAD['expected_gate'], 'permit_or_STOP_changed')
    require(result['historical_flags']['TUSHARE_UNIFIED_ENABLED'] == 'false', 'tushare_enabled')
    require(result['active_historical_executions'] == 0, 'tushare_history_active')
    require(result['runtime']['runtime_sha256'] == PAYLOAD['expected_runtime_sha256'], 'runtime_changed')
    prefs = {p['job_id']: p.get('enabled') for p in result['preferences']}
    require(prefs.get('tushare_historical_sync') is False, 'tushare_history_preference_enabled')
    require(prefs.get('tushare_realtime_quotes_hourly') is False, 'tushare_realtime_preference_enabled')
    save(name, result)
    return result


def db_probe(name):
    result = json.loads(run(['docker', 'exec', '-i', MONGO, 'mongosh', '--quiet', '--norc',
                             '--file', '/dev/stdin'], PAYLOAD['db_probe']))
    require(result['active_supply_executions'] == 0, 'supply_or_history_execution_active')
    prefs = {p['id']: p.get('enabled') for p in result['preferences']}
    for job in DISABLED:
        if job == 'quotes_ingestion_service' and job not in prefs:
            continue
        require(prefs.get(job) is False, 'unsafe_preference:' + job)
    save(name, result)
    return result


def check_jobs(result):
    require(result.get('success') is True, 'scheduler_query_failed')
    jobs = {j['id']: j for j in result['data']}
    for ident in JOBS:
        if ident in {'quotes_ingestion_service', 'tushare_historical_sync'} and ident not in jobs:
            continue  # independently proved disabled by sealed environment and DB preferences
        j = jobs.get(ident, {})
        require(j.get('has_running_execution') is False, 'job_active_or_missing:' + ident)
        if ident in DISABLED:
            require(j.get('paused') is True and 'next_run_time' in j and j['next_run_time'] is None,
                    'job_not_paused:' + ident)
        elif j.get('next_run_time'):
            due = datetime.fromisoformat(j['next_run_time'].replace('Z', '+00:00'))
            require(due.tzinfo is not None and (due - datetime.now(timezone.utc)).total_seconds() > 600,
                    'historical_job_due_within_switch_window')


def api_probe(name, frontend=False):
    code = PAYLOAD['api_probe']
    if frontend:
        import ssl
        mount = next(m for m in inspect('frontend')['Mounts'] if m['Destination'] == '/run/secrets/tls-cert.pem')
        ca = Path(mount['Source']).read_text()
        require(digest(ssl.PEM_cert_to_DER_cert(ca)) == CERT_HASH, 'frontend_certificate_changed')
        # Reuse login/query parsing, replacing transport with verified HTTPS through nginx.
        code = "ns={'__name__':'query_helper'}\nexec(" + repr(code) + ",ns)\n"
        code += 'import http.client,ssl,socket,json,sys,hashlib\nca=' + repr(ca) + '\n'
        code += '''
def fetch(path, token=None, body=None):
    context=ssl.create_default_context(cadata=ca)
    c=http.client.HTTPSConnection('localhost',8080,context=context,timeout=15)
    c._create_connection=lambda address,timeout,source_address: socket.create_connection(('frontend',address[1]),timeout,source_address)
    headers={'Content-Type':'application/json'}
    if token:headers['Authorization']='Bearer '+token
    try:
        c.request('POST' if body is not None else 'GET',path,body=json.dumps(body) if body is not None else None,headers=headers)
        r=c.getresponse()
        if r.status!=200:raise RuntimeError('frontend_http_status_'+str(r.status))
        raw=r.read()
        if path=='/':return {'sha256':hashlib.sha256(raw).hexdigest()}
        return json.loads(raw)
    finally:c.close()
try:
    result=ns['query'](json.load(sys.stdin),fetch=fetch)
    result['index']=fetch('/')
    print(json.dumps(result))
except Exception:
    print('FRONTEND_API_PROBE_FAILED')
    sys.exit(1)
'''
    result = json.loads(run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', code],
                            json.dumps({'auth': PAYLOAD['auth'], 'jobs': JOBS})))
    check_jobs(result)
    save(name, result)
    return result


def protected_check():
    code = 'import pathlib,hashlib,json\nexpected=' + repr(PAYLOAD['protected_files']) + '\n'
    code += "print(json.dumps({p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in expected}))\n"
    require(json.loads(in_backend(code)) == PAYLOAD['protected_files'], 'protected_permit_changed')


def ingress_probe():
    # Only after installed module hashes pass; never invoke these on the old release.
    require(in_backend("from pathlib import Path; print(Path('/app/runtime/tushare-history-batch-v1/STOP').is_file())").strip() == 'True', 'batch_stop_missing')
    code = PAYLOAD['ingress_api_probe']
    result = json.loads(run(['docker','exec','-i',BACKEND,'python3','-B','-c',code],
                            json.dumps({'auth':PAYLOAD['auth']})))
    require(result.get('passed') is True and result.get('statuses') == [400,400], 'ingress_rejection_failed')
    save('ingress-api-after.json', result)


def smoke():
    code = 'import sys\nsys.argv=["smoke","--app-root","/app"]\n'
    code += 'exec(' + repr(PAYLOAD['history_smoke']) + ',{"__name__":"__main__","__file__":"/app/deploy/history_contract_smoke.py"})\n'
    result = in_backend(code)
    require('history contract smoke OK' in result, 'history_smoke_failed')
    code = 'import types,sys\nm=types.ModuleType("quote_guard_runtime")\n'
    code += 'exec(' + repr(PAYLOAD['runtime_tool']) + ',m.__dict__)\nsys.modules["quote_guard_runtime"]=m\n'
    code += 'sys.argv=' + repr(['smoke', '--installed', '--runtime-entrypoint', '/app/deploy/migration_production_entrypoint.py', '--expected-entrypoint-sha256', ENTRY_HASH]) + '\n'
    code += 'exec(' + repr(PAYLOAD['guard_smoke']) + ',{"__name__":"__main__","__file__":"/tmp/guard-smoke.py"})\n'
    guard = in_backend(code)
    require('guard smoke OK (isolated process)' in guard, 'quote_guard_smoke_failed')
    save('installed-smoke.json', {'history': result, 'guard': guard})


def stop_backend():
    ids = run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=ta-live-088a60b962bf',
               '--filter', 'label=com.docker.compose.service=backend']).split()
    for ident in ids:
        c = json.loads(run(['docker', 'inspect', ident]))[0]
        if c['State']['Running']:
            run(['docker', 'stop', '--time', '30', ident], timeout=50)
        c = json.loads(run(['docker', 'inspect', ident]))[0]
        require(c['State']['Running'] is False and c['State']['Pid'] == 0, 'backend_not_stopped')


def up(new):
    run(chain(new) + ['up', '-d', '--no-deps', '--force-recreate', '--pull', 'never', 'backend'], timeout=150)


def healthy():
    for _ in range(36):
        c = inspect()
        require(c['State']['Running'] is True, 'backend_not_running')
        if c['State'].get('Health', {}).get('Status') == 'healthy':
            return
        time.sleep(5)
    raise RuntimeError('backend_health_timeout')


def preflight():
    global CHAIN, SEALS
    require(digest((STAGED / 'SHA256SUMS').read_bytes()) == PAYLOAD['checksums_sha256'], 'checksum_list_changed')
    names = {'SHA256SUMS'}
    for line in (STAGED / 'SHA256SUMS').read_text().splitlines():
        sha, name = line.split('  ', 1)
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe_manifest_path')
        names.add(name)
        require(not (STAGED / name).is_symlink() and digest((STAGED / name).read_bytes()) == sha, 'staged_file_changed')
    require({str(p.relative_to(STAGED)) for p in STAGED.rglob('*') if p.is_file()} == names, 'staged_files_changed')
    before = inspect()
    labels = before['Config']['Labels']
    CHAIN = ['docker', 'compose', '--project-directory', labels['com.docker.compose.project.working_dir'],
             '--project-name', labels['com.docker.compose.project'], '--profile', 'cutover-approved']
    for path in labels['com.docker.compose.project.config_files'].split(','):
        CHAIN += ['-f', path]
    require(CHAIN == PAYLOAD['expected_chain'], 'running_compose_chain_changed')
    bcfg, acfg = config(False), config(True)
    require(digest(canonical(acfg)) == PAYLOAD['config_hashes']['after'], 'candidate_config_changed')
    override = json.loads((STAGED / 'compose.history-proxy.json').read_text())
    allowed = {'backend': {v['target']: v['source'] for v in override['services']['backend']['volumes']}}
    normalized = json.loads(json.dumps(bcfg))
    target = '/app/deploy/history_batch_adapters.py'
    mount = next(v for v in normalized['services']['backend']['volumes'] if v['target'] == target)
    require(mount.get('type') == 'bind' and mount.get('read_only') is True, 'old_adapter_bind_unsafe')
    require(mount['source'] == PAYLOAD['old_adapter_source'], 'old_adapter_source_changed')
    mount['source'] = allowed['backend'].pop(target)
    require(not VERIFY.compare(normalized, acfg, allowed), 'unexpected_compose_change')
    check_running(False)
    for idx, path in enumerate(CHAIN):
        if idx and CHAIN[idx - 1] == '-f':
            save('backup/compose-sources/' + str(idx) + '.json', Path(path).read_bytes())
    for name, obj in [('backend-inspect', before), ('frontend-inspect', inspect('frontend')),
                      ('backend-compose-argv', CHAIN), ('backend-config', bcfg), ('candidate-config', acfg)]:
        save('backup/' + name + '.json', obj)
    require(before['State'].get('Health', {}).get('Status') == 'healthy', 'baseline_unhealthy')
    mounts = {m['Destination']: m['Source'] for m in before['Mounts']}
    SEALS = {name: mounts['/run/secrets/' + name] for name in PAYLOAD['seal_hashes']}
    seal_check(backup=True)
    save('backup/module-hashes.json', module_hashes(False, backup=True))
    protected_check()
    audit('audit-before.json')
    db_probe('db-before.json')
    api_probe('jobs-before.json')
    api_probe('frontend-before.json', frontend=True)
    save('backup/complete.json', {'status': 'verified', 'rollback': 'saved history release chain, no source overwrites',
                                 'files': {str(p.relative_to(ATTEMPT / 'backup')): digest(p.read_bytes())
                                           for p in (ATTEMPT / 'backup').rglob('*') if p.is_file()}})


def rollback():
    event('rollback_stopping_backend')
    stop_backend()
    seal_check()
    db_probe('db-before-rollback.json')
    require(digest(canonical(config(False))) == PAYLOAD['config_hashes']['before'], 'rollback_config_drift')
    event('rollback_recreating_history_release')
    up(False)
    healthy()
    check_running(False)
    module_hashes(False)
    protected_check()
    audit('audit-rollback.json')
    api_probe('jobs-rollback.json')
    api_probe('frontend-rollback.json', frontend=True)
    event('rolled_back_history_release')


def main():
    attempted = False
    try:
        event('preflight_and_backup')
        preflight()
        if PAYLOAD['check_only']:
            event('preflight_passed_no_switch')
            return 0
        event('stopping_backend')
        attempted = True  # even a failed/timeout stop may have stopped the service
        stop_backend()
        baseline = db_probe('db-stopped.json')
        seal_check()
        event('recreating_backend_with_resolved_collection_fix')
        up(True)
        healthy()
        event('validating_installed_release')
        check_running(True)
        save('installed-module-hashes.json', module_hashes(True))
        seal_check()
        smoke()
        protected_check()
        audit('audit-after.json')
        ingress_probe()
        api_probe('jobs-after.json')
        after_frontend = api_probe('frontend-after.json', frontend=True)
        before_frontend = json.loads((ATTEMPT / 'frontend-before.json').read_text())
        require(after_frontend['index'] == before_frontend['index'], 'frontend_index_changed')
        after = db_probe('db-after.json')
        require(after['hashes'] == baseline['hashes'], 'guarded_business_content_changed')
        require(after['preferences'] == baseline['preferences'], 'preferences_changed')
        save('completion.json', {'status': 'deployed_proxy_fix_gate_stopped',
             'source_commit': MANIFEST['source_head'], 'backend_mounts': 39,
             'business_content_unchanged': True, 'seals_unchanged': True,
             'real_upstream_acceptance': 'not_performed'})
        event('complete')
        return 0
    except BaseException as exc:
        STATE['failure_type'] = type(exc).__name__
        STATE['failure_code'] = str(exc) if isinstance(exc, RuntimeError) else 'details_withheld'
        event('failed')
        if attempted:
            try:
                rollback()
            except BaseException as rollback_error:
                STATE['rollback_failure_type'] = type(rollback_error).__name__
                STATE['rollback_failure_code'] = str(rollback_error) if isinstance(rollback_error, RuntimeError) else 'details_withheld'
                try:
                    stop_backend()
                except BaseException:
                    pass
                event('rollback_failed_stop_requested')
        return 1


if __name__ == '__main__':
    os.umask(0o077)
    PAYLOAD = json.load(sys.stdin)
    ATTEMPT = ROOT / 'attempts' / PAYLOAD['attempt']
    require(ATTEMPT.parent == ROOT / 'attempts' and ATTEMPT.is_dir(), 'attempt_invalid')
    lock = (ROOT / 'switch.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    MANIFEST = json.loads((STAGED / 'manifest.json').read_text())
    VERIFY = types.ModuleType('compose_verify')
    exec(PAYLOAD['verify_tool'], VERIFY.__dict__)
    STATE = {'stage': 'initializing', 'events': []}
    raise SystemExit(main())
