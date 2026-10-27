"""One-off v12 guarded switch. Auth arrives only on stdin; no secret logging."""
import hashlib
import importlib.util
import inspect as source_inspect
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path('/home/ubuntu/k-tradingagents-paper-guard-v12')
STAGED = ROOT / 'staged'
BACKUP = Path('/home/ubuntu/k-tradingagents-paper-guard-v11/backup')
BACKEND = 'ta-live-088a60b962bf-backend-1'
FRONTEND = 'ta-live-088a60b962bf-frontend-1'
MONGO = 'ta-live-088a60b962bf-mongodb-1'
JOBS = ['akshare_quotes_sync','quotes_ingestion_service','etf_sync','tushare_realtime_quotes_hourly']
STATE = {'stage': 'initializing', 'events': []}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def private_json(name, value):
    path = ROOT / name
    temp = ROOT / (name + '.tmp')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)


def event(stage):
    STATE['stage'] = stage
    STATE['events'].append({'stage': stage, 'utc': datetime.now(timezone.utc).isoformat()})
    private_json('switch-state.json', STATE)
    print(stage, flush=True)


def run(args, data=None, timeout=60):
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        private_json('last-command-failure.json', {'executable':args[0], 'exit_code':result.returncode,
                     'stdout':result.stdout, 'stderr':result.stderr})
        # Raw errors stay in the restricted production directory, never in the displayed summary.
        raise RuntimeError('command_failed:' + args[0] + ':' + str(result.returncode))
    return result.stdout


def inspect(service):
    return json.loads(run(['docker', 'inspect', BACKEND if service == 'backend' else FRONTEND]))[0]


def in_backend(code):
    return run(['docker','exec','-i',BACKEND,'python3','-B','-'], code)


def db_probe():
    report = json.loads(run(['docker','exec','-i',MONGO,'mongosh','--quiet','--norc','--file','/dev/stdin'], PAYLOAD['db_probe']))
    require(report['active_supply_executions'] == 0, 'supply_execution_active')
    prefs = {p['id']: p.get('enabled') for p in report['preferences']}
    for ident in JOBS:
        if ident == 'quotes_ingestion_service' and ident not in prefs:
            continue
        require(prefs.get(ident) is False, 'unsafe_supply_preference:' + ident)
    return report


def api_probe(name):
    raw = run(['docker','exec','-i',BACKEND,'python3','-B','-c',PAYLOAD['api_probe']],
              json.dumps({'auth': PAYLOAD['auth'], 'jobs': JOBS}))
    result = json.loads(raw)
    require(result.get('success') is True, 'jobs_api_failed')
    jobs = {j['id']: j for j in result['data']}
    for ident in JOBS:
        if ident == 'quotes_ingestion_service' and ident not in jobs:
            continue
        j = jobs.get(ident, {})
        require(j.get('paused') is True and j.get('has_running_execution') is False
                and 'next_run_time' in j and j['next_run_time'] is None, 'jobs_not_paused:' + ident)
    private_json(name, result)
    return result


def config(service, new):
    args = CHAINS[service] + (['-f', str(ROOT / ('compose.' + service + '.json'))] if new else [])
    return json.loads(run(args + ['config','--format','json']))


def check_running(service, cfg):
    c = inspect(service)
    image = json.loads(run(['docker','image','inspect',cfg['services'][service]['image']]))
    require(c['Image'] == PAYLOAD['image_ids'][service], 'image_changed:' + service)
    require(not VERIFY.compare_running(service, c, image, cfg), 'running_config_mismatch:' + service)
    return c


def seal_probe():
    code = 'import hashlib,json,pathlib\npaths=' + repr(PAYLOAD['seal_paths']) + '\n'
    code += "print(json.dumps({k:hashlib.sha256(pathlib.Path(v).read_bytes()).hexdigest() for k,v in paths.items()}))\n"
    return json.loads(in_backend(code))


def runtime_probe():
    code = "ns={'__name__':'guard_runtime'}\nexec(" + repr((STAGED/'deploy/quote_guard_runtime.py').read_text()) + ",ns)\n"
    code += "env,report=ns['reconstruct']('/app/deploy/migration_production_entrypoint.py'," + repr(MANIFEST['entrypoint_sha256']) + ",'/run/secrets')\n"
    code += "import json; print(json.dumps(report))\n"
    result = json.loads(in_backend(code))
    require(result['runtime_sha256'] == PAYLOAD['seal_hashes']['runtime-config.json'], 'runtime_seal_changed')
    return result


def verify_module_hashes(new):
    code = 'import pathlib,hashlib,json\npaths=' + repr([i['target'] for i in MODULES]) + '\n'
    code += "print(json.dumps({p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() if pathlib.Path(p).exists() else None for p in paths}))\n"
    hashes = json.loads(in_backend(code))
    for item in MODULES:
        expected = item['sha256'] if new else item.get('baseline_sha256')
        require(hashes[item['target']] == expected, 'module_hash_mismatch:' + item['path'])
    return hashes


def overwrite(path, data, original):
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    with os.fdopen(fd, 'r+b') as stream:
        info = os.fstat(stream.fileno())
        require((info.st_ino, info.st_dev) == (original['inode'], original['device']), 'source_inode_changed')
        stream.write(data)
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())
    require(digest(Path(path).read_bytes()) == digest(data), 'inplace_write_failed')
    require(Path(path).stat().st_ino == original['inode'], 'inplace_inode_changed')


def stop_backend(allow_absent=False):
    if allow_absent:
        ids = run(['docker','ps','-aq','--filter','label=com.docker.compose.project=ta-live-088a60b962bf',
                   '--filter','label=com.docker.compose.service=backend']).split()
        for ident in ids:
            c = json.loads(run(['docker','inspect',ident]))[0]
            if c['State']['Running']:
                run(['docker','stop','--time','30',ident], timeout=50)
            c = json.loads(run(['docker','inspect',ident]))[0]
            require(c['State']['Running'] is False and c['State']['Pid'] == 0, 'backend_did_not_stop')
        return
    run(['docker','stop','--time','30',BACKEND], timeout=50)
    c = inspect('backend')
    require(c['State']['Running'] is False and c['State']['Pid'] == 0, 'backend_did_not_stop')


def up(service, new):
    args = CHAINS[service] + (['-f', str(ROOT / ('compose.'+service+'.json'))] if new else [])
    run(args + ['up','-d','--no-deps','--force-recreate','--pull','never',service], timeout=150)


def healthy():
    for _ in range(36):
        c = inspect('backend')
        require(c['State']['Running'] is True, 'backend_not_running')
        if c['State'].get('Health', {}).get('Status') == 'healthy':
            return
        time.sleep(5)
    raise RuntimeError('backend_health_timeout')


def smoke():
    code = 'import types,sys\nm=types.ModuleType("quote_guard_runtime")\n'
    code += 'exec(' + repr((STAGED/'deploy/quote_guard_runtime.py').read_text()) + ',m.__dict__)\n'
    code += 'sys.modules["quote_guard_runtime"]=m\n'
    code += 'sys.argv=' + repr(['smoke','--installed','--runtime-entrypoint','/app/deploy/migration_production_entrypoint.py',
                               '--expected-entrypoint-sha256',MANIFEST['entrypoint_sha256']]) + '\n'
    code += 'exec(' + repr((STAGED/'deploy/quote_guard_smoke.py').read_text()) + ',{"__name__":"__main__","__file__":"/tmp/guard-smoke.py"})\n'
    result = in_backend(code)
    require('guard smoke OK (isolated process)' in result, 'installed_smoke_failed')
    private_json('installed-smoke.json', {'result': result})


def verify_frontend_resources(expected, timeout=45):
    """Wait for the listener, but never retry HTTP errors or mismatched bytes."""
    import hashlib
    import time
    import urllib.error
    import urllib.request

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + timeout
    attempts = 0
    while True:
        attempts += 1
        try:
            for path, expected_hash in expected.items():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError('frontend_http_ready_timeout')
                with opener.open('http://frontend/' + path, timeout=min(5, remaining)) as response:
                    data = response.read()
                if hashlib.sha256(data).hexdigest() != expected_hash:
                    raise RuntimeError('frontend_http_hash_mismatch')
            return {'verified_http_resources': len(expected), 'attempts': attempts}
        except urllib.error.HTTPError:
            raise RuntimeError('frontend_http_status_failed') from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError('frontend_http_ready_timeout') from None
            time.sleep(min(1, remaining))


def frontend_http_probe():
    index = (STAGED/'frontend-dist/index.html').read_text()
    paths = ['index.html'] + re.findall(r'(?:src|href)=["\'](/(?:js|css|assets)/[^"\']+)["\']', index)
    checks = {}
    for value in paths:
        path = value.lstrip('/')
        require('..' not in Path(path).parts, 'unsafe_asset_path')
        checks[path] = digest((STAGED/'frontend-dist'/path).read_bytes())
    code = source_inspect.getsource(verify_frontend_resources)
    code += '\nimport json\nprint(json.dumps(verify_frontend_resources(' + repr(checks) + ')))\n'
    return json.loads(in_backend(code))


def preflight():
    require(digest((STAGED/'SHA256SUMS').read_bytes()) == PAYLOAD['checksums_sha256'], 'manifest_list_changed')
    expected_files = {'SHA256SUMS'}
    for line in (STAGED/'SHA256SUMS').read_text().splitlines():
        sha, name = line.split('  ',1)
        expected_files.add(name)
        require(digest((STAGED/name).read_bytes()) == sha, 'staged_file_changed:' + name)
    require({str(p.relative_to(STAGED)) for p in STAGED.rglob('*') if p.is_file()} == expected_files, 'staged_file_set_changed')
    for entry in BACKUP_INFO['files']:
        require(digest((BACKUP/entry['backup']).read_bytes()) == entry['sha256'], 'backup_changed')
        require(digest(Path(entry['source']).read_bytes()) == entry['sha256'], 'source_changed_since_backup')
        require(Path(entry['source']).stat().st_ino == entry['inode'], 'source_inode_changed_since_backup')
    require(digest((BACKUP/'frontend-before.tar').read_bytes()) == BACKUP_INFO['frontend_archive_sha256'], 'frontend_backup_changed')
    for service in ['backend','frontend']:
        before = config(service, False)
        after = config(service, True)
        require(digest(canonical(before)) == PAYLOAD['config_hashes'][service]['before'], 'before_config_changed:' + service)
        require(digest(canonical(after)) == PAYLOAD['config_hashes'][service]['after'], 'candidate_config_changed:' + service)
        allowed = {service: {v['target']:v['source'] for v in FULL_OVERRIDE['services'][service]['volumes']}}
        require(not VERIFY.compare(before,after,allowed), 'compose_diff_failed:' + service)
        check_running(service,before)
    verify_module_hashes(False)
    require(seal_probe() == PAYLOAD['seal_hashes'], 'seal_changed')
    private_json('runtime-before-switch.json', runtime_probe())
    private_json('db-before-stop.json', db_probe())
    api_probe('jobs-immediate-before.json')


def rollback():
    event('rollback_stopping_backend')
    stop_backend(allow_absent=True)
    # The sealed files are host bind sources and remain readable with backend stopped.
    for name, source in PAYLOAD['seal_host_paths'].items():
        require(digest(Path(source).read_bytes()) == PAYLOAD['seal_hashes'][name], 'rollback_seal_changed')
    db_probe()  # fail closed if saved preferences may restore supply
    event('rollback_restoring_sources')
    for item in REPLACEMENTS:
        original = ORIGINALS[item['host_source']]
        data = (BACKUP/original['backup']).read_bytes()
        require(digest(data) == item['baseline_sha256'], 'rollback_backup_invalid')
        overwrite(item['host_source'], data, original)
    up('backend', False)
    healthy()
    verify_module_hashes(False)
    api_probe('jobs-after-rollback.json')
    check_running('backend', config('backend', False))
    if STATE.get('frontend_attempted'):
        up('frontend', False)
        check_running('frontend', config('frontend', False))
    event('rolled_back')


def main():
    stopped = False
    try:
        event('preflight')
        preflight()
        if PAYLOAD.get('check_only'):
            event('preflight_passed_no_switch')
            return 0
        event('stopping_backend')
        # Mark intent before stop so an error after Docker stops is also recovered.
        stopped = True
        stop_backend()
        event('backend_stopped')
        baseline = db_probe()
        private_json('db-stopped-baseline.json', baseline)
        event('replacing_three_sources')
        for item in REPLACEMENTS:
            original = ORIGINALS[item['host_source']]
            require(digest(Path(item['host_source']).read_bytes()) == item['baseline_sha256'], 'baseline_changed_after_stop')
            overwrite(item['host_source'], (STAGED/item['path']).read_bytes(), original)
        event('recreating_backend')
        up('backend', True)
        healthy()
        event('validating_backend')
        check_running('backend', config('backend', True))
        private_json('runtime-module-hashes.json', verify_module_hashes(True))
        require(seal_probe() == PAYLOAD['seal_hashes'], 'seal_changed_after_switch')
        smoke()
        api_probe('jobs-after-backend.json')
        event('recreating_frontend')
        STATE['frontend_attempted'] = True
        up('frontend', True)
        require(inspect('frontend')['State']['Running'] is True, 'frontend_not_running')
        check_running('frontend', config('frontend', True))
        event('validating_frontend_and_data')
        private_json('frontend-http.json', frontend_http_probe())
        after = db_probe()
        private_json('db-after-switch.json', after)
        require(after['hashes'] == baseline['hashes'], 'business_records_changed_during_switch')
        private_json('completion.json', {'backend_mounts':len(inspect('backend')['Mounts']),
          'frontend_mounts':len(inspect('frontend')['Mounts']), 'business_hashes_unchanged':True,
          'seal_hashes_unchanged':True, 'source_commit':MANIFEST['source_head'],
          'status':'deployed_guarded_writes_disabled'})
        event('complete')
        return 0
    except BaseException as exc:
        STATE['failure_category'] = type(exc).__name__
        STATE['failure_code'] = str(exc) if isinstance(exc,RuntimeError) else 'unexpected_error'
        event('failed')
        if stopped:
            try:
                rollback()
            except BaseException as rollback_error:
                STATE['rollback_failure_category'] = type(rollback_error).__name__
                STATE['rollback_failure_code'] = str(rollback_error) if isinstance(rollback_error,RuntimeError) else 'unexpected_error'
                # Attempt containment if recovery itself fails. Do not restart again.
                try: stop_backend(allow_absent=True)
                except BaseException: pass
                event('rollback_failed_backend_stop_requested')
        return 1


if __name__ == '__main__':
    PAYLOAD = json.load(sys.stdin)
    MANIFEST = json.loads((STAGED/'manifest.json').read_text())
    BACKUP_INFO = json.loads((BACKUP/'complete.json').read_text())
    ORIGINALS = {i['source']:i for i in BACKUP_INFO['files']}
    CHAINS = {s:json.loads((BACKUP/(s+'-compose-argv.json')).read_text()) for s in ['backend','frontend']}
    MODULES = [i for i in MANIFEST['files'] if i['mode'] != 'tool']
    REPLACEMENTS = [i for i in MODULES if i['mode'] == 'replace']
    FULL_OVERRIDE = json.loads((STAGED/'compose.paper-guard.json').read_text())
    spec = importlib.util.spec_from_file_location('merge_verify',STAGED/'deploy/verify_compose_merge.py')
    VERIFY = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(VERIFY)
    raise SystemExit(main())
