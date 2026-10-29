"""Stage/check/execute the frozen three-file ingress release; never releases STOP or permits.

SSH receives credentials only via stdin. Status output excludes raw command errors.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

REPO = Path(__file__).resolve().parents[1]
REMOTE_ROOT = '/home/ubuntu/k-tradingagents-history-button-v1-20261029'
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/production-20261028')
RELEASE = Path('/home/ubuntu/quote-guard-releases/history-button-v1-20261029')
ARCHIVE_SHA = '9577c7416b172be84b53a05c0a9adcf73a62e9fdbe54e22ce3338ee19205f6d2'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()


STAGE = r'''
import base64,hashlib,io,json,os,pathlib,sys,tarfile
os.umask(0o077)
p=json.load(sys.stdin)
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-v1-20261029')
raw=pathlib.Path('/home/ubuntu/history-button-9577c7416b17.tgz').read_bytes()
if hashlib.sha256(raw).hexdigest()!=p['archive_sha256']:raise RuntimeError('archive_hash')
root.mkdir(mode=0o700,exist_ok=True)
stage=root/'staged'
if stage.exists():
    if (root/'release.tgz').read_bytes()!=raw:raise RuntimeError('existing_stage_differs')
else:
    tmp=root/'staging-incomplete'
    tmp.mkdir(mode=0o700)
    with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as tar:
        seen=set()
        for item in tar.getmembers():
            rel=pathlib.PurePosixPath(item.name)
            if not item.isfile() or rel.is_absolute() or '..' in rel.parts or item.name in seen:raise RuntimeError('unsafe_archive')
            seen.add(item.name)
            target=tmp/item.name
            target.parent.mkdir(parents=True,exist_ok=True)
            with target.open('xb') as f:f.write(tar.extractfile(item).read())
            target.chmod(0o644)
    expected={'SHA256SUMS'}
    for line in (tmp/'SHA256SUMS').read_text().splitlines():
        sha,name=line.split('  ',1)
        if name not in seen or hashlib.sha256((tmp/name).read_bytes()).hexdigest()!=sha:raise RuntimeError('checksum_failed')
        expected.add(name)
    if seen!=expected:raise RuntimeError('file_set_failed')
    (root/'release.tgz').write_bytes(raw)
    tmp.rename(stage)
print(json.dumps({'staged':True,'archive_sha256':p['archive_sha256'],'service_changed':False}))
'''

LAUNCH = r'''
import datetime,hashlib,json,os,pathlib,subprocess,sys,uuid
os.umask(0o077)
request=json.load(sys.stdin)
uploaded=pathlib.Path('/home/ubuntu')/('history-button-switch-upload-'+request['upload_sha256']+'.json')
raw=uploaded.read_bytes()
if hashlib.sha256(raw).hexdigest()!=request['upload_sha256']:raise RuntimeError('upload_hash')
p=json.loads(raw)
p['payload']['auth']=request['auth']
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-v1-20261029')
code=p['code'].encode()
sha=hashlib.sha256(code).hexdigest()
if sha!=p['code_sha256']:raise RuntimeError('controller_hash')
script=root/('switch-'+sha+'.py')
if script.exists():
    if script.read_bytes()!=code:raise RuntimeError('controller_changed')
else:
    with script.open('xb') as f:f.write(code)
attempt=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
directory=root/'attempts'/attempt
directory.mkdir(parents=True,mode=0o700)
p['payload']['attempt']=attempt
if not p['payload']['check_only']:
    # One authorized switch; an ambiguous SSH result must be inspected, never relaunched.
    fd=os.open(root/'execution-reserved',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    os.fsync(fd);os.close(fd)
with (directory/'process.log').open('ab') as log:
    child=subprocess.Popen(['python3','-B',str(script)],stdin=subprocess.PIPE,stdout=log,stderr=log,start_new_session=True)
    child.stdin.write(json.dumps(p['payload']).encode())
    child.stdin.close()
report={'attempt':attempt,'pid':child.pid,'check_only':p['payload']['check_only'],'controller_sha256':sha}
(directory/'process.json').write_text(json.dumps(report))
(root/'latest-attempt.json').write_text(json.dumps(report))
print(json.dumps(report))
'''

STATUS = r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-v1-20261029')
process=json.loads((root/'latest-attempt.json').read_text())
directory=root/'attempts'/process['attempt']
state=json.loads((directory/'state.json').read_text()) if (directory/'state.json').exists() else {'stage':'starting'}
out={'process':process,'state':state,'process_exists':(pathlib.Path('/proc')/str(process['pid'])).exists()}
if (directory/'completion.json').exists():out['completion']=json.loads((directory/'completion.json').read_text())
print(json.dumps(out))
'''


def payload(check_only, credentials):
    spec = importlib.util.spec_from_file_location('auth_helper', REPO / 'deploy/query_quote_guard_jobs.py')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    baseline = read_json(EVIDENCE / 'baseline-hashes.json')
    before = read_json(EVIDENCE / 'backend-v12-config.json')
    v12 = read_json(Path('/home/ubuntu/quote-guard-releases/v12-20261028/manifest.json'))
    probe = (EVIDENCE / 'db_guard_probe.js').read_text()
    probe = probe.replace("'tushare_realtime_quotes_hourly'];", "'tushare_realtime_quotes_hourly','tushare_historical_sync','akshare_historical_sync'];")
    probe = probe.replace("'paper_market_rules']", "'paper_market_rules','stock_daily_quotes']")
    probe = probe.replace('const count = col.countDocuments({});', "const query = name === 'stock_daily_quotes' ? {data_source:'tushare'} : {};\n    const count = col.countDocuments(query);")
    probe = probe.replace('col.find({}).sort', 'col.find(query).sort')
    result = {
        'auth': helper.read_auth(credentials, credentials=True), 'api_probe': helper.REMOTE,
        'check_only': check_only,
        'checksums_sha256': digest((RELEASE / 'SHA256SUMS').read_bytes()),
        'seal_hashes': baseline['seals'],
        'expected_gate': read_json(EVIDENCE / 'history-audit-before.json')['gate'],
        'image_id': read_json(EVIDENCE / 'backend-inspect.json')[0]['Image'],
        'expected_chain': read_json(EVIDENCE / 'backend-v12-compose_argv.json'),
        'config_hashes': {'before': digest(canonical(before)), 'after': read_json(EVIDENCE / 'history-compose-preflight.json')['new_config_sha256']},
        'old_modules': {f['target']: f['sha256'] for f in v12['files'] if f['mode'] != 'tool'},
        'runtime_tool': (REPO / 'deploy/quote_guard_runtime.py').read_text(),
        'guard_smoke': (REPO / 'deploy/quote_guard_smoke.py').read_text(),
        'verify_tool': (REPO / 'deploy/verify_compose_merge.py').read_text(), 'db_probe': probe,
    }


    fresh = read_json(EVIDENCE / 'history-button-preflight.json')
    previous = read_json(EVIDENCE / 'history-ingress-production-20261028T165211Z-50f64b00.json')
    result['old_modules'] = previous['evidence']['installed-module-hashes.json']
    result.update(expected_chain=fresh['chain'], config_hashes=fresh['config_hashes'], image_id=fresh['image_id'],
        old_scheduler_source=fresh['old_scheduler_source'],
        expected_runtime_sha256=read_json(EVIDENCE / 'history-audit-before.json')['runtime']['runtime_sha256'],
        protected_files=read_json(EVIDENCE / 'history-write-package-v1.json')['request']['protected_hashes'],
        audit_tool=(REPO/'deploy/audit_tushare_history.py').read_text(),
        history_smoke=(REPO/'deploy/history_contract_smoke.py').read_text(),
        ingress_api_probe=(REPO/'deploy/history_button_closed_probe.py').read_text())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['stage', 'check', 'execute', 'status', 'collect'], required=True)
    parser.add_argument('--credentials-file', type=Path)
    args = parser.parse_args()
    data = None
    if args.action == 'stage':
        raw = RELEASE.with_suffix('.tgz').read_bytes()
        if digest(raw) != ARCHIVE_SHA:
            raise SystemExit('local_archive_changed')
        upload(RELEASE.with_suffix('.tgz'), '/home/ubuntu/history-button-9577c7416b17.tgz')
        code, data = STAGE, {'archive_sha256': ARCHIVE_SHA}
    elif args.action in ['check', 'execute']:
        if args.credentials_file is None:
            parser.error('credentials file required')
        controller = (REPO / 'deploy/switch_history_button.py').read_text()
        request = {'code': controller, 'code_sha256': digest(controller.encode()),
                   'payload': payload(args.action == 'check', args.credentials_file)}
        auth = request['payload'].pop('auth')
        raw = json.dumps(request).encode()
        sha = digest(raw)
        # Large non-secret artifacts use SCP; credentials stay in the small SSH stdin message.
        with tempfile.NamedTemporaryFile(prefix='history-button-switch-', suffix='.json') as stream:
            stream.write(raw)
            stream.flush()
            upload(Path(stream.name), '/home/ubuntu/history-button-switch-upload-' + sha + '.json')
        code, data = LAUNCH, {'upload_sha256': sha, 'auth': auth}
    else:
        code = STATUS
        if args.action == 'collect':
            code = code.replace('print(json.dumps(out))', '''
names=['process.json','state.json','completion.json','backup/complete.json',
       'audit-before.json','audit-after.json','db-before.json','db-stopped.json','db-after.json',
       'jobs-before.json','jobs-after.json','frontend-before.json','frontend-after.json',
       'installed-module-hashes.json','installed-smoke.json','ingress-api-after.json','audit-rollback.json',
       'jobs-rollback.json','frontend-rollback.json']
out['evidence']={name:json.loads((directory/name).read_text()) for name in names if (directory/name).exists()}
import subprocess
current=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1','ta-live-088a60b962bf-frontend-1']))
out['current_services']=[{'name':c['Name'],'id':c['Id'],'image':c['Image'],'running':c['State']['Running'],
    'health':c['State'].get('Health',{}).get('Status'),'started_at':c['State']['StartedAt'],
    'mount_count':len(c['Mounts'])} for c in current]
print(json.dumps(out))
''')
    remote = 'sudo -n python3 -B -c ' + shlex.quote(code)
    try:
        result = subprocess.run(['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
                                 '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'osaka-migration', remote],
                                input=json.dumps(data) if data is not None else None,
                                text=True, capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        print('REMOTE_CALL_TIMED_OUT: inspect status before retrying; credentials withheld')
        return 1
    if result.returncode:
        fd = os.open(EVIDENCE / 'history-ingress-remote-error.json', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump({'returncode': result.returncode, 'stderr': result.stderr}, stream)
        print('REMOTE_CALL_FAILED: exit=' + str(result.returncode) + '; inspect status before retrying; raw errors withheld')
        return 1
    report = json.loads(result.stdout)
    if args.action == 'collect':
        target = EVIDENCE / ('history-button-production-' + report['process']['attempt'] + '.json')
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(report, stream, indent=2)
        print(json.dumps({'evidence_file': str(target), 'stage': report['state']['stage'],
                          'current_services': report['current_services']}, ensure_ascii=False))
        return 0
    print(json.dumps(report, ensure_ascii=False))
    if args.action in ['check', 'execute']:
        target = EVIDENCE / ('history-button-switch-' + report['attempt'] + '.json')
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(report, stream, indent=2)
    return 0


def upload(local, remote):
    try:
        result = subprocess.run(['scp', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
                                 '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', str(local),
                                 'osaka-migration:' + remote], capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        raise SystemExit('ARTIFACT_UPLOAD_TIMED_OUT: no service action requested') from None
    if result.returncode:
        fd = os.open(EVIDENCE / 'history-ingress-artifact-upload-error.json', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump({'returncode': result.returncode, 'stderr': result.stderr.decode(errors='replace')}, stream)
        raise SystemExit('ARTIFACT_UPLOAD_FAILED: no service action requested')


if __name__ == '__main__':
    raise SystemExit(main())
