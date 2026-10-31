"""Freeze, stage and run the approved one-stock/two-call acceptance exactly once.

No token travels over SSH. The isolated backend process selects the existing
credential read-only. Never retries the sample or alters either original gate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

REPO = Path(__file__).resolve().parents[1]
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/production-20261028')
ROOT = '/home/ubuntu/k-tradingagents-history-sample-20261028-600000-v1'
SSH = ['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
       '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'osaka-migration']

DRIVER = r'''
import hashlib,json,os,pathlib,subprocess,time
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-sample-20261028-600000-v1')
os.umask(0o077)
def save(name,value):
    tmp=root/(name+'.tmp')
    with tmp.open('w') as f:json.dump(value,f,indent=2);f.flush();os.fsync(f.fileno())
    tmp.replace(root/name)
package=json.loads((root/'package.json').read_text())
code='REQUEST='+repr(package['request'])+'\nexec('+repr(package['code'])+',{"__name__":"__main__","REQUEST":REQUEST})\n'
save('status.json',{'stage':'running','started_at':time.time(),'source_commit':package['source_commit']})
try:
    result=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],
        input=code,text=True,capture_output=True,timeout=120)
    if result.stderr:(root/'stderr.private').write_text(result.stderr)
    report=json.loads(result.stdout)
    save('result.json',report)
    save('status.json',{'stage':'complete' if report.get('status')=='passed' and result.returncode==0 else 'failed',
        'finished_at':time.time(),'exit_code':result.returncode,'sample_status':report.get('status'),
        'real_attempts':report.get('real_attempts'),'reason':report.get('reason'),
        'original_gates_unchanged':report.get('original_gates_unchanged'),'source_commit':package['source_commit']})
except BaseException as error:
    # No retry. Even if the docker exec client was interrupted, bound further sends independently.
    stop="from pathlib import Path; p=Path('/app/runtime/history-acceptance-20261028-600000-v1'); (p/'STOP').touch(mode=0o600,exist_ok=True) if p.is_dir() else None"
    try:subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',stop],capture_output=True,timeout=15)
    except BaseException:pass
    save('status.json',{'stage':'failed_unknown_outcome','error_type':type(error).__name__,
                       'automatic_retry':False,'source_commit':package['source_commit']})
'''

STAGE = r'''
import hashlib,json,os,pathlib,sys
os.umask(0o077)
request=json.load(sys.stdin)
raw=pathlib.Path('/home/ubuntu/history-sample-upload-'+request['sha256']+'.json').read_bytes()
if hashlib.sha256(raw).hexdigest()!=request['sha256']:raise RuntimeError('upload_hash')
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-sample-20261028-600000-v1')
if root.exists():
    if (root/'package.json').read_bytes()!=raw:raise RuntimeError('existing_package_differs')
else:
    root.mkdir(mode=0o700)
    (root/'package.json').write_bytes(raw)
    package=json.loads(raw)
    (root/'driver.py').write_text(package['driver'])
print(json.dumps({'staged':True,'sha256':request['sha256'],'network_requests':0}))
'''

EXECUTE = r'''
import hashlib,json,os,pathlib,subprocess,sys
os.umask(0o077)
request=json.load(sys.stdin)
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-sample-20261028-600000-v1')
raw=(root/'package.json').read_bytes()
if hashlib.sha256(raw).hexdigest()!=request['sha256']:raise RuntimeError('package_changed')
package=json.loads(raw)
if (root/'driver.py').read_text()!=package['driver']:raise RuntimeError('driver_changed')
# Atomically reserve the one authorized run; never delete this marker on failure.
fd=os.open(root/'execution-reserved',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
os.fsync(fd);os.close(fd)
with (root/'process.log').open('ab') as log:
    child=subprocess.Popen(['python3','-B',str(root/'driver.py')],stdin=subprocess.DEVNULL,
                           stdout=log,stderr=log,start_new_session=True)
report={'pid':child.pid,'sha256':request['sha256'],'started':True}
(root/'process.json').write_text(json.dumps(report))
print(json.dumps(report))
'''

STATUS = r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-sample-20261028-600000-v1')
out={}
for name in ['process.json','status.json','result.json']:
    if (root/name).exists():out[name]=json.loads((root/name).read_text())
print(json.dumps(out))
'''


def ssh(code, data=None, *, timeout=60):
    try:
        result = subprocess.run(SSH + ['sudo -n python3 -B -c ' + shlex.quote(code)],
                                input=json.dumps(data) if data is not None else None,
                                text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise SystemExit('SSH_TIMEOUT: inspect status; never retry an uncertain sample launch') from None
    if result.returncode:
        raise SystemExit('SSH_FAILED: inspect status; raw errors withheld; no automatic retry')
    return json.loads(result.stdout)


def run_configuration(run):
    if run not in ('v1', 'v2'):
        raise ValueError('unapproved_run')
    suffix = '' if run == 'v1' else '-v2'
    def render(code):
        return code.replace('20261028-600000-v1', '20261028-600000-' + run)
    return {
        'artifact': EVIDENCE / ('history-sample-package' + suffix + '.json'),
        'result': EVIDENCE / ('history-sample-production-result' + suffix + '.json'),
        'driver': render(DRIVER), 'stage': render(STAGE),
        'execute': render(EXECUTE), 'status': render(STATUS),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['freeze', 'stage', 'execute', 'status', 'collect'], required=True)
    parser.add_argument('--run', choices=['v1', 'v2'], required=True)
    args = parser.parse_args()
    config = run_configuration(args.run)
    artifact = config['artifact']
    if args.action == 'freeze':
        require_clean = subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO)
        if require_clean:
            raise SystemExit('commit reviewed scripts before freezing')
        probe = json.loads((EVIDENCE / 'history-sampling-preflight.json').read_text())
        # Only old-gate bytes are read here; no provisioning or credential access.
        code = "import pathlib,hashlib,json\nout={}\n"
        protected = ['/app/runtime/tushare-egress', '/app/runtime/tushare-manual-daily-v1']
        if args.run == 'v2':
            protected.append('/app/runtime/history-acceptance-20261028-600000-v1')
        code += 'for root in ' + repr(protected) + ':\n'
        code += " p=pathlib.Path(root)\n if not (p/'STOP').is_file():raise RuntimeError('STOP_missing')\n"
        code += " for name in ['STOP','permit.json','state.json','gate.lock']:\n  f=p/name\n  if f.exists():out[str(f)]=hashlib.sha256(f.read_bytes()).hexdigest()\n"
        code += 'print(json.dumps(out))\n'
        host = "import subprocess,json\np=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],input=" + repr(code) + ",text=True,capture_output=True)\nif p.returncode:raise RuntimeError('gate_read_failed')\nprint(p.stdout)"
        gates = ssh(host)
        request = dict(execute=True, run_id=args.run, symbol='600000', start='20261021', end='20261027', budget=2,
            source_hashes=probe['source_hashes'], original_gate_hashes=gates,
            runtime_tool=(REPO / 'deploy/quote_guard_runtime.py').read_text())
        package = dict(schema=1, request=request, driver=config['driver'],
            code=(REPO / 'deploy/history_sample_acceptance.py').read_text(),
            source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip())
        fd = os.open(artifact, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:json.dump(package, stream, sort_keys=True)
        print(json.dumps({'frozen':str(artifact),'sha256':hashlib.sha256(artifact.read_bytes()).hexdigest(), 'upstream_requests':0}))
        return
    if args.action in ['status', 'collect']:
        result = ssh(config['status'])
        if args.action == 'collect':
            target = config['result']
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as stream:json.dump(result, stream, indent=2)
        print(json.dumps({k:v for k,v in result.items() if k != 'result.json'}, ensure_ascii=False))
        return
    sha = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if args.action == 'stage':
        copied = subprocess.run(['scp', '-F', SSH[2], '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            str(artifact), 'osaka-migration:/home/ubuntu/history-sample-upload-' + sha + '.json'],
            capture_output=True, timeout=60)
        if copied.returncode:raise SystemExit('SCP_FAILED: no sample launched')
        print(json.dumps(ssh(config['stage'], {'sha256':sha})))
    else:
        print(json.dumps(ssh(config['execute'], {'sha256':sha})))


if __name__ == '__main__':
    main()
