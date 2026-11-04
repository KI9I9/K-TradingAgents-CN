"""One explicit continuation after HTTP status diagnostics; no deployment or new grant.

check is detached/read-only (including TLS-only probes); freeze binds its exact
data/state fingerprints; execute backs up, reconciles again, then triggers the
same plan once. After uncertain launch, use status; never repeat execute.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import uuid
import zlib

from query_quote_guard_jobs import read_auth

REPO=Path(__file__).resolve().parents[1]
ROOT='/home/ubuntu/k-tradingagents-history-http-status-recovery-20261105-v1'
RELEASE=Path('/home/ubuntu/quote-guard-releases/history-http-status-recovery-v1-20261105')


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def require(value,reason):
    if not value:raise RuntimeError(reason)


DRIVER=r'''
import hashlib,json,os,pathlib,subprocess,sys,time
os.umask(0o077);r=json.load(sys.stdin);directory=pathlib.Path(r['directory'])
package=json.loads((directory/'package.json').read_text())
def save(name,value):
 p=directory/name;tmp=p.with_suffix('.tmp')
 with tmp.open('w') as f:json.dump(value,f);f.flush();os.fsync(f.fileno())
 tmp.replace(p)
save('status.json',dict(stage='checking' if r['check_only'] else 'backup_reconcile_submit',at=time.time()))
verified=False
try:
 i=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],timeout=15))[0]
 assert i['State']['Running'] and i['State'].get('Health',{}).get('Status')=='healthy' and len(i['Mounts'])==45,'backend_not_healthy'
 assert i['Image']==package['image'],'image_changed'
 if not r['check_only']:assert i['Id']==package['backend_id'],'backend_changed'
 payload=dict(package['request'],auth=r.get('auth'),check_only=r['check_only'])
 code="import json,sys\nr=json.load(sys.stdin)\nc={'__name__':'recovery_core'};exec(r.pop('core'),c)\nh={'__name__':'recovery_helper'};exec(r.pop('helper'),h)\nreadonly=r.pop('check_only')\nraise SystemExit(c['main'](r,recoverer=h['recover_raw_http_error'],expected_reason='http_error',raw_count=107128,run_directory='/app/runtime/history-http-status-recovery-20261105-v1',scheduled_recovery=True,check_only=readonly))"
 payload.update(core=package['core'],helper=package['helper'])
 p=subprocess.run(['docker','exec','-i',i['Id'],'python3','-B','-c',code],input=json.dumps(payload),text=True,capture_output=True,timeout=1800)
 (directory/'stderr.private').write_text(p.stderr)
 rows=[s.split(':',1)[1] for s in p.stdout.splitlines() if s.startswith('BUTTON_ACCEPTANCE_JSON:')]
 assert len(rows)==1,'recovery_result_unproven'
 report=json.loads(rows[0]);report['backend_id']=i['Id'];save('result.json',report)
 verified=p.returncode==0 and report['status']==('checked' if r['check_only'] else 'passed')
 save('status.json',dict(stage='complete' if verified else 'failed',at=time.time(),reason=report.get('reason')))
except BaseException as error:
 save('status.json',dict(stage='unknown_outcome_inspect_only',error_type=type(error).__name__,at=time.time()))
finally:
 if not r['check_only'] and not verified:
  code="from pathlib import Path;from tradingagents.utils.tushare_batch import BatchGate;p=Path('/app/runtime/tushare-history-generations/20aa2215a7904e0d8d6601b14b8a6c90');BatchGate(p).stop();print((p/'STOP').is_file())"
  try:
   p=subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',code],capture_output=True,text=True,timeout=20)
   save('stop-confirmation.json',dict(stopped=p.returncode==0 and p.stdout.splitlines()[-1:]==['True']))
  except BaseException:save('stop-confirmation.json',dict(stopped=False))
'''

LAUNCH=r'''
import hashlib,json,os,pathlib,subprocess,sys,zlib
os.umask(0o077);r=json.loads(zlib.decompress(sys.stdin.buffer.read()))
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-http-status-recovery-20261105-v1')
root.mkdir(mode=0o700,exist_ok=True)
raw=r['package'].encode();assert hashlib.sha256(raw).hexdigest()==r['sha256'],'package_changed'
package=json.loads(raw)
if r['check_only']:
 directory=root/'checks'/r['attempt'];directory.mkdir(mode=0o700,parents=True)
else:
 fd=os.open(root/'execution-reserved',os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600);os.fsync(fd);os.close(fd)
 directory=root/'execute';directory.mkdir(mode=0o700)
(directory/'package.json').write_bytes(raw)
with (directory/'process.log').open('ab') as log:
 child=subprocess.Popen(['python3','-B','-c',package['driver']],stdin=subprocess.PIPE,stdout=log,stderr=log,start_new_session=True)
 child.stdin.write(json.dumps(dict(directory=str(directory),check_only=r['check_only'],auth=r.get('auth'))).encode());child.stdin.close()
process=dict(pid=child.pid,attempt=r['attempt'],check_only=r['check_only'],sha256=r['sha256'])
(directory/'process.json').write_text(json.dumps(process))
if r['check_only']:(root/'latest-check.json').write_text(json.dumps(dict(directory=str(directory))))
print(json.dumps(process))
'''

STATUS=r'''
import json,os,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-http-status-recovery-20261105-v1')
out=dict(execution_reserved=(root/'execution-reserved').exists())
for label,directory in [('execute',root/'execute'),('check',pathlib.Path(json.loads((root/'latest-check.json').read_text())['directory']) if (root/'latest-check.json').exists() else root/'no-check')]:
 if directory.exists():
  row={name:json.loads((directory/name).read_text()) for name in ('process.json','status.json','result.json','stop-confirmation.json') if (directory/name).exists()}
  if 'process.json' in row:
   try:os.kill(row['process.json']['pid'],0);row['process_exists']=True
   except ProcessLookupError:row['process_exists']=False
  out[label]=row
print(json.dumps(out))
'''


def package():
    source_hashes = {
        "/app/app/worker/tushare_sync_service.py": "60f84fbb29f3fd57012be496e29a0d860a7beed2269187cbb054288a55761d3d",
        "/app/app/services/tushare_history_batches.py": "6ef0962a6e01e5e2ea4a71e1d981990206ec32920b80196357fe0dc3fa766c21",
        "/app/app/services/scheduler_runtime.py": "7a0981c3878adfe562210e131625b4f777f1534f3729b3aa18f6d5010e37e3b3",
        "/app/tradingagents/utils/tushare_manual.py": "0ef56f2de5f1acc6b9aaa6950c63db1eb679349b232b45971bd7768b6d42e234",
        "/app/tradingagents/utils/tushare_egress.py": "91bf30024cb4c95c060859b4e13169b2f86bf87a4eb75387bbe11f9ce5a30877",
    }
    return dict(driver=DRIVER,image="sha256:29ad3c39036f0f2cd19e433c9153147ef19ae82ed908c9c1c8a8900641abc277",
        core=(REPO/'deploy/history_timeout_recovery.py').read_text(),
        helper=(REPO/'deploy/history_tls_recovery.py').read_text(),
        request=dict(source_hashes=source_hashes,
            runtime_tool=(REPO/'deploy/quote_guard_runtime.py').read_text(),
            generation='/app/runtime/tushare-history-generations/20aa2215a7904e0d8d6601b14b8a6c90',
            used=11881,plan='3efcbb8f7256e5a00a5db5b26e80b12e847331a36377d04be76b63d10daa9081',
            execution='6aeb092a18ec167ff2d87f70',active=dict(code='002180.SZ',end='2026-11-04',
                id='efcff5b358bc45d582007b82cce073d8',sent=['daily'],start='2026-11-02')))


def remote(code,data=None,mss=False,control_socket=None):
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S',str(control_socket) if control_socket else 'none',
         '-o','BatchMode=yes','-o','ConnectTimeout=20','-o','ConnectionAttempts=1','-o','IPQoS=none',
         '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2']
    if mss:cmd+=['-o','ProxyCommand=python3 -B /home/ubuntu/quote-guard-releases/paper-supply-research-20261102/ssh_mss_probe.py']
    cmd+=['osaka-migration','sudo -n python3 -B -c '+shlex.quote(code)]
    path=RELEASE/('receipt-'+str(time.time_ns()))
    try:p=subprocess.run(cmd,input=zlib.compress(canonical(data)) if data is not None else None,capture_output=True,timeout=55)
    except subprocess.TimeoutExpired:raise SystemExit('management_timeout_status_only') from None
    if p.returncode:
        path.with_suffix('.failure.private').write_text(json.dumps(dict(returncode=p.returncode,stderr=p.stderr.decode(errors='replace'))))
        raise SystemExit('management_failed_status_only')
    value=json.loads(p.stdout);path.with_suffix('.json').write_text(json.dumps(value,indent=2));return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['check','freeze','execute','status'],required=True)
    parser.add_argument('--check-result',type=Path)
    parser.add_argument('--credentials-file',type=Path)
    parser.add_argument('--mss',action='store_true')
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args();os.umask(0o077);RELEASE.mkdir(mode=0o700,exist_ok=True)
    if args.action=='status':value=remote(STATUS,mss=args.mss,control_socket=args.control_socket)
    elif args.action=='freeze':
        require(args.check_result is not None,'check_receipt_required')
        check=json.loads(args.check_result.read_text())['check'];result=check['result.json']
        require(check['status.json']['stage']=='complete' and result['status']=='checked'
                and check['process.json']['sha256']==sha(canonical(package())),'successful_current_check_required')
        value=package();value['backend_id']=result['backend_id'];value['request']['baseline']=result['baseline']
        raw=canonical(value)
        with (RELEASE/'controller.json').open('xb') as stream:stream.write(raw)
        with (RELEASE/'SHA256SUMS').open('x') as stream:stream.write(sha(raw)+'  controller.json\n')
        value=dict(frozen=True,sha256=sha(raw),used=11881,resume_code='002180.SZ')
    else:
        require(not subprocess.check_output(['git','status','--porcelain'],cwd=REPO),'commit_before_launch')
        if args.action=='check':raw=canonical(package())
        else:
            require(args.credentials_file is not None,'credentials_required')
            raw=(RELEASE/'controller.json').read_bytes()
            require((RELEASE/'SHA256SUMS').read_text()==sha(raw)+'  controller.json\n','frozen_package_changed')
            frozen=json.loads(raw);current=package()
            require({k:v for k,v in frozen.items() if k not in ('backend_id','request')}==
                    {k:v for k,v in current.items() if k!='request'}
                    and {k:v for k,v in frozen['request'].items() if k!='baseline'}==current['request'],'operator_changed')
        request=dict(package=raw.decode(),sha256=sha(raw),check_only=args.action=='check',attempt=uuid.uuid4().hex)
        if args.action=='execute':request['auth']=read_auth(args.credentials_file,True)
        value=remote(LAUNCH,request,args.mss,args.control_socket)
    print(json.dumps(value,ensure_ascii=False))


if __name__=='__main__':main()
