"""Freeze/launch one finite history schedule activation; never auto-replay it.

Code deployment must already have passed. Normal auth travels only through stdin.
The launcher also requires the approved local transport renewal to have succeeded.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from prepare_history_schedule import REPO,EVIDENCE,RELEASE,PREFLIGHT,ROOT as SWITCH_ROOT,remote,digest
from query_quote_guard_jobs import read_auth

ROOT='/home/ubuntu/k-tradingagents-history-schedule-opening-v1-20261102'

DRIVER=r'''
import json,sys,subprocess,pathlib,os,hashlib,time
os.umask(0o077);r=json.load(sys.stdin);root=pathlib.Path(r['root'])
assert not (root/'started').exists(),'activation_already_reserved'
p=pathlib.Path(r['switch_root']);latest=json.loads((p/'latest-attempt.json').read_text());done=json.loads((p/'attempts'/latest['attempt']/'completion.json').read_text())
assert done['status']=='deployed_schedule_permission_absent' and done['source_commit']==r['source_head'] and done['backend_mounts']==43,'deployment_not_verified'
root.mkdir(mode=0o700,exist_ok=True)
assert hashlib.sha256(r['code'].encode()).hexdigest()==r['code_sha256'],'activation_source_changed'
fd=os.open(root/'started',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
os.fsync(fd);os.close(fd)
code='import json,sys\nr=json.load(sys.stdin)\nns={"__name__":"activation"}\nexec(r["code"],ns)\nraise SystemExit(ns["main"](r["request"]))'
payload=dict(code=r['code'],request=r['request'])
p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',code],input=json.dumps(payload),text=True,capture_output=True,timeout=3600)
(root/'activation.log').write_text(p.stdout+'\n'+p.stderr)
rows=[line.split(':',1)[1] for line in p.stdout.splitlines() if line.startswith('SCHEDULE_OPENING_JSON:')]
result=dict(returncode=p.returncode,result=json.loads(rows[0]) if len(rows)==1 else None)
(root/'result.json').write_text(json.dumps(result))
# A restart check is intentionally separate from the data run and happens before
# the first cron. Only the selected gate is stopped if verification fails.
if p.returncode==0 and result['result'] and result['result']['status']=='armed':
 try:
  inspected=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],text=True,timeout=15))[0]
  assert len(inspected['Mounts'])==43,'mount_count_changed'
  subprocess.run(['docker','restart','--time','30',inspected['Id']],check=True,capture_output=True,timeout=90)
  healthy=False
  for _ in range(36):
   inspected=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],text=True,timeout=10))[0]
   if inspected['State'].get('Health',{}).get('Status')=='healthy':healthy=True;break
   if not inspected['State']['Running']:break
   time.sleep(5)
  assert healthy,'restart_unhealthy'
  probe=r['verify_code']
  checked=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',probe],input=json.dumps(r['request']),text=True,capture_output=True,timeout=45)
  (root/'restart.log').write_text(checked.stdout+'\n'+checked.stderr)
  rows=[line.split(':',1)[1] for line in checked.stdout.splitlines() if line.startswith('SCHEDULE_RESTART_JSON:')]
  assert checked.returncode==0 and len(rows)==1,'restart_preference_unverified'
  result['restart']=json.loads(rows[0]);result['status']='armed_restart_verified'
 except BaseException:
  stopped=subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c','from tradingagents.utils.tushare_batch import selected_gate; g=selected_gate(); g.stop(); print((g.directory/"STOP").is_file())'],text=True,capture_output=True,timeout=20)
  result.update(status='restart_failed',gate_stop_confirmed=stopped.returncode==0 and stopped.stdout.splitlines()[-1:] == ['True'])
(root/'result.json').write_text(json.dumps(result))
print(json.dumps(result))
'''

VERIFY=r'''
import json,sys,urllib.request
r=json.load(sys.stdin)
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*a,**k):raise RuntimeError('redirect_refused')
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
def fetch(path,body=None,token=None):
 headers={'Content-Type':'application/json'}
 if token:headers['Authorization']='Bearer '+token
 req=urllib.request.Request('http://127.0.0.1:8000'+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
 with opener.open(req,timeout=15) as response:return json.load(response)
login=fetch('/api/auth/login',r['auth']);assert login.get('success') is True,'auth_failed'
token=login['data']['access_token'];jobs=fetch('/api/scheduler/jobs',token=token)['data']
from tradingagents.utils.tushare_manual import JOB_IDS
from app.services.tushare_history_schedule import load_schedule,CRON
permit=load_schedule();target='tushare_historical_sync'
for job_id in JOB_IDS:
 job=next(j for j in jobs if j['id']==job_id)
 assert job['paused'] is (job_id!=target),'preference_not_restored'
 if job_id==target:
  assert job['next_run_time'] and "hour='18', minute='30'" in job['trigger'],'cron_not_restored'
print('SCHEDULE_RESTART_JSON:'+json.dumps(dict(preference_restored=True,cron=CRON,generation=permit['generation'],expires_at=permit['expires_at'])))
'''


def package():
    baseline=json.loads((EVIDENCE/'history-schedule-baseline-20261102.json').read_text())['inside']
    fresh=json.loads(PREFLIGHT.read_text());manifest=json.loads((RELEASE/'manifest.json').read_text())
    installed=fresh['modules']|{e['target']:e['sha256'] for e in manifest['files']}
    code=(REPO/'deploy/history_schedule_opening.py').read_text()
    return dict(root=ROOT,switch_root=SWITCH_ROOT,source_head=manifest['source_head'],code=code,code_sha256=digest(code.encode()),verify_code=VERIFY,driver=DRIVER,
        request=dict(source_hashes=installed,history_bson=fresh['history_bson'],runtime=(REPO/'deploy/quote_guard_runtime.py').read_text(),
            previous_directory=baseline['generation'],previous_policy_sha256=digest(json.dumps(baseline['policy'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()),previous_plan=baseline['plan_sha256'],previous_execution=baseline['job']['_id']))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze','execute','status'],required=True)
    parser.add_argument('--control-socket',type=Path);parser.add_argument('--credentials-file',type=Path)
    args=parser.parse_args();os.umask(0o077);artifact=RELEASE/'opening.json'
    if args.action=='freeze':
        assert not subprocess.check_output(['git','status','--porcelain'],cwd=REPO),'commit_before_freeze'
        request=package()
        with artifact.open('x') as stream:json.dump(request,stream)
        print(json.dumps(dict(frozen=True,package_sha256=digest(artifact.read_bytes()))));return
    if args.action=='status':
        code="import pathlib,json; root=pathlib.Path("+repr(ROOT)+"); print(json.dumps({'reserved':root.exists(),'result':json.loads((root/'result.json').read_text()) if (root/'result.json').exists() else None,'process':json.loads((root/'process.json').read_text()) if (root/'process.json').exists() else None}))"
        result=remote(code,None,args.control_socket)
    else:
        assert args.credentials_file,'credentials_required'
        transport=subprocess.run(['sudo','-n','python3','-B',str(REPO/'deploy/renew_history_transport.py'),'--action','status'],text=True,capture_output=True,timeout=30)
        assert transport.returncode==0,'transport_inspection_failed'
        renewed=json.loads(transport.stdout)
        assert renewed['result'] and renewed['result']['status']=='renewed' and renewed['active']=='active' and renewed['deadline']=='2026-11-08T06:00:00+00:00','finite_transport_renewal_required'
        request=json.loads(artifact.read_text());assert request['driver']==DRIVER and request['code']==(REPO/'deploy/history_schedule_opening.py').read_text(),'frozen_activation_changed'
        request['request']['auth']=read_auth(args.credentials_file,True)
        code="import subprocess,pathlib,json,sys,os\nos.umask(0o077)\nr=json.load(sys.stdin);root=pathlib.Path(r['root']);root.mkdir(mode=0o700,exist_ok=True)\nfd=os.open(root/'launch-reserved',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.fsync(fd);os.close(fd)\nwith (root/'process.log').open('ab') as log:\n child=subprocess.Popen(['python3','-B','-c',r['driver']],stdin=subprocess.PIPE,stdout=log,stderr=log,start_new_session=True);child.stdin.write(json.dumps(r).encode());child.stdin.close()\np=dict(pid=child.pid,package_sha256=r['package_sha256']);(root/'process.json').write_text(json.dumps(p));print(json.dumps(p))"
        request['package_sha256']=digest(artifact.read_bytes());result=remote(code,request,args.control_socket)
    (EVIDENCE/('history-schedule-opening-'+args.action+'-'+str(time.time_ns())+'.json')).write_text(json.dumps(result,indent=2))
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
