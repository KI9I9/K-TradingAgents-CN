"""Prepare read-only first; execute only after approval of the exact frozen package hash."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from run_history_sample import EVIDENCE, REPO, ssh

ARTIFACT = EVIDENCE / 'history-write-package-v1.json'
ROOT = '/home/ubuntu/k-tradingagents-history-write-20261028-600000-v1'
DRIVER = r'''
import json,os,pathlib,subprocess,time
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-write-20261028-600000-v1')
os.umask(0o077)
pkg=json.loads((root/'package.json').read_text())
def save(name,value):
 p=root/(name+'.tmp')
 with p.open('w') as f:json.dump(value,f);f.flush();os.fsync(f.fileno())
 p.replace(root/name)
request=pkg['request']
code='REQUEST='+repr(request)+'\nexec('+repr(pkg['code'])+',{"__name__":"__main__","REQUEST":REQUEST})'
save('status.json',{'stage':'running','started_at':time.time()})
try:
 p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],input=code,text=True,capture_output=True,timeout=120)
 report=json.loads(p.stdout)
 save('result.json',report)
 save('status.json',{'stage':'complete' if p.returncode==0 and report.get('status')=='passed' else 'failed',
      'finished_at':time.time(),'exit_code':p.returncode,'automatic_retry':False})
except BaseException as e:
 save('status.json',{'stage':'unknown_outcome','error_type':type(e).__name__,'automatic_retry':False})
'''
START = r'''
import hashlib,json,os,pathlib,subprocess,sys
os.umask(0o077)
incoming=json.load(sys.stdin);raw=incoming['raw'].encode()
if hashlib.sha256(raw).hexdigest()!=incoming['sha256']:raise RuntimeError('package_hash')
pkg=json.loads(raw)
i=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
current={'id':i['Id'],'image':i['Image'],'runtime_mount':[m for m in i['Mounts'] if m['Destination']=='/app/runtime']}
if current!=pkg['expected_container'] or i['State']['Health']['Status']!='healthy':raise RuntimeError('container_drift')
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-write-20261028-600000-v1')
root.mkdir(mode=0o700) # one launch only, including ambiguous/disconnected outcomes
for name,data in [('package.json',raw),('driver.py',pkg['driver'].encode())]:
 with (root/name).open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
with (root/'process.log').open('ab') as log:
 child=subprocess.Popen(['python3','-B',str(root/'driver.py')],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
print(json.dumps({'started':True,'pid':child.pid,'sha256':incoming['sha256']}))
'''
STATUS = r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-write-20261028-600000-v1')
print(json.dumps({n:json.loads((root/n).read_text()) for n in ['status.json','result.json'] if (root/n).is_file()}))
'''


def save(path, data):
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as f:json.dump(data,f,sort_keys=True);f.flush();os.fsync(f.fileno())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',required=True,choices=['prepare','execute','status','collect'])
    parser.add_argument('--approve-package')
    args=parser.parse_args()
    if args.action=='prepare':
        if ARTIFACT.exists():raise SystemExit('frozen package exists; do not overwrite')
        if subprocess.check_output(['git','status','--porcelain'],cwd=REPO):raise SystemExit('commit before prepare')
        captured=(EVIDENCE/'history-sample-production-result-v2.json').read_text()
        old=json.loads((EVIDENCE/'history-sample-package-v2.json').read_text())['request']
        protected=list(old['original_gate_hashes'])
        for name in ['STOP','gate.lock','permit.json']:
            protected.append('/app/runtime/history-acceptance-20261028-600000-v2/'+name)
        code='import json,pathlib,hashlib\npaths='+repr(protected)+'\nprint(json.dumps({p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in paths}))'
        host="import subprocess\np=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],input="+repr(code)+",capture_output=True,text=True,timeout=20)\nif p.returncode:raise RuntimeError('protected_read_failed')\nprint(p.stdout)"
        hashes=ssh(host)
        if any(hashes[p]!=h for p,h in old['original_gate_hashes'].items()):raise SystemExit('old_gate_drift')
        source_hashes={p:hashlib.sha256((REPO/p).read_bytes()).hexdigest() for p in [
            'app/services/historical_data_service.py','app/services/historical_contract.py']}
        request=dict(mode='preflight',approved=False,captured_json=captured,
            captured_sha256=hashlib.sha256(captured.encode()).hexdigest(),source_hashes=source_hashes,
            protected_hashes=hashes,runtime_tool=(REPO/'deploy/quote_guard_runtime.py').read_text())
        body=(REPO/'deploy/history_write_acceptance.py').read_text()
        probe='REQUEST='+repr(request)+'\nexec('+repr(body)+',{"__name__":"__main__","REQUEST":REQUEST})'
        host="import subprocess,json\np=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],input="+repr(probe)+",capture_output=True,text=True,timeout=45)\nprint(p.stdout if p.stdout else json.dumps({'status':'preflight_failed','exit_code':p.returncode}))"
        result=ssh(host)
        inventory=ssh("import subprocess,json\ni=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]\nprint(json.dumps({'id':i['Id'],'image':i['Image'],'runtime_mount':[m for m in i['Mounts'] if m['Destination']=='/app/runtime']}))")
        mounts=inventory['runtime_mount']
        if len(mounts)!=1 or mounts[0]['Type']!='bind' or not mounts[0]['RW']:raise SystemExit('persistent_backup_mount_required')
        result['container']=inventory
        if result.get('status')!='preflight_passed':raise SystemExit(json.dumps(result))
        save(EVIDENCE/'history-write-preflight-v1.json',result)
        request.update(mode='execute',approved=True,baseline=result['baseline'])
        package=dict(schema=1,request=request,code=body,driver=DRIVER,expected_container=inventory,
            source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip())
        save(ARTIFACT,package)
        print(json.dumps({'prepared':str(ARTIFACT),'sha256':hashlib.sha256(ARTIFACT.read_bytes()).hexdigest(),
            'baseline':result['baseline'],'target_count':5,'business_database_writes':False,'upstream_requests':0}))
    elif args.action=='execute':
        raw=ARTIFACT.read_text();digest=hashlib.sha256(raw.encode()).hexdigest()
        if args.approve_package!=digest:raise SystemExit('explicit approved package hash required')
        print(json.dumps(ssh(START,{'raw':raw,'sha256':digest})))
    else:
        result=ssh(STATUS)
        if args.action=='collect':save(EVIDENCE/'history-write-production-result-v1.json',result)
        print(json.dumps(result))


if __name__=='__main__':main()
