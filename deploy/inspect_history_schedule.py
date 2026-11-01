"""Read-only schedule baseline: no data API request and no database writes."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess

REPO = Path(__file__).resolve().parents[1]
FILES = ('app/worker/tushare_sync_service.py', 'app/services/tushare_history_batches.py',
         'app/services/scheduler_runtime.py', 'tradingagents/utils/tushare_manual.py')

INSIDE = r'''
import json,sys,types,hashlib
from pathlib import Path
from pymongo import MongoClient
r=json.load(sys.stdin)
rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
root=Path('/app/runtime/tushare-history-generations')
a=json.loads((root/'active.json').read_text());g=root/a['generation']
policy=json.loads((g/'policy.json').read_text());s=json.loads((g/'state.json').read_text())
c=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=10000)
try:
 db=c[env['MONGODB_DATABASE']]
 row=db.scheduler_executions.find_one({'job_id':'tushare_historical_sync'},sort=[('timestamp',-1)]) or {}
 job={k:row.get(k) for k in ('_id','status','timestamp','updated_at','finished_at','progress','success_count','skipped_count','skipped_items','result','error_message','plan_sha256')}
 counts={n:db[n].count_documents({'data_source':'tushare'}) for n in ('stock_daily_quotes','stock_daily_quotes_unadjusted')}
 out=dict(generation=str(g),policy=policy,state={k:s[k] for k in ('phase','reason','used','days','active')},plan_sha256=s['plan']['plan_sha256'],stopped=(g/'STOP').exists(),job=job,counts=counts,
   source_hashes={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in r['files']},legacy_stop=Path('/app/runtime/tushare-egress/STOP').is_file(),quote_gates=effective['gates'],legacy_unified=env.get('TUSHARE_UNIFIED_ENABLED'),
   historical_preference=db.scheduler_preferences.find_one({'_id':'tushare_historical_sync'}))
 print('SCHEDULE_INSPECTION:'+json.dumps(out,default=str))
finally:c.close()
'''

HOST = r'''
import subprocess,json,sys
r=json.load(sys.stdin)
i=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],text=True,timeout=10))[0]
p=subprocess.run(['docker','exec','-i',i['Id'],'python3','-B','-c',r['code']],input=json.dumps(r['request']),text=True,capture_output=True,timeout=35)
lines=[s.split(':',1)[1] for s in p.stdout.splitlines() if s.startswith('SCHEDULE_INSPECTION:')]
assert p.returncode==0 and len(lines)==1,'inspection_failed_details_withheld'
out=dict(backend=dict(id=i['Id'],image=i['Image'],user=i['Config']['User'],health=i['State'].get('Health',{}).get('Status'),mounts=i['Mounts'],compose_labels={k:v for k,v in i['Config']['Labels'].items() if k.startswith('com.docker.compose.')}),inside=json.loads(lines[0]))
print(json.dumps(out))
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args()
    assert not args.output.exists(),'output_already_exists'
    request=dict(code=INSIDE,request=dict(runtime=(REPO/'deploy/quote_guard_runtime.py').read_text(),files=['/app/'+p for p in FILES]))
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S',str(args.control_socket) if args.control_socket else 'none','-C','-o','BatchMode=yes','-o','IPQoS=none','-o','ConnectTimeout=20','osaka-migration','sudo -n python3 -B -c '+shlex.quote(HOST)]
    p=subprocess.run(cmd,input=json.dumps(request),capture_output=True,text=True,timeout=65)
    if p.returncode:raise SystemExit('inspection_management_failed; no production mutation')
    result=json.loads(p.stdout)
    fd=os.open(args.output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream:json.dump(result,stream,indent=2)
    inside=result['inside']
    print(json.dumps(dict(backend_health=result['backend']['health'],mount_count=len(result['backend']['mounts']),job=inside['job'],counts=inside['counts'],policy={k:v for k,v in inside['policy'].items() if k!='codes'},used=inside['state']['used'],legacy_stop=inside['legacy_stop']),ensure_ascii=False))


if __name__=='__main__':main()
