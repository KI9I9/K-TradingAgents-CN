"""Read-only compare original BSON records to the recovery backup during a run.

No full records or credentials leave production. New raw records may be added;
each original _id must still have byte-identical BSON. Backup files are read
back and checked against their frozen manifest before querying MongoDB.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess

REPO=Path(__file__).resolve().parents[1]
REMOTE=r'''
import pathlib,json,sys,types,hashlib
from bson import BSON,decode_all
from pymongo import MongoClient
r=json.load(sys.stdin);root=pathlib.Path('/app/runtime/history-http-reuse-recovery-20261101-v1')
manifest=json.loads((root/'backup-manifest.json').read_text())
assert manifest['used']==2298 and manifest['plan']=='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4','backup_identity_changed'
rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
env,_=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=15000)
out={}
try:
 db=client[env['MONGODB_DATABASE']]
 for name,count in [('stock_daily_quotes',2112),('stock_daily_quotes_unadjusted',38641)]:
  raw=(root/(name+'.bson')).read_bytes();expected=manifest['hashes'][name+'.bson']
  assert hashlib.sha256(raw).hexdigest()==expected,'backup_hash_changed'
  rows=decode_all(raw);assert len(rows)==count,'backup_count_changed'
  current=[]
  for start in range(0,len(rows),1000):
   ids=[row['_id'] for row in rows[start:start+1000]]
   current.extend(db[name].find({'_id':{'$in':ids}}).max_time_ms(15000))
  original=b''.join(BSON.encode(row) for row in sorted(current,key=lambda row:str(row['_id'])))
  assert len(current)==count and hashlib.sha256(original).hexdigest()==expected,'original_records_changed'
  total=db[name].count_documents({'data_source':'tushare'},maxTimeMS=15000)
  assert total>=count and (name!='stock_daily_quotes' or total==count),'unexpected_inventory'
  out[name]=dict(original_count=count,original_bson_sha256=expected,original_unchanged=True,current_count=total)
 print(json.dumps(dict(passed=True,collections=out)))
finally:client.close()
'''
HOST=r'''
import json,sys,subprocess
r=json.load(sys.stdin)
p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',r['code']],input=json.dumps(r['request']),text=True,capture_output=True,timeout=80)
assert p.returncode==0,'record_verification_failed_details_withheld'
print(p.stdout)
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control-socket',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    assert not args.output.exists(),'output_already_exists'
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
         '-S',str(args.control_socket) if args.control_socket else 'none','-C','-o','BatchMode=yes',
         '-o','IPQoS=none','-o','ConnectTimeout=25','osaka-migration','sudo -n python3 -B -c '+shlex.quote(HOST)]
    p=subprocess.run(cmd,input=json.dumps(dict(code=REMOTE,request=dict(runtime=(REPO/'deploy/quote_guard_runtime.py').read_text()))),capture_output=True,text=True,timeout=110)
    if p.returncode:raise SystemExit('verification_failed_or_management_unavailable; no records modified')
    result=json.loads(p.stdout)
    fd=os.open(args.output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream:json.dump(result,stream,indent=2)
    print(json.dumps(result))


if __name__=='__main__':main()
