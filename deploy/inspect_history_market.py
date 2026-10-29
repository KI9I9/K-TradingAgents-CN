"""Read-only production stock universe/data inventory, no provider calls."""
import json
from pathlib import Path
from run_history_sample import ssh, EVIDENCE
ROOT=Path(__file__).resolve().parents[1]
REMOTE=r'''
import json,sys,types,os,hashlib
from pymongo import MongoClient
from bson import BSON
r=json.load(sys.stdin)
rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
c=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=10000,connectTimeoutMS=5000)
db=c[env['MONGODB_DATABASE']]
from deploy.history_sync_plan import _code
rows=list(db.stock_basic_info.find({}, {'symbol':1,'code':1,'ts_code':1,'list_status':1,'market':1,'data_source':1}))
codes=set();excluded=set();invalid=0;invalid_examples=[]
for row in rows:
 raw=row.get('ts_code') or row.get('symbol') or row.get('code')
 try:code=_code(raw)
 except Exception:
  invalid+=1;invalid_examples.append(raw);continue
 if row.get('list_status')=='D':excluded.add(code)
 else:codes.add(code)
codes-=excluded
history=list(db.stock_daily_quotes.find({'data_source':'tushare'}))
raw=b''.join(BSON.encode(x) for x in sorted(history,key=lambda x:str(x['_id'])))
from tradingagents.utils.tushare_batch import selected_gate
g=selected_gate()
with g.locked() as fd:
 p,s=g.read(fd);stopped=g.stopped(fd)
report=dict(pool=sorted(codes),basic_count=len(rows),excluded_delisted=sorted(excluded),invalid_basic_rows=invalid,invalid_examples=invalid_examples,
 history_count=len(history),history_sha256=hashlib.sha256(raw).hexdigest(),
 series_counts={code:sum(x.get('symbol')==code for x in history) for code in sorted(set(x.get('symbol','') for x in history))},
 gate=dict(directory=str(g.directory),stopped=stopped,phase=s['phase'],active=s['active'],used=s['used']),
 scheduled=list(db.scheduler_preferences.find({'_id':{'$in':['tushare_historical_sync','tushare_realtime_quotes_hourly']}},{'_id':1,'enabled':1})))
print('MARKET_INVENTORY:'+json.dumps(report))
c.close()
'''
if __name__=='__main__':
    request={'runtime':(ROOT/'deploy/quote_guard_runtime.py').read_text()}
    wrapper="""import json,sys,subprocess
r=json.load(sys.stdin)
p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',r['code']],input=json.dumps(r['request']),text=True,capture_output=True,timeout=45)
lines=[x.split(':',1)[1] for x in p.stdout.splitlines() if x.startswith('MARKET_INVENTORY:')]
if p.returncode or len(lines)!=1:raise RuntimeError('inventory_failed')
print(lines[0])
"""
    report=ssh(wrapper,{'code':REMOTE,'request':request})
    target=EVIDENCE/'history-market-inventory-20261029.json'
    with target.open('x') as f:json.dump(report,f,indent=2)
    print(json.dumps({k:v for k,v in report.items() if k!='pool'},ensure_ascii=False))
    print('pool_count='+str(len(report['pool'])))
