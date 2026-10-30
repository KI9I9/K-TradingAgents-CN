"""Read-only database/API verification after raw daily launch; login audits only."""
import json
from pathlib import Path
from run_history_sample import ssh,EVIDENCE
from query_quote_guard_jobs import read_auth
ROOT=Path(__file__).resolve().parents[1]
REMOTE=r'''
import hashlib,json,sys,types,urllib.request
from pathlib import Path
from bson import BSON
from pymongo import MongoClient
r=json.load(sys.stdin)
rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
env,_=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
c=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=15000)
db=c[env['MONGODB_DATABASE']]
old=list(db.stock_daily_quotes.find({'data_source':'tushare'}))
old_hash=hashlib.sha256(b''.join(BSON.encode(x) for x in sorted(old,key=lambda x:str(x['_id'])))).hexdigest()
assert old_hash==r['old_sha256'] and len(old)==r['old_count'], 'old_history_changed'
registry=Path('/app/runtime/tushare-history-generations')
selection=json.loads((registry/'active.json').read_text());g=registry/selection['generation']
p=json.loads((g/'policy.json').read_text());s=json.loads((g/'state.json').read_text())
assert p['schema']==2 and p['minute_requests']==40 and p['daily_requests']==7500
receipt=json.loads((g/'retirement.json').read_text());previous=Path(receipt['previous_directory'])
assert receipt['previous_hashes']=={str(x.relative_to(previous)):hashlib.sha256(x.read_bytes()).hexdigest() for x in previous.rglob('*') if x.is_file()}, 'old_ledger_changed'
rows=list(db.stock_daily_quotes_unadjusted.find({'data_source':'tushare'}))
assert rows, 'no_raw_history_yet'
for row in rows:
    assert row['price_adjustment']=='none' and row['source_endpoint']=='daily'
    assert row['volume_unit']=='lot' and row['amount_unit']=='CNY' and row['history_contract_version']==1
    assert row['market']=='CN' and row['period']=='daily'
    assert p['min_date']<=row['trade_date']<=s['plan']['end']
    assert row['low']<=min(row['open'],row['close'])<=max(row['open'],row['close'])<=row['high']
    assert row['volume']>=0 and (row['amount'] is None or row['amount']>=0)
    assert 'is_final' not in row and 'trading_status' not in row
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*a,**kw):raise RuntimeError('redirect_refused')
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
def http(path,body=None,token=None):
    headers={'Content-Type':'application/json'}
    if token:headers['Authorization']='Bearer '+token
    req=urllib.request.Request('http://127.0.0.1:8000'+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
    with opener.open(req,timeout=15) as response:return json.load(response)
login=http('/api/auth/login',r['auth']);assert login.get('success') is True
symbol=sorted(set(x['symbol'] for x in rows))[0]
response=http('/api/stock-sync/history-unadjusted/'+symbol+'?limit=120',token=login['data']['access_token'])
assert response.get('success') is True and response['data']['price_adjustment']=='none'
expected=sorted([x for x in rows if x['symbol']==symbol],key=lambda x:x['trade_date'])[-120:]
expected=[{k:v for k,v in x.items() if k not in ('_id','created_at','updated_at')} for x in expected]
assert response['data']['items']==expected, 'raw_api_mismatch'
job=db.scheduler_executions.find_one({'job_id':'tushare_historical_sync'},sort=[('timestamp',-1)]) or {}
result=dict(old_history_unchanged=True,old_history_count=len(old),old_history_sha256=old_hash,
    old_ledger_unchanged=True,raw_history_count=len(rows),raw_series_count=len(set(x['symbol'] for x in rows)),
    api_symbol=symbol,api_rows=len(expected),api_matches=True,generation=str(g),stopped=(g/'STOP').exists(),
    phase=s['phase'],reason=s['reason'],used=s['used'],pool_count=len(p['codes']),
    job={k:job.get(k) for k in ('status','processed_items','total_items','progress_message','error_message')})
print('BASIC_VERIFICATION:'+json.dumps(result,default=str))
c.close()
'''

def verify():
    inventory=json.loads((EVIDENCE/'history-basic-inventory-20261030.json').read_text())
    wrapper="""import json,subprocess,sys
r=json.load(sys.stdin)
p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',r['code']],input=json.dumps(r['request']),text=True,capture_output=True,timeout=45)
lines=[x.split(':',1)[1] for x in p.stdout.splitlines() if x.startswith('BASIC_VERIFICATION:')]
if p.returncode or len(lines)!=1:raise RuntimeError('basic_verification_failed')
print(lines[0])
"""
    return ssh(wrapper,dict(code=REMOTE,request=dict(runtime=(ROOT/'deploy/quote_guard_runtime.py').read_text(),
        old_sha256=inventory['history_sha256'],old_count=inventory['history_count'],
        auth=read_auth(Path('/home/ubuntu/.ta_ops_cred'),True))))

if __name__=='__main__':print(json.dumps(verify(),ensure_ascii=False))
