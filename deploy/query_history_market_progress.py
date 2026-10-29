"""Read-only current history permit, task progress and Tushare series counts."""
import json
from pathlib import Path
from run_history_sample import ssh
ROOT=Path(__file__).resolve().parents[1]
REMOTE=r'''
import json,sys,types
from pathlib import Path
from pymongo import MongoClient
r=json.load(sys.stdin)
rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
env,_=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
registry=Path('/app/runtime/tushare-history-generations')
active=json.loads((registry/'active.json').read_text());folder=registry/active['generation']
policy=json.loads((folder/'policy.json').read_text());state=json.loads((folder/'state.json').read_text())
c=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=10000)
db=c[env['MONGODB_DATABASE']]
row=db.scheduler_executions.find_one({'job_id':'tushare_historical_sync'},sort=[('timestamp',-1)]) or {}
job={k:row.get(k) for k in ('_id','status','timestamp','updated_at','progress','processed_items','total_items','success_count','skipped_count','error_message','progress_message','plan_sha256')}
q={'data_source':'tushare'}
print('MARKET_PROGRESS:'+json.dumps(dict(generation=str(folder),stopped=(folder/'STOP').exists(),phase=state['phase'],reason=state['reason'],api_code=state.get('api_code'),used=state['used'],days=state['days'],active=state['active'],pool_count=len(policy['codes']),expires_at=policy['expires_at'],job=job,history_count=db.stock_daily_quotes.count_documents(q),series_count=len(db.stock_daily_quotes.distinct('symbol',q))),default=str))
c.close()
'''

def query():
    wrapper="""import json,sys,subprocess
r=json.load(sys.stdin)
p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',r['code']],input=json.dumps(r['request']),text=True,capture_output=True,timeout=30)
lines=[x.split(':',1)[1] for x in p.stdout.splitlines() if x.startswith('MARKET_PROGRESS:')]
if p.returncode or len(lines)!=1:raise RuntimeError('progress_query_failed')
print(lines[0])
"""
    return ssh(wrapper,dict(code=REMOTE,request=dict(runtime=(ROOT/'deploy/quote_guard_runtime.py').read_text())))

if __name__=='__main__':print(json.dumps(query(),ensure_ascii=False))
