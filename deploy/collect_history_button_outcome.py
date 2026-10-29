"""Read-only reconciliation after a lost/mixed controller stdout; never triggers jobs."""
import json
import os
from run_history_sample import ssh,EVIDENCE

REMOTE=r'''
import json,pathlib,subprocess
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-acceptance-20261029-v1')
p=json.loads((root/'package.json').read_text())
probe='REQUEST='+repr(p['request'])+'\n'+r"""
import hashlib,json,pathlib,types
from bson import BSON
from pymongo import MongoClient
from datetime import datetime,timezone
rt=types.ModuleType('rt');exec(REQUEST['runtime_tool'],rt.__dict__)
e,_=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
c=MongoClient(host=e['MONGODB_HOST'],port=int(e['MONGODB_PORT']),username=e['MONGODB_USERNAME'],password=e['MONGODB_PASSWORD'],authSource=e.get('MONGODB_AUTH_SOURCE','admin'),serverSelectionTimeoutMS=10000)
db=c[e['MONGODB_DATABASE']]
def sha(raw):return hashlib.sha256(raw).hexdigest()
def bson(rows):return b''.join(BSON.encode(r) for r in sorted(rows,key=lambda r:str(r['_id'])))
g=pathlib.Path('/app/runtime/tushare-history-batch-v1');run=pathlib.Path('/app/runtime/history-button-acceptance-20261029-v1')
state=json.loads((g/'state.json').read_text());plan=state['plan']
report=json.loads((run/'result.json').read_text())
manifest=json.loads((run/'backup-manifest.json').read_text())
backup_verified=all(sha((run/name).read_bytes())==digest for name,digest in manifest['hashes'].items())
rows=list(db.stock_daily_quotes.find({'data_source':'tushare'}))
series=[r for r in rows if r.get('symbol')=='600000' and r.get('period')=='daily']
others=[r for r in rows if r['_id'] not in {x['_id'] for x in series}]
series_same=sha(bson(series))==REQUEST['series_bson_sha256']
others_same=sha(bson(others))==REQUEST['other_bson_sha256']
protected={p:sha(pathlib.Path(p).read_bytes()) for p in REQUEST['protected_hashes']}
executions=list(db.scheduler_executions.find({'job_id':'tushare_historical_sync','plan_sha256':plan['plan_sha256']},
 {'_id':1,'status':1,'error_message':1,'timestamp':1,'processed_items':1}))
journal=[]
for f in (g/'runs').rglob('600000.SH.json'):
 d=json.loads(f.read_text());payload=d.get('payload') or {};docs=payload.get('documents',[])
 journal.append(dict(phase=d['phase'],fetch_attempts=d['fetch_attempts'],payload_sha256=d.get('payload_sha256'),
   document_count=len(docs),dates=[x['trade_date'] for x in docs],
   units=[{'volume_unit':x.get('volume_unit'),'amount_unit':x.get('amount_unit')} for x in docs]))
report.update(reconciled_at=datetime.now(timezone.utc).isoformat(),backup_verified=backup_verified,
 new_gate_stopped=(g/'STOP').is_file(),old_gates_unchanged=protected==REQUEST['protected_hashes'],
 all_tushare_data_unchanged=series_same and others_same,series_count=len(series),other_count=len(others),
 plan_sha256=plan['plan_sha256'],ledger=dict(phase=state['phase'],used=state['used'],active=state['active']),
 executions=executions,journal=journal)
print('OUTCOME_JSON:'+json.dumps(report,default=str))
"""
r=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],input=probe,text=True,capture_output=True,timeout=30)
if r.returncode:raise RuntimeError('read_only_probe_failed')
reports=[l.removeprefix('OUTCOME_JSON:') for l in r.stdout.splitlines() if l.startswith('OUTCOME_JSON:')]
if len(reports)!=1:raise RuntimeError('report_missing')
print(reports[0])
'''

if __name__=='__main__':
    report=ssh(REMOTE)
    path=EVIDENCE/'history-button-real-outcome-20261029.json'
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2)
    print(json.dumps({'evidence':str(path),**report},ensure_ascii=False))
