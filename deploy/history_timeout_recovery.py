"""One explicit recovery: back up raw history, reconcile, then resume original plan."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
import types
import urllib.request
from bson import BSON
from pymongo import MongoClient

RUN=Path('/app/runtime/history-timeout-recovery-20261030-v1')


def require(value,reason):
    if not value:raise RuntimeError(reason)


def save(name,value):
    raw=value if isinstance(value,bytes) else json.dumps(value,sort_keys=True,default=str).encode()
    fd=os.open(RUN/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    fd=os.open(RUN,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)
    require((RUN/name).read_bytes()==raw,'backup_readback_failed')
    return hashlib.sha256(raw).hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise RuntimeError('redirect_refused')


def main(r, *, recoverer=None, expected_reason='timeout', raw_count=13136,
         run_directory=None):
    global RUN
    if run_directory is not None:
        RUN=Path(run_directory)
    os.umask(0o077)
    gate=None;client=None;reserved=False
    report=dict(status='failed',stage='preflight',trigger_sent=False,new_gate_stopped=True)
    try:
        require(expected_reason in ('timeout','tls_failed')
                and (expected_reason=='timeout' or recoverer is not None),'invalid_recovery_reason')
        require(not RUN.exists(),'already_reserved')
        require(shutil.disk_usage('/app/runtime').free>1024**3,'backup_space')
        for p,h in r['source_hashes'].items():require(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h,'source_changed')
        runtime=types.ModuleType('runtime');exec(r['runtime_tool'],runtime.__dict__)
        env,effective=runtime.reconstruct('/app/deploy/migration_production_entrypoint.py',
            'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
        require(all(not v['enabled'] for v in effective['gates'].values()),'quote_gate_enabled')
        require(str(env.get('TUSHARE_UNIFIED_ENABLED')).lower()=='false','legacy_enabled')
        options=dict(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],
            password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,
            retryReads=False,serverSelectionTimeoutMS=10000,socketTimeoutMS=15000)
        client=MongoClient(**options);db=client[env['MONGODB_DATABASE']]
        require(db.scheduler_executions.count_documents({'job_id':{'$regex':'^tushare_'},'status':{'$in':['running','cancelling']}})==0,'execution_active')
        for job in ('tushare_historical_sync','tushare_realtime_quotes_hourly'):
            require((db.scheduler_preferences.find_one({'_id':job}) or {}).get('enabled') is False,'scheduled_job_enabled')
        from tradingagents.utils.tushare_batch import selected_gate,recover_raw_timeout
        gate=selected_gate()
        require(str(gate.directory)==r['generation'],'generation_changed')
        with gate.locked() as fd:
            _,s=gate.read(fd)
            require(gate.stopped(fd) and s['phase']=='blocked' and s['reason']==expected_reason
                    and s['used']==r['used'] and s['plan']['plan_sha256']==r['plan'],'state_changed')
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
        def http(path,body,token=None):
            headers={'Content-Type':'application/json'}
            if token:headers['Authorization']='Bearer '+token
            req=urllib.request.Request('http://127.0.0.1:8000'+path,data=json.dumps(body).encode(),headers=headers)
            with opener.open(req,timeout=30) as response:return json.load(response)
        login=http('/api/auth/login',r['auth']);require(login.get('success') is True,'auth_failed')
        token=login['data']['access_token']
        RUN.mkdir(mode=0o700);reserved=True
        report['stage']='backup'
        hashes={};before={}
        for name,expected in [('stock_daily_quotes',2112),('stock_daily_quotes_unadjusted',raw_count)]:
            rows=list(db[name].find({'data_source':'tushare'}).max_time_ms(15000))
            require(len(rows)==expected,'inventory_changed')
            raw=b''.join(BSON.encode(row) for row in sorted(rows,key=lambda x:str(x['_id'])))
            hashes[name+'.bson']=save(name+'.bson',raw);before[name]=rows
            hashes[name+'-indexes.json']=save(name+'-indexes.json',db[name].index_information())
        # Preserve every old ledger byte, including STOP, before modifying just one item.
        ledger={str(p.relative_to(gate.directory)):p.read_bytes().hex() for p in gate.directory.rglob('*') if p.is_file()}
        hashes['ledger-before.json']=save('ledger-before.json',ledger)
        save('backup-manifest.json',dict(hashes=hashes,source_hashes=r['source_hashes'],plan=r['plan'],used=r['used']))
        os.environ.update(env)
        async def recover():
            from motor.motor_asyncio import AsyncIOMotorClient
            motor=AsyncIOMotorClient(**options)
            try:return await (recoverer or recover_raw_timeout)(gate,motor[env['MONGODB_DATABASE']].stock_daily_quotes_unadjusted,
                expected_plan=r['plan'],expected_used=r['used'])
            finally:motor.close()
        report['stage']='reconcile'
        receipt=asyncio.run(recover());save('recovery.json',receipt)
        report['stage']='verify_data'
        # No writes should have occurred during recovery; preserve exact BSON proof.
        for name,rows in before.items():
            current=list(db[name].find({'data_source':'tushare'}).max_time_ms(15000))
            raw=b''.join(BSON.encode(row) for row in sorted(current,key=lambda x:str(x['_id'])))
            require(hashlib.sha256(raw).hexdigest()==hashes[name+'.bson'],'data_changed_during_recovery')
        save('trigger-reserved.json',dict(at=time.time(),plan=r['plan']))
        report['stage']='submit'
        report['trigger_sent']=True
        response=http('/api/scheduler/jobs/tushare_historical_sync/trigger',{},token)
        save('trigger-response.json',response);require(response.get('success') is True,'button_rejected')
        report.update(status='passed',stage='submitted',new_gate_stopped=False,used_requests=r['used'],
            resume_code=receipt['resume_code'],plan=r['plan'],backup_directory=str(RUN),execution_status='submitted')
    except BaseException as error:
        report.update(error_type=type(error).__name__,reason=str(error) if isinstance(error,RuntimeError) else 'details_withheld')
    finally:
        if report['status']!='passed' and gate is not None:gate.stop()
        if client is not None:client.close()
        if reserved:save('result.json',report)
        print('BUTTON_ACCEPTANCE_JSON:'+json.dumps(report))
    return 0 if report['status']=='passed' else 1

if __name__=='__main__':raise SystemExit(main(json.load(sys.stdin)))
