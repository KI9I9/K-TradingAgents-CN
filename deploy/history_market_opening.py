"""Back up existing Tushare history and open the approved finite A-share pool.

One reserved operator run. Normal success leaves the permit usable. Failure
stops the new permit, without deleting or rewriting existing evidence/data.
"""
import asyncio
from datetime import datetime,timedelta,timezone
import hashlib,json,os,shutil,sys,time,types,urllib.request,urllib.error
from pathlib import Path
from bson import BSON
from pymongo import MongoClient

RUN=Path('/app/runtime/history-market-opening-20261029-v1')
BJ=timezone(timedelta(hours=8))
BACKUP_KEY={'data_source':'tushare'}


def require(value,reason):
    if not value:raise RuntimeError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def save(name,value):
    raw=value if isinstance(value,bytes) else json.dumps(value,sort_keys=True,default=str).encode()
    fd=os.open(RUN/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    fd=os.open(RUN,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)
    require((RUN/name).read_bytes()==raw,'backup_readback_failed')
    return sha(raw)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise RuntimeError('redirect_refused')


def main(r):
    os.umask(0o077)
    report=dict(status='failed',trigger_sent=False,new_gate_stopped=True)
    gate=None;client=None;reserved=False
    try:
        require(not RUN.exists(),'opening_already_reserved')
        require(shutil.disk_usage('/app/runtime').free>=3*1024**3,'insufficient_backup_space')
        for path,digest in r['source_hashes'].items():require(sha(Path(path).read_bytes())==digest,'source_drift')
        rt=types.ModuleType('runtime');exec(r['runtime_tool'],rt.__dict__)
        env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py',
            'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
        require(all(not v['enabled'] for v in effective['gates'].values()),'quote_gate_enabled')
        require(str(env.get('TUSHARE_UNIFIED_ENABLED')).lower()=='false','legacy_tushare_enabled')
        client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],
            password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,
            serverSelectionTimeoutMS=10000,socketTimeoutMS=10000)
        db=client[env['MONGODB_DATABASE']];col=db.stock_daily_quotes
        for job in ('tushare_historical_sync','tushare_realtime_quotes_hourly'):
            require((db.scheduler_preferences.find_one({'_id':job}) or {}).get('enabled') is False,'scheduled_job_enabled')
        require(db.scheduler_executions.count_documents({'job_id':{'$regex':'^tushare_'},'status':{'$in':['running','cancelling']}})==0,'existing_execution_active')
        from deploy.history_sync_plan import _code
        codes=set();excluded=set()
        for row in db.stock_basic_info.find({}, {'symbol':1,'code':1,'ts_code':1,'list_status':1}):
            code=_code(row.get('ts_code') or row.get('symbol') or row.get('code'))
            (excluded if row.get('list_status')=='D' else codes).add(code)
        codes=sorted(codes-excluded)
        require(codes==r['codes'],'stock_pool_changed')
        rows=list(col.find(BACKUP_KEY).max_time_ms(15000))
        raw=b''.join(BSON.encode(row) for row in sorted(rows,key=lambda row:str(row['_id'])))
        require(sha(raw)==r['history_sha256'] and len(rows)==2095,'history_baseline_changed')
        from tradingagents.utils.tushare_batch import selected_gate,activate_generation
        previous=selected_gate()
        require(str(previous.directory)==r['previous_directory'],'generation_changed')
        with previous.locked() as fd:
            _,state=previous.read(fd)
            require(previous.stopped(fd) and state['phase']=='ready' and state['active'] is None,'predecessor_not_idle_stopped')
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
        def http(path,body,token=None):
            headers={'Content-Type':'application/json'}
            if token:headers['Authorization']='Bearer '+token
            req=urllib.request.Request('http://127.0.0.1:8000'+path,data=json.dumps(body).encode(),headers=headers)
            with opener.open(req,timeout=20) as response:return json.load(response)
        login=http('/api/auth/login',r['auth'])
        require(login.get('success') is True,'normal_auth_failed')
        token=login['data']['access_token']
        now=datetime.now(BJ)
        require(now.date().isoformat()==r['opening_date'],'opening_date_changed')
        policy=dict(schema=1,codes=codes,min_date=r['min_date'],max_date='2026-11-03',
            expires_at=datetime(2026,11,3,14,tzinfo=BJ).timestamp(),max_requests=72000,daily_requests=12000,
            minute_requests=60,write_enabled=True)
        require(time.time()<policy['expires_at'],'opening_expired')
        RUN.mkdir(mode=0o700);reserved=True
        hashes={'history-before.bson':save('history-before.bson',raw),'indexes.json':save('indexes.json',col.index_information()),
            'policy.json':save('policy.json',policy),'previous-active.json':save('previous-active.json',Path('/app/runtime/tushare-history-generations/active.json').read_bytes())}
        save('backup-manifest.json',dict(hashes=hashes,count=len(rows),source_hashes=r['source_hashes']))
        os.environ.update(env)
        async def activate():
            from motor.motor_asyncio import AsyncIOMotorClient
            from app.core.database import _store_mongo_loop_binding,get_mongo_db
            motor=AsyncIOMotorClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],
                authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=10000)
            try:
                _store_mongo_loop_binding(asyncio.get_running_loop(),motor,motor[env['MONGODB_DATABASE']])
                return await activate_generation(policy,get_mongo_db().stock_daily_quotes)
            finally:motor.close()
        gate=asyncio.run(activate())
        save('generation.json',{'directory':str(gate.directory)})
        # Reserve before sending. Lost HTTP output is never automatically retried.
        save('trigger-reserved.json',dict(job='tushare_historical_sync',at=time.time()))
        report['trigger_sent']=True
        response=http('/api/scheduler/jobs/tushare_historical_sync/trigger',{},token)
        save('trigger-response.json',response)
        require(response.get('success') is True,'original_button_rejected')
        gate.status();gate.assert_selected()
        report.update(status='passed',new_gate_stopped=False,generation_directory=str(gate.directory),
            pool_count=len(codes),min_date=policy['min_date'],expires_at=policy['expires_at'],
            minute_requests=60,daily_requests=12000,max_requests=72000,
            execution_status='submitted',backup_directory=str(RUN),backup_sha256=hashes['history-before.bson'])
    except BaseException as error:
        report.update(error_type=type(error).__name__,reason=str(error) if isinstance(error,RuntimeError) else 'details_withheld')
    finally:
        if report['status']!='passed' and gate is not None:
            gate.stop();report['new_gate_stopped']=(gate.directory/'STOP').is_file()
        if client is not None:client.close()
        if reserved:save('result.json',report)
        print('BUTTON_ACCEPTANCE_JSON:'+json.dumps(report))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main(json.load(sys.stdin)))
