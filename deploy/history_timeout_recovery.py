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
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument

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


def raw_history(db,name,backup=False):
    """Stream exact BSON and its readback, without holding the market in RAM."""
    collection=db.get_collection(name,codec_options=CodecOptions(document_class=RawBSONDocument))
    stream=None;h=hashlib.sha256();count=0
    try:
        if backup:
            fd=os.open(RUN/(name+'.bson'),os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            stream=os.fdopen(fd,'wb')
        for row in collection.find({'data_source':'tushare'}).sort('_id',1).batch_size(500).max_time_ms(30000):
            h.update(row.raw);count+=1
            if stream is not None:stream.write(row.raw)
        if stream is not None:stream.flush();os.fsync(stream.fileno())
    finally:
        if stream is not None:stream.close()
    if backup:
        readback=hashlib.sha256()
        with (RUN/(name+'.bson')).open('rb') as f:
            for chunk in iter(lambda:f.read(1024*1024),b''):readback.update(chunk)
        require(readback.hexdigest()==h.hexdigest(),'backup_readback_failed')
    return dict(count=count,sha256=h.hexdigest())


def main(r, *, recoverer=None, expected_reason='timeout', raw_count=13136,
         run_directory=None, scheduled_recovery=False, check_only=False):
    global RUN
    if run_directory is not None:
        RUN=Path(run_directory)
    os.umask(0o077)
    gate=None;client=None;reserved=False
    report=dict(status='failed',stage='preflight',trigger_sent=False,new_gate_stopped=True)
    try:
        require(expected_reason in ('timeout','tls_failed','network_failed','http_error')
                and (expected_reason=='timeout' or recoverer is not None),'invalid_recovery_reason')
        require(not check_only or scheduled_recovery,'invalid_check_mode')
        require(not scheduled_recovery or expected_reason in ('network_failed','http_error'),'invalid_scheduled_recovery')
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
            require((db.scheduler_preferences.find_one({'_id':job}) or {}).get('enabled')
                    is (scheduled_recovery and job=='tushare_historical_sync'),'scheduled_job_enabled')
        from tradingagents.utils.tushare_batch import selected_gate,recover_raw_timeout
        gate=selected_gate()
        require(str(gate.directory)==r['generation'],'generation_changed')
        with gate.locked() as fd:
            _,s=gate.read(fd)
            require(gate.stopped(fd) and s['phase']=='blocked' and s['reason']==expected_reason
                    and s['used']==r['used'] and s['plan']['plan_sha256']==r['plan'],'state_changed')
        baseline=None
        if scheduled_recovery:
            require(s['active']==r['active'],'attempt_changed')
            from app.services.tushare_history_schedule import PERMIT, CRON, TIMEZONE
            permit=json.loads(PERMIT.read_text())
            policy=json.loads((gate.directory/'policy.json').read_text())
            from tradingagents.utils.tushare_batch import sha as policy_sha
            require(permit['generation']==str(gate.directory) and permit['policy_sha256']==policy_sha(policy)
                    and permit['cron']==CRON and permit['timezone']==TIMEZONE
                    and permit['expires_at']==policy['expires_at']>time.time(),'schedule_binding_changed')
            require(policy['minute_requests']==40 and policy['daily_requests']==7500
                    and policy['max_requests']==40000 and len(policy['codes'])==5577
                    and policy['schema']==2 and policy['write_enabled'] is True
                    and policy['min_date']=='2026-09-30' and policy['max_date']=='2026-11-06'
                    and policy['expires_at']==1794117600,'policy_changed')
            latest=db.scheduler_executions.find_one({'job_id':'tushare_historical_sync'},sort=[('timestamp',-1)])
            require(latest and str(latest['_id'])==r['execution'] and latest.get('is_manual') is False
                    and latest.get('status')=='partial' and latest.get('plan_sha256')==r['plan'],'execution_changed')
            for job in ('tushare_basic_info_sync','tushare_financial_sync','tushare_status_check'):
                require((db.scheduler_preferences.find_one({'_id':job}) or {}).get('enabled') is False,'other_job_enabled')
            require(Path('/app/runtime/tushare-egress/STOP').is_file(),'legacy_stop_missing')
            protected=[Path(gate.selection[0])/'active.json',gate.directory/'state.json',
                gate.directory/'policy.json',gate.directory/'STOP',PERMIT,Path('/app/runtime/tushare-egress/STOP')]
            baseline={'files':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in protected},'history':{}}
            for name,expected in [('stock_daily_quotes',2112),('stock_daily_quotes_unadjusted',raw_count)]:
                actual=raw_history(db,name);require(actual['count']==expected,'inventory_changed')
                baseline['history'][name]=actual
            require(check_only or baseline==r['baseline'],'recovery_baseline_changed')
        os.environ.update(env)
        async def recover(readonly=False):
            from motor.motor_asyncio import AsyncIOMotorClient
            motor=AsyncIOMotorClient(**options)
            try:
                kwargs=dict(expected_plan=r['plan'],expected_used=r['used'])
                if scheduled_recovery:kwargs.update(expected_active=r['active'],check_only=readonly)
                return await (recoverer or recover_raw_timeout)(gate,
                    motor[env['MONGODB_DATABASE']].stock_daily_quotes_unadjusted,**kwargs)
            finally:motor.close()
        if check_only:
            receipt=asyncio.run(recover(True))
            require(all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h
                        for p,h in baseline['files'].items()),'state_changed_during_check')
            report.update(status='checked',stage='ready_no_recovery',baseline=baseline,recovery=receipt,
                          upstream_requests=0,new_gate_stopped=True)
            return 0
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
            if scheduled_recovery:
                actual=raw_history(db,name,backup=True)
                require(actual==baseline['history'][name],'history_changed_during_backup')
                hashes[name+'.bson']=actual['sha256'];before[name]=None
            else:
                rows=list(db[name].find({'data_source':'tushare'}).max_time_ms(15000))
                require(len(rows)==expected,'inventory_changed')
                raw=b''.join(BSON.encode(row) for row in sorted(rows,key=lambda x:str(x['_id'])))
                hashes[name+'.bson']=save(name+'.bson',raw);before[name]=rows
            hashes[name+'-indexes.json']=save(name+'-indexes.json',db[name].index_information())
        # Preserve every old ledger byte, including STOP, before modifying just one item.
        ledger={str(p.relative_to(gate.directory)):p.read_bytes().hex() for p in gate.directory.rglob('*') if p.is_file()}
        hashes['ledger-before.json']=save('ledger-before.json',ledger)
        save('backup-manifest.json',dict(hashes=hashes,source_hashes=r['source_hashes'],plan=r['plan'],used=r['used']))
        if scheduled_recovery:
            require(all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h
                        for p,h in baseline['files'].items()),'state_changed_during_backup')
            require(all(hashes[name+'.bson']==v['sha256'] for name,v in baseline['history'].items()),
                    'backup_does_not_match_checked_raw_bson')
        report['stage']='reconcile'
        receipt=asyncio.run(recover());save('recovery.json',receipt)
        report['stage']='verify_data'
        # No writes should have occurred during recovery; preserve exact BSON proof.
        for name,rows in before.items():
            if scheduled_recovery:
                require(raw_history(db,name)==baseline['history'][name],'data_changed_during_recovery')
            else:
                current=list(db[name].find({'data_source':'tushare'}).max_time_ms(15000))
                raw=b''.join(BSON.encode(row) for row in sorted(current,key=lambda x:str(x['_id'])))
                require(hashlib.sha256(raw).hexdigest()==hashes[name+'.bson'],'data_changed_during_recovery')
        if scheduled_recovery:
            with gate.locked() as fd:
                _,current=gate.read(fd)
                require(current==dict(s,active=None,phase='ready',reason='none',api_code=None)
                        and not gate.stopped(fd),'recovered_state_changed')
            require(all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h
                        for p,h in baseline['files'].items()
                        if p not in (str(gate.directory/'state.json'),str(gate.directory/'STOP'))),
                    'protected_permission_changed')
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
        if not check_only and report['status']!='passed' and gate is not None:gate.stop()
        if client is not None:client.close()
        if reserved:save('result.json',report)
        print('BUTTON_ACCEPTANCE_JSON:'+json.dumps(report))
    return 0 if report['status']=='passed' else 1

if __name__=='__main__':raise SystemExit(main(json.load(sys.stdin)))
