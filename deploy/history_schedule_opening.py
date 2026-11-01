"""Explicit operator-only schedule activation after the code/transport checks.

No data requests here: backups and reconciliation precede permission; the normal
resume API arms the first real APScheduler cron execution. Never resets counters.
"""
import asyncio
from datetime import datetime,timedelta,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import types
import urllib.request

from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

RUN=Path('/app/runtime/history-schedule-opening-v1-20261102')
PERMIT=Path('/app/runtime/tushare-history-schedule.json')
BJ=timezone(timedelta(hours=8))
EXPIRES=datetime(2026,11,8,14,tzinfo=BJ).timestamp()


def require(condition,reason):
    if not condition:raise RuntimeError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def save(name,value):
    path=RUN/name;path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    raw=value if isinstance(value,bytes) else json.dumps(value,sort_keys=True,default=str).encode()
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    require(path.read_bytes()==raw,'backup_readback_failed')
    return sha(raw)


def snapshot(db,name,expected):
    col=db.get_collection(name,codec_options=CodecOptions(document_class=RawBSONDocument))
    path=RUN/(name+'.bson');fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    h=hashlib.sha256();count=0
    with os.fdopen(fd,'wb') as stream:
        for row in col.find({'data_source':'tushare'}).sort('_id',1).batch_size(500).max_time_ms(30000):
            stream.write(row.raw);h.update(row.raw);count+=1
        stream.flush();os.fsync(stream.fileno())
    actual=dict(count=count,sha256=h.hexdigest())
    require(actual==expected,'history_baseline_changed:'+name)
    require(sha(path.read_bytes())==actual['sha256'],'history_backup_readback_failed')
    save(name+'-indexes.json',db[name].index_information())
    return actual


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise RuntimeError('redirect_refused')


def main(r):
    os.umask(0o077);client=None;gate=None;fd=None;reserved=False;resume_reserved=False
    report=dict(status='failed',schedule_enabled=False,upstream_requests=0)
    try:
        require(not RUN.exists() and not PERMIT.exists(),'opening_already_reserved')
        require(shutil.disk_usage('/app/runtime').free>=3*1024**3,'backup_space_insufficient')
        for path,digest in r['source_hashes'].items():require(sha(Path(path).read_bytes())==digest,'source_drift')
        rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
        env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
        require(all(not v['enabled'] for v in effective['gates'].values()),'quote_gate_enabled')
        require(str(env.get('TUSHARE_UNIFIED_ENABLED')).lower()=='false','legacy_tushare_enabled')
        require(Path('/app/runtime/tushare-egress/STOP').is_file(),'legacy_STOP_missing')
        client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=10000,socketTimeoutMS=30000)
        db=client[env['MONGODB_DATABASE']]
        from tradingagents.utils.tushare_batch import selected_gate,activate_generation,sha as policy_sha
        from tradingagents.utils.tushare_egress import _private_fd
        from tradingagents.utils.tushare_manual import JOB_IDS
        from app.services.tushare_history_schedule import CRON,TIMEZONE,load_schedule
        from apscheduler.triggers.cron import CronTrigger
        from deploy.history_batch_journal import Workspace
        previous=selected_gate();previous.assert_selected()
        fd=os.open(previous.admission_path,os.O_RDWR|os.O_NOFOLLOW);_private_fd(fd)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        policy,state=previous.status()
        require(str(previous.directory)==r['previous_directory'] and policy_sha(policy)==r['previous_policy_sha256'],'predecessor_changed')
        require(state['used']==5597 and state['active'] is None and state['plan']['plan_sha256']==r['previous_plan'],'predecessor_state_changed')
        require(Workspace(previous.directory/'runs').status(r['previous_plan'])['complete'],'previous_plan_incomplete')
        for job_id in JOB_IDS:
            require((db.scheduler_preferences.find_one({'_id':job_id}) or {}).get('enabled') is False,'scheduled_job_enabled')
        require(db.scheduler_executions.count_documents({'job_id':{'$regex':'^tushare_'},'status':{'$in':['running','cancelling']}})==0,'execution_active')
        latest=db.scheduler_executions.find_one({'job_id':'tushare_historical_sync'},sort=[('timestamp',-1)])
        require(latest and str(latest['_id'])==r['previous_execution'] and latest['status']=='success','previous_result_changed')
        from deploy.history_sync_plan import _code
        codes=set();excluded=set()
        for row in db.stock_basic_info.find({}, {'symbol':1,'code':1,'ts_code':1,'list_status':1}):
            code=_code(row.get('ts_code') or row.get('symbol') or row.get('code'))
            (excluded if row.get('list_status')=='D' else codes).add(code)
        require(sorted(codes-excluded)==policy['codes'],'stock_pool_changed')
        now=datetime.now(BJ)
        due=CronTrigger.from_crontab(CRON,timezone=TIMEZONE).get_next_fire_time(None,now)
        require(600<(due-now).total_seconds() and due.timestamp()<EXPIRES and now.date().isoformat()<='2026-11-06','schedule_window_invalid')
        new_policy=dict(policy,schema=2,min_date='2026-09-30',max_date='2026-11-06',expires_at=EXPIRES,max_requests=40000,daily_requests=7500,minute_requests=40,write_enabled=True)
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
        def http(path,body=None,token=None):
            headers={'Content-Type':'application/json'}
            if token:headers['Authorization']='Bearer '+token
            request=urllib.request.Request('http://127.0.0.1:8000'+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
            with opener.open(request,timeout=20) as response:return json.load(response)
        login=http('/api/auth/login',r['auth']);require(login.get('success') is True,'normal_auth_failed')
        token=login['data']['access_token']
        RUN.mkdir(mode=0o700);reserved=True
        backups={name:snapshot(db,name,expected) for name,expected in r['history_bson'].items()}
        save('previous-preferences.json',list(db.scheduler_preferences.find({'_id':{'$in':list(JOB_IDS)}})))
        save('previous-execution.json',latest);save('new-policy.json',new_policy)
        registry=Path('/app/runtime/tushare-history-generations')
        shutil.copytree(registry,RUN/'generation-backup')
        tree={str(p.relative_to(registry)):sha(p.read_bytes()) for p in registry.rglob('*') if p.is_file()}
        require(tree=={str(p.relative_to(RUN/'generation-backup')):sha(p.read_bytes()) for p in (RUN/'generation-backup').rglob('*') if p.is_file()},'ledger_backup_mismatch')
        save('backup-manifest.json',dict(history=backups,ledger_files=tree,completed=True))
        previous.stop();save('previous-stopped.json',dict(directory=str(previous.directory),at=time.time()))
        os.close(fd);fd=None
        os.environ.update(env)
        async def activate():
            from motor.motor_asyncio import AsyncIOMotorClient
            motor=AsyncIOMotorClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=10000,socketTimeoutMS=30000)
            try:return await activate_generation(new_policy,motor[env['MONGODB_DATABASE']].stock_daily_quotes_unadjusted)
            finally:motor.close()
        gate=asyncio.run(activate())
        selected,state_after=gate.status()
        require(selected==new_policy,'activated_policy_mismatch')
        require(all(state_after[k]==state[k] for k in ('used','days','recent','last_time')),'counter_carry_failed')
        save('generation.json',dict(directory=str(gate.directory),policy_sha256=policy_sha(new_policy),used=state_after['used'],days=state_after['days']))
        now=datetime.now(BJ)
        due=CronTrigger.from_crontab(CRON,timezone=TIMEZONE).get_next_fire_time(None,now)
        require(600<(due-now).total_seconds() and due.timestamp()<EXPIRES,'activation_missed_safe_window')
        permit=dict(schema=1,generation=str(gate.directory),policy_sha256=policy_sha(new_policy),cron=CRON,timezone=TIMEZONE,expires_at=EXPIRES)
        permit_fd=os.open(PERMIT,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(permit_fd,'w') as stream:json.dump(permit,stream,sort_keys=True);stream.flush();os.fsync(stream.fileno())
        directory_fd=os.open(PERMIT.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory_fd)
        finally:os.close(directory_fd)
        require(load_schedule(gate)==permit,'schedule_permit_invalid')
        save('resume-reserved.json',dict(at=time.time(),due=due.isoformat()));resume_reserved=True
        response=http('/api/scheduler/jobs/tushare_historical_sync/resume',{},token)
        require(response.get('success') is True,'resume_rejected')
        job=http('/api/scheduler/jobs/tushare_historical_sync',token=token)['data']
        require(job['paused'] is False and job['next_run_time'] and job['max_instances']==1 and job['misfire_grace_time']==300,'schedule_not_enabled')
        next_run=datetime.fromisoformat(job['next_run_time'].replace('Z','+00:00'))
        require(next_run==due and next_run.astimezone(BJ).hour==18 and next_run.astimezone(BJ).minute==30,'next_run_mismatch')
        for job_id in set(JOB_IDS)-{'tushare_historical_sync'}:
            require((db.scheduler_preferences.find_one({'_id':job_id}) or {}).get('enabled') is False,'other_job_enabled')
        save('schedule-job.json',job)
        report.update(status='armed',schedule_enabled=True,next_run_time=next_run.isoformat(),expires_at=EXPIRES,
            generation=str(gate.directory),used=state_after['used'],pool_count=len(policy['codes']),
            daily_requests=7500,minute_requests=40,max_requests=40000,real_scheduled_acceptance='pending_first_cron_completion')
    except BaseException as error:
        report.update(error_type=type(error).__name__,reason=str(error) if isinstance(error,RuntimeError) else 'details_withheld')
        if gate is not None:gate.stop()
        if resume_reserved:
            try:http('/api/scheduler/jobs/tushare_historical_sync/pause',{},token)
            except Exception:report['pause_unconfirmed']=True
    finally:
        if fd is not None:os.close(fd)
        if client is not None:client.close()
        if reserved:save('result.json',report)
        print('SCHEDULE_OPENING_JSON:'+json.dumps(report))
    return 0 if report['status']=='armed' else 1


if __name__=='__main__':raise SystemExit(main(json.load(__import__('sys').stdin)))
