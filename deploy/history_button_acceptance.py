"""One authorized real HTTP trigger; production-only backup, bounded policy, fail closed.

Invoked by detached run_history_button_acceptance. No credentials in artifacts.
Never repeats the trigger, renews policy, or rolls back business records.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import types
import urllib.request
import urllib.error

from bson import BSON
from pymongo import MongoClient

RUN = Path('/app/runtime/history-button-acceptance-20261029-v1')
GATE = Path('/app/runtime/tushare-history-batch-v1')
JOB = 'tushare_historical_sync'
SERIES = dict(symbol='600000', data_source='tushare', period='daily')
DAYS = ['2026-10-27', '2026-10-28']
BJ = timezone(timedelta(hours=8))
ENTRY = 'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b'


def require(value, code):
    if not value:
        raise RuntimeError(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bson_rows(rows):
    return b''.join(BSON.encode(r) for r in sorted(rows, key=lambda r: str(r['_id'])))


def save(name, raw):
    if not isinstance(raw, bytes):
        raw = json.dumps(raw, sort_keys=True, default=str).encode()
    fd = os.open(RUN/name, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    fd = os.open(RUN, os.O_RDONLY|os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
    require((RUN/name).read_bytes() == raw, 'backup_readback_failed')
    return sha(raw)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('redirect_refused')


def main(request):
    os.umask(0o077)
    report = dict(status='failed', trigger_sent=False, automatic_retry=False, automatic_rollback=False)
    client = None
    reserved = False
    owns_gate = False
    token = None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def http(path, body=None):
        headers = {'Content-Type':'application/json'}
        if token: headers['Authorization'] = 'Bearer '+token
        req = urllib.request.Request('http://127.0.0.1:8000'+path,
            data=json.dumps(body).encode() if body is not None else None, headers=headers)
        try:
            with opener.open(req, timeout=20) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def protected():
        return {p:sha(Path(p).read_bytes()) for p in request['protected_hashes']}

    def date_check():
        now = datetime.now(BJ)
        end = (now.date() if now.hour>=18 else now.date()-timedelta(days=1)).isoformat()
        require(end == DAYS[-1], 'authorized_end_date_changed')

    try:
        date_check()
        for path, digest in request['source_hashes'].items():
            require(sha(Path(path).read_bytes()) == digest, 'installed_source_changed')
        require(not GATE.exists() and not RUN.exists(), 'acceptance_or_gate_already_exists')
        require(protected() == request['protected_hashes'], 'old_gate_changed')
        rt = types.ModuleType('runtime'); exec(request['runtime_tool'], rt.__dict__)
        env, effective = rt.reconstruct('/app/deploy/migration_production_entrypoint.py', ENTRY, '/run/secrets')
        require(all(not v['enabled'] for v in effective['gates'].values()), 'quote_gate_enabled')
        require(str(env.get('TUSHARE_UNIFIED_ENABLED')).lower() == 'false', 'legacy_tushare_enabled')
        client = MongoClient(host=env['MONGODB_HOST'], port=int(env['MONGODB_PORT']),
            username=env['MONGODB_USERNAME'], password=env['MONGODB_PASSWORD'],
            authSource=env.get('MONGODB_AUTH_SOURCE','admin'), serverSelectionTimeoutMS=10000,
            retryWrites=False, retryReads=False, socketTimeoutMS=10000, connectTimeoutMS=10000)
        db = client[env['MONGODB_DATABASE']]; col = db.stock_daily_quotes
        def idle():
            for job in ('tushare_historical_sync','tushare_realtime_quotes_hourly'):
                require((db.scheduler_preferences.find_one({'_id':job}) or {}).get('enabled') is False, 'legacy_task_enabled')
            require(db.scheduler_executions.count_documents({'job_id':{'$regex':'^tushare_'},
                'status':{'$in':['running','cancelling']}}) == 0, 'tushare_task_inflight')
        idle()
        all_before = list(col.find({'data_source':'tushare'}).max_time_ms(10000))
        before = list(col.find(SERIES).max_time_ms(10000))
        other = [r for r in all_before if r['_id'] not in {r['_id'] for r in before}]
        require(len(all_before)==2094 and len(before)==5 and len(other)==2089, 'baseline_count_drift')
        require(max(r['trade_date'] for r in before)==DAYS[0], 'cursor_changed')
        require(sha(bson_rows(before))==request['series_bson_sha256'], 'series_content_changed')
        require(sha(bson_rows(other))==request['other_bson_sha256'], 'other_history_changed')
        from app.services.historical_contract import validate_existing_contract
        validate_existing_contract(before,'qfq')
        indexes = col.index_information()
        from deploy.history_batch_adapters import UNIQUE_KEY
        require(any(v.get('unique') is True and v.get('key')==UNIQUE_KEY and not v.get('sparse')
            and not v.get('partialFilterExpression') for v in indexes.values()), 'unique_index_missing')
        status, login = http('/api/auth/login', request['auth'])
        require(status==200 and login.get('success') is True, 'normal_login_failed')
        token = login['data']['access_token']
        # Durable private backup is completed before any new grant or trigger.
        RUN.mkdir(mode=0o700); reserved=True
        hashes = {'series-before.bson':save('series-before.bson',bson_rows(before)),
                  'other-tushare-before.bson':save('other-tushare-before.bson',bson_rows(other)),
                  'indexes.json':save('indexes.json',indexes)}
        save('backup-manifest.json',dict(hashes=hashes, series_count=5, other_count=2089,
            cursor=DAYS[0], approved_dates=DAYS, source_hashes=request['source_hashes']))
        report['backup'] = dict(directory=str(RUN), hashes=hashes)
        date_check(); idle()
        require(bson_rows(list(col.find({'data_source':'tushare'})))==bson_rows(all_before),'pre_grant_data_changed')
        require(protected()==request['protected_hashes'],'pre_grant_old_gate_changed')
        from tradingagents.utils.tushare_batch import provision, BatchGate
        from deploy.history_batch_journal import Workspace
        policy = dict(schema=1,codes=['600000.SH'],min_date=DAYS[0],max_date=DAYS[-1],
            expires_at=time.time()+3600,max_requests=2,daily_requests=2,minute_requests=2,write_enabled=True)
        owns_gate=True  # also STOP a partially created gate if provisioning fails
        gate=provision(GATE,policy); Workspace(GATE/'runs').provision()
        save('approved-policy.json',policy)
        date_check()
        save('trigger-reserved.json',{'utc':datetime.now(timezone.utc).isoformat(),'method':'POST','job':JOB})
        report['trigger_sent']=True
        status, response = http('/api/scheduler/jobs/'+JOB+'/trigger', {})
        report['trigger_http_status']=status
        save('trigger-response.json',response)
        require(status==200 and response.get('success') is True,'original_button_rejected')
        deadline=time.monotonic()+180
        record=None
        while time.monotonic()<deadline:
            state=json.loads((GATE/'state.json').read_text())
            plan=state.get('plan')
            if plan:
                records=list(db.scheduler_executions.find({'job_id':JOB,'plan_sha256':plan['plan_sha256']}))
                require(len(records)<=1,'unexpected_multiple_executions')
                record=records[0] if records else None
                if record and record['status'] not in ('running','cancelling'):break
            time.sleep(1)
        # Stop immediately on terminal result/timeout, before further read-only verification.
        gate.stop(); report['new_gate_stopped']=(GATE/'STOP').is_file()
        state=json.loads((GATE/'state.json').read_text()); save('ledger-after.json',state)
        report['used_requests']=state['used']
        require(record is not None and record['status']=='success','task_not_successful')
        save('execution.json',record)
        plan=state['plan']; require(plan['pool']==['600000.SH'] and plan['end']==DAYS[-1], 'plan_scope')
        require(len(plan['items'])==1 and plan['items'][0]['start']==DAYS[0] and plan['items'][0]['end']==DAYS[-1], 'item_scope')
        require(state['used']==2 and state['active'] is None and state['phase']=='ready','request_accounting')
        workspace=Workspace(GATE/'runs'); checkpoint=workspace.status(plan['plan_sha256'])
        require(checkpoint['complete'],'journal_incomplete')
        with workspace.locked():
            _,_,states=workspace._load(plan['plan_sha256'])
            payload=states['600000.SH']['payload']
        docs=payload['documents']; require(sorted(d['trade_date'] for d in docs)==DAYS,'returned_dates')
        require(len(docs)==2 and all(d['volume_unit']=='lot' and d['amount_unit']=='CNY' for d in docs),'units_or_row_count')
        after=list(col.find(SERIES)); require(len(after)==6,'persisted_count')
        expected={d['trade_date']:d for d in docs}
        old={d['trade_date']:d for d in before}
        for row in after:
            day=row['trade_date']
            if day in expected:
                require({k:v for k,v in row.items() if k not in ('_id','created_at','updated_at')}==expected[day], 'stored_contract_mismatch')
                if day in old:
                    require(all(row.get(k)==old[day].get(k) for k in ('_id','created_at')), 'overlap_identity_changed')
                    business={k:v for k,v in old[day].items() if k not in ('_id','created_at','updated_at')}
                    if business==expected[day]:require(bson_rows([row])==bson_rows([old[day]]),'unchanged_overlap_not_idempotent')
            else:require(day in old and bson_rows([row])==bson_rows([old[day]]),'older_history_changed')
        after_other=list(col.find({'data_source':'tushare','_id':{'$nin':[r['_id'] for r in after]}}))
        require(bson_rows(other)==bson_rows(after_other),'unrelated_history_changed')
        require(col.index_information()==indexes,'indexes_changed')
        require(protected()==request['protected_hashes'],'old_gates_changed_after')
        idle()
        status, history=http('/api/scheduler/jobs/'+JOB+'/executions?limit=10')
        require(status==200 and history.get('success') is True,'history_api_failed')
        visible=history['data']['items']
        require(any(r.get('plan_sha256')==plan['plan_sha256'] and r.get('status')=='success' for r in visible),'success_not_visible_in_original_history')
        save('history-api.json',history)
        save('series-after.bson',bson_rows(after))
        report.update(status='passed',execution_id=str(record['_id']),execution_status=record['status'],
            plan_sha256=plan['plan_sha256'],dates=DAYS,saved_bars=2,new_bars=1,series_count=6,
            volume_unit='lot',amount_unit='CNY',other_history_unchanged=True,old_gates_unchanged=True,
            original_history_api_shows_success=True)
    except BaseException as error:
        report.update(status='failed',error_type=type(error).__name__,
            reason=str(error) if isinstance(error,RuntimeError) else 'details_withheld')
    finally:
        if owns_gate and GATE.is_dir():
            try:
                from tradingagents.utils.tushare_batch import BatchGate
                BatchGate(GATE).stop(); report['new_gate_stopped']=(GATE/'STOP').is_file()
            except BaseException: report['new_gate_stopped']=False
        if client is not None:client.close()
        if reserved:save('result.json',report)
        print('BUTTON_ACCEPTANCE_JSON:'+json.dumps(report,default=str))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':
    raise SystemExit(main(json.load(sys.stdin)))
