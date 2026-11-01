"""Read-only acceptance after the first real cron (run inside backend).

Original BSON stays in production. Only counts, outcome and match verdicts print.
Does not fetch data, enable a job, repair a record or recover a stopped permit.
"""
import hashlib
import json
from pathlib import Path
import struct
import sys
import types

from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

OPENING=Path('/app/runtime/history-schedule-opening-v1-20261102')


def preserved(collection,path,expected):
    h=hashlib.sha256();count=0;batch={}
    def verify():
        found={row['_id']:row.raw for row in collection.find({'_id':{'$in':list(batch)}}).max_time_ms(15000)}
        if found!=batch:raise RuntimeError('original_record_changed')
        batch.clear()
    with path.open('rb') as stream:
        while True:
            header=stream.read(4)
            if not header:break
            if len(header)!=4:raise RuntimeError('backup_truncated')
            length=struct.unpack('<i',header)[0]
            if not 5<=length<=16*1024*1024:raise RuntimeError('backup_invalid')
            raw=header+stream.read(length-4)
            if len(raw)!=length:raise RuntimeError('backup_truncated')
            row=RawBSONDocument(raw)
            if row['_id'] in batch:raise RuntimeError('duplicate_backup_id')
            batch[row['_id']]=raw;h.update(raw);count+=1
            if len(batch)==500:verify()
        if batch:verify()
    if dict(count=count,sha256=h.hexdigest())!=expected:raise RuntimeError('backup_manifest_mismatch')
    return count


def main(r):
    client=None
    try:
        rt=types.ModuleType('runtime');exec(r['runtime'],rt.__dict__)
        env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
        if not all(not g['enabled'] for g in effective['gates'].values()):raise RuntimeError('quote_gate_open')
        from tradingagents.utils.tushare_batch import selected_gate
        from deploy.history_batch_journal import Workspace
        gate=selected_gate();_,state=gate.status()
        client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=10000,socketTimeoutMS=30000)
        db=client[env['MONGODB_DATABASE']]
        plan=state['plan']
        if plan is None:raise RuntimeError('first_cron_not_started')
        row=db.scheduler_executions.find_one({'job_id':'tushare_historical_sync','plan_sha256':plan['plan_sha256'],'is_manual':False},sort=[('timestamp',-1)])
        if not row or row.get('status')!='success' or row.get('result',{}).get('complete') is not True:raise RuntimeError('first_cron_not_successful_yet')
        if state['used']<=5597 or state['active'] is not None or not Workspace(gate.directory/'runs').status(plan['plan_sha256'])['complete']:raise RuntimeError('scheduled_plan_not_complete')
        manifest=json.loads((OPENING/'backup-manifest.json').read_text())
        originals={}
        for name,expected in manifest['history'].items():
            originals[name]=preserved(db.get_collection(name,codec_options=CodecOptions(document_class=RawBSONDocument)),OPENING/(name+'.bson'),expected)
        raw=db.stock_daily_quotes_unadjusted
        count=raw.count_documents({'data_source':'tushare'})
        last=raw.find_one({'data_source':'tushare'},sort=[('trade_date',-1)])
        if count<=originals['stock_daily_quotes_unadjusted'] or last['trade_date']<plan['end']:raise RuntimeError('scheduled_end_not_ingested')
        report=dict(status='passed',execution_id=str(row['_id']),is_manual=False,plan_sha256=plan['plan_sha256'],end=plan['end'],success_count=row.get('success_count'),skipped_items=row.get('skipped_items'),used=state['used'],originals_preserved=originals,raw_count=count,latest_trade_date=last['trade_date'])
    except Exception as error:
        report=dict(status='not_accepted',reason=str(error) if isinstance(error,RuntimeError) else 'details_withheld')
    finally:
        if client is not None:client.close()
    print('SCHEDULE_ACCEPTANCE_JSON:'+json.dumps(report))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main(json.load(sys.stdin)))
