"""Explicit one-shot recovery of the saved 2026-10-29 button response.

No permit creation, provider import, journal reset, or task-status rewriting.
All writes require mode=execute; check mode only reads. Failed writes never retry.
"""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import types

PLAN='2ce5c8123249fd51c98493af3318434f29845519c41b36752722f8a1d0394d5e'
PAYLOAD='cf6eaa4c5f164d05458c7db22e0fbc7916fbee31046dd5a9741f1cb86dfe2bb8'
SERIES_SHA='d2f58cb0006bb0abbe72548779dea8f8fe2a63dc862d5782d4a917ced37b929f'
OTHER_SHA='811ce8d0cde35f23318b88d76d05d53c320c2e005b45755242d6013a368050e8'
GATE=Path('/app/runtime/tushare-history-batch-v1')
RUN=Path('/app/runtime/history-button-saved-recovery-20261029-v1')
DAYS=['2026-10-27','2026-10-28']


def require(value,reason):
    if not value:raise RuntimeError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def bson_rows(rows):
    from bson import BSON
    return b''.join(BSON.encode(r) for r in sorted(rows,key=lambda r:str(r['_id'])))


def save(name,value):
    raw=value if isinstance(value,bytes) else json.dumps(value,sort_keys=True,default=str).encode()
    fd=os.open(RUN/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    fd=os.open(RUN,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)
    require((RUN/name).read_bytes()==raw,'backup_readback_failed')


async def recover(adapter,item,payload,reserve):
    """No fetch capability used; reserve durably before the sole write call."""
    require(await adapter.validate(item,payload),'saved_payload_invalid')
    require(await adapter.preflight(item)==item['observation_sha256'],'inventory_changed')
    await reserve()
    await adapter.write(item,payload)
    require(await adapter.matches(item,payload),'recovery_write_not_confirmed')


def network_guard(host,port):
    addresses={a[4][0] for a in socket.getaddrinfo(host,port,type=socket.SOCK_STREAM)}
    def audit(event,args):
        if event=='socket.getaddrinfo':
            require(args[0] in addresses|{host} and args[1]==port,'non_database_dns_forbidden')
        elif event=='socket.connect':
            address=args[1]
            require(isinstance(address,tuple) and address[0] in addresses and address[1]==port,'non_database_network_forbidden')
        elif event=='socket.sendto':raise RuntimeError('datagram_forbidden')
        elif event=='import' and args[0].startswith('tradingagents.dataflows.providers'):
            raise RuntimeError('provider_import_forbidden')
    sys.addaudithook(audit)


async def main(request):
    os.umask(0o077)
    reserved=False;client=None
    report=dict(status='failed',upstream_requests=0,write_attempted=False,automatic_retry=False)
    try:
        require(request['mode'] in ('check','execute'),'invalid_mode')
        require(not RUN.exists(),'recovery_already_reserved')
        for p,digest in request['source_hashes'].items():
            require(sha(Path(p).read_bytes())==digest,'installed_source_changed')
        def protected():return {p:sha(Path(p).read_bytes()) for p in request['protected_hashes']}
        require(protected()==request['protected_hashes'],'protected_gate_changed')
        require((GATE/'STOP').is_file(),'batch_stop_missing')
        rt=types.ModuleType('runtime');exec(request['runtime_tool'],rt.__dict__)
        env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py',
            'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
        require(all(not v['enabled'] for v in effective['gates'].values()),'quote_gate_enabled')
        require(env.get('TUSHARE_UNIFIED_ENABLED')=='false','legacy_tushare_enabled')
        os.environ.update(env)
        network_guard(env['MONGODB_HOST'],int(env['MONGODB_PORT']))
        from motor.motor_asyncio import AsyncIOMotorClient
        from pymongo.write_concern import WriteConcern
        client=AsyncIOMotorClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),
            username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],
            authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,
            serverSelectionTimeoutMS=10000,socketTimeoutMS=10000,connectTimeoutMS=10000,
            w='majority',journal=True,wTimeoutMS=10000)
        db=client[env['MONGODB_DATABASE']]
        # Install a loop binding, not app startup: no indexes, tasks, Redis or provider.
        from app.core.database import _store_mongo_loop_binding,get_mongo_db
        _store_mongo_loop_binding(asyncio.get_running_loop(),client,db)
        from app.services.historical_data_service import HistoricalDataService
        from deploy.history_batch_adapters import HistoricalAdapter
        from deploy.history_batch_journal import Workspace,digest
        from tradingagents.utils.tushare_batch import BatchGate
        gate=BatchGate(GATE)
        with gate.locked() as fd:
            policy,ledger=gate.read(fd)  # read despite STOP; NEVER call status/authorize
        require(ledger['used']==2 and ledger['active'] is None and ledger['phase']=='ready','ledger_changed')
        require(ledger['plan']['plan_sha256']==PLAN,'plan_changed')
        ws=Workspace(GATE/'runs')
        with ws.locked():
            _,plan,states=ws._load(PLAN)
            state=states['600000.SH'];item=plan['items'][0];payload=state['payload']
            require(state['phase']=='writing' and state['fetch_attempts']==1,'journal_changed')
            require(digest(payload)==PAYLOAD==state['payload_sha256'],'payload_changed')
        require(len(plan['items'])==1 and item['code']=='600000.SH' and item['start']==DAYS[0] and item['end']==DAYS[-1],'plan_scope')
        require(sorted(d['trade_date'] for d in payload['documents'])==DAYS,'document_scope')
        require(len(payload['documents'])==2,'too_many_rows')
        # Preserve every old journal/gate byte, including lock-file contents.
        gate_hashes={str(p):sha(p.read_bytes()) for p in GATE.rglob('*') if p.is_file()}
        async def assert_idle():
            for job in ('tushare_historical_sync','tushare_realtime_quotes_hourly'):
                require((await db.scheduler_preferences.find_one({'_id':job}) or {}).get('enabled') is False,'task_enabled')
            require(await db.scheduler_executions.count_documents({'job_id':{'$regex':'^tushare_'},'status':{'$in':['running','cancelling']}})==0,'task_active')
        await assert_idle()
        col=get_mongo_db().stock_daily_quotes
        rows=await col.find({'data_source':'tushare'}).to_list(length=20001)
        before=[r for r in rows if r.get('symbol')=='600000' and r.get('period')=='daily']
        other=[r for r in rows if r['_id'] not in {x['_id'] for x in before}]
        require(len(before)==5 and len(other)==2089,'record_count_changed')
        require(sha(bson_rows(before))==SERIES_SHA and sha(bson_rows(other))==OTHER_SHA,'baseline_changed')
        indexes=await col.index_information()
        async def forbidden(*a,**k):raise RuntimeError('fetch_forbidden')
        service=HistoricalDataService()
        adapter=HistoricalAdapter(col,service,forbidden,forbidden,forbidden)
        require(await adapter.validate(item,payload),'payload_contract_invalid')
        require(await adapter.preflight(item)==item['observation_sha256'],'inventory_changed')
        # Exercise actual proxy resolution before reserving or attempting writes.
        await service.validate_tushare_series('600000','daily','CN','qfq')
        from deploy.history_batch_adapters import same_collection
        require(same_collection(service.db.stock_daily_quotes,col),'service_collection_mismatch')
        report.update(plan_sha256=PLAN,payload_sha256=PAYLOAD,baseline_verified=True,documents=2)
        if request['mode']=='check':
            report['status']='preflight_passed'
        else:
            require(request.get('approved') is True,'recovery_not_approved')
            RUN.mkdir(mode=0o700);reserved=True
            save('series-before.bson',bson_rows(before));save('other-before.bson',bson_rows(other))
            save('indexes.json',indexes);save('frozen-payload.json',payload)
            save('manifest.json',dict(plan_sha256=PLAN,payload_sha256=PAYLOAD,gate_hashes=gate_hashes,
                protected_hashes=request['protected_hashes'],source_hashes=request['source_hashes']))
            async def reserve():
                await assert_idle()
                require(protected()==request['protected_hashes'],'old_gates_changed')
                require({str(p):sha(p.read_bytes()) for p in GATE.rglob('*') if p.is_file()}==gate_hashes,'batch_gate_changed')
                save('write-reserved.json',{'utc':datetime.now(timezone.utc).isoformat(),'maximum_rows':2})
                report['write_attempted']=True
            await recover(adapter,item,payload,reserve)
            after=await col.find({'data_source':'tushare'}).to_list(length=20001)
            after_series=[r for r in after if r.get('symbol')=='600000' and r.get('period')=='daily']
            after_other=[r for r in after if r['_id'] not in {x['_id'] for x in after_series}]
            require(len(after_series)==6 and bson_rows(after_other)==bson_rows(other),'unrelated_data_changed')
            require(await col.index_information()==indexes,'indexes_changed')
            require(protected()==request['protected_hashes'],'old_gates_changed')
            require({str(p):sha(p.read_bytes()) for p in GATE.rglob('*') if p.is_file()}==gate_hashes,'batch_gate_changed')
            await assert_idle()
            save('series-after.bson',bson_rows(after_series))
            report.update(status='passed',new_rows=1,series_count=6,other_unchanged=True,old_journal_unchanged=True,
                failed_execution_preserved=True,all_stops_preserved=True)
    except BaseException as error:
        report.update(error_type=type(error).__name__,reason=str(error) if isinstance(error,RuntimeError) else 'details_withheld')
    finally:
        if client is not None:client.close()
        if reserved:save('result.json',report)
        print('RECOVERY_JSON:'+json.dumps(report,default=str))
    return 0 if report['status'] in ('passed','preflight_passed') else 1


if __name__=='__main__':raise SystemExit(asyncio.run(main(json.load(sys.stdin))))
