"""Real pandas/contracts/service + provider method bodies; no network or production DB."""
import asyncio
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, patch

from test_tushare_history import history, frame, provider, Collection, Cursor, DB_MODULE, ROOT
from app.services.historical_contract import tushare_documents
from deploy.history_batch_adapters import Inventory, HistoricalAdapter, raw_hash, raw_json
from deploy.history_sync_plan import build_plan
from deploy.history_batch_journal import Workspace, BatchStopped


def no_network(event, args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.sendto'):
        raise RuntimeError('OfflineAdapterTest')
sys.addaudithook(no_network)


class ReadCursor(Cursor):
    def max_time_ms(self, value):
        assert value == 10000
        return self


class ReadCollection(Collection):
    def find(self, query):
        return ReadCursor(super().find(query).rows)


class Job:
    def __init__(self, item):
        self.approved = dict(codes=[item['code']], start=item['start'].replace('-', ''), end=item['end'].replace('-', ''))
        self.inside = False
        self.finished = False
        self.failed = False
        self.denied = False
    @contextmanager
    def context(self):
        self.inside = True
        try: yield
        finally: self.inside = False
    def assert_ready(self):
        if self.denied: raise RuntimeError('synthetic gate closed')
    def finish(self): self.finished = True
    def fail(self, *args): self.failed = True


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.col = ReadCollection()
        self.db = types.SimpleNamespace(stock_daily_quotes=self.col)
        module = types.ModuleType(DB_MODULE)
        module.get_database = module.get_mongo_db = lambda: self.db
        patcher = patch.dict(sys.modules, {DB_MODULE: module}); patcher.start(); self.addCleanup(patcher.stop)
        self.service = history.HistoricalDataService()
        old = frame(('20261018',))
        await self.service.save_historical_data('600000', old, 'tushare')
        self.baseline = deepcopy(self.col.rows)
        inventory = await Inventory(self.col).collect(['600000', '600000.SH'])
        self.plan = build_plan(**inventory, end='2026-10-19', now=datetime(2026,10,20,tzinfo=timezone.utc),
            quota=dict(daily_requests=10, rolling_minute_requests=4, evidence_ref='synthetic'))
        self.item = self.plan['items'][0]
        self.ws = Workspace(Path(self.tmp.name)/'runs'); self.ws.provision(); self.identity = self.ws.create(self.plan)
        self.job = Job(self.item)
        raw = frame(('20261018','20261019')).rename(columns={'volume':'vol'})
        self.provider, self.sdk, self.namespace = provider(raw)
        async def factory():
            assert self.job.inside
            return self.provider
        self.factory = AsyncMock(side_effect=factory)
        self.job_factory = AsyncMock(return_value=self.job)
        self.authorize = AsyncMock(return_value=True)
        self.adapter = HistoricalAdapter(self.col, self.service, self.factory, self.job_factory, self.authorize)

    async def advance(self):
        return await self.ws.advance_async(self.identity, self.adapter, max_items=1)

    async def payload(self): return await self.adapter.fetch(self.item)

    async def test_real_provider_service_and_journal_append_and_replay(self):
        self.assertTrue((await self.advance())['complete'])
        self.assertEqual(len(self.col.rows), 2)
        self.assertEqual(self.col.rows[0], self.baseline[0])
        self.assertEqual((self.col.rows[1]['volume'],self.col.rows[1]['amount']), (123.45,678900.))
        before = raw_json(self.col.rows)
        self.assertEqual((await self.advance())['processed'],0)
        self.assertEqual(raw_json(self.col.rows), before)
        self.sdk.pro_bar.assert_called_once()
        self.assertTrue(self.job.finished)
        self.assertEqual(self.authorize.await_args_list[0].args[0], self.identity)

    async def test_inventory_is_same_source_and_period_only(self):
        self.col.rows += [dict(symbol='600000',trade_date='2099-01-01',data_source=source,period=period)
                         for source,period in [('akshare','daily'),('tushare','weekly')]]
        observation,_ = await Inventory(self.col).snapshot('600000')
        self.assertEqual((observation['count'],observation['latest']), (1,'2026-10-18'))
        self.assertEqual(self.factory.await_count,0)

    async def test_inventory_returns_legacy_blocker(self):
        del self.col.rows[0]['history_contract_version']
        data = await Inventory(self.col).collect(['600000'])
        self.assertFalse(data['series']['600000.SH']['contract_verified'])
        self.assertEqual(self.factory.await_count,0)

    async def test_missing_unique_index_is_not_empty_inventory(self):
        self.col.index_information = AsyncMock(return_value={})
        with self.assertRaisesRegex(BatchStopped,'unique_index_missing'):
            await Inventory(self.col).collect(['600000'])

    async def test_database_error_propagates(self):
        self.col.index_information = AsyncMock(side_effect=RuntimeError('synthetic DB failure'))
        with self.assertRaises(RuntimeError): await self.advance()
        self.factory.assert_not_awaited(); self.job_factory.assert_not_awaited()

    async def test_unknown_series_empty_is_explicit(self):
        result = await Inventory(self.col).collect(['600001'])
        self.assertEqual(result['series']['600001.SH']['count'],0)
        self.assertIsNone(result['series']['600001.SH']['latest'])
        self.assertEqual(result['series']['600001.SH']['content_sha256'],raw_hash([]))

    async def test_scope_mismatch_does_not_initialize_provider(self):
        self.job.approved['end']='20261020'
        with self.assertRaisesRegex(BatchStopped,'transport_scope_mismatch'): await self.advance()
        self.factory.assert_not_awaited(); self.assertTrue(self.job.failed)

    async def test_stopped_transport_does_not_initialize_provider(self):
        self.job.denied=True
        with self.assertRaises(RuntimeError): await self.advance()
        self.factory.assert_not_awaited(); self.assertTrue(self.job.failed)

    async def test_write_authority_separate_from_fetch_authority(self):
        self.authorize.side_effect=lambda identity,item,action: action!='write'
        with self.assertRaisesRegex(BatchStopped,'not_authorized'): await self.advance()
        self.assertEqual(self.col.rows,self.baseline)
        self.assertEqual(self.ws.status(self.identity)['counts']['fetched'],1)
        self.authorize.side_effect=None
        self.assertTrue((await self.advance())['complete'])
        self.sdk.pro_bar.assert_called_once()

    async def test_after_write_crash_reconciles_real_service_without_rewrite(self):
        original=self.adapter.write
        async def write(item,payload):
            await original(item,payload)
            raise asyncio.CancelledError()
        self.adapter.write=write
        with self.assertRaises(asyncio.CancelledError): await self.advance()
        before=raw_json(self.col.rows); writes=len(self.col.calls)
        self.adapter.write=original
        self.assertTrue((await self.advance())['complete'])
        self.assertEqual(len(self.col.calls),writes)
        self.assertEqual(before,raw_json(self.col.rows))
        self.sdk.pro_bar.assert_called_once()

    async def test_reconcile_rejects_wrong_values_with_same_count(self):
        payload=await self.payload(); await self.adapter.write(self.item,payload)
        self.col.rows[-1]['amount']+=1
        self.assertFalse(await self.adapter.matches(self.item,payload))

    async def test_reconcile_rejects_unexpected_extra_date(self):
        payload=await self.payload(); await self.adapter.write(self.item,payload)
        self.col.rows.append({**self.col.rows[-1],'trade_date':'2026-10-20'})
        self.assertFalse(await self.adapter.matches(self.item,payload))

    async def test_reconcile_rejects_timestamp_change_on_identical_overlap(self):
        payload=await self.payload(); await self.adapter.write(self.item,payload)
        self.col.rows[0]['updated_at']=datetime(2000,1,1)
        self.assertFalse(await self.adapter.matches(self.item,payload))

    async def test_qfq_revision_refused_before_write(self):
        self.sdk.pro_bar.return_value.loc[0,'close']=11.5
        result=await self.advance()
        self.assertTrue(result['complete'])
        self.assertEqual(result['outcomes'],[dict(code='600000.SH',reason='history_adjustment_or_price_revision')])
        self.assertEqual(self.col.rows,self.baseline)
        self.assertTrue(self.job.finished)
        self.assertFalse(self.job.failed)
        self.assertEqual((await self.advance())['processed'],0)

    async def test_frozen_frame_tamper_refused(self):
        payload=await self.payload(); payload['documents'][0]['volume']*=100
        self.assertFalse(await self.adapter.validate(self.item,payload))
        with self.assertRaisesRegex(BatchStopped,'write_preflight_failed'):
            await self.adapter.write(self.item,payload)
        self.assertEqual(self.col.rows,self.baseline)

    async def test_write_through_fresh_actual_loop_safe_collection_proxies(self):
        import ast
        from typing import Callable
        from motor.motor_asyncio import AsyncIOMotorCollection
        from pymongo import MongoClient
        tree = ast.parse((ROOT/'app/core/database.py').read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                   and n.name == '_LoopSafeMongoCollectionProxy')
        ns = dict(Callable=Callable, AsyncIOMotorCollection=AsyncIOMotorCollection)
        exec(compile(ast.Module(body=[cls], type_ignores=[]), '<actual-proxy>', 'exec'), ns)
        proxy = ns['_LoopSafeMongoCollectionProxy']
        # No server: real delegate identity, synthetic storage operations.
        client = MongoClient('mongodb://127.0.0.1:1', connect=False)
        self.addCleanup(client.close)
        self.col.delegate = client.synthetic.stock_daily_quotes
        underlying = self.col
        class FreshDatabase:
            @property
            def stock_daily_quotes(self):
                return proxy(lambda: underlying)
        self.db = FreshDatabase()
        self.adapter.inventory.collection = self.db.stock_daily_quotes
        self.assertIsNot(self.db.stock_daily_quotes, self.db.stock_daily_quotes)
        self.assertTrue((await self.advance())['complete'])
        self.assertEqual(len(self.col.rows), 2)
        self.assertEqual(self.col.rows[0], self.baseline[0])

    async def test_saved_recovery_never_fetches_and_preserves_original_journal(self):
        from deploy.recover_saved_history import recover
        payload = await self.payload()
        before = {str(p): p.read_bytes() for p in Path(self.tmp.name).rglob('*') if p.is_file()}
        self.adapter.fetch = AsyncMock(side_effect=AssertionError('no fetch'))
        reserve = AsyncMock()
        await recover(self.adapter, self.item, payload, reserve)
        reserve.assert_awaited_once()
        self.adapter.fetch.assert_not_called()
        self.assertEqual(len(self.col.rows), 2)
        self.assertEqual(before, {str(p): p.read_bytes() for p in Path(self.tmp.name).rglob('*') if p.is_file()})

    async def test_saved_recovery_drift_never_reserves_or_writes(self):
        from deploy.recover_saved_history import recover
        payload = await self.payload()
        self.col.rows[0]['updated_at'] = datetime(2000, 1, 1)
        reserve = AsyncMock()
        self.adapter.write = AsyncMock()
        with self.assertRaisesRegex(RuntimeError, 'inventory_changed'):
            await recover(self.adapter, self.item, payload, reserve)
        reserve.assert_not_called()
        self.adapter.write.assert_not_called()

    async def test_saved_recovery_uncertain_write_never_retries(self):
        from deploy.recover_saved_history import recover
        payload = await self.payload()
        original = self.adapter.write
        async def uncertain(*args):
            await original(*args)
            raise RuntimeError('reply_lost')
        self.adapter.write = AsyncMock(side_effect=uncertain)
        with self.assertRaisesRegex(RuntimeError, 'reply_lost'):
            await recover(self.adapter, self.item, payload, AsyncMock())
        self.adapter.write.assert_awaited_once()
        self.assertEqual(len(self.col.rows), 2)

    async def test_service_database_mismatch_refused_before_write(self):
        payload=await self.payload()
        other=ReadCollection();other.rows=deepcopy(self.baseline)
        module=types.ModuleType(DB_MODULE)
        module.get_database=module.get_mongo_db=lambda:types.SimpleNamespace(stock_daily_quotes=other)
        with patch.dict(sys.modules,{DB_MODULE:module}):
            with self.assertRaisesRegex(BatchStopped,'service_collection_mismatch'):
                await self.adapter.write(self.item,payload)
        self.assertEqual(other.calls,[])
        self.assertEqual(self.col.rows,self.baseline)

    async def test_inventory_drift_before_save_refuses(self):
        payload=await self.payload(); self.col.rows[0]['updated_at']=datetime(2000,1,1)
        with self.assertRaisesRegex(BatchStopped,'write_preflight_failed'):
            await self.adapter.write(self.item,payload)
        self.assertEqual(len(self.col.rows),1)

    async def exercise_real_gate(self, omit_factor=False):
        from tradingagents.utils import tushare_manual as policy
        now=datetime(2026,10,19,19,tzinfo=policy.BJ).timestamp()
        raw=self.sdk.pro_bar.return_value.copy(deep=True)
        sends=[]
        with patch.object(policy.time,'time',return_value=now):
            gate=policy.provision(Path(self.tmp.name)/'synthetic-gate')
            async def make_job(item):
                return gate.begin(dict(symbol=item['code'],days=2,data_source='tushare',
                    sync_historical=True,sync_realtime=False,sync_financial=False,sync_basic=False),acceptance=True)
            async def make_provider():
                self.assertIsNotNone(policy.CURRENT.get())
                return self.provider
            self.adapter.job_factory=make_job
            self.adapter.provider_factory=make_provider
            self.namespace['_manual_current']=policy.CURRENT
            self.namespace['_manual_frame']=policy.validate_frame
            def sdk(**kwargs):
                job=policy.CURRENT.get()
                params=dict(ts_code='600000.SH',start_date='20261018',end_date='20261019')
                for api in (('daily',) if omit_factor else ('daily','adj_factor')):
                    def send():
                        state=json.loads((gate.directory/'state.json').read_text())
                        # The actual gate persisted this attempt before calling operation.
                        self.assertEqual(state['phase'],'in_flight')
                        self.assertEqual(len(state['attempts']),len(sends)+1)
                        sends.append(api)
                        if api=='daily':
                            return dict(fields=list(raw.columns),items=raw.values.tolist())
                        return dict(fields=['ts_code','trade_date','adj_factor'],
                            items=[['600000.SH',day,17.3] for day in ('20261018','20261019')])
                    job.perform(api,'',params,send)
                return raw.copy(deep=True)
            self.sdk.pro_bar.side_effect=sdk
            if omit_factor:
                with self.assertRaises(policy.TushareEgressStopped): await self.advance()
                self.assertEqual(self.col.rows,self.baseline)
                self.assertTrue((gate.directory/'STOP').exists())
            else:
                self.assertTrue((await self.advance())['complete'])
                state=(gate.directory/'state.json').read_bytes()
                await self.advance()
                self.assertEqual(state,(gate.directory/'state.json').read_bytes())
            self.assertEqual(sends,['daily'] if omit_factor else ['daily','adj_factor'])

    async def test_actual_request_ledger_debits_before_send_and_replay_keeps_counts(self):
        await self.exercise_real_gate()

    async def test_actual_request_ledger_missing_factor_refuses_finish(self):
        await self.exercise_real_gate(omit_factor=True)

    async def test_async_entry_stays_on_callers_loop(self):
        expected=asyncio.get_running_loop()
        async def check(identity,item,action):
            self.assertIs(asyncio.get_running_loop(),expected)
            await asyncio.sleep(0)
            return True
        self.authorize.side_effect=check
        self.assertTrue((await self.advance())['complete'])
        with self.assertRaisesRegex(BatchStopped,'use_advance_async'):
            self.ws.advance(self.identity,self.adapter,max_items=1)


if __name__=='__main__': unittest.main()
