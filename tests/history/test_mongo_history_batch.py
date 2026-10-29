"""Real loopback Mongo integration for the batch adapter; synthetic provider only."""
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import urlparse
from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError
from test_tushare_history import history, frame, provider, DB_MODULE
from deploy.history_batch_adapters import Inventory, HistoricalAdapter, raw_json
from deploy.history_sync_plan import build_plan
from deploy.history_batch_journal import Workspace, BatchStopped

URI=os.environ.get('HISTORY_TEST_MONGO_URI','')
if urlparse(URI).hostname!='127.0.0.1': raise SystemExit('explicit loopback test URI required')


class SyntheticJob:
    def __init__(self,item):
        self.approved=dict(codes=[item['code']],start=item['start'].replace('-',''),end=item['end'].replace('-',''))
    @contextmanager
    def context(self): yield
    def assert_ready(self): pass
    def finish(self): pass
    def fail(self,*args): pass


class MongoBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client=AsyncIOMotorClient(URI,serverSelectionTimeoutMS=3000,retryWrites=False)
        self.db=self.client['history_batch_test_'+uuid4().hex]
        self.col=self.db.stock_daily_quotes
        await self.col.create_index([('symbol',1),('trade_date',1),('data_source',1),('period',1)],unique=True)
        module=types.ModuleType(DB_MODULE)
        module.get_database=module.get_mongo_db=lambda:self.db
        self.patcher=patch.dict(sys.modules,{DB_MODULE:module});self.patcher.start()
        self.temp=tempfile.TemporaryDirectory()
        self.service=history.HistoricalDataService()
        await self.service.save_historical_data('600000',frame(('20261017','20261018')),'tushare')
        self.baseline=await self.col.find({}).to_list(length=100)
        inventory=await Inventory(self.col).collect(['600000'])
        self.plan=build_plan(**inventory,end='2026-10-20',now=datetime(2026,10,21,tzinfo=timezone.utc),
            quota=dict(daily_requests=10,rolling_minute_requests=4,evidence_ref='synthetic'))
        self.ws=Workspace(Path(self.temp.name)/'runs');self.ws.provision();self.identity=self.ws.create(self.plan)
        obj,self.sdk,_=provider(frame(('20261018','20261019','20261020')).rename(columns={'volume':'vol'}))
        self.factory=AsyncMock(return_value=obj)
        self.adapter=HistoricalAdapter(self.col,self.service,self.factory,
            AsyncMock(side_effect=lambda item:SyntheticJob(item)),AsyncMock(return_value=True))

    async def asyncTearDown(self):
        try: await self.client.drop_database(self.db.name)
        finally:
            self.client.close();self.patcher.stop();self.temp.cleanup()

    async def advance(self):
        return await self.ws.advance_async(self.identity,self.adapter,max_items=1)

    async def rows(self): return await self.col.find({}).to_list(length=100)

    async def test_append_replay_preserves_bson_and_untouched_records(self):
        self.assertTrue((await self.advance())['complete'])
        rows=await self.rows();self.assertEqual(len(rows),4)
        for old in self.baseline:
            self.assertEqual(raw_json([old]),raw_json([next(r for r in rows if r['_id']==old['_id'])]))
        before=raw_json(rows)
        await self.advance()
        self.assertEqual(raw_json(await self.rows()),before)
        self.sdk.pro_bar.assert_called_once()

    async def test_partial_write_retained_and_not_retried(self):
        await self.db.command('collMod','stock_daily_quotes',validator={'trade_date':{'$ne':'2026-10-20'}})
        with self.assertRaises(BulkWriteError): await self.advance()
        rows=await self.rows();self.assertEqual(len(rows),3)
        before=raw_json(rows)
        with self.assertRaisesRegex(BatchStopped,'write_outcome_unresolved'): await self.advance()
        self.assertEqual(raw_json(await self.rows()),before)
        self.sdk.pro_bar.assert_called_once()
        self.assertEqual(self.ws.status(self.identity)['counts']['writing'],1)

    async def test_cancel_after_real_write_only_reconciles(self):
        original=self.adapter.write
        async def write(item,payload):
            await original(item,payload);raise asyncio.CancelledError()
        self.adapter.write=write
        with self.assertRaises(asyncio.CancelledError): await self.advance()
        before=raw_json(await self.rows())
        self.adapter.write=AsyncMock(side_effect=AssertionError('must not write again'))
        self.assertTrue((await self.advance())['complete'])
        self.assertEqual(raw_json(await self.rows()),before)
        self.adapter.write.assert_not_awaited();self.sdk.pro_bar.assert_called_once()

    async def test_missing_index_refuses_provider(self):
        await self.col.drop_indexes()
        with self.assertRaisesRegex(BatchStopped,'unique_index_missing'): await self.advance()
        self.factory.assert_not_awaited()
        self.assertEqual(raw_json(await self.rows()),raw_json(self.baseline))

    async def test_reconcile_detects_changes_outside_requested_dates(self):
        original=self.adapter.write
        async def write(item,payload):
            await original(item,payload);raise asyncio.CancelledError()
        self.adapter.write=write
        with self.assertRaises(asyncio.CancelledError): await self.advance()
        await self.col.update_one({'trade_date':'2026-10-17'},{'$set':{'amount':1.0}})
        with self.assertRaisesRegex(BatchStopped,'write_outcome_unresolved'): await self.advance()
        self.assertEqual(self.ws.status(self.identity)['counts']['committed'],0)
        self.sdk.pro_bar.assert_called_once()


if __name__=='__main__':unittest.main()
