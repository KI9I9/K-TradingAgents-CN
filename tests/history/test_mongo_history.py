"""Real MongoDB acceptance; refuses non-loopback URIs and uses only a fresh test DB."""
import asyncio
import copy
from datetime import date, timedelta
import os
import sys
import types
import unittest
from unittest.mock import patch
from urllib.parse import urlparse
import uuid

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError
from test_tushare_history import history, frame, HistoricalContractError, DB_MODULE

URI = os.environ.get('HISTORY_TEST_MONGO_URI', '')
if urlparse(URI).hostname != '127.0.0.1':
    raise SystemExit('An explicitly provided loopback test MongoDB URI is required')


class MongoAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = AsyncIOMotorClient(URI, serverSelectionTimeoutMS=3000)
        self.db = self.client['history_contract_test_' + uuid.uuid4().hex]
        self.collection = self.db.stock_daily_quotes
        await self.collection.create_index([('symbol',1),('trade_date',1),('data_source',1),('period',1)],unique=True)
        module = types.ModuleType(DB_MODULE)
        module.get_database = module.get_mongo_db = lambda:self.db
        self.patcher = patch.dict(sys.modules,{DB_MODULE:module})
        self.patcher.start()
        self.service = history.HistoricalDataService()

    async def asyncTearDown(self):
        try:
            await self.client.drop_database(self.db.name)
        finally:
            self.patcher.stop()
            self.client.close()

    async def save(self, data):
        return await self.service.save_historical_data('600000',data,'tushare')

    async def test_real_upsert_replay_and_concurrent_requests(self):
        counts = await asyncio.gather(self.save(frame()),self.save(frame()))
        self.assertEqual(counts,[1,1])
        self.assertEqual(await self.collection.count_documents({}),1)
        before = await self.collection.find_one({})
        self.assertEqual((before['volume'],before['amount']),(123.45,678900.))
        self.assertEqual(await self.save(frame()),1)
        self.assertEqual(await self.collection.find_one({}),before)

    async def test_series_and_qfq_guards_with_real_queries(self):
        unrelated = dict(symbol='600000',trade_date='2026-10-26',data_source='akshare',period='daily',volume=1)
        await self.collection.insert_one(copy.deepcopy(unrelated))
        await self.save(frame(('20261025',)))
        self.assertEqual(await self.save(frame(('20261025','20261026'))),2)
        data = frame(('20261026','20261027'))
        data.loc[0,'close'] = 10.5
        with self.assertRaises(HistoricalContractError): await self.save(data)
        self.assertEqual(await self.collection.count_documents({'trade_date':'2026-10-27'}),0)
        self.assertEqual((await self.collection.find_one({'data_source':'akshare'}))['volume'],1)

    async def test_partial_server_failure_is_visible_and_replay_repairs_prefix(self):
        days = [(date(2025,1,1)+timedelta(days=i)).strftime('%Y%m%d') for i in range(201)]
        data = frame(days)
        rejected = (date(2025,1,1)+timedelta(days=200)).isoformat()
        await self.db.command('collMod','stock_daily_quotes',validator={'trade_date':{'$ne':rejected}})
        with self.assertRaises(BulkWriteError): await self.save(data)
        self.assertEqual(await self.collection.count_documents({}),200)
        await self.db.command('collMod','stock_daily_quotes',validator={})
        self.assertEqual(await self.save(data),201)
        self.assertEqual(await self.collection.count_documents({}),201)
        self.assertEqual(await self.save(data),201)
        self.assertEqual(await self.collection.count_documents({}),201)


if __name__=='__main__':
    unittest.main(verbosity=2)
