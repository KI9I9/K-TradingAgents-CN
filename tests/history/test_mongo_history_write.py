"""Real local MongoDB tests for the bounded production-write acceptance tool."""
import asyncio
import os
import sys
import unittest
from urllib.parse import urlparse
import uuid
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from test_history_write_acceptance import w, fixture, ROOT

URI=os.environ.get('HISTORY_TEST_MONGO_URI','')
if urlparse(URI).hostname!='127.0.0.1':raise SystemExit('loopback test URI required')


class MongoWriteAcceptance(unittest.TestCase):
    def setUp(self):
        self.client=MongoClient(URI,serverSelectionTimeoutMS=3000,retryWrites=False)
        self.db=self.client['history_write_test_'+uuid.uuid4().hex]
        self.col=self.db.stock_daily_quotes
        self.col.create_index([('symbol',1),('trade_date',1),('data_source',1),('period',1)],unique=True)
        self.contract,self.capture=fixture();self.ids={d:ObjectId() for d in w.DAYS}
        self.bound=w.BoundedCollection(self.col,self.capture['documents'],self.ids,True)
        self.modules=dict(sys.modules)
        self.service,self.contract=w.load_service(ROOT,self.bound)
        self.frame=w.captured_frame(self.capture,self.contract)

    def tearDown(self):
        self.client.drop_database(self.db.name);self.client.close()
        for name in ('app','app.core','app.services','app.core.database','app.services.historical_contract'):
            if name in self.modules:sys.modules[name]=self.modules[name]
            else:sys.modules.pop(name,None)

    def save(self):
        return asyncio.run(w.save_and_replay(self.service,self.frame,self.col,self.capture['documents'],self.ids))

    def test_actual_service_insert_replay_preserves_ids_timestamps_and_other_sources(self):
        self.col.insert_one(dict(symbol='600000',data_source='akshare',period='daily',trade_date=w.DAYS[0],volume=42))
        self.col.insert_one(dict(symbol='999999',data_source='tushare',period='daily',trade_date=w.DAYS[0],volume=43))
        before=w.bson_rows(list(self.col.find({})))
        rows=self.save()
        self.assertEqual(len(rows),5);self.assertEqual(self.bound.batches,1)
        after=w.bson_rows(list(self.col.find({'_id':{'$nin':list(self.ids.values())}})))
        self.assertEqual(before,after)
        self.assertEqual(len(list(self.col.find({}))),7)

    def test_concurrent_existing_row_is_never_overwritten(self):
        doc=dict(self.capture['documents'][0]);self.col.insert_one(doc)
        before=w.bson_rows(list(self.col.find({})))
        with self.assertRaisesRegex(ValueError,'write_count'):self.save()
        self.assertEqual(before,w.bson_rows(list(self.col.find({}))))
        self.assertEqual(self.bound.batches,0)

    def test_partial_write_latches_and_keeps_owned_prefix(self):
        self.db.command('collMod','stock_daily_quotes',validator={'trade_date':{'$ne':w.DAYS[2]}})
        with self.assertRaises(BulkWriteError):self.save()
        rows=list(self.col.find(w.SERIES));self.assertEqual(len(rows),2)
        self.assertTrue(all(r['_id']==self.ids[r['trade_date']] for r in rows))
        with self.assertRaisesRegex(ValueError,'write_budget_or_mode'):self.save()
        self.assertEqual(len(list(self.col.find(w.SERIES))),2)

    def test_missing_unique_index_refuses_before_writes(self):
        self.col.drop_indexes()
        with self.assertRaisesRegex(ValueError,'history_unique_index_missing'):self.save()
        self.assertEqual(self.col.count_documents({}),0)
        self.assertEqual(self.bound.batches,0)


if __name__=='__main__':unittest.main(verbosity=2)
