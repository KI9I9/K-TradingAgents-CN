"""No upstream: exact-scope writer boundary and captured-data reconstruction."""
import asyncio
import copy
from datetime import datetime
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, AsyncMock, patch

from bson import ObjectId
from pymongo import ReplaceOne, UpdateOne

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('write_sample', ROOT/'deploy/history_write_acceptance.py')
w = importlib.util.module_from_spec(spec); spec.loader.exec_module(w)


def fixture():
    spec = importlib.util.spec_from_file_location('contract', ROOT/'app/services/historical_contract.py')
    c = importlib.util.module_from_spec(spec); spec.loader.exec_module(c)
    import pandas as pd
    f = pd.DataFrame([dict(trade_date=day, open=10., high=12., low=9., close=11., volume=123.45,
                          amount=678.9, pre_close=10., change=1., pct_chg=10.) for day in w.DAYS])
    f.attrs['historical_contract'] = dict(source='tushare',symbol='600000',period='daily',
        volume_unit='lot',amount_unit='thousand_CNY',start_date=w.DAYS[0],end_date=w.DAYS[-1],
        endpoint='pro_bar',price_adjustment='qfq')
    docs = c.tushare_documents('600000',f,'CN','daily')
    captured = dict(status='passed',real_attempts=2,replay_identical=True,differences=[],documents=docs)
    return c, captured


class WriteBoundary(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.contract, self.captured = fixture()
        self.docs = self.captured['documents']
        self.ids = {day:ObjectId() for day in w.DAYS}
        self.raw = Mock()
        self.bound = w.BoundedCollection(self.raw,self.docs,self.ids,True)

    def operations(self):
        now = datetime.utcnow()
        return [UpdateOne({**w.SERIES,'trade_date':d['trade_date']},
            {'$setOnInsert':dict(d,created_at=now,updated_at=now)},upsert=True) for d in self.docs]

    def test_capture_reconstruction_preserves_contract(self):
        frame = w.captured_frame(self.captured,self.contract)
        self.assertEqual(self.contract.tushare_documents('600000',frame,'CN','daily'),self.docs)
        self.assertEqual(frame.iloc[0]['amount'],678.9)

    def test_bad_capture_rejected(self):
        for key,value in [('status','failed'),('real_attempts',1),('replay_identical',False),('differences',[{}])]:
            bad=copy.deepcopy(self.captured);bad[key]=value
            with self.assertRaises(ValueError):w.captured_frame(bad,self.contract)
        bad=copy.deepcopy(self.captured);bad['documents'][0]['symbol']='000001'
        with self.assertRaises(ValueError):w.captured_frame(bad,self.contract)

    async def test_preflight_write_disabled(self):
        self.bound.write_enabled=False
        with self.assertRaises(ValueError):await self.bound.bulk_write(self.operations(),True)
        self.raw.bulk_write.assert_not_called()

    async def test_exact_batch_only_once_and_owned_ids(self):
        await self.bound.bulk_write(self.operations(),True)
        sent=self.raw.bulk_write.call_args.args[0]
        for op in sent:self.assertEqual(op._doc['$setOnInsert']['_id'],self.ids[op._filter['trade_date']])
        with self.assertRaises(ValueError):await self.bound.bulk_write(self.operations(),True)
        self.assertEqual(self.raw.bulk_write.call_count,1)

    async def test_uncertain_write_not_retried(self):
        self.raw.bulk_write.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):await self.bound.bulk_write(self.operations(),True)
        with self.assertRaises(ValueError):await self.bound.bulk_write(self.operations(),True)
        self.assertEqual(self.raw.bulk_write.call_count,1)

    async def test_all_operations_validated_before_send(self):
        for mutation in ('symbol','payload','update','replace','duplicate','count'):
            ops=self.operations()
            if mutation=='symbol':ops[-1]._filter['symbol']='000001'
            if mutation=='payload':ops[-1]._doc['$setOnInsert']['volume']*=100
            if mutation=='update':ops[-1]._doc={'$set':ops[-1]._doc['$setOnInsert']}
            if mutation=='replace':ops[-1]=ReplaceOne(ops[-1]._filter,ops[-1]._doc['$setOnInsert'],upsert=True)
            if mutation=='duplicate':ops[-1]=ops[0]
            if mutation=='count':ops.pop()
            with self.assertRaises(ValueError):await self.bound.bulk_write(ops,True)
            self.raw.bulk_write.assert_not_called()

    def test_no_broad_reads(self):
        with self.assertRaises(ValueError):self.bound.find({})
        self.raw.find.assert_not_called()

    def test_preflight_main_never_writes_or_creates_run(self):
        import contextlib, io, json, tempfile
        collection=Mock()
        collection.find.side_effect=lambda query: [] if query==w.SERIES else [{'_id':ObjectId()} for _ in range(2089)]
        collection.index_information.return_value={}
        db=Mock();db.get_collection.return_value=collection
        db.scheduler_preferences.find_one.return_value={'enabled':False}
        db.scheduler_executions.count_documents.return_value=0
        client=Mock();client.__getitem__=Mock(return_value=db)
        service=Mock();service.validate_tushare_series=AsyncMock(return_value=[])
        captured=json.dumps({'result.json':self.captured})
        env=dict(MONGODB_HOST='synthetic',MONGODB_PORT='27017',MONGODB_USERNAME='synthetic',
                 MONGODB_PASSWORD='synthetic',MONGODB_DATABASE='synthetic',TUSHARE_UNIFIED_ENABLED='false')
        request=dict(mode='preflight',source_hashes={},protected_hashes={},captured_json=captured,
            captured_sha256=w.sha(captured.encode()),runtime_tool='def reconstruct(*a): return '+repr((env,{'gates':{'write':{'enabled':False}}})))
        with tempfile.TemporaryDirectory() as tmp, patch.object(w,'RUN',Path(tmp)/'uncreated'), \
                patch.object(w,'MongoClient',return_value=client), \
                patch.object(w,'load_service',return_value=(service,self.contract)), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(w.main(request),0)
            self.assertFalse(w.RUN.exists())
        self.assertEqual(json.loads(output.getvalue())['status'],'preflight_passed')
        collection.bulk_write.assert_not_called()
        service.save_historical_data.assert_not_called()

    def test_real_capture_roundtrip(self):
        # Optional local evidence is tested separately; synthetic tests remain portable.
        import json
        path=Path('/home/ubuntu/quote-guard-releases/production-20261028/history-sample-production-result-v2.json')
        if not path.exists():self.skipTest('local captured evidence unavailable')
        captured=json.loads(path.read_text())['result.json']
        self.assertEqual(self.contract.tushare_documents('600000',w.captured_frame(captured,self.contract),'CN','daily'),captured['documents'])


if __name__=='__main__':unittest.main()
