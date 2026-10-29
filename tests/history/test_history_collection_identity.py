"""Use actual loop-safe proxy bodies and real Motor/PyMongo without a server."""
import ast
from pathlib import Path
import sys
import unittest
from typing import Callable, Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from deploy.history_batch_adapters import same_collection

# Avoid initializing production settings; execute the unmodified proxy bodies.
tree=ast.parse((ROOT/'app/core/database.py').read_text())
names={'_LoopSafeMongoCollectionProxy','_LoopSafeMongoDatabaseProxy'}
body=[n for n in tree.body if isinstance(n,ast.ClassDef) and n.name in names]
ns=dict(Callable=Callable,Optional=Optional,AsyncIOMotorCollection=AsyncIOMotorCollection,
        AsyncIOMotorDatabase=AsyncIOMotorDatabase)
exec(compile(ast.Module(body=body,type_ignores=[]),'<actual-loop-safe-proxies>','exec'),ns)


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.client=AsyncIOMotorClient('mongodb://127.0.0.1:1',connect=False)
        self.other=AsyncIOMotorClient('mongodb://127.0.0.1:1',connect=False)
        self.addCleanup(self.client.close);self.addCleanup(self.other.close)
        ns['_get_bound_mongo_db']=lambda:self.client['synthetic']
        self.db=ns['_LoopSafeMongoDatabaseProxy']()

    def test_actual_proxies_are_distinct_but_target_matches(self):
        a,b=self.db.stock_daily_quotes,self.db.stock_daily_quotes
        self.assertIsNot(a,b)
        self.assertNotEqual(a,b)  # reproduces production failure before the fix
        self.assertTrue(same_collection(a,b))
        self.assertTrue(same_collection(a,self.client.synthetic.stock_daily_quotes))

    def test_other_collection_or_database_refused(self):
        a=self.db.stock_daily_quotes
        self.assertFalse(same_collection(a,self.client.synthetic.other))
        self.assertFalse(same_collection(a,self.client.another.stock_daily_quotes))

    def test_same_namespace_different_client_refused(self):
        self.assertFalse(same_collection(self.db.stock_daily_quotes,self.other.synthetic.stock_daily_quotes))

    def test_unproven_delegate_refused(self):
        from types import SimpleNamespace
        self.assertFalse(same_collection(SimpleNamespace(delegate='same'),SimpleNamespace(delegate='same')))


if __name__=='__main__':unittest.main()
