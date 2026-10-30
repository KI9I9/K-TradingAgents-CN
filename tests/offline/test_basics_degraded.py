"""Execute the actual status methods with a database double; no app startup."""
import ast
import asyncio
from copy import deepcopy
from pathlib import Path
import unittest
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[2]
source = ast.parse((ROOT/'app/services/multi_source_basics_sync_service.py').read_text())
cls = next(n for n in source.body if isinstance(n,ast.ClassDef) and n.name=='MultiSourceBasicsSyncService')
cls.body = [n for n in cls.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))
            and n.name in ('_normalize_status','_successful_summary','_with_inventory')]
module = ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),cls],type_ignores=[])
namespace = dict(COLLECTION_NAME='stock_basic_info')
exec(compile(ast.fix_missing_locations(module),'<actual service methods>','exec'),namespace)
Service = namespace['MultiSourceBasicsSyncService']

class StatusTests(unittest.TestCase):
    def test_legacy_local_success_becomes_degraded_without_mutation(self):
        old = dict(status='success',errors=0,data_sources_used=['stock_list:__local_universe__','industry_mapping:system_industry_mapping'])
        original = deepcopy(old)
        result = Service._normalize_status(old)
        self.assertEqual(result['status'],'success_with_errors')
        self.assertTrue(result['degraded'])
        self.assertIn('本地已有',result['message'])
        self.assertIn('指标保留',result['message'])
        self.assertEqual(old,original)
        self.assertIsNone(Service._successful_summary(old))

    def test_external_list_without_metrics_is_partial(self):
        result = Service._normalize_status(dict(status='success',data_sources_used=['stock_list:akshare']))
        self.assertTrue(result['degraded'])
        self.assertNotIn('外部股票列表获取失败',result['message'])

    def test_external_complete_is_success(self):
        row = dict(status='success',data_sources_used=['stock_list:akshare','daily_data:akshare'])
        self.assertEqual(Service._normalize_status(row),row)
        self.assertIsNotNone(Service._successful_summary(row))

    def test_bad_last_success_removed_but_real_success_preserved(self):
        local = dict(status='success',data_sources_used=['stock_list:__local_universe__'])
        external = dict(status='success',data_sources_used=['stock_list:akshare','daily_data:akshare'])
        collection = type('Collection',(),{'count_documents':AsyncMock(return_value=11154)})()
        for previous,expected in [(local,None),(external,external)]:
            row = dict(local,last_success=previous)
            result = asyncio.run(Service()._with_inventory({'stock_basic_info':collection},row))
            self.assertEqual(result['last_success'],expected)
            self.assertEqual(result['existing_records'],11154)

if __name__=='__main__':unittest.main()
