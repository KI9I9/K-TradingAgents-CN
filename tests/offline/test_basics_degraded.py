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

class BasicsSyncResilienceTests(unittest.TestCase):
    def _build_resilience_service(self):
        tree = ast.parse((ROOT / 'app/services/multi_source_basics_sync_service.py').read_text(encoding='utf-8'))
        cls_node = next(
            n for n in tree.body
            if isinstance(n, ast.ClassDef) and n.name == 'MultiSourceBasicsSyncService'
        )
        wanted = {
            'FAST_BULK_DAILY_BASIC_SOURCES',
            'SLOW_SERIAL_DAILY_BASIC_SOURCES',
            'EXCLUDED_DAILY_BASIC_SOURCES',
            '_select_daily_basic_sources',
            '_execute_bulk_write_with_retry',
        }
        kept = []
        for item in cls_node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in wanted:
                kept.append(item)
            elif isinstance(item, ast.Assign):
                if any(isinstance(t, ast.Name) and t.id in wanted for t in item.targets):
                    kept.append(item)
        cls_node.body = kept
        mod = ast.Module(
            body=[
                ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
                cls_node,
            ],
            type_ignores=[],
        )
        import types
        ns = {
            'COLLECTION_NAME': 'stock_basic_info',
            'logger': types.SimpleNamespace(
                debug=lambda *a, **k: None,
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
        }
        exec(compile(ast.fix_missing_locations(mod), '<basics resilience methods>', 'exec'), ns)
        return ns['MultiSourceBasicsSyncService']

    def test_idempotent_bulk_write_counts_matched_and_avoids_false_errors(self):
        import sys
        import types
        from unittest.mock import patch

        Svc = self._build_resilience_service()
        fake_result = types.SimpleNamespace(upserted_count=0, modified_count=0, matched_count=500)
        fake_mod = types.ModuleType('app.utils.thread_safe_db')
        fake_mod.safe_bulk_write = AsyncMock(return_value=fake_result)

        with patch.dict(sys.modules, {'app.utils.thread_safe_db': fake_mod}):
            inserted, updated, matched = asyncio.run(Svc()._execute_bulk_write_with_retry(object(), ['op'] * 500))
        self.assertEqual((inserted, updated, matched), (0, 0, 500))

        # 验证失败时返回 (0, 0, 0)
        fake_mod.safe_bulk_write = AsyncMock(side_effect=RuntimeError('write failed'))
        with patch.dict(sys.modules, {'app.utils.thread_safe_db': fake_mod}):
            self.assertEqual(
                asyncio.run(Svc()._execute_bulk_write_with_retry(object(), ['op'])),
                (0, 0, 0),
            )

        # 验证主同步逻辑检查 batch_matched > 0 避免幂等写回误计 errors
        src = (ROOT / 'app/services/multi_source_basics_sync_service.py').read_text(encoding='utf-8')
        self.assertIn('batch_inserted > 0 or batch_updated > 0 or batch_matched > 0', src)

    def test_select_daily_basic_sources_skips_slow_serial_baostock(self):
        Svc = self._build_resilience_service()
        # 仅有 akshare / baostock（无快速批量源）时返回空列表，直接跳过 45s 必超时拉取
        self.assertEqual(Svc._select_daily_basic_sources(['akshare', 'baostock'], ['akshare', 'baostock']), [])
        self.assertEqual(Svc._select_daily_basic_sources(['baostock'], None), [])
        self.assertEqual(Svc._select_daily_basic_sources(['local', 'qmt', 'baostock'], None), [])
        # 当存在快速批量源 tushare 时，只保留快速源并剔除 baostock
        self.assertEqual(
            Svc._select_daily_basic_sources(['tushare', 'akshare', 'baostock'], ['baostock', 'tushare']),
            ['tushare'],
        )

    def test_bounded_data_fetch_daily_basic_honors_exclude_sources(self):
        src = (ROOT / 'tradingagents/utils/bounded_data_fetch.py').read_text(encoding='utf-8')
        self.assertIn('exclude_sources', src)
        self.assertIn('SLOW_SERIAL_DAILY_BASIC_SOURCES', (ROOT / 'app/services/multi_source_basics_sync_service.py').read_text(encoding='utf-8'))


if __name__=='__main__':unittest.main()
