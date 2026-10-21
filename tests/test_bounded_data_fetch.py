"""Offline regressions: fake upstream, real child termination, no business data."""
import ast
import asyncio
import json
import logging
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

import pandas as pd

from tradingagents.utils import bounded_data_fetch as bounded


class BoundedFetchTests(unittest.TestCase):
    def test_unknown_operation_rejected(self):
        with self.assertRaises(ValueError):
            bounded.fetch("not_allowed")

    def test_oversized_deadline_rejected(self):
        with self.assertRaises(ValueError):
            bounded.fetch("stock_list", timeout=181)

    def test_real_worker_protocol_has_no_import_log_prefix(self):
        result = subprocess.run([sys.executable,"-B",bounded.__file__],
            input=b'{"operation":"not_allowed","arguments":{}}',capture_output=True,timeout=10)
        self.assertEqual(json.loads(result.stdout),{"ok":False,"category":"SDKError"})

    def test_worker_bootstrap_finds_project_from_unrelated_cwd(self):
        code = "import importlib.util,sys;spec=importlib.util.spec_from_file_location('worker_test',sys.argv[1]);m=importlib.util.module_from_spec(spec);sys.modules['worker_test']=m;spec.loader.exec_module(m);m._perform=lambda *_:(__import__('app') and 'IMPORTED',None);m._main()"
        result = subprocess.run([sys.executable,"-B","-c",code,bounded.__file__],
            cwd="/tmp",input=b'{"operation":"trade_date","arguments":{}}',capture_output=True,timeout=10)
        self.assertEqual(json.loads(result.stdout),{"ok":True,"value":"IMPORTED"})

    def test_frame_roundtrip_preserves_stock_code_and_null(self):
        result, source = bounded._decode(json.dumps({"ok":True,"frame":{"columns":["code","metric"],"data":[["000001",None]]},"source":"akshare"}))
        self.assertEqual(result.iloc[0]["code"], "000001")
        self.assertIsNone(result.iloc[0]["metric"])
        self.assertEqual(source, "akshare")

    def test_empty_and_absent_frame_not_successful_data(self):
        frame, _ = bounded._decode('{"ok":true,"frame":null,"source":null}')
        self.assertIsNone(frame)

    def test_network_error_carries_only_safe_circuit(self):
        with self.assertRaises(bounded.DataFetchNetworkError) as caught:
            bounded._decode('{"ok":false,"category":"network","circuit":{"failures":2,"retry_after":42}}')
        self.assertEqual(caught.exception.circuit["failures"], 2)

    def test_sdk_error_not_misclassified_as_network(self):
        with self.assertRaises(RuntimeError) as caught:
            bounded._decode('{"ok":false,"category":"SDKError"}')
        self.assertNotIsInstance(caught.exception, bounded.DataFetchNetworkError)

    def _process_factory(self, code):
        original = subprocess.Popen
        children = []
        def factory(argv, **kwargs):
            child = original([sys.executable,"-B","-c",code], **kwargs)
            children.append(child)
            return child
        return factory, children

    def test_hard_timeout_reaps_real_blocked_child(self):
        factory, children = self._process_factory("import time;time.sleep(20)")
        with patch.object(bounded.subprocess,"Popen",side_effect=factory):
            with self.assertRaises(TimeoutError):
                bounded.fetch("stock_list", timeout=0.15)
        self.assertIsNotNone(children[0].poll())

    def test_successful_child_reaped(self):
        factory, children = self._process_factory('print(\'{"ok":true,"value":"20261021"}\')')
        with patch.object(bounded.subprocess,"Popen",side_effect=factory):
            result = bounded.fetch("trade_date", timeout=5)
        self.assertEqual(result, "20261021")
        self.assertEqual(children[0].poll(), 0)

    def test_async_cancellation_reaps_real_child_before_return(self):
        factory, children = self._process_factory("import time;time.sleep(20)")
        async def run():
            task = asyncio.create_task(bounded.fetch_async("stock_list", timeout=10))
            for _ in range(100):
                if children:break
                await asyncio.sleep(0.01)
            self.assertTrue(children)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertIsNotNone(children[0].poll())
        with patch.object(bounded.subprocess,"Popen",side_effect=factory):
            asyncio.run(run())

    def test_stock_list_timeout_is_not_retried(self):
        from app.services.multi_source_basics_sync_service import MultiSourceBasicsSyncService
        fake = AsyncMock(side_effect=TimeoutError())
        with patch.object(bounded,"fetch_async",fake):
            with self.assertRaises(TimeoutError):
                asyncio.run(MultiSourceBasicsSyncService()._fetch_stock_list_with_retry(None,["akshare"]))
        self.assertEqual(fake.await_count, 1)

    def test_stock_list_uses_same_order_and_excludes_local_in_child(self):
        from app.services.multi_source_basics_sync_service import MultiSourceBasicsSyncService
        fake = AsyncMock(return_value=(pd.DataFrame({"code":["000001"]}),"akshare"))
        with patch.object(bounded,"fetch_async",fake):
            frame, source = asyncio.run(MultiSourceBasicsSyncService()._fetch_stock_list_with_retry(None,["akshare"]))
        self.assertEqual(source,"akshare")
        fake.assert_awaited_once_with("stock_list",{"preferred_sources":["akshare"]},timeout=45)

    def test_history_preserves_circuit_across_children(self):
        from tradingagents.dataflows.providers.china.akshare import AKShareProvider
        provider = AKShareProvider.__new__(AKShareProvider)
        provider._history_circuit_lock = threading.Lock()
        provider._history_failure_count = 1
        provider._history_retry_after = 10.0
        fake = AsyncMock(return_value=(pd.DataFrame({"close":[1.0]}),{"failures":2,"retry_after":42.0}))
        with patch.object(bounded,"fetch_async",fake):
            frame = asyncio.run(provider.get_historical_data("000001","2026-10-20","2026-10-21"))
        self.assertEqual(provider._history_retry_after,42.0)
        self.assertEqual(fake.call_args.args[1]["circuit"],{"failures":1,"retry_after":10.0})
        self.assertEqual(frame.iloc[0]["close"],1.0)

    def test_requests_missing_or_none_timeout_gets_finite_default(self):
        tree = ast.parse(Path("/app/tradingagents/dataflows/providers/china/akshare.py").read_text())
        function = next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=="patched_get")
        condition = function.body[1]
        self.assertIsInstance(condition,ast.If)
        code = compile(ast.fix_missing_locations(ast.Module(body=[condition],type_ignores=[])),"<timeout-default>","exec")
        for supplied, expected in [({},10),({"timeout":None},10),({"timeout":3},3)]:
            exec(code,{"kwargs":supplied})
            self.assertEqual(supplied["timeout"],expected)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    result = unittest.TextTestRunner(verbosity=0).run(unittest.defaultTestLoader.loadTestsFromTestCase(BoundedFetchTests))
    print(json.dumps({"tests":result.testsRun,"failures":len(result.failures),"errors":len(result.errors)}))
    raise SystemExit(not result.wasSuccessful())
