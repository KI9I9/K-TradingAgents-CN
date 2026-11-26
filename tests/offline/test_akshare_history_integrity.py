"""Offline guards for the AKShare historical batch contract.

These tests intentionally inspect the implementation rather than import the
application (the offline runner does not install pandas/akshare/MongoDB).
They protect the two failure modes that previously made a run look complete:
per-symbol batch errors were discarded, and an idempotent Mongo write was
reported as zero records.
"""
import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


def _method(path, class_name, method_name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == method_name:
                    return item
    raise AssertionError(f"missing {class_name}.{method_name}")


def _names(node):
    return {item.id for item in ast.walk(node) if isinstance(item, ast.Name)}


class AkShareHistoryIntegrityTests(unittest.TestCase):
    def test_batch_errors_are_consumed_and_retried(self):
        method = _method(
            ROOT / "app/worker/akshare_sync_service.py",
            "AKShareSyncService",
            "_process_historical_batch",
        )
        names = _names(method)
        self.assertIn("fetch_errors", names)
        self.assertIn("failed_codes", names)
        self.assertIn("retry_chunks", names)
        self.assertTrue(any(
            isinstance(item, ast.Constant) and item.value == "error_count"
            for item in ast.walk(method)
        ))

    def test_akshare_write_uses_strict_error_propagation(self):
        method = _method(
            ROOT / "app/worker/akshare_sync_service.py",
            "AKShareSyncService",
            "_process_historical_batch",
        )
        calls = [
            node for node in ast.walk(method)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "save_historical_data"
        ]
        self.assertEqual(len(calls), 1)
        self.assertTrue(any(
            kw.arg == "raise_on_error"
            and isinstance(kw.value, ast.Constant)
            and kw.value.value is True
            for kw in calls[0].keywords
        ))

    def test_idempotent_bulk_write_counts_matched_documents(self):
        method = _method(
            ROOT / "app/services/historical_data_service.py",
            "HistoricalDataService",
            "_execute_bulk_write_with_retry",
        )
        attrs = {
            node.attr for node in ast.walk(method)
            if isinstance(node, ast.Attribute)
        }
        self.assertTrue(any(
            isinstance(item, ast.Constant) and item.value == "matched_count"
        for item in ast.walk(method)
        ))
        self.assertIn("raise_on_error", _names(method))

    def test_is_data_fresh_handles_bson_utc_and_rejects_future_timestamps(self):
        from datetime import datetime, timezone
        import sys
        import types
        from typing import Any, Optional

        sys.path.insert(0, str(ROOT))
        from app.services.quote_contract import CN_TZ

        method = _method(
            ROOT / "app/worker/akshare_sync_service.py",
            "AKShareSyncService",
            "_is_data_fresh",
        )
        module = ast.Module(body=[method], type_ignores=[])
        ast.fix_missing_locations(module)
        ns = {
            "Any": Any,
            "Optional": Optional,
            "datetime": datetime,
            "logger": types.SimpleNamespace(
                warning=lambda *a, **k: None,
                debug=lambda *a, **k: None,
            ),
        }
        exec(compile(module, "akshare_sync_service.py", "exec"), ns)
        fn = ns["_is_data_fresh"]

        now_cn = datetime(2026, 11, 9, 10, 0, 0, tzinfo=CN_TZ)
        # BSON naive UTC (5m ago = 01:55 UTC vs 10:00+08:00 = 02:00 UTC) -> True
        self.assertTrue(fn(None, datetime(2026, 11, 9, 1, 55, 0), minutes=15, now=now_cn))
        # BSON naive UTC (20m ago = 01:40 UTC) -> False
        self.assertFalse(fn(None, datetime(2026, 11, 9, 1, 40, 0), minutes=15, now=now_cn))
        # Future timestamp (+10m in future = 02:10 UTC) -> False
        self.assertFalse(
            fn(None, datetime(2026, 11, 9, 2, 10, 0, tzinfo=timezone.utc), minutes=15, now=now_cn)
        )
        # Small clock skew (+30s in future, within 60s bound) -> True
        self.assertTrue(
            fn(None, datetime(2026, 11, 9, 2, 0, 30, tzinfo=timezone.utc), minutes=15, now=now_cn)
        )
        # ISO string with +08:00 (10m ago) -> True
        self.assertTrue(fn(None, "2026-11-09T09:50:00+08:00", minutes=15, now=now_cn))
        # Invalid/empty values -> False
        self.assertFalse(fn(None, "not-a-timestamp", minutes=15, now=now_cn))
        self.assertFalse(fn(None, None, minutes=15, now=now_cn))

    def test_bounded_data_fetch_circuit_epoch_roundtrip_and_merge(self):
        import sys
        sys.path.insert(0, str(ROOT))
        from tradingagents.utils.bounded_data_fetch import (
            _export_circuit_state,
            _hydrate_circuit_state,
            merge_circuit_state,
        )

        # 子进程 A 在 mono=100.0 时触发 300s 熔断，导出 UTC epoch
        exported = _export_circuit_state(2, 400.0, now_wall=1_700_000_000.0, now_mono=100.0)
        self.assertEqual(exported["failures"], 2)
        self.assertAlmostEqual(exported["remaining_seconds"], 300.0, places=2)
        self.assertAlmostEqual(exported["retry_after_epoch"], 1_700_000_300.0, places=2)

        # 子进程 B 在完全不同的单调时钟基准 mono=5000.0、wall + 60s 时还原，仍保留 240s 熔断
        failures, mono_deadline = _hydrate_circuit_state(
            exported, now_wall=1_700_000_060.0, now_mono=5000.0
        )
        self.assertEqual(failures, 2)
        self.assertAlmostEqual(mono_deadline, 5240.0, places=2)

        # 熔断窗口自然过期后还原，应清零 failures 允许半开探测
        expired_failures, expired_deadline = _hydrate_circuit_state(
            exported, now_wall=1_700_000_305.0, now_mono=6000.0
        )
        self.assertEqual(expired_failures, 0)
        self.assertEqual(expired_deadline, 0.0)

        # 会话合并：任一并发批次触发熔断后，后续批次即使返回未熔断也保留激活期更长的熔断窗口
        merged = merge_circuit_state(
            {"failures": 0, "remaining_seconds": 0.0, "retry_after_epoch": 0.0},
            exported,
            now_wall=1_700_000_010.0,
        )
        self.assertGreater(merged["remaining_seconds"], 280.0)
        self.assertEqual(merged["retry_after_epoch"], 1_700_000_300.0)

        merged_after_ok = merge_circuit_state(
            merged,
            {"failures": 0, "remaining_seconds": 0.0, "retry_after_epoch": 0.0},
            now_wall=1_700_000_020.0,
        )
        self.assertEqual(merged_after_ok["retry_after_epoch"], 1_700_000_300.0)
        self.assertGreaterEqual(merged_after_ok["failures"], 2)

    def test_batch_propagates_circuit_and_tags_retryable_errors(self):
        import asyncio
        import sys
        import time
        import types
        from datetime import datetime, timedelta
        from typing import Any, Dict, List, Optional
        from unittest.mock import AsyncMock

        sys.path.insert(0, str(ROOT))
        batch_method = _method(
            ROOT / "app/worker/akshare_sync_service.py",
            "AKShareSyncService",
            "_process_historical_batch",
        )
        retry_method = _method(
            ROOT / "app/worker/akshare_sync_service.py",
            "AKShareSyncService",
            "retry_failed_symbols",
        )
        cls_node = ast.ClassDef(
            name="_BatchHarness",
            bases=[],
            keywords=[],
            body=[batch_method, retry_method],
            decorator_list=[],
        )
        module = ast.Module(body=[cls_node], type_ignores=[])
        ast.fix_missing_locations(module)
        ns = {
            "Any": Any,
            "Dict": Dict,
            "List": List,
            "Optional": Optional,
            "asyncio": asyncio,
            "datetime": datetime,
            "timedelta": timedelta,
            "logger": types.SimpleNamespace(
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                debug=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
        }
        exec(compile(module, "akshare_sync_service.py", "exec"), ns)
        Harness = ns["_BatchHarness"]

        class _FakeFrame:
            def __init__(self, empty=False):
                self.empty = empty

        seen_circuits = []
        tripped_epoch = time.time() + 300.0

        async def _fake_fetch_async(operation, arguments, timeout=45):
            seen_circuits.append(dict(arguments.get("circuit") or {}))
            codes = arguments["codes"]
            if "000001" in codes:
                return (
                    {"000002": _FakeFrame(empty=True), "000003": _FakeFrame(empty=False)},
                    {"000001": "network"},
                    {"failures": 2, "remaining_seconds": 300.0, "retry_after_epoch": tripped_epoch},
                )
            return (
                {code: _FakeFrame(empty=False) for code in codes},
                {},
                {"failures": 2, "remaining_seconds": 295.0, "retry_after_epoch": tripped_epoch},
            )

        import tradingagents.utils.bounded_data_fetch as bdf
        orig_fetch_async = bdf.fetch_async
        bdf.fetch_async = _fake_fetch_async
        try:
            svc = Harness()
            svc.historical_service = types.SimpleNamespace(
                save_historical_data=AsyncMock(return_value=10)
            )
            svc.sync_historical_data = AsyncMock(
                return_value={"success_count": 1, "error_count": 0, "errors": []}
            )

            session_circuit = {"failures": 0, "remaining_seconds": 0.0, "retry_after_epoch": 0.0}
            batch1 = asyncio.run(
                svc._process_historical_batch(
                    ["000001", "000002", "000003"],
                    start_date="2026-11-01",
                    end_date="2026-11-10",
                    period="daily",
                    incremental=False,
                    circuit_state=session_circuit,
                )
            )
            # 000001 网络失败被标记可重试，000002 空 DataFrame 仅计 no_data_count
            self.assertEqual(batch1["success_count"], 1)
            self.assertEqual(batch1["no_data_count"], 1)
            self.assertEqual(batch1["error_count"], 1)
            self.assertEqual(len(batch1["errors"]), 1)
            err = batch1["errors"][0]
            self.assertEqual(err["code"], "000001")
            self.assertEqual(err["error_category"], "retryable_error")
            self.assertTrue(err["is_retryable"])

            # 会话熔断状态已保留，且第 2 批会携带激活的 retry_after_epoch
            self.assertGreater(session_circuit["retry_after_epoch"], time.time())
            asyncio.run(
                svc._process_historical_batch(
                    ["000004"],
                    start_date="2026-11-01",
                    end_date="2026-11-10",
                    period="daily",
                    incremental=False,
                    circuit_state=session_circuit,
                )
            )
            # 至少经历了：batch1 首次、batch1 小批重试、batch2，后续调用都携带已激活熔断
            self.assertGreaterEqual(len(seen_circuits), 3)
            self.assertGreater(seen_circuits[-1]["retry_after_epoch"], time.time())
            self.assertGreater(seen_circuits[-1]["remaining_seconds"], 0.0)

            # retry_failed_symbols 能够识别该错误并实际触发重试
            retry_res = asyncio.run(svc.retry_failed_symbols(batch1["errors"]))
            self.assertEqual(retry_res["total_retried"], 1)
            svc.sync_historical_data.assert_awaited_once()
            self.assertEqual(svc.sync_historical_data.call_args.kwargs["symbols"], ["000001"])
        finally:
            bdf.fetch_async = orig_fetch_async

    def test_akshare_stop_and_running_checks_avoid_new_mongo_client(self):
        path = ROOT / "app/worker/akshare_sync_service.py"
        should_stop = _method(path, "AKShareSyncService", "_should_stop")
        self.assertNotIn("MongoClient", _names(should_stop))
        self.assertIn("get_mongo_db_sync", _names(should_stop))

        tree = ast.parse(path.read_text(encoding="utf-8"))
        check_fn = next(
            n for n in tree.body
            if isinstance(n, ast.AsyncFunctionDef) and n.name == "_check_task_running"
        )
        self.assertNotIn("MongoClient", _names(check_fn))
        self.assertIn("get_mongo_db_sync", _names(check_fn))
        self.assertIn("build_active_running_query", _names(check_fn))


if __name__ == "__main__":
    unittest.main()
