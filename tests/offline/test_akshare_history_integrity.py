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


if __name__ == "__main__":
    unittest.main()
