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


if __name__ == "__main__":
    unittest.main()
