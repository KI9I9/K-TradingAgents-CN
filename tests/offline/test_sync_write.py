"""写入侧行为测试：采集时刻整批固定 + 通道切换 $unset + 四条路径同一契约。

`_mongo_update_spec` 从真实源文件抽取执行；$set/$unset 合并语义在内存里模拟
（MongoDB 的 `$set` 不会删除未出现的字段，这正是旧 quote_time 残留的原因）。
"""

import ast
import os
import sys
import unittest

os.environ.setdefault("QUOTES_WRITE_ENABLED", "1")
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, ".")

from app.services.quote_contract import (  # noqa: E402
    CN_TZ,
    prepare_quote_write,
    quote_is_usable,
)

SRC = Path("app/worker/akshare_sync_service.py")
NOW = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)


def load_update_spec():
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_mongo_update_spec":
            module = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(module)
            ns: Dict[str, Any] = {}
            exec(compile(module, str(SRC), "exec"), ns)
            return ns["_mongo_update_spec"]
    raise AssertionError("akshare_sync_service.py 缺少 _mongo_update_spec")


def apply_spec(existing: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    """模拟 MongoDB update_one 的 $set/$unset 语义。"""
    doc = dict(existing)
    doc.update(spec.get("$set") or {})
    for field in (spec.get("$unset") or {}):
        doc.pop(field, None)
    return doc


class TestUpdateSpec(unittest.TestCase):
    def setUp(self):
        self.spec_builder = load_update_spec()

    def test_plain_set(self):
        spec = self.spec_builder({"close": 10.0}, [])
        self.assertEqual(spec, {"$set": {"close": 10.0}})

    def test_unset_only_when_absent_from_doc(self):
        spec = self.spec_builder({"close": 10.0, "quote_time": "x"}, ["quote_time"])
        self.assertNotIn("$unset", spec)

    def test_channel_switch_clears_old_quote_time(self):
        """评审 P2-4：upstream → fetch 时，$set 删不掉旧字段。"""
        existing = {
            "code": "600000", "trade_date": "2026-10-26", "close": 8.0,
            "high": 9.0, "low": 7.0, "quote_time": "2026-10-26T14:00:00+08:00",
            "quote_time_source": "upstream", "fetched_at": "2026-10-26T14:00:05+08:00",
        }
        doc, unset, reason = prepare_quote_write(
            {"close": 10.0, "high": 11.0, "low": 9.0, "symbol": "600000",
             "trading_status": "trading"},
            "600000", "2026-10-27", NOW, 1800)
        self.assertEqual(reason, "ok")
        merged = apply_spec(existing, self.spec_builder(doc, unset))
        self.assertNotIn("quote_time", merged)
        self.assertEqual(merged["quote_time_source"], "fetch")
        self.assertEqual(merged["close"], 10.0)
        self.assertTrue(quote_is_usable(merged, NOW)[0])


class TestCapturedAtSemantics(unittest.TestCase):
    def test_batch_capture_time_survives_late_write(self):
        """评审 P1-1：14:00 采集、15:10 才写，fetched_at 仍是 14:00。"""
        captured = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)
        write_at = datetime(2026, 10, 27, 15, 10, tzinfo=CN_TZ)
        doc, _, _ = prepare_quote_write(
            {"close": 10.0, "high": 11.0, "low": 9.0, "trading_status": "trading"},
            "600000", "2026-10-27", write_at, 1800, captured_at=captured)
        self.assertTrue(doc["fetched_at"].startswith("2026-10-27T14:00:00"))
        self.assertTrue(doc["updated_at"].startswith("2026-10-27T15:10:00"))
        # 15:10 已过 15:30 之前但超出采集后有效期 → 必须拒绝
        later = datetime(2026, 10, 27, 15, 20, tzinfo=CN_TZ)
        ok, reason = quote_is_usable({**doc, "trade_date": "2026-10-27"}, later)
        self.assertFalse(ok)
        self.assertIn("snapshot_too_old", reason)


class TestWritePathsCoverage(unittest.TestCase):
    def test_all_paths_share_contract(self):
        src = SRC.read_text(encoding="utf-8")
        self.assertNotIn('{"$set": quotes_data}', src)
        # 四条写入路径：批量循环 / 批处理 / bulk_write / 单只保存
        self.assertGreaterEqual(src.count("prepare_quote_write("), 4)
        self.assertGreaterEqual(src.count("_mongo_update_spec("), 4)

    def test_no_raw_set_without_normalization(self):
        src = SRC.read_text(encoding="utf-8")
        for line in src.splitlines():
            if "market_quotes.update_one" in line or "market_quotes.bulk_write" in line:
                continue
            self.assertNotIn('$set": quotes_data}', line)


if __name__ == "__main__":
    unittest.main(verbosity=2)
