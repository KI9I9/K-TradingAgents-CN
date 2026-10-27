"""真实同步入口测试：AST 抽取 AkShareSyncService.sync_realtime_quotes，注入假 self。

覆盖第四轮评审指出的、之前只测到契约层而漏掉的分支：
- 单股失败 → 转全量批量（曾在 captured_at 未赋值处抛 UnboundLocalError）；
- 全部拒写 → 必须 success=False（否则调度层记成成功）；
- 空标的池 → 失败；
- 盘前 → 整轮跳过且零写入。
"""

import ast
import asyncio
import os
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, ".")
os.environ.setdefault("QUOTES_WRITE_ENABLED", "1")

from app.services.quote_contract import (  # noqa: E402
    CN_TZ,
    build_failure_stats,
    cn_now,
    prepare_quote_write,
    resolve_trade_date,
    sync_round_skip_reason,
)

SRC = Path("app/worker/akshare_sync_service.py")
NOW = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)


class _Logger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


def _extract_methods(names):
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    found = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.AsyncFunctionDef) and item.name in names:
                    found[item.name] = item
    missing = set(names) - set(found)
    if missing:
        raise AssertionError(f"缺少方法: {sorted(missing)}")
    module = ast.Module(body=list(found.values()), type_ignores=[])
    ast.fix_missing_locations(module)
    return module


def load_entry(now):
    """把真实方法编译进一个命名空间，返回可直接 await 的函数。"""
    module = _extract_methods({"sync_realtime_quotes"})
    ns: Dict[str, Any] = {
        "cn_now": lambda: now,
        "sync_round_skip_reason": sync_round_skip_reason,
        "resolve_trade_date": resolve_trade_date,
        "prepare_quote_write": prepare_quote_write,
        "build_failure_stats": build_failure_stats,
        "logger": _Logger(),
        "datetime": datetime,
        "timedelta": timedelta,
        "asyncio": asyncio,
        "Any": Any,
        "Dict": Dict,
        "List": list,
        "Optional": Optional,
    }
    exec(compile(module, str(SRC), "exec"), ns)
    return ns["sync_realtime_quotes"]


class FakeCursor:
    def __init__(self, doc):
        self._doc = doc

    async def find_one(self, *a, **k):
        return self._doc

    async def update_one(self, *a, **k):
        return types.SimpleNamespace(matched_count=0, modified_count=0, upserted_id=None)


class FakeDB:
    def __init__(self):
        self.writes = []
        self.market_quotes = FakeMarketQuotes(self)

    def __getitem__(self, name):
        return self.market_quotes


class FakeMarketQuotes:
    def __init__(self, db):
        self.db = db

    async def find_one(self, *a, **k):
        return None

    async def update_one(self, query, spec, upsert=False):
        self.db.writes.append(spec)
        return types.SimpleNamespace(matched_count=1, modified_count=1, upserted_id=None)


class FakeProvider:
    def __init__(self, quote=None, batch=None):
        self._quote = quote
        self._batch = batch or {}
        self.batch_calls = 0

    async def get_stock_quotes(self, symbol):
        return self._quote

    async def get_batch_stock_quotes(self, symbols):
        self.batch_calls += 1
        return self._batch


class FakeSelf:
    def __init__(self, provider, listed, now):
        self.provider = provider
        self._listed = listed
        self._now = now
        self.db = FakeDB()
        self.snapshot_interval_seconds = 1800
        self.batch_size = 100
        self.rate_limit_delay = 0
        self._run_trade_date = None
        self.batch_calls = []

    def _resolve_job_id(self, manual_mode):
        return None

    async def _get_all_listed_symbols(self):
        return list(self._listed)

    def _is_data_fresh(self, value, minutes=15):
        return False

    def _is_data_complete(self, doc):
        return False

    async def _get_and_save_quotes(self, symbol):
        return False

    async def _batch_update_quotes_to_db(self, quotes_map, captured_at=None):
        self.batch_calls.append(captured_at)
        return len(quotes_map)


def install_calendar(is_trade_day=True, source="akshare"):
    mod = types.ModuleType("app.services.trading_calendar_service")

    class Cal:
        async def is_today_trade_day(self):
            return is_trade_day

        async def _read_cache(self):
            return {"source": source}

    mod.get_trading_calendar_service = lambda: Cal()
    sys.modules["app.services.trading_calendar_service"] = mod


class TestSyncEntry(unittest.TestCase):
    def tearDown(self):
        sys.modules.pop("app.services.trading_calendar_service", None)

    def test_single_failure_falls_back_without_unbound_error(self):
        """评审 P1-2：单股失败转批量时 captured_at 必须已赋值。"""
        install_calendar()
        provider = FakeProvider(
            quote=None,
            batch={"600000": {"close": 10.0, "high": 11.0, "low": 9.0,
                              "trading_status": "trading"}},
        )
        fake = FakeSelf(provider, ["600000"], NOW)
        entry = load_entry(NOW)
        stats = asyncio.run(entry(fake, symbols=["600000"], force=True))
        self.assertEqual(fake.batch_calls and fake.batch_calls[0] is not None, True)
        self.assertEqual(stats["error_count"], 0)
        self.assertEqual(stats["success_count"], 1)

    def test_all_rejected_marks_failed(self):
        """两只见不完整行情 → 全部拒写 → 必须 success=False。"""
        install_calendar()
        provider = FakeProvider(
            batch={"600000": {"close": None, "high": 11.0, "low": 9.0},
                   "000001": {"close": None, "high": 5.0, "low": 4.0}},
        )
        fake = FakeSelf(provider, ["600000", "000001"], NOW)
        entry = load_entry(NOW)
        stats = asyncio.run(entry(fake, symbols=["600000", "000001"], force=True))
        self.assertEqual(stats["success_count"], 0)
        self.assertEqual(stats["error_count"], 2)
        self.assertIs(stats.get("success"), False)
        self.assertEqual(fake.db.writes, [])

    def test_empty_pool_is_failure(self):
        install_calendar()
        fake = FakeSelf(FakeProvider(batch={}), [], NOW)
        entry = load_entry(NOW)
        stats = asyncio.run(entry(fake, symbols=None, force=True))
        self.assertIs(stats.get("success"), False)
        self.assertGreaterEqual(stats["error_count"], 1)

    def test_pre_open_skips_and_writes_nothing(self):
        install_calendar()
        pre_open = datetime(2026, 10, 27, 9, 0, tzinfo=CN_TZ)
        fake = FakeSelf(FakeProvider(batch={}), ["600000"], pre_open)
        entry = load_entry(pre_open)
        stats = asyncio.run(entry(fake, symbols=["600000"], force=True))
        self.assertEqual(stats.get("reason"), "pre_open")
        self.assertEqual(fake.db.writes, [])
        self.assertEqual(fake.batch_calls, [])

    def test_calendar_fallback_skips(self):
        install_calendar(source="fallback")
        fake = FakeSelf(FakeProvider(batch={}), ["600000"], NOW)
        entry = load_entry(NOW)
        stats = asyncio.run(entry(fake, symbols=["600000"], force=True))
        self.assertEqual(stats.get("reason"), "calendar_unverified")
        self.assertEqual(fake.db.writes, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
