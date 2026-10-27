"""真实同步入口测试：AST 抽取 AkShareSyncService.sync_realtime_quotes，注入假 self。

覆盖第四轮评审指出的、之前只测到契约层而漏掉的分支：
- 单股失败 → 转全量批量（曾在 captured_at 未赋值处抛 UnboundLocalError）；
- 全部拒写 → 必须 success=False（否则调度层记成成功）；
- 空标的池 → 失败；
- 盘前 → 整轮跳过且零写入。
"""

import ast
import asyncio
import pathlib
import os
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Set
from unittest.mock import patch

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
        "Set": Set,
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
        return set(quotes_map)


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


def bind_real_writers(fake, now=NOW):
    """执行真实入口、单只保存、批量写入和更新构造，仅替换外部依赖。"""
    entry = load_entry(now)
    ns = entry.__globals__
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    spec = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == "_mongo_update_spec")
    exec(compile(ast.Module(body=[spec], type_ignores=[]), str(SRC), "exec"), ns)
    ns["pymongo"] = types.SimpleNamespace(
        UpdateOne=lambda query, update, upsert: (query, update, upsert))
    exec(compile(_extract_methods({"_batch_update_quotes_to_db", "_get_and_save_quotes"}),
                 str(SRC), "exec"), ns)
    for name in ("_batch_update_quotes_to_db", "_get_and_save_quotes"):
        setattr(fake, name, types.MethodType(ns[name], fake))
    return entry


class TestConfirmedWrites(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"QUOTES_WRITE_ENABLED": "1"})
        self.env.start()
        self.addCleanup(self.env.stop)
        install_calendar()
        self.addCleanup(sys.modules.pop, "app.services.trading_calendar_service", None)
        self.quote = {"close": 10.0, "high": 11.0, "low": 9.0}
        self.fake = FakeSelf(FakeProvider(batch={"600000": dict(self.quote)}), ["600000"], NOW)
        self.entry = bind_real_writers(self.fake)
        self.bulk_calls = []

        async def bulk(operations, ordered=False):
            self.bulk_calls.append(operations)
            return types.SimpleNamespace(acknowledged=True, matched_count=len(operations),
                                         modified_count=0, upserted_count=0)
        self.fake.db.market_quotes.bulk_write = bulk

    def run_entry(self):
        return asyncio.run(self.entry(self.fake, symbols=["600000"], manual_mode=True))

    def test_closed_gate_manual_fallback_has_no_writes_or_success(self):
        os.environ["QUOTES_WRITE_ENABLED"] = "0"
        stats = self.run_entry()
        self.assertIs(stats.get("success"), False)
        self.assertEqual(stats["success_count"], 0)
        self.assertEqual(self.bulk_calls, [])
        self.assertEqual(self.fake.db.writes, [])

    def test_database_exception_does_not_report_success(self):
        async def fail(*a, **k):
            raise RuntimeError("simulated database failure")
        self.fake.db.market_quotes.bulk_write = fail
        stats = self.run_entry()
        self.assertEqual(stats["success_count"], 0)
        self.assertIs(stats.get("success"), False)

    def test_acknowledged_idempotent_write_is_success(self):
        stats = self.run_entry()
        self.assertEqual(stats["success_count"], 1)
        self.assertEqual(stats["error_count"], 0)
        self.assertEqual(len(self.bulk_calls), 1)

    def test_unacknowledged_write_is_not_success(self):
        async def unacknowledged(*a, **k):
            return types.SimpleNamespace(acknowledged=False)
        self.fake.db.market_quotes.bulk_write = unacknowledged
        self.assertIs(self.run_entry().get("success"), False)

    def test_inconsistent_acknowledged_count_is_not_success(self):
        async def missing(*a, **k):
            return types.SimpleNamespace(acknowledged=True, matched_count=0, upserted_count=0)
        self.fake.db.market_quotes.bulk_write = missing
        self.assertIs(self.run_entry().get("success"), False)

    def test_rejected_target_not_success_when_other_symbol_written(self):
        self.fake.provider._batch = {"600000": {"close": None}, "600001": self.quote}
        self.assertIs(self.run_entry().get("success"), False)
        self.assertEqual(len(self.bulk_calls[0]), 1)

    def test_single_provider_batch_requires_target_confirmation(self):
        self.fake.provider._quote = {
            "_from_batch": True, "_batch_data": {"600001": self.quote}}
        self.assertFalse(asyncio.run(self.fake._get_and_save_quotes("600000")))
        self.assertEqual(len(self.bulk_calls), 1)

    def test_later_failed_batch_preserves_only_prior_confirmations(self):
        async def bulk(operations, ordered=False):
            self.bulk_calls.append(operations)
            if len(self.bulk_calls) == 2:
                raise RuntimeError("second batch failed, possibly partially written")
            return types.SimpleNamespace(acknowledged=True, matched_count=len(operations),
                                         modified_count=0, upserted_count=0)
        self.fake.db.market_quotes.bulk_write = bulk
        payload = {str(600000 + i): self.quote for i in range(101)}
        confirmed = asyncio.run(self.fake._batch_update_quotes_to_db(payload, NOW))
        self.assertEqual(confirmed, set(list(payload)[:100]))

    def test_independent_calls_do_not_share_confirmation_state(self):
        async def concurrent():
            return await asyncio.gather(
                self.fake._batch_update_quotes_to_db({"600000": self.quote}, NOW),
                self.fake._batch_update_quotes_to_db({"600001": self.quote}, NOW))
        self.assertEqual(asyncio.run(concurrent()), [{"600000"}, {"600001"}])


class TestWriteGateCoverage(unittest.TestCase):
    """写闸必须覆盖所有已知 market_quotes 写入点，而不是只覆盖 AKShare 批量路径。"""

    WRITERS = (
        "app/routers/stock_sync.py",
        "app/services/quotes_ingestion_service.py",
        "app/services/stock_data_service.py",
        "app/worker/qmt_sync_service.py",
        "app/worker/akshare_sync_service.py",
    )

    def test_all_known_writers_reference_gate(self):
        for path in self.WRITERS:
            text = pathlib.Path(path).read_text(encoding="utf-8")
            self.assertTrue("quotes_write_enabled" in text or "prepare_quote_write" in text, path)

    def test_other_writers_refuse_without_database_access(self):
        def forbidden_db():
            raise AssertionError("写闸关闭时不得取得数据库")

        class ForbiddenDB:
            def __getattr__(self, name):
                raise AssertionError("写闸关闭时不得访问集合")

        adapter = types.SimpleNamespace(
            is_available=lambda: True,
            get_realtime_quotes=lambda: {"600000": {"close": 10}})
        cases = [
            ("app/services/quotes_ingestion_service.py", "_bulk_upsert",
             ({"600000": {"close": 10}}, "2026-10-27")),
            ("app/services/stock_data_service.py", "update_market_quotes",
             ("600000", {"close": 10})),
            ("app/worker/qmt_sync_service.py", "sync_realtime_quotes", ()),
        ]
        for path, name, args in cases:
            with self.subTest(writer=path):
                tree = ast.parse(pathlib.Path(path).read_text(encoding="utf-8"))
                node = next(n for n in ast.walk(tree)
                            if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
                ns = {"quotes_write_enabled": lambda: False, "get_mongo_db": forbidden_db,
                      "logger": _Logger(), "datetime": datetime, "Dict": Dict, "Any": Any,
                      "Optional": Optional, "List": list}
                exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), ns)
                fake = types.SimpleNamespace(adapter=adapter, db=ForbiddenDB())
                asyncio.run(ns[name](fake, *args))

    def test_history_backfill_skips_when_gate_closed(self):
        """个股历史同步回写 market_quotes 不得绕过写闸。"""
        src = pathlib.Path("app/routers/stock_sync.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        node = None
        for item in tree.body:
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "_sync_latest_to_market_quotes":
                node = item
                break
        self.assertIsNotNone(node, "找不到 _sync_latest_to_market_quotes")

        module = ast.Module(body=[node], type_ignores=[])
        ast.fix_missing_locations(module)

        touched = {"db": False}

        def forbidden_db():
            touched["db"] = True
            raise AssertionError("写闸关闭时不应访问数据库")

        ns = {
            "quotes_write_enabled": lambda: False,
            "get_mongo_db": forbidden_db,
            "logger": _Logger(),
            "str": str,
            "Dict": Dict,
            "Any": Any,
            "datetime": datetime,
        }
        exec(compile(module, "stock_sync.py", "exec"), ns)
        result = asyncio.run(ns["_sync_latest_to_market_quotes"]("600000"))
        self.assertIsNone(result)
        self.assertFalse(touched["db"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
