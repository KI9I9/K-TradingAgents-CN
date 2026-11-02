"""真实 paper.py 取价函数的行为测试（AST 抽取 + 假数据库）。

不启动 FastAPI、不连 MongoDB：把 `_load_cn_candidates` / `_get_last_price_detail` /
`_get_today_price_range_detail` / `_build_limit_notice` 从源文件抽取后执行，
数据库用内存对象替代。这样测的是**真实函数体**，不是重写的等价逻辑。
"""

import ast
import asyncio
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

sys.path.insert(0, ".")

from app.services.quote_contract import (  # noqa: E402
    CN_TZ,
    cn_now,
    resolve_cn_price,
    resolve_cn_range,
    validate_price,
)

PAPER = Path("app/routers/paper.py")
NOW = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)
CLOSE = datetime(2026, 10, 27, 15, 30, tzinfo=CN_TZ)


class FakeCollection:
    def __init__(self, doc):
        self._doc = doc

    async def find_one(self, query=None, projection=None, sort=None, **kw):
        """遵守 MongoDB 投影语义：投影里没有的字段不会返回。

        之前原样返回整条记录，掩盖了「投影漏字段 → 准入判断误拒」的缺陷。
        """
        if not self._doc or not projection:
            return self._doc
        keep = {k for k, v in projection.items() if v and k != "_id"}
        return {k: v for k, v in self._doc.items() if k in keep}


class FakeDB:
    def __init__(self, mapping):
        self._mapping = mapping

    def __getitem__(self, name):
        return FakeCollection(self._mapping.get(name))


def load_paper_functions(names, db_mapping, now, extra_namespace=None):
    """从真实源文件抽取函数并执行，注入假 DB 与固定时钟。"""
    tree = ast.parse(PAPER.read_text(encoding="utf-8"))
    wanted = []
    fn_types = (ast.FunctionDef, ast.AsyncFunctionDef)
    for node in tree.body:
        if isinstance(node, fn_types) and node.name in names:
            wanted.append(node)
    found = {n.name for n in wanted}
    missing = set(names) - found
    if missing:
        raise AssertionError(f"paper.py 缺少函数: {sorted(missing)}")

    module = ast.Module(body=wanted, type_ignores=[])
    ast.fix_missing_locations(module)

    namespace = {
        "Dict": Dict, "Any": Any, "Optional": Optional, "Tuple": Tuple,
        "datetime": datetime, "timedelta": timedelta,
        "cn_now": lambda: now,
        "resolve_cn_price": resolve_cn_price,
        "resolve_cn_range": resolve_cn_range,
        "validate_price": validate_price,
        "get_mongo_db": lambda: FakeDB(db_mapping),
        "logger": _SilentLogger(),
    }
    namespace.update(extra_namespace or {})
    exec(compile(module, str(PAPER), "exec"), namespace)
    return namespace


class _SilentLogger:
    def warning(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


def quote_doc(**kw):
    base = {
        "code": "600000", "trade_date": "2026-10-27",
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
        "quote_time_source": "fetch",
        "trading_status": "trading",
        "fetched_at": (NOW - timedelta(minutes=10)).isoformat(),
        "snapshot_interval_seconds": 1800,
        "updated_at": NOW.isoformat(),
    }
    base.update(kw)
    return base


def bar_doc(**kw):
    base = {
        "trade_date": "2026-10-27", "open": 9.5, "high": 11.0, "low": 9.0,
        "close": 10.0, "is_final": True, "trading_status": "trading",
        "updated_at": "2026-10-27T15:10:00+08:00",
    }
    base.update(kw)
    return base


class TestPaperPriceChain(unittest.TestCase):
    def _run(self, mapping, now=NOW, names=("_load_cn_candidates", "_get_last_price_detail")):
        ns = load_paper_functions(names, mapping, now)
        return ns

    def test_invalid_quote_price_falls_back_to_daily_with_daily_basis(self):
        """评审复现的反例：A 价配 B 说明。"""
        fresh = quote_doc(close=0, fetched_at=(CLOSE - timedelta(minutes=10)).isoformat())
        ns = self._run({"market_quotes": fresh, "stock_daily_quotes": bar_doc()}, now=CLOSE)
        price, basis, reason = asyncio.run(ns["_get_last_price_detail"]("600000", "CN", True))
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["source"], "stock_daily_quotes")
        self.assertEqual(reason, "ok")

    def test_fresh_quote_preferred(self):
        ns = self._run({"market_quotes": quote_doc(), "stock_daily_quotes": bar_doc()})
        price, basis, _ = asyncio.run(ns["_get_last_price_detail"]("600000", "CN", True))
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["source"], "market_quotes")

    def test_stale_quote_intraday_rejected(self):
        stale = quote_doc(fetched_at=(NOW - timedelta(minutes=40)).isoformat())
        ns = self._run({"market_quotes": stale, "stock_daily_quotes": None})
        price, basis, reason = asyncio.run(ns["_get_last_price_detail"]("600000", "CN", True))
        self.assertIsNone(price)
        self.assertIsNone(basis)
        self.assertIn("snapshot_too_old", reason)

    def test_display_returns_etf_close_intraday(self):
        """评审 P2-6：展示路径不应被成交闸门收紧。"""
        etf = {"close": 3.14, "trade_date": "2026-10-24",
               "updated_at": "2026-10-24T15:30:00+08:00"}
        ns = self._run({"market_quotes": None, "stock_basic_info": None,
                        "etf_daily_quotes": etf})
        price, basis, reason = asyncio.run(ns["_get_last_price_detail"]("159865", "ETF", False))
        self.assertEqual(price, 3.14)
        self.assertEqual(reason, "display")
        self.assertEqual(basis["channel"], "display")

    def test_display_uses_basic_info_when_no_quote(self):
        ns = self._run({"market_quotes": None, "stock_basic_info": {"current_price": 8.8}})
        price, _, _ = asyncio.run(ns["_get_last_price_detail"]("600000", "CN", False))
        self.assertEqual(price, 8.8)


class TestPaperRangeChain(unittest.TestCase):
    def test_range_from_quote_after_close(self):
        ns = load_paper_functions(
            ("_load_cn_candidates", "_get_today_price_range_detail"), {}, CLOSE)
        ns["get_mongo_db"] = lambda: FakeDB(
            {"market_quotes": quote_doc(fetched_at=(CLOSE - timedelta(minutes=10)).isoformat())})
        rng, basis, reason = asyncio.run(ns["_get_today_price_range_detail"]("600000", "CN"))
        self.assertEqual(reason, "ok")
        self.assertEqual(rng, {"high": 11.0, "low": 9.0})
        self.assertEqual(basis["source"], "market_quotes")

    def test_limit_notice_mentions_specified_price(self):
        ns = load_paper_functions(("_build_limit_notice", "_format_hhmmss"), {}, NOW)
        text = ns["_build_limit_notice"](
            10.5,
            {"channel": "fetch", "as_of": "2026-10-27T14:00:00+08:00",
             "delay_seconds": 600, "max_delay_seconds": 1920},
            11.0, 9.0)
        self.assertIn("指定", text)
        self.assertIn("10.50", text)
        self.assertIn("14:00:00", text)
        self.assertIn("有效期", text)
        self.assertIn("上游报价时间未知", text)

    def test_limit_notice_upstream_does_not_claim_unknown(self):
        ns = load_paper_functions(("_build_limit_notice", "_format_hhmmss"), {}, NOW)
        text = ns["_build_limit_notice"](
            10.5, {"channel": "upstream", "as_of": "2026-10-27T13:50:00+08:00"}, 11.0, 9.0)
        self.assertIn("上游行情时间 13:50:00", text)
        self.assertNotIn("上游报价时间未知", text)

    def test_display_falls_back_when_quote_price_invalid(self):
        """评审 P2-5：应按「价格不可用」回退，而不是「记录不存在」。"""
        ns = load_paper_functions(
            ("_load_cn_candidates", "_get_last_price_detail"),
            {"market_quotes": quote_doc(close=0), "stock_basic_info": {"current_price": 10.0}},
            NOW)
        price, basis, reason = asyncio.run(ns["_get_last_price_detail"]("600000", "CN", False))
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["source"], "stock_basic_info")


class TestPaperStructure(unittest.TestCase):
    def test_no_second_query_for_basis(self):
        """定价依据只能来自取价那条记录；不允许再有 _get_quote_basis 二次查库。"""
        src = PAPER.read_text(encoding="utf-8")
        self.assertNotIn("_get_quote_basis", src)
        self.assertIn("_get_last_price_detail", src)
        self.assertIn("_get_today_price_range_detail", src)

    def test_order_uses_detail_and_notice(self):
        src = PAPER.read_text(encoding="utf-8")
        self.assertIn("pricing_notice", src)
        self.assertIn("_build_limit_notice", src)
        self.assertNotIn('response_payload["pricing_notice"] = pricing_basis["notice"]', src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
