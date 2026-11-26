"""Offline unittest suite for REQ-2 Candidate 4:
- multi_market_stocks.py & unified_stock_service.py ETF whitelist, CN+ETF merged search,
  security_type/market contract, etf_daily_quotes mapping, re.escape & bounded limits.
- stock_data.py /api/stock-data/search ETF merge, multi-source fallback & regex escaping.
- favorites_service.py 56/58/18 exchange recognition, multi-source fallback, and
  etf_daily_quotes / market_quotes -> stock_daily_quotes quote enrichment with quote_date.

Pure Python stdlib (no FastAPI/Motor/PyMongo/Pandas required).
"""

import ast
import asyncio
import copy
import re
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.offline.test_analysis_control_plane_and_etf import (
    FakeCollection,
    FakeCursor,
    FakeDB,
    FakeHTTPException,
    _SilentLogger,
    _TempModules,
    _extract_class_methods,
    _extract_functions,
    _load_stock_utils_namespace,
)


class BoundedFakeCursor(FakeCursor):
    """Enforces that production queries never perform unbounded to_list(None)."""

    def __init__(self, docs: List[Dict[str, Any]], *, forbid_unbounded: bool = True):
        super().__init__(docs)
        self._forbid_unbounded = forbid_unbounded
        self.limit_called = False

    def limit(self, n: int):
        self.limit_called = True
        return super().limit(n)

    async def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        if self._forbid_unbounded and length is None and not self.limit_called:
            raise AssertionError("Unbounded to_list(None) without limit() is forbidden")
        return await super().to_list(length)


class BoundedFakeCollection(FakeCollection):
    def find(self, query: Optional[Dict[str, Any]] = None, projection: Optional[Dict[str, int]] = None) -> BoundedFakeCursor:
        base_cursor = super().find(query, projection)
        return BoundedFakeCursor(base_cursor._docs, forbid_unbounded=True)


class BoundedFakeDB(FakeDB):
    def __init__(self, initial: Optional[Dict[str, List[Dict[str, Any]]]] = None):
        self._collections: Dict[str, BoundedFakeCollection] = {}
        for name, docs in (initial or {}).items():
            self._collections[name] = BoundedFakeCollection(docs)

    def __getitem__(self, name: str) -> BoundedFakeCollection:
        if name not in self._collections:
            self._collections[name] = BoundedFakeCollection()
        return self._collections[name]


def _load_unified_stock_service_cls(su: Dict[str, Any]):
    ns: Dict[str, Any] = {
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "Any": Any,
        "re": re,
        "AsyncIOMotorDatabase": Any,
        "StockUtils": su["StockUtils"],
        "SecurityType": su["SecurityType"],
        "logger": _SilentLogger(),
    }
    return _extract_class_methods(
        ROOT / "app/services/unified_stock_service.py",
        "UnifiedStockService",
        [
            "__init__",
            "get_stock_info",
            "_get_source_priority",
            "get_stock_quote",
            "_search_single_market",
            "search_stocks",
            "get_daily_quotes",
            "get_supported_markets",
        ],
        ns,
    )


class UnifiedStockSearchAndMultiMarketTests(unittest.TestCase):
    def setUp(self):
        self.su = _load_stock_utils_namespace()
        self.UnifiedStockService = _load_unified_stock_service_cls(self.su)
        self.db = BoundedFakeDB({
            "stock_basic_info": [
                {"_id": "oid1", "code": "600519", "symbol": "600519", "name": "贵州茅台", "market": "主板", "source": "akshare"},
                {"_id": "oid2", "code": "600519", "symbol": "600519", "name": "贵州茅台(TS)", "market": "主板", "source": "tushare"},
                {"_id": "oid3", "code": "000004", "symbol": "000004", "name": "*ST国华", "market": "主板", "source": "tushare"},
            ],
            "etf_basic_info": [
                {"_id": "e1", "code": "588000", "symbol": "588000", "name": "科创50ETF", "fund_type": "股票型ETF", "source": "akshare"},
                {"_id": "e2", "code": "560010", "symbol": "560010", "name": "中证1000ETF", "fund_type": "股票型ETF", "source": "tushare"},
                {"_id": "e3", "code": "180101", "symbol": "180101", "name": "蛇口产园REIT", "fund_type": "REITs", "source": "akshare"},
            ],
            "etf_daily_quotes": [
                {"code": "588000", "symbol": "588000", "trade_date": "2026-10-26", "close": 1.05, "pct_chg": 0.5},
                {"code": "588000", "symbol": "588000", "trade_date": "2026-10-27", "close": 1.12, "pct_chg": 2.35},
            ],
            "market_quotes": [],
            "stock_daily_quotes": [
                {"code": "600519", "symbol": "600519", "trade_date": "2026-10-27", "close": 1520.0, "pct_chg": 1.25, "period": "daily"},
                {"code": "600519", "symbol": "600519", "trade_date": "2026-10-30", "close": 9999.0, "pct_chg": 99.0, "period": "weekly"},
            ],
        })

    def test_etf_quotes_collection_mapping_and_latest_quote(self):
        svc = self.UnifiedStockService(self.db)
        self.assertEqual(svc.collection_map["ETF"]["quotes"], "etf_daily_quotes")

        etf_quote = asyncio.run(svc.get_stock_quote("ETF", "588000.SH"))
        self.assertIsNotNone(etf_quote)
        self.assertEqual(etf_quote["close"], 1.12)
        self.assertEqual(etf_quote["quote_date"], "2026-10-27")
        self.assertEqual(etf_quote["security_type"], "ETF")
        self.assertEqual(etf_quote["market"], "ETF")

        # CN stock falls back to stock_daily_quotes when market_quotes is empty
        cn_quote = asyncio.run(svc.get_stock_quote("CN", "600519.SH"))
        self.assertIsNotNone(cn_quote)
        self.assertEqual(cn_quote["close"], 1520.0)
        self.assertEqual(cn_quote["quote_date"], "2026-10-27")
        self.assertEqual(cn_quote["security_type"], "STOCK")

    def test_cn_search_merges_etf_and_carries_security_type_and_market(self):
        svc = self.UnifiedStockService(self.db)

        # Search by 588000 on CN market hits etf_basic_info with security_type=ETF
        hits = asyncio.run(svc.search_stocks("CN", "588000", limit=10))
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["code"], "588000")
        self.assertEqual(hits[0]["security_type"], "ETF")
        self.assertEqual(hits[0]["market"], "ETF")
        self.assertNotIn("_id", hits[0])

        # Search by Chinese ETF name on CN market
        hits_name = asyncio.run(svc.search_stocks("CN", "科创50", limit=10))
        self.assertEqual(len(hits_name), 1)
        self.assertEqual(hits_name[0]["code"], "588000")
        self.assertEqual(hits_name[0]["security_type"], "ETF")

        # Search stock deduplicates by source priority (tushare > akshare) and tags STOCK/CN
        hits_stock = asyncio.run(svc.search_stocks("CN", "贵州茅台", limit=10))
        self.assertEqual(len(hits_stock), 1)
        self.assertEqual(hits_stock[0]["name"], "贵州茅台(TS)")
        self.assertEqual(hits_stock[0]["security_type"], "STOCK")
        self.assertEqual(hits_stock[0]["market"], "CN")

        # Direct ETF market search
        hits_etf_only = asyncio.run(svc.search_stocks("ETF", "560", limit=10))
        self.assertEqual(len(hits_etf_only), 1)
        self.assertEqual(hits_etf_only[0]["code"], "560010")
        self.assertEqual(hits_etf_only[0]["security_type"], "ETF")
        self.assertEqual(hits_etf_only[0]["market"], "ETF")

    def test_regex_special_characters_are_escaped_without_error(self):
        svc = self.UnifiedStockService(self.db)
        hits = asyncio.run(svc.search_stocks("CN", "*ST", limit=10))
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["code"], "000004")
        self.assertEqual(hits[0]["name"], "*ST国华")

        # Unbalanced regex metacharacters must not raise re.error
        hits_meta = asyncio.run(svc.search_stocks("CN", "ETF(50[", limit=10))
        self.assertEqual(hits_meta, [])

    def test_multi_market_router_whitelists_etf_and_rejects_unknown_market(self):
        ns: Dict[str, Any] = {
            "Optional": Optional, "Dict": Dict, "Any": Any, "List": List,
            "Depends": lambda fn=None: fn,
            "Query": lambda default=None, **_k: default,
            "HTTPException": FakeHTTPException,
            "status": types.SimpleNamespace(HTTP_400_BAD_REQUEST=400, HTTP_404_NOT_FOUND=404, HTTP_500_INTERNAL_SERVER_ERROR=500),
            "get_current_user": lambda: None,
            "get_mongo_db": lambda: self.db,
            "ok": lambda data=None, message="ok": {"success": True, "data": data, "message": message},
            "UnifiedStockService": self.UnifiedStockService,
            "StockUtils": self.su["StockUtils"],
            "SUPPORTED_MARKETS": ("CN", "HK", "US", "ETF"),
            "logger": _SilentLogger(),
        }
        _extract_functions(ROOT / "app/routers/multi_market_stocks.py", ["search_stocks", "get_stock_quote"], ns)

        res = asyncio.run(ns["search_stocks"](market="ETF", q="588000", limit=10, include_etf=True, current_user={}))
        self.assertTrue(res["success"])
        self.assertEqual(res["data"]["total"], 1)
        self.assertEqual(res["data"]["stocks"][0]["security_type"], "ETF")

        with self.assertRaises(FakeHTTPException) as ctx:
            asyncio.run(ns["search_stocks"](market="CRYPTO", q="BTC", limit=10, include_etf=True, current_user={}))
        self.assertEqual(ctx.exception.status_code, 400)


class StockDataSearchRouterTests(unittest.TestCase):
    def test_stock_data_search_merges_etf_falls_back_sources_and_escapes_regex(self):
        su = _load_stock_utils_namespace()
        db = BoundedFakeDB({
            "stock_basic_info": [
                # Preferred source 'tushare' has no matching doc -> must fall back to 'akshare'
                {"symbol": "600519", "code": "600519", "name": "贵州茅台", "source": "akshare"},
                {"symbol": "000004", "code": "000004", "name": "*ST国华", "source": "baostock"},
            ],
            "etf_basic_info": [
                {"code": "588000", "name": "科创50ETF", "fund_type": "股票ETF", "source": "akshare"},
                {"code": "180101", "name": "蛇口产园REIT", "fund_type": "REITs", "source": "akshare"},
            ],
        })

        stub_db_mod = types.ModuleType("app.core.database")
        stub_db_mod.get_mongo_db = lambda: db

        stub_dsp = types.ModuleType("app.core.data_source_priority")
        stub_dsp.get_preferred_data_source_async = AsyncMock(return_value="tushare")
        stub_dsp.get_enabled_data_sources_async = AsyncMock(return_value=["tushare", "akshare", "baostock"])

        stub_su = types.ModuleType("tradingagents.utils.stock_utils")
        stub_su.StockUtils = su["StockUtils"]
        stub_su.SecurityType = su["SecurityType"]
        stub_su.ETF_SH_PREFIXES = su["ETF_SH_PREFIXES"]
        stub_su.ETF_SZ_PREFIXES = su["ETF_SZ_PREFIXES"]

        fake_stock_data_svc = types.SimpleNamespace(
            _standardize_basic_info=lambda d: {
                "symbol": d.get("symbol") or d.get("code"),
                "code": d.get("code") or d.get("symbol"),
                "name": d.get("name"),
                "source": d.get("source"),
            }
        )

        ns: Dict[str, Any] = {
            "Dict": Dict, "List": List, "Any": Any, "Optional": Optional,
            "Query": lambda default=None, **_k: default,
            "Depends": lambda fn=None: fn,
            "HTTPException": FakeHTTPException,
            "status": types.SimpleNamespace(HTTP_500_INTERNAL_SERVER_ERROR=500),
            "get_current_user": lambda: None,
            "get_stock_data_service": lambda: fake_stock_data_svc,
        }
        _extract_functions(ROOT / "app/routers/stock_data.py", ["_standardize_etf_search_doc", "search_stocks"], ns)

        with _TempModules({
            "app.core.database": stub_db_mod,
            "app.core.data_source_priority": stub_dsp,
            "tradingagents.utils.stock_utils": stub_su,
        }):
            # 1) ETF exact code & name
            res_etf = asyncio.run(ns["search_stocks"](keyword="588000.SH", limit=10, current_user={}))
            self.assertTrue(res_etf["success"])
            self.assertEqual(res_etf["total"], 1)
            self.assertEqual(res_etf["data"][0]["security_type"], "ETF")
            self.assertEqual(res_etf["data"][0]["full_symbol"], "588000.SH")
            self.assertEqual(res_etf["data"][0]["sse"], "上海证券交易所")

            res_sz_etf = asyncio.run(ns["search_stocks"](keyword="180101", limit=10, current_user={}))
            self.assertEqual(res_sz_etf["data"][0]["full_symbol"], "180101.SZ")
            self.assertEqual(res_sz_etf["data"][0]["sse"], "深圳证券交易所")

            # 2) Multi-source fallback (tushare empty -> akshare)
            res_cn = asyncio.run(ns["search_stocks"](keyword="贵州茅台", limit=10, current_user={}))
            self.assertEqual(res_cn["total"], 1)
            self.assertEqual(res_cn["data"][0]["symbol"], "600519")
            self.assertEqual(res_cn["source"], "akshare")

            # 3) Regex special character safety (*ST)
            res_st = asyncio.run(ns["search_stocks"](keyword="*ST", limit=10, current_user={}))
            self.assertEqual(res_st["total"], 1)
            self.assertEqual(res_st["data"][0]["symbol"], "000004")


class FavoritesServiceEtfAndQuoteFallbackTests(unittest.TestCase):
    def test_favorites_etf_exchanges_and_quote_daily_fallback(self):
        su = _load_stock_utils_namespace()
        uid = "user-fav-1"
        db = BoundedFakeDB({
            "user_favorites": [{
                "user_id": uid,
                "favorites": [
                    {"stock_code": "588000", "stock_name": "科创50ETF", "market": "A股", "tags": []},
                    {"stock_code": "560010", "stock_name": "中证1000ETF", "market": "ETF", "tags": []},
                    {"stock_code": "180101", "stock_name": "180101", "market": "ETF", "tags": []},
                    {"stock_code": "600519", "stock_name": "贵州茅台", "market": "A股", "tags": []},
                    {"stock_code": "000001", "stock_name": "平安银行", "market": "A股", "tags": []},
                ],
            }],
            "stock_basic_info": [
                # Only akshare populated (not preferred tushare) -> tests multi-source fallback
                {"code": "600519", "name": "贵州茅台", "market": "主板", "sse": "上海证券交易所", "source": "akshare"},
                {"code": "000001", "name": "平安银行", "market": "主板", "sse": "深圳证券交易所", "source": "baostock"},
            ],
            "etf_basic_info": [
                {"code": "588000", "name": "科创50ETF"},
                {"code": "560010", "name": "中证1000ETF"},
                {"code": "180101", "name": "蛇口产园REIT"},
            ],
            # market_quotes only has 000001; 600519 & all ETFs must fall back cleanly
            "market_quotes": [
                {"code": "000001", "close": 11.50, "pct_chg": 0.88, "volume": 100000, "trade_date": "2026-10-27"},
            ],
            "etf_daily_quotes": [
                {"code": "588000", "trade_date": "2026-10-26", "close": 1.00, "pct_chg": 0.0, "volume": 50000},
                {"code": "588000", "trade_date": "2026-10-27", "close": 1.10, "pct_chg": None, "volume": 60000},
                {"code": "560010", "trade_date": "2026-10-27", "close": 2.45, "pct_chg": -1.2, "volume": 30000},
                {"code": "180101", "trade_date": "2026-10-27", "close": 3.18, "pct_chg": 0.63, "volume": 12000},
            ],
            "stock_daily_quotes": [
                {"symbol": "600519", "trade_date": "2026-10-27", "close": 1580.0, "pct_chg": 2.15, "volume": 25000, "period": "daily"},
                # Weekly bar on a later synthetic date must be ignored by daily_period_only=True
                {"symbol": "600519", "trade_date": "2026-10-30", "close": 9999.0, "pct_chg": 99.0, "period": "weekly"},
            ],
        })

        stub_uc = types.ModuleType("app.core.unified_config")
        stub_uc.UnifiedConfigManager = lambda: types.SimpleNamespace(
            get_data_source_configs_async=AsyncMock(return_value=[
                types.SimpleNamespace(type="tushare", enabled=True),
                types.SimpleNamespace(type="akshare", enabled=True),
                types.SimpleNamespace(type="baostock", enabled=True),
            ])
        )

        ns: Dict[str, Any] = {
            "asyncio": asyncio,
            "Dict": Dict, "List": List, "Any": Any, "Optional": Optional,
            "datetime": datetime,
            "ObjectId": str,
            "get_mongo_db": lambda: db,
            "StockUtils": su["StockUtils"],
            "StockMarket": su["StockMarket"],
            "SecurityType": su["SecurityType"],
            "ETF_SH_PREFIXES": su["ETF_SH_PREFIXES"],
            "ETF_SZ_PREFIXES": su["ETF_SZ_PREFIXES"],
            "logger": _SilentLogger(),
        }
        _extract_functions(
            ROOT / "app/services/favorites_service.py",
            ["_normalize_quote_date", "_resolve_etf_exchange", "_extract_quote_metrics"],
            ns,
        )
        FavCls = _extract_class_methods(
            ROOT / "app/services/favorites_service.py",
            "FavoritesService",
            ["__init__", "_get_db", "_is_valid_object_id", "_format_favorite", "_fetch_latest_daily_quotes", "get_user_favorites"],
            ns,
        )
        svc = FavCls()

        with _TempModules({"app.core.unified_config": stub_uc}):
            items = asyncio.run(svc.get_user_favorites(uid))

        by_code = {it["stock_code"]: it for it in items}

        # 56/58/18 ETF exchanges + etf_daily_quotes enrichment
        self.assertEqual(by_code["588000"]["exchange"], "上海证券交易所")
        self.assertEqual(by_code["588000"]["board"], "ETF")
        self.assertEqual(by_code["588000"]["current_price"], 1.10)
        # pct_chg computed from prev close (1.00 -> 1.10 = +10.0%)
        self.assertEqual(by_code["588000"]["change_percent"], 10.0)
        self.assertEqual(by_code["588000"]["quote_date"], "2026-10-27")

        self.assertEqual(by_code["560010"]["exchange"], "上海证券交易所")
        self.assertEqual(by_code["560010"]["current_price"], 2.45)
        self.assertEqual(by_code["560010"]["change_percent"], -1.2)
        self.assertEqual(by_code["560010"]["quote_date"], "2026-10-27")

        self.assertEqual(by_code["180101"]["exchange"], "深圳证券交易所")
        self.assertEqual(by_code["180101"]["stock_name"], "蛇口产园REIT")
        self.assertEqual(by_code["180101"]["current_price"], 3.18)
        self.assertEqual(by_code["180101"]["change_percent"], 0.63)
        self.assertEqual(by_code["180101"]["quote_date"], "2026-10-27")

        # 600519 falls back from empty market_quotes to stock_daily_quotes (daily bar only)
        self.assertEqual(by_code["600519"]["exchange"], "上海证券交易所")
        self.assertEqual(by_code["600519"]["current_price"], 1580.0)
        self.assertEqual(by_code["600519"]["change_percent"], 2.15)
        self.assertEqual(by_code["600519"]["quote_date"], "2026-10-27")

        # 000001 uses market_quotes directly
        self.assertEqual(by_code["000001"]["exchange"], "深圳证券交易所")
        self.assertEqual(by_code["000001"]["current_price"], 11.50)
        self.assertEqual(by_code["000001"]["change_percent"], 0.88)
        self.assertEqual(by_code["000001"]["quote_date"], "2026-10-27")


class ModifiedFilesSyntaxGuardTests(unittest.TestCase):
    def test_all_req2_modified_python_files_parse_cleanly(self):
        rel_paths = [
            "tradingagents/utils/stock_utils.py",
            "app/routers/analysis.py",
            "app/routers/unified_tasks.py",
            "app/services/simple_analysis_service.py",
            "app/services/thread_worker.py",
            "app/worker/analysis_worker.py",
            "app/routers/multi_market_stocks.py",
            "app/services/unified_stock_service.py",
            "app/routers/stock_data.py",
            "app/services/favorites_service.py",
        ]
        for rel in rel_paths:
            path = ROOT / rel
            source = path.read_text(encoding="utf-8")
            ast.parse(source, filename=str(path))


if __name__ == "__main__":
    unittest.main()
