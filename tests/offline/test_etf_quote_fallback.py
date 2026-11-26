"""Offline tests for unsynced CN ETF display quote fallback in GET /api/stocks/{code}/quote.

Uses only stdlib stubs inside isolated module loading so it runs cleanly in
minimal offline environments without FastAPI, Pydantic, Motor, or PyMongo.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
STOCKS_PATH = REPO_ROOT / "app/routers/stocks.py"


def _load_isolated_stocks_module():
    fastapi_mod = types.ModuleType("fastapi")

    class _APIRouter:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return lambda fn: fn

        def post(self, *args, **kwargs):
            return lambda fn: fn

    class _HTTPException(Exception):
        def __init__(self, status_code=500, detail=None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi_mod.APIRouter = _APIRouter
    fastapi_mod.Depends = lambda x=None: x
    fastapi_mod.HTTPException = _HTTPException
    fastapi_mod.Query = lambda default=None, **kwargs: default
    fastapi_mod.status = types.SimpleNamespace(
        HTTP_400_BAD_REQUEST=400,
        HTTP_404_NOT_FOUND=404,
        HTTP_500_INTERNAL_SERVER_ERROR=500,
    )

    auth_mod = types.ModuleType("app.routers.auth_db")
    auth_mod.get_current_user = lambda: {"id": "u1"}

    db_mod = types.ModuleType("app.core.database")
    db_mod.get_mongo_db = lambda: None

    resp_mod = types.ModuleType("app.core.response")
    resp_mod.ok = lambda data=None, message="success": {
        "success": True,
        "data": data,
        "message": message,
    }

    ff_mod = types.ModuleType("core.skill_runtime.fundamental_factors")
    ff_mod.get_fundamental_factor_snapshot = lambda *a, **k: {}

    app_pkg = types.ModuleType("app")
    app_pkg.__path__ = []
    app_routers_pkg = types.ModuleType("app.routers")
    app_routers_pkg.__path__ = []
    app_core_pkg = types.ModuleType("app.core")
    app_core_pkg.__path__ = []
    core_pkg = types.ModuleType("core")
    core_pkg.__path__ = []
    core_skill_pkg = types.ModuleType("core.skill_runtime")
    core_skill_pkg.__path__ = []

    stubs = {
        "fastapi": fastapi_mod,
        "app": app_pkg,
        "app.routers": app_routers_pkg,
        "app.routers.auth_db": auth_mod,
        "app.core": app_core_pkg,
        "app.core.database": db_mod,
        "app.core.response": resp_mod,
        "core": core_pkg,
        "core.skill_runtime": core_skill_pkg,
        "core.skill_runtime.fundamental_factors": ff_mod,
    }
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("_offline_stocks_router", STOCKS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod, _HTTPException


class _FakeCollection:
    def __init__(self, doc=None):
        self._doc = doc

    async def find_one(self, *args, **kwargs):
        return self._doc


class _FakeDB:
    def __init__(self, etf_quote=None, etf_basic=None):
        self._cols = {
            "etf_daily_quotes": _FakeCollection(etf_quote),
            "etf_basic_info": _FakeCollection(etf_basic),
        }

    def __getitem__(self, name):
        return self._cols.get(name, _FakeCollection(None))


class EtfQuoteFallbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stocks_router, cls.HTTPException = _load_isolated_stocks_module()

    def test_unsynced_588000_uses_sina_display_fallback_and_never_404s(self):
        fake_db = _FakeDB()
        fallback_payload = {
            "code": "588000",
            "name": "科创50ETF",
            "open": 1.05,
            "pre_close": 1.00,
            "close": 1.06,
            "high": 1.07,
            "low": 1.04,
            "volume": 123456.0,
            "amount": 130000.0,
            "pct_chg": 6.0,
            "trade_date": "2026-11-26",
            "updated_at": "2026-11-26T15:00:00+08:00",
        }
        with patch.object(self.stocks_router, "get_mongo_db", return_value=fake_db), \
             patch.object(self.stocks_router, "_fetch_public_etf_display_quote", AsyncMock(return_value=fallback_payload)):
            resp = asyncio.run(self.stocks_router.get_quote("588000", force_refresh=False, current_user={"id": "u1"}))

        self.assertTrue(resp["success"])
        data = resp["data"]
        self.assertEqual(data["code"], "588000")
        self.assertEqual(data["name"], "科创50ETF")
        self.assertEqual(data["price"], 1.06)
        self.assertEqual(data["change_percent"], 6.0)
        self.assertEqual(data["market"], "ETF")
        self.assertEqual(data["security_type"], "ETF")
        self.assertFalse(data["synced"])
        self.assertEqual(data["data_source"], "sina_l1_fallback")

    def test_unsynced_588000_when_fallback_unavailable_returns_soft_ok(self):
        fake_db = _FakeDB()
        with patch.object(self.stocks_router, "get_mongo_db", return_value=fake_db), \
             patch.object(self.stocks_router, "_fetch_public_etf_display_quote", AsyncMock(return_value=None)):
            resp = asyncio.run(self.stocks_router.get_quote("588000", force_refresh=False, current_user={"id": "u1"}))

        self.assertTrue(resp["success"])
        data = resp["data"]
        self.assertEqual(data["code"], "588000")
        self.assertEqual(data["market"], "ETF")
        self.assertEqual(data["security_type"], "ETF")
        self.assertIsNone(data["price"])
        self.assertFalse(data["synced"])
        self.assertIn("尚未同步", data["hint"])

    def test_synced_159941_reads_db_directly_without_fallback(self):
        fake_db = _FakeDB(
            etf_quote={"code": "159941", "close": 1.496, "pct_chg": 0.5, "trade_date": "2026-11-26"},
            etf_basic={"code": "159941", "name": "纳指ETF广发"},
        )
        fallback_mock = AsyncMock()
        with patch.object(self.stocks_router, "get_mongo_db", return_value=fake_db), \
             patch.object(self.stocks_router, "_fetch_public_etf_display_quote", fallback_mock):
            resp = asyncio.run(self.stocks_router.get_quote("159941", force_refresh=False, current_user={"id": "u1"}))

        fallback_mock.assert_not_awaited()
        data = resp["data"]
        self.assertTrue(data["synced"])
        self.assertEqual(data["name"], "纳指ETF广发")
        self.assertEqual(data["price"], 1.496)
        self.assertEqual(data["security_type"], "ETF")

    def test_non_etf_missing_stock_still_raises_404(self):
        fake_db = _FakeDB()

        class _NoConfigMgr:
            async def get_data_source_configs_async(self):
                return []

        unified_cfg_mod = types.ModuleType("app.core.unified_config")
        unified_cfg_mod.UnifiedConfigManager = _NoConfigMgr
        with patch.dict(sys.modules, {"app.core.unified_config": unified_cfg_mod}), \
             patch.object(self.stocks_router, "get_mongo_db", return_value=fake_db):
            with self.assertRaises(self.HTTPException) as ctx:
                asyncio.run(self.stocks_router.get_quote("600999", force_refresh=False, current_user={"id": "u1"}))
        self.assertEqual(ctx.exception.status_code, 404)

    def test_public_etf_sina_parser_decodes_gb18030_packet(self):
        packet = (
            'var hq_str_sh588000="科创50ETF,1.050,1.000,1.060,1.070,1.040,1.059,1.060,'
            + ",".join(["123400", "130000"] + ["0"] * 20)
            + ',2026-11-26,15:00:00,00";\n'
        ).encode("gb18030")

        class _Resp:
            content = packet

            def raise_for_status(self):
                return None

        class _Client:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url, headers=None):
                self.url = url
                self.headers = headers
                return _Resp()

        httpx_mod = types.ModuleType("httpx")
        httpx_mod.AsyncClient = _Client
        with patch.dict(sys.modules, {"httpx": httpx_mod}):
            parsed = asyncio.run(self.stocks_router._fetch_public_etf_display_quote("588000"))

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["name"], "科创50ETF")
        self.assertEqual(parsed["close"], 1.06)
        self.assertEqual(parsed["pct_chg"], 6.0)
        self.assertEqual(parsed["trade_date"], "2026-11-26")

    def test_public_etf_helper_is_pure_read_only_no_db_writes(self):
        tree = ast.parse(STOCKS_PATH.read_text(encoding="utf-8"))
        fn = next(
            node for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_fetch_public_etf_display_quote"
        )
        src = ast.get_source_segment(STOCKS_PATH.read_text(encoding="utf-8"), fn) or ""
        for forbidden in ("insert_one", "update_one", "get_tushare_provider", "etf_daily_quotes"):
            self.assertNotIn(forbidden, src)


if __name__ == "__main__":
    unittest.main()
