"""Offline tests for ETF validation details/messages and skipping ETF corporate financials in single sync."""
from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_PATH = REPO_ROOT / "app/services/data_validation_service.py"
STOCK_SYNC_PATH = REPO_ROOT / "app/routers/stock_sync.py"


def _load_isolated_validation_module():
    core_cfg = types.ModuleType("app.core.config")
    core_cfg.settings = SimpleNamespace(TIMEZONE="Asia/Shanghai")

    core_db = types.ModuleType("app.core.database")
    core_db.get_mongo_db = lambda: None

    stubs = {
        "app": types.ModuleType("app"),
        "app.core": types.ModuleType("app.core"),
        "app.core.config": core_cfg,
        "app.core.database": core_db,
    }
    stubs["app"].__path__ = []
    stubs["app.core"].__path__ = []

    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("_offline_data_validation", VALIDATION_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


def _load_isolated_stock_sync_module():
    fastapi_mod = types.ModuleType("fastapi")

    class _DummyRouter:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return lambda fn: fn

        def post(self, *args, **kwargs):
            return lambda fn: fn

    class _HTTPException(Exception):
        def __init__(self, status_code: int, detail: str = ""):
            self.status_code = status_code
            self.detail = detail
            super().__init__(detail)

    fastapi_mod.APIRouter = _DummyRouter
    fastapi_mod.Depends = lambda x: x
    fastapi_mod.HTTPException = _HTTPException
    fastapi_mod.Query = lambda *args, **kwargs: None

    pydantic_mod = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **kwargs):
            for key, val in kwargs.items():
                setattr(self, key, val)

        def model_dump(self):
            return dict(self.__dict__)

        def dict(self):
            return dict(self.__dict__)

    pydantic_mod.BaseModel = _BaseModel
    pydantic_mod.Field = lambda default=..., **kwargs: default

    auth_db = types.ModuleType("app.routers.auth_db")
    auth_db.get_current_user = lambda: {}

    core_resp = types.ModuleType("app.core.response")
    core_resp.ok = lambda data=None, message="": {"success": True, "data": data, "message": message}

    core_db = types.ModuleType("app.core.database")
    core_db.get_mongo_db = lambda: None

    ts_mod = types.ModuleType("app.worker.tushare_sync_service")
    ts_mod.get_tushare_sync_service = AsyncMock()
    ak_mod = types.ModuleType("app.worker.akshare_sync_service")
    ak_mod.get_akshare_sync_service = AsyncMock()

    fin_svc = SimpleNamespace(sync_single_stock=AsyncMock(return_value={"akshare": True}))
    fin_mod = types.ModuleType("app.worker.financial_data_sync_service")
    fin_mod.get_financial_sync_service = AsyncMock(return_value=fin_svc)

    etf_svc = SimpleNamespace(
        _sync_daily_by_source=AsyncMock(return_value={"success_count": 1, "records_inserted": 10, "errors": []}),
        _sync_basic_by_source=AsyncMock(return_value={"success_count": 1, "errors": []}),
    )
    etf_mod = types.ModuleType("app.worker.etf_sync_service")
    etf_mod.get_etf_sync_service = AsyncMock(return_value=etf_svc)

    dv_mod = types.ModuleType("app.services.data_validation_service")
    dv_mod.get_data_validation_service = lambda: None

    qc_mod = types.ModuleType("app.services.quote_contract")
    qc_mod.quotes_write_enabled = lambda: False

    stubs = {
        "fastapi": fastapi_mod,
        "pydantic": pydantic_mod,
        "app": types.ModuleType("app"),
        "app.routers": types.ModuleType("app.routers"),
        "app.routers.auth_db": auth_db,
        "app.core": types.ModuleType("app.core"),
        "app.core.response": core_resp,
        "app.core.database": core_db,
        "app.worker": types.ModuleType("app.worker"),
        "app.worker.tushare_sync_service": ts_mod,
        "app.worker.akshare_sync_service": ak_mod,
        "app.worker.financial_data_sync_service": fin_mod,
        "app.worker.etf_sync_service": etf_mod,
        "app.services": types.ModuleType("app.services"),
        "app.services.data_validation_service": dv_mod,
        "app.services.quote_contract": qc_mod,
    }
    stubs["app"].__path__ = []
    stubs["app.routers"].__path__ = []
    stubs["app.core"].__path__ = []
    stubs["app.worker"].__path__ = []
    stubs["app.services"].__path__ = []

    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("_offline_stock_sync", STOCK_SYNC_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod, fin_mod, fin_svc, etf_svc


class _FakeEtfBasicColl:
    async def find_one(self, query, projection=None):
        return {"code": "159330", "name": "沪深300ETF东财", "list_date": "2024-09-20"}


class _FakeEtfDailyColl:
    async def find_one(self, query, projection=None, sort=None):
        if sort == [("trade_date", 1)]:
            return {"code": "159330", "trade_date": "2025-11-27"}
        if sort == [("trade_date", -1)] or query.get("trade_date") == "2026-11-10":
            return {"code": "159330", "trade_date": "2026-11-10"}
        if "symbol" in query:
            return None
        return {"code": "159330", "trade_date": "2026-11-10"}

    async def count_documents(self, query):
        return 230


class _FakeValidationDB:
    def __init__(self):
        self._colls = {
            "etf_basic_info": _FakeEtfBasicColl(),
            "etf_daily_quotes": _FakeEtfDailyColl(),
        }

    async def list_collection_names(self):
        return list(self._colls.keys())

    def __getitem__(self, name):
        return self._colls[name]


class ValidationAndEtfSyncTests(unittest.TestCase):
    def test_etf_validation_returns_etf_message_and_skipped_financial_flag(self):
        mod = _load_isolated_validation_module()
        svc = mod.DataValidationService()
        svc.db = _FakeValidationDB()

        result = asyncio.run(
            svc.validate_stock_data(
                symbol="159330",
                analysis_date="2026-11-10",
                market_type="etf",
                check_basic_info=True,
                check_historical_data=True,
                check_financial_data=False,
            )
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(result.message, "ETF 159330 的数据校验通过")
        self.assertEqual(result.missing_data, [])
        self.assertEqual(
            result.details.get("financial_data"),
            {"skipped": True, "reason": "ETF 不适用上市公司财务报表检查"},
        )

    def test_single_sync_skips_corporate_financials_for_etf(self):
        mod, fin_mod, fin_svc, etf_svc = _load_isolated_stock_sync_module()
        fin_mod.get_financial_sync_service.reset_mock()
        fin_svc.sync_single_stock.reset_mock()
        etf_svc._sync_daily_by_source.reset_mock()

        req = mod.SingleStockSyncRequest(
            symbol="159330",
            sync_realtime=False,
            sync_historical=True,
            sync_financial=True,
            sync_basic=False,
            data_source="akshare",
            days=365,
        )
        res = asyncio.run(mod.run_single_stock_sync(req))

        fin_mod.get_financial_sync_service.assert_not_awaited()
        fin_svc.sync_single_stock.assert_not_awaited()
        etf_svc._sync_daily_by_source.assert_awaited_once_with("akshare", ["159330"], 365, True)
        self.assertEqual(res["financial_sync"]["success"], True)
        self.assertEqual(res["financial_sync"]["skipped"], True)
        self.assertTrue(res["overall_success"])


if __name__ == "__main__":
    unittest.main()
