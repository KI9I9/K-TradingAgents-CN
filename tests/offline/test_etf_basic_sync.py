"""Offline tests for weekly lightweight ETF basic list sync (sync_basic_only & scheduling)."""
from __future__ import annotations

import asyncio
import importlib.util
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
ETF_SYNC_PATH = REPO_ROOT / "app/worker/etf_sync_service.py"
CONFIG_PATH = REPO_ROOT / "app/core/config.py"
MAIN_PATH = REPO_ROOT / "app/main.py"
SYNC_ROUTER_PATH = REPO_ROOT / "app/routers/sync.py"


def _load_isolated_etf_sync_module():
    core_cfg = types.ModuleType("app.core.config")
    core_cfg.settings = types.SimpleNamespace(ETF_SYNC_ITEM_TIMEOUT_SECONDS=60)

    core_db = types.ModuleType("app.core.database")
    core_db.get_mongo_db = lambda: None

    dsp = types.ModuleType("app.core.data_source_priority")
    dsp.get_enabled_data_sources_async = AsyncMock(return_value=["tushare", "akshare"])

    qmt_mod = types.ModuleType("app.services.data_sources.qmt_adapter")
    qmt_mod.QMTAdapter = object

    sched_mod = types.ModuleType("app.services.scheduler_service")

    class _TaskCancelledException(Exception):
        pass

    sched_mod.TaskCancelledException = _TaskCancelledException
    sched_mod.mark_job_completed = AsyncMock()

    stubs = {
        "app": types.ModuleType("app"),
        "app.core": types.ModuleType("app.core"),
        "app.core.config": core_cfg,
        "app.core.database": core_db,
        "app.core.data_source_priority": dsp,
        "app.services": types.ModuleType("app.services"),
        "app.services.data_sources": types.ModuleType("app.services.data_sources"),
        "app.services.data_sources.qmt_adapter": qmt_mod,
        "app.services.scheduler_service": sched_mod,
    }
    stubs["app"].__path__ = []
    stubs["app.core"].__path__ = []
    stubs["app.services"].__path__ = []
    stubs["app.services.data_sources"].__path__ = []

    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("_offline_etf_sync", ETF_SYNC_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod, sched_mod


class _FakeBasicCollection:
    def __init__(self):
        self.upserts = []

    async def find_one(self, *args, **kwargs):
        return None

    async def update_one(self, filter_q, update_q, upsert=False):
        self.upserts.append((filter_q, update_q["$set"], upsert))


class _FakeDB:
    def __init__(self):
        self.basic = _FakeBasicCollection()

    def __getitem__(self, name):
        return self.basic


class EtfBasicSyncTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod, cls.sched_mod = _load_isolated_etf_sync_module()

    def test_sync_basic_only_defaults_to_akshare_and_skips_daily_quotes(self):
        svc = self.mod.EtfSyncService()
        svc.db = _FakeDB()
        svc.rate_limit_delay = 0
        svc._report_job_progress = AsyncMock()
        svc._sync_basic_by_source = AsyncMock(
            return_value={
                "total_processed": 1000,
                "success_count": 998,
                "skipped_count": 2,
                "error_count": 0,
                "errors": [],
                "data_source": "akshare",
            }
        )
        svc._sync_daily_by_source = AsyncMock(side_effect=AssertionError("must not sync daily quotes"))

        with patch.dict(sys.modules, {"app.services.scheduler_service": self.sched_mod}):
            result = asyncio.run(svc.sync_basic_only(force_update=False, job_id="etf_basic_sync"))
        svc._sync_basic_by_source.assert_awaited_once_with(
            "akshare",
            False,
            job_id="etf_basic_sync",
            stage_start=5,
            stage_end=95,
        )
        svc._sync_daily_by_source.assert_not_called()
        self.assertEqual(result["data_source_used"], "akshare")
        self.assertEqual(result["total_processed"], 1000)
        self.assertEqual(result["success_count"], 998)

    def test_sync_basic_by_source_survives_valuation_timeout_and_upserts_rows(self):
        svc = self.mod.EtfSyncService()
        svc.db = _FakeDB()
        svc.rate_limit_delay = 0
        svc._report_job_progress = AsyncMock()

        sample_rows = [
            {"code": "588000", "name": "科创50ETF华夏", "full_symbol": "588000.SH", "fund_type": "ETF"}
        ]
        orig_wait_for = asyncio.wait_for

        async def _selective_wait_for(coro, timeout=None):
            if timeout == 30.0:
                if hasattr(coro, "close"):
                    coro.close()
                raise asyncio.TimeoutError()
            return await orig_wait_for(coro, timeout=timeout)

        with patch.dict(self.mod._FETCH_BASIC, {"akshare": lambda: sample_rows}), \
             patch.object(self.mod.asyncio, "wait_for", side_effect=_selective_wait_for):
            stats = asyncio.run(svc._sync_basic_by_source("akshare", force_update=False))

        self.assertEqual(stats["total_processed"], 1)
        self.assertEqual(stats["success_count"], 1)
        self.assertEqual(stats["error_count"], 0)
        self.assertEqual(len(svc.db.basic.upserts), 1)
        fq, doc, upsert = svc.db.basic.upserts[0]
        self.assertTrue(upsert)
        self.assertEqual(fq, {"code": "588000", "source": "akshare"})
        self.assertEqual(doc["name"], "科创50ETF华夏")

    def test_config_scheduler_and_router_expose_weekly_basic_sync(self):
        cfg_src = CONFIG_PATH.read_text(encoding="utf-8")
        main_src = MAIN_PATH.read_text(encoding="utf-8")
        router_src = SYNC_ROUTER_PATH.read_text(encoding="utf-8")

        self.assertIn("ETF_BASIC_SYNC_ENABLED", cfg_src)
        self.assertIn('default="0 5 * * sat"', cfg_src)
        self.assertIn("run_etf_basic_sync", main_src)
        self.assertIn('id="etf_basic_sync"', main_src)
        self.assertIn('"/etf/basic/run"', router_src)
        self.assertIn("get_current_user", router_src)


if __name__ == "__main__":
    unittest.main()
