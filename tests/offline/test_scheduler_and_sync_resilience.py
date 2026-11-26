"""Offline unit tests for scheduler state machine, sync resilience, gates, and ETF hardening.

Pure Python standard-library unittest: runnable without MongoDB, Redis, pandas,
APScheduler, or network access.
"""
from __future__ import annotations

import ast
import asyncio
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.quote_contract import CN_TZ


def _parse_file(rel_path: str) -> ast.Module:
    return ast.parse((ROOT / rel_path).read_text(encoding="utf-8"))


def _extract_method(rel_path: str, class_name: str, method_name: str):
    tree = _parse_file(rel_path)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == method_name:
                    return item
    raise AssertionError(f"missing {class_name}.{method_name} in {rel_path}")


def _extract_func(rel_path: str, func_name: str):
    tree = _parse_file(rel_path)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
            return node
    raise AssertionError(f"missing {func_name} in {rel_path}")


def _walk_names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _match_mongo_filter(doc: dict, filt: dict) -> bool:
    for key, expected in (filt or {}).items():
        if key == "$or":
            if not any(_match_mongo_filter(doc, clause) for clause in expected):
                return False
            continue
        if key == "$and":
            if not all(_match_mongo_filter(doc, clause) for clause in expected):
                return False
            continue
        actual = doc.get(key)
        if isinstance(expected, dict):
            for op, val in expected.items():
                if op == "$in":
                    if actual not in val:
                        return False
                elif op == "$nin":
                    if actual in val:
                        return False
                elif op == "$ne":
                    if actual == val:
                        return False
                elif op == "$gte":
                    if actual is None or actual < val:
                        return False
                elif op == "$gt":
                    if actual is None or actual <= val:
                        return False
                elif op == "$lte":
                    if actual is None or actual > val:
                        return False
                elif op == "$lt":
                    if actual is None or actual >= val:
                        return False
                elif op == "$exists":
                    present = key in doc
                    if bool(val) != present:
                        return False
                elif op == "$regex":
                    import re
                    if actual is None or re.search(val, str(actual)) is None:
                        return False
                else:
                    raise NotImplementedError(f"unsupported op {op}")
        else:
            if actual != expected:
                return False
    return True


class _FakeSyncCollection:
    def __init__(self, docs=None):
        self.docs = [dict(d) for d in (docs or [])]
        self._seq = len(self.docs)

    def find_one(self, filt=None, sort=None):
        matched = [d for d in self.docs if _match_mongo_filter(d, filt or {})]
        if sort:
            for field, direction in reversed(sort):
                matched.sort(key=lambda d: d.get(field) or datetime.min, reverse=(direction < 0))
        return dict(matched[0]) if matched else None

    def insert_one(self, doc):
        self._seq += 1
        stored = dict(doc)
        stored.setdefault("_id", f"id-{self._seq}")
        self.docs.append(stored)
        return types.SimpleNamespace(inserted_id=stored["_id"])

    def update_one(self, filt, update, upsert=False):
        for idx, doc in enumerate(self.docs):
            if _match_mongo_filter(doc, filt):
                if "$set" in update:
                    self.docs[idx].update(update["$set"])
                return types.SimpleNamespace(matched_count=1, modified_count=1, upserted_id=None)
        if upsert:
            self._seq += 1
            created = {"_id": f"id-{self._seq}"}
            created.update(update.get("$set", {}))
            self.docs.append(created)
            return types.SimpleNamespace(matched_count=0, modified_count=0, upserted_id=created["_id"])
        return types.SimpleNamespace(matched_count=0, modified_count=0, upserted_id=None)


class _FakeAsyncCollection:
    def __init__(self, docs=None):
        self._sync = _FakeSyncCollection(docs)

    @property
    def docs(self):
        return self._sync.docs

    async def find_one(self, filt=None, sort=None):
        return self._sync.find_one(filt, sort=sort)

    async def insert_one(self, doc):
        return self._sync.insert_one(doc)

    async def update_one(self, filt, update, upsert=False):
        return self._sync.update_one(filt, update, upsert=upsert)


def _load_scheduler_helpers(now_ref: datetime):
    """Compile the actual SchedulerService outcome/progress methods offline."""
    rel = "app/services/scheduler_service.py"
    tree = _parse_file(rel)

    wanted_funcs = {
        "get_utc8_now",
        "get_execution_activity_time",
        "build_active_running_query",
        "build_recent_execution_query",
        "update_job_progress",
    }
    wanted_methods = {"_on_job_executed", "_record_job_execution"}

    top_nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted_funcs:
            top_nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "TaskCancelledException":
            top_nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "SchedulerService":
            methods = [
                item for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in wanted_methods
            ]
            top_nodes.append(
                ast.ClassDef(
                    name="SchedulerService",
                    bases=[],
                    keywords=[],
                    body=methods,
                    decorator_list=[],
                )
            )

    mod = ast.Module(body=top_nodes, type_ignores=[])
    ast.fix_missing_locations(mod)

    noop_logger = types.SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    ns = {
        "asyncio": asyncio,
        "Any": object,
        "Dict": dict,
        "List": list,
        "Optional": object,
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
        "UTC_8": timezone(timedelta(hours=8)),
        "ACTIVE_EXECUTION_WINDOW_MINUTES": 30,
        "FALLBACK_EXECUTION_WINDOW_MINUTES": 60,
        "JobExecutionEvent": object,
        "logger": noop_logger,
        "now_tz": lambda: now_ref,
        "_scheduler_instance": None,
        "json": __import__("json"),
    }
    exec(compile(mod, rel, "exec"), ns)
    return ns


class SchedulerStateMachineTests(unittest.TestCase):
    def _make_runtime_stub(self):
        # Load real outcome() from app/services/scheduler_runtime.py without bson
        tree = _parse_file("app/services/scheduler_runtime.py")
        outcome_node = next(
            n for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "outcome"
        )
        mod = ast.Module(body=[outcome_node], type_ignores=[])
        ast.fix_missing_locations(mod)
        ns = {}
        exec(compile(mod, "scheduler_runtime.py", "exec"), ns)
        fake_mod = types.ModuleType("app.services.scheduler_runtime")
        fake_mod.outcome = ns["outcome"]
        return fake_mod

    def test_on_job_executed_maps_skipped_and_failed_via_outcome(self):
        now_cn = datetime(2026, 11, 26, 10, 0, 0, tzinfo=CN_TZ)
        ns = _load_scheduler_helpers(now_cn)
        SchedulerService = ns["SchedulerService"]

        executions = _FakeAsyncCollection()
        fake_db = types.SimpleNamespace(scheduler_executions=executions)

        svc = SchedulerService()
        svc.scheduler = types.SimpleNamespace(get_job=lambda jid: types.SimpleNamespace(name=jid, kwargs={}))
        svc._get_db = lambda: fake_db
        svc._update_redis_execution_cache = AsyncMock()

        async def _run():
            # 1) 业务跳过不得写成 success
            ev_skip = types.SimpleNamespace(
                job_id="akshare_quotes_sync",
                retval={"skipped": True, "reason": "非交易时段"},
                scheduled_run_time=now_cn - timedelta(seconds=1),
            )
            svc._on_job_executed(ev_skip)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            # 2) 业务失败不得写成 success
            ev_fail = types.SimpleNamespace(
                job_id="favorites_data_sync",
                retval={"success": False, "error": "财务接口异常"},
                scheduled_run_time=now_cn - timedelta(seconds=2),
            )
            svc._on_job_executed(ev_fail)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        with patch.dict(sys.modules, {"app.services.scheduler_runtime": self._make_runtime_stub()}):
            asyncio.run(_run())

        by_job = {d["job_id"]: d for d in executions.docs}
        self.assertEqual(by_job["akshare_quotes_sync"]["status"], "skipped")
        self.assertTrue(by_job["akshare_quotes_sync"]["skipped"])
        self.assertEqual(by_job["akshare_quotes_sync"]["progress"], 100)
        self.assertEqual(by_job["favorites_data_sync"]["status"], "failed")
        self.assertIn("财务接口异常", by_job["favorites_data_sync"]["error_message"])

    def test_on_job_executed_does_not_overwrite_worker_finalized_terminal_record(self):
        now_cn = datetime(2026, 11, 26, 18, 45, 0, tzinfo=CN_TZ)
        now_naive = now_cn.replace(tzinfo=None)
        ns = _load_scheduler_helpers(now_cn)
        SchedulerService = ns["SchedulerService"]

        executions = _FakeAsyncCollection([
            {
                "_id": "exec-partial-1",
                "job_id": "akshare_historical_sync",
                "status": "partial",
                "timestamp": now_naive - timedelta(minutes=10),
                "updated_at": now_naive - timedelta(seconds=1),
                "finished_at": now_naive - timedelta(seconds=1),
                "progress": 100,
                "error_count": 3,
            }
        ])
        fake_db = types.SimpleNamespace(scheduler_executions=executions)

        svc = SchedulerService()
        svc.scheduler = types.SimpleNamespace(get_job=lambda jid: types.SimpleNamespace(name=jid, kwargs={}))
        svc._get_db = lambda: fake_db
        svc._update_redis_execution_cache = AsyncMock()

        async def _run():
            ev = types.SimpleNamespace(
                job_id="akshare_historical_sync",
                retval={"success_count": 100, "error_count": 3},
                scheduled_run_time=now_cn - timedelta(minutes=10),
            )
            svc._on_job_executed(ev)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        with patch.dict(sys.modules, {"app.services.scheduler_runtime": self._make_runtime_stub()}):
            asyncio.run(_run())

        # 终态 partial 保持不变且没有额外插入第二条 success 记录
        self.assertEqual(len(executions.docs), 1)
        self.assertEqual(executions.docs[0]["status"], "partial")

    def test_update_job_progress_100_keeps_running_and_refreshes_heartbeat(self):
        now_cn = datetime(2026, 11, 26, 18, 40, 0, tzinfo=CN_TZ)
        now_naive = now_cn.replace(tzinfo=None)
        ns = _load_scheduler_helpers(now_cn)

        sync_executions = _FakeSyncCollection([
            {
                "_id": "run-1",
                "job_id": "akshare_historical_sync",
                "status": "running",
                "progress": 80,
                "timestamp": now_naive - timedelta(minutes=20),
                "updated_at": now_naive - timedelta(minutes=2),
            }
        ])
        fake_sync_db = types.SimpleNamespace(scheduler_executions=sync_executions)
        ns["get_mongo_db_sync"] = lambda: fake_sync_db

        fake_db_mod = types.ModuleType("app.core.database")
        fake_db_mod.get_mongo_db_sync = lambda: fake_sync_db
        fake_db_mod.get_redis_sync_client = lambda: None
        fake_redis_mod = types.ModuleType("app.core.redis_client")
        fake_redis_mod.RedisKeys = types.SimpleNamespace(
            SCHEDULER_JOB_PROGRESS="prog:{job_id}",
            SCHEDULER_JOB_STATUS="stat:{job_id}",
        )

        with patch.dict(sys.modules, {
            "app.core.database": fake_db_mod,
            "app.core.redis_client": fake_redis_mod,
        }):
            asyncio.run(ns["update_job_progress"]("akshare_historical_sync", progress=100, message="收尾中"))

        doc = sync_executions.docs[0]
        self.assertEqual(doc["progress"], 100)
        self.assertEqual(doc["status"], "running")
        self.assertEqual(doc["updated_at"], now_naive)
        self.assertNotIn("finished_at", doc)


class BaoStockAndPoolResilienceTests(unittest.TestCase):
    def test_baostock_market_quotes_blocked_when_quotes_write_disabled(self):
        rel = "app/worker/baostock_sync_service.py"
        m1 = _extract_method(rel, "BaoStockSyncService", "_update_stock_quotes")
        m2 = _extract_method(rel, "BaoStockSyncService", "_update_historical_data")
        cls_node = ast.ClassDef(name="_BaoHarness", bases=[], keywords=[], body=[m1, m2], decorator_list=[])
        mod = ast.Module(body=[cls_node], type_ignores=[])
        ast.fix_missing_locations(mod)

        ns = {
            "Any": object,
            "Dict": dict,
            "datetime": datetime,
            "logger": types.SimpleNamespace(
                debug=lambda *a, **k: None,
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
            "get_historical_data_service": AsyncMock(),
        }
        exec(compile(mod, rel, "exec"), ns)
        Harness = ns["_BaoHarness"]

        class _FakeHistDF:
            empty = False
            def __len__(self):
                return 1
            @property
            def iloc(self):
                class _Iloc:
                    def __getitem__(self, idx):
                        return {"date": "2026-11-26"}
                return _Iloc()

        market_quotes = types.SimpleNamespace(update_one=AsyncMock())
        svc = Harness()
        svc.db = types.SimpleNamespace(market_quotes=market_quotes, stock_daily_quotes=object())
        svc.historical_service = types.SimpleNamespace(save_historical_data=AsyncMock(return_value=1))

        with patch.dict(os.environ, {"QUOTES_WRITE_ENABLED": "false"}, clear=False):
            asyncio.run(svc._update_stock_quotes({"code": "600519", "close": 1500.0}))
            saved = asyncio.run(svc._update_historical_data("600519", _FakeHistDF(), period="daily"))
            self.assertEqual(saved, 1)
            market_quotes.update_one.assert_not_called()

        with patch.dict(os.environ, {"QUOTES_WRITE_ENABLED": "true"}, clear=False):
            asyncio.run(svc._update_stock_quotes({"code": "600519", "close": 1500.0}))
            market_quotes.update_one.assert_awaited_once()

    def test_baostock_and_pool_heartbeat_and_no_temp_mongo_client(self):
        bao_check = _extract_func("app/worker/baostock_sync_service.py", "_check_task_running")
        self.assertNotIn("MongoClient", _walk_names(bao_check))
        self.assertIn("get_mongo_db_sync", _walk_names(bao_check))
        self.assertIn("build_active_running_query", _walk_names(bao_check))

        pool_stop = _extract_method("app/worker/unified_thread_pool_sync_service.py", "UnifiedThreadPoolSyncService", "_should_stop")
        pool_shutdown = _extract_method("app/worker/unified_thread_pool_sync_service.py", "UnifiedThreadPoolSyncService", "shutdown")
        self.assertNotIn("MongoClient", _walk_names(pool_stop))
        self.assertIn("get_mongo_db_sync", _walk_names(pool_stop))
        self.assertNotIn("MongoClient", _walk_names(pool_shutdown))
        self.assertIn("get_mongo_db_sync", _walk_names(pool_shutdown))

        # 实测：开始超过 50 分钟但 2 分钟前仍有 updated_at 心跳的任务，绝不被误判僵尸
        now_cn = datetime(2026, 11, 26, 19, 10, 0, tzinfo=CN_TZ)
        now_naive = now_cn.replace(tzinfo=None)
        sched_ns = _load_scheduler_helpers(now_cn)

        sync_executions = _FakeSyncCollection([
            {
                "_id": "long-job-1",
                "job_id": "baostock_historical_sync",
                "status": "running",
                "timestamp": now_naive - timedelta(minutes=50),
                "updated_at": now_naive - timedelta(minutes=2),
            }
        ])
        fake_sync_db = types.SimpleNamespace(scheduler_executions=sync_executions)

        fake_db_mod = types.ModuleType("app.core.database")
        fake_db_mod.get_mongo_db_sync = lambda: fake_sync_db
        fake_sched_mod = types.ModuleType("app.services.scheduler_service")
        fake_sched_mod.ACTIVE_EXECUTION_WINDOW_MINUTES = 30
        fake_sched_mod.build_active_running_query = sched_ns["build_active_running_query"]
        fake_sched_mod.get_utc8_now = sched_ns["get_utc8_now"]

        mod = ast.Module(body=[bao_check], type_ignores=[])
        ast.fix_missing_locations(mod)
        fn_ns = {
            "Tuple": tuple,
            "Optional": object,
            "timedelta": timedelta,
            "logger": types.SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None),
        }
        exec(compile(mod, "baostock_sync_service.py", "exec"), fn_ns)

        with patch.dict(sys.modules, {
            "app.core.database": fake_db_mod,
            "app.services.scheduler_service": fake_sched_mod,
        }):
            is_running, inst_id = asyncio.run(fn_ns["_check_task_running"]("baostock_historical_sync"))
            self.assertTrue(is_running)
            self.assertEqual(inst_id, "long-job-1")
            self.assertEqual(sync_executions.docs[0]["status"], "running")

            # 心跳陈旧（超过 45 分钟无 updated_at）则标失败并返回 False
            sync_executions.docs[0]["updated_at"] = now_naive - timedelta(minutes=45)
            is_running2, inst_id2 = asyncio.run(fn_ns["_check_task_running"]("baostock_historical_sync"))
            self.assertFalse(is_running2)
            self.assertIsNone(inst_id2)
            self.assertEqual(sync_executions.docs[0]["status"], "failed")


class FavoritesAndIndustryGateTests(unittest.TestCase):
    def test_favorites_filters_disabled_tushare_and_reports_subtask_failure(self):
        rel = "app/worker/favorites_sync_task.py"
        f_allow = _extract_func(rel, "_is_tushare_sync_allowed")
        f_sources = _extract_func(rel, "_get_sync_data_sources")
        f_run = _extract_func(rel, "run_favorites_data_sync")

        mod = ast.Module(body=[f_allow, f_sources, f_run], type_ignores=[])
        ast.fix_missing_locations(mod)
        ns = {
            "os": os,
            "Path": Path,
            "List": list,
            "datetime": datetime,
            "logger": types.SimpleNamespace(
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
            "get_all_favorite_a_stocks": AsyncMock(return_value=["600519"]),
            "sync_favorites_historical_data": AsyncMock(return_value={"success": True, "synced_count": 1, "total_count": 1}),
            "sync_favorites_financial_data": AsyncMock(return_value={"success": False, "error": "财务接口异常", "synced_count": 0, "total_count": 1}),
        }
        exec(compile(mod, rel, "exec"), ns)

        with patch.dict(os.environ, {"ENABLE_TUSHARE": "false"}, clear=False):
            self.assertEqual(ns["_get_sync_data_sources"](["tushare", "akshare"]), ["akshare"])
            self.assertEqual(ns["_get_sync_data_sources"](["local"]), ["akshare", "baostock"])

        with patch.dict(os.environ, {"ENABLE_TUSHARE": "true", "TA_TUSHARE_ROUTE": "disabled"}, clear=False):
            self.assertFalse(ns["_is_tushare_sync_allowed"]())

        fake_sched_mod = types.ModuleType("app.services.scheduler_service")
        fake_sched_mod.update_job_progress = AsyncMock()
        with patch.dict(sys.modules, {"app.services.scheduler_service": fake_sched_mod}):
            result = asyncio.run(ns["run_favorites_data_sync"]())
        self.assertFalse(result["success"])
        self.assertIn("财务接口异常", result["error"])

    def test_official_industry_respects_tushare_gate_and_timeout(self):
        rel = "app/services/official_industry_service.py"
        f_allow = _extract_func(rel, "_is_tushare_industry_fallback_allowed")
        f_fetch = _extract_func(rel, "_fetch_mapping_from_tushare")
        f_norm_code = _extract_func(rel, "_normalize_code")
        f_norm_payload = _extract_func(rel, "_normalize_mapping_payload")

        mod = ast.Module(body=[f_norm_code, f_norm_payload, f_allow, f_fetch], type_ignores=[])
        ast.fix_missing_locations(mod)
        ns = {
            "asyncio": asyncio,
            "os": os,
            "Path": Path,
            "Any": object,
            "Dict": dict,
            "Tuple": tuple,
            "TUSHARE_INDUSTRY_FETCH_TIMEOUT_SECONDS": 20.0,
            "logger": types.SimpleNamespace(
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
            ),
        }
        exec(compile(mod, rel, "exec"), ns)

        # 1) 禁用时直接跳过，绝不实例化 TushareProvider
        provider_instantiated = False

        class _ExplodingProvider:
            def __init__(self):
                nonlocal provider_instantiated
                provider_instantiated = True
                raise AssertionError("should not instantiate TushareProvider when gated")

        fake_ts_mod = types.ModuleType("tradingagents.dataflows.providers.china.tushare")
        fake_ts_mod.TushareProvider = _ExplodingProvider
        with patch.dict(os.environ, {"ENABLE_TUSHARE": "false"}, clear=False), \
             patch.dict(sys.modules, {"tradingagents.dataflows.providers.china.tushare": fake_ts_mod}):
            res = asyncio.run(ns["_fetch_mapping_from_tushare"]())
        self.assertEqual(res, {})
        self.assertFalse(provider_instantiated)

        # 2) 允许调用但 connect 超时时，在 timeout_seconds 内干净返回 {}
        class _SlowProvider:
            async def connect(self):
                await asyncio.sleep(0.5)
                return True

        fake_ts_mod.TushareProvider = _SlowProvider
        ns["_is_tushare_industry_fallback_allowed"] = lambda: (True, "ok")
        with patch.dict(sys.modules, {"tradingagents.dataflows.providers.china.tushare": fake_ts_mod}):
            t0 = time.monotonic()
            res_timeout = asyncio.run(ns["_fetch_mapping_from_tushare"](timeout_seconds=0.05))
            elapsed = time.monotonic() - t0
        self.assertEqual(res_timeout, {})
        self.assertLess(elapsed, 0.4)


class EtfAndMainScheduleTests(unittest.TestCase):
    def test_etf_sync_basic_enforces_hard_timeout_and_is_fresh_handles_timezones(self):
        rel = "app/worker/etf_sync_service.py"
        m_basic = _extract_method(rel, "EtfSyncService", "_sync_basic_by_source")
        m_fresh = _extract_method(rel, "EtfSyncService", "_is_fresh")
        cls_node = ast.ClassDef(name="_EtfHarness", bases=[], keywords=[], body=[m_basic, m_fresh], decorator_list=[])
        mod = ast.Module(body=[cls_node], type_ignores=[])
        ast.fix_missing_locations(mod)

        import threading
        release_evt = threading.Event()

        def _hanging_fetch():
            release_evt.wait(timeout=2.0)
            return [{"code": "510300"}]

        ns = {
            "asyncio": asyncio,
            "Any": object,
            "Dict": dict,
            "List": list,
            "Optional": object,
            "datetime": datetime,
            "partial": __import__("functools").partial,
            "ETF_BASIC_INFO_COLLECTION": "etf_basic_info",
            "_FETCH_BASIC": {"akshare": _hanging_fetch},
            "_normalize_etf_code": lambda s: str(s).zfill(6),
            "logger": types.SimpleNamespace(
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                debug=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
        }
        exec(compile(mod, rel, "exec"), ns)
        Harness = ns["_EtfHarness"]
        svc = Harness()
        svc.db = object()
        svc.basic_fetch_timeout_seconds = 0.05
        svc._report_job_progress = AsyncMock()

        async def _run_timeout_check():
            start = time.monotonic()
            res = await svc._sync_basic_by_source("akshare", force_update=True)
            dur = time.monotonic() - start
            release_evt.set()
            return res, dur

        stats, elapsed = asyncio.run(_run_timeout_check())
        self.assertLess(elapsed, 0.35)
        self.assertEqual(stats["error_count"], 1)
        self.assertIn("超时", stats["errors"][0]["error"])

        # _is_fresh 时区与未来时间戳边界测试
        now_cn = datetime(2026, 11, 26, 10, 0, 0, tzinfo=CN_TZ)
        # BSON naive UTC: 01:30 UTC == 09:30+08:00 (30m ago) -> True
        self.assertTrue(svc._is_fresh(datetime(2026, 11, 26, 1, 30, 0), hours=24, now=now_cn))
        # BSON naive UTC: 25h ago -> False
        self.assertFalse(svc._is_fresh(datetime(2026, 11, 25, 0, 30, 0), hours=24, now=now_cn))
        # Aware +08:00 ISO string (2h ago) -> True
        self.assertTrue(svc._is_fresh("2026-11-26T08:00:00+08:00", hours=24, now=now_cn))
        # Future timestamp (+10m in future) -> False
        self.assertFalse(svc._is_fresh(datetime(2026, 11, 26, 2, 10, 0, tzinfo=timezone.utc), hours=24, now=now_cn))
        # Invalid / None -> False
        self.assertFalse(svc._is_fresh(None, hours=24, now=now_cn))
        self.assertFalse(svc._is_fresh("bad-ts", hours=24, now=now_cn))

    def test_main_uses_multi_source_singleton_and_registers_premarket_calendar(self):
        tree = _parse_file("app/main.py")
        src = (ROOT / "app/main.py").read_text(encoding="utf-8")

        # 确认不再直接实例化 MultiSourceBasicsSyncService()，改调单例 get_multi_source_sync_service()
        direct_inits = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "MultiSourceBasicsSyncService"
        ]
        self.assertEqual(direct_inits, [])
        self.assertIn("multi_source_service = get_multi_source_sync_service()", src)

        # 确认盘前 09:10 CST 工作日交易日历刷新任务已注册
        add_job_calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_job"
        ]
        pre_market_found = False
        for call in add_job_calls:
            id_kw = next((kw for kw in call.keywords if kw.arg == "id"), None)
            if id_kw and isinstance(id_kw.value, ast.Constant) and id_kw.value.value == "trading_calendar_pre_market_refresh":
                trigger = call.args[1]
                self.assertIsInstance(trigger, ast.Call)
                kw_map = {kw.arg: kw.value.value for kw in trigger.keywords if isinstance(kw.value, ast.Constant)}
                self.assertEqual(kw_map.get("day_of_week"), "mon-fri")
                self.assertEqual(kw_map.get("hour"), 9)
                self.assertEqual(kw_map.get("minute"), 10)
                pre_market_found = True
        self.assertTrue(pre_market_found, "missing trading_calendar_pre_market_refresh 09:10 job in app/main.py")


if __name__ == "__main__":
    unittest.main()
