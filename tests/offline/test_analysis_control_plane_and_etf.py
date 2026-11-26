"""Offline unittest suite for REQ-2: unified ETF prefix/symbol normalization,
analysis pipeline (precheck/single/batch), worker crash/zombie prevention,
and unified v2 control plane (cancel/mark-failed/delete/feedback/retry/history/zombies).

Uses only Python stdlib so it runs cleanly in minimal offline environments
without FastAPI, Pydantic, Motor, PyMongo, Redis, or Pandas installed.
"""

import ast
import asyncio
import copy
import re
import sys
import time
import types
import unittest
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Minimal stdlib-only Fake MongoDB / Fake Redis / Fake MemoryStateManager
# ---------------------------------------------------------------------------

def _get_nested(doc: Dict[str, Any], dotted_key: str):
    cur: Any = doc
    for part in dotted_key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _MISSING
    return cur


class _MissingSentinel:
    pass


_MISSING = _MissingSentinel()


def _normalize_dt_for_cmp(val: Any):
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    return val


def _match_condition(doc: Dict[str, Any], query: Optional[Dict[str, Any]]) -> bool:
    if not query:
        return True
    for key, expected in query.items():
        if key == "$and":
            if not all(_match_condition(doc, sub) for sub in expected):
                return False
        elif key == "$or":
            if not any(_match_condition(doc, sub) for sub in expected):
                return False
        else:
            actual = _get_nested(doc, key)
            if isinstance(expected, dict):
                for op, operand in expected.items():
                    if op == "$eq":
                        if actual is _MISSING or actual != operand:
                            return False
                    elif op == "$in":
                        if actual is _MISSING or actual not in operand:
                            return False
                    elif op == "$nin":
                        if actual is not _MISSING and actual in operand:
                            return False
                    elif op == "$exists":
                        exists = actual is not _MISSING
                        if exists != bool(operand):
                            return False
                    elif op == "$lt":
                        if actual is _MISSING or actual is None:
                            return False
                        if not (_normalize_dt_for_cmp(actual) < _normalize_dt_for_cmp(operand)):
                            return False
                    elif op == "$lte":
                        if actual is _MISSING or actual is None:
                            return False
                        if not (_normalize_dt_for_cmp(actual) <= _normalize_dt_for_cmp(operand)):
                            return False
                    elif op == "$gte":
                        if actual is _MISSING or actual is None:
                            return False
                        if not (_normalize_dt_for_cmp(actual) >= _normalize_dt_for_cmp(operand)):
                            return False
                    elif op == "$regex":
                        if actual is _MISSING or actual is None:
                            return False
                        flags = re.IGNORECASE if "i" in str(expected.get("$options", "")) else 0
                        if re.search(operand, str(actual), flags) is None:
                            return False
                    elif op == "$options":
                        continue
                    else:
                        raise NotImplementedError(f"Unsupported operator: {op}")
            else:
                if expected is None:
                    if actual is not _MISSING and actual is not None:
                        return False
                elif actual is _MISSING or actual != expected:
                    return False
    return True


def _apply_projection(doc: Dict[str, Any], projection: Optional[Dict[str, int]]) -> Dict[str, Any]:
    copied = copy.deepcopy(doc)
    if not projection:
        return copied
    include_fields = {k for k, v in projection.items() if v and k != "_id"}
    exclude_id = projection.get("_id") == 0
    if include_fields:
        out = {k: v for k, v in copied.items() if k in include_fields}
        if not exclude_id and "_id" in copied:
            out["_id"] = copied["_id"]
        return out
    if exclude_id:
        copied.pop("_id", None)
    return copied


class _WriteResult:
    def __init__(self, matched_count: int = 0, modified_count: int = 0, deleted_count: int = 0, upserted_id: Any = None):
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count
        self.upserted_id = upserted_id


class FakeCursor:
    def __init__(self, docs: List[Dict[str, Any]]):
        self._docs = list(docs)
        self._limit_val: Optional[int] = None
        self._skip_val: int = 0

    def sort(self, field_name: str, direction: int = 1):
        reverse = direction < 0

        def _key(d: Dict[str, Any]):
            val = _get_nested(d, field_name)
            if val is _MISSING or val is None:
                return (0, "")
            val = _normalize_dt_for_cmp(val)
            if isinstance(val, datetime):
                return (1, val.isoformat())
            return (1, str(val))

        self._docs.sort(key=_key, reverse=reverse)
        return self

    def skip(self, n: int):
        self._skip_val = max(0, int(n))
        return self

    def limit(self, n: int):
        self._limit_val = max(0, int(n))
        return self

    def _sliced(self) -> List[Dict[str, Any]]:
        res = self._docs[self._skip_val:]
        if self._limit_val is not None:
            res = res[:self._limit_val]
        return res

    async def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        res = self._sliced()
        if length is not None:
            res = res[:length]
        return res

    def __aiter__(self):
        self._iter = iter(self._sliced())
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeCollection:
    def __init__(self, docs: Optional[List[Dict[str, Any]]] = None):
        self.docs: List[Dict[str, Any]] = [copy.deepcopy(d) for d in (docs or [])]
        self.find_queries: List[Dict[str, Any]] = []

    async def count_documents(self, query: Optional[Dict[str, Any]] = None) -> int:
        return sum(1 for d in self.docs if _match_condition(d, query))

    async def find_one(self, query: Optional[Dict[str, Any]] = None, projection: Optional[Dict[str, int]] = None):
        for d in self.docs:
            if _match_condition(d, query):
                return _apply_projection(d, projection)
        return None

    def find(self, query: Optional[Dict[str, Any]] = None, projection: Optional[Dict[str, int]] = None) -> FakeCursor:
        self.find_queries.append(copy.deepcopy(query or {}))
        matched = [_apply_projection(d, projection) for d in self.docs if _match_condition(d, query)]
        return FakeCursor(matched)

    async def insert_one(self, doc: Dict[str, Any]):
        self.docs.append(copy.deepcopy(doc))
        return _WriteResult(matched_count=1, modified_count=1)

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any], upsert: bool = False):
        for d in self.docs:
            if _match_condition(d, query):
                if "$set" in update:
                    d.update(copy.deepcopy(update["$set"]))
                return _WriteResult(matched_count=1, modified_count=1)
        if upsert:
            new_doc = {}
            for k, v in query.items():
                if not k.startswith("$"):
                    new_doc[k] = v
            if "$set" in update:
                new_doc.update(copy.deepcopy(update["$set"]))
            self.docs.append(new_doc)
            return _WriteResult(upserted_id="upserted")
        return _WriteResult()

    async def update_many(self, query: Dict[str, Any], update: Dict[str, Any]):
        matched = 0
        for d in self.docs:
            if _match_condition(d, query):
                matched += 1
                if "$set" in update:
                    d.update(copy.deepcopy(update["$set"]))
        return _WriteResult(matched_count=matched, modified_count=matched)

    async def replace_one(self, query: Dict[str, Any], replacement: Dict[str, Any], upsert: bool = False):
        for i, d in enumerate(self.docs):
            if _match_condition(d, query):
                self.docs[i] = copy.deepcopy(replacement)
                return _WriteResult(matched_count=1, modified_count=1)
        if upsert:
            self.docs.append(copy.deepcopy(replacement))
            return _WriteResult(upserted_id="upserted")
        return _WriteResult()

    async def delete_one(self, query: Dict[str, Any]):
        for i, d in enumerate(self.docs):
            if _match_condition(d, query):
                self.docs.pop(i)
                return _WriteResult(deleted_count=1)
        return _WriteResult(deleted_count=0)


class FakeDB:
    def __init__(self, initial: Optional[Dict[str, List[Dict[str, Any]]]] = None):
        self._collections: Dict[str, FakeCollection] = {}
        for name, docs in (initial or {}).items():
            self._collections[name] = FakeCollection(docs)

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self._collections:
            self._collections[name] = FakeCollection()
        return self._collections[name]

    def __getattr__(self, name: str) -> FakeCollection:
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]


class FakeRedis:
    def __init__(self):
        self.hashes: Dict[str, Dict[str, str]] = {}
        self.deleted_keys: List[str] = []

    async def hset(self, key: str, mapping: Optional[Dict[str, Any]] = None, **kwargs):
        bucket = self.hashes.setdefault(key, {})
        for k, v in (mapping or {}).items():
            bucket[str(k)] = str(v)
        for k, v in kwargs.items():
            bucket[str(k)] = str(v)
        return len(bucket)

    async def hgetall(self, key: str) -> Dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            self.deleted_keys.append(key)
            if key in self.hashes:
                del self.hashes[key]
                removed += 1
        return removed


class FakeHTTPException(Exception):
    def __init__(self, status_code: int, detail: Any = ""):
        super().__init__(f"{status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


class FakeTaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FakeAnalysisStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"


class FakeAnalysisTaskType(str, Enum):
    STOCK_ANALYSIS = "stock_analysis"
    ETF_ANALYSIS = "etf_analysis"


class FakeAnalysisEngine(str, Enum):
    LEGACY = "legacy"
    UNIFIED = "unified"
    V2 = "v2"


class FakeObjectId(str):
    def __new__(cls, value: Optional[str] = None):
        if value is None:
            return super().__new__(cls, uuid.uuid4().hex[:24])
        val = str(value)
        if len(val) != 24 or any(c not in "0123456789abcdefABCDEF" for c in val):
            raise ValueError(f"Invalid ObjectId: {val}")
        return super().__new__(cls, val)


class FakeMemoryManager:
    def __init__(self):
        self.tasks: Dict[str, types.SimpleNamespace] = {}

    async def create_task(self, task_id: str, user_id: str, stock_code: str, parameters=None, stock_name=None):
        st = types.SimpleNamespace(
            task_id=task_id,
            user_id=user_id,
            stock_code=stock_code,
            status=FakeTaskStatus.PENDING,
            progress=0,
            message="",
            error_message=None,
            parameters=parameters or {},
        )
        self.tasks[task_id] = st
        return st

    async def get_task(self, task_id: str):
        return self.tasks.get(task_id)

    async def update_task_status(
        self,
        task_id: str,
        status: FakeTaskStatus,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        current_step: Optional[str] = None,
        current_step_name: Optional[str] = None,
        current_step_description: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        t = self.tasks.get(task_id)
        if not t:
            return False
        t.status = status
        if progress is not None:
            t.progress = progress
        if message is not None:
            t.message = message
        if error_message is not None:
            t.error_message = error_message
        return True

    async def remove_task(self, task_id: str) -> bool:
        return self.tasks.pop(task_id, None) is not None

    async def cleanup_zombie_tasks(self, max_running_hours: int = 2) -> int:
        return 1


class _SilentLogger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


class _TempModules:
    """Context manager to inject stub modules into sys.modules and cleanly restore afterwards."""

    def __init__(self, modules: Dict[str, Any]):
        self._modules = modules
        self._originals: Dict[str, Any] = {}

    def __enter__(self):
        for name, mod in self._modules.items():
            self._originals[name] = sys.modules.get(name, _MISSING)
            sys.modules[name] = mod
        return self

    def __exit__(self, exc_type, exc, tb):
        for name, orig in self._originals.items():
            if orig is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = orig


def _load_stock_utils_namespace() -> Dict[str, Any]:
    """Load real tradingagents/utils/stock_utils.py with stdlib-only logger stub."""
    path = ROOT / "tradingagents/utils/stock_utils.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    body = [
        node for node in tree.body
        if not (
            isinstance(node, ast.ImportFrom)
            and node.module == "tradingagents.utils.logging_init"
        )
    ]
    mod = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(mod)
    ns: Dict[str, Any] = {"get_logger": lambda *_a, **_k: _SilentLogger()}
    exec(compile(mod, str(path), "exec"), ns)
    return ns


def _extract_functions(path: Path, names: List[str], namespace: Dict[str, Any]) -> Dict[str, Any]:
    """Extract top-level functions (stripping FastAPI router decorators) from real source file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    wanted = []
    name_set = set(names)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in name_set:
            cloned = copy.deepcopy(node)
            cloned.decorator_list = []
            wanted.append(cloned)
    missing = name_set - {n.name for n in wanted}
    if missing:
        raise AssertionError(f"{path} missing functions: {sorted(missing)}")
    mod = ast.Module(body=wanted, type_ignores=[])
    ast.fix_missing_locations(mod)
    exec(compile(mod, str(path), "exec"), namespace)
    return namespace


def _extract_class_methods(path: Path, class_name: str, method_names: List[str], namespace: Dict[str, Any]):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls_node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    cloned = copy.deepcopy(cls_node)
    method_set = set(method_names)
    cloned.body = [
        n for n in cloned.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in method_set
    ]
    missing = method_set - {n.name for n in cloned.body}
    if missing:
        raise AssertionError(f"{path}:{class_name} missing methods: {sorted(missing)}")
    mod = ast.Module(body=[cloned], type_ignores=[])
    ast.fix_missing_locations(mod)
    exec(compile(mod, str(path), "exec"), namespace)
    return namespace[class_name]


# ---------------------------------------------------------------------------
# 1. StockUtils unified ETF prefix & symbol normalization tests
# ---------------------------------------------------------------------------

class StockUtilsContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.su = _load_stock_utils_namespace()

    def test_etf_prefixes_and_security_type_recognition(self):
        self.assertEqual(
            self.su["ETF_PREFIXES"],
            ("50", "51", "52", "56", "58", "15", "16", "18"),
        )
        StockUtils = self.su["StockUtils"]
        SecurityType = self.su["SecurityType"]
        for etf_code in ("500001", "510300", "520001", "560010", "588000", "588000.SH", "159915", "160119", "180101", "180101.SZ"):
            self.assertEqual(StockUtils.identify_security_type(etf_code), SecurityType.FUND, etf_code)
            self.assertTrue(self.su["is_etf_symbol"](etf_code), etf_code)

        for stock_code in ("600519", "600519.SH", "000001.SZ", "300750", "688981", "920001.BJ", "830799.BJ"):
            self.assertEqual(StockUtils.identify_security_type(stock_code), SecurityType.STOCK, stock_code)
            self.assertFalse(self.su["is_etf_symbol"](stock_code), stock_code)

    def test_market_identification_and_symbol_normalization(self):
        StockUtils = self.su["StockUtils"]
        StockMarket = self.su["StockMarket"]
        normalize_symbol = self.su["normalize_symbol"]

        # Strip CN/BJ suffixes while keeping US dot/hyphen & .HK
        self.assertEqual(normalize_symbol("600519.SH"), "600519")
        self.assertEqual(normalize_symbol("000001.sz"), "000001")
        self.assertEqual(normalize_symbol("920001.BJ"), "920001")
        self.assertEqual(normalize_symbol("600519.SS"), "600519")
        self.assertEqual(normalize_symbol("600519.XSHG"), "600519")
        self.assertEqual(normalize_symbol("000001.XSHE"), "000001")
        self.assertEqual(normalize_symbol("BRK.B"), "BRK.B")
        self.assertEqual(normalize_symbol("BF-B"), "BF-B")
        self.assertEqual(normalize_symbol("00700.HK"), "00700.HK")

        # Market detection covers .BJ and dotted/hyphenated US tickers
        self.assertEqual(StockUtils.identify_stock_market("920001.BJ"), StockMarket.CHINA_A)
        self.assertEqual(StockUtils.identify_stock_market("830001.BJ"), StockMarket.CHINA_A)
        self.assertEqual(StockUtils.identify_stock_market("588000.SH"), StockMarket.CHINA_A)
        self.assertEqual(StockUtils.identify_stock_market("00700.HK"), StockMarket.HONG_KONG)
        self.assertEqual(StockUtils.identify_stock_market("BRK.B"), StockMarket.US)
        self.assertEqual(StockUtils.identify_stock_market("BF-B"), StockMarket.US)


# ---------------------------------------------------------------------------
# 2. analysis.py precheck / single / batch / unified control plane tests
# ---------------------------------------------------------------------------

class AnalysisRouterAndControlPlaneTests(unittest.TestCase):
    def setUp(self):
        self.su = _load_stock_utils_namespace()
        self.user_a_id = "507f1f77bcf86cd799439012"
        self.user_b_id = "507f1f77bcf86cd799439013"
        self.user_a = {"id": self.user_a_id, "username": "alice"}
        self.user_b = {"id": self.user_b_id, "username": "bob"}
        self.memory_manager = FakeMemoryManager()
        self.redis = FakeRedis()

    def _build_analysis_env(self, db: FakeDB, config_obj: Any = None, enqueue_side_effect=None):
        created_tasks: List[Dict[str, Any]] = []
        enqueued_calls: List[Dict[str, Any]] = []
        cancelled_redis_ids: List[str] = []

        class _FakeTaskService:
            async def create_task(self, user_id, task_type, task_params, engine_type="auto", preference_type="neutral", batch_id=None):
                tid = f"task-{len(created_tasks) + 1}"
                created_at = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)
                doc = {
                    "task_id": tid,
                    "user_id": user_id,
                    "task_type": task_type.value if hasattr(task_type, "value") else str(task_type),
                    "task_params": copy.deepcopy(task_params),
                    "batch_id": batch_id,
                    "status": "pending",
                    "progress": 0,
                    "created_at": created_at,
                }
                created_tasks.append(doc)
                await db.unified_analysis_tasks.insert_one(doc)
                return types.SimpleNamespace(
                    task_id=tid,
                    status=FakeAnalysisStatus.PENDING,
                    created_at=created_at,
                    task_type=task_type,
                    task_params=task_params,
                )

        class _FakeQueueService:
            def __init__(self, r: FakeRedis):
                self.r = r
                self.tasks: Dict[str, Dict[str, Any]] = {}
                self.batch_tasks: Dict[str, List[str]] = {}

            async def enqueue_task(self, user_id, symbol, params, batch_id=None, task_id=None):
                if enqueue_side_effect is not None:
                    res = enqueue_side_effect(symbol, task_id, params)
                    if asyncio.iscoroutine(res):
                        await res
                enqueued_calls.append({"user_id": user_id, "symbol": symbol, "params": copy.deepcopy(params), "batch_id": batch_id, "task_id": task_id})
                self.tasks[task_id] = {"id": task_id, "user": user_id, "symbol": symbol, "status": "queued", "parameters": params}
                if batch_id:
                    self.batch_tasks.setdefault(batch_id, []).append(task_id)
                return task_id

            async def get_task(self, task_id: str):
                return self.tasks.get(task_id)

            async def get_batch(self, batch_id: str):
                data = await self.r.hgetall("qa:batch:" + batch_id)
                if not data:
                    return None
                res = dict(data)
                res["tasks"] = list(self.batch_tasks.get(batch_id, []))
                return res

            async def cancel_task(self, task_id: str) -> bool:
                cancelled_redis_ids.append(task_id)
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "cancelled"
                    return True
                return False

        queue_svc = _FakeQueueService(self.redis)
        task_svc = _FakeTaskService()

        class _FakeParams:
            def __init__(self, **kw):
                self.market_type = kw.get("market_type", "A股")
                self.research_depth = kw.get("research_depth", "标准")
                self.selected_analysts = kw.get("selected_analysts", ["market", "fundamentals"])
                self.engine = kw.get("engine", FakeAnalysisEngine.V2)
                self._extra = kw

            def model_dump(self):
                return {
                    "market_type": self.market_type,
                    "research_depth": self.research_depth,
                    "selected_analysts": list(self.selected_analysts),
                    "engine": self.engine,
                }

        class _FakeSingleReq:
            def __init__(self, symbol=None, stock_code=None, parameters=None):
                self.symbol = symbol
                self.stock_code = stock_code
                self.parameters = parameters

            def get_symbol(self):
                return self.symbol or self.stock_code or ""

        class _FakeBatchReq:
            def __init__(self, symbols, title="批量测试", description=None, parameters=None):
                self.symbols = symbols
                self.title = title
                self.description = description
                self.parameters = parameters

            def get_symbols(self):
                return self.symbols

        stub_config_service = types.SimpleNamespace(
            get_system_config=AsyncMock(return_value=config_obj)
        )
        stub_api_key_utils = types.ModuleType("app.utils.api_key_utils")
        stub_api_key_utils.is_valid_api_key = lambda k: bool(k and len(str(k).strip()) >= 5 and not str(k).startswith("your_"))
        stub_api_key_utils.get_env_api_key_for_provider = lambda _p: None

        stub_jdyun = types.ModuleType("app.core.jdyun")
        stub_jdyun.is_jdyun_mode = lambda: False
        stub_jdyun.get_jdyun_api_key = lambda: None

        stub_task_mod = types.ModuleType("app.services.task_analysis_service")
        stub_task_mod.get_task_analysis_service = lambda: task_svc

        stub_mem_mod = types.ModuleType("app.services.memory_state_manager")
        stub_mem_mod.get_memory_state_manager = lambda: self.memory_manager
        stub_mem_mod.TaskStatus = FakeTaskStatus

        stub_queue_mod = types.ModuleType("app.services.queue_service")
        stub_queue_mod.get_queue_service = lambda: queue_svc

        stub_keys_mod = types.ModuleType("app.services.queue.keys")
        stub_keys_mod.BATCH_PREFIX = "qa:batch:"
        stub_keys_mod.TASK_PREFIX = "qa:task:"

        stub_models_analysis = types.ModuleType("app.models.analysis")
        stub_models_analysis.AnalysisTaskType = FakeAnalysisTaskType
        stub_models_analysis.AnalysisStatus = FakeAnalysisStatus

        stub_models_user = types.ModuleType("app.models.user")
        stub_models_user.PyObjectId = FakeObjectId

        stub_config_mod = types.ModuleType("app.services.config_service")
        stub_config_mod.config_service = stub_config_service

        stub_db_mod = types.ModuleType("app.core.database")
        stub_db_mod.get_mongo_db = lambda: db

        stub_simple_svc = types.SimpleNamespace(memory_manager=self.memory_manager)

        ns: Dict[str, Any] = {
            "Dict": Dict, "List": List, "Optional": Optional, "Any": Any,
            "datetime": datetime, "time": time, "uuid": uuid, "asyncio": asyncio,
            "HTTPException": FakeHTTPException,
            "Query": lambda default=None, **_k: default,
            "Depends": lambda fn=None: fn,
            "BackgroundTasks": object,
            "get_current_user": lambda: None,
            "QueueService": _FakeQueueService,
            "get_queue_service": lambda: queue_svc,
            "get_simple_analysis_service": lambda: stub_simple_svc,
            "get_unified_analysis_service": lambda: None,
            "get_mongo_db": lambda: db,
            "now_tz": lambda: datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc),
            "ObjectId": FakeObjectId,
            "StockUtils": self.su["StockUtils"],
            "StockMarket": self.su["StockMarket"],
            "SecurityType": self.su["SecurityType"],
            "AnalysisEngine": FakeAnalysisEngine,
            "AnalysisTaskType": FakeAnalysisTaskType,
            "SingleAnalysisRequest": _FakeSingleReq,
            "BatchAnalysisRequest": _FakeBatchReq,
            "AnalysisParameters": _FakeParams,
            "AnalysisFeedbackRequest": types.SimpleNamespace,
            "format_http_error_detail": lambda e: str(e),
            "_CACHE_SOURCE_TYPES": {"local", "mongodb", "local_file"},
            "logger": _SilentLogger(),
        }

        _extract_functions(
            ROOT / "app/routers/analysis.py",
            [
                "get_market_type_from_symbol",
                "_build_user_id_candidates",
                "_to_py_object_id",
                "_is_admin_user",
                "_is_task_owner",
                "_resolve_symbol_and_etf_routing",
                "_check_llm_availability",
                "_extract_doc_symbol",
                "_format_history_datetime",
                "_normalize_history_doc",
                "get_user_risk_preference",
                "submit_single_analysis",
                "precheck_analysis",
                "submit_analysis_feedback",
                "submit_batch_analysis",
                "get_batch",
                "cancel_task",
                "get_user_analysis_history",
                "mark_task_as_failed",
                "retry_task",
                "delete_task",
            ],
            ns,
        )

        modules = {
            "app.services.config_service": stub_config_mod,
            "app.utils.api_key_utils": stub_api_key_utils,
            "app.core.jdyun": stub_jdyun,
            "app.services.task_analysis_service": stub_task_mod,
            "app.services.memory_state_manager": stub_mem_mod,
            "app.services.queue_service": stub_queue_mod,
            "app.services.queue.keys": stub_keys_mod,
            "app.models.analysis": stub_models_analysis,
            "app.models.user": stub_models_user,
            "app.core.database": stub_db_mod,
        }
        return ns, modules, queue_svc, created_tasks, enqueued_calls, cancelled_redis_ids

    def test_precheck_etf_56_58_18_hk_us_and_llm_availability(self):
        db = FakeDB({
            "stock_basic_info": [
                {"code": "600519", "symbol": "600519", "name": "贵州茅台"},
                {"code": "920001", "symbol": "920001", "name": "北交测试"},
            ],
            "etf_basic_info": [
                {"code": "588000", "name": "科创50ETF"},
                {"code": "560010", "name": "中证1000ETF"},
                {"code": "180101", "name": "蛇口产园REIT"},
            ],
            "llm_providers": [{"name": "deepseek", "is_active": True, "api_key": "sk-test-valid-12345"}],
        })
        ds = types.SimpleNamespace(enabled=True, type="akshare")

        # 1) Missing LLM -> blocked with reason="no_llm"
        cfg_no_llm = types.SimpleNamespace(data_source_configs=[ds], llm_configs=[])
        ns, mods, *_ = self._build_analysis_env(db, cfg_no_llm)
        with _TempModules(mods):
            res = asyncio.run(ns["precheck_analysis"](symbol="588000", market_type=None, user=self.user_a))
        self.assertFalse(res["can_submit"])
        self.assertEqual(res["reason"], "no_llm")

        # 2) Enabled LLM with placeholder key and unmatched provider -> reason="no_llm_key"
        cfg_bad_key = types.SimpleNamespace(
            data_source_configs=[ds],
            llm_configs=[types.SimpleNamespace(enabled=True, provider="openai", model_name="gpt-4o", api_key="your_key_here")],
        )
        ns, mods, *_ = self._build_analysis_env(db, cfg_bad_key)
        with _TempModules(mods):
            res = asyncio.run(ns["precheck_analysis"](symbol="588000", market_type=None, user=self.user_a))
        self.assertFalse(res["can_submit"])
        self.assertEqual(res["reason"], "no_llm_key")

        # 3) Valid LLM via llm_providers: 588000 / 560010 / 180101 route to etf_basic_info
        cfg_ok = types.SimpleNamespace(
            data_source_configs=[ds],
            llm_configs=[types.SimpleNamespace(enabled=True, provider="deepseek", model_name="deepseek-chat", api_key=None)],
        )
        ns, mods, *_ = self._build_analysis_env(db, cfg_ok)
        with _TempModules(mods):
            for sym in ("588000", "588000.SH", "560010", "180101"):
                res = asyncio.run(ns["precheck_analysis"](symbol=sym, market_type=None, user=self.user_a))
                self.assertTrue(res["can_submit"], sym)
                self.assertEqual(res["details"]["basics_collection"], "etf_basic_info")
                self.assertTrue(res["details"]["is_etf"])

            # Beijing Exchange (.BJ) suffix normalized and validated against stock_basic_info
            res_bj = asyncio.run(ns["precheck_analysis"](symbol="920001.BJ", market_type=None, user=self.user_a))
            self.assertTrue(res_bj["can_submit"])
            self.assertEqual(res_bj["details"]["market"], "CN")
            self.assertEqual(res_bj["details"]["symbol"], "920001")
            self.assertEqual(res_bj["details"]["basics_collection"], "stock_basic_info")

            # HK / US should not be falsely blocked against CN stock_basic_info
            res_us = asyncio.run(ns["precheck_analysis"](symbol="BRK.B", market_type=None, user=self.user_a))
            self.assertTrue(res_us["can_submit"])
            self.assertEqual(res_us["details"]["market"], "US")
            self.assertEqual(res_us["details"]["basics_collection"], "stock_basic_info_us")

            res_hk = asyncio.run(ns["precheck_analysis"](symbol="00700.HK", market_type=None, user=self.user_a))
            self.assertTrue(res_hk["can_submit"])
            self.assertEqual(res_hk["details"]["market"], "HK")
            self.assertEqual(res_hk["details"]["basics_collection"], "stock_basic_info_hk")

    def test_single_analysis_preserves_etf_metadata_and_handles_enqueue_failure(self):
        db = FakeDB()
        ns, mods, _q, created, enqueued, _ = self._build_analysis_env(db)
        req = ns["SingleAnalysisRequest"](
            symbol="588000.SH",
            parameters=ns["AnalysisParameters"](market_type="A股", engine=FakeAnalysisEngine.V2),
        )
        with _TempModules(mods):
            resp = asyncio.run(ns["submit_single_analysis"](req, None, user=self.user_a))

        self.assertTrue(resp["success"])
        self.assertEqual(resp["data"]["market_type"], "ETF")
        self.assertTrue(resp["data"]["is_etf"])
        self.assertEqual(resp["data"]["task_type"], "etf_analysis")
        self.assertEqual(created[0]["task_type"], "etf_analysis")
        self.assertEqual(created[0]["task_params"]["market_type"], "ETF")
        self.assertTrue(created[0]["task_params"]["is_etf"])
        self.assertEqual(enqueued[0]["symbol"], "588000")
        self.assertEqual(enqueued[0]["params"]["market_type"], "ETF")
        self.assertTrue(enqueued[0]["params"]["is_etf"])

        # Enqueue failure must write back FAILED to DB and memory_manager
        def _fail_enqueue(_sym, _tid, _params):
            raise RuntimeError("redis down")

        db_fail = FakeDB()
        ns_f, mods_f, *_ = self._build_analysis_env(db_fail, enqueue_side_effect=_fail_enqueue)
        req_f = ns_f["SingleAnalysisRequest"](symbol="560010", parameters=ns_f["AnalysisParameters"]())
        with _TempModules(mods_f):
            with self.assertRaises(FakeHTTPException) as ctx:
                asyncio.run(ns_f["submit_single_analysis"](req_f, None, user=self.user_a))
        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(db_fail.unified_analysis_tasks.docs[0]["status"], "failed")
        self.assertEqual(self.memory_manager.tasks["task-1"].status, FakeTaskStatus.FAILED)

    def test_batch_analysis_splits_etf_and_stock_persists_redis_batch_and_reports_failed_submissions(self):
        db = FakeDB()

        def _partial_fail(symbol, _tid, _params):
            if symbol == "180101":
                raise RuntimeError("queue full")

        ns, mods, _q, created, enqueued, _ = self._build_analysis_env(db, enqueue_side_effect=_partial_fail)
        req = ns["BatchAnalysisRequest"](
            symbols=["600519.SH", "588000.SH", "180101"],
            title="混合批测",
            parameters=ns["AnalysisParameters"](market_type="A股", engine=FakeAnalysisEngine.V2),
        )
        with _TempModules(mods):
            resp = asyncio.run(ns["submit_batch_analysis"](req, user=self.user_a))

        data = resp["data"]
        self.assertTrue(resp["success"])
        self.assertEqual(data["total_tasks"], 3)
        self.assertEqual(data["submitted_tasks"], 2)
        self.assertEqual(len(data["failed_submissions"]), 1)
        self.assertEqual(data["failed_submissions"][0]["symbol"], "180101")
        self.assertEqual(data["status"], "partial_failed")

        by_symbol = {c["task_params"]["symbol"]: c for c in created}
        self.assertEqual(by_symbol["600519"]["task_type"], "stock_analysis")
        self.assertEqual(by_symbol["588000"]["task_type"], "etf_analysis")
        self.assertEqual(by_symbol["588000"]["task_params"]["market_type"], "ETF")
        self.assertTrue(by_symbol["588000"]["task_params"]["is_etf"])
        self.assertEqual(by_symbol["180101"]["task_type"], "etf_analysis")

        # Failed item is written back to failed in unified_analysis_tasks
        failed_doc = next(d for d in db.unified_analysis_tasks.docs if d["task_params"]["symbol"] == "180101")
        self.assertEqual(failed_doc["status"], "failed")

        # Redis batch metadata persisted under qa:batch:<batch_id>
        batch_key = "qa:batch:" + data["batch_id"]
        self.assertIn(batch_key, self.redis.hashes)
        self.assertEqual(self.redis.hashes[batch_key]["user"], self.user_a_id)
        self.assertEqual(self.redis.hashes[batch_key]["submitted"], "2")
        self.assertEqual(self.redis.hashes[batch_key]["failed"], "1")

        # GET /api/analysis/batches/{batch_id} returns persisted batch for owner and 404 for other user
        batch_view = asyncio.run(ns["get_batch"](data["batch_id"], user=self.user_a, svc=_q))
        self.assertEqual(batch_view["id"], data["batch_id"])
        self.assertEqual(batch_view["user"], self.user_a_id)
        self.assertEqual(len(batch_view["tasks"]), 2)
        with self.assertRaises(FakeHTTPException) as ctx_b:
            asyncio.run(ns["get_batch"](data["batch_id"], user=self.user_b, svc=_q))
        self.assertEqual(ctx_b.exception.status_code, 404)

    def test_control_plane_cancel_mark_failed_delete_idor_feedback_retry_and_history(self):
        t_cancel = "task-v2-cancel"
        t_fail = "task-v2-fail"
        t_del = "task-v2-del"
        t_fb = "task-v2-fb"
        t_retry = "task-v2-retry"

        db = FakeDB({
            "unified_analysis_tasks": [
                {"task_id": t_cancel, "user_id": FakeObjectId(self.user_a_id), "task_type": "etf_analysis", "status": "processing", "task_params": {"symbol": "588000", "market_type": "ETF", "is_etf": True}, "created_at": datetime(2026, 11, 18, 9, 0)},
                {"task_id": t_fail, "user_id": FakeObjectId(self.user_a_id), "task_type": "stock_analysis", "status": "processing", "task_params": {"symbol": "600519", "market_type": "cn"}, "created_at": datetime(2026, 11, 19, 9, 0)},
                {"task_id": t_del, "user_id": FakeObjectId(self.user_a_id), "task_type": "stock_analysis", "status": "completed", "task_params": {"symbol": "000001", "market_type": "cn"}, "created_at": datetime(2026, 11, 17, 9, 0)},
                {"task_id": t_fb, "user_id": FakeObjectId(self.user_a_id), "task_type": "etf_analysis", "status": "completed", "task_params": {"symbol": "560010", "market_type": "ETF"}, "created_at": datetime(2026, 11, 16, 9, 0)},
                {"task_id": t_retry, "user_id": FakeObjectId(self.user_a_id), "task_type": "etf_analysis", "status": "failed", "task_params": {"symbol": "588000", "market_type": "ETF", "is_etf": True}, "created_at": datetime(2026, 11, 10, 9, 0)},
            ],
            "analysis_tasks": [],
        })
        asyncio.run(self.memory_manager.create_task(t_cancel, self.user_a_id, "588000"))
        asyncio.run(self.memory_manager.create_task(t_fail, self.user_a_id, "600519"))
        asyncio.run(self.memory_manager.create_task(t_del, self.user_a_id, "000001"))

        ns, mods, queue_svc, _, _, cancelled_redis = self._build_analysis_env(db)
        queue_svc.tasks[t_cancel] = {"id": t_cancel, "user": self.user_a_id, "status": "processing"}
        queue_svc.tasks[t_fail] = {"id": t_fail, "user": self.user_a_id, "status": "processing"}

        with _TempModules(mods):
            # 1) Non-owner DELETE blocked with 403 (IDOR protection)
            with self.assertRaises(FakeHTTPException) as ctx_idor:
                asyncio.run(ns["delete_task"](t_del, user=self.user_b))
            self.assertEqual(ctx_idor.exception.status_code, 403)
            self.assertIsNotNone(asyncio.run(db.unified_analysis_tasks.find_one({"task_id": t_del})))

            # Owner DELETE succeeds across DB, memory, and Redis
            del_res = asyncio.run(ns["delete_task"](t_del, user=self.user_a))
            self.assertTrue(del_res["success"])
            self.assertIsNone(asyncio.run(db.unified_analysis_tasks.find_one({"task_id": t_del})))
            self.assertIsNone(asyncio.run(self.memory_manager.get_task(t_del)))

            # 2) Cancel syncs unified_analysis_tasks + Redis + memory_manager
            cancel_res = asyncio.run(ns["cancel_task"](t_cancel, user=self.user_a, svc=queue_svc))
            self.assertTrue(cancel_res["success"])
            doc_c = asyncio.run(db.unified_analysis_tasks.find_one({"task_id": t_cancel}))
            self.assertEqual(doc_c["status"], "cancelled")
            self.assertIn(t_cancel, cancelled_redis)
            self.assertEqual(self.memory_manager.tasks[t_cancel].status, FakeTaskStatus.CANCELLED)

            # 3) Mark-failed syncs unified_analysis_tasks + Redis + memory_manager
            mf_res = asyncio.run(ns["mark_task_as_failed"](t_fail, user=self.user_a))
            self.assertTrue(mf_res["success"])
            doc_f = asyncio.run(db.unified_analysis_tasks.find_one({"task_id": t_fail}))
            self.assertEqual(doc_f["status"], "failed")
            self.assertEqual(self.memory_manager.tasks[t_fail].status, FakeTaskStatus.FAILED)

            # 4) Feedback works on v2 unified_analysis_tasks
            fb_res = asyncio.run(
                ns["submit_analysis_feedback"](
                    t_fb,
                    types.SimpleNamespace(rating=5, comment="非常准确"),
                    user=self.user_a,
                )
            )
            self.assertEqual(fb_res["rating"], 5)
            doc_fb = asyncio.run(db.unified_analysis_tasks.find_one({"task_id": t_fb}))
            self.assertEqual(doc_fb["user_rating"], 5)
            self.assertEqual(db.analysis_feedback.docs[0]["code"], "560010")

            # 5) user/history pushes symbol & date filters down to MongoDB
            hist_res = asyncio.run(
                ns["get_user_analysis_history"](
                    user=self.user_a,
                    status=None,
                    start_date="2026-11-10",
                    end_date="2026-11-10",
                    symbol="588000.SH",
                    stock_code=None,
                    market_type="ETF",
                    page=1,
                    page_size=1,
                )
            )
            self.assertTrue(hist_res["success"])
            self.assertEqual(hist_res["data"]["total"], 1)
            self.assertEqual(hist_res["data"]["tasks"][0]["task_id"], t_retry)
            # Confirm symbol & created_at filters were pushed down into the MongoDB query
            last_q = db.unified_analysis_tasks.find_queries[-1]
            self.assertIn("$and", last_q)
            self.assertTrue(any("created_at" in clause for clause in last_q["$and"]))

        # 6) retry_task raises 503 and writes FAILED back when enqueue fails
        def _fail_retry(_sym, _tid, _params):
            raise RuntimeError("redis unavailable")

        ns_r, mods_r, *_ = self._build_analysis_env(db, enqueue_side_effect=_fail_retry)
        with _TempModules(mods_r):
            with self.assertRaises(FakeHTTPException) as ctx_retry:
                asyncio.run(ns_r["retry_task"](t_retry, user=self.user_a))
        self.assertEqual(ctx_retry.exception.status_code, 503)
        new_doc = asyncio.run(db.unified_analysis_tasks.find_one({"task_id": "task-1"}))
        self.assertEqual(new_doc["status"], "failed")
        self.assertEqual(new_doc["task_type"], "etf_analysis")
        self.assertIn("入队失败", new_doc["error_message"])


# ---------------------------------------------------------------------------
# 3. unified_tasks.py DELETE Redis/memory sync & regex escape
# ---------------------------------------------------------------------------

class UnifiedTasksRouterTests(unittest.TestCase):
    def test_delete_v2_task_cleans_redis_and_memory_state(self):
        uid = "507f1f77bcf86cd799439012"
        user = {"id": uid, "username": "alice"}
        redis_cancelled: List[str] = []
        mem_mgr = FakeMemoryManager()
        asyncio.run(mem_mgr.create_task("v2-t1", uid, "588000"))

        fake_task_obj = types.SimpleNamespace(task_id="v2-t1", user_id=FakeObjectId(uid), status=FakeAnalysisStatus.PROCESSING)
        fake_svc = types.SimpleNamespace(
            get_task=AsyncMock(return_value=fake_task_obj),
            cancel_task=AsyncMock(return_value=True),
        )
        fake_queue = types.SimpleNamespace(
            cancel_task=AsyncMock(side_effect=lambda tid: redis_cancelled.append(tid) or True)
        )

        stub_queue_mod = types.ModuleType("app.services.queue_service")
        stub_queue_mod.get_queue_service = lambda: fake_queue
        stub_mem_mod = types.ModuleType("app.services.memory_state_manager")
        stub_mem_mod.get_memory_state_manager = lambda: mem_mgr
        stub_mem_mod.TaskStatus = FakeTaskStatus

        ns: Dict[str, Any] = {
            "PyObjectId": FakeObjectId,
            "get_task_analysis_service": lambda: fake_svc,
            "HTTPException": FakeHTTPException,
            "Depends": lambda fn=None: fn,
            "get_current_user": lambda: None,
            "logger": _SilentLogger(),
        }
        _extract_functions(ROOT / "app/routers/unified_tasks.py", ["cancel_task"], ns)

        with _TempModules({
            "app.services.queue_service": stub_queue_mod,
            "app.services.memory_state_manager": stub_mem_mod,
        }):
            res = asyncio.run(ns["cancel_task"]("v2-t1", user=user))

        self.assertTrue(res["success"])
        self.assertEqual(redis_cancelled, ["v2-t1"])
        self.assertEqual(mem_mgr.tasks["v2-t1"].status, FakeTaskStatus.CANCELLED)


# ---------------------------------------------------------------------------
# 4. analysis_worker.py MAX_STALE_DAYS fix & ETF JIT path
# ---------------------------------------------------------------------------

class AnalysisWorkerJitSyncTests(unittest.TestCase):
    def setUp(self):
        self.su = _load_stock_utils_namespace()

    def test_jit_sync_identifies_56_58_18_etf_and_avoids_stock_providers(self):
        @dataclass
        class _FakeDVR:
            is_valid: bool
            message: str
            missing_data: List[str]
            details: Dict[str, Any]

        stub_dvs = types.ModuleType("app.services.data_validation_service")
        stub_dvs.DataValidationResult = _FakeDVR

        stub_su = types.ModuleType("tradingagents.utils.stock_utils")
        stub_su.StockUtils = self.su["StockUtils"]
        stub_su.SecurityType = self.su["SecurityType"]

        stub_pd = types.ModuleType("pandas")

        # Poison stock data_source_priority to prove ETF never enters stock JIT branch
        stub_dsp = types.ModuleType("app.core.data_source_priority")
        stub_dsp.get_enabled_data_sources_async = AsyncMock(
            side_effect=AssertionError("ETF JIT must not invoke A-share stock provider path")
        )

        synced_calls: List[List[str]] = []

        class _FakeEtfSyncService:
            def __init__(self, should_fail: bool = False):
                self.should_fail = should_fail

            async def sync_symbols(self, symbols, force_update=False, days=60):
                synced_calls.append(list(symbols))
                if self.should_fail:
                    raise RuntimeError("etf upstream timeout")
                return {
                    "data_source_used": "akshare",
                    "success_count": len(symbols),
                    "records_inserted": 5,
                    "errors": [],
                }

        active_etf_svc = _FakeEtfSyncService(should_fail=False)

        async def _get_etf_svc():
            return active_etf_svc

        stub_etf_sync = types.ModuleType("app.worker.etf_sync_service")
        stub_etf_sync.get_etf_sync_service = _get_etf_svc

        db = FakeDB({
            "etf_daily_quotes": [
                {"code": "588000", "symbol": "588000", "trade_date": "2026-10-27", "close": 1.12},
            ],
        })
        stub_db_mod = types.ModuleType("app.core.database")
        stub_db_mod.get_mongo_db = lambda: db

        ns: Dict[str, Any] = {
            "Dict": Dict, "Any": Any, "Optional": Optional,
            "datetime": datetime, "timedelta": timedelta,
            "logger": _SilentLogger(),
        }
        _extract_functions(ROOT / "app/worker/analysis_worker.py", ["_jit_sync_etf_data", "jit_sync_stock_data"], ns)

        with _TempModules({
            "pandas": stub_pd,
            "app.services.data_validation_service": stub_dvs,
            "tradingagents.utils.stock_utils": stub_su,
            "app.core.data_source_priority": stub_dsp,
            "app.worker.etf_sync_service": stub_etf_sync,
            "app.core.database": stub_db_mod,
        }):
            # 1) Online ETF JIT sync succeeds for 588000 / 588000.SH / 560010 / 180101
            for etf_code, expected_norm in (("588000", "588000"), ("588000.SH", "588000"), ("560010", "560010"), ("180101", "180101")):
                res = asyncio.run(ns["jit_sync_stock_data"](etf_code, {"market_type": "cn"}))
                self.assertTrue(res.is_valid, etf_code)
                self.assertTrue(res.details["is_etf"], etf_code)
                self.assertEqual(res.details["market_type"], "ETF", etf_code)
                self.assertEqual(res.details["jit_mode"], "etf", etf_code)
                self.assertEqual(res.details["symbol"], expected_norm, etf_code)

            self.assertEqual(synced_calls, [["588000"], ["588000"], ["560010"], ["180101"]])

            # 2) Online ETF sync fails -> falls back to local etf_daily_quotes
            active_etf_svc = _FakeEtfSyncService(should_fail=True)
            res_fb = asyncio.run(ns["jit_sync_stock_data"]("588000", {"market_type": "ETF"}))
            self.assertTrue(res_fb.is_valid)
            self.assertEqual(res_fb.details["jit_mode"], "etf_local_fallback")
            self.assertEqual(res_fb.details["latest_date"], "2026-10-27")

            # 3) Online ETF sync fails AND no local etf_daily_quotes -> returns is_valid=False
            res_miss = asyncio.run(ns["jit_sync_stock_data"]("560010", {"market_type": "ETF"}))
            self.assertFalse(res_miss.is_valid)
            self.assertEqual(res_miss.missing_data, ["etf_daily_quotes"])

    def test_jit_sync_stale_fallback_does_not_raise_max_stale_days_name_error(self):
        @dataclass
        class _FakeDVR:
            is_valid: bool
            message: str
            missing_data: List[str]
            details: Dict[str, Any]

        class _FakeSeries:
            def __init__(self, vals):
                self._vals = vals

            def max(self):
                return max(self._vals)

        class _FakeDF:
            empty = False
            columns = ["trade_date", "close"]

            def __getitem__(self, col):
                return _FakeSeries(["2026-10-01"])

        stub_pd = types.ModuleType("pandas")
        stub_pd.isna = lambda v: v is None
        stub_pd.to_datetime = lambda s: datetime.strptime(str(s)[:10], "%Y-%m-%d")

        stub_dvs = types.ModuleType("app.services.data_validation_service")
        stub_dvs.DataValidationResult = _FakeDVR

        stub_su = types.ModuleType("tradingagents.utils.stock_utils")
        stub_su.StockUtils = self.su["StockUtils"]
        stub_su.SecurityType = self.su["SecurityType"]

        stub_dsp = types.ModuleType("app.core.data_source_priority")
        stub_dsp.get_enabled_data_sources_async = AsyncMock(return_value=["akshare"])

        fake_provider = types.SimpleNamespace(get_historical_data=AsyncMock(return_value=_FakeDF()))
        stub_ak = types.ModuleType("tradingagents.dataflows.providers.china.akshare")
        stub_ak.get_akshare_provider = lambda: fake_provider

        class _FakeHistSvc:
            async def _ensure_indexes(self):
                pass

            async def save_historical_data(self, **_kw):
                return 1

        stub_hist = types.ModuleType("app.services.historical_data_service")
        stub_hist.HistoricalDataService = _FakeHistSvc

        stub_db_mod = types.ModuleType("app.core.database")
        stub_db_mod.get_mongo_db = lambda: FakeDB()

        stub_cal = types.ModuleType("app.services.trading_calendar_service")
        stub_cal.get_trading_calendar_service = lambda: types.SimpleNamespace(
            get_last_trade_date=AsyncMock(return_value="2026-11-20")
        )

        ns: Dict[str, Any] = {
            "Dict": Dict, "Any": Any, "Optional": Optional,
            "datetime": datetime, "timedelta": timedelta,
            "logger": _SilentLogger(),
        }
        _extract_functions(ROOT / "app/worker/analysis_worker.py", ["jit_sync_stock_data"], ns)

        with _TempModules({
            "pandas": stub_pd,
            "app.services.data_validation_service": stub_dvs,
            "tradingagents.utils.stock_utils": stub_su,
            "app.core.data_source_priority": stub_dsp,
            "tradingagents.dataflows.providers.china.akshare": stub_ak,
            "app.services.historical_data_service": stub_hist,
            "app.core.database": stub_db_mod,
            "app.services.trading_calendar_service": stub_cal,
        }):
            res = asyncio.run(ns["jit_sync_stock_data"]("600519", {"market_type": "cn"}))

        self.assertTrue(res.is_valid)
        self.assertTrue(res.details.get("data_stale"))
        self.assertIn("3 个工作日", res.message)


# ---------------------------------------------------------------------------
# 5. thread_worker.py exception fallback (prevents permanent zombie tasks)
# ---------------------------------------------------------------------------

class ThreadWorkerZombiePreventionTests(unittest.TestCase):
    def test_process_task_exception_marks_db_and_memory_failed_with_friendly_message(self):
        su = _load_stock_utils_namespace()
        db = FakeDB()
        mem_mgr = FakeMemoryManager()
        uid = "507f1f77bcf86cd799439012"
        tid = "tw-task-boom"

        task_obj = types.SimpleNamespace(
            task_id=tid,
            user_id=uid,
            task_type=FakeAnalysisTaskType.ETF_ANALYSIS,
            status=FakeAnalysisStatus.PENDING,
            started_at=None,
            completed_at=None,
            progress=0,
            current_step=None,
            message=None,
            error_message=None,
        )
        updated_snapshots: List[str] = []

        class _FakeTaskService:
            async def get_task(self, _id):
                return task_obj

            async def _update_task(self, t):
                updated_snapshots.append(t.status.value)

            async def execute_task(self, _t):
                raise RuntimeError("LLM provider timeout")

        acked: List[tuple] = []

        class _FakeQueue:
            async def ack_task(self, task_id, success):
                acked.append((task_id, success))

            async def cancel_task(self, _task_id):
                return True

        stub_mem = types.ModuleType("app.services.memory_state_manager")
        stub_mem.get_memory_state_manager = lambda: mem_mgr
        stub_mem.TaskStatus = FakeTaskStatus

        stub_tz = types.ModuleType("app.utils.timezone")
        stub_tz.now_tz = lambda: datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)

        stub_err_fmt = types.ModuleType("app.utils.error_formatter")
        stub_err_fmt.ErrorFormatter = types.SimpleNamespace(
            format_user_message=lambda msg: f"友好提示: {msg}"
        )

        stub_aw = types.ModuleType("app.worker.analysis_worker")
        stub_aw.jit_sync_stock_data = AsyncMock(
            return_value=types.SimpleNamespace(is_valid=True, message="ok")
        )

        ns: Dict[str, Any] = {
            "Dict": Dict, "Any": Any, "Optional": Optional,
            "datetime": datetime,
            "get_mongo_db": lambda: db,
            "UnifiedAnalysisTask": Any,
            "AnalysisTaskType": FakeAnalysisTaskType,
            "AnalysisStatus": FakeAnalysisStatus,
            "StockUtils": su["StockUtils"],
            "SecurityType": su["SecurityType"],
            "logger": _SilentLogger(),
        }
        ThreadWorkerCls = _extract_class_methods(
            ROOT / "app/services/thread_worker.py",
            "ThreadWorker",
            ["_process_task_async", "_mark_task_failed_fallback"],
            ns,
        )
        worker = ThreadWorkerCls.__new__(ThreadWorkerCls)
        worker.task_service = _FakeTaskService()
        worker.queue_service = _FakeQueue()

        stub_bson = types.ModuleType("bson")
        stub_bson.ObjectId = FakeObjectId

        with _TempModules({
            "bson": stub_bson,
            "app.services.memory_state_manager": stub_mem,
            "app.utils.timezone": stub_tz,
            "app.utils.error_formatter": stub_err_fmt,
            "app.worker.analysis_worker": stub_aw,
        }):
            asyncio.run(worker._process_task_async({
                "id": tid,
                "symbol": "588000.SH",
                "user": uid,
                "parameters": {"engine": "v2", "market_type": "ETF"},
            }))

        self.assertEqual(task_obj.status, FakeAnalysisStatus.FAILED)
        self.assertEqual(task_obj.error_message, "友好提示: LLM provider timeout")
        self.assertEqual(mem_mgr.tasks[tid].status, FakeTaskStatus.FAILED)
        self.assertEqual(mem_mgr.tasks[tid].error_message, "友好提示: LLM provider timeout")
        self.assertEqual(acked, [(tid, False)])

    def test_jit_failure_and_cancellation_do_not_leave_zombie_or_wrong_ack(self):
        su = _load_stock_utils_namespace()
        db = FakeDB()
        mem_mgr = FakeMemoryManager()
        uid = "507f1f77bcf86cd799439012"

        class TaskCancelledException(Exception):
            pass

        def _make_worker(task_obj, execute_exc=None, jit_valid=True):
            acked: List[tuple] = []
            cancelled_q: List[str] = []

            class _FakeTaskService:
                async def get_task(self, _id):
                    return task_obj

                async def _update_task(self, _t):
                    pass

                async def execute_task(self, _t):
                    if execute_exc is not None:
                        raise execute_exc

            class _FakeQueue:
                async def ack_task(self, task_id, success):
                    acked.append((task_id, success))

                async def cancel_task(self, task_id):
                    cancelled_q.append(task_id)
                    return True

            ns: Dict[str, Any] = {
                "Dict": Dict, "Any": Any, "Optional": Optional,
                "datetime": datetime,
                "get_mongo_db": lambda: db,
                "UnifiedAnalysisTask": Any,
                "AnalysisTaskType": FakeAnalysisTaskType,
                "AnalysisStatus": FakeAnalysisStatus,
                "StockUtils": su["StockUtils"],
                "SecurityType": su["SecurityType"],
                "logger": _SilentLogger(),
            }
            ThreadWorkerCls = _extract_class_methods(
                ROOT / "app/services/thread_worker.py",
                "ThreadWorker",
                ["_process_task_async", "_mark_task_failed_fallback"],
                ns,
            )
            w = ThreadWorkerCls.__new__(ThreadWorkerCls)
            w.task_service = _FakeTaskService()
            w.queue_service = _FakeQueue()

            stub_mem = types.ModuleType("app.services.memory_state_manager")
            stub_mem.get_memory_state_manager = lambda: mem_mgr
            stub_mem.TaskStatus = FakeTaskStatus

            stub_tz = types.ModuleType("app.utils.timezone")
            stub_tz.now_tz = lambda: datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)

            stub_err_fmt = types.ModuleType("app.utils.error_formatter")
            stub_err_fmt.ErrorFormatter = types.SimpleNamespace(format_user_message=lambda msg: msg)

            stub_aw = types.ModuleType("app.worker.analysis_worker")
            stub_aw.jit_sync_stock_data = AsyncMock(
                return_value=types.SimpleNamespace(is_valid=jit_valid, message="ETF日线缺失" if not jit_valid else "ok")
            )
            stub_bson = types.ModuleType("bson")
            stub_bson.ObjectId = FakeObjectId
            mods = {
                "bson": stub_bson,
                "app.services.memory_state_manager": stub_mem,
                "app.utils.timezone": stub_tz,
                "app.utils.error_formatter": stub_err_fmt,
                "app.worker.analysis_worker": stub_aw,
            }
            return w, mods, acked, cancelled_q

        # 1) JIT ValueError marks unified_analysis_tasks + memory_manager FAILED
        t_jit = types.SimpleNamespace(
            task_id="tw-jit-fail", user_id=uid, task_type=FakeAnalysisTaskType.ETF_ANALYSIS,
            status=FakeAnalysisStatus.PENDING, started_at=None, completed_at=None,
            progress=0, current_step=None, message=None, error_message=None,
        )
        w1, m1, ack1, _ = _make_worker(t_jit, jit_valid=False)
        with _TempModules(m1):
            asyncio.run(w1._process_task_async({"id": "tw-jit-fail", "symbol": "588000", "user": uid, "parameters": {"engine": "v2"}}))
        self.assertEqual(t_jit.status, FakeAnalysisStatus.FAILED)
        self.assertIn("ETF日线缺失", t_jit.error_message)
        self.assertEqual(mem_mgr.tasks["tw-jit-fail"].status, FakeTaskStatus.FAILED)
        self.assertEqual(ack1, [("tw-jit-fail", False)])

        # 2) TaskCancelledException keeps CANCELLED and skips ack_task(False)
        t_cancel = types.SimpleNamespace(
            task_id="tw-cancelled", user_id=uid, task_type=FakeAnalysisTaskType.STOCK_ANALYSIS,
            status=FakeAnalysisStatus.PENDING, started_at=None, completed_at=None,
            progress=0, current_step=None, message=None, error_message=None,
        )
        w2, m2, ack2, can2 = _make_worker(t_cancel, execute_exc=TaskCancelledException("用户取消"), jit_valid=True)
        with _TempModules(m2):
            asyncio.run(w2._process_task_async({"id": "tw-cancelled", "symbol": "600519", "user": uid, "parameters": {"engine": "v2"}}))
        self.assertEqual(t_cancel.status, FakeAnalysisStatus.CANCELLED)
        self.assertEqual(mem_mgr.tasks["tw-cancelled"].status, FakeTaskStatus.CANCELLED)
        self.assertEqual(ack2, [])
        self.assertEqual(can2, ["tw-cancelled"])


# ---------------------------------------------------------------------------
# 6. simple_analysis_service.py zombie inspection & cleanup (v2 + legacy)
# ---------------------------------------------------------------------------

class SimpleAnalysisServiceZombieTests(unittest.TestCase):
    def test_get_and_cleanup_zombie_tasks_covers_unified_and_legacy_tables(self):
        now_utc = datetime.now(timezone.utc)
        old_time = now_utc - timedelta(hours=5)
        fresh_time = now_utc - timedelta(minutes=10)

        db = FakeDB({
            "unified_analysis_tasks": [
                {
                    "task_id": "u-zombie",
                    "user_id": "u1",
                    "task_type": "etf_analysis",
                    "status": "processing",
                    "task_params": {"symbol": "588000"},
                    "started_at": old_time,
                    "created_at": old_time,
                },
                {
                    "task_id": "u-fresh",
                    "user_id": "u1",
                    "task_type": "stock_analysis",
                    "status": "processing",
                    "task_params": {"symbol": "600519"},
                    "started_at": fresh_time,
                    "created_at": fresh_time,
                },
            ],
            "analysis_tasks": [
                {
                    "task_id": "l-zombie",
                    "user_id": "u1",
                    "stock_code": "000001",
                    "status": "pending",
                    "created_at": old_time,
                },
            ],
        })

        ns: Dict[str, Any] = {
            "Dict": Dict, "List": List, "Any": Any, "Optional": Optional,
            "datetime": datetime,
            "get_mongo_db": lambda: db,
            "logger": _SilentLogger(),
        }
        SvcCls = _extract_class_methods(
            ROOT / "app/services/simple_analysis_service.py",
            "SimpleAnalysisService",
            ["get_zombie_tasks", "cleanup_zombie_tasks"],
            ns,
        )
        svc = SvcCls.__new__(SvcCls)
        svc.memory_manager = FakeMemoryManager()

        zombies = asyncio.run(svc.get_zombie_tasks(max_running_hours=2))
        zombie_ids = [z["task_id"] for z in zombies]
        self.assertEqual(zombie_ids, ["u-zombie", "l-zombie"])
        self.assertEqual(zombies[0]["source_collection"], "unified_analysis_tasks")
        self.assertEqual(zombies[0]["stock_code"], "588000")
        self.assertEqual(zombies[1]["source_collection"], "analysis_tasks")

        summary = asyncio.run(svc.cleanup_zombie_tasks(max_running_hours=2))
        self.assertTrue(summary["success"])
        self.assertEqual(summary["unified_cleaned"], 1)
        self.assertEqual(summary["legacy_cleaned"], 1)
        self.assertEqual(summary["mongo_cleaned"], 2)

        u_zombie = asyncio.run(db.unified_analysis_tasks.find_one({"task_id": "u-zombie"}))
        u_fresh = asyncio.run(db.unified_analysis_tasks.find_one({"task_id": "u-fresh"}))
        l_zombie = asyncio.run(db.analysis_tasks.find_one({"task_id": "l-zombie"}))
        self.assertEqual(u_zombie["status"], "failed")
        self.assertEqual(u_fresh["status"], "processing")
        self.assertEqual(l_zombie["status"], "failed")


if __name__ == "__main__":
    unittest.main()
