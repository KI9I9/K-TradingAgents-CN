"""Offline contract tests for paper trading T+1/ETF fees/PnL and review timestamp/LLM/router fixes.

Pure Python standard library (unittest) + AST execution of real production functions
against a type-strict FakeMongoDB (matching MongoDB's BSON String vs BSON Date semantics).
"""
from __future__ import annotations

import ast
import copy
import re
import sys
import types
import unittest
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.services import paper_quote_supply as supply  # noqa: E402
from app.services.quote_contract import (  # noqa: E402
    CN_TZ,
    parse_quote_time,
    resolve_cn_price,
    resolve_cn_range,
    validate_price,
)

PAPER_PATH = ROOT / "app/routers/paper.py"
REVIEW_ROUTER_PATH = ROOT / "app/routers/review.py"
REVIEW_SERVICE_PATH = ROOT / "app/services/trade_review_service.py"


# --------------------------------------------------------------------------- #
# Type-strict in-memory FakeMongoDB (enforces BSON String != BSON Date!)
# --------------------------------------------------------------------------- #
def _normalize_dt_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_nested(doc: dict, dotted_key: str) -> Any:
    cur: Any = doc
    for part in dotted_key.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _set_nested(doc: dict, dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    cur = doc
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def _inc_nested(doc: dict, dotted_key: str, delta: float) -> None:
    existing = _get_nested(doc, dotted_key) or 0
    _set_nested(doc, dotted_key, existing + delta)


def _compare_values(field_val: Any, op: str, target: Any) -> bool:
    """Enforce MongoDB type-bracket comparison: String only matches String, Date only matches Date."""
    if field_val is None:
        return False
    if isinstance(field_val, datetime) and isinstance(target, datetime):
        lhs = _normalize_dt_utc(field_val)
        rhs = _normalize_dt_utc(target)
    elif type(field_val) is str and type(target) is str:
        lhs, rhs = field_val, target
    elif isinstance(field_val, (int, float)) and not isinstance(field_val, bool) and isinstance(target, (int, float)) and not isinstance(target, bool):
        lhs, rhs = field_val, target
    else:
        # Cross-type range comparison (e.g. ISO str vs datetime) NEVER matches in MongoDB.
        return False

    if op == "$gte":
        return lhs >= rhs
    if op == "$lte":
        return lhs <= rhs
    if op == "$gt":
        return lhs > rhs
    if op == "$lt":
        return lhs < rhs
    raise ValueError(f"unsupported op: {op}")


def _matches(doc: dict, query: Optional[dict]) -> bool:
    if not query:
        return True
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in expected):
                return False
            continue
        if key == "$and":
            if not all(_matches(doc, sub) for sub in expected):
                return False
            continue
        actual = _get_nested(doc, key)
        if isinstance(expected, dict):
            for op, val in expected.items():
                if op == "$in":
                    if actual not in val:
                        return False
                elif op in ("$gte", "$lte", "$gt", "$lt"):
                    if not _compare_values(actual, op, val):
                        return False
                else:
                    raise ValueError(f"unsupported operator: {op}")
        else:
            if actual != expected:
                return False
    return True


def _apply_projection(doc: dict, projection: Optional[dict]) -> dict:
    doc = copy.deepcopy(doc)
    if not projection:
        return doc
    include = {k for k, v in projection.items() if v and k != "_id"}
    exclude = {k for k, v in projection.items() if not v}
    if include:
        result = {k: v for k, v in doc.items() if k in include}
        if "_id" not in exclude and "_id" in doc:
            result["_id"] = doc["_id"]
        return result
    return {k: v for k, v in doc.items() if k not in exclude}


class FakeCursor:
    def __init__(self, docs: List[dict]):
        self._docs = list(docs)

    def sort(self, field: str, direction: int = 1) -> "FakeCursor":
        def sort_key(d: dict):
            val = _get_nested(d, field)
            if isinstance(val, datetime):
                return (1, _normalize_dt_utc(val).isoformat())
            if val is None:
                return (0, "")
            return (2, str(val))

        self._docs.sort(key=sort_key, reverse=(direction < 0))
        return self

    def skip(self, count: int) -> "FakeCursor":
        self._docs = self._docs[count:]
        return self

    def limit(self, count: int) -> "FakeCursor":
        self._docs = self._docs[:count]
        return self

    async def to_list(self, length=None) -> List[dict]:
        if length is None:
            return copy.deepcopy(self._docs)
        return copy.deepcopy(self._docs[:length])


class FakeCollection:
    def __init__(self, docs: Optional[List[dict]] = None):
        self.docs: List[dict] = [copy.deepcopy(d) for d in (docs or [])]
        self._seq = 1

    async def find_one(self, query=None, projection=None, sort=None, **kw) -> Optional[dict]:
        matched = [d for d in self.docs if _matches(d, query)]
        if sort and matched:
            for field, direction in reversed(sort):
                matched.sort(key=lambda d: d.get(field) or 0, reverse=(direction < 0))
        if not matched:
            return None
        return _apply_projection(matched[0], projection)

    def find(self, query=None, projection=None, **kw) -> FakeCursor:
        matched = [_apply_projection(d, projection) for d in self.docs if _matches(d, query)]
        return FakeCursor(matched)

    async def count_documents(self, query=None) -> int:
        return sum(1 for d in self.docs if _matches(d, query))

    async def insert_one(self, doc: dict):
        stored = copy.deepcopy(doc)
        if "_id" not in stored:
            stored["_id"] = f"oid-{self._seq}"
            self._seq += 1
        self.docs.append(stored)
        return types.SimpleNamespace(inserted_id=stored["_id"])

    async def update_one(self, query: dict, update: dict, upsert: bool = False):
        for doc in self.docs:
            if _matches(doc, query):
                for k, v in (update.get("$set") or {}).items():
                    _set_nested(doc, k, copy.deepcopy(v))
                for k, v in (update.get("$inc") or {}).items():
                    _inc_nested(doc, k, v)
                return types.SimpleNamespace(matched_count=1, modified_count=1)
        if upsert:
            new_doc = copy.deepcopy(query)
            for k, v in (update.get("$set") or {}).items():
                _set_nested(new_doc, k, copy.deepcopy(v))
            await self.insert_one(new_doc)
            return types.SimpleNamespace(matched_count=0, modified_count=1)
        return types.SimpleNamespace(matched_count=0, modified_count=0)

    async def delete_one(self, query: dict):
        for idx, doc in enumerate(self.docs):
            if _matches(doc, query):
                self.docs.pop(idx)
                return types.SimpleNamespace(deleted_count=1)
        return types.SimpleNamespace(deleted_count=0)

    async def delete_many(self, query: dict):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _matches(d, query)]
        return types.SimpleNamespace(deleted_count=before - len(self.docs))


class FakeMongoDB:
    def __init__(self, initial: Optional[Dict[str, List[dict]]] = None):
        self._cols: Dict[str, FakeCollection] = {}
        for name, docs in (initial or {}).items():
            self._cols[name] = FakeCollection(docs)

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self._cols:
            self._cols[name] = FakeCollection()
        return self._cols[name]


class _SilentLogger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


class _StubHTTPException(Exception):
    def __init__(self, status_code: int, detail: Any = None):
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


# --------------------------------------------------------------------------- #
# AST loaders for real production modules (zero fastapi/pydantic/bson runtime)
# --------------------------------------------------------------------------- #
def _load_paper_runtime(db: FakeMongoDB, clock_fn: Callable[[], datetime]) -> dict:
    tree = ast.parse(PAPER_PATH.read_text(encoding="utf-8"))
    wanted_names = {
        "ETF_CODE_PREFIXES",
        "INITIAL_CASH_BY_MARKET",
        "_detect_market_and_code",
        "_is_cn_etf_symbol",
        "_resolve_market_and_code",
        "_get_or_create_account",
        "_get_market_rules",
        "_calculate_commission",
        "_resolve_cn_t_plus",
        "_compute_available_qty",
        "_get_available_quantity",
        "_get_stock_name",
        "_load_cn_candidates",
        "_price_failure_reason",
        "_quote_failure",
        "_get_last_price_detail",
        "_get_today_price_range_detail",
        "_build_market_notice",
        "_format_hhmmss",
        "_build_limit_notice",
        "_position_valuation",
        "get_account",
        "place_order",
        "list_positions",
    }
    body = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted_names:
            body.append(node)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in wanted_names:
                    body.append(node)
                    break

    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)

    @dataclass
    class _PlaceOrderRequest:
        code: str
        side: str
        quantity: int
        market: Optional[str] = None
        price: Optional[float] = None
        analysis_id: Optional[str] = None

    ns: Dict[str, Any] = {
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "Tuple": Tuple,
        "datetime": datetime,
        "timedelta": timedelta,
        "re": re,
        "CN_TZ": CN_TZ,
        "cn_now": clock_fn,
        "parse_quote_time": parse_quote_time,
        "resolve_cn_price": resolve_cn_price,
        "resolve_cn_range": resolve_cn_range,
        "validate_price": validate_price,
        "get_mongo_db": lambda: db,
        "serialize_document": lambda d: dict(d),
        "ok": lambda data=None, message="ok": {"success": True, "data": data, "message": message},
        "HTTPException": _StubHTTPException,
        "status": types.SimpleNamespace(HTTP_400_BAD_REQUEST=400),
        "Depends": lambda *a, **k: None,
        "Query": lambda *a, **k: None,
        "Body": lambda *a, **k: None,
        "get_current_user": None,
        "PlaceOrderRequest": _PlaceOrderRequest,
        "router": types.SimpleNamespace(
            get=lambda *a, **k: lambda fn: fn,
            post=lambda *a, **k: lambda fn: fn,
        ),
        "logger": _SilentLogger(),
    }
    exec(compile(module, str(PAPER_PATH), "exec"), ns)
    return ns


def _load_review_service_runtime() -> dict:
    tree = ast.parse(REVIEW_SERVICE_PATH.read_text(encoding="utf-8"))
    top_names = {
        "CN_TZ",
        "_PROVIDER_DEFAULT_MODELS",
        "_PROVIDER_DEFAULT_URLS",
        "build_timestamp_range_query",
        "_merge_timestamp_filter",
    }
    cls_methods = {
        "_append_review_guardrails",
        "_sanitize_periodic_review_result",
        "get_trading_statistics",
        "create_periodic_review",
        "_calculate_period_statistics",
        "_build_trades_summary",
        "_resolve_default_llm_config",
        "_invoke_llm_text",
        "_call_ai_periodic_review",
    }

    body: List[ast.stmt] = [
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    ]
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in top_names:
            body.append(node)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in top_names:
                    body.append(node)
                    break
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id in top_names:
            body.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "TradeReviewService":
            trimmed = copy.deepcopy(node)
            trimmed.body = [
                item for item in trimmed.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in cls_methods
            ]
            body.append(trimmed)

    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)

    class _PeriodType(str, Enum):
        WEEK = "week"
        MONTH = "month"
        QUARTER = "quarter"
        YEAR = "year"

    class _ReviewStatus(str, Enum):
        PENDING = "pending"
        PROCESSING = "processing"
        COMPLETED = "completed"
        FAILED = "failed"

    @dataclass
    class _TradingStatistics:
        total_trades: int = 0
        winning_trades: int = 0
        losing_trades: int = 0
        win_rate: float = 0.0
        total_pnl: float = 0.0
        avg_profit: float = 0.0
        avg_loss: float = 0.0
        profit_loss_ratio: float = 0.0
        max_single_profit: float = 0.0
        max_single_loss: float = 0.0
        max_drawdown: float = 0.0
        avg_holding_days: float = 0.0
        total_commission: float = 0.0

        def model_dump(self):
            return dict(self.__dict__)

    @dataclass
    class _AIPeriodicReview:
        overall_score: int = 0
        summary: str = ""
        trading_style: str = ""
        common_mistakes: List[str] = dc_field(default_factory=list)
        improvement_areas: List[str] = dc_field(default_factory=list)
        action_plan: List[str] = dc_field(default_factory=list)
        best_trade: Optional[str] = None
        worst_trade: Optional[str] = None

        def model_dump(self):
            return dict(self.__dict__)

    @dataclass
    class _TradeSummaryItem:
        code: str
        name: Optional[str] = None
        side: str = ""
        quantity: int = 0
        price: float = 0.0
        pnl: float = 0.0
        pnl_pct: float = 0.0
        timestamp: str = ""

        def model_dump(self):
            return dict(self.__dict__)

    @dataclass
    class _PeriodicReviewReport:
        review_id: str
        user_id: str
        period_type: _PeriodType
        period_start: datetime
        period_end: datetime
        source: str = "paper"
        statistics: Any = None
        trades_summary: List[Any] = dc_field(default_factory=list)
        ai_review: Any = None
        status: _ReviewStatus = _ReviewStatus.COMPLETED
        execution_time: float = 0.0
        created_at: datetime = dc_field(default_factory=lambda: datetime.now(CN_TZ))

        def model_dump(self, by_alias: bool = False):
            return {
                "review_id": self.review_id,
                "user_id": self.user_id,
                "period_type": self.period_type.value,
                "period_start": self.period_start,
                "period_end": self.period_end,
                "source": self.source,
                "statistics": self.statistics.model_dump() if self.statistics else None,
                "trades_summary": [t.model_dump() for t in self.trades_summary],
                "ai_review": self.ai_review.model_dump() if self.ai_review else None,
                "status": self.status.value,
                "execution_time": self.execution_time,
                "created_at": self.created_at,
            }

    @dataclass
    class _PeriodicReviewResponse:
        review_id: str
        status: _ReviewStatus
        period_type: _PeriodType
        period_start: Optional[datetime] = None
        period_end: Optional[datetime] = None
        statistics: Any = None
        ai_review: Any = None
        execution_time: float = 0.0
        created_at: Optional[datetime] = None

    @dataclass
    class _CreatePeriodicReviewRequest:
        period_type: _PeriodType
        start_date: str
        end_date: str
        source: str = "paper"

    import uuid as _uuid

    ns: Dict[str, Any] = {
        "Any": Any,
        "Callable": Callable,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
        "uuid": _uuid,
        "ObjectId": lambda x: str(x),
        "now_tz": lambda: datetime(2026, 11, 20, 16, 0, tzinfo=CN_TZ),
        "sanitize_periodic_review_payload": lambda payload: dict(payload),
        "PeriodType": _PeriodType,
        "ReviewStatus": _ReviewStatus,
        "TradingStatistics": _TradingStatistics,
        "AIPeriodicReview": _AIPeriodicReview,
        "TradeSummaryItem": _TradeSummaryItem,
        "PeriodicReviewReport": _PeriodicReviewReport,
        "PeriodicReviewResponse": _PeriodicReviewResponse,
        "CreatePeriodicReviewRequest": _CreatePeriodicReviewRequest,
        "logger": _SilentLogger(),
    }
    exec(compile(module, str(REVIEW_SERVICE_PATH), "exec"), ns)
    return ns


def _load_review_router_runtime(build_ts_query_fn: Callable, license_service_obj: Any) -> dict:
    tree = ast.parse(REVIEW_ROUTER_PATH.read_text(encoding="utf-8"))
    wanted = {"create_trade_review", "get_reviewable_trades"}
    body = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted
    ]
    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)

    ns: Dict[str, Any] = {
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "datetime": datetime,
        "CN_TZ": CN_TZ,
        "parse_quote_time": parse_quote_time,
        "build_timestamp_range_query": build_ts_query_fn,
        "HTTPException": _StubHTTPException,
        "Depends": lambda *a, **k: None,
        "Query": lambda *a, **k: None,
        "get_current_user": None,
        "get_license_info": None,
        "LicenseInfo": Any,
        "CreateTradeReviewRequest": Any,
        "get_license_service": lambda: license_service_obj,
        "ok": lambda data=None, message="ok": {"success": True, "data": data, "message": message},
        "router": types.SimpleNamespace(
            get=lambda *a, **k: lambda fn: fn,
            post=lambda *a, **k: lambda fn: fn,
        ),
        "logger": _SilentLogger(),
    }
    exec(compile(module, str(REVIEW_ROUTER_PATH), "exec"), ns)
    return ns


# --------------------------------------------------------------------------- #
# 1. Dynamic T+1, CN ETF stamp-duty exemption, Net PnL, and BSE boundary tests
# --------------------------------------------------------------------------- #
class TestPaperDynamicT1AndCommission(unittest.IsolatedAsyncioTestCase):
    async def test_dynamic_t1_unfreezes_next_day_without_market_rules_and_locks_only_intraday_addon(self):
        day1 = datetime(2026, 11, 25, 14, 0, tzinfo=CN_TZ)
        day2 = datetime(2026, 11, 26, 10, 0, tzinfo=CN_TZ)
        day3 = datetime(2026, 11, 27, 10, 0, tzinfo=CN_TZ)
        clock = {"now": day2}

        # Note: paper_market_rules intentionally EMPTY -> CN must still enforce default t_plus=1!
        db = FakeMongoDB({
            "paper_accounts": [{
                "user_id": "u1",
                "cash": {"CNY": 1_000_000.0, "HKD": 1_000_000.0, "USD": 100_000.0},
                "realized_pnl": {"CNY": 0.0, "HKD": 0.0, "USD": 0.0},
            }],
            "paper_positions": [{
                "_id": "pos-1",
                "user_id": "u1",
                "code": "600519",
                "name": "贵州茅台",
                "market": "CN",
                "currency": "CNY",
                "quantity": 1000,
                "available_qty": 0,  # stale static DB value from yesterday's buy!
                "avg_cost": 10.0,
            }],
            "paper_trades": [{
                "user_id": "u1",
                "code": "600519",
                "market": "CN",
                "side": "buy",
                "quantity": 1000,
                "price": 10.0,
                "amount": 10000.0,
                "timestamp": day1.isoformat(),
            }],
            "stock_basic_info": [{"code": "600519", "name": "贵州茅台", "current_price": 12.0}],
        })
        ns = _load_paper_runtime(db, lambda: clock["now"])

        # Day 2 morning: yesterday's 1000 shares are dynamically unlocked on BOTH /positions & /account
        pos_resp = await ns["list_positions"]({"id": "u1"})
        acc_resp = await ns["get_account"]({"id": "u1"})
        self.assertEqual(pos_resp["data"]["items"][0]["available_qty"], 1000)
        self.assertEqual(acc_resp["data"]["positions"][0]["available_qty"], 1000)

        # Day 2 intraday add-on buy of 400 shares (mock strict quote = 12.0)
        live_quote = {
            "code": "600519", "close": 12.0, "high": 12.5, "low": 11.5,
            "trade_date": "2026-11-26", "quote_time": day2.isoformat(),
            "quote_time_source": "upstream", "fetched_at": day2.isoformat(),
            "data_source": "sina_l1", "trading_status": "normal",
            "quote_delivery": "on_demand", "max_quote_age_seconds": 90,
        }
        with patch.object(supply, "get_paper_quote", AsyncMock(return_value=(True, live_quote, "ok"))):
            buy_req = ns["PlaceOrderRequest"](code="600519", side="buy", quantity=400, market="CN")
            await ns["place_order"](buy_req, {"id": "u1"})

            # Total 1400, only today's 400 locked -> available_qty == 1000
            pos_after_buy = await ns["list_positions"]({"id": "u1"})
            self.assertEqual(pos_after_buy["data"]["items"][0]["quantity"], 1400)
            self.assertEqual(pos_after_buy["data"]["items"][0]["available_qty"], 1000)

            # Selling 1100 exceeds available 1000 -> rejected 400
            with self.assertRaises(_StubHTTPException) as cm:
                sell_too_much = ns["PlaceOrderRequest"](code="600519", side="sell", quantity=1100, market="CN")
                await ns["place_order"](sell_too_much, {"id": "u1"})
            self.assertEqual(cm.exception.status_code, 400)
            self.assertIn("可用 1000", str(cm.exception.detail))

            # Selling 600 succeeds -> remaining 800, available 400
            sell_ok = ns["PlaceOrderRequest"](code="600519", side="sell", quantity=600, market="CN")
            await ns["place_order"](sell_ok, {"id": "u1"})
            pos_after_sell = await ns["list_positions"]({"id": "u1"})
            self.assertEqual(pos_after_sell["data"]["items"][0]["quantity"], 800)
            self.assertEqual(pos_after_sell["data"]["items"][0]["available_qty"], 400)

        # Day 3: all remaining 800 shares automatically unlocked
        clock["now"] = day3
        pos_day3 = await ns["list_positions"]({"id": "u1"})
        self.assertEqual(pos_day3["data"]["items"][0]["available_qty"], 800)

    async def test_t1_uses_cn_tz_midnight_boundary_and_t0_for_hk_us(self):
        # 2026-11-26 00:05+08:00 == 2026-11-25 16:05 UTC (different UTC date, same CN date!)
        now_cn = datetime(2026, 11, 26, 0, 5, tzinfo=CN_TZ)
        before_cn_midnight = datetime(2026, 11, 25, 23, 55, tzinfo=CN_TZ)
        after_cn_midnight_utc = datetime(2026, 11, 25, 16, 2, tzinfo=timezone.utc)  # 2026-11-26 00:02+08:00

        db = FakeMongoDB({
            "paper_trades": [
                {"user_id": "u1", "code": "000001", "side": "buy", "quantity": 300, "timestamp": before_cn_midnight.isoformat()},
                {"user_id": "u1", "code": "000001", "side": "buy", "quantity": 200, "timestamp": after_cn_midnight_utc},
            ]
        })
        ns = _load_paper_runtime(db, lambda: now_cn)

        # Only the 200 bought after CN midnight (00:02 CST) is locked; 300 bought at 23:55 CST yesterday is unlocked!
        avail_cn = await ns["_compute_available_qty"]("u1", "000001", "CN", 500)
        self.assertEqual(avail_cn, 300)

        # HK/US are T+0 -> full qty available
        self.assertEqual(await ns["_compute_available_qty"]("u1", "00700", "HK", 500), 500)
        self.assertEqual(await ns["_compute_available_qty"]("u1", "AAPL", "US", 500), 500)

    async def test_cn_etf_stamp_duty_exempt_net_pnl_deducts_commission_and_bse_rejected(self):
        now = datetime(2026, 11, 26, 14, 0, tzinfo=CN_TZ)
        yesterday = datetime(2026, 11, 25, 14, 0, tzinfo=CN_TZ)
        cn_rules = {
            "market": "CN",
            "rules": {
                "t_plus": 1,
                "commission": {"rate": 0.0003, "min": 5.0, "stamp_duty_rate": 0.001},
            },
        }
        db = FakeMongoDB({
            "paper_market_rules": [cn_rules],
            "paper_accounts": [{
                "user_id": "u1",
                "cash": {"CNY": 100_000.0, "HKD": 0.0, "USD": 0.0},
                "realized_pnl": {"CNY": 0.0, "HKD": 0.0, "USD": 0.0},
            }],
            "paper_positions": [
                {
                    "_id": "p-etf", "user_id": "u1", "code": "588000", "name": "科创50ETF",
                    "market": "CN", "currency": "CNY", "quantity": 10000, "available_qty": 0, "avg_cost": 1.0,
                },
                {
                    "_id": "p-stk", "user_id": "u1", "code": "600519", "name": "贵州茅台",
                    "market": "CN", "currency": "CNY", "quantity": 1000, "available_qty": 0, "avg_cost": 10.0,
                },
            ],
            "paper_trades": [
                {"user_id": "u1", "code": "588000", "side": "buy", "quantity": 10000, "timestamp": yesterday.isoformat()},
                {"user_id": "u1", "code": "600519", "side": "buy", "quantity": 1000, "timestamp": yesterday.isoformat()},
            ],
        })
        ns = _load_paper_runtime(db, lambda: now)

        # 1. Verify all 8 CN ETF prefixes exempt from stamp duty; stocks still charged stamp duty
        rules_dict = cn_rules["rules"]
        for prefix in ("50", "51", "52", "56", "58", "15", "16", "18"):
            etf_code = prefix + "0001"
            comm = ns["_calculate_commission"]("CN", "sell", 100_000.0, rules_dict, code=etf_code)
            self.assertEqual(comm, 30.0, f"ETF {etf_code} should pay commission only (no stamp duty)")
        # Explicit market="ETF" also exempt
        self.assertEqual(ns["_calculate_commission"]("ETF", "sell", 100_000.0, rules_dict), 30.0)
        # Regular CN stock pays commission (30) + stamp duty (100) = 130
        self.assertEqual(ns["_calculate_commission"]("CN", "sell", 100_000.0, rules_dict, code="600519"), 130.0)

        # 2. Execute sell on 588000 ETF and 600519 stock; verify net PnL = gross_pnl - commission
        def _quote(code, price):
            return {
                "code": code, "close": price, "high": price + 0.5, "low": max(0.1, price - 0.5),
                "trade_date": "2026-11-26", "quote_time": now.isoformat(),
                "quote_time_source": "upstream", "fetched_at": now.isoformat(),
                "data_source": "sina_l1", "trading_status": "normal",
                "quote_delivery": "on_demand", "max_quote_age_seconds": 90,
            }

        with patch.object(supply, "get_paper_quote", AsyncMock(return_value=(True, _quote("588000", 1.2), "ok"))):
            etf_res = await ns["place_order"](
                ns["PlaceOrderRequest"](code="588000", side="sell", quantity=10000, market="ETF"),
                {"id": "u1"},
            )
        etf_order = etf_res["data"]["order"]
        # Notional = 12,000; commission = max(3.6, 5.0) = 5.0 (NO 12.0 stamp duty!)
        self.assertEqual(etf_order["commission"], 5.0)
        self.assertEqual(etf_order["gross_pnl"], 2000.0)
        self.assertEqual(etf_order["pnl"], 1995.0)

        with patch.object(supply, "get_paper_quote", AsyncMock(return_value=(True, _quote("600519", 12.0), "ok"))):
            stk_res = await ns["place_order"](
                ns["PlaceOrderRequest"](code="600519", side="sell", quantity=1000, market="CN"),
                {"id": "u1"},
            )
        stk_order = stk_res["data"]["order"]
        # Notional = 12,000; commission = max(3.6, 5.0) + stamp_duty 12.0 = 17.0
        self.assertEqual(stk_order["commission"], 17.0)
        self.assertEqual(stk_order["gross_pnl"], 2000.0)
        self.assertEqual(stk_order["pnl"], 1983.0)

        acc = await db["paper_accounts"].find_one({"user_id": "u1"})
        self.assertEqual(acc["realized_pnl"]["CNY"], round(1995.0 + 1983.0, 2))

        # 3. BSE 92/83/87/43 order boundary remains strictly rejected
        svc = supply.PaperQuoteSupply(
            clock=lambda: now,
            permit=lambda _n: ({
                "version": 2, "enabled": True, "source": "sina_l1",
                "scope": "paper_execution_sh_sz",
                "not_before": (now - timedelta(hours=1)).isoformat(),
                "expires_at": (now + timedelta(hours=2)).isoformat(),
                "renewal_of_sha256": "a" * 64,
            }, "ok"),
            calendar=AsyncMock(return_value=True),
            fetch=AsyncMock(side_effect=AssertionError("BSE must never trigger upstream fetch")),
        )
        with patch.object(supply, "get_paper_quote", svc.get):
            for bse_code in ("920363", "830799", "870001", "430001", "920363.BJ"):
                with self.assertRaises(_StubHTTPException) as cm:
                    await ns["place_order"](
                        ns["PlaceOrderRequest"](code=bse_code, side="buy", quantity=100, market="CN"),
                        {"id": "u1"},
                    )
                self.assertEqual(cm.exception.status_code, 400)
                self.assertIn("暂不支持该证券的盘中报价", str(cm.exception.detail))


# --------------------------------------------------------------------------- #
# 2. Review & statistics timestamp compatibility, LLM fallback, router fixes
# --------------------------------------------------------------------------- #
class TestReviewContractAndFallback(unittest.IsolatedAsyncioTestCase):
    async def test_timestamp_range_query_matches_iso_string_and_bson_datetime_including_end_date(self):
        svc_ns = _load_review_service_runtime()
        build_ts = svc_ns["build_timestamp_range_query"]
        q = build_ts("timestamp", "2026-11-01", "2026-11-10")
        self.assertIn("$or", q)
        self.assertEqual(q["$or"][0]["timestamp"]["$gte"], "2026-11-01T00:00:00")
        self.assertEqual(q["$or"][0]["timestamp"]["$lte"], "2026-11-10T23:59:59.999999")
        self.assertEqual(q["$or"][1]["timestamp"]["$gte"], datetime(2026, 11, 1, 0, 0, 0, tzinfo=CN_TZ))
        self.assertEqual(q["$or"][1]["timestamp"]["$lte"], datetime(2026, 11, 10, 23, 59, 59, 999999, tzinfo=CN_TZ))
        self.assertEqual(build_ts("timestamp", None, None), {})
        self.assertEqual(build_ts("timestamp", "   ", ""), {})

        # Type-strict FakeMongoDB proves BOTH ISO String and BSON datetime match (incl. end_date)
        docs = [
            {"id": "before_str", "user_id": "u1", "side": "sell", "pnl": 999, "commission": 1, "timestamp": "2026-10-31T23:59:59"},
            {"id": "in_str_end_day", "user_id": "u1", "side": "sell", "pnl": 200, "commission": 5, "timestamp": "2026-11-10T14:30:00"},
            {"id": "in_str_tz", "user_id": "u1", "side": "sell", "pnl": -50, "commission": 5, "timestamp": "2026-11-10T23:15:00+08:00"},
            {"id": "in_bson_utc", "user_id": "u1", "side": "sell", "pnl": 100, "commission": 5, "timestamp": datetime(2026, 11, 10, 15, 30, 0)},  # 23:30 CST
            {"id": "after_bson_utc", "user_id": "u1", "side": "sell", "pnl": -999, "commission": 1, "timestamp": datetime(2026, 11, 10, 16, 0, 1)},  # Nov 11 00:00:01 CST
        ]
        db = FakeMongoDB({"paper_trades": docs})

        # Verify old BSON-only query WOULD return 0 for ISO string docs
        iso_only_db = FakeMongoDB({"paper_trades": [docs[1]]})
        old_bson_hits = await iso_only_db["paper_trades"].find({
            "user_id": "u1",
            "timestamp": {"$gte": datetime(2026, 11, 1), "$lte": datetime(2026, 11, 10, 23, 59, 59)},
        }).to_list(None)
        self.assertEqual(old_bson_hits, [])

        # Verify get_trading_statistics hits all 3 in-range sells (incl. end_date ISO strings & BSON)
        service = svc_ns["TradeReviewService"]()
        service.db = db
        service.paper_trades_collection = "paper_trades"
        service.periodic_reviews_collection = "periodic_reviews"
        stats = await service.get_trading_statistics("u1", "2026-11-01", "2026-11-10")
        self.assertEqual(stats.total_trades, 3)
        self.assertEqual(stats.winning_trades, 2)
        self.assertEqual(stats.losing_trades, 1)
        self.assertEqual(stats.total_pnl, 250.0)
        self.assertEqual(stats.total_commission, 15.0)

    async def test_periodic_review_uses_configured_non_dashscope_llm_and_provider_fallback(self):
        svc_ns = _load_review_service_runtime()
        db = FakeMongoDB({
            "paper_trades": [
                {
                    "_id": "t1", "user_id": "u1", "code": "588000", "market": "CN",
                    "side": "buy", "quantity": 1000, "price": 1.0, "amount": 1000.0,
                    "commission": 5.0, "pnl": 0.0, "timestamp": "2026-11-05T10:00:00",
                },
                {
                    "_id": "t2", "user_id": "u1", "code": "588000", "market": "CN",
                    "side": "sell", "quantity": 1000, "price": 1.2, "amount": 1200.0,
                    "commission": 5.0, "pnl": 195.0, "timestamp": "2026-11-10T14:50:00",
                },
            ],
            "users": [{
                "_id": "u1",
                "preferences": {"default_model": "deepseek-chat"},
            }],
            "system_configs": [{
                "is_active": True,
                "version": 1,
                "default_model": "qwen-plus",
                "llm_configs": [
                    {"provider": "dashscope", "model_name": "qwen-plus", "enabled": True, "api_key": ""},
                    {"provider": "deepseek", "model_name": "deepseek-chat", "enabled": True, "api_key": "sk-test-deepseek-key"},
                ],
            }],
            "llm_providers": [
                {"name": "dashscope", "is_active": True, "api_key": "your_dashscope_key_here"},
                {"name": "deepseek", "is_active": True, "api_key": "sk-test-deepseek-key", "default_base_url": "https://api.deepseek.com"},
            ],
        })
        service = svc_ns["TradeReviewService"]()
        service.db = db
        service.paper_trades_collection = "paper_trades"
        service.periodic_reviews_collection = "periodic_reviews"

        captured_cfgs: List[dict] = []

        async def fake_invoke(prompt: str, llm_cfg: dict, *, return_response: bool = False):
            captured_cfgs.append(dict(llm_cfg))
            return '{"overall_score": 82, "summary": "纪律良好", "trading_style": "波段", "common_mistakes": [], "improvement_areas": ["提前记录计划"], "action_plan": ["复盘跟踪"]}'

        service._invoke_llm_text = fake_invoke

        req = svc_ns["CreatePeriodicReviewRequest"](
            period_type=svc_ns["PeriodType"].MONTH,
            start_date="2026-11-01",
            end_date="2026-11-10",
            source="paper",
        )
        clean_env = {
            "DASHSCOPE_API_KEY": "",
            "DEEPSEEK_API_KEY": "",
            "OPENAI_API_KEY": "",
            "GOOGLE_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "OPENROUTER_API_KEY": "",
            "SILICONFLOW_API_KEY": "",
            "VOLCENGINE_API_KEY": "",
            "ARK_API_KEY": "",
            "ZHIPU_API_KEY": "",
            "MOONSHOT_API_KEY": "",
        }
        with patch.dict("os.environ", clean_env, clear=False), \
             patch.dict(sys.modules, {"app.services.config_provider": None}):
            resp = await service.create_periodic_review("u1", req)

        self.assertEqual(resp.status, svc_ns["ReviewStatus"].COMPLETED)
        self.assertEqual(resp.statistics.total_trades, 1)
        self.assertEqual(resp.statistics.total_pnl, 195.0)
        self.assertEqual(resp.ai_review.summary, "纪律良好")
        self.assertEqual(len(captured_cfgs), 1)
        self.assertEqual(captured_cfgs[0]["provider"], "deepseek")
        self.assertEqual(captured_cfgs[0]["model_name"], "deepseek-chat")

        # Fallback test: even when user prefers DashScope (missing valid key) and an unconfigured
        # ollama row exists in llm_providers, falls back to active OpenAI provider with valid key!
        db_fallback = FakeMongoDB({
            "system_configs": [{
                "is_active": True,
                "version": 1,
                "default_model": "qwen-plus",
                "llm_configs": [],
            }],
            "llm_providers": [
                {"name": "dashscope", "is_active": True, "api_key": "your_key_here"},
                {"name": "ollama", "is_active": True, "api_key": ""},
                {"name": "openai", "is_active": True, "api_key": "sk-test-openai-key", "default_model": "gpt-4o-mini"},
            ],
        })
        service.db = db_fallback
        with patch.dict("os.environ", clean_env, clear=False), \
             patch.dict(sys.modules, {"app.services.config_provider": None}):
            resolved = await service._resolve_default_llm_config(user_id="u1")
        self.assertEqual(resolved["provider"], "openai")
        self.assertEqual(resolved["model_name"], "gpt-4o-mini")

    async def test_review_router_preserves_403_and_fixes_completion_status_and_etf_name(self):
        svc_ns = _load_review_service_runtime()
        license_svc = types.SimpleNamespace(has_feature=lambda info, feat: False)
        router_ns = _load_review_router_runtime(svc_ns["build_timestamp_range_query"], license_svc)

        # 1. create_trade_review must propagate 403 HTTPException (not swallow into 500)
        req = types.SimpleNamespace(
            code="600519", trade_ids=["t1"], review_type="complete_trade",
            trading_system_id="plan-1",
        )
        lic = types.SimpleNamespace(plan="free")
        with self.assertRaises(_StubHTTPException) as cm:
            await router_ns["create_trade_review"](req, {"id": "u1"}, lic)
        self.assertEqual(cm.exception.status_code, 403)
        self.assertEqual(cm.exception.detail["code"], "FEATURE_REQUIRED")

        # 2. get_reviewable_trades:
        #    - ISO String trades matched across start_date..end_date (incl. end_date)
        #    - 588000 bought 1000, sold 100 (1 buy, 1 sell, holding 900) -> status == "holding" & ETF name resolved!
        #    - 600519 bought 500 + 500, sold 1000 (position deleted) -> status == "completed"
        db = FakeMongoDB({
            "paper_trades": [
                {"_id": "t1", "user_id": "u1", "code": "588000", "market": "CN", "side": "buy", "quantity": 1000, "price": 1.0, "amount": 1000.0, "pnl": 0, "timestamp": "2026-11-05T10:00:00"},
                {"_id": "t2", "user_id": "u1", "code": "588000", "market": "CN", "side": "sell", "quantity": 100, "price": 1.1, "amount": 110.0, "pnl": 10.0, "timestamp": "2026-11-10T14:30:00"},
                {"_id": "t3", "user_id": "u1", "code": "600519", "market": "CN", "side": "buy", "quantity": 500, "price": 10.0, "amount": 5000.0, "pnl": 0, "timestamp": "2026-11-02T10:00:00"},
                {"_id": "t4", "user_id": "u1", "code": "600519", "market": "CN", "side": "buy", "quantity": 500, "price": 10.0, "amount": 5000.0, "pnl": 0, "timestamp": "2026-11-03T10:00:00"},
                {"_id": "t5", "user_id": "u1", "code": "600519", "market": "CN", "side": "sell", "quantity": 1000, "price": 11.0, "amount": 11000.0, "pnl": 1000.0, "timestamp": datetime(2026, 11, 10, 6, 30, 0)},
            ],
            "paper_positions": [
                {"user_id": "u1", "code": "588000", "quantity": 900},
            ],
            "stock_basic_info": [
                {"code": "600519", "name": "贵州茅台"},
            ],
            "etf_basic_info": [
                {"code": "588000", "name": "科创50ETF"},
            ],
        })
        fake_db_mod = types.ModuleType("app.core.database")
        fake_db_mod.get_mongo_db = lambda: db
        with patch.dict(sys.modules, {"app.core.database": fake_db_mod}):
            res = await router_ns["get_reviewable_trades"](
                code=None, start_date="2026-11-01", end_date="2026-11-10",
                page=1, page_size=20, current_user={"id": "u1"},
            )
        self.assertTrue(res["success"])
        data = res["data"]
        self.assertEqual(data["total"], 5)

        by_code = {item["code"]: item for item in data["all_stocks"]}
        # Buy 1000, sell 100 -> MUST stay "holding" (not "completed") and resolve ETF name
        self.assertEqual(by_code["588000"]["status"], "holding")
        self.assertEqual(by_code["588000"]["name"], "科创50ETF")
        self.assertEqual(by_code["588000"]["remaining_quantity"], 900)
        # Buy 500+500, sell 1000 with no remaining position -> "completed"
        self.assertEqual(by_code["600519"]["status"], "completed")
        self.assertEqual(by_code["600519"]["name"], "贵州茅台")
        self.assertEqual(by_code["600519"]["remaining_quantity"], 0)
        self.assertEqual([c["code"] for c in data["completed_stocks"]], ["600519"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
