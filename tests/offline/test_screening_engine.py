"""Offline unit tests for the multi-collection stock screening engine.

Pure Python standard-library unittest with an in-memory async FakeMongoDB supporting
find/sort/skip/limit/count_documents/aggregate across stock_basic_info,
stock_financial_periods, stock_technical_indicators, market_quotes, and
stock_daily_quotes.
"""
from __future__ import annotations

import ast
import asyncio
import math
from pathlib import Path
import re
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _resolve_field(doc: dict, expr):
    if isinstance(expr, str) and expr.startswith("$"):
        parts = expr[1:].split(".")
        cur = doc
        for part in parts:
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur
    if isinstance(expr, dict) and "$ifNull" in expr:
        for candidate in expr["$ifNull"]:
            val = _resolve_field(doc, candidate)
            if val is not None:
                return val
        return None
    return expr


def _val_equals(a, b) -> bool:
    if isinstance(a, float) and math.isnan(a) and isinstance(b, float) and math.isnan(b):
        return True
    return a == b


def _val_in(actual, container) -> bool:
    return any(_val_equals(actual, item) for item in container)


def _match_filter(doc: dict, filt: dict) -> bool:
    for key, expected in (filt or {}).items():
        if key == "$and":
            if not all(_match_filter(doc, c) for c in expected):
                return False
            continue
        if key == "$or":
            if not any(_match_filter(doc, c) for c in expected):
                return False
            continue
        actual = doc.get(key)
        if isinstance(expected, dict):
            options = expected.get("$options", "")
            flags = re.IGNORECASE if "i" in str(options) else 0
            for op, val in expected.items():
                if op == "$options":
                    continue
                if op == "$gt":
                    if actual is None or (isinstance(actual, float) and math.isnan(actual)) or actual <= val:
                        return False
                elif op == "$gte":
                    if actual is None or (isinstance(actual, float) and math.isnan(actual)) or actual < val:
                        return False
                elif op == "$lt":
                    if actual is None or (isinstance(actual, float) and math.isnan(actual)) or actual >= val:
                        return False
                elif op == "$lte":
                    if actual is None or (isinstance(actual, float) and math.isnan(actual)) or actual > val:
                        return False
                elif op == "$eq":
                    if not _val_equals(actual, val):
                        return False
                elif op == "$ne":
                    if _val_equals(actual, val):
                        return False
                elif op == "$in":
                    if not _val_in(actual, val):
                        return False
                elif op == "$nin":
                    if _val_in(actual, val):
                        return False
                elif op == "$exists":
                    if bool(val) != (key in doc):
                        return False
                elif op == "$regex":
                    if actual is None or re.search(val, str(actual), flags) is None:
                        return False
                elif op == "$not":
                    if isinstance(val, re.Pattern):
                        if actual is not None and val.search(str(actual)) is not None:
                            return False
                    elif _match_filter({key: actual}, {key: val}):
                        return False
                else:
                    raise NotImplementedError(f"Unsupported filter operator: {op}")
        else:
            if not _val_equals(actual, expected):
                return False
    return True


class _FakeAsyncCursor:
    def __init__(self, docs):
        self._docs = [dict(d) for d in docs]
        self._index = 0

    def sort(self, sort_spec, direction=None):
        if isinstance(sort_spec, str):
            pairs = [(sort_spec, direction or 1)]
        else:
            pairs = list(sort_spec)
        for field, d in reversed(pairs):
            reverse = d < 0
            # Match MongoDB default sort behavior (None before numbers in asc, after in desc)
            def _key(item, f=field, rev=reverse):
                val = item.get(f)
                if val is None:
                    return (0 if not rev else -1, 0.0, "")
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    return (1, float(val), "")
                return (2, 0.0, str(val))
            self._docs.sort(key=_key, reverse=reverse)
        return self

    def skip(self, n: int):
        self._docs = self._docs[n:]
        return self

    def limit(self, n: int):
        self._docs = self._docs[:n]
        return self

    async def to_list(self, length=None):
        if length is None:
            return list(self._docs)
        return list(self._docs[:length])

    def __aiter__(self):
        self._index = 0
        return self

    async def __anext__(self):
        if self._index >= len(self._docs):
            raise StopAsyncIteration
        item = self._docs[self._index]
        self._index += 1
        return item


class _FakeCollection:
    def __init__(self, docs=None, *, fail_with: Exception | None = None):
        self.docs = [dict(d) for d in (docs or [])]
        self.fail_with = fail_with

    def find(self, query=None, projection=None):
        if self.fail_with is not None:
            raise self.fail_with
        matched = [dict(d) for d in self.docs if _match_filter(d, query or {})]
        return _FakeAsyncCursor(matched)

    async def find_one(self, query=None, projection=None, sort=None):
        if self.fail_with is not None:
            raise self.fail_with
        cursor = self.find(query, projection)
        if sort:
            cursor = cursor.sort(sort)
        items = await cursor.to_list(length=1)
        return items[0] if items else None

    async def distinct(self, field: str):
        if self.fail_with is not None:
            raise self.fail_with
        seen = []
        for d in self.docs:
            val = d.get(field)
            if val is not None and val not in seen:
                seen.append(val)
        return seen

    async def count_documents(self, query=None):
        if self.fail_with is not None:
            raise self.fail_with
        return sum(1 for d in self.docs if _match_filter(d, query or {}))

    def aggregate(self, pipeline):
        if self.fail_with is not None:
            raise self.fail_with
        rows = [dict(d) for d in self.docs]
        for stage in pipeline:
            if "$match" in stage:
                rows = [r for r in rows if _match_filter(r, stage["$match"])]
            elif "$sort" in stage:
                pairs = list(stage["$sort"].items())
                for field, d in reversed(pairs):
                    reverse = d < 0
                    rows.sort(
                        key=lambda r, f=field: (r.get(f) is not None, r.get(f) or ""),
                        reverse=reverse,
                    )
            elif "$group" in stage:
                spec = stage["$group"]
                id_expr = spec["_id"]
                buckets = {}
                order = []
                for row in rows:
                    key = _resolve_field(row, id_expr)
                    if key not in buckets:
                        buckets[key] = []
                        order.append(key)
                    buckets[key].append(row)
                grouped = []
                for key in order:
                    group_rows = buckets[key]
                    out = {"_id": key}
                    for field, op_spec in spec.items():
                        if field == "_id":
                            continue
                        if "$first" in op_spec:
                            out[field] = _resolve_field(group_rows[0], op_spec["$first"])
                        elif "$push" in op_spec:
                            pushed = []
                            push_spec = op_spec["$push"]
                            for gr in group_rows:
                                if isinstance(push_spec, dict):
                                    pushed.append({k: _resolve_field(gr, v) for k, v in push_spec.items()})
                                else:
                                    pushed.append(_resolve_field(gr, push_spec))
                            out[field] = pushed
                        elif "$min" in op_spec:
                            vals = [_resolve_field(gr, op_spec["$min"]) for gr in group_rows]
                            vals = [v for v in vals if v is not None]
                            out[field] = min(vals) if vals else None
                        elif "$max" in op_spec:
                            vals = [_resolve_field(gr, op_spec["$max"]) for gr in group_rows]
                            vals = [v for v in vals if v is not None]
                            out[field] = max(vals) if vals else None
                        elif "$avg" in op_spec:
                            vals = [_resolve_field(gr, op_spec["$avg"]) for gr in group_rows]
                            vals = [v for v in vals if isinstance(v, (int, float))]
                            out[field] = (sum(vals) / len(vals)) if vals else None
                        elif "$sum" in op_spec:
                            if op_spec["$sum"] == 1:
                                out[field] = len(group_rows)
                            else:
                                vals = [_resolve_field(gr, op_spec["$sum"]) or 0 for gr in group_rows]
                                out[field] = sum(vals)
                    grouped.append(out)
                rows = grouped
            else:
                raise NotImplementedError(f"Unsupported aggregate stage: {stage}")
        return _FakeAsyncCursor(rows)


class _FakeMongoDB:
    def __init__(self, collections: dict[str, _FakeCollection]):
        self._collections = collections

    def __getitem__(self, name: str) -> _FakeCollection:
        return self._collections.setdefault(name, _FakeCollection())


def _load_database_screening_service(fake_db: _FakeMongoDB):
    """Load the actual DatabaseScreeningService class and its helpers with fake DB & lightweight deps."""
    from datetime import datetime, timedelta
    from typing import Iterable

    src = (ROOT / "app/services/database_screening_service.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    helper_nodes = [
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name.startswith("_")
    ]
    cls_node = next(
        n for n in tree.body
        if isinstance(n, ast.ClassDef) and n.name == "DatabaseScreeningService"
    )
    mod = ast.Module(body=[*helper_nodes, cls_node], type_ignores=[])
    ast.fix_missing_locations(mod)

    noop_logger = types.SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    ns = {
        "Any": object,
        "Dict": dict,
        "Iterable": Iterable,
        "List": list,
        "Optional": object,
        "Set": set,
        "Tuple": tuple,
        "datetime": datetime,
        "timedelta": timedelta,
        "math": math,
        "re": re,
        "logger": noop_logger,
        "get_mongo_db": lambda: fake_db,
        "_build_factor_diagnostics": lambda *a, **k: {},
        "_build_growth_factor_warnings": lambda *a, **k: {},
        "_build_quality_factor_warnings": lambda *a, **k: {},
        "_build_value_factor_warnings": lambda *a, **k: {},
        "_compute_cagr": lambda *a, **k: None,
        "_compute_yoy_growth": lambda *a, **k: None,
        "_estimate_altman_z_score": lambda *a, **k: None,
        "_estimate_beneish_m_score": lambda *a, **k: None,
        "_estimate_interest_coverage": lambda *a, **k: None,
        "_estimate_piotroski_f_score": lambda *a, **k: None,
        "_estimate_roic": lambda *a, **k: None,
    }
    exec(compile(mod, "app/services/database_screening_service.py", "exec"), ns)
    return ns["DatabaseScreeningService"]()


class ScreeningEngineTests(unittest.TestCase):
    def _priority_stub(self, preferred: str = "akshare"):
        mod = types.ModuleType("app.core.data_source_priority")
        mod.get_preferred_data_source_async = AsyncMock(return_value=preferred)
        return mod

    def test_multi_collection_intersection_and_same_field_merge(self):
        fake_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection([
                {"code": "600519", "name": "贵州茅台", "source": "akshare", "pe": 22.0, "total_mv": 2000.0},
                {"code": "000001", "name": "亏损负PE", "source": "akshare", "pe": -5.0, "total_mv": 500.0},
                {"code": "000002", "name": "高PE股", "source": "akshare", "pe": 45.0, "total_mv": 400.0},
                {"code": "000003", "name": "低ROE股", "source": "akshare", "pe": 18.0, "total_mv": 300.0},
                {"code": "000004", "name": "高RSI股", "source": "akshare", "pe": 15.0, "total_mv": 250.0},
                {"code": "000005", "name": "低涨幅股", "source": "akshare", "pe": 16.0, "total_mv": 220.0},
                {"code": "000006", "name": "*ST测试", "source": "akshare", "pe": 12.0, "total_mv": 100.0},
            ]),
            "stock_financial_periods": _FakeCollection([
                {"code": "600519", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 28.5, "revenue_yoy": 14.2},
                {"code": "600519", "source": "akshare", "report_period": "20250930", "ann_date": "20251030", "roe": 24.0},
                {"code": "000001", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 20.0},
                {"code": "000002", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 20.0},
                {"code": "000003", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 8.0},
                {"code": "000004", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 19.0},
                {"code": "000005", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 19.0},
                {"code": "000006", "source": "akshare", "report_period": "20260930", "ann_date": "20261030", "roe": 25.0},
            ]),
            "stock_technical_indicators": _FakeCollection([
                {"code": "600519", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 26.5, "ma20": 1520.0},
                {"code": "600519", "source": "akshare", "trade_date": "2026-11-20", "rsi14": 68.0, "ma20": 1500.0},
                {"code": "000001", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 25.0},
                {"code": "000002", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 25.0},
                {"code": "000003", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 25.0},
                {"code": "000004", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 62.0},
                {"code": "000005", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 24.0},
                {"code": "000006", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 22.0},
            ]),
            "market_quotes": _FakeCollection([
                {"code": "600519", "close": 1550.0, "pct_chg": 3.4, "amount": 500000.0},
                {"code": "000001", "close": 10.0, "pct_chg": 4.0},
                {"code": "000002", "close": 20.0, "pct_chg": 4.0},
                {"code": "000003", "close": 15.0, "pct_chg": 4.0},
                {"code": "000004", "close": 15.0, "pct_chg": 4.0},
                {"code": "000005", "close": 15.0, "pct_chg": 0.5},
                {"code": "000006", "close": 5.0, "pct_chg": 4.5},
            ]),
            "stock_daily_quotes": _FakeCollection([]),
        })

        svc = _load_database_screening_service(fake_db)
        conditions = [
            {"field": "pe", "operator": ">", "value": 0},
            {"field": "pe", "operator": "<", "value": 30},
            {"field": "roe", "operator": ">=", "value": 15},
            {"field": "rsi14", "operator": "<=", "value": 30},
            {"field": "pct_chg", "operator": ">", "value": 2},
            {"field": "is_st", "operator": "==", "value": False},
        ]

        # 1) 验证多集合条件分流与 _build_query 合并同字段多条件，不互相覆盖
        grouped = svc._classify_conditions(conditions)
        self.assertEqual(len(grouped["basic"]), 2)
        self.assertEqual(len(grouped["basic_post"]), 1)
        self.assertEqual(len(grouped["financial"]), 1)
        self.assertEqual(len(grouped["technical"]), 1)
        self.assertEqual(len(grouped["quote"]), 1)
        built = asyncio.run(svc._build_query(grouped["basic"]))
        self.assertEqual(built["pe"], {"$gt": 0, "$lt": 30})

        # 2) 验证多集合交集仅精确命中 600519（排除负PE、高PE、低ROE、高RSI、低涨幅与ST）
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            items, total = asyncio.run(svc.screen_stocks(conditions, limit=50, offset=0, source="akshare"))

        self.assertEqual(total, 1)
        self.assertEqual([it["code"] for it in items], ["600519"])
        self.assertEqual(items[0]["roe"], 28.5)
        self.assertEqual(items[0]["rsi14"], 26.5)
        self.assertEqual(items[0]["pct_chg"], 3.4)
        self.assertEqual(items[0]["revenue_yoy"], 14.2)

    def test_market_quotes_empty_falls_back_to_stock_daily_quotes_latest_bar(self):
        fake_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection([
                {"code": "300750", "name": "宁德时代", "source": "akshare", "pe": 20.0, "total_mv": 900.0},
                {"code": "002594", "name": "比亚迪", "source": "akshare", "pe": 25.0, "total_mv": 800.0},
            ]),
            "market_quotes": _FakeCollection([]),  # 大阪部署停写/空表
            "stock_daily_quotes": _FakeCollection([
                # 300750 旧日线涨幅 0.2%，最新日线涨幅 4.2%
                {"symbol": "300750", "period": "daily", "data_source": "akshare", "trade_date": "2026-11-24", "close": 200.0, "pct_chg": 0.2},
                {"symbol": "300750", "period": "daily", "data_source": "akshare", "trade_date": "2026-11-25", "close": 208.4, "pct_chg": 4.2, "amount": 880000.0},
                # 002594 缺失 pct_chg，由 close/pre_close 推算为 +3.0%
                {"symbol": "002594", "period": "daily", "data_source": "akshare", "trade_date": "2026-11-25", "close": 103.0, "pre_close": 100.0},
            ]),
        })

        svc = _load_database_screening_service(fake_db)
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            items, total = asyncio.run(
                svc.screen_stocks(
                    [{"field": "pct_chg", "operator": ">", "value": 2.5}],
                    limit=50,
                    offset=0,
                    source="akshare",
                )
            )
        self.assertEqual(total, 2)
        by_code = {it["code"]: it for it in items}
        self.assertAlmostEqual(by_code["300750"]["pct_chg"], 4.2)
        self.assertAlmostEqual(by_code["300750"]["close"], 208.4)
        self.assertAlmostEqual(by_code["002594"]["pct_chg"], 3.0)

    def test_over_2000_candidates_no_small_cap_truncation(self):
        basic_docs = []
        daily_docs = []
        # 构造 2,300 只股票：前 2,295 只高市值不满足涨幅，最后 5 只小盘股（排名 > 2000）满足 pct_chg > 5
        for idx in range(1, 2301):
            code = f"{idx:06d}"
            is_target = idx > 2295
            basic_docs.append({
                "code": code,
                "name": f"股票{idx}",
                "source": "akshare",
                "total_mv": float(10 + (idx - 2295)) if is_target else float(5000 - idx),
                "pe": 18.0,
            })
            daily_docs.append({
                "symbol": code,
                "period": "daily",
                "data_source": "akshare",
                "trade_date": "2026-11-25",
                "close": 20.0,
                "pct_chg": 7.5 if is_target else 0.5,
            })

        fake_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection(basic_docs),
            "market_quotes": _FakeCollection([]),
            "stock_daily_quotes": _FakeCollection(daily_docs),
        })

        svc = _load_database_screening_service(fake_db)
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            items, total = asyncio.run(
                svc.screen_stocks(
                    [{"field": "pct_chg", "operator": ">", "value": 5.0}],
                    limit=50,
                    offset=0,
                    order_by=[{"field": "total_mv", "direction": "desc"}],
                    source="akshare",
                )
            )
        self.assertEqual(total, 5)
        self.assertEqual(
            [it["code"] for it in items],
            ["002300", "002299", "002298", "002297", "002296"],
        )

    def test_sort_nulls_last_for_asc_and_desc(self):
        fake_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection([
                {"code": "000001", "name": "空PE", "source": "akshare", "pe": None, "total_mv": 100.0},
                {"code": "000002", "name": "中PE", "source": "akshare", "pe": 12.0, "total_mv": 200.0},
                {"code": "000003", "name": "低PE", "source": "akshare", "pe": 5.0, "total_mv": 300.0},
                {"code": "000004", "name": "NaNPE", "source": "akshare", "pe": float("nan"), "total_mv": 400.0},
                {"code": "000005", "name": "高PE", "source": "akshare", "pe": 20.0, "total_mv": 500.0},
            ]),
        })
        svc = _load_database_screening_service(fake_db)
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            asc_items, asc_total = asyncio.run(
                svc.screen_stocks([], order_by=[{"field": "pe", "direction": "asc"}], source="akshare")
            )
            desc_items, desc_total = asyncio.run(
                svc.screen_stocks([], order_by=[{"field": "pe", "direction": "desc"}], source="akshare")
            )

        self.assertEqual(asc_total, 5)
        self.assertEqual([it["code"] for it in asc_items[:3]], ["000003", "000002", "000005"])
        self.assertEqual(set(it["code"] for it in asc_items[3:]), {"000001", "000004"})

        self.assertEqual(desc_total, 5)
        self.assertEqual([it["code"] for it in desc_items[:3]], ["000005", "000002", "000003"])
        self.assertEqual(set(it["code"] for it in desc_items[3:]), {"000001", "000004"})

        # 同时验证跨集合排序路径（_sort_results_nulls_last）对技术指标 None/NaN 也严格 nulls last
        fake_db_tech = _FakeMongoDB({
            "stock_basic_info": _FakeCollection([
                {"code": "000001", "name": "空RSI", "source": "akshare", "total_mv": 100.0},
                {"code": "000002", "name": "高RSI", "source": "akshare", "total_mv": 200.0},
                {"code": "000003", "name": "低RSI", "source": "akshare", "total_mv": 300.0},
                {"code": "000004", "name": "NaNRSI", "source": "akshare", "total_mv": 400.0},
            ]),
            "stock_technical_indicators": _FakeCollection([
                {"code": "000002", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 60.0},
                {"code": "000003", "source": "akshare", "trade_date": "2026-11-25", "rsi14": 25.0},
                {"code": "000004", "source": "akshare", "trade_date": "2026-11-25", "rsi14": float("nan")},
            ]),
        })
        svc_tech = _load_database_screening_service(fake_db_tech)
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            tech_asc, tech_total = asyncio.run(
                svc_tech.screen_stocks([], order_by=[{"field": "rsi14", "direction": "asc"}], source="akshare")
            )
        self.assertEqual(tech_total, 4)
        self.assertEqual([it["code"] for it in tech_asc[:2]], ["000003", "000002"])
        self.assertEqual(set(it["code"] for it in tech_asc[2:]), {"000001", "000004"})

    def test_fast_path_uses_real_count_and_enriches_daily_quotes(self):
        basic_docs = [
            {"code": f"{idx:06d}", "name": f"股{idx}", "source": "akshare", "total_mv": float(idx)}
            for idx in range(1, 136)
        ]
        daily_docs = [
            {"symbol": f"{idx:06d}", "period": "daily", "data_source": "akshare", "trade_date": "2026-11-25", "close": 10.0 + idx, "pct_chg": 1.5}
            for idx in range(1, 136)
        ]
        fake_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection(basic_docs),
            "market_quotes": _FakeCollection([]),
            "stock_daily_quotes": _FakeCollection(daily_docs),
        })
        svc = _load_database_screening_service(fake_db)
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            items, total = asyncio.run(
                svc.screen_stocks([], limit=10, offset=0, order_by=[{"field": "total_mv", "direction": "desc"}], source="akshare")
            )
        # 必须是真实 count_documents (135)，而非估算 1010
        self.assertEqual(total, 135)
        self.assertEqual(len(items), 10)
        self.assertEqual(items[0]["code"], "000135")
        self.assertAlmostEqual(items[0]["close"], 145.0)
        self.assertAlmostEqual(items[0]["pct_chg"], 1.5)

    def test_get_field_statistics_routes_to_owning_collections(self):
        fake_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection([
                {"code": "600519", "pe": 20.0},
                {"code": "000001", "pe": 10.0},
            ]),
            "stock_financial_periods": _FakeCollection([
                {"code": "600519", "report_period": "20260930", "ann_date": "20261030", "roe": 30.0},
                {"code": "600519", "report_period": "20250930", "ann_date": "20251030", "roe": 10.0},
                {"code": "000001", "report_period": "20260930", "ann_date": "20261030", "roe": 10.0},
            ]),
            "stock_technical_indicators": _FakeCollection([
                {"code": "600519", "trade_date": "2026-11-25", "rsi14": 40.0},
                {"code": "000001", "trade_date": "2026-11-25", "rsi14": 20.0},
            ]),
            "market_quotes": _FakeCollection([]),
            "stock_daily_quotes": _FakeCollection([
                {"symbol": "600519", "period": "daily", "trade_date": "2026-11-25", "pct_chg": 4.0},
                {"symbol": "000001", "period": "daily", "trade_date": "2026-11-25", "pct_chg": -2.0},
            ]),
        })
        svc = _load_database_screening_service(fake_db)

        pe_stats = asyncio.run(svc.get_field_statistics("pe"))
        roe_stats = asyncio.run(svc.get_field_statistics("roe"))
        rsi_stats = asyncio.run(svc.get_field_statistics("rsi14"))
        pct_stats = asyncio.run(svc.get_field_statistics("pct_chg"))

        self.assertEqual((pe_stats["count"], pe_stats["min"], pe_stats["max"]), (2, 10.0, 20.0))
        self.assertEqual((roe_stats["count"], roe_stats["min"], roe_stats["max"]), (2, 10.0, 30.0))
        self.assertEqual((rsi_stats["count"], rsi_stats["min"], rsi_stats["max"]), (2, 20.0, 40.0))
        self.assertEqual((pct_stats["count"], pct_stats["min"], pct_stats["max"]), (2, -2.0, 4.0))

    def test_underlying_error_raises_structured_chinese_error_never_fake_zero(self):
        # 1) DatabaseScreeningService 底层异常明确抛出
        failing_db = _FakeMongoDB({
            "stock_basic_info": _FakeCollection(fail_with=RuntimeError("MongoDB timeout")),
        })
        db_svc = _load_database_screening_service(failing_db)
        with patch.dict(sys.modules, {"app.core.data_source_priority": self._priority_stub("akshare")}):
            with self.assertRaises(RuntimeError) as db_ctx:
                asyncio.run(db_svc.screen_stocks([{"field": "pe", "operator": ">", "value": 0}], source="akshare"))
        self.assertIn("MongoDB timeout", str(db_ctx.exception))

        # 2) EnhancedScreeningService 不再吞错返回 total=0，而是抛出 ScreeningExecutionError
        enh_tree = ast.parse((ROOT / "app/services/enhanced_screening_service.py").read_text(encoding="utf-8"))
        err_cls = next(n for n in enh_tree.body if isinstance(n, ast.ClassDef) and n.name == "ScreeningExecutionError")
        enh_cls = next(n for n in enh_tree.body if isinstance(n, ast.ClassDef) and n.name == "EnhancedScreeningService")
        enh_cls.body = [
            n for n in enh_cls.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            and n.name in ("screen_stocks", "_screen_with_database", "_analyze_conditions")
        ]
        enh_mod = ast.Module(body=[err_cls, enh_cls], type_ignores=[])
        ast.fix_missing_locations(enh_mod)
        enh_ns = {
            "Any": object,
            "Dict": dict,
            "List": list,
            "Optional": object,
            "Tuple": tuple,
            "ScreeningCondition": object,
            "time": __import__("time"),
            "logger": types.SimpleNamespace(
                info=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
            "_analyze_conditions_util": lambda conds: {"can_use_database": True},
        }
        exec(compile(enh_mod, "app/services/enhanced_screening_service.py", "exec"), enh_ns)
        ScreeningExecutionError = enh_ns["ScreeningExecutionError"]
        enh_svc = enh_ns["EnhancedScreeningService"]()
        enh_svc.db_service = types.SimpleNamespace(
            can_handle_conditions=AsyncMock(return_value=True),
            screen_stocks=AsyncMock(side_effect=RuntimeError("MongoDB timeout")),
        )
        with self.assertRaises(ScreeningExecutionError) as enh_ctx:
            asyncio.run(enh_svc.screen_stocks([]))
        self.assertIn("选股筛选查询失败", str(enh_ctx.exception))

        # 3) app/routers/screening.py 的 run_screening / enhanced_screening 透出 503 结构化中文错误
        router_tree = ast.parse((ROOT / "app/routers/screening.py").read_text(encoding="utf-8"))
        run_fn = next(n for n in router_tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "run_screening")
        enh_fn = next(n for n in router_tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "enhanced_screening")
        run_fn.decorator_list = []
        enh_fn.decorator_list = []
        r_mod = ast.Module(body=[run_fn, enh_fn], type_ignores=[])
        ast.fix_missing_locations(r_mod)

        class _FakeHTTPException(Exception):
            def __init__(self, status_code: int, detail):
                super().__init__(str(detail))
                self.status_code = status_code
                self.detail = detail

        r_ns = {
            "HTTPException": _FakeHTTPException,
            "Depends": lambda x: x,
            "get_current_user": lambda: {"id": "u1"},
            "ScreeningRequest": object,
            "ScreeningResponse": lambda **k: k,
            "NewScreeningRequest": object,
            "NewScreeningResponse": lambda **k: k,
            "ScreeningExecutionError": ScreeningExecutionError,
            "_convert_legacy_conditions_to_new_format": lambda c: [],
            "enhanced_svc": types.SimpleNamespace(
                screen_stocks=AsyncMock(side_effect=ScreeningExecutionError("选股筛选查询失败：MongoDB timeout"))
            ),
            "logger": types.SimpleNamespace(
                info=lambda *a, **k: None,
                error=lambda *a, **k: None,
            ),
        }
        exec(compile(r_mod, "app/routers/screening.py", "exec"), r_ns)

        req_legacy = types.SimpleNamespace(
            conditions={}, market="CN", date=None, adj="qfq", limit=50, offset=0, order_by=[]
        )
        with self.assertRaises(_FakeHTTPException) as run_exc:
            asyncio.run(r_ns["run_screening"](req_legacy, {"id": "u1"}))
        self.assertEqual(run_exc.exception.status_code, 503)
        self.assertEqual(run_exc.exception.detail["code"], "SCREENING_QUERY_FAILED")
        self.assertIn("选股筛选", run_exc.exception.detail["message"])

        req_enh = types.SimpleNamespace(
            conditions=[], market="CN", date=None, adj="qfq", limit=50, offset=0, order_by=[], use_database_optimization=True
        )
        with self.assertRaises(_FakeHTTPException) as enh_exc:
            asyncio.run(r_ns["enhanced_screening"](req_enh, {"id": "u1"}))
        self.assertEqual(enh_exc.exception.status_code, 503)
        self.assertEqual(enh_exc.exception.detail["code"], "SCREENING_QUERY_FAILED")
        self.assertIn("选股筛选", enh_exc.exception.detail["message"])


if __name__ == "__main__":
    unittest.main()
