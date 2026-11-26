"""
基于MongoDB的股票筛选服务
支持跨集合条件分流（stock_basic_info / stock_financial_periods /
stock_technical_indicators / market_quotes + stock_daily_quotes 日线回退）、
同字段多条件合并、精确候选集交集与 nulls last 排序。
"""

import logging
import math
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from app.core.database import get_mongo_db
from core.skill_runtime.fundamental_factors import (
    _build_factor_diagnostics,
    _build_growth_factor_warnings,
    _build_quality_factor_warnings,
    _build_value_factor_warnings,
    _compute_cagr,
    _compute_yoy_growth,
    _estimate_altman_z_score,
    _estimate_beneish_m_score,
    _estimate_interest_coverage,
    _estimate_piotroski_f_score,
    _estimate_roic,
)

logger = logging.getLogger(__name__)


def _normalize_code(code: Any) -> str:
    value = str(code or "").strip()
    if not value:
        return ""
    if "." in value:
        value = value.split(".", 1)[0]
    return value.zfill(6) if value.isdigit() else value


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return True
    return False


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not _is_missing_value(value):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "y", "是", "st"):
            return True
        if lowered in ("false", "0", "no", "n", "否", "非st"):
            return False
    return None


def _resolve_is_st(doc: Dict[str, Any]) -> bool:
    coerced = _coerce_bool(doc.get("is_st"))
    if coerced is not None:
        return coerced
    name = str(doc.get("name") or "").upper()
    return "ST" in name


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
        if math.isnan(numeric) or math.isinf(numeric):
            return None
        return numeric
    except (TypeError, ValueError):
        return None


def _normalize_sort_value(value: Any) -> Tuple[int, Any]:
    """统一排序键，避免混合类型比较触发 TypeError。"""
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    return (1, str(value))


class DatabaseScreeningService:
    """基于数据库的多集合股票筛选服务"""

    QUOTE_FIELDS = frozenset({"close", "pct_chg", "amount", "volume"})
    TECHNICAL_FIELDS = frozenset({
        "ma20", "rsi14", "kdj_k", "kdj_d", "kdj_j", "dif", "dea", "macd_hist",
    })
    DIRECT_FINANCIAL_FIELDS = frozenset({
        "roe", "roa", "gross_margin", "netprofit_margin",
        "revenue_ttm", "net_profit_ttm", "n_cashflow_act",
        "debt_to_assets", "assets_to_eqt", "current_ratio", "quick_ratio",
        "cash_ratio", "dividend_yield", "report_period",
        "revenue_yoy", "net_profit_yoy", "oper_profit_yoy",
    })
    DERIVED_FINANCIAL_FIELDS = frozenset({
        "pe_ttm", "pb_mrq", "peg", "roic",
        "cash_conversion", "accrual_ratio", "asset_turnover",
        "gross_profit_to_assets", "interest_coverage",
        "inventory_turnover", "receivable_turnover", "net_cash_position",
        "fcf", "fcf_yield", "fcf_margin", "ocf_yield",
        "revenue_cagr_3y", "profit_cagr_3y",
        "working_capital_change", "net_working_capital",
        "piotroski_f_score", "altman_z_score", "beneish_m_score",
    })
    FINANCIAL_FIELDS = DIRECT_FINANCIAL_FIELDS | DERIVED_FINANCIAL_FIELDS
    BASIC_POST_FIELDS = frozenset({"is_st"})
    BASIC_DIRECT_FIELDS = frozenset({
        "code", "symbol", "name", "industry", "area", "market", "list_date",
        "total_mv", "circ_mv", "market_cap",
        "pe", "pb", "ps_ttm", "turnover_rate", "volume_ratio",
        "keyword",
    })

    FIELD_ALIASES = {
        "market_cap": "total_mv",
        "symbol": "code",
        "pe_ratio": "pe",
        "pb_ratio": "pb",
        "turnover": "turnover_rate",
        "change_percent": "pct_chg",
        "price": "close",
    }

    def __init__(self):
        self.collection_name = "stock_basic_info"

        # 支持的筛选字段映射（统一对外兼容全部基础、财务、技术与行情因子）
        self.basic_fields = {
            # 基本信息
            "code": "code",
            "symbol": "code",
            "name": "name",
            "industry": "industry",
            "area": "area",
            "market": "market",
            "list_date": "list_date",

            # 市值信息 (亿元)
            "total_mv": "total_mv",
            "circ_mv": "circ_mv",
            "market_cap": "total_mv",

            # 估值与财务指标
            "pe": "pe",
            "pb": "pb",
            "pe_ratio": "pe",
            "pb_ratio": "pb",
            "pe_ttm": "pe_ttm",
            "ps_ttm": "ps_ttm",
            "pb_mrq": "pb_mrq",
            "roe": "roe",
            "roa": "roa",
            "gross_margin": "gross_margin",
            "netprofit_margin": "netprofit_margin",
            "revenue_ttm": "revenue_ttm",
            "net_profit_ttm": "net_profit_ttm",
            "n_cashflow_act": "n_cashflow_act",
            "revenue_yoy": "revenue_yoy",
            "net_profit_yoy": "net_profit_yoy",
            "oper_profit_yoy": "oper_profit_yoy",
            "peg": "peg",
            "roic": "roic",
            "cash_conversion": "cash_conversion",
            "accrual_ratio": "accrual_ratio",
            "asset_turnover": "asset_turnover",
            "gross_profit_to_assets": "gross_profit_to_assets",
            "interest_coverage": "interest_coverage",
            "inventory_turnover": "inventory_turnover",
            "receivable_turnover": "receivable_turnover",
            "net_cash_position": "net_cash_position",
            "piotroski_f_score": "piotroski_f_score",
            "altman_z_score": "altman_z_score",
            "beneish_m_score": "beneish_m_score",
            "fcf": "fcf",
            "fcf_yield": "fcf_yield",
            "fcf_margin": "fcf_margin",
            "ocf_yield": "ocf_yield",
            "revenue_cagr_3y": "revenue_cagr_3y",
            "profit_cagr_3y": "profit_cagr_3y",
            "working_capital_change": "working_capital_change",

            # 交易指标
            "turnover_rate": "turnover_rate",
            "turnover": "turnover_rate",
            "volume_ratio": "volume_ratio",

            # 技术指标（stock_technical_indicators）
            "ma20": "ma20",
            "rsi14": "rsi14",
            "kdj_k": "kdj_k",
            "kdj_d": "kdj_d",
            "kdj_j": "kdj_j",
            "dif": "dif",
            "dea": "dea",
            "macd_hist": "macd_hist",

            # 扩展财务指标
            "debt_to_assets": "debt_to_assets",
            "assets_to_eqt": "assets_to_eqt",
            "current_ratio": "current_ratio",
            "quick_ratio": "quick_ratio",
            "cash_ratio": "cash_ratio",
            "dividend_yield": "dividend_yield",
            "net_working_capital": "net_working_capital",
            "is_st": "is_st",
            "report_period": "report_period",

            # 搜索关键词（虚拟字段）
            "keyword": "keyword",

            # 行情字段（market_quotes 缺失/空时回退 stock_daily_quotes 最新日线）
            "pct_chg": "pct_chg",
            "change_percent": "pct_chg",
            "amount": "amount",
            "close": "close",
            "price": "close",
            "volume": "volume",
        }

        # 支持的操作符
        self.operators = {
            ">": "$gt",
            "<": "$lt",
            ">=": "$gte",
            "<=": "$lte",
            "==": "$eq",
            "!=": "$ne",
            "between": "$between",
            "in": "$in",
            "not_in": "$nin",
            "contains": "$regex",
        }

    def _resolve_field_name(self, field: Optional[str]) -> Optional[str]:
        if not field:
            return None
        return self.FIELD_ALIASES.get(field, self.basic_fields.get(field))

    def _get_field_group(self, field: str) -> str:
        canonical = self._resolve_field_name(field) or field
        if canonical in self.QUOTE_FIELDS:
            return "quote"
        if canonical in self.TECHNICAL_FIELDS:
            return "technical"
        if canonical in self.FINANCIAL_FIELDS:
            return "financial"
        if canonical in self.BASIC_POST_FIELDS:
            return "basic_post"
        return "basic"

    @staticmethod
    def _extract_condition_parts(condition: Any) -> Tuple[str, str, Any]:
        if isinstance(condition, dict):
            return (
                condition.get("field"),
                condition.get("operator") or condition.get("op"),
                condition.get("value"),
            )
        return (condition.field, condition.operator, condition.value)

    async def can_handle_conditions(self, conditions: List[Dict[str, Any]]) -> bool:
        """检查是否可以完全通过数据库筛选处理这些条件"""
        for condition in conditions:
            field, operator, _ = self._extract_condition_parts(condition)
            if field not in self.basic_fields:
                logger.debug(f"字段 {field} 不支持数据库筛选")
                return False
            if operator not in self.operators:
                logger.debug(f"操作符 {operator} 不支持数据库筛选")
                return False
        return True

    def _classify_conditions(self, conditions: List[Any]) -> Dict[str, List[Any]]:
        """将条件分流至 basic / basic_post / financial / technical / quote 五组。"""
        groups: Dict[str, List[Any]] = {
            "basic": [],
            "basic_post": [],
            "financial": [],
            "technical": [],
            "quote": [],
        }
        for condition in conditions or []:
            field, _, _ = self._extract_condition_parts(condition)
            group = self._get_field_group(field)
            groups[group].append(condition)
        return groups

    def _separate_conditions(self, conditions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """向后兼容：分离非行情条件与行情条件。"""
        non_quote = []
        quote = []
        for condition in conditions or []:
            field, _, _ = self._extract_condition_parts(condition)
            if self._get_field_group(field) == "quote":
                quote.append(condition)
            else:
                non_quote.append(condition)
        return non_quote, quote

    @staticmethod
    def _merge_field_condition(
        query: Dict[str, Any],
        and_clauses: List[Dict[str, Any]],
        db_field: str,
        condition_expr: Dict[str, Any],
    ) -> None:
        """合并同字段多条件，避免后者覆盖前者（如 pe > 0 与 pe < 30）。"""
        if db_field not in query:
            query[db_field] = dict(condition_expr)
            return
        existing = query[db_field]
        if isinstance(existing, dict) and not (set(existing.keys()) & set(condition_expr.keys())):
            existing.update(condition_expr)
        else:
            and_clauses.append({db_field: dict(condition_expr)})

    async def _build_query(self, conditions: List[Any]) -> Dict[str, Any]:
        """构建 MongoDB 查询条件，合并同字段多条件且不互相覆盖。"""
        query: Dict[str, Any] = {}
        and_clauses: List[Dict[str, Any]] = []

        for condition in conditions or []:
            field, operator, value = self._extract_condition_parts(condition)
            logger.debug(f"🔍 [_build_query] 处理条件: field={field}, operator={operator}, value={value}")

            if field == "keyword":
                escaped = re.escape(str(value))
                keyword_or = {
                    "$or": [
                        {"code": {"$regex": escaped, "$options": "i"}},
                        {"name": {"$regex": escaped, "$options": "i"}},
                        {"symbol": {"$regex": escaped, "$options": "i"}},
                    ]
                }
                if "$or" not in query:
                    query["$or"] = keyword_or["$or"]
                else:
                    and_clauses.append(keyword_or)
                continue

            db_field = self._resolve_field_name(field)
            if not db_field:
                continue

            if operator == "between":
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    self._merge_field_condition(
                        query,
                        and_clauses,
                        db_field,
                        {"$gte": value[0], "$lte": value[1]},
                    )
            elif operator == "contains":
                self._merge_field_condition(
                    query,
                    and_clauses,
                    db_field,
                    {"$regex": re.escape(str(value)), "$options": "i"},
                )
            elif operator in self.operators:
                mongo_op = self.operators[operator]
                self._merge_field_condition(query, and_clauses, db_field, {mongo_op: value})

        if and_clauses:
            query["$and"] = and_clauses

        return query

    def _build_sort_conditions(self, order_by: Optional[List[Dict[str, str]]]) -> List[Tuple[str, int]]:
        """构建排序条件（兼容旧字段别名）"""
        if not order_by:
            return [("total_mv", -1)]

        sort_conditions: List[Tuple[str, int]] = []
        for order in order_by:
            field = order.get("field") if isinstance(order, dict) else getattr(order, "field", None)
            direction = order.get("direction", "desc") if isinstance(order, dict) else getattr(order, "direction", "desc")
            db_field = self._resolve_field_name(field)
            if not db_field:
                continue
            sort_direction = -1 if str(direction or "desc").lower() == "desc" else 1
            sort_conditions.append((db_field, sort_direction))

        return sort_conditions or [("total_mv", -1)]

    def _sort_results_nulls_last(
        self,
        results: List[Dict[str, Any]],
        order_by: Optional[List[Dict[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        """多键排序并严格保证缺失/None/NaN 值排在末尾（nulls last）。"""
        if len(results) <= 1:
            return list(results)

        sort_specs = self._build_sort_conditions(order_by)
        ordered = list(results)
        for db_field, direction in reversed(sort_specs):
            is_desc = direction < 0
            non_null = []
            null_items = []
            for item in ordered:
                val = item.get(db_field)
                if _is_missing_value(val):
                    null_items.append(item)
                else:
                    non_null.append(item)
            non_null.sort(key=lambda x: _normalize_sort_value(x.get(db_field)), reverse=is_desc)
            ordered = non_null + null_items
        return ordered

    def _matches_condition(self, item: Dict[str, Any], condition: Any) -> bool:
        field, operator, target = self._extract_condition_parts(condition)
        canonical = self._resolve_field_name(field) or field

        if canonical == "is_st":
            actual_bool = _resolve_is_st(item)
            target_bool = _coerce_bool(target)
            if target_bool is None:
                return False
            if operator == "==":
                return actual_bool == target_bool
            if operator == "!=":
                return actual_bool != target_bool
            return False

        if canonical == "keyword":
            needle = str(target or "").lower()
            haystacks = (
                str(item.get("code") or "").lower(),
                str(item.get("name") or "").lower(),
                str(item.get("symbol") or "").lower(),
            )
            return any(needle in h for h in haystacks)

        actual = item.get(canonical)
        if _is_missing_value(actual):
            return False

        if operator in (">", "<", ">=", "<=", "between"):
            num_actual = _to_float_or_none(actual)
            if num_actual is None:
                return False
            if operator == "between":
                if not isinstance(target, (list, tuple)) or len(target) != 2:
                    return False
                low = _to_float_or_none(target[0])
                high = _to_float_or_none(target[1])
                if low is None or high is None:
                    return False
                return low <= num_actual <= high
            num_target = _to_float_or_none(target)
            if num_target is None:
                return False
            if operator == ">":
                return num_actual > num_target
            if operator == "<":
                return num_actual < num_target
            if operator == ">=":
                return num_actual >= num_target
            if operator == "<=":
                return num_actual <= num_target

        if operator == "==":
            num_actual = _to_float_or_none(actual)
            num_target = _to_float_or_none(target)
            if num_actual is not None and num_target is not None:
                return num_actual == num_target
            return str(actual) == str(target)

        if operator == "!=":
            num_actual = _to_float_or_none(actual)
            num_target = _to_float_or_none(target)
            if num_actual is not None and num_target is not None:
                return num_actual != num_target
            return str(actual) != str(target)

        if operator == "in":
            if not isinstance(target, (list, tuple, set)):
                return False
            return actual in target or str(actual) in {str(v) for v in target}

        if operator == "not_in":
            if not isinstance(target, (list, tuple, set)):
                return True
            return actual not in target and str(actual) not in {str(v) for v in target}

        if operator == "contains":
            return str(target or "").lower() in str(actual).lower()

        return False

    def _matches_all_conditions(self, item: Dict[str, Any], conditions: List[Any]) -> bool:
        return all(self._matches_condition(item, cond) for cond in conditions)

    async def screen_stocks(
        self,
        conditions: List[Dict[str, Any]],
        limit: int = 50,
        offset: int = 0,
        order_by: Optional[List[Dict[str, str]]] = None,
        source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        基于数据库的多集合交集股票筛选。

        分流策略：
        - basic: stock_basic_info
        - financial: stock_financial_periods（最新报告期 + 派生指标）
        - technical: stock_technical_indicators（最新交易日）
        - quote: market_quotes（缺失/空表时批量回退 stock_daily_quotes 最新日线）
        """
        try:
            db = get_mongo_db()

            if not source:
                from app.core.data_source_priority import get_preferred_data_source_async
                source = await get_preferred_data_source_async(market_category="a_shares")
                logger.info(f"✅ [database_screening] 最终使用的数据源: {source}")

            grouped = self._classify_conditions(conditions)
            basic_conditions = grouped["basic"]
            basic_post_conditions = grouped["basic_post"]
            financial_conditions = grouped["financial"]
            technical_conditions = grouped["technical"]
            quote_conditions = grouped["quote"]

            sort_conditions = self._build_sort_conditions(order_by)
            sort_groups = {self._get_field_group(field) for field, _ in sort_conditions}
            sort_is_basic_only = sort_groups.issubset({"basic"})

            has_any_conditions = any(grouped.values())
            has_cross_collection_conditions = bool(
                basic_post_conditions or financial_conditions or technical_conditions or quote_conditions
            )

            logger.info(
                "📋 [database_screening] 条件分流: basic=%s, basic_post=%s, financial=%s, technical=%s, quote=%s, sort=%s",
                len(basic_conditions),
                len(basic_post_conditions),
                len(financial_conditions),
                len(technical_conditions),
                len(quote_conditions),
                sort_conditions,
            )

            # 快速路径：无筛选条件且仅按基础字段排序 → 真实 count + nulls last 分页 + 全量展示富集
            if not has_any_conditions and sort_is_basic_only:
                logger.info("⚡ 使用快速路径：直接查询 stock_basic_info（含真实计数与行情回退富集）")
                results, total_count = await self._query_basic_info_directly(
                    db, source, sort_conditions, limit, offset
                )
                return results, total_count

            # 仅含基础直接条件且排序也在基础字段内 → 数据库分页 + 真实计数 + 仅对当前页做富集
            if not has_cross_collection_conditions and sort_is_basic_only:
                basic_query = await self._build_query(basic_conditions)
                basic_query["source"] = source
                results, total_count = await self._query_basic_collection_nulls_last(
                    db, basic_query, sort_conditions, limit, offset
                )
                codes = [r.get("code") for r in results if r.get("code")]
                if codes:
                    await self._enrich_with_financial_data(results, codes, preferred_source=source)
                    await self._enrich_with_quote_data(results, codes, preferred_source=source)
                    await self._enrich_with_technical_data(results, codes, preferred_source=source)
                return results, total_count

            # ── 多集合候选集交集引擎（无 2000 只截断）──────────────────────────
            # 1) basic 集合候选池（全量读取符合基础条件的候选，消除 2000 只截断）
            basic_query = await self._build_query(basic_conditions)
            basic_query["source"] = source

            basic_collection = db["stock_basic_info"]
            basic_docs_by_code: Dict[str, Dict[str, Any]] = {}
            async for doc in basic_collection.find(basic_query):
                formatted = self._format_result(doc)
                code = _normalize_code(formatted.get("code"))
                if not code:
                    continue
                formatted["code"] = code
                if basic_post_conditions and not self._matches_all_conditions(formatted, basic_post_conditions):
                    continue
                basic_docs_by_code[code] = formatted

            candidate_codes: Set[str] = set(basic_docs_by_code.keys())
            if not candidate_codes:
                return [], 0

            tech_map: Dict[str, Dict[str, Any]] = {}
            quote_map: Dict[str, Dict[str, Any]] = {}
            financial_enriched = False
            technical_enriched = False
            quote_enriched = False

            # 2) technical 集合候选交集（stock_technical_indicators 最新日期）
            if technical_conditions:
                tech_map = await self._load_technical_map(
                    db, list(candidate_codes), preferred_source=source, raise_on_error=True
                )
                technical_enriched = True
                tech_matched: Set[str] = set()
                for code in candidate_codes:
                    td = tech_map.get(code)
                    if td and self._matches_all_conditions(td, technical_conditions):
                        tech_matched.add(code)
                candidate_codes &= tech_matched
                if not candidate_codes:
                    return [], 0

            # 3) quote 集合候选交集（market_quotes 缺失或空时批量回退 stock_daily_quotes 最新日线）
            if quote_conditions:
                quote_map = await self._load_quote_map(
                    db, list(candidate_codes), preferred_source=source, raise_on_error=True
                )
                quote_enriched = True
                quote_matched: Set[str] = set()
                for code in candidate_codes:
                    qd = quote_map.get(code)
                    if qd and self._matches_all_conditions(qd, quote_conditions):
                        quote_matched.add(code)
                candidate_codes &= quote_matched
                if not candidate_codes:
                    return [], 0

            # 组装剩余候选列表
            candidates = [basic_docs_by_code[code] for code in candidate_codes]
            if technical_enriched:
                self._merge_technical_into_results(candidates, tech_map)
            if quote_enriched:
                self._merge_quotes_into_results(candidates, quote_map)

            # 4) financial 集合候选交集（stock_financial_periods 最新报告期 + 派生因子）
            if financial_conditions:
                cand_codes = [item["code"] for item in candidates]
                await self._enrich_with_financial_data(
                    candidates, cand_codes, preferred_source=source, raise_on_error=True
                )
                financial_enriched = True
                candidates = [
                    item for item in candidates
                    if self._matches_all_conditions(item, financial_conditions)
                ]
                if not candidates:
                    return [], 0

            # 精确总数
            total_count = len(candidates)

            # 若排序字段涉及尚未加载的集合，在排序前对候选集补充该集合字段
            remaining_codes = [item["code"] for item in candidates]
            if "technical" in sort_groups and not technical_enriched:
                tech_map = await self._load_technical_map(
                    db, remaining_codes, preferred_source=source, raise_on_error=False
                )
                self._merge_technical_into_results(candidates, tech_map)
                technical_enriched = True
            if "quote" in sort_groups and not quote_enriched:
                quote_map = await self._load_quote_map(
                    db, remaining_codes, preferred_source=source, raise_on_error=False
                )
                self._merge_quotes_into_results(candidates, quote_map)
                quote_enriched = True
            if "financial" in sort_groups and not financial_enriched:
                await self._enrich_with_financial_data(
                    candidates, remaining_codes, preferred_source=source, raise_on_error=False
                )
                financial_enriched = True

            # 统一 nulls last 排序并分页
            candidates = self._sort_results_nulls_last(candidates, order_by)
            page_results = candidates[offset:offset + limit]

            # 对当前页补齐展示所需字段（仅限 <= limit 条）
            page_codes = [item["code"] for item in page_results if item.get("code")]
            if page_codes:
                if not financial_enriched:
                    await self._enrich_with_financial_data(
                        page_results, page_codes, preferred_source=source, raise_on_error=False
                    )
                if not quote_enriched:
                    await self._enrich_with_quote_data(
                        page_results, page_codes, preferred_source=source, raise_on_error=False
                    )
                if not technical_enriched:
                    await self._enrich_with_technical_data(
                        page_results, page_codes, preferred_source=source, raise_on_error=False
                    )

            logger.info(
                f"✅ 数据库多集合筛选完成: 总数={total_count}, 返回={len(page_results)}, 数据源={source}"
            )
            return page_results, total_count

        except Exception as e:
            logger.error(f"❌ 数据库筛选失败: {e}")
            raise RuntimeError(f"数据库筛选失败: {str(e)}") from e

    async def _query_basic_collection_nulls_last(
        self,
        db,
        query: Dict[str, Any],
        sort_conditions: List[Tuple[str, int]],
        limit: int,
        offset: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """在 stock_basic_info 上实现精确 count_documents + nulls last 分页。"""
        collection = db["stock_basic_info"]
        total_count = await collection.count_documents(query)
        if total_count == 0 or offset >= total_count:
            return [], total_count

        primary_field = sort_conditions[0][0] if sort_conditions else "total_mv"

        # 构造非空查询与空值查询，确保升序和降序均将缺失/None/NaN 排在最后
        nan_val = float("nan")
        non_null_query = {"$and": [query, {primary_field: {"$exists": True, "$nin": [None, nan_val]}}]}
        non_null_count = await collection.count_documents(non_null_query)

        results: List[Dict[str, Any]] = []
        if offset < non_null_count:
            cursor = collection.find(non_null_query)
            if sort_conditions:
                cursor = cursor.sort(sort_conditions)
            cursor = cursor.skip(offset).limit(limit)
            async for doc in cursor:
                results.append(self._format_result(doc))

        remaining = limit - len(results)
        if remaining > 0:
            null_skip = max(0, offset - non_null_count)
            null_query = {
                "$and": [
                    query,
                    {
                        "$or": [
                            {primary_field: {"$in": [None, nan_val]}},
                            {primary_field: {"$exists": False}},
                        ]
                    },
                ]
            }
            null_cursor = collection.find(null_query)
            if len(sort_conditions) > 1:
                null_cursor = null_cursor.sort(sort_conditions[1:])
            else:
                null_cursor = null_cursor.sort([("code", 1)])
            null_cursor = null_cursor.skip(null_skip).limit(remaining)
            async for doc in null_cursor:
                results.append(self._format_result(doc))

        return results, total_count

    async def _query_basic_info_directly(
        self,
        db,
        source: str,
        sort_conditions: List[Tuple[str, int]],
        limit: int,
        offset: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """快速路径：直接查询 stock_basic_info，补齐真实 count_documents、nulls last 与行情/财务富集。"""
        try:
            query = {"source": source}
            results, total_count = await self._query_basic_collection_nulls_last(
                db, query, sort_conditions, limit, offset
            )
            codes = [r.get("code") for r in results if r.get("code")]
            if codes:
                await self._enrich_with_financial_data(results, codes, preferred_source=source)
                await self._enrich_with_quote_data(results, codes, preferred_source=source)
                await self._enrich_with_technical_data(results, codes, preferred_source=source)

            logger.info(
                f"✅ 快速路径查询完成: 总数={total_count}, 返回={len(results)}, 数据源={source}"
            )
            return results, total_count

        except Exception as e:
            logger.error(f"❌ 快速路径查询失败: {e}")
            raise

    async def _load_technical_map(
        self,
        db,
        codes: List[str],
        preferred_source: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """批量加载 stock_technical_indicators 中每只股票最新交易日的技术指标快照。"""
        if not codes:
            return {}
        try:
            normalized_codes = list({_normalize_code(c) for c in codes if _normalize_code(c)})
            if not normalized_codes:
                return {}
            collection = db["stock_technical_indicators"]
            query = {
                "$or": [
                    {"code": {"$in": normalized_codes}},
                    {"symbol": {"$in": normalized_codes}},
                ]
            }
            best_by_code: Dict[str, Tuple[Tuple[str, int], Dict[str, Any]]] = {}
            async for doc in collection.find(query):
                code = _normalize_code(doc.get("code") or doc.get("symbol"))
                if not code:
                    continue
                trade_date = str(doc.get("trade_date") or "")
                source_rank = 1 if (preferred_source and doc.get("source") == preferred_source) else 0
                rank = (trade_date, source_rank)
                prev = best_by_code.get(code)
                if prev is None or rank > prev[0]:
                    snapshot = {
                        field: doc.get(field)
                        for field in self.TECHNICAL_FIELDS
                        if doc.get(field) is not None
                    }
                    if trade_date:
                        snapshot["technical_trade_date"] = trade_date
                    best_by_code[code] = (rank, snapshot)
            return {code: payload for code, (_, payload) in best_by_code.items()}
        except Exception as e:
            logger.warning(f"⚠️ 加载技术指标快照失败: {e}")
            if raise_on_error:
                raise
            return {}

    @staticmethod
    def _merge_technical_into_results(
        results: List[Dict[str, Any]],
        tech_map: Dict[str, Dict[str, Any]],
    ) -> None:
        for item in results:
            code = _normalize_code(item.get("code"))
            td = tech_map.get(code)
            if not td:
                continue
            for key, val in td.items():
                if key == "technical_trade_date":
                    continue
                if val is not None:
                    item[key] = val

    async def _enrich_with_technical_data(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        preferred_source: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> None:
        if not codes:
            return
        db = get_mongo_db()
        tech_map = await self._load_technical_map(
            db, codes, preferred_source=preferred_source, raise_on_error=raise_on_error
        )
        self._merge_technical_into_results(results, tech_map)

    @staticmethod
    def _extract_quote_payload(doc: Dict[str, Any], *, quote_source: str) -> Optional[Dict[str, Any]]:
        close = _to_float_or_none(doc.get("close"))
        pre_close = _to_float_or_none(doc.get("pre_close"))
        pct_chg = _to_float_or_none(doc.get("pct_chg"))
        if pct_chg is None:
            pct_chg = _to_float_or_none(doc.get("change_pct"))
        if pct_chg is None and close is not None and pre_close not in (None, 0):
            pct_chg = round((close - pre_close) / pre_close * 100.0, 4)
        amount = _to_float_or_none(doc.get("amount"))
        volume = _to_float_or_none(doc.get("volume"))
        if close is None and pct_chg is None and amount is None and volume is None:
            return None
        return {
            "close": close,
            "pct_chg": pct_chg,
            "amount": amount,
            "volume": volume,
            "open": _to_float_or_none(doc.get("open")),
            "high": _to_float_or_none(doc.get("high")),
            "low": _to_float_or_none(doc.get("low")),
            "pre_close": pre_close,
            "trade_date": doc.get("trade_date"),
            "quote_source": quote_source,
        }

    async def _load_daily_quotes_fallback(
        self,
        db,
        codes: List[str],
        preferred_source: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """批量从 stock_daily_quotes 获取每只股票最新一个交易日的日线行情。"""
        if not codes:
            return {}
        daily_coll = db["stock_daily_quotes"]
        normalized_codes = list({_normalize_code(c) for c in codes if _normalize_code(c)})
        if not normalized_codes:
            return {}

        best_by_code: Dict[str, Tuple[Tuple[str, int], Dict[str, Any]]] = {}

        async def _scan_codes(target_codes: List[str], min_trade_date: Optional[str] = None) -> None:
            query: Dict[str, Any] = {
                "$or": [
                    {"code": {"$in": target_codes}},
                    {"symbol": {"$in": target_codes}},
                ]
            }
            if min_trade_date:
                query["trade_date"] = {"$gte": min_trade_date}

            async for doc in daily_coll.find(query):
                period = doc.get("period")
                if period not in (None, "daily"):
                    continue
                code = _normalize_code(doc.get("code") or doc.get("symbol"))
                if not code:
                    continue
                payload = self._extract_quote_payload(doc, quote_source="stock_daily_quotes")
                if not payload:
                    continue
                trade_date = str(doc.get("trade_date") or "")
                ds = str(doc.get("data_source") or doc.get("source") or "")
                source_rank = 1 if (preferred_source and ds == preferred_source) else 0
                rank = (trade_date, source_rank)
                prev = best_by_code.get(code)
                if prev is None or rank > prev[0]:
                    best_by_code[code] = (rank, payload)

        # 探测最新交易日，缩小扫描窗口以适配千万行生产日线库；停牌股再对缺失代码做全量回补。
        min_date_str: Optional[str] = None
        try:
            latest_doc = await daily_coll.find_one(
                {"period": "daily"},
                {"trade_date": 1},
                sort=[("trade_date", -1)],
            )
            if latest_doc is None:
                latest_doc = await daily_coll.find_one(
                    {},
                    {"trade_date": 1},
                    sort=[("trade_date", -1)],
                )
            latest_td = str((latest_doc or {}).get("trade_date") or "").strip()
            if len(latest_td) == 10:
                cutoff_dt = datetime.strptime(latest_td, "%Y-%m-%d") - timedelta(days=30)
                min_date_str = cutoff_dt.strftime("%Y-%m-%d")
        except Exception:
            min_date_str = None

        await _scan_codes(normalized_codes, min_trade_date=min_date_str)

        if min_date_str:
            still_missing = [c for c in normalized_codes if c not in best_by_code]
            if still_missing:
                await _scan_codes(still_missing, min_trade_date=None)

        return {code: payload for code, (_, payload) in best_by_code.items()}

    async def _load_quote_map(
        self,
        db,
        codes: List[str],
        preferred_source: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """优先批量读 market_quotes，缺失或空表时自动批量回退 stock_daily_quotes 最新日线。"""
        if not codes:
            return {}
        try:
            normalized_codes = list({_normalize_code(c) for c in codes if _normalize_code(c)})
            if not normalized_codes:
                return {}

            quotes_map: Dict[str, Dict[str, Any]] = {}
            quotes_collection = db["market_quotes"]
            quotes_cursor = quotes_collection.find({"code": {"$in": normalized_codes}})
            async for quote in quotes_cursor:
                code = _normalize_code(quote.get("code") or quote.get("symbol"))
                if not code:
                    continue
                payload = self._extract_quote_payload(quote, quote_source="market_quotes")
                if payload:
                    quotes_map[code] = payload

            missing_codes = [c for c in normalized_codes if c not in quotes_map]
            if missing_codes:
                fallback_map = await self._load_daily_quotes_fallback(
                    db, missing_codes, preferred_source=preferred_source
                )
                quotes_map.update(fallback_map)

            return quotes_map
        except Exception as e:
            logger.warning(f"⚠️ 批量加载行情数据失败: {e}")
            if raise_on_error:
                raise
            return {}

    @staticmethod
    def _merge_quotes_into_results(
        results: List[Dict[str, Any]],
        quotes_map: Dict[str, Dict[str, Any]],
    ) -> None:
        for result in results:
            code = _normalize_code(result.get("code"))
            qd = quotes_map.get(code)
            if not qd:
                continue
            for key, val in qd.items():
                if key == "quote_source":
                    continue
                if val is not None and result.get(key) is None:
                    result[key] = val

    async def _enrich_with_financial_data(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        preferred_source: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> None:
        """批量查询财务数据（最新报告期 + 多期派生因子）并填充到结果中。"""
        try:
            db = get_mongo_db()
            financial_collection = db["stock_financial_periods"]
            basic_info_collection = db["stock_basic_info"]

            if not preferred_source:
                from app.core.data_source_priority import get_preferred_data_source_async
                preferred_source = await get_preferred_data_source_async(market_category="a_shares")

            normalized_codes = list({_normalize_code(c) for c in codes if _normalize_code(c)})
            if not normalized_codes:
                return

            basic_info_map: Dict[str, Dict[str, Any]] = {}
            basic_info_cursor = basic_info_collection.find(
                {"code": {"$in": normalized_codes}, "source": preferred_source},
                {
                    "_id": 0,
                    "code": 1,
                    "source": 1,
                    "pe": 1,
                    "pe_ttm": 1,
                    "pb": 1,
                    "pb_mrq": 1,
                    "ps": 1,
                    "ps_ttm": 1,
                    "dividend_yield": 1,
                    "total_mv": 1,
                    "market_cap": 1,
                },
            )
            async for basic_info_doc in basic_info_cursor:
                basic_info_map[_normalize_code(basic_info_doc.get("code"))] = basic_info_doc

            def build_snapshot_record(snapshot: Dict[str, Any]) -> Dict[str, Any]:
                raw_balance = snapshot.get("raw_balance_sheet") or {}
                raw_income = snapshot.get("raw_income_statement") or {}
                raw_cashflow = snapshot.get("raw_cashflow_statement") or {}
                raw_indicators = snapshot.get("raw_financial_indicators") or {}
                return {
                    "report_period": snapshot.get("report_period"),
                    "revenue_ttm": snapshot.get("revenue_ttm"),
                    "revenue": snapshot.get("revenue"),
                    "oper_rev": snapshot.get("oper_rev"),
                    "net_profit_ttm": snapshot.get("net_profit_ttm"),
                    "net_profit": snapshot.get("net_profit"),
                    "net_income": snapshot.get("net_income"),
                    "total_profit": snapshot.get("total_profit"),
                    "oper_profit": snapshot.get("oper_profit"),
                    "operating_profit": snapshot.get("operating_profit"),
                    "ebit": snapshot.get("ebit"),
                    "oper_cost": snapshot.get("oper_cost"),
                    "oper_exp": snapshot.get("oper_exp"),
                    "admin_exp": snapshot.get("admin_exp"),
                    "rd_exp": snapshot.get("rd_exp"),
                    "fin_exp": snapshot.get("fin_exp"),
                    "roe": snapshot.get("roe"),
                    "roa": snapshot.get("roa"),
                    "gross_margin": snapshot.get("gross_margin"),
                    "netprofit_margin": snapshot.get("netprofit_margin"),
                    "debt_to_assets": snapshot.get("debt_to_assets"),
                    "assets_to_eqt": snapshot.get("assets_to_eqt"),
                    "current_ratio": snapshot.get("current_ratio"),
                    "quick_ratio": snapshot.get("quick_ratio"),
                    "cash_ratio": snapshot.get("cash_ratio"),
                    "total_assets": snapshot.get("total_assets"),
                    "total_cur_assets": snapshot.get("total_cur_assets"),
                    "total_nca": snapshot.get("total_nca"),
                    "total_cur_liab": snapshot.get("total_cur_liab"),
                    "total_liab": snapshot.get("total_liab"),
                    "total_equity": snapshot.get("total_equity"),
                    "total_ncl": snapshot.get("total_ncl"),
                    "money_cap": snapshot.get("money_cap"),
                    "fix_assets": snapshot.get("fix_assets"),
                    "accounts_receiv": snapshot.get("accounts_receiv"),
                    "inventories": snapshot.get("inventories"),
                    "n_cashflow_act": snapshot.get("n_cashflow_act"),
                    "n_cashflow_inv_act": snapshot.get("n_cashflow_inv_act"),
                    "n_cashflow_fin_act": snapshot.get("n_cashflow_fin_act"),
                    "balance_sheet": raw_balance,
                    "income_statement": raw_income,
                    "cashflow_statement": raw_cashflow,
                    "financial_indicators": raw_indicators,
                    "raw_data": {
                        "balance_sheet": [raw_balance] if raw_balance else [],
                        "income_statement": [raw_income] if raw_income else [],
                        "cashflow_statement": [raw_cashflow] if raw_cashflow else [],
                        "financial_indicators": [raw_indicators] if raw_indicators else [],
                    },
                }

            def is_newer_report_period(candidate: Any, current: Any) -> bool:
                candidate_value = str(candidate or "")
                current_value = str(current or "")
                return bool(candidate_value) and candidate_value > current_value

            snapshot_fields = (
                "roe",
                "roa",
                "gross_margin",
                "netprofit_margin",
                "revenue_ttm",
                "net_profit_ttm",
                "n_cashflow_act",
                "debt_to_assets",
                "assets_to_eqt",
                "current_ratio",
                "quick_ratio",
                "cash_ratio",
                "dividend_yield",
                "net_working_capital",
            )

            def _build_fin_pipeline(target_codes: List[str], match_source: Optional[str]) -> List[Dict[str, Any]]:
                match_stage: Dict[str, Any] = {"code": {"$in": target_codes}}
                if match_source:
                    match_stage["source"] = match_source
                return [
                    {"$match": match_stage},
                    {"$sort": {"code": 1, "report_period": -1, "ann_date": -1}},
                    {
                        "$group": {
                            "_id": "$code",
                            "roe": {"$first": "$roe"},
                            "roa": {"$first": "$roa"},
                            "net_profit": {"$first": "$net_profit"},
                            "total_profit": {"$first": "$total_profit"},
                            "oper_profit": {"$first": "$oper_profit"},
                            "gross_margin": {"$first": "$gross_margin"},
                            "netprofit_margin": {"$first": "$netprofit_margin"},
                            "revenue": {"$first": "$revenue"},
                            "oper_rev": {"$first": "$oper_rev"},
                            "net_income": {"$first": "$net_income"},
                            "total_assets": {"$first": "$total_assets"},
                            "total_nca": {"$first": "$total_nca"},
                            "fix_assets": {"$first": "$fix_assets"},
                            "oper_cost": {"$first": "$oper_cost"},
                            "oper_exp": {"$first": "$oper_exp"},
                            "admin_exp": {"$first": "$admin_exp"},
                            "rd_exp": {"$first": "$rd_exp"},
                            "fin_exp": {"$first": "$fin_exp"},
                            "ebit": {"$first": "$ebit"},
                            "operating_profit": {"$first": "$operating_profit"},
                            "pe_ttm": {"$first": "$pe_ttm"},
                            "pb_mrq": {"$first": "$pb_mrq"},
                            "piotroski_f_score": {"$first": "$piotroski_f_score"},
                            "altman_z_score": {"$first": "$altman_z_score"},
                            "beneish_m_score": {"$first": "$beneish_m_score"},
                            "revenue_ttm": {"$first": "$revenue_ttm"},
                            "net_profit_ttm": {"$first": "$net_profit_ttm"},
                            "n_cashflow_act": {"$first": "$n_cashflow_act"},
                            "n_cashflow_inv_act": {"$first": "$n_cashflow_inv_act"},
                            "n_cashflow_fin_act": {"$first": "$n_cashflow_fin_act"},
                            "revenue_yoy": {"$first": "$revenue_yoy"},
                            "net_profit_yoy": {"$first": "$net_profit_yoy"},
                            "oper_profit_yoy": {"$first": "$oper_profit_yoy"},
                            "roic": {"$first": "$roic"},
                            "fcf": {"$first": "$fcf"},
                            "fcf_yield": {"$first": "$fcf_yield"},
                            "fcf_margin": {"$first": "$fcf_margin"},
                            "ocf_yield": {"$first": "$ocf_yield"},
                            "peg": {"$first": "$peg"},
                            "assets_to_eqt": {"$first": "$assets_to_eqt"},
                            "debt_to_assets": {"$first": "$debt_to_assets"},
                            "current_ratio": {"$first": "$current_ratio"},
                            "quick_ratio": {"$first": "$quick_ratio"},
                            "cash_ratio": {"$first": "$cash_ratio"},
                            "dividend_yield": {"$first": "$dividend_yield"},
                            "total_cur_assets": {"$first": "$total_cur_assets"},
                            "total_cur_liab": {"$first": "$total_cur_liab"},
                            "total_liab": {"$first": "$total_liab"},
                            "total_equity": {"$first": "$total_equity"},
                            "money_cap": {"$first": "$money_cap"},
                            "total_ncl": {"$first": "$total_ncl"},
                            "accounts_receiv": {"$first": "$accounts_receiv"},
                            "inventories": {"$first": "$inventories"},
                            "period_snapshots": {
                                "$push": {
                                    "total_cur_assets": "$total_cur_assets",
                                    "total_cur_liab": "$total_cur_liab",
                                    "total_assets": "$total_assets",
                                    "total_liab": "$total_liab",
                                    "total_ncl": "$total_ncl",
                                    "fix_assets": "$fix_assets",
                                    "accounts_receiv": "$accounts_receiv",
                                    "inventories": "$inventories",
                                    "revenue_ttm": "$revenue_ttm",
                                    "revenue": "$revenue",
                                    "oper_rev": "$oper_rev",
                                    "net_profit_ttm": "$net_profit_ttm",
                                    "net_profit": "$net_profit",
                                    "net_income": "$net_income",
                                    "total_profit": "$total_profit",
                                    "oper_profit": "$oper_profit",
                                    "operating_profit": "$operating_profit",
                                    "ebit": "$ebit",
                                    "oper_cost": "$oper_cost",
                                    "oper_exp": "$oper_exp",
                                    "admin_exp": "$admin_exp",
                                    "rd_exp": "$rd_exp",
                                    "fin_exp": "$fin_exp",
                                    "roe": "$roe",
                                    "roa": "$roa",
                                    "gross_margin": "$gross_margin",
                                    "netprofit_margin": "$netprofit_margin",
                                    "debt_to_assets": "$debt_to_assets",
                                    "assets_to_eqt": "$assets_to_eqt",
                                    "current_ratio": "$current_ratio",
                                    "quick_ratio": "$quick_ratio",
                                    "cash_ratio": "$cash_ratio",
                                    "n_cashflow_act": "$n_cashflow_act",
                                    "n_cashflow_inv_act": "$n_cashflow_inv_act",
                                    "n_cashflow_fin_act": "$n_cashflow_fin_act",
                                    "total_nca": "$total_nca",
                                    "total_equity": "$total_equity",
                                    "money_cap": "$money_cap",
                                    "raw_balance_sheet": "$raw_data.balance_sheet",
                                    "raw_income_statement": "$raw_data.income_statement",
                                    "raw_cashflow_statement": "$raw_data.cashflow_statement",
                                    "raw_financial_indicators": "$raw_data.financial_indicators",
                                    "report_period": "$report_period",
                                }
                            },
                            "report_period": {"$first": "$report_period"},
                        }
                    },
                ]

            aggregated_docs: Dict[str, Dict[str, Any]] = {}
            async for doc in financial_collection.aggregate(_build_fin_pipeline(normalized_codes, preferred_source)):
                code = _normalize_code(doc.get("_id"))
                if code:
                    aggregated_docs[code] = doc

            # 若部分股票在首选数据源中无财务记录，回退查询任意数据源的最新财报期
            missing_fin_codes = [c for c in normalized_codes if c not in aggregated_docs]
            if missing_fin_codes:
                async for doc in financial_collection.aggregate(_build_fin_pipeline(missing_fin_codes, None)):
                    code = _normalize_code(doc.get("_id"))
                    if code and code not in aggregated_docs:
                        aggregated_docs[code] = doc

            financial_data_map: Dict[str, Dict[str, Any]] = {}
            for code, doc in aggregated_docs.items():
                debt_to_assets = doc.get("debt_to_assets")
                assets_to_eqt = doc.get("assets_to_eqt")
                current_ratio = doc.get("current_ratio")
                quick_ratio = doc.get("quick_ratio")
                cash_ratio = doc.get("cash_ratio")
                dividend_yield = doc.get("dividend_yield")
                total_cur_assets = doc.get("total_cur_assets")
                total_cur_liab = doc.get("total_cur_liab")
                total_liab = doc.get("total_liab")
                total_equity = doc.get("total_equity")
                money_cap = doc.get("money_cap")
                total_ncl = doc.get("total_ncl")
                accounts_receiv = doc.get("accounts_receiv")
                inventories = doc.get("inventories")

                net_working_capital = None
                if total_cur_assets is not None and total_cur_liab is not None:
                    net_working_capital = (total_cur_assets - total_cur_liab) / 1e8

                working_capital_change = None
                period_snapshots = doc.get("period_snapshots") or []
                if len(period_snapshots) > 1:
                    current_snapshot = period_snapshots[0] or {}
                    previous_snapshot = period_snapshots[1] or {}
                    current_nwc = None
                    previous_nwc = None
                    if current_snapshot.get("total_cur_assets") is not None and current_snapshot.get("total_cur_liab") is not None:
                        current_nwc = current_snapshot.get("total_cur_assets") - current_snapshot.get("total_cur_liab")
                    if previous_snapshot.get("total_cur_assets") is not None and previous_snapshot.get("total_cur_liab") is not None:
                        previous_nwc = previous_snapshot.get("total_cur_assets") - previous_snapshot.get("total_cur_liab")
                    if current_nwc is not None and previous_nwc is not None:
                        working_capital_change = (current_nwc - previous_nwc) / 1e8

                fcf = doc.get("fcf")
                if fcf is None and doc.get("n_cashflow_act") is not None and doc.get("n_cashflow_inv_act") is not None:
                    fcf = doc.get("n_cashflow_act") + doc.get("n_cashflow_inv_act")

                cash_conversion = None
                if doc.get("n_cashflow_act") is not None and doc.get("net_profit") not in (None, 0):
                    cash_conversion = doc.get("n_cashflow_act") / doc.get("net_profit")

                accrual_ratio = None
                if doc.get("net_profit") is not None and doc.get("n_cashflow_act") is not None and doc.get("total_assets") not in (None, 0):
                    accrual_ratio = (doc.get("net_profit") - doc.get("n_cashflow_act")) / doc.get("total_assets") * 100

                revenue_base = doc.get("revenue_ttm") if doc.get("revenue_ttm") not in (None, 0) else doc.get("revenue")
                fcf_margin = doc.get("fcf_margin")
                if fcf_margin is None and fcf is not None and revenue_base not in (None, 0):
                    fcf_margin = fcf / revenue_base * 100

                asset_turnover = None
                if revenue_base not in (None, 0) and doc.get("total_assets") not in (None, 0):
                    asset_turnover = revenue_base / doc.get("total_assets")

                gross_profit_to_assets = None
                if revenue_base not in (None, 0) and doc.get("gross_margin") is not None and doc.get("total_assets") not in (None, 0):
                    gross_profit_to_assets = revenue_base * doc.get("gross_margin") / 100 / doc.get("total_assets") * 100

                interest_coverage = None
                inventory_turnover = None
                cost_base = doc.get("oper_cost")
                if cost_base in (None, 0) and revenue_base not in (None, 0) and doc.get("gross_margin") is not None:
                    cost_base = revenue_base * (1 - doc.get("gross_margin") / 100)
                if cost_base not in (None, 0) and inventories not in (None, 0):
                    inventory_turnover = cost_base / inventories

                receivable_turnover = None
                if revenue_base not in (None, 0) and accounts_receiv not in (None, 0):
                    receivable_turnover = revenue_base / accounts_receiv

                net_cash_position = None
                if money_cap is not None and doc.get("total_assets") not in (None, 0):
                    debt_proxy = total_ncl
                    if debt_proxy is None and total_liab is not None and total_cur_liab is not None:
                        debt_proxy = total_liab - total_cur_liab
                    if debt_proxy is not None:
                        net_cash_position = (money_cap - max(debt_proxy, 0)) / doc.get("total_assets") * 100

                financial_records = [build_snapshot_record(snapshot) for snapshot in period_snapshots if snapshot]
                revenue_yoy = doc.get("revenue_yoy")
                net_profit_yoy = doc.get("net_profit_yoy")
                oper_profit_yoy = doc.get("oper_profit_yoy")
                revenue_cagr_3y = None
                profit_cagr_3y = None
                roic = doc.get("roic")
                piotroski_f_score = None
                beneish_m_score = None

                if financial_records:
                    latest_financial = financial_records[0]
                    factor_context = {
                        "symbol": code,
                        "basic_info": {},
                        "market_quotes": {},
                        "financial_records": financial_records,
                        "latest_financial": latest_financial,
                        "current_price": None,
                    }
                    computed_rev_yoy = _compute_yoy_growth(financial_records, "revenue")
                    if computed_rev_yoy is not None:
                        revenue_yoy = computed_rev_yoy
                    computed_np_yoy = _compute_yoy_growth(financial_records, "net_profit")
                    if computed_np_yoy is not None:
                        net_profit_yoy = computed_np_yoy
                    computed_op_yoy = _compute_yoy_growth(financial_records, "oper_profit")
                    if computed_op_yoy is not None:
                        oper_profit_yoy = computed_op_yoy
                    revenue_cagr_3y = _compute_cagr(financial_records, (("revenue_ttm",), ("revenue",), ("oper_rev",)), 3)
                    profit_cagr_3y = _compute_cagr(financial_records, (("net_profit_ttm",), ("net_profit",), ("net_income",)), 3)
                    computed_roic = _estimate_roic(latest_financial)
                    if computed_roic is not None:
                        roic = computed_roic
                    interest_coverage = _estimate_interest_coverage(latest_financial)
                    piotroski_f_score = _estimate_piotroski_f_score(factor_context)
                    beneish_m_score = _estimate_beneish_m_score(factor_context)

                financial_data_map[code] = {
                    "roe": doc.get("roe"),
                    "roa": doc.get("roa"),
                    "netprofit_margin": doc.get("netprofit_margin"),
                    "gross_margin": doc.get("gross_margin"),
                    "pe_ttm": doc.get("pe_ttm"),
                    "pb_mrq": doc.get("pb_mrq"),
                    "revenue_ttm": doc.get("revenue_ttm"),
                    "revenue_yoy": revenue_yoy,
                    "net_profit_ttm": doc.get("net_profit_ttm"),
                    "net_profit_yoy": net_profit_yoy,
                    "oper_profit_yoy": oper_profit_yoy,
                    "n_cashflow_act": doc.get("n_cashflow_act"),
                    "fcf": fcf,
                    "fcf_yield": doc.get("fcf_yield"),
                    "fcf_margin": fcf_margin,
                    "ocf_yield": doc.get("ocf_yield"),
                    "peg": doc.get("peg"),
                    "cash_conversion": cash_conversion,
                    "accrual_ratio": accrual_ratio,
                    "asset_turnover": asset_turnover,
                    "gross_profit_to_assets": gross_profit_to_assets,
                    "interest_coverage": interest_coverage,
                    "inventory_turnover": inventory_turnover,
                    "receivable_turnover": receivable_turnover,
                    "net_cash_position": net_cash_position,
                    "piotroski_f_score": piotroski_f_score if piotroski_f_score is not None else doc.get("piotroski_f_score"),
                    "altman_z_score": doc.get("altman_z_score"),
                    "beneish_m_score": beneish_m_score if beneish_m_score is not None else doc.get("beneish_m_score"),
                    "debt_to_assets": debt_to_assets,
                    "assets_to_eqt": assets_to_eqt,
                    "current_ratio": current_ratio,
                    "quick_ratio": quick_ratio,
                    "cash_ratio": cash_ratio,
                    "dividend_yield": dividend_yield,
                    "total_equity": total_equity,
                    "roic": roic,
                    "revenue_cagr_3y": revenue_cagr_3y,
                    "profit_cagr_3y": profit_cagr_3y,
                    "working_capital_change": working_capital_change,
                    "net_working_capital": net_working_capital,
                    "report_period": doc.get("report_period"),
                    "_financial_records": financial_records,
                }

            for result in results:
                code = _normalize_code(result.get("code"))
                if code in financial_data_map:
                    financial_data = financial_data_map[code]
                    raw_basic_info = dict(basic_info_map.get(code) or {})
                    basic_info_context = {
                        **raw_basic_info,
                        "total_mv": raw_basic_info.get("total_mv", result.get("total_mv")),
                        "market_cap": raw_basic_info.get("market_cap", result.get("total_mv")),
                    }
                    latest_report_period = financial_data.get("report_period")
                    newer_snapshot = is_newer_report_period(latest_report_period, result.get("report_period"))
                    if latest_report_period is not None and (newer_snapshot or result.get("report_period") is None):
                        result["report_period"] = latest_report_period

                    for field_name in snapshot_fields:
                        field_value = financial_data.get(field_name)
                        if field_value is None:
                            continue
                        if newer_snapshot or result.get(field_name) is None:
                            result[field_name] = field_value

                    if newer_snapshot or result.get("pb_mrq") is None:
                        total_mv = result.get("total_mv")
                        total_equity = financial_data.get("total_equity")
                        if total_mv not in (None, 0) and total_equity not in (None, 0):
                            result["pb_mrq"] = (total_mv * 100000000) / total_equity
                        elif financial_data.get("pb_mrq") is not None:
                            result["pb_mrq"] = financial_data.get("pb_mrq")
                    if newer_snapshot or result.get("pe_ttm") is None:
                        total_mv = result.get("total_mv")
                        net_profit_ttm = financial_data.get("net_profit_ttm")
                        if total_mv not in (None, 0) and net_profit_ttm not in (None, 0):
                            result["pe_ttm"] = (total_mv * 100000000) / net_profit_ttm
                        elif financial_data.get("pe_ttm") is not None:
                            result["pe_ttm"] = financial_data.get("pe_ttm")

                    for derived_key in (
                        "asset_turnover",
                        "gross_profit_to_assets",
                        "interest_coverage",
                        "inventory_turnover",
                        "receivable_turnover",
                        "net_cash_position",
                        "piotroski_f_score",
                        "beneish_m_score",
                        "fcf",
                        "fcf_margin",
                        "cash_conversion",
                        "accrual_ratio",
                    ):
                        if result.get(derived_key) is None and financial_data.get(derived_key) is not None:
                            result[derived_key] = financial_data.get(derived_key)

                    for override_key in (
                        "revenue_yoy",
                        "net_profit_yoy",
                        "oper_profit_yoy",
                        "revenue_cagr_3y",
                        "profit_cagr_3y",
                        "roic",
                        "working_capital_change",
                    ):
                        if financial_data.get(override_key) is not None:
                            result[override_key] = financial_data.get(override_key)

                    if result.get("peg") is None:
                        if financial_data.get("peg") is not None:
                            result["peg"] = financial_data.get("peg")
                        else:
                            pe_ttm = result.get("pe_ttm")
                            growth = financial_data.get("net_profit_yoy")
                            if pe_ttm not in (None, 0) and growth not in (None, 0) and growth > 0:
                                result["peg"] = pe_ttm / growth
                    if result.get("fcf_yield") is None:
                        if financial_data.get("fcf_yield") is not None:
                            result["fcf_yield"] = financial_data.get("fcf_yield")
                        else:
                            fcf_val = financial_data.get("fcf")
                            total_mv = result.get("total_mv")
                            if fcf_val is not None and total_mv not in (None, 0):
                                result["fcf_yield"] = fcf_val / (total_mv * 1e8) * 100
                    if result.get("ocf_yield") is None:
                        if financial_data.get("ocf_yield") is not None:
                            result["ocf_yield"] = financial_data.get("ocf_yield")
                        else:
                            ocf = financial_data.get("n_cashflow_act")
                            total_mv = result.get("total_mv")
                            if ocf is not None and total_mv not in (None, 0):
                                result["ocf_yield"] = ocf / (total_mv * 1e8) * 100

                    if result.get("altman_z_score") is None:
                        total_mv = result.get("total_mv")
                        financial_records = financial_data.get("_financial_records") or []
                        if total_mv not in (None, 0) and financial_records:
                            altman_context = {
                                "symbol": code,
                                "basic_info": {"total_mv": total_mv},
                                "market_quotes": {},
                                "financial_records": financial_records,
                                "latest_financial": financial_records[0],
                                "current_price": None,
                            }
                            result["altman_z_score"] = _estimate_altman_z_score(altman_context)
                        if result.get("altman_z_score") is None and financial_data.get("altman_z_score") is not None:
                            result["altman_z_score"] = financial_data.get("altman_z_score")

                    financial_records = financial_data.get("_financial_records") or []
                    if financial_records:
                        latest_financial = financial_records[0]
                        diagnostics_context = {
                            "symbol": code,
                            "basic_info": basic_info_context,
                            "market_quotes": {},
                            "financial_records": financial_records,
                            "latest_financial": latest_financial,
                            "current_price": None,
                        }
                        cautious_factor_values = {
                            "piotroski_f_score": result.get("piotroski_f_score"),
                            "altman_z_score": result.get("altman_z_score"),
                            "beneish_m_score": result.get("beneish_m_score"),
                        }
                        factor_diagnostics = _build_factor_diagnostics(diagnostics_context, cautious_factor_values)
                        null_factor_diagnostics = {
                            field_name: diagnostic
                            for field_name, diagnostic in factor_diagnostics.items()
                            if result.get(field_name) is None
                        }
                        if null_factor_diagnostics:
                            result["factor_diagnostics"] = null_factor_diagnostics

                        factor_values_for_warnings = {
                            "pe_ttm": result.get("pe_ttm"),
                            "pb": result.get("pb"),
                            "pb_mrq": result.get("pb_mrq"),
                            "ps_ttm": result.get("ps_ttm"),
                            "dividend_yield": result.get("dividend_yield"),
                            "industry_pe_median": result.get("industry_pe_median"),
                            "industry_pb_median": result.get("industry_pb_median"),
                            "roic": result.get("roic"),
                            "interest_coverage": result.get("interest_coverage"),
                            "inventory_turnover": result.get("inventory_turnover"),
                            "net_cash_position": result.get("net_cash_position"),
                            "net_profit_ttm": result.get("net_profit_ttm"),
                            "fcf": result.get("fcf"),
                        }
                        factor_warnings = {
                            **_build_value_factor_warnings(diagnostics_context, factor_values_for_warnings, {}, {}),
                            **_build_quality_factor_warnings(diagnostics_context, factor_values_for_warnings),
                            **_build_growth_factor_warnings(diagnostics_context, factor_values_for_warnings),
                        }
                        if factor_warnings:
                            result["factor_warnings"] = factor_warnings

            logger.debug(f"✅ 已填充 {len(financial_data_map)} 条财务数据")

        except Exception as e:
            logger.warning(f"⚠️ 填充财务数据失败: {e}")
            if raise_on_error:
                raise

    def _format_result(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """格式化查询结果，统一使用后端字段名"""
        code = doc.get("code", "")
        market_type = "A股"
        if code:
            if code.startswith("6"):
                market_type = "A股"
            elif code.startswith(("0", "3")):
                market_type = "A股"
            elif code.startswith(("8", "4", "92")):
                market_type = "A股"

        result = {
            # 基础信息
            "code": doc.get("code"),
            "name": doc.get("name"),
            "industry": doc.get("industry"),
            "area": doc.get("area"),
            "market": market_type,
            "board": doc.get("market"),
            "exchange": doc.get("sse"),
            "list_date": doc.get("list_date"),

            # 市值信息（亿元）
            "total_mv": doc.get("total_mv"),
            "circ_mv": doc.get("circ_mv"),

            # 财务指标
            "pe": doc.get("pe"),
            "pb": doc.get("pb"),
            "pe_ttm": doc.get("pe_ttm"),
            "ps_ttm": doc.get("ps_ttm"),
            "pb_mrq": doc.get("pb_mrq"),
            "roe": doc.get("roe"),
            "roa": doc.get("roa"),
            "gross_margin": doc.get("gross_margin"),
            "netprofit_margin": doc.get("netprofit_margin"),
            "peg": doc.get("peg"),
            "roic": doc.get("roic"),
            "asset_turnover": doc.get("asset_turnover"),
            "gross_profit_to_assets": doc.get("gross_profit_to_assets"),
            "interest_coverage": doc.get("interest_coverage"),
            "inventory_turnover": doc.get("inventory_turnover"),
            "receivable_turnover": doc.get("receivable_turnover"),
            "net_cash_position": doc.get("net_cash_position"),
            "piotroski_f_score": doc.get("piotroski_f_score"),
            "altman_z_score": doc.get("altman_z_score"),
            "beneish_m_score": doc.get("beneish_m_score"),
            "revenue_cagr_3y": doc.get("revenue_cagr_3y"),
            "profit_cagr_3y": doc.get("profit_cagr_3y"),
            "revenue_ttm": doc.get("revenue_ttm"),
            "revenue_yoy": doc.get("revenue_yoy"),
            "net_profit_ttm": doc.get("net_profit_ttm"),
            "net_profit_yoy": doc.get("net_profit_yoy"),
            "oper_profit_yoy": doc.get("oper_profit_yoy"),
            "n_cashflow_act": doc.get("n_cashflow_act"),
            "fcf": doc.get("fcf"),
            "fcf_yield": doc.get("fcf_yield"),
            "fcf_margin": doc.get("fcf_margin"),
            "ocf_yield": doc.get("ocf_yield"),
            "cash_conversion": doc.get("cash_conversion"),
            "accrual_ratio": doc.get("accrual_ratio"),
            "working_capital_change": doc.get("working_capital_change"),

            # 扩展财务指标
            "debt_to_assets": doc.get("debt_to_assets"),
            "assets_to_eqt": doc.get("assets_to_eqt"),
            "current_ratio": doc.get("current_ratio"),
            "quick_ratio": doc.get("quick_ratio"),
            "cash_ratio": doc.get("cash_ratio"),
            "dividend_yield": doc.get("dividend_yield"),
            "net_working_capital": doc.get("net_working_capital"),
            "is_st": _resolve_is_st(doc),
            "report_period": doc.get("report_period"),

            # 交易指标
            "turnover_rate": doc.get("turnover_rate"),
            "volume_ratio": doc.get("volume_ratio"),

            # 行情数据
            "close": doc.get("close"),
            "pct_chg": doc.get("pct_chg"),
            "amount": doc.get("amount"),
            "volume": doc.get("volume"),
            "open": doc.get("open"),
            "high": doc.get("high"),
            "low": doc.get("low"),

            # 技术指标
            "ma20": doc.get("ma20"),
            "rsi14": doc.get("rsi14"),
            "kdj_k": doc.get("kdj_k"),
            "kdj_d": doc.get("kdj_d"),
            "kdj_j": doc.get("kdj_j"),
            "dif": doc.get("dif"),
            "dea": doc.get("dea"),
            "macd_hist": doc.get("macd_hist"),

            # 元数据
            "source": doc.get("source", "database"),
            "updated_at": doc.get("updated_at"),
        }

        return {k: v for k, v in result.items() if v is not None}

    @staticmethod
    def _summarize_numeric_values(field: str, values: Iterable[Any]) -> Dict[str, Any]:
        nums = [v for v in (_to_float_or_none(item) for item in values) if v is not None]
        if not nums:
            return {"field": field, "count": 0}
        return {
            "field": field,
            "min": min(nums),
            "max": max(nums),
            "avg": round(sum(nums) / len(nums), 2),
            "count": len(nums),
        }

    async def get_field_statistics(self, field: str) -> Dict[str, Any]:
        """按字段归属集合查询统计信息（basic / financial / technical / quote+daily回退）。"""
        try:
            db_field = self._resolve_field_name(field)
            if not db_field:
                return {}

            db = get_mongo_db()
            group = self._get_field_group(db_field)

            async def _aggregate_simple(collection, match_extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
                match_cond: Dict[str, Any] = {db_field: {"$exists": True, "$ne": None}}
                if match_extra:
                    match_cond.update(match_extra)
                pipeline = [
                    {"$match": match_cond},
                    {
                        "$group": {
                            "_id": None,
                            "min": {"$min": f"${db_field}"},
                            "max": {"$max": f"${db_field}"},
                            "avg": {"$avg": f"${db_field}"},
                            "count": {"$sum": 1},
                        }
                    },
                ]
                result = await collection.aggregate(pipeline).to_list(length=1)
                if result:
                    stats = result[0]
                    avg_value = stats.get("avg")
                    return {
                        "field": field,
                        "min": stats.get("min"),
                        "max": stats.get("max"),
                        "avg": round(avg_value, 2) if avg_value is not None else None,
                        "count": stats.get("count", 0),
                    }
                return {"field": field, "count": 0}

            if group == "technical":
                collection = db["stock_technical_indicators"]
                pipeline = [
                    {"$match": {db_field: {"$exists": True, "$ne": None}}},
                    {"$sort": {"code": 1, "trade_date": -1}},
                    {"$group": {"_id": "$code", "val": {"$first": f"${db_field}"}}},
                    {
                        "$group": {
                            "_id": None,
                            "min": {"$min": "$val"},
                            "max": {"$max": "$val"},
                            "avg": {"$avg": "$val"},
                            "count": {"$sum": 1},
                        }
                    },
                ]
                result = await collection.aggregate(pipeline).to_list(length=1)
                if result:
                    stats = result[0]
                    avg_value = stats.get("avg")
                    return {
                        "field": field,
                        "min": stats.get("min"),
                        "max": stats.get("max"),
                        "avg": round(avg_value, 2) if avg_value is not None else None,
                        "count": stats.get("count", 0),
                    }
                return {"field": field, "count": 0}

            if group == "quote":
                stats = await _aggregate_simple(db["market_quotes"])
                if stats.get("count", 0) > 0:
                    return stats
                # market_quotes 空/停写时，按 stock_daily_quotes 最新交易日统计
                daily_coll = db["stock_daily_quotes"]
                latest_doc = await daily_coll.find_one(
                    {"period": "daily"}, {"trade_date": 1}, sort=[("trade_date", -1)]
                )
                if latest_doc is None:
                    latest_doc = await daily_coll.find_one({}, {"trade_date": 1}, sort=[("trade_date", -1)])
                latest_td = (latest_doc or {}).get("trade_date")
                match_daily: Dict[str, Any] = {db_field: {"$exists": True, "$ne": None}}
                if latest_td:
                    match_daily["trade_date"] = latest_td
                pipeline = [
                    {"$match": match_daily},
                    {"$sort": {"trade_date": -1}},
                    {"$group": {"_id": {"$ifNull": ["$code", "$symbol"]}, "val": {"$first": f"${db_field}"}}},
                    {
                        "$group": {
                            "_id": None,
                            "min": {"$min": "$val"},
                            "max": {"$max": "$val"},
                            "avg": {"$avg": "$val"},
                            "count": {"$sum": 1},
                        }
                    },
                ]
                result = await daily_coll.aggregate(pipeline).to_list(length=1)
                if result:
                    row = result[0]
                    avg_value = row.get("avg")
                    return {
                        "field": field,
                        "min": row.get("min"),
                        "max": row.get("max"),
                        "avg": round(avg_value, 2) if avg_value is not None else None,
                        "count": row.get("count", 0),
                    }
                return {"field": field, "count": 0}

            if group == "financial":
                fin_coll = db["stock_financial_periods"]
                pipeline = [
                    {"$match": {db_field: {"$exists": True, "$ne": None}}},
                    {"$sort": {"code": 1, "report_period": -1, "ann_date": -1}},
                    {"$group": {"_id": "$code", "val": {"$first": f"${db_field}"}}},
                    {
                        "$group": {
                            "_id": None,
                            "min": {"$min": "$val"},
                            "max": {"$max": "$val"},
                            "avg": {"$avg": "$val"},
                            "count": {"$sum": 1},
                        }
                    },
                ]
                result = await fin_coll.aggregate(pipeline).to_list(length=1)
                if result and result[0].get("count", 0) > 0:
                    row = result[0]
                    avg_value = row.get("avg")
                    return {
                        "field": field,
                        "min": row.get("min"),
                        "max": row.get("max"),
                        "avg": round(avg_value, 2) if avg_value is not None else None,
                        "count": row.get("count", 0),
                    }
                # 若财务集合未直接存储（如 pe_ttm/pb_mrq/dividend_yield 落在 stock_basic_info），回退查 stock_basic_info
                return await _aggregate_simple(db["stock_basic_info"])

            return await _aggregate_simple(db["stock_basic_info"])

        except Exception as e:
            logger.error(f"获取字段统计失败: {e}")
            return {"field": field, "error": str(e)}

    async def _enrich_with_quote_data(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        preferred_source: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> None:
        """批量从 market_quotes 补充行情数据，缺失或空表时自动回退 stock_daily_quotes 最新日线。"""
        if not codes:
            return
        db = get_mongo_db()
        quotes_map = await self._load_quote_map(
            db, codes, preferred_source=preferred_source, raise_on_error=raise_on_error
        )
        self._merge_quotes_into_results(results, quotes_map)
        logger.debug(f"✅ 行情数据补充完成: {len(quotes_map)}/{len(codes)} 只股票有行情")

    def _apply_quote_filter(
        self,
        results: List[Dict[str, Any]],
        quote_conditions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """在内存中根据行情条件过滤结果"""
        return [item for item in results if self._matches_all_conditions(item, quote_conditions)]

    async def _filter_by_quotes(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        quote_conditions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """兼容旧调用入口：批量加载行情（含日线回退）后执行过滤。"""
        db = get_mongo_db()
        quotes_map = await self._load_quote_map(db, codes, raise_on_error=True)
        self._merge_quotes_into_results(results, quotes_map)
        return self._apply_quote_filter(results, quote_conditions)

    async def get_available_values(self, field: str, limit: int = 100) -> List[str]:
        """获取字段的可选值列表（用于枚举类型字段）"""
        try:
            db_field = self._resolve_field_name(field)
            if not db_field:
                return []

            db = get_mongo_db()
            group = self._get_field_group(db_field)
            if group == "financial":
                collection = db["stock_financial_periods"]
            elif group == "technical":
                collection = db["stock_technical_indicators"]
            else:
                collection = db["stock_basic_info"]

            values = await collection.distinct(db_field)
            values = [v for v in values if v is not None]
            values.sort()
            return values[:limit]

        except Exception as e:
            logger.error(f"获取字段可选值失败: {e}")
            return []


# 全局服务实例
_database_screening_service: Optional[DatabaseScreeningService] = None


def get_database_screening_service() -> DatabaseScreeningService:
    """获取数据库筛选服务实例"""
    global _database_screening_service
    if _database_screening_service is None:
        _database_screening_service = DatabaseScreeningService()
    return _database_screening_service
