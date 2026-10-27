from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any, List, Tuple
from datetime import datetime
import logging
import re

from app.routers.auth_db import get_current_user
from app.core.database import get_mongo_db
from app.core.response import ok
from app.services.database.serialization import serialize_document
from app.services.quote_contract import (
    cn_now,
    resolve_cn_price,
    resolve_cn_range,
    validate_price,
)

router = APIRouter(prefix="/paper", tags=["paper"])
logger = logging.getLogger("webapi")

ETF_CODE_PREFIXES = ("50", "51", "52", "56", "58", "15", "16", "18")


# 每个市场的初始资金配置
INITIAL_CASH_BY_MARKET = {
    "CNY": 1_000_000.0,   # A股：100万人民币
    "HKD": 1_000_000.0,   # 港股：100万港币
    "USD": 100_000.0      # 美股：10万美元
}


class PlaceOrderRequest(BaseModel):
    code: str = Field(..., description="股票代码（支持A股/ETF/港股/美股）")
    side: Literal["buy", "sell"]
    quantity: int = Field(..., gt=0)
    market: Optional[str] = Field(None, description="市场类型 (CN/ETF/HK/US)，不传则自动识别")
    # 可选：指定价格（如果不指定，则使用当前实时价格）
    price: Optional[float] = Field(None, description="指定价格（必须在当天最高最低价之间）")
    # 可选：关联的分析ID，便于从分析页面一键下单后追踪
    analysis_id: Optional[str] = None


def _detect_market_and_code(code: str) -> Tuple[str, str]:
    """
    检测股票代码的市场类型并标准化代码

    Returns:
        (market, normalized_code): 市场类型和标准化后的代码
            - CN: A股（6位数字）
            - HK: 港股（4-5位数字或带.HK后缀）
            - US: 美股（字母代码）
    """
    code = code.strip().upper()

    # 港股：带 .HK 后缀
    if code.endswith('.HK'):
        return ('HK', code[:-3].zfill(5))

    # 美股：纯字母
    if re.match(r'^[A-Z]+$', code):
        return ('US', code)

    # 港股：4-5位数字
    if re.match(r'^\d{4,5}$', code):
        return ('HK', code.zfill(5))

    # A股：6位数字
    if re.match(r'^\d{6}$', code):
        return ('CN', code)

    # 默认当作A股，补齐6位
    return ('CN', code.zfill(6))


def _is_cn_etf_symbol(code: str) -> bool:
    code_str = str(code or "").strip().upper().zfill(6)
    return len(code_str) == 6 and code_str.isdigit() and code_str.startswith(ETF_CODE_PREFIXES)


def _resolve_market_and_code(code: str, market: Optional[str] = None) -> Tuple[str, str, bool]:
    normalized_code = str(code or "").strip().upper()
    requested_market = str(market or "").strip().upper()

    if requested_market == "ETF":
        normalized_code = normalized_code.zfill(6)
        return "CN", normalized_code, True

    if requested_market in {"CN", "HK", "US"}:
        if requested_market == "CN":
            normalized_code = normalized_code.zfill(6)
        elif requested_market == "HK" and normalized_code.endswith('.HK'):
            normalized_code = normalized_code[:-3].zfill(5)
        return requested_market, normalized_code, requested_market == "CN" and _is_cn_etf_symbol(normalized_code)

    detected_market, normalized_code = _detect_market_and_code(code)
    is_etf = detected_market == "CN" and _is_cn_etf_symbol(normalized_code)
    return detected_market, normalized_code, is_etf


async def _get_or_create_account(user_id: str) -> Dict[str, Any]:
    """获取或创建账户（多货币）"""
    db = get_mongo_db()
    acc = await db["paper_accounts"].find_one({"user_id": user_id})
    if not acc:
        now = datetime.utcnow().isoformat()
        acc = {
            "user_id": user_id,
            # 多货币现金账户
            "cash": {
                "CNY": INITIAL_CASH_BY_MARKET["CNY"],
                "HKD": INITIAL_CASH_BY_MARKET["HKD"],
                "USD": INITIAL_CASH_BY_MARKET["USD"]
            },
            # 多货币已实现盈亏
            "realized_pnl": {
                "CNY": 0.0,
                "HKD": 0.0,
                "USD": 0.0
            },
            # 账户设置
            "settings": {
                "auto_currency_conversion": False,
                "default_market": "CN"
            },
            "created_at": now,
            "updated_at": now,
        }
        await db["paper_accounts"].insert_one(acc)
    else:
        # 兼容旧账户结构：如果 cash 或 realized_pnl 仍为标量，迁移为多货币对象
        updates: Dict[str, Any] = {}
        try:
            cash_val = acc.get("cash")
            if not isinstance(cash_val, dict):
                base_cash = float(cash_val or 0.0)
                updates["cash"] = {"CNY": base_cash, "HKD": 0.0, "USD": 0.0}

            pnl_val = acc.get("realized_pnl")
            if not isinstance(pnl_val, dict):
                base_pnl = float(pnl_val or 0.0)
                updates["realized_pnl"] = {"CNY": base_pnl, "HKD": 0.0, "USD": 0.0}

            if updates:
                updates["updated_at"] = datetime.utcnow().isoformat()
                await db["paper_accounts"].update_one({"user_id": user_id}, {"$set": updates})
                # 重新读取迁移后的账户
                acc = await db["paper_accounts"].find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"❌ 账户结构迁移失败 user_id={user_id}: {e}")

    if acc:
        acc = serialize_document(acc)

    return acc


async def _get_market_rules(market: str) -> Optional[Dict[str, Any]]:
    """获取市场规则配置"""
    db = get_mongo_db()
    rules_doc = await db["paper_market_rules"].find_one({"market": market})
    if rules_doc:
        return rules_doc.get("rules", {})
    return None


def _calculate_commission(market: str, side: str, amount: float, rules: Dict[str, Any]) -> float:
    """计算手续费"""
    if not rules or "commission" not in rules:
        return 0.0

    commission_config = rules["commission"]
    commission = 0.0

    # 佣金
    comm_rate = commission_config.get("rate", 0.0)
    comm_min = commission_config.get("min", 0.0)
    commission += max(amount * comm_rate, comm_min)

    # 印花税（仅卖出）
    if side == "sell" and "stamp_duty_rate" in commission_config:
        commission += amount * commission_config["stamp_duty_rate"]

    # 其他费用（港股）
    if market == "HK":
        if "transaction_levy_rate" in commission_config:
            commission += amount * commission_config["transaction_levy_rate"]
        if "trading_fee_rate" in commission_config:
            commission += amount * commission_config["trading_fee_rate"]
        if "settlement_fee_rate" in commission_config:
            commission += amount * commission_config["settlement_fee_rate"]

    # SEC费用（美股，仅卖出）
    if market == "US" and side == "sell" and "sec_fee_rate" in commission_config:
        commission += amount * commission_config["sec_fee_rate"]

    return round(commission, 2)


async def _get_available_quantity(user_id: str, code: str, market: str) -> int:
    """获取可用数量（考虑T+1限制）"""
    db = get_mongo_db()
    pos = await db["paper_positions"].find_one({"user_id": user_id, "code": code})

    if not pos:
        return 0

    total_qty = pos.get("quantity", 0)

    # A股T+1：今天买入的不能卖出
    if market == "CN":
        # 获取市场规则
        rules = await _get_market_rules(market)
        if rules and rules.get("t_plus", 0) > 0:
            # 查询今天的买入数量
            today = datetime.utcnow().date().isoformat()
            pipeline = [
                {"$match": {
                    "user_id": user_id,
                    "code": code,
                    "side": "buy",
                    "timestamp": {"$gte": today}
                }},
                {"$group": {"_id": None, "total": {"$sum": "$quantity"}}}
            ]
            today_buy = await db["paper_trades"].aggregate(pipeline).to_list(1)
            today_buy_qty = today_buy[0]["total"] if today_buy else 0
            return max(0, total_qty - today_buy_qty)

    # 港股/美股T+0：全部可用
    return total_qty


async def _get_stock_name(code: str, market: str) -> str:
    """
    获取股票名称（支持多市场）

    - A股：从 stock_basic_info 表获取
    - 港股/美股：使用 ForeignStockService.get_quote()（已包含 name 字段）

    Args:
        code: 股票代码
        market: 市场类型 (CN/HK/US)

    Returns:
        股票名称
    """
    try:
        # A股：从 stock_basic_info 获取
        if market in {"CN", "ETF"}:
            db = get_mongo_db()
            code6 = str(code or "").strip().zfill(6)
            stock_info = await db["stock_basic_info"].find_one({
                "$or": [{"code": code}, {"code": code6}, {"symbol": code}, {"symbol": code6}]
            })
            if stock_info and stock_info.get("name"):
                return stock_info.get("name")
            etf_info = await db["etf_basic_info"].find_one({"code": code6}, {"_id": 0, "name": 1})
            if etf_info and etf_info.get("name"):
                return etf_info.get("name")
            return f"A股{code}"

        # 港股/美股：使用 ForeignStockService（复用已获取的行情数据）
        elif market in ["HK", "US"]:
            try:
                from app.services.foreign_stock_service import ForeignStockService
                db = get_mongo_db()
                service = ForeignStockService(db=db)

                # 获取行情（会优先从缓存读取，避免重复API调用）
                quote = await service.get_quote(market, code, force_refresh=False)

                if quote and quote.get("name"):
                    name = quote.get("name")
                    # 避免返回默认格式的名称
                    if not name.startswith("港股") and not name.startswith("美股"):
                        return name

                return f"{'港股' if market == 'HK' else '美股'}{code}"
            except Exception as e:
                logger.warning(f"⚠️ {market}股名称获取失败 {code}: {e}")
                return f"{'港股' if market == 'HK' else '美股'}{code}"

        else:
            return f"股票{code}"

    except Exception as e:
        logger.error(f"❌ 获取股票名称失败 {code} ({market}): {e}")
        return f"股票{code}"


async def _load_cn_candidates(
    db, code: str, code6: str, now: datetime, mode: str
) -> Dict[str, Optional[Dict[str, Any]]]:
    """只负责取数，判定交给 quote_contract.resolve_cn_*（取数与判定分开才好测）。"""
    projection = {
        "_id": 0, "close": 1, "high": 1, "low": 1, "open": 1, "trade_date": 1,
        "updated_at": 1, "quote_time": 1, "quote_time_source": 1,
        "fetched_at": 1, "snapshot_interval_seconds": 1,
    }
    code_filter = {"$or": [{"code": code}, {"code": code6}, {"symbol": code}, {"symbol": code6}]}
    candidates: Dict[str, Optional[Dict[str, Any]]] = {
        "market_quotes": await db["market_quotes"].find_one(code_filter, projection),
        "stock_basic_info": None,
        "etf_daily_quotes": None,
        "stock_daily_quotes": None,
    }

    if mode == "display":
        candidates["stock_basic_info"] = await db["stock_basic_info"].find_one(
            code_filter, {"_id": 0, "current_price": 1, "updated_at": 1}
        )

    if mode == "display":
        daily_query: Dict[str, Any] = {"symbol": code6, "period": "daily"}
    else:
        daily_query = {"symbol": code6, "period": "daily", "trade_date": now.date().isoformat()}

    candidates["etf_daily_quotes"] = await db["etf_daily_quotes"].find_one(
        {"code": code6},
        {"_id": 0, "high": 1, "low": 1, "open": 1, "close": 1, "trade_date": 1, "updated_at": 1},
        sort=[("trade_date", -1)],
    )
    candidates["stock_daily_quotes"] = await db["stock_daily_quotes"].find_one(
        daily_query,
        {"_id": 0, "high": 1, "low": 1, "open": 1, "close": 1, "trade_date": 1, "updated_at": 1},
        sort=[("trade_date", -1)],
    )
    return candidates


async def _get_last_price_detail(
    code: str, market: str, strict: bool = True
) -> Tuple[Optional[float], Optional[Dict[str, Any]], str]:
    """返回 (价格, 与价格同源的依据, 原因码)。

    价格与依据出自**同一条记录**，不再成交后二次查库补说明（并发下会 A 价配 B 说明）。
    """
    db = get_mongo_db()

    if market in {"CN", "ETF"}:
        code6 = str(code or "").strip().zfill(6)
        now = cn_now()
        mode = "strict" if strict else "display"
        candidates = await _load_cn_candidates(db, code, code6, now, mode)
        return resolve_cn_price(candidates, now, strict=strict)

    if market in {"HK", "US"}:
        try:
            from app.services.foreign_stock_service import ForeignStockService

            service = ForeignStockService(db=db)
            quote = await service.get_quote(market, code, force_refresh=False)
            if quote:
                price, _ = validate_price(
                    quote.get("price") or quote.get("current_price") or quote.get("close")
                )
                if price is None:
                    return None, None, "price_invalid"
                basis = {
                    "source": "foreign_stock_service",
                    "channel": "external",
                    "as_of": None,
                    "trade_date": None,
                    "delay_seconds": None,
                    "max_delay_seconds": None,
                    "notice": "模拟成交按外部行情服务返回的最新价定价；未提供行情时间，可能存在延迟。",
                }
                return price, basis, "ok"
            return None, None, "quote_missing"
        except Exception as e:
            logger.error(f"❌ 获取{market}股价格失败 {code}: {e}")
            return None, None, f"foreign_error:{e}"

    return None, None, "market_unsupported"


async def _get_last_price(code: str, market: str, strict: bool = False) -> Optional[float]:
    """兼容旧调用：只返回价格。需要依据时用 _get_last_price_detail。"""
    price, _basis, reason = await _get_last_price_detail(code, market, strict=strict)
    if price is None:
        logger.warning(f"⚠️ 取价失败 {code} ({market}, strict={strict}): {reason}")
    return price


async def _get_today_price_range_detail(
    code: str, market: str
) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, Any]], str]:
    """返回 (区间, 与区间同源的依据, 原因码)。用于指定价格单的校验。"""
    db = get_mongo_db()

    if market in {"CN", "ETF"}:
        code6 = str(code or "").strip().zfill(6)
        now = cn_now()
        candidates = await _load_cn_candidates(db, code, code6, now, "strict")
        return resolve_cn_range(candidates, now)

    if market in {"HK", "US"}:
        try:
            from app.services.foreign_stock_service import ForeignStockService

            service = ForeignStockService(db=db)
            quote = await service.get_quote(market, code, force_refresh=False)
            if quote:
                high, _ = validate_price(quote.get("high") or quote.get("day_high"))
                low, _ = validate_price(quote.get("low") or quote.get("day_low"))
                if high is not None and low is not None and low <= high:
                    basis = {
                        "source": "foreign_stock_service",
                        "channel": "external",
                        "as_of": None,
                        "trade_date": None,
                        "delay_seconds": None,
                        "max_delay_seconds": None,
                        "notice": "价格区间取自外部行情服务；未提供行情时间，可能存在延迟。",
                    }
                    return {"high": high, "low": low}, basis, "ok"
                return None, None, "range_invalid"
            return None, None, "quote_missing"
        except Exception as e:
            logger.error(f"❌ 获取{market}股价格区间失败 {code}: {e}")
            return None, None, f"foreign_error:{e}"

    return None, None, "market_unsupported"


async def _get_today_price_range(code: str, market: str) -> Optional[Dict[str, float]]:
    """兼容旧调用：只返回区间。"""
    price_range, _basis, reason = await _get_today_price_range_detail(code, market)
    if price_range is None:
        logger.warning(f"⚠️ 无法获取当日价格区间 {code} ({market}): {reason}")
    return price_range


def _build_market_notice(basis: Optional[Dict[str, Any]]) -> Optional[str]:
    return basis.get("notice") if isinstance(basis, dict) else None


def _format_hhmmss(value: Any) -> str:
    text = str(value or "")
    return text[11:19] if len(text) >= 19 else "未知时刻"


def _build_limit_notice(
    price: float, basis: Optional[Dict[str, Any]], high: float, low: float
) -> str:
    """指定价单的告知：说明「区间」来自哪条依据，以及该依据的时效边界。

    成交价是用户给的，所以不能照抄市价单的「按快照定价」；这里描述的是区间校验依据。
    """
    basis = basis or {}
    channel = basis.get("channel")
    span = f"区间 {low:.2f}–{high:.2f}"

    if channel == "daily_close":
        day = basis.get("trade_date") or "当日"
        return f"按你指定的价格 {price:.2f} 成交；{span} 取自 {day} 收盘日线校验。"

    stamp = _format_hhmmss(basis.get("as_of"))
    if channel == "upstream":
        return f"按你指定的价格 {price:.2f} 成交；{span} 取自上游行情时间 {stamp} 的行情校验。"

    minutes = int((basis.get("max_delay_seconds") or 0) // 60)
    elapsed = int(max(0, basis.get("delay_seconds") or 0) // 60)
    return (
        f"按你指定的价格 {price:.2f} 成交；{span} 取自 {stamp} 采集的快照校验，"
        f"距采集已 {elapsed} 分钟，采集后有效期为 {minutes} 分钟。"
        f"上游报价时间未知，可能存在额外延迟。"
    )


def _zfill_code(code: str) -> str:
    s = str(code).strip()
    if len(s) == 6 and s.isdigit():
        return s
    return s.zfill(6)


@router.get("/account", response_model=dict)
async def get_account(current_user: dict = Depends(get_current_user)):
    """获取或创建纸上账户，返回资金与持仓估值汇总（支持多市场）"""
    db = get_mongo_db()
    acc = await _get_or_create_account(current_user["id"])

    # 聚合持仓估值（按货币分类）
    positions = await db["paper_positions"].find({"user_id": current_user["id"]}).to_list(None)

    positions_value_by_currency = {
        "CNY": 0.0,
        "HKD": 0.0,
        "USD": 0.0
    }

    detailed_positions: List[Dict[str, Any]] = []
    for p in positions:
        code = p.get("code")
        name = p.get("name", "")  # 从数据库获取股票名称
        market = p.get("market", "CN")
        currency = p.get("currency", "CNY")
        qty = int(p.get("quantity", 0))
        avg_cost = float(p.get("avg_cost", 0.0))
        available_qty = p.get("available_qty", qty)

        # 获取最新价
        last = await _get_last_price(code, market)
        mkt_value = round((last or 0.0) * qty, 2)
        positions_value_by_currency[currency] += mkt_value

        detailed_positions.append({
            "code": code,
            "name": name,  # 添加股票名称到响应
            "market": market,
            "currency": currency,
            "quantity": qty,
            "available_qty": available_qty,
            "avg_cost": avg_cost,
            "last_price": last,
            "market_value": mkt_value,
            "unrealized_pnl": None if last is None else round((last - avg_cost) * qty, 2)
        })

    # 计算总资产（按货币分别显示）
    cash = acc.get("cash", {})
    realized_pnl = acc.get("realized_pnl", {})

    # 兼容旧格式（单一现金）
    if not isinstance(cash, dict):
        cash = {"CNY": float(cash), "HKD": 0.0, "USD": 0.0}
    if not isinstance(realized_pnl, dict):
        realized_pnl = {"CNY": float(realized_pnl), "HKD": 0.0, "USD": 0.0}

    summary = {
        "cash": {
            "CNY": round(float(cash.get("CNY", 0.0)), 2),
            "HKD": round(float(cash.get("HKD", 0.0)), 2),
            "USD": round(float(cash.get("USD", 0.0)), 2)
        },
        "realized_pnl": {
            "CNY": round(float(realized_pnl.get("CNY", 0.0)), 2),
            "HKD": round(float(realized_pnl.get("HKD", 0.0)), 2),
            "USD": round(float(realized_pnl.get("USD", 0.0)), 2)
        },
        "positions_value": positions_value_by_currency,
        "equity": {
            "CNY": round(float(cash.get("CNY", 0.0)) + positions_value_by_currency["CNY"], 2),
            "HKD": round(float(cash.get("HKD", 0.0)) + positions_value_by_currency["HKD"], 2),
            "USD": round(float(cash.get("USD", 0.0)) + positions_value_by_currency["USD"], 2)
        },
        "updated_at": acc.get("updated_at"),
    }

    return ok({"account": summary, "positions": detailed_positions})


@router.post("/order", response_model=dict)
async def place_order(payload: PlaceOrderRequest, current_user: dict = Depends(get_current_user)):
    """提交市价单，按最新价即时成交（支持多市场）"""
    db = get_mongo_db()

    # 1. 识别市场类型
    market, normalized_code, is_etf = _resolve_market_and_code(payload.code, payload.market)

    side = payload.side
    qty = int(payload.quantity)
    analysis_id = payload.analysis_id if hasattr(payload, "analysis_id") else None
    # 🔥 修复：直接从 Pydantic 模型获取 price 字段（Pydantic 模型可以直接访问字段）
    specified_price = payload.price
    
    logger.info(f"📊 [模拟交易下单] 收到请求: code={normalized_code}, market={market}, is_etf={is_etf}, side={side}, qty={qty}, specified_price={specified_price}")

    # 2. 确定货币
    currency_map = {
        "CN": "CNY",
        "HK": "HKD",
        "US": "USD"
    }
    currency = currency_map.get(market, "CNY")

    # 3. 获取账户
    acc = await _get_or_create_account(current_user["id"])

    # 4. 获取价格（价格与其定价依据出自同一条记录，不再二次查库）
    pricing_basis: Optional[Dict[str, Any]] = None
    pricing_notice: Optional[str] = None

    if specified_price is not None and specified_price > 0:
        # 指定价格单：区间只用于校验，成交价是用户给的价格
        price_range, range_basis, range_reason = await _get_today_price_range_detail(
            normalized_code, market
        )
        if not price_range:
            logger.warning(f"⚠️ [模拟交易] 区间校验失败 {normalized_code}: {range_reason}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"行情缺失、过期或无效，暂不可校验价格，请稍后再试（{normalized_code}）。"
                    f"具体原因见服务端日志"
                )
            )

        high = price_range.get("high", 0.0)
        low = price_range.get("low", 0.0)

        if specified_price < low or specified_price > high:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"指定价格 {specified_price:.2f} 超出当天价格区间 [{low:.2f}, {high:.2f}]，模拟交易不能成功"
            )

        price = float(specified_price)
        pricing_basis = range_basis
        pricing_notice = _build_limit_notice(price, range_basis, high, low)
        logger.info(f"✅ [模拟交易] 使用指定价格: {normalized_code} = {price:.2f} (区间: [{low:.2f}, {high:.2f}])")
    else:
        # 市价单：成交价必须是可证明「属于今天且可用于成交」的行情
        price, pricing_basis, price_reason = await _get_last_price_detail(
            normalized_code, market, strict=True
        )
        if price is None:
            logger.warning(f"⚠️ [模拟交易] 市价单取价失败 {normalized_code}: {price_reason}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"行情缺失、过期或无效，暂不可下单（{normalized_code}）。"
                    f"具体原因见服务端日志"
                )
            )
        pricing_notice = _build_market_notice(pricing_basis)
        logger.info(f"✅ [模拟交易] 使用实时价格: {normalized_code} = {price:.2f}")

    # 4.1 记录定价依据（来源 / 时间 / 采集后有效期），与成交同源
    if pricing_basis:
        logger.info(
            "📌 [模拟交易] 定价依据: source=%s channel=%s as_of=%s delay=%ss 有效期=%ss",
            pricing_basis.get("source"),
            pricing_basis.get("channel"),
            pricing_basis.get("as_of"),
            pricing_basis.get("delay_seconds"),
            pricing_basis.get("max_delay_seconds"),
        )
    if pricing_notice:
        logger.warning("⚠️ [模拟交易] %s", pricing_notice)

    # 5. 计算金额
    notional = round(price * qty, 2)

    # 6. 获取市场规则并计算手续费
    rules = await _get_market_rules(market)
    commission = _calculate_commission(market, side, notional, rules) if rules else 0.0
    total_cost = notional + commission

    # 7. 获取持仓
    pos = await db["paper_positions"].find_one({"user_id": current_user["id"], "code": normalized_code})

    now_iso = datetime.utcnow().isoformat()
    realized_pnl_delta = 0.0

    # 8. 执行买卖逻辑
    if side == "buy":
        # 资金检查（使用对应货币的账户）
        cash = acc.get("cash", {})
        if isinstance(cash, dict):
            available_cash = float(cash.get(currency, 0.0))
        else:
            # 兼容旧格式
            available_cash = float(cash) if currency == "CNY" else 0.0

        if available_cash < total_cost:
            raise HTTPException(
                status_code=400,
                detail=f"可用{currency}不足：需要 {total_cost:.2f}，可用 {available_cash:.2f}"
            )

        # 扣除资金（从对应货币账户）
        new_cash = round(available_cash - total_cost, 2)
        await db["paper_accounts"].update_one(
            {"user_id": current_user["id"]},
            {"$set": {f"cash.{currency}": new_cash, "updated_at": now_iso}}
        )

        # 更新/创建持仓：加权平均成本
        if not pos:
            # 获取股票名称（根据市场类型）
            stock_name = await _get_stock_name(normalized_code, market)

            new_pos = {
                "user_id": current_user["id"],
                "code": normalized_code,
                "name": stock_name,  # 添加股票名称
                "market": market,
                "currency": currency,
                "quantity": qty,
                "available_qty": qty if market != "CN" else 0,  # A股T+1，今天买入不可用
                "frozen_qty": 0,
                "avg_cost": price,
                "updated_at": now_iso
            }
            await db["paper_positions"].insert_one(new_pos)
        else:
            old_qty = int(pos.get("quantity", 0))
            old_cost = float(pos.get("avg_cost", 0.0))
            new_qty = old_qty + qty
            new_avg = round((old_cost * old_qty + price * qty) / new_qty, 4) if new_qty > 0 else price

            # A股T+1：新买入的不可用
            if market == "CN":
                new_available = pos.get("available_qty", old_qty)  # 保持原有可用数量
            else:
                new_available = new_qty  # 港股/美股T+0，全部可用

            await db["paper_positions"].update_one(
                {"_id": pos["_id"]},
                {"$set": {
                    "quantity": new_qty,
                    "available_qty": new_available,
                    "avg_cost": new_avg,
                    "updated_at": now_iso
                }}
            )

    else:  # sell
        # 检查可用数量（考虑T+1）
        available_qty = await _get_available_quantity(current_user["id"], normalized_code, market)
        if available_qty < qty:
            raise HTTPException(
                status_code=400,
                detail=f"可用持仓不足：需要 {qty}，可用 {available_qty}"
            )

        old_qty = int(pos.get("quantity", 0))
        avg_cost = float(pos.get("avg_cost", 0.0))
        new_qty = old_qty - qty
        pnl = round((price - avg_cost) * qty, 2)
        realized_pnl_delta = pnl

        # 卖出收入（加到对应货币账户，扣除手续费）
        net_proceeds = notional - commission
        await db["paper_accounts"].update_one(
            {"user_id": current_user["id"]},
            {
                "$inc": {
                    f"cash.{currency}": net_proceeds,
                    f"realized_pnl.{currency}": realized_pnl_delta
                },
                "$set": {"updated_at": now_iso}
            }
        )

        # 更新持仓
        if new_qty == 0:
            await db["paper_positions"].delete_one({"_id": pos["_id"]})
        else:
            new_available = max(0, pos.get("available_qty", old_qty) - qty)
            await db["paper_positions"].update_one(
                {"_id": pos["_id"]},
                {"$set": {
                    "quantity": new_qty,
                    "available_qty": new_available,
                    "updated_at": now_iso
                }}
            )

    # 9. 记录订单与成交（即成）
    order_doc = {
        "user_id": current_user["id"],
        "code": normalized_code,
        "market": market,
        "currency": currency,
        "side": side,
        "quantity": qty,
        "price": price,
        "amount": notional,
        "commission": commission,
        "status": "filled",
        "created_at": now_iso,
        "filled_at": now_iso,
        "pricing_basis": pricing_basis,
    }
    if analysis_id:
        order_doc["analysis_id"] = analysis_id
    await db["paper_orders"].insert_one(order_doc)

    trade_doc = {
        "user_id": current_user["id"],
        "code": normalized_code,
        "market": market,
        "currency": currency,
        "side": side,
        "quantity": qty,
        "price": price,
        "amount": notional,
        "commission": commission,
        "pnl": realized_pnl_delta if side == "sell" else 0.0,
        "timestamp": now_iso,
    }
    if analysis_id:
        trade_doc["analysis_id"] = analysis_id
    await db["paper_trades"].insert_one(trade_doc)

    order_payload = {k: v for k, v in order_doc.items() if k != "_id"}
    response_payload: Dict[str, Any] = {"order": order_payload}
    if pricing_notice:
        response_payload["pricing_notice"] = pricing_notice
    return ok(response_payload)


@router.get("/positions", response_model=dict)
async def list_positions(current_user: dict = Depends(get_current_user)):
    """获取持仓列表（支持多市场）"""
    db = get_mongo_db()
    items = await db["paper_positions"].find({"user_id": current_user["id"]}).to_list(None)
    enriched: List[Dict[str, Any]] = []
    for p in items:
        code = p.get("code")
        name = p.get("name", "")  # 从数据库获取股票名称
        market = p.get("market", "CN")
        currency = p.get("currency", "CNY")
        qty = int(p.get("quantity", 0))
        available_qty = p.get("available_qty", qty)
        avg_cost = float(p.get("avg_cost", 0.0))

        last = await _get_last_price(code, market)
        mkt = round((last or 0.0) * qty, 2)
        enriched.append({
            "code": code,
            "name": name,  # 添加股票名称到响应
            "market": market,
            "currency": currency,
            "quantity": qty,
            "available_qty": available_qty,
            "avg_cost": avg_cost,
            "last_price": last,
            "market_value": mkt,
            "unrealized_pnl": None if last is None else round((last - avg_cost) * qty, 2)
        })
    return ok({"items": enriched})


@router.get("/orders", response_model=dict)
async def list_orders(limit: int = Query(50, ge=1, le=200), current_user: dict = Depends(get_current_user)):
    db = get_mongo_db()
    cursor = db["paper_orders"].find({"user_id": current_user["id"]}).sort("created_at", -1).limit(limit)
    items = await cursor.to_list(None)
    # 去除 _id
    cleaned = [{k: v for k, v in it.items() if k != "_id"} for it in items]
    return ok({"items": cleaned})


@router.get("/quote/{code}", response_model=dict)
async def get_quote(
    code: str,
    market: Optional[str] = Query(None, description="市场类型 (CN/ETF/HK/US)，不传则自动识别"),
    current_user: dict = Depends(get_current_user)
):
    """获取股票价格信息（当前价格和当天价格区间）"""
    # 1. 识别市场类型
    market, normalized_code, is_etf = _resolve_market_and_code(code, market)
    
    # 2. 获取当前价格
    current_price = await _get_last_price(normalized_code, market)
    
    # 3. 获取当天价格区间
    price_range = await _get_today_price_range(normalized_code, market)
    
    return ok({
        "code": normalized_code,
        "market": "ETF" if is_etf else market,
        "current_price": current_price,
        "high": price_range.get("high") if price_range else None,
        "low": price_range.get("low") if price_range else None
    })


class InitializeAccountRequest(BaseModel):
    """初始化账户请求（设置初始金额）"""
    initial_cash: Dict[str, float] = Field(
        default_factory=lambda: {
            "CNY": INITIAL_CASH_BY_MARKET["CNY"],
            "HKD": INITIAL_CASH_BY_MARKET["HKD"],
            "USD": INITIAL_CASH_BY_MARKET["USD"]
        },
        description="各市场的初始资金（CNY/HKD/USD）"
    )


class ResetAccountRequest(BaseModel):
    """重置账户请求"""
    initial_cash: Optional[Dict[str, float]] = Field(
        None,
        description="重置后的初始资金（CNY/HKD/USD），如果不提供则使用默认值"
    )


@router.post("/initialize", response_model=dict)
async def initialize_account(
    request: InitializeAccountRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    初始化账户（设置初始金额）
    
    如果账户已存在，会更新初始金额（清空持仓和订单，重置为指定金额）
    如果账户不存在，会创建新账户
    """
    db = get_mongo_db()
    user_id = current_user["id"]
    
    # 验证金额
    initial_cash = request.initial_cash
    for currency, amount in initial_cash.items():
        if amount < 0:
            raise HTTPException(status_code=400, detail=f"{currency} 初始金额不能为负数")
    
    # 检查账户是否存在
    existing_acc = await db["paper_accounts"].find_one({"user_id": user_id})
    
    if existing_acc:
        # 账户已存在，清空持仓和订单，重置为指定金额
        await db["paper_positions"].delete_many({"user_id": user_id})
        await db["paper_orders"].delete_many({"user_id": user_id})
        await db["paper_trades"].delete_many({"user_id": user_id})
        
        now = datetime.utcnow().isoformat()
        update_data = {
            "cash": initial_cash,
            "realized_pnl": {
                "CNY": 0.0,
                "HKD": 0.0,
                "USD": 0.0
            },
            "updated_at": now
        }
        await db["paper_accounts"].update_one(
            {"user_id": user_id},
            {"$set": update_data}
        )
        logger.info(f"✅ 账户已初始化: user_id={user_id}, initial_cash={initial_cash}")
    else:
        # 账户不存在，创建新账户
        now = datetime.utcnow().isoformat()
        acc = {
            "user_id": user_id,
            "cash": initial_cash,
            "realized_pnl": {
                "CNY": 0.0,
                "HKD": 0.0,
                "USD": 0.0
            },
            "settings": {
                "auto_currency_conversion": False,
                "default_market": "CN"
            },
            "created_at": now,
            "updated_at": now,
        }
        await db["paper_accounts"].insert_one(acc)
        logger.info(f"✅ 账户已创建: user_id={user_id}, initial_cash={initial_cash}")
    
    # 返回更新后的账户
    acc = await _get_or_create_account(user_id)
    return ok({
        "message": "账户已初始化",
        "cash": acc.get("cash", {}),
        "account": acc
    })


@router.post("/reset", response_model=dict)
async def reset_account(
    confirm: bool = Query(False),
    request: Optional[ResetAccountRequest] = Body(None),
    current_user: dict = Depends(get_current_user)
):
    """
    重置账户（支持多货币和自定义初始金额）
    
    如果提供了 initial_cash，则使用指定的金额；否则使用默认金额
    """
    if not confirm:
        raise HTTPException(status_code=400, detail="请设置 confirm=true 以确认重置")
    
    db = get_mongo_db()
    user_id = current_user["id"]
    
    # 确定重置后的初始金额
    if request and request.initial_cash:
        initial_cash = request.initial_cash
        # 验证金额
        for currency, amount in initial_cash.items():
            if amount < 0:
                raise HTTPException(status_code=400, detail=f"{currency} 初始金额不能为负数")
    else:
        # 使用默认金额
        initial_cash = INITIAL_CASH_BY_MARKET.copy()
    
    # 清空所有数据
    await db["paper_accounts"].delete_many({"user_id": user_id})
    await db["paper_positions"].delete_many({"user_id": user_id})
    await db["paper_orders"].delete_many({"user_id": user_id})
    await db["paper_trades"].delete_many({"user_id": user_id})
    
    # 重新创建账户（使用指定的初始金额）
    now = datetime.utcnow().isoformat()
    acc = {
        "user_id": user_id,
        "cash": initial_cash,
        "realized_pnl": {
            "CNY": 0.0,
            "HKD": 0.0,
            "USD": 0.0
        },
        "settings": {
            "auto_currency_conversion": False,
            "default_market": "CN"
        },
        "created_at": now,
        "updated_at": now,
    }
    await db["paper_accounts"].insert_one(acc)
    
    logger.info(f"✅ 账户已重置: user_id={user_id}, initial_cash={initial_cash}")
    return ok({"message": "账户已重置", "cash": acc.get("cash", {})})


# ==================== 市场规则配置 API ====================

@router.get("/market-rules")
async def get_market_rules():
    """获取所有市场的交易规则配置"""
    db = get_mongo_db()
    rules = await db["paper_market_rules"].find({}, {"_id": 0}).to_list(None)
    if not rules:
        # 如果没有配置，返回默认值（scripts 目录在社区版被剔除，缺失时返回空并提示初始化）
        try:
            from scripts.init_paper_trading_market_rules import MARKET_RULES
            return ok(MARKET_RULES)
        except ImportError:
            return ok([])
    return ok(rules)


class CommissionUpdateRequest(BaseModel):
    rules: List[Dict[str, Any]] = Field(..., description="各市场的佣金配置")


@router.post("/market-rules/commission")
async def update_market_rules_commission(req: CommissionUpdateRequest):
    """更新各市场的佣金配置"""
    db = get_mongo_db()
    updated = 0
    for item in req.rules:
        market = item.get("market")
        commission = item.get("commission", {})
        if not market:
            continue
        # 只更新 commission 部分，保留其他规则
        result = await db["paper_market_rules"].update_one(
            {"market": market},
            {"$set": {f"rules.commission.{k}": v for k, v in commission.items()}}
        )
        if result.modified_count > 0:
            updated += 1
        elif result.matched_count == 0:
            # 市场规则不存在，创建基础记录
            logger.warning(f"市场 {market} 规则不存在，跳过佣金配置更新")

    logger.info(f"✅ 更新佣金配置: {updated} 个市场")
    return ok({"message": f"已更新 {updated} 个市场的佣金配置", "updated": updated})