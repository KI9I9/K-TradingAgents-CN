"""行情时间与字段契约（纯标准库，可离线测试）。

为什么单独抽一个模块：
- 供给侧（写入）与消费侧（下单）必须用**同一套**时间/日期判定，否则两边各写一份，
  迟早出现「写的时候认为是今天、读的时候认为是昨天」的分歧。
- 本模块不依赖 pandas / pymongo / fastapi，可在无第三方包的机器上直接跑行为测试。

四条硬约定：
1. **时间三态**：BSON date（pymongo 默认解出 naive UTC）、ISO 字符串（`+08:00` / `Z`）、
   缺失或非法。前两者可解析；第三者一律**拒绝**——放行等于护栏失效。
2. **行情时间 ≠ 采集时间**。`quote_time` 是上游声明的行情发生时间；`fetched_at` 是我们
   拉取的时刻。AKShare 快照接口不提供逐行行情时间，因此 `quote_time` 为 None，
   `quote_time_source="fetch"`。严格成交路径默认**不接受**这种弱证据（见
   `ALLOW_FETCH_TIME_AS_QUOTE_TIME`）——用「我刚拉过」冒充「报价很新」是本轮评审
   明确否决的做法。
3. **完整快照才可写**。成交要用的 close/high/low 必须同一次载荷齐全且有效；不完整载荷
   **整条拒写**，既不刷新旧价格，也不刷新旧价格的日期与时间。
4. **交易所时区固定 UTC+8**，不跟随进程 TZ、不跟随展示时区。A 股无夏令时。
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

# A 股交易所时区。固定 UTC+8，中国不实行夏令时。
CN_TZ = timezone(timedelta(hours=8))

# 连续竞价 09:30-11:30 / 13:00-15:00；15:00-15:30 为收盘缓冲（等收盘价落库）。
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(15, 30)

# 报价允许的最大陈旧时长（作用于 quote_time，不是采集时间）。
DEFAULT_MAX_QUOTE_AGE_SECONDS = 30 * 60
# 允许的时钟正向漂移。
DEFAULT_MAX_FUTURE_SECONDS = 60

# 严格成交路径是否允许把「采集时间」当作行情时间。
#   False（默认）：拿不到上游行情时间就拒绝成交 —— 评审要求「不得用当前时间代替证据」。
#   True：退化为「采集时刻 + 会话内」的弱保证，仅用于本地联调，生产不建议。
ALLOW_FETCH_TIME_AS_QUOTE_TIME = False

# 日线回退：必须在收盘后，且该日线本身是收盘后写入的（完成状态代理）。
DAILY_BAR_FINAL_AFTER = time(15, 0)
DAILY_FALLBACK_AFTER = time(15, 5)

# 成交必须齐全的价格字段
REQUIRED_PRICE_FIELDS = ("close", "high", "low")

# 这些字段为 None 时不得写入（除成交必需字段外，其余只是不覆盖已有值）
PROTECTED_FIELDS = (
    "open", "high", "low", "close", "pre_close",
    "price", "current_price", "pct_chg", "change", "change_percent",
    "volume", "amount",
)

# 批量接口历史字段名 → 标准字段名
ALIAS_TO_STANDARD = (("open", "open_price"), ("high", "high_price"), ("low", "low_price"))


def cn_now() -> datetime:
    """当前交易所时间（aware）。"""
    return datetime.now(CN_TZ)


# --------------------------------------------------------------------------- #
# 时间解析
# --------------------------------------------------------------------------- #
def parse_quote_time(value: Any) -> Optional[datetime]:
    """把时间统一解析为 aware datetime；无法解析返回 None。

    支持：
    - `datetime`：naive 视为 **UTC**（pymongo 默认 BSON 解码就是这样），aware 原样返回
    - `str`：ISO 8601（含 `Z`、`+08:00`、无时区）、`YYYY-MM-DD HH:MM:SS`
      * 无时区字符串按**交易所时区**解释
    - 其他类型（None / 数字 / bool / list / dict）→ None，调用方必须拒绝
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed: Optional[datetime] = None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=CN_TZ)
        return parsed

    return None


def coerce_trade_date(value: Any) -> Optional[str]:
    """把交易日归一化为 `YYYY-MM-DD`；无法识别返回 None。"""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10 and text[4] in "-/" and text[7] in "-/":
            return text.replace("/", "-")
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return None


def is_same_trade_date(value: Any, today: date) -> bool:
    """行情所属交易日是否等于 `today`（交易所时区下的日期）。"""
    return coerce_trade_date(value) == today.isoformat()


# --------------------------------------------------------------------------- #
# 数值校验
# --------------------------------------------------------------------------- #
def validate_price(value: Any) -> Tuple[Optional[float], str]:
    """单个价格：必须是有限的正数。bool 不是价格。"""
    if isinstance(value, bool):
        return None, "bool_not_price"
    if value is None:
        return None, "missing"
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None, "not_numeric"
    if not math.isfinite(price):
        return None, "not_finite"
    if price <= 0:
        return None, "non_positive"
    return price, "ok"


def validate_price_range(quote: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], str]:
    """校验并返回 (high, low)。任何一项不合法都返回 (None, None, 原因)。"""
    if not quote:
        return None, None, "quote_missing"
    high, high_reason = validate_price(quote.get("high"))
    if high is None:
        return None, None, f"high_{high_reason}"
    low, low_reason = validate_price(quote.get("low"))
    if low is None:
        return None, None, f"low_{low_reason}"
    if low > high:
        return None, None, "low_gt_high"
    return high, low, "ok"


# --------------------------------------------------------------------------- #
# 消费侧判定
# --------------------------------------------------------------------------- #
def quote_is_usable(
    quote: Dict[str, Any],
    now: datetime,
    max_age_seconds: int = DEFAULT_MAX_QUOTE_AGE_SECONDS,
    max_future_seconds: int = DEFAULT_MAX_FUTURE_SECONDS,
    require_trade_date: bool = True,
) -> Tuple[bool, str]:
    """判断一条行情能否用于「按今天的价格成交」。

    只看 `quote_time`（上游行情时间）。`fetched_at` / `updated_at` 是采集时间，
    默认**不作为**新鲜度证据。
    """
    if not quote:
        return False, "quote_missing"

    if require_trade_date and not is_same_trade_date(quote.get("trade_date"), now.date()):
        return False, f"trade_date_mismatch:{quote.get('trade_date')}"

    quote_time = parse_quote_time(quote.get("quote_time"))
    if quote_time is None:
        if not ALLOW_FETCH_TIME_AS_QUOTE_TIME:
            return False, "quote_time_missing"
        quote_time = parse_quote_time(quote.get("fetched_at") or quote.get("updated_at"))
        if quote_time is None:
            return False, "quote_time_and_fetch_time_missing"

    age = (now - quote_time).total_seconds()
    if age > max_age_seconds:
        return False, f"quote_stale:{int(age)}s"
    if age < -max_future_seconds:
        return False, f"quote_from_future:{int(-age)}s"
    return True, "ok"


def daily_fallback_allowed(now: datetime) -> Tuple[bool, str]:
    """日线回退的时间闸门：只在收盘之后。

    盘中若存在当日未完成日线，用它当成交价会拿到残缺区间，因此一律不允许。
    """
    if now.time() < DAILY_FALLBACK_AFTER:
        return False, "daily_fallback_before_close"
    return True, "ok"


def daily_bar_is_final(bar: Dict[str, Any], now: datetime) -> Tuple[bool, str]:
    """判断一根当日日线是否「已收盘、可用于成交」。

    - trade_date 必须是今日
    - 该行必须在收盘后写入（updated_at 的本地时间 >= 15:00）—— 这是「完成状态」的
      可用代理：09:35 写入的当日行必然是盘中残缺数据
    - OHLC 必须齐全且有效
    """
    if not bar:
        return False, "bar_missing"
    if not is_same_trade_date(bar.get("trade_date"), now.date()):
        return False, f"bar_trade_date_mismatch:{bar.get('trade_date')}"

    written_at = parse_quote_time(bar.get("updated_at"))
    if written_at is None:
        return False, "bar_write_time_missing"
    local_written = written_at.astimezone(CN_TZ)
    if local_written.date() != now.date():
        return False, f"bar_written_other_day:{local_written.date()}"
    if local_written.time() < DAILY_BAR_FINAL_AFTER:
        return False, f"bar_written_intraday:{local_written.strftime('%H:%M')}"

    high, low, reason = validate_price_range(bar)
    if high is None:
        return False, f"bar_range_invalid:{reason}"
    close, close_reason = validate_price(bar.get("close"))
    if close is None:
        return False, f"bar_close_invalid:{close_reason}"
    return True, "ok"


# --------------------------------------------------------------------------- #
# 供给侧判定
# --------------------------------------------------------------------------- #
def sync_round_skip_reason(
    now: datetime,
    is_today_trade_day: bool,
    calendar_source: Optional[str] = None,
) -> Optional[str]:
    """本轮行情同步是否整轮跳过。返回 None 表示可以写。"""
    if not is_today_trade_day:
        return "not_trade_day"
    # API 全失败时会退回「普通工作日」，无法区分节假日与交易日 —— 宁可整轮不写。
    if calendar_source not in ("tushare", "akshare"):
        return "calendar_unverified"
    local_time = now.time()
    if local_time < SESSION_OPEN:
        return "pre_open"
    if local_time > SESSION_CLOSE:
        return "after_close"
    return None


def resolve_trade_date(now: datetime) -> str:
    """本轮写入使用的交易日（仅在闸门放行后调用）。"""
    return now.date().isoformat()


def build_failure_stats(reason: str, context: str) -> Dict[str, Any]:
    """构造调度层能识别为 failed 的结果。

    `scheduler_runtime.outcome()` 对 dict 只看 `success is False`；
    `unified_thread_pool_sync_service` 在方法不抛异常时无条件 `success = True`，
    所以**必须**由被调方显式给出 `success=False`，否则空跑会被记成成功。
    """
    return {
        "success": False,
        "error": reason,
        "error_count": 1,
        "errors": [{"error": reason, "context": context}],
    }


def _normalize_volume(value: Any, unit: Optional[str]) -> Any:
    """把成交量统一为「手」。

    库内标准是手（批量快照天然是手）。单只接口历史上把「总手」乘了 100 变成股，
    这里按 `volume_unit` 显式换算，避免 100 倍偏差。
    """
    if value is None:
        return None
    if (unit or "lot").lower() in ("share", "shares", "股"):
        try:
            return float(value) / 100.0
        except (TypeError, ValueError):
            return value
    return value


def prepare_quote_write(
    quotes_data: Dict[str, Any],
    symbol: str,
    trade_date: Optional[str],
    now: datetime,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """把上游字典规范化为「可提交写入」的文档。

    返回 (doc, reason)。reason != "ok" 时 doc 为 None，调用方**不得写库**。

    关键点（对应评审 P1-1）：成交要用的 close/high/low 必须同一次载荷齐全且有效，
    否则整条拒写。这样旧记录的 close/high/low 与它的 trade_date/时间戳保持原样，
    不会出现「旧价格 + 新时间」这种可被护栏放行的组合。
    """
    payload: Dict[str, Any] = dict(quotes_data or {})

    for standard, alias in ALIAS_TO_STANDARD:
        if payload.get(standard) is None and payload.get(alias) is not None:
            payload[standard] = payload[alias]

    # 完整性：成交必需字段
    for field in REQUIRED_PRICE_FIELDS:
        _, reason = validate_price(payload.get(field))
        if reason != "ok":
            return None, f"incomplete_snapshot:{field}_{reason}"

    high, low, range_reason = validate_price_range(payload)
    if high is None:
        return None, f"incomplete_snapshot:{range_reason}"

    payload["volume"] = _normalize_volume(payload.get("volume"), payload.get("volume_unit"))

    doc: Dict[str, Any] = dict(payload)
    doc["symbol"] = doc.get("symbol") or symbol
    doc["code"] = doc.get("code") or symbol

    # 交易日：优先保留上游/写入侧已确定的日期，缺省才注入
    if not doc.get("trade_date") and trade_date:
        doc["trade_date"] = trade_date

    quote_time = parse_quote_time(doc.get("quote_time"))
    doc["quote_time"] = quote_time.isoformat() if quote_time else None
    doc["quote_time_source"] = "upstream" if quote_time else "fetch"
    doc["fetched_at"] = now.isoformat()
    # updated_at 仅表示「本条记录被写入的时间」，不代表报价新鲜；保留是为了兼容旧读取方。
    doc["updated_at"] = now.isoformat()

    for key in list(doc.keys()):
        if key in PROTECTED_FIELDS and key not in REQUIRED_PRICE_FIELDS and doc[key] is None:
            doc.pop(key)
    if doc.get("quote_time") is None:
        doc.pop("quote_time")
    return doc, "ok"
