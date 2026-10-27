"""行情时间与字段契约（纯标准库，可离线测试）。

五条硬约定：
1. **时间三态**：BSON date（pymongo 默认解出 naive UTC）、ISO 字符串（`+08:00` / `Z`）、
   缺失或非法。前两者可解析；第三者一律**拒绝**。
2. **行情时间 ≠ 采集时间**。`quote_time` 只来自上游；上游没给就保持 None，
   **绝不用采集时间伪造 quote_time**。采集时间单独放 `fetched_at`。
3. **无 quote_time 时的准入是有边界的**：只接受「采集时刻 + 声明的快照周期」构成的
   近实时快照，且必须满足：交易日为今日、当前处于交易时段、年龄 ≤
   `快照周期 + 余量`、快照周期本身不超过硬上限。凭据类型记在 `quote_time_source`。
4. **完整快照才可写**：close/high/low 必须同一次载荷齐全且有效；不完整载荷**整条拒写**，
   既不刷新价格，也不刷新日期与时间 —— 失败不得续期。
5. **交易所时区固定 UTC+8**，不跟随进程 TZ、不跟随展示时区。

关于第 3 条的业务含义（King 2026-10-27 决策）：
允许「有明确延迟边界的近实时快照」用于模拟成交，但延迟边界必须可计算、可展示、
可拒绝。因此本模块不提供「开关式放行」，而是把边界写成数据：
写入侧记录 `snapshot_interval_seconds`，消费侧据此计算允许延迟并在成交时回报。
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

# A 股交易所时区。固定 UTC+8，中国不实行夏令时。
CN_TZ = timezone(timedelta(hours=8))

# 连续竞价 09:30-11:30 / 13:00-15:00；15:00-15:30 为收盘缓冲。
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(15, 30)

# 上游直接给出 quote_time 时，允许的最大陈旧时长。
DEFAULT_MAX_QUOTE_AGE_SECONDS = 30 * 60
# 允许的时钟正向漂移。
DEFAULT_MAX_FUTURE_SECONDS = 60

# —— 近实时快照（无 quote_time）的准入边界 ——
# 写入侧未声明周期时的默认值：与 30 分钟 cron 对齐。
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 30 * 60
# 周期未声明或声明值不合理时的硬上限：超过即拒绝，不做「自动放大」。
MAX_SNAPSHOT_INTERVAL_SECONDS = 40 * 60
# 采集延迟余量（网络 + 抓取耗时）。
SNAPSHOT_DELAY_MARGIN_SECONDS = 2 * 60
# 快照仅在交易时段内可被用于成交。
SNAPSHOT_SESSION_END = time(15, 30)

QUOTE_TIME_SOURCE_UPSTREAM = "upstream"
QUOTE_TIME_SOURCE_FETCH = "fetch"

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
    """把时间统一解析为 aware datetime；无法解析返回 None。"""
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
# 近实时快照的延迟边界
# --------------------------------------------------------------------------- #
def snapshot_delay_bound(quote: Dict[str, Any]) -> Tuple[Optional[int], str]:
    """计算该快照允许的最大年龄（秒）。

    依据写入侧声明的 `snapshot_interval_seconds`：
    - 未声明 → 用 `DEFAULT_SNAPSHOT_INTERVAL_SECONDS`
    - 声明值 ≤0 或 > 硬上限 → 拒绝（不做自动放大，避免「声明 24 小时」绕过边界）
    """
    raw = (quote or {}).get("snapshot_interval_seconds")
    if raw is None:
        interval = DEFAULT_SNAPSHOT_INTERVAL_SECONDS
    else:
        if isinstance(raw, bool):
            return None, "snapshot_interval_invalid"
        try:
            interval = int(raw)
        except (TypeError, ValueError):
            return None, "snapshot_interval_invalid"
    if interval <= 0 or interval > MAX_SNAPSHOT_INTERVAL_SECONDS:
        return None, "snapshot_interval_invalid"
    return min(interval + SNAPSHOT_DELAY_MARGIN_SECONDS, MAX_SNAPSHOT_INTERVAL_SECONDS), "ok"


def in_snapshot_session(now: datetime) -> bool:
    return SESSION_OPEN <= now.time() <= SNAPSHOT_SESSION_END


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

    优先看 `quote_time`（上游行情时间）。没有 `quote_time` 时，只有在声明了快照周期、
    处于交易时段、且采集时间在「周期 + 余量」之内，才以近实时快照身份放行，
    原因码带 `ok_fetch_snapshot:<allowed>s`，便于调用方展示延迟边界。
    """
    if not quote:
        return False, "quote_missing"

    if require_trade_date and not is_same_trade_date(quote.get("trade_date"), now.date()):
        return False, f"trade_date_mismatch:{quote.get('trade_date')}"

    quote_time = parse_quote_time(quote.get("quote_time"))
    if quote_time is not None:
        age = (now - quote_time).total_seconds()
        if age > max_age_seconds:
            return False, f"quote_stale:{int(age)}s"
        if age < -max_future_seconds:
            return False, f"quote_from_future:{int(-age)}s"
        return True, "ok"

    # —— 近实时快照分支 ——
    source = (quote.get("quote_time_source") or "").lower()
    if source != QUOTE_TIME_SOURCE_FETCH:
        return False, "quote_time_missing"

    allowed, bound_reason = snapshot_delay_bound(quote)
    if allowed is None:
        return False, bound_reason

    if not in_snapshot_session(now):
        return False, f"outside_snapshot_session:{now.strftime('%H:%M')}"

    fetched_at = parse_quote_time(quote.get("fetched_at"))
    if fetched_at is None:
        return False, "fetched_at_missing"

    age = (now - fetched_at).total_seconds()
    if age > allowed:
        return False, f"snapshot_too_old:{int(age)}s>{allowed}s"
    if age < -max_future_seconds:
        return False, f"snapshot_from_future:{int(-age)}s"
    return True, f"ok_fetch_snapshot:{allowed}s"


def quote_pricing_basis(quote: Dict[str, Any], now: datetime) -> Optional[Dict[str, Any]]:
    """给成交回报用的「定价依据」。不参与放行判定，只负责把边界讲清楚。"""
    if not quote:
        return None

    quote_time = parse_quote_time(quote.get("quote_time"))
    if quote_time is not None:
        delay = int((now - quote_time).total_seconds())
        return {
            "source": QUOTE_TIME_SOURCE_UPSTREAM,
            "as_of": quote_time.isoformat(),
            "delay_seconds": delay,
            "max_delay_seconds": DEFAULT_MAX_QUOTE_AGE_SECONDS,
            "notice": None,
        }

    allowed, _ = snapshot_delay_bound(quote)
    if allowed is None:
        return None
    fetched_at = parse_quote_time(quote.get("fetched_at"))
    if fetched_at is None:
        return None
    delay = int((now - fetched_at).total_seconds())
    minutes = max(1, allowed // 60)
    return {
        "source": QUOTE_TIME_SOURCE_FETCH,
        "as_of": fetched_at.astimezone(CN_TZ).isoformat(),
        "delay_seconds": delay,
        "max_delay_seconds": allowed,
        "notice": (
            f"模拟成交按 {fetched_at.astimezone(CN_TZ).strftime('%H:%M:%S')} 采集的行情快照定价，"
            f"该快照最大延迟约 {minutes} 分钟；非交易所逐笔成交价。"
        ),
    }


def daily_fallback_allowed(now: datetime) -> Tuple[bool, str]:
    """日线回退的时间闸门：只在收盘之后。"""
    if now.time() < DAILY_FALLBACK_AFTER:
        return False, "daily_fallback_before_close"
    return True, "ok"


def daily_bar_is_final(bar: Dict[str, Any], now: datetime) -> Tuple[bool, str]:
    """判断一根当日日线是否「已收盘、可用于成交」。"""
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
    if calendar_source not in ("tushare", "akshare"):
        return "calendar_unverified"
    local_time = now.time()
    if local_time < SESSION_OPEN:
        return "pre_open"
    if local_time > SESSION_CLOSE:
        return "after_close"
    return None


def resolve_trade_date(now: datetime) -> str:
    return now.date().isoformat()


def build_failure_stats(reason: str, context: str) -> Dict[str, Any]:
    """构造调度层能识别为 failed 的结果。"""
    return {
        "success": False,
        "error": reason,
        "error_count": 1,
        "errors": [{"error": reason, "context": context}],
    }


def _normalize_volume(value: Any, unit: Optional[str]) -> Any:
    """把成交量统一为「手」：库内标准是手，单只接口历史上写的是股。"""
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
    snapshot_interval_seconds: Optional[int] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """把上游字典规范化为「可提交写入」的文档。

    返回 (doc, reason)。reason != "ok" 时 doc 为 None，调用方**不得写库**。

    - 成交要用的 close/high/low 必须同一次载荷齐全且有效，否则整条拒写；
    - `quote_time` 只透传上游，**不伪造**；没有就留空，用 `quote_time_source="fetch"`
      标记这是采集时刻；
    - 写入 `snapshot_interval_seconds`，让消费侧能算出延迟边界。
    """
    payload: Dict[str, Any] = dict(quotes_data or {})

    for standard, alias in ALIAS_TO_STANDARD:
        if payload.get(standard) is None and payload.get(alias) is not None:
            payload[standard] = payload[alias]

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

    if not doc.get("trade_date") and trade_date:
        doc["trade_date"] = trade_date

    quote_time = parse_quote_time(doc.get("quote_time"))
    if quote_time is not None:
        doc["quote_time"] = quote_time.isoformat()
    else:
        # 不伪造行情时间：没有就删掉，靠 quote_time_source 说明依据
        doc.pop("quote_time", None)
    doc["quote_time_source"] = QUOTE_TIME_SOURCE_UPSTREAM if quote_time else QUOTE_TIME_SOURCE_FETCH

    interval = snapshot_interval_seconds
    if interval is None:
        interval = doc.get("snapshot_interval_seconds")
    if interval is None:
        interval = DEFAULT_SNAPSHOT_INTERVAL_SECONDS
    doc["snapshot_interval_seconds"] = int(interval)

    doc["fetched_at"] = now.isoformat()
    # updated_at 仅表示「本条记录被写入的时间」，不代表报价新鲜；保留以兼容旧读取方。
    doc["updated_at"] = now.isoformat()

    for key in list(doc.keys()):
        if key in PROTECTED_FIELDS and key not in REQUIRED_PRICE_FIELDS and doc[key] is None:
            doc.pop(key)
    return doc, "ok"
