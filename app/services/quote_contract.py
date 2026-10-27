"""行情时间与字段契约（纯标准库，可离线测试）。

六条硬约定：
1. **时间三态**：BSON date（pymongo 默认解出 naive UTC）、ISO 字符串（`+08:00` / `Z`）、
   缺失或非法。前两者可解析；第三者一律**拒绝**。
2. **行情时间 ≠ 采集时间**。`quote_time` 只来自上游；上游没给就保持 None，
   **绝不用采集时间伪造 quote_time**。采集时间单独放 `fetched_at`，且**在取得该批
   快照的那一刻固定**，写库时不得重新起算（`updated_at` 才是写入时间）。
3. **无 quote_time 时的准入是有边界的**：只接受「采集时刻 + 声明的快照周期」构成的
   近实时快照，且必须满足：交易日为今日、当前处于交易时段、年龄 ≤
   `快照周期 + 余量`、快照周期本身不超过硬上限。凭据类型记在 `quote_time_source`。
   这约束的是**采集后有效期**，不是上游报价的真实延迟——上游若返回缓存旧值，
   本系统无法感知，提示文案必须如实说明。
4. **完整快照才可写**：close/high/low 必须同一次载荷齐全且有效（含 low ≤ close ≤ high）；
   不完整载荷**整条拒写**，既不刷新价格，也不刷新日期与时间 —— 失败不得续期。
   通道切换（upstream → fetch）必须显式 `$unset` 旧 `quote_time`，`$set` 删不掉字段。
5. **定价依据必须与成交同源**：`*_and_basis()` 从**同一条记录**同时产出价格与依据，
   禁止成交后再查一次库补说明（并发下会拿 A 价配 B 说明）。
6. **交易所时区固定 UTC+8**，不跟随进程 TZ、不跟随展示时区。

关于第 3 条的业务含义（King 2026-10-27 决策）：
允许「有明确延迟边界的近实时快照」用于模拟成交，但边界必须可计算、可展示、可拒绝。
因此本模块不提供「开关式放行」，而是把边界写成数据：写入侧记录
`snapshot_interval_seconds` 与 `fetched_at`，消费侧据此计算采集后有效期并在成交时回报。
"""

from __future__ import annotations

import math
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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

# —— 写入总闸 ——
# 供给隔离期间默认关闭：所有写入路径（定时、手动、单只、批量）都经过
# prepare_quote_write，因此这一处即可覆盖全部入口，不依赖调度任务是否暂停。
DEFAULT_QUOTES_WRITE_ENABLED = False

# —— 交易状态 ——
# 状态未知不等于可交易；只有明确「交易中」才允许作为成交依据。
TRADING_STATUS_VALUES = ("trading", "normal", "交易", "交易中", "正常", "连续竞价")
SUSPENDED_VALUES = ("suspended", "halted", "停牌")
ALLOW_MISSING_TRADING_STATUS = False

# —— 日线最终性 ——
# 只认显式最终标记；写入时间不能证明数据完整或口径正确。
BAR_FINAL_STATES = ("final", "closed", "finished", "已收盘", "最终")

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

# 成交量单位：库内标准是「手」。
VOLUME_UNIT_LOT = "lot"
VOLUME_UNIT_SHARE = "share"
_SHARE_UNITS = ("share", "shares", "股")


def quotes_write_enabled(env=None) -> bool:
    """行情写入总闸。默认关闭；只有显式开启才允许写库。

    覆盖全部入口的关键：定时任务、手动同步、单只保存、批量写入都必须经过
    `prepare_quote_write`，闸门放在这里，就不需要依赖「任务是否暂停」这一假设。
    """
    environ = env if env is not None else os.environ
    raw = environ.get("QUOTES_WRITE_ENABLED")
    if raw is None:
        return DEFAULT_QUOTES_WRITE_ENABLED
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def trading_status_ok(record: Dict[str, Any]) -> Tuple[Optional[bool], str]:
    """返回 (是否可交易, 原因)。None 表示未知，调用方必须拒绝。"""
    raw = (record or {}).get("trading_status")
    if raw is None:
        if ALLOW_MISSING_TRADING_STATUS:
            return True, "trading_status_assumed"
        return None, "trading_status_missing"
    if not isinstance(raw, str):
        return None, "trading_status_invalid"
    text = raw.strip().lower()
    if text in TRADING_STATUS_VALUES:
        return True, "ok"
    if text in SUSPENDED_VALUES:
        return None, "security_suspended"
    return None, f"trading_status_unknown:{raw}"


def bar_finality_ok(bar: Dict[str, Any]) -> Tuple[bool, str]:
    """日线最终性证据：只认显式标记，不用写入时间代替。"""
    if (bar or {}).get("is_final") is True:
        return True, "ok"
    raw = (bar or {}).get("final_state")
    if isinstance(raw, str) and raw.strip().lower() in BAR_FINAL_STATES:
        return True, "ok"
    return False, "bar_finality_unproven"


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
    """校验 high/low；若带 close，则必须满足 low ≤ close ≤ high。"""
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

    if quote.get("close") is not None:
        close, close_reason = validate_price(quote.get("close"))
        if close is None:
            return None, None, f"close_{close_reason}"
        if close < low or close > high:
            return None, None, f"close_out_of_range:{close}"
    return high, low, "ok"


# --------------------------------------------------------------------------- #
# 近实时快照的延迟边界
# --------------------------------------------------------------------------- #
def snapshot_delay_bound(quote: Dict[str, Any]) -> Tuple[Optional[int], str]:
    """计算该快照允许的采集后有效期（秒）。

    依据写入侧声明的 `snapshot_interval_seconds`：
    - 未声明 → 用 `DEFAULT_SNAPSHOT_INTERVAL_SECONDS`
    - 声明值 ≤0 / > 硬上限 / 非有限数 / 非数值 → 拒绝（不自动放大）
    """
    raw = (quote or {}).get("snapshot_interval_seconds")
    if raw is None:
        interval = DEFAULT_SNAPSHOT_INTERVAL_SECONDS
    elif isinstance(raw, bool):
        return None, "snapshot_interval_invalid"
    elif isinstance(raw, float):
        if not math.isfinite(raw):
            return None, "snapshot_interval_invalid"
        interval = int(raw)
    elif isinstance(raw, int):
        interval = raw
    else:
        try:
            interval = int(str(raw).strip())
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
    原因码带 `ok_fetch_snapshot:<allowed>s`，便于调用方展示采集后有效期。
    """
    if not quote:
        return False, "quote_missing"

    if require_trade_date and not is_same_trade_date(quote.get("trade_date"), now.date()):
        return False, f"trade_date_mismatch:{quote.get('trade_date')}"

    status_ok, status_reason = trading_status_ok(quote)
    if not status_ok:
        return False, status_reason

    raw_quote_time = quote.get("quote_time")
    if raw_quote_time not in (None, ""):
        quote_time = parse_quote_time(raw_quote_time)
        if quote_time is None:
            # 上游给了时间但不可解析：既不能用它放行，也不能悄悄退化成「快照」。
            return False, "quote_time_invalid"
        age = (now - quote_time).total_seconds()
        if age > max_age_seconds:
            return False, f"quote_stale:{int(age)}s"
        if age < -max_future_seconds:
            return False, f"quote_from_future:{int(-age)}s"
        return True, "ok"

    # —— 近实时快照分支 ——
    source = quote.get("quote_time_source")
    if source is None:
        return False, "quote_time_missing"
    if not isinstance(source, str):
        return False, "quote_time_source_invalid"
    if source.strip().lower() != QUOTE_TIME_SOURCE_FETCH:
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


# --------------------------------------------------------------------------- #
# 定价依据（必须与成交同源）
# --------------------------------------------------------------------------- #
def _notice_upstream(quote_time: datetime) -> str:
    return (
        f"按上游行情时间 {quote_time.astimezone(CN_TZ).strftime('%H:%M:%S')} 的行情模拟定价。"
    )


def _notice_fetch(fetched_at: datetime, delay_seconds: int, allowed: int) -> str:
    local = fetched_at.astimezone(CN_TZ)
    elapsed = max(0, delay_seconds) // 60
    minutes = max(1, allowed // 60)
    return (
        f"按 {local.strftime('%H:%M:%S')} 采集的快照模拟定价；距采集已 {elapsed} 分钟，"
        f"采集后有效期为 {minutes} 分钟。上游报价时间未知，可能存在额外延迟。"
    )


def _notice_daily(trade_date: Optional[str]) -> str:
    day = coerce_trade_date(trade_date) or "当日"
    return f"按 {day} 收盘价模拟定价；上游报价时间未知。"


def quote_pricing_basis(quote: Dict[str, Any], now: datetime) -> Optional[Dict[str, Any]]:
    """单条行情（market_quotes）的定价依据。不参与放行判定。"""
    if not quote:
        return None

    quote_time = parse_quote_time(quote.get("quote_time"))
    if quote_time is not None:
        return {
            "source": "market_quotes",
            "channel": QUOTE_TIME_SOURCE_UPSTREAM,
            "as_of": quote_time.astimezone(CN_TZ).isoformat(),
            "trade_date": coerce_trade_date(quote.get("trade_date")),
            "delay_seconds": int((now - quote_time).total_seconds()),
            "max_delay_seconds": DEFAULT_MAX_QUOTE_AGE_SECONDS,
            "notice": _notice_upstream(quote_time),
        }

    allowed, _ = snapshot_delay_bound(quote)
    fetched_at = parse_quote_time(quote.get("fetched_at"))
    if allowed is None or fetched_at is None:
        return None
    delay = int((now - fetched_at).total_seconds())
    return {
        "source": "market_quotes",
        "channel": QUOTE_TIME_SOURCE_FETCH,
        "as_of": fetched_at.astimezone(CN_TZ).isoformat(),
        "trade_date": coerce_trade_date(quote.get("trade_date")),
        "delay_seconds": delay,
        "max_delay_seconds": allowed,
        "notice": _notice_fetch(fetched_at, delay, allowed),
    }


def bar_pricing_basis(bar: Dict[str, Any], source: str, now: datetime) -> Dict[str, Any]:
    """日线记录的定价依据。"""
    written_at = parse_quote_time(bar.get("updated_at"))
    trade_date = coerce_trade_date(bar.get("trade_date"))
    return {
        "source": source,
        "channel": "daily_close",
        "as_of": written_at.astimezone(CN_TZ).isoformat() if written_at else None,
        "trade_date": trade_date,
        "delay_seconds": None,
        "max_delay_seconds": None,
        "notice": _notice_daily(trade_date),
    }


def quote_price_and_basis(
    quote: Dict[str, Any], now: datetime
) -> Tuple[Optional[float], Optional[Dict[str, Any]], str]:
    """从**同一条** market_quotes 记录产出价格与依据。"""
    usable, reason = quote_is_usable(quote, now)
    if not usable:
        return None, None, reason
    high, low, range_reason = validate_price_range(quote)
    if high is None:
        return None, None, f"range_{range_reason}"
    price, price_reason = validate_price(quote.get("close"))
    if price is None:
        return None, None, f"close_{price_reason}"
    basis = quote_pricing_basis(quote, now)
    if basis is None:
        return None, None, "basis_unavailable"
    return price, basis, "ok"


def quote_range_and_basis(
    quote: Dict[str, Any], now: datetime
) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, Any]], str]:
    """从**同一条** market_quotes 记录产出区间与依据。"""
    usable, reason = quote_is_usable(quote, now)
    if not usable:
        return None, None, reason
    high, low, range_reason = validate_price_range(quote)
    if high is None:
        return None, None, f"range_{range_reason}"
    basis = quote_pricing_basis(quote, now)
    if basis is None:
        return None, None, "basis_unavailable"
    return {"high": high, "low": low}, basis, "ok"


def bar_price_and_basis(
    bar: Dict[str, Any], source: str, now: datetime
) -> Tuple[Optional[float], Optional[Dict[str, Any]], str]:
    """从**同一条**日线记录产出收盘价与依据。"""
    final, reason = daily_bar_is_final(bar, now)
    if not final:
        return None, None, reason
    price, price_reason = validate_price(bar.get("close"))
    if price is None:
        return None, None, f"close_{price_reason}"
    return price, bar_pricing_basis(bar, source, now), "ok"


def bar_range_and_basis(
    bar: Dict[str, Any], source: str, now: datetime
) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, Any]], str]:
    """从**同一条**日线记录产出区间与依据。"""
    final, reason = daily_bar_is_final(bar, now)
    if not final:
        return None, None, reason
    high, low, range_reason = validate_price_range(bar)
    if high is None:
        return None, None, f"range_{range_reason}"
    return {"high": high, "low": low}, bar_pricing_basis(bar, source, now), "ok"


def display_price_and_basis(
    record: Dict[str, Any], source: str, now: datetime
) -> Tuple[Optional[float], Optional[Dict[str, Any]]]:
    """展示用参考价：**不做时效与完成状态校验**，只要求数值有效。

    与严格成交路径分开，避免「为了禁止旧价成交」把持仓估值一起打死。
    """
    if not record:
        return None, None
    price, _ = validate_price(record.get("close", record.get("current_price")))
    if price is None:
        return None, None
    trade_date = coerce_trade_date(record.get("trade_date"))
    basis = {
        "source": source,
        "channel": "display",
        "as_of": record.get("updated_at"),
        "trade_date": trade_date,
        "delay_seconds": None,
        "max_delay_seconds": None,
        "notice": f"展示用参考价（{trade_date or '最近可得'}），不可用于成交。",
    }
    return price, basis


# --------------------------------------------------------------------------- #
# 组合解析：把「查哪些表」与「怎么判定」分开，判定部分纯函数可测
# --------------------------------------------------------------------------- #
def resolve_cn_price(
    candidates: Dict[str, Optional[Dict[str, Any]]],
    now: datetime,
    strict: bool = True,
) -> Tuple[Optional[float], Optional[Dict[str, Any]], str]:
    """A 股/ETF 取价：返回 (价格, 与价格同源的依据, 原因码)。

    candidates 键：`market_quotes` / `stock_basic_info` / `etf_daily_quotes` / `stock_daily_quotes`
    """
    candidates = candidates or {}
    quote = candidates.get("market_quotes")

    if strict:
        price, basis, reason = quote_price_and_basis(quote, now) if quote else (None, None, "quote_missing")
        if price is not None:
            return price, basis, "ok"

        allowed, gate_reason = daily_fallback_allowed(now)
        if not allowed:
            return None, None, f"{reason}|{gate_reason}"

        for key in ("etf_daily_quotes", "stock_daily_quotes"):
            record = candidates.get(key)
            if not record:
                continue
            price, basis, bar_reason = bar_price_and_basis(record, key, now)
            if price is not None:
                return price, basis, "ok"
            reason = f"{reason}|{key}:{bar_reason}"
        return None, None, reason

    # —— 展示路径：不限时效，逐级回退，但必须标明这是展示价 ——
    if quote:
        price, basis = display_price_and_basis(quote, "market_quotes", now)
        if price is not None:
            return price, basis, "display"

    basic = candidates.get("stock_basic_info")
    if basic:
        price, basis = display_price_and_basis(basic, "stock_basic_info", now)
        if price is not None:
            return price, basis, "display"

    for key in ("etf_daily_quotes", "stock_daily_quotes"):
        record = candidates.get(key)
        if record:
            price, basis = display_price_and_basis(record, key, now)
            if price is not None:
                return price, basis, "display"
    return None, None, "display_unavailable"


def resolve_cn_range(
    candidates: Dict[str, Optional[Dict[str, Any]]],
    now: datetime,
) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, Any]], str]:
    """A 股/ETF 当日价格区间（用于指定价格单校验）。只走严格口径。"""
    candidates = candidates or {}
    quote = candidates.get("market_quotes")
    if quote:
        rng, basis, reason = quote_range_and_basis(quote, now)
        if rng is not None:
            return rng, basis, "ok"
    else:
        reason = "quote_missing"

    allowed, gate_reason = daily_fallback_allowed(now)
    if not allowed:
        return None, None, f"{reason}|{gate_reason}"

    for key in ("etf_daily_quotes", "stock_daily_quotes"):
        record = candidates.get(key)
        if not record:
            continue
        rng, basis, bar_reason = bar_range_and_basis(record, key, now)
        if rng is not None:
            return rng, basis, "ok"
        reason = f"{reason}|{key}:{bar_reason}"
    return None, None, reason


# --------------------------------------------------------------------------- #
# 日线完成度
# --------------------------------------------------------------------------- #
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

    final_ok, final_reason = bar_finality_ok(bar)
    if not final_ok:
        return False, final_reason

    status_ok, status_reason = trading_status_ok(bar)
    if not status_ok:
        return False, status_reason

    written_at = parse_quote_time(bar.get("updated_at"))
    if written_at is None:
        return False, "bar_write_time_missing"
    local_written = written_at.astimezone(CN_TZ)
    if local_written.date() != now.date():
        return False, f"bar_written_other_day:{local_written.date()}"
    if local_written.time() < DAILY_BAR_FINAL_AFTER:
        return False, f"bar_written_intraday:{local_written.strftime('%H:%M')}"
    # 写入时间不能来自未来：否则「今晚 23:00 写」会提前满足完成条件
    if (local_written - now.astimezone(CN_TZ)).total_seconds() > DEFAULT_MAX_FUTURE_SECONDS:
        return False, f"bar_write_time_in_future:{local_written.strftime('%H:%M')}"

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


def _normalize_volume(value: Any, unit: Optional[str]) -> Tuple[Any, str]:
    """把成交量统一为「手」，并返回应写入的单位标记。

    只有**显式声明**为股/份的才换算；未声明时按「手」处理（现有两个写入源的真实口径），
    但换算成功后必须把 `volume_unit` 同步改成 `lot`，否则库里单位与数值不一致。
    """
    unit_text = (unit or VOLUME_UNIT_LOT)
    if not isinstance(unit_text, str):
        return value, VOLUME_UNIT_LOT
    if unit_text.strip().lower() in _SHARE_UNITS:
        try:
            return float(value) / 100.0, VOLUME_UNIT_LOT
        except (TypeError, ValueError):
            return value, VOLUME_UNIT_LOT
    return value, VOLUME_UNIT_LOT


# --------------------------------------------------------------------------- #
# 写入侧规范化
# --------------------------------------------------------------------------- #
def prepare_quote_write(
    quotes_data: Dict[str, Any],
    symbol: str,
    trade_date: Optional[str],
    now: datetime,
    snapshot_interval_seconds: Optional[int] = None,
    captured_at: Optional[datetime] = None,
) -> Tuple[Optional[Dict[str, Any]], List[str], str]:
    """把上游字典规范化为「可提交写入」的文档与 `$unset` 列表。

    返回 (doc, unset_fields, reason)。reason != "ok" 时 doc 为 None，调用方**不得写库**。

    - 成交要用的 close/high/low 必须同一次载荷齐全且有效，否则整条拒写；
    - `fetched_at` 是**采集时刻**，取 payload 自带值，否则用调用方传入的 `captured_at`，
      再否则才退回 `now`；`updated_at` 一律是写入时刻，二者不混用；
    - `quote_time` 只透传上游；上游给了但解析不了 → 整条拒写（不降级成快照）；
    - 没有 `quote_time` 时返回 `["quote_time"]` 作为 `$unset`，防止旧通道字段残留。
    """
    if not quotes_write_enabled():
        return None, [], "quotes_write_disabled"

    payload: Dict[str, Any] = dict(quotes_data or {})

    for standard, alias in ALIAS_TO_STANDARD:
        if payload.get(standard) is None and payload.get(alias) is not None:
            payload[standard] = payload[alias]

    for field in REQUIRED_PRICE_FIELDS:
        _, reason = validate_price(payload.get(field))
        if reason != "ok":
            return None, [], f"incomplete_snapshot:{field}_{reason}"

    high, low, range_reason = validate_price_range(payload)
    if high is None:
        return None, [], f"incomplete_snapshot:{range_reason}"

    # —— 时间语义 ——
    raw_quote_time = payload.get("quote_time")
    quote_time: Optional[datetime] = None
    if raw_quote_time not in (None, ""):
        quote_time = parse_quote_time(raw_quote_time)
        if quote_time is None:
            return None, [], "quote_time_invalid"

    raw_fetched = payload.get("fetched_at")
    fetched_at: Optional[datetime]
    if raw_fetched not in (None, ""):
        fetched_at = parse_quote_time(raw_fetched)
        if fetched_at is None:
            return None, [], "fetched_at_invalid"
    else:
        fetched_at = captured_at or now
    if (fetched_at - now).total_seconds() > DEFAULT_MAX_FUTURE_SECONDS:
        return None, [], "fetched_at_from_future"

    interval = snapshot_interval_seconds
    if interval is None:
        interval = payload.get("snapshot_interval_seconds")
    if interval is None:
        interval = DEFAULT_SNAPSHOT_INTERVAL_SECONDS
    _, interval_reason = snapshot_delay_bound({"snapshot_interval_seconds": interval})
    if interval_reason != "ok":
        return None, [], interval_reason

    volume, volume_unit = _normalize_volume(payload.get("volume"), payload.get("volume_unit"))

    doc: Dict[str, Any] = dict(payload)
    doc["symbol"] = doc.get("symbol") or symbol
    doc["code"] = doc.get("code") or symbol
    if not doc.get("trade_date") and trade_date:
        doc["trade_date"] = trade_date

    unset: List[str] = []
    if quote_time is not None:
        doc["quote_time"] = quote_time.isoformat()
        doc["quote_time_source"] = QUOTE_TIME_SOURCE_UPSTREAM
    else:
        doc.pop("quote_time", None)
        unset.append("quote_time")
        doc["quote_time_source"] = QUOTE_TIME_SOURCE_FETCH

    doc["snapshot_interval_seconds"] = int(interval)
    doc["fetched_at"] = fetched_at.isoformat()
    # updated_at 仅表示「本条记录被写入的时间」，不代表报价新鲜。
    doc["updated_at"] = now.isoformat()
    if volume is not None:
        doc["volume"] = volume
        doc["volume_unit"] = volume_unit

    for key in list(doc.keys()):
        if key in PROTECTED_FIELDS and key not in REQUIRED_PRICE_FIELDS and doc[key] is None:
            doc.pop(key)
    return doc, unset, "ok"
