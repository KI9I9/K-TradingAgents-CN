"""行情时间与字段契约（纯标准库，可离线测试）。

为什么单独抽一个模块：
- 供给侧（写入）与消费侧（下单）必须用**同一套**时间/日期判定，否则两边各写一份，
  迟早出现「写的时候认为是今天、读的时候认为是昨天」的分歧。
- 本模块不依赖 pandas / pymongo / fastapi，可在无第三方包的机器上直接跑行为测试。

三条硬约定：
1. **时间三态**：BSON date（pymongo 默认解出 naive UTC）、ISO 字符串（`+08:00` / `Z`）、
   缺失或非法。前两者可解析；第三者一律**拒绝**——放行等于护栏失效。
2. **交易所时区固定 UTC+8**，不跟随进程 TZ、不跟随展示时区。A 股无夏令时。
3. **「盘前跳过」只是供给侧不写伪数据，不能替代消费侧的交易日与行情时间校验。**
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

# 报价允许的最大陈旧时长。语义：**报价本身**的最大年龄，不是采集故障的容忍时间。
DEFAULT_MAX_QUOTE_AGE_SECONDS = 30 * 60
# 允许的时钟正向漂移（服务端比本机快）。只容忍 60 秒，不做分钟级宽限。
DEFAULT_MAX_FUTURE_SECONDS = 60

# 这些字段为 None 时不得写入，避免用空值覆盖已有的有效行情。
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
    """把行情时间统一解析为 aware datetime；无法解析返回 None。

    支持：
    - `datetime`：naive 视为 **UTC**（pymongo 默认 BSON 解码就是这样），aware 原样返回
    - `str`：ISO 8601（含 `Z`、`+08:00`、无时区）、`YYYY-MM-DD HH:MM:SS`
      * 无时区字符串按**交易所时区**解释（容器 TZ 已确认为 Asia/Shanghai）
    - 其他类型（None / 数字 / bool / list）→ None，调用方必须拒绝
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
    """把交易日归一化为 `YYYY-MM-DD`；无法识别返回 None。

    兼容 `2026-10-27`、`20261027`、`date` / `datetime` 三种存储形态。
    """
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

    返回 (是否可用, 原因)。原因为 `ok` 或机器可读的拒绝码，便于日志聚合。
    """
    if not quote:
        return False, "quote_missing"

    if require_trade_date and not is_same_trade_date(quote.get("trade_date"), now.date()):
        return False, f"trade_date_mismatch:{quote.get('trade_date')}"

    quote_time = parse_quote_time(quote.get("updated_at"))
    if quote_time is None:
        # 缺失、空串、坏值、数字全部落到这里 —— 必须拒绝，不能放行
        return False, f"quote_time_unparseable:{type(quote.get('updated_at')).__name__}"

    age = (now - quote_time).total_seconds()
    if age > max_age_seconds:
        return False, f"quote_stale:{int(age)}s"
    if age < -max_future_seconds:
        return False, f"quote_from_future:{int(-age)}s"
    return True, "ok"


def validate_price_range(quote: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], str]:
    """校验并返回 (high, low)。任何一项不合法都返回 (None, None, 原因)。"""
    if not quote:
        return None, None, "quote_missing"
    try:
        high = float(quote.get("high"))
        low = float(quote.get("low"))
    except (TypeError, ValueError):
        return None, None, "not_numeric"
    if not math.isfinite(high) or not math.isfinite(low):
        return None, None, "not_finite"
    if high <= 0 or low <= 0:
        return None, None, "non_positive"
    if low > high:
        return None, None, "low_gt_high"
    return high, low, "ok"


# --------------------------------------------------------------------------- #
# 供给侧判定
# --------------------------------------------------------------------------- #
def sync_round_skip_reason(
    now: datetime,
    is_today_trade_day: bool,
    calendar_source: Optional[str] = None,
) -> Optional[str]:
    """本轮行情同步是否整轮跳过。返回 None 表示可以写。

    - `not_trade_day`：日历确认今天不交易
    - `calendar_unverified`：日历来自「所有 API 失败→退回工作日」的兜底，
      无法区分节假日与交易日；或拿不到 source。宁可不写。
    - `pre_open`：09:30 前。盘前快照等价于昨收，写进去只会制造「看起来新鲜」的假数据。
    - `after_close`：15:30 后（cron 不会触发，手动触发同样不写）
    """
    if not is_today_trade_day:
        return "not_trade_day"
    if calendar_source != "tushare" and calendar_source != "akshare":
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


def build_quote_doc(
    quotes_data: Dict[str, Any],
    symbol: str,
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """把上游/批量的行情字典规范化为待写入文档。

    三件事：
    1. 补齐 `symbol` / `code`；
    2. 历史字段名兜底（`open_price` → `open` 等），避免只有别名时消费侧读不到；
    3. **剔除值为 None 的价格字段**，防止用空值覆盖库里已有的有效行情。
    """
    doc: Dict[str, Any] = dict(quotes_data or {})
    doc["symbol"] = doc.get("symbol") or symbol
    doc["code"] = doc.get("code") or symbol

    for standard, alias in ALIAS_TO_STANDARD:
        if doc.get(standard) is None and doc.get(alias) is not None:
            doc[standard] = doc[alias]

    if trade_date and not doc.get("trade_date"):
        doc["trade_date"] = trade_date

    for key in list(doc.keys()):
        if key in PROTECTED_FIELDS and doc[key] is None:
            doc.pop(key)
    return doc
