"""On-demand paper quotes; no database writes and no Tushare calls.

An absent/invalid/expired runtime permit makes this path inert. A valid permit
selects this source exclusively for execution: a failed fetch must not silently
fall back to an older database quote. Prices, timestamp and suspension status
come from one response. The existing quote contract still decides execution.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import stat
import time
from collections import deque
from datetime import datetime, time as day_time
from pathlib import Path
from typing import Callable, Optional

from app.services.quote_contract import (
    CN_TZ, cn_now, parse_quote_time, quote_is_usable, validate_price_range,
)

PERMIT_PATH = Path("/app/runtime/paper-quote-supply.json")
ENDPOINT = "https://hq.sinajs.cn/list="
MAX_AGE_SECONDS = 90
MAX_RESPONSE_BYTES = 8192
MAX_REQUESTS_PER_MINUTE = 20  # per backend process; not a cluster-wide quota
MAX_IN_FLIGHT = 2
PERMIT_MAX_SECONDS = {1: 86400, 2: 7 * 86400}
CALENDAR_RETRY_COOLDOWN_SECONDS = 300
CALENDAR_MAX_ATTEMPTS_PER_DAY = 3
EXPIRY_WARNING_HOURS = 36.0
SUPPORTED_SCOPE = "paper_execution_sh_sz"
STATUS_CODES = {"00": "normal", **{s: "suspended" for s in ("01", "02", "03", "04", "05", "07")}}


def in_continuous_session(now: datetime) -> bool:
    local = now.astimezone(CN_TZ)
    clock = local.time()
    return local.weekday() < 5 and (
        day_time(9, 30) <= clock < day_time(11, 30)
        or day_time(13) <= clock < day_time(15)
    )


def source_symbol(code: str) -> str:
    """Only the documented SH/SZ stock/fund layout; BJ is not yet evidenced."""
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
        raise ValueError("symbol_invalid")
    if code.startswith(("60", "68", "50", "51", "52", "56", "58")):
        return "sh" + code
    if code.startswith(("00", "30", "15", "16", "18")):
        return "sz" + code
    raise ValueError("symbol_unsupported")


def permit_status(now: datetime, path: Path = PERMIT_PATH) -> tuple[Optional[dict], str]:
    """Read-only capability: initial 24h or explicitly renewed, at most seven days.

    Version 1 keeps its original limit. Version 2 requires the hash of the
    replaced permit for operator traceability; reading never renews either.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
                return None, "paper_supply_invalid"
            raw = stream.read(4097)
        if len(raw) > 4096:
            return None, "paper_supply_invalid"
        doc = json.loads(raw)
        if (not isinstance(doc, dict) or type(doc.get("version")) is not int or doc.get("version") not in PERMIT_MAX_SECONDS
                or doc.get("enabled") is not True
                or doc.get("source") != "sina_l1"
                or doc.get("scope") != "paper_execution_sh_sz"):
            return None, "paper_supply_disabled" if isinstance(doc, dict) and doc.get('enabled') is False else "paper_supply_invalid"
        if doc['version'] == 2 and (not isinstance(doc.get('renewal_of_sha256'), str)
                or re.fullmatch(r'[0-9a-f]{64}', doc['renewal_of_sha256']) is None):
            return None, "paper_supply_invalid"
        start = parse_quote_time(doc.get("not_before"))
        end = parse_quote_time(doc.get("expires_at"))
        if start is None or end is None or not (0 < (end-start).total_seconds() <= PERMIT_MAX_SECONDS[doc['version']]):
            return None, "paper_supply_invalid"
        if now < start:
            return None, "paper_supply_not_started"
        if now >= end:
            return None, "paper_supply_expired"
        return doc, "ok"
    except FileNotFoundError:
        return None, "paper_supply_disabled"
    except (OSError, ValueError, TypeError, OverflowError):
        return None, "paper_supply_invalid"


def read_permit(now: datetime, path: Path = PERMIT_PATH) -> Optional[dict]:
    """Compatibility reader: status reporting never changes admission policy."""
    return permit_status(now, path)[0]


def _read_permit_raw_safe(path: Path = PERMIT_PATH) -> Optional[dict]:
    """Read-only safe reader for non-secret timing metadata on expired/not-started permits."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
                return None
            raw = stream.read(4097)
        if len(raw) > 4096:
            return None
        doc = json.loads(raw)
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError, TypeError, OverflowError):
        return None


def _build_permit_summary(local_now: datetime, meta_doc: Optional[dict], status_code: str) -> dict:
    expires_at: Optional[str] = None
    remaining_hours: Optional[float] = None
    scope = SUPPORTED_SCOPE

    if isinstance(meta_doc, dict):
        if isinstance(meta_doc.get("scope"), str) and meta_doc["scope"] == SUPPORTED_SCOPE:
            scope = meta_doc["scope"]
        end_dt = parse_quote_time(meta_doc.get("expires_at"))
        if end_dt is not None:
            end_cn = end_dt.astimezone(CN_TZ)
            expires_at = end_cn.isoformat()
            delta_hours = (end_cn - local_now).total_seconds() / 3600.0
            remaining_hours = round(max(0.0, delta_hours), 2)

    expiry_warning = (
        status_code == "paper_supply_expired"
        or (remaining_hours is not None and remaining_hours < EXPIRY_WARNING_HOURS)
    )
    return {
        "permit_status": status_code,
        "expires_at": expires_at,
        "remaining_hours": remaining_hours,
        "expiry_warning": bool(expiry_warning),
        "supported_scope": scope,
    }


def read_permit_summary(now: datetime, path: Path = PERMIT_PATH) -> dict:
    """Read-only sanitized permit summary without paths, hashes, URLs, or secrets."""
    local_now = now.astimezone(CN_TZ)
    doc, status_code = permit_status(local_now, path)
    meta_doc = doc
    if meta_doc is None and status_code in ("paper_supply_expired", "paper_supply_not_started"):
        meta_doc = _read_permit_raw_safe(path)
    return _build_permit_summary(local_now, meta_doc, status_code)


_SAFE_OBSERVABILITY_KEYS = (
    "event",
    "code",
    "selected",
    "available",
    "reason",
    "source",
    "quote_time",
    "fetched_at",
    "trading_status",
    "trading_status_raw",
)


def sanitize_observability_for_api(snapshot: Optional[dict]) -> dict:
    """Strip hashes, paths, URLs, and sensitive keys before returning over HTTP."""
    if not isinstance(snapshot, dict):
        return {"counts": {}, "in_flight": 0, "window_requests": 0, "last_observation": None}
    counts = snapshot.get("counts")
    safe_counts = {str(k): int(v) for k, v in counts.items()} if isinstance(counts, dict) else {}
    last_obs = snapshot.get("last_observation")
    safe_last: Optional[dict] = None
    if isinstance(last_obs, dict):
        safe_last = {k: last_obs[k] for k in _SAFE_OBSERVABILITY_KEYS if k in last_obs}
    return {
        "counts": safe_counts,
        "in_flight": int(snapshot.get("in_flight", 0) or 0),
        "window_requests": int(snapshot.get("window_requests", 0) or 0),
        "last_observation": safe_last,
    }


def build_supply_status_message(
    *,
    in_session: bool,
    permit_state: str,
    expires_at: Optional[str],
    remaining_hours: Optional[float],
    expiry_warning: bool,
) -> str:
    """User-facing Chinese status summary for the PaperTrading banner."""
    scope_note = "支持沪深A股与ETF（暂不支持北交所 92/83/87/43）"
    if permit_state == "ok":
        if in_session:
            if expiry_warning and remaining_hours is not None:
                return f"盘中实时成交行情正常，许可剩余约 {remaining_hours:.1f} 小时即将到期，请及时人工续期；{scope_note}。"
            expiry_label = expires_at[:16].replace("T", " ") if expires_at else "有效期内"
            return f"盘中实时成交行情正常（许可有效期至 {expiry_label}）；{scope_note}。"
        if expiry_warning and remaining_hours is not None:
            return f"当前非连续竞价时段（09:30–11:30、13:00–15:00），许可剩余约 {remaining_hours:.1f} 小时即将到期；{scope_note}。"
        return f"当前非连续竞价时段（09:30–11:30、13:00–15:00），盘中许可正常有效；{scope_note}。"
    if permit_state == "paper_supply_expired":
        return f"模拟交易盘中报价许可已过期，暂不可实时下单，请联系管理员人工续期；{scope_note}。"
    if permit_state == "paper_supply_not_started":
        return f"模拟交易盘中报价许可尚未到达生效时间；{scope_note}。"
    if permit_state == "paper_supply_disabled":
        return f"模拟交易盘中报价许可尚未启用，请联系管理员配置；{scope_note}。"
    return f"模拟交易盘中报价许可配置异常，请联系管理员核查；{scope_note}。"


def parse_sina_quote(raw: bytes, code: str, captured_at: datetime) -> dict:
    """Parse data, never execute returned JavaScript.

    Field positions are zero-based: 30=date, 31=time, 32=suspension code.
    Source reference: https://www.sinacloud.com/doc/api.html (financehq).
    This is vendor status, not an exchange-certified feed. Unknown codes fail
    closed; neither nonzero volume nor wall clock implies normal status.
    """
    symbol = source_symbol(code)
    if not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("response_size_invalid")
    try:
        text = raw.decode("gb18030", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError("response_encoding_invalid") from exc
    match = re.fullmatch(r'\s*var\s+hq_str_' + symbol + r'="([^"\r\n]*)";?\s*', text)
    if match is None:
        raise ValueError("response_symbol_or_format_invalid")
    fields = match.group(1).split(",")
    if len(fields) not in (33, 34) or (len(fields) == 34 and fields[33] != ""):
        raise ValueError("response_layout_unknown")
    stamp = fields[30] + "T" + fields[31]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", stamp):
        raise ValueError("upstream_time_invalid")
    quote_time = datetime.fromisoformat(stamp).replace(tzinfo=CN_TZ)
    status = STATUS_CODES.get(fields[32])
    if status is None:
        raise ValueError("upstream_status_unknown")
    # Suspension is not hidden just because the price fields are empty/zero.
    if status == "suspended":
        raise ValueError("security_suspended")

    def number(index):
        value = float(fields[index])
        if not math.isfinite(value) or value < 0:
            raise ValueError("upstream_number_invalid")
        return value

    doc = {
        "symbol": code, "code": code, "name": fields[0],
        "open": number(1), "pre_close": number(2), "close": number(3),
        "high": number(4), "low": number(5),
        "volume": number(8) / 100, "volume_unit": "lot", "amount": number(9),
        "trade_date": fields[30], "quote_time": quote_time.isoformat(),
        "quote_time_source": "upstream", "fetched_at": captured_at.isoformat(),
        "data_source": "sina_l1", "trading_status": status,
        "quote_delivery": "on_demand", "max_quote_age_seconds": MAX_AGE_SECONDS,
        "trading_status_raw": fields[32], "trading_status_source": "sina_l1_field_32",
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
    }
    high, _, reason = validate_price_range(doc)
    if high is None:
        raise ValueError(reason)
    usable, reason = quote_is_usable(doc, captured_at, max_age_seconds=MAX_AGE_SECONDS)
    if not usable:
        raise ValueError(reason)
    if not in_continuous_session(quote_time):
        raise ValueError("upstream_outside_session")
    return doc


async def cached_trade_day(now: datetime) -> Optional[bool]:
    """Read only the dated, verified Redis value; None means unavailable."""
    from app.core.database import get_redis_client
    try:
        redis = get_redis_client()
        raw = await redis.get("trading_calendar:last_trade_date")
        doc = json.loads(raw) if raw else {}
        today = now.astimezone(CN_TZ).date().isoformat()
        if (doc.get("source") not in ("tushare", "akshare")
                or doc.get("fetched_at") != today
                or type(doc.get("is_today_trade_day")) is not bool):
            return None
        return doc["is_today_trade_day"] and doc.get("last_trade_date") == today
    except Exception:
        return None


async def public_trade_date() -> Optional[str]:
    # Deliberately call ONLY the public Sina calendar branch. Calling the
    # service's general refresh would try Tushare first and violate isolation.
    from app.services.trading_calendar_service import get_trading_calendar_service
    return await get_trading_calendar_service()._fetch_from_akshare()


class VerifiedTradeCalendar:
    """Bounded public calendar verification with failure backoff; no weekday guess.

    A startup Redis value alone would become stale the following morning. This
    fallback avoids requiring a backend restart to trade on a subsequent day.
    Failed lookups stay closed and may retry only after a bounded cooldown
    (default 300s) up to a strict daily attempt cap (default 3/day). It never
    writes Redis cache itself and never guesses based on weekday.
    """
    def __init__(
        self,
        cached=cached_trade_day,
        fetch=public_trade_date,
        *,
        cooldown_seconds: float = CALENDAR_RETRY_COOLDOWN_SECONDS,
        max_attempts_per_day: int = CALENDAR_MAX_ATTEMPTS_PER_DAY,
        monotonic: Optional[Callable[[], float]] = None,
    ):
        self.cached, self.fetch = cached, fetch
        self.cooldown_seconds = max(0.0, float(cooldown_seconds))
        self.max_attempts_per_day = max(1, int(max_attempts_per_day))
        self._monotonic = monotonic
        self.day: Optional[str] = None
        self.verified = False
        self.attempts = 0
        self.last_attempt_ts: Optional[float] = None

    def _clock_tick(self, now: datetime) -> float:
        if self._monotonic is not None:
            return float(self._monotonic())
        return float(now.timestamp())

    async def peek_verified(self, now: datetime) -> Optional[bool]:
        """Pure read-only calendar status check; zero external network calls."""
        try:
            cached = await asyncio.wait_for(self.cached(now), timeout=1)
        except Exception:
            cached = None
        if cached is not None:
            return cached is True
        today = now.astimezone(CN_TZ).date().isoformat()
        if self.day == today and self.attempts > 0:
            return self.verified
        return None

    async def __call__(self, now: datetime) -> bool:
        try:
            cached = await asyncio.wait_for(self.cached(now), timeout=1)
        except (TimeoutError, asyncio.TimeoutError):
            cached = None
        if cached is not None:
            return cached is True
        today = now.astimezone(CN_TZ).date().isoformat()
        if self.day != today:
            self.day = today
            self.verified = False
            self.attempts = 0
            self.last_attempt_ts = None
        if self.verified:
            return True
        if self.attempts >= self.max_attempts_per_day:
            return False
        tick = self._clock_tick(now)
        if self.last_attempt_ts is not None and (tick - self.last_attempt_ts) < self.cooldown_seconds:
            return False
        self.attempts += 1
        self.last_attempt_ts = tick
        try:
            self.verified = await asyncio.wait_for(self.fetch(), timeout=5) == today
        except Exception:
            self.verified = False
        return self.verified


async def fetch_sina(code: str) -> bytes:
    import httpx
    # Fixed HTTPS host, TLS verification on, no retries, redirects or proxy env.
    async with httpx.AsyncClient(timeout=4, follow_redirects=False, trust_env=False) as client:
        async with client.stream("GET", ENDPOINT + source_symbol(code), headers={
            "Referer": "https://finance.sina.com.cn/", "User-Agent": "PaperQuoteSupply/1.0",
        }) as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise ValueError("response_size_invalid")
            return bytes(body)


def summarize_quote_observability(
    code: str, selected: bool, doc: Optional[dict], reason: str
) -> dict:
    """Structured observability summary without raw payloads, URLs, or security names."""
    if doc is None:
        return {
            "event": "paper_quote_probe",
            "code": code,
            "selected": bool(selected),
            "available": False,
            "reason": reason,
            "source": None,
            "quote_time": None,
            "fetched_at": None,
            "trading_status": None,
            "trading_status_raw": None,
            "payload_sha256": None,
        }
    return {
        "event": "paper_quote_probe",
        "code": code,
        "selected": bool(selected),
        "available": True,
        "reason": reason,
        "source": doc.get("data_source"),
        "quote_time": doc.get("quote_time"),
        "fetched_at": doc.get("fetched_at"),
        "trading_status": doc.get("trading_status"),
        "trading_status_raw": doc.get("trading_status_raw"),
        "payload_sha256": doc.get("payload_sha256"),
    }


class PaperQuoteSupply:
    def __init__(self, *, clock: Callable = cn_now, permit: Callable = permit_status,
                 calendar: Optional[Callable] = None, fetch: Callable = fetch_sina,
                 monotonic: Callable = time.monotonic):
        self.clock, self.permit, self.fetch = clock, permit, fetch
        self.calendar = calendar if calendar is not None else VerifiedTradeCalendar()
        self.monotonic = monotonic
        self.requests = deque()
        self.in_flight = 0
        self.stats: dict[str, int] = {}
        self.last_observation: Optional[dict] = None

    def _resolve_permit(self, now: datetime) -> tuple[Optional[dict], str]:
        result = self.permit(now)
        if isinstance(result, tuple) and len(result) == 2:
            doc, reason = result
            return doc, str(reason or "paper_supply_disabled")
        if result is None:
            return None, "paper_supply_disabled"
        return result, "ok"

    def _record(
        self, code: str, selected: bool, doc: Optional[dict], reason: str
    ) -> tuple[bool, Optional[dict], str]:
        self.stats[reason] = self.stats.get(reason, 0) + 1
        self.last_observation = summarize_quote_observability(code, selected, doc, reason)
        return selected, doc, reason

    def snapshot_observability(self) -> dict:
        return {
            "counts": dict(self.stats),
            "in_flight": self.in_flight,
            "window_requests": len(self.requests),
            "last_observation": (
                dict(self.last_observation) if self.last_observation is not None else None
            ),
        }

    async def status_summary(self, now: Optional[datetime] = None, path: Path = PERMIT_PATH) -> dict:
        """Pure read-only status summary for HTTP/ops inspection; zero external calls."""
        current = (now if now is not None else self.clock()).astimezone(CN_TZ)
        in_session = in_continuous_session(current)
        if path != PERMIT_PATH or self.permit is permit_status:
            permit_meta = read_permit_summary(current, path)
        else:
            doc, status_code = self._resolve_permit(current)
            permit_meta = _build_permit_summary(current, doc, status_code)
        calendar_verified: Optional[bool] = None
        if hasattr(self.calendar, "peek_verified"):
            calendar_verified = await self.calendar.peek_verified(current)
        message = build_supply_status_message(
            in_session=in_session,
            permit_state=permit_meta["permit_status"],
            expires_at=permit_meta["expires_at"],
            remaining_hours=permit_meta["remaining_hours"],
            expiry_warning=permit_meta["expiry_warning"],
        )
        return {
            "in_session": in_session,
            "permit_status": permit_meta["permit_status"],
            "expires_at": permit_meta["expires_at"],
            "remaining_hours": permit_meta["remaining_hours"],
            "expiry_warning": permit_meta["expiry_warning"],
            "calendar_verified": calendar_verified,
            "supported_scope": permit_meta["supported_scope"],
            "message": message,
            "observability": sanitize_observability_for_api(self.snapshot_observability()),
        }

    async def get(self, code: str) -> tuple[bool, Optional[dict], str]:
        """(selected, quote, reason); selected failures forbid database fallback."""
        now = self.clock()
        permission, permit_reason = self._resolve_permit(now)
        if permission is None:
            return self._record(code, False, None, permit_reason)
        try:
            source_symbol(code)
            if not in_continuous_session(now):
                return self._record(code, True, None, "outside_continuous_session")
            if not await asyncio.wait_for(self.calendar(now), timeout=6.5):
                return self._record(code, True, None, "calendar_unverified")
            # No await between admission and reservation: concurrent requests
            # in the same event loop cannot oversubscribe these process limits.
            tick = self.monotonic()
            while self.requests and tick - self.requests[0] >= 60:
                self.requests.popleft()
            if len(self.requests) >= MAX_REQUESTS_PER_MINUTE or self.in_flight >= MAX_IN_FLIGHT:
                return self._record(code, True, None, "paper_supply_busy")
            admission_time = self.clock()
            if not in_continuous_session(admission_time):
                return self._record(code, True, None, "outside_continuous_session")
            if self._resolve_permit(admission_time)[0] != permission:
                return self._record(code, True, None, "paper_supply_revoked")
            self.requests.append(tick)
            self.in_flight += 1
            try:
                raw = await asyncio.wait_for(self.fetch(code), timeout=5)
            finally:
                self.in_flight -= 1
            captured_at = self.clock()
            if self._resolve_permit(captured_at)[0] != permission:
                return self._record(code, True, None, "paper_supply_revoked")
            if not in_continuous_session(captured_at):
                return self._record(code, True, None, "outside_continuous_session")
            doc = parse_sina_quote(raw, code, captured_at)
            return self._record(code, True, doc, "ok")
        except (TimeoutError, asyncio.TimeoutError):
            return self._record(code, True, None, "paper_supply_timeout")
        except ValueError as exc:
            return self._record(code, True, None, str(exc))
        except Exception:
            # Do not expose provider response bodies, URLs or auth information.
            return self._record(code, True, None, "paper_supply_unavailable")


_supply = PaperQuoteSupply()


async def get_paper_quote(code: str) -> tuple[bool, Optional[dict], str]:
    return await _supply.get(code)


async def get_paper_supply_status(now: Optional[datetime] = None, path: Path = PERMIT_PATH) -> dict:
    return await _supply.status_summary(now=now, path=path)
