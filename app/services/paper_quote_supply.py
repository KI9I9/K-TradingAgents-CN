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
    text = raw.decode("gb18030", errors="strict")
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
    """At most one public calendar attempt per process/day; no weekday guess.

    A startup Redis value alone would become stale the following morning. This
    fallback avoids requiring a backend restart to trade on a subsequent day.
    A failed lookup remains closed until next day or an operator supplies a
    fresh verified Redis value. It never writes that cache itself.
    """
    def __init__(self, cached=cached_trade_day, fetch=public_trade_date):
        self.cached, self.fetch = cached, fetch
        self.day = None
        self.verified = False

    async def __call__(self, now: datetime) -> bool:
        try:
            cached = await asyncio.wait_for(self.cached(now), timeout=1)
        except (TimeoutError, asyncio.TimeoutError):
            cached = None
        if cached is not None:
            return cached is True
        today = now.astimezone(CN_TZ).date().isoformat()
        if self.day != today:
            self.day, self.verified = today, False
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


class PaperQuoteSupply:
    def __init__(self, *, clock: Callable = cn_now, permit: Callable = read_permit,
                 calendar: Optional[Callable] = None, fetch: Callable = fetch_sina,
                 monotonic: Callable = time.monotonic):
        self.clock, self.permit, self.fetch = clock, permit, fetch
        self.calendar = calendar if calendar is not None else VerifiedTradeCalendar()
        self.monotonic = monotonic
        self.requests = deque()
        self.in_flight = 0

    async def get(self, code: str) -> tuple[bool, Optional[dict], str]:
        """(selected, quote, reason); selected failures forbid database fallback."""
        now = self.clock()
        permission = self.permit(now)
        if permission is None:
            return False, None, "paper_supply_disabled"
        try:
            source_symbol(code)
            if not in_continuous_session(now):
                return True, None, "outside_continuous_session"
            if not await asyncio.wait_for(self.calendar(now), timeout=6.5):
                return True, None, "calendar_unverified"
            # No await between admission and reservation: concurrent requests
            # in the same event loop cannot oversubscribe these process limits.
            tick = self.monotonic()
            while self.requests and tick - self.requests[0] >= 60:
                self.requests.popleft()
            if len(self.requests) >= MAX_REQUESTS_PER_MINUTE or self.in_flight >= MAX_IN_FLIGHT:
                return True, None, "paper_supply_busy"
            admission_time = self.clock()
            if not in_continuous_session(admission_time):
                return True, None, "outside_continuous_session"
            if self.permit(admission_time) != permission:
                return True, None, "paper_supply_revoked"
            self.requests.append(tick)
            self.in_flight += 1
            try:
                raw = await asyncio.wait_for(self.fetch(code), timeout=5)
            finally:
                self.in_flight -= 1
            captured_at = self.clock()
            if self.permit(captured_at) != permission:
                return True, None, "paper_supply_revoked"
            if not in_continuous_session(captured_at):
                return True, None, "outside_continuous_session"
            return True, parse_sina_quote(raw, code, captured_at), "ok"
        except (TimeoutError, asyncio.TimeoutError):
            return True, None, "paper_supply_timeout"
        except ValueError as exc:
            return True, None, str(exc)
        except Exception:
            # Do not expose provider response bodies, URLs or auth information.
            return True, None, "paper_supply_unavailable"


_supply = PaperQuoteSupply()


async def get_paper_quote(code: str) -> tuple[bool, Optional[dict], str]:
    return await _supply.get(code)
