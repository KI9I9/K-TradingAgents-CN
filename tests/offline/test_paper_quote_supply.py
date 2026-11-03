"""Real parser -> real supply -> real paper resolver, without network or Mongo."""
import asyncio
import json
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, ".")
from app.services import paper_quote_supply as supply
from app.services.quote_contract import CN_TZ, quote_price_and_basis
from test_paper_pricing import load_paper_functions, quote_doc, bar_doc

NOW = datetime(2026, 11, 2, 14, 0, tzinfo=CN_TZ)


def packet(code="600000", status="00", stamp=None, **changes):
    f = ["0"] * 33
    f[:10] = ["测试", "10", "9.8", "10.2", "11", "9", "10.1", "10.2", "123400", "1234000"]
    stamp = stamp or NOW
    f[30:33] = [stamp.date().isoformat(), stamp.strftime("%H:%M:%S"), status]
    for index, value in changes.items():
        f[int(index)] = str(value)
    return ('var hq_str_' + supply.source_symbol(code) + '="' + ','.join(f) + '";\n').encode("gb18030")


def permission(**changes):
    doc = {"version": 1, "enabled": True, "source": "sina_l1", "scope": "paper_execution_sh_sz",
           "not_before": (NOW-timedelta(minutes=30)).isoformat(),
           "expires_at": (NOW+timedelta(hours=1)).isoformat()}
    doc.update(changes)
    return doc


class PermitAndParser(unittest.TestCase):
    def test_renewed_permit_is_explicit_finite_and_does_not_expand_v1(self):
        start = NOW - timedelta(hours=1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'permit.json'
            def read(**changes):
                value = permission(version=2, renewal_of_sha256='a'*64,
                    not_before=start.isoformat(), expires_at=(start+timedelta(days=7)).isoformat())
                value.update(changes); path.write_text(json.dumps(value)); path.chmod(0o600)
                return supply.read_permit(NOW, path)
            self.assertIsNotNone(read())
            self.assertIsNone(read(version=1))
            self.assertIsNone(read(version=3))
            self.assertIsNone(read(version=True))
            for proof in (None, '', 'a'*63, 'Z'*64, 123):
                self.assertIsNone(read(renewal_of_sha256=proof))
            self.assertIsNone(read(expires_at=(start+timedelta(days=7, seconds=1)).isoformat()))
            self.assertIsNone(read(expires_at=NOW.isoformat()))
            self.assertIsNone(read(not_before=(NOW+timedelta(seconds=1)).isoformat()))
            self.assertIsNone(read(scope='all'))
            self.assertIsNone(read(source='tushare'))

    def test_absent_invalid_expired_wrong_scope_permit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "permit.json"
            self.assertIsNone(supply.read_permit(NOW, path))
            for value in ("invalid", [], permission(enabled=False), permission(source="tushare"),
                          permission(scope="all"), permission(expires_at=NOW.isoformat()),
                          permission(expires_at=(NOW+timedelta(days=2)).isoformat())):
                path.write_text(json.dumps(value))
                path.chmod(0o640)
                self.assertIsNone(supply.read_permit(NOW, path), value)
            path.write_text(json.dumps(permission()))
            self.assertEqual(supply.read_permit(NOW, path), permission())
            path.chmod(0o666)
            self.assertIsNone(supply.read_permit(NOW, path))

    def test_equity_etf_same_packet_prices_time_status_and_lots(self):
        for code in ("600000", "000001", "159865", "510300"):
            doc = supply.parse_sina_quote(packet(code), code, NOW)
            price, basis, reason = quote_price_and_basis(doc, NOW)
            self.assertEqual((price, reason), (10.2, "ok"))
            self.assertEqual(doc["volume"], 1234)
            self.assertEqual(basis["source"], "sina_l1_on_demand")
            self.assertEqual(basis["as_of"], NOW.isoformat())
            self.assertEqual(basis["max_delay_seconds"], 90)
            self.assertEqual(basis["trading_status_raw"], "00")
            self.assertEqual(len(basis["payload_sha256"]), 64)

    def test_observed_optional_trailing_empty_field(self):
        raw = packet().replace(b'";', b',";')
        self.assertEqual(supply.parse_sina_quote(raw, "600000", NOW)["close"], 10.2)

    def test_suspended_unknown_and_missing_status_never_inferred(self):
        for status in ("01", "02", "03", "04", "05", "07", "", "99", "-3"):
            with self.assertRaisesRegex(ValueError, "security_suspended|upstream_status_unknown"):
                supply.parse_sina_quote(packet(status=status), "600000", NOW)

    def test_invalid_or_inconsistent_price_rejected(self):
        for index, value in ((3, "0"), (3, "12"), (4, "nan"), (5, "inf"), (8, "-1")):
            with self.assertRaises(ValueError):
                supply.parse_sina_quote(packet(**{str(index): value}), "600000", NOW)

    def test_payload_identity_and_layout_not_guessed(self):
        for raw in (packet("000001"), b'alert(1)', b'', packet()+packet(),
                    packet().replace(b'00";', b'00,unknown";')):
            with self.assertRaises(ValueError):
                supply.parse_sina_quote(raw, "600000", NOW)
        for code in ("920000", "830000", "600000.SH", "600000&list=xx", "000001\n"):
            with self.assertRaises(ValueError):
                supply.source_symbol(code)

    def test_old_future_previous_day_and_invalid_time_rejected(self):
        for stamp in (NOW-timedelta(seconds=91), NOW+timedelta(seconds=61), NOW-timedelta(days=1)):
            with self.assertRaises(ValueError):
                supply.parse_sina_quote(packet(stamp=stamp), "600000", NOW)
        with self.assertRaises(ValueError):
            supply.parse_sina_quote(packet(**{"31": "bad"}), "600000", NOW)

    def test_consumption_rechecks_age_and_cannot_expand_limit(self):
        doc = supply.parse_sina_quote(packet(), "600000", NOW)
        self.assertIsNone(quote_price_and_basis(doc, NOW+timedelta(seconds=91))[0])
        for value in (99999, True, "90", float("nan")):
            self.assertIsNone(quote_price_and_basis(dict(doc, max_quote_age_seconds=value), NOW)[0])


class SupplyBehavior(unittest.IsolatedAsyncioTestCase):
    def make_supply(self, **kw):
        config = dict(clock=lambda: NOW, permit=lambda now: permission(),
                      calendar=AsyncMock(return_value=True), fetch=AsyncMock(return_value=packet()))
        config.update(kw)
        return supply.PaperQuoteSupply(**config)

    async def test_disabled_does_no_network_or_calendar_work(self):
        service = self.make_supply(permit=lambda now: None)
        self.assertEqual(await service.get("600000"), (False, None, "paper_supply_disabled"))
        service.calendar.assert_not_called()
        service.fetch.assert_not_called()

    async def test_calendar_unverified_does_not_fetch(self):
        service = self.make_supply(calendar=AsyncMock(return_value=False))
        self.assertEqual((await service.get("600000"))[2], "calendar_unverified")
        service.fetch.assert_not_called()

    async def test_calendar_timeout_does_not_fetch(self):
        service = self.make_supply(calendar=AsyncMock(side_effect=asyncio.TimeoutError))
        self.assertEqual((await service.get("600000"))[2], "paper_supply_timeout")
        service.fetch.assert_not_called()

    async def test_premarket_lunch_close_weekend_no_requests(self):
        for clock in (NOW.replace(hour=9, minute=29), NOW.replace(hour=11, minute=30),
                      NOW.replace(hour=12), NOW.replace(hour=15), NOW.replace(day=1)):
            service = self.make_supply(clock=lambda: clock)
            self.assertEqual((await service.get("600000"))[2], "outside_continuous_session")
            service.fetch.assert_not_called()

    async def test_revocation_while_fetching_discards_response(self):
        calls = 0
        def permit(now):
            nonlocal calls
            calls += 1
            return permission() if calls <= 2 else None
        service = self.make_supply(permit=permit)
        self.assertEqual(await service.get("600000"), (True, None, "paper_supply_revoked"))

    async def test_crossing_session_end_discards_response(self):
        values = iter([NOW, NOW, NOW.replace(hour=15)])
        service = self.make_supply(clock=lambda: next(values))
        self.assertEqual((await service.get("600000"))[2], "outside_continuous_session")

    async def test_calendar_crossing_session_end_does_not_fetch_quote(self):
        values = iter([NOW, NOW.replace(hour=15)])
        service = self.make_supply(clock=lambda: next(values))
        self.assertEqual((await service.get("600000"))[2], "outside_continuous_session")
        service.fetch.assert_not_called()

    async def test_failed_fetch_has_no_retry_or_stale_cache(self):
        for failure in (asyncio.TimeoutError(), RuntimeError("provider failure")):
            service = self.make_supply(fetch=AsyncMock(side_effect=failure))
            selected, doc, reason = await service.get("600000")
            self.assertTrue(selected)
            self.assertIsNone(doc)
            self.assertIn(reason, ("paper_supply_timeout", "paper_supply_unavailable"))
            service.fetch.assert_awaited_once()

    async def test_request_rate_reserves_failed_attempts(self):
        service = self.make_supply(fetch=AsyncMock(side_effect=RuntimeError), monotonic=lambda: 10)
        for _ in range(supply.MAX_REQUESTS_PER_MINUTE):
            await service.get("600000")
        self.assertEqual((await service.get("600000"))[2], "paper_supply_busy")
        self.assertEqual(service.fetch.await_count, supply.MAX_REQUESTS_PER_MINUTE)

    async def test_concurrency_bounded_and_cancelled_request_releases_slot(self):
        started = asyncio.Event()
        async def blocked(code):
            started.set()
            await asyncio.Event().wait()
        service = self.make_supply(fetch=blocked)
        tasks = [asyncio.create_task(service.get("600000")) for _ in range(2)]
        await started.wait()
        await asyncio.sleep(0)
        self.assertEqual((await service.get("600000"))[2], "paper_supply_busy")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(service.in_flight, 0)

    async def test_actual_paper_market_and_limit_paths_consume_one_packet(self):
        mapping = {"market_quotes": quote_doc(close=10.5), "stock_daily_quotes": bar_doc()}
        names = ("_load_cn_candidates", "_get_last_price_detail", "_get_today_price_range_detail")
        ns = load_paper_functions(names, mapping, NOW)
        service = self.make_supply()
        with patch.object(supply, "get_paper_quote", service.get):
            price, basis, reason = await ns["_get_last_price_detail"]("600000", "CN", True)
            span, range_basis, range_reason = await ns["_get_today_price_range_detail"]("600000", "CN")
        self.assertEqual((price, reason), (10.2, "ok"))
        self.assertEqual(span, {"high": 11, "low": 9})
        self.assertEqual(range_reason, "ok")
        self.assertEqual(basis, range_basis)
        self.assertEqual(service.fetch.await_count, 2)  # two independent order types

    async def test_failure_never_falls_back_to_database_and_display_never_fetches(self):
        good = quote_doc(trade_date=NOW.date().isoformat(), fetched_at=NOW.isoformat())
        names = ("_load_cn_candidates", "_get_last_price_detail", "_get_today_price_range_detail")
        ns = load_paper_functions(names, {"market_quotes": good}, NOW)
        with patch.object(supply, "get_paper_quote", AsyncMock(return_value=(True, None, "security_suspended"))) as get:
            self.assertIsNone((await ns["_get_last_price_detail"]("600000", "CN", True))[0])
            self.assertIsNone((await ns["_get_today_price_range_detail"]("600000", "CN"))[0])
            count = get.await_count
            self.assertEqual((await ns["_get_last_price_detail"]("600000", "CN", False))[0], 10)
            self.assertEqual(get.await_count, count)

    async def test_preview_price_and_range_share_one_fetch_not_old_display_price(self):
        extra = {
            "router": types.SimpleNamespace(get=lambda *a, **k: lambda fn: fn),
            "Query": lambda *a, **k: None, "Depends": lambda *a, **k: None,
            "get_current_user": None, "ok": lambda data: data,
            "_resolve_market_and_code": lambda code, market: ("CN", code, False),
        }
        ns = load_paper_functions(("_load_cn_candidates", "get_quote"), {}, NOW, extra)
        service = self.make_supply()
        with patch.object(supply, "get_paper_quote", service.get):
            preview = await ns["get_quote"]("600000", "CN", {"id": "test"})
        self.assertEqual(preview["current_price"], 10.2)
        self.assertEqual((preview["high"], preview["low"]), (11, 9))
        self.assertEqual(preview["pricing_basis"]["source"], "sina_l1_on_demand")
        service.fetch.assert_awaited_once()


class CalendarBehavior(unittest.IsolatedAsyncioTestCase):
    async def test_verified_cache_avoids_refresh_and_explicit_holiday_denies(self):
        for value in (True, False):
            fetch = AsyncMock(return_value=NOW.date().isoformat())
            calendar = supply.VerifiedTradeCalendar(AsyncMock(return_value=value), fetch)
            self.assertIs(await calendar(NOW), value)
            fetch.assert_not_called()

    async def test_stale_cache_uses_public_source_once_and_refreshes_next_day(self):
        fetch = AsyncMock(side_effect=[NOW.date().isoformat(), (NOW+timedelta(days=1)).date().isoformat()])
        calendar = supply.VerifiedTradeCalendar(AsyncMock(return_value=None), fetch)
        self.assertTrue(await calendar(NOW))
        self.assertTrue(await calendar(NOW))
        self.assertEqual(fetch.await_count, 1)
        self.assertTrue(await calendar(NOW+timedelta(days=1)))
        self.assertEqual(fetch.await_count, 2)

    async def test_failed_public_calendar_does_not_retry_or_guess_weekday(self):
        for value in (None, "2026-10-30", "2026-11-03", "bad"):
            fetch = AsyncMock(return_value=value)
            calendar = supply.VerifiedTradeCalendar(AsyncMock(return_value=None), fetch)
            self.assertFalse(await calendar(NOW))
            self.assertFalse(await calendar(NOW))
            self.assertEqual(fetch.await_count, 1)


class HttpAdapterBehavior(unittest.IsolatedAsyncioTestCase):
    async def test_fixed_https_referer_no_proxy_redirects_or_retry(self):
        try:
            import httpx
        except ImportError:
            self.skipTest("httpx requires the application dependencies")
        real_client = httpx.AsyncClient
        options, requests = [], []
        def handler(request):
            requests.append(request)
            self.assertEqual(str(request.url), supply.ENDPOINT+"sh600000")
            self.assertEqual(request.headers["referer"], "https://finance.sina.com.cn/")
            return httpx.Response(200, content=packet())
        def factory(**kw):
            options.append(kw)
            return real_client(transport=httpx.MockTransport(handler), **kw)
        with patch.object(httpx, "AsyncClient", factory):
            self.assertEqual(await supply.fetch_sina("600000"), packet())
        self.assertEqual(len(requests), 1)
        self.assertIs(options[0]["trust_env"], False)
        self.assertIs(options[0]["follow_redirects"], False)

    async def test_http_errors_redirects_and_oversize_responses_rejected(self):
        try:
            import httpx
        except ImportError:
            self.skipTest("httpx requires the application dependencies")
        real_client = httpx.AsyncClient
        for status, content in ((302, b''), (403, b''), (200, b'x'*(supply.MAX_RESPONSE_BYTES+1))):
            requests = []
            def handler(request):
                requests.append(request)
                return httpx.Response(status, content=content, headers={"Location":"https://example.org/"})
            def factory(**kw):
                return real_client(transport=httpx.MockTransport(handler), **kw)
            with patch.object(httpx, "AsyncClient", factory):
                with self.assertRaises((httpx.HTTPStatusError, ValueError)):
                    await supply.fetch_sina("600000")
            self.assertEqual(len(requests), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
