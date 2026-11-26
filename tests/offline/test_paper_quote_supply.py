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

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from app.services import paper_quote_supply as supply
from app.services.quote_contract import CN_TZ, quote_price_and_basis
import test_paper_pricing
test_paper_pricing.PAPER = REPO_ROOT / "app/routers/paper.py"
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


class P2HardeningAndObservabilityTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_8192_bytes_accepted_and_8193_rejected(self):
        base = packet("600000")
        pad = supply.MAX_RESPONSE_BYTES - len(base)
        exact_8192 = packet("600000", **{"0": "测试" + ("X" * pad)})
        self.assertEqual(len(exact_8192), 8192)
        self.assertEqual(supply.parse_sina_quote(exact_8192, "600000", NOW)["close"], 10.2)

        over_8193 = packet("600000", **{"0": "测试" + ("X" * (pad + 1))})
        self.assertEqual(len(over_8193), 8193)
        with self.assertRaisesRegex(ValueError, "^response_size_invalid$"):
            supply.parse_sina_quote(over_8193, "600000", NOW)

    async def test_invalid_gb18030_bytes_raise_response_encoding_invalid(self):
        bad_raw = b'var hq_str_sh600000="\xff\xfe\xfd' + b",0" * 32 + b'";\n'
        with self.assertRaisesRegex(ValueError, "^response_encoding_invalid$"):
            supply.parse_sina_quote(bad_raw, "600000", NOW)

        service = supply.PaperQuoteSupply(
            clock=lambda: NOW,
            permit=lambda now: permission(),
            calendar=AsyncMock(return_value=True),
            fetch=AsyncMock(return_value=bad_raw),
        )
        selected, quote, reason = await service.get("600000")
        self.assertEqual((selected, quote, reason), (True, None, "response_encoding_invalid"))

    async def test_observability_summary_and_counters_redact_raw_payload_and_name(self):
        raw_ok = packet("159865", **{"0": "机密证券名称不外泄"})
        raw_susp = packet("601059", status="03", **{"0": "停牌名称不外泄"})
        service = supply.PaperQuoteSupply(
            clock=lambda: NOW,
            permit=lambda now: permission(),
            calendar=AsyncMock(return_value=True),
            fetch=AsyncMock(side_effect=[raw_ok, raw_susp]),
        )
        selected_ok, doc_ok, reason_ok = await service.get("159865")
        self.assertTrue(selected_ok)
        self.assertIsNotNone(doc_ok)
        self.assertEqual(reason_ok, "ok")

        obs_ok = service.snapshot_observability()
        self.assertEqual(obs_ok["counts"], {"ok": 1})
        last_ok = obs_ok["last_observation"]
        self.assertEqual(last_ok["code"], "159865")
        self.assertTrue(last_ok["available"])
        self.assertEqual(last_ok["source"], "sina_l1")
        self.assertEqual(last_ok["trading_status"], "normal")
        self.assertEqual(last_ok["trading_status_raw"], "00")
        self.assertEqual(len(last_ok["payload_sha256"]), 64)
        self.assertNotIn("机密证券名称不外泄", json.dumps(obs_ok, ensure_ascii=False))
        self.assertNotIn("hq.sinajs.cn", json.dumps(obs_ok, ensure_ascii=False))

        selected_s, doc_s, reason_s = await service.get("601059")
        self.assertEqual((selected_s, doc_s, reason_s), (True, None, "security_suspended"))
        obs_after = service.snapshot_observability()
        self.assertEqual(obs_after["counts"], {"ok": 1, "security_suspended": 1})
        self.assertFalse(obs_after["last_observation"]["available"])
        self.assertIsNone(obs_after["last_observation"]["payload_sha256"])
        self.assertNotIn("停牌名称不外泄", json.dumps(obs_after, ensure_ascii=False))

    def test_sh_sz_etf_prefix_matrix_and_bse_closed_rejection(self):
        expected = {
            "600000": "sh600000",
            "688001": "sh688001",
            "501000": "sh501000",
            "510300": "sh510300",
            "520000": "sh520000",
            "560000": "sh560000",
            "588000": "sh588000",
            "000001": "sz000001",
            "300750": "sz300750",
            "159865": "sz159865",
            "160101": "sz160101",
            "180101": "sz180101",
        }
        for code, sym in expected.items():
            self.assertEqual(supply.source_symbol(code), sym)
        for bse in ("920363", "830799", "870001", "430001"):
            with self.assertRaisesRegex(ValueError, "^symbol_unsupported$"):
                supply.source_symbol(bse)


class SchedulePermitRecoveryTests(unittest.TestCase):
    @staticmethod
    def _load_scheduler_module():
        import importlib.util

        repo_root = Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location(
            "schedule_paper_quote_permit_mod",
            repo_root / "deploy" / "schedule_paper_quote_permit.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _setup_permit_env(self, root: Path, permit_doc: dict):
        runtime_dir = root / "runtime"
        runtime_dir.mkdir(mode=0o700)
        backup_dir = root / "backups"
        permit_path = runtime_dir / "paper-quote-supply.json"
        raw = json.dumps(permit_doc, sort_keys=True, separators=(",", ":")).encode()
        permit_path.write_bytes(raw)
        permit_path.chmod(0o600)
        return runtime_dir, backup_dir, permit_path, raw

    def _fake_run(self, runtime_dir: Path, permit_path: Path):
        inspect_payload = json.dumps([
            {
                "Id": "backend-test-id",
                "Image": "img:test",
                "State": {"Running": True, "Health": {"Status": "healthy"}},
                "Mounts": [{"Destination": "/app/runtime", "Type": "bind", "RW": True, "Source": str(runtime_dir)}],
            }
        ])

        def runner(args, *, check=True):
            if args[:2] == ["docker", "inspect"]:
                return types.SimpleNamespace(returncode=0, stdout=inspect_payload, stderr="")
            if args[:2] == ["docker", "exec"]:
                doc = json.loads(permit_path.read_bytes())
                verify_out = json.dumps({
                    "sha256": __import__("hashlib").sha256(permit_path.read_bytes()).hexdigest(),
                    "status": "paper_supply_not_started",
                    "not_before": doc["not_before"],
                    "expires_at": doc["expires_at"],
                })
                return types.SimpleNamespace(returncode=0, stdout=verify_out, stderr="")
            raise AssertionError(f"unexpected command: {args}")

        return runner

    def test_expired_previous_refused_without_explicit_flag(self):
        mod = self._load_scheduler_module()
        now = datetime(2026, 11, 22, 12, 0, tzinfo=CN_TZ)  # Sunday
        expired_doc = {
            "version": 2,
            "enabled": True,
            "source": "sina_l1",
            "scope": "paper_execution_sh_sz",
            "not_before": "2026-11-10T09:30:00+08:00",
            "expires_at": "2026-11-16T20:00:00+08:00",
            "renewal_of_sha256": "a" * 64,
        }
        with tempfile.TemporaryDirectory() as tmp:
            runtime_dir, backup_dir, permit_path, raw = self._setup_permit_env(Path(tmp), expired_doc)
            argv = [
                "--not-before", "2026-11-23T09:30:00+08:00",
                "--expires-at", "2026-11-27T20:00:00+08:00",
                "--previous-sha256", mod.digest(raw),
                "--execute",
            ]
            with patch.object(mod, "now_cn", return_value=now), \
                 patch.object(mod, "BACKUP_PARENT", backup_dir), \
                 patch.object(mod, "run", side_effect=self._fake_run(runtime_dir, permit_path)):
                with self.assertRaises(SystemExit) as ctx:
                    mod.main(argv)
            self.assertEqual(str(ctx.exception), "previous_permit_expired")
            self.assertEqual(permit_path.read_bytes(), raw)

    def test_allow_expired_previous_recovers_atomically_and_preserves_guards(self):
        mod = self._load_scheduler_module()
        now = datetime(2026, 11, 22, 12, 0, tzinfo=CN_TZ)  # Sunday
        expired_doc = {
            "version": 2,
            "enabled": True,
            "source": "sina_l1",
            "scope": "paper_execution_sh_sz",
            "not_before": "2026-11-10T09:30:00+08:00",
            "expires_at": "2026-11-16T20:00:00+08:00",
            "renewal_of_sha256": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as tmp:
            runtime_dir, backup_dir, permit_path, raw = self._setup_permit_env(Path(tmp), expired_doc)
            # 1) Wrong sha256 still rejected even with --allow-expired-previous
            with patch.object(mod, "now_cn", return_value=now), \
                 patch.object(mod, "BACKUP_PARENT", backup_dir), \
                 patch.object(mod, "run", side_effect=self._fake_run(runtime_dir, permit_path)):
                with self.assertRaises(SystemExit) as ctx:
                    mod.main([
                        "--not-before", "2026-11-23T09:30:00+08:00",
                        "--expires-at", "2026-11-27T20:00:00+08:00",
                        "--previous-sha256", "0" * 64,
                        "--allow-expired-previous",
                    ])
                self.assertEqual(str(ctx.exception), "previous_permit_changed")

                # 2) Window > 7d still rejected
                with self.assertRaises(SystemExit) as ctx2:
                    mod.main([
                        "--not-before", "2026-11-23T09:30:00+08:00",
                        "--expires-at", "2026-12-01T09:30:00+08:00",
                        "--previous-sha256", mod.digest(raw),
                        "--allow-expired-previous",
                    ])
                self.assertEqual(str(ctx2.exception), "permit_window_invalid")

                # 3) Explicit manual recovery succeeds with backup and atomic replace
                import io, contextlib

                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    rc = mod.main([
                        "--not-before", "2026-11-23T09:30:00+08:00",
                        "--expires-at", "2026-11-27T20:00:00+08:00",
                        "--previous-sha256", mod.digest(raw),
                        "--allow-expired-previous",
                        "--execute",
                    ])
                self.assertEqual(rc, 0)
                out = json.loads(buf.getvalue().strip())
                self.assertTrue(out["ok"])
                self.assertTrue(out["previous_expired"])
                self.assertTrue(out["allow_expired_previous"])
                new_doc = json.loads(permit_path.read_bytes())
                self.assertEqual(new_doc["version"], 2)
                self.assertEqual(new_doc["renewal_of_sha256"], mod.digest(raw))
                self.assertEqual(new_doc["not_before"], "2026-11-23T09:30:00+08:00")
                self.assertEqual(new_doc["expires_at"], "2026-11-27T20:00:00+08:00")
                backup_root = Path(out["backup"])
                self.assertEqual((backup_root / "paper-quote-supply.json").read_bytes(), raw)
                self.assertTrue((backup_root / "completion.json").is_file())

    def test_allow_expired_previous_still_rejects_v1_permit(self):
        mod = self._load_scheduler_module()
        now = datetime(2026, 11, 22, 12, 0, tzinfo=CN_TZ)
        v1_doc = {
            "version": 1,
            "enabled": True,
            "source": "sina_l1",
            "scope": "paper_execution_sh_sz",
            "not_before": "2026-11-10T09:30:00+08:00",
            "expires_at": "2026-11-11T09:30:00+08:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            runtime_dir, backup_dir, permit_path, raw = self._setup_permit_env(Path(tmp), v1_doc)
            with patch.object(mod, "now_cn", return_value=now), \
                 patch.object(mod, "BACKUP_PARENT", backup_dir), \
                 patch.object(mod, "run", side_effect=self._fake_run(runtime_dir, permit_path)):
                with self.assertRaises(SystemExit) as ctx:
                    mod.main([
                        "--not-before", "2026-11-23T09:30:00+08:00",
                        "--expires-at", "2026-11-27T20:00:00+08:00",
                        "--previous-sha256", mod.digest(raw),
                        "--allow-expired-previous",
                    ])
            self.assertEqual(str(ctx.exception), "previous_permit_not_v2_active")


class CalendarRetryAndTradingCalendarTests(unittest.IsolatedAsyncioTestCase):
    async def test_calendar_retries_after_300s_cooldown_and_caps_at_3_per_day(self):
        # First call times out; within <300s no retry; at 300s retries and recovers
        fetch = AsyncMock(side_effect=[TimeoutError("transient"), NOW.date().isoformat()])
        calendar = supply.VerifiedTradeCalendar(AsyncMock(return_value=None), fetch)
        self.assertFalse(await calendar(NOW))
        self.assertEqual(fetch.await_count, 1)
        self.assertFalse(await calendar(NOW + timedelta(seconds=299)))
        self.assertEqual(fetch.await_count, 1)
        self.assertTrue(await calendar(NOW + timedelta(seconds=300)))
        self.assertEqual(fetch.await_count, 2)
        # Once verified for today, no further fetches today
        self.assertTrue(await calendar(NOW + timedelta(seconds=1000)))
        self.assertEqual(fetch.await_count, 2)

        # Persistent failure capped at 3 attempts/day, resets next CN day
        always_fail = AsyncMock(side_effect=RuntimeError("down"))
        cal2 = supply.VerifiedTradeCalendar(AsyncMock(return_value=None), always_fail)
        for step in (0, 150, 300, 600, 900, 1500):
            self.assertFalse(await cal2(NOW + timedelta(seconds=step)))
        self.assertEqual(always_fail.await_count, 3)
        # Next day resets quota
        self.assertFalse(await cal2(NOW + timedelta(days=1)))
        self.assertEqual(always_fail.await_count, 4)

    async def test_trading_calendar_bypasses_tushare_when_restricted_and_uses_cn_tz(self):
        from app.services.trading_calendar_service import TradingCalendarService

        svc = TradingCalendarService()
        # Simulate UTC midnight crossing where CN_TZ is already 2026-11-26 01:00
        cn_early = datetime(2026, 11, 26, 1, 0, tzinfo=CN_TZ)
        with patch.dict("os.environ", {"TA_TUSHARE_ROUTE": "mainland_unix"}, clear=False), \
             patch.object(TradingCalendarService, "_now_cn", return_value=cn_early), \
             patch.object(svc, "_fetch_from_tushare", new=AsyncMock(return_value="2026-11-25")) as ts_mock, \
             patch.object(svc, "_fetch_from_akshare", new=AsyncMock(return_value="2026-11-25")) as ak_mock:
            self.assertTrue(svc._is_tushare_restricted())
            self.assertEqual(svc._today_str(), "2026-11-26")
            trade_date, source = await svc._fetch_last_trade_date()
            ts_mock.assert_not_awaited()
            ak_mock.assert_awaited_once()
            self.assertEqual((trade_date, source), ("2026-11-25", "akshare"))

    async def test_trading_calendar_akshare_enforces_explicit_timeout(self):
        import time as _time
        from app.services.trading_calendar_service import TradingCalendarService

        svc = TradingCalendarService()
        fake_ak = types.SimpleNamespace(tool_trade_date_hist_sina=lambda: _time.sleep(0.3))
        fake_pd = types.SimpleNamespace()
        with patch.dict(sys.modules, {"akshare": fake_ak, "pandas": fake_pd}):
            result = await svc._fetch_from_akshare(timeout=0.05)
        self.assertIsNone(result)


class GranularPermitAndSupplyStatusApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_supply_get_records_exact_permit_failure_reasons(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "permit.json"
            service = supply.PaperQuoteSupply(
                clock=lambda: NOW,
                permit=lambda now: supply.permit_status(now, path),
                calendar=AsyncMock(return_value=True),
                fetch=AsyncMock(return_value=packet()),
            )
            # 1) Missing file -> paper_supply_disabled
            self.assertEqual(await service.get("600000"), (False, None, "paper_supply_disabled"))

            # 2) Expired permit -> paper_supply_expired
            path.write_text(json.dumps(permission(expires_at=(NOW - timedelta(minutes=1)).isoformat())))
            path.chmod(0o600)
            self.assertEqual(await service.get("600000"), (False, None, "paper_supply_expired"))

            # 3) Not started -> paper_supply_not_started
            path.write_text(json.dumps(permission(
                not_before=(NOW + timedelta(hours=1)).isoformat(),
                expires_at=(NOW + timedelta(hours=5)).isoformat(),
            )))
            self.assertEqual(await service.get("600000"), (False, None, "paper_supply_not_started"))

            # 4) Corrupt file -> paper_supply_invalid
            path.write_bytes(b"{broken")
            self.assertEqual(await service.get("600000"), (False, None, "paper_supply_invalid"))

            obs = service.snapshot_observability()
            self.assertEqual(obs["counts"], {
                "paper_supply_disabled": 1,
                "paper_supply_expired": 1,
                "paper_supply_not_started": 1,
                "paper_supply_invalid": 1,
            })
            self.assertEqual(obs["last_observation"]["reason"], "paper_supply_invalid")
            service.fetch.assert_not_called()

    async def test_supply_status_endpoint_contract_warning_and_redaction(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "permit.json"
            # Active permit with 12h remaining (<36h -> expiry_warning=True)
            doc = permission(
                version=2,
                renewal_of_sha256="f" * 64,
                not_before=(NOW - timedelta(hours=2)).isoformat(),
                expires_at=(NOW + timedelta(hours=12)).isoformat(),
            )
            path.write_text(json.dumps(doc))
            path.chmod(0o600)

            cal_fetch = AsyncMock(return_value=NOW.date().isoformat())
            quote_fetch = AsyncMock(return_value=packet("600000"))
            service = supply.PaperQuoteSupply(
                clock=lambda: NOW,
                permit=lambda now: supply.permit_status(now, path),
                calendar=supply.VerifiedTradeCalendar(AsyncMock(return_value=True), cal_fetch),
                fetch=quote_fetch,
            )
            # Populate last_observation containing payload_sha256 internally
            self.assertEqual((await service.get("600000"))[2], "ok")
            self.assertEqual(quote_fetch.await_count, 1)

            # Call real get_supply_status from app/routers/paper.py
            extra = {
                "router": types.SimpleNamespace(get=lambda *a, **k: lambda fn: fn),
                "Depends": lambda *a, **k: None,
                "get_current_user": None,
                "ok": lambda data=None, message="ok": {"success": True, "data": data, "message": message},
            }
            ns = load_paper_functions(("get_supply_status",), {}, NOW, extra)

            async def _status(now=None, **_kw):
                return await service.status_summary(now=now, path=path)

            with patch.object(supply, "get_paper_supply_status", side_effect=_status):
                envelope = await ns["get_supply_status"]({"id": "user-1"})

            # Zero additional network calls from status endpoint
            self.assertEqual(quote_fetch.await_count, 1)
            cal_fetch.assert_not_called()

            # Verify locked envelope schema
            self.assertTrue(envelope["success"])
            self.assertIsInstance(envelope["message"], str)
            data = envelope["data"]
            expected_keys = {
                "in_session", "permit_status", "expires_at", "remaining_hours",
                "expiry_warning", "calendar_verified", "supported_scope",
                "message", "observability",
            }
            self.assertEqual(set(data.keys()), expected_keys)
            self.assertTrue(data["in_session"])
            self.assertEqual(data["permit_status"], "ok")
            self.assertEqual(data["remaining_hours"], 12.0)
            self.assertTrue(data["expiry_warning"])
            self.assertTrue(data["calendar_verified"])
            self.assertEqual(data["supported_scope"], "paper_execution_sh_sz")
            self.assertIn("92/83/87/43", data["message"])

            # Verify zero leakage of paths, hashes, URLs, or secrets
            serialized = json.dumps(envelope, ensure_ascii=False)
            self.assertNotIn("payload_sha256", serialized)
            self.assertNotIn("f" * 64, serialized)
            self.assertNotIn(str(path), serialized)
            self.assertNotIn("hq.sinajs.cn", serialized)

            # >36h remaining -> expiry_warning=False
            doc_long = permission(
                version=2,
                renewal_of_sha256="e" * 64,
                not_before=(NOW - timedelta(hours=1)).isoformat(),
                expires_at=(NOW + timedelta(hours=48)).isoformat(),
            )
            path.write_text(json.dumps(doc_long))
            summary_long = await service.status_summary(now=NOW, path=path)
            self.assertEqual(summary_long["remaining_hours"], 48.0)
            self.assertFalse(summary_long["expiry_warning"])

            # Expired -> remaining_hours=0.0, expiry_warning=True
            doc_exp = permission(
                version=2,
                renewal_of_sha256="d" * 64,
                not_before=(NOW - timedelta(hours=48)).isoformat(),
                expires_at=(NOW - timedelta(hours=1)).isoformat(),
            )
            path.write_text(json.dumps(doc_exp))
            summary_exp = await service.status_summary(now=NOW, path=path)
            self.assertEqual(summary_exp["permit_status"], "paper_supply_expired")
            self.assertEqual(summary_exp["remaining_hours"], 0.0)
            self.assertTrue(summary_exp["expiry_warning"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
