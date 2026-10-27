#!/usr/bin/env python3
"""行情时间/字段契约 的行为测试（纯标准库，可离线跑）。

本轮（v4）重点：King 决定的「有延迟边界的近实时快照」——
不伪造 quote_time、把延迟边界写成数据、超期即拒绝、失败不续期。

运行：python3 -B tests/offline/test_quote_contract.py
"""

import importlib.util
import sys
import types
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(module_name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


if "bson" not in sys.modules:
    _bson = types.ModuleType("bson")

    class _ObjectId(str):
        pass

    _bson.ObjectId = _ObjectId
    sys.modules["bson"] = _bson

qc = _load("quote_contract", "app/services/quote_contract.py")
sched = _load("scheduler_runtime", "app/services/scheduler_runtime.py")

CN_TZ = qc.CN_TZ


def at(hour, minute, day=27):
    return datetime(2026, 10, day, hour, minute, tzinfo=CN_TZ)


NOW = at(10, 0)
TODAY = "2026-10-27"


def naive_utc(y, mo, d, h, mi, s=0):
    return datetime(y, mo, d, h, mi, s)


def apply_set_idempotent(old: dict, doc: dict) -> dict:
    merged = dict(old)
    for key, value in doc.items():
        if key != "_id":
            merged[key] = value
    return merged


def snapshot_doc(fetched_minutes_ago: int, interval: int = 1800, **extra):
    doc = {
        "code": "600000",
        "trade_date": TODAY,
        "high": 9.0,
        "low": 7.0,
        "close": 8.0,
        "quote_time_source": "fetch",
        "fetched_at": (NOW - timedelta(minutes=fetched_minutes_ago)).isoformat(),
        "snapshot_interval_seconds": interval,
    }
    doc.update(extra)
    return doc


class TestParseQuoteTime(unittest.TestCase):
    def test_bson_naive_datetime_treated_as_utc(self):
        parsed = qc.parse_quote_time(naive_utc(2026, 10, 27, 1, 55))
        self.assertEqual(parsed.utcoffset(), timedelta(0))
        self.assertEqual(parsed.astimezone(CN_TZ).strftime("%H:%M"), "09:55")

    def test_iso_with_offset(self):
        self.assertEqual(
            qc.parse_quote_time("2026-10-27T09:58:00+08:00").utcoffset(), timedelta(hours=8)
        )

    def test_iso_z(self):
        self.assertEqual(qc.parse_quote_time("2026-10-27T01:58:00Z").utcoffset(), timedelta(0))

    def test_invalid_values_return_none(self):
        for value in ("", "   ", "bad-value", None, 12345, 1.5, True, [], {}):
            with self.subTest(value=value):
                self.assertIsNone(qc.parse_quote_time(value))


class TestCoerceTradeDate(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(qc.coerce_trade_date("2026-10-27"), TODAY)
        self.assertEqual(qc.coerce_trade_date("20261027"), TODAY)
        self.assertEqual(qc.coerce_trade_date(date(2026, 10, 27)), TODAY)
        self.assertEqual(qc.coerce_trade_date(at(9, 0)), TODAY)

    def test_unrecognised(self):
        for value in (None, "", "2026-1-7", "abc", 20261027):
            with self.subTest(value=value):
                self.assertIsNone(qc.coerce_trade_date(value))


class TestValidatePrice(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(qc.validate_price(10)[0], 10.0)
        self.assertEqual(qc.validate_price("10.5")[0], 10.5)

    def test_infinite_rejected(self):
        for bad in (float("inf"), float("-inf"), float("nan")):
            with self.subTest(bad=bad):
                self.assertIsNone(qc.validate_price(bad)[0])

    def test_bool_rejected(self):
        self.assertEqual(qc.validate_price(True)[1], "bool_not_price")
        self.assertIsNone(qc.validate_price(False)[0])

    def test_non_positive_and_none(self):
        self.assertEqual(qc.validate_price(0)[1], "non_positive")
        self.assertEqual(qc.validate_price(None)[1], "missing")


class TestSnapshotBoundary(unittest.TestCase):
    """无 quote_time 时，放行必须有可计算的延迟边界。"""

    def test_accepted_within_declared_bound(self):
        ok, reason = qc.quote_is_usable(snapshot_doc(20), NOW)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "ok_fetch_snapshot:1920s")

    def test_rejected_beyond_bound(self):
        ok, reason = qc.quote_is_usable(snapshot_doc(40), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("snapshot_too_old"), reason)

    def test_custom_interval_shrinks_bound(self):
        # 周期 6 分钟 → 允许 8 分钟；10 分钟前采集即超期
        ok, reason = qc.quote_is_usable(snapshot_doc(10, interval=360), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("snapshot_too_old"), reason)
        self.assertIn("480s", reason)

    def test_absurd_interval_rejected(self):
        ok, reason = qc.quote_is_usable(snapshot_doc(5, interval=24 * 3600), NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "snapshot_interval_invalid")

    def test_missing_interval_uses_default(self):
        doc = snapshot_doc(20)
        doc.pop("snapshot_interval_seconds")
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "ok_fetch_snapshot:1920s")

    def test_outside_session_rejected(self):
        doc = snapshot_doc(5)
        ok, reason = qc.quote_is_usable(doc, at(8, 50))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("outside_snapshot_session"), reason)

    def test_source_must_be_fetch(self):
        doc = snapshot_doc(5, quote_time_source="upstream")
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "quote_time_missing")

    def test_fetched_at_missing_rejected(self):
        doc = snapshot_doc(5)
        doc.pop("fetched_at")
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "fetched_at_missing")

    def test_trade_date_mismatch_rejected_even_if_fresh(self):
        doc = snapshot_doc(1, trade_date="2026-10-26")
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("trade_date_mismatch"), reason)

    def test_upstream_quote_time_takes_precedence(self):
        """有上游行情时间时，以它为准；采集时间再新也不能改变判定。"""
        doc = snapshot_doc(40, quote_time=(NOW - timedelta(minutes=2)).isoformat())
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "ok")

    def test_stale_upstream_quote_time_rejected(self):
        doc = snapshot_doc(1, quote_time=(NOW - timedelta(minutes=60)).isoformat())
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_stale"), reason)


class TestPricingBasis(unittest.TestCase):
    def test_fetch_basis_notice_mentions_bound(self):
        basis = qc.quote_pricing_basis(snapshot_doc(20), NOW)
        self.assertEqual(basis["source"], "fetch")
        self.assertEqual(basis["max_delay_seconds"], 1920)
        self.assertEqual(basis["delay_seconds"], 1200)
        self.assertIn("32 分钟", basis["notice"])

    def test_upstream_basis_has_no_notice(self):
        doc = snapshot_doc(1, quote_time=(NOW - timedelta(minutes=2)).isoformat())
        basis = qc.quote_pricing_basis(doc, NOW)
        self.assertEqual(basis["source"], "upstream")
        self.assertIsNone(basis["notice"])

    def test_invalid_interval_yields_no_basis(self):
        self.assertIsNone(qc.quote_pricing_basis(snapshot_doc(5, interval=0), NOW))


class TestPrepareQuoteWrite(unittest.TestCase):
    OLD_DOC = {
        "_id": "x",
        "code": "600000",
        "close": 8.0,
        "high": 9.0,
        "low": 7.0,
        "trade_date": "2026-10-26",
        "quote_time_source": "fetch",
        "fetched_at": "2026-10-26T14:30:00+08:00",
        "snapshot_interval_seconds": 1800,
    }

    def test_partial_snapshot_rejected_and_old_doc_untouched(self):
        payload = {"price": 12.0, "close": None, "high": None, "low": None}
        doc, reason = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertIsNone(doc)
        self.assertTrue(reason.startswith("incomplete_snapshot"), reason)

        merged = self.OLD_DOC if doc is None else apply_set_idempotent(self.OLD_DOC, doc)
        self.assertEqual(merged["trade_date"], "2026-10-26")
        self.assertEqual(merged["fetched_at"], "2026-10-26T14:30:00+08:00")
        ok, _ = qc.quote_is_usable(merged, NOW)
        self.assertFalse(ok)

    def test_missing_one_field_rejected(self):
        doc, reason = qc.prepare_quote_write({"close": 8.0, "high": 9.0}, "600000", TODAY, NOW)
        self.assertIsNone(doc)
        self.assertIn("low_missing", reason)

    def test_does_not_fabricate_quote_time(self):
        doc, reason = qc.prepare_quote_write(
            {"close": 8.0, "high": 9.0, "low": 7.0, "volume": 1000}, "600000", TODAY, NOW
        )
        self.assertEqual(reason, "ok")
        self.assertNotIn("quote_time", doc)
        self.assertEqual(doc["quote_time_source"], "fetch")
        self.assertEqual(doc["fetched_at"], NOW.isoformat())
        self.assertEqual(doc["trade_date"], TODAY)

    def test_declared_interval_written(self):
        doc, _ = qc.prepare_quote_write(
            {"close": 8.0, "high": 9.0, "low": 7.0}, "600000", TODAY, NOW, 360
        )
        self.assertEqual(doc["snapshot_interval_seconds"], 360)

    def test_upstream_quote_time_preserved(self):
        payload = {
            "close": 8.0, "high": 9.0, "low": 7.0,
            "quote_time": "2026-10-27T09:58:00+08:00",
        }
        doc, _ = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertEqual(doc["quote_time_source"], "upstream")
        self.assertTrue(doc["quote_time"].startswith("2026-10-27T09:58:00"))

    def test_alias_filled(self):
        payload = {"price": 11.3, "close": 11.3, "open_price": 11.1,
                   "high_price": 11.4, "low_price": 11.0}
        doc, reason = qc.prepare_quote_write(payload, "000001", TODAY, NOW)
        self.assertEqual(reason, "ok")
        self.assertEqual(doc["high"], 11.4)
        self.assertEqual(doc["low"], 11.0)

    def test_volume_normalised_from_shares(self):
        payload = {"close": 8.0, "high": 9.0, "low": 7.0,
                   "volume": 123400, "volume_unit": "share"}
        doc, _ = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertAlmostEqual(doc["volume"], 1234.0)

    def test_volume_lot_untouched(self):
        payload = {"close": 8.0, "high": 9.0, "low": 7.0, "volume": 1234, "volume_unit": "lot"}
        doc, _ = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertEqual(doc["volume"], 1234)

    def test_non_finite_price_rejected(self):
        payload = {"close": float("inf"), "high": 9.0, "low": 7.0}
        doc, reason = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertIsNone(doc)
        self.assertIn("not_finite", reason)


class TestDailyFallback(unittest.TestCase):
    def _bar(self, written_at):
        return {
            "trade_date": TODAY,
            "open": 9.5, "high": 11.0, "low": 9.0, "close": 10.0,
            "updated_at": written_at,
        }

    def test_intraday_bar_rejected_at_afternoon(self):
        ok, reason = qc.daily_bar_is_final(self._bar("2026-10-27T09:35:00+08:00"), at(14, 0))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_written_intraday"), reason)

    def test_after_close_bar_accepted(self):
        ok, reason = qc.daily_bar_is_final(self._bar("2026-10-27T18:05:00+08:00"), at(19, 0))
        self.assertTrue(ok, reason)

    def test_yesterday_bar_rejected(self):
        bar = self._bar("2026-10-26T18:05:00+08:00")
        bar["trade_date"] = "2026-10-26"
        ok, reason = qc.daily_bar_is_final(bar, at(19, 0))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_trade_date_mismatch"), reason)

    def test_bar_without_write_time_rejected(self):
        ok, reason = qc.daily_bar_is_final(self._bar(None), at(19, 0))
        self.assertFalse(ok)
        self.assertEqual(reason, "bar_write_time_missing")

    def test_fallback_gate(self):
        self.assertFalse(qc.daily_fallback_allowed(at(14, 0))[0])
        self.assertTrue(qc.daily_fallback_allowed(at(15, 30))[0])
        self.assertTrue(qc.daily_fallback_allowed(at(19, 0))[0])


class TestSyncRoundGate(unittest.TestCase):
    def test_pre_open_skipped(self):
        self.assertEqual(qc.sync_round_skip_reason(at(9, 0), True, "akshare"), "pre_open")

    def test_session_allowed(self):
        self.assertIsNone(qc.sync_round_skip_reason(at(9, 30), True, "akshare"))
        self.assertIsNone(qc.sync_round_skip_reason(at(14, 59), True, "tushare"))

    def test_after_close_skipped(self):
        self.assertEqual(qc.sync_round_skip_reason(at(15, 31), True, "akshare"), "after_close")

    def test_not_trade_day(self):
        self.assertEqual(qc.sync_round_skip_reason(at(10, 0), False, "akshare"), "not_trade_day")

    def test_fallback_calendar_is_unverified(self):
        self.assertEqual(
            qc.sync_round_skip_reason(at(10, 0), True, "fallback"), "calendar_unverified"
        )

    def test_missing_source_is_unverified(self):
        self.assertEqual(qc.sync_round_skip_reason(at(10, 0), True, None), "calendar_unverified")


class TestSchedulerOutcome(unittest.TestCase):
    def test_empty_pool_is_failed(self):
        status, message = sched.outcome(
            qc.build_failure_stats("标的池为空", "sync_realtime_quotes_no_symbols")
        )
        self.assertEqual(status, "failed")
        self.assertIn("标的池为空", message)

    def test_legacy_stats_with_only_errors_would_be_success(self):
        status, _ = sched.outcome({"error_count": 0, "errors": [{"error": "标的池为空"}]})
        self.assertEqual(status, "success")

    def test_skipped_maps_to_skipped(self):
        self.assertEqual(sched.outcome({"skipped": True, "reason": "pre_open"})[0], "skipped")

    def test_normal_result_is_success(self):
        self.assertEqual(
            sched.outcome({"success_count": 10, "error_count": 0, "errors": []})[0], "success"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
