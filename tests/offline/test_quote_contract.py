"""行情契约行为测试（纯标准库，不连数据库、不发网络请求）。

覆盖第三轮/第四轮评审复现过的反例：
- 采集时刻整批固定，写库不得重新起算；
- 通道切换必须 $unset 旧 quote_time；
- 定价依据必须与成交同源（不得二次查库补说明）；
- 展示路径与成交路径分离；
- 数值与类型边界（inf / bool / 非字符串来源 / 未来写入时间）。
"""

import os
import sys
import unittest

os.environ.setdefault("QUOTES_WRITE_ENABLED", "1")
from datetime import datetime, timedelta

sys.path.insert(0, ".")

from app.services.quote_contract import (  # noqa: E402
    CN_TZ,
    build_failure_stats,
    daily_bar_is_final,
    daily_fallback_allowed,
    display_price_and_basis,
    parse_quote_time,
    prepare_quote_write,
    quote_is_usable,
    quote_price_and_basis,
    quote_range_and_basis,
    resolve_cn_price,
    resolve_cn_range,
    snapshot_delay_bound,
    sync_round_skip_reason,
    validate_price,
    validate_price_range,
    bar_price_and_basis,
    coerce_trade_date,
)

NOW = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)   # 交易日盘中
CLOSE = datetime(2026, 10, 27, 15, 30, tzinfo=CN_TZ)


def q(**kw):
    base = {
        "code": "600000",
        "trade_date": "2026-10-27",
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
        "quote_time_source": "fetch",
        "trading_status": "trading",
        "fetched_at": (NOW - timedelta(minutes=10)).isoformat(),
        "snapshot_interval_seconds": 1800,
        "updated_at": NOW.isoformat(),
    }
    base.update(kw)
    return base


def bar(**kw):
    base = {
        "symbol": "600000", "trade_date": "2026-10-27",
        "open": 9.5, "high": 11.0, "low": 9.0, "close": 10.0,
        "is_final": True, "trading_status": "trading",
        "updated_at": "2026-10-27T15:10:00+08:00",
    }
    base.update(kw)
    return base


class TestTimeParsing(unittest.TestCase):
    def test_iso_with_offset(self):
        dt = parse_quote_time("2026-10-27T09:40:00+08:00")
        self.assertEqual((dt.hour, dt.minute), (9, 40))

    def test_iso_z(self):
        dt = parse_quote_time("2026-10-27T01:40:00Z").astimezone(CN_TZ)
        self.assertEqual((dt.hour, dt.minute), (9, 40))

    def test_naive_datetime_is_utc(self):
        raw = datetime(2026, 10, 27, 1, 40)
        dt = parse_quote_time(raw)
        self.assertEqual(dt.utcoffset().total_seconds(), 0)
        self.assertEqual(dt.astimezone(CN_TZ).hour, 9)

    def test_garbage(self):
        self.assertIsNone(parse_quote_time("bad-value"))
        self.assertIsNone(parse_quote_time(12345))
        self.assertIsNone(parse_quote_time(None))

    def test_trade_date_normalize(self):
        self.assertEqual(coerce_trade_date("20261027"), "2026-10-27")
        self.assertEqual(coerce_trade_date("2026/10/27"), "2026-10-27")
        self.assertIsNone(coerce_trade_date("27-10-2026"))


class TestPriceValidation(unittest.TestCase):
    def test_bool_is_not_price(self):
        self.assertEqual(validate_price(True), (None, "bool_not_price"))

    def test_inf_and_nan(self):
        self.assertEqual(validate_price(float("inf")), (None, "not_finite"))
        self.assertEqual(validate_price(float("nan")), (None, "not_finite"))

    def test_string_numeric(self):
        self.assertEqual(validate_price("10.5"), (10.5, "ok"))

    def test_zero_negative(self):
        self.assertEqual(validate_price(0)[1], "non_positive")
        self.assertEqual(validate_price(-1)[1], "non_positive")

    def test_range_low_gt_high(self):
        self.assertEqual(validate_price_range({"high": 9, "low": 11})[2], "low_gt_high")

    def test_range_close_out_of_bounds(self):
        _, _, reason = validate_price_range({"high": 11, "low": 9, "close": 100})
        self.assertTrue(reason.startswith("close_out_of_range"), reason)

    def test_range_ok(self):
        self.assertEqual(validate_price_range({"high": 11, "low": 9, "close": 10})[2], "ok")

    def test_range_missing_high(self):
        self.assertTrue(validate_price_range({"low": 9})[2].startswith("high_"))


class TestSnapshotBound(unittest.TestCase):
    def test_declared_interval(self):
        self.assertEqual(snapshot_delay_bound({"snapshot_interval_seconds": 1800}), (1920, "ok"))

    def test_absent_uses_default(self):
        self.assertEqual(snapshot_delay_bound({}), (1920, "ok"))

    def test_hard_cap(self):
        self.assertEqual(snapshot_delay_bound({"snapshot_interval_seconds": 2400}), (2400, "ok"))

    def test_over_cap_rejected(self):
        self.assertEqual(snapshot_delay_bound({"snapshot_interval_seconds": 3000})[1],
                         "snapshot_interval_invalid")

    def test_infinity_rejected_not_raised(self):
        self.assertEqual(snapshot_delay_bound({"snapshot_interval_seconds": float("inf")})[1],
                         "snapshot_interval_invalid")

    def test_bool_rejected(self):
        self.assertEqual(snapshot_delay_bound({"snapshot_interval_seconds": True})[1],
                         "snapshot_interval_invalid")

    def test_string_numeric(self):
        self.assertEqual(snapshot_delay_bound({"snapshot_interval_seconds": "1800"}), (1920, "ok"))


class TestQuoteUsable(unittest.TestCase):
    def test_fetch_snapshot_ok(self):
        ok, reason = quote_is_usable(q(), NOW)
        self.assertTrue(ok)
        self.assertEqual(reason, "ok_fetch_snapshot:1920s")

    def test_fetch_too_old(self):
        ok, reason = quote_is_usable(
            q(fetched_at=(NOW - timedelta(minutes=40)).isoformat()), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("snapshot_too_old"), reason)

    def test_trade_date_mismatch(self):
        ok, reason = quote_is_usable(q(trade_date="2026-10-26"), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("trade_date_mismatch"))

    def test_missing_source(self):
        item = q()
        item.pop("quote_time_source")
        self.assertEqual(quote_is_usable(item, NOW), (False, "quote_time_missing"))

    def test_source_not_string(self):
        ok, reason = quote_is_usable(q(quote_time_source=123), NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "quote_time_source_invalid")

    def test_outside_session(self):
        ok, reason = quote_is_usable(q(), datetime(2026, 10, 27, 16, 0, tzinfo=CN_TZ))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("outside_snapshot_session"))

    def test_upstream_quote_time_ok(self):
        ok, reason = quote_is_usable(
            q(quote_time="2026-10-27T13:50:00+08:00", quote_time_source="upstream"), NOW)
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_upstream_quote_time_stale(self):
        ok, reason = quote_is_usable(
            q(quote_time="2026-10-27T13:00:00+08:00", quote_time_source="upstream"), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_stale"))

    def test_invalid_quote_time_not_treated_as_snapshot(self):
        """上游给了时间但解析不了：不能悄悄退化成新鲜快照。"""
        ok, reason = quote_is_usable(q(quote_time="bad-value"), NOW)
        self.assertFalse(ok)
        self.assertEqual(reason, "quote_time_invalid")

    def test_future_fetch_rejected(self):
        ok, reason = quote_is_usable(
            q(fetched_at=(NOW + timedelta(minutes=10)).isoformat()), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("snapshot_from_future"))


class TestBasisSameSource(unittest.TestCase):
    def test_fetch_notice_no_overclaim(self):
        _, basis, reason = quote_price_and_basis(q(), NOW)
        self.assertEqual(reason, "ok")
        notice = basis["notice"]
        self.assertIn("采集", notice)
        self.assertIn("采集后有效期", notice)
        self.assertIn("上游报价时间未知", notice)
        self.assertNotIn("最大延迟", notice)

    def test_fetch_basis_delay(self):
        _, basis, _ = quote_price_and_basis(q(), NOW)
        self.assertEqual(basis["delay_seconds"], 600)
        self.assertEqual(basis["max_delay_seconds"], 1920)

    def test_upstream_channel(self):
        _, basis, _ = quote_price_and_basis(
            q(quote_time="2026-10-27T13:50:00+08:00"), NOW)
        self.assertEqual(basis["channel"], "upstream")

    def test_basis_comes_from_same_record(self):
        record = q(close=10.5, trade_date="2026-10-27")
        price, basis, _ = quote_price_and_basis(record, NOW)
        self.assertEqual(price, 10.5)
        self.assertEqual(basis["source"], "market_quotes")
        self.assertEqual(basis["trade_date"], "2026-10-27")

    def test_range_basis(self):
        rng, basis, _ = quote_range_and_basis(q(), NOW)
        self.assertEqual(rng, {"high": 11.0, "low": 9.0})
        self.assertEqual(basis["channel"], "fetch")

    def test_invalid_price_still_returns_basis_none(self):
        price, basis, reason = quote_price_and_basis(q(close=0), NOW)
        self.assertIsNone(price)
        self.assertIsNone(basis)
        self.assertIn("close", reason)

    def test_display_basis_marks_not_tradable(self):
        price, basis = display_price_and_basis(q(trade_date="2026-10-20"), "market_quotes", NOW)
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["channel"], "display")
        self.assertIn("不可用于成交", basis["notice"])

    def test_bar_basis_source_label(self):
        price, basis, reason = bar_price_and_basis(bar(), "stock_daily_quotes", CLOSE)
        self.assertEqual(reason, "ok")
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["source"], "stock_daily_quotes")
        self.assertEqual(basis["channel"], "daily_close")


class TestDailyBar(unittest.TestCase):
    def test_final_ok(self):
        self.assertEqual(daily_bar_is_final(bar(), CLOSE)[0], True)

    def test_intraday_write_rejected(self):
        ok, reason = daily_bar_is_final(bar(updated_at="2026-10-27T09:35:00+08:00"), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_written_intraday"))

    def test_future_write_rejected(self):
        ok, reason = daily_bar_is_final(bar(updated_at="2026-10-27T23:00:00+08:00"), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_write_time_in_future"), reason)

    def test_close_out_of_range(self):
        ok, reason = daily_bar_is_final(bar(close=100), CLOSE)
        self.assertFalse(ok)
        self.assertIn("close_out_of_range", reason)

    def test_missing_write_time(self):
        item = bar()
        item.pop("updated_at")
        self.assertEqual(daily_bar_is_final(item, CLOSE)[1], "bar_write_time_missing")

    def test_other_day(self):
        ok, reason = daily_bar_is_final(bar(trade_date="2026-10-26"), CLOSE)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_trade_date_mismatch"))

    def test_gate_before_close(self):
        self.assertEqual(daily_fallback_allowed(NOW)[1], "daily_fallback_before_close")
        self.assertEqual(daily_fallback_allowed(CLOSE)[0], True)


class TestPrepareWrite(unittest.TestCase):
    def test_complete_payload(self):
        doc, unset, reason = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "symbol": "600000"},
            "600000", "2026-10-27", NOW, 1800)
        self.assertEqual(reason, "ok")
        self.assertEqual((doc["close"], doc["high"], doc["low"]), (10.0, 11.0, 9.0))
        self.assertEqual(doc["quote_time_source"], "fetch")
        self.assertEqual(unset, ["quote_time"])
        self.assertEqual(doc["trade_date"], "2026-10-27")

    def test_incomplete_rejected(self):
        doc, unset, reason = prepare_quote_write(
            {"close": None, "high": 11, "low": 9}, "600000", "2026-10-27", NOW)
        self.assertIsNone(doc)
        self.assertEqual(unset, [])
        self.assertTrue(reason.startswith("incomplete_snapshot:close_"))

    def test_close_out_of_range_rejected(self):
        doc, _, reason = prepare_quote_write(
            {"close": 100, "high": 11, "low": 9}, "600000", "2026-10-27", NOW)
        self.assertIsNone(doc)
        self.assertIn("close_out_of_range", reason)

    def test_payload_fetched_at_preserved(self):
        """评审 P1-1：写库不得把采集时刻重新起算。"""
        captured = "2026-10-27T14:00:00+08:00"
        later = datetime(2026, 10, 27, 15, 10, tzinfo=CN_TZ)
        doc, _, _ = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "fetched_at": captured},
            "600000", "2026-10-27", later, 1800)
        self.assertTrue(doc["fetched_at"].startswith("2026-10-27T14:00:00"))
        self.assertTrue(doc["updated_at"].startswith("2026-10-27T15:10:00"))

    def test_captured_at_used_when_absent(self):
        captured = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)
        later = datetime(2026, 10, 27, 15, 10, tzinfo=CN_TZ)
        doc, _, _ = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9}, "600000", "2026-10-27", later, 1800,
            captured_at=captured)
        self.assertTrue(doc["fetched_at"].startswith("2026-10-27T14:00:00"))

    def test_invalid_fetched_at_rejected(self):
        doc, _, reason = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "fetched_at": "bad"},
            "600000", "2026-10-27", NOW)
        self.assertIsNone(doc)
        self.assertEqual(reason, "fetched_at_invalid")

    def test_invalid_quote_time_rejected_not_degraded(self):
        doc, _, reason = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "quote_time": "13:70"},
            "600000", "2026-10-27", NOW)
        self.assertIsNone(doc)
        self.assertEqual(reason, "quote_time_invalid")

    def test_upstream_channel_no_unset(self):
        doc, unset, _ = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "quote_time": "2026-10-27T13:55:00+08:00"},
            "600000", "2026-10-27", NOW)
        self.assertEqual(unset, [])
        self.assertEqual(doc["quote_time_source"], "upstream")

    def test_volume_share_converted_and_unit_synced(self):
        doc, _, _ = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "volume": 123400, "volume_unit": "share"},
            "600000", "2026-10-27", NOW)
        self.assertEqual(doc["volume"], 1234.0)
        self.assertEqual(doc["volume_unit"], "lot")

    def test_volume_lot_unchanged(self):
        doc, _, _ = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "volume": 1234, "volume_unit": "lot"},
            "600000", "2026-10-27", NOW)
        self.assertEqual(doc["volume"], 1234)

    def test_interval_infinity_rejected(self):
        doc, _, reason = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9}, "600000", "2026-10-27", NOW,
            float("inf"))
        self.assertIsNone(doc)
        self.assertEqual(reason, "snapshot_interval_invalid")

    def test_interval_bool_rejected(self):
        doc, _, reason = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9}, "600000", "2026-10-27", NOW, True)
        self.assertIsNone(doc)
        self.assertEqual(reason, "snapshot_interval_invalid")

    def test_none_protected_fields_dropped(self):
        doc, _, _ = prepare_quote_write(
            {"close": 10, "high": 11, "low": 9, "open": None, "amount": None},
            "600000", "2026-10-27", NOW)
        self.assertNotIn("open", doc)
        self.assertNotIn("amount", doc)


class TestResolveCnPrice(unittest.TestCase):
    def test_same_source_when_quote_invalid_price(self):
        """评审反例：market_quotes 时间有效但 close=0，日线有效 → 必须用日线且依据也是日线。"""
        candidates = {
            "market_quotes": q(close=0),
            "stock_daily_quotes": bar(),
        }
        price, basis, reason = resolve_cn_price(candidates, CLOSE, strict=True)
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["source"], "stock_daily_quotes")
        self.assertEqual(reason, "ok")

    def test_quote_preferred_when_usable(self):
        price, basis, _ = resolve_cn_price({"market_quotes": q(), "stock_daily_quotes": bar()},
                                           NOW, strict=True)
        self.assertEqual(price, 10.0)
        self.assertEqual(basis["source"], "market_quotes")

    def test_no_source_at_all(self):
        price, basis, reason = resolve_cn_price({"market_quotes": None}, NOW, strict=True)
        self.assertIsNone(price)
        self.assertIsNone(basis)
        self.assertIn("daily_fallback_before_close", reason)

    def test_stale_quote_after_close_without_daily(self):
        stale = q(fetched_at=(CLOSE - timedelta(minutes=90)).isoformat())
        price, basis, reason = resolve_cn_price({"market_quotes": stale}, CLOSE, strict=True)
        self.assertIsNone(price)
        self.assertIn("snapshot_too_old", reason)

    def test_display_uses_stale_quote(self):
        old = q(trade_date="2026-10-20", fetched_at="2026-10-20T10:00:00+08:00")
        price, basis, reason = resolve_cn_price({"market_quotes": old}, NOW, strict=False)
        self.assertEqual(price, 10.0)
        self.assertEqual(reason, "display")
        self.assertEqual(basis["channel"], "display")

    def test_display_falls_back_to_basic_info(self):
        price, basis, _ = resolve_cn_price(
            {"market_quotes": None, "stock_basic_info": {"current_price": 8.8}}, NOW, strict=False)
        self.assertEqual(price, 8.8)
        self.assertEqual(basis["source"], "stock_basic_info")

    def test_display_intraday_uses_last_etf_bar(self):
        """评审 P2-6：盘中展示不能因为日线未完成就返回 None。"""
        old_etf = {"close": 3.14, "trade_date": "2026-10-24",
                   "updated_at": "2026-10-24T15:30:00+08:00"}
        price, basis, _ = resolve_cn_price({"etf_daily_quotes": old_etf}, NOW, strict=False)
        self.assertEqual(price, 3.14)
        self.assertIn("不可用于成交", basis["notice"])


class TestResolveCnRange(unittest.TestCase):
    def test_from_quote(self):
        rng, basis, reason = resolve_cn_range({"market_quotes": q()}, NOW)
        self.assertEqual(reason, "ok")
        self.assertEqual(rng, {"high": 11.0, "low": 9.0})
        self.assertEqual(basis["source"], "market_quotes")

    def test_falls_back_to_daily(self):
        stale = q(fetched_at=(CLOSE - timedelta(minutes=90)).isoformat())
        rng, basis, _ = resolve_cn_range({"market_quotes": stale, "stock_daily_quotes": bar()}, CLOSE)
        self.assertEqual(rng, {"high": 11.0, "low": 9.0})
        self.assertEqual(basis["source"], "stock_daily_quotes")

    def test_before_close_rejects(self):
        stale = q(fetched_at=(NOW - timedelta(minutes=90)).isoformat())
        rng, _, reason = resolve_cn_range({"market_quotes": stale}, NOW)
        self.assertIsNone(rng)
        self.assertIn("daily_fallback_before_close", reason)


class TestSyncGate(unittest.TestCase):
    def test_non_trade_day(self):
        self.assertEqual(sync_round_skip_reason(NOW, False, "tushare"), "not_trade_day")

    def test_calendar_unverified(self):
        self.assertEqual(sync_round_skip_reason(NOW, True, "fallback"), "calendar_unverified")

    def test_pre_open(self):
        self.assertEqual(
            sync_round_skip_reason(datetime(2026, 10, 27, 9, 0, tzinfo=CN_TZ), True, "akshare"),
            "pre_open")

    def test_after_close(self):
        self.assertEqual(
            sync_round_skip_reason(datetime(2026, 10, 27, 15, 45, tzinfo=CN_TZ), True, "akshare"),
            "after_close")

    def test_ok(self):
        self.assertIsNone(sync_round_skip_reason(NOW, True, "akshare"))

    def test_failure_stats_shape(self):
        stats = build_failure_stats("pool_empty", "ctx")
        self.assertFalse(stats["success"])
        self.assertEqual(stats["error_count"], 1)


class TestWriteGate(unittest.TestCase):
    def test_default_is_disabled(self):
        from app.services.quote_contract import quotes_write_enabled

        self.assertFalse(quotes_write_enabled({}))

    def test_enabled_values(self):
        from app.services.quote_contract import quotes_write_enabled

        for raw in ("1", "true", "on", "YES"):
            self.assertTrue(quotes_write_enabled({"QUOTES_WRITE_ENABLED": raw}), raw)

    def test_prepare_refused_when_disabled(self):
        saved = os.environ.pop("QUOTES_WRITE_ENABLED", None)
        try:
            doc, unset, reason = prepare_quote_write(
                {"close": 10, "high": 11, "low": 9}, "600000", "2026-10-27", NOW)
            self.assertIsNone(doc)
            self.assertEqual(unset, [])
            self.assertEqual(reason, "quotes_write_disabled")
        finally:
            if saved is not None:
                os.environ["QUOTES_WRITE_ENABLED"] = saved


class TestTradingStatus(unittest.TestCase):
    def test_missing_rejected(self):
        item = q()
        item.pop("trading_status")
        self.assertEqual(quote_is_usable(item, NOW), (False, "trading_status_missing"))

    def test_suspended_rejected(self):
        self.assertEqual(quote_is_usable(q(trading_status="停牌"), NOW),
                         (False, "security_suspended"))

    def test_unknown_value_rejected(self):
        ok, reason = quote_is_usable(q(trading_status="weird"), NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("trading_status_unknown"), reason)

    def test_wrong_type_rejected(self):
        self.assertEqual(quote_is_usable(q(trading_status=1), NOW),
                         (False, "trading_status_invalid"))

    def test_trading_passes(self):
        self.assertTrue(quote_is_usable(q(trading_status="交易中"), NOW)[0])


class TestBarFinality(unittest.TestCase):
    def test_unproven_rejected(self):
        item = bar()
        item.pop("is_final")
        self.assertEqual(daily_bar_is_final(item, CLOSE), (False, "bar_finality_unproven"))

    def test_final_state_string(self):
        item = bar()
        item.pop("is_final")
        item["final_state"] = "closed"
        self.assertTrue(daily_bar_is_final(item, CLOSE)[0])

    def test_suspended_bar_rejected(self):
        self.assertEqual(daily_bar_is_final(bar(trading_status="停牌"), CLOSE),
                         (False, "security_suspended"))


class TestRangeConsistency(unittest.TestCase):
    def test_strict_price_also_checks_range(self):
        """评审 P2-4：市价取价必须与区间校验同口径。"""
        price, basis, reason = quote_price_and_basis(q(close=12, high=11, low=9), NOW)
        self.assertIsNone(price)
        self.assertIsNone(basis)
        self.assertIn("close_out_of_range", reason)


if __name__ == "__main__":
    unittest.main(verbosity=2)
