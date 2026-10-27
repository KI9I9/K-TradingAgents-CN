#!/usr/bin/env python3
"""行情时间/字段契约 + 调度失败判定 的行为测试（纯标准库，可离线跑）。

针对第三轮评审逐条补的反例：
- P1-1 不完整快照不得刷新旧价格的时间（用「$set 合并」语义模拟验证）
- P1-2 行情时间缺失 → 严格路径拒绝，不得用采集时间冒充
- P1-3 盘中写入的当日日线 → 不得用于成交
- P2-4 inf / nan / bool 不得当作价格
- P1-5 单只路径「股」单位 → 统一换算为「手」

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


# scheduler_runtime 顶层 import bson；测试环境没有 pymongo，打最小 stub。
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
    """模拟 pymongo 默认解码出来的 naive UTC。"""
    return datetime(y, mo, d, h, mi, s)


def apply_set_idempotent(old: dict, doc: dict) -> dict:
    """模拟 MongoDB {"$set": doc} 的合并语义。"""
    merged = dict(old)
    for key, value in doc.items():
        if key != "_id":
            merged[key] = value
    return merged


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

    def test_naive_string_uses_exchange_tz(self):
        self.assertEqual(
            qc.parse_quote_time("2026-10-27 09:58:00").utcoffset(), timedelta(hours=8)
        )

    def test_invalid_values_return_none(self):
        for value in ("", "   ", "bad-value", None, 12345, 1.5, True, [], {}):
            with self.subTest(value=value):
                self.assertIsNone(qc.parse_quote_time(value))


class TestCoerceTradeDate(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(qc.coerce_trade_date("2026-10-27"), TODAY)
        self.assertEqual(qc.coerce_trade_date("20261027"), TODAY)
        self.assertEqual(qc.coerce_trade_date("2026/10/27"), TODAY)
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
        self.assertIsNone(qc.validate_price(float("inf"))[0])
        self.assertIsNone(qc.validate_price(float("-inf"))[0])
        self.assertIsNone(qc.validate_price(float("nan"))[0])

    def test_bool_rejected(self):
        """True/False 是 int 的子类，绝不能当价格。"""
        self.assertEqual(qc.validate_price(True)[1], "bool_not_price")
        self.assertIsNone(qc.validate_price(False)[0])

    def test_non_positive_and_none(self):
        self.assertEqual(qc.validate_price(0)[1], "non_positive")
        self.assertEqual(qc.validate_price(-1)[1], "non_positive")
        self.assertEqual(qc.validate_price(None)[1], "missing")


class TestQuoteIsUsable(unittest.TestCase):
    def test_quote_time_present_accepted(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": TODAY, "quote_time": "2026-10-27T09:58:00+08:00"}, NOW
        )
        self.assertTrue(ok, reason)

    def test_quote_time_from_bson_date(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": TODAY, "quote_time": naive_utc(2026, 10, 27, 1, 55)}, NOW
        )
        self.assertTrue(ok, reason)

    def test_missing_quote_time_rejected_even_if_fetched_now(self):
        """评审 P1-2：刚采集过 ≠ 报价新鲜。"""
        ok, reason = qc.quote_is_usable(
            {
                "trade_date": TODAY,
                "fetched_at": "2026-10-27T09:59:59+08:00",
                "updated_at": "2026-10-27T09:59:59+08:00",
            },
            NOW,
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "quote_time_missing")

    def test_fetch_time_allowed_only_when_policy_flipped(self):
        original = qc.ALLOW_FETCH_TIME_AS_QUOTE_TIME
        try:
            qc.ALLOW_FETCH_TIME_AS_QUOTE_TIME = True
            ok, reason = qc.quote_is_usable(
                {"trade_date": TODAY, "fetched_at": "2026-10-27T09:59:00+08:00"}, NOW
            )
            self.assertTrue(ok, reason)
        finally:
            qc.ALLOW_FETCH_TIME_AS_QUOTE_TIME = original

    def test_stale_quote_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": TODAY, "quote_time": "2026-10-27T09:00:00+08:00"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_stale"), reason)

    def test_future_quote_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": TODAY, "quote_time": "2026-10-27T10:30:00+08:00"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_from_future"), reason)

    def test_trade_date_mismatch_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-26", "quote_time": "2026-10-27T09:58:00+08:00"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("trade_date_mismatch"), reason)


class TestPrepareQuoteWrite(unittest.TestCase):
    """评审 P1-1 的核心反例：不完整快照不得刷新旧价格的时间。"""

    OLD_DOC = {
        "_id": "x",
        "code": "600000",
        "symbol": "600000",
        "close": 8.0,
        "high": 9.0,
        "low": 7.0,
        "trade_date": "2026-10-26",
        "updated_at": "2026-10-26T15:05:00+08:00",
        "quote_time": "2026-10-26T14:59:00+08:00",
    }

    def test_partial_snapshot_is_rejected_and_old_doc_untouched(self):
        payload = {"price": 12.0, "close": None, "high": None, "low": None}
        doc, reason = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertIsNone(doc)
        self.assertTrue(reason.startswith("incomplete_snapshot"), reason)

        # 没有 doc 就不会发生 $set，旧记录原样保留
        merged = self.OLD_DOC if doc is None else apply_set_idempotent(self.OLD_DOC, doc)
        self.assertEqual(merged["trade_date"], "2026-10-26")
        self.assertEqual(merged["updated_at"], "2026-10-26T15:05:00+08:00")
        self.assertEqual(merged["high"], 9.0)
        ok, why = qc.quote_is_usable(merged, NOW)
        self.assertFalse(ok, why)

    def test_missing_one_field_rejected(self):
        payload = {"close": 8.0, "high": 9.0}  # 缺 low
        doc, reason = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertIsNone(doc)
        self.assertIn("low_missing", reason)

    def test_complete_snapshot_written_but_not_tradable_without_quote_time(self):
        payload = {"close": 8.0, "high": 9.0, "low": 7.0, "volume": 1000}
        doc, reason = qc.prepare_quote_write(payload, "600000", TODAY, NOW)
        self.assertEqual(reason, "ok")
        self.assertEqual(doc["trade_date"], TODAY)
        self.assertEqual(doc["quote_time_source"], "fetch")
        self.assertNotIn("quote_time", doc)
        ok, why = qc.quote_is_usable(doc, NOW)
        self.assertFalse(ok)
        self.assertEqual(why, "quote_time_missing")

    def test_alias_filled(self):
        payload = {"price": 11.3, "close": 11.3, "open_price": 11.1,
                   "high_price": 11.4, "low_price": 11.0}
        doc, reason = qc.prepare_quote_write(payload, "000001", TODAY, NOW)
        self.assertEqual(reason, "ok")
        self.assertEqual(doc["high"], 11.4)
        self.assertEqual(doc["low"], 11.0)

    def test_volume_normalised_from_shares(self):
        """评审 P1-5：单只路径曾写「股」，这里按 volume_unit 归一为「手」。"""
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
    """评审 P1-3：盘中写入的当日日线不得用于成交。"""

    def _bar(self, written_at):
        return {
            "trade_date": TODAY,
            "open": 9.5, "high": 11.0, "low": 9.0, "close": 10.0,
            "updated_at": written_at,
        }

    def test_intraday_bar_rejected_at_afternoon(self):
        bar = self._bar("2026-10-27T09:35:00+08:00")
        ok, reason = qc.daily_bar_is_final(bar, at(14, 0))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_written_intraday"), reason)

    def test_after_close_bar_accepted(self):
        bar = self._bar("2026-10-27T18:05:00+08:00")
        ok, reason = qc.daily_bar_is_final(bar, at(19, 0))
        self.assertTrue(ok, reason)

    def test_yesterday_bar_rejected(self):
        bar = self._bar("2026-10-26T18:05:00+08:00")
        bar["trade_date"] = "2026-10-26"
        ok, reason = qc.daily_bar_is_final(bar, at(19, 0))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("bar_trade_date_mismatch"), reason)

    def test_bar_without_write_time_rejected(self):
        bar = self._bar(None)
        ok, reason = qc.daily_bar_is_final(bar, at(19, 0))
        self.assertFalse(ok)
        self.assertEqual(reason, "bar_write_time_missing")

    def test_fallback_gate_before_and_after_close(self):
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
        stats = qc.build_failure_stats("标的池为空", "sync_realtime_quotes_no_symbols")
        status, message = sched.outcome(stats)
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
