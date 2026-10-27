#!/usr/bin/env python3
"""行情时间/字段契约 + 调度失败判定 的行为测试（纯标准库，可离线跑）。

设计原则（针对上一轮评审的意见）：
- 测**行为**，不测「源码里是否包含某个字符串」。
- 直接加载 `app/services/quote_contract.py` 与 `app/services/scheduler_runtime.py`
  的真实实现；后者只需给 `bson` 打一个最小 stub，不改被测代码。
- 时间三类（BSON naive UTC / ISO 字符串 / 缺失或非法）逐一覆盖，
  其中「缺失或非法」必须**被拒绝**，这是上一版护栏的漏洞。
- 用真实 provider 返回值（`return_all=False` 的键集合）跑完整链路：
  上游字典 → 写入规范化 → 消费侧判定，而不是手写理想 payload。

运行：python3 -B tests/offline/test_quote_contract.py
"""

import importlib.util
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
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
NOW = datetime(2026, 10, 27, 10, 0, tzinfo=CN_TZ)


def naive_utc(y, mo, d, h, mi, s=0):
    """模拟 pymongo 默认解码出来的 naive UTC。"""
    return datetime(y, mo, d, h, mi, s)


class TestParseQuoteTime(unittest.TestCase):
    def test_bson_naive_datetime_treated_as_utc(self):
        parsed = qc.parse_quote_time(naive_utc(2026, 10, 27, 1, 55))
        self.assertEqual(parsed.utcoffset(), timedelta(0))
        # 01:55 UTC == 09:55 +08:00
        self.assertEqual(parsed.astimezone(CN_TZ).strftime("%H:%M"), "09:55")

    def test_iso_with_offset(self):
        parsed = qc.parse_quote_time("2026-10-27T09:58:00+08:00")
        self.assertEqual(parsed.utcoffset(), timedelta(hours=8))

    def test_iso_z(self):
        parsed = qc.parse_quote_time("2026-10-27T01:58:00Z")
        self.assertEqual(parsed.utcoffset(), timedelta(0))

    def test_naive_string_uses_exchange_tz(self):
        parsed = qc.parse_quote_time("2026-10-27 09:58:00")
        self.assertEqual(parsed.utcoffset(), timedelta(hours=8))

    def test_aware_datetime_passthrough(self):
        value = datetime(2026, 10, 27, 9, 58, tzinfo=CN_TZ)
        self.assertEqual(qc.parse_quote_time(value), value)

    def test_invalid_values_return_none(self):
        for value in ("", "   ", "bad-value", None, 12345, 1.5, True, [], {}):
            with self.subTest(value=value):
                self.assertIsNone(qc.parse_quote_time(value))


class TestCoerceTradeDate(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(qc.coerce_trade_date("2026-10-27"), "2026-10-27")
        self.assertEqual(qc.coerce_trade_date("20261027"), "2026-10-27")
        self.assertEqual(qc.coerce_trade_date("2026/10/27"), "2026-10-27")
        self.assertEqual(qc.coerce_trade_date(date(2026, 10, 27)), "2026-10-27")
        self.assertEqual(
            qc.coerce_trade_date(datetime(2026, 10, 27, 9, 0, tzinfo=CN_TZ)),
            "2026-10-27",
        )

    def test_unrecognised(self):
        for value in (None, "", "2026-1-7", "abc", 20261027):
            with self.subTest(value=value):
                self.assertIsNone(qc.coerce_trade_date(value))


class TestQuoteIsUsable(unittest.TestCase):
    def test_fresh_bson_datetime_accepted(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-27", "updated_at": naive_utc(2026, 10, 27, 1, 55)}, NOW
        )
        self.assertTrue(ok, reason)

    def test_fresh_iso_string_accepted(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-27", "updated_at": "2026-10-27T09:58:00+08:00"}, NOW
        )
        self.assertTrue(ok, reason)

    def test_trade_date_plain_format_accepted(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "20261027", "updated_at": "2026-10-27T09:58:00+08:00"}, NOW
        )
        self.assertTrue(ok, reason)

    def test_stale_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-27", "updated_at": "2026-10-27T09:00:00+08:00"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_stale"), reason)

    def test_future_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-27", "updated_at": "2026-10-27T10:30:00+08:00"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_from_future"), reason)

    def test_small_clock_skew_tolerated(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-27", "updated_at": "2026-10-27T10:00:30+08:00"}, NOW
        )
        self.assertTrue(ok, reason)

    def test_missing_time_rejected(self):
        """上一版护栏的漏洞：updated_at 缺失会 return True。"""
        ok, reason = qc.quote_is_usable({"trade_date": "2026-10-27"}, NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_time_unparseable"), reason)

    def test_bad_time_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-27", "updated_at": "bad-value"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_time_unparseable"), reason)

    def test_numeric_time_rejected(self):
        ok, reason = qc.quote_is_usable({"trade_date": "2026-10-27", "updated_at": 1730000000}, NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_time_unparseable"), reason)

    def test_trade_date_mismatch_rejected(self):
        ok, reason = qc.quote_is_usable(
            {"trade_date": "2026-10-26", "updated_at": "2026-10-27T09:58:00+08:00"}, NOW
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("trade_date_mismatch"), reason)

    def test_missing_trade_date_rejected_when_required(self):
        ok, reason = qc.quote_is_usable({"updated_at": "2026-10-27T09:58:00+08:00"}, NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("trade_date_mismatch"), reason)

    def test_trade_date_optional(self):
        ok, reason = qc.quote_is_usable(
            {"updated_at": "2026-10-27T09:58:00+08:00"}, NOW, require_trade_date=False
        )
        self.assertTrue(ok, reason)


class TestValidatePriceRange(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(qc.validate_price_range({"high": 10, "low": 9})[:2], (10.0, 9.0))

    def test_string_numbers(self):
        self.assertEqual(qc.validate_price_range({"high": "10", "low": "9"})[:2], (10.0, 9.0))

    def test_missing(self):
        self.assertEqual(qc.validate_price_range({})[2], "quote_missing")
        self.assertEqual(qc.validate_price_range({"high": 10})[2], "not_numeric")

    def test_low_greater_than_high(self):
        self.assertEqual(qc.validate_price_range({"high": 9, "low": 10})[2], "low_gt_high")

    def test_non_positive(self):
        self.assertEqual(qc.validate_price_range({"high": 0, "low": 0})[2], "non_positive")

    def test_not_finite(self):
        self.assertEqual(
            qc.validate_price_range({"high": float("nan"), "low": 1})[2], "not_finite"
        )


class TestSyncRoundGate(unittest.TestCase):
    def at(self, hour, minute):
        return datetime(2026, 10, 27, hour, minute, tzinfo=CN_TZ)

    def test_pre_open_skipped(self):
        self.assertEqual(qc.sync_round_skip_reason(self.at(9, 0), True, "akshare"), "pre_open")

    def test_session_allowed(self):
        self.assertIsNone(qc.sync_round_skip_reason(self.at(9, 30), True, "akshare"))
        self.assertIsNone(qc.sync_round_skip_reason(self.at(14, 59), True, "tushare"))

    def test_after_close_skipped(self):
        self.assertEqual(qc.sync_round_skip_reason(self.at(15, 31), True, "akshare"), "after_close")

    def test_not_trade_day(self):
        self.assertEqual(
            qc.sync_round_skip_reason(self.at(10, 0), False, "akshare"), "not_trade_day"
        )

    def test_fallback_calendar_is_unverified(self):
        """日历 API 全失败会退回普通工作日，节假日会被误判成交易日。"""
        self.assertEqual(
            qc.sync_round_skip_reason(self.at(10, 0), True, "fallback"), "calendar_unverified"
        )

    def test_missing_source_is_unverified(self):
        self.assertEqual(
            qc.sync_round_skip_reason(self.at(10, 0), True, None), "calendar_unverified"
        )


class TestBuildQuoteDoc(unittest.TestCase):
    """用真实 provider 返回的键集合跑链路。"""

    # 未修复的批量分支（return_all=False）实际返回的键
    LEGACY = {
        "symbol": "000001", "name": "平安银行", "price": 11.3, "change": 0.1,
        "change_percent": 0.89, "volume": 1323113, "amount": 1.48e9, "pre_close": 11.2,
        "open_price": 11.15, "high_price": 11.34, "low_price": 11.13,
        "data_source": "akshare", "sync_status": "success",
    }

    # 修复后的批量分支（本分支 defef53）会多出这些键
    FIXED = dict(
        LEGACY,
        open=11.15, high=11.34, low=11.13, close=11.3, current_price=11.3, pct_chg=0.89,
        updated_at="2026-10-27T09:58:00+08:00",
    )

    def test_symbol_code_and_trade_date_injected(self):
        doc = qc.build_quote_doc({"price": 1.0}, "600000", "2026-10-27")
        self.assertEqual(doc["symbol"], "600000")
        self.assertEqual(doc["code"], "600000")
        self.assertEqual(doc["trade_date"], "2026-10-27")

    def test_existing_trade_date_not_overwritten(self):
        doc = qc.build_quote_doc({"trade_date": "2026-10-24"}, "600000", "2026-10-27")
        self.assertEqual(doc["trade_date"], "2026-10-24")

    def test_alias_mapped_to_standard(self):
        doc = qc.build_quote_doc(dict(self.LEGACY), "000001", "2026-10-27")
        self.assertEqual(doc["high"], 11.34)
        self.assertEqual(doc["low"], 11.13)
        self.assertEqual(doc["open"], 11.15)

    def test_none_values_dropped(self):
        doc = qc.build_quote_doc({"close": None, "high": 10, "amount": None}, "600000")
        self.assertNotIn("close", doc)
        self.assertNotIn("amount", doc)
        self.assertEqual(doc["high"], 10)

    def test_chain_legacy_payload_still_rejected(self):
        """上游未修复时：写入文档有区间，但没有行情时间 → 消费侧必须拒绝。"""
        doc = qc.build_quote_doc(dict(self.LEGACY), "000001", "2026-10-27")
        self.assertIsNotNone(qc.validate_price_range(doc)[0])
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("quote_time_unparseable"), reason)

    def test_chain_fixed_payload_accepted(self):
        doc = qc.build_quote_doc(dict(self.FIXED), "000001", "2026-10-27")
        ok, reason = qc.quote_is_usable(doc, NOW)
        self.assertTrue(ok, reason)
        self.assertEqual(qc.validate_price_range(doc)[:2], (11.34, 11.13))


class TestSchedulerOutcome(unittest.TestCase):
    """直接调用真实 outcome()，确认空池结果会被判 failed 而不是 success。"""

    def test_empty_pool_is_failed(self):
        stats = qc.build_failure_stats("标的池为空", "sync_realtime_quotes_no_symbols")
        status, message = sched.outcome(stats)
        self.assertEqual(status, "failed")
        self.assertIn("标的池为空", message)

    def test_legacy_stats_with_only_errors_would_be_success(self):
        """回归护栏：只往 errors 里塞、不给 success=False，就会被记成 success。"""
        status, _ = sched.outcome({"error_count": 0, "errors": [{"error": "标的池为空"}]})
        self.assertEqual(status, "success")

    def test_skipped_maps_to_skipped(self):
        status, _ = sched.outcome({"skipped": True, "reason": "pre_open"})
        self.assertEqual(status, "skipped")

    def test_normal_result_is_success(self):
        status, _ = sched.outcome({"success_count": 10, "error_count": 0, "errors": []})
        self.assertEqual(status, "success")


if __name__ == "__main__":
    unittest.main(verbosity=2)
