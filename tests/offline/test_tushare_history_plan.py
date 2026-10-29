"""Planning invariants; importing this module cannot initialize a provider."""
import copy
from datetime import datetime, timezone, timedelta
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('history_plan', ROOT/'deploy/history_sync_plan.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


def observation(latest='2026-10-27', count=5, verified=True):
    return dict(data_source='tushare', market='CN', period='daily', count=count,
                latest=latest, contract_verified=verified, content_sha256='a'*64)


class HistoryPlanTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 29, 0, 55, tzinfo=timezone(timedelta(hours=8)))
        self.series = {'600000.SH': observation()}

    def plan(self, **kwargs):
        values = dict(symbols=['600000'], series=self.series, end='2026-10-28', now=self.now)
        values.update(kwargs)
        return p.build_plan(**values)

    def test_cursor_overlap_preserved(self):
        result = self.plan()
        self.assertEqual(result['items'][0]['start'], '2026-10-27')
        self.assertEqual(result['total_requests'], 2)
        self.assertFalse(result['execution_authorized'])
        self.assertFalse(result['full_market_ready'])

    def test_pool_order_and_duplicates_do_not_change_plan(self):
        series = {**self.series, '000001.SZ': observation()}
        self.assertEqual(self.plan(symbols=['600000', '000001', '600000.SH'], series=series),
                         self.plan(symbols=['000001.SZ', '600000.SH'], series=dict(reversed(list(series.items())))))

    def test_legacy_is_not_silently_skipped(self):
        self.series['600000.SH']['contract_verified'] = False
        result = self.plan()
        self.assertIsNone(result['total_requests'])
        self.assertEqual(result['blocked_count'], 1)
        self.assertEqual(result['items'][0]['reason'], 'legacy_contract_unproven')

    def test_empty_series_keeps_year_bootstrap_and_blocks(self):
        self.series['600000.SH'] = observation(None, 0)
        item = self.plan()['items'][0]
        self.assertEqual(item['start'], '2025-10-29')
        self.assertEqual(item['reason'], 'window_exceeds_current_transport')

    def test_explicit_short_bootstrap(self):
        self.series['600000.SH'] = observation(None, 0)
        self.assertEqual(self.plan(bootstrap_start='2026-10-21')['candidate_count'], 1)

    def test_exact_thirty_day_boundary(self):
        self.series['600000.SH']['latest'] = '2026-09-29'
        self.assertEqual(self.plan()['candidate_count'], 1)
        self.series['600000.SH']['latest'] = '2026-09-28'
        self.assertEqual(self.plan()['blocked_count'], 1)

    def test_akshare_cursor_cannot_substitute(self):
        self.series['600000.SH']['data_source'] = 'akshare'
        with self.assertRaises(p.PlanError): self.plan()

    def test_missing_inventory_is_not_empty_series(self):
        with self.assertRaises(p.PlanError): self.plan(series={})

    def test_extra_inventory_refused(self):
        with self.assertRaises(p.PlanError):
            self.plan(series={**self.series, '000001.SZ': observation()})

    def test_pool_rejects_fund_index_suffix_and_empty(self):
        for codes in ([], ['159865'], ['600000.SZ'], ['000001.SH'], [True]):
            with self.subTest(codes=codes), self.assertRaises(p.PlanError): self.plan(symbols=codes)

    def test_today_cutoff_in_china_timezone(self):
        with self.assertRaises(p.PlanError): self.plan(end='2026-10-29')
        result = self.plan(end='2026-10-29', now=datetime(2026,10,29,10,tzinfo=timezone.utc))
        self.assertEqual(result['candidate_count'], 1)

    def test_future_and_naive_clock_refused(self):
        with self.assertRaises(p.PlanError): self.plan(end='2026-10-30')
        with self.assertRaises(p.PlanError): self.plan(now=self.now.replace(tzinfo=None))

    def test_cursor_ahead_blocks_not_rewrites(self):
        self.series['600000.SH']['latest'] = '2026-10-29'
        self.assertEqual(self.plan()['items'][0]['reason'], 'cursor_after_requested_end')

    def test_snapshot_drift_changes_identity(self):
        before = self.plan()
        self.series['600000.SH']['content_sha256'] = 'b'*64
        self.assertNotEqual(before['plan_sha256'], self.plan()['plan_sha256'])

    def test_inputs_not_mutated(self):
        before = copy.deepcopy(self.series)
        self.plan()
        self.assertEqual(before, self.series)

    def test_unknown_quota_does_not_invent_eta(self):
        self.assertIsNone(self.plan()['quota'])
        self.assertIn('quota_unverified', self.plan()['unresolved'])

    def test_odd_budget_accounts_for_two_requests_per_stock(self):
        quota = dict(daily_requests=5, rolling_minute_requests=4, evidence_ref='synthetic-only')
        series = {code: observation() for code in ['600000.SH', '600001.SH', '600002.SH']}
        result = self.plan(symbols=list(series), series=series, quota=quota)
        self.assertEqual(result['quota']['candidate_stocks_per_day'], 2)
        self.assertEqual(result['quota']['candidate_budget_days_lower_bound'], 2)
        self.assertFalse(result['execution_authorized'])

    def test_invalid_quota_evidence(self):
        for value in (True, 0, 1, 1.5):
            with self.subTest(value=value), self.assertRaises(p.PlanError):
                self.plan(quota=dict(daily_requests=value, rolling_minute_requests=4, evidence_ref='synthetic'))

    def test_invalid_inventory_metadata(self):
        for field, value in [('count', True), ('count', -1), ('latest', '2026-02-30'),
                             ('contract_verified', 'true'), ('content_sha256', ''), ('period', 'weekly')]:
            with self.subTest(field=field), self.assertRaises(p.PlanError):
                self.plan(series={'600000.SH': {**observation(), field:value}})

    def test_empty_with_cursor_refused(self):
        with self.assertRaises(p.PlanError):
            self.plan(series={'600000.SH': observation(count=0)})


if __name__ == '__main__':
    unittest.main()
