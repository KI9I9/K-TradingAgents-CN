"""Real provider normalization and sync method; fake vendors/DB only."""
import ast
import asyncio
from datetime import datetime, timedelta
from functools import partial
import logging
import math
from pathlib import Path
import re
import sys
import types
from typing import Any, Dict, List, Optional
import unittest
from unittest.mock import patch, Mock

ROOT = Path(__file__).resolve().parents[2]
tree = ast.parse((ROOT / 'app/worker/etf_sync_service.py').read_text())
names = {'_is_missing', '_safe_float', '_normalize_date_text', '_normalize_etf_code',
         '_get_full_symbol', '_is_etf_code', '_normalize_etf_history_frame', '_fetch_etf_hist_akshare',
         '_parse_etf_sina_payload'}
nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
nodes += [n for c in tree.body if isinstance(c, ast.ClassDef) for n in c.body
          if isinstance(n, ast.AsyncFunctionDef) and n.name == '_sync_daily_by_source']
ns = dict(globals(), ETF_SH_PREFIXES=('50', '51', '52', '56', '58'),
          ETF_SZ_PREFIXES=('15', '16', '18'), ETF_BJ_PREFIXES=('89',),
          ETF_DAILY_QUOTES_COLLECTION='etf_daily_quotes', logger=logging.getLogger('fixture'))
exec(compile(ast.Module(body=nodes, type_ignores=[]), '<actual-etf-methods>', 'exec'), ns)
route = ast.parse((ROOT / 'app/routers/stock_sync.py').read_text())
exec(compile(ast.Module(body=[n for n in route.body if isinstance(n, ast.FunctionDef)
    and n.name in ('_get_single_task_status', '_get_single_task_message')], type_ignores=[]),
    '<actual-task-result>', 'exec'), ns)


class Frame:
    def __init__(self, rows): self.rows = rows; self.empty = not rows
    def iterrows(self): return enumerate(self.rows)


class History(unittest.TestCase):
    def test_real_sina_assignment_with_trailing_comment_is_data_only(self):
        parse = ns['_parse_etf_sina_payload']
        self.assertEqual(parse(b'var KLC_K2_sz159865="abc/=="; /* vendor comment */\n'), 'abc/==')
        self.assertEqual(parse(b'var KLC_K2_sz159865="abc/==";'), 'abc/==')
        for raw in [b'var x="abc"; alert(1)', b'var x=fetch("x")', b'<html>error</html>']:
            with self.assertRaises(ValueError): parse(raw)

    def row(self, **extra):
        return dict(date='2026-11-02', open=0.51, high=0.523, low=0.508,
                    close=0.52, volume=273851160, amount=141653288, **extra)

    def normalize(self, frame, endpoint='fund_etf_hist_sina', divisor=100):
        return ns['_normalize_etf_history_frame'](frame, '159865', '20251030', '20261102', endpoint, divisor)

    def test_sina_dates_and_share_to_lot_conversion(self):
        old = self.row(); old['date'] = '2025-01-01'
        future = self.row(); future['date'] = '2026-11-03'
        rows = self.normalize(Frame([old, self.row(), future]))
        self.assertEqual(len(rows), 1); row = rows[0]
        self.assertEqual(row['volume'], 2738511.6)
        self.assertEqual((row['amount'], row['price_adjustment'], row['source_endpoint']),
                         (141653288, 'none', 'fund_etf_hist_sina'))
        self.assertEqual((row['volume_unit'], row['amount_unit']), ('lot', 'CNY'))
        self.assertNotIn('is_final', row); self.assertNotIn('trading_status', row)

    def test_em_volume_remains_lots(self):
        frame = Frame([{'日期': '2026-11-02', '开盘': .51, '最高': .523, '最低': .508,
                        '收盘': .52, '成交量': 2738511.6, '成交额': 141653288}])
        self.assertEqual(self.normalize(frame, 'fund_etf_hist_em', 1)[0]['volume'], 2738511.6)

    def test_invalid_ohlc_nonfinite_or_duplicate_refused(self):
        for key, value in [('close', 12), ('open', float('nan')), ('volume', -1)]:
            row = self.row(); row[key] = value
            with self.assertRaises(ValueError): self.normalize(Frame([row]))
        with self.assertRaises(ValueError): self.normalize(Frame([self.row(), self.row()]))

    def test_em_failure_or_empty_uses_sina_but_never_tushare(self):
        fake = types.ModuleType('akshare')
        for outcome in [ConnectionError('unreachable'), Frame([])]:
            fake.fund_etf_hist_em = Mock(side_effect=outcome) if isinstance(outcome, Exception) else Mock(return_value=outcome)
            fallback = Mock(return_value=Frame([self.row()]))
            with patch.dict(sys.modules, {'akshare': fake}), patch.dict(ns, {'_fetch_etf_hist_sina_frame': fallback}):
                result = ns['_fetch_etf_hist_akshare']('159865', '20251030', '20261102')
            self.assertEqual(result[0]['source_endpoint'], 'fund_etf_hist_sina')
            fallback.assert_called_once_with('159865')
            self.assertEqual(fake.fund_etf_hist_em.call_args.kwargs['adjust'], '')

    def test_both_empty_refused(self):
        fake = types.ModuleType('akshare'); fake.fund_etf_hist_em = Mock(return_value=Frame([]))
        with patch.dict(sys.modules, {'akshare': fake}), patch.dict(ns, {'_fetch_etf_hist_sina_frame': lambda _: Frame([])}):
            with self.assertRaisesRegex(ValueError, '未返回'): ns['_fetch_etf_hist_akshare']('159865', '20251030', '20261102')

    def test_true_sync_method_reports_empty_as_failure_without_writing(self):
        async def noop(*a, **kw): pass
        async def window(*a, **kw): return '20251030', '20261102', 'fixture'
        col = types.SimpleNamespace(bulk_write=Mock(side_effect=AssertionError('no DB writes')))
        service = types.SimpleNamespace(db={'etf_daily_quotes': col}, _report_job_progress=noop,
            _calculate_stage_progress=lambda *a: 100, _resolve_daily_sync_window=window,
            item_timeout_seconds=1, rate_limit_delay=0)
        scheduler = types.ModuleType('app.services.scheduler_service')
        scheduler.TaskCancelledException = type('TaskCancelledException', (RuntimeError,), {})
        with patch.dict(sys.modules, {'app.services.scheduler_service': scheduler}), patch.dict(ns, {'_FETCH_HIST': {'akshare': lambda *a: []}}):
            result = asyncio.run(ns['_sync_daily_by_source'](service, 'akshare', ['159865'], 365, True))
        self.assertEqual((result['success_count'], result['error_count'], result['records_inserted']), (0, 1, 0))
        self.assertIn('未返回', result['errors'][0]['error']); col.bulk_write.assert_not_called()

    def test_task_with_no_success_is_failed_not_partial(self):
        self.assertEqual(ns['_get_single_task_status']({'overall_success': False,
            'historical_sync': {'success': False}}), 'failed')
        self.assertEqual(ns['_get_single_task_status']({'overall_success': False,
            'historical_sync': {'success': True}, 'basic_sync': {'success': False}}), 'success_with_errors')
        self.assertEqual(ns['_get_single_task_status']({'overall_success': True}), 'success')


if __name__ == '__main__': unittest.main()
