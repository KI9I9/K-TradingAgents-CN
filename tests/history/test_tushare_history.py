"""Real pandas/service/provider/task bodies; only SDK, DB and app bootstrap are replaced.

Run with the isolated dependencies in deploy/requirements-history-test.txt.
No credentials, network requests or production database are used.
"""
import ast
import asyncio
import copy
from datetime import date, datetime, timedelta, timezone
import importlib.util
import logging
from pathlib import Path
import re
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
from pandas.testing import assert_frame_equal
from pymongo.results import BulkWriteResult

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.services.historical_contract import HistoricalContractError, PERIODS, trade_date
from tradingagents.utils.tushare_egress import TushareEgressStopped

NOW = datetime(2026, 10, 28, 19, tzinfo=timezone(timedelta(hours=8)))
DB_MODULE = 'app.core.database'
bootstrap = types.ModuleType(DB_MODULE)
bootstrap.get_database = Mock()
with patch.dict(sys.modules, {DB_MODULE: bootstrap}):
    spec = importlib.util.spec_from_file_location('real_history', ROOT/'app/services/historical_data_service.py')
    history = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(history)


def frame(days=('20261026',), symbol='600000', period='daily'):
    data = pd.DataFrame([dict(trade_date=day, ts_code=symbol + '.SH', open=10., high=12.,
                              low=9., close=11., pre_close=10., volume=123.45,
                              amount=678.9, pct_chg=10., change=1.) for day in days])
    data.attrs['historical_contract'] = dict(
        source='tushare', symbol=symbol, period=period, volume_unit='lot', amount_unit='thousand_CNY',
        start_date='20200101', end_date='20261028', endpoint='pro_bar', price_adjustment='qfq')
    return data


class Cursor:
    def __init__(self, rows): self.rows = rows
    async def to_list(self, length): return copy.deepcopy(self.rows[:length])
    def __aiter__(self):
        async def items():
            for row in self.rows: yield copy.deepcopy(row)
        return items()


class Collection:
    def __init__(self):
        self.rows, self.calls = [], []
        self.failure = None
        self.acknowledged = True
        self.confirm = True
    def find(self, query):
        return Cursor([r for r in self.rows if all(r.get(k) == v for k, v in query.items())])
    async def find_one(self, query, projection=None, sort=None):
        rows = self.find(query).rows
        if sort:
            key, direction = sort[0]
            rows = sorted(rows, key=lambda row: row[key], reverse=direction < 0)
        return copy.deepcopy(rows[0]) if rows else None
    async def create_index(self, *a, **k): pass
    async def index_information(self):
        return {'unique': {'key': [('symbol', 1), ('trade_date', 1), ('data_source', 1), ('period', 1)], 'unique': True}}
    async def bulk_write(self, operations, ordered):
        self.calls.append(operations)
        if self.failure: raise self.failure
        matched = inserted = 0
        for op in operations:
            old = next((r for r in self.rows if all(r.get(k) == v for k, v in op._filter.items())), None)
            replacement = copy.deepcopy(op._doc)
            if old is not None:
                matched += 1
                if '$setOnInsert' not in replacement:
                    old.clear()
                    old.update(replacement)
            else:
                inserted += 1
                self.rows.append(replacement.get('$setOnInsert', replacement))
        return BulkWriteResult({'nMatched': matched if self.confirm else 0,
                                'nUpserted': inserted if self.confirm else 0, 'nModified': 0},
                               self.acknowledged)


def extract(path, names, namespace):
    tree = ast.parse((ROOT/path).read_text())
    nodes = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert len(nodes) == len(names)
    module = ast.Module(body=nodes, type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
    return namespace


def provider(raw):
    sdk = Mock(pro_bar=Mock(return_value=raw))
    async def sdk_call(function, **kwargs): return function(**kwargs)
    namespace = dict(asyncio=types.SimpleNamespace(to_thread=sdk_call), pd=pd, datetime=datetime, date=date, timezone=timezone,
                     timedelta=timedelta, Optional=object, Union=object)
    from typing import Optional, Union
    namespace.update(Optional=Optional, Union=Union, TUSHARE_AVAILABLE=True, ts=sdk,
                     TushareEgressStopped=TushareEgressStopped, raise_if_egress_blocked=Mock(),
                     _manual_current=types.SimpleNamespace(get=lambda: None), _manual_frame=Mock())
    extract(Path('tradingagents/dataflows/providers/china/tushare.py'),
            {'get_historical_data', '_standardize_historical_data', '_format_date'}, namespace)
    obj = types.SimpleNamespace(is_available=lambda: True, api=Mock(), logger=logging.getLogger('test'),
                                _normalize_ts_code=lambda s: s + '.SH', _is_etf=lambda s: False)
    for name in ('get_historical_data', '_standardize_historical_data', '_format_date'):
        setattr(obj, name, types.MethodType(namespace[name], obj))
    return obj, sdk, namespace


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.collection = Collection()
        self.db = types.SimpleNamespace(stock_daily_quotes=self.collection, stock_basic_info=Collection())
        module = types.ModuleType(DB_MODULE)
        module.get_database = module.get_mongo_db = lambda: self.db
        patcher = patch.dict(sys.modules, {DB_MODULE: module})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.service = history.HistoricalDataService()
        self.service.db = self.db

    async def save(self, data, **kw):
        return await self.service.save_historical_data('600000', data, 'tushare', **kw)

    async def test_units_and_input_immutability(self):
        data = frame()
        original = data.copy(deep=True)
        self.assertEqual(await self.save(data), 1)
        row = self.collection.rows[0]
        self.assertEqual((row['volume'], row['amount']), (123.45, 678900.))
        self.assertEqual((row['volume_unit'], row['amount_unit']), ('lot', 'CNY'))
        self.assertNotIn('is_final', row)
        self.assertNotIn('trading_status', row)
        assert_frame_equal(data, original)

    async def test_akshare_existing_units_not_changed(self):
        data = frame()
        original = data.copy(deep=True)
        self.assertEqual(await self.service.save_historical_data('600000',data,'akshare'),1)
        self.assertEqual((self.collection.rows[0]['volume'],self.collection.rows[0]['amount']),(123.45,678.9))
        assert_frame_equal(data,original)

    async def test_repeated_same_frame_does_not_scale_or_write_again(self):
        data = frame()
        await self.save(data)
        before = copy.deepcopy(self.collection.rows)
        self.assertEqual(await self.save(data), 1)
        self.assertEqual(self.collection.rows, before)
        self.assertEqual(len(self.collection.calls), 1)

    async def test_zero_volume_and_amount_preserved(self):
        data = frame()
        data.loc[0, ['volume', 'amount']] = 0
        await self.save(data)
        self.assertEqual(self.collection.rows[0]['volume'], 0)
        self.assertEqual(self.collection.rows[0]['amount'], 0)

    async def test_missing_amount_stays_missing(self):
        data = frame()
        data['amount'] = None
        await self.save(data)
        self.assertIsNone(self.collection.rows[0]['amount'])

    async def test_native_vol_alias(self):
        await self.save(frame().rename(columns={'volume': 'vol'}))
        self.assertEqual(self.collection.rows[0]['volume'], 123.45)

    async def test_conflicting_alias_rejected(self):
        data = frame()
        data['vol'] = 999
        with self.assertRaisesRegex(HistoricalContractError, 'conflicting_volume'): await self.save(data)
        self.assertEqual(self.collection.calls, [])

    async def test_full_batch_validated_before_first_write(self):
        data = frame(('20261025', '20261026'))
        data.loc[1, 'close'] = 99
        with self.assertRaisesRegex(HistoricalContractError, 'ohlc_invalid'): await self.save(data)
        self.assertEqual(self.collection.calls, [])

    async def test_dates_do_not_fall_back_to_today(self):
        for invalid in [None, '20260230', 'not-a-date', 17, pd.NaT]:
            with self.subTest(invalid=invalid):
                data = frame()
                data['trade_date'] = invalid
                with self.assertRaises(HistoricalContractError): await self.save(data)
        self.assertEqual(self.collection.calls, [])

    async def test_duplicate_date_rejected(self):
        with self.assertRaises(HistoricalContractError): await self.save(frame(('20261026', '20261026')))
        self.assertEqual(self.collection.calls, [])

    async def test_symbol_or_requested_range_mismatch_rejected(self):
        for field, value in [('ts_code', '000001.SZ'), ('trade_date', '19990101')]:
            data = frame()
            data[field] = value
            with self.assertRaises(HistoricalContractError): await self.save(data)
        self.assertEqual(self.collection.calls, [])

    async def test_nan_infinite_negative_required_values_rejected(self):
        for field, value in [('volume', float('nan')), ('volume', -1), ('close', float('inf')),
                             ('amount', float('inf')), ('open', 0)]:
            data = frame()
            data.loc[0, field] = value
            with self.assertRaises(HistoricalContractError): await self.save(data)
        self.assertEqual(self.collection.calls, [])

    async def test_missing_provenance_and_already_converted_units_rejected(self):
        for metadata in [{}, {**frame().attrs['historical_contract'], 'amount_unit': 'CNY'}]:
            data = frame()
            data.attrs['historical_contract'] = metadata
            with self.assertRaisesRegex(HistoricalContractError, 'provenance_unproven'): await self.save(data)
        self.assertEqual(self.collection.calls, [])

    async def test_legacy_series_is_not_silently_migrated(self):
        self.collection.rows = [dict(symbol='600000', data_source='tushare', period='daily',
                                    market='CN', trade_date='2026-10-25', volume=999)]
        before = copy.deepcopy(self.collection.rows)
        with self.assertRaisesRegex(HistoricalContractError, 'existing_contract_unproven'):
            await self.save(frame())
        self.assertEqual(self.collection.rows, before)

    async def test_qfq_append_requires_matching_overlap(self):
        await self.save(frame(('20261025',)))
        with self.assertRaisesRegex(HistoricalContractError, 'overlap_missing'): await self.save(frame())
        self.assertEqual(await self.save(frame(('20261025', '20261026'))), 2)
        self.assertEqual(len(self.collection.rows), 2)

    async def test_qfq_revision_refuses_without_partial_new_day(self):
        await self.save(frame(('20261025',)))
        data = frame(('20261025', '20261026'))
        data.loc[0, 'close'] = 10.5
        before = copy.deepcopy(self.collection.rows)
        with self.assertRaisesRegex(HistoricalContractError, 'adjustment_or_price_revision'): await self.save(data)
        self.assertEqual(self.collection.rows, before)

    async def test_other_source_and_period_untouched(self):
        unrelated = [dict(symbol='600000', data_source='akshare', period='daily', trade_date='2026-10-26'),
                     dict(symbol='600000', data_source='tushare', period='monthly', trade_date='2026-10-27')]
        self.collection.rows = copy.deepcopy(unrelated)
        await self.save(frame())
        self.assertEqual(self.collection.rows[:2], unrelated)

    async def test_unacknowledged_or_short_write_raises(self):
        self.collection.acknowledged = False
        with self.assertRaisesRegex(RuntimeError, 'write_not_confirmed'): await self.save(frame())
        self.collection.rows.clear()
        self.collection.acknowledged = True
        self.collection.confirm = False
        with self.assertRaisesRegex(RuntimeError, 'write_not_confirmed'): await self.save(frame())

    async def test_database_exception_not_swallowed(self):
        self.collection.failure = RuntimeError('database unavailable')
        with self.assertRaisesRegex(RuntimeError, 'database unavailable'): await self.save(frame())

    async def test_missing_unique_index_refuses_before_write(self):
        with patch.object(self.collection, 'index_information', return_value={}):
            with self.assertRaisesRegex(HistoricalContractError, 'unique_index_missing'): await self.save(frame())
        self.assertEqual(self.collection.calls, [])

    async def test_matched_but_unmodified_result_counts_as_confirmed(self):
        await self.save(frame())
        data = frame()
        data['amount'] = 700
        self.assertEqual(await self.save(data), 1)
        self.assertEqual(self.collection.rows[0]['amount'], 700000)

    async def test_overwrite_false_retains_existing_record(self):
        await self.save(frame())
        before = copy.deepcopy(self.collection.rows)
        data = frame()
        data['amount'] = 700
        self.assertEqual(await self.save(data, overwrite=False), 1)
        self.assertEqual(self.collection.rows, before)

    async def test_latest_date_scoped_by_period_and_errors_propagate(self):
        self.collection.rows = [dict(symbol='600000', data_source='tushare', period=p, trade_date=d)
                                for p, d in [('daily', '2026-10-25'), ('monthly', '2026-10-27')]]
        self.assertEqual(await self.service.get_latest_date('600000', 'tushare', period='daily', strict=True), '2026-10-25')
        with patch.object(self.collection, 'find_one', side_effect=RuntimeError('unavailable')):
            with self.assertRaises(RuntimeError):
                await self.service.get_latest_date('600000', 'tushare', period='daily', strict=True)

    async def test_real_provider_to_real_writer(self):
        raw = frame().rename(columns={'volume': 'vol'})
        raw.attrs.clear()
        obj, sdk, _ = provider(raw)
        result = await obj.get_historical_data('600000', '2026-10-25', '2026-10-26')
        self.assertEqual(await self.save(result), 1)
        self.assertEqual(self.collection.rows[0]['volume'], 123.45)
        self.assertEqual(sdk.pro_bar.call_args.kwargs['adj'], 'qfq')
        self.assertIn('api', sdk.pro_bar.call_args.kwargs)

    async def test_provider_period_and_stop_prevent_sdk_calls(self):
        obj, sdk, namespace = provider(frame())
        with self.assertRaises(ValueError): await obj.get_historical_data('600000', '2026-10-25', period='bad')
        namespace['raise_if_egress_blocked'].side_effect = TushareEgressStopped('disabled')
        with self.assertRaises(TushareEgressStopped): await obj.get_historical_data('600000', '2026-10-25')
        sdk.pro_bar.assert_not_called()

    async def test_etf_weekly_not_mislabeled_as_weekly(self):
        obj, sdk, _ = provider(frame())
        obj._is_etf = lambda _: True
        self.assertIsNone(await obj.get_historical_data('510300', '2026-10-25', period='weekly'))
        obj.api.fund_daily.assert_not_called()
        sdk.pro_bar.assert_not_called()


class TaskTests(unittest.IsolatedAsyncioTestCase):
    setUp = HistoryTests.setUp

    def worker(self, now=NOW):
        from typing import Any, Dict, List, Optional, Tuple
        namespace = dict(asyncio=asyncio, datetime=datetime, timedelta=timedelta, timezone=timezone,
                         re=re, cn_now=lambda: now, PERIODS=PERIODS, trade_date=trade_date,
                         Any=Any, Dict=Dict, List=List, Optional=Optional, Tuple=Tuple,
                         logger=logging.getLogger('task-test'), TushareEgressStopped=TushareEgressStopped,
                         raise_if_egress_blocked=Mock(), bind_loop_safe_mongo_db=lambda self:self.db,
                         _is_etf_code=lambda symbol:symbol.startswith(('15', '51')))
        names = {'sync_historical_data', '_get_last_sync_date', '_save_historical_data', 'run_tushare_historical_sync'}
        extract(Path('app/worker/tushare_sync_service.py'), names, namespace)
        obj = types.SimpleNamespace(db=self.db, historical_service=self.service,
                                    rate_limiter=types.SimpleNamespace(acquire=AsyncMock(),
                                        get_stats=lambda:dict(current_calls=1,max_calls=200,total_waits=0,total_wait_time=0)))
        raw = frame().rename(columns={'volume':'vol'})
        raw.attrs.clear()
        obj.provider, sdk, _ = provider(raw)
        for name in names - {'run_tushare_historical_sync'}:
            setattr(obj,name,types.MethodType(namespace[name],obj))
        return obj, sdk, namespace

    async def sync(self, obj, **kwargs):
        return await obj.sync_historical_data(symbols=['600000'], start_date='2026-10-25',
                                              end_date='2026-10-26', auto_retry=False, **kwargs)

    async def test_real_worker_provider_and_writer_chain(self):
        obj, sdk, _ = self.worker()
        stats = await self.sync(obj)
        self.assertIs(stats['success'], True)
        self.assertEqual((stats['success_count'],stats['total_records']), (1,1))
        self.assertEqual(self.collection.rows[0]['amount'],678900.)
        self.assertEqual(sdk.pro_bar.call_count,1)

    async def test_all_database_writes_fail_reports_failure(self):
        obj, _, _ = self.worker()
        self.collection.failure = RuntimeError('write refused')
        stats = await self.sync(obj)
        self.assertIs(stats['success'], False)
        self.assertEqual((stats['success_count'], stats['error_count']), (0,1))

    async def test_legacy_series_refused_before_sdk_request(self):
        obj, sdk, _ = self.worker()
        self.collection.rows = [dict(symbol='600000',data_source='tushare',period='daily',
                                    market='CN',trade_date='2025-10-10')]
        stats = await self.sync(obj)
        self.assertIs(stats['success'],False)
        sdk.pro_bar.assert_not_called()
        self.assertEqual(self.collection.calls,[])

    async def test_80_percent_success_still_reports_incomplete(self):
        obj, sdk, _ = self.worker()
        obj._should_stop = AsyncMock(return_value=False)
        self.db.scheduler_executions = Collection()
        def response(**kw):
            raw = frame().rename(columns={'volume':'vol'})
            raw['ts_code'] = kw['ts_code']
            if kw['ts_code']=='600004.SH': raw['close']=99
            return raw
        sdk.pro_bar.side_effect=response
        scheduler=types.ModuleType('app.services.scheduler_service')
        scheduler.TaskCancelledException=type('TaskCancelledException',(Exception,),{})
        scheduler.mark_job_completed=AsyncMock()
        scheduler.update_job_progress=AsyncMock()
        with patch.dict(sys.modules,{scheduler.__name__:scheduler}):
            stats=await obj.sync_historical_data(symbols=[f'60000{i}' for i in range(5)],
                start_date='2026-10-25',end_date='2026-10-26',auto_retry=False,job_id='test-only')
        self.assertEqual((stats['success_count'],stats['error_count']),(4,1))
        self.assertIs(stats['success'],False)
        self.assertIsNotNone(scheduler.mark_job_completed.call_args.args[2])

    async def test_unconfirmed_record_count_reports_failure(self):
        obj, _, _ = self.worker()
        obj._save_historical_data = AsyncMock(return_value=0)
        stats = await self.sync(obj)
        self.assertIs(stats['success'],False)
        self.assertEqual(stats['total_records'],0)

    async def test_empty_pool_is_failure_not_etf_success(self):
        obj, sdk, _ = self.worker()
        stats = await obj.sync_historical_data(symbols=[], auto_retry=False)
        self.assertIs(stats['success'],False)
        self.assertEqual(stats['reason'],'empty_symbol_pool')
        sdk.pro_bar.assert_not_called()

    async def test_china_1800_boundary_independent_of_host_timezone(self):
        for hour, should_skip in [(17,True),(18,False)]:
            obj, sdk, _ = self.worker(NOW.replace(hour=hour))
            stats = await obj.sync_historical_data(symbols=['600000'],start_date='2026-10-25',auto_retry=False)
            self.assertEqual(bool(stats.get('skipped')),should_skip)
            self.assertEqual(sdk.pro_bar.call_count,0 if should_skip else 1)

    async def test_explicit_previous_day_runs_before_1800(self):
        obj, sdk, _ = self.worker(NOW.replace(hour=9))
        self.assertTrue((await self.sync(obj))['success'])
        self.assertEqual(sdk.pro_bar.call_count,1)

    async def test_cursor_overlaps_last_bar_of_same_period(self):
        obj, _, _ = self.worker()
        self.collection.rows = [dict(symbol='600000', data_source='tushare',period=p,trade_date=d)
                                for p,d in [('daily','2026-10-25'),('monthly','2026-10-27')]]
        self.assertEqual(await obj._get_last_sync_date('600000',period='daily'),'2026-10-25')
        self.assertEqual(await obj._get_last_sync_date('600000',period='monthly'),'2026-10-27')

    async def test_cursor_db_failure_does_not_trigger_full_refetch(self):
        obj, _, _ = self.worker()
        with patch.object(self.collection,'find_one',side_effect=RuntimeError('db unavailable')):
            with self.assertRaises(RuntimeError): await obj._get_last_sync_date('600000')

    async def test_no_cursor_limits_default_window_to_one_year(self):
        obj, _, _ = self.worker()
        self.assertEqual(await obj._get_last_sync_date('600000'),'2025-10-28')

    async def test_duplicate_symbols_processed_once(self):
        obj, sdk, _ = self.worker()
        stats = await obj.sync_historical_data(symbols=['600000','600000'],start_date='2026-10-25',
                                               end_date='2026-10-26',auto_retry=False)
        self.assertTrue(stats['success'])
        self.assertEqual(sdk.pro_bar.call_count,1)

    async def test_invalid_range_or_period_no_provider_call(self):
        obj, sdk, _ = self.worker()
        for kwargs in [dict(start_date='2026-10-27',end_date='2026-10-26'),dict(period='bogus')]:
            stats = await obj.sync_historical_data(symbols=['600000'],auto_retry=False,**kwargs)
            self.assertIs(stats['success'],False)
        sdk.pro_bar.assert_not_called()

    async def test_stop_propagates_even_with_force_or_manual_flags(self):
        obj, sdk, namespace = self.worker()
        namespace['raise_if_egress_blocked'].side_effect = TushareEgressStopped('disabled')
        with self.assertRaises(TushareEgressStopped): await self.sync(obj)
        with self.assertRaises(TushareEgressStopped):
            await namespace['run_tushare_historical_sync'](_force_execute=True,_manual_trigger=True)
        sdk.pro_bar.assert_not_called()
        self.assertEqual(self.collection.calls,[])

    async def test_scheduler_wrapper_does_not_mask_false_business_result(self):
        obj, _, namespace = self.worker()
        namespace['_check_task_running'] = AsyncMock(return_value=(False,None))
        namespace['create_tushare_sync_service'] = AsyncMock(return_value=obj)
        unified = types.SimpleNamespace(execute_sync_method=AsyncMock(return_value=
                         types.SimpleNamespace(success=True,result={'success':False,'error_count':1})))
        module = types.ModuleType('app.worker.unified_thread_pool_sync_service')
        module.get_unified_thread_pool_sync_service = AsyncMock(return_value=unified)
        scheduler = types.ModuleType('app.services.scheduler_service')
        scheduler.TaskCancelledException = type('TaskCancelledException',(Exception,),{})
        with patch.dict(sys.modules,{module.__name__:module,scheduler.__name__:scheduler}):
            with self.assertRaisesRegex(RuntimeError,'sync_incomplete'):
                await namespace['run_tushare_historical_sync']()


if __name__ == '__main__':
    logging.disable(logging.CRITICAL)
    unittest.main(verbosity=2)
