"""使用模拟接口验证增量日期、空结果和短时回退，不请求真实行情。"""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, MagicMock, patch
import pandas as pd
from requests.exceptions import ConnectionError
from app.worker.akshare_sync_service import AKShareSyncService
from tradingagents.dataflows.providers.china.akshare import AKShareProvider, AKShareNetworkError

MODULE='tradingagents.dataflows.providers.china.akshare'


class IncrementalTests(unittest.IsolatedAsyncioTestCase):
    async def test_up_to_date_stock_skips_request_but_updates_progress(self):
        service=AKShareSyncService()
        service._should_stop=AsyncMock(return_value=False)
        service._get_last_sync_date=AsyncMock(return_value='2026-10-15')
        service.provider=SimpleNamespace(get_historical_data=AsyncMock())
        with patch('app.services.scheduler_service.update_job_progress',new=AsyncMock()) as progress:
            stats=await service._process_historical_batch(['000001'],None,'2026-10-14',incremental=True,job_id='test',original_total_symbols=1)
        service.provider.get_historical_data.assert_not_awaited()
        self.assertEqual(stats['skipped_count'],1)
        self.assertEqual(stats['error_count'],0)
        self.assertEqual(stats['processed_count'],1)
        self.assertIn('无需更新 1',progress.await_args.kwargs['message'])

    async def test_equal_dates_are_requested_and_empty_is_not_network_failure(self):
        service=AKShareSyncService()
        service.provider=SimpleNamespace(get_historical_data=AsyncMock(return_value=pd.DataFrame()))
        stats=await service._process_historical_batch(['000001'],'2026-10-14','2026-10-14')
        service.provider.get_historical_data.assert_awaited_once()
        self.assertEqual(stats['no_data_count'],1)
        self.assertEqual(stats['error_count'],0)

    async def test_real_network_error_is_still_an_error(self):
        service=AKShareSyncService()
        service.provider=SimpleNamespace(get_historical_data=AsyncMock(side_effect=AKShareNetworkError('网络连接失败')))
        stats=await service._process_historical_batch(['000001'],'2026-10-13','2026-10-14')
        self.assertEqual(stats['error_count'],1)
        self.assertEqual(stats['no_data_count'],0)


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with patch.object(AKShareProvider,'_initialize_akshare'):
            self.provider=AKShareProvider()
        self.provider.connected=True
        self.provider.ak=SimpleNamespace(stock_zh_a_hist=Mock(side_effect=ConnectionError('Connection aborted')))
        self.provider._get_historical_data_sina_fallback=AsyncMock(return_value=pd.DataFrame())
        self.provider._get_historical_data_tx_fallback=AsyncMock(return_value=pd.DataFrame())
        self.provider._standardize_historical_columns=Mock(side_effect=lambda frame,code:frame)

    async def get(self, **kwargs):
        with patch(MODULE+'.asyncio.sleep',new=AsyncMock()):
            return await self.provider.get_historical_data('000001',kwargs.pop('start','2026-10-13'),'2026-10-14',**kwargs)

    async def test_reverse_range_never_calls_any_source(self):
        self.assertTrue((await self.get(start='2026-10-15')).empty)
        self.provider.ak.stock_zh_a_hist.assert_not_called()
        self.provider._get_historical_data_sina_fallback.assert_not_awaited()

    async def test_clean_empty_fallback_is_not_reported_as_network_failure(self):
        self.assertTrue((await self.get()).empty)
        self.provider._get_historical_data_tx_fallback.assert_awaited_once()

    async def test_fallback_request_failure_is_not_treated_as_empty(self):
        self.provider._get_historical_data_sina_fallback.side_effect=ConnectionError('断连')
        with self.assertRaises(AKShareNetworkError):
            await self.get()

    async def test_fallback_data_wins_over_first_source_failure(self):
        self.provider._get_historical_data_sina_fallback.side_effect=ConnectionError('断连')
        self.provider._get_historical_data_tx_fallback.return_value=pd.DataFrame({'date':['2026-10-14'],'close':[10]})
        self.assertEqual(len(await self.get()),1)

    async def test_circuit_skips_primary_then_retries_after_cooldown(self):
        with patch(MODULE+'.time.monotonic',return_value=100):
            await self.get()
            await self.get()
            self.assertEqual(self.provider.ak.stock_zh_a_hist.call_count,6)
            await self.get()
            self.assertEqual(self.provider.ak.stock_zh_a_hist.call_count,6)
        self.provider.ak.stock_zh_a_hist.side_effect=None
        self.provider.ak.stock_zh_a_hist.return_value=pd.DataFrame({'date':['2026-10-14']})
        with patch(MODULE+'.time.monotonic',return_value=401):
            await self.get()
        self.assertEqual(self.provider.ak.stock_zh_a_hist.call_count,7)
        self.assertEqual(self.provider._history_failure_count,0)
        self.assertEqual(self.provider._history_retry_after,0)

    async def test_programming_error_is_not_hidden_as_no_data(self):
        self.provider.ak.stock_zh_a_hist.side_effect=ValueError('字段解析失败')
        with self.assertRaises(ValueError):
            await self.get()
        self.provider._get_historical_data_sina_fallback.assert_not_awaited()

    async def test_weekly_failure_does_not_fall_back_to_daily_prices(self):
        with self.assertRaises(AKShareNetworkError):
            await self.get(period='weekly')
        self.provider._get_historical_data_sina_fallback.assert_not_awaited()


class CompletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_completion_distinguishes_covered_empty_and_failed(self):
        from app.services.scheduler_service import mark_job_completed
        for covered,empty,failed,expected in [(2,0,0,'success'),(0,2,0,'skipped'),(1,0,1,'partial'),(0,0,2,'failed')]:
            db=MagicMock()
            db.scheduler_executions.find_one.return_value={'_id':'test'}
            with patch('app.services.scheduler_service.get_mongo_db_sync',return_value=db), \
                 patch('app.core.database.get_redis_sync_client',return_value=None):
                await mark_job_completed('akshare_historical_sync',{
                    'total_processed':2,'success_count':0,'skipped_count':covered,
                    'no_data_count':empty,'error_count':failed,'errors':[],
                })
            update=db.scheduler_executions.update_one.call_args.args[1]['$set']
            self.assertEqual(update['status'],expected)
            self.assertIn(f'无需更新 {covered}',update['progress_message'])
            self.assertEqual(update['error_count'],failed)


if __name__=='__main__':
    unittest.main()
