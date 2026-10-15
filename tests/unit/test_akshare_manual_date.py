"""验证手动增量同步的北京时间边界与调度入口参数传递。"""
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from app.worker.akshare_sync_service import AKShareSyncService, run_akshare_historical_sync

MODULE = 'app.worker.akshare_sync_service'


class ManualDateTests(unittest.IsolatedAsyncioTestCase):
    async def execute(self, time, **kwargs):
        service = AKShareSyncService()
        service._process_historical_batch = AsyncMock(return_value={
            'success_count': 1, 'error_count': 0, 'total_records': 1,
            'errors': [], 'processed_count': 1,
        })
        with patch(MODULE+'.datetime') as clock, patch(MODULE+'.bind_loop_safe_mongo_db', return_value=MagicMock()):
            clock.now.return_value = datetime.fromisoformat(time)
            clock.utcnow.return_value = datetime(2026,10,15)
            result = await service.sync_historical_data(
                symbols=['000001'], start_date='2026-10-01', **kwargs
            )
            clock.now.assert_called_once_with(timezone(timedelta(hours=8)))
        return result, service._process_historical_batch

    async def test_manual_before_18_uses_yesterday_including_month_boundary(self):
        for time, expected in [('2026-10-15T00:25:00','2026-10-14'),
                               ('2026-10-15T17:59:59','2026-10-14'),
                               ('2026-11-01T00:00:00','2026-10-31')]:
            result, batch = await self.execute(time, manual_trigger=True)
            self.assertFalse(result.get('skipped',False))
            self.assertEqual(batch.await_args.args[2], expected)

    async def test_manual_at_and_after_18_uses_today(self):
        for time in ['2026-10-15T18:00:00','2026-10-15T23:59:59']:
            _, batch = await self.execute(time, manual_trigger=True)
            self.assertEqual(batch.await_args.args[2],'2026-10-15')

    async def test_scheduled_before_18_still_skips(self):
        result, batch = await self.execute('2026-10-15T10:00:00')
        self.assertTrue(result['skipped'])
        batch.assert_not_awaited()

    async def test_explicit_end_date_is_preserved(self):
        _, batch = await self.execute('2026-10-15T10:00:00',manual_trigger=True,end_date='2026-10-12')
        self.assertEqual(batch.await_args.args[2],'2026-10-12')
        result, batch = await self.execute('2026-10-15T10:00:00',manual_trigger=True,end_date='2026-10-15')
        self.assertTrue(result['skipped'])
        batch.assert_not_awaited()

    async def test_full_sync_date_is_unchanged(self):
        _, batch = await self.execute('2026-10-15T10:00:00',manual_trigger=True,incremental=False)
        self.assertEqual(batch.await_args.args[2],'2026-10-15')

    async def test_direct_manual_api_uses_yesterday(self):
        _, batch = await self.execute('2026-10-15T10:00:00',manual_mode=True)
        self.assertEqual(batch.await_args.args[2],'2026-10-14')

    async def test_scheduler_wrapper_passes_manual_trigger(self):
        pool = SimpleNamespace(execute_sync_method=AsyncMock(return_value=SimpleNamespace(success=True,result={})))
        with patch('app.worker.unified_thread_pool_sync_service.get_unified_thread_pool_sync_service',new=AsyncMock(return_value=pool)), \
             patch(MODULE+'.get_akshare_sync_service',new=AsyncMock(return_value=AKShareSyncService())):
            await run_akshare_historical_sync(_manual_trigger=True)
        self.assertTrue(pool.execute_sync_method.await_args.kwargs['method_kwargs']['manual_trigger'])


if __name__ == '__main__':
    unittest.main()
