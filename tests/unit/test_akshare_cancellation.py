"""AKShare 取消回归测试：模拟行情与数据库，不访问外部接口。"""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from app.worker.akshare_sync_service import AKShareSyncService
from app.services.scheduler_service import TaskCancelledException
from app.services.scheduler_runtime import outcome


class CancellationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = AKShareSyncService()
        self.service._current_job_id = 'akshare_historical_sync'
        self.service.provider = SimpleNamespace(get_historical_data=AsyncMock(return_value=None))

    async def check_records(self, records):
        client = MagicMock()
        collection = client.__getitem__.return_value.scheduler_executions
        def find(query, **kwargs):
            for doc in records:
                if 'status' not in query or doc['status'] in query['status']['$in']:
                    return doc
            return None
        collection.find_one.side_effect = find
        with patch('pymongo.MongoClient', return_value=client):
            result = await self.service._should_stop('akshare_historical_sync')
        client.close.assert_called_once()
        return result

    async def test_newer_skipped_record_cannot_hide_active_cancellation(self):
        self.assertTrue(await self.check_records([
            {'status': 'skipped', 'cancel_requested': False},
            {'status': 'running', 'cancel_requested': True},
        ]))

    async def test_old_cancelled_record_does_not_stop_new_running_task(self):
        self.assertFalse(await self.check_records([
            {'status': 'running', 'cancel_requested': False},
            {'status': 'cancelled', 'cancel_requested': True},
        ]))

    async def batch(self, progress_error):
        self.service._should_stop = AsyncMock(return_value=False)
        with patch('app.services.scheduler_service.update_job_progress', new=AsyncMock(side_effect=progress_error)):
            return await self.service._process_historical_batch(
                ['000001', '000002'], '2026-10-01', '2026-10-13',
                job_id='akshare_historical_sync', original_total_symbols=2,
            )

    async def test_progress_cancellation_prevents_next_stock_request(self):
        with self.assertRaises(TaskCancelledException):
            await self.batch(TaskCancelledException('用户取消'))
        self.assertEqual(self.service.provider.get_historical_data.await_count, 1)

    async def test_progress_storage_failure_still_allows_next_stock(self):
        await self.batch(RuntimeError('进度缓存暂时不可用'))
        self.assertEqual(self.service.provider.get_historical_data.await_count, 2)

    async def test_stop_inside_last_batch_finishes_as_cancelled(self):
        self.service._should_stop = AsyncMock(side_effect=[False, True])
        db, redis = MagicMock(), MagicMock()
        db.scheduler_executions.find_one.return_value = {'_id': 'test', 'processed_items': 0, 'total_items': 2}
        redis.get.return_value = '{}'
        with patch('app.worker.akshare_sync_service.bind_loop_safe_mongo_db', return_value=db), \
             patch('app.core.database.get_mongo_db_sync', return_value=db), \
             patch('app.core.database.get_redis_sync_client', return_value=redis):
            result = await self.service.sync_historical_data(
                start_date='2026-10-01', end_date='2026-10-13',
                symbols=['000001', '000002'], incremental=False,
            )
        self.assertTrue(result['cancelled'])
        self.assertEqual(outcome(result)[0], 'cancelled')
        self.service.provider.get_historical_data.assert_not_awaited()
        self.assertEqual(db.scheduler_executions.update_one.call_args.args[1]['$set']['status'], 'cancelled')
        self.assertEqual(json.loads(redis.setex.call_args.args[2])['status'], 'cancelled')


if __name__ == '__main__':
    unittest.main()
