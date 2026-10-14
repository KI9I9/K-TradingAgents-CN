"""调度持久化与手动任务收尾的回归测试，不调用行情接口。"""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.services.scheduler_runtime import save_enabled, restore_preferences, finalize_execution, run_manual


class Cursor:
    def __init__(self, docs): self.docs = iter(docs)
    def __aiter__(self): return self
    async def __anext__(self):
        try: return next(self.docs)
        except StopIteration: raise StopAsyncIteration


class Collection:
    def __init__(self): self.docs = {}
    def find(self, query): return Cursor(list(self.docs.values()))
    async def update_one(self, query, update, upsert=False):
        key = query['_id']
        doc = self.docs.get(key)
        if doc is None and upsert: doc = self.docs.setdefault(key, {'_id': key})
        if doc is None: return
        if 'job_id' in query and doc.get('job_id') != query['job_id']: return
        if 'status' in query and doc.get('status') not in query['status']['$in']: return
        doc.update(update['$set'])


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = SimpleNamespace(scheduler_preferences=Collection(), scheduler_executions=Collection())
        self.schedulers = []

    def scheduler(self):
        s = AsyncIOScheduler()
        s.add_job(lambda: None, 'interval', hours=24, id='sample')
        s.start(paused=True)
        self.schedulers.append(s)
        return s

    async def asyncTearDown(self):
        for s in self.schedulers: s.shutdown(wait=False)
        await asyncio.sleep(0)

    async def test_enabled_and_disabled_survive_restart(self):
        s = self.scheduler()
        for enabled in (False, True):
            await save_enabled(s, self.db, 'sample', enabled)
            restarted = self.scheduler()
            await restore_preferences(restarted, self.db)
            self.assertEqual(restarted.get_job('sample').next_run_time is not None, enabled)

    async def test_save_failure_rolls_back(self):
        s = self.scheduler()
        before = s.get_job('sample').next_run_time
        self.db.scheduler_preferences.update_one = AsyncMock(side_effect=RuntimeError('数据库不可用'))
        with self.assertRaises(RuntimeError): await save_enabled(s, self.db, 'sample', False)
        self.assertEqual(s.get_job('sample').next_run_time, before)

    async def execute(self, func, initial='running'):
        oid = ObjectId()
        doc = {'_id': oid, 'job_id': 'sample', 'status': initial, 'progress': 37}
        self.db.scheduler_executions.docs[oid] = doc
        async def finish(**kwargs): await finalize_execution(self.db, 'sample', str(oid), **kwargs)
        await run_manual(func, {}, finish)
        return doc

    async def test_skipped_finishes_without_sync_success(self):
        async def job(): return {'success': True, 'skipped': True, 'message': '18点前跳过'}
        doc = await self.execute(job)
        self.assertEqual(doc['status'], 'skipped')
        self.assertEqual(doc['processed_items'], 0)
        self.assertIn('18点', doc['progress_message'])
        self.assertIn('finished_at', doc)

    async def test_exception_and_business_failure_finish(self):
        async def raises(): raise RuntimeError('接口不可用')
        async def failed(): return {'success': False, 'error': '无权限'}
        for job in (raises, failed): self.assertEqual((await self.execute(job))['status'], 'failed')

    async def test_sync_success_finishes(self):
        self.assertEqual((await self.execute(lambda: {'success': True}))['status'], 'success')

    async def test_preserves_worker_terminal_result(self):
        doc = await self.execute(lambda: None, initial='partial')
        self.assertEqual(doc['status'], 'partial')
        self.assertEqual(doc['progress'], 37)

    async def test_execution_isolation(self):
        oid = ObjectId()
        self.db.scheduler_executions.docs[oid] = {'_id': oid, 'job_id': 'other', 'status': 'running'}
        await finalize_execution(self.db, 'sample', str(oid), result={'skipped': True})
        self.assertEqual(self.db.scheduler_executions.docs[oid]['status'], 'running')

    async def test_cancelled_task_finalizes(self):
        finish = AsyncMock()
        async def job(): raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError): await run_manual(job, {}, finish)
        self.assertTrue(finish.await_args.kwargs['cancelled'])


if __name__ == '__main__': unittest.main(verbosity=2)
