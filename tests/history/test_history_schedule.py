"""Real scheduled wrapper -> batch/gate/provider/journal/store, without sockets."""
import asyncio
import ast
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from test_history_basic_tier import BasicTierTests, button, gate_module, frame, raw_json
from test_history_button import extract
from app.services import tushare_history_schedule as schedule
from fastapi import HTTPException


class ScheduledHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.f=BasicTierTests()
        await self.f.asyncSetUp()
        self.permit=Path(self.f.tmp.name)/'schedule.json'
        self.permitpatch=patch.object(schedule,'PERMIT',self.permit)
        self.permitpatch.start()
        self.selection=patch.object(schedule,'selected_gate',side_effect=lambda:self.f.gate)
        self.selection.start()
        self.calendar=types.SimpleNamespace(_read_cache=AsyncMock(side_effect=lambda:dict(
            fetched_at=self.f.clock.date().isoformat(),source='akshare',last_trade_date=self.f.clock.date().isoformat())),
            _fetch_from_akshare=AsyncMock(return_value=None))
        self.calpatch=patch('app.services.trading_calendar_service.get_trading_calendar_service',return_value=self.calendar)
        self.calpatch.start()
        self.write_permit()
        self.worker=extract(Path('app/worker/tushare_sync_service.py'),'run_tushare_historical_sync',{})

    def write_permit(self):
        p,_=self.f.gate.status()
        value=dict(schema=1,generation=str(self.f.gate.directory),policy_sha256=gate_module.sha(p),
                   cron=schedule.CRON,timezone=schedule.TIMEZONE,expires_at=p['expires_at'])
        self.permit.write_text(json.dumps(value));self.permit.chmod(0o600)

    async def asyncTearDown(self):
        for p in (self.calpatch,self.selection,self.permitpatch):p.stop()
        await self.f.asyncTearDown();self.f.doCleanups()

    async def test_real_scheduled_entry_waits_for_daily_only_completion(self):
        result=await self.worker()
        self.assertTrue(result['success'])
        self.assertTrue(result['_execution_recorded'])
        row=self.f.executions.rows['1']
        self.assertEqual(row['status'],'success')
        self.assertFalse(row['is_manual'])
        self.assertEqual(self.f.sent,['daily'])
        self.assertFalse(button._TASKS)
        self.assertEqual(len(self.f.col.rows),2)
        self.assertEqual(raw_json(self.f.legacy_col.rows),self.f.old)

    async def test_next_day_builds_incremental_plan_and_keeps_old_records(self):
        await self.worker()
        first=self.f.state()['plan']
        original=deepcopy(self.f.col.rows)
        self.f.clock+=timedelta(days=1)
        self.f.raw=frame(('20261019','20261020')).rename(columns={'volume':'vol'})
        await self.worker()
        second=self.f.state()['plan']
        self.assertNotEqual(first['plan_sha256'],second['plan_sha256'])
        self.assertEqual(second['items'][0]['start'],'2026-10-19')
        self.assertEqual(second['end'],'2026-10-20')
        self.assertEqual(self.f.state()['used'],2)
        self.assertEqual(len(self.f.col.rows),3)
        for row in original:self.assertIn(row,self.f.col.rows)

    async def test_duplicate_day_has_no_second_request(self):
        await self.worker();before=self.f.state()['used']
        await self.worker()
        self.assertEqual(self.f.state()['used'],before)
        self.assertEqual(self.f.executions.rows['2']['reason'],'already_scheduled_today')

    async def test_only_history_may_be_scheduled_and_no_force_or_full_mode(self):
        for options in ({'incremental':False},{'_force_execute':True},{'_resume_from_index':0}):
            with self.assertRaises(ValueError):await self.worker(**options)
        self.assertFalse(self.f.sent)

    async def test_missing_mismatched_or_public_permit_never_sends(self):
        self.permit.chmod(0o644)
        with self.assertRaises(HTTPException):await self.worker()
        self.permit.chmod(0o600)
        p=json.loads(self.permit.read_text());p['policy_sha256']='0'*64
        self.permit.write_text(json.dumps(p))
        with self.assertRaises(HTTPException):await self.worker()
        self.permit.unlink()
        with self.assertRaises(HTTPException):await self.worker()
        self.assertFalse(self.f.sent)
        self.assertFalse((self.f.gate.directory/'STOP').exists())

    async def test_pre_close_weekend_holiday_and_unverified_calendar_skip(self):
        self.f.clock=(self.f.clock+timedelta(days=1)).replace(hour=17)
        await self.worker()
        self.assertEqual(self.f.executions.rows['1']['reason'],'before_daily_sync_time')
        self.f.clock=self.f.clock.replace(hour=19)
        self.calendar._read_cache.return_value=None
        self.calendar._read_cache.side_effect=None
        await self.worker()
        self.assertEqual(self.f.executions.rows['2']['reason'],'calendar_unverified')
        self.calendar._read_cache.return_value=dict(fetched_at='2026-10-20',source='akshare',last_trade_date='2026-10-19')
        await self.worker()
        self.assertEqual(self.f.executions.rows['3']['reason'],'non_trading_day')
        self.assertFalse(self.f.sent)
        self.assertFalse(list(self.f.gate.directory.glob('scheduled-*.json')))
        self.assertEqual(await schedule.trade_day_reason(self.f.clock+timedelta(days=4)), 'non_trading_day')

    async def test_fallback_calendar_is_not_trading_evidence(self):
        self.calendar._read_cache.side_effect=None
        self.calendar._read_cache.return_value=dict(fetched_at='2026-10-19',source='fallback',last_trade_date='2026-10-19')
        await self.worker()
        self.assertEqual(self.f.executions.rows['1']['reason'],'calendar_unverified')
        self.assertFalse(self.f.sent)

    async def test_manual_and_scheduled_share_the_admission_lock(self):
        entered,release=asyncio.Event(),asyncio.Event()
        async def factory():
            entered.set();await release.wait();return self.f.provider
        self.f.factory.side_effect=factory
        task=asyncio.create_task(self.worker())
        await asyncio.wait_for(entered.wait(),2)
        other=button.HistoryBatchService(self.f.gate,self.f.workspace,self.f.col,self.f.executions,
            self.f.history,self.f.factory,clock=lambda:self.f.clock)
        try:
            with self.assertRaises(HTTPException) as error:await other.submit()
            self.assertIn('busy',str(error.exception.detail))
        finally:
            release.set();await task
        self.assertEqual(self.f.sent,['daily'])

    async def test_failure_keeps_stop_and_next_tick_cannot_recover(self):
        from tradingagents.utils.tushare_egress import TushareEgressStopped
        self.f.send_failure=TushareEgressStopped('timeout')
        with patch.object(gate_module.time,'sleep'):result=await self.worker()
        self.assertFalse(result['success'])
        self.assertTrue(result['_execution_recorded'])
        self.assertTrue((self.f.gate.directory/'STOP').exists())
        self.assertEqual(self.f.state()['used'],3)
        before=len(self.f.sent)
        with self.assertRaises(HTTPException):await self.worker()
        self.assertEqual(len(self.f.sent),before)

    async def test_preparation_failure_after_reservation_stops_generation(self):
        self.f.service._prepare=AsyncMock(side_effect=RuntimeError('synthetic inventory mismatch'))
        with self.assertRaises(RuntimeError):await self.worker()
        self.assertTrue((self.f.gate.directory/'STOP').exists())
        self.assertFalse(self.f.sent)

    def scheduler(self):
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from tradingagents.utils.tushare_manual import JOB_IDS
        scheduler=AsyncIOScheduler(timezone=schedule.TIMEZONE)
        for job_id in JOB_IDS:
            scheduler.add_job(self.worker,'cron',hour=5,id=job_id,kwargs={'incremental':True})
        scheduler.start(paused=True)
        self.addAsyncCleanup(self.shutdown,scheduler)
        return scheduler

    async def shutdown(self,scheduler):
        scheduler.shutdown(wait=False)
        await asyncio.sleep(0)

    def preferences(self):
        # Minimal persisted Mongo-shaped collection, shared by real schedulers.
        class Preferences:
            def __init__(self):self.rows={}
            async def update_one(self,q,u,upsert=False):self.rows[q['_id']]=dict(_id=q['_id'],**u['$set'])
            async def count_documents(self,q):return sum(self.rows.get(k,{}).get('enabled')==q['enabled'] for k in q['_id']['$in'])
            def find(self,q):
                async def rows():
                    for row in list(self.rows.values()):yield row
                return rows()
        return types.SimpleNamespace(scheduler_preferences=Preferences())

    async def test_history_choice_and_approved_cron_survive_restart_others_stay_closed(self):
        from app.services.scheduler_runtime import save_enabled,restore_preferences
        from tradingagents.utils.tushare_manual import JOB_IDS
        db=self.preferences();first=self.scheduler()
        for enabled in (True,False,True):
            await save_enabled(first,db,schedule.JOB_ID,enabled)
            restarted=self.scheduler()
            await restore_preferences(restarted,db)
            job=restarted.get_job(schedule.JOB_ID)
            self.assertEqual(job.next_run_time is not None,enabled)
            self.assertEqual(str(job.trigger.timezone),schedule.TIMEZONE)
            self.assertEqual((job.max_instances,job.coalesce,job.misfire_grace_time),(1,True,300))
            self.assertEqual(str(job.trigger),"cron[month='*', day='*', day_of_week='mon-fri', hour='18', minute='30']")
            for job_id in set(JOB_IDS)-{schedule.JOB_ID}:
                self.assertIsNone(restarted.get_job(job_id).next_run_time)
                with self.assertRaises(ValueError):await save_enabled(first,db,job_id,True)
        self.assertFalse(self.f.sent)

    async def test_invalid_permission_disables_saved_choice_on_restart(self):
        from app.services.scheduler_runtime import save_enabled,restore_preferences
        db=self.preferences();first=self.scheduler()
        await save_enabled(first,db,schedule.JOB_ID,True)
        self.permit.unlink()
        restarted=self.scheduler();await restore_preferences(restarted,db)
        self.assertIsNone(restarted.get_job(schedule.JOB_ID).next_run_time)
        self.assertFalse(db.scheduler_preferences.rows[schedule.JOB_ID]['enabled'])
        with self.assertRaises(Exception):await save_enabled(first,db,schedule.JOB_ID,True)

    async def test_permission_or_preference_failure_restores_original_trigger_and_pause(self):
        from app.services.scheduler_runtime import save_enabled
        db=self.preferences();scheduler=self.scheduler()
        scheduler.pause_job(schedule.JOB_ID)
        job=scheduler.get_job(schedule.JOB_ID);before=(job.trigger,job.next_run_time)
        db.scheduler_preferences.update_one=AsyncMock(side_effect=RuntimeError('db unavailable'))
        with self.assertRaises(RuntimeError):await save_enabled(scheduler,db,schedule.JOB_ID,True)
        job=scheduler.get_job(schedule.JOB_ID)
        self.assertEqual((job.trigger,job.next_run_time),before)
        with patch.object(scheduler,'pause_job',side_effect=RuntimeError('partial scheduler mutation')):
            with self.assertRaises(RuntimeError):await save_enabled(scheduler,db,schedule.JOB_ID,True)
        job=scheduler.get_job(schedule.JOB_ID)
        self.assertEqual((job.trigger,job.next_run_time),before)

    async def test_scheduler_callback_does_not_insert_success_over_recorded_failure(self):
        from datetime import datetime
        tree=ast.parse(Path('app/services/scheduler_service.py').read_text())
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='_on_job_executed')
        ns=dict(JobExecutionEvent=object,asyncio=asyncio,datetime=datetime)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),'<callback>','exec'),ns)
        recorder=AsyncMock()
        receiver=types.SimpleNamespace(_record_job_execution=recorder)
        self.f.send_failure=__import__('tradingagents.utils.tushare_egress',fromlist=['TushareEgressStopped']).TushareEgressStopped('timeout')
        with patch.object(gate_module.time,'sleep'):result=await self.worker()
        ns['_on_job_executed'](receiver,types.SimpleNamespace(job_id=schedule.JOB_ID,retval=result))
        await asyncio.sleep(0)
        recorder.assert_not_awaited()
        self.assertEqual(len(self.f.executions.rows),1)
        self.assertEqual(self.f.executions.rows['1']['status'],'failed')

    async def test_reschedule_cannot_bypass_cron_or_enable_paused_history(self):
        scheduler=self.scheduler();scheduler.pause_job(schedule.JOB_ID)
        receiver=types.SimpleNamespace(scheduler=scheduler,_record_job_action=AsyncMock())
        ns=dict(logger=Mock())
        method=extract(Path('app/services/scheduler_service.py'),'reschedule_job',ns)
        config=types.ModuleType('app.core.config');config.settings=types.SimpleNamespace(TIMEZONE=schedule.TIMEZONE)
        with patch.dict(sys.modules,{'app.core.config':config}):
            self.assertFalse(await method(receiver,schedule.JOB_ID,'* * * * *'))
            self.assertTrue(await method(receiver,schedule.JOB_ID,schedule.CRON))
        self.assertIsNone(scheduler.get_job(schedule.JOB_ID).next_run_time)

    async def test_real_apscheduler_dispatch_waits_for_batch_completion(self):
        from datetime import datetime,timezone,timedelta
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.events import EVENT_JOB_EXECUTED,EVENT_JOB_ERROR
        scheduler=AsyncIOScheduler();finished=asyncio.Event();events=[]
        def completed(event):events.append(event);finished.set()
        scheduler.add_listener(completed,EVENT_JOB_EXECUTED|EVENT_JOB_ERROR)
        scheduler.add_job(self.worker,'date',run_date=datetime.now(timezone.utc)+timedelta(milliseconds=80),id=schedule.JOB_ID)
        scheduler.start();self.addAsyncCleanup(self.shutdown,scheduler)
        await asyncio.wait_for(finished.wait(),5)
        self.assertIsNone(events[0].exception)
        self.assertTrue(events[0].retval['success'])
        self.assertEqual(self.f.executions.rows['1']['status'],'success')
        self.assertFalse(button._TASKS)

    async def test_full_pool_preparation_failure_is_awaited_and_returns_failure(self):
        policy,_=self.f.gate.status()
        policy['codes']=[str(i).zfill(6)+'.SZ' for i in range(1,52)]
        self.f.gate=gate_module.provision(Path(self.f.tmp.name)/'full-pool',policy)
        from deploy.history_batch_journal import Workspace
        self.f.workspace=Workspace(self.f.gate.directory/'runs');self.f.workspace.provision()
        self.f.service.gate,self.f.service.workspace=self.f.gate,self.f.workspace
        self.write_permit()
        self.f.service._prepare=AsyncMock(side_effect=RuntimeError('synthetic inventory drift'))
        result=await self.worker()
        self.assertFalse(result['success'])
        self.assertEqual(self.f.executions.rows['1']['status'],'failed')
        self.assertTrue((self.f.gate.directory/'STOP').exists())
        self.assertFalse(button._TASKS)
        self.assertFalse(self.f.sent)


if __name__=='__main__':
    unittest.main(defaultTest='ScheduledHistoryTests')
