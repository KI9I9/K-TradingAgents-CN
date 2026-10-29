"""Original HTTP route and scheduler method -> real service/gate/journal/store.

ASGI transport, synthetic SDK responses and in-memory Mongo-shaped collection;
no auth credentials, external sockets, production settings or permits.
"""
import ast
import asyncio
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys
import tempfile
import types
from typing import Optional, Dict, Any
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI, HTTPException, Depends, Query, Request
import httpx
from test_history_batch_adapters import ReadCollection
from test_tushare_history import history, frame, provider, DB_MODULE, ROOT
from tradingagents.utils import tushare_batch as gate_module
from tradingagents.utils.tushare_manual import CURRENT
from deploy.history_batch_journal import Workspace
from app.services import tushare_history_batches as button


def extract(path,name,namespace):
    tree=ast.parse((ROOT/path).read_text())
    node=next(n for n in ast.walk(tree) if isinstance(n,ast.AsyncFunctionDef) and n.name==name)
    node.decorator_list=[]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),str(path),'exec'),namespace)
    return namespace[name]


class Executions:
    def __init__(self):self.rows={}
    async def insert_one(self,record):
        key=str(len(self.rows)+1);self.rows[key]=dict(deepcopy(record),_id=key)
        return types.SimpleNamespace(inserted_id=key)
    async def find_one(self,query):return deepcopy(self.rows.get(query['_id']))
    async def update_one(self,query,update):self.rows[query['_id']].update(deepcopy(update['$set']))


class ButtonTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.clock=datetime(2026,10,19,19,tzinfo=gate_module.BJ)
        self.timepatch=patch.object(gate_module.time,'time',side_effect=lambda:self.clock.timestamp())
        self.timepatch.start();self.addCleanup(self.timepatch.stop)
        policy=dict(schema=1,codes=['600000.SH'],min_date='2025-10-19',max_date='2026-10-20',
            expires_at=self.clock.timestamp()+2*86400,max_requests=4,daily_requests=4,minute_requests=4,write_enabled=True)
        self.gate=gate_module.provision(Path(self.tmp.name)/'gate',policy)
        self.workspace=Workspace(self.gate.directory/'runs');self.workspace.provision()
        self.col=ReadCollection();self.db=types.SimpleNamespace(stock_daily_quotes=self.col)
        dbmodule=types.ModuleType(DB_MODULE);dbmodule.get_database=dbmodule.get_mongo_db=lambda:self.db
        self.dbpatch=patch.dict(sys.modules,{DB_MODULE:dbmodule});self.dbpatch.start();self.addCleanup(self.dbpatch.stop)
        self.history=history.HistoricalDataService()
        await self.history.save_historical_data('600000',frame(('20261018',)),'tushare')
        raw=frame(('20261018','20261019')).rename(columns={'volume':'vol'})
        self.provider,self.sdk,ns=provider(raw)
        ns['_manual_current']=CURRENT
        self.sent=[];self.send_failure=None
        def sdk(**kwargs):
            job=CURRENT.get();self.assertIsNotNone(job)
            for api in ('daily','adj_factor'):
                def operation():
                    s=json.loads((self.gate.directory/'state.json').read_text())
                    self.assertEqual(s['phase'],'in_flight')
                    self.assertEqual(s['used'],len(self.sent)+1)
                    self.sent.append(api)
                    if self.send_failure:raise self.send_failure
                    if api=='daily':return dict(fields=list(raw.columns),items=raw.values.tolist())
                    return dict(fields=['ts_code','trade_date','adj_factor'],items=[['600000.SH',day,17.3] for day in ('20261018','20261019')])
                job.perform(api,'',dict(ts_code='600000.SH',start_date=kwargs['start_date'],end_date=kwargs['end_date']),operation)
            return raw.copy(deep=True)
        self.sdk.pro_bar.side_effect=sdk
        self.factory=AsyncMock(return_value=self.provider);self.executions=Executions()
        self.service=button.HistoryBatchService(self.gate,self.workspace,self.col,self.executions,self.history,self.factory,clock=lambda:self.clock)
        self.submitpatch=patch.object(button,'submit_history_batch',side_effect=self.service.submit)
        self.submitpatch.start();self.addCleanup(self.submitpatch.stop)
        self.progress=Mock()
        router_stub=types.ModuleType('app.routers.scheduler');router_stub._update_scheduler_progress_cache=self.progress
        self.routerpatch=patch.dict(sys.modules,{'app.routers.scheduler':router_stub});self.routerpatch.start();self.addCleanup(self.routerpatch.stop)
        method=extract(Path('app/services/scheduler_service.py'),'trigger_job',dict(Optional=Optional,Dict=Dict,Any=Any,HTTPException=HTTPException))
        class Scheduler:
            trigger_job=method
        self.scheduler=Scheduler()
        async def user(request:Request):return {'is_admin':request.headers.get('x-test-admin')=='yes'}
        def get_service():return self.scheduler
        ns=dict(Optional=Optional,HTTPException=HTTPException,Depends=Depends,Query=Query,
            get_current_user=user,get_scheduler_service=get_service,SchedulerService=Scheduler,
            logger=Mock(),ok=lambda **kw:dict(success=True,**kw))
        route=extract(Path('app/routers/scheduler.py'),'trigger_job',ns)
        app=FastAPI();app.post('/api/scheduler/jobs/{job_id}/trigger')(route)
        self.http=httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test')

    async def asyncTearDown(self):
        if button._TASKS:await asyncio.gather(*list(button._TASKS),return_exceptions=True)
        await self.http.aclose()

    async def click(self,query='',admin=True):
        result=await self.http.post('/api/scheduler/jobs/tushare_historical_sync/trigger'+query,
            headers={'x-test-admin':'yes' if admin else 'no'})
        if button._TASKS:await asyncio.gather(*list(button._TASKS),return_exceptions=True)
        return result

    def state(self):
        with self.gate.locked() as fd:return self.gate.read(fd)[1]

    async def test_original_button_reaches_provider_store_and_visible_success(self):
        response=await self.click()
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.sent,['daily','adj_factor'])
        self.assertEqual(len(self.col.rows),2)
        self.assertEqual(self.executions.rows['1']['status'],'success')
        self.assertEqual(self.executions.rows['1']['processed_items'],1)
        self.assertTrue(self.executions.rows['1']['result']['success'])
        self.assertEqual(self.state()['used'],2)
        self.assertTrue(self.progress.called)

    async def test_second_click_does_not_repeat_completed_scope(self):
        await self.click();before=deepcopy(self.col.rows)
        self.assertEqual((await self.click()).status_code,200)
        self.assertEqual(self.col.rows,before);self.assertEqual(len(self.sent),2)

    async def test_force_does_not_bypass_gate_or_concurrency(self):
        self.assertEqual((await self.click('?force=true')).status_code,400)
        self.assertEqual(self.sent,[]);self.assertEqual(self.executions.rows,{})

    async def test_nonadmin_does_not_create_task_or_send(self):
        self.assertEqual((await self.click(admin=False)).status_code,403)
        self.assertEqual(self.executions.rows,{});self.assertEqual(self.sent,[])

    async def test_stop_before_click_returns_actionable_error(self):
        self.gate.stop();response=await self.click()
        self.assertEqual(response.status_code,400);self.assertIn('disabled',response.text)
        self.factory.assert_not_awaited();self.assertEqual(self.executions.rows,{})

    async def test_missing_policy_is_not_success(self):
        (self.gate.directory/'policy.json').unlink()
        response=await self.click();self.assertEqual(response.status_code,400)
        self.assertIn('not_authorized',response.text);self.assertEqual(self.sent,[])

    async def test_daily_limit_does_not_autoincrease(self):
        await self.click()
        # Same day full-year mode changes plan, but still uses the same finite ledger.
        self.assertEqual((await self.click('?incremental=false')).status_code,200)
        self.assertEqual(self.state()['used'],4)
        self.clock+=timedelta(days=1)
        response=await self.click();self.assertEqual(response.status_code,400)
        self.assertIn('budget_exhausted',response.text);self.assertEqual(len(self.sent),4)

    async def test_year_bootstrap_reaches_original_button_not_thirty_day_block(self):
        self.col.rows=[]
        response=await self.click()
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.executions.rows['1']['status'],'success')
        self.assertEqual(self.state()['plan']['items'][0]['start'],'2025-10-19')
        self.assertEqual(self.sent,['daily','adj_factor'])

    async def test_legacy_data_named_before_any_request(self):
        del self.col.rows[0]['history_contract_version']
        response=await self.click();self.assertEqual(response.status_code,400)
        self.assertIn('600000.SH:legacy_contract_unproven',response.text)
        self.assertEqual(self.sent,[]);self.assertEqual(self.executions.rows,{})

    async def test_upstream_failure_visible_and_never_retried(self):
        self.send_failure=gate_module.TushareEgressStopped('usage_limit',40203)
        self.assertEqual((await self.click()).status_code,200)
        self.assertEqual(self.sent,['daily'])
        self.assertEqual(self.executions.rows['1']['status'],'failed')
        self.assertIn('usage_limit',self.executions.rows['1']['error_message'])
        self.assertEqual((await self.click()).status_code,400)
        self.assertEqual(self.sent,['daily']);self.assertEqual(len(self.col.rows),1)

    async def test_next_day_resumes_saved_result_with_fixed_old_dates(self):
        original=self.gate.authorize
        def check(identity,item,action):
            if action=='write':raise gate_module.TushareEgressStopped('not_authorized')
            return original(identity,item,action)
        with patch.object(self.gate,'authorize',side_effect=check):
            self.assertEqual((await self.click()).status_code,200)
        self.assertEqual(len(self.sent),2);self.assertEqual(len(self.col.rows),1)
        self.clock+=timedelta(days=1)
        self.assertEqual((await self.click()).status_code,200)
        self.assertEqual(len(self.sent),2);self.assertEqual(len(self.col.rows),2)
        self.assertEqual(self.executions.rows['2']['status'],'success')
        self.assertEqual(self.state()['plan']['end'],'2026-10-19')

    async def test_expired_policy_is_not_renewed_on_click(self):
        self.clock+=timedelta(days=3)
        response=await self.click();self.assertEqual(response.status_code,400)
        self.assertIn('expired',response.text);self.assertEqual(self.sent,[])

    async def test_completed_data_drift_is_not_reported_as_success(self):
        await self.click();self.col.rows[-1]['amount']+=1
        response=await self.click();self.assertEqual(response.status_code,400)
        self.assertEqual(len(self.sent),2)
        self.assertEqual(len(self.executions.rows),1)

    async def test_old_resume_kwargs_do_not_reenter_unsafe_worker(self):
        with self.assertRaises(HTTPException):
            await self.scheduler.trigger_job('tushare_historical_sync',{'_resume_from_index':1})
        self.assertEqual(self.sent,[])

    async def test_parallel_click_is_rejected_before_second_task(self):
        event=asyncio.Event();original=self.factory.side_effect
        async def wait_factory():await event.wait();return self.provider
        self.factory.side_effect=wait_factory
        first=await self.http.post('/api/scheduler/jobs/tushare_historical_sync/trigger',headers={'x-test-admin':'yes'})
        self.assertEqual(first.status_code,200)
        await asyncio.sleep(0)
        second=await self.http.post('/api/scheduler/jobs/tushare_historical_sync/trigger',headers={'x-test-admin':'yes'})
        self.assertEqual(second.status_code,400);self.assertIn('busy',second.text)
        event.set()
        if button._TASKS:await asyncio.gather(*list(button._TASKS))
        self.assertEqual(len(self.executions.rows),1)


if __name__=='__main__':unittest.main()
