"""Real ingress/route method bodies; no provider, network or production writes."""
import ast
from datetime import datetime, timezone, timedelta
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'deploy'))
from offline_tushare_support import policy, egress, load

class HTTPException(Exception):
    def __init__(self,status_code,detail):self.status_code=status_code;self.detail=detail
fastapi=types.ModuleType('fastapi');fastapi.HTTPException=HTTPException
sys.modules['fastapi']=fastapi
m=load('app.services.tushare_manual_sync',ROOT/'app/services/tushare_manual_sync.py')

class ContractError(ValueError):pass
contract=types.ModuleType('app.services.historical_contract');contract.HistoricalContractError=ContractError
history=types.ModuleType('app.services.historical_data_service')
route=types.ModuleType('app.routers.stock_sync')
response=types.ModuleType('app.core.response');response.ok=lambda **kwargs:kwargs


def extract(path,name):
    tree=ast.parse((ROOT/path).read_text())
    node=next(n for n in ast.walk(tree) if isinstance(n,ast.AsyncFunctionDef) and n.name==name)
    node.decorator_list=[];node.returns=None;node.args.defaults=[]
    for arg in node.args.args:arg.annotation=None
    ns=dict(HTTPException=HTTPException,logger=Mock())
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),str(path),'exec'),ns)
    return ns[name]


class Ingress(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.validator=AsyncMock(return_value=[])
        history.HistoricalDataService=Mock(return_value=types.SimpleNamespace(validate_tushare_series=self.validator))
        route._request_to_dict=lambda r:r
        route._create_stock_sync_task=AsyncMock(return_value={'task_id':'synthetic'})
        self.patcher=patch.dict(sys.modules,{'app.services.historical_data_service':history,
            'app.services.historical_contract':contract,'app.routers.stock_sync':route,'app.core.response':response})
        self.patcher.start();self.addCleanup(self.patcher.stop)
        self.approved=dict(codes=['600000.SH'],start='20261021',end='20261027')
        self.now=datetime(2026,10,28,10,tzinfo=timezone(timedelta(hours=8)))
        self.payload=dict(symbol='600000',days=8,data_source='tushare',sync_historical=True,
                          sync_realtime=False,sync_financial=False,sync_basic=False)
        self.gate=Mock()
        self.gate.status.return_value=dict(reason='none',mode='manual',busy=False)
        self.gate.begin.return_value=Mock(approved=self.approved)
        self.clock=patch.object(m,'scope',return_value=self.approved);self.clock.start();self.addCleanup(self.clock.stop)

    async def test_batch_legacy_rejected_before_reservation_or_task(self):
        self.approved['codes'].append('000001.SZ')
        self.validator.side_effect=[[],ContractError('history_existing_contract_unproven')]
        with patch.object(m,'DailyGate',return_value=self.gate):
            with self.assertRaises(HTTPException) as error:await m.submit(self.payload,'batch',{})
        self.assertEqual(error.exception.status_code,409)
        self.assertIn('000001.SZ',error.exception.detail)
        self.gate.begin.assert_not_called();route._create_stock_sync_task.assert_not_called()
        self.assertEqual(self.validator.await_count,2)

    async def test_stopped_gate_does_not_query_history_or_reserve(self):
        self.gate.status.return_value['reason']='disabled'
        with patch.object(m,'DailyGate',return_value=self.gate):
            with self.assertRaises(HTTPException) as error:await m.submit(self.payload,'single',{})
        self.assertEqual(error.exception.status_code,409)
        self.validator.assert_not_awaited();self.gate.begin.assert_not_called()

    async def test_invalid_request_does_not_open_gate(self):
        with patch.object(m,'scope',side_effect=egress.TushareEgressStopped('invalid_request')),patch.object(m,'DailyGate') as gate:
            with self.assertRaises(HTTPException) as error:await m.submit({},'single',{})
        self.assertEqual(error.exception.status_code,422);gate.assert_not_called()

    async def test_database_error_does_not_reserve(self):
        self.validator.side_effect=RuntimeError('synthetic_database_error')
        with patch.object(m,'DailyGate',return_value=self.gate):
            with self.assertRaises(RuntimeError):await m.submit(self.payload,'single',{})
        self.gate.begin.assert_not_called();route._create_stock_sync_task.assert_not_called()

    async def test_window_without_qfq_overlap_rejected(self):
        self.validator.return_value=[{'trade_date':'2026-09-01'}]
        with self.assertRaises(HTTPException):await m.preflight_series(self.approved,self.now)

    async def test_new_series_and_existing_overlap_accepted(self):
        await m.preflight_series(self.approved,self.now)
        self.validator.return_value=[{'trade_date':'2026-10-27'}]
        await m.preflight_series(self.approved,self.now)
        self.assertEqual(self.validator.await_count,2)

    async def test_today_before_cutoff_and_future_refused(self):
        for end in ('20261028','20261029'):
            with self.assertRaises(HTTPException):await m.preflight_series({**self.approved,'end':end},self.now)
        self.validator.assert_not_awaited()
        await m.preflight_series({**self.approved,'end':'20261028'},datetime(2026,10,28,10,tzinfo=timezone.utc))
        self.validator.assert_awaited_once()

    async def test_scope_change_during_preflight_latches_before_task(self):
        job=self.gate.begin.return_value;job.approved={**self.approved,'end':'20261028'}
        with patch.object(m,'DailyGate',return_value=self.gate):
            with self.assertRaises(HTTPException):await m.submit(self.payload,'single',{})
        job.fail.assert_called_once_with('invalid_request');route._create_stock_sync_task.assert_not_called()

    async def test_valid_submission_queues_once_after_preflight(self):
        def create(coro):coro.close();return Mock()
        with patch.object(m,'DailyGate',return_value=self.gate),patch.object(m.asyncio,'create_task',side_effect=create) as task:
            result=await m.submit(self.payload,'single',{})
        self.assertEqual(result['data']['task_id'],'synthetic')
        self.gate.begin.assert_called_once();task.assert_called_once();m._TASKS.clear()

    async def test_background_rechecks_before_provider_initialization(self):
        with patch.object(m,'preflight_series',side_effect=HTTPException(409,'synthetic')):
            with self.assertRaises(HTTPException):await m.sync_scope(Mock(approved=self.approved))
        # Worker module is not loaded by this test; importing it would fail without app dependencies.

    async def test_scheduler_force_and_normal_reject_before_side_effects(self):
        fn=extract('app/services/scheduler_service.py','trigger_job')
        batch=types.ModuleType('app.services.tushare_history_batches')
        batch.submit_history_batch=AsyncMock(side_effect=HTTPException(400,'unconfigured policy'))
        self.patcher2=patch.dict(sys.modules,{'app.services.tushare_history_batches':batch})
        self.patcher2.start();self.addCleanup(self.patcher2.stop)
        for force in (False,True):
            service=Mock();kwargs={}
            with self.assertRaises(HTTPException) as error:await fn(service,'tushare_historical_sync',kwargs,force)
            self.assertEqual(error.exception.status_code,400);self.assertEqual(kwargs,{})
            self.assertEqual(service.mock_calls,[])
        m.reject_scheduled_job('akshare_historical_sync')

    async def test_resume_and_retry_reject_before_history_mutations(self):
        for name in ('resume_suspended_execution','retry_failed_symbols'):
            fn=extract('app/routers/scheduler.py',name)
            col=Mock();col.find_one=AsyncMock(return_value={'job_id':'tushare_historical_sync','status':'suspended'})
            service=Mock();service._get_db.return_value=types.SimpleNamespace(scheduler_executions=col)
            kwargs=dict(execution_id='000000000000000000000001',user={'is_admin':True},service=service)
            if name=='retry_failed_symbols':kwargs.update(start_date=None,end_date=None,period='daily')
            with self.assertRaises(HTTPException) as error:await fn(**kwargs)
            self.assertEqual(error.exception.status_code,400)
            col.update_one.assert_not_called();col.update_many.assert_not_called();col.find.assert_not_called()

if __name__=='__main__':unittest.main()
