"""Original scheduler button -> fixed-plan, bounded historical execution.

No policy is created by an HTTP request. A separately provisioned finite policy
and journal directory are required; all other Tushare scheduled jobs stay closed.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import fcntl
import os

from fastapi import HTTPException
from tradingagents.utils.tushare_batch import BatchGate, DIRECTORY, selected_gate
from tradingagents.utils.tushare_egress import TushareEgressStopped, _private_fd
from deploy.history_batch_journal import Workspace, BatchStopped
from deploy.history_batch_adapters import Inventory, HistoricalAdapter
from deploy.history_sync_plan import build_plan, PlanError

JOB_ID='tushare_historical_sync'
_TASKS=set()
BJ=timezone(timedelta(hours=8))


def message(reason):
    return {
        'not_authorized':'历史同步授权尚未配置，未发送上游请求。',
        'disabled':'历史同步已停止，需核查停止原因后重新授权。',
        'expired':'历史同步授权已到期，未自动续期。',
        'budget_exhausted':'本次或今日请求预算已用完；已完成的数据保留，未自动增加额度。',
        'usage_limit':'上游报告调用限额，已停止后续请求。',
        'insufficient_points':'上游积分不足，已停止后续请求。',
        'generation_changed':'授权已切换，请重新提交普通执行。',
        'generation_incomplete':'授权切换未完成，已停止执行，请运维核查。',
        'busy':'历史同步正在运行，请查看任务进度。',
        'request_outcome_unknown':'上次请求结果不确定，已停止自动重试，请核查执行记录。',
        'write_outcome_unresolved':'上次写入结果未完全匹配，已保留现场，不会重复写入。',
        'cancelled':'历史同步已取消，已保存的数据和取数结果保留。',
    }.get(reason,'历史同步已停止，请核查执行记录。')+' ['+reason+']'


class HistoryBatchService:
    def __init__(self,gate,workspace,collection,executions,historical_service,provider_factory,clock=None):
        self.gate,self.workspace=gate,workspace
        self.collection,self.executions=collection,executions
        self.historical_service,self.provider_factory=historical_service,provider_factory
        self.clock=clock or (lambda:datetime.now(BJ))

    async def submit(self,*,force=False,incremental=True):
        if force or type(incremental) is not bool:
            raise HTTPException(400,'历史同步不支持强制绕过并发或授权检查，请使用普通执行。')
        fd=None
        try:
            fd=os.open(self.gate.admission_path,os.O_RDWR|os.O_NOFOLLOW)
            _private_fd(fd)
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise TushareEgressStopped('busy') from None
            self.gate.assert_selected()
            policy,state=self.gate.status()
            plan=state['plan']
            now=self.clock().astimezone(BJ)
            # Pre-18:00 clicks target yesterday; no inferred trading day/finality.
            end=(now.date() if now.hour>=18 else now.date()-timedelta(days=1)).isoformat()
            expected='incremental_daily_qfq' if incremental else 'year_daily_qfq'
            checkpoint=self.workspace.status(plan['plan_sha256']) if plan is not None else None
            reuse=plan is not None and (not checkpoint['complete'] or (plan['end']==end and plan['mode']==expected))
            if reuse:
                if plan['mode']!=expected:
                    raise HTTPException(400,'已有未完成的历史计划，请使用相同同步模式继续，不能替换其范围。')
                if checkpoint['complete']:
                    # Repeated clicks are read-only reconciliation, not new requests.
                    adapter=HistoricalAdapter(self.collection,self.historical_service,self.provider_factory,None,None)
                    with self.workspace.locked():
                        _,_,saved=self.workspace._load(plan['plan_sha256'])
                        for item in plan['items']:
                            self.gate.authorize(plan['plan_sha256'],item,'reconcile')
                            if not await adapter.matches(item,saved[item['code']]['payload']):
                                raise HTTPException(400,'已完成计划的数据发生变化，已停止自动重复取数，请核查。')
            else:
                inventory=await Inventory(self.collection).collect(policy['codes'])
                plan=build_plan(**inventory,end=end,now=now,incremental=incremental,max_window_days=366,
                    quota=dict(daily_requests=policy['daily_requests'],rolling_minute_requests=policy['minute_requests'],
                               evidence_ref='explicit-local-policy'))
                blocked=[i for i in plan['items'] if i['status']=='blocked']
                if blocked:
                    preview=', '.join(i['code']+':'+i['reason'] for i in blocked[:8])
                    raise HTTPException(400,f'历史计划有 {len(blocked)} 只股票未通过检查，未请求上游：{preview}')
                self.gate._validate_plan(policy,plan)
                try:self.workspace.create(plan)
                except FileExistsError:
                    # Only a complete, structurally verified existing plan is reusable.
                    self.workspace.status(plan['plan_sha256'])
                self.gate.bind(plan)
            checkpoint=self.workspace.status(plan['plan_sha256'])
            if checkpoint['counts']['fetching']:
                raise BatchStopped('request_outcome_unknown')
            identity=plan['plan_sha256']
            # A budget check before enqueue; writes/reconcile may continue without
            # spending a new HTTP request when a frozen response already exists.
            with self.workspace.locked():
                _,_,states=self.workspace._load(identity)
                pending=next((i for i in plan['items'] if states[i['code']]['phase']=='pending'),None)
                has_saved=any(s['phase'] in ('fetched','writing') for s in states.values())
            if pending is not None and not has_saved:self.gate.authorize(identity,pending,'fetch')
            now=self.clock()
            record=dict(job_id=JOB_ID,job_name='历史数据同步（Tushare）',status='running',timestamp=now,
                scheduled_time=now,updated_at=now,is_manual=True,cancel_requested=False,
                plan_sha256=identity,job_kwargs={'incremental':incremental},
                total_items=len(plan['items']),processed_items=checkpoint['counts']['committed'],
                progress=int(100*checkpoint['counts']['committed']/len(plan['items'])),
                progress_message=f"同步授权范围内 {len(plan['items'])} 只股票；结束日期 {plan['end']}")
            inserted=await self.executions.insert_one(record)
            task=asyncio.create_task(self._run(fd,inserted.inserted_id,plan))
            _TASKS.add(task);task.add_done_callback(_TASKS.discard)
            fd=None  # background owns admission lock until it exits
            return True
        except FileNotFoundError:
            raise HTTPException(400,message('not_authorized')) from None
        except (TushareEgressStopped,BatchStopped,PlanError) as error:
            reason=error.reason if isinstance(error,TushareEgressStopped) else str(error)
            raise HTTPException(400,message(reason)) from None
        finally:
            if fd is not None:os.close(fd)

    async def _run(self,fd,execution_id,plan):
        identity=plan['plan_sha256']
        async def authorize(plan_hash,item,action):
            current=await self.executions.find_one({'_id':execution_id})
            if not current or current.get('cancel_requested'):raise BatchStopped('cancelled')
            return self.gate.authorize(plan_hash,item,action)
        async def job_factory(item):return self.gate.begin(item)
        adapter=HistoricalAdapter(self.collection,self.historical_service,self.provider_factory,job_factory,authorize)
        reason=None
        try:
            while True:
                result=await self.workspace.advance_async(identity,adapter,max_items=1)
                await self._progress(execution_id,result,'success' if result['complete'] else 'running',plan)
                if result['complete']:break
                await asyncio.sleep(0)
        except BaseException as error:
            reason=error.reason if isinstance(error,TushareEgressStopped) else str(error) if isinstance(error,BatchStopped) else 'interrupted'
            try:
                checkpoint=self.workspace.status(identity)
                result=dict(committed=checkpoint['counts']['committed'],total=len(plan['items']),complete=False)
                status='cancelled' if reason=='cancelled' else 'partial' if result['committed'] else 'failed'
                await self._progress(execution_id,result,status,plan,message(reason))
            except Exception:
                # Journal is authoritative; never retry network/DB side effects
                # just because the progress collection is unavailable.
                pass
            if not isinstance(error,Exception):raise
        finally:
            os.close(fd)

    async def _progress(self,execution_id,result,status,plan,error=None):
        count,total=result['committed'],result['total']
        update=dict(status=status,updated_at=self.clock(),processed_items=count,total_items=total,
            success_count=count,progress=int(100*count/total),
            progress_message=error or f"已完成 {count}/{total} 只股票（授权范围，结束日期 {plan['end']}）",
            error_message=error,result=dict(success=result['complete'],success_count=count,total_processed=total,
                plan_sha256=plan['plan_sha256'],end=plan['end']))
        if status!='running':update['finished_at']=self.clock()
        await self.executions.update_one({'_id':execution_id},{'$set':update})
        # Existing scheduler progress endpoint reads this cache first.
        from app.routers.scheduler import _update_scheduler_progress_cache
        _update_scheduler_progress_cache(JOB_ID,str(execution_id),status,update['progress_message'],
            progress=update['progress'],processed_items=count,total_items=total)


async def submit_history_batch(*,force=False,incremental=True):
    try:
        gate=selected_gate()
        gate.status()  # refuse BEFORE opening database/provider on unconfigured hosts
    except TushareEgressStopped as error:raise HTTPException(400,message(error.reason)) from None
    from app.core.database import get_mongo_db
    from app.services.historical_data_service import HistoricalDataService
    db=get_mongo_db()
    async def provider_factory():
        from tradingagents.dataflows.providers.china.tushare import TushareProvider
        provider=TushareProvider()
        if not await provider.connect():raise TushareEgressStopped('not_authorized')
        return provider
    service=HistoryBatchService(gate,Workspace(gate.directory/'runs'),db.stock_daily_quotes,
        db.scheduler_executions,HistoricalDataService(),provider_factory)
    return await service.submit(force=force,incremental=incremental)
