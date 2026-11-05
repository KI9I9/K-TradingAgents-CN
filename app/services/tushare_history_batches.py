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
from tradingagents.utils.tushare_egress import TushareEgressStopped, _private_fd, history_transport_session
from deploy.history_batch_journal import Workspace, BatchStopped
from deploy.history_batch_adapters import Inventory, HistoricalAdapter
from deploy.history_sync_plan import build_plan, PlanError

JOB_ID='tushare_historical_sync'
_TASKS=set()
BJ=timezone(timedelta(hours=8))


def message(reason, http_status=None, api_code=None):
    messages = {
        'not_authorized':'历史同步授权尚未配置，未发送上游请求。',
        'disabled':'历史同步运行许可已关闭（不是登录权限）；请联系运维核查停止原因并恢复。',
        'expired':'历史同步授权已到期，未自动续期。',
        'budget_exhausted':'本次或今日请求预算已用完；已完成的数据保留，未自动增加额度。',
        'timeout':'上游请求连续超时，已保留完成数据及断点；需核验后从未完成股票继续。',
        'usage_limit':'上游报告调用限额，已停止后续请求。',
        'insufficient_points':'上游积分不足，已停止后续请求。',
        'service_unavailable':'上游服务返回服务器错误，已停止后续请求；未继续消耗额度。',
        'api_rejected':'上游拒绝了本次 API 请求，已停止后续请求。',
        'unclassified':'上游返回未分类 API 错误，无法据此判断权限或额度；已停止后续请求。',
        'generation_changed':'授权已切换，请重新提交普通执行。',
        'generation_incomplete':'授权切换未完成，已停止执行，请运维核查。',
        'busy':'历史同步正在运行，请查看任务进度。',
        'request_outcome_unknown':'上次请求结果不确定，已停止自动重试，请核查执行记录。',
        'write_outcome_unresolved':'上次写入结果未完全匹配，已保留现场，不会重复写入。',
        'cancelled':'历史同步已取消，已保存的数据和取数结果保留。',
    }
    details = []
    if type(api_code) is int and -1000000 <= api_code <= 1000000:
        details.append('Tushare API 错误码 '+str(api_code))
    if type(http_status) is int and 100 <= http_status <= 599:
        details.append('上游 HTTP 状态码 '+str(http_status))
    detail = ('（'+'；'.join(details)+'）') if details else ''
    return messages.get(reason,'历史同步已停止，请核查执行记录。') + detail + ' ['+reason+']'


class HistoryBatchService:
    def __init__(self,gate,workspace,collection,executions,historical_service,provider_factory,clock=None):
        self.gate,self.workspace=gate,workspace
        self.collection,self.executions=collection,executions
        self.historical_service,self.provider_factory=historical_service,provider_factory
        self.clock=clock or (lambda:datetime.now(BJ))

    def record_now(self):
        # Scheduler records use naive Beijing time throughout the existing API,
        # activity queries and zombie checker. Keep policy/plan clocks aware.
        return self.clock().astimezone(BJ).replace(tzinfo=None)

    async def submit(self,*,force=False,incremental=True,scheduled=False):
        if force or type(incremental) is not bool:
            raise HTTPException(400,'历史同步不支持强制绕过并发或授权检查，请使用普通执行。')
        if type(scheduled) is not bool or (scheduled and incremental is not True):
            raise HTTPException(400,'定时历史同步仅支持非复权增量执行。')
        self.scheduled=scheduled
        reserved=False
        fd=None
        try:
            fd=os.open(self.gate.admission_path,os.O_RDWR|os.O_NOFOLLOW)
            _private_fd(fd)
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise TushareEgressStopped('busy') from None
            self.gate.assert_selected()
            policy,state=self.gate.status()
            if scheduled:
                from app.services.tushare_history_schedule import load_schedule, trade_day_reason, reserve_day
                load_schedule(self.gate)
                now=self.clock().astimezone(BJ)
                reason=await trade_day_reason(now)
                if reason is None:
                    reserved=reserve_day(self.gate,now)
                    if not reserved:reason='already_scheduled_today'
                if reason:
                    recorded=self.record_now()
                    await self.executions.insert_one(dict(job_id=JOB_ID,job_name='历史数据同步（Tushare）',
                        status='skipped',timestamp=recorded,scheduled_time=recorded,updated_at=recorded,finished_at=recorded,
                        is_manual=False,reason=reason,progress=100,processed_items=0,success_count=0,
                        progress_message='定时历史同步未取数：'+reason))
                    return dict(_execution_recorded=True, success=True, skipped=True, reason=reason)
            if len(policy['codes'])>50:
                now=self.record_now()
                inserted=await self.executions.insert_one(dict(job_id=JOB_ID,job_name='历史数据同步（Tushare）',
                    status='running',timestamp=now,scheduled_time=now,updated_at=now,is_manual=not scheduled,
                    cancel_requested=False,job_kwargs={'incremental':incremental},total_items=len(policy['codes']),
                    processed_items=0,progress=0,progress_message='正在核验全市场股票池与历史断点'))
                if scheduled:
                    owned,fd=fd,None
                    await self._prepare_and_run(owned,inserted.inserted_id,policy,state,incremental)
                    return await self._scheduled_result(inserted.inserted_id)
                task=asyncio.create_task(self._prepare_and_run(fd,inserted.inserted_id,policy,state,incremental))
                _TASKS.add(task);task.add_done_callback(_TASKS.discard)
                fd=None
                return True
            plan,checkpoint=await self._prepare(policy,state,incremental)
            now=self.record_now()
            record=dict(job_id=JOB_ID,job_name='历史数据同步（Tushare）',status='running',timestamp=now,
                scheduled_time=now,updated_at=now,is_manual=not scheduled,cancel_requested=False,
                plan_sha256=plan['plan_sha256'],job_kwargs={'incremental':incremental},
                total_items=len(plan['items']),processed_items=checkpoint['counts']['committed'],
                progress=int(100*checkpoint['counts']['committed']/len(plan['items'])),
                progress_message=f"同步授权范围内 {len(plan['items'])} 只股票；结束日期 {plan['end']}")
            inserted=await self.executions.insert_one(record)
            if scheduled:
                owned,fd=fd,None
                await self._run(owned,inserted.inserted_id,plan)
                return await self._scheduled_result(inserted.inserted_id)
            task=asyncio.create_task(self._run(fd,inserted.inserted_id,plan))
            _TASKS.add(task);task.add_done_callback(_TASKS.discard)
            fd=None  # background owns admission lock until it exits
            return True
        except FileNotFoundError:
            if reserved:self.gate.stop()
            raise HTTPException(400,message('not_authorized')) from None
        except (TushareEgressStopped,BatchStopped,PlanError) as error:
            if reserved:self.gate.stop()
            reason=error.reason if isinstance(error,TushareEgressStopped) else str(error)
            raise HTTPException(400,message(reason,getattr(error,'http_status',None),getattr(error,'api_code',None))) from None
        except BaseException:
            if reserved:self.gate.stop()
            raise
        finally:
            if fd is not None:os.close(fd)

    async def _scheduled_result(self, execution_id):
        """Let the scheduler retain the batch's one detailed terminal record."""
        row=await self.executions.find_one({'_id':execution_id})
        if not row or row.get('status') not in ('success','partial','failed','cancelled'):
            raise RuntimeError('scheduled_execution_result_unverified')
        return dict(_execution_recorded=True, execution_id=str(execution_id),
                    success=row['status']=='success', status=row['status'])

    async def _prepare(self,policy,state,incremental):
        plan=state['plan']
        now=self.clock().astimezone(BJ)
        # Pre-18:00 clicks target yesterday; no inferred trading day/finality.
        end=(now.date() if now.hour>=18 else now.date()-timedelta(days=1)).isoformat()
        adjustment = 'none' if policy['schema'] == 2 else 'qfq'
        self.price_adjustment = adjustment
        expected=('incremental_daily_' if incremental else 'year_daily_') + adjustment
        checkpoint=self.workspace.status(plan['plan_sha256']) if plan is not None else None
        reuse=plan is not None and (not checkpoint['complete'] or (plan['end']==end and plan['mode']==expected))
        if reuse:
            if plan['mode']!=expected:
                raise HTTPException(400,'已有未完成的历史计划，请使用相同同步模式继续，不能替换其范围。')
            if checkpoint['complete']:
                # Repeated clicks are read-only reconciliation, not new requests.
                adapter=HistoricalAdapter(self.collection,self.historical_service,self.provider_factory,None,None,price_adjustment=adjustment)
                with self.workspace.locked():
                    _,_,saved=self.workspace._load(plan['plan_sha256'])
                    for item in plan['items']:
                        if item['status']=='blocked':
                            continue
                        self.gate.authorize(plan['plan_sha256'],item,'reconcile')
                        if not await adapter.matches(item,saved[item['code']]['payload']):
                            raise HTTPException(400,'已完成计划的数据发生变化，已停止自动重复取数，请核查。')
        else:
            inventory=await Inventory(self.collection,adjustment).collect(policy['codes'])
            plan=build_plan(**inventory,end=end,now=now,incremental=incremental,max_window_days=366,price_adjustment=adjustment,
                bootstrap_start=max(policy['min_date'],(now.date()-timedelta(days=365)).isoformat()) if incremental else None,
                quota=dict(daily_requests=policy['daily_requests'],rolling_minute_requests=policy['minute_requests'],
                           evidence_ref='explicit-local-policy'))
            blocked=[i for i in plan['items'] if i['status']=='blocked']
            if blocked and getattr(self,'scheduled',False):
                raise PlanError('scheduled_inventory_blocked')
            if blocked and len(blocked)==len(plan['items']):
                preview=', '.join(i['code']+':'+i['reason'] for i in blocked[:8])
                raise HTTPException(400,f'历史计划有 {len(blocked)} 只股票未通过检查，未请求上游：{preview}')
            self.gate._validate_plan(policy,plan)
            try:self.workspace.create(plan)
            except FileExistsError:
                # Only a complete, structurally verified existing plan is reusable.
                self.workspace.status(plan['plan_sha256'])
            self.gate.bind(plan)
        checkpoint=self.workspace.status(plan['plan_sha256'])
        if getattr(self,'scheduled',False) and not checkpoint['complete'] and reuse:
            # A prior interrupted/manual run needs explicit review, rather than
            # silently consuming today's scheduled slot to resume an old end date.
            raise BatchStopped('scheduled_previous_plan_incomplete')
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
        return plan,checkpoint

    async def _prepare_and_run(self,fd,execution_id,policy,state,incremental):
        try:
            plan,checkpoint=await self._prepare(policy,state,incremental)
            current=await self.executions.find_one({'_id':execution_id})
            if not current or current.get('cancel_requested'):
                raise BatchStopped('cancelled')
            await self.executions.update_one({'_id':execution_id},{'$set':{'plan_sha256':plan['plan_sha256']}})
        except BaseException as error:
            os.close(fd)
            if getattr(self,'scheduled',False):self.gate.stop()
            detail=error.detail if isinstance(error,HTTPException) else message(
                error.reason if isinstance(error,TushareEgressStopped) else str(error) if isinstance(error,BatchStopped) else 'interrupted',
                getattr(error,'http_status',None), getattr(error,'api_code',None))
            await self.executions.update_one({'_id':execution_id},{'$set':dict(status='failed',finished_at=self.record_now(),error_message=detail,progress_message=detail)})
            if not isinstance(error,Exception):raise
            return
        await self._run(fd,execution_id,plan)

    async def _run(self,fd,execution_id,plan):
        identity=plan['plan_sha256']
        async def authorize(plan_hash,item,action):
            current=await self.executions.find_one({'_id':execution_id})
            if not current or current.get('cancel_requested'):raise BatchStopped('cancelled')
            return self.gate.authorize(plan_hash,item,action)
        async def job_factory(item):return self.gate.begin(item)
        adapter=HistoricalAdapter(self.collection,self.historical_service,self.provider_factory,job_factory,authorize,
            price_adjustment='none' if plan['mode'].endswith('_none') else 'qfq')
        reason=None
        try:
            async def progress(result):
                await self._progress(execution_id,result,'running',plan)
                if getattr(self,'scheduled',False) and (result.get('skipped') or any(o['reason']!='no_daily_rows' for o in result.get('outcomes',[]))):
                    self.gate.stop()
                    raise BatchStopped('write_outcome_unresolved')
            with history_transport_session():
                while True:
                    result=await self.workspace.advance_async(identity,adapter,max_items=10000,on_progress=progress)
                    if getattr(self,'scheduled',False) and (result.get('skipped') or any(o['reason']!='no_daily_rows' for o in result.get('outcomes',[]))):
                        self.gate.stop()
                    await self._progress(execution_id,result,('partial' if (result.get('skipped') or any(o['reason']!='no_daily_rows' for o in result.get('outcomes',[]))) else 'success') if result['complete'] else 'running',plan)
                    if result['complete']:break
                    await asyncio.sleep(0)
        except BaseException as error:
            if getattr(self,'scheduled',False):self.gate.stop()
            reason=error.reason if isinstance(error,TushareEgressStopped) else str(error) if isinstance(error,BatchStopped) else 'interrupted'
            http_status=getattr(error,'http_status',None)
            try:
                checkpoint=self.workspace.status(identity)
                result=dict(committed=checkpoint['counts']['committed'],total=len(plan['items']),complete=False,
                            outcomes=checkpoint['outcomes'])
                status='cancelled' if reason=='cancelled' else 'partial' if result['committed'] else 'failed'
                await self._progress(execution_id,result,status,plan,message(
                    reason, http_status, getattr(error,'api_code',None)))
            except Exception:
                # Journal is authoritative; never retry network/DB side effects
                # just because the progress collection is unavailable.
                pass
            if not isinstance(error,Exception):raise
        finally:
            os.close(fd)

    async def _progress(self,execution_id,result,status,plan,error=None):
        count,total=result['committed'],result['total']
        skipped=[dict(code=i['code'],reason=i['reason']) for i in plan['items'] if i['status']=='blocked']
        finished=count+len(skipped)
        outcomes=result.get('outcomes',[])
        skipped+=outcomes
        count-=len(outcomes)
        update=dict(status=status,updated_at=self.record_now(),processed_items=count,total_items=total,
            success_count=count,skipped_count=len(skipped),skipped_items=skipped,progress=int(100*finished/total),
            price_adjustment='none' if plan['mode'].endswith('_none') else 'qfq',
            progress_message=error or ('非复权日线：' if plan['mode'].endswith('_none') else '前复权日线：') + f"已同步 {count}/{total} 只，跳过 {len(skipped)} 只（可查看跳过名单，结束日期 {plan['end']}）",
            error_message=error,result=dict(success=result['complete'] and all(i['reason']=='no_daily_rows' for i in skipped),complete=result['complete'],skipped_items=skipped,success_count=count,total_processed=finished,
                plan_sha256=plan['plan_sha256'],end=plan['end']))
        if status!='running':update['finished_at']=self.record_now()
        await self.executions.update_one({'_id':execution_id},{'$set':update})
        # Existing scheduler progress endpoint reads this cache first.
        from app.routers.scheduler import _update_scheduler_progress_cache
        _update_scheduler_progress_cache(JOB_ID,str(execution_id),status,update['progress_message'],
            progress=update['progress'],processed_items=count,total_items=total)


async def submit_history_batch(*,force=False,incremental=True,scheduled=False):
    try:
        gate=selected_gate()
        policy,_ = gate.status()  # refuse BEFORE opening database/provider on unconfigured hosts
    except TushareEgressStopped as error:raise HTTPException(400,message(
        error.reason, getattr(error,'http_status',None), getattr(error,'api_code',None))) from None
    from app.core.database import get_mongo_db
    from app.services.historical_data_service import HistoricalDataService
    db=get_mongo_db()
    async def provider_factory():
        from tradingagents.dataflows.providers.china.tushare import TushareProvider
        provider=TushareProvider()
        if not await provider.connect():raise TushareEgressStopped('not_authorized')
        return provider
    raw = policy['schema'] == 2
    collection = db.stock_daily_quotes_unadjusted if raw else db.stock_daily_quotes
    service=HistoryBatchService(gate,Workspace(gate.directory/'runs'),collection,
        db.scheduler_executions,HistoricalDataService(unadjusted_stock=raw),provider_factory)
    return await service.submit(force=force,incremental=incremental,scheduled=scheduled)
