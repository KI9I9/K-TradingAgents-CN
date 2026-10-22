"""Authenticated manual ingress and bounded background orchestration only."""
import asyncio
from datetime import datetime
from fastapi import HTTPException
from tradingagents.utils.tushare_egress import TushareEgressStopped
from tradingagents.utils.tushare_manual import DailyGate

_TASKS = set()  # Strong references until completion; no detached/restarted jobs.
MESSAGES = {
    'invalid_request': 'Tushare 仅开放 1～5 只明确指定的 A 股、仅历史日线、最近 1～30 天；请取消实时、财务及基础数据。',
    'budget_exhausted': 'Tushare 今日请求额度不足（10 次/日，每只股票需 2 次），请勿反复提交。',
    'busy': '已有一个 Tushare 手动任务运行中；若服务曾意外中断，需要人工核查，不会自动重跑。',
    'not_authorized': 'Tushare 手动策略尚未开放。',
    'disabled': 'Tushare 请求已安全停止，需要人工核查。',
    'expired': 'Tushare 出口授权已到期或时钟异常，不会自动续期。',
    'usage_limit': '上游返回调用限额，已停止后续请求；不要重复点击。',
    'insufficient_points': '上游返回积分不足，已停止后续请求。',
    'response_invalid': '上游数据为空或不完整，已停止，不会将未复权数据作为成功结果。',
}


def safe_message(error):
    reason = error.reason if isinstance(error, TushareEgressStopped) else 'interrupted'
    return MESSAGES.get(reason, 'Tushare 同步未完成，已停止后续请求，需要人工核查。') + ' ['+reason+']'


async def sync_scope(job):
    # New service object, unchanged local initialization and persistence logic.
    from app.worker.tushare_sync_service import create_tushare_sync_service
    service = await create_tushare_sync_service()
    approved = job.approved
    dates = {key: datetime.strptime(approved[key], '%Y%m%d').strftime('%Y-%m-%d')
             for key in ('start', 'end')}
    total = 0
    for code in approved['codes']:
        job.assert_ready()
        stats = await service.sync_historical_data(
            symbols=[code[:6]], start_date=dates['start'], end_date=dates['end'],
            incremental=False, all_history=False, period='daily', job_id=None,
            auto_retry=False, max_auto_retries=0)
        job.assert_ready()
        if (stats.get('success_count') != 1 or stats.get('total_processed') != 1
                or stats.get('errors') or stats.get('error_count') or stats.get('skipped')
                or not 1 <= stats.get('total_records', 0) <= 30):
            raise TushareEgressStopped('response_invalid')
        total += stats['total_records']
    return total


async def execute(job, task_id, task_type, payload):
    from app.routers.stock_sync import _update_stock_sync_task, _sync_latest_to_market_quotes, _utc_now_iso
    with job.context():
        try:
            await _update_stock_sync_task(task_id, dict(status='running', started_at=_utc_now_iso(),
                message='Tushare 手动历史同步中；最多 4 次请求/分钟，大批次会等待配额窗口', error=None))
            total = await sync_scope(job)
            symbols = [c[:6] for c in job.approved['codes']]
            if task_type == 'single':
                await _sync_latest_to_market_quotes(symbols[0])
                result = dict(symbol=symbols[0], overall_success=True, historical_sync=dict(
                    success=True, records=total, message=f'同步了 {total} 条历史记录'),
                    realtime_sync=None, financial_sync=None, basic_sync=None)
            else:
                result = dict(symbols=symbols, total=len(symbols), total_success=len(symbols),
                    historical_sync=dict(success_count=len(symbols), error_count=0, total_records=total,
                                         message=f'成功同步 {len(symbols)} 只股票，共 {total} 条记录'),
                    financial_sync=None, basic_sync=None)
            job.finish()
            await _update_stock_sync_task(task_id, dict(status='success', result=result,
                message=f'Tushare 手动历史同步完成，共 {total} 条记录', finished_at=_utc_now_iso(), error=None))
        except BaseException as error:
            job.fail(error.reason if isinstance(error, TushareEgressStopped) else 'interrupted')
            await _update_stock_sync_task(task_id, dict(status='failed', finished_at=_utc_now_iso(),
                message=safe_message(error), error=safe_message(error)))
            if not isinstance(error, Exception):
                raise


async def submit(request, task_type, current_user):
    from app.routers.stock_sync import _request_to_dict, _create_stock_sync_task
    from app.core.response import ok
    payload = _request_to_dict(request)
    try:
        job = DailyGate().begin(payload)
    except TushareEgressStopped as error:
        code = 422 if error.reason == 'invalid_request' else 429 if error.reason == 'budget_exhausted' else 409
        raise HTTPException(status_code=code, detail=safe_message(error)) from None
    try:
        codes = [code[:6] for code in job.approved['codes']]
        payload['symbol' if task_type == 'single' else 'symbols'] = codes[0] if task_type == 'single' else codes
        record = await _create_stock_sync_task(task_type, payload, current_user)
        task = asyncio.create_task(execute(job, record['task_id'], task_type, payload))
        _TASKS.add(task)
        task.add_done_callback(_TASKS.discard)
        return ok(data=record, message='Tushare 受限手动历史同步任务已提交')
    except BaseException:
        job.fail()
        raise
