"""调度偏好持久化和手动任务收尾；与行情、模型配置集合隔离。"""
import asyncio
import inspect
from datetime import datetime, timedelta, timezone

from bson import ObjectId


def now_local():
    """沿用调度执行记录的北京时间存储约定。"""
    return datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)


async def save_enabled(scheduler, db, job_id, enabled):
    """保存显式开关选择；保存失败时恢复原调度状态。"""
    from tradingagents.utils.tushare_manual import JOB_IDS
    if enabled and job_id in JOB_IDS:
        raise ValueError("Tushare 首期仅允许股票详情页的受限手动历史同步，自动任务不可启用")
    job = scheduler.get_job(job_id)
    if job is None:
        raise ValueError(f"任务不存在: {job_id}")
    previous = job.next_run_time
    try:
        if enabled:
            scheduler.resume_job(job_id)
        else:
            scheduler.pause_job(job_id)
        await db.scheduler_preferences.update_one(
            {"_id": job_id},
            {"$set": {"enabled": enabled, "updated_at": now_local()}},
            upsert=True,
        )
    except Exception:
        scheduler.modify_job(job_id, next_run_time=previous)
        raise


async def restore_preferences(scheduler, db):
    """在调度器开始派发任务之前恢复已保存的开关。"""
    from tradingagents.utils.tushare_manual import enforce_manual_jobs
    await enforce_manual_jobs(scheduler, db)
    async for pref in db.scheduler_preferences.find({}):
        job_id = pref['_id']
        if scheduler.get_job(job_id) is None:
            continue
        if pref.get('enabled') is True:
            scheduler.resume_job(job_id)
        elif pref.get('enabled') is False:
            scheduler.pause_job(job_id)


def outcome(result=None, error=None, cancelled=False):
    """区分调用结束、业务失败和业务跳过，避免把跳过标记成同步成功。"""
    if cancelled:
        return 'cancelled', '任务已取消'
    if error is not None:
        return 'failed', str(error)[:500]
    if isinstance(result, dict):
        if result.get('cancelled'):
            return 'cancelled', result.get('message', '任务已取消')
        if result.get('skipped'):
            return 'skipped', result.get('message') or result.get('reason') or '本次任务已跳过'
        if result.get('success') is False:
            return 'failed', str(result.get('error') or result.get('message') or '任务执行失败')[:500]
        return 'success', result.get('message') or '任务执行完成'
    if result is False:
        return 'failed', '任务返回失败'
    if getattr(result, 'errors', None):
        return 'failed', str(result.errors[0])[:500]
    return 'success', '任务执行完成'


async def finalize_execution(db, job_id, execution_id, result=None, error=None, cancelled=False):
    """仅收尾本次仍未结束的记录，不覆盖工作器已写入的详细结果。"""
    if not execution_id:
        return
    status, message = outcome(result, error, cancelled)
    timestamp = now_local()
    update = {
        'status': status,
        'progress_message': message,
        'updated_at': timestamp,
        'finished_at': timestamp,
    }
    if status in {'success', 'skipped'}:
        update['progress'] = 100
    if status == 'skipped':
        update.update(skipped=True, processed_items=0, success_count=0)
    if status == 'failed':
        update['error_message'] = message
    await db.scheduler_executions.update_one(
        {'_id': ObjectId(execution_id), 'job_id': job_id, 'status': {'$in': ['running', 'cancelling']}},
        {'$set': update},
    )


async def run_manual(func, kwargs, finish):
    """等待实际执行并统一收尾，涵盖异常和协程取消。"""
    result = None
    error = None
    cancelled = False
    try:
        if inspect.iscoroutinefunction(func):
            result = await func(**kwargs)
        else:
            result = await asyncio.to_thread(func, **kwargs)
    except asyncio.CancelledError:
        cancelled = True
        raise
    except Exception as exc:
        error = exc
    finally:
        await finish(result=result, error=error, cancelled=cancelled)
