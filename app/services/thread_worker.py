"""
线程池任务处理器
使用 ThreadPoolExecutor 在 Backend 进程内处理队列任务
"""

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any
from datetime import datetime

from app.core.database import get_mongo_db, get_redis_client
from app.services.queue_service import QueueService
from app.services.task_analysis_service import TaskAnalysisService
from app.models.analysis import UnifiedAnalysisTask, AnalysisTaskType, AnalysisStatus
from tradingagents.utils.stock_utils import StockUtils, SecurityType

logger = logging.getLogger(__name__)


class ThreadWorker:
    """线程池任务处理器"""
    
    def __init__(self, max_workers: int = 3):
        """
        初始化线程池 Worker
        
        Args:
            max_workers: 最大并发线程数（默认 3）
        """
        self.max_workers = max_workers
        self.executor: Optional[ThreadPoolExecutor] = None
        self.running = False
        self.worker_id = f"thread-worker-{threading.get_ident()}"
        self.queue_service: Optional[QueueService] = None
        self.task_service: Optional[TaskAnalysisService] = None
        self._loop_task: Optional[asyncio.Task] = None
        self._gap_resolution_poll_interval_seconds = 2.0
        self._gap_resolution_last_poll_at = 0.0
        
        logger.info(f"🔧 ThreadWorker 初始化: max_workers={max_workers}")
    
    async def start(self):
        """启动线程池 Worker"""
        if self.running:
            logger.warning("ThreadWorker 已经在运行")
            return
        
        logger.info("=" * 60)
        logger.info("🚀 启动线程池 Worker...")
        logger.info(f"   最大并发数: {self.max_workers}")
        logger.info(f"   Worker ID: {self.worker_id}")
        logger.info("=" * 60)
        
        # 初始化服务
        logger.info("⏳ [ThreadWorker] 初始化 Redis 客户端代理")
        redis = get_redis_client()
        logger.info("✅ [ThreadWorker] Redis 客户端代理初始化完成")
        logger.info("⏳ [ThreadWorker] 初始化 QueueService")
        self.queue_service = QueueService(redis)
        logger.info("✅ [ThreadWorker] QueueService 初始化完成")
        logger.info("⏳ [ThreadWorker] 初始化 TaskAnalysisService")
        self.task_service = TaskAnalysisService()
        logger.info("✅ [ThreadWorker] TaskAnalysisService 初始化完成")
        
        # 创建线程池
        logger.info("⏳ [ThreadWorker] 创建线程池执行器")
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="analysis-worker"
        )
        logger.info("✅ [ThreadWorker] 线程池执行器创建完成")
        
        self.running = True
        
        # 启动队列监听循环
        logger.info("⏳ [ThreadWorker] 启动队列监听循环")
        self._loop_task = asyncio.create_task(self._queue_loop())
        
        logger.info("✅ 线程池 Worker 启动成功")
    
    async def stop(self):
        """停止线程池 Worker"""
        if not self.running:
            return
        
        logger.info("🛑 停止线程池 Worker...")
        self.running = False
        
        # 取消队列循环
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        
        # 关闭线程池
        if self.executor:
            logger.info("   等待线程池任务完成...")
            self.executor.shutdown(wait=True, cancel_futures=False)
            logger.info("   线程池已关闭")
        
        logger.info("✅ 线程池 Worker 已停止")
    
    async def _queue_loop(self):
        """队列监听循环"""
        logger.info("📡 队列监听循环启动")

        while self.running:
            try:
                now_ts = time.time()
                if now_ts - self._gap_resolution_last_poll_at >= self._gap_resolution_poll_interval_seconds:
                    await self._tick_gap_resolution_jobs()
                    self._gap_resolution_last_poll_at = now_ts

                # 从队列获取任务
                task_data = await self.queue_service.dequeue_task(self.worker_id)

                if not task_data:
                    # 队列为空，等待一会儿
                    await asyncio.sleep(1)
                    continue

                # 提交任务到线程池（不等待完成）
                task_id = task_data.get("id")
                logger.info(f"📊 收到任务: {task_id}, 提交到线程池...")

                # 🔥 直接在当前事件循环中处理任务（不使用线程池）
                # 因为任务本身是 I/O 密集型，asyncio 已经提供了并发能力
                asyncio.create_task(self._process_task_async(task_data))

            except asyncio.CancelledError:
                logger.info("队列循环被取消")
                break
            except Exception as e:
                logger.error(f"队列循环错误: {e}", exc_info=True)
                await asyncio.sleep(1)

        logger.info("📡 队列监听循环结束")

    async def _tick_gap_resolution_jobs(self):
        """轮询并推进 Agent 工坊缺口补齐异步 job。"""
        try:
            from core.tools.implementations.agent_builder.nanobot_agent_builder_tools import (
                process_agent_workshop_gap_resolution_jobs_once,
            )
        except ImportError:
            # 社区版无 Agent 工坊模块（manifest 剔除 app/models/agent_workshop 等），
            # 一次性提示后静默停用轮询，避免每 tick 刷 WARNING。
            if not getattr(type(self), "_gap_job_unavailable_logged", False):
                type(self)._gap_job_unavailable_logged = True
                logger.info(
                    "[GapJob] Agent 工坊模块不可用（社区版），缺口补齐轮询已停用"
                )
            return

        try:
            result = await process_agent_workshop_gap_resolution_jobs_once(
                limit=1,
                worker_id=self.worker_id,
            )
            status = str((result or {}).get("status") or "").strip().lower()
            processed = int((result or {}).get("processed") or 0)
            attempted = int((result or {}).get("attempted") or 0)
            claim_skipped = int((result or {}).get("claim_skipped") or 0)
            active_jobs = int((result or {}).get("active_jobs") or 0)

            if status != "ok":
                logger.debug("[GapJob][Tick] status=%s err=%s", status, result.get("message", ""))
                return

            if processed > 0 or attempted > 0:
                logger.info(
                    "[GapJob][Tick] worker_id=%s active_jobs=%s processed=%s attempted=%s claim_skipped=%s",
                    self.worker_id,
                    active_jobs,
                    processed,
                    attempted,
                    claim_skipped,
                )
        except Exception as exc:
            logger.warning("[GapJob][Tick] worker_id=%s err=%s", self.worker_id, exc)
    


    async def _process_task_async(self, task_data: Dict[str, Any]):
        """
        异步处理任务

        Args:
            task_data: 任务数据
        """
        task_id = task_data.get("id")
        raw_stock_code = task_data.get("symbol")
        stock_code = StockUtils.normalize_symbol(raw_stock_code) or raw_stock_code
        user_id = task_data.get("user")

        logger.info(f"🔧 开始处理任务: {task_id} - {stock_code}")

        success = False
        already_cancelled = False
        existing_task = None
        engine_type = "v2"

        try:
            # 解析任务参数
            # 🔥 修复：queue_service.get_task() 将 params 解析后存储到 parameters 字段
            parameters_dict = task_data.get("parameters", {})
            if not parameters_dict:
                # 兼容旧版本：如果 parameters 不存在，尝试从 params 读取
                parameters_dict = task_data.get("params", {})
                if isinstance(parameters_dict, str):
                    import json
                    parameters_dict = json.loads(parameters_dict)
            if not isinstance(parameters_dict, dict):
                parameters_dict = {}

            # 🔍 调试：打印参数内容
            logger.info(f"🔍 [DEBUG] 任务参数: {parameters_dict}")

            # 检查引擎类型（默认使用 v2 引擎）
            engine_type = parameters_dict.get("engine", "v2")

            if engine_type == "v2":
                existing_task = await self.task_service.get_task(task_id)
                if existing_task:
                    # 如果任务在排队期间已被取消，直接跳过执行，避免继续消耗资源
                    if existing_task.status == AnalysisStatus.CANCELLED:
                        logger.info(f"🚫 [ThreadWorker] 任务在执行前已被取消，跳过: {task_id}")
                        already_cancelled = True
                        try:
                            await self.queue_service.cancel_task(task_id)
                        except Exception as cancel_err:
                            logger.warning(f"⚠️ [ThreadWorker] 清理已取消任务队列状态失败: {task_id} - {cancel_err}")
                        return

                    from app.services.memory_state_manager import get_memory_state_manager, TaskStatus
                    from app.utils.timezone import now_tz

                    memory_manager = get_memory_state_manager()
                    memory_task = await memory_manager.get_task(task_id)
                    if not memory_task:
                        await memory_manager.create_task(
                            task_id=task_id,
                            user_id=user_id,
                            stock_code=stock_code,
                            parameters=parameters_dict,
                            stock_name=None,
                        )

                    existing_task.status = AnalysisStatus.PROCESSING
                    if not existing_task.started_at:
                        existing_task.started_at = now_tz()
                    existing_task.progress = max(existing_task.progress or 0, 1)
                    existing_task.current_step = "数据准备"
                    existing_task.message = "正在准备分析所需数据..."
                    await self.task_service._update_task(existing_task)

                    await memory_manager.update_task_status(
                        task_id=task_id,
                        status=TaskStatus.RUNNING,
                        progress=existing_task.progress,
                        message=existing_task.message,
                        current_step="data_preparation",
                        current_step_name="数据准备",
                        current_step_description=existing_task.message
                    )

            # 📡 JIT 数据同步：分析前主动拉取最新行情，确保不使用陈旧数据
            try:
                from app.worker.analysis_worker import jit_sync_stock_data
                jit_params = dict(parameters_dict)
                if existing_task and getattr(existing_task, "task_type", None) == AnalysisTaskType.ETF_ANALYSIS:
                    jit_params["is_etf"] = True
                    jit_params["task_type"] = AnalysisTaskType.ETF_ANALYSIS.value
                    jit_params["market_type"] = "ETF"
                jit_result = await jit_sync_stock_data(stock_code, jit_params)
                if not jit_result.is_valid:
                    logger.error(f"❌ [ThreadWorker] JIT 数据同步失败: {jit_result.message}")
                    raise ValueError(f"数据准备失败: {jit_result.message}")
                logger.info(f"✅ [ThreadWorker] JIT 数据同步完成: {jit_result.message}")
            except ValueError:
                raise
            except Exception as jit_err:
                logger.warning(f"⚠️ [ThreadWorker] JIT 数据同步异常（继续分析）: {jit_err}")

            logger.info(f"🔧 使用 {engine_type} 引擎执行任务: {task_id}")

            if engine_type == "v2":
                # 使用 v2.0 统一任务引擎
                if existing_task:
                    # 任务已存在，直接执行
                    logger.info(f"📋 任务已存在，直接执行: {task_id} (task_type={existing_task.task_type})")
                    executed_task = await self.task_service.execute_task(existing_task)
                else:
                    # 任务不存在，创建并执行
                    from bson import ObjectId
                    sec_type = StockUtils.identify_security_type(stock_code)
                    requested_market = parameters_dict.get("market_type")
                    task_type_hint = str(parameters_dict.get("task_type") or "").strip().lower()
                    is_etf = (
                        bool(parameters_dict.get("is_etf"))
                        or str(requested_market or "").strip().upper() == "ETF"
                        or task_type_hint == "etf_analysis"
                        or sec_type == SecurityType.FUND
                    )
                    if is_etf:
                        inferred_task_type = AnalysisTaskType.ETF_ANALYSIS
                        parameters_dict["market_type"] = "ETF"
                        parameters_dict["is_etf"] = True
                        parameters_dict["task_type"] = AnalysisTaskType.ETF_ANALYSIS.value
                        parameters_dict.setdefault("selected_analysts", ["etf", "market", "news"])
                        logger.info(f"📋 任务不存在，按 ETF 流程重建任务: {task_id}")
                    else:
                        inferred_task_type = AnalysisTaskType.STOCK_ANALYSIS
                        logger.info(f"📋 任务不存在，按股票流程重建任务: {task_id}")

                    executed_task = await self.task_service.create_and_execute_task(
                        user_id=ObjectId(user_id) if user_id else ObjectId(),
                        task_type=inferred_task_type,
                        task_params={
                            "symbol": stock_code,
                            "stock_code": stock_code,
                            **parameters_dict
                        },
                        engine_type="v2",
                        task_id=task_id  # 🔥 直接传递 task_id 参数
                    )

                final_task = executed_task or existing_task
                final_status = getattr(final_task, "status", None) if final_task is not None else None
                final_status_val = final_status.value if hasattr(final_status, "value") else str(final_status or "")
                if final_status_val == AnalysisStatus.CANCELLED.value:
                    already_cancelled = True
                    try:
                        await self.queue_service.cancel_task(task_id)
                    except Exception as cancel_err:
                        logger.warning(f"⚠️ [ThreadWorker] 同步已取消任务队列状态失败: {task_id} - {cancel_err}")
                    logger.info(f"🚫 [v2引擎] 任务执行期间已取消: {task_id}")
                    return

                logger.info(f"✅ [v2引擎] 任务完成: {task_id}")

            elif engine_type == "unified":
                # 使用统一分析服务
                from app.services.unified_analysis_service import UnifiedAnalysisService

                unified_service = UnifiedAnalysisService()
                await unified_service.analyze_stock(
                    stock_code=stock_code,
                    user_id=user_id,
                    task_id=task_id,
                    **parameters_dict
                )
                logger.info(f"✅ [unified引擎] 任务完成: {task_id}")

            else:
                # 旧版 legacy 引擎已停用 — 拒绝执行
                logger.error(
                    f"❌ [ThreadWorker] 拒绝执行 legacy 引擎任务: task_id={task_id}, engine={engine_type}"
                )
                raise RuntimeError(
                    "[已停用] thread_worker 拒绝 engine=%s 的任务，"
                    "请将队列参数 engine 改为 'v2'。"
                    "详见 docs/99-archive/deprecated-features/legacy-agents-deprecation.md" % engine_type
                )
                # pragma: no cover - 以下原代码保留但不执行，为 v4.0 删除做准备
                # 使用 legacy 引擎
                from app.services.simple_analysis_service import SimpleAnalysisService
                from app.models.analysis import SingleAnalysisRequest, AnalysisParameters

                # 🔥 从参数字典构造 AnalysisParameters 对象
                params = AnalysisParameters(**parameters_dict) if parameters_dict else AnalysisParameters()

                # 🔥 构造 SingleAnalysisRequest 对象
                request = SingleAnalysisRequest(
                    symbol=stock_code,
                    stock_code=stock_code,
                    parameters=params
                )

                simple_service = SimpleAnalysisService()
                await simple_service.execute_analysis_background(
                    task_id=task_id,
                    user_id=user_id,
                    request=request
                )
                logger.info(f"✅ [legacy引擎] 任务完成: {task_id}")

            success = True

        except Exception as e:
            logger.error(f"❌ 任务执行失败: {task_id} - {e}", exc_info=True)
            success = False
            if e.__class__.__name__ == "TaskCancelledException":
                already_cancelled = True
                try:
                    await self.queue_service.cancel_task(task_id)
                except Exception:
                    pass
            await self._mark_task_failed_fallback(
                task_id=task_id,
                error=e,
                existing_task=existing_task,
            )

        finally:
            # 确认任务完成（已取消的任务保持 cancelled，避免被 ack 覆写为 failed）
            if not already_cancelled:
                try:
                    await self.queue_service.ack_task(task_id, success)
                    logger.info(f"✅ 任务已确认: {task_id} (成功: {success})")
                except Exception as e:
                    logger.error(f"❌ 确认任务失败: {task_id} - {e}")

    async def _mark_task_failed_fallback(
        self,
        task_id: Optional[str],
        error: Exception,
        existing_task: Optional[UnifiedAnalysisTask] = None,
    ) -> None:
        """异常兜底：同步将 unified_analysis_tasks 与 memory_manager 置为 FAILED（或保留 CANCELLED），防止永久僵尸。"""
        if not task_id:
            return

        is_cancelled = error.__class__.__name__ == "TaskCancelledException"
        try:
            from app.utils.error_formatter import ErrorFormatter
            friendly_message = ErrorFormatter.format_user_message(str(error)) or str(error)
        except Exception:
            friendly_message = str(error) or "任务执行失败"

        try:
            from app.utils.timezone import now_tz
            completed_at = now_tz()
        except Exception:
            completed_at = datetime.utcnow()

        # 1) 同步更新 unified_analysis_tasks
        try:
            target_task = None
            if self.task_service is not None:
                try:
                    target_task = await self.task_service.get_task(task_id)
                except Exception:
                    target_task = existing_task
            if target_task is None:
                target_task = existing_task

            if target_task is not None and self.task_service is not None:
                current_status = target_task.status
                status_str = current_status.value if hasattr(current_status, "value") else str(current_status)
                if status_str not in ("completed", "cancelled", "failed"):
                    if is_cancelled:
                        target_task.status = AnalysisStatus.CANCELLED
                        target_task.message = "任务已取消"
                    else:
                        target_task.status = AnalysisStatus.FAILED
                        target_task.error_message = friendly_message
                        target_task.message = friendly_message
                    target_task.completed_at = completed_at
                    await self.task_service._update_task(target_task)
            else:
                db = get_mongo_db()
                target_status = AnalysisStatus.CANCELLED.value if is_cancelled else AnalysisStatus.FAILED.value
                update_fields: Dict[str, Any] = {
                    "status": target_status,
                    "completed_at": completed_at,
                    "updated_at": completed_at,
                    "message": "任务已取消" if is_cancelled else friendly_message,
                }
                if not is_cancelled:
                    update_fields["error_message"] = friendly_message
                await db.unified_analysis_tasks.update_one(
                    {
                        "task_id": task_id,
                        "status": {"$nin": ["completed", "cancelled", "failed"]},
                    },
                    {"$set": update_fields},
                )
        except Exception as db_err:
            logger.error(f"❌ [ThreadWorker] 回写 unified_analysis_tasks 失败状态异常: {task_id} - {db_err}")

        # 2) 同步更新 memory_manager
        try:
            from app.services.memory_state_manager import get_memory_state_manager, TaskStatus
            memory_manager = get_memory_state_manager()
            memory_task = await memory_manager.get_task(task_id)
            mem_status = None
            if memory_task is not None:
                raw_st = getattr(memory_task, "status", None) if not isinstance(memory_task, dict) else memory_task.get("status")
                mem_status = raw_st.value if hasattr(raw_st, "value") else str(raw_st or "")
            if mem_status not in ("completed", "cancelled", "failed"):
                if is_cancelled:
                    await memory_manager.update_task_status(
                        task_id=task_id,
                        status=TaskStatus.CANCELLED,
                        message="任务已取消",
                    )
                else:
                    await memory_manager.update_task_status(
                        task_id=task_id,
                        status=TaskStatus.FAILED,
                        message=friendly_message,
                        error_message=friendly_message,
                    )
        except Exception as mem_err:
            logger.error(f"❌ [ThreadWorker] 回写 memory_manager 失败状态异常: {task_id} - {mem_err}")


# 全局 ThreadWorker 实例
_thread_worker: Optional[ThreadWorker] = None


async def start_thread_worker(max_workers: int = 3):
    """启动全局线程池 Worker"""
    global _thread_worker

    if _thread_worker is not None:
        logger.warning("ThreadWorker 已经启动")
        return

    _thread_worker = ThreadWorker(max_workers=max_workers)
    await _thread_worker.start()


async def stop_thread_worker():
    """停止全局线程池 Worker"""
    global _thread_worker

    if _thread_worker is None:
        return

    await _thread_worker.stop()
    _thread_worker = None


def get_thread_worker() -> Optional[ThreadWorker]:
    """获取全局线程池 Worker 实例"""
    return _thread_worker

