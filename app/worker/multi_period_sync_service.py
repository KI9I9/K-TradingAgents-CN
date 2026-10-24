#!/usr/bin/env python3
"""
多周期历史数据同步服务
支持日线、周线、月线数据的统一同步
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from app.services.historical_data_service import get_historical_data_service
from app.worker.tushare_sync_service import TushareSyncService
from app.worker.akshare_sync_service import AKShareSyncService
from app.worker.baostock_sync_service import BaoStockSyncService

logger = logging.getLogger(__name__)

# 单批次内个股请求的并发度上限。可用 HISTORICAL_SYNC_CONCURRENCY_<SOURCE> 覆盖。
# 取值依据：每只个股请求都会起一个独立解释器子进程重新 import pandas/akshare，
# 属于 CPU 密集型冷启动；实测在 4 vCPU 上并发 8 反而慢于并发 4
# （24 只样本：4 并发 112.2s vs 8 并发 136.2s），故默认对齐核数。
_DEFAULT_SYNC_CONCURRENCY = {"akshare": 4, "baostock": 4, "tushare": 2}
_MAX_SYNC_CONCURRENCY = 32

# 同时推进的批次数。批内为网络 IO 等待，多批并行近似线性提速。
_DEFAULT_BATCH_CONCURRENCY = {"akshare": 3, "baostock": 2, "tushare": 1}


def _sync_concurrency(data_source: str) -> int:
    """返回指定数据源的单批并发度；非法配置回退到默认值。"""
    raw = os.getenv(f"HISTORICAL_SYNC_CONCURRENCY_{data_source.upper()}")
    if raw is not None:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            logger.warning(f"⚠️ 并发度配置非法({data_source}={raw!r})，使用默认值")
        else:
            if 1 <= value <= _MAX_SYNC_CONCURRENCY:
                return value
            logger.warning(
                f"⚠️ 并发度超出范围({data_source}={value})，"
                f"有效区间 1-{_MAX_SYNC_CONCURRENCY}，使用默认值"
            )
    return _DEFAULT_SYNC_CONCURRENCY.get(data_source, 4)


def _batch_concurrency(data_source: str) -> int:
    """返回同时推进的批次数；非法配置回退到默认值。

    批内是网络 IO 等待，CPU 空闲，因此多批并行可近似线性提速。
    默认值保守，避免倍数堆叠后被上游判定为异常流量。
    """
    raw = os.getenv(f"HISTORICAL_SYNC_BATCH_CONCURRENCY_{data_source.upper()}")
    if raw is not None:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            logger.warning(f"⚠️ 批并发配置非法({data_source}={raw!r})，使用默认值")
        else:
            if 1 <= value <= 8:
                return value
            logger.warning(f"⚠️ 批并发超出范围({data_source}={value})，有效区间 1-8，使用默认值")
    return _DEFAULT_BATCH_CONCURRENCY.get(data_source, 3)


@dataclass
class MultiPeriodSyncStats:
    """多周期同步统计"""
    total_symbols: int = 0
    daily_records: int = 0
    weekly_records: int = 0
    monthly_records: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class MultiPeriodSyncService:
    """多周期历史数据同步服务"""
    
    def __init__(self):
        self.historical_service = None
        self.tushare_service = None
        self.akshare_service = None
        self.baostock_service = None
        
    async def initialize(self):
        """初始化服务"""
        try:
            self.historical_service = await get_historical_data_service()
            
            # 初始化各数据源服务
            self.tushare_service = TushareSyncService()
            await self.tushare_service.initialize()
            
            self.akshare_service = AKShareSyncService()
            await self.akshare_service.initialize()
            
            self.baostock_service = BaoStockSyncService()
            await self.baostock_service.initialize()
            
            logger.info("✅ 多周期同步服务初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 多周期同步服务初始化失败: {e}")
            raise
    
    async def sync_multi_period_data(
        self,
        symbols: List[str] = None,
        periods: List[str] = None,
        data_sources: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        all_history: bool = False
    ) -> MultiPeriodSyncStats:
        """
        同步多周期历史数据

        Args:
            symbols: 股票代码列表，None表示所有股票
            periods: 周期列表 (daily/weekly/monthly)
            data_sources: 数据源列表 (tushare/akshare/baostock)
            start_date: 开始日期
            end_date: 结束日期
            all_history: 是否同步所有历史数据（忽略时间范围）
        """
        if self.historical_service is None:
            await self.initialize()
        
        # 默认参数
        if periods is None:
            periods = ["daily", "weekly", "monthly"]
        if data_sources is None:
            data_sources = ["tushare", "akshare", "baostock"]
        if symbols is None:
            symbols = await self._get_all_symbols()

        # 处理all_history参数
        if all_history:
            start_date, end_date = await self._get_full_history_date_range()
            logger.info(f"🔄 启用全历史数据同步模式: {start_date} 到 {end_date}")

        stats = MultiPeriodSyncStats()
        stats.total_symbols = len(symbols)

        logger.info(f"🔄 开始多周期数据同步: {len(symbols)}只股票, "
                   f"周期{periods}, 数据源{data_sources}, "
                   f"时间范围: {start_date or '默认'} 到 {end_date or '今天'}")
        
        try:
            # 🔥 优化：不同数据源可以并行执行（因为它们使用不同的API，互不影响）
            # 同一种数据源的不同周期仍然串行执行（避免API限流）
            
            # 为每个数据源创建任务（每个数据源内部串行处理所有周期）
            tasks = []
            task_info = []  # 保存任务信息，用于结果处理
            
            for data_source in data_sources:
                # 为每个数据源创建一个任务，该任务会串行处理该数据源的所有周期
                task = self._sync_data_source_all_periods(
                    data_source, periods, symbols, start_date, end_date
                )
                tasks.append(task)
                task_info.append(data_source)
            
            # 并行执行所有数据源的同步任务
            if tasks:
                logger.info(f"🚀 开始并行同步 {len(tasks)} 个数据源的历史数据: {task_info}")
                
                # 使用 asyncio.gather 并行执行
                period_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 处理结果
                for i, (data_source, result) in enumerate(zip(task_info, period_results)):
                    if isinstance(result, Exception):
                        logger.error(f"❌ {data_source} 历史数据同步失败: {result}", exc_info=True)
                        stats.error_count += len(symbols)  # 假设全部失败
                    else:
                        # result 是一个字典，包含各周期的统计信息
                        for period in periods:
                            period_stats = result.get(period, {})
                            
                            # 累计统计
                            if period == "daily":
                                stats.daily_records += period_stats.get("records", 0)
                            elif period == "weekly":
                                stats.weekly_records += period_stats.get("records", 0)
                            elif period == "monthly":
                                stats.monthly_records += period_stats.get("records", 0)
                            
                            stats.success_count += period_stats.get("success", 0)
                            stats.error_count += period_stats.get("errors", 0)
                            
                            # 进度日志
                            logger.info(f"📊 {data_source}-{period}同步完成: "
                                       f"{period_stats.get('records', 0)}条记录")
            else:
                logger.warning("⚠️ 没有可用的数据源进行历史数据同步")
            
            logger.info(f"✅ 多周期数据同步完成: "
                       f"日线{stats.daily_records}, 周线{stats.weekly_records}, "
                       f"月线{stats.monthly_records}条记录")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ 多周期数据同步失败: {e}")
            stats.errors.append(str(e))
            return stats
    
    async def _sync_data_source_all_periods(
        self,
        data_source: str,
        periods: List[str],
        symbols: List[str],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        同步某个数据源的所有周期数据（串行执行，避免API限流）
        
        Returns:
            字典，key为周期名称，value为该周期的统计信息
        """
        results = {}
        
        # 同一种数据源的不同周期串行执行（避免API限流）
        for period in periods:
            try:
                period_stats = await self._sync_period_data(
                    data_source, period, symbols, start_date, end_date
                )
                results[period] = period_stats
            except Exception as e:
                logger.error(f"❌ {data_source}-{period}同步失败: {e}")
                results[period] = {"records": 0, "success": 0, "errors": len(symbols)}
        
        return results
    
    async def _sync_period_data(
        self,
        data_source: str,
        period: str,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Any]:
        """同步特定周期的数据"""
        stats = {"records": 0, "success": 0, "errors": 0}
        
        try:
            logger.info(f"📈 开始同步{data_source}-{period}数据: {len(symbols)}只股票")
            
            # 选择对应的服务
            if data_source == "tushare":
                service = self.tushare_service
            elif data_source == "akshare":
                service = self.akshare_service
            elif data_source == "baostock":
                service = self.baostock_service
            else:
                logger.error(f"❌ 不支持的数据源: {data_source}")
                return stats
            
            # 分批 + 批间并发：批内是网络 IO 等待，多批并行近似线性提速
            batch_size = 50
            batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
            batch_limit = _batch_concurrency(data_source)
            semaphore = asyncio.Semaphore(batch_limit)
            progress = {"done": 0}

            async def _run_batch(batch: List[str]) -> Dict[str, Any]:
                async with semaphore:
                    result = await self._sync_batch_period_data(
                        service, data_source, period, batch, start_date, end_date
                    )
                    progress["done"] += len(batch)
                    logger.info(
                        f"📊 {data_source}-{period}进度: {progress['done']}/{len(symbols)} "
                        f"本批成功{result['success']} 失败{result['errors']}"
                    )
                    await asyncio.sleep(0.5)
                    return result

            logger.info(
                f"🚀 {data_source}-{period} 共 {len(batches)} 批，批间并发 {batch_limit}"
            )
            results = await asyncio.gather(
                *(_run_batch(batch) for batch in batches), return_exceptions=True
            )

            failed_batches = 0
            for result in results:
                if isinstance(result, BaseException):
                    failed_batches += 1
                    logger.error(f"❌ {data_source}-{period} 批次异常: {result}")
                    continue
                stats["records"] += result["records"]
                stats["success"] += result["success"]
                stats["errors"] += result["errors"]

            if batches and failed_batches == len(batches):
                logger.error(
                    f"🛑 {data_source}-{period} 全部 {len(batches)} 批异常，本轮无有效结果"
                )

            return stats
            
        except Exception as e:
            logger.error(f"❌ {data_source}-{period}同步失败: {e}")
            stats["errors"] += 1
            return stats
    
    async def _sync_batch_period_data(
        self,
        service,
        data_source: str,
        period: str,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Any]:
        """同步批次周期数据（受控并发）。

        批次内个股请求并发执行，并发度由 ``_sync_concurrency`` 决定。
        每只股票独立捕获异常，单只失败不影响同批其余股票。

        并发安全性说明：累加在事件循环单线程内完成，且 ``+=`` 前后没有
        ``await`` 让出点，因此不需要额外加锁；落库侧使用
        ``(symbol, trade_date, data_source, period)`` 唯一索引 + upsert，
        不同个股之间不存在写冲突。
        """
        stats = {"records": 0, "success": 0, "errors": 0}
        if data_source not in ("tushare", "akshare", "baostock"):
            return stats
        if not symbols:
            return stats

        # akshare 走批量通道：一个子进程拉整批，摊薄解释器冷启动开销
        if data_source == "akshare":
            return await self._sync_akshare_batch(period, symbols, start_date, end_date)

        concurrency = _sync_concurrency(data_source)
        semaphore = asyncio.Semaphore(concurrency)
        error_samples: List[str] = []

        async def _sync_one(symbol: str) -> None:
            async with semaphore:
                try:
                    hist_data = await service.provider.get_historical_data(
                        symbol, start_date, end_date, period
                    )
                except Exception as exc:
                    stats["errors"] += 1
                    if len(error_samples) < 5:
                        error_samples.append(f"{symbol}: {type(exc).__name__}: {exc}")
                    return

                if hist_data is None or hist_data.empty:
                    stats["errors"] += 1
                    if len(error_samples) < 5:
                        error_samples.append(f"{symbol}: 空数据")
                    return

                try:
                    saved_count = await self.historical_service.save_historical_data(
                        symbol=symbol,
                        data=hist_data,
                        data_source=data_source,
                        market="CN",
                        period=period
                    )
                except Exception as exc:
                    stats["errors"] += 1
                    if len(error_samples) < 5:
                        error_samples.append(f"{symbol}: 落库失败 {type(exc).__name__}: {exc}")
                    return

                stats["records"] += saved_count
                stats["success"] += 1

        await asyncio.gather(*(_sync_one(symbol) for symbol in symbols))

        if error_samples:
            # 只报样例，避免单批 50 只全失败时刷屏
            logger.warning(
                f"⚠️ {data_source}-{period} 批次失败 {stats['errors']} 只，"
                f"样例: " + " | ".join(error_samples)
            )

        return stats
    
    async def _sync_akshare_batch(
        self,
        period: str,
        symbols: List[str],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Any]:
        """akshare 批量通道：一次子进程拉整批，再逐只落库。

        冷启动（import pandas/akshare 约 3-5 秒）按批摊薄，而不是按只支付。
        落库保持逐只执行，单只写入毫秒级，不构成瓶颈。
        """
        from tradingagents.utils.bounded_data_fetch import fetch_async

        stats = {"records": 0, "success": 0, "errors": 0}
        try:
            frames, fetch_errors, _circuit = await fetch_async(
                "akshare_history_batch",
                {
                    "codes": list(symbols),
                    "start_date": start_date,
                    "end_date": end_date,
                    "period": period,
                },
                timeout=180,
            )
        except Exception as exc:
            logger.warning(
                f"⚠️ akshare 批量拉取失败 {len(symbols)} 只: {type(exc).__name__}: {exc}"
            )
            stats["errors"] += len(symbols)
            return stats

        for symbol in symbols:
            frame = frames.get(symbol)
            if frame is None or frame.empty:
                stats["errors"] += 1
                continue
            try:
                saved_count = await self.historical_service.save_historical_data(
                    symbol=symbol,
                    data=frame,
                    data_source="akshare",
                    market="CN",
                    period=period
                )
            except Exception as exc:
                stats["errors"] += 1
                logger.debug(f"akshare 落库失败 {symbol}: {type(exc).__name__}: {exc}")
                continue
            stats["records"] += saved_count
            stats["success"] += 1

        if fetch_errors:
            logger.info(
                f"ℹ️ akshare 批次 {len(symbols)} 只，拉取阶段失败 {len(fetch_errors)} 只"
            )

        return stats

    async def _get_all_symbols(self) -> List[str]:
        """获取所有股票代码"""
        try:
            # 从数据库获取股票列表
            from app.core.database import get_mongo_db
            db = get_mongo_db()
            collection = db.stock_basic_info

            # stock_basic_info 按 (code, source) 存多条，直接取 symbol 会把同一只股票取两次
            # （本库 5577 只 → 11154 条），导致同步请求量与耗时直接翻倍。此处按 symbol 去重。
            cursor = collection.find({}, {"symbol": 1})
            symbols = sorted({
                doc["symbol"] async for doc in cursor
                if isinstance(doc.get("symbol"), str) and doc["symbol"]
            })

            logger.info(f"📊 获取股票列表: {len(symbols)}只股票（已去重）")
            return symbols

        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
            return []

    async def _get_full_history_date_range(self) -> tuple[str, str]:
        """获取全历史数据的日期范围"""
        try:
            from datetime import datetime, timedelta

            # 结束日期：今天
            end_date = datetime.now().strftime('%Y-%m-%d')

            # 开始日期：根据数据源确定
            # Tushare: 1990年开始
            # AKShare: 1990年开始
            # BaoStock: 1990年开始
            # 为了安全起见，从1990年开始
            start_date = "1990-01-01"

            logger.info(f"📅 全历史数据范围: {start_date} 到 {end_date}")
            return start_date, end_date

        except Exception as e:
            logger.error(f"❌ 获取全历史日期范围失败: {e}")
            # 默认返回最近5年的数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
            return start_date, end_date
    
    async def get_sync_statistics(self) -> Dict[str, Any]:
        """获取同步统计信息"""
        try:
            if self.historical_service is None:
                await self.initialize()
            
            # 按周期统计
            from app.core.database import get_mongo_db
            db = get_mongo_db()
            collection = db.stock_daily_quotes
            
            pipeline = [
                {"$group": {
                    "_id": {
                        "period": "$period",
                        "data_source": "$data_source"
                    },
                    "count": {"$sum": 1},
                    "latest_date": {"$max": "$trade_date"}
                }}
            ]
            
            results = await collection.aggregate(pipeline).to_list(length=None)
            
            # 格式化统计结果
            stats = {}
            for result in results:
                period = result["_id"]["period"]
                source = result["_id"]["data_source"]
                
                if period not in stats:
                    stats[period] = {}
                
                stats[period][source] = {
                    "count": result["count"],
                    "latest_date": result["latest_date"]
                }
            
            return {
                "period_statistics": stats,
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ 获取同步统计失败: {e}")
            return {}


# 全局服务实例
_multi_period_sync_service = None


async def get_multi_period_sync_service() -> MultiPeriodSyncService:
    """获取多周期同步服务实例"""
    global _multi_period_sync_service
    if _multi_period_sync_service is None:
        _multi_period_sync_service = MultiPeriodSyncService()
        await _multi_period_sync_service.initialize()
    return _multi_period_sync_service


# APScheduler任务函数
async def run_multi_period_sync(periods: List[str] = None):
    """APScheduler任务：多周期数据同步"""
    try:
        service = await get_multi_period_sync_service()
        result = await service.sync_multi_period_data(periods=periods)
        logger.info(f"✅ 多周期数据同步完成: {result}")
        return result
    except Exception as e:
        logger.error(f"❌ 多周期数据同步失败: {e}")
        raise


async def run_daily_sync():
    """APScheduler任务：日线数据同步"""
    return await run_multi_period_sync(["daily"])


async def run_weekly_sync():
    """APScheduler任务：周线数据同步"""
    return await run_multi_period_sync(["weekly"])


async def run_monthly_sync():
    """APScheduler任务：月线数据同步"""
    return await run_multi_period_sync(["monthly"])
