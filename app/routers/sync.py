"""
Sync router for stock basics and ETF synchronization
- POST /api/sync/stock_basics/run -> trigger full sync
- GET  /api/sync/stock_basics/status -> get last status
- POST /api/sync/etf/run -> trigger ETF sync (etf_basic_info + etf_daily_quotes)
Requires MongoDB initialized by app lifespan.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.routers.auth_db import get_current_user
from app.services.basics_sync_service import get_basics_sync_service

router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.post("/stock_basics/run")
async def run_stock_basics_sync(force: bool = False):
    try:
        service = get_basics_sync_service()
        result = await service.start_full_sync(force=force)
        return {
            "success": True,
            "message": result.get("message") or "Synchronization task started",
            "data": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock_basics/status")
async def get_stock_basics_status():
    service = get_basics_sync_service()
    status = await service.get_status()
    return {"success": True, "data": status}


@router.post("/etf/run")
async def trigger_etf_sync(
    force: bool = Query(False, description="是否强制覆盖已有数据"),
    days: int = Query(30, ge=1, le=365, description="强制同步回溯天数（force=false 时忽略，自动走首次全量/后续增量）"),
):
    """触发 ETF 数据同步（etf_basic_info + etf_daily_quotes）"""
    try:
        from app.worker.etf_sync_service import run_etf_sync
        result = await run_etf_sync(force_update=force, days=days)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/etf/basic/run")
async def trigger_etf_basic_sync(
    force: bool = Query(False, description="是否强制刷新全部 ETF 基础信息"),
    current_user: dict = Depends(get_current_user),
):
    """轻量触发全市场 ETF 基础清单同步（仅更新 etf_basic_info，不拉取日线，不消耗 Tushare 额度）"""
    _ = current_user
    try:
        from app.worker.etf_sync_service import run_etf_basic_sync
        result = await run_etf_basic_sync(force_update=force)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

