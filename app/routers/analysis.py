"""
股票分析API路由
增强版本，支持优先级、进度跟踪、任务管理等功能
"""

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
import time
import uuid
import asyncio

from app.routers.auth_db import get_current_user
from app.services.queue_service import get_queue_service, QueueService
from app.services.analysis_service import get_analysis_service
from app.services.simple_analysis_service import get_simple_analysis_service
from app.services.unified_analysis_service import get_unified_analysis_service
from app.services.websocket_manager import get_websocket_manager
from app.utils.error_formatter import ErrorFormatter
from app.models.analysis import (
    SingleAnalysisRequest, BatchAnalysisRequest, AnalysisParameters,
    AnalysisTaskResponse, AnalysisBatchResponse, AnalysisHistoryQuery,
    AnalysisEngine, AnalysisTaskType,
)
from tradingagents.utils.stock_utils import StockUtils, StockMarket, SecurityType
from app.core.database import get_mongo_db
from app.utils.timezone import now_tz
from bson import ObjectId

router = APIRouter()
logger = logging.getLogger("webapi")


def format_http_error_detail(exc: Exception) -> str:
    return ErrorFormatter.format_user_message(str(exc))


# 🔥 统一的结果数据格式化函数
def normalize_result_data(result_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    统一格式化分析结果数据，确保所有接口返回一致的格式

    主要处理：
    1. recommendation 字段：使用合规术语，不包含具体价格
    2. key_points 字段：转换 JSON 字段名为中文描述
    3. decision 字段：支持新旧字段名
    4. action 字段：将旧术语（买入/卖出/持有）映射为合规术语（乐观/审慎/中性）
    """
    if not result_data:
        return result_data

    # 🔥 术语映射：将旧术语转换为合规术语
    ACTION_TRANSLATION = {
        'BUY': '乐观', 'SELL': '审慎', 'HOLD': '中性',
        'buy': '乐观', 'sell': '审慎', 'hold': '中性',
        '买入': '乐观', '卖出': '审慎', '持有': '中性',  # 兼容旧数据
        '看涨': '乐观', '看跌': '审慎', '中性': '中性',  # 兼容旧数据
        '乐观': '乐观', '审慎': '审慎', '中性': '中性',  # 保持新术语不变
    }

    # 1. 处理 decision 字段（支持新旧字段名）
    decision = result_data.get('decision', {})
    if isinstance(decision, dict):
        # 支持新旧字段名
        raw_action = decision.get('analysis_view') or decision.get('action')
        # 🔥 映射为合规术语
        action = ACTION_TRANSLATION.get(raw_action, raw_action) if raw_action else None

        # 🔥 更新 decision 对象中的 action 字段为合规术语
        if action and raw_action != action:
            decision['action'] = action
            # 如果有 analysis_view 字段，也更新它
            if 'analysis_view' in decision:
                decision['analysis_view'] = action

        # 🔥 优先使用 price_analysis_range（价格区间），不使用 target_price（单个值）
        price_range = decision.get('price_analysis_range')
        # 如果没有 price_analysis_range，才使用 target_price（但这是单个值，不应该用于价格区间显示）
        if not price_range:
            target_price = decision.get('target_price')
            if target_price:
                logger.warning(f"⚠️ [normalize_result_data] decision 中没有 price_analysis_range，只有 target_price: {target_price}，前端将显示 '-'")
        
        confidence = decision.get('confidence')
        # 🔥 优先从 decision 中获取 risk_score（风险经理的评估结果）
        risk_score = decision.get('risk_score')
        risk_level = decision.get('risk_level')
        reasoning = decision.get('reasoning', '')
        
        # 🔥 调试日志：记录 decision 字段
        logger.info(f"🔍 [normalize_result_data] decision 字段检查: price_analysis_range={price_range}, risk_score={risk_score}, target_price={decision.get('target_price')}")

        # 2. 格式化 recommendation（如果为空或包含旧术语）
        recommendation = result_data.get('recommendation', '')
        if not recommendation or '投资建议' in recommendation or '目标价格' in recommendation or '买入' in recommendation or '卖出' in recommendation:
            # 重新生成合规的 recommendation
            recommendation = f"分析观点：{action}；" if action else ""
            if reasoning and len(reasoning) < 100:
                recommendation += f"分析依据：{reasoning}"
            elif reasoning and reasoning != "暂无分析推理":
                short_reasoning = reasoning[:50] + "..." if len(reasoning) > 50 else reasoning
                recommendation += f"分析依据：{short_reasoning}"
            result_data['recommendation'] = recommendation

        # 3. 格式化 key_points（如果包含 JSON 字段名）
        key_points = result_data.get('key_points', [])
        if key_points and isinstance(key_points, list):
            # 检查是否包含 JSON 字段名（如 "analysis_view": "中性"）
            if any('"' in str(kp) and ':' in str(kp) for kp in key_points):
                # 重新生成 key_points
                new_key_points = []
                if action:
                    new_key_points.append(f"分析观点: {action}")
                # 🔥 只显示价格区间，不显示单个值
                if price_range:
                    if isinstance(price_range, (list, tuple)) and len(price_range) == 2:
                        new_key_points.append(f"价格分析区间: {price_range[0]}-{price_range[1]}元")
                    else:
                        logger.warning(f"⚠️ [normalize_result_data] price_range 不是有效的区间格式: {price_range}")
                # 不添加 target_price 到 key_points（因为这是单个值，不符合合规要求）
                if confidence is not None:
                    new_key_points.append(f"置信度: {confidence * 100:.1f}%" if confidence <= 1 else f"置信度: {confidence}%")
                if risk_level:
                    new_key_points.append(f"风险等级: {risk_level}")
                if risk_score is not None:
                    new_key_points.append(f"风险评分: {risk_score * 100:.1f}%" if risk_score <= 1 else f"风险评分: {risk_score}%")

                result_data['key_points'] = new_key_points[:5]

    return result_data


def _has_meaningful_text(value: Any, min_length: int = 1) -> bool:
    return isinstance(value, str) and len(value.strip()) >= min_length


def _summarize_result_completeness(result_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(result_data, dict):
        return {
            "has_summary": False,
            "has_recommendation": False,
            "report_count": 0,
            "has_action": False,
            "has_reasoning": False,
            "has_price_range": False,
            "has_confidence": False,
            "has_risk_score": False,
            "is_complete": False,
        }

    decision = result_data.get("decision") if isinstance(result_data.get("decision"), dict) else {}
    reports = result_data.get("reports") if isinstance(result_data.get("reports"), dict) else {}
    summary = {
        "has_summary": _has_meaningful_text(result_data.get("summary"), 8),
        "has_recommendation": _has_meaningful_text(result_data.get("recommendation"), 8),
        "report_count": len([key for key, value in reports.items() if _has_meaningful_text(value, 10)]),
        "has_action": bool(decision.get("analysis_view") or decision.get("action")),
        "has_reasoning": _has_meaningful_text(decision.get("reasoning"), 10),
        "has_price_range": decision.get("price_analysis_range") is not None,
        "has_confidence": decision.get("confidence") is not None,
        "has_risk_score": decision.get("risk_score") is not None,
    }
    summary["is_complete"] = bool(
        summary["has_summary"] or
        summary["report_count"] > 0 or
        summary["has_action"] or
        summary["has_reasoning"]
    )
    return summary


def _log_result_completeness(source: str, task_id: str, result_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    completeness = _summarize_result_completeness(result_data)
    logger.info(
        f"📊 [RESULT] 来源={source} task={task_id} 完整性: "
        f"summary={completeness['has_summary']} recommendation={completeness['has_recommendation']} "
        f"reports={completeness['report_count']} action={completeness['has_action']} "
        f"reasoning={completeness['has_reasoning']} price_range={completeness['has_price_range']} "
        f"confidence={completeness['has_confidence']} risk_score={completeness['has_risk_score']} "
        f"complete={completeness['is_complete']}"
    )
    return completeness


def get_market_type_from_symbol(symbol: str) -> str:
    """根据股票代码获取 market_type

    Args:
        symbol: 股票代码

    Returns:
        market_type: "cn" (A股), "hk" (港股), "us" (美股)
    """
    market = StockUtils.identify_stock_market(symbol)

    # 映射 StockMarket 枚举到 market_type 字符串
    market_type_mapping = {
        StockMarket.CHINA_A: "cn",
        StockMarket.HONG_KONG: "hk",
        StockMarket.US: "us",
        StockMarket.UNKNOWN: "cn"  # 默认为中国市场
    }

    return market_type_mapping.get(market, "cn")


def _build_user_id_candidates(user_id: Any) -> List[Any]:
    """构造同时兼容 str 与 ObjectId 的 user_id 候选列表。"""
    uid_str = str(user_id or "").strip()
    candidates: List[Any] = []
    if uid_str:
        candidates.append(uid_str)
    if uid_str == "admin":
        admin_oid_str = "507f1f77bcf86cd799439011"
        if admin_oid_str not in candidates:
            candidates.append(admin_oid_str)
        try:
            candidates.append(ObjectId(admin_oid_str))
        except Exception:
            pass
    elif uid_str:
        try:
            candidates.append(ObjectId(uid_str))
        except Exception:
            pass
    return candidates


def _to_py_object_id(user_id: Any):
    """兼容 admin 字符串与 24 位 hex 字符串，安全构造 PyObjectId。"""
    from app.models.user import PyObjectId
    uid_str = str(user_id or "").strip()
    if uid_str == "admin":
        return PyObjectId("507f1f77bcf86cd799439011")
    return PyObjectId(uid_str)


def _is_admin_user(user: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(user, dict):
        return False
    return bool(
        user.get("username") == "admin"
        or user.get("is_admin")
        or str(user.get("role") or "").lower() == "admin"
    )


def _is_task_owner(task_user_id: Any, user: Optional[Dict[str, Any]]) -> bool:
    """校验当前用户是否为任务所有者（管理员放行）。"""
    if _is_admin_user(user):
        return True
    if task_user_id is None or not isinstance(user, dict):
        return False
    task_uid_str = str(task_user_id).strip()
    if not task_uid_str:
        return False
    candidate_strs = {str(c) for c in _build_user_id_candidates(user.get("id"))}
    return task_uid_str in candidate_strs


def _resolve_symbol_and_etf_routing(
    raw_symbol: str,
    requested_market: Optional[str] = None,
) -> tuple:
    """统一标准化证券代码并判定是否走 ETF 分析链路。

    Returns:
        (normalized_symbol, is_etf, resolved_market_type)
    """
    norm_symbol = StockUtils.normalize_symbol(raw_symbol)
    if not norm_symbol:
        return "", False, "cn"

    req_market = str(requested_market or "").strip()
    sec_type = StockUtils.identify_security_type(norm_symbol)
    is_etf = (req_market.upper() == "ETF") or (sec_type == SecurityType.FUND)
    if is_etf:
        return norm_symbol, True, "ETF"

    detected_market = StockUtils.identify_stock_market(norm_symbol)
    if detected_market != StockMarket.UNKNOWN:
        return norm_symbol, False, get_market_type_from_symbol(norm_symbol)

    req_map = {
        "A股": "cn", "CN": "cn", "cn": "cn",
        "港股": "hk", "HK": "hk", "hk": "hk",
        "美股": "us", "US": "us", "us": "us",
    }
    resolved_market = req_map.get(req_market, req_map.get(req_market.upper(), "cn"))
    return norm_symbol, False, resolved_market


async def _check_llm_availability(config: Any, db: Any) -> tuple:
    """检查系统是否已配置至少一个可用 LLM（兼容模型配置、厂家配置、环境变量与京东云模式）。"""
    from app.utils.api_key_utils import is_valid_api_key, get_env_api_key_for_provider

    try:
        from app.core.jdyun import is_jdyun_mode, get_jdyun_api_key
        if is_jdyun_mode() and is_valid_api_key(get_jdyun_api_key()):
            return True, "ok", "LLM 就绪（京东云模式）", {"available_llms": ["jdyun"]}
    except Exception:
        pass

    raw_llms = getattr(config, "llm_configs", None) if config is not None else None
    if raw_llms is None and isinstance(config, dict):
        raw_llms = config.get("llm_configs")
    raw_llms = raw_llms or []

    enabled_llms = []
    for item in raw_llms:
        is_enabled = item.get("enabled", True) if isinstance(item, dict) else getattr(item, "enabled", True)
        if is_enabled:
            enabled_llms.append(item)

    if not enabled_llms:
        return (
            False,
            "no_llm",
            "尚未配置可用的大模型（LLM），请先在系统配置中启用大模型",
            {"enabled_llms": [], "available_llms": []},
        )

    # 读取已启用的厂家密钥（可选，表不存在时静默降级）
    provider_key_map: Dict[str, bool] = {}
    try:
        providers_coll = db["llm_providers"]
        provider_docs = await providers_coll.find({}, {"name": 1, "api_key": 1, "is_active": 1}).limit(200).to_list(length=200)
        for pdoc in provider_docs or []:
            pname = str(pdoc.get("name") or "").strip().lower()
            if not pname:
                continue
            if pdoc.get("is_active", True) and is_valid_api_key(pdoc.get("api_key")):
                provider_key_map[pname] = True
    except Exception:
        pass

    enabled_names: List[str] = []
    available_names: List[str] = []
    for item in enabled_llms:
        model_name = item.get("model_name") if isinstance(item, dict) else getattr(item, "model_name", None)
        provider_raw = item.get("provider") if isinstance(item, dict) else getattr(item, "provider", None)
        api_key = item.get("api_key") if isinstance(item, dict) else getattr(item, "api_key", None)
        provider = str(getattr(provider_raw, "value", provider_raw) or "").strip().lower()
        label = str(model_name or provider or "unknown")
        enabled_names.append(label)

        has_key = (
            provider in ("local", "ollama", "localai")
            or is_valid_api_key(api_key)
            or provider_key_map.get(provider, False)
            or bool(provider and get_env_api_key_for_provider(provider))
        )
        if has_key:
            available_names.append(label)

    if not available_names:
        return (
            False,
            "no_llm_key",
            "已启用的大模型缺少有效的 API Key，请先在系统配置中完善大模型密钥",
            {"enabled_llms": enabled_names, "available_llms": []},
        )

    return (
        True,
        "ok",
        "LLM 可用",
        {"enabled_llms": enabled_names, "available_llms": available_names},
    )


def _extract_doc_symbol(doc: Dict[str, Any]) -> str:
    params = doc.get("task_params") or doc.get("parameters") or {}
    return str(
        doc.get("symbol")
        or doc.get("stock_code")
        or doc.get("stock_symbol")
        or params.get("symbol")
        or params.get("stock_code")
        or params.get("code")
        or ""
    )


def _format_history_datetime(val: Any) -> Optional[str]:
    if not val:
        return None
    if hasattr(val, "isoformat"):
        if val.tzinfo is None:
            from datetime import timezone, timedelta
            val = val.replace(tzinfo=timezone(timedelta(hours=8)))
        return val.isoformat()
    return str(val)


def _normalize_history_doc(doc: Dict[str, Any], source_collection: str) -> Dict[str, Any]:
    params = doc.get("task_params") if source_collection == "unified_analysis_tasks" else doc.get("parameters")
    params = params if isinstance(params, dict) else {}
    sym = _extract_doc_symbol(doc)
    raw_status = doc.get("status", "pending")
    status_str = raw_status.value if hasattr(raw_status, "value") else str(raw_status)
    frontend_status = "running" if status_str == "processing" else status_str
    task_type = doc.get("task_type") or ("etf_analysis" if params.get("is_etf") else "stock_analysis")
    market_type = params.get("market_type") or doc.get("market_type") or ("ETF" if task_type == "etf_analysis" else "cn")
    user_field = doc.get("user_id", doc.get("user"))
    return {
        "task_id": doc.get("task_id"),
        "user_id": str(user_field) if user_field is not None else None,
        "symbol": sym,
        "stock_code": sym,
        "stock_symbol": sym,
        "stock_name": doc.get("stock_name") or params.get("stock_name"),
        "task_type": task_type,
        "market_type": market_type,
        "status": frontend_status,
        "progress": int(doc.get("progress", 0) or 0),
        "message": doc.get("message", ""),
        "current_step": doc.get("current_step", ""),
        "error_message": doc.get("error_message") or doc.get("last_error"),
        "created_at": _format_history_datetime(doc.get("created_at")),
        "start_time": _format_history_datetime(doc.get("started_at") or doc.get("created_at")),
        "end_time": _format_history_datetime(doc.get("completed_at")),
        "parameters": params,
        "execution_time": doc.get("execution_time"),
        "tokens_used": doc.get("tokens_used"),
        "source_collection": source_collection,
    }


async def get_user_risk_preference(user_id: str) -> str:
    """获取用户的风险偏好设置

    Args:
        user_id: 用户ID

    Returns:
        风险偏好: 'conservative'(保守) / 'neutral'(中性) / 'aggressive'(激进)
        默认返回 'neutral'
    """
    try:
        db = get_mongo_db()
        users_collection = db["users"]

        # 查询用户
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if user and "preferences" in user:
            risk_pref = user["preferences"].get("risk_preference", "neutral")
            logger.info(f"📊 获取用户 {user_id} 的风险偏好: {risk_pref}")
            return risk_pref

        logger.info(f"📊 用户 {user_id} 未设置风险偏好，使用默认值: neutral")
        return "neutral"
    except Exception as e:
        logger.warning(f"⚠️ 获取用户风险偏好失败: {e}，使用默认值: neutral")
        return "neutral"

# 兼容性：保留原有的请求模型
class SingleAnalyzeRequest(BaseModel):
    symbol: str
    parameters: dict = Field(default_factory=dict)

class BatchAnalyzeRequest(BaseModel):
    symbols: List[str]
    parameters: dict = Field(default_factory=dict)
    title: str = Field(default="批量分析", description="批次标题")
    description: Optional[str] = Field(None, description="批次描述")

# 新版API端点
@router.post("/single", response_model=Dict[str, Any])
async def submit_single_analysis(
    request: SingleAnalysisRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user)
):
    """
    提交单股分析任务 - 支持 AB 测试

    通过 request.parameters.engine 参数选择引擎:
    - v2 (默认): 使用新引擎 WorkflowEngine + v2 agent
    - unified: 使用统一引擎 WorkflowEngine (LangGraph 动态构建)
    - legacy: 已停用（v3.0 起封堵，返回 400）
    """
    try:
        logger.info("🎯 收到单股分析请求")
        logger.info(f"👤 用户信息: {user}")
        logger.info(f"📊 请求数据: {request}")

        # 判断使用哪个引擎
        # v3.0 起默认使用 V2 引擎（旧版 legacy 已停用）
        engine_type = AnalysisEngine.V2
        if request.parameters and hasattr(request.parameters, "engine"):
            engine_type = request.parameters.engine
            logger.info(f"🔍 [引擎判断] 从 request.parameters.engine 获取: {engine_type} (类型: {type(engine_type)})")
            # v3.0 起拒绝 legacy 引擎
            if engine_type == AnalysisEngine.LEGACY:
                raise HTTPException(
                    status_code=400,
                    detail="[已停用] legacy 引擎已停用，请改用 v2 引擎（request.parameters.engine='v2'）。"
                    "详见 docs/99-archive/deprecated-features/legacy-agents-deprecation.md"
                )
        else:
            logger.info(f"ℹ️ [引擎判断] request.parameters 中没有 engine 字段，使用默认引擎: {engine_type.value}")
            if request.parameters:
                logger.info(f"🔍 [引擎判断] request.parameters 类型: {type(request.parameters)}")
                logger.info(f"🔍 [引擎判断] request.parameters 内容: {request.parameters}")
                if hasattr(request.parameters, "model_dump"):
                    logger.info(f"🔍 [引擎判断] request.parameters.model_dump(): {request.parameters.model_dump()}")

        logger.info(f"🔧 [AB测试] 最终使用引擎: {engine_type.value} (枚举值: {engine_type})")

        # 根据引擎类型选择任务创建方式
        # 🔍 兼容性检查：同时支持枚举和字符串比较
        is_v2_engine = (
            engine_type == AnalysisEngine.V2 or 
            (isinstance(engine_type, str) and engine_type.lower() == "v2") or
            (hasattr(engine_type, "value") and engine_type.value == "v2")
        )
        
        if is_v2_engine:
            logger.info(f"✅ [引擎判断] 确认为 v2 引擎，使用统一任务服务")
            # v2 引擎：使用统一任务服务创建 UnifiedAnalysisTask
            from app.services.task_analysis_service import get_task_analysis_service
            from app.models.analysis import AnalysisTaskType
            from app.models.user import PyObjectId

            task_service = get_task_analysis_service()

            # 准备任务参数 - 标准化代码并将 parameters 展开到顶层
            raw_symbol = request.get_symbol()
            user_market_raw = (request.parameters and getattr(request.parameters, "market_type", None)) or None
            symbol, is_etf, resolved_market = _resolve_symbol_and_etf_routing(raw_symbol, user_market_raw)
            if not symbol:
                raise HTTPException(status_code=400, detail="股票代码不能为空")

            logger.info(f"📊 [单股分析] 标的 {raw_symbol} -> {symbol}, is_etf={is_etf}, market_type={resolved_market}")

            task_params = {
                "symbol": symbol,
                "stock_code": symbol,
                "market_type": resolved_market,
            }

            # 将 request.parameters 的所有字段合并到 task_params
            if request.parameters:
                params_dict = request.parameters.model_dump()
                # 移除 engine 字段（这是路由层使用的，不是任务参数）
                params_dict.pop("engine", None)
                task_params.update(params_dict)

            # 🔥 ETF 路由：保留 market_type="ETF"、is_etf=True、task_type="etf_analysis"，不再覆写为 "cn"
            if is_etf:
                task_type = AnalysisTaskType.ETF_ANALYSIS
                task_params["market_type"] = "ETF"
                task_params["is_etf"] = True
                task_params["task_type"] = AnalysisTaskType.ETF_ANALYSIS.value
                task_params["selected_analysts"] = ["etf", "market", "news"]  # ETF 流程固定分析师
                logger.info(f"📊 [单股分析] {symbol} 识别为 ETF，保留 market_type=ETF / is_etf=True / task_type=etf_analysis")
            else:
                task_type = AnalysisTaskType.STOCK_ANALYSIS
                task_params["market_type"] = resolved_market
                task_params["is_etf"] = False
                task_params["task_type"] = AnalysisTaskType.STOCK_ANALYSIS.value

            logger.info(f"📦 v2任务参数: {task_params}")

            # 🔥 从用户偏好读取风险偏好设置
            preference_type = await get_user_risk_preference(user["id"])
            logger.info(f"📊 [单股分析] 使用用户风险偏好: {preference_type}")

            # 创建统一任务（不执行）
            task = await task_service.create_task(
                user_id=_to_py_object_id(user["id"]),
                task_type=task_type,
                task_params=task_params,
                engine_type="auto",  # 自动选择引擎
                preference_type=preference_type  # 🔥 使用用户偏好设置
            )

            # 🔑 关键：同时在内存中创建任务状态（用于快速查询）
            from app.services.memory_state_manager import get_memory_state_manager, TaskStatus
            memory_manager = get_memory_state_manager()
            await memory_manager.create_task(
                task_id=task.task_id,
                user_id=user["id"],
                stock_code=symbol,
                parameters=task_params,
                stock_name=None  # 可以后续补充
            )
            logger.info(f"✅ [v2引擎] 任务已创建到内存: {task.task_id}")

            # 🔥 v2 引擎：将任务提交到队列，由 Worker 处理
            from app.services.queue_service import get_queue_service
            queue_service = get_queue_service()
            
            # 准备队列参数
            queue_params = {
                "engine": "v2",  # 标记为 v2 引擎
                "task_id": task.task_id,
                "symbol": symbol,
                "stock_code": symbol,
                "user_id": user["id"],
                **task_params  # 包含所有任务参数（含 is_etf / task_type / market_type）
            }
            
            # 提交到队列（入队失败时回写 FAILED 避免僵尸任务）
            try:
                await queue_service.enqueue_task(
                    user_id=user["id"],
                    symbol=symbol,
                    params=queue_params,
                    batch_id=None,  # 单股分析没有批次ID
                    task_id=task.task_id  # 使用已生成的任务ID
                )
            except Exception as enqueue_err:
                fail_msg = f"任务入队失败: {enqueue_err}"
                logger.error(f"❌ [v2引擎] {fail_msg} (task_id={task.task_id})")
                try:
                    db = get_mongo_db()
                    await db.unified_analysis_tasks.update_one(
                        {"task_id": task.task_id},
                        {"$set": {
                            "status": "failed",
                            "error_message": fail_msg,
                            "message": fail_msg,
                            "completed_at": now_tz(),
                            "updated_at": now_tz(),
                        }},
                    )
                except Exception:
                    pass
                try:
                    await memory_manager.update_task_status(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        message=fail_msg,
                        error_message=fail_msg,
                    )
                except Exception:
                    pass
                raise HTTPException(status_code=503, detail=format_http_error_detail(enqueue_err))

            logger.info(f"✅ [v2引擎] 任务已提交到队列: {task.task_id}")

            result = {
                "task_id": task.task_id,  # 使用 UUID task_id，不是 ObjectId
                "status": task.status.value,
                "task_type": task_type.value,
                "is_etf": is_etf,
                "market_type": task_params["market_type"],
                "created_at": task.created_at.isoformat() if task.created_at else None
            }
            task_id = task.task_id  # 使用 UUID task_id
            user_id = user["id"]
        else:
            # unified 引擎：使用 simple_analysis_service 创建任务并入队
            # （legacy 引擎已在前面拦截抛 400；unified 实际走 WorkflowEngine，属于新引擎路径）
            legacy_service = get_simple_analysis_service()
            result = await legacy_service.create_analysis_task(user["id"], request)
            # 🔥 注意：create_analysis_task 内部已经将任务提交到队列，不需要后台执行

        # 🔥 所有引擎的任务都已提交到队列，由 Worker 进程处理
        # 不再需要在 API 进程中后台执行任务
        logger.info(f"✅ 分析任务已提交到队列: {result}")

        return {
            "success": True,
            "data": result,
            "message": f"分析任务已提交到队列，等待 Worker 处理 (引擎: {engine_type.value})"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 提交单股分析任务失败: {e}")
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))


# 测试路由 - 验证路由是否被正确注册
@router.get("/test-route")
async def test_route():
    """测试路由是否工作"""
    logger.info("🧪 测试路由被调用了！")
    return {"message": "测试路由工作正常", "timestamp": time.time()}


# =============================================================================
# 分析前预检接口
# =============================================================================

# 缓存层/非真实 API 数据源类型，不视为可用数据源
_CACHE_SOURCE_TYPES = {"local", "mongodb", "local_file"}


@router.get("/precheck", response_model=Dict[str, Any])
async def precheck_analysis(
    symbol: Optional[str] = Query(None, description="待分析的股票代码（可选）"),
    market_type: Optional[str] = Query(None, description="市场类型（可选：CN/HK/US/ETF）"),
    user: dict = Depends(get_current_user),
):
    """
    分析提交前预检：检查数据源配置、LLM 可用性、基础数据同步状态、目标标的是否存在。

    - 支持 50/51/52/56/58/15/16/18 全前缀 ETF（含 588000 / 56xxxx / 18xxxx）
    - 兼顾 HK / US 市场（检查对应市场基础表并在未本地同步时允许在线降级）
    - 预检可用 LLM 配置，避免提交后必然失败
    """
    from app.services.config_service import config_service

    try:
        # ── 1. 检查数据源配置 ──
        config = await config_service.get_system_config()
        enabled_real_sources: List[str] = []
        if config and getattr(config, "data_source_configs", None):
            for ds in config.data_source_configs:
                ds_type = getattr(getattr(ds, "type", None), "value", getattr(ds, "type", None))
                if getattr(ds, "enabled", False) and str(ds_type or "").lower() not in _CACHE_SOURCE_TYPES:
                    enabled_real_sources.append(str(ds_type))

        if not enabled_real_sources:
            return {
                "can_submit": False,
                "reason": "no_datasource",
                "message": "尚未配置可用的数据源，请先完成数据源配置",
                "redirect": "/settings/config",
                "redirect_tab": "datasource",
                "details": {"enabled_real_sources": []},
            }

        db = get_mongo_db()

        # ── 2. 检查 LLM 可用性 ──
        llm_ok, llm_reason, llm_message, llm_details = await _check_llm_availability(config, db)
        if not llm_ok:
            return {
                "can_submit": False,
                "reason": llm_reason,
                "message": llm_message,
                "redirect": "/settings/config",
                "redirect_tab": "llm",
                "details": {
                    "enabled_real_sources": enabled_real_sources,
                    **llm_details,
                },
            }

        # ── 3. 识别市场与证券类型（支持 56/58/18 ETF 与 HK/US） ──
        norm_symbol = StockUtils.normalize_symbol(symbol) if symbol else ""
        req_market = str(market_type or "").strip().upper()
        is_etf = bool(
            req_market == "ETF"
            or (norm_symbol and StockUtils.identify_security_type(norm_symbol) == SecurityType.FUND)
        )

        stock_market = StockUtils.identify_stock_market(symbol) if symbol else StockMarket.CHINA_A
        if req_market in ("HK", "港股"):
            stock_market = StockMarket.HONG_KONG
        elif req_market in ("US", "美股"):
            stock_market = StockMarket.US

        if is_etf:
            market_code = "ETF"
            basics_collection = "etf_basic_info"
            security_label = "ETF"
        elif stock_market == StockMarket.HONG_KONG:
            market_code = "HK"
            basics_collection = "stock_basic_info_hk"
            security_label = "港股"
        elif stock_market == StockMarket.US:
            market_code = "US"
            basics_collection = "stock_basic_info_us"
            security_label = "美股"
        else:
            market_code = "CN"
            basics_collection = "stock_basic_info"
            security_label = "股票"

        # ── 4. 检查基础数据同步与目标标的存在性 ──
        basics_count = await db[basics_collection].count_documents({})
        if market_code in ("HK", "US"):
            # HK / US 市场优先查询本地基础表，未同步时放行并标注在线降级
            target_exists = False
            if norm_symbol and basics_count > 0:
                candidates = [norm_symbol]
                if market_code == "HK":
                    plain_hk = norm_symbol.replace(".HK", "")
                    candidates.extend([plain_hk, plain_hk.zfill(5), f"{plain_hk}.HK", f"{plain_hk.zfill(5)}.HK"])
                unique_candidates = list(dict.fromkeys([c for c in candidates if c]))
                doc = await db[basics_collection].find_one(
                    {"$or": [{"code": {"$in": unique_candidates}}, {"symbol": {"$in": unique_candidates}}]}
                )
                target_exists = doc is not None
            return {
                "can_submit": True,
                "reason": "ok",
                "message": (
                    f"{security_label}数据就绪，可以开始分析"
                    if target_exists or not norm_symbol
                    else f"{security_label} {norm_symbol} 未在本地基础表命中，分析时将自动在线获取行情"
                ),
                "details": {
                    "enabled_real_sources": enabled_real_sources,
                    "basics_collection": basics_collection,
                    "basics_count": basics_count,
                    "market": market_code,
                    "is_etf": False,
                    "symbol": norm_symbol or symbol,
                    "target_exists": target_exists,
                    "online_fallback": not target_exists,
                    "llm_ready": True,
                    **llm_details,
                },
            }

        if basics_count == 0:
            return {
                "can_submit": False,
                "reason": "no_basics",
                "message": f"{security_label}基础数据尚未同步，请先执行数据同步",
                "redirect": "/settings/sync",
                "details": {
                    "enabled_real_sources": enabled_real_sources,
                    "basics_collection": basics_collection,
                    "basics_count": 0,
                    "market": market_code,
                    "is_etf": is_etf,
                    "llm_ready": True,
                    **llm_details,
                },
            }

        target_exists = True
        if norm_symbol:
            query = {"$or": [{"code": norm_symbol}, {"symbol": norm_symbol}]}
            doc = await db[basics_collection].find_one(query)
            target_exists = doc is not None

        if norm_symbol and not target_exists:
            return {
                "can_submit": False,
                "reason": "stock_not_found",
                "message": f"{security_label} {norm_symbol} 的基础数据未同步，请先同步该{security_label}数据",
                "redirect": "/settings/sync",
                "details": {
                    "enabled_real_sources": enabled_real_sources,
                    "basics_collection": basics_collection,
                    "basics_count": basics_count,
                    "market": market_code,
                    "is_etf": is_etf,
                    "symbol": norm_symbol,
                    "llm_ready": True,
                    **llm_details,
                },
            }

        # ── 5. 全部通过 ──
        return {
            "can_submit": True,
            "reason": "ok",
            "message": "数据就绪，可以开始分析",
            "details": {
                "enabled_real_sources": enabled_real_sources,
                "basics_collection": basics_collection,
                "basics_count": basics_count,
                "market": market_code,
                "is_etf": is_etf,
                "symbol": norm_symbol or symbol,
                "target_exists": target_exists,
                "llm_ready": True,
                **llm_details,
            },
        }
    except Exception as e:
        logger.error(f"❌ 分析预检失败: {e}", exc_info=True)
        # 预检本身失败不阻止分析（降级），让后续 Worker 的校验兜底
        return {
            "can_submit": True,
            "reason": "precheck_error",
            "message": "预检服务异常，已放行提交",
            "details": {"error": str(e)},
        }


@router.get("/tasks/{task_id}/status", response_model=Dict[str, Any])
async def get_task_status_new(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """获取分析任务状态（新版异步实现）
    
    查询优先级：
    1. MongoDB (unified_analysis_tasks) - 真实数据源，Worker 进程会更新这里
    2. MongoDB (analysis_tasks) - 旧版任务
    3. 内存状态管理器 - 仅作为缓存（进程内，Worker 和 API 进程分开）
    """
    try:
        logger.info(f"🔍 [NEW ROUTE] 进入新版状态查询路由: {task_id}")
        logger.info(f"👤 [NEW ROUTE] 用户: {user}")

        # 🔥 优先从 MongoDB 查询（真实数据源，Worker 进程会更新这里）
        # 因为 Worker 进程和 API 进程是分开的，内存状态管理器不共享
        from app.core.database import get_mongo_db
        from bson import ObjectId
        db = get_mongo_db()

        # 首先尝试从 unified_analysis_tasks 集合中查找（v2 引擎任务）
        unified_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
        if unified_task:
            logger.info(f"✅ [STATUS] 从unified_analysis_tasks找到任务: {task_id}")

            # 🔑 关键：区分步骤名称和消息
            # current_step 存储的是简短的步骤名称（如 "市场分析师"）
            # message 存储的是详细的描述（如 "📈 市场分析师正在分析技术指标和市场趋势..."）
            current_step_name = unified_task.get("current_step", "")
            current_step_description = unified_task.get("message", current_step_name)

            # 获取状态，并做兼容性映射（前端期望 'running' 而不是 'processing'）
            backend_status = unified_task.get("status", "pending")
            # 🔥 状态映射：processing → running, cancelled → cancelled, failed → failed
            if backend_status == "processing":
                frontend_status = "running"
            elif backend_status == "cancelled":
                frontend_status = "cancelled"  # 保持 cancelled 状态
            else:
                frontend_status = backend_status

            # 🔧 计算时间估算（遵循旧版本逻辑）
            from app.services.progress.tracker import RedisProgressTracker
            from datetime import datetime, timezone

            # 获取任务参数
            task_params = unified_task.get("task_params", {})
            progress_pct = unified_task.get("progress", 0)
            start_time = unified_task.get("started_at")
            end_time = unified_task.get("completed_at")

            # 计算时间估算
            elapsed_time = 0
            remaining_time = 0
            estimated_total_time = 0

            if end_time:
                # 任务已完成，使用最终执行时间
                from dateutil import parser

                if isinstance(start_time, str):
                    try:
                        start_time = parser.parse(start_time)
                    except Exception as e:
                        logger.warning(f"⚠️ 无法解析开始时间: {start_time}, 错误: {e}")
                        start_time = None

                if isinstance(end_time, str):
                    try:
                        end_time = parser.parse(end_time)
                    except Exception as e:
                        logger.warning(f"⚠️ 无法解析结束时间: {end_time}, 错误: {e}")
                        end_time = None

                if start_time and end_time:
                    # 确保时区一致
                    if start_time.tzinfo is None:
                        start_time = start_time.replace(tzinfo=timezone.utc)
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)

                    elapsed_time = (end_time - start_time).total_seconds()
                    estimated_total_time = elapsed_time  # 已完成任务的总时长就是已用时间
                    remaining_time = 0

            elif start_time:
                # 任务进行中
                if isinstance(start_time, str):
                    try:
                        from dateutil import parser
                        start_time = parser.parse(start_time)
                    except Exception as e:
                        logger.warning(f"⚠️ 无法解析开始时间: {start_time}, 错误: {e}")
                        start_time = None

                if start_time:
                    # 确保使用本地时间（带时区）
                    current_time = datetime.now(timezone.utc)

                    # 确保 start_time 有时区信息
                    if start_time.tzinfo is None:
                        # 假设数据库中的时间是 UTC
                        start_time = start_time.replace(tzinfo=timezone.utc)

                    # 计算已用时间
                    elapsed_time = (current_time - start_time).total_seconds()

                    # 🔑 关键：使用任务创建时预估的总时长（固定值），而不是根据进度动态计算
                    # 获取分析师列表和研究深度
                    analysts = task_params.get("selected_analysts", [])
                    research_depth = task_params.get("research_depth", "快速")
                    llm_model = task_params.get("quick_analysis_model", "dashscope")
                    llm_provider = llm_model.split("-")[0] if llm_model else "dashscope"

                    # 使用 RedisProgressTracker 的内部方法计算基准时间（预估总时长）
                    temp_tracker = RedisProgressTracker(
                        task_id="temp",
                        analysts=analysts,
                        research_depth=research_depth,
                        llm_provider=llm_provider
                    )
                    estimated_total_time = temp_tracker._get_base_total_time()

                    # 预计剩余 = 预估总时长 - 已用时间
                    remaining_time = max(0, estimated_total_time - elapsed_time)

            # 🔑 尝试从 Redis 获取详细的步骤信息
            steps_info = []
            completed_steps = []
            try:
                from app.core.redis_client import get_redis_service, RedisKeys

                redis_service = get_redis_service()
                # 🔥 检查 Redis 是否已初始化
                try:
                    # 尝试访问 redis 属性，如果未初始化会抛出异常
                    _ = redis_service.redis
                except RuntimeError as redis_err:
                    # Redis 未初始化，跳过 Redis 查询，但不影响主流程
                    logger.debug(f"📊 [STATUS] Redis未初始化，跳过步骤信息查询: {redis_err}")
                else:
                    # Redis 已初始化，可以安全查询
                    progress_key = RedisKeys.TASK_PROGRESS.format(task_id=task_id)
                    progress_data = await redis_service.get_json(progress_key)

                    if progress_data and "steps" in progress_data:
                        steps_info = progress_data["steps"]
                        # 提取已完成的步骤名称
                        completed_steps = [
                            step["name"] for step in steps_info
                            if step.get("status") == "completed"
                        ]
                        logger.info(f"📊 [STATUS] 从Redis获取到步骤信息: {len(steps_info)}个步骤, {len(completed_steps)}个已完成")
            except Exception as e:
                # 其他异常（如连接错误等）只记录警告，不影响主流程
                logger.debug(f"📊 [STATUS] 从Redis获取步骤信息失败（不影响主流程）: {e}")

            # 🔥 构造消息：根据状态决定显示内容
            error_message = unified_task.get("error_message")
            if frontend_status == "cancelled":
                # 取消状态：显示取消消息
                display_message = error_message or "任务已被用户取消"
            elif frontend_status == "failed":
                # 失败状态：显示错误消息
                display_message = error_message or "任务执行失败"
            elif frontend_status == "completed":
                # 完成状态
                display_message = "分析完成"
            else:
                # 运行中：显示当前步骤
                display_message = current_step_description or f"任务{frontend_status}中..."

            # 构造状态响应（统一任务格式，兼容前端期望的字段名）
            status_data = {
                "task_id": unified_task.get("task_id"),
                "user_id": str(unified_task.get("user_id", "")),
                "status": frontend_status,  # 使用映射后的状态
                "progress": progress_pct,
                "message": display_message,  # 🔥 使用优化后的消息

                # 🔑 关键：区分步骤名称和描述
                "current_step_name": current_step_name,  # ✅ 简短的步骤名称（如 "市场分析师"）
                "current_step_description": current_step_description,  # ✅ 详细的描述（如 "📈 市场分析师正在分析..."）
                "current_step": current_step_name,  # 保留兼容性

                "start_time": unified_task.get("started_at"),
                "end_time": end_time,
                "elapsed_time": elapsed_time,  # ✅ 使用计算后的值
                "remaining_time": remaining_time,  # ✅ 使用计算后的值
                "estimated_total_time": estimated_total_time,  # ✅ 使用计算后的值
                "symbol": task_params.get("symbol") or task_params.get("stock_code"),
                "stock_code": task_params.get("stock_code") or task_params.get("symbol"),
                "stock_symbol": task_params.get("symbol") or task_params.get("stock_code"),
                "task_type": unified_task.get("task_type"),
                "source": "unified_tasks",  # 标记数据来源

                # 🆕 添加步骤信息
                "steps": steps_info,  # 所有步骤的详细信息
                "completed_steps": completed_steps,  # 已完成的步骤名称列表

                # 添加结果数据（如果有）
                "result_data": unified_task.get("result"),
                "error_message": error_message,  # 🔥 保留原始错误消息
                "parameters": task_params,  # 添加参数信息
                "execution_time": unified_task.get("execution_time"),
                "tokens_used": unified_task.get("tokens_used"),
            }

            # 🔥 格式化 result_data（如果存在）
            if status_data.get("result_data"):
                logger.info(f"📊 [STATUS] ========== unified_tasks result_data 格式化前 ==========")
                logger.info(f"📊 [STATUS] recommendation: {status_data['result_data'].get('recommendation', '')[:200]}")
                logger.info(f"📊 [STATUS] key_points: {status_data['result_data'].get('key_points', [])[:3]}")

                status_data["result_data"] = normalize_result_data(status_data["result_data"])

                logger.info(f"📊 [STATUS] ========== unified_tasks result_data 格式化后 ==========")
                logger.info(f"📊 [STATUS] recommendation: {status_data['result_data'].get('recommendation', '')[:200]}")
                logger.info(f"📊 [STATUS] key_points: {status_data['result_data'].get('key_points', [])[:3]}")

            logger.info(f"📊 [STATUS] 返回状态: status={status_data['status']}, progress={status_data['progress']}, step={status_data['current_step_name']}")

            return {
                "success": True,
                "data": status_data,
                "message": "任务状态获取成功"
            }

        # 然后从analysis_tasks集合中查找（旧版任务）
        task_result = await db.analysis_tasks.find_one({"task_id": task_id})

        if task_result:
                logger.info(f"✅ [STATUS] 从analysis_tasks找到任务: {task_id}")

                # 构造状态响应（正在进行的任务）
                status = task_result.get("status", "pending")
                progress = task_result.get("progress", 0)
                current_step = task_result.get("current_step", "")
                message = task_result.get("message", f"任务{status}中...")

                # 计算时间信息
                from datetime import datetime
                start_time = task_result.get("started_at") or task_result.get("created_at")

                # 如果是字符串，转换为 datetime
                if isinstance(start_time, str):
                    try:
                        from dateutil import parser
                        start_time = parser.parse(start_time)
                    except Exception as e:
                        logger.warning(f"⚠️ 无法解析开始时间: {start_time}, 错误: {e}")
                        start_time = None

                current_time = datetime.utcnow()
                elapsed_time = 0
                estimated_total_time = task_result.get("estimated_total_time", 225.0)  # 默认225秒
                remaining_time = 0

                if start_time:
                    # 确保 start_time 是 naive datetime（移除时区信息）
                    if start_time.tzinfo is not None:
                        start_time = start_time.replace(tzinfo=None)
                    elapsed_time = (current_time - start_time).total_seconds()

                    # 根据进度估算剩余时间
                    if progress > 0 and progress < 100:
                        remaining_time = max(0, estimated_total_time - elapsed_time)

                # 🔥 状态映射：processing → running（前端期望 running）
                frontend_status = "running" if status == "processing" else status

                # 获取参数信息
                parameters = task_result.get("parameters", {})

                status_data = {
                    "task_id": task_id,
                    "user_id": str(task_result.get("user_id", "")),
                    "status": frontend_status,
                    "progress": progress,
                    "message": message,
                    "current_step_name": current_step,
                    "current_step_description": message,
                    "current_step": current_step,
                    "start_time": start_time,
                    "end_time": task_result.get("completed_at"),
                    "elapsed_time": elapsed_time,
                    "remaining_time": remaining_time,
                    "estimated_total_time": estimated_total_time,
                    "symbol": task_result.get("symbol") or task_result.get("stock_code"),
                    "stock_code": task_result.get("symbol") or task_result.get("stock_code"),
                    "stock_symbol": task_result.get("symbol") or task_result.get("stock_code"),
                    "task_type": "stock_analysis",
                    "source": "analysis_tasks",  # 标记数据来源
                    "steps": [],
                    "completed_steps": [],
                    "result_data": None,
                    "error_message": task_result.get("error_message"),
                    "parameters": parameters,
                    "execution_time": task_result.get("execution_time", 0.0),
                    "tokens_used": task_result.get("tokens_used", 0)
                }

                return {
                    "success": True,
                    "data": status_data,
                    "message": "任务状态获取成功"
                }

        # 如果analysis_tasks中没有找到，再从analysis_reports集合中查找（已完成的任务）
        mongo_result = await db.analysis_reports.find_one({"task_id": task_id})

        if mongo_result:
            logger.info(f"✅ [STATUS] 从analysis_reports找到任务: {task_id}")

            # 构造状态响应（模拟已完成的任务）
            # 计算已完成任务的时间信息
            start_time = mongo_result.get("created_at")
            end_time = mongo_result.get("updated_at")
            elapsed_time = 0
            if start_time and end_time:
                elapsed_time = (end_time - start_time).total_seconds()

            status_data = {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "分析完成（从历史记录恢复）",
                "current_step": "completed",
                "start_time": start_time,
                "end_time": end_time,
                "elapsed_time": elapsed_time,
                "remaining_time": 0,
                "estimated_total_time": elapsed_time,  # 已完成任务的总时长就是已用时间
                "stock_code": mongo_result.get("stock_symbol"),
                "stock_symbol": mongo_result.get("stock_symbol"),
                "analysts": mongo_result.get("analysts", []),
                "research_depth": mongo_result.get("research_depth", "快速"),
                "source": "mongodb_reports"  # 标记数据来源
            }

            return {
                "success": True,
                "data": status_data,
                "message": "任务状态获取成功（从历史记录恢复）"
            }
        else:
            logger.warning(f"❌ [STATUS] MongoDB中也未找到: {task_id} trace={task_id}")
            raise HTTPException(status_code=404, detail="任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 获取任务状态失败: {e}")
        raise HTTPException(status_code=500, detail=format_http_error_detail(e))

@router.get("/tasks/{task_id}/result", response_model=Dict[str, Any])
async def get_task_result(
    task_id: str,
    include_state: bool = Query(False, description="是否包含完整工作流状态（调试用）"),
    include_detailed: bool = Query(False, description="是否包含详细分析数据（调试用）"),
    user: dict = Depends(get_current_user)
):
    """获取分析任务结果
    
    默认只返回必要字段（reports, decision, summary等），不包含 state 和 detailed_analysis。
    如需完整数据，可通过查询参数 include_state=true&include_detailed=true 获取。
    """
    try:
        logger.info(f"🔍 [RESULT] 获取任务结果: {task_id}")
        logger.info(f"👤 [RESULT] 用户: {user}")

        analysis_service = get_simple_analysis_service()
        task_status = await analysis_service.get_task_status(task_id)

        result_data = None

        if task_status and task_status.get('status') == 'completed':
            # 从内存中获取结果数据
            result_data = task_status.get('result_data')
            logger.info(f"📊 [RESULT] 从内存中获取到结果数据")

            # 🔍 调试：检查内存中的数据结构
            if result_data:
                logger.info(f"📊 [RESULT] ========== 格式化前 ==========")
                logger.info(f"📊 [RESULT] 内存数据键: {list(result_data.keys())}")
                logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")
                if result_data.get('decision'):
                    decision = result_data['decision']
                    logger.info(f"📊 [RESULT] decision: action={decision.get('action')}, analysis_view={decision.get('analysis_view')}, target_price={decision.get('target_price')}, price_analysis_range={decision.get('price_analysis_range')}")

                # 🔥 统一格式化结果数据
                result_data = normalize_result_data(result_data)

                completeness = _log_result_completeness("memory", task_id, result_data)

                logger.info(f"📊 [RESULT] ========== 格式化后 ==========")
                logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")

                if not completeness["is_complete"]:
                    logger.warning(f"⚠️ [RESULT] 内存中的 result_data 不完整，继续回退查询持久化结果: {task_id}")
                    result_data = None
            else:
                logger.warning(f"⚠️ [RESULT] 内存中result_data为空")

        if not result_data:
            # 内存中没有找到，尝试从MongoDB中查找
            logger.info(f"📊 [RESULT] 内存中未找到，尝试从MongoDB查找: {task_id}")

            from app.core.database import get_mongo_db
            db = get_mongo_db()

            # 🔧 首先从 unified_analysis_tasks 集合中查找（v2 引擎任务）
            unified_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
            if unified_task and unified_task.get("result"):
                logger.info(f"✅ [RESULT] 从unified_analysis_tasks找到结果: {task_id}")

                result = unified_task["result"]
                task_params = unified_task.get("task_params", {})

                # 计算执行时间
                execution_time = 0
                completed_at = unified_task.get("completed_at")
                started_at = unified_task.get("started_at")
                if completed_at and started_at:
                    try:
                        # 如果是 datetime 对象，直接相减
                        if hasattr(completed_at, 'timestamp') and hasattr(started_at, 'timestamp'):
                            execution_time = (completed_at - started_at).total_seconds()
                        # 如果是字符串，先解析
                        elif isinstance(completed_at, str) and isinstance(started_at, str):
                            from dateutil import parser
                            completed_dt = parser.parse(completed_at)
                            started_dt = parser.parse(started_at)
                            execution_time = (completed_dt - started_dt).total_seconds()
                    except Exception as e:
                        logger.warning(f"⚠️ 计算执行时间失败: {e}")
                        execution_time = 0

                # 构造结果数据（兼容前端期望的格式）
                result_data = {
                    "analysis_id": unified_task.get("task_id"),  # 使用 task_id 作为 analysis_id
                    "stock_symbol": task_params.get("symbol") or task_params.get("stock_code"),
                    "stock_code": task_params.get("stock_code") or task_params.get("symbol"),
                    "analysis_date": task_params.get("analysis_date"),
                    "summary": result.get("summary", ""),
                    "recommendation": result.get("recommendation", ""),
                    "confidence_score": result.get("confidence_score", 0.0),
                    "risk_level": result.get("risk_level", "中等"),
                    "key_points": result.get("key_points", []),
                    "execution_time": execution_time,
                    "tokens_used": result.get("tokens_used", 0),
                    "analysts": task_params.get("selected_analysts", []),
                    "research_depth": task_params.get("research_depth", "快速"),
                    "reports": result.get("reports", {}),
                    "created_at": unified_task.get("created_at"),
                    "updated_at": unified_task.get("completed_at"),
                    "status": "completed",
                    "decision": result.get("decision", {}),
                    "source": "unified_tasks"  # 标记数据来源
                }
                
                # 🔥 可选字段：根据查询参数决定是否包含
                if include_state:
                    result_data["state"] = result.get("state", {})
                if include_detailed:
                    result_data["detailed_analysis"] = result.get("detailed_analysis", {})

                logger.info(f"📊 [RESULT] ========== unified_tasks 格式化前 ==========")
                logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")
                if result_data.get('decision'):
                    decision = result_data['decision']
                    logger.info(f"📊 [RESULT] decision: action={decision.get('action')}, analysis_view={decision.get('analysis_view')}, target_price={decision.get('target_price')}, price_analysis_range={decision.get('price_analysis_range')}")

                # 🔥 统一格式化结果数据
                result_data = normalize_result_data(result_data)
                completeness = _log_result_completeness("unified_tasks", task_id, result_data)

                logger.info(f"📊 [RESULT] ========== unified_tasks 格式化后 ==========")
                logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")

                # 🔑 防御性检查：如果 unified_analysis_tasks 数据不完整，
                # 回退到 analysis_reports（可能已经先保存了完整数据）
                if not completeness["is_complete"]:
                    logger.warning(f"⚠️ [RESULT] unified_analysis_tasks 数据不完整，回退查询 analysis_reports: {task_id}")
                    result_data = None

            # 如果 unified_analysis_tasks 中没有找到或数据不完整，再从 analysis_reports 集合中查找
            if not result_data:
                mongo_result = await db.analysis_reports.find_one({"task_id": task_id})

                if not mongo_result:
                    # 兼容旧数据：旧记录可能没有 task_id，但 analysis_id 存在于 analysis_tasks.result
                    tasks_doc_for_id = await db.analysis_tasks.find_one({"task_id": task_id}, {"result.analysis_id": 1})
                    analysis_id = tasks_doc_for_id.get("result", {}).get("analysis_id") if tasks_doc_for_id else None
                    if analysis_id:
                        logger.info(f"🔎 [RESULT] 按analysis_id兜底查询 analysis_reports: {analysis_id}")
                        mongo_result = await db.analysis_reports.find_one({"analysis_id": analysis_id})
            else:
                # 如果从 unified_analysis_tasks 找到了结果，就不需要查询 analysis_reports
                mongo_result = None

            if mongo_result:
                logger.info(f"✅ [RESULT] 从MongoDB找到结果: {task_id}")

                # 直接使用MongoDB中的数据结构（与web目录保持一致）
                result_data = {
                    "analysis_id": mongo_result.get("analysis_id"),
                    "stock_symbol": mongo_result.get("stock_symbol"),
                    "stock_code": mongo_result.get("stock_symbol"),  # 兼容性
                    "analysis_date": mongo_result.get("analysis_date"),
                    "summary": mongo_result.get("summary", ""),
                    "recommendation": mongo_result.get("recommendation", ""),
                    "confidence_score": mongo_result.get("confidence_score", 0.0),
                    "risk_level": mongo_result.get("risk_level", "中等"),
                    "key_points": mongo_result.get("key_points", []),
                    "execution_time": mongo_result.get("execution_time", 0),
                    "tokens_used": mongo_result.get("tokens_used", 0),
                    "analysts": mongo_result.get("analysts", []),
                    "research_depth": mongo_result.get("research_depth", "快速"),
                    "reports": mongo_result.get("reports", {}),
                    "created_at": mongo_result.get("created_at"),
                    "updated_at": mongo_result.get("updated_at"),
                    "status": mongo_result.get("status", "completed"),
                    "decision": mongo_result.get("decision", {}),
                    "source": "mongodb"  # 标记数据来源
                }

                logger.info(f"📊 [RESULT] ========== MongoDB 格式化前 ==========")
                logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")
                if result_data.get('decision'):
                    decision = result_data['decision']
                    logger.info(f"📊 [RESULT] decision: action={decision.get('action')}, analysis_view={decision.get('analysis_view')}")

                # 🔥 统一格式化结果数据
                result_data = normalize_result_data(result_data)
                _log_result_completeness("mongodb", task_id, result_data)

                logger.info(f"📊 [RESULT] ========== MongoDB 格式化后 ==========")
                logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")
            else:
                # 兜底：analysis_tasks 集合中的 result 字段
                tasks_doc = await db.analysis_tasks.find_one(
                    {"task_id": task_id},
                    {"result": 1, "symbol": 1, "stock_code": 1, "created_at": 1, "completed_at": 1}
                )
                if tasks_doc and tasks_doc.get("result"):
                    r = tasks_doc["result"] or {}
                    logger.info("✅ [RESULT] 从analysis_tasks.result 找到结果")
                    # 获取股票代码 (优先使用symbol)
                    symbol = (tasks_doc.get("symbol") or tasks_doc.get("stock_code") or
                             r.get("stock_symbol") or r.get("stock_code"))
                    # 🔥 清理 reports 中的重复数据（如果存在）
                    reports_raw = r.get("reports", {})
                    cleaned_reports = {}
                    if isinstance(reports_raw, dict):
                        for key, value in reports_raw.items():
                            # 跳过 structured_reports（如果存在，避免重复）
                            if key != "structured_reports":
                                cleaned_reports[key] = value
                    
                    result_data = {
                        "analysis_id": r.get("analysis_id"),
                        "stock_symbol": symbol,
                        "stock_code": symbol,  # 兼容字段
                        "analysis_date": r.get("analysis_date"),
                        "summary": r.get("summary", ""),
                        "recommendation": r.get("recommendation", ""),
                        "confidence_score": r.get("confidence_score", 0.0),
                        "risk_level": r.get("risk_level", "中等"),
                        "key_points": r.get("key_points", []),
                        "execution_time": r.get("execution_time", 0),
                        "tokens_used": r.get("tokens_used", 0),
                        "analysts": r.get("analysts", []),
                        "research_depth": r.get("research_depth", "快速"),
                        "reports": cleaned_reports,  # 🔥 使用清理后的 reports
                        "created_at": tasks_doc.get("created_at"),
                        "updated_at": tasks_doc.get("completed_at"),
                        "status": r.get("status", "completed"),
                        "decision": r.get("decision", {}),
                        "source": "analysis_tasks"  # 数据来源标记
                    }
                    
                    # 🔥 可选字段：根据查询参数决定是否包含
                    if include_state:
                        result_data["state"] = r.get("state", {})
                    if include_detailed:
                        result_data["detailed_analysis"] = r.get("detailed_analysis", {})

                    logger.info(f"📊 [RESULT] ========== analysis_tasks 格式化前 ==========")
                    logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                    logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")
                    if result_data.get('decision'):
                        decision = result_data['decision']
                        logger.info(f"📊 [RESULT] decision: action={decision.get('action')}, analysis_view={decision.get('analysis_view')}")

                    # 🔥 统一格式化结果数据
                    result_data = normalize_result_data(result_data)
                    _log_result_completeness("analysis_tasks", task_id, result_data)

                    logger.info(f"📊 [RESULT] ========== analysis_tasks 格式化后 ==========")
                    logger.info(f"📊 [RESULT] recommendation: {result_data.get('recommendation', '')[:200]}")
                    logger.info(f"📊 [RESULT] key_points: {result_data.get('key_points', [])[:3]}")

        if not result_data:
            logger.warning(f"❌ [RESULT] 所有数据源都未找到结果: {task_id}")
            raise HTTPException(status_code=404, detail="分析结果不存在")

        if not result_data:
            raise HTTPException(status_code=404, detail="分析结果不存在")

        # 处理reports字段 - 如果没有reports字段，优先尝试从文件系统加载，其次从state中提取
        if 'reports' not in result_data or not result_data['reports']:
            import os
            from pathlib import Path

            stock_symbol = result_data.get('stock_symbol') or result_data.get('stock_code')
            # analysis_date 可能是日期或时间戳字符串，这里只取日期部分
            analysis_date_raw = result_data.get('analysis_date')
            analysis_date = str(analysis_date_raw)[:10] if analysis_date_raw else None

            loaded_reports = {}
            try:
                # 1) 尝试从环境变量 TRADINGAGENTS_RESULTS_DIR 指定的位置读取
                base_env = os.getenv('TRADINGAGENTS_RESULTS_DIR')
                project_root = Path.cwd()
                if base_env:
                    base_path = Path(base_env)
                    if not base_path.is_absolute():
                        base_path = project_root / base_env
                else:
                    base_path = project_root / 'results'

                candidate_dirs = []
                if stock_symbol and analysis_date:
                    candidate_dirs.append(base_path / stock_symbol / analysis_date / 'reports')
                # 2) 兼容其他保存路径
                if stock_symbol and analysis_date:
                    candidate_dirs.append(project_root / 'data' / 'analysis_results' / stock_symbol / analysis_date / 'reports')
                    candidate_dirs.append(project_root / 'data' / 'analysis_results' / 'detailed' / stock_symbol / analysis_date / 'reports')

                for d in candidate_dirs:
                    if d.exists() and d.is_dir():
                        for f in d.glob('*.md'):
                            try:
                                content = f.read_text(encoding='utf-8')
                                if content and content.strip():
                                    loaded_reports[f.stem] = content.strip()
                            except Exception:
                                pass
                if loaded_reports:
                    result_data['reports'] = loaded_reports
                    # 若 summary / recommendation 缺失，尝试从同名报告补全
                    if not result_data.get('summary') and loaded_reports.get('summary'):
                        result_data['summary'] = loaded_reports.get('summary')
                    if not result_data.get('recommendation') and loaded_reports.get('recommendation'):
                        result_data['recommendation'] = loaded_reports.get('recommendation')
                    logger.info(f"📁 [RESULT] 从文件系统加载到 {len(loaded_reports)} 个报告: {list(loaded_reports.keys())}")
            except Exception as fs_err:
                logger.warning(f"⚠️ [RESULT] 从文件系统加载报告失败: {fs_err}")

            if 'reports' not in result_data or not result_data['reports']:
                logger.info(f"📊 [RESULT] reports字段缺失，尝试从state中提取")

                # 从state中提取报告内容
                reports = {}
                state = result_data.get('state', {})

                if isinstance(state, dict):
                    # 优先从引擎产出的 structured_reports 动态提取报告字段
                    structured_reports = state.get('structured_reports', {})
                    if isinstance(structured_reports, dict) and structured_reports:
                        report_fields = [
                            field for field, entry in structured_reports.items()
                            if isinstance(entry, dict) and entry.get("success") and entry.get("content")
                        ]
                    else:
                        # 回退：旧版硬编码列表
                        report_fields = [
                            'market_report',
                            'sentiment_report',
                            'news_report',
                            'fundamentals_report',
                            'chip_report',
                            'investment_plan',
                            'trader_investment_plan',
                            'final_trade_decision'
                        ]

                    # 从state中提取报告内容
                    for field in report_fields:
                        value = state.get(field, "")
                        if isinstance(value, str) and len(value.strip()) > 10:
                            reports[field] = value.strip()

                    # 处理研究团队辩论状态报告
                    investment_debate_state = state.get('investment_debate_state', {})
                    if isinstance(investment_debate_state, dict):
                        # 提取多头研究员历史
                        bull_content = investment_debate_state.get('bull_history', "")
                        if isinstance(bull_content, str) and len(bull_content.strip()) > 10:
                            reports['bull_researcher'] = bull_content.strip()

                        # 提取空头研究员历史
                        bear_content = investment_debate_state.get('bear_history', "")
                        if isinstance(bear_content, str) and len(bear_content.strip()) > 10:
                            reports['bear_researcher'] = bear_content.strip()

                        # 提取研究经理分析
                        judge_decision = investment_debate_state.get('judge_decision', "")
                        if isinstance(judge_decision, str) and len(judge_decision.strip()) > 10:
                            reports['research_team_decision'] = judge_decision.strip()

                    # 处理风险管理团队辩论状态报告
                    risk_debate_state = state.get('risk_debate_state', {})
                    if isinstance(risk_debate_state, dict):
                        # 提取激进分析师历史
                        risky_content = risk_debate_state.get('risky_history', "")
                        if isinstance(risky_content, str) and len(risky_content.strip()) > 10:
                            reports['risky_analyst'] = risky_content.strip()

                        # 提取保守分析师历史
                        safe_content = risk_debate_state.get('safe_history', "")
                        if isinstance(safe_content, str) and len(safe_content.strip()) > 10:
                            reports['safe_analyst'] = safe_content.strip()

                        # 提取中性分析师历史
                        neutral_content = risk_debate_state.get('neutral_history', "")
                        if isinstance(neutral_content, str) and len(neutral_content.strip()) > 10:
                            reports['neutral_analyst'] = neutral_content.strip()

                        # 提取风险评估师决策
                        risk_decision = risk_debate_state.get('judge_decision', "")
                        if isinstance(risk_decision, str) and len(risk_decision.strip()) > 10:
                            reports['risk_management_decision'] = risk_decision.strip()

                    logger.info(f"📊 [RESULT] 从state中提取到 {len(reports)} 个报告: {list(reports.keys())}")
                    result_data['reports'] = reports
                else:
                    logger.warning(f"⚠️ [RESULT] state字段不是字典类型: {type(state)}")

        # 确保reports字段中的所有内容都是字符串类型
        if 'reports' in result_data and result_data['reports']:
            reports = result_data['reports']
            if isinstance(reports, dict):
                # 确保每个报告内容都是字符串且不为空
                cleaned_reports = {}
                for key, value in reports.items():
                    if isinstance(value, str) and value.strip():
                        # 确保字符串不为空
                        cleaned_reports[key] = value.strip()
                    elif value is not None:
                        # 如果不是字符串，转换为字符串
                        str_value = str(value).strip()
                        if str_value:  # 只保存非空字符串
                            cleaned_reports[key] = str_value
                    # 如果value为None或空字符串，则跳过该报告

                result_data['reports'] = cleaned_reports
                logger.info(f"📊 [RESULT] 清理reports字段，包含 {len(cleaned_reports)} 个有效报告")

                # 如果清理后没有有效报告，设置为空字典
                if not cleaned_reports:
                    logger.warning(f"⚠️ [RESULT] 清理后没有有效报告")
                    result_data['reports'] = {}
            else:
                logger.warning(f"⚠️ [RESULT] reports字段不是字典类型: {type(reports)}")
                result_data['reports'] = {}

        # 补全关键字段：recommendation/summary/key_points
        try:
            reports = result_data.get('reports', {}) or {}
            decision = result_data.get('decision', {}) or {}

            # recommendation 优先使用决策摘要或报告中的决策
            if not result_data.get('recommendation'):
                rec_candidates = []
                if isinstance(decision, dict):
                    # 🔥 合规修改：支持新旧字段名
                    action = decision.get('analysis_view') or decision.get('action')
                    price_range = decision.get('price_analysis_range') or decision.get('target_price')
                    confidence = decision.get('confidence')

                    if action:
                        parts = [
                            f"分析观点: {action}",
                            f"价格分析区间: {price_range}" if price_range else None,
                            f"置信度: {confidence}" if confidence is not None else None
                        ]
                        rec_candidates.append("；".join([p for p in parts if p]))
                # 从报告中兜底
                for k in ['final_trade_decision', 'investment_plan']:
                    v = reports.get(k)
                    if isinstance(v, str) and len(v.strip()) > 10:
                        rec_candidates.append(v.strip())
                if rec_candidates:
                    # 取最有信息量的一条（最长）
                    result_data['recommendation'] = max(rec_candidates, key=len)[:2000]

            # summary 从若干报告拼接生成
            if not result_data.get('summary'):
                sum_candidates = []
                for k in ['market_report', 'fundamentals_report', 'sentiment_report', 'news_report']:
                    v = reports.get(k)
                    if isinstance(v, str) and len(v.strip()) > 50:
                        sum_candidates.append(v.strip())
                if sum_candidates:
                    result_data['summary'] = ("\n\n".join(sum_candidates))[:3000]

            # key_points 兜底
            if not result_data.get('key_points'):
                kp = []
                if isinstance(decision, dict):
                    # 🔥 合规修改：支持新旧字段名和新术语
                    action = decision.get('analysis_view') or decision.get('action')
                    price_range = decision.get('price_analysis_range') or decision.get('target_price')
                    confidence = decision.get('confidence')
                    risk_level = decision.get('risk_level')

                    if action:
                        kp.append(f"分析观点: {action}")
                    if price_range:
                        # 处理价格区间（可能是列表或单个值）
                        if isinstance(price_range, (list, tuple)) and len(price_range) == 2:
                            kp.append(f"价格分析区间: {price_range[0]}-{price_range[1]}元")
                        else:
                            kp.append(f"参考价格: {price_range}元")
                    if confidence is not None:
                        kp.append(f"置信度: {confidence}")
                    if risk_level:
                        kp.append(f"风险等级: {risk_level}")
                # 从reports中截取前几句作为要点
                for k in ['investment_plan', 'final_trade_decision']:
                    v = reports.get(k)
                    if isinstance(v, str) and len(v.strip()) > 10:
                        kp.append(v.strip()[:120])
                if kp:
                    result_data['key_points'] = kp[:5]
        except Exception as fill_err:
            logger.warning(f"⚠️ [RESULT] 补全关键字段时出错: {fill_err}")


        # 进一步兜底：从 detailed_analysis 推断并补全
        try:
            if not result_data.get('summary') or not result_data.get('recommendation') or not result_data.get('reports'):
                da = result_data.get('detailed_analysis')
                # 若reports仍为空，放入一份原始详细分析，便于前端“查看报告详情”
                if (not result_data.get('reports')) and isinstance(da, str) and len(da.strip()) > 20:
                    result_data['reports'] = {'detailed_analysis': da.strip()}
                elif (not result_data.get('reports')) and isinstance(da, dict) and da:
                    # 将字典的长文本项放入reports
                    extracted = {}
                    for k, v in da.items():
                        if isinstance(v, str) and len(v.strip()) > 20:
                            extracted[k] = v.strip()
                    if extracted:
                        result_data['reports'] = extracted

                # 补 summary
                if not result_data.get('summary'):
                    if isinstance(da, str) and da.strip():
                        result_data['summary'] = da.strip()[:3000]
                    elif isinstance(da, dict) and da:
                        # 取最长的文本作为摘要
                        texts = [v.strip() for v in da.values() if isinstance(v, str) and v.strip()]
                        if texts:
                            result_data['summary'] = max(texts, key=len)[:3000]

                # 补 recommendation
                if not result_data.get('recommendation'):
                    rec = None
                    if isinstance(da, str):
                        # 简单基于关键字提取包含“建议”的段落
                        import re
                        m = re.search(r'(投资建议|建议|结论)[:：]?\s*(.+)', da)
                        if m:
                            rec = m.group(0)
                    elif isinstance(da, dict):
                        for key in ['final_trade_decision', 'investment_plan', '结论', '建议']:
                            v = da.get(key)
                            if isinstance(v, str) and len(v.strip()) > 10:
                                rec = v.strip()
                                break
                    if rec:
                        result_data['recommendation'] = rec[:2000]
        except Exception as da_err:
            logger.warning(f"⚠️ [RESULT] 从detailed_analysis补全失败: {da_err}")

        # 严格的数据格式化和验证
        def safe_string(value, default=""):
            """安全地转换为字符串"""
            if value is None:
                return default
            if isinstance(value, str):
                return value
            return str(value)

        def safe_number(value, default=0):
            """安全地转换为数字"""
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return value
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        def safe_list(value, default=None):
            """安全地转换为列表"""
            if default is None:
                default = []
            if value is None:
                return default
            if isinstance(value, list):
                return value
            return default

        def safe_dict(value, default=None):
            """安全地转换为字典"""
            if default is None:
                default = {}
            if value is None:
                return default
            if isinstance(value, dict):
                return value
            return default

        # 🔍 调试：检查最终构建前的result_data
        logger.info(f"🔍 [FINAL] 构建最终结果前，result_data键: {list(result_data.keys())}")
        logger.info(f"🔍 [FINAL] result_data中有decision: {bool(result_data.get('decision'))}")
        if result_data.get('decision'):
            logger.info(f"🔍 [FINAL] decision内容: {result_data['decision']}")

        # 构建严格验证的结果数据（基础字段，必须返回）
        final_result_data = {
            "analysis_id": safe_string(result_data.get("analysis_id"), "unknown"),
            "stock_symbol": safe_string(result_data.get("stock_symbol"), "UNKNOWN"),
            "stock_code": safe_string(result_data.get("stock_code"), "UNKNOWN"),
            "analysis_date": safe_string(result_data.get("analysis_date"), "2025-08-20"),
            "source": safe_string(result_data.get("source"), "unknown"),
            "summary": safe_string(result_data.get("summary"), "分析摘要暂无"),
            "recommendation": safe_string(result_data.get("recommendation"), "投资建议暂无"),
            "confidence_score": safe_number(result_data.get("confidence_score"), 0.0),
            "risk_level": safe_string(result_data.get("risk_level"), "中等"),
            "key_points": safe_list(result_data.get("key_points")),
            "execution_time": safe_number(result_data.get("execution_time"), 0),
            "tokens_used": safe_number(result_data.get("tokens_used"), 0),
            "analysts": safe_list(result_data.get("analysts")),
            "research_depth": safe_string(result_data.get("research_depth"), "快速"),
            # 🔥 关键修复：添加decision字段！
            "decision": safe_dict(result_data.get("decision"))
        }
        
        # 🔥 可选字段：根据查询参数决定是否返回
        if include_state:
            final_result_data["state"] = safe_dict(result_data.get("state"))
        if include_detailed:
            final_result_data["detailed_analysis"] = safe_dict(result_data.get("detailed_analysis"))

        # 特别处理reports字段 - 确保每个报告都是有效字符串
        reports_data = safe_dict(result_data.get("reports"))
        validated_reports = {}

        for report_key, report_content in reports_data.items():
            # 确保报告键是字符串
            safe_key = safe_string(report_key, "unknown_report")

            # 确保报告内容是非空字符串
            if report_content is None:
                validated_content = "报告内容暂无"
            elif isinstance(report_content, str):
                validated_content = report_content.strip() if report_content.strip() else "报告内容为空"
            else:
                validated_content = str(report_content).strip() if str(report_content).strip() else "报告内容格式错误"

            validated_reports[safe_key] = validated_content

        final_result_data["reports"] = validated_reports

        logger.info(f"✅ [RESULT] 成功获取任务结果: {task_id}")
        logger.info(f"📊 [RESULT] 最终返回 {len(final_result_data.get('reports', {}))} 个报告")
        logger.info(f"📊 [RESULT] 报告列表: {list(validated_reports.keys())}")

        # 🔍 调试：检查最终返回的数据
        logger.info(f"🔍 [FINAL] 最终返回数据键: {list(final_result_data.keys())}")
        logger.info(f"🔍 [FINAL] 最终返回中有decision: {bool(final_result_data.get('decision'))}")
        if final_result_data.get('decision'):
            logger.info(f"🔍 [FINAL] 最终decision内容: {final_result_data['decision']}")

        return {
            "success": True,
            "data": final_result_data,
            "message": "分析结果获取成功"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [RESULT] 获取任务结果失败: {e}")
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))


class AnalysisFeedbackRequest(BaseModel):
    """分析任务反馈请求"""
    rating: int = Field(..., ge=1, le=5, description="评分 1-5")
    comment: str = Field(default="", description="反馈内容")


@router.post("/tasks/{task_id}/feedback")
async def submit_analysis_feedback(
    task_id: str,
    req: AnalysisFeedbackRequest,
    user: dict = Depends(get_current_user),
):
    """提交分析任务反馈（评分+评论，兼容 v2 unified_analysis_tasks 与旧表 analysis_tasks）"""
    try:
        db = get_mongo_db()
        # 优先查询 v2 主集合 unified_analysis_tasks，再回退旧集合 analysis_tasks
        unified_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
        legacy_task = await db.analysis_tasks.find_one({"task_id": task_id})
        task = unified_task or legacy_task
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        owner_id = task.get("user_id", task.get("user"))
        if owner_id is not None and not _is_task_owner(owner_id, user):
            raise HTTPException(status_code=403, detail="无权对此任务提交反馈")

        task_code = _extract_doc_symbol(task) or task.get("code", "")

        # 写入反馈
        feedback_doc = {
            "task_id": task_id,
            "user_id": str(user["id"]),
            "rating": req.rating,
            "comment": req.comment,
            "code": task_code,
            "created_at": now_tz(),
        }
        await db.analysis_feedback.replace_one(
            {"task_id": task_id, "user_id": str(user["id"])},
            feedback_doc,
            upsert=True,
        )

        # 同步更新任务中的评分字段
        feedback_update = {"$set": {"user_rating": req.rating, "user_feedback": req.comment, "updated_at": now_tz()}}
        if unified_task:
            await db.unified_analysis_tasks.update_one({"task_id": task_id}, feedback_update)
        if legacy_task:
            await db.analysis_tasks.update_one({"task_id": task_id}, feedback_update)

        return {"success": True, "message": "反馈提交成功", "rating": req.rating}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 提交分析反馈失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/all", response_model=Dict[str, Any])
async def list_all_tasks(
    user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="任务状态过滤"),
    limit: Optional[int] = Query(None, description="返回数量限制（可选，不设置则返回所有）"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """获取所有任务列表（不限用户）

    注意：如果不指定 limit，将返回所有记录，由前端处理分页
    """
    try:
        logger.info(f"📋 查询所有任务列表")

        # 如果没有指定 limit，则返回所有记录
        actual_limit = limit if limit is not None else 999999

        tasks = await get_simple_analysis_service().list_all_tasks(
            status=status,
            limit=actual_limit,
            offset=offset
        )

        return {
            "success": True,
            "data": {
                "tasks": tasks,
                "total": len(tasks),
                "limit": limit,
                "offset": offset
            },
            "message": "任务列表获取成功"
        }

    except Exception as e:
        logger.error(f"❌ 获取任务列表失败: {e}")
        raise HTTPException(status_code=500, detail=format_http_error_detail(e))

@router.get("/tasks", response_model=Dict[str, Any])
async def list_user_tasks(
    user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="任务状态过滤"),
    limit: Optional[int] = Query(None, description="返回数量限制（可选，不设置则返回所有）"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """获取用户的任务列表

    注意：如果不指定 limit，将返回所有记录，由前端处理分页
    """
    try:
        logger.info(f"📋 查询用户任务列表: {user['id']}")

        # 如果没有指定 limit，则返回所有记录
        actual_limit = limit if limit is not None else 999999

        tasks = await get_simple_analysis_service().list_user_tasks(
            user_id=user["id"],
            status=status,
            limit=actual_limit,
            offset=offset
        )

        return {
            "success": True,
            "data": {
                "tasks": tasks,
                "total": len(tasks),
                "limit": limit,
                "offset": offset
            },
            "message": "任务列表获取成功"
        }

    except Exception as e:
        logger.error(f"❌ 获取任务列表失败: {e}")
        raise HTTPException(status_code=500, detail=format_http_error_detail(e))

@router.post("/batch", response_model=Dict[str, Any])
async def submit_batch_analysis(
    request: BatchAnalysisRequest,
    user: dict = Depends(get_current_user)
):
    """
    提交批量分析任务（真正的并发执行）- 支持 AB 测试

    通过 request.parameters.engine 参数选择引擎:
    - v2 (默认): 使用新引擎 WorkflowEngine + v2 agent
    - unified: 使用统一引擎 WorkflowEngine
    - legacy: 已停用（v3.0 起封堵，返回 400）

    ⚠️ 注意：不使用 BackgroundTasks，因为它是串行执行的！
    改用 asyncio.create_task 实现真正的并发执行。
    """
    try:
        logger.info(f"🎯 [批量分析] 收到批量分析请求: title={request.title}")

        # 判断使用哪个引擎
        # v3.0 起默认使用 V2 引擎（旧版 legacy 已停用）
        engine_type = AnalysisEngine.V2
        if request.parameters and hasattr(request.parameters, "engine"):
            engine_type = request.parameters.engine
            # v3.0 起拒绝 legacy 引擎
            if engine_type == AnalysisEngine.LEGACY:
                raise HTTPException(
                    status_code=400,
                    detail="[已停用] legacy 引擎已停用，请改用 v2 引擎（request.parameters.engine='v2'）。"
                    "详见 docs/99-archive/deprecated-features/legacy-agents-deprecation.md"
                )

        logger.info(f"🔧 [AB测试-批量] 使用引擎: {engine_type.value}")

        simple_service = get_simple_analysis_service()
        unified_service = get_unified_analysis_service()
        batch_id = str(uuid.uuid4())
        task_ids: List[str] = []
        mapping: List[Dict[str, str]] = []

        # 获取股票代码列表 (兼容旧字段)
        stock_symbols = request.get_symbols()
        logger.info(f"📊 [批量分析] 股票代码列表: {stock_symbols}")

        # 验证股票代码列表
        if not stock_symbols:
            raise ValueError("股票代码列表不能为空")

        # 🔧 限制批量分析的股票数量（最多10个）
        MAX_BATCH_SIZE = 10
        if len(stock_symbols) > MAX_BATCH_SIZE:
            raise ValueError(f"批量分析最多支持 {MAX_BATCH_SIZE} 个股票，当前提交了 {len(stock_symbols)} 个")

        # 为每只标的创建单股/ETF 分析任务（按标的自动区分 ETF_ANALYSIS / STOCK_ANALYSIS）
        created_entries: List[Dict[str, Any]] = []
        preference_type = await get_user_risk_preference(user["id"])
        from app.services.memory_state_manager import get_memory_state_manager, TaskStatus
        memory_manager = get_memory_state_manager()

        for i, raw_symbol in enumerate(stock_symbols):
            logger.info(f"📝 [批量分析] 正在创建第 {i+1}/{len(stock_symbols)} 个任务: {raw_symbol}")
            user_market_raw = (request.parameters and getattr(request.parameters, "market_type", None)) or None
            # 批量混选 A股+ETF 时，单个标的是否为 ETF 以代码识别为准；若用户显式选了 ETF 则尊重
            symbol, is_etf, resolved_market = _resolve_symbol_and_etf_routing(
                raw_symbol,
                user_market_raw if len(stock_symbols) == 1 else (
                    "ETF" if str(user_market_raw or "").strip().upper() == "ETF" and StockUtils.identify_security_type(raw_symbol) == SecurityType.FUND
                    else None
                ),
            )
            if not symbol:
                raise ValueError(f"第 {i+1} 个股票代码不能为空")

            single_req = SingleAnalysisRequest(
                symbol=symbol,
                stock_code=symbol,  # 兼容字段
                parameters=request.parameters
            )

            try:
                per_task_params: Dict[str, Any] = {
                    "symbol": symbol,
                    "stock_code": symbol,
                    "market_type": resolved_market,
                    "batch_id": batch_id,
                }
                if request.parameters:
                    params_dict = request.parameters.model_dump()
                    params_dict.pop("engine", None)
                    per_task_params.update(params_dict)

                if is_etf:
                    task_type = AnalysisTaskType.ETF_ANALYSIS
                    per_task_params["market_type"] = "ETF"
                    per_task_params["is_etf"] = True
                    per_task_params["task_type"] = AnalysisTaskType.ETF_ANALYSIS.value
                    per_task_params["selected_analysts"] = ["etf", "market", "news"]
                else:
                    task_type = AnalysisTaskType.STOCK_ANALYSIS
                    per_task_params["market_type"] = resolved_market
                    per_task_params["is_etf"] = False
                    per_task_params["task_type"] = AnalysisTaskType.STOCK_ANALYSIS.value

                if engine_type == AnalysisEngine.V2:
                    from app.services.task_analysis_service import get_task_analysis_service
                    from app.models.user import PyObjectId

                    task_service = get_task_analysis_service()
                    task = await task_service.create_task(
                        user_id=_to_py_object_id(user["id"]),
                        task_type=task_type,
                        task_params=per_task_params,
                        engine_type="auto",
                        preference_type=preference_type,
                        batch_id=batch_id,
                    )
                    task_id = task.task_id
                    await memory_manager.create_task(
                        task_id=task_id,
                        user_id=user["id"],
                        stock_code=symbol,
                        parameters=per_task_params,
                        stock_name=None,
                    )
                else:
                    create_res = await simple_service.create_analysis_task(user["id"], single_req)
                    task_id = create_res.get("task_id")
                    if not task_id:
                        raise RuntimeError(f"创建任务失败：未返回task_id (symbol={symbol})")

                task_ids.append(task_id)
                mapping.append({
                    "symbol": symbol,
                    "stock_code": symbol,
                    "task_id": task_id,
                    "task_type": task_type.value,
                    "is_etf": is_etf,
                    "market_type": per_task_params["market_type"],
                })
                created_entries.append({
                    "symbol": symbol,
                    "task_id": task_id,
                    "task_params": per_task_params,
                })
                logger.info(f"✅ [批量分析] 已创建任务: {task_id} - {symbol} (task_type={task_type.value})")
            except Exception as create_error:
                logger.error(f"❌ [批量分析] 创建任务失败: {symbol}, 错误: {create_error}", exc_info=True)
                raise

        # 🔧 使用队列系统统一管理任务，持久化 Redis batch:<id> 元数据并返回 failed_submissions
        from app.services.queue_service import get_queue_service
        from app.services.queue.keys import BATCH_PREFIX
        queue_service = get_queue_service()

        submitted_task_ids: List[str] = []
        failed_submissions: List[Dict[str, Any]] = []
        db = get_mongo_db()

        for entry in created_entries:
            symbol = entry["symbol"]
            task_id = entry["task_id"]
            per_task_params = entry["task_params"]

            queue_params = {
                "task_id": task_id,
                "symbol": symbol,
                "stock_code": symbol,
                "user_id": user["id"],
                "batch_id": batch_id,
                "engine": engine_type.value,
                **per_task_params,
            }

            try:
                await queue_service.enqueue_task(
                    user_id=user["id"],
                    symbol=symbol,
                    params=queue_params,
                    batch_id=batch_id,
                    task_id=task_id,
                )
                submitted_task_ids.append(task_id)
                logger.info(f"✅ [批量分析] 任务已入队: {task_id} - {symbol} (引擎: {engine_type.value})")
            except Exception as enqueue_err:
                fail_reason = str(enqueue_err)
                logger.error(f"❌ [批量分析] 任务入队失败: {task_id} - {symbol}, 错误: {enqueue_err}", exc_info=True)
                failed_submissions.append({
                    "symbol": symbol,
                    "task_id": task_id,
                    "error": fail_reason,
                })
                # 回写失败状态，避免未入队的任务留在数据库成为永久 pending 僵尸
                try:
                    await db.unified_analysis_tasks.update_one(
                        {"task_id": task_id},
                        {"$set": {
                            "status": "failed",
                            "error_message": f"批量入队失败: {fail_reason}",
                            "message": f"批量入队失败: {fail_reason}",
                            "completed_at": now_tz(),
                            "updated_at": now_tz(),
                        }},
                    )
                except Exception:
                    pass
                try:
                    await memory_manager.update_task_status(
                        task_id=task_id,
                        status=TaskStatus.FAILED,
                        message=f"批量入队失败: {fail_reason}",
                        error_message=f"批量入队失败: {fail_reason}",
                    )
                except Exception:
                    pass

        # 持久化批次元数据到 Redis，打通 GET /api/analysis/batches/{batch_id}
        batch_status = (
            "failed" if len(submitted_task_ids) == 0
            else ("partial_failed" if failed_submissions else "submitted")
        )
        now_ts = str(int(time.time()))
        batch_meta = {
            "id": batch_id,
            "user": str(user["id"]),
            "title": str(request.title or "批量分析"),
            "description": str(request.description or ""),
            "status": batch_status,
            "engine": engine_type.value,
            "total_tasks": str(len(task_ids)),
            "submitted": str(len(submitted_task_ids)),
            "failed": str(len(failed_submissions)),
            "created_at": now_ts,
        }
        try:
            if getattr(queue_service, "r", None) is not None:
                await queue_service.r.hset(BATCH_PREFIX + batch_id, mapping=batch_meta)
        except Exception as batch_redis_err:
            logger.warning(f"⚠️ [批量分析] 写入 Redis batch 元数据失败: {batch_id} - {batch_redis_err}")

        logger.info(
            f"🚀 [批量分析] 批次 {batch_id} 提交完成: 成功入队 {len(submitted_task_ids)}/{len(task_ids)}, "
            f"失败 {len(failed_submissions)} (引擎: {engine_type.value})"
        )

        return {
            "success": len(submitted_task_ids) > 0,
            "data": {
                "batch_id": batch_id,
                "total_tasks": len(task_ids),
                "submitted_tasks": len(submitted_task_ids),
                "task_ids": task_ids,
                "submitted_task_ids": submitted_task_ids,
                "mapping": mapping,
                "failed_submissions": failed_submissions,
                "status": batch_status,
                "engine": engine_type.value,
            },
            "message": (
                f"批量分析任务已提交到队列，成功入队 {len(submitted_task_ids)}/{len(task_ids)} 个标的"
                + (f"，{len(failed_submissions)} 个入队失败" if failed_submissions else "")
                + f" (引擎: {engine_type.value})"
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [批量分析] 提交失败: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))

# 兼容性：保留原有端点
@router.post("/analyze")
async def analyze_single(
    req: SingleAnalyzeRequest,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """单股分析（兼容性端点）"""
    try:
        task_id = await svc.enqueue_task(
            user_id=user["id"],
            symbol=req.symbol,
            params=req.parameters
        )
        return {"task_id": task_id, "status": "queued"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))

@router.post("/analyze/batch")
async def analyze_batch(
    req: BatchAnalyzeRequest,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """批量分析（兼容性端点）"""
    try:
        batch_id, submitted = await svc.create_batch(
            user_id=user["id"],
            symbols=req.symbols,
            params=req.parameters
        )
        return {"batch_id": batch_id, "submitted": submitted}
    except Exception as e:
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))

@router.get("/batches/{batch_id}")
async def get_batch(batch_id: str, user: dict = Depends(get_current_user), svc: QueueService = Depends(get_queue_service)):
    b = await svc.get_batch(batch_id)
    if not b or not _is_task_owner(b.get("user"), user):
        raise HTTPException(status_code=404, detail="batch not found")
    return b

# 任务和批次查询端点
# 注意：这个路由被移到了 /tasks/{task_id}/status 之后，避免路由冲突
# @router.get("/tasks/{task_id}")
# async def get_task(
#     task_id: str,
#     user: dict = Depends(get_current_user),
#     svc: QueueService = Depends(get_queue_service)
# ):
#     """获取任务详情"""
#     t = await svc.get_task(task_id)
#     if not t or t.get("user") != user["id"]:
#         raise HTTPException(status_code=404, detail="任务不存在")
#     return t

# 原有的路由已被新的异步实现替代
# @router.get("/tasks/{task_id}/status")
# async def get_task_status_old(
#     task_id: str,
#     user: dict = Depends(get_current_user)
# ):
#     """获取任务状态和进度（旧版实现）"""
#     try:
#         status = await get_analysis_service().get_task_status(task_id)
#         if not status:
#             raise HTTPException(status_code=404, detail="任务不存在")
#         return {
#             "success": True,
#             "data": status
#         }
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """取消任务（同步联动 unified_analysis_tasks + analysis_tasks + Redis 队列 + memory_manager）"""
    try:
        from app.services.memory_state_manager import get_memory_state_manager, TaskStatus

        db = get_mongo_db()
        memory_manager = get_memory_state_manager()

        unified_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
        legacy_task = await db.analysis_tasks.find_one({"task_id": task_id})
        redis_task = None
        try:
            redis_task = await svc.get_task(task_id)
        except Exception as redis_err:
            logger.warning(f"⚠️ [cancel] 读取 Redis 任务失败（继续校验 DB）: {task_id} - {redis_err}")
        mem_task = await memory_manager.get_task(task_id)

        if not any([unified_task, legacy_task, redis_task, mem_task]):
            raise HTTPException(status_code=404, detail="任务不存在")

        # 校验任务所有权
        owner_id = None
        if unified_task is not None:
            owner_id = unified_task.get("user_id", unified_task.get("user"))
        elif legacy_task is not None:
            owner_id = legacy_task.get("user_id", legacy_task.get("user"))
        elif redis_task is not None:
            owner_id = redis_task.get("user") or redis_task.get("user_id")
        elif mem_task is not None:
            owner_id = getattr(mem_task, "user_id", None)

        if not _is_task_owner(owner_id, user):
            raise HTTPException(status_code=403, detail="无权取消此任务")

        # 终态校验（已完成/已失败不可再取消）
        current_status = None
        for src_doc in (unified_task, legacy_task, redis_task):
            if src_doc and src_doc.get("status"):
                raw_st = src_doc.get("status")
                current_status = raw_st.value if hasattr(raw_st, "value") else str(raw_st)
                break
        if current_status is None and mem_task is not None:
            raw_mem_st = getattr(mem_task, "status", None)
            if raw_mem_st is not None:
                current_status = raw_mem_st.value if hasattr(raw_mem_st, "value") else str(raw_mem_st)
        if current_status in ("completed", "failed"):
            raise HTTPException(status_code=400, detail=f"任务当前状态为 {current_status}，无法取消")

        now_ts = now_tz()
        cancel_fields = {
            "status": "cancelled",
            "message": "任务已被用户取消",
            "error_message": "任务已被用户取消",
            "completed_at": now_ts,
            "updated_at": now_ts,
        }

        updated_unified = False
        if unified_task is not None:
            res = await db.unified_analysis_tasks.update_one(
                {"task_id": task_id, "status": {"$nin": ["completed", "failed"]}},
                {"$set": cancel_fields},
            )
            updated_unified = bool(res.modified_count > 0 or current_status == "cancelled")

        updated_legacy = False
        if legacy_task is not None:
            res = await db.analysis_tasks.update_one(
                {"task_id": task_id, "status": {"$nin": ["completed", "failed"]}},
                {"$set": {**cancel_fields, "last_error": "任务已被用户取消"}},
            )
            updated_legacy = bool(res.modified_count > 0 or current_status == "cancelled")

        redis_cancelled = False
        try:
            redis_cancelled = bool(await svc.cancel_task(task_id))
        except Exception as redis_err:
            logger.warning(f"⚠️ [cancel] Redis 取消任务失败（已更新 DB）: {task_id} - {redis_err}")

        mem_cancelled = False
        try:
            mem_cancelled = bool(
                await memory_manager.update_task_status(
                    task_id=task_id,
                    status=TaskStatus.CANCELLED,
                    message="任务已被用户取消",
                    current_step="cancelled",
                )
            )
        except Exception as mem_err:
            logger.warning(f"⚠️ [cancel] 内存状态更新失败: {task_id} - {mem_err}")

        if updated_unified or updated_legacy or redis_cancelled or mem_cancelled or current_status == "cancelled":
            return {
                "success": True,
                "message": "任务已取消",
                "details": {
                    "unified_updated": updated_unified,
                    "legacy_updated": updated_legacy,
                    "redis_cancelled": redis_cancelled,
                    "memory_updated": mem_cancelled,
                },
            }
        raise HTTPException(status_code=400, detail="取消任务失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))

@router.get("/user/queue-status")
async def get_user_queue_status(
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """获取用户队列状态"""
    try:
        status = await svc.get_user_queue_status(user["id"])
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))

@router.get("/user/history")
async def get_user_analysis_history(
    user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="任务状态过滤"),
    start_date: Optional[str] = Query(None, description="开始日期，YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，YYYY-MM-DD"),
    symbol: Optional[str] = Query(None, description="股票代码"),
    stock_code: Optional[str] = Query(None, description="股票代码(已废弃,使用symbol)"),
    market_type: Optional[str] = Query(None, description="市场类型"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小")
):
    """获取用户分析历史（将 symbol/date/status/market_type 筛选下推到 MongoDB，主查 unified_analysis_tasks 兼容 analysis_tasks）"""
    try:
        from datetime import datetime, timedelta

        db = get_mongo_db()
        uid_candidates = _build_user_id_candidates(user["id"])

        # 解析日期过滤（含 end_date 当天）
        date_range: Dict[str, Any] = {}
        if start_date:
            try:
                start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d")
                date_range["$gte"] = start_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="start_date 格式应为 YYYY-MM-DD")
        if end_date:
            try:
                end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d") + timedelta(days=1)
                date_range["$lt"] = end_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="end_date 格式应为 YYYY-MM-DD")

        raw_query_symbol = symbol or stock_code
        query_symbol = StockUtils.normalize_symbol(raw_query_symbol) if raw_query_symbol else None

        status_candidates: Optional[List[str]] = None
        if status:
            st_lower = str(status).strip().lower()
            if st_lower in ("running", "processing"):
                status_candidates = ["processing", "running"]
            else:
                status_candidates = [st_lower]

        def _build_collection_query(is_unified: bool) -> Dict[str, Any]:
            and_clauses: List[Dict[str, Any]] = [
                {"$or": [{"user_id": {"$in": uid_candidates}}, {"user": {"$in": uid_candidates}}]}
            ]
            if status_candidates:
                and_clauses.append({"status": {"$in": status_candidates}})
            if date_range:
                and_clauses.append({"created_at": date_range})
            if query_symbol:
                sym_variants = list(dict.fromkeys([query_symbol, str(raw_query_symbol).strip().upper(), str(raw_query_symbol).strip()]))
                if is_unified:
                    and_clauses.append({
                        "$or": [
                            {"task_params.symbol": {"$in": sym_variants}},
                            {"task_params.stock_code": {"$in": sym_variants}},
                            {"task_params.code": {"$in": sym_variants}},
                            {"symbol": {"$in": sym_variants}},
                            {"stock_code": {"$in": sym_variants}},
                        ]
                    })
                else:
                    and_clauses.append({
                        "$or": [
                            {"symbol": {"$in": sym_variants}},
                            {"stock_code": {"$in": sym_variants}},
                            {"stock_symbol": {"$in": sym_variants}},
                            {"parameters.symbol": {"$in": sym_variants}},
                            {"parameters.stock_code": {"$in": sym_variants}},
                        ]
                    })
            if market_type:
                mkt = str(market_type).strip()
                mkt_upper = mkt.upper()
                if mkt_upper == "ETF":
                    if is_unified:
                        and_clauses.append({
                            "$or": [
                                {"task_type": "etf_analysis"},
                                {"task_params.market_type": {"$in": ["ETF", "etf"]}},
                                {"task_params.is_etf": True},
                            ]
                        })
                    else:
                        and_clauses.append({
                            "$or": [
                                {"parameters.market_type": {"$in": ["ETF", "etf"]}},
                                {"market_type": {"$in": ["ETF", "etf"]}},
                            ]
                        })
                else:
                    mkt_aliases = list(dict.fromkeys([mkt, mkt.lower(), mkt_upper]))
                    if mkt_upper in ("CN", "A股"):
                        mkt_aliases = list(dict.fromkeys(mkt_aliases + ["CN", "cn", "A股"]))
                    elif mkt_upper in ("HK", "港股"):
                        mkt_aliases = list(dict.fromkeys(mkt_aliases + ["HK", "hk", "港股"]))
                    elif mkt_upper in ("US", "美股"):
                        mkt_aliases = list(dict.fromkeys(mkt_aliases + ["US", "us", "美股"]))
                    field_path = "task_params.market_type" if is_unified else "parameters.market_type"
                    and_clauses.append({
                        "$or": [
                            {field_path: {"$in": mkt_aliases}},
                            {"market_type": {"$in": mkt_aliases}},
                        ]
                    })
            return {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

        unified_query = _build_collection_query(is_unified=True)
        legacy_query = _build_collection_query(is_unified=False)

        skip = (page - 1) * page_size
        fetch_window = skip + page_size

        unified_total = await db.unified_analysis_tasks.count_documents(unified_query)
        legacy_total = await db.analysis_tasks.count_documents(legacy_query)

        unified_docs = (
            await db.unified_analysis_tasks.find(unified_query)
            .sort("created_at", -1)
            .limit(fetch_window)
            .to_list(length=fetch_window)
        )
        legacy_docs = (
            await db.analysis_tasks.find(legacy_query)
            .sort("created_at", -1)
            .limit(fetch_window)
            .to_list(length=fetch_window)
        )

        merged_map: Dict[str, Dict[str, Any]] = {}
        for doc in unified_docs or []:
            tid = doc.get("task_id")
            if tid:
                merged_map[tid] = _normalize_history_doc(doc, "unified_analysis_tasks")
        duplicate_count = 0
        for doc in legacy_docs or []:
            tid = doc.get("task_id")
            if not tid:
                continue
            if tid in merged_map:
                duplicate_count += 1
            else:
                merged_map[tid] = _normalize_history_doc(doc, "analysis_tasks")

        merged_list = list(merged_map.values())
        merged_list.sort(key=lambda x: str(x.get("created_at") or x.get("start_time") or ""), reverse=True)
        page_items = merged_list[skip:skip + page_size]
        total = max(len(merged_list), unified_total + legacy_total - duplicate_count)

        return {
            "success": True,
            "data": {
                "tasks": page_items,
                "total": total,
                "page": page,
                "page_size": page_size
            },
            "message": "历史查询成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=format_http_error_detail(e))

# WebSocket 端点
@router.websocket("/ws/task/{task_id}")
async def websocket_task_progress(websocket: WebSocket, task_id: str):
    """WebSocket 端点：实时获取任务进度"""
    import json
    websocket_manager = get_websocket_manager()

    try:
        await websocket_manager.connect(websocket, task_id)

        # 发送连接确认消息
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "task_id": task_id,
            "message": "WebSocket 连接已建立"
        }))

        # 保持连接活跃
        while True:
            try:
                # 接收客户端的心跳消息
                data = await websocket.receive_text()
                # 可以处理客户端发送的消息
                logger.debug(f"📡 收到 WebSocket 消息: {data}")
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.warning(f"⚠️ WebSocket 消息处理错误: {e}")
                break

    except WebSocketDisconnect:
        logger.info(f"🔌 WebSocket 客户端断开连接: {task_id}")
    except Exception as e:
        logger.error(f"❌ WebSocket 连接错误: {e}")
    finally:
        await websocket_manager.disconnect(websocket, task_id)

# 任务详情查询路由（放在最后避免与 /tasks/{task_id}/status 冲突）
@router.get("/tasks/{task_id}/details")
async def get_task_details(
    task_id: str,
    user: dict = Depends(get_current_user),
    svc: QueueService = Depends(get_queue_service)
):
    """获取任务详情（使用不同的路径避免冲突）"""
    t = await svc.get_task(task_id)
    if not t or t.get("user") != user["id"]:
        raise HTTPException(status_code=404, detail="任务不存在")
    return t


# ==================== 僵尸任务管理 ====================

@router.get("/admin/zombie-tasks")
async def get_zombie_tasks(
    max_running_hours: int = Query(default=2, ge=1, le=72, description="最大运行时长（小时）"),
    user: dict = Depends(get_current_user)
):
    """获取僵尸任务列表（仅管理员）

    僵尸任务：长时间处于 processing/running/pending 状态的任务
    """
    # 检查管理员权限
    if user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可访问")

    try:
        svc = get_simple_analysis_service()
        zombie_tasks = await svc.get_zombie_tasks(max_running_hours)

        return {
            "success": True,
            "data": zombie_tasks,
            "total": len(zombie_tasks),
            "max_running_hours": max_running_hours
        }
    except Exception as e:
        logger.error(f"❌ 获取僵尸任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取僵尸任务失败: {str(e)}")


@router.post("/admin/cleanup-zombie-tasks")
async def cleanup_zombie_tasks(
    max_running_hours: int = Query(default=2, ge=1, le=72, description="最大运行时长（小时）"),
    user: dict = Depends(get_current_user)
):
    """清理僵尸任务（仅管理员）

    将长时间处于 processing/running/pending 状态的任务标记为失败
    """
    # 检查管理员权限
    if user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可访问")

    try:
        svc = get_simple_analysis_service()
        result = await svc.cleanup_zombie_tasks(max_running_hours)

        return {
            "success": True,
            "data": result,
            "message": f"已清理 {result.get('total_cleaned', 0)} 个僵尸任务"
        }
    except Exception as e:
        logger.error(f"❌ 清理僵尸任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"清理僵尸任务失败: {str(e)}")


@router.post("/tasks/{task_id}/mark-failed")
async def mark_task_as_failed(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """将指定任务标记为失败（同步联动 unified_analysis_tasks + analysis_tasks + Redis + memory_manager）"""
    try:
        from app.services.memory_state_manager import TaskStatus

        svc = get_simple_analysis_service()
        db = get_mongo_db()

        unified_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
        legacy_task = await db.analysis_tasks.find_one({"task_id": task_id})
        mem_task = await svc.memory_manager.get_task(task_id)

        if not any([unified_task, legacy_task, mem_task]):
            raise HTTPException(status_code=404, detail="任务不存在")

        owner_id = (
            unified_task.get("user_id", unified_task.get("user")) if unified_task
            else legacy_task.get("user_id", legacy_task.get("user")) if legacy_task
            else getattr(mem_task, "user_id", None)
        )
        if not _is_task_owner(owner_id, user):
            raise HTTPException(status_code=403, detail="无权操作此任务")

        now_ts = now_tz()

        # 1. 更新内存状态
        await svc.memory_manager.update_task_status(
            task_id=task_id,
            status=TaskStatus.FAILED,
            message="手动标记为失败",
            error_message="用户手动标记为失败"
        )

        # 2. 更新 v2 主集合 unified_analysis_tasks 与旧集合 analysis_tasks
        unified_modified = 0
        if unified_task is not None:
            u_res = await db.unified_analysis_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "status": "failed",
                        "error_message": "用户手动标记为失败",
                        "message": "用户手动标记为失败",
                        "completed_at": now_ts,
                        "updated_at": now_ts,
                    }
                },
            )
            unified_modified = u_res.modified_count

        legacy_modified = 0
        if legacy_task is not None:
            l_res = await db.analysis_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "status": "failed",
                        "last_error": "用户手动标记为失败",
                        "error_message": "用户手动标记为失败",
                        "completed_at": now_ts,
                        "updated_at": now_ts,
                    }
                },
            )
            legacy_modified = l_res.modified_count

        # 3. 同步清理 Redis 队列处理中/待处理状态
        try:
            queue_service = get_queue_service()
            await queue_service.cancel_task(task_id)
        except Exception as q_err:
            logger.warning(f"⚠️ [mark-failed] 清理 Redis 队列任务失败（不影响主流程）: {task_id} - {q_err}")

        if unified_modified > 0 or legacy_modified > 0 or mem_task is not None:
            logger.info(f"✅ 任务 {task_id} 已标记为失败 (unified={unified_modified}, legacy={legacy_modified})")
            return {
                "success": True,
                "message": "任务已标记为失败",
            }
        return {
            "success": True,
            "message": "任务已是失败状态",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 标记任务失败: {task_id} - {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"标记任务失败: {str(e)}"
        )


@router.post("/tasks/{task_id}/retry")
async def retry_task(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """重新创建失败/已取消的分析任务（支持 ETF 与旧任务；入队失败回写 FAILED 并显式报错）"""
    try:
        from app.services.task_analysis_service import get_task_analysis_service
        from app.services.memory_state_manager import get_memory_state_manager, TaskStatus
        from app.models.analysis import AnalysisTaskType
        from app.models.user import PyObjectId

        db = get_mongo_db()
        uid_candidates = _build_user_id_candidates(user["id"])

        # 查找任务（主查 unified_analysis_tasks，兼容 analysis_tasks）
        failed_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
        old_task = None if failed_task else await db.analysis_tasks.find_one({"task_id": task_id})

        source_task = failed_task or old_task
        if not source_task:
            raise HTTPException(status_code=404, detail="任务不存在或不属于当前用户")

        owner_id = source_task.get("user_id", source_task.get("user"))
        if not _is_task_owner(owner_id, user):
            raise HTTPException(status_code=403, detail="无权重试此任务")

        if failed_task is not None:
            task_status = failed_task.get("status")
            status_str = task_status.value if hasattr(task_status, "value") else str(task_status)
            if status_str not in ["failed", "cancelled"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"只能重新创建失败或已取消的任务，当前状态: {status_str}"
                )
            raw_params = dict(failed_task.get("task_params") or {})
            raw_symbol = raw_params.get("symbol") or raw_params.get("stock_code") or failed_task.get("symbol")
            raw_task_type = failed_task.get("task_type")
        else:
            raw_params = dict(old_task.get("parameters") or {})
            raw_symbol = (
                raw_params.get("symbol")
                or raw_params.get("stock_code")
                or old_task.get("symbol")
                or old_task.get("stock_code")
            )
            raw_task_type = old_task.get("task_type")

        if not raw_symbol:
            raise HTTPException(status_code=400, detail="任务参数中缺少股票代码")

        req_market = raw_params.get("market_type") or (old_task.get("market_type") if old_task else None)
        symbol, is_etf, resolved_market = _resolve_symbol_and_etf_routing(raw_symbol, req_market)
        if str(raw_task_type or "").lower() == "etf_analysis":
            is_etf = True
            resolved_market = "ETF"

        if is_etf:
            task_type = AnalysisTaskType.ETF_ANALYSIS
            task_params = {
                **raw_params,
                "symbol": symbol,
                "stock_code": symbol,
                "market_type": "ETF",
                "is_etf": True,
                "task_type": AnalysisTaskType.ETF_ANALYSIS.value,
                "selected_analysts": raw_params.get("selected_analysts") or ["etf", "market", "news"],
            }
        else:
            try:
                task_type = AnalysisTaskType(raw_task_type) if isinstance(raw_task_type, str) else AnalysisTaskType.STOCK_ANALYSIS
            except ValueError:
                task_type = AnalysisTaskType.STOCK_ANALYSIS
            task_params = {
                **raw_params,
                "symbol": symbol,
                "stock_code": symbol,
                "market_type": resolved_market,
                "is_etf": False,
                "task_type": task_type.value,
            }

        preference_type = await get_user_risk_preference(user["id"])
        task_service = get_task_analysis_service()
        new_task = await task_service.create_task(
            user_id=_to_py_object_id(user["id"]),
            task_type=task_type,
            task_params=task_params,
            engine_type="auto",
            preference_type=preference_type,
        )

        memory_manager = get_memory_state_manager()
        try:
            await memory_manager.create_task(
                task_id=new_task.task_id,
                user_id=user["id"],
                stock_code=symbol,
                parameters=task_params,
                stock_name=None,
            )
        except Exception:
            pass

        queue_service = get_queue_service()
        queue_params = {
            "engine": "v2",
            "task_id": new_task.task_id,
            "user_id": user["id"],
            **task_params,
        }

        try:
            await queue_service.enqueue_task(
                user_id=user["id"],
                symbol=symbol,
                params=queue_params,
                task_id=new_task.task_id,
            )
            logger.info(f"✅ [重试任务] 任务已加入队列: {task_id} -> {new_task.task_id}")
        except Exception as enqueue_err:
            fail_msg = f"重试任务入队失败: {enqueue_err}"
            logger.error(f"❌ [重试任务] {fail_msg} (new_task_id={new_task.task_id})", exc_info=True)
            now_ts = now_tz()
            try:
                await db.unified_analysis_tasks.update_one(
                    {"task_id": new_task.task_id},
                    {"$set": {
                        "status": "failed",
                        "error_message": fail_msg,
                        "message": fail_msg,
                        "completed_at": now_ts,
                        "updated_at": now_ts,
                    }},
                )
            except Exception:
                pass
            try:
                await memory_manager.update_task_status(
                    task_id=new_task.task_id,
                    status=TaskStatus.FAILED,
                    message=fail_msg,
                    error_message=fail_msg,
                )
            except Exception:
                pass
            raise HTTPException(
                status_code=503,
                detail=f"任务重新入队失败: {format_http_error_detail(enqueue_err)}",
            )

        return {
            "success": True,
            "task_id": new_task.task_id,
            "task_type": task_type.value,
            "is_etf": is_etf,
            "market_type": task_params["market_type"],
            "message": "任务已重新创建并加入队列",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [重试任务] 重新创建任务失败: {task_id} - {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"重新创建任务失败: {str(e)}"
        )


@router.delete("/tasks/{task_id}")
async def delete_task(
    task_id: str,
    user: dict = Depends(get_current_user)
):
    """删除指定任务（强制 user_id 归属校验，同步清理 unified_analysis_tasks + analysis_tasks + Redis + memory_manager）"""
    try:
        from app.services.queue.keys import TASK_PREFIX

        svc = get_simple_analysis_service()
        db = get_mongo_db()

        unified_task = await db.unified_analysis_tasks.find_one({"task_id": task_id})
        legacy_task = await db.analysis_tasks.find_one({"task_id": task_id})
        mem_task = await svc.memory_manager.get_task(task_id)

        redis_task = None
        queue_service = None
        try:
            queue_service = get_queue_service()
            redis_task = await queue_service.get_task(task_id)
        except Exception as q_err:
            logger.debug(f"⚠️ [delete] 查询 Redis 任务忽略异常: {task_id} - {q_err}")

        if not any([unified_task, legacy_task, mem_task, redis_task]):
            raise HTTPException(status_code=404, detail="任务不存在")

        # 严格校验 user_id 归属（防越权删除）
        owner_id = (
            unified_task.get("user_id", unified_task.get("user")) if unified_task
            else legacy_task.get("user_id", legacy_task.get("user")) if legacy_task
            else getattr(mem_task, "user_id", None) if mem_task
            else (redis_task.get("user") or redis_task.get("user_id")) if redis_task
            else None
        )
        if not _is_task_owner(owner_id, user):
            raise HTTPException(status_code=403, detail="无权删除此任务")

        # 1. 从内存删除
        await svc.memory_manager.remove_task(task_id)

        # 2. 从 MongoDB 双表删除
        uid_candidates = _build_user_id_candidates(user["id"])
        delete_filter: Dict[str, Any] = {"task_id": task_id}
        if not _is_admin_user(user):
            delete_filter["$or"] = [
                {"user_id": {"$in": uid_candidates}},
                {"user": {"$in": uid_candidates}},
            ]

        u_res = await db.unified_analysis_tasks.delete_one(delete_filter)
        l_res = await db.analysis_tasks.delete_one(delete_filter)

        # 3. 清理 Redis 队列任务与哈希
        if queue_service is not None:
            try:
                await queue_service.cancel_task(task_id)
                if getattr(queue_service, "r", None) is not None:
                    await queue_service.r.delete(TASK_PREFIX + task_id)
            except Exception as q_err:
                logger.warning(f"⚠️ [delete] 清理 Redis 任务失败: {task_id} - {q_err}")

        deleted_total = (u_res.deleted_count or 0) + (l_res.deleted_count or 0)
        logger.info(f"✅ 任务 {task_id} 已删除 (unified={u_res.deleted_count}, legacy={l_res.deleted_count})")
        return {
            "success": True,
            "deleted_count": deleted_total,
            "message": "任务已删除",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 删除任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除任务失败: {str(e)}")
