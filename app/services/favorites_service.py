"""
股票关注列表服务
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId

from app.core.database import get_mongo_db
from app.models.user import FavoriteStock
from tradingagents.utils.stock_utils import (
    StockUtils,
    StockMarket,
    SecurityType,
    ETF_SH_PREFIXES,
    ETF_SZ_PREFIXES,
)
# 已移除在线行情补齐功能，只从数据库读取数据
# 用户可以通过"同步实时行情"按钮来更新数据
# from app.services.quotes_service import get_quotes_service

logger = logging.getLogger("webapi")


def _normalize_quote_date(value: Any) -> Optional[str]:
    """将 trade_date / updated_at 标准化为 YYYY-MM-DD 字符串。"""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10:
        return text[:10].replace("/", "-")
    return text


def _resolve_etf_exchange(code: str) -> str:
    """根据统一 ETF 前缀识别交易所（含 56/58 上交所、18 深交所）。"""
    norm = StockUtils.normalize_symbol(code)
    if norm.startswith(ETF_SH_PREFIXES):
        return "上海证券交易所"
    if norm.startswith(ETF_SZ_PREFIXES):
        return "深圳证券交易所"
    if norm.startswith(("89",)):
        return "北京证券交易所"
    return "-"


def _extract_quote_metrics(latest_doc: Dict[str, Any], prev_doc: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """从行情/日线文档中提取现价、涨跌幅、成交量与行情日期。"""
    close_val = latest_doc.get("close")
    if close_val is None:
        close_val = latest_doc.get("price")

    pct_val = latest_doc.get("pct_chg")
    if pct_val is None:
        pct_val = latest_doc.get("change_percent")
    if pct_val is None and close_val is not None:
        pre_close = latest_doc.get("pre_close")
        if pre_close is None and prev_doc is not None:
            pre_close = prev_doc.get("close")
        try:
            if pre_close is not None and float(pre_close) > 0:
                pct_val = round((float(close_val) - float(pre_close)) / float(pre_close) * 100, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            pct_val = None

    quote_date = _normalize_quote_date(
        latest_doc.get("trade_date") or latest_doc.get("quote_date") or latest_doc.get("updated_at")
    )
    return {
        "current_price": close_val,
        "change_percent": pct_val,
        "volume": latest_doc.get("volume"),
        "quote_date": quote_date,
    }


class FavoritesService:
    """股票关注列表服务类"""
    
    def __init__(self):
        self.db = None
    
    async def _get_db(self):
        """获取数据库连接"""
        if self.db is None:
            self.db = get_mongo_db()
        return self.db

    async def _ensure_tags_exist(self, user_id: str, tags: Optional[List[str]]) -> List[str]:
        """确保股票关注列表中使用到的标签已同步到 user_tags 集合。"""
        normalized_tags: List[str] = []
        seen = set()
        for item in tags or []:
            tag_name = str(item or "").strip()
            if not tag_name or tag_name in seen:
                continue
            seen.add(tag_name)
            normalized_tags.append(tag_name)

        if not normalized_tags:
            return []

        try:
            from app.services.tags_service import tags_service

            existing_tags = {
                tag.get("name")
                for tag in await tags_service.list_tags(user_id)
                if tag.get("name")
            }
            for tag_name in normalized_tags:
                if tag_name in existing_tags:
                    continue
                try:
                    await tags_service.create_tag(user_id=user_id, name=tag_name)
                    existing_tags.add(tag_name)
                except Exception:
                    logger.debug("[FavoritesService] 创建缺失标签失败: %s", tag_name, exc_info=True)
        except Exception:
            logger.warning("[FavoritesService] 自动补建标签失败，继续保存股票关注列表", exc_info=True)

        return normalized_tags

    def _is_valid_object_id(self, user_id: str) -> bool:
        """
        检查是否是有效的ObjectId格式
        注意：这里只检查格式，不代表数据库中实际存储的是ObjectId类型
        为了兼容性，我们统一使用 user_favorites 集合存储股票关注列表
        """
        # 强制返回 False，统一使用 user_favorites 集合
        return False

    def _format_favorite(self, favorite: Dict[str, Any]) -> Dict[str, Any]:
        """格式化收藏条目（仅基础信息，不包含实时行情）。
        行情将在 get_user_favorites 中批量富集。
        """
        added_at = favorite.get("added_at")
        if isinstance(added_at, datetime):
            added_at = added_at.isoformat()
        raw_code = favorite.get("stock_code")
        norm_code = StockUtils.normalize_symbol(raw_code) if raw_code else ""
        is_etf = (
            str(favorite.get("market") or "").strip().upper() == "ETF"
            or StockUtils.identify_security_type(norm_code) == SecurityType.FUND
        )
        return {
            "stock_code": raw_code,
            "stock_name": favorite.get("stock_name"),
            "market": favorite.get("market", "A股"),
            "security_type": "ETF" if is_etf else "STOCK",
            "added_at": added_at,
            "tags": favorite.get("tags", []),
            "notes": favorite.get("notes", ""),
            "alert_price_high": favorite.get("alert_price_high"),
            "alert_price_low": favorite.get("alert_price_low"),
            "alert_gain_price": favorite.get("alert_gain_price"),
            # 行情占位，稍后填充
            "current_price": None,
            "change_percent": None,
            "volume": None,
            "quote_date": None,
        }

    async def _fetch_latest_daily_quotes(
        self,
        db: Any,
        collection_name: str,
        codes: List[str],
        *,
        daily_period_only: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """按标的从日线集合批量拉取最新一条日线（必要时读前一日收盘价计算涨跌幅）。"""
        unique_codes = list(dict.fromkeys([c for c in codes if c]))
        if not unique_codes:
            return {}

        coll = db[collection_name]

        async def _load_one(norm_code: str) -> tuple[str, Optional[Dict[str, Any]]]:
            query: Dict[str, Any] = {"$or": [{"code": norm_code}, {"symbol": norm_code}]}
            if daily_period_only:
                query = {
                    "$and": [
                        query,
                        {"$or": [{"period": "daily"}, {"period": {"$exists": False}}, {"period": None}]},
                    ]
                }
            projection = {
                "code": 1,
                "symbol": 1,
                "close": 1,
                "pre_close": 1,
                "pct_chg": 1,
                "change_percent": 1,
                "volume": 1,
                "amount": 1,
                "trade_date": 1,
                "updated_at": 1,
                "_id": 0,
            }
            try:
                cursor = coll.find(query, projection).sort("trade_date", -1).limit(5)
                docs = await cursor.to_list(length=5)
                if not docs:
                    return norm_code, None
                latest = docs[0]
                latest_date = _normalize_quote_date(latest.get("trade_date"))
                prev = None
                for candidate in docs[1:]:
                    cand_date = _normalize_quote_date(candidate.get("trade_date"))
                    if not latest_date or cand_date != latest_date:
                        prev = candidate
                        break
                return norm_code, _extract_quote_metrics(latest, prev)
            except Exception as exc:
                logger.warning(f"⚠️ 从 {collection_name} 读取 {norm_code} 最新日线失败: {exc}")
                return norm_code, None

        pairs = await asyncio.gather(*(_load_one(code) for code in unique_codes))
        return {code: metrics for code, metrics in pairs if metrics is not None}
    async def update_alert_gain_price(self, user_id: str, stock_code: str, gain_price: Optional[float]) -> bool:
        """设置或清除止盈提醒价格"""
        db = await self._get_db()
        is_oid = self._is_valid_object_id(user_id)
        prefix = "favorite_stocks.$." if is_oid else "favorites.$."
        update_fields = {prefix + "alert_gain_price": gain_price}
        if is_oid:
            result = await db.users.update_one(
                {"_id": ObjectId(user_id), "favorite_stocks.stock_code": stock_code},
                {"$set": update_fields}
            )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {"user_id": user_id, "favorites.stock_code": stock_code},
                {"$set": {**update_fields, "updated_at": datetime.utcnow()}}
            )
            return result.modified_count > 0

    async def get_user_favorites(self, user_id: str) -> List[Dict[str, Any]]:
        """
        获取用户股票关注列表列表，并从数据库读取实时行情进行富集（兼容字符串ID与ObjectId）。
        
        注意：
        - 只从数据库（market_quotes、market_quotes_hk、market_quotes_us）读取数据
        - 不会调用在线 API 拉取最新价格，避免接口超时
        - 用户可以通过"同步实时行情"按钮来更新数据
        - 后台定时任务会定期更新数据库中的实时行情数据
        """
        import time
        start_time = time.time()
        db = await self._get_db()

        logger.info(f"🔍 [get_user_favorites] 开始查询用户股票关注列表: user_id={user_id}")

        favorites: List[Dict[str, Any]] = []
        if self._is_valid_object_id(user_id):
            # 先尝试使用 ObjectId 查询
            user = await db.users.find_one({"_id": ObjectId(user_id)})
            # 如果 ObjectId 查询失败，尝试使用字符串查询
            if user is None:
                user = await db.users.find_one({"_id": user_id})
            favorites = (user or {}).get("favorite_stocks", [])
            logger.info(f"✅ [get_user_favorites] 从 users 集合获取到 {len(favorites)} 只股票关注列表")
        else:
            # 🔥 严格过滤：只查询当前用户的股票关注列表
            # 使用精确匹配，确保只获取当前用户的数据
            doc = await db.user_favorites.find_one({"user_id": {"$eq": user_id}})
            if doc:
                # 🔥 双重验证：确保文档的 user_id 确实匹配
                doc_user_id = doc.get("user_id")
                if doc_user_id != user_id:
                    logger.error(f"❌ [get_user_favorites] 用户ID不匹配！查询: {user_id}, 文档: {doc_user_id}")
                    favorites = []
                else:
                    favorites = doc.get("favorites", [])
                    logger.info(f"✅ [get_user_favorites] 从 user_favorites 集合获取到 {len(favorites)} 只股票关注列表 (user_id={doc_user_id})")
            else:
                logger.warning(f"⚠️ [get_user_favorites] 未找到用户股票关注列表数据: user_id={user_id}")
                # 🔥 调试：检查是否有其他用户的文档（仅用于调试）
                all_docs_count = await db.user_favorites.count_documents({})
                logger.info(f"🔍 [get_user_favorites] 数据库中共有 {all_docs_count} 个文档")
                if all_docs_count > 0:
                    # 只查询前5个文档用于调试
                    all_docs = await db.user_favorites.find({}, {"user_id": 1, "favorites": 1}).limit(5).to_list(length=5)
                    for idx, d in enumerate(all_docs):
                        logger.info(f"  文档 {idx+1}: user_id={d.get('user_id')}, favorites数量={len(d.get('favorites', []))}")
                favorites = []

        # 先格式化基础字段
        items = [self._format_favorite(fav) for fav in favorites]

        # 批量获取股票基础信息（板块、交易所等；支持多源降级与 ETF 识别）
        codes = [it.get("stock_code") for it in items if it.get("stock_code")]
        norm_codes = list(
            dict.fromkeys(
                [StockUtils.normalize_symbol(c) or str(c).strip() for c in codes if c]
            )
        )
        if norm_codes:
            try:
                # 🔥 获取数据源优先级配置
                enabled_sources: List[str] = []
                try:
                    from app.core.unified_config import UnifiedConfigManager
                    config = UnifiedConfigManager()
                    data_source_configs = await config.get_data_source_configs_async()
                    enabled_sources = [
                        ds.type.lower() for ds in data_source_configs
                        if ds.enabled and ds.type.lower() in ["tushare", "multi_source", "akshare", "baostock"]
                    ]
                except Exception:
                    enabled_sources = []

                source_priority: List[str] = []
                for src in [*enabled_sources, "tushare", "multi_source", "akshare", "baostock"]:
                    if src not in source_priority:
                        source_priority.append(src)
                priority_rank = {src: idx for idx, src in enumerate(source_priority)}

                # 从 stock_basic_info 批量获取基础信息（按优先级降级，不再锁死单一 preferred_source）
                basic_info_coll = db["stock_basic_info"]
                basic_limit = max(len(norm_codes) * 5, 50)
                cursor = basic_info_coll.find(
                    {"$or": [{"code": {"$in": norm_codes}}, {"symbol": {"$in": norm_codes}}]},
                    {"code": 1, "symbol": 1, "name": 1, "sse": 1, "market": 1, "source": 1, "_id": 0}
                ).limit(basic_limit)
                basic_docs = await cursor.to_list(length=basic_limit)

                basic_ranked: Dict[str, tuple[int, Dict[str, Any]]] = {}
                for doc in (basic_docs or []):
                    raw_c = doc.get("code") or doc.get("symbol")
                    if not raw_c:
                        continue
                    c_norm = StockUtils.normalize_symbol(str(raw_c)) or str(raw_c).strip()
                    rank = priority_rank.get(str(doc.get("source") or "").strip().lower(), len(priority_rank) + 1)
                    if c_norm not in basic_ranked or rank < basic_ranked[c_norm][0]:
                        basic_ranked[c_norm] = (rank, doc)
                basic_map = {k: v[1] for k, v in basic_ranked.items()}

                # 收集 stock_basic_info 未命中或本身属于 ETF 的代码，查询 etf_basic_info
                etf_lookup_codes: List[str] = []
                for it in items:
                    c_norm = StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code") or "").strip()
                    is_etf_code = (
                        it.get("security_type") == "ETF"
                        or StockUtils.identify_security_type(c_norm) == SecurityType.FUND
                    )
                    if is_etf_code or c_norm not in basic_map:
                        etf_lookup_codes.append(c_norm)
                etf_lookup_codes = list(dict.fromkeys([c for c in etf_lookup_codes if c]))

                etf_set = set()
                etf_name_map: Dict[str, Optional[str]] = {}
                if etf_lookup_codes:
                    etf_coll = db["etf_basic_info"]
                    etf_limit = max(len(etf_lookup_codes) * 3, 30)
                    etf_cursor = etf_coll.find(
                        {"$or": [{"code": {"$in": etf_lookup_codes}}, {"symbol": {"$in": etf_lookup_codes}}]},
                        {"code": 1, "symbol": 1, "name": 1, "fund_name": 1, "etf_name": 1, "_id": 0}
                    ).limit(etf_limit)
                    etf_docs = await etf_cursor.to_list(length=etf_limit)
                    for d in (etf_docs or []):
                        c_norm = StockUtils.normalize_symbol(d.get("code") or d.get("symbol"))
                        if not c_norm:
                            continue
                        etf_set.add(c_norm)
                        etf_name = d.get("name") or d.get("fund_name") or d.get("etf_name")
                        if etf_name and c_norm not in etf_name_map:
                            etf_name_map[c_norm] = etf_name

                for it in items:
                    raw_code = it.get("stock_code")
                    c_norm = StockUtils.normalize_symbol(raw_code) or str(raw_code or "").strip()
                    is_etf = (
                        c_norm in etf_set
                        or str(it.get("market") or "").strip().upper() == "ETF"
                        or StockUtils.identify_security_type(c_norm) == SecurityType.FUND
                    )
                    if is_etf:
                        it["security_type"] = "ETF"
                        it["board"] = "ETF"
                        it["exchange"] = _resolve_etf_exchange(c_norm)
                        if not it.get("stock_name") or it["stock_name"] == raw_code:
                            it["stock_name"] = etf_name_map.get(c_norm) or it.get("stock_name") or raw_code
                    elif c_norm in basic_map:
                        basic = basic_map[c_norm]
                        it["board"] = basic.get("market", "-")
                        it["exchange"] = basic.get("sse", "-")
                        if (not it.get("stock_name") or it["stock_name"] == raw_code) and basic.get("name"):
                            it["stock_name"] = basic.get("name")
                    else:
                        it["board"] = "-"
                        it["exchange"] = "-"
            except Exception as e:
                logger.warning(f"⚠️ 批量获取基础信息失败: {e}")
                # 查询失败时设置默认值（仍尽量按前缀识别 ETF 交易所）
                for it in items:
                    c_norm = StockUtils.normalize_symbol(it.get("stock_code"))
                    if StockUtils.identify_security_type(c_norm) == SecurityType.FUND:
                        it["security_type"] = "ETF"
                        it["board"] = "ETF"
                        it["exchange"] = _resolve_etf_exchange(c_norm)
                    else:
                        it["board"] = "-"
                        it["exchange"] = "-"

        # 批量获取行情（按市场/证券类型分别处理，ETF 查 etf_daily_quotes，A股 market_quotes → stock_daily_quotes 回退）
        if codes:
            try:
                etf_items: List[Dict[str, Any]] = []
                a_share_items: List[Dict[str, Any]] = []
                hk_items: List[Dict[str, Any]] = []
                us_items: List[Dict[str, Any]] = []
                
                for it in items:
                    market = str(it.get("market") or "A股").strip()
                    raw_code = it.get("stock_code") or ""
                    c_norm = StockUtils.normalize_symbol(raw_code) or str(raw_code).strip()
                    detected_market = StockUtils.identify_stock_market(raw_code)
                    if market in ("港股", "HK") or detected_market == StockMarket.HONG_KONG:
                        hk_items.append(it)
                    elif market in ("美股", "US") or detected_market == StockMarket.US:
                        us_items.append(it)
                    elif (
                        market.upper() == "ETF"
                        or it.get("security_type") == "ETF"
                        or it.get("board") == "ETF"
                        or StockUtils.identify_security_type(c_norm) == SecurityType.FUND
                    ):
                        etf_items.append(it)
                    else:
                        a_share_items.append(it)

                # 1. 处理 ETF：从 etf_daily_quotes 最新日线回填现价/涨跌幅/quote_date（缺失时兼容回查 market_quotes）
                if etf_items:
                    etf_codes = [
                        StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code")).strip()
                        for it in etf_items
                        if it.get("stock_code")
                    ]
                    if etf_codes:
                        etf_quotes_map = await self._fetch_latest_daily_quotes(
                            db, "etf_daily_quotes", etf_codes, daily_period_only=False
                        )
                        missing_etf_codes: List[str] = []
                        for it in etf_items:
                            c_norm = StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code") or "").strip()
                            metrics = etf_quotes_map.get(c_norm)
                            if metrics:
                                it["current_price"] = metrics["current_price"]
                                it["change_percent"] = metrics["change_percent"]
                                if metrics["volume"] is not None:
                                    it["volume"] = metrics["volume"]
                                it["quote_date"] = metrics["quote_date"]
                            else:
                                missing_etf_codes.append(c_norm)

                        if missing_etf_codes:
                            try:
                                mq_coll = db["market_quotes"]
                                mq_limit = max(len(missing_etf_codes) * 2, 20)
                                mq_docs = await mq_coll.find(
                                    {"$or": [{"code": {"$in": missing_etf_codes}}, {"symbol": {"$in": missing_etf_codes}}]},
                                    {"code": 1, "symbol": 1, "close": 1, "price": 1, "pct_chg": 1, "change_percent": 1, "volume": 1, "trade_date": 1, "updated_at": 1, "_id": 0}
                                ).limit(mq_limit).to_list(length=mq_limit)
                                mq_map = {
                                    (StockUtils.normalize_symbol(d.get("code") or d.get("symbol")) or ""): _extract_quote_metrics(d)
                                    for d in (mq_docs or [])
                                }
                                for it in etf_items:
                                    if it.get("current_price") is not None:
                                        continue
                                    c_norm = StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code") or "").strip()
                                    metrics = mq_map.get(c_norm)
                                    if metrics:
                                        it["current_price"] = metrics["current_price"]
                                        it["change_percent"] = metrics["change_percent"]
                                        if metrics["volume"] is not None:
                                            it["volume"] = metrics["volume"]
                                        it["quote_date"] = metrics["quote_date"]
                            except Exception as e:
                                logger.warning(f"⚠️ ETF 回查 market_quotes 失败: {e}")

                # 2. 处理 A 股：优先 market_quotes，缺失或无价格时自动回退 stock_daily_quotes 最新日线
                if a_share_items:
                    a_share_codes = [
                        StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code")).strip()
                        for it in a_share_items
                        if it.get("stock_code")
                    ]
                    if a_share_codes:
                        quotes_map: Dict[str, Dict[str, Any]] = {}
                        try:
                            coll = db["market_quotes"]
                            mq_limit = max(len(a_share_codes) * 2, 20)
                            cursor = coll.find(
                                {"$or": [{"code": {"$in": a_share_codes}}, {"symbol": {"$in": a_share_codes}}]},
                                {"code": 1, "symbol": 1, "close": 1, "price": 1, "pct_chg": 1, "change_percent": 1, "volume": 1, "amount": 1, "trade_date": 1, "updated_at": 1, "_id": 0}
                            ).limit(mq_limit)
                            docs = await cursor.to_list(length=mq_limit)
                            for d in (docs or []):
                                c_norm = StockUtils.normalize_symbol(d.get("code") or d.get("symbol"))
                                if c_norm:
                                    quotes_map[c_norm] = _extract_quote_metrics(d)
                        except Exception as e:
                            logger.warning(f"⚠️ 查询A股 market_quotes 失败: {e}")

                        fallback_codes: List[str] = []
                        for it in a_share_items:
                            c_norm = StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code") or "").strip()
                            metrics = quotes_map.get(c_norm)
                            if metrics and metrics.get("current_price") is not None:
                                it["current_price"] = metrics["current_price"]
                                it["change_percent"] = metrics["change_percent"]
                                if metrics["volume"] is not None:
                                    it["volume"] = metrics["volume"]
                                it["quote_date"] = metrics["quote_date"]
                            else:
                                fallback_codes.append(c_norm)

                        if fallback_codes:
                            daily_map = await self._fetch_latest_daily_quotes(
                                db, "stock_daily_quotes", fallback_codes, daily_period_only=True
                            )
                            for it in a_share_items:
                                if it.get("current_price") is not None:
                                    continue
                                c_norm = StockUtils.normalize_symbol(it.get("stock_code")) or str(it.get("stock_code") or "").strip()
                                metrics = daily_map.get(c_norm)
                                if metrics:
                                    it["current_price"] = metrics["current_price"]
                                    it["change_percent"] = metrics["change_percent"]
                                    if metrics["volume"] is not None:
                                        it["volume"] = metrics["volume"]
                                    it["quote_date"] = metrics["quote_date"]
                
                # 3. 处理港股：优先 market_quotes_hk，缺失回退 stock_daily_quotes_hk
                if hk_items:
                    hk_codes = [str(it.get("stock_code")).strip() for it in hk_items if it.get("stock_code")]
                    if hk_codes:
                        hk_missing: List[str] = []
                        try:
                            coll = db["market_quotes_hk"]
                            hk_limit = max(len(hk_codes) * 2, 20)
                            cursor = coll.find(
                                {"$or": [{"code": {"$in": hk_codes}}, {"symbol": {"$in": hk_codes}}]},
                                {"code": 1, "symbol": 1, "close": 1, "price": 1, "pct_chg": 1, "change_percent": 1, "volume": 1, "amount": 1, "trade_date": 1, "updated_at": 1, "_id": 0}
                            ).limit(hk_limit)
                            docs = await cursor.to_list(length=hk_limit)
                            quotes_map = {str(d.get("code") or d.get("symbol")): _extract_quote_metrics(d) for d in (docs or [])}
                            for it in hk_items:
                                code = str(it.get("stock_code")).strip()
                                metrics = quotes_map.get(code)
                                if metrics and metrics.get("current_price") is not None:
                                    it["current_price"] = metrics["current_price"]
                                    it["change_percent"] = metrics["change_percent"]
                                    if metrics["volume"] is not None:
                                        it["volume"] = metrics["volume"]
                                    it["quote_date"] = metrics["quote_date"]
                                else:
                                    hk_missing.append(code)
                        except Exception as e:
                            logger.warning(f"⚠️ 查询港股行情失败: {e}")
                            hk_missing = list(hk_codes)
                        if hk_missing:
                            hk_daily_map = await self._fetch_latest_daily_quotes(
                                db, "stock_daily_quotes_hk", hk_missing, daily_period_only=False
                            )
                            for it in hk_items:
                                if it.get("current_price") is not None:
                                    continue
                                code = str(it.get("stock_code")).strip()
                                metrics = hk_daily_map.get(code)
                                if metrics:
                                    it["current_price"] = metrics["current_price"]
                                    it["change_percent"] = metrics["change_percent"]
                                    if metrics["volume"] is not None:
                                        it["volume"] = metrics["volume"]
                                    it["quote_date"] = metrics["quote_date"]
                
                # 4. 处理美股：优先 market_quotes_us，缺失回退 stock_daily_quotes_us
                if us_items:
                    us_codes = [str(it.get("stock_code")).strip().upper() for it in us_items if it.get("stock_code")]
                    if us_codes:
                        us_missing: List[str] = []
                        try:
                            coll = db["market_quotes_us"]
                            us_limit = max(len(us_codes) * 2, 20)
                            cursor = coll.find(
                                {"$or": [{"code": {"$in": us_codes}}, {"symbol": {"$in": us_codes}}]},
                                {"code": 1, "symbol": 1, "close": 1, "price": 1, "pct_chg": 1, "change_percent": 1, "volume": 1, "amount": 1, "trade_date": 1, "updated_at": 1, "_id": 0}
                            ).limit(us_limit)
                            docs = await cursor.to_list(length=us_limit)
                            quotes_map = {str(d.get("code") or d.get("symbol")).upper(): _extract_quote_metrics(d) for d in (docs or [])}
                            for it in us_items:
                                code = str(it.get("stock_code")).strip().upper()
                                metrics = quotes_map.get(code)
                                if metrics and metrics.get("current_price") is not None:
                                    it["current_price"] = metrics["current_price"]
                                    it["change_percent"] = metrics["change_percent"]
                                    if metrics["volume"] is not None:
                                        it["volume"] = metrics["volume"]
                                    it["quote_date"] = metrics["quote_date"]
                                else:
                                    us_missing.append(code)
                        except Exception as e:
                            logger.warning(f"⚠️ 查询美股行情失败: {e}")
                            us_missing = list(us_codes)
                        if us_missing:
                            us_daily_map = await self._fetch_latest_daily_quotes(
                                db, "stock_daily_quotes_us", us_missing, daily_period_only=False
                            )
                            for it in us_items:
                                if it.get("current_price") is not None:
                                    continue
                                code = str(it.get("stock_code")).strip().upper()
                                metrics = us_daily_map.get(code)
                                if metrics:
                                    it["current_price"] = metrics["current_price"]
                                    it["change_percent"] = metrics["change_percent"]
                                    if metrics["volume"] is not None:
                                        it["volume"] = metrics["volume"]
                                    it["quote_date"] = metrics["quote_date"]
                            
            except Exception as e:
                logger.warning(f"⚠️ 批量获取行情失败: {e}")
                # 查询失败时保持占位 None，避免影响基础功能
                pass

        # 记录性能日志
        elapsed_time = time.time() - start_time
        logger.info(f"✅ [get_user_favorites] 查询完成: user_id={user_id}, 返回 {len(items)} 只股票关注列表, 耗时 {elapsed_time:.2f}秒")
        if elapsed_time > 5:
            logger.warning(f"⚠️ [get_user_favorites] 接口耗时较长: {elapsed_time:.2f}秒，可能存在性能问题")

        return items

    async def add_favorite(
        self,
        user_id: str,
        stock_code: str,
        stock_name: str,
        market: str = "A股",
        tags: List[str] = None,
        notes: str = "",
        alert_price_high: Optional[float] = None,
        alert_price_low: Optional[float] = None
    ) -> bool:
        """添加股票到股票关注列表（兼容字符串ID与ObjectId）"""
        import logging
        logger = logging.getLogger("webapi")

        try:
            logger.info(f"🔧 [add_favorite] 开始添加到研究列表: user_id={user_id}, stock_code={stock_code}")

            db = await self._get_db()
            logger.info(f"🔧 [add_favorite] 数据库连接获取成功")

            normalized_tags = await self._ensure_tags_exist(user_id, tags)

            favorite_stock = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "market": market,
                "added_at": datetime.utcnow(),
                "tags": normalized_tags,
                "notes": notes,
                "alert_price_high": alert_price_high,
                "alert_price_low": alert_price_low
            }

            logger.info(f"🔧 [add_favorite] 股票关注列表数据构建完成: {favorite_stock}")

            is_oid = self._is_valid_object_id(user_id)
            logger.info(f"🔧 [add_favorite] 用户ID类型检查: is_valid_object_id={is_oid}")

            if is_oid:
                logger.info(f"🔧 [add_favorite] 使用 ObjectId 方式添加到 users 集合")

                # 先尝试使用 ObjectId 查询
                result = await db.users.update_one(
                    {"_id": ObjectId(user_id)},
                    {
                        "$push": {"favorite_stocks": favorite_stock},
                        "$setOnInsert": {"favorite_stocks": []}
                    }
                )
                logger.info(f"🔧 [add_favorite] ObjectId查询结果: matched_count={result.matched_count}, modified_count={result.modified_count}")

                # 如果 ObjectId 查询失败，尝试使用字符串查询
                if result.matched_count == 0:
                    logger.info(f"🔧 [add_favorite] ObjectId查询失败，尝试使用字符串ID查询")
                    result = await db.users.update_one(
                        {"_id": user_id},
                        {
                            "$push": {"favorite_stocks": favorite_stock}
                        }
                    )
                    logger.info(f"🔧 [add_favorite] 字符串ID查询结果: matched_count={result.matched_count}, modified_count={result.modified_count}")

                success = result.matched_count > 0
                logger.info(f"🔧 [add_favorite] 返回结果: {success}")
                return success
            else:
                logger.info(f"🔧 [add_favorite] 使用字符串ID方式添加到 user_favorites 集合")
                result = await db.user_favorites.update_one(
                    {"user_id": user_id},
                    {
                        "$setOnInsert": {"user_id": user_id, "created_at": datetime.utcnow()},
                        "$push": {"favorites": favorite_stock},
                        "$set": {"updated_at": datetime.utcnow()}
                    },
                    upsert=True
                )
                logger.info(f"🔧 [add_favorite] 更新结果: matched_count={result.matched_count}, modified_count={result.modified_count}, upserted_id={result.upserted_id}")
                logger.info(f"🔧 [add_favorite] 返回结果: True")
                return True
        except Exception as e:
            logger.error(f"❌ [add_favorite] 添加到研究列表异常: {type(e).__name__}: {str(e)}", exc_info=True)
            raise

    async def remove_favorite(self, user_id: str, stock_code: str) -> bool:
        """从股票关注列表中移除股票（兼容字符串ID与ObjectId）"""
        db = await self._get_db()

        if self._is_valid_object_id(user_id):
            # 先尝试使用 ObjectId 查询
            result = await db.users.update_one(
                {"_id": ObjectId(user_id)},
                {"$pull": {"favorite_stocks": {"stock_code": stock_code}}}
            )
            # 如果 ObjectId 查询失败，尝试使用字符串查询
            if result.matched_count == 0:
                result = await db.users.update_one(
                    {"_id": user_id},
                    {"$pull": {"favorite_stocks": {"stock_code": stock_code}}}
                )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {"user_id": user_id},
                {
                    "$pull": {"favorites": {"stock_code": stock_code}},
                    "$set": {"updated_at": datetime.utcnow()}
                }
            )
            return result.modified_count > 0

    async def update_favorite(
        self,
        user_id: str,
        stock_code: str,
        tags: Optional[List[str]] = None,
        notes: Optional[str] = None,
        alert_price_high: Optional[float] = None,
        alert_price_low: Optional[float] = None,
        clear_alert_price_low: bool = False,
    ) -> bool:
        """更新股票关注列表信息（兼容字符串ID与ObjectId）"""
        db = await self._get_db()

        # 统一构建更新字段（根据不同集合的字段路径设置前缀）
        is_oid = self._is_valid_object_id(user_id)
        prefix = "favorite_stocks.$." if is_oid else "favorites.$."
        update_fields: Dict[str, Any] = {}
        if tags is not None:
            normalized_tags = await self._ensure_tags_exist(user_id, tags)
            update_fields[prefix + "tags"] = normalized_tags
        if notes is not None:
            update_fields[prefix + "notes"] = notes
        if alert_price_high is not None:
            update_fields[prefix + "alert_price_high"] = alert_price_high
        if clear_alert_price_low:
            update_fields[prefix + "alert_price_low"] = None
        elif alert_price_low is not None:
            update_fields[prefix + "alert_price_low"] = alert_price_low

        if not update_fields:
            return True

        if is_oid:
            result = await db.users.update_one(
                {
                    "_id": ObjectId(user_id),
                    "favorite_stocks.stock_code": stock_code
                },
                {"$set": update_fields}
            )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {
                    "user_id": user_id,
                    "favorites.stock_code": stock_code
                },
                {
                    "$set": {
                        **update_fields,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0

    async def is_favorite(self, user_id: str, stock_code: str) -> bool:
        """检查股票是否在股票关注列表中（兼容字符串ID与ObjectId）"""
        import logging
        logger = logging.getLogger("webapi")

        try:
            logger.info(f"🔧 [is_favorite] 检查股票关注列表: user_id={user_id}, stock_code={stock_code}")

            db = await self._get_db()

            is_oid = self._is_valid_object_id(user_id)
            logger.info(f"🔧 [is_favorite] 用户ID类型: is_valid_object_id={is_oid}")

            if is_oid:
                # 先尝试使用 ObjectId 查询
                user = await db.users.find_one(
                    {
                        "_id": ObjectId(user_id),
                        "favorite_stocks.stock_code": stock_code
                    }
                )

                # 如果 ObjectId 查询失败，尝试使用字符串查询
                if user is None:
                    logger.info(f"🔧 [is_favorite] ObjectId查询未找到，尝试使用字符串ID查询")
                    user = await db.users.find_one(
                        {
                            "_id": user_id,
                            "favorite_stocks.stock_code": stock_code
                        }
                    )

                result = user is not None
                logger.info(f"🔧 [is_favorite] 查询结果: {result}")
                return result
            else:
                doc = await db.user_favorites.find_one(
                    {
                        "user_id": user_id,
                        "favorites.stock_code": stock_code
                    }
                )
                result = doc is not None
                logger.info(f"🔧 [is_favorite] 字符串ID查询结果: {result}")
                return result
        except Exception as e:
            logger.error(f"❌ [is_favorite] 检查股票关注列表异常: {type(e).__name__}: {str(e)}", exc_info=True)
            raise

    async def get_user_tags(self, user_id: str) -> List[str]:
        """获取用户使用的所有标签（兼容字符串ID与ObjectId）"""
        db = await self._get_db()

        if self._is_valid_object_id(user_id):
            pipeline = [
                {"$match": {"_id": ObjectId(user_id)}},
                {"$unwind": "$favorite_stocks"},
                {"$unwind": "$favorite_stocks.tags"},
                {"$group": {"_id": "$favorite_stocks.tags"}},
                {"$sort": {"_id": 1}}
            ]
            result = await db.users.aggregate(pipeline).to_list(None)
        else:
            pipeline = [
                {"$match": {"user_id": user_id}},
                {"$unwind": "$favorites"},
                {"$unwind": "$favorites.tags"},
                {"$group": {"_id": "$favorites.tags"}},
                {"$sort": {"_id": 1}}
            ]
            result = await db.user_favorites.aggregate(pipeline).to_list(None)

        return [item["_id"] for item in result if item.get("_id")]

    def _get_mock_price(self, stock_code: str) -> float:
        """获取模拟股价"""
        # 基于股票代码生成模拟价格
        base_price = hash(stock_code) % 100 + 10
        return round(base_price + (hash(stock_code) % 1000) / 100, 2)
    
    def _get_mock_change(self, stock_code: str) -> float:
        """获取模拟涨跌幅"""
        # 基于股票代码生成模拟涨跌幅
        change = (hash(stock_code) % 2000 - 1000) / 100
        return round(change, 2)
    
    def _get_mock_volume(self, stock_code: str) -> int:
        """获取模拟成交量"""
        # 基于股票代码生成模拟成交量
        return (hash(stock_code) % 10000 + 1000) * 100


# 创建全局实例
favorites_service = FavoritesService()
