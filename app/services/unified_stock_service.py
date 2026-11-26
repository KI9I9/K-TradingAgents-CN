#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一股票数据服务（跨市场，支持多数据源）

功能：
1. 跨市场数据访问（A股/港股/美股）
2. 多数据源优先级查询
3. 统一的查询接口

设计说明：
- 参考A股多数据源设计
- 同一股票可有多个数据源记录
- 通过 (code, source) 联合查询
- 数据源优先级从数据库配置读取
"""

import logging
import re
from typing import Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase

from tradingagents.utils.stock_utils import StockUtils, SecurityType

logger = logging.getLogger("webapi")


class UnifiedStockService:
    """统一股票数据服务（跨市场，支持多数据源）"""

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        
        # 集合映射
        self.collection_map = {
            "CN": {
                "basic_info": "stock_basic_info",
                "quotes": "market_quotes",
                "daily": "stock_daily_quotes",
                "financial": "stock_financial_data",
                "news": "stock_news"
            },
            "HK": {
                "basic_info": "stock_basic_info_hk",
                "quotes": "market_quotes_hk",
                "daily": "stock_daily_quotes_hk",
                "financial": "stock_financial_data_hk",
                "news": "stock_news_hk"
            },
            "US": {
                "basic_info": "stock_basic_info_us",
                "quotes": "market_quotes_us",
                "daily": "stock_daily_quotes_us",
                "financial": "stock_financial_data_us",
                "news": "stock_news_us"
            },
            "ETF": {
                "basic_info": "etf_basic_info",
                "quotes": "etf_daily_quotes",  # ETF 行情落库于 etf_daily_quotes（非 market_quotes）
                "daily": "etf_daily_quotes",
                "financial": None,  # ETF 无 PE/PB/ROE
                "news": "stock_news"
            }
        }

    async def get_stock_info(
        self, 
        market: str, 
        code: str, 
        source: Optional[str] = None
    ) -> Optional[Dict]:
        """
        获取股票基础信息（支持多数据源）
        
        Args:
            market: 市场类型 (CN/HK/US/ETF)
            code: 股票代码
            source: 指定数据源（可选）
        
        Returns:
            股票基础信息字典
        """
        market = market.upper()
        norm_code = StockUtils.normalize_symbol(code) or str(code or "").strip()
        if market == "CN" and StockUtils.identify_security_type(norm_code) == SecurityType.FUND:
            etf_doc = await self.get_stock_info("ETF", norm_code, source=source)
            if etf_doc:
                return etf_doc

        collection_name = self.collection_map[market]["basic_info"]
        collection = self.db[collection_name]
        code_filter = {"$or": [{"code": norm_code}, {"symbol": norm_code}]}
        
        if source:
            # 指定数据源
            query = {**code_filter, "source": source}
            doc = await collection.find_one(query, {"_id": 0})
            if doc:
                logger.debug(f"✅ 使用指定数据源: {source}")
        else:
            # 🔥 按优先级查询（参考A股设计）
            source_priority = await self._get_source_priority(market)
            doc = None
            
            for src in source_priority:
                query = {**code_filter, "source": src}
                doc = await collection.find_one(query, {"_id": 0})
                if doc:
                    logger.debug(f"✅ 使用数据源: {src} (优先级查询)")
                    break
            
            # 如果没有找到，尝试不指定source查询（兼容旧数据）
            if not doc:
                doc = await collection.find_one(code_filter, {"_id": 0})
                if doc:
                    logger.debug(f"✅ 使用默认数据源（兼容模式）")
        
        if doc:
            doc = dict(doc)
            is_etf = market == "ETF" or StockUtils.identify_security_type(norm_code) == SecurityType.FUND
            doc["security_type"] = "ETF" if is_etf else "STOCK"
            doc.setdefault("code", norm_code)
            doc.setdefault("symbol", norm_code)
            if is_etf:
                doc["market"] = "ETF"
        return doc

    async def _get_source_priority(self, market: str) -> List[str]:
        """
        从数据库获取数据源优先级
        
        Args:
            market: 市场类型 (CN/HK/US/ETF)
        
        Returns:
            数据源优先级列表
        """
        if market == "ETF":
            return ["akshare", "tushare", "qmt"]

        market_category_map = {
            "CN": "a_shares",
            "HK": "hk_stocks",
            "US": "us_stocks"
        }

        market_category_id = market_category_map.get(market)
        
        try:
            # 从 datasource_groupings 集合查询（加 limit 防全表扫描）
            groupings = await self.db.datasource_groupings.find({
                "market_category_id": market_category_id,
                "enabled": True
            }).sort("priority", -1).limit(50).to_list(length=50)
            
            if groupings:
                priority_list = [g["data_source_name"] for g in groupings if g.get("data_source_name")]
                if priority_list:
                    logger.debug(f"📊 {market} 数据源优先级（从数据库）: {priority_list}")
                    return priority_list
        except Exception as e:
            logger.warning(f"⚠️ 从数据库读取数据源优先级失败: {e}")
        
        # 默认优先级
        default_priority = {
            "CN": ["tushare", "multi_source", "akshare", "baostock"],
            "HK": ["yfinance_hk", "akshare_hk"],
            "US": ["yfinance_us"]
        }
        priority_list = default_priority.get(market, [])
        logger.debug(f"📊 {market} 数据源优先级（默认）: {priority_list}")
        return priority_list

    async def get_stock_quote(self, market: str, code: str) -> Optional[Dict]:
        """
        获取实时/最新行情（ETF 查 etf_daily_quotes 最新日线；A 股缺失实时行情时回退最新日线）
        
        Args:
            market: 市场类型 (CN/HK/US/ETF)
            code: 股票代码
        
        Returns:
            行情字典
        """
        market = market.upper()
        norm_code = StockUtils.normalize_symbol(code) or str(code or "").strip()
        is_etf = market == "ETF" or (
            market == "CN" and StockUtils.identify_security_type(norm_code) == SecurityType.FUND
        )
        code_filter = {"$or": [{"code": norm_code}, {"symbol": norm_code}]}

        if is_etf:
            etf_collection = self.db[self.collection_map["ETF"]["quotes"]]
            cursor = etf_collection.find(code_filter, {"_id": 0}).sort("trade_date", -1).limit(1)
            docs = await cursor.to_list(length=1)
            if docs:
                doc = dict(docs[0])
                doc.setdefault("code", norm_code)
                doc["market"] = "ETF"
                doc["security_type"] = "ETF"
                if doc.get("trade_date") and not doc.get("quote_date"):
                    doc["quote_date"] = str(doc["trade_date"])[:10]
                return doc
            return None

        collection_name = self.collection_map[market]["quotes"]
        collection = self.db[collection_name]
        doc = await collection.find_one(code_filter, {"_id": 0})
        if doc:
            doc = dict(doc)
            doc.setdefault("code", norm_code)
            doc["security_type"] = "STOCK"
            return doc

        # 回退最新日线行情
        daily_name = self.collection_map[market].get("daily")
        if daily_name:
            daily_coll = self.db[daily_name]
            daily_query: Dict = code_filter
            if market == "CN":
                daily_query = {
                    "$and": [
                        code_filter,
                        {"$or": [{"period": "daily"}, {"period": {"$exists": False}}, {"period": None}]},
                    ]
                }
            cursor = daily_coll.find(daily_query, {"_id": 0}).sort("trade_date", -1).limit(1)
            daily_docs = await cursor.to_list(length=1)
            if daily_docs:
                latest = dict(daily_docs[0])
                latest.setdefault("code", norm_code)
                latest["security_type"] = "STOCK"
                if latest.get("trade_date") and not latest.get("quote_date"):
                    latest["quote_date"] = str(latest["trade_date"])[:10]
                return latest
        return None

    async def _search_single_market(
        self,
        target_market: str,
        escaped_query: str,
        fetch_limit: int,
    ) -> Dict[str, Dict]:
        """在单个市场的 basic_info 集合中执行转义正则 + 有界 limit 检索并按数据源优先级去重。"""
        collection_name = self.collection_map[target_market]["basic_info"]
        collection = self.db[collection_name]

        or_clauses = [
            {"code": {"$regex": escaped_query, "$options": "i"}},
            {"symbol": {"$regex": escaped_query, "$options": "i"}},
            {"name": {"$regex": escaped_query, "$options": "i"}},
            {"name_en": {"$regex": escaped_query, "$options": "i"}},
        ]
        if target_market == "ETF":
            or_clauses.extend([
                {"fund_name": {"$regex": escaped_query, "$options": "i"}},
                {"etf_name": {"$regex": escaped_query, "$options": "i"}},
            ])
        filter_query = {"$or": or_clauses}

        cursor = collection.find(filter_query, {"_id": 0}).limit(fetch_limit)
        matched_docs = await cursor.to_list(length=fetch_limit)
        if not matched_docs:
            return {}

        source_priority = await self._get_source_priority(target_market)
        priority_rank = {src.lower(): idx for idx, src in enumerate(source_priority)}
        unique_results: Dict[str, Dict] = {}

        for raw_doc in matched_docs:
            doc = dict(raw_doc)
            doc.pop("_id", None)
            code = str(doc.get("code") or doc.get("symbol") or "").strip()
            if not code:
                continue

            src_key = str(doc.get("source") or "").strip().lower()
            cur_rank = priority_rank.get(src_key, len(priority_rank) + 1)

            if code not in unique_results:
                unique_results[code] = (cur_rank, doc)
            else:
                prev_rank, _ = unique_results[code]
                if cur_rank < prev_rank:
                    unique_results[code] = (cur_rank, doc)

        normalized: Dict[str, Dict] = {}
        for code, (_, doc) in unique_results.items():
            is_etf = (
                target_market == "ETF"
                or StockUtils.identify_security_type(code) == SecurityType.FUND
            )
            item = dict(doc)
            item["code"] = code
            item["symbol"] = str(item.get("symbol") or code).strip()
            item["name"] = item.get("name") or item.get("fund_name") or item.get("etf_name") or code
            if target_market == "CN" and item.get("market") and item.get("market") not in ("CN", "ETF", "HK", "US"):
                item.setdefault("board", item.get("market"))
            item["security_type"] = "ETF" if is_etf else "STOCK"
            item["market"] = "ETF" if is_etf else target_market
            normalized[code] = item

        return normalized

    async def search_stocks(
        self, 
        market: str, 
        query: str, 
        limit: int = 20,
        include_etf: bool = True,
    ) -> List[Dict]:
        """
        搜索股票/ETF（转义正则、限制单次扫描上限、按数据源优先级去重）
        
        Args:
            market: 市场类型 (CN/HK/US/ETF)
            query: 搜索关键词
            limit: 返回数量限制
            include_etf: 当 market == "CN" 时是否默认合并 etf_basic_info 检索结果
        
        Returns:
            股票/ETF 列表，每项均携带 security_type ("ETF" | "STOCK") 与 market
        """
        market = market.upper()
        raw_query = str(query or "").strip()
        if not raw_query:
            return []

        normalized_query = StockUtils.normalize_symbol(raw_query) or raw_query
        if market == "HK" and normalized_query.upper().endswith(".HK"):
            normalized_query = normalized_query[:-3]
        escaped_query = re.escape(normalized_query)
        safe_limit = max(1, min(int(limit or 20), 100))
        fetch_limit = min(max(safe_limit * 5, 50), 500)

        merged: Dict[str, Dict] = {}
        primary_hits = await self._search_single_market(market, escaped_query, fetch_limit)
        merged.update(primary_hits)

        if market == "CN" and include_etf:
            etf_hits = await self._search_single_market("ETF", escaped_query, fetch_limit)
            for code, etf_item in etf_hits.items():
                # 若同代码同时出现在两表，ETF 记录优先覆盖以保证 security_type="ETF" / market="ETF"
                merged[code] = etf_item

        if not merged:
            return []

        q_lower = normalized_query.lower()

        def _sort_key(item: Dict) -> tuple:
            code_val = str(item.get("code") or "").lower()
            name_val = str(item.get("name") or "").lower()
            if code_val == q_lower:
                rank = 0
            elif code_val.startswith(q_lower):
                rank = 1
            elif name_val == q_lower:
                rank = 2
            elif name_val.startswith(q_lower):
                rank = 3
            else:
                rank = 4
            return (rank, code_val)

        result_list = sorted(merged.values(), key=_sort_key)[:safe_limit]
        logger.info(f"🔍 搜索 {market} 市场: '{raw_query}' -> {len(result_list)} 条结果（已去重）")
        return result_list

    async def get_daily_quotes(
        self,
        market: str,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        获取历史K线数据
        
        Args:
            market: 市场类型 (CN/HK/US/ETF)
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            limit: 返回数量限制
        
        Returns:
            K线数据列表
        """
        market = market.upper()
        norm_code = StockUtils.normalize_symbol(code) or str(code or "").strip()
        collection_name = self.collection_map[market]["daily"]
        collection = self.db[collection_name]
        
        query: Dict = {"$or": [{"code": norm_code}, {"symbol": norm_code}]}
        if start_date or end_date:
            query["trade_date"] = {}
            if start_date:
                query["trade_date"]["$gte"] = start_date
            if end_date:
                query["trade_date"]["$lte"] = end_date
        
        safe_limit = max(1, min(int(limit or 100), 1000))
        cursor = collection.find(query, {"_id": 0}).sort("trade_date", -1).limit(safe_limit)
        return await cursor.to_list(length=safe_limit)

    async def get_supported_markets(self) -> List[Dict]:
        """
        获取支持的市场列表
        
        Returns:
            市场列表
        """
        return [
            {
                "code": "CN",
                "name": "A股",
                "name_en": "China A-Share",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            },
            {
                "code": "ETF",
                "name": "场内ETF",
                "name_en": "China ETF",
                "currency": "CNY",
                "timezone": "Asia/Shanghai"
            },
            {
                "code": "HK",
                "name": "港股",
                "name_en": "Hong Kong Stock",
                "currency": "HKD",
                "timezone": "Asia/Hong_Kong"
            },
            {
                "code": "US",
                "name": "美股",
                "name_en": "US Stock",
                "currency": "USD",
                "timezone": "America/New_York"
            }
        ]

