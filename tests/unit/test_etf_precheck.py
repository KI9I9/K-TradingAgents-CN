"""分析提交入口必须识别 ETF，且不能绕过缺数据或配置检查。"""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from app.routers.analysis import precheck_analysis
from app.services.task_analysis_service import TaskAnalysisService, AnalysisTaskType


class PrecheckTests(unittest.IsolatedAsyncioTestCase):
    async def check(self, symbol, collection, count=1, found=True, enabled=True):
        db=MagicMock()
        coll=db.__getitem__.return_value
        coll.count_documents=AsyncMock(return_value=count)
        coll.find_one=AsyncMock(return_value={'code':symbol} if found else None)
        config=SimpleNamespace(data_source_configs=[SimpleNamespace(type='akshare',enabled=enabled)])
        with patch('app.routers.analysis.get_mongo_db',return_value=db), patch('app.services.config_service.config_service.get_system_config',new=AsyncMock(return_value=config)):
            result=await precheck_analysis(symbol=symbol,user={'id':'test'})
        if enabled:
            self.assertTrue(all(c.args==(collection,) for c in db.__getitem__.call_args_list))
        return result

    async def test_etf_can_submit_without_stock_basic(self):
        result=await self.check('159941','etf_basic_info')
        self.assertTrue(result['can_submit'])
        self.assertEqual(result['reason'],'ok')

    async def test_stock_still_uses_stock_collection(self):
        self.assertTrue((await self.check('000001','stock_basic_info'))['can_submit'])

    async def test_missing_etf_remains_blocked(self):
        result=await self.check('159941','etf_basic_info',found=False)
        self.assertFalse(result['can_submit'])
        self.assertEqual(result['reason'],'stock_not_found')

    async def test_empty_etf_collection_remains_blocked(self):
        result=await self.check('159941','etf_basic_info',count=0)
        self.assertFalse(result['can_submit'])
        self.assertEqual(result['reason'],'no_basics')

    async def test_no_source_remains_blocked(self):
        result=await self.check('159941','etf_basic_info',enabled=False)
        self.assertFalse(result['can_submit'])
        self.assertEqual(result['reason'],'no_datasource')

    async def test_etf_task_with_cn_market_validates_as_etf(self):
        validator=SimpleNamespace(validate_stock_data=AsyncMock(return_value='passed'))
        task=SimpleNamespace(task_params={'symbol':'159941','market_type':'cn','analysis_date':'2026-10-15'},task_type=AnalysisTaskType.ETF_ANALYSIS)
        with patch('app.services.data_validation_service.get_data_validation_service',return_value=validator):
            result=await TaskAnalysisService._validate_task_data(None,task)
        self.assertEqual(result,'passed')
        self.assertEqual(validator.validate_stock_data.call_args.kwargs['market_type'],'etf')

if __name__=='__main__':
    unittest.main()
