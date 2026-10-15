"""ETF 校验及基础信息备用来源的回归测试。"""
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.data_validation_service import DataValidationService
from app.worker.etf_sync_service import _fetch_etf_spot_list_akshare


class ETFValidationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = MagicMock()
        self.service = DataValidationService()
        self.service.db = self.db

    async def test_etf_uses_own_basic_collection(self):
        self.db.__getitem__.return_value.find_one = AsyncMock(return_value={"name": "纳指ETF广发"})
        result = await self.service._check_basic_info(self.db, '159941', 'etf')
        self.assertTrue(result[0])
        self.db.__getitem__.assert_called_once_with('etf_basic_info')

    async def test_stock_collection_unchanged(self):
        self.db.__getitem__.return_value.find_one = AsyncMock(return_value={"name": "平安银行"})
        result = await self.service._check_basic_info(self.db, '000001', 'cn')
        self.assertTrue(result[0])
        self.db.__getitem__.assert_called_once_with('stock_basic_info')

    async def test_missing_etf_does_not_fall_back_to_stock(self):
        self.db.__getitem__.return_value.find_one = AsyncMock(return_value=None)
        result = await self.service.validate_stock_data('159941', '2026-10-15', 'ETF')
        self.assertFalse(result.is_valid)
        self.assertEqual(result.details['basic_info']['collection'], 'etf_basic_info')
        self.assertIn('ETF 基础数据', result.message)

    async def test_financial_check_is_skipped(self):
        self.service._check_basic_info = AsyncMock(return_value=(True, '', {}))
        self.service._check_financial_data = AsyncMock(side_effect=AssertionError('不应读取股票财务'))
        result = await self.service.validate_stock_data('159941', '2026-10-15', 'ETF', check_financial_data=True)
        self.assertTrue(result.is_valid)
        self.assertTrue(result.details['financial_data']['skipped'])
        self.service._check_financial_data.assert_not_called()

    async def test_missing_etf_history_never_reads_stock_history(self):
        self.db.list_collection_names = AsyncMock(return_value=['stock_daily_quotes'])
        result = await self.service._check_historical_data(self.db, '159941', 'etf', '2025-10-15', '2026-10-15', '2026-10-15')
        self.assertFalse(result[0])
        self.assertEqual(result[2]['collections_checked'], ['etf_daily_quotes'])
        self.db.__getitem__.assert_not_called()

    async def test_etf_history_and_freshness(self):
        self.db.list_collection_names = AsyncMock(return_value=['etf_daily_quotes'])
        coll = self.db.__getitem__.return_value
        coll.find_one = AsyncMock(return_value={'code': '159941', 'trade_date': '2026-10-15'})
        coll.count_documents = AsyncMock(return_value=243)
        result = await self.service._check_historical_data(self.db, '159941', 'etf', '2025-10-15', '2026-10-15', '2026-10-15')
        self.assertTrue(result[0])
        fresh = await self.service._check_data_freshness(self.db, '159941', 'etf', '2026-10-15')
        self.assertTrue(fresh[0])
        self.assertEqual(fresh[2]['collection'], 'etf_daily_quotes')
        self.assertTrue(all(c.args == ('etf_daily_quotes',) for c in self.db.__getitem__.call_args_list))

    async def test_insufficient_etf_history_still_fails(self):
        self.db.list_collection_names = AsyncMock(return_value=['etf_daily_quotes'])
        coll = self.db.__getitem__.return_value
        coll.find_one = AsyncMock(return_value={'trade_date': '2026-10-15'})
        coll.count_documents = AsyncMock(return_value=5)
        result = await self.service._check_historical_data(self.db, '159941', 'etf', '2026-10-01', '2026-10-15', '2026-10-15')
        self.assertFalse(result[0])
        self.assertEqual(result[2]['min_required'], 30)


class ETFFallbackTests(unittest.TestCase):
    def test_sina_fallback_normalizes_exchange_prefix(self):
        import pandas as pd
        frame = pd.DataFrame([{'代码': 'sz159941', '名称': '纳指ETF广发'}])
        with patch('akshare.fund_etf_spot_em', side_effect=ConnectionError('离线测试')), patch('akshare.fund_etf_category_sina', return_value=frame):
            result = _fetch_etf_spot_list_akshare()
        self.assertEqual(result[0]['code'], '159941')
        self.assertEqual(result[0]['full_symbol'], '159941.SZ')

    def test_both_sources_failed_returns_no_fabricated_data(self):
        with patch('akshare.fund_etf_spot_em', side_effect=ConnectionError('离线测试')), patch('akshare.fund_etf_category_sina', side_effect=ConnectionError('离线测试')):
            self.assertEqual(_fetch_etf_spot_list_akshare(), [])


if __name__ == '__main__':
    unittest.main()
