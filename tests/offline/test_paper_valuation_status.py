"""Real account, position and preview bodies; no database/network writes."""
import asyncio
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import tempfile
import types
from typing import List
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, '.')
from test_paper_pricing import load_paper_functions, NOW
from app.services import paper_quote_supply as supply
from app.services.quote_contract import CN_TZ, parse_quote_time


class Cursor:
    def __init__(self, rows): self.rows = rows
    async def to_list(self, _): return list(self.rows)


class Collection:
    def __init__(self, rows): self.rows = rows
    def find(self, query): return Cursor([p for p in self.rows if p['user_id'] == query['user_id']])


def position(code='600000', **changes):
    return dict(dict(user_id='u', code=code, quantity=100, avg_cost=8, market='CN', currency='CNY'), **changes)


class Valuation(unittest.IsolatedAsyncioTestCase):
    def namespace(self, rows, prices):
        extra = dict(List=List, parse_quote_time=parse_quote_time, CN_TZ=CN_TZ,
            router=types.SimpleNamespace(get=lambda *a, **kw: lambda f: f),
            Depends=lambda *a, **kw: None, get_current_user=None, ok=lambda value: value,
            _get_or_create_account=AsyncMock(return_value={'cash': {'CNY':1000,'HKD':500,'USD':100},'realized_pnl':{}}))
        ns = load_paper_functions(('_position_valuation','get_account','list_positions'), {}, NOW, extra)
        ns['get_mongo_db'] = lambda: {'paper_positions':Collection(rows)}
        async def quote(code, market, strict):
            self.assertFalse(strict)  # Display valuation must not consume on-demand execution quota.
            return prices.get(code, (None,None,'display_unavailable'))
        ns['_get_last_price_detail'] = AsyncMock(side_effect=quote)
        return ns

    async def test_missing_one_price_makes_only_affected_currency_incomplete(self):
        rows = [position(),position('000001'),position('00700',market='HK',currency='HKD')]
        basis = {'source':'stock_daily_quotes','channel':'display','trade_date':'2026-10-26',
                 'as_of':datetime(2026,10,26,10)}  # BSON naive UTC -> explicit Beijing timestamp.
        ns = self.namespace(rows, {'600000':(10,basis,'display'),'00700':(20,None,'display')})
        result = await ns['get_account']({'id':'u'})
        account = result['account']; items = result['positions']
        self.assertIsNone(account['positions_value']['CNY']); self.assertIsNone(account['equity']['CNY'])
        self.assertEqual(account['cash']['CNY'],1000)
        self.assertEqual(account['valuation']['CNY'],{'complete':False,'missing_count':1,'known_positions_value':1000})
        self.assertEqual(account['positions_value']['HKD'],2000); self.assertEqual(account['equity']['HKD'],2500)
        self.assertEqual(items[0]['valuation_basis']['as_of'],'2026-10-26T18:00:00+08:00')
        self.assertIsNone(items[1]['market_value']); self.assertIsNone(items[1]['unrealized_pnl'])
        listed = await ns['list_positions']({'id':'u'})
        self.assertEqual(listed['items'], items)

    async def test_valid_zero_pnl_and_empty_portfolio_remain_zero(self):
        ns = self.namespace([position()],{'600000':(8,None,'display')})
        result = await ns['get_account']({'id':'u'})
        self.assertEqual(result['positions'][0]['unrealized_pnl'],0)
        self.assertEqual(result['account']['equity']['CNY'],1800)
        empty = self.namespace([position(quantity=0)],{})
        result = await empty['get_account']({'id':'u'})
        self.assertEqual(result['account']['positions_value']['CNY'],0)
        self.assertEqual(result['account']['equity']['CNY'],1000)
        self.assertTrue(result['account']['valuation']['CNY']['complete'])
        empty['_get_last_price_detail'].assert_not_called()

    async def test_all_missing_prices_do_not_manufacture_zero_or_cost_valuation(self):
        ns = self.namespace([position()],{})
        result = await ns['get_account']({'id':'u'})
        self.assertIsNone(result['account']['equity']['CNY'])
        self.assertEqual(result['positions'][0]['valuation_status'],'missing')
        self.assertIsNone(result['positions'][0]['last_price'])


class Preview(unittest.IsolatedAsyncioTestCase):
    async def test_provider_failure_reason_survives_real_preview_without_display_fallback(self):
        extra = dict(router=types.SimpleNamespace(get=lambda *a, **kw: lambda f: f),
            Query=lambda *a, **kw: None, Depends=lambda *a, **kw: None, get_current_user=None,
            ok=lambda value: value, _resolve_market_and_code=lambda c,m:('CN',c,False))
        ns = load_paper_functions(('_load_cn_candidates','get_quote'),{},NOW,extra)
        for code in ('security_suspended','outside_continuous_session','paper_supply_timeout','calendar_unverified'):
            with patch.object(supply,'get_paper_quote',AsyncMock(return_value=(True,None,code))):
                result = await ns['get_quote']('600000','CN',{'id':'u'})
            self.assertFalse(result['available']); self.assertIsNone(result['current_price'])
            self.assertEqual(result['unavailable_reason']['code'],code)
            self.assertTrue(result['unavailable_reason']['message'])

    async def test_expiry_is_reported_instead_of_an_old_display_price(self):
        extra = dict(router=types.SimpleNamespace(get=lambda *a, **kw: lambda f: f),
            Query=lambda *a, **kw: None, Depends=lambda *a, **kw: None, get_current_user=None,
            ok=lambda value: value, _resolve_market_and_code=lambda c,m:('CN',c,False))
        ns = load_paper_functions(('_load_cn_candidates','get_quote'),
            {'stock_basic_info':{'current_price':88}},NOW,extra)
        with patch.object(supply,'get_paper_quote',AsyncMock(return_value=(False,None,'paper_supply_disabled'))), \
             patch.object(supply,'permit_status',return_value=(None,'paper_supply_expired')):
            result = await ns['get_quote']('600000','CN',{'id':'u'})
        self.assertIsNone(result['current_price'])
        self.assertEqual(result['unavailable_reason']['code'],'paper_supply_expired')

    async def test_allowlisted_error_does_not_expose_provider_exception(self):
        ns = load_paper_functions((),{},NOW)
        result = ns['_quote_failure']('foreign_error:SECRET_CANARY')
        self.assertNotIn('SECRET_CANARY',json.dumps(result))
        self.assertEqual(result['code'],'quote_unavailable')


class PermitStatus(unittest.TestCase):
    def test_reason_reporting_preserves_admission_and_distinguishes_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'permit.json'
            self.assertEqual(supply.permit_status(NOW,path)[1],'paper_supply_disabled')
            doc = dict(version=1,enabled=True,source='sina_l1',scope='paper_execution_sh_sz',
                       not_before=(NOW-timedelta(hours=1)).isoformat(),expires_at=NOW.isoformat())
            path.write_text(json.dumps(doc)); path.chmod(0o600)
            self.assertEqual(supply.permit_status(NOW,path),(None,'paper_supply_expired'))
            self.assertIsNone(supply.read_permit(NOW,path))
            doc.update(not_before=(NOW+timedelta(hours=1)).isoformat(),expires_at=(NOW+timedelta(hours=2)).isoformat())
            path.write_text(json.dumps(doc))
            self.assertEqual(supply.permit_status(NOW,path)[1],'paper_supply_not_started')
            doc['enabled'] = False; path.write_text(json.dumps(doc))
            self.assertEqual(supply.permit_status(NOW,path)[1],'paper_supply_disabled')
            path.write_bytes(b'invalid')
            self.assertEqual(supply.permit_status(NOW,path)[1],'paper_supply_invalid')


if __name__ == '__main__': unittest.main()
