"""Execute the production probe's real module loader/parser with fake HTTPS I/O."""
import json
import hashlib
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'deploy'))
import probe_paper_supply_production as probe


class RealProbe(unittest.TestCase):
    def test_memory_loader_positive_suspension_and_transport_error(self):
        try:
            import httpx
        except ImportError:
            self.skipTest('requires application httpx dependency')
        request={'modules':{}}
        for name in ('quote_contract','paper_quote_supply'):
            raw=(Path(__file__).resolve().parents[2]/'app/services'/(name+'.py')).read_bytes()
            request['modules']['app.services.'+name]=dict(source=raw.decode(),sha256=hashlib.sha256(raw).hexdigest())
        prefix=r'''
import httpx,sys,types
from datetime import datetime
from zoneinfo import ZoneInfo
real_client=httpx.AsyncClient
def handler(request):
 symbol=str(request.url).split('list=')[1]
 if symbol=='sz000001':raise httpx.ConnectError('synthetic failure')
 now=datetime(2026,11,2,14,tzinfo=ZoneInfo('Asia/Shanghai'))
 f=['0']*33
 f[:10]=['test','10','9.8','10.2','11','9','10.1','10.2','123400','1234000']
 f[30:33]=[now.date().isoformat(),now.strftime('%H:%M:%S'),'03' if symbol=='sh601059' else '00']
 return httpx.Response(200,content=('var hq_str_'+symbol+'="'+','.join(f)+'";').encode())
httpx.AsyncClient=lambda **kw:real_client(transport=httpx.MockTransport(handler),**kw)
calendar=types.ModuleType('app.services.trading_calendar_service')
class Calendar:
 async def _fetch_from_akshare(self):return '2026-11-02'
calendar.TradingCalendarService=Calendar
calendar.get_trading_calendar_service=lambda:Calendar()
sys.modules[calendar.__name__]=calendar
'''
        # Fix only the local test's clock, keeping complete source loading and
        # real HTTP adapter/parser logic; no production source file is edited.
        code=probe.INSIDE.replace(
            'from app.services.trading_calendar_service import TradingCalendarService',
            "from app.services.trading_calendar_service import TradingCalendarService\n"
            "cn_now=lambda:datetime(2026,11,2,14,tzinfo=ZoneInfo('Asia/Shanghai'))")
        result=subprocess.run([sys.executable,'-B','-c',prefix+code],
            input=json.dumps(request),text=True,capture_output=True,timeout=10,check=True)
        rows=[s.split(':',1)[1] for s in result.stdout.splitlines() if s.startswith('PAPER_PUBLIC_PROBE:')]
        self.assertEqual(len(rows),1)
        receipt=json.loads(rows[0]);quotes={q['symbol']:q for q in receipt['quotes']}
        self.assertTrue(quotes['600000']['usable'])
        self.assertTrue(quotes['159865']['usable'])
        self.assertEqual(quotes['601059']['reason'],'security_suspended')
        self.assertEqual(quotes['000001']['error_type'],'ConnectError')
        self.assertEqual(receipt['database_writes'],0)
        self.assertFalse(receipt['permit_installed'])
        self.assertIsNotNone(receipt['public_calendar'])


if __name__=='__main__':
    unittest.main(verbosity=2)
