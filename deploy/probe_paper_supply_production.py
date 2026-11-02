"""Bounded read-only public quote/calendar probe in the existing backend.

Candidate modules are loaded in a separate interpreter, without replacing files,
installing a permit, initializing the application or touching database state.
Raw public responses are retained only in the private local receipt.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import zlib

from prepare_paper_quote_supply import CANDIDATE, EVIDENCE, load_candidate

INSIDE = r'''
import asyncio,base64,hashlib,json,os,signal,sys,types
signal.signal(signal.SIGALRM,lambda *_:os._exit(124));signal.alarm(35)
request=json.load(sys.stdin)
for name in ['app.services.quote_contract','app.services.paper_quote_supply']:
 item=request['modules'][name];raw=item['source'].encode()
 assert hashlib.sha256(raw).hexdigest()==item['sha256'],'probe_source_changed'
 module=types.ModuleType(name);module.__file__='<in-memory-probe>/'+name
 sys.modules[name]=module;exec(compile(raw,module.__file__,'exec'),module.__dict__)
from app.services.paper_quote_supply import cn_now,fetch_sina,parse_sina_quote,public_trade_date,read_permit
from app.services.trading_calendar_service import TradingCalendarService
async def forbidden(*a,**kw):raise AssertionError('Tushare forbidden in probe')
TradingCalendarService._fetch_from_tushare=forbidden
assert read_permit(cn_now()) is None,'paper_permit_present'
async def main():
 out={'quotes':[],'database_writes':0,'permit_installed':False}
 for symbol in ['600000','000001','159865','601059']:
  try:
   raw=await asyncio.wait_for(fetch_sina(symbol),5);now=cn_now()
   row=dict(symbol=symbol,fetched_at=now.isoformat(),bytes=len(raw),
    sha256=hashlib.sha256(raw).hexdigest(),raw=base64.b64encode(raw).decode())
   try:
    doc=parse_sina_quote(raw,symbol,now)
    row.update(usable=True,quote_time=doc['quote_time'],trading_status=doc['trading_status'],reason='ok')
   except ValueError as error:row.update(usable=False,reason=str(error))
  except Exception as error:row=dict(symbol=symbol,usable=False,error_type=type(error).__name__)
  out['quotes'].append(row)
 try:out['public_calendar']=await asyncio.wait_for(public_trade_date(),5)
 except Exception as error:out['public_calendar']=None;out['calendar_error']=type(error).__name__
 out['finished_at']=cn_now().isoformat();print('PAPER_PUBLIC_PROBE:'+json.dumps(out))
asyncio.run(main())
'''

HOST = r'''
import json,subprocess,sys
q=json.load(sys.stdin)
result=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',q['code']],
 input=json.dumps(q['request']),text=True,capture_output=True,timeout=45)
rows=[s.split(':',1)[1] for s in result.stdout.splitlines() if s.startswith('PAPER_PUBLIC_PROBE:')]
assert result.returncode==0 and len(rows)==1,'public_probe_failed_details_withheld'
print(rows[0])
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args();os.umask(0o077)
    _,files,_=load_candidate(CANDIDATE)
    request={'modules':{target.removeprefix('/app/').removesuffix('.py').replace('/','.'):item
                        for target,item in files.items() if target.endswith(('quote_contract.py','paper_quote_supply.py'))}}
    packet=dict(code=HOST,request=dict(code=INSIDE,request=request))
    bootstrap="import io,json,sys,zlib;q=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(q['request']));exec(q['code'])"
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
         '-S',str(args.control_socket) if args.control_socket else 'none','-o','BatchMode=yes',
         '-o','ConnectTimeout=10','-o','ConnectionAttempts=1','-o','IPQoS=none',
         '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','osaka-migration',
         'sudo -n python3 -B -c '+shlex.quote(bootstrap)]
    path=EVIDENCE/('production-public-'+str(time.time_ns())+'.json')
    try:
        result=subprocess.run(cmd,input=zlib.compress(json.dumps(packet).encode()),capture_output=True,timeout=65)
    except subprocess.TimeoutExpired:
        raise SystemExit('public_probe_timeout_no_success_evidence') from None
    if result.returncode:
        path.with_suffix('.stderr.private').write_bytes(result.stderr)
        raise SystemExit('public_probe_failed_details_saved_privately')
    value=json.loads(result.stdout);path.write_text(json.dumps(value,indent=2))
    for row in value['quotes']:row.pop('raw',None)
    print(json.dumps(dict(evidence=str(path),result=value)))


if __name__=='__main__':
    main()
