"""One-use HEAD-only check through the existing production Unix socket.

Does not install modules, recreate containers, read credentials, call data APIs
or remove STOP. Ambiguous start results must be resolved with status.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

REPO = Path(__file__).resolve().parents[1]
ROOT = '/home/ubuntu/k-tradingagents-history-http-reuse-probe-20261101-v1'
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/production-20261028')

INVENTORY = r'''
import pathlib,json,hashlib
r=pathlib.Path('/app/runtime/tushare-history-generations')
a=json.loads((r/'active.json').read_text());d=r/a['generation'];s=json.loads((d/'state.json').read_text())
print(json.dumps(dict(generation=str(d),stopped=(d/'STOP').exists(),phase=s['phase'],reason=s['reason'],used=s['used'],plan=s['plan']['plan_sha256'],
 protected={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [r/'active.json',d/'STOP',d/'state.json'] if p.exists()},
 modules={p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in ['/app/tradingagents/utils/tushare_egress.py','/app/app/services/tushare_history_batches.py']})))
'''

DRIVER = r'''
import json,pathlib,subprocess,hashlib,time,threading,os
root=pathlib.Path(__file__).resolve().parent
manifest=json.loads((root/'manifest.json').read_text())
def inventory():
 p=subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',manifest['inventory']],capture_output=True,text=True,timeout=12)
 assert p.returncode==0,'inventory_failed'
 return json.loads(p.stdout)
os.umask(0o077)
def save(value):
 with open(root/'result.pending','w') as f:json.dump(value,f);f.flush();os.fsync(f.fileno())
 os.replace(root/'result.pending',root/'result.json')
result=dict(ok=False,stage='starting',at=time.time(),observations=[])
save(result)
kill=threading.Timer(115,lambda:os._exit(124));kill.daemon=True;kill.start()
conn=None
try:
 before=inventory();assert before==manifest['before'],'state_changed_before_probe'
 source=(root/'candidate_tushare_egress.py').read_bytes()
 assert hashlib.sha256(source).hexdigest()==manifest['source_sha256'],'source_changed'
 ns={'__name__':'isolated_head_probe'};exec(compile(source,'<candidate-egress>','exec'),ns)
 assert ns['HOST']=='api.waditu.com'
 ns['SOCKET_PATH']='/run/ta-tushare-egress/upstream.sock'
 conn=ns['UnixTLSConnection'](30)
 result['stage']='head_only';save(result)
 for index in range(3):
  start=time.monotonic()
  if index==0:conn.connect()
  else:conn.begin_request()
  conn.request('HEAD','/',headers={'Connection':'keep-alive','Accept':'*/*'})
  response=conn.getresponse();response.read(1024)
  obs=dict(index=index,status=response.status,version=response.version,will_close=response.will_close,
           socket_open=conn.sock is not None,seconds=round(time.monotonic()-start,3))
  result['observations'].append(obs);save(result)
  assert response.status==200 and response.version==11 and not response.will_close and response.isclosed() and conn.sock is not None,'head_not_reusable'
  conn.finish_request()
 result.update(ok=True,stage='passed')
except BaseException as error:
 result.update(ok=False,stage='failed',error_type=type(error).__name__)
finally:
 if conn is not None:conn.abort()
 try:
  after=inventory();result['after']=after;result['protected_unchanged']=after==manifest['before']
  if not result['protected_unchanged']:result.update(ok=False,stage='state_changed')
 except Exception as error:result.update(ok=False,stage='verification_failed',verify_error_type=type(error).__name__)
 result['finished_at']=time.time();save(result);kill.cancel()
'''

START = r'''
import json,sys,pathlib,subprocess,hashlib,os,time
r=json.load(sys.stdin);root=pathlib.Path(r['root']);os.umask(0o077)
assert not root.exists(),'already_reserved'
p=subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',r['inventory']],capture_output=True,text=True,timeout=12)
assert p.returncode==0,'inventory_failed'
b=json.loads(p.stdout)
assert b['stopped'] and b['phase']=='blocked' and b['reason']=='timeout' and b['used']==2298,'business_state_changed'
assert b['generation']=='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa','generation_changed'
assert b['plan']=='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4','plan_changed'
assert hashlib.sha256(r['source'].encode()).hexdigest()==r['source_sha256'],'source_invalid'
root.mkdir(mode=0o700)
for name,value in [('candidate_tushare_egress.py',r['source']),('driver.py',r['driver'])]:
 with open(root/name,'x') as f:f.write(value);f.flush();os.fsync(f.fileno())
manifest=dict(before=b,source_sha256=r['source_sha256'],operator_commit=r['operator_commit'],inventory=r['inventory'],created_at=time.time())
with open(root/'manifest.json','x') as f:json.dump(manifest,f);f.flush();os.fsync(f.fileno())
with open(root/'stdout.private','x') as out,open(root/'stderr.private','x') as err:
 p=subprocess.Popen(['python3','-B',str(root/'driver.py')],stdin=subprocess.DEVNULL,stdout=out,stderr=err,start_new_session=True)
with open(root/'pid.json','x') as f:json.dump(dict(pid=p.pid),f)
print(json.dumps(dict(started=True,pid=p.pid,root=str(root))))
'''

STATUS = r'''
import pathlib,json
p=pathlib.Path('/home/ubuntu/k-tradingagents-history-http-reuse-probe-20261101-v1');r=dict(reserved=p.exists())
for name in ['manifest.json','pid.json','result.json']:
 if (p/name).exists():r[name]=json.loads((p/name).read_text())
print(json.dumps(r))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['start', 'status'], required=True)
    args = parser.parse_args()
    request = None
    if args.action == 'start':
        assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO), 'commit_before_start'
        source = (REPO / 'tradingagents/utils/tushare_egress.py').read_text()
        request = dict(root=ROOT, source=source, source_sha256=hashlib.sha256(source.encode()).hexdigest(),
            driver=DRIVER, inventory=INVENTORY,
            operator_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip())
    cmd = ['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
        '-S', 'none', '-C', '-o', 'BatchMode=yes', '-o', 'IPQoS=none',
        '-o', 'ConnectTimeout=25', '-o', 'ConnectionAttempts=1', 'osaka-migration',
        'sudo -n python3 -B -c ' + shlex.quote(START if request is not None else STATUS)]
    os.umask(0o077)
    prefix = EVIDENCE / ('history-head-probe-' + args.action + '-' + str(time.time_ns()))
    try:
        p = subprocess.run(cmd, input=json.dumps(request) if request else None,
            capture_output=True, text=True, timeout=65)
    except subprocess.TimeoutExpired:
        raise SystemExit('management_timeout: use status, never repeat start') from None
    prefix.with_suffix('.private').write_text(p.stderr)
    if p.returncode:
        raise SystemExit('management_failed: use status, never repeat start')
    result = json.loads(p.stdout)
    prefix.with_suffix('.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
