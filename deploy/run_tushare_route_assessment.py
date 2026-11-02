"""One bounded, detached TLS-only route comparison; never changes live routing.

No credentials, HTTP requests, application imports, policy changes or DB access.
After an uncertain start, use status. The server directory is never reused.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

ROOT = '/home/ubuntu/k-tradingagents-route-assessment-20261102-v1'
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/ssh-diagnosis-20261102')
CONFIG = '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config'

PROBE = r'''
import concurrent.futures,json,os,pathlib,socket,ssl,threading,time
root=pathlib.Path(__file__).resolve().parent
os.umask(0o077)
def save(name,value):
 p=root/name;t=p.with_suffix('.pending')
 with t.open('w') as f:json.dump(value,f);f.flush();os.fsync(f.fileno())
 os.replace(t,p)
def probe(route):
 observations=[]
 for index in range(3):
  started=time.monotonic();stage='connect';raw=None;secure=None
  row=dict(index=index+1,at=time.time())
  def expire():
   # Shutdown the original descriptor, which TLS wrap detaches, via a duplicate.
   if watchdog_socket[0] is not None:
    try:watchdog_socket[0].shutdown(socket.SHUT_RDWR)
    except OSError:pass
  watchdog_socket=[None];timer=threading.Timer(30,expire);timer.daemon=True
  try:
   ctx=ssl.create_default_context();ctx.minimum_version=ssl.TLSVersion.TLSv1_2
   if route=='direct':raw=socket.socket(socket.AF_INET,socket.SOCK_STREAM);endpoint=('api.waditu.com',443)
   else:raw=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);endpoint='/run/ta-tushare-egress/upstream.sock'
   watchdog_socket[0]=raw.dup();timer.start();raw.settimeout(15);raw.connect(endpoint)
   row['connect_s']=round(time.monotonic()-started,3);stage='tls_handshake'
   raw.settimeout(max(.01,30-(time.monotonic()-started)))
   secure=ctx.wrap_socket(raw,server_hostname='api.waditu.com')
   row.update(verified=True,tls=secure.version())
  except Exception as error:row.update(verified=False,stage=stage,error_type=type(error).__name__)
  finally:
   timer.cancel()
   if secure is not None:secure.close()
   if raw is not None:raw.close()
   if watchdog_socket[0] is not None:watchdog_socket[0].close()
  row['elapsed_s']=round(time.monotonic()-started,3);observations.append(row)
  save(route+'.json',dict(route=route,observations=observations))
 return dict(route=route,observations=observations)
save('status.json',dict(stage='running',started_at=time.time(),pid=os.getpid()))
# Independent watchdog bounds DNS/SSL and interpreter shutdown too.
deadline=threading.Timer(105,lambda:os._exit(124));deadline.daemon=True;deadline.start()
try:
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
  rows=list(pool.map(probe,['direct','existing_unix']))
 save('result.json',dict(stage='complete',finished_at=time.time(),routes=rows,http_requests=0))
 save('status.json',dict(stage='complete',finished_at=time.time(),pid=os.getpid()))
except BaseException as error:
 save('status.json',dict(stage='failed',error_type=type(error).__name__,finished_at=time.time(),pid=os.getpid()))
finally:deadline.cancel()
'''

START = r'''
import hashlib,json,os,pathlib,subprocess,sys
os.umask(0o077);r=json.load(sys.stdin);root=pathlib.Path(r['root'])
assert hashlib.sha256(r['probe'].encode()).hexdigest()==r['sha256'],'source_changed'
root.mkdir(mode=0o700)  # Atomic one-shot reservation, never remove/reuse it.
for name,value in [('probe.py',r['probe']),('manifest.json',json.dumps(dict(sha256=r['sha256'],http_requests=0)))]:
 with (root/name).open('x') as f:f.write(value);f.flush();os.fsync(f.fileno())
with (root/'stderr.private').open('xb') as log:
 child=subprocess.Popen(['python3','-B',str(root/'probe.py')],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
with (root/'process.json').open('x') as f:json.dump(dict(pid=child.pid),f);f.flush();os.fsync(f.fileno())
print(json.dumps(dict(reserved=True,pid=child.pid,sha256=r['sha256'])))
'''

STATUS = r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-route-assessment-20261102-v1')
r=dict(reserved=root.exists())
for name in ['manifest.json','process.json','status.json','direct.json','existing_unix.json','result.json']:
 if (root/name).is_file():r[name]=json.loads((root/name).read_text())
print(json.dumps(r))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['start', 'status'], required=True)
    parser.add_argument('--control-socket', type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    EVIDENCE.mkdir(mode=0o700, exist_ok=True)
    command = ['ssh', '-F', CONFIG, '-S', str(args.control_socket) if args.control_socket else 'none',
               '-o', 'ConnectTimeout=20', '-o', 'IPQoS=none',
               'osaka-migration', 'sudo -n python3 -B -c ' + shlex.quote(START if args.action == 'start' else STATUS)]
    request = dict(root=ROOT, probe=PROBE, sha256=hashlib.sha256(PROBE.encode()).hexdigest())
    prefix = EVIDENCE / ('routes-' + args.action + '-' + str(time.time_ns()))
    try:
        p = subprocess.run(command, input=json.dumps(request).encode() if args.action == 'start' else None,
                           capture_output=True, timeout=40)
        receipt = dict(returncode=p.returncode, stdout=p.stdout.decode(errors='replace'),
                       stderr=p.stderr.decode(errors='replace'))
    except subprocess.TimeoutExpired as error:
        receipt = dict(transport_timeout=True, stdout=(error.stdout or b'').decode(errors='replace'),
                       stderr=(error.stderr or b'').decode(errors='replace'))
    with prefix.with_suffix('.private').open('x') as f:
        json.dump(receipt, f)
    if receipt.get('returncode') != 0:
        raise SystemExit('transport_failed: use status; never repeat uncertain start')
    result = json.loads(receipt['stdout'])
    with prefix.with_suffix('.json').open('x') as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
