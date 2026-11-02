"""Read-only production baseline for on-demand quotes; no provider requests."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import zlib

REPO = Path(__file__).resolve().parents[1]
FILES = ("app/routers/paper.py", "app/services/quote_contract.py",
         "app/services/paper_quote_supply.py", "app/services/trading_calendar_service.py")

INSIDE = r'''
import hashlib,json,sys,types,importlib.util
from pathlib import Path
request=json.load(sys.stdin)
runtime=types.ModuleType('runtime');exec(request['runtime'],runtime.__dict__)
env,effective=runtime.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
def digest(path):
 p=Path(path);return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None
root=Path('/app/runtime/tushare-history-generations')
active=json.loads((root/'active.json').read_text());generation=root/active['generation']
state=json.loads((generation/'state.json').read_text())
out=dict(modules={p:digest(p) for p in request['files']},gates=effective['gates'],
 permit_exists=Path('/app/runtime/paper-quote-supply.json').exists(),
 history=dict(generation=str(generation),phase=state['phase'],active=state['active'],used=state['used'],
 stopped=(generation/'STOP').exists(),schedule_sha256=digest('/app/runtime/tushare-history-schedule.json')),
 dependencies={name:importlib.util.find_spec(name) is not None for name in ('httpx','akshare')})
try:
 import redis
 client=redis.Redis(host=env.get('REDIS_HOST','redis'),port=int(env.get('REDIS_PORT',6379)),
  password=env.get('REDIS_PASSWORD') or None,db=int(env.get('REDIS_DB',0)),
  socket_connect_timeout=3,socket_timeout=3)
 raw=client.get('trading_calendar:last_trade_date');cache=json.loads(raw) if raw else {}
 out['calendar']={k:cache.get(k) for k in ('fetched_at','last_trade_date','is_today_trade_day','source')}
 client.close()
except Exception:out['calendar']=None
print('PAPER_SUPPLY_BASELINE:'+json.dumps(out))
'''

HOST = r'''
import subprocess,json,sys
request=json.load(sys.stdin)
info=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],timeout=8))[0]
result=subprocess.run(['docker','exec','-i',info['Id'],'python3','-B','-c',request['code']],
 input=json.dumps(request['request']),text=True,capture_output=True,timeout=20)
lines=[line.split(':',1)[1] for line in result.stdout.splitlines() if line.startswith('PAPER_SUPPLY_BASELINE:')]
assert result.returncode==0 and len(lines)==1,'baseline_failed_details_withheld'
out=dict(backend=dict(id=info['Id'],image=info['Image'],user=info['Config']['User'],
 health=info['State'].get('Health',{}).get('Status'),mounts=info['Mounts'],
 compose_labels={k:v for k,v in info['Config']['Labels'].items() if k.startswith('com.docker.compose.')}),
 inside=json.loads(lines[0]))
print(json.dumps(out))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("output_already_exists")
    os.umask(0o077)
    request = dict(code=INSIDE, request=dict(
        runtime=(REPO/"deploy/quote_guard_runtime.py").read_text(), files=["/app/"+p for p in FILES]))
    bootstrap = "import sys,json,zlib,io;q=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(q['request']));exec(q['code'])"
    cmd = ["ssh", "-F", "/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config", "-S", "none",
           "-o", "BatchMode=yes", "-o", "IPQoS=none", "-o", "ConnectTimeout=10",
           "-o", "ConnectionAttempts=1", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2",
           "osaka-migration", "sudo -n python3 -B -c " + shlex.quote(bootstrap)]
    try:
        result = subprocess.run(cmd, input=zlib.compress(json.dumps(dict(code=HOST, request=request)).encode()),
                                capture_output=True, timeout=45)
    except subprocess.TimeoutExpired as error:
        with args.output.with_suffix(".failure.private").open("x") as stream:
            json.dump(dict(reason="timeout", stdout=(error.stdout or b"").decode(errors="replace"),
                           stderr=(error.stderr or b"").decode(errors="replace")), stream)
        raise SystemExit("read_only_inspection_timeout") from None
    if result.returncode:
        with args.output.with_suffix(".failure.private").open("x") as stream:
            json.dump(dict(returncode=result.returncode, stdout=result.stdout.decode(errors="replace"),
                           stderr=result.stderr.decode(errors="replace")), stream)
        raise SystemExit("read_only_inspection_failed")
    doc = json.loads(result.stdout)
    doc["collected_at"] = time.time()
    with args.output.open("x") as stream:
        json.dump(doc, stream, indent=2)
    print(json.dumps(dict(health=doc["backend"]["health"], mounts=len(doc["backend"]["mounts"]),
                          inside=doc["inside"])))


if __name__ == "__main__":
    main()
