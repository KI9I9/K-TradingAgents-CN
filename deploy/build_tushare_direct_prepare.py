"""Build a production-console BACKUP/PREFLIGHT only script. Never enables routing."""
import argparse
import hashlib
import os
from pathlib import Path

from build_route_assessment_console import BACKEND_ID, IMAGE

ROOT = '/home/ubuntu/k-tradingagents-direct-route-20261103-v2'
EXPIRY = 1794117600  # 2026-11-08 14:00 Asia/Shanghai; no extension.
UNIT = 'ta-tushare-egress.service'
SOCKET = 'ta-tushare-egress.socket'
EXPIRY_UNIT = 'ta-tushare-direct-expire-20261103'
DROPIN_PATH = '/etc/systemd/system/ta-tushare-egress.service.d/90-direct-route-20261103.conf'
PROBE_UNIT = 'ta-tushare-direct-preflight-20261103-v2.service'
# Match the original service sandbox, using a distinct transient dynamic user.
# The proxy's DynamicUser identity is not a static account for runuser.
SANDBOX = dict(DynamicUser='yes', User='ta-tushare-proxy', UMask='0077',
               LimitCORE='0', LimitNOFILE='64', MemoryMax='32M', TasksMax='8', CPUQuota='20%',
               NoNewPrivileges='yes', CapabilityBoundingSet='', AmbientCapabilities='',
               PrivateTmp='yes', PrivateDevices='yes', ProtectSystem='strict', ProtectHome='yes',
               ProtectKernelTunables='yes', ProtectKernelModules='yes', ProtectControlGroups='yes',
               RestrictSUIDSGID='yes', RestrictNamespaces='yes', LockPersonality='yes',
               RestrictAddressFamilies='AF_UNIX AF_INET')

DROPIN = f'''[Service]
ExecCondition=/usr/bin/python3 -c "import sys,time; sys.exit(0 if time.time() < {EXPIRY} else 1)"
ExecStart=
ExecStart=/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 api.waditu.com:443
'''
EXPIRY_SERVICE = f'''[Unit]
Description=Close only the approved finite direct Tushare route

[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl stop {SOCKET} {UNIT}
'''
EXPIRY_TIMER = f'''[Unit]
Description=Expiry of direct Tushare route at original approved deadline

[Timer]
OnCalendar=2026-11-08 06:00:00 UTC
Persistent=true
AccuracySec=1s
RandomizedDelaySec=0
Unit={EXPIRY_UNIT}.service

[Install]
WantedBy=timers.target
'''

TLS_PROBE = r'''
import json,socket,ssl,time
ctx=ssl.create_default_context();ctx.minimum_version=ssl.TLSVersion.TLSv1_2
rows=[]
for index in range(3):
 start=time.monotonic()
 with socket.create_connection(('api.waditu.com',443),timeout=10) as raw:
  raw.settimeout(max(.01,30-(time.monotonic()-start)))
  with ctx.wrap_socket(raw,server_hostname='api.waditu.com') as secure:
   rows.append(dict(index=index+1,verified=True,tls=secure.version(),elapsed_s=round(time.monotonic()-start,3)))
print(json.dumps(rows))
'''

PREPARE = r'''
import configparser,fcntl,hashlib,json,os,pathlib,stat,subprocess,time
os.umask(0o077)
def require(ok,reason):
 if not ok:raise RuntimeError(reason)
def digest(raw):return hashlib.sha256(raw).hexdigest()
def run(args,timeout=12):
 p=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
 if p.returncode and args[0] in ('systemd-run','systemd-analyze'):
  save(root/('command-failure-'+str(time.time_ns())+'.json'),json.dumps(dict(program=args[0],returncode=p.returncode,stderr=p.stderr)).encode())
 require(p.returncode==0,'command_failed:'+args[0]);return p.stdout.strip()
def properties(unit):
 fields=['FragmentPath','DropInPaths','ExecStart','ExecCondition','User','Group','DynamicUser','ActiveState','SubState','IPAddressDeny','IPAddressAllow','PrivateNetwork','RestrictAddressFamilies','Listen']
 return dict(l.split('=',1) for l in run(['systemctl','show',unit]+sum([['-p',k] for k in fields],[])).splitlines() if '=' in l)
def entry(path):
 s=path.lstat();require(stat.S_ISREG(s.st_mode),'not_regular:'+str(path))
 return dict(sha256=digest(path.read_bytes()),mode=stat.S_IMODE(s.st_mode),uid=s.st_uid,gid=s.st_gid)
def save(path,raw):
 fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
def atomic_json(path,value):
 tmp=path.with_suffix('.pending')
 save(tmp,json.dumps(value,sort_keys=True).encode());os.replace(tmp,path)
def report():
 r=dict(prepared=False,root=str(root))
 for name in ['status.json','manifest.json']:
  if (root/name).is_file():r[name]=json.loads((root/name).read_text())
 if (root/'manifest.json').exists():r['manifest_sha256']=digest((root/'manifest.json').read_bytes());r['prepared']=r.get('status.json',{}).get('stage')=='prepared_no_route_change'
 print(json.dumps(r),flush=True)
root=pathlib.Path(request['root'])
require(os.geteuid()==0,'sudo_required')
b=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
require(b['Id']==request['backend_id'] and b['Image']==request['image'],'wrong_host_or_backend_changed')
require(b['State']['Running'] and b['State'].get('Health',{}).get('Status')=='healthy' and len(b['Mounts'])==44,'backend_not_healthy_or_mounts_changed')
if root.exists():
 require(root.is_dir() and root.resolve()==root and stat.S_IMODE(root.stat().st_mode)==0o700 and root.stat().st_uid==0,'unsafe_existing_directory')
 report()
else:
 locks=[];reserved=False
 try:
  runtime=pathlib.Path(next(m['Source'] for m in b['Mounts'] if m['Destination']=='/app/runtime'))
  require(runtime.is_absolute() and runtime.resolve()==runtime,'runtime_path_invalid')
  admission=runtime/'tushare-history-batch-v1/admission.lock'
  f=os.open(admission,os.O_RDONLY|os.O_NOFOLLOW);locks.append(f);fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
  reg=runtime/'tushare-history-generations';active=reg/'active.json';selection=json.loads(active.read_text())
  require(selection['generation']=='20aa2215a7904e0d8d6601b14b8a6c90','generation_changed')
  generation=reg/selection['generation']
  f=os.open(generation/'gate.lock',os.O_RDONLY|os.O_NOFOLLOW);locks.append(f);fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
  policy=json.loads((generation/'policy.json').read_text());state=json.loads((generation/'state.json').read_text())
  require(state['phase']=='blocked' and state['reason']=='network_failed' and state['used']==6425,'history_state_changed')
  require(state['plan']['plan_sha256']=='d97ddaa79fea5efd6d1fd481c7623d3124a38e5c50fda3f27a0c47a4815e4588','plan_changed')
  require(state['active']==dict(code='002309.SZ',start='2026-10-29',end='2026-11-02',id='289f124728af45e8a27475e5bdd908e9',sent=['daily']),'active_attempt_changed')
  require(policy['expires_at']==request['expiry']>time.time(),'expiry_changed_or_expired')
  schedule=runtime/'tushare-history-schedule.json';permit=json.loads(schedule.read_text())
  policy_hash=digest(json.dumps(policy,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
  require(policy['schema']==2 and policy['write_enabled'] is True and len(policy['codes'])==5577 and policy['minute_requests']==40 and policy['daily_requests']==7500 and policy['max_requests']==40000 and policy['min_date']=='2026-09-30' and policy['max_date']=='2026-11-06','policy_changed')
  require(permit['generation']=='/app/runtime/tushare-history-generations/'+selection['generation'] and permit['policy_sha256']==selection['policy_sha256']==state['policy_sha256']==policy_hash,'policy_binding_changed')
  require(permit['expires_at']==policy['expires_at'] and permit['cron']=='30 18 * * mon-fri' and permit['timezone']=='Asia/Shanghai','schedule_changed')
  retirement=generation/'retirement.json'
  require(digest(retirement.read_bytes())==selection['retirement_sha256'],'retirement_binding_changed')
  protected=[active,generation/'state.json',generation/'policy.json',generation/'STOP',schedule,runtime/'tushare-egress/STOP',retirement]
  protected_info={str(p):entry(p) for p in protected}
  service=properties(request['unit']);sock=properties(request['socket'])
  unit_directory=pathlib.Path(request['unit_directory'])
  require(service['FragmentPath']==str(unit_directory/request['unit']) and sock['FragmentPath']==str(unit_directory/request['socket']),'unit_path_changed')
  require(not service.get('DropInPaths') and not sock.get('DropInPaths') and not service.get('ExecCondition'),'existing_overrides')
  require(all(p['ActiveState']=='active' for p in [service,sock]),'proxy_not_active')
  require(service['User']==service['Group']=='ta-tushare-proxy' and service.get('DynamicUser')=='yes' and service['PrivateNetwork']=='no' and not service.get('IPAddressDeny') and not service.get('IPAddressAllow'),'proxy_security_context_changed')
  argv=service['ExecStart'].split('argv[]=',1)[1].split(' ;',1)[0]
  require(argv=='/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 127.0.0.1:32879','proxy_command_changed')
  require(sock['Listen']==request['socket_path']+' (Stream)','socket_changed')
  socket_path=pathlib.Path(request['socket_path']);si=socket_path.stat();require(stat.S_ISSOCK(si.st_mode),'not_socket')
  destinations={request['dropin']:request['dropin_source'],str(unit_directory/(request['expiry_unit']+'.service')):request['expiry_service'],str(unit_directory/(request['expiry_unit']+'.timer')):request['expiry_timer']}
  require(all(not pathlib.Path(p).exists() and not pathlib.Path(p).is_symlink() for p in destinations),'candidate_already_installed')
  originals={str(unit_directory/u):entry(unit_directory/u) for u in [request['unit'],request['socket']]}
  root.mkdir(mode=0o700);reserved=True
  (root/'backup').mkdir(mode=0o700);(root/'candidate').mkdir(mode=0o700)
  save(root/'operator-source.py',request['operator_source'].encode())
  atomic_json(root/'status.json',dict(stage='preparing_no_route_change',at=time.time()))
  for index,p in enumerate(list(originals)+list(protected_info)):
   raw=pathlib.Path(p).read_bytes();expected=(originals|protected_info)[p]['sha256'];require(digest(raw)==expected,'source_drift')
   save(root/'backup'/str(index),raw);require(digest((root/'backup'/str(index)).read_bytes())==expected,'backup_readback_failed')
  candidates={}
  for path,source in destinations.items():
   name=pathlib.Path(path).name;save(root/'candidate'/name,source.encode());candidates[path]=dict(file=name,sha256=digest(source.encode()))
  original_source=pathlib.Path(service['FragmentPath']).read_text()
  parser=configparser.ConfigParser(interpolation=None);parser.optionxform=str;parser.read_string(original_source)
  require(set(parser['Service'])==set(request['sandbox'])|{'Type','ExecStart','Restart','StandardOutput','StandardError'},'unexpected_service_options')
  require(all(parser['Service'][k]==v for k,v in request['sandbox'].items()),'sandbox_changed')
  merged=(original_source+'\n'+request['dropin_source']).encode()
  save(root/'candidate'/request['unit'],merged)
  save(root/'candidate'/request['socket'],pathlib.Path(sock['FragmentPath']).read_bytes())
  run(['systemd-analyze','verify',str(root/'candidate'/request['unit']),str(root/'candidate'/(request['expiry_unit']+'.service')),str(root/'candidate'/(request['expiry_unit']+'.timer'))],timeout=20)
  print('BACKUP_VERIFIED; checking direct TLS in matching transient DynamicUser sandbox; HTTP requests=0',flush=True)
  probe_properties=dict(request['sandbox'],User='ta-ts-direct-probe',RuntimeMaxSec='100',WorkingDirectory='/',Restart='no')
  probe_command=['systemd-run','--quiet','--wait','--pipe','--collect','--service-type=exec','--unit='+request['probe_unit']]
  for k,v in probe_properties.items():probe_command.append('--property='+k+'='+v)
  tls=json.loads(run(probe_command+['/usr/bin/python3','-B','-c',request['tls_probe']],timeout=110))
  require(len(tls)==3 and all(p['verified'] and p['tls'] in ['TLSv1.2','TLSv1.3'] for p in tls),'direct_tls_unverified')
  require(time.time()<request['expiry'],'expired_during_check')
  require(all(entry(pathlib.Path(p))==v for p,v in (originals|protected_info).items()),'protected_files_changed')
  require(properties(request['unit'])==service and properties(request['socket'])==sock,'unit_state_changed')
  after=socket_path.stat();require((si.st_dev,si.st_ino)==(after.st_dev,after.st_ino),'socket_inode_changed')
  manifest=dict(schema=1,backend_id=b['Id'],image=b['Image'],mounts=44,runtime_source=str(runtime),expiry=request['expiry'],originals=originals,protected=protected_info,service=service,socket=sock,socket_inode=[si.st_dev,si.st_ino],candidates=candidates,source_sha256=digest(request['operator_source'].encode()),backup_files={str(i):p for i,p in enumerate(list(originals)+list(protected_info))},tls_probes=tls,probe_unit=request['probe_unit'],probe_properties=probe_properties,prepared_at=time.time(),http_requests=0)
  save(root/'manifest.json',json.dumps(manifest,sort_keys=True,indent=2).encode())
  atomic_json(root/'status.json',dict(stage='prepared_no_route_change',at=time.time()))
  report()
 except BaseException as error:
  if reserved:atomic_json(root/'status.json',dict(stage='failed_no_route_change',reason=str(error) if isinstance(error,RuntimeError) else type(error).__name__,at=time.time()))
  print(json.dumps(dict(prepared=False,route_changed=False,error=str(error) if isinstance(error,RuntimeError) else type(error).__name__)),flush=True)
  raise SystemExit(1)
 finally:
  for f in reversed(locks):os.close(f)
'''


def request():
    return dict(root=ROOT, backend_id=BACKEND_ID, image=IMAGE, expiry=EXPIRY, unit=UNIT, socket=SOCKET,
                expiry_unit=EXPIRY_UNIT, dropin=DROPIN_PATH, dropin_source=DROPIN,
                expiry_service=EXPIRY_SERVICE, expiry_timer=EXPIRY_TIMER, tls_probe=TLS_PROBE,
                operator_source=PREPARE, unit_directory='/etc/systemd/system',
                socket_path='/run/ta-tushare-egress/upstream.sock', sandbox=SANDBOX, probe_unit=PROBE_UNIT)


def build():
    compile(PREPARE, 'direct_route_prepare', 'exec')
    compile(TLS_PROBE, 'direct_route_tls', 'exec')
    payload = request()
    payload.pop('operator_source')
    if "'''" in PREPARE:
        raise ValueError('triple_quote_in_source')
    code = ('request=' + repr(payload) + "\nPREPARE=r'''" + PREPARE + "'''\n"
            "request['operator_source']=PREPARE\nexec(compile(PREPARE,'direct_route_prepare','exec'))\n")
    compile(code, 'direct_route_console', 'exec')
    # The request is static code/config, with no token or password. Nothing is
    # activated in /etc; candidates and backups remain under the private root.
    return "#!/bin/sh\n# Production BACKUP/PREFLIGHT only. Does not apply the route.\nsudo -n python3 -B - <<'TA_DIRECT_PREPARE_PY'\n" + code + "\nTA_DIRECT_PREPARE_PY\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raw = build().encode()
    fd = os.open(args.output, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    print('sha256=' + hashlib.sha256(raw).hexdigest())
    print('path=' + str(args.output.resolve()))


if __name__ == '__main__':
    main()
