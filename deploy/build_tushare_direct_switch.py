"""Freeze a console switch against the returned production backup manifest.

Builds files locally only; executing the resulting switch needs operator approval.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

import build_tushare_direct_prepare as prepared

WORKER_UNIT = 'ta-tushare-direct-switch-20261103-v1.service'
GUARD_UNIT = 'ta-tushare-direct-rollback-20261103-v1'

LAUNCH = r'''
import hashlib,json,os,pathlib,stat,subprocess,time
os.umask(0o077)
def require(ok,reason):
 if not ok:raise RuntimeError(reason)
def digest(raw):return hashlib.sha256(raw).hexdigest()
def save(path,raw):
 fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
r=package['request'];root=pathlib.Path(r['root']);work=root/'switch-v1'
require(os.geteuid()==0,'root_required')
require(root.resolve()==root and root.stat().st_uid==0 and stat.S_IMODE(root.stat().st_mode)==0o700,'unsafe_preparation_directory')
backend=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],timeout=12))[0]
require(backend['Id']==package['backend_id'] and backend['Image']==package['image'],'wrong_host_or_backend_changed')
require(digest((root/'manifest.json').read_bytes())==r['manifest_sha256'],'manifest_changed')
def report():
 result=dict(reserved=work.exists(),manifest_sha256=r['manifest_sha256'],http_requests=0)
 for name in ['status.json','committed.json','switch-error.json','worker-error.json']:
  if (work/name).is_file():result[name]=json.loads((work/name).read_text())
 print(json.dumps(result),flush=True)
if work.exists():
 require(work.resolve()==work and work.stat().st_uid==0 and stat.S_IMODE(work.stat().st_mode)==0o700,'unsafe_switch_directory')
 report()
else:
 require(backend['State']['Running'] and backend['State'].get('Health',{}).get('Status')=='healthy' and len(backend['Mounts'])==44,'backend_not_healthy')
 require(time.time()+300<r['expiry'],'expired_or_too_close')
 require(digest(package['worker'].encode())==r['worker_sha256'],'worker_changed')
 work.mkdir(mode=0o700)
 save(work/'worker.py',package['worker'].encode())
 save(work/'request.json',json.dumps(r,sort_keys=True,separators=(',',':')).encode())
 save(work/'operator.lock',b'')
 save(work/'status.json',json.dumps(dict(stage='reserved',at=time.time())).encode())
 p=subprocess.run(['systemd-run','--quiet','--collect','--service-type=exec','--unit='+r['worker_unit'],'--property=RuntimeMaxSec=150','--property=TimeoutStopSec=5','--property=UMask=0077','/usr/bin/python3','-B',str(work/'worker.py'),str(work/'request.json')],capture_output=True,text=True,timeout=20)
 save(work/'launch.json',json.dumps(dict(returncode=p.returncode,stderr=p.stderr,at=time.time())).encode())
 require(p.returncode==0,'worker_launch_failed_inspect_only')
 print('SWITCH_RESERVED; detached worker; HTTP requests=0',flush=True)
 deadline=time.monotonic()+115
 while time.monotonic()<deadline:
  stage=json.loads((work/'status.json').read_text())['stage']
  if stage in ('passed','rolled_back','rolled_back_expired_transport_closed','rollback_incomplete_transport_closed') or (work/'worker-error.json').exists():break
  time.sleep(1)
 report()
'''

STATUS = r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-direct-route-20261103-v2/switch-v1')
r=dict(reserved=root.exists())
for name in ['status.json','committed.json','switch-error.json','worker-error.json','launch.json']:
 if (root/name).is_file():r[name]=json.loads((root/name).read_text())
print(json.dumps(r))
'''


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def freeze(receipt):
    record = json.loads(receipt.read_text())
    record = record.get('receipt', record)
    manifest = record['manifest.json']
    raw = json.dumps(manifest, sort_keys=True, indent=2).encode()
    if (not record['prepared'] or record['status.json']['stage'] != 'prepared_no_route_change'
            or record['root'] != prepared.ROOT or hashlib.sha256(raw).hexdigest() != record['manifest_sha256']):
        raise ValueError('successful_exact_preparation_required')
    if (manifest['backend_id'] != prepared.BACKEND_ID or manifest['image'] != prepared.IMAGE
            or manifest['expiry'] != prepared.EXPIRY or manifest['mounts'] != 44
            or manifest['http_requests'] != 0 or len(manifest['tls_probes']) != 3
            or not all(row['verified'] for row in manifest['tls_probes'])):
        raise ValueError('preparation_contract_changed')
    if manifest['source_sha256'] != hashlib.sha256(prepared.PREPARE.encode()).hexdigest():
        raise ValueError('preparation_source_changed')
    candidates = {
        str(Path('/etc/systemd/system') / (prepared.EXPIRY_UNIT + '.service')): prepared.EXPIRY_SERVICE,
        str(Path('/etc/systemd/system') / (prepared.EXPIRY_UNIT + '.timer')): prepared.EXPIRY_TIMER,
        prepared.DROPIN_PATH: prepared.DROPIN,
    }
    if set(candidates) != set(manifest['candidates']) or not all(
            hashlib.sha256(source.encode()).hexdigest() == manifest['candidates'][path]['sha256']
            for path, source in candidates.items()):
        raise ValueError('candidate_contract_changed')
    worker = Path(__file__).with_name('tushare_direct_switch.py').read_text()
    compile(worker, 'worker', 'exec')
    r = dict(root=prepared.ROOT, manifest_sha256=record['manifest_sha256'], expiry=prepared.EXPIRY,
             unit=prepared.UNIT, socket=prepared.SOCKET, dropin=prepared.DROPIN_PATH,
             socket_path='/run/ta-tushare-egress/upstream.sock', unit_directory='/etc/systemd/system',
             expiry_unit=prepared.EXPIRY_UNIT, worker_unit=WORKER_UNIT, guard_unit=GUARD_UNIT,
             approved_path='/etc/ta-tushare-egress/APPROVED', candidate_sources=candidates,
             transport_sha256=hashlib.sha256(Path(__file__).resolve().parents[1].joinpath('tradingagents/utils/tushare_egress.py').read_bytes()).hexdigest(),
             worker_sha256=hashlib.sha256(worker.encode()).hexdigest(),
             old_command='/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 127.0.0.1:32879',
             new_command='/usr/lib/systemd/systemd-socket-proxyd --connections-max=4 api.waditu.com:443')
    return dict(request=r, worker=worker, backend_id=prepared.BACKEND_ID, image=prepared.IMAGE)


def script(package):
    if "'''" in package['worker']:
        raise ValueError('triple_quote_in_worker')
    metadata = {k: v for k, v in package.items() if k != 'worker'}
    code = ('package=' + repr(metadata) + "\npackage['worker']=r'''" + package['worker'] + "'''\n" + LAUNCH)
    compile(code, 'direct_switch_console', 'exec')
    return "#!/bin/sh\n# APPROVED SWITCH ONLY; status on rerun.\nsudo -n /usr/bin/python3 -B - <<'TA_DIRECT_SWITCH_PY'\n" + code + '\nTA_DIRECT_SWITCH_PY\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    package = freeze(args.receipt)
    args.output.mkdir(mode=0o700)
    files = {'controller.json': canonical(package), '生产直连切换.sh': script(package).encode(),
             '生产直连切换状态.sh': ("#!/bin/sh\nsudo -n /usr/bin/python3 -B - <<'TA_DIRECT_STATUS_PY'\n" + STATUS + '\nTA_DIRECT_STATUS_PY\n').encode()}
    sums = []
    for name, raw in files.items():
        with (args.output / name).open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        sums.append(hashlib.sha256(raw).hexdigest() + '  ' + name)
    (args.output / 'SHA256SUMS').write_text('\n'.join(sums) + '\n')
    print(json.dumps(dict(frozen=True, directory=str(args.output), sha256=hashlib.sha256(files['controller.json']).hexdigest(),
                          manifest_sha256=package['request']['manifest_sha256'], expires_at=prepared.EXPIRY)))


if __name__ == '__main__':
    main()
