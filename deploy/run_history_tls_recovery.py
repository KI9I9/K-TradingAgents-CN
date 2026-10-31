"""One-use recovery of the 2173-request raw plan after repaired SSH/TLS.

Never rerun execute after an ambiguous result: inspect --action status instead.
No app deployment, new policy, budget reset, or automatic TLS retry is involved.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

import run_history_timeout_recovery as previous
import run_history_sample as transport
from query_quote_guard_jobs import read_auth

base=previous.base
REPO=Path(__file__).resolve().parents[1]
ROOT='/home/ubuntu/k-tradingagents-history-tls-recovery-20261030-v1'
BACKUP='/app/runtime/history-tls-recovery-20261030-v1'
LAUNCH=base.LAUNCH.replace(base.ROOT,ROOT)
STATUS=base.STATUS.replace(base.ROOT,ROOT)
DRIVER=base.DRIVER.replace(base.ROOT,ROOT)


def package():
    manifest=json.loads((base.RELEASE/'manifest.json').read_text())
    core=(REPO/'deploy/history_timeout_recovery.py').read_text()
    tls=(REPO/'deploy/history_tls_recovery.py').read_text()
    # Embed the reviewed operator sources; neither is installed into the app.
    code=("import json,sys\n"
          "core={'__name__':'operator_backup'}\nexec("+repr(core)+",core)\n"
          "tls={'__name__':'operator_tls'}\nexec("+repr(tls)+",tls)\n"
          "raise SystemExit(core['main'](json.load(sys.stdin),"
          "recoverer=tls['recover_raw_tls'],expected_reason='tls_failed',"
          "raw_count=36642,run_directory="+repr(BACKUP)+"))\n")
    return dict(source_head=manifest['source_head'],archive_sha256=base.ARCHIVE_SHA,
                operator_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
                driver=DRIVER,code=code,request=dict(
                    source_hashes={f['target']:f['sha256'] for f in manifest['files']},
                    runtime_tool=(REPO/'deploy/quote_guard_runtime.py').read_text(),
                    generation='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa',
                    used=2173,plan='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4'))


UPLOAD_AND_LAUNCH=r'''
import sys,json,hashlib,os,pathlib,io,subprocess
os.umask(0o077)
r=json.load(sys.stdin);raw=r['package'].encode()
assert hashlib.sha256(raw).hexdigest()==r['sha256'],'upload_changed'
assert not pathlib.Path('/home/ubuntu/k-tradingagents-history-tls-recovery-20261030-v1').exists(),'already_reserved'
# Check the real transport before reserving the sole recovery attempt. The
# reconciliation itself repeats these probes after the full database backup.
probe="from tradingagents.utils.tushare_egress import UnixTLSConnection\nfor _ in range(3):\n c=UnixTLSConnection(30)\n try:c.connect()\n finally:c.close()\nprint('TLS_READY')"
proc=subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',probe],capture_output=True,text=True,timeout=95)
assert proc.returncode==0 and proc.stdout.splitlines()[-1:]==['TLS_READY'],'transport_not_ready'
p=pathlib.Path('/home/ubuntu/history-button-acceptance-upload-'+r['sha256']+'.json')
if p.exists():assert p.read_bytes()==raw,'existing_upload_changed'
else:
 fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
assert p.read_bytes()==raw
sys.stdin=io.StringIO(json.dumps(dict(sha256=r['sha256'],auth=r['auth'])))
exec(r['launch'])
'''


def configure_transport():
    # Only the management channel carries compressible operator source. The
    # fixed upstream TLS route, certificate rules and service expiry are intact.
    transport.SSH=[x.replace('ConnectTimeout=10','ConnectTimeout=20') for x in transport.SSH]
    transport.SSH[1:1]=['-C','-o','IPQoS=none','-o','ControlMaster=auto',
        '-o','ControlPath=/home/ubuntu/quote-guard-releases/.ssh-market/tls-compressed-control',
        '-o','ControlPersist=600','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=4']


def main():
    global ROOT,BACKUP,LAUNCH,STATUS,DRIVER,UPLOAD_AND_LAUNCH
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze','execute','status','collect'],required=True)
    parser.add_argument('--run',choices=['v1','v2'],default='v1')
    parser.add_argument('--credentials-file',type=Path)
    args=parser.parse_args()
    if args.run=='v1' and args.action in ('freeze','execute'):
        raise SystemExit('v1_already_consumed: inspect status; never reuse its reservation')
    if args.run=='v2':
        old=ROOT
        ROOT=ROOT.replace('20261030-v1','20261030-v2')
        BACKUP=BACKUP.replace('20261030-v1','20261030-v2')
        LAUNCH=LAUNCH.replace(old,ROOT)
        STATUS=STATUS.replace(old,ROOT)
        DRIVER=DRIVER.replace(old,ROOT)
        UPLOAD_AND_LAUNCH=UPLOAD_AND_LAUNCH.replace(old,ROOT)
    configure_transport()
    if args.action in ('status','collect'):
        result=transport.ssh(STATUS)
        if args.action=='collect':
            target=transport.EVIDENCE/('history-tls-recovery-result-20261030-'+args.run+'.json')
            fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as f:json.dump(result,f,indent=2)
        print(json.dumps(result,ensure_ascii=False));return
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO):
        raise SystemExit('commit_operator_before_freeze')
    if hashlib.sha256(base.RELEASE.with_suffix('.tgz').read_bytes()).hexdigest()!=base.ARCHIVE_SHA:
        raise SystemExit('application_archive_changed')
    raw=json.dumps(package(),sort_keys=True).encode();sha=hashlib.sha256(raw).hexdigest()
    artifact=transport.EVIDENCE/('history-tls-recovery-package-'+sha+'.json')
    if artifact.exists():
        if artifact.read_bytes()!=raw:raise SystemExit('frozen_package_changed')
    else:
        fd=os.open(artifact,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    if args.action=='freeze':
        print(json.dumps(dict(artifact=str(artifact),sha256=sha,upstream_requests=0)));return
    if args.credentials_file is None:parser.error('credentials_file_required')
    result=transport.ssh(UPLOAD_AND_LAUNCH,dict(package=raw.decode(),sha256=sha,
        launch=LAUNCH,auth=read_auth(args.credentials_file,True)),timeout=150)
    print(json.dumps(result))


if __name__=='__main__':main()
