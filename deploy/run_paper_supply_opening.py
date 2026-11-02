"""Freeze/check/activate a 24-hour paper-only quote permit after delivery.

This package is separate from delivery, so activation failure leaves the new
application safely installed with its permit closed. Never creates an order.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import types
import zlib

import run_history_button_switch as launch
import run_paper_supply_switch as delivery
from prepare_paper_quote_supply import CONTROLLER,EVIDENCE,REPO,ROOT,canonical,digest,require
from query_quote_guard_jobs import read_auth,REMOTE as QUERY

RELEASE=CONTROLLER.parent/'paper-supply-opening-v1-20261102'
ACTIVATION_ROOT=ROOT/'activation'

REMOTE_CONTROLLER=r'''
import fcntl,json,os,pathlib,subprocess,sys
os.umask(0o077)
payload=json.load(sys.stdin)
root=pathlib.Path('/home/ubuntu/k-tradingagents-paper-supply-20261102')
directory=root/'activation/attempts'/payload['attempt']
assert directory.parent==root/'activation/attempts' and directory.is_dir(),'invalid_attempt'
lock=(root/'switch.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
def save(name,value):
 p=directory/name;tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(value,indent=2));tmp.replace(p)
def event(stage):save('state.json',{'stage':stage})
try:
 event('checking_installed_quote_delivery')
 previous=json.loads((root/'latest-attempt.json').read_text())
 complete=json.loads((root/'attempts'/previous['attempt']/'completion.json').read_text())
 assert complete['status']=='deployed_paper_permit_absent' and complete['source_head']==payload['source_head'],'delivery_not_accepted'
 info=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],timeout=10))[0]
 assert info['State']['Running'] and info['State'].get('Health',{}).get('Status')=='healthy','backend_unhealthy'
 assert len(info['Mounts'])==44 and info['Image']==payload['request']['image'],'installed_release_changed'
 event('checking_permission_only' if payload['check_only'] else 'activating_and_querying_quotes')
 result=subprocess.run(['docker','exec','-i',info['Id'],'python3','-B','-c',payload['backend_code']],
  input=json.dumps(payload),text=True,capture_output=True,timeout=130)
 rows=[s.split(':',1)[1] for s in result.stdout.splitlines() if s.startswith('PAPER_OPENING_RESULT:')]
 if len(rows)!=1:
  save('private-failure.json',dict(returncode=result.returncode,stdout=result.stdout,stderr=result.stderr))
  raise RuntimeError('opening_result_unproven_check_permit')
 value=json.loads(rows[0]);save('completion.json',value)
 event(value['status'])
except BaseException as error:
 save('failure.json',{'error_type':type(error).__name__,'reason':str(error) if type(error) is RuntimeError else 'details_withheld'})
 event('opening_unproven_inspect_before_any_retry')
 sys.exit(1)
'''


def freeze():
    require(not subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(),'commit_before_freeze')
    raw=(delivery.RELEASE/'controller.json').read_bytes()
    require((delivery.RELEASE/'SHA256SUMS').read_text()==digest(raw)+'  controller.json\n','delivery_package_changed')
    package=json.loads(raw);base=package['payload'];request=base['request']
    helper=types.ModuleType('prepare');helper.__file__=str(ROOT/'deploy/prepare.py')
    require(digest(base['prepare_code'].encode())==request['controller_sha256'],'prepare_controller_changed')
    exec(base['prepare_code'],helper.__dict__)
    payload=dict(check_only=False,delivery_package_sha256=digest(raw),request=request,
        source_head=request['manifest']['source_head'],entry_hash=helper.ENTRY_HASH,
        snapshot_code=helper.BACKEND_SNAPSHOT,query_code=QUERY,
        backend_code=(REPO/'deploy/paper_supply_opening.py').read_text(),
        modules=request['modules']|{target:item['sha256'] for target,item in request['files'].items()},
        policy=dict(version=1,duration_seconds=86400,source='sina_l1',scope='paper_execution_sh_sz'))
    value=dict(code=REMOTE_CONTROLLER,code_sha256=digest(REMOTE_CONTROLLER.encode()),payload=payload)
    RELEASE.mkdir(mode=0o700);raw=canonical(value)
    with (RELEASE/'controller.json').open('xb') as stream:stream.write(raw)
    with (RELEASE/'SHA256SUMS').open('x') as stream:stream.write(digest(raw)+'  controller.json\n')
    return dict(frozen=True,sha256=digest(raw),delivery_package_sha256=payload['delivery_package_sha256'],
                duration_seconds=86400,scope=payload['policy']['scope'],orders_created=0)


def remote(action,credentials=None):
    if action in ('check','execute'):
        raw=(RELEASE/'controller.json').read_bytes()
        require((RELEASE/'SHA256SUMS').read_text()==digest(raw)+'  controller.json\n','opening_package_changed')
        value=json.loads(raw)
        require(value['code']==REMOTE_CONTROLLER and value['payload']['backend_code']==
                (REPO/'deploy/paper_supply_opening.py').read_text(),'opening_controller_changed')
        value['payload']['check_only']=action=='check';upload=canonical(value);identity=digest(upload)
        path='/home/ubuntu/history-button-switch-upload-'+identity+'.json'
        code="import pathlib,json,sys,os,io,hashlib\nos.umask(0o077)\nr=json.load(sys.stdin);raw=r['upload'].encode();p=pathlib.Path("+repr(path)+")\n"
        code+="assert hashlib.sha256(raw).hexdigest()==r['upload_sha256'],'upload_hash_changed'\n"
        if action=='execute':
            code+="from datetime import datetime\nfrom zoneinfo import ZoneInfo\nn=datetime.now(ZoneInfo('Asia/Shanghai'));m=n.hour*60+n.minute\nassert n.weekday()<5 and (570<=m<685 or 780<=m<895),'not_in_activation_window'\n"
        code+="if p.exists():assert p.read_bytes()==raw,'upload_changed'\nelse:\n with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
        code+="pathlib.Path("+repr(str(ACTIVATION_ROOT))+").mkdir(mode=0o700,exist_ok=True)\n"
        code+="sys.stdin=io.StringIO(json.dumps({'upload_sha256':r['upload_sha256'],'auth':r['auth']}))\n"
        code+=launch.LAUNCH.replace(launch.REMOTE_ROOT,str(ACTIVATION_ROOT))
        data=dict(upload=upload.decode(),upload_sha256=identity,auth=read_auth(credentials,True))
    else:
        code=launch.STATUS.replace(launch.REMOTE_ROOT,str(ACTIVATION_ROOT))
        code=code.replace("process=json.loads((root/'latest-attempt.json').read_text())",
             "if not (root/'latest-attempt.json').exists():\n print(json.dumps({'attempt_started':False,'reserved':root.exists()}));raise SystemExit(0)\nprocess=json.loads((root/'latest-attempt.json').read_text())")
        data=None
    bootstrap="import io,json,sys,zlib;v=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(v['data']));exec(v['code'])"
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S','none',
         '-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ConnectionAttempts=1','-o','IPQoS=none',
         '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','osaka-migration',
         'sudo -n python3 -B -c '+shlex.quote(bootstrap)]
    path=EVIDENCE/('opening-'+action+'-'+str(time.time_ns())+'.json')
    try:result=subprocess.run(cmd,input=zlib.compress(json.dumps(dict(code=code,data=data)).encode()),capture_output=True,timeout=60)
    except subprocess.TimeoutExpired:
        raise SystemExit('opening_management_timeout_status_only') from None
    if result.returncode:
        path.with_suffix('.failure.private').write_text(json.dumps(dict(returncode=result.returncode,
            stdout=result.stdout.decode(errors='replace'),stderr=result.stderr.decode(errors='replace'))))
        raise SystemExit('opening_management_failed_status_only')
    value=json.loads(result.stdout);path.write_text(json.dumps(value,indent=2));return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze','check','execute','status'],required=True)
    parser.add_argument('--credentials-file',type=Path)
    args=parser.parse_args();os.umask(0o077)
    if args.action in ('check','execute'):require(args.credentials_file is not None,'credentials_required')
    print(json.dumps(freeze() if args.action=='freeze' else remote(args.action,args.credentials_file)))


if __name__=='__main__':main()
