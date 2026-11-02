"""Freeze/check/deliver the prepared quote candidate; never opens its permit.

Freeze requires the successful production preparation receipt. Actual execution
is reserved once and detached from SSH; uncertain results are status-only.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import zlib

import run_history_button_switch as launch
from prepare_paper_quote_supply import CONTROLLER, EVIDENCE, REPO, ROOT, canonical, digest, require
from query_quote_guard_jobs import read_auth, REMOTE as QUERY

RELEASE=CONTROLLER.parent/'paper-supply-switch-v1-20261102'


def freeze(preflight_file):
    require(not subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(),'commit_before_freeze')
    preflight=json.loads(preflight_file.read_text())
    require(preflight.get('ok') is True and preflight.get('stage')=='prepared_no_switch','successful_production_preparation_required')
    helper=json.loads((CONTROLLER/'controller.json').read_text())
    require(digest(helper['code'].encode())==helper['request']['controller_sha256']==preflight['controller_sha256'],'prepare_controller_changed')
    code=(REPO/'deploy/switch_paper_quote_supply.py').read_text()
    package=dict(code=code,code_sha256=digest(code.encode()),payload=dict(check_only=False,
        prepare_code=helper['code'],request=helper['request'],preflight=preflight,query_code=QUERY))
    RELEASE.mkdir(mode=0o700)
    raw=canonical(package)
    with (RELEASE/'controller.json').open('xb') as stream:stream.write(raw)
    with (RELEASE/'SHA256SUMS').open('x') as stream:stream.write(digest(raw)+'  controller.json\n')
    return dict(frozen=True,sha256=digest(raw),source_head=helper['request']['manifest']['source_head'],
                quote_permit_installed=False)


def remote(action,credentials=None,control_socket=None):
    old_root=launch.REMOTE_ROOT
    if action in ('check','execute'):
        raw=(RELEASE/'controller.json').read_bytes()
        require((RELEASE/'SHA256SUMS').read_text()==digest(raw)+'  controller.json\n','frozen_package_changed')
        package=json.loads(raw)
        require(package['code']==(REPO/'deploy/switch_paper_quote_supply.py').read_text()
                and digest(package['code'].encode())==package['code_sha256'],'controller_changed')
        package['payload']['check_only']=action=='check'
        upload=canonical(package);identity=digest(upload)
        # The legacy envelope provides detached execution and one-shot reserve;
        # its fixed upload prefix is harmless because the content hash is unique.
        path='/home/ubuntu/history-button-switch-upload-'+identity+'.json'
        code="import pathlib,sys,json,io,hashlib,os\nos.umask(0o077)\nr=json.load(sys.stdin);raw=r['upload'].encode();p=pathlib.Path("+repr(path)+")\n"
        code+="assert hashlib.sha256(raw).hexdigest()==r['upload_sha256'],'upload_hash_changed'\nif p.exists():assert p.read_bytes()==raw,'upload_changed'\nelse:\n with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
        code+="sys.stdin=io.StringIO(json.dumps({'upload_sha256':r['upload_sha256'],'auth':r['auth']}))\n"
        code+=launch.LAUNCH.replace(old_root,str(ROOT))
        data=dict(upload=upload.decode(),upload_sha256=identity,auth=read_auth(credentials,True))
    else:
        code=launch.STATUS.replace(old_root,str(ROOT))
        code=code.replace("process=json.loads((root/'latest-attempt.json').read_text())",
             "if not (root/'latest-attempt.json').exists():\n print(json.dumps({'attempt_started':False,'reserved':root.exists()}));raise SystemExit(0)\nprocess=json.loads((root/'latest-attempt.json').read_text())")
        data=None
    bootstrap="import io,json,sys,zlib;v=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(v['data']));exec(v['code'])"
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
         '-S',str(control_socket) if control_socket else 'none','-o','BatchMode=yes',
         '-o','ConnectTimeout=10','-o','ConnectionAttempts=1','-o','IPQoS=none',
         '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','osaka-migration',
         'sudo -n python3 -B -c '+shlex.quote(bootstrap)]
    name=EVIDENCE/('switch-'+action+'-'+str(time.time_ns()))
    try:result=subprocess.run(cmd,input=zlib.compress(json.dumps(dict(code=code,data=data)).encode()),capture_output=True,timeout=60)
    except subprocess.TimeoutExpired as error:
        name.with_suffix('.failure.private').write_text(json.dumps(dict(timeout=True,
            stdout=(error.stdout or b'').decode(errors='replace'),stderr=(error.stderr or b'').decode(errors='replace'))))
        raise SystemExit('management_timeout_status_only_do_not_repeat_execute') from None
    if result.returncode:
        name.with_suffix('.failure.private').write_text(json.dumps(dict(returncode=result.returncode,
             stdout=result.stdout.decode(errors='replace'),stderr=result.stderr.decode(errors='replace'))))
        raise SystemExit('management_failed_status_only_do_not_repeat_execute')
    value=json.loads(result.stdout);name.with_suffix('.json').write_text(json.dumps(value,indent=2))
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze','check','execute','status'],required=True)
    parser.add_argument('--preflight-file',type=Path)
    parser.add_argument('--credentials-file',type=Path)
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args();os.umask(0o077)
    if args.action=='freeze':
        require(args.preflight_file is not None,'preflight_file_required')
        result=freeze(args.preflight_file)
    else:
        if args.action in ('check','execute'):require(args.credentials_file is not None,'credentials_required')
        result=remote(args.action,args.credentials_file,args.control_socket)
    print(json.dumps(result))


if __name__=='__main__':
    main()
