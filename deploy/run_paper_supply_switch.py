"""Freeze/check/deliver the prepared quote candidate; never opens its permit.

Freeze requires the successful production preparation receipt. Actual execution
is reserved once and detached from SSH; uncertain results are status-only.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time
import zlib

import run_history_button_switch as launch
from prepare_paper_quote_supply import CONTROLLER, EVIDENCE, REPO, ROOT, canonical, digest, require
from query_quote_guard_jobs import read_auth, REMOTE as QUERY

RELEASE=CONTROLLER.parent/'paper-supply-switch-v2-20261102'


def freeze(preflight_file,recovery_file=None,preferences_diff_file=None,recovery_attempt=None):
    require(not subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(),'commit_before_freeze')
    preflight=json.loads(preflight_file.read_text())
    require(preflight.get('ok') is True and preflight.get('stage')=='prepared_no_switch','successful_production_preparation_required')
    helper=json.loads((CONTROLLER/'controller.json').read_text())
    require(digest(helper['code'].encode())==helper['request']['controller_sha256']==preflight['controller_sha256'],'prepare_controller_changed')
    code=(REPO/'deploy/switch_paper_quote_supply.py').read_text()
    package=dict(code=code,code_sha256=digest(code.encode()),payload=dict(check_only=False,
        prepare_code=helper['code'],request=helper['request'],preflight=preflight,query_code=QUERY))
    if recovery_file is not None:
        from switch_paper_quote_supply import PREFERENCE_TIMESTAMP_JOBS
        require(preferences_diff_file is not None and recovery_attempt is not None
                and re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}',recovery_attempt),
                'recovery_field_evidence_required')
        def evidence(path):
            value=json.loads(path.read_text())
            if 'stdout' in value:
                require(value.get('returncode')==0,'successful_recovery_evidence_required')
                value=json.loads(value['stdout'])
            return value
        recovery=evidence(recovery_file);fields=evidence(preferences_diff_file)
        require(recovery.get('status')=='original_backend_recovered'
                and recovery.get('running') is True and recovery.get('health')=='healthy'
                and recovery.get('mounts')==43 and recovery.get('quote_permit_absent') is True
                and recovery.get('protected_except_preferences_digest_unchanged') is True,
                'original_recovery_unproven')
        changes=fields.get('changes',[])
        require(type(fields.get('before_count')) is int
                and len(PREFERENCE_TIMESTAMP_JOBS)<=fields['before_count']<=20000
                and fields['before_count']==fields.get('after_count')
                and {v['job_id'] for v in changes}==set(PREFERENCE_TIMESTAMP_JOBS)
                and len(changes)==len(PREFERENCE_TIMESTAMP_JOBS)
                and all(v['changed_fields']==['updated_at'] and v['before_enabled'] is False
                        and v['after_enabled'] is False for v in changes),'unexpected_preference_changes')
        package['payload']['baseline_recovery']=dict(attempt=recovery_attempt,report=recovery,
            preference_diff=fields)
    RELEASE.mkdir(mode=0o700)
    raw=canonical(package)
    with (RELEASE/'controller.json').open('xb') as stream:stream.write(raw)
    with (RELEASE/'SHA256SUMS').open('x') as stream:stream.write(digest(raw)+'  controller.json\n')
    return dict(frozen=True,sha256=digest(raw),source_head=helper['request']['manifest']['source_head'],
                quote_permit_installed=False)


def remote(action,credentials=None,control_socket=None,reuse_base=None):
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
        code="import pathlib,sys,json,io,hashlib,os\nos.umask(0o077)\nr=json.load(sys.stdin)\n"
        data=dict(upload_sha256=identity,auth=read_auth(credentials,True))
        if reuse_base is None:
            data['upload']=upload.decode()
            code+="raw=r['upload'].encode()\n"
        else:
            # Reuse verified non-secret bytes already uploaded for v1. Rebuild
            # only the controller and recovery anchor, then compare the entire
            # canonical package, both locally and remotely, with the frozen SHA.
            base_raw=reuse_base.read_bytes();base=json.loads(base_raw)
            replacement=dict(code=package['code'],code_sha256=package['code_sha256'],
                check_only=package['payload']['check_only'],
                baseline_recovery=package['payload'].get('baseline_recovery'))
            rebuilt=json.loads(base_raw)
            rebuilt.update(code=replacement['code'],code_sha256=replacement['code_sha256'])
            rebuilt['payload']['check_only']=replacement['check_only']
            if replacement['baseline_recovery'] is not None:
                rebuilt['payload']['baseline_recovery']=replacement['baseline_recovery']
            require(canonical(rebuilt)==upload,'reuse_base_cannot_reconstruct_frozen_package')
            data.update(base_sha256=digest(base_raw),replacement=replacement)
            code+="base=pathlib.Path('/home/ubuntu')/('history-button-switch-upload-'+r['base_sha256']+'.json')\n"
            code+="original=base.read_bytes();assert hashlib.sha256(original).hexdigest()==r['base_sha256'],'reuse_base_changed'\n"
            code+="value=json.loads(original);patch=r['replacement'];value.update(code=patch['code'],code_sha256=patch['code_sha256'])\n"
            code+="value['payload']['check_only']=patch['check_only']\nif patch['baseline_recovery'] is not None:value['payload']['baseline_recovery']=patch['baseline_recovery']\n"
            code+="raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode()\n"
        code+="p=pathlib.Path("+repr(path)+")\n"
        code+="assert hashlib.sha256(raw).hexdigest()==r['upload_sha256'],'upload_hash_changed'\nif p.exists():assert p.read_bytes()==raw,'upload_changed'\nelse:\n with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
        code+="sys.stdin=io.StringIO(json.dumps({'upload_sha256':r['upload_sha256'],'auth':r['auth']}))\n"
        code+=launch.LAUNCH.replace(old_root,str(ROOT)).replace(
            "root/'execution-reserved'", "root/'execution-v2-reserved'")
    else:
        code=launch.STATUS.replace(old_root,str(ROOT))
        code=code.replace("process=json.loads((root/'latest-attempt.json').read_text())",
             "if not (root/'latest-attempt.json').exists():\n print(json.dumps({'attempt_started':False,'reserved':root.exists()}));raise SystemExit(0)\nprocess=json.loads((root/'latest-attempt.json').read_text())")
        code=code.replace('print(json.dumps(out))',
            "out['execution_reserved']=(root/'execution-v2-reserved').exists()\n"
            "out['v1_execution_reserved']=(root/'execution-reserved').exists()\nprint(json.dumps(out))")
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
    parser.add_argument('--recovery-file',type=Path)
    parser.add_argument('--preferences-diff-file',type=Path)
    parser.add_argument('--recovery-attempt')
    parser.add_argument('--credentials-file',type=Path)
    parser.add_argument('--control-socket',type=Path)
    parser.add_argument('--reuse-base-package',type=Path)
    args=parser.parse_args();os.umask(0o077)
    if args.action=='freeze':
        require(args.preflight_file is not None,'preflight_file_required')
        result=freeze(args.preflight_file,args.recovery_file,args.preferences_diff_file,args.recovery_attempt)
    else:
        if args.action in ('check','execute'):require(args.credentials_file is not None,'credentials_required')
        result=remote(args.action,args.credentials_file,args.control_socket,args.reuse_base_package)
    print(json.dumps(result))


if __name__=='__main__':
    main()
