"""Execute/check one frozen scheduling release; no permission renewal or data run.

Uses the existing detached launch envelope, with a distinct root/reservation.
Never uses the earlier release's payload, execute reservation or recovery script.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

import run_history_button_switch as envelope
from prepare_history_schedule import REPO,EVIDENCE,RELEASE,ROOT,PREFLIGHT,remote,db_probe,digest
from query_quote_guard_jobs import read_auth,REMOTE as QUERY


def controller():return (REPO/'deploy/switch_history_schedule.py').read_text()


def payload():
    fresh=json.loads(PREFLIGHT.read_text())
    assert fresh['ok'] and fresh['stage']=='prepared_no_switch','successful_fresh_preflight_required'
    prior=json.loads((RELEASE.parent/'history-http-reuse-v2-20261101/controller.json').read_text())['payload']
    values={k:prior[k] for k in ('seal_hashes','expected_gate','expected_runtime_sha256')}
    values.update(api_probe=QUERY,check_only=False,checksums_sha256=fresh['checksums_sha256'],
        expected_chain=fresh['chain'],config_hashes=fresh['config_hashes'],image_id=fresh['image_id'],
        old_sources=fresh['old_sources'],new_targets=fresh['new_targets'],old_modules=fresh['modules'],
        protected_files=fresh['protected_files'],generation_tree_sha256=fresh['generation_tree_sha256'],
        runtime_tool=(REPO/'deploy/quote_guard_runtime.py').read_text(),guard_smoke=(REPO/'deploy/quote_guard_smoke.py').read_text(),
        verify_tool=(REPO/'deploy/verify_compose_merge.py').read_text(),db_probe=db_probe(),
        audit_tool=(REPO/'deploy/audit_tushare_history.py').read_text(),history_smoke=(REPO/'deploy/history_contract_smoke.py').read_text())
    assert digest((RELEASE/'SHA256SUMS').read_bytes())==fresh['checksums_sha256'],'stage_differs_from_frozen'
    return values


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze-controller','check','execute','status','collect'],required=True)
    parser.add_argument('--credentials-file',type=Path)
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args();os.umask(0o077)
    artifact=RELEASE/'controller.json'
    if args.action=='freeze-controller':
        import subprocess
        assert not subprocess.check_output(['git','status','--porcelain'],cwd=REPO),'commit_before_freeze'
        request=dict(code=controller(),code_sha256=digest(controller().encode()),payload=payload())
        with artifact.open('x') as f:json.dump(request,f)
        print(json.dumps(dict(controller_sha256=request['code_sha256'],package_sha256=digest(artifact.read_bytes()))));return
    old='/home/ubuntu/k-tradingagents-history-button-v1-20261029'
    data=None
    if args.action in ('check','execute'):
        assert args.credentials_file is not None,'credentials_required'
        request=json.loads(artifact.read_text());assert request['code']==controller(),'controller_changed_since_freeze'
        request['payload']['check_only']=args.action=='check'
        raw=json.dumps(request).encode();identity=digest(raw)
        upload='/home/ubuntu/history-button-switch-upload-'+identity+'.json'
        launch=envelope.LAUNCH.replace(old,ROOT)
        code="import pathlib,sys,json,hashlib,io,os\nos.umask(0o077)\nr=json.load(sys.stdin);raw=r['upload'].encode();p=pathlib.Path("+repr(upload)+")\n"
        code+="assert hashlib.sha256(raw).hexdigest()==r['upload_sha256'],'upload_changed'\nif p.exists():assert p.read_bytes()==raw,'upload_changed'\nelse:\n with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
        code+="sys.stdin=io.StringIO(json.dumps(dict(upload_sha256=r['upload_sha256'],auth=r['auth'])))\n"+launch
        data=dict(upload=raw.decode(),upload_sha256=identity,auth=read_auth(args.credentials_file,True))
    else:
        code=envelope.STATUS.replace(old,ROOT)
        code=code.replace("process=json.loads((root/'latest-attempt.json').read_text())", "if not (root/'latest-attempt.json').exists():\n print(json.dumps({'reserved':root.exists(),'attempt_started':False})); raise SystemExit(0)\nprocess=json.loads((root/'latest-attempt.json').read_text())")
        if args.action=='collect':
            code=code.replace('print(json.dumps(out))',"names=['completion.json','state.json','backup/complete.json','installed-module-hashes.json','db-before.json','db-stopped.json','db-after.json','audit-before.json','audit-after.json','jobs-before.json','jobs-after.json','ingress-api-after.json','jobs-rollback.json']; out['evidence']={n:json.loads((directory/n).read_text()) for n in names if (directory/n).exists()}; print(json.dumps(out))")
    result=remote(code,data,args.control_socket)
    (EVIDENCE/('history-schedule-switch-'+args.action+'-'+str(time.time_ns())+'.json')).write_text(json.dumps(result,indent=2))
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
