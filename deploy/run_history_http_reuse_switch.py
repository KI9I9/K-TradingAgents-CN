"""One additional bind, one replacement; reuse audited switch/rollback checks.

Requires prepare_history_http_reuse.py's fresh backup and frozen package.
No STOP removal or historical data request is performed by this controller.
"""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

import run_history_basic_switch as prior
from prepare_history_http_reuse import RELEASE, ROOT, EVIDENCE, REPO, db_probe
from query_quote_guard_jobs import read_auth

base = prior.base
previous_payload = base.payload
old_root = '/home/ubuntu/k-tradingagents-history-basic-v1-20261030'
base.RELEASE = RELEASE
for name in ('LAUNCH', 'STATUS'):
    setattr(base, name, getattr(base, name).replace(old_root, ROOT))
base.STATUS = base.STATUS.replace(
    "process=json.loads((root/'latest-attempt.json').read_text())",
    "if not (root/'latest-attempt.json').exists():\n    print(json.dumps({'reserved':root.exists(),'attempt_started':False})); raise SystemExit(0)\nprocess=json.loads((root/'latest-attempt.json').read_text())")


def controller():
    code = prior.controller().replace(old_root, ROOT).replace('compose.history-basic.json', 'compose.history-http-reuse.json')
    code = code.replace('deployed_basic_gate_stopped', 'deployed_http_reuse_gate_stopped')
    code = code.replace("require(len(current['Mounts']) == 39, 'mount_count_mismatch')",
                        "require(len(current['Mounts']) == (40 if new else 39), 'mount_count_mismatch')")
    start = code.index("    for target, source in allowed['backend'].items():")
    end = code.index('    require(not VERIFY.compare(normalized', start)
    code = code[:start] + '''    for target, source in list(allowed['backend'].items()):
        matches = [v for v in normalized['services']['backend']['volumes'] if v['target'] == target]
        if matches:
            require(target == '/app/app/services/tushare_history_batches.py' and len(matches) == 1, 'unexpected_replacement')
            mount = matches[0]
            require(mount.get('type') == 'bind' and mount.get('read_only') is True, 'old_bind_unsafe')
            require(mount['source'] == PAYLOAD['old_sources'][target], 'old_source_changed')
            mount['source'] = source
            allowed['backend'].pop(target)
    require(set(allowed['backend']) == {'/app/tradingagents/utils/tushare_egress.py'}, 'unexpected_new_bind')
''' + code[end:]
    code = code.replace("'backend_mounts': 39,", "'backend_mounts': 40,")
    # Keep the transport/worker pair installed together before importing services.
    marker = "        smoke()\n"
    assert code.count(marker) == 1
    code = code.replace(marker, '''        require(in_backend("from tradingagents.utils.tushare_egress import history_transport_session; from app.services.tushare_history_batches import HistoryBatchService; print('IMPORT_OK')").splitlines()[-1] == 'IMPORT_OK', 'reuse_pair_import_failed')
''' + marker)
    ast.parse(code)
    return code


def payload(check_only, credentials):
    p = previous_payload(check_only, credentials)
    fresh = json.loads((EVIDENCE / 'history-http-reuse-preflight.json').read_text())
    assert fresh['ok'] and fresh['stage'] == 'prepared_no_switch'
    assert p['checksums_sha256'] == fresh['checksums_sha256'], 'stage_differs_from_frozen_package'
    p.update(expected_chain=fresh['chain'], config_hashes=fresh['config_hashes'], image_id=fresh['image_id'],
             old_sources=fresh['old_sources'], old_modules=fresh['modules'], generation_tree_sha256=fresh['generation_tree_sha256'],
             db_probe=db_probe())
    p['ingress_api_probe'] = p['ingress_api_probe'].replace('[usage_limit]', '[timeout]')
    return p


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['freeze-controller', 'check', 'execute', 'status', 'collect'], required=True)
    parser.add_argument('--credentials-file', type=Path)
    parser.add_argument('--control-socket', type=Path, help='Existing private SSH master, if available')
    args = parser.parse_args()
    os.umask(0o077)
    artifact = RELEASE / 'controller.json'
    if args.action == 'freeze-controller':
        assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO), 'commit_before_freeze'
        source = controller()
        # Auth stays out of the frozen artifact; the existing helper only reads it.
        assert args.credentials_file is not None, 'credentials_file_required'
        request = dict(code=source, code_sha256=hashlib.sha256(source.encode()).hexdigest(), payload=payload(False, args.credentials_file))
        request['payload'].pop('auth')
        raw = json.dumps(request).encode()
        with artifact.open('xb') as f:
            f.write(raw)
        print(json.dumps(dict(controller_sha256=request['code_sha256'], package_sha256=hashlib.sha256(raw).hexdigest())))
        return
    data = None
    if args.action in ('check', 'execute'):
        assert args.credentials_file is not None, 'credentials_file_required'
        raw = artifact.read_bytes()
        request = json.loads(raw)
        assert request['code'] == controller(), 'controller_changed_since_freeze'
        request['payload']['check_only'] = args.action == 'check'
        raw = json.dumps(request).encode()
        sha = hashlib.sha256(raw).hexdigest()
        # The one-use reservation remains on the host even after management loss.
        launch = base.LAUNCH
        upload = '/home/ubuntu/history-button-switch-upload-' + sha + '.json'
        code = 'import pathlib,sys,json,hashlib,io,os\nos.umask(0o077)\nr=json.load(sys.stdin);raw=r["upload"].encode();p=pathlib.Path(' + repr(upload) + ')\n'
        code += "assert hashlib.sha256(raw).hexdigest()==r['upload_sha256'],'upload_changed'\n"
        code += "if p.exists():assert p.read_bytes()==raw,'existing_upload_changed'\nelse:\n with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
        code += "sys.stdin=io.StringIO(json.dumps(dict(upload_sha256=r['upload_sha256'],auth=r['auth'])))\n" + launch
        data = dict(upload=raw.decode(), upload_sha256=sha, auth=read_auth(args.credentials_file, True))
    else:
        code = base.STATUS
        if args.action == 'collect':
            code = code.replace('print(json.dumps(out))', '''
names=['process.json','state.json','completion.json','backup/complete.json','installed-module-hashes.json',
 'db-before.json','db-stopped.json','db-after.json','jobs-before.json','jobs-after.json','installed-smoke.json',
 'audit-before.json','audit-after.json','frontend-before.json','frontend-after.json','ingress-api-after.json',
 'audit-rollback.json','jobs-rollback.json']
out['evidence']={n:json.loads((directory/n).read_text()) for n in names if (directory/n).exists()}
print(json.dumps(out))
''')
    cmd = ['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config', '-S', str(args.control_socket) if args.control_socket else 'none', '-C',
           '-o', 'BatchMode=yes', '-o', 'IPQoS=none', '-o', 'ConnectTimeout=25', 'osaka-migration',
           'sudo -n python3 -B -c ' + shlex.quote(code)]
    prefix = EVIDENCE / ('history-http-switch-' + args.action + '-' + str(time.time_ns()))
    try:
        p = subprocess.run(cmd, input=json.dumps(data) if data else None, capture_output=True, text=True, timeout=90)
    except subprocess.TimeoutExpired:
        raise SystemExit('management_timeout: inspect status; never relaunch uncertain execute') from None
    prefix.with_suffix('.private').write_text(p.stderr)
    if p.returncode:
        raise SystemExit('management_failed: inspect status; errors withheld')
    report = json.loads(p.stdout)
    prefix.with_suffix('.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
