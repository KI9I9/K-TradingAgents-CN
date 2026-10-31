"""One explicit continuation after the HTTP reuse release, with full backup.

Reuses the audited raw timeout reconciliation, retains all 2298 charged requests,
and submits the original button once. No new policy, scope or expiry is created.
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

import run_history_tls_recovery as previous
from prepare_history_http_reuse import REPO, RELEASE, ROOT as SWITCH, EVIDENCE, PREFLIGHT
from query_quote_guard_jobs import read_auth

ROOT = '/home/ubuntu/k-tradingagents-history-http-reuse-recovery-20261101-v2'
BACKUP = '/app/runtime/history-http-reuse-recovery-20261101-v2'
LAUNCH = previous.LAUNCH.replace(previous.ROOT, ROOT).replace(
    '/home/ubuntu/k-tradingagents-sync-fix-v1-20261030', SWITCH).replace(
    'deployed_sync_fix_gate_stopped', 'deployed_http_reuse_gate_stopped')
DRIVER = previous.DRIVER.replace(previous.ROOT, ROOT)
STATUS = previous.STATUS.replace(previous.ROOT, ROOT)


def package():
    manifest = json.loads((RELEASE / 'manifest.json').read_text())
    fresh = json.loads(PREFLIGHT.read_text())
    hashes = fresh['modules'] | {f['target']: f['sha256'] for f in manifest['files']}
    core = (REPO / 'deploy/history_timeout_recovery.py').read_text()
    tls = (REPO / 'deploy/history_tls_recovery.py').read_text()
    code = ("import json,sys\ncore={'__name__':'operator_backup'}\nexec(" + repr(core) + ",core)\n"
            "tls={'__name__':'operator_tls'}\nexec(" + repr(tls) + ",tls)\n"
            "raise SystemExit(core['main'](json.load(sys.stdin),recoverer=tls['recover_raw_timeout_verified'],"
            "expected_reason='timeout',raw_count=38641,run_directory=" + repr(BACKUP) + "))\n")
    for source in (code, LAUNCH, DRIVER, STATUS):
        ast.parse(source)
    return dict(source_head=manifest['source_head'], operator_head=subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(), driver=DRIVER, code=code,
        request=dict(source_hashes=hashes, runtime_tool=(REPO / 'deploy/quote_guard_runtime.py').read_text(),
                     generation='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa',
                     used=2298, plan='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['freeze', 'execute', 'status', 'collect'], required=True)
    parser.add_argument('--credentials-file', type=Path)
    parser.add_argument('--control-socket', type=Path, help='Existing private SSH master, if available')
    args = parser.parse_args()
    os.umask(0o077)
    artifact = RELEASE / 'recovery.json'
    if args.action == 'freeze':
        assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO), 'commit_before_freeze'
        raw = json.dumps(package(), sort_keys=True).encode()
        with artifact.open('xb') as f:
            f.write(raw)
        print(json.dumps(dict(sha256=hashlib.sha256(raw).hexdigest(), used=2298, expected_raw=38641, upstream_requests=0)))
        return
    data = None
    if args.action == 'execute':
        assert args.credentials_file is not None, 'credentials_file_required'
        raw = artifact.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        code = "import json,pathlib,hashlib,sys,io,os\nos.umask(0o077)\nr=json.load(sys.stdin);raw=r['package'].encode()\n"
        code += "assert hashlib.sha256(raw).hexdigest()==r['sha256'],'upload_changed'\n"
        code += "p=pathlib.Path('/home/ubuntu/history-button-acceptance-upload-'+r['sha256']+'.json')\n"
        code += "if p.exists():assert p.read_bytes()==raw,'existing_upload_changed'\nelse:\n with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
        code += "sys.stdin=io.StringIO(json.dumps(dict(sha256=r['sha256'],auth=r['auth'])))\n" + LAUNCH
        data = dict(package=raw.decode(), sha256=sha, auth=read_auth(args.credentials_file, True))
    else:
        code = STATUS
    ast.parse(code)
    cmd = ['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config', '-S', str(args.control_socket) if args.control_socket else 'none', '-C',
           '-o', 'BatchMode=yes', '-o', 'IPQoS=none', '-o', 'ConnectTimeout=25', 'osaka-migration',
           'sudo -n python3 -B -c ' + shlex.quote(code)]
    prefix = EVIDENCE / ('history-http-recovery-' + args.action + '-' + str(time.time_ns()))
    try:
        p = subprocess.run(cmd, input=json.dumps(data) if data else None, capture_output=True, text=True, timeout=90)
    except subprocess.TimeoutExpired:
        raise SystemExit('management_timeout: inspect status; never relaunch uncertain recovery') from None
    prefix.with_suffix('.private').write_text(p.stderr)
    if p.returncode:
        raise SystemExit('management_failed: inspect status; errors withheld')
    report = json.loads(p.stdout)
    prefix.with_suffix('.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
