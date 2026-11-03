"""Freeze, precheck and launch one finite paper-quote continuity release.

Expiry is an explicit absolute CLI argument. No default grants a renewal.
Credentials are passed only over stdin and are absent from frozen packages.
"""
import argparse
import hashlib
import inspect
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import zlib

from etf_history_delivery import SNAPSHOT
from paper_supply_continuity import ROOT, TARGET
from paper_supply_opening import validate_quote
from query_quote_guard_jobs import REMOTE, read_auth
from run_history_button_switch import LAUNCH, STATUS, REMOTE_ROOT

REPO = Path(__file__).resolve().parents[1]


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def freeze(args):
    from datetime import datetime, timedelta
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO).strip(),
            'commit_before_freeze')
    expiry = datetime.fromisoformat(args.expires_at)
    require(expiry.tzinfo is not None and timedelta(0) < expiry-datetime.now(expiry.tzinfo) <= timedelta(days=7),
            'explicit_finite_future_expiry_required')
    runtime = json.loads(args.runtime.read_text())
    if 'stdout' in runtime:
        require(runtime.get('returncode') == 0, 'runtime_query_failed')
        runtime = json.loads(runtime['stdout'])
    require(runtime['health'] == 'healthy' and runtime['mount_count'] == 45, 'runtime_baseline_changed')
    old = json.loads(args.baseline_package.read_bytes())['payload']['request']
    require(old['sources'].keys() == {'/app/app/routers/stock_sync.py', '/app/app/worker/etf_sync_service.py'},
            'ETF_baseline_invalid')
    modules = dict(old['modules'])
    modules.update({name: row['sha256'] for name, row in old['sources'].items()})
    for name, expected in modules.items():
        if name != TARGET:
            require(digest((REPO/name.removeprefix('/app/')).read_bytes()) == expected,
                    'unrelated_local_source_changed:'+name)
    source = (REPO/TARGET.removeprefix('/app/')).read_text()
    request = dict(source_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
        backend_id=runtime['id'], image=runtime['image'], modules=modules,
        chain=old['chain']+['-f', '/home/ubuntu/k-tradingagents-etf-history-20261103/staged/compose.etf-history.json'],
        candidate=source, candidate_sha256=digest(source.encode()),
        previous_permit_sha256=args.previous_permit_sha256, expires_at=expiry.isoformat(),
        runtime=(REPO/'deploy/quote_guard_runtime.py').read_text(), snapshot_code=SNAPSHOT,
        query_code=REMOTE, validate_code=inspect.getsource(validate_quote),
        verify_code=(REPO/'deploy/verify_compose_merge.py').read_text(),
        prepare_code=(REPO/'deploy/prepare_paper_quote_supply.py').read_text(),
        renewal_code=(REPO/'deploy/renew_paper_quote_supply.py').read_text())
    require(len(args.previous_permit_sha256) == 64 and
            all(c in '0123456789abcdef' for c in args.previous_permit_sha256), 'previous_hash_invalid')
    code = (REPO/'deploy/paper_supply_continuity.py').read_text()
    package = dict(code=code, code_sha256=digest(code.encode()), payload=dict(request=request, check_only=True))
    raw = canonical(package)
    args.package.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with args.package.open('xb') as stream:
        stream.write(raw)
    args.package.chmod(0o600)
    args.package.with_suffix('.sha256').write_text(digest(raw)+'\n')
    return dict(frozen=True, sha256=digest(raw), source_head=request['source_head'],
                expires_at=request['expires_at'], orders_created=0)


def remote(args):
    data = None
    if args.action in ('check', 'execute'):
        raw = args.package.read_bytes()
        require(digest(raw) == args.package.with_suffix('.sha256').read_text().strip(), 'package_changed')
        package = json.loads(raw)
        require(digest(package['code'].encode()) == package['code_sha256'], 'controller_changed')
        package['payload']['check_only'] = args.action == 'check'
        upload = canonical(package); sha = digest(upload)
        code = '''import json,sys,io,pathlib,hashlib,os
os.umask(0o077)
q=json.load(sys.stdin);raw=q['upload'].encode()
assert hashlib.sha256(raw).hexdigest()==q['upload_sha256'],'upload_changed'
p=pathlib.Path('/home/ubuntu')/('history-button-switch-upload-'+q['upload_sha256']+'.json')
if p.exists():assert p.read_bytes()==raw,'upload_collision'
else:
 with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
'''
        code += 'root=pathlib.Path('+repr(str(ROOT))+')\nroot.mkdir(exist_ok=True,mode=0o711)\nroot.chmod(0o711)\n'
        code += "sys.stdin=io.StringIO(json.dumps(dict(upload_sha256=q['upload_sha256'],auth=q['auth'])))\n"
        code += LAUNCH.replace(REMOTE_ROOT, str(ROOT))
        data = dict(upload=upload.decode(), upload_sha256=sha, auth=read_auth(args.credentials, True))
    else:
        code = STATUS.replace(REMOTE_ROOT, str(ROOT))
    bootstrap = "import json,sys,zlib,io;q=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(q['data']));exec(q['code'])"
    cmd = ['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config', '-S', str(args.control_socket),
        '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-o', 'ConnectionAttempts=1', '-o', 'IPQoS=none',
        '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', 'osaka-migration',
        'sudo -n python3 -B -c '+shlex.quote(bootstrap)]
    try:
        result = subprocess.run(cmd, input=zlib.compress(json.dumps(dict(code=code, data=data)).encode()),
                                capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        raise SystemExit('management_timeout_status_only_do_not_repeat_execute') from None
    receipt = args.package.parent/(args.action+'-'+str(time.time_ns())+'.private')
    receipt.write_text(json.dumps(dict(returncode=result.returncode, stdout=result.stdout.decode(errors='replace'),
                                      stderr=result.stderr.decode(errors='replace')))); receipt.chmod(0o600)
    require(result.returncode == 0, 'management_failed_status_only_receipt_saved')
    return json.loads(result.stdout)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['freeze', 'check', 'execute', 'status'], required=True)
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--runtime', type=Path)
    parser.add_argument('--baseline-package', type=Path)
    parser.add_argument('--expires-at')
    parser.add_argument('--previous-permit-sha256')
    parser.add_argument('--credentials', type=Path)
    parser.add_argument('--control-socket', type=Path, default=Path('none'))
    args = parser.parse_args(); os.umask(0o077)
    if args.action == 'freeze':
        require(all((args.runtime, args.baseline_package, args.expires_at, args.previous_permit_sha256)),
                'freeze_requires_baseline_and_explicit_renewal')
    if args.action in ('check', 'execute'):
        require(args.credentials is not None, 'credentials_required')
    print(json.dumps(freeze(args) if args.action == 'freeze' else remote(args)))
