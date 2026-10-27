#!/usr/bin/env python3
"""启动已测试的 v12 切换流程；需要显式 --action check 或 execute。

执行进程独立于 SSH 会话；凭据只经 stdin，进度/回滚状态保存在生产受限目录。
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]
REMOTE_ROOT = '/home/ubuntu/k-tradingagents-paper-guard-v12'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['check','execute'], required=True)
    parser.add_argument('--credentials-file', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, default=Path('/home/ubuntu/quote-guard-releases/production-20261028'))
    parser.add_argument('--release', type=Path, default=Path('/home/ubuntu/quote-guard-releases/v12-20261028'))
    parser.add_argument('--ssh-config', default='/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config')
    parser.add_argument('--host', default='osaka-migration')
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('guard_auth',ROOT/'deploy/query_quote_guard_jobs.py')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    baseline = read_json(args.evidence/'baseline-hashes.json')
    inspect = read_json(args.evidence/'backend-inspect.json')[0]
    mounts = {m['Destination']:m['Source'] for m in inspect['Mounts']}
    names = ['runtime-config.json','launch-context.json','activation-approval.json']
    script = (ROOT/'deploy/switch_quote_guard_v12.py').read_text()
    payload = {
        'auth': helper.read_auth(args.credentials_file, credentials=True),
        'api_probe': helper.REMOTE,
        'db_probe': (args.evidence/'db_guard_probe.js').read_text(),
        'check_only': args.action == 'check',
        'checksums_sha256': digest((args.release/'SHA256SUMS').read_bytes()),
        'seal_hashes': baseline['seals'],
        'seal_paths': {n:'/run/secrets/'+n for n in names},
        'seal_host_paths': {n:mounts['/run/secrets/'+n] for n in names},
        'image_ids': {s:read_json(args.evidence/(s+'-inspect.json'))[0]['Image'] for s in ['backend','frontend']},
        'config_hashes': {},
    }
    for service in ['backend','frontend']:
        payload['config_hashes'][service] = {
            key:digest(json.dumps(read_json(args.evidence/(service+suffix)),sort_keys=True,separators=(',',':')).encode())
            for key,suffix in [('before','-config.json'),('after','-v12-config.json')]
        }
    launcher = r'''
import datetime,hashlib,json,os,pathlib,subprocess,sys,uuid
request=json.load(sys.stdin)
root=pathlib.Path('/home/ubuntu/k-tradingagents-paper-guard-v12')
data=request['code'].encode()
if hashlib.sha256(data).hexdigest()!=request['code_sha256']:raise RuntimeError('script_hash_invalid')
script=root/('switch_quote_guard_v12-'+request['code_sha256']+'.py')
if script.exists():
    if script.read_bytes()!=data:raise RuntimeError('existing_switch_script_differs')
else:
    fd=os.open(script,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as f:f.write(data)
pidfile=root/'switch-process.json'
if pidfile.exists():
    prior=json.loads(pidfile.read_text())['pid']
    cmdline=pathlib.Path('/proc')/str(prior)/'cmdline'
    if cmdline.exists() and str(root/'switch_quote_guard_v12').encode() in cmdline.read_bytes():raise RuntimeError('switch_already_active')
    # Preserve every previous attempt before reusing the current-state filenames.
    archive=root/'attempts'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8])
    archive.mkdir(parents=True,mode=0o700)
    for name in ['switch-state.json','switch-process.json','switch-process.log','last-command-failure.json',
                 'runtime-before-switch.json','db-before-stop.json','jobs-immediate-before.json',
                 'db-stopped-baseline.json','runtime-module-hashes.json','installed-smoke.json',
                 'jobs-after-backend.json','frontend-http.json','db-after-switch.json','completion.json',
                 'jobs-after-rollback.json']:
        source=root/name
        if source.exists():source.rename(archive/name)
logfd=os.open(root/'switch-process.log',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
with os.fdopen(logfd,'ab') as log:
    child=subprocess.Popen(['python3','-B',str(script)],stdin=subprocess.PIPE,stdout=log,stderr=log,start_new_session=True)
    child.stdin.write(json.dumps(request['payload']).encode())
    child.stdin.close()
report={'pid':child.pid,'check_only':request['payload']['check_only'],'script_sha256':request['code_sha256'],'script_path':str(script)}
fd=os.open(pidfile,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:json.dump(report,f)
print(json.dumps(report))
'''
    request = {'code':script,'code_sha256':digest(script.encode()),'payload':payload}
    result = subprocess.run(['ssh','-F',args.ssh_config,'-o','BatchMode=yes','-o','ConnectTimeout=10',args.host,
                             'sudo -n python3 -c '+shlex.quote(launcher)],
                            input=json.dumps(request),text=True,capture_output=True,timeout=60)
    if result.returncode:
        print('SWITCH_LAUNCH_FAILED: no credentials or raw remote errors displayed; inspect restricted process state before retry')
        return 1
    print(json.dumps(json.loads(result.stdout)))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
