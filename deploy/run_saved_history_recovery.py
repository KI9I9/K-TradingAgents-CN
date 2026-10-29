"""Run the reviewed saved-response recovery once; no credentials or upstream API."""
import argparse,json,os,tempfile,hashlib
from pathlib import Path
from run_history_sample import ssh,EVIDENCE
from run_history_button_switch import upload
ROOT=Path(__file__).resolve().parents[1]
REMOTE_ROOT='/home/ubuntu/k-tradingagents-history-saved-recovery-20261029-v1'
DRIVER=r'''
import json,pathlib,subprocess,os
os.umask(0o077)
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-saved-recovery-20261029-v1')
p=json.loads((root/'package.json').read_text())
def save(name,value):
    temp=root/(name+'.tmp')
    with temp.open('w') as f:json.dump(value,f,indent=2);f.flush();os.fsync(f.fileno())
    temp.replace(root/name)
save('status.json',{'stage':'running'})
try:
    r=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',p['code']],
        input=json.dumps(p['request']),text=True,capture_output=True,timeout=120)
    (root/'stdout.private').write_text(r.stdout);(root/'stderr.private').write_text(r.stderr)
    lines=[l.removeprefix('RECOVERY_JSON:') for l in r.stdout.splitlines() if l.startswith('RECOVERY_JSON:')]
    if len(lines)!=1:raise RuntimeError('result_missing')
    report=json.loads(lines[0]);save('result.json',report)
    save('status.json',{'stage':'complete' if r.returncode==0 and report.get('status')=='passed' else 'failed','reason':report.get('reason')})
except BaseException as e:
    save('status.json',{'stage':'unknown_outcome','error_type':type(e).__name__,'automatic_retry':False})
'''
LAUNCH=r'''
import json,os,pathlib,hashlib,subprocess,sys
os.umask(0o077)
r=json.load(sys.stdin);raw=pathlib.Path('/home/ubuntu/saved-recovery-'+r['sha256']+'.json').read_bytes()
if hashlib.sha256(raw).hexdigest()!=r['sha256']:raise RuntimeError('upload_changed')
p=json.loads(raw)
switch=pathlib.Path('/home/ubuntu/k-tradingagents-history-proxy-v1-20261029')
latest=json.loads((switch/'latest-attempt.json').read_text())
result=json.loads((switch/'attempts'/latest['attempt']/'completion.json').read_text())
if result['status']!='deployed_proxy_fix_gate_stopped':raise RuntimeError('proxy_fix_not_verified')
if p['request']['mode']=='check':
    proc=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',p['code']],input=json.dumps(p['request']),text=True,capture_output=True,timeout=50)
    lines=[l.removeprefix('RECOVERY_JSON:') for l in proc.stdout.splitlines() if l.startswith('RECOVERY_JSON:')]
    if len(lines)!=1:raise RuntimeError('preflight_output_missing')
    print(lines[0])
else:
    root=pathlib.Path('/home/ubuntu/k-tradingagents-history-saved-recovery-20261029-v1')
    root.mkdir(mode=0o700)  # irrevocable one-run reservation, never auto retry
    (root/'package.json').write_bytes(raw);(root/'driver.py').write_text(p['driver'])
    with (root/'process.log').open('ab') as log:
        child=subprocess.Popen(['python3','-B',str(root/'driver.py')],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    out={'pid':child.pid,'package_sha256':r['sha256']}
    (root/'process.json').write_text(json.dumps(out));print(json.dumps(out))
'''
STATUS=r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-saved-recovery-20261029-v1')
out={'reserved':root.exists()}
for n in ('process.json','status.json','result.json'):
 if (root/n).exists():out[n]=json.loads((root/n).read_text())
print(json.dumps(out))
'''


def package(mode):
    deployed=json.loads((EVIDENCE/'history-button-production-20261029T034911Z-e5c43f74.json').read_text())
    hashes=deployed['evidence']['installed-module-hashes.json'].copy()
    manifest=json.loads(Path('/home/ubuntu/quote-guard-releases/history-proxy-v1-20261029/manifest.json').read_text())
    hashes.update({e['target']:e['sha256'] for e in manifest['files']})
    protected=json.loads((EVIDENCE/'history-write-package-v1.json').read_text())['request']['protected_hashes']
    protected.update(json.loads((EVIDENCE/'history-proxy-preflight.json').read_text())['batch_protected'])
    return dict(code=(ROOT/'deploy/recover_saved_history.py').read_text(),driver=DRIVER,
        request=dict(mode=mode,approved=mode=='execute',source_hashes=hashes,protected_hashes=protected,
            runtime_tool=(ROOT/'deploy/quote_guard_runtime.py').read_text()))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--action',choices=['check','execute','status','collect'],required=True)
    args=parser.parse_args()
    if args.action in ('status','collect'):
        report=ssh(STATUS)
        if args.action=='collect':
            path=EVIDENCE/'history-saved-recovery-result-20261029.json'
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2)
    else:
        if args.action=='execute':
            check=json.loads((EVIDENCE/'history-saved-recovery-check.json').read_text())
            if check.get('status')!='preflight_passed':raise SystemExit('preflight_required')
        raw=json.dumps(package(args.action),sort_keys=True).encode();digest=hashlib.sha256(raw).hexdigest()
        artifact=EVIDENCE/('saved-recovery-package-'+digest+'.json')
        if not artifact.exists():
            fd=os.open(artifact,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'wb') as f:f.write(raw)
        upload(artifact,'/home/ubuntu/saved-recovery-'+digest+'.json')
        report=ssh(LAUNCH,{'sha256':digest})
        if args.action=='check':
            path=EVIDENCE/'history-saved-recovery-check.json'
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2)
    print(json.dumps(report,ensure_ascii=False))
