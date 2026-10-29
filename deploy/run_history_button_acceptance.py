"""Detached single real-button acceptance. Credentials via stdin, never disk/argv."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

from run_history_sample import ssh, EVIDENCE
from run_history_button_switch import upload, RELEASE, ARCHIVE_SHA
from query_quote_guard_jobs import read_auth

REPO=Path(__file__).resolve().parents[1]
ROOT='/home/ubuntu/k-tradingagents-history-button-acceptance-20261029-v1'

DRIVER=r'''
import json,os,pathlib,subprocess,sys,time
os.umask(0o077)
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-acceptance-20261029-v1')
p=json.loads((root/'package.json').read_text())
p['request']['auth']=json.load(sys.stdin)['auth']
def save(name,value):
    tmp=root/(name+'.tmp')
    with tmp.open('w') as f:json.dump(value,f,indent=2);f.flush();os.fsync(f.fileno())
    tmp.replace(root/name)
save('status.json',{'stage':'running','started_at':time.time()})
try:
    result=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',p['code']],
        input=json.dumps(p['request']),capture_output=True,text=True,timeout=260)
    if result.stderr:(root/'stderr.private').write_text(result.stderr)
    (root/'stdout.private').write_text(result.stdout)
    reports=[line.removeprefix('BUTTON_ACCEPTANCE_JSON:') for line in result.stdout.splitlines() if line.startswith('BUTTON_ACCEPTANCE_JSON:')]
    if len(reports)!=1:raise RuntimeError('missing_or_duplicate_result')
    report=json.loads(reports[0])
    save('result.json',report)
    save('status.json',{'stage':'complete' if result.returncode==0 and report.get('status')=='passed' else 'failed',
        'exit_code':result.returncode,'reason':report.get('reason'),'new_gate_stopped':report.get('new_gate_stopped'),
        'used_requests':report.get('used_requests')})
except BaseException as error:
    save('status.json',{'stage':'failed_unknown_outcome','error_type':type(error).__name__,'automatic_retry':False})
finally:
    # Separate stop attempt on any outcome, including lost docker-exec output.
    code="from pathlib import Path; from tradingagents.utils.tushare_batch import BatchGate; p=Path('/app/runtime/tushare-history-batch-v1'); BatchGate(p).stop() if p.is_dir() else None; print(p.is_dir() and (p/'STOP').is_file())"
    try:
        r=subprocess.run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',code],capture_output=True,text=True,timeout=15)
        save('stop-confirmation.json',{'stopped':r.returncode==0 and r.stdout.splitlines()[-1:]==['True']})
    except BaseException:save('stop-confirmation.json',{'stopped':False})
'''

LAUNCH=r'''
import hashlib,json,os,pathlib,subprocess,sys
os.umask(0o077)
r=json.load(sys.stdin)
raw=pathlib.Path('/home/ubuntu/history-button-acceptance-upload-'+r['sha256']+'.json').read_bytes()
if hashlib.sha256(raw).hexdigest()!=r['sha256']:raise RuntimeError('upload_changed')
p=json.loads(raw)
switch=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-v1-20261029')
latest=json.loads((switch/'latest-attempt.json').read_text())
completion=json.loads((switch/'attempts'/latest['attempt']/'completion.json').read_text())
if completion['status']!='deployed_button_gate_absent':raise RuntimeError('deployment_not_verified')
if completion['source_commit']!=p['source_head']:raise RuntimeError('deployment_source_changed')
listeners=subprocess.check_output(['ss','-H','-ltn','sport','=',':32879'],text=True)
if '127.0.0.1:32879' not in listeners:raise RuntimeError('forward_listener_missing')
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-acceptance-20261029-v1')
root.mkdir(mode=0o700)  # permanent reservation; never rerun an ambiguous launch
(root/'package.json').write_bytes(raw)
(root/'driver.py').write_text(p['driver'])
with (root/'process.log').open('ab') as log:
    child=subprocess.Popen(['python3','-B',str(root/'driver.py')],stdin=subprocess.PIPE,stdout=log,stderr=log,start_new_session=True)
    child.stdin.write(json.dumps({'auth':r['auth']}).encode());child.stdin.close()
report={'pid':child.pid,'package_sha256':r['sha256'],'started':True}
(root/'process.json').write_text(json.dumps(report))
print(json.dumps(report))
'''

STATUS=r'''
import json,pathlib
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-acceptance-20261029-v1')
out={'reserved':root.exists()}
for name in ['process.json','status.json','result.json','stop-confirmation.json']:
    if (root/name).exists():out[name]=json.loads((root/name).read_text())
print(json.dumps(out))
'''


def package():
    manifest=json.loads((RELEASE/'manifest.json').read_text())
    baseline=json.loads((EVIDENCE/'history-ingress-production-20261028T165211Z-50f64b00.json').read_text())
    hashes=baseline['evidence']['installed-module-hashes.json'].copy()
    hashes.update({e['target']:e['sha256'] for e in manifest['files']})
    return dict(source_head=manifest['source_head'],archive_sha256=ARCHIVE_SHA,driver=DRIVER,
        code=(REPO/'deploy/history_button_acceptance.py').read_text(),
        request=dict(source_hashes=hashes,
            protected_hashes=json.loads((EVIDENCE/'history-write-package-v1.json').read_text())['request']['protected_hashes'],
            runtime_tool=(REPO/'deploy/quote_guard_runtime.py').read_text(),
            series_bson_sha256='d2f58cb0006bb0abbe72548779dea8f8fe2a63dc862d5782d4a917ced37b929f',
            other_bson_sha256='811ce8d0cde35f23318b88d76d05d53c320c2e005b45755242d6013a368050e8'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['execute','status','collect'],required=True)
    parser.add_argument('--credentials-file',type=Path)
    args=parser.parse_args()
    if args.action=='execute':
        if args.credentials_file is None:parser.error('credentials file required')
        if subprocess.run(['systemctl','is-active','--quiet','ta-tushare-forward.service']).returncode:
            raise SystemExit('forward_service_not_active')
        if subprocess.check_output(['git','status','--porcelain'],cwd=REPO):
            raise SystemExit('commit controller before acceptance')
        if hashlib.sha256(RELEASE.with_suffix('.tgz').read_bytes()).hexdigest()!=ARCHIVE_SHA:
            raise SystemExit('application_archive_changed')
        raw=json.dumps(package(),sort_keys=True).encode();sha=hashlib.sha256(raw).hexdigest()
        artifact=EVIDENCE/('history-button-acceptance-package-'+sha+'.json')
        fd=os.open(artifact,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as f:f.write(raw)
        upload(artifact,'/home/ubuntu/history-button-acceptance-upload-'+sha+'.json')
        result=ssh(LAUNCH,dict(sha256=sha,auth=read_auth(args.credentials_file,True)))
    else:
        result=ssh(STATUS)
        if args.action=='collect':
            target=EVIDENCE/'history-button-real-acceptance-20261029.json'
            fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as f:json.dump(result,f,indent=2)
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
