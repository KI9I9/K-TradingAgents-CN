"""Read-only, detached recovery audit; no deployment, permits or market requests.

The report compares live BSON with the retained pre-switch backup, executes the
real preference guard on both versions and reads the normal scheduler API.
Credentials remain in stdin/memory. An ambiguous launch is recovered by status.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import zlib

from prepare_paper_quote_supply import EVIDENCE, ROOT, canonical, digest
from query_quote_guard_jobs import read_auth
from switch_paper_quote_supply import PREFERENCES_GUARD

DIRECTORY = ROOT/'diagnostics/recovery-audit-v1'
BASE_SHA = '98afbbbf131ab8b23842bc081ccd079aeb95ca267c4d733903f6d5ac810782c7'

TAIL = r'''
import io
from contextlib import redirect_stdout
from bson import BSON, decode_all
old_raw=base64.b64decode(request['old_preferences'])
assert hashlib.sha256(old_raw).hexdigest()==request['before_preferences']['sha256'], 'old_preference_backup_changed'
new_raw=bytes(raw)
old={str(row['_id']):row for row in decode_all(old_raw)}
new={str(row['_id']):row for row in decode_all(new_raw)}
changes=[]
for job in sorted(set(old)|set(new)):
    a=old.get(job,{});b=new.get(job,{})
    keys=sorted(k for k in set(a)|set(b) if k not in a or k not in b
                or BSON.encode({'v':a[k]})!=BSON.encode({'v':b[k]}))
    if keys:changes.append(dict(job_id=job,changed_fields=keys,
        before_enabled=a.get('enabled'),after_enabled=b.get('enabled')))
guards={}
for label,raw,preference in [('before',old_raw,request['before_preferences']),
                             ('after',new_raw,collections['scheduler_preferences'])]:
    collections['scheduler_preferences']=preference
    output=io.StringIO()
    with redirect_stdout(output):exec(request['guard_code'],globals())
    rows=[s.split(':',1)[1] for s in output.getvalue().splitlines() if s.startswith('PAPER_PREF_GUARD:')]
    assert len(rows)==1,'guard_result_invalid'
    guards[label]=json.loads(rows[0])
print('PREF_RECOVERY:'+json.dumps(dict(before_count=len(old),after_count=len(new),
    changes=changes,guards=guards)))
'''

CHILD = r'''
import base64,hashlib,json,os,pathlib,subprocess,sys,types
os.umask(0o077)
p=json.load(sys.stdin);root=pathlib.Path(p['root']);directory=pathlib.Path(p['directory'])
def save(name,value):
    path=directory/name;tmp=path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value,indent=2));tmp.replace(path)
def event(stage):save('state.json',dict(stage=stage))
def run(argv,data=None,timeout=60):
    result=subprocess.run(argv,input=json.dumps(data) if data is not None else None,
        text=True,capture_output=True,timeout=timeout)
    if result.returncode:
        save('private-command-failure.json',dict(stdout=result.stdout,stderr=result.stderr))
        raise RuntimeError('read_command_failed_details_private')
    return result.stdout
try:
    event('checking_original_backend')
    raw=(root.parent/('history-button-switch-upload-'+p['base_sha']+'.json')).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==p['base_sha'],'frozen_original_changed'
    frozen=json.loads(raw);request=frozen['payload']['request']
    attempt=root/'attempts/20261102T070244Z-97c5cc40'
    recovery=json.loads((attempt/'recovery-original.json').read_text())
    before=json.loads((attempt/'snapshot-before.json').read_text())
    info=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
    assert info['Id']==recovery['id'] and info['Image']==request['image'] and len(info['Mounts'])==43,'original_container_changed'
    assert info['State']['Running'] and info['State'].get('Health',{}).get('Status')=='healthy','original_backend_unhealthy'
    helper=types.ModuleType('prepare');helper.__file__=str(root/'deploy/prepare.py')
    exec(frozen['payload']['prepare_code'],helper.__dict__)
    event('comparing_raw_preferences_and_protected_state')
    data=dict(runtime=request['runtime'],entry_hash=helper.ENTRY_HASH,modules=request['modules'],backup=False,
        old_preferences=base64.b64encode((attempt/'backup/database/scheduler_preferences.bson').read_bytes()).decode(),
        before_preferences=before['collections']['scheduler_preferences'],guard_code=p['guard_code'])
    output=run(['docker','exec','-i',info['Id'],'python3','-B','-c',helper.BACKEND_SNAPSHOT+p['tail']],data)
    def marked(prefix):
        rows=[s.split(':',1)[1] for s in output.splitlines() if s.startswith(prefix+':')]
        assert len(rows)==1,'snapshot_result_invalid'
        return json.loads(rows[0])
    snapshot=marked('PAPER_SNAPSHOT');snapshot.pop('backups');fields=marked('PREF_RECOVERY')
    def protected(value):
        result=json.loads(json.dumps(value));del result['collections']['scheduler_preferences']['sha256']
        return result
    assert protected(before)==protected(snapshot),'other_protected_state_changed'
    assert fields['guards']['before']==fields['guards']['after'],'preference_semantics_changed'
    expected={'tushare_basic_info_sync','tushare_financial_sync','tushare_status_check','tushare_realtime_quotes_hourly'}
    assert fields['before_count']==fields['after_count'] and len(fields['changes'])==4,'preference_set_changed'
    assert {c['job_id'] for c in fields['changes']}==expected,'unexpected_preference_jobs'
    assert all(c['changed_fields']==['updated_at'] and c['before_enabled'] is False
               and c['after_enabled'] is False for c in fields['changes']),'unexpected_preference_fields'
    event('reading_scheduler_api')
    jobs=['akshare_quotes_sync','quotes_ingestion_service','etf_sync','tushare_realtime_quotes_hourly',
          'tushare_historical_sync','akshare_historical_sync']
    report=json.loads(run(['docker','exec','-i',info['Id'],'python3','-B','-c',frozen['payload']['query_code']],
        dict(auth=p['auth'],jobs=jobs),timeout=40))
    control=types.ModuleType('control');exec(frozen['code'],control.__dict__);control.check_jobs(report)
    fields.update(protected_state_unchanged=True,jobs=report)
    save('snapshot.json',snapshot);save('preference-diff.json',fields)
    # This evidence file is required by the v2 preflight; no runtime configuration changes.
    proof=attempt/'recovery-preference-diff.json'
    if proof.exists():assert json.loads(proof.read_text())==fields,'previous_recovery_evidence_differs'
    else:proof.write_text(json.dumps(fields))
    save('completion.json',dict(ok=True,status='original_recovery_audited',
        backend=dict(id=info['Id'],health='healthy',mounts=43),preference_diff=fields,
        used=snapshot['used'],history_counts=snapshot['history_counts'],quote_permit_absent=True))
    event('complete')
except BaseException as error:
    save('completion.json',dict(ok=False,status='recovery_audit_failed',error_type=type(error).__name__,
        reason=str(error) if type(error) in (AssertionError,RuntimeError) else 'details_private'))
    event('failed');sys.exit(1)
'''

STATUS = r'''
import json,pathlib,subprocess,sys
p=json.load(sys.stdin);d=pathlib.Path(p['directory']);out={'reserved':d.exists()}
for name in ['process','state','completion']:
    if (d/(name+'.json')).exists():out[name]=json.loads((d/(name+'.json')).read_text())
c=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],timeout=10))[0]
out['current_backend']={'id':c['Id'],'running':c['State']['Running'],
    'health':c['State'].get('Health',{}).get('Status'),'mounts':len(c['Mounts'])}
print(json.dumps(out))
'''

LAUNCH = r'''
import json,os,pathlib,subprocess,sys
os.umask(0o077);p=json.load(sys.stdin);directory=pathlib.Path(p['directory'])
if directory.exists():
    sys.stdin=__import__('io').StringIO(json.dumps(p));exec(p['status_code']);raise SystemExit(0)
directory.parent.mkdir(mode=0o700,exist_ok=True);directory.mkdir(mode=0o700)
script=directory/'audit.py';script.write_text(p.pop('child_code'));p.pop('status_code')
with (directory/'process.log').open('ab') as log:
    child=subprocess.Popen(['python3','-B',str(script)],stdin=subprocess.PIPE,stdout=log,stderr=log,start_new_session=True)
    child.stdin.write(json.dumps(p).encode());child.stdin.close()
report={'pid':child.pid,'read_only':True};(directory/'process.json').write_text(json.dumps(report))
print(json.dumps(report))
'''


def remote(action,credentials=None):
    data=dict(root=str(ROOT),directory=str(DIRECTORY))
    if action=='inspect':
        data.update(auth=read_auth(credentials,True),base_sha=BASE_SHA,guard_code=PREFERENCES_GUARD,
                    tail=TAIL,child_code=CHILD,status_code=STATUS)
    bootstrap="import io,json,sys,zlib;v=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(v['data']));exec(v['code'])"
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S','none',
        '-o','BatchMode=yes','-o','ConnectTimeout=25','-o','ConnectionAttempts=1','-o','IPQoS=none',
        '-o','ServerAliveInterval=10','-o','ServerAliveCountMax=2','osaka-migration',
        'sudo -n python3 -B -c '+shlex.quote(bootstrap)]
    target=EVIDENCE/('recovery-audit-'+action+'-'+str(time.time_ns())+'.json')
    try:
        result=subprocess.run(cmd,input=zlib.compress(canonical(dict(code=LAUNCH if action=='inspect' else STATUS,data=data))),
                              capture_output=True,timeout=65)
    except subprocess.TimeoutExpired:
        raise SystemExit('recovery_inspection_timeout_query_status') from None
    if result.returncode:
        target.with_suffix('.failure.private').write_text(json.dumps(dict(returncode=result.returncode,
            stdout=result.stdout.decode(errors='replace'),stderr=result.stderr.decode(errors='replace'))))
        raise SystemExit('recovery_inspection_unavailable_query_status')
    value=json.loads(result.stdout);target.write_text(json.dumps(value,indent=2))
    return dict(receipt=str(target),result=value)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['inspect','status'],required=True)
    parser.add_argument('--credentials-file',type=Path)
    args=parser.parse_args();os.umask(0o077)
    if args.action=='inspect' and args.credentials_file is None:parser.error('credentials required')
    print(json.dumps(remote(args.action,args.credentials_file)))


if __name__=='__main__':main()
