"""Detached quote-source delivery controller. Never installs a quote permit.

The caller supplies a frozen, successful production preparation receipt and the
exact preparation controller. A fresh backup precedes downtime. A failed switch
restores the original compose chain, without rolling back business data.
"""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import types

ROOT = Path('/home/ubuntu/k-tradingagents-paper-supply-20261102')
BACKEND = 'ta-live-088a60b962bf-backend-1'
JOBS = ['akshare_quotes_sync','quotes_ingestion_service','etf_sync',
        'tushare_realtime_quotes_hourly','tushare_historical_sync','akshare_historical_sync']
HISTORY = {'tushare_historical_sync','akshare_historical_sync'}
MIN_HISTORY_WINDOW_SECONDS = 1800  # Includes the possible rollback window.


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':')).encode()


def save(name, value):
    path=ATTEMPT/name
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    raw=value if isinstance(value,bytes) else json.dumps(value,indent=2).encode()
    tmp=path.with_name(path.name+'.tmp')
    fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    tmp.replace(path)


def event(stage):
    STATE['stage']=stage
    STATE['events'].append(dict(stage=stage,at=datetime.now(timezone.utc).isoformat()))
    save('state.json',STATE)
    print(stage,flush=True)


def run(argv,data=None,timeout=60):
    result=subprocess.run(argv,input=data,text=True,capture_output=True,timeout=timeout)
    if result.returncode:
        save('last-command-failure.json',dict(command=argv[:2],returncode=result.returncode,
             stdout=result.stdout,stderr=result.stderr))
        raise RuntimeError('command_failed_details_saved_privately')
    return result.stdout


def inspect():
    return json.loads(run(['docker','inspect',BACKEND]))[0]


def chain(new):
    return PAYLOAD['preflight']['original_chain']+(
        ['-f',str(ROOT/'staged/compose.paper-supply.json')] if new else [])


def config(new):
    return json.loads(run(chain(new)+['config','--format','json']))


def expected_modules(new):
    result=dict(REQUEST['modules'])
    if new:result.update({target:item['sha256'] for target,item in REQUEST['files'].items()})
    return result


def snapshot(new,backup=False):
    request=dict(runtime=REQUEST['runtime'],entry_hash=HELPER.ENTRY_HASH,
                 modules=expected_modules(new),backup=backup)
    raw=run(['docker','exec','-i',BACKEND,'python3','-B','-c',HELPER.BACKEND_SNAPSHOT],json.dumps(request))
    rows=[line.split(':',1)[1] for line in raw.splitlines() if line.startswith('PAPER_SNAPSHOT:')]
    require(len(rows)==1,'snapshot_invalid')
    result=json.loads(rows[0])
    backups=result.pop('backups')
    if backup:
        for name,encoded in backups.items():
            require(name in result['collections'],'unexpected_backup_collection')
            content=base64.b64decode(encoded,validate=True)
            require(digest(content)==result['collections'][name]['sha256'],'backup_hash_mismatch')
            save('backup/database/'+name+'.bson',content)
        require(set(backups)==set(result['collections']),'backup_collection_missing')
        for path,encoded in result['runtime_files'].items():
            name=Path(path).relative_to('/app/runtime')
            require('..' not in name.parts,'unsafe_runtime_backup')
            save('backup/runtime/'+str(name),base64.b64decode(encoded,validate=True))
    return result


def protected(snapshot_value):
    return {key:value for key,value in snapshot_value.items() if key!='modules'}


def seal_check(info):
    mounts={m['Destination']:m['Source'] for m in info['Mounts'] if m['Type']=='bind'}
    for name,expected in REQUEST['seals'].items():
        source=mounts.get('/run/secrets/'+name)
        require(source is not None and digest(Path(source).read_bytes())==expected,'seal_changed')


def check_running(new):
    cfg=config(new);info=inspect()
    expected=PAYLOAD['preflight']
    require(digest(canonical(cfg))==expected['config_hashes']['after' if new else 'before'],'config_changed')
    require(info['Image']==REQUEST['image'] and len(info['Mounts'])==(44 if new else 43),'running_release_changed')
    require(info['State']['Running'] and info['State'].get('Health',{}).get('Status')=='healthy','backend_unhealthy')
    image=json.loads(run(['docker','image','inspect',cfg['services']['backend']['image']]))
    require(not VERIFY.compare_running('backend',info,image,cfg),'running_config_changed')
    seal_check(info)
    return info


def check_jobs(report,now=None):
    require(report.get('success') is True,'jobs_query_failed')
    jobs={job['id']:job for job in report['data']}
    require(set(jobs)==set(JOBS)-set(report['missing_jobs']),'jobs_set_invalid')
    require(set(report['missing_jobs'])<={'quotes_ingestion_service'},'required_job_missing')
    now=now or datetime.now(timezone.utc)
    for name,job in jobs.items():
        require(job.get('has_running_execution') is False,'job_active')
        if name in HISTORY:
            require(job.get('paused') is False and job.get('next_run_time'),'history_schedule_missing')
            due=datetime.fromisoformat(job['next_run_time'].replace('Z','+00:00'))
            require(due.tzinfo is not None and (due-now).total_seconds()>MIN_HISTORY_WINDOW_SECONDS,'history_due_within_switch_window')
        else:
            require(job.get('paused') is True and 'next_run_time' in job and job['next_run_time'] is None,
                    'quote_job_not_paused')


def api_probe(name):
    # The normal login may write session/audit records; credentials and tokens
    # are neither persisted by this controller nor emitted to process arguments.
    output=run(['docker','exec','-i',BACKEND,'python3','-B','-c',PAYLOAD['query_code']],
               json.dumps(dict(auth=PAYLOAD['auth'],jobs=JOBS)))
    report=json.loads(output);check_jobs(report);save(name,report)
    return report


def staged_check():
    stage=ROOT/'staged'
    require(digest((stage/'SHA256SUMS').read_bytes())==PAYLOAD['preflight']['checksums_sha256'],'staged_checksums_changed')
    names={'SHA256SUMS'}
    for line in (stage/'SHA256SUMS').read_text().splitlines():
        expected,name=line.split('  ',1);relative=Path(name)
        require(not relative.is_absolute() and '..' not in relative.parts and name not in names,'unsafe_staged_manifest')
        names.add(name);path=stage/name
        require(not path.is_symlink() and digest(path.read_bytes())==expected,'staged_file_changed')
        require(path.stat().st_mode&0o777==0o644,'staged_mode_changed')
    require({str(p.relative_to(stage)) for p in stage.rglob('*') if p.is_file()}==names,'staged_file_set_changed')


def original_check():
    # The rollback chain uses retained original files, never reverse-copying
    # sources into live bind mounts or restoring database contents.
    backup=ROOT/'backup'
    complete=json.loads((backup/'complete.json').read_text())
    for name,expected in complete['files'].items():
        relative=Path(name)
        require(not relative.is_absolute() and '..' not in relative.parts,'unsafe_backup_manifest')
        require(digest((backup/name).read_bytes())==expected,'original_backup_changed')
    paths=[argv for i,argv in enumerate(chain(False)) if i and chain(False)[i-1]=='-f']
    for index,path in enumerate(paths):
        require(Path(path).read_bytes()==(backup/'compose'/(str(index)+'.source')).read_bytes(),'original_compose_changed')
    for target,source in PAYLOAD['preflight']['old_sources'].items():
        require(digest(Path(source).read_bytes())==REQUEST['modules'][target],'original_source_changed')


def preflight():
    fresh=PAYLOAD['preflight']
    require(fresh['ok'] and fresh['stage']=='prepared_no_switch','successful_preparation_required')
    require(digest(canonical(json.loads((ROOT/'preflight.json').read_text())))==digest(canonical(fresh)),'preparation_receipt_changed')
    require(canonical(json.loads((ROOT/'request.json').read_text()))==canonical(REQUEST),'preparation_request_changed')
    staged_check();original_check()
    info=check_running(False)
    require(info['Id']==fresh['backend_id'],'baseline_container_changed')
    labels=info['Config']['Labels']
    require(labels['com.docker.compose.project.config_files'].split(',')==[
        arg for i,arg in enumerate(chain(False)) if i and chain(False)[i-1]=='-f'],'compose_chain_changed')
    HELPER.compose_change(config(False),config(True),REQUEST['override'],VERIFY)
    before=snapshot(False)
    api_probe('jobs-before.json')
    output=run(HELPER.native_import_argv(info,
        {target:str(ROOT/'staged'/item['path']) for target,item in REQUEST['files'].items()},
        {target:item['sha256'] for target,item in REQUEST['files'].items()}),timeout=65)
    rows=[line.split(':',1)[1] for line in output.splitlines() if line.startswith('PAPER_IMPORT_OK:')]
    require(len(rows)==1 and json.loads(rows[0])==dict(uid=1000,full_app_import=True,permit_absent=True),'isolated_import_failed')
    current=snapshot(False);require(before==current,'production_changed_during_switch_preflight')
    backed=snapshot(False,backup=True);require(current==backed,'production_changed_during_backup')
    save('snapshot-before.json',backed)
    save('backup/complete.json',dict(files={str(p.relative_to(ATTEMPT/'backup')):digest(p.read_bytes())
         for p in (ATTEMPT/'backup').rglob('*') if p.is_file()},original_chain=chain(False),
         inherited_backup_manifest_sha256=digest((ROOT/'backup/complete.json').read_bytes())))
    # A long import/backup cannot consume the protected scheduler window.
    api_probe('jobs-ready.json')
    return backed


def stop_backend():
    project=chain(False)[chain(False).index('--project-name')+1]
    ids=run(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project,
             '--filter','label=com.docker.compose.service=backend']).split()
    # Compose may already have removed the failed replacement. An empty set
    # means there is nothing left to stop; it must not prevent rollback.
    for ident in ids:
        info=json.loads(run(['docker','inspect',ident]))[0]
        if info['State']['Running']:run(['docker','stop','--time','30',ident],timeout=50)
        stopped=json.loads(run(['docker','inspect',ident]))[0]
        require(not stopped['State']['Running'] and stopped['State']['Pid']==0,'backend_not_stopped')


def up(new):
    run(chain(new)+['up','-d','--no-deps','--force-recreate','--pull','never','backend'],timeout=150)


def healthy():
    for _ in range(36):
        info=inspect();require(info['State']['Running'],'backend_not_running')
        if info['State'].get('Health',{}).get('Status')=='healthy':return
        time.sleep(5)
    raise RuntimeError('backend_health_timeout')


def rollback(before):
    event('rollback_stopping_backend');stop_backend()
    original_check();seal_check(json.loads((ROOT/'backup/backend-inspect.json').read_text()))
    require(digest(canonical(config(False)))==PAYLOAD['preflight']['config_hashes']['before'],'rollback_config_changed')
    event('rollback_recreating_original_backend');up(False);healthy();check_running(False)
    after=snapshot(False);save('snapshot-rollback.json',after)
    require(protected(after)==protected(before),'protected_state_changed_preserve_for_investigation')
    api_probe('jobs-rollback.json');event('rolled_back_quote_delivery')


def main():
    attempted=False;before=None
    try:
        event('preflight_and_backup');before=preflight()
        if PAYLOAD['check_only']:
            event('preflight_passed_no_switch');return 0
        event('stopping_backend');attempted=True;stop_backend()
        event('recreating_backend_with_quote_source');up(True);healthy();check_running(True)
        after=snapshot(True);save('snapshot-after.json',after)
        require(protected(after)==protected(before),'protected_state_changed')
        api_probe('jobs-after.json')
        save('completion.json',dict(status='deployed_paper_permit_absent',backend_mounts=44,
             source_head=REQUEST['manifest']['source_head'],business_data_unchanged=True,
             history_plan_unchanged=True,quote_permit_installed=False))
        event('complete');return 0
    except BaseException as error:
        STATE['failure_type']=type(error).__name__
        STATE['failure_code']=str(error) if type(error) is RuntimeError else 'details_withheld'
        event('failed')
        if attempted:
            try:rollback(before)
            except BaseException as rollback_error:
                STATE['rollback_failure_type']=type(rollback_error).__name__
                STATE['rollback_failure_code']=str(rollback_error) if type(rollback_error) is RuntimeError else 'details_withheld'
                try:stop_backend()
                except BaseException:pass
                event('rollback_failed_stop_requested')
        return 1


if __name__=='__main__':
    os.umask(0o077);PAYLOAD=json.load(sys.stdin)
    ATTEMPT=ROOT/'attempts'/PAYLOAD['attempt']
    require(ATTEMPT.parent==ROOT/'attempts' and ATTEMPT.is_dir(),'invalid_attempt')
    lock=(ROOT/'switch.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    require(digest(PAYLOAD['prepare_code'].encode())==PAYLOAD['request']['controller_sha256'],'prepare_controller_changed')
    HELPER=types.ModuleType('paper_prepare');HELPER.__file__=str(ROOT/'deploy/prepare.py')
    exec(compile(PAYLOAD['prepare_code'],HELPER.__file__,'exec'),HELPER.__dict__)
    REQUEST=PAYLOAD['request'];VERIFY=types.ModuleType('compose_verify');exec(REQUEST['verify'],VERIFY.__dict__)
    STATE=dict(stage='initializing',events=[])
    raise SystemExit(main())
