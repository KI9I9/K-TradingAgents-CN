"""One-shot ETF history code/static delivery; never grants market/Tushare access.

Run through a frozen, detached controller. Originals, configuration and database
snapshots stay private on production. Rollback restores code and the old index,
never database data. Only the stock-sync bind source and one new ETF bind change.
"""
import base64
import copy
from datetime import datetime, timezone
import fcntl
import hashlib
import inspect as source_inspect
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import types

ROOT = Path('/home/ubuntu/k-tradingagents-etf-history-20261103')
BACKEND = 'ta-live-088a60b962bf-backend-1'
FRONTEND = 'ta-live-088a60b962bf-frontend-1'
NEW_TARGET = '/app/app/worker/etf_sync_service.py'
OLD_TARGET = '/app/app/routers/stock_sync.py'
CLOSED = ['akshare_quotes_sync', 'etf_sync', 'tushare_realtime_quotes_hourly']


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    raw = value if isinstance(value, bytes) else canonical(value)
    with path.with_suffix(path.suffix+'.tmp').open('wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    path.with_suffix(path.suffix+'.tmp').replace(path)


def event(stage):
    STATE.update(stage=stage, at=datetime.now(timezone.utc).isoformat())
    save(ATTEMPT/'state.json', STATE)
    print(stage, flush=True)


def run(argv, data=None, timeout=60):
    result = subprocess.run(argv, input=data, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        save(ATTEMPT/'command-failure.private.json', dict(returncode=result.returncode,
             stdout=result.stdout, stderr=result.stderr))
        raise RuntimeError('command_failed_private_receipt')
    return result.stdout


def info(name=BACKEND):
    return json.loads(run(['docker', 'inspect', name]))[0]


def chain(new=False):
    return REQUEST['chain'] + (['-f', str(ROOT/'staged/compose.etf-history.json')] if new else [])


def config(new=False):
    return json.loads(run(chain(new)+['config', '--format', 'json']))


def config_check(before, after):
    expected = copy.deepcopy(before)
    mounts = VERIFY.mount_map(expected['services']['backend'])
    require(NEW_TARGET not in mounts and mounts[OLD_TARGET]['read_only'] is True, 'unexpected_bind_baseline')
    mounts[OLD_TARGET]['source'] = str(ROOT/'staged/app/routers/stock_sync.py')
    require(not VERIFY.compare(expected, after, {'backend': {
        NEW_TARGET: str(ROOT/'staged/app/worker/etf_sync_service.py')}}), 'compose_scope_drift')


def running(new=False):
    value = info(); cfg = config(new)
    require(value['Image'] == REQUEST['image'] and len(value['Mounts']) == (45 if new else 44), 'backend_identity_drift')
    image = json.loads(run(['docker', 'image', 'inspect', cfg['services']['backend']['image']]))
    require(not VERIFY.compare_running('backend', value, image, cfg), 'running_config_drift')
    require(value['State']['Running'] and value['State'].get('Health', {}).get('Status') == 'healthy', 'backend_unhealthy')
    return value


SNAPSHOT = r'''
import base64,hashlib,json,sys,types,time
from pathlib import Path
from bson import BSON
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
q=json.load(sys.stdin)
rt=types.ModuleType('runtime');exec(q['runtime'],rt.__dict__)
env,effective=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
modules={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in q['modules']}
assert modules==q['modules'],'module_drift'
root=Path('/app/runtime/tushare-history-generations')
active=json.loads((root/'active.json').read_text());generation=root/active['generation']
assert generation.parent==root,'unsafe_generation'
state=json.loads((generation/'state.json').read_text())
assert state['active'] is None and state['phase']=='ready' and not (generation/'STOP').exists(),'history_not_idle'
assert Path('/app/runtime/tushare-egress/STOP').is_file(),'legacy_STOP_missing'
paths=[root/'active.json',Path('/app/runtime/tushare-history-schedule.json'),Path('/app/runtime/tushare-egress/STOP')]
paths += [p for p in generation.rglob('*') if p.is_file() and not p.name.endswith('.lock')]
paper=Path('/app/runtime/paper-quote-supply.json')
if paper.exists():paths.append(paper)
runtime={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
backup=Path(q['backup']) if q.get('backup') else None
if backup:
 backup.mkdir(parents=True,mode=0o700)
 for p in paths:
  dst=backup/'runtime'/p.relative_to('/app/runtime');dst.parent.mkdir(parents=True,exist_ok=True,mode=0o700);dst.write_bytes(p.read_bytes());dst.chmod(0o600)
client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=10000)
try:
 db=client[env['MONGODB_DATABASE']]
 assert db.scheduler_executions.count_documents({'status':{'$in':['running','cancelling']}})==0,'scheduled_work_active'
 assert db.stock_sync_tasks.count_documents({'status':{'$in':['pending','running']}})==0,'manual_work_active'
 collections={}
 ignore={'tushare_basic_info_sync','tushare_financial_sync','tushare_status_check','tushare_realtime_quotes_hourly'}
 for name in ['market_quotes','paper_accounts','paper_positions','paper_orders','paper_trades','paper_market_rules','scheduler_preferences','etf_daily_quotes','etf_basic_info']:
  query={'code':'159865'} if name.startswith('etf_') else {}
  col=db.get_collection(name,codec_options=CodecOptions(document_class=RawBSONDocument))
  raw=bytearray();normalized=bytearray();count=0
  for row in col.find(query).sort('_id',1).max_time_ms(10000):
   count+=1;raw.extend(row.raw);assert len(raw)<=32*1024*1024 and count<=20000,'backup_limit'
   if name=='scheduler_preferences' and row['_id'] in ignore:
    assert row.get('enabled') is False,'closed_job_enabled'
    d=dict(row);d.pop('updated_at',None);normalized.extend(BSON.encode(d))
   else:normalized.extend(row.raw)
  collections[name]=dict(count=count,sha256=hashlib.sha256(normalized).hexdigest(),raw_sha256=hashlib.sha256(raw).hexdigest())
  if backup:
   (backup/(name+'.bson')).write_bytes(raw);(backup/(name+'.bson')).chmod(0o600)
  if name.startswith('etf_') and backup:(backup/(name+'-indexes.json')).write_text(json.dumps(db[name].index_information()))
 history_counts={n:db[n].count_documents({'data_source':'tushare'}) for n in ['stock_daily_quotes','stock_daily_quotes_unadjusted']}
finally:client.close()
print('ETF_SNAPSHOT:'+json.dumps(dict(modules=modules,effective=effective,runtime=runtime,collections=collections,history_counts=history_counts,used=state['used'])))
'''


def snapshot(new=False, backup=False):
    modules = dict(REQUEST['modules'])
    if new:
        modules.update({target: item['sha256'] for target, item in REQUEST['sources'].items()})
    request = dict(runtime=REQUEST['runtime'], modules=modules,
                   backup='/app/runtime/etf-chart-delivery-20261103/'+PAYLOAD['attempt'] if backup else None)
    output = run(['docker', 'exec', '-i', BACKEND, 'python3', '-B', '-c', SNAPSHOT], json.dumps(request))
    rows = [json.loads(line.split(':', 1)[1]) for line in output.splitlines() if line.startswith('ETF_SNAPSHOT:')]
    require(len(rows) == 1, 'snapshot_missing')
    return rows[0]


def protected(value):
    value = copy.deepcopy(value);value.pop('modules')
    for row in value['collections'].values():row.pop('raw_sha256')
    return value


def jobs(label):
    output = run(['docker','exec','-i',BACKEND,'python3','-B','-c',REQUEST['query']],
                 json.dumps(dict(auth=PAYLOAD['auth'], jobs=CLOSED+['tushare_historical_sync','akshare_historical_sync'])))
    result = json.loads(output)
    require(result.get('success') is True and not result.get('missing_jobs'), 'jobs_missing')
    for job in result['data']:
        require(job.get('has_running_execution') is False, 'job_active')
        if job['id'] in CLOSED:require(job.get('paused') is True and job.get('next_run_time') is None, 'quote_job_enabled')
        else:
            require(job.get('paused') is False and job.get('next_run_time'), 'history_schedule_missing')
            due=datetime.fromisoformat(job['next_run_time'].replace('Z','+00:00'))
            require((due-datetime.now(timezone.utc)).total_seconds()>1800,'schedule_due_soon')
    save(ATTEMPT/(label+'.json'), result)


def frontend_root():
    f = info(FRONTEND)
    require(f['Id'] == REQUEST['frontend_id'], 'frontend_identity_changed')
    return Path(next(m['Source'] for m in f['Mounts'] if m['Destination'] == '/usr/share/nginx/html'))


def frontend_probe(checks):
    import ssl
    f=info(FRONTEND)
    cert=Path(next(m['Source'] for m in f['Mounts'] if m['Destination']=='/run/secrets/tls-cert.pem')).read_text()
    require(digest(ssl.PEM_cert_to_DER_cert(cert)) == 'd8245a033b6e0ed360a5b7b64a14b8757b15fcec9a90a69cd0300e0dd2935be4','frontend_cert_drift')
    code=REQUEST['http_probe']+'\nimport json\nprint(json.dumps(verify_frontend_resources('+repr(checks)+','+repr(cert)+',timeout=60)))'
    return json.loads(run(['docker','exec',BACKEND,'python3','-B','-c',code],timeout=70))


def stage():
    ROOT.mkdir(exist_ok=True, mode=0o711);ROOT.chmod(0o711)
    for target, item in REQUEST['sources'].items():
        require(target in (OLD_TARGET,NEW_TARGET), 'unexpected_source')
        raw=item['source'].encode();require(digest(raw)==item['sha256'],'candidate_source_changed')
        path=ROOT/'staged'/target.removeprefix('/app/')
        path.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
        if path.exists():require(path.read_bytes()==raw,'staged_source_changed')
        else:path.write_bytes(raw)
        path.chmod(0o644)
    override=dict(services=dict(backend=dict(volumes=[dict(type='bind',source=str(ROOT/'staged'/target.removeprefix('/app/')),
        target=target,read_only=True,bind={'create_host_path':False}) for target in sorted(REQUEST['sources'])])))
    save(ROOT/'staged/compose.etf-history.json',override)
    for name,item in REQUEST['frontend_delta'].items():
        require(not Path(name).is_absolute() and '..' not in Path(name).parts,'unsafe_frontend_path')
        raw=base64.b64decode(item['base64'],validate=True);require(digest(raw)==item['sha256'],'candidate_asset_changed')
        path=ROOT/'staged/frontend'/name;path.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
        if path.exists():require(path.read_bytes()==raw,'staged_asset_changed')
        else:path.write_bytes(raw)
        path.chmod(0o644)
    # mkdir(parents=True) applies the process umask to intermediate parents.
    # Every staged code ancestor must be traversable by the container uid 1000.
    (ROOT/'staged').chmod(0o755)
    for path in (ROOT/'staged').rglob('*'):
        if path.is_dir():path.chmod(0o755)


def preflight():
    stage();b=running(False)
    require(b['Id']==REQUEST['backend_id'],'backend_changed_since_freeze')
    before=config(False);after=config(True);config_check(before,after)
    save(ATTEMPT/'backup/backend-inspect.private.json',b)
    save(ATTEMPT/'backup/config-before.private.json',before);save(ATTEMPT/'backup/config-after.private.json',after)
    for i,path in enumerate(REQUEST['chain_files']):save(ATTEMPT/('backup/compose/'+str(i)),Path(path).read_bytes())
    front=frontend_root()
    actual={str(p.relative_to(front)):digest(p.read_bytes()) for p in front.rglob('*') if p.is_file()}
    require(actual==REQUEST['frontend_before'],'frontend_files_drift')
    for name in REQUEST['frontend_delta']:
        if name in actual:
            require(name=='index.html','asset_collision')
            save(ATTEMPT/'backup/frontend'/name,(front/name).read_bytes())
    for target in REQUEST['sources']:
        raw=run(['docker','exec',BACKEND,'cat',target]).encode()
        require(digest(raw)==REQUEST['modules'][target],'original_source_changed')
        save(ATTEMPT/'backup/sources'/target.removeprefix('/app/'),raw)
    jobs('jobs-before')
    before=snapshot(backup=True);save(ATTEMPT/'snapshot-before.json',before)
    argv=HELPER.native_import_argv(b,{t:str(ROOT/'staged'/t.removeprefix('/app/')) for t in REQUEST['sources']},
        {t:i['sha256'] for t,i in REQUEST['sources'].items()})
    output=run(argv,timeout=65);require('PAPER_IMPORT_OK:' in output,'native_import_failed')
    # Probe the exact staged Sina helper without importing the application or writing DB.
    output=run(['docker','exec','-i',BACKEND,'python3','-B','-c',REQUEST['sina_probe']],timeout=50)
    rows=[json.loads(line.split(':',1)[1]) for line in output.splitlines() if line.startswith('ETF_PROVIDER:')]
    require(len(rows)==1 and rows[0]['count']>100 and rows[0]['last']['trade_date']=='2026-11-02','sina_probe_failed')
    save(ATTEMPT/'provider.json',rows[0])
    require(protected(snapshot())==protected(before),'production_changed_during_preflight')
    jobs('jobs-ready')
    save(ATTEMPT/'backup/manifest.json',{str(p.relative_to(ATTEMPT/'backup')):digest(p.read_bytes()) for p in (ATTEMPT/'backup').rglob('*') if p.is_file()})
    return before


def stop():
    # A failed Compose up can remove the replacement before returning an error.
    # Absence must not prevent recreating the original during rollback.
    project=REQUEST['chain'][REQUEST['chain'].index('--project-name')+1]
    ids=run(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project,
             '--filter','label=com.docker.compose.service=backend']).split()
    for ident in ids:
        v=info(ident)
        if v['State']['Running']:run(['docker','stop','--time','30',ident],timeout=45)
        require(not info(ident)['State']['Running'],'backend_still_running')


def up(new):
    run(chain(new)+['up','-d','--no-deps','--force-recreate','--pull','never','backend'],timeout=150)
    for _ in range(36):
        v=info();require(v['State']['Running'],'backend_stopped')
        if v['State'].get('Health',{}).get('Status')=='healthy':return
        time.sleep(5)
    raise RuntimeError('health_timeout')


def publish_frontend():
    root=frontend_root()
    # Immutable assets first, index last. Keep the old chunks for open browser tabs.
    for name,item in REQUEST['frontend_delta'].items():
        if name=='index.html':continue
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
        require(not path.exists(),'asset_collision_after_preflight')
        raw=(ROOT/'staged/frontend'/name).read_bytes();require(digest(raw)==item['sha256'],'staged_asset_changed')
        with path.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        path.chmod(0o644)
    save(root/'index.html',(ROOT/'staged/frontend/index.html').read_bytes());(root/'index.html').chmod(0o644)


def main():
    attempted=False;front_attempted=False;before=None
    try:
        event('preflight_and_backup');before=preflight()
        if PAYLOAD['check_only']:event('prepared_no_switch');return 0
        event('stopping_backend');attempted=True;stop()
        event('recreating_backend');up(True);running(True)
        require(protected(snapshot(True))==protected(before),'protected_state_changed')
        jobs('jobs-after')
        event('publishing_static_assets');front_attempted=True;publish_frontend()
        checks={p:item['sha256'] for p,item in REQUEST['frontend_delta'].items()}
        save(ATTEMPT/'frontend-http.json',frontend_probe(checks))
        save(ATTEMPT/'snapshot-after.json',snapshot(True))
        save(ATTEMPT/'completion.json',dict(status='deployed',source_head=REQUEST['source_head'],backend_mounts=45,
            frontend_resources_verified=len(checks),database_unchanged=True))
        event('complete');return 0
    except BaseException as error:
        STATE['failure']=str(error) if isinstance(error,RuntimeError) else type(error).__name__;event('failed')
        if attempted:
            try:
                event('rolling_back')
                if front_attempted:
                    root=frontend_root();save(root/'index.html',(ATTEMPT/'backup/frontend/index.html').read_bytes());(root/'index.html').chmod(0o644)
                stop();up(False);running(False)
                require(protected(snapshot())==protected(before),'rollback_state_changed_preserve_database')
                jobs('jobs-rollback');event('rolled_back')
            except BaseException as rollback_error:
                STATE['rollback_failure']=str(rollback_error) if isinstance(rollback_error,RuntimeError) else type(rollback_error).__name__
                event('rollback_requires_attention')
        return 1


if __name__=='__main__':
    os.umask(0o077);PAYLOAD=json.load(sys.stdin);REQUEST=PAYLOAD['request']
    ATTEMPT=ROOT/'attempts'/PAYLOAD['attempt'];require(ATTEMPT.parent==ROOT/'attempts' and ATTEMPT.is_dir(),'invalid_attempt')
    lock=(ROOT/'switch.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    VERIFY=types.ModuleType('verify');exec(REQUEST['verify'],VERIFY.__dict__)
    HELPER=types.ModuleType('prepare');HELPER.__file__=str(ROOT/'prepare.py');exec(REQUEST['prepare'],HELPER.__dict__)
    STATE=dict(stage='starting');raise SystemExit(main())
