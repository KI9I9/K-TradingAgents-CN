"""Prepare quote delivery without stopping services or granting quote access.

Local freeze verifies the existing candidate and records all controller bytes.
Remote prepare only backs up, stages source and imports an isolated, networkless
container. It contains no live-container restart, permission opening or order.
An uncertain remote result is recovered with status, never a repeated prepare.
"""
import argparse
import base64
import copy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import subprocess
import time
import types
import zlib

REPO = Path(__file__).resolve().parents[1]
ROOT = Path('/home/ubuntu/k-tradingagents-paper-supply-20261102')
BACKEND = 'ta-live-088a60b962bf-backend-1'
CANDIDATE = Path('/home/ubuntu/quote-guard-releases/paper-supply-v1-20261102')
CONTROLLER = Path('/home/ubuntu/quote-guard-releases/paper-supply-preflight-v1-20261102')
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/paper-supply-research-20261102')
TARGETS = {'/app/app/routers/paper.py', '/app/app/services/quote_contract.py',
           '/app/app/services/paper_quote_supply.py'}
NEW_TARGET = '/app/app/services/paper_quote_supply.py'
ENTRY_HASH = 'd75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def require(value, reason):
    if not value:
        raise ValueError(reason)


def load_candidate(directory):
    checks = {}
    for line in (directory/'SHA256SUMS').read_text().splitlines():
        expected, path = line.split('  ', 1)
        pure = PurePosixPath(path)
        require(not pure.is_absolute() and '..' not in pure.parts and str(pure) == path,
                'unsafe_candidate_path')
        require(path not in checks and len(expected) == 64, 'invalid_checksums')
        file = directory/path
        require(file.is_file() and not file.is_symlink() and digest(file.read_bytes()) == expected,
                'candidate_hash_changed')
        checks[path] = expected
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*') if p.is_file()}
    require(actual == set(checks) | {'SHA256SUMS'}, 'candidate_file_set_changed')
    manifest = json.loads((directory/'manifest.json').read_text())
    require({item['target'] for item in manifest['files']} == TARGETS
            and len(manifest['files']) == 3 and manifest['mounts_before'] == 43
            and manifest['mounts_after'] == 44, 'candidate_scope_changed')
    files = {}
    for item in manifest['files']:
        require(item['target'] == '/app/'+item['path'] and item['sha256'] == checks[item['path']],
                'candidate_manifest_changed')
        files[item['target']] = dict(source=(directory/item['path']).read_text(), **item)
    override = json.loads((directory/'compose.paper-supply.json').read_text())
    volumes = override['services']['backend']['volumes']
    require(len(volumes) == 3 and {v['target'] for v in volumes} == TARGETS,
            'candidate_override_changed')
    for volume in volumes:
        require(volume == dict(type='bind', source=str(ROOT/'staged'/volume['target'].removeprefix('/app/')),
                               target=volume['target'], read_only=True, bind={'create_host_path': False}),
                'candidate_bind_changed')
    return manifest, files, override


def compose_change(before, after, override, verifier):
    """Only two exact source replacements and one exact read-only addition."""
    normalized = copy.deepcopy(before)
    mounts = verifier.mount_map(normalized['services']['backend'])
    old_sources, added = {}, {}
    for volume in override['services']['backend']['volumes']:
        target = volume['target']
        if target in mounts:
            require(target != NEW_TARGET and mounts[target].get('type') == 'bind'
                    and mounts[target].get('read_only') is True, 'unexpected_existing_bind')
            old_sources[target] = mounts[target]['source']
            mounts[target]['source'] = volume['source']
        else:
            require(target == NEW_TARGET, 'unexpected_new_bind')
            added[target] = volume['source']
    require(set(old_sources) == TARGETS-{NEW_TARGET} and set(added) == {NEW_TARGET},
            'wrong_mount_change_set')
    require(not verifier.compare(normalized, after, {'backend': added}), 'compose_config_drift')
    return old_sources


def native_import_argv(info, staged_sources, expected):
    """No production environment, secrets, database or runtime volume is passed."""
    sources = {m['Destination']: m['Source'] for m in info['Mounts']
               if m['Type'] == 'bind' and m['Destination'].endswith('.py')}
    sources.update(staged_sources)
    argv = ['docker', 'run', '--rm', '--pull', 'never', '--network', 'none', '--read-only',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--memory', '1g',
            '--cpus', '1', '--pids-limit', '128', '--log-driver', 'none',
            '--user', info['Config']['User'], '--tmpfs', '/tmp:rw,nosuid,nodev,mode=1777',
            '--tmpfs', '/app/data:rw,nosuid,nodev,mode=1777',
            '--tmpfs', '/app/logs:rw,nosuid,nodev,mode=1777',
            '--env', 'TRADINGAGENTS_DATA_DIR=/tmp/data',
            '--env', 'TRADINGAGENTS_CACHE_DIR=/tmp/cache', '--entrypoint', 'python3']
    for target, source in sorted(sources.items()):
        require(target.startswith('/app/') and target.endswith('.py') and Path(source).is_absolute(),
                'invalid_source_mount')
        argv += ['--mount', 'type=bind,source='+source+',target='+target+',readonly']
    code = (
        "import signal,os,pathlib,hashlib,json; "
        "signal.signal(signal.SIGALRM,lambda *_:os._exit(124));signal.alarm(45); "
        "expected="+repr(expected)+"; "
        "assert {p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in expected}==expected; "
        "import httpx,akshare; assert hasattr(akshare,'tool_trade_date_hist_sina'); "
        "import app.main; from app.services.paper_quote_supply import read_permit,cn_now; "
        "assert read_permit(cn_now()) is None; "
        "print('PAPER_IMPORT_OK:'+json.dumps({'uid':os.geteuid(),'full_app_import':True,'permit_absent':True}))"
    )
    return argv+[info['Image'], '-B', '-c', code]


BACKEND_SNAPSHOT = r'''
import json,sys,types,hashlib,base64,time
from pathlib import Path
request=json.load(sys.stdin)
runtime=types.ModuleType('runtime');exec(request['runtime'],runtime.__dict__)
env,report=runtime.reconstruct('/app/deploy/migration_production_entrypoint.py',request['entry_hash'],'/run/secrets')
modules={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() if Path(p).is_file() else None for p in request['modules']}
assert modules==request['modules'],'production_modules_changed'
assert not Path('/app/runtime/paper-quote-supply.json').exists(),'paper_permit_already_present'
assert Path('/app/runtime/tushare-egress/STOP').is_file(),'legacy_STOP_missing'
root=Path('/app/runtime/tushare-history-generations');active=json.loads((root/'active.json').read_text())
generation=root/active['generation'];state=json.loads((generation/'state.json').read_text())
assert generation.parent==root and state['active'] is None and state['phase']=='ready' and not (generation/'STOP').exists(),'history_not_idle'
permit=json.loads(Path('/app/runtime/tushare-history-schedule.json').read_text())
assert permit['generation']==str(generation) and permit['expires_at']>time.time(),'history_permission_changed_or_expired'
paths=[root/'active.json',generation/'policy.json',generation/'state.json',
 Path('/app/runtime/tushare-history-schedule.json'),Path('/app/runtime/tushare-egress/STOP')]
runtime_files={str(p):base64.b64encode(p.read_bytes()).decode() for p in paths}
from pymongo import MongoClient
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=5000,socketTimeoutMS=10000)
try:
 db=client[env['MONGODB_DATABASE']]
 jobs=['akshare_quotes_sync','quotes_ingestion_service','etf_sync','tushare_realtime_quotes_hourly','tushare_historical_sync','akshare_historical_sync']
 assert db.scheduler_executions.count_documents({'job_id':{'$in':jobs},'status':{'$in':['running','cancelling']}})==0,'execution_active'
 prefs={r['_id']:r.get('enabled') for r in db.scheduler_preferences.find({'_id':{'$in':jobs}})}
 assert prefs.get('tushare_historical_sync') is True,'history_schedule_preference_changed'
 for job in ['akshare_quotes_sync','etf_sync','tushare_realtime_quotes_hourly']:
  assert prefs.get(job) is False,'quote_preference_enabled'
 collections={};backups={}
 for name in ['market_quotes','paper_accounts','paper_positions','paper_orders','paper_trades','paper_market_rules','scheduler_preferences']:
  col=db.get_collection(name,codec_options=CodecOptions(document_class=RawBSONDocument))
  count=col.count_documents({});assert count<=20000,'backup_too_large'
  raw=bytearray()
  for row in col.find({}).sort('_id',1).max_time_ms(10000):
   assert len(raw)+len(row.raw)<=32*1024*1024,'backup_bytes_exceeded'
   raw.extend(row.raw)
  collections[name]=dict(count=count,sha256=hashlib.sha256(raw).hexdigest())
  if request['backup']:backups[name]=base64.b64encode(raw).decode()
 history_counts={n:db[n].count_documents({'data_source':'tushare'}) for n in ['stock_daily_quotes','stock_daily_quotes_unadjusted']}
finally:client.close()
print('PAPER_SNAPSHOT:'+json.dumps(dict(modules=modules,runtime=report,runtime_files=runtime_files,
 generation=str(generation),used=state['used'],preferences=prefs,collections=collections,
 backups=backups,history_counts=history_counts)))
'''


def prepare_remote(request):
    """All backups remain on production; return only evidence hashes/metadata."""
    root = ROOT
    require(not root.exists(), 'already_reserved_use_status')
    root.mkdir(mode=0o700)
    # Docker's uid 1000 must traverse the release to read staged code. Backup
    # subdirectories stay 0700; the release itself cannot be listed by others.
    root.chmod(0o711)

    def save(name, value, mode=0o600):
        path = root/name
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        raw = value if isinstance(value, bytes) else json.dumps(value, indent=2).encode()
        fd = os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, mode)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw); os.fchmod(stream.fileno(), mode); stream.flush(); os.fsync(stream.fileno())
        require(path.read_bytes() == raw and path.stat().st_mode & 0o777 == mode, 'saved_file_mismatch')

    def run(argv, data=None, timeout=40):
        result = subprocess.run(argv, input=data, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            save('last-command-failure.json', dict(command=argv[:2],returncode=result.returncode,
                                                  stdout=result.stdout,stderr=result.stderr))
            raise RuntimeError('command_failed_details_saved_privately')
        return result.stdout

    def snapshot(backup):
        payload = dict(runtime=request['runtime'], entry_hash=ENTRY_HASH,
                       modules=request['modules'], backup=backup)
        output = run(['docker','exec','-i',BACKEND,'python3','-B','-c',BACKEND_SNAPSHOT], json.dumps(payload), 60)
        rows = [line.split(':',1)[1] for line in output.splitlines() if line.startswith('PAPER_SNAPSHOT:')]
        require(len(rows)==1,'invalid_backend_snapshot')
        return json.loads(rows[0])

    result = dict(ok=False, stage='preflight', started_at=time.time(), controller_sha256=request['controller_sha256'])
    try:
        save('request.json', request)
        verifier = types.ModuleType('verify'); exec(request['verify'], verifier.__dict__)
        info = json.loads(run(['docker','inspect',BACKEND]))[0]
        require(info['State']['Running'] and info['State'].get('Health',{}).get('Status')=='healthy'
                and len(info['Mounts'])==43 and info['Image']==request['image'], 'backend_baseline_changed')
        labels = info['Config']['Labels']
        chain = ['docker','compose','--project-directory',labels['com.docker.compose.project.working_dir'],
                 '--project-name',labels['com.docker.compose.project'],'--profile','cutover-approved']
        config_paths = labels['com.docker.compose.project.config_files'].split(',')
        for path in config_paths: chain += ['-f',path]
        before = json.loads(run(chain+['config','--format','json']))
        image = json.loads(run(['docker','image','inspect',before['services']['backend']['image']]))
        require(not verifier.compare_running('backend',info,image,before), 'running_config_drift')
        after = json.loads(run(chain+['-f','-','config','--format','json'],json.dumps(request['override'])))
        old_sources = compose_change(before,after,request['override'],verifier)
        initial = snapshot(True)
        for name, encoded in initial.pop('backups').items():
            save('backup/database/'+name+'.bson', base64.b64decode(encoded,validate=True))
        for path, encoded in initial['runtime_files'].items():
            save('backup/runtime/'+path.removeprefix('/app/runtime/'),base64.b64decode(encoded,validate=True))
        for target, source in old_sources.items():
            raw = Path(source).read_bytes()
            require(digest(raw)==request['modules'][target], 'source_file_changed')
            save('backup/modules/'+target.removeprefix('/app/'), raw)
        sealed = {}
        for mount in info['Mounts']:
            name = Path(mount['Destination']).name
            if mount['Destination'].startswith('/run/secrets/') and name in request['seals']:
                raw = Path(mount['Source']).read_bytes()
                require(digest(raw)==request['seals'][name], 'seal_changed')
                save('backup/seals/'+name,raw); sealed[name]=digest(raw)
        require(sealed==request['seals'], 'seal_set_changed')
        for index,path in enumerate(config_paths):save('backup/compose/'+str(index)+'.source',Path(path).read_bytes())
        for name, value in [('backend-inspect',info),('compose-before',before),('compose-candidate',after),
                            ('original-compose-argv',chain),('snapshot',initial)]:
            save('backup/'+name+'.json',value)
        stage = root/'staged'; stage.mkdir(mode=0o755); stage.chmod(0o755)
        staged = {}
        for target, item in request['files'].items():
            raw = item['source'].encode()
            require(digest(raw)==item['sha256'],'candidate_changed')
            save('staged/'+item['path'],raw,0o644)
            staged[target] = str(stage/item['path'])
        for path in sorted(stage.rglob('*')):
            if path.is_dir():path.chmod(0o755)
        save('staged/compose.paper-supply.json',request['override'],0o644)
        save('staged/manifest.json',request['manifest'],0o644)
        output = run(native_import_argv(info,staged,{p:e['sha256'] for p,e in request['files'].items()}),timeout=65)
        rows = [line.split(':',1)[1] for line in output.splitlines() if line.startswith('PAPER_IMPORT_OK:')]
        require(len(rows)==1,'native_import_failed')
        native = json.loads(rows[0]);require(native['uid']==1000 and native['full_app_import'] and native['permit_absent'], 'native_identity_failed')
        final = snapshot(False); final.pop('backups')
        require(initial==final,'production_changed_during_preflight')
        running = json.loads(run(['docker','inspect',BACKEND]))[0]
        require(running['Id']==info['Id'] and running['State'].get('Health',{}).get('Status')=='healthy', 'backend_changed_during_preflight')
        checks = {str(p.relative_to(stage)):digest(p.read_bytes()) for p in stage.rglob('*') if p.is_file()}
        save('staged/SHA256SUMS',''.join(h+'  '+p+'\n' for p,h in sorted(checks.items())).encode(),0o644)
        save('backup/complete.json',dict(files={str(p.relative_to(root/'backup')):digest(p.read_bytes())
             for p in (root/'backup').rglob('*') if p.is_file()},original_compose_argv=chain,
             rollback_argv=chain+['up','-d','--no-deps','--force-recreate','backend']))
        result.update(ok=True,stage='prepared_no_switch',backend_id=info['Id'],image=info['Image'],
                      original_chain=chain,old_sources=old_sources,config_hashes=dict(before=digest(canonical(before)),after=digest(canonical(after))),
                      native_import=native,collections=initial['collections'],history_counts=initial['history_counts'],
                      generation=initial['generation'],used=initial['used'],preferences=initial['preferences'],
                      checksums_sha256=digest((stage/'SHA256SUMS').read_bytes()))
    except Exception as error:
        result.update(stage='failed',error_type=type(error).__name__,reason=str(error) if isinstance(error,ValueError) else 'details_withheld')
    result['finished_at']=time.time(); save('preflight.json',result)
    return result


def freeze():
    require(not subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(), 'commit_before_freeze')
    manifest, files, override = load_candidate(CANDIDATE)
    old = json.loads((CANDIDATE.parent/'history-schedule-v2-20261102/package.json').read_text())
    modules = old['backend_request']['modules'] | {p:e['sha256'] for p,e in old['files'].items()}
    modules['/app/app/services/trading_calendar_service.py'] = manifest['calendar_baseline_sha256']
    for item in manifest['files']:
        require(modules.get(item['target'])==item['baseline_sha256'],'candidate_baseline_differs')
        modules[item['target']] = item['baseline_sha256']
    code = Path(__file__).read_text()
    request = dict(controller_sha256=digest(code.encode()),manifest=manifest,files=files,override=override,
                   modules=modules,image=old['image'],seals=old['seals'],
                   verify=(REPO/'deploy/verify_compose_merge.py').read_text(),
                   runtime=(REPO/'deploy/quote_guard_runtime.py').read_text())
    CONTROLLER.mkdir(mode=0o700)
    (CONTROLLER/'controller.json').write_text(json.dumps(dict(code=code,request=request)))
    return dict(frozen=True,sha256=digest((CONTROLLER/'controller.json').read_bytes()),source_head=manifest['source_head'])


REMOTE_BOOTSTRAP = "import sys,json,zlib;v=json.loads(zlib.decompress(sys.stdin.buffer.read()));ns={'__name__':'paper_preflight','__file__':v['module_file'],'REQUEST':v['request']};exec(compile(v['code'],v['module_file'],'exec'),ns)"


def remote_envelope(envelope, action):
    envelope = copy.deepcopy(envelope)
    envelope['module_file'] = str(ROOT/'controller.py')
    if action=='prepare':
        envelope['code'] += "\nprint(json.dumps(prepare_remote(REQUEST)))\n"
    else:
        require(action=='status','unknown_remote_action')
        envelope['code'] = "from pathlib import Path;import json;p=Path("+repr(str(ROOT))+ ");f=p/'preflight.json';print(json.dumps({'reserved':p.exists(),'result':json.loads(f.read_text()) if f.exists() else None}))"
    return envelope


def remote(action, control_socket=None):
    envelope = json.loads((CONTROLLER/'controller.json').read_text())
    require(digest(envelope['code'].encode())==envelope['request']['controller_sha256'],'controller_changed')
    envelope = remote_envelope(envelope,action)
    cmd = ['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S',str(control_socket) if control_socket else 'none',
           '-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ConnectionAttempts=1','-o','IPQoS=none',
           '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','osaka-migration',
           'sudo -n python3 -B -c '+shlex.quote(REMOTE_BOOTSTRAP)]
    name = EVIDENCE/('preflight-'+action+'-'+str(time.time_ns()))
    try:
        p = subprocess.run(cmd,input=zlib.compress(json.dumps(envelope).encode()),capture_output=True,timeout=210)
    except subprocess.TimeoutExpired as error:
        name.with_suffix('.stdout.private').write_bytes(error.stdout or b'')
        name.with_suffix('.stderr.private').write_bytes(error.stderr or b'')
        raise SystemExit('management_timeout_use_status_do_not_repeat_prepare') from None
    name.with_suffix('.stdout.private').write_bytes(p.stdout)
    name.with_suffix('.stderr.private').write_bytes(p.stderr)
    require(p.returncode==0,'management_failed_use_status_before_prepare')
    value=json.loads(p.stdout);name.with_suffix('.json').write_text(json.dumps(value,indent=2))
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze','prepare','status'],required=True)
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args();os.umask(0o077)
    result=freeze() if args.action=='freeze' else remote(args.action,args.control_socket)
    print(json.dumps(result))


if __name__=='__main__':
    main()
