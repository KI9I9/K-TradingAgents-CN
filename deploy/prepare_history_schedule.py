"""Freeze/stage the history-only schedule release; never stop or enable services.

Immutable original binds/images stay available for rollback. Production backups
stay on production; stdout contains only metadata and hashes.
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

REPO=Path(__file__).resolve().parents[1]
EVIDENCE=Path('/home/ubuntu/quote-guard-releases/production-20261028')
RELEASE=Path('/home/ubuntu/quote-guard-releases/history-schedule-v1-20261102')
ROOT='/home/ubuntu/k-tradingagents-history-schedule-v1-20261102'
PREFLIGHT=EVIDENCE/'history-schedule-v1-preflight.json'
FILES=('app/worker/tushare_sync_service.py','app/services/tushare_history_batches.py',
       'app/services/scheduler_runtime.py','tradingagents/utils/tushare_manual.py',
       'app/services/tushare_history_schedule.py','app/services/scheduler_service.py')
NEW={f'/app/{p}' for p in FILES[2:5]}


def digest(raw):return hashlib.sha256(raw).hexdigest()


def db_probe():
    source=(EVIDENCE/'db_guard_probe.js').read_text()
    source=source.replace("'tushare_realtime_quotes_hourly'];", "'tushare_realtime_quotes_hourly','tushare_historical_sync','akshare_historical_sync'];")
    source=source.replace("'paper_market_rules']", "'paper_market_rules','stock_daily_quotes','stock_daily_quotes_unadjusted']")
    source=source.replace('const count = col.countDocuments({});', "const query = name.startsWith('stock_daily_quotes') ? {data_source:'tushare'} : {};\n    const count = col.countDocuments(query);")
    return source.replace('col.find({}).sort','col.find(query).sort').replace('count > 20000', "count > (name === 'stock_daily_quotes_unadjusted' ? 100000 : 20000)")


BACKEND=r'''
import pathlib,json,hashlib,base64,types
paths=REQUEST['modules']
modules={p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() if pathlib.Path(p).exists() else None for p in paths}
assert modules==paths,'modules_changed'
root=pathlib.Path('/app/runtime/tushare-history-generations')
a=json.loads((root/'active.json').read_text());g=root/a['generation'];s=json.loads((g/'state.json').read_text())
assert str(g)==REQUEST['generation'] and s['phase']=='ready' and s['reason']=='none' and s['used']==5597 and s['active'] is None and not (g/'STOP').exists(),'generation_changed'
assert s['plan']['plan_sha256']==REQUEST['plan'],'plan_changed'
from deploy.history_batch_journal import Workspace
assert Workspace(g/'runs').status(REQUEST['plan'])['complete'],'plan_incomplete'
assert not pathlib.Path('/app/runtime/tushare-history-schedule.json').exists(),'schedule_already_present'
tree={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
rt=types.ModuleType('runtime');exec(REQUEST['runtime'],rt.__dict__)
env,_=rt.reconstruct('/app/deploy/migration_production_entrypoint.py','d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b','/run/secrets')
from pymongo import MongoClient
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
client=MongoClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),retryWrites=False,retryReads=False,serverSelectionTimeoutMS=10000,socketTimeoutMS=20000)
history={}
try:
 for name in ('stock_daily_quotes','stock_daily_quotes_unadjusted'):
  col=client[env['MONGODB_DATABASE']].get_collection(name,codec_options=CodecOptions(document_class=RawBSONDocument));h=hashlib.sha256();count=0
  for row in col.find({'data_source':'tushare'}).sort('_id',1).batch_size(500).max_time_ms(20000):h.update(row.raw);count+=1
  history[name]=dict(count=count,sha256=h.hexdigest())
finally:client.close()
print(json.dumps(dict(modules=modules,generation_tree_sha256=hashlib.sha256(json.dumps(tree,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
 history_bson=history,
 protected_files={p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in REQUEST['protected_paths']},
 backups={p:base64.b64encode(pathlib.Path(p).read_bytes()).decode() for p in REQUEST['targets'] if pathlib.Path(p).exists()})))
'''

REMOTE=r'''
import pathlib,json,sys,subprocess,hashlib,base64,os,time,types
os.umask(0o077);r=json.load(sys.stdin);root=pathlib.Path(r['root'])
assert not root.exists(),'already_prepared: use status'
root.mkdir(mode=0o700)
def save(name,value,mode=0o600):
 p=root/name;p.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 raw=value if isinstance(value,bytes) else json.dumps(value,indent=2).encode()
 fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
 with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fchmod(f.fileno(),mode);os.fsync(f.fileno())
 assert p.read_bytes()==raw and p.stat().st_mode & 0o777==mode,'saved_file_mismatch'
def run(argv,data=None,timeout=60):
 p=subprocess.run(argv,input=data,text=True,timeout=timeout,capture_output=True)
 if p.returncode:
  save('last-command-failure.json',dict(command=argv[:2],returncode=p.returncode,stdout=p.stdout,stderr=p.stderr))
  raise RuntimeError('command_failed_details_saved_privately')
 return p.stdout
def sha(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
result=dict(ok=False,stage='preflight',at=time.time())
try:
 save('package.json',r)
 v=types.ModuleType('verify');exec(r['verify'],v.__dict__)
 i=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
 assert i['State']['Running'] and i['State'].get('Health',{}).get('Status')=='healthy' and len(i['Mounts'])==40,'backend_changed'
 assert i['Image']==r['image'],'image_changed'
 labels=i['Config']['Labels'];chain=['docker','compose','--project-directory',labels['com.docker.compose.project.working_dir'],'--project-name',labels['com.docker.compose.project'],'--profile','cutover-approved']
 for p in labels['com.docker.compose.project.config_files'].split(','):chain+=['-f',p]
 before=json.loads(run(chain+['config','--format','json']))
 image=json.loads(run(['docker','image','inspect',before['services']['backend']['image']]))
 assert not v.compare_running('backend',i,image,before),'running_config_mismatch'
 after=json.loads(run(chain+['-f','-','config','--format','json'],json.dumps(r['override'])))
 normalized=json.loads(json.dumps(before));mounts={m['target']:m for m in normalized['services']['backend']['volumes']};allowed={};old_sources={}
 for new in r['override']['services']['backend']['volumes']:
  target=new['target']
  if target in mounts:
   assert target not in r['new_targets'] and mounts[target].get('read_only') is True,'unexpected_existing_bind'
   old_sources[target]=mounts[target]['source'];mounts[target]['source']=new['source']
  else:
   assert target in r['new_targets'],'unexpected_new_bind'
   allowed[target]=new['source']
 assert len(old_sources)==3 and set(allowed)==set(r['new_targets']) and not v.compare(normalized,after,{'backend':allowed}),'unexpected_config_change'
 code='REQUEST='+repr(r['backend_request'])+'\n'+r['backend']
 b=json.loads(run(['docker','exec','-i',i['Id'],'python3','-B','-'],code,timeout=90))
 for p,raw in b.pop('backups').items():save('backup/modules/'+p.removeprefix('/app/'),base64.b64decode(raw))
 seals={}
 for m in i['Mounts']:
  if m['Destination'].startswith('/run/secrets/') and pathlib.Path(m['Destination']).name in r['seals']:
   raw=pathlib.Path(m['Source']).read_bytes();name=pathlib.Path(m['Destination']).name
   assert hashlib.sha256(raw).hexdigest()==r['seals'][name],'seal_changed'
   seals[name]=r['seals'][name];save('backup/seals/'+name,raw)
 assert len(seals)==3,'seals_missing'
 db=json.loads(run(['docker','exec','-i','ta-live-088a60b962bf-mongodb-1','mongosh','--quiet','--norc','--file','/dev/stdin'],r['db_probe'],timeout=90))
 assert db['active_supply_executions']==0,'execution_active'
 assert db['hashes']['stock_daily_quotes']['count']==2112 and db['hashes']['stock_daily_quotes_unadjusted']['count']==94597,'inventory_changed'
 prefs={p['id']:p.get('enabled') for p in db['preferences']}
 for name in ['akshare_quotes_sync','etf_sync','tushare_realtime_quotes_hourly','tushare_historical_sync']:assert prefs.get(name) is False,'job_enabled'
 for idx,p in enumerate(chain):
  if idx and chain[idx-1]=='-f':save('backup/compose-sources/'+str(idx)+'.json',pathlib.Path(p).read_bytes())
 for name,value in [('backend-inspect',i),('backend-config',before),('backend-compose-argv',chain),('candidate-config',after),('db',db)]:save('backup/'+name+'.json',value)
 stage=root/'staged';stage.mkdir(mode=0o700)
 for target,entry in r['files'].items():
  raw=entry['source'].encode();assert hashlib.sha256(raw).hexdigest()==entry['sha256'],'candidate_changed'
  save('staged/'+target.removeprefix('/app/'),raw,0o644)
 save('staged/compose.history-schedule.json',r['override'],0o644);save('staged/manifest.json',r['manifest'],0o644)
 members={str(p.relative_to(stage)):hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.rglob('*') if p.is_file()}
 save('staged/SHA256SUMS',''.join(h+'  '+p+'\n' for p,h in sorted(members.items())).encode(),0o644)
 # Native import as the actual application identity, with source binds only.
 # No network, credentials, configuration, database/runtime volumes or lifespan.
 sources={m['Destination']:m['Source'] for m in i['Mounts'] if m['Type']=='bind' and m['Destination'].endswith('.py')}
 sources.update({t:str(stage/t.removeprefix('/app/')) for t in r['files']})
 argv=['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--memory','1g','--cpus','1','--pids-limit','128','--log-driver','none','--user',i['Config']['User'],'--tmpfs','/tmp:rw,nosuid,nodev,mode=1777','--tmpfs','/app/data:rw,nosuid,nodev,mode=1777','--tmpfs','/app/logs:rw,nosuid,nodev,mode=1777','--env','TRADINGAGENTS_DATA_DIR=/tmp/data','--env','TRADINGAGENTS_CACHE_DIR=/tmp/cache','--entrypoint','python3']
 for target,source in sources.items():argv+=['--mount','type=bind,source='+source+',target='+target+',readonly']
 check="import signal,os,pathlib,hashlib,json; signal.signal(signal.SIGALRM,lambda *_:os._exit(124));signal.alarm(50); expected="+repr({p:e['sha256'] for p,e in r['files'].items()})+"; assert {p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in expected}==expected; import app.main; print('SCHEDULE_IMPORT_OK:'+json.dumps({'uid':os.geteuid(),'gid':os.getegid(),'full_app_import':True}))"
 output=run(argv+[i['Image'],'-B','-c',check],timeout=65)
 rows=[line.split(':',1)[1] for line in output.splitlines() if line.startswith('SCHEDULE_IMPORT_OK:')]
 assert len(rows)==1,'native_import_failed'
 readable=json.loads(rows[0]);assert readable['uid']==1000 and readable['full_app_import'],'native_identity_failed'
 save('candidate-access.json',readable)
 result.update(ok=True,stage='prepared_no_switch',chain=chain,image_id=i['Image'],backend_id=i['Id'],old_sources=old_sources,new_targets=r['new_targets'],config_hashes=dict(before=sha(before),after=sha(after)),modules=b['modules'],protected_files=b['protected_files'],generation_tree_sha256=b['generation_tree_sha256'],history_bson=b['history_bson'],db=db,checksums_sha256=hashlib.sha256((stage/'SHA256SUMS').read_bytes()).hexdigest(),package_sha256=sha(r),native_import=readable)
 save('backup/complete.json',dict(files={str(p.relative_to(root/'backup')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'backup').rglob('*') if p.is_file()},rollback='original compose chain; original sources and image untouched'))
except BaseException as e:result.update(stage='failed',error_type=type(e).__name__,reason=str(e) if isinstance(e,AssertionError) else 'details_withheld')
result['finished_at']=time.time();save('preflight.json',result);print(json.dumps(result))
'''


def package():
    baseline=json.loads((EVIDENCE/'history-schedule-baseline-20261102.json').read_text())
    previous=json.loads((EVIDENCE/'http-reuse-v2-execute-final-collect.private').read_text())
    old_request=json.loads((RELEASE.parent/'history-http-reuse-v2-20261101/controller.json').read_text())['payload']
    modules=previous['evidence']['installed-module-hashes.json']|baseline['inside']['source_hashes']
    modules['/app/app/services/tushare_history_schedule.py']=None
    files={};entries=[]
    for p in FILES:
        source=(REPO/p).read_text();ast.parse(source);target='/app/'+p
        if modules[target] is not None:
            assert digest(subprocess.check_output(['git','show','dd2e30f:'+p],cwd=REPO))==modules[target],'baseline_source_differs:'+p
        files[target]=dict(source=source,sha256=digest(source.encode()))
        entries.append(dict(path=p,target=target,sha256=files[target]['sha256'],baseline_sha256=modules[target],mode='new_bind' if target in NEW else 'replace_bind'))
    manifest=dict(source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),files=entries,mounts_before=40,mounts_after=43)
    override={'services':{'backend':{'volumes':[dict(type='bind',source=ROOT+'/staged/'+p,target='/app/'+p,read_only=True,bind={'create_host_path':False}) for p in FILES]}}}
    return dict(root=ROOT,files=files,manifest=manifest,override=override,image=baseline['backend']['image'],new_targets=sorted(NEW),seals=old_request['seal_hashes'],backend=BACKEND,verify=(REPO/'deploy/verify_compose_merge.py').read_text(),db_probe=db_probe(),backend_request=dict(modules=modules,generation=baseline['inside']['generation'],plan=baseline['inside']['plan_sha256'],protected_paths=list(old_request['protected_files']),targets=list(files),runtime=(REPO/'deploy/quote_guard_runtime.py').read_text()))


def remote(code,request,socket):
    argv=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S',str(socket) if socket else 'none','-C','-o','BatchMode=yes','-o','IPQoS=none','-o','ConnectTimeout=20','osaka-migration','sudo -n python3 -B -c '+shlex.quote(code)]
    try:p=subprocess.run(argv,input=json.dumps(request) if request is not None else None,capture_output=True,text=True,timeout=240)
    except subprocess.TimeoutExpired:raise SystemExit('management_timeout: inspect status, never repeat uncertain mutations') from None
    stamp=EVIDENCE/('history-schedule-management-'+str(time.time_ns()))
    stamp.with_suffix('.private').write_text(p.stderr)
    if p.returncode:raise SystemExit('management_failed; details withheld; inspect status before retry')
    result=json.loads(p.stdout);stamp.with_suffix('.json').write_text(json.dumps(result,indent=2));return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['freeze','prepare','status'],required=True)
    parser.add_argument('--control-socket',type=Path)
    args=parser.parse_args();os.umask(0o077)
    if args.action=='freeze':
        assert not subprocess.check_output(['git','status','--porcelain'],cwd=REPO),'commit_before_freeze'
        request=json.loads(json.dumps(package(),sort_keys=True));RELEASE.mkdir(mode=0o700)
        (RELEASE/'package.json').write_text(json.dumps(request,sort_keys=True))
        members={'manifest.json':json.dumps(request['manifest'],indent=2).encode(),'compose.history-schedule.json':json.dumps(request['override'],indent=2).encode()}
        members.update({p.removeprefix('/app/'):e['source'].encode() for p,e in request['files'].items()})
        for p,raw in members.items():
            path=RELEASE/p;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
        (RELEASE/'SHA256SUMS').write_text(''.join(digest(raw)+'  '+p+'\n' for p,raw in sorted(members.items())))
        print(json.dumps(dict(frozen=True,package_sha256=digest((RELEASE/'package.json').read_bytes()),source_head=request['manifest']['source_head'],mounts='40->43')));return
    if args.action=='prepare':
        result=remote(REMOTE,json.loads((RELEASE/'package.json').read_text()),args.control_socket)
    else:
        result=remote("from pathlib import Path; import json; p=Path("+repr(ROOT)+"); f=p/'preflight.json'; print(json.dumps({'reserved':p.exists(),'result':json.loads(f.read_text()) if f.exists() else None}))",None,args.control_socket).get('result')
    if result and result.get('ok'):
        if PREFLIGHT.exists():assert json.loads(PREFLIGHT.read_text())==result,'preflight_changed'
        else:PREFLIGHT.write_text(json.dumps(result,indent=2))
    print(json.dumps(dict(result=result and {k:result.get(k) for k in ('ok','stage','reason','package_sha256','checksums_sha256','native_import')})))


if __name__=='__main__':main()
