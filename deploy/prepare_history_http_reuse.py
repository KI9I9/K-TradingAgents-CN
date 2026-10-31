"""Freeze and back up the two-file HTTP reuse release; never switch services.

The transport is currently in the image: delivery adds one read-only bind,
whereas the history worker replaces an existing bind (39 -> 40).
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

REPO = Path(__file__).resolve().parents[1]
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/production-20261028')
RELEASE = Path('/home/ubuntu/quote-guard-releases/history-http-reuse-v2-20261101')
ROOT = '/home/ubuntu/k-tradingagents-history-http-reuse-v2-20261101'
PREFLIGHT = EVIDENCE / 'history-http-reuse-v2-preflight.json'
FILES = ('tradingagents/utils/tushare_egress.py', 'app/services/tushare_history_batches.py')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def db_probe():
    source = (EVIDENCE / 'db_guard_probe.js').read_text()
    source = source.replace("'tushare_realtime_quotes_hourly'];", "'tushare_realtime_quotes_hourly','tushare_historical_sync','akshare_historical_sync'];")
    source = source.replace("'paper_market_rules']", "'paper_market_rules','stock_daily_quotes','stock_daily_quotes_unadjusted']")
    source = source.replace('const count = col.countDocuments({});', "const query = name.startsWith('stock_daily_quotes') ? {data_source:'tushare'} : {};\n    const count = col.countDocuments(query);")
    source = source.replace('col.find({}).sort', 'col.find(query).sort')
    return source.replace('count > 20000', "count > (name === 'stock_daily_quotes_unadjusted' ? 100000 : 20000)")


BACKEND = r'''
import pathlib,hashlib,json,base64
paths=REQUEST['modules']
modules={p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in paths}
assert modules==paths,'modules_changed'
root=pathlib.Path('/app/runtime/tushare-history-generations')
a=json.loads((root/'active.json').read_text());g=root/a['generation'];s=json.loads((g/'state.json').read_text())
assert str(g)=='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa','generation_changed'
assert (g/'STOP').is_file() and s['phase']=='blocked' and s['reason']=='timeout' and s['used']==2298,'state_changed'
assert s['plan']['plan_sha256']=='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4','plan_changed'
tree={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
print(json.dumps(dict(modules=modules,generation_tree_sha256=hashlib.sha256(json.dumps(tree,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
    state=dict(used=s['used'],phase=s['phase'],reason=s['reason'],plan=s['plan']['plan_sha256']),
    backups={p:base64.b64encode(pathlib.Path(p).read_bytes()).decode() for p in REQUEST['targets']})))
'''

REMOTE = r'''
import pathlib,json,sys,subprocess,hashlib,base64,os,time,types
os.umask(0o077);r=json.load(sys.stdin);root=pathlib.Path(r['root'])
assert not root.exists(),'already_prepared: use status'
root.mkdir(mode=0o700)
def save(name,value,mode=0o600):
 p=root/name;p.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 raw=value if isinstance(value,bytes) else json.dumps(value,indent=2).encode()
 fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
 with os.fdopen(fd,'wb') as f:
  f.write(raw);f.flush();os.fchmod(f.fileno(),mode);os.fsync(f.fileno())
 assert (p.stat().st_mode & 0o777)==mode,'saved_mode_mismatch'
 assert p.read_bytes()==raw,'backup_readback_failed'
def run(argv,data=None):return subprocess.check_output(argv,input=data,text=True,timeout=50)
def sha(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
result=dict(ok=False,stage='preflight',at=time.time())
try:
 save('package.json',r)
 v=types.ModuleType('verify');exec(r['verify'],v.__dict__)
 i=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
 assert i['State']['Running'] and i['State'].get('Health',{}).get('Status')=='healthy' and len(i['Mounts'])==39,'backend_changed'
 labels=i['Config']['Labels'];chain=['docker','compose','--project-directory',labels['com.docker.compose.project.working_dir'],'--project-name',labels['com.docker.compose.project'],'--profile','cutover-approved']
 for p in labels['com.docker.compose.project.config_files'].split(','):chain+=['-f',p]
 before=json.loads(run(chain+['config','--format','json']))
 image=json.loads(run(['docker','image','inspect',before['services']['backend']['image']]))
 assert i['Image']=='sha256:29ad3c39036f0f2cd19e433c9153147ef19ae82ed908c9c1c8a8900641abc277','image_changed'
 assert not v.compare_running('backend',i,image,before),'running_config_mismatch'
 after=json.loads(run(chain+['-f','-','config','--format','json'],json.dumps(r['override'])))
 normalized=json.loads(json.dumps(before));mounts={m['target']:m for m in normalized['services']['backend']['volumes']};allowed={};old_sources={}
 for new in r['override']['services']['backend']['volumes']:
  target=new['target']
  if target in mounts:
   assert target=='/app/app/services/tushare_history_batches.py' and mounts[target].get('read_only') is True,'unexpected_existing_bind'
   old_sources[target]=mounts[target]['source'];mounts[target]['source']=new['source']
  else:
   assert target=='/app/tradingagents/utils/tushare_egress.py','unexpected_new_bind'
   allowed[target]=new['source']
 assert len(allowed)==1 and not v.compare(normalized,after,{'backend':allowed}),'unexpected_config_change'
 code='REQUEST='+repr(dict(modules=r['modules'],targets=list(r['files'])))+'\n'+r['backend']
 b=json.loads(run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-'],code))
 for p,raw in b.pop('backups').items():save('backup/modules/'+p.removeprefix('/app/'),base64.b64decode(raw))
 seals={}
 for m in i['Mounts']:
  if m['Destination'] in ['/run/secrets/runtime-config.json','/run/secrets/launch-context.json','/run/secrets/activation-approval.json']:
   raw=pathlib.Path(m['Source']).read_bytes();name=pathlib.Path(m['Destination']).name
   assert hashlib.sha256(raw).hexdigest()==r['seals'][name],'seal_changed'
   seals[name]=r['seals'][name];save('backup/seals/'+name,raw)
 assert len(seals)==3,'seals_missing'
 db=json.loads(run(['docker','exec','-i','ta-live-088a60b962bf-mongodb-1','mongosh','--quiet','--norc','--file','/dev/stdin'],r['db_probe']))
 assert db['active_supply_executions']==0,'execution_active'
 assert db['hashes']['stock_daily_quotes']['count']==2112 and db['hashes']['stock_daily_quotes_unadjusted']['count']==38641,'inventory_changed'
 prefs={p['id']:p.get('enabled') for p in db['preferences']}
 for name in ['akshare_quotes_sync','etf_sync','tushare_realtime_quotes_hourly','tushare_historical_sync']:assert prefs.get(name) is False,'job_enabled'
 for idx,p in enumerate(chain):
  if idx and chain[idx-1]=='-f':save('backup/compose-sources/'+str(idx)+'.json',pathlib.Path(p).read_bytes())
 save('backup/backend-inspect.json',i);save('backup/backend-config.json',before);save('backup/backend-compose-argv.json',chain)
 save('backup/candidate-config.json',after);save('backup/db.json',db)
 stage=root/'staged';stage.mkdir(mode=0o700)
 for target,entry in r['files'].items():
  raw=entry['source'].encode();assert hashlib.sha256(raw).hexdigest()==entry['sha256'],'candidate_changed'
  save('staged/'+target.removeprefix('/app/'),raw,0o644)
 save('staged/compose.history-http-reuse.json',r['override'],0o644)
 save('staged/manifest.json',r['manifest'],0o644)
 members={str(p.relative_to(stage)):hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.rglob('*') if p.is_file()}
 save('staged/SHA256SUMS',''.join(h+'  '+p+'\n' for p,h in sorted(members.items())).encode(),0o644)
 # A root-readable hash is insufficient: the application runs as appuser.
 # Verify the actual staged bind in a disposable, disconnected process with
 # the configured identity; never mount secrets, runtime or database volumes.
 access_code="import pathlib,hashlib,json,os; expected="+repr({p:e['sha256'] for p,e in r['files'].items()})+"; assert {p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in expected}==expected; [compile(pathlib.Path(p).read_bytes(),p,'exec') for p in expected]; print(json.dumps({'readable':True,'uid':os.geteuid(),'gid':os.getegid()}))"
 access=['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--memory','128m','--cpus','1','--pids-limit','32','--log-driver','none','--user',i['Config']['User'],'--entrypoint','python3']
 for target in r['files']:
  access+=['--mount','type=bind,source='+str(stage/target.removeprefix('/app/'))+',target='+target+',readonly']
 access+=[i['Image'],'-B','-c',"import signal; signal.signal(signal.SIGALRM,lambda *_: __import__('os')._exit(124)); signal.alarm(10); "+access_code]
 readable=json.loads(run(access));assert readable['readable'] is True,'candidate_not_readable'
 save('candidate-access.json',readable)
 result.update(ok=True,stage='prepared_no_switch',chain=chain,image_id=i['Image'],backend_id=i['Id'],old_sources=old_sources,
     config_hashes=dict(before=sha(before),after=sha(after)),modules=b['modules'],generation_tree_sha256=b['generation_tree_sha256'],state=b['state'],db=db,
     checksums_sha256=hashlib.sha256((stage/'SHA256SUMS').read_bytes()).hexdigest(),package_sha256=sha(r))
 save('backup/complete.json',dict(files={str(p.relative_to(root/'backup')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'backup').rglob('*') if p.is_file()},rollback='original compose chain; original sources and image untouched'))
except BaseException as e:result.update(stage='failed',error_type=type(e).__name__,reason=str(e) if isinstance(e,AssertionError) else 'details_withheld')
result['finished_at']=time.time();save('preflight.json',result);print(json.dumps(result))
'''

STATUS = "import pathlib,json; p=pathlib.Path(" + repr(ROOT) + "); f=p/'preflight.json'; print(json.dumps(dict(reserved=p.exists(),result=json.loads(f.read_text()) if f.exists() else None)))"


def package():
    probes = sorted(EVIDENCE.glob('history-head-probe-status-*.json'))
    probe = next((json.loads(p.read_text()) for p in reversed(probes)
                  if json.loads(p.read_text()).get('result.json', {}).get('ok')), None)
    assert probe and probe['result.json']['protected_unchanged'], 'successful_HEAD_evidence_required'
    assert digest((REPO / FILES[0]).read_bytes()) == probe['manifest.json']['source_sha256'], 'probe_candidate_changed'
    baseline = json.loads((EVIDENCE / 'sync-fix-production-20261030T080524Z-028a5c7d.json').read_text())
    modules = baseline['evidence']['installed-module-hashes.json'] | probe['manifest.json']['before']['modules']
    entries = []
    files = {}
    for path in FILES:
        source = (REPO / path).read_text()
        ast.parse(source)
        target = '/app/' + path
        assert digest(subprocess.check_output(['git', 'show', '9ea073e:' + path], cwd=REPO)) == modules[target]
        entries.append(dict(path=path, target=target, sha256=digest(source.encode()), baseline_sha256=modules[target],
                            mode='new_bind' if path == FILES[0] else 'replace_bind'))
        files[target] = dict(source=source, sha256=digest(source.encode()))
    manifest = dict(source_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
                    files=entries, mounts_before=39, mounts_after=40)
    override = {'services': {'backend': {'volumes': [dict(type='bind', source=ROOT + '/staged/' + p,
        target='/app/' + p, read_only=True, bind={'create_host_path': False}) for p in FILES]}}}
    return dict(root=ROOT, files=files, modules=modules, manifest=manifest, override=override,
                seals=json.loads((EVIDENCE / 'baseline-hashes.json').read_text())['seals'], backend=BACKEND,
                verify=(REPO / 'deploy/verify_compose_merge.py').read_text(), db_probe=db_probe())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['freeze', 'prepare', 'status'], required=True)
    parser.add_argument('--control-socket', type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    if args.action == 'freeze':
        assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO), 'commit_before_freeze'
        # Use the same sorted-key representation the remote JSON loader sees.
        # Otherwise equivalent manifest objects produce different byte hashes.
        request = json.loads(json.dumps(package(), sort_keys=True))
        RELEASE.mkdir(mode=0o700)
        (RELEASE / 'package.json').write_text(json.dumps(request, sort_keys=True))
        (RELEASE / 'manifest.json').write_text(json.dumps(request['manifest'], indent=2))
        # Match the exact staged bytes/checksum list verified by the controller.
        stage_files = {'manifest.json': json.dumps(request['manifest'], indent=2).encode(),
                       'compose.history-http-reuse.json': json.dumps(request['override'], indent=2).encode()}
        for target, entry in request['files'].items():
            stage_files[target.removeprefix('/app/')] = entry['source'].encode()
        for name, raw in stage_files.items():
            path = RELEASE / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        (RELEASE / 'SHA256SUMS').write_text(''.join(digest(raw) + '  ' + name + '\n'
            for name, raw in sorted(stage_files.items())))
        print(json.dumps(dict(frozen=True, package_sha256=digest((RELEASE / 'package.json').read_bytes()),
                              source_head=request['manifest']['source_head'], mounts='39->40')))
        return
    data = (RELEASE / 'package.json').read_text() if args.action == 'prepare' else None
    cmd = ['ssh', '-F', '/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config', '-S', str(args.control_socket) if args.control_socket else 'none', '-C',
           '-o', 'BatchMode=yes', '-o', 'IPQoS=none', '-o', 'ConnectTimeout=25', 'osaka-migration',
           'sudo -n python3 -B -c ' + shlex.quote(REMOTE if data else STATUS)]
    prefix = EVIDENCE / ('history-http-prepare-' + args.action + '-' + str(time.time_ns()))
    try:
        p = subprocess.run(cmd, input=data, capture_output=True, text=True, timeout=100)
    except subprocess.TimeoutExpired:
        raise SystemExit('management_timeout: inspect status, never repeat an uncertain prepare') from None
    prefix.with_suffix('.private').write_text(p.stderr)
    if p.returncode:
        raise SystemExit('management_failed: inspect status; errors withheld')
    result = json.loads(p.stdout)
    prefix.with_suffix('.json').write_text(json.dumps(result, indent=2))
    report = result.get('result') if args.action == 'status' else result
    if report and report.get('ok'):
        target = PREFLIGHT
        if target.exists():
            assert json.loads(target.read_text()) == report, 'preflight_evidence_changed'
        else:
            target.write_text(json.dumps(report, indent=2))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
