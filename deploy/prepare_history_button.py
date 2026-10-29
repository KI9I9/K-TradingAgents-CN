"""Production inspection and private code backup only. No deployment or policy grant."""
import json
import os
from pathlib import Path
from run_history_sample import ssh, EVIDENCE

ROOT=Path(__file__).resolve().parents[1]
RELEASE=Path('/home/ubuntu/quote-guard-releases/history-button-v1-20261029')


def main():
    manifest=json.loads((RELEASE/'manifest.json').read_text())
    request=dict(manifest=manifest,override=json.loads((RELEASE/'compose.history-button.json').read_text()),
        verify=(ROOT/'deploy/verify_compose_merge.py').read_text())
    code='REQUEST='+repr(request)+'\n'+r'''
import subprocess,json,hashlib,types,pathlib,os,datetime
os.umask(0o077)
def run(args,data=None):return subprocess.check_output(args,input=data,text=True,timeout=40)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def digest(value):return sha(json.dumps(value,sort_keys=True,separators=(',',':')).encode())
v=types.ModuleType('verify');exec(REQUEST['verify'],v.__dict__)
i=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
f=json.loads(run(['docker','inspect','ta-live-088a60b962bf-frontend-1']))[0]
if len(i['Mounts'])!=34 or i['State'].get('Health',{}).get('Status')!='healthy':raise RuntimeError('baseline_state')
l=i['Config']['Labels'];chain=['docker','compose','--project-directory',l['com.docker.compose.project.working_dir'],
    '--project-name',l['com.docker.compose.project'],'--profile','cutover-approved']
for p in l['com.docker.compose.project.config_files'].split(','):chain+=['-f',p]
before=json.loads(run(chain+['config','--format','json']))
after=json.loads(run(chain+['-f','-','config','--format','json'],json.dumps(REQUEST['override'])))
image=json.loads(run(['docker','image','inspect',before['services']['backend']['image']]))
if v.compare_running('backend',i,image,before):raise RuntimeError('running_config_mismatch')
allowed={'backend':{m['target']:m['source'] for m in REQUEST['override']['services']['backend']['volumes']}}
normalized=json.loads(json.dumps(before));target='/app/app/services/scheduler_service.py'
mount=next(m for m in normalized['services']['backend']['volumes'] if m['target']==target)
if mount.get('type')!='bind' or mount.get('read_only') is not True:raise RuntimeError('existing_bind_unsafe')
old_source=mount['source'];mount['source']=allowed['backend'].pop(target)
if v.compare(normalized,after,allowed):raise RuntimeError('unexpected_config_change')
new={m['target'] for m in REQUEST['override']['services']['backend']['volumes']}-{target}
if any(m['Destination'] in new for m in i['Mounts']):raise RuntimeError('new_target_already_mounted')
probe="import pathlib,json,hashlib; paths="+repr([e['target'] for e in REQUEST['manifest']['files']])+"; print(json.dumps({p:(hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() if pathlib.Path(p).exists() else None) for p in paths}))"
hashes=json.loads(run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',probe]))
for entry in REQUEST['manifest']['files']:
    if entry['mode']=='replace_bind':
        if hashes[entry['target']]!=entry['baseline_sha256']:raise RuntimeError('source_baseline_changed')
    elif hashes[entry['target']] is not None:raise RuntimeError('new_file_already_exists')
root=pathlib.Path('/home/ubuntu/k-tradingagents-history-button-v1-20261029-backup')
root.mkdir(mode=0o700)  # refuse overwriting a previous backup
files={}
def save(name,raw):
    p=root/name;p.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with p.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    files[name]=sha(raw)
save('scheduler_service.py',pathlib.Path(old_source).read_bytes())
for n,p in enumerate(l['com.docker.compose.project.config_files'].split(',')):save('compose/'+str(n)+'.json',pathlib.Path(p).read_bytes())
for name,value in [('backend-inspect.json',i),('frontend-inspect.json',f),('before.json',before),('candidate.json',after),('compose-chain.json',chain)]:
    save(name,json.dumps(value,sort_keys=True).encode())
for m in i['Mounts']:
    if m['Destination'].startswith('/run/secrets/') and pathlib.Path(m['Source']).is_file():
        save('seals/'+pathlib.Path(m['Destination']).name,pathlib.Path(m['Source']).read_bytes())
for name,d in files.items():
    if sha((root/name).read_bytes())!=d:raise RuntimeError('backup_verify_failed')
save('manifest.json',json.dumps(files,sort_keys=True).encode())
print(json.dumps({'source_head':REQUEST['manifest']['source_head'],'backup':str(root),'backup_file_count':len(files),
    'chain':chain,'image_id':i['Image'],'backend_id':i['Id'],'frontend_id':f['Id'],'old_scheduler_source':old_source,
    'mounts_before':34,'mounts_after':39,'source_hashes':hashes,
    'config_hashes':{'before':digest(before),'after':digest(after)},'service_changed':False,'policy_granted':False}))
'''
    report=ssh(code)
    out=EVIDENCE/'history-button-preflight.json'
    fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2)
    print(json.dumps({'evidence':str(out),**report}))


if __name__=='__main__':main()
