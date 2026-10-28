"""Read-only live Compose comparison. Does not stage files or restart services."""
import json
import os
from pathlib import Path
from run_history_sample import ssh, EVIDENCE
from run_history_ingress_switch import RELEASE, REPO


def main():
    override=json.loads((RELEASE/'compose.history-ingress.json').read_text())
    code='OVERRIDE='+repr(override)+'\nVERIFY_CODE='+repr((REPO/'deploy/verify_compose_merge.py').read_text())+'\n'
    code+='''import subprocess,json,hashlib,types
v=types.ModuleType('verify');exec(VERIFY_CODE,v.__dict__)
def run(args,data=None):return subprocess.check_output(args,input=data,text=True,timeout=30)
def digest(obj):return hashlib.sha256(json.dumps(obj,sort_keys=True,separators=(',',':')).encode()).hexdigest()
i=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
l=i['Config']['Labels']
chain=['docker','compose','--project-directory',l['com.docker.compose.project.working_dir'],'--project-name',l['com.docker.compose.project'],'--profile','cutover-approved']
for p in l['com.docker.compose.project.config_files'].split(','):chain+=['-f',p]
before=json.loads(run(chain+['config','--format','json']))
after=json.loads(run(chain+['-f','-','config','--format','json'],json.dumps(OVERRIDE)))
allowed={'backend':{m['target']:m['source'] for m in OVERRIDE['services']['backend']['volumes']}}
if v.compare(before,after,allowed):raise RuntimeError('unexpected_config_change')
image=json.loads(run(['docker','image','inspect',before['services']['backend']['image']]))
if v.compare_running('backend',i,image,before):raise RuntimeError('running_config_mismatch')
if len(i['Mounts'])!=31 or i['State']['Health']['Status']!='healthy':raise RuntimeError('baseline_state')
if any(m['Destination'] in allowed['backend'] for m in i['Mounts']):raise RuntimeError('already_mounted')
print(json.dumps({'chain':chain,'image_id':i['Image'],'backend_id':i['Id'],'health':'healthy','mounts_before':31,'mounts_after':34,'config_hashes':{'before':digest(before),'after':digest(after)},'service_changed':False}))
'''
    report=ssh(code)
    target=EVIDENCE/'history-ingress-switch-preflight.json'
    fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2)
    print(json.dumps({'output':str(target),**report}))

if __name__=='__main__':main()
