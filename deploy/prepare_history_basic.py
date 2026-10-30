"""Read-only current configuration verification; computes exact candidate hash."""
import json,os
from pathlib import Path
from run_history_sample import ssh,EVIDENCE
from run_history_button_switch import canonical,digest
ROOT=Path(__file__).resolve().parents[1]
RELEASE=Path('/home/ubuntu/quote-guard-releases/history-basic-v1-20261030')
if __name__=='__main__':
    previous=json.loads((EVIDENCE/'history-market-preflight.json').read_text())
    chain=previous['chain']+['-f','/home/ubuntu/k-tradingagents-history-market-v1-20261029/staged/compose.history-market.json']
    request=dict(chain=chain,before_sha=previous['config_hashes']['after'],image=previous['image_id'],
        override=json.loads((RELEASE/'compose.history-basic.json').read_text()),
        verify=(ROOT/'deploy/verify_compose_merge.py').read_text())
    code='R='+repr(request)+'\n'+r'''
import json,subprocess,hashlib,types
v=types.ModuleType('verify');exec(R['verify'],v.__dict__)
def run(args,data=None):return subprocess.check_output(args,input=data,text=True,timeout=40)
def sha(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
before=json.loads(run(R['chain']+['config','--format','json']))
if sha(before)!=R['before_sha']:raise RuntimeError('config_changed')
after=json.loads(run(R['chain']+['-f','-','config','--format','json'],json.dumps(R['override'])))
i=json.loads(run(['docker','inspect','ta-live-088a60b962bf-backend-1']))[0]
image=json.loads(run(['docker','image','inspect',before['services']['backend']['image']]))
if i['Image']!=R['image'] or len(i['Mounts'])!=39 or i['State'].get('Health',{}).get('Status')!='healthy':raise RuntimeError('runtime_changed')
if v.compare_running('backend',i,image,before):raise RuntimeError('running_config_mismatch')
normalized=json.loads(json.dumps(before));old_sources={}
for new in R['override']['services']['backend']['volumes']:
 mount=next(m for m in normalized['services']['backend']['volumes'] if m['target']==new['target'])
 old_sources[new['target']]=mount['source'];mount['source']=new['source']
if v.compare(normalized,after,{}):raise RuntimeError('unexpected_config_change')
probe="import pathlib,hashlib,json; p=pathlib.Path('/app/runtime/tushare-history-generations'); from tradingagents.utils.tushare_batch import selected_gate; assert (selected_gate().directory/'STOP').is_file(); print(json.dumps({str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in p.rglob('*') if f.is_file()}))"
protected=json.loads(run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',probe]))
probe="import pathlib,json,hashlib; p=pathlib.Path('/app/runtime/history-button-saved-recovery-20261029-v1'); r=json.loads((p/'result.json').read_text()); assert r['status']=='passed'; from tradingagents.utils.tushare_batch import selected_gate; g=selected_gate(); assert (g.directory/'STOP').is_file(); print(json.dumps({'series_bson_sha256':hashlib.sha256((p/'series-after.bson').read_bytes()).hexdigest()}))"
recovery=json.loads(run(['docker','exec','ta-live-088a60b962bf-backend-1','python3','-B','-c',probe]).splitlines()[-1])
print(json.dumps(dict(recovery=recovery,chain=R['chain'],image_id=R['image'],backend_id=i['Id'],old_sources=old_sources,batch_protected=protected,
    config_hashes=dict(before=sha(before),after=sha(after)))))
'''
    report=ssh(code);path=EVIDENCE/'history-basic-preflight.json'
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2)
    print(json.dumps(report))
