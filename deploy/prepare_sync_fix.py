"""Read-only exact compose merge check against the observed production baseline."""
import json,os
from pathlib import Path
from run_history_sample import ssh,EVIDENCE
ROOT=Path(__file__).resolve().parents[1]
RELEASE=Path('/home/ubuntu/quote-guard-releases/sync-fix-v1-20261030')
REMOTE=r'''
import json,hashlib,subprocess,types,sys
r=json.load(sys.stdin);old=r['baseline'];v=types.ModuleType('verify');exec(r['verify'],v.__dict__)
def run(a,data=None):return subprocess.check_output(a,input=data,text=True,timeout=45)
def sha(x):return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':')).encode()).hexdigest()
before=json.loads(run(old['chain']+['config','--format','json']))
assert before==old['config'],'config_changed'
after=json.loads(run(old['chain']+['-f','-','config','--format','json'],json.dumps(r['override'])))
normalized=json.loads(json.dumps(before))
for service in ('backend','frontend'):
 i=json.loads(run(['docker','inspect','ta-live-088a60b962bf-'+service+'-1']))[0]
 image=json.loads(run(['docker','image','inspect',before['services'][service]['image']]))
 assert not v.compare_running(service,i,image,before),'runtime_changed'
 for new in r['override']['services'][service]['volumes']:
  mount=next(m for m in normalized['services'][service]['volumes'] if m['target']==new['target'])
  assert mount['read_only'] is True
  mount['source']=new['source']
assert not v.compare(normalized,after,{}),'unexpected_config_change'
old.pop('config');old['config_hashes']=dict(before=sha(before),after=sha(after))
print(json.dumps(old))
'''
if __name__=='__main__':
    report=ssh(REMOTE,dict(baseline=json.loads((EVIDENCE/'sync-fix-preflight-initial.json').read_text()),
        override=json.loads((RELEASE/'compose.sync-fix.json').read_text()),verify=(ROOT/'deploy/verify_compose_merge.py').read_text()))
    fd=os.open(EVIDENCE/'sync-fix-preflight.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(report,f)
    print(json.dumps(dict(config_hashes=report['config_hashes'],verified=True)))
