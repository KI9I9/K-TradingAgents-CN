"""Freeze three ingress files only; no deployment, permit renewal or DB writes."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

BASELINE='f782ddb5a42a4d73b9463be41af27a6aa0cd20a4'
FILES=('app/services/tushare_manual_sync.py','app/services/scheduler_service.py','app/routers/scheduler.py')

def sha(raw):return hashlib.sha256(raw).hexdigest()

def build(repo,output,release_dir,audit):
    def git(*args):return subprocess.check_output(['git','-C',str(repo),*args])
    if git('status','--porcelain').strip():raise ValueError('commit_before_freezing')
    changed=set(git('diff','--name-only',BASELINE,'HEAD','--','app/','tradingagents/','frontend/').decode().splitlines())
    if changed!=set(FILES):raise ValueError('delivery_scope_mismatch')
    if not release_dir.is_absolute() or audit['mounts']:raise ValueError('new_bind_precondition')
    entries=[]
    for p in FILES:
        old=sha(git('show',BASELINE+':'+p))
        if audit['source_hashes'].get(p)!=old:raise ValueError('production_baseline_mismatch')
        entries.append(dict(path=p,target='/app/'+p,baseline_sha256=old,sha256=sha((repo/p).read_bytes())))
    manifest=dict(schema=1,baseline=BASELINE,source_head=git('rev-parse','HEAD').decode().strip(),
        files=entries,expected_backend_mounts_before=31,expected_backend_mounts_after=34,
        authorization='artifact only; no deployment, permit reset, job activation or business write')
    output.mkdir(parents=True,exist_ok=False)
    for p in FILES:
        dest=output/p;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((repo/p).read_bytes())
    override={'services':{'backend':{'volumes':[dict(type='bind',source=str(release_dir/p),
        target='/app/'+p,read_only=True,bind={'create_host_path':False}) for p in FILES]}}}
    for name,value in [('manifest.json',manifest),('compose.history-ingress.json',override)]:
        (output/name).write_text(json.dumps(value,indent=2)+'\n')
    (output/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(output))+'\n'
        for p in sorted(output.rglob('*')) if p.is_file()))
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--release-dir',type=Path,required=True)
    p.add_argument('--audit',type=Path,required=True);a=p.parse_args()
    m=build(Path(__file__).resolve().parents[1],a.output.resolve(),a.release_dir,json.loads(a.audit.read_text()))
    print(json.dumps({'files':len(m['files']),'source_head':m['source_head'],'output':str(a.output)}))
