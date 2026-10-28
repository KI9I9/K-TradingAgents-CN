#!/usr/bin/env python3
"""Freeze the four-file history addition on top of deployed v12, without deployment.

Requires a clean committed tree and the read-only production audit. Existing v12
mounts and frontend stay in their own immutable release directory.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

BASELINE='cfc629fe7e9fcb49477fd9208fa2013294095098'
FILES=('app/services/historical_contract.py','app/services/historical_data_service.py',
       'app/worker/tushare_sync_service.py','tradingagents/dataflows/providers/china/tushare.py')
TOOLS=('deploy/history_contract_smoke.py','deploy/audit_tushare_history.py',
       'deploy/verify_compose_merge.py','deploy/quote_guard_runtime.py')


def sha(data): return hashlib.sha256(data).hexdigest()


def build(repo, output, release_dir, audit):
    def git(*args): return subprocess.check_output(['git','-C',str(repo),*args])
    if git('status','--porcelain').strip(): raise ValueError('commit_before_freezing_release')
    changed=set(git('diff','--name-only',BASELINE,'HEAD','--','app/','tradingagents/').decode().splitlines())
    if changed != set(FILES): raise ValueError('history_delivery_scope_mismatch')
    if not release_dir.is_absolute(): raise ValueError('absolute_production_release_path_required')
    hashes=audit['module_hashes']
    entries=[]
    for path in FILES:
        entry={'path':path,'target':'/app/'+path,'sha256':sha((repo/path).read_bytes())}
        if path != FILES[0]:
            entry['baseline_sha256']=sha(git('show',BASELINE+':'+path))
            if hashes.get(path)!=entry['baseline_sha256']: raise ValueError('production_baseline_mismatch:'+path)
        entries.append(entry)
    manifest=dict(schema=1,kind='history-addition-to-v12',source_head=git('rev-parse','HEAD').decode().strip(),
        baseline=BASELINE,files=entries,expected_backend_mounts_before=27,expected_backend_mounts_after=31,
        required_quote_contract_sha256=hashes['app/services/quote_contract.py'],
        required_runtime_sha256=audit['runtime']['runtime_sha256'],
        authorization='artifact only; no production switch, STOP release, supply activation or legacy migration')
    output.mkdir(parents=True,exist_ok=False)
    for path in FILES+TOOLS:
        target=output/path
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes((repo/path).read_bytes())
    override={'services':{'backend':{'volumes':[
        dict(type='bind',source=str(release_dir/path),target='/app/'+path,read_only=True,
             bind={'create_host_path':False}) for path in FILES]}}}
    for name,value in [('manifest.json',manifest),('compose.history.json',override)]:
        (output/name).write_text(json.dumps(value,indent=2)+'\n')
    (output/'SHA256SUMS').write_text(''.join(sha(path.read_bytes())+'  '+str(path.relative_to(output))+'\n'
        for path in sorted(output.rglob('*')) if path.is_file()))
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--release-dir',type=Path,required=True)
    parser.add_argument('--audit',type=Path,required=True)
    args=parser.parse_args()
    result=build(args.repo.resolve(),args.output.resolve(),args.release_dir,json.loads(args.audit.read_text()))
    print(json.dumps({'files':len(result['files']),'source_head':result['source_head'],'output':str(args.output)}))


if __name__=='__main__': main()
