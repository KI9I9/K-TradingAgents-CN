"""Freeze the original-button fix. Never stages, deploys, grants or sends requests."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

FILES=('app/services/scheduler_service.py','app/services/tushare_history_batches.py',
       'tradingagents/utils/tushare_batch.py','deploy/history_sync_plan.py',
       'deploy/history_batch_journal.py','deploy/history_batch_adapters.py')
BASELINE='33c69c033927240e7f74b4cb6b822c855bd52ce7'
ROOT=Path(__file__).resolve().parents[1]


def sha(raw):return hashlib.sha256(raw).hexdigest()


def build(output,release_dir):
    def git(*args):return subprocess.check_output(['git','-C',str(ROOT),*args])
    if git('status','--porcelain').strip():raise ValueError('commit_before_freezing')
    changed=set(git('diff','--name-only',BASELINE,'HEAD','--','app/','tradingagents/','frontend/').decode().splitlines())
    if changed!={p for p in FILES if p.startswith(('app/','tradingagents/'))}:raise ValueError('unexpected_application_change')
    if not release_dir.is_absolute():raise ValueError('absolute_release_path_required')
    output.mkdir(parents=True,exist_ok=False)
    entries=[]
    for path in FILES:
        raw=(ROOT/path).read_bytes();target=output/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
        entry=dict(path=path,target='/app/'+path,sha256=sha(raw),mode='replace_bind' if path==FILES[0] else 'new_bind')
        if path==FILES[0]:entry['baseline_sha256']=sha(git('show',BASELINE+':'+path))
        entries.append(entry)
    manifest=dict(schema=1,source_head=git('rev-parse','HEAD').decode().strip(),files=entries,
        mounts_before=34,mounts_after=39,scope='backend only; frontend and original STOP files unchanged',
        activation='separate finite policy required; absent policy refuses before task/provider creation')
    override={'services':{'backend':{'volumes':[dict(type='bind',source=str(release_dir/p),target='/app/'+p,
        read_only=True,bind={'create_host_path':False}) for p in FILES]}}}
    for name,value in [('manifest.json',manifest),('compose.history-button.json',override)]:
        (output/name).write_text(json.dumps(value,indent=2)+'\n')
    (output/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(output))+'\n'
        for p in sorted(output.rglob('*')) if p.is_file()))
    archive=output.with_suffix('.tgz')
    with tarfile.open(archive,'x:gz') as tar:
        for p in sorted(output.rglob('*')):
            if p.is_file():tar.add(p,arcname=str(p.relative_to(output)),recursive=False)
    archive.with_suffix('.tgz.sha256').write_text(sha(archive.read_bytes())+'\n')
    return dict(archive=str(archive),sha256=sha(archive.read_bytes()),source_head=manifest['source_head'])


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--release-dir',type=Path,required=True)
    a=p.parse_args();print(json.dumps(build(a.output.resolve(),a.release_dir)))
