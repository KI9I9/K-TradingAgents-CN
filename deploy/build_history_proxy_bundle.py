"""Freeze only the resolved-collection fix; does not deploy or recover data."""
import hashlib,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=Path('/home/ubuntu/quote-guard-releases/history-proxy-v1-20261029')
STAGE='/home/ubuntu/k-tradingagents-history-proxy-v1-20261029/staged'
FILE='deploy/history_batch_adapters.py'
def sha(raw):return hashlib.sha256(raw).hexdigest()
if __name__=='__main__':
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip()
    OUT.mkdir(mode=0o700)
    raw=(ROOT/FILE).read_bytes();target=OUT/FILE;target.parent.mkdir();target.write_bytes(raw)
    old=Path('/home/ubuntu/quote-guard-releases/history-button-v1-20261029')/FILE
    manifest=dict(source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        files=[dict(path=FILE,target='/app/'+FILE,sha256=sha(raw),baseline_sha256=sha(old.read_bytes()),mode='replace_bind')],
        mounts_before=39,mounts_after=39)
    (OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    override={'services':{'backend':{'volumes':[dict(type='bind',source=STAGE+'/'+FILE,target='/app/'+FILE,
        read_only=True,bind={'create_host_path':False})]}}}
    (OUT/'compose.history-proxy.json').write_text(json.dumps(override,indent=2)+'\n')
    (OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in sorted(OUT.rglob('*')) if p.is_file()))
    with tarfile.open(OUT.with_suffix('.tgz'),'x:gz') as t:
        for p in sorted(OUT.rglob('*')):
            if p.is_file():t.add(p,arcname=str(p.relative_to(OUT)),recursive=False)
    digest=sha(OUT.with_suffix('.tgz').read_bytes())
    OUT.with_suffix('.tgz.sha256').write_text(digest+'\n')
    print(json.dumps({'source_head':manifest['source_head'],'sha256':digest,'archive':str(OUT.with_suffix('.tgz'))}))
