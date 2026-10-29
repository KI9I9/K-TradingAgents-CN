"""Freeze only the resolved-collection fix; does not deploy or recover data."""
import hashlib,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=Path('/home/ubuntu/quote-guard-releases/history-market-v1-20261029')
STAGE='/home/ubuntu/k-tradingagents-history-market-v1-20261029/staged'
FILES=('app/services/tushare_history_batches.py','tradingagents/utils/tushare_batch.py','deploy/history_batch_adapters.py','deploy/history_batch_journal.py','deploy/history_sync_plan.py')
def sha(raw):return hashlib.sha256(raw).hexdigest()
if __name__=='__main__':
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip()
    OUT.mkdir(mode=0o700)
    baseline=json.loads(Path('/home/ubuntu/quote-guard-releases/production-20261028/history-generations-production-20261029T090044Z-9df7b36a.json').read_text())['evidence']['installed-module-hashes.json']
    entries=[]
    for path in FILES:
        raw=(ROOT/path).read_bytes();target=OUT/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
        old=Path('/home/ubuntu/quote-guard-releases/history-button-v1-20261029')/path
        entries.append(dict(path=path,target='/app/'+path,sha256=sha(raw),baseline_sha256=baseline['/app/'+path],mode='replace_bind'))
    manifest=dict(source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        files=entries,mounts_before=39,mounts_after=39)
    (OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    override={'services':{'backend':{'volumes':[dict(type='bind',source=STAGE+'/'+path,target='/app/'+path,
        read_only=True,bind={'create_host_path':False}) for path in FILES]}}}
    (OUT/'compose.history-market.json').write_text(json.dumps(override,indent=2)+'\n')
    (OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in sorted(OUT.rglob('*')) if p.is_file()))
    with tarfile.open(OUT.with_suffix('.tgz'),'x:gz') as t:
        for p in sorted(OUT.rglob('*')):
            if p.is_file():t.add(p,arcname=str(p.relative_to(OUT)),recursive=False)
    digest=sha(OUT.with_suffix('.tgz').read_bytes())
    OUT.with_suffix('.tgz.sha256').write_text(digest+'\n')
    print(json.dumps({'source_head':manifest['source_head'],'sha256':digest,'archive':str(OUT.with_suffix('.tgz'))}))
