"""Freeze replacement binds and built UI against verified live hashes."""
import hashlib,json,shutil,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=Path('/home/ubuntu/quote-guard-releases/sync-fix-v1-20261030')
STAGE='/home/ubuntu/k-tradingagents-sync-fix-v1-20261030/staged'
FILES=('app/services/tushare_history_batches.py','tradingagents/utils/tushare_batch.py','deploy/history_batch_journal.py','app/services/multi_source_basics_sync_service.py')
def sha(raw):return hashlib.sha256(raw).hexdigest()
if __name__=='__main__':
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip()
    OUT.mkdir(mode=0o700)
    fresh=json.loads(Path('/home/ubuntu/quote-guard-releases/production-20261028/sync-fix-preflight-initial.json').read_text())
    entries=[]
    for path in FILES:
        old=subprocess.check_output(['git','show','208a30f:'+path],cwd=ROOT)
        assert sha(old)==fresh['modules']['/app/'+path],path
        raw=(ROOT/path).read_bytes();target=OUT/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
        entries.append(dict(path=path,target='/app/'+path,sha256=sha(raw),baseline_sha256=sha(old),mode='replace_bind'))
    built=Path('/tmp/quote-guard-frontend-nkhx0kxi/frontend')
    for p in (ROOT/'frontend/src').rglob('*'):
        if p.is_file():assert p.read_bytes()==(built/p.relative_to(ROOT/'frontend')).read_bytes(),str(p)
    log=Path('/tmp/sync-fix-frontend-build.log').read_text()
    assert 'built in' in log
    shutil.copytree(built/'dist',OUT/'frontend-dist')
    (OUT/'frontend-build.log').write_text(log)
    manifest=dict(source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),files=entries,mounts_before=39,mounts_after=39)
    (OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    override={'services':{'backend':{'volumes':[dict(type='bind',source=STAGE+'/'+p,target='/app/'+p,read_only=True,bind={'create_host_path':False}) for p in FILES]},'frontend':{'volumes':[dict(type='bind',source=STAGE+'/frontend-dist',target='/usr/share/nginx/html',read_only=True,bind={'create_host_path':False})]}}}
    (OUT/'compose.sync-fix.json').write_text(json.dumps(override,indent=2)+'\n')
    (OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in sorted(OUT.rglob('*')) if p.is_file()))
    with tarfile.open(OUT.with_suffix('.tgz'),'x:gz') as t:
        for p in sorted(OUT.rglob('*')):
            if p.is_file():t.add(p,arcname=str(p.relative_to(OUT)),recursive=False)
    digest=sha(OUT.with_suffix('.tgz').read_bytes());OUT.with_suffix('.tgz.sha256').write_text(digest+'\n')
    print(json.dumps(dict(sha256=digest,source_head=manifest['source_head'])))
