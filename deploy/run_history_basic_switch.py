"""Replace nine existing binds; keep stopped permits and all historical data intact."""
from pathlib import Path
import json
import run_history_market_switch as prior
base=prior.base
previous_payload=base.payload
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/history-basic-v1-20261030')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
base.ARTIFACT_PREFIX='history-basic'
old_upload=base.ARCHIVE_REMOTE
base.ARCHIVE_REMOTE='/home/ubuntu/history-basic-'+base.ARCHIVE_SHA[:12]+'.tgz'
for name in ('STAGE','LAUNCH','STATUS'):
    setattr(base,name,getattr(base,name).replace('history-market-v1-20261029','history-basic-v1-20261030').replace(old_upload,base.ARCHIVE_REMOTE))


def controller():
    source = prior.controller().replace('history-market-v1-20261029','history-basic-v1-20261030').replace('compose.history-market.json','compose.history-basic.json').replace('deployed_market_gate_stopped','deployed_basic_gate_stopped')
    marker = "    require(json.loads(in_backend(code)) == PAYLOAD['protected_files'], 'protected_permit_changed')"
    probe = "import pathlib,hashlib,json; root=pathlib.Path('/app/runtime/tushare-history-generations'); values={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}; print(hashlib.sha256(json.dumps(values,sort_keys=True,separators=(',',':')).encode()).hexdigest())"
    assert marker in source
    return source.replace(marker, marker + '\n    require(in_backend(' + repr(probe) + ").strip() == PAYLOAD['generation_tree_sha256'], 'generation_tree_changed')")


def payload(check_only,credentials):
    p=previous_payload(check_only,credentials)
    fresh=json.loads((base.EVIDENCE/'history-basic-preflight.json').read_text())
    deployed=json.loads((base.EVIDENCE/'history-market-production-20261029T161721Z-4948fa8d.json').read_text())
    p.update(expected_chain=fresh['chain'],config_hashes=fresh['config_hashes'],image_id=fresh['image_id'],old_sources=fresh['old_sources'],old_modules=deployed['evidence']['installed-module-hashes.json'])
    import hashlib
    p['generation_tree_sha256']=hashlib.sha256(json.dumps(fresh['batch_protected'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    p['ingress_api_probe']=p['ingress_api_probe'].replace('[disabled]','[usage_limit]')
    return p


base.payload=payload
base.controller_source=controller
if __name__=='__main__':raise SystemExit(base.main())
