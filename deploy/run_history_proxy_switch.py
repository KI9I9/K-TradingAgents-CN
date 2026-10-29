"""Reuse verified detached switch/rollback for one replacement bind, 39 -> 39."""
from pathlib import Path
import json
import run_history_button_switch as base

original_payload=base.payload
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/history-proxy-v1-20261029')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
base.CONTROLLER='switch_history_proxy.py'
base.ARTIFACT_PREFIX='history-proxy'
old_upload=base.ARCHIVE_REMOTE
base.ARCHIVE_REMOTE='/home/ubuntu/history-proxy-'+base.ARCHIVE_SHA[:12]+'.tgz'
for name in ('STAGE','LAUNCH','STATUS'):
    setattr(base,name,getattr(base,name).replace('history-button-v1-20261029','history-proxy-v1-20261029').replace(old_upload,base.ARCHIVE_REMOTE))


def payload(check_only,credentials):
    p=original_payload(check_only,credentials)
    fresh=json.loads((base.EVIDENCE/'history-proxy-preflight.json').read_text())
    deployed=json.loads((base.EVIDENCE/'history-button-production-20261029T034911Z-e5c43f74.json').read_text())
    p.update(expected_chain=fresh['chain'],config_hashes=fresh['config_hashes'],image_id=fresh['image_id'],
        old_adapter_source=fresh['old_adapter_source'],old_modules=deployed['evidence']['installed-module-hashes.json'])
    p['protected_files'].update(fresh['batch_protected'])
    p['ingress_api_probe']=p['ingress_api_probe'].replace(
        'if Path("/app/runtime/tushare-history-batch-v1").exists():raise ValueError("gate_present")',
        'if not Path("/app/runtime/tushare-history-batch-v1/STOP").is_file():raise ValueError("stop_missing")'
    ).replace('[not_authorized]','[disabled]')
    return p


base.payload=payload
if __name__=='__main__':raise SystemExit(base.main())
