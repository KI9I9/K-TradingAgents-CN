"""Two replaced binds, unchanged mount count, no grant during deployment."""
import json
from pathlib import Path
import run_history_proxy_switch as previous
base=previous.base
prior_payload=base.payload
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/history-generations-v1-20261029')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
base.ARTIFACT_PREFIX='history-generations'
old_upload=base.ARCHIVE_REMOTE
base.ARCHIVE_REMOTE='/home/ubuntu/history-generations-'+base.ARCHIVE_SHA[:12]+'.tgz'
for name in ('STAGE','LAUNCH','STATUS'):
    setattr(base,name,getattr(base,name).replace('history-proxy-v1-20261029','history-generations-v1-20261029').replace(old_upload,base.ARCHIVE_REMOTE))


def controller():
    s=(base.REPO/'deploy/switch_history_proxy.py').read_text().replace('history-proxy-v1-20261029','history-generations-v1-20261029').replace('compose.history-proxy.json','compose.history-generations.json').replace('deployed_proxy_fix_gate_stopped','deployed_generations_no_new_grant')
    a=s.index("    target = '/app/deploy/history_batch_adapters.py'")
    b=s.index('    require(not VERIFY.compare(normalized',a)
    s=s[:a]+'''    for target, source in allowed['backend'].items():
        mount = next(v for v in normalized['services']['backend']['volumes'] if v['target'] == target)
        require(mount.get('type') == 'bind' and mount.get('read_only') is True, 'old_bind_unsafe')
        require(mount['source'] == PAYLOAD['old_sources'][target], 'old_source_changed')
        mount['source'] = source
    allowed = {}
'''+s[b:]
    s=s.replace("    code = PAYLOAD['ingress_api_probe']", "    require(in_backend(\"from pathlib import Path; print(Path('/app/runtime/tushare-history-generations').exists())\").strip() == 'False', 'unexpected_new_grant')\n    code = PAYLOAD['ingress_api_probe']")
    return s


def payload(check_only,credentials):
    p=prior_payload(check_only,credentials)
    fresh=json.loads((base.EVIDENCE/'history-generations-preflight.json').read_text())
    deployed=json.loads((base.EVIDENCE/'history-proxy-production-20261029T070423Z-77503269.json').read_text())
    p.update(expected_chain=fresh['chain'],config_hashes=fresh['config_hashes'],image_id=fresh['image_id'],
        old_sources=fresh['old_sources'],old_modules=deployed['evidence']['installed-module-hashes.json'])
    return p


base.payload=payload
base.controller_source=controller
if __name__=='__main__':raise SystemExit(base.main())
