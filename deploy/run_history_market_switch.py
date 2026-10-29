"""Replace four existing binds after backup, preserving all stopped permits."""
from pathlib import Path
import json
import run_history_generations_switch as prior
base=prior.base
previous_payload=base.payload
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/history-market-v1-20261029')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
base.ARTIFACT_PREFIX='history-market'
old_upload=base.ARCHIVE_REMOTE
base.ARCHIVE_REMOTE='/home/ubuntu/history-market-'+base.ARCHIVE_SHA[:12]+'.tgz'
for name in ('STAGE','LAUNCH','STATUS'):
    setattr(base,name,getattr(base,name).replace('history-generations-v1-20261029','history-market-v1-20261029').replace(old_upload,base.ARCHIVE_REMOTE))


def controller():
    s=prior.controller().replace('history-generations-v1-20261029','history-market-v1-20261029').replace('compose.history-generations.json','compose.history-market.json').replace('deployed_generations_no_new_grant','deployed_market_gate_stopped')
    s=s.replace("from pathlib import Path; print(Path('/app/runtime/tushare-history-generations').exists())", "from tradingagents.utils.tushare_batch import selected_gate; g=selected_gate(); print(not (g.directory/'STOP').is_file())")
    s=s.replace(").strip() == 'False', 'unexpected_new_grant'", " ).splitlines()[-1] == 'False', 'unexpected_new_grant'")
    return s


def payload(check_only,credentials):
    p=previous_payload(check_only,credentials)
    fresh=json.loads((base.EVIDENCE/'history-market-preflight.json').read_text())
    deployed=json.loads((base.EVIDENCE/'history-generations-production-20261029T090044Z-9df7b36a.json').read_text())
    p.update(expected_chain=fresh['chain'],config_hashes=fresh['config_hashes'],image_id=fresh['image_id'],old_sources=fresh['old_sources'],old_modules=deployed['evidence']['installed-module-hashes.json'])
    return p


base.payload=payload
base.controller_source=controller
if __name__=='__main__':raise SystemExit(base.main())
