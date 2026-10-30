"""Detached, one-use timeout recovery of the reviewed raw plan; no new grant."""
import json
from pathlib import Path
import run_history_basic_opening as prior
base=prior.base
old=base.ROOT
base.ROOT='/home/ubuntu/k-tradingagents-history-timeout-recovery-20261030-v1'
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/sync-fix-v1-20261030')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
for name in ('LAUNCH','STATUS','DRIVER'):
    setattr(base,name,getattr(base,name).replace(old,base.ROOT))
base.LAUNCH=base.LAUNCH.replace('history-basic-v1-20261030','sync-fix-v1-20261030').replace('deployed_basic_gate_stopped','deployed_sync_fix_gate_stopped')


def package():
    root=Path(__file__).resolve().parents[1]
    m=json.loads((base.RELEASE/'manifest.json').read_text())
    return dict(source_head=m['source_head'],archive_sha256=base.ARCHIVE_SHA,driver=base.DRIVER,
        code=(root/'deploy/history_timeout_recovery.py').read_text(),request=dict(
            source_hashes={f['target']:f['sha256'] for f in m['files']},
            runtime_tool=(root/'deploy/quote_guard_runtime.py').read_text(),
            generation='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa',used=779,
            plan='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4'))

base.package=package
if __name__=='__main__':base.main()
