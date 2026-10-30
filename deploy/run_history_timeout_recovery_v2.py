"""Second audited continuation after the fixed-target SSH transport reconnected.

Retains v1 backup/receipt and all 782 charged requests. No app redeploy or policy
change. Check status before any execution; the new root permanently reserves one
operator attempt, including ambiguous outcomes.
"""
import json
from pathlib import Path
import run_history_timeout_recovery as previous

base=previous.base
old=base.ROOT
base.ROOT='/home/ubuntu/k-tradingagents-history-timeout-recovery-20261030-v2'
for name in ('LAUNCH','STATUS','DRIVER'):
    setattr(base,name,getattr(base,name).replace(old,base.ROOT))


def package():
    root=Path(__file__).resolve().parents[1]
    m=json.loads((base.RELEASE/'manifest.json').read_text())
    code=(root/'deploy/history_timeout_recovery.py').read_text().replace(
        '/app/runtime/history-timeout-recovery-20261030-v1',
        '/app/runtime/history-timeout-recovery-20261030-v2')
    return dict(source_head=m['source_head'],archive_sha256=base.ARCHIVE_SHA,driver=base.DRIVER,
        code=code,request=dict(source_hashes={f['target']:f['sha256'] for f in m['files']},
            runtime_tool=(root/'deploy/quote_guard_runtime.py').read_text(),
            generation='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa',used=782,
            plan='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4'))

base.package=package
if __name__=='__main__':base.main()
