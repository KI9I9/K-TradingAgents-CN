"""Open the approved 140-point raw daily path once; retain old blocked evidence."""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import run_history_market_opening as prior
base=prior.base
OLD_ROOT=base.ROOT
base.ROOT='/home/ubuntu/k-tradingagents-history-basic-opening-20261030-v1'
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/history-basic-v1-20261030')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
for name in ('LAUNCH','STATUS','DRIVER'):
    setattr(base,name,getattr(base,name).replace(OLD_ROOT,base.ROOT))
base.LAUNCH=base.LAUNCH.replace('history-market-v1-20261029','history-basic-v1-20261030').replace('deployed_market_gate_stopped','deployed_basic_gate_stopped')


def package():
    root=Path(__file__).resolve().parents[1]
    manifest=json.loads((base.RELEASE/'manifest.json').read_text())
    inventory=json.loads((base.EVIDENCE/'history-basic-inventory-20261030.json').read_text())
    hashes=json.loads((base.EVIDENCE/'history-market-production-20261029T161721Z-4948fa8d.json').read_text())['evidence']['installed-module-hashes.json']
    hashes.update({f['target']:f['sha256'] for f in manifest['files']})
    now=datetime.now(timezone(timedelta(hours=8)))
    if now.date().isoformat()!='2026-10-30':raise ValueError('review opening date before execution')
    return dict(source_head=manifest['source_head'],archive_sha256=base.ARCHIVE_SHA,driver=base.DRIVER,
        code=(root/'deploy/history_basic_opening.py').read_text(),request=dict(source_hashes=hashes,
            codes=inventory['pool'],history_sha256=inventory['history_sha256'],history_count=inventory['history_count'],
            previous_directory=inventory['gate']['directory'],previous_active=inventory['gate']['active'],
            runtime_tool=(root/'deploy/quote_guard_runtime.py').read_text(),opening_date=now.date().isoformat(),
            min_date='2026-09-30'))


base.package=package
if __name__=='__main__':
    import sys, os
    if sys.argv[1:] == ['--action', 'collect']:
        from run_history_sample import ssh, EVIDENCE
        result=ssh(base.STATUS)
        target=EVIDENCE/'history-basic-opening-result-20261030.json'
        fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:json.dump(result,f,indent=2)
        print(json.dumps(result,ensure_ascii=False))
    else:
        base.main()
