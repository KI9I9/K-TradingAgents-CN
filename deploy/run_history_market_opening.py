"""Open the approved full A-share pool and submit the original button once.

Keeps the finite permit active on verified submission. Unknown/failed outcomes
stop it. Use --action status after any uncertain launch; never retry execute.
"""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import run_history_generation_acceptance as generation

base=generation.base
OLD_ROOT=base.ROOT
base.ROOT='/home/ubuntu/k-tradingagents-history-market-opening-20261029-v1'
base.RELEASE=Path('/home/ubuntu/quote-guard-releases/history-market-v1-20261029')
base.ARCHIVE_SHA=base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
for name in ('LAUNCH','STATUS','DRIVER'):
    setattr(base,name,getattr(base,name).replace(OLD_ROOT,base.ROOT))
base.LAUNCH=base.LAUNCH.replace('history-generations-v1-20261029','history-market-v1-20261029').replace('deployed_generations_no_new_grant','deployed_market_gate_stopped')
base.DRIVER=base.DRIVER.replace('try:\n    result=subprocess.run','verified=False\ntry:\n    result=subprocess.run',1)
base.DRIVER=base.DRIVER.replace("    save('result.json',report)","    verified=result.returncode==0 and report.get('status')=='passed' and report.get('new_gate_stopped') is False\n    save('result.json',report)",1)
start=base.DRIVER.index('    # Separate stop attempt')
base.DRIVER=base.DRIVER[:start]+'    if not verified:\n'+''.join('    '+line+'\n' for line in base.DRIVER[start:].splitlines())


def package():
    root=Path(__file__).resolve().parents[1]
    manifest=json.loads((base.RELEASE/'manifest.json').read_text())
    inventory=json.loads((base.EVIDENCE/'history-market-pool-reviewed-20261029.json').read_text())
    hashes=json.loads((base.EVIDENCE/'history-generations-production-20261029T090044Z-9df7b36a.json').read_text())['evidence']['installed-module-hashes.json']
    hashes.update({f['target']:f['sha256'] for f in manifest['files']})
    now=datetime.now(timezone(timedelta(hours=8)))
    if now.date().isoformat()!='2026-10-29':raise ValueError('review opening date before execution')
    return dict(source_head=manifest['source_head'],archive_sha256=base.ARCHIVE_SHA,driver=base.DRIVER,
        code=(root/'deploy/history_market_opening.py').read_text(),request=dict(source_hashes=hashes,
            codes=inventory['pool'],history_sha256=inventory['history_sha256'],previous_directory=inventory['gate']['directory'],
            runtime_tool=(root/'deploy/quote_guard_runtime.py').read_text(),opening_date=now.date().isoformat(),
            min_date=(now.date()-timedelta(days=30)).isoformat()))


base.package=package
if __name__=='__main__':base.main()
