"""One real original-button run for 600000 under a separately selected generation.

Two requests/one hour, one or two dates from the existing 10-28 cursor; stop on
completion. No change to predecessor STOP/journals. Date is frozen before upload.
"""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import run_history_button_acceptance as base
from run_history_sample import EVIDENCE

ROOT=Path(__file__).resolve().parents[1]
RELEASE=Path('/home/ubuntu/quote-guard-releases/history-generations-v1-20261029')
NEW_ROOT='/home/ubuntu/k-tradingagents-history-generation-acceptance-20261029-v1'
base.DRIVER=base.DRIVER.replace(base.ROOT,NEW_ROOT)
base.LAUNCH=base.LAUNCH.replace(base.ROOT,NEW_ROOT).replace('history-button-v1-20261029','history-generations-v1-20261029').replace('deployed_button_gate_absent','deployed_generations_no_new_grant')
base.STATUS=base.STATUS.replace(base.ROOT,NEW_ROOT)
base.RELEASE=RELEASE
base.ARCHIVE_SHA=RELEASE.with_suffix('.tgz.sha256').read_text().strip()
base.ROOT=NEW_ROOT
old_stop="from pathlib import Path; from tradingagents.utils.tushare_batch import BatchGate; p=Path('/app/runtime/tushare-history-batch-v1'); BatchGate(p).stop() if p.is_dir() else None; print(p.is_dir() and (p/'STOP').is_file())"
new_stop="from pathlib import Path; from tradingagents.utils.tushare_batch import selected_gate; p=Path('/app/runtime/tushare-history-generations'); g=selected_gate() if p.is_dir() else None; g.stop() if g else None; print(bool(g) and (g.directory/'STOP').is_file())"
assert old_stop in base.DRIVER
base.DRIVER=base.DRIVER.replace(old_stop,new_stop)


def acceptance_code():
    s=(ROOT/'deploy/history_button_acceptance.py').read_text()
    s=s.replace("RUN = Path('/app/runtime/history-button-acceptance-20261029-v1')", "RUN = Path('/app/runtime/history-generation-acceptance-20261029-v1')")
    s=s.replace("GATE = Path('/app/runtime/tushare-history-batch-v1')", "GATE = None\nREGISTRY = Path('/app/runtime/tushare-history-generations')")
    s=s.replace("    os.umask(0o077)\n    report", "    global GATE, DAYS\n    DAYS = request['days']\n    require(DAYS in (['2026-10-28'], ['2026-10-28','2026-10-29']), 'unapproved_dates')\n    os.umask(0o077)\n    report",1)
    s=s.replace('not GATE.exists() and not RUN.exists()', 'not REGISTRY.exists() and not RUN.exists()')
    s=s.replace('len(all_before)==2094 and len(before)==5', 'len(all_before)==2095 and len(before)==6')
    s=s.replace('series_count=5, other_count=2089','series_count=6, other_count=2089')
    a=s.index('        from tradingagents.utils.tushare_batch import provision, BatchGate')
    b=s.index("        save('approved-policy.json',policy)",a)
    s=s[:a]+'''        from tradingagents.utils.tushare_batch import activate_generation
        from deploy.history_batch_journal import Workspace
        import asyncio
        os.environ.update(env)
        policy = dict(schema=1,codes=['600000.SH'],min_date=DAYS[0],max_date=DAYS[-1],
            expires_at=time.time()+3600,max_requests=2,daily_requests=2,minute_requests=2,write_enabled=True)
        async def activate():
            from motor.motor_asyncio import AsyncIOMotorClient
            from app.core.database import _store_mongo_loop_binding,get_mongo_db
            motor=AsyncIOMotorClient(host=env['MONGODB_HOST'],port=int(env['MONGODB_PORT']),
                username=env['MONGODB_USERNAME'],password=env['MONGODB_PASSWORD'],authSource=env.get('MONGODB_AUTH_SOURCE','admin'),
                retryWrites=False,retryReads=False,serverSelectionTimeoutMS=10000)
            try:
                _store_mongo_loop_binding(asyncio.get_running_loop(),motor,motor[env['MONGODB_DATABASE']])
                return await activate_generation(policy,get_mongo_db().stock_daily_quotes)
            finally:motor.close()
        gate=asyncio.run(activate());GATE=gate.directory;owns_gate=True
        report['generation_directory']=str(GATE)
'''+s[b:]
    s=s.replace('len(docs)==2 and all(', 'len(docs)==len(DAYS) and all(')
    s=s.replace("require(len(after)==6,'persisted_count')", "require(len(after)==6+len(DAYS)-1,'persisted_count')")
    s=s.replace('dates=DAYS,saved_bars=2,new_bars=1,series_count=6,','dates=DAYS,saved_bars=len(DAYS),new_bars=len(after)-len(before),series_count=len(after),')
    s=s.replace('if owns_gate and GATE.is_dir():','if owns_gate and GATE is not None and GATE.is_dir():')
    return s


def package():
    manifest=json.loads((RELEASE/'manifest.json').read_text())
    old=json.loads((EVIDENCE/'history-proxy-production-20261029T070423Z-77503269.json').read_text())
    hashes=old['evidence']['installed-module-hashes.json'].copy()
    hashes.update({e['target']:e['sha256'] for e in manifest['files']})
    preflight=json.loads((EVIDENCE/'history-generations-preflight.json').read_text())
    protected=json.loads((EVIDENCE/'history-write-package-v1.json').read_text())['request']['protected_hashes']
    protected.update(preflight['batch_protected'])
    now=datetime.now(timezone(timedelta(hours=8)))
    end=(now.date() if now.hour>=18 else now.date()-timedelta(days=1)).isoformat()
    if end not in ('2026-10-28','2026-10-29'):raise ValueError('date_scope_changed')
    days=['2026-10-28'] if end=='2026-10-28' else ['2026-10-28','2026-10-29']
    return dict(source_head=manifest['source_head'],archive_sha256=base.ARCHIVE_SHA,driver=base.DRIVER,
        code=acceptance_code(),request=dict(days=days,source_hashes=hashes,protected_hashes=protected,
            runtime_tool=(ROOT/'deploy/quote_guard_runtime.py').read_text(),
            series_bson_sha256=preflight['recovery']['series_bson_sha256'],
            other_bson_sha256='811ce8d0cde35f23318b88d76d05d53c320c2e005b45755242d6013a368050e8'))


base.package=package
if __name__=='__main__':main=base.main;main()
