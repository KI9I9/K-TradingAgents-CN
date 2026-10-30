"""Audited timeout/status repair; existing grants remain stopped during deployment."""
import ast
import hashlib
import json
from pathlib import Path
import run_history_basic_switch as prior
base = prior.base
previous_payload = base.payload
base.RELEASE = Path('/home/ubuntu/quote-guard-releases/sync-fix-v1-20261030')
base.ARCHIVE_SHA = base.RELEASE.with_suffix('.tgz.sha256').read_text().strip()
base.ARTIFACT_PREFIX = 'sync-fix'
old_upload = base.ARCHIVE_REMOTE
base.ARCHIVE_REMOTE = '/home/ubuntu/sync-fix-' + base.ARCHIVE_SHA[:12] + '.tgz'
for name in ('STAGE','LAUNCH','STATUS'):
    setattr(base,name,getattr(base,name).replace('history-basic-v1-20261030','sync-fix-v1-20261030').replace(old_upload,base.ARCHIVE_REMOTE))
# nginx must be able to traverse its mounted static directory.
base.STAGE = base.STAGE.replace("    tmp.rename(stage)", "    for p in (tmp/'frontend-dist').rglob('*'):\n        if p.is_dir():p.chmod(0o755)\n    (tmp/'frontend-dist').chmod(0o755)\n    tmp.rename(stage)")


def controller():
    source = prior.controller().replace('history-basic-v1-20261030','sync-fix-v1-20261030').replace('compose.history-basic.json','compose.sync-fix.json').replace('deployed_basic_gate_stopped','deployed_sync_fix_gate_stopped')
    marker = '    allowed = {}\n'
    addition = """    current_frontend = json.loads(run(PAYLOAD['frontend_chain'] + ['config','--format','json']))
    normalized['services']['frontend'] = current_frontend['services']['frontend']
    front = next(v for v in normalized['services']['frontend']['volumes'] if v['target']=='/usr/share/nginx/html')
    require(front['source']==PAYLOAD['old_frontend_source'] and front.get('read_only') is True, 'frontend_source_changed')
    front['source']=str(STAGED/'frontend-dist')
"""
    assert marker in source
    source = source.replace(marker,addition+marker)
    source = source.replace("'--pull', 'never', 'backend'], timeout=150)", "'--pull', 'never', 'backend'], timeout=150)\n    run((chain(True) if new else PAYLOAD['frontend_chain']) + ['up','-d','--no-deps','--force-recreate','--pull','never','frontend'], timeout=150)")
    marker = "    require(len(current['Mounts']) == 39, 'mount_count_mismatch')"
    source = source.replace(marker,marker+'''
    front = inspect('frontend')
    front_cfg = cfg if new else json.loads(run(PAYLOAD['frontend_chain'] + ['config','--format','json']))
    image = json.loads(run(['docker','image','inspect',front_cfg['services']['frontend']['image']]))
    require(not VERIFY.compare_running('frontend',front,image,front_cfg), 'frontend_runtime_mismatch')
''')
    source = source.replace("require(after_frontend['index'] == before_frontend['index'], 'frontend_index_changed')", "require(after_frontend['index']['sha256'] == PAYLOAD['frontend_index_sha256'], 'frontend_index_mismatch')")
    # Static files remain recoverable at their untouched old source.
    source = source.replace("    seal_check(backup=True)", "    save('backup/frontend-index.html', (Path(PAYLOAD['old_frontend_source'])/'index.html').read_bytes())\n    seal_check(backup=True)")
    ast.parse(source)
    return source


def payload(check_only,credentials):
    p = previous_payload(check_only,credentials)
    fresh = json.loads((base.EVIDENCE/'sync-fix-preflight.json').read_text())
    p.update(expected_chain=fresh['chain'],config_hashes=fresh['config_hashes'],image_id=fresh['image_id'],old_sources=fresh['old_sources'],old_frontend_source=fresh['frontend_source'],frontend_chain=fresh['frontend_chain'])
    deployed=json.loads((base.EVIDENCE/'history-basic-production-20261030T043707Z-858679c3.json').read_text())
    p['old_modules']=deployed['evidence']['installed-module-hashes.json'] | fresh['modules']
    p['generation_tree_sha256']=hashlib.sha256(json.dumps(fresh['generation_tree'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    p['ingress_api_probe']=p['ingress_api_probe'].replace('[usage_limit]','[timeout]')
    p['db_probe']=p['db_probe'].replace("'stock_daily_quotes']", "'stock_daily_quotes','stock_daily_quotes_unadjusted']")
    p['frontend_index_sha256']=hashlib.sha256((base.RELEASE/'frontend-dist/index.html').read_bytes()).hexdigest()
    return p

base.payload=payload
base.controller_source=controller
if __name__=='__main__':raise SystemExit(base.main())
