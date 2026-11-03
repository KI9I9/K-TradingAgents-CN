"""Freeze and launch ETF delivery, with credential stdin and one-shot execution."""
import argparse
import ast
import base64
import hashlib
import inspect
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import zlib

from query_quote_guard_jobs import read_auth, REMOTE as QUERY
from run_history_button_switch import LAUNCH, STATUS
from switch_quote_guard_v12 import verify_frontend_resources
from etf_history_delivery import ROOT, NEW_TARGET, OLD_TARGET, canonical, digest, require

REPO=Path(__file__).resolve().parents[1]
BASE={OLD_TARGET:'46fec7a8f1bfbea20d51e0bda6639574a04049d3cf71a87afaaa5b5ed7569f56',
      NEW_TARGET:'5e6b3ae4adb6aab4ed6df41d4a3cccbeb61a32ebff542d0ec49551053b6597d9'}


def freeze(args):
    require(not subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(),'commit_before_freeze')
    selected=json.loads(json.loads(args.mounts.read_text())['stdout'])
    backend=selected['backend'];frontend=selected['frontend']
    labels=backend['labels'];files=labels['com.docker.compose.project.config_files'].split(',')
    chain=['docker','compose','--project-name',labels['com.docker.compose.project'],
           '--project-directory',labels['com.docker.compose.project.working_dir']]
    for path in files:chain+=['-f',path]
    modules={}
    for m in backend['mounts']:
        p=m['Destination']
        if p.startswith('/app/') and p.endswith('.py'):
            modules[p]=digest((REPO/p.removeprefix('/app/')).read_bytes())
    modules.update(BASE)
    sources={t:dict(source=(REPO/t.removeprefix('/app/')).read_text(),
               sha256=digest((REPO/t.removeprefix('/app/')).read_bytes())) for t in BASE}
    old=json.loads(args.inventory.read_text());new={}
    for p in args.dist.rglob('*'):
        if not p.is_file():continue
        require(not p.is_symlink(),'asset_symlink')
        name=str(p.relative_to(args.dist));raw=p.read_bytes();sha=digest(raw)
        if old.get(name)==sha:continue
        require(name not in old or name=='index.html','nonimmutable_asset_changed')
        new[name]=dict(sha256=sha,base64=base64.b64encode(raw).decode())
    require('index.html' in new and len(new)>1,'frontend_delta_missing')
    request=dict(source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
        backend_id=backend['id'],image=backend['image'],frontend_id=frontend['id'],chain=chain,chain_files=files,
        modules=modules,sources=sources,frontend_before=old,frontend_delta=new,
        runtime=(REPO/'deploy/quote_guard_runtime.py').read_text(),verify=(REPO/'deploy/verify_compose_merge.py').read_text(),
        prepare=(REPO/'deploy/prepare_paper_quote_supply.py').read_text(),query=QUERY,
        http_probe=inspect.getsource(verify_frontend_resources),sina_probe=args.probe.read_text())
    # Probe bytes must be extracted from exactly the candidate, not a stale copy.
    source=sources[NEW_TARGET]['source'];tree=ast.parse(source)
    for node in tree.body:
        if isinstance(node,ast.FunctionDef) and node.name in ('_fetch_etf_hist_sina_frame','_parse_etf_sina_payload','_normalize_etf_history_frame'):
            require(ast.get_source_segment(source,node) in request['sina_probe'],'provider_probe_differs')
    code=(REPO/'deploy/etf_history_delivery.py').read_text()
    package=dict(code=code,code_sha256=digest(code.encode()),payload=dict(request=request,check_only=True))
    raw=canonical(package);args.package.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with args.package.open('xb') as f:f.write(raw)
    args.package.chmod(0o600)
    args.package.with_suffix('.sha256').write_text(digest(raw)+'\n')
    return dict(frozen=True,sha256=digest(raw),frontend_delta_files=len(new),source_head=request['source_head'])


def remote(args):
    payload=None
    if args.action in ('check','execute'):
        raw=args.package.read_bytes()
        require(digest(raw)==args.package.with_suffix('.sha256').read_text().strip(),'package_changed')
        package=json.loads(raw);package['payload']['check_only']=args.action=='check'
        upload=canonical(package);sha=digest(upload)
        code="import json,sys,io,pathlib,hashlib,os\nos.umask(0o077)\nq=json.load(sys.stdin)\nraw=q['upload'].encode();assert hashlib.sha256(raw).hexdigest()==q['upload_sha256']\np=pathlib.Path('/home/ubuntu')/('history-button-switch-upload-'+q['upload_sha256']+'.json')\nif p.exists():assert p.read_bytes()==raw\nelse:p.write_bytes(raw)\nsys.stdin=io.StringIO(json.dumps(dict(upload_sha256=q['upload_sha256'],auth=q['auth'])))\n"
        code+='pathlib.Path('+repr(str(ROOT))+').mkdir(mode=0o711,exist_ok=True)\n'
        code+=LAUNCH.replace('/home/ubuntu/k-tradingagents-history-button-v1-20261029',str(ROOT))
        payload=dict(upload=upload.decode(),upload_sha256=sha,auth=read_auth(args.credentials,True))
    else:code=STATUS.replace('/home/ubuntu/k-tradingagents-history-button-v1-20261029',str(ROOT))
    bootstrap="import json,sys,zlib,io;q=json.loads(zlib.decompress(sys.stdin.buffer.read()));sys.stdin=io.StringIO(json.dumps(q['data']));exec(q['code'])"
    cmd=['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config','-S',str(args.control_socket),
        '-o','BatchMode=yes','-o','ConnectTimeout=12','-o','ConnectionAttempts=1','-o','IPQoS=none',
        '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','osaka-migration','sudo -n python3 -B -c '+shlex.quote(bootstrap)]
    try:r=subprocess.run(cmd,input=zlib.compress(json.dumps(dict(code=code,data=payload)).encode()),capture_output=True,timeout=60)
    except subprocess.TimeoutExpired:raise SystemExit('management_timeout_inspect_status_do_not_repeat_execute')
    receipt=args.package.parent/(args.action+'-'+str(time.time_ns())+'.private')
    receipt.write_text(json.dumps(dict(returncode=r.returncode,stdout=r.stdout.decode(errors='replace'),stderr=r.stderr.decode(errors='replace'))));receipt.chmod(0o600)
    require(r.returncode==0,'remote_failed_receipt_saved_use_status')
    return json.loads(r.stdout)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--action',choices=['freeze','check','execute','status'],required=True)
    p.add_argument('--package',type=Path,required=True)
    p.add_argument('--mounts',type=Path);p.add_argument('--inventory',type=Path)
    p.add_argument('--dist',type=Path);p.add_argument('--probe',type=Path)
    p.add_argument('--credentials',type=Path)
    p.add_argument('--control-socket',type=Path)
    args=p.parse_args();os.umask(0o077)
    print(json.dumps(freeze(args) if args.action=='freeze' else remote(args)))
