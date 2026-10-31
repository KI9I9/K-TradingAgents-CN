"""Approved, one-use 10-minute SSH443 listener trial; never triggers history.

After ambiguous start results use status, never repeat start. The listener has
its own server-side RuntimeMaxSec. No original SSH config is changed.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import subprocess
import time

import query_history_market_progress as progress
from tushare_port_trial_support import RULE, SOURCE, remote_cleanup

REPO = Path(__file__).resolve().parents[1]
ROOT = '/home/ubuntu/k-tradingagents-ssh443-trial-20261031-v1'
UNIT = 'ta-tushare-ssh443-trial-20261031-v1.service'
EVIDENCE = Path('/home/ubuntu/quote-guard-releases/production-20261028')
EXPECTED = '49ddc2999ab1978c74ca064380e2beeef80f1e04ae17a0eaba0a8bcc4853ede4'
OPTIONS = ['-p', '443']
for value in (
    'AddressFamily=inet', 'ListenAddress=0.0.0.0:443',
    'PidFile=/run/ta-tushare-ssh443-trial.pid', 'AllowUsers=ta-tushare-in',
    'AuthenticationMethods=publickey', 'PubkeyAuthentication=yes',
    'PasswordAuthentication=no', 'KbdInteractiveAuthentication=no',
    'HostbasedAuthentication=no', 'ForceCommand=/usr/sbin/nologin',
    'MaxSessions=0', 'PermitTTY=no', 'X11Forwarding=no',
    'AllowAgentForwarding=no', 'PermitTunnel=no',
    'AllowStreamLocalForwarding=no', 'AllowTcpForwarding=remote',
    'GatewayPorts=no', 'PermitOpen=none', 'PermitListen=127.0.0.1:32879',
    'ClientAliveInterval=30', 'ClientAliveCountMax=3',
):
    OPTIONS += ['-o', value]

REMOTE = r'''
import json,sys,subprocess,pathlib,hashlib,os,time,stat
r=json.load(sys.stdin);root=pathlib.Path(r['root']);unit=r['unit']
os.umask(0o077)
def run(args,timeout=10):
    p=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
    if p.returncode:raise RuntimeError('command_failed:'+args[0])
    return p.stdout
def state():
    p=subprocess.run(['systemctl','show',unit,'-p','ActiveState','-p','MainPID','-p','RuntimeMaxUSec','-p','KillMode'],capture_output=True,text=True,timeout=5)
    return dict(line.split('=',1) for line in p.stdout.splitlines() if '=' in line)
def status():
    out={'reserved':root.exists(),'unit':state(),
         'listener':run(['ss','-H','-ltnp','sport','=',':443'])}
    for name in ('manifest.json','result.json'):
        if (root/name).exists():out[name]=json.loads((root/name).read_text())
    if r['firewall']:
        p=subprocess.run(['iptables','-w','10','-C','INPUT']+r['rule'],capture_output=True,timeout=15)
        out['temporary_rule_absent']=p.returncode==1
        p=subprocess.run(['systemctl','show',r['cleanup_unit']+'.timer','-p','ActiveState','-p','SubState','-p','NextElapseUSecMonotonic'],capture_output=True,text=True,timeout=5)
        out['cleanup_timer']=dict(x.split('=',1) for x in p.stdout.splitlines() if '=' in x)
        receipts=sorted(root.glob('cleanup-*.json'))
        if receipts:out['cleanup_result']=json.loads(receipts[-1].read_text())
    return out
def save(name,value):
    raw=json.dumps(value,sort_keys=True).encode()
    fd=os.open(root/name,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
if r['action']=='status':print(json.dumps(status()));sys.exit(0)
if r['action']=='stop':
    if r['firewall']:
        p=subprocess.run(['python3','-B','-c',r['cleanup_code']],capture_output=True,text=True,timeout=65)
        assert p.returncode==0,'cleanup_process_failed'
        result=json.loads(p.stdout.splitlines()[-1])
        if result['closed']:
            subprocess.run(['systemctl','stop',r['cleanup_unit']+'.timer'],capture_output=True,timeout=10)
        print(json.dumps(status()));sys.exit(0)
    # A missing collected unit is already stopped; never touch original sshd.
    s=state()
    if s.get('ActiveState') not in (None,'inactive'):
        run(['systemctl','stop',unit],timeout=15)
    print(json.dumps(status()));sys.exit(0)
assert r['action']=='start'
assert not root.exists(),'already_reserved'
assert time.time()+600<1793685600,'deadline'
assert not run(['ss','-H','-ltn','sport','=',':443']).strip(),'port_in_use'
assert state().get('ActiveState') in (None,'inactive'),'unit_in_use'
if r['firewall']:
    assert len(sys.argv)>1 and sys.argv[1].split()[0]==r['source'],'source_changed'
    assert hashlib.sha256(run(['iptables','-S','INPUT']).encode()).hexdigest()==r['input_sha256'],'firewall_changed'
    assert subprocess.run(['iptables','-w','10','-C','INPUT']+r['rule'],capture_output=True,timeout=15).returncode==1,'rule_exists_or_unreadable'
base=['/usr/sbin/sshd','-f','/etc/ssh/sshd_config']+r['options']
run(base+['-t'])
effective=run(base+['-T','-C','user=ta-tushare-in,host=localhost,addr=127.0.0.1'])
assert hashlib.sha256(effective.encode()).hexdigest()==r['expected'],'effective_config_changed'
# Read-only inventory. No login or scheduler API call occurs.
p=subprocess.run(['docker','exec','-i','ta-live-088a60b962bf-backend-1','python3','-B','-c',r['inventory_code']],
    input=json.dumps({'runtime':r['runtime']}),capture_output=True,text=True,timeout=30)
assert p.returncode==0,'inventory_query_failed'
rows=[x.split(':',1)[1] for x in p.stdout.splitlines() if x.startswith('MARKET_PROGRESS:')]
assert len(rows)==1,'inventory_output'
inventory=json.loads(rows[0])
assert inventory['stopped'] and inventory['phase']=='blocked' and inventory['reason']=='timeout','state_changed'
assert inventory['used']==2298 and inventory['raw_history_count']==38641 and inventory['history_count']==2112,'inventory_changed'
assert inventory['permit_plan']=='44ae01f972073cd2d6ec5167e8d35e15793365392480572c541cac7efaa7e1c4','plan_changed'
assert inventory['generation']=='/app/runtime/tushare-history-generations/2097c3ae1c61487e87cb7b393340f3fa','generation_changed'
assert inventory['running_jobs']==0,'task_running'
assert inventory['active']['code']=='300840.SZ','checkpoint_changed'
root.mkdir(mode=0o700)  # Exclusive reservation; no retry if any later step fails.
save('inventory-before.json',inventory)
manifest={'effective_sha256':r['expected'],'unit':unit,'started_at':time.time(),'runtime_seconds':600,'files':{},'operator_commit':r['operator_commit'],'operator_sha256':r['operator_sha256']}
try:
    files=[pathlib.Path('/etc/ssh/sshd_config')]+sorted(pathlib.Path('/etc/ssh/sshd_config.d').glob('*.conf'))
    (root/'configs').mkdir(mode=0o700)
    for n,source in enumerate(files):
        raw=source.read_bytes();target=root/'configs'/str(n)
        fd=os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        assert target.read_bytes()==raw,'backup_readback_failed'
        manifest['files'][str(source)]={'sha256':hashlib.sha256(raw).hexdigest(),'backup':str(target),'mode':stat.S_IMODE(source.stat().st_mode)}
    save('manifest.json',manifest)
    assert not run(['ss','-H','-ltn','sport','=',':443']).strip(),'port_in_use'
    assert time.time()+600<1793685600,'deadline'
    if r['firewall']:
        raw=run(['iptables-save']).encode();fd=os.open(root/'iptables-before.rules',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        save('firewall-manifest.json',dict(sha256=hashlib.sha256(raw).hexdigest(),source=r['source'],rule=r['rule']))
        fd=os.open(root/'cleanup.py',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:f.write(r['cleanup_code']);f.flush();os.fsync(f.fileno())
        run(['systemd-run','--unit='+r['cleanup_unit'],'--on-active=600s',
             '--timer-property=AccuracySec=1s','--property=TimeoutStartSec=65',
             '/usr/bin/python3','-B',str(root/'cleanup.py')])
        timer=run(['systemctl','show',r['cleanup_unit']+'.timer','-p','ActiveState','-p','SubState','-p','NextElapseUSecMonotonic'])
        assert 'ActiveState=active' in timer and 'SubState=waiting' in timer,'cleanup_timer_inactive'
        save('timer-armed.json',dict(properties=timer,at=time.time(),delay_seconds=600))
        assert hashlib.sha256(run(['iptables','-S','INPUT']).encode()).hexdigest()==r['input_sha256'],'firewall_changed'
        run(['iptables','-w','10','-I','INPUT','1']+r['rule'])
    run(['systemd-run','--unit='+unit,'--collect','--property=RuntimeMaxSec=600',
         '--property=KillMode=control-group','--property=Restart=no','--property=TimeoutStopSec=10',
         '--property=UMask=0077','--property=LimitCORE=0',
         '/usr/sbin/sshd','-D','-e','-f','/etc/ssh/sshd_config']+r['options'])
    for _ in range(20):
        s=state();listener=run(['ss','-H','-ltnp','sport','=',':443'])
        if s.get('ActiveState')=='active' and 'pid='+s.get('MainPID','missing')+',' in listener:break
        time.sleep(0.1)
    assert s.get('RuntimeMaxUSec')=='10min' and s.get('KillMode')=='control-group','auto_exit_unproven'
    assert s.get('ActiveState')=='active' and 'pid='+s.get('MainPID','missing')+',' in listener,'listener_missing'
    save('result.json',{'status':'started','pid':s['MainPID'],'auto_exit_seconds':600,'at':time.time()})
except BaseException as e:
    if r['firewall']:
        subprocess.run(['python3','-B','-c',r['cleanup_code']],capture_output=True,timeout=65)
    else:
        subprocess.run(['systemctl','stop',unit],capture_output=True,timeout=15)
    save('result.json',{'status':'failed','error_type':type(e).__name__})
print(json.dumps(status()))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['start', 'status', 'stop'], required=True)
    parser.add_argument('--run',choices=['v1','v2'],default='v1')
    args = parser.parse_args()
    if args.action=='start' and args.run=='v1':
        raise SystemExit('v1_consumed: status/stop only')
    if args.action == 'start' and subprocess.check_output(['git','status','--porcelain'],cwd=REPO):
        raise SystemExit('commit_operator_before_start')
    inventory = progress.REMOTE.replace("q={'data_source':'tushare'}", "q={'data_source':'tushare'}\nrunning_jobs=db.scheduler_executions.count_documents({'job_id':{'$regex':'^tushare_'},'status':{'$in':['running','cancelling']}})")
    inventory = inventory.replace('dict(generation=', 'dict(running_jobs=running_jobs,permit_plan=s["plan"]["plan_sha256"],generation=')
    root=ROOT.replace('-v1','-'+args.run);unit=UNIT.replace('-v1','-'+args.run)
    cleanup_unit='ta-tushare-ssh443-firewall-cleanup-20261031-'+args.run
    request = dict(action=args.action,root=root,unit=unit,options=OPTIONS,expected=EXPECTED,
                   firewall=args.run=='v2',rule=RULE,source=SOURCE,cleanup_unit=cleanup_unit,
                   cleanup_code=remote_cleanup(root,unit),
                   input_sha256='134e166bd5a7a66445e480c5db22ca813e0cccea24bd23529824734b29a12ab2',
                   operator_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
                   operator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    if args.action=='start':
        request.update(inventory_code=inventory,runtime=(REPO/'deploy/quote_guard_runtime.py').read_text())
    command = ['ssh','-F','/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config',
               '-S','none','-C','-o','BatchMode=yes','-o','IPQoS=none',
               '-o','ConnectTimeout=12','-o','ConnectionAttempts=1',
               'osaka-migration','sudo -n python3 -B -c '+shlex.quote(REMOTE)+' "$SSH_CONNECTION"']
    try:
        p = subprocess.run(command,input=json.dumps(request),capture_output=True,text=True,timeout=90)
    except subprocess.TimeoutExpired:
        raise SystemExit('management_timeout: inspect status; never repeat start') from None
    if p.returncode:
        # Operator assertions have fixed, non-secret reason identifiers. Avoid
        # exposing arbitrary SSH/Python stderr or silently hiding failed checks.
        reason=re.search(r'AssertionError: ([a-z_]+)\s*$',p.stderr)
        if reason:
            raise SystemExit('preflight_failed:'+reason[1]+'; inspect status before any retry')
        raise SystemExit('management_failed: inspect status; never repeat start')
    result = json.loads(p.stdout)
    target=EVIDENCE/('ssh443-trial-'+args.run+'-'+args.action+'-'+str(time.time_ns())+'.json')
    fd=os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as f:json.dump(result,f,indent=2)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
