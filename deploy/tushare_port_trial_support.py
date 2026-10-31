"""Self-contained timer payloads for the approved temporary transport trial.

These contain no credentials and never trigger tasks, call data APIs or modify
business collections. Prefix variables are supplied by the operator controller.
"""

SOURCE = '150.158.26.43'
RULE = ['-s', SOURCE+'/32', '-p', 'tcp', '--dport', '443', '-m', 'conntrack',
        '--ctstate', 'NEW', '-m', 'comment', '--comment',
        'ta-tushare-443-trial-20261031-v2', '-j', 'ACCEPT']

REMOTE_CLEANUP = r'''
import subprocess,json,pathlib,time,os
errors=[]
def call(args):
    try:return subprocess.run(args,capture_output=True,text=True,timeout=18)
    except Exception as e:
        errors.append(type(e).__name__);return None
check=call(['iptables','-w','10','-C','INPUT']+RULE)
if check is None or check.returncode!=1:
    call(['iptables','-w','10','-D','INPUT']+RULE)
# Always attempt both closures, even if rule deletion fails.
call(['systemctl','stop',UNIT])
check=call(['iptables','-w','10','-C','INPUT']+RULE)
rule_absent=check is not None and check.returncode==1
ports=call(['ss','-H','-ltnp','sport','=',':443'])
port_absent=ports is not None and ports.returncode==0 and not ports.stdout.strip()
state=call(['systemctl','show',UNIT,'-p','ActiveState','-p','MainPID'])
props=dict(x.split('=',1) for x in state.stdout.splitlines() if '=' in x) if state else {}
inactive=props.get('ActiveState')=='inactive' and props.get('MainPID')=='0'
result=dict(rule_absent=rule_absent,port_absent=port_absent,unit_inactive=inactive,
            closed=rule_absent and port_absent and inactive,errors=errors,at=time.time())
folder=pathlib.Path(ROOT)
if folder.is_dir():
    fd=os.open(folder/('cleanup-'+str(time.time_ns())+'.json'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(result,f);f.flush();os.fsync(f.fileno())
print(json.dumps(result))
'''

LOCAL_OVERRIDE = ('[Service]\nExecStart=\nExecStart=/usr/bin/ssh -F '
                  '/etc/ta-tushare-egress/ssh_config -p 443 -o '
                  'HostKeyAlias=217.142.241.226 -N osaka-tushare-egress\n')
LOCAL_DROPIN = '/run/systemd/system/ta-tushare-forward.service.d/443-trial.conf'
LOCAL_ROOT = '/home/ubuntu/quote-guard-releases/ssh443-trial-local-20261031-v2'
LOCAL_TIMER = 'ta-tushare-ssh443-local-restore-20261031-v2'

LOCAL_RESTORE = r'''
import pathlib,json,hashlib,subprocess,time,os
root=pathlib.Path(ROOT);p=pathlib.Path(DROPIN)
manifest=json.loads((root/'manifest.json').read_text())
result={'restored':False,'at':time.time()}
try:
    if p.exists():
        assert hashlib.sha256(p.read_bytes()).hexdigest()==EXPECTED,'override_changed'
        p.unlink()
    # Also reload/restart on repeated cleanup: the previous attempt could have
    # removed the file but lost its connection before the manager was reloaded.
    subprocess.run(['systemctl','daemon-reload'],check=True,capture_output=True,timeout=15)
    subprocess.run(['systemctl','restart','ta-tushare-forward.service'],check=True,capture_output=True,timeout=25)
    unchanged=all(hashlib.sha256(pathlib.Path(k).read_bytes()).hexdigest()==v['sha256'] for k,v in manifest['files'].items())
    actual=subprocess.check_output(['systemctl','show','ta-tushare-forward.service','-p','ExecStart','--value'],text=True,timeout=5)
    result.update(restored=unchanged and not p.exists() and '-p 443' not in actual,
                  original_files_unchanged=unchanged,override_absent=not p.exists())
except Exception as e:result.update(error_type=type(e).__name__)
fd=os.open(root/('restore-'+str(time.time_ns())+'.json'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:json.dump(result,f);f.flush();os.fsync(f.fileno())
print(json.dumps(result))
'''


def remote_cleanup(root, unit):
    return 'ROOT='+repr(root)+'\nUNIT='+repr(unit)+'\nRULE='+repr(RULE)+'\n'+REMOTE_CLEANUP


def local_restore():
    import hashlib
    return ('ROOT='+repr(LOCAL_ROOT)+'\nDROPIN='+repr(LOCAL_DROPIN)+
            '\nEXPECTED='+repr(hashlib.sha256(LOCAL_OVERRIDE.encode()).hexdigest())+
            '\n'+LOCAL_RESTORE)
