"""Temporary local forward switch with an independent 8-minute restore timer.

Run switch only after SSH443 authentication succeeds. Ambiguous switch results
are resolved by status/restore, never a second switch. No upstream API calls.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from tushare_port_trial_support import (LOCAL_ROOT, LOCAL_TIMER, LOCAL_DROPIN,
                                       LOCAL_OVERRIDE, local_restore)


def run(args, timeout=20):
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL, timeout=timeout)


def main():
    args=argparse.ArgumentParser(description=__doc__)
    args.add_argument('--action',choices=['switch','status','restore'],required=True)
    args.add_argument('--auth-receipt',type=Path)
    parsed=args.parse_args();action=parsed.action
    assert os.geteuid()==0,'root_required'
    os.umask(0o077)
    root=Path(LOCAL_ROOT);dropin=Path(LOCAL_DROPIN)
    if action=='status':
        print(json.dumps(dict(reserved=root.exists(),override_present=dropin.exists(),
                              exec_start=run(['systemctl','show','ta-tushare-forward.service','-p','ExecStart','--value']))));return
    if action=='restore':
        if not root.exists():
            assert not dropin.exists(),'unexpected_override'
            print(json.dumps(dict(restored=True,never_switched=True)));return
        result=json.loads(run(['python3','-B','-c',local_restore()],timeout=55).splitlines()[-1])
        if result['restored']:
            run(['systemctl','stop',LOCAL_TIMER+'.timer'])
        print(json.dumps(result));return
    assert not root.exists() and not dropin.exists(),'already_reserved_or_overridden'
    assert parsed.auth_receipt is not None,'authentication_receipt_required'
    auth=json.loads(parsed.auth_receipt.read_text())
    assert (auth['run']=='v2' and auth['authenticated'] is True and auth['steady'] is True
            and auth['port']==443 and auth['host']=='217.142.241.226'
            and 0<=time.time()-auth['at']<120),'fresh_authentication_required'
    # Detect source/config drift from the original backed-up version.
    before=Path('/home/ubuntu/quote-guard-releases/ssh443-trial-local-20261031-v1/manifest.json')
    baseline=json.loads(before.read_text())
    paths={str(p) for p in [Path('/etc/systemd/system/ta-tushare-forward.service'),Path('/etc/ta-tushare-egress/ssh_config')]}
    for folder in ['/etc/systemd/system/ta-tushare-forward.service.d','/run/systemd/system/ta-tushare-forward.service.d']:
        paths.update(str(p) for p in Path(folder).glob('*.conf'))
    assert paths==set(baseline['files']),'unit_files_changed'
    assert all(hashlib.sha256(Path(k).read_bytes()).hexdigest()==v['sha256'] for k,v in baseline['files'].items()),'original_config_changed'
    base=['ssh','-G','-F','/etc/ta-tushare-egress/ssh_config']
    def effective(extra):
        d={}
        for line in run(base+extra+['osaka-tushare-egress']).splitlines():
            k,_,v=line.partition(' ');d.setdefault(k,[]).append(v)
        return d
    a=effective([]);b=effective(['-p','443','-o','HostKeyAlias=217.142.241.226'])
    assert {k for k in a.keys()|b.keys() if a.get(k)!=b.get(k)}=={'port','hostkeyalias'},'unexpected_config_change'
    assert b['port']==['443'] and b['stricthostkeychecking']==['true'],'trust_changed'
    root.mkdir(mode=0o700)
    manifest={'files':{}}
    for n,(source,entry) in enumerate(baseline['files'].items()):
        raw=Path(source).read_bytes();p=root/str(n);p.write_bytes(raw);p.chmod(0o600)
        assert p.read_bytes()==raw
        manifest['files'][source]={'sha256':entry['sha256'],'backup':str(p)}
    (root/'manifest.json').write_text(json.dumps(manifest));(root/'manifest.json').chmod(0o600)
    restore=root/'restore.py';restore.write_text(local_restore());restore.chmod(0o600)
    try:
        run(['systemd-run','--unit='+LOCAL_TIMER,'--on-active=480s',
             '--timer-property=AccuracySec=1s','--property=TimeoutStartSec=55',
             '/usr/bin/python3','-B',str(restore)])
        timer=run(['systemctl','show',LOCAL_TIMER+'.timer','-p','ActiveState','-p','SubState'])
        assert 'ActiveState=active' in timer and 'SubState=waiting' in timer,'restore_timer_not_armed'
        dropin.parent.mkdir(parents=True,exist_ok=True)
        fd=os.open(dropin,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'w') as f:f.write(LOCAL_OVERRIDE);f.flush();os.fsync(f.fileno())
        run(['systemctl','stop','ta-tushare-forward.service'])
        run(['systemctl','daemon-reload'])
        run(['systemctl','start','ta-tushare-forward.service'])
        print(json.dumps(dict(switched=True,restore_after_seconds=480)))
    except BaseException:
        print(run(['python3','-B','-c',local_restore()],timeout=55));raise


if __name__=='__main__':
    main()
