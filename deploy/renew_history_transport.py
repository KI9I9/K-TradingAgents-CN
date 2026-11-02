"""Back up/check/renew the existing fixed-route transport, without new upstream calls.

Run with sudo on devbox. Prepare is reversible backups only. Apply requires the
separately approved finite window; original service, keys, TLS and route stay intact.
"""
from datetime import datetime,timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT=Path('/home/ubuntu/quote-guard-releases/history-schedule-transport-v2-20261102')
AUTH=Path('/etc/ta-tushare-egress/authorization.json')
TIMER=Path('/etc/systemd/system/ta-tushare-expire.timer')
WATCH=(AUTH,TIMER,Path('/etc/ta-tushare-egress/check_deadline.py'),
       Path('/etc/systemd/system/ta-tushare-forward.service'),
       Path('/etc/systemd/system/ta-tushare-forward.service.d/transport-resilience.conf'),
       Path('/etc/systemd/system/ta-tushare-expire.service'),
       Path('/etc/ta-tushare-egress/ssh_config'))
EXPIRY=int(datetime(2026,11,8,6,tzinfo=timezone.utc).timestamp())
WINDOW=dict(schema=1,purpose='approved_tushare_channel_only',not_before=EXPIRY-604800,
            expires_at=EXPIRY,start_utc='2026-11-01T06:00:00+00:00',expiry_utc='2026-11-08T06:00:00+00:00')
NEW_TIMER=b'''[Unit]
Description=Fixed expiry for approved Tushare SSH transport

[Timer]
OnCalendar=2026-11-08 06:00:00 UTC
Persistent=true
AccuracySec=1s
RandomizedDelaySec=0
Unit=ta-tushare-expire.service

[Install]
WantedBy=timers.target
'''


def sha(raw):return hashlib.sha256(raw).hexdigest()


def run(argv):return subprocess.check_output(argv,text=True,timeout=20,stderr=subprocess.DEVNULL).strip()


def timer_deadline_usec():
    # Never parse systemctl's localized CST string: GNU date interprets CST as
    # America/Chicago, even when the host uses Asia/Shanghai (14 hours apart).
    value=run(['busctl','get-property','org.freedesktop.systemd1',
        '/org/freedesktop/systemd1/unit/ta_2dtushare_2dexpire_2etimer',
        'org.freedesktop.systemd1.Timer','NextElapseUSecRealtime']).split()
    assert len(value)==2 and value[0]=='t' and value[1].isdigit(),'timer_deadline_invalid'
    return int(value[1])


def private(name,raw):
    path=ROOT/name;path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    return sha(raw)


def replace(path,raw,mode,uid,gid):
    tmp=path.with_name(path.name+'.schedule-next')
    fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw);stream.flush();os.fchown(stream.fileno(),uid,gid)
        os.fchmod(stream.fileno(),mode);os.fsync(stream.fileno())
    os.replace(tmp,path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action',choices=['prepare','apply','status'],required=True)
    args=parser.parse_args();os.umask(0o077);assert os.geteuid()==0,'sudo_required'
    if args.action=='status':
        print(json.dumps(dict(prepared=(ROOT/'manifest.json').exists(),
            result=json.loads((ROOT/'result.json').read_text()) if (ROOT/'result.json').exists() else None,
            active=run(['systemctl','is-active','ta-tushare-forward.service']),
            deadline=json.loads(AUTH.read_text())['expiry_utc'])));return
    if args.action=='prepare':
        assert not ROOT.exists(),'already_prepared'
        assert run(['systemctl','is-active','ta-tushare-forward.service'])=='active','transport_not_active'
        run(['runuser','-u','ta-tushare-forward','--','python3','-B',str(WATCH[2]),str(AUTH)])
        assert timer_deadline_usec()>0,'timer_not_scheduled'
        ROOT.mkdir(mode=0o700)
        files={}
        for path in WATCH:
            assert path.is_file() and not path.is_symlink(),'unexpected_config_path'
            info=path.stat();raw=path.read_bytes()
            files[str(path)]=dict(sha256=private('backup/'+str(path).lstrip('/'),raw),mode=info.st_mode&0o777,uid=info.st_uid,gid=info.st_gid)
        private('candidate/authorization.json',json.dumps(WINDOW,indent=2).encode())
        private('candidate/ta-tushare-expire.timer',NEW_TIMER)
        assert WINDOW['not_before']<=time.time()<WINDOW['expires_at'],'candidate_window_invalid'
        run(['python3','-B',str(WATCH[2]),str(ROOT/'candidate/authorization.json')])
        run(['systemd-analyze','verify',str(ROOT/'candidate/ta-tushare-expire.timer')])
        manifest=dict(schema=1,files=files,candidate=WINDOW,timer_sha256=sha(NEW_TIMER),
                      old_deadline=json.loads(AUTH.read_text())['expiry_utc'])
        identity=private('manifest.json',json.dumps(manifest,indent=2).encode())
        print(json.dumps(dict(prepared=True,manifest_sha256=identity,old_deadline=manifest['old_deadline'],new_deadline=WINDOW['expiry_utc'],transport_changed=False)));return
    manifest=json.loads((ROOT/'manifest.json').read_text())
    assert manifest['candidate']==WINDOW and manifest['timer_sha256']==sha(NEW_TIMER),'candidate_changed'
    for path,entry in manifest['files'].items():
        path=Path(path);info=path.stat()
        assert (sha(path.read_bytes())==entry['sha256'] and info.st_mode&0o777==entry['mode']
                and info.st_uid==entry['uid'] and info.st_gid==entry['gid']),'config_drift'
    assert run(['systemctl','is-active','ta-tushare-forward.service'])=='active','transport_not_active'
    assert WINDOW['not_before']<=time.time()<WINDOW['expires_at'],'candidate_expired'
    private('apply-reserved',str(time.time()).encode())
    applied=False
    try:
        applied=True
        for path,raw in ((AUTH,json.dumps(WINDOW,indent=2).encode()),(TIMER,NEW_TIMER)):
            entry=manifest['files'][str(path)]
            replace(path,raw,entry['mode'],entry['uid'],entry['gid'])
        run(['python3','-B',str(WATCH[2]),str(AUTH)])
        run(['runuser','-u','ta-tushare-forward','--','python3','-B',str(WATCH[2]),str(AUTH)])
        run(['systemd-analyze','verify',str(TIMER)])
        run(['systemctl','daemon-reload']);run(['systemctl','restart','ta-tushare-expire.timer'])
        assert run(['systemctl','is-active','ta-tushare-expire.timer'])=='active','timer_not_active'
        assert timer_deadline_usec()==EXPIRY*1000000,'timer_deadline_mismatch'
        assert run(['systemctl','is-active','ta-tushare-forward.service'])=='active','transport_not_active_after_reload'
        for path in WATCH[2:]:assert sha(path.read_bytes())==manifest['files'][str(path)]['sha256'],'fixed_transport_changed'
        result=dict(status='renewed',expires_at=EXPIRY,expiry_utc=WINDOW['expiry_utc'],route_keys_tls_unchanged=True)
    except BaseException:
        result=dict(status='failed',restored=False)
        if applied:
            for path in (AUTH,TIMER):
                raw=(ROOT/'backup'/str(path).lstrip('/')).read_bytes()
                entry=manifest['files'][str(path)]
                replace(path,raw,entry['mode'],entry['uid'],entry['gid'])
            run(['systemctl','daemon-reload']);run(['systemctl','restart','ta-tushare-expire.timer'])
            result['restored']=all(sha(p.read_bytes())==manifest['files'][str(p)]['sha256'] for p in (AUTH,TIMER))
    private('result.json',json.dumps(result).encode());print(json.dumps(result))
    if result['status']!='renewed':raise SystemExit(1)


if __name__=='__main__':main()
