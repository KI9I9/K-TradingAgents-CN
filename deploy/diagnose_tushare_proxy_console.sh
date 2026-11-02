#!/bin/sh
# Read-only diagnostics; no HTTP, route/config/task/database changes.
sudo -n /usr/bin/python3 -B - <<'TA_PROXY_DIAG_PY'
import concurrent.futures, json, os, pathlib, stat, subprocess, time
os.umask(0o077)
root = pathlib.Path('/home/ubuntu/k-tradingagents-direct-route-20261103-v1')
def capture(args, timeout=10, cwd=None):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return dict(returncode=p.returncode, stdout=p.stdout, stderr=p.stderr)
    except subprocess.TimeoutExpired:
        return dict(timeout=True)
b = capture(['docker', 'inspect', 'ta-live-088a60b962bf-backend-1'])
if b.get('returncode') != 0:
    raise SystemExit('backend_inspect_failed')
b = json.loads(b['stdout'])[0]
if (b['Id'] != '5612c6d8dd6c50762b5d9bde8ad88e790b6217e276fa8b4d110ff9d5c61b8a69'
        or b['Image'] != 'sha256:29ad3c39036f0f2cd19e433c9153147ef19ae82ed908c9c1c8a8900641abc277'):
    raise SystemExit('wrong_host_or_backend_changed')
if root.resolve() != root or not root.is_dir() or root.stat().st_uid != 0 or stat.S_IMODE(root.stat().st_mode) != 0o700:
    raise SystemExit('invalid_existing_backup_directory')
tls = """import socket,ssl,json,time,sys
t=time.monotonic()
with socket.create_connection(('api.waditu.com',443),timeout=10) as raw:
 raw.settimeout(15)
 ctx=ssl.create_default_context();ctx.minimum_version=ssl.TLSVersion.TLSv1_2
 with ctx.wrap_socket(raw,server_hostname='api.waditu.com') as secure:
  print(json.dumps(dict(verified=True,tls=secure.version(),elapsed_s=round(time.monotonic()-t,3),python=sys.executable)))
"""
commands = {
    'root_system_python': ['/usr/bin/python3', '-B', '-c', tls],
    'proxy_default_python': ['runuser', '-u', 'ta-tushare-proxy', '--', 'python3', '-B', '-c', tls],
    'proxy_system_python': ['runuser', '-u', 'ta-tushare-proxy', '--', '/usr/bin/python3', '-B', '-c', tls],
}
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    results = dict(zip(commands, pool.map(lambda args: capture(args, 35), commands.values())))
results['proxy_system_clean_cwd'] = capture(commands['proxy_system_python'], 35, cwd='/')
results['proxy_identity'] = capture(['id', 'ta-tushare-proxy'])
rules = capture(['iptables-save', '-t', 'filter'])
results['output_rules'] = dict(returncode=rules.get('returncode'), stderr=rules.get('stderr'),
    relevant_lines=[line for line in rules.get('stdout', '').splitlines()
                    if '--uid-owner' in line or line.startswith((':OUTPUT ', '-A OUTPUT '))])
results['preparation_status'] = json.loads((root / 'status.json').read_text())
results['original_units'] = {name: (root / 'backup' / name).read_text() for name in ['0', '1']}
results.update(http_requests=0, route_changed=False, at=time.time())
path = root / ('diagnostic-' + str(time.time_ns()) + '.json')
with path.open('x') as f:
    json.dump(results, f); f.flush(); os.fsync(f.fileno())
print('PRODUCTION_IDENTITY_VERIFIED; HTTP requests=0; route unchanged')
print(json.dumps(dict(receipt=str(path), **results)))
TA_PROXY_DIAG_PY
