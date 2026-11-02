"""Build a human-readable console script; never contacts production itself."""
import argparse
import hashlib
import os
from pathlib import Path

from run_tushare_route_assessment import PROBE, ROOT, START, STATUS

BACKEND_ID = '5612c6d8dd6c50762b5d9bde8ad88e790b6217e276fa8b4d110ff9d5c61b8a69'
IMAGE = 'sha256:29ad3c39036f0f2cd19e433c9153147ef19ae82ed908c9c1c8a8900641abc277'


def build():
    # These are our fixed Python sources, not user input. Keep the shell artifact
    # readable rather than executing an opaque encoded download as root.
    for source in (PROBE, START, STATUS):
        if "'''" in source:
            raise ValueError('triple_quote_in_source')
    code = f'''import hashlib,io,json,pathlib,subprocess,sys,time
backend=json.loads(subprocess.check_output(['docker','inspect','ta-live-088a60b962bf-backend-1'],timeout=10))[0]
if backend['Id']!={BACKEND_ID!r} or backend['Image']!={IMAGE!r}:raise SystemExit('wrong_host_or_backend_changed')
if not backend['State']['Running'] or backend['State'].get('Health',{{}}).get('Status')!='healthy':raise SystemExit('backend_not_healthy')
if len(backend['Mounts'])!=44:raise SystemExit('mounts_changed')
print('PRODUCTION_IDENTITY_VERIFIED; TLS only; HTTP requests=0',flush=True)
PROBE=r\x27\x27\x27{PROBE}\x27\x27\x27
START=r\x27\x27\x27{START}\x27\x27\x27
STATUS=r\x27\x27\x27{STATUS}\x27\x27\x27
root=pathlib.Path({ROOT!r})
if root.exists():
 print('EXISTING_ASSESSMENT: collect only, no restart',flush=True)
else:
 request=dict(root=str(root),probe=PROBE,sha256=hashlib.sha256(PROBE.encode()).hexdigest())
 sys.stdin=io.StringIO(json.dumps(request))
 exec(compile(START,'route_assessment_start','exec'))
 sys.stdin=sys.__stdin__
deadline=time.monotonic()+110
while time.monotonic()<deadline:
 if (root/'result.json').exists():break
 if (root/'status.json').exists() and json.loads((root/'status.json').read_text()).get('stage')=='failed':break
 time.sleep(1)
exec(compile(STATUS,'route_assessment_status','exec'))
'''
    compile(code, 'route_assessment_console', 'exec')
    return "#!/bin/sh\n# Paste this entire file into a terminal on the PRODUCTION HOST.\n# No credentials, no HTTP, no changes to routes, tasks, databases or app files.\nsudo -n python3 -B - <<'TA_ROUTE_ASSESSMENT_PY'\n" + code + "\nTA_ROUTE_ASSESSMENT_PY\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raw = build().encode()
    fd = os.open(args.output, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    print('sha256=' + hashlib.sha256(raw).hexdigest())
    print('path=' + str(args.output.resolve()))


if __name__ == '__main__':
    main()
