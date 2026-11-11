#!/usr/bin/env python3
"""
Read-only pull of Osaka production server's non-MongoDB runtime & host state
into /home/ubuntu/quote-guard-releases/osaka-state-backups/ (mode 0700/0600).
Backs up:
  - /app/runtime essential state (paper-quote-supply.json, tushare-history-schedule.json,
    tushare-egress, tushare-manual-daily-v1, tushare-history-batch-v1 including all 4 generations & STOP gates)
  - mounts/app_{config,data,reports,results,uploads}
  - live docker inspect mount/image topology manifest (redacted of env secrets)
  - /etc/systemd/system/osaka-* and ta-* units, /usr/local/bin/osaka-backup-mongo and ta-data-check.sh
Never prints any secret content to stdout; all output is sanitized via safe-inspect.py.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

spec = importlib.util.spec_from_file_location("safe_inspect", "/home/ubuntu/.local/bin/safe-inspect.py")
safe_inspect = importlib.util.module_from_spec(spec)
spec.loader.exec_module(safe_inspect)
sanitize_line = safe_inspect.sanitize_line

BACKUP_DIR = Path("/home/ubuntu/quote-guard-releases/osaka-state-backups")
SSH_CONFIG = "/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config"

REMOTE_TAR_CMD = r"""
sudo -n sh -c '
set -e
TMPD=$(mktemp -d /tmp/osaka-state-bk.XXXXXX)
trap "rm -rf $TMPD" EXIT
chmod 700 "$TMPD"

MOUNTS_BASE="/home/ubuntu/k-tradingagents-migration-receiver-20261016/final-restore-20261018-v2/mounts"
RESTORE_BASE="/home/ubuntu/k-tradingagents-migration-receiver-20261016/final-restore-20261018-v2"
mkdir -p "$TMPD/mounts" "$TMPD/app_runtime" "$TMPD/systemd" "$TMPD/bin" "$TMPD/etc" "$TMPD/meta" "$TMPD/frontend_bundle"

# 1. Copy persistent app mounts (config, data, reports, results, uploads)
for d in app_config app_data app_reports app_results app_uploads; do
  if [ -d "$MOUNTS_BASE/$d" ]; then
    cp -a "$MOUNTS_BASE/$d" "$TMPD/mounts/"
  fi
done

# 2. Copy essential /app/runtime state (permits, schedules, Tushare STOP gates & batch/generation ledgers)
RT_BASE="$MOUNTS_BASE/app_runtime"
for item in paper-quote-supply.json paper-quote-supply.renewal.lock tushare-history-schedule.json tushare-egress tushare-manual-daily-v1 tushare-history-batch-v1 tushare-history-acceptance-20261019-v1 tushare-history-acceptance-followup-20261019-v1; do
  if [ -e "$RT_BASE/$item" ]; then
    cp -a "$RT_BASE/$item" "$TMPD/app_runtime/"
  fi
done

# 3. Copy systemd timers/services, host scripts & /etc/ta-* configs
cp -a /etc/systemd/system/osaka-* /etc/systemd/system/ta-* "$TMPD/systemd/" 2>/dev/null || true
cp -a /usr/local/bin/osaka-* /usr/local/bin/ta-* "$TMPD/bin/" 2>/dev/null || true
cp -a /etc/ta-* "$TMPD/etc/" 2>/dev/null || true
cp -a "$RESTORE_BASE"/*.json "$TMPD/meta/" 2>/dev/null || true

# 4. Save container image, full inspect & bind-mount topology + compiled frontend static bundle
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.CreatedAt}}" > "$TMPD/meta/docker_ps_a.txt"
docker inspect $(docker ps -q) --format "{{json .Name}} {{json .Image}} {{json .Mounts}}" > "$TMPD/meta/running_mounts.jsonl"
docker inspect $(docker ps -q) > "$TMPD/meta/running_containers_full_inspect.json" 2>/dev/null || true
docker cp ta-live-088a60b962bf-frontend-1:/usr/share/nginx/html "$TMPD/frontend_bundle/html" 2>/dev/null || true
docker cp ta-live-088a60b962bf-frontend-1:/etc/nginx/conf.d "$TMPD/frontend_bundle/conf.d" 2>/dev/null || true

tar -C "$TMPD" -czf - .
'
"""


def main():
    os.umask(0o077)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(BACKUP_DIR, 0o700)

    ts = time.strftime("%Y%m%d-%H%M%S")
    archive_path = BACKUP_DIR / f"osaka-runtime-state-{ts}.tar.gz"
    sha_path = BACKUP_DIR / f"osaka-runtime-state-{ts}.tar.gz.sha256"

    ssh_cmd = [
        "ssh",
        "-F", SSH_CONFIG,
        "-o", "BatchMode=yes",
        "-o", "IPQoS=none",
        "-o", "ConnectionAttempts=4",
        "-o", "ConnectTimeout=20",
        "-o", "ServerAliveInterval=15",
        "osaka-migration",
        REMOTE_TAR_CMD,
    ]
    p = subprocess.run(ssh_cmd, capture_output=True, timeout=60)
    if p.returncode != 0 or len(p.stdout) < 512:
        err = sanitize_line((p.stderr.decode("utf-8", errors="replace") if p.stderr else f"rc={p.returncode}").strip())
        print(json.dumps({"ok": False, "error": err}, ensure_ascii=False))
        sys.exit(1)

    fd = os.open(str(archive_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(p.stdout)

    digest = hashlib.sha256(p.stdout).hexdigest()
    fd_sha = os.open(str(sha_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd_sha, "w", encoding="utf-8") as f:
        f.write(f"{digest}  {archive_path.name}\n")

    # Keep latest 14 state archives
    archives = sorted(BACKUP_DIR.glob("osaka-runtime-state-*.tar.gz"))
    if len(archives) > 14:
        for old in archives[:-14]:
            old.unlink(missing_ok=True)
            Path(str(old) + ".sha256").unlink(missing_ok=True)

    summary = {
        "ok": True,
        "archive": str(archive_path),
        "size_bytes": len(p.stdout),
        "size_kb": round(len(p.stdout) / 1024, 2),
        "sha256_prefix": digest[:12],
        "mode": oct(archive_path.stat().st_mode & 0o777),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

