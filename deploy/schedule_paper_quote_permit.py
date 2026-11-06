"""Schedule a bounded future paper-quote permit without restarting backend.

The current permit is replaced atomically while the market is closed.  The
candidate is inactive until ``not_before``; no quote API is called by this
tool.  It is intended for the weekend gap between an expiring permit and the
next trading session.  A later in-session probe must still verify live stock
and ETF quotes before orders are considered usable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import subprocess
import time
import uuid

CN_TZ = timezone(timedelta(hours=8))
BACKEND = "ta-live-088a60b962bf-backend-1"
PERMIT = "paper-quote-supply.json"


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def parse_time(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("timezone_required")
    return parsed.astimezone(CN_TZ)


def run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(args, text=True, capture_output=True, timeout=45)
    if check and result.returncode:
        raise RuntimeError(f"command_failed:{args[0]}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--not-before", required=True,
                        help="Future CN time, for example 2026-11-09T09:30:00+08:00")
    parser.add_argument("--expires-at", required=True,
                        help="Absolute CN expiry, for example 2026-11-11T20:00:00+08:00")
    parser.add_argument("--previous-sha256", required=True)
    parser.add_argument("--execute", action="store_true", help="perform the atomic replacement")
    args = parser.parse_args()

    now = datetime.now(CN_TZ)
    start = parse_time(args.not_before)
    end = parse_time(args.expires_at)
    if len(args.previous_sha256) != 64 or any(c not in "0123456789abcdef" for c in args.previous_sha256):
        raise SystemExit("invalid_previous_sha256")
    if start <= now:
        raise SystemExit("not_before_must_be_future")
    if end <= start or (end - start).total_seconds() > 7 * 86400:
        raise SystemExit("permit_window_invalid")
    if start.weekday() >= 5:
        raise SystemExit("not_before_must_be_weekday")
    if start.time().hour != 9 or start.time().minute != 30:
        raise SystemExit("not_before_must_be_session_open")

    inspect = json.loads(run(["docker", "inspect", BACKEND]).stdout)[0]
    if not inspect["State"].get("Running") or inspect["State"].get("Health", {}).get("Status") != "healthy":
        raise SystemExit("backend_not_healthy")
    mounts = [m for m in inspect["Mounts"] if m.get("Destination") == "/app/runtime"]
    if len(mounts) != 1 or mounts[0].get("Type") != "bind" or mounts[0].get("RW") is not True:
        raise SystemExit("runtime_mount_invalid")
    host_path = Path(mounts[0]["Source"]) / PERMIT
    if host_path.is_symlink() or not host_path.is_file():
        raise SystemExit("permit_host_file_invalid")
    old = host_path.read_bytes()
    owner = host_path.stat()
    if digest(old) != args.previous_sha256:
        raise SystemExit("previous_permit_changed")
    old_doc = json.loads(old)
    if old_doc.get("version") != 2 or old_doc.get("enabled") is not True:
        raise SystemExit("previous_permit_not_v2_active")
    old_end = parse_time(old_doc["expires_at"])
    if old_end <= now:
        raise SystemExit("previous_permit_expired")

    candidate = json.dumps({
        "version": 2, "enabled": True, "source": "sina_l1",
        "scope": "paper_execution_sh_sz", "not_before": start.isoformat(),
        "expires_at": end.isoformat(), "activation_id": uuid.uuid4().hex,
        "renewal_of_sha256": digest(old),
    }, sort_keys=True, separators=(",", ":")).encode()

    result = {
        "ok": False, "check_only": not args.execute,
        "backend_id": inspect["Id"], "image": inspect["Image"],
        "mount_count": len(inspect["Mounts"]), "previous_sha256": digest(old),
        "candidate_sha256": digest(candidate), "not_before": start.isoformat(),
        "expires_at": end.isoformat(), "old_expires_at": old_end.isoformat(),
    }
    if not args.execute:
        result["status"] = "ready_no_replace"
        print(json.dumps(result, ensure_ascii=False))
        return 0

    backup_root = Path("/home/ubuntu/quote-guard-releases") / (
        "paper-quote-permit-schedule-" + time.strftime("%Y%m%d-%H%M%S"))
    backup_root.mkdir(mode=0o700)
    backup = backup_root / PERMIT
    backup.write_bytes(old)
    backup.chmod(0o600)
    (backup_root / "manifest.json").write_text(json.dumps({**result, "backup": str(backup)}, indent=2))
    (backup_root / "manifest.json").chmod(0o600)

    temporary = host_path.with_name("." + PERMIT + ".scheduled-" + uuid.uuid4().hex)
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(candidate); stream.flush(); os.fsync(stream.fileno())
        # The backend runs as UID 1000.  os.replace() otherwise leaves the
        # root-owned temporary inode unreadable to the application.
        os.chown(temporary, owner.st_uid, owner.st_gid)
        os.chmod(temporary, owner.st_mode & 0o777)
        if host_path.read_bytes() != old:
            raise RuntimeError("permit_changed_before_replace")
        os.replace(temporary, host_path)
        directory_fd = os.open(host_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if host_path.read_bytes() != candidate:
            raise RuntimeError("candidate_readback_failed")
        verify = run(["docker", "exec", BACKEND, "python3", "-B", "-c",
                      "import json; from app.services.paper_quote_supply import cn_now,permit_status; "
                      "p=json.load(open('/app/runtime/paper-quote-supply.json')); "
                      "print(json.dumps({'sha256':__import__('hashlib').sha256(open('/app/runtime/paper-quote-supply.json','rb').read()).hexdigest(),'status':permit_status(cn_now())[1],'not_before':p['not_before'],'expires_at':p['expires_at']}))"]).stdout.strip()
        result.update(ok=True, status="scheduled", backup=str(backup_root), verification=json.loads(verify))
        (backup_root / "completion.json").write_text(json.dumps(result, indent=2))
        (backup_root / "completion.json").chmod(0o600)
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        if host_path.read_bytes() == candidate:
            host_path.write_bytes(old); os.chown(host_path, owner.st_uid, owner.st_gid)
            os.chmod(host_path, owner.st_mode & 0o777)
        raise
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
