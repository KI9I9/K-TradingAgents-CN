"""Freeze the on-demand quote candidate locally; never deploy or grant access."""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile

REPO = Path(__file__).resolve().parents[1]
BASELINE = "f9e37f2"
FILES = ("app/routers/paper.py", "app/services/quote_contract.py", "app/services/paper_quote_supply.py")
CALENDAR = "app/services/trading_calendar_service.py"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def git(*args):
    return subprocess.check_output(["git", "-C", str(REPO), *args])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if git("status", "--porcelain").strip():
        raise SystemExit("commit_before_freeze")
    output = args.output.resolve()
    if output.exists() or output.with_suffix(".tgz").exists():
        raise SystemExit("immutable_output_already_exists")
    changed = set(git("diff", "--name-only", BASELINE, "--", "app/", "tradingagents/", "frontend/").decode().splitlines())
    if changed != set(FILES):
        raise SystemExit("application_delivery_scope_changed")
    os.umask(0o077)
    output.mkdir(mode=0o700)
    manifest = dict(source_head=git("rev-parse", "HEAD").decode().strip(), baseline=BASELINE,
                    files=[], mounts_before=43, mounts_after=44,
                    fresh_production_preflight="required_not_yet_passed",
                    grant_installed=False, business_database_writes=0,
                    calendar_baseline_sha256=sha(git("show", BASELINE+":"+CALENDAR)))
    production_root = "/home/ubuntu/k-tradingagents-paper-supply-20261102/staged"
    volumes = []
    for path in FILES:
        raw = (REPO/path).read_bytes()
        ast.parse(raw)
        file = output/path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(raw)
        baseline = sha(git("show", BASELINE+":"+path)) if path != FILES[-1] else None
        manifest["files"].append(dict(path=path, target="/app/"+path, sha256=sha(raw),
                                      baseline_sha256=baseline, mode="new_bind" if baseline is None else "replace_bind"))
        volumes.append(dict(type="bind", source=production_root+"/"+path, target="/app/"+path,
                            read_only=True, bind={"create_host_path": False}))
    for path in ("deploy/inspect_paper_quote_supply.py", "deploy/probe_paper_quote_supply.py",
                 "deploy/verify_compose_merge.py", "docs/review-20261027/模拟交易按需行情恢复-20261102.md"):
        file = output/path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes((REPO/path).read_bytes())
    (output/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    (output/"compose.paper-supply.json").write_text(json.dumps({"services": {"backend": {"volumes": volumes}}}, indent=2)+"\n")
    (output/"permit.disabled.example.json").write_text(json.dumps(dict(
        version=1, enabled=False, source="sina_l1", scope="paper_execution_sh_sz",
        not_before="REQUIRES_SEPARATE_ACTIVATION", expires_at="REQUIRES_SEPARATE_ACTIVATION"), indent=2)+"\n")
    members = sorted(p for p in output.rglob("*") if p.is_file())
    (output/"SHA256SUMS").write_text("".join(sha(p.read_bytes())+"  "+str(p.relative_to(output))+"\n" for p in members))
    archive = output.with_suffix(".tgz")
    with tarfile.open(archive, "x:gz") as bundle:
        for p in sorted(output.rglob("*")):
            if p.is_file():
                bundle.add(p, arcname=str(p.relative_to(output)), recursive=False)
    archive.with_suffix(".tgz.sha256").write_text(sha(archive.read_bytes())+"\n")
    print(json.dumps(dict(source_head=manifest["source_head"], sha256=sha(archive.read_bytes()),
                          archive=str(archive), deployment_ready=False, reason="fresh_production_preflight_required")))


if __name__ == "__main__":
    main()
