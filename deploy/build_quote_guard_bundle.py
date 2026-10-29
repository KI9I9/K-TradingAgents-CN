#!/usr/bin/env python3
"""冻结本地行情修复交付物；不联网、不调用 Docker、不操作生产。

导出当前工作区的已审范围，manifest 记录每个文件的精确哈希。
HEAD 仅用于溯源，不能代替未提交修改的内容哈希。前端 dist 另行构建验收。
"""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile


BASELINE = "9dd06bd"
REPLACEMENTS = {
    "app/core/config.py": "/home/ubuntu/k-tradingagents-cron-fix-20261025-v1/config.py",
    "tradingagents/dataflows/providers/china/akshare.py": (
        "/home/ubuntu/k-tradingagents-sync-routing-fix-20261022-v2/"
        "tradingagents/dataflows/providers/china/akshare.py"),
    "app/worker/akshare_sync_service.py": (
        "/home/ubuntu/k-tradingagents-akshare-batch-20261023-v1/"
        "app/worker/akshare_sync_service.py"),
}
BIND_ADDITIONS = (
    "app/services/tushare_history_batches.py",
    "tradingagents/utils/tushare_batch.py",
    "deploy/history_batch_adapters.py",
    "deploy/history_batch_journal.py",
    "deploy/history_sync_plan.py",
    "app/services/tushare_manual_sync.py",
    "app/services/scheduler_service.py",
    "app/routers/scheduler.py",
    "app/routers/paper.py",
    "app/services/quote_contract.py",
    "app/routers/stock_sync.py",
    "app/services/quotes_ingestion_service.py",
    "app/services/stock_data_service.py",
    "app/worker/qmt_sync_service.py",
    "app/services/historical_contract.py",
    "app/services/historical_data_service.py",
    "app/worker/tushare_sync_service.py",
    "tradingagents/dataflows/providers/china/tushare.py",
)
BACKEND_FILES = tuple(REPLACEMENTS) + BIND_ADDITIONS
FRONTEND_SOURCE = "frontend/src/views/PaperTrading/index.vue"
TOOLS = ("deploy/verify_compose_merge.py", "deploy/quote_guard_smoke.py",
         "deploy/quote_guard_runtime.py")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def changed_application_files(repo):
    paths = ("app/", "tradingagents/", "frontend/")
    tracked = git(repo, "diff", "--name-only", "-z", BASELINE, "--", *paths)
    untracked = git(repo, "ls-files", "--others", "--exclude-standard", "-z", "--", *paths)
    return {p.decode() for p in (tracked + untracked).split(b"\0") if p}


def check_coverage(changed):
    missing = set(changed) - set(BACKEND_FILES) - {FRONTEND_SOURCE}
    if missing:
        raise ValueError(f"交付清单未覆盖应用修改，先审查并更新清单: {sorted(missing)}")


def backend_override(release_dir):
    root = Path(release_dir)
    if not root.is_absolute():
        raise ValueError("生产暂存目录必须是绝对路径")
    return {"services": {"backend": {"volumes": [
        {"type": "bind", "source": str(root / path), "target": "/app/" + path,
         "read_only": True, "bind": {"create_host_path": False}}
        for path in BIND_ADDITIONS
    ]}}}


def attach_frontend(repo, destination, archive, report_path):
    """只接受与当前源码相符的已构建产物；逐文件展开，不信任 tar 路径/链接。"""
    report = json.loads(Path(report_path).read_text())
    expected_sources = {"src/views/PaperTrading/index.vue", "package.json", "package-lock.json"}
    if (report.get("command") != "npm run build" or report.get("exit_code") != 0
            or set(report.get("source", {})) != expected_sources):
        raise ValueError("前端构建证据不完整")
    for path, digest in report["source"].items():
        if sha256((Path(repo) / "frontend" / path).read_bytes()) != digest:
            raise ValueError("前端构建源码与当前工作区不符")
    if sha256(Path(archive).read_bytes()) != report.get("sha256"):
        raise ValueError("前端归档哈希不符")
    if sha256(Path(report["build_log"]).read_bytes()) != report.get("build_log_sha256"):
        raise ValueError("前端构建日志哈希不符")
    root = destination / "frontend-dist"
    root.mkdir(exist_ok=False)
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        seen = set()
        if sum(m.size for m in members) > 256 * 1024 * 1024:
            raise ValueError("前端展开大小超限")
        for member in members:
            path = PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or ".." in path.parts
                    or str(path) != member.name or member.name in seen):
                raise ValueError("前端归档含非普通文件、不安全路径或重复条目")
            seen.add(member.name)
        for member in members:
            target = root / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as stream:
                target.write_bytes(stream.read())
            target.chmod(0o644)
    if sha256((root / "index.html").read_bytes()) != report.get("index_sha256"):
        raise ValueError("前端入口哈希不符")
    (destination / "frontend-build.json").write_text(json.dumps(report, indent=2) + "\n")
    (destination / "frontend-build.log").write_bytes(Path(report["build_log"]).read_bytes())
    return {"archive_sha256": report["sha256"], "index_sha256": report["index_sha256"],
            "files": len(seen), "source": report["source"]}


def delivery_override(release_dir, frontend=False):
    override = backend_override(release_dir)
    if frontend:
        override["services"]["frontend"] = {"volumes": [
            {"type": "bind", "source": str(Path(release_dir) / "frontend-dist"),
             "target": "/usr/share/nginx/html", "read_only": True,
             "bind": {"create_host_path": False}}]}
    return override


def write_checksums(destination):
    lines = [f"{sha256(path.read_bytes())}  {path.relative_to(destination)}\n"
             for path in sorted(destination.rglob("*"))
             if path.is_file() and path != destination / "SHA256SUMS"]
    (destination / "SHA256SUMS").write_text("".join(lines), encoding="utf-8")


def build_bundle(repo, destination):
    repo, destination = Path(repo).resolve(), Path(destination).resolve()
    check_coverage(changed_application_files(repo))
    # 新目录才允许导出，防止重试覆盖已经核验/部署的发布物。
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema": 1, "baseline": BASELINE,
        "source_head": git(repo, "rev-parse", "HEAD").decode().strip(),
        "source_dirty": bool(git(repo, "status", "--porcelain")),
        "purpose": "CN/ETF guard and Tushare history contract; supply disabled, not trading recovery",
        "frontend_built": False,
        "frontend_source_sha256": sha256((repo / FRONTEND_SOURCE).read_bytes()),
        "entrypoint_sha256": sha256((repo / "deploy/migration_production_entrypoint.py").read_bytes()),
        "files": [],
    }
    for path in (*BACKEND_FILES, *TOOLS):
        data = (repo / path).read_bytes()
        target = destination / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        item = {"path": path, "sha256": sha256(data)}
        if path in BACKEND_FILES:
            item.update(target="/app/" + path,
                        mode="replace" if path in REPLACEMENTS else "bind")
            if path in REPLACEMENTS:
                item["host_source"] = REPLACEMENTS[path]
            if git(repo, "ls-tree", "--name-only", BASELINE, "--", path).strip():
                item["baseline_sha256"] = sha256(git(repo, "show", f"{BASELINE}:{path}"))
        else:
            item["mode"] = "tool"
        manifest["files"].append(item)
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [f"{item['sha256']}  {item['path']}\n" for item in manifest["files"]]
    lines.append(f"{sha256((destination / 'manifest.json').read_bytes())}  manifest.json\n")
    (destination / "SHA256SUMS").write_text("".join(lines), encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True, help="尚不存在的本地发布目录")
    parser.add_argument("--release-dir", help="仅生成 override JSON；不操作该生产目录")
    parser.add_argument("--frontend-archive", type=Path)
    parser.add_argument("--frontend-report", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    archive = output.with_name(output.name + ".tgz")
    checksum = archive.with_name(archive.name + ".sha256")
    if archive.exists() or checksum.exists():
        parser.error("归档或校验文件已存在，请使用新的发布目录")
    if bool(args.frontend_archive) != bool(args.frontend_report):
        parser.error("前端归档与构建报告必须一起提供")
    override = delivery_override(args.release_dir, bool(args.frontend_archive)) if args.release_dir else None
    manifest = build_bundle(args.repo, output)
    if args.frontend_archive:
        manifest["frontend"] = attach_frontend(args.repo, output, args.frontend_archive, args.frontend_report)
        manifest["frontend_built"] = True
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    if override is not None:
        override_path = output / "compose.paper-guard.json"
        override_path.write_text(json.dumps(override, indent=2) + "\n", encoding="utf-8")
    write_checksums(output)
    with tarfile.open(archive, "w:gz") as bundle:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                bundle.add(path, arcname=str(path.relative_to(output)))
    checksum.write_text(f"{sha256(archive.read_bytes())}  {archive.name}\n", encoding="utf-8")
    print(f"Backend files: {len(BACKEND_FILES)}; replacements: 3; new binds: {len(BIND_ADDITIONS)}")
    print(f"Source HEAD: {manifest['source_head']}; dirty: {manifest['source_dirty']}")
    print(f"Bundle: {archive}\nChecksum: {checksum}")
    print(f"前端已打包: {manifest['frontend_built']}；此包本身不代表获准部署。")


if __name__ == "__main__":
    main()
