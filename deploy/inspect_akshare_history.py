"""Read-only AKShare history inventory.

The command does not call AKShare, trigger a scheduler job, alter scheduler
preferences, or write MongoDB. It is intended to be run from the devbox when
the production SSH route is available.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess


REPO = Path(__file__).resolve().parents[1]
ENTRY_HASH = "d75d2ac13363b5b46284684d39c05c22c79886903174a0e09552175537ecf82b"
FILES = (
    "app/worker/akshare_sync_service.py",
    "app/services/historical_data_service.py",
)


INSIDE = r'''
import hashlib, json, sys, types
from pathlib import Path
from pymongo import MongoClient

request = json.load(sys.stdin)
runtime = types.ModuleType("runtime")
exec(request["runtime"], runtime.__dict__)
env, effective = runtime.reconstruct(
    "/app/deploy/migration_production_entrypoint.py",
    request["entry_hash"],
    "/run/secrets",
)
client = MongoClient(
    host=env["MONGODB_HOST"], port=int(env["MONGODB_PORT"]),
    username=env["MONGODB_USERNAME"], password=env["MONGODB_PASSWORD"],
    authSource=env.get("MONGODB_AUTH_SOURCE", "admin"),
    retryWrites=False, retryReads=False,
    serverSelectionTimeoutMS=5000, socketTimeoutMS=10000,
)
try:
    db = client[env["MONGODB_DATABASE"]]
    collection = db.stock_daily_quotes
    match = {"data_source": "akshare", "period": "daily"}
    summary = next(collection.aggregate([
        {"$match": match},
        {"$group": {
            "_id": None, "count": {"$sum": 1},
            "first": {"$min": "$trade_date"},
            "last": {"$max": "$trade_date"},
            "symbols": {"$addToSet": "$symbol"},
        }},
        {"$project": {
            "_id": 0, "count": 1, "first": 1, "last": 1,
            "symbol_count": {"$size": "$symbols"},
        }},
    ], maxTimeMS=15000), {
        "count": 0, "first": None, "last": None, "symbol_count": 0,
    })
    latest = summary.get("last")
    latest_summary = {"trade_date": latest, "count": 0, "symbol_count": 0}
    if latest:
        latest_summary = next(collection.aggregate([
            {"$match": {**match, "trade_date": latest}},
            {"$group": {"_id": None, "count": {"$sum": 1}, "symbols": {"$addToSet": "$symbol"}}},
            {"$project": {"_id": 0, "trade_date": {"$literal": latest}, "count": 1,
                          "symbol_count": {"$size": "$symbols"}}},
        ], maxTimeMS=15000), latest_summary)
    job = db.scheduler_executions.find_one(
        {"job_id": "akshare_historical_sync"},
        sort=[("timestamp", -1)],
        projection={"_id": 0, "status": 1, "timestamp": 1, "updated_at": 1,
                    "finished_at": 1, "progress": 1, "processed_items": 1,
                    "total_items": 1, "success_count": 1, "error_count": 1,
                    "no_data_count": 1, "message": 1, "error_message": 1},
    ) or {}
    preference = db.scheduler_preferences.find_one(
        {"_id": "akshare_historical_sync"}, {"_id": 0, "enabled": 1}
    ) or {}
    source_hashes = {
        path: hashlib.sha256((Path("/app") / path).read_bytes()).hexdigest()
        for path in request["files"]
    }
    print("AKSHARE_HISTORY_INVENTORY:" + json.dumps({
        "summary": summary,
        "latest_day": latest_summary,
        "latest_execution": job,
        "preference": preference,
        "source_hashes": source_hashes,
        "quote_gates": effective["gates"],
    }, default=str))
finally:
    client.close()
'''


HOST = r'''
import json, subprocess, sys
request = json.load(sys.stdin)
inspect = json.loads(subprocess.check_output(
    ["docker", "inspect", "ta-live-088a60b962bf-backend-1"],
    text=True, timeout=10,
))[0]
proc = subprocess.run(
    ["docker", "exec", "-i", inspect["Id"], "python3", "-B", "-c", request["inside"]],
    input=json.dumps(request["request"]), text=True,
    capture_output=True, timeout=35,
)
lines = [line.split(":", 1)[1] for line in proc.stdout.splitlines()
         if line.startswith("AKSHARE_HISTORY_INVENTORY:")]
if proc.returncode or len(lines) != 1:
    raise SystemExit("akshare_inventory_failed")
print(json.dumps({
    "backend": {
        "id": inspect["Id"], "image": inspect["Image"],
        "health": inspect["State"].get("Health", {}).get("Status"),
        "mount_count": len(inspect["Mounts"]),
    },
    "inside": json.loads(lines[0]),
}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--control-socket", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("output_already_exists")
    request = {
        "inside": INSIDE,
        "request": {
            "runtime": (REPO / "deploy/quote_guard_runtime.py").read_text(),
            "entry_hash": ENTRY_HASH,
            "files": list(FILES),
        },
    }
    command = [
        "ssh", "-F", "/home/ubuntu/.ssh/osaka-migration-20261016-hKbxOKhx/config",
        "-S", str(args.control_socket) if args.control_socket else "none",
        "-C", "-o", "BatchMode=yes", "-o", "IPQoS=none",
        "-o", "ConnectTimeout=20", "osaka-migration",
        "sudo -n python3 -B -c " + shlex.quote(HOST),
    ]
    result = subprocess.run(command, input=json.dumps(request), text=True,
                            capture_output=True, timeout=65)
    if result.returncode:
        raise SystemExit("inspection_failed_no_production_mutation")
    report = json.loads(result.stdout)
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
    inside = report["inside"]
    print(json.dumps({
        "health": report["backend"]["health"],
        "mount_count": report["backend"]["mount_count"],
        "summary": inside["summary"],
        "latest_day": inside["latest_day"],
        "latest_execution": inside["latest_execution"],
        "preference": inside["preference"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
