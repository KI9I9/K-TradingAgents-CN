"""部署预检的离线反例；不执行 Docker/SSH，不读取生产配置。"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


merge = load("merge_check", "deploy/verify_compose_merge.py")
runtime = load("runtime_check", "deploy/quote_guard_runtime.py")
entrypoint = ROOT / "deploy/migration_production_entrypoint.py"
ENTRY_HASH = hashlib.sha256(entrypoint.read_bytes()).hexdigest()


def fixture():
    volume = {"type": "bind", "source": "/old", "target": "/app/old", "read_only": True}
    service = {"image": "app:stable", "volumes": [volume], "environment": {"KEY": "value"}}
    before = {"name": "project", "services": {"backend": service, "frontend": copy.deepcopy(service)}}
    image = {"Id": "sha256:" + "a" * 64, "RepoTags": ["app:stable"],
             "Config": {"Entrypoint": ["python"], "Cmd": ["main.py"], "Env": ["PATH=/bin"]}}
    container = {"Image": image["Id"], "Config": {"Entrypoint": ["python"], "Cmd": ["main.py"],
                  "Env": ["PATH=/bin", "KEY=value"]},
                 "Mounts": [{"Type": "bind", "Source": "/old", "Destination": "/app/old", "RW": False}]}
    after = copy.deepcopy(before)
    after["services"]["backend"]["volumes"].append(
        {"type": "bind", "source": "/release/new", "target": "/app/new", "read_only": True,
         "bind": {"create_host_path": False}})
    return before, after, container, image


class ComposeTests(unittest.TestCase):
    def setUp(self):
        self.b, self.a, self.c, self.i = fixture()
        self.allow = {"backend": {"/app/new": "/release/new"}}

    def check(self):
        return merge.compare(self.b, self.a, self.allow)

    def test_exact_addition(self):
        self.assertEqual(self.check(), [])

    def test_all_service_fields_checked_without_secret_values(self):
        for key in ["privileged", "restart", "networks", "healthcheck", "environment"]:
            with self.subTest(key=key):
                self.a["services"]["backend"][key] = "SECRET_CANARY"
                errors = self.check()
                self.assertTrue(errors)
                self.assertNotIn("SECRET_CANARY", str(errors))
                self.a = fixture()[1]

    def test_top_level_drift(self):
        self.a["networks"] = {"default": {"external": True}}
        self.assertTrue(self.check())

    def test_existing_mount_options_cannot_change(self):
        self.a["services"]["backend"]["volumes"][0]["bind"] = {"propagation": "rshared"}
        self.assertTrue(self.check())

    def test_new_mount_wrong_type_source_rw_or_options(self):
        for field, value in [("type", "volume"), ("source", "/wrong"), ("read_only", False),
                             ("bind", {"create_host_path": True}), ("consistency", "cached")]:
            with self.subTest(field=field):
                self.a["services"]["backend"]["volumes"][-1][field] = value
                self.assertTrue(self.check())
                self.a = fixture()[1]

    def test_duplicate_target_rejected(self):
        self.a["services"]["backend"]["volumes"].append(self.a["services"]["backend"]["volumes"][-1])
        with self.assertRaises(ValueError):
            self.check()

    def test_unknown_allow_service(self):
        self.allow["typo"] = {"/x": "/y"}
        self.assertTrue(self.check())

    def test_omitted_and_empty_are_distinct(self):
        self.a["services"]["backend"]["command"] = []
        self.assertTrue(self.check())

    def test_running_both_services_image_defaults(self):
        for service in ("backend", "frontend"):
            self.assertEqual(merge.compare_running(service, self.c, self.i, self.b), [])

    def test_same_tag_different_image_id(self):
        self.i["Id"] = "sha256:" + "b" * 64
        self.assertTrue(merge.compare_running("backend", self.c, self.i, self.b))

    def test_wrong_image_reference(self):
        self.i["RepoTags"] = ["wrong:tag"]
        self.assertTrue(merge.compare_running("backend", self.c, self.i, self.b))

    def test_explicit_empty_command_and_entrypoint(self):
        self.b["services"]["backend"]["entrypoint"] = []
        self.assertTrue(merge.compare_running("backend", self.c, self.i, self.b))
        self.c["Config"]["Entrypoint"] = []
        self.c["Config"]["Cmd"] = None
        self.assertEqual(merge.compare_running("backend", self.c, self.i, self.b), [])
        self.b["services"]["backend"]["entrypoint"] = None
        self.b["services"]["backend"]["command"] = []
        self.c["Config"]["Entrypoint"] = ["python"]
        self.assertEqual(merge.compare_running("backend", self.c, self.i, self.b), [])

    def test_environment_error_does_not_print_value(self):
        self.c["Config"]["Env"] += ["PASSWORD=SECRET_CANARY"]
        errors = merge.compare_running("frontend", self.c, self.i, self.b)
        self.assertTrue(errors)
        self.assertNotIn("SECRET_CANARY", str(errors))

    def test_running_mount_drift(self):
        self.c["Mounts"][0]["RW"] = True
        self.assertTrue(merge.compare_running("backend", self.c, self.i, self.b))

    def test_named_volume_uses_resolved_name(self):
        self.b["volumes"] = {"data": {"name": "project_data"}}
        self.b["services"]["backend"]["volumes"] = [
            {"type": "volume", "source": "data", "target": "/data"}]
        self.c["Mounts"] = [{"Type": "volume", "Name": "project_data", "Source": "/var/lib/docker/x",
                             "Destination": "/data", "RW": True}]
        self.assertEqual(merge.compare_running("backend", self.c, self.i, self.b), [])


def sealed_fixture(root):
    # 完全虚构的凭据/批准字段，仅供纯函数测试，绝不接触真实配置。
    environment = {"MONGODB_USERNAME": "fake", "MONGODB_PASSWORD": "fake",
                   "REDIS_PASSWORD": "fake", "JWT_SECRET": "SECRET_CANARY", "CSRF_SECRET": "fake",
                   "MONGODB_DATABASE": "k_tradingagents", "DEBUG": "false",
                   "QUOTES_INGEST_ENABLED": "false", "AKSHARE_QUOTES_SYNC_ENABLED": "false"}
    raw = json.dumps({"schema": 1, "backend_environment": environment,
                      "service_credentials": {"mongodb_username": "fake", "mongodb_password": "fake",
                                              "redis_password": "fake"}}).encode()
    context = {"schema": 1, "kind": "pending_cutover_launch", "startup_mode": "preserve_original",
               "project": "ta-live-000000000000", "snapshot_manifest_sha256": "a" * 64,
               "original_ttl_enabled": False, "runtime_file_sha256": hashlib.sha256(raw).hexdigest(),
               "access_origin": "https://127.0.0.1:18088"}
    approval = {"schema": 1, "purpose": "activate_final_snapshot", "snapshot_manifest_sha256": "a" * 64,
                "target_project": context["project"], "access_origin": context["access_origin"],
                "original_ttl_enabled": False}
    for key in ("normal_startup_approved", "configured_outbound_approved", "automatic_tasks_mcp_embedding_approved",
                "source_stopped_confirmed", "restore_verified"):
        approval[key] = True
    for name, data in [("runtime-config.json", raw), ("launch-context.json", json.dumps(context).encode()),
                       ("activation-approval.json", json.dumps(approval).encode())]:
        p = root / name
        p.write_bytes(data)
        p.chmod(0o600)


class RuntimeTests(unittest.TestCase):
    def test_injected_enabled_overrides_exec_disabled(self):
        with self.assertRaises(ValueError):
            runtime.check_environment({"QUOTES_WRITE_ENABLED": "0"}, {"QUOTES_WRITE_ENABLED": "true"})

    def test_inherited_enabled_is_not_absent(self):
        with self.assertRaises(ValueError):
            runtime.check_environment({"QUOTES_WRITE_ENABLED": "1"}, {})

    def test_missing_or_malformed_task_flags_are_blockers(self):
        for value in (None, "", "flase", "true"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runtime.check_environment({}, {"QUOTES_INGEST_ENABLED": value})

    def test_sealed_false_overrides_container_true(self):
        effective, report = runtime.check_environment({"QUOTES_WRITE_ENABLED": "1"},
            {key: "false" for key in ("QUOTES_WRITE_ENABLED", *runtime.REQUIRED_DISABLED)})
        self.assertEqual(effective["QUOTES_WRITE_ENABLED"], "false")
        self.assertEqual(report["QUOTES_WRITE_ENABLED"]["source"], "sealed")

    def test_valid_seal_and_report_without_secrets_or_side_effects(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            sealed_fixture(root)
            originals = {p.name: p.read_bytes() for p in root.iterdir()}
            inherited = dict(os.environ)
            effective, report = runtime.reconstruct(entrypoint, ENTRY_HASH, root, {})
            self.assertEqual(report["gates"]["QUOTES_WRITE_ENABLED"]["source"], "contract_default")
            self.assertNotIn("SECRET_CANARY", json.dumps(report))
            self.assertNotIn("JWT_SECRET", json.dumps(report))
            self.assertEqual(inherited, dict(os.environ))
            self.assertEqual(originals, {p.name: p.read_bytes() for p in root.iterdir()})
            self.assertEqual(effective["DOTENV_FILE"], "/dev/null")

    def test_bad_seal_and_wrong_entrypoint_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            sealed_fixture(root)
            with self.assertRaises(ValueError):
                runtime.reconstruct(entrypoint, "0" * 64, root, {})
            with (root / "runtime-config.json").open("ab") as stream:
                stream.write(b" ")
            with self.assertRaises(ValueError):
                runtime.reconstruct(entrypoint, ENTRY_HASH, root, {})

    def test_installed_smoke_requires_runtime_evidence(self):
        result = subprocess.run([sys.executable, "-B", str(ROOT / "deploy/quote_guard_smoke.py"), "--installed"],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--installed 必须", result.stderr)

    def test_optimized_python_cannot_bypass_smoke(self):
        result = subprocess.run([sys.executable, "-B", "-O", str(ROOT / "deploy/quote_guard_smoke.py")],
                                env={**os.environ, "QUOTES_WRITE_ENABLED": "1"},
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("写闸必须关闭", result.stderr)

    def test_installed_smoke_uses_reconstructed_environment(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            sealed_fixture(root)
            args = [sys.executable, "-B", str(ROOT / "deploy/quote_guard_smoke.py"), "--installed",
                    "--app-root", str(ROOT), "--runtime-entrypoint", str(entrypoint),
                    "--expected-entrypoint-sha256", ENTRY_HASH, "--secrets-dir", str(root)]
            env = {**os.environ, "QUOTES_WRITE_ENABLED": "0"}
            result = subprocess.run(args, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("isolated process", result.stdout)
            self.assertNotIn("SECRET_CANARY", result.stdout + result.stderr)
            # 构造合法封印但写闸打开；exec 子进程仍为 0，必须拒绝。
            raw = json.loads((root / "runtime-config.json").read_text())
            raw["backend_environment"]["QUOTES_WRITE_ENABLED"] = "true"
            (root / "runtime-config.json").write_text(json.dumps(raw))
            ctx = json.loads((root / "launch-context.json").read_text())
            ctx["runtime_file_sha256"] = hashlib.sha256((root / "runtime-config.json").read_bytes()).hexdigest()
            (root / "launch-context.json").write_text(json.dumps(ctx))
            result = subprocess.run(args, env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("guard smoke OK", result.stdout)
            self.assertNotIn("SECRET_CANARY", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
