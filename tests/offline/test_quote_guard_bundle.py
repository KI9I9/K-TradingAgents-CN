"""发布物覆盖与离线冒烟测试：不执行 Docker/SSH/npm。"""
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bundle = load("quote_bundle", "deploy/build_quote_guard_bundle.py")
smoke = load("quote_smoke", "deploy/quote_guard_smoke.py")
contract = load("quote_contract", "app/services/quote_contract.py")


class TestBundle(unittest.TestCase):
    def make_frontend(self, temp, name="index.html"):
        root = Path(temp)
        archive = root / "frontend.tgz"
        data = b"<html>test frontend</html>"
        with tarfile.open(archive, "w:gz") as tar:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        log = root / "build.log"
        log.write_text("test fixture only")
        report = {"command": "npm run build", "exit_code": 0,
                  "source": {p: bundle.sha256((ROOT / "frontend" / p).read_bytes())
                             for p in ("src/views/PaperTrading/index.vue", "package.json", "package-lock.json")},
                  "sha256": bundle.sha256(archive.read_bytes()), "index_sha256": bundle.sha256(data),
                  "build_log": str(log), "build_log_sha256": bundle.sha256(log.read_bytes())}
        report_path = root / "report.json"
        report_path.write_text(json.dumps(report))
        output = root / "output"
        output.mkdir()
        return output, archive, report_path

    def test_frontend_export_and_readonly_override(self):
        with tempfile.TemporaryDirectory() as temp:
            output, archive, report = self.make_frontend(temp)
            result = bundle.attach_frontend(ROOT, output, archive, report)
            self.assertEqual(result["files"], 1)
            bundle.write_checksums(output)
            self.assertIn("frontend-dist/index.html", (output / "SHA256SUMS").read_text())
            volume = bundle.delivery_override("/release", True)["services"]["frontend"]["volumes"][0]
            self.assertEqual(volume["source"], "/release/frontend-dist")
            self.assertEqual(volume["target"], "/usr/share/nginx/html")
            self.assertTrue(volume["read_only"])
            self.assertFalse(volume["bind"]["create_host_path"])

    def test_frontend_path_escape_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            output, archive, report = self.make_frontend(temp, "../escape")
            with self.assertRaisesRegex(ValueError, "不安全路径"):
                bundle.attach_frontend(ROOT, output, archive, report)
            self.assertFalse((output / "escape").exists())

    def test_frontend_wrong_archive_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            output, archive, report = self.make_frontend(temp)
            archive.write_bytes(archive.read_bytes() + b"tamper")
            with self.assertRaisesRegex(ValueError, "归档哈希"):
                bundle.attach_frontend(ROOT, output, archive, report)

    def test_frontend_stale_source_or_failed_build_rejected(self):
        for field in ("source", "exit_code"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                output, archive, report = self.make_frontend(temp)
                data = json.loads(report.read_text())
                if field == "source":
                    data["source"]["package.json"] = "0" * 64
                else:
                    data["exit_code"] = 1
                report.write_text(json.dumps(data))
                with self.assertRaises(ValueError):
                    bundle.attach_frontend(ROOT, output, archive, report)

    def test_every_changed_application_file_is_in_delivery(self):
        bundle.check_coverage(bundle.changed_application_files(ROOT))

    def test_new_unlisted_writer_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "未覆盖"):
            bundle.check_coverage({"app/worker/new_quotes_writer.py"})

    def test_export_matches_actual_workspace_and_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "release"
            manifest = bundle.build_bundle(ROOT, output)
            backend = [item for item in manifest["files"] if item["mode"] != "tool"]
            self.assertEqual(len(backend), 21)
            self.assertEqual(sum(item["mode"] == "replace" for item in backend), 3)
            for item in manifest["files"]:
                data = (output / item["path"]).read_bytes()
                self.assertEqual(data, (ROOT / item["path"]).read_bytes())
                self.assertEqual(bundle.sha256(data), item["sha256"])
                if "baseline_sha256" in item:
                    base = bundle.git(ROOT, "show", f"{bundle.BASELINE}:{item['path']}")
                    self.assertEqual(bundle.sha256(base), item["baseline_sha256"])
            # 清单每条路径都可在解包后的目录内解析，不带 devbox 绝对路径。
            for line in (output / "SHA256SUMS").read_text().splitlines():
                digest, path = line.split("  ", 1)
                self.assertFalse(Path(path).is_absolute())
                self.assertEqual(bundle.sha256((output / path).read_bytes()), digest)
            self.assertFalse(manifest["frontend_built"])
            with self.assertRaises(FileExistsError):
                bundle.build_bundle(ROOT, output)

    def test_override_delivers_all_eighteen_added_files_read_only(self):
        root = "/srv/reviewed-release"
        volumes = bundle.backend_override(root)["services"]["backend"]["volumes"]
        self.assertEqual({v["target"] for v in volumes},
                         {"/app/" + p for p in bundle.BIND_ADDITIONS})
        self.assertEqual(len(volumes), 18)
        for volume in volumes:
            self.assertEqual(volume["type"], "bind")
            self.assertTrue(volume["read_only"])
            self.assertFalse(volume["bind"]["create_host_path"])
            self.assertEqual(volume["source"], root + volume["target"][4:])

    def test_smoke_passes_only_with_writes_disabled(self):
        with patch.dict(os.environ, {"QUOTES_WRITE_ENABLED": "0"}):
            smoke.smoke(contract)
        with patch.dict(os.environ, {"QUOTES_WRITE_ENABLED": "1"}):
            with self.assertRaisesRegex(AssertionError, "写闸必须关闭"):
                smoke.smoke(contract)


if __name__ == "__main__":
    unittest.main(verbosity=2)
