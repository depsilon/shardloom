# SPDX-License-Identifier: Apache-2.0
"""Pure fixture checks; no network, native binaries or package execution."""
import importlib.util
import io
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("registry_release_evidence", ROOT / "scripts/registry_release_evidence.py")
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)


class RegistryReleaseEvidenceTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix" and shutil.which("cargo"), "POSIX Cargo metadata fixture")
    def test_observer_uses_configured_external_cargo_target(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            root, target = base / "Documents/repo", base / "local-cache"
            (root / "src").mkdir(parents=True)
            (root / "src/lib.rs").write_text("")
            (root / "Cargo.toml").write_text('[package]\nname="metadata-fixture"\nversion="0.0.0"\nedition="2021"\n')
            with patch.dict(os.environ, {"CARGO_TARGET_DIR": str(target)}), \
                    patch.object(evidence, "available_bytes", return_value=100 << 30):
                observer = evidence.Observer(root)
            self.assertEqual(observer.target, target)
            self.assertEqual(observer.directory.parent, target)
            self.assertFalse((root / "target").exists())
            self.assertEqual(observer.local_ref(observer.directory / "capture.json"), str(observer.directory / "capture.json"))
            self.assertEqual(observer.local_ref(root / "docs/proof.json"), "docs/proof.json")

    @unittest.skipUnless(os.name == "posix" and shutil.which("cargo"), "POSIX Cargo metadata fixture")
    def test_observer_rejects_synced_cargo_target_before_workspace_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            root, target = base / "repo", base / "Documents/cargo-target"
            (root / "src").mkdir(parents=True)
            (root / "src/lib.rs").write_text("")
            (root / "Cargo.toml").write_text('[package]\nname="metadata-fixture"\nversion="0.0.0"\nedition="2021"\n')
            with patch.dict(os.environ, {"CARGO_TARGET_DIR": str(target)}), \
                    patch.object(evidence.Path, "home", return_value=base), \
                    patch.object(evidence.sys, "platform", "darwin"):
                with self.assertRaisesRegex(ValueError, "cloud-managed"):
                    evidence.Observer(root)
            self.assertFalse(target.exists())

    @unittest.skipUnless(os.name == "posix" and shutil.which("cargo"), "POSIX Cargo metadata fixture")
    def test_failed_metadata_does_not_start_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with self.assertRaisesRegex(ValueError, "offline Cargo metadata failed"):
                evidence.Observer(root)
            self.assertFalse((root / "target").exists())

    def test_stream_digest_and_bound(self):
        self.assertEqual(evidence.stream_sha(io.BytesIO(b"fixture"), 7),
                         (evidence.sha_bytes(b"fixture"), 7))
        with self.assertRaises(ValueError):
            evidence.stream_sha(io.BytesIO(b"oversized"), 3)

    def test_archive_path_and_metadata_rejection(self):
        for path in ("../escape", "/absolute", "x/../escape", "x\\escape"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                evidence.safe_member(path)
        self.assertEqual(evidence.package_metadata(b"Name: shardloom\nVersion: 0.2.4\n\n")["version"], "0.2.4")
        for raw in (b"Name: other\nVersion: 0.2.4\n", b"Name: shardloom\nVersion: 0.2.3\n"):
            with self.assertRaises(ValueError):
                evidence.package_metadata(raw)

    def test_complete_registry_inventory_rejects_digest_url_and_size_drift(self):
        rows = [{"filename": item[0], "sha256": "a" * 64, "size": 8, "url": "https://example.invalid/" + item[0]}
                for item in evidence.KINDS.values()]
        proof = {"schema_version": "shardloom.python_registry_package_proof.v1", "proof_status": "passed",
                 "package_version": "0.2.4", "channel_id": "testpypi", "fallback_attempted": False,
                 "external_engine_invoked": False, "registry_release_artifacts": rows}
        def live():
            return {"urls": [dict(row, digests={"sha256": row["sha256"]}) for row in rows]}
        self.assertEqual(len(evidence.validate_inventory(proof, live(), "testpypi")), 4)
        for key, value in (("size", 9), ("url", "different"), ("digests", {"sha256": "b" * 64})):
            changed = live(); changed["urls"][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                evidence.validate_inventory(proof, changed, "testpypi")

    def test_wheel_exact_platform_cli_and_metadata(self):
        filename, platform, binary = evidence.KINDS["python-dist-macos"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / filename
            def write(extra=False, version="0.2.4", relocated=False):
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("shardloom-0.2.4.dist-info/METADATA", f"Name: shardloom\nVersion: {version}\n\n")
                    archive.writestr("shardloom-0.2.4.dist-info/WHEEL", "Root-Is-Purelib: false\nTag: cp313-cp313-macosx_26_0_arm64\n\n")
                    prefix = "shardloom-0.2.4.data/purelib/" if relocated else ""
                    archive.writestr(prefix + f"shardloom/bin/{platform}/{binary}", b"inert fixture, never executable")
                    if extra:
                        archive.writestr("shardloom/bin/other/shardloom", b"unexpected")
            write()
            result = evidence.inspect_distribution(path, "python-dist-macos", b"")
            self.assertEqual(result["bundled_cli"]["sha256"], evidence.sha_bytes(b"inert fixture, never executable"))
            write(relocated=True)
            self.assertTrue(evidence.inspect_distribution(path, "python-dist-macos", b"")["bundled_cli"]["member"].startswith("shardloom-0.2.4.data/purelib/"))
            write(extra=True)
            with self.assertRaises(ValueError):
                evidence.inspect_distribution(path, "python-dist-macos", b"")
            write(version="0.2.3")
            with self.assertRaises(ValueError):
                evidence.inspect_distribution(path, "python-dist-macos", b"")


if __name__ == "__main__":
    unittest.main()
