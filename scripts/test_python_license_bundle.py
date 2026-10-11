"""Integrity and fail-closed checks for bundled native dependency notices."""

import hashlib
import json
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch

import check_dependency_audit as audit
import check_python_license_bundle as notices
from check_production_usability_gate import validate_docs
from stage_native_python_package import stage


class PythonLicenseBundleTests(unittest.TestCase):
    def fixture(self, root):
        text = b"Copyright fixture. Permission is granted for this test fixture.\n"
        sha = hashlib.sha256(text).hexdigest()
        (root / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
        (root / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "fixture"\nversion = "1.0.0"\n'
            'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
            f'checksum = "{"a" * 64}"\n'
        )
        source = root / "fixture"
        source.mkdir()
        (source / "LICENSE").write_bytes(text)
        inventory = root / "inventory.json"
        inventory.write_text(json.dumps({"packages": [{
            "name": "fixture", "version": "1.0.0", "license": "MIT",
            "source": "registry+https://github.com/rust-lang/crates.io-index",
            "repository": "https://example.invalid/fixture",
            "manifest_path": str(source / "Cargo.toml"), "files": [{
                "source_path": str(source / "LICENSE"), "package_relative_path": "LICENSE",
                "evidence_source": "registry_cache", "bytes": len(text), "sha256": sha,
            }],
        }]}))
        notices.generate(root, inventory)
        return root / notices.BUNDLE / "texts" / f"{sha}.txt"

    def test_checked_in_bundle_covers_every_locked_registry_package(self):
        report = notices.verify(Path(__file__).resolve().parents[1])
        self.assertEqual(report["status"], "passed")
        self.assertGreater(report["registry_packages"], 0)

    def test_modified_missing_and_unattributed_texts_are_rejected(self):
        for mutation in ("modified", "missing", "unreferenced", "lock_changed", "component_missing"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                text = self.fixture(root)
                notices.verify(root)
                if mutation == "modified":
                    text.write_text("different copyright")
                elif mutation == "missing":
                    text.unlink()
                elif mutation == "unreferenced":
                    (text.parent / "unreferenced.txt").write_text("unattributed")
                elif mutation == "lock_changed":
                    lock = root / "Cargo.lock"
                    lock.write_text(lock.read_text().replace("a" * 64, "b" * 64))
                else:
                    path = root / notices.BUNDLE / "manifest.json"
                    manifest = json.loads(path.read_text())
                    manifest["components"] = []
                    path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    notices.verify(root)

    def test_native_crate_is_in_runtime_no_fallback_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "shardloom-python"
            crate.mkdir()
            (crate / "Cargo.toml").write_text('[dependencies]\ndatafusion = "1"\n')
            (root / "python").mkdir()
            (root / "python/pyproject.toml").write_text('[project]\ndependencies = []\n')
            with patch.object(audit, "ROOT", root):
                report = audit.check_runtime_dependency_scope()
            self.assertFalse(report["fallback_dependency_absent"])
            self.assertEqual(report["forbidden_runtime_dependencies"], ["datafusion"])

    @unittest.skipUnless(sys.platform in {"darwin", "linux"}, "native extension staging uses a Unix provider")
    def test_staging_refuses_existing_destination_without_changing_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sentinel = root / "sentinel"
            sentinel.write_bytes(b"preserve")
            with self.assertRaisesRegex(ValueError, "new destination"):
                stage(root, root, root / "missing.so", root / "missing-cli")
            self.assertEqual(sentinel.read_bytes(), b"preserve")
            self.assertEqual(list(root.iterdir()), [sentinel])

    def test_usability_legal_gate_accepts_additional_notices_but_requires_own_license(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "python").mkdir()
            path = root / "python/pyproject.toml"
            path.write_text('license-files = ["THIRD_PARTY_LICENSES/texts/*.txt", "NOTICE", "LICENSE"]\n')
            self.assertNotIn("python/pyproject.toml must include LICENSE and NOTICE files", validate_docs(root)[1])
            path.write_text('license-files = ["NOTICE", "THIRD_PARTY_LICENSES/texts/*.txt"]\n')
            self.assertIn("python/pyproject.toml must include LICENSE and NOTICE files", validate_docs(root)[1])


if __name__ == "__main__":
    unittest.main()
