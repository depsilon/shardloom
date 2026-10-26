# SPDX-License-Identifier: Apache-2.0
"""Contracts for generated Python and CLI application references."""

from __future__ import annotations

import copy
import importlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_WRITER_PATH = REPO_ROOT / "scripts" / "write_public_api_reference.py"


def load_reference_writer():
    scripts = str(REFERENCE_WRITER_PATH.parent)
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    spec = importlib.util.spec_from_file_location(
        "write_public_api_reference_for_test",
        REFERENCE_WRITER_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {REFERENCE_WRITER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublicApiReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.writer = load_reference_writer()

    def _existing_application_snapshot(self) -> dict:
        index = json.loads((REPO_ROOT / self.writer.INDEX).read_text(encoding="utf-8"))
        return copy.deepcopy(index["application"])

    def _copy_registry(self, root: Path) -> Path:
        source = REPO_ROOT / self.writer.REGISTRY
        target = root / self.writer.REGISTRY
        target.parent.mkdir(parents=True)
        shutil.copyfile(source, target)
        return target

    def _copy_native_reference_inputs(self, root: Path) -> Path:
        matrix = json.loads((REPO_ROOT / self.writer.PACKAGE_MATRIX).read_text(encoding="utf-8"))
        channel = next(row for row in matrix["channels"]
                       if row["channel_id"] == matrix["user_guide"]["artifact_channel"])
        provenance = Path(channel["provenance_ref"])
        for path in (Path("Cargo.toml"), Path("Cargo.lock"), Path("shardloom-vortex/Cargo.toml"),
                     self.writer.PACKAGE_MATRIX, provenance):
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO_ROOT / path, target)
        return root / provenance

    def test_generated_files_are_deterministic_and_current(self) -> None:
        generated = self.writer.generated_files(REPO_ROOT)
        self.assertEqual(generated, self.writer.generated_files(REPO_ROOT))
        for relative_path, expected in generated.items():
            with self.subTest(path=relative_path):
                actual = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
                self.assertEqual(actual, expected)

    def test_python_application_snapshot_matches_lazy_package_discovery(self) -> None:
        python_source = str(REPO_ROOT / "python" / "src")
        if python_source not in sys.path:
            sys.path.insert(0, python_source)
        shardloom = importlib.import_module("shardloom")

        namespace_names = {"native", "diagnostics", "testing"}
        discovered = set(shardloom.__all__) - {"__version__"} - namespace_names
        index = json.loads((REPO_ROOT / self.writer.INDEX).read_text(encoding="utf-8"))
        application_rows = index["application"]["python_symbols"]
        recorded = {
            row["name"] for row in application_rows
            if row.get("audience") == "application"
        }
        recorded.discard("__version__")
        recorded.difference_update(namespace_names)

        self.assertEqual(recorded, discovered)

    def test_native_metadata_matches_resolved_provider_and_selected_installed_release(self) -> None:
        facts, inputs = self.writer.native_reference(REPO_ROOT)
        index = json.loads((REPO_ROOT / self.writer.INDEX).read_text(encoding="utf-8"))
        matrix = json.loads((REPO_ROOT / self.writer.PACKAGE_MATRIX).read_text(encoding="utf-8"))
        recorded = index["native_python_surface"]
        self.assertEqual(recorded["native_ipc_provider_version"],
                         self.writer.upstream_vortex_lock_version(REPO_ROOT))
        self.assertEqual(facts["hosted_integration_release_tag"], matrix["user_guide"]["release_tag"])
        self.assertTrue(facts["hosted_integration_complete"])
        for key, value in facts.items():
            self.assertEqual(recorded[key], value)
        for path, expected_hash in inputs.items():
            self.assertEqual(index["application"]["source_inputs"][path], expected_hash)
        # This historical release-specific fact must not be promoted with current availability.
        self.assertFalse(recorded["included_in_published_0_5_1"])

    def test_native_metadata_rejects_manifest_lockfile_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._copy_native_reference_inputs(root)
            lock = root / "Cargo.lock"
            version = self.writer.upstream_vortex_lock_version(root)
            text = lock.read_text(encoding="utf-8")
            original = f'name = "vortex"\nversion = "{version}"'
            self.assertEqual(text.count(original), 1)
            lock.write_text(text.replace(original, original[:-1] + '-changed"'), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "matching Vortex manifest and lockfile"):
                self.writer.native_reference(root)

    def test_native_metadata_rejects_unbound_release_proof(self) -> None:
        for field, value in (("package_version", "99.0.0"), ("release_source_commit", "0" * 40),
                             ("proof_status", "failed"),
                             ("runtime_and_packaging_source_equal_to_release", False)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                provenance = self._copy_native_reference_inputs(root)
                proof = json.loads(provenance.read_text(encoding="utf-8"))
                proof[field] = value
                provenance.write_text(json.dumps(proof), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "does not match the selected release"):
                    self.writer.native_reference(root)

    def test_native_metadata_requires_each_selected_native_wheel_once(self) -> None:
        for mutation in ("missing", "duplicate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                provenance = self._copy_native_reference_inputs(root)
                proof = json.loads(provenance.read_text(encoding="utf-8"))
                lanes = proof["native_installed_wheel_proofs"]
                if mutation == "missing":
                    lanes.pop()
                else:
                    lanes[-1] = copy.deepcopy(lanes[0])
                provenance.write_text(json.dumps(proof), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "every selected native wheel exactly once"):
                    self.writer.native_reference(root)

    def test_native_metadata_rejects_failed_empty_or_skipped_integration(self) -> None:
        for field, value in (("status", "failed"), ("tests", 0), ("tests", True),
                             ("failures", 1), ("errors", 1), ("skipped", 1),
                             ("failures", False), ("errors", 0.0), ("skipped", False)):
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                provenance = self._copy_native_reference_inputs(root)
                proof = json.loads(provenance.read_text(encoding="utf-8"))
                proof["native_installed_wheel_proofs"][0]["integration"][field] = value
                provenance.write_text(json.dumps(proof), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "incomplete or unbound"):
                    self.writer.native_reference(root)

    def test_native_metadata_rejects_unbound_installed_artifacts(self) -> None:
        for field in ("source_commit", "wheel_sha256", "native_library_sha256", "cli_sha256"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                provenance = self._copy_native_reference_inputs(root)
                proof = json.loads(provenance.read_text(encoding="utf-8"))
                proof["native_installed_wheel_proofs"][0][field] = "changed"
                provenance.write_text(json.dumps(proof), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "incomplete or unbound"):
                    self.writer.native_reference(root)

    def test_source_only_command_reference_rejects_changed_registry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = self._copy_registry(root)
            snapshot = self._existing_application_snapshot()
            snapshot["command_registry"]["source_sha256"] = self.writer.digest(registry)
            registry.write_text(
                registry.read_text(encoding="utf-8") + "\n// registry source changed\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "command metadata is stale"):
                self.writer.command_reference(
                    root,
                    None,
                    snapshot,
                )

    def test_source_only_command_reference_rejects_corrupted_saved_audience_map(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = self._copy_registry(root)
            snapshot = self._existing_application_snapshot()
            command_registry = snapshot["command_registry"]
            command_registry["source_sha256"] = self.writer.digest(registry)
            first_name = command_registry["registered_commands"][0]
            command_registry["audiences"][first_name] = "corrupt"

            with self.assertRaisesRegex(ValueError, "names/audiences disagree"):
                self.writer.command_reference(root, None, snapshot)

    def test_source_only_command_reference_rejects_missing_or_duplicate_application_rows(
        self,
    ) -> None:
        for mutation in ("missing", "duplicate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                registry = self._copy_registry(root)
                snapshot = self._existing_application_snapshot()
                command_registry = snapshot["command_registry"]
                command_registry["source_sha256"] = self.writer.digest(registry)
                rows = command_registry["application_metadata"]
                self.assertTrue(rows)
                if mutation == "missing":
                    rows.pop()
                else:
                    rows.insert(0, copy.deepcopy(rows[0]))

                with self.assertRaisesRegex(ValueError, "application metadata"):
                    self.writer.command_reference(root, None, snapshot)

    def test_registry_names_rejects_duplicate_declarations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / self.writer.REGISTRY
            registry.parent.mkdir(parents=True)
            registry.write_text(
                'pub const REGISTERED_COMMANDS: &[&str] = &["status", "run", "status"];\n'
                'pub const APPLICATION_COMMANDS: &[&str] = &["status"];\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "duplicate command"):
                self.writer.registry_names(root, "REGISTERED_COMMANDS")


if __name__ == "__main__":
    unittest.main()
