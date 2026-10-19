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
