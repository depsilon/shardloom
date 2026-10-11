"""Artifact selection and non-installing staging contracts for native Python."""

import os
from pathlib import Path
import tempfile
import unittest

from build_native_python import native_artifact, stage_library


def artifact(path: Path, *, name: str = "_native", kind: str = "cdylib") -> dict:
    return {"reason": "compiler-artifact",
            "target": {"name": name, "crate_types": [kind]},
            "filenames": [str(path)]}


class NativePythonStagingTests(unittest.TestCase):
    def test_cargo_report_selects_only_the_native_dynamic_library(self):
        library = Path("/resolved/target/debug/lib_native.dylib")
        messages = [artifact(Path("/resolved/target/debug/shardloom"), name="shardloom"),
                    artifact(Path("/resolved/target/debug/lib_native.rlib"), kind="rlib"),
                    artifact(library), artifact(library), {"reason": "build-finished"}]
        self.assertEqual(native_artifact(messages), library)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            native_artifact(messages[:2])
        with self.assertRaisesRegex(ValueError, "exactly one"):
            native_artifact(messages + [artifact(Path("/other/lib_native.so"))])

    def test_staging_replaces_atomically_and_preserves_an_open_older_artifact(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / "lib_native.dylib"
            source.write_bytes(b"first native artifact")
            source.chmod(0o755)
            output = root / "staged"
            destination = stage_library(source, output)
            self.assertEqual(destination.name, "_native.abi3.so")
            with destination.open("rb") as older:
                old_identity = os.fstat(older.fileno()).st_ino
                source.write_bytes(b"replacement native artifact")
                self.assertEqual(stage_library(source, output), destination)
                self.assertEqual(older.read(), b"first native artifact")
                self.assertNotEqual(destination.stat().st_ino, old_identity)
            self.assertEqual(destination.read_bytes(), source.read_bytes())
            self.assertEqual(destination.stat().st_mode & 0o777, 0o755)
            self.assertEqual(list(output.iterdir()), [destination])

    def test_symlink_destination_is_refused_without_changing_its_target(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / "built.so"
            source.write_bytes(b"new")
            victim = root / "retain.so"
            victim.write_bytes(b"retain")
            (root / "_native.abi3.so").symlink_to(victim)
            with self.assertRaisesRegex(ValueError, "destination.*symlink"):
                stage_library(source, root)
            self.assertEqual(victim.read_bytes(), b"retain")

    def test_source_must_be_a_regular_non_symlink_artifact(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / "built.so"
            source.write_bytes(b"native")
            alias = root / "alias.so"
            alias.symlink_to(source)
            output = root / "staged"
            for invalid in (alias, root, root / "missing.so"):
                with self.subTest(source=invalid):
                    with self.assertRaisesRegex(ValueError, "regular built artifact"):
                        stage_library(invalid, output)
                    self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
