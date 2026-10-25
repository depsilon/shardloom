"""Unit tests for revision-bound CI artifact recording and validation."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import ci_build_artifact as artifact


class ArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "checkout"
        self.root.mkdir()
        (self.root / "Cargo.lock").write_text("test lock\n")
        self.identity = {
            "revision": "abc123",
            "cargo_lock_sha256": artifact.sha(self.root / "Cargo.lock"),
            "os": "Linux",
            "architecture": "x86_64",
            "workspace_root": str(self.root.resolve()),
            "rustc": "rustc 1.90.0",
            "cargo": "cargo 1.90.0",
        }
        self.source_dir = self.root / "sources"
        self.source_dir.mkdir()
        self.source = self.source_dir / "native-tests"
        self.source.write_bytes(b"fake executable payload")
        self.destination = self.root / "bundle"

    def record(self) -> dict:
        with patch.object(artifact, "identity", return_value=self.identity.copy()):
            return artifact.record_files(self.destination, "native-tests",
                {"native-tests": self.source}, build={"command": ["cargo", "test"],
                    "compiler_features": ["release-user-surfaces"],
                    "compiler_profile": {"opt_level": "2", "debug_assertions": True,
                                         "overflow_checks": True, "test": True, "debuginfo": 1}},
                root=self.root)

    def verify(self, *, identity: dict | None = None) -> dict:
        with patch.object(artifact, "identity", return_value=(identity or self.identity).copy()):
            return artifact.verify_bundle(self.destination, "native-tests", root=self.root)

    def test_regular_payload_round_trips_with_manifest_hash_and_bytes(self) -> None:
        manifest = self.record()
        verified = self.verify()
        self.assertEqual(verified, manifest)
        evidence = manifest["files"]["native-tests"]
        self.assertEqual(evidence["bytes"], len(b"fake executable payload"))
        self.assertEqual(evidence["sha256"], artifact.sha(self.destination / "native-tests"))

    def test_rejects_stale_revision_and_platform(self) -> None:
        self.record()
        for key, value in (("revision", "different"), ("os", "Darwin"), ("architecture", "arm64")):
            changed = self.identity.copy()
            changed[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "revision, platform"):
                self.verify(identity=changed)

    def test_rejects_profile_and_feature_drift(self) -> None:
        manifest = self.record()
        for key, value in (("profile", "release"),
                           ("requested_features", ["wrong-feature"])):
            changed = json.loads(json.dumps(manifest))
            changed[key] = value
            (self.destination / "manifest.json").write_text(json.dumps(changed))
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "configuration mismatch"):
                self.verify()
        (self.destination / "manifest.json").write_text(json.dumps(manifest))

    def test_rejects_payload_byte_edits(self) -> None:
        self.record()
        (self.destination / "native-tests").write_bytes(b"edited payload")
        with self.assertRaisesRegex(ValueError, "content mismatch"):
            self.verify()

    def test_consumer_rechecks_actual_compiler_settings(self) -> None:
        manifest = self.record()
        manifest["build"]["compiler_profile"]["debug_assertions"] = False
        (self.destination / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "optimization profile"):
            self.verify()

    def test_restore_requires_explicit_replacement_and_preserves_identity(self) -> None:
        self.record()
        target = self.root / "restored"
        target.write_bytes(b"earlier generated CI binary")
        previous = artifact.sha(target)
        with patch.object(artifact, "identity", return_value=self.identity.copy()):
            with self.assertRaisesRegex(ValueError, "existing artifact"):
                artifact.restore_bundle(self.destination, "native-tests", target, root=self.root)
            self.assertEqual(artifact.sha(target), previous)
            receipt = artifact.restore_bundle(self.destination, "native-tests", target,
                                              replace_existing=True, root=self.root)
        self.assertEqual(receipt["previous_sha256"], previous)
        self.assertEqual(receipt["sha256"], artifact.sha(self.source))
        self.assertEqual(target.read_bytes(), self.source.read_bytes())

    def test_explicit_restore_still_rejects_symlinks_and_damaged_sources(self) -> None:
        self.record()
        target = self.root / "restored"
        target.symlink_to(self.source)
        with patch.object(artifact, "identity", return_value=self.identity.copy()):
            with self.assertRaisesRegex(ValueError, "symlink"):
                artifact.restore_bundle(self.destination, "native-tests", target,
                                        replace_existing=True, root=self.root)
            target.unlink()
            target.write_bytes(b"keep me")
            (self.destination / "native-tests").write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "content mismatch"):
                artifact.restore_bundle(self.destination, "native-tests", target,
                                        replace_existing=True, root=self.root)
        self.assertEqual(target.read_bytes(), b"keep me")

    def test_rejects_missing_or_extra_files(self) -> None:
        self.record()
        (self.destination / "native-tests").unlink()
        with self.assertRaisesRegex(ValueError, "missing or unexpected"):
            self.verify()
        (self.destination / "native-tests").write_bytes(b"fake executable payload")
        (self.destination / "unexpected").write_text("extra")
        with self.assertRaisesRegex(ValueError, "missing or unexpected"):
            self.verify()

    def test_rejects_path_traversal_in_artifact_filename(self) -> None:
        with patch.object(artifact, "identity", return_value=self.identity.copy()):
            with self.assertRaisesRegex(ValueError, "invalid CI artifact destination"):
                artifact.record_files(self.destination, "native-tests",
                    {"../escape": self.source}, build={}, root=self.root)

    def test_rejects_symlink_payload_and_symlink_manifest(self) -> None:
        link = self.source_dir / "linked"
        link.symlink_to(self.source)
        with patch.object(artifact, "identity", return_value=self.identity.copy()):
            with self.assertRaisesRegex(ValueError, "regular file"):
                artifact.record_files(self.destination, "native-tests", {"native-tests": link},
                                      build={}, root=self.root)
        shutil.rmtree(self.destination)
        self.record()
        manifest_path = self.destination / "manifest.json"
        manifest_path.unlink()
        manifest_path.symlink_to(self.root / "Cargo.lock")
        with self.assertRaisesRegex(ValueError, "manifest must be a regular file"):
            self.verify()

    def test_rejects_symlink_payload_after_bundle_creation(self) -> None:
        self.record()
        payload = self.destination / "native-tests"
        payload.unlink()
        payload.symlink_to(self.source)
        with self.assertRaisesRegex(ValueError, "regular file"):
            self.verify()


class CompilerArtifactTests(unittest.TestCase):
    @staticmethod
    def message(*, opt_level: str = "2", debug_assertions: bool = True,
                overflow_checks: bool = True, features: list[str] | None = None) -> str:
        row = {
            "reason": "compiler-artifact",
            "target": {"name": "shardloom_vortex"},
            "profile": {"test": True, "opt_level": opt_level, "debuginfo": 1,
                        "debug_assertions": debug_assertions, "overflow_checks": overflow_checks},
            "features": features or ["release-user-surfaces"],
        }
        return json.dumps(row) + "\n"

    def test_accepts_ci_native_optimized_assertion_and_overflow_profile(self) -> None:
        row = artifact.compiler_artifact(self.message(), "native-tests")
        self.assertEqual(row["profile"]["opt_level"], "2")
        self.assertTrue(row["profile"]["debug_assertions"])
        self.assertTrue(row["profile"]["overflow_checks"])

    def test_rejects_wrong_optimization_assertions_or_overflow(self) -> None:
        variants = [
            {"opt_level": "0"},
            {"debug_assertions": False},
            {"overflow_checks": False},
        ]
        for changes in variants:
            values = {"opt_level": "2", "debug_assertions": True, "overflow_checks": True}
            values.update(changes)
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "optimization profile"):
                artifact.compiler_artifact(self.message(**values), "native-tests")

    def test_rejects_observation_feature_in_ordinary_extension(self) -> None:
        row = {
            "reason": "compiler-artifact", "target": {"name": "_native"},
            "profile": {"test": False},
            "features": ["python-native", "output-window-observation"],
        }
        with self.assertRaisesRegex(ValueError, "observation and ordinary products"):
            artifact.compiler_artifact(json.dumps(row), "native-extension")


class BuildCommandTests(unittest.TestCase):
    def test_native_tests_command_is_locked_ci_native_no_run(self) -> None:
        self.assertEqual(artifact.build_command("native-tests"), [
            "cargo", "test", "--locked", "--profile", "ci-native", "-p", "shardloom-vortex",
            "--lib", "--features", "release-user-surfaces", "--no-run", "--message-format=json",
        ])

    def test_cli_debug_and_release_are_distinct_profiles(self) -> None:
        debug = artifact.build_command("cli-debug")
        release = artifact.build_command("cli-release")
        self.assertEqual(debug, ["cargo", "build", "--locked", "--profile", "dev", "-p",
            "shardloom-cli", "--bin", "shardloom", "--features", "release-user-surfaces",
            "--message-format=json"])
        self.assertEqual(release, ["cargo", "build", "--locked", "--profile", "release", "-p",
            "shardloom-cli", "--bin", "shardloom", "--features", "release-user-surfaces",
            "--message-format=json"])
        self.assertNotEqual(debug, release)


if __name__ == "__main__":
    unittest.main()
