"""Verify patched-provider test commands against the frozen CI recipes."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import ci_provider_tests as providers


# Transcribed from .github/workflows/ci.yml at frozen baseline
# 782c636c59a6686090bebcb07fe27b4641644e13 (native_vortex_tests lane).
FROZEN_CRATES = ("vortex-array", "vortex-fsst", "vortex-runend", "vortex-layout",
                 "vortex-file", "vortex-io", "vortex-buffer", "vortex-zstd")


# Cargo commands as they appeared in the frozen workflow, before the intentional
# addition of --locked. Keeping these recipes here makes the comparison work in
# shallow checkouts and source archives without invoking Git.
FROZEN_COMMANDS = {
    "vortex-array": [
        "cargo test --manifest-path vendor/vortex-array/Cargo.toml --features _test-harness,table-display --lib -- --test-threads=2",
        "cargo clippy --manifest-path vendor/vortex-array/Cargo.toml --features _test-harness,table-display --lib -- -D warnings",
    ],
    "vortex-fsst": [
        'cargo test --manifest-path vendor/vortex-fsst/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-fsst/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- -D warnings',
    ],
    "vortex-runend": [
        'cargo test --manifest-path vendor/vortex-runend/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-runend/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- -D warnings',
    ],
    "vortex-layout": [
        'cargo test --manifest-path vendor/vortex-layout/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --features tokio,vortex-io/tokio,vortex-array/_test-harness --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-layout/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --features tokio,vortex-io/tokio,vortex-array/_test-harness --lib --tests -- -D warnings',
    ],
    "vortex-file": [
        'cargo test --manifest-path vendor/vortex-file/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --config patch.crates-io.vortex-layout.path="vendor/vortex-layout" --features tokio,vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-file/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --config patch.crates-io.vortex-layout.path="vendor/vortex-layout" --features tokio,vortex-array/table-display --lib --tests -- -D warnings',
    ],
    "vortex-io": [
        'cargo test --manifest-path vendor/vortex-io/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features object_store --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-io/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features object_store --lib --tests -- -D warnings',
    ],
    "vortex-buffer": [
        "cargo test --manifest-path vendor/vortex-buffer/Cargo.toml --all-features",
        "cargo clippy --locked --manifest-path vendor/vortex-buffer/Cargo.toml --all-features --all-targets -- -D warnings",
    ],
    "vortex-zstd": [
        'cargo test --manifest-path vendor/vortex-zstd/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-zstd/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- -D warnings',
    ],
}


def normalize_frozen_command(command: list[str]) -> list[str]:
    """Apply only the authorized --locked delta to a frozen Cargo invocation."""
    if command[0] != "cargo":
        raise AssertionError(command)
    if "--locked" not in command:
        command = [command[0], command[1], "--locked", *command[2:]]
    return command


class ProviderCommandTests(unittest.TestCase):
    def test_all_eight_providers_are_assigned_once_to_three_groups(self) -> None:
        assigned = [crate for group in providers.GROUPS.values() for crate in group]
        self.assertEqual(len(assigned), 8)
        self.assertEqual(set(assigned), set(FROZEN_CRATES))
        self.assertEqual(len(set(assigned)), len(assigned))
        self.assertEqual(set(providers.CONFIGURATIONS), set(FROZEN_CRATES))
        self.assertEqual(set(providers.GROUPS), {"arrays", "io", "buffer"})

    def test_generated_commands_preserve_frozen_feature_patch_target_and_thread_behavior(self) -> None:
        self.assertEqual(set(FROZEN_COMMANDS), set(FROZEN_CRATES))
        for crate in FROZEN_CRATES:
            with self.subTest(crate=crate):
                self.assertEqual(providers.commands(crate),
                                 [normalize_frozen_command(row.split())
                                  for row in FROZEN_COMMANDS[crate]])

        for crate in ("vortex-array", "vortex-fsst", "vortex-runend", "vortex-zstd"):
            test_command, clippy_command = providers.commands(crate)
            self.assertEqual(test_command[-3:], ["--lib", "--", "--test-threads=2"])
            self.assertIn("--lib", clippy_command)
            self.assertNotIn("--tests", clippy_command)
        for crate in ("vortex-layout", "vortex-file", "vortex-io"):
            self.assertIn("--tests", providers.commands(crate)[1])
        buffer_test, buffer_clippy = providers.commands("vortex-buffer")
        self.assertIn("--all-features", buffer_test)
        self.assertNotIn("--lib", buffer_test)
        self.assertNotIn("--test-threads=2", buffer_test)
        self.assertIn("--all-targets", buffer_clippy)


class RunGroupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / ".github/provider-locks").mkdir(parents=True)
        (self.root / "vendor").mkdir()
        self.group = "buffer"
        for crate in providers.GROUPS[self.group]:
            (self.root / ".github/provider-locks" / f"{crate}.lock").write_text(f"{crate} lock\n")
            (self.root / "vendor" / crate).mkdir()

    def test_missing_lock_is_rejected_and_failed_report_written(self) -> None:
        (self.root / ".github/provider-locks/vortex-buffer.lock").unlink()
        output = self.root / "out"
        with self.assertRaisesRegex(ValueError, "missing reviewed provider lockfile"):
            providers.run_group(self.group, output, root=self.root)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "failed")
        self.assertIn("missing reviewed provider lockfile", report["error"])

    def test_successful_mocked_commands_copy_reviewed_lock_and_write_pass_report(self) -> None:
        output = self.root / "out"
        with patch.object(providers.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            report = providers.run_group(self.group, output, root=self.root)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(run.call_count, 2)
        self.assertEqual((self.root / "vendor/vortex-buffer/Cargo.lock").read_text(), "vortex-buffer lock\n")
        saved = json.loads((output / "report.json").read_text())
        self.assertEqual(saved["status"], "passed")
        self.assertEqual(len(saved["commands"]), 2)

    def test_command_failure_is_rejected_and_failed_report_written(self) -> None:
        output = self.root / "out"
        with patch.object(providers.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                providers.run_group(self.group, output, root=self.root)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "failed")

    def test_lock_modified_by_command_is_rejected_and_failed_report_written(self) -> None:
        output = self.root / "out"
        def mutate_lock(command, **kwargs):
            (self.root / "vendor/vortex-buffer/Cargo.lock").write_text("changed by command\n")
            return subprocess.CompletedProcess(command, 0)

        with patch.object(providers.subprocess, "run", side_effect=mutate_lock):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                providers.run_group(self.group, output, root=self.root)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "failed")


if __name__ == "__main__":
    unittest.main()
