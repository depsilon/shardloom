# SPDX-License-Identifier: Apache-2.0
"""Small real-filesystem and process tests; no datasets or engine builds."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from local_uat_storage import (
    StorageGuardError, accounted_bytes, check_budgets, validate_paths,
)

RUNNER = Path(__file__).with_name("run_clickbench_ingest_uat.sh")


class StorageGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name) / "home"
        self.root = self.home / "LocalData/uat"
        self.root.mkdir(parents=True)
        self.source = self.root / "sources/hits.parquet"
        self.source.parent.mkdir()
        self.source.write_bytes(b"source-fixture")
        self.target = self.root / "vortex/result.vortex"
        self.logs = self.root / "logs/run"

    def test_local_paths_and_unsynced_external_source(self):
        self.assertEqual(validate_paths(
            self.root, self.source, self.target, self.logs,
            home=self.home, platform="darwin",
        )[0], self.root.resolve())

    def test_macos_synced_locations_fail_closed(self):
        for name in ("Desktop", "Documents", "Library/CloudStorage",
                     "Library/Mobile Documents",
                     "Library/Application Support/CloudDocs"):
            with self.subTest(name=name), self.assertRaises(StorageGuardError):
                validate_paths(
                    self.root, self.home / name / "data.parquet", self.target, self.logs,
                    home=self.home, platform="darwin",
                )

    def test_symlink_and_case_variants_cannot_bypass_path_guard(self):
        synced = self.home / "Documents"
        synced.mkdir()
        alias = self.home / "alias"
        alias.symlink_to(synced, target_is_directory=True)
        for source in (alias / "data", self.home / "dOcUmEnTs/data"):
            with self.assertRaises(StorageGuardError):
                validate_paths(self.root, source, self.target, self.logs,
                               home=self.home, platform="darwin")

    def test_target_cannot_escape_accounted_workspace(self):
        for target in (self.home / "elsewhere/out.vortex", self.source):
            with self.assertRaises(StorageGuardError):
                validate_paths(self.root, self.source, target, self.logs)
        self.target.parent.mkdir()
        self.target.symlink_to(self.home / "outside")
        with self.assertRaises(StorageGuardError):
            validate_paths(self.root, self.source, self.target, self.logs)

    def test_accounting_counts_sparse_files_and_deduplicates_hardlinks(self):
        payload = self.root / "sparse"
        with payload.open("wb") as stream:
            stream.truncate(2 * 1024 * 1024)
        before = accounted_bytes(self.root)
        self.assertGreaterEqual(before, 2 * 1024 * 1024)
        os.link(payload, self.root / "alias")
        self.assertEqual(accounted_bytes(self.root), before)
        (self.root / "loop").symlink_to(self.root, target_is_directory=True)
        self.assertLess(accounted_bytes(self.root), before + 8192)

    def test_all_budgets_and_candidate_reservation_are_enforced(self):
        self.logs.mkdir(parents=True)
        (self.logs / "stdout").write_bytes(b"x" * 32)
        kwargs = dict(min_free_bytes=20, reserve_bytes=40,
                      max_workspace_bytes=100_000, max_log_bytes=100_000)
        with patch("local_uat_storage.available_bytes", return_value=100):
            self.assertEqual(check_budgets(self.root, self.target, self.logs, **kwargs)["free_bytes"], 100)
            for override in (dict(min_free_bytes=70), dict(max_workspace_bytes=1),
                             dict(max_log_bytes=1)):
                with self.subTest(override=override), self.assertRaises(StorageGuardError):
                    check_budgets(self.root, self.target, self.logs, **(kwargs | override))

    def fake_binary(self, mode):
        binary = self.home / "fake-shardloom"
        binary.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import sys, time\n"
            f"mode = {mode!r}\n"
            "target = Path(sys.argv[sys.argv.index('--output') + 1])\n"
            "target.write_bytes(b'vortex-fixture')\n"
            "target.with_suffix('.pid').write_text(str(__import__('os').getpid()))\n"
            "if mode == 'flood':\n"
            "    print('x' * (2 * 1024 * 1024), flush=True)\n"
            "    time.sleep(10)\n"
            "elif mode == 'wait':\n"
            "    time.sleep(10)\n"
            "else:\n"
            "    print('{}', flush=True)\n",
            encoding="utf-8",
        )
        binary.chmod(0o755)
        return binary

    def run_harness(self, mode="ok", extra=()):
        return subprocess.run(
            ["bash", str(RUNNER), "--uat-root", str(self.root),
             "--binary", str(self.fake_binary(mode)), "--min-free-gib", "0",
             "--max-artifact-gb", "0.01", "--max-workspace-gib", "0.1",
             "--progress-interval-seconds", "0.05",
             "--skip-source-residency-check", *extra],
            text=True, capture_output=True, timeout=15,
        )

    def test_root_override_updates_both_default_paths_and_completes(self):
        result = self.run_harness()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        summary = json.loads(next((self.root / "logs").glob("*/prepare_summary.json")).read_text())
        self.assertEqual(summary["source"], str(self.source))
        self.assertEqual(summary["target"], str(self.root / "vortex/hits-parquet-100m.vortex"))
        self.assertEqual(summary["stop_reason"], "process_completed")
        self.assertGreater(summary["native_peak_rss_bytes"], 0)
        self.assertGreater(summary["native_process_seconds"], 0)
        self.assertFalse((self.root / ".ingest-uat.lock").exists())

    def test_preflight_failure_preserves_existing_target_and_creates_no_logs(self):
        self.target.parent.mkdir()
        self.target.write_bytes(b"keep-existing-artifact")
        result = self.run_harness(extra=("--target", str(self.target),
                                        "--replace-existing", "--max-workspace-gib", "0"))
        self.assertEqual(result.returncode, 78, result.stdout + result.stderr)
        self.assertEqual(self.target.read_bytes(), b"keep-existing-artifact")
        self.assertFalse((self.root / "logs").exists())

    def test_log_flood_is_stopped_and_reported_as_failure(self):
        result = self.run_harness("flood", ("--max-log-mib", "0.5"))
        self.assertNotEqual(result.returncode, 0)
        summary = json.loads(next((self.root / "logs").glob("*/prepare_summary.json")).read_text())
        self.assertEqual(summary["stop_reason"], "storage_budget_guard_failed")
        marker = self.root / "vortex/hits-parquet-100m.pid"
        writer_pid = int(marker.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(writer_pid, 0)
        self.assertFalse((self.root / ".ingest-uat.lock").exists())

    def test_replacement_preserves_backups_staging_and_glob_lookalikes(self):
        self.target.parent.mkdir()
        self.target = self.target.with_name("result[1].vortex")
        self.target.write_bytes(b"replace-exactly-this")
        preserved = [
            self.target.with_name("result1.vortex"),
            self.target.with_name("result[1].vortex.backup"),
            self.target.with_name("result[1] 2.vortex"),
            self.target.with_name(".result[1].vortex.shardloom-tmp-unknown"),
        ]
        for path in preserved:
            path.write_bytes(b"keep-unowned-file")
        result = self.run_harness(extra=("--target", str(self.target), "--replace-existing"))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.target.read_bytes(), b"vortex-fixture")
        for path in preserved:
            self.assertEqual(path.read_bytes(), b"keep-unowned-file")

    def test_replacement_does_not_delete_a_source_with_target_backup_name(self):
        self.target.parent.mkdir()
        self.source = self.target.with_name(self.target.name + ".source")
        self.source.write_bytes(b"source-fixture")
        self.target.write_bytes(b"old-artifact")
        result = self.run_harness(extra=("--source", str(self.source),
                                        "--target", str(self.target), "--replace-existing"))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.source.read_bytes(), b"source-fixture")
        self.assertEqual(self.target.read_bytes(), b"vortex-fixture")

    def test_second_run_cannot_enter_locked_workspace(self):
        (self.root / ".ingest-uat.lock").mkdir()
        result = self.run_harness()
        self.assertEqual(result.returncode, 75, result.stdout + result.stderr)
        self.assertTrue((self.root / ".ingest-uat.lock").exists())
        self.assertFalse((self.root / "logs").exists())

    def test_interrupt_stops_child_and_releases_lock(self):
        child = subprocess.Popen(
            ["bash", str(RUNNER), "--uat-root", str(self.root),
             "--binary", str(self.fake_binary("wait")), "--min-free-gib", "0",
             "--max-artifact-gb", "0.01", "--max-workspace-gib", "0.1",
             "--progress-interval-seconds", "0.05", "--skip-source-residency-check"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            marker = self.root / "vortex/hits-parquet-100m.pid"
            deadline = time.monotonic() + 5
            while not marker.exists():
                self.assertLess(time.monotonic(), deadline, "fake writer did not start")
                time.sleep(0.01)
            writer_pid = int(marker.read_text())
            child.terminate()
            stdout, stderr = child.communicate(timeout=8)
            self.assertEqual(child.returncode, 143, stdout + stderr)
            with self.assertRaises(ProcessLookupError):
                os.kill(writer_pid, 0)
            self.assertFalse((self.root / ".ingest-uat.lock").exists())
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
