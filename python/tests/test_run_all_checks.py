# SPDX-License-Identifier: Apache-2.0
"""Tests for the unified in-process validator runner (`scripts/run_all_checks.py`)."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import release_report_utils  # noqa: E402
import run_all_checks  # noqa: E402


class UnifiedCheckRunnerTests(unittest.TestCase):
    def test_cached_document_reads_hits_misses_and_deepcopy_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            text_file = root / "sample.md"
            text_file.write_text("hello shardloom\n", encoding="utf-8")
            json_file = root / "sample.json"
            json_file.write_text('{"items": [1, 2]}\n', encoding="utf-8")

            with release_report_utils.cached_document_reads() as cache:
                self.assertEqual(release_report_utils.read_text(text_file), "hello shardloom\n")
                self.assertEqual(release_report_utils.read_text(text_file), "hello shardloom\n")

                first_json = release_report_utils.load_json(json_file)
                self.assertEqual(first_json, {"items": [1, 2]})
                first_json["items"].append(999)

                second_json = release_report_utils.load_json(json_file)
                self.assertEqual(second_json, {"items": [1, 2]})

                self.assertEqual(release_report_utils.read_text(root / "missing.txt"), "")
                self.assertEqual(release_report_utils.read_text(root / "missing.txt"), "")

                stats = cache.stats()
                self.assertEqual(stats["misses"], 3)
                self.assertEqual(stats["hits"], 3)
                self.assertEqual(stats["unique_paths"], 3)

            self.assertIsNone(release_report_utils._ACTIVE_DOCUMENT_CACHE)

    def test_discover_and_list_cover_all_check_scripts(self) -> None:
        on_disk = sorted(p.stem for p in SCRIPTS_DIR.glob("check_*.py"))
        discovered = run_all_checks.discover_checks(SCRIPTS_DIR)
        self.assertEqual(sorted(discovered), on_disk)
        self.assertEqual(len(on_disk), 68)

        listing = run_all_checks.list_payload(REPO_ROOT)
        self.assertEqual(listing["discovered_script_count"], len(on_disk))
        self.assertFalse(listing["fallback_attempted"])
        self.assertFalse(listing["external_engine_invoked"])
        self.assertEqual(listing["claim_gate_status"], "not_claim_grade")

        suites = {row["name"]: row["suite"] for row in listing["checks"]}
        self.assertEqual(suites["check_public_status_docs"], "docs")
        self.assertEqual(suites["check_published_channel_proofs"], "library_helper")
        self.assertEqual(suites["check_registry_bundled_proof"], "library_helper")
        self.assertEqual(suites["check_release_readiness"], "gated")

        proc = subprocess.run(
            [sys.executable, "-B", str(SCRIPTS_DIR / "run_all_checks.py"), "--list"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        cli_listing = json.loads(proc.stdout)
        self.assertEqual(cli_listing["discovered_script_count"], len(on_disk))

    def test_subset_in_process_uses_document_cache_and_matches_standalone(self) -> None:
        subset = [
            "check_public_status_docs",
            "check_v1_inclusion_scope",
            "check_contribution_governance",
            "check_security_posture",
        ]
        report = run_all_checks.build_report(repo_root=REPO_ROOT, checks=subset)
        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["executed_count"], len(subset))
        self.assertEqual(report["passed_count"], len(subset))
        self.assertEqual(report["failed_count"], 0)
        self.assertGreater(report["document_cache"]["hits"], 0)
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertEqual(report["claim_gate_status"], "not_claim_grade")

        with tempfile.TemporaryDirectory() as tmp:
            standalone = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(SCRIPTS_DIR / "check_v1_inclusion_scope.py"),
                    "--repo-root",
                    str(REPO_ROOT),
                    "--output",
                    str(Path(tmp) / "v1-scope.json"),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(standalone.returncode, 0, standalone.stderr)

    def test_docs_suite_passes_cleanly_in_process(self) -> None:
        report = run_all_checks.build_report(repo_root=REPO_ROOT, suite="docs")
        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["executed_count"], len(run_all_checks.DOCS_SUITE_NAMES))
        self.assertEqual(report["failed_count"], 0)
        self.assertGreater(report["document_cache"]["hits"], 0)
        self.assertGreater(report["document_cache"]["unique_paths"], 0)

    def test_unknown_and_helper_checks_fail_explicitly(self) -> None:
        unknown = run_all_checks.build_report(
            repo_root=REPO_ROOT, checks=["check_does_not_exist"]
        )
        self.assertEqual(unknown["status"], "failed")
        self.assertGreater(len(unknown["blockers"]), 0)

        helper = run_all_checks.build_report(
            repo_root=REPO_ROOT, checks=["check_published_channel_proofs"]
        )
        self.assertEqual(helper["status"], "failed")
        self.assertGreater(len(helper["blockers"]), 0)


if __name__ == "__main__":
    unittest.main()
