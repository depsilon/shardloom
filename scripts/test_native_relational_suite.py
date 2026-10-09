# SPDX-License-Identifier: Apache-2.0
"""A family union must preserve complete coverage and immutable provenance."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from run_native_relational_suite import (
    FAMILIES, combine_summaries, file_sha256, run_family, source_identity,
)


class NativeRelationalSuiteTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.cohorts = []
        for family in FAMILIES:
            source = self.root / f"{family}.input"
            source.write_text("independently declared input")
            summary = {
                "schema_version": "shardloom.native_relational_python_acceptance.v1",
                "acceptance_family": family, "status": "passed", "binary_sha256": "frozen",
                "fallback_attempted": False, "external_engine_invoked": False,
                "cases": [{"name": family, "passed": True, "complete_rows_verified": 2}],
                "envelope_files": [{"path": str(self.root / f"{family}.envelope")}],
                "envelope_archives": [], "source_sha256": {source.name: file_sha256(source)},
                "source_files": [{"path": str(source), "sha256": file_sha256(source),
                                  "identity": source_identity(source)}],
            }
            path = self.root / f"{family}.json"
            path.write_text(json.dumps(summary))
            self.cohorts.append({"family": family, "path": str(path), "sha256": file_sha256(path)})

    def mutate(self, index, action):
        item = self.cohorts[index]
        path = Path(item["path"])
        summary = json.loads(path.read_text())
        action(summary)
        path.write_text(json.dumps(summary))
        item["sha256"] = file_sha256(path)

    def test_complete_union_retains_each_child_and_source(self):
        report = combine_summaries(self.cohorts, dict.fromkeys(FAMILIES, 2))
        self.assertEqual(report["case_count"], len(FAMILIES))
        self.assertEqual(report["complete_rows_verified"], 2 * len(FAMILIES))
        self.assertEqual(len(report["envelope_files"]), len(FAMILIES))
        self.assertEqual(len(report["source_files"]), len(FAMILIES))
        self.assertEqual(report["cohort_summaries"], self.cohorts)

    def test_missing_duplicate_failed_or_extra_coverage_is_not_success(self):
        for cohorts in [self.cohorts[:-1], self.cohorts + [self.cohorts[0]], self.cohorts[::-1]]:
            with self.assertRaises(ValueError):
                combine_summaries(cohorts)
        for expected in [{}, dict.fromkeys(FAMILIES, 3), {**dict.fromkeys(FAMILIES, 2), "extra": 1}]:
            with self.assertRaises(ValueError):
                combine_summaries(self.cohorts, expected)
        with self.assertRaisesRegex(ValueError, "invalid frozen"):
            combine_summaries(self.cohorts, dict.fromkeys(FAMILIES, True))
        self.mutate(1, lambda summary: summary.update(status="failed"))
        with self.assertRaises(ValueError):
            combine_summaries(self.cohorts)

    def test_provenance_and_child_identity_are_checked(self):
        self.mutate(1, lambda summary: summary.update(binary_sha256="other"))
        with self.assertRaisesRegex(ValueError, "provenance"):
            combine_summaries(self.cohorts)
        self.mutate(1, lambda summary: summary.update(binary_sha256="frozen"))
        Path(self.cohorts[1]["path"]).write_text("{}")
        with self.assertRaisesRegex(ValueError, "summary changed"):
            combine_summaries(self.cohorts)

    def test_source_mutation_and_duplicate_case_are_rejected(self):
        self.mutate(1, lambda summary: summary["cases"][0].update(name=FAMILIES[0]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            combine_summaries(self.cohorts)
        self.mutate(1, lambda summary: summary["cases"][0].update(name=FAMILIES[1]))
        (self.root / f"{FAMILIES[0]}.input").write_text("changed")
        with self.assertRaisesRegex(ValueError, "source changed"):
            combine_summaries(self.cohorts)

    def test_shared_sources_require_identical_frozen_declarations(self):
        source = json.loads(Path(self.cohorts[0]["path"]).read_text())["source_files"][0]
        self.mutate(1, lambda summary: summary["source_files"].append(dict(source)))
        report = combine_summaries(self.cohorts, dict.fromkeys(FAMILIES, 2))
        self.assertEqual(len(report["source_files"]), len(FAMILIES))
        self.assertEqual(report["source_files"][0], source)
        self.assertEqual(report["case_count"], len(FAMILIES))
        self.assertEqual(report["cohort_summaries"], self.cohorts)
        self.mutate(1, lambda summary: summary["source_files"][-1].update(extra="changed"))
        with self.assertRaisesRegex(ValueError, "shared source provenance differs"):
            combine_summaries(self.cohorts)

    def test_shared_source_hash_and_generation_are_rechecked(self):
        source = json.loads(Path(self.cohorts[0]["path"]).read_text())["source_files"][0]
        self.mutate(1, lambda summary: summary["source_files"].append(dict(source)))
        self.mutate(1, lambda summary: summary["source_files"][-1].update(sha256="changed"))
        with self.assertRaisesRegex(ValueError, "source changed"):
            combine_summaries(self.cohorts)
        self.mutate(1, lambda summary: summary["source_files"][-1].update(
            sha256=source["sha256"], identity=[0] * 5))
        with self.assertRaisesRegex(ValueError, "source changed"):
            combine_summaries(self.cohorts)

    def test_duplicate_source_within_one_family_is_rejected(self):
        self.mutate(0, lambda summary: summary["source_files"].append(dict(summary["source_files"][0])))
        with self.assertRaisesRegex(ValueError, "family contains duplicate source_files"):
            combine_summaries(self.cohorts)

    def test_duplicate_envelope_paths_are_rejected(self):
        self.mutate(1, lambda summary: summary["envelope_files"][0].update(
            path=str(self.root / f"{FAMILIES[0]}.envelope")))
        with self.assertRaisesRegex(ValueError, "duplicate envelope"):
            combine_summaries(self.cohorts)

    @unittest.skipUnless(os.name == "posix", "suite supervision requires POSIX groups")
    def test_family_deadline_retains_failed_receipt_and_reaps_process(self):
        log = self.root / "deadline.log"
        with self.assertRaises(TimeoutError):
            run_family([sys.executable, "-c", "import time; time.sleep(30)"],
                       log, lambda: None, 0.1)
        receipt = json.loads(log.with_suffix(".json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["log_sha256"], file_sha256(log))
        with self.assertRaises(ProcessLookupError):
            os.kill(receipt["pid"], 0)

    @unittest.skipUnless(os.name == "posix", "suite supervision requires POSIX groups")
    def test_child_failure_is_retained_without_successful_union(self):
        log = self.root / "failed.log"
        with self.assertRaisesRegex(ValueError, "returned 7"):
            run_family([sys.executable, "-c", "raise SystemExit(7)"], log, lambda: None, 30)
        receipt = json.loads(log.with_suffix(".json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["returncode"], 7)


if __name__ == "__main__":
    unittest.main()
