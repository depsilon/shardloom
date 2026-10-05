"""Completion eligibility requires real current evidence and declared scope coverage."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import check_compute_engine_completion_gate as gate
from python.tests import test_native_benchmark_evidence as packets


class ComputeEngineCompletionGateTests(unittest.TestCase):
    def report(self, root, benchmark=None, *, phase="- [x] completed\n", review="- [x] reviewed\n", **kwargs):
        phase_path, review_path = root / "phase.md", root / "review.md"
        phase_path.write_text(phase)
        review_path.write_text(review)
        return gate.build_report(benchmark_results=benchmark, phase_plan=phase_path,
                                 global_review=review_path, **kwargs)

    def test_missing_empty_and_retired_evidence_never_pass_vacuously(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "report.json"
            for payload in [None, {}, {"published_benchmark_rows": [{"status": "success"}]},
                            {"schema_version": "shardloom.public_native_benchmark.v1", "records": []}]:
                with self.subTest(payload=payload):
                    if payload is not None:
                        path.write_text(json.dumps(payload))
                    report = self.report(root, path if payload is not None else None)
                    self.assertEqual(report["status"], "blocked")
                    self.assertFalse(report["completion_claim_allowed"])
                    self.assertFalse(report["publication_allowed"])

    def test_verified_probe_does_not_establish_full_scope_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, *_ = packets.NativeBenchmarkEvidenceTests().make_packet(root)
            self.assertEqual(gate.validate_manifest(path)[0], [])
            report = self.report(root, path)
            evidence = report["benchmark_evidence"]
            self.assertEqual(evidence["recorded_case_count"], 2)
            self.assertEqual(evidence["native_case_count"], 1)
            self.assertEqual(set(evidence["missing_formats"]), set(gate.FORMAT_ORDER) - {"csv"})
            self.assertEqual(len(evidence["missing_workloads"]), len(gate.WORKLOADS) - 1)
            self.assertFalse(report["completion_claim_allowed"])

    def test_tampered_process_evidence_is_reverified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, _, native_log, *_ = packets.NativeBenchmarkEvidenceTests().make_packet(root)
            native_log.write_text("{}")
            report = self.report(root, path)
            self.assertTrue(any("content changed" in item for item in report["blockers"]))
            self.assertFalse(report["completion_claim_allowed"])

    def test_verified_full_scope_still_requires_completed_plan_and_review(self):
        # The validator's retained-byte checks are exercised above and in the
        # packet tests. Here the stub isolates the scope/plan conjunction.
        payload = {"configuration": {"formats": list(gate.FORMAT_ORDER),
                                      "scenarios": list(gate.WORKLOADS)},
                   "records": [{"engine": "shardloom"}]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "verified-report.json"
            with mock.patch.object(gate, "validate_manifest", return_value=([], payload)) as validate:
                accepted = self.report(root, path)
                validate.assert_called_once_with(path)
                self.assertTrue(accepted["completion_claim_allowed"])
                self.assertFalse(accepted["performance_claim_allowed"])
                self.assertFalse(accepted["publication_allowed"])
                blocked = self.report(root, path, phase="- [ ] unfinished runtime\n", review="- [ ] unclassified issue\n")
                self.assertFalse(blocked["completion_claim_allowed"])
                self.assertEqual(blocked["phase_plan_unchecked_count"], 1)
                self.assertTrue(blocked["global_review_unchecked_rows_block_completion"])

    def test_declared_coverage_cannot_override_failed_evidence_verification(self):
        payload = {"configuration": {"formats": list(gate.FORMAT_ORDER),
                                      "scenarios": list(gate.WORKLOADS)}, "records": []}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch.object(gate, "validate_manifest", return_value=(["native payload mismatch"], payload)):
                report = self.report(root, root / "bad.json")
            self.assertEqual(report["blockers"], ["native payload mismatch"])
            self.assertFalse(report["completion_claim_allowed"])


if __name__ == "__main__":
    unittest.main()
