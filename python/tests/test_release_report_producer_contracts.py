# SPDX-License-Identifier: Apache-2.0
"""Exercise release consumers with current static report producers."""

from __future__ import annotations

import contextlib
import copy
import io
import json
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import check_release_readiness as readiness
import check_golden_workflows as golden
import check_v1_example_replay as replay
import check_user_route_capability_report as routes
import check_v1_correctness_conformance as conformance
import check_v1_local_output_sink_scope as output_scope
import check_v1_source_prepared_state_scope as source_scope
import check_v1_vortex_runtime_scope as vortex_scope


class ReleaseReportProducerContractsTests(unittest.TestCase):
    def test_golden_complete_result_proof_reaches_replay_consumer(self) -> None:
        fields = {
            "result_values_json": '[{"id":1}]',
            "result_payload_complete": "true",
            "output_row_count": "1",
            "result_schema_json": json.dumps({"Struct": [{"names": ["id"], "dtypes": [{"Primitive": ["i64", False]}]}, False]}),
            "result_schema_format": "vortex.dtype.serde.v1",
            "fallback_attempted": "false", "external_engine_invoked": "false",
            "public_workflow_fallback_attempted": "false",
            "public_workflow_external_engine_invoked": "false",
        }
        for changed, value in ((None, None), ("result_values_json", '[{"id":2}]'),
                               ("result_payload_complete", "false"), ("external_engine_invoked", "true")):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                actual = dict(fields)
                if changed is not None:
                    actual[changed] = value
                payload = {"status": "success", "fallback": {"attempted": False, "allowed": False},
                           "fields": [{"key": key, "value": value} for key, value in actual.items()]}
                completed = subprocess.CompletedProcess([], 0, json.dumps(payload), "")
                with patch.object(golden, "run_subprocess", return_value=completed):
                    stage = golden.run_cli_stage(
                        repo_root=Path(directory), binary=Path("unused"), stage_dir=Path(directory),
                        stage_id="complete-readback", args=[], expected_fields={}, expected_rows=[{"id": 1}],
                    )
                self.assertEqual(stage["complete_result_rows_verified"], changed is None, stage["blockers"])
                self.assertEqual(replay.workflow_replay_verified({"stages": [stage]}), changed is None)
                if changed is None:
                    for field, invalid in (("complete_result_rows_verified", "true"),
                                           ("complete_result_row_count", -1), ("status", "failed")):
                        denied = copy.deepcopy(stage)
                        denied[field] = invalid
                        self.assertFalse(replay.workflow_replay_verified({"stages": [denied]}))

    def test_current_correctness_matrix_matches_executable_contract(self) -> None:
        matrix = json.loads((REPO_ROOT / conformance.DEFAULT_MATRIX).read_text())
        _, blockers = conformance._validate_matrix(matrix)
        self.assertEqual(blockers, [])

    def test_conformance_accepts_actual_vortex_source_and_output_reports(self) -> None:
        for producer, consumer in (
            (vortex_scope.build_report, conformance._validate_vortex),
            (source_scope.build_report, conformance._validate_source),
            (output_scope.build_report, conformance._validate_output),
        ):
            with self.subTest(producer=producer.__module__):
                report = producer(REPO_ROOT)
                self.assertEqual(report["status"], "passed", report["blockers"])
                _, blockers = consumer(report)
                self.assertEqual(blockers, [])

    def _readiness_checks(self, reports: dict[str, dict]) -> dict[str, dict]:
        # Unrelated release inputs remain absent and blocked. Only the supplied
        # producer/consumer contracts are under test; no release is authorized.
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "readiness.json"
            with (
                patch.object(sys, "argv", ["check_release_readiness.py", "--repo-root", str(REPO_ROOT), "--output", str(output)]),
                patch.object(readiness, "load_json", side_effect=lambda path: reports.get(path.name)),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(readiness.main(), 1)
            result = json.loads(output.read_text())
            self.assertFalse(result["public_release_claim_allowed"])
            return {row["name"]: row for row in result["checks"]}

    def test_readiness_accepts_actual_route_reports_and_rejects_admitted_external_route(self) -> None:
        reports = {
            "v1-vortex-runtime-scope-report.json": vortex_scope.build_report(REPO_ROOT),
            "user-route-capability-report.json": routes.build_report(REPO_ROOT),
        }
        checks = self._readiness_checks(reports)
        for name in ("v1_vortex_runtime_scope_gate", "user_route_capability_report"):
            self.assertEqual(checks[name]["blockers"], [])
        reports["v1-vortex-runtime-scope-report.json"]["user_route_rows"][1]["route_runtime_status"] = "global_runtime_supported"
        reports["user-route-capability-report.json"]["rows"][1]["owner"] = "shared_native_workflow"
        checks = self._readiness_checks(reports)
        for name in ("v1_vortex_runtime_scope_gate", "user_route_capability_report"):
            self.assertTrue(any("externally gated" in text for text in checks[name]["blockers"]))

    def test_readiness_accepts_actual_sink_summary_and_rejects_retired_route_count(self) -> None:
        summary, blockers = conformance._validate_output(output_scope.build_report(REPO_ROOT))
        self.assertEqual(blockers, [])
        reports = {"v1-correctness-conformance-report.json": {
            "schema_version": "shardloom.v1_correctness_conformance_report.v1",
            "status": "passed", "summaries": {"local_output_sink": summary},
        }}
        checks = self._readiness_checks(reports)
        mismatch = "local_output_sink.output_route_count="
        self.assertFalse(any(mismatch in text for text in checks["v1_correctness_conformance_gate"]["blockers"]))
        summary["output_route_count"] = 7
        checks = self._readiness_checks(reports)
        self.assertTrue(any(mismatch in text for text in checks["v1_correctness_conformance_gate"]["blockers"]))


if __name__ == "__main__":
    unittest.main()
