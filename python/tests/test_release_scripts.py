from __future__ import annotations

import json
import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class ReleaseScriptTests(unittest.TestCase):
    def _load_script_module(self, script_name: str, module_name: str) -> object:
        module_path = REPO_ROOT / "scripts" / script_name
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        script_dir = str(module_path.parent)
        inserted = False
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
            inserted = True
        previous_module = sys.modules.get(module_name)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            if previous_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = previous_module
            if inserted:
                sys.path.remove(script_dir)
        return module

    def test_architecture_tracker_missing_inputs_fail_even_when_blocked_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            output = repo_root / "target" / "tracker.json"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "check_release_architecture_tracker.py"),
                    "--repo-root",
                    str(repo_root),
                    "--output",
                    "target/tracker.json",
                    "--allow-blocked",
                ],
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "blocked")
            self.assertGreater(report["missing_required_input_count"], 0)
            self.assertTrue(report["missing_required_inputs"])
            self.assertTrue(
                any(
                    "missing required architecture tracker input" in blocker
                    for blocker in report["blockers"]
                )
            )
            self.assertFalse(report["fallback_attempted"])
            self.assertFalse(report["external_engine_invoked"])

    def test_benchmark_promoter_recomputes_stale_runtime_validation(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_artifact_for_test"
        )

        row = {
            "engine": "shardloom-native-vortex",
            "storage_format": "csv",
            "scenario_name": "stale validation",
            "status": "success",
            "source_state_id": "source-state://stale-validation",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "fixture_smoke_only",
            "runtime_execution_validation": {
                "status": "passed",
                "surface_id": "stale.cached.report",
            },
        }

        with self.assertRaisesRegex(RuntimeError, "failed runtime validation"):
            module.runtime_validation_for_row(row)

    def test_benchmark_promoter_preserves_claim_grade_readiness(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_claim_grade_for_test"
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "claim grade row",
            "status": "success",
            "source_state_id": "source-state://claim-grade-row",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.claim-grade-row",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "metrics": {
                "query_runtime_millis": 1.0,
                "claim_gate_status": "claim_grade",
                "claim_grade_requirements_met": False,
                "claim_grade_missing_evidence": ["stale metrics value"],
            },
        }

        [published] = module.published_rows([row])

        self.assertIs(published["claim_grade_requirements_met"], True)
        self.assertEqual(published["claim_grade_missing_evidence"], [])
        self.assertEqual(published["runtime_execution_validation_status"], "passed")

    def test_golden_workflow_gate_requires_external_engine_marker(self) -> None:
        module = self._load_script_module(
            "check_golden_workflows.py", "check_golden_workflows_for_test"
        )

        blockers = module.no_fallback_blockers(
            {
                "schema_version": "shardloom.output.v2",
                "fallback": {"attempted": False, "allowed": False},
                "fields": [{"key": "fallback_attempted", "value": "false"}],
            },
            "fixture",
        )

        self.assertIn("fixture: external engine marker is missing", blockers)

    def test_ci_gate_matrix_scopes_commands_to_declared_job(self) -> None:
        module = self._load_script_module(
            "check_ci_gate_matrix.py", "check_ci_gate_matrix_for_test"
        )

        workflow = """
name: ci
jobs:
  release-readiness:
    steps:
      - run: python scripts/check_release_readiness.py
  ci-gate-matrix:
    steps:
      - run: python scripts/check_ci_gate_matrix.py
"""
        doc = (
            "ci_gate_matrix_contract\n"
            "python scripts/check_ci_gate_matrix.py\n"
            "target/ci-gate-matrix-report.json\n"
            "CI matrix drift contract\n"
        )

        status = module.lane_status(module.REQUIRED_LANES[-1], workflow, doc)

        self.assertEqual(status["status"], "failed")
        self.assertIn(
            "workflow job ci-gate-matrix missing artifact ref: target/ci-gate-matrix-report.json",
            status["blockers"],
        )

    def test_ci_gate_matrix_requires_hard_release_without_allow_blocked(self) -> None:
        module = self._load_script_module(
            "check_ci_gate_matrix.py", "check_ci_gate_matrix_readiness_for_test"
        )

        release_lane = next(
            lane
            for lane in module.REQUIRED_LANES
            if lane.lane_id == "release_readiness_reports"
        )

        self.assertIn("python scripts/check_release_readiness.py", release_lane.commands)
        self.assertNotIn(
            "python scripts/check_release_readiness.py --allow-blocked",
            release_lane.commands,
        )
        self.assertIn("continue-on-error: true", release_lane.workflow_markers)

    def test_benchmark_constitution_rejects_null_stage_timings(self) -> None:
        module = self._load_script_module(
            "check_benchmark_constitution.py", "check_benchmark_constitution_for_test"
        )

        missing = module.row_missing_fields(
            {
                "engine": "shardloom-native-vortex",
                "scenario_name": "null timing",
                "source_state_id": "source-state://null-timing",
                "selected_execution_mode": "native_vortex",
                "output_format": "inline_jsonl",
                "correctness_digest": "fnv1a64:abc",
                "cache_mode": "cold",
                "scenario_compute_millis": None,
                "cost_unit": "local_wall_time",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            },
            environment={"cpu": "test"},
            build_profile={"build_profile": "debug"},
            claim_bearing=False,
        )

        self.assertIn("stage_timings", missing)

    def test_admitted_semantics_missing_matrix_reports_remaining_gaps(self) -> None:
        module = self._load_script_module(
            "check_admitted_semantics_matrix.py",
            "check_admitted_semantics_matrix_for_test",
        )

        _rows, summary = module.validate_matrix_manifest(None, {"case_b", "case_a"})

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["remaining_matrix_gaps"], ["case_a", "case_b"])

    def test_website_readiness_mirror_diagnostics_use_repo_root(self) -> None:
        module = self._load_script_module(
            "check_website_readiness.py", "check_website_readiness_for_test"
        )

        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir) / "checkout"
            source = repo_root / "docs" / "architecture" / "flow.md"
            mirror = repo_root / "website" / "assets" / "data" / "flow.md"
            source.parent.mkdir(parents=True)
            mirror.parent.mkdir(parents=True)
            source.write_text("canonical\n", encoding="utf-8")
            mirror.write_text("stale\n", encoding="utf-8")

            blockers: list[str] = []
            module.check_mirrored_file(
                source=source,
                mirror=mirror,
                label="flow snapshot",
                repo_root=repo_root,
                blockers=blockers,
            )

        self.assertEqual(
            blockers,
            [
                "flow snapshot drift: website/assets/data/flow.md does not match "
                "docs/architecture/flow.md"
            ],
        )


if __name__ == "__main__":
    unittest.main()
