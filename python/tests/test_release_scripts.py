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
            "selected_execution_mode": "prepared_vortex",
            "prepared_state_id": "prepared-state://claim-grade-row",
            "prepared_state_digest": "sha256:claim-grade-row",
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
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 1.4,
                "python_harness_overhead_millis": 0.4,
                "claim_gate_status": "claim_grade",
                "claim_grade_requirements_met": False,
                "claim_grade_missing_evidence": ["stale metrics value"],
            },
        }

        [published] = module.published_rows([row])

        self.assertIs(published["claim_grade_requirements_met"], True)
        self.assertEqual(published["claim_grade_missing_evidence"], [])
        self.assertEqual(published["runtime_execution_validation_status"], "passed")

    def test_benchmark_promoter_demotes_claim_grade_without_cold_lane_split(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_cold_lane_gate_for_test"
        )

        row = {
            "engine": "shardloom",
            "storage_format": "csv",
            "scenario_name": "claim grade missing cold lane",
            "status": "success",
            "selected_execution_mode": "compatibility_import_certified",
            "timing_scope": "cold_certified_end_to_end",
            "preparation_included": True,
            "compatibility_import_included": True,
            "source_state_id": "source-state://claim-grade-missing-cold-lane",
            "data_decoded": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "runtime_execution_certificate_id": "execution.claim-grade-missing-cold-lane",
            "runtime_execution_certificate_status": "certified",
            "claim_gate_status": "claim_grade",
            "claim_grade_requirements_met": True,
            "claim_grade_missing_evidence": [],
            "metrics": {
                "query_runtime_millis": 1.0,
                "source_read_millis": 0.1,
                "compatibility_to_vortex_import_millis": 0.2,
                "vortex_array_build_millis": 0.1,
                "vortex_write_millis": 0.1,
                "vortex_reopen_verify_millis": 0.1,
                "operator_compute_millis": 0.2,
                "total_runtime_millis": 1.0,
                "cli_process_wall_millis": 1.2,
                "python_harness_overhead_millis": 0.2,
            },
        }

        [published] = module.published_rows([row])

        self.assertEqual(published["claim_gate_status"], "not_claim_grade")
        self.assertFalse(published["claim_grade_requirements_met"])
        self.assertIn(
            "cold_lane_timing_split_status!=complete",
            published["claim_grade_missing_evidence"][0],
        )
        summary = module.comparative_summary(
            {"dataset": {}, "generated_at_utc": "2026-01-01T00:00:00Z"},
            [row],
            REPO_ROOT / "target" / "claim-grade-missing-cold-lane.json",
            "full_local_plus_spark",
        )
        self.assertEqual(
            summary["claim_gate_distribution"]["rows"][0][0],
            "not_claim_grade",
        )

    def test_full_local_plus_spark_keeps_broad_formats_supported_until_refresh(self) -> None:
        from benchmarks.traditional_analytics import run as benchmark_run
        from benchmarks.traditional_analytics.benchmark_registry import PROFILES

        profile = PROFILES["full_local_plus_spark"]

        self.assertEqual(
            benchmark_run.FORMAT_ORDER,
            ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc"),
        )
        self.assertEqual(
            profile.required_formats,
            ("csv", "parquet"),
        )
        self.assertEqual(profile.optional_formats, ("jsonl", "arrow-ipc", "avro", "orc"))

    def test_benchmark_promoter_keeps_deferred_broad_formats_optional(self) -> None:
        module = self._load_script_module(
            "promote_benchmark_artifact.py", "promote_benchmark_formats_for_test"
        )
        rows = [
            {"storage_format": data_format}
            for data_format in ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc")
        ]

        table = module.format_coverage_table(
            {
                "format_order": [
                    "csv",
                    "jsonl",
                    "parquet",
                    "arrow-ipc",
                    "avro",
                    "orc",
                ]
            },
            rows,
            "full_local_plus_spark",
        )
        by_format = {row[0]: row for row in table["rows"]}

        for data_format in ("csv", "parquet"):
            self.assertEqual(by_format[data_format][1], "required")
            self.assertEqual(by_format[data_format][2], "available")
        for data_format in ("jsonl", "arrow-ipc", "avro", "orc"):
            self.assertEqual(by_format[data_format][1], "optional")
            self.assertEqual(by_format[data_format][2], "available")

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

    def _write_production_usability_docs(self, repo_root: Path) -> None:
        docs = {
            "README.md": (
                "docs/getting-started/install.md\n"
                "docs/getting-started/first-10-minutes.md\n"
                "scripts\\release_dry_run_proof.py\n"
                "package-channel evidence is still gated\n"
            ),
            "docs/getting-started/install.md": (
                "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1\n"
                "pip --no-index\n"
                "SHARDLOOM_BIN\n"
            ),
            "docs/getting-started/first-10-minutes.md": (
                "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1\n"
                "ctx.from_rows\nctx.range\npublic package release\n"
            ),
            "docs/release/release-dry-run-proof.md": (
                "clean virtual environment\n"
                "generated_source_user_rows_smoke_performed=true\n"
                "prepared_native_benchmark_smoke_performed=true\n"
            ),
            "docs/release/production-usability-gate.md": (
                "shardloom.production_usability_gate.v1\n"
                "python scripts\\check_production_usability_gate.py\n"
                "public_release_claim_allowed=false\n"
            ),
            "docs/release/package-channel-readiness-matrix.md": (
                "Package Channel Readiness Matrix\nscripts/release_dry_run_proof.py\n"
            ),
            "docs/release/hard-release-readiness-gate.md": (
                "public_release_claim_allowed=false\nclean_conda_env_install_status=passed\n"
            ),
            "docs/release/known-unsupported-paths.md": (
                "fallback_attempted=false\nexternal_engine_invoked=false\n"
            ),
            "website-src/src/pages/start.astro": (
                "release_dry_run_proof.py\ncheck_production_usability_gate.py\n"
            ),
            "SECURITY.md": "security policy\n",
            "LICENSE": "Apache-2.0\n",
            "NOTICE": "ShardLoom\n",
            "python/pyproject.toml": 'license-files = ["LICENSE", "NOTICE"]\n',
        }
        for relative, content in docs.items():
            path = repo_root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def _production_usability_payloads(self, module: object, repo_root: Path) -> dict[str, object]:
        wheel = repo_root / "python" / "dist" / "shardloom-0.1.0-py3-none-any.whl"
        binary = repo_root / "target" / "debug" / "shardloom.exe"
        wheel.parent.mkdir(parents=True, exist_ok=True)
        binary.parent.mkdir(parents=True, exist_ok=True)
        wheel.write_text("wheel", encoding="utf-8")
        binary.write_text("binary", encoding="utf-8")
        false_fields = {
            "publication_attempted": False,
            "tag_created": False,
            "secrets_required": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        rows = [
            {
                "id": row_id,
                "support_state": "executable",
                "claim_gate_status": "claim_safe_discovery",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for row_id in [
                "cli_status_capability_reports",
                "python_status_capabilities",
                "python_generated_source_helpers",
                "cli_prepared_vortex_batch_benchmark",
            ]
        ]
        rows.extend(
            [
                {
                    "id": row_id,
                    "support_state": "blocked",
                    "claim_gate_status": "not_claim_grade",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
                for row_id in ["claim_production_readiness", "claim_package_publication"]
            ]
        )
        rows.extend(
            {
                "id": f"dummy_{index}",
                "support_state": "report_only",
                "claim_gate_status": "not_claim_grade",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for index in range(14)
        )
        return {
            "dry_run": {
                "schema_version": "shardloom.release_dry_run_proof.v1",
                "proof_status": "passed",
                "clean_venv_install_status": "passed",
                "clean_conda_env_install_status": "skipped_tool_missing",
                "clean_conda_env_install_required": False,
                "local_wheel": str(wheel),
                "local_cli_binary": str(binary),
                "publication_attempted": False,
                "tag_created": False,
                "secrets_required": False,
                "external_runtime_dependencies_added": False,
                "fallback_engine_dependency_added": False,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "public_package_release_claim_allowed": False,
                "wheel_import_and_client_smoke_performed": True,
                "cli_status_smoke_performed": True,
                "cli_capabilities_smoke_performed": True,
                "local_python_example_smoke_performed": True,
                "generated_output_proof_distinct_from_no_dataset_smoke": True,
                "generated_source_user_rows_smoke_performed": True,
                "generated_source_range_smoke_performed": True,
                "prepared_native_benchmark_smoke_performed": True,
                "provenance_dry_run_performed": True,
                "sbom_checksum_manifest_generated": True,
                "steps": [
                    {"name": name, "returncode": 0}
                    for name in module.DRY_RUN_REQUIRED_STEPS
                ],
            },
            "package_report": {
                "schema_version": "shardloom.package_channel_readiness_report.v1",
                "status": "passed",
                "local_gate_evidence_required": True,
                "local_gate_evidence_status": "passed",
                "public_package_release_claim_allowed": False,
                "ready_channel_count": 0,
                "expected_channel_count": 9,
                **false_fields,
            },
            "release_security": {
                "schema_version": "shardloom.release_security_gate_report.v1",
                "status": "passed",
                "blockers": [],
                **false_fields,
            },
            "contribution_governance": {
                "schema_version": "shardloom.contribution_governance_report.v1",
                "status": "passed",
                "blockers": [],
                **false_fields,
            },
            "final_rehearsal": {
                "schema_version": "shardloom.final_release_rehearsal_report.v1",
                "status": "blocked",
                "rehearsal_status": "blocked",
                "claim_gate_status": "not_claim_grade",
                "local_artifacts_only": True,
                "public_release_claim_allowed": False,
                "public_package_claim_allowed": False,
                "publication_human_approved": False,
                "signing_key_used": False,
                "blockers": ["hard release claim still blocked"],
                **false_fields,
            },
            "website_report": {
                "schema_version": "shardloom.website_readiness.v3",
                "checked_pages": ["start.html"],
                "checked_assets": ["assets/site.css"],
                "blockers": [],
            },
            "runs_today": {
                "schema_version": "shardloom.runs_today_support_matrix.v1",
                "all_rows_no_fallback_no_external_engine": True,
                "performance_claim_allowed": False,
                "support_state_counts": {"blocked": 2},
                "rows": rows,
            },
        }

    def test_production_usability_gate_accepts_local_no_publication_evidence(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py", "check_production_usability_gate_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_production_usability_docs(repo_root)
            payloads = self._production_usability_payloads(module, repo_root)

            report = module.build_report(
                repo_root=repo_root,
                release_dry_run_ref="target/release-dry-run-proof/transcript.json",
                package_channel_report_ref="target/package-channel-readiness-report.json",
                release_security_report_ref="target/release-security-gate-report.json",
                contribution_governance_report_ref="target/contribution-governance-report.json",
                final_release_rehearsal_report_ref="target/final-release-rehearsal/final-release-rehearsal-report.json",
                website_readiness_report_ref="target/website-readiness-report.json",
                benchmark_manifest_ref="website/assets/benchmarks/latest/manifest.json",
                runs_today_matrix_ref="docs/status/runs-today-support-matrix.json",
                dry_run=payloads["dry_run"],
                package_report=payloads["package_report"],
                release_security=payloads["release_security"],
                contribution_governance=payloads["contribution_governance"],
                final_rehearsal=payloads["final_rehearsal"],
                website_report=payloads["website_report"],
                benchmark_manifest_path=REPO_ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json",
                runs_today=payloads["runs_today"],
            )

            self.assertEqual(report["status"], "passed", report["blockers"])
            self.assertEqual(report["claim_gate_status"], "not_claim_grade")
            self.assertFalse(report["public_release_claim_allowed"])
            self.assertFalse(report["public_package_claim_allowed"])
            self.assertIn("GAR-RUNTIME-IMPL-4S", report["covered_phase_items"])

    def test_production_usability_gate_rejects_fallback_or_publication_drift(self) -> None:
        module = self._load_script_module(
            "check_production_usability_gate.py", "check_production_usability_gate_blocker_for_test"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_production_usability_docs(repo_root)
            payloads = self._production_usability_payloads(module, repo_root)
            payloads["dry_run"]["fallback_attempted"] = True

            _, blockers = module.validate_release_dry_run(repo_root, payloads["dry_run"])

            self.assertIn("release dry-run fallback_attempted must be false", blockers)

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
        self.assertIn("cold_lane_attribution", missing)

    def test_benchmark_constitution_accepts_complete_cold_lane_split(self) -> None:
        module = self._load_script_module(
            "check_benchmark_constitution.py",
            "check_benchmark_constitution_cold_lane_for_test",
        )

        missing = module.row_missing_fields(
            {
                "engine": "shardloom-prepared-vortex",
                "scenario_name": "warm prepared query",
                "source_format": "vortex",
                "selected_execution_mode": "prepared_vortex",
                "output_format": "inline_jsonl",
                "correctness_digest": "fnv1a64:abc",
                "cache_mode": "warm",
                "query_runtime_millis": 1.0,
                "vortex_scan_millis": 0.2,
                "operator_compute_millis": 0.5,
                "evidence_render_millis": 0.1,
                "cli_process_wall_millis": 2.0,
                "python_harness_overhead_millis": 0.3,
                "cold_lane_timing_split_status": "complete",
                "cost_unit": "local_wall_time",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            },
            environment={"cpu": "test"},
            build_profile={"build_profile": "debug"},
            claim_bearing=True,
        )

        self.assertNotIn("stage_timings", missing)
        self.assertNotIn("cold_lane_attribution", missing)

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

    def test_foundry_dev_stack_starter_accepts_local_runtime_proof(self) -> None:
        module = self._load_script_module(
            "check_foundry_dev_stack_starter.py",
            "check_foundry_dev_stack_starter_for_test",
        )

        manifest = json.loads(
            (REPO_ROOT / "docs" / "foundry" / "dev-stack-starter-kit.json").read_text(
                encoding="utf-8"
            )
        )
        doc_text = (
            REPO_ROOT / "docs" / "foundry" / "dev-stack-starter-kit.md"
        ).read_text(encoding="utf-8")

        blockers = module.validate_manifest(manifest)
        blockers.extend(module.validate_doc(doc_text))
        blockers.extend(module.validate_example_files(REPO_ROOT))

        self.assertEqual(blockers, [])

    def test_foundry_proof_posture_promotes_local_style_generated_and_staged_proof(self) -> None:
        module = self._load_script_module(
            "foundry_proof_of_use.py",
            "foundry_proof_of_use_for_test",
        )
        transform = {
            "generated_output_execution_performed": True,
            "generated_source_created": True,
            "generated_source_kind": "user_rows",
            "generated_source_row_count": 2,
            "generated_source_certificate_status": "present",
            "output_native_io_certificate_status": "certified_local_file_sink",
            "generated_output_fanout_output_count": 1,
            "generated_output_fanout_result_reuse_hit": True,
            "foundry_style_output_api_invoked": True,
            "foundry_style_result_dataset_written": True,
            "foundry_style_evidence_dataset_written": True,
            "staged_input_transform_execution_performed": True,
            "staged_input_transform_output_row_count": 3,
            "output_evidence_dataset_written": True,
        }

        fanout = module.foundry_generated_output_fanout_posture(transform)
        boundary = module.foundry_generated_output_boundary(transform)
        scale = module.foundry_scale_proof_boundary(27, transform)

        self.assertEqual(fanout["support_status"], "local_style_smoke_supported")
        self.assertEqual(fanout["claim_gate_status"], "fixture_smoke_only")
        self.assertEqual(fanout["blockers"], [])
        self.assertFalse(fanout["foundry_output_api_invoked"])
        self.assertTrue(fanout["foundry_style_output_api_invoked"])
        self.assertEqual(
            boundary["boundary_status"],
            "local_style_dataset_output_written_real_foundry_blocked",
        )
        self.assertFalse(boundary["public_foundry_generated_output_claim_allowed"])
        self.assertEqual(
            scale["proof_boundary_status"],
            "local_style_staged_transform_and_evidence_dataset_written_real_foundry_blocked",
        )
        self.assertEqual(scale["foundry_style_input_dataset_count"], 1)
        self.assertEqual(scale["foundry_style_output_dataset_count"], 2)


if __name__ == "__main__":
    unittest.main()
