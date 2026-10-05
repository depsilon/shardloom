from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from shardloom import ShardLoomContext


def _load_scope_validator():
    script = REPO_ROOT / "scripts" / "check_v1_source_prepared_state_scope.py"
    spec = importlib.util.spec_from_file_location(
        "check_v1_source_prepared_state_scope_for_test",
        script,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load v1 source/prepared-state scope validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class V1SourcePreparedStateScopeTests(unittest.TestCase):
    def test_context_discovery_is_side_effect_free_and_exposes_one_native_route(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            before = set(Path(tempdir).iterdir())
            with (
                mock.patch.object(Path, "mkdir", side_effect=AssertionError("mkdir called")),
                mock.patch.object(Path, "write_text", side_effect=AssertionError("write called")),
            ):
                report = ShardLoomContext(client=None).source_prepared_state_scope_report()
            self.assertEqual(set(Path(tempdir).iterdir()), before)

        self.assertEqual(report.prepared_route_ids, ("native_vortex_query",))
        self.assertEqual(report.state_owner, "ResidentVortexSession")
        self.assertEqual(report.reuse_scope, "native_session_or_explicit_vortex_artifact")
        self.assertEqual(
            report.reuse_policy,
            "validate_source_generation_and_declaration_before_each_execution",
        )
        self.assertFalse(report.query_answers_cached)
        self.assertTrue(report.all_no_fallback_no_external_engine)
        self.assertEqual(len(report.invalidation_case_ids), 7)
        self.assertEqual(len(report.golden_fixture_paths), 3)
        self.assertEqual(report.claim_gate_status, "not_claim_grade")
        self.assertFalse(report.performance_claim_allowed)
        self.assertFalse(report.production_claim_allowed)
        self.assertFalse(report.spark_replacement_claim_allowed)

    def test_validator_accepts_the_current_declarative_contract_only(self) -> None:
        validator = _load_scope_validator()

        report = validator.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["v1_scope_ready"])
        self.assertEqual(report["route_ids"], ["native_vortex_query"])
        self.assertEqual(report["state_owner"], "ResidentVortexSession")
        self.assertFalse(report["query_answers_cached"])
        self.assertEqual(len(report["invalidation_case_ids"]), 7)
        self.assertEqual(len(report["fixture_specs"]), 3)
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertEqual(report["claim_gate_status"], "not_claim_grade")
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        self.assertFalse(report["performance_claim_allowed"])
        self.assertFalse(report["production_claim_allowed"])
        self.assertFalse(report["spark_replacement_claim_allowed"])

    def _copy_fixtures(self, destination: Path) -> None:
        source = REPO_ROOT / "docs" / "architecture" / "fixtures" / "v1-source-prepared-state"
        shutil.copytree(source, destination / "docs/architecture/fixtures/v1-source-prepared-state")

    def test_validator_rejects_missing_fixture_and_matrix_case_drift(self) -> None:
        validator = _load_scope_validator()
        report = validator.load_context_report(REPO_ROOT)
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._copy_fixtures(root)
            missing = root / validator.FIXTURE_PATHS[1]
            missing.unlink()
            blockers, _ = validator.validate_fixtures(root, report)
            self.assertTrue(any("fixture unreadable or missing" in item for item in blockers))

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._copy_fixtures(root)
            matrix = root / validator.FIXTURE_PATHS[2]
            payload = json.loads(matrix.read_text(encoding="utf-8"))
            payload["cases"].pop()
            matrix.write_text(json.dumps(payload), encoding="utf-8")
            blockers, _ = validator.validate_fixtures(root, report)
            self.assertTrue(any("cases does not match" in item for item in blockers))

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._copy_fixtures(root)
            source_state = root / validator.FIXTURE_PATHS[0]
            payload = json.loads(source_state.read_text(encoding="utf-8"))
            payload["source_inputs"]["compatibility_formats"].remove("orc")
            source_state.write_text(json.dumps(payload), encoding="utf-8")
            blockers, _ = validator.validate_fixtures(root, report)
            self.assertTrue(any("source_inputs does not match" in item for item in blockers))

    def test_validator_rejects_unsafe_fixture_metadata(self) -> None:
        validator = _load_scope_validator()
        report = validator.load_context_report(REPO_ROOT)
        unsafe_fields = {
            "route_id": "retired_prepared_route",
            "state_owner": "PythonManifestCache",
            "reuse_policy": "reuse_without_generation_validation",
            "query_answers_cached": True,
            "fallback_attempted": True,
        }
        for field, value in unsafe_fields.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tempdir:
                root = Path(tempdir)
                self._copy_fixtures(root)
                fixture = root / validator.FIXTURE_PATHS[0]
                payload = json.loads(fixture.read_text(encoding="utf-8"))
                payload[field] = value
                fixture.write_text(json.dumps(payload), encoding="utf-8")
                blockers, _ = validator.validate_fixtures(root, report)
                self.assertTrue(
                    any(field in blocker for blocker in blockers),
                    blockers,
                )

    def test_validator_has_no_benchmark_artifact_input_or_legacy_route_fields(self) -> None:
        validator = _load_scope_validator()
        report = validator.build_report(REPO_ROOT)
        self.assertNotIn("benchmark_artifact_summary", report)
        self.assertNotIn("prepared_user_route_rows", report)
        self.assertNotIn("internal_source_smoke_route_ids", report)
        self.assertNotIn("generated_route_ids", report)
        self.assertNotIn("source_prepared_benchmark_required_fields_ready", report)
        self.assertNotIn("--benchmark-artifact", validator.parse_args.__code__.co_consts)


if __name__ == "__main__":
    unittest.main()
