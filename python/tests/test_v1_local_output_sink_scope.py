import copy
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from shardloom import ShardLoomContext


def _load_scope_validator():
    script = REPO_ROOT / "scripts" / "check_v1_local_output_sink_scope.py"
    spec = importlib.util.spec_from_file_location("check_v1_local_output_sink_scope_for_test", script)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load v1 local output/sink scope validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _ReportProxy:
    def __init__(self, base, *, routes=None):
        self._base = base
        self.output_user_route_rows = base.output_user_route_rows if routes is None else routes

    def __getattr__(self, name):
        return getattr(self._base, name)


class V1LocalOutputSinkScopeTests(unittest.TestCase):
    def test_context_report_exposes_single_native_owner_and_declared_fields(self) -> None:
        report = ShardLoomContext(client=None).local_output_sink_scope_report()
        route, = report.output_user_route_rows

        self.assertEqual(report.schema_version, "shardloom.v1_local_output_sink_scope.v1")
        self.assertEqual(report.scope_document, "docs/architecture/v1-local-output-sink-scope.md")
        self.assertEqual(report.output_route_ids, ("native_vortex_query",))
        self.assertEqual(route.owner, "shared_native_workflow")
        self.assertEqual(route.execution_mode, "native_vortex")
        self.assertTrue(report.v1_scope_ready)
        self.assertTrue(report.all_write_methods_registered)
        self.assertTrue(report.all_write_methods_no_fallback_no_external_engine)
        self.assertTrue(report.all_output_routes_no_fallback_no_external_engine)
        self.assertTrue(report.all_output_routes_emit_sink_evidence)
        self.assertTrue(report.all_feature_gated_formats_labeled)
        self.assertTrue(report.write_policy_contract_ready)
        self.assertEqual(len(report.supported_output_formats), 8)
        self.assertEqual(len(report.default_output_formats), 3)
        self.assertEqual(len(report.user_write_methods), 10)
        self.assertEqual(len(report.required_runtime_fields), 14)
        self.assertIn("native_vortex_result_export_target_count", report.required_runtime_fields)
        self.assertIn("json", report.supported_output_formats)
        self.assertIn("write_json", report.user_write_methods)
        self.assertIn("append_mode", report.unsupported_boundary_ids)
        self.assertFalse(report.performance_claim_allowed)
        self.assertFalse(report.production_claim_allowed)
        self.assertFalse(report.spark_replacement_claim_allowed)

    def test_scope_validator_reports_declarative_contract_without_runtime_claims(self) -> None:
        module = _load_scope_validator()
        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["evidence_class"], "declarative_contract")
        self.assertFalse(report["runtime_evidence_verified"])
        self.assertTrue(report["declarative_contract_ready"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertEqual(report["output_route_ids"], ["native_vortex_query"])
        self.assertEqual(report["output_user_route_rows"][0]["owner"], "shared_native_workflow")
        self.assertEqual(report["output_user_route_rows"][0]["execution_mode"], "native_vortex")
        self.assertEqual(len(report["required_runtime_fields"]), 14)
        self.assertEqual(len(report["golden_fixture_paths"]), 3)
        self.assertNotIn("benchmark_artifact_summary", report)
        self.assertNotIn("local_output_sink_benchmark_replay_ready", report)

    def test_context_validator_rejects_duplicate_or_misowned_output_route(self) -> None:
        module = _load_scope_validator()
        report = ShardLoomContext(client=None).local_output_sink_scope_report()
        route = copy.copy(report.output_user_route_rows[0])
        object.__setattr__(route, "owner", "separate_output_engine")

        blockers = module.validate_context_report(_ReportProxy(report, routes=(route,)))

        self.assertTrue(any("owner must be shared_native_workflow" in item for item in blockers))

    def _copied_fixture_tree(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        fixture_source = REPO_ROOT / "docs/architecture/fixtures/v1-local-output-sink"
        fixture_target = root / "docs/architecture/fixtures/v1-local-output-sink"
        fixture_target.mkdir(parents=True)
        for source in fixture_source.glob("*.json"):
            shutil.copyfile(source, fixture_target / source.name)
        return tmp, root

    def _mutate_fixture(self, root: Path, filename: str, edit) -> None:
        path = root / "docs/architecture/fixtures/v1-local-output-sink" / filename
        payload = json.loads(path.read_text(encoding="utf-8"))
        edit(payload)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_fixture_validator_rejects_field_set_and_policy_drift(self) -> None:
        module = _load_scope_validator()
        report = ShardLoomContext(client=None).local_output_sink_scope_report()

        for filename, edit, expected in (
            (
                "output-evidence-fields-golden.json",
                lambda payload: payload["declared_fields"].pop(),
                "declared fields mismatch",
            ),
            (
                "output-policy-matrix.json",
                lambda payload: next(
                    policy for policy in payload["policies"]
                    if policy["policy_id"] == "explicit_allow_overwrite"
                ).update(
                    runtime_posture="supported_explicit_local_policy",
                    existing_target_allowed=True,
                ),
                "policy contract mismatch for explicit_allow_overwrite",
            ),
        ):
            with self.subTest(filename=filename):
                tmp, root = self._copied_fixture_tree()
                try:
                    self._mutate_fixture(root, filename, edit)
                    blockers, _ = module.validate_fixtures(root, report)
                    self.assertTrue(any(expected in item for item in blockers), blockers)
                finally:
                    tmp.cleanup()

    def test_fixture_validator_rejects_synthetic_runtime_or_replay_evidence(self) -> None:
        module = _load_scope_validator()
        report = ShardLoomContext(client=None).local_output_sink_scope_report()
        tmp, root = self._copied_fixture_tree()
        try:
            self._mutate_fixture(
                root,
                "output-evidence-fields-golden.json",
                lambda payload: payload.update(
                    runtime_evidence_verified=True,
                    replay_result="verified",
                ),
            )
            blockers, _ = module.validate_fixtures(root, report)
            self.assertTrue(any("runtime_evidence_verified" in item for item in blockers), blockers)
            self.assertTrue(any("must not claim a replay result" in item for item in blockers), blockers)
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
