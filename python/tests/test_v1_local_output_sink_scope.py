import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from shardloom import ShardLoomContext


def _load_scope_validator():
    script = REPO_ROOT / "scripts" / "check_v1_local_output_sink_scope.py"
    spec = importlib.util.spec_from_file_location(
        "check_v1_local_output_sink_scope_for_test",
        script,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load v1 local output/sink scope validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class V1LocalOutputSinkScopeTests(unittest.TestCase):
    def test_context_report_exposes_v1_scope_contract(self) -> None:
        report = ShardLoomContext(client=None).local_output_sink_scope_report()

        self.assertEqual(
            report.schema_version,
            "shardloom.v1_local_output_sink_scope.v1",
        )
        self.assertEqual(
            report.scope_document,
            "docs/architecture/v1-local-output-sink-scope.md",
        )
        self.assertTrue(report.v1_scope_ready)
        self.assertTrue(report.all_write_methods_registered)
        self.assertTrue(report.all_write_methods_no_fallback_no_external_engine)
        self.assertTrue(report.all_output_routes_no_fallback_no_external_engine)
        self.assertTrue(report.all_output_routes_emit_sink_evidence)
        self.assertTrue(report.all_feature_gated_formats_labeled)
        self.assertTrue(report.write_policy_contract_ready)
        self.assertEqual(len(report.supported_output_formats), 7)
        self.assertEqual(len(report.user_write_methods), 9)
        self.assertIn("append_mode", report.unsupported_boundary_ids)
        self.assertFalse(report.performance_claim_allowed)
        self.assertFalse(report.production_claim_allowed)
        self.assertFalse(report.spark_replacement_claim_allowed)

    def test_scope_validator_passes_current_repo_contract(self) -> None:
        module = _load_scope_validator()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["v1_scope_ready"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertTrue(report["local_output_sink_benchmark_required_fields_ready"])
        self.assertTrue(report["local_output_sink_benchmark_replay_ready"])
        self.assertGreater(
            report["local_output_sink_benchmark_rows_with_required_fields"],
            0,
        )
        self.assertEqual(len(report["supported_output_formats"]), 7)
        self.assertEqual(len(report["user_write_methods"]), 9)
        self.assertEqual(len(report["golden_fixture_paths"]), 3)


if __name__ == "__main__":
    unittest.main()
