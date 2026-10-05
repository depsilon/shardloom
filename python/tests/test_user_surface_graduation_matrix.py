from __future__ import annotations

import importlib.util
import re
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_graduation_module():
    module_path = REPO_ROOT / "scripts" / "check_user_surface_graduation_matrix.py"
    spec = importlib.util.spec_from_file_location(
        "check_user_surface_graduation_matrix_for_test",
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    script_dir = str(module_path.parent)
    original_path = list(sys.path)
    sys.path[:] = [entry for entry in sys.path if entry != script_dir]
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_path
        sys.modules.pop(spec.name, None)
    return module


def documented_cli_command_count() -> int:
    status_path = REPO_ROOT / "docs" / "status" / "cli-command-registry.md"
    match = re.search(
        r"^Registered command count:\s*(\d+)\s*$",
        status_path.read_text(encoding="utf-8"),
        flags=re.MULTILINE,
    )
    assert match is not None, "missing registered command count in CLI registry status"
    return int(match.group(1))


class UserSurfaceGraduationMatrixTests(unittest.TestCase):
    def test_validator_accepts_a_hand_declared_complete_shared_native_matrix(self) -> None:
        module = load_graduation_module()
        context_methods = module.public_methods(
            REPO_ROOT, "python/src/shardloom/context.py", "ShardLoomContext"
        )
        client_methods = module.public_methods(
            REPO_ROOT, "python/src/shardloom/client.py", "ShardLoomClient"
        )
        shared = {
            "row_id": "shared_native_workflow",
            "graduation_posture": "high_level_context",
            "support_state": "global_runtime_supported",
            "cli_commands": ["run"],
            "context_methods": ["sql"],
            "client_methods": ["public_workflow_run"],
            "runtime_route": "native_vortex_query",
            "promotion_criteria": "shared native execution with typed results",
            "evidence_refs": ["native_vortex_plan_route_family"],
            "claim_boundary": "Admitted operations execute natively; unsupported operations fail explicitly.",
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        other = {
            **shared,
            "row_id": "diagnostics_and_declarations",
            "graduation_posture": "diagnostic_only",
            "support_state": "diagnostic_only",
            "cli_commands": [],
            "context_methods": [name for name in context_methods if name != "sql"],
            "client_methods": [name for name in client_methods if name != "public_workflow_run"],
            "runtime_route": "side_effect_free_metadata_or_deterministic_diagnostic",
        }
        rows = [shared, other]
        matrix = SimpleNamespace(
            schema_version=module.SCHEMA_VERSION,
            posture_vocabulary=module.POSTURE_VOCABULARY,
            all_rows_have_allowed_posture=True,
            all_high_level_rows_have_runtime_evidence=True,
            all_no_fallback_no_external_engine=True,
        )

        self.assertEqual(module.validate_python_matrix(REPO_ROOT, matrix, rows), [])

    def test_current_repo_graduation_matrix_covers_cli_and_python_surfaces(self) -> None:
        module = load_graduation_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertGreaterEqual(report["matrix_row_count"], 1)
        self.assertGreater(report["context_method_count"], 0)
        self.assertGreater(report["client_method_count"], 0)
        self.assertEqual(report["cli_command_count"], documented_cli_command_count())
        self.assertEqual(report["report_kind"], "static_surface_classification")
        self.assertFalse(report["runtime_execution_performed"])
        self.assertFalse(report["performance_evidence_produced"])
        self.assertTrue(report["acceptance_summary"]["static_discovery_is_not_runtime_or_performance_proof"])
        self.assertTrue(report["acceptance_summary"]["shared_native_workflow_uses_native_query_route"])
        self.assertTrue(
            report["acceptance_summary"]["all_python_context_methods_classified"]
        )
        self.assertTrue(report["acceptance_summary"]["all_cli_commands_classified"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

        by_id = {row["row_id"]: row for row in report["rows"]}
        self.assertEqual(
            by_id["shared_native_workflow"]["graduation_posture"],
            "high_level_context",
        )
        self.assertEqual(by_id["shared_native_workflow"]["runtime_route"], "native_vortex_query")
        self.assertIn("from_rows", by_id["shared_native_workflow"]["context_methods"])
        self.assertIn("public_workflow_run", by_id["shared_native_workflow"]["client_methods"])

    def test_validator_rejects_missing_public_python_method_coverage(self) -> None:
        module = load_graduation_module()
        matrix = module.load_python_matrix(REPO_ROOT)
        rows = [
            module.row_payload(row)
            for row in matrix.rows
            if row.row_id != "context_construction"
        ]

        blockers = module.validate_python_matrix(REPO_ROOT, matrix, rows)

        self.assertTrue(
            any("context methods lack graduation posture" in blocker for blocker in blockers)
        )

    def test_validator_rejects_duplicate_public_python_method_coverage(self) -> None:
        module = load_graduation_module()
        matrix = module.load_python_matrix(REPO_ROOT)
        rows = [module.row_payload(row) for row in matrix.rows]
        rows[0]["client_methods"].append("run")

        blockers = module.validate_python_matrix(REPO_ROOT, matrix, rows)

        self.assertTrue(
            any("client methods have multiple graduation postures" in blocker for blocker in blockers)
        )

    def test_validator_rejects_unknown_methods_and_wrong_shared_route_owner(self) -> None:
        module = load_graduation_module()
        matrix = module.load_python_matrix(REPO_ROOT)
        rows = [module.row_payload(row) for row in matrix.rows]
        shared = next(row for row in rows if row["row_id"] == "shared_native_workflow")
        shared["runtime_route"] = "format_specific_runtime"
        shared["context_methods"].append("not_a_context_method")

        blockers = module.validate_python_matrix(REPO_ROOT, matrix, rows)

        self.assertTrue(any("shared_native_workflow must use" in blocker for blocker in blockers))
        self.assertTrue(any("references missing context methods" in blocker for blocker in blockers))


if __name__ == "__main__":
    unittest.main()
