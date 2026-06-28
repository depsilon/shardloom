from __future__ import annotations

import importlib.util
import re
import sys
import unittest
from pathlib import Path


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
    def test_current_repo_graduation_matrix_covers_cli_and_python_surfaces(self) -> None:
        module = load_graduation_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertGreaterEqual(report["matrix_row_count"], 10)
        self.assertEqual(report["context_method_count"], 97)
        self.assertEqual(report["client_method_count"], 122)
        self.assertEqual(report["cli_command_count"], documented_cli_command_count())
        self.assertTrue(
            report["acceptance_summary"]["all_python_context_methods_classified"]
        )
        self.assertTrue(report["acceptance_summary"]["all_cli_commands_classified"])
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])

        by_id = {row["row_id"]: row for row in report["rows"]}
        self.assertEqual(
            by_id["local_sql_python_dataframe_runtime"]["graduation_posture"],
            "high_level_context",
        )
        self.assertIn(
            "local-source-runtime",
            by_id["local_sql_python_dataframe_runtime"]["cli_commands"],
        )
        self.assertEqual(
            by_id["internal_source_smoke_client_helpers"]["graduation_posture"],
            "not_user_facing",
        )
        self.assertEqual(
            by_id["feature_gated_structured_local_inputs"]["graduation_posture"],
            "feature_gated",
        )

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


if __name__ == "__main__":
    unittest.main()
