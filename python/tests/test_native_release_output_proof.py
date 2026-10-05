# SPDX-License-Identifier: Apache-2.0
"""Release proofs must validate committed values and the native execution result."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from release_dry_run_proof import generated_range_runtime_script, generated_user_rows_runtime_script


class NativeReleaseOutputProofTests(unittest.TestCase):
    def run_proof(self, factory, rows, **report_changes):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "quoted ' native output.jsonl"
            report = SimpleNamespace(
                envelope=SimpleNamespace(status="success", human_text="native success"),
                output_commit_status="committed", fallback_attempted=False,
                external_engine_invoked=False, native_io_certificate_status="certified",
                claim_gate_status="not_claim_grade",
            )
            for key, value in report_changes.items():
                setattr(report, key, value)

            def write_output(path, **_options):
                self.assertEqual(Path(path), target)
                target.write_text("".join(json.dumps(row) + "\n" for row in rows))
                return report

            frame = SimpleNamespace(write=write_output)
            context = SimpleNamespace(from_rows=Mock(return_value=frame), range=Mock(return_value=frame))
            output = io.StringIO()
            with patch("shardloom.context", return_value=context), contextlib.redirect_stdout(output):
                exec(factory(target), {})
            return output.getvalue()

    def test_user_rows_and_range_validate_complete_committed_files(self):
        user_rows = [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]
        for factory, rows in (
            (generated_user_rows_runtime_script, user_rows),
            (generated_range_runtime_script, [{"id": value} for value in range(8)]),
        ):
            with self.subTest(factory=factory.__name__):
                self.assertIn(f"native_result_rows_validated={len(rows)}", self.run_proof(factory, rows))

    def test_success_without_correct_values_or_safe_commit_fails(self):
        expected = [{"id": value} for value in range(8)]
        for rows, changes in (
            (expected[:-1], {}),
            ([{"id": 999}, *expected[1:]], {}),
            (expected, {"fallback_attempted": True}),
            (expected, {"external_engine_invoked": True}),
            (expected, {"output_commit_status": "not_committed"}),
            (expected, {"envelope": SimpleNamespace(status="unsupported", human_text="denied")}),
        ):
            with self.subTest(changes=changes, rows=rows), self.assertRaises(AssertionError):
                self.run_proof(generated_range_runtime_script, rows, **changes)


if __name__ == "__main__":
    unittest.main()
