from __future__ import annotations

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom.errors import ShardLoomCommandError
from shardloom.models import OutputEnvelope


class HumanCommandErrorTests(unittest.TestCase):
    def error(self, diagnostics: list[dict[str, object]]) -> ShardLoomCommandError:
        envelope = OutputEnvelope.from_field_mapping(
            {"fallback_attempted": "false", "execution": "blocked_before_execution"},
            command="run", status="error", summary="The query could not run.",
            human_text="Original report",
        )
        raw = copy.deepcopy(envelope.raw)
        raw["diagnostics"] = diagnostics
        return ShardLoomCommandError(
            ("shardloom", "run", "s3://private-user@bucket/data?token=private-token"),
            2, OutputEnvelope.from_json(raw), "captured-secret-stderr",
        )

    def diagnostic(self, **changes: object) -> dict[str, object]:
        value: dict[str, object] = {
            "code": "SL_INVALID_INPUT", "severity": "error", "category": "invalid_input",
            "message": "Invalid input provided.", "feature": "source_validation",
            "reason": "The input file changed after it was opened.",
            "suggested_next_step": "Reopen the source before running another query.",
            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
        }
        value.update(changes)
        return value

    def test_problem_and_recovery_precede_classification_without_changing_payload(self) -> None:
        error = self.error([self.diagnostic()])
        original = copy.deepcopy(error.envelope.raw)
        text = str(error)
        self.assertTrue(text.startswith("The input file changed after it was opened.\n"))
        self.assertLess(text.index("Next step: Reopen"), text.index("Diagnostic: SL_INVALID_INPUT"))
        self.assertIn("(error; invalid_input)", text)
        self.assertIn("Status: error; exit code: 2; fallback_attempted=False", text)
        self.assertEqual(error.envelope.raw, original)
        self.assertEqual(error.diagnostics[0].message, "Invalid input provided.")
        self.assertFalse(error.diagnostics[0].fallback.attempted)

    def test_fatal_then_error_take_precedence_over_earlier_warning(self) -> None:
        warning = self.diagnostic(severity="warning", message="An earlier warning.")
        failure = self.diagnostic(message="The query failed.", reason="Unsupported data type.")
        fatal = self.diagnostic(severity="fatal", message="Execution stopped.")
        for diagnostics, first in [
            ([warning, failure], "The query failed."),
            ([warning, failure, fatal], "Execution stopped."),
        ]:
            with self.subTest(first=first):
                error = self.error(diagnostics)
                self.assertTrue(str(error).startswith(first + "\n"))
                self.assertEqual(len(error.diagnostics), len(diagnostics))

    def test_missing_or_blank_recovery_is_not_invented(self) -> None:
        for empty in [None, "", "  \t"]:
            with self.subTest(empty=empty):
                error = self.error([self.diagnostic(reason=empty, suggested_next_step=empty)])
                self.assertTrue(str(error).startswith("Invalid input provided.\nDiagnostic:"))
                self.assertNotIn("Next step:", str(error))

    def test_no_diagnostic_uses_summary_and_preserves_status(self) -> None:
        error = self.error([])
        self.assertTrue(str(error).startswith("The query could not run.\nStatus: error;"))
        self.assertNotIn("Diagnostic:", str(error))
        self.assertNotIn("Next step:", str(error))

    def test_warning_alone_does_not_replace_the_failure_summary(self) -> None:
        warning = self.diagnostic(severity="warning", message="A writer metadata warning.")
        error = self.error([warning])
        self.assertTrue(str(error).startswith("The query could not run.\nStatus: error;"))
        self.assertEqual(error.diagnostics[0].message, warning["message"])
        self.assertNotIn("Next step:", str(error))

    def test_display_does_not_echo_command_credentials_or_captured_stderr(self) -> None:
        error = self.error([self.diagnostic()])
        for secret in ["private-user", "private-token", "captured-secret-stderr"]:
            self.assertNotIn(secret, str(error))
        self.assertIn("private-token", error.command[-1])
        self.assertEqual(error.stderr, "captured-secret-stderr")


if __name__ == "__main__":
    unittest.main()
