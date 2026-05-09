from __future__ import annotations

import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from shardloom import ShardLoomClient, ShardLoomCommandError, ShardLoomProtocolError


class ShardLoomClientTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_status_appends_json_format_and_parses_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "engine", "value": "shardloom"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertEqual(result.command, "status")
        self.assertEqual(result.status, "success")
        self.assertFalse(result.fallback.attempted)
        self.assertEqual(result.field_map["engine"], "shardloom")

    def test_capabilities_scope_uses_explicit_scope(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["capabilities", "python", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "capabilities",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "scope", "value": "python"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).capabilities("python")

        self.assertEqual(result.field_map["scope"], "python")

    def test_vortex_run_passes_explicit_runtime_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["vortex-run", "file.vortex", "count", "8", "2", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "fallback_execution_allowed", "value": "false"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_run(
            "file.vortex", "count", memory_gb=8, max_parallelism=2
        )

        self.assertEqual(result.command, "vortex-run")
        self.assertEqual(result.field_map["fallback_execution_allowed"], "false")

    def test_unsupported_envelope_raises_with_diagnostics_and_fallback(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "capabilities",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "sql",
                        "reason": "not implemented",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [],
                }))
                sys.exit(1)
                """
            )
        )

        with self.assertRaises(ShardLoomCommandError) as raised:
            ShardLoomClient(binary=binary).capabilities("sql")

        error = raised.exception
        self.assertEqual(error.returncode, 1)
        self.assertFalse(error.envelope.fallback.attempted)
        self.assertEqual(error.envelope.diagnostics[0].code, "UnsupportedSql")

    def test_invalid_json_raises_protocol_error(self) -> None:
        binary = self.fake_cli("print('not-json')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_missing_envelope_fields_raise_protocol_error(self) -> None:
        binary = self.fake_cli("print('{\"schema_version\":\"shardloom.output.v1\"}')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_error_diagnostic_marks_envelope_as_error(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "Example",
                        "severity": "error",
                        "category": "invalid_input",
                        "message": "bad",
                        "feature": None,
                        "reason": None,
                        "suggested_next_step": None,
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [],
                }))
                """
            )
        )

        with self.assertRaises(ShardLoomCommandError):
            ShardLoomClient(binary=binary).status()

    def test_non_json_format_is_rejected(self) -> None:
        client = ShardLoomClient(binary=["shardloom"])

        with self.assertRaises(ValueError):
            client.run(["status", "--format", "text"])

    def test_explicit_binary_overrides_env_default(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "api-compat-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )
        old = os.environ.get("SHARDLOOM_BIN")
        os.environ["SHARDLOOM_BIN"] = "ignored-test-value"
        try:
            result = ShardLoomClient(binary=binary).api_compat_plan()
        finally:
            if old is None:
                os.environ.pop("SHARDLOOM_BIN", None)
            else:
                os.environ["SHARDLOOM_BIN"] = old

        self.assertEqual(result.command, "api-compat-plan")


if __name__ == "__main__":
    unittest.main()
