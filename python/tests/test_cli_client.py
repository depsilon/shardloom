from __future__ import annotations

import json
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from shardloom import (
    __version__,
    context as shardloom_context,
    CompatibilitySourceSmokeReport,
    ContextCapabilities,
    CapabilityView,
    LocalVortexPrimitiveSmokeReport,
    ShardLoomBinaryNotFoundError,
    ShardLoomClient,
    ShardLoomCommandError,
    ShardLoomContext,
    ShardLoomProtocolError,
    OutputEnvelope,
)


class ShardLoomClientTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_package_exports_non_placeholder_version(self) -> None:
        self.assertRegex(__version__, r"^\d+\.\d+\.\d+")
        self.assertNotEqual(__version__, "0.0.0")

    def test_status_appends_json_format_and_parses_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
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

    def test_typed_envelope_payloads_are_preserved(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "result": {"fields": [{"key": "engine", "value": "shardloom"}]},
                    "result_refs": [{"id": "result.local", "kind": "json", "status": "available", "uri": None}],
                    "artifacts": [{"artifact_id": "artifact.evidence", "artifact_kind": "evidence", "status": "available", "payload": {"fields": []}}],
                    "artifact_refs": [{"id": "artifact.ref", "kind": "file", "status": "available", "uri": "artifact.json"}],
                    "certificates": [{"id": "certificate.execution", "kind": "execution_certificate", "status": "available", "uri": None}],
                    "policy": {"fields": [{"key": "fallback_execution_allowed", "value": "false"}]},
                    "lifecycle": {"fields": [{"key": "phase", "value": "report_only"}]},
                    "capability_snapshot": {"fields": [{"key": "scope", "value": "status"}]},
                    "fields": [{"key": "engine", "value": "shardloom"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertEqual(result.result["fields"][0]["key"], "engine")
        self.assertEqual(result.result_refs[0]["id"], "result.local")
        self.assertEqual(result.artifacts[0]["artifact_id"], "artifact.evidence")
        self.assertEqual(result.artifact_refs[0]["uri"], "artifact.json")
        self.assertEqual(result.certificates[0]["kind"], "execution_certificate")
        self.assertEqual(result.policy["fields"][0]["key"], "fallback_execution_allowed")
        self.assertEqual(result.lifecycle["fields"][0]["value"], "report_only")
        self.assertEqual(result.capability_snapshot["fields"][0]["value"], "status")

    def test_capabilities_scope_uses_explicit_scope(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["capabilities", "python", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
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

    def test_from_env_reads_client_configuration_without_running_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
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

        client = ShardLoomClient.from_env(
            {
                "SHARDLOOM_REPO_ROOT": "repo",
                "SHARDLOOM_PROFILE_ORDER": "debug,release",
                "SHARDLOOM_TIMEOUT_SECONDS": "5",
            },
            binary=binary,
        )

        self.assertEqual(client.status().field("engine"), "shardloom")

    def test_from_env_rejects_invalid_timeout(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient.from_env({"SHARDLOOM_TIMEOUT_SECONDS": "soon"})

    def test_context_constructor_is_side_effect_free(self) -> None:
        ctx = shardloom_context(binary=["definitely-missing-shardloom"])

        self.assertIsInstance(ctx, ShardLoomContext)

    def test_smoke_check_runs_no_dataset_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["status", "--format", "json"]:
                    command = "status"
                    fields = [
                        {"key": "engine", "value": "shardloom"},
                        {"key": "cli_binary_version", "value": "0.1.0-test"},
                    ]
                elif args == ["capabilities", "python", "--format", "json"]:
                    command = "capabilities"
                    fields = [
                        {"key": "scope", "value": "python"},
                        {"key": "surface_components", "value": "thin_cli_json_wrapper,python_api"},
                    ]
                elif args == ["capabilities", "deployment", "--format", "json"]:
                    command = "capabilities"
                    fields = [{"key": "scope", "value": "deployment"}]
                elif args == ["input-adapters", "--format", "json"]:
                    command = "input-adapters"
                    fields = [{"key": "plan_only", "value": "true"}]
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).smoke_check()

        self.assertEqual(
            report.commands,
            ("status", "capabilities", "capabilities", "input-adapters"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertEqual(report.python_capabilities.field("scope"), "python")
        self.assertEqual(report.deployment_capabilities.field("scope"), "deployment")
        self.assertTrue(report.input_adapters.field_bool("plan_only"))
        self.assertEqual(report.python_package_version, __version__)
        self.assertEqual(report.protocol_version, "shardloom.output.v2")
        self.assertEqual(report.cli_version, "0.1.0-test")
        self.assertEqual(report.resolved_cli_path, sys.executable)
        self.assertIn("python_api", report.feature_gates)

    def test_context_capabilities_collects_typed_views_without_dataset_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["status", "--format", "json"]:
                    command = "status"
                    fields = [{"key": "fallback_execution_allowed", "value": "false"}]
                elif args == ["input-adapters", "--format", "json"]:
                    command = "input-adapters"
                    fields = [{"key": "plan_only", "value": "true"}]
                elif len(args) == 4 and args[0] == "capabilities" and args[2:] == ["--format", "json"]:
                    command = "capabilities"
                    scope = args[1]
                    fields = [
                        {"key": "scope", "value": scope},
                        {"key": "capability_status", "value": "planned"},
                    ]
                    if scope == "adapters":
                        fields.append({"key": "adapter_certification_required", "value": "true"})
                    if scope == "operators":
                        fields.append({"key": "materialization_boundary_reported", "value": "true"})
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        ctx = shardloom_context(binary=binary)
        capabilities = ctx.capabilities()

        self.assertIsInstance(capabilities, ContextCapabilities)
        self.assertIsInstance(capabilities.python, CapabilityView)
        self.assertEqual(capabilities.python.field("scope"), "python")
        self.assertEqual(capabilities.deployment.field("scope"), "deployment")
        self.assertEqual(capabilities.functions.capability_state, "planned")
        self.assertEqual(capabilities.sql_support.scope, "sql")
        self.assertIn("adapter_certification_required", capabilities.adapters.required_gates)
        self.assertIn(
            "materialization_boundary_reported",
            capabilities.operators.materialization_boundaries,
        )
        self.assertTrue(capabilities.input_adapters.field_bool("plan_only"))
        self.assertFalse(capabilities.fallback_attempted)
        self.assertEqual(ctx.functions().field("scope"), "functions")

    def test_vortex_run_passes_explicit_runtime_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["vortex-run", "file.vortex", "count", "8", "2", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
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
        self.assertEqual(result.field("fallback_execution_allowed"), "false")
        self.assertTrue(result.field_bool("fallback_execution_allowed") is False)

    def test_vortex_count_helper_dispatches_default_and_local_execution(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["vortex-count", "file.vortex", "--format", "json"]:
                    fields = [{"key": "local_execution", "value": "false"}]
                elif args == [
                    "vortex-count",
                    "file.vortex",
                    "--execute-local-encoded-count",
                    "8",
                    "2",
                    "--format",
                    "json",
                ]:
                    fields = [{"key": "local_execution", "value": "true"}]
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-count",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        default = client.vortex_count("file.vortex")
        executed = client.vortex_count(
            "file.vortex",
            execute_local_encoded_count=True,
            memory_gb=8,
            max_parallelism=2,
        )

        self.assertFalse(default.field_bool("local_execution"))
        self.assertTrue(executed.field_bool("local_execution"))

    def test_local_vortex_primitive_helpers_dispatch_cli_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                expected = {
                    ("vortex-count-where", "file.vortex", "gte:value:3", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-count-where",
                    ("vortex-filter", "file.vortex", "gte:value:3", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-filter",
                    ("vortex-project", "file.vortex", "metric,value", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-project",
                    ("vortex-filter-project", "file.vortex", "gte:value:3", "metric,value", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-filter-project",
                }
                command = expected.get(tuple(args))
                if command is None:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "local_execution", "value": "true"}],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        count_where = client.vortex_count_where(
            "file.vortex",
            "gte:value:3",
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )
        filtered = client.vortex_filter(
            "file.vortex",
            "gte:value:3",
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )
        projected = client.vortex_project(
            "file.vortex",
            ["metric", "value"],
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )
        filter_project = client.vortex_filter_project(
            "file.vortex",
            "gte:value:3",
            ("metric", "value"),
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )

        self.assertEqual(count_where.command, "vortex-count-where")
        self.assertEqual(filtered.command, "vortex-filter")
        self.assertEqual(projected.command, "vortex-project")
        self.assertEqual(filter_project.command, "vortex-filter-project")

    def test_local_vortex_primitive_smoke_dispatches_certified_fixture_workflow(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                expected = {
                    ("vortex-run", "file.vortex", "count", "3", "4", "--format", "json"): (
                        "vortex-run",
                        [{"key": "local_primitive_rows_scanned", "value": "5"}],
                    ),
                    ("vortex-count-where", "file.vortex", "gte:value:3", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-count-where",
                        [{"key": "filtered_count_local_execution_rows_selected", "value": "3"}],
                    ),
                    ("vortex-filter", "file.vortex", "gte:value:3", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-filter",
                        [{"key": "filter_local_execution_rows_selected", "value": "3"}],
                    ),
                    ("vortex-project", "file.vortex", "metric,value", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-project",
                        [{"key": "project_local_execution_rows_projected", "value": "5"}],
                    ),
                    ("vortex-filter-project", "file.vortex", "gte:value:3", "metric,value", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-filter-project",
                        [{"key": "filter_project_local_execution_rows_projected", "value": "3"}],
                    ),
                }
                matched = expected.get(tuple(args))
                if matched is None:
                    raise AssertionError(args)
                command, command_fields = matched
                fields = [
                    {"key": "fallback_execution_allowed", "value": "false"},
                    {"key": "local_primitive_native_io_certificate_emitted", "value": "true"},
                    {"key": "local_primitive_native_io_certificate_status", "value": "certified"},
                    {"key": "local_primitive_native_io_certified", "value": "true"},
                    {"key": "local_primitive_native_io_data_materialized", "value": "false"},
                    {"key": "local_primitive_native_io_fallback_attempted", "value": "false"},
                    {"key": "local_primitive_execution_certificate_emitted", "value": "true"},
                    {"key": "local_primitive_execution_certificate_status", "value": "certified"},
                    {"key": "local_primitive_execution_certificate_correctness_passed", "value": "true"},
                    {"key": "local_primitive_execution_certificate_fallback_attempted", "value": "false"},
                ]
                fields.extend(command_fields)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).local_vortex_primitive_smoke(
            "file.vortex",
            columns=("metric", "value"),
            memory_gb=3,
            max_parallelism=4,
        )

        self.assertIsInstance(report, LocalVortexPrimitiveSmokeReport)
        self.assertEqual(
            report.commands,
            (
                "vortex-run",
                "vortex-count-where",
                "vortex-filter",
                "vortex-project",
                "vortex-filter-project",
            ),
        )
        self.assertTrue(report.all_certified)
        self.assertEqual(report.uncertified_commands, ())
        self.assertFalse(report.fallback_attempted)
        self.assertEqual(report.count.field_int("local_primitive_rows_scanned"), 5)
        self.assertEqual(
            report.filter_project.field_int("filter_project_local_execution_rows_projected"),
            3,
        )

    def test_vortex_project_helper_dispatches_default_plan_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "vortex-project",
                    "file.vortex",
                    "metric",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-project",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "local_execution", "value": "false"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_project("file.vortex", "metric")

        self.assertEqual(result.command, "vortex-project")
        self.assertFalse(result.field_bool("local_execution"))

    def test_vortex_filter_helpers_dispatch_default_plan_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                expected = {
                    ("vortex-count-where", "file.vortex", "gte:value:3", "--format", "json"): "vortex-count-where",
                    ("vortex-filter", "file.vortex", "gte:value:3", "--format", "json"): "vortex-filter",
                    ("vortex-filter-project", "file.vortex", "gte:value:3", "metric", "--format", "json"): "vortex-filter-project",
                }
                command = expected.get(tuple(args))
                if command is None:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "local_execution", "value": "false"}],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        count_where = client.vortex_count_where("file.vortex", "gte:value:3")
        filtered = client.vortex_filter("file.vortex", "gte:value:3")
        filter_project = client.vortex_filter_project(
            "file.vortex",
            "gte:value:3",
            "metric",
        )

        self.assertEqual(count_where.command, "vortex-count-where")
        self.assertEqual(filtered.command, "vortex-filter")
        self.assertEqual(filter_project.command, "vortex-filter-project")
        self.assertFalse(filter_project.field_bool("local_execution"))

    def test_vortex_local_execution_helpers_validate_resource_arguments(self) -> None:
        client = ShardLoomClient(binary=["shardloom"])

        with self.assertRaises(ValueError):
            client.vortex_count("file.vortex", execute_local_encoded_count=True)
        with self.assertRaises(ValueError):
            client.vortex_filter(
                "file.vortex",
                "gte:value:3",
                memory_gb=1,
            )
        with self.assertRaises(ValueError):
            client.vortex_project("file.vortex", [])
        with self.assertRaises(ValueError):
            client.vortex_filter_project(
                "file.vortex",
                "gte:value:3",
                "metric",
                execute_local_primitive=True,
                memory_gb=0,
                max_parallelism=2,
            )

    def test_unsupported_envelope_raises_with_diagnostics_and_fallback(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
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
        binary = self.fake_cli("print('{\"schema_version\":\"shardloom.output.v2\"}')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_successful_plan_with_error_diagnostic_remains_inspectable(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
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

        result = ShardLoomClient(binary=binary).status()

        self.assertFalse(result.is_error)
        self.assertTrue(result.has_error_diagnostics)

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
                    "schema_version": "shardloom.output.v2",
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

    def test_env_binary_is_resolved_from_client_environment(self) -> None:
        client = ShardLoomClient(env={"SHARDLOOM_BIN": sys.executable, "PATH": ""})

        command = client._command(["status"])

        self.assertEqual(command[0], sys.executable)

    def test_subprocess_env_merges_overrides_with_inherited_environment(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, os
                assert os.environ.get("SHARDLOOM_TEST_INHERITED") == "base"
                assert os.environ.get("SHARDLOOM_TEST_OVERRIDE") == "override"
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
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
        old = os.environ.get("SHARDLOOM_TEST_INHERITED")
        os.environ["SHARDLOOM_TEST_INHERITED"] = "base"
        try:
            result = ShardLoomClient(
                binary=binary,
                env={"SHARDLOOM_TEST_OVERRIDE": "override"},
            ).status()
        finally:
            if old is None:
                os.environ.pop("SHARDLOOM_TEST_INHERITED", None)
            else:
                os.environ["SHARDLOOM_TEST_INHERITED"] = old

        self.assertEqual(result.command, "status")

    def test_relative_env_binary_resolves_from_client_cwd(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        root = Path(tempdir.name)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        binary = bin_dir / "shardloom"
        binary.write_text("", encoding="utf-8")

        client = ShardLoomClient(
            env={"SHARDLOOM_BIN": str(Path("bin") / "shardloom"), "PATH": ""},
            cwd=root,
        )
        command = client._command(["status"])

        self.assertEqual(Path(command[0]), binary)

    def test_missing_binary_raises_deterministic_error(self) -> None:
        client = ShardLoomClient(env={"PATH": ""})

        with self.assertRaises(ShardLoomBinaryNotFoundError) as raised:
            client.status()

        error = raised.exception
        message = str(error)
        self.assertIn("ShardLoom CLI binary could not be resolved", message)
        self.assertIn("SHARDLOOM_BIN", message)
        self.assertFalse(error.fallback.attempted)
        self.assertFalse(error.fallback.allowed)
        self.assertEqual(error.diagnostics[0].code, "SL_BINARY_NOT_FOUND")
        self.assertEqual(error.diagnostics[0].category, "configuration")
        self.assertEqual(error.diagnostics[0].feature, "python_cli_binary_resolution")
        self.assertFalse(error.diagnostics[0].fallback.attempted)

        payload = error.to_error_payload("status")
        self.assertEqual(payload["schema_version"], "shardloom.output.v2")
        self.assertEqual(payload["command"], "status")
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["fallback"]["attempted"], False)
        self.assertEqual(payload["fallback"]["allowed"], False)
        self.assertEqual(payload["diagnostics"][0]["code"], "SL_BINARY_NOT_FOUND")
        self.assertEqual(payload["result"], {"fields": []})
        self.assertEqual(payload["artifacts"], [])
        self.assertEqual(payload["certificates"], [])
        self.assertIn(
            {"key": "command_family", "value": "python_binary_resolution"},
            payload["lifecycle"]["fields"],
        )
        self.assertIn(
            {"key": "fallback_execution_allowed", "value": "false"},
            payload["policy"]["fields"],
        )
        self.assertEqual(OutputEnvelope.from_json(payload).command, "status")
        json.dumps(payload, sort_keys=True)

    def test_invalid_env_binary_raises_deterministic_error(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        missing = Path(tempdir.name) / "missing-shardloom"
        client = ShardLoomClient.from_env(
            {"SHARDLOOM_BIN": str(missing), "PATH": ""}
        )

        with self.assertRaises(ShardLoomBinaryNotFoundError) as raised:
            client.status()

        error = raised.exception
        self.assertIn("SHARDLOOM_BIN points to", str(error))
        self.assertIn("missing-shardloom", error.diagnostics[0].reason or "")
        self.assertFalse(error.to_error_payload("status")["fallback"]["attempted"])

    def test_from_repo_resolves_target_binary_lazily(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        root = Path(tempdir.name)
        target = root / "target" / "debug"
        target.mkdir(parents=True)
        (target / "shardloom").write_text("", encoding="utf-8")
        (target / "shardloom.exe").write_text("", encoding="utf-8")

        client = ShardLoomClient.from_repo(root, profile_order=("debug",))
        command = client._command(["status"])

        self.assertTrue(command[0].endswith(("shardloom", "shardloom.exe")))
        self.assertIn(str(target), command[0])

    def test_traditional_analytics_vortex_run_passes_explicit_inputs(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "fact.vortex",
                    "dim.vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "rows_scanned", "value": "42"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_run(
            "selective filter", "fact.vortex", "dim.vortex"
        )

        self.assertEqual(result.field_int("rows_scanned"), 42)

    def test_live_etl_smoke_dispatches_csv_and_vortex_modes(self) -> None:
        csv_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "csv/file ingest",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "input_format", "value": "csv"}],
                }))
                """
            )
        )
        csv_result = ShardLoomClient(binary=csv_binary).live_etl_smoke(
            "csv/file ingest",
            "fact.csv",
            "dim.csv",
            input_format="csv",
            workspace="work",
        )
        self.assertEqual(csv_result.command, "traditional-analytics-run")

        vortex_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "wide projection",
                    "fact.vortex",
                    "dim.vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "input_format", "value": "vortex"}],
                }))
                """
            )
        )
        vortex_result = ShardLoomClient(binary=vortex_binary).live_etl_smoke(
            "wide projection",
            "fact.vortex",
            "dim.vortex",
            input_format="vortex",
        )
        self.assertEqual(vortex_result.command, "traditional-analytics-vortex-run")

    def test_live_etl_smoke_accepts_common_compatibility_formats(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "hash join",
                    "fact.parquet",
                    "dim.parquet",
                    "--workspace",
                    "work",
                    "--input-format",
                    "parquet",
                    "--compat-output-format",
                    "orc",
                    "--memory-gb",
                    "8",
                    "--max-parallelism",
                    "4",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "source_format", "value": "parquet"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_smoke(
            "hash join",
            "fact.parquet",
            "dim.parquet",
            input_format="parquet",
            workspace="work",
            compatibility_output_format="orc",
            memory_gb=8,
            max_parallelism=4,
        )

        self.assertEqual(result.field("source_format"), "parquet")

    def test_live_etl_smoke_rejects_unknown_format(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient(binary=["shardloom"]).live_etl_smoke(
                "csv/file ingest", "fact.unknown", "dim.unknown", input_format="unknown"
            )

    def test_live_etl_csv_to_vortex_replay_runs_import_then_native_replay(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "traditional-analytics-run",
                        "status": "success",
                        "summary": "csv ok",
                        "human_text": "csv ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "fact_vortex_path", "value": "work/fact.vortex"},
                            {"key": "dim_vortex_path", "value": "work/dim.vortex"},
                        ],
                    }))
                elif args == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "work/fact.vortex",
                    "work/dim.vortex",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "traditional-analytics-vortex-run",
                        "status": "success",
                        "summary": "native ok",
                        "human_text": "native ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [{"key": "source_format", "value": "vortex"}],
                    }))
                else:
                    raise AssertionError(args)
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
        )

        self.assertEqual(result.csv_import.command, "traditional-analytics-run")
        self.assertEqual(
            result.native_vortex.command if result.native_vortex else None,
            "traditional-analytics-vortex-run",
        )
        self.assertEqual(result.fact_vortex_path, "work/fact.vortex")
        self.assertEqual(result.dim_vortex_path, "work/dim.vortex")
        self.assertTrue(result.native_replay_ran)
        self.assertFalse(result.fallback_attempted)

    def test_live_etl_csv_to_vortex_replay_can_skip_native_replay(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "csv ok",
                    "human_text": "csv ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "fact_vortex_path", "value": "work/fact.vortex"},
                        {"key": "dim_vortex_path", "value": "work/dim.vortex"},
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
            replay_native=False,
        )

        self.assertIsNone(result.native_vortex)
        self.assertFalse(result.native_replay_ran)

    def test_live_etl_csv_to_vortex_replay_requires_emitted_vortex_paths(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "csv ok",
                    "human_text": "csv ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
                "selective filter",
                "fact.csv",
                "dim.csv",
                workspace="work",
            )

    def test_dynamic_work_shaping_and_sizing_feedback_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "dynamic-work-shaping-plan",
                    "memory-pressure",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "dynamic-work-shaping-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "profile", "value": "memory-pressure"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).dynamic_work_shaping_plan(
            "memory-pressure"
        )

        self.assertEqual(result.field("profile"), "memory-pressure")

        feedback_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "sizing-feedback-plan",
                    "8",
                    "task-too-large,memory-pressure-high",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sizing-feedback-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "target_task_bytes_changed", "value": "true"}],
                }))
                """
            )
        )
        feedback = ShardLoomClient(binary=feedback_binary).sizing_feedback_plan(
            8, ["task-too-large", "memory-pressure-high"]
        )
        self.assertTrue(feedback.field_bool("target_task_bytes_changed"))

    def test_benchmark_and_world_class_plan_helpers(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "benchmark-claim-evidence-plan",
                    "traditional-analytics",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "benchmark-claim-evidence-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "scope", "value": "traditional-analytics"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).benchmark_claim_evidence_plan(
            "traditional-analytics"
        )

        self.assertEqual(result.field("scope"), "traditional-analytics")

    def test_input_adapter_and_input_plan_helpers(self) -> None:
        adapters_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["input-adapters", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-adapters",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "critical_structured_adapter_order", "value": "native_vortex,parquet,arrow_ipc,csv,jsonl"},
                        {"key": "parquet_status", "value": "planned"}
                    ],
                }))
                """
            )
        )

        adapters = ShardLoomClient(binary=adapters_binary).input_adapters()

        self.assertEqual(adapters.command, "input-adapters")
        self.assertEqual(adapters.field("parquet_status"), "planned")

        plan_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["input-plan", "file://tmp/data.parquet", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "plan_only", "value": "true"}],
                }))
                """
            )
        )

        input_plan = ShardLoomClient(binary=plan_binary).input_plan(
            "file://tmp/data.parquet"
        )

        self.assertEqual(input_plan.command, "input-plan")
        self.assertTrue(input_plan.field_bool("plan_only"))

    def test_compatibility_source_smoke_dispatches_report_only_input_plans(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                fields = []
                command = args[0] if args else ""
                if args == ["input-adapters", "--format", "json"]:
                    fields = [
                        {"key": "critical_structured_adapter_order", "value": "native_vortex,parquet,arrow_ipc,csv,jsonl"},
                        {"key": "csv_status", "value": "planned"},
                        {"key": "jsonl_status", "value": "planned"},
                        {"key": "parquet_status", "value": "planned"},
                        {"key": "plan_only", "value": "true"},
                        {"key": "write_io", "value": "false"},
                    ]
                elif args == ["native-io-envelope-plan", "--format", "json"]:
                    fields = [
                        {"key": "per_path_certificate_required", "value": "true"},
                        {"key": "adapter_fidelity_report_required", "value": "true"},
                        {"key": "materialization_boundary_required_for_rows", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                    ]
                elif args[:1] == ["input-plan"] and args[-2:] == ["--format", "json"]:
                    uri = args[1]
                    source_kind = uri.rsplit(".", 1)[-1].replace("ndjson", "jsonl")
                    fields = [
                        {"key": "source_kind", "value": source_kind},
                        {"key": "adapter_kind", "value": "compatibility_file_adapter"},
                        {"key": "dataset_format", "value": source_kind},
                        {"key": "capability_status", "value": "planned"},
                        {"key": "metadata_availability", "value": "deferred"},
                        {"key": "fidelity", "value": "compatibility_logical"},
                        {"key": "materialization_risk", "value": "medium"},
                        {"key": "native_vortex", "value": "false"},
                        {"key": "compatibility_structured", "value": "true"},
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "data_materialized", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_execution_allowed", "value": "false"},
                    ]
                else:
                    raise AssertionError(args)

                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).compatibility_source_smoke(
            {
                "csv": "fact.csv",
                "jsonl": "events.jsonl",
                "parquet": "fact.parquet",
            }
        )

        self.assertIsInstance(report, CompatibilitySourceSmokeReport)
        self.assertEqual(
            report.commands,
            (
                "input-adapters",
                "native-io-envelope-plan",
                "input-plan",
                "input-plan",
                "input-plan",
            ),
        )
        self.assertEqual(report.compatibility_source_names, ("csv", "jsonl", "parquet"))
        self.assertEqual(report.planned_source_names, ("csv", "jsonl", "parquet"))
        self.assertTrue(report.all_plan_only)
        self.assertFalse(report.fallback_attempted)
        self.assertEqual(report.sources[1].plan.field("source_kind"), "jsonl")


if __name__ == "__main__":
    unittest.main()
