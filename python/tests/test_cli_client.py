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
    RestApiContractPlan,
    RestApiDataPlane,
    RestApiDiscoveryContract,
    RestApiEventStream,
    RestApiLocalLifecycle,
    RestApiPlanPreview,
    RestApiSecurityGovernance,
    WorkflowReadinessSmokeReport,
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

    def test_live_fixture_client_methods_dispatch_expected_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["live-change-contract-plan", "--format", "json"]:
                    command = "live-change-contract-plan"
                    fields = [
                        {"key": "change_record_field_order", "value": "key,operation,sequence,event_time_ms,processing_time_ms,source_offset,schema_digest,payload_ref"},
                        {"key": "change_operation_vocabulary", "value": "append,upsert,delete,retract,tombstone"},
                        {"key": "fixture_operator_vocabulary", "value": "filter,project,count,count_where,group_count"},
                        {"key": "runtime_execution", "value": "false"},
                    ]
                elif args == ["live-fixture-run", "project", "key,metric", "--format", "json"]:
                    command = "live-fixture-run"
                    fields = [
                        {"key": "fixture_operator", "value": "project"},
                        {"key": "input_change_record_count", "value": "10"},
                        {"key": "active_state_key_count", "value": "3"},
                        {"key": "output_row_count", "value": "3"},
                        {"key": "output_rows", "value": "key=a,metric=east|key=b,metric=west|key=e,metric=east"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "state_certificate_status", "value": "certified"},
                        {"key": "continuous_view_certificate_status", "value": "certified"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["hybrid-overlay-run", "group-count", "metric", "--format", "json"]:
                    command = "hybrid-overlay-run"
                    fields = [
                        {"key": "fixture_operator", "value": "group_count"},
                        {"key": "base_row_count", "value": "4"},
                        {"key": "hot_change_record_count", "value": "6"},
                        {"key": "merged_row_count", "value": "3"},
                        {"key": "output_rows", "value": "east:group_count:2|west:group_count:1"},
                        {"key": "delta_overlay_certificate_status", "value": "certified"},
                        {"key": "micro_segment_flush_evidence_status", "value": "certified"},
                        {"key": "layout_health_bundle_status", "value": "compaction_recommended"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
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
        client = ShardLoomClient(binary=binary)

        contract = client.live_change_contract_plan()
        fixture = client.live_fixture_run("project", ("key", "metric"))
        hybrid = client.hybrid_overlay_run("group-count", "metric")

        self.assertEqual(contract.operations, ("append", "upsert", "delete", "retract", "tombstone"))
        self.assertFalse(contract.runtime_execution)
        self.assertEqual(fixture.operator, "project")
        self.assertEqual(fixture.output_row_count, 3)
        self.assertTrue(fixture.all_certified)
        self.assertFalse(fixture.fallback_attempted)
        self.assertFalse(fixture.external_engine_invoked)
        self.assertEqual(hybrid.operator, "group_count")
        self.assertEqual(hybrid.base_row_count, 4)
        self.assertEqual(hybrid.hot_change_record_count, 6)
        self.assertEqual(hybrid.merged_row_count, 3)
        self.assertEqual(hybrid.output_rows, ("east:group_count:2", "west:group_count:1"))
        self.assertEqual(hybrid.layout_health_status, "compaction_recommended")
        self.assertTrue(hybrid.all_certified)
        self.assertTrue(hybrid.runtime_execution)
        self.assertFalse(hybrid.data_read)
        self.assertFalse(hybrid.write_io)
        self.assertFalse(hybrid.fallback_attempted)
        self.assertFalse(hybrid.external_engine_invoked)

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
        self.assertEqual(capabilities.engines.field("scope"), "engines")
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
        with self.assertRaises(ValueError):
            client.vortex_write_intent_plan("file.vortex", [])

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

    def test_rest_api_contract_plan_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["rest-api-contract-plan", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-contract-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "api_version", "value": "v1"},
                        {"key": "openapi_version", "value": "3.2.0"},
                        {"key": "openapi_contract_path", "value": "docs/api/shardloom-openapi-v1.yaml"},
                        {"key": "represented_resources", "value": "health,version,capabilities,governance"},
                        {"key": "discovery_endpoint_paths", "value": "/v1/health,/v1/capabilities"},
                        {"key": "openapi_contract_artifact_checked_in", "value": "true"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_contract_plan()

        self.assertIsInstance(result, RestApiContractPlan)
        self.assertEqual(result.api_version, "v1")
        self.assertEqual(result.openapi_version, "3.2.0")
        self.assertEqual(result.openapi_contract_path, "docs/api/shardloom-openapi-v1.yaml")
        self.assertEqual(result.represented_resources[-1], "governance")
        self.assertEqual(result.discovery_endpoint_paths, ("/v1/health", "/v1/capabilities"))
        self.assertTrue(result.contract_artifact_checked_in)
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.fallback_attempted)

    def test_serve_discovery_contract_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "serve", "--mode", "discovery", "--bind", "127.0.0.1:8787", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "serve",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "api_version", "value": "v1"},
                        {"key": "openapi_version", "value": "3.2.0"},
                        {"key": "openapi_contract_path", "value": "docs/api/shardloom-openapi-v1.yaml"},
                        {"key": "represented_resources", "value": "health,version,capabilities"},
                        {"key": "discovery_endpoint_paths", "value": "/v1/health,/v1/capabilities"},
                        {"key": "server_mode", "value": "discovery"},
                        {"key": "bind", "value": "127.0.0.1:8787"},
                        {"key": "serve_command_contract_only", "value": "true"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).serve_discovery_contract()

        self.assertIsInstance(result, RestApiDiscoveryContract)
        self.assertEqual(result.server_mode, "discovery")
        self.assertEqual(result.bind, "127.0.0.1:8787")
        self.assertTrue(result.contract_only)
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.fallback_attempted)

    def test_rest_api_plan_preview_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-plan-preview", "unsupported-operator", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-plan-preview",
                    "status": "unsupported",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "SL_UNSUPPORTED_SQL",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "rest_api_plan_operator",
                        "reason": "unsupported operator rejected without fallback execution",
                        "suggested_next_step": "rewrite",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [
                        {"key": "scenario", "value": "unsupported-operator"},
                        {"key": "preview_status", "value": "unsupported"},
                        {"key": "plan_handle", "value": "plan://cg23/unsupported-operator"},
                        {"key": "preview_operations", "value": "plan_handle,validate,explain,estimate,unsupported_report,certification_preview"},
                        {"key": "stage_order", "value": "parser,binder,native_logical,native_physical,execution_readiness,evidence_readiness,certification"},
                        {"key": "parser_stage_status", "value": "ready"},
                        {"key": "binder_stage_status", "value": "ready"},
                        {"key": "native_logical_stage_status", "value": "unsupported"},
                        {"key": "native_physical_stage_status", "value": "not_evaluated"},
                        {"key": "execution_readiness_stage_status", "value": "not_evaluated"},
                        {"key": "evidence_readiness_stage_status", "value": "not_evaluated"},
                        {"key": "certification_stage_status", "value": "not_evaluated"},
                        {"key": "problem_details_emitted", "value": "true"},
                        {"key": "problem_details_type", "value": "https://shardloom.dev/problems/unsupported-plan-operator"},
                        {"key": "problem_details_status", "value": "422"},
                        {"key": "problem_details_diagnostic_code", "value": "SL_UNSUPPORTED_SQL"},
                        {"key": "unsupported_reason", "value": "unsupported operator rejected without fallback execution"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_plan_preview(
            "unsupported-operator",
            check=False,
        )

        self.assertIsInstance(result, RestApiPlanPreview)
        self.assertEqual(result.scenario, "unsupported-operator")
        self.assertEqual(result.preview_status, "unsupported")
        self.assertEqual(result.plan_handle, "plan://cg23/unsupported-operator")
        self.assertIn("certification_preview", result.operations)
        self.assertEqual(result.stage_order[2], "native_logical")
        self.assertEqual(result.stage_statuses["native_logical"], "unsupported")
        self.assertTrue(result.problem_details_emitted)
        self.assertEqual(
            result.problem_details_type,
            "https://shardloom.dev/problems/unsupported-plan-operator",
        )
        self.assertEqual(result.problem_details_status, 422)
        self.assertEqual(result.problem_details_diagnostic_code, "SL_UNSUPPORTED_SQL")
        self.assertIn("without fallback", result.unsupported_reason or "")
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.runtime_execution)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_local_lifecycle_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-local-lifecycle", "certified-local-batch", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-local-lifecycle",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "certified-local-batch"},
                        {"key": "lifecycle_status", "value": "succeeded"},
                        {"key": "query_id", "value": "query://cg23/certified-local-batch/0001"},
                        {"key": "result_ref", "value": "result://cg23/certified-local-batch/0001"},
                        {"key": "lifecycle_operations", "value": "execute,status,cancel,retry,profile,certificates,lineage,results,artifacts,cleanup"},
                        {"key": "result_policies", "value": "inline_json:decoded_rows,vortex_artifact:native_vortex_artifact,arrow_ipc_decoded_boundary:decoded_columnar_boundary"},
                        {"key": "inline_json_available", "value": "true"},
                        {"key": "vortex_artifact_available", "value": "true"},
                        {"key": "arrow_ipc_materialization", "value": "decoded_columnar_boundary"},
                        {"key": "arrow_ipc_certified_native", "value": "false"},
                        {"key": "result_ttl_seconds", "value": "3600"},
                        {"key": "cleanup_required", "value": "true"},
                        {"key": "non_certified_path_blocked", "value": "false"},
                        {"key": "cancellation_status", "value": "not_requested"},
                        {"key": "retry_status", "value": "not_requested"},
                        {"key": "query_execution", "value": "true"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "local_execution_performed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_local_lifecycle()

        self.assertIsInstance(result, RestApiLocalLifecycle)
        self.assertEqual(result.scenario, "certified-local-batch")
        self.assertEqual(result.lifecycle_status, "succeeded")
        self.assertEqual(result.query_id, "query://cg23/certified-local-batch/0001")
        self.assertEqual(result.result_ref, "result://cg23/certified-local-batch/0001")
        self.assertIn("cleanup", result.lifecycle_operations)
        self.assertIn("vortex_artifact:native_vortex_artifact", result.result_policies)
        self.assertTrue(result.inline_json_available)
        self.assertTrue(result.vortex_artifact_available)
        self.assertEqual(result.arrow_ipc_materialization, "decoded_columnar_boundary")
        self.assertFalse(result.arrow_ipc_certified_native)
        self.assertEqual(result.result_ttl_seconds, 3600)
        self.assertTrue(result.cleanup_required)
        self.assertFalse(result.non_certified_path_blocked)
        self.assertEqual(result.cancellation_status, "not_requested")
        self.assertEqual(result.retry_status, "not_requested")
        self.assertTrue(result.query_execution)
        self.assertTrue(result.runtime_execution)
        self.assertTrue(result.local_execution_performed)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_event_stream_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-event-stream", "certified-live-fixture", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-event-stream",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "certified-live-fixture"},
                        {"key": "event_stream_status", "value": "certified_fixture"},
                        {"key": "stream_id", "value": "event-stream://cg23/live-fixture/group-count"},
                        {"key": "stream_ref", "value": "event-stream://cg23/live-fixture/group-count"},
                        {"key": "engine_mode", "value": "live"},
                        {"key": "delivery_protocols", "value": "server_sent_events,websocket_optional"},
                        {"key": "event_types", "value": "progress,state,checkpoint,watermark,certificate,lineage,benchmark,hybrid_hot_cold_contribution"},
                        {"key": "certificate_ref_summary", "value": "certificates/cg22/live/fixture/freshness.json,certificates/cg22/live/fixture/group-count/execution.json"},
                        {"key": "asyncapi_contract_path", "value": "docs/api/shardloom-asyncapi-events-v1.yaml"},
                        {"key": "sse_first", "value": "true"},
                        {"key": "websocket_required", "value": "false"},
                        {"key": "event_count", "value": "7"},
                        {"key": "workload_certified", "value": "true"},
                        {"key": "production_claim_allowed", "value": "false"},
                        {"key": "broker_required", "value": "false"},
                        {"key": "broker_io", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_event_stream()

        self.assertIsInstance(result, RestApiEventStream)
        self.assertEqual(result.scenario, "certified-live-fixture")
        self.assertEqual(result.event_stream_status, "certified_fixture")
        self.assertEqual(result.stream_id, "event-stream://cg23/live-fixture/group-count")
        self.assertEqual(result.stream_ref, "event-stream://cg23/live-fixture/group-count")
        self.assertEqual(result.engine_mode, "live")
        self.assertIn("server_sent_events", result.delivery_protocols)
        self.assertIn("watermark", result.event_types)
        self.assertIn("certificates/cg22/live/fixture/freshness.json", result.certificate_refs)
        self.assertEqual(
            result.asyncapi_contract_path,
            "docs/api/shardloom-asyncapi-events-v1.yaml",
        )
        self.assertTrue(result.sse_first)
        self.assertFalse(result.websocket_required)
        self.assertEqual(result.event_count, 7)
        self.assertTrue(result.workload_certified)
        self.assertFalse(result.production_claim_allowed)
        self.assertFalse(result.broker_required)
        self.assertFalse(result.broker_io)
        self.assertFalse(result.object_store_io)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_security_governance_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-security-governance", "safe-local-default", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-security-governance",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "safe-local-default"},
                        {"key": "governance_status", "value": "available_contract"},
                        {"key": "auth_postures", "value": "local_only:available_default,token:reference_only_contract"},
                        {"key": "api_scopes", "value": "read:allowed_local_metadata,write:policy_required,agent:dry_run_explain_estimate_certify_only"},
                        {"key": "mcp_tools", "value": "dry_run:allowed,explain:allowed,estimate:allowed,certify_preview:allowed,execute:blocked_policy_required"},
                        {"key": "evidence_model_signals", "value": "opentelemetry_traces,openlineage_facets,problem_details_errors,cloudevents,certificate_refs"},
                        {"key": "credential_references_only", "value": "true"},
                        {"key": "secrets_redacted", "value": "true"},
                        {"key": "raw_secret_values_present", "value": "false"},
                        {"key": "destructive_policy_required", "value": "true"},
                        {"key": "destructive_policy_present", "value": "false"},
                        {"key": "destructive_operations_allowed", "value": "false"},
                        {"key": "mcp_dry_run_default", "value": "true"},
                        {"key": "mcp_effectful_tools_allowed", "value": "false"},
                        {"key": "mcp_discovery_side_effect_free", "value": "true"},
                        {"key": "opentelemetry_exporter_enabled", "value": "false"},
                        {"key": "openlineage_facets_mapped", "value": "true"},
                        {"key": "problem_details_mapped", "value": "true"},
                        {"key": "cloudevents_mapped", "value": "true"},
                        {"key": "certificate_refs_mapped", "value": "true"},
                        {"key": "credential_resolution", "value": "false"},
                        {"key": "secret_resolution", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_security_governance()

        self.assertIsInstance(result, RestApiSecurityGovernance)
        self.assertEqual(result.scenario, "safe-local-default")
        self.assertEqual(result.governance_status, "available_contract")
        self.assertIn("token:reference_only_contract", result.auth_postures)
        self.assertIn("write:policy_required", result.api_scopes)
        self.assertIn("certify_preview:allowed", result.mcp_tools)
        self.assertIn("opentelemetry_traces", result.evidence_model_signals)
        self.assertTrue(result.credential_references_only)
        self.assertTrue(result.secrets_redacted)
        self.assertFalse(result.raw_secret_values_present)
        self.assertTrue(result.destructive_policy_required)
        self.assertFalse(result.destructive_policy_present)
        self.assertFalse(result.destructive_operations_allowed)
        self.assertTrue(result.mcp_dry_run_default)
        self.assertFalse(result.mcp_effectful_tools_allowed)
        self.assertTrue(result.mcp_discovery_side_effect_free)
        self.assertFalse(result.opentelemetry_exporter_enabled)
        self.assertTrue(result.openlineage_facets_mapped)
        self.assertTrue(result.problem_details_mapped)
        self.assertTrue(result.cloudevents_mapped)
        self.assertTrue(result.certificate_refs_mapped)
        self.assertFalse(result.credential_resolution)
        self.assertFalse(result.secret_resolution)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_data_plane_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-data-plane", "standards-matrix", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-data-plane",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "standards-matrix"},
                        {"key": "data_plane_status", "value": "standards_matrix_available"},
                        {"key": "transfer_modes", "value": "vortex_artifact:native_vortex_artifact,arrow_ipc_decoded_boundary:decoded_columnar_boundary,flight_ticket_future:decoded_columnar_boundary"},
                        {"key": "standards_names", "value": "iceberg_rest_catalog,polaris,gravitino,delta_sharing,substrait,wasi_webassembly_components,nats_jetstream,redpanda,kafka_compatible,paimon,fluss"},
                        {"key": "preferred_large_payload_modes", "value": "vortex_artifact,object_reference,paged_json"},
                        {"key": "large_payload_threshold_bytes", "value": "1048576"},
                        {"key": "rest_control_plane_sufficient_for_local_use", "value": "true"},
                        {"key": "flight_adbc_required_for_basic_local_use", "value": "false"},
                        {"key": "flight_ticket_requested", "value": "false"},
                        {"key": "flight_ticket_supported", "value": "false"},
                        {"key": "adbc_endpoint_requested", "value": "false"},
                        {"key": "adbc_endpoint_supported", "value": "false"},
                        {"key": "decoded_columnar_boundary_declared", "value": "true"},
                        {"key": "materialization_declared", "value": "true"},
                        {"key": "result_policy_declared", "value": "true"},
                        {"key": "standards_matrix_count", "value": "11"},
                        {"key": "flight_server_started", "value": "false"},
                        {"key": "adbc_endpoint_opened", "value": "false"},
                        {"key": "broker_io", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "catalog_probe", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_data_plane(
            "standards-matrix"
        )

        self.assertIsInstance(result, RestApiDataPlane)
        self.assertEqual(result.scenario, "standards-matrix")
        self.assertEqual(result.data_plane_status, "standards_matrix_available")
        self.assertIn("vortex_artifact:native_vortex_artifact", result.transfer_modes)
        self.assertIn("iceberg_rest_catalog", result.standards_names)
        self.assertIn("wasi_webassembly_components", result.standards_names)
        self.assertIn("vortex_artifact", result.preferred_large_payload_modes)
        self.assertEqual(result.large_payload_threshold_bytes, 1048576)
        self.assertTrue(result.rest_control_plane_sufficient_for_local_use)
        self.assertFalse(result.flight_adbc_required_for_basic_local_use)
        self.assertFalse(result.flight_ticket_requested)
        self.assertFalse(result.flight_ticket_supported)
        self.assertFalse(result.adbc_endpoint_requested)
        self.assertFalse(result.adbc_endpoint_supported)
        self.assertTrue(result.decoded_columnar_boundary_declared)
        self.assertTrue(result.materialization_declared)
        self.assertTrue(result.result_policy_declared)
        self.assertEqual(result.standards_matrix_count, 11)
        self.assertFalse(result.flight_server_started)
        self.assertFalse(result.adbc_endpoint_opened)
        self.assertFalse(result.broker_io)
        self.assertFalse(result.object_store_io)
        self.assertFalse(result.catalog_probe)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

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

    def test_workflow_readiness_smoke_dispatches_no_write_planning_bundle(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                target = "file://tmp/out.vortex"
                compat = "file://tmp/out.parquet"
                workspace = "target/stage"
                args = sys.argv[1:]

                def emit(command, fields, diagnostics=None):
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": command,
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": diagnostics or [],
                        "fields": [
                            {"key": "fallback_execution_allowed", "value": "false"},
                            {"key": "execution", "value": "not_performed"},
                            *fields,
                        ],
                    }))

                if args == ["vortex-output-plan", target, "--format", "json"]:
                    emit("vortex-output-plan", [{"key": "target_format", "value": "vortex"}])
                elif args == ["translation-plan", compat, "--format", "json"]:
                    emit("translation-plan", [])
                elif args == ["plan-export", "native", "--format", "json"]:
                    emit("plan-export", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "write_io", "value": "false"},
                        {"key": "read_io", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "external_engine_execution", "value": "false"},
                    ])
                elif args[:2] == ["vortex-write-intent-plan", target] and args[-2:] == ["--format", "json"]:
                    assert args[2] == "native-vortex-target,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,commit-protocol-available,staged-output-required", args
                    emit("vortex-write-intent-plan", [
                        {"key": "target_uri", "value": target},
                        {"key": "target_is_native_vortex", "value": "true"},
                        {"key": "write_execution_allowed", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "manifest_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "upstream_vortex_write_called", "value": "false"},
                    ])
                elif args[:3] == ["vortex-output-payload-plan", target, workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-output-payload-plan", [
                        {"key": "payload_write_allowed", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "upstream_vortex_write_called", "value": "false"},
                    ])
                elif args[:2] == ["vortex-staged-manifest-file-plan", workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-staged-manifest-file-plan", [
                        {"key": "manifest_file_written", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "commit_performed", "value": "false"},
                    ])
                elif args[:2] == ["vortex-commit-marker-plan", workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-commit-marker-plan", [
                        {"key": "commit_marker_written", "value": "false"},
                        {"key": "marker_write_allowed", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                    ])
                elif args[:2] == ["vortex-commit-intent-plan", target] and args[-2:] == ["--format", "json"]:
                    emit("vortex-commit-intent-plan", [
                        {"key": "commit_execution_allowed", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "recovery_action_executed", "value": "false"},
                    ])
                elif args[:4] == ["vortex-commit-protocol-plan", target, "not-started", "validate-intent"] and args[-2:] == ["--format", "json"]:
                    emit("vortex-commit-protocol-plan", [
                        {"key": "commit_execution_allowed", "value": "false"},
                        {"key": "commit_marker_written", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                    ])
                elif args[:3] == ["vortex-local-commit-recovery-plan", target, workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-local-commit-recovery-plan", [
                        {"key": "rollback_executed", "value": "false"},
                        {"key": "cleanup_performed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                    ])
                elif args == ["table-intelligence-plan", "--format", "json"]:
                    emit("table-intelligence-plan", [{"key": "plan_only", "value": "true"}])
                elif args in (["table-compat-plan", "iceberg", "--format", "json"], ["table-compat-plan", "delta", "--format", "json"]):
                    emit("table-compat-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["layout-health-plan", "healthy", "--format", "json"]:
                    emit("layout-health-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["compaction-plan", "small-files", "--format", "json"]:
                    emit("compaction-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["cg9-catalog-metadata-gate", "--format", "json"]:
                    emit("cg9-catalog-metadata-gate", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "claim_blocked", "value": "true"},
                        {"key": "catalog_io_allowed", "value": "false"},
                        {"key": "object_store_io_allowed", "value": "false"},
                        {"key": "write_io_allowed", "value": "false"},
                    ])
                elif args[0] in {
                    "object-store-request-plan",
                    "object-store-range-plan",
                    "object-store-coalesce-plan",
                    "object-store-schedule-plan",
                    "object-store-checkpoint-retry-plan",
                    "object-store-commit-plan",
                } and args[-2:] == ["--format", "json"]:
                    emit(args[0], [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args[0] == "input-plan" and args[-2:] == ["--format", "json"]:
                    emit("input-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "source_kind", "value": "parquet"},
                        {"key": "capability_status", "value": "planned"},
                        {"key": "data_read", "value": "false"},
                        {"key": "data_materialized", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["capabilities", "migration", "--format", "json"]:
                    emit("capabilities", [
                        {"key": "scope", "value": "migration"},
                        {"key": "side_effect_free", "value": "true"},
                        {"key": "migration_report_count", "value": "5"},
                    ])
                elif args == ["correctness-plan", "--format", "json"]:
                    emit("correctness-plan", [
                        {"key": "status", "value": "planned"},
                        {"key": "fixture_count", "value": "36"},
                    ])
                elif args == ["benchmark-claim-evidence-plan", "foundation", "--format", "json"]:
                    emit("benchmark-claim-evidence-plan", [
                        {"key": "claim_evidence_status", "value": "needs_evidence"},
                        {"key": "performance_claim_allowed", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                    ], diagnostics=[{
                        "code": "SL_NOT_IMPLEMENTED",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "needs evidence",
                        "feature": "benchmark",
                        "reason": "missing measurements",
                        "suggested_next_step": "collect evidence",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    }])
                elif args == ["world-class-sufficiency-plan", "--format", "json"]:
                    emit("world-class-sufficiency-plan", [
                        {"key": "fallback_attempted", "value": "false"},
                    ])
                else:
                    raise AssertionError(args)
                """
            )
        )

        report = ShardLoomClient(binary=binary).workflow_readiness_smoke(
            target_uri="file://tmp/out.vortex",
            compatibility_target_uri="file://tmp/out.parquet",
            workspace_path="target/stage",
        )

        self.assertIsInstance(report, WorkflowReadinessSmokeReport)
        self.assertEqual(len(report.plans), 30)
        self.assertEqual(report.output_commit[0].name, "vortex_output_target")
        self.assertIn("remote_input_s3_parquet", report.plan_names)
        self.assertIn("benchmark_claim_evidence", report.blocked_plan_names)
        self.assertIn("catalog_metadata_gate", report.blocked_plan_names)
        self.assertFalse(report.fallback_attempted)
        self.assertTrue(report.all_no_write)
        self.assertTrue(report.all_report_only_or_planned)
        self.assertEqual(
            report.output_commit[3].envelope.field("target_uri"),
            "file://tmp/out.vortex",
        )

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
