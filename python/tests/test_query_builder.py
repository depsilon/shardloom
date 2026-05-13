from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

import shardloom as sl
from shardloom import LazyFrame, ShardLoomClient, ShardLoomContext

_FAKE_CLI_ENVELOPE_PRELUDE = textwrap.dedent(
    """
    import json as _shardloom_json

    _shardloom_original_json_dumps = _shardloom_json.dumps

    def _shardloom_fill_typed_envelope(value):
        if isinstance(value, dict) and value.get("schema_version") == "shardloom.output.v2":
            value = dict(value)
            value.setdefault("result", {"fields": value.get("fields", [])})
            value.setdefault("result_refs", [])
            value.setdefault("artifacts", [])
            value.setdefault("artifact_refs", [])
            value.setdefault("certificates", [])
            value.setdefault("policy", {"fields": []})
            value.setdefault("lifecycle", {"fields": []})
            value.setdefault("capability_snapshot", {"fields": []})
        return value

    def _shardloom_json_dumps(value, *args, **kwargs):
        return _shardloom_original_json_dumps(
            _shardloom_fill_typed_envelope(value),
            *args,
            **kwargs,
        )

    _shardloom_json.dumps = _shardloom_json_dumps
    """
)


class LazyWorkflowBuilderTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(_FAKE_CLI_ENVELOPE_PRELUDE + "\n" + body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_top_level_readers_are_lazy_and_build_operation_summary(self) -> None:
        frame = (
            sl.read_csv(
                "events.csv",
                schema={"id": "int64", "amount": "float64"},
                binary=["definitely-missing-shardloom"],
            )
            .filter("id > 0")
            .select(["id", "amount"])
            .limit(10)
        )
        json_frame = sl.read_json(
            "events.ndjson",
            schema={"payload": "string"},
            binary=["definitely-missing-shardloom"],
        )

        self.assertIsInstance(frame, LazyFrame)
        self.assertEqual(frame.source_format, "csv")
        self.assertEqual(frame.source.schema_map["id"], "int64")
        self.assertEqual(json_frame.source_format, "json")
        self.assertEqual(
            frame.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> limit(10)",
        )

    def test_lazy_builder_validates_empty_operations(self) -> None:
        frame = sl.read_parquet("orders.parquet", binary=["definitely-missing-shardloom"])

        with self.assertRaises(ValueError):
            frame.filter("")
        with self.assertRaises(ValueError):
            frame.select([])
        with self.assertRaises(TypeError):
            frame.limit(True)
        with self.assertRaises(ValueError):
            frame.limit(-1)
        with self.assertRaises(ValueError):
            sl.read_vortex(
                "orders.vortex",
                client=ShardLoomClient(binary=["shardloom"]),
                binary=["shardloom"],
            )

    def test_context_readers_reuse_context_client_for_plan_inspection(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "input-plan",
                    "customers.parquet",
                    "--source-format",
                    "parquet",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-plan",
                    "status": "success",
                    "summary": "input plan report",
                    "human_text": "input plan",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "fallback_execution_allowed", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        plan = ctx.read_parquet("customers.parquet").select("customer_id").plan()

        self.assertEqual(plan.command, "input-plan")
        self.assertTrue(plan.field_bool("plan_only"))
        self.assertFalse(plan.field_bool("data_read"))
        self.assertFalse(plan.fallback.attempted)

    def test_non_vortex_plan_uses_declared_source_format_not_uri_suffix(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "input-plan",
                    "events.data",
                    "--source-format",
                    "csv",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-plan",
                    "status": "success",
                    "summary": "input plan report",
                    "human_text": "input plan",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "dataset_format", "value": "csv"},
                        {"key": "plan_only", "value": "true"}
                    ],
                }))
                """
            )
        )

        plan = sl.read_csv("events.data", binary=binary).plan()

        self.assertEqual(plan.command, "input-plan")
        self.assertEqual(plan.field("dataset_format"), "csv")

    def test_context_engine_intent_is_lazy_and_flows_to_lazy_frame(self) -> None:
        ctx = ShardLoomContext(
            ShardLoomClient(binary=["definitely-missing-shardloom"]),
            engine="hybrid",
        )

        frame = ctx.read_vortex("orders.vortex").filter("gte:value:3")

        self.assertEqual(ctx.engine, "hybrid")
        self.assertEqual(frame.engine_mode, "hybrid")
        self.assertEqual(frame.with_engine("batch").engine_mode, "batch")
        with self.assertRaises(ValueError):
            ShardLoomContext(ShardLoomClient(binary=["shardloom"]), engine="spark")

    def test_engine_selection_report_is_explicit_and_no_fallback(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "engine-selection-plan",
                    "live",
                    "unbounded",
                    "append-only",
                    "changelog",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "engine-selection-plan",
                    "status": "success",
                    "summary": "engine selection plan",
                    "human_text": "selected",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "requested_engine_mode", "value": "live"},
                        {"key": "selection_status", "value": "selected"},
                        {"key": "selected_engine_mode", "value": "live"},
                        {"key": "rejection_reasons", "value": "none"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        workflow = sl.read_vortex(
            "orders.vortex",
            client=ShardLoomClient(binary=binary),
            engine_mode="live",
        )

        report = workflow.engine_selection(
            boundedness="unbounded",
            update_mode="append-only",
            output_mode="changelog",
        )

        self.assertEqual(report.requested_engine_mode, "live")
        self.assertEqual(report.selection_status, "selected")
        self.assertEqual(report.selected_engine_mode, "live")
        self.assertEqual(report.rejection_reasons, ())
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_missing_dataframe_affordances_return_report_only_unsupported(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                assert args[-2:] == ["--format", "json"], args
                parts = args[:-2]
                assert parts[0] == "workflow-unsupported-plan", args
                operation = parts[1]
                workflow_summary = parts[2]
                target_ref = parts[3] if len(parts) == 4 else "none"
                canonical = {
                    "from-pandas": "from_pandas",
                    "from-arrow-table": "from_arrow_table",
                    "from-arrow-ipc": "from_arrow_ipc",
                    "to-pandas": "to_pandas",
                    "to-arrow": "to_arrow",
                    "to-arrow-table": "to_arrow_table",
                    "to-arrow-ipc": "to_arrow_ipc",
                    "to-numpy": "to_numpy",
                    "to-python-objects": "to_python_objects",
                    "with-column": "with_column",
                    "group-by": "group_by",
                    "agg": "agg",
                    "sort": "sort",
                    "limit": "limit",
                    "write-vortex": "write_vortex",
                    "write-parquet": "write_parquet",
                    "sql-parse": "sql_parse",
                    "sql-bind": "sql_bind",
                    "sql-plan": "sql_plan",
                    "sql-execute": "sql_execute",
                    "schema-contract": "schema_contract",
                    "describe-schema": "describe_schema",
                    "validate-schema": "validate_schema",
                    "data-quality": "data_quality",
                    "data-quality-summary": "data_quality_summary",
                }.get(operation, operation)
                write_required = operation.startswith("write-") or operation == "quarantine"
                materialization_required = operation in {
                    "collect", "from-pandas", "from-arrow-table", "from-arrow-ipc",
                    "to-pandas", "to-arrow", "to-arrow-table", "to-arrow-ipc",
                    "to-numpy", "to-python-objects", "write-vortex", "write-parquet",
                    "quarantine", "preview", "display",
                }
                runtime_required = operation not in {
                    "from-pandas", "from-arrow-table", "from-arrow-ipc",
                    "schema-contract", "schema", "describe-schema", "validate-schema",
                    "data-quality", "sql-parse", "sql-bind", "sql-plan",
                }
                code = (
                    "SL_UNSUPPORTED_SQL"
                    if operation in {
                        "sql", "sql-parse", "sql-bind", "sql-plan", "sql-execute",
                        "with-column", "group-by", "agg", "sort", "join",
                        "aggregate", "window",
                    }
                    else "SL_UNSUPPORTED_EFFECT"
                    if operation == "quarantine"
                    else "SL_MATERIALIZATION_REQUIRED"
                    if materialization_required
                    else "SL_NOT_IMPLEMENTED"
                )
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": code,
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": f"cg21.workflow.{canonical}",
                        "reason": f"{canonical} is unsupported",
                        "suggested_next_step": "inspect capability and evidence reports",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    }],
                    "fields": [
                        {"key": "mode", "value": "workflow_unsupported_plan"},
                        {"key": "workflow_operation", "value": canonical},
                        {"key": "workflow_summary", "value": workflow_summary},
                        {"key": "target_ref", "value": target_ref},
                        {"key": "blocker_id", "value": f"cg21.workflow.{canonical}.unsupported"},
                        {"key": "required_evidence", "value": "execution_certificate,native_io_certificate"},
                        {"key": "suggested_next_action", "value": "inspect capability and evidence reports"},
                        {"key": "materialization_required", "value": str(materialization_required).lower()},
                        {"key": "write_required", "value": str(write_required).lower()},
                        {"key": "runtime_required", "value": str(runtime_required).lower()},
                        {"key": "plan_only", "value": "true"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        workflow = (
            sl.read_csv("events.csv", client=ShardLoomClient(binary=binary))
            .filter("id > 0")
            .select("id", "amount")
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        reports = (
            workflow.profile(),
            workflow.collect(),
            sl.from_pandas(object(), client=ShardLoomClient(binary=binary)),
            sl.from_arrow_table(object(), client=ShardLoomClient(binary=binary)),
            sl.from_arrow_ipc("events.arrow", client=ShardLoomClient(binary=binary)),
            workflow.to_pandas(),
            workflow.to_arrow(),
            workflow.to_arrow_table(),
            workflow.to_arrow_ipc(),
            workflow.to_numpy(),
            workflow.to_python_objects(),
            workflow.with_column("date", "to_date(ts)"),
            workflow.group_by("id").agg(total="sum(amount)"),
            workflow.agg("sum(amount)"),
            workflow.sort("amount", descending=True),
            workflow.write_vortex("out.vortex"),
            workflow.write_parquet("out.parquet"),
            workflow.sql("select * from events"),
            ctx.sql_parse("select * from events"),
            ctx.sql_bind("select * from events"),
            ctx.sql_plan("select * from events"),
            ctx.sql_execute("select * from events"),
            workflow.join("dim.csv", on="id"),
            workflow.aggregate("sum(amount)"),
            workflow.window("row_number() over (partition by id)"),
            workflow.schema_contract({"id": "int64"}),
            workflow.schema(),
            workflow.describe_schema(),
            workflow.validate_schema({"id": "int64"}),
            workflow.data_quality_check("not_null:id"),
            workflow.data_quality_summary(),
            workflow.quarantine("bad.vortex"),
            workflow.preview(limit=5),
            workflow.display(),
        )

        self.assertEqual(len(reports), 34)
        for report in reports:
            self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
            self.assertEqual(report.envelope.status, "unsupported")
            self.assertTrue(report.blocker_id and report.blocker_id.startswith("cg21.workflow."))
            if report.operation in {"from-pandas", "from-arrow-table", "from-arrow-ipc"}:
                self.assertTrue(report.envelope.field("workflow_summary", "").startswith("read_"))
            elif report.operation.startswith("sql-"):
                self.assertEqual(report.envelope.field("workflow_summary"), "sql(statement)")
            else:
                summary = report.envelope.field("workflow_summary")
                self.assertTrue(summary and summary.startswith("read_csv(events.csv)"))
            self.assertEqual(
                report.required_evidence,
                ("execution_certificate", "native_io_certificate"),
            )
            self.assertEqual(
                report.suggested_next_action,
                "inspect capability and evidence reports",
            )
            self.assertFalse(report.fallback_attempted)
            self.assertFalse(report.runtime_execution)
            self.assertFalse(report.data_read)
            self.assertFalse(report.write_io)
        by_operation = {report.operation: report for report in reports}
        self.assertEqual(
            by_operation["with-column"].envelope.field("target_ref"),
            "date=to_date(ts)",
        )
        agg_targets = [
            report.envelope.field("target_ref")
            for report in reports
            if report.operation == "agg"
        ]
        self.assertIn("group_by:id;agg:total=sum(amount)", agg_targets)
        self.assertIn("sum(amount)", agg_targets)
        self.assertEqual(by_operation["agg"].envelope.field("workflow_operation"), "agg")
        self.assertEqual(
            by_operation["sort"].envelope.field("target_ref"),
            "desc:amount",
        )
        self.assertEqual(
            by_operation["write-vortex"].envelope.field("target_ref"),
            "out.vortex",
        )
        self.assertTrue(by_operation["write-vortex"].envelope.field_bool("write_required"))
        self.assertEqual(
            by_operation["sql"].envelope.field("target_ref"),
            "select * from events",
        )
        self.assertFalse(by_operation["sql-parse"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["sql-bind"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["sql-plan"].envelope.field_bool("runtime_required"))
        self.assertTrue(by_operation["sql-execute"].envelope.field_bool("runtime_required"))
        self.assertEqual(by_operation["window"].envelope.field("workflow_operation"), "window")
        self.assertFalse(by_operation["schema-contract"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["schema"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["describe-schema"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["validate-schema"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["data-quality"].envelope.field_bool("runtime_required"))
        self.assertEqual(by_operation["quarantine"].envelope.field("target_ref"), "bad.vortex")
        self.assertTrue(by_operation["quarantine"].envelope.field_bool("write_required"))
        self.assertTrue(by_operation["preview"].envelope.field_bool("materialization_required"))
        self.assertEqual(by_operation["display"].envelope.field("workflow_operation"), "display")

    def test_engine_capability_matrix_view_exposes_blocked_live_hybrid_claims(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == ["engine-capability-matrix", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "engine-capability-matrix",
                    "status": "success",
                    "summary": "engine capability matrix",
                    "human_text": "matrix",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "engine_modes", "value": "batch,live,hybrid"},
                        {"key": "live_hybrid_claim_blocked_count", "value": "2"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary), engine="auto")

        matrix = ctx.engine_capability_matrix()

        self.assertEqual(matrix.engine_modes, ("batch", "live", "hybrid"))
        self.assertEqual(matrix.live_hybrid_claim_blocked_count, 2)
        self.assertFalse(matrix.fallback_attempted)
        self.assertFalse(matrix.external_engine_invoked)

    def test_context_exposes_rest_api_contract_views(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                command = args[0]
                if args == ["rest-api-contract-plan", "--format", "json"]:
                    fields = [
                        {"key": "api_version", "value": "v1"},
                        {"key": "openapi_version", "value": "3.2.0"},
                        {"key": "openapi_contract_path", "value": "docs/api/shardloom-openapi-v1.yaml"},
                        {"key": "represented_resources", "value": "health,version,capabilities,governance"},
                        {"key": "discovery_endpoint_paths", "value": "/v1/health,/v1/capabilities"},
                        {"key": "openapi_contract_artifact_checked_in", "value": "true"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                    ]
                elif args == ["serve", "--mode", "discovery", "--bind", "127.0.0.1:8787", "--format", "json"]:
                    fields = [
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
                        {"key": "fallback_attempted", "value": "false"},
                    ]
                elif args == ["rest-api-plan-preview", "certified-local-batch", "--format", "json"]:
                    fields = [
                        {"key": "scenario", "value": "certified-local-batch"},
                        {"key": "preview_status", "value": "certified_preview"},
                        {"key": "plan_handle", "value": "plan://cg23/certified-local-batch"},
                        {"key": "preview_operations", "value": "plan_handle,validate,explain,estimate,unsupported_report,certification_preview"},
                        {"key": "stage_order", "value": "parser,binder,native_logical,native_physical,execution_readiness,evidence_readiness,certification"},
                        {"key": "parser_stage_status", "value": "ready"},
                        {"key": "binder_stage_status", "value": "ready"},
                        {"key": "native_logical_stage_status", "value": "ready"},
                        {"key": "native_physical_stage_status", "value": "ready"},
                        {"key": "execution_readiness_stage_status", "value": "ready"},
                        {"key": "evidence_readiness_stage_status", "value": "ready"},
                        {"key": "certification_stage_status", "value": "certified"},
                        {"key": "problem_details_emitted", "value": "false"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"},
                    ]
                elif args == ["rest-api-local-lifecycle", "certified-local-batch", "--format", "json"]:
                    fields = [
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
                        {"key": "query_execution", "value": "true"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "local_execution_performed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"},
                    ]
                elif args == ["rest-api-event-stream", "certified-live-fixture", "--format", "json"]:
                    fields = [
                        {"key": "scenario", "value": "certified-live-fixture"},
                        {"key": "event_stream_status", "value": "certified_fixture"},
                        {"key": "stream_id", "value": "event-stream://cg23/live-fixture/group-count"},
                        {"key": "stream_ref", "value": "event-stream://cg23/live-fixture/group-count"},
                        {"key": "engine_mode", "value": "live"},
                        {"key": "delivery_protocols", "value": "server_sent_events,websocket_optional"},
                        {"key": "event_types", "value": "progress,state,checkpoint,watermark,certificate,lineage,benchmark,hybrid_hot_cold_contribution"},
                        {"key": "certificate_ref_summary", "value": "certificates/cg22/live/fixture/freshness.json"},
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
                        {"key": "execution_delegated", "value": "false"},
                    ]
                elif args == ["rest-api-security-governance", "safe-local-default", "--format", "json"]:
                    fields = [
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
                        {"key": "execution_delegated", "value": "false"},
                    ]
                elif args == ["rest-api-data-plane", "artifact-reference-default", "--format", "json"]:
                    fields = [
                        {"key": "scenario", "value": "artifact-reference-default"},
                        {"key": "data_plane_status", "value": "contract_available"},
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
                        {"key": "execution_delegated", "value": "false"},
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
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        contract = ctx.rest_api_contract_plan()
        discovery = ctx.serve_discovery_contract()
        preview = ctx.rest_api_plan_preview()
        lifecycle = ctx.rest_api_local_lifecycle()
        event_stream = ctx.rest_api_event_stream()
        security = ctx.rest_api_security_governance()
        data_plane = ctx.rest_api_data_plane()

        self.assertEqual(contract.api_version, "v1")
        self.assertEqual(contract.openapi_version, "3.2.0")
        self.assertIn("governance", contract.represented_resources)
        self.assertTrue(contract.contract_artifact_checked_in)
        self.assertFalse(contract.server_started)
        self.assertFalse(contract.network_listener_opened)
        self.assertFalse(contract.fallback_attempted)
        self.assertEqual(discovery.server_mode, "discovery")
        self.assertEqual(discovery.bind, "127.0.0.1:8787")
        self.assertTrue(discovery.contract_only)
        self.assertFalse(discovery.server_started)
        self.assertFalse(discovery.network_listener_opened)
        self.assertEqual(preview.preview_status, "certified_preview")
        self.assertEqual(preview.plan_handle, "plan://cg23/certified-local-batch")
        self.assertEqual(preview.stage_statuses["certification"], "certified")
        self.assertFalse(preview.problem_details_emitted)
        self.assertFalse(preview.runtime_execution)
        self.assertFalse(preview.fallback_attempted)
        self.assertFalse(preview.execution_delegated)
        self.assertEqual(lifecycle.lifecycle_status, "succeeded")
        self.assertEqual(lifecycle.result_ref, "result://cg23/certified-local-batch/0001")
        self.assertTrue(lifecycle.inline_json_available)
        self.assertTrue(lifecycle.vortex_artifact_available)
        self.assertFalse(lifecycle.arrow_ipc_certified_native)
        self.assertTrue(lifecycle.runtime_execution)
        self.assertTrue(lifecycle.local_execution_performed)
        self.assertFalse(lifecycle.fallback_attempted)
        self.assertFalse(lifecycle.execution_delegated)
        self.assertEqual(event_stream.event_stream_status, "certified_fixture")
        self.assertEqual(event_stream.engine_mode, "live")
        self.assertIn("server_sent_events", event_stream.delivery_protocols)
        self.assertIn("watermark", event_stream.event_types)
        self.assertTrue(event_stream.sse_first)
        self.assertFalse(event_stream.websocket_required)
        self.assertTrue(event_stream.workload_certified)
        self.assertFalse(event_stream.production_claim_allowed)
        self.assertFalse(event_stream.broker_required)
        self.assertFalse(event_stream.broker_io)
        self.assertFalse(event_stream.object_store_io)
        self.assertFalse(event_stream.fallback_attempted)
        self.assertEqual(security.governance_status, "available_contract")
        self.assertIn("token:reference_only_contract", security.auth_postures)
        self.assertIn("write:policy_required", security.api_scopes)
        self.assertIn("certify_preview:allowed", security.mcp_tools)
        self.assertIn("opentelemetry_traces", security.evidence_model_signals)
        self.assertTrue(security.credential_references_only)
        self.assertTrue(security.secrets_redacted)
        self.assertFalse(security.raw_secret_values_present)
        self.assertTrue(security.destructive_policy_required)
        self.assertFalse(security.destructive_operations_allowed)
        self.assertTrue(security.mcp_dry_run_default)
        self.assertFalse(security.mcp_effectful_tools_allowed)
        self.assertFalse(security.opentelemetry_exporter_enabled)
        self.assertTrue(security.openlineage_facets_mapped)
        self.assertTrue(security.problem_details_mapped)
        self.assertFalse(security.credential_resolution)
        self.assertFalse(security.secret_resolution)
        self.assertFalse(security.fallback_attempted)
        self.assertEqual(data_plane.data_plane_status, "contract_available")
        self.assertIn("vortex_artifact:native_vortex_artifact", data_plane.transfer_modes)
        self.assertIn("iceberg_rest_catalog", data_plane.standards_names)
        self.assertIn("vortex_artifact", data_plane.preferred_large_payload_modes)
        self.assertEqual(data_plane.large_payload_threshold_bytes, 1048576)
        self.assertTrue(data_plane.rest_control_plane_sufficient_for_local_use)
        self.assertFalse(data_plane.flight_adbc_required_for_basic_local_use)
        self.assertFalse(data_plane.flight_ticket_supported)
        self.assertFalse(data_plane.adbc_endpoint_supported)
        self.assertTrue(data_plane.decoded_columnar_boundary_declared)
        self.assertTrue(data_plane.materialization_declared)
        self.assertTrue(data_plane.result_policy_declared)
        self.assertEqual(data_plane.standards_matrix_count, 11)
        self.assertFalse(data_plane.flight_server_started)
        self.assertFalse(data_plane.adbc_endpoint_opened)
        self.assertFalse(data_plane.broker_io)
        self.assertFalse(data_plane.object_store_io)
        self.assertFalse(data_plane.catalog_probe)
        self.assertFalse(data_plane.fallback_attempted)

    def test_live_and_hybrid_fixture_reports_are_explicit(self) -> None:
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
                        {"key": "fallback_attempted", "value": "false"},
                    ]
                elif args == ["live-fixture-run", "group-count", "metric", "--format", "json"]:
                    command = "live-fixture-run"
                    fields = [
                        {"key": "fixture_operator", "value": "group_count"},
                        {"key": "input_change_record_count", "value": "10"},
                        {"key": "active_state_key_count", "value": "3"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "output_rows", "value": "east:group_count:2|west:group_count:1"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "state_certificate_status", "value": "certified"},
                        {"key": "continuous_view_certificate_status", "value": "certified"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
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
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
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
        ctx = ShardLoomContext(ShardLoomClient(binary=binary), engine="live")

        contract = ctx.live_change_contract_plan()
        fixture = ctx.live_fixture_run("group-count", "metric")
        hybrid = ctx.hybrid_overlay_run("group-count", "metric")

        self.assertEqual(contract.change_record_fields[0], "key")
        self.assertIn("tombstone", contract.operations)
        self.assertIn("group_count", contract.fixture_operators)
        self.assertFalse(contract.runtime_execution)
        self.assertFalse(contract.fallback_attempted)
        self.assertEqual(fixture.operator, "group_count")
        self.assertEqual(fixture.input_change_record_count, 10)
        self.assertEqual(fixture.active_state_key_count, 3)
        self.assertEqual(fixture.output_rows, ("east:group_count:2", "west:group_count:1"))
        self.assertTrue(fixture.all_certified)
        self.assertTrue(fixture.runtime_execution)
        self.assertFalse(fixture.data_read)
        self.assertFalse(fixture.write_io)
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

    def test_lazy_workflow_report_collects_explain_estimate_and_certify_surfaces(self) -> None:
        expected_workflow = (
            "read_vortex(orders.vortex) -> filter(gte:value:3) -> "
            "select(metric,value) -> limit(5)"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                args = sys.argv[1:]
                status = "success"
                command = None
                fields = []
                diagnostics = []
                returncode = 0

                if args == ["vortex-read-plan", "orders.vortex", "--format", "json"]:
                    command = "vortex-read-plan"
                    fields = [
                        {{"key": "plan_only", "value": "true"}},
                        {{"key": "data_read", "value": "false"}},
                        {{"key": "data_materialized", "value": "false"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                elif args == ["explain", {expected_workflow!r}, "--format", "json"]:
                    command = "explain"
                    status = "unsupported"
                    returncode = 1
                    fields = [
                        {{"key": "mode", "value": "plan_only"}},
                        {{"key": "materialization_boundary_reported", "value": "false"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                    diagnostics = [{{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "planning",
                        "reason": "Real planning is not implemented yet.",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}}
                    }}]
                elif args == ["estimate", {expected_workflow!r}, "--format", "json"]:
                    command = "estimate"
                    status = "unsupported"
                    returncode = 1
                    fields = [
                        {{"key": "mode", "value": "plan_only"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                    diagnostics = [{{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "estimation",
                        "reason": "Native estimate planning is not implemented yet.",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}}
                    }}]
                elif args == ["execution-certificate-plan", "--format", "json"]:
                    command = "execution-certificate-plan"
                    fields = [
                        {{"key": "certificate_evaluation_performed", "value": "false"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                elif args == ["native-io-envelope-plan", "--format", "json"]:
                    command = "native-io-envelope-plan"
                    fields = [
                        {{"key": "materialization_boundary_reported", "value": "true"}},
                        {{"key": "per_path_certificate_required", "value": "true"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                elif args == ["capabilities", "certification", "--format", "json"]:
                    command = "capabilities"
                    fields = [
                        {{"key": "scope", "value": "certification"}},
                        {{"key": "certification_status", "value": "planned"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                else:
                    raise AssertionError(args)

                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": status,
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": diagnostics,
                    "fields": fields,
                }}))
                sys.exit(returncode)
                """
            )
        )
        workflow = (
            sl.read_vortex("orders.vortex", client=ShardLoomClient(binary=binary))
            .filter("gte:value:3")
            .select("metric", "value")
            .limit(5)
        )

        report = workflow.unsupported_report()

        self.assertEqual(report.input_plan.command, "vortex-read-plan")
        self.assertEqual(report.explain.status, "unsupported")
        self.assertEqual(report.estimate.status, "unsupported")
        self.assertEqual(
            report.certification.execution_certificate_plan.command,
            "execution-certificate-plan",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertIn(
            "Real planning is not implemented yet.",
            report.unsupported_reasons,
        )
        self.assertIn(
            "Native estimate planning is not implemented yet.",
            report.unsupported_reasons,
        )
        self.assertIn(
            "native-io-envelope-plan:materialization_boundary_reported=true",
            report.materialization_boundaries,
        )


if __name__ == "__main__":
    unittest.main()
