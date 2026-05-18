from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

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

    def test_from_rows_write_invokes_generated_source_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-user-rows-smoke",
                    "target/generated.jsonl",
                    "id:int64,label:utf8",
                    "id=1,label=alpha;id=2,label=beta",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-user-rows-smoke",
                    "status": "success",
                    "summary": "generated",
                    "human_text": "generated",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/generated.jsonl"},
                        {"key": "generated_source_kind", "value": "user_rows"},
                        {"key": "generated_source_row_count", "value": "2"},
                        {"key": "generated_source_created", "value": "true"},
                        {"key": "source_io_performed", "value": "false"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "sql_dataframe_runtime_claim_allowed", "value": "false"},
                        {"key": "object_store_lakehouse_claim_allowed", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.from_rows(
            [
                {"id": 1, "label": "alpha"},
                {"id": 2, "label": "beta"},
            ]
        ).write("target/generated.jsonl")

        self.assertEqual(report.envelope.command, "generated-source-user-rows-smoke")
        self.assertEqual(report.output_path, "target/generated.jsonl")
        self.assertEqual(report.generated_source_kind, "user_rows")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertEqual(report.generated_source_certificate_status, "present")
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_file_sink",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_from_rows_validates_scoped_generated_source_inputs(self) -> None:
        with self.assertRaises(ValueError):
            sl.from_rows([], binary=["definitely-missing-shardloom"])
        with self.assertRaises(TypeError):
            sl.from_rows([object()], binary=["definitely-missing-shardloom"])  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            sl.from_rows(
                [{"id": 1}, {"id": 2, "label": "extra"}],
                binary=["definitely-missing-shardloom"],
            )
        with self.assertRaises(TypeError):
            sl.from_rows(
                [{"id": 1}, {"id": "two"}],
                binary=["definitely-missing-shardloom"],
            )

    def test_range_write_invokes_engine_native_generated_source_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-range-smoke",
                    "target/range.jsonl",
                    "2",
                    "8",
                    "--step",
                    "2",
                    "--column",
                    "id",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-range-smoke",
                    "status": "success",
                    "summary": "generated range",
                    "human_text": "generated range",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/range.jsonl"},
                        {"key": "generated_source_kind", "value": "range"},
                        {"key": "generated_source_range_start", "value": "2"},
                        {"key": "generated_source_range_end", "value": "8"},
                        {"key": "generated_source_range_step", "value": "2"},
                        {"key": "generated_source_range_column", "value": "id"},
                        {"key": "generated_source_row_count", "value": "3"},
                        {"key": "generated_source_created", "value": "true"},
                        {"key": "source_io_performed", "value": "false"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "sql_dataframe_runtime_claim_allowed", "value": "false"},
                        {"key": "object_store_lakehouse_claim_allowed", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.range(2, 8, step=2, column="id").write("target/range.jsonl")

        self.assertEqual(report.envelope.command, "generated-source-range-smoke")
        self.assertEqual(report.output_path, "target/range.jsonl")
        self.assertEqual(report.generated_source_kind, "range")
        self.assertEqual(report.generated_source_row_count, 3)
        self.assertEqual(report.generated_source_range_start, 2)
        self.assertEqual(report.generated_source_range_end, 8)
        self.assertEqual(report.generated_source_range_step, 2)
        self.assertEqual(report.generated_source_range_column, "id")
        self.assertEqual(report.generated_source_certificate_status, "present")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_range_validates_scoped_generated_source_inputs(self) -> None:
        with self.assertRaises(TypeError):
            sl.range(True, 10, binary=["definitely-missing-shardloom"])
        with self.assertRaises(TypeError):
            sl.range(0, "10", binary=["definitely-missing-shardloom"])  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            sl.range(0, 10, step=0, binary=["definitely-missing-shardloom"])
        with self.assertRaises(ValueError):
            sl.range(0, 10, column="", binary=["definitely-missing-shardloom"])

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

    def test_engine_selection_report_reads_external_engine_from_typed_policy(self) -> None:
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
                    "policy": {
                        "fields": [
                            {"key": "external_engine_invoked", "value": "true"}
                        ]
                    },
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

        self.assertTrue(report.external_engine_invoked)

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

    def test_engine_capability_matrix_reads_external_engine_from_typed_policy(self) -> None:
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
                    "policy": {
                        "fields": [
                            {"key": "external_engine_invoked", "value": "true"}
                        ]
                    },
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

        self.assertTrue(matrix.external_engine_invoked)

    def test_context_exposes_universal_compatibility_scoreboard(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == ["capabilities", "compatibility", "--format", "json"], sys.argv
                fields = [
                    {"key": "scope", "value": "compatibility"},
                    {"key": "universal_compatibility_scoreboard_schema_version", "value": "shardloom.universal_compatibility_coverage_scoreboard.v1"},
                    {"key": "universal_compatibility_scoreboard_id", "value": "gar-compat-1.universal_compatibility_coverage_scoreboard"},
                    {"key": "universal_compatibility_scoreboard_docs_ref", "value": "docs/architecture/universal-compatibility-coverage-scoreboard.md"},
                    {"key": "universal_compatibility_scoreboard_data_ref", "value": "docs/architecture/universal-compatibility-coverage-scoreboard.json"},
                    {"key": "universal_compatibility_support_status_vocabulary", "value": "runtime-supported,smoke-supported,report-only,blocked,not-planned"},
                    {"key": "universal_compatibility_row_count", "value": "4"},
                    {"key": "universal_compatibility_row_order", "value": "vortex,object_store_s3_gcs_adls,sql_values_literals,foundry"},
                    {"key": "universal_compatibility_runtime_supported_count", "value": "1"},
                    {"key": "universal_compatibility_smoke_supported_count", "value": "0"},
                    {"key": "universal_compatibility_report_only_count", "value": "2"},
                    {"key": "universal_compatibility_blocked_count", "value": "1"},
                    {"key": "universal_compatibility_claim_boundary", "value": "capability map only"},
                    {"key": "universal_compatibility_all_rows_fallback_attempted_false", "value": "true"},
                    {"key": "universal_compatibility_all_rows_external_engine_invoked_false", "value": "true"},
                    {"key": "universal_compatibility_object_store_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_table_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_foundry_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_sql_dataframe_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_contract_schema_version", "value": "shardloom.universal_compatibility.generated_output_contract.v1"},
                    {"key": "universal_compatibility_generated_output_contract_id", "value": "gar-compat-1b.source_free_generated_output_contract"},
                    {"key": "universal_compatibility_generated_output_row_order", "value": "no_dataset_smoke,python_ctx_from_rows,sql_values,local_output_only_generated_source_posture"},
                    {"key": "universal_compatibility_generated_output_python_row_order", "value": "python_ctx_from_rows"},
                    {"key": "universal_compatibility_generated_output_sql_row_order", "value": "sql_values"},
                    {"key": "universal_compatibility_generated_output_dataframe_row_order", "value": ""},
                    {"key": "universal_compatibility_generated_output_claim_gate_status", "value": "fixture_smoke_only"},
                    {"key": "universal_compatibility_generated_output_no_dataset_smoke_separate", "value": "true"},
                    {"key": "universal_compatibility_generated_output_local_output_only", "value": "true"},
                    {"key": "universal_compatibility_generated_output_output_certificate_required", "value": "true"},
                    {"key": "universal_compatibility_generated_output_object_store_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_foundry_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_broad_sql_dataframe_claim_allowed", "value": "false"},
                    {"key": "universal_compatibility_generated_output_all_rows_fallback_attempted_false", "value": "true"},
                    {"key": "universal_compatibility_generated_output_all_rows_external_engine_invoked_false", "value": "true"},
                    {"key": "universal_compatibility_object_store_ladder_schema_version", "value": "shardloom.universal_compatibility.object_store_admission_ladder.v1"},
                    {"key": "universal_compatibility_object_store_ladder_id", "value": "gar-compat-1c.object_store_runtime_admission_ladder"},
                    {"key": "universal_compatibility_object_store_ladder_provider_scope", "value": "s3,gcs,adls"},
                    {"key": "universal_compatibility_object_store_ladder_row_order", "value": "object_store_uri_parse,credential_policy,public_no_credential_read,authenticated_read,byte_range_read,write_staging,commit_protocol"},
                    {"key": "universal_compatibility_object_store_ladder_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_object_store_ladder_all_rows_no_effects", "value": "true"},
                    {"key": "universal_compatibility_table_format_matrix_schema_version", "value": "shardloom.universal_compatibility.table_format_boundary_matrix.v1"},
                    {"key": "universal_compatibility_table_format_matrix_id", "value": "gar-compat-1d.table_format_boundary_matrix"},
                    {"key": "universal_compatibility_table_format_matrix_format_scope", "value": "iceberg,delta,hudi"},
                    {"key": "universal_compatibility_table_format_matrix_row_order", "value": "table_metadata_read,table_scan,delete_tombstone,commit,object_store_coupling"},
                    {"key": "universal_compatibility_table_format_matrix_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_table_format_matrix_local_metadata_smoke_available", "value": "true"},
                    {"key": "universal_compatibility_table_format_matrix_all_rows_no_io_no_fallback", "value": "true"},
                    {"key": "universal_compatibility_database_warehouse_matrix_schema_version", "value": "shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1"},
                    {"key": "universal_compatibility_database_warehouse_matrix_id", "value": "gar-compat-1e.database_warehouse_import_export_boundary"},
                    {"key": "universal_compatibility_database_warehouse_matrix_endpoint_scope", "value": "sqlite,postgres,mysql,jdbc,odbc,snowflake,bigquery,databricks_sql"},
                    {"key": "universal_compatibility_database_warehouse_matrix_row_order", "value": "sqlite_file,postgres,jdbc_odbc,snowflake,bigquery,databricks_sql"},
                    {"key": "universal_compatibility_database_warehouse_matrix_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_database_warehouse_matrix_import_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_database_warehouse_matrix_export_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_database_warehouse_matrix_query_pushdown_supported", "value": "false"},
                    {"key": "universal_compatibility_database_warehouse_matrix_all_rows_no_effects", "value": "true"},
                ]
                for row_id, scope, family, connector_type, status, credential, network, blocker in [
                    ("sqlite_file", "sqlite", "database_file", "embedded_file_database", "report-only", "false", "false", "gar-compat-1e.sqlite_import_export_runtime_blocked"),
                    ("postgres", "postgres", "database_service", "network_database", "blocked", "true", "true", "gar-compat-1e.postgres_connector_runtime_blocked"),
                    ("jdbc_odbc", "jdbc,odbc", "connector_bridge", "driver_bridge", "blocked", "true", "true", "gar-compat-1e.jdbc_odbc_driver_loading_blocked"),
                    ("snowflake", "snowflake", "warehouse_service", "cloud_warehouse", "blocked", "true", "true", "gar-compat-1e.snowflake_connector_runtime_blocked"),
                    ("bigquery", "bigquery", "warehouse_service", "cloud_warehouse", "blocked", "true", "true", "gar-compat-1e.bigquery_connector_runtime_blocked"),
                    ("databricks_sql", "databricks_sql", "warehouse_service", "cloud_warehouse", "blocked", "true", "true", "gar-compat-1e.databricks_sql_connector_runtime_blocked"),
                ]:
                    prefix = f"universal_compatibility_database_warehouse_matrix_row_{row_id}"
                    fields.extend([
                        {"key": f"{prefix}_endpoint_scope", "value": scope},
                        {"key": f"{prefix}_endpoint_family", "value": family},
                        {"key": f"{prefix}_connector_type", "value": connector_type},
                        {"key": f"{prefix}_support_status", "value": status},
                        {"key": f"{prefix}_credential_required", "value": credential},
                        {"key": f"{prefix}_network_required", "value": network},
                        {"key": f"{prefix}_driver_dependency_required", "value": "true"},
                        {"key": f"{prefix}_credential_resolution_performed", "value": "false"},
                        {"key": f"{prefix}_network_probe_performed", "value": "false"},
                        {"key": f"{prefix}_driver_loaded", "value": "false"},
                        {"key": f"{prefix}_import_runtime_supported", "value": "false"},
                        {"key": f"{prefix}_export_runtime_supported", "value": "false"},
                        {"key": f"{prefix}_query_pushdown_supported", "value": "false"},
                        {"key": f"{prefix}_external_baseline_only", "value": "true"},
                        {"key": f"{prefix}_native_io_certificate_status", "value": "not_emitted_blocked"},
                        {"key": f"{prefix}_fallback_attempted", "value": "false"},
                        {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                        {"key": f"{prefix}_blocker_id", "value": blocker},
                        {"key": f"{prefix}_required_evidence", "value": "future_evidence"},
                        {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                        {"key": f"{prefix}_claim_boundary", "value": "claim boundary"},
                    ])
                for row_id, behavior, status, local_smoke, blocker in [
                    ("table_metadata_read", "metadata_read", "report-only", "true", "gar-compat-1d.table_format_metadata_runtime_blocked"),
                    ("table_scan", "table_scan", "blocked", "false", "gar-compat-1d.table_scan_runtime_blocked"),
                    ("delete_tombstone", "delete_tombstone", "report-only", "true", "gar-compat-1d.delete_tombstone_runtime_blocked"),
                    ("commit", "commit", "blocked", "false", "gar-compat-1d.table_commit_blocked"),
                    ("object_store_coupling", "object_store_coupling", "blocked", "false", "gar-compat-1d.object_store_coupling_blocked"),
                ]:
                    prefix = f"universal_compatibility_table_format_matrix_row_{row_id}"
                    fields.extend([
                        {"key": f"{prefix}_format_scope", "value": "iceberg,delta,hudi"},
                        {"key": f"{prefix}_behavior", "value": behavior},
                        {"key": f"{prefix}_support_status", "value": status},
                        {"key": f"{prefix}_local_metadata_smoke_related", "value": local_smoke},
                        {"key": f"{prefix}_table_format_dependency_required", "value": "true"},
                        {"key": f"{prefix}_catalog_io_allowed", "value": "false"},
                        {"key": f"{prefix}_object_store_io_allowed", "value": "false"},
                        {"key": f"{prefix}_table_metadata_read_allowed", "value": "false"},
                        {"key": f"{prefix}_table_data_read_allowed", "value": "false"},
                        {"key": f"{prefix}_delete_tombstone_runtime_allowed", "value": "false"},
                        {"key": f"{prefix}_write_io_allowed", "value": "false"},
                        {"key": f"{prefix}_commit_allowed", "value": "false"},
                        {"key": f"{prefix}_rollback_allowed", "value": "false"},
                        {"key": f"{prefix}_native_io_certificate_status", "value": "not_emitted_blocked"},
                        {"key": f"{prefix}_fallback_attempted", "value": "false"},
                        {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                        {"key": f"{prefix}_blocker_id", "value": blocker},
                        {"key": f"{prefix}_required_evidence", "value": "future_evidence"},
                        {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                        {"key": f"{prefix}_claim_boundary", "value": "claim boundary"},
                    ])
                for row_id, stage, status, credential_policy_status, blocker in [
                    ("object_store_uri_parse", "uri_parse", "report-only", "not_required_for_parse", "gar-compat-1c.uri_parse_only_no_provider_runtime"),
                    ("credential_policy", "credential_policy", "blocked", "required_not_admitted", "gar-compat-1c.credential_resolution_blocked"),
                    ("public_no_credential_read", "public_no_credential_read", "blocked", "public_read_policy_required", "gar-compat-1c.public_read_network_runtime_blocked"),
                    ("authenticated_read", "authenticated_read", "blocked", "authenticated_read_policy_required", "gar-compat-1c.authenticated_read_runtime_blocked"),
                    ("byte_range_read", "byte_range_read", "blocked", "read_policy_required", "gar-compat-1c.byte_range_read_runtime_blocked"),
                    ("write_staging", "write_staging", "blocked", "write_policy_required", "gar-compat-1c.write_staging_runtime_blocked"),
                    ("commit_protocol", "commit_protocol", "blocked", "commit_policy_required", "gar-compat-1c.commit_protocol_runtime_blocked"),
                ]:
                    prefix = f"universal_compatibility_object_store_ladder_row_{row_id}"
                    fields.extend([
                        {"key": f"{prefix}_provider_scope", "value": "s3,gcs,adls"},
                        {"key": f"{prefix}_stage", "value": stage},
                        {"key": f"{prefix}_support_status", "value": status},
                        {"key": f"{prefix}_credential_policy_status", "value": credential_policy_status},
                        {"key": f"{prefix}_credential_resolution_performed", "value": "false"},
                        {"key": f"{prefix}_network_probe_allowed", "value": "false"},
                        {"key": f"{prefix}_provider_probe_allowed", "value": "false"},
                        {"key": f"{prefix}_byte_range_read_allowed", "value": "false"},
                        {"key": f"{prefix}_full_file_read_allowed", "value": "false"},
                        {"key": f"{prefix}_local_cache_allowed", "value": "false"},
                        {"key": f"{prefix}_write_io_allowed", "value": "false"},
                        {"key": f"{prefix}_commit_protocol_allowed", "value": "false"},
                        {"key": f"{prefix}_object_store_io", "value": "false"},
                        {"key": f"{prefix}_write_io", "value": "false"},
                        {"key": f"{prefix}_native_io_certificate_status", "value": "not_emitted_blocked"},
                        {"key": f"{prefix}_fallback_attempted", "value": "false"},
                        {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                        {"key": f"{prefix}_blocker_id", "value": blocker},
                        {"key": f"{prefix}_required_evidence", "value": "future_evidence"},
                        {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                        {"key": f"{prefix}_claim_boundary", "value": "claim boundary"},
                    ])
                for row_id, surface, family, status, runtime, write_io, generated, output_io, source_cert, output_cert, generated_cert, claim_status, blocker in [
                    ("no_dataset_smoke", "no-dataset smoke / capability proof", "no_dataset_smoke", "smoke-supported", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_no_output_data", "not_applicable_no_generated_rows", "smoke_only", "gar-gen-1.no_dataset_smoke_not_generated_output"),
                    ("python_ctx_from_rows", "Python ctx.from_rows([...]).write(local_jsonl)", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_jsonl_smoke_only"),
                    ("sql_values", "SQL VALUES (...)", "sql_generated_source", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_report_only", "not_emitted_report_only", "not_claim_grade", "gar-gen-1.sql_values_runtime_not_implemented"),
                    ("local_output_only_generated_source_posture", "Generated-source local-output-only posture", "output_boundary", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "local_output_certificate_required", "not_emitted_report_only", "not_claim_grade", "gar-compat-1b.non_local_generated_output_blocked"),
                ]:
                    prefix = f"universal_compatibility_generated_output_row_{row_id}"
                    fields.extend([
                        {"key": f"{prefix}_user_visible_surface", "value": surface},
                        {"key": f"{prefix}_surface_family", "value": family},
                        {"key": f"{prefix}_support_status", "value": status},
                        {"key": f"{prefix}_runtime_execution", "value": runtime},
                        {"key": f"{prefix}_data_read", "value": "false"},
                        {"key": f"{prefix}_write_io", "value": write_io},
                        {"key": f"{prefix}_source_io_performed", "value": "false"},
                        {"key": f"{prefix}_generated_source_created", "value": generated},
                        {"key": f"{prefix}_output_io_performed", "value": output_io},
                        {"key": f"{prefix}_source_native_io_certificate_status", "value": source_cert},
                        {"key": f"{prefix}_output_native_io_certificate_status", "value": output_cert},
                        {"key": f"{prefix}_generated_source_certificate_status", "value": generated_cert},
                        {"key": f"{prefix}_fallback_attempted", "value": "false"},
                        {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                        {"key": f"{prefix}_blocker_id", "value": blocker},
                        {"key": f"{prefix}_required_evidence", "value": "future_evidence"},
                        {"key": f"{prefix}_claim_gate_status", "value": claim_status},
                        {"key": f"{prefix}_claim_boundary", "value": "claim boundary"},
                    ])
                for row_id, surface, family, direction, status, runtime, report_only, credential, network, source_io, output_io, native_status, generated_status, claim_status, blocker, claim_boundary in [
                    ("vortex", "Vortex", "native_file_layout", "read_write", "runtime-supported", "true", "false", "false", "false", "true", "true", "scoped_local_vortex_evidence_backed", "not_applicable", "fixture_smoke_only", "gar-compat-1a.vortex_universal_runtime_evidence_missing", "scoped local Vortex evidence only"),
                    ("object_store_s3_gcs_adls", "S3 / GCS / ADLS", "object_store", "read_write", "blocked", "false", "false", "true", "true", "false", "false", "not_emitted", "not_applicable", "not_claim_grade", "gar-compat-1c.object_store_runtime_blocked", "no object-store runtime claim"),
                    ("sql_values_literals", "SQL VALUES / literals", "sql_frontend", "api", "report-only", "false", "true", "false", "false", "false", "false", "not_emitted", "not_emitted_report_only", "not_claim_grade", "gar-compat-1b.sql_source_free_runtime_blocked", "no SQL runtime claim"),
                    ("foundry", "Foundry", "platform_integration", "api", "report-only", "false", "true", "true", "true", "false", "false", "not_emitted", "not_emitted_report_only", "not_claim_grade", "gar-compat-1a.foundry_platform_proof_missing", "future validation target only"),
                ]:
                    prefix = f"universal_compatibility_row_{row_id}"
                    fields.extend([
                        {"key": f"{prefix}_surface", "value": surface},
                        {"key": f"{prefix}_surface_family", "value": family},
                        {"key": f"{prefix}_direction", "value": direction},
                        {"key": f"{prefix}_support_status", "value": status},
                        {"key": f"{prefix}_runtime_supported", "value": runtime},
                        {"key": f"{prefix}_smoke_supported", "value": "false"},
                        {"key": f"{prefix}_report_only", "value": report_only},
                        {"key": f"{prefix}_credential_required", "value": credential},
                        {"key": f"{prefix}_network_required", "value": network},
                        {"key": f"{prefix}_source_io_performed", "value": source_io},
                        {"key": f"{prefix}_output_io_performed", "value": output_io},
                        {"key": f"{prefix}_native_io_certificate_status", "value": native_status},
                        {"key": f"{prefix}_generated_source_certificate_status", "value": generated_status},
                        {"key": f"{prefix}_fallback_attempted", "value": "false"},
                        {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                        {"key": f"{prefix}_claim_gate_status", "value": claim_status},
                        {"key": f"{prefix}_blocker_id", "value": blocker},
                        {"key": f"{prefix}_required_future_evidence", "value": "future_evidence"},
                        {"key": f"{prefix}_claim_boundary", "value": claim_boundary},
                    ])
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "capabilities",
                    "status": "success",
                    "summary": "compatibility scoreboard",
                    "human_text": "compatibility scoreboard",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        scoreboard = ShardLoomContext(ShardLoomClient(binary=binary)).compatibility_scoreboard()

        self.assertEqual(
            scoreboard.schema_version,
            "shardloom.universal_compatibility_coverage_scoreboard.v1",
        )
        self.assertEqual(
            scoreboard.data_ref,
            "docs/architecture/universal-compatibility-coverage-scoreboard.json",
        )
        self.assertEqual(scoreboard.runtime_supported_count, 1)
        self.assertEqual(scoreboard.blocked_count, 1)
        self.assertEqual(scoreboard.row("object-store-s3-gcs-adls").support_status, "blocked")
        self.assertTrue(scoreboard.row("vortex").supported_for_runtime_claims)
        self.assertTrue(scoreboard.row("foundry").blocked_or_report_only)
        self.assertFalse(scoreboard.object_store_runtime_supported)
        self.assertFalse(scoreboard.sql_dataframe_runtime_supported)
        self.assertFalse(scoreboard.foundry_runtime_supported)
        self.assertTrue(scoreboard.all_rows_no_fallback_no_external_engine)
        generated = scoreboard.source_free_generated_output_contract
        self.assertEqual(
            generated.schema_version,
            "shardloom.universal_compatibility.generated_output_contract.v1",
        )
        self.assertEqual(generated.python_row_order, ("python_ctx_from_rows",))
        self.assertTrue(generated.no_dataset_smoke_separate)
        self.assertTrue(generated.local_output_only)
        self.assertTrue(generated.output_certificate_required)
        self.assertFalse(generated.object_store_runtime_supported)
        self.assertFalse(generated.foundry_runtime_supported)
        self.assertFalse(generated.broad_sql_dataframe_claim_allowed)
        self.assertTrue(generated.all_no_fallback_no_external_engine)
        self.assertTrue(generated.row("python-ctx-from-rows").fixture_smoke_supported)
        self.assertTrue(generated.row("python_ctx_from_rows").generated_source_created)
        self.assertTrue(generated.row("sql_values").report_only)
        self.assertFalse(generated.row("sql_values").runtime_execution)
        self.assertEqual(
            generated.row("local_output_only_generated_source_posture").blocker_id,
            "gar-compat-1b.non_local_generated_output_blocked",
        )
        object_store = scoreboard.object_store_admission_ladder
        self.assertEqual(
            object_store.schema_version,
            "shardloom.universal_compatibility.object_store_admission_ladder.v1",
        )
        self.assertEqual(object_store.provider_scope, ("s3", "gcs", "adls"))
        self.assertFalse(object_store.runtime_supported)
        self.assertTrue(object_store.all_rows_no_effects)
        self.assertTrue(object_store.row("object-store-uri-parse").no_effects_no_fallback)
        self.assertEqual(object_store.row("credential_policy").support_status, "blocked")
        self.assertEqual(
            object_store.row("authenticated_read").credential_policy_status,
            "authenticated_read_policy_required",
        )
        self.assertEqual(
            object_store.row("byte_range_read").blocker_id,
            "gar-compat-1c.byte_range_read_runtime_blocked",
        )
        self.assertFalse(object_store.row("write_staging").write_io_allowed)
        table_formats = scoreboard.table_format_boundary_matrix
        self.assertEqual(
            table_formats.schema_version,
            "shardloom.universal_compatibility.table_format_boundary_matrix.v1",
        )
        self.assertEqual(table_formats.format_scope, ("iceberg", "delta", "hudi"))
        self.assertFalse(table_formats.runtime_supported)
        self.assertTrue(table_formats.local_metadata_smoke_available)
        self.assertTrue(table_formats.all_rows_no_io_no_fallback)
        self.assertTrue(table_formats.row("table-metadata-read").no_io_no_fallback)
        self.assertEqual(table_formats.row("table_scan").support_status, "blocked")
        self.assertEqual(
            table_formats.row("commit").blocker_id,
            "gar-compat-1d.table_commit_blocked",
        )
        self.assertFalse(table_formats.row("object_store_coupling").object_store_io_allowed)
        database_warehouses = scoreboard.database_warehouse_boundary_matrix
        self.assertEqual(
            database_warehouses.schema_version,
            "shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1",
        )
        self.assertIn("snowflake", database_warehouses.endpoint_scope)
        self.assertFalse(database_warehouses.runtime_supported)
        self.assertTrue(database_warehouses.all_rows_no_effects)
        self.assertTrue(database_warehouses.row("sqlite-file").no_effects_no_fallback)
        self.assertEqual(database_warehouses.row("postgres").support_status, "blocked")
        self.assertEqual(
            database_warehouses.row("jdbc_odbc").blocker_id,
            "gar-compat-1e.jdbc_odbc_driver_loading_blocked",
        )
        self.assertFalse(database_warehouses.row("bigquery").query_pushdown_supported)
        self.assertTrue(database_warehouses.row("databricks_sql").external_baseline_only)

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
                        {"key": "execution_mode_vocabulary", "value": "auto,compatibility_import_certified,prepared_vortex,native_vortex,direct_compatibility_transient"},
                        {"key": "execution_mode_selection_schema_version", "value": "shardloom.execution_mode_selection_report.v1"},
                        {"key": "execution_mode_selection_fields", "value": "requested_execution_mode,selected_execution_mode,mode_selection_reason,support_status,fallback_attempted,external_engine_invoked"},
                        {"key": "rest_execution_mode_support_status", "value": "report_only"},
                        {"key": "unsupported_execution_mode_diagnostic_code", "value": "SL_UNSUPPORTED_EXECUTION_MODE"},
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
        self.assertIn("native_vortex", contract.execution_mode_vocabulary)
        self.assertEqual(
            contract.execution_mode_selection_schema_version,
            "shardloom.execution_mode_selection_report.v1",
        )
        self.assertIn("fallback_attempted", contract.execution_mode_selection_fields)
        self.assertEqual(contract.rest_execution_mode_support_status, "report_only")
        self.assertEqual(
            contract.unsupported_execution_mode_diagnostic_code,
            "SL_UNSUPPORTED_EXECUTION_MODE",
        )
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
