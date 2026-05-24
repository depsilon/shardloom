from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from datetime import date, datetime, timezone
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
        arrow_frame = sl.read_arrow_ipc(
            "events.arrow",
            schema={"id": "int64"},
            binary=["definitely-missing-shardloom"],
        )
        avro_frame = sl.read_avro(
            "events.avro",
            schema={"id": "int64"},
            binary=["definitely-missing-shardloom"],
        )
        orc_frame = sl.read_orc(
            "events.orc",
            schema={"id": "int64"},
            binary=["definitely-missing-shardloom"],
        )
        inferred_csv_frame = sl.read(
            "events.csv",
            schema={"id": "int64"},
            binary=["definitely-missing-shardloom"],
        )
        inferred_json_frame = sl.read(
            "events.jsonl",
            schema={"payload": "string"},
            binary=["definitely-missing-shardloom"],
        )
        inferred_arrow_frame = sl.read(
            "events.feather",
            schema={"id": "int64"},
            binary=["definitely-missing-shardloom"],
        )
        inferred_vortex_frame = sl.read(
            "events.vortex",
            binary=["definitely-missing-shardloom"],
        )

        self.assertIsInstance(frame, LazyFrame)
        self.assertEqual(frame.source_format, "csv")
        self.assertEqual(frame.source.schema_map["id"], "int64")
        self.assertEqual(json_frame.source_format, "json")
        self.assertEqual(arrow_frame.source_format, "arrow-ipc")
        self.assertEqual(arrow_frame.source.schema_map["id"], "int64")
        self.assertEqual(arrow_frame.operation_summary, "read_arrow_ipc(events.arrow)")
        self.assertEqual(avro_frame.source_format, "avro")
        self.assertEqual(avro_frame.operation_summary, "read_avro(events.avro)")
        self.assertEqual(orc_frame.source_format, "orc")
        self.assertEqual(orc_frame.operation_summary, "read_orc(events.orc)")
        self.assertEqual(inferred_csv_frame.source_format, "csv")
        self.assertEqual(inferred_json_frame.source_format, "json")
        self.assertEqual(inferred_arrow_frame.source_format, "arrow-ipc")
        self.assertEqual(inferred_vortex_frame.source_format, "vortex")
        self.assertEqual(
            frame.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> limit(10)",
        )
        with self.assertRaisesRegex(ValueError, "cannot infer a local source adapter"):
            sl.read("events.data", binary=["definitely-missing-shardloom"])
        with self.assertRaisesRegex(ValueError, "schema=.*not supported for Vortex"):
            sl.read(
                "events.vortex",
                schema={"id": "int64"},
                binary=["definitely-missing-shardloom"],
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
                    "--source-kind",
                    "user_rows",
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
                        {"key": "output_workspace_path_safety_status", "value": "enforced"},
                        {"key": "output_commit_mode", "value": "atomic_rename_same_directory"},
                        {"key": "output_commit_status", "value": "committed"},
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
        self.assertEqual(report.workspace_path_safety_status, "enforced")
        self.assertEqual(report.output_commit_mode, "atomic_rename_same_directory")
        self.assertEqual(report.output_commit_status, "committed")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")
        self.assertIsInstance(report.evidence_summary, sl.EvidenceSummary)
        self.assertEqual(report.evidence_summary.generated_source_kind, "user_rows")
        self.assertEqual(report.evidence_summary.generated_source_row_count, 2)
        self.assertEqual(report.evidence_summary.output_path, "target/generated.jsonl")
        self.assertEqual(
            report.evidence_summary.output_native_io_certificate_status,
            "certified_local_file_sink",
        )
        self.assertFalse(report.claim_summary.fallback_attempted)
        self.assertFalse(report.claim_summary.external_engine_invoked)
        self.assertEqual(report.claim_summary.claim_gate_status, "fixture_smoke_only")
        self.assertFalse(report.claim_summary.public_performance_claim_allowed)

    def test_generated_rows_select_and_with_column_write_transformed_rows(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-user-rows-smoke",
                    "target/generated-transformed.jsonl",
                    "id:int64,segment:utf8",
                    "id=1,segment=north;id=2,segment=north",
                    "--source-kind",
                    "user_rows",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-user-rows-smoke",
                    "status": "success",
                    "summary": "generated transformed",
                    "human_text": "generated transformed",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/generated-transformed.jsonl"},
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
                        {"key": "performance_claim_allowed", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.from_rows(
                [
                    {"id": 1, "label": "alpha"},
                    {"id": 2, "label": "beta"},
                ]
            )
            .with_column("segment", "lit('north')")
            .select("id", "segment")
            .write("target/generated-transformed.jsonl")
        )

        self.assertEqual(report.envelope.command, "generated-source-user-rows-smoke")
        self.assertEqual(report.generated_source_kind, "user_rows")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_collect_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.result_jsonl, '{"id":2,"label":"beta"}\n')
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertEqual(report.first_result_row, {"id": 2, "label": "beta"})
        self.assertEqual(report.output_row_count, 1)
        self.assertEqual(report.selected_row_count, 2)
        self.assertFalse(report.output_io_performed)
        self.assertEqual(report.output_native_io_certificate_status, "not_requested")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_to_python_objects_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "user_surface_runtime_scope", "value": "format_neutral_sql_frontend"},
                        {"key": "format_specific_boundary_scope", "value": "read_adapter_and_write_sink_only"},
                        {"key": "format_specific_compute_path", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        rows = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter(sl.col("amount") >= 10)
            .limit(2)
            .to_python_objects()
        )

        self.assertEqual(rows, ({"id": 2, "label": "beta"},))

    def test_local_csv_query_builder_schema_quality_helpers_invoke_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label,amount FROM 'target/input.csv' WHERE amount >= 10 LIMIT 100",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\",\\"amount\\":10}\\n{\\"id\\":2,\\"label\\":null,\\"amount\\":15}\\n{\\"id\\":3,\\"label\\":\\"alpha\\",\\"amount\\":21}\\n"},
                        {"key": "output_row_count", "value": "3"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "user_surface_runtime_scope", "value": "format_neutral_sql_frontend"},
                        {"key": "format_specific_boundary_scope", "value": "read_adapter_and_write_sink_only"},
                        {"key": "format_specific_compute_path", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))
        workflow = (
            ctx.read_csv("target/input.csv")
            .select("id", "label", "amount")
            .filter(sl.col("amount") >= 10)
        )

        schema = workflow.schema()
        described = workflow.describe_schema()
        validation = workflow.validate_schema(
            {"id": "int64", "label": "string", "amount": "int64"}
        )
        quality_summary = workflow.data_quality_summary()
        quality_checks = workflow.data_quality_check(
            "not_null:id",
            "not_null:label",
            "unique:id",
            "unique:label",
        )

        self.assertIsInstance(schema, sl.WorkflowSchemaReport)
        self.assertEqual(schema.schema_map, {"id": "int64", "label": "utf8", "amount": "int64"})
        self.assertEqual(schema.field("label").null_count, 1)
        self.assertEqual(schema.nullable_fields, ("label",))
        self.assertFalse(schema.fallback_attempted)
        self.assertFalse(schema.external_engine_invoked)
        self.assertEqual(described.field_names, ("id", "label", "amount"))
        self.assertTrue(validation.valid)
        self.assertEqual(validation.dtype_mismatches, ())
        self.assertEqual(quality_summary.null_counts, {"id": 0, "label": 1, "amount": 0})
        self.assertEqual(quality_summary.row_count, 3)
        self.assertFalse(quality_checks.passed)
        by_check = {result.check: result for result in quality_checks.checks}
        self.assertTrue(by_check["not_null:id"].passed)
        self.assertFalse(by_check["not_null:label"].passed)
        self.assertEqual(by_check["not_null:label"].failing_row_count, 1)
        self.assertTrue(by_check["unique:id"].passed)
        self.assertFalse(by_check["unique:label"].passed)
        self.assertEqual(by_check["unique:label"].failing_row_count, 1)

    def test_local_parquet_query_builder_collect_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.parquet' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_format", "value": "parquet"},
                        {"key": "source_adapter_id", "value": "local_parquet_input_adapter"},
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_parquet("target/input.parquet")
            .select("id", "label")
            .filter("amount >= 10")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("source_adapter_id"), "local_parquet_input_adapter")
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_arrow_ipc_query_builder_collect_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.arrow' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_format", "value": "arrow_ipc"},
                        {"key": "source_adapter_id", "value": "local_arrow_ipc_input_adapter"},
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_arrow_ipc("target/input.arrow")
            .select("id", "label")
            .filter("amount >= 10")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("source_adapter_id"),
            "local_arrow_ipc_input_adapter",
        )
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_avro_query_builder_collect_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.avro' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_format", "value": "avro"},
                        {"key": "source_adapter_id", "value": "local_avro_input_adapter"},
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_avro("target/input.avro")
            .select("id", "label")
            .filter("amount >= 10")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("source_adapter_id"), "local_avro_input_adapter")
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_orc_query_builder_collect_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.orc' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_format", "value": "orc"},
                        {"key": "source_adapter_id", "value": "local_orc_input_adapter"},
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_orc("target/input.orc")
            .select("id", "label")
            .filter("amount >= 10")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("source_adapter_id"), "local_orc_input_adapter")
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_column_expression_builder_lowers_to_sql_filter_predicates(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE ((amount >= 10 AND label LIKE '%ta%') OR closed_at IS NULL) LIMIT 5",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "string_predicate_runtime_execution", "value": "true"},
                        {"key": "null_predicate_runtime_execution", "value": "true"},
                        {"key": "null_predicate_operator", "value": "is_null"},
                        {"key": "null_predicate_source_column", "value": "closed_at"},
                        {"key": "null_predicate_null_semantics", "value": "sql_is_null_is_not_null"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))
        predicate = ((sl.col("amount") >= 10) & sl.col("label").contains("ta")) | sl.col(
            "closed_at"
        ).is_null()

        report = (
            ctx.read_csv("target/input.csv")
            .filter(predicate)
            .select("id", "label")
            .limit(5)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")
        self.assertIsInstance(report.claim_summary, sl.ClaimSummary)
        self.assertEqual(report.evidence_summary.command, "sql-local-source-smoke")
        self.assertEqual(report.evidence_summary.output_row_count, 1)
        self.assertFalse(report.claim_summary.fallback_attempted)
        self.assertFalse(report.claim_summary.external_engine_invoked)
        self.assertFalse(report.claim_summary.public_performance_claim_allowed)
        self.assertTrue(report.null_predicate_runtime_execution)
        self.assertEqual(report.null_predicate_operator, ("is_null",))
        self.assertEqual(report.null_predicate_source_columns, ("closed_at",))
        self.assertEqual(
            report.null_predicate_null_semantics,
            "sql_is_null_is_not_null",
        )

    def test_column_expression_builder_lowers_boolean_predicates(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id FROM 'target/input.csv' WHERE active IS TRUE LIMIT 5",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "boolean_predicate"},
                        {"key": "boolean_predicate_runtime_execution", "value": "true"},
                        {"key": "boolean_predicate_operator", "value": "is_true"},
                        {"key": "boolean_predicate_source_column", "value": "active"},
                        {"key": "boolean_predicate_null_semantics", "value": "sql_where_true_only_null_filters_out"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .filter(sl.col("active").is_true())
            .select("id")
            .limit(5)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "boolean_predicate")
        self.assertTrue(report.boolean_predicate_runtime_execution)
        self.assertEqual(report.boolean_predicate_operator, ("is_true",))
        self.assertEqual(report.boolean_predicate_source_columns, ("active",))
        self.assertEqual(
            report.boolean_predicate_null_semantics,
            "sql_where_true_only_null_filters_out",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_column_expression_builder_lowers_is_not_boolean_predicates(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id FROM 'target/input.csv' WHERE active IS NOT TRUE LIMIT 5",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2}\\n{\\"id\\":3}\\n"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "predicate_operator_family", "value": "boolean_predicate"},
                        {"key": "boolean_predicate_runtime_execution", "value": "true"},
                        {"key": "boolean_predicate_operator", "value": "is_not_true"},
                        {"key": "boolean_predicate_source_column", "value": "active"},
                        {"key": "boolean_predicate_null_semantics", "value": "sql_boolean_is_not_true_false_null_matches"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .filter(sl.col("active").is_not_true())
            .select("id")
            .limit(5)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "boolean_predicate")
        self.assertTrue(report.boolean_predicate_runtime_execution)
        self.assertEqual(report.boolean_predicate_operator, ("is_not_true",))
        self.assertEqual(report.boolean_predicate_source_columns, ("active",))
        self.assertEqual(
            report.boolean_predicate_null_semantics,
            "sql_boolean_is_not_true_false_null_matches",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_column_expression_builder_formats_admitted_predicate_families(self) -> None:
        self.assertEqual(
            str(sl.col("event_dt").cast("date32") >= date(2026, 5, 19)),
            "CAST(event_dt AS date32) >= DATE '2026-05-19'",
        )
        self.assertEqual(
            str(sl.col("raw_amount").try_cast("int64") >= 10),
            "TRY_CAST(raw_amount AS int64) >= 10",
        )
        self.assertEqual(
            str(sl.try_cast(sl.col("raw_amount"), "int64") == 42),
            "TRY_CAST(raw_amount AS int64) = 42",
        )
        self.assertEqual(
            str(sl.col("label").isin(["alpha", "gamma"])),
            "label IN ('alpha','gamma')",
        )
        self.assertEqual(
            str(sl.col("label").isin("alpha", None)),
            "label IN ('alpha',NULL)",
        )
        self.assertEqual(
            str(sl.col("label").not_in(["alpha", "gamma"])),
            "label NOT IN ('alpha','gamma')",
        )
        self.assertEqual(
            str(sl.col("id").isin_source("target/allowed.csv", "id")),
            "id IN (SELECT id FROM 'target/allowed.csv')",
        )
        self.assertEqual(
            str(sl.col("id").not_in_source("target/blocked.csv", "id")),
            "id NOT IN (SELECT id FROM 'target/blocked.csv')",
        )
        self.assertEqual(
            str(sl.col("amount").between(10, 20)),
            "(amount >= 10 AND amount <= 20)",
        )
        self.assertEqual(
            str(
                sl.col("event_dt")
                .cast("date32")
                .between(date(2026, 5, 1), date(2026, 5, 31))
            ),
            "(CAST(event_dt AS date32) >= DATE '2026-05-01' AND CAST(event_dt AS date32) <= DATE '2026-05-31')",
        )
        self.assertEqual(
            str(sl.col("event_dt").date_add_days(7) >= date(2026, 5, 26)),
            "DATE_ADD_DAYS(event_dt, 7) >= DATE '2026-05-26'",
        )
        self.assertEqual(
            str(sl.col("event_dt").date_sub_days("1") == date(2026, 5, 18)),
            "DATE_SUB_DAYS(event_dt, 1) = DATE '2026-05-18'",
        )
        self.assertEqual(
            str(sl.col("event_dt").cast("date32").date_add_days(-2) < date(2026, 5, 20)),
            "DATE_ADD_DAYS(CAST(event_dt AS date32), -2) < DATE '2026-05-20'",
        )
        self.assertEqual(
            str(
                sl.col("event_ts").timestamp_add_seconds(60)
                >= datetime(2026, 5, 19, 12, 35, 45, tzinfo=timezone.utc)
            ),
            "TIMESTAMP_ADD_SECONDS(event_ts, 60) >= TIMESTAMP '2026-05-19T12:35:45Z'",
        )
        self.assertEqual(
            str(
                sl.col("event_ts")
                .cast("timestamp")
                .timestamp_sub_seconds("45")
                < datetime(2026, 5, 19, 12, 34, 30, tzinfo=timezone.utc)
            ),
            "TIMESTAMP_SUB_SECONDS(CAST(event_ts AS timestamp_micros), 45) < TIMESTAMP '2026-05-19T12:34:30Z'",
        )
        self.assertEqual(
            str(sl.col("end_date").date_diff_days(sl.col("start_date")) >= 2),
            "DATE_DIFF_DAYS(end_date, start_date) >= 2",
        )
        self.assertEqual(
            str(
                sl.col("event_end")
                .cast("timestamp")
                .timestamp_diff_seconds(sl.col("event_ts").cast("timestamp"))
                >= 120
            ),
            "TIMESTAMP_DIFF_SECONDS(CAST(event_end AS timestamp_micros), CAST(event_ts AS timestamp_micros)) >= 120",
        )
        self.assertEqual(
            str(sl.col("event_dt").date_year() == 2026),
            "DATE_YEAR(event_dt) = 2026",
        )
        self.assertEqual(
            str(sl.col("event_dt").cast("date32").date_month() == 5),
            "DATE_MONTH(CAST(event_dt AS date32)) = 5",
        )
        self.assertEqual(
            str(sl.col("event_dt").date_day() >= 19),
            "DATE_DAY(event_dt) >= 19",
        )
        self.assertEqual(
            str(sl.col("event_ts").cast("timestamp").timestamp_hour() == 12),
            "TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) = 12",
        )
        self.assertEqual(
            str(
                sl.col("event_ts")
                >= datetime(2026, 5, 19, 12, 30, 45, 123456, tzinfo=timezone.utc)
            ),
            "event_ts >= TIMESTAMP '2026-05-19T12:30:45.123456Z'",
        )
        self.assertEqual(
            str(sl.col("event_ts").timestamp_second() == 45),
            "TIMESTAMP_SECOND(event_ts) = 45",
        )
        self.assertEqual(str(sl.col("f.amount") >= 10), "f.amount >= 10")
        self.assertEqual(str(sl.col("label").startswith("al")), "label LIKE 'al%'")
        self.assertEqual(str(sl.col("label").endswith("ta")), "label LIKE '%ta'")
        self.assertEqual(str(sl.col("label").not_like("%tmp%")), "label NOT LIKE '%tmp%'")
        self.assertEqual(str(sl.col("label").not_contains("tmp")), "label NOT LIKE '%tmp%'")
        self.assertEqual(str(sl.col("label").not_startswith("tmp")), "label NOT LIKE 'tmp%'")
        self.assertEqual(str(sl.col("label").not_endswith("tmp")), "label NOT LIKE '%tmp'")
        self.assertEqual(str(sl.col("label").lower() == "alpha"), "LOWER(label) = 'alpha'")
        self.assertEqual(str(sl.col("label").upper() != "BETA"), "UPPER(label) != 'BETA'")
        self.assertEqual(str(sl.col("label").trim() == "gamma"), "TRIM(label) = 'gamma'")
        self.assertEqual(
            str(sl.concat(sl.col("label"), "-", sl.col("segment")) == "alpha-north"),
            "CONCAT(label, '-', segment) = 'alpha-north'",
        )
        self.assertEqual(
            str(sl.col("label").substr(2, 3) == "lph"),
            "SUBSTR(label, 2, 3) = 'lph'",
        )
        self.assertEqual(
            str(sl.substring(sl.col("label"), "1", 2) == "al"),
            "SUBSTR(label, 1, 2) = 'al'",
        )
        self.assertEqual(str(sl.col("label").left(2) == "al"), "LEFT(label, 2) = 'al'")
        self.assertEqual(
            str(sl.right(sl.col("label"), "2") == "ha"), "RIGHT(label, 2) = 'ha'"
        )
        self.assertEqual(
            str(sl.col("label").replace(" ", "_") == "alpha_beta"),
            "REPLACE(label, ' ', '_') = 'alpha_beta'",
        )
        self.assertEqual(str(sl.col("amount") + 5 >= 20), "amount + 5 >= 20")
        self.assertEqual(str(sl.col("amount") - 3 < 10), "amount - 3 < 10")
        self.assertEqual(str(sl.col("amount") * 2 == 40), "amount * 2 = 40")
        self.assertEqual(str(sl.col("ratio") / 2.0 > 0.5), "ratio / 2.0 > 0.5")
        self.assertEqual(str(sl.col("closed_at").is_not_null()), "closed_at IS NOT NULL")
        self.assertEqual(str(sl.col("active").is_true()), "active IS TRUE")
        self.assertEqual(str(sl.col("active").is_false()), "active IS FALSE")
        self.assertEqual(str(sl.col("active").is_not_true()), "active IS NOT TRUE")
        self.assertEqual(str(sl.col("active").is_not_false()), "active IS NOT FALSE")

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            sl.col("event_dt") >= datetime(2026, 5, 19, 12, 30)
        with self.assertRaises(ValueError):
            sl.col("label").contains("%")
        with self.assertRaises(ValueError):
            sl.col("label").isin([])
        with self.assertRaises(ValueError):
            sl.col("id").isin_source("target/allowed.csv", "bad column")
        with self.assertRaises(ValueError):
            sl.col("id").isin_source("target/has'quote.csv", "id")
        with self.assertRaises(ValueError):
            sl.col("amount").between(None, 10)
        with self.assertRaises(ValueError):
            sl.col("bad column")
        with self.assertRaises(ValueError):
            sl.col("amount>=10")
        with self.assertRaises(ValueError):
            sl.col("too.many.parts")
        with self.assertRaises(ValueError):
            sl.col("event_dt").date_add_days(True)
        with self.assertRaises(ValueError):
            sl.col("event_dt").date_add_days("1 day")
        with self.assertRaises(ValueError):
            sl.col("event_dt").date_add_days(366_001)
        with self.assertRaises(ValueError):
            sl.col("event_ts").timestamp_add_seconds(True)
        with self.assertRaises(ValueError):
            sl.col("event_ts").timestamp_add_seconds("1 minute")
        with self.assertRaises(ValueError):
            sl.col("event_ts").timestamp_add_seconds(31_622_400_001)
        with self.assertRaises(TypeError):
            sl.col("event_dt").date_diff_days(
                datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
            )
        with self.assertRaises(TypeError):
            sl.col("event_ts").timestamp_diff_seconds(date(2026, 5, 19))
        with self.assertRaises(ValueError):
            sl.col("amount") + True
        with self.assertRaises(TypeError):
            sl.col("amount") * "2"
        with self.assertRaisesRegex(ValueError, "at least one shardloom column"):
            sl.concat("alpha", "beta")
        with self.assertRaisesRegex(ValueError, "bare column names"):
            sl.concat(sl.col("label").lower(), "x")
        with self.assertRaisesRegex(ValueError, "substring start"):
            sl.col("label").substr(0, 2)
        with self.assertRaisesRegex(ValueError, "left count"):
            sl.col("label").left(-1)
        with self.assertRaisesRegex(TypeError, "right requires"):
            sl.right("label", 2)
        with self.assertRaisesRegex(ValueError, "replace search literal"):
            sl.col("label").replace("", "x")

    def test_column_expression_builder_exposes_date_extract_report_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,event_date FROM 'target/input.csv' WHERE (DATE_YEAR(event_date) = 2026 AND DATE_MONTH(event_date) = 5) LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"event_date\\":\\"2026-05-19\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "date_extract_runtime_execution", "value": "true"},
                        {"key": "date_extract_operator", "value": "date_year,date_month"},
                        {"key": "date_extract_source_column", "value": "event_date,event_date"},
                        {"key": "date_arithmetic_runtime_execution", "value": "false"},
                        {"key": "date_arithmetic_operator", "value": "not_applicable"},
                        {"key": "date_arithmetic_days", "value": "not_applicable"},
                        {"key": "date_arithmetic_source_column", "value": "not_applicable"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "event_date")
            .where(
                (sl.col("event_date").date_year() == 2026)
                & (sl.col("event_date").date_month() == 5)
            )
            .limit(10)
            .collect()
        )

        self.assertTrue(report.date_extract_runtime_execution)
        self.assertEqual(report.date_extract_operator, ("date_year", "date_month"))
        self.assertEqual(report.date_extract_source_columns, ("event_date", "event_date"))
        self.assertFalse(report.date_arithmetic_runtime_execution)
        self.assertEqual(report.date_arithmetic_operator, ())
        self.assertEqual(report.date_arithmetic_days, ())
        self.assertEqual(report.date_arithmetic_source_columns, ())

    def test_column_expression_builder_exposes_timestamp_report_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,event_ts FROM 'target/input.csv' WHERE (event_ts >= TIMESTAMP '2026-05-19T12:00:00Z' AND TIMESTAMP_HOUR(event_ts) = 12) LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"event_ts\\":\\"2026-05-19T12:30:45Z\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "timestamp_literal_runtime_execution", "value": "true"},
                        {"key": "timestamp_extract_runtime_execution", "value": "true"},
                        {"key": "timestamp_extract_operator", "value": "timestamp_hour"},
                        {"key": "timestamp_extract_source_column", "value": "event_ts"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "event_ts")
            .where(
                (sl.col("event_ts") >= datetime(2026, 5, 19, 12, tzinfo=timezone.utc))
                & (sl.col("event_ts").timestamp_hour() == 12)
            )
            .limit(10)
            .collect()
        )

        self.assertTrue(report.timestamp_literal_runtime_execution)
        self.assertTrue(report.timestamp_extract_runtime_execution)
        self.assertEqual(report.timestamp_extract_operator, ("timestamp_hour",))
        self.assertEqual(report.timestamp_extract_source_columns, ("event_ts",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_column_expression_builder_exposes_timestamp_arithmetic_report_fields(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,event_ts FROM 'target/input.csv' WHERE TIMESTAMP_ADD_SECONDS(event_ts, 60) >= TIMESTAMP '2026-05-19T12:35:45Z' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"event_ts\\":\\"2026-05-19T12:34:45Z\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "timestamp_arithmetic"},
                        {"key": "timestamp_literal_runtime_execution", "value": "true"},
                        {"key": "timestamp_arithmetic_runtime_execution", "value": "true"},
                        {"key": "timestamp_arithmetic_operator", "value": "timestamp_add_seconds"},
                        {"key": "timestamp_arithmetic_seconds", "value": "60"},
                        {"key": "timestamp_arithmetic_source_column", "value": "event_ts"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "event_ts")
            .where(
                sl.col("event_ts").timestamp_add_seconds(60)
                >= datetime(2026, 5, 19, 12, 35, 45, tzinfo=timezone.utc)
            )
            .limit(10)
            .collect()
        )

        self.assertTrue(report.timestamp_literal_runtime_execution)
        self.assertTrue(report.timestamp_arithmetic_runtime_execution)
        self.assertEqual(
            report.timestamp_arithmetic_operator,
            ("timestamp_add_seconds",),
        )
        self.assertEqual(report.timestamp_arithmetic_seconds, ("60",))
        self.assertEqual(report.timestamp_arithmetic_source_columns, ("event_ts",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_column_expression_builder_exposes_string_transform_report_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE LOWER(label) = 'alpha' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"Alpha\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "string_transform"},
                        {"key": "string_transform_runtime_execution", "value": "true"},
                        {"key": "string_transform_operator", "value": "lower"},
                        {"key": "string_transform_source_column", "value": "label"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter(sl.col("label").lower() == "alpha")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "string_transform")
        self.assertTrue(report.string_transform_runtime_execution)
        self.assertEqual(report.string_transform_operator, ("lower",))
        self.assertEqual(report.string_transform_source_columns, ("label",))
        self.assertEqual(report.result_jsonl, '{"id":1,"label":"Alpha"}\n')
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_column_expression_builder_exposes_string_function_report_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "string_function"},
                        {"key": "string_function_runtime_execution", "value": "true"},
                        {"key": "string_function_operator", "value": "concat"},
                        {"key": "string_function_source_column", "value": "label+segment"},
                        {"key": "string_function_literal_count", "value": "2"},
                        {"key": "string_function_rhs_dtype", "value": "utf8"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter(sl.concat(sl.col("label"), "-", sl.col("segment")) == "alpha-north")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "string_function")
        self.assertTrue(report.string_function_runtime_execution)
        self.assertEqual(report.string_function_operator, ("concat",))
        self.assertEqual(report.string_function_source_columns, ("label+segment",))
        self.assertEqual(report.string_function_literal_counts, (2,))
        self.assertEqual(report.string_function_rhs_dtypes, ("utf8",))
        self.assertEqual(report.result_jsonl, '{"id":1,"label":"alpha"}\n')
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_where_between_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE (amount >= 10 AND amount <= 20) LIMIT 5",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "logical_predicate_operator", "value": "and"},
                        {"key": "logical_predicate_leaf_count", "value": "2"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .where(sl.col("amount").between(10, 20))
            .select("id", "label")
            .limit(5)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertEqual(report.logical_predicate_operator, "and")
        self.assertEqual(report.logical_predicate_leaf_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_where_negated_predicates_invoke_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE (label NOT IN ('alpha','gamma') AND label NOT LIKE '%lt%') LIMIT 5",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "logical_predicate_operator", "value": "and"},
                        {"key": "logical_predicate_leaf_count", "value": "2"},
                        {"key": "in_predicate_runtime_execution", "value": "true"},
                        {"key": "in_list_value_count", "value": "2"},
                        {"key": "string_predicate_runtime_execution", "value": "true"},
                        {"key": "string_predicate_operator", "value": "contains"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .where(sl.col("label").not_in(["alpha", "gamma"]) & sl.col("label").not_contains("lt"))
            .select("id", "label")
            .limit(5)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.result_rows, ({"id": 2, "label": "beta"},))
        self.assertEqual(report.logical_predicate_operator, "and")
        self.assertEqual(report.logical_predicate_leaf_count, 2)
        self.assertTrue(report.in_predicate_runtime_execution)
        self.assertTrue(report.string_predicate_runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_projection_limit_without_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_projection_limit"},
                        {"key": "filter_runtime_execution", "value": "false"},
                        {"key": "predicate_operator_family", "value": "none"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.projection-limit.execution.v1"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.read_csv("target/input.csv").select("id", "label").limit(2).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("sql_statement_kind"), "local_source_projection_limit")
        self.assertFalse(report.filter_runtime_execution)
        self.assertEqual(report.predicate_operator_family, "none")
        self.assertEqual(report.selected_row_count, 3)
        self.assertEqual(
            report.envelope.field("execution_certificate_ref"),
            "sql-local-source.csv.projection-limit.execution.v1",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_preview_uses_select_star_limit(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT * FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_projection_limit"},
                        {"key": "filter_runtime_execution", "value": "false"},
                        {"key": "predicate_operator_family", "value": "none"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.read_csv("target/input.csv").preview(limit=2)
        head_report = ctx.read_csv("target/input.csv").head(limit=2)
        take_report = ctx.read_csv("target/input.csv").take(2)

        for preview_report in (report, head_report, take_report):
            self.assertEqual(preview_report.envelope.command, "sql-local-source-smoke")
            self.assertEqual(
                preview_report.envelope.field("sql_statement_kind"),
                "local_source_projection_limit",
            )
            self.assertEqual(preview_report.output_row_count, 2)
            self.assertFalse(preview_report.fallback_attempted)
            self.assertFalse(preview_report.external_engine_invoked)

    def test_local_csv_query_builder_logical_and_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND label LIKE '%ta' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "logical_predicate_operator", "value": "and"},
                        {"key": "logical_predicate_leaf_count", "value": "2"},
                        {"key": "string_predicate_runtime_execution", "value": "true"},
                        {"key": "string_predicate_operator", "value": "ends_with"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "1"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10 AND label LIKE '%ta'")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "logical_predicate")
        self.assertTrue(report.logical_predicate_runtime_execution)
        self.assertEqual(report.logical_predicate_operator, "and")
        self.assertEqual(report.logical_predicate_leaf_count, 2)
        self.assertTrue(report.string_predicate_runtime_execution)
        self.assertEqual(report.string_predicate_operator, ("ends_with",))
        self.assertEqual(report.result_jsonl, '{"id":2,"label":"beta"}\n')
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_logical_or_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 OR label LIKE '%ta' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n{\\"id\\":3,\\"label\\":\\"delta\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "logical_predicate_operator", "value": "or"},
                        {"key": "logical_predicate_leaf_count", "value": "2"},
                        {"key": "string_predicate_runtime_execution", "value": "true"},
                        {"key": "string_predicate_operator", "value": "ends_with"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10 OR label LIKE '%ta'")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "logical_predicate")
        self.assertTrue(report.logical_predicate_runtime_execution)
        self.assertEqual(report.logical_predicate_operator, "or")
        self.assertEqual(report.logical_predicate_leaf_count, 2)
        self.assertTrue(report.string_predicate_runtime_execution)
        self.assertEqual(report.string_predicate_operator, ("ends_with",))
        self.assertEqual(
            report.result_jsonl,
            '{"id":2,"label":"beta"}\n{"id":3,"label":"delta"}\n',
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_parenthesized_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%') LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n{\\"id\\":3,\\"label\\":\\"gamma\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "logical_predicate_operator", "value": "and"},
                        {"key": "logical_predicate_leaf_count", "value": "3"},
                        {"key": "string_predicate_runtime_execution", "value": "true"},
                        {"key": "string_predicate_operator", "value": "ends_with,starts_with"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%')")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "logical_predicate")
        self.assertTrue(report.logical_predicate_runtime_execution)
        self.assertEqual(report.logical_predicate_operator, "and")
        self.assertEqual(report.logical_predicate_leaf_count, 3)
        self.assertTrue(report.string_predicate_runtime_execution)
        self.assertEqual(report.string_predicate_operator, ("ends_with", "starts_with"))
        self.assertEqual(
            report.result_jsonl,
            '{"id":2,"label":"beta"}\n{"id":3,"label":"gamma"}\n',
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_logical_not_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE NOT label LIKE '%ta' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "logical_predicate"},
                        {"key": "logical_predicate_runtime_execution", "value": "true"},
                        {"key": "logical_predicate_operator", "value": "not"},
                        {"key": "logical_predicate_leaf_count", "value": "1"},
                        {"key": "string_predicate_runtime_execution", "value": "true"},
                        {"key": "string_predicate_operator", "value": "ends_with"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "1"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("NOT label LIKE '%ta'")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "logical_predicate")
        self.assertTrue(report.logical_predicate_runtime_execution)
        self.assertEqual(report.logical_predicate_operator, "not")
        self.assertEqual(report.logical_predicate_leaf_count, 1)
        self.assertTrue(report.string_predicate_runtime_execution)
        self.assertEqual(report.string_predicate_operator, ("ends_with",))
        self.assertEqual(report.result_jsonl, '{"id":1,"label":"alpha"}\n')
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_in_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE label IN ('alpha','gamma') LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":3,\\"label\\":\\"gamma\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "in_predicate"},
                        {"key": "in_predicate_runtime_execution", "value": "true"},
                        {"key": "in_list_value_count", "value": "2"},
                        {"key": "in_list_null_value_count", "value": "0"},
                        {"key": "in_predicate_null_semantics", "value": "not_applicable"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("label IN ('alpha','gamma')")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "in_predicate")
        self.assertTrue(report.in_predicate_runtime_execution)
        self.assertEqual(report.in_list_value_count, 2)
        self.assertEqual(report.in_list_null_value_count, 0)
        self.assertEqual(report.in_predicate_null_semantics, "not_applicable")
        self.assertEqual(
            report.result_jsonl,
            '{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_null_aware_in_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE label IN ('alpha',NULL) LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "in_predicate"},
                        {"key": "in_predicate_runtime_execution", "value": "true"},
                        {"key": "in_list_value_count", "value": "2"},
                        {"key": "in_list_null_value_count", "value": "1"},
                        {"key": "in_predicate_null_semantics", "value": "sql_three_valued_where_filter"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "1"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter(sl.col("label").isin("alpha", None))
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "in_predicate")
        self.assertTrue(report.in_predicate_runtime_execution)
        self.assertEqual(report.in_list_value_count, 2)
        self.assertEqual(report.in_list_null_value_count, 1)
        self.assertEqual(
            report.in_predicate_null_semantics, "sql_three_valued_where_filter"
        )
        self.assertEqual(report.result_jsonl, '{"id":1,"label":"alpha"}\n')
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_in_subquery_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv') LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":3,\\"label\\":\\"gamma\\"}\\n"},
                        {"key": "predicate_operator_family", "value": "in_subquery"},
                        {"key": "in_predicate_runtime_execution", "value": "true"},
                        {"key": "in_list_value_count", "value": "3"},
                        {"key": "in_list_null_value_count", "value": "1"},
                        {"key": "in_subquery_runtime_execution", "value": "true"},
                        {"key": "in_subquery_source_column", "value": "id"},
                        {"key": "in_subquery_source_format", "value": "csv"},
                        {"key": "in_subquery_materialized_value_count", "value": "3"},
                        {"key": "in_subquery_materialized_null_value_count", "value": "1"},
                        {"key": "in_predicate_null_semantics", "value": "sql_three_valued_where_filter"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))
        allowed = ctx.read_csv("target/allowed.csv")

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter(sl.col("id").isin_source(allowed, "id"))
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "in_subquery")
        self.assertTrue(report.in_predicate_runtime_execution)
        self.assertEqual(report.in_list_value_count, 3)
        self.assertEqual(report.in_list_null_value_count, 1)
        self.assertTrue(report.in_subquery_runtime_execution)
        self.assertEqual(report.in_subquery_source_columns, ("id",))
        self.assertEqual(report.in_subquery_source_formats, ("csv",))
        self.assertEqual(report.in_subquery_materialized_value_count, 3)
        self.assertEqual(report.in_subquery_materialized_null_value_count, 1)
        self.assertEqual(
            report.in_predicate_null_semantics, "sql_three_valued_where_filter"
        )
        self.assertEqual(
            report.result_jsonl,
            '{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_scalar_aggregate_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"count_all\\":2,\\"sum_amount\\":36,\\"avg_amount\\":18.0,\\"min_amount\\":15,\\"max_amount\\":21}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "scalar_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount),avg(amount),min(amount),max(amount)"},
                        {"key": "projected_columns", "value": "count_all,sum_amount,avg_amount,min_amount,max_amount"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        aggregate_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .aggregate("count(*)", "sum(amount)", "avg(amount)", "min(amount)", "max(amount)")
        )
        self.assertIsInstance(aggregate_workflow, sl.LazyFrame)
        report = aggregate_workflow.limit(1).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"count_all":2,"sum_amount":36,"avg_amount":18.0,"min_amount":15,"max_amount":21}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(
            report.aggregate_functions,
            ("count(*)", "sum(amount)", "avg(amount)", "min(amount)", "max(amount)"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_named_scalar_aggregate_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"rows\\":2,\\"total_amount\\":36}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "scalar_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "projected_columns", "value": "rows,total_amount"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        aggregate_workflow = ctx.read_csv("target/input.csv").filter(
            "amount >= 10"
        ).agg(rows="count(*)", total_amount="sum(amount)")
        self.assertIsInstance(aggregate_workflow, sl.LazyFrame)
        report = aggregate_workflow.limit(1).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.result_jsonl, '{"rows":2,"total_amount":36}\n')
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.aggregate_alias_runtime_execution)
        self.assertEqual(report.aggregate_aliases, ("rows", "total_amount"))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_scalar_aggregate_order_by_topn_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 ORDER BY total_amount DESC,rows DESC LIMIT 1",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"rows\\":2,\\"total_amount\\":36}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_aggregate_order_by_topn_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "scalar_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "multi_key_scalar_topn"},
                        {"key": "sort_keys", "value": "total_amount,rows"},
                        {"key": "sort_direction", "value": "desc,desc"},
                        {"key": "top_n_limit", "value": "1"},
                        {"key": "projected_columns", "value": "rows,total_amount"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.aggregate-order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        aggregate_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .agg(rows="count(*)", total_amount="sum(amount)")
        )
        self.assertIsInstance(aggregate_workflow, sl.LazyFrame)
        report = (
            aggregate_workflow.sort("total_amount", "rows", descending=True)
            .limit(1)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.result_jsonl, '{"rows":2,"total_amount":36}\n')
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("total_amount", "rows"))
        self.assertEqual(report.sort_direction, "desc,desc")
        self.assertEqual(report.top_n_limit, 1)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_count_invokes_scalar_aggregate_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT count(*) FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source count",
                    "human_text": "sql local source count",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"count_all\\":2}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "scalar_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*)"},
                        {"key": "projected_columns", "value": "count_all"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.read_csv("target/input.csv").filter(sl.col("amount") >= 10).count()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.result_jsonl, '{"count_all":2}\n')
        self.assertEqual(report.first_result_row, {"count_all": 2})
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_group_by_aggregate_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,count(*),sum(amount) FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"count_all\\":2,\\"sum_amount\\":36}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region"},
                        {"key": "group_by_key_arity", "value": "1"},
                        {"key": "group_by_multi_key_runtime_execution", "value": "false"},
                        {"key": "group_by_group_count", "value": "1"},
                        {"key": "projected_columns", "value": "region,count_all,sum_amount"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.group-by-aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .group_by("region")
            .agg("count(*)", "sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        report = grouped_workflow.limit(10).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","count_all":2,"sum_amount":36}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)"))
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("region",))
        self.assertEqual(report.group_by_key_arity, 1)
        self.assertFalse(report.group_by_multi_key_runtime_execution)
        self.assertEqual(report.group_by_group_count, 1)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_group_by_aggregate_order_by_topn_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region ORDER BY total_amount DESC,rows DESC LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"rows\\":2,\\"total_amount\\":36}\\n{\\"region\\":\\"west\\",\\"rows\\":1,\\"total_amount\\":15}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_order_by_topn_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region"},
                        {"key": "group_by_key_arity", "value": "1"},
                        {"key": "group_by_group_count", "value": "2"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "multi_key_scalar_topn"},
                        {"key": "sort_keys", "value": "total_amount,rows"},
                        {"key": "sort_direction", "value": "desc,desc"},
                        {"key": "top_n_limit", "value": "2"},
                        {"key": "projected_columns", "value": "region,rows,total_amount"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.group-by-aggregate-order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .group_by("region")
            .agg(rows="count(*)", total_amount="sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        sorted_workflow = grouped_workflow.sort("total_amount", "rows", descending=True)
        self.assertIsInstance(sorted_workflow, sl.LazyFrame)
        report = sorted_workflow.limit(2).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","rows":2,"total_amount":36}\n'
            '{"region":"west","rows":1,"total_amount":15}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("region",))
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("total_amount", "rows"))
        self.assertEqual(report.sort_direction, "desc,desc")
        self.assertEqual(report.top_n_limit, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_aggregate_having_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region HAVING (total_amount >= 10 AND rows >= 2) ORDER BY total_amount DESC LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"rows\\":2,\\"total_amount\\":22}\\n{\\"region\\":\\"west\\",\\"rows\\":2,\\"total_amount\\":19}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_order_by_topn_filter_limit_having"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region"},
                        {"key": "having_runtime_execution", "value": "true"},
                        {"key": "having_operator_family", "value": "logical_predicate"},
                        {"key": "having_source_column", "value": "total_amount,rows"},
                        {"key": "having_input_row_count", "value": "3"},
                        {"key": "having_selected_row_count", "value": "2"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "sort_keys", "value": "total_amount"},
                        {"key": "sort_direction", "value": "desc"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "5"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.group-by-aggregate-order-by-topn-filter-limit-having.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter(sl.col("amount") >= 0)
            .group_by("region")
            .agg(rows="count(*)", total_amount="sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        having_workflow = grouped_workflow.filter(
            (sl.col("total_amount") >= 10) & (sl.col("rows") >= 2)
        )
        self.assertIsInstance(having_workflow, sl.LazyFrame)
        self.assertIn(
            "having((total_amount >= 10 AND rows >= 2))",
            having_workflow.operation_summary,
        )
        report = having_workflow.sort("total_amount", descending=True).limit(10).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","rows":2,"total_amount":22}\n'
            '{"region":"west","rows":2,"total_amount":19}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertTrue(report.group_by_runtime_execution)
        self.assertTrue(report.having_runtime_execution)
        self.assertEqual(report.having_operator_family, "logical_predicate")
        self.assertEqual(report.having_source_columns, ("total_amount", "rows"))
        self.assertEqual(report.having_input_row_count, 3)
        self.assertEqual(report.having_selected_row_count, 2)
        self.assertTrue(report.order_by_runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_filter_after_having_stays_unsupported(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "workflow-unsupported-plan",
                    "collect",
                    "read_csv(target/input.csv) -> group_by(region) -> aggregate(count(*) AS rows,sum(amount) AS total_amount) -> having(rows >= 2) -> filter(region = 'east') -> limit(10)",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "workflow operation unsupported",
                    "human_text": "workflow unsupported operation",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "operation", "value": "collect"},
                        {"key": "workflow_summary", "value": "read_csv(target/input.csv) -> group_by(region) -> aggregate(count(*) AS rows,sum(amount) AS total_amount) -> having(rows >= 2) -> filter(region = 'east') -> limit(10)"},
                        {"key": "blocker_id", "value": "cg21.workflow.collect.runtime_not_admitted"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        workflow = (
            ctx.read_csv("target/input.csv")
            .group_by("region")
            .agg(rows="count(*)", total_amount="sum(amount)")
            .filter("rows >= 2")
            .filter("region = 'east'")
            .limit(10)
        )
        report = workflow.collect()

        self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
        self.assertEqual(report.operation, "collect")
        self.assertFalse(report.runtime_execution)
        self.assertFalse(report.fallback_attempted)

    def test_local_csv_query_builder_filter_after_aggregate_topn_stays_unsupported(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "workflow-unsupported-plan",
                    "collect",
                    "read_csv(target/input.csv) -> group_by(region) -> aggregate(count(*) AS rows) -> sort(asc,rows) -> limit(10) -> filter(rows >= 2)",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "workflow operation unsupported",
                    "human_text": "workflow unsupported operation",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "operation", "value": "collect"},
                        {"key": "workflow_summary", "value": "read_csv(target/input.csv) -> group_by(region) -> aggregate(count(*) AS rows) -> sort(asc,rows) -> limit(10) -> filter(rows >= 2)"},
                        {"key": "blocker_id", "value": "cg21.workflow.collect.runtime_not_admitted"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        workflow = (
            ctx.read_csv("target/input.csv")
            .group_by("region")
            .agg(rows="count(*)")
            .sort("rows")
            .limit(10)
            .filter("rows >= 2")
        )
        report = workflow.collect()

        self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
        self.assertEqual(report.operation, "collect")
        self.assertFalse(report.runtime_execution)
        self.assertFalse(report.fallback_attempted)

    def test_local_csv_query_builder_group_key_order_by_topn_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region ORDER BY region ASC,total_amount ASC LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"rows\\":2,\\"total_amount\\":36}\\n{\\"region\\":\\"north\\",\\"rows\\":1,\\"total_amount\\":12}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_order_by_topn_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region"},
                        {"key": "group_by_key_arity", "value": "1"},
                        {"key": "group_by_group_count", "value": "2"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "multi_key_scalar_topn"},
                        {"key": "sort_keys", "value": "region,total_amount"},
                        {"key": "sort_direction", "value": "asc,asc"},
                        {"key": "top_n_limit", "value": "2"},
                        {"key": "projected_columns", "value": "region,rows,total_amount"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "4"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.group-by-aggregate-order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .group_by("region")
            .agg(rows="count(*)", total_amount="sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        sorted_workflow = grouped_workflow.sort("region", "total_amount")
        self.assertIsInstance(sorted_workflow, sl.LazyFrame)
        report = sorted_workflow.limit(2).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","rows":2,"total_amount":36}\n'
            '{"region":"north","rows":1,"total_amount":12}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertTrue(report.group_by_runtime_execution)
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("region", "total_amount"))
        self.assertEqual(report.sort_direction, "asc,asc")
        self.assertEqual(report.top_n_limit, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_count_distinct_aggregate_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,count(DISTINCT customer_id) AS unique_customers,count(*) AS rows FROM 'target/input.csv' WHERE amount >= 8 GROUP BY region LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source count distinct",
                    "human_text": "sql local source count distinct",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"unique_customers\\":2,\\"rows\\":4}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(DISTINCT customer_id),count(*)"},
                        {"key": "aggregate_output_columns", "value": "unique_customers,rows"},
                        {"key": "distinct_aggregate_runtime_execution", "value": "true"},
                        {"key": "distinct_aggregate_function", "value": "count(DISTINCT customer_id)"},
                        {"key": "distinct_aggregate_column", "value": "customer_id"},
                        {"key": "distinct_aggregate_null_semantics", "value": "sql_count_distinct_ignores_nulls"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region"},
                        {"key": "group_by_key_arity", "value": "1"},
                        {"key": "group_by_group_count", "value": "1"},
                        {"key": "projected_columns", "value": "region,unique_customers,rows"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "4"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter(sl.col("amount") >= 8)
            .group_by("region")
            .agg(unique_customers=sl.count_distinct("customer_id"), rows="count(*)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        report = grouped_workflow.limit(10).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","unique_customers":2,"rows":4}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertEqual(
            report.aggregate_functions,
            ("count(DISTINCT customer_id)", "count(*)"),
        )
        self.assertEqual(report.aggregate_output_columns, ("unique_customers", "rows"))
        self.assertTrue(report.distinct_aggregate_runtime_execution)
        self.assertEqual(
            report.distinct_aggregate_functions,
            ("count(DISTINCT customer_id)",),
        )
        self.assertEqual(report.distinct_aggregate_columns, ("customer_id",))
        self.assertEqual(
            report.distinct_aggregate_null_semantics,
            "sql_count_distinct_ignores_nulls",
        )
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("region",))
        self.assertEqual(report.group_by_key_arity, 1)
        self.assertEqual(report.group_by_group_count, 1)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_multi_key_group_by_aggregate_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,segment,count(*),sum(amount) FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region,segment LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"segment\\":\\"retail\\",\\"count_all\\":2,\\"sum_amount\\":36}\\n{\\"region\\":\\"west\\",\\"segment\\":\\"enterprise\\",\\"count_all\\":1,\\"sum_amount\\":18}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region,segment"},
                        {"key": "group_by_key_arity", "value": "2"},
                        {"key": "group_by_multi_key_runtime_execution", "value": "true"},
                        {"key": "group_by_group_count", "value": "2"},
                        {"key": "projected_columns", "value": "region,segment,count_all,sum_amount"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.group-by-aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .group_by("region", "segment")
            .agg("count(*)", "sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        report = grouped_workflow.limit(10).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","segment":"retail","count_all":2,"sum_amount":36}\n'
            '{"region":"west","segment":"enterprise","count_all":1,"sum_amount":18}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)"))
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("region", "segment"))
        self.assertEqual(report.group_by_key_arity, 2)
        self.assertTrue(report.group_by_multi_key_runtime_execution)
        self.assertEqual(report.group_by_group_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_named_multi_key_group_by_aggregate_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT region,segment,count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region,segment LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"region\\":\\"east\\",\\"segment\\":\\"retail\\",\\"rows\\":2,\\"total_amount\\":36}\\n{\\"region\\":\\"west\\",\\"segment\\":\\"enterprise\\",\\"rows\\":1,\\"total_amount\\":18}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_group_by_aggregate_filter_limit"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "region,segment"},
                        {"key": "group_by_key_arity", "value": "2"},
                        {"key": "group_by_multi_key_runtime_execution", "value": "true"},
                        {"key": "group_by_group_count", "value": "2"},
                        {"key": "projected_columns", "value": "region,segment,rows,total_amount"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.group-by-aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_csv("target/input.csv")
            .filter("amount >= 10")
            .group_by("region", "segment")
            .agg(rows="count(*)", total_amount="sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        report = grouped_workflow.limit(10).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"region":"east","segment":"retail","rows":2,"total_amount":36}\n'
            '{"region":"west","segment":"enterprise","rows":1,"total_amount":18}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.aggregate_alias_runtime_execution)
        self.assertEqual(report.aggregate_aliases, ("rows", "total_amount"))
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("region", "segment"))
        self.assertEqual(report.group_by_key_arity, 2)
        self.assertTrue(report.group_by_multi_key_runtime_execution)
        self.assertEqual(report.group_by_group_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_group_by_without_aggregate_cannot_lower_to_projection_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "workflow-unsupported-plan",
                    "collect",
                    "read_csv(target/input.csv) -> group_by(region) -> limit(10)",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "workflow operation unsupported",
                    "human_text": "workflow unsupported operation",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "operation", "value": "collect"},
                        {"key": "workflow_summary", "value": "read_csv(target/input.csv) -> group_by(region) -> limit(10)"},
                        {"key": "blocker_id", "value": "cg21.workflow.collect.runtime_not_admitted"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))
        workflow = ctx.read_csv("target/input.csv")._append(
            sl.WorkflowOperation("group_by", ("region",))
        ).limit(10)

        report = workflow.collect()

        self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
        self.assertEqual(report.operation, "collect")
        self.assertFalse(report.runtime_execution)
        self.assertFalse(report.fallback_attempted)

    def test_local_csv_query_builder_order_by_topn_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 ORDER BY amount DESC LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":3,\\"label\\":\\"gamma\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_order_by_topn_filter_limit"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "single_key_scalar_topn"},
                        {"key": "sort_keys", "value": "amount"},
                        {"key": "sort_direction", "value": "desc"},
                        {"key": "sort_null_ordering", "value": "nulls_blocked_for_fixture_smoke"},
                        {"key": "top_n_limit", "value": "2"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        sorted_workflow = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10")
            .sort("amount", descending=True)
        )
        self.assertIsInstance(sorted_workflow, sl.LazyFrame)
        report = sorted_workflow.limit(2).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"id":3,"label":"gamma"}\n{"id":2,"label":"beta"}\n',
        )
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("amount",))
        self.assertEqual(report.sort_direction, "desc")
        self.assertEqual(report.sort_null_ordering, "nulls_blocked_for_fixture_smoke")
        self.assertEqual(report.top_n_limit, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_utf8_order_by_topn_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 ORDER BY label ASC LIMIT 3",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":4,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n{\\"id\\":3,\\"label\\":\\"gamma\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_order_by_topn_filter_limit"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "single_key_scalar_topn"},
                        {"key": "sort_keys", "value": "label"},
                        {"key": "sort_direction", "value": "asc"},
                        {"key": "sort_null_ordering", "value": "nulls_blocked_for_fixture_smoke"},
                        {"key": "top_n_limit", "value": "3"},
                        {"key": "output_row_count", "value": "3"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        sorted_workflow = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10")
            .sort("label")
        )
        self.assertIsInstance(sorted_workflow, sl.LazyFrame)
        report = sorted_workflow.limit(3).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"id":4,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n',
        )
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("label",))
        self.assertEqual(report.sort_direction, "asc")
        self.assertEqual(report.sort_null_ordering, "nulls_blocked_for_fixture_smoke")
        self.assertEqual(report.top_n_limit, 3)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_multi_key_order_by_topn_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 ORDER BY amount DESC,id DESC LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":4,\\"label\\":\\"delta\\"}\\n{\\"id\\":3,\\"label\\":\\"gamma\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_order_by_topn_filter_limit"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "multi_key_scalar_topn"},
                        {"key": "sort_keys", "value": "amount,id"},
                        {"key": "sort_direction", "value": "desc,desc"},
                        {"key": "sort_null_ordering", "value": "nulls_blocked_for_fixture_smoke"},
                        {"key": "top_n_limit", "value": "2"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "4"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "output_native_io_certificate_status", "value": "not_requested"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        sorted_workflow = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .filter("amount >= 10")
            .sort("amount", "id", descending=True)
        )
        self.assertIsInstance(sorted_workflow, sl.LazyFrame)
        report = sorted_workflow.limit(2).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.result_jsonl,
            '{"id":4,"label":"delta"}\n{"id":3,"label":"gamma"}\n',
        )
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("amount", "id"))
        self.assertEqual(report.sort_direction, "desc,desc")
        self.assertEqual(report.sort_null_ordering, "nulls_blocked_for_fixture_smoke")
        self.assertEqual(report.top_n_limit, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_sql_local_source_report_exposes_join_evidence(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join",
                    "human_text": "sql local source join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"enterprise\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id"},
                        {"key": "join_right_key", "value": "d.customer_id"},
                        {"key": "join_left_keys", "value": "f.customer_id"},
                        {"key": "join_right_keys", "value": "d.customer_id"},
                        {"key": "join_key_arity", "value": "1"},
                        {"key": "join_multi_key_runtime_execution", "value": "false"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_rows_output", "value": "1"},
                        {"key": "join_memory_estimate_bytes", "value": "2240"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.inner-equi-join-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        report = client.sql_local_source_smoke(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d "
            "ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10"
        )

        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "inner_equi")
        self.assertEqual(report.join_left_key, "f.customer_id")
        self.assertEqual(report.join_right_key, "d.customer_id")
        self.assertEqual(report.join_left_keys, ("f.customer_id",))
        self.assertEqual(report.join_right_keys, ("d.customer_id",))
        self.assertEqual(report.join_key_arity, 1)
        self.assertFalse(report.join_multi_key_runtime_execution)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_rows_output, 1)
        self.assertEqual(report.join_memory_estimate_bytes, 2240)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_rejects_aggregate_before_join_lowering(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary=["definitely-missing-shardloom"]))

        aggregate_first = ctx.read_csv("target/fact.csv").agg(rows="count(*)")
        self.assertIsInstance(aggregate_first, LazyFrame)
        joined = aggregate_first.join(ctx.read_csv("target/dim.csv"), on="customer_id")
        self.assertIsInstance(joined, LazyFrame)

        self.assertIsNone(joined._sql_local_source_statement())

    def test_local_csv_query_builder_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join",
                    "human_text": "sql local source join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"enterprise\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id"},
                        {"key": "join_right_key", "value": "d.customer_id"},
                        {"key": "join_left_keys", "value": "f.customer_id"},
                        {"key": "join_right_keys", "value": "d.customer_id"},
                        {"key": "join_key_arity", "value": "1"},
                        {"key": "join_multi_key_runtime_execution", "value": "false"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_rows_output", "value": "1"},
                        {"key": "join_memory_estimate_bytes", "value": "2240"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on="customer_id")
            .select("f.id", "d.segment")
            .filter("f.amount >= 10")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "inner_equi")
        self.assertEqual(report.join_left_key, "f.customer_id")
        self.assertEqual(report.join_right_key, "d.customer_id")
        self.assertEqual(report.join_left_keys, ("f.customer_id",))
        self.assertEqual(report.join_right_keys, ("d.customer_id",))
        self.assertEqual(report.join_key_arity, 1)
        self.assertFalse(report.join_multi_key_runtime_execution)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_rows_output, 1)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_expression_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.amount > d.threshold ORDER BY f.id ASC,d.threshold ASC LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source expression join",
                    "human_text": "sql local source expression join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"base\\"}\\n{\\"f.id\\":3,\\"d.segment\\":\\"base\\"}\\n{\\"f.id\\":3,\\"d.segment\\":\\"premium\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_expression_join_order_by_topn_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_expression"},
                        {"key": "join_on_predicate_runtime_execution", "value": "true"},
                        {"key": "join_on_predicate_operator_family", "value": "column_compare"},
                        {"key": "join_on_predicate_source_column", "value": "f.amount,d.threshold"},
                        {"key": "join_key_arity", "value": "0"},
                        {"key": "join_multi_key_runtime_execution", "value": "false"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_candidate_row_count", "value": "6"},
                        {"key": "join_rows_output", "value": "3"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.inner-expression-join-order-by-topn-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), condition="f.amount > d.threshold")
            .select("f.id", "d.segment")
            .sort("f.id", "d.threshold")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "inner_expression")
        self.assertTrue(report.join_on_predicate_runtime_execution)
        self.assertEqual(report.join_on_predicate_operator_family, "column_compare")
        self.assertEqual(
            report.join_on_predicate_source_columns, ("f.amount", "d.threshold")
        )
        self.assertEqual(report.join_key_arity, 0)
        self.assertFalse(report.join_multi_key_runtime_execution)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_candidate_row_count, 6)
        self.assertEqual(report.join_rows_output, 3)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_rejects_ambiguous_join_condition_api(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary=["definitely-missing-shardloom"]))
        frame = ctx.read_csv("target/fact.csv")
        dim = ctx.read_csv("target/dim.csv")

        with self.assertRaisesRegex(ValueError, "either on= equi keys or condition="):
            frame.join(dim, on="customer_id", condition="f.amount > d.threshold")

        with self.assertRaisesRegex(ValueError, "cross joins do not accept condition="):
            frame.join(dim, how="cross", condition="f.amount > d.threshold")

    def test_local_csv_query_builder_left_outer_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f LEFT JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id ORDER BY f.id ASC LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source left outer join",
                    "human_text": "sql local source left outer join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":4,\\"d.segment\\":null}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_left_outer_equi_join_order_by_topn_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "left_outer_equi"},
                        {"key": "join_left_key", "value": "f.customer_id"},
                        {"key": "join_right_key", "value": "d.customer_id"},
                        {"key": "join_key_arity", "value": "1"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_candidate_row_count", "value": "3"},
                        {"key": "join_unmatched_left_row_count", "value": "1"},
                        {"key": "join_unmatched_right_row_count", "value": "0"},
                        {"key": "join_rows_output", "value": "4"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on="customer_id", how="left")
            .select("f.id", "d.segment")
            .sort("f.id")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "left_outer_equi")
        self.assertEqual(report.join_key_arity, 1)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_candidate_row_count, 3)
        self.assertEqual(report.join_unmatched_left_row_count, 1)
        self.assertEqual(report.join_unmatched_right_row_count, 0)
        self.assertEqual(report.join_rows_output, 4)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_cross_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f CROSS JOIN 'target/dim.csv' AS d WHERE f.id = 2 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source cross join",
                    "human_text": "sql local source cross join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"seed\\"}\\n{\\"f.id\\":2,\\"d.segment\\":\\"enterprise\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_cross_join_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "cross"},
                        {"key": "join_key_arity", "value": "0"},
                        {"key": "join_matched_row_count", "value": "4"},
                        {"key": "join_candidate_row_count", "value": "4"},
                        {"key": "join_rows_output", "value": "2"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), how="cross")
            .select("f.id", "d.segment")
            .filter("f.id = 2")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "cross")
        self.assertEqual(report.join_key_arity, 0)
        self.assertEqual(report.join_candidate_row_count, 4)
        self.assertEqual(report.join_rows_output, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_right_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f RIGHT JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source right outer join",
                    "human_text": "sql local source right outer join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":null,\\"d.segment\\":\\"orphan\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_right_outer_equi_join_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "right_outer_equi"},
                        {"key": "join_key_arity", "value": "1"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_candidate_row_count", "value": "3"},
                        {"key": "join_unmatched_left_row_count", "value": "0"},
                        {"key": "join_unmatched_right_row_count", "value": "1"},
                        {"key": "join_rows_output", "value": "4"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on="customer_id", how="right_outer")
            .select("f.id", "d.segment")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "right_outer_equi")
        self.assertEqual(report.join_key_arity, 1)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_candidate_row_count, 3)
        self.assertEqual(report.join_unmatched_left_row_count, 0)
        self.assertEqual(report.join_unmatched_right_row_count, 1)
        self.assertEqual(report.join_rows_output, 4)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_invalid_join_how_is_deterministic(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary=self.fake_cli("")))

        with self.assertRaisesRegex(
            ValueError,
            "join how must be one of inner, left, right, full, semi, anti, or cross",
        ):
            ctx.read_csv("target/fact.csv").join(
                ctx.read_csv("target/dim.csv"),
                on="customer_id",
                how="natural",
            )

    def test_local_csv_query_builder_join_topn_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join topn",
                    "human_text": "sql local source join topn",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":5,\\"d.segment\\":\\"startup\\"}\\n{\\"f.id\\":3,\\"d.segment\\":\\"consumer\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_order_by_topn_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_key_arity", "value": "2"},
                        {"key": "join_multi_key_runtime_execution", "value": "true"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_rows_output", "value": "2"},
                        {"key": "join_computed_projection_runtime_execution", "value": "false"},
                        {"key": "join_order_by_top_n_runtime_execution", "value": "true"},
                        {"key": "join_projection_operator_family", "value": "raw_projection_topn"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_keys", "value": "f.amount"},
                        {"key": "sort_direction", "value": "desc"},
                        {"key": "top_n_limit", "value": "2"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on=("customer_id", "region"))
            .select("f.id", "d.segment")
            .filter("f.amount >= 10")
            .sort("f.amount", descending=True)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_key_arity, 2)
        self.assertTrue(report.join_multi_key_runtime_execution)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_rows_output, 2)
        self.assertFalse(report.join_computed_projection_runtime_execution)
        self.assertTrue(report.join_order_by_top_n_runtime_execution)
        self.assertEqual(report.join_projection_operator_family, "raw_projection_topn")
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("f.amount",))
        self.assertEqual(report.sort_direction, "desc")
        self.assertEqual(report.top_n_limit, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_multi_key_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join",
                    "human_text": "sql local source join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"enterprise\\"}\\n{\\"f.id\\":3,\\"d.segment\\":\\"consumer\\"}\\n{\\"f.id\\":5,\\"d.segment\\":\\"startup\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id,f.region"},
                        {"key": "join_right_key", "value": "d.customer_id,d.region"},
                        {"key": "join_left_keys", "value": "f.customer_id,f.region"},
                        {"key": "join_right_keys", "value": "d.customer_id,d.region"},
                        {"key": "join_key_arity", "value": "2"},
                        {"key": "join_multi_key_runtime_execution", "value": "true"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_rows_output", "value": "3"},
                        {"key": "join_memory_estimate_bytes", "value": "4032"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on=("customer_id", "region"))
            .select("f.id", "d.segment")
            .filter("f.amount >= 10")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "inner_equi")
        self.assertEqual(report.join_left_key, "f.customer_id,f.region")
        self.assertEqual(report.join_right_key, "d.customer_id,d.region")
        self.assertEqual(report.join_left_keys, ("f.customer_id", "f.region"))
        self.assertEqual(report.join_right_keys, ("d.customer_id", "d.region"))
        self.assertEqual(report.join_key_arity, 2)
        self.assertTrue(report.join_multi_key_runtime_execution)
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_rows_output, 3)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_join_computed_projection_topn_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment,f.amount + d.discount AS adjusted,CONCAT(d.segment, '-', f.region) AS segment_region FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 3",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join computed topn",
                    "human_text": "sql local source join computed topn",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":5,\\"d.segment\\":\\"startup\\",\\"adjusted\\":28,\\"segment_region\\":\\"startup-eu\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_computed_projection_order_by_topn_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_key_arity", "value": "2"},
                        {"key": "join_multi_key_runtime_execution", "value": "true"},
                        {"key": "join_matched_row_count", "value": "5"},
                        {"key": "join_rows_output", "value": "3"},
                        {"key": "join_computed_projection_runtime_execution", "value": "true"},
                        {"key": "join_order_by_top_n_runtime_execution", "value": "true"},
                        {"key": "join_projection_operator_family", "value": "computed_projection_topn"},
                        {"key": "generic_expression_projection_runtime_execution", "value": "true"},
                        {"key": "generic_expression_projection_source_column", "value": "d.discount+f.amount"},
                        {"key": "generic_expression_projection_output_column", "value": "adjusted"},
                        {"key": "string_function_projection_runtime_execution", "value": "true"},
                        {"key": "string_function_projection_source_column", "value": "d.segment+f.region"},
                        {"key": "string_function_projection_output_column", "value": "segment_region"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_keys", "value": "f.amount"},
                        {"key": "sort_direction", "value": "desc"},
                        {"key": "top_n_limit", "value": "3"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on=("customer_id", "region"))
            .select("f.id", "d.segment")
            .with_column("adjusted", sl.col("f.amount") + sl.col("d.discount"))
            .with_column(
                "segment_region",
                sl.concat(sl.col("d.segment"), "-", sl.col("f.region")),
            )
            .filter("f.amount >= 10")
            .sort("f.amount", descending=True)
            .limit(3)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_key_arity, 2)
        self.assertTrue(report.join_multi_key_runtime_execution)
        self.assertEqual(report.join_matched_row_count, 5)
        self.assertEqual(report.join_rows_output, 3)
        self.assertTrue(report.join_computed_projection_runtime_execution)
        self.assertTrue(report.join_order_by_top_n_runtime_execution)
        self.assertEqual(report.join_projection_operator_family, "computed_projection_topn")
        self.assertTrue(report.generic_expression_projection_runtime_execution)
        self.assertEqual(
            report.generic_expression_projection_source_columns,
            ("d.discount+f.amount",),
        )
        self.assertEqual(report.generic_expression_projection_output_columns, ("adjusted",))
        self.assertTrue(report.string_function_projection_runtime_execution)
        self.assertEqual(
            report.string_function_projection_source_columns,
            ("d.segment+f.region",),
        )
        self.assertEqual(
            report.string_function_projection_output_columns,
            ("segment_region",),
        )
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("f.amount",))
        self.assertEqual(report.sort_direction, "desc")
        self.assertEqual(report.top_n_limit, 3)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_join_scalar_aggregate_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT count(*) AS rows,sum(f.amount) AS total_amount FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE d.segment = 'enterprise' LIMIT 1",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join aggregate",
                    "human_text": "sql local source join aggregate",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"rows\\":3,\\"total_amount\\":41}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_aggregate_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id"},
                        {"key": "join_right_key", "value": "d.customer_id"},
                        {"key": "join_key_arity", "value": "1"},
                        {"key": "join_multi_key_runtime_execution", "value": "false"},
                        {"key": "join_matched_row_count", "value": "4"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "scalar_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(f.amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "join_aggregate_runtime_execution", "value": "true"},
                        {"key": "join_aggregate_operator_family", "value": "scalar_join_aggregate"},
                        {"key": "join_aggregate_group_count", "value": "0"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.inner-equi-join-aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on="customer_id")
            .filter("d.segment = 'enterprise'")
            .agg(rows="count(*)", total_amount="sum(f.amount)")
            .limit(1)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(f.amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.aggregate_alias_runtime_execution)
        self.assertEqual(report.aggregate_aliases, ("rows", "total_amount"))
        self.assertTrue(report.join_aggregate_runtime_execution)
        self.assertEqual(report.join_aggregate_operator_family, "scalar_join_aggregate")
        self.assertEqual(report.join_aggregate_group_count, 0)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_join_scalar_aggregate_order_by_topn_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT count(*) AS rows,sum(f.amount) AS total_amount FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE d.segment = 'enterprise' ORDER BY total_amount DESC,rows DESC LIMIT 1",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join aggregate",
                    "human_text": "sql local source join aggregate",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"rows\\":3,\\"total_amount\\":41}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_aggregate_order_by_topn_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id"},
                        {"key": "join_right_key", "value": "d.customer_id"},
                        {"key": "join_key_arity", "value": "1"},
                        {"key": "join_multi_key_runtime_execution", "value": "false"},
                        {"key": "join_matched_row_count", "value": "4"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "scalar_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(f.amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "join_aggregate_runtime_execution", "value": "true"},
                        {"key": "join_aggregate_operator_family", "value": "scalar_join_aggregate"},
                        {"key": "join_aggregate_group_count", "value": "0"},
                        {"key": "order_by_runtime_execution", "value": "true"},
                        {"key": "top_n_runtime_execution", "value": "true"},
                        {"key": "sort_operator_family", "value": "multi_key_scalar_topn"},
                        {"key": "sort_keys", "value": "total_amount,rows"},
                        {"key": "sort_direction", "value": "desc,desc"},
                        {"key": "top_n_limit", "value": "1"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.inner-equi-join-aggregate-order-by-topn-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on="customer_id")
            .filter("d.segment = 'enterprise'")
            .agg(rows="count(*)", total_amount="sum(f.amount)")
            .sort("total_amount", "rows", descending=True)
            .limit(1)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(f.amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.join_aggregate_runtime_execution)
        self.assertEqual(report.join_aggregate_operator_family, "scalar_join_aggregate")
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("total_amount", "rows"))
        self.assertEqual(report.sort_direction, "desc,desc")
        self.assertEqual(report.top_n_limit, 1)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_multi_key_join_group_by_aggregate_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT d.segment,count(*) AS rows,sum(f.amount) AS total_amount FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 GROUP BY d.segment LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join group aggregate",
                    "human_text": "sql local source join group aggregate",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"d.segment\\":\\"consumer\\",\\"rows\\":1,\\"total_amount\\":21}\\n{\\"d.segment\\":\\"enterprise\\",\\"rows\\":2,\\"total_amount\\":37}\\n{\\"d.segment\\":\\"startup\\",\\"rows\\":1,\\"total_amount\\":23}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_group_by_aggregate_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id,f.region"},
                        {"key": "join_right_key", "value": "d.customer_id,d.region"},
                        {"key": "join_left_keys", "value": "f.customer_id,f.region"},
                        {"key": "join_right_keys", "value": "d.customer_id,d.region"},
                        {"key": "join_key_arity", "value": "2"},
                        {"key": "join_multi_key_runtime_execution", "value": "true"},
                        {"key": "join_matched_row_count", "value": "4"},
                        {"key": "aggregate_runtime_execution", "value": "true"},
                        {"key": "aggregate_operator_family", "value": "grouped_aggregate"},
                        {"key": "aggregate_functions", "value": "count(*),sum(f.amount)"},
                        {"key": "aggregate_output_columns", "value": "rows,total_amount"},
                        {"key": "aggregate_alias_runtime_execution", "value": "true"},
                        {"key": "aggregate_aliases", "value": "rows,total_amount"},
                        {"key": "group_by_runtime_execution", "value": "true"},
                        {"key": "group_by_columns", "value": "d.segment"},
                        {"key": "group_by_key_arity", "value": "1"},
                        {"key": "group_by_multi_key_runtime_execution", "value": "false"},
                        {"key": "group_by_group_count", "value": "3"},
                        {"key": "join_aggregate_runtime_execution", "value": "true"},
                        {"key": "join_aggregate_operator_family", "value": "grouped_join_aggregate"},
                        {"key": "join_aggregate_group_count", "value": "3"},
                        {"key": "selected_row_count", "value": "4"},
                        {"key": "output_row_count", "value": "3"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.csv.inner-equi-join-group-by-aggregate-filter-limit.execution.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join(ctx.read_csv("target/dim.csv"), on=("customer_id", "region"))
            .filter("f.amount >= 10")
            .group_by("d.segment")
            .agg(rows="count(*)", total_amount="sum(f.amount)")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_left_keys, ("f.customer_id", "f.region"))
        self.assertEqual(report.join_right_keys, ("d.customer_id", "d.region"))
        self.assertEqual(report.join_key_arity, 2)
        self.assertTrue(report.join_multi_key_runtime_execution)
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(f.amount)"))
        self.assertEqual(report.aggregate_output_columns, ("rows", "total_amount"))
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("d.segment",))
        self.assertEqual(report.group_by_key_arity, 1)
        self.assertFalse(report.group_by_multi_key_runtime_execution)
        self.assertEqual(report.group_by_group_count, 3)
        self.assertTrue(report.join_aggregate_runtime_execution)
        self.assertEqual(report.join_aggregate_operator_family, "grouped_join_aggregate")
        self.assertEqual(report.join_aggregate_group_count, 3)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_json_query_builder_join_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.jsonl' AS f INNER JOIN 'target/dim.jsonl' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join",
                    "human_text": "sql local source join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"enterprise\\"}\\n"},
                        {"key": "source_format", "value": "jsonl"},
                        {"key": "right_source_format", "value": "jsonl"},
                        {"key": "join_source_formats", "value": "jsonl,jsonl"},
                        {"key": "sql_statement_kind", "value": "local_source_inner_equi_join_filter_limit"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "join_left_key", "value": "f.customer_id"},
                        {"key": "join_right_key", "value": "d.customer_id"},
                        {"key": "join_matched_row_count", "value": "3"},
                        {"key": "join_rows_output", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_json("target/fact.jsonl")
            .join(ctx.read_json("target/dim.jsonl"), on="customer_id")
            .select("f.id", "d.segment")
            .filter("f.amount >= 10")
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("source_format"), "jsonl")
        self.assertEqual(report.envelope.field("right_source_format"), "jsonl")
        self.assertEqual(report.envelope.field("join_source_formats"), "jsonl,jsonl")
        self.assertTrue(report.join_runtime_execution)
        self.assertEqual(report.join_type, "inner_equi")
        self.assertEqual(report.join_left_key, "f.customer_id")
        self.assertEqual(report.join_right_key, "d.customer_id")
        self.assertEqual(report.join_matched_row_count, 3)
        self.assertEqual(report.join_rows_output, 1)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_join_write_invokes_sql_smoke_output(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
                    "--output-format",
                    "jsonl",
                    "--output",
                    "target/joined.jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source join",
                    "human_text": "sql local source join",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"f.id\\":2,\\"d.segment\\":\\"enterprise\\"}\\n"},
                        {"key": "output_path", "value": "target/joined.jsonl"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_workspace_path_safety_status", "value": "enforced"},
                        {"key": "output_commit_mode", "value": "staged_replace_with_backup_same_directory"},
                        {"key": "output_commit_status", "value": "committed"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                        {"key": "join_runtime_execution", "value": "true"},
                        {"key": "join_type", "value": "inner_equi"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/fact.csv")
            .join("target/dim.csv", on="customer_id")
            .select("f.id", "d.segment")
            .filter("f.amount >= 10")
            .limit(10)
            .write("target/joined.jsonl", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/joined.jsonl")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(report.output_native_io_certificate_status, "certified_local_jsonl_sink")
        self.assertTrue(report.join_runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_with_column_literal_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label,'north' AS segment FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source literal projection",
                    "human_text": "sql local source literal projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\",\\"segment\\":\\"north\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_literal_projection_filter_limit"},
                        {"key": "literal_projection_runtime_execution", "value": "true"},
                        {"key": "literal_projection_columns", "value": "segment"},
                        {"key": "literal_projection_count", "value": "1"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "label")
            .with_column("segment", "lit('north')")
            .filter("amount >= 10")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_literal_projection_filter_limit",
        )
        self.assertEqual(report.envelope.field("literal_projection_columns"), "segment")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_numeric_arithmetic_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,amount + 5 AS adjusted FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"adjusted\\":20}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "numeric_arithmetic_projection_runtime_execution", "value": "true"},
                        {"key": "numeric_arithmetic_projection_operator", "value": "add"},
                        {"key": "numeric_arithmetic_projection_source_column", "value": "amount"},
                        {"key": "numeric_arithmetic_projection_output_column", "value": "adjusted"},
                        {"key": "numeric_arithmetic_projection_rhs_dtype", "value": "int64"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("adjusted", sl.col("amount") + 5)
            .filter(sl.col("amount") >= 10)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_computed_projection_filter_limit",
        )
        self.assertTrue(report.numeric_arithmetic_projection_runtime_execution)
        self.assertEqual(report.numeric_arithmetic_projection_operator, ("add",))
        self.assertEqual(report.numeric_arithmetic_projection_source_columns, ("amount",))
        self.assertEqual(report.numeric_arithmetic_projection_output_columns, ("adjusted",))
        self.assertEqual(report.numeric_arithmetic_projection_rhs_dtypes, ("int64",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_json_query_builder_with_column_without_select_invokes_sql_smoke(
        self,
    ) -> None:
        statement = "SELECT *,amount + 5 AS adjusted FROM 'target/input.jsonl' WHERE amount >= 10 LIMIT 2"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":2,\\"amount\\":15,\\"label\\":\\"beta\\",\\"adjusted\\":20}}\\n"}},
                        {{"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"}},
                        {{"key": "source_format", "value": "jsonl"}},
                        {{"key": "numeric_arithmetic_projection_runtime_execution", "value": "true"}},
                        {{"key": "numeric_arithmetic_projection_operator", "value": "add"}},
                        {{"key": "numeric_arithmetic_projection_source_column", "value": "amount"}},
                        {{"key": "numeric_arithmetic_projection_output_column", "value": "adjusted"}},
                        {{"key": "numeric_arithmetic_projection_rhs_dtype", "value": "int64"}},
                        {{"key": "output_row_count", "value": "1"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_json("target/input.jsonl")
            .with_column("adjusted", sl.col("amount") + 5)
            .filter(sl.col("amount") >= 10)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_computed_projection_filter_limit",
        )
        self.assertEqual(report.envelope.field("source_format"), "jsonl")
        self.assertTrue(report.numeric_arithmetic_projection_runtime_execution)
        self.assertEqual(report.numeric_arithmetic_projection_operator, ("add",))
        self.assertEqual(report.numeric_arithmetic_projection_source_columns, ("amount",))
        self.assertEqual(report.numeric_arithmetic_projection_output_columns, ("adjusted",))
        self.assertEqual(report.numeric_arithmetic_projection_rhs_dtypes, ("int64",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_json_query_builder_with_column_sort_invokes_sql_smoke(self) -> None:
        statement = "SELECT *,amount + 5 AS adjusted FROM 'target/input.jsonl' WHERE amount >= 10 ORDER BY adjusted DESC LIMIT 2"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection topn",
                    "human_text": "sql local source computed projection topn",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":3,\\"amount\\":21,\\"label\\":\\"gamma\\",\\"adjusted\\":26}}\\n{{\\"id\\":2,\\"amount\\":15,\\"label\\":\\"beta\\",\\"adjusted\\":20}}\\n"}},
                        {{"key": "sql_statement_kind", "value": "local_source_computed_projection_order_by_topn_filter_limit"}},
                        {{"key": "source_format", "value": "jsonl"}},
                        {{"key": "computed_projection_runtime_execution", "value": "true"}},
                        {{"key": "computed_projection_top_n_runtime_execution", "value": "true"}},
                        {{"key": "computed_projection_operator_family", "value": "computed_projection_topn"}},
                        {{"key": "numeric_arithmetic_projection_runtime_execution", "value": "true"}},
                        {{"key": "numeric_arithmetic_projection_operator", "value": "add"}},
                        {{"key": "numeric_arithmetic_projection_source_column", "value": "amount"}},
                        {{"key": "numeric_arithmetic_projection_output_column", "value": "adjusted"}},
                        {{"key": "sort_keys", "value": "adjusted"}},
                        {{"key": "sort_direction", "value": "desc"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_json("target/input.jsonl")
            .with_column("adjusted", sl.col("amount") + 5)
            .filter(sl.col("amount") >= 10)
            .sort("adjusted", descending=True)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_computed_projection_order_by_topn_filter_limit",
        )
        self.assertTrue(report.computed_projection_runtime_execution)
        self.assertTrue(report.computed_projection_top_n_runtime_execution)
        self.assertEqual(
            report.computed_projection_operator_family,
            "computed_projection_topn",
        )
        self.assertEqual(report.sort_keys, ("adjusted",))
        self.assertEqual(report.sort_direction, "desc")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_window_row_number_invokes_sql_smoke(self) -> None:
        statement = "SELECT id,region,amount,ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM 'target/input.csv' WHERE amount >= 10 LIMIT 4"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source window row number",
                    "human_text": "sql local source window row number",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":1,\\"region\\":\\"east\\",\\"amount\\":10,\\"rn\\":3}}\\n{{\\"id\\":2,\\"region\\":\\"east\\",\\"amount\\":30,\\"rn\\":1}}\\n"}},
                        {{"key": "sql_statement_kind", "value": "local_source_window_filter_limit"}},
                        {{"key": "window_runtime_execution", "value": "true"}},
                        {{"key": "window_operator_family", "value": "row_number"}},
                        {{"key": "window_function", "value": "row_number"}},
                        {{"key": "window_partition_columns", "value": "region"}},
                        {{"key": "window_order_by_columns", "value": "amount"}},
                        {{"key": "window_order_by_directions", "value": "desc"}},
                        {{"key": "window_output_columns", "value": "rn"}},
                        {{"key": "window_row_number_runtime_execution", "value": "true"}},
                        {{"key": "output_row_count", "value": "2"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "region", "amount")
            .filter(sl.col("amount") >= 10)
            .window(
                sl.row_number(
                    partition_by="region",
                    order_by="amount",
                    descending=True,
                    alias="rn",
                )
            )
            .limit(4)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_window_filter_limit",
        )
        self.assertTrue(report.window_runtime_execution)
        self.assertEqual(report.window_operator_family, "row_number")
        self.assertEqual(report.window_function, ("row_number",))
        self.assertEqual(report.window_partition_columns, ("region",))
        self.assertEqual(report.window_order_by_columns, ("amount",))
        self.assertEqual(report.window_order_by_directions, ("desc",))
        self.assertEqual(report.window_output_columns, ("rn",))
        self.assertTrue(report.window_row_number_runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_window_blocks_post_window_reordering(self) -> None:
        workflow = sl.read_csv(
            "target/input.csv",
            binary=["definitely-missing-shardloom"],
        ).window(sl.row_number(order_by="amount", alias="rn"))

        self.assertEqual(
            workflow.limit(5)._sql_local_source_statement(),
            "SELECT *,ROW_NUMBER() OVER (ORDER BY amount ASC) AS rn FROM 'target/input.csv' LIMIT 5",
        )
        self.assertIsNone(workflow.select("id").limit(5)._sql_local_source_statement())
        self.assertIsNone(workflow.filter("amount > 1").limit(5)._sql_local_source_statement())
        self.assertIsNone(workflow.sort("amount").limit(5)._sql_local_source_statement())

    def test_local_csv_query_builder_window_rank_dense_rank_invokes_sql_smoke(self) -> None:
        statement = "SELECT id,region,amount,RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS r,DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS dr FROM 'target/input.csv' LIMIT 6"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source window rank",
                    "human_text": "sql local source window rank",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":1,\\"region\\":\\"east\\",\\"amount\\":30,\\"r\\":1,\\"dr\\":1}}\\n"}},
                        {{"key": "sql_statement_kind", "value": "local_source_window_limit"}},
                        {{"key": "window_runtime_execution", "value": "true"}},
                        {{"key": "window_operator_family", "value": "ranking"}},
                        {{"key": "window_function", "value": "rank,dense_rank"}},
                        {{"key": "window_partition_columns", "value": "region;region"}},
                        {{"key": "window_order_by_columns", "value": "amount;amount"}},
                        {{"key": "window_order_by_directions", "value": "desc;desc"}},
                        {{"key": "window_output_columns", "value": "r,dr"}},
                        {{"key": "window_row_number_runtime_execution", "value": "false"}},
                        {{"key": "window_rank_runtime_execution", "value": "true"}},
                        {{"key": "window_dense_rank_runtime_execution", "value": "true"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id", "region", "amount")
            .window(
                sl.rank(
                    partition_by="region",
                    order_by="amount",
                    descending=True,
                    alias="r",
                ),
                sl.dense_rank(
                    partition_by="region",
                    order_by="amount",
                    descending=True,
                    alias="dr",
                ),
            )
            .limit(6)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.window_operator_family, "ranking")
        self.assertEqual(report.window_function, ("rank", "dense_rank"))
        self.assertEqual(report.window_partition_columns, ("region", "region"))
        self.assertEqual(report.window_order_by_columns, ("amount", "amount"))
        self.assertEqual(report.window_order_by_directions, ("desc", "desc"))
        self.assertEqual(report.window_output_columns, ("r", "dr"))
        self.assertFalse(report.window_row_number_runtime_execution)
        self.assertTrue(report.window_rank_runtime_execution)
        self.assertTrue(report.window_dense_rank_runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_generic_expression_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,(amount + tax) * 2 AS gross FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source generic expression projection",
                    "human_text": "sql local source generic expression projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"gross\\":40}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "generic_expression_projection_runtime_execution", "value": "true"},
                        {"key": "generic_expression_projection_source_column", "value": "amount+tax"},
                        {"key": "generic_expression_projection_output_column", "value": "gross"},
                        {"key": "generic_expression_projection_operator_family", "value": "numeric_binary"},
                        {"key": "generic_expression_projection_binary_operator_count", "value": "2"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("gross", (sl.col("amount") + sl.col("tax")) * 2)
            .filter(sl.col("amount") >= 10)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_computed_projection_filter_limit",
        )
        self.assertTrue(report.generic_expression_projection_runtime_execution)
        self.assertEqual(
            report.generic_expression_projection_source_columns, ("amount+tax",)
        )
        self.assertEqual(report.generic_expression_projection_output_columns, ("gross",))
        self.assertEqual(
            report.generic_expression_projection_operator_families,
            ("numeric_binary",),
        )
        self.assertEqual(report.generic_expression_projection_binary_operator_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_generic_expression_filter_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id FROM 'target/input.csv' WHERE (amount + tax) * 2 >= 40 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source generic expression predicate",
                    "human_text": "sql local source generic expression predicate",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_projection_filter_limit"},
                        {"key": "predicate_operator_family", "value": "generic_expression"},
                        {"key": "filter_runtime_execution", "value": "true"},
                        {"key": "generic_expression_predicate_runtime_execution", "value": "true"},
                        {"key": "generic_expression_predicate_source_column", "value": "amount+tax"},
                        {"key": "generic_expression_predicate_operator_family", "value": "numeric_binary"},
                        {"key": "generic_expression_predicate_binary_operator_count", "value": "2"},
                        {"key": "generic_expression_predicate_comparison_operator", "value": "gte"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .filter((sl.col("amount") + sl.col("tax")) * 2 >= 40)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(
            report.envelope.field("sql_statement_kind"),
            "local_source_projection_filter_limit",
        )
        self.assertEqual(report.predicate_operator_family, "generic_expression")
        self.assertTrue(report.filter_runtime_execution)
        self.assertTrue(report.generic_expression_predicate_runtime_execution)
        self.assertEqual(
            report.generic_expression_predicate_source_columns, ("amount+tax",)
        )
        self.assertEqual(
            report.generic_expression_predicate_operator_families,
            ("numeric_binary",),
        )
        self.assertEqual(report.generic_expression_predicate_binary_operator_count, 2)
        self.assertEqual(report.generic_expression_predicate_comparison_operators, ("gte",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_numeric_abs_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,ABS(amount) AS magnitude FROM 'target/input.csv' WHERE ABS(amount) >= 4 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source numeric abs projection",
                    "human_text": "sql local source numeric abs projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"magnitude\\":5}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "predicate_operator_family", "value": "numeric_abs"},
                        {"key": "numeric_abs_runtime_execution", "value": "true"},
                        {"key": "numeric_abs_source_column", "value": "amount"},
                        {"key": "numeric_abs_rhs_dtype", "value": "int64"},
                        {"key": "numeric_abs_projection_runtime_execution", "value": "true"},
                        {"key": "numeric_abs_projection_source_column", "value": "amount"},
                        {"key": "numeric_abs_projection_output_column", "value": "magnitude"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("magnitude", sl.abs(sl.col("amount")))
            .filter(sl.col("amount").abs() >= 4)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "numeric_abs")
        self.assertTrue(report.numeric_abs_runtime_execution)
        self.assertEqual(report.numeric_abs_source_columns, ("amount",))
        self.assertEqual(report.numeric_abs_rhs_dtypes, ("int64",))
        self.assertTrue(report.numeric_abs_projection_runtime_execution)
        self.assertEqual(report.numeric_abs_projection_source_columns, ("amount",))
        self.assertEqual(report.numeric_abs_projection_output_columns, ("magnitude",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_numeric_rounding_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,FLOOR(amount) AS bucket FROM 'target/input.csv' WHERE ROUND(amount) >= 4 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source numeric rounding projection",
                    "human_text": "sql local source numeric rounding projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"bucket\\":4.0}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "predicate_operator_family", "value": "numeric_rounding"},
                        {"key": "numeric_rounding_runtime_execution", "value": "true"},
                        {"key": "numeric_rounding_operator", "value": "round"},
                        {"key": "numeric_rounding_source_column", "value": "amount"},
                        {"key": "numeric_rounding_rhs_dtype", "value": "int64"},
                        {"key": "numeric_rounding_projection_runtime_execution", "value": "true"},
                        {"key": "numeric_rounding_projection_operator", "value": "floor"},
                        {"key": "numeric_rounding_projection_source_column", "value": "amount"},
                        {"key": "numeric_rounding_projection_output_column", "value": "bucket"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("bucket", sl.floor(sl.col("amount")))
            .filter(sl.col("amount").round() >= 4)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "numeric_rounding")
        self.assertTrue(report.numeric_rounding_runtime_execution)
        self.assertEqual(report.numeric_rounding_operators, ("round",))
        self.assertEqual(report.numeric_rounding_source_columns, ("amount",))
        self.assertEqual(report.numeric_rounding_rhs_dtypes, ("int64",))
        self.assertTrue(report.numeric_rounding_projection_runtime_execution)
        self.assertEqual(report.numeric_rounding_projection_operators, ("floor",))
        self.assertEqual(report.numeric_rounding_projection_source_columns, ("amount",))
        self.assertEqual(report.numeric_rounding_projection_output_columns, ("bucket",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_string_transform_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,LOWER(label) AS normalized FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"normalized\\":\\"beta\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "string_transform_projection_runtime_execution", "value": "true"},
                        {"key": "string_transform_projection_operator", "value": "lower"},
                        {"key": "string_transform_projection_source_column", "value": "label"},
                        {"key": "string_transform_projection_output_column", "value": "normalized"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("normalized", sl.col("label").lower())
            .filter(sl.col("amount") >= 10)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.string_transform_projection_runtime_execution)
        self.assertEqual(report.string_transform_projection_operator, ("lower",))
        self.assertEqual(report.string_transform_projection_source_columns, ("label",))
        self.assertEqual(
            report.string_transform_projection_output_columns, ("normalized",)
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_string_length_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,LENGTH(label) AS label_len FROM 'target/input.csv' WHERE LENGTH(label) >= 4 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source string length projection",
                    "human_text": "sql local source string length projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label_len\\":4}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "predicate_operator_family", "value": "string_length"},
                        {"key": "string_length_runtime_execution", "value": "true"},
                        {"key": "string_length_source_column", "value": "label"},
                        {"key": "string_length_rhs_dtype", "value": "int64"},
                        {"key": "string_length_projection_runtime_execution", "value": "true"},
                        {"key": "string_length_projection_source_column", "value": "label"},
                        {"key": "string_length_projection_output_column", "value": "label_len"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("label_len", sl.length(sl.col("label")))
            .filter(sl.col("label").length() >= 4)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "string_length")
        self.assertTrue(report.string_length_runtime_execution)
        self.assertEqual(report.string_length_source_columns, ("label",))
        self.assertEqual(report.string_length_rhs_dtypes, ("int64",))
        self.assertTrue(report.string_length_projection_runtime_execution)
        self.assertEqual(report.string_length_projection_source_columns, ("label",))
        self.assertEqual(report.string_length_projection_output_columns, ("label_len",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_string_function_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,CONCAT(label, '-', segment) AS label_key,SUBSTR(label, 1, 3) AS prefix,LEFT(label, 2) AS left_edge,RIGHT(label, 2) AS right_edge,REPLACE(label, 'a', '') AS scrubbed FROM 'target/input.csv' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source string function projection",
                    "human_text": "sql local source string function projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label_key\\":\\"alpha-north\\",\\"prefix\\":\\"alp\\",\\"scrubbed\\":\\"lph\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "predicate_operator_family", "value": "string_function"},
                        {"key": "string_function_runtime_execution", "value": "true"},
                        {"key": "string_function_operator", "value": "concat"},
                        {"key": "string_function_source_column", "value": "label+segment"},
                        {"key": "string_function_literal_count", "value": "2"},
                        {"key": "string_function_rhs_dtype", "value": "utf8"},
                        {"key": "string_function_projection_runtime_execution", "value": "true"},
                        {"key": "string_function_projection_operator", "value": "concat,substr,left,right,replace"},
                        {"key": "string_function_projection_source_column", "value": "label+segment,label,label,label,label"},
                        {"key": "string_function_projection_output_column", "value": "label_key,prefix,left_edge,right_edge,scrubbed"},
                        {"key": "string_function_projection_literal_count", "value": "1,2,1,1,2"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("label_key", sl.concat(sl.col("label"), "-", sl.col("segment")))
            .with_column("prefix", sl.col("label").substr(1, 3))
            .with_column("left_edge", sl.left(sl.col("label"), 2))
            .with_column("right_edge", sl.col("label").right("2"))
            .with_column("scrubbed", sl.replace(sl.col("label"), "a", ""))
            .filter(sl.concat(sl.col("label"), "-", sl.col("segment")) == "alpha-north")
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "string_function")
        self.assertTrue(report.string_function_runtime_execution)
        self.assertEqual(report.string_function_operator, ("concat",))
        self.assertEqual(report.string_function_source_columns, ("label+segment",))
        self.assertEqual(report.string_function_literal_counts, (2,))
        self.assertEqual(report.string_function_rhs_dtypes, ("utf8",))
        self.assertTrue(report.string_function_projection_runtime_execution)
        self.assertEqual(
            report.string_function_projection_operator,
            ("concat", "substr", "left", "right", "replace"),
        )
        self.assertEqual(
            report.string_function_projection_source_columns,
            ("label+segment", "label", "label", "label", "label"),
        )
        self.assertEqual(
            report.string_function_projection_output_columns,
            ("label_key", "prefix", "left_edge", "right_edge", "scrubbed"),
        )
        self.assertEqual(report.string_function_projection_literal_counts, (1, 2, 1, 1, 2))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_cast_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,CAST(amount AS float64) AS amount_float,CAST(event_date AS date32) AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"amount_float\\":8.0,\\"event_day\\":\\"2026-05-19\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "cast_projection_runtime_execution", "value": "true"},
                        {"key": "cast_projection_source_column", "value": "amount,event_date"},
                        {"key": "cast_projection_output_column", "value": "amount_float,event_day"},
                        {"key": "cast_projection_target_dtype", "value": "float64,date32"},
                        {"key": "cast_projection_mode", "value": "strict,strict"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("amount_float", sl.col("amount").cast("float64"))
            .with_column("event_day", sl.col("event_date").cast("date32"))
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.cast_projection_runtime_execution)
        self.assertEqual(
            report.cast_projection_source_columns, ("amount", "event_date")
        )
        self.assertEqual(
            report.cast_projection_output_columns, ("amount_float", "event_day")
        )
        self.assertEqual(report.cast_projection_target_dtypes, ("float64", "date32"))
        self.assertEqual(report.cast_projection_modes, ("strict", "strict"))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_try_cast_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,TRY_CAST(raw_amount AS int64) AS amount_i64 FROM 'target/input.csv' WHERE TRY_CAST(raw_amount AS int64) >= 10 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source try cast",
                    "human_text": "sql local source try cast",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":3,\\"amount_i64\\":15}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "predicate_operator_family", "value": "cast"},
                        {"key": "cast_runtime_execution", "value": "true"},
                        {"key": "cast_source_column", "value": "raw_amount"},
                        {"key": "cast_target_dtype", "value": "int64"},
                        {"key": "cast_mode", "value": "try"},
                        {"key": "cast_projection_runtime_execution", "value": "true"},
                        {"key": "cast_projection_source_column", "value": "raw_amount"},
                        {"key": "cast_projection_output_column", "value": "amount_i64"},
                        {"key": "cast_projection_target_dtype", "value": "int64"},
                        {"key": "cast_projection_mode", "value": "try"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("amount_i64", sl.try_cast(sl.col("raw_amount"), "int64"))
            .filter(sl.col("raw_amount").try_cast("int64") >= 10)
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.predicate_operator_family, "cast")
        self.assertTrue(report.cast_runtime_execution)
        self.assertEqual(report.cast_source_columns, ("raw_amount",))
        self.assertEqual(report.cast_target_dtypes, ("int64",))
        self.assertEqual(report.cast_modes, ("try",))
        self.assertTrue(report.cast_projection_runtime_execution)
        self.assertEqual(report.cast_projection_source_columns, ("raw_amount",))
        self.assertEqual(report.cast_projection_output_columns, ("amount_i64",))
        self.assertEqual(report.cast_projection_target_dtypes, ("int64",))
        self.assertEqual(report.cast_projection_modes, ("try",))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_temporal_extract_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,DATE_YEAR(CAST(event_date AS date32)) AS event_year,TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) AS event_hour FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"event_year\\":2026,\\"event_hour\\":12}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "date_extract_projection_runtime_execution", "value": "true"},
                        {"key": "date_extract_projection_operator", "value": "date_year"},
                        {"key": "date_extract_projection_source_column", "value": "event_date"},
                        {"key": "date_extract_projection_output_column", "value": "event_year"},
                        {"key": "timestamp_extract_projection_runtime_execution", "value": "true"},
                        {"key": "timestamp_extract_projection_operator", "value": "timestamp_hour"},
                        {"key": "timestamp_extract_projection_source_column", "value": "event_ts"},
                        {"key": "timestamp_extract_projection_output_column", "value": "event_hour"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("event_year", sl.col("event_date").cast("date32").date_year())
            .with_column(
                "event_hour",
                sl.col("event_ts").cast("timestamp_micros").timestamp_hour(),
            )
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.date_extract_projection_runtime_execution)
        self.assertEqual(report.date_extract_projection_operator, ("date_year",))
        self.assertEqual(report.date_extract_projection_source_columns, ("event_date",))
        self.assertEqual(report.date_extract_projection_output_columns, ("event_year",))
        self.assertTrue(report.timestamp_extract_projection_runtime_execution)
        self.assertEqual(report.timestamp_extract_projection_operator, ("timestamp_hour",))
        self.assertEqual(
            report.timestamp_extract_projection_source_columns, ("event_ts",)
        )
        self.assertEqual(
            report.timestamp_extract_projection_output_columns, ("event_hour",)
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_date_arithmetic_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,DATE_ADD_DAYS(CAST(event_date AS date32), 7) AS next_week,DATE_SUB_DAYS(CAST(event_date AS date32), 1) AS prior_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"next_week\\":\\"2026-05-26\\",\\"prior_day\\":\\"2026-05-18\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "date_arithmetic_projection_runtime_execution", "value": "true"},
                        {"key": "date_arithmetic_projection_operator", "value": "date_add_days,date_sub_days"},
                        {"key": "date_arithmetic_projection_days", "value": "7,1"},
                        {"key": "date_arithmetic_projection_source_column", "value": "event_date,event_date"},
                        {"key": "date_arithmetic_projection_output_column", "value": "next_week,prior_day"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column(
                "next_week", sl.col("event_date").cast("date32").date_add_days(7)
            )
            .with_column(
                "prior_day", sl.col("event_date").cast("date32").date_sub_days(1)
            )
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.date_arithmetic_projection_runtime_execution)
        self.assertEqual(
            report.date_arithmetic_projection_operator,
            ("date_add_days", "date_sub_days"),
        )
        self.assertEqual(report.date_arithmetic_projection_days, ("7", "1"))
        self.assertEqual(
            report.date_arithmetic_projection_source_columns,
            ("event_date", "event_date"),
        )
        self.assertEqual(
            report.date_arithmetic_projection_output_columns,
            ("next_week", "prior_day"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_timestamp_arithmetic_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,TIMESTAMP_ADD_SECONDS(CAST(event_ts AS timestamp_micros), 90) AS shifted_ts,TIMESTAMP_SUB_SECONDS(CAST(event_ts AS timestamp_micros), 45) AS prior_ts FROM 'target/input.csv' WHERE TIMESTAMP_ADD_SECONDS(event_ts, 60) >= TIMESTAMP '2026-05-19T12:35:45Z' LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source computed projection",
                    "human_text": "sql local source computed projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"shifted_ts\\":\\"2026-05-19T12:36:15Z\\",\\"prior_ts\\":\\"2026-05-19T12:34:00Z\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "timestamp_arithmetic_runtime_execution", "value": "true"},
                        {"key": "timestamp_arithmetic_operator", "value": "timestamp_add_seconds"},
                        {"key": "timestamp_arithmetic_seconds", "value": "60"},
                        {"key": "timestamp_arithmetic_source_column", "value": "event_ts"},
                        {"key": "timestamp_arithmetic_projection_runtime_execution", "value": "true"},
                        {"key": "timestamp_arithmetic_projection_operator", "value": "timestamp_add_seconds,timestamp_sub_seconds"},
                        {"key": "timestamp_arithmetic_projection_seconds", "value": "90,45"},
                        {"key": "timestamp_arithmetic_projection_source_column", "value": "event_ts,event_ts"},
                        {"key": "timestamp_arithmetic_projection_output_column", "value": "shifted_ts,prior_ts"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column(
                "shifted_ts",
                sl.col("event_ts")
                .cast("timestamp")
                .timestamp_add_seconds(90),
            )
            .with_column(
                "prior_ts",
                sl.col("event_ts")
                .cast("timestamp")
                .timestamp_sub_seconds(45),
            )
            .filter(
                sl.col("event_ts").timestamp_add_seconds(60)
                >= datetime(2026, 5, 19, 12, 35, 45, tzinfo=timezone.utc)
            )
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.timestamp_arithmetic_runtime_execution)
        self.assertEqual(
            report.timestamp_arithmetic_operator,
            ("timestamp_add_seconds",),
        )
        self.assertEqual(report.timestamp_arithmetic_seconds, ("60",))
        self.assertEqual(report.timestamp_arithmetic_source_columns, ("event_ts",))
        self.assertTrue(report.timestamp_arithmetic_projection_runtime_execution)
        self.assertEqual(
            report.timestamp_arithmetic_projection_operator,
            ("timestamp_add_seconds", "timestamp_sub_seconds"),
        )
        self.assertEqual(report.timestamp_arithmetic_projection_seconds, ("90", "45"))
        self.assertEqual(
            report.timestamp_arithmetic_projection_source_columns,
            ("event_ts", "event_ts"),
        )
        self.assertEqual(
            report.timestamp_arithmetic_projection_output_columns,
            ("shifted_ts", "prior_ts"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_temporal_difference_invokes_sql_smoke(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,DATE_DIFF_DAYS(CAST(end_date AS date32), start_date) AS age_days,TIMESTAMP_DIFF_SECONDS(CAST(event_end AS timestamp_micros), CAST(event_ts AS timestamp_micros)) AS elapsed_seconds FROM 'target/input.csv' WHERE DATE_DIFF_DAYS(end_date, start_date) >= 2 LIMIT 10",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source temporal difference projection",
                    "human_text": "sql local source temporal difference projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"age_days\\":2,\\"elapsed_seconds\\":185}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "generic_expression_predicate_runtime_execution", "value": "true"},
                        {"key": "generic_expression_predicate_source_column", "value": "end_date+start_date"},
                        {"key": "generic_expression_predicate_operator_family", "value": "temporal_difference"},
                        {"key": "generic_expression_predicate_binary_operator_count", "value": "0"},
                        {"key": "generic_expression_predicate_comparison_operator", "value": "gte"},
                        {"key": "generic_expression_projection_runtime_execution", "value": "true"},
                        {"key": "generic_expression_projection_source_column", "value": "end_date+start_date,event_end+event_ts"},
                        {"key": "generic_expression_projection_output_column", "value": "age_days,elapsed_seconds"},
                        {"key": "generic_expression_projection_operator_family", "value": "cast+temporal_difference,cast+temporal_difference"},
                        {"key": "generic_expression_projection_binary_operator_count", "value": "0"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column(
                "age_days",
                sl.col("end_date").cast("date32").date_diff_days(sl.col("start_date")),
            )
            .with_column(
                "elapsed_seconds",
                sl.col("event_end")
                .cast("timestamp")
                .timestamp_diff_seconds(sl.col("event_ts").cast("timestamp")),
            )
            .filter(sl.col("end_date").date_diff_days(sl.col("start_date")) >= 2)
            .limit(10)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.generic_expression_predicate_runtime_execution)
        self.assertEqual(
            report.generic_expression_predicate_source_columns,
            ("end_date+start_date",),
        )
        self.assertEqual(
            report.generic_expression_predicate_operator_families,
            ("temporal_difference",),
        )
        self.assertEqual(report.generic_expression_predicate_binary_operator_count, 0)
        self.assertEqual(
            report.generic_expression_predicate_comparison_operators,
            ("gte",),
        )
        self.assertTrue(report.generic_expression_projection_runtime_execution)
        self.assertEqual(
            report.generic_expression_projection_source_columns,
            ("end_date+start_date", "event_end+event_ts"),
        )
        self.assertEqual(
            report.generic_expression_projection_output_columns,
            ("age_days", "elapsed_seconds"),
        )
        self.assertEqual(
            report.generic_expression_projection_operator_families,
            ("cast+temporal_difference", "cast+temporal_difference"),
        )
        self.assertEqual(report.generic_expression_projection_binary_operator_count, 0)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_null_coalesce_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,COALESCE(label, 'unknown') AS label_clean,COALESCE(amount, 0) AS amount_clean,COALESCE(CAST(event_date AS date32), DATE '2026-01-01') AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source null coalesce projection",
                    "human_text": "sql local source null coalesce projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label_clean\\":\\"unknown\\",\\"amount_clean\\":0,\\"event_day\\":\\"2026-01-01\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "null_coalesce_projection_runtime_execution", "value": "true"},
                        {"key": "null_coalesce_projection_source_column", "value": "label,amount,event_date"},
                        {"key": "null_coalesce_projection_output_column", "value": "label_clean,amount_clean,event_day"},
                        {"key": "null_coalesce_projection_fallback_dtype", "value": "utf8,int64,date32"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("label_clean", sl.col("label").fill_null("unknown"))
            .with_column("amount_clean", sl.col("amount").fill_null(0))
            .with_column(
                "event_day",
                sl.col("event_date").cast("date32").fill_null(date(2026, 1, 1)),
            )
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.null_coalesce_projection_runtime_execution)
        self.assertEqual(
            report.null_coalesce_projection_source_columns,
            ("label", "amount", "event_date"),
        )
        self.assertEqual(
            report.null_coalesce_projection_output_columns,
            ("label_clean", "amount_clean", "event_day"),
        )
        self.assertEqual(
            report.null_coalesce_projection_fallback_dtypes,
            ("utf8", "int64", "date32"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_nullif_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,NULLIF(label, 'missing') AS label_clean,NULLIF(amount, 0) AS amount_clean,NULLIF(CAST(event_date AS date32), DATE '2026-01-01') AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source nullif projection",
                    "human_text": "sql local source nullif projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label_clean\\":null,\\"amount_clean\\":null,\\"event_day\\":null}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "nullif_projection_runtime_execution", "value": "true"},
                        {"key": "nullif_projection_source_column", "value": "label,amount,event_date"},
                        {"key": "nullif_projection_output_column", "value": "label_clean,amount_clean,event_day"},
                        {"key": "nullif_projection_sentinel_dtype", "value": "utf8,int64,date32"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("label_clean", sl.col("label").null_if("missing"))
            .with_column("amount_clean", sl.null_if(sl.col("amount"), 0))
            .with_column(
                "event_day",
                sl.col("event_date").cast("date32").null_if(date(2026, 1, 1)),
            )
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.nullif_projection_runtime_execution)
        self.assertEqual(
            report.nullif_projection_source_columns,
            ("label", "amount", "event_date"),
        )
        self.assertEqual(
            report.nullif_projection_output_columns,
            ("label_clean", "amount_clean", "event_day"),
        )
        self.assertEqual(
            report.nullif_projection_sentinel_dtypes,
            ("utf8", "int64", "date32"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_conditional_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 'small' END AS size_band,CASE WHEN event_date >= DATE '2026-01-01' THEN DATE '2026-12-31' ELSE DATE '2025-12-31' END AS cutoff_day,CASE WHEN amount >= 10 THEN preferred_label ELSE fallback_label END AS label_choice FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source conditional projection",
                    "human_text": "sql local source conditional projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"size_band\\":\\"large\\",\\"cutoff_day\\":\\"2026-12-31\\",\\"label_choice\\":\\"preferred-beta\\"}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "conditional_projection_runtime_execution", "value": "true"},
                        {"key": "conditional_projection_predicate_family", "value": "comparison,comparison,comparison"},
                        {"key": "conditional_projection_source_column", "value": "amount,event_date,amount+fallback_label+preferred_label"},
                        {"key": "conditional_projection_output_column", "value": "size_band,cutoff_day,label_choice"},
                        {"key": "conditional_projection_then_dtype", "value": "utf8,date32,utf8"},
                        {"key": "conditional_projection_else_dtype", "value": "utf8,date32,utf8"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("size_band", sl.case_when(sl.col("amount") >= 10, "large", "small"))
            .with_column(
                "cutoff_day",
                sl.case_when(
                    sl.col("event_date") >= date(2026, 1, 1),
                    date(2026, 12, 31),
                    date(2025, 12, 31),
                ),
            )
            .with_column(
                "label_choice",
                sl.case_when(
                    sl.col("amount") >= 10,
                    sl.col("preferred_label"),
                    sl.col("fallback_label"),
                ),
            )
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.conditional_projection_runtime_execution)
        self.assertEqual(
            report.conditional_projection_predicate_families,
            ("comparison", "comparison", "comparison"),
        )
        self.assertEqual(
            report.conditional_projection_source_columns,
            ("amount", "event_date", "amount+fallback_label+preferred_label"),
        )
        self.assertEqual(
            report.conditional_projection_output_columns,
            ("size_band", "cutoff_day", "label_choice"),
        )
        self.assertEqual(
            report.conditional_projection_then_dtypes,
            ("utf8", "date32", "utf8"),
        )
        self.assertEqual(
            report.conditional_projection_else_dtypes,
            ("utf8", "date32", "utf8"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_with_column_predicate_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,amount >= 10 AS is_large,label IS NULL AS missing_label,active IS NOT TRUE AS inactive_or_unknown FROM 'target/input.csv' WHERE id >= 1 LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source predicate projection",
                    "human_text": "sql local source predicate projection",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"is_large\\":true,\\"missing_label\\":false,\\"inactive_or_unknown\\":true}\\n"},
                        {"key": "sql_statement_kind", "value": "local_source_computed_projection_filter_limit"},
                        {"key": "predicate_projection_runtime_execution", "value": "true"},
                        {"key": "predicate_projection_predicate_family", "value": "comparison,null_predicate,boolean_predicate"},
                        {"key": "predicate_projection_source_column", "value": "amount,label,active"},
                        {"key": "predicate_projection_output_column", "value": "is_large,missing_label,inactive_or_unknown"},
                        {"key": "predicate_projection_null_semantics", "value": "sql_three_valued_boolean_or_null_projection,sql_is_null_is_not_null,sql_boolean_is_not_true_false_null_matches"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("is_large", sl.col("amount") >= 10)
            .with_column("missing_label", sl.col("label").is_null())
            .with_column("inactive_or_unknown", sl.col("active").is_not_true())
            .filter(sl.col("id") >= 1)
            .limit(2)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertTrue(report.predicate_projection_runtime_execution)
        self.assertEqual(
            report.predicate_projection_predicate_families,
            ("comparison", "null_predicate", "boolean_predicate"),
        )
        self.assertEqual(
            report.predicate_projection_source_columns,
            ("amount", "label", "active"),
        )
        self.assertEqual(
            report.predicate_projection_output_columns,
            ("is_large", "missing_label", "inactive_or_unknown"),
        )
        self.assertEqual(
            report.predicate_projection_null_semantics,
            (
                "sql_three_valued_boolean_or_null_projection",
                "sql_is_null_is_not_null",
                "sql_boolean_is_not_true_false_null_matches",
            ),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_local_csv_query_builder_write_invokes_sql_smoke_output(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "jsonl",
                    "--output",
                    "target/out.jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_path", "value": "target/out.jsonl"},
                        {"key": "output_format", "value": "jsonl"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "2"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_workspace_path_safety_status", "value": "enforced"},
                        {"key": "output_commit_mode", "value": "staged_replace_with_backup_same_directory"},
                        {"key": "output_commit_status", "value": "committed"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .filter("amount >= 10")
            .limit(2)
            .write("target/out.jsonl", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/out.jsonl")
        self.assertEqual(report.output_format, "jsonl")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_jsonl_sink",
        )
        self.assertEqual(report.workspace_path_safety_status, "enforced")
        self.assertEqual(
            report.output_commit_mode,
            "staged_replace_with_backup_same_directory",
        )
        self.assertEqual(report.output_commit_status, "committed")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_write_csv_invokes_sql_smoke_output(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "csv",
                    "--output",
                    "target/out.csv",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_path", "value": "target/out.csv"},
                        {"key": "output_format", "value": "csv"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_csv_sink"},
                        {"key": "output_certificate_ref", "value": "sql-local-source.csv.local-csv-output.native-io.v1"},
                        {"key": "result_replay_verified", "value": "true"},
                        {"key": "output_replay_status", "value": "verified_local_sink_artifacts"},
                        {"key": "output_replay_millis", "value": "1"},
                        {"key": "output_fidelity_report_status", "value": "scoped_local_output_fidelity_reported"},
                        {"key": "output_fidelity_loss", "value": "csv:csv_text_roundtrip_loses_static_type_metadata"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .write_csv("target/out.csv", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/out.csv")
        self.assertEqual(report.output_format, "csv")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_csv_sink",
        )
        self.assertEqual(
            report.envelope.field("output_certificate_ref"),
            "sql-local-source.csv.local-csv-output.native-io.v1",
        )
        self.assertTrue(report.result_replay_verified)
        self.assertEqual(report.output_replay_status, "verified_local_sink_artifacts")
        self.assertEqual(report.output_replay_millis, 1)
        self.assertEqual(
            report.output_fidelity_report_status,
            "scoped_local_output_fidelity_reported",
        )
        self.assertEqual(
            report.output_fidelity_loss,
            ("csv:csv_text_roundtrip_loses_static_type_metadata",),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_fanout_invokes_sql_smoke_outputs(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "inline-jsonl",
                    "--fanout-output",
                    "jsonl=target/out.jsonl",
                    "--fanout-output",
                    "csv=target/out.csv",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source fanout output",
                    "human_text": "sql local source fanout output",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_route", "value": "local_fanout"},
                        {"key": "output_format", "value": "jsonl"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_fanout_performed", "value": "true"},
                        {"key": "fanout_output_count", "value": "2"},
                        {"key": "fanout_output_formats", "value": "jsonl,csv"},
                        {"key": "fanout_output_paths", "value": "target/out.jsonl,target/out.csv"},
                        {"key": "fanout_output_digests", "value": "jsonl:abc,csv:def"},
                        {"key": "fanout_output_workspace_path_safety_statuses", "value": "jsonl:true,csv:true"},
                        {"key": "fanout_output_commit_modes", "value": "jsonl:atomic_rename_same_directory,csv:atomic_rename_same_directory"},
                        {"key": "fanout_output_native_io_certificate_statuses", "value": "jsonl:certified_local_jsonl_sink,csv:certified_local_csv_sink"},
                        {"key": "fanout_output_replay_statuses", "value": "jsonl:verified_local_file_digest,csv:verified_local_file_digest"},
                        {"key": "fanout_output_fidelity_statuses", "value": "jsonl:logical_rows_replay_verified,csv:logical_rows_replay_verified_type_metadata_not_preserved"},
                        {"key": "fanout_output_fidelity_loss", "value": "jsonl:jsonl_text_roundtrip_not_full_type_metadata_fidelity,csv:csv_text_roundtrip_loses_static_type_metadata"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_fanout_sinks"},
                        {"key": "result_replay_verified", "value": "true"},
                        {"key": "output_replay_status", "value": "verified_local_sink_artifacts"},
                        {"key": "fanout_result_reuse_hit", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .fanout(
                {"jsonl": "target/out.jsonl", "csv": "target/out.csv"},
                allow_overwrite=True,
            )
        )

        self.assertEqual(report.envelope.field("output_route"), "local_fanout")
        self.assertTrue(report.output_io_performed)
        self.assertTrue(report.output_fanout_performed)
        self.assertEqual(report.fanout_output_count, 2)
        self.assertEqual(report.fanout_output_formats, ("jsonl", "csv"))
        self.assertEqual(
            report.fanout_output_paths,
            ("target/out.jsonl", "target/out.csv"),
        )
        self.assertEqual(report.fanout_output_digests, ("jsonl:abc", "csv:def"))
        self.assertEqual(
            report.fanout_output_workspace_path_safety_statuses,
            ("jsonl:true", "csv:true"),
        )
        self.assertEqual(
            report.fanout_output_commit_modes,
            (
                "jsonl:atomic_rename_same_directory",
                "csv:atomic_rename_same_directory",
            ),
        )
        self.assertTrue(report.result_replay_verified)
        self.assertEqual(report.output_replay_status, "verified_local_sink_artifacts")
        self.assertEqual(
            report.fanout_output_replay_statuses,
            ("jsonl:verified_local_file_digest", "csv:verified_local_file_digest"),
        )
        self.assertEqual(
            report.fanout_output_fidelity_statuses,
            (
                "jsonl:logical_rows_replay_verified",
                "csv:logical_rows_replay_verified_type_metadata_not_preserved",
            ),
        )
        self.assertEqual(
            report.fanout_output_fidelity_loss,
            (
                "jsonl:jsonl_text_roundtrip_not_full_type_metadata_fidelity",
                "csv:csv_text_roundtrip_loses_static_type_metadata",
            ),
        )
        self.assertTrue(report.fanout_result_reuse_hit)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_fanout_sinks",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_write_parquet_invokes_sql_smoke_output(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "parquet",
                    "--output",
                    "target/out.parquet",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_path", "value": "target/out.parquet"},
                        {"key": "output_format", "value": "parquet"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_parquet_sink"},
                        {"key": "output_certificate_ref", "value": "sql-local-source.local-parquet-output.native-io.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .write_parquet("target/out.parquet", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/out.parquet")
        self.assertEqual(report.output_format, "parquet")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_parquet_sink",
        )
        self.assertEqual(
            report.envelope.field("output_certificate_ref"),
            "sql-local-source.local-parquet-output.native-io.v1",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_write_arrow_ipc_invokes_sql_smoke_output(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "arrow-ipc",
                    "--output",
                    "target/out.arrow",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_path", "value": "target/out.arrow"},
                        {"key": "output_format", "value": "arrow_ipc"},
                        {"key": "output_row_count", "value": "2"},
                        {"key": "selected_row_count", "value": "3"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_arrow_ipc_sink"},
                        {"key": "output_certificate_ref", "value": "sql-local-source.local-arrow-ipc-output.native-io.v1"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .write_arrow_ipc("target/out.arrow", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/out.arrow")
        self.assertEqual(report.output_format, "arrow_ipc")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_arrow_ipc_sink",
        )
        self.assertEqual(
            report.envelope.field("output_certificate_ref"),
            "sql-local-source.local-arrow-ipc-output.native-io.v1",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_write_vortex_invokes_sql_smoke_output(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "vortex",
                    "--output",
                    "target/out.vortex",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1,\\"label\\":\\"alpha\\"}\\n{\\"id\\":2,\\"label\\":\\"beta\\"}\\n"},
                        {"key": "output_path", "value": "target/out.vortex"},
                        {"key": "output_format", "value": "vortex"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_vortex_sink"},
                        {"key": "output_certificate_ref", "value": "sql-local-source.local-vortex-output.native-io.v1"},
                        {"key": "vortex_output_runtime_execution", "value": "true"},
                        {"key": "vortex_output_reopen_verified", "value": "true"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:1234"},
                        {"key": "vortex_output_row_count", "value": "2"},
                        {"key": "result_replay_verified", "value": "true"},
                        {"key": "output_replay_status", "value": "verified_local_sink_artifacts"},
                        {"key": "output_fidelity_report_status", "value": "scoped_local_output_fidelity_reported"},
                        {"key": "output_fidelity_loss", "value": "vortex:flat_scalar_only_no_broad_vortex_writer_fidelity_claim"},
                        {"key": "upstream_vortex_write_called", "value": "true"},
                        {"key": "upstream_vortex_scan_called", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .write_vortex("target/out.vortex", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/out.vortex")
        self.assertEqual(report.output_format, "vortex")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_vortex_sink",
        )
        self.assertTrue(report.vortex_output_runtime_execution)
        self.assertTrue(report.vortex_output_reopen_verified)
        self.assertEqual(report.vortex_artifact_digest, "fnv64:1234")
        self.assertEqual(report.vortex_output_row_count, 2)
        self.assertTrue(report.result_replay_verified)
        self.assertEqual(report.output_replay_status, "verified_local_sink_artifacts")
        self.assertEqual(
            report.output_fidelity_loss,
            ("vortex:flat_scalar_only_no_broad_vortex_writer_fidelity_claim",),
        )
        self.assertTrue(report.upstream_vortex_write_called)
        self.assertTrue(report.upstream_vortex_scan_called)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_write_avro_and_orc_normalize_formats(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                assert args[0] == "sql-local-source-smoke", args
                output_format = args[args.index("--output-format") + 1]
                output_path = args[args.index("--output") + 1]
                certificate_status = {
                    "avro": "certified_local_avro_sink",
                    "orc": "certified_local_orc_sink",
                }[output_format]
                certificate_ref = {
                    "avro": "sql-local-source.local-avro-output.native-io.v1",
                    "orc": "sql-local-source.local-orc-output.native-io.v1",
                }[output_format]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":1}\\n"},
                        {"key": "output_path", "value": output_path},
                        {"key": "output_format", "value": output_format},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_native_io_certificate_status", "value": certificate_status},
                        {"key": "output_certificate_ref", "value": certificate_ref},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        avro_report = (
            ctx.read_csv("target/input.csv")
            .select(["id"])
            .limit(1)
            .write_avro("target/out.avro", allow_overwrite=True)
        )
        orc_report = (
            ctx.read_csv("target/input.csv")
            .select(["id"])
            .limit(1)
            .write_orc("target/out.orc", allow_overwrite=True)
        )

        self.assertEqual(avro_report.output_format, "avro")
        self.assertEqual(
            avro_report.output_native_io_certificate_status,
            "certified_local_avro_sink",
        )
        self.assertEqual(orc_report.output_format, "orc")
        self.assertEqual(
            orc_report.output_native_io_certificate_status,
            "certified_local_orc_sink",
        )

    def test_local_csv_query_builder_write_parquet_checks_errors_by_default(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,label FROM 'target/input.csv' LIMIT 2",
                    "--output-format",
                    "parquet",
                    "--output",
                    "target/out.parquet",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "error",
                    "summary": "Parquet sink blocked",
                    "human_text": "Parquet sink blocked",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "blocked"}
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        with self.assertRaises(sl.ShardLoomCommandError):
            (
                ctx.read_csv("target/input.csv")
                .select(["id", "label"])
                .limit(2)
                .write_parquet("target/out.parquet", allow_overwrite=True)
            )

    def test_local_csv_query_builder_with_column_literal_write_csv_invokes_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    "SELECT id,'north' AS segment FROM 'target/input.csv' WHERE amount >= 10 LIMIT 2",
                    "--output-format",
                    "csv",
                    "--output",
                    "target/out.csv",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source literal projection csv output",
                    "human_text": "sql local source literal projection csv output",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "result_jsonl", "value": "{\\"id\\":2,\\"segment\\":\\"north\\"}\\n"},
                        {"key": "output_path", "value": "target/out.csv"},
                        {"key": "output_format", "value": "csv"},
                        {"key": "literal_projection_runtime_execution", "value": "true"},
                        {"key": "literal_projection_columns", "value": "segment"},
                        {"key": "output_row_count", "value": "1"},
                        {"key": "selected_row_count", "value": "1"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_csv_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv("target/input.csv")
            .select("id")
            .with_column("segment", "lit('north')")
            .filter(sl.col("amount") >= 10)
            .limit(2)
            .write_csv("target/out.csv", allow_overwrite=True)
        )

        self.assertEqual(report.output_path, "target/out.csv")
        self.assertEqual(report.output_format, "csv")
        self.assertEqual(report.envelope.field("literal_projection_columns"), "segment")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(
            report.output_native_io_certificate_status,
            "certified_local_csv_sink",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_literal_table_write_uses_literal_table_source_kind(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-user-rows-smoke",
                    "target/literal-table.jsonl",
                    "code:utf8,weight:float64",
                    "code=A,weight=1.5;code=B,weight=2.0",
                    "--source-kind",
                    "literal_table",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-user-rows-smoke",
                    "status": "success",
                    "summary": "literal table",
                    "human_text": "literal table",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/literal-table.jsonl"},
                        {"key": "generated_source_kind", "value": "literal_table"},
                        {"key": "generated_source_row_count", "value": "2"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.literal_table(
            [
                {"code": "A", "weight": 1.5},
                {"code": "B", "weight": 2.0},
            ]
        ).write("target/literal-table.jsonl")

        self.assertEqual(report.generated_source_kind, "literal_table")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_calendar_write_generates_date_dimension_rows(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-user-rows-smoke",
                    "target/calendar.jsonl",
                    "dt:utf8,year:int64,month:int64,day:int64,day_of_week:int64",
                    "dt=2026-05-18,year=2026,month=5,day=18,day_of_week=1;dt=2026-05-19,year=2026,month=5,day=19,day_of_week=2",
                    "--source-kind",
                    "calendar",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-user-rows-smoke",
                    "status": "success",
                    "summary": "calendar",
                    "human_text": "calendar",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/calendar.jsonl"},
                        {"key": "generated_source_kind", "value": "calendar"},
                        {"key": "generated_source_row_count", "value": "2"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.calendar(
            "2026-05-18",
            "2026-05-20",
            column="dt",
        ).write("target/calendar.jsonl")

        self.assertEqual(report.generated_source_kind, "calendar")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

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
        with self.assertRaises(ValueError):
            sl.literal_table([], binary=["definitely-missing-shardloom"])
        with self.assertRaises(ValueError):
            sl.calendar(
                "2026-05-20",
                "2026-05-18",
                binary=["definitely-missing-shardloom"],
            )
        source = sl.from_rows([{"id": 1}], binary=["definitely-missing-shardloom"])
        with self.assertRaises(ValueError):
            source.select("missing")
        with self.assertRaises(ValueError):
            source.with_column("bad", "id + 1")
        with self.assertRaises(ValueError):
            source.with_column("bad", "lit(null)")

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

    def test_range_limit_preserves_engine_native_generated_source_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-range-smoke",
                    "target/range-limited.jsonl",
                    "2",
                    "6",
                    "--step",
                    "2",
                    "--column",
                    "id",
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
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
                        {"key": "output_path", "value": "target/range-limited.jsonl"},
                        {"key": "generated_source_kind", "value": "range"},
                        {"key": "generated_source_range_start", "value": "2"},
                        {"key": "generated_source_range_end", "value": "6"},
                        {"key": "generated_source_range_step", "value": "2"},
                        {"key": "generated_source_range_column", "value": "id"},
                        {"key": "generated_source_row_count", "value": "2"},
                        {"key": "generated_source_created", "value": "true"},
                        {"key": "source_io_performed", "value": "false"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.range(2, 8, step=2, column="id").limit(2).write(
            "target/range-limited.jsonl",
            allow_overwrite=True,
        )

        self.assertEqual(report.envelope.command, "generated-source-range-smoke")
        self.assertEqual(report.output_path, "target/range-limited.jsonl")
        self.assertEqual(report.generated_source_kind, "range")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertEqual(report.generated_source_range_start, 2)
        self.assertEqual(report.generated_source_range_end, 6)
        self.assertEqual(report.generated_source_range_step, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_range_limit_aliases_and_validation(self) -> None:
        source = sl.range(10, 0, step=-2, binary=["definitely-missing-shardloom"])

        self.assertEqual(source.limit(2).end, 6)
        self.assertEqual(source.head(1).end, 8)
        self.assertEqual(source.take(0).end, 10)
        self.assertEqual(source.take(100).end, 0)
        with self.assertRaises(TypeError):
            source.limit(True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            source.limit(-1)

    def test_range_filter_with_column_limit_invokes_generated_source_sql_smoke(self) -> None:
        statement = (
            "SELECT value AS id, CASE WHEN value >= 5 THEN 1 ELSE 0 END AS bucket "
            "FROM range(1, 8, 1) WHERE value >= 3 LIMIT 2"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/range-query.jsonl",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql",
                    "human_text": "generated sql",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "output_path", "value": "target/range-query.jsonl"}},
                        {{"key": "generated_source_kind", "value": "sql_generate_series_range"}},
                        {{"key": "generated_source_row_count", "value": "2"}},
                        {{"key": "generated_source_range_start", "value": "1"}},
                        {{"key": "generated_source_range_end", "value": "8"}},
                        {{"key": "generated_source_range_step", "value": "1"}},
                        {{"key": "generated_source_range_column", "value": "value"}},
                        {{"key": "generated_source_sql_generator_function", "value": "range"}},
                        {{"key": "generated_source_range_end_inclusive", "value": "false"}},
                        {{"key": "sql_source_free_filter_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_filter_source_column", "value": "value"}},
                        {{"key": "sql_source_free_filter_predicate", "value": "value>=3"}},
                        {{"key": "sql_source_free_filter_selected_row_count", "value": "5"}},
                        {{"key": "sql_source_free_limit_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_limit_count", "value": "2"}},
                        {{"key": "sql_source_free_projection_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_projection_source_column", "value": "value"}},
                        {{"key": "sql_source_free_projection_columns", "value": "id,bucket"}},
                        {{"key": "sql_source_free_projection_expressions", "value": "value,case(value>=5?1:0)"}},
                        {{"key": "generated_source_created", "value": "true"}},
                        {{"key": "generated_source_certificate_status", "value": "present"}},
                        {{"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.range(1, 8, column="id")
            .filter(sl.col("id") >= 3)
            .with_column("bucket", sl.case_when(sl.col("id") >= 5, 1, 0))
            .limit(2)
            .write("target/range-query.jsonl", allow_overwrite=True)
        )

        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.generated_source_kind, "sql_generate_series_range")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertEqual(report.generated_source_range_start, 1)
        self.assertEqual(report.generated_source_range_end, 8)
        self.assertEqual(report.generated_source_range_step, 1)
        self.assertEqual(report.generated_source_range_column, "value")
        self.assertEqual(report.generated_source_sql_generator_function, "range")
        self.assertFalse(report.generated_source_range_end_inclusive)
        self.assertTrue(report.sql_source_free_filter_runtime_execution)
        self.assertEqual(report.sql_source_free_filter_source_column, "value")
        self.assertEqual(report.sql_source_free_filter_predicate, "value>=3")
        self.assertEqual(report.sql_source_free_filter_selected_row_count, 5)
        self.assertTrue(report.sql_source_free_limit_runtime_execution)
        self.assertEqual(report.sql_source_free_limit_count, 2)
        self.assertTrue(report.sql_source_free_projection_runtime_execution)
        self.assertEqual(report.sql_source_free_projection_source_column, "value")
        self.assertEqual(report.sql_source_free_projection_columns, ("id", "bucket"))
        self.assertEqual(
            report.sql_source_free_projection_expressions,
            ("value", "case(value>=5?1:0)"),
        )
        self.assertEqual(report.output_path, "target/range-query.jsonl")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_range_filter_with_column_sort_limit_invokes_generated_source_sql_smoke(
        self,
    ) -> None:
        statement = (
            "SELECT value AS id, value * 2 AS doubled "
            "FROM range(1, 8, 1) WHERE value >= 3 ORDER BY doubled DESC LIMIT 2"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/range-query-topn.jsonl",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql",
                    "human_text": "generated sql",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "output_path", "value": "target/range-query-topn.jsonl"}},
                        {{"key": "generated_source_kind", "value": "sql_generate_series_range"}},
                        {{"key": "generated_source_row_count", "value": "2"}},
                        {{"key": "generated_source_range_start", "value": "1"}},
                        {{"key": "generated_source_range_end", "value": "8"}},
                        {{"key": "generated_source_range_step", "value": "1"}},
                        {{"key": "generated_source_range_column", "value": "value"}},
                        {{"key": "generated_source_sql_generator_function", "value": "range"}},
                        {{"key": "generated_source_range_end_inclusive", "value": "false"}},
                        {{"key": "sql_source_free_filter_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_filter_source_column", "value": "value"}},
                        {{"key": "sql_source_free_filter_predicate", "value": "value>=3"}},
                        {{"key": "sql_source_free_filter_selected_row_count", "value": "5"}},
                        {{"key": "sql_source_free_order_by_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_top_n_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_sort_operator_family", "value": "single_key_int64_topn"}},
                        {{"key": "sql_source_free_sort_keys", "value": "doubled"}},
                        {{"key": "sql_source_free_sort_direction", "value": "desc"}},
                        {{"key": "sql_source_free_sort_input_row_count", "value": "5"}},
                        {{"key": "sql_source_free_top_n_limit", "value": "2"}},
                        {{"key": "sql_source_free_limit_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_limit_count", "value": "2"}},
                        {{"key": "sql_source_free_projection_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_projection_source_column", "value": "value"}},
                        {{"key": "sql_source_free_projection_columns", "value": "id,doubled"}},
                        {{"key": "sql_source_free_projection_expressions", "value": "value,value*2"}},
                        {{"key": "generated_source_created", "value": "true"}},
                        {{"key": "generated_source_certificate_status", "value": "present"}},
                        {{"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.range(1, 8, column="id")
            .filter(sl.col("id") >= 3)
            .with_column("doubled", sl.col("id") * 2)
            .sort("doubled", descending=True)
            .limit(2)
            .write("target/range-query-topn.jsonl", allow_overwrite=True)
        )

        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.generated_source_kind, "sql_generate_series_range")
        self.assertEqual(report.generated_source_row_count, 2)
        self.assertTrue(report.sql_source_free_filter_runtime_execution)
        self.assertTrue(report.sql_source_free_order_by_runtime_execution)
        self.assertTrue(report.sql_source_free_top_n_runtime_execution)
        self.assertEqual(
            report.sql_source_free_sort_operator_family,
            "single_key_int64_topn",
        )
        self.assertEqual(report.sql_source_free_sort_keys, ("doubled",))
        self.assertEqual(report.sql_source_free_sort_direction, ("desc",))
        self.assertEqual(report.sql_source_free_sort_input_row_count, 5)
        self.assertEqual(report.sql_source_free_top_n_limit, 2)
        self.assertTrue(report.sql_source_free_limit_runtime_execution)
        self.assertEqual(report.sql_source_free_limit_count, 2)
        self.assertTrue(report.sql_source_free_projection_runtime_execution)
        self.assertEqual(report.sql_source_free_projection_columns, ("id", "doubled"))
        self.assertEqual(
            report.sql_source_free_projection_expressions,
            ("value", "value*2"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_range_filter_with_column_sort_limit_fanout_invokes_generated_source_sql_smoke(
        self,
    ) -> None:
        statement = (
            "SELECT value AS id, value * 2 AS doubled "
            "FROM range(1, 8, 1) WHERE value >= 3 ORDER BY doubled DESC LIMIT 2"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/range-query-topn.jsonl",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--fanout-output",
                    "csv=target/range-query-topn.csv",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql fanout",
                    "human_text": "generated sql fanout",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "output_path", "value": "target/range-query-topn.jsonl"}},
                        {{"key": "output_route", "value": "local_sink_and_fanout"}},
                        {{"key": "generated_source_kind", "value": "sql_generate_series_range"}},
                        {{"key": "generated_source_row_count", "value": "2"}},
                        {{"key": "sql_source_free_order_by_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_top_n_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_sort_operator_family", "value": "single_key_int64_topn"}},
                        {{"key": "sql_source_free_sort_keys", "value": "doubled"}},
                        {{"key": "sql_source_free_sort_direction", "value": "desc"}},
                        {{"key": "output_io_performed", "value": "true"}},
                        {{"key": "output_fanout_performed", "value": "true"}},
                        {{"key": "result_reuse_for_fanout", "value": "true"}},
                        {{"key": "fanout_result_reuse_hit", "value": "true"}},
                        {{"key": "result_replay_verified", "value": "true"}},
                        {{"key": "output_replay_status", "value": "verified_local_sink_artifacts"}},
                        {{"key": "output_replay_millis", "value": "1"}},
                        {{"key": "output_fidelity_report_status", "value": "scoped_local_output_fidelity_reported"}},
                        {{"key": "output_fidelity_loss", "value": "jsonl:jsonl_text_roundtrip_not_full_type_metadata_fidelity,csv:csv_text_roundtrip_loses_static_type_metadata"}},
                        {{"key": "fanout_output_count", "value": "1"}},
                        {{"key": "fanout_output_formats", "value": "csv"}},
                        {{"key": "fanout_output_paths", "value": "target/range-query-topn.csv"}},
                        {{"key": "fanout_output_digests", "value": "csv:fnv64:abc"}},
                        {{"key": "fanout_output_workspace_path_safety_statuses", "value": "csv:true"}},
                        {{"key": "fanout_output_commit_modes", "value": "csv:atomic_rename_same_directory"}},
                        {{"key": "fanout_output_native_io_certificate_statuses", "value": "csv:certified_local_file_sink"}},
                        {{"key": "fanout_output_replay_statuses", "value": "csv:verified_local_file_digest"}},
                        {{"key": "fanout_output_fidelity_statuses", "value": "csv:logical_rows_replay_verified_type_metadata_not_preserved"}},
                        {{"key": "fanout_output_fidelity_loss", "value": "csv:csv_text_roundtrip_loses_static_type_metadata"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.range(1, 8, column="id")
            .filter(sl.col("id") >= 3)
            .with_column("doubled", sl.col("id") * 2)
            .sort("doubled", descending=True)
            .limit(2)
            .fanout(
                {"jsonl": "target/range-query-topn.jsonl", "csv": "target/range-query-topn.csv"},
                allow_overwrite=True,
            )
        )

        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.output_route, "local_sink_and_fanout")
        self.assertTrue(report.output_fanout_performed)
        self.assertTrue(report.result_reuse_for_fanout)
        self.assertTrue(report.fanout_result_reuse_hit)
        self.assertTrue(report.result_replay_verified)
        self.assertEqual(report.output_replay_status, "verified_local_sink_artifacts")
        self.assertEqual(report.output_replay_millis, 1)
        self.assertEqual(
            report.output_fidelity_report_status,
            "scoped_local_output_fidelity_reported",
        )
        self.assertEqual(
            report.output_fidelity_loss,
            (
                "jsonl:jsonl_text_roundtrip_not_full_type_metadata_fidelity",
                "csv:csv_text_roundtrip_loses_static_type_metadata",
            ),
        )
        self.assertEqual(report.fanout_output_count, 1)
        self.assertEqual(report.fanout_output_formats, ("csv",))
        self.assertEqual(report.fanout_output_paths, ("target/range-query-topn.csv",))
        self.assertEqual(report.fanout_output_digests, ("csv:fnv64:abc",))
        self.assertEqual(
            report.fanout_output_workspace_path_safety_statuses,
            ("csv:true",),
        )
        self.assertEqual(
            report.fanout_output_commit_modes,
            ("csv:atomic_rename_same_directory",),
        )
        self.assertEqual(
            report.fanout_output_native_io_certificate_statuses,
            ("csv:certified_local_file_sink",),
        )
        self.assertEqual(
            report.fanout_output_replay_statuses,
            ("csv:verified_local_file_digest",),
        )
        self.assertEqual(
            report.fanout_output_fidelity_statuses,
            ("csv:logical_rows_replay_verified_type_metadata_not_preserved",),
        )
        self.assertEqual(
            report.fanout_output_fidelity_loss,
            ("csv:csv_text_roundtrip_loses_static_type_metadata",),
        )
        self.assertTrue(report.sql_source_free_order_by_runtime_execution)
        self.assertTrue(report.sql_source_free_top_n_runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_range_validates_scoped_generated_source_inputs(self) -> None:
        with self.assertRaises(TypeError):
            sl.range(True, 10, binary=["definitely-missing-shardloom"])
        with self.assertRaises(TypeError):
            sl.range(0, "10", binary=["definitely-missing-shardloom"])  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            sl.range(0, 10, step=0, binary=["definitely-missing-shardloom"])
        with self.assertRaises(ValueError):
            sl.range(0, 10, column="", binary=["definitely-missing-shardloom"])

    def test_sequence_write_invokes_engine_native_generated_source_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sequence-smoke",
                    "target/sequence.jsonl",
                    "1",
                    "6",
                    "--step",
                    "2",
                    "--column",
                    "seq",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sequence-smoke",
                    "status": "success",
                    "summary": "generated sequence",
                    "human_text": "generated sequence",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/sequence.jsonl"},
                        {"key": "generated_source_kind", "value": "sequence"},
                        {"key": "generated_source_range_start", "value": "1"},
                        {"key": "generated_source_range_end", "value": "6"},
                        {"key": "generated_source_range_step", "value": "2"},
                        {"key": "generated_source_range_column", "value": "seq"},
                        {"key": "generated_source_row_count", "value": "3"},
                        {"key": "generated_source_created", "value": "true"},
                        {"key": "source_io_performed", "value": "false"},
                        {"key": "output_io_performed", "value": "true"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sequence(1, 6, step=2, column="seq").write("target/sequence.jsonl")

        self.assertEqual(report.envelope.command, "generated-source-sequence-smoke")
        self.assertEqual(report.output_path, "target/sequence.jsonl")
        self.assertEqual(report.generated_source_kind, "sequence")
        self.assertEqual(report.generated_source_row_count, 3)
        self.assertEqual(report.generated_source_range_start, 1)
        self.assertEqual(report.generated_source_range_end, 6)
        self.assertEqual(report.generated_source_range_step, 2)
        self.assertEqual(report.generated_source_range_column, "seq")
        self.assertEqual(report.generated_source_certificate_status, "present")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_sql_source_free_write_invokes_generated_source_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/sql-values.jsonl",
                    "VALUES (1, 'alpha')",
                    "--output-format",
                    "jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql",
                    "human_text": "generated sql",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/sql-values.jsonl"},
                        {"key": "generated_source_kind", "value": "sql_values"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_created", "value": "true"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql_values("VALUES (1, 'alpha')").write("target/sql-values.jsonl")

        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.generated_source_kind, "sql_values")
        self.assertEqual(report.generated_source_row_count, 1)
        self.assertEqual(report.generated_source_certificate_status, "present")
        self.assertEqual(report.output_native_io_certificate_status, "certified_local_file_sink")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_context_sql_source_free_write_invokes_generated_source_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/sql-select.jsonl",
                    "SELECT 1 AS id, 'alpha' AS label",
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql",
                    "human_text": "generated sql",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/sql-select.jsonl"},
                        {"key": "generated_source_kind", "value": "sql_literal_select"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_created", "value": "true"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        workflow = ctx.sql("SELECT 1 AS id, 'alpha' AS label", check=False)
        report = workflow.write("target/sql-select.jsonl", allow_overwrite=True)

        self.assertIsInstance(workflow, sl.SqlWorkflow)
        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.generated_source_kind, "sql_literal_select")
        self.assertEqual(report.output_path, "target/sql-select.jsonl")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_context_sql_source_free_fanout_invokes_generated_source_sql_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/sql-select.jsonl",
                    "SELECT 1 AS id, 'alpha' AS label",
                    "--output-format",
                    "jsonl",
                    "--fanout-output",
                    "csv=target/sql-select.csv",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql fanout",
                    "human_text": "generated sql fanout",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": "target/sql-select.jsonl"},
                        {"key": "output_route", "value": "local_sink_and_fanout"},
                        {"key": "generated_source_kind", "value": "sql_literal_select"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "output_fanout_performed", "value": "true"},
                        {"key": "fanout_output_count", "value": "1"},
                        {"key": "fanout_output_formats", "value": "csv"},
                        {"key": "fanout_result_reuse_hit", "value": "true"},
                        {"key": "result_replay_verified", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql("SELECT 1 AS id, 'alpha' AS label", check=False).fanout(
            (("jsonl", "target/sql-select.jsonl"), ("csv", "target/sql-select.csv")),
            allow_overwrite=True,
        )

        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.output_route, "local_sink_and_fanout")
        self.assertTrue(report.output_fanout_performed)
        self.assertEqual(report.fanout_output_count, 1)
        self.assertEqual(report.fanout_output_formats, ("csv",))
        self.assertTrue(report.fanout_result_reuse_hit)
        self.assertTrue(report.result_replay_verified)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_context_sql_generate_series_projection_write_invokes_generated_source_sql_smoke(
        self,
    ) -> None:
        statement = "SELECT value AS id, value + 10 AS shifted, CASE WHEN value >= 6 THEN 1 ELSE 0 END AS is_high FROM generate_series(2, 8, 2)"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/sql-generate-series.jsonl",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql",
                    "human_text": "generated sql",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "output_path", "value": "target/sql-generate-series.jsonl"}},
                        {{"key": "generated_source_kind", "value": "sql_generate_series_range"}},
                        {{"key": "generated_source_row_count", "value": "4"}},
                        {{"key": "generated_source_range_start", "value": "2"}},
                        {{"key": "generated_source_range_end", "value": "8"}},
                        {{"key": "generated_source_range_step", "value": "2"}},
                        {{"key": "generated_source_range_column", "value": "value"}},
                        {{"key": "generated_source_sql_generator_function", "value": "generate_series"}},
                        {{"key": "generated_source_range_end_inclusive", "value": "true"}},
                        {{"key": "sql_source_free_projection_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_projection_source_column", "value": "value"}},
                        {{"key": "sql_source_free_projection_columns", "value": "id,shifted,is_high"}},
                        {{"key": "sql_source_free_projection_expressions", "value": "value,value+10,case(value>=6?1:0)"}},
                        {{"key": "generated_source_created", "value": "true"}},
                        {{"key": "generated_source_certificate_status", "value": "present"}},
                        {{"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        workflow = ctx.sql(statement, check=False)
        report = workflow.write("target/sql-generate-series.jsonl", allow_overwrite=True)

        self.assertIsInstance(workflow, sl.SqlWorkflow)
        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.generated_source_kind, "sql_generate_series_range")
        self.assertEqual(report.generated_source_row_count, 4)
        self.assertEqual(report.generated_source_range_start, 2)
        self.assertEqual(report.generated_source_range_end, 8)
        self.assertEqual(report.generated_source_range_step, 2)
        self.assertEqual(report.generated_source_range_column, "value")
        self.assertEqual(report.generated_source_sql_generator_function, "generate_series")
        self.assertTrue(report.generated_source_range_end_inclusive)
        self.assertTrue(report.sql_source_free_projection_runtime_execution)
        self.assertEqual(report.sql_source_free_projection_source_column, "value")
        self.assertEqual(
            report.sql_source_free_projection_columns,
            ("id", "shifted", "is_high"),
        )
        self.assertEqual(
            report.sql_source_free_projection_expressions,
            ("value", "value+10", "case(value>=6?1:0)"),
        )
        self.assertEqual(report.output_path, "target/sql-generate-series.jsonl")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_context_sql_generate_series_filter_limit_write_invokes_generated_source_sql_smoke(
        self,
    ) -> None:
        statement = "SELECT value AS id FROM range(1, 8) WHERE value >= 3 LIMIT 2"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-sql-smoke",
                    "target/sql-range-filter-limit.jsonl",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-sql-smoke",
                    "status": "success",
                    "summary": "generated sql",
                    "human_text": "generated sql",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "output_path", "value": "target/sql-range-filter-limit.jsonl"}},
                        {{"key": "generated_source_kind", "value": "sql_generate_series_range"}},
                        {{"key": "generated_source_row_count", "value": "2"}},
                        {{"key": "sql_source_free_filter_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_filter_source_column", "value": "value"}},
                        {{"key": "sql_source_free_filter_predicate", "value": "value>=3"}},
                        {{"key": "sql_source_free_filter_selected_row_count", "value": "5"}},
                        {{"key": "sql_source_free_limit_runtime_execution", "value": "true"}},
                        {{"key": "sql_source_free_limit_count", "value": "2"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        workflow = ctx.sql(statement, check=False)
        report = workflow.write("target/sql-range-filter-limit.jsonl", allow_overwrite=True)

        self.assertIsInstance(workflow, sl.SqlWorkflow)
        self.assertEqual(report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(report.generated_source_kind, "sql_generate_series_range")
        self.assertTrue(report.sql_source_free_filter_runtime_execution)
        self.assertEqual(report.sql_source_free_filter_predicate, "value>=3")
        self.assertTrue(report.sql_source_free_limit_runtime_execution)
        self.assertEqual(report.sql_source_free_limit_count, 2)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_generated_source_write_csv_helpers_invoke_generated_source_smokes(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                command = sys.argv[1]
                if command == "generated-source-range-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-range-smoke",
                        "target/range.csv",
                        "1",
                        "4",
                        "--step",
                        "1",
                        "--column",
                        "id",
                        "--output-format",
                        "csv",
                        "--allow-overwrite",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/range.csv"},
                        {"key": "output_format", "value": "csv"},
                        {"key": "generated_source_kind", "value": "range"},
                        {"key": "generated_source_row_count", "value": "3"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                elif command == "generated-source-sql-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-sql-smoke",
                        "target/sql-values.csv",
                        "VALUES (1, 'alpha')",
                        "--output-format",
                        "csv",
                        "--allow-overwrite",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/sql-values.csv"},
                        {"key": "output_format", "value": "csv"},
                        {"key": "generated_source_kind", "value": "sql_values"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_file_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                else:
                    raise AssertionError(sys.argv)

                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "generated source",
                    "human_text": "generated source",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        range_report = ctx.range(1, 4, column="id").write_csv(
            "target/range.csv",
            allow_overwrite=True,
        )
        sql_report = ctx.sql_values("VALUES (1, 'alpha')").write_csv(
            "target/sql-values.csv",
            allow_overwrite=True,
        )

        self.assertEqual(range_report.envelope.command, "generated-source-range-smoke")
        self.assertEqual(range_report.output_path, "target/range.csv")
        self.assertEqual(range_report.output_format, "csv")
        self.assertEqual(range_report.generated_source_kind, "range")
        self.assertFalse(range_report.fallback_attempted)
        self.assertFalse(range_report.external_engine_invoked)

        self.assertEqual(sql_report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(sql_report.output_path, "target/sql-values.csv")
        self.assertEqual(sql_report.output_format, "csv")
        self.assertEqual(sql_report.generated_source_kind, "sql_values")
        self.assertFalse(sql_report.fallback_attempted)
        self.assertFalse(sql_report.external_engine_invoked)

    def test_generated_source_structured_output_helpers_invoke_generated_source_smokes(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                command = sys.argv[1]
                if command == "generated-source-user-rows-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-user-rows-smoke",
                        "target/generated.parquet",
                        "id:int64,label:utf8",
                        "id=1,label=alpha",
                        "--source-kind",
                        "user_rows",
                        "--output-format",
                        "parquet",
                        "--allow-overwrite",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/generated.parquet"},
                        {"key": "output_format", "value": "parquet"},
                        {"key": "generated_source_kind", "value": "user_rows"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_parquet_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                elif command == "generated-source-range-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-range-smoke",
                        "target/range.arrow",
                        "1",
                        "4",
                        "--step",
                        "1",
                        "--column",
                        "id",
                        "--output-format",
                        "arrow-ipc",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/range.arrow"},
                        {"key": "output_format", "value": "arrow_ipc"},
                        {"key": "generated_source_kind", "value": "range"},
                        {"key": "generated_source_row_count", "value": "3"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_arrow_ipc_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                elif command == "generated-source-sql-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-sql-smoke",
                        "target/sql.orc",
                        "VALUES (1, 'alpha')",
                        "--output-format",
                        "orc",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/sql.orc"},
                        {"key": "output_format", "value": "orc"},
                        {"key": "generated_source_kind", "value": "sql_values"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_orc_sink"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                else:
                    raise AssertionError(sys.argv)

                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "generated source structured output",
                    "human_text": "generated source structured output",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        rows_report = ctx.from_rows([{"id": 1, "label": "alpha"}]).write_parquet(
            "target/generated.parquet",
            allow_overwrite=True,
        )
        range_report = ctx.range(1, 4, column="id").write_arrow_ipc("target/range.arrow")
        sql_report = ctx.sql_values("VALUES (1, 'alpha')").write_orc("target/sql.orc")

        self.assertEqual(rows_report.envelope.command, "generated-source-user-rows-smoke")
        self.assertEqual(rows_report.output_format, "parquet")
        self.assertEqual(
            rows_report.output_native_io_certificate_status,
            "certified_local_parquet_sink",
        )
        self.assertFalse(rows_report.fallback_attempted)
        self.assertFalse(rows_report.external_engine_invoked)

        self.assertEqual(range_report.envelope.command, "generated-source-range-smoke")
        self.assertEqual(range_report.output_format, "arrow_ipc")
        self.assertEqual(
            range_report.output_native_io_certificate_status,
            "certified_local_arrow_ipc_sink",
        )
        self.assertFalse(range_report.fallback_attempted)
        self.assertFalse(range_report.external_engine_invoked)

        self.assertEqual(sql_report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(sql_report.output_format, "orc")
        self.assertEqual(
            sql_report.output_native_io_certificate_status,
            "certified_local_orc_sink",
        )
        self.assertFalse(sql_report.fallback_attempted)
        self.assertFalse(sql_report.external_engine_invoked)

    def test_generated_source_write_vortex_invokes_vortex_sink_and_exposes_evidence(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                command = sys.argv[1]
                if command == "generated-source-user-rows-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-user-rows-smoke",
                        "target/generated.vortex",
                        "id:int64,label:utf8",
                        "id=1,label=alpha",
                        "--source-kind",
                        "user_rows",
                        "--output-format",
                        "vortex",
                        "--allow-overwrite",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/generated.vortex"},
                        {"key": "output_format", "value": "vortex"},
                        {"key": "generated_source_kind", "value": "user_rows"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_vortex_sink"},
                        {"key": "vortex_output_runtime_execution", "value": "true"},
                        {"key": "vortex_output_reopen_verified", "value": "true"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:abc"},
                        {"key": "vortex_output_row_count", "value": "1"},
                        {"key": "upstream_vortex_write_called", "value": "true"},
                        {"key": "upstream_vortex_scan_called", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                elif command == "generated-source-range-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-range-smoke",
                        "target/range.vortex",
                        "1",
                        "3",
                        "--step",
                        "1",
                        "--column",
                        "id",
                        "--output-format",
                        "vortex",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/range.vortex"},
                        {"key": "output_format", "value": "vortex"},
                        {"key": "generated_source_kind", "value": "range"},
                        {"key": "generated_source_row_count", "value": "2"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_vortex_sink"},
                        {"key": "vortex_output_runtime_execution", "value": "true"},
                        {"key": "vortex_output_reopen_verified", "value": "true"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:def"},
                        {"key": "vortex_output_row_count", "value": "2"},
                        {"key": "upstream_vortex_write_called", "value": "true"},
                        {"key": "upstream_vortex_scan_called", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                elif command == "generated-source-sql-smoke":
                    assert sys.argv[1:] == [
                        "generated-source-sql-smoke",
                        "target/sql.vortex",
                        "VALUES (1, 'alpha')",
                        "--output-format",
                        "vortex",
                        "--format",
                        "json",
                    ], sys.argv
                    fields = [
                        {"key": "output_path", "value": "target/sql.vortex"},
                        {"key": "output_format", "value": "vortex"},
                        {"key": "generated_source_kind", "value": "sql_values"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_vortex_sink"},
                        {"key": "vortex_output_runtime_execution", "value": "true"},
                        {"key": "vortex_output_reopen_verified", "value": "true"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:fed"},
                        {"key": "vortex_output_row_count", "value": "1"},
                        {"key": "upstream_vortex_write_called", "value": "true"},
                        {"key": "upstream_vortex_scan_called", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                else:
                    raise AssertionError(sys.argv)

                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "generated source Vortex output",
                    "human_text": "generated source Vortex output",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        rows_report = ctx.from_rows([{"id": 1, "label": "alpha"}]).write_vortex(
            "target/generated.vortex",
            allow_overwrite=True,
        )
        range_report = ctx.range(1, 3, column="id").write_vortex("target/range.vortex")
        sql_report = ctx.sql_values("VALUES (1, 'alpha')").write_vortex("target/sql.vortex")

        self.assertEqual(rows_report.output_format, "vortex")
        self.assertEqual(
            rows_report.output_native_io_certificate_status,
            "certified_local_vortex_sink",
        )
        self.assertTrue(rows_report.vortex_output_runtime_execution)
        self.assertTrue(rows_report.vortex_output_reopen_verified)
        self.assertEqual(rows_report.vortex_artifact_digest, "fnv64:abc")
        self.assertEqual(rows_report.vortex_output_row_count, 1)
        self.assertTrue(rows_report.upstream_vortex_write_called)
        self.assertTrue(rows_report.upstream_vortex_scan_called)
        self.assertFalse(rows_report.fallback_attempted)
        self.assertFalse(rows_report.external_engine_invoked)

        self.assertEqual(range_report.envelope.command, "generated-source-range-smoke")
        self.assertEqual(range_report.output_format, "vortex")
        self.assertTrue(range_report.vortex_output_runtime_execution)
        self.assertEqual(range_report.vortex_artifact_digest, "fnv64:def")
        self.assertFalse(range_report.external_engine_invoked)

        self.assertEqual(sql_report.envelope.command, "generated-source-sql-smoke")
        self.assertEqual(sql_report.output_format, "vortex")
        self.assertTrue(sql_report.vortex_output_runtime_execution)
        self.assertEqual(sql_report.vortex_artifact_digest, "fnv64:fed")
        self.assertFalse(sql_report.fallback_attempted)

    def test_context_sql_local_source_collect_invokes_sql_smoke(self) -> None:
        statement = "SELECT id FROM 'target/input.csv' WHERE id >= 1 LIMIT 2"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned 2 bounded row(s)",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":1}}\\n{{\\"id\\":2}}\\n"}},
                        {{"key": "output_row_count", "value": "2"}},
                        {{"key": "selected_row_count", "value": "2"}},
                        {{"key": "predicate_operator_family", "value": "comparison"}},
                        {{"key": "output_io_performed", "value": "false"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql(statement).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.output_row_count, 2)
        self.assertEqual(report.selected_row_count, 2)
        self.assertEqual(report.predicate_operator_family, "comparison")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_context_sql_local_source_write_invokes_sql_smoke(self) -> None:
        statement = "SELECT id FROM 'target/input.csv' WHERE id >= 1 LIMIT 2"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--output",
                    "target/sql-local.jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned 2 bounded row(s)",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":1}}\\n{{\\"id\\":2}}\\n"}},
                        {{"key": "output_path", "value": "target/sql-local.jsonl"}},
                        {{"key": "output_row_count", "value": "2"}},
                        {{"key": "selected_row_count", "value": "2"}},
                        {{"key": "output_io_performed", "value": "true"}},
                        {{"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql(statement).write("target/sql-local.jsonl", allow_overwrite=True)

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.output_path, "target/sql-local.jsonl")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(report.output_native_io_certificate_status, "certified_local_jsonl_sink")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_read_json_query_builder_projection_filter_limit_invokes_sql_smoke(self) -> None:
        statement = "SELECT id,label FROM 'target/input.jsonl' WHERE amount >= 10 LIMIT 2"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--output",
                    "target/json-result.jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned 2 bounded row(s)",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":2,\\"label\\":\\"beta\\"}}\\n{{\\"id\\":3,\\"label\\":\\"gamma\\"}}\\n"}},
                        {{"key": "output_path", "value": "target/json-result.jsonl"}},
                        {{"key": "output_row_count", "value": "2"}},
                        {{"key": "selected_row_count", "value": "2"}},
                        {{"key": "source_format", "value": "jsonl"}},
                        {{"key": "source_io_performed", "value": "true"}},
                        {{"key": "source_state_id", "value": "source-state-jsonl-1"}},
                        {{"key": "source_state_digest", "value": "sha256:jsonl-source-state"}},
                        {{"key": "output_io_performed", "value": "true"}},
                        {{"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_json("target/input.jsonl")
            .select("id", "label")
            .filter("amount >= 10")
            .limit(2)
            .write("target/json-result.jsonl", allow_overwrite=True)
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.output_path, "target/json-result.jsonl")
        self.assertEqual(report.output_row_count, 2)
        self.assertEqual(report.selected_row_count, 2)
        self.assertEqual(report.envelope.field("source_format"), "jsonl")
        self.assertTrue(report.envelope.field_bool("source_io_performed", False))
        self.assertEqual(report.envelope.field("source_state_id"), "source-state-jsonl-1")
        self.assertTrue(report.output_io_performed)
        self.assertEqual(report.output_native_io_certificate_status, "certified_local_jsonl_sink")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_read_json_query_builder_scalar_aggregate_invokes_sql_smoke(self) -> None:
        statement = (
            "SELECT count(*),sum(amount),avg(amount) FROM 'target/input.jsonl' "
            "WHERE amount >= 10 LIMIT 1"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned one aggregate row",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"count_all\\":2,\\"sum_amount\\":36,\\"avg_amount\\":18.0}}\\n"}},
                        {{"key": "source_format", "value": "jsonl"}},
                        {{"key": "sql_statement_kind", "value": "local_source_aggregate_filter_limit"}},
                        {{"key": "aggregate_runtime_execution", "value": "true"}},
                        {{"key": "aggregate_operator_family", "value": "scalar_aggregate"}},
                        {{"key": "aggregate_functions", "value": "count(*),sum(amount),avg(amount)"}},
                        {{"key": "source_certificate_ref", "value": "sql-local-source.jsonl.compatibility-source.v1"}},
                        {{"key": "execution_certificate_ref", "value": "sql-local-source.jsonl.aggregate-filter-limit.execution.v1"}},
                        {{"key": "materialization_boundary", "value": "local_jsonl_row_materialization_to_expression_semantics"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        aggregate_workflow = (
            ctx.read_json("target/input.jsonl")
            .filter("amount >= 10")
            .aggregate("count(*)", "sum(amount)", "avg(amount)")
        )
        self.assertIsInstance(aggregate_workflow, sl.LazyFrame)
        report = aggregate_workflow.limit(1).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("source_format"), "jsonl")
        self.assertEqual(
            report.result_jsonl,
            '{"count_all":2,"sum_amount":36,"avg_amount":18.0}\n',
        )
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "scalar_aggregate")
        self.assertEqual(report.aggregate_functions, ("count(*)", "sum(amount)", "avg(amount)"))
        self.assertEqual(
            report.envelope.field("execution_certificate_ref"),
            "sql-local-source.jsonl.aggregate-filter-limit.execution.v1",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_read_json_query_builder_group_by_aggregate_invokes_sql_smoke(self) -> None:
        statement = (
            "SELECT label,count(*),sum(amount) FROM 'target/input.jsonl' "
            "WHERE amount >= 10 GROUP BY label LIMIT 10"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "jsonl",
                    "--output",
                    "target/json-grouped.jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned grouped aggregate rows",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"label\\":\\"beta\\",\\"count_all\\":2,\\"sum_amount\\":36}}\\n"}},
                        {{"key": "output_path", "value": "target/json-grouped.jsonl"}},
                        {{"key": "source_format", "value": "jsonl"}},
                        {{"key": "aggregate_runtime_execution", "value": "true"}},
                        {{"key": "aggregate_operator_family", "value": "grouped_aggregate"}},
                        {{"key": "aggregate_functions", "value": "count(*),sum(amount)"}},
                        {{"key": "group_by_runtime_execution", "value": "true"}},
                        {{"key": "group_by_columns", "value": "label"}},
                        {{"key": "group_by_group_count", "value": "1"}},
                        {{"key": "output_io_performed", "value": "true"}},
                        {{"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"}},
                        {{"key": "execution_certificate_ref", "value": "sql-local-source.jsonl.group-by-aggregate-filter-limit.execution.v1"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        grouped_workflow = (
            ctx.read_json("target/input.jsonl")
            .filter("amount >= 10")
            .group_by("label")
            .agg("count(*)", "sum(amount)")
        )
        self.assertIsInstance(grouped_workflow, sl.LazyFrame)
        report = grouped_workflow.limit(10).write(
            "target/json-grouped.jsonl",
            allow_overwrite=True,
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.output_path, "target/json-grouped.jsonl")
        self.assertTrue(report.aggregate_runtime_execution)
        self.assertEqual(report.aggregate_operator_family, "grouped_aggregate")
        self.assertTrue(report.group_by_runtime_execution)
        self.assertEqual(report.group_by_columns, ("label",))
        self.assertEqual(report.group_by_group_count, 1)
        self.assertTrue(report.output_io_performed)
        self.assertEqual(report.output_native_io_certificate_status, "certified_local_jsonl_sink")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_read_json_query_builder_order_by_topn_invokes_sql_smoke(self) -> None:
        statement = (
            "SELECT id,label FROM 'target/input.jsonl' WHERE amount >= 0 "
            "ORDER BY amount DESC LIMIT 2"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned sorted rows",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":3,\\"label\\":\\"gamma\\"}}\\n{{\\"id\\":2,\\"label\\":\\"beta\\"}}\\n"}},
                        {{"key": "source_format", "value": "jsonl"}},
                        {{"key": "order_by_runtime_execution", "value": "true"}},
                        {{"key": "top_n_runtime_execution", "value": "true"}},
                        {{"key": "sort_operator_family", "value": "single_key_scalar_topn"}},
                        {{"key": "sort_keys", "value": "amount"}},
                        {{"key": "sort_direction", "value": "desc"}},
                        {{"key": "sort_null_ordering", "value": "nulls_blocked_for_fixture_smoke"}},
                        {{"key": "top_n_limit", "value": "2"}},
                        {{"key": "execution_certificate_ref", "value": "sql-local-source.jsonl.order-by-topn-filter-limit.execution.v1"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        sorted_workflow = (
            ctx.read_json("target/input.jsonl")
            .select("id", "label")
            .filter("amount >= 0")
            .sort("amount", descending=True)
        )
        self.assertIsInstance(sorted_workflow, sl.LazyFrame)
        report = sorted_workflow.limit(2).collect()

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.envelope.field("source_format"), "jsonl")
        self.assertTrue(report.order_by_runtime_execution)
        self.assertTrue(report.top_n_runtime_execution)
        self.assertEqual(report.sort_keys, ("amount",))
        self.assertEqual(report.sort_direction, "desc")
        self.assertEqual(report.top_n_limit, 2)
        self.assertEqual(
            report.envelope.field("execution_certificate_ref"),
            "sql-local-source.jsonl.order-by-topn-filter-limit.execution.v1",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_read_json_plain_json_projection_filter_limit_invokes_sql_smoke(self) -> None:
        statement = "SELECT id FROM 'target/input.json' WHERE id >= 1 LIMIT 1"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "sql-local-source-smoke",
                    {statement!r},
                    "--output-format",
                    "inline-jsonl",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "SQL local-source smoke returned 1 bounded row(s)",
                    "human_text": "SQL local-source smoke",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "result_jsonl", "value": "{{\\"id\\":1}}\\n"}},
                        {{"key": "output_row_count", "value": "1"}},
                        {{"key": "selected_row_count", "value": "1"}},
                        {{"key": "source_format", "value": "json"}},
                        {{"key": "source_io_performed", "value": "true"}},
                        {{"key": "source_adapter_id", "value": "local_json_input_adapter"}},
                        {{"key": "source_adapter_status", "value": "smoke_supported"}},
                        {{"key": "ingress_route", "value": "direct_transient"}},
                        {{"key": "vortex_ingest_performed", "value": "false"}},
                        {{"key": "prepared_state_created", "value": "false"}},
                        {{"key": "selected_execution_mode", "value": "direct_compatibility_transient"}},
                        {{"key": "timing_scope", "value": "direct_one_shot"}},
                        {{"key": "runtime_execution", "value": "true"}},
                        {{"key": "write_io", "value": "false"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}},
                        {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_json("target/input.json")
            .select("id")
            .filter("id >= 1")
            .limit(1)
            .collect()
        )

        self.assertEqual(report.envelope.command, "sql-local-source-smoke")
        self.assertEqual(report.output_row_count, 1)
        self.assertEqual(report.selected_row_count, 1)
        self.assertEqual(report.envelope.field("source_format"), "json")
        self.assertEqual(report.envelope.field("source_adapter_id"), "local_json_input_adapter")
        self.assertEqual(report.envelope.field("ingress_route"), "direct_transient")
        self.assertEqual(report.envelope.field("vortex_ingest_performed"), "false")
        self.assertEqual(report.envelope.field("prepared_state_created"), "false")
        self.assertEqual(report.envelope.field("timing_scope"), "direct_one_shot")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")

    def test_context_sql_source_free_collect_remains_deterministic_unsupported(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "workflow-unsupported-plan",
                    "sql-source-free-projection",
                    "sql(statement)",
                    "source_free_sql_collect_requires_write_output",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "workflow operation unsupported",
                    "human_text": "workflow unsupported operation",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "operation", "value": "sql-source-free-projection"},
                        {"key": "workflow_summary", "value": "sql(statement)"},
                        {"key": "target_ref", "value": "source_free_sql_collect_requires_write_output"},
                        {"key": "blocker_id", "value": "gar-gen-1.sql_source_free_projection.runtime_not_admitted"},
                        {"key": "required_evidence", "value": "execution_certificate,native_io_certificate"},
                        {"key": "suggested_next_action", "value": "inspect capability and evidence reports"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql("VALUES (1, 'alpha')").collect()

        self.assertEqual(report.operation, "sql-source-free-projection")
        self.assertTrue(report.blocker_id)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.runtime_execution)
        self.assertEqual(report.evidence_summary.status, "unsupported")
        self.assertEqual(report.claim_summary.blocker_id, report.blocker_id)
        self.assertFalse(report.claim_summary.fallback_attempted)

    def test_context_sql_broad_table_query_remains_deterministic_unsupported(self) -> None:
        statement = "SELECT * FROM events"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "workflow-unsupported-plan",
                    "sql",
                    "sql(statement)",
                    {statement!r},
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "workflow operation unsupported",
                    "human_text": "workflow unsupported operation",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "operation", "value": "sql"}},
                        {{"key": "workflow_summary", "value": "sql(statement)"}},
                        {{"key": "target_ref", "value": {statement!r}}},
                        {{"key": "blocker_id", "value": "cg21.workflow.sql.runtime_not_admitted"}},
                        {{"key": "required_evidence", "value": "execution_certificate,native_io_certificate"}},
                        {{"key": "suggested_next_action", "value": "inspect capability and evidence reports"}},
                        {{"key": "runtime_execution", "value": "false"}},
                        {{"key": "data_read", "value": "false"}},
                        {{"key": "write_io", "value": "false"}},
                        {{"key": "fallback_attempted", "value": "false"}}
                    ],
                }}))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql(statement).collect()

        self.assertEqual(report.operation, "sql")
        self.assertEqual(report.envelope.field("target_ref"), statement)
        self.assertTrue(report.blocker_id)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.runtime_execution)

    def test_context_sql_quoted_local_file_literal_does_not_admit_broad_from(self) -> None:
        statement = "SELECT 'target/input.csv' AS path FROM events"
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "workflow-unsupported-plan",
                    "sql",
                    "sql(statement)",
                    {statement!r},
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "workflow operation unsupported",
                    "human_text": "workflow unsupported operation",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "operation", "value": "sql"}},
                        {{"key": "workflow_summary", "value": "sql(statement)"}},
                        {{"key": "target_ref", "value": {statement!r}}},
                        {{"key": "blocker_id", "value": "cg21.workflow.sql.runtime_not_admitted"}},
                        {{"key": "runtime_execution", "value": "false"}},
                        {{"key": "data_read", "value": "false"}},
                        {{"key": "write_io", "value": "false"}},
                        {{"key": "fallback_attempted", "value": "false"}}
                    ],
                }}))
                sys.exit(1)
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql(statement).collect()

        self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
        self.assertEqual(report.envelope.field("target_ref"), statement)
        self.assertFalse(report.runtime_execution)
        self.assertFalse(report.data_read)
        self.assertFalse(report.fallback_attempted)

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
                    "write-arrow-ipc": "write_arrow_ipc",
                    "write-avro": "write_avro",
                    "write-orc": "write_orc",
                    "sql-parse": "sql_parse",
                    "sql-bind": "sql_bind",
                    "sql-plan": "sql_plan",
                    "sql-execute": "sql_execute",
                    "dataframe-source-free-projection": "dataframe_source_free_projection",
                    "dataframe-generated-with-column": "dataframe_generated_with_column",
                    "object-store-generated-output": "object_store_generated_output",
                    "foundry-generated-output": "foundry_generated_output",
                    "schema-contract": "schema_contract",
                    "describe-schema": "describe_schema",
                    "validate-schema": "validate_schema",
                    "data-quality": "data_quality",
                    "data-quality-summary": "data_quality_summary",
                }.get(operation, operation)
                write_required = (
                    operation.startswith("write-")
                    or operation == "quarantine"
                    or operation in {"object-store-generated-output", "foundry-generated-output"}
                )
                materialization_required = operation in {
                    "collect", "from-pandas", "from-arrow-table", "from-arrow-ipc",
                    "to-pandas", "to-arrow", "to-arrow-table", "to-arrow-ipc",
                    "to-numpy", "to-python-objects", "write-vortex", "write-parquet",
                    "write-arrow-ipc", "write-avro", "write-orc",
                    "quarantine", "preview", "head", "take", "display",
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
                        "sql-values", "sql-literal-select",
                        "with-column", "group-by", "agg", "sort", "join",
                        "aggregate", "window",
                    }
                    else "SL_OBJECT_STORE_UNSUPPORTED"
                    if operation == "object-store-generated-output"
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
            workflow.with_column("date", "to_date(ts)"),
            workflow.group_by("id").agg(total="sum(amount)"),
            workflow.agg("sum(amount)"),
            workflow.sort("amount", "amount", descending=True),
            workflow.write_vortex("out.vortex", check=False),
            workflow.write_parquet("out.parquet", check=False),
            ctx.sql_parse("select * from events"),
            ctx.sql_bind("select * from events"),
            ctx.sql_plan("select * from events"),
            ctx.sql_execute("select * from events"),
            workflow.join(
                sl.read_csv("dim.csv", client=ShardLoomClient(binary=binary)).filter("id > 0"),
                on=("id", "other_id"),
                how="left",
            ),
            workflow.aggregate("sum(amount)"),
            workflow.sort("amount").window("row_number() over (partition by id)"),
            workflow.schema_contract({"id": "int64"}),
            workflow.data_quality_check("regex:id"),
            workflow.quarantine("bad.vortex"),
            sl.read_csv("events.data", client=ShardLoomClient(binary=binary)).preview(limit=5),
            sl.read_csv("events.data", client=ShardLoomClient(binary=binary)).head(limit=5),
            sl.read_csv("events.data", client=ShardLoomClient(binary=binary)).take(5),
            workflow.display(),
            ctx.dataframe_source_free_projection("lit(1).alias('value')"),
            ctx.dataframe_generated_with_column("value", "lit(1)"),
            ctx.generated_output_to_object_store("s3://bucket/out.jsonl"),
            ctx.foundry_generated_output("foundry://dataset/output"),
        )

        self.assertEqual(len(reports), 34)
        for report in reports:
            self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
            self.assertEqual(report.envelope.status, "unsupported")
            self.assertTrue(report.blocker_id)
            self.assertTrue(
                report.blocker_id.startswith("cg21.workflow.")
                or report.blocker_id.startswith("gar-gen-1.")
            )
            if report.operation in {"from-pandas", "from-arrow-table", "from-arrow-ipc"}:
                self.assertTrue(report.envelope.field("workflow_summary", "").startswith("read_"))
            elif report.operation in {"sql-parse", "sql-bind", "sql-plan", "sql-execute"}:
                self.assertEqual(report.envelope.field("workflow_summary"), "sql(statement)")
            elif report.operation in {
                "dataframe-source-free-projection",
                "dataframe-generated-with-column",
                "object-store-generated-output",
                "foundry-generated-output",
            }:
                self.assertTrue(
                    report.envelope.field("workflow_summary", "").startswith("source_free(")
                )
            elif report.operation == "preview":
                self.assertEqual(report.envelope.field("workflow_summary"), "read_csv(events.data)")
            elif report.operation in {"head", "take"}:
                self.assertEqual(report.envelope.field("workflow_summary"), "read_csv(events.data)")
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
            "desc:amount,amount",
        )
        self.assertEqual(
            by_operation["write-vortex"].envelope.field("target_ref"),
            "out.vortex",
        )
        self.assertTrue(by_operation["write-vortex"].envelope.field_bool("write_required"))
        self.assertFalse(by_operation["sql-parse"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["sql-bind"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["sql-plan"].envelope.field_bool("runtime_required"))
        self.assertTrue(by_operation["sql-execute"].envelope.field_bool("runtime_required"))
        self.assertEqual(by_operation["window"].envelope.field("workflow_operation"), "window")
        self.assertFalse(by_operation["schema-contract"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["data-quality"].envelope.field_bool("runtime_required"))
        self.assertEqual(by_operation["quarantine"].envelope.field("target_ref"), "bad.vortex")
        self.assertTrue(by_operation["quarantine"].envelope.field_bool("write_required"))
        self.assertTrue(by_operation["preview"].envelope.field_bool("materialization_required"))
        self.assertTrue(by_operation["head"].envelope.field_bool("materialization_required"))
        self.assertEqual(by_operation["head"].envelope.field("target_ref"), "5")
        self.assertTrue(by_operation["take"].envelope.field_bool("materialization_required"))
        self.assertEqual(by_operation["take"].envelope.field("target_ref"), "5")
        self.assertEqual(by_operation["display"].envelope.field("workflow_operation"), "display")
        self.assertEqual(
            by_operation["dataframe-source-free-projection"].envelope.field("workflow_operation"),
            "dataframe_source_free_projection",
        )
        self.assertEqual(
            by_operation["dataframe-generated-with-column"].envelope.field("target_ref"),
            "value=lit(1)",
        )
        self.assertTrue(
            by_operation["object-store-generated-output"].envelope.field_bool("write_required")
        )
        self.assertTrue(
            by_operation["foundry-generated-output"].envelope.field_bool("write_required")
        )

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
                    {"key": "universal_compatibility_generated_output_row_order", "value": "no_dataset_smoke,python_ctx_from_rows,python_ctx_range,python_ctx_sequence,python_ctx_literal_table,python_ctx_calendar,python_generated_source_write,local_output_only_generated_source_posture,sql_literal_select,sql_values,sql_source_free_projection,sql_generate_series_range,dataframe_source_free_projection,dataframe_generated_with_column"},
                    {"key": "universal_compatibility_generated_output_python_row_order", "value": "python_ctx_from_rows,python_ctx_range,python_ctx_sequence,python_ctx_literal_table,python_ctx_calendar,python_generated_source_write"},
                    {"key": "universal_compatibility_generated_output_sql_row_order", "value": "sql_literal_select,sql_values,sql_source_free_projection,sql_generate_series_range"},
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
                    ("python_ctx_from_rows", "Python ctx.from_rows([...]).write(local_jsonl_or_csv)", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_jsonl_csv_smoke_only"),
                    ("python_ctx_range", "Python ctx.range(...).write(local_jsonl_or_csv)", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_range_jsonl_csv_smoke_only"),
                    ("python_ctx_sequence", "Python ctx.sequence(...).write(local_jsonl_or_csv)", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_sequence_jsonl_csv_smoke_only"),
                    ("python_ctx_literal_table", "Python ctx.literal_table([...]).write(local_jsonl_or_csv)", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_literal_table_jsonl_csv_smoke_only"),
                    ("python_ctx_calendar", "Python ctx.calendar(start,end).write(local_jsonl_or_csv)", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_calendar_jsonl_csv_smoke_only"),
                    ("python_generated_source_write", "Generated-source write path", "python_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_supported_generated_source_write_smokes_only"),
                    ("local_output_only_generated_source_posture", "Generated-source local-output-only posture", "output_boundary", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "local_output_certificate_required", "not_emitted_report_only", "not_claim_grade", "gar-compat-1b.non_local_generated_output_blocked"),
                    ("sql_literal_select", "SQL SELECT literal expressions", "sql_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_sql_literal_select_jsonl_csv_smoke_only"),
                    ("sql_values", "SQL VALUES (...)", "sql_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_sql_values_jsonl_csv_smoke_only"),
                    ("sql_source_free_projection", "SQL source-free projection", "sql_generated_source", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_report_only", "not_emitted_report_only", "not_claim_grade", "gar-gen-1.sql_source_free_projection_runtime_not_implemented"),
                    ("sql_generate_series_range", "SQL generate_series/range", "sql_generated_source", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_sql_generate_series_range_jsonl_csv_smoke_only"),
                    ("dataframe_source_free_projection", "DataFrame source-free projection", "dataframe_generated_source", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_report_only", "not_emitted_report_only", "not_claim_grade", "gar-gen-1.dataframe_source_free_projection_runtime_not_implemented"),
                    ("dataframe_generated_with_column", "DataFrame generated with_column", "dataframe_generated_source", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_report_only", "not_emitted_report_only", "not_claim_grade", "gar-gen-1.dataframe_generated_with_column_runtime_not_implemented"),
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
                    ("sql_values_literals", "SQL VALUES / literals", "sql_frontend", "api", "smoke-supported", "false", "false", "false", "false", "false", "true", "local_output_certificate_required", "scoped_local_jsonl_csv_smoke", "fixture_smoke_only", "none_scoped_local_sql_values_literals_jsonl_csv_smoke_only", "source-free SQL VALUES/literal local JSONL/CSV fixture smoke only"),
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
        self.assertEqual(
            generated.python_row_order,
            (
                "python_ctx_from_rows",
                "python_ctx_range",
                "python_ctx_sequence",
                "python_ctx_literal_table",
                "python_ctx_calendar",
                "python_generated_source_write",
            ),
        )
        self.assertTrue(generated.no_dataset_smoke_separate)
        self.assertTrue(generated.local_output_only)
        self.assertTrue(generated.output_certificate_required)
        self.assertFalse(generated.object_store_runtime_supported)
        self.assertFalse(generated.foundry_runtime_supported)
        self.assertFalse(generated.broad_sql_dataframe_claim_allowed)
        self.assertTrue(generated.all_no_fallback_no_external_engine)
        self.assertTrue(generated.row("python-ctx-from-rows").fixture_smoke_supported)
        self.assertTrue(generated.row("python_ctx_from_rows").generated_source_created)
        self.assertTrue(generated.row("python_ctx_sequence").runtime_execution)
        self.assertTrue(generated.row("python_ctx_literal_table").fixture_smoke_supported)
        self.assertTrue(generated.row("python_ctx_calendar").runtime_execution)
        self.assertTrue(generated.row("sql_values").fixture_smoke_supported)
        self.assertTrue(generated.row("sql_values").runtime_execution)
        self.assertTrue(generated.row("sql_generate_series_range").fixture_smoke_supported)
        self.assertTrue(generated.row("sql_generate_series_range").runtime_execution)
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
