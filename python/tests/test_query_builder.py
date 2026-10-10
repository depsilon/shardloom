from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom import LazyFrame, ShardLoomClient, ShardLoomContext
from shardloom.models import OutputEnvelope
from shardloom.query import (
    WorkflowOperation,
    _embedded_vortex_input_uri,
    _normalize_local_output_format,
    _public_write_request_for_format,
    _rewrite_predicate_with_computed_columns,
    _vortex_expression_scalar_payload,
)

_FAKE_CLI_ENVELOPE_PRELUDE = textwrap.dedent(
    """
    import json as _shardloom_json
    import sys as _shardloom_sys

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

    def fake_public_local_write_cli(
        self, *, requested_output: str, output_path: str, output_format: str
    ) -> list[str]:
        return self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                args = sys.argv[1:]
                assert args[:2] == ["run", "dataframe"], sys.argv
                assert args[args.index("--input") + 1] == "target/input.csv", sys.argv
                assert args[args.index("--input-format") + 1] == "csv", sys.argv
                assert "target/input.csv" in args[args.index("--sql") + 1], sys.argv
                assert "target/input.csv" in args[args.index("--plan") + 1], sys.argv
                assert args[args.index("--request") + 1] == {requested_output!r}, sys.argv
                assert args[args.index("--output") + 1] == {output_path!r}, sys.argv
                assert args[args.index("--execution-policy") + 1] == "vortex_middle", sys.argv
                assert args[args.index("--materialization-policy") + 1] == "bounded", sys.argv
                assert args[args.index("--evidence-level") + 1] == "production_admitted_local_workflow", sys.argv
                assert args[args.index("--bounded") + 1] == "true", sys.argv
                assert "--allow-overwrite" in args, sys.argv
                assert args[args.index("--max-parallelism") + 1] == "2", sys.argv
                assert args[-2:] == ["--format", "json"], sys.argv
                fields = [
                    ["public_workflow_facade_command", "run"],
                    ["public_workflow_requested_output", {requested_output!r}],
                    ["native_vortex_result_export_path", {output_path!r}],
                    ["native_vortex_result_export_format", {output_format!r}],
                    ["output_io_performed", "true"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                ]
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "run",
                    "status": "success",
                    "summary": "public local write",
                    "human_text": "public local write",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [{{"key": key, "value": value}} for key, value in fields],
                }}))
                """
            ),
        )


    def assert_public_local_file_vortex_middle_blocked(
        self,
        envelope: sl.OutputEnvelope,
        *,
        requested_output: str,
    ) -> sl.PublicWorkflowExecution:
        execution = sl.PublicWorkflowExecution(envelope)
        self.assertEqual(envelope.command, "run")
        self.assertEqual(execution.facade_command, "run")
        self.assertTrue(execution.route_attached)
        self.assertEqual(execution.route_id, "blocked")
        self.assertEqual(execution.route_status, "blocked")
        self.assertEqual(execution.resolved_internal_command, "not_resolved")
        self.assertEqual(execution.vortex_normalization_point, "not_applicable")
        self.assertEqual(execution.execution_mode, "blocked")
        self.assertFalse(execution.preparation_included)
        self.assertFalse(execution.runtime_execution)
        self.assertFalse(execution.fallback_attempted)
        self.assertFalse(execution.external_engine_invoked)
        self.assertFalse(execution.public_workflow_fallback_attempted)
        self.assertFalse(execution.public_workflow_external_engine_invoked)
        self.assertEqual(
            execution.blocker_id,
            "cg21.route.local_file_vortex_middle_required",
        )
        self.assertEqual(
            envelope.field("public_workflow_requested_output"),
            requested_output,
        )
        return execution

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
        inferred_vortex_schema_frame = sl.read(
            "events.vortex",
            schema={"id": "int64"},
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
        self.assertEqual(inferred_vortex_schema_frame.source_format, "vortex")
        self.assertEqual(inferred_vortex_schema_frame.source.schema_map["id"], "int64")
        self.assertEqual(
            inferred_vortex_schema_frame.operation_summary,
            "read_vortex(events.vortex)",
        )
        self.assertEqual(
            frame.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> limit(10)",
        )
        with self.assertRaisesRegex(ValueError, "cannot infer a local source adapter"):
            sl.read("events.data", binary=["definitely-missing-shardloom"])

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

    def test_local_source_query_builder_rejects_raw_sql_clause_breakouts(self) -> None:
        frame = sl.read_csv("target/input.csv", binary=["definitely-missing-shardloom"])

        with self.assertRaisesRegex(ValueError, "clause keyword"):
            frame.filter("id > 0 UNION SELECT secret FROM 'target/secret.csv'")
        with self.assertRaisesRegex(ValueError, "clause keyword 'from'"):
            frame.having("count(*) > 0 FROM 'target/secret.csv'")
        with self.assertRaisesRegex(ValueError, "statement separators"):
            frame.filter("id > 0; SELECT secret FROM 'target/secret.csv'")

        self.assertIsNone(
            frame.select("id FROM 'target/secret.csv'").limit(10)._sql_local_source_statement()
        )
        self.assertIsNone(
            frame.aggregate("count(*) FROM 'target/secret.csv'")
            .limit(1)
            ._sql_local_source_statement()
        )
        self.assertIsNone(
            frame.sort("id LIMIT 100").limit(10)._sql_local_source_statement()
        )

    def test_local_source_query_builder_keeps_typed_subquery_predicates_scoped(self) -> None:
        statement = (
            sl.read_csv("target/input.csv", binary=["definitely-missing-shardloom"])
            .filter(sl.col("id").isin_source("target/allowed.csv", "id", limit=5))
            .select("id")
            .limit(10)
            ._sql_local_source_statement()
        )

        self.assertEqual(
            statement,
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' LIMIT 5) LIMIT 10",
        )

    def test_workflow_route_uses_shared_cli_contract_for_sql_and_dataframe(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                assert args[-2:] == ["--format", "json"], sys.argv
                surface = args[1]
                if surface == "dataframe":
                    assert args[:6] == ["route", "dataframe", "--input", "target/input.csv", "--input-format", "csv"], sys.argv
                    assert args[args.index("--plan") + 1] == "read_csv(target/input.csv) -> select(id) -> limit(10)", sys.argv
                    assert args[args.index("--evidence-level") + 1] in {
                        "production_admitted_local_workflow",
                        "runtime_smoke",
                    }, sys.argv
                elif surface == "sql":
                    assert args[:2] == ["route", "sql"], sys.argv
                    assert args[args.index("--sql") + 1] == "SELECT id FROM 'target/input.csv' LIMIT 10", sys.argv
                    assert args[args.index("--plan") + 1] == "sql(statement)", sys.argv
                    assert args[args.index("--evidence-level") + 1] == "runtime_smoke", sys.argv
                else:
                    raise AssertionError(sys.argv)
                assert args[args.index("--request") + 1] == "collect", sys.argv
                assert args[args.index("--execution-policy") + 1] == "vortex_middle", sys.argv
                assert args[args.index("--materialization-policy") + 1] == "bounded", sys.argv
                assert args[args.index("--bounded") + 1] == "true", sys.argv
                fields = [
                    ["public_workflow_route_schema_version", "shardloom.public_workflow_route.v1"],
                    ["route_id", "blocked"],
                    ["route_status", "blocked"],
                    ["resolved_internal_command", "not_resolved"],
                    ["surface", surface],
                    ["start_state", "blocked"],
                    ["vortex_normalization_point", "not_applicable"],
                    ["vortex_middle_status", "blocked_or_unsupported"],
                    ["execution_mode", "blocked"],
                    ["preparation_included", "false"],
                    ["query_timing_starts_after_preparation", "false"],
                    ["route_side_effect_free", "true"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                    ["blocker_id", "cg21.route.local_file_vortex_middle_required"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "route",
                    "status": "unsupported",
                    "summary": "public workflow route",
                    "human_text": "route",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        dataframe_route = (
            ctx.read_csv("target/input.csv").select("id").limit(10).route(check=False)
        )
        sql_route = ctx.sql("SELECT id FROM 'target/input.csv' LIMIT 10").route(
            check=False
        )
        live_context_route = ShardLoomContext(
            ShardLoomClient(binary=binary),
            engine="live",
        ).route(
            "dataframe",
            input_uri="target/input.csv",
            input_format="csv",
            plan_summary="read_csv(target/input.csv) -> select(id) -> limit(10)",
            bounded=True,
            check=False,
        )

        self.assertIsInstance(dataframe_route, sl.PublicWorkflowRoute)
        self.assertEqual(dataframe_route.route_id, "blocked")
        self.assertEqual(sql_route.route_id, dataframe_route.route_id)
        self.assertEqual(live_context_route.route_id, dataframe_route.route_id)
        self.assertEqual(
            dataframe_route.resolved_internal_command,
            sql_route.resolved_internal_command,
        )
        self.assertEqual(
            live_context_route.resolved_internal_command,
            dataframe_route.resolved_internal_command,
        )
        self.assertEqual(
            dataframe_route.vortex_normalization_point,
            "not_applicable",
        )
        self.assertTrue(dataframe_route.side_effect_free)
        self.assertFalse(dataframe_route.fallback_attempted)
        self.assertFalse(dataframe_route.external_engine_invoked)
        self.assertEqual(
            dataframe_route.blocker_id,
            "cg21.route.local_file_vortex_middle_required",
        )
        self.assertEqual(dataframe_route.as_dict()["execution_mode"], "blocked")


    def test_workflow_route_blocks_unbounded_collect_at_admission(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "route",
                    "dataframe",
                    "--input",
                    "target/input.csv",
                    "--input-format",
                    "csv",
                    "--source-bindings",
                    json.dumps({"target/input.csv": {"input_format": "csv"}}, separators=(",", ":")),
                    "--sql",
                    "SELECT * FROM 'target/input.csv'",
                    "--plan",
                    "read_csv(target/input.csv)",
                    "--request",
                    "collect",
                    "--execution-policy",
                    "vortex_middle",
                    "--materialization-policy",
                    "bounded",
                    "--evidence-level",
                    "production_admitted_local_workflow",
                    "--bounded",
                    "false",
                    "--format",
                    "json",
                ], sys.argv
                fields = [
                    ["public_workflow_route_schema_version", "shardloom.public_workflow_route.v1"],
                    ["route_id", "blocked"],
                    ["route_status", "blocked"],
                    ["resolved_internal_command", "not_resolved"],
                    ["surface", "dataframe"],
                    ["start_state", "blocked"],
                    ["vortex_normalization_point", "not_applicable"],
                    ["execution_mode", "blocked"],
                    ["preparation_included", "false"],
                    ["query_timing_starts_after_preparation", "false"],
                    ["route_side_effect_free", "true"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                    ["blocker_id", "cg21.route.unbounded_collect_blocked"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "route",
                    "status": "unsupported",
                    "summary": "public workflow route",
                    "human_text": "route",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        route = ctx.read_csv("target/input.csv").route(check=False)

        self.assertEqual(route.route_status, "blocked")
        self.assertEqual(route.blocker_id, "cg21.route.unbounded_collect_blocked")
        self.assertEqual(route.resolved_internal_command, "not_resolved")
        self.assertTrue(route.side_effect_free)
        self.assertFalse(route.fallback_attempted)
        self.assertFalse(route.external_engine_invoked)


    def test_workflow_prepare_uses_public_facade_with_attached_route(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "prepare",
                    "dataframe",
                    "--input",
                    "target/input.csv",
                    "--input-format",
                    "csv",
                    "--plan",
                    "read_csv(target/input.csv)",
                    "--request",
                    "prepare",
                    "--output",
                    "target/input.vortex",
                    "--execution-policy",
                    "prepare_once",
                    "--materialization-policy",
                    "bounded",
                    "--evidence-level",
                    "production_admitted_local_workflow",
                    "--bounded",
                    "true",
                    "--memory-bytes",
                    "4294967296",
                    "--max-parallelism",
                    "2",
                    "--memory-origin",
                    "execution_call",
                    "--parallelism-origin",
                    "execution_call",
                    "--format",
                    "json",
                ], sys.argv
                fields = [
                    ["public_workflow_facade_schema_version", "shardloom.public_workflow_execution_facade.v1"],
                    ["public_workflow_route_attached", "true"],
                    ["public_workflow_facade_command", "prepare"],
                    ["public_workflow_route_id", "local_file_prepare_once"],
                    ["public_workflow_route_status", "admitted"],
                    ["public_workflow_resolved_internal_command", "vortex-prepare"],
                    ["public_workflow_vortex_normalization_point", "VortexPreparedState"],
                    ["public_workflow_execution_mode", "prepared_vortex"],
                    ["public_workflow_preparation_included", "true"],
                    ["runtime_execution", "true"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                    ["public_workflow_fallback_attempted", "false"],
                    ["public_workflow_external_engine_invoked", "false"],
                    ["public_workflow_blocker_id", "none"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "prepare",
                    "status": "success",
                    "summary": "public workflow prepare",
                    "human_text": "prepare",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))

        execution = ctx.read_csv("target/input.csv").prepare("target/input.vortex")

        self.assertIsInstance(execution, sl.PublicWorkflowExecution)
        self.assertEqual(execution.facade_command, "prepare")
        self.assertTrue(execution.route_attached)
        self.assertEqual(execution.route_id, "local_file_prepare_once")
        self.assertEqual(execution.resolved_internal_command, "vortex-prepare")
        self.assertTrue(execution.preparation_included)
        self.assertFalse(execution.fallback_attempted)
        self.assertFalse(execution.external_engine_invoked)

    def test_workflow_prepare_forwards_schema_hints_only_for_text_adapters(self) -> None:
        client = ShardLoomClient(binary="unused-shardloom", memory_gb=4, max_parallelism=2)
        schema = {"code": "utf8"}
        for format_name, read in (
            ("csv", sl.read_csv), ("json", sl.read_json),
            ("parquet", sl.read_parquet), ("arrow-ipc", sl.read_arrow_ipc),
            ("avro", sl.read_avro), ("orc", sl.read_orc),
            ("vortex", sl.read_vortex),
        ):
            with self.subTest(format=format_name), mock.patch.object(
                client, "public_workflow_prepare"
            ) as prepare:
                read(f"source.{format_name}", schema=schema, client=client).prepare(
                    "prepared.vortex"
                )
                self.assertEqual(
                    prepare.call_args.kwargs["source_schema"],
                    tuple(schema.items()) if format_name in {"csv", "json"} else None,
                )


    def test_materialized_input_boundaries_create_generated_rows(self) -> None:
        class FakeDataFrame:
            def to_dict(self, orient: str = "dict") -> list[dict[str, object]]:
                if orient != "records":
                    raise AssertionError(orient)
                return [
                    {"id": 1, "label": "alpha"},
                    {"id": 2, "label": "beta"},
                ]

        class FakeArrowTable:
            def __init__(self, rows: list[dict[str, object]]) -> None:
                self._rows = rows

            def to_pylist(self) -> list[dict[str, object]]:
                return self._rows

        class FakeArrowReader:
            def read_all(self) -> FakeArrowTable:
                return FakeArrowTable(
                    [
                        {"id": 3, "label": "gamma"},
                        {"id": 4, "label": "delta"},
                    ]
                )

        class FakeArrowIpc:
            @staticmethod
            def open_stream(_source: object) -> FakeArrowReader:
                return FakeArrowReader()

        class FakePyArrowModule:
            BufferReader = bytes
            ipc = FakeArrowIpc

        client = ShardLoomClient(binary=["definitely-missing-shardloom"], memory_gb=4, max_parallelism=2)

        pandas_source = sl.from_pandas(FakeDataFrame(), client=client)
        arrow_source = sl.from_arrow_table(
            FakeArrowTable(
                [
                    {"id": 1, "label": "alpha"},
                    {"id": 2, "label": "beta"},
                ]
            ),
            client=client,
        )
        with mock.patch.dict(sys.modules, {"pyarrow": FakePyArrowModule}):
            ipc_source = sl.from_arrow_ipc(b"fake-ipc", client=client)

        for source in (pandas_source, arrow_source):
            self.assertIsInstance(source, sl.LazyFrame)
            self.assertEqual(source.source.schema, (("id", "int64"), ("label", "utf8")))
            self.assertEqual(dict(source.source.memory_input)["rows"], (("1", "alpha"), ("2", "beta")))
            self.assertFalse(source.client is None)

        self.assertIsInstance(ipc_source, sl.LazyFrame)
        self.assertEqual(ipc_source.source.schema, (("id", "int64"), ("label", "utf8")))
        self.assertEqual(dict(ipc_source.source.memory_input)["rows"], (("3", "gamma"), ("4", "delta")))

    def test_from_arrow_ipc_falls_back_to_file_reader_for_file_payloads(self) -> None:
        class FakeArrowTable:
            def to_pylist(self) -> list[dict[str, object]]:
                return [{"id": 5, "label": "file"}]

        class FakeArrowReader:
            def read_all(self) -> FakeArrowTable:
                return FakeArrowTable()

        class FakeArrowIpc:
            @staticmethod
            def open_stream(_source: object) -> FakeArrowReader:
                raise ValueError("not an Arrow IPC stream")

            @staticmethod
            def open_file(_source: object) -> FakeArrowReader:
                return FakeArrowReader()

        class FakePyArrowModule:
            BufferReader = bytes
            ipc = FakeArrowIpc

        with mock.patch.dict(sys.modules, {"pyarrow": FakePyArrowModule}):
            ipc_source = sl.from_arrow_ipc(
                b"fake-file-ipc",
                client=ShardLoomClient(binary=["definitely-missing-shardloom"], memory_gb=4, max_parallelism=2),
            )

        self.assertEqual(ipc_source.source.schema, (("id", "int64"), ("label", "utf8")))
        self.assertEqual(dict(ipc_source.source.memory_input)["rows"], (("5", "file"),))


    def test_local_csv_query_builder_malformed_regex_quality_and_quarantine_are_unsupported(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1] == "workflow-unsupported-plan", sys.argv
                operation = sys.argv[2]
                assert operation in {"data-quality", "quarantine"}, sys.argv
                assert sys.argv[3:] == [
                    "read_csv(target/input.csv) -> select(id,label)",
                    "regex:label:[",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "SL_NOT_IMPLEMENTED",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": f"cg21.workflow.{operation}",
                        "reason": "regex quality rule is not admitted",
                        "suggested_next_step": "inspect capability and evidence reports",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [
                        {"key": "workflow_operation", "value": operation},
                        {"key": "blocker_id", "value": f"cg21.workflow.{operation}.unsupported"},
                        {"key": "required_evidence", "value": "execution_certificate,native_io_certificate"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"}
                    ],
                }))
                """
            )
        )
        workflow = (
            ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))
            .read_csv("target/input.csv")
            .select("id", "label")
        )

        quality_report = workflow.data_quality_check("regex:label:[")
        quarantine_report = workflow.quarantine("target/bad.jsonl", "regex:label:[", check=False)

        self.assertIsInstance(quality_report, sl.UnsupportedWorkflowOperationReport)
        self.assertEqual(quality_report.operation, "data-quality")
        self.assertFalse(quality_report.fallback_attempted)
        self.assertFalse(quality_report.external_engine_invoked)
        self.assertIsInstance(quarantine_report, sl.UnsupportedWorkflowOperationReport)
        self.assertEqual(quarantine_report.operation, "quarantine")
        self.assertFalse(quarantine_report.fallback_attempted)
        self.assertFalse(quarantine_report.external_engine_invoked)


    def test_sql_statement_with_limit_caps_existing_top_level_limit(self) -> None:
        from shardloom.query import _sql_statement_with_limit

        self.assertEqual(
            _sql_statement_with_limit("SELECT * FROM 'events.csv' LIMIT 10", 2),
            "SELECT * FROM 'events.csv' LIMIT 2",
        )
        self.assertEqual(
            _sql_statement_with_limit(
                "SELECT * FROM (SELECT * FROM 'events.csv' LIMIT 10) AS sub",
                2,
            ),
            "SELECT * FROM (SELECT * FROM 'events.csv' LIMIT 10) AS sub LIMIT 2",
        )


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
            str(sl.col("amount").cast("decimal128(10,2)") >= "10.00"),
            "CAST(amount AS decimal128(10,2)) >= '10.00'",
        )
        self.assertEqual(
            str(sl.col("amount").try_cast("numeric(10, 2)")),
            "TRY_CAST(amount AS decimal128(10,2))",
        )
        self.assertEqual(
            str(sl.try_cast(sl.col("raw_amount"), "int64") == 42),
            "TRY_CAST(raw_amount AS int64) = 42",
        )
        self.assertEqual(str(sl.col("payload").cast("binary")), "CAST(payload AS binary)")
        self.assertEqual(str(sl.col("payload").cast("blob")), "CAST(payload AS binary)")
        self.assertEqual(
            str(sl.col("payload").try_cast("varbinary")),
            "TRY_CAST(payload AS binary)",
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
        self.assertEqual(str(sl.outer("id")), "outer.id")
        self.assertEqual(
            str(
                sl.col("id").isin_source(
                    "target/allowed.csv",
                    "id",
                    where=sl.col("active").is_true(),
                    order_by="score",
                    descending=True,
                    limit=2,
                )
            ),
            "id IN (SELECT id FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2)",
        )
        self.assertEqual(
            str(
                sl.col("id").isin_source(
                    "target/allowed.csv",
                    sl.col("allowed.id"),
                    source_alias="allowed",
                    where=sl.col("allowed.active").is_true(),
                    order_by=sl.col("allowed.score"),
                    descending=True,
                    limit=2,
                )
            ),
            "id IN (SELECT allowed.id FROM 'target/allowed.csv' AS allowed WHERE allowed.active IS TRUE ORDER BY allowed.score DESC LIMIT 2)",
        )
        self.assertEqual(
            str(
                sl.col("id").isin_source(
                    "target/allowed.csv",
                    "allowed_id",
                    where=sl.col("allowed_id") == sl.outer("id"),
                )
            ),
            "id IN (SELECT allowed_id FROM 'target/allowed.csv' WHERE allowed_id = outer.id)",
        )
        self.assertEqual(
            str(
                sl.col("id").isin_source(
                    "target/grouped.csv",
                    "id",
                    group_by="id",
                    having="count(*) >= 2 AND id = outer.id",
                )
            ),
            "id IN (SELECT id FROM 'target/grouped.csv' GROUP BY id HAVING count(*) >= 2 AND id = outer.id)",
        )
        self.assertEqual(
            str(sl.col("id").not_in_source("target/blocked.csv", "id")),
            "id NOT IN (SELECT id FROM 'target/blocked.csv')",
        )
        self.assertEqual(
            str(
                sl.col("id").not_in_source(
                    "target/grouped-blocked.csv",
                    "id",
                    group_by="id",
                    having="count(*) >= 2 AND id = outer.id",
                )
            ),
            "id NOT IN (SELECT id FROM 'target/grouped-blocked.csv' GROUP BY id HAVING count(*) >= 2 AND id = outer.id)",
        )
        self.assertEqual(
            str(sl.col("id").any_source("=", "target/allowed.csv", "id")),
            "id = ANY (SELECT id FROM 'target/allowed.csv')",
        )
        self.assertEqual(
            str(
                sl.all_source(
                    "amount",
                    "gt",
                    "target/thresholds.csv",
                    "threshold",
                    where=sl.col("active").is_true(),
                    order_by="score",
                    descending=True,
                    limit=2,
                )
            ),
            "amount > ALL (SELECT threshold FROM 'target/thresholds.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2)",
        )
        self.assertEqual(
            str(
                sl.col("amount").all_source(
                    ">",
                    "target/thresholds.csv",
                    "threshold",
                    where=sl.outer("group_id") == sl.col("group_id"),
                )
            ),
            "amount > ALL (SELECT threshold FROM 'target/thresholds.csv' WHERE outer.group_id = group_id)",
        )
        self.assertEqual(
            str(
                sl.col("amount").all_source(
                    ">",
                    "target/grouped-thresholds.csv",
                    "threshold",
                    group_by="threshold",
                    having="min(id) = outer.id AND count(*) >= 1",
                    order_by="threshold",
                    limit=10,
                )
            ),
            "amount > ALL (SELECT threshold FROM 'target/grouped-thresholds.csv' GROUP BY threshold HAVING min(id) = outer.id AND count(*) >= 1 ORDER BY threshold ASC LIMIT 10)",
        )
        self.assertEqual(
            str(sl.row_in(["id", "label"], [(1, "alpha"), (3, "gamma"), (5, None)])),
            "(id,label) IN ((1,'alpha'),(3,'gamma'),(5,NULL))",
        )
        self.assertEqual(
            str(sl.row_not_in(("id", "label"), ((1, "alpha"), (3, "gamma")))),
            "(id,label) NOT IN ((1,'alpha'),(3,'gamma'))",
        )
        self.assertEqual(
            str(
                sl.row_in_source(
                    ("id", "label"),
                    "target/allowed.csv",
                    ("allowed.id", "allowed.label"),
                    source_alias="allowed",
                    where=sl.col("allowed.active").is_true(),
                    order_by="allowed.score",
                    descending=True,
                    limit=3,
                )
            ),
            "(id,label) IN (SELECT allowed.id,allowed.label FROM 'target/allowed.csv' AS allowed WHERE allowed.active IS TRUE ORDER BY allowed.score DESC LIMIT 3)",
        )
        self.assertEqual(
            str(
                sl.row_in_source(
                    ("id", "label"),
                    "target/allowed.csv",
                    ("allowed_id", "allowed_label"),
                    where=sl.col("allowed_id") == sl.outer("id"),
                )
            ),
            "(id,label) IN (SELECT allowed_id,allowed_label FROM 'target/allowed.csv' WHERE allowed_id = outer.id)",
        )
        self.assertEqual(
            str(
                sl.row_in_source(
                    ("id", "label"),
                    "target/grouped-pairs.csv",
                    ("id", "label"),
                    group_by=("id", "label"),
                    having="count(*) >= 2 AND id = outer.id",
                )
            ),
            "(id,label) IN (SELECT id,label FROM 'target/grouped-pairs.csv' GROUP BY id,label HAVING count(*) >= 2 AND id = outer.id)",
        )
        self.assertEqual(
            str(
                sl.row_not_in_source(
                    ["id", "label"],
                    "target/blocked.csv",
                    ["blocked_id", "blocked_label"],
                )
            ),
            "(id,label) NOT IN (SELECT blocked_id,blocked_label FROM 'target/blocked.csv')",
        )
        self.assertEqual(
            str(
                sl.exists_source(
                    "target/allowed.csv",
                    source_alias="allowed",
                    select=["allowed.id", "allowed.label"],
                    where=sl.col("allowed.active").is_true(),
                    order_by="allowed.score",
                    descending=True,
                    limit=1,
                )
            ),
            "EXISTS (SELECT allowed.id,allowed.label FROM 'target/allowed.csv' AS allowed WHERE allowed.active IS TRUE ORDER BY allowed.score DESC LIMIT 1)",
        )
        self.assertEqual(
            str(
                sl.exists_source(
                    "target/allowed.csv",
                    where=sl.col("allowed_id") == sl.outer("id"),
                )
            ),
            "EXISTS (SELECT * FROM 'target/allowed.csv' WHERE allowed_id = outer.id)",
        )
        self.assertEqual(
            str(
                sl.exists_source(
                    "target/grouped.csv",
                    select="id",
                    group_by="id",
                    having="count(*) >= 2 AND id = outer.id",
                    order_by="id",
                    limit=10,
                )
            ),
            "EXISTS (SELECT id FROM 'target/grouped.csv' GROUP BY id HAVING count(*) >= 2 AND id = outer.id ORDER BY id ASC LIMIT 10)",
        )
        self.assertEqual(
            str(sl.not_exists_source("target/blocked.csv", select=1, limit=0)),
            "NOT EXISTS (SELECT 1 FROM 'target/blocked.csv' LIMIT 0)",
        )
        self.assertEqual(
            str(
                sl.not_exists_source(
                    "target/grouped-blocked.csv",
                    select="id",
                    group_by="id",
                    having="count(*) >= 2 AND id = outer.id",
                )
            ),
            "NOT EXISTS (SELECT id FROM 'target/grouped-blocked.csv' GROUP BY id HAVING count(*) >= 2 AND id = outer.id)",
        )
        with self.assertRaises(ValueError):
            sl.outer("outer.id")
        with self.assertRaisesRegex(ValueError, "reserved"):
            sl.exists_source("target/allowed.csv", source_alias="outer")
        with self.assertRaisesRegex(ValueError, "HAVING requires group_by"):
            sl.exists_source("target/allowed.csv", having="count(*) >= 1")
        self.assertEqual(
            str(sl.exists_source("target/allowed.csv", select=1, group_by="id", limit=33)),
            "EXISTS (SELECT 1 FROM 'target/allowed.csv' GROUP BY id LIMIT 33)",
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
            str(
                sl.col("event_dt").date_add_days(sl.interval_days(1))
                >= date(2026, 5, 20)
            ),
            "DATE_ADD_DAYS(event_dt, INTERVAL '1' DAY) >= DATE '2026-05-20'",
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
                sl.col("event_ts").timestamp_add_seconds(sl.interval_minutes(1))
                >= datetime(2026, 5, 19, 12, 35, 45, tzinfo=timezone.utc)
            ),
            "TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' MINUTE) >= TIMESTAMP '2026-05-19T12:35:45Z'",
        )
        self.assertEqual(str(sl.IntervalLiteral("1", "minutes")), "INTERVAL '1' MINUTE")
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
        self.assertEqual(str(sl.col("label").like("a%a")), "label LIKE 'a%a'")
        self.assertEqual(str(sl.col("label").like("_l%")), "label LIKE '_l%'")
        self.assertEqual(
            str(sl.col("label").like("al!_%", escape="!")),
            "label LIKE 'al!_%' ESCAPE '!'",
        )
        self.assertEqual(str(sl.col("label").startswith("al")), "label LIKE 'al%'")
        self.assertEqual(str(sl.col("label").endswith("ta")), "label LIKE '%ta'")
        self.assertEqual(str(sl.col("label").not_like("%tmp%")), "label NOT LIKE '%tmp%'")
        self.assertEqual(
            str(sl.col("label").not_like("tmp!_%", escape="!")),
            "label NOT LIKE 'tmp!_%' ESCAPE '!'",
        )
        self.assertEqual(str(sl.col("label").rlike("^a.*a$")), "label RLIKE '^a.*a$'")
        self.assertEqual(str(sl.col("label").regex("^a.*a$")), "label RLIKE '^a.*a$'")
        self.assertEqual(str(sl.col("label").matches("^a.*a$")), "label RLIKE '^a.*a$'")
        self.assertEqual(str(sl.col("label").not_rlike("^tmp")), "label NOT RLIKE '^tmp'")
        self.assertEqual(str(sl.col("label").not_regex("^tmp")), "label NOT RLIKE '^tmp'")
        self.assertEqual(str(sl.col("label").not_matches("^tmp")), "label NOT RLIKE '^tmp'")
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
        with self.assertRaisesRegex(
            ValueError, "LIKE escape character must be exactly one character"
        ):
            sl.col("label").like("al!_%", escape="!!")
        self.assertEqual(
            str(sl.col("label").replace(" ", "_") == "alpha_beta"),
            "REPLACE(label, ' ', '_') = 'alpha_beta'",
        )
        self.assertEqual(str(sl.col("payload_hex").unhex()), "UNHEX(payload_hex)")
        self.assertEqual(str(sl.unhex(sl.col("payload_hex"))), "UNHEX(payload_hex)")
        self.assertEqual(
            str(sl.col("payload_hex").unhex() == b"\x00\xff\x10"),
            "UNHEX(payload_hex) = X'00ff10'",
        )
        self.assertEqual(
            str(sl.col("payload_hex").trim().lower().unhex()),
            "UNHEX(LOWER(TRIM(payload_hex)))",
        )
        self.assertEqual(
            str(sl.col("payload_hex").trim().lower().unhex() == b"\x00\xff\x10"),
            "UNHEX(LOWER(TRIM(payload_hex))) = X'00ff10'",
        )
        self.assertEqual(
            str(sl.col("payload_b64").from_base64()), "FROM_BASE64(payload_b64)"
        )
        self.assertEqual(
            str(sl.from_base64(sl.col("payload_b64"))), "FROM_BASE64(payload_b64)"
        )
        self.assertEqual(
            str(sl.from_base64(sl.concat(sl.col("prefix"), sl.col("suffix")))),
            "FROM_BASE64(CONCAT(prefix, suffix))",
        )
        self.assertEqual(
            str(sl.col("payload_hex").unhex().byte_length()),
            "BYTE_LENGTH(UNHEX(payload_hex))",
        )
        self.assertEqual(
            str(sl.byte_length(sl.concat(sl.col("prefix"), sl.col("suffix")).cast("blob"))),
            "BYTE_LENGTH(CAST(CONCAT(prefix, suffix) AS binary))",
        )
        self.assertEqual(
            str(sl.byte_length(sl.concat(sl.col("label"), " AS ").cast("binary"))),
            "BYTE_LENGTH(CAST(CONCAT(label, ' AS ') AS binary))",
        )
        self.assertEqual(
            str(sl.col("payload_b64").from_base64().byte_length() >= 4),
            "BYTE_LENGTH(FROM_BASE64(payload_b64)) >= 4",
        )
        self.assertEqual(str(sl.col("label").byte_length()), "BYTE_LENGTH(label)")
        self.assertEqual(
            str(sl.byte_length(sl.ColumnExpression("UNHEX('aa')"))),
            "BYTE_LENGTH(UNHEX('aa'))",
        )
        self.assertEqual(
            str(
                sl.concat(sl.col("label").trim().lower(), "-", sl.col("segment").upper())
                == "alpha-north"
            ),
            "CONCAT(LOWER(TRIM(label)), '-', UPPER(segment)) = 'alpha-north'",
        )
        self.assertEqual(
            str(sl.col("label").trim().replace(" ", "_").length() >= 5),
            "LENGTH(REPLACE(TRIM(label), ' ', '_')) >= 5",
        )
        self.assertEqual(str(sl.col("amount") + 5 >= 20), "amount + 5 >= 20")
        self.assertEqual(str(sl.col("amount") - 3 < 10), "amount - 3 < 10")
        self.assertEqual(str(sl.col("amount") * 2 == 40), "amount * 2 = 40")
        self.assertEqual(str(sl.col("ratio") / 2.0 > 0.5), "ratio / 2.0 > 0.5")
        self.assertEqual(str(sl.col("closed_at").is_not_null()), "closed_at IS NOT NULL")
        self.assertEqual(
            str(sl.col("label").is_distinct_from(sl.col("peer"))),
            "label IS DISTINCT FROM peer",
        )
        self.assertEqual(
            str(sl.col("label").is_not_distinct_from(None)),
            "label IS NOT DISTINCT FROM NULL",
        )
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
            sl.row_in(["id"], [(1,)])
        with self.assertRaises(ValueError):
            sl.row_in(["id", "id"], [(1, 1)])
        with self.assertRaises(ValueError):
            sl.row_in(["id", "label"], [(1,)])
        with self.assertRaises(ValueError):
            sl.col("id").isin_source("target/allowed.csv", "bad column")
        self.assertIn(
            "FROM 'target/has''quote.csv'",
            sl.col("id").isin_source("target/has'quote.csv", "id").sql,
        )
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
            sl.col("event_dt").date_add_days(sl.interval_seconds(1))
        with self.assertRaises(ValueError):
            sl.col("event_dt").date_add_days(1 << 64)
        with self.assertRaises(ValueError):
            sl.interval_days(True)
        with self.assertRaises(ValueError):
            sl.interval_seconds("1 second")
        with self.assertRaisesRegex(ValueError, "DAY, HOUR, MINUTE, or SECOND"):
            sl.IntervalLiteral(1, "WEEKS")
        with self.assertRaises(ValueError):
            sl.col("event_ts").timestamp_add_seconds(True)
        with self.assertRaises(ValueError):
            sl.col("event_ts").timestamp_add_seconds("1 minute")
        with self.assertRaises(ValueError):
            sl.col("event_ts").timestamp_add_seconds(1 << 64)
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
        with self.assertRaisesRegex(ValueError, "string expressions currently admit"):
            sl.concat(sl.col("label").length(), "x")
        with self.assertRaisesRegex(ValueError, "substring start"):
            sl.col("label").substr(0, 2)
        with self.assertRaisesRegex(ValueError, "left count"):
            sl.col("label").left(-1)
        with self.assertRaisesRegex(TypeError, "right requires"):
            sl.right("label", 2)
        with self.assertRaisesRegex(ValueError, "replace search literal"):
            sl.col("label").replace("", "x")
        with self.assertRaisesRegex(TypeError, "unhex requires"):
            sl.unhex("payload_hex")
        with self.assertRaisesRegex(TypeError, "from_base64 requires"):
            sl.from_base64("payload_b64")


    def test_schema_declared_dataframe_projection_rewrites_fail_closed(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary=self.fake_cli("")))
        workflow = ctx.read_csv(
            "target/input.csv",
            schema={"id": "int64", "amount": "int64"},
        )

        with self.assertRaisesRegex(ValueError, "rename output column names must be unique"):
            workflow.rename({"amount": "id"})
        with self.assertRaisesRegex(ValueError, "rename referenced unknown"):
            workflow.rename({"missing": "renamed"})
        with self.assertRaisesRegex(ValueError, "drop referenced unknown"):
            workflow.drop("missing")
        with self.assertRaisesRegex(ValueError, "drop must leave at least one"):
            workflow.drop("id", "amount")
        with self.assertRaisesRegex(ValueError, "fillna referenced unknown"):
            workflow.fillna({"missing": 0})
        with self.assertRaisesRegex(ValueError, "null-mask referenced unknown"):
            workflow.isna("missing")


    def test_schema_declared_dataframe_query_dropna_after_limit_fails_closed(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1] == "workflow-unsupported-plan", sys.argv
                assert sys.argv[2] == "dropna", sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported dropna",
                    "human_text": "unsupported dropna",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "operation", "value": sys.argv[2]},
                        {"key": "blocker_id", "value": "workflow.dropna.unsupported"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = (
            ctx.read_csv(
                "target/input.csv",
                schema={"id": "int64", "amount": "utf8", "label": "utf8"},
            )
            .limit(10)
            .dropna(subset=["label"])
        )

        self.assertIsInstance(report, sl.UnsupportedWorkflowOperationReport)
        self.assertEqual(report.operation, "dropna")
        self.assertFalse(report.runtime_execution)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)


    def test_local_csv_query_builder_top_n_after_limit_preserves_input_bound(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary="unused-shardloom"))
        source = ctx.read_csv("target/input.csv").select("id", "amount").limit(10)

        largest = source.nlargest(5, "amount")
        smallest = source.nsmallest(3, "amount")

        for workflow, direction, count in [(largest, "DESC", 5), (smallest, "ASC", 3)]:
            self.assertIsInstance(workflow, LazyFrame)
            statement = workflow._relation_statement()
            self.assertIn("LIMIT 10)", statement)
            self.assertIn(f"ORDER BY amount {direction}", statement)
            self.assertTrue(statement.endswith(f"LIMIT {count}"))


    def test_local_csv_query_builder_rejects_aggregate_before_join_lowering(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary=["definitely-missing-shardloom"]))

        aggregate_first = ctx.read_csv("target/fact.csv").agg(rows="count(*)")
        self.assertIsInstance(aggregate_first, LazyFrame)
        joined = aggregate_first.join(ctx.read_csv("target/dim.csv"), on="customer_id")
        self.assertIsInstance(joined, LazyFrame)

        self.assertIsNone(joined._sql_local_source_statement())


    def test_local_csv_query_builder_rejects_ambiguous_join_condition_api(self) -> None:
        ctx = ShardLoomContext(ShardLoomClient(binary=["definitely-missing-shardloom"]))
        frame = ctx.read_csv("target/fact.csv")
        dim = ctx.read_csv("target/dim.csv")

        with self.assertRaisesRegex(ValueError, "either on= equi keys or condition="):
            frame.join(dim, on="customer_id", condition="f.amount > d.threshold")

        with self.assertRaisesRegex(ValueError, "cross joins do not accept condition="):
            frame.join(dim, how="cross", condition="f.amount > d.threshold")


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


    def test_local_csv_query_builder_with_column_alias_filter_rewrites_to_expression(
        self,
    ) -> None:
        workflow = (
            sl.read_csv(
                "target/input.csv",
                schema={"id": "int64", "dirty_numeric": "utf8"},
                binary=["definitely-missing-shardloom"],
            )
            .with_column("amount_float", sl.col("dirty_numeric").cast("float64"))
            .filter(sl.col("amount_float") >= 0)
            .limit(1000)
        )

        self.assertEqual(
            workflow._sql_local_source_statement(),
            "SELECT *,CAST(dirty_numeric AS float64) AS amount_float FROM "
            "'target/input.csv' WHERE (CAST(dirty_numeric AS float64)) >= 0 LIMIT 1000",
        )


    def test_local_csv_query_builder_window_admits_final_sort(self) -> None:
        workflow = sl.read_csv(
            "target/input.csv",
            binary=["definitely-missing-shardloom"],
        ).window(sl.row_number(order_by="amount", alias="rn"))

        self.assertEqual(
            workflow.limit(5)._sql_local_source_statement(),
            "SELECT *,ROW_NUMBER() OVER (ORDER BY amount ASC) AS rn FROM 'target/input.csv' LIMIT 5",
        )
        self.assertEqual(
            workflow.distinct().limit(5)._sql_local_source_statement(),
            "SELECT DISTINCT *,ROW_NUMBER() OVER (ORDER BY amount ASC) AS rn FROM 'target/input.csv' LIMIT 5",
        )
        self.assertIsNone(workflow.select("id").limit(5)._sql_local_source_statement())
        self.assertIsNone(workflow.filter("amount > 1").limit(5)._sql_local_source_statement())
        self.assertEqual(
            workflow.sort("amount").limit(5)._sql_local_source_statement(),
            "SELECT *,ROW_NUMBER() OVER (ORDER BY amount ASC) AS rn FROM 'target/input.csv' ORDER BY amount ASC LIMIT 5",
        )


    def test_computed_column_filter_rewrite_preserves_expression_precedence(self) -> None:
        rewritten = _rewrite_predicate_with_computed_columns(
            "gross >= 40",
            (("gross", "(amount + tax) * 2"),),
        )

        self.assertEqual(rewritten, "((amount + tax) * 2) >= 40")

    def test_computed_column_filter_rewrite_expands_chained_aliases(self) -> None:
        rewritten = _rewrite_predicate_with_computed_columns(
            "gross >= 40",
            (
                ("net", "amount + tax"),
                ("gross", "net * 2"),
            ),
        )

        self.assertEqual(rewritten, "((amount + tax) * 2) >= 40")


        # Native preparation rejects Variant output for composed flat-scalar
        # relations; declarations do not infer output types or read the source.


    def test_local_csv_query_builder_sample_rng_object_stays_deterministic_blocker(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                assert args[-2:] == ["--format", "json"], args
                parts = args[:-2]
                assert parts[0] == "workflow-unsupported-plan", args
                target_ref = parts[3] if len(parts) == 4 else "none"
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "SL_UNSUPPORTED_SQL",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "cg21.workflow.sample",
                        "reason": "sample RNG object parity requires an explicit deterministic contract",
                        "suggested_next_step": "use sample(n=..., seed=<int>), sample(n=..., random_state=<int>), or sample(..., weights='<column>')",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    }],
                    "fields": [
                        {"key": "mode", "value": "workflow_unsupported_plan"},
                        {"key": "workflow_operation", "value": "sample"},
                        {"key": "target_ref", "value": target_ref},
                        {"key": "blocker_id", "value": "cg21.workflow.sample.rng_object_contract_missing"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ],
                }))
                sys.exit(1)
                """
            )
        )
        workflow = sl.read_csv("events.csv", client=ShardLoomClient(binary=binary))

        class FakeRandomState:
            pass

        rng_report = workflow.sample(n=2, random_state=FakeRandomState())

        self.assertEqual(rng_report.operation, "sample")
        self.assertEqual(
            rng_report.envelope.field("target_ref"),
            "n=2,seed=FakeRandomState",
        )
        self.assertFalse(rng_report.fallback_attempted)

        with self.assertRaisesRegex(ValueError, "sample fraction"):
            workflow.sample(frac=1.2)


    def test_local_vortex_describe_columns_without_declared_schema_blocks(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                import sys

                args = sys.argv[1:]
                assert args[0] == "workflow-unsupported-plan", args
                operation = args[1]
                target_ref = args[3] if len(args) > 3 else "none"
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "mode", "value": "workflow_unsupported_plan"},
                        {"key": "workflow_operation", "value": operation},
                        {"key": "target_ref", "value": target_ref},
                        {"key": "blocker_id", "value": "cg21.workflow.describe.declared_schema_required"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "data_read", "value": "false"},
                    ],
                }))
                sys.exit(1)
                """
            ),
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.read_vortex("target/fact.vortex").describe("id")

        self.assertIsInstance(report, sl.UnsupportedWorkflowOperationReport)
        self.assertEqual(report.operation, "describe")
        self.assertEqual(report.envelope.field("target_ref"), "columns=id")
        self.assertFalse(report.fallback_attempted)

    def test_local_csv_query_builder_write_csv_routes_through_public_run_facade(
        self,
    ) -> None:
        binary = self.fake_public_local_write_cli(
            requested_output="write_csv",
            output_path="target/out-public.csv",
            output_format="csv",
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .write_csv("target/out-public.csv", allow_overwrite=True)
        )

        self.assertIsInstance(report, sl.VortexWorkflowExecutionReport)
        self.assertEqual(report.envelope.field("public_workflow_requested_output"), "write_csv")
        self.assertEqual(report.output_path, "target/out-public.csv")
        self.assertEqual(report.output_format, "csv")
        self.assertTrue(report.output_io_performed)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)


    def test_local_csv_query_builder_write_structured_binary_uses_public_run_facade(
        self,
    ) -> None:
        writers = (
            (
                "vortex",
                "write_vortex",
                "target/out.vortex",
                lambda: (
                    ctx.read_csv("target/input.csv")
                    .select(["id", "label"])
                    .limit(2)
                    .write_vortex("target/out.vortex", allow_overwrite=True)
                ),
            ),
            (
                "parquet",
                "write_parquet",
                "target/out.parquet",
                lambda: (
                    ctx.read_csv("target/input.csv")
                    .select(["id", "label"])
                    .limit(2)
                    .write_parquet("target/out.parquet", allow_overwrite=True)
                ),
            ),
            (
                "arrow-ipc",
                "write_arrow_ipc",
                "target/out.arrow",
                lambda: (
                    ctx.read_csv("target/input.csv")
                    .select(["id", "label"])
                    .limit(2)
                    .write_arrow_ipc("target/out.arrow", allow_overwrite=True)
                ),
            ),
            (
                "avro",
                "write_avro",
                "target/out.avro",
                lambda: (
                    ctx.read_csv("target/input.csv")
                    .select(["id", "label"])
                    .limit(2)
                    .write_avro("target/out.avro", allow_overwrite=True)
                ),
            ),
        )

        for output_format, requested_output, output_path, writer in writers:
            with self.subTest(output_format=output_format):
                cli_format = {
                    "vortex": "vortex",
                    "parquet": "parquet",
                    "arrow-ipc": "arrow_ipc",
                    "avro": "avro",
                }[output_format]
                binary = self.fake_public_local_write_cli(
                    requested_output=requested_output,
                    output_path=output_path,
                    output_format=cli_format,
                )
                ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))
                report = writer()
                self.assertIsInstance(report, sl.VortexWorkflowExecutionReport)
                self.assertEqual(
                    report.envelope.field("public_workflow_requested_output"),
                    requested_output,
                )
                self.assertEqual(report.output_path, output_path)
                self.assertEqual(report.output_format, cli_format)
                self.assertTrue(report.output_io_performed)
                self.assertFalse(report.fallback_attempted)
                self.assertFalse(report.external_engine_invoked)

    def test_local_csv_query_builder_write_parquet_exposes_typed_nested_sink_boundary(
        self,
    ) -> None:
        binary = self.fake_public_local_write_cli(
            requested_output="write_parquet",
            output_path="target/nested.parquet",
            output_format="parquet",
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))

        report = (
                ctx.read_csv("target/input.csv")
                .select("id")
                .with_columns(
                    {
                        "values": sl.array(1, 2, None),
                        "payload": sl.struct("label", "amount"),
                    }
                )
                .limit(2)
                .write_parquet("target/nested.parquet", allow_overwrite=True)
        )

        self.assertIsInstance(report, sl.VortexWorkflowExecutionReport)
        self.assertEqual(report.envelope.field("public_workflow_requested_output"), "write_parquet")
        self.assertEqual(report.output_path, "target/nested.parquet")
        self.assertEqual(report.output_format, "parquet")
        self.assertTrue(report.output_io_performed)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)


    def test_local_csv_query_builder_write_vortex_uses_public_run_facade(self) -> None:
        binary = self.fake_public_local_write_cli(
            requested_output="write_vortex",
            output_path="target/out.vortex",
            output_format="vortex",
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))

        report = (
            ctx.read_csv("target/input.csv")
            .select(["id", "label"])
            .limit(2)
            .write_vortex("target/out.vortex", allow_overwrite=True)
        )

        self.assertIsInstance(report, sl.VortexWorkflowExecutionReport)
        self.assertEqual(report.envelope.field("public_workflow_requested_output"), "write_vortex")
        self.assertEqual(report.output_path, "target/out.vortex")
        self.assertEqual(report.output_format, "vortex")
        self.assertTrue(report.output_io_performed)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)


    def test_local_csv_query_builder_write_parquet_blocks_unadmitted_expression_shape(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                assert args[:6] == ["run", "dataframe", "--input", "target/input.csv", "--input-format", "csv"], sys.argv
                assert args[args.index("--sql") + 1] == "SELECT * FROM (SELECT id,amount + 5 AS adjusted FROM (SELECT id FROM (SELECT * FROM 'target/input.csv') AS _sl_stage_0) AS _sl_stage_1) AS _sl_stage_2 LIMIT 2", sys.argv
                assert args[args.index("--plan") + 1] == "read_csv(target/input.csv) -> select(id) -> with_column(adjusted,amount + 5) -> limit(2)", sys.argv
                assert args[args.index("--request") + 1] == "write_parquet", sys.argv
                assert args[args.index("--output") + 1] == "target/out.parquet", sys.argv
                assert args[args.index("--execution-policy") + 1] == "vortex_middle", sys.argv
                assert args[args.index("--materialization-policy") + 1] == "bounded", sys.argv
                assert args[args.index("--evidence-level") + 1] == "production_admitted_local_workflow", sys.argv
                assert args[args.index("--bounded") + 1] == "true", sys.argv
                assert args[args.index("--max-parallelism") + 1] == "2", sys.argv
                assert "--allow-overwrite" in args, sys.argv
                assert args[-2:] == ["--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "run",
                    "status": "unsupported",
                    "summary": "unadmitted structured Parquet expression blocked",
                    "human_text": "unadmitted structured Parquet expression blocked",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "public_workflow_route_id", "value": "blocked"},
                        {"key": "public_workflow_route_status", "value": "blocked"},
                        {"key": "public_workflow_requested_output", "value": "write_parquet"},
                        {"key": "public_workflow_blocker_id", "value": "cg21.route.local_file_vortex_middle_required"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "output_io_performed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "blocked"}
                    ],
                }))
                sys.exit(1)
                """
            ),
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))

        with self.assertRaises(sl.ShardLoomCommandError):
            (
                ctx.read_csv("target/input.csv")
                .select("id")
                .with_column("adjusted", sl.col("amount") + 5)
                .limit(2)
                .write_parquet("target/out.parquet", allow_overwrite=True)
            )


    def test_from_rows_validates_scoped_generated_source_inputs(self) -> None:
        client = ShardLoomClient(binary=["definitely-missing-shardloom"], memory_gb=4, max_parallelism=2)
        with self.assertRaises(ValueError):
            sl.from_rows([], client=client)
        with self.assertRaises(TypeError):
            sl.from_rows([object()], client=client)  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            sl.from_rows(
                [{"id": 1}, {"id": 2, "label": "extra"}],
                client=client,
            )
        with self.assertRaises(TypeError):
            sl.from_rows(
                [{"id": 1}, {"id": "two"}],
                client=client,
            )
        with self.assertRaises(ValueError):
            sl.literal_table([], client=client)
        with self.assertRaises(ValueError):
            sl.calendar(
                "2026-05-20",
                "2026-05-18",
                client=client,
            )
        source = sl.from_rows([{"id": 1}], client=client)
        self.assertIn("SELECT missing", source.select("missing")._relation_statement())
        self.assertIn("id + 1 AS bad", source.with_column("bad", sl.col("id") + 1)._relation_statement())
        self.assertIn("NULL AS bad", source.with_column("bad", "lit(null)")._relation_statement())


    def test_range_limit_aliases_and_validation(self) -> None:
        source = sl.range(10, 0, step=-2, binary=["definitely-missing-shardloom"])

        for count in (0, 1, 2, 100):
            limited = source.limit(count)
            self.assertEqual(limited.source.memory_input, source.source.memory_input)
            self.assertEqual(limited.operations[-1], WorkflowOperation("limit", (str(count),)))
            self.assertIn(f"LIMIT {count}", limited._relation_statement())
        self.assertEqual(source.operations, ())
        with self.assertRaises(TypeError):
            source.limit(True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            source.limit(-1)


    def test_range_validates_scoped_generated_source_inputs(self) -> None:
        with self.assertRaises(TypeError):
            sl.range(True, 10, binary=["definitely-missing-shardloom"])
        with self.assertRaises(TypeError):
            sl.range(0, "10", binary=["definitely-missing-shardloom"])  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            sl.range(0, 10, step=0, binary=["definitely-missing-shardloom"])
        with self.assertRaises(ValueError):
            sl.range(0, 10, column="", binary=["definitely-missing-shardloom"])


    def test_context_sql_embedded_vortex_manifest_broad_query_uses_native_input_binding(self) -> None:
        statement = (
            "SELECT COUNT(*) FROM 'hits_parts.vortex-manifest' "
            "WHERE AdvEngineID <> 0"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                assert sys.argv[1:] == [
                    "run",
                    "sql",
                    "--input",
                    "hits_parts.vortex-manifest",
                    "--input-format",
                    "vortex",
                    "--sql",
                    {statement!r},
                    "--plan",
                    "sql(statement)",
                    "--request",
                    "collect",
                    "--execution-policy",
                    "vortex_middle",
                    "--materialization-policy",
                    "bounded",
                    "--evidence-level",
                    "production_admitted_local_workflow",
                    "--bounded",
                    "true",
                    "--memory-bytes",
                    "4294967296",
                    "--max-parallelism",
                    "1",
                    "--memory-origin",
                    "execution_call",
                    "--parallelism-origin",
                    "execution_call",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": "run",
                    "status": "success",
                    "summary": "native Vortex count_where primitive",
                    "human_text": "public workflow run",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": [],
                    "fields": [
                        {{"key": "public_workflow_route_id", "value": "native_vortex_count_where"}},
                        {{"key": "native_vortex_input_binding_mode", "value": "manifest"}},
                        {{"key": "native_vortex_partitioned_input_binding", "value": "true"}},
                        {{"key": "fallback_attempted", "value": "false"}},
                        {{"key": "external_engine_invoked", "value": "false"}}
                    ],
                }}))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        report = ctx.sql(statement).collect(
            memory_gb=4,
            max_parallelism=1,
        )

        self.assertEqual(report.envelope.command, "run")
        self.assertEqual(
            report.envelope.field("public_workflow_route_id"),
            "native_vortex_count_where",
        )
        self.assertEqual(report.envelope.field("native_vortex_input_binding_mode"), "manifest")
        self.assertTrue(report.envelope.field_bool("native_vortex_partitioned_input_binding"))
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)


    def test_sql_without_limit_preserves_native_collection_budget_denial(self) -> None:
        client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        failure = OutputEnvelope.from_field_mapping(
            {"reason": "native admission denied the declared query", "fallback_attempted": "false",
             "external_engine_invoked": "false"}, command="run", status="error")
        for statement in ["SELECT id FROM 'target/input.csv'"]:
            with self.subTest(statement=statement), mock.patch.object(
                client, "public_workflow_run", return_value=SimpleNamespace(envelope=failure)
            ) as execute, mock.patch.object(client, "workflow_unsupported_plan") as unsupported:
                report = ShardLoomContext(client).sql(statement).collect(check=False)
                execute.assert_called_once()
                unsupported.assert_not_called()
                self.assertEqual(execute.call_args.kwargs["sql_statement"], statement)
                self.assertEqual(execute.call_args.kwargs["materialization_policy"], "bounded")
                self.assertNotIn("input_uri", execute.call_args.kwargs)
                self.assertNotIn("source_bindings", execute.call_args.kwargs)
                self.assertIs(report.envelope, failure)


    def test_source_free_sql_collect_returns_the_native_payload(self) -> None:
        client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        reply = OutputEnvelope.from_field_mapping({
            "result_jsonl": '{"column_1":1,"column_2":"alpha"}\n',
            "result_schema_format": "vortex.dtype.serde.v1",
            "result_schema_json": json.dumps({"Struct": [{"names": ["column_1", "column_2"],
                "dtypes": [{"Primitive": ["i64", False]}, {"Utf8": False}]}, False]}),
            "fallback_attempted": "false", "external_engine_invoked": "false",
        }, command="run")
        with mock.patch.object(client, "public_workflow_run", return_value=SimpleNamespace(envelope=reply)) as execute:
            report = ShardLoomContext(client).sql("VALUES (1, 'alpha')").collect(check=True)
            execute.assert_called_once()
            self.assertEqual(execute.call_args.kwargs["sql_statement"], "VALUES (1, 'alpha')")
            self.assertEqual(report.python_objects, ({"column_1": 1, "column_2": "alpha"},))

    def test_sql_table_admission_errors_are_owned_by_the_native_engine(self) -> None:
        client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        failure = OutputEnvelope.from_field_mapping(
            {"reason": "native admission denied the declared query", "fallback_attempted": "false",
             "external_engine_invoked": "false"}, command="run", status="error")
        for statement in ['SELECT * FROM events']:
            with self.subTest(statement=statement), mock.patch.object(
                client, "public_workflow_run", return_value=SimpleNamespace(envelope=failure)
            ) as execute, mock.patch.object(client, "workflow_unsupported_plan") as unsupported:
                report = ShardLoomContext(client).sql(statement).collect(check=False)
                execute.assert_called_once()
                unsupported.assert_not_called()
                self.assertEqual(execute.call_args.kwargs["sql_statement"], statement)
                self.assertEqual(execute.call_args.kwargs["materialization_policy"], "bounded")
                self.assertNotIn("input_uri", execute.call_args.kwargs)
                self.assertNotIn("source_bindings", execute.call_args.kwargs)
                self.assertIs(report.envelope, failure)

    def test_projected_file_literal_never_becomes_a_source_binding(self) -> None:
        client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        failure = OutputEnvelope.from_field_mapping(
            {"reason": "native admission denied the declared query", "fallback_attempted": "false",
             "external_engine_invoked": "false"}, command="run", status="error")
        for statement in ["SELECT 'target/input.csv' AS path FROM events", "SELECT 'bait.vortex' AS path FROM events"]:
            with self.subTest(statement=statement), mock.patch.object(
                client, "public_workflow_run", return_value=SimpleNamespace(envelope=failure)
            ) as execute, mock.patch.object(client, "workflow_unsupported_plan") as unsupported:
                report = ShardLoomContext(client).sql(statement).collect(check=False)
                execute.assert_called_once()
                unsupported.assert_not_called()
                self.assertEqual(execute.call_args.kwargs["sql_statement"], statement)
                self.assertEqual(execute.call_args.kwargs["materialization_policy"], "bounded")
                self.assertNotIn("input_uri", execute.call_args.kwargs)
                self.assertNotIn("source_bindings", execute.call_args.kwargs)
                self.assertIs(report.envelope, failure)

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
                    "drop-duplicates": "drop_duplicates",
                    "pivot-table": "pivot_table",
                    "value-counts": "value_counts",
                    "map-rows": "map_rows",
                    "set-index": "set_index",
                    "reset-index": "reset_index",
                    "sort-index": "sort_index",
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
                    or operation == "foundry-generated-output"
                )
                materialization_required = operation in {
                    "collect", "from-pandas", "from-arrow-table", "from-arrow-ipc",
                    "to-pandas", "to-arrow", "to-arrow-table", "to-arrow-ipc",
                    "to-numpy", "to-python-objects", "write-vortex", "write-parquet",
                    "write-arrow-ipc", "write-avro", "write-orc",
                    "quarantine", "preview", "head", "take", "display",
                    "tail", "describe", "nunique", "value-counts", "value_counts",
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
                        "aggregate", "window", "merge", "pivot-table", "rolling",
                        "dropna", "astype", "nlargest", "nsmallest",
                        "duplicated", "drop-duplicates", "mask", "replace",
                        "set-index", "reset-index", "sort-index",
                    }
                    else "SL_UNSUPPORTED_EFFECT"
                    if operation in {
                        "quarantine", "apply", "pipe", "transform", "applymap",
                        "map", "map-rows", "eval",
                    }
                    else "SL_MATERIALIZATION_REQUIRED"
                    if materialization_required
                    else "SL_NOT_IMPLEMENTED"
                )
                blocker_ids = {
                    "melt": "cg21.workflow.melt.nested_or_broad_index_contract_missing",
                }
                blocker_id = blocker_ids.get(
                    canonical,
                    f"cg21.workflow.{canonical}.unsupported",
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
                        {"key": "blocker_id", "value": blocker_id},
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
            sl.read_csv("events.csv", client=ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))
            .filter("id > 0")
            .select("id", "amount")
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))

        reports = (
            sl.from_pandas(object(), client=ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2)),
            sl.from_arrow_table(object(), client=ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2)),
            sl.from_arrow_ipc("events.arrow", client=ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2)),
            workflow.with_column("date", "to_date(ts)"),
            workflow.sort("amount", "amount", descending=True),
            workflow.dropna(axis=1),
            workflow.fillna({"amount": 0}, method="ffill"),
            workflow.sample(random_state="seed"),
            workflow.explode("items.payload.deep"),
            workflow.merge(
                sl.read_csv("other.csv", client=ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2)),
                how="left",
            ),
            workflow.concat([sl.read_csv("other.csv", client=ShardLoomClient(binary=binary, memory_gb=4, max_parallelism=2))]),
            workflow.pivot(index=["id", "customer_id"], columns="label", values="amount"),
            workflow.pivot_table(
                index="id",
                columns="label",
                values="amount",
                aggfunc="median",
            ),
            workflow.melt(id_vars="id", value_vars=["amount"], col_level=0),
            workflow.rolling(window="3d"),
            workflow.duplicated(subset=["id"], keep="first", ignore_index=True),
            workflow.mask("amount < 0", other=0, axis=1),
            workflow.replace("bad", "good", regex=True, limit=1),
            workflow.apply("row_udf"),
            workflow.pipe("workflow_udf", "arg1", config="strict"),
            workflow.transform("column_udf"),
            workflow.applymap("cell_udf"),
            workflow.map("value_udf"),
            workflow.map_rows("row_udf"),
            workflow.eval("amount + tax", engine="python"),
            workflow.set_index("id"),
            workflow.sort_index(ascending=False),
            ctx.sql_parse("select * from events"),
            ctx.sql_bind("select * from events"),
            ctx.sql_plan("select * from events"),
            ctx.sql_execute("select * from events"),
            workflow.data_quality_check("regex:id"),
            ctx.foundry_generated_output("foundry://dataset/output"),
        )

        self.assertEqual(len(reports), 33)
        for index, report in enumerate(reports):
            self.assertIsInstance(report, sl.UnsupportedWorkflowOperationReport, f"entry {index}: {report}")
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
            elif report.operation == "foundry-generated-output":
                self.assertTrue(
                    report.envelope.field("workflow_summary", "").startswith("source_free(")
                )
            elif report.operation == "schema":
                summary = report.envelope.field("workflow_summary") or ""
                self.assertTrue(
                    summary.startswith("read_sql(statement)")
                    or summary.startswith("sql(statement)")
                )
            elif report.operation == "preview":
                summary = report.envelope.field("workflow_summary")
                self.assertTrue(
                    summary == "read_csv(events.data)"
                    or (summary or "").startswith("read_sql(statement)")
                    or (summary or "").startswith("sql(statement)")
                )
            elif report.operation in {"head", "take"}:
                self.assertEqual(report.envelope.field("workflow_summary"), "read_csv(events.data)")
            elif report.operation == "display":
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
        self.assertEqual(
            by_operation["sort"].envelope.field("target_ref"),
            "desc:amount,amount",
        )
        self.assertEqual(
            by_operation["dropna"].envelope.field("target_ref"),
            "subset=none;how=any;axis=columns",
        )
        self.assertEqual(
            by_operation["fillna"].envelope.field("target_ref"),
            "value={amount=0};axis=rows;inplace=false;method=ffill",
        )
        self.assertEqual(
            by_operation["sample"].envelope.field("target_ref"),
            "n=1,seed=seed",
        )
        self.assertEqual(
            by_operation["explode"].envelope.field("target_ref"),
            "items.payload.deep",
        )
        self.assertEqual(
            by_operation["pipe"].envelope.field("target_ref"),
            "callable=workflow_udf;arg_count=1;config=strict",
        )
        self.assertEqual(
            by_operation["transform"].envelope.field("target_ref"),
            "callable=column_udf",
        )
        self.assertEqual(
            by_operation["applymap"].envelope.field("target_ref"),
            "callable=cell_udf",
        )
        self.assertEqual(
            by_operation["eval"].envelope.field("target_ref"),
            "expr=amount + tax;engine=python",
        )
        self.assertEqual(
            by_operation["merge"].envelope.field("target_ref"),
            "how=left;on=implicit_common_columns;read_csv(other.csv)",
        )
        self.assertEqual(
            by_operation["concat"].envelope.field("target_ref"),
            "axis=0;join=outer;read_csv(other.csv)",
        )
        self.assertEqual(
            by_operation["pivot"].envelope.field("target_ref"),
            "index=id,customer_id;columns=label;values=amount",
        )
        self.assertEqual(
            by_operation["pivot-table"].envelope.field("workflow_operation"),
            "pivot_table",
        )
        self.assertEqual(
            by_operation["pivot-table"].envelope.field("target_ref"),
            "index=id;columns=label;values=amount;aggfunc=median",
        )
        self.assertEqual(
            by_operation["melt"].envelope.field("target_ref"),
            "id_vars=id;value_vars=amount;ignore_index=true;col_level=0",
        )
        self.assertEqual(
            by_operation["melt"].blocker_id,
            "cg21.workflow.melt.nested_or_broad_index_contract_missing",
        )
        self.assertEqual(
            by_operation["rolling"].envelope.field("target_ref"),
            "window=3d;center=false",
        )
        self.assertEqual(
            by_operation["duplicated"].envelope.field("target_ref"),
            "subset=id;keep=first;ignore_index=True",
        )
        self.assertEqual(
            by_operation["mask"].envelope.field("target_ref"),
            "cond=amount < 0;other=0;axis=columns;inplace=false;level=null",
        )
        self.assertEqual(
            by_operation["replace"].envelope.field("target_ref"),
            "to_replace=bad;value=good;regex=true;inplace=false;method=null;limit=1",
        )
        self.assertEqual(by_operation["apply"].envelope.field("target_ref"), "callable=row_udf")
        self.assertEqual(by_operation["map"].envelope.field("target_ref"), "callable=value_udf")
        self.assertEqual(
            by_operation["map-rows"].envelope.field("workflow_operation"),
            "map_rows",
        )
        self.assertEqual(by_operation["map-rows"].envelope.field("target_ref"), "callable=row_udf")
        self.assertEqual(
            by_operation["set-index"].envelope.field("workflow_operation"),
            "set_index",
        )
        self.assertEqual(
            by_operation["set-index"].envelope.field("target_ref"),
            "keys=id;drop=true",
        )
        self.assertEqual(
            by_operation["sort-index"].envelope.field("workflow_operation"),
            "sort_index",
        )
        self.assertEqual(
            by_operation["sort-index"].envelope.field("target_ref"),
            "ascending=false",
        )
        self.assertFalse(by_operation["sql-parse"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["sql-bind"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["sql-plan"].envelope.field_bool("runtime_required"))
        self.assertTrue(by_operation["sql-execute"].envelope.field_bool("runtime_required"))
        self.assertFalse(by_operation["data-quality"].envelope.field_bool("runtime_required"))
        self.assertTrue(
            by_operation["foundry-generated-output"].envelope.field_bool("write_required")
        )

    def test_scoped_index_noop_affordances_preserve_lazy_plan(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1] == "workflow-unsupported-plan", sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported index operation",
                    "human_text": "unsupported index operation",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "operation", "value": sys.argv[2]},
                        {"key": "blocker_id", "value": "test.unsupported_index_shape"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "not_claim_grade"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))
        workflow = (
            ctx.read_csv("events.csv")
            .filter("id > 0")
            .select("id", "amount")
        )

        reset = workflow.reset_index(drop=True)
        sorted_by_index = workflow.sort_index()
        explicit_sorted_by_index = workflow.sort_index(ascending=True)
        indexed = workflow.set_index("id", drop=False)

        self.assertIs(reset, workflow)
        self.assertIs(sorted_by_index, workflow)
        self.assertIs(explicit_sorted_by_index, workflow)
        self.assertIsInstance(indexed, sl.LazyFrame)
        self.assertEqual(
            workflow.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount)",
        )
        self.assertEqual(
            indexed.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> set_index(id)",
        )

        reset_materialized = workflow.reset_index()
        descending_sort = workflow.sort_index(ascending=False)
        dropped_index_column = workflow.set_index("id")
        sorted_explicit_index = indexed.sort_index()
        descending_explicit_index = indexed.sort_index(ascending=False)
        reset_explicit_index = indexed.reset_index()
        reset_drop_explicit_index = indexed.reset_index(drop=True)

        self.assertIsInstance(reset_materialized, sl.LazyFrame)
        self.assertEqual(reset_materialized.operations[-1].kind, "expression_project")
        self.assertEqual(json.loads(reset_materialized.operations[-1].values[0])["rewrites"], [
            {"kind": "row_number", "start": 0, "target_column": "index"},
        ])
        self.assertIsInstance(descending_sort, sl.UnsupportedWorkflowOperationReport)
        self.assertEqual(descending_sort.operation, "sort-index")
        self.assertFalse(descending_sort.fallback_attempted)
        self.assertIsInstance(dropped_index_column, sl.UnsupportedWorkflowOperationReport)
        self.assertEqual(dropped_index_column.operation, "set-index")
        self.assertIsInstance(sorted_explicit_index, sl.LazyFrame)
        self.assertEqual(
            sorted_explicit_index.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> "
            "set_index(id) -> sort(asc,id)",
        )
        self.assertIsInstance(descending_explicit_index, sl.LazyFrame)
        self.assertEqual(
            descending_explicit_index.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> "
            "set_index(id) -> sort(desc,id)",
        )
        self.assertIsInstance(reset_explicit_index, sl.LazyFrame)
        self.assertEqual(
            reset_explicit_index.operations,
            reset_materialized.operations,
        )
        self.assertIsInstance(reset_drop_explicit_index, sl.LazyFrame)
        self.assertEqual(
            reset_drop_explicit_index.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount)",
        )
        self.assertFalse(dropped_index_column.fallback_attempted)

    def test_schema_free_explicit_column_rewrites_stay_on_lazy_vortex_route(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import sys

                raise AssertionError(f"unexpected fake CLI argv: {sys.argv[1:]}")
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))
        workflow = ctx.read_csv("events.csv")

        dropna = workflow.dropna(subset=["label"])
        dropna_all = workflow.select("id", "label").dropna(how="all")
        dropna_thresh = workflow.select("id", "label", "amount").dropna(thresh=2)
        dropna_noop = workflow.select("id", "label").dropna(thresh=0)
        isna = workflow.isna("amount")
        notna = workflow.notna("amount")
        fillna = workflow.select("id", "amount").fillna({"amount": 0})
        fillna_axis = workflow.select("id", "amount").fillna(
            {"amount": 0},
            axis=0,
            inplace=False,
        )
        fillna_columns_axis = workflow.select("id", "amount").fillna(
            {"amount": 0},
            axis=1,
            inplace=False,
        )
        fill_null_axis = workflow.select("id", "amount").fill_null(
            {"amount": 0},
            axis="index",
        )
        fill_null_columns_axis = workflow.select("id", "amount").fill_null(
            {"amount": 0},
            axis="columns",
        )
        astype = workflow.select("id", "amount").astype({"amount": "int64"})
        renamed = workflow.select("id", "amount").rename({"amount": "order_amount"})
        dropped = workflow.select("id", "amount").drop(columns=["amount"])
        dedup = workflow.select("id").drop_duplicates(subset=["id"])

        for frame in (
            dropna,
            dropna_all,
            dropna_thresh,
            dropna_noop,
            isna,
            notna,
            fillna,
            fillna_axis,
            fillna_columns_axis,
            fill_null_axis,
            fill_null_columns_axis,
            astype,
            renamed,
            dropped,
            dedup,
        ):
            self.assertIsInstance(frame, sl.LazyFrame)

        self.assertEqual(
            dropna.operation_summary,
            "read_csv(events.csv) -> filter(label IS NOT NULL)",
        )
        self.assertEqual(
            dropna_all.operation_summary,
            "read_csv(events.csv) -> select(id,label) -> filter(id IS NOT NULL OR label IS NOT NULL)",
        )
        self.assertEqual(
            dropna_thresh.operation_summary,
            "read_csv(events.csv) -> select(id,label,amount) -> "
            "filter((id IS NOT NULL AND label IS NOT NULL) OR "
            "(id IS NOT NULL AND amount IS NOT NULL) OR "
            "(label IS NOT NULL AND amount IS NOT NULL))",
        )
        self.assertEqual(
            dropna_noop.operation_summary,
            "read_csv(events.csv) -> select(id,label)",
        )
        self.assertEqual(
            isna.operation_summary,
            "read_csv(events.csv) -> select(amount IS NULL AS amount)",
        )
        self.assertEqual(
            notna.operation_summary,
            "read_csv(events.csv) -> select(amount IS NOT NULL AS amount)",
        )
        self.assertEqual(
            fillna.operation_summary,
            "read_csv(events.csv) -> select(id,amount) -> "
            "select(id,COALESCE(amount, 0) AS amount)",
        )
        self.assertEqual(fillna_axis.operation_summary, fillna.operation_summary)
        self.assertEqual(fillna_columns_axis.operation_summary, fillna.operation_summary)
        self.assertEqual(fill_null_axis.operation_summary, fillna.operation_summary)
        self.assertEqual(fill_null_columns_axis.operation_summary, fillna.operation_summary)
        self.assertEqual(
            astype.operation_summary,
            "read_csv(events.csv) -> select(id,amount) -> "
            "select(id,CAST(amount AS int64) AS amount)",
        )
        self.assertEqual(
            renamed.operation_summary,
            "read_csv(events.csv) -> select(id,amount) -> "
            "select(id,amount AS order_amount)",
        )
        self.assertEqual(
            dropped.operation_summary,
            "read_csv(events.csv) -> select(id,amount) -> select(id)",
        )
        self.assertEqual(
            dedup.operation_summary,
            "read_csv(events.csv) -> select(id) -> drop_duplicates(id,keep=first)",
        )


    def test_embedded_vortex_input_uri_requires_all_refs_to_be_one_vortex_source(self) -> None:
        self.assertEqual(
            _embedded_vortex_input_uri("SELECT * FROM 'fact.vortex' LIMIT 1"),
            "fact.vortex",
        )
        self.assertIsNone(
            _embedded_vortex_input_uri(
                "SELECT * FROM 'fact.vortex' f JOIN 'dim.csv' d ON f.id = d.id"
            )
        )
        self.assertIsNone(
            _embedded_vortex_input_uri(
                "SELECT * FROM 'fact.vortex' f JOIN 'dim.vortex' d ON f.id = d.id"
            )
        )


    def test_generated_output_to_remote_object_store_is_report_only_without_staging(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                assert args == [
                    "workflow-unsupported-plan",
                    "object-store-generated-output",
                    "source_free(object_store_generated_output)",
                    "s3://bucket/out.jsonl",
                    "--format",
                    "json",
                ], args
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "remote object-store generated output remains blocked",
                    "human_text": "remote object-store generated output remains blocked",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "SL_OBJECT_STORE_UNSUPPORTED",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "remote object-store generated output remains blocked",
                        "feature": "cg21.workflow.object_store_generated_output",
                        "reason": "live object-store providers are outside the scoped local-emulator route",
                        "suggested_next_step": "use a local-emulator fixture path or inspect the object-store runtime plan",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    }],
                    "fields": [
                        {"key": "workflow_operation", "value": "object_store_generated_output"},
                        {"key": "workflow_summary", "value": "source_free(object_store_generated_output)"},
                        {"key": "target_ref", "value": "s3://bucket/out.jsonl"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ],
                }))
                """
            )
        )

        report = ShardLoomContext(
            ShardLoomClient(binary=binary)
        ).generated_output_to_object_store(
            "s3://bucket/out.jsonl",
            check=False,
        )

        self.assertEqual(report.envelope.command, "workflow-unsupported-plan")
        self.assertEqual(report.operation, "object-store-generated-output")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

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
                    {"key": "universal_compatibility_runtime_supported_count", "value": "2"},
                    {"key": "universal_compatibility_smoke_supported_count", "value": "1"},
                    {"key": "universal_compatibility_report_only_count", "value": "1"},
                    {"key": "universal_compatibility_blocked_count", "value": "0"},
                    {"key": "universal_compatibility_claim_boundary", "value": "capability map only"},
                    {"key": "universal_compatibility_all_rows_fallback_attempted_false", "value": "true"},
                    {"key": "universal_compatibility_all_rows_external_engine_invoked_false", "value": "true"},
                    {"key": "universal_compatibility_object_store_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_table_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_foundry_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_sql_dataframe_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_contract_schema_version", "value": "shardloom.universal_compatibility.generated_output_contract.v1"},
                    {"key": "universal_compatibility_generated_output_contract_id", "value": "gar-compat-1b.source_free_generated_output_contract"},
                    {"key": "universal_compatibility_generated_output_row_order", "value": "no_dataset_smoke,python_ctx_from_rows,python_ctx_range,python_ctx_sequence,python_ctx_literal_table,python_ctx_calendar,python_generated_source_write,local_output_only_generated_source_posture,sql_literal_select,sql_values,sql_source_free_projection,sql_generate_series_range,dataframe_source_free_projection,dataframe_generated_with_column,object_store_local_emulator_generated_output,object_store_live_provider_generated_output,foundry_style_generated_output,foundry_live_platform_generated_output"},
                    {"key": "universal_compatibility_generated_output_python_row_order", "value": "python_ctx_from_rows,python_ctx_range,python_ctx_sequence,python_ctx_literal_table,python_ctx_calendar,python_generated_source_write"},
                    {"key": "universal_compatibility_generated_output_sql_row_order", "value": "sql_literal_select,sql_values,sql_source_free_projection,sql_generate_series_range"},
                    {"key": "universal_compatibility_generated_output_dataframe_row_order", "value": "dataframe_source_free_projection,dataframe_generated_with_column"},
                    {"key": "universal_compatibility_generated_output_platform_row_order", "value": "object_store_local_emulator_generated_output,object_store_live_provider_generated_output,foundry_style_generated_output,foundry_live_platform_generated_output"},
                    {"key": "universal_compatibility_generated_output_claim_gate_status", "value": "fixture_smoke_only"},
                    {"key": "universal_compatibility_generated_output_no_dataset_smoke_separate", "value": "true"},
                    {"key": "universal_compatibility_generated_output_local_output_only", "value": "true"},
                    {"key": "universal_compatibility_generated_output_output_certificate_required", "value": "true"},
                    {"key": "universal_compatibility_generated_output_object_store_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_object_store_local_emulator_runtime_supported", "value": "true"},
                    {"key": "universal_compatibility_generated_output_foundry_runtime_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_foundry_style_runtime_supported", "value": "true"},
                    {"key": "universal_compatibility_generated_output_live_platform_api_supported", "value": "false"},
                    {"key": "universal_compatibility_generated_output_broad_sql_dataframe_claim_allowed", "value": "false"},
                    {"key": "universal_compatibility_generated_output_all_rows_fallback_attempted_false", "value": "true"},
                    {"key": "universal_compatibility_generated_output_all_rows_external_engine_invoked_false", "value": "true"},
                    {"key": "universal_compatibility_object_store_ladder_schema_version", "value": "shardloom.universal_compatibility.object_store_admission_ladder.v1"},
                    {"key": "universal_compatibility_object_store_ladder_id", "value": "gar-compat-1c.object_store_runtime_admission_ladder"},
                    {"key": "universal_compatibility_object_store_ladder_provider_scope", "value": "s3,gcs,adls"},
                    {"key": "universal_compatibility_object_store_ladder_row_order", "value": "object_store_uri_parse,credential_policy,public_no_credential_read,authenticated_read,byte_range_read,full_file_read,local_cache,write_staging,commit_protocol"},
                    {"key": "universal_compatibility_object_store_ladder_runtime_supported", "value": "true"},
                    {"key": "universal_compatibility_object_store_ladder_public_no_credential_read_supported", "value": "true"},
                    {"key": "universal_compatibility_object_store_ladder_all_rows_no_effects", "value": "false"},
                    {"key": "universal_compatibility_object_store_ladder_all_live_provider_effects_disabled", "value": "true"},
                    {"key": "universal_compatibility_object_store_ladder_all_rows_no_fallback_no_external_engine", "value": "true"},
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
                for row_id, stage, status, credential_policy_status, byte_range, full_file, object_io, native_status, claim_status, blocker in [
                    ("object_store_uri_parse", "uri_parse", "report-only", "not_required_for_parse", "false", "false", "false", "not_emitted_report_only", "not_claim_grade", "gar-compat-1c.uri_parse_only_no_provider_runtime"),
                    ("credential_policy", "credential_policy", "blocked", "required_not_admitted", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.credential_resolution_blocked"),
                    ("public_no_credential_read", "public_no_credential_read", "smoke-supported", "public_no_credential_fixture_admitted", "true", "true", "true", "public_fixture_smoke_only", "public_fixture_smoke_only", "none_public_no_credential_fixture_profile_only"),
                    ("authenticated_read", "authenticated_read", "blocked", "authenticated_read_policy_required", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.authenticated_read_runtime_blocked"),
                    ("byte_range_read", "byte_range_read", "blocked", "read_policy_required", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.byte_range_read_runtime_blocked"),
                    ("full_file_read", "full_file_read", "blocked", "read_policy_required", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.full_file_read_runtime_blocked"),
                    ("local_cache", "local_cache", "blocked", "cache_source_policy_required", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.local_cache_runtime_blocked"),
                    ("write_staging", "write_staging", "blocked", "write_policy_required", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.write_staging_runtime_blocked"),
                    ("commit_protocol", "commit_protocol", "blocked", "commit_policy_required", "false", "false", "false", "not_emitted_blocked", "not_claim_grade", "gar-compat-1c.commit_protocol_runtime_blocked"),
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
                        {"key": f"{prefix}_byte_range_read_allowed", "value": byte_range},
                        {"key": f"{prefix}_full_file_read_allowed", "value": full_file},
                        {"key": f"{prefix}_local_cache_allowed", "value": "false"},
                        {"key": f"{prefix}_write_io_allowed", "value": "false"},
                        {"key": f"{prefix}_commit_protocol_allowed", "value": "false"},
                        {"key": f"{prefix}_object_store_io", "value": object_io},
                        {"key": f"{prefix}_write_io", "value": "false"},
                        {"key": f"{prefix}_native_io_certificate_status", "value": native_status},
                        {"key": f"{prefix}_fallback_attempted", "value": "false"},
                        {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                        {"key": f"{prefix}_blocker_id", "value": blocker},
                        {"key": f"{prefix}_required_evidence", "value": "future_evidence"},
                        {"key": f"{prefix}_claim_gate_status", "value": claim_status},
                        {"key": f"{prefix}_claim_boundary", "value": "claim boundary"},
                    ])
                for row_id, surface, family, status, runtime, write_io, generated, output_io, source_cert, output_cert, generated_cert, claim_status, blocker in [
                    ("no_dataset_smoke", "no-dataset smoke / capability proof", "no_dataset_smoke", "smoke-supported", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_no_output_data", "not_applicable_no_generated_rows", "smoke_only", "gar-gen-1.no_dataset_smoke_not_generated_output"),
                    ("python_ctx_from_rows", "Python ctx.from_rows([...]).write(local_jsonl_csv_or_feature_gated_structured)", "python_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_jsonl_csv_structured_runtime"),
                    ("python_ctx_range", "Python ctx.range(...).write(local_jsonl_csv_or_feature_gated_structured)", "python_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_range_jsonl_csv_structured_runtime"),
                    ("python_ctx_sequence", "Python ctx.sequence(...).write(local_jsonl_csv_or_feature_gated_structured)", "python_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_sequence_jsonl_csv_structured_runtime"),
                    ("python_ctx_literal_table", "Python ctx.literal_table([...]).write(local_jsonl_csv_or_feature_gated_structured)", "python_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_literal_table_jsonl_csv_structured_runtime"),
                    ("python_ctx_calendar", "Python ctx.calendar(start,end).write(local_jsonl_csv_or_feature_gated_structured)", "python_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_calendar_jsonl_csv_structured_runtime"),
                    ("python_generated_source_write", "Python GeneratedRowsSource/GeneratedRangeSource/GeneratedSqlSource.write(local_jsonl_csv_or_feature_gated_structured)", "python_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_supported_generated_source_write_runtime"),
                    ("local_output_only_generated_source_posture", "Generated-source local-output-only posture", "output_boundary", "report-only", "false", "false", "false", "false", "not_applicable_no_source_dataset", "local_output_certificate_required", "not_emitted_report_only", "not_claim_grade", "gar-compat-1b.non_local_generated_output_blocked"),
                    ("sql_literal_select", "SQL SELECT literal expressions", "sql_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_sql_literal_select_jsonl_csv_structured_runtime"),
                    ("sql_values", "SQL VALUES (...)", "sql_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_sql_values_jsonl_csv_structured_runtime"),
                    ("sql_source_free_projection", "SQL source-free range projection", "sql_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_sql_range_projection_jsonl_csv_structured_runtime"),
                    ("sql_generate_series_range", "SQL generate_series/range", "sql_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_sql_generate_series_range_jsonl_csv_structured_runtime"),
                    ("dataframe_source_free_projection", "DataFrame source-free projection", "dataframe_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_dataframe_literal_projection_jsonl_csv_structured_runtime"),
                    ("dataframe_generated_with_column", "Scoped generated DataFrame with_column", "dataframe_generated_source", "runtime-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "required_for_runtime_output", "required_for_runtime", "not_claim_grade", "none_scoped_local_generated_with_column_jsonl_csv_structured_runtime"),
                    ("object_store_local_emulator_generated_output", "Python ctx.generated_output_to_object_store(local_path, profile=local-emulator)", "platform_generated_output", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "local_emulator_object_store_write_certificate_required", "required_for_runtime", "fixture_smoke_only", "none_scoped_local_emulator_generated_output_to_object_store_smoke_only"),
                    ("object_store_live_provider_generated_output", "Generated output to live S3/GCS/ADLS object-store URI", "platform_generated_output", "blocked", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_blocked", "not_emitted_blocked", "not_claim_grade", "gar-gen-1.object_store_generated_output_live_provider_blocked"),
                    ("foundry_style_generated_output", "Python ctx.foundry_generated_output(local_dataset_path)", "platform_generated_output", "smoke-supported", "true", "true", "true", "true", "not_applicable_no_source_dataset", "local_foundry_style_dataset_output_evidence", "required_for_runtime", "fixture_smoke_only", "none_local_foundry_style_generated_output_dataset_proof_only"),
                    ("foundry_live_platform_generated_output", "Generated output to real Foundry dataset/API", "platform_generated_output", "blocked", "false", "false", "false", "false", "not_applicable_no_source_dataset", "not_emitted_blocked", "not_emitted_blocked", "not_claim_grade", "gar-gen-1.foundry_generated_output_runtime_not_implemented"),
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
                for row_id, surface, family, direction, status, runtime, smoke, report_only, credential, network, source_io, output_io, native_status, generated_status, claim_status, blocker, claim_boundary in [
                    ("vortex", "Vortex", "native_file_layout", "read_write", "runtime-supported", "true", "true", "false", "false", "false", "true", "true", "scoped_local_vortex_evidence_backed", "not_applicable", "fixture_smoke_only", "gar-compat-1a.vortex_universal_runtime_evidence_missing", "scoped local Vortex evidence only"),
                    ("object_store_s3_gcs_adls", "S3 / GCS / ADLS", "object_store", "read_write", "smoke-supported", "false", "true", "false", "false", "false", "true", "false", "public_fixture_smoke_only", "not_applicable", "public_fixture_smoke_only", "none_public_no_credential_fixture_profile_only", "public fixture object-store read smoke only"),
                    ("sql_values_literals", "SQL VALUES / literals", "sql_frontend", "api", "runtime-supported", "true", "false", "false", "false", "false", "false", "true", "local_output_certificate_required", "scoped_local_jsonl_csv_structured_runtime", "not_claim_grade", "none_scoped_local_sql_values_literals_jsonl_csv_structured_runtime", "source-free SQL VALUES/literal local JSONL/CSV and feature-gated flat scalar structured generated-output runtime"),
                    ("foundry", "Foundry", "platform_integration", "api", "report-only", "false", "false", "true", "true", "true", "false", "false", "not_emitted", "not_emitted_report_only", "not_claim_grade", "gar-compat-1a.foundry_platform_proof_missing", "future validation target only"),
                ]:
                    prefix = f"universal_compatibility_row_{row_id}"
                    fields.extend([
                        {"key": f"{prefix}_surface", "value": surface},
                        {"key": f"{prefix}_surface_family", "value": family},
                        {"key": f"{prefix}_direction", "value": direction},
                        {"key": f"{prefix}_support_status", "value": status},
                        {"key": f"{prefix}_runtime_supported", "value": runtime},
                        {"key": f"{prefix}_smoke_supported", "value": smoke},
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
        self.assertEqual(scoreboard.runtime_supported_count, 2)
        self.assertEqual(scoreboard.blocked_count, 0)
        self.assertEqual(scoreboard.row("object-store-s3-gcs-adls").support_status, "smoke-supported")
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
        self.assertTrue(generated.object_store_local_emulator_runtime_supported)
        self.assertFalse(generated.foundry_runtime_supported)
        self.assertTrue(generated.foundry_style_runtime_supported)
        self.assertFalse(generated.live_platform_api_supported)
        self.assertFalse(generated.broad_sql_dataframe_claim_allowed)
        self.assertTrue(generated.all_no_fallback_no_external_engine)
        self.assertTrue(generated.row("python-ctx-from-rows").runtime_supported)
        self.assertTrue(generated.row("python_ctx_from_rows").generated_source_created)
        self.assertTrue(generated.row("python_ctx_sequence").runtime_execution)
        self.assertTrue(generated.row("python_ctx_literal_table").runtime_supported)
        self.assertTrue(generated.row("python_ctx_calendar").runtime_execution)
        self.assertTrue(generated.row("sql_values").runtime_supported)
        self.assertTrue(generated.row("sql_values").runtime_execution)
        self.assertTrue(generated.row("sql_source_free_projection").runtime_supported)
        self.assertTrue(generated.row("sql_source_free_projection").runtime_execution)
        self.assertTrue(generated.row("sql_generate_series_range").runtime_supported)
        self.assertTrue(generated.row("sql_generate_series_range").runtime_execution)
        self.assertEqual(
            generated.platform_row_order,
            (
                "object_store_local_emulator_generated_output",
                "object_store_live_provider_generated_output",
                "foundry_style_generated_output",
                "foundry_live_platform_generated_output",
            ),
        )
        self.assertTrue(
            generated.row("object_store_local_emulator_generated_output").fixture_smoke_supported
        )
        self.assertTrue(
            generated.row("object_store_local_emulator_generated_output").runtime_execution
        )
        self.assertEqual(
            generated.row("object_store_live_provider_generated_output").support_status,
            "blocked",
        )
        self.assertTrue(generated.row("foundry_style_generated_output").fixture_smoke_supported)
        self.assertFalse(
            generated.row("foundry_live_platform_generated_output").runtime_execution
        )
        self.assertTrue(generated.row("dataframe_generated_with_column").runtime_supported)
        self.assertTrue(generated.row("dataframe_generated_with_column").runtime_execution)
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
        self.assertTrue(object_store.runtime_supported)
        self.assertTrue(object_store.public_no_credential_read_supported)
        self.assertFalse(object_store.all_rows_no_effects)
        self.assertTrue(object_store.all_live_provider_effects_disabled)
        self.assertTrue(object_store.all_no_fallback_no_external_engine)
        self.assertTrue(object_store.row("object-store-uri-parse").no_effects_no_fallback)
        self.assertEqual(object_store.row("public_no_credential_read").support_status, "smoke-supported")
        self.assertTrue(object_store.row("public_no_credential_read").byte_range_read_allowed)
        self.assertTrue(object_store.row("public_no_credential_read").full_file_read_allowed)
        self.assertTrue(object_store.row("public_no_credential_read").object_store_io)
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
                        {"key": "execution_mode_vocabulary", "value": "compatibility_import_certified,prepared_vortex,native_vortex"},
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
                elif args == [
                    "live-fixture-run", "group-count", "metric",
                    "--memory-bytes", "4294967296", "--max-parallelism", "2",
                    "--memory-origin", "context", "--parallelism-origin", "context",
                    "--format", "json",
                ]:
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
                elif args == [
                    "hybrid-overlay-run", "group-count", "metric",
                    "--memory-bytes", "4294967296", "--max-parallelism", "2",
                    "--memory-origin", "context", "--parallelism-origin", "context",
                    "--format", "json",
                ]:
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
        ctx = ShardLoomContext(
            ShardLoomClient(binary=binary), engine="live", memory_gb=4, max_parallelism=2
        )

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


    def test_ordered_grouped_aggregate_forwards_resources_through_dataframe_facades(self) -> None:
        expected_sql_by_uri = {
            "fact.vortex": (
                "SELECT category,sum(sample) AS total,count(*) AS entries "
                "FROM 'fact.vortex' GROUP BY category "
                "ORDER BY total ASC NULLS LAST,category ASC NULLS LAST LIMIT 7"
            ),
            "fact.csv": (
                "SELECT category,sum(sample) AS total,count(*) AS entries "
                "FROM 'fact.csv' GROUP BY category "
                "ORDER BY total ASC NULLS LAST,category ASC NULLS LAST LIMIT 7"
            ),
        }
        sources = (
            ("fact.vortex", "vortex", None),
            ("fact.csv", "csv", {"category": "utf8", "sample": "float64"}),
        )
        writers = (
            ("write_jsonl", "jsonl"),
            ("write_json", "json"),
            ("write_csv", "csv"),
            ("write_parquet", "parquet"),
            ("write_arrow_ipc", "arrow-ipc"),
            ("write_avro", "avro"),
            ("write_orc", "orc"),
            ("write_vortex", "vortex"),
        )
        spill = {"workspace": "/tmp/shardloom-spill", "quota_bytes": 8192, "buffer_bytes": 4096}

        for uri, source_format, schema in sources:
            expected_sql = expected_sql_by_uri[uri]
            binding = {"input_format": source_format}
            if schema is not None:
                binding["source_schema"] = "category:utf8,sample:float64"
            expected_bindings = {uri: binding}
            actions = [
                ("collect", "run", "collect", None),
                ("route", "route", "collect", None),
                ("run", "run", "collect", None),
                *((alias, "run", alias, output_format) for alias, output_format in writers),
            ]
            for action_name, command, request, output_format in actions:
                with self.subTest(source=uri, action=action_name):
                    output_path = (
                        None if output_format is None else f"target/out.{output_format}"
                    )
                    binary = self.fake_cli(
                        textwrap.dedent(
                            f"""
                            import json, sys

                            args = sys.argv[1:]
                            assert args[:2] == [{command!r}, "dataframe"], args
                            def option(name):
                                assert name in args, (name, args)
                                return args[args.index(name) + 1]
                            assert option("--input") == {uri!r}, args
                            assert option("--input-format") == {source_format!r}, args
                            assert option("--sql") == {expected_sql!r}, args
                            assert option("--request") == {request!r}, args
                            assert option("--memory-bytes") == "3221225472", args
                            assert option("--max-parallelism") == "2", args
                            assert option("--memory-origin") == "execution_call", args
                            assert option("--parallelism-origin") == "execution_call", args
                            assert json.loads(option("--spill")) == {spill!r}, args
                            if {output_path!r} is None:
                                assert "--output" not in args, args
                                assert json.loads(option("--source-bindings")) == {expected_bindings!r}, args
                            else:
                                assert option("--output") == {output_path!r}, args
                                assert json.loads(option("--source-bindings")) == {expected_bindings!r}, args
                                if {source_format!r} == "csv":
                                    assert option("--source-schema") == "category:utf8,sample:float64", args
                                else:
                                    assert "--source-schema" not in args, args
                            assert "--native-vortex-provider-scenario" not in args, args
                            assert args[-2:] == ["--format", "json"], args
                            result_jsonl = json.dumps(
                                {{"category": "a", "total": 2.5, "entries": 1}},
                                separators=(",", ":"),
                            ) + chr(10)
                            fields = [
                                {{"key": "public_workflow_requested_output", "value": {request!r}}},
                                {{"key": "result_jsonl", "value": result_jsonl}},
                                {{"key": "result_payload_complete", "value": "true"}},
                                {{"key": "output_row_count", "value": "1"}},
                                {{"key": "output_path", "value": {output_path!r} or "none"}},
                                {{"key": "output_io_performed", "value": "true" if {output_path!r} else "false"}},
                                {{"key": "fallback_attempted", "value": "false"}},
                                {{"key": "external_engine_invoked", "value": "false"}},
                            ]
                            print(json.dumps({{
                                "schema_version": "shardloom.output.v2",
                                "command": {command!r},
                                "status": "success",
                                "summary": "ordered grouped aggregate",
                                "human_text": "ordered grouped aggregate",
                                "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                                "diagnostics": [],
                                "fields": fields,
                            }}))
                            """
                        ),
                    )
                    client = ShardLoomClient(binary=binary)
                    if source_format == "vortex":
                        frame = sl.read_vortex(uri, client=client)
                    else:
                        frame = sl.read_csv(uri, schema=schema, client=client)
                    workflow = (
                        frame.group_by("category")
                        .agg(total="sum(sample)", entries="count(*)")
                        .sort("total", "category", nulls="last")
                        .limit(7)
                    )
                    if action_name == "collect":
                        report = workflow.collect(
                            memory_gb=3, max_parallelism=2, spill=spill, check=True
                        )
                    elif action_name == "route":
                        report = workflow.route(
                            memory_gb=3, max_parallelism=2, spill=spill, check=True
                        )
                    elif action_name == "run":
                        report = workflow.run(
                            memory_gb=3, max_parallelism=2, spill=spill, check=True
                        )
                    else:
                        report = getattr(workflow, action_name)(
                            output_path, memory_gb=3, max_parallelism=2, spill=spill,
                            check=True,
                        )
                    self.assertEqual(report.envelope.status, "success")

    def test_ordered_grouped_aggregate_forwards_resources_through_sql_facades(self) -> None:
        writers = (
            ("write_jsonl", "jsonl"),
            ("write_json", "json"),
            ("write_csv", "csv"),
            ("write_parquet", "parquet"),
            ("write_arrow_ipc", "arrow-ipc"),
            ("write_avro", "avro"),
            ("write_orc", "orc"),
            ("write_vortex", "vortex"),
        )
        spill = {"workspace": "/tmp/shardloom-spill", "quota_bytes": 8192, "buffer_bytes": 4096}
        for uri, source_format, schema in (
            ("fact.vortex", "vortex", None),
            ("fact.csv", "csv", {"category": "utf8", "sample": "float64"}),
        ):
            expected_sql = (
                "SELECT category,sum(sample) AS total,count(*) AS entries "
                f"FROM '{uri}' GROUP BY category "
                "ORDER BY total ASC NULLS LAST,category ASC NULLS LAST LIMIT 7"
            )
            binding = {"input_format": source_format}
            if schema is not None:
                binding["source_schema"] = "category:utf8,sample:float64"
            expected_bindings = {uri: binding}
            for action_name, command, request, output_format in (
                ("collect", "run", "collect", None),
                ("route", "route", "collect", None),
                ("run", "run", "collect", None),
                *((alias, "run", alias, fmt) for alias, fmt in writers),
            ):
                with self.subTest(source=uri, action=action_name):
                    output_path = None if output_format is None else f"target/sql-out.{output_format}"
                    binary = self.fake_cli(
                        textwrap.dedent(
                            f"""
                            import json, sys

                            args = sys.argv[1:]
                            assert args[:2] == [{command!r}, "sql"], args
                            def option(name):
                                assert name in args, (name, args)
                                return args[args.index(name) + 1]
                            assert option("--sql") == {expected_sql!r}, args
                            assert option("--request") == {request!r}, args
                            assert option("--memory-bytes") == "3221225472", args
                            assert option("--max-parallelism") == "2", args
                            assert option("--memory-origin") == "execution_call", args
                            assert option("--parallelism-origin") == "execution_call", args
                            assert json.loads(option("--spill")) == {spill!r}, args
                            assert json.loads(option("--source-bindings")) == {expected_bindings!r}, args
                            assert "--input" not in args and "--input-format" not in args, args
                            if {output_path!r} is None:
                                assert "--output" not in args, args
                            else:
                                assert option("--output") == {output_path!r}, args
                            assert "--source-schema" not in args, args
                            assert "--native-vortex-provider-scenario" not in args, args
                            assert args[-2:] == ["--format", "json"], args
                            result_jsonl = json.dumps(
                                {{"category": "a", "total": 2.5, "entries": 1}},
                                separators=(",", ":"),
                            ) + chr(10)
                            fields = [
                                {{"key": "public_workflow_requested_output", "value": {request!r}}},
                                {{"key": "result_jsonl", "value": result_jsonl}},
                                {{"key": "result_payload_complete", "value": "true"}},
                                {{"key": "output_row_count", "value": "1"}},
                                {{"key": "output_path", "value": {output_path!r} or "none"}},
                                {{"key": "output_io_performed", "value": "true" if {output_path!r} else "false"}},
                                {{"key": "fallback_attempted", "value": "false"}},
                                {{"key": "external_engine_invoked", "value": "false"}},
                            ]
                            print(json.dumps({{
                                "schema_version": "shardloom.output.v2",
                                "command": {command!r},
                                "status": "success",
                                "summary": "SQL ordered grouped aggregate",
                                "human_text": "SQL ordered grouped aggregate",
                                "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                                "diagnostics": [],
                                "fields": fields,
                            }}))
                            """
                        ),
                    )
                    client = ShardLoomClient(binary=binary)
                    if source_format == "vortex":
                        frame = sl.read_vortex(uri, client=client)
                    else:
                        frame = sl.read_csv(uri, schema=schema, client=client)
                    workflow = sl.SqlWorkflow(
                        expected_sql,
                        client,
                        source_bindings=frame._declared_sources(),
                    )
                    if action_name == "collect":
                        report = workflow.collect(
                            memory_gb=3, max_parallelism=2, spill=spill, check=True
                        )
                    elif action_name == "route":
                        report = workflow.route(
                            memory_gb=3, max_parallelism=2, spill=spill, check=True
                        )
                    elif action_name == "run":
                        report = workflow.run(
                            memory_gb=3, max_parallelism=2, spill=spill, check=True
                        )
                    else:
                        report = getattr(workflow, action_name)(
                            output_path, memory_gb=3, max_parallelism=2, spill=spill,
                            check=True,
                        )
                    self.assertEqual(report.envelope.status, "success")


    def test_native_relational_renderer_keeps_input_limit_and_duplicate_aggregate_stages_nested(self) -> None:
        client = ShardLoomClient(binary=self.fake_cli(""))
        source = sl.read_vortex("fact.vortex", client=client)
        input_limited = source.limit(2).group_by("category").agg(total="sum(sample)")
        statement = input_limited._native_relational_statement()
        self.assertIsNotNone(statement)
        assert statement is not None
        self.assertIn("FROM (SELECT", statement)
        self.assertLess(statement.index("LIMIT 2"), statement.index("GROUP BY category"))
        self.assertNotEqual(
            statement,
            "SELECT category,sum(sample) AS total FROM 'fact.vortex' LIMIT 2 GROUP BY category",
        )

        duplicate_aggregate = source.group_by("category").agg(total="sum(sample)")._append(
            WorkflowOperation("aggregate", ("sum(total) AS grand_total",))
        )
        duplicate_statement = duplicate_aggregate._native_relational_statement()
        self.assertIsNotNone(duplicate_statement)
        assert duplicate_statement is not None
        self.assertIn("FROM (SELECT category,sum(sample) AS total", duplicate_statement)
        self.assertEqual(duplicate_statement.count(" GROUP BY category"), 1)
        self.assertIn("SELECT sum(total) AS grand_total FROM (", duplicate_statement)

        invalid_stage_order = source.group_by("category").agg(total="sum(sample)")._append(
            WorkflowOperation("group_by", ("category",))
        )
        self.assertIsNone(invalid_stage_order._native_relational_statement())


if __name__ == "__main__":
    unittest.main()
