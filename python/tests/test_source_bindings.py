from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom.client import ShardLoomClient
from shardloom.models import OutputEnvelope
from shardloom import read_tsv as package_read_tsv
from shardloom.query import (
    _source_format_for_local_source_ref,
    _source_format_for_sql_input_ref,
    _source_kind_from_path,
    _workflow_source_bindings,
    read,
    read_tsv,
    sql,
)
from shardloom.session import ShardLoomSession
from shardloom.context import ShardLoomContext


class SourceBindingsTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        self.commands: list[list[str]] = []

        def capture_run(args: list[str], *, check: bool = True) -> OutputEnvelope:
            self.commands.append(args)
            return OutputEnvelope.from_field_mapping({}, command=args[0])

        self.client.run = capture_run  # type: ignore[method-assign]

    def _bindings_arg(self, args: list[str]) -> str:
        self.assertIn("--source-bindings", args)
        return args[args.index("--source-bindings") + 1]

    def test_explicit_collection_format_preserves_json_lines_and_schema_without_io(self) -> None:
        schema = {"id": "int64", "label": "utf8"}
        context = ShardLoomContext(self.client)
        session = ShardLoomSession(self.client, memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(session.close)
        frames = [
            read("missing/records.manifest", input_format="jsonl", schema=schema, client=self.client),
            context.read("missing/records", input_format="ndjson", schema=schema),
            session.read("missing/parts/**", input_format="jsonl", schema=schema),
        ]
        for frame in frames:
            self.assertEqual(frame.source.source_format, "jsonl")
            self.assertEqual(frame.source.schema_map, schema)
            self.assertEqual(_workflow_source_bindings((frame.source,)), {
                frame.source.uri: {"input_format": "jsonl", "source_schema": tuple(schema.items())},
            })
            self.assertIs(frame.client, self.client)
        self.assertEqual(frames[-1].resources, session.resources)
        self.assertEqual(frames[-1].resource_limits, session.resource_limits)
        self.assertEqual(self.commands, [])
        with self.assertRaisesRegex(ValueError, "source_format must be"):
            read("missing/records", input_format="unknown", client=self.client)

    def test_route_and_run_serialize_bindings_compactly(self) -> None:
        bindings = {
            "s3://bucket/a.csv?version=7": {
                "input_format": "csv",
                "source_schema": {"label": "utf8", "count": "int64"},
            },
            "local/path/data.jsonl": {
                "input_format": "jsonl",
                "source_schema": [("enabled", "bool"), ("value", "float64")],
            },
            "local/path/table.tsv": {
                "input_format": "tsv",
                "source_schema": {"id": "int64", "label": "utf8"},
            },
        }

        self.client.public_workflow_route("sql", source_bindings=bindings)
        self.client.public_workflow_run("sql", source_bindings=bindings)

        self.assertEqual(len(self.commands), 2)
        for args in self.commands:
            raw = self._bindings_arg(args)
            self.assertNotIn(" ", raw)
            self.assertEqual(
                json.loads(raw),
                {
                    "s3://bucket/a.csv?version=7": {
                        "input_format": "csv",
                        "source_schema": "label:utf8,count:int64",
                    },
                    "local/path/data.jsonl": {
                        "input_format": "jsonl",
                        "source_schema": "enabled:bool,value:float64",
                    },
                    "local/path/table.tsv": {
                        "input_format": "tsv",
                        "source_schema": "id:int64,label:utf8",
                    },
                },
            )

    def test_tsv_factory_session_and_sql_bindings_are_declarations(self) -> None:
        frame = read_tsv(
            "local/path/table.tsv",
            schema={"id": "int64", "label": "utf8"},
            client=self.client,
        )
        self.assertEqual(frame.source.source_format, "tsv")
        self.assertEqual(frame.source.schema_map, {"id": "int64", "label": "utf8"})
        self.assertIs(frame.client, self.client)
        self.assertIs(package_read_tsv, read_tsv)

        context = ShardLoomContext(self.client)
        context_frame = context.read_tsv("local/path/context.tsv", schema={"label": "utf8"})
        self.assertEqual(context_frame.source.source_format, "tsv")
        self.assertEqual(context_frame.source.schema_map, {"label": "utf8"})
        self.assertIs(context_frame.client, self.client)

        session = ShardLoomSession(self.client, memory_bytes=32 * 1024 * 1024, max_parallelism=1)
        session_frame = session.read_tsv("local/path/session.tsv", schema={"id": "int64"})
        self.assertEqual(session_frame.source.source_format, "tsv")
        self.assertEqual(session_frame.source.schema_map, {"id": "int64"})
        self.assertEqual(session_frame.resources, session.resources)
        self.assertEqual(session_frame.resource_limits, session.resource_limits)

        self.assertEqual(_source_format_for_sql_input_ref("records.tsv"), "tsv")
        inferred = sql("SELECT id FROM source", input="records.tsv", client=self.client)
        self.assertEqual(inferred.source.relation.input_format, "tsv")
        explicit = sql("SELECT id FROM source", input="records.tsv", input_format="tsv", client=self.client)
        self.assertEqual(explicit.source.relation.input_format, "tsv")

    def test_compressed_text_source_inference_is_local_suffix_aware_only(self) -> None:
        cases = (
            ("events.csv.gz", "csv"),
            ("events.CSV.ZSTD", "csv"),
            ("events.TSV.ZST", "tsv"),
            ("events.tsv.GZ", "tsv"),
            ("events.json.zstd", "json"),
            ("events.JSON.GZ", "json"),
            ("events.jsonl.GZ", "json"),
            ("events.JSONL.ZST", "json"),
            ("events.ndjson.zSt", "json"),
            ("events.NDJSON.ZSTD", "json"),
        )
        for uri, expected in cases:
            with self.subTest(uri=uri):
                self.assertEqual(_source_kind_from_path(uri), expected)
                self.assertEqual(_source_format_for_local_source_ref(uri), expected)
                sql_format = ("ndjson" if ".ndjson." in uri.lower() else
                              "jsonl" if ".jsonl." in uri.lower() else expected)
                self.assertEqual(_source_format_for_sql_input_ref(uri), sql_format)
                frame = sql("SELECT * FROM source", input=uri, client=self.client)
                self.assertEqual(frame.source.relation.input_format, sql_format)

        # A question mark in a local glob remains a glob wildcard, not a URL query delimiter.
        glob = "/tmp/events/part-?.csv.gz"
        self.assertEqual(_source_format_for_local_source_ref(glob), "csv")
        self.assertEqual(_source_format_for_sql_input_ref(glob), "csv")
        self.assertEqual(
            sql("SELECT * FROM source", input=glob, client=self.client)
            .source.relation.input_format,
            "csv",
        )

        # Remote query/fragment syntax keeps its existing nonlocal inference behavior.
        for uri in (
            "s3://bucket/events.csv.gz?version=7",
            "s3://bucket/events.csv.gz",
            "https://example.test/events.jsonl.zst#part",
        ):
            with self.subTest(uri=uri):
                self.assertIsNone(_source_format_for_local_source_ref(uri))
                self.assertIsNone(_source_format_for_sql_input_ref(uri))

        for uri in ("events.parquet.gz", "events.avro.zst", "events.arrow.zstd"):
            with self.subTest(uri=uri):
                self.assertIsNone(_source_format_for_local_source_ref(uri))
                with self.assertRaises(ValueError):
                    _source_kind_from_path(uri)

        self.assertIsNone(_source_format_for_local_source_ref("/tmp/events.csv?query"))
        self.assertEqual(
            sql("SELECT * FROM source", input="events.csv.gz", input_format="tsv", client=self.client)
            .source.relation.input_format,
            "tsv",
        )

    def test_route_and_run_omit_none_or_empty_bindings(self) -> None:
        for method in (self.client.public_workflow_route, self.client.public_workflow_run):
            with self.subTest(method=method.__name__, bindings=None):
                method("sql", source_bindings=None)
                self.assertNotIn("--source-bindings", self.commands[-1])
            with self.subTest(method=method.__name__, bindings={}):
                method("sql", source_bindings={})
                self.assertNotIn("--source-bindings", self.commands[-1])

    def test_invalid_binding_shapes_and_limits_are_rejected(self) -> None:
        invalid: tuple[tuple[str, Any, type[Exception]], ...] = (
            ("nonmapping", ["csv"], TypeError),
            ("nonstring URI", {1: {"input_format": "csv"}}, TypeError),
            ("missing format", {"path": {"source_schema": "id:int64"}}, ValueError),
            (
                "unknown field",
                {"path": {"input_format": "csv", "extra": True}},
                ValueError,
            ),
            ("empty format", {"path": {"input_format": "  "}}, ValueError),
            (
                "too many entries",
                {f"path-{index}": {"input_format": "csv"} for index in range(129)},
                ValueError,
            ),
            (
                "oversized serialized JSON",
                {"path": {"input_format": "x" * (256 * 1024)}},
                ValueError,
            ),
        )
        for label, bindings, error_type in invalid:
            with self.subTest(case=label), self.assertRaises(error_type):
                self.client.public_workflow_route("sql", source_bindings=bindings)


if __name__ == "__main__":
    unittest.main()
