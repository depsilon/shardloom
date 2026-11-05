"""Resource, source ownership and adapter controls for native rich columns."""

from __future__ import annotations

import gc
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_native_binding import _load_native
from test_native_rich_binding import IDENTITY_BYTES, _field, _fixture, _rows


class NativeRichControlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def client(self, policy="extended"):
        import shardloom as sl
        client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20,
                                   max_parallelism=1, float_policy=policy)
        self.addCleanup(client.close)
        return client

    def assert_table(self, actual, expected):
        self.assertTrue(actual.schema.equals(expected.schema, check_metadata=True),
                        (actual.schema, expected.schema))
        received, wanted = _rows(actual), _rows(expected)
        self.assertEqual(len(received), len(wanted))
        for index, pair in enumerate(zip(received, wanted)):
            self.assertEqual(*pair, f"row {index}")

    def test_dense_embedding_streamed_input_capable_sinks_and_failed_publication(self):
        import shardloom as sl
        import pyarrow as pa
        import pyarrow.parquet as parquet
        from shardloom.errors import ShardLoomCommandError

        table = _fixture(39).slice(1, 33)
        client = self.client()
        schema = dict(sl.from_arrow_table(table, client=client).collect(check=True).result_schema)
        events = []

        def producer():
            events.append("opened")
            try:
                yield table.slice(0, 0)
                yield from table.to_batches(max_chunksize=7)
                events.append("ended")
            finally:
                events.append("closed")

        frame = sl.from_batches(producer, schema=schema, streaming=True, client=client)
        with tempfile.TemporaryDirectory(prefix="shardloom-embedding-stream-") as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ShardLoomCommandError, "embedding identity"):
                frame.fanout({"vortex": root / "denied.vortex", "orc": root / "denied.orc"})
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [])
            targets = {name: root / ("rich." + name) for name in ("vortex", "arrow_ipc", "parquet")}
            with patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON cells")):
                frame.fanout(targets)
            self.assertEqual(events, ["opened", "ended", "closed"])
            for name, path in targets.items():
                with self.subTest(format=name):
                    self.assert_table(getattr(sl, "read_" + name)(path, client=client)
                                      .collect(check=True).to_arrow(), table)
                    if name == "parquet":
                        self.assertTrue(parquet.read_schema(path).equals(table.schema, check_metadata=True))
                    elif name == "arrow_ipc":
                        with pa.ipc.open_file(path) as reader:
                            self.assert_table(reader.read_all(), table)

            # PyArrow 25.0.1 cannot read null fixed-size-list parents, including
            # files it wrote itself. Keep the complete native roundtrip above;
            # check independent production of all rows and independent readback
            # of a separate control without null vector parents. Arrow IPC above
            # independently checks every row, including the null parents.
            independent_input = root / "independent.parquet"
            parquet.write_table(table, independent_input, use_dictionary=False)
            self.assert_table(sl.read_parquet(independent_input, client=client)
                              .collect(check=True).to_arrow(), table)
            independent_output = root / "present-vectors.parquet"
            frame.where("id % 17 <> 0").write_parquet(independent_output, check=True)
            expected_present = table.filter(pa.array([row["id"].as_py() % 17 != 0
                for row in table.to_struct_array()]))
            self.assert_table(parquet.read_table(independent_output), expected_present)

            failure = LookupError("rich producer failure after values")

            def broken():
                try:
                    yield table
                    raise failure
                finally:
                    events.append("failure closed")

            failed = sl.from_batches(broken, schema=schema, streaming=True, client=client)
            with self.assertRaises(LookupError) as raised:
                failed.write_vortex(root / "failed.vortex", check=True)
            self.assertIs(raised.exception, failure)
            self.assertEqual(events[-1], "failure closed")
            self.assertEqual(set(root.iterdir()), set(targets.values()) | {independent_input, independent_output})

    def test_dense_embedding_large_dimension_component_nulls_and_unknown_identity(self):
        import shardloom as sl
        import pyarrow as pa
        dimension = 65537
        values = [1.5] * dimension
        values[1], values[-1] = None, -0.0
        vectors = pa.FixedSizeListArray.from_arrays(pa.array(values, pa.float32()),
            type=pa.list_(pa.field("item", pa.float32()), dimension))
        field = _field(pa, "embedding", vectors.type, nullable=False).with_metadata({
            b"ARROW:extension:name": b"shardloom.embedding.v1",
            b"ARROW:extension:metadata": b'{"version":1}',
        })
        table = pa.Table.from_arrays([vectors], schema=pa.schema([field]))
        client = self.client()
        source = sl.from_arrow_table(table, client=client)
        with tempfile.TemporaryDirectory(prefix="shardloom-embedding-dimension-") as directory:
            target = Path(directory) / "wide.vortex"
            source.write_vortex(target, check=True)
            self.assert_table(sl.read_vortex(target, client=client).collect(check=True).to_arrow(), table)

    def test_dense_embedding_strict_policy_and_malformed_metadata_release_admission(self):
        import pyarrow as pa
        strict = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(strict.close)
        table = _fixture(3).slice(1, 2)
        with self.assertRaisesRegex(Exception, "finite|float_policy"):
            strict.from_arrow(table)
        self.assertEqual(strict.snapshot()["reserved_bytes"], 0)
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        for metadata in (b'{"version":2}', b'{"version":1,"version":1}',
                         b'{"version":1,"model_id":[]}', b'{"version":1,"unexpected":null}'):
            field = table.schema.field("embedding").with_metadata({
                b"ARROW:extension:name": b"shardloom.embedding.v1",
                b"ARROW:extension:metadata": metadata,
            })
            invalid = pa.Table.from_arrays([table.column("embedding")], schema=pa.schema([field]))
            with self.subTest(metadata=metadata), self.assertRaises(Exception):
                session.from_arrow(invalid)
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        with self.assertRaisesRegex(Exception, "memory"):
            session.from_arrow(table, resources=self.native.ExecutionResources(4096, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_dense_embedding_cli_dispatch_preserves_typed_finite_result_schema(self):
        import shardloom as sl
        from shardloom import types as t
        from shardloom._input_schema import normalize_schema
        from shardloom._result_schema import schema_fields, python_rows
        schema = {"id": t.int64(), "content": t.binary(),
                  "embedding": t.embedding(t.float32(), 2, metadata={"space_id": "cli/declared"})}
        expected = [{"id": 1, "content": b"\x00\xff", "embedding": [-0.0, 1.5]},
                    {"id": 2, "content": b"", "embedding": [None, 2.0]},
                    {"id": 3, "content": None, "embedding": None}]
        client = self.client("strict")
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1))
        self.addCleanup(session.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-embedding-command-") as directory:
            path = Path(directory) / "values.vortex"
            sl.from_rows(expected, schema=schema, client=client).write_vortex(path, check=True)
            envelope = json.loads(session.command(["run", "cli", "--input", str(path),
                "--input-format", "vortex", "--request", "collect", "--bounded", "true",
                "--execution-policy", "native_vortex", "--native-vortex-operation-family",
                "filter_project_limit", "--vortex-primitive", "project", "--vortex-columns",
                "id,content,embedding"]))
            self.assertEqual(envelope["status"], "success", envelope)
            self.assertFalse(envelope["fallback"]["attempted"])
            self.assertFalse(envelope["fallback"]["allowed"])
            fields = {field["key"]: field["value"] for field in
                      [*envelope.get("fields", []), *envelope.get("result", {}).get("fields", [])]}
            declared = schema_fields(fields["result_schema_json"], fields["result_schema_format"])
            self.assertEqual(declared, normalize_schema(schema))
            self.assertEqual(python_rows(json.loads(fields["result_values_json"]), declared), expected)

    def test_dense_embedding_comparison_keys_fail_before_execution(self):
        import pyarrow as pa
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        table = _fixture(3)
        nested = pa.StructArray.from_arrays([table.column("embedding").combine_chunks()],
                                            fields=[table.schema.field("embedding")])
        source = session.from_arrow(table.append_column("nested", nested))
        for query in (
            "SELECT embedding FROM 'memory://rich' ORDER BY embedding",
            "SELECT embedding, COUNT(*) AS n FROM 'memory://rich' GROUP BY embedding",
            "SELECT DISTINCT embedding FROM 'memory://rich'",
            "SELECT id FROM 'memory://rich' WHERE embedding = embedding",
            "SELECT l.id AS id FROM 'memory://rich' AS l JOIN 'memory://rich' AS r ON l.embedding = r.embedding",
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY embedding ORDER BY id) AS n FROM 'memory://rich'",
            "SELECT nested FROM 'memory://rich' ORDER BY nested",
        ):
            with self.subTest(query=query), self.assertRaisesRegex(Exception, "embedding values are payload-only"):
                plan = session.sql(query, sources=[("memory://rich", source)])
                plan.collect()

    def test_dense_embedding_reader_owners_cancel_and_changed_file(self):
        import pyarrow as pa
        table = _fixture(13).slice(1, 11)
        session = self.native.Session(self.native.ExecutionResources(128 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        source = session.from_arrow(pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=3)))
        plan = session.sql("SELECT * FROM 'memory://rich'", sources=[("memory://rich", source)])
        result = plan.collect()
        exported = pa.RecordBatchReader.from_stream(result).read_all()
        self.assert_table(exported, table)
        plan.release_prepared()
        reserved = session.snapshot()["reserved_bytes"]
        self.assertGreater(reserved, 0)
        with self.assertRaisesRegex(Exception, "memory"):
            plan.collect(resources=self.native.ExecutionResources(reserved - 1, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], reserved)

        with tempfile.TemporaryDirectory(prefix="shardloom-embedding-generation-") as directory:
            path = Path(directory) / "source.vortex"
            plan.write(path, "write_vortex")
            file_plan = session.sql(f"SELECT * FROM '{path}'", files=[(str(path), "vortex", None)])
            retained_file = file_plan.collect()
            retained_table = pa.RecordBatchReader.from_stream(retained_file).read_all()
            path.write_bytes(b"changed source generation")
            with self.assertRaisesRegex(Exception, "prepared source changed"):
                file_plan.collect()
            self.assert_table(retained_table, table)

        stream = plan.iter_batches()
        first = next(stream)
        stream.close()
        self.assertIsNone(stream.report)
        cancelled = plan.iter_batches()
        kept = next(cancelled)
        session.cancel()
        with self.assertRaisesRegex(Exception, "cancelled"):
            next(cancelled)
        cancelled.close()
        self.assertIsNone(cancelled.report)
        del source, plan, result, file_plan, retained_file, retained_table, exported, stream, cancelled
        session.close()
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        for batch in (first, kept):
            retained = pa.RecordBatchReader.from_stream(batch).read_all()
            self.assert_table(retained, table.slice(0, retained.num_rows))
            self.assertEqual(retained.schema.field("embedding").metadata[b"ARROW:extension:metadata"], IDENTITY_BYTES)
        del batch, retained, first, kept
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
