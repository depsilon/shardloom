"""Vector failure, visibility, producer, policy and retained-owner controls."""

from __future__ import annotations

import gc
import json
from pathlib import Path
import tempfile
import unittest

if __package__:
    from .test_native_binding import _load_native
    from .test_native_vector_representations_binding import _rows, _digest, _schema_shape
    from .vector_fixture import fixture, tagged
else:
    from test_native_binding import _load_native
    from test_native_vector_representations_binding import _rows, _digest, _schema_shape
    from vector_fixture import fixture, tagged


class NativeVectorControlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, policy="extended"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def assert_table(self, actual, expected):
        self.assertEqual(_schema_shape(actual.schema), _schema_shape(expected.schema))
        self.assertEqual(_digest(_rows(actual)), _digest(_rows(expected)))

    def test_vector_bad_relations_metadata_and_required_parameters_release(self):
        import pyarrow as pa
        session = self.native.Session(self.native.ExecutionResources(32 << 20, 1), float_policy="extended")
        self.addCleanup(session.close)
        sparse = pa.struct([pa.field("size", pa.uint64()), pa.field("indices", pa.list_(pa.int64())),
                            pa.field("values", pa.list_(pa.float32()))])
        per_vector = pa.struct([pa.field("codes", pa.list_(pa.int64())), pa.field("scale", pa.float32()),
                                pa.field("zero_point", pa.int64())])
        blocked = pa.struct([pa.field("codes", pa.list_(pa.int64())), pa.field("scale", pa.list_(pa.float32())),
                             pa.field("zero_point", pa.list_(pa.int64()))])
        cases = [
            (sparse, "shardloom.sparse_vector.v1", {"version": 1, "index_policy": "strictly_increasing"}, [
                {"size": None, "indices": [], "values": []}, {"size": 0, "indices": None, "values": []},
                {"size": 0, "indices": [], "values": None}, {"size": 2, "indices": [None], "values": [1.0]},
                {"size": 2, "indices": [-1], "values": [1.0]}, {"size": 2, "indices": [2], "values": [1.0]},
                {"size": 2, "indices": [0], "values": []}, {"size": 2, "indices": [0, 0], "values": [1.0, 2.0]},
                {"size": 2, "indices": [1, 0], "values": [1.0, 2.0]},
            ]),
            (per_vector, "shardloom.quantized_vector.v1", {"version": 1, "parameter_mode": "per_vector"}, [
                {"codes": None, "scale": 1.0, "zero_point": 0},
                *[{"codes": [], "scale": value, "zero_point": 0}
                  for value in (None, 0.0, -0.0, -1.0, float("inf"), float("-inf"), float("nan"))],
                {"codes": [], "scale": 1.0, "zero_point": None},
            ]),
            (blocked, "shardloom.quantized_vector.v1", {"version": 1, "parameter_mode": "blocked", "block_size": 2}, [
                {"codes": [1], "scale": None, "zero_point": [0]},
                {"codes": [1], "scale": [1.0], "zero_point": None},
                {"codes": [1, 2, 3], "scale": [1.0], "zero_point": [0, 1]},
                {"codes": [], "scale": [1.0], "zero_point": [0]},
                {"codes": [1], "scale": [None], "zero_point": [0]},
                {"codes": [1], "scale": [1.0], "zero_point": [None]},
            ]),
        ]
        for storage, name, metadata, rows in cases:
            field = tagged(pa, "v", storage, name, metadata)
            for row in rows:
                invalid = pa.Table.from_arrays([pa.array([row], storage)], schema=pa.schema([field]))
                with self.subTest(row=row), self.assertRaises(Exception):
                    session.from_arrow(invalid)
                self.assertEqual(session.snapshot()["reserved_bytes"], 0)
            valid = {"size": 0, "indices": [], "values": []} if "sparse" in name else (
                {"codes": [], "scale": [], "zero_point": []} if "block_size" in metadata else
                {"codes": [], "scale": 1.0, "zero_point": 0})
            for raw in (b'{}', b'{"version":2}', b'{"version":1,"version":1}',
                        b'{"version":1,"unknown":null}', b'{"version":1.0}', b'[]', b''):
                invalid_field = field.with_metadata({b"ARROW:extension:name": name.encode(),
                    b"ARROW:extension:metadata": raw})
                for values in ([valid], []):
                    invalid = pa.Table.from_arrays([pa.array(values, storage)], schema=pa.schema([invalid_field]))
                    with self.subTest(metadata=raw, empty=not values), self.assertRaises(Exception):
                        session.from_arrow(invalid)
                    self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_vector_hidden_children_and_sparse_nonfinite_policy_remain_distinct_from_scale_rules(self):
        import pyarrow as pa
        from shardloom import types as t
        context = self.context()
        storage = pa.struct([pa.field("size", pa.uint64(), nullable=False),
            pa.field("indices", pa.list_(pa.int64()), nullable=False),
            pa.field("values", pa.list_(pa.float32()), nullable=False)])
        field = tagged(pa, "s", storage, "shardloom.sparse_vector.v1", {"version": 1, "index_policy": "preserve"})
        hidden = pa.StructArray.from_arrays([pa.array([None, 3], pa.uint64()),
            pa.array([None, [0, 1, 2]], storage.field("indices").type),
            pa.array([None, [float("inf"), float("nan"), -0.0]], storage.field("values").type)],
            fields=list(storage), mask=pa.array([True, False]))
        table = pa.Table.from_arrays([hidden], schema=pa.schema([field]))
        self.assert_table(context.from_arrow_table(table).collect(check=True).to_arrow(), table)
        strict = self.context("strict")
        with self.assertRaisesRegex(Exception, "finite|float_policy"):
            strict.from_arrow_table(table)
        # Scale validation must use the stored primitive, including conversion
        # underflow: a positive Python double cannot become a zero half scale.
        for value in (0.0, -0.0, float("nan"), float("inf"), 1e-40):
            with self.subTest(scale=value), self.assertRaises(Exception):
                context.from_rows([{"q": {"codes": [], "scale": value, "zero_point": 0}}],
                    schema={"q": t.quantized_vector(t.int8(), scale=t.float16())}).collect(check=True)

    def test_vector_rich_keys_and_unsupported_destinations_refuse_before_producer_work(self):
        import pyarrow as pa
        context = self.context()
        table, _ = fixture(pa)
        table = table.select(["id", "sparse", "quantized", "blocked", "collection"])
        frame = context.from_arrow_table(table)
        for key in ("sparse", "quantized", "blocked", "collection"):
            for query in (
                f"SELECT {key} FROM vectors ORDER BY {key}",
                f"SELECT {key}, COUNT(*) AS n FROM vectors GROUP BY {key}",
                f"SELECT DISTINCT {key} FROM vectors",
                f"SELECT id FROM vectors WHERE {key} = {key}",
                f"SELECT l.id AS id FROM vectors AS l JOIN vectors AS r ON l.{key} = r.{key}",
                f"SELECT id, ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY id) AS n FROM vectors",
            ):
                with self.subTest(query=query), self.assertRaisesRegex(Exception, "vectors are payload-only"):
                    context.sql(query, relations={"vectors": frame}).collect(check=True)
        with self.assertRaisesRegex(Exception, "arithmetic requires"):
            context.sql("SELECT sparse + sparse AS total FROM vectors", relations={"vectors": frame}).collect(check=True)
        events = []
        output_table = table.select(["id", "blocked"])
        schema = dict(context.from_arrow_table(output_table).collect(check=True).result_schema)

        def producer():
            events.append("opened")
            yield output_table

        stream = context.from_batches(producer, schema=schema, streaming=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-refusal-") as directory:
            root = Path(directory)
            for name in ("csv", "json", "jsonl", "avro", "orc"):
                with self.subTest(format=name), self.assertRaisesRegex(Exception, "vector interpretation"):
                    getattr(stream, "write_" + name)(root / ("denied." + name), check=True)
                self.assertEqual(events, [])
                self.assertEqual(list(root.iterdir()), [])
            with self.assertRaisesRegex(Exception, "vector interpretation"):
                stream.fanout({"vortex": root / "denied.vortex", "orc": root / "denied.orc"})
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [])

    def test_vector_streaming_fanout_and_partial_failure_preserve_all_fields_and_cleanup(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        context = self.context()
        table, _ = fixture(pa, repetitions=7)
        table = table.slice(1, 39)
        schema = dict(context.from_arrow_table(table).collect(check=True).result_schema)
        events = []

        def producer():
            events.append("opened")
            try:
                yield table.slice(0, 0)
                yield from table.to_batches(max_chunksize=7)
                events.append("ended")
            finally:
                events.append("closed")

        with tempfile.TemporaryDirectory(prefix="shardloom-vector-stream-") as directory:
            root = Path(directory)
            targets = {name: root / ("vector." + name) for name in ("vortex", "arrow_ipc", "parquet")}
            context.from_batches(producer, schema=schema, streaming=True).fanout(targets)
            self.assertEqual(events, ["opened", "ended", "closed"])
            for name, path in targets.items():
                self.assert_table(getattr(context, "read_" + name)(path).collect(check=True).to_arrow(), table)
            independent = root / "independent.parquet"
            pq.write_table(table, independent, use_dictionary=False)
            self.assert_table(context.read_parquet(independent).collect(check=True).to_arrow(), table)
            failure = LookupError("vector producer stopped after a complete batch")

            def broken():
                try:
                    yield table
                    raise failure
                finally:
                    events.append("failure closed")

            with self.assertRaises(LookupError) as raised:
                context.from_batches(broken, schema=schema, streaming=True).write_vortex(root / "failed.vortex", check=True)
            self.assertIs(raised.exception, failure)
            self.assertEqual(events[-1], "failure closed")
            self.assertEqual(set(root.iterdir()), set(targets.values()) | {independent})

    def test_vector_retained_readers_shrunk_grants_source_changes_and_cancellation(self):
        import pyarrow as pa
        from shardloom._native_backend import NativeResultBatch
        table, _ = fixture(pa, repetitions=20)
        table = table.slice(1, 117)
        session = self.native.Session(self.native.ExecutionResources(128 << 20, 1))
        self.addCleanup(session.close)
        source = session.from_arrow(pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=3)))
        plan = session.sql("SELECT * FROM 'memory://vectors'", sources=[("memory://vectors", source)])
        result = plan.collect()
        plan.release_prepared()
        reserved = session.snapshot()["reserved_bytes"]
        self.assertGreater(reserved, 0)
        with self.assertRaisesRegex(Exception, "memory"):
            plan.collect(resources=self.native.ExecutionResources(reserved - 1, 1))
        self.assertEqual(session.snapshot()["reserved_bytes"], reserved)
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-generation-") as directory:
            path = Path(directory) / "source.vortex"
            plan.write(path, "write_vortex")
            file_plan = session.sql(f"SELECT * FROM '{path}'", files=[(str(path), "vortex", None)])
            retained_file = file_plan.collect()
            path.write_bytes(b"changed source generation")
            with self.assertRaisesRegex(Exception, "prepared source changed"):
                file_plan.collect()
            retained_table = pa.RecordBatchReader.from_stream(retained_file).read_all()
            self.assertEqual(_digest(_rows(retained_table)), _digest(_rows(table)))
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
        del source, plan, result, file_plan, retained_file, retained_table, stream, cancelled
        session.close()
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        for batch in (first, kept):
            retained = NativeResultBatch(batch, 0).to_arrow()
            self.assert_table(retained, table.slice(0, retained.num_rows))
        del batch, retained, first, kept
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_vector_large_lists_preserve_declared_nullabilities_and_refuse_dictionary_wrappers(self):
        import pyarrow as pa
        context = self.context()
        for flags in range(128):
            nullable = lambda bit: bool(flags & (1 << bit))
            sparse = pa.struct([
                pa.field("size", pa.uint64(), nullable=nullable(3)),
                pa.field("indices", pa.large_list(pa.field("item", pa.uint64(), nullable=nullable(1))), nullable=nullable(4)),
                pa.field("values", pa.large_list(pa.field("item", pa.float32(), nullable=nullable(2))), nullable=nullable(5)),
            ])
            blocked = pa.struct([
                pa.field("codes", pa.large_list(pa.field("item", pa.int8(), nullable=nullable(1))), nullable=nullable(3)),
                pa.field("scale", pa.large_list(pa.field("item", pa.float16(), nullable=nullable(2))), nullable=nullable(4)),
                pa.field("zero_point", pa.large_list(pa.field("item", pa.int8(), nullable=nullable(6))), nullable=nullable(5)),
            ])
            fields = [tagged(pa, "s", sparse, "shardloom.sparse_vector.v1",
                            {"version": 1, "index_policy": "strictly_increasing"}, nullable=nullable(0)),
                      tagged(pa, "q", blocked, "shardloom.quantized_vector.v1",
                            {"version": 1, "parameter_mode": "blocked", "block_size": 2}, nullable=nullable(0))]
            rows = [{"s": {"size": (1 << 64) - 1, "indices": [0, (1 << 64) - 2], "values": [-0.0, 1.5]},
                     "q": {"codes": [-128, 0, 127], "scale": [0.5, 2.0], "zero_point": [-128, 127]}}]
            if nullable(0):
                rows.append({"s": None, "q": None})
            table = pa.Table.from_pylist(rows, schema=pa.schema(fields))
            with self.subTest(flags=flags):
                result = context.from_arrow_table(table).collect(check=True)
                # Native List offsets are Int32 on Arrow export. Check logical
                # schema and visible values without changing numeric primitives.
                self.assertEqual(_schema_shape(result.to_arrow().schema), _schema_shape(table.schema))
                self.assertEqual(json.dumps(result.python_objects, sort_keys=True), json.dumps(rows, sort_keys=True))
                self.assertEqual(context.from_rows(result.python_objects,
                    schema=dict(result.result_schema)).collect(check=True).result_schema, result.result_schema)
        table, _ = fixture(pa)
        for name in ("sparse", "quantized", "blocked"):
            array = table.column(name).combine_chunks()
            field = table.schema.field(name)
            dictionary = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2], pa.int8()), array)
            for wrapped, declared in [
                (array, field.with_metadata({b"ARROW:extension:name": b"shardloom.unknown_vector.v2"})),
                (dictionary, pa.field(name, dictionary.type, metadata=field.metadata)),
            ]:
                with self.subTest(name=name, dtype=declared.type), self.assertRaises(Exception):
                    context.from_arrow_table(pa.Table.from_arrays([wrapped], schema=pa.schema([declared])))

    def test_vector_cli_json_envelope_preserves_exact_types_values_and_identity(self):
        import pyarrow as pa
        from shardloom._result_schema import schema_fields, python_rows
        context = self.context()
        table, rows = fixture(pa)
        session = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
        self.addCleanup(session.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-envelope-") as directory:
            path = Path(directory) / "source.vortex"
            context.from_arrow_table(table).write_vortex(path, check=True)
            expected_schema = context.read_vortex(path).collect(check=True).result_schema
            envelope = json.loads(session.command(["run", "cli", "--input", str(path),
                "--input-format", "vortex", "--request", "collect", "--bounded", "true",
                "--execution-policy", "native_vortex", "--native-vortex-operation-family",
                "filter_project_limit", "--vortex-primitive", "project", "--vortex-columns", ",".join(table.schema.names)]))
            self.assertEqual(envelope["status"], "success", envelope)
            self.assertFalse(envelope["fallback"]["attempted"])
            fields = {field["key"]: field["value"] for field in
                      [*envelope.get("fields", []), *envelope.get("result", {}).get("fields", [])]}
            declared = schema_fields(fields["result_schema_json"], fields["result_schema_format"])
            self.assertEqual(declared, expected_schema)
            actual = python_rows(json.loads(fields["result_values_json"]), declared)
            self.assertEqual(json.dumps(actual, sort_keys=True), json.dumps(rows, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
