"""Exact binary16 storage and scalar semantics through public native surfaces."""

from __future__ import annotations

import math
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .float16_fixture import BITS, FINITE_BITS, _half_array, _wide, _rows
    from .test_native_float_policy_binding import bit_class, semantic_key, reduce_bits, result_bits
else:
    from test_native_binding import _load_native
    from float16_fixture import BITS, FINITE_BITS, _half_array, _wide, _rows
    from test_native_float_policy_binding import bit_class, semantic_key, reduce_bits, result_bits


class NativeFloat16BindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="shardloom-float16-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.sequence = 0

    def context(self, policy="extended"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def table(self, bits=BITS):
        pa = self.pa
        return pa.Table.from_arrays([pa.array(range(len(bits)), pa.int64()), _half_array(pa, bits)],
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.float16())]))

    def assert_table(self, actual, expected):
        self.assertTrue(actual.schema.equals(expected.schema, check_metadata=True),
                        (actual.schema, expected.schema))
        self.assertEqual(_rows(actual), _rows(expected))

    def five(self, frame, relation, sql, expected):
        import shardloom as sl
        policy = frame.client.float_policy
        native = self.native.Session(self.native.ExecutionResources(128 << 20, 1), float_policy=policy)
        self.addCleanup(native.close)
        with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
            for label, query in (("dataframe", frame), ("python_sql", sl.sql(sql, client=frame.client))):
                with self.subTest(surface=label):
                    result = query.collect(check=True, memory_bytes=128 << 20, max_parallelism=1)
                    self.assertFalse(result.envelope.field_bool("fallback_attempted"))
                    self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
                    self.assert_table(result.to_arrow(), expected)
            for label, plan in (("typed_plan", native.plan(relation)),
                ("embedded_workflow", native.workflow(["sql", "--sql", sql, "--request", "collect",
                    "--bounded", "true", "--memory-bytes", str(128 << 20), "--max-parallelism", "1"]))):
                with self.subTest(surface=label):
                    result = plan.collect()
                    self.assertEqual(result.report["fallback_attempted"], "false")
                    self.assertEqual(result.report["external_engine_invoked"], "false")
                    self.assert_table(self.pa.RecordBatchReader.from_stream(result).read_all(), expected)
        self.sequence += 1
        path = self.root / f"cli-{self.sequence}.arrow"
        cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
            transport="subprocess", use_persistent_worker=False, memory_bytes=128 << 20,
            max_parallelism=1, timeout=60, float_policy=policy)
        try:
            report = sl.sql(sql, client=cli).write_arrow_ipc(path, check=True)
            self.assertFalse(report.envelope.field_bool("fallback_attempted"))
            self.assertFalse(report.envelope.field_bool("external_engine_invoked"))
            with self.pa.ipc.open_file(path) as reader:
                self.assert_table(reader.read_all(), expected)
        finally:
            cli.close()

    def test_float16_exact_python_construction_and_row_extraction(self):
        from shardloom import types as t
        context = self.context()
        rows = [{"x": _wide(bits)} for bits in BITS]
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            frame = context.from_rows(rows, schema={"x": t.float16()})
            result = frame.collect(check=True)
        expected = self.pa.Table.from_arrays([_half_array(self.pa, BITS)], names=["x"])
        self.assert_table(result.to_arrow(), expected)
        for actual, original in zip(result.python_objects, rows):
            if original["x"] is None:
                self.assertIsNone(actual["x"])
            else:
                self.assertEqual(struct.pack("<d", actual["x"]), struct.pack("<d", original["x"]))
        self.assertEqual(result.result_schema, (("x", t.float16()),))
        for value in (0.1, 65520.0, 2**-25, 2**64 + 1, True,
                      struct.unpack("<d", struct.pack("<Q", 0x7ff8000000000001))[0]):
            with self.subTest(value=value), self.assertRaises(Exception):
                context.from_rows([{"x": value}], schema={"x": t.float16()})
        strict = self.context("strict")
        for value in (math.inf, -math.inf, _wide(0x7c01)):
            with self.assertRaisesRegex(Exception, "finite|float_policy"):
                strict.from_rows([{"x": value}], schema={"x": t.float16()})
        context.close()
        del frame
        self.assert_table(result.to_arrow(), expected)

    def test_float16_every_pattern_persists_without_widening(self):
        import pyarrow.parquet as pq
        pa, context = self.pa, self.context()
        expected = self.table(list(range(1 << 16)))
        source = context.from_arrow_table(expected)
        result = source.collect(check=True)
        self.assert_table(result.to_arrow(), expected)
        for name in ("vortex", "arrow_ipc", "parquet"):
            path = self.root / ("all-bits." + name)
            getattr(source, "write_" + name)(path, check=True)
            actual = getattr(context, "read_" + name)(path).collect(check=True).to_arrow()
            self.assert_table(actual, expected)
            if name == "arrow_ipc":
                with pa.ipc.open_file(path) as reader:
                    self.assert_table(reader.read_all(), expected)
            elif name == "parquet":
                self.assert_table(pq.read_table(path), expected)
        context.close()
        del source
        self.assert_table(result.to_arrow(), expected)

    def test_float16_nonrepresentable_threshold_uses_exact_public_evaluation(self):
        from shardloom import functions as f
        pa, context = self.pa, self.context("strict")
        bits = FINITE_BITS
        expected = self.table(bits)
        selected = [row for row, value in enumerate(bits) if value is not None and _wide(value) < 0.1]
        wanted = expected.take(pa.array(selected, pa.int64()))
        source = context.from_arrow_table(expected)
        self.assert_table(source.where("x < 0.1").collect(check=True).to_arrow(), wanted)
        self.assert_table(context.sql("SELECT * FROM input WHERE x < 0.1", relations={"input": source})
                          .collect(check=True).to_arrow(), wanted)
        for name in ("vortex", "arrow_ipc", "parquet"):
            path = self.root / ("threshold." + {"arrow_ipc": "arrow"}.get(name, name))
            getattr(source, "write_" + name)(path, check=True)
            frame = getattr(context, "read_" + name)(path)
            expression = self.native.Expression
            relation = self.native.Relation.scan(str(path)).filter(expression.column("x") < expression.literal(0.1))
            self.five(frame.where(f.col("x") < 0.1), relation, f"SELECT * FROM '{path}' WHERE x < 0.1", wanted)

    def test_float16_scalar_expressions_and_classification_keep_result_domains(self):
        from shardloom import functions as f
        pa, context, source = self.pa, self.context(), self.table()
        path = self.root / "expressions.vortex"
        context.from_arrow_table(source).write_vortex(path, check=True)
        selected, typed, arrays, fields = ["id"], [("id", self.native.Expression.column("id"))], [source["id"]], [source.schema.field("id")]
        for name in ("is_nan", "is_infinite", "is_finite", "signbit"):
            selected.append(getattr(f, name)("x").alias(name))
            typed.append((name, self.native.Expression.call(name, [self.native.Expression.column("x")])))
            arrays.append(pa.array([None if bits is None else bit_class(bits, 16)[name] for bits in BITS], pa.bool_()))
            fields.append(pa.field(name, pa.bool_()))
        for name, public, native, calculate in (
            ("plus", f.col("x") + 0.5, self.native.Expression.column("x") + self.native.Expression.literal(0.5), lambda x: x + 0.5),
            ("times", f.col("x") * 0.0, self.native.Expression.column("x") * self.native.Expression.literal(0.0), lambda x: x * 0.0)):
            selected.append(public.alias(name))
            typed.append((name, native))
            values = [None if bits is None else struct.unpack("<d", struct.pack("<Q", result_bits(calculate(_wide(bits)))))[0] for bits in BITS]
            arrays.append(pa.array(values, pa.float64()))
            fields.append(pa.field(name, pa.float64()))
        expected = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        self.five(context.read_vortex(path).select(*selected), self.native.Relation.scan(str(path)).project(typed),
                  "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'", expected)

    def test_float16_key_identity_and_preserving_reducers_match_all_surfaces(self):
        from shardloom import functions as f
        pa, context, source = self.pa, self.context(), self.table()
        path = self.root / "keys.vortex"
        context.from_arrow_table(source).write_vortex(path, check=True)
        frame, scan = context.read_vortex(path), self.native.Relation.scan(str(path))
        order = sorted(range(len(BITS)), key=lambda row: semantic_key(BITS[row], 16))
        self.five(frame.sort("x", nulls="last"), scan.sort([("x", False, "last")]),
                  f"SELECT * FROM '{path}' ORDER BY x NULLS LAST", source.take(pa.array(order)))
        groups = {}
        for row, bits in enumerate(BITS):
            groups.setdefault(semantic_key(bits, 16), [row, 0])[1] += 1
        entries = [groups[key] for key in sorted(groups)]
        expected = pa.Table.from_arrays([_half_array(pa, [BITS[row] for row, _ in entries]),
            pa.array([count for _, count in entries], pa.uint64())],
            schema=pa.schema([source.schema.field("x"), pa.field("n", pa.uint64(), nullable=False)]))
        self.five(frame.group_by("x").agg(f.count().alias("n")).sort("x", nulls="last"),
            scan.aggregate(["x"], [self.native.AggregateMeasure("count", None, "n")]).sort([("x", False, "last")]),
            f"SELECT x, COUNT(*) AS n FROM '{path}' GROUP BY x ORDER BY x NULLS LAST", expected)
        selected, typed, arrays, fields = [], [], [], []
        for index, (name, level) in enumerate((("sum", None), ("avg", None), ("min", None), ("max", None),
                                             ("quantile_disc", 1.0), ("quantile_cont", 1.0))):
            alias = "r" + str(index)
            selected.append(getattr(f, name)("x", *([] if level is None else [level])).alias(alias))
            typed.append(self.native.AggregateMeasure(name, "x", alias, quantile_level=level))
            bits = reduce_bits(BITS, name, 16, level)
            preserving = name in ("min", "max", "quantile_disc")
            arrays.append(_half_array(pa, [bits]) if preserving else pa.array(
                [None if bits is None else struct.unpack("<d", struct.pack("<Q", bits))[0]], pa.float64()))
            fields.append(pa.field(alias, pa.float16() if preserving else pa.float64()))
        self.five(frame.agg(*selected), scan.aggregate([], typed),
                  "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'",
                  pa.Table.from_arrays(arrays, schema=pa.schema(fields)))

    def test_float16_dictionary_and_masked_nonfinite_admission(self):
        pa, context, strict = self.pa, self.context(), self.context("strict")
        values = _half_array(pa, [0x3c00, 0x7c01, None, 0x8000])
        array = pa.DictionaryArray.from_arrays(pa.array([3, 0, 1, 2, None, 1], pa.int8()), values)
        source = pa.table({"x": array})
        expected = pa.table({"x": _half_array(pa, [0x8000, 0x3c00, 0x7c01, None, None, 0x7c01])})
        # File dictionaries use the established ingest admission. Caller-owned
        # encoded Arrow transport has its own narrower, feature-gated contract.
        path = self.root / "dictionary.arrow"
        with pa.ipc.new_file(path, source.schema) as writer:
            writer.write_table(source)
        self.assert_table(context.read_arrow_ipc(path).collect(check=True).to_arrow(), expected)
        # The existing dictionary policy checks its complete valid value domain,
        # including entries not referenced by a code. Binary16 follows it too.
        unreferenced = pa.table({"x": pa.DictionaryArray.from_arrays(pa.array([0], pa.int8()), values)})
        unreferenced_path = self.root / "unreferenced.arrow"
        with pa.ipc.new_file(unreferenced_path, unreferenced.schema) as writer:
            writer.write_table(unreferenced)
        with self.assertRaisesRegex(Exception, "non-finite dictionary"):
            strict.read_arrow_ipc(unreferenced_path).collect(check=True)
        hidden = pa.Array.from_buffers(pa.float16(), 2,
            [pa.py_buffer(b"\x02"), pa.py_buffer(struct.pack("<HH", 0x7c01, 0x3c00))], null_count=1)
        table = pa.table({"x": hidden})
        self.assert_table(strict.from_arrow_table(table).collect(check=True).to_arrow(), table)
        raw = _half_array(pa, [0x7c01, 0x3c00])
        parent = pa.FixedSizeListArray.from_arrays(raw, 1, mask=pa.array([True, False]))
        table = pa.table({"parent": parent})
        self.assert_table(strict.from_arrow_table(table).collect(check=True).to_arrow(), table)
        visible = pa.table({"parent": pa.FixedSizeListArray.from_arrays(raw, 1)})
        with self.assertRaisesRegex(Exception, "finite|float_policy"):
            strict.from_arrow_table(visible)

    def test_float16_strict_file_selection_and_classification_observe_only_their_domain(self):
        from shardloom import functions as f
        pa, extended, strict = self.pa, self.context(), self.context("strict")
        source = self.table([0x7c01, 0x3c00])
        path = self.root / "strict.vortex"
        extended.from_arrow_table(source).write_vortex(path, check=True)
        frame = strict.read_vortex(path)
        self.assert_table(frame.where("id = 1").collect(check=True).to_arrow(), source.slice(1, 1))
        self.assert_table(frame.collect(check=True).to_arrow(), source)
        for observed in (frame.select((f.col("x") + 1.0).alias("x")),
                         frame.where(f.col("x") > 0.0), frame.sort("x")):
            with self.assertRaisesRegex(Exception, "finite|nonfinite"):
                observed.collect(check=True)
        classified = frame.select(f.is_nan("x").alias("nan")).collect(check=True).to_arrow()
        self.assert_table(classified, pa.table({"nan": pa.array([True, False], pa.bool_())}))
        for sql in (f"SELECT x FROM '{path}' UNION ALL SELECT 1.0 AS x",
                    f"SELECT CASE WHEN id = 0 THEN x ELSE 1.0 END AS x FROM '{path}'"):
            with self.subTest(sql=sql), self.assertRaisesRegex(Exception, "finite|nonfinite"):
                strict.sql(sql).collect(check=True)
        hidden = strict.sql(f"SELECT CASE WHEN id = 1 THEN x ELSE 1.0 END AS x FROM '{path}'")
        self.assert_table(hidden.collect(check=True).to_arrow(), pa.table({"x": pa.array([1.0, 1.0], pa.float64())}))

    def test_float16_mixed_keys_widened_sets_and_range_windows(self):
        import shardloom as sl
        from shardloom import functions as f
        pa, context = self.pa, self.context()
        left = context.from_arrow_table(self.table([0x8000, 0x7c01, 0x3e00]))
        right_table = pa.Table.from_arrays([pa.array([3, 4], pa.int64()),
            pa.array([0.0, _wide(0xfc42)], pa.float64())],
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.float64())]))
        right = context.from_arrow_table(right_table)
        result = context.sql("SELECT l.id AS left_id, r.id AS right_id FROM l JOIN r ON l.x = r.x ORDER BY left_id",
                             relations={"l": left, "r": right}).collect(check=True).to_arrow()
        expected = pa.Table.from_arrays([pa.array([0, 1], pa.int64()), pa.array([3, 4], pa.int64())],
            schema=pa.schema([pa.field("left_id", pa.int64(), nullable=False),
                              pa.field("right_id", pa.int64(), nullable=False)]))
        self.assert_table(result, expected)
        union = context.sql("SELECT id,x FROM l UNION ALL SELECT id,x FROM r ORDER BY id",
                            relations={"l": left, "r": right}).collect(check=True).to_arrow()
        expected = pa.Table.from_arrays([pa.array(range(5), pa.int64()),
            pa.array([_wide(0x8000), _wide(0x7c01), 1.5, 0.0, _wide(0xfc42)], pa.float64())],
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.float64())]))
        self.assert_table(union, expected)
        promoted = context.sql("SELECT id, CASE WHEN id < 3 THEN x ELSE 1.0 END AS x FROM l ORDER BY id",
                               relations={"l": left}).collect(check=True).to_arrow()
        self.assert_table(promoted, expected.slice(0, 3))
        promoted = context.sql("SELECT id, COALESCE(x,1.0) AS x FROM l ORDER BY id",
                               relations={"l": left}).collect(check=True).to_arrow()
        nonnullable = pa.schema([expected.schema.field("id"), pa.field("x", pa.float64(), nullable=False)])
        self.assert_table(promoted, pa.Table.from_arrays(expected.slice(0, 3).columns, schema=nonnullable))
        bits = [0xbc00, 0x8000, 0, 0x3800, 0x3c00]
        frame = context.from_arrow_table(self.table(bits))
        window = sl.Window.order_by("x").range_between(-0.5, 0.5)
        result = frame.select("id", "x", f.sum("x").over(window).alias("total"),
                              f.min("x").over(window).alias("minimum")).sort("id").collect(check=True).to_arrow()
        numbers = [_wide(value) for value in bits]
        total, minimum = [], []
        for number in numbers:
            members = [index for index, other in enumerate(numbers) if number - 0.5 <= other <= number + 0.5]
            total.append(sum((numbers[index] for index in members), 0.0))
            minimum.append(bits[min(members, key=lambda index: numbers[index])])
        expected = self.table(bits).append_column("total", pa.array(total, pa.float64())).append_column(
            "minimum", _half_array(pa, minimum))
        self.assert_table(result, expected)


if __name__ == "__main__":
    unittest.main()
