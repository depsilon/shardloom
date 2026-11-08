"""Raw selection, inspectors and refusals through the shared public expressions."""

from pathlib import Path
import tempfile
import unittest

if __package__:
    from .test_native_binding import _load_native
    from .vector_expression_fixture import dense_array
    from .vector_fixture import fixture
else:
    from test_native_binding import _load_native
    from vector_expression_fixture import dense_array
    from vector_fixture import fixture


def raw_components(array):
    width = array.type.bit_width // 8
    data = memoryview(array.buffers()[1])
    return [None if not array[index].is_valid else
            int.from_bytes(data[(array.offset + index) * width:(array.offset + index + 1) * width], "little")
            for index in range(len(array))]


def raw_lists(chunked):
    return [None if not row.is_valid else raw_components(row.values)
            for array in chunked.chunks for row in array]


class NativeVectorExpressionSemanticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, policy="strict"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def test_vector_selection_keeps_original_float_bits_under_both_policies(self):
        import pyarrow as pa
        from shardloom import functions as f
        # Both signs, signalling/quiet NaNs, infinities and null components.
        patterns = {
            16: [0x7c01, 0xfc00, 0x8000, None, 0, 0x7c00, 0xfe41],
            32: [0x7f800042, 0xff800000, 0x80000000, None, 0, 0x7f800000, 0xffc00081],
            64: [0x7ff0000000000042, 0xfff0000000000000, 0x8000000000000000,
                 None, 0, 0x7ff0000000000000, 0xfff8000000000081],
        }
        with tempfile.TemporaryDirectory(prefix="shardloom-vector-carriage-") as directory:
            writer = self.context("extended")
            for width, bits in patterns.items():
                rows = [bits, None, [], [None, bits[0]]]
                encoded = [[None if value is None else f"{value:0{width//4}x}" for value in row]
                           if row is not None else None for row in rows]
                table = pa.table({"id": pa.array(range(4), pa.int64()),
                                  "v": dense_array(pa, width, encoded)})
                path = Path(directory) / f"bits{width}.vortex"
                writer.from_arrow_table(table).write_vortex(path, check=True)
                for policy in ("strict", "extended"):
                    context = self.context(policy)
                    source = context.read_vortex(path)
                    expressions = [f.col("id"), f.vector_values("v").alias("values"),
                        f.vector_element("v", 0).alias("first"),
                        f.vector_slice("v", 1, -1).alias("middle"),
                        f.vector_dimension("v").alias("dimension")]
                    for query in [source.select(*expressions), context.sql(
                            "SELECT " + ",".join(map(str, expressions)) + " FROM input",
                            relations={"input": source})]:
                        with self.subTest(width=width, policy=policy):
                            actual = query.collect(check=True).to_arrow()
                            self.assertEqual(actual.column("first").type.bit_width, width)
                            self.assertEqual(raw_lists(actual.column("values")), rows)
                            self.assertEqual(raw_lists(actual.column("middle")),
                                             [None if row is None else row[1:-1] for row in rows])
                            self.assertEqual([bit for chunk in actual.column("first").chunks
                                              for bit in raw_components(chunk)],
                                             [bits[0], None, None, None])
                            self.assertEqual(actual.column("dimension").to_pylist(), [7, None, 0, 2])
                            aliased = query.select("id", "values AS renamed", "first AS selected")
                            carried = aliased.where("id <> 2").sort("id").limit(2).collect(check=True).to_arrow()
                            self.assertEqual(raw_lists(carried.column("renamed")), rows[:2])
                            self.assertEqual([bit for chunk in carried.column("selected").chunks
                                              for bit in raw_components(chunk)], [bits[0], None])
                            streamed = []
                            for batch in query.iter_batches(batch_rows=2):
                                streamed.extend(raw_lists(batch.to_arrow().column("values")))
                            self.assertEqual(streamed, rows)
                    if policy == "strict":
                        # Only explicitly carried fields bypass numerical
                        # observation at output; sibling inputs and true numeric
                        # consumers keep their existing strict rules.
                        with self.assertRaisesRegex(Exception, "finite"):
                            source.select(f.vector_values("v").alias("carried"), "v").collect(check=True)
                        with self.assertRaisesRegex(Exception, "nonfinite vector component"):
                            source.where("id = 0").select(
                                f.vector_norm(f.vector_slice("v", 0, 1)).alias("observed")).collect(check=True)
                        with self.assertRaisesRegex(Exception, "finite"):
                            source.select(f.vector_element("v", 0).alias("first")).sort(
                                "first", nulls="last").collect(check=True)
                    # Null-component arithmetic is decided before the otherwise
                    # rejected signalling NaN in this same selected row.
                    for query in [source.where("id = 3").select(f.vector_norm("v").alias("norm")),
                                  context.sql("SELECT VECTOR_NORM(v) AS norm FROM input WHERE id = 3",
                                              relations={"input": source})]:
                        self.assertEqual(query.collect(check=True).to_rows(), ({"norm": None},))

    def test_vector_inspection_and_dense_refusals_for_rich_representations(self):
        import pyarrow as pa
        from shardloom import functions as f
        context = self.context()
        table, rows = fixture(pa)
        source = context.from_arrow_table(table)
        columns = ["sparse", "ordered", "quantized", "blocked"]
        expressions = [f.vector_dimension(name).alias(name) for name in columns]
        expressions.extend([f.tensor_shape("tensor").alias("tensor"),
                            f.tensor_shape("variable").alias("variable")])
        expected = tuple({**{name: None if row[name] is None else
                                 row[name]["size"] if name in ("sparse", "ordered") else
                                 len(row[name]["codes"]) for name in columns},
                          "tensor": [2], "variable": row["variable"]["shape"]} for row in rows)
        for query in [source.select(*expressions), context.sql(
                "SELECT " + ",".join(map(str, expressions)) + " FROM input", relations={"input": source})]:
            self.assertEqual(query.collect(check=True).to_rows(), expected)
        for column in [*columns, "tensor", "variable", "collection"]:
            declarations = [f.vector_values(column), f.vector_element(column, 0),
                f.vector_slice(column, 0), f.vector_norm(column), f.vector_normalize(column),
                f.vector_dot(column, column), f.vector_l2_distance(column, column),
                f.vector_cosine_similarity(column, column)]
            if column in ("tensor", "variable", "collection"):
                declarations.append(f.vector_dimension(column))
            for expression in declarations:
                for query in [source.select(expression.alias("v")), context.sql(
                        f"SELECT {expression} AS v FROM input", relations={"input": source})]:
                    with self.subTest(column=column, expression=str(expression)), self.assertRaisesRegex(
                            Exception, "dense Float16/Float32/Float64"):
                        query.schema()

    def test_vector_null_parent_does_not_hide_argument_errors_or_constructor_limits(self):
        import pyarrow as pa
        from shardloom import functions as f, types as t
        context = self.context()
        source = context.from_rows([{"v": None, "x": 1.0}],
                                   schema={"v": t.list_(t.float64()), "x": t.float64()})
        for expression in ["VECTOR_ELEMENT(v,CAST(1/0 AS int64))",
                           "VECTOR_SLICE(v,CAST(1/0 AS int64))"]:
            for query in [source.select(expression + " AS v"), context.sql(
                    f"SELECT {expression} AS v FROM input", relations={"input": source})]:
                with self.assertRaisesRegex(Exception, "division by zero"):
                    query.collect(check=True)
        # The widened literal path must not remove the existing bound on
        # typed native nonliteral constructors. SQL ARRAY remains literal-only.
        session = self.native.Session(self.native.ExecutionResources(128 << 20, 1))
        self.addCleanup(session.close)
        supplied = session.from_arrow(pa.table({"x": [1.0]}))
        expression = self.native.Expression
        for count in (128, 129):
            array = expression.list([expression.column("x")] * count)
            relation = self.native.Relation.scan("memory://dynamic-list").project([
                ("d", expression.call("vector_dimension", [array]))])
            plan = session.plan(relation, sources=[("memory://dynamic-list", supplied)])
            if count == 128:
                self.assertEqual(plan.collect().row_export()[0], [{"d": 128}])
            else:
                with self.assertRaisesRegex(Exception, "list constructor exceeds 128"):
                    plan.collect()
        # Contextual floating inference must not change ordinary all-null arrays.
        self.assertEqual(context.range(0, 1).select(f"{f.array([None])} AS v").schema()[0][1].parameters[0].name,
                         "bool")


if __name__ == "__main__":
    unittest.main()
