"""Shared vector arithmetic and binding through SQL and Python declarations."""

from collections import defaultdict
import json
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .vector_expression_fixture import (
        NUMERICAL_OPERATIONS, assert_numeric, corpus, dense_array, overflow,
    )
else:
    from test_native_binding import _load_native
    from vector_expression_fixture import (
        NUMERICAL_OPERATIONS, assert_numeric, corpus, dense_array, overflow,
    )


class NativeVectorExpressionBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, policy="strict"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=128 << 20,
                             max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def test_vector_all_270_finite_references_through_both_frontdoors_and_policies(self):
        import pyarrow as pa
        from shardloom import functions as f
        cases = corpus("numerical_reference_r2")["cases"]
        self.assertEqual(len(cases), 270)
        groups = defaultdict(list)
        for case in cases:
            groups[case["left_width"], case["right_width"]].append(case)
        # IN lowers to ordinary comparisons; sixteen rows respect both the
        # literal-count limit and the existing expression-depth bound.
        batches = [(widths, rows[start:start + 16]) for widths, rows in groups.items()
                   for start in range(0, len(rows), 16)]
        coverage = set()
        for policy in ("strict", "extended"):
            context = self.context(policy)
            for (left_width, right_width), rows in batches:
                table = pa.table({"id": pa.array(range(len(rows)), pa.int64()),
                    "a": dense_array(pa, left_width, [row["left"] for row in rows]),
                    "b": dense_array(pa, right_width, [row["right"] for row in rows])})
                source = context.from_arrow_table(table)
                for name, function, arguments in NUMERICAL_OPERATIONS:
                    admitted = [index for index, row in enumerate(rows)
                                if policy == "extended" or not overflow(row["expected"][name])]
                    rejected = sorted(set(range(len(rows))) - set(admitted))
                    if admitted:
                        condition = "id IN (" + ",".join(map(str, admitted)) + ")"
                        queries = {
                            "dataframe": source.where(condition).select("id", getattr(f, function)(*arguments).alias("value")),
                            "sql": context.sql(f"SELECT id,{function}({','.join(arguments)}) AS value FROM input WHERE {condition}", relations={"input": source}),
                        }
                        for frontdoor, query in queries.items():
                            with self.subTest(policy=policy, widths=(left_width, right_width), function=name, frontdoor=frontdoor):
                                with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                                    result = query.collect(check=True)
                                self.assertEqual(result.envelope.field("fallback_attempted"), "false")
                                self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
                                actual = result.to_rows()
                                self.assertEqual(sorted(row["id"] for row in actual), admitted)
                                for row in actual:
                                    case = rows[row["id"]]
                                    assert_numeric(self, case, name, row["value"])
                                    coverage.add((policy, frontdoor, case["id"], name))
                    # Strict overflow is a final-result refusal. Cosine and
                    # normalization are still tested above even when a norm or
                    # product cannot be represented as an ordinary F64 value.
                    for index in rejected:
                        for frontdoor in ("dataframe", "sql"):
                            query = (source.where(f"id = {index}").select(getattr(f, function)(*arguments).alias("value"))
                                     if frontdoor == "dataframe" else context.sql(
                                         f"SELECT {function}({','.join(arguments)}) AS value FROM input WHERE id = {index}", relations={"input": source}))
                            with self.subTest(case=rows[index]["id"], function=name, frontdoor=frontdoor), self.assertRaisesRegex(Exception, "nonfinite vector result"):
                                query.collect(check=True)
                            coverage.add((policy, frontdoor, rows[index]["id"], name))
            context.close()
        self.assertEqual(len(coverage), 2 * 2 * len(cases) * len(NUMERICAL_OPERATIONS))

    def test_vector_dense_admission_contextual_arrays_and_integer_controls(self):
        import shardloom as sl
        from shardloom import functions as f, types as t
        context = self.context()
        opened = []
        def producer():
            opened.append(True)
            yield [{"v": []}]
        rejected = [t.list_(child) for child in (
            t.int64(), t.boolean(), t.string(), t.decimal128(8, 2), t.list_(t.float64()))]
        rejected.extend([t.float64(), t.int64(), t.boolean(), t.string(), t.decimal128(8, 2)])
        unary = ["vector_dimension", "vector_values", "vector_norm", "vector_normalize"]
        for dtype in rejected:
            source = context.from_batches(producer, schema={"v": dtype}, streaming=True)
            for function, suffix in [(name, []) for name in unary] + [
                ("vector_element", [0]), ("vector_slice", [0, 1]),
                ("vector_dot", [f.array([1.0])]),
                ("vector_l2_distance", [f.array([1.0])]),
                ("vector_cosine_similarity", [f.array([1.0])]),
            ]:
                declaration = getattr(f, function)("v", *suffix)
                for query in [source.select(declaration.alias("out")),
                              context.sql(f"SELECT {declaration.sql} AS out FROM input", relations={"input": source})]:
                    with self.subTest(dtype=str(dtype), function=function), self.assertRaisesRegex(Exception, "dense Float16/Float32/Float64"):
                        query.schema()
                    self.assertEqual(opened, [])
        for values, expected in [([], 0), ([None, None], 2)]:
            array = f.array(values)
            for query in [context.range(0, 1).select(f.vector_dimension(array).alias("d")),
                          context.sql(f"SELECT VECTOR_DIMENSION({array}) AS d")]:
                self.assertEqual(query.collect(check=True).to_rows(), ({"d": expected},))
        empty = context.range(0, 0)
        for value in [f.array([1, 2]), f.lit(1), f.lit(1.0), f.lit(None)]:
            declarations = [getattr(f, name)(value) for name in unary]
            declarations.extend([f.vector_element(value, 0), f.vector_slice(value, 0)])
            for name in ("vector_dot", "vector_l2_distance", "vector_cosine_similarity"):
                declarations.extend([getattr(f, name)(value, f.array([1.0])),
                                     getattr(f, name)(f.array([1.0]), value)])
            for declaration in declarations:
                for query in [empty.select(declaration.alias("n")), context.sql(
                        f"SELECT {declaration} AS n FROM input", relations={"input": empty})]:
                    with self.subTest(expression=str(declaration)), self.assertRaisesRegex(
                            Exception, "dense Float16/Float32/Float64"):
                        query.schema()
        for dtype, indices in [(t.int8(), [-128, -1, 0, 127]), (t.int16(), [-32768, -2, 1, 32767]),
                               (t.int32(), [-(1 << 31), -1, 0, (1 << 31)-1]),
                               (t.int64(), [-(1 << 63), -1, 0, (1 << 63)-1]),
                               (t.uint8(), [0, 1, 2, 255]), (t.uint16(), [0, 1, 2, 65535]),
                               (t.uint32(), [0, 1, 2, (1 << 32)-1]),
                               (t.uint64(), [0, 1, 2, (1 << 64)-1])]:
            indices = [*indices, None]
            source = context.from_rows([{"v": [3.0, 4.0], "i": value} for value in indices],
                                       schema={"v": t.list_(t.float32()), "i": dtype})
            expected = tuple({"e": None if i is None or not -2 <= i < 2 else [3.0, 4.0][i],
                              "s": None if i is None else [3.0, 4.0][i:]} for i in indices)
            for query in [source.select(f.vector_element("v", "i").alias("e"), f.vector_slice("v", "i").alias("s")),
                          context.sql("SELECT VECTOR_ELEMENT(v,i) AS e,VECTOR_SLICE(v,i) AS s FROM input", relations={"input": source})]:
                self.assertEqual(query.collect(check=True).to_rows(), expected)

    def test_vector_identity_refusals_precede_producers_and_explicit_values_admit(self):
        from shardloom import functions as f, types as t
        context = self.context()
        cases = corpus("semantic_reference_r1")["binding_cases"]
        opened = []
        for case in cases:
            def dtype(value):
                if "embedding" in value:
                    metadata = dict(value["embedding"])
                    metadata.pop("version")
                    return t.embedding(t.float64(), value["fixed"], metadata=metadata)
                return t.fixed_size_list(t.float64(), value["fixed"])
            def producer():
                opened.append(True)
                yield []
            source = context.from_batches(producer, schema={"a": dtype(case["left"]), "b": dtype(case["right"])}, streaming=True)
            for operation in ("vector_dot", "vector_l2_distance", "vector_cosine_similarity"):
                expression = getattr(f, operation)("a", "b")
                for query in [source.select(expression.alias("value")),
                              context.sql(f"SELECT {expression.sql} AS value FROM input", relations={"input": source})]:
                    with self.subTest(case=case["id"], operation=operation):
                        if case["accepted"]:
                            self.assertEqual(query.schema()[0][1].name, "float64")
                        else:
                            with self.assertRaisesRegex(Exception, "embedding|dimensions differ"):
                                query.schema()
                    self.assertEqual(opened, [])
            if case["left"]["fixed"] == case["right"]["fixed"]:
                expression = f.vector_dot(f.vector_values("a"), f.vector_values("b"))
                self.assertEqual(source.select(expression.alias("v")).schema()[0][1].name, "float64")
                self.assertEqual(opened, [])

    def test_tensor_shape_fixed_variable_integer_boolean_null_and_zero_dimensions(self):
        from shardloom import functions as f, types as t
        context = self.context()
        source = context.from_rows([
            {"fixed": [True] * 6, "variable": {"data": list(range(6)), "shape": [2, 3]}, "scalar": [42], "zero": []},
            {"fixed": None, "variable": None, "scalar": None, "zero": None},
            {"fixed": [False] * 6, "variable": {"data": [], "shape": [0, 3]}, "scalar": [7], "zero": []},
        ], schema={"fixed": t.fixed_shape_tensor(t.boolean(), [2, 3], permutation=[1, 0]),
                   "variable": t.variable_shape_tensor(t.int16(), 2, permutation=[1, 0], uniform_shape=[None, 3]),
                   "scalar": t.fixed_shape_tensor(t.int64(), []),
                   "zero": t.fixed_shape_tensor(t.int8(), [(1 << 63)-1, 0])})
        expressions = [f.tensor_shape("fixed").alias("fixed"), f.tensor_shape("fixed", "physical").alias("physical"),
                       f.tensor_shape("variable").alias("variable"), f.tensor_shape("scalar").alias("scalar"),
                       f.tensor_shape("zero").alias("zero")]
        expected = ({"fixed": [3, 2], "physical": [2, 3], "variable": [3, 2], "scalar": [], "zero": [(1 << 63)-1, 0]},
                    {name: None for name in ("fixed", "physical", "variable", "scalar", "zero")},
                    {"fixed": [3, 2], "physical": [2, 3], "variable": [3, 0], "scalar": [], "zero": [(1 << 63)-1, 0]})
        for query in [source.select(*expressions), context.sql("SELECT " + ",".join(map(str, expressions)) + " FROM input", relations={"input": source})]:
            self.assertEqual(query.collect(check=True).to_rows(), expected)


if __name__ == "__main__":
    unittest.main()
