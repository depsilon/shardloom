"""Vector expression declarations stay inert and preserve their inputs."""

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom import ExecutionResourceLimits
from shardloom import functions as f
from shardloom.query import ColumnExpression, WorkflowSource


class VectorExpressionDeclarationTests(unittest.TestCase):
    def test_exports_and_function_sql(self) -> None:
        cases = (
            (f.vector_dimension("v"), "VECTOR_DIMENSION(v)"),
            (f.vector_values("v"), "VECTOR_VALUES(v)"),
            (f.vector_element("v", -2), "VECTOR_ELEMENT(v,-2)"),
            (f.vector_slice("v", 1, 4), "VECTOR_SLICE(v,1,4)"),
            (f.vector_dot("left_v", "right_v"), "VECTOR_DOT(left_v,right_v)"),
            (f.vector_l2_distance("left_v", "right_v"),
             "VECTOR_L2_DISTANCE(left_v,right_v)"),
            (f.vector_cosine_similarity("left_v", "right_v"),
             "VECTOR_COSINE_SIMILARITY(left_v,right_v)"),
            (f.vector_norm("v"), "VECTOR_NORM(v)"),
            (f.vector_normalize("v"), "VECTOR_NORMALIZE(v)"),
        )
        names = (
            "vector_dimension", "tensor_shape", "vector_values", "vector_element",
            "vector_slice", "vector_dot", "vector_l2_distance",
            "vector_cosine_similarity", "vector_norm", "vector_normalize",
        )
        for name in names:
            with self.subTest(export=name):
                self.assertIn(name, f.__all__)
                self.assertTrue(callable(getattr(f, name)))
        for expression, expected in cases:
            with self.subTest(sql=expected):
                self.assertEqual(expression.sql, expected)
        self.assertEqual(f.tensor_shape("tensor").sql,
                         "TENSOR_SHAPE(tensor,'logical')")
        self.assertEqual(f.tensor_shape("tensor", "physical").sql,
                         "TENSOR_SHAPE(tensor,'physical')")

    def test_tensor_layout_requires_exact_supported_literal(self) -> None:
        for layout in ("LOGICAL", "Physical", "logical ", "row_major", 1, None):
            with self.subTest(layout=layout), self.assertRaises(ValueError):
                f.tensor_shape("tensor", layout)

    def test_column_syntax_literals_and_slice_omission(self) -> None:
        self.assertEqual(f.vector_dot("left", "right").sql, "VECTOR_DOT(left,right)")
        self.assertEqual(f.vector_dot(f.col("left"), f.col("right")).sql,
                         "VECTOR_DOT(left,right)")
        self.assertEqual(f.vector_dimension(f.array([1.0, 2.0])).sql,
                         "VECTOR_DIMENSION(ARRAY[1.0,2.0])")
        self.assertEqual(f.vector_slice("v", 2).sql, "VECTOR_SLICE(v,2)")
        self.assertEqual(f.vector_slice("v", 2, None).sql, "VECTOR_SLICE(v,2,NULL)")

    def test_integer_controls_and_column_controls(self) -> None:
        controls = (
            (-1, "-1"),
            (-(1 << 63), str(-(1 << 63))),
            ((1 << 64) - 1, str((1 << 64) - 1)),
        )
        for value, rendered in controls:
            with self.subTest(value=value):
                self.assertEqual(f.vector_element("v", value).sql,
                                 f"VECTOR_ELEMENT(v,{rendered})")
                self.assertEqual(f.vector_slice("v", value).sql,
                                 f"VECTOR_SLICE(v,{rendered})")
        self.assertEqual(f.vector_element("v", "idx").sql, "VECTOR_ELEMENT(v,idx)")
        self.assertEqual(f.vector_slice("v", "start", "end").sql,
                         "VECTOR_SLICE(v,start,end)")
        for value in (True, False, 1.0, 1j, object()):
            with self.subTest(value=value), self.assertRaises(TypeError):
                f.vector_element("v", value)
            with self.subTest(slice_value=value), self.assertRaises(TypeError):
                f.vector_slice("v", value)

    def test_composite_array_keeps_other_operand_bindings(self) -> None:
        source = WorkflowSource("vortex", "declared.vortex")
        limits = ExecutionResourceLimits(memory_bytes=4096, max_parallelism=2)
        vector = ColumnExpression("stored_vector", (source,), limits)
        expression = f.vector_dot(f.array([1.0, 2.0]), vector)
        self.assertEqual(expression.sql,
                         "VECTOR_DOT(ARRAY[1.0,2.0],stored_vector)")
        self.assertEqual(expression.source_bindings, (source,))
        self.assertEqual(expression.resource_limits, limits)


if __name__ == "__main__":
    unittest.main()
