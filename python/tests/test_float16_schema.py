"""Pure-Python schema and finite typed-JSON admission for Float16."""

import json
import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom import types as t
from shardloom._input_schema import normalize_schema, _wire_type, encode_cell
from shardloom._result_schema import ResultType, schema_fields


class Float16SchemaTests(unittest.TestCase):
    def test_constructor_and_primitive_serde(self):
        dtype = t.float16(nullable=False)
        self.assertEqual(dtype, ResultType("float16", False))
        self.assertIn("float16", t.__all__)
        fields = (("half", dtype),)
        wire = {"Struct": [{"names": ["half"], "dtypes": [_wire_type(dtype)]}, False]}
        self.assertEqual(schema_fields(json.dumps(wire), "vortex.dtype.serde.v1"), fields)
        self.assertEqual(_wire_type(dtype), {"Primitive": ["f16", False]})
        self.assertEqual(normalize_schema(dict(fields)), fields)

    def test_embedding_and_fixed_and_variable_tensor_declarations(self):
        embedding = t.embedding(t.float16(nullable=False), 3, metadata={"space_id": "half"})
        fixed = t.fixed_shape_tensor(t.float16(nullable=False), [2, 2])
        variable = t.variable_shape_tensor(t.float16(nullable=False), 2, uniform_shape=[None, 2])
        for dtype in (embedding, fixed, variable):
            fields = (("value", dtype),)
            self.assertEqual(normalize_schema(dict(fields)), fields)
            wire = {"Struct": [{"names": ["value"], "dtypes": [_wire_type(dtype)]}, False]}
            self.assertEqual(schema_fields(json.dumps(wire), "vortex.dtype.serde.v1"), fields)

    def test_finite_exact_typed_json_values(self):
        dtype = t.float16()
        for value in (1.5, 0.0, -0.0, 65504.0, 2**-24):
            with self.subTest(value=value):
                encoded = encode_cell(dtype, value)
                decoded = json.loads(encoded)
                self.assertEqual(decoded, value)
                if value == 0:
                    self.assertEqual(math.copysign(1.0, decoded), math.copysign(1.0, value))

    def test_inexact_or_nonfinite_values_are_rejected(self):
        dtype = t.float16()
        for value in (0.1, 65520.0, 2**-25, True, float("nan"), float("inf"), -float("inf")):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                encode_cell(dtype, value)

    def test_inferred_float_and_scalar_contracts_are_unchanged(self):
        from shardloom.query import _infer_memory_column_type
        self.assertEqual(_infer_memory_column_type([0.1]), "float64")
        with self.assertRaisesRegex(ValueError, "no native scalar parameter"):
            t.scalar(1.5, t.float16())


if __name__ == "__main__":
    unittest.main()
