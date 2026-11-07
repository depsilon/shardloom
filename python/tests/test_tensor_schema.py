"""Fixed tensor shape and metadata contracts without a native runtime."""

import json
import unittest

from shardloom import types as t
from shardloom._input_schema import normalize_schema, _wire_type, encode_cell
from shardloom._result_schema import ResultType, schema_fields, python_rows
from shardloom._tensor import decode_metadata, metadata_json
from shardloom.errors import ShardLoomProtocolError


class TensorSchemaTests(unittest.TestCase):
    def test_fixed_tensor_declared_shape_metadata_and_nullability_roundtrip(self):
        shape = [2, 3, 4]
        names = ["Ω", "", "Ω"]
        permutation = [2, 0, 1]
        dtype = t.fixed_shape_tensor(t.float32(nullable=False), shape, dim_names=names,
                                     permutation=permutation)
        self.assertEqual(dtype.parameters[1], 24)
        self.assertEqual(dict(dtype.parameters[2]), {
            "shape": (2, 3, 4), "dim_names": ("Ω", "", "Ω"), "permutation": (2, 0, 1)})
        self.assertFalse(dtype.parameters[0].nullable)
        self.assertTrue(dtype.nullable)
        fields = (("tensor", dtype),)
        self.assertEqual(normalize_schema(dict(fields)), fields)
        wire = {"Struct": [{"names": ["tensor"], "dtypes": [_wire_type(dtype)]}, False]}
        self.assertEqual(schema_fields(json.dumps(wire), "vortex.dtype.serde.v1"), fields)
        shape[0] = 7
        names[0] = "changed"
        permutation.reverse()
        self.assertEqual(dict(dtype.parameters[2])["shape"], (2, 3, 4))
        self.assertEqual(dict(dtype.parameters[2])["dim_names"][0], "Ω")
        self.assertEqual(dict(dtype.parameters[2])["permutation"], (2, 0, 1))

    def test_fixed_tensor_rank_zero_and_zero_dimensions_keep_distinct_values(self):
        scalar = t.fixed_shape_tensor(t.int32(), ())
        annotated = t.fixed_shape_tensor(t.int32(), (), dim_names=(), permutation=())
        self.assertNotEqual(scalar, annotated)
        self.assertEqual(scalar.parameters[1], 1)
        self.assertEqual(encode_cell(scalar, [17]), "[17]")
        self.assertEqual(encode_cell(scalar, [None]), "[null]")
        empty = t.fixed_shape_tensor(t.float64(), [(1 << 63) - 1, (1 << 63) - 1, 0])
        self.assertEqual(empty.parameters[1], 0)
        self.assertEqual(encode_cell(empty, []), "[]")
        self.assertIsNone(encode_cell(empty, None))
        self.assertEqual(python_rows([{"t": []}, {"t": None}], (("t", empty),)),
                         [{"t": []}, {"t": None}])
        self.assertEqual(t.fixed_shape_tensor(t.uint64(), [2**32 - 1]).parameters[1], 2**32 - 1)

    def test_fixed_tensor_all_element_types_and_nullability(self):
        for constructor in [t.boolean, t.int8, t.int16, t.int32, t.int64, t.uint8, t.uint16,
                            t.uint32, t.uint64, t.float32, t.float64]:
            for component in [False, True]:
                for outer in [False, True]:
                    dtype = t.fixed_shape_tensor(constructor(nullable=component), [2], nullable=outer)
                    self.assertEqual(dtype.parameters[0].nullable, component)
                    self.assertEqual(dtype.nullable, outer)
        for item in [t.string(), t.binary(), t.list_(t.int32()), t.decimal128(8, 2)]:
            with self.subTest(item=item), self.assertRaisesRegex(ValueError, "tensor elements"):
                t.fixed_shape_tensor(item, [1])
        with self.assertRaisesRegex(ValueError, "scalar parameter"):
            t.scalar([1], t.fixed_shape_tensor(t.int32(), [1]))

    def test_fixed_tensor_invalid_metadata_is_never_silently_normalized(self):
        for fields in [
            {"shape": None}, {"shape": [-1]}, {"shape": [True]}, {"shape": [1.0]},
            {"shape": [1 << 63, 0]}, {"shape": [1 << 32]}, {"shape": [65536, 65536]},
            {"shape": [2], "dim_names": None}, {"shape": [2], "dim_names": []},
            {"shape": [2], "dim_names": [1]}, {"shape": [2], "permutation": []},
            {"shape": [2], "permutation": [1]}, {"shape": [2], "permutation": [True]},
            {"shape": [2, 3], "permutation": [0, 0]}, {"shape": [], "unknown": 7},
            {"shape": [], "permutation": None},
        ]:
            with self.subTest(fields=fields), self.assertRaises((ValueError, TypeError)):
                normalize_schema({"t": {"type": "fixed_shape_tensor", "item": "int32", **fields}})
        for raw in ['{}', '{"shape":[],"shape":[]}', '{"shape":[2],"dim_names":null}']:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode_metadata(raw)
        invalid = ResultType("fixed_shape_tensor", True, (t.int32(), 2, (("shape", (3,)),)))
        with self.assertRaisesRegex(ValueError, "shape product"):
            normalize_schema({"t": invalid})

    def test_fixed_tensor_transport_rejects_changed_width_identity_and_noncanonical_bytes(self):
        dtype = t.fixed_shape_tensor(t.float64(), [2, 3], permutation=[1, 0])
        for change in ["shape", "id", "bytes"]:
            wire = _wire_type(dtype)
            if change == "shape":
                wire["Extension"]["storage_dtype"]["FixedSizeList"][1] = 5
            elif change == "id":
                wire["Extension"]["id"] = "unknown.tensor.v1"
            else:
                wire["Extension"]["metadata"] = list(b'{ "permutation": [1,0], "shape": [2,3] }')
            with self.subTest(change=change), self.assertRaises(ShardLoomProtocolError):
                schema_fields(json.dumps({"Struct": [{"names": ["t"], "dtypes": [wire]}, False]}),
                              "vortex.dtype.serde.v1")
        self.assertEqual(metadata_json(dtype.parameters[2]), '{"permutation":[1,0],"shape":[2,3]}')
        with self.assertRaisesRegex(ValueError, "width"):
            encode_cell(dtype, [1, 2])


if __name__ == "__main__":
    unittest.main()
