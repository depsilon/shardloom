"""Variable tensor schemas and row-shape relations without native execution."""

import copy
import json
import unittest

from shardloom import types as t
from shardloom._input_schema import normalize_schema, _wire_type, encode_cell
from shardloom._result_schema import ResultType, schema_fields, python_rows
from shardloom._variable_tensor import decode_metadata, metadata_json
from shardloom.errors import ShardLoomProtocolError


def _decode(wire):
    return schema_fields(json.dumps({"Struct": [
        {"names": ["tensor"], "dtypes": [wire]}, False]}), "vortex.dtype.serde.v1")[0][1]


class VariableTensorSchemaTests(unittest.TestCase):
    def test_variable_tensor_metadata_and_schema_are_immutable_and_roundtrip(self):
        names, permutation, uniform = ["Ω", "", "Ω"], [2, 0, 1], [None, 2, None]
        dtype = t.variable_shape_tensor(t.float32(nullable=False), 3, dim_names=names,
                                        permutation=permutation, uniform_shape=uniform)
        storage, metadata = dtype.parameters
        self.assertEqual(dtype.label, "variable_shape_tensor<float32,3>")
        self.assertEqual(dict(metadata), {"dim_names": tuple(names), "permutation": tuple(permutation),
                                          "uniform_shape": tuple(uniform)})
        self.assertEqual(storage.name, "struct")
        self.assertEqual(tuple(name for name, _ in storage.parameters), ("data", "shape"))
        self.assertTrue(storage.nullable)
        for _, child in storage.parameters:
            self.assertFalse(child.nullable)
            self.assertFalse(child.parameters[0].nullable)
        self.assertEqual(_decode(_wire_type(dtype)), dtype)
        self.assertEqual(normalize_schema({"tensor": dtype}), (("tensor", dtype),))
        names[0], permutation[0], uniform[0] = "changed", 0, 9
        self.assertEqual(dict(metadata)["dim_names"][0], "Ω")
        self.assertEqual(dict(metadata)["permutation"], (2, 0, 1))
        self.assertIsNone(dict(metadata)["uniform_shape"][0])

    def test_variable_tensor_storage_retains_all_nullability_flags(self):
        for constructor in (t.boolean, t.int8, t.int16, t.int32, t.int64, t.uint8,
                            t.uint16, t.uint32, t.uint64, t.float32, t.float64):
            for flags in range(32):
                storage = t.struct({
                    "data": t.list_(constructor(nullable=bool(flags & 16)), nullable=bool(flags & 2)),
                    "shape": t.fixed_size_list(t.int32(nullable=bool(flags & 4)), 2, nullable=bool(flags & 8)),
                }, nullable=bool(flags & 1))
                dtype = ResultType("variable_shape_tensor", storage.nullable, (storage, ()))
                self.assertEqual(normalize_schema({"tensor": dtype}), (("tensor", dtype),))
                self.assertEqual(_decode(_wire_type(dtype)), dtype)
        for item in (t.string(), t.binary(), t.list_(t.int32()), t.decimal128(8, 2)):
            with self.subTest(item=item), self.assertRaises(ValueError):
                t.variable_shape_tensor(item, 1)

    def test_variable_tensor_scalar_zero_product_and_null_are_distinct(self):
        scalar = t.variable_shape_tensor(t.int32(), 0)
        annotated = t.variable_shape_tensor(t.int32(), 0, dim_names=[], permutation=[], uniform_shape=[])
        self.assertNotEqual(scalar, annotated)
        self.assertEqual(metadata_json(annotated.parameters[1]), '{"dim_names":[],"permutation":[],"uniform_shape":[]}')
        for data in ([17], [None]):
            value = {"data": data, "shape": []}
            self.assertEqual(json.loads(encode_cell(scalar, value)), value)
        with self.assertRaises(ValueError):
            encode_cell(scalar, {"data": [], "shape": []})
        empty = t.variable_shape_tensor(t.float64(), 4)
        value = {"data": [], "shape": [(1 << 31) - 1] * 3 + [0]}
        self.assertEqual(json.loads(encode_cell(empty, value)), value)
        self.assertIsNone(encode_cell(empty, None))
        self.assertEqual(python_rows([{"tensor": value}, {"tensor": None}], (("tensor", empty),)),
                         [{"tensor": value}, {"tensor": None}])

    def test_variable_tensor_refuses_invalid_metadata_and_declaration_mixtures(self):
        for rank in (-1, True, 2.0, 1 << 32):
            with self.subTest(rank=rank), self.assertRaises((ValueError, TypeError)):
                t.variable_shape_tensor(t.int32(), rank)
        for options in ({"dim_names": None}, {"dim_names": []}, {"dim_names": [1]},
                        {"permutation": None}, {"permutation": [True]}, {"permutation": [1]},
                        {"uniform_shape": None}, {"uniform_shape": [-1]},
                        {"uniform_shape": [1 << 31]}, {"uniform_shape": [False]},
                        {"uniform_shape": [1.0]}, {"uniform_shape": []}, {"shape": [1]}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                normalize_schema({"tensor": {"type": "variable_shape_tensor", "item": "int32", "ndim": 1, **options}})
        for raw in ('[]', '{"permutation":[0],"permutation":[0]}', '{"permutations":[0]}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode_metadata(raw, 1)
        dtype = t.variable_shape_tensor(t.int32(), 1)
        storage = dtype.parameters[0]
        for declaration in (
            {"storage": storage, "metadata": {}, "item": "int32", "ndim": 1},
            {"storage": storage, "metadata": {}, "uniform_shape": [1]},
            {"storage": storage, "metadata": {}, "nullable": False},
            {"item": "int32", "ndim": 1, "metadata": {}},
        ):
            with self.subTest(declaration=declaration), self.assertRaises((ValueError, TypeError)):
                normalize_schema({"tensor": {"type": "variable_shape_tensor", **declaration}})

    def test_variable_tensor_visible_values_require_complete_consistent_shapes(self):
        dtype = t.variable_shape_tensor(t.int32(), 2, uniform_shape=[None, 2])
        good = {"data": [1, None, 3, 4], "shape": [2, 2]}
        self.assertEqual(json.loads(encode_cell(dtype, good)), good)
        for value in ([1, 2], {}, {"data": [], "shape": [], "extra": 1},
                      {"data": None, "shape": [0, 2]}, {"data": [], "shape": None},
                      {"data": [], "shape": [0, None]}, {"data": [], "shape": [0, -1]},
                      {"data": [], "shape": [0, 3]}, {"data": [], "shape": [0, True]},
                      {"data": [], "shape": [0, 1 << 31]}, {"data": [1], "shape": [1, 2]},
                      {"data": [], "shape": [0]}, {"data": [], "shape": [0, 2, 0]}):
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                encode_cell(dtype, value)
            with self.subTest(protocol=value), self.assertRaises(ShardLoomProtocolError):
                python_rows([{"tensor": value}], (("tensor", dtype),))

    def test_variable_tensor_transport_refuses_changed_schema_and_noncanonical_bytes(self):
        original = _wire_type(t.variable_shape_tensor(t.float64(), 2, uniform_shape=[2, None]))
        for change in ("order", "rank", "dimension", "data", "id", "bytes", "byte_bool"):
            wire = copy.deepcopy(original)
            extension = wire["Extension"]
            fields = extension["storage_dtype"]["Struct"][0]
            if change == "order":
                fields["names"].reverse()
            elif change == "rank":
                fields["dtypes"][1]["FixedSizeList"][1] = 1
            elif change == "dimension":
                fields["dtypes"][1]["FixedSizeList"][0]["Primitive"][0] = "i64"
            elif change == "data":
                fields["dtypes"][0] = {"FixedSizeList": [{"Primitive": ["f64", True]}, 2, False]}
            elif change == "id":
                extension["id"] = "unknown.tensor.v1"
            elif change == "bytes":
                extension["metadata"] = list(b'{ "uniform_shape": [2,null] }')
            else:
                extension["metadata"][0] = True
            with self.subTest(change=change), self.assertRaises(ShardLoomProtocolError):
                _decode(wire)

    def test_variable_tensor_simple_and_explicit_declarations_share_schema_limits(self):
        for declaration in ({"type": "variable_shape_tensor", "item": "int32", "ndim": 1},
                            t.variable_shape_tensor(t.int32(), 1)):
            nested = declaration
            for _ in range(21):
                nested = {"type": "list", "item": nested}
            normalize_schema({"tensor": nested})
            with self.assertRaisesRegex(ValueError, "depth 24"):
                normalize_schema({"tensor": {"type": "list", "item": nested}})
        fields = {f"t{index}": {"type": "variable_shape_tensor", "item": "int32", "ndim": 1}
                  for index in range(1024)}
        with self.assertRaisesRegex(ValueError, "4096 nodes"):
            normalize_schema({"row": {"type": "struct", "fields": fields}})
        with self.assertRaises((ValueError, TypeError)):
            normalize_schema({"tensor": ResultType("variable_shape_tensor", True, (object(), ()))})


if __name__ == "__main__":
    unittest.main()
