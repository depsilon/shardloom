"""Sparse and quantized schemas, exact wire metadata and typed row contracts."""
import copy
import json
import math
import unittest

from shardloom import types as t
from shardloom._input_schema import normalize_schema, _wire_type, encode_cell
from shardloom._result_schema import ResultType, schema_fields, python_rows
from shardloom import _vector
from shardloom.errors import ShardLoomProtocolError


def decode(wire):
    return schema_fields(json.dumps({"Struct": [{"names": ["v"], "dtypes": [wire]}, False]}),
                         "vortex.dtype.serde.v1")[0][1]


class VectorRepresentationSchemaTests(unittest.TestCase):
    def test_vector_constructors_keep_identity_presence_and_declared_nullability(self):
        identity = {"space_id": "未知 Ω", "model_version": None}
        sparse = t.sparse_vector(t.float16(nullable=False), index=t.uint64(), metadata=identity)
        identity["space_id"] = "changed"
        self.assertEqual(dict(sparse.parameters[1]), {"version": 1, "index_policy": "preserve",
            "space_id": "未知 Ω", "model_version": None})
        storage = dict(sparse.parameters[0].parameters)
        self.assertFalse(storage["size"].nullable)
        self.assertFalse(storage["indices"].nullable)
        self.assertTrue(storage["indices"].parameters[0].nullable)
        self.assertFalse(storage["values"].parameters[0].nullable)
        quantized = t.quantized_vector(t.int8(), scale=t.float16(nullable=False), block_size=2)
        storage = dict(quantized.parameters[0].parameters)
        self.assertTrue(storage["codes"].parameters[0].nullable)
        self.assertFalse(storage["scale"].parameters[0].nullable)
        self.assertFalse(storage["zero_point"].parameters[0].nullable)
        for dtype in (sparse, quantized):
            self.assertEqual(decode(_wire_type(dtype)), dtype)
            self.assertEqual(normalize_schema({"v": dtype}), (("v", dtype),))

    def test_vector_explicit_storage_preserves_all_widths_and_nullability_flags(self):
        for integer in sorted(_vector.INTEGERS):
            for floating in sorted(_vector.FLOATS):
                for kind, blocked in (("sparse_vector", False), ("quantized_vector", False), ("quantized_vector", True)):
                    for flags in range(128):
                        nullable = lambda bit: bool(flags & (1 << bit))
                        index = {"type": integer, "nullable": nullable(1)}
                        real = {"type": floating, "nullable": nullable(2)}
                        if kind == "sparse_vector":
                            storage = t.struct({"size": t.uint64(nullable=nullable(3)),
                                "indices": t.list_(index, nullable=nullable(4)),
                                "values": t.list_(real, nullable=nullable(5))}, nullable=nullable(0))
                            metadata = {"version": 1, "index_policy": "preserve"}
                        else:
                            zero = {"type": integer, "nullable": nullable(6)}
                            storage = t.struct({"codes": t.list_(index, nullable=nullable(3)),
                                "scale": t.list_(real, nullable=nullable(4)) if blocked else real,
                                "zero_point": t.list_(zero, nullable=nullable(5)) if blocked else zero}, nullable=nullable(0))
                            metadata = {"version": 1, "parameter_mode": "blocked" if blocked else "per_vector"}
                            if blocked:
                                metadata["block_size"] = 2
                        dtype = normalize_schema({"v": {"type": kind, "storage": storage,
                            "metadata": metadata, "nullable": storage.nullable}})[0][1]
                        self.assertEqual(dtype.parameters[0], storage)
                        self.assertEqual(decode(_wire_type(dtype)), dtype)

    def test_sparse_values_preserve_duplicate_order_wide_indices_and_signed_zero(self):
        dtype = t.sparse_vector(t.float16(), index=t.uint64())
        value = {"size": (1 << 64) - 1, "indices": [(1 << 64) - 2, 0, (1 << 64) - 2],
                 "values": [1.5, -0.0, None]}
        restored = json.loads(encode_cell(dtype, value))
        self.assertEqual(restored, value)
        self.assertEqual(math.copysign(1, restored["values"][1]), -1)
        self.assertEqual(python_rows([{"v": value}, {"v": None}], (("v", dtype),)), [{"v": value}, {"v": None}])
        self.assertEqual(json.loads(encode_cell(dtype, {"size": 0, "indices": [], "values": []})),
                         {"size": 0, "indices": [], "values": []})
        for bad in ({"size": 2, "indices": [2], "values": [1]}, {"size": 2, "indices": [-1], "values": [1]},
                    {"size": 2, "indices": [None], "values": [1]}, {"size": 2, "indices": [False], "values": [1]},
                    {"size": 2, "indices": [0], "values": []}, {"size": None, "indices": [], "values": []}):
            with self.subTest(bad=bad), self.assertRaises((TypeError, ValueError)):
                encode_cell(dtype, bad)
        ordered = t.sparse_vector("float32", index_policy="strictly_increasing", uniform_size=8)
        encode_cell(ordered, {"size": 8, "indices": [0, 3, 7], "values": [None, -0.0, 1.0]})
        for indices in ([0, 0, 7], [0, 7, 3]):
            with self.assertRaisesRegex(ValueError, "strictly increasing"):
                encode_cell(ordered, {"size": 8, "indices": indices, "values": [1, 2, 3]})

    def test_quantized_empty_partial_blocks_and_required_parameters(self):
        scalar = t.quantized_vector("uint64", scale="float16")
        empty = {"codes": [], "scale": 0.5, "zero_point": (1 << 64) - 1}
        self.assertEqual(json.loads(encode_cell(scalar, empty)), empty)
        blocked = t.quantized_vector("int8", scale="float32", block_size=2)
        good = {"codes": [-128, None, 127], "scale": [0.5, 2.0], "zero_point": [-128, 127]}
        self.assertEqual(json.loads(encode_cell(blocked, good)), good)
        encode_cell(blocked, {"codes": [], "scale": [], "zero_point": []})
        for scale in (None, 0, -0.0, -1, float("nan"), float("inf"), True):
            with self.subTest(scale=scale), self.assertRaises((TypeError, ValueError)):
                encode_cell(scalar, {**empty, "scale": scale})
        for bad in ({"codes": [], "scale": [0.5], "zero_point": [0]},
                    {"codes": [1], "scale": [None], "zero_point": [0]},
                    {"codes": [1], "scale": [0.5], "zero_point": [None]},
                    {"codes": [128], "scale": [0.5], "zero_point": [0]},
                    {"codes": [1], "scale": [0.5], "zero_point": [128]}):
            with self.subTest(bad=bad), self.assertRaises((TypeError, ValueError)):
                encode_cell(blocked, bad)

    def test_vector_metadata_and_declaration_errors_are_explicit(self):
        for kind, raw in (("sparse_vector", '{}'), ("sparse_vector", '{"version":1,"index_policy":"preserve","version":1}'),
            ("sparse_vector", '{"version":1,"index_policy":"preserve","uniform_size":null}'),
            ("sparse_vector", '{"version":true,"index_policy":"preserve"}'),
            ("quantized_vector", '{"version":1,"parameter_mode":"blocked","block_size":0}'),
            ("quantized_vector", '{"version":1,"parameter_mode":"per_vector","block_size":2}')):
            with self.subTest(raw=raw), self.assertRaises((TypeError, ValueError)):
                _vector.decode_metadata(kind, raw)
        for item in ("int32", "binary", t.list_("float32")):
            with self.assertRaises(ValueError):
                t.sparse_vector(item)
        for code in ("float16", "bool", t.list_("int8")):
            with self.assertRaises(ValueError):
                t.quantized_vector(code)
        for block in (0, -1, True, 2.0, 1 << 64):
            with self.assertRaises(ValueError):
                t.quantized_vector("int8", block_size=block)
        original = t.sparse_vector("float16")
        for extra in ({"item": "float16"}, {"uniform_size": 3}, {"nullable": False}):
            with self.assertRaises((TypeError, ValueError)):
                normalize_schema({"v": {"type": "sparse_vector", "storage": original.parameters[0],
                    "metadata": dict(original.parameters[1]), **extra}})

    def test_vector_wire_refuses_metadata_width_order_and_identity_corruption(self):
        for kind, dtype in (("sparse_vector", t.sparse_vector("float16")),
                            ("quantized_vector", t.quantized_vector("uint64", block_size=2))):
            for change in ("id", "bytes", "byte_bool", "order", "width", "metadata"):
                wire = copy.deepcopy(_wire_type(dtype))
                ext = wire["Extension"]
                storage = ext["storage_dtype"]["Struct"][0]
                if change == "id":
                    ext["id"] = "shardloom.vector.unknown"
                elif change == "bytes":
                    ext["metadata"].insert(0, 32)
                elif change == "byte_bool":
                    ext["metadata"][0] = True
                elif change == "order":
                    storage["names"].reverse()
                elif change == "width":
                    storage["dtypes"][0] = {"Primitive": ["i64", False]}
                else:
                    ext["metadata"] = list(b'{"version":1}')
                with self.subTest(kind=kind, change=change), self.assertRaises(ShardLoomProtocolError):
                    decode(wire)

    def test_vector_nested_limits_and_parent_nulls_match_explicit_declarations(self):
        for declaration in ({"type": "sparse_vector", "item": "float16"}, t.sparse_vector("float16"),
                            {"type": "quantized_vector", "code": "int8", "block_size": 2},
                            t.quantized_vector("int8", block_size=2)):
            nested = declaration
            for _ in range(21):
                nested = {"type": "list", "item": nested}
            normalize_schema({"v": nested})
            with self.assertRaisesRegex(ValueError, "depth 24"):
                normalize_schema({"v": {"type": "list", "item": nested}})
        dtype = t.list_(t.struct({"s": t.sparse_vector("float16"), "q": t.quantized_vector("int8")}))
        value = [None, {"s": None, "q": None}]
        self.assertEqual(json.loads(encode_cell(dtype, value)), value)
        with self.assertRaises((ValueError, TypeError)):
            normalize_schema({"v": ResultType("sparse_vector", True, (object(), ()))})


if __name__ == "__main__":
    unittest.main()
