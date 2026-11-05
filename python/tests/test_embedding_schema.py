"""Inert embedding schema declarations, independent of native execution."""

import json
import unittest

from shardloom._embedding import decode_metadata, metadata_json
from shardloom._input_schema import normalize_schema, _wire_type, encode_cell
from shardloom._result_schema import ResultType, schema_fields, python_rows
from shardloom.errors import ShardLoomProtocolError


class EmbeddingSchemaTests(unittest.TestCase):
    def test_declaration_roundtrip_keeps_identity_shape_and_nullability(self):
        from shardloom import types as t
        metadata = {"space_id": "未知 Ω", "model_id": None, "normalization": "caller-defined"}
        fields = normalize_schema({"v": {"type": "embedding", "size": 65537,
            "item": {"type": "float32", "nullable": False}, "metadata": metadata}})
        dtype = fields[0][1]
        self.assertEqual(dtype.parameters[1], 65537)
        self.assertEqual(dict(dtype.parameters[2]), {**metadata, "version": 1})
        self.assertEqual(normalize_schema(dict(fields)), fields)
        wire = {"Struct": [{"names": ["v"], "dtypes": [_wire_type(dtype)]}, False]}
        self.assertEqual(schema_fields(json.dumps(wire), "vortex.dtype.serde.v1"), fields)
        self.assertFalse(dtype.parameters[0].nullable)
        self.assertTrue(dtype.nullable)
        self.assertEqual(t.embedding(t.float32(nullable=False), 65537, metadata=metadata), dtype)
        metadata["space_id"] = "changed caller mapping"
        self.assertEqual(dict(dtype.parameters[2])["space_id"], "未知 Ω")
        with self.assertRaisesRegex(ValueError, "scalar parameter"):
            t.scalar([1.0], t.embedding(t.float32(), 1))

    def test_finite_typed_cells_keep_values_and_missing_vectors_distinct(self):
        dtype = normalize_schema({"v": {"type": "embedding", "size": 2, "item": "float64"}})[0][1]
        self.assertEqual(encode_cell(dtype, [-0.0, None]), "[-0.0,null]")
        self.assertEqual(encode_cell(dtype, None), None)
        self.assertEqual(python_rows([{"v": [1.5, None]}], (("v", dtype),)), [{"v": [1.5, None]}])
        with self.assertRaises(ValueError):
            encode_cell(dtype, [1.0])
        with self.assertRaises(ValueError):
            encode_cell(dtype, [float("inf"), None])

    def test_unknown_or_malformed_identity_is_not_silently_discarded(self):
        invalid = [
            {"size": 0}, {"size": -1}, {"size": True}, {"size": 1 << 32},
            {"item": "float16"}, {"item": "int32"},
            {"metadata": {"version": True}}, {"metadata": {"version": 2}},
            {"metadata": {"space_id": ""}}, {"metadata": {"space_id": []}},
            {"metadata": {"unrecognized": "x"}},
        ]
        for override in invalid:
            with self.subTest(override=override), self.assertRaises((ValueError, TypeError)):
                normalize_schema({"v": {"type": "embedding", "size": 2, "item": "float32", **override}})
        for raw in ['{}', '{"version":1,"version":1}', '{"version":1,"space_id":"x","space_id":"y"}']:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode_metadata(raw)
        duplicate = ResultType("embedding", True, (ResultType("float32", True), 2,
            (("version", 1), ("version", 1))))
        with self.assertRaises(ValueError):
            normalize_schema({"v": duplicate})

    def test_result_decoder_rejects_foreign_versions_and_noncanonical_encoding(self):
        dtype = normalize_schema({"v": {"type": "embedding", "size": 2, "item": "float64"}})[0][1]
        for raw in ['{"version":2}', '{ "version": 1 }', '{"version":1,"version":1}']:
            wire = _wire_type(dtype)
            wire["Extension"]["metadata"] = list(raw.encode())
            with self.subTest(raw=raw), self.assertRaises(ShardLoomProtocolError):
                schema_fields(json.dumps({"Struct": [{"names": ["v"], "dtypes": [wire]}, False]}),
                              "vortex.dtype.serde.v1")
        self.assertEqual(metadata_json(dtype.parameters[2]), '{"version":1}')


if __name__ == "__main__":
    unittest.main()
