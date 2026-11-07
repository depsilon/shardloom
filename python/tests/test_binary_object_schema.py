"""Inert managed-binary schema declarations, independent of native handles."""

import importlib.util
import json
import unittest

from shardloom._input_schema import normalize_schema, _wire_type, encode_cell
from shardloom._result_schema import ResultType, schema_fields, python_rows, arrow_table
from shardloom.errors import ShardLoomProtocolError


EXTENSION_ID = "shardloom.binary-object.v1"
METADATA = b'{"version":1}'
FORMAT = "vortex.dtype.serde.v1"


def result_schema(dtype):
    return {"Struct": [{"names": ["value"], "dtypes": [_wire_type(dtype)]}, False]}


class BinaryObjectSchemaTests(unittest.TestCase):
    def test_public_constructor_and_nested_schema_roundtrip(self):
        from shardloom import types as t

        nullable = t.binary_object()
        nonnullable = t.binary_object(nullable=False)
        self.assertEqual(nullable, ResultType("binary_object", True))
        self.assertEqual(nonnullable, ResultType("binary_object", False))
        declarations = {
            "items": t.list_(nullable),
            "record": t.struct({"payload": nonnullable, "optional": nullable}, nullable=False),
        }
        fields = normalize_schema(declarations)
        self.assertEqual(normalize_schema(dict(fields)), fields)
        wire = {"Struct": [{"names": [name for name, _ in fields],
                            "dtypes": [_wire_type(dtype) for _, dtype in fields]}, False]}
        self.assertEqual(schema_fields(json.dumps(wire), FORMAT), fields)

    def test_wire_uses_exact_binary_extension(self):
        for nullable in (True, False):
            dtype = normalize_schema({"value": {"type": "binary_object", "nullable": nullable}})[0][1]
            wire = _wire_type(dtype)
            self.assertEqual(wire, {"Extension": {"id": EXTENSION_ID,
                "metadata": list(METADATA), "storage_dtype": {"Binary": nullable}}})
            self.assertEqual(schema_fields(json.dumps(result_schema(dtype)), FORMAT), (("value", dtype),))

    def test_unknown_version_noncanonical_metadata_and_wrong_storage_are_rejected(self):
        invalid = [
            {"id": EXTENSION_ID, "metadata": list(b'{"version":2}'), "storage_dtype": {"Binary": True}},
            {"id": EXTENSION_ID, "metadata": list(b'{ "version":1}'), "storage_dtype": {"Binary": True}},
            {"id": EXTENSION_ID, "metadata": list(METADATA), "storage_dtype": {"Utf8": True}},
            {"id": EXTENSION_ID, "metadata": list(METADATA), "storage_dtype": {"Binary": True}, "extra": 1},
            {"id": EXTENSION_ID, "metadata": [float(byte) for byte in METADATA], "storage_dtype": {"Binary": True}},
        ]
        for payload in invalid:
            wire = {"Struct": [{"names": ["value"], "dtypes": [{"Extension": payload}]}, False]}
            with self.subTest(payload=payload), self.assertRaises(ShardLoomProtocolError):
                schema_fields(json.dumps(wire), FORMAT)

    def test_input_parameters_and_json_cell_paths_reject_binary_objects(self):
        from shardloom import types as t

        for declaration in (
            {"type": "binary_object", "item": "utf8"},
            {"type": "binary_object", "metadata": {"version": 1}},
            {"type": "binary_object", "size": 3},
        ):
            with self.subTest(declaration=declaration), self.assertRaisesRegex(ValueError, "unknown native input"):
                normalize_schema({"value": declaration})
        dtype = t.binary_object()
        with self.assertRaisesRegex(ValueError, "in-process native handles"):
            encode_cell(dtype, b"opaque")
        with self.assertRaisesRegex(ShardLoomProtocolError, "typed native row export"):
            python_rows([{"value": "descriptor"}], (("value", dtype),))
        self.assertEqual(encode_cell(dtype, None), None)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "optional PyArrow schema dependency")
    def test_arrow_nested_schema_preserves_extension_metadata(self):
        import pyarrow as pa
        from shardloom import types as t

        fields = normalize_schema({"payload": t.binary_object(nullable=False),
                                   "nested": t.list_(t.binary_object())})
        table = arrow_table([], fields, pa)
        payload = table.schema.field("payload")
        nested_item = table.schema.field("nested").type.value_field
        for field, nullable in ((payload, False), (nested_item, True)):
            self.assertTrue(pa.types.is_binary(field.type))
            self.assertEqual(field.nullable, nullable)
            self.assertEqual(field.metadata, {
                b"ARROW:extension:name": EXTENSION_ID.encode(),
                b"ARROW:extension:metadata": METADATA,
            })


if __name__ == "__main__":
    unittest.main()
