"""Actual native metadata controls explicit Python result conversions."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal, localcontext
import importlib.util
import json
import sys
import unittest
import uuid
from pathlib import Path

if not sys.flags.isolated:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom._result_schema import arrow_table, python_rows, schema_fields
from shardloom.errors import ShardLoomProtocolError
from shardloom.models import OutputEnvelope
from shardloom.query import Result


FORMAT = "vortex.dtype.serde.v1"


def schema(fields):
    return {"Struct": [{"names": list(fields), "dtypes": list(fields.values())}, False]}


def fields(declarations):
    return schema_fields(json.dumps(schema(declarations)), FORMAT)


class NativeResultSchemaTests(unittest.TestCase):
    def test_uuid_metadata_and_canonical_wire_values_preserve_every_bit(self):
        declaration = {"Extension": {"id": "vortex.uuid", "metadata": [],
                                     "storage_dtype": {"FixedSizeList": [{"Primitive": ["u8", False]}, 16, True]}}}
        native_fields = fields({"id": declaration})
        values = [None, "00000000-0000-0000-0000-000000000000",
                  "ffffffff-ffff-ffff-ffff-ffffffffffff", "01234567-89ab-cdef-fedc-ba9876543210"]
        self.assertEqual(python_rows([{"id": value} for value in values], native_fields),
                         [{"id": None if value is None else uuid.UUID(value)} for value in values])
        for bad in (1, b"\0" * 16, "0123456789abcdeffedcba9876543210",
                    "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", "{00000000-0000-0000-0000-000000000000}"):
            with self.assertRaises(ShardLoomProtocolError):
                python_rows([{"id": bad}], native_fields)
        for mutate in (lambda wire: wire.update(metadata=[4]),
                       lambda wire: wire.update(metadata=""),
                       lambda wire: wire["storage_dtype"]["FixedSizeList"].__setitem__(1, 15),
                       lambda wire: wire["storage_dtype"]["FixedSizeList"][0]["Primitive"].__setitem__(1, True)):
            changed = json.loads(json.dumps(declaration))
            mutate(changed["Extension"])
            with self.assertRaises(ShardLoomProtocolError):
                fields({"id": changed})

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "optional PyArrow conversion dependency")
    def test_uuid_arrow_diagnostic_conversion_keeps_type_including_empty_and_nested(self):
        pa = __import__("pyarrow")
        declaration = {"Extension": {"id": "vortex.uuid", "metadata": [],
                                     "storage_dtype": {"FixedSizeList": [{"Primitive": ["u8", False]}, 16, True]}}}
        nested = {"Struct": [{"names": ["child"], "dtypes": [declaration]}, True]}
        native_fields = fields({"id": declaration, "items": {"List": [declaration, True]},
            "pair": {"FixedSizeList": [declaration, 2, True]},
            "empty": {"FixedSizeList": [declaration, 0, True]}, "nested": nested})
        value = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        for rows in ([], [
                {"id": value, "items": [None, value], "pair": [value, None], "empty": [], "nested": {"child": value}},
                {"id": None, "items": None, "pair": None, "empty": None, "nested": None},
                {"id": None, "items": [], "pair": [None, None], "empty": [], "nested": {"child": None}}]):
            table = arrow_table(rows, native_fields, pa)
            table.validate(full=True)
            self.assertEqual(table.schema.field("id").type, pa.uuid())
            self.assertEqual(table.schema.field("items").type.value_type, pa.uuid())
            self.assertEqual(table.schema.field("pair").type.value_type, pa.uuid())
            self.assertEqual(table.schema.field("empty").type.list_size, 0)
            self.assertEqual(table.schema.field("nested").type.field("child").type, pa.uuid())
            self.assertEqual(table.to_pylist(), python_rows(rows, native_fields))

    def test_variant_diagnostic_scalar_rows_preserve_legacy_melt_without_reconstruction(self):
        # The existing native mixed-MELT producer emits these complete scalar
        # rows with a Variant result dtype; keep Result.python_objects usable.
        expected = [
            {"account_key": 1, "kind": "amount", "cell": (1 << 64) - 1},
            {"account_key": 1, "kind": "label", "cell": "π"},
            {"account_key": None, "kind": "amount", "cell": 0},
            {"account_key": None, "kind": "label", "cell": None},
            {"account_key": 3, "kind": "amount", "cell": 42},
            {"account_key": 3, "kind": "label", "cell": "end"},
        ]
        declarations = {"account_key": {"Primitive": ["u64", True]},
                        "kind": {"Utf8": False}, "cell": {"Variant": True}}
        report = Result(None, "collect", OutputEnvelope.from_field_mapping({
            "result_values_json": json.dumps(expected),
            "result_schema_json": json.dumps(schema(declarations)),
            "result_schema_format": FORMAT,
        }))
        self.assertEqual(report.python_objects, tuple(expected))
        native_fields = fields({"payload": {"Variant": True}})
        self.assertEqual(native_fields[0][1].name, "variant")
        for value in (7, True, 2.5, "text", None):
            self.assertEqual(python_rows([{"payload": value}], native_fields), [{"payload": value}])
        for value in ({"key": "value"}, [7], {"metadata": "010000", "value": "00"}):
            with self.assertRaisesRegex(ShardLoomProtocolError, "native Arrow export"):
                python_rows([{"payload": value}], native_fields)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "optional PyArrow conversion dependency")
    def test_variant_arrow_requires_encoded_values_and_keeps_empty_identity(self):
        pa = __import__("pyarrow")
        native_fields = fields({"payload": {"Variant": True}})
        table = arrow_table([], native_fields, pa)
        self.assertEqual(table.schema.field("payload").metadata,
                         {b"ARROW:extension:name": b"arrow.parquet.variant", b"ARROW:extension:metadata": b""})
        self.assertFalse(table.schema.field("payload").type.field("metadata").nullable)
        with self.assertRaisesRegex(ShardLoomProtocolError, "native Arrow export"):
            arrow_table([{"payload": 7}], native_fields, pa)
        nested = fields({"nested": schema({"cell": {"Variant": True}})})
        rows = [{"nested": {"cell": 7}}]
        self.assertEqual(python_rows(rows, nested), rows)
        with self.assertRaisesRegex(ShardLoomProtocolError, "native Arrow export"):
            arrow_table(rows, nested, pa)

    def test_empty_report_retains_exact_field_names_order_types_and_nullability(self):
        declarations = {"n,λ": {"Primitive": ["i64", True]},
                        "t\"ext": {"Utf8": False}, "flag": {"Bool": True}}
        for payload in [{"result_jsonl": ""}, {"result_values_json": "[]"}]:
            report = Result(None, "collect", OutputEnvelope.from_field_mapping({
                **payload, "result_schema_json": json.dumps(schema(declarations)),
                "result_schema_format": FORMAT,
            }))
            self.assertEqual(report.result_columns, tuple(declarations))
            self.assertEqual([(dtype.name, dtype.nullable) for _, dtype in report.result_schema],
                             [("int64", True), ("utf8", False), ("bool", True)])
            self.assertEqual(report.python_objects, ())

    def test_scalar_widths_and_nulls_are_preserved_without_float_coercion(self):
        declarations = {"low": {"Primitive": ["i64", True]}, "high": {"Primitive": ["u64", False]},
                        "f": {"Primitive": ["f64", True]}, "flag": {"Bool": False}, "text": {"Utf8": True}}
        expected = [{"low": -(1 << 63), "high": (1 << 64) - 1, "f": 1.25, "flag": True, "text": "港\nλ"},
                    {"low": None, "high": 0, "f": None, "flag": False, "text": None}]
        self.assertEqual(python_rows(expected, fields(declarations)), expected)

    def test_nested_binary_decimal_and_temporal_values_restore_their_logical_types(self):
        decimal = {"Decimal": [{"precision": 38, "scale": 6}, True]}
        date = {"Extension": {"id": "vortex.date", "metadata": [4], "storage_dtype": {"Primitive": ["i32", True]}}}
        timestamp = {"Extension": {"id": "vortex.timestamp", "metadata": [1, 0, 0], "storage_dtype": {"Primitive": ["i64", True]}}}
        declarations = {
            "amount": decimal, "blob": {"Binary": True}, "day": date, "time": timestamp,
            "nested": {"List": [{"Struct": [{"names": ["b", "n"],
                        "dtypes": [{"Binary": True}, decimal]}, True]}, True]},
            "fixed": {"FixedSizeList": [{"Primitive": ["i64", True]}, 2, False]},
        }
        coefficient = "99999999999999999999999999999999999999"
        raw = [{"amount": f"decimal128(38,6):{coefficient}", "blob": "00ffab", "day": -1, "time": -1,
                "nested": [None, {"b": "", "n": "decimal128(38,6):-1000001"}], "fixed": [1, None]}]
        with localcontext() as context:
            context.prec = 3
            actual = python_rows(raw, fields(declarations))
        self.assertEqual(actual, [{
            "amount": Decimal("99999999999999999999999999999999.999999"), "blob": b"\x00\xff\xab",
            "day": dt.date(1969, 12, 31), "time": dt.datetime(1969, 12, 31, 23, 59, 59, 999999),
            "nested": [None, {"b": b"", "n": Decimal("-1.000001")}], "fixed": [1, None],
        }])

    def test_malformed_schema_is_a_protocol_error(self):
        invalid = [None, {}, {"Extension": []}, {"Extension": {"id": "unknown", "storage_dtype": "Null"}},
                   {"Primitive": ["invalid", False]}, {"Utf8": "false"},
                   schema({"n": {"Decimal": [{"precision": 39, "scale": 0}, True]}}),
                   {"Struct": [{"names": ["n", "n"], "dtypes": ["Null", "Null"]}, False]},
                   {"Struct": [{"names": ["n"], "dtypes": []}, False]},
                   schema({"n": {"FixedSizeList": ["Null", -1, False]}})]
        nested = "Null"
        for _ in range(66):
            nested = {"List": [nested, True]}
        invalid.append(schema({"n": nested}))
        for wire in invalid:
            with self.subTest(wire=wire), self.assertRaises(ShardLoomProtocolError):
                schema_fields(json.dumps(wire), FORMAT)
        with self.assertRaises(ShardLoomProtocolError):
            schema_fields(json.dumps(schema({"n": "Null"})), "unknown")

    def test_value_schema_mismatch_never_silently_coerces(self):
        cases = [({"Primitive": ["u64", True]}, -1), ({"Primitive": ["i64", False]}, 1 << 63),
                 ({"Primitive": ["i32", False]}, True), ({"Primitive": ["i64", False]}, None),
                 ({"Primitive": ["f64", False]}, float("inf")), ({"Bool": False}, 1),
                 ({"Utf8": False}, 2), ({"Binary": False}, "ff aa"),
                 ({"Decimal": [{"precision": 3, "scale": 2}, False]}, "decimal128(3,2):1000"),
                 ({"Decimal": [{"precision": 3, "scale": 2}, False]}, "decimal128(3,1):1"),
                 ({"FixedSizeList": ["Null", 2, False]}, [None]), ("Null", 1),
                 (schema({"x": "Null"}), {"other": None}), (schema({"x": "Null"}), 7)]
        for dtype, value in cases:
            with self.subTest(dtype=dtype, value=value), self.assertRaises(ShardLoomProtocolError):
                python_rows([{"n": value}], fields({"n": dtype}))
        with self.assertRaises(ShardLoomProtocolError):
            python_rows([{"extra": None}], fields({"n": "Null"}))

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "optional PyArrow conversion dependency")
    def test_arrow_keeps_native_widths_nested_fields_empty_schema_and_values(self):
        import pyarrow as pa
        declarations = {"n,λ": {"Primitive": ["u64", True]}, "blob": {"Binary": True},
                        "amount": {"Decimal": [{"precision": 38, "scale": 6}, True]},
                        "items": {"List": [{"Primitive": ["i64", True]}, True]},
                        "flag": {"Bool": False}}
        native_fields = fields(declarations)
        expected_schema = pa.schema([pa.field("n,λ", pa.uint64()), pa.field("blob", pa.binary()),
                                     pa.field("amount", pa.decimal128(38, 6)),
                                     pa.field("items", pa.list_(pa.field("item", pa.int64()))),
                                     pa.field("flag", pa.bool_(), nullable=False)])
        empty = arrow_table([], native_fields, pa)
        self.assertEqual(empty.schema, expected_schema)
        self.assertEqual(empty.num_rows, 0)
        raw = [{"n,λ": (1 << 64) - 1, "blob": "ff", "amount": "decimal128(38,6):-1000001",
                "items": [None, -(1 << 63)], "flag": True}]
        table = arrow_table(raw, native_fields, pa)
        self.assertEqual(table.schema, expected_schema)
        self.assertEqual(table.to_pylist(), [{"n,λ": (1 << 64) - 1, "blob": b"\xff", "amount": Decimal("-1.000001"),
                                           "items": [None, -(1 << 63)], "flag": True}])


if __name__ == "__main__":
    unittest.main()
