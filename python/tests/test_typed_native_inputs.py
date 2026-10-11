"""Exact typed input declarations and producer conversion before native execution."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, localcontext
import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from shardloom import ShardLoomClient, ShardLoomContext
from shardloom._batches import validate_inputs
from shardloom._result_schema import schema_fields


class TypedNativeInputTests(unittest.TestCase):
    def setUp(self):
        self.client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        self.context = ShardLoomContext(self.client)

    def declaration(self, frame):
        return json.loads(json.dumps(dict(frame.source.memory_input)))

    def cell(self, specification, value):
        frame = self.context.from_rows([{"v": value}], schema={"v": specification})
        cell = self.declaration(frame)["rows"][0][0]
        return json.loads(cell) if cell is not None else None

    def test_legacy_aliases_keep_exact_wire_and_resident_identity(self):
        expected = {"kind": "rows", "schema": [["n", "int64"], ["f", "float64"],
                    ["b", "bool"], ["s", "utf8"]], "rows": [["7", "1.25", "true", "λ"], [None] * 4]}
        frame = self.context.from_rows([{"n": 7, "f": 1.25, "b": True, "s": "λ"},
                                       dict.fromkeys(("n", "f", "b", "s"))],
                                      schema={"n": "INTEGER", "f": "double", "b": "boolean", "s": "str"})
        self.assertEqual(self.declaration(frame), expected)
        self.assertEqual(frame.source.uri, "memory://input/" + hashlib.sha256(
            json.dumps(expected, sort_keys=True, ensure_ascii=False).encode()).hexdigest())

    def test_exact_integer_endpoints_and_type_rejections(self):
        for signed in (True, False):
            for bits in (8, 16, 32, 64):
                token = ("int" if signed else "uint") + str(bits)
                specification = {"type": token, "nullable": False}
                lower, upper = (-(1 << (bits - 1)), (1 << (bits - 1)) - 1) if signed else (0, (1 << bits) - 1)
                for value in (lower, upper):
                    with self.subTest(token=token, value=value):
                        self.assertEqual(self.cell(specification, value), value)
                for value in (lower - 1, upper + 1, True, 1.0, None):
                    with self.subTest(token=token, value=value), self.assertRaises((ValueError, TypeError)):
                        self.cell(specification, value)

    def test_float32_requires_exact_finite_storage(self):
        for value in (-0.0, 1.25, 1 << 24, 2.0 ** -149):
            result = self.cell("float32", value)
            self.assertEqual(result, value)
            if value == 0:
                self.assertEqual(str(result), "-0.0")
        for value in (0.1, (1 << 24) + 1, float("nan"), float("inf"), True, 1 << 1024):
            with self.subTest(value=str(value)), self.assertRaises((ValueError, TypeError)):
                self.cell("float32", value)

    def test_decimal_conversion_is_exact_and_context_independent(self):
        with localcontext() as context:
            context.prec = 2
            self.assertEqual(self.cell("decimal128(38,2)", Decimal("123456789012345678901234567890123456.78")),
                             "decimal128(38,2):12345678901234567890123456789012345678")
            self.assertEqual(self.cell("decimal128(5,2)", Decimal("-12.3400")), "decimal128(5,2):-1234")
            self.assertEqual(self.cell("decimal128(1,0)", Decimal("0E+999999999")), "decimal128(1,0):0")
        for value in (Decimal("0.001"), Decimal("1000"), Decimal("1E+999999999"),
                      Decimal("NaN"), Decimal("Infinity"), 1.25, "1.25"):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                self.cell("decimal128(5,2)", value)

    def test_binary_and_full_temporal_storage(self):
        for value in (b"\x00\xffA", bytearray(b"\x00\xffA"), memoryview(b"\x00\xffA")):
            self.assertEqual(self.cell("binary", value), "00ff41")
        self.assertEqual(self.cell("date32", dt.date(1969, 12, 31)), -1)
        self.assertEqual(self.cell("timestamp_micros", dt.datetime(1969, 12, 31, 23, 59, 59, 999999)), -1)
        for token, bits in (("date32", 32), ("timestamp_micros", 64)):
            for value in (-(1 << (bits - 1)), (1 << (bits - 1)) - 1):
                self.assertEqual(self.cell(token, value), value)
        for token, value in (("binary", "ff"), ("date32", dt.datetime(2026, 1, 1)),
                             ("timestamp_micros", dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)),
                             ("date32", 1 << 31), ("timestamp_micros", 1 << 63)):
            with self.subTest(token=token), self.assertRaises((TypeError, ValueError)):
                self.cell(token, value)

    def test_nested_rows_and_batches_share_schema_and_values_without_eager_demand(self):
        schema = {"v": {"type": "struct", "fields": {
            "points": {"type": "list", "item": {"type": "fixed_size_list", "size": 2,
                                               "item": {"type": "int16", "nullable": False}}},
            "bytes": "binary", "amount": "decimal128(8,2)",
            "label": {"type": "utf8", "nullable": False}}}}
        rows = [{"v": {"points": [(1, -2), None, []], "bytes": b"\0", "amount": Decimal("1.23"), "label": "λ"}}]
        with self.assertRaisesRegex(ValueError, "wrong width"):
            self.context.from_rows(rows, schema=schema)
        rows[0]["v"]["points"][-1] = (32767, -32768)
        rows.append({"v": None})
        producer = mock.Mock(return_value=iter([rows]))
        with mock.patch.object(self.client, "public_workflow_run") as execute:
            resident = self.context.from_rows(rows, schema=schema)
            streamed = self.context.from_batches(producer, schema=schema, streaming=True)
            declared = self.declaration(resident)
            batch = streamed.source.batch_input
            self.assertEqual(self.declaration(streamed)["schema"], declared["schema"])
            self.assertEqual(batch.encode(rows), declared["rows"])
            validate_inputs({streamed.source.uri: {"memory_input": self.declaration(streamed)}},
                            {streamed.source.uri: batch})
            producer.assert_not_called()
            execute.assert_not_called()
        wire = declared["schema"][0][1]["native"]
        self.assertEqual(wire["encoding"], "vortex.dtype.serde.v1")
        fields = schema_fields(wire["dtype"], wire["encoding"])
        self.assertEqual([name for name, _ in fields], ["points", "bytes", "amount", "label"])
        self.assertFalse(fields[-1][1].nullable)
        original = self.declaration(resident)
        rows[0]["v"]["label"] = "changed"
        schema["v"]["fields"]["label"]["nullable"] = True
        self.assertEqual(self.declaration(resident), original)
        with self.assertRaisesRegex(ValueError, "nonnullable"):
            batch.encode([{"v": {"points": [], "bytes": b"", "amount": None, "label": None}}])

    def test_nullability_and_typed_empty_input_remain_declared(self):
        for streaming in (False, True):
            frame = self.context.from_batches([], schema={"v": {"type": "uint8", "nullable": False}},
                                              streaming=streaming)
            self.assertEqual(frame.source.batch_input.encode([]), [])
            with self.assertRaisesRegex(ValueError, "nonnullable"):
                frame.source.batch_input.encode([{"v": None}])
        frame = self.context.from_rows([], schema={"v": "binary"})
        self.assertEqual(self.declaration(frame)["rows"], [])
        self.assertEqual(json.loads(self.declaration(frame)["schema"][0][1]["native"]["dtype"]), {"Binary": True})
        self.assertEqual(self.cell({"type": "fixed_size_list", "item": "uint8", "size": 0}, []), [])

    def test_malformed_schema_and_nested_values_fail_before_execution(self):
        deep = "int32"
        for _ in range(26):
            deep = {"type": "list", "item": deep}
        for specification in ("float16", "decimal128(39,2)", "decimal128(2,3)",
                              {"type": "int32", "nullable": 1}, {"type": "int32", "extra": True},
                              {"type": "struct", "fields": {}}, {"type": "list"},
                              {"type": "fixed_size_list", "item": "int32", "size": True},
                              {"type": "fixed_size_list", "item": "int32", "size": 1 << 32}, deep):
            with self.subTest(specification=str(specification)[:100]), self.assertRaises((TypeError, ValueError)):
                self.context.from_rows([], schema={"v": specification})
        for specification, value in (({"type": "list", "item": "int32"}, "1,2"),
                                     ({"type": "struct", "fields": {"x": "int32"}}, {"x": 1, "y": 2}),
                                     ({"type": "list", "item": {"type": "bool", "nullable": False}}, [None])):
            with self.assertRaises((TypeError, ValueError)):
                self.cell(specification, value)

    def test_rich_values_require_explicit_schema_and_respect_frame_bytes(self):
        for value in (Decimal("1.2"), b"x", dt.date(2026, 1, 1), [1], {"x": 1}):
            with self.assertRaises(TypeError):
                self.context.from_rows([{"v": value}])
        frame = self.context.from_batches([], schema={"v": "binary"})
        with self.assertRaisesRegex(ValueError, "8 MiB"):
            frame.source.batch_input.encode([{"v": b"x" * (4 << 20)}])


if __name__ == "__main__":
    unittest.main()
