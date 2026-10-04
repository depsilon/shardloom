"""Typed unary literals are declared losslessly; execution is covered elsewhere."""

from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.query import (
    _normalize_pivot_policy_kwargs,
    _vortex_expression_scalar_payload,
)


class TypedUnaryDeclarations(unittest.TestCase):
    def test_exact_scalar_payloads_and_matching_declared_types(self) -> None:
        values = [
            (
                b"\x00\xff",
                "binary",
                {"type": "binary", "value": "00ff"},
            ),
            (
                bytearray(b"\x00\xff"),
                "binary",
                {"type": "binary", "value": "00ff"},
            ),
            (
                Decimal("1.2300"),
                "decimal128(20,2)",
                {"type": "decimal128(5,4)", "value": "1.2300"},
            ),
            (
                date(1969, 12, 31),
                "date32",
                {"type": "date32", "value": "1969-12-31"},
            ),
            (
                datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
                "timestamp_micros",
                {"type": "timestamp_micros", "value": "1970-01-01T00:00:00.000001Z"},
            ),
        ]
        for value, declared_dtype, expected in values:
            with self.subTest(value=value, declared_dtype=declared_dtype):
                self.assertEqual(
                    _vortex_expression_scalar_payload(
                        value, target_dtype=None
                    ),
                    expected,
                )
                self.assertEqual(
                    _vortex_expression_scalar_payload(
                        value, target_dtype=declared_dtype
                    ),
                    expected,
                )

    def test_decimal_precision_is_context_independent_and_invalid_values_fail_early(self) -> None:
        large = Decimal("12345678901234567890.123456789012345678")
        expected = {
            "type": "decimal128(38,18)",
            "value": "12345678901234567890.123456789012345678",
        }
        with localcontext() as context:
            context.prec = 2
            self.assertEqual(
                _vortex_expression_scalar_payload(large, target_dtype=None),
                expected,
            )
        for value in [
            Decimal("NaN"),
            Decimal("Infinity"),
            Decimal("1E+1000000"),
            Decimal("1E-1000000"),
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _vortex_expression_scalar_payload(value, target_dtype=None)

    def test_declared_typed_nulls_and_incompatible_domains(self) -> None:
        for dtype in ["binary", "decimal128(20,2)", "date32", "timestamp_micros"]:
            with self.subTest(dtype=dtype):
                self.assertEqual(
                    _vortex_expression_scalar_payload(None, target_dtype=dtype),
                    {"type": "null", "value": None},
                )
                self.assertIsNone(
                    _vortex_expression_scalar_payload(
                        None, target_dtype=dtype, allow_null=False
                    )
                )

        for value, dtype in [
            (1, "date32"),
            (date(1969, 12, 31), "timestamp_micros"),
            ("text", "binary"),
            (1.25, "decimal128(20,2)"),
        ]:
            with self.subTest(value=value, dtype=dtype):
                self.assertIsNone(
                    _vortex_expression_scalar_payload(value, target_dtype=dtype)
                )

    def test_existing_primitive_payload_mappings_are_unchanged(self) -> None:
        values = [
            (None, "null", {"type": "null", "value": None}),
            (True, "boolean", {"type": "boolean", "value": True}),
            (-7, "int64", {"type": "int64", "value": -7}),
            (7, "int64", {"type": "int64", "value": 7}),
            (1 << 63, "uint64", {"type": "uint64", "value": 1 << 63}),
            (1.5, "float64", {"type": "float64", "value": 1.5}),
            ("text", "utf8", {"type": "utf8", "value": "text"}),
        ]
        for value, dtype, expected in values:
            with self.subTest(value=value):
                self.assertEqual(
                    _vortex_expression_scalar_payload(value, target_dtype=None),
                    expected,
                )
                if value is not None:
                    declared_dtype = {
                        "boolean": "boolean",
                        "int64": "int64",
                        "uint64": "uint64",
                        "float64": "float64",
                        "utf8": "utf8",
                    }[dtype]
                    self.assertEqual(
                        _vortex_expression_scalar_payload(
                            value, target_dtype=declared_dtype
                        ),
                        expected,
                    )

    def test_pivot_fill_policy_encodes_typed_and_primitive_values(self) -> None:
        typed = [
            (b"\x00\xff", {"type": "binary", "value": "00ff"}),
            (bytearray(b"\x00\xff"), {"type": "binary", "value": "00ff"}),
            (
                Decimal("1.2300"),
                {"type": "decimal128(5,4)", "value": "1.2300"},
            ),
            (date(1969, 12, 31), {"type": "date32", "value": "1969-12-31"}),
            (
                datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
                {
                    "type": "timestamp_micros",
                    "value": "1970-01-01T00:00:00.000001Z",
                },
            ),
        ]
        for value, expected in typed:
            with self.subTest(value=value):
                self.assertEqual(
                    _normalize_pivot_policy_kwargs({"fill_value": value}),
                    {"fill_value": expected},
                )

        for value in [1, True, 1.5, "text"]:
            with self.subTest(value=value):
                self.assertEqual(
                    _normalize_pivot_policy_kwargs({"fill_value": value}),
                    {"fill_value": value},
                )

    def test_binary_and_decimal_replace_lower_to_typed_unary_payloads(self) -> None:
        context = sl.ShardLoomContext()
        source = context.read_vortex(
            "input.vortex",
            schema={
                "id": "uint64",
                "payload": "binary",
                "amount": "decimal128(20,2)",
                "day": "date32",
                "instant": "timestamp_micros",
            },
        )
        binary = source.select("id", "payload", "amount", "day", "instant").replace(
            {"payload": b"\x00\xff"}, {"payload": b"\x10"}
        )
        self.assertIsInstance(binary, sl.LazyFrame)
        self.assertEqual(binary.operations[-1].kind, "expression_project")
        binary_payload = json.loads(binary.operations[-1].values[0])
        self.assertEqual(
            binary_payload["rewrites"][0]["to_replace"],
            {"type": "binary", "value": "00ff"},
        )
        self.assertEqual(
            binary_payload["rewrites"][0]["replacement"],
            {"type": "binary", "value": "10"},
        )

        decimal = context.read_vortex(
            "input.vortex", schema={"amount": "decimal128(20,2)"}
        ).select("amount").replace(Decimal("1.20"), Decimal("2.30"))
        self.assertIsInstance(decimal, sl.LazyFrame)
        self.assertEqual(decimal.operations[-1].kind, "expression_project")
        decimal_rewrite = json.loads(decimal.operations[-1].values[0])["rewrites"][0]
        self.assertEqual(
            decimal_rewrite["to_replace"],
            {"type": "decimal128(3,2)", "value": "1.20"},
        )
        self.assertEqual(
            decimal_rewrite["replacement"],
            {"type": "decimal128(3,2)", "value": "2.30"},
        )

    def test_decimal_pivot_fill_lowers_to_typed_payload(self) -> None:
        workflow = (
            sl.ShardLoomContext()
            .read_vortex(
                "input.vortex",
                schema={
                    "id": "uint64",
                    "payload": "binary",
                    "amount": "decimal128(20,2)",
                },
            )
            .pivot_table(
                index="id",
                columns="payload",
                values="amount",
                aggfunc="first",
                fill_value=Decimal("0.125"),
            )
        )
        self.assertIsInstance(workflow, sl.LazyFrame)
        self.assertEqual(workflow.operations[-1].kind, "pivot")
        payload = json.loads(workflow.operations[-1].values[0])
        self.assertEqual(
            payload["fill_value"],
            {"type": "decimal128(3,3)", "value": "0.125"},
        )


if __name__ == "__main__":
    unittest.main()
