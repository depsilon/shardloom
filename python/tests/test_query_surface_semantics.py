# SPDX-License-Identifier: Apache-2.0
"""Adversarial controls for the broad corpus's strict result gate."""
from __future__ import annotations

from dataclasses import replace
import datetime as dt
from decimal import Decimal, localcontext
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks" / "traditional_analytics"))

from query_surface.semantics import (
    ComparisonLimit, ErrorContract, FloatPolicy, ResultContract, ResultMismatch,
    ResultType, TypedResult, compare, schema_record, result_record, result_from_record,
    schema_from_record,
)


class StrictQueryResultTests(unittest.TestCase):
    def contract(self, dtype="int64", *, nullable=False, ordered=False, **kwargs):
        return ResultContract((("v", ResultType(dtype, nullable)),), ordered=ordered, **kwargs)

    def check(self, wanted, actual, contract=None):
        contract = contract or self.contract()
        compare(TypedResult(contract.schema, [{"v": v} for v in wanted]),
                TypedResult(contract.schema, [{"v": v} for v in actual]), contract)

    def test_schema_names_order_width_nullability_and_decimal_scale_are_exact(self):
        schema = (("a", ResultType("int64", False)), ("b", ResultType("utf8", True)))
        contract = ResultContract(schema)
        for changed in (
            schema[::-1], (("alias", schema[0][1]), schema[1]),
            (("a", ResultType("int32", False)), schema[1]),
            (("a", ResultType("int64", True)), schema[1]),
        ):
            with self.subTest(schema=changed), self.assertRaises(ResultMismatch):
                compare(TypedResult(schema, []), TypedResult(changed, []), contract)
        decimal = (("d", ResultType("decimal128", True, (18, 2))),)
        with self.assertRaises(ResultMismatch):
            compare(TypedResult(decimal, []),
                    TypedResult((("d", ResultType("decimal128", True, (18, 3))),), []),
                    ResultContract(decimal))

    def test_unordered_multiset_preserves_duplicate_multiplicity(self):
        self.check([1, 2, 1], [2, 1, 1])
        with self.assertRaises(ResultMismatch):
            self.check([1, 2, 1], [2, 2, 1])
        with self.assertRaises(ResultMismatch):
            self.check([1, 1], [1])

    def test_order_is_observable_when_declared(self):
        with self.assertRaises(ResultMismatch):
            self.check([1, 2], [2, 1], self.contract(ordered=True))

    def test_empty_results_still_require_exact_schema(self):
        self.check([], [])
        with self.assertRaises(ResultMismatch):
            compare(TypedResult(self.contract().schema, []), TypedResult((), []), self.contract())

    def test_row_columns_cannot_be_omitted_or_invented(self):
        contract = self.contract()
        for row in ({}, {"v": 1, "extra": 4}):
            with self.assertRaises(ResultMismatch):
                compare(TypedResult(contract.schema, [{"v": 1}]), TypedResult(contract.schema, [row]), contract)

    def test_null_zero_nan_and_empty_text_are_distinct(self):
        with self.assertRaises(ResultMismatch):
            self.check([None], [0], self.contract(nullable=True))
        with self.assertRaises(ResultMismatch):
            self.check([None], [""], self.contract("utf8", nullable=True))
        with self.assertRaises(ResultMismatch):
            self.check([None], [float("nan")], self.contract("float64", nullable=True))
        with self.assertRaises(ResultMismatch):
            self.check([None], [None])

    def test_integer_domain_never_coerces_bool_float_or_overflow(self):
        for actual in (True, 1.0, 1 << 63):
            with self.subTest(actual=actual), self.assertRaises(ResultMismatch):
                self.check([1], [actual])
        self.check([2**63 + 17], [2**63 + 17], self.contract("uint64"))
        with self.assertRaises(ResultMismatch):
            self.check([0], [-1], self.contract("uint64"))

    def test_decimal_is_exact_independent_of_decimal_context(self):
        schema = (("v", ResultType("decimal128", False, (38, 6))),)
        contract = ResultContract(schema)
        value = Decimal("12345678901234567890123456789012.345678")
        with localcontext() as context:
            context.prec = 4
            self.check([value], [value], contract)
            with self.assertRaises(ResultMismatch):
                self.check([value], [Decimal("12345678901234567890123456789012.345677")], contract)
        for actual in (1.0, "1.00", Decimal("0.0000001"), Decimal("1e38"), Decimal("NaN")):
            with self.subTest(actual=actual), self.assertRaises(ResultMismatch):
                self.check([Decimal("1")], [actual], contract)

    def test_float_policy_is_explicit_and_does_not_round(self):
        with self.assertRaises(ResultMismatch):
            self.check([1.0], [1.00001], self.contract("float64"))
        contract = self.contract("float64", floats={"v": FloatPolicy(relative=1e-12, reason="sum order")})
        self.check([1.0], [1.0 + 1e-13], contract)
        with self.assertRaises(ResultMismatch):
            self.check([1.0], [1.0 + 1e-10], contract)
        for value in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                FloatPolicy(absolute=value)
        with self.assertRaises(ValueError):
            self.contract(floats={"v": FloatPolicy()})

    def test_nonfinite_and_signed_zero_require_declared_semantics(self):
        contract = self.contract("float64")
        self.check([float("inf"), -float("inf")], [-float("inf"), float("inf")], contract)
        for wanted, actual in (([0.0], [-0.0]), ([float("inf")], [-float("inf")]),
                               ([float("nan")], [float("nan")])):
            with self.subTest(wanted=wanted), self.assertRaises(ResultMismatch):
                self.check(wanted, actual, contract)
        policy = FloatPolicy(nan_equal=True, signed_zero_equal=True, reason="explicit SQL policy")
        self.check([float("nan"), 0.0], [-0.0, float("nan")], replace(contract, floats={"v": policy}))

    def test_tolerant_multiset_needs_complete_matching_not_greedy(self):
        contract = self.contract("float64", floats={"v": FloatPolicy(absolute=1, reason="test boundary")})
        # Greedy 0 -> 0.5 leaves 1 unable to match -1. A valid pairing exists.
        self.check([0.0, 1.0], [0.5, -1.0], contract)
        # Cancelling the exact 0 -> 0 pair would also wrongly reject this.
        self.check([0.0, 1.0], [0.0, -1.0], contract)
        with self.assertRaises(ResultMismatch):
            self.check([0.0, 1.0], [-1.0, -1.0], contract)

    def test_tolerant_matching_budget_is_inconclusive_not_pass(self):
        contract = self.contract("float64", max_matching_edges=1,
                                 floats={"v": FloatPolicy(absolute=1, reason="test boundary")})
        with self.assertRaises(ComparisonLimit):
            self.check([0.0, 1.0], [0.5, -1.0], contract)

    def test_temporal_normalization_is_exact_at_negative_epoch(self):
        self.check([dt.date(1969, 12, 31)], [-1], self.contract("date32"))
        self.check([dt.datetime(1969, 12, 31, 23, 59, 59, 999999)], [-1], self.contract("timestamp_micros"))
        with self.assertRaises(ResultMismatch):
            self.check([dt.datetime(1970, 1, 1)],
                       [dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)], self.contract("timestamp_micros"))

    def test_nested_values_preserve_child_nullability_and_order(self):
        schema = (("v", ResultType("list", True, (ResultType("int64", False),))),)
        contract = ResultContract(schema)
        self.check([[1, 2], []], [[], [1, 2]], contract)
        with self.assertRaises(ResultMismatch):
            self.check([[1, 2]], [[2, 1]], contract)
        with self.assertRaises(ResultMismatch):
            self.check([[1, 2]], [[1, None]], contract)
        self.assertEqual(schema_record(schema)[0]["type"]["parameters"][0]["nullable"], False)
        float_schema = (("v", ResultType("list", False, (ResultType("float64", False),))),)
        for ordered in (False, True):
            with self.assertRaises(ResultMismatch):
                self.check([[float("nan")]], [[float("nan")]], ResultContract(float_schema, ordered=ordered))

    def test_error_code_and_stage_are_part_of_the_contract(self):
        expected = ErrorContract("SLNATIVE_INVALID_CAST", "execute")
        expected.check("SLNATIVE_INVALID_CAST", "execute")
        for code, stage in (("SLNATIVE_UNSUPPORTED", "execute"), ("SLNATIVE_INVALID_CAST", "bind")):
            with self.assertRaises(ResultMismatch):
                expected.check(code, stage)

    def test_portable_result_records_preserve_exact_rich_values(self):
        schema = (("d", ResultType("decimal128", True, (38, 6))),
                  ("f", ResultType("float64", False)),
                  ("s", ResultType("struct", False, (("float64", ResultType("utf8", False)),))),
                  ("t", ResultType("timestamp_micros", False)))
        original = TypedResult(schema, [{"d": Decimal("12345678901234567890123456789012.345678"),
                                        "f": -0.0, "s": {"float64": "not a wire tag"},
                                        "t": dt.datetime(1969, 12, 31, 23, 59, 59, 999999)}])
        import json
        encoded = json.loads(json.dumps(result_record(original), allow_nan=False))
        compare(original, result_from_record(encoded), ResultContract(schema))
        encoded["rows"][0]["extra"] = 1
        with self.assertRaises(ValueError):
            result_from_record(encoded)

    def test_contract_cannot_change_via_callers_mutable_policy_map(self):
        policies = {"v": FloatPolicy()}
        contract = self.contract("float64", floats=policies)
        policies["v"] = FloatPolicy(absolute=100, reason="mutated")
        with self.assertRaises(ResultMismatch):
            self.check([1.0], [2.0], contract)
        with self.assertRaises(TypeError):
            contract.floats["v"] = policies["v"]
        for kwargs in ({"nan_equal": 1}, {"signed_zero_equal": "false"}, {"reason": None}):
            with self.assertRaises(ValueError):
                FloatPolicy(**kwargs)

    def test_wire_rejects_malformed_types_and_preserves_all_row_fields(self):
        for name, parameters in (("list", []), ("fixed_size_list", ["bad", 3]),
                                 ("decimal128", [0, 2]), ("decimal128", [38, True]),
                                 ("timestamp", ["days", None]), ("int64", [32])):
            with self.subTest(name=name, parameters=parameters), self.assertRaises(ValueError):
                schema_from_record([{"name": "v", "type": {"name": name, "nullable": True, "parameters": parameters}}])
        schema = self.contract().schema
        for row in ({"v": 1, "extra": 2}, {"v": True}, {"v": None}, {}):
            with self.subTest(row=row), self.assertRaises(ResultMismatch):
                result_record(TypedResult(schema, [row]))
        with self.assertRaises(ResultMismatch):
            result_from_record({"schema": schema_record(schema), "rows": [{"v": True}]})


if __name__ == "__main__":
    unittest.main()
