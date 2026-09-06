# SPDX-License-Identifier: Apache-2.0
import copy
import gzip
import hashlib
import json
from pathlib import Path
import unittest
import tempfile

from run_heldout_operator_uat import (
    WORKERS, archive_stdout, cases, command_args, comparisons, concise_execution_fields, exact_equal, fixture_rows,
    group_oracle, scalar_oracle, validate_diagnostic, validate_values,
)


def envelope(value):
    payload = str(value) if type(value) is int else "native values=" + json.dumps({"rows": len(value), "values": value})
    return {"status": "success", "human_text": "result summary: " + payload, "fallback": {"attempted": False},
            "fields": [{"key": "public_workflow_fallback_attempted", "value": "false"},
                       {"key": "public_workflow_external_engine_invoked", "value": "false"}]}


class HeldoutOperatorTests(unittest.TestCase):
    def test_summary_keeps_bounded_counters_without_repeating_full_envelope(self):
        source = {"fields": [
            {"key": "local_primitive_aggregate_first_pass_accessor_nanos", "value": "452"},
            {"key": "resident_completed_executions", "value": "1"},
            {"key": "public_workflow_fallback_attempted", "value": "false"},
            {"key": "public_workflow_very_large_inventory", "value": "x" * 150000},
            {"key": "resident_very_large_future_label", "value": "x" * 150000},
        ]}
        self.assertEqual(concise_execution_fields(source), {
            "local_primitive_aggregate_first_pass_accessor_nanos": "452",
            "resident_completed_executions": "1", "public_workflow_fallback_attempted": "false"})
        with self.assertRaises(ValueError):
            concise_execution_fields({"fields": [{"key": f"resident_future_{index}", "value": "1"} for index in range(129)]})

    def test_summary_preserves_numeric_decode_and_copy_work_including_zero(self):
        prefix = "local_primitive_aggregate_native_numeric_accessor_"
        for calls, rows, copied, elapsed in [
            ("3", "23", "184", "9007199254740993"),
            ("3", "23", "0", "9007199254740993"),
            (0, 0, 0, 0),
        ]:
            with self.subTest(calls=calls, copied=copied):
                expected = {prefix + key: value for key, value in {
                    "native_decode_calls": calls,
                    "rows": rows,
                    "typed_value_bytes_copied": copied,
                    "decode_and_typed_copy_nanos": elapsed,
                }.items()}
                source = {"status": "success", "human_text": "result " + "x" * 150000,
                          "fields": [{"key": key, "value": value} for key, value in expected.items()] + [
                              {"key": prefix + "oversized_detail", "value": "x" * 150000},
                              {"key": prefix + "structured_detail", "value": {"values": [1, 2, 3]}},
                              {"key": "unrelated_result_values", "value": "[1,2,3]"},
                          ]}
                original = copy.deepcopy(source)
                actual = concise_execution_fields(source)
                self.assertEqual(actual, expected)
                for key, value in expected.items():
                    self.assertIs(type(actual[key]), type(value))
                self.assertEqual(source, original)

    def test_stdout_archive_is_lossless_hashed_and_never_overwrites_existing_evidence(self):
        raw = ('{"utf8":"東京🙂","exact":9223372036854775807,"escaped":"\\n"}\n' * 200).encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new.stdout.json"
            path.write_bytes(raw)
            record = archive_stdout(path)
            compressed = path.parent / record["envelope"]
            self.assertFalse(path.exists())
            with gzip.open(compressed, "rb") as source:
                self.assertEqual(source.read(), raw)
            self.assertEqual(record["stdout_raw_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(record["stdout_raw_bytes"], len(raw))
            self.assertLess(record["stdout_gzip_bytes"], len(raw))
            previous = compressed.read_bytes()
            path.write_bytes(b"another output")
            with self.assertRaises(FileExistsError):
                archive_stdout(path)
            self.assertEqual(path.read_bytes(), b"another output")
            self.assertEqual(compressed.read_bytes(), previous)

    def test_oracle_has_independent_exact_null_group_distinct_and_empty_expectations(self):
        rows = [{"key": None, "v": 3}, {"key": "λ", "v": None},
                {"key": None, "v": -1}, {"key": "λ", "v": 3}]
        self.assertEqual(scalar_oracle(rows, "v"),
                         {"n": 4, "present": 3, "total": 5.0, "average": 5 / 3, "smallest": -1, "largest": 3})
        self.assertEqual(scalar_oracle([], "v"),
                         {"n": 0, "present": 0, "total": None, "average": None, "smallest": None, "largest": None})
        self.assertEqual(group_oracle(rows, ("key",)), [{"key": None, "n": 2}, {"key": "λ", "n": 2}])
        self.assertEqual(group_oracle(rows, ("key",), lambda group: {
            "different": len({row["v"] for row in group if row["v"] is not None})}),
            [{"key": None, "different": 2}, {"key": "λ", "different": 1}])

    def test_fixture_is_deterministic_bounded_skewed_unicode_nullable_and_exact_integer(self):
        rows = fixture_rows(4096)
        self.assertEqual(rows, fixture_rows(4096))
        self.assertEqual(len({row["unique_text"] for row in rows}), len(rows))
        self.assertGreater(sum(row["category_text"] == "hot-set" for row in rows), len(rows) * 0.89)
        self.assertEqual(rows[0]["exact_identifier"], -(2**63))
        self.assertEqual(rows[-1]["exact_identifier"], 2**63 - 1)
        self.assertTrue(all(abs(row["exact_identifier"]) > 2**53 for row in rows))
        self.assertTrue(any(row["optional_text"] is None for row in rows))
        self.assertTrue(any(row["category_text"] == "東京" for row in rows))
        self.assertTrue(any(row["category_text"] == "" for row in rows))
        for size in (0, 63, 131073):
            with self.assertRaises(ValueError):
                fixture_rows(size)
        matrix = cases(rows)
        self.assertEqual(len({case["name"] for case in matrix}), 16)
        self.assertEqual({case["family"] for case in matrix}, {
            "scalar", "distinct", "numeric_group", "string_group", "composite_group",
            "string_transform", "relational_sort", "relational_collect", "overflow_diagnostic"})
        for worker in WORKERS:
            command = command_args(Path("/tmp/fixture.vortex"), matrix[0], worker)
            self.assertEqual(command[command.index("--max-parallelism") + 1], str(worker))
            self.assertEqual(command[command.index("--execution-policy") + 1], "native_vortex")

    def test_full_values_preserve_type_precision_multiplicity_order_and_no_fallback(self):
        case = {"expected": [{"n": 2, "key": None}, {"n": 1, "key": 2**63 - 1}],
                "comparison": "exact_typed_multiset"}
        rows = copy.deepcopy(case["expected"])
        self.assertEqual(validate_values(envelope(rows), case), validate_values(envelope(rows[::-1]), case))
        for mutation in ([rows[0]], [rows[0], rows[0]], [{"n": 2.0, "key": None}, rows[1]],
                         [rows[0], {"n": 1, "key": float(2**63 - 1)}]):
            with self.assertRaises(ValueError):
                validate_values(envelope(mutation), case)
        ordered = {**case, "comparison": "ordered_exact_typed_values"}
        with self.assertRaises(ValueError):
            validate_values(envelope(rows[::-1]), ordered)
        unsafe = envelope(rows)
        unsafe["fields"].append({"key": "nested_external_engine_invoked", "value": "true"})
        with self.assertRaises(ValueError):
            validate_values(unsafe, case)
        unsafe = envelope(rows)
        unsafe["fallback"]["attempted"] = True
        with self.assertRaises(ValueError):
            validate_values(unsafe, case)
        self.assertFalse(exact_equal(1, True))
        self.assertFalse(exact_equal(1.0, 1))
        self.assertFalse(exact_equal(float("nan"), float("nan")))
        self.assertFalse(exact_equal(1.0000000000000002, 1.0))

    def test_unrelated_failures_and_wrapped_success_cannot_pass_overflow_case(self):
        case = cases(fixture_rows(64))[-1]
        failure = {"status": "error", "human_text": "direct int64 offset key overflowed; no fallback execution was attempted",
                   "fallback": {"attempted": False},
                   "diagnostics": [{"code": "SL-NATIVE-001", "fallback": {"attempted": False}}]}
        self.assertEqual(validate_diagnostic(failure, case, 1), ["SL-NATIVE-001"])
        for code in (0, -9):
            with self.assertRaises(ValueError):
                validate_diagnostic(failure, case, code)
        for key, value in (("status", "success"), ("human_text", "unsupported route; no fallback execution was attempted"),
                           ("human_text", "memory counter overflowed; no fallback execution was attempted"),
                           ("diagnostics", []), ("diagnostics", [{"code": "SL-NATIVE-001", "fallback": {"attempted": True}}])):
            with self.assertRaises(ValueError):
                validate_diagnostic({**failure, key: value}, case, 1)

    def test_scoring_requires_every_pair_and_warmup_and_excludes_expected_errors(self):
        selected = [cases(fixture_rows(64))[0]]
        records = [{"case": selected[0]["name"], "requested_workers": 4, "variant": name,
                    "sample": sample, "warmup": sample == 0, "passed": True,
                    "seconds": 2.0 if name == "baseline" else 1.0}
                   for name in ("baseline", "candidate") for sample in range(4)]
        result = comparisons(records, selected, [4], 3)["metadata_count.workers_4"]
        self.assertTrue(result["complete"])
        self.assertEqual(result["median_paired_ratio"], 2.0)
        self.assertEqual(result["candidate"]["sample_count"], 3)
        for incomplete in (records[:-1], records + [records[0]],
                           [{**record, "passed": False} if index == 0 else record for index, record in enumerate(records)]):
            result = comparisons(incomplete, selected, [4], 3)["metadata_count.workers_4"]
            self.assertFalse(result["complete"])
            self.assertNotIn("median_paired_ratio", result)
        diagnostic = cases(fixture_rows(64))[-1]
        diagnostics = [{**record, "case": diagnostic["name"]} for record in records]
        result = comparisons(diagnostics, [diagnostic], [4], 3)["checked_signed_group_overflow.workers_4"]
        self.assertTrue(result["complete"])
        self.assertNotIn("median_paired_ratio", result)


if __name__ == "__main__":
    unittest.main()
