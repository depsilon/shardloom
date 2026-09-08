# SPDX-License-Identifier: Apache-2.0
import copy
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
import tempfile
from unittest import mock

import run_heldout_operator_uat as heldout
from run_heldout_operator_uat import (
    WORKERS, archive_stdout, cases, command_args, comparisons, concise_execution_fields, exact_equal, fixture_rows,
    fixture_input_writer, group_oracle, scalar_oracle, validate_diagnostic, validate_distinct_workers, validate_values,
)


def envelope(value):
    payload = str(value) if type(value) is int else "native values=" + json.dumps({"rows": len(value), "values": value})
    return {"status": "success", "human_text": "result summary: " + payload, "fallback": {"attempted": False},
            "fields": [{"key": "public_workflow_fallback_attempted", "value": "false"},
                       {"key": "public_workflow_external_engine_invoked", "value": "false"}]}


class HeldoutOperatorTests(unittest.TestCase):
    def test_jsonl_fixture_needs_no_pyarrow_and_preserves_generated_rows(self):
        rows = fixture_rows(64)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.jsonl"
            with mock.patch.dict(sys.modules, {"pyarrow": None, "pyarrow.parquet": None}):
                writer, metadata = fixture_input_writer("jsonl")
                writer(rows, path)
            self.assertEqual(metadata, {"format": "jsonl"})
            self.assertEqual([json.loads(line) for line in path.read_text().splitlines()], rows)
            with self.assertRaises(FileExistsError):
                writer(rows, path)

    def test_missing_optional_parquet_dependency_fails_before_creating_run_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "uat"
            args = SimpleNamespace(rows=64, cases=None, require_distinct_workers=False,
                                   fixture_format="parquet", uat_root=root)
            with mock.patch.dict(sys.modules, {"pyarrow": None, "pyarrow.parquet": None}):
                with self.assertRaisesRegex(ValueError, "requires optional PyArrow"):
                    heldout.execute(args)
            self.assertFalse(root.exists())

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "optional PyArrow fixture environment")
    def test_parquet_fixture_has_explicit_nullability_and_complete_exact_values(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        rows = fixture_rows(64)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.parquet"
            writer, metadata = fixture_input_writer("parquet")
            writer(rows, path)
            actual = pq.read_table(path)
            self.assertEqual(actual.to_pylist(), rows)
            self.assertEqual(len(actual.schema), 8)
            self.assertEqual({field.name for field in actual.schema if field.nullable},
                             {"optional_text", "optional_units"})
            for field in actual.schema:
                expected = pa.string() if field.name in {"category_text", "optional_text", "unique_text"} else pa.int64()
                self.assertEqual(field.type, expected)
            self.assertEqual(metadata["writer_version"], pa.__version__)
            self.assertEqual(cases(actual.to_pylist()), cases(rows))
            previous = path.read_bytes()
            with self.assertRaises(FileExistsError):
                writer(rows, path)
            self.assertEqual(path.read_bytes(), previous)

    def test_required_distinct_workers_prove_complete_drained_work_and_resource_bounds(self):
        prefix = "local_primitive_aggregate_workers_"
        counters = {"rows": 64, "exact_distinct_committed_rows": 64, "submitted_chunks": 2,
                    "completed_chunks": 2, "outstanding_chunks": 0, "provider_background_workers": 0,
                    "shared_live_peak_bytes": 128, "shared_live_limit_bytes": 1024,
                    "cpu_ceiling": 4, "compute_threads": 3, "worker_busy_elapsed_nanos": 50,
                    "inline_busy_elapsed_nanos": 0}
        fields = {prefix + key: str(value) for key, value in counters.items()}
        fields["local_primitive_aggregate_update_strategy"] = "complete_integer_pair_partition_distinct"
        validate_distinct_workers(fields, 4, 64)
        inline = {**fields, prefix + "cpu_ceiling": "1", prefix + "compute_threads": "0",
                  prefix + "worker_busy_elapsed_nanos": "0", prefix + "inline_busy_elapsed_nanos": "50"}
        validate_distinct_workers(inline, 1, 64)
        validate_distinct_workers(inline, 4, 64)  # Admission may grant fewer lanes than requested.
        for key in fields:
            if key == prefix + "inline_busy_elapsed_nanos":
                continue
            with self.subTest(missing=key), self.assertRaises(ValueError):
                validate_distinct_workers({name: value for name, value in fields.items() if name != key}, 4, 64)
        for key, value in (("rows", "63"), ("exact_distinct_committed_rows", "63"),
                           ("submitted_chunks", "0"), ("completed_chunks", "1"),
                           ("outstanding_chunks", "1"), ("provider_background_workers", "1"),
                           ("shared_live_limit_bytes", "0"), ("shared_live_limit_bytes", str(2 * heldout.GIB)),
                           ("shared_live_peak_bytes", "1025"),
                           ("cpu_ceiling", "5"), ("compute_threads", "4"), ("compute_threads", "0"),
                           ("worker_busy_elapsed_nanos", "0"), ("rows", True), ("rows", 64.0),
                           ("rows", "-1"), ("rows", "64.0")):
            with self.subTest(counter=key, value=value), self.assertRaises(ValueError):
                validate_distinct_workers({**fields, prefix + key: value}, 4, 64)
        for key, value in (("compute_threads", "1"), ("inline_busy_elapsed_nanos", "0")):
            with self.subTest(inline=key), self.assertRaises(ValueError):
                validate_distinct_workers({**inline, prefix + key: value}, 1, 64)
        with self.assertRaises(ValueError):
            validate_distinct_workers({**fields, "local_primitive_aggregate_update_strategy": "typed"}, 4, 64)

    def test_worker_requirement_rejects_a_case_selection_without_worker_acceptance(self):
        args = SimpleNamespace(rows=64, cases=["nullable_numeric_scalar"], require_distinct_workers=True)
        with self.assertRaisesRegex(ValueError, "at least one selected integer distinct"):
            heldout.execute(args)

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

    def test_summary_preserves_encoded_reduction_work_without_expanded_payload(self):
        prefix = "local_primitive_aggregate_encoded_numeric_reduction_"
        expected = {prefix + "logical_rows": "100000000", prefix + "child_rows": "4",
                    prefix + "constant_arrays": "0", prefix + "elapsed_nanos": "9007199254740993",
                    "local_primitive_scan_segment_reuse_hits": "2",
                    "local_primitive_scan_segment_reuse_retention_live_owned_bytes": "97"}
        source = {"fields": [{"key": key, "value": value} for key, value in expected.items()]}
        self.assertEqual(concise_execution_fields(source), expected)

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
        self.assertEqual(len({case["name"] for case in matrix}), 19)
        self.assertEqual({case["family"] for case in matrix}, {
            "scalar", "distinct", "numeric_group", "string_group", "composite_group",
            "string_transform", "relational_sort", "relational_collect", "overflow_diagnostic"})
        for worker in WORKERS:
            command = command_args(Path("/tmp/fixture.vortex"), matrix[0], worker)
            self.assertEqual(command[command.index("--max-parallelism") + 1], str(worker))
            self.assertEqual(command[command.index("--execution-policy") + 1], "native_vortex")

    def test_integer_distinct_oracle_keeps_exact_ids_global_ties_and_offsets(self):
        matrix = {case["name"]: case for case in cases(fixture_rows(133))}
        expected = [{"cohort_code": key, "different": 19} for key in (-1, 0, 1)]
        for name in ("exact_integer_distinct_topk", "repeated_integer_distinct_topk"):
            self.assertEqual(matrix[name]["expected"], expected)
            self.assertEqual(matrix[name]["comparison"], "ordered_exact_typed_values")
        rows = fixture_rows(133)
        # All identifiers remain distinct as integers; binary64 rounds many
        # together. This mutation must change the independently computed result.
        for row in rows:
            row["exact_identifier"] = float(row["exact_identifier"])
        rounded = {case["name"]: case for case in cases(rows)}
        self.assertNotEqual(rounded["exact_integer_distinct_topk"]["expected"], expected)

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
