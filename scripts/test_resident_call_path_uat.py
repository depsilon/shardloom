# SPDX-License-Identifier: Apache-2.0
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from run_resident_call_path_uat import (
    Worker, cases, command_args, fixture_rows, paired_order, percentiles,
    request_options, validate, validate_candidate_aggregate, validate_candidate_count_where, validate_candidate_reuse, validate_preparation,
)


def envelope(value):
    return {"status": "success", "human_text": "result summary: native_collect values=" +
            json.dumps({"rows": len(value), "values": value}), "fields": [
                {"key": "public_workflow_fallback_attempted", "value": "false"},
                {"key": "public_workflow_external_engine_invoked", "value": "false"},
            ]}


class ResidentCallPathTests(unittest.TestCase):
    def test_held_out_fixture_covers_null_utf8_exact_integer_and_empty_selection(self):
        rows = fixture_rows()
        self.assertEqual(rows, fixture_rows())
        self.assertEqual(len(rows), 32)
        self.assertTrue(any(row["nullable_label"] is None for row in rows))
        self.assertTrue(any(row["nullable_label"] == "東京" for row in rows))
        self.assertTrue(all(abs(row["exact_identifier"]) > 2**53 for row in rows))
        self.assertEqual(cases(rows)[2]["expected"], rows[24:])
        self.assertEqual(cases(rows)[3]["expected"], [])
        counts = {case["name"]: case for case in cases(rows) if case["primitive"] == "count_where"}
        self.assertEqual(counts["filtered_count"]["expected"], 8)
        self.assertEqual(counts["empty_filtered_count"]["expected"], 0)
        self.assertTrue(all("columns" not in case for case in counts.values()))

    def test_public_command_and_python_options_describe_same_native_operation(self):
        source = Path("/tmp/renamed.vortex")
        for case in cases(fixture_rows()):
            command = command_args(source, case)
            options = request_options(source, case)
            self.assertEqual(command[command.index("--input") + 1], options["input_uri"])
            if "sql" in case:
                self.assertEqual(command[1], "sql")
                self.assertEqual(command[command.index("--sql") + 1], options["sql_statement"])
                self.assertNotIn("--vortex-primitive", command)
                self.assertNotIn("vortex_primitive", options)
            else:
                self.assertEqual(command[command.index("--vortex-primitive") + 1], options["vortex_primitive"])
            if "columns" in case:
                self.assertEqual(command[command.index("--vortex-columns") + 1], ",".join(options["vortex_columns"]))
            if "predicate" in case:
                self.assertEqual(command[command.index("--vortex-predicate") + 1], options["vortex_predicate"])

    def test_percentiles_use_nearest_rank_and_alternation_is_balanced(self):
        result = percentiles(list(range(1, 101)))
        self.assertEqual((result["p50_seconds"], result["p95_seconds"], result["p99_seconds"]), (50, 95, 99))
        self.assertEqual(paired_order(0), ("baseline", "candidate"))
        self.assertEqual(paired_order(1), ("candidate", "baseline"))
        for invalid in ([], [-1], [float("inf")], [float("nan")]):
            with self.assertRaises(ValueError):
                percentiles(invalid)

    def test_validation_requires_complete_values_integer_precision_and_no_fallback(self):
        rows = fixture_rows()
        self.assertEqual(validate(envelope(rows), rows), validate(envelope(rows), rows))
        changed = json.loads(json.dumps(rows))
        changed[0]["exact_identifier"] = float(changed[0]["exact_identifier"])
        with self.assertRaises(ValueError):
            validate(envelope(changed), rows)
        with self.assertRaises(ValueError):
            validate(envelope(rows[:1]), rows)
        unsafe = envelope(rows)
        unsafe["fields"].append({"key": "engine_external_engine_invoked", "value": "true"})
        with self.assertRaises(ValueError):
            validate(unsafe, rows)
        with self.assertRaises(ValueError):
            validate_preparation(unsafe)
        with self.assertRaises(ValueError):
            validate_preparation({"status": "success", "fields": []})

    def test_count_requires_prepared_reader_and_per_call_execution_evidence(self):
        for surface in ("persistent_worker", "python_client"):
            validate_candidate_reuse({"resident_source_opens": "1",
                                      "resident_completed_executions": "4"}, surface, 3)
            for fields in ({}, {"resident_source_opens": "1"},
                           {"resident_source_opens": "2", "resident_completed_executions": "4"},
                           {"resident_source_opens": "1", "resident_completed_executions": "1"}):
                with self.assertRaises(ValueError):
                    validate_candidate_reuse(fields, surface, 3)
        validate_candidate_reuse({"resident_source_opens": "1",
                                  "resident_completed_executions": "1"}, "fresh_cli_process", 3)

    def test_filtered_count_requires_actual_native_proof_without_invented_oracle(self):
        fields = {"filtered_count_local_execution_count": "8",
                  "local_primitive_native_io_certificate_emitted": "true",
                  "local_primitive_native_io_certified": "true",
                  "local_primitive_execution_certificate_emitted": "false",
                  "local_primitive_no_query_answer_cache": "true",
                  "resident_source_generation_validation": "before_and_after_native_scan_including_metadata_pruned_result"}
        validate_candidate_count_where(fields, 8)
        for key in fields:
            missing = dict(fields)
            del missing[key]
            with self.assertRaises(ValueError):
                validate_candidate_count_where(missing, 8)
        for key, value in (("filtered_count_local_execution_count", "0"),
                           ("local_primitive_execution_certificate_emitted", "true"),
                           ("local_primitive_native_io_certified", "false")):
            with self.assertRaises(ValueError):
                validate_candidate_count_where({**fields, key: value}, 8)

    def test_aggregate_complete_expectations_and_fresh_execution_evidence(self):
        aggregates = {case["name"]: case for case in cases(fixture_rows()) if case["primitive"] == "aggregate"}
        self.assertEqual(aggregates["scalar_integer_aggregate"]["expected"],
                         [{"rows_alias": 32, "unique_alias": 32, "total_alias": 496.0}])
        self.assertEqual(aggregates["filtered_integer_aggregate"]["expected"],
                         [{"rows_alias": 8, "unique_alias": 8, "total_alias": 220.0}])
        self.assertEqual(aggregates["grouped_exact_distinct_aggregate"]["expected"],
                         [{"cohort_key": i, "unique_alias": 1} for i in range(3, 8)])
        fields = {"local_primitive_native_io_certificate_emitted": "true",
                  "local_primitive_native_io_certified": "true",
                  "local_primitive_execution_certificate_emitted": "false",
                  "local_primitive_no_query_answer_cache": "true",
                  "resident_aggregate_handle_retained": "true",
                  "resident_aggregate_lowering_reused": "true",
                  "resident_source_generation_validation": "before_and_after_native_scan_including_metadata_pruned_result"}
        validate_candidate_aggregate(fields, "persistent_worker", 2)
        validate_candidate_aggregate({**fields, "resident_aggregate_lowering_reused": "false"}, "fresh_cli_process", 2)
        for key in fields:
            missing = dict(fields)
            del missing[key]
            with self.assertRaises(ValueError):
                validate_candidate_aggregate(missing, "persistent_worker", 2)

    @unittest.skipUnless(os.name == "posix", "native process group fixture")
    def test_worker_retains_exact_raw_response_and_times_out_without_leaking_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fake-worker"
            binary.write_text(f"#!{sys.executable}\nimport sys, json, time\n"
                              "for line in sys.stdin:\n"
                              " request = json.loads(line)\n"
                              " if request['args'] == ['stall']: time.sleep(30)\n"
                              " print(json.dumps({'args': request['args']}), flush=True)\n")
            binary.chmod(0o755)
            worker = Worker(binary, root / "stderr")
            try:
                result, seconds, raw = worker.request(["first"], 10, lambda: None)
                self.assertEqual(result, {"args": ["first"]})
                self.assertEqual(json.loads(raw), result)
                self.assertGreaterEqual(seconds, 0)
                self.assertEqual(worker.request(["second"], 10, lambda: None)[0], {"args": ["second"]})
                with self.assertRaises(TimeoutError):
                    worker.request(["stall"], 0.02, lambda: None)
            finally:
                worker.close()
            self.assertIsNotNone(worker.process.poll())


if __name__ == "__main__":
    unittest.main()
