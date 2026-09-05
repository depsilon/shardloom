# SPDX-License-Identifier: Apache-2.0
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from run_resident_call_path_uat import (
    Worker, cases, command_args, fixture_rows, paired_order, percentiles,
    request_options, validate, validate_preparation,
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

    def test_public_command_and_python_options_describe_same_native_operation(self):
        source = Path("/tmp/renamed.vortex")
        for case in cases(fixture_rows()):
            command = command_args(source, case)
            options = request_options(source, case)
            self.assertEqual(command[command.index("--input") + 1], options["input_uri"])
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
