# SPDX-License-Identifier: Apache-2.0
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

from run_clickbench_query_uat import equivalent, extract_result, run_command, run_profiled_command, score, strict_json


def envelope(summary):
    return {"status": "success", "human_text": summary, "fields": [
        {"key": "public_workflow_fallback_attempted", "value": "false"},
        {"key": "public_workflow_external_engine_invoked", "value": "false"},
    ]}


class ClickBenchUatTests(unittest.TestCase):
    def test_complete_scalar_and_row_results_preserve_integer_precision(self):
        self.assertEqual(extract_result(envelope("value summary: 99997497")), 99997497)
        rows = [{"UserID": 435090932899640449, "label": "\u03bb", "absent": None}]
        result = envelope("result summary: native_collect values=" + json.dumps({"rows": 1, "values": rows}))
        self.assertEqual(extract_result(result), rows)
        self.assertEqual(extract_result(envelope('result summary: aggregate values={"rows":1,"values":{"x":3}}')), [{"x": 3}])

    def test_descriptors_truncated_values_and_missing_evidence_fail_closed(self):
        for summary in ('result summary: projected_columns=id rows=4',
                        'result summary: aggregate values={"rows":4,"values":[{"x":1}]}'):
            with self.assertRaises(ValueError):
                extract_result(envelope(summary))
        for change in ({"fields": []}, {"status": "failed"}):
            with self.assertRaises(ValueError):
                extract_result(envelope("value summary: 4") | change)
        unsafe = envelope("value summary: 4")
        unsafe["fields"].append({"key": "kernel_external_engine_invoked", "value": "true"})
        with self.assertRaises(ValueError):
            extract_result(unsafe)

    def test_result_comparison_handles_order_nulls_and_float_tolerance(self):
        self.assertFalse(equivalent([1, 2], [2, 1]))
        self.assertFalse(equivalent(435090932899640449, float(435090932899640449)))
        self.assertFalse(equivalent(None, "null"))
        self.assertTrue(equivalent(1.0, 1.0 + 1e-14))
        self.assertFalse(equivalent(1.0, 1.01))
        for invalid in ('[NaN]', '[Infinity]', '[1e999]'):
            with self.assertRaises(ValueError):
                strict_json(invalid)

    def test_hot_score_uses_only_second_and_third_runs(self):
        records = [{"query": 1, "run": i + 1, "seconds": seconds, "passed": True} for i, seconds in enumerate([1.0, 5.0, 3.0])]
        result = score(records, 1)
        self.assertEqual(result["query_total_seconds"], 1.0)
        self.assertEqual(result["hot_total_seconds"], 3.0)
        self.assertEqual(result["runs_completed"], 3)
        self.assertFalse(score(records[:2], 1)["complete"])
        self.assertFalse(score([records[0]] * 3, 1)["complete"])

    def test_process_clock_excludes_watchdog_wait_and_timeout_stops_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def guard():
                pass
            result = run_command([sys.executable, "-c", "print('{}')"], root / "out", root / "err", 10, guard)
            self.assertEqual(result["returncode"], 0)
            self.assertFalse(result["guard_failures"])
            started = time.monotonic()
            result = run_command([sys.executable, "-c", "import time; time.sleep(30)"], root / "slow", root / "slowerr", 0.1, guard)
            self.assertNotEqual(result["returncode"], 0)
            self.assertIn("timeout", result["guard_failures"][0])
            self.assertLess(time.monotonic() - started, 5)

    def test_targeted_results_cannot_be_scored_as_complete_full_suite(self):
        records = [{"query": q, "run": r, "seconds": float(r), "passed": True}
                   for q in [34, 35] for r in [3, 1, 2]]
        self.assertFalse(score(records, 43)["complete"])
        result = score(records, 43, [34, 35])
        self.assertEqual(result["query_ids"], [34, 35])
        self.assertEqual(result["hot_total_seconds"], 4.0)
        for selected in ([], [0], [44], [34, 34]):
            with self.assertRaises(ValueError):
                score(records, 43, selected)

    def test_native_profile_covers_one_child_and_complete_output(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "profile"
            result = run_profiled_command([sys.executable, "-c", "print('complete output')"],
                                          prefix, 10, lambda: None)
            self.assertEqual(result["returncode"], 0)
            self.assertFalse(result["guard_failures"])
            self.assertGreater(result["native_peak_rss_bytes"], 0)
            self.assertGreaterEqual(result["user_cpu_seconds"], 0)
            self.assertGreaterEqual(result["system_cpu_seconds"], 0)
            self.assertLess(result["seconds"], result["supervised_wall_seconds"])
            self.assertEqual(prefix.with_suffix(".stdout.json").read_text(), "complete output\n")

    @unittest.skipUnless(os.name == "posix", "process-group ownership requires POSIX")
    def test_profile_timeout_kills_native_child_that_ignores_termination(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "ignores-term"
            result = run_profiled_command([
                sys.executable, "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
            ], prefix, 0.3, lambda: None)
            self.assertTrue(result["guard_failures"])
            pid = int(prefix.with_suffix(".pid").read_text())
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.05)
            else:
                self.fail("timed-out native child survived its supervisor's process group")


if __name__ == "__main__":
    unittest.main()
