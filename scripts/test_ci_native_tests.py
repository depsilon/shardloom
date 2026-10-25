"""Unit tests for native libtest inventory, partitioning, and result checks."""
from __future__ import annotations

import math
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import ci_native_tests as native


class ParseInventoryTests(unittest.TestCase):
    def test_parses_exact_test_names_and_summary(self) -> None:
        self.assertEqual(
            native.parse_inventory("alpha::case: test\nbeta: test\n2 tests, 0 benchmarks\n"),
            ["alpha::case", "beta"],
        )

    def test_rejects_duplicate_names(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            native.parse_inventory("alpha: test\nalpha: test\n")

    def test_rejects_benchmarks_and_unrecognized_lines(self) -> None:
        for output in ("bench: benchmark\n", "mystery output\n", "2 tests, 1 benchmarks\n"):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, "unexpected"):
                native.parse_inventory(output)


class PartitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tests = [f"test_{index}" for index in range(8)]
        self.baseline = {
            "ignored_tests": [],
            "estimated_seconds": {"test_0": 8, "test_1": 7, "test_2": 6, "test_3": 5,
                                   "test_4": 4, "test_5": 3, "test_6": 2, "test_7": 1},
            "unmeasured_test_weight_seconds": 1,
        }

    def test_lpt_longest_first_spreads_work_deterministically_and_covers_once(self) -> None:
        partitions = native.partition_tests(self.tests, [], self.baseline, count=4)
        self.assertEqual([row["tests"] for row in partitions],
                         [["test_0", "test_7"], ["test_1", "test_6"],
                          ["test_2", "test_5"], ["test_3", "test_4"]])
        self.assertEqual([row["estimated_seconds"] for row in partitions], [9, 9, 9, 9])
        self.assertEqual(partitions, native.partition_tests(self.tests, [], self.baseline, count=4))

    def test_unmeasured_tests_are_included_with_default_weight(self) -> None:
        baseline = {**self.baseline, "estimated_seconds": {}, "unmeasured_test_weight_seconds": 2}
        partitions = native.partition_tests(["unknown_b", "known", "unknown_a"], [], baseline, count=3)
        self.assertEqual([row["tests"] for row in partitions], [["known"], ["unknown_a"], ["unknown_b"]])
        self.assertEqual([row["estimated_seconds"] for row in partitions], [2, 2, 2])

    def test_rejects_nonpositive_or_nonfinite_default_and_estimate_weights(self) -> None:
        for invalid in (0, -1, math.nan, math.inf, -math.inf):
            with self.subTest(default=invalid), self.assertRaisesRegex(ValueError, "finite positive"):
                native.partition_tests(self.tests, [], {**self.baseline,
                    "unmeasured_test_weight_seconds": invalid})
            with self.subTest(estimate=invalid), self.assertRaisesRegex(ValueError, "finite positive"):
                native.partition_tests(self.tests, [], {**self.baseline,
                    "estimated_seconds": {"test_0": invalid}})

    def test_rejects_ignored_policy_drift(self) -> None:
        self.baseline["ignored_tests"] = ["test_0"]
        with self.assertRaisesRegex(ValueError, "ignored-test policy"):
            native.partition_tests(self.tests, [], self.baseline)
        with self.assertRaisesRegex(ValueError, "ignored-test policy"):
            native.partition_tests(self.tests, ["test_1"], self.baseline)

    def test_coverage_rejects_missing_duplicate_unexpected_names_and_duplicate_ids(self) -> None:
        correct = [{"id": 0, "tests": ["a"]}, {"id": 1, "tests": ["b"]}]
        bad_cases = [
            [{"id": 0, "tests": ["a"]}],
            [{"id": 0, "tests": ["a", "a"]}, {"id": 1, "tests": ["b"]}],
            [{"id": 0, "tests": ["a", "invented"]}, {"id": 1, "tests": ["b"]}],
            [{"id": 0, "tests": ["a"]}, {"id": 0, "tests": ["b"]}],
        ]
        with self.assertRaisesRegex(ValueError, "duplicate native partition IDs"):
            native.verify_coverage(["a", "b"], bad_cases[-1])
        for partitions in bad_cases[:-1]:
            with self.subTest(partitions=partitions), self.assertRaisesRegex(ValueError, "omit, duplicate or invent"):
                native.verify_coverage(["a", "b"], partitions)
        native.verify_coverage(["a", "b"], correct)


class ParseResultsTests(unittest.TestCase):
    def test_accepts_complete_pass_ignored_and_should_panic_outcomes(self) -> None:
        output = """running 3 tests
test suite::passes ... ok
test suite::expected_panic - should panic ... ok
test suite::skipped ... ignored, guarded
test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 4 filtered out
"""
        self.assertEqual(native.parse_results(output,
            ["suite::passes", "suite::expected_panic", "suite::skipped"], ["suite::skipped"]),
            {"passed": 2, "ignored": 1,
             "tests": ["suite::passes", "suite::expected_panic", "suite::skipped"]})

    def test_rejects_zero_missing_duplicate_or_invented_outcomes(self) -> None:
        summary = "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
        outputs = [
            summary,
            "test a ... ok\n" + summary,
            "test a ... ok\ntest a ... ok\ntest b ... ok\n" + summary,
            "test a ... ok\ntest invented ... ok\n" + summary,
        ]
        for output in outputs:
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, "every assigned test"):
                native.parse_results(output, ["a", "b"], [])

    def test_rejects_expanded_ignored_set(self) -> None:
        output = "test a ... ignored\ntest b ... ok\ntest result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\n"
        with self.assertRaisesRegex(ValueError, "ignored-test policy"):
            native.parse_results(output, ["a", "b"], [])

    def test_rejects_nonzero_failures_and_dishonest_summary(self) -> None:
        cases = [
            "test a ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n",
            "test a ... ok\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n",
        ]
        for output in cases:
            with self.subTest(output=output), self.assertRaises(ValueError):
                native.parse_results(output, ["a"], [])


class ProcessAndEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "shardloom-vortex").mkdir()
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        (self.bundle / "manifest.json").write_text("fixture")
        self.manifest = {"identity": {"revision": "fixture-revision"}}
        self.suite = {"tests": ["one"], "ignored": [],
                      "partitions": [{"id": 0, "tests": ["one"]}]}
        self.results = self.root / "results"
        self.log = "test one ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n"
        self.binary = self.bundle / "native-tests"

    def executable(self, body: str) -> None:
        self.binary.write_text(f"#!{sys.executable}\n" + body)
        self.binary.chmod(0o755)

    def run_case(self, body: str, *, timeout: float = 5) -> dict:
        self.executable(body)
        with patch.object(native, "verify_plan", return_value=(self.manifest, self.suite)):
            return native.run_partition(self.bundle, 0, self.results / "0", root=self.root,
                                        timeout=timeout)

    def check_aggregate(self) -> dict:
        with patch.object(native, "verify_plan", return_value=(self.manifest, self.suite)):
            return native.aggregate(self.bundle, self.results, root=self.root)

    def test_runs_real_child_process_and_rechecks_log_before_coverage_credit(self) -> None:
        receipt = self.run_case(f"print({self.log!r})\n")
        self.assertTrue(receipt["children_drained"])
        self.assertEqual(self.check_aggregate()["passed"], 1)
        (self.results / "0/tests.log").write_text("missing results\n")
        with self.assertRaisesRegex(ValueError, "log is missing or changed"):
            self.check_aggregate()

    def test_rehashed_log_still_must_prove_complete_test_outcomes(self) -> None:
        self.run_case(f"print({self.log!r})\n")
        log = self.results / "0/tests.log"
        log.write_text("test result: ok. 1 passed; 0 failed; 0 ignored;\n")
        path = self.results / "0/report.json"
        row = json.loads(path.read_text())
        row["log_sha256"] = native.sha(log)
        path.write_text(json.dumps(row))
        with self.assertRaisesRegex(ValueError, "every assigned test"):
            self.check_aggregate()

    def test_passing_parent_with_running_child_fails_and_records_cleanup_problem(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "left a child process"):
            self.run_case("import subprocess, sys\n"
                          "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
                          f"print({self.log!r})\n")
        receipt = json.loads((self.results / "0/report.json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertFalse(receipt["children_drained"])

    def test_nonzero_exit_and_deadline_never_receive_pass_credit(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "failed with"):
            self.run_case(f"print({self.log!r})\nraise SystemExit(1)\n")
        receipt = json.loads((self.results / "0/report.json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.results = self.root / "timeout-results"
        with self.assertRaisesRegex(RuntimeError, "deadline"):
            self.run_case("import time\ntime.sleep(30)\n", timeout=0.01)
        self.assertEqual(json.loads((self.results / "0/report.json").read_text())["status"], "failed")

    def test_missing_duplicate_and_stale_receipts_fail(self) -> None:
        self.run_case(f"print({self.log!r})\n")
        path = self.results / "0/report.json"
        original = json.loads(path.read_text())
        for key, value in (("revision", "stale"), ("children_drained", False),
                           ("expected_tests", []), ("test_threads", 4)):
            path.write_text(json.dumps({**original, key: value}))
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "evidence"):
                self.check_aggregate()
        path.write_text(json.dumps(original))
        duplicate = self.results / "duplicate"
        duplicate.mkdir()
        (duplicate / "report.json").write_text(json.dumps(original))
        with self.assertRaisesRegex(ValueError, "partition reports"):
            self.check_aggregate()
        (duplicate / "report.json").unlink()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "partition reports"):
            self.check_aggregate()


if __name__ == "__main__":
    unittest.main()
