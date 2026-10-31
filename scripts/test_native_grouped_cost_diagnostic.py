#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Attribution rejection controls; no engine execution or external input files."""
import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import native_grouped_cost_diagnostic as diagnostic
from query_surface.semantics import ResultContract, ResultType, TypedResult, result_record


class AttributionControls(unittest.TestCase):
    def setUp(self):
        self.schema = (("group_key", ResultType("int64", False)),
                       ("value_sum", ResultType("float64", True)))
        self.actual = [{"group_key": 0, "value_sum": 3.0}]
        self.reference = dict(status="passed", case_id="case", case_identity="identity",
            comparison_engine_invoked=False, reference=result_record(TypedResult(self.schema, self.actual)))
        self.job = dict(case_id="case", case_identity="identity")
        self.case = dict(case_id="case", case_identity="identity", job={"path": "job"},
                         reference_path="reference")
        self.receipt = dict(returncode=0, process_group_drained=True, direct_child_reaped=True,
                            timed_out=False, interrupted_signal=None)
        self.log = "test result: ok. 1 passed; 0 failed; 0 ignored;"
        self.observation = dict(schema_version="shardloom.grouped_cost_observation.v1",
            job=copy.deepcopy(self.job), complete_consumption=True, native_io_certified=True,
            fallback_attempted=False, external_engine_invoked=False,
            memory_grant_bytes=2 << 30, max_parallelism=2, final_reserved_bytes=0,
            denied_reservations=0, peak_reserved_bytes=100,
            source=dict(end_of_input_observed=True, output_ownership_detached=True),
            phases=[dict(phase=name, exclusive_nanos=10, calls=1, engine_classified=index < 9)
                    for index, name in enumerate(diagnostic.PHASES)],
            engine_execution_ns=100, classified_engine_ns=90, unclassified_engine_ns=10,
            actual_schema={"Struct": [{"names": ["group_key", "value_sum"],
                "dtypes": [{"Primitive": ["i64", False]}, {"Primitive": ["f64", True]}]}, False]},
            actual_rows=copy.deepcopy(self.actual))

    def validate(self):
        with patch.object(diagnostic, "read", side_effect=lambda path:
                          self.job if path == "job" else self.reference):
            return diagnostic.validate(self.case, self.observation, self.receipt, self.log,
                {"case": SimpleNamespace(contract=ResultContract(self.schema, ordered=True))})

    def test_complete_control_passes(self):
        self.assertEqual(self.validate(), self.reference["reference"])

    def test_changed_or_missing_complete_values_fail(self):
        self.observation["actual_rows"][0]["value_sum"] = 4.0
        with self.assertRaises(AssertionError):
            self.validate()
        self.observation["actual_rows"] = []
        with self.assertRaises(AssertionError):
            self.validate()

    def test_schema_nullability_drift_fails(self):
        self.observation["actual_schema"]["Struct"][0]["dtypes"][1]["Primitive"][1] = False
        with self.assertRaises(AssertionError):
            self.validate()

    def test_overlapping_or_forged_phase_accounting_fails(self):
        for key, value in (("engine_execution_ns", 80), ("classified_engine_ns", 91),
                           ("unclassified_engine_ns", 9)):
            with self.subTest(key=key):
                before = self.observation[key]
                self.observation[key] = value
                with self.assertRaises(AssertionError):
                    self.validate()
                self.observation[key] = before

    def test_missing_activation_or_invalid_phase_count_fails(self):
        for key, value in (("calls", 0), ("exclusive_nanos", -1), ("calls", True)):
            with self.subTest(key=key, value=value):
                phase = self.observation["phases"][0]
                before = phase[key]
                phase[key] = value
                with self.assertRaises(AssertionError):
                    self.validate()
                phase[key] = before

    def test_incomplete_ownership_or_fallback_fails(self):
        for key in ("complete_consumption", "native_io_certified", "fallback_attempted",
                    "external_engine_invoked"):
            with self.subTest(key=key):
                before = self.observation[key]
                self.observation[key] = not before
                with self.assertRaises(AssertionError):
                    self.validate()
                self.observation[key] = before
        self.observation["source"]["output_ownership_detached"] = False
        with self.assertRaises(AssertionError):
            self.validate()

    def test_memory_leak_and_grant_breach_fail(self):
        for key, value in (("final_reserved_bytes", 1), ("denied_reservations", 1),
                           ("peak_reserved_bytes", (2 << 30) + 1)):
            with self.subTest(key=key):
                before = self.observation[key]
                self.observation[key] = value
                with self.assertRaises(AssertionError):
                    self.validate()
                self.observation[key] = before

    def test_wrong_job_fails(self):
        self.observation["job"]["case_identity"] = "different"
        with self.assertRaises(AssertionError):
            self.validate()

    def test_failed_supervision_cannot_pass_with_complete_output(self):
        for key, value in (("returncode", 1), ("process_group_drained", False),
                           ("timed_out", True), ("interrupted_signal", 15)):
            with self.subTest(key=key):
                before = self.receipt[key]
                self.receipt[key] = value
                with self.assertRaises(AssertionError):
                    self.validate()
                self.receipt[key] = before

    def test_missing_or_duplicate_observation_fails(self):
        line = "test example ... GROUPED_COST " + json.dumps(self.observation)
        self.assertEqual(diagnostic.parse_observation(line), self.observation)
        for log in ("", line + "\n" + line):
            with self.subTest(log=log[:20]), self.assertRaises(AssertionError):
                diagnostic.parse_observation(log)


if __name__ == "__main__":
    unittest.main()
