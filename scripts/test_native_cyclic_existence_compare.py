# SPDX-License-Identifier: Apache-2.0
"""Adversarial checks of comparison completeness, native clocks and fixed gates."""
import copy
import unittest

import native_cyclic_existence_compare as compare
import native_cyclic_existence_workload as workload
from query_timing_report import TIMING_BOUNDARY, TIMING_CLOCK


class ComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.protocol = {"comparison": compare.comparison_contract(), "workload": workload.manifest()}
        cls.expected = {cell["case"]: cell for cell in cls.protocol["workload"]["cells"]}

    def workflow(self, cell):
        expected = self.expected[cell["case"]]
        engine = 1.0 if cell["role"] == "ordinary" else (0.95 if cell["case"] in workload.TARGETS else 1.02)
        calls = [{"call": index, "ordered_values_equal": True, "collect_seconds": engine + 0.1,
                  "engine_execution_seconds": engine,
                  "result": {key: expected["expected"][key] for key in ("rows", "sha256")},
                  "schema": expected["expected"]["schema"], "delivery": {"complete": True},
                  "after_result_release": {"reserved_bytes": 200},
                  "report": {"external_engine_invoked": "false", "fallback_attempted": "false",
                             "resident_relational_declaration_reused": "true",
                             "native_query_timing_reported": "true",
                             "native_query_timing_clock": TIMING_CLOCK,
                             "native_query_timing_boundary": TIMING_BOUNDARY,
                             "native_query_execution_nanos": str(round(engine * 1e9)),
                             "native_result_serialization_nanos": "0"}}
                 for index in range(cell["calls"])]
        return {"cell": cell, "calls": calls,
                "sources": {name: {key: source[key] for key in ("rows", "sha256")}
                            for name, source in expected["sources"].items()},
                "initial": {"reserved_bytes": 0}, "final": {"reserved_bytes": 0},
                "source_credits": 100, "prepared_credits": 200,
                "after_prepared_release": {"reserved_bytes": 100},
                "engine_execution_seconds": sum(call["engine_execution_seconds"] for call in calls),
                "complete_workflow_seconds": 2.0 * cell["calls"]}

    def workflows(self):
        return [self.workflow(cell) for cell in self.protocol["comparison"]["schedule"]]

    def test_fixed_schedule_preserves_every_case_call_role_sample_and_cohort(self):
        schedule = self.protocol["comparison"]["schedule"]
        self.assertEqual(len(schedule), 520)
        self.assertEqual(sum(cell["calls"] for cell in schedule), 1300)
        self.assertEqual(len({tuple(cell.items()) for cell in schedule}), 520)
        self.assertEqual(schedule[0], dict(cohort="A", case="abundant", calls=1, sample=0, role="ordinary"))
        self.assertEqual(schedule[260], dict(cohort="B", case="cyclic_small", calls=4, sample=0, role="ordinary"))
        decision = compare.score(self.workflows(), self.protocol)
        self.assertTrue(decision["passed"])
        self.assertEqual(len(decision["cells"]), 52)
        self.assertEqual(decision["engine_call_count"], 1300)

    def test_missing_reordered_or_duplicate_workflows_cannot_be_scored(self):
        for changed in [lambda rows: rows.pop(), lambda rows: rows.__delitem__(slice(260, None)),
                        lambda rows: rows.__setitem__(0, rows[1]),
                        lambda rows: rows.reverse()]:
            rows = self.workflows()
            changed(rows)
            with self.assertRaises(ValueError):
                compare.score(rows, self.protocol)

    def test_values_schema_sources_clocks_and_credit_corruptions_are_rejected(self):
        cell = self.protocol["comparison"]["schedule"][0]
        changes = [
            lambda row: row["calls"][0]["result"].update(sha256="0" * 64),
            lambda row: row["calls"][0].update(schema={}),
            lambda row: row["calls"][0]["delivery"].update(complete=False),
            lambda row: row["calls"][0].update(ordered_values_equal=False),
            lambda row: row["sources"]["r"].update(rows=0),
            lambda row: row["final"].update(reserved_bytes=1),
            lambda row: row["after_prepared_release"].update(reserved_bytes=200),
            lambda row: row["calls"][0]["after_result_release"].update(reserved_bytes=100),
            lambda row: row["calls"][0]["report"].update(external_engine_invoked="true"),
            lambda row: row["calls"][0]["report"].update(native_query_timing_clock="cpu"),
            lambda row: row["calls"][0].update(engine_execution_seconds=0.5),
            lambda row: row["calls"][0].update(collect_seconds=0.5),
            lambda row: row.update(engine_execution_seconds=0.5),
            lambda row: row.update(complete_workflow_seconds=0.5),
            lambda row: row.update(complete_workflow_seconds=float("nan")),
            lambda row: row.update(engine_execution_seconds=float("inf")),
        ]
        for index, change in enumerate(changes):
            row = copy.deepcopy(self.workflow(cell))
            change(row)
            with self.subTest(index=index), self.assertRaises(ValueError):
                compare.verify_workflow(row, cell, self.expected[cell["case"]])

    def test_one_control_in_one_cohort_prevents_retention(self):
        rows = self.workflows()
        for row in rows:
            cell = row["cell"]
            if (cell["cohort"], cell["case"], cell["calls"], cell["role"]) == ("B", "star_control", 1, "candidate"):
                row["calls"][0]["report"]["native_query_execution_nanos"] = "1100000000"
                row["calls"][0]["engine_execution_seconds"] = 1.1
                row["engine_execution_seconds"] = 1.1
        decision = compare.score(rows, self.protocol)
        self.assertFalse(decision["passed"])
        failures = [row for row in decision["cells"] if not row["engine_pass"]]
        self.assertEqual([(row["cohort"], row["case"], row["calls"]) for row in failures], [("B", "star_control", 1)])

    def test_median_uses_all_five_samples_and_complete_workflow_gate(self):
        rows = self.workflows()
        for row in rows:
            cell = row["cell"]
            if (cell["cohort"], cell["case"], cell["calls"], cell["role"]) == ("A", "abundant", 1, "candidate"):
                engine = 0.01 if cell["sample"] == 0 else 1.0
                row["calls"][0]["report"]["native_query_execution_nanos"] = str(round(engine * 1e9))
                row["calls"][0]["engine_execution_seconds"] = engine
                row["engine_execution_seconds"] = engine
            if (cell["cohort"], cell["case"], cell["calls"], cell["role"]) == ("A", "rare", 4, "candidate"):
                row["complete_workflow_seconds"] = 8.5
        decision = compare.score(rows, self.protocol)
        self.assertFalse(decision["passed"])
        target = next(row for row in decision["cells"] if (row["cohort"], row["case"], row["calls"]) == ("A", "abundant", 1))
        self.assertEqual(target["medians"]["candidate"]["engine_execution_seconds"], 1.0)
        self.assertFalse(target["engine_pass"])
        target = next(row for row in decision["cells"] if (row["cohort"], row["case"], row["calls"]) == ("A", "rare", 4))
        self.assertTrue(target["engine_pass"])
        self.assertFalse(target["complete_workflow_pass"])

    def test_exact_gates_are_inclusive_without_an_absolute_noise_allowance(self):
        rows = self.workflows()
        for row in rows:
            if row["cell"]["role"] != "candidate":
                continue
            engine = 0.98 if row["cell"]["case"] in workload.TARGETS else 1.05
            for call in row["calls"]:
                call["engine_execution_seconds"] = engine
                call["report"]["native_query_execution_nanos"] = str(round(engine * 1e9))
            row["engine_execution_seconds"] = sum(call["engine_execution_seconds"] for call in row["calls"])
            row["complete_workflow_seconds"] = 2.1 * row["cell"]["calls"]
        self.assertTrue(compare.score(rows, self.protocol)["passed"])
        for row in rows:
            cell = row["cell"]
            if (cell["cohort"], cell["case"], cell["calls"], cell["role"]) == ("B", "semi_small", 1, "candidate"):
                row["calls"][0]["engine_execution_seconds"] = 1.050000001
                row["calls"][0]["report"]["native_query_execution_nanos"] = "1050000001"
                row["engine_execution_seconds"] = 1.050000001
        self.assertFalse(compare.score(rows, self.protocol)["passed"])

    def test_relaxed_prospective_gate_is_rejected_even_without_file_validation(self):
        changed = copy.deepcopy(self.protocol)
        changed["comparison"]["control_engine_ratio_max"] = 1.06
        with self.assertRaises(ValueError):
            compare.score(self.workflows(), changed)


if __name__ == "__main__":
    unittest.main()
