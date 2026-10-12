# SPDX-License-Identifier: Apache-2.0
import copy
from pathlib import Path
import unittest

from native_output_reuse import REPETITIONS, bind_reuse_case, reuse_cases, reuse_comparisons, validate_reuse
from run_native_output_uat import expected_output, export_args, validate_array_evidence, validate_output
from run_heldout_operator_uat import fixture_rows
from native_workflow_test_support import structured_envelope as envelope


class NativeOutputAcceptanceTests(unittest.TestCase):
    def test_reuse_proof_requires_real_metadata_and_payload_control(self):
        cases = reuse_cases(expected_output(fixture_rows(64), 64))
        for case in cases:
            report = envelope(case["expected"])
            report["fields"].extend([
                {"key": "local_primitive_arrays_read_count", "value": "0"},
                {"key": "local_primitive_aggregate_update_strategy", "value": "exact_file_footer_scalar_completion"},
                {"key": "relational_scan_rows_delivered", "value": "64"},
                {"key": "public_workflow_route_id", "value": "native_vortex_filter_project"},
                {"key": "resident_completed_executions", "value": "1"},
            ])
            if case["name"] == "range_prune":
                report["fields"] = [field for field in report["fields"]
                                    if not field["key"].startswith(("local_primitive_", "relational_"))]
                evidence = validate_reuse(report, case, candidate=True)
                self.assertNotIn("local_primitive_arrays_read_count", evidence)
                broken = copy.deepcopy(report)
                broken["fields"][-1]["value"] = "0"
                with self.assertRaises(ValueError):
                    validate_reuse(broken, case, candidate=True)
                continue
            validate_reuse(report, case, candidate=True)
            key = ("relational_scan_rows_delivered" if case["name"] == "scan_control"
                   else "local_primitive_arrays_read_count")
            for count in ("١", "-1", "1.5", "", "0" if case["name"] == "scan_control" else "1"):
                broken = copy.deepcopy(report)
                next(field for field in broken["fields"] if field["key"] == key)["value"] = count
                with self.subTest(case=case["name"], count=count), self.assertRaises(ValueError):
                    validate_reuse(broken, case, candidate=True)
            missing = copy.deepcopy(report)
            missing["fields"] = [field for field in missing["fields"] if field["key"] != key]
            with self.assertRaises(ValueError):
                validate_reuse(missing, case, candidate=True)
        aggregate = cases[0]
        self.assertEqual(aggregate["expected"], [{"n": 64, "lo": 0, "hi": 63, "present": 64}])
        self.assertEqual([row["shipment_sequence"] for row in cases[2]["expected"]], list(range(63, 56, -1)))

    def test_reuse_binds_exact_file_for_all_public_sql_routes(self):
        path = Path("/tmp/later job's output.vortex")
        for case in reuse_cases(expected_output(fixture_rows(64), 64)):
            before = copy.deepcopy(case)
            bound = bind_reuse_case(case, path)
            self.assertEqual(bound["expected"], case["expected"])
            self.assertEqual(case, before)
            self.assertIn("FROM '/tmp/later job''s output.vortex'", bound["sql"])
            self.assertNotIn("FROM heldout", bound["sql"])
        with self.assertRaises(ValueError):
            bind_reuse_case({"sql": "SELECT 1"}, path)

    def test_lifecycle_gate_rejects_hidden_write_cost_and_missing_samples(self):
        records = [{"rows": 64, "sample": sample, "warmup": False, "variant": variant,
                    "seconds": 0.1, "passed": True, "output": {"bytes": 4096},
                    "reuse": [{"job": job, "repeat": repeat, "seconds": 0.05, "passed": True}
                              for repeat in range(REPETITIONS)
                              for job in ("integer_aggregate", "range_prune", "scan_control")]}
                   for sample in range(1, 6) for variant in ("baseline", "candidate")]
        self.assertTrue(reuse_comparisons(records, [64])["64"]["passed"])
        costly = copy.deepcopy(records)
        for record in costly:
            if record["variant"] == "candidate":
                record["seconds"] = 0.3
        outcome = reuse_comparisons(costly, [64])["64"]
        self.assertFalse(outcome["passed"])
        self.assertFalse(outcome["metrics"]["repeated_lifecycle"]["passed"])
        for broken in (records[:-1], records[:8]):
            with self.assertRaises(ValueError):
                reuse_comparisons(broken, [64])
        for value in (float("nan"), float("inf"), -0.1, True):
            broken = copy.deepcopy(records)
            broken[0]["seconds"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                reuse_comparisons(broken, [64])
        broken = copy.deepcopy(records)
        broken[0]["reuse"].pop()
        with self.assertRaises(ValueError):
            reuse_comparisons(broken, [64])

    def test_required_native_path_cannot_pass_with_legacy_path_or_wrong_checksum(self):
        fields = {
            "native_vortex_result_export_kind": "owned_native_array_stream",
            "native_vortex_array_sink_adapter_payload_bytes_copied": "0",
            "native_vortex_array_sink_scalar_values_materialized": "0",
            "native_vortex_array_sink_source_generation_validated": "true",
            "native_vortex_array_sink_dtype_and_row_count_validated": "true",
            "native_vortex_array_sink_output_sha256": "a" * 64,
            "native_vortex_array_sink_arrays_submitted": "5",
            "native_vortex_array_sink_writer_input_batch_bound": "3",
            "arrow_converted": "false",
        }
        validate_array_evidence(fields, "a" * 64)
        for key, bad in [("native_vortex_result_export_kind", "primitive_row_stream"),
                         ("native_vortex_array_sink_output_sha256", "b" * 64),
                         ("native_vortex_array_sink_arrays_submitted", "0"),
                         ("native_vortex_array_sink_source_generation_validated", "false"),
                         ("native_vortex_array_sink_scalar_values_materialized", "1")]:
            with self.assertRaises(ValueError):
                validate_array_evidence({**fields, key: bad}, "a" * 64)

    def test_complete_reopened_values_reject_loss_rounding_reordering_and_fallback(self):
        expected = expected_output(fixture_rows(64), 64)
        validate_output(envelope(expected), expected)
        rounded = copy.deepcopy(expected)
        rounded[-1]["exact_identifier"] = float(rounded[-1]["exact_identifier"])
        wrong_null = copy.deepcopy(expected)
        wrong_null[0]["renamed_label"] = "null"
        for actual in (expected[:-1], expected[::-1], rounded, wrong_null):
            with self.assertRaises(ValueError):
                validate_output(envelope(actual), expected)
        fallback = envelope(expected)
        fallback["fallback"]["attempted"] = True
        with self.assertRaises(ValueError):
            validate_output(fallback, expected)

    def test_fixture_projection_preserves_exact_nulls_and_aliases(self):
        expected = expected_output(fixture_rows(64), 2)
        self.assertEqual(expected, [
            {"renamed_label": None, "shipment_sequence": 0, "exact_identifier": -(2**63)},
            {"renamed_label": "東京", "shipment_sequence": 1, "exact_identifier": 2**60 + 1},
        ])
        command = export_args(Path("/tmp/source.vortex"), Path("/tmp/result.vortex"), 2, 4)
        self.assertEqual(command[command.index("--request") + 1], "write_vortex")
        self.assertEqual(command[command.index("--vortex-source-order-limit") + 1], "2")
        self.assertEqual(command[command.index("--max-parallelism") + 1], "4")


if __name__ == "__main__":
    unittest.main()
