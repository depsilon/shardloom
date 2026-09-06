# SPDX-License-Identifier: Apache-2.0
import copy
from pathlib import Path
import unittest

from run_native_output_uat import expected_output, export_args, validate_array_evidence, validate_output
from run_heldout_operator_uat import fixture_rows
from test_heldout_operator_uat import envelope


class NativeOutputAcceptanceTests(unittest.TestCase):
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
