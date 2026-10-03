# SPDX-License-Identifier: Apache-2.0
"""Regression checks for ambiguous retained native report fields."""
import unittest

from run_native_unary_uat import require_unique_report_fields


class NativeReportEvidenceTests(unittest.TestCase):
    def test_repeated_report_fields_are_denied_even_when_values_agree(self):
        for key, values in [
            ("fallback_attempted", ["false", "false"]),
            ("external_engine_invoked", ["false", "false"]),
            ("local_primitive_no_query_answer_cache", ["true", "false"]),
        ]:
            with self.subTest(key=key):
                raw = {"fields": [{"key": key, "value": value} for value in values]}
                with self.assertRaisesRegex(ValueError, key):
                    require_unique_report_fields(raw)

    def test_distinct_field_names_can_share_values_without_being_modified(self):
        raw = {"fields": [{"key": key, "value": "false"} for key in
                          ["fallback_attempted", "external_engine_invoked"]]}
        before = repr(raw)
        require_unique_report_fields(raw)
        self.assertEqual(repr(raw), before)


if __name__ == "__main__":
    unittest.main()
