# SPDX-License-Identifier: Apache-2.0
"""Metadata-only evidence cannot silently relax earlier projection checks."""
import unittest

from parquet_demand_completion_replay import validate_projection_work


def metadata_fields(rows=17):
    fields = {key: '0' for key in (
        'native_source_demand_projected_columns', 'native_source_demand_physical_leaf_columns',
        'native_source_demand_rows_decoded', 'native_source_demand_row_groups_read',
        'native_source_demand_data_read_bytes', 'native_source_demand_data_read_requests',
        'native_input_batches', 'native_input_batch_rows')}
    fields.update(native_source_demand_available_columns='3', native_source_demand_sources='1',
                  native_input_batch_sources='1', native_source_metadata_count_sources='1',
                  native_source_metadata_count_rows=str(rows), native_input_end_observed='true',
                  native_input_output_ownership_detached='true',
                  native_source_demand_validation_scope='metadata_only;no_data_page_or_value_validation;not_full_ingest_certification',
                  local_primitive_native_io_pushdown_accepted_operations='native_bound_scan,native_source_metadata_count_all',
                  local_primitive_native_io_pushdown_proof_basis=(
                      f'metadata_count_sources=1 metadata_count_rows={rows};'
                      'exact_nonnegative_file_and_row_group_counts;checked_u64_accumulation;'
                      'complete_held_source_inventory;unfiltered_scalar_count_all;'
                      'metadata_validation_only_no_page_or_value_validation;'
                      'no_payload_reader;metadata_and_fingerprint_admission_reads_separate;'
                      'held_generation_checked_through_final_consumer'))
    return fields


class DirectCountEvidenceTests(unittest.TestCase):
    def test_historical_projection_remains_valid_without_new_capability(self):
        fields = dict(native_source_demand_projected_columns='1', native_source_demand_available_columns='3')
        validate_projection_work(fields)
        fields['native_source_demand_projected_columns'] = '0'
        with self.assertRaises(ValueError):
            validate_projection_work(fields)

    def test_complete_metadata_evidence_supports_zero_and_exact_u64_totals(self):
        for rows in (0, 17, (1 << 53) + 1, (1 << 64) - 1):
            validate_projection_work(metadata_fields(rows))

    def test_missing_or_changed_capability_completion_and_scope_fail(self):
        for key in metadata_fields():
            with self.subTest(key=key):
                fields = metadata_fields()
                del fields[key]
                with self.assertRaises(ValueError):
                    validate_projection_work(fields)

        for key, value in (
            ('native_source_metadata_count_sources', '0'),
            ('native_source_metadata_count_sources', '2'),
            ('native_input_batch_sources', '2'),
            ('native_source_metadata_count_rows', str(1 << 64)),
            ('native_source_metadata_count_rows', '1'),
            ('native_source_demand_available_columns', '0'),
            ('native_source_demand_physical_leaf_columns', '1'),
            ('native_source_demand_rows_decoded', '17'),
            ('native_source_demand_data_read_requests', '1'),
            ('native_input_batches', '1'),
            ('native_input_end_observed', 'false'),
            ('native_input_output_ownership_detached', 'false'),
            ('local_primitive_native_io_pushdown_accepted_operations', 'native_bound_scan'),
            ('native_source_demand_validation_scope', 'full_source_values'),
        ):
            with self.subTest(key=key, value=value):
                fields = metadata_fields()
                fields[key] = value
                with self.assertRaises(ValueError):
                    validate_projection_work(fields)

    def test_each_proof_token_is_required_as_a_complete_token(self):
        key = 'local_primitive_native_io_pushdown_proof_basis'
        proof = metadata_fields()[key]
        for token in proof.replace(';', ' ').split():
            for replacement in ('', 'not_' + token, token + '_unproven'):
                with self.subTest(token=token, replacement=replacement):
                    fields = metadata_fields()
                    fields[key] = proof.replace(token, replacement)
                    with self.assertRaises(ValueError):
                        validate_projection_work(fields)


if __name__ == '__main__':
    unittest.main()
