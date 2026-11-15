"""Verify actual dictionary work alongside complete clocks, resources and source identity."""
import dictionary_support as support
from dictionary_contract import PROOF, check, coverage, work as dictionary_work
from activation_observations import inspect, successful_packet


def work(report, job):
    result = inspect(report, job)
    fields = report['fields']
    result['dictionary'] = dictionary_work(fields)
    result['provider_arrays_read'] = int(fields['local_primitive_arrays_read_count']) if 'local_primitive_arrays_read_count' in fields else None
    if result['dictionary']:
        assert 'dictionary_text_contract=v1;vortex_0.93.0_DictArray;strict_prepared_utf8_kernels;complete_domain_validation;referenced_values_only;independent_code_and_value_ownership;exact_consumer_value_equality;counts_are_owner_invocations_not_unique_query_rows;decoder_bytes_and_process_rss_unobserved;no_external_engine' in fields[PROOF]
        assert fields['local_primitive_native_io_certificate_status'] == 'certified'
        if result['dictionary']['dictionary_batches']:
            assert 'native_dictionary_text_expression' in fields['local_primitive_native_io_pushdown_accepted_operations']
    return result


def candidate_activation(result, report, row):
    if row.get('dictionary_version') == 1:
        assert check(report['fields'], coverage(row)) == result['dictionary']


def paired_work(pairs, protocol):
    declared = protocol['acceptance']['dictionary_coverage']
    wanted = [row['id'] for row in declared if row['required_work_witness']]
    assert len(wanted) == len(set(wanted)) == protocol['acceptance']['required_work_reduction_witnesses'] == 210
    by_id, witnesses = {pair['case']['id']: pair for pair in pairs}, []
    for id_ in wanted:
        pair = by_id[id_]
        row, before, after = pair['case'], pair['baseline']['work'], pair['candidate']['work']
        assert row['dictionary_version'] == 1 and coverage(row)['required_work_witness']
        assert before['dictionary'] == {}
        observed = after['dictionary']
        assert observed['dictionary_batches'] > 0
        assert observed['dictionary_kernel_calls'] + observed['row_kernel_calls'] < observed['logical_rows']
        assert observed['domain_slots'] < observed['retained_dictionary_rows']
        before_sources, after_sources = before['file_scans']['sources'], after['file_scans']['sources']
        assert len(before_sources) == len(after_sources) == 1
        assert before_sources[0]['generation_fingerprint'] == after_sources[0]['generation_fingerprint']
        witnesses.append(dict(id=id_, surface=row['surface'], profile=row['profile'], requirement=row['requirement'],
            candidate_dictionary_work=observed,
            baseline_completed_requested_read_bytes=before_sources[0]['completed_read_bytes'],
            candidate_completed_requested_read_bytes=after_sources[0]['completed_read_bytes'],
            baseline_dictionary_work_counter_available=False,
            work_scope='candidate native text-kernel calls are less than the corresponding logical expression rows; candidate dictionary values remain encoded for consumers; no baseline kernel count, zero-decode, device-I/O or process-RSS claim'))
    return witnesses
