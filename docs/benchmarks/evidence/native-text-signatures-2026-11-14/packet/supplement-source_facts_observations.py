"""Verify finite-source admission and actual encoded comparisons with complete resource evidence."""
import source_facts_support as support
from source_facts_contract import PROOF, check, coverage
from activation_observations import inspect, successful_packet


def work(report, job):
    result = inspect(report, job)
    fields = report['fields']
    observed = support.source_facts_cases.literals.observations(fields.get(PROOF))
    result['encoded_compare'] = observed
    result['provider_arrays_read'] = int(fields['local_primitive_arrays_read_count']) if 'local_primitive_arrays_read_count' in fields else None
    if observed:
        assert fields['local_primitive_native_io_certificate_status'] == 'certified'
        scope = observed['native_encoded_compare_scope']
        assert scope in {'exclusive_operation', 'unavailable'}
        counters = {name for name in observed if name.endswith(('_calls', '_rows'))}
        expected = {'native_encoded_compare_' + family + '_' + unit
                    for family in ('alp', 'bitpacked', 'for') for unit in ('calls', 'rows')}
        assert counters == (expected if scope == 'exclusive_operation' else set())
        if scope == 'exclusive_operation':
            assert 'native_encoded_compare_scope=exclusive_operation;successful_adaptor_dispatches;logical_operand_rows;not_unique_rows;decoder_bytes_unobserved;vortex_0.93.0' in fields[PROOF]
        if observed.get('source_footer_finite_float_scan_invocations', 0):
            assert 'source_footer_finite_float_predicate' in fields['local_primitive_native_io_pushdown_accepted_operations']
    return result


def candidate_activation(result, report, row):
    if row.get('source_facts_version') == 1:
        assert check(report['fields'], coverage(row)) == result['encoded_compare']


def paired_work(pairs, protocol):
    declared = protocol['acceptance']['source_fact_coverage']
    assert sum(row['required_finite_admission'] for row in declared) == protocol['acceptance']['required_finite_admission_witnesses'] == 620
    wanted = [row for row in declared if row['required_dispatch_family'] is not None]
    assert len(wanted) == len({row['id'] for row in wanted}) == protocol['acceptance']['required_actual_encoded_dispatch_witnesses'] == 150
    by_id, witnesses = {pair['case']['id']: pair for pair in pairs}, []
    for declaration in wanted:
        pair = by_id[declaration['id']]
        row, before, after = pair['case'], pair['baseline']['work'], pair['candidate']['work']
        assert row['source_facts_version'] == 1 and coverage(row) == declaration
        assert before['encoded_compare'] == {}
        observed = after['encoded_compare']
        family = declaration['required_dispatch_family']
        assert observed['native_encoded_compare_' + family + '_calls'] > 0
        assert observed['native_encoded_compare_' + family + '_rows'] > 0
        if family == 'alp':
            assert observed['source_footer_finite_float_scan_invocations'] > 0
        before_sources, after_sources = before['file_scans']['sources'], after['file_scans']['sources']
        assert len(before_sources) == len(after_sources) == 1
        assert before_sources[0]['generation_fingerprint'] == after_sources[0]['generation_fingerprint']
        witnesses.append(dict(id=row['id'], surface=row['surface'], profile=row['profile'], requirement=row['requirement'],
            required_dispatch_family=family, candidate_encoded_compare_work=observed,
            baseline_completed_requested_read_bytes=before_sources[0]['completed_read_bytes'],
            candidate_completed_requested_read_bytes=after_sources[0]['completed_read_bytes'],
            baseline_encoded_compare_counter_available=False,
            work_scope='successful native compressed comparison adaptor calls on a bound source; repeated/nested logical operand rows remain counted; baseline encoded work was unobserved; no decoded-byte, unique-row or avoided-read claim'))
    return witnesses
