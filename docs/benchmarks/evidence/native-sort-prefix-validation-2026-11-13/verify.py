"""Verify portable Sort prefix evidence without queries or the full oracle."""
from collections import Counter, defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile

BASELINE = '4d5fa4a46268380a634e78f057abba087f4042f5'
EXPECTED_GROUPS = dict(owned_source_control=10, vortex_no_file_kernel=321,
    vortex_other_file_kernel=36, vortex_reported_file_sort=21)
CHECKS = {
    'prefix-fmt-r1.json', 'prefix-default-clippy-r1.json', 'prefix-native-clippy-r1.json',
    'prefix-default-tests-r1.json', 'prefix-native-tests-r1.json', 'prefix-installed-wheel-r1.json',
}
RUST_FILES = {'shardloom-vortex/src/local_primitives/'+name for name in (
    'native_payload.rs', 'native_payload_validation.rs', 'native_relational_batch.rs',
    'native_relational_sort.rs', 'native_relational_sort_tests.rs',
    'native_relational_sort_payload_prefix_tests.rs',
    'native_relational_sort_payload_prefix_rich_tests.rs')}
REQUIRED_TESTS = {
    'native_relational_sort_prefix_'+name for name in (
        'accepts_empty_rich_inputs_without_output',
        'bounds_resident_nested_and_decimal_output',
        'ignores_masked_and_unreferenced_invalid_children',
        'keeps_complete_cast_and_nullability_validation',
        'keeps_consumer_failure_before_later_rich_error',
        'keeps_discarded_extension_and_variant_errors',
        'keeps_discarded_utf8_failure_coordinates',
        'keeps_rich_error_order_across_sorted_batches',
        'keeps_rich_types_values_masks_and_last_owner_credit',
        'keeps_unobserved_payload_policy_barriers',
        'preserves_float16_bits_and_common_type_promotion',
        'reuses_complete_typed_validation_for_flat_payloads',
        'rich_late_denial_and_cancellation_refund_credits')}


def identity(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def clock(value):
    assert type(value) in (int,float) and math.isfinite(value) and value >= 0
    return value


def safe_name(name):
    path = PurePosixPath(name)
    assert not path.is_absolute() and '..' not in path.parts and str(path) == name


def rows(raw):
    return [json.loads(line) for line in gzip.decompress(raw).splitlines()]


def pipeline(fields):
    assert fields['fallback_attempted'] == fields['external_engine_invoked'] == 'false'
    assert fields['relational_provider_selection_reason'] == 'native_operator_pipeline'
    proof = fields['local_primitive_native_io_pushdown_proof_basis']
    assert 'native_execution_pipeline=native_operator_pipeline' in proof
    assert 'native_pipeline_catch_all_execution=false' in proof
    result = {}
    for key in ('native_pipeline_operator_invocations','native_pipeline_file_kernel_invocations'):
        found = re.findall(r'\b'+key+r'=(\d+)\b',proof)
        assert len(found) == 1
        result[key] = int(found[0])
    assert result['native_pipeline_operator_invocations'] > 0
    assert 0 <= result['native_pipeline_file_kernel_invocations'] <= result['native_pipeline_operator_invocations']
    return dict(common_pipeline=True,catch_all=False,external_fallback=False,
        reported_file_sort='local_primitive_sort_provider_background_workers' in fields,**result)


def counters(fields):
    pairs = re.findall(r'(?:^|[ ;])([a-z][a-z0-9_]*)=([0-9]+)(?=$|[ ;])',
        fields['local_primitive_native_io_pushdown_proof_basis'])
    assert len(pairs) == len(dict(pairs))
    result = {'proof.'+key:int(value) for key,value in pairs}
    result.update({'report.'+key:int(value) for key,value in fields.items()
                   if re.fullmatch(r'[0-9]+',str(value))})
    return result


def verify(root):
    if not __debug__:
        raise RuntimeError('verification requires Python assertions; do not use -O')
    manifest_raw = (root/'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    assert manifest['schema'] == 'shardloom.shared_sort_prefix_validation_evidence.v1'
    assert manifest['baseline_source_commit'] == BASELINE
    actual = {str(path.relative_to(root)) for path in root.rglob('*') if path.is_file()}
    assert actual <= set(manifest['files']) | {'manifest.json','verification.json'}
    assert not any(path.is_symlink() for path in root.rglob('*'))
    for name, expected in manifest['files'].items():
        safe_name(name)
        assert identity((root/name).read_bytes()) == expected, name
    saved = {}
    with tarfile.open(root/'observations.tar.gz','r:gz') as archive:
        members = archive.getmembers()
        assert len(members) == len({item.name for item in members})
        assert {item.name for item in members} == set(manifest['archive_members'])
        for member in members:
            safe_name(member.name)
            assert member.isfile()
            raw = archive.extractfile(member).read()
            expected = manifest['archive_members'][member.name]
            assert identity(raw) == {key:expected[key] for key in ('bytes','sha256')}
            saved[member.name] = raw

    def read(name):
        return json.loads(saved[name])

    def linked(record, name):
        assert name in saved, name
        assert record['sha256'] == identity(saved[name])['sha256'], name
        if 'bytes' in record:
            assert record['bytes'] == len(saved[name]), name
        elif 'generation' in record:
            assert record['generation']['bytes'] == len(saved[name]), name

    origins = {record['origin']:name for name,record in manifest['archive_members'].items()}
    cohort_name = 'current/prefix-sort-cohort-r1.json'
    cohort = read(cohort_name)
    assert cohort['schema'] == 'shardloom.shared_sort_prefix_candidate_cohort.v1'
    assert cohort['status'] == 'frozen_candidate_current_baseline_bound'
    assert cohort['case_count'] == len(cohort['cases']) == 388
    assert cohort['original_declarations_verified'] == 10000
    assert cohort['query_executions'] == cohort['repeats'] == cohort['warmups'] == 0
    assert cohort['attempts_per_case'] == 1 and cohort['primary_clock'] == 'engine_execution_seconds'
    assert cohort['classification_only'] is True
    assert cohort['classification_groups'] == EXPECTED_GROUPS
    assert cohort['baseline_source_commit'] == BASELINE
    assert cohort['baseline_source_tree'] == '9932e68ac3592e02f33ea1a25d1d5cb597d42754'
    assert cohort['current_baseline_status_counts'] == {'passed':388}
    assert cohort['current_baseline_engine_seconds'] == 337.804658081
    assert cohort['runtime_import_assertion'] == dict(native_imported=False,polars_imported=False)
    external = manifest['external_artifacts']
    assert len(external) == 1
    ledger_identity = external[0]
    assert ledger_identity['path'].endswith('/payload-cohort-r1/attempts.sqlite')
    assert ledger_identity in cohort['source_identities']
    assert not any(name.endswith('attempts.sqlite') for name in saved)
    for record in cohort['source_identities']:
        if record == ledger_identity:
            continue
        assert record['path'] in origins
        linked(record,origins[record['path']])

    baseline_summary = read('current/payload-cohort-r1/verified-summary-r1.json')
    baseline_complete = read('current/payload-cohort-r1/complete.json')
    baseline_protocol = read('current/payload-cohort-protocol-r1.json')
    baseline_certificates = rows(saved['current/payload-cohort-r1/certificate-cases-r1.jsonl.gz'])
    baseline_cases = rows(saved['current/payload-cohort-r1/verified-cases-r1.jsonl.gz'])
    assert len(baseline_cases) == len(baseline_certificates) == 388
    assert baseline_summary['status'] == 'passed_offline_replay'
    assert baseline_summary['source_commit'] == BASELINE
    assert baseline_summary['source_tree'] == cohort['baseline_source_tree']
    assert baseline_summary['successful_cases'] == baseline_summary['attempted_cases'] == 388
    assert baseline_summary['unsuccessful_cases'] == 0
    assert baseline_summary['outcomes'] == {'passed':388}
    assert baseline_summary['successful_engine_seconds'] == 337.804658081
    assert baseline_summary['complete_candidate_engine_seconds'] == 337.804658081
    assert baseline_summary['independent_complete_results_replayed'] is True
    assert baseline_complete['status'] == 'complete_observation'
    assert baseline_complete['posthash_verified']
    assert baseline_complete['total'] == baseline_complete['attempt_slots'] == 388
    assert baseline_complete['ledger'] == baseline_summary['ledger']
    assert baseline_complete['ledger']['path'] == ledger_identity['path']
    assert ledger_identity['bytes'] == baseline_summary['ledger']['generation']['bytes']
    assert ledger_identity['sha256'] == baseline_summary['ledger']['sha256']
    assert baseline_complete['updated'] == baseline_protocol['runtime_binding'] == cohort['runtime_binding']
    assert baseline_complete['updated']['head'] == BASELINE
    for key, name in (
        ('case_rows','current/payload-cohort-r1/verified-cases-r1.jsonl.gz'),
        ('certificates','current/payload-cohort-r1/certificate-cases-r1.jsonl.gz'),
        ('protocol','current/payload-cohort-protocol-r1.json')):
        linked(baseline_summary[key], name)

    design = read('original/design.json')
    assert design['count'] == len(design['cases']) == 10000
    protocol_name = 'current/prefix-cohort-protocol-r1.json'
    complete_name = 'current/prefix-cohort-r1/complete.json'
    summary_name = 'current/prefix-cohort-r1/verified-summary-r1.json'
    case_name = 'current/prefix-cohort-r1/verified-cases-r1.jsonl.gz'
    certificate_name = 'current/prefix-cohort-r1/certificate-cases-r1.jsonl.gz'
    protocol, complete, summary = read(protocol_name), read(complete_name), read(summary_name)
    candidate, certificates = rows(saved[case_name]), rows(saved[certificate_name])
    assert manifest['source_commit'] == summary['source_commit']
    assert summary['status'] == 'passed_offline_replay' and summary['query_executions'] == 0
    assert summary['attempted_cases'] == summary['successful_cases'] == 388
    assert summary['unsuccessful_cases'] == 0 and summary['outcomes'] == {'passed':388}
    assert summary['independent_complete_results_replayed'] is True
    assert summary['no_denied_reservations'] and summary['no_spill']
    assert summary['primary_clock'] == protocol['primary_clock'] == 'engine_execution_seconds'
    assert summary['attempts_per_case'] == complete['attempts_per_case'] == 1
    assert summary['repeated_baseline_queries'] == complete['requested_repeats'] == 0
    assert complete['status'] == 'complete_observation' and complete['posthash_verified']
    assert complete['total'] == complete['attempt_slots'] == 388
    assert complete['updated'] == protocol['runtime_binding']
    assert complete['updated']['head'] == summary['source_commit']
    assert complete['updated']['source_tree'] == summary['source_tree']
    assert protocol['status'] == 'admitted_before_execution'
    assert protocol['expected_attempt_count'] == protocol['required_complete_result_count'] == 388
    assert protocol['attempts_per_case'] == 1 and protocol['warmups'] == 0
    assert protocol['runtime_binding']['baseline']['source_commit'] == BASELINE
    assert protocol['runtime_binding']['baseline']['engine_seconds'] == 337.804658081
    linked(cohort['protocol_identity'],origins[cohort['protocol_identity']['path']])
    linked(protocol['cohort'],cohort_name)
    linked(protocol['design'],'original/design.json')
    linked(protocol['inputs'],'original/inputs.json')
    linked(protocol['frozen'],'original/frozen.json')
    linked(summary['cohort'],cohort_name)
    linked(summary['protocol'],protocol_name)
    linked(summary['case_rows'],case_name)
    linked(summary['certificates'],certificate_name)
    linked(manifest['cohort_summary'],summary_name)
    linked(manifest['cohort'],cohort_name)

    supervision_name = 'current/prefix-cohort-supervision-r1.json'
    supervision = read(supervision_name)
    assert supervision['status'] == 'passed_capture' and supervision['returncode'] == 0
    assert supervision['process_group_drained'] and supervision['direct_child_reaped']
    assert supervision['runtime_and_source_unchanged'] and not supervision['timed_out']
    assert supervision['interrupted_signal'] is None
    linked(supervision['log_identity'],'current/prefix-cohort-supervision-r1.log')
    linked(supervision['complete'],complete_name)
    for path, record in supervision['drivers'].items():
        assert path in origins
        linked(record,origins[path])

    work_review_name = 'current/prefix-work-review-r1.json'
    work_review = read(work_review_name)
    assert work_review['status'] == 'passed_saved_work_review' and work_review['query_executions'] == 0
    assert work_review['cases'] == 388
    assert work_review['source_commit'] == summary['source_commit']
    assert work_review['baseline_source_commit'] == BASELINE
    for record in work_review['source_identities']:
        assert record['path'] in origins
        linked(record,origins[record['path']])

    groups, counts, seen, baseline_clocks, candidate_clocks = defaultdict(list), Counter(), set(), [], []
    review_groups, review_changes, review_details = defaultdict(Counter), Counter(), []
    for seq, (row, certificate, declared) in enumerate(zip(candidate,certificates,cohort['cases'])):
        baseline = declared['current_baseline']
        item, group = declared['declaration'], declared['historical_classification_group']
        baseline_certificate = baseline['certificate']
        saved_baseline = baseline_cases[seq]
        saved_certificate = baseline_certificates[seq]
        assert saved_baseline['seq'] == saved_certificate['seq'] == seq
        assert saved_baseline['id'] == saved_certificate['id'] == item['id']
        assert saved_baseline['case_identity'] == declared['case_identity']
        assert saved_baseline['original_ordinal'] == declared['original_ordinal']
        assert saved_baseline['comparison_group'] == group
        assert baseline_certificate == {key:value for key,value in saved_certificate.items()
                                        if key not in {'seq','id'}}
        assert baseline['resources'] == saved_baseline['resources']
        assert baseline['work'] == saved_baseline['current']['work']
        assert baseline['engine_execution_seconds'] == saved_baseline['current']['engine_seconds']
        assert baseline['workflow_seconds'] == saved_baseline['current']['workflow_seconds']
        assert baseline['actual_rows'] == saved_baseline['current']['actual_rows']
        assert baseline['ledger'] == dict(path=ledger_identity['path'],table='attempt',sequence=seq)
        assert baseline['saved_attempt']['sequence'] == seq
        assert baseline['saved_attempt']['status'] == baseline['saved_attempt']['packet_classified_status'] == 'passed'
        assert baseline['saved_attempt']['files']['one.stdout.json']['sha256'] == saved_certificate['raw_stdout_sha256']
        assert baseline['protocol']['path'] == cohort['protocol_identity']['path']
        assert baseline['protocol']['runtime_binding'] == baseline_protocol['runtime_binding']
        assert baseline['protocol']['schema'] == baseline_protocol['schema']
        assert baseline['protocol']['status'] == baseline_protocol['status']
        assert baseline['source_commit'] == row['previous_source_commit'] == BASELINE
        assert baseline['source_tree'] == cohort['baseline_source_tree']
        assert baseline['status'] == baseline_certificate['status'] == 'passed'
        assert baseline_certificate['engine_timing_reported'] is True
        assert row['seq'] == certificate['seq'] == declared['candidate_order'] == seq
        assert row['id'] == certificate['id'] == item['id'] and row['id'] not in seen
        seen.add(row['id'])
        assert row['case_identity'] == declared['case_identity'] == item['case_identity']
        assert design['cases'][declared['original_ordinal']] == item
        assert row['original_ordinal'] == declared['original_ordinal']
        assert row['comparison_group'] == group
        assert row['family'] == item['family'] and row['profile'] == item['profile']
        assert row['surface'] == declared['public_surface'] == item['surface']
        assert row['native_format'] == declared['native_format'] == item['native_format']
        expected = declared['submitted_workload']['saved_public_job']['expected']
        before = clock(baseline['engine_execution_seconds'])
        after = clock(certificate['engine_seconds'])
        assert before == clock(baseline_certificate['engine_seconds']) == clock(row['previous_engine_seconds'])
        assert after == clock(row['current']['engine_seconds'])
        assert row['current']['status'] == certificate['status'] == 'passed'
        assert row['current']['actual_rows'] == baseline['actual_rows'] == len(expected['rows'])
        assert certificate['engine_timing_reported'] is True
        assert row['previous_work'] == baseline['work']
        assert row['previous_ledger'] == baseline['ledger']['path']
        assert row['current']['work'] is not None
        old_fields, fields = baseline_certificate['fields'], certificate['fields']
        old_pipeline, new_pipeline = pipeline(old_fields), pipeline(fields)
        assert old_pipeline['native_pipeline_operator_invocations'] == new_pipeline['native_pipeline_operator_invocations']
        assert old_pipeline['native_pipeline_file_kernel_invocations'] == new_pipeline['native_pipeline_file_kernel_invocations']
        assert old_pipeline['reported_file_sort'] == new_pipeline['reported_file_sort']
        assert row['pipeline_evidence'] == new_pipeline
        counts['common_native_pipeline_cases'] += 1
        counts['reported_file_sort_cases'] += int(new_pipeline['reported_file_sort'])
        counts['cases_with_file_kernel'] += int(new_pipeline['native_pipeline_file_kernel_invocations']>0)
        if group == 'owned_source_control':
            old_columns = int(old_fields['native_source_demand_projected_columns'])
            new_columns = int(fields['native_source_demand_projected_columns'])
            assert old_columns == new_columns
            assert fields['native_source_demand_prepared_input_bytes_written'] == '0'
            assert row['source_work'] == dict(scope='owned_demanded_source_columns',before_columns=old_columns,
                after_columns=new_columns,unchanged=True)
            counts['unchanged_control_column_counts'] += 1
        else:
            assert row['source_work'] == dict(scope='native_vortex_no_comparable_per_source_column_counter')
        assert set(row['resources']) == {
            'execution_resource_observed_native_peak_reserved_bytes',
            'execution_resource_observed_native_reserved_bytes',
            'execution_resource_observed_native_denied_reservations',
            'execution_resource_observed_spill_bytes'}
        for key,value in row['resources'].items():
            assert value == int(fields[key]) and value >= 0
        assert fields['execution_resource_observed_native_denied_reservations'] == '0'
        assert fields['execution_resource_observed_spill_io_performed'] == 'false'
        assert fields['execution_resource_observed_spill_bytes'] == '0'

        left, right = counters(old_fields), counters(fields)
        changed = {key:[left.get(key),right.get(key)] for key in sorted(left.keys()|right.keys())
                   if left.get(key)!=right.get(key)}
        review_changes.update(changed.keys())
        metrics, resource_deltas = {}, {}
        for key in ('native_pipeline_operator_invocations','native_pipeline_file_kernel_invocations'):
            name = 'proof.'+key
            metrics[key] = dict(before=left[name],after=right[name],delta=right[name]-left[name])
        for key in ('execution_resource_observed_native_peak_reserved_bytes',
                    'execution_resource_observed_native_reserved_bytes'):
            name = 'report.'+key
            resource_deltas[key] = dict(before=left[name],after=right[name],delta=right[name]-left[name])
        old_sort, new_sort = old_pipeline['reported_file_sort'], new_pipeline['reported_file_sort']
        review_groups[group]['cases'] += 1
        review_groups[group]['reported_file_sort_removed'] += int(old_sort and not new_sort)
        review_groups[group]['reported_file_sort_added'] += int(new_sort and not old_sort)
        review_groups[group]['reported_file_sort_retained'] += int(old_sort and new_sort)
        review_groups[group]['cases_with_more_file_kernels'] += int(metrics['native_pipeline_file_kernel_invocations']['delta']>0)
        review_groups[group]['cases_with_fewer_file_kernels'] += int(metrics['native_pipeline_file_kernel_invocations']['delta']<0)
        review_groups[group]['peak_reserved_increased'] += int(resource_deltas['execution_resource_observed_native_peak_reserved_bytes']['delta']>0)
        review_groups[group]['retained_reserved_increased'] += int(resource_deltas['execution_resource_observed_native_reserved_bytes']['delta']>0)
        review_groups[group]['higher_engine_time'] += int(after>before)
        review_details.append(dict(seq=seq,id=row['id'],group=group,surface=row['surface'],
            native_format=row['native_format'],baseline_source_commit=BASELINE,
            before_stdout_sha256=baseline_certificate['raw_stdout_sha256'],
            after_stdout_sha256=certificate['raw_stdout_sha256'],before_engine_seconds=before,
            after_engine_seconds=after,reported_file_sort=dict(before=old_sort,after=new_sort),
            pipeline=metrics,resources=resource_deltas,counter_changes=changed))
        baseline_clocks.append(before)
        candidate_clocks.append(after)
        groups[group].append(dict(before=before,after=after,source=BASELINE,reported_file_sort=new_sort,
            file_kernel_invocations=new_pipeline['native_pipeline_file_kernel_invocations']))

    assert seen == {item['declaration']['id'] for item in cohort['cases']}
    assert len(candidate) == len(certificates) == len(seen) == 388
    assert dict(counts) == summary['counts'] and counts['unchanged_control_column_counts'] == 10
    baseline_total, candidate_total = math.fsum(baseline_clocks), math.fsum(candidate_clocks)
    assert baseline_total == cohort['current_baseline_engine_seconds'] == 337.804658081
    assert candidate_total == summary['successful_engine_seconds'] == summary['complete_candidate_engine_seconds']
    assert summary['baseline_source_commit'] == BASELINE
    assert summary['baseline_engine_seconds'] == baseline_total
    assert summary['complete_engine_gain_seconds'] == baseline_total-candidate_total
    assert summary['lower_complete_cohort_engine_time'] == (candidate_total<baseline_total)
    assert {name:len(values) for name,values in groups.items()} == EXPECTED_GROUPS
    for group, values in groups.items():
        group_summary = summary['groups'][group]
        base = math.fsum(row['before'] for row in values)
        cand = math.fsum(row['after'] for row in values)
        assert group_summary['cases'] == len(values)
        assert group_summary['baseline_engine_seconds'] == base
        assert group_summary['candidate_engine_seconds'] == cand
        assert group_summary['baseline_source_commits'] == [BASELINE]
        assert group_summary['cases_with_higher_engine_time'] == sum(row['after']>row['before'] for row in values)
        assert group_summary['reported_file_sorts'] == sum(row['reported_file_sort'] for row in values)
        assert group_summary['file_kernel_cases'] == sum(row['file_kernel_invocations']>0 for row in values)
    assert work_review['baseline_engine_seconds'] == baseline_total
    assert work_review['candidate_engine_seconds'] == candidate_total
    assert work_review['lower_complete_cohort_engine_time'] == (candidate_total<baseline_total)
    assert work_review['groups'] == {name:dict(values) for name,values in review_groups.items()}
    assert work_review['changed_counter_occurrences'] == dict(review_changes)
    assert work_review['details'] == review_details
    assert work_review['maximum_peak_reservation_increase'] == max(
        row['resources']['execution_resource_observed_native_peak_reserved_bytes']['delta'] for row in review_details)
    assert work_review['maximum_retained_reservation_increase'] == max(
        row['resources']['execution_resource_observed_native_reserved_bytes']['delta'] for row in review_details)

    runtime_name = 'current/prefix-runtime-r1-identity.json'
    validation_name = 'current/prefix-validation-r1.json'
    runtime, validation = read(runtime_name), read(validation_name)
    build_name = 'current/'+runtime['build_receipt']
    build = read(build_name)
    assert runtime['status'] == validation['status'] == build['status'] == 'passed'
    assert runtime['profile'] == 'release' and runtime['publication_attempted'] is False
    assert runtime['source_commit'] == validation['source_commit'] == summary['source_commit']
    assert runtime['source_tree'] == validation['source_tree'] == summary['source_tree']
    assert build['returncode'] == 0 and build['sources_unchanged']
    assert build['process_group_drained'] and build['direct_child_reaped']
    assert build['command'] == ['cargo','build','--offline','--locked','--release',
        '--features','release-user-surfaces','-p','shardloom-cli','-p','shardloom-python']
    linked(validation['runtime'],runtime_name)
    linked(validation['expectations'],'current/prefix-check-expectations-r1.json')
    linked(runtime['build_receipt_identity'],build_name)
    fingerprint = hashlib.sha256(json.dumps(build['source_files'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    assert fingerprint == runtime['source_fingerprint_sha256']
    proof = protocol['runtime_binding']['runtime_proof']
    assert proof['head'] == runtime['source_commit'] and proof['source_tree'] == runtime['source_tree']
    assert proof['source_fingerprint_sha256'] == fingerprint
    for path, record in proof['proofs'].items():
        assert path in origins
        linked(record, origins[path])
    assert set(proof['runtime']) == set(protocol['runtime_binding']['runtime']) == {'binary','library'}
    for key, record in proof['runtime'].items():
        assert {field:record[field] for field in ('bytes','sha256')} == runtime['files'][record['path']]
        bound = protocol['runtime_binding']['runtime'][key]
        assert bound['path'] == record['path'] and bound['sha256'] == record['sha256']
        assert bound['generation']['bytes'] == record['bytes']
    assert set(validation['checks']) == CHECKS
    assert validation['native_validation_scope'] == 'full_workspace_all_targets_default_and_native'
    assert validation['full_native_library_suite_completed'] is True
    for name,record in validation['checks'].items():
        linked(record,'current/'+name)
        check = read('current/'+name)
        assert check['status'] == 'passed' and check['returncode'] == 0
        assert check['sources_unchanged'] and check['process_group_drained'] and check['direct_child_reaped']
        assert check['source_files'] == build['source_files']
        if '-tests-' in name or '-clippy-' in name:
            expected = ['cargo','test' if '-tests-' in name else 'clippy','--offline','--locked','--workspace','--all-targets']
            if '-native-' in name:
                expected += ['--features','python-native,native-encoded-exchange-experiment']
            if '-clippy-' in name:
                expected += ['--','-D','warnings']
            assert check['command'] == expected
        elif '-fmt-' in name:
            assert check['command'] == ['cargo','fmt','--all','--','--check']
    for name,record in validation['installed_checks'].items():
        linked(record,'current/'+name)
    expectations = read('current/prefix-check-expectations-r1.json')
    assert expectations['default'] == dict(passed=3325,failed=0,ignored=0,test_binaries=108)
    assert expectations['native'] == dict(passed=7157,failed=0,ignored=39,test_binaries=120)
    assert expectations['native_library'] == dict(passed=4184,failed=0,ignored=36)
    assert expectations['installed'] == dict(tests=442,failures=0,errors=0,skipped=0,status='passed')
    assert len(expectations['required_new_tests']) == len(REQUIRED_TESTS)
    assert set(expectations['required_new_tests']) == REQUIRED_TESTS
    for mode in ('default','native'):
        name = 'prefix-'+mode+'-tests-r1'
        log = saved['current/logs/'+name+'.log'].decode()
        reported = [tuple(map(int,row)) for row in re.findall(
            r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',log)]
        expected = expectations[mode]
        assert len(reported) == expected['test_binaries']
        assert tuple(sum(row[i] for row in reported) for i in range(3)) == (
            expected['passed'],expected['failed'],expected['ignored'])
        assert validation['test_counts'][name] == dict(passed=expected['passed'],failed=0,
            ignored=expected['ignored'],test_binaries=expected['test_binaries'])
        if mode == 'native':
            native_library = expectations['native_library']
            assert f"running {native_library['passed']+native_library['ignored']} tests" in log
            assert (native_library['passed'],native_library['failed'],native_library['ignored']) in reported
            for test in expectations['required_new_tests']:
                assert any(line.startswith('test ') and line.rstrip().endswith(f'::{test} ... ok')
                    for line in log.splitlines()), test
    installed = read('current/prefix-installed-wheel-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == expectations['installed']
    assert read('current/prefix-installed-wheel-r1/integration.json') == installed['integration']
    assert validation['installed_python_tests'] == installed['integration']
    assert installed['publication_attempted'] is False
    binaries = {Path(path).name:record for path,record in runtime['files'].items()}
    assert installed['integrity']['native_library_sha256'] == binaries['_native.abi3.so']['sha256']
    assert installed['integrity']['cli_sha256'] == binaries['shardloom']['sha256']
    source_files = {name for name in saved if name.startswith('source/files/')}
    archived_rust_paths = archived_source_paths(manifest)
    assert sorted(manifest['rust_source_files']) == sorted(archived_rust_paths) == sorted(RUST_FILES)
    assert source_files == {'source/files/'+path for path in RUST_FILES}
    for name in source_files:
        path = name.removeprefix('source/files/')
        assert identity(saved[name]) == build['source_files'][path]
    for test in REQUIRED_TESTS:
        assert sum(len(re.findall(r'\bfn '+re.escape(test)+r'\(', saved[name].decode()))
                   for name in source_files) == 1, test
    assert identity(saved['design/native-sort-prefix-validation-2026-11-13.md']) == (
        build['source_files']['docs/architecture/native-sort-prefix-validation-2026-11-13.md'])
    assert manifest['archive_members']['source/prefix.patch']['origin'].startswith('git diff --binary '+BASELINE)
    return dict(status='passed',query_executions=0,manifest_sha256=identity(manifest_raw)['sha256'],
        verified_archive_members=len(saved),source_commit=summary['source_commit'],
        baseline_source_commit=BASELINE,cases=388,baseline_engine_seconds=baseline_total,
        candidate_engine_seconds=candidate_total,baseline_groups=summary['groups'],
        lower_complete_cohort_engine_time=candidate_total<baseline_total,
        scope='Source-bound single-current-baseline comparison with complete saved results, certificates, '
            'work/resource deltas and clocks. Complete answers were independently replayed locally; the '
            'portable verifier does not repeat the full oracle or establish complete optimization coverage.')


def archived_source_paths(manifest):
    return {name.removeprefix('source/files/') for name in manifest['archive_members']
            if name.startswith('source/files/')}


if __name__ == '__main__':
    target = Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent
    print(json.dumps(verify(target),indent=2))
