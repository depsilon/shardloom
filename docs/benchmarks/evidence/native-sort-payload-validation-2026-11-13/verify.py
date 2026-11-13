"""Verify portable Sort payload evidence without running any query or oracle."""
from collections import Counter, defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile


def identity(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def clock(value):
    assert type(value) in (int, float) and math.isfinite(value) and value >= 0
    return value


def safe_name(name):
    path = PurePosixPath(name)
    assert not path.is_absolute() and '..' not in path.parts and str(path) == name


def counters(fields):
    pairs = re.findall(r'(?:^|[ ;])([a-z][a-z0-9_]*)=([0-9]+)(?=$|[ ;])',
        fields['local_primitive_native_io_pushdown_proof_basis'])
    assert len(pairs) == len(dict(pairs))
    result = {'proof.'+key:int(value) for key,value in pairs}
    result.update({'report.'+key:int(value) for key,value in fields.items()
                   if re.fullmatch(r'[0-9]+',str(value))})
    return result


def pipeline(fields):
    assert fields['fallback_attempted'] == fields['external_engine_invoked'] == 'false'
    assert fields['relational_provider_selection_reason'] == 'native_operator_pipeline'
    proof = fields['local_primitive_native_io_pushdown_proof_basis']
    assert 'native_execution_pipeline=native_operator_pipeline' in proof
    assert 'native_pipeline_catch_all_execution=false' in proof
    counts = {}
    for key in ('native_pipeline_operator_invocations', 'native_pipeline_file_kernel_invocations'):
        values = re.findall(r'\b'+key+r'=(\d+)\b', proof)
        assert len(values) == 1
        counts[key] = int(values[0])
    assert 0 <= counts['native_pipeline_file_kernel_invocations'] <= counts['native_pipeline_operator_invocations']
    assert counts['native_pipeline_operator_invocations'] > 0
    return dict(common_pipeline=True, catch_all=False, external_fallback=False,
                reported_file_sort='local_primitive_sort_provider_background_workers' in fields, **counts)


def verify(root):
    if not __debug__:
        raise RuntimeError('verification requires Python assertions; do not use -O')
    manifest_raw = (root/'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    assert manifest['schema'] == 'shardloom.sort_payload_validation_evidence.v1'
    actual = {str(path.relative_to(root)) for path in root.rglob('*') if path.is_file()}
    assert actual <= set(manifest['files']) | {'manifest.json', 'verification.json'}
    assert not any(path.is_symlink() for path in root.rglob('*'))
    for name, expected in manifest['files'].items():
        safe_name(name)
        assert identity((root/name).read_bytes()) == expected, name
    saved = {}
    with tarfile.open(root/'observations.tar.gz', 'r:gz') as archive:
        members = archive.getmembers()
        assert len(members) == len({item.name for item in members})
        assert {item.name for item in members} == set(manifest['archive_members'])
        for member in members:
            safe_name(member.name)
            assert member.isfile()
            raw = archive.extractfile(member).read()
            expected = manifest['archive_members'][member.name]
            assert identity(raw) == {key:expected[key] for key in ('bytes', 'sha256')}
            saved[member.name] = raw

    def read(name):
        return json.loads(saved[name])

    def rows(name):
        return [json.loads(line) for line in gzip.decompress(saved[name]).splitlines()]

    def linked(record, name):
        assert record['sha256'] == identity(saved[name])['sha256'], name
        if 'bytes' in record:
            assert record['bytes'] == len(saved[name]), name
        elif 'generation' in record:
            assert record['generation']['bytes'] == len(saved[name]), name

    origins = {record['origin']:name for name,record in manifest['archive_members'].items()}
    design = read('original/design.json')
    assert design['count'] == len(design['cases']) == 10000
    baseline_rows, baseline_certificates, baseline_summaries = {}, {}, {}
    for label, count in [('original-10k', 10000), ('sort-demand', 606)]:
        prefix = 'baseline/'+label+'/'
        summary = read(prefix+'verified-summary-r1.json')
        previous = rows(prefix+'verified-cases-r1.jsonl.gz')
        certificates = rows(prefix+'certificate-cases-r1.jsonl.gz')
        assert summary['status'] == 'passed_offline_replay'
        assert summary['successful_cases'] == summary['attempted_cases'] == len(previous) == len(certificates) == count
        assert summary['unsuccessful_cases'] == 0 and summary['outcomes'] == {'passed':count}
        assert summary['primary_clock'] == 'engine_execution_seconds'
        linked(summary['case_rows'], prefix+'verified-cases-r1.jsonl.gz')
        for seq, (row, certificate) in enumerate(zip(previous, certificates)):
            assert row['seq'] == certificate['seq'] == seq
            assert row['id'] == certificate['id']
            assert row['current']['status'] == certificate['status'] == 'passed'
            assert certificate['engine_timing_reported'] is True
            assert clock(certificate['engine_seconds']) == clock(row['current']['engine_seconds'])
        baseline_rows[label] = {row['id']:row for row in previous}
        baseline_certificates[label] = {row['id']:row for row in certificates}
        baseline_summaries[label] = summary
        assert len(baseline_rows[label]) == len(baseline_certificates[label]) == count
    assert baseline_summaries['original-10k']['source_commit'] == '9f4580c723f876f087d3e75fb56153b4b099ef20'
    assert baseline_summaries['sort-demand']['source_commit'] == 'cac86e8b0a5fa5fa8bf4c900dbef7a703746bfa8'
    complete_prior = rows('current/coverage-inventory-r1/observed-query-evidence.jsonl.gz')
    assert len(complete_prior) == 10000
    prior_fields = {row['id']:row for row in complete_prior}
    assert len(prior_fields) == 10000
    for ordinal, old in enumerate(complete_prior):
        case = design['cases'][ordinal]
        cert = baseline_certificates['original-10k'][old['id']]
        assert old['seq'] == ordinal and old['id'] == case['id']
        assert old['case_identity'] == case['case_identity']
        assert old['source_commit'] == baseline_summaries['original-10k']['source_commit']
        assert old['raw_stdout_sha256'] == cert['raw_stdout_sha256']
        assert clock(old['engine_seconds']) == clock(cert['engine_seconds'])
        assert {key:old['fields'][key] for key in cert['fields']} == cert['fields']

    prefix, folder = 'current/', 'current/payload-cohort-r1/'
    cohort = read(prefix+'payload-sort-cohort-r1.json')
    protocol = read(prefix+'payload-cohort-protocol-r1.json')
    complete = read(folder+'complete.json')
    summary = read(folder+'verified-summary-r1.json')
    linked(manifest['cohort_summary'], folder+'verified-summary-r1.json')
    supervision = read(prefix+'payload-cohort-supervision-r1.json')
    work_review = read(prefix+'payload-work-review-r1.json')
    assert work_review['status'] == 'passed_saved_work_review' and work_review['query_executions'] == 0
    assert work_review['cases'] == 388 and work_review['source_commit'] == summary['source_commit']
    for record in work_review['source_identities']:
        assert record['path'] in origins
        linked(record, origins[record['path']])
    candidate = rows(folder+'verified-cases-r1.jsonl.gz')
    certificates = rows(folder+'certificate-cases-r1.jsonl.gz')
    assert cohort['status'] == 'frozen_before_candidate_execution'
    assert cohort['case_count'] == len(cohort['cases']) == len(candidate) == len(certificates) == 388
    assert cohort['query_executions'] == cohort['repeats'] == cohort['warmups'] == 0
    assert cohort['attempts_per_case'] == 1 and cohort['baseline_queries_to_repeat'] == 0
    expected_groups = dict(owned_source_control=10, vortex_no_file_kernel=321,
                           vortex_other_file_kernel=36, vortex_reported_file_sort=21)
    assert cohort['groups'] == expected_groups
    for record in cohort['source_identities']:
        if record['path'] in origins:
            linked(record, origins[record['path']])
    assert summary['status'] == 'passed_offline_replay' and summary['query_executions'] == 0
    assert summary['primary_clock'] == protocol['primary_clock'] == cohort['primary_clock'] == 'engine_execution_seconds'
    assert summary['successful_cases'] == summary['attempted_cases'] == 388
    assert summary['unsuccessful_cases'] == 0 and summary['failures'] == []
    assert summary['independent_complete_results_replayed'] is True
    assert summary['no_denied_reservations'] and summary['no_spill']
    assert summary['outcomes'] == complete['outcomes'] == {'passed':388}
    assert summary['attempts_per_case'] == complete['attempts_per_case'] == 1
    assert summary['repeated_baseline_queries'] == complete['requested_repeats'] == 0
    assert complete['status'] == 'complete_observation' and complete['posthash_verified']
    assert complete['total'] == complete['attempt_slots'] == 388
    assert complete['updated'] == protocol['runtime_binding']
    assert complete['updated']['head'] == summary['source_commit'] == manifest['source_commit']
    assert complete['updated']['source_tree'] == summary['source_tree']
    assert protocol['expected_attempt_count'] == protocol['required_complete_result_count'] == 388
    assert protocol['attempts_per_case'] == 1 and protocol['warmups'] == 0
    assert protocol['execution']['threads'] == 2 and protocol['execution']['native_memory_bytes'] == 2 << 30
    linked(protocol['design'], 'original/design.json')
    linked(protocol['cohort'], prefix+'payload-sort-cohort-r1.json')
    linked(summary['cohort'], prefix+'payload-sort-cohort-r1.json')
    linked(summary['protocol'], prefix+'payload-cohort-protocol-r1.json')
    linked(summary['case_rows'], folder+'verified-cases-r1.jsonl.gz')
    linked(summary['certificates'], folder+'certificate-cases-r1.jsonl.gz')
    assert supervision['status'] == 'passed_capture' and supervision['returncode'] == 0
    assert supervision['process_group_drained'] and supervision['direct_child_reaped']
    assert supervision['runtime_and_source_unchanged'] and not supervision['timed_out']
    assert supervision['interrupted_signal'] is None
    linked(supervision['log_identity'], prefix+'payload-cohort-supervision-r1.log')
    linked(supervision['complete'], folder+'complete.json')
    for path, record in supervision['drivers'].items():
        assert path in origins
        linked(record, origins[path])
    groups, counts, seen, clocks = defaultdict(list), Counter(), set(), []
    review_groups, review_changes, review_details = defaultdict(Counter), Counter(), []
    for seq, (row, declared, certificate) in enumerate(zip(candidate, cohort['cases'], certificates)):
        item, group = declared['declaration'], declared['comparison_group']
        assert row['seq'] == certificate['seq'] == seq
        assert row['id'] == certificate['id'] == item['id'] and row['id'] not in seen
        seen.add(row['id'])
        assert row['original_ordinal'] == declared['original_ordinal']
        assert item == design['cases'][row['original_ordinal']]
        assert row['case_identity'] == item['case_identity']
        assert row['comparison_group'] == group
        assert row['current']['status'] == certificate['status'] == 'passed'
        assert certificate['engine_timing_reported'] is True
        for field in ('family', 'profile', 'surface', 'native_format'):
            assert row[field] == item[field]
        label = 'sort-demand' if group == 'owned_source_control' else 'original-10k'
        old = baseline_rows[label][row['id']]
        old_certificate = baseline_certificates[label][row['id']]
        old_fields = old_certificate['fields'] if label == 'sort-demand' else prior_fields[row['id']]['fields']
        assert old['seq'] == declared['baseline_seq']
        before = clock(old['current']['engine_seconds'])
        assert before == clock(row['previous_engine_seconds']) == clock(declared['baseline_engine_seconds'])
        assert row['previous_work'] == declared['baseline_work'] == old['current']['work']
        assert row['previous_source_commit'] == declared['baseline_source_commit'] == baseline_summaries[label]['source_commit']
        assert row['previous_ledger'] == declared['baseline_ledger'] == ('sort-cohort-r1' if label=='sort-demand' else 'full10k-r1')
        if label == 'original-10k':
            assert row['native_format'] == 'vortex'
            assert declared['baseline_raw_stdout_sha256'] == old_certificate['raw_stdout_sha256']
            old_pipeline = pipeline(old_fields)
            actual_group = ('vortex_reported_file_sort' if old_pipeline['reported_file_sort'] else
                            'vortex_other_file_kernel' if old_pipeline['native_pipeline_file_kernel_invocations'] else
                            'vortex_no_file_kernel')
            assert actual_group == group
        after = clock(row['current']['engine_seconds'])
        assert after == clock(certificate['engine_seconds'])
        fields = certificate['fields']
        now = pipeline(fields)
        assert row['pipeline_evidence'] == now
        counts['common_native_pipeline_cases'] += 1
        counts['reported_file_sort_cases'] += int(now['reported_file_sort'])
        counts['cases_with_file_kernel'] += int(now['native_pipeline_file_kernel_invocations'] > 0)
        if group == 'owned_source_control':
            old_columns = int(old_fields['native_source_demand_projected_columns'])
            new_columns = int(fields['native_source_demand_projected_columns'])
            assert old_columns == new_columns
            assert fields['native_source_demand_prepared_input_bytes_written'] == '0'
            assert row['source_work'] == dict(scope='owned_demanded_source_columns',
                before_columns=old_columns, after_columns=new_columns, unchanged=True)
            counts['unchanged_control_column_counts'] += 1
        else:
            assert row['source_work'] == dict(scope='native_vortex_no_comparable_per_source_column_counter')
        assert set(row['resources']) == {
            'execution_resource_observed_native_peak_reserved_bytes',
            'execution_resource_observed_native_reserved_bytes',
            'execution_resource_observed_native_denied_reservations',
            'execution_resource_observed_spill_bytes',
        }
        for key, value in row['resources'].items():
            assert value == int(fields[key]) and value >= 0
        assert fields['execution_resource_observed_native_denied_reservations'] == '0'
        assert fields['execution_resource_observed_spill_io_performed'] == 'false'
        assert fields['execution_resource_observed_spill_bytes'] == '0'
        left, right = counters(old_fields), counters(fields)
        changed = {key:[left.get(key),right.get(key)] for key in sorted(left.keys()|right.keys())
                   if left.get(key)!=right.get(key)}
        review_changes.update(changed.keys())
        metrics, resources = {}, {}
        for key in ('native_pipeline_operator_invocations','native_pipeline_file_kernel_invocations'):
            name = 'proof.'+key
            metrics[key] = dict(before=left[name],after=right[name],delta=right[name]-left[name])
        for key in ('execution_resource_observed_native_peak_reserved_bytes','execution_resource_observed_native_reserved_bytes'):
            name = 'report.'+key
            resources[key] = dict(before=left[name],after=right[name],delta=right[name]-left[name])
        old_sort, new_sort = 'local_primitive_sort_provider_background_workers' in old_fields, now['reported_file_sort']
        review_groups[group]['cases'] += 1
        review_groups[group]['reported_file_sort_removed'] += int(old_sort and not new_sort)
        review_groups[group]['reported_file_sort_added'] += int(new_sort and not old_sort)
        review_groups[group]['reported_file_sort_retained'] += int(old_sort and new_sort)
        review_groups[group]['cases_with_more_file_kernels'] += int(metrics['native_pipeline_file_kernel_invocations']['delta']>0)
        review_groups[group]['cases_with_fewer_file_kernels'] += int(metrics['native_pipeline_file_kernel_invocations']['delta']<0)
        review_groups[group]['peak_reserved_increased'] += int(resources['execution_resource_observed_native_peak_reserved_bytes']['delta']>0)
        review_groups[group]['retained_reserved_increased'] += int(resources['execution_resource_observed_native_reserved_bytes']['delta']>0)
        review_groups[group]['higher_engine_time'] += int(after>before)
        review_details.append(dict(seq=seq,id=row['id'],group=group,surface=row['surface'],native_format=row['native_format'],
            baseline_source_commit=row['previous_source_commit'],before_stdout_sha256=old_certificate['raw_stdout_sha256'],
            after_stdout_sha256=certificate['raw_stdout_sha256'],before_engine_seconds=before,after_engine_seconds=after,
            reported_file_sort=dict(before=old_sort,after=new_sort),pipeline=metrics,resources=resources,counter_changes=changed))
        clocks.append(after)
        groups[group].append(dict(before=before, after=after, source=row['previous_source_commit'], **now))
    assert dict(counts) == summary['counts'] and counts['unchanged_control_column_counts'] == 10
    assert work_review['groups'] == {name:dict(values) for name,values in review_groups.items()}
    assert work_review['changed_counter_occurrences'] == dict(review_changes)
    assert work_review['details'] == review_details
    assert work_review['maximum_peak_reservation_increase'] == max(
        row['resources']['execution_resource_observed_native_peak_reserved_bytes']['delta'] for row in review_details)
    assert work_review['maximum_retained_reservation_increase'] == max(
        row['resources']['execution_resource_observed_native_reserved_bytes']['delta'] for row in review_details)
    assert {name:len(values) for name,values in groups.items()} == expected_groups
    assert math.fsum(clocks) == summary['successful_engine_seconds'] == summary['complete_candidate_engine_seconds']
    for group, values in groups.items():
        baseline_seconds = math.fsum(row['before'] for row in values)
        sources = sorted({row['source'] for row in values})
        assert cohort['baseline_groups'][group] == dict(cases=len(values), engine_seconds=baseline_seconds, source_commits=sources)
        assert summary['groups'][group] == dict(cases=len(values), baseline_engine_seconds=baseline_seconds,
            candidate_engine_seconds=math.fsum(row['after'] for row in values), baseline_source_commits=sources,
            cases_with_higher_engine_time=sum(row['after']>row['before'] for row in values),
            reported_file_sorts=sum(row['reported_file_sort'] for row in values),
            file_kernel_cases=sum(row['native_pipeline_file_kernel_invocations']>0 for row in values))

    runtime_name = prefix+'payload-runtime-r1-identity.json'
    runtime, validation = read(runtime_name), read(prefix+'payload-validation-r1.json')
    build_name = prefix+runtime['build_receipt']
    build = read(build_name)
    assert runtime['status'] == validation['status'] == build['status'] == 'passed'
    assert runtime['profile'] == 'release' and runtime['publication_attempted'] is False
    assert runtime['source_commit'] == validation['source_commit'] == summary['source_commit']
    assert runtime['source_tree'] == validation['source_tree'] == summary['source_tree']
    assert build['returncode'] == 0 and build['sources_unchanged'] and build['process_group_drained'] and build['direct_child_reaped']
    assert build['command'] == ['cargo','build','--offline','--locked','--release',
        '--features','release-user-surfaces','-p','shardloom-cli','-p','shardloom-python']
    linked(validation['runtime'], runtime_name)
    linked(runtime['build_receipt_identity'], build_name)
    fingerprint = hashlib.sha256(json.dumps(build['source_files'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    assert fingerprint == runtime['source_fingerprint_sha256']
    proof = protocol['runtime_binding']['runtime_proof']
    assert proof['head'] == runtime['source_commit'] and proof['source_tree'] == runtime['source_tree']
    assert proof['source_fingerprint_sha256'] == fingerprint
    for path, record in proof['proofs'].items():
        assert path in origins
        linked(record, origins[path])
    for record in proof['runtime'].values():
        assert {key:record[key] for key in ('bytes','sha256')} == runtime['files'][record['path']]
    required = {'payload-'+name+'.json' for name in ('fmt-r3','default-clippy-r3','native-clippy-r3',
        'default-tests-r2','native-tests-r2','installed-wheel-r1')}
    assert set(validation['checks']) == required
    assert validation['native_validation_scope'] == 'full_workspace_all_targets_default_and_native'
    assert validation['full_native_library_suite_completed'] is True
    for name, record in validation['checks'].items():
        linked(record, prefix+name)
        check = read(prefix+name)
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
    for name, record in validation['installed_checks'].items():
        linked(record, prefix+name)
    for mode, passed, ignored, binaries in [('default',3325,0,108), ('native',7145,39,120)]:
        name = 'payload-'+mode+'-tests-r2'
        log = saved[prefix+'logs/'+name+'.log'].decode()
        reported = [tuple(map(int,row)) for row in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',log)]
        assert len(reported) == binaries
        assert tuple(sum(row[i] for row in reported) for i in range(3)) == (passed,0,ignored)
        assert validation['test_counts'][name] == dict(passed=passed, failed=0, ignored=ignored, test_binaries=binaries)
        if mode == 'native':
            assert (4172,0,36) in reported
            for test in ('file_sort_kernel_preserves_hidden_and_outside_prefix_payload_errors',
                         'projection_before_sort_retains_file_kernel_with_unobserved_source_payload',
                         'text_order_keys_retain_file_kernel_with_integer_payload',
                         'sort_payload_validation_respects_prior_filter_and_nulls',
                         'sort_preserves_validation_of_hidden_nested_payload',
                         'sort_preserves_validation_of_hidden_text_payload'):
                assert any(line.startswith('test ') and line.rstrip().endswith('::'+test+' ... ok') for line in log.splitlines())
    installed = read(prefix+'payload-installed-wheel-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == dict(status='passed', tests=442, failures=0, errors=0, skipped=0)
    assert read(prefix+'payload-installed-wheel-r1/integration.json') == installed['integration']
    assert validation['installed_python_tests'] == installed['integration']
    assert installed['publication_attempted'] is False
    binaries = {Path(path).name:record for path,record in runtime['files'].items()}
    assert installed['integrity']['native_library_sha256'] == binaries['_native.abi3.so']['sha256']
    assert installed['integrity']['cli_sha256'] == binaries['shardloom']['sha256']
    source_files = [name for name in saved if name.startswith('source/files/')]
    assert set(source_files) == {'source/files/shardloom-vortex/src/'+name for name in (
        'local_primitive_relational_provider.rs','local_primitive_relational_provider_plan.rs',
        'local_primitive_relational_provider_tests.rs','local_primitive_relational_provider_payload_tests.rs',
        'local_primitive_relational_tests.rs')}
    for name in source_files:
        assert identity(saved[name]) == build['source_files'][name.removeprefix('source/files/')]
    return dict(status='passed', query_executions=0, manifest_sha256=identity(manifest_raw)['sha256'],
        verified_archive_members=len(saved), source_commit=summary['source_commit'], cases=388,
        groups=summary['groups'], candidate_engine_seconds=math.fsum(clocks), combined_historical_engine_total=None,
        scope='Saved identities, per-group engine clocks, complete certificate fields, runtime/source/check bindings. '
              'Complete answers were replayed locally against independent original references; this portable '
              'verifier does not repeat the full oracle or establish complete optimization coverage.')


if __name__ == '__main__':
    target = Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent
    print(json.dumps(verify(target), indent=2))
