"""Replay compact source-demand identities, decisions and clocks; run no query."""
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
    result = {'proof.'+key: int(value) for key,value in pairs}
    result.update({'report.'+key: int(value) for key,value in fields.items()
                   if re.fullmatch(r'[0-9]+',str(value))})
    return result


def verify(root):
    if not __debug__:
        raise RuntimeError('verification requires Python assertions; do not use -O')
    manifest_raw = (root/'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    assert manifest['schema'] == 'shardloom.source_demand_connection_evidence.v1'
    actual = {str(path.relative_to(root)) for path in root.rglob('*') if path.is_file()}
    assert actual <= set(manifest['files']) | {'manifest.json', 'verification.json'}
    assert not any(path.is_symlink() for path in root.rglob('*'))
    for name, expected in manifest['files'].items():
        safe_name(name)
        path = root/name
        assert not path.is_symlink() and identity(path.read_bytes()) == expected, name
    saved = {}
    with tarfile.open(root/'observations.tar.gz', 'r:gz') as archive:
        members = archive.getmembers()
        assert len(members) == len({item.name for item in members})
        assert {item.name for item in members} == set(manifest['archive_members'])
        for member in members:
            safe_name(member.name)
            assert member.isfile()
            data = archive.extractfile(member).read()
            expected = manifest['archive_members'][member.name]
            assert identity(data) == {key: expected[key] for key in ('bytes', 'sha256')}
            saved[member.name] = data

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

    baseline = read('baseline/verified-summary-r1.json')
    linked(baseline['case_rows'], 'baseline/verified-cases-r1.jsonl.gz')
    base_rows = rows('baseline/verified-cases-r1.jsonl.gz')
    assert baseline['successful_cases'] == baseline['attempted_cases'] == len(base_rows) == 10000
    assert baseline['source_commit'] == '9f4580c723f876f087d3e75fb56153b4b099ef20'
    assert baseline['unsuccessful_cases'] == 0 and baseline['outcomes'] == {'passed': 10000}
    assert baseline['primary_clock'] == 'engine_execution_seconds'
    previous = {row['id']: row for row in base_rows}
    assert len(previous) == len(base_rows)
    base_certificates = rows('baseline/certificate-cases-r1.jsonl.gz')
    base_certificate_export = read('baseline/certificate-export-r1.json')
    linked(base_certificate_export['output'],'baseline/certificate-cases-r1.jsonl.gz')
    assert base_certificate_export['status'] == 'passed'
    assert base_certificate_export['cases'] == len(base_certificates) == 10000
    prior_certificates = {row['id']:row for row in base_certificates}
    assert len(prior_certificates) == 10000
    for seq,(row,certificate) in enumerate(zip(base_rows,base_certificates)):
        assert row['seq'] == certificate['seq'] == seq
        assert row['id'] == certificate['id'] and certificate['status'] == 'passed'
        assert certificate['engine_timing_reported'] is True
        assert clock(certificate['engine_seconds']) == clock(row['current']['engine_seconds'])
    complete_base_certificates = rows('current/baseline-connection-certificates-r1.jsonl.gz')
    complete_base_export = read('current/baseline-connection-certificates-r1.json')
    linked(complete_base_export['output'],'current/baseline-connection-certificates-r1.jsonl.gz')
    assert complete_base_export['status'] == 'passed' and complete_base_export['query_executions'] == 0
    assert complete_base_export['cases'] == len(complete_base_certificates) == 819
    assert complete_base_export['source_commit'] == baseline['source_commit']
    assert complete_base_export['ledger']['sha256'] == baseline['ledger']['sha256']
    full_prior_certificates = {row['id']:row for row in complete_base_certificates}
    assert len(full_prior_certificates) == 819
    for record in complete_base_certificates:
        limited = prior_certificates[record['id']]
        assert {key:value for key,value in record.items() if key!='fields'} == {
            key:value for key,value in limited.items() if key!='fields'}
        assert {key:record['fields'][key] for key in limited['fields']} == limited['fields']
    assert math.fsum(clock(row['current']['engine_seconds']) for row in base_rows) == baseline['complete_10000_engine_seconds']
    design = read('original/design.json')
    assert design['count'] == len(design['cases']) == 10000
    outcomes, selected_sets = {}, {}
    configurations = {
        'sort': dict(count=606, matched=606, narrowed=98,
                     groups={'new_column_reduction':98, 'already_narrowed':19, 'all_fields_required':489},
                     native_tests=7128, native_library_tests=4155, fmt='r2', lint='r2'),
        'cardinality': dict(count=213, matched=135, narrowed=96,
                     groups={'new_column_reduction':96, 'already_narrowed':38,
                             'metadata_count_control':1, 'native_vortex_control':78},
                     native_tests=7139, native_library_tests=4166, fmt='r1', lint='r4'),
    }
    for label, expected in configurations.items():
        prefix = 'current/'
        name = label+'-cohort-r1/'
        summary = read(prefix+name+'verified-summary-r1.json')
        cohort_name = prefix+label+'-demand-cohort-r1.json'
        cohort = read(cohort_name)
        protocol_name = prefix+label+'-cohort-protocol-r1.json'
        protocol = read(protocol_name)
        complete = read(prefix+name+'complete.json')
        supervision = read(prefix+label+'-cohort-supervision-r1.json')
        candidate = rows(prefix+name+'verified-cases-r1.jsonl.gz')
        certificates = rows(prefix+name+'certificate-cases-r1.jsonl.gz')
        work_review = read(prefix+label+'-work-review-r1.json')
        linked(work_review['certificate_export'], prefix+name+'certificate-cases-r1.jsonl.gz')
        assert work_review['status']=='passed_saved_work_review' and work_review['query_executions']==0
        assert work_review['source_commit']==summary['source_commit'] and work_review['cases']==expected['count']
        assert len(certificates)==len(candidate)
        for key, target in [('case_rows',prefix+name+'verified-cases-r1.jsonl.gz'),
                            ('protocol',protocol_name), ('cohort',cohort_name)]:
            linked(summary[key],target)
        assert summary['status'] == 'passed_offline_replay' and summary['query_executions'] == 0
        assert summary['primary_clock'] == cohort['primary_clock'] == protocol['primary_clock'] == 'engine_execution_seconds'
        assert summary['attempted_cases'] == summary['successful_cases'] == len(candidate) == expected['count']
        assert summary['unsuccessful_cases'] == 0 and summary['failures'] == []
        assert summary['outcomes'] == complete['outcomes'] == {'passed': expected['count']}
        assert summary['attempts_per_case'] == complete['attempts_per_case'] == 1
        assert summary['repeated_baseline_queries'] == complete['requested_repeats'] == 0
        assert summary['independent_complete_results_replayed'] is True
        assert complete['status'] == 'complete_observation' and complete['posthash_verified']
        assert complete['total'] == complete['attempt_slots'] == expected['count']
        assert complete['updated']==protocol['runtime_binding']
        assert complete['updated']['head']==summary['source_commit']
        assert complete['updated']['source_tree']==summary['source_tree']
        assert protocol['expected_attempt_count'] == protocol['required_complete_result_count'] == expected['count']
        assert protocol['attempts_per_case'] == 1 and protocol['warmups'] == 0
        linked(protocol['design'], 'original/design.json')
        linked(protocol['cohort'], cohort_name)
        assert supervision['status'] == 'passed_capture' and supervision['returncode'] == 0
        assert supervision['process_group_drained'] and supervision['direct_child_reaped']
        assert supervision['runtime_and_source_unchanged'] and not supervision['timed_out']
        assert supervision['interrupted_signal'] is None
        assert cohort['status'] == 'frozen_before_candidate_execution'
        assert cohort['case_count'] == len(cohort['cases']) == expected['count']
        assert cohort['attempts_per_case'] == 1 and cohort['warmups'] == cohort['repeats'] == 0
        assert cohort['groups'] == expected['groups']
        all_before, all_after, groups, counts, ids = [], [], defaultdict(list), Counter(), set()
        review_groups, review_changes, review_details = defaultdict(Counter), Counter(), []
        for seq, (row, declared, certificate) in enumerate(zip(candidate,cohort['cases'],certificates)):
            descriptor = declared['declaration']
            prior = previous[row['id']]
            original = declared['original_ordinal']
            assert row['seq'] == seq and row['original_ordinal'] == prior['seq'] == original
            assert descriptor == design['cases'][original]
            assert row['id'] == descriptor['id'] == prior['id'] and row['id'] not in ids
            ids.add(row['id'])
            assert row['case_identity'] == descriptor['case_identity']
            assert row['current']['status'] == prior['current']['status'] == 'passed'
            for field in ('family','surface','profile','native_format'):
                assert row[field] == descriptor[field]
            before = clock(prior['current']['engine_seconds'])
            assert before == clock(row['previous_engine_seconds']) == clock(declared['baseline_engine_seconds'])
            assert row['previous_work'] == declared['baseline_work'] == prior['current']['work']
            after = clock(row['current']['engine_seconds'])
            assert certificate['seq']==seq and certificate['id']==row['id'] and certificate['status']=='passed'
            assert certificate['engine_timing_reported'] is True and clock(certificate['engine_seconds'])==after
            group = row['comparison_group']
            assert group == declared['comparison_group']
            demand = row['demand_comparison']
            assert demand['before_columns'] == declared['baseline_projected_columns']
            assert demand['required_columns'] == declared['required_projected_columns']
            if declared['required_projected_columns'] is None:
                assert label == 'cardinality' and group == 'native_vortex_control'
                assert descriptor['native_format'] == 'vortex' and demand['matches_dependency_demand'] is None
                counts['native_vortex_controls_completed'] += 1
            else:
                value = int(row['current']['work']['native_source_demand_projected_columns'])
                assert demand['after_columns'] == value == declared['required_projected_columns']
                assert demand['matches_dependency_demand'] is True
                narrowed = value < demand['before_columns']
                assert demand['narrowed'] == narrowed and demand['expanded'] is False
                counts['matches_required_columns'] += 1
                counts['narrowed_columns'] += int(narrowed)
                counts['expanded_columns'] += 0
                counts[group+'_matching_columns'] += 1
            pipeline = row['pipeline_evidence']
            assert pipeline['common_pipeline'] and not pipeline['catch_all'] and not pipeline['external_fallback']
            fields=certificate['fields']
            if demand['required_columns'] is not None:
                assert int(fields['native_source_demand_projected_columns'])==demand['after_columns']
            assert fields['fallback_attempted']==fields['external_engine_invoked']=='false'
            assert fields['relational_provider_selection_reason']=='native_operator_pipeline'
            basis=fields['local_primitive_native_io_pushdown_proof_basis']
            assert 'native_execution_pipeline=native_operator_pipeline' in basis
            assert 'native_pipeline_catch_all_execution=false' in basis
            for key in ('native_pipeline_operator_invocations','native_pipeline_file_kernel_invocations'):
                values=re.findall(r'\b'+key+r'=(\d+)\b',basis)
                assert len(values)==1 and int(values[0])==pipeline[key]
            assert fields['execution_resource_observed_native_denied_reservations']=='0'
            assert fields['execution_resource_observed_spill_io_performed']=='false'
            assert fields['execution_resource_observed_spill_bytes']=='0'
            assert 0 <= pipeline['native_pipeline_file_kernel_invocations'] <= pipeline['native_pipeline_operator_invocations']
            assert pipeline['native_pipeline_operator_invocations'] > 0
            old_certificate = full_prior_certificates[row['id']]
            old_fields = old_certificate['fields']
            left,right = counters(old_fields),counters(fields)
            changed = {key:[left.get(key),right.get(key)] for key in sorted(left.keys()|right.keys())
                       if left.get(key)!=right.get(key)}
            review_changes.update(changed.keys())
            read_key = 'native_source_demand_data_read_bytes'
            work,old_work = row['current']['work'],row['previous_work']
            reads = None
            if read_key in work and read_key in old_work:
                old_read,new_read = int(old_work[read_key]),int(work[read_key])
                assert int(fields[read_key]) == new_read and int(old_fields[read_key]) == old_read
                direction = 'decreased' if new_read<old_read else 'increased' if new_read>old_read else 'unchanged'
                review_groups[group]['read_bytes_'+direction] += 1
                reads = dict(before=old_read,after=new_read,direction=direction)
            else:
                review_groups[group]['read_bytes_unavailable'] += 1
            peak = 'execution_resource_observed_native_peak_reserved_bytes'
            retained = 'execution_resource_observed_native_reserved_bytes'
            resources = {key:dict(before=int(old_fields[key]),after=int(fields[key]),
                         delta=int(fields[key])-int(old_fields[key])) for key in (peak,retained)}
            review_groups[group]['cases'] += 1
            review_groups[group]['peak_reserved_increased'] += int(resources[peak]['delta']>0)
            review_groups[group]['retained_reserved_increased'] += int(resources[retained]['delta']>0)
            review_details.append(dict(id=row['id'],seq=seq,group=group,surface=row['surface'],
                native_format=row['native_format'],before_stdout_sha256=old_certificate['raw_stdout_sha256'],
                after_stdout_sha256=certificate['raw_stdout_sha256'],read_bytes=reads,
                resource=resources,counter_changes=changed))
            counts['common_native_pipeline_cases'] += 1
            groups[group].append((before,after))
            all_before.append(before); all_after.append(after)
        assert dict(counts) == summary['work_counts']
        assert counts['matches_required_columns'] == expected['matched']
        assert counts['narrowed_columns'] == expected['narrowed'] == summary['newly_narrowed_cases']
        assert summary['required_column_demand_matched'] is True
        assert work_review['details'] == review_details
        assert work_review['groups'] == {key:dict(value) for key,value in review_groups.items()}
        assert work_review['changed_counter_occurrences'] == dict(review_changes)
        assert work_review['maximum_peak_reservation_increase'] == max(
            row['resource']['execution_resource_observed_native_peak_reserved_bytes']['delta'] for row in review_details)
        assert work_review['maximum_retained_reservation_increase'] == max(
            row['resource']['execution_resource_observed_native_reserved_bytes']['delta'] for row in review_details)
        assert math.fsum(all_before) == cohort['baseline_engine_seconds'] == summary['baseline_engine_seconds']
        assert math.fsum(all_after) == summary['successful_engine_seconds'] == summary['complete_cohort_engine_seconds']
        assert summary['lower_complete_cohort_engine_time'] == (math.fsum(all_after) < math.fsum(all_before))
        assert {key:len(value) for key,value in groups.items()} == expected['groups']
        for group, values in groups.items():
            assert summary['groups'][group] == dict(cases=len(values),
                baseline_engine_seconds=math.fsum(row[0] for row in values),
                candidate_engine_seconds=math.fsum(row[1] for row in values),
                cases_with_higher_engine_time=sum(row[1]>row[0] for row in values))
        runtime_name = prefix+label+'-runtime-r1-identity.json'
        runtime = read(runtime_name)
        validation = read(prefix+label+'-validation-r1.json')
        build_name = prefix+runtime['build_receipt']
        build = read(build_name)
        assert runtime['status'] == validation['status'] == build['status'] == 'passed'
        assert build['returncode'] == 0 and build['sources_unchanged']
        assert build['direct_child_reaped'] and build['process_group_drained']
        assert build['command'] == ['cargo','build','--offline','--locked','--release',
            '--features','release-user-surfaces','-p','shardloom-cli','-p','shardloom-python']
        assert runtime['source_commit'] == validation['source_commit'] == summary['source_commit']
        assert runtime['source_tree'] == validation['source_tree'] == summary['source_tree']
        assert runtime['source_commit'] == manifest['source_commits'][label]
        proof = protocol['runtime_binding']['runtime_proof']
        assert proof['head'] == runtime['source_commit'] and proof['source_tree'] == runtime['source_tree']
        assert proof['source_fingerprint_sha256'] == runtime['source_fingerprint_sha256']
        for path,record in proof['proofs'].items():
            assert path in origins, path
            linked(record,origins[path])
        for record in proof['runtime'].values():
            assert {key:record[key] for key in ('bytes','sha256')} == runtime['files'][record['path']]
        linked(validation['runtime'],runtime_name)
        linked(runtime['build_receipt_identity'],build_name)
        fingerprint = hashlib.sha256(json.dumps(build['source_files'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
        assert fingerprint == runtime['source_fingerprint_sha256']
        assert validation['native_validation_scope'] == 'full_workspace_all_targets_default_and_native'
        assert validation['full_native_library_suite_completed'] is True
        required_checks = {label+'-'+name+'.json' for name in (
            'default-clippy-r1','default-tests-r1','fmt-'+expected['fmt'],
            'installed-wheel-r1','native-clippy-'+expected['lint'],'native-tests-r1')}
        assert set(validation['checks']) == required_checks
        for check_name, binding in validation['checks'].items():
            linked(binding,prefix+check_name)
            check = read(prefix+check_name)
            assert check['status'] == 'passed' and check['returncode'] == 0
            assert check['sources_unchanged'] and check['process_group_drained'] and check['direct_child_reaped']
            assert check['source_files'] == build['source_files']
            if '-tests-' in check_name or '-clippy-' in check_name:
                command = ['cargo','test' if '-tests-' in check_name else 'clippy',
                           '--offline','--locked','--workspace','--all-targets']
                if '-native-' in check_name:
                    command += ['--features','python-native,native-encoded-exchange-experiment']
                if '-clippy-' in check_name:
                    command += ['--','-D','warnings']
                assert check['command'] == command
            elif '-fmt-' in check_name:
                assert check['command'] == ['cargo','fmt','--all','--','--check']
        for check_name,binding in validation['installed_checks'].items():
            linked(binding,prefix+check_name)
        counts = validation['test_counts']
        assert counts[label+'-default-tests-r1']['passed'] == 3325
        assert counts[label+'-native-tests-r1']['passed'] == expected['native_tests']
        assert counts[label+'-native-tests-r1']['ignored'] == 39
        for mode,passed,ignored,binaries in [('default',3325,0,108),('native',expected['native_tests'],39,120)]:
            check_name=label+'-'+mode+'-tests-r1'
            log=saved[prefix+'logs/'+check_name+'.log'].decode()
            reported=[tuple(map(int,match)) for match in re.findall(
                r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',log)]
            assert len(reported)==binaries
            assert tuple(sum(row[index] for row in reported) for index in range(3))==(passed,0,ignored)
            assert counts[check_name]==dict(passed=passed,failed=0,ignored=ignored,test_binaries=binaries)
            if mode=='native':
                assert (expected['native_library_tests'],0,36) in reported
        installed = read(prefix+label+'-installed-wheel-r1/report.json')
        assert installed['status'] == installed['uninstall_status'] == 'passed'
        assert installed['integration'] == dict(status='passed',tests=442,failures=0,errors=0,skipped=0)
        assert installed['publication_attempted'] is False
        assert read(prefix+label+'-installed-wheel-r1/integration.json') == installed['integration']
        binaries = {Path(path).name:record for path,record in runtime['files'].items()}
        assert installed['integrity']['native_library_sha256'] == binaries['_native.abi3.so']['sha256']
        assert installed['integrity']['cli_sha256'] == binaries['shardloom']['sha256']
        selected_sets[label] = ids
        outcomes[label] = dict(cases=len(candidate), source_commit=summary['source_commit'],
            baseline_engine_seconds=math.fsum(all_before), candidate_engine_seconds=math.fsum(all_after),
            matched_source_column_cases=expected['matched'], newly_narrowed_cases=expected['narrowed'])
    assert not selected_sets['sort'] & selected_sets['cardinality']
    assert selected_sets['sort'] | selected_sets['cardinality'] == full_prior_certificates.keys()
    return dict(status='passed',query_executions=0,
        manifest_sha256=identity(manifest_raw)['sha256'],verified_archive_members=len(saved),
        cohorts=outcomes, combined_engine_total=None,
        scope='Exact saved identities, dependency counts, pipeline records, clocks and source-bound checks. '
              'Complete answers were independently replayed locally against original reference ledgers; '
              'this portable verifier does not repeat that answer replay or execute queries.')


if __name__ == '__main__':
    root = Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent
    print(json.dumps(verify(root),indent=2))
