"""Verify the optimization-coverage observation packet without native execution."""
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
from collections import Counter, defaultdict

ACTIVATION_COUNTERS = {
    'source_buffer_transfer': 'intake_columnar_buffer_bytes_transferred',
    'materialized_relation_reuse': 'relational_materialized_reuses',
    'metadata_pruned_scan': 'scans_metadata_pruned',
    'rank_then_selected_payload_source': 'ranked_sources',
    'adaptive_native_scan_policy_exercised': 'adaptive_native_scan_completed_windows',
    'adaptive_native_scan_window_shrunk': 'adaptive_native_scan_shrunk_windows',
    'adaptive_native_scan_window_grown': 'adaptive_native_scan_grown_windows',
}


def identity(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def clock(value):
    assert type(value) in (int, float) and math.isfinite(value) and value >= 0
    return value


def verify(root):
    assert __debug__, 'verification requires assertions'
    manifest_raw = (root / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    assert manifest['schema'] == 'shardloom.native_optimization_coverage_evidence.v1'
    expected_names = set(manifest['files']) | {'manifest.json', 'verification.json'}
    actual_names = {str(path.relative_to(root)) for path in root.rglob('*') if path.is_file()}
    assert actual_names <= expected_names
    for name, expected in manifest['files'].items():
        relative = PurePosixPath(name)
        assert not relative.is_absolute() and '..' not in relative.parts
        path = root / name
        assert not path.is_symlink() and identity(path.read_bytes()) == expected, name
    archived = {}
    with tarfile.open(root / 'observations.tar.gz', 'r:gz') as archive:
        members = archive.getmembers()
        assert len({member.name for member in members}) == len(members)
        assert {member.name for member in members} == set(manifest['archive_members'])
        for member in members:
            assert member.isfile() and not PurePosixPath(member.name).is_absolute()
            assert '..' not in PurePosixPath(member.name).parts
            data = archive.extractfile(member).read()
            expected = manifest['archive_members'][member.name]
            assert identity(data) == {key: expected[key] for key in ('bytes', 'sha256')}, member.name
            archived[member.name] = data
    def read(name):
        return json.loads((root / name).read_bytes())
    def original(name):
        return json.loads(archived['current/' + name])
    summary, previous = read('summary.json'), read('previous-summary.json')
    assert summary['status'] == 'passed_offline_replay'
    assert summary['primary_clock'] == previous['primary_clock'] == 'engine_execution_seconds'
    assert summary['source_commit'] == manifest['source_commit']
    assert summary['source_tree'] == manifest['source_tree']
    assert summary['case_rows']['sha256'] == manifest['files']['cases.jsonl.gz']['sha256']
    assert summary['previous_case_rows']['sha256'] == manifest['files']['previous-cases.jsonl.gz']['sha256']
    assert summary['previous_summary']['sha256'] == manifest['files']['previous-summary.json']['sha256']
    assert summary['complete']['sha256'] == identity(archived['current/full10k-r1/complete.json'])['sha256']
    protocol = original('full10k-protocol-r1.json')
    assert summary['protocol']['sha256'] == identity(archived['current/full10k-protocol-r1.json'])['sha256']
    assert protocol['attempts_per_case'] == 1 and protocol['warmups'] == 0
    assert protocol['expected_attempt_count'] == protocol['required_complete_result_count'] == 10000
    for name in ('inputs', 'frozen'):
        assert protocol[name]['sha256'] == identity(archived['original/' + name + '.json'])['sha256']
    complete = original('full10k-r1/complete.json')
    assert complete['status'] == 'complete_observation' and complete['posthash_verified']
    assert complete['total'] == complete['attempt_slots'] == 10000
    assert complete['outcomes'] == {'passed': 10000}
    assert complete['attempts_per_case'] == 1 and complete['requested_repeats'] == 0
    supervision = original('full10k-supervision-r1.json')
    assert supervision['status'] == 'passed_capture' and supervision['returncode'] == 0
    assert supervision['process_group_drained'] and supervision['direct_child_reaped']
    assert supervision['runtime_and_source_unchanged']
    assert not supervision['timed_out'] and supervision['interrupted_signal'] is None
    design = json.loads(archived['original/design.json'])
    assert protocol['design']['sha256'] == identity(archived['original/design.json'])['sha256']
    assert len(design['cases']) == design['count'] == 10000
    audit = read('pipeline-audit.json')
    assert audit['status'] == 'passed' and audit['query_executions'] == 0
    assert audit['source_commit'] == summary['source_commit']
    for key in ('all_successful_cases_single_native_pipeline', 'all_catch_all_execution_false',
                'all_fallback_attempted_false', 'all_external_engine_invoked_false'):
        assert audit[key] is True, key
    certificates = original('full10k-r1/certificate-export-r1.json')
    assert certificates['status'] == 'passed' and certificates['query_executions'] == 0
    assert certificates['output'] == manifest['files']['certificates.jsonl.gz']
    assert certificates['ledger']['sha256'] == summary['ledger']['sha256']
    totals, old_totals, seen = [], [], set()
    family_current, family_previous, kernel_families = defaultdict(list), defaultdict(list), Counter()
    counts, maximum_operators, maximum_kernels = Counter(), 0, 0
    activation_counts = defaultdict(Counter)
    with gzip.open(root / 'cases.jsonl.gz', 'rt') as current_stream, \
         gzip.open(root / 'previous-cases.jsonl.gz', 'rt') as previous_stream, \
         gzip.open(root / 'certificates.jsonl.gz', 'rt') as certificates_stream:
        old_rows, proofs = iter(previous_stream), iter(certificates_stream)
        for seq, line in enumerate(current_stream):
            row, old, proof = json.loads(line), json.loads(next(old_rows)), json.loads(next(proofs))
            declared = design['cases'][seq]
            assert row['seq'] == old['seq'] == proof['seq'] == seq
            assert row['id'] == old['id'] == proof['id'] == declared['id']
            assert row['id'] not in seen
            seen.add(row['id'])
            assert row['current']['status'] == old['current']['status'] == proof['status'] == 'passed'
            for key in ('family', 'surface', 'profile'):
                assert row[key] == old[key] == declared[key], (row['id'], key)
            now, before = clock(row['current']['engine_seconds']), clock(old['current']['engine_seconds'])
            assert before == clock(row['previous_engine_seconds'])
            assert now == clock(proof['engine_seconds']) and proof['engine_timing_reported'] is True
            fields = proof['fields']
            assert fields['relational_provider_selection_reason'] == 'native_operator_pipeline'
            assert fields['fallback_attempted'] == fields['external_engine_invoked'] == 'false'
            basis = fields['local_primitive_native_io_pushdown_proof_basis']
            assert 'native_execution_pipeline=native_operator_pipeline' in basis
            assert 'native_pipeline_catch_all_execution=false' in basis
            def number(name):
                found = re.findall(r'\b' + re.escape(name) + r'=(\d+)\b', basis)
                assert len(found) == 1
                return int(found[0])
            operators = number('native_pipeline_operator_invocations')
            kernels = number('native_pipeline_file_kernel_invocations')
            assert 0 <= kernels <= operators and operators > 0
            pairs = re.findall(r'(?:^|[ ;])([a-z][a-z0-9_]*)=([0-9]+)(?=$|[ ;])', basis)
            assert len(pairs) == len(dict(pairs))
            metrics = {key:int(value) for key, value in pairs}
            group = 'with_file_kernel' if kernels else 'without_file_kernel'
            activation_counts['cases'][group] += 1
            for label, key in ACTIVATION_COUNTERS.items():
                if key in metrics:
                    activation_counts[label]['reported'] += 1
                    if metrics[key] > 0:
                        activation_counts[label]['positive'] += 1
                        activation_counts[label][group] += 1
            work = row['current']['work']
            projected = work.get('native_source_demand_projected_columns')
            available = work.get('native_source_demand_available_columns')
            if projected is not None and available is not None:
                activation_counts['direct_source_narrow_column_demand']['reported'] += 1
                if 0 < int(projected) < int(available):
                    activation_counts['direct_source_narrow_column_demand']['positive'] += 1
                    activation_counts['direct_source_narrow_column_demand'][group] += 1
            counts.update(cases=1, operator_invocations=operators, file_kernel_invocations=kernels)
            maximum_operators, maximum_kernels = max(maximum_operators, operators), max(maximum_kernels, kernels)
            if kernels:
                counts['cases_with_file_kernels'] += 1
                kernel_families[row['family']] += 1
            if 'native_source_demand_prepared_input_bytes_written' in fields:
                assert fields['native_source_demand_prepared_input_bytes_written'] == '0'
                counts['direct_source_cases_reporting_zero_prepared_bytes'] += 1
            totals.append(now)
            old_totals.append(before)
            family_current[row['family']].append(now)
            family_previous[row['family']].append(before)
        assert next(old_rows, None) is None and next(proofs, None) is None
    assert counts == Counter(audit['counts'])
    assert dict(kernel_families) == audit['file_kernel_case_families']
    assert maximum_operators == audit['maximum_operator_invocations_per_case']
    assert maximum_kernels == audit['maximum_file_kernel_invocations_per_case']
    assert len(seen) == summary['attempted_cases'] == summary['successful_cases'] == 10000
    assert summary['unsuccessful_cases'] == summary['newly_rejected_cases'] == summary['other_unsuccessful_cases'] == 0
    assert summary['failure_groups'] == [] and summary['outcomes'] == {'passed': 10000}
    assert summary['previous_unsuccessful_cases_engine_seconds'] == 0
    assert math.fsum(totals) == summary['successful_engine_seconds'] == summary['complete_10000_engine_seconds']
    assert math.fsum(old_totals) == summary['previous_same_successful_cases_engine_seconds'] == previous['complete_10000_engine_seconds']
    assert summary['attempts_per_case'] == 1 and summary['requested_repeats'] == 0
    assert set(family_current) == set(summary['families'])
    for name, values in family_current.items():
        assert summary['families'][name] == dict(
            successful_engine_seconds=dict(cases=len(values), engine_seconds=math.fsum(values)),
            previous_same_successful_cases_engine_seconds=dict(cases=len(values), engine_seconds=math.fsum(family_previous[name])))
    validation = read('validation-inventory.json')
    assert validation['receipt_count'] == len(validation['receipts'])
    assert validation['receipt_count'] > len(validation['final_accepted_check_set'])
    assert validation['discrepancies'] == []
    statuses = Counter()
    for record in validation['receipts']:
        raw = archived['current/' + record['name']]
        receipt = json.loads(raw)
        assert identity(raw) == record['identity']
        assert receipt['status'] == record['status'] and receipt.get('returncode') == record['returncode']
        statuses[receipt['status']] += 1
    assert dict(statuses) == validation['statuses'] and statuses['failed'] > 0
    runtime = original('runtime-r1-identity.json')
    admitted = original('validation-before-10k.json')
    build = original(runtime['build_receipt'])
    assert runtime['status'] == admitted['status'] == build['status'] == 'passed'
    assert runtime['source_commit'] == admitted['source_commit'] == summary['source_commit']
    assert runtime['source_tree'] == admitted['source_tree'] == summary['source_tree']
    assert admitted['runtime'] == identity(archived['current/runtime-r1-identity.json'])
    assert runtime['build_receipt_identity'] == identity(archived['current/' + runtime['build_receipt']])
    fingerprint = hashlib.sha256(json.dumps(build['source_files'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert fingerprint == runtime['source_fingerprint_sha256']
    for name, expected in admitted['checks'].items():
        assert identity(archived['current/' + name]) == expected
    for name, expected in admitted['installed_checks'].items():
        assert identity(archived['current/' + name]) == expected
    for name in validation['final_accepted_check_set']:
        receipt = original(name)
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['sources_unchanged'] and receipt['process_group_drained']
        assert receipt['source_files'] == build['source_files']
    assert admitted['native_validation_scope'] == 'focused_changed_owners_plus_all_cli_python_targets'
    assert admitted['full_native_library_suite_completed'] is False
    assert admitted['focused_native_tests_passed'] == 172
    assert set(admitted['checks']) == set(validation['final_accepted_check_set'])
    assert admitted['test_counts'] == {
        'default-tests-r1': dict(passed=3325, failed=0, ignored=0),
        'native-cli-python-tests-r1': dict(passed=1567, failed=0, ignored=3),
        'native-focused-final-r1': dict(passed=172, failed=0, ignored=0),
    }
    installed = original('installed-wheel-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == dict(status='passed', tests=442, failures=0, errors=0, skipped=0)
    assert installed['publication_attempted'] is False
    binaries = {Path(name).name: value for name, value in runtime['files'].items()}
    assert installed['integrity']['native_library_sha256'] == binaries['_native.abi3.so']['sha256']
    assert installed['integrity']['cli_sha256'] == binaries['shardloom']['sha256']
    activation = read('optimization-activation.json')
    assert activation['status'] == 'passed' and activation['query_executions'] == 0
    assert activation['source_commit'] == summary['source_commit']
    assert activation['counter_keys'] == ACTIVATION_COUNTERS
    assert activation['counts'] == dict(activation_counts)
    assert activation['inputs']['verified-cases-r1.jsonl.gz'] == manifest['files']['cases.jsonl.gz']
    assert activation['inputs']['certificate-cases-r1.jsonl.gz'] == manifest['files']['certificates.jsonl.gz']
    assert activation['overall_optimization_coverage_percent'] is None
    assert activation['overall_coverage_status'] == 'not_complete_existing_and_planned_mechanisms_remain_open'
    assert activation['counts']['cases'] == {
        'with_file_kernel': counts['cases_with_file_kernels'],
        'without_file_kernel': 10000 - counts['cases_with_file_kernels'],
    }
    return dict(status='passed', manifest_sha256=identity(manifest_raw)['sha256'],
                verified_packet_files=len(manifest['files']), verified_archive_members=len(archived),
                cases=len(seen), successful_engine_seconds=math.fsum(totals),
                previous_same_cases_engine_seconds=math.fsum(old_totals),
                unsuccessful_cases=0, query_executions=0,
                scope='Compact identities, counts, complete saved clock comparison, selected raw certificate fields and validation history; full answer correctness was replayed locally against the original reference ledgers and is not independently rerun by this compact verifier.')


if __name__ == '__main__':
    root = Path(sys.argv[1]) if len(sys.argv) == 2 else Path(__file__).resolve().parent
    print(json.dumps(verify(root), indent=2))
