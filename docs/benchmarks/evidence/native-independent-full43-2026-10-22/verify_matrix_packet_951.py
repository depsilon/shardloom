"""Replay the corrected 22-task matrix and its interruption without engine execution."""
import argparse
from contextlib import contextmanager
import fnmatch
import gzip
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path, PurePosixPath
import posixpath
import sys
from types import SimpleNamespace


def require(value, reason):
    if not value:
        raise ValueError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def helpers(repository):
    directory = repository / 'docs/benchmarks/evidence/perf12-comparisons-2026-10-22'
    sys.path[:0] = [str(directory), str(repository), str(repository / 'scripts')]
    legacy = load_module('retained_matrix_helpers', directory / 'verify_packet_929.py')
    payload = load_module('retained_payload_helpers', directory / 'verify_collected_payloads.py')
    correction = load_module('retained_correction_helpers', repository /
        'docs/benchmarks/evidence/native-independent-full43-2026-10-22/verify_correctness_packet_950.py')
    return legacy, payload, correction


@contextmanager
def recorded_validator(evidence, original_root, repository):
    import check_benchmark_artifact_completeness as validator

    class RecordedPath(PurePosixPath):
        def resolve(self, *args, **kwargs):
            require(self.is_absolute(), 'recorded path is not absolute')
            return RecordedPath(posixpath.normpath(str(self)))

        def stat(self):
            return SimpleNamespace(st_size=evidence.identity(self)['bytes'])

        def read_bytes(self):
            return evidence.raw(self)

        def read_text(self, encoding='utf-8', errors=None):
            return self.read_bytes().decode(encoding, errors or 'strict')

        def glob(self, pattern):
            for name in sorted(evidence.members.keys() | evidence.omitted.keys()):
                path = RecordedPath(name)
                if path.parent == self and fnmatch.fnmatchcase(path.name, pattern):
                    yield path

    saved = {key: getattr(validator, key) for key in ('Path', 'ROOT', 'load_json', 'file_sha256')}
    validator.Path = RecordedPath
    validator.ROOT = RecordedPath(original_root)
    validator.load_json = lambda path: evidence.read(path)
    validator.file_sha256 = lambda path: evidence.identity(path)['sha256']
    try:
        yield validator, RecordedPath
    finally:
        for name, value in saved.items():
            setattr(validator, name, value)


def replay(repository, manifest, packet, *, check_current_runtime=False):
    legacy, payload, correction = helpers(repository)

    class Evidence(legacy.Evidence):
        def read(self, path):
            raw = self.raw(path)
            return correction.strict_json(gzip.decompress(raw) if str(path).endswith('.gz') else raw)

    evidence = Evidence(packet)
    require(manifest['status'] == 'built_and_byte_verified_not_primary_acceptance', 'manifest scope differs')
    require(manifest['member_count'] == len(evidence.members)
            and manifest['omitted_payload_count'] == len(evidence.omitted), 'member coverage differs')
    evidence.check(manifest['builder'])
    evidence.check(manifest['verifier'])
    require(digest(Path(__file__).read_bytes()) == manifest['verifier']['sha256'], 'matrix verifier source differs')
    for item in manifest['trusted_replay_sources']:
        raw = (repository / item['repository_path']).read_bytes()
        require(len(raw) == item['bytes'] and digest(raw) == item['sha256'], 'trusted replay source changed')
    protocol = evidence.read(evidence.check(manifest['protocol']))
    accepted = evidence.read(evidence.check(manifest['acceptance']))
    original = evidence.read(evidence.check(accepted['matrix']))
    continuation = evidence.read(evidence.check(accepted['continuation']))
    plan = evidence.read(evidence.check(accepted['continuation_plan']))
    require(protocol['status'] == 'frozen_before_execution', 'original protocol missing')
    require(accepted['status'] == 'passed_corrected_declared_matrix_with_retained_interruption', 'acceptance scope differs')
    require(accepted['uninterrupted_single_run_claimed'] is False
            and accepted['incomplete_cohort_records_counted_as_acceptance'] == 0
            and accepted['guard_limits_changed'] is False
            and not any(accepted['claims'].values()), 'unearned acceptance claim')
    require(accepted['source_and_baseline_environment_unchanged'] is True
            and accepted['lock_released'] is True and not accepted['native_processes_remaining'],
            'completion verification missing')
    require(accepted['protocol'] == manifest['protocol'], 'acceptance protocol differs')
    require(accepted['source_commit'] == original['harness_commit'] == protocol['source_commit'], 'harness source differs')
    require(accepted['runtime_source_commit'] == original['runtime_commit'] == protocol['runtime_source_commit'], 'runtime source differs')
    require(accepted['binary_sha256'] == original['binary_sha256'] == protocol['runtime']['sha256'], 'executable differs')
    require(original['build_manifest_sha256'] == protocol['runtime_binding']['sha256']
            and original['driver_sha256'] == protocol['driver']['sha256'], 'original driver binding differs')
    require(protocol['driver_bytes_unchanged'] is False, 'retention change erased')
    for key in ('runtime', 'driver', 'original_driver', 'runtime_binding', 'compactor',
                'compacted_log_reader', 'identity_program', 'verification_program', 'supervisor_program',
                'freeze_program', 'compaction_rehearsal', 'prior_protocol', 'prior_verification',
                'preflight_failure', 'preflight_failure_log'):
        evidence.check(protocol[key])
    require(original['status'] == 'failed' and len(original['cohorts']) == 13
            and 'call-000034.pid' in original['error'], 'log-retention interruption erased')
    interruption = evidence.read(evidence.check(accepted['log_retention_interruption']))
    require(interruption['status'] == 'identified_post_cohort_log_retention_interruption'
            and interruption['interruption'] == original['error']
            and interruption['completed_native_reference_pairs'] == 594
            and interruption['remaining_cohort_ordinals'] == list(range(14, 19)),
            'interruption attribution differs')
    require(continuation['status'] == 'passed_remaining_five_cohorts'
            and continuation['plan'] == accepted['continuation_plan']
            and len(continuation['cohorts']) == 5
            and continuation['lock_released'] and not continuation['owned_native_processes_remaining'],
            'remaining-cohort completion differs')
    require(plan['status'] == 'frozen_before_remaining_execution'
            and plan['original_protocol'] == manifest['protocol']
            and plan['original_failed_matrix'] == accepted['matrix']
            and plan['interruption'] == accepted['log_retention_interruption']
            and plan['completed_cohorts_repeated'] is False
            and plan['remaining_ordinals'] == list(range(14, 19))
            and plan['remaining_schedule'] == protocol['schedule'][13:]
            and plan['compiled_source_workload_and_environment_unchanged'] is True
            and plan['guard_limits_changed'] is False
            and plan['resources'] == protocol['resources'], 'continuation contract differs')
    for item in plan['helpers'] + [plan['original_supervision'], accepted['verifier']]:
        evidence.check(item)
    supervisor = evidence.read(plan['original_supervision']['path'])
    require(supervisor['status'] == 'failed' and supervisor['returncode'] == 1
            and supervisor['process_group_drained'] is True, 'interrupted process cleanup differs')
    binding = evidence.read(protocol['runtime_binding']['path'])
    runtime = evidence.read(evidence.check(binding['runtime_verification']))
    build = evidence.read(evidence.check(binding['build_receipt']))
    evidence.check(binding['build_plan'])
    require(binding['status'] == 'verified_corrected_runtime_source_binding'
            and binding['compiled_sources_equal_to_verified_corrected_build'] is True
            and binding['fresh_build_for_matrix_claimed'] is False
            and binding['source_commit'] == runtime['source_commit'] == protocol['runtime_source_commit']
            and binding['source_tree'] == runtime['source_tree']
            and runtime['executable'] == protocol['runtime'], 'corrected runtime binding differs')
    require(binding['source_hashes'] == protocol['runtime_source_hashes']
            and len(binding['source_hashes']) == 870, 'runtime source coverage differs')
    require(build['status'] == 'passed' and build['returncode'] == 0 and build['process_group_drained']
            and build['source_before'] == build['source_after'], 'corrected build verification differs')
    for name, expected in binding['source_hashes'].items():
        if name.endswith('.rs') or Path(name).name in ('Cargo.toml', 'Cargo.lock'):
            require(build['source_after'][name] == expected, 'compiled file binding differs')
        else:
            require(digest(evidence.raw(manifest['original_repository'] + '/' + name)) == expected,
                    'Python runtime binding differs')
        if check_current_runtime:
            require(digest((repository / name).read_bytes()) == expected, 'current runtime source changed')
    correctness_directory = repository / 'docs/benchmarks/evidence/native-independent-full43-2026-10-22'
    correctness_review = correction.strict_json((correctness_directory / 'review.json').read_bytes())
    correctness_packet = (correctness_directory / 'records.json.xz').read_bytes()
    for name, raw in [('review', (correctness_directory / 'review.json').read_bytes()), ('packet', correctness_packet)]:
        expected = manifest['independent_full43'][name]
        require(len(raw) == expected['bytes'] and digest(raw) == expected['sha256'], 'independent correctness evidence differs')
    require(correctness_review['runtime_source_commit'] == runtime['source_commit']
            and correctness_review['native_runs'][-1]['runtime'] == binding['runtime_verification'],
            'matrix does not use the independently checked runtime')
    correction.verify(correctness_review, correction.strict_json(lzma.decompress(correctness_packet)), repository)
    baseline = protocol['baseline']
    before = evidence.read(evidence.check(baseline['environment_observation']))
    after = evidence.read(evidence.check(accepted['environment_after']))
    require(before == after, 'baseline environment bytes changed')
    require(baseline['engine'] == 'pandas' and baseline['version'] == accepted['baseline_version'] == '3.0.3',
            'baseline engine/version differs')
    require(before['distributions'] == baseline['distributions']
            and before['installed_distributions'] == baseline['installed_distributions']
            and before['python'] == baseline['python']
            and before['interpreter']['path'] == baseline['interpreter'], 'baseline metadata differs')
    require(len(before['distributions']) == 10 and len(before['installed_distributions']) == 9
            and {**before['installed_distributions'], 'shardloom': '0.7.0'} == before['distributions'],
            'baseline distribution origins differ')
    inventory = before['files']
    require(len(inventory) == len({row['path'] for row in inventory}) == 6832
            and digest(json.dumps(inventory, sort_keys=True).encode()) == before['inventory_sha256'],
            'baseline file inventory differs')
    evidence.check(baseline['install_report'])
    cohorts = original['cohorts'] + continuation['cohorts']
    require(len(cohorts) == len(accepted['cohorts']) == len(protocol['schedule']) == 18, 'cohort count differs')
    require(len(protocol['workload_declarations']) == len(accepted['tasks']) == 22, 'task count differs')
    require(len(accepted['completed_log_compactions']) == 12
            and accepted['retained_raw_log_cohorts'] == list(range(13, 19)), 'log retention coverage differs')
    for index, ref in enumerate(accepted['completed_log_compactions'], 1):
        item = evidence.read(evidence.check(ref))
        cohort = cohorts[index - 1]
        require(item['status'] == 'passed_verified_compaction' and item['cohort'] == index
                and item['report']['path'] == cohort['report']
                and item['report']['sha256'] == cohort['report_sha256']
                and item['program_sha256'] == protocol['compactor']['sha256']
                and item['same_log_limit_bytes'] == protocol['resources']['max_log_bytes']
                and item['all_members_reopened_and_hash_verified']
                and not item['open_handles'] and not item['guard_limits_changed']
                and not item['runtime_or_workload_changed'], 'completed compaction differs')
        require(cohort['completed_log_compaction']['manifest'] == ref
                and len(item['members']) == cohort['completed_log_compaction']['members_verified'],
                'compacted member coverage differs')
        for member in item['members']:
            evidence.check(member)
        evidence.check(item['archive'])
    original_root = manifest['original_repository']
    require(original_root == protocol['command'][protocol['command'].index('--repo') + 1], 'repository binding differs')
    for name, expected in protocol['harness_source_hashes'].items():
        require(digest((repository / name).read_bytes()) == expected
                and digest(evidence.raw(original_root + '/' + name)) == expected, 'harness source changed')
    validator_name = 'scripts/check_benchmark_artifact_completeness.py'
    require((repository / validator_name).read_bytes() == evidence.raw(original_root + '/' + validator_name),
            'completeness validator changed')
    from benchmarks.traditional_analytics.workloads import WORKLOADS
    from run_clickbench_query_uat import equivalent
    tasks = {name: [] for name in protocol['workload_declarations']}
    collected = []
    count = 0
    with recorded_validator(evidence, original_root, repository) as (validator, RecordedPath):
        for spec, cohort, observation in zip(protocol['schedule'], cohorts, accepted['cohorts']):
            require(cohort['command'] == spec['command'], 'cohort command differs')
            require(cohort['receipt']['returncode'] == 0 and not cohort['receipt']['guard_failures'], 'cohort execution failed')
            require(cohort['report'] == observation['report']['path']
                    and cohort['report_sha256'] == observation['report']['sha256'], 'accepted report differs')
            evidence.check(observation['report'])
            blockers, report = validator.validate_manifest(RecordedPath(cohort['report']))
            require(not blockers, 'complete original validator rejected report: ' + str(blockers))
            require(report['status'] == 'passed' and report['summary']['complete'], 'incomplete report')
            require(report['harness_sources'] == protocol['harness_source_hashes']
                    and report['workload_declarations'] == protocol['workload_declarations'], 'report source/declarations differ')
            require(report['binary']['sha256'] == protocol['runtime']['sha256'], 'report executable differs')
            config = report['configuration']
            require((config['rows'], config['dim_rows'], config['dataset_profile'], config['repeats'],
                     config['timeout'], config['memory_gb'], config['max_parallelism']) ==
                    (1000, 20, 'tiny_smoke', 1, 60.0, 1, 2), 'scale or resource settings differ')
            require(all(config[key] == spec[key] for key in ('input_state', 'output_format', 'formats')),
                    'cohort dimensions differ')
            require(report['engines'] == ['pandas', 'shardloom'] and report['reference_engine'] == 'pandas',
                    'reference role differs')
            require(report['resource_policy'] == {'native_memory_gb': 1, 'native_max_parallelism': 2,
                    'sampled_process_tree_rss_limit_bytes': 1610612736,
                    'rss_is_sampled_not_an_allocator_guarantee': True}, 'resource policy differs')
            records = report['records']
            keyed = {(row['engine'], row['format'], row['scenario'], row['repeat']): (index, row)
                     for index, row in enumerate(records)}
            expected = {(engine, fmt, name, 1) for engine in ('pandas', 'shardloom')
                        for fmt in spec['formats'] for name in tasks}
            require(len(records) == spec['expected_records'] == len(keyed) and set(keyed) == expected,
                    'record coverage differs')
            for record in records:
                if record['engine'] != 'shardloom':
                    continue
                calls = correction.strict_json(record['evidence']['benchmark_native_calls'])
                for call in calls:
                    require(validator._option(call['command'], '--memory-gb') == '1'
                            and validator._option(call['command'], '--max-parallelism') == '2',
                            'native resources differ')
                if spec['output_format'] == 'collect':
                    workload = WORKLOADS[record['scenario']]
                    statements = [call for call in calls if validator._option(call['command'], '--request') == 'collect']
                    require(len(statements) == len(workload.statements), 'collected statement coverage differs')
                    for index, call in enumerate(statements):
                        require(digest(evidence.raw(call['stdout'])) == call['stdout_sha256'], 'collected stdout differs')
                        checked = payload.check_payload(evidence.read(call['stdout']), workload, index)
                        collected.append({'task': record['scenario'], **checked})
            for fmt in spec['formats']:
                for name in tasks:
                    ni, native = keyed['shardloom', fmt, name, 1]
                    bi, reference = keyed['pandas', fmt, name, 1]
                    require(bi < ni and reference['engine_version'] == '3.0.3', 'reference order/version differs')
                    require(equivalent(native['result'], reference['result']), 'complete result values differ')
                    tasks[name].append({'cohort': spec['ordinal'], 'input_format': fmt,
                        'baseline_input_format': reference['comparison_input_format'],
                        'native_record_index': ni, 'baseline_record_index': bi,
                        'native_normalized_result_sha256': native['result_sha256'],
                        'baseline_normalized_result_sha256': reference['result_sha256'],
                        'status': 'passed', 'matches_reference': True})
            count += len(records)
    require(tasks == accepted['tasks'] and all(len(rows) == 32 for rows in tasks.values()), 'per-task comparisons differ')
    require(count == 1408 and accepted['complete_result_pairs'] == accepted['native_records'] == 704
            and accepted['baseline_records'] == 704, 'matrix totals differ')
    require(len(collected) == 368 and len({row['task'] for row in collected}) == 22
            and sum(row['terminal_order_checked'] for row in collected) == 48, 'original collect coverage differs')
    return {'status': 'passed_corrected_matrix_portable_replay', 'complete_result_pairs': 704,
            'cohorts': 18, 'tasks': 22, 'logical_schema_statements': 368, 'terminal_order_statements': 48,
            'observed_null_values': sum(row['null_values'] for row in collected),
            'interruption_retained': True, 'completed_cohorts_repeated': False,
            'uninterrupted_single_run_claimed': False, 'runtime_executed': False,
            'omitted_physical_payloads_reopened': False, 'current_runtime_source_checked': check_current_runtime,
            'independent_full43_runtime_binding_verified': True,
            'members': len(evidence.members), 'omitted_payload_identities': len(evidence.omitted),
            'performance_or_broad_parity_claim': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--check-current-runtime', action='store_true')
    args = parser.parse_args()
    _, _, correction = helpers(args.repository.resolve())
    manifest = correction.strict_json(args.manifest.read_bytes())
    raw = args.manifest.parent.joinpath(Path(manifest['archive']['path']).name).read_bytes()
    require(len(raw) == manifest['archive']['bytes'] and digest(raw) == manifest['archive']['sha256'], 'packet bytes changed')
    result = replay(args.repository.resolve(), manifest, correction.strict_json(lzma.decompress(raw)),
                    check_current_runtime=args.check_current_runtime)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
