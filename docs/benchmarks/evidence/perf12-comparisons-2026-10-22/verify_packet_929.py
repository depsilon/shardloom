"""Replay exact declared-workflow evidence without executing engines or reopening retired payloads."""
import argparse
import base64
import fnmatch
import gzip
import hashlib
import json
import lzma
from pathlib import Path, PurePosixPath
import posixpath
import sys
from types import SimpleNamespace

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def require(value, reason):
    if not value:
        raise ValueError(reason)

def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON key: ' + key)
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('non-finite JSON value: ' + value)
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)

def check_environment(baseline, preflight, postflight):
    require(preflight['status'] == 'passed_import_and_dependency_checks'
        and postflight['status'] == 'passed_complete_distribution_observation'
        and postflight['original_run_observation'] is False
        and postflight['runtime_executed'] is False, 'baseline observation scope differs')
    require(postflight['distributions'] == baseline['distributions'],
        'complete baseline distribution mapping differs')
    require(preflight['distributions'] == postflight['installed_distributions'],
        'installed baseline distribution mapping differs')
    installed = postflight['installed_distributions']
    source = postflight['source_tree_distributions']
    require(set(source) == {'shardloom'} and not (installed.keys() & source.keys())
        and {**installed, **source} == postflight['distributions'],
        'baseline distribution origin mapping differs')
    metadata = postflight['metadata']
    require(len(metadata) == len(postflight['distributions'])
        and len({row['name'] for row in metadata}) == len(metadata)
        and {row['name']: row['version'] for row in metadata} == postflight['distributions'],
        'baseline metadata inventory differs')
    for origin, values in [('installed', installed), ('source_tree', source)]:
        require({row['name']: row['version'] for row in metadata if row['origin'] == origin} == values,
            'baseline metadata origins differ')
    require(postflight['python_full'] == baseline['python']
        and postflight['python'] == preflight['python'] == baseline['python'].split()[0]
        and postflight['interpreter'] == baseline['interpreter'], 'baseline interpreter differs')
    observer = postflight['observer_source'].encode()
    require(len(observer) == postflight['observer']['bytes']
        and digest(observer) == postflight['observer']['sha256'], 'baseline observer source differs')

class Evidence:
    def __init__(self, packet):
        require(type(packet['schema_version']) is int and packet['schema_version'] == 1, 'changed packet schema')
        require(packet['packet_status'] == 'evidence_packet_built_not_primary_acceptance'
            and not any(packet['claims'].values()), 'changed packet scope')
        self.members = {row['path']: row for row in packet['members']}
        require(len(self.members) == len(packet['members']), 'duplicate stored member')
        self.omitted = {row['path']: row for row in packet['omissions']}
        require(len(self.omitted) == len(packet['omissions']), 'duplicate omitted payload')
        require(not (self.members.keys() & self.omitted.keys()), 'payload both stored and omitted')
        for name, row in self.members.items():
            raw = self.raw(name)
            require(type(row['bytes']) is int and len(raw) == row['bytes'] and digest(raw) == row['sha256'], 'changed member: ' + name)
        for name, row in self.omitted.items():
            require(type(row['bytes']) is int and row['bytes'] >= 0 and isinstance(row['sha256'], str)
                and len(row['sha256']) == 64 and all(c in '0123456789abcdef' for c in row['sha256']), 'invalid omitted identity: ' + name)

    def raw(self, path):
        row = self.members[str(path)]
        require(row['encoding'] in ('utf-8', 'base64'), 'unknown member encoding')
        return row['data'].encode('utf-8') if row['encoding'] == 'utf-8' else base64.b64decode(row['data'], validate=True)

    def read(self, path):
        raw = self.raw(path)
        return strict_json(gzip.decompress(raw) if str(path).endswith('.gz') else raw)

    def identity(self, path):
        name = str(path)
        if name in self.members:
            return self.members[name]
        return self.omitted[name]

    def check(self, reference):
        row = self.identity(reference['path'])
        require(all(row[key] == reference[key] for key in ('bytes', 'sha256')), 'substituted reference: ' + reference['path'])
        return reference['path']

def replay(repository, manifest, packet):
    evidence = Evidence(packet)
    require(manifest['status'] == 'built_and_byte_verified_not_primary_acceptance', 'changed manifest status')
    require(manifest['member_count'] == len(evidence.members), 'member count differs')
    require(manifest['omitted_payload_count'] == len(evidence.omitted), 'omitted payload count differs')
    require(manifest['selected_top_level_json_py_names'] == packet['selected_top_level_json_py_names'],
        'selected source inventory differs')
    evidence.check(manifest['builder'])
    protocol = evidence.read(evidence.check(manifest['protocol']))
    accepted = evidence.read(evidence.check(manifest['acceptance']))
    continuation = evidence.read(evidence.check(manifest['continuation']))
    require(protocol['status'] == 'frozen_before_execution', 'missing original protocol')
    require(accepted['status'] == 'passed_declared_matrix_with_explicit_continuation', 'changed acceptance scope')
    require(continuation['status'] == 'passed_complete_replacement_cohort', 'missing complete replacement')
    require(accepted['source_commit'] == protocol['source_commit'], 'changed harness source')
    require(accepted['runtime_source_commit'] == protocol['runtime_source_commit'], 'changed runtime source')
    require(accepted['binary_sha256'] == protocol['runtime']['sha256'], 'changed runtime binary')
    require(accepted['baseline_version'] == protocol['baseline']['version'] == '3.0.3', 'changed baseline version')
    require(accepted['incomplete_cohort_records_counted_as_acceptance'] == 0
        and accepted['uninterrupted_single_run_claimed'] is False and accepted['guard_limits_changed'] is False,
        'failure or continuation erased')
    require(all(value is False for value in accepted['claims'].values()), 'unearned acceptance claim')
    evidence.check(protocol['runtime'])
    evidence.check(protocol['driver'])
    require(protocol['driver_bytes_unchanged'] is True
        and protocol['driver']['sha256'] == protocol['original_driver']['sha256'], 'driver identity differs')
    binding = evidence.read(evidence.check(protocol['runtime_binding']))
    build = evidence.read(evidence.check(binding['build_receipt']))
    require(binding['status'] == 'verified_reuse_of_published_runtime'
        and binding['runtime_sources_equal_to_release'] is True and binding['fresh_build_claimed'] is False,
        'runtime reuse evidence differs')
    require(binding['source_hashes'] == protocol['runtime_source_hashes'], 'runtime source inventory differs')
    require(build['status'] == 'passed' and build['source_tree_clean'] is True
        and build['source_commit'] == binding['source_commit'] == protocol['runtime_source_commit']
        and build['source_tree'] == binding['source_tree'], 'release source binding differs')
    require(build['binary_sha256'] == binding['binary_sha256'] == protocol['runtime']['sha256']
        and build['binary_bytes'] == protocol['runtime']['bytes'], 'release executable binding differs')
    for name, expected in protocol['runtime_source_hashes'].items():
        require(digest((repository / name).read_bytes()) == expected, 'runtime replay source changed: ' + name)
    evidence.check(protocol['baseline']['install_report'])
    environment = evidence.read(evidence.check(protocol['baseline']['environment_observation']))
    postflight_ref = manifest['baseline_environment_postflight']
    require(postflight_ref['path'] == 'baseline-environment-postflight.json', 'unexpected baseline observation path')
    postflight_raw = Path(__file__).with_name(postflight_ref['path']).read_bytes()
    require(len(postflight_raw) == postflight_ref['bytes']
        and digest(postflight_raw) == postflight_ref['sha256'], 'baseline postflight identity differs')
    postflight = strict_json(postflight_raw)
    require(postflight['protocol'] == manifest['protocol']
        and postflight['preflight'] == protocol['baseline']['environment_observation']
        and postflight['source_version']['sha256'] == protocol['runtime_source_hashes']['python/src/shardloom/_version.py'],
        'baseline postflight source binding differs')
    check_environment(protocol['baseline'], environment, postflight)
    evidence.check(accepted['verifier'])
    require(accepted['source_and_baseline_environment_unchanged'] is True,
        'original post-run environment verification missing')
    original = evidence.read(evidence.check(accepted['original_failed_matrix']))
    failed = evidence.read(evidence.check(accepted['original_incomplete_cohort']))
    require(original['status'] == failed['status'] == 'failed', 'failed observation changed')
    require(original['harness_commit'] == protocol['source_commit']
        and original['runtime_commit'] == protocol['runtime_source_commit']
        and original['binary_sha256'] == protocol['runtime']['sha256']
        and original['build_manifest_sha256'] == protocol['runtime_binding']['sha256']
        and original['driver_sha256'] == protocol['driver']['sha256'], 'original execution binding differs')
    require(failed['error'].startswith('StorageGuardError: UAT retained log budget exceeded:')
        and failed['summary']['complete'] is False, 'storage interruption changed')
    require(accepted['original_failure'] == failed['error'], 'failure reason differs')
    continued_protocol = evidence.read(evidence.check(accepted['continuation_protocol']))
    require(continued_protocol['partial_failed_cohort_reused'] is False
        and continued_protocol['runtime_baseline_workload_and_limits_unchanged'] is True, 'continuation scope differs')
    evidence.check(continued_protocol['original_protocol'])
    evidence.check(continued_protocol['original_failed_matrix'])
    evidence.check(continued_protocol['failed_cohort'])
    require(continued_protocol['replacement_cohort'] == protocol['schedule'][17], 'replacement declaration differs')
    cohorts = original['cohorts'][:17] + [continuation]
    require(len(protocol['schedule']) == len(accepted['cohorts']) == len(cohorts) == 18, 'cohort count differs')
    require(len(accepted['tasks']) == len(protocol['workload_declarations']) == 22, 'task count differs')
    original_root = protocol['command'][protocol['command'].index('--repo') + 1]
    require(PurePosixPath(original_root).is_absolute(), 'recorded repository is not absolute')
    for name, expected in protocol['harness_source_hashes'].items():
        require(digest((repository / name).read_bytes()) == expected, 'replay source changed: ' + name)
        require(digest(evidence.raw(original_root + '/' + name)) == expected, 'archived source changed: ' + name)
    validator_name = 'scripts/check_benchmark_artifact_completeness.py'
    require((repository / validator_name).read_bytes() == evidence.raw(original_root + '/' + validator_name), 'completeness validator changed')
    sys.path[:0] = [str(repository), str(repository / 'scripts')]
    import check_benchmark_artifact_completeness as validator
    from run_clickbench_query_uat import equivalent

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
    def frozen_json(path):
        raw = evidence.raw(path)
        return validator.strict_json((gzip.decompress(raw) if str(path).endswith('.gz') else raw).decode())
    validator.Path = RecordedPath
    validator.ROOT = RecordedPath(original_root)
    validator.load_json = frozen_json
    validator.file_sha256 = lambda path: evidence.identity(path)['sha256']
    count, native_count = 0, 0
    tasks = {name: [] for name in protocol['workload_declarations']}
    try:
        for spec, cohort, observed in zip(protocol['schedule'], cohorts, accepted['cohorts']):
            require(cohort['command'] == spec['command'], 'changed cohort command')
            require(cohort['receipt']['returncode'] == 0 and cohort['receipt']['guard_failures'] == [],
                'cohort process or guard failure')
            require(cohort['report'] == observed['report']['path'], 'accepted report differs')
            evidence.check(observed['report'])
            require(evidence.identity(cohort['report'])['sha256'] == cohort['report_sha256'], 'changed cohort report')
            blockers, report = validator.validate_manifest(RecordedPath(cohort['report']))
            require(not blockers, 'complete original validator rejected report: ' + str(blockers))
            require(report['harness_sources'] == protocol['harness_source_hashes'], 'changed report source')
            require(report['workload_declarations'] == protocol['workload_declarations'], 'changed workload declarations')
            require(report['binary']['sha256'] == protocol['runtime']['sha256'], 'different candidate')
            config = report['configuration']
            require((config['rows'], config['dim_rows'], config['dataset_profile'], config['repeats'],
                config['timeout'], config['memory_gb'], config['max_parallelism'])
                == (1000, 20, 'tiny_smoke', 1, 60.0, 1, 2), 'scale or resource settings differ')
            require(report['engines'] == ['pandas', 'shardloom']
                and report['reference_engine'] == 'pandas', 'independent reference differs')
            require(report['resource_policy'] == {'native_memory_gb': 1, 'native_max_parallelism': 2,
                'sampled_process_tree_rss_limit_bytes': 1610612736,
                'rss_is_sampled_not_an_allocator_guarantee': True}, 'resource policy differs')
            require(report['configuration']['input_state'] == spec['input_state']
                and report['configuration']['output_format'] == spec['output_format']
                and report['configuration']['formats'] == spec['formats'], 'cohort dimensions differ')
            records = report['records']
            require(len(records) == spec['expected_records'], 'record count differs')
            keyed = {(row['engine'], row['format'], row['scenario'], row['repeat']): (index, row)
                for index, row in enumerate(records)}
            for row in records:
                if row['engine'] == 'shardloom':
                    for call in strict_json(row['evidence']['benchmark_native_calls']):
                        require(validator._option(call['command'], '--memory-gb') == '1'
                            and validator._option(call['command'], '--max-parallelism') == '2',
                            'native call resource request differs')
            for fmt in spec['formats']:
                for name in tasks:
                    ni, native = keyed['shardloom', fmt, name, 1]
                    bi, reference = keyed['pandas', fmt, name, 1]
                    require(bi < ni and reference['engine_version'] == '3.0.3', 'reference version/order differs')
                    require(equivalent(native['result'], reference['result']), 'complete result values differ')
                    tasks[name].append({'cohort': spec['ordinal'], 'input_format': fmt,
                        'baseline_input_format': reference['comparison_input_format'],
                        'native_record_index': ni, 'baseline_record_index': bi,
                        'native_normalized_result_sha256': native['result_sha256'],
                        'baseline_normalized_result_sha256': reference['result_sha256'],
                        'status': 'passed', 'matches_reference': True})
                    native_count += 1
            count += len(records)
        require(tasks == accepted['tasks'] and all(len(rows) == 32 for rows in tasks.values()), 'per-task observations differ')
        require(count == 1408 and native_count == accepted['complete_result_pairs'] == 704, 'complete matrix count differs')
    finally:
        for name, value in saved.items():
            setattr(validator, name, value)
    return {'status': 'passed_portable_complete_value_replay', 'members': len(evidence.members),
        'omitted_payload_identities': len(evidence.omitted), 'cohorts': 18, 'tasks': 22,
        'complete_result_pairs': 704, 'failed_attempt_retained': True,
        'complete_baseline_distributions': len(postflight['distributions']),
        'installed_baseline_distributions': len(postflight['installed_distributions']),
        'source_tree_distributions': postflight['source_tree_distributions'],
        'supplemental_metadata_observation_is_original_run_evidence': False,
        'runtime_executed': False, 'omitted_physical_payloads_reopened': False,
        'performance_or_broad_parity_claim': False}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = strict_json(args.manifest.read_text())
    archive = args.manifest.parent / Path(manifest['archive']['path']).name
    raw = archive.read_bytes()
    require(len(raw) == manifest['archive']['bytes'] and digest(raw) == manifest['archive']['sha256'], 'packet bytes changed')
    result = replay(args.repository.resolve(), manifest, strict_json(lzma.decompress(raw)))
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))

if __name__ == '__main__':
    main()
