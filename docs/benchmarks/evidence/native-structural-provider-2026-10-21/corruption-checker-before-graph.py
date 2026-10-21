# SPDX-License-Identifier: Apache-2.0
"""Challenge the structural evidence replay with bounded record corruptions."""
import argparse
from collections import deque
import copy
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import re


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def replace_member(packet, path, raw):
    row = packet['members'][path]
    assert row['encoding'] == 'utf8'
    old = dict(path=path, bytes=row['bytes'], sha256=row['sha256'])
    packet['members'][path] = dict(data=raw.decode(), encoding='utf8', bytes=len(raw), sha256=sha(raw))
    return old, dict(path=path, bytes=len(raw), sha256=sha(raw))


def repair_references(packet, first):
    # Apply identity transitions in creation order. A newer transition for one
    # member must not overtake an older one and leave references at its middle
    # hash when several changed records share an ancestor.
    pending = deque([first])
    changed = set()
    while pending:
        old, new = pending.popleft()
        marker = old['path'], old['sha256']
        if marker in changed:
            continue
        changed.add(marker)

        def rewrite(value):
            if isinstance(value, dict):
                result = {key: rewrite(child) for key, child in value.items()}
                if all(value.get(key) == old[key] for key in ('path', 'bytes', 'sha256')):
                    result.update(new)
                for key, child in value.items():
                    if key.endswith('_sha256') and child == old['sha256'] and value.get(key[:-7]) == old['path']:
                        result[key] = new['sha256']
                return result
            if isinstance(value, list):
                return [rewrite(child) for child in value]
            return value

        for key in list(packet):
            if key != 'members':
                packet[key] = rewrite(packet[key])
        for name, member in list(packet['members'].items()):
            if not name.endswith('.json') or member['encoding'] != 'utf8' or old['sha256'] not in member['data']:
                continue
            value = json.loads(member['data'])
            updated = rewrite(value)
            if name.removesuffix('.json') + '.log' == old['path'] and isinstance(value, dict):
                if value.get('log_sha256') == old['sha256']:
                    updated['log_sha256'] = new['sha256']
                    updated['output_bytes'] = new['bytes']
            if updated != value:
                raw = (json.dumps(updated, sort_keys=True, allow_nan=False) + '\n').encode()
                pending.append(replace_member(packet, name, raw))


def mutate_json(packet, path, change):
    value = json.loads(packet['members'][path]['data'])
    change(value)
    repair_references(packet, replace_member(packet, path, (json.dumps(value, sort_keys=True, allow_nan=False) + '\n').encode()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--verifier', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_bytes())
    raw = (args.manifest.parent / manifest['packet']['file']).read_bytes()
    assert len(raw) == manifest['packet']['bytes'] and sha(raw) == manifest['packet']['sha256']
    assert sha(args.verifier.read_bytes()) == manifest['verifier']['sha256']
    packet = json.loads(lzma.decompress(raw))
    spec = importlib.util.spec_from_file_location('structural_evidence_replay', args.verifier)
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    source = json.loads(packet['members'][packet['source']['path']]['data'])
    blobs = checker.source_blobs(args.repository, packet['code_commit'], sorted(source['source_hashes']))
    assert checker.verify(packet, args.repository, blobs) == manifest['replay']
    evidence = checker.Evidence(packet)
    plan = evidence.linked(packet['plan'])
    accepted = {name: evidence.linked(ref) for name, ref in packet['acceptance'].items()}
    phases = {name: evidence.linked(ref) for name, ref in packet['phases'].items()}
    native_result = evidence.linked(packet['core_gates']['metadata-locators-native-release-880'])
    prior = evidence.linked(source['previous_source'])
    cli_failure = evidence.linked(prior['cli_failure'])
    cases = []

    def json_case(name, path, change, expected):
        cases.append((name, lambda candidate: mutate_json(candidate, path, change), expected))

    json_case('source-closure-omission', packet['source']['path'],
              lambda value: value['source_hashes'].pop('Cargo.lock'), 'source closure incomplete')
    cases.append(('required-native-gate-omission', lambda value: value['core_gates'].pop('metadata-locators-native-release-880'), 'required gate missing'))
    json_case('notice-only-bridge-denial', packet['source']['path'],
              lambda value: value.update(unchanged_all_compiled_and_test_inputs=False), 'notice source bridge changed')
    json_case('cleanup-original-test-substitution', prior['cli_failure']['path'],
              lambda value: value.update(original_test=value['original_test'] + '\n// substituted\n'), 'unrelated CLI test changed')
    json_case('missing-process-drain', phases['native-cli']['run']['receipt']['path'],
              lambda value: value.update(process_group_drained=False), 'guard did not drain')
    json_case('wheel-test-omission', phases['wheel']['report']['path'],
              lambda value: value['integration'].update(tests=117), 'installed proof incomplete')
    json_case('public-case-omission', accepted['public']['summary']['path'],
              lambda value: value['cases'].pop(), 'public coverage incomplete')
    json_case('full43-query-duplicate', accepted['full43']['summary']['path'],
              lambda value: value['records'][1].update(query=value['records'][0]['query']), 'Full43 query omitted or duplicated')
    json_case('full43-resource-substitution', accepted['full43']['summary']['path'],
              lambda value: value.update(memory_gb=23), 'Full43 resources changed')
    json_case('full43-clock-substitution', accepted['full43']['summary']['path'],
              lambda value: value['records'][0].update(engine_execution_seconds=value['records'][0]['engine_execution_seconds'] + 1), 'Full43 clock changed')

    def corrupt_reference(value):
        row = value['records'][0]
        row['values'][0][0] += 1
        row['values_sha256'] = checker.canonical(row['values'])

    json_case('full43-complete-value-substitution', plan['reference']['path'], corrupt_reference, 'Full43 values changed')
    json_case('private-package-origin-substitution', plan['dependency_environment']['path'],
              lambda value: value['origins'][0]['files'][0].update(sha256='0' * 64), 'private package copy changed')
    json_case('historical-failure-omission', packet['history_inventory']['path'],
              lambda value: value['records'].pop(), 'historical record omitted')
    timing = {name: evidence.linked(ref) for name, ref in packet['timing_diagnosis'].items()}
    cases.append(('timing-history-omission', lambda value: value['timing_diagnosis'].pop('targeted'), 'timing history omitted'))
    json_case('targeted-group-omission', packet['timing_diagnosis']['targeted']['path'],
              lambda value: value['groups'].pop(), 'timing group omitted or reordered')
    json_case('targeted-full43-mislabel', packet['timing_diagnosis']['targeted']['path'],
              lambda value: value.update(full43_result=True), 'targeted result mislabeled Full43')
    json_case('isolated-clock-substitution', timing['isolated']['summary']['path'],
              lambda value: value['records'][0].update(engine_execution_seconds=value['records'][0]['engine_execution_seconds'] + 1), 'Full43 clock changed')
    json_case('isolated-workload-substitution', timing['isolated']['plan']['path'],
              lambda value: value.update(parallel_work=True), 'isolated run scope changed')
    json_case('isolated-cleanup-denial', timing['isolated']['run']['path'],
              lambda value: value.update(uat_lock_released=False), 'isolated cleanup or identity failed')
    json_case('diagnostic-ratio-substitution', packet['timing_diagnosis']['comparison']['path'],
              lambda value: value.update(candidate_to_control_total_ratio=0), 'comparison ratio changed')

    def omit_new_test(candidate):
        path = native_result['log']['path']
        text = candidate['members'][path]['data']
        pattern = r'(?m)^test [^\n]*::native_structural_metadata_layout_cache_requires_admission_before_construction \.\.\. ok$'
        text, n = re.subn(pattern, lambda match: match.group(0).removesuffix('ok') + 'ignored', text)
        assert n == 1
        repair_references(candidate, replace_member(candidate, path, text.encode()))

    cases.append(('new-native-regression-omission', omit_new_test, 'new native test did not pass'))
    results = []
    for name, change, expected in cases:
        candidate = {key: copy.deepcopy(value) for key, value in packet.items() if key != 'members'}
        candidate['members'] = dict(packet['members'])
        change(candidate)
        try:
            checker.verify(candidate, args.repository, blobs)
        except (ValueError, KeyError, AssertionError) as error:
            message = str(error)
            assert expected in message, (name, expected, message)
            results.append(dict(name=name, status='rejected', diagnostic=message))
        else:
            raise AssertionError('accepted corrupted evidence: ' + name)
    assert checker.verify(packet, args.repository, blobs) == manifest['replay']
    result = dict(schema='shardloom.structural_acceptance_corruption_checks.v1', status='passed',
                  packet_sha256=manifest['packet']['sha256'], verifier_sha256=manifest['verifier']['sha256'],
                  checker_sha256=sha(Path(__file__).read_bytes()),
                  cases=results, rejected=len(results), valid_packet_replayed_before_and_after=True,
                  native_execution_performed=False)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(dict(status='passed', rejected=len(results))))


if __name__ == '__main__':
    main()
