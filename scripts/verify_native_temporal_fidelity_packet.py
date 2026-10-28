# SPDX-License-Identifier: Apache-2.0
"""Verify temporal acceptance from a checkout without executing archived code."""
from pathlib import Path
import argparse
import json
import re
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'python/src'),
               str(REPO / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from query_surface.replay import MAX_ARCHIVE, MAX_FILE, bounded_bytes, need, object_files, sha

HEAD = '34d7ee14a6e22734fbb400bae67eaf8a225b64ac'
ARCHIVE_SHA = 'b596e28365db1343d16583094fb0764dfa81c65b6c63339e6d8d34606c12b39a'
GATES = ('r49-default-clippy', 'r51-native-clippy', 'r52-default-workspace-tests',
         'r59-datetime-provider-tests', 'r61-datetime-provider-clippy',
         'r62-catalog-adapter-tests', 'r63-native-workspace-tests',
         'r64-final-temporal-build', 'r65-freeze-final-temporal-runtime',
         'r66-final-temporal-public', 'r67-wheel-tools', 'r68-installed-wheel')
FAILED = {2, 4, 5, 9, 10, 13, 18, 19, 20, 21, 22, 23, 24, 25, 31, 35, 36,
          39, 41, 42, 48, 50, 53, 55, 56, 57, 58, 60}
PUBLIC_NAMES = [
    'test_temporal_metadata', 'test_temporal_declarations', 'test_adbc_temporal',
    *['test_native_shared_provider_binding.NativeSharedProviderBindingTests.test_' + name
      for name in ('temporal_calendar_workflows_match_all_five_surfaces',
                   'temporal_text_casts_preserve_whitespace_on_all_five_surfaces',
                   'temporal_python_rows_batches_and_retained_arrow_keep_exact_ticks',
                   'temporal_nested_null_parents_persist_exact_types_and_ticks',
                   'temporal_bad_streamed_values_preserve_destination_and_zone_admission')]]


def verify(files, manifest):
    need(manifest['verification_kind'] == 'temporal_fidelity'
         and manifest['source_head'] == HEAD, 'temporal packet identity changed')

    def raw(name):
        key = 'evidence/' + name
        need(key in files, 'missing linked evidence: ' + name)
        return files[key]

    def load(name):
        return strict_json(raw(name))

    gates = {}
    for name in GATES:
        gate = load(name + '.json')
        need(gate['status'] == 'passed' and gate['returncode'] == 0,
             'required gate failed: ' + name)
        need(gate['process_group_drained'] is True and gate['direct_child_reaped'] is True
             and gate['timed_out'] is False and gate['interrupted_signal'] is None,
             'gate process cleanup incomplete: ' + name)
        need(sha(raw(name + '.log')) == gate['log_sha256'], 'gate log changed: ' + name)
        gates[name] = {'command': gate['command'], 'head': gate['head'],
                       'receipt_sha256': sha(raw(name + '.json')),
                       'log_sha256': gate['log_sha256'], 'status': 'passed'}

    totals = {}
    for name, expected in [('r52-default-workspace-tests', (3274, 0, 0)),
                           ('r63-native-workspace-tests', (6301, 0, 35)),
                           ('r59-datetime-provider-tests', (89, 0, 0))]:
        log = raw(name + '.log').decode()
        rows = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
        need(rows and all(row[0] == 'ok' for row in rows), 'failed or absent test results: ' + name)
        count = tuple(sum(int(row[index]) for row in rows) for index in (1, 2, 3))
        need(count == expected, 'test coverage changed: ' + name)
        totals[name] = dict(zip(('passed', 'failed', 'ignored'), count))
    ignored = [line for line in raw('r63-native-workspace-tests.log').decode().splitlines()
               if ' ... ignored' in line]
    need(len(ignored) == 35, 'native ignore inventory changed')

    build = load('r64-final-temporal-build.json')
    need(build['head'] == HEAD and build['working_tree'] == ''
         and build['changed_source_inventory'] == {}, 'final build source changed')
    need(build['command'] == ['cargo', 'build', '--locked', '--offline', '-p', 'shardloom-cli',
                             '-p', 'shardloom-python', '--features', 'release-user-surfaces',
                             '--profile', 'ci-native'], 'final build configuration changed')
    runtime = load('temporal-native-r5/manifest.json')
    need(runtime['profile'] == 'ci-native'
         and runtime['build_receipt'] == 'r64-final-temporal-build.json'
         and runtime['build_receipt_sha256'] == sha(raw(runtime['build_receipt']))
         and set(runtime['files']) == {'shardloom', '_native.abi3.so'},
         'runtime build binding changed')
    public = load('temporal-public-r5.json')
    need({k: public[k] for k in ('status', 'tests', 'errors', 'failures', 'skipped')}
         == dict(status='passed', tests=22, errors=0, failures=0, skipped=0),
         'public temporal matrix incomplete')
    need(public['test_names'] == PUBLIC_NAMES, 'public temporal cases changed')
    need(public['runtime_manifest_sha256'] == sha(raw('temporal-native-r5/manifest.json'))
         and public['products'] == {name: item['sha256'] for name, item in runtime['files'].items()},
         'public temporal runtime changed')

    package = load('installed-wheel-r1/report.json')
    need(package['status'] == package['uninstall_status'] == 'passed'
         and package['publication_attempted'] is False and package['installation_performed'] is True
         and package['installed_load']['isolated_install'] is True,
         'installed package lifecycle incomplete')
    need(package['integration'] == dict(status='passed', tests=199, failures=0, errors=0, skipped=0),
         'installed package test coverage changed')
    need(package['integrity']['cli_sha256'] == runtime['files']['shardloom']['sha256']
         and package['integrity']['native_library_sha256'] == runtime['files']['_native.abi3.so']['sha256'],
         'installed package used another runtime')
    for step in package['steps']:
        need(step['returncode'] == 0
             and sha(raw('installed-wheel-r1/' + step['log'])) == step['log_sha256'],
             'package step failed or log changed')
    source = load('native-test-source-binding.json')
    need(source['source_head'] == HEAD and source['status'] == 'passed'
         and source['gate_receipt_sha256'] == sha(raw('r63-native-workspace-tests.json')),
         'native test source binding changed')
    native = load('r63-native-workspace-tests.json')
    need(source['final_source_records'] == native['changed_source_inventory'],
         'native tests and final source differ')

    observations, numbers = {}, set()
    for path in sorted(files):
        match = re.fullmatch(r'evidence/r(\d+)-[^/]+\.json', path)
        if not match:
            continue
        value = strict_json(files[path])
        if 'status' not in value or 'command' not in value:
            continue
        number = int(match.group(1))
        need(number not in numbers, 'duplicate historical observation')
        numbers.add(number)
        expected_status = 'failed' if number in FAILED else 'passed'
        need(value['status'] == expected_status
             and ((value.get('returncode') != 0) if number in FAILED else value.get('returncode') == 0),
             'historical observation disposition changed: ' + path)
        observations[path] = {key: value.get(key) for key in
                             ('status', 'returncode', 'error', 'timed_out', 'interrupted_signal')}
        if 'log_sha256' in value:
            need(sha(files[path[:-5] + '.log']) == value['log_sha256'], 'historical log changed')
    # The original receipt inventory has no r26; preserve its numbering.
    need(numbers == set(range(1, 69)) - {26}, 'historical observations incomplete')
    return dict(schema_version='shardloom.temporal_fidelity_acceptance.v1', status='passed',
        source_head=HEAD, build_profile='ci-native', required_local_gates=gates, test_totals=totals,
        ignored_native_tests=ignored, temporal_public_tests=22, installed_package_tests=199,
        runtime_files=runtime['files'], wheel=package['integrity'], observations=observations,
        performance_claim=False, package_publication_attempted=False, item_20_complete=False,
        limitations=['Current-host optimized checked build; hosted platform gates are recorded separately.',
            'Earlier local gates retain their actual HEAD and dirty-source inventories; they are not relabelled as clean builds.',
            'Thirty-two native ignores are explicit measurement/fixture helpers; three require Windows CI.',
            'No new 347-case or Full43 timing observation; prior measurements remain unchanged.',
            'Raw timestamp storage covers i64; calendar evaluation has its separately declared bounded domain.',
            'Later as-of/time-series, numeric, recursive-composition and explicit batch-function work remains open.',
            'Content hashes establish internal consistency, not signed provenance; large runtime/wheel payloads remain external.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / 'manifest.json', MAX_FILE))
    archive = bounded_bytes(args.packet / 'packet.tar.xz', MAX_ARCHIVE)
    need(sha(archive) == ARCHIVE_SHA, 'accepted temporal archive changed')
    files = object_files(manifest, archive)
    result = verify(files, manifest)
    need(result == strict_json(bounded_bytes(args.packet / 'acceptance.json', MAX_FILE)),
         'acceptance summary differs from replay')
    print(json.dumps({'status': 'passed', 'files': len(files),
                      'observations': len(result['observations']),
                      'temporal_public_tests': result['temporal_public_tests'],
                      'installed_package_tests': result['installed_package_tests']}))


if __name__ == '__main__':
    main()
