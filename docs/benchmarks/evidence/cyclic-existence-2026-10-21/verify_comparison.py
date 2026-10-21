# SPDX-License-Identifier: Apache-2.0
"""Record/replay the complete cyclic-existence comparison without native execution."""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[3]
sys.path.insert(0, str(REPOSITORY / 'scripts'))
import native_cyclic_existence_compare as compare
import native_cyclic_existence_workload as workload

CHECKS = {
    'cyclic-existence-format-851-a1': 'passed',
    'cyclic-existence-format-851-a2': 'passed',
    'cyclic-existence-format-851-a3': 'passed',
    'cyclic-existence-format-851-a4': 'passed',
    'cyclic-existence-candidate-tests-851-a1': 'passed',
    'cyclic-existence-candidate-tests-851-a2': 'failed',
    'cyclic-existence-candidate-tests-851-a3': 'passed',
    'cyclic-existence-candidate-clippy-851-a1': 'failed',
    'cyclic-existence-candidate-clippy-851-a2': 'passed',
    'cyclic-existence-comparison-tests-851-a1': 'passed',
    'cyclic-existence-native-all-targets-851-a1': 'passed',
    'cyclic-existence-fmt-852-a1': 'passed',
    'cyclic-existence-comparison-tests-852-a1': 'passed',
    'cyclic-existence-native-clippy-852-a1': 'passed',
    'cyclic-existence-workspace-clippy-852-a1': 'passed',
    'cyclic-existence-workspace-tests-852-a1': 'passed',
    'cyclic-existence-candidate-public-build-852-a1': 'passed',
    'cyclic-existence-candidate-public-prerequisite-852-a1': 'passed',
    'cyclic-existence-comparison-852-a1': 'passed',
}
FILES = (
    'cyclic-existence-failed-denial-test-851-a2.rs',
    'cyclic-existence-candidate-build-input-852.json',
    'cyclic-existence-candidate-build-852.json',
    'cyclic-existence-scoring-protocol-852.json',
    'cyclic-existence-postcomparison-source-852.json',
    'post_cyclic_comparison_852.py',
    'freeze_cyclic_candidate_852.py',
    'run_guarded_observed_757.py',
)
SCORE_CHECKS = {
    'cyclic-existence-candidate-tests-851-a3',
    'cyclic-existence-native-all-targets-851-a1',
    'cyclic-existence-native-clippy-852-a1',
    'cyclic-existence-fmt-852-a1',
    'cyclic-existence-workspace-clippy-852-a1',
    'cyclic-existence-workspace-tests-852-a1',
    'cyclic-existence-comparison-tests-852-a1',
    'cyclic-existence-candidate-public-prerequisite-852-a1',
}
MANIFEST = HERE / 'comparison.json'
PACKET = HERE / 'comparison.json.xz'


def identity(data):
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}


def stored(record):
    return {key: record[key] for key in ('bytes', 'sha256')}


def require(value, detail):
    compare.require(value, detail)


def checked_members(packet):
    members = packet['members']
    for name, row in members.items():
        require(identity(row['text'].encode()) == stored(row), f'changed member: {name}')
    return members


def verify_sources(revision, sources):
    require(len(sources) == 2065 and len({row['file'] for row in sources}) == 2065,
            'candidate source closure changed')
    request = ''.join(f"{revision}:{row['file']}\n" for row in sources).encode()
    output = subprocess.check_output(['git', 'cat-file', '--batch'], input=request, cwd=REPOSITORY)
    position = 0
    for row in sources:
        newline = output.index(b'\n', position)
        header = output[position:newline].split()
        require(len(header) == 3 and header[1] == b'blob', f"missing committed source: {row['file']}")
        end = newline + 1 + int(header[2])
        require(header[0].decode() == row['git_blob']
                and identity(output[newline + 1:end]) == stored(row), f"changed committed source: {row['file']}")
        require(output[end:end + 1] == b'\n', 'malformed source boundary')
        position = end + 1
    require(position == len(output), 'extra source bytes')


def verify_artifacts(build):
    require(build['status'] == 'passed' and build['source_unchanged'] is True,
            'build lacks unchanged-source proof')
    artifacts = {item['target']['name']: item for item in build['artifacts']}
    require(set(artifacts) == {'_native', 'shardloom_vortex', 'shardloom_runtime'},
            'missing actual native compiler artifacts')
    for item in artifacts.values():
        require(item['fresh'] is False and item['profile']['opt_level'] == '3'
                and item['profile']['test'] is False, 'candidate was not freshly compiled without test observers')


def verify_reported_resources(result, cell):
    snapshots = [result['initial'], result['final'], result['after_prepared_release']]
    snapshots += [call['after_result_release'] for call in result['calls']]
    for snapshot in snapshots:
        require(snapshot['memory_limit_bytes'] == workload.GRANT
                and snapshot['admitted_max_parallelism'] == 1, 'reported resource grant changed')
        require(type(snapshot['reserved_bytes']) is int
                and 0 <= snapshot['reserved_bytes'] <= snapshot['peak_reserved_bytes'] <= workload.GRANT,
                'reported native reservation exceeded the declared grant')
    require(result['initial']['completed_executions'] == 0
            and result['final']['completed_executions'] == cell['calls'], 'completed native call count changed')
    for index, call in enumerate(result['calls']):
        require(call['after_result_release']['completed_executions'] == index + 1,
                'native prepared call was omitted or repeated')


def verify_workflow_receipts(summary, protocol, members):
    schedule = protocol['comparison']['schedule']
    require(summary['schema'] == compare.SCHEMA and summary['status'] == 'passed'
            and len(summary['workflow_receipts']) == len(summary['workflows']) == 520,
            'comparison incomplete')
    protocol_identity = stored(members['cyclic-existence-scoring-protocol-852.json'])
    require(summary['protocol'] == protocol_identity, 'summary used another protocol')
    for index, (cell, result, receipt) in enumerate(zip(schedule, summary['workflows'], summary['workflow_receipts'])):
        verify_reported_resources(result, cell)
        name = f'workflows/workflow-{index:03d}'
        raw = members[name + '.json']
        child = json.loads(raw['text'])
        require(child['schema'] == compare.SCHEMA and child['status'] == 'passed'
                and child['index'] == index and child['protocol'] == protocol_identity,
                'missing or substituted child receipt')
        require(child['library'] == protocol['libraries'][cell['role']]
                and child['workflow'] == result, 'child build or workflow was substituted')
        require(receipt['index'] == index and receipt['result'] == stored(raw)
                and receipt['log'] == stored(members[name + '.log'])
                and compare.nonnegative(receipt['diagnostic_process_seconds']), 'child evidence identity changed')


def replay(manifest, packet):
    members = checked_members(packet)
    expected_names = set(FILES) | {name + suffix for name in CHECKS for suffix in ('.json', '.log')}
    expected_names |= {'workflows/comparison.json'}
    expected_names |= {f'workflows/workflow-{index:03d}{suffix}' for index in range(520) for suffix in ('.json', '.log')}
    expected_names |= {'drivers/' + name for name in (
        'native_cyclic_existence_compare.py', 'native_cyclic_existence_workload.py',
        'query_timing_report.py', 'test_native_cyclic_existence_compare.py')}
    require(set(members) == expected_names, 'packet member coverage changed')
    get = lambda name: json.loads(members[name]['text'])
    protocol = get('cyclic-existence-scoring-protocol-852.json')
    require(protocol['schema'] == 'shardloom.cyclic_existence_scoring_protocol.v1', 'changed protocol schema')
    intake_manifest_data = (HERE / 'intake.json').read_bytes()
    intake_packet_data = (HERE / 'intake.json.xz').read_bytes()
    require(identity(intake_manifest_data) == manifest['intake_manifest']
            and identity(intake_packet_data) == manifest['intake_packet'], 'ordinary intake was substituted')
    spec = importlib.util.spec_from_file_location('cyclic_intake_replay', HERE / 'verify.py')
    intake_replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(intake_replay)
    intake_manifest = json.loads(intake_manifest_data)
    intake_packet = json.loads(lzma.decompress(intake_packet_data))
    require(intake_replay.replay(intake_manifest, intake_packet) == intake_manifest['replay'],
            'ordinary intake failed replay')
    original_members = intake_packet['members']
    original_row = original_members['cyclic-existence-intake-protocol-850-v3.json']
    original = json.loads(original_row['text'])
    require(protocol['intake_protocol']['sha256'] == compare.INTAKE_SHA256
            and stored(protocol['intake_protocol']) == stored(original_row), 'changed original intake protocol')
    require(protocol['comparison'] == original['comparison'] == compare.comparison_contract(),
            'changed schedule or numerical gates')
    require(protocol['workload'] == original['workload']
            and workload.canonical(protocol['workload']) == workload.canonical(workload.manifest()),
            'changed independent workload')
    require(set(protocol['libraries']) == set(protocol['builds']) == {'ordinary', 'candidate'}, 'missing role')
    require(protocol['libraries']['ordinary'] == original['library']
            and protocol['libraries']['ordinary']['sha256'] != protocol['libraries']['candidate']['sha256'],
            'substituted or identical library roles')
    ordinary_row = original_members['cyclic-existence-baseline-build-850.json']
    candidate_row = members['cyclic-existence-candidate-build-852.json']
    for role, row in (('ordinary', ordinary_row), ('candidate', candidate_row)):
        require(stored(protocol['builds'][role]) == stored(row), 'changed build identity')
        build = json.loads(row['text'])
        verify_artifacts(build)
        require(build['library'] == protocol['libraries'][role], 'library does not match compiler proof')
    inputs = get('cyclic-existence-candidate-build-input-852.json')
    build = json.loads(candidate_row['text'])
    require(inputs['head'] == manifest['candidate_commit'] and inputs['dirty'] == []
            and inputs['source_inputs'] == build['source_inputs'], 'source changed before/after compilation')
    post = get('cyclic-existence-postcomparison-source-852.json')
    require(post['status'] == 'passed' and post['head'] == inputs['head']
            and post['source_inputs'] == inputs['source_inputs'], 'compiled source changed during comparison')
    require(stored(post['comparison']) == stored(members['workflows/comparison.json'])
            and stored(post['guard']) == stored(members['cyclic-existence-comparison-852-a1.json'])
            and stored(post['helper']) == stored(members['post_cyclic_comparison_852.py']),
            'post-comparison verification was substituted')
    require(stored(build['input']) == stored(members['cyclic-existence-candidate-build-input-852.json']),
            'changed pre-build inventory')
    verify_sources(inputs['head'], inputs['source_inputs'])
    for owner in (inputs, build, protocol):
        require(stored(owner['helper']) == stored(members['freeze_cyclic_candidate_852.py']), 'changed build/freezing helper')
    for name, status in CHECKS.items():
        record, log = get(name + '.json'), members[name + '.log']
        require(record['status'] == status and (record['returncode'] == 0) == (status == 'passed')
                and record['process_group_drained'] is True and record['log_sha256'] == log['sha256'],
                f'changed check: {name}')
        require(record['driver_sha256'] == members['run_guarded_observed_757.py']['sha256'], 'changed workload guard')
    require(set(protocol['checks']) == SCORE_CHECKS, 'missing prerequisite verification')
    for name, row in protocol['checks'].items():
        require(stored(row) == stored(members[name + '.json']), 'substituted verification receipt')
    build_check = get('cyclic-existence-candidate-public-build-852-a1.json')
    require(build_check['head'] == inputs['head'] and build_check['working_tree'] == '', 'build did not use frozen commit')
    require(stored(build['build_receipt']) == stored(members['cyclic-existence-candidate-public-build-852-a1.json'])
            and stored(build['build_log']) == stored(members['cyclic-existence-candidate-public-build-852-a1.log']),
            'compiler evidence substituted')
    compiler_log = members['cyclic-existence-candidate-public-build-852-a1.log']['text']
    emitted = [json.loads(line) for line in compiler_log.splitlines() if line.startswith('{')]
    for item in build['artifacts']:
        require(item in emitted, 'compiler artifact missing from actual build log')
    expected_python = {row['file']: stored(row) for row in inputs['source_inputs'] if row['file'].startswith('python/')}
    require(protocol['python_sources'] == expected_python, 'public source coverage changed')
    for name, row in protocol['drivers'].items():
        require(stored(members['drivers/' + name]) == row
                and compare.identity(REPOSITORY / 'scripts' / name) == row, 'driver source changed')
    require('Ran 7 tests' in members['cyclic-existence-comparison-tests-852-a1.log']['text']
            and 'Ran 3 tests' in members['cyclic-existence-candidate-public-prerequisite-852-a1.log']['text'],
            'required comparison/public tests missing')
    for name, count, ignored in (('cyclic-existence-native-all-targets-851-a1', 3079, 31),
                                 ('cyclic-existence-workspace-tests-852-a1', 3235, 0)):
        rows = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', members[name + '.log']['text'])
        require(sum(int(row[0]) for row in rows) == count and sum(int(row[1]) for row in rows) == 0
                and sum(int(row[2]) for row in rows) == ignored, 'broad test coverage changed')
    counter_lines = re.findall(r'CYCLIC_EXISTS_WORK (\{[^\n]+\})', members['cyclic-existence-candidate-tests-851-a3.log']['text'])
    observed_work = [json.loads(line) for line in counter_lines]
    require(len(observed_work) == 8, 'actual candidate work counters missing')
    expected_work = []
    for case in workload.TARGETS:
        for call in range(2):
            visits = 262144 if case in ('rare', 'absent') else 4096
            output = 0 if case == 'absent' else 4096
            expected_work.append(dict(case=case, call=call, executions=1, t_build_rows=4096,
                s_build_rows=8192 if case == 'skew_wide' else 4096, r_rows=4096,
                s_visits=visits, t_probes=visits, witness_exits=output, r_gathered_rows=output))
    require(sorted(observed_work, key=lambda row: (row['case'], row['call']))
            == sorted(expected_work, key=lambda row: (row['case'], row['call'])), 'actual work changed')
    summary = get('workflows/comparison.json')
    verify_workflow_receipts(summary, protocol, members)
    decision = compare.score(summary['workflows'], protocol)
    require(summary['decision'] == decision, 'recorded decision differs from fixed-gate replay')
    progress = [json.loads(line) for line in members['cyclic-existence-comparison-852-a1.log']['text'].splitlines() if line.startswith('{')]
    require(progress == [dict(completed_workflows=index + 1, **cell)
                         for index, cell in enumerate(protocol['comparison']['schedule'])], 'actual workflow schedule changed')
    return dict(status='passed', candidate_commit=inputs['head'], source_files=2065,
        workflows=520, engine_calls=1300, score_cells=len(decision['cells']),
        verification_receipts=len(CHECKS), preserved_failed_checks=2,
        candidate_work_observations=len(observed_work), retention_gate_passed=decision['passed'],
        maximum_final_denied_reservations=max(row['final']['denied_reservations'] for row in summary['workflows']),
        maximum_peak_reserved_bytes=max(row['final']['peak_reserved_bytes'] for row in summary['workflows']),
        failed_engine_cells=[dict(cohort=row['cohort'], case=row['case'], calls=row['calls'])
                             for row in decision['cells'] if not row['engine_pass']],
        failed_complete_workflow_cells=[dict(cohort=row['cohort'], case=row['case'], calls=row['calls'])
                                       for row in decision['cells'] if not row['complete_workflow_pass']])


def record(evidence):
    require(not MANIFEST.exists() and not PACKET.exists(), 'comparison packet already exists')
    members = {}

    def add(path, name):
        raw = path.read_bytes()
        members[name] = dict(**identity(raw), text=raw.decode())

    for name in FILES:
        add(evidence / name, name)
    for name in CHECKS:
        for suffix in ('.json', '.log'):
            add(evidence / (name + suffix), name + suffix)
    for path in sorted((evidence / 'cyclic-existence-comparison-852-a1').iterdir()):
        require(path.is_file(), 'unexpected comparison directory')
        add(path, 'workflows/' + path.name)
    protocol = json.loads(members['cyclic-existence-scoring-protocol-852.json']['text'])
    for name in protocol['drivers']:
        add(REPOSITORY / 'scripts' / name, 'drivers/' + name)
    inputs = json.loads(members['cyclic-existence-candidate-build-input-852.json']['text'])
    manifest = dict(schema='shardloom.cyclic_existence_comparison_evidence.v1',
        candidate_commit=inputs['head'], intake_manifest=identity((HERE / 'intake.json').read_bytes()),
        intake_packet=identity((HERE / 'intake.json.xz').read_bytes()),
        scope='Finite private candidate comparison; public-package acceptance and release are separate.')
    packet = dict(members=members)
    manifest['replay'] = replay(manifest, packet)
    raw = json.dumps(packet, sort_keys=True, allow_nan=False).encode()
    compressed = lzma.compress(raw)
    require(lzma.decompress(compressed) == raw, 'compressed packet failed exact readback')
    manifest['packet'] = dict(file=PACKET.name, members=len(members), **identity(compressed))
    with PACKET.open('xb') as stream:
        stream.write(compressed)
    with MANIFEST.open('x') as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(manifest['replay'], sort_keys=True))


def verify():
    manifest = json.loads(MANIFEST.read_bytes())
    compressed = PACKET.read_bytes()
    require(identity(compressed) == stored(manifest['packet']), 'packet identity changed')
    packet = json.loads(lzma.decompress(compressed))
    require(len(packet['members']) == manifest['packet']['members'], 'packet coverage changed')
    result = replay(manifest, packet)
    require(result == manifest['replay'], 'summary differs from replay')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--record', type=Path)
    args = parser.parse_args()
    record(args.record) if args.record else verify()
