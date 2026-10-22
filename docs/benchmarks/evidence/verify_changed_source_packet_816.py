"""Verify the finite refresh evidence without original files or executables.

Reconstruct the experimental sources from Git and the preserved delta, replay
the hash-pinned independent value oracle, and check every frozen decision with
exact rational comparisons. This validates records, not retired native payloads.
"""
import argparse
import ast
import base64
from fractions import Fraction
import hashlib
import io
import json
import lzma
from pathlib import Path
import statistics
import subprocess
import tempfile

EXPECTED_PACKET = 'a3f257a567fc218a347d91a93e6d52295894591ea3351e5ab0642313bb70e0d2'
BASE = '656e9508974358f9b3f532b249841c1e7b4df0f6'
ROOT = '/Users/dylan/LocalData/shardloom/conditional-investigations-20261019/'
ORIGINAL_REPOSITORY = '/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo/'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def reconstruct(repository, scratch, snapshot, archive):
    assert archive['head'] == snapshot['head'] == BASE
    with tempfile.TemporaryDirectory(prefix='changed-source-replay-', dir=scratch) as directory:
        work = Path(directory)
        patch = work / 'frozen.patch'
        patch.write_text(archive['diff'])
        numstat = subprocess.check_output(['git', 'apply', '--numstat', '--', str(patch)], cwd=work, text=True)
        changed = []
        for line in numstat.splitlines():
            name = line.split('\t', 2)[2]
            relative = Path(name)
            assert not relative.is_absolute() and '..' not in relative.parts
            path = work / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=repository))
            changed.append(name)
        assert len(changed) == 3
        subprocess.run(['git', 'apply', '--check', '--', str(patch)], cwd=work, check=True)
        subprocess.run(['git', 'apply', '--', str(patch)], cwd=work, check=True)
        for name, text in archive['untracked'].items():
            relative = Path(name)
            assert not relative.is_absolute() and '..' not in relative.parts
            path = work / relative
            assert not path.exists()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        assert len(archive['untracked']) == 11
        files = snapshot['files']
        assert len(files) == 1910
        base_names = [name for name in files if not (work / name).exists()]
        queries = ''.join(BASE + ':' + name + '\n' for name in base_names).encode()
        stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], input=queries, cwd=repository))
        checked = {}
        for name in base_names:
            object_id, kind, size = stream.readline().strip().split()
            assert len(object_id) == 40 and kind == b'blob'
            raw = stream.read(int(size))
            assert len(raw) == int(size) and stream.read(1) == b'\n'
            checked[name] = sha(raw)
        assert not stream.read()
        for name, expected in files.items():
            actual = checked[name] if name in checked else sha((work / name).read_bytes())
            assert actual == expected, name
    return {'files': 1910, 'changed_tracked': 3, 'added_sources_and_designs': 11,
            'unchanged_git_blobs': len(base_names)}


def load_oracle(raw):
    assert sha(raw) == '45ec996c876593a2582e5fe34813f4baf288e2684861534edac2e061c6c708df'
    module = ast.parse(raw.decode())
    names = {'finite', 'close', 'row', 'partition_ranges', 'content_identity', 'input_content',
             'verify_workflow', 'verify_pair', 'expected_grid'}
    selected = [node for node in module.body if isinstance(node, (ast.Import, ast.ImportFrom, ast.Assign))
                or isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {}
    exec(compile(ast.Module(body=selected, type_ignores=[]), '<pinned pure value oracle>', 'exec'), namespace)
    assert names <= namespace.keys()
    return namespace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    scratch = args.scratch.resolve(strict=True)
    assert scratch.is_dir() and not any(part in ('Desktop', 'Documents', 'CloudDocs', 'Mobile Documents') for part in scratch.parts)
    compressed = args.packet.read_bytes()
    assert len(compressed) == 1240480 and sha(compressed) == EXPECTED_PACKET
    packet = json.loads(lzma.decompress(compressed))
    assert packet['schema_version'] == 'shardloom.changed_source_results.v1'
    assert packet['base_commit'] == BASE and packet['production_admitted'] is False
    assert packet['outcome'] == 'reject_finite_unconditional_adapter'
    members = {}
    for member in packet['members']:
        assert member['path'] not in members
        raw = base64.b64decode(member['base64'], validate=True)
        assert len(raw) == member['bytes'] and sha(raw) == member['sha256']
        members[member['path']] = raw
    assert len(members) == 517 and sum(map(len, members.values())) == 34219733

    def raw_at(name):
        return members[name if Path(name).is_absolute() else ROOT + name]

    def read_at(name):
        return json.loads(raw_at(name))

    binding_raw = raw_at('changed-source-comparison-release-816-binding.json')
    binding = json.loads(binding_raw)
    snapshot_raw = raw_at(binding['source_manifest'])
    assert sha(snapshot_raw) == binding['source_manifest_sha256']
    snapshot = json.loads(snapshot_raw)
    archive = json.loads(lzma.decompress(raw_at('changed-source-comparison-debug-816-source.json.xz')))
    reconstruction = reconstruct(args.repository, scratch, snapshot, archive)
    protocol_name = 'docs/architecture/native-changed-source-comparison-design-2026-10-20.md'
    assert sha(raw_at(ORIGINAL_REPOSITORY + protocol_name)) == binding['protocol_sha256'] == snapshot['files'][protocol_name]
    assert sha(raw_at('run_changed_source_comparison_816.py')) == binding['driver_sha256']
    assert sha(raw_at('verify_changed_source_comparison_816.py')) == binding['verifier_sha256']
    assert sha(raw_at('run_guarded.py')) == binding['guard_sha256']
    build_raw = raw_at(binding['build_receipt'])
    build = json.loads(build_raw)
    assert sha(build_raw) == binding['build_receipt_sha256']
    assert build['status'] == 'passed' and build['process_group_drained'] and build['direct_child_reaped']
    build_log = raw_at(str(Path(binding['build_receipt']).with_suffix('.log')))
    assert sha(build_log) == binding['build_log_sha256'] == build['log_sha256']
    compiled = [json.loads(line) for line in build_log.decode().splitlines() if line.startswith('{')]
    for role, executable in binding['executables'].items():
        rows = [row for row in compiled if row.get('executable') == executable['path']]
        assert len(rows) == 1 and not rows[0]['fresh']
        assert rows[0]['features'] == executable['features'] and rows[0]['profile'] == executable['profile']
        assert executable['profile']['test'] and executable['profile']['opt_level'] == '3'
        assert role in ('ordinary', 'candidate')
    assert 'test result: ok. 10 passed; 0 failed; 2 ignored;' in raw_at('changed-source-comparison-release-tests-816.log').decode()
    oracle = load_oracle(raw_at('verify_changed_source_comparison_816.py'))
    values = {}
    counts = {'workflows': 0, 'timed_queries': 0, 'validation_queries': 0, 'validated_rows': 0}
    pids = set()
    for mode, tag in [('preflight', 'changed-source-comparison-preflight-816-a1'), ('measure', 'changed-source-comparison-measured-816-a1')]:
        run = read_at(tag + '/run.json')
        assert run['status'] == 'passed' and run['mode'] == mode and run['binding'] == binding
        assert run['binding_sha256'] == sha(binding_raw)
        assert run['driver_sha256'] == binding['driver_sha256'] and run['verifier_sha256'] == binding['verifier_sha256']
        grid = oracle['expected_grid'](mode)
        assert [(item['cohort'], item['case'], item['repeat'], item['role']) for item in run['workflows']] == grid
        for key, item in zip(grid, run['workflows'], strict=True):
            value_raw = raw_at(tag + '/' + item['name'] + '/result.json')
            assert sha(value_raw) == item['result_sha256']
            value = json.loads(value_raw)
            assert value['mode'] == mode and value['case'] == key[1] and value['role'] == key[3]
            assert value['build_sha256'] == binding['executables'][key[3]]['sha256']
            receipt_name = tag + '-' + item['name']
            receipt_raw = raw_at(receipt_name + '.json')
            receipt = json.loads(receipt_raw)
            log = raw_at(receipt_name + '.log')
            assert sha(receipt_raw) == item['receipt_sha256']
            assert sha(log) == receipt['log_sha256'] == item['log_sha256']
            assert receipt['status'] == 'passed' and receipt['process_group_drained'] and receipt['direct_child_reaped']
            assert value['pid'] == receipt['pid'] == item['pid'] and value['pid'] not in pids
            pids.add(value['pid'])
            assert 'test result: ok. 1 passed;' in log.decode()
            oracle['verify_workflow'](value)
            counts['workflows'] += 1
            for stage in value['stages']:
                counts['timed_queries'] += len(stage['consumer']['calls'])
                counts['validation_queries'] += len(stage['validation']['partitions'])
                counts['validated_rows'] += stage['validation']['rows']
            values[(mode, *key)] = value
        for cohort, case, repeat, role in grid:
            if role == 'ordinary':
                oracle['verify_pair'](values[(mode, cohort, case, repeat, role)], values[(mode, cohort, case, repeat, 'candidate')])
    assert counts == packet['counts'] == {'workflows': 84, 'timed_queries': 336, 'validation_queries': 1596, 'validated_rows': 12160512}
    decision_raw = raw_at('changed-source-comparison-measured-816-a1/verified.json')
    assert sha(decision_raw) == packet['decision_sha256']
    decision = json.loads(decision_raw)
    assert decision['decision'] == packet['outcome']
    decisions = []
    for row in decision['decisions']:
        medians = {}
        for role in ('ordinary', 'candidate'):
            samples = [values[('measure', row['cohort'], row['case'], repeat, role)] for repeat in range(3)]
            # The frozen scorer sums the component observations in Python. It
            # checks the stored Rust totals separately, within rounding error.
            # Preserve that scoring boundary rather than require bit identity
            # between two independently rounded sums of the same observations.
            scores = [oracle['verify_workflow'](sample) for sample in samples]
            measurements = {key: [score[key] for score in scores]
                            for key in ('complete', 'engine', 'initial', 'refresh')}
            medians[role] = {key: statistics.median(Fraction(str(value)) for value in samples) for key, samples in measurements.items()}
            for key, value in medians[role].items():
                assert float(value) == row['medians'][role][key]
        ratios = {key: medians['candidate'][key] / medians['ordinary'][key] for key in medians['ordinary']}
        storage = []
        for repeat in range(3):
            for index in range(2):
                ordinary = values[('measure', row['cohort'], row['case'], repeat, 'ordinary')]['stages'][index]['written']['bytes']
                candidate = values[('measure', row['cohort'], row['case'], repeat, 'candidate')]['stages'][index]['written']['bytes']
                storage.append({'repeat': repeat, 'stage': index, 'ordinary_bytes': ordinary, 'candidate_bytes': candidate,
                                'extra_bytes': candidate - ordinary, 'passed': candidate * 100 <= ordinary * 105 + 65536 * 100})
        assert storage == row['storage']
        target = next(case[4] for case in oracle['CASES'] if case[0] == row['case'])
        assert target == row['target']
        gates = {'complete': ratios['complete'] <= (Fraction(98, 100) if target else Fraction(105, 100)),
                 'initial': ratios['initial'] <= Fraction(105, 100), 'engine': ratios['engine'] <= Fraction(105, 100),
                 'storage': all(item['passed'] for item in storage)}
        if target:
            gates['refresh'] = ratios['refresh'] <= Fraction(98, 100)
        assert gates == row['gates'] and all(gates.values()) == row['passed']
        decisions.append({'cohort': row['cohort'], 'case': row['case'], 'gates': gates})
    assert len(decisions) == 12 and not all(all(row['gates'].values()) for row in decisions)

    lifecycle_tag = 'changed-source-lifecycle-815-b1'
    lifecycle = read_at(lifecycle_tag + '/lifecycle-result.json')
    assert lifecycle['status'] == 'passed' and lifecycle['distinct_processes'] == 4
    assert lifecycle['native_consumer_calls'] == 36 and lifecycle['cleanup_status'] == 'complete'
    assert len(lifecycle['retired_payloads']) == 19
    assert len({phase['pid'] for phase in lifecycle['phases']}) == 4
    for phase in lifecycle['phases']:
        raw = raw_at(lifecycle_tag + '/' + phase['phase'] + '.json')
        assert sha(raw) == phase['result_sha256']
        value = json.loads(raw)
        assert value['pid'] == phase['pid'] and value['native_reserved_after_drop'] == 0
        assert value['publication']['ordinary_writer_recipe'] == oracle['RECIPE']
        if phase['phase'] in ('consume-rewrite', 'verify'):
            operations = value['consumer']['operations']
            assert len(operations) == 18
            projections = [operation for operation in operations if operation['operation'] == 'projection']
            ranges = oracle['partition_ranges'](oracle['CASES'][1], True)
            for operation, (_, ordinals) in zip(projections, ranges, strict=True):
                expected = json.dumps([oracle['row'](ordinal) for ordinal in ordinals], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
                assert operation['values_sha256'] == sha(expected) and operation['rows'] == 8192
    failures = []
    for name, raw in members.items():
        if Path(name).parent == Path(ROOT) and name.endswith('.json'):
            value = json.loads(raw)
            if isinstance(value, dict) and 'command' in value and value.get('status') == 'failed':
                failures.append(Path(name).name)
    assert failures
    result = {'status': 'passed', 'packet_sha256': EXPECTED_PACKET, 'members': len(members),
              'source_reconstruction': reconstruction, 'counts': counts, 'gate_decisions': decisions,
              'outcome': packet['outcome'], 'lifecycle_processes': 4, 'lifecycle_queries': 36,
              'failed_attempts_preserved': sorted(failures), 'physical_payloads_reopened': False,
              'limitations': packet['limitations']}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'status': 'passed', 'members': len(members), 'workflows': counts['workflows'],
                      'outcome': result['outcome'], 'source_files': 1910, 'output_sha256': sha(args.output.read_bytes())}))


if __name__ == '__main__':
    main()
