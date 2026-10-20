"""Preserve and replay the finite learned-search comparison without executables."""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import subprocess
import sys
import tempfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
BASE = '8721506c487b873cc0598ead0b78b4e22339ff99'
HELPERS = ('run_learned_cost_826.py', 'learned_cost_oracle_826.py',
    'learned_intake_oracle_825_corrected.py', 'run_guarded.py', 'native_workload_guard_816.py',
    'qualify_learned_binary_826.py', 'package_learned_cost_826.py')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(path):
    raw = path.read_bytes()
    return dict(path=str(path), bytes=len(raw), sha256=digest(raw))


def packed(path):
    raw = path.read_bytes()
    return dict(source_path=str(path), bytes=len(raw), sha256=digest(raw), text=raw.decode())


def read(packet, name):
    member = packet['members'][name]
    raw = member['text'].encode()
    assert len(raw) == member['bytes'] and digest(raw) == member['sha256'], name
    return raw


def matches(raw, expected):
    assert len(raw) == expected['bytes'] and digest(raw) == expected['sha256']


def verify(path, scratch, repository):
    packet = json.loads(lzma.decompress(path.read_bytes()))
    assert packet['schema'] == 'shardloom.learned_cost_packet.v1'
    assert packet['base_commit'] == BASE
    for name in packet['members']:
        assert '..' not in Path(name).parts and not Path(name).is_absolute()
        read(packet, name)
    guards, failed = [], []
    for name in packet['guards']:
        record = json.loads(read(packet, 'evidence/' + name + '.json'))
        assert record['head'] == BASE
        assert record['status'] in ('passed', 'failed')
        assert record['log_sha256'] == digest(read(packet, 'evidence/' + name + '.log'))
        if record.get('pid') is not None:
            assert record['process_group_drained'] and record['direct_child_reaped']
        if record['status'] == 'passed':
            assert record['returncode'] == 0
            guards.append(name)
        else:
            failed.append(name)
    reported = json.loads(read(packet, 'evidence/learned-cost-826-score.json'))
    result_names = {name.removeprefix('results/') for name in packet['members']
                    if name.startswith('results/')}
    assert result_names == set(reported['files'])
    for name, expected in reported['files'].items():
        matches(read(packet, 'results/' + name), expected)
    sources = {}
    source_checks = {}
    assert reported['binaries']['baseline']['executable']['sha256'] != reported['binaries']['candidate']['executable']['sha256']
    for role, binary in reported['binaries'].items():
        assert role in ('baseline', 'candidate')
        source_name = 'evidence/' + Path(binary['source']['path']).name
        raw = read(packet, source_name)
        matches(raw, binary['source'])
        source = json.loads(raw)
        assert source['role'] == role and source['binding']['base_commit'] == BASE
        sources[role] = source
        proof = binary['compilation']
        assert proof['compiler_artifact']['fresh'] is False
        assert proof['compiler_artifact']['profile']['opt_level'] == '3'
        assert 'release-user-surfaces' in proof['compiler_artifact']['features']
        assert proof['individually_hashed_prebuild_inputs'] == len(proof['compilation_inputs'])
        assert len(proof['packages']) == 11
        for name, expected in proof['compilation_inputs'].items():
            assert source['binding']['files'][name] == expected
            if repository is not None:
                raw = (source['changed_source_text'][name].encode()
                    if name in source['changed_source_text'] else
                    subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=repository))
                matches(raw, expected)
        source_checks[role] = len(proof['compilation_inputs']) if repository else None
        # Qualification changed only the copied executable's basename and kept
        # its exact compiled bytes/inode. The ordinary guard recognizes it.
        assert Path(binary['executable']['path']).name.startswith('shardloom-')
        assert binary['executable']['bytes'] == binary['compiled']['bytes']
        assert binary['executable']['sha256'] == binary['compiled']['sha256']
        stem = f"learned-cost-826-{role}-r{source['revision']}"
        qualification = json.loads(read(packet, 'evidence/' + stem + '-qualification.json'))
        assert qualification['compiled_bytes_unchanged'] and qualification['guard_recognizes_name']
        assert qualification['original_inode'] == qualification['qualified_inode']
        assert qualification['executable'] == binary['executable']
        matches(read(packet, 'evidence/' + stem + '-unqualified-binary.json'),
                qualification['original_manifest'])
        matches(read(packet, 'evidence/' + stem + '-binary.json'), qualification['qualified_manifest'])
        for kind in ('clippy', 'build', 'admit', 'qualify', 'unit', 'preflight'):
            assert stem + '-' + kind in guards
    common = sources['baseline']['binding']['common']
    assert sources['candidate']['binding']['common'] == common == reported['common']
    for key in ('driver', 'oracle', 'original_reference', 'guard', 'concurrency_guard'):
        matches(read(packet, 'evidence/' + Path(common[key]['path']).name), common[key])
    for key in ('protocol', 'harness', 'observer'):
        matches(read(packet, 'shared/' + key), common[key])
    with tempfile.TemporaryDirectory(prefix='learned-cost-replay-', dir=scratch) as temporary:
        root = Path(temporary)
        for name in ('learned_intake_oracle_825_corrected.py', 'learned_cost_oracle_826.py'):
            (root / name).write_bytes(read(packet, 'evidence/' + name))
        for name in packet['members']:
            if name.startswith('results/'):
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(read(packet, name))
        # Load both archived modules explicitly, so an already-imported local
        # module cannot silently supply different reference logic.
        loaded = {}
        previous = {}
        try:
            for name in ('learned_intake_oracle_825_corrected', 'learned_cost_oracle_826'):
                previous[name] = sys.modules.get(name)
                spec = importlib.util.spec_from_file_location(name, root / (name + '.py'))
                module = importlib.util.module_from_spec(spec)
                sys.modules[name] = module
                spec.loader.exec_module(module)
                loaded[name] = module
            oracle = loaded['learned_cost_oracle_826']
            assert common['expectations'] == oracle.reference.expectations()
            validator = oracle.Validator(root / 'results')
            records = []
            for role in ('baseline', 'candidate'):
                for cell, _, _ in oracle.CELLS:
                    filename = f'preflight-preflight-{cell}-{role}-0.json'
                    record = json.loads(read(packet, 'results/' + filename))
                    records.append(validator.record(record, role=role, mode='preflight',
                        cohort='preflight', sample=0, cell=cell))
            for cell, _, _ in oracle.CELLS:
                pair = {row['role']:row for row in records if row['cell'] == cell}
                assert pair['candidate']['peak_reserved_bytes'] <= pair['baseline']['peak_reserved_bytes'] + 128
            for cohort in ('A', 'B'):
                verified = json.loads(read(packet, f'evidence/learned-cost-826-cohort-{cohort}-verified.json'))
                assert verified['status'] == 'passed'
                order, expected_records = [], []
                cells = oracle.CELLS if cohort == 'A' else tuple(reversed(oracle.CELLS))
                for cell, _, _ in cells:
                    for sample in range(8):
                        roles = ('baseline', 'candidate') if sample % 2 == 0 else ('candidate', 'baseline')
                        if cohort == 'B':
                            roles = tuple(reversed(roles))
                        for role in roles:
                            order.append(dict(cell=cell, sample=sample, role=role))
                            filename = f'measure-{cohort}-{cell}-{role}-{sample}.json'
                            record = json.loads(read(packet, 'results/' + filename))
                            checked = validator.record(record, role=role, mode='measure',
                                cohort=cohort, sample=sample, cell=cell)
                            records.append(checked)
                            expected_records.append(checked)
                assert verified['order'] == order and verified['verified'] == expected_records
                assert 'learned-cost-826-cohort-' + cohort in guards
            replay = oracle.score(root / 'results')
            assert all(reported[key] == value for key, value in replay.items())
            assert len(records) == 272
            assert len(list((root / 'results').glob('*.json'))) == len(records)
            assert len(list((root / 'results/witnesses').glob('*.json'))) == len(validator.used) == 16
        finally:
            for name, old in previous.items():
                if old is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = old
    return dict(status='passed', members=len(packet['members']), successful_guards=len(guards),
        preserved_failed_guards=len(failed), failed_guards=failed, workflows=len(records),
        calls=len(records)*2, verified_source_rows=sum(row['source_rows'] for row in records),
        verified_output_rows=sum(row['output_rows'] for row in records),
        compiled_source_checks=source_checks, retention_gate_passed=replay['retention_gate_passed'],
        complete_results_and_original_order_replayed=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--scratch-root', type=Path, default=E)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    if args.verify:
        print(json.dumps(verify(args.verify, args.scratch_root, args.repository)), flush=True)
        return
    reported = json.loads((E / 'learned-cost-826-score.json').read_text())
    assert reported['status'] == 'passed'
    members, guards = {}, []
    for path in sorted(E.glob('learned-cost-826-*.json')):
        record = json.loads(path.read_text())
        members['evidence/' + path.name] = packed(path)
        if 'command' in record:
            guards.append(path.stem)
            log = path.with_suffix('.log')
            members['evidence/' + log.name] = packed(log)
    for name in HELPERS:
        members['evidence/' + name] = packed(E / name)
    for key in ('protocol', 'harness', 'observer'):
        path = Path(reported['common'][key]['path'])
        matches(path.read_bytes(), reported['common'][key])
        members['shared/' + key] = packed(path)
    for path in sorted((E / 'learned-cost-826-results').rglob('*')):
        if path.is_file():
            members['results/' + str(path.relative_to(E / 'learned-cost-826-results'))] = packed(path)
    for name, path in (
        ('supervisor.py', Path('/Users/dylan/LocalData/shardloom/perf-all-20260906/bounded_build_process_resume_20260912.py')),
        ('local_uat_storage.py', W / 'scripts/local_uat_storage.py'),
    ):
        members['infrastructure/' + name] = packed(path)
    packet = dict(schema='shardloom.learned_cost_packet.v1', base_commit=BASE,
        guards=guards, members=members, decision='frozen two-cohort finite gate; broader owner remains open')
    path = E / 'learned-cost-packet-826.json.xz'
    raw = json.dumps(packet, sort_keys=True, separators=(',', ':')).encode()
    with path.open('xb') as stream:
        stream.write(lzma.compress(raw, preset=6))
    assert lzma.decompress(path.read_bytes()) == raw
    result = verify(path, E, W)
    result.update(packet=identity(path), packager=identity(Path(__file__)),
        members={name:{key:value for key,value in item.items() if key!='text'} for name,item in members.items()})
    with (E / 'learned-cost-packet-826.manifest.json').open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({key:value for key,value in result.items() if key!='members'}), flush=True)


if __name__ == '__main__':
    main()
