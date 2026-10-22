"""Preserve and replay the rejected region projection experiment without payloads.

Replay validates complete recorded values, geometry, read ranges, prerequisites,
source/build provenance and the frozen decision. Retired payload identities are
captured evidence; replay never represents them as newly read physical files.
"""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import re
import subprocess
import sys
import tempfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
BASE = '603e8ec163a0cbcb24bbf6712a9a4e311e46b187'
HELPERS = ('run_region_intake_828.py', 'run_region_projection_830.py',
           'verify_region_intake_828.py', 'verify_region_projection_830.py',
           'run_guarded.py', 'native_workload_guard_816.py',
           'package_region_projection_831.py')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(path):
    raw = path.read_bytes()
    return dict(path=str(path), bytes=len(raw), sha256=digest(raw))


def member(path):
    raw = path.read_bytes()
    return dict(source_path=str(path), bytes=len(raw), sha256=digest(raw), text=raw.decode())


def matches(raw, expected):
    assert len(raw) == expected['bytes'] and digest(raw) == expected['sha256']


def read(packet, name):
    row = packet['members'][name]
    raw = row['text'].encode()
    matches(raw, row)
    return raw


def parsed(packet, name):
    return json.loads(read(packet, name))


def archived_member_identity(packet, name):
    row = packet['members'][name]
    read(packet, name)
    return dict(path=row['source_path'], bytes=row['bytes'], sha256=row['sha256'])


def source_records(packet):
    records = {}
    for revision in (1, 2, 3):
        key = f'region-intake-828-r{revision}'
        source = parsed(packet, key + '-source.json')
        content = parsed(packet, key + '-source-content.json')
        records[key] = dict(source=source, patch=content['patch'], added=content['untracked'])
    for role in ('baseline', 'candidate'):
        key = f'region-projection-830-{role}-r1'
        source = parsed(packet, key + '-source.json')
        records[key] = dict(source=source, patch=source['patch'], added=source['added_sources'])
    return records


def verify_sources(packet, repository, scratch):
    records = source_records(packet)
    for row in records.values():
        assert row['source']['binding']['base_commit'] == BASE
        for name, text in row['added'].items():
            assert not Path(name).is_absolute() and '..' not in Path(name).parts
            if name in row['source']['binding']['files']:
                matches(text.encode(), row['source']['binding']['files'][name])
    base = records['region-projection-830-baseline-r1']['source']['binding']['files']
    candidate = records['region-projection-830-candidate-r1']['source']['binding']['files']
    assert base.keys() == candidate.keys()
    assert [name for name in base if base[name] != candidate[name]] == [
        'vendor/vortex-layout/src/layouts/flat/reader.rs']
    if repository is None:
        return dict(git_source_replay=False, source_versions=len(records))
    added = {name for row in records.values() for name in row['added']}
    names = {name for row in records.values() for name in row['source']['binding']['files']}
    tracked = sorted(names - added)
    original = {}
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repository,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name in tracked:
            assert not Path(name).is_absolute() and '..' not in Path(name).parts and '\n' not in name
            process.stdin.write((BASE + ':' + name + '\n').encode())
            process.stdin.flush()
            header = process.stdout.readline().decode().strip().split()
            assert len(header) == 3 and header[1] == 'blob', (name, header)
            raw = process.stdout.read(int(header[2]))
            assert process.stdout.read(1) == b'\n'
            original[name] = raw
    finally:
        process.stdin.close()
        process.stdout.close()
        assert process.wait(timeout=10) == 0
    counts = {}
    with tempfile.TemporaryDirectory(prefix='region-source-replay-', dir=scratch) as temporary:
        for key, row in records.items():
            root = Path(temporary) / key
            root.mkdir()
            modified = re.findall(r'^diff --git a/(\S+) b/\1$', row['patch'], re.M)
            assert set(modified) <= {'shardloom-vortex/src/local_primitives.rs',
                                     'vendor/vortex-layout/src/layouts/flat/reader.rs'}
            assert modified
            for name in modified:
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(original[name])
            subprocess.run(['git', 'apply', '--check', '-'], cwd=root,
                           input=(row['patch'].rstrip() + '\n').encode(), check=True)
            subprocess.run(['git', 'apply', '-'], cwd=root,
                           input=(row['patch'].rstrip() + '\n').encode(), check=True)
            for name, expected in row['source']['binding']['files'].items():
                if name in row['added']:
                    raw = row['added'][name].encode()
                elif name in modified:
                    raw = (root / name).read_bytes()
                else:
                    raw = original[name]
                matches(raw, expected)
            counts[key] = len(row['source']['binding']['files'])
    return dict(git_source_replay=True, source_versions=len(records), checked_files=counts)


def verify_builds(packet):
    result = {}
    prefixes = ['region-intake-828-r2', 'region-intake-828-r3']
    prefixes += [f'region-projection-830-{role}-r1' for role in ('baseline', 'candidate')]
    for prefix in prefixes:
        binary = parsed(packet, prefix + '-binary.json')
        source_raw = read(packet, prefix + '-source.json')
        matches(source_raw, binary['source'])
        source = json.loads(source_raw)
        artifact = binary['artifact']
        assert artifact['fresh'] is False and artifact['profile']['test']
        assert artifact['profile']['opt_level'] == '3'
        assert 'release-user-surfaces' in artifact['features']
        assert artifact['executable'] == binary['executable']['path']
        log = read(packet, prefix + '-build.log').decode()
        events = [json.loads(line) for line in log.splitlines() if line.startswith('{')]
        assert artifact in events and {'reason': 'build-finished', 'success': True} in events
        assert len(binary['packages']) == 11
        inputs = set()
        for package in binary['packages']:
            assert package['inputs']
            inputs.update(package['inputs'])
        assert inputs == set(binary['compilation_inputs'])
        for name, expected in binary['compilation_inputs'].items():
            assert source['binding']['files'][name] == expected
        if 'projection-830' in prefix:
            frozen = parsed(packet, prefix + '-frozen-binary.json')
            assert frozen['artifact'] == artifact
            assert frozen['source'] == binary['source']
            assert frozen['compilation_inputs'] == binary['compilation_inputs']
            for field in ('bytes', 'sha256'):
                assert frozen['executable'][field] == binary['executable'][field]
            assert len(frozen['selectors']) == 9
            assert sum(len(row['tests']) for row in frozen['selectors']) == 25
            assert frozen['provider_sha256'] == source['binding']['files'][
                'vendor/vortex-layout/src/layouts/flat/reader.rs']['sha256']
        result[prefix] = len(binary['compilation_inputs'])
    baseline = parsed(packet, 'region-projection-830-baseline-r1-frozen-binary.json')
    candidate = parsed(packet, 'region-projection-830-candidate-r1-frozen-binary.json')
    assert baseline['executable']['sha256'] != candidate['executable']['sha256']
    assert baseline['selectors'] == candidate['selectors']
    return result


def replay_values(packet, scratch):
    original_e = Path(packet['original_evidence_root'])
    original_w = Path(packet['original_workspace'])
    plan = parsed(packet, 'region-projection-830-plan-r1.json')
    verified = parsed(packet, 'region-intake-828-r3-verified.json')
    payloads = {item['path']: item for name, item in plan['inputs'].items() if name.endswith('.vortex')}
    assert len(payloads) == 8
    for item in payloads.values():
        assert item == verified['files'][Path(item['path']).name]
    with tempfile.TemporaryDirectory(prefix='region-result-replay-', dir=scratch) as temporary:
        root = Path(temporary)
        mapping, reverse = {}, {}
        for name, item in packet['members'].items():
            destination = root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(read(packet, name))
            assert item['source_path'] not in mapping
            mapping[item['source_path']] = destination
            reverse[str(destination)] = item['source_path']

        def mapped(path):
            value = str(path)
            if value in reverse:
                return Path(value)
            assert value in mapping, ('unarchived file requested', value)
            return mapping[value]

        captured_uses = set()

        def archived_identity(path):
            value = reverse.get(str(path), str(path))
            if Path(value).is_relative_to(root):
                value = str(original_e / Path(value).relative_to(root))
            if value in payloads:
                captured_uses.add(value)
                return payloads[value]
            result = identity(mapped(value))
            result['path'] = value
            return result

        previous, loaded = {}, {}
        modules = ('run_region_intake_828', 'run_region_projection_830',
                   'verify_region_intake_828', 'verify_region_projection_830')
        try:
            for name in modules:
                previous[name] = sys.modules.get(name)
                spec = importlib.util.spec_from_file_location(name, root / (name + '.py'))
                module = importlib.util.module_from_spec(spec)
                sys.modules[name] = module
                if name == 'verify_region_intake_828':
                    source = (root / (name + '.py')).read_text()
                    old = "assert witness['source_bytes'] == source.stat().st_size"
                    assert source.count(old) == 1
                    # Only this physical-file size check is redirected to the
                    # frozen captured identity. All value/range/oracle logic is
                    # the exact original verifier. No payload is fabricated.
                    source = source.replace(old, "assert witness['source_bytes'] == d.identity(source)['bytes']")
                    exec(compile(source, str(root / (name + '.py')), 'exec'), module.__dict__)
                else:
                    spec.loader.exec_module(module)
                loaded[name] = module
            d, r, i, v = (loaded[name] for name in modules)
            d.E, d.W, d.identity = original_e, original_w, archived_identity
            r.E, r.W = original_e, original_w
            r.FIXTURES = original_e / 'region-intake-828-r3-results'
            r.read = lambda path: json.loads(mapped(path).read_text())
            v.E, v.Path = original_e, mapped
            intake = i.validate(root / 'region-intake-828-r3-results')
            for key, value in intake.items():
                assert verified[key] == value, key
            assert intake['eligible_count_signal'] is True
            proofs = {role: v.prerequisites(role, 1) for role in ('baseline', 'candidate')}
            observations = {role: v.read_rows(role, 1) for role in ('baseline', 'candidate')}
            reported = parsed(packet, 'region-projection-830-read-gate-r1.json')
            assert reported['prerequisites'] == proofs
            assert reported['observations'] == json.loads(json.dumps(observations))
            gates = []
            for profile in ('sparse_zoned', 'sparse_plain'):
                for parallelism in (1, 4):
                    base = [row for row in observations['baseline'][0]
                            if row['profile'] == profile and row['parallelism'] == parallelism]
                    candidate = [row for row in observations['candidate'][0]
                                 if row['profile'] == profile and row['parallelism'] == parallelism]
                    assert len(base) == len(candidate) == 2
                    bmin = min(row['operation_bytes'] for row in base)
                    cmax = max(row['operation_bytes'] for row in candidate)
                    bpmin = min(row['rejected_projection']['read_bytes'] for row in base)
                    cpmax = max(row['rejected_projection']['read_bytes'] for row in candidate)
                    gates.append(dict(profile=profile, parallelism=parallelism,
                        baseline_min_total=bmin, candidate_max_total=cmax,
                        baseline_min_rejected=bpmin, candidate_max_rejected=cpmax,
                        passed=cmax * 10 <= bmin * 9 and bmin - cmax >= 65536 and cpmax * 4 <= bpmin * 3))
            assert reported['gates'] == gates
            assert reported['status'] == 'passed' and reported['complete_observed_calls'] == 64
            assert reported['eligible_for_timings'] is False
            assert reported['decision'] == 'rejected_actual_read_gate'
            assert sum(row['passed'] for row in gates) == 1
            assert captured_uses == set(payloads)
        finally:
            for name in modules:
                if previous.get(name) is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = previous[name]
    return dict(intake_calls=32, comparison_calls=64, ordered_calls=32,
                complete_source_rows=intake['verified_source_rows'],
                selected_rows=sum(row['verified_selected_rows'] for row in proofs.values()),
                regression_tests=sum(row['regression_tests'] for row in proofs.values()),
                gates=gates, eligible_for_timings=False, physical_payloads_revalidated=False,
                captured_payload_identities=len(payloads))


def verify(path, scratch, repository=None):
    packet = json.loads(lzma.decompress(path.read_bytes()))
    assert packet['schema'] == 'shardloom.region_projection_packet.v1'
    assert packet['base_commit'] == BASE
    for name in packet['members']:
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        read(packet, name)
    guards, failed = [], []
    for name in packet['guards']:
        record = parsed(packet, name + '.json')
        assert record['head'] == BASE
        assert record['status'] in ('passed', 'failed')
        assert record['log_sha256'] == digest(read(packet, name + '.log'))
        assert record['process_group_drained'] and record['direct_child_reaped']
        if record['status'] == 'passed':
            assert record['returncode'] == 0
            guards.append(name)
        else:
            assert record['returncode'] != 0
            failed.append(name)
    assert set(failed) == {'region-intake-828-r1-build', 'region-intake-828-r2-clippy',
                           'region-candidate-830-clippy-a1', 'region-candidate-830-clippy-a2'}
    plan = parsed(packet, 'region-projection-830-plan-r1.json')
    for key in ('driver', 'oracle', 'base_driver', 'base_oracle', 'selectors', 'intake_verified'):
        matches(read(packet, Path(plan[key]['path']).name), plan[key])
    matches(read(packet, 'protocol/candidate.md'), plan['protocol'])
    assert plan['source_baseline_commit'] == BASE
    assert plan['profiles'] == ['sparse_zoned', 'sparse_plain', 'all_true_zoned', 'all_false_zoned',
                                'mixed_zoned', 'nullable_zoned', 'misaligned_zoned', 'skew_zoned']
    assert plan['parallelism'] == [1, 4] and plan['repetitions'] == [0, 1]
    decision = parsed(packet, 'region-projection-830-read-gate-r1.json')
    assert decision['plan'] == archived_member_identity(packet, 'region-projection-830-plan-r1.json')
    assert decision['protocol'] == plan['protocol'] and decision['oracle'] == plan['oracle']
    for role, phases in decision['checks'].items():
        assert role in ('baseline', 'candidate')
        for phase, receipt in phases.items():
            prefix = f'region-projection-830-{role}-r1-{phase}'
            assert receipt == dict(receipt=archived_member_identity(packet, prefix + '.json'),
                                   log=archived_member_identity(packet, prefix + '.log'))
    verified_intake = parsed(packet, 'region-intake-828-r3-verified.json')
    assert verified_intake['observation_plan'] == archived_member_identity(packet,
        'region-intake-828-r3-observation-plan.json')
    for name, expected in verified_intake['files'].items():
        if name.endswith('.vortex'):
            assert expected == plan['inputs'][name]
        else:
            assert expected == archived_member_identity(packet, 'region-intake-828-r3-results/' + name)
    schedule = []
    cells = [(profile, p, 16) for profile in plan['profiles'] for p in (1, 4)]
    cells += [(profile, p, 1) for profile in ('all_true_zoned', 'mixed_zoned') for p in (1, 4)]
    for cohort in ('A', 'B'):
        for repeat in range(3):
            for cell, (profile, parallelism, calls) in enumerate(cells):
                roles = ['baseline', 'candidate']
                if (cell + repeat + (cohort == 'B')) % 2:
                    roles.reverse()
                schedule.extend(dict(cohort=cohort, repetition=repeat, cell=cell, profile=profile,
                                     parallelism=parallelism, calls=calls, role=role) for role in roles)
    assert plan['schedule'] == schedule and len(schedule) == 240
    assert sum(row['calls'] for row in schedule) == 3120
    assert not any('timings-r1/' in name for name in packet['members'])
    before = parsed(packet, 'region-projection-831-before-restoration.json')
    assert before['status'] == 'passed' and before['timing_run_exists'] is False
    assert before['read_gate'] == archived_member_identity(packet, 'region-projection-830-read-gate-r1.json')
    coverage = parsed(packet, 'region-guard-coverage-831.json')
    assert coverage['status'] == 'limitation_recorded'
    assert coverage['exclusive_workload_isolation_proven'] is False
    assert coverage['observed_overlap_established'] is False
    assert coverage['timing_comparison_executed'] is False
    assert coverage['guard']['sha256'] == digest(read(packet, 'native_workload_guard_816.py'))
    assert [row['role'] for row in coverage['roles']] == ['baseline', 'candidate']
    for row in coverage['roles']:
        assert row['recognized_by_existing_guard'] is False
        assert row['executable'] == parsed(packet,
            f"region-projection-830-{row['role']}-r1-frozen-binary.json")['executable']
        assert Path(row['executable']['path']).name.startswith('region-projection-')
    intake_plan = parsed(packet, 'region-intake-828-r3-observation-plan.json')
    for key in ('source', 'oracle'):
        matches(read(packet, Path(intake_plan[key]['path']).name), intake_plan[key])
    matches(read(packet, 'protocol/intake.md'), intake_plan['protocol'])
    for name in ['region-intake-828-r3-' + phase for phase in ('build', 'qualify', 'clippy', 'observe', 'verify')]:
        assert name in guards
    for role in ('baseline', 'candidate'):
        for phase in ('build', 'qualify', 'clippy', 'prerequisites', 'regressions', 'observed'):
            assert f'region-projection-830-{role}-r1-{phase}' in guards
    builds = verify_builds(packet)
    sources = verify_sources(packet, repository, scratch)
    values = replay_values(packet, scratch)
    return dict(status='passed', members=len(packet['members']), passed_guards=len(guards),
                failed_attempts=failed, compiled_source_files=builds,
                source_verification=sources, exclusive_workload_isolation_proven=False, **values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--scratch-root', type=Path, default=E)
    parser.add_argument('--repository', type=Path)
    parser.add_argument('--attempt', type=int, default=1)
    args = parser.parse_args()
    if args.verify:
        print(json.dumps(verify(args.verify, args.scratch_root, args.repository)), flush=True)
        return
    paths = {E / name for name in HELPERS}
    for pattern in ('region-intake-828-*', 'region-intake-fmt-*', 'region-candidate-830-*',
                    'region-projection-830-*', 'region-projection-831-before-restoration.json',
                    'region-guard-coverage-831.json',
                    'region-regression-selectors-830.json', 'region-selector-primary-verification-830.json',
                    'region-verifier-negative-checks-830.json'):
        for path in E.glob(pattern):
            paths.update([path] if path.is_file() else [p for p in path.rglob('*') if p.is_file()])
    payloads = sorted(path for path in paths if path.suffix == '.vortex')
    assert len(payloads) == 8
    plan = json.loads((E / 'region-projection-830-plan-r1.json').read_text())
    for path in payloads:
        assert identity(path) == plan['inputs'][path.name]
    for role in ('baseline', 'candidate'):
        binary = json.loads((E / f'region-projection-830-{role}-r1-frozen-binary.json').read_text())
        assert identity(Path(binary['executable']['path'])) == binary['executable']
    paths.difference_update(payloads)
    members = {str(path.relative_to(E)): member(path) for path in sorted(paths)}
    members['protocol/intake.md'] = member(W / 'docs/architecture/native-region-projection-intake-2026-10-20.md')
    members['protocol/candidate.md'] = member(W / 'docs/architecture/native-region-projection-design-2026-10-20.md')
    guards = []
    for name, row in members.items():
        if '/' not in name and name.endswith('.json'):
            record = json.loads(row['text'])
            if 'process_group_drained' in record and 'log_sha256' in record:
                guards.append(name.removesuffix('.json'))
    packet = dict(schema='shardloom.region_projection_packet.v1', base_commit=BASE,
        original_evidence_root=str(E), original_workspace=str(W), members=members,
        guards=sorted(guards), payload_scope='Captured identities only; complete witnesses and results retained.')
    raw = json.dumps(packet, sort_keys=True, separators=(',', ':')).encode()
    output = E / f'region-projection-packet-831-r{args.attempt}.json.xz'
    with output.open('xb') as stream:
        stream.write(lzma.compress(raw, preset=6))
    assert lzma.decompress(output.read_bytes()) == raw
    result = verify(output, args.scratch_root, W)
    result.update(packet=identity(output), packager=identity(Path(__file__)),
        members={name: {key: value for key, value in row.items() if key != 'text'} for name, row in members.items()})
    with output.with_suffix('.manifest.json').open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'members'}), flush=True)


if __name__ == '__main__':
    main()
