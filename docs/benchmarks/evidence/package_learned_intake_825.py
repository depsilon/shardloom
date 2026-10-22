"""Compact and independently replay complete intake results without an engine binary."""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import subprocess
import tempfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
PREFIX = 'learned-intake-825-r3'
GUARDS = (
    'learned-intake-825-r1-freeze', 'learned-intake-825-r2-freeze',
    'learned-intake-825-r2-clippy', 'learned-intake-825-r3-freeze',
    'learned-intake-825-r3-clippy', 'learned-intake-825-r3-build',
    'learned-intake-825-r3-compile-proof', 'learned-intake-825-r3-compile-proof-corrected',
    'learned-intake-825-r3-unit', 'learned-intake-825-r3-compile-proof-complete',
    'learned-intake-825-r3-observe', 'learned-intake-825-r3-verify',
    'learned-intake-825-r3-verify-corrected', 'learned-intake-825-r3-verify-corrected-final',
)
FAILED = {
    'learned-intake-825-r1-freeze', 'learned-intake-825-r2-clippy',
    'learned-intake-825-r3-compile-proof', 'learned-intake-825-r3-compile-proof-corrected',
    'learned-intake-825-r3-verify', 'learned-intake-825-r3-verify-corrected',
}
HELPERS = (
    'run_guarded.py', 'native_workload_guard_816.py', 'run_learned_intake_825.py',
    'learned-intake-driver-825-r1.py', 'learned_intake_oracle_825.py',
    'learned_intake_oracle_825_corrected.py', 'verify_learned_compile_825.py',
    'verify_learned_compile_825_before_fixture_bridge.py',
    'verify_learned_compile_825_before_coverage_fix.py', 'verify_learned_intake_corrected_825.py',
    'learned-intake-protocol-825-before-identity-fix.md', 'learned-intake-observer-825-r2.rs',
    'weighted-search-provider-verified-824.json', 'weighted-search-provider-inventory-824.md',
    'learned-search-research-825.md', 'package_learned_intake_825.py',
)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def packed(raw, source):
    return dict(bytes=len(raw), sha256=digest(raw), text=raw.decode(), source_path=str(source))


def read(packet, name):
    member = packet['members'][name]
    raw = member['text'].encode()
    assert len(raw) == member['bytes'] and digest(raw) == member['sha256'], name
    return raw


def verify(path, scratch, repository=None):
    packet = json.loads(lzma.decompress(path.read_bytes()))
    assert packet['schema'] == 'shardloom.learned_intake_packet.v1'
    for name in packet['members']:
        assert '..' not in Path(name).parts and not Path(name).is_absolute()
        read(packet, name)
    for name in GUARDS:
        record = json.loads(read(packet, 'evidence/' + name + '.json'))
        assert record['status'] == ('failed' if name in FAILED else 'passed')
        assert record['returncode'] == (1 if name in FAILED else 0)
        assert record['process_group_drained'] and record['direct_child_reaped']
        assert record['log_sha256'] == digest(read(packet, 'evidence/' + name + '.log'))
    old = read(packet, 'evidence/learned_intake_oracle_825.py')
    corrected = read(packet, 'evidence/learned_intake_oracle_825_corrected.py')
    assert corrected == old.replace(b"'I64'", b"'i64'").replace(b"'F64'", b"'f64'")
    with tempfile.TemporaryDirectory(prefix='learned-intake-replay-', dir=scratch) as temporary:
        root = Path(temporary)
        oracle_file = root / 'oracle.py'
        oracle_file.write_bytes(corrected)
        spec = importlib.util.spec_from_file_location('learned_replay_oracle', oracle_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for name in packet['members']:
            if name.startswith('results/'):
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(read(packet, name))
        replay = module.validate(root / 'results')
    reported = json.loads(read(packet, 'evidence/' + PREFIX + '-verified-corrected.json'))
    assert all(reported[key] == value for key, value in replay.items())
    source = json.loads(read(packet, 'evidence/' + PREFIX + '-source.json'))
    assert source['binding']['base_commit'] == packet['base_commit']
    compiled = json.loads(read(packet, 'evidence/' + PREFIX + '-compilation-proof.json'))
    assert compiled['status'] == 'passed'
    assert compiled['source']['sha256'] == digest(read(packet, 'evidence/' + PREFIX + '-source.json'))
    assert compiled['executable'] == reported['binary']['executable']
    source_checks = None
    if repository is not None:
        source_checks = 0
        for name, expected in compiled['compilation_inputs'].items():
            member = 'worktree/' + name
            raw = (read(packet, member) if member in packet['members'] else
                   subprocess.check_output(['git', 'show', packet['base_commit'] + ':' + name], cwd=repository))
            assert len(raw) == expected['bytes'] and digest(raw) == expected['sha256'], name
            source_checks += 1
    return dict(status='passed', members=len(packet['members']),
        successful_guards=len(GUARDS)-len(FAILED), preserved_failed_guards=len(FAILED),
        calls=replay['calls'], verified_source_rows=replay['verified_source_rows'],
        verified_output_rows=replay['verified_output_rows'], compiled_source_checks=source_checks,
        compilation_inputs=len(compiled['compilation_inputs']),
        individually_hashed_prebuild_inputs=compiled['individually_hashed_prebuild_inputs'],
        git_bound_fixtures=len(compiled['git_bound_fixtures']),
        search_work_prerequisite=True, timing_claim=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--scratch-root', type=Path, default=E)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    if args.verify:
        print(json.dumps(verify(args.verify, args.scratch_root, args.repository)), flush=True)
        return
    import run_learned_intake_825 as driver
    driver.verify_source(E / (PREFIX + '-source.json'))
    members = {}
    for name in HELPERS:
        members['evidence/' + name] = packed((E / name).read_bytes(), E / name)
    for name in GUARDS:
        for suffix in ('.json', '.log'):
            path = E / (name + suffix)
            members['evidence/' + path.name] = packed(path.read_bytes(), path)
    for name in ('learned-intake-825-r2-source.json', PREFIX + '-source.json',
                 PREFIX + '-binary.json', PREFIX + '-compilation-proof.json',
                 PREFIX + '-verified-corrected.json'):
        path = E / name
        members['evidence/' + name] = packed(path.read_bytes(), path)
    root = E / (PREFIX + '-results')
    for path in sorted(root.rglob('*')):
        if path.is_file():
            members['results/' + str(path.relative_to(root))] = packed(path.read_bytes(), path)
    names = set(subprocess.check_output(['git', 'diff', '--name-only', 'HEAD'], cwd=W, text=True).splitlines())
    names.update(subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=W, text=True).splitlines())
    assert len(names) == 7, names
    for name in sorted(names):
        members['worktree/' + name] = packed((W / name).read_bytes(), W / name)
    for label, path in (
        ('supervisor.py', Path('/Users/dylan/LocalData/shardloom/perf-all-20260906/bounded_build_process_resume_20260912.py')),
        ('local_uat_storage.py', W / 'scripts/local_uat_storage.py'),
    ):
        members['infrastructure/' + label] = packed(path.read_bytes(), path)
    patch = subprocess.check_output(['git', 'diff', '--binary', 'HEAD'], cwd=W)
    members['worktree.patch'] = packed(patch, 'git diff --binary HEAD')
    packet = dict(schema='shardloom.learned_intake_packet.v1', base_commit=driver.BASE,
        prerequisite='large resident CDF with repeated exact public-path search work',
        decision='intake passed; predictor and timed comparison require a separate frozen protocol',
        failures={
            'source_identity':'one incorrect commit character, corrected before compilation',
            'lint':'observer finish needed an explicit drop of its session owner',
            'compile_inventory':'three fixtures use prebuild Git binding; complete target dependency closure is 1546, not full-repository source count',
            'schema_reference':'pinned PType serializes lowercase; only reference spellings changed; no query rerun',
            'verifier_source_assertion':'source uses cfg_attr around serde rename; corrected before successful replay',
        }, members=members)
    path = E / 'learned-intake-packet-825.json.xz'
    payload = json.dumps(packet, sort_keys=True, separators=(',', ':')).encode()
    with path.open('xb') as stream:
        stream.write(lzma.compress(payload, preset=6))
    assert lzma.decompress(path.read_bytes()) == payload
    result = verify(path, E, W)
    driver.verify_source(E / (PREFIX + '-source.json'))
    result.update(packet=driver.identity(path), packager=driver.identity(__file__),
        members={name:{key:value for key,value in member.items() if key!='text'} for name,member in members.items()})
    driver.save(E / 'learned-intake-packet-825.manifest.json', result)
    print(json.dumps({key:value for key,value in result.items() if key!='members'}), flush=True)


if __name__ == '__main__':
    main()
