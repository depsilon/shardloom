"""Replay saved dependency acceptance offline (Python 3.11+); no tests are rerun."""
import argparse
import difflib
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
parser.add_argument('--repo-root', type=Path)
args = parser.parse_args()

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

report = json.loads((args.packet / 'acceptance.json').read_bytes())
assert report['schema_version'] == 'shardloom.compatible_dependency_acceptance.v1'
assert report['status'] == 'local_acceptance_passed'
assert report['archive'] == 'verification.tar.xz'
raw = (args.packet / report['archive']).read_bytes()
assert sha(raw) == report['archive_sha256']
expected = report['members']
assert len({row['path'] for row in expected}) == len(expected)
assert sum(row['bytes'] for row in expected) < 64 << 20
data = {}
with tarfile.open(fileobj=io.BytesIO(raw), mode='r:xz') as archive:
    assert archive.getnames() == [row['path'] for row in expected]
    for row, member in zip(expected, archive.getmembers()):
        path = PurePosixPath(row['path'])
        assert member.isfile() and not path.is_absolute() and '..' not in path.parts
        assert member.size == row['bytes']
        content = archive.extractfile(member).read()
        assert sha(content) == row['sha256']
        data[row['path']] = content
assert Path(__file__).read_bytes() == data['current/tools/verify_dependency_packet.py']

def read(relative):
    return json.loads(data[relative])

required = ['format-r1', 'static-docs-r1', 'compressed-text-tests-r1',
    'default-clippy-r1', 'default-tests-r1', 'native-clippy-r1', 'native-tests-r1',
    'feature-matrix-r1', 'msrv-default-r1', 'msrv-native-r1', 'dependency-audit-r1',
    'release-build-r1', 'freeze-runtime-r1', 'installed-wheel-r1', 'python-core-r1']
assert [check['receipt'] for check in report['checks']] == [name + '.json' for name in required]
assert set(report['code_snapshot']) == {'Cargo.toml', 'Cargo.lock', 'shardloom-vortex/Cargo.toml'}
graph = read('current/combined-graph-r1.json')
assert graph['status'] == 'passed' and graph['build_sources'] == report['code_snapshot']
assert graph['proposal_heads'] == report['original_proposal_heads']
assert graph['base'] == report['source_base'] and not report['changed_runtime_source']
for check in report['checks']:
    result = read('current/' + check['receipt'])
    assert result['status'] == 'passed' and result['returncode'] == 0 and result['process_group_drained']
    assert result['command'] == check['command']
    for name, identity in report['code_snapshot'].items():
        assert result['changed_source_inventory'][name] == identity
    log = data['current/' + check['receipt'].removesuffix('.json') + '.log']
    assert sha(log) == result['log_sha256']
    if 'rust_tests' in check:
        matches = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
        counts = dict(zip(('passed', 'failed', 'ignored'),
            (sum(int(row[i]) for row in matches) for i in range(3))))
        assert counts == check['rust_tests'] and counts['failed'] == 0
for name, identity in report['code_snapshot'].items():
    raw = data['current/source-blobs/' + identity['sha256']]
    assert len(raw) == identity['bytes'] and sha(raw) == identity['sha256']
    if args.repo_root:
        assert (args.repo_root / name).read_bytes() == raw

sequence = read('current/validation-sequence-r1.json')
assert sequence['status'] == 'passed' and sequence['completed'] == required[:-1]
assert read('current/core-followup-sequence-r1.json')['status'] == 'passed'
core = read('current/python-core-r1/core.json')
assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
core_match = re.search(rb'Ran (\d+) tests? in [0-9.]+s', data['current/python-core-r1.log'])
assert core_match
for name, expected_value in [('status', core['status']),
        ('discovered_test_cases', core['test_count']), ('reported_test_runs', int(core_match.group(1))),
        ('skip_records', core['skipped_count']), ('failures', 0), ('errors', 0)]:
    assert report['python_core'][name] == expected_value

inventory = read('original-proposals/primary-verification-r1.json')
assert inventory['status'] == 'passed'

def source(name, version):
    key = f'{name}-{version}'
    raw = data['current/prior-archive-replay-r1/' + key + '.crate']
    assert sha(raw) == inventory['archives'][key]['archive_sha256']
    assert len(raw) == inventory['archives'][key]['archive_bytes']
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        members = [m for m in archive.getmembers() if m.isfile()]
        assert all(m.name.startswith(key + '/') for m in members)
        assert len({m.name for m in members}) == len(members)
        return {m.name.removeprefix(key + '/'): archive.extractfile(m).read() for m in members}

for name, old, new in [('flate2', '1.1.9', '1.1.10'), ('tonic', '0.14.8', '0.14.9'),
                       ('tonic-prost', '0.14.6', '0.14.8')]:
    before, after = source(name, old), source(name, new)
    changes = []
    for path in sorted(set(before) | set(after)):
        a, b = before.get(path), after.get(path)
        if a == b:
            continue
        row = dict(path=path, before_sha256=sha(a) if a is not None else None,
                   after_sha256=sha(b) if b is not None else None)
        if path.startswith('src/') and path.endswith('.rs'):
            row['diff'] = ''.join(difflib.unified_diff((a or b'').decode().splitlines(True),
                (b or b'').decode().splitlines(True), fromfile=old + '/' + path, tofile=new + '/' + path))
        changes.append(row)
    actual = dict(crate=name, before=old, after=new,
        before_archive_sha256=inventory['archives'][f'{name}-{old}']['archive_sha256'],
        after_archive_sha256=inventory['archives'][f'{name}-{new}']['archive_sha256'],
        complete_archive_members_compared=True, changed_files=changes)
    assert actual == read('current/archived-source-comparisons/' + name + '.json')

notices = read('reviewed-notices/manifest.json')
lock = tomllib.loads(data['current/source-blobs/' + report['code_snapshot']['Cargo.lock']['sha256']].decode())
locked = {(p['name'], p['version'], p['source']): p['checksum'] for p in lock['package']
          if p.get('source', '').startswith('registry+')}
components = notices['components']
assert {(p['name'], p['version'], p['source']): p['crate_checksum_sha256'] for p in components} == locked
texts, records = set(), 0
for component in components + notices['vendored_components']:
    assert component['license'] and component['files']
    for row in component['files']:
        raw = data['reviewed-notices/' + row['text']]
        assert sha(raw) == row['sha256'] and len(raw) == row['bytes']
        assert row['text'] == 'texts/' + row['sha256'] + '.txt'
        assert row.get('crate_path') or row.get('upstream_url')
        texts.add(row['text'])
        records += 1
assert dict(status='passed', registry_packages=len(components),
    vendored_packages=len(notices['vendored_components']), license_file_records=records,
    unique_text_files=len(texts)) == report['dependency_notices']
assert {p.removeprefix('reviewed-notices/') for p in data if p.startswith('reviewed-notices/texts/')} == texts
installed = read('current/installed-proof-r1/report.json')
assert installed['status'] == installed['uninstall_status'] == 'passed'
assert installed['integration'] == report['installed_package']
assert installed['integration']['status'] == 'passed' and installed['integration']['tests'] == 319
assert installed['integration']['failures'] == installed['integration']['errors'] == installed['integration']['skipped'] == 0
assert installed['integrity']['licenses'] == report['dependency_notices']
assert not installed['publication_attempted'] and not installed['performance_claim']
for key in ('cli_sha256', 'native_library_sha256'):
    assert installed['integrity'][key] == report[key]
assert installed['integrity']['wheel_sha256'] == report['installed_wheel_sha256']
assert not report['publication_attempted'] and not report['performance_claim']
print(json.dumps(dict(status='passed', archive_members=len(data), complete_source_comparisons=3,
    dependency_notices=report['dependency_notices'], installed_package=report['installed_package'],
    scope='Offline archive, source-comparison, complete-notice and saved-check replay; no runtime tests or benchmarks rerun.')))
