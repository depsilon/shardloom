"""Verify saved fixed-tensor correctness evidence offline; does not rerun queries."""
import argparse
import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import tarfile

REQUIRED = ['format-r2', 'default-clippy-r1', 'default-tests-r1', 'native-clippy-r3',
    'native-tests-r1', 'feature-matrix-r1', 'tensor-minimal-feature-r2', 'tensor-binding-r2', 'native-build-r2',
    'freeze-runtime-r2', 'context-native-r1', 'python-core-r2', 'static-docs-r1', 'installed-wheel-r1']


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def verify(packet, repo_root=None):
    report = json.loads((packet / 'acceptance.json').read_bytes())
    assert report['schema_version'] == 'shardloom.fixed_shape_tensor_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed'
    assert report['archive'] == 'verification.tar.xz'
    raw = (packet / report['archive']).read_bytes()
    assert digest(raw) == report['archive_sha256']
    members = report['members']
    assert len({row['path'] for row in members}) == len(members)
    assert sum(row['bytes'] for row in members) < 64 << 20
    data = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:xz') as archive:
        assert archive.getnames() == [row['path'] for row in members]
        for row, entry in zip(members, archive.getmembers()):
            path = PurePosixPath(row['path'])
            assert entry.isfile() and not path.is_absolute() and '..' not in path.parts
            assert entry.size == row['bytes']
            content = archive.extractfile(entry).read()
            assert digest(content) == row['sha256']
            data[row['path']] = content
    assert Path(__file__).read_bytes() == data['current/tools/verify_tensor_packet.py']

    def read(name):
        return json.loads(data[name])

    source = report['code_snapshot']
    assert source and all(Path(name).suffix in {'.rs', '.py', '.toml'} for name in source)
    final_build = read('current/native-build-r2.json')
    build_source = {name: identity for name, identity in final_build['changed_source_inventory'].items()
                    if Path(name).suffix in {'.rs', '.py', '.toml'}}
    harness = read('current/harness-correction-r1.json')
    assert harness['status'] == 'passed' and harness['source_base'] == report['source_base']
    assert not harness['runtime_implementation_changed'] and not harness['native_rebuild_required']
    assert report['validation_fixture_correction'] == harness['correction']
    validation_only = harness['validation_only_files']
    assert set(validation_only) == {'python/tests/test_public_context.py', 'scripts/prove_native_python_wheel.py'}
    assert set(source) == set(build_source) | set(validation_only)
    assert not set(build_source).intersection(validation_only)
    assert all(source[name] == identity for name, identity in build_source.items())
    for name, proof in validation_only.items():
        assert source[name] == proof['after']
        before = data['current/source-blobs/' + proof['before']['sha256']]
        after = data['current/source-blobs/' + proof['after']['sha256']]
        assert len(before) == proof['before']['bytes'] and digest(before) == proof['before']['sha256']
        expected = before.decode()
        for old, new in proof['replacements']:
            assert expected.count(old) == 1
            expected = expected.replace(old, new, 1)
        assert expected.encode() == after
    diagnostic = harness['unchanged_diagnostic_source']
    content = data['current/source-blobs/' + diagnostic['sha256']]
    assert len(content) == diagnostic['bytes'] and digest(content) == diagnostic['sha256']
    assert b'prepared source changed; prepare the source again' in content
    if repo_root:
        assert (repo_root / diagnostic['path']).read_bytes() == content
    original_core = read('current/python-core-r1/core.json')
    assert original_core == harness['original_outcome']
    assert original_core['status'] == 'failed' and original_core['failure_count'] == 6 and original_core['error_count'] == 4
    original_log = data['current/python-core-r1.log']
    assert len(original_log) == harness['original_log']['bytes'] and digest(original_log) == harness['original_log']['sha256']
    assert not report['changed_dependencies']
    assert [row['receipt'] for row in report['checks']] == [name + '.json' for name in REQUIRED]
    for entry in report['checks']:
        result = read('current/' + entry['receipt'])
        assert result['status'] == 'passed' and result['returncode'] == 0
        assert result['process_group_drained'] and result['direct_child_reaped']
        assert result['command'] == entry['command']
        assert result['head'] == report['source_base']
        log = data['current/' + entry['receipt'].removesuffix('.json') + '.log']
        assert digest(log) == result['log_sha256']
        if 'rust_tests' in entry:
            matches = re.findall(rb'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
            counts = dict(zip(('passed', 'failed', 'ignored'),
                (sum(int(row[i]) for row in matches) for i in range(3))))
            assert counts == entry['rust_tests'] and counts['failed'] == 0
        name = entry['receipt'].removesuffix('.json')
        for filename, identity in source.items():
            relevant = (Path(filename).suffix == '.rs' and name in {
                'format-r2', 'default-clippy-r1', 'default-tests-r1', 'native-clippy-r3',
                'native-tests-r1', 'feature-matrix-r1', 'tensor-minimal-feature-r2'}) or name in {
                    'tensor-binding-r2', 'native-build-r2', 'freeze-runtime-r2', 'context-native-r1', 'python-core-r2', 'static-docs-r1', 'installed-wheel-r1'}
            if relevant:
                if filename in validation_only and name in {'tensor-binding-r2', 'native-build-r2', 'freeze-runtime-r2'}:
                    assert filename not in result['changed_source_inventory']
                    continue
                observed = result['changed_source_inventory'][filename]
                if observed != identity:
                    assert name in {'default-clippy-r1', 'default-tests-r1', 'native-tests-r1', 'feature-matrix-r1'}
                    assert filename == 'shardloom-vortex/src/native_tensor.rs'
                    before = data['current/source-blobs/' + observed['sha256']]
                    after = data['current/source-blobs/' + identity['sha256']]
                    gate = b'#[cfg(feature = "universal-format-io")]\npub(crate) const ARROW_ID:'
                    assert after.count(gate) == 1
                    assert after.replace(gate, b'pub(crate) const ARROW_ID:', 1) == before
    for filename, identity in source.items():
        content = data['current/source-blobs/' + identity['sha256']]
        assert len(content) == identity['bytes'] and digest(content) == identity['sha256']
        if repo_root:
            assert (repo_root / filename).read_bytes() == content, filename

    continuity = read('current/runtime-production-continuity-r2.json')
    initial_build = read('current/native-build-r1.json')
    assert continuity['status'] == 'passed' and continuity['enabled_runtime_equivalent']
    assert continuity['source_base'] == report['source_base']
    assert report['source_verification_scope'] == continuity['verification_scope']
    assert set(continuity['files']) == {name for name in source if name.endswith('.rs')}
    changed = []
    for filename, proof in continuity['files'].items():
        before = data['current/source-blobs/' + initial_build['changed_source_inventory'][filename]['sha256']]
        after = data['current/source-blobs/' + source[filename]['sha256']]
        assert digest(before) == proof['before_sha256'] and digest(after) == proof['current_sha256']
        if before != after:
            changed.append(filename)
            if filename == 'shardloom-vortex/src/native_tensor.rs':
                gate = b'#[cfg(feature = "universal-format-io")]\npub(crate) const ARROW_ID:'
                assert proof['classification'] == 'arrow_constant_feature_gate_only'
                assert after.count(gate) == 1 and after.replace(gate, b'pub(crate) const ARROW_ID:', 1) == before
            else:
                assert proof['classification'] == 'test_fixture_only'
                marker = b'#[cfg(test)]\nmod tests {'
                assert before.count(marker) == after.count(marker) == 1
                assert before.split(marker)[0] == after.split(marker)[0]
        else:
            assert proof['classification'] == 'identical'
    assert changed == ['shardloom-vortex/src/native_tensor.rs', 'shardloom-vortex/src/native_tensor_arrow.rs']
    variants = continuity['resolved_feature_variants']
    for name in ('native_tests', 'release_runtime'):
        assert 'universal-format-io' in variants[name]['vortex_features']
    assert not set(variants['default']['vortex_features']).intersection({'vortex-local-primitives', 'vortex-write'})
    if repo_root:
        for name, identity in continuity['unchanged_manifests'].items():
            content = (repo_root / name).read_bytes()
            assert len(content) == identity['bytes'] and digest(content) == identity['sha256']

    workspace = read('current/workspace-sequence-r1.json')
    assert workspace['status'] == 'passed' and workspace['completed'] == [
        'format-r1', 'default-clippy-r1', 'default-tests-r1', 'native-tests-r1', 'feature-matrix-r1']
    sequence = read('current/python-sequence-r1.json')
    assert sequence['status'] == 'failed' and sequence['active_stage'] == 'python-core-r1'
    assert sequence['completed'] == [
        'format-r2', 'tensor-minimal-feature-r2', 'native-clippy-r3',
        'tensor-binding-r2', 'native-build-r2', 'freeze-runtime-r2']
    followup = read('current/python-followup-sequence-r1.json')
    assert followup['status'] == 'passed' and followup['prior_sequence'] == 'python-sequence-r1.json'
    assert followup['completed'] == ['context-native-r1', 'python-core-r2', 'static-docs-r1', 'installed-wheel-r1']
    core = read('current/python-core-r2/core.json')
    assert core['status'] == 'passed' and core['failure_count'] == core['error_count'] == 0
    match = re.search(rb'Ran (\d+) tests? in [0-9.]+s', data['current/python-core-r2.log'])
    assert match
    expected_core = dict(status='passed', discovered_test_cases=core['test_count'],
        reported_test_runs=int(match.group(1)), skip_records=core['skipped_count'], failures=0, errors=0)
    assert report['python_core'] == expected_core
    installed = read('current/installed-proof-r1/report.json')
    assert installed['status'] == installed['uninstall_status'] == 'passed'
    assert installed['integration'] == report['installed_package']
    assert installed['integration']['tests'] >= 350 and installed['integration']['status'] == 'passed'
    assert all(installed['integration'][key] == 0 for key in ('failures', 'errors', 'skipped'))
    assert not installed['publication_attempted'] and not installed['performance_claim']
    for key in ('cli_sha256', 'native_library_sha256', 'wheel_sha256'):
        assert report[key] == installed['integrity'][key]
    frozen = read('current/runtime-r2-identity.json')
    assert frozen['status'] == 'passed' and frozen['build_receipt'] == 'native-build-r2.json'
    assert frozen['build_receipt_sha256'] == digest(data['current/native-build-r2.json'])
    identities = {Path(row['path']).name: row['sha256'] for row in frozen['files']}
    assert identities == {'shardloom': report['cli_sha256'], 'lib_native.dylib': report['native_library_sha256']}
    log = data['current/installed-proof-r1/integration.log'].decode()
    observed = sorted(re.findall(r'^(test_fixed_tensor_\S+) .* \.\.\. ok$', log, re.MULTILINE))
    assert len(observed) == len(set(observed)) == 12
    assert observed == report['tensor_installed_tests']
    context_tests = sorted(re.findall(r'^test_\S+ \((test_public_context\.\S+)\) \.\.\. ok$', log, re.MULTILINE))
    context_source = data['current/source-blobs/' + source['python/tests/test_public_context.py']['sha256']]
    expected_context = sorted('test_public_context.' + node.name + '.' + method.name
        for node in ast.parse(context_source).body if isinstance(node, ast.ClassDef)
        for method in node.body if isinstance(method, ast.FunctionDef) and method.name.startswith('test_'))
    assert len(expected_context) == 19 and context_tests == expected_context == report['context_installed_tests']
    failures = [name for name in sorted(data) if name.startswith('current/')
                and name.count('/') == 1 and name.endswith('.json')
                and read(name).get('status') == 'failed']
    assert failures == [entry['receipt'] for entry in report['original_failures']]
    assert all(entry['disposition'] for entry in report['original_failures'])
    for name in failures:
        value = read(name)
        if 'log_sha256' in value:
            assert digest(data[name.removesuffix('.json') + '.log']) == value['log_sha256']

    provider = read('current/provider-review/receipt.json')
    archive_raw = data['current/provider-review/vortex-tensor-0.90.0.crate']
    assert provider['status'] == 'downloaded_verified_source_only'
    assert provider['version'] == '0.90.0' and provider['license'] == 'Apache-2.0'
    assert not provider['runtime_dependency_added'] and digest(archive_raw) == provider['checksum']
    assert len(archive_raw) == provider['archive_bytes']
    with tarfile.open(fileobj=io.BytesIO(archive_raw), mode='r:gz') as archive:
        actual = {entry.name.removeprefix('vortex-tensor-0.90.0/'): entry
                  for entry in archive.getmembers() if entry.isfile()}
        assert set(actual) == set(provider['files'])
        for name, identity in provider['files'].items():
            content = archive.extractfile(actual[name]).read()
            assert len(content) == identity['bytes'] and digest(content) == identity['sha256']

    for prefix, number, stem in [('managed', 1659, 'binary'), ('dependency', 1660, 'dependency')]:
        closeout = read(prefix + '/' + stem + '-closeout-r1.json')
        assert closeout['status'] == 'complete' and closeout['pr_checks_passed'] == 58
        assert closeout['main_checks_passed'] == 55 and not closeout['review_findings']
        assert report['preceding_integrations'][prefix] == dict(pr=number,
            merge_commit=closeout['merge_commit'], pr_checks_passed=58, main_checks_passed=55)
        for name, identity in closeout['receipts'].items():
            content = data[prefix + '/' + name]
            assert len(content) == identity['bytes'] and digest(content) == identity['sha256']
        if repo_root:
            for name, identity in closeout['committed_evidence'].items():
                content = (repo_root / name).read_bytes()
                assert len(content) == identity['bytes'] and digest(content) == identity['sha256']
        main = read(prefix + '/' + stem + '-main' + str(number) + '-r1.json')
        assert main['status'] == 'passed' and main['source_commit'] == closeout['merge_commit']
        assert main['pr_head'] == closeout['pr_head'] and main['source_tree'] == closeout['source_tree']
        for name, checksum in main['raw_sources'].items():
            assert digest(data[prefix + '/' + name]) == checksum
        raw_checks = read(prefix + '/' + stem + '-main' + str(number) + '-r1-raw/checks.json')
        assert raw_checks['total_count'] == len(raw_checks['check_runs'])
        required = set(main['required_hosted_check_names'])
        assert len(required) == 55
        checks = [row for row in raw_checks['check_runs'] if row['name'] in required]
        assert {row['name'] for row in checks} == required and len(checks) == 55
        assert all(row['head_sha'] == closeout['merge_commit'] and row['status'] == 'completed'
                   and row['conclusion'] == 'success' for row in checks)
    assert report['source_base'] == report['preceding_integrations']['dependency']['merge_commit']
    assert not report['performance_claim'] and not report['publication_attempted']
    assert not report['whole_item_19_complete'] and report['hosted_integration'] == 'pending'
    return dict(status='passed', archive_members=len(members), changed_sources=len(source),
                installed_tests=installed['integration']['tests'], tensor_tests=len(observed))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root)))
