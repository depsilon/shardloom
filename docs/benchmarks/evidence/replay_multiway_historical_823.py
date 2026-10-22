"""Replay complete historical source/result evidence without Git or an engine.

Extracts only a fully enumerated regular-file packet into a temporary local
directory. Historical executable identities remain metadata; binaries are not
needed to recheck the saved observations or their finite decision.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
import tempfile

ORIGINAL_E = Path('/Users/dylan/LocalData/shardloom/conditional-investigations-20261019')
ORIGINAL_W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(path):
    path = Path(path)
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(path=str(path), bytes=path.stat().st_size, sha256=digest)


def check(data, expected):
    assert len(data) == expected['bytes'] and sha(data) == expected['sha256']


def load(path):
    return json.loads(Path(path).read_bytes())


def safe_member(name):
    p = PurePosixPath(name)
    assert not p.is_absolute() and p.parts and '..' not in p.parts
    assert str(p) == name and '\\' not in name
    return p


def local(root, original):
    return root / Path(original).relative_to(ORIGINAL_E)


def source_closure(root):
    pool = root / 'multiway-source-portability-821'
    versions = load(pool / 'versions.json')['frozen_versions']
    projection = load(pool / 'projection-version.json')
    assert projection['version'] not in versions
    versions[projection['version']] = projection
    assert len(versions) == 6
    checked, extra = 0, 0
    source_maps = {}
    for name, version in versions.items():
        path = root / (name + '-input.json')
        manifest = load(path)
        check(path.read_bytes(), version.get('manifest_identity', version.get('manifest')))
        assert manifest['files'].keys() == version['files'].keys()
        for source, expected in manifest['files'].items():
            value = version['files'][source]
            assert {key: value[key] for key in ('bytes', 'sha256')} == expected
            assert value['blob'] == 'blobs/' + value['sha256']
            check((pool / value['blob']).read_bytes(), expected)
            checked += 1
        extras = version['changed_or_untracked_text_outside_manifest']
        for value in extras.values():
            assert value['blob'] == 'blobs/' + value['sha256']
            check((pool / value['blob']).read_bytes(), value)
            extra += 1
        archive = local(root, manifest['source_archive'])
        assert sha(archive.read_bytes()) == manifest['source_archive_sha256']
        source_maps[name] = dict(version['files'], **extras)
    assert checked == 11432 and extra == 21, (checked, extra)
    public = load(pool / 'public-sources.json')['accepted_source_record']
    record_path = local(root, public['path'])
    check(record_path.read_bytes(), public)
    record = load(record_path)
    assert public['files'].keys() == record['source_hashes'].keys()
    for source, expected in record['source_hashes'].items():
        value = public['files'][source]
        assert value['sha256'] == expected
        check((pool / value['blob']).read_bytes(), value)
    assert len(public['files']) == 2227
    old_scripts = load(root / 'multiway-historical-scripts-818b.json')['members']
    historical = {}
    for row in old_scripts:
        data = base64.b64decode(row['base64'], validate=True)
        check(data, row)
        historical[row['path'], row['sha256']] = data
    bindings = ['multiway-probe-binding-818b.json', 'multiway-probe-binding-818c.json',
                'multiway-integration-binding-819a.json', 'multiway-projection-binding-821a.json']
    binding_entries = 0
    binary_ids = []
    for filename in bindings:
        binding = load(root / filename)
        manifest_path = local(root, binding['manifest'])
        assert sha(manifest_path.read_bytes()) == binding['manifest_sha256']
        version_name = manifest_path.name.removesuffix('-input.json')
        for original, expected in binding['files'].items():
            path = Path(original)
            if path.is_relative_to(ORIGINAL_E):
                retained = local(root, path)
                data = retained.read_bytes()
                if sha(data) != expected['sha256']:
                    data = historical[original, expected['sha256']]
            else:
                relative = str(path.relative_to(ORIGINAL_W))
                value = source_maps[version_name][relative]
                data = (pool / value['blob']).read_bytes()
            check(data, expected)
            binding_entries += 1
        binary_ids.append(dict(binding=filename, sha256=binding['binary_sha256'], bytes=binding.get('binary_bytes')))
    assert binding_entries == 89
    intake = load(root / 'multiway-intake-binding-817a.json')
    assert intake['manifest_sha256'] == sha((root / 'multiway-intake-build-817a-input.json').read_bytes())
    assert intake['files_verified'] == 1902
    binary_ids.append(dict(binding='multiway-intake-binding-817a.json', sha256=intake['executable_sha256']))
    return dict(compile_versions=6, compile_file_references=checked, extra_source_references=extra,
                public_source_references=len(public['files']), binding_file_references=binding_entries,
                historical_binary_identities=binary_ids, historical_public_labels_do_not_imply_acceptance=True)


def replay(root, output):
    results = []
    def run(script, arguments, expected_name):
        destination = output / expected_name
        # -I excludes the extracted helper directory; each reviewed verifier
        # imports its adjacent oracle, so use an explicit isolated launcher.
        launcher = 'import runpy,sys; sys.path.insert(0,sys.argv[1]); script=sys.argv[2]; sys.argv=sys.argv[2:]; runpy.run_path(script,run_name="__main__")'
        command = [sys.executable, '-I', '-B', '-c', launcher, str(root), str(root / script), *map(str, arguments), str(destination)]
        done = subprocess.run(command, cwd=output, capture_output=True, text=True, timeout=600)
        (output / (expected_name + '.log')).write_text(done.stdout + done.stderr)
        assert done.returncode == 0, (script, done.stdout, done.stderr)
        result = load(destination)
        assert result == load(root / expected_name), expected_name
        results.append(dict(file=expected_name, sha256=sha(destination.read_bytes()),
            workflows=result['workflows'], queries=result['queries'],
            complete_output_rows_checked=result['complete_output_rows_checked'],
            negative_controls_rejected=result.get('negative_controls_rejected', result.get('negative_validator_controls_rejected')),
            quantitative_gates_passed=result.get('quantitative_gates_passed'), failed_gates=result.get('failed_gates', [])))
        print(json.dumps(dict(replayed=expected_name, queries=result['queries'])), flush=True)
        return result
    run('verify_multiway_intake_817.py', [root / 'multiway-intake-817a-results.jsonl'], 'multiway-intake-817a-verified.json')
    families = [('multiway-', '819a', 'verify_multiway_screen_818.py'),
                ('multiway-integrated-', '820a', 'verify_multiway_screen_819.py'),
                ('multiway-projected-', '821a', 'verify_multiway_screen_819.py')]
    for prefix, suffix, verifier in families:
        for stage, label in [('preflight', 'preflight'), ('A', 'cohortA'), ('B', 'cohortB')]:
            name = prefix + label + '-' + suffix
            result = run(verifier, [stage, root / 'multiway-references-818a', root / (name + '-witnesses'),
                                   root / (name + '-results.jsonl')], name + '-verified.json')
            assert result['queries'] == (80 if stage == 'preflight' else 304)
            if stage != 'preflight':
                assert result['quantitative_gates_passed'] is not (prefix == 'multiway-projected-' and stage == 'A')
            invocation = load(root / (name + '-invocation.json'))
            binding = load(local(root, invocation['binding']))
            assert invocation['binding_sha256'] == sha(local(root, invocation['binding']).read_bytes())
            assert invocation['returncode'] == 0
            assert invocation['source_inputs_checked_before'] == invocation['source_inputs_checked_after'] == binding['input_files_checked']
            assert invocation['binary_sha256_before'] == invocation['binary_sha256_after'] == binding['binary_sha256']
            assert invocation['records_sha256'] == result['records_sha256']
            for witness, expected in invocation['witnesses'].items():
                check((root / (name + '-witnesses') / witness).read_bytes(), expected)
            receipt = load(root / (name + '.json'))
            assert receipt['status'] == 'passed' and receipt['returncode'] == 0 and receipt['process_group_drained']
            assert receipt['log_sha256'] == sha((root / (name + '.log')).read_bytes())
    assert sum(row['queries'] for row in results) == 2080
    assert sum(row['complete_output_rows_checked'] for row in results) == 16160040
    failed = [row for row in results if row['quantitative_gates_passed'] is False]
    assert len(failed) == 1
    assert {(row['cell'], row['calls'], row['gate']) for row in failed[0]['failed_gates']} == {
        ('star_control', 1, 'engine_execution_ns'), ('star_control', 1, 'complete_ns')}
    disposition = load(root / 'multiway-projected-disposition-822.json')
    assert disposition['outcome'] == 'not_retained' and disposition['production_accepted'] is False
    assert disposition['timing_failure_repeated'] is False
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--scratch-root', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists() and args.scratch_root.is_dir()
    assert not any(part in ('Desktop', 'Documents', 'CloudDocs', 'Mobile Documents') for part in args.scratch_root.resolve().parts)
    archive_id = identity(args.archive)
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    assert manifest['production_accepted'] is False and manifest['historical_only'] is True
    rows = manifest['members']
    expected = {row['member']: row for row in rows}
    assert len(expected) == len(rows) and 'MANIFEST.json' not in expected
    assert sum(row['bytes'] for row in rows) < 256 << 20
    for name in expected:
        safe_member(name)
    with tempfile.TemporaryDirectory(prefix='multiway-evidence-replay-', dir=args.scratch_root) as temporary:
        root = Path(temporary) / 'evidence'
        root.mkdir()
        output = Path(temporary) / 'verified'
        output.mkdir()
        with tarfile.open(args.archive, 'r:xz') as archive:
            members = archive.getmembers()
            assert len(members) == len(expected) + 1
            assert {member.name for member in members} == set(expected) | {'MANIFEST.json'}
            for member in members:
                safe_member(member.name)
                assert member.isfile() and not member.issym() and not member.islnk()
                if member.name == 'MANIFEST.json':
                    assert archive.extractfile(member).read() == manifest_bytes
                    continue
                row = expected[member.name]
                assert member.size == row['bytes']
                target = root / member.name
                target.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with archive.extractfile(member) as stream, target.open('xb') as destination:
                    for chunk in iter(lambda: stream.read(1 << 20), b''):
                        digest.update(chunk)
                        destination.write(chunk)
                assert digest.hexdigest() == row['sha256'] and target.stat().st_size == row['bytes']
        closure = source_closure(root)
        result = replay(root, output)
        assert identity(args.archive) == archive_id
        report = dict(status='passed', archive=archive_id, manifest=identity(args.manifest), verifier=identity(__file__),
            members_verified=len(rows) + 1, content_bytes_verified=sum(row['bytes'] for row in rows),
            sources=closure, results=result, queries_replayed=sum(row['queries'] for row in result),
            complete_output_rows_checked=sum(row['complete_output_rows_checked'] for row in result),
            production_accepted=False, outcome='not_retained', timing_failure_repeated=False,
            historical_results_reproduced_exactly=True, requires_git=False, requires_engine=False)
    with args.output.open('x') as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({key: value for key, value in report.items() if key not in ('sources', 'results')}), flush=True)


if __name__ == '__main__':
    main()
