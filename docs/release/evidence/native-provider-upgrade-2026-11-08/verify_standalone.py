"""Verify standalone evidence against exact current provider/configuration inputs."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re

SELECTION = {
    'vortex-array': ('provider-array-final-r1.json', 'provider-array-final-r1/report.json'),
    'vortex-arrow': ('vortex-arrow-standalone-r3.json', 'vortex-arrow-standalone-r3/report.json'),
    **{name: ('provider-affected-standalone-r2.json', 'provider-affected-standalone-r2/report.json')
       for name in ('vortex-layout', 'vortex-file', 'vortex-fsst', 'vortex-zstd',
                    'vortex-datetime-parts', 'vortex-io', 'arrow-data')},
    'vortex-buffer': ('standalone-final-r1.json', 'standalone-final-r1/buffer/report.json'),
    **{name: ('standalone-final-r1.json', 'standalone-final-r1/formats/report.json')
       for name in ('orc-rust', 'arrow-avro', 'arrow-ipc', 'arrow-array', 'parquet')},
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw), sha256=sha(raw))


def source_fingerprint(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def configuration(raw):
    tree = ast.parse(raw)
    config = None
    statements = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in
                {'GROUPS', 'CONFIGURATIONS'} for t in node.targets):
            if any(isinstance(t, ast.Name) and t.id == 'CONFIGURATIONS' for t in node.targets):
                config = ast.literal_eval(node.value)
        else:
            statements.append(ast.dump(node, include_attributes=False))
    assert isinstance(config, dict)
    return config, statements


def commands(crate, config):
    args = ['--manifest-path', 'vendor/' + crate + '/Cargo.toml']
    for patch in config['patches']:
        args += ['--config', 'patch.crates-io.' + patch + '.path="vendor/' + patch + '"']
    if config.get('no_default_features'):
        args.append('--no-default-features')
    if config['features']:
        args += ['--features', config['features']]
    elif not config.get('no_default_features'):
        args += ['--all-features']
    if crate == 'vortex-buffer':
        return [['cargo', 'test', '--locked', *args],
                ['cargo', 'clippy', '--locked', *args, '--all-targets', '--', '-D', 'warnings']]
    return [['cargo', 'test', '--locked', *args,
             *(['--lib', '--tests'] if config.get('integration_tests') else ['--lib']), '--', '--test-threads=2'],
            ['cargo', 'clippy', '--locked', *args,
             *(['--lib', '--tests'] if config.get('clippy_tests') else ['--lib']), '--', '-D', 'warnings']]


def verify(read, current_sources):
    obj = lambda path: json.loads(read('observations/provider/' + path))

    def raw_source(sources, path):
        item = sources[path]
        raw = read('source-blobs/' + item['sha256'])
        assert identity(raw) == item, path
        return raw

    driver = 'scripts/ci_provider_tests.py'
    current_config, current_logic = configuration(raw_source(current_sources, driver))
    assert set(current_config) == set(SELECTION)
    rows = []
    for crate, (guard_path, child_path) in sorted(SELECTION.items()):
        guard, child = obj(guard_path), obj(child_path)
        assert guard['status'] == child['status'] == 'passed'
        assert guard['returncode'] == 0 and guard['process_group_drained'] and guard['direct_child_reaped']
        assert guard['fixed_source_validation'] and not guard['allow_lockfile_update']
        assert not guard['timed_out'] and guard['interrupted_signal'] is None
        sources = guard['build_sources']
        assert source_fingerprint(sources) == guard['build_source_fingerprint_sha256']
        for point in ('pre_execution', 'post_execution'):
            validation = guard['build_source_verification'][point]
            assert validation['head_matches'] and validation['path_set_matches'] and not validation['changed_paths']
            assert validation['fingerprint_sha256'] == source_fingerprint(sources)
        assert sha(read('observations/provider/' + guard_path.removesuffix('.json') + '.log')) == guard['log_sha256']
        old_config, old_logic = configuration(raw_source(sources, driver))
        assert old_config[crate] == current_config[crate] and old_logic == current_logic
        config = current_config[crate]
        providers = [crate, *config['patches']]
        prefixes = tuple('vendor/' + p + '/' for p in providers)
        extra = {'.github/provider-locks/' + crate + '.lock',
                 config.get('clippy_conf_dir', 'vendor/vortex-array') + '/clippy.toml',
                 'scripts/ci_build_artifact.py', 'rust-toolchain.toml'}
        def relevant(path):
            return (path.startswith(prefixes) and not path.endswith('/Cargo.lock')) or path in extra or path.startswith('.cargo/')
        paths = {p for p in sources.keys() | current_sources.keys() if relevant(p)}
        changes = []
        compiled_paths = []
        for path in sorted(paths):
            before, after = sources.get(path), current_sources.get(path)
            if before != after:
                assert path.endswith('/upstream-provenance.json'), 'standalone source drift: ' + crate + ':' + path
                changes.append(dict(path=path, before=before, after=after,
                    disposition='Only source/license metadata changed or was added; current archive replay separately verifies it.'))
            if before and 'bytes' in before:
                raw_source(sources, path)
            if after and 'bytes' in after:
                raw = raw_source(current_sources, path)
                if path.endswith('.rs') or path.endswith('/Cargo.toml') or path.endswith('/build.rs'):
                    leading_comments = True
                    for line in raw.splitlines():
                        stripped = line.lstrip()
                        if stripped and not stripped.startswith(b'//'):
                            leading_comments = False
                        if b'upstream-provenance.json' in line:
                            assert leading_comments and stripped.startswith(b'//'), path
                    compiled_paths.append(path)
        expected_commands = commands(crate, config)
        observed = [entry for entry in child['commands'] if entry['crate'] == crate]
        assert len(observed) == 2 and [entry['command'] for entry in observed] == expected_commands
        outcomes = []
        for entry in observed:
            assert entry['returncode'] == 0
            lock = '.github/provider-locks/' + crate + '.lock'
            assert entry['lock_sha256'] == current_sources[lock]['sha256'] == sources[lock]['sha256']
            assert entry['clippy_conf_dir'].endswith('/' + config.get('clippy_conf_dir', 'vendor/vortex-array'))
            for key in ('arrow_test_data', 'parquet_test_data'):
                if key in config:
                    assert entry[key].endswith('/' + config[key])
            log_path = str(Path(child_path).parent / entry['log'])
            raw = read('observations/provider/' + log_path)
            assert sha(raw) == entry['log_sha256']
            matches = re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
            counts = dict(zip(('passed', 'failed', 'ignored'),
                (sum(int(row[i]) for row in matches) for i in range(3)))) if matches else None
            if entry['command'][1] == 'test':
                assert counts is not None and counts['passed'] > 0 and counts['failed'] == 0
            else:
                assert b'Finished `dev` profile' in raw and b'error:' not in raw
            outcomes.append(dict(command=entry['command'], log=log_path, log_sha256=sha(raw), rust_tests=counts))
        rows.append(dict(provider=crate, guard_receipt=guard_path, child_report=child_path,
                         guard_receipt_identity=identity(read('observations/provider/' + guard_path)),
                         child_report_identity=identity(read('observations/provider/' + child_path)),
                         relevant_input_paths=len(paths), identical_compiled_files=len(compiled_paths),
                         provenance_only_changes=changes, configuration_unchanged=True,
                         driver_logic_unchanged=True, outcomes=outcomes))
    return dict(status='passed', provider_count=len(rows), providers=rows,
                current_source_fingerprint_sha256=source_fingerprint(current_sources),
                scope='Completed provider test/lint evidence reused only after exact source/lock/config verification; provenance-only updates are replayed separately.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-receipt', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    n = Path(__file__).resolve().parent.parent
    assert not args.output.exists()
    receipt = json.loads((n / args.source_receipt).read_bytes())
    assert receipt['status'] == 'passed' and receipt['fixed_source_validation']
    def read(path):
        return (n / path.removeprefix('observations/provider/')).read_bytes()
    result = verify(read, receipt['build_sources'])
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(status='passed', providers=result['provider_count'])))
