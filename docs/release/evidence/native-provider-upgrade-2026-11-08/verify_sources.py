"""Replay pinned provider, graph, fixture and notice identities without network access."""
import hashlib
import io
import json
from pathlib import PurePosixPath
import re
import tarfile
import tomllib


PROVIDERS = {
    'arrow-array': '59.3.2', 'arrow-avro': '59.3.2', 'arrow-data': '59.3.2',
    'arrow-ipc': '59.3.2', 'parquet': '59.3.2', 'orc-rust': '0.9.0',
    **{name: '0.93.0' for name in ('vortex-array', 'vortex-arrow', 'vortex-buffer',
        'vortex-datetime-parts', 'vortex-file', 'vortex-fsst', 'vortex-io',
        'vortex-layout', 'vortex-zstd')},
}
LICENSES = {'Apache-2.0', 'MIT', 'Apache-2.0 AND MIT', 'MIT OR Apache-2.0',
            'Apache-2.0 OR MIT'}
VORTEX_COMMIT = 'c1439e69bf833fc702a60335ed674a1674ca358d'
VORTEX_LICENSE = 'cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return {'bytes': len(raw), 'sha256': sha(raw)}


def package_key(package):
    return (package['name'], package['version'], package.get('source'),
            package.get('checksum'))


def safe(path):
    value = PurePosixPath(path)
    assert path and not value.is_absolute() and '..' not in value.parts
    assert str(value) == path
    return path


def graph(raw):
    packages = tomllib.loads(raw.decode())['package']
    assert len({package_key(p) for p in packages}) == len(packages)
    return packages


def verify(read, sources, baseline_lock_paths):
    def obj(path):
        return json.loads(read(path))

    def current(path):
        entry = sources[path]
        raw = read('source-blobs/' + entry['sha256'])
        assert identity(raw) == entry, 'source:blob:' + path
        return raw

    cache = {}

    def crate(name, version, checksum):
        if checksum not in cache:
            raw = read('crate-blobs/' + checksum + '.crate')
            assert sha(raw) == checksum, 'archive:checksum:' + name
            prefix = name + '-' + version + '/'
            members = {}
            with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
                for member in archive:
                    safe(member.name)
                    assert member.name.startswith(prefix) and member.isfile()
                    relative = member.name.removeprefix(prefix)
                    assert relative not in members
                    content = archive.extractfile(member).read()
                    assert len(content) == member.size
                    members[relative] = content
            cache[checksum] = members
        members = cache[checksum]
        package = tomllib.loads(members['Cargo.toml'].decode())['package']
        assert (package['name'], package['version']) == (name, version)
        return members

    current_graph = graph(current('Cargo.lock'))
    before_graph = graph(read('baseline/provider/Cargo.lock'))
    current_keys, before_keys = ({package_key(p) for p in rows}
                                  for rows in (current_graph, before_graph))
    assert {p['version'] for p in current_graph if p['name'].startswith('arrow')
            or p['name'] == 'parquet'} == {'59.3.2'}, 'graph:single_arrow'
    assert {p['version'] for p in current_graph if p['name'].startswith('vortex')} == {'0.93.0'}
    assert not any(re.search(r'(^|[-_])(datafusion|duckdb|spark|polars|velox|trino|dask|ray)([-_]|$)',
                            p['name'], re.I) for p in current_graph)
    inventory = obj('observations/provider/resolved-graph-inventory-r3.json')
    proof = obj('observations/provider/resolved-graph-primary-verification-r3.json')
    assert proof['status'] == 'passed'
    assert proof['inventory_sha256'] == sha(read('observations/provider/resolved-graph-inventory-r3.json'))
    expected = {('base', key) for key in before_keys - current_keys}
    expected |= {('current', key) for key in current_keys - before_keys}
    records = inventory['r2_manifest_evidence']
    assert {(r['side'], package_key(r['lock_record'])) for r in records} == expected
    assert len(records) == len(expected) == proof['changed_package_records']
    verified = []
    for row in records:
        package, evidence = row['lock_record'], row['manifest_evidence']
        name, version, source, checksum = package_key(package)
        if source is None:
            path = 'vendor/' + name + '/Cargo.toml'
            raw = current(path) if row['side'] == 'current' else read('baseline/provider/' + path)
        else:
            assert source == 'registry+https://github.com/rust-lang/crates.io-index'
            raw = crate(name, version, checksum)['Cargo.toml']
            assert all(a['sha256'] == checksum for a in evidence['archives'])
        assert sha(raw) == evidence['manifest_sha256']
        manifest = tomllib.loads(raw.decode())['package']
        assert (manifest['name'], manifest['version']) == (name, version)
        assert manifest['license'] in LICENSES
        minimum = manifest.get('rust-version')
        assert not minimum or tuple(map(int, minimum.split('.'))) <= (1, 96, 0)
        assert manifest['license'] == evidence['license']
        assert minimum == evidence.get('rust-version')
        verified.append({'side': row['side'], 'package': package,
                         'manifest_sha256': sha(raw), 'license': manifest['license'],
                         'rust_version': minimum})
    assert verified == proof['verified']
    assert proof['lock_hashes'] == {'base': sha(read('baseline/provider/Cargo.lock')),
                                   'current': sha(current('Cargo.lock'))}

    provenance_paths = {p for p in sources if p.startswith('vendor/')
                        and p.endswith('/upstream-provenance.json') and 'sha256' in sources[p]}
    assert provenance_paths == {'vendor/' + name + '/upstream-provenance.json' for name in PROVIDERS}
    provider_rows = []
    for name, version in sorted(PROVIDERS.items()):
        prefix = 'vendor/' + name + '/'
        provenance = json.loads(current(prefix + 'upstream-provenance.json'))
        assert provenance['provider'] == name and provenance['version'] == version
        original = crate(name, version, provenance['registry_archive_sha256'])
        assert {p: sha(raw) for p, raw in original.items()} == provenance['original_files_sha256']
        expected_files = {p: sha(raw) for p, raw in original.items()
                          if p not in provenance.get('omitted_files', {})}
        expected_files.update(provenance.get('modified_files_sha256', {}))
        expected_files.update(provenance.get('added_files_sha256', {}))
        actual_files = {p.removeprefix(prefix) for p, entry in sources.items()
                        if p.startswith(prefix) and 'sha256' in entry}
        assert actual_files == set(expected_files) | {'upstream-provenance.json'}, 'provider:coverage:' + name
        for relative, digest in expected_files.items():
            assert sha(current(prefix + relative)) == digest, 'provider:map:' + name + '/' + relative
        if name.startswith('vortex-'):
            assert provenance['vcs_info']['git']['sha1'] == VORTEX_COMMIT
            assert sha(current(prefix + 'LICENSE')) == VORTEX_LICENSE
        for relative, fixture in provenance.get('supplemental_test_files', {}).items():
            raw = current(prefix + relative)
            assert sha(raw) == fixture.get('current_sha256', fixture['sha256'])
            assert len(raw) == fixture.get('current_bytes', fixture['bytes'])
            if fixture.get('current_sha256') is None:
                assert hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest() == fixture['git_blob_sha1']
            if path := fixture.get('test_source'):
                test = current(prefix + path)
                assert sha(test) == fixture['test_source_current_sha256']
                assert sha(original[path]) == fixture['test_source_published_archive_sha256']
                if module := fixture.get('retained_test_module'):
                    marker = module['marker'].encode()
                    tail = test[test.index(marker):]
                    assert identity(tail) == {k: module[k] for k in ('bytes', 'sha256')}
        provider_rows.append({'name': name, 'version': version, 'files': len(expected_files)})

    development = obj('observations/provider/standalone-graph-primary-review-r2.json')
    all_standalone = set()
    assert {row['path'] for row in development['graphs']} == {
        '.github/provider-locks/' + name + '.lock' for name in PROVIDERS}
    for row in development['graphs']:
        raw = current(row['path'])
        assert sha(raw) == row['sha256']
        packages = graph(raw)
        assert len(packages) == row['package_count']
        versions = sorted({p['version'] for p in packages if p['name'].startswith('arrow')
                           or p['name'] == 'parquet'})
        assert versions == row['arrow_versions']
        assert versions in ([], ['59.3.2']) or row['path'].endswith('/orc-rust.lock') and versions == ['58.4.1', '59.3.2']
        assert all(p['version'] == '0.93.0' for p in packages if p['name'].startswith('vortex'))
        assert not any(any(token in p['name'].lower() for token in
                           ('datafusion', 'duckdb', 'spark', 'polars', 'velox', 'vortex-runend')) for p in packages)
        all_standalone.update(package_key(p) for p in packages)
    prior = set()
    for path in baseline_lock_paths:
        prior.update(package_key(p) for p in graph(read('baseline/provider/' + path)))
    absent = all_standalone - current_keys
    new = absent - prior
    assert len(absent & prior) == development['unchanged_prior_development_identities']
    assert {(p['name'], p['version'], 'registry+https://github.com/rust-lang/crates.io-index', p['checksum'])
            for p in development['new_development_packages']} == new
    for row in development['new_development_packages']:
        members = crate(row['name'], row['version'], row['checksum'])
        raw = members['Cargo.toml']
        assert sha(raw) == row['manifest_sha256']
        package = tomllib.loads(raw.decode())['package']
        assert package['license'] == row['license'] and package['license'] in LICENSES
        assert package.get('rust-version') == row['declared_rust_version']
        for legal in row['root_legal_members']:
            assert identity(members[legal['path']]) == {k: legal[k] for k in ('bytes', 'sha256')}

    fixtures = obj('observations/provider/parquet-fixture-installation-r1.json')['files']
    assert len(fixtures) == 268
    for path, entry in fixtures.items():
        raw = current('vendor/parquet/' + path)
        assert identity(raw) == {k: entry[k] for k in ('bytes', 'sha256')}
        assert hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest() == entry['git_blob_sha1']
        assert (entry['source_repository'], entry['source_commit']) in {
            ('apache/parquet-testing', '09f3cdbde45302f0f0c689c950e465e98a9df960'),
            ('apache/arrow-testing', '735ae7128d571398dd798d7ff004adebeb342883')}

    notices = json.loads(current('python/THIRD_PARTY_LICENSES/manifest.json'))
    locked = {(p['name'], p['version'], p['source']): p['checksum']
              for p in current_graph if p.get('source', '').startswith('registry+')}
    assert {(p['name'], p['version'], p['source']): p['crate_checksum_sha256']
            for p in notices['components']} == locked, 'notices:graph'
    texts, legal_records = set(), 0
    assert {p['name'] for p in notices['vendored_components']} == set(PROVIDERS)
    for component in notices['vendored_components']:
        name = component['name']
        assert component['version'] == PROVIDERS[name]
        assert component['provenance'] == json.loads(current('vendor/' + name + '/upstream-provenance.json')), 'notices:provider_provenance:' + name
    for component in notices['components'] + notices['vendored_components']:
        assert component['license'] and component['files']
        for entry in component['files']:
            raw = current('python/THIRD_PARTY_LICENSES/' + entry['text'])
            assert identity(raw) == {k: entry[k] for k in ('bytes', 'sha256')}
            assert entry['text'] == 'texts/' + entry['sha256'] + '.txt'
            assert entry.get('crate_path') or entry.get('upstream_url')
            texts.add(entry['text'])
            legal_records += 1
    assert {p.removeprefix('python/THIRD_PARTY_LICENSES/') for p, entry in sources.items()
            if p.startswith('python/THIRD_PARTY_LICENSES/texts/') and 'sha256' in entry} == texts
    notice_summary = dict(status='passed', registry_packages=len(notices['components']),
                          vendored_packages=len(notices['vendored_components']),
                          license_file_records=legal_records, unique_text_files=len(texts))
    assert notice_summary == dict(status='passed', registry_packages=431, vendored_packages=15,
                                  license_file_records=829, unique_text_files=249)
    return dict(status='passed', changed_graph_records=len(records), providers=provider_rows,
                standalone_graphs=len(development['graphs']), new_development_packages=len(new), parquet_fixtures=len(fixtures),
                dependency_notices=notice_summary, offline=True)
