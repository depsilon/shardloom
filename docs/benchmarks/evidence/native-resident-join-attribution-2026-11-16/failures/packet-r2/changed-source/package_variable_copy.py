"""Preserve complete fresh and historical evidence without executing a query."""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tarfile

OUT = Path(__file__).resolve().parent.parent
ROOT = OUT.parent / 'optimization-investigation-queue-worktree'
Q = OUT.parent / 'selected-dictionary-2026-11-15'
GIB, MIB = 1 << 30, 1 << 20


def read(path):
    return json.loads(Path(path).read_bytes())


def identity(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), path
    stat = path.stat()
    with path.open('rb') as stream:
        sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) == (
        stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns), path
    return dict(path=str(path), generation=dict(device=stat.st_dev, inode=stat.st_ino, bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns, ctime_ns=stat.st_ctime_ns), sha256=sha)


def shape(record):
    return dict(bytes=record.get('bytes', record.get('generation', {}).get('bytes')), sha256=record['sha256'])


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', required=True)
    args = parser.parse_args()
    assert args.revision.startswith('r') and args.revision[1:].isdigit()
    destination = OUT / ('variable-copy-packet-' + args.revision)
    assert not destination.exists() and str(destination).startswith('/Users/dylan/LocalData/')
    assert shutil.disk_usage(OUT).free >= 14 * GIB
    protocol_path = OUT / 'variable-copy-protocol-r1.json'
    protocol = read(protocol_path)
    assert protocol['case_count'] == 403 and protocol['reused_baseline_count'] == 0
    replay_path, disposition_path = OUT / 'variable-copy-replay-r1.json', OUT / 'variable-copy-disposition-r1.json'
    replay, decision = read(replay_path), read(disposition_path)
    assert replay['decision'] == decision['performance_decision'] == 'retain'
    assert replay['fresh']['complete'] and replay['fresh_packets_replayed'] == 806
    manifests = {r: read(protocol['runtimes'][r]['manifest']['path']) for r in ('baseline', 'candidate')}
    builds = {r: read(m['build_receipt']['path']) for r, m in manifests.items()}
    members, origins, external, external_reasons = {}, {}, {}, {}
    for path in protocol['actual_source_paths']:
        external_reasons[path] = 'input_payload_identity_retained_complete_results_and_independent_references_archived'
    for m in manifests.values():
        external_reasons.update({p: 'native_runtime_identity_retained_payload_excluded' for p in m['files']})
        external_reasons[m['source_archive']['path']] = 'source_archive_identity_retained_all_source_members_archived'
    for path in [*protocol['consumer_environment']['inherited_files'], protocol['consumer_environment']['resolved_executable']]:
        external_reasons[path] = 'execution_environment_identity_retained_not_used_by_portable_replay'
    diagnostic = {name: read(OUT / name / 'protocol.json') for name in ('diagnostic-r1', 'diagnostic-r2', 'gather-work-r1')}
    for p in diagnostic.values():
        external_reasons[p['binary']['path']] = 'native_runtime_identity_retained_payload_excluded'
    installed = OUT / 'installed-candidate-wheel-r1'
    wheel = read(installed / 'report.json')['integrity']
    wheel_path = installed / 'dist' / wheel['wheel']
    external_reasons[str(wheel_path)] = 'installed_wheel_identity_retained_installation_reports_archived'

    def add(path, expected=None):
        path = Path(path)
        origin = str(path)
        if origin in origins or origin in external:
            actual = members[origins[origin]]['identity'] if origin in origins else external[origin]['identity']
        elif origin in external_reasons and expected is not None and 'generation' in expected:
            actual = {k: expected[k] for k in ('path', 'generation', 'sha256')}
            external[origin] = dict(identity=actual, reason=external_reasons[origin], payload_reopened_by_packager=False)
        else:
            actual = identity(path)
            if origin in external_reasons:
                external[origin] = dict(identity=actual, reason=external_reasons[origin], payload_reopened_by_packager=True)
            else:
                name = ('data/' + path.relative_to(OUT.parent).as_posix() if path.is_relative_to(OUT.parent)
                        else 'extra/' + hashlib.sha256(origin.encode()).hexdigest() + '/' + path.name)
                assert name not in members
                members[name] = dict(origin=origin, identity=actual)
                origins[origin] = name
        if expected is not None:
            assert shape(actual) == shape(expected), origin
            if 'generation' in expected:
                assert actual == {k: expected[k] for k in ('path', 'generation', 'sha256')}, origin
        return origin

    for row in protocol['frozen_files']:
        add(row['path'], row)
    for m in manifests.values():
        for path, expected in m['files'].items():
            add(path, expected)
        build = Path(m['build_receipt']['path'])
        add(build)
        add(build.parent / 'logs' / (build.stem + '.log'))
    assert shape(identity(wheel_path)) == dict(bytes=wheel['wheel_bytes'], sha256=wheel['wheel_sha256'])
    add(wheel_path)
    snapshots = {'candidate': {name: str(ROOT / name) for name in builds['candidate']['source_files']}}
    # Every archived source member is verified, including members reused by both
    # runtimes. Only changed baseline members require a second stored copy.
    archive_verification = {}
    preserved = OUT / 'portable-baseline-source-r1'
    for role in ('baseline', 'candidate'):
        m, expected = manifests[role], builds[role]['source_files']
        assert shape(identity(m['source_archive']['path'])) == shape(m['source_archive'])
        seen, mapping = set(), {}
        with tarfile.open(m['source_archive']['path'], 'r:gz') as archive:
            for entry in archive:
                assert entry.isfile() and not entry.issym() and not entry.islnk()
                name = entry.name
                assert name in expected and name not in seen and '..' not in Path(name).parts
                seen.add(name)
                raw = archive.extractfile(entry).read()
                assert dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()) == expected[name]
                if builds['candidate']['source_files'].get(name) == expected[name]:
                    origin = ROOT / name
                else:
                    assert role == 'baseline'
                    origin = preserved / name
                    if not origin.exists():
                        origin.parent.mkdir(parents=True, exist_ok=True)
                        with origin.open('xb') as stream:
                            stream.write(raw)
                mapping[name] = add(origin, expected[name])
        assert seen == set(expected)
        snapshots[role] = mapping
        archive_verification[role] = dict(members=len(seen), source_fingerprint=fingerprint(expected),
                                          archive=m['source_archive'], all_members_content_verified=True)
    # All small task-owned records, logs, helpers and failure evidence are kept.
    skip_prefixes = ('continuation', 'variable-copy-package-', 'variable-copy-portable-', 'portable-replay-')
    for path in sorted(OUT.iterdir()):
        if path.is_file() and not path.name.startswith(skip_prefixes) and path.suffix in {'.py', '.json', '.md', '.log', '.patch', '.txt', '.gz', '.rs'}:
            add(path)
    for directory in ('postprocessing', 'logs', 'jobs', 'r1-source-preservation', 'diagnostic-r1', 'diagnostic-r2',
                      'gather-work-r1', 'variable-copy-baseline-r1', 'variable-copy-candidate-r1'):
        for path in sorted((OUT / directory).rglob('*')):
            if path.is_file() and path.suffix in {'.py', '.json', '.md', '.log', '.txt', '.gz', '.rs', '.sqlite'}:
                add(path)
    for path in sorted(installed.iterdir()):
        if path.is_file() and path.suffix in {'.log', '.json'}:
            add(path)
    catalog = defaultdict(dict)
    for origin, member in origins.items():
        catalog[(members[member]['identity']['generation']['bytes'], members[member]['identity']['sha256'])][origin] = True
    diagnostic_bindings = {}
    for label, p in diagnostic.items():
        bindings = {}
        for record in p['files']:
            origin = record['path']
            wanted = shape(record)
            if origin in external_reasons:
                add(origin, record)
            elif origin in origins and shape(members[origins[origin]]['identity']) == wanted:
                # Same bytes may have a later generation after a deliberate restore.
                if members[origins[origin]]['identity'] != record:
                    bindings[origin] = origin
            else:
                matches = catalog[(wanted['bytes'], wanted['sha256'])]
                if matches:
                    bindings[origin] = sorted(matches)[0]
                else:
                    add(origin, record)
        diagnostic_bindings[label] = bindings
        build = read(p['build_receipt']['path'])
        mapping = {}
        for name, wanted in build['source_files'].items():
            current = str(ROOT / name)
            if current in origins and shape(members[origins[current]]['identity']) == wanted:
                mapping[name] = current
            else:
                matches = catalog[(wanted['bytes'], wanted['sha256'])]
                assert matches, ('unpreserved historical source', label, name, wanted)
                mapping[name] = sorted(matches)[0]
        snapshots[label] = mapping
    assert set(row['path'] for row in protocol['frozen_files']) <= set(origins) | set(external)
    unpacked = sum(row['identity']['generation']['bytes'] for row in members.values())
    assert unpacked <= 10 * GIB and shutil.disk_usage(OUT).free >= 12 * GIB + min(2 * GIB, unpacked + 64 * MIB)
    destination.mkdir()
    joined = destination / 'evidence.tar.gz'
    with joined.open('xb') as target, gzip.GzipFile(fileobj=target, mode='wb', filename='', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as archive:
            for name, row in sorted(members.items()):
                source = Path(row['origin'])
                assert identity(source) == row['identity'], source
                entry = tarfile.TarInfo(name)
                entry.size, entry.mode, entry.mtime = row['identity']['generation']['bytes'], 0o644, 0
                with source.open('rb') as stream:
                    archive.addfile(entry, stream)
    archive_identity = shape(identity(joined))
    assert archive_identity['bytes'] <= 2 * GIB
    parts, digest, count = [], hashlib.sha256(), 0
    with joined.open('rb') as source:
        while raw := source.read(48 * MIB):
            name = 'evidence.tar.gz.part-' + str(len(parts) + 1).zfill(3)
            with (destination / name).open('xb') as target:
                target.write(raw)
            parts.append(name)
            digest.update(raw)
            count += len(raw)
    assert dict(bytes=count, sha256=digest.hexdigest()) == archive_identity
    joined.unlink()
    for source, target in [('portable_variable_copy.py', 'verify.py'),
                           ('propagation_portable_store.py', 'propagation_portable_store.py'),
                           ('variable_copy_portable_store.py', 'variable_copy_portable_store.py')]:
        shutil.copyfile(OUT / 'postprocessing' / source, destination / target)
    (destination / 'README.md').write_text('''# Resident join attribution and variable result copying

Run `python3 -I -B verify.py` with Python 3.13 or later and all packet files together.
The archive includes complete raw results and independent references for 403 fresh
requests per runtime, complete source bytes for both runtimes and all diagnostic
source variants, check logs, preserved failures, and the unchanged frozen protocol.
Replay forbids original-path reads, execution-library imports, processes and network.
It executes zero queries and tests deliberately corrupted stored results.

Decision: retain the candidate under the frozen complete-cohort rule. Engine time
is 60.778284678 -> 60.597904154 seconds; all 186 individual regressions are retained.
One observation per request/runtime does not establish repeatability or per-change
causation. These 403 requests are not the full corpus or all join workloads.

The 54 corrected instrumented diagnostics and three debug gather observations are
replayed separately; the earlier failed one-ULP transport attempt remains failed.
Native binaries, input payloads, execution environments, installed wheel and source
archive containers are identity-only exclusions; their source members, complete
results and proof records are archived. Replaying evidence does not rebuild or
rerun a native runtime. Local acceptance does not imply a merge or release.
''')
    files = {p.name: shape(identity(p)) for p in destination.iterdir() if p.is_file()}
    manifest = dict(schema='shardloom.variable_copy_portable.v1', out=str(OUT), roots=dict(candidate=str(ROOT)),
        archive=dict(format='concatenated_gzip_tar', parts=parts, **archive_identity), files=files,
        archive_members=members, external_artifacts=external, source_snapshots=snapshots,
        source_archive_verification=archive_verification, diagnostic_bindings=diagnostic_bindings,
        protocol=identity(protocol_path), replay=identity(replay_path), disposition=identity(disposition_path),
        query_executions_during_packaging=0, publication_attempted=False, complete_raw_ledgers_retained=True,
        original_references_retained=True, declared_fresh_results_per_runtime=403, reused_baseline_observations=0,
        unpacked_bytes=unpacked, packager=identity(__file__))
    with (destination / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps(dict(status='packaged', packet=str(destination), members=len(members),
        archive_bytes=archive_identity['bytes'], unpacked_bytes=unpacked, source_members={k: len(v) for k, v in snapshots.items()},
        external_artifacts=len(external), query_executions=0)), flush=True)


if __name__ == '__main__':
    main()
