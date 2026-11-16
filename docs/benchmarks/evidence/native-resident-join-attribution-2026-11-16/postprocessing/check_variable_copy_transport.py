"""Test the exact packet transport against malformed archives and manifests."""
import argparse
from copy import deepcopy
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile


def content(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def load(name, path, expected):
    assert content(path.read_bytes()) == expected
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


def make(root, *, name='data/member', kind='file', duplicate=False, second=False):
    packet, extracted = root / 'packet', root / 'extracted'
    packet.mkdir(); extracted.mkdir()
    raw = b'complete stored evidence\n'
    identity = dict(path='/original/member', generation=dict(bytes=len(raw)), sha256=content(raw)['sha256'])
    members = {name: dict(origin='/original/member', identity=identity)}
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as archive:
            entry = tarfile.TarInfo(name)
            entry.size = len(raw)
            if kind != 'file':
                entry.size = 0
                entry.type = tarfile.SYMTYPE if kind == 'symlink' else tarfile.LNKTYPE
                entry.linkname = '/outside'
            archive.addfile(entry, io.BytesIO(raw) if kind == 'file' else None)
            if duplicate:
                archive.addfile(entry, io.BytesIO(raw))
            if second:
                other = tarfile.TarInfo('data/second'); other.size = len(raw)
                archive.addfile(other, io.BytesIO(raw))
                members['data/second'] = deepcopy(members[name])
    compressed = buffer.getvalue()
    part = 'evidence.tar.gz.part-001'
    (packet / part).write_bytes(compressed)
    manifest = dict(schema='shardloom.variable_copy_portable.v1', query_executions_during_packaging=0,
        publication_attempted=False, complete_raw_ledgers_retained=True, original_references_retained=True,
        declared_fresh_results_per_runtime=403, reused_baseline_observations=0,
        files={part: content(compressed)}, archive_members=members, external_artifacts={},
        archive=dict(format='concatenated_gzip_tar', parts=[part], **content(compressed)))
    return packet, extracted, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    real = json.loads((args.packet / 'manifest.json').read_bytes())
    load('propagation_portable_store', args.packet / 'propagation_portable_store.py', real['files']['propagation_portable_store.py'])
    transport = load('variable_copy_portable_store', args.packet / 'variable_copy_portable_store.py', real['files']['variable_copy_portable_store.py'])
    results = []
    cases = ['valid', 'wrong_schema', 'wrong_count', 'reused_observation', 'part_tamper', 'missing_part',
             'unlisted_file', 'packet_symlink', 'unsafe_part', 'wrong_archive_hash', 'wrong_member_hash',
             'wrong_member_size', 'traversal_member', 'absolute_member', 'symlink_member', 'hardlink_member',
             'duplicate_archive_member', 'duplicate_origin', 'overlapping_external', 'unlisted_archive_member']
    for case in cases:
        with tempfile.TemporaryDirectory(prefix='variable-copy-transport-') as directory:
            root = Path(directory)
            kwargs = {}
            if case == 'traversal_member': kwargs['name'] = '../escape'
            if case == 'absolute_member': kwargs['name'] = '/absolute'
            if case == 'symlink_member': kwargs['kind'] = 'symlink'
            if case == 'hardlink_member': kwargs['kind'] = 'hardlink'
            if case == 'duplicate_archive_member': kwargs['duplicate'] = True
            if case == 'duplicate_origin': kwargs['second'] = True
            packet, extracted, manifest = make(root, **kwargs)
            part = 'evidence.tar.gz.part-001'
            member = next(iter(manifest['archive_members']))
            if case == 'wrong_schema': manifest['schema'] = 'wrong'
            if case == 'wrong_count': manifest['declared_fresh_results_per_runtime'] = 402
            if case == 'reused_observation': manifest['reused_baseline_observations'] = 1
            if case == 'part_tamper': (packet / part).write_bytes(b'changed')
            if case == 'missing_part': (packet / part).unlink()
            if case == 'unlisted_file': (packet / 'unexpected').write_text('unexpected')
            if case == 'packet_symlink': (packet / 'link').symlink_to(packet / part)
            if case == 'unsafe_part': manifest['archive']['parts'] = ['../escape']
            if case == 'wrong_archive_hash': manifest['archive']['sha256'] = '0' * 64
            if case == 'wrong_member_hash': manifest['archive_members'][member]['identity']['sha256'] = '0' * 64
            if case == 'wrong_member_size': manifest['archive_members'][member]['identity']['generation']['bytes'] += 1
            if case == 'overlapping_external':
                row = manifest['archive_members'][member]
                manifest['external_artifacts'][row['origin']] = dict(identity=row['identity'], reason='native_runtime_identity_retained_payload_excluded')
            if case == 'unlisted_archive_member': manifest['archive_members'] = {}
            (packet / 'manifest.json').write_text(json.dumps(manifest))
            try:
                value = transport.Store(packet, extracted)
                assert value.raw('/original/member') == b'complete stored evidence\n'
            except (AssertionError, KeyError, ValueError, OSError, tarfile.TarError) as error:
                assert case != 'valid', (case, error)
                results.append(dict(control=case, status='rejected', error=type(error).__name__))
            else:
                assert case == 'valid', 'invalid archive accepted: ' + case
                results.append(dict(control=case, status='accepted_valid_control'))
    report = dict(status='passed', controls=results, rejected_count=19, valid_controls=1,
        query_executions=0, packet_manifest=content((args.packet / 'manifest.json').read_bytes()),
        verifier=content(Path(__file__).read_bytes()), transport=real['files']['variable_copy_portable_store.py'],
        primitive=real['files']['propagation_portable_store.py'])
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(status='passed', rejected_count=19, valid_controls=1, query_executions=0)))


if __name__ == '__main__':
    main()
