"""Strict delta transport over the independently verified original evidence packet."""
import hashlib
import json
from pathlib import Path
import shutil
import tarfile

from propagation_portable_store import (
    Store as ParentStore, archive_stream, content, safe_name, shape,
)


class Store(ParentStore):
    def __init__(self, packet, directory, parent):
        self.packet, self.directory, self.parent = packet, directory, parent
        self.manifest = json.loads((packet / 'manifest.json').read_bytes())
        manifest = self.manifest
        assert manifest['schema'] == 'shardloom.selected_dictionary_portable.v1'
        assert manifest['query_executions_during_packaging'] == 0
        assert not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 2190
        assert manifest['declared_lifecycle_operations_per_runtime'] == 540
        assert content(parent.packet / 'manifest.json') == manifest['baseline_packet_manifest']
        assert not any(path.is_symlink() for path in packet.rglob('*'))
        assert {path.name for path in packet.iterdir() if path.is_file()} == set(manifest['files']) | {'manifest.json'}
        for name, expected in manifest['files'].items():
            safe_name(name)
            assert content(packet / name) == expected, name
        self.members = dict(manifest['archive_members'])
        for name, row in self.members.items():
            safe_name(name)
            assert row['identity']['path'] == row['origin'], name
            size = row['identity']['generation']['bytes']
            assert type(size) is int and size >= 0, name
        self.local_origins = {row['origin']: name for name, row in self.members.items()}
        self.inherited = manifest['inherited_members']
        self.transports = manifest['inherited_transport_files']
        self.external = manifest['external_artifacts']
        groups = [set(self.local_origins), set(self.inherited), set(self.transports), set(self.external)]
        assert len(self.local_origins) == len(self.members)
        assert sum(map(len, groups)) == len(set().union(*groups)), 'ambiguous stored origin'
        self.origins = dict(self.local_origins)
        self.origins.update({origin: row['parent_origin'] for origin, row in self.inherited.items()})
        self.origins.update({origin: row['parent_file'] for origin, row in self.transports.items()})
        for origin, row in self.inherited.items():
            assert row['identity']['path'] == origin
            assert row['parent_origin'] in parent.origins
            assert shape(parent.identity(row['parent_origin'])) == shape(row['identity']), origin
        for origin, row in self.transports.items():
            assert row['identity']['path'] == origin
            safe_name(row['parent_file'])
            assert row['parent_file'] in set(parent.manifest['files']) | {'manifest.json'}
            assert content(parent.packet / row['parent_file']) == shape(row['identity']), origin
        for origin, row in self.external.items():
            assert row['identity']['path'] == origin
            assert row['reason'] in {
                'input_payload_identity_retained_full_results_and_independent_references_archived',
                'native_runtime_identity_retained_payload_excluded',
                'execution_environment_identity_retained_not_used_by_portable_replay',
                'candidate_source_archive_identity_retained_all_source_members_archived',
            }
        with archive_stream(packet, manifest, directory) as compressed, tarfile.open(fileobj=compressed, mode='r:gz') as archive:
            entries = archive.getmembers()
            assert len(entries) == len({entry.name for entry in entries}) == len(self.members)
            assert {entry.name for entry in entries} == set(self.members)
            unpacked = sum(entry.size for entry in entries)
            assert unpacked <= 10 << 30
            assert shutil.disk_usage(directory).free >= (12 << 30) + unpacked
            for entry in entries:
                safe_name(entry.name)
                assert entry.isfile() and not entry.issym() and not entry.islnk()
                wanted = self.members[entry.name]['identity']
                assert entry.size == wanted['generation']['bytes']
                target = directory / entry.name
                target.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with archive.extractfile(entry) as source, target.open('xb') as destination:
                    while block := source.read(1 << 20):
                        digest.update(block); destination.write(block)
                assert digest.hexdigest() == wanted['sha256'], entry.name

    def path(self, path):
        origin = str(path)
        if origin in self.local_origins:
            return self.directory / self.local_origins[origin]
        if origin in self.inherited:
            return self.parent.path(self.inherited[origin]['parent_origin'])
        if origin in self.transports:
            return self.parent.packet / self.transports[origin]['parent_file']
        raise AssertionError('portable replay tried an unbundled file: ' + origin)

    def identity(self, path):
        origin = str(path)
        if origin in self.local_origins:
            record = self.members[self.local_origins[origin]]['identity']
        elif origin in self.inherited:
            record = self.inherited[origin]['identity']
        elif origin in self.transports:
            record = self.transports[origin]['identity']
        else:
            raise AssertionError('portable replay tried an unbundled identity: ' + origin)
        assert content(self.path(origin)) == shape(record), origin
        return record
