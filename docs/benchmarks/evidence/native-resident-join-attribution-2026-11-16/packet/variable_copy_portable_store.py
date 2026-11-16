"""Strict standalone transport over the existing verified archive primitives."""
import hashlib
import json
from pathlib import Path
import shutil
import tarfile

from propagation_portable_store import Store as BaseStore, archive_stream, content, safe_name


class Store(BaseStore):
    def __init__(self, packet, directory):
        self.packet, self.directory = packet, directory
        self.manifest = json.loads((packet / 'manifest.json').read_bytes())
        manifest = self.manifest
        assert manifest['schema'] == 'shardloom.variable_copy_portable.v1'
        assert manifest['query_executions_during_packaging'] == 0
        assert not manifest['publication_attempted']
        assert manifest['complete_raw_ledgers_retained'] and manifest['original_references_retained']
        assert manifest['declared_fresh_results_per_runtime'] == 403
        assert manifest['reused_baseline_observations'] == 0
        assert not any(path.is_symlink() for path in packet.rglob('*'))
        assert {p.name for p in packet.iterdir() if p.is_file()} == set(manifest['files']) | {'manifest.json'}
        for name, wanted in manifest['files'].items():
            safe_name(name)
            assert content(packet / name) == wanted, name
        self.members = dict(manifest['archive_members'])
        self.origins = {row['origin']: name for name, row in self.members.items()}
        self.external = manifest['external_artifacts']
        assert len(self.members) == len(self.origins)
        assert not set(self.origins) & set(self.external)
        for name, row in self.members.items():
            safe_name(name)
            assert row['origin'] == row['identity']['path']
            assert type(row['identity']['generation']['bytes']) is int
            assert row['identity']['generation']['bytes'] >= 0
        for origin, row in self.external.items():
            assert row['identity']['path'] == origin
            assert row['reason'] in {
                'input_payload_identity_retained_complete_results_and_independent_references_archived',
                'native_runtime_identity_retained_payload_excluded',
                'execution_environment_identity_retained_not_used_by_portable_replay',
                'source_archive_identity_retained_all_source_members_archived',
                'installed_wheel_identity_retained_installation_reports_archived',
            }
        with archive_stream(packet, manifest, directory) as compressed:
            with tarfile.open(fileobj=compressed, mode='r:gz') as archive:
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
                    with archive.extractfile(entry) as source, target.open('xb') as output:
                        while block := source.read(1 << 20):
                            digest.update(block)
                            output.write(block)
                    assert digest.hexdigest() == wanted['sha256'], entry.name
