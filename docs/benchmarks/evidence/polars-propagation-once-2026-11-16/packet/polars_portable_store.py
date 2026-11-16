"""Strict archive access for the one-pass external baseline observation."""
import hashlib
import json
from pathlib import Path
import shutil
import tarfile

from propagation_portable_store import Store as BaseStore, archive_stream, content, safe_name, shape


class Store(BaseStore):
    def __init__(self, packet, directory):
        packet, directory = packet.resolve(), directory.resolve()
        self.packet, self.directory = packet, directory
        self.manifest = json.loads((packet/'manifest.json').read_bytes())
        m = self.manifest
        assert m['schema'] == 'shardloom.polars_propagation_once_portable.v1'
        assert m['query_executions_during_packaging'] == 0 and not m['publication_attempted']
        assert m['complete_raw_ledgers_retained'] and m['original_references_retained']
        assert m['declared_polars_attempts'] == m['historical_native_observations'] == 11579
        assert m['polars_outcomes'] == {'passed':11489,'incorrect':90}
        assert m['complete_competitive_baseline'] is False
        assert not any(path.is_symlink() for path in packet.rglob('*'))
        assert {path.name for path in packet.iterdir() if path.is_file()} == set(m['files']) | {'manifest.json'}
        for name,wanted in m['files'].items():
            safe_name(name)
            assert content(packet/name) == wanted, name
        self.members = dict(m['archive_members'])
        self.origins = {row['origin']:name for name,row in self.members.items()}
        self.external = m['external_artifacts']
        assert len(self.members) == len(self.origins)
        assert not set(self.origins) & set(self.external)
        for name,row in self.members.items():
            safe_name(name)
            assert row['origin'] == row['identity']['path']
            assert type(row['identity']['generation']['bytes']) is int and row['identity']['generation']['bytes'] >= 0
        for origin,row in self.external.items():
            assert row['identity']['path'] == origin
            assert row['reason'] in {'input_payload_identity_only_independent_references_and_readback_proofs_retained',
                'execution_environment_identity_only_never_loaded_by_portable_replay'}
        with archive_stream(packet,m,directory) as compressed, tarfile.open(fileobj=compressed,mode='r:gz') as archive:
            entries = archive.getmembers()
            assert len(entries) == len({e.name for e in entries}) == len(self.members)
            assert {e.name for e in entries} == set(self.members)
            unpacked = sum(e.size for e in entries)
            assert unpacked <= 3 << 30
            assert shutil.disk_usage(directory).free >= (12 << 30) + unpacked
            for entry in entries:
                safe_name(entry.name)
                assert entry.isfile() and not entry.issym() and not entry.islnk()
                wanted = self.members[entry.name]['identity']
                assert entry.size == wanted['generation']['bytes']
                target = directory/entry.name
                target.parent.mkdir(parents=True,exist_ok=True)
                digest = hashlib.sha256()
                with archive.extractfile(entry) as source,target.open('xb') as output:
                    while block:=source.read(1<<20):
                        digest.update(block);output.write(block)
                assert digest.hexdigest() == wanted['sha256'],entry.name
        self.reverse = {str(directory/name):origin for origin,name in self.origins.items()}

    def original(self,path):
        return self.reverse.get(str(path),str(path))

    def path(self,path):
        return super().path(self.original(path))

    def identity(self,path):
        return super().identity(self.original(path))

    def virtual_exists(self,path):
        origin = self.original(path)
        return origin in self.origins or any(name.startswith(origin+'/') for name in self.origins)

    def virtual_children(self,path):
        origin = self.original(path).rstrip('/')+'/'
        return sorted({origin+name[len(origin):].split('/')[0] for name in self.origins if name.startswith(origin)})
