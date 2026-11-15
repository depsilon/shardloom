"""Check the offline verifier's actual imports using relocated, identity-checked Python sources."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def identity(path):
    data = path.read_bytes()
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def shape(record):
    return dict(bytes=record['generation']['bytes'], sha256=record['sha256'])


def main():
    out = Path(__file__).resolve().parent
    packet = out / 'text-packet-r2'
    manifest = json.loads((packet / 'manifest.json').read_bytes())
    rows = {row['origin']: dict(row, archive_name=name) for name, row in manifest['archive_members'].items()}
    rows.update({row['origin']: row for row in manifest['supplemental_members']})
    n = out.parent / 'native-dictionary-domain-2026-11-14'
    missing = n / 'dictionary_observations.py'
    prior = json.loads((Path(manifest['worktree']) / 'docs/benchmarks/evidence/native-dictionary-domain-2026-11-14/packet/manifest.json').read_bytes())
    (record,) = (row['identity'] for row in prior['archive_members'].values() if row['origin'] == str(missing))
    assert str(missing) not in rows and identity(missing) == shape(record)
    rows[str(missing)] = dict(origin=str(missing), identity=record,
        archive_name='data/' + missing.relative_to(out.parent).as_posix())
    python_rows = {origin: row for origin, row in rows.items() if origin.endswith('.py')}
    with tempfile.TemporaryDirectory(prefix='text-offline-import-closure-') as temporary:
        temp = Path(temporary)
        mapping = {}
        for origin, row in python_rows.items():
            source = Path(origin)
            assert identity(source) == shape(row['identity']), origin
            destination = temp / row['archive_name']
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            assert identity(destination) == shape(row['identity'])
            mapping[origin] = str(destination)
        verifier = temp / 'portable.py'
        shutil.copyfile(out / 'portable_text_v3.py', verifier)
        (temp / 'mapping.json').write_text(json.dumps(dict(manifest=manifest, paths=mapping)))
        runner = '''import importlib.util,json,os,sys
from pathlib import Path
root=Path(sys.argv[1])
blocked=('/Users/dylan/LocalData/shardloom/','/Users/dylan/Documents/shardloom-local-repo/','/Users/dylan/.cache/shardloom/')
def audit(event,args):
 if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)) and os.path.abspath(os.fsdecode(os.fspath(args[0]))).startswith(blocked):
  raise RuntimeError('original source/runtime/evidence read forbidden')
 if event in ('socket.connect','subprocess.Popen','os.system'):
  raise RuntimeError('execution or network forbidden')
sys.addaudithook(audit)
payload=json.loads((root/'mapping.json').read_bytes())
spec=importlib.util.spec_from_file_location('relocated_text_verifier',root/'portable.py')
verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
class Store:
 manifest=payload['manifest']
 def path(self,path): return Path(payload['paths'][str(path)])
 def read(self,path): raise RuntimeError('data read is not part of import closure')
 def identity(self,path): raise RuntimeError('identity read is not part of import closure')
 def verify(self,*args,**kwargs): raise RuntimeError('verification is not part of import closure')
 def closing_db(self,*args,**kwargs): raise RuntimeError('database read is not part of import closure')
verifier.bind(Store())
# These are the only additional function-local imports in the two portable helpers.
import activation_test_inventory,text_read_evidence
reverse={str(Path(value).resolve()):key for key,value in payload['paths'].items()}
loaded=[]
for name,module in sorted(sys.modules.items()):
 path=getattr(module,'__file__',None)
 if path and str(Path(path).resolve()) in reverse:
  loaded.append(dict(module=name,origin=reverse[str(Path(path).resolve())]))
assert any(row['module']=='dictionary_observations' for row in loaded)
assert any(row['module']=='source_facts_observations' for row in loaded)
assert any(row['module']=='text_read_evidence' for row in loaded)
assert all(not str(getattr(module,'__file__','')).startswith(blocked) for module in sys.modules.values())
print(json.dumps(dict(status='passed_relocated_actual_imports',loaded=loaded,original_reads_forbidden=True,query_executions=0)))
'''
        result = subprocess.run([sys.executable, '-I', '-B', '-c', runner, str(temp)],
            text=True, capture_output=True, cwd=temp, timeout=120)
        record_path = out / 'text-import-closure-execution-r1.json'
        with record_path.open('x') as stream:
            json.dump(dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr,
                original_paths_forbidden=True, helper=identity(Path(__file__))), stream, indent=2)
            stream.write('\n')
        assert result.returncode == 0, result.stderr
        checked = json.loads(result.stdout)
    inventory = dict(status='passed_primary_relocated_import_closure',
        scope='Actual portable bind plus all additional function-local portable-helper imports; full semantic replay is a separate gate.',
        packet_manifest=identity(packet / 'manifest.json'), relocated_python_sources=len(python_rows),
        source_files={origin: shape(row['identity']) for origin,row in python_rows.items()},
        missing_replay_helpers=[str(missing)], loaded_modules=checked['loaded'],
        duplicate_import_origins_resolved_by_actual_relocated_imports=True,
        original_source_reads_forbidden=True, network_and_subprocesses_forbidden=True,
        execution=identity(record_path), helper=identity(Path(__file__)), query_executions=0,
        leaf_assignment='Primary takeover after incomplete static traversal; no leaf files were consumed.')
    destination = out / 'text-import-inventory-r1.json'
    with destination.open('x') as stream:
        json.dump(inventory, stream, indent=2)
        stream.write('\n')
    (out / 'text-import-inventory-r1.md').write_text(
        '# Relocated text verifier import closure\n\n'
        f'All {len(python_rows)} archived Python sources plus the missing dictionary helper were identity-checked and relocated. '
        f'The actual verifier bind and additional lazy imports loaded {len(checked["loaded"])} local modules with original reads, network and subprocesses forbidden.\n\n'
        '`text_replay_v3.py:10` imports `text_observations.py`; its line 5 imports the missing '
        '`dictionary_observations.py`. Its line 4 imports the already repaired `source_facts_observations.py`. '
        'The dictionary module is bound to the frozen accepted dictionary closeout through its controls and packet manifest. '
        'Duplicate archived module names were resolved by actual relocated imports; every loaded origin is recorded. '
        'Full result replay and data/provenance checks remain a separate required gate. No engine queries ran.\n')
    primary = dict(status='passed_primary_import_inventory_verification', inventory=identity(destination),
        missing_replay_helpers=[str(missing)], actual_relocated_imports_passed=True,
        loaded_local_modules=len(checked['loaded']), source_identities_verified=len(python_rows), query_executions=0)
    with (out / 'text-import-primary-r1.json').open('x') as stream:
        json.dump(primary, stream, indent=2)
        stream.write('\n')
    print(json.dumps(primary))


if __name__ == '__main__':
    main()
