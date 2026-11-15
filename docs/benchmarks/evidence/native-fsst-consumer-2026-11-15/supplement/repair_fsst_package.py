"""Complete offline helper imports while preserving every original archive byte."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

OUT = Path(__file__).resolve().parent


def identity(path):
    path = Path(path)
    before = path.stat()
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    stable = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    assert stable(before) == stable(after)
    return dict(path=str(path), generation=dict(device=before.st_dev, inode=before.st_ino,
        bytes=before.st_size, mtime_ns=before.st_mtime_ns, ctime_ns=before.st_ctime_ns), sha256=digest)


def shape(record):
    return dict(bytes=record.get('bytes', record.get('generation', {}).get('bytes')), sha256=record['sha256'])


def read(path):
    return json.loads(Path(path).read_bytes())


def main():
    original, destination = OUT / 'fsst-packet-r1', OUT / 'fsst-packet-r2'
    assert __debug__ and not destination.exists()
    assert shutil.disk_usage(OUT).free >= (12 << 30) + (128 << 20)
    old = read(original / 'manifest.json')
    before = {path.name: shape(identity(path)) for path in original.iterdir() if path.is_file()}
    assert old['files'] == {name: value for name, value in before.items() if name != 'manifest.json'}
    failure_path = OUT / 'fsst-packet-relocated-r1.json'
    failure = read(failure_path)
    assert failure['returncode'] == 1 and failure['original_paths_forbidden']
    assert "No module named 'source_facts_observations'" in failure['stderr']
    failed_guard = read(OUT / 'fsst-packet-controls-check-r1.json')
    assert failed_guard['status'] == 'failed' and failed_guard['sources_unchanged'] and failed_guard['process_group_drained']
    protocol = read(old['protocol']['path'])
    assert identity(old['protocol']['path']) == old['protocol']
    previous = read(protocol['original_baseline_protocol']['path'])
    assert identity(protocol['original_baseline_protocol']['path']) == protocol['original_baseline_protocol']
    frozen = {row['path']: row for row in previous['frozen_files']}
    rows = {row['origin']: dict(row, archive_name=name) for name, row in old['archive_members'].items()}
    additions, missing = {}, {}

    def add(path, expected=None):
        path = Path(path)
        name = str(path)
        actual = identity(path)
        if expected is not None:
            assert actual == expected
        if name in rows:
            assert actual == rows[name]['identity']
            return
        assert name not in old['external_artifacts']
        archive_name = ('data/' + path.relative_to(OUT.parent).as_posix() if path.is_relative_to(OUT.parent)
            else 'extra/' + hashlib.sha256(name.encode()).hexdigest() + '/' + path.name)
        row = dict(origin=name, identity=actual, archive_name=archive_name)
        additions[name] = rows[name] = row

    root = Path(old['worktree'])
    for dirname, prefix, module in (
        ('native-source-domain-facts-2026-11-14', 'source-facts', 'source_facts_observations.py'),
        ('native-dictionary-domain-2026-11-14', 'dictionary', 'dictionary_observations.py')):
        directory = OUT.parent / dirname
        closeout_path = directory / (prefix + '-closeout-r1.json')
        add(closeout_path, frozen[str(closeout_path)])
        closeout = read(closeout_path)
        controls_path = directory / (prefix + '-packet-controls-r1.json')
        controls = read(controls_path)
        assert shape(identity(controls_path)) == closeout['inputs'][controls_path.name]
        assert controls['status'] == 'passed'
        add(controls_path)
        prior_path = root / 'docs/benchmarks/evidence' / dirname / 'packet/manifest.json'
        assert shape(identity(prior_path)) == controls['manifest']
        add(prior_path)
        prior = read(prior_path)
        module_path = directory / module
        (record,) = (row['identity'] for row in prior['archive_members'].values() if row['origin'] == str(module_path))
        assert str(module_path) not in rows
        add(module_path, record)
        missing[str(module_path)] = record
    for name in ('portable_fsst_v2.py', 'test_fsst_packet_v2.py', 'repair_fsst_package.py'):
        add(OUT / name)
    python_rows = {origin: row for origin, row in rows.items() if origin.endswith('.py')}
    with tempfile.TemporaryDirectory(prefix='fsst-repair-imports-') as temporary:
        directory = Path(temporary)
        paths = {}
        for origin, row in python_rows.items():
            assert identity(origin) == row['identity'], origin
            target = directory / row['archive_name']
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(origin, target)
            assert shape(identity(target)) == shape(row['identity'])
            paths[origin] = str(target)
        (directory / 'mapping.json').write_text(json.dumps(dict(manifest=old, paths=paths)))
        runner = '''import importlib.util,json,os,sys,hashlib
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
origin=str(Path(payload['manifest']['out'])/'portable_fsst_v2.py')
spec=importlib.util.spec_from_file_location('fsst_offline_import',payload['paths'][origin])
verifier=importlib.util.module_from_spec(spec);sys.modules[spec.name]=verifier;spec.loader.exec_module(verifier)
class Store:
 manifest=payload['manifest']
 def path(self,path):return Path(payload['paths'][str(path)])
 def read(self,path):raise RuntimeError('data read outside import check')
 def identity(self,path):raise RuntimeError('identity read outside import check')
 def verify(self,*args,**kwargs):raise RuntimeError('verification outside import check')
 def closing_db(self,*args,**kwargs):raise RuntimeError('database outside import check')
verifier.bind(Store())
reverse={str(Path(value).resolve()):key for key,value in payload['paths'].items()}
loaded=[]
for name,module in sorted(sys.modules.items()):
 path=getattr(module,'__file__',None)
 if path and str(Path(path).resolve()) in reverse:
  data=Path(path).read_bytes()
  loaded.append(dict(module=name,origin=reverse[str(Path(path).resolve())],identity=dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())))
assert {'dictionary_observations','source_facts_observations','fsst_replay'}<={row['module'] for row in loaded}
assert all(not str(getattr(module,'__file__','')).startswith(blocked) for module in sys.modules.values())
print(json.dumps(dict(status='passed_relocated_actual_imports',loaded=loaded,original_reads_forbidden=True,network_and_subprocess_forbidden=True,query_executions=0)))
'''
        result = subprocess.run([sys.executable, '-I', '-B', '-c', runner, str(directory)],
            capture_output=True, text=True, timeout=120, cwd=directory)
        with (OUT / 'fsst-import-closure-execution-r1.json').open('x') as stream:
            json.dump(dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr,
                original_paths_forbidden=True, queries_executed=0), stream, indent=2); stream.write('\n')
        assert result.returncode == 0, result.stderr
        closure = json.loads(result.stdout)
        closure.update(relocated_python_sources=len(python_rows), helper=shape(identity(Path(__file__))))
        with (OUT / 'fsst-import-closure-r1.json').open('x') as stream:
            json.dump(closure, stream, indent=2); stream.write('\n')
    for name in ('fsst-import-closure-r1.json', 'fsst-import-closure-execution-r1.json',
        'fsst-packet-relocated-r1.json', 'fsst-packet-controls-check-r1.json', 'fsst-package-check-r2.json'):
        add(OUT / name)
    for name in ('fsst-packet-controls-check-r1.log', 'fsst-package-check-r2.log'):
        add(OUT / 'logs' / name)
    for name in ('manifest.json', 'verify.py', 'README.md'):
        add(original / name)
    destination.mkdir()
    for name in old['files']:
        target = {'verify.py': 'original-verify.py', 'README.md': 'original-README.md'}.get(name, name)
        os.link(original / name, destination / target)
        assert shape(identity(destination / target)) == old['files'][name]
    supplemental = []
    for index, row in enumerate(additions.values()):
        # Linking the original transport can change only its ctime; its content
        # stays byte-identical and the recorded historical identity remains valid.
        source = Path(row['origin'])
        assert shape(identity(source)) == shape(row['identity'])
        transport = 'supplement-' + str(index + 1).zfill(3) + '-' + source.name
        shutil.copyfile(source, destination / transport)
        assert shape(identity(destination / transport)) == shape(row['identity'])
        supplemental.append(dict(row, transport=transport))
    shutil.copyfile(OUT / 'portable_fsst_v2.py', destination / 'verify.py')
    (destination / 'README.md').write_text('''# Complete FSST offline helper imports

Run `python3 -I -B verify.py` with Python 3.13 or newer. The original relocated
import failure is preserved. Two shared observation validators are supplied from
their byte-exact, previously verified dictionary and source-domain packets. Their
closeouts, controls and manifests are bound through the original baseline protocol.
All original archive parts, observations, references, clocks and acceptance rules
are unchanged. No query was rerun. The previous explanation follows.

''' + (original / 'README.md').read_text())
    manifest = dict(old)
    manifest.update(schema='shardloom.native_fsst_prefilter_portable.v2', supplemental_members=supplemental,
        archive_repair=dict(original_packet_manifest=identity(original / 'manifest.json'),
            failure_record=identity(failure_path), missing_shared_modules=missing,
            import_closure=shape(identity(OUT / 'fsst-import-closure-r1.json')),
            original_archive_bytes_unchanged=True, original_protocol_results_and_acceptance_unchanged=True,
            query_executions=0, helper=identity(Path(__file__))))
    manifest['files'] = {path.name: shape(identity(path)) for path in sorted(destination.iterdir()) if path.is_file()}
    with (destination / 'manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2); stream.write('\n')
    assert before == {path.name: shape(identity(path)) for path in original.iterdir() if path.is_file()}
    record = dict(status='completed_offline_import_repair_pending_complete_replay',
        original_packet=identity(original / 'manifest.json'), corrected_packet=identity(destination / 'manifest.json'),
        missing_shared_modules=missing, original_archive_bytes_unchanged=True, added_members=len(supplemental),
        actual_relocated_imports=len(closure['loaded']), query_executions=0)
    with (OUT / 'fsst-package-repair-r1.json').open('x') as stream:
        json.dump(record, stream, indent=2); stream.write('\n')
    print(json.dumps({key: record[key] for key in ('status', 'added_members', 'actual_relocated_imports', 'query_executions')}))


if __name__ == '__main__':
    main()
