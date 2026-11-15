"""Independently replay the text-summary decline and completed archive checks."""
import hashlib
import importlib.util
import json
from pathlib import Path


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def verify_files(root, manifest):
    paths = list(root.rglob('*'))
    assert not any(path.is_symlink() for path in paths)
    assert {path.relative_to(root).as_posix() for path in paths if path.is_file()} == set(manifest['files']) | {'manifest.json'}
    for name, wanted in manifest['files'].items():
        relative = Path(name)
        assert not relative.is_absolute() and '..' not in relative.parts
        assert relative.as_posix() == name and '\\' not in name
        assert identity(root / relative) == wanted, name


def main():
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / 'manifest.json').read_bytes())
    assert manifest['schema'] == 'shardloom.native_text_signature_closeout.v1'
    assert manifest['verified_packet_moved_without_changes']
    assert manifest['query_executions'] == 0 and not manifest['publication_attempted']
    verify_files(root, manifest)
    read = lambda name: json.loads((root / 'supplement' / name).read_bytes())
    controls = read('text-packet-controls-r3.json')
    assert controls['status'] == 'passed_packet_corruption_controls'
    assert controls['count'] == len(controls['controls']) == 61
    assert len({row['name'] for row in controls['controls']}) == 61
    assert all(row['status'] == 'rejected' for row in controls['controls'])
    assert controls['original_source_runtime_evidence_reads_forbidden'] and controls['original_packet_unchanged']
    assert controls['query_executions'] == 0
    assert controls['manifest'] == identity(root / 'packet/manifest.json')
    assert controls['helper'] == identity(root / 'supplement/test_text_packet_v3.py')
    relocated = read('text-packet-relocated-r3.json')
    assert relocated['returncode'] == 0 and relocated['original_paths_forbidden']
    assert json.loads(relocated['stdout']) == controls['relocated_complete_replay']
    source = read('text-source-r1.json')
    assert source['status'] == 'preserved_exact_candidate_source'
    assert source['disposition'] == 'experimental_not_retained'
    assert source['parent'] == source['runtime_build_head'] == '0e6d15a658c060bd11e9a1001a71515f69f2064e'
    assert source['head'] == '0c4aee0b7e7c97f73598482d2b4b69b4b0f5c531'
    assert source['source_bytes_equal_to_executed_candidate'] and source['committed_blobs_verified'] == 5823
    assert len(source['files']) == 46
    assert not source['runtime_rebuilt_after_commit'] and not source['publication_attempted']
    disposition = read('text-disposition-r1.json')
    assert disposition['candidate_source'] == source
    assert disposition['disposition'] == 'drop' and disposition['status'] == 'complete_experiment_decline'
    assert disposition['complete_results_replayed'] == 18080
    assert disposition['complete_lifecycle_operations_replayed'] == 1080
    assert disposition['work_requirements_passed'] and not disposition['thresholds_changed']
    for name in ('text-source-preservation-r1.json', 'text-package-check-r1.json',
                 'text-package-repair-check-r1.json', 'text-package-repair-check-r2.json',
                 'text-packet-controls-check-r3.json'):
        receipt = read(name)
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['sources_unchanged'] and receipt['process_group_drained'] and receipt['direct_child_reaped']
    for version, module in ((1, 'source_facts_observations'), (2, 'dictionary_observations')):
        receipt = read(f'text-packet-controls-check-r{version}.json')
        assert receipt['status'] == 'failed' and receipt['returncode'] == 1
        assert receipt['sources_unchanged'] and receipt['process_group_drained'] and receipt['direct_child_reaped']
        failed = read(f'text-packet-relocated-r{version}.json')
        assert failed['returncode'] == 1 and failed['original_paths_forbidden']
        assert f"No module named '{module}'" in failed['stderr']
    spec = importlib.util.spec_from_file_location('text_portable_closeout', root / 'packet/verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    result = verifier.verify(root / 'packet')
    assert result == controls['relocated_complete_replay']
    assert result['disposition'] == disposition['disposition']
    assert result['source_fingerprint_sha256'] == source['source_fingerprint_sha256']
    assert result['fresh'] == disposition['fresh'] and result['lifecycle'] == disposition['lifecycle']
    print(json.dumps(dict(status='passed_complete_closeout_replay', query_executions=0,
        complete_results=18080, lifecycle_operations=1080, disposition='drop',
        saved_evidence_negative_controls=28, packet_negative_controls=61,
        source_commit=source['head'], source_fingerprint_sha256=source['source_fingerprint_sha256'],
        fresh=result['fresh'], lifecycle=result['lifecycle'])))


if __name__ == '__main__':
    main()
