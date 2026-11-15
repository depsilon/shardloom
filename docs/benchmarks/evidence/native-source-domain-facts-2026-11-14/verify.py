"""Replay the source-facts experiment and its completed portable controls."""
import hashlib
import importlib.util
import json
from pathlib import Path


def identity(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def verify_files(root, manifest):
    expected = manifest['files']
    paths = list(root.rglob('*'))
    assert not any(path.is_symlink() for path in paths)
    assert {path.relative_to(root).as_posix() for path in paths if path.is_file()} == set(expected) | {'manifest.json'}
    for name, wanted in expected.items():
        relative = Path(name)
        assert not relative.is_absolute() and '..' not in relative.parts
        assert relative.as_posix() == name and '\\' not in name
        assert identity(root / relative) == wanted, name


def main():
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / 'manifest.json').read_bytes())
    assert manifest['schema'] == 'shardloom.native_source_facts_closeout_packet.v1'
    assert manifest['original_packet_moved_without_changes']
    assert manifest['query_executions'] == 0 and not manifest['publication_attempted']
    verify_files(root, manifest)
    read = lambda name: json.loads((root / 'supplement' / name).read_bytes())
    controls = read('source-facts-packet-controls-r1.json')
    assert controls['status'] == 'passed' and controls['controls_rejected'] == len(controls['controls']) == 50
    assert len({row['name'] for row in controls['controls']}) == 50
    assert all(row['status'] == 'rejected' for row in controls['controls'])
    assert controls['original_source_and_runtime_reads_forbidden'] and controls['original_packet_unchanged']
    assert controls['query_executions'] == 0
    assert controls['manifest'] == identity(root / 'packet/manifest.json')
    assert controls['helper'] == identity(root / 'supplement/test_source_facts_packet.py')
    relocated = read('source-facts-packet-relocated-r1.json')
    assert relocated['returncode'] == 0 and relocated['original_paths_forbidden']
    assert json.loads(relocated['stdout']) == controls['relocated_replay']
    source = read('source-facts-source-r1.json')
    assert source['status'] == 'preserved_exact_candidate_source'
    assert source['disposition'] in {'retain', 'drop'}
    assert source['parent'] == source['runtime_build_head'] == 'e5a3c344037d05276664ea04a00a266014a9788d'
    assert source['source_bytes_equal_to_executed_candidate'] and source['committed_blobs_verified'] > 0
    assert len(source['files']) == 36
    assert not source['runtime_rebuilt_after_commit'] and not source['publication_attempted']
    disposition = read('source-facts-disposition-r1.json')
    assert disposition['candidate_source'] == source
    assert disposition['disposition'] == source['disposition']
    assert disposition['status'] == ('complete_experiment_acceptance' if source['disposition'] == 'retain'
                                     else 'complete_experiment_decline')
    assert disposition['complete_results_replayed'] == 5572
    assert disposition['complete_lifecycle_operations_replayed'] == 240
    assert disposition['work_requirements_passed']
    for name in ('source-facts-source-preservation-r1.json', 'source-facts-package-r1.json',
                 'source-facts-portable-verification-r1.json', 'source-facts-packet-control-verification-r1.json'):
        receipt = read(name)
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['sources_unchanged'] and receipt['process_group_drained'] and receipt['direct_child_reaped']
    spec = importlib.util.spec_from_file_location('source_facts_portable', root / 'packet/verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    result = verifier.verify(root / 'packet')
    assert result == controls['relocated_replay'] == read('source-facts-portable-replay-r1.json')
    assert result['disposition'] == disposition['disposition']
    assert result['source_fingerprint_sha256'] == source['source_fingerprint_sha256']
    assert result['fresh'] == disposition['fresh'] and result['lifecycle'] == disposition['lifecycle']
    print(json.dumps(dict(status='passed_complete_closeout_replay', query_executions=0,
        complete_results=5572, lifecycle_operations=240, disposition=disposition['disposition'],
        saved_evidence_negative_controls=28, packet_negative_controls=50,
        source_commit=source['head'], source_fingerprint_sha256=source['source_fingerprint_sha256'],
        fresh=result['fresh'], lifecycle=result['lifecycle'])))


if __name__ == '__main__':
    main()
