"""Verify the retained regional experiment and its completed portable controls."""
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
    assert manifest['schema'] == 'shardloom.native_regional_closeout_packet.v1'
    assert manifest['original_packet_moved_without_changes']
    assert manifest['query_executions'] == 0 and not manifest['publication_attempted']
    verify_files(root, manifest)
    read = lambda name: json.loads((root / 'supplement' / name).read_bytes())
    controls = read('regional-packet-controls-r1.json')
    assert controls['status'] == 'passed' and controls['controls_rejected'] == len(controls['controls']) == 36
    assert len({row['name'] for row in controls['controls']}) == 36
    assert all(row['status'] == 'rejected' for row in controls['controls'])
    assert controls['original_source_and_runtime_reads_forbidden'] and controls['original_packet_unchanged']
    assert controls['query_executions'] == 0
    assert controls['manifest'] == identity(root / 'packet/manifest.json')
    assert controls['helper'] == identity(root / 'supplement/test_regional_packet.py')
    relocated = read('regional-packet-relocated-r1.json')
    assert relocated['returncode'] == 0 and relocated['original_paths_forbidden']
    assert json.loads(relocated['stdout']) == controls['relocated_replay']
    source = read('regional-source-r1.json')
    assert source['status'] == 'preserved_exact_candidate_source'
    assert source['disposition'] == 'retained_local_source'
    assert source['head'] == 'c9baefd00b20ef1ee1aec3cd49c72b07f762d77d'
    assert source['parent'] == source['runtime_build_head'] == '43e99d5828ff923cab8fcc43a359435b3c13c222'
    assert source['source_bytes_equal_to_executed_candidate'] and source['committed_blobs_verified'] == 5745
    assert not source['runtime_rebuilt_after_commit'] and not source['publication_attempted']
    disposition = read('regional-disposition-r1.json')
    assert disposition['candidate_source'] == source
    assert disposition['status'] == 'complete_experiment_acceptance' and disposition['disposition'] == 'retain'
    for name, status in (
        ('regional-package-r1.json', 'failed'),
        ('regional-package-r2.json', 'passed'),
        ('regional-portable-verification-r1.json', 'passed'),
        ('regional-packet-control-verification-r1.json', 'passed'),
    ):
        receipt = read(name)
        assert receipt['status'] == status and receipt['sources_unchanged'] and receipt['process_group_drained']
        assert receipt['direct_child_reaped']
        assert receipt['returncode'] == (1 if status == 'failed' else 0)
    spec = importlib.util.spec_from_file_location('regional_portable', root / 'packet/verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    result = verifier.verify(root / 'packet')
    assert result == controls['relocated_replay'] == read('regional-portable-replay-r1.json')
    assert result['disposition'] == 'retain'
    assert result['fresh'] == disposition['fresh'] and result['lifecycle'] == disposition['lifecycle']
    print(json.dumps(dict(status='passed_complete_closeout_replay', query_executions=0,
        complete_results=1974, lifecycle_operations=40, disposition='retain',
        saved_evidence_negative_controls=34, packet_negative_controls=36,
        source_commit=source['head'], source_fingerprint_sha256=source['source_fingerprint_sha256'],
        fresh=result['fresh'], lifecycle=result['lifecycle'])))


if __name__ == '__main__':
    main()
