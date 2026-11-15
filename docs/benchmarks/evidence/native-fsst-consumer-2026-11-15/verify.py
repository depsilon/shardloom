"""Verify the complete FSST decision and closeout from portable saved evidence."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def verify(root):
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    manifest = json.loads((root / 'manifest.json').read_bytes())
    assert manifest['schema'] == 'shardloom.native_fsst_closeout_packet.v1'
    assert manifest['closeout_state'] == 'complete_local_evidence_and_docs'
    assert manifest['original_packet_moved_without_changes'] and manifest['query_executions'] == 0
    assert not manifest['publication_attempted'] and not manifest['whole_investigation_queue_complete']
    paths = list(root.rglob('*'))
    assert not any(path.is_symlink() for path in paths)
    assert {path.relative_to(root).as_posix() for path in paths if path.is_file()} == set(manifest['files']) | {'manifest.json'}
    for name, expected in manifest['files'].items():
        relative = Path(name)
        assert not relative.is_absolute() and '..' not in relative.parts and '\\' not in name
        assert relative.as_posix() == name and content(root / relative) == expected, name
    read = lambda name: json.loads((root / 'supplement' / name).read_bytes())
    controls = read('fsst-packet-controls-r2.json')
    assert controls['status'] == 'passed_packet_corruption_controls' and controls['count'] == len(controls['controls']) == 57
    assert len({row['name'] for row in controls['controls']}) == 57
    assert all(row['status'] == 'rejected' for row in controls['controls'])
    assert controls['original_source_runtime_evidence_reads_forbidden'] and controls['original_packet_unchanged']
    assert controls['query_executions'] == 0
    assert controls['manifest'] == content(root / 'packet/manifest.json')
    assert controls['helper'] == content(root / 'supplement/test_fsst_packet_v2.py')
    relocated = read('fsst-packet-relocated-r2.json')
    assert relocated['returncode'] == 0 and relocated['original_paths_forbidden'] and relocated['network_and_subprocess_forbidden']
    assert json.loads(relocated['stdout']) == controls['relocated_complete_replay']
    repairs = read('fsst-repair-controls-r1.json')
    assert repairs['status'] == 'passed_repair_provenance_controls' and repairs['count'] == len(repairs['controls']) == 8
    assert all(row['status'] == 'rejected' for row in repairs['controls'])
    assert len({row['name'] for row in repairs['controls']}) == 8
    assert repairs['original_packet_unchanged'] and repairs['query_executions'] == 0
    assert repairs['manifest'] == content(root / 'packet/manifest.json')
    assert repairs['helper'] == content(root / 'supplement/test_fsst_repair.py')
    source = read('fsst-source-r1.json')
    assert source['status'] == 'preserved_exact_candidate_source' and source['disposition'] == 'retained_complete_comparison'
    assert source['head'] == '45be8e0bd1d9cef920c88fa9bf91811deade41c4'
    assert source['parent'] == source['runtime_build_head'] == '873e60bca36f5030cec27c3baf59017ca49ad8ab'
    assert source['source_bytes_equal_to_executed_candidate'] and source['committed_blobs_verified'] == 5794
    assert len(source['files']) == 21 and not source['runtime_rebuilt_after_commit'] and not source['publication_attempted']
    disposition = read('fsst-disposition-r1.json')
    assert disposition['candidate_source'] == source and disposition['disposition'] == 'retain'
    assert disposition['complete_results_replayed'] == 18080 and disposition['complete_lifecycle_operations_replayed'] == 1080
    assert not disposition['thresholds_changed'] and disposition['additional_query_executions'] == 0
    for name in ('fsst-source-preservation-r1.json', 'fsst-package-check-r2.json',
                 'fsst-package-repair-check-r1.json', 'fsst-packet-controls-check-r2.json', 'fsst-repair-controls-check-r1.json'):
        receipt = read(name)
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['sources_unchanged'] and receipt['process_group_drained'] and receipt['direct_child_reaped']
        fingerprint = hashlib.sha256(json.dumps(receipt['source_files'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        assert fingerprint == source['source_fingerprint_sha256']
    final_source = read('fsst-final-source-r1.json')
    assert final_source['status'] == 'passed_exact_runtime_and_document_only_delta'
    assert final_source['runtime_source_fingerprint_sha256'] == source['source_fingerprint_sha256']
    assert final_source['candidate_source_commit'] == source['head']
    assert final_source['all_non_document_sources_unchanged'] and final_source['native_artifacts_unchanged']
    assert all(name.startswith('docs/') for name in final_source['changed_source_paths'])
    docs = read('fsst-docs-validation-r1.json')
    assert docs['status'] == 'passed_complete_closeout_documentation' and docs['query_executions'] == 0
    for name, expected in docs['reports'].items():
        assert content(root / 'supplement' / name) == expected
    assert docs['local_links_checked'] > 0 and not docs['missing_local_links']
    public, gaps, tracker = (read(name) for name in ('fsst-public-status-docs-r1.json',
        'fsst-gap-burndown-r1.json', 'fsst-queue-tracker-r1.json'))
    assert public['status'] == gaps['status'] == 'passed' and not public['blockers'] and not gaps['blockers']
    assert tracker['status'] == 'blocked' and not tracker['missing_required_inputs']
    assert len(tracker['blockers']) == 1 and re.fullmatch(r'phase plan has unchecked items: [1-9]\d*', tracker['blockers'][0])
    assert tracker['claim_gate_status'] == 'not_claim_grade' and not tracker['public_release_claim_allowed']
    spec = importlib.util.spec_from_file_location('fsst_portable_closeout', root / 'packet/verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    result = verifier.verify(root / 'packet')
    assert result == controls['relocated_complete_replay']
    assert result['source_fingerprint_sha256'] == source['source_fingerprint_sha256']
    assert result['disposition'] == disposition['disposition']
    assert result['fresh'] == disposition['fresh'] and result['lifecycle'] == disposition['lifecycle']
    return dict(status='passed_complete_closeout_replay', query_executions=0,
        complete_results=18080, lifecycle_operations=1080, disposition='retain',
        acceptance_controls=12, packet_corruption_controls=57, repair_provenance_controls=8,
        source_commit=source['head'], source_fingerprint_sha256=source['source_fingerprint_sha256'],
        fresh=result['fresh'], lifecycle=result['lifecycle'], causal_speedup_claim=False,
        whole_investigation_queue_complete=False, matcher_lifetime_complete=False,
        required_spatial_execution_complete=False)


if __name__ == '__main__':
    print(json.dumps(verify(Path(__file__).resolve().parent)))
