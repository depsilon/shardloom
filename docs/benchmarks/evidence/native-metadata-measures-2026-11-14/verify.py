"""Verify the declined experiment and its final restoration/control supplement."""
import hashlib
import importlib.util
import json
from pathlib import Path


def identity(path):
    with path.open('rb') as stream:
        digest=hashlib.file_digest(stream,'sha256').hexdigest()
    return dict(bytes=path.stat().st_size,sha256=digest)


def verify_files(root,manifest):
    expected=manifest['files']
    paths=list(root.rglob('*'))
    assert not any(path.is_symlink() for path in paths)
    assert {path.relative_to(root).as_posix() for path in paths if path.is_file()}==set(expected)|{'manifest.json'}
    for name,wanted in expected.items():
        relative=Path(name)
        assert not relative.is_absolute() and '..' not in relative.parts
        assert relative.as_posix()==name and '\\' not in name
        assert identity(root/relative)==wanted,name


def main():
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    root=Path(__file__).resolve().parent
    manifest=json.loads((root/'manifest.json').read_bytes())
    assert manifest['schema']=='shardloom.native_metadata_closeout_packet.v1'
    verify_files(root,manifest)
    read=lambda name:json.loads((root/'supplement'/name).read_bytes())
    controls=read('metadata-packet-controls-r1.json')
    assert controls['status']=='passed' and controls['controls_rejected']==len(controls['controls'])==33
    assert all(row['status']=='rejected' for row in controls['controls'])
    assert controls['original_source_and_runtime_reads_forbidden'] and controls['original_packet_unchanged']
    assert controls['query_executions']==0
    assert controls['manifest']==identity(root/'packet/manifest.json')
    assert controls['helper']==identity(root/'supplement/test_metadata_packet.py')
    relocated=read('metadata-packet-relocated-r2.json')
    assert relocated['returncode']==0 and relocated['original_paths_forbidden']
    assert json.loads(relocated['stdout'])==controls['relocated_replay']
    restored=read('metadata-source-restoration-r1.json')
    assert restored['status']=='restored_accepted_source' and not restored['production_candidate_retained']
    assert restored['retained_commit']=='046488dc3431669aba51c9cc16d818b9b5eff3e2'
    assert restored['experimental_commit_retained']=='6f96504739a5c99a2268d5ac59a4d2de677fb652'
    assert restored['tracked_tree_equals_accepted_base'] and restored['unrelated_worktrees_untouched']
    assert restored['query_executions']==0 and not restored['publication_attempted']
    for name,digest in restored['proof_files'].items():
        assert identity(root/'supplement'/name)['sha256']==digest
    failure=read('metadata-packet-control-failure-r1.json')
    assert failure['status']=='failed_control_driver' and failure['query_executions']==0
    cleanup=read('metadata-control-extraction-cleanup-r1.json')
    assert cleanup['status']=='retired_verified_failed_control_extraction'
    assert cleanup['all_bytes_equal_to_packet_members'] and cleanup['no_open_handles'] and cleanup['path_absent']
    assert cleanup['original_packet_retained'] and cleanup['source_runtime_and_input_payloads_untouched']
    for name,status in (('metadata-package-r1.json','failed'),('metadata-package-r2.json','passed'),
                        ('metadata-packet-controls-r2.json','passed')):
        receipt=read(name)
        assert receipt['status']==status and receipt['sources_unchanged'] and receipt['process_group_drained']
        assert receipt['direct_child_reaped']
    spec=importlib.util.spec_from_file_location('metadata_portable',root/'packet/verify.py')
    verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
    result=verifier.verify(root/'packet')
    assert result==controls['relocated_replay']==read('metadata-portable-replay-r1.json')
    assert result['disposition']=='drop'
    print(json.dumps(dict(status='passed_complete_closeout_replay',query_executions=0,
        complete_results=1274,lifecycle_operations=40,disposition='drop',
        saved_evidence_negative_controls=27,packet_negative_controls=33,
        accepted_source_restored=True,experimental_source_preserved=True)))


if __name__=='__main__':
    main()
