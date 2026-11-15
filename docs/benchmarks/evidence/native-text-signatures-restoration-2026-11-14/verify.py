"""Check preserved source-restoration and installed-packet verification receipts."""
import hashlib
import json
from pathlib import Path


def identity(path):
    data = path.read_bytes()
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def main():
    if not __debug__:
        raise RuntimeError('assertions must remain enabled')
    root = Path(__file__).resolve().parent
    read = lambda name: json.loads((root / name).read_bytes())
    manifest = read('manifest.json')
    assert manifest['schema'] == 'shardloom.native_text_signature_restoration.v1'
    assert not any(path.is_symlink() for path in root.rglob('*'))
    assert {path.relative_to(root).as_posix() for path in root.rglob('*') if path.is_file()} == set(manifest['files']) | {'manifest.json'}
    for name, expected in manifest['files'].items():
        assert Path(name).as_posix() == name and not Path(name).is_absolute() and '..' not in Path(name).parts
        assert identity(root / name) == expected
    restored = read('text-source-restoration-r2.json')
    assert restored['status'] == 'restored_accepted_source'
    assert restored['tracked_tree_equals_accepted_base'] and restored['accepted_runtime_code_unchanged']
    assert restored['untracked_packet_preserved'] and not restored['production_candidate_retained']
    assert restored['original_restoration_script_post_switch_assertion_failed'] and not restored['source_switch_repeated']
    assert restored['original_failure'] == identity(root / 'logs/text-source-restoration-r1.log')
    assert restored['original_helper'] == identity(root / 'restore_text_source.py')
    assert restored['helper'] == identity(root / 'verify_text_restoration.py')
    assert all(name.startswith('docs/') for name in restored['repository_differences_from_accepted_runtime'])
    observation = root.parent / 'native-text-signatures-2026-11-14'
    assert restored['packet_manifest'] == identity(observation / 'manifest.json') == manifest['observation_packet_manifest']
    for name in ('text-source-restoration-check-r2.json', 'text-closeout-verification-r1.json'):
        value = read(name)
        assert value['status'] == 'passed' and value['returncode'] == 0
        assert value['sources_unchanged'] and value['process_group_drained'] and value['direct_child_reaped']
    complete = read('logs/text-closeout-verification-r1.log')
    assert complete['status'] == 'passed_complete_closeout_replay' and complete['disposition'] == 'drop'
    assert complete['complete_results'] == 18080 and complete['lifecycle_operations'] == 1080
    assert complete['query_executions'] == restored['query_executions'] == 0
    print(json.dumps(dict(status='passed_source_restoration_packet', query_executions=0,
        retained_commit=restored['retained_commit'], experimental_commit=restored['experimental_commit_retained'],
        accepted_runtime_code_unchanged=True, original_failure_preserved=True)))


if __name__ == '__main__':
    main()
