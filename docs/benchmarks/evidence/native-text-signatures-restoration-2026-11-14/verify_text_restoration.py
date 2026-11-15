"""Verify the completed source switch after the original/rebased commit assertion mismatch."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess

from text_runtime import OUT, ROOT, sources, verify_runtime


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def read(path):
    return json.loads(path.read_bytes())


def identity(path):
    data = path.read_bytes()
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def main():
    assert __debug__
    accepted = '0e6d15a658c060bd11e9a1001a71515f69f2064e'
    rejected = '0c4aee0b7e7c97f73598482d2b4b69b4b0f5c531'
    branch = 'codex/native-read-consumer-batching'
    assert git('rev-parse', 'HEAD') == accepted and git('branch', '--show-current') == branch
    assert not git('diff', '--name-only') and not git('diff', '--cached', '--name-only')
    assert git('rev-parse', 'codex/native-predicate-zones') == rejected
    assert git('rev-parse', 'codex/native-optimization-integration') == '494b41cca98349b21dfabd09a129a205c6664ecb'
    packet = ROOT / 'docs/benchmarks/evidence/native-text-signatures-2026-11-14'
    untracked = set(git('ls-files', '--others', '--exclude-standard').splitlines())
    assert untracked == {path.relative_to(ROOT).as_posix() for path in packet.rglob('*') if path.is_file()}
    assert identity(packet / 'manifest.json') == read(OUT / 'text-packet-installation-r1.json')['manifest']
    final = read(OUT / 'logs/text-closeout-verification-r1.log')
    assert final['status'] == 'passed_complete_closeout_replay' and final['disposition'] == 'drop'
    failure_path = OUT / 'logs/text-source-restoration-r1.log'
    failure = failure_path.read_text()
    assert "Switched to a new branch 'codex/native-read-consumer-batching'" in failure
    assert "assert closeout['commit']=='494b41cca98349b21dfabd09a129a205c6664ecb'" in failure
    assert failure.rstrip().endswith('AssertionError')
    assert not (OUT / 'text-source-restoration-r1.json').exists()
    facts = OUT.parent / 'native-source-domain-facts-2026-11-14'
    closeout = read(facts / 'source-facts-closeout-r1.json')
    assert closeout['commit'] == '6ede731e3d6b4fea366816f35c7a7318c390be91'
    assert closeout['disposition'] == 'retain'
    equivalent = '494b41cca98349b21dfabd09a129a205c6664ecb'
    assert git('rev-parse', closeout['commit'] + '^{tree}') == git('rev-parse', equivalent + '^{tree}') == closeout['tree']
    assert git('rev-parse', accepted + '^') == equivalent
    changes = git('diff', '--name-only', equivalent, accepted).splitlines()
    assert changes and all(name.startswith('docs/') for name in changes)
    checker = OUT.parent / 'native-dictionary-domain-2026-11-14/dictionary_runtime.py'
    spec = importlib.util.spec_from_file_location('text_restoration_accepted_runtime', checker)
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    runtime.OUT, runtime.ROOT = facts, ROOT
    baseline, build = runtime.verify_runtime('runtime-r4', current_sources=False)
    assert baseline['source_fingerprint_sha256'] == closeout['source_fingerprint_sha256']
    current = sources()
    differences = {name: dict(executed=build['source_files'].get(name), current=current.get(name))
        for name in sorted(set(current) | set(build['source_files']))
        if current.get(name) != build['source_files'].get(name)}
    assert all(name.startswith('docs/') for name in differences)
    candidate, _ = verify_runtime('runtime-r3', current_sources=False)
    assert candidate['source_fingerprint_sha256'] == read(OUT / 'text-source-r1.json')['source_fingerprint_sha256']
    result = dict(status='restored_accepted_source', worktree=str(ROOT), retained_commit=accepted,
        retained_tree=git('rev-parse', 'HEAD^{tree}'), branch=branch,
        experimental_commit_retained=rejected, experimental_branch_retained='codex/native-predicate-zones',
        tracked_tree_equals_accepted_base=True, production_candidate_retained=False,
        original_restoration_script_post_switch_assertion_failed=True,
        original_failure=identity(failure_path), original_helper=identity(OUT / 'restore_text_source.py'),
        failure_reason='The immutable C closeout records its original commit; the integration branch has the same tree with changed parents. Source switch and prior branch/worktree assertions completed before this mismatch.',
        original_accepted_commit=closeout['commit'], equivalent_integration_commit=equivalent,
        equivalent_integration_tree=closeout['tree'], later_accepted_changes_are_docs_only=changes,
        source_switch_repeated=False, existing_experimental_and_integration_heads_unchanged=True,
        accepted_runtime=str(facts / 'runtime-r4'), accepted_runtime_manifest=identity(facts / 'runtime-r4-identity.json'),
        accepted_runtime_source_fingerprint=baseline['source_fingerprint_sha256'], accepted_runtime_code_unchanged=True,
        repository_differences_from_accepted_runtime=differences,
        packet_manifest=identity(packet / 'manifest.json'), untracked_packet_preserved=True,
        query_executions=0, publication_attempted=False, helper=identity(Path(__file__)))
    with (OUT / 'text-source-restoration-r2.json').open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps({key:result[key] for key in ('status', 'retained_commit', 'branch',
        'experimental_commit_retained', 'accepted_runtime_code_unchanged', 'source_switch_repeated', 'query_executions')}))


if __name__ == '__main__':
    main()
