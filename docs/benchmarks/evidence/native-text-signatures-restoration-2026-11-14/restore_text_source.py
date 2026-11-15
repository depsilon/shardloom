"""Keep the declined text branch and resume from the accepted source-domain runtime."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess

from text_runtime import OUT, ROOT, checked_receipt, sources, verify_runtime


def git(*args):
    return subprocess.check_output(['git',*args],cwd=ROOT).decode().strip()


def read(path):
    return json.loads(path.read_bytes())


def identity(path):
    data=path.read_bytes()
    return dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())


def worktrees():
    return {block.splitlines()[0].removeprefix('worktree '):block
            for block in git('worktree','list','--porcelain').split('\n\n')}


def branches():
    return dict(line.split() for line in git('for-each-ref','--format=%(refname) %(objectname)',
                                            'refs/heads').splitlines())


def main():
    assert __debug__
    manifest,build=verify_runtime('runtime-r3')
    rejected='0c4aee0b7e7c97f73598482d2b4b69b4b0f5c531'
    accepted='0e6d15a658c060bd11e9a1001a71515f69f2064e'
    next_branch='codex/native-read-consumer-batching'
    rejected_branch='codex/native-predicate-zones'
    assert git('rev-parse','HEAD')==rejected
    assert git('branch','--show-current')==rejected_branch
    assert not git('diff','--name-only') and not git('diff','--cached','--name-only')
    for name in ('text-packet-integration-r1','text-closeout-verification-r1'):
        checked_receipt(OUT/(name+'.json'),build['source_files'])
    final=read(OUT/'logs/text-closeout-verification-r1.log')
    assert final['status']=='passed_complete_closeout_replay' and final['disposition']=='drop'
    assert final['query_executions']==0 and final['complete_results']==18080
    packet=ROOT/'docs/benchmarks/evidence/native-text-signatures-2026-11-14'
    untracked=set(filter(None,git('ls-files','--others','--exclude-standard').splitlines()))
    assert untracked=={p.relative_to(ROOT).as_posix() for p in packet.rglob('*') if p.is_file()}
    packet_manifest=identity(packet/'manifest.json')
    before_worktrees,before_branches=worktrees(),branches()
    assert 'refs/heads/'+next_branch not in before_branches
    assert before_branches['refs/heads/'+rejected_branch]==rejected
    subprocess.run(['git','switch','-c',next_branch,accepted],cwd=ROOT,check=True)
    assert git('rev-parse','HEAD')==accepted and git('branch','--show-current')==next_branch
    assert not git('diff',accepted,'--name-only') and not git('diff','--cached','--name-only')
    assert git('rev-parse','HEAD^{tree}')==git('rev-parse',accepted+'^{tree}')
    assert set(filter(None,git('ls-files','--others','--exclude-standard').splitlines()))==untracked
    assert identity(packet/'manifest.json')==packet_manifest
    after_worktrees,after_branches=worktrees(),branches()
    assert set(before_worktrees)==set(after_worktrees)
    assert {k:v for k,v in before_worktrees.items() if k!=str(ROOT)}=={
        k:v for k,v in after_worktrees.items() if k!=str(ROOT)}
    assert after_branches==dict(before_branches,**{'refs/heads/'+next_branch:accepted})
    facts=OUT.parent/'native-source-domain-facts-2026-11-14'
    # Use an independent checker instance: the source-facts wrapper otherwise
    # mutates the dictionary module that also backs the candidate checker.
    checker=OUT.parent/'native-dictionary-domain-2026-11-14/dictionary_runtime.py'
    spec=importlib.util.spec_from_file_location('accepted_source_facts_runtime',checker)
    runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
    runtime.OUT, runtime.ROOT = facts, ROOT
    baseline,baseline_build=runtime.verify_runtime('runtime-r4',current_sources=False)
    closeout=read(facts/'source-facts-closeout-r1.json')
    assert closeout['commit']=='494b41cca98349b21dfabd09a129a205c6664ecb' and closeout['disposition']=='retain'
    assert closeout['source_fingerprint_sha256']==baseline['source_fingerprint_sha256']
    current=sources()
    differences={name:dict(executed=baseline_build['source_files'].get(name),current=current.get(name))
        for name in sorted(set(current)|set(baseline_build['source_files']))
        if current.get(name)!=baseline_build['source_files'].get(name)}
    assert all(name.startswith('docs/') for name in differences)
    assert verify_runtime('runtime-r3',current_sources=False)[0]==manifest
    result=dict(status='restored_accepted_source',worktree=str(ROOT),retained_commit=accepted,
        retained_tree=git('rev-parse','HEAD^{tree}'),branch=next_branch,
        experimental_commit_retained=rejected,experimental_branch_retained=rejected_branch,
        tracked_tree_equals_accepted_base=True,unrelated_worktrees_untouched=True,
        existing_branch_heads_unchanged=True,production_candidate_retained=False,
        accepted_runtime=str(facts/'runtime-r4'),accepted_runtime_manifest=identity(facts/'runtime-r4-identity.json'),
        accepted_runtime_source_fingerprint=baseline['source_fingerprint_sha256'],
        accepted_runtime_code_unchanged=True,repository_differences_from_accepted_runtime=differences,
        packet_manifest=packet_manifest,untracked_packet_preserved=True,
        proof_files={name:identity(OUT/name) for name in ('text-disposition-r1.json',
            'text-packet-relocated-r3.json','text-packet-controls-r3.json',
            'text-closeout-verification-r1.json')},query_executions=0,publication_attempted=False)
    with (OUT/'text-source-restoration-r1.json').open('x') as stream:
        json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps({key:result[key] for key in ('status','retained_commit','branch',
        'experimental_commit_retained','accepted_runtime_code_unchanged','query_executions')}))


if __name__=='__main__':
    main()
