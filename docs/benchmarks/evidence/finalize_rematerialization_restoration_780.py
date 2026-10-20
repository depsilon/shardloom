"""Verify exact accepted-source restoration and preserve the finite rejection packet."""

import difflib
import hashlib
import json
import lzma
from pathlib import Path
import subprocess
import tarfile

E = Path(__file__).resolve().parent
W = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D = W / 'docs/benchmarks/evidence'
BASE = '0b52edf4676e430c62719ad240aa2634b627319e'
PACK = E / 'rematerialization-portable-779'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), path
    raw = path.read_bytes()
    return dict(path=str(path), bytes=len(raw), sha256=sha(raw))


def save(path, raw):
    if path.exists():
        assert path.read_bytes() == raw, path
    else:
        with path.open('xb') as stream:
            stream.write(raw)
    assert path.read_bytes() == raw


def runtime(path):
    return path.endswith('.rs') or Path(path).name in ('Cargo.toml', 'Cargo.toml.orig', 'Cargo.lock')


def main():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=W, text=True).strip() == BASE
    proof_path = E / 'rematerialization-packet-verification-779.json'
    proof = json.loads(proof_path.read_text())
    plan_path = E / 'rematerialization-restoration-plan-779.json'
    plan = json.loads(plan_path.read_text())
    index_path = PACK / 'rematerialization-portable-779.json'
    index = json.loads(index_path.read_text())
    assert proof['status'] == index['status'] == 'passed'
    assert proof['decision'] == index['decision'] == plan['decision'] == 'reject'
    assert proof['archive_sha256'] == plan['archive_sha256'] == index['archive']['sha256']
    assert proof['member_count'] == index['member_count'] == 2177
    assert proof['every_source_schema_output_value_and_decision_recomputed'] is True
    assert proof['workflows'] == {'rematerialization-debug-779-a5':81,
                                 'rematerialization-release-779-a1':357}
    assert len(proof['reconstructed_versions']) == 6 and len(plan['files']) == 9
    assert plan['baseline'] == BASE and plan['all_removed_source_content_preserved'] is True
    for row in plan['files']:
        path = W / row['path']
        if row['tracked']:
            assert sha(path.read_bytes()) == row['baseline_sha256']
        else:
            assert not path.exists()
    expected = {}
    for entry in subprocess.check_output(['git','ls-tree','-rz','--full-tree',BASE], cwd=W).split(b'\0'):
        if not entry:
            continue
        descriptor, name = entry.split(b'\t')
        name = name.decode()
        if runtime(name):
            expected[name] = descriptor.decode().split()[2]
    current = subprocess.check_output(['git','ls-files','-co','--exclude-standard','-z'],cwd=W,text=True).split('\0')
    assert {p for p in current if p and runtime(p)} == set(expected)
    assert len(expected) == 1899
    inventory = []
    for path, blob in sorted(expected.items()):
        raw = (W/path).read_bytes()
        assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest() == blob, path
        inventory.append(dict(path=path,bytes=len(raw),sha256=sha(raw),baseline_git_blob=blob))
    accepted_path = E/'compact-index-acceptance-source-764.json'
    accepted = json.loads(accepted_path.read_text())['source_hashes']
    assert len(accepted) == 2217
    for path,digest in accepted.items():
        assert sha((W/path).read_bytes()) == digest, path
    fmt_path = E/'rematerialization-restored-fmt-780.json'
    fmt = json.loads(fmt_path.read_text())
    assert fmt['status']=='passed' and fmt['returncode']==0 and fmt['process_group_drained'] is True
    assert fmt['command']==['/Users/dylan/.cargo/bin/rustup','run','1.99.0','cargo','fmt','--all','--','--check']
    assert fmt['log_sha256']==sha((E/'rematerialization-restored-fmt-780.log').read_bytes())
    for key in ('archive','manifest'):
        actual=identity(PACK/index[key]['file'])
        assert (actual['bytes'],actual['sha256']) == (index[key]['bytes'],index[key]['sha256'])
    # A derived removal diff supplements the observed pre-removal hashes and
    # exact post-removal contents. It is not claimed to be the tool transcript.
    removal=[]
    with tarfile.open(PACK/index['archive']['file'],'r:xz') as archive:
        freeze=json.loads(archive.extractfile('evidence/rematerialization-release-779-a1-source-freeze.json').read())
        source_by_path={r['path']:r for r in freeze['sources']}
        patch=archive.extractfile('evidence/rematerialization-release-779-a1-tracked.patch').read()
        assert sha(patch)==freeze['patch_sha256']
        for row in plan['files']:
            if row['tracked']:
                continue
            raw=archive.extractfile('evidence/rematerialization-release-779-a1-extra-sources/'+row['path']).read()
            assert sha(raw)==row['observed_sha256']==source_by_path[row['path']]['sha256']
            removal.extend(difflib.unified_diff(raw.decode().splitlines(keepends=True),[],
                            fromfile='a/'+row['path'],tofile='/dev/null'))
    deletion_path=E/'rematerialization-added-files-removal-780.patch.xz'
    save(deletion_path,lzma.compress(''.join(removal).encode()))
    copied=[]

    def copy(source):
        source=Path(source);target=D/source.name;raw=source.read_bytes()
        save(target,raw)
        copied.append(dict(path=str(target.relative_to(W)),bytes=len(raw),sha256=sha(raw)))

    for source in (index_path,PACK/index['archive']['file'],PACK/index['manifest']['file']):
        copy(source)
    names=('rematerialization-protocol-778.json','rematerialization-validation-repairs-779.json',
           'rematerialization-release-779-a1-decision.json','rematerialization-release-779-a1-preflight-analysis.json',
           'rematerialization-debug-779-a5-analysis.json','rematerialization-release-779-a1-sequence-completion.json',
           'rematerialization-packet-verification-779.json','rematerialization-restoration-plan-779.json',
           'rematerialization-package-779.json','rematerialization-package-779.log',
           'rematerialization-packet-check-779.json','rematerialization-packet-check-779.log',
           'rematerialization-restored-fmt-780.json','rematerialization-restored-fmt-780.log',
           'rematerialization-added-files-removal-780.patch.xz','compact-index-acceptance-source-764.json',
           'verify_rematerialization_packet_779.py','finalize_rematerialization_restoration_780.py')
    for name in names:
        copy(E/name)
    result=dict(schema_version='shardloom.rematerialization_restoration.v1',status='passed',baseline=BASE,
                runtime_input_count=len(inventory),runtime_sources=inventory,all_runtime_inputs_match_baseline=True,
                candidate_source_changes_removed=plan['files'],prior_accepted_source_files=len(accepted),
                all_prior_accepted_source_contents_unchanged=True,accepted_source_manifest=identity(accepted_path),
                packet_verification=identity(proof_path),restoration_plan=identity(plan_path),copied_evidence=copied,
                format_check=identity(fmt_path),finalizer=identity(__file__),finite_scope_result='reject',
                release_workflows=357,debug_workflows_without_decision_credit=81,
                native_suites_or_benchmarks_repeated_after_restoration=False,production_behavior_changed=False,
                full43_performance_claim=False)
    output=E/'rematerialization-restoration-780.json'
    save(output,(json.dumps(result,indent=2)+'\n').encode());copy(output)
    print(json.dumps(dict(status='passed',runtime_inputs=1899,accepted_source_files=2217,
                          removed_candidate_source_files=9,copied_evidence_files=len(copied))))


if __name__=='__main__':
    main()
