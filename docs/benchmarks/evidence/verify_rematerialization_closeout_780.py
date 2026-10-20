"""Independently check the retained packet, production equality and documented decision."""

import hashlib
import json
from pathlib import Path
import re
import subprocess

E=Path(__file__).resolve().parent
W=Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
D=W/'docs/benchmarks/evidence'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    restoration=json.loads((D/'rematerialization-restoration-780.json').read_text())
    assert restoration['status']=='passed' and restoration['finite_scope_result']=='reject'
    assert restoration['production_behavior_changed'] is False
    assert restoration['runtime_input_count']==1899 and restoration['prior_accepted_source_files']==2217
    assert restoration['native_suites_or_benchmarks_repeated_after_restoration'] is False
    for row in restoration['runtime_sources']:
        raw=(W/row['path']).read_bytes()
        assert len(raw)==row['bytes'] and sha(raw)==row['sha256'],row['path']
    for path,digest in json.loads((D/'compact-index-acceptance-source-764.json').read_text())['source_hashes'].items():
        assert sha((W/path).read_bytes())==digest,path
    for row in restoration['candidate_source_changes_removed']:
        if row['tracked']:
            assert sha((W/row['path']).read_bytes())==row['baseline_sha256']
        else:
            assert not (W/row['path']).exists()
    for row in restoration['copied_evidence']:
        raw=(W/row['path']).read_bytes()
        assert len(raw)==row['bytes'] and sha(raw)==row['sha256']
        original=E/Path(row['path']).name
        if not original.exists():
            original=E/'rematerialization-portable-779'/original.name
        assert original.read_bytes()==raw
    index=json.loads((D/'rematerialization-portable-779.json').read_text())
    for key in ('archive','manifest'):
        row=index[key];raw=(D/row['file']).read_bytes()
        assert len(raw)==row['bytes'] and sha(raw)==row['sha256']
    proof=json.loads((D/'rematerialization-packet-verification-779.json').read_text())
    assert proof['member_count']==index['member_count']==2177
    assert proof['all_members_and_original_bytes_verified'] is True
    assert proof['every_source_schema_output_value_and_decision_recomputed'] is True
    assert proof['full_witnesses_verified']==69 and proof['guarded_receipts_verified']==30
    assert [r['source_files'] for r in proof['reconstructed_versions']]==[1904,1904,1905,1905,1905,1905]
    assert proof['workflows']=={'rematerialization-debug-779-a5':81,'rematerialization-release-779-a1':357}
    protocol=(D/'rematerialization-protocol-778.json').read_bytes()
    assert sha(protocol)=='dbce5d1135a89704463f7b54c6557a58902c1aa41f6b4ad89778a1241e390442'
    assert sha((W/'docs/architecture/native-rematerialization-design-2026-10-19.md').read_bytes())=='5874c7b75c76eefee596ccb1ccd77ed2ea673b9ad10f6ed9a85a46a8bc801e66'
    decision=json.loads((D/'rematerialization-release-779-a1-decision.json').read_text())
    assert decision['status']=='passed' and decision['decision']=='reject' and decision['workflows']==240
    cells={(c['cohort'],c['cell']):c for c in decision['cells']}
    expected_failures={('A','one-32768-1'),('A','wide-dependencies-32768-64'),
                       ('B','wide-dependencies-32768-64'),('A','all-null-32768-8'),('B','all-null-32768-8')}
    actual=set()
    for c in decision['cells']:
        m=c['medians'];a=m['retain'];b=m['regenerate']
        for clock in ('engine_execution_ns','complete_workflow_ns'):
            failed=b[clock]-a[clock]>1_000_000 and b[clock]>a[clock]*1.05
            if failed:
                actual.add((c['cohort'],c['cell'],clock))
        assert b['peak_reserved_bytes']-a['peak_reserved_bytes']<=65536
        if c['target']:
            assert a['peak_reserved_bytes']-b['peak_reserved_bytes']>=131072
            assert b['peak_reserved_bytes']<=a['peak_reserved_bytes']*.8
    assert actual=={(*pair,clock) for pair in expected_failures for clock in ('engine_execution_ns','complete_workflow_ns')}
    assert {(f['cohort'],f['cell'],f['gate']) for f in decision['failures']}==actual
    report_path=W/'docs/architecture/native-rematerialization-2026-10-19.md'
    report=report_path.read_text()
    for cell,label in [('wide-dependencies-32768-64','Wide dependencies'),('all-null-32768-8','All-null'),('one-32768-1','One derived output')]:
        for cohort in ('A','B'):
            if (cohort,cell) not in expected_failures:
                continue
            m=cells[(cohort,cell)]['medians'];a=m['retain']['engine_execution_ns'];b=m['regenerate']['engine_execution_ns']
            assert f'| {label} / {cohort} | {a/1e6:.3f} | {b/1e6:.3f} | {(b/a-1)*100:.2f}% |' in report
    for rows,width in ((32768,8),(32768,64),(65536,8),(65536,64)):
        cell=f'mixed-{rows}-{width}'
        m=cells[('A',cell)]['medians'];a=m['retain']['peak_reserved_bytes'];b=m['regenerate']['peak_reserved_bytes']
        assert a==cells[('B',cell)]['medians']['retain']['peak_reserved_bytes']
        assert b==cells[('B',cell)]['medians']['regenerate']['peak_reserved_bytes']
        assert f'| {rows:,} / {width} | {a:,} | {b:,} | {(1-b/a)*100:.2f}% |' in report
    architecture=W/'docs/architecture'
    sequence=(architecture/'native-conditional-investigations-2026-10-19.md').read_text()
    assert 'eight finite investigations completed' in sequence and '13 rows awaiting' in sequence
    assert [int(n) for n in re.findall(r'^\| (\d+) \|',sequence,re.M)]==list(range(1,22))
    phase=(architecture/'phased-execution-plan.md').read_text()
    assert 'Eight finite decisions are complete' in phase and 'Thirteen rows await' in phase
    assert '    - [ ] `PERF-06.rematerialization` **Conditional.**' in phase
    for n in range(1,24):
        assert f'| CG-{n} |' in phase
    paths=[report_path,architecture/'native-rematerialization-design-2026-10-19.md',
           architecture/'native-conditional-investigations-2026-10-19.md',
           architecture/'phased-execution-plan.md',architecture/'phased-execution-completed-ledger.md']
    links=0
    for path in paths:
        for url in re.findall(r'\]\(([^)]+)\)',path.read_text()):
            if '://' in url or url.startswith('#'):
                continue
            target=url.split('#')[0]
            if target:
                assert (path.parent/target).exists(),(path,url)
                links+=1
    subprocess.run(['git','diff','--check'],cwd=W,check=True)
    result=dict(status='passed',finite_result='reject',runtime_files=1899,accepted_source_files=2217,
                all_copied_evidence_exact=True,all_table_values_recomputed=True,all_21_rows_preserved=True,
                all_23_competitive_gates_preserved=True,local_links_verified=links,
                finite_decisions=8,remaining_first_decisions=13,
                source_sha256=sha(Path(__file__).read_bytes()),
                reviewed_objections=['Whole dependency and pinned source remain charged.',
                                     'Inactive control failure is preserved without causal attribution.',
                                     'Broad prior acceptance is reused only after exact source restoration.'])
    raw=(json.dumps(result,indent=2)+'\n').encode()
    for path in (E/'rematerialization-closeout-780.json',D/'rematerialization-closeout-780.json'):
        with path.open('xb') as stream:
            stream.write(raw)
    print(json.dumps(result))


if __name__=='__main__':
    main()
