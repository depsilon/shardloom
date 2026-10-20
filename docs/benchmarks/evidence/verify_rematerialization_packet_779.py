"""Reopen the entire portable packet, reconstruct every source version and recompute its decision."""

import hashlib
import json
import lzma
from pathlib import Path
import subprocess
import tarfile
import tempfile

import analyze_rematerialization_779 as analysis
import rematerialization_driver_779 as driver

OUT, REPO = driver.OUT, driver.REPO
PACK = OUT/'rematerialization-portable-779'
LABEL = 'rematerialization-release-779-a1'
DEBUG = 'rematerialization-debug-779-a5'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    index_path = PACK/'rematerialization-portable-779.json'
    index = json.loads(index_path.read_text())
    assert index['status']=='passed' and index['production_head']==driver.PRODUCTION_HEAD
    for key in ('archive','manifest'):
        row=index[key];path=PACK/row['file']
        assert path.stat().st_size==row['bytes'] and driver.sha(path)==row['sha256']
    manifest=json.loads(lzma.decompress((PACK/index['manifest']['file']).read_bytes()))
    rows=manifest['members']; names=[r['path'] for r in rows]
    assert len(rows)==index['member_count'] and names==sorted(set(names))
    kept={}
    with tarfile.open(PACK/index['archive']['file'],'r:xz') as archive:
        assert [m.name for m in archive.getmembers()]==names
        for row in rows:
            member=archive.getmember(row['path']);path=Path(member.name)
            assert member.isfile() and not path.is_absolute() and '..' not in path.parts
            raw=archive.extractfile(member).read()
            assert len(raw)==row['bytes']==member.size and sha(raw)==row['sha256']
            original=row['original_path']
            if not original.startswith(('git:','generated:')):
                p=Path(original)
                assert p.is_file() and not p.is_symlink() and p.read_bytes()==raw,p
            if '/witnesses/' in member.name:
                assert sha(raw)==path.stem
            kept[member.name]=raw
    by_original={r['original_path']:r for r in rows}
    assert len(by_original)==len(rows)
    base_inventory=json.loads(kept['sources/accepted-base.json'])
    base={r['path']:kept['sources/accepted-base/'+r['path']] for r in base_inventory['sources']}
    accepted=json.loads(kept['evidence/compact-index-acceptance-source-764.json'])['source_hashes']
    assert len(base)==1899 and len(accepted)==2217
    for row in base_inventory['sources']:
        assert len(base[row['path']])==row['bytes']
        assert sha(base[row['path']])==row['sha256']==accepted[row['path']]
    modified=[
        'shardloom-vortex/src/local_primitive_prepared_relational.rs',
        'shardloom-vortex/src/local_primitive_relational_tests.rs',
        'shardloom-vortex/src/local_primitive_relational_transform.rs']
    versions=[]
    labels=[('rematerialization-debug-attempt-778-a1',False),
            ('rematerialization-debug-attempt-779-a2',False),
            ('rematerialization-debug-attempt-779-a3',False),
            ('rematerialization-debug-attempt-779-a4',False), (DEBUG,True),(LABEL,True)]
    with tempfile.TemporaryDirectory(prefix='rematerialization-packet-reopen-',dir=OUT) as tmp:
        temporary=Path(tmp)
        for label,new_format in labels:
            freeze_name=label+('-source-freeze.json' if new_format else '-source.json')
            freeze=json.loads(kept['evidence/'+freeze_name])
            patch_name=label+'-tracked.patch' if new_format else freeze['patch']
            patch=kept['evidence/'+patch_name]
            assert sha(patch)==freeze['patch_sha256']
            stage=temporary/label;stage.mkdir()
            for relative in modified:
                target=stage/relative;target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(base[relative])
            patch_file=temporary/(label+'.patch');patch_file.write_bytes(patch)
            subprocess.run(['git','apply','--check','--',str(patch_file)],cwd=stage,check=True,capture_output=True)
            subprocess.run(['git','apply','--whitespace=nowarn','--',str(patch_file)],cwd=stage,check=True,capture_output=True)
            assembled=dict(base)
            for relative in modified:assembled[relative]=(stage/relative).read_bytes()
            for relative in freeze['extra_sources']:
                assert relative not in assembled
                assembled[relative]=kept[f'evidence/{label}-extra-sources/{relative}']
            assert sorted(assembled)==[r['path'] for r in freeze['sources']]
            for row in freeze['sources']:
                assert len(assembled[row['path']])==row['bytes']
                assert sha(assembled[row['path']])==row['sha256'],(label,row['path'])
            for helper in freeze.get('helpers',[]):
                row=by_original[helper['path']]
                assert (row['bytes'],row['sha256'])==(helper['bytes'],helper['sha256'])
            versions.append(dict(label=label,source_files=len(assembled),all_source_and_helper_bytes_match=True))
    protocol=json.loads(kept['evidence/rematerialization-protocol-778.json'])
    assert sha(kept['evidence/rematerialization-protocol-778.json'])==driver.PROTOCOL_SHA
    assert kept['evidence/analyze_rematerialization_779.py']==Path(analysis.__file__).read_bytes()
    cells=protocol['workload']['cells'];by_cell={c['id']:c for c in cells}
    release=[];totals={};witnesses=set()
    # Native values/schemas are recomputed from archived full bytes, independent
    # of the native program's own expected-output hash or its summaries.
    for label,count,stream_count in ((DEBUG,81,3),(LABEL,357,9)):
        prefix=f'evidence/{label}-data/'
        streams={n:b for n,b in kept.items() if n.startswith(prefix) and n.endswith('.jsonl')}
        assert len(streams)==stream_count
        records=[json.loads(line) for raw in streams.values() for line in raw.splitlines()]
        assert len(records)==count
        for record in records:
            cell=by_cell[record['cell_id']];rows_count=record['rows']
            key=json.dumps(cell,sort_keys=True)
            expected=analysis.expected_bytes(key,rows_count,False)
            source=analysis.expected_bytes(key,rows_count,True)
            checks=[('source_rows_witness',source,False),
                    ('actual_rows_witness',expected,False),('expected_rows_witness',expected,False),
                    ('source_schema_witness',analysis.schema(analysis.oracle.source_fields(cell)),True),
                    ('actual_schema_witness',analysis.schema(analysis.oracle.output_fields(cell)),True),
                    ('expected_schema_witness',analysis.schema(analysis.oracle.output_fields(cell)),True)]
            for name,wanted,is_json in checks:
                reference=record[name];member=prefix+reference['path'];raw=kept[member]
                assert len(raw)==reference['bytes'] and sha(raw)==reference['sha256']
                assert analysis.typed_equal(json.loads(raw),wanted) if is_json else raw==wanted
                witnesses.add(member)
            if label==LABEL and record['mode']=='measure':release.append(record)
        totals[label]=count
    actual_witnesses={n for n in kept if '/witnesses/' in n}
    assert actual_witnesses==witnesses
    recomputed=analysis.decide(release,cells)
    decision=json.loads(kept[f'evidence/{LABEL}-decision.json'])
    for key in ('decision','failures','cells'):assert recomputed[key]==decision[key]
    assert decision['decision']==index['decision']
    guarded=0
    for name,raw in kept.items():
        if name.startswith('evidence/') and name.endswith('.json'):
            item=json.loads(raw)
            if isinstance(item,dict) and 'log_sha256' in item:
                assert item['process_group_drained'] is True
                assert sha(kept[name.removesuffix('.json')+'.log'])==item['log_sha256']
                guarded+=1
    assert guarded>=20
    current=json.loads((OUT/f'{LABEL}-source-freeze.json').read_text())
    driver.verify_sources(current)
    assert subprocess.check_output(['git','diff','HEAD','--binary'],cwd=REPO)==kept[f'evidence/{LABEL}-tracked.patch']
    candidate={r['path']:r for r in current['sources']}
    restoration=[dict(path=p,tracked=p in modified,observed_sha256=candidate[p]['sha256'],
                      baseline_sha256=sha(base[p]) if p in modified else None)
                 for p in [*modified,*current['extra_sources']]]
    driver.save(OUT/'rematerialization-restoration-plan-779.json',dict(
        status='verified_before_any_candidate_removal',baseline=driver.PRODUCTION_HEAD,
        decision=index['decision'],files=restoration,archive_sha256=index['archive']['sha256'],
        all_removed_source_content_preserved=True))
    result=dict(status='passed',decision=index['decision'],archive_sha256=index['archive']['sha256'],
                manifest_sha256=index['manifest']['sha256'],member_count=len(rows),
                all_members_and_original_bytes_verified=True,reconstructed_versions=versions,
                workflows=totals,full_witnesses_verified=len(witnesses),guarded_receipts_verified=guarded,
                every_source_schema_output_value_and_decision_recomputed=True)
    driver.save(OUT/'rematerialization-packet-verification-779.json',result)
    print(json.dumps(result))


if __name__=='__main__':
    main()
