"""Replay source and raw-counter provenance for the finite prerequisite decisions."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import tarfile


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def match(raw, row):
    assert len(raw) == row['bytes'] and digest(raw) == row['sha256']


def sources(record, repository):
    for name, row in record['sources'].items():
        raw = subprocess.check_output(['git', 'show', record['source_commit'] + ':' + name], cwd=repository)
        match(raw, row)
        lines = raw.decode().splitlines()
        for excerpt in row.get('excerpts', []):
            assert '\n'.join(lines[excerpt['start'] - 1:excerpt['end']]) == excerpt['text']
    return len(record['sources'])


def verify_region(record, repository):
    assert record['schema'] == 'shardloom.region_dependent_prerequisites.v1'
    assert record['runtime_changed'] is False and record['new_workload_executed'] is False
    assert [row['id'] for row in record['decisions']] == [
        'PERF-10.ordered_candidates', 'PERF-09.design', 'PERF-12.topology']
    assert all(row['status'] == 'missing_prerequisite' and not row['substep_complete']
               for row in record['decisions'])
    provider = record['provider_rejection']
    for name, row in provider['artifacts'].items():
        match((repository / name).read_bytes(), row)
    manifest = json.loads((repository / provider['manifest']).read_text())
    assert manifest['status'] == 'passed' and not manifest['eligible_for_timings']
    assert manifest['comparison_calls'] == 64 and manifest['intake_calls'] == 32
    return dict(status='passed', source_files=sources(record, repository), decisions=3,
                missing_prerequisites=3, substeps_completed=0, new_workload_executed=False)


def verify_pgo(record, repository):
    assert record['schema'] == 'shardloom.perf13_prerequisite_acceptance.v1'
    assert record['status'] == 'missing_prerequisite' and not record['substep_complete']
    assert record['pgo_training_executed'] is False and record['runtime_changed'] is False
    inventory = record['raw_counter_inventory']
    encoded = record['raw_counter_inventory_bytes'].encode()
    assert json.loads(encoded) == inventory
    assert digest(encoded) == record['raw_counter_inventory_sha256']
    archive = repository / record['counter_archive']['repository_path']
    match(archive.read_bytes(), record['counter_archive'])
    rows = inventory['calls']
    keys = [(r['query'], r['order'], r['run'], r['role']) for r in rows]
    expected = {(q, order, run, role) for q in (3, 23, 29)
                for order in ('forward', 'reverse') for run in (1, 2, 3)
                for role in ('control', 'candidate')}
    assert len(keys) == len(set(keys)) == 36 and set(keys) == expected
    with tarfile.open(archive, 'r:xz') as stream:
        def archived(row):
            member = stream.getmember(row['archive_member'])
            assert member.isfile()
            raw = stream.extractfile(member).read()
            match(raw, row)
            return json.loads(raw)
        result = archived(inventory['evidence_bundle']['cohort_result'])
        protocol = archived(inventory['evidence_bundle']['cohort_protocol'])
        assert result['status'] == 'passed' and result['complete_values'] is True
        assert len(result['records']) == protocol['complete_calls'] == 48
        assert protocol['query_ids'] == [3, 23, 29, 34]
        result_rows = {(r['query'], r['order'], r['run'], r['role']): r for r in result['records']}
        for row in rows:
            raw = archived(row)
            assert raw == result_rows[(row['query'], row['order'], row['run'], row['role'])]['timing']
            assert raw['returncode'] == 0 and raw['direct_child_reaped']
            assert raw['counter_provenance'] == row['counter_provenance']
            for key in ('seconds', 'pid', 'utc_started', 'user_cpu_seconds', 'system_cpu_seconds'):
                assert raw[key] == row[key]
            for key in ('ri_instructions', 'ri_cycles', 'ri_user_time', 'ri_system_time',
                        'ri_proc_start_abstime', 'ri_proc_exit_abstime', 'ri_diskio_bytesread',
                        'ri_lifetime_max_phys_footprint', 'ri_runnable_time'):
                assert raw['darwin_rusage_v4'][key] == row[key]
        medians = inventory['reported_summary']['literal_medians']
        assert len(medians) == 12
        for median in medians:
            selected = [r for r in rows if all(r[k] == median[k] for k in ('query', 'order', 'role'))]
            assert len(selected) == median['calls'] == 3
            for key in ('seconds', 'ri_instructions', 'ri_cycles'):
                assert statistics.median(r[key] for r in selected) == median['median_' + key]
    tests = record['orchestration_tests']
    guard_raw = tests['receipt_bytes'].encode()
    assert digest(guard_raw) == tests['receipt_sha256']
    receipt = json.loads(guard_raw)
    assert receipt['status'] == 'passed' and receipt['returncode'] == 0
    assert receipt['head'] == record['source_commit']
    assert receipt['process_group_drained'] and receipt['direct_child_reaped']
    assert digest(tests['log'].encode()) == receipt['log_sha256']
    assert 'Ran 2 tests' in tests['log'] and '\nOK\n' in tests['log']
    assert receipt['command'][-2:] == [
        'PgoTests.test_failed_training_never_merges_or_builds_profile_use',
        'PgoTests.test_three_frozen_binaries_flags_and_fresh_merge_inputs']
    for name, row in record['historical_screen_artifacts'].items():
        match((repository / name).read_bytes(), row)
    assert record['current_toolchain']['rustc']['stdout'].splitlines()[0].startswith('rustc 1.99.0 ')
    assert 'LLVM version: 23.1.1' in record['current_toolchain']['rustc']['stdout']
    assert 'LLVM version 23.1.1-rust-1.99.0-stable' in record['current_toolchain']['llvm_profdata']['stdout']
    return dict(status='passed', source_files=sources(record, repository),
                raw_counter_calls=36, median_cells=12, orchestration_tests=2,
                scope='historical whole-process counters; no current dispatch attribution',
                pgo_training_executed=False, substeps_completed=0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--region', type=Path, required=True)
    parser.add_argument('--pgo', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(dict(region=verify_region(json.loads(args.region.read_text()), args.repository),
                          pgo=verify_pgo(json.loads(args.pgo.read_text()), args.repository))), flush=True)


if __name__ == '__main__':
    main()
