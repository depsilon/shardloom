"""Verify saved dictionary-repair observations without running a native query."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sqlite3
import subprocess
import sys
import tarfile
import tempfile

BASE = '8f426b3d2e774be2a9d30a731c33b8c830378d99'
FROZEN_SHA256 = '38c7a349ec04645e094248d20cfd3d8325e33a63ad76dd9bdc1543412bf7681d'
NATIVE = 'shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment'
TEST_SOURCE = 'shardloom-vortex/src/native_provider_dictionary_tests.rs'
PROVIDER_SOURCE = 'shardloom-vortex/src/native_provider_chunked.rs'
REPLAY_SUPPORT = (
    'scripts/native_workflow_protocol.py',
    'scripts/clickbench_reference_packet.py',
    'scripts/clickbench_native_sql.py',
)
CONTROL_NAMES = {
    'native_test_failure', 'undrained_native_process', 'mutable_source_validation',
    'provider_source_changed', 'provider_lock_changed', 'supplemental_protocol_source_changed',
    'regression_test_missing_with_rebound_log', 'runtime_library_mismatch',
    'original_failed_check_omitted', 'original_failure_laundered_as_performance_pass',
    'repair_cohort_laundered_as_full10k', 'failed_request_omitted', 'archive_byte_changed',
    'relocated_source_only',
}
CHECKS = {
    'format-r2': ['cargo','fmt','--all','--','--check'],
    'default-clippy-r1': ['cargo','clippy','--offline','--locked','--workspace','--all-targets','--','-D','warnings'],
    'default-tests-r1': ['env','RUST_TEST_THREADS=4','cargo','test','--offline','--locked','--workspace','--all-targets'],
    'native-clippy-r2': ['cargo','clippy','--offline','--locked','--workspace','--all-targets','--features',NATIVE,'--','-D','warnings'],
    'native-tests-r1': ['env','RUST_TEST_THREADS=4','cargo','test','--offline','--locked','--workspace','--all-targets','--features',NATIVE],
    'dictionary-focused-r2': ['cargo','test','--offline','--locked','--workspace','--lib','--features',NATIVE,'native_dictionary_suspended_','--','--nocapture','--test-threads=4'],
    'release-build-r1': ['env','PYO3_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python3','cargo','build','--offline','--locked','--release','--features','release-user-surfaces','-p','shardloom-cli','-p','shardloom-python'],
}
ORIGINAL_OUTCOMES = {'passed':9904,'crashed':76,'harness_error':20}
STATUS_PROSE = {
    'docs/architecture/phased-execution-plan.md',
    'docs/architecture/native-dictionary-execution-repair-2026-11-10.md',
    'docs/architecture/native-shared-pipeline-investigations-2026-11-10.md',
    'docs/architecture/native-variant-expressions-2026-11-10.md',
    'docs/architecture/native-rich-data-lifecycle-2026-10-24.md',
    'docs/architecture/local-development-storage.md',
}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(raw):
    return dict(bytes=len(raw),sha256=digest(raw))


def fingerprint(value):
    return digest(json.dumps(value,sort_keys=True,separators=(',',':')).encode())


def rust_counts(raw):
    rows = re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',raw)
    return dict(zip(('passed','failed','ignored'),(sum(int(row[i]) for row in rows) for i in range(3))))


def read_archive(packet):
    report = json.loads((packet/'acceptance.json').read_bytes())
    assert report['schema'] == 'shardloom.dictionary_repair_acceptance.v1'
    assert report['status'] == 'local_acceptance_passed' and report['source_base'] == BASE
    assert report['performance_claim'] is False and report['complete_10k_performance_claim'] is False
    rows = report['members']
    assert rows == sorted(rows,key=lambda r:r['path'])
    assert len({r['path'] for r in rows}) == len(rows)
    assert sum(row['bytes'] for row in rows) < 512 << 20
    for row in rows:
        path = PurePosixPath(row['path'])
        assert not path.is_absolute() and str(path) == row['path']
        assert all(part not in ('','.','..') for part in path.parts)
    stored = report['stored_members']
    assert stored == sorted(stored,key=lambda row:row['path'])
    assert len({row['path'] for row in stored}) == len(stored)
    assert sum(row['bytes'] for row in stored) == sum(row['bytes'] for row in rows)
    stored_by_name = {row['path']:row for row in stored}
    data = {}
    for ordinal,record in enumerate(report['archives']):
        assert record['path'] == f'verification-{ordinal+1:03d}.tar.xz'
        archive_path = packet/record['path']
        assert identity(archive_path.read_bytes()) == {k:record[k] for k in ('bytes','sha256')}
        assert record['bytes'] < 64 << 20
        with tarfile.open(archive_path,'r:xz') as archive:
            members = archive.getmembers()
            assert [m.name for m in members] == record['members']
            for member in members:
                assert member.name not in data
                row = stored_by_name[member.name]
                path = PurePosixPath(member.name)
                assert member.isfile() and not path.is_absolute() and all(p not in ('','.','..') for p in path.parts)
                assert member.size == row['bytes'] and member.size <= 24 << 20
                raw = archive.extractfile(member).read()
                assert identity(raw) == {k:row[k] for k in ('bytes','sha256')}
                data[member.name] = raw
    assert set(data) == set(stored_by_name)
    for row in report['assemblies']:
        assert row['path'] not in data
        raw = b''.join(data.pop(name) for name in row['parts'])
        assert identity(raw) == {k:row[k] for k in ('bytes','sha256')}
        data[row['path']] = raw
    assert set(data) == {row['path'] for row in rows}
    for row in rows:
        assert identity(data[row['path']]) == {k:row[k] for k in ('bytes','sha256')}
    assert data['current/tools/verify_dictionary_repair_packet.py'] == Path(__file__).read_bytes()
    return report,data


def source_equal(before,after,data):
    assert set(before) == set(after)
    changes = [name for name in before if before[name] != after[name]]
    if not changes:
        return
    assert changes == [TEST_SOURCE]
    old = data['source-blobs/'+before[TEST_SOURCE]['sha256']]
    new = data['source-blobs/'+after[TEST_SOURCE]['sha256']]
    phrase = b'/// Restrict the registry so eager take kernels cannot bypass ExecuteSlot.'
    assert old.count(phrase) == 1
    assert old.replace(phrase,phrase.replace(b'ExecuteSlot',b'`ExecuteSlot`')) == new


def replay_sources(data):
    """Retain the frozen client and add its source-bound protocol imports."""
    sources = {name.removeprefix('harness/'):raw for name,raw in data.items()
        if name.startswith('harness/')}
    original = json.loads(data['original/current-baseline-10k-r1.json'])
    candidate = json.loads(data['current/release-build-r1.json'])
    for receipt in (original,candidate):
        assert receipt['head'] == BASE
        assert fingerprint(receipt['build_sources']) == receipt['build_source_fingerprint_sha256']
    for name in REPLAY_SUPPORT:
        assert name not in sources
        item = original['build_sources'][name]
        assert item == candidate['build_sources'][name]
        raw = data['source-blobs/'+item['sha256']]
        assert identity(raw) == item
        sources[name] = raw
    return sources


def verify_claims(report,data):
    def read(name):
        return json.loads(data[name])
    old = read('original/current-baseline-10k-r1/failed-observation-replay-r1.json')
    new = read('current/correctness-cohort-r1/verified-correctness-r1.json')
    assert report['performance_claim'] is False and report['complete_10k_performance_claim'] is False
    assert old['observed_attempts'] == 10000 and old['original_outcomes'] == ORIGINAL_OUTCOMES
    assert old['complete_performance_result_valid'] is False and old['engine_execution_seconds'] is None
    assert old['verification_passed'] and old['verified_correct_results'] == 9904
    assert new['status'] == 'passed' and new['candidate_outcomes'] == {'passed':517}
    assert new['original_selected_outcomes'] == {'passed':421,'crashed':76,'harness_error':20}
    assert new['original_failed_cases'] == 96 and new['passing_controls'] == 421
    assert new['complete_10k_performance_claim'] is False and new['query_executions_during_replay'] == 0
    assert new['attempts_per_case'] == 1 and new['requested_repeats'] == 0 and not new['failures']
    assert report['original_observation'] == dict(attempts=10000,outcomes=ORIGINAL_OUTCOMES,
        complete_performance_result_valid=False,engine_execution_seconds=None)
    assert report['repair_cohort'] == dict(cases=517,original_failed_cases=96,passing_controls=421,
        outcomes={'passed':517},selection_uses_timing=False)
    failed = {}
    for name,raw in data.items():
        if name.startswith('current/') and name.endswith('.json') and name.count('/') == 1:
            value = json.loads(raw)
            if isinstance(value,dict) and (str(value.get('status')).startswith('failed') or value.get('status') == 'interrupted'):
                failed[name] = identity(raw)
    assert {row['receipt']:row['identity'] for row in report['original_failed_checks']} == failed
    assert all(row['disposition'] for row in report['original_failed_checks'])


def verify_sources(report,data,repo=None):
    verify_claims(report,data)
    replay_sources(data)
    def read(name):
        return json.loads(data[name])
    build = read('current/release-build-r1.json')
    sources = build['build_sources']
    assert report['source_snapshot'] == sources
    assert report['source_fingerprint_sha256'] == build['build_source_fingerprint_sha256'] == fingerprint(sources)
    receipts = {}
    for name,command in CHECKS.items():
        receipt = read('current/'+name+'.json')
        receipts[name] = receipt
        assert receipt['command'] == command and receipt['base'] == BASE
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['fixed_source_validation'] and not receipt['allow_lockfile_update']
        assert receipt['log_sha256'] == digest(data['current/'+name+'.log'])
        assert fingerprint(receipt['build_sources']) == receipt['build_source_fingerprint_sha256']
        for phase in ('pre_execution','post_execution'):
            check = receipt['build_source_verification'][phase]
            assert check['head_matches'] and check['path_set_matches'] and not check['changed_paths']
            assert check['fingerprint_sha256'] == receipt['build_source_fingerprint_sha256']
        source_equal(receipt['build_sources'],sources,data)
        for item in (*receipt['build_sources'].values(),*receipt['tool_source_inventory'].values()):
            if 'sha256' in item and 'symlink' not in item:
                assert identity(data['source-blobs/'+item['sha256']]) == item
        assert report['checks'][name] == dict(command=command,status='passed',
            rust_tests=rust_counts(data['current/'+name+'.log']))
    tests = sorted(re.findall(rb'fn (native_dictionary_suspended_\w+)\(',data['source-blobs/'+sources[TEST_SOURCE]['sha256']]))
    assert len(tests) == 3 and report['required_native_tests'] == [name.decode() for name in tests]
    for check in ('native-tests-r1','dictionary-focused-r2'):
        log = data['current/'+check+'.log']
        for test in tests:
            assert re.search(rb'^test \S*::'+test+rb' \.\.\. ok$',log,re.M),test
        assert rust_counts(log)['failed'] == 0
    before = read('current/suspended-dictionary-regression-r2.json')
    assert before['status'] == 'failed' and before['returncode'] == 101
    assert before['process_group_drained'] and before['direct_child_reaped']
    assert before['log_sha256'] == digest(data['current/suspended-dictionary-regression-r2.log'])
    assert b'DictSlots codes slot' in data['current/suspended-dictionary-regression-r2.log']
    old_provider = before['build_sources'][PROVIDER_SOURCE]
    assert old_provider != sources[PROVIDER_SOURCE]
    assert identity(data['source-blobs/'+old_provider['sha256']]) == old_provider
    assert b'!dict.codes().is::<Chunked>()' in data['source-blobs/'+old_provider['sha256']]
    runtime = read('current/runtime-r1-identity.json')
    assert runtime['status'] == 'passed' and runtime['profile'] == 'release'
    assert runtime['build_receipt'] == 'release-build-r1.json'
    assert runtime['build_receipt_identity'] == identity(data['current/release-build-r1.json'])
    assert runtime['build_source_fingerprint_sha256'] == fingerprint(sources)
    assert {Path(r['path']).name for r in runtime['files']} == {'shardloom','lib_native.dylib'}
    copied = read('current/runtime-library-copy-r1.json')
    library = next(row for row in runtime['files'] if row['path'] == copied['source'])
    assert copied['status'] == 'passed' and copied['identity'] == {k:library[k] for k in ('bytes','sha256')}
    for name in ('docs-checks-r1','doc-contracts-r1'):
        receipt = read('current/'+name+'.json')
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['fixed_source_validation'] and not receipt['allow_lockfile_update']
        assert receipt['process_group_drained'] and receipt['direct_child_reaped']
        assert receipt['log_sha256'] == digest(data['current/'+name+'.log'])
        late = receipt['build_sources']
        changed = {key for key in set(late)|set(sources) if late.get(key) != sources.get(key)}
        assert changed <= STATUS_PROSE
        assert fingerprint(late) == receipt['build_source_fingerprint_sha256']
        for phase in ('pre_execution','post_execution'):
            check = receipt['build_source_verification'][phase]
            assert check['head_matches'] and check['path_set_matches'] and not check['changed_paths']
            assert check['fingerprint_sha256'] == fingerprint(late)
        if name == 'doc-contracts-r1':
            assert receipt['command'] == ['env','RUST_TEST_THREADS=4','cargo','test','--offline','--locked','-p','shardloom-contract-tests','--all-targets']
            assert rust_counts(data['current/'+name+'.log'])['failed'] == 0
        else:
            assert receipt['command'] == ['/opt/homebrew/opt/python@3.13/libexec/bin/python3','-B',
                '/Users/dylan/LocalData/shardloom/dictionary-execution-repair-2026-11-10/tools/check_docs.py']
    if repo is not None:
        # Only named documentation may advance after engine validation. All
        # runtime, vendor, build configuration, harness and test bytes stay exact.
        for name,item in sources.items():
            if name in STATUS_PROSE:
                continue
            path = repo/name
            if 'symlink' in item:
                assert path.is_symlink() and str(path.readlink()) == item['symlink']
            elif item.get('absent'):
                assert not path.exists()
            else:
                assert path.is_file() and not path.is_symlink() and identity(path.read_bytes()) == item,name
        paths = set()
        excluded = ('docs/benchmarks/evidence/','docs/release/evidence/','docs/dependencies/evidence/')
        for command in (['git','ls-files','--cached','-z'],['git','ls-files','--others','--exclude-standard','-z']):
            paths.update(name for name in subprocess.check_output(command,cwd=repo).decode().split('\0')
                if name and not name.startswith(('.git/',*excluded)) and '.git' not in Path(name).parts)
        assert (paths ^ set(sources)) <= STATUS_PROSE
    return build,runtime


def replay_saved(report,data,temporary):
    """Use the byte-exact frozen comparator; inputs and runtimes are not opened."""
    temporary = temporary.resolve()
    def read(name):
        return json.loads(data[name])
    def bound(row):
        raw = data[report['locations'][row['path']]]
        wanted = dict(bytes=row.get('bytes',row.get('generation',{}).get('bytes')),sha256=row['sha256'])
        assert identity(raw) == wanted,row['path']
        return raw
    design,inputs,frozen = (read('frozen/'+name+'.json') for name in ('design','inputs','frozen'))
    assert digest(data['frozen/frozen.json']) == FROZEN_SHA256
    prepared = read('frozen/native-preparation/complete.json')
    assert len(design['cases']) == frozen['total'] == 10000
    assert bound(frozen['design']) == data['frozen/design.json']
    assert bound(frozen['inputs']) == data['frozen/inputs.json']
    source_manifest = read('frozen/investigation-original-harness-source-r1.json')
    assert source_manifest['commit'] == '0155355e7927e6f8a4747daae5a6daf3438e822b'
    assert source_manifest['published_frozen_sha256'] == digest(data['frozen/frozen.json'])
    assert len(frozen['harness']) == len(source_manifest['harness_sha256']) == 65
    for row in frozen['harness']:
        raw = bound(row)
        relative = report['locations'][row['path']].removeprefix('harness/')
        assert source_manifest['harness_sha256'][relative] == digest(raw)
    for row in frozen['references']:
        bound(row)
    for name,raw in data.items():
        if name.endswith('/attempts.sqlite') or (name.startswith('frozen/reference-') and name.endswith('.sqlite')):
            path = temporary/name
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(raw)
    harness = temporary/'harness'
    source_bytes = replay_sources(data)
    for name,raw in source_bytes.items():
        path = harness/name
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(raw)
    sys.path[:0] = [str(harness/'scripts'),str(harness/'python/src'),str(harness/'benchmarks/traditional_analytics')]
    from query_surface.corpus import corpus
    from query_surface.expanded_catalog import build,spaces
    from query_surface.expanded_freeze import unpack
    from query_surface.expanded_report import raw_files,replay
    from query_timing_report import query_phase_timings
    assert not any(name == 'shardloom._native' or name.startswith('shardloom._native.') for name in sys.modules)
    for name,module in list(sys.modules.items()):
        if name.startswith(('query_surface','shardloom')) or name in (
                'query_timing_report','native_workflow_protocol','local_uat_storage',
                'resources','run_clickbench_query_uat','timed_native_command',
                'clickbench_reference_packet','clickbench_native_sql'):
            path = Path(module.__file__).resolve()
            assert path.is_relative_to(harness)
            assert path.read_bytes() == source_bytes[path.relative_to(harness).as_posix()]
    selected = read('current/correctness-cohort-selection-r1.json')
    protocol = read('current/correctness-cohort-protocol-r1.json')
    verified = read('current/correctness-cohort-r1/verified-correctness-r1.json')
    old_report = read('original/current-baseline-10k-r1/failed-observation-replay-r1.json')
    old_failures = {row['seq']:row for row in old_report['failures']}
    assert old_report['verification_passed'] and old_report['original_outcomes'] == ORIGINAL_OUTCOMES
    assert old_report['source_commit'] == BASE and old_report['observed_attempts'] == 10000
    assert old_report['complete_performance_result_valid'] is False and old_report['engine_execution_seconds'] is None
    assert selected['status'] == 'selected_before_candidate_execution' and selected['query_executions'] == 0
    assert selected['selection_uses_timing'] is False and selected['selected_requests'] == 517
    assert protocol['selected_count'] == 517 and protocol['attempts_per_case'] == 1 and protocol['warmups'] == 0
    assert protocol['complete_10k_performance_claim'] is False
    assert bound(protocol['selection']) == data['current/correctness-cohort-selection-r1.json']
    assert selected['cases'] == sorted(selected['cases'],key=lambda row:row['seq'])
    assert len({row['seq'] for row in selected['cases']}) == 517
    for row in selected['cases']:
        assert row['case'] == design['cases'][row['seq']]
    assert verified['status'] == 'passed' and verified['all_selected_results_correct'] and verified['native_no_fallback']
    assert verified['candidate_outcomes'] == {'passed':517} and verified['original_failed_cases'] == 96
    assert verified['passing_controls'] == 421 and verified['complete_10k_performance_claim'] is False
    assert verified['runtime_binding'] == protocol['runtime_binding']
    runtime = read('current/runtime-r1-identity.json')
    identities = {Path(row['path']).name:row for row in runtime['files']}
    candidate_binding = protocol['runtime_binding']
    assert candidate_binding['source_head'] == BASE
    assert candidate_binding['source_fingerprint_sha256'] == report['source_fingerprint_sha256']
    assert bound(candidate_binding['source_build_receipt']) == data['current/release-build-r1.json']
    for role,name in (('binary','shardloom'),('library','lib_native.dylib')):
        row = candidate_binding['runtime'][role]
        assert dict(bytes=row['generation']['bytes'],sha256=row['sha256']) == {k:identities[name][k] for k in ('bytes','sha256')}
    for row in candidate_binding['proof_files']:
        bound(row)
    assert bound(verified['selection']) == data['current/correctness-cohort-selection-r1.json']
    assert bound(verified['protocol']) == data['current/correctness-cohort-protocol-r1.json']
    specifications = {spec.name:spec for spec in spaces()}
    originals = {item.id:item for item in corpus(interactions='all',include_pending=False)}
    results = {}
    with ExitStack() as stack:
        def database(name):
            conn = sqlite3.connect('file:'+str(temporary/name)+'?mode=ro',uri=True)
            stack.callback(conn.close)
            return conn
        refs = {profile:database('frozen/reference-'+profile+'/expectations.sqlite')
            for profile in {row['profile'] for row in design['cases']}}
        for role,prefix,rows,wanted in (
            ('original','original/current-baseline-10k-r1',design['cases'],ORIGINAL_OUTCOMES),
            ('candidate','current/correctness-cohort-r1',[row['case'] for row in selected['cases']],{'passed':517}),
        ):
            done,binding = read(prefix+'/complete.json'),read(prefix+'/binding.json')
            assert done['status'] == 'complete_observation' and done['stage'] == 'native'
            assert done['total'] == done['attempt_slots'] == len(rows)
            assert done['attempts_per_case'] == 1 and done['requested_repeats'] == 0 and done['posthash_verified']
            assert done['outcomes'] == wanted
            assert binding['contract'] == {key:done[key] for key in ('frozen','stage','prerequisites','updated')}
            assert bound(done['ledger']) == data[prefix+'/attempts.sqlite']
            assert bound(done['binding']) == data[prefix+'/binding.json']
            guard = read(('original/current-baseline-10k-r1' if role == 'original' else 'current/correctness-cohort-r1')+'.json')
            assert guard['status'] == 'passed' and guard['returncode'] == 0
            assert guard['process_group_drained'] and guard['direct_child_reaped'] and guard['query_workload']
            assert guard['build_inventory_policy'] == guard['evidence_inventory_policy'] == 'before_and_after_only'
            assert guard['log_sha256'] == digest(data[('original/current-baseline-10k-r1' if role == 'original' else 'current/correctness-cohort-r1')+'.log'])
            if role == 'candidate':
                assert done['updated'] == protocol['runtime_binding']
                assert guard['build_source_fingerprint_sha256'] == report['source_fingerprint_sha256']
                assert bound(verified['complete']) == data[prefix+'/complete.json']
            config = dict(inputs,runtime=done['updated']['runtime'])
            entries = iter(database(prefix+'/attempts.sqlite').execute('SELECT seq,id,status,packet FROM attempt ORDER BY seq'))
            observed = []
            for seq,row in enumerate(rows):
                item = build(row,specifications) if 'cell' in row else originals[row['id']]
                ref = refs[row['profile']].execute('SELECT identity,value FROM expected WHERE id=?',(row['id'],)).fetchone()
                assert ref and ref[0] == item.identity == row['case_identity']
                entry = next(entries,None)
                assert entry is not None and entry[:2] == (seq,item.id)
                record = replay(entry,row,'native',unpack(ref[1]),item,config,prepared,design['execution'])
                assert record['status'] == entry[2]
                files = raw_files(unpack(entry[3]))
                if record['status'] == 'passed':
                    observation = json.loads(files['one.stdout.json'])['observation']
                    clocks = observation['clocks']
                    fields = query_phase_timings(observation['fields'],clocks['complete_workflow_seconds'])
                    assert clocks['engine_timing_reported'] is True
                    assert fields['engine_execution_seconds'] == clocks['engine_execution_seconds'] == record['engine_seconds']
                    assert fields['result_serialization_seconds'] == clocks['result_serialization_seconds']
                    assert type(record['engine_seconds']) in (int,float) and math.isfinite(record['engine_seconds']) and record['engine_seconds'] >= 0
                else:
                    assert role == 'original' and record['engine_seconds'] is None
                    failure = old_failures[seq]
                    assert failure['id'] == item.id and failure['case_identity'] == item.identity
                    assert failure['status'] == record['status']
                    assert failure['stderr'] == files['one.stderr.txt'].decode('utf-8')
                    assert failure['packet_blob_sha256'] == digest(entry[3])
                observed.append(dict(record=record,job_sha256=digest(files['one.job.json']),id=item.id))
            assert next(entries,None) is None
            assert dict(Counter(r['record']['status'] for r in observed)) == wanted
            results[role] = observed
    failed = {seq for seq,row in enumerate(results['original']) if row['record']['status'] != 'passed'}
    assert len(failed) == 96 and failed == {row['seq'] for row in selected['cases'] if row['previous_status'] != 'passed'}
    assert failed == {row['seq'] for row in old_report['failures']}
    affected = {design['cases'][seq]['archetype'] for seq in failed}
    related,broad = {},{}
    reasons = defaultdict(list)
    for seq in failed:
        reasons[seq].append('original_failed_request')
    for seq,row in enumerate(design['cases']):
        if seq in failed:
            continue
        if row.get('archetype') in affected:
            related.setdefault((row['archetype'],row['surface'],row['profile'],row['native_format']),seq)
        broad.setdefault((row['family'],row['surface'],row['native_format']),seq)
    for seq in related.values():
        reasons[seq].append('first_passing_affected_archetype_surface_profile_format')
    for seq in broad.values():
        reasons[seq].append('first_passing_family_surface_format')
    assert len(related) == selected['related_control_cells'] == 269
    assert len(broad) == selected['family_surface_format_controls'] == 236
    assert sorted(reasons) == [row['seq'] for row in selected['cases']]
    assert sorted(affected) == selected['affected_archetypes']
    saved = [json.loads(line) for line in gzip.decompress(bound(verified['case_rows'])).splitlines()]
    assert len(saved) == 517
    old_controls,new_controls = [],[]
    for seq,(selection,row) in enumerate(zip(selected['cases'],saved)):
        original = results['original'][selection['seq']]
        candidate = results['candidate'][seq]
        assert row['seq'] == seq and row['original_seq'] == selection['seq'] and row['id'] == candidate['id'] == original['id']
        assert row['baseline'] == original['record'] and row['candidate'] == candidate['record']
        assert row['job_sha256'] == dict(baseline=original['job_sha256'],candidate=candidate['job_sha256'])
        assert selection['previous_job_sha256'] == original['job_sha256']
        assert selection['previous_status'] == original['record']['status']
        assert selection['reasons'] == row['reasons'] == reasons[selection['seq']]
        if original['record']['status'] == 'passed':
            old_controls.append(original['record']['engine_seconds'])
            new_controls.append(candidate['record']['engine_seconds'])
    assert len(old_controls) == 421
    assert math.fsum(r['record']['engine_seconds'] for r in results['candidate']) == verified['engine_execution_seconds']
    assert verified['matched_passing_control_diagnostic'] == dict(cases=421,
        candidate_engine_seconds=math.fsum(new_controls),baseline_engine_seconds=math.fsum(old_controls),performance_acceptance=False)
    return dict(original_attempts=10000,original_outcomes=ORIGINAL_OUTCOMES,repaired_cases=96,
        passing_controls=421,candidate_outcomes={'passed':517},query_executions=0,
        complete_10k_performance_claim=False)


def verify_controls(packet,data):
    controls = json.loads((packet/'corruption-controls.json').read_bytes())
    assert controls['schema'] == 'shardloom.dictionary_packet_controls.v1'
    assert controls['status'] == 'passed' and controls['query_executions'] == 0
    assert controls['packet_manifest'] == identity((packet/'acceptance.json').read_bytes())
    assert controls['verifier'] == identity(Path(__file__).read_bytes())
    assert controls['control_source'] == identity(data['current/tools/check_dictionary_packet_controls.py'])
    assert len(controls['cases']) == len(CONTROL_NAMES)
    assert {row['name'] for row in controls['cases']} == CONTROL_NAMES
    for row in controls['cases']:
        if row['name'] == 'relocated_source_only':
            assert row['status'] == 'passed'
            replayed = row['replay']
            assert replayed['status'] == 'passed' and replayed['query_executions'] == 0
            assert replayed['source_fingerprint_sha256'] == fingerprint(json.loads(
                data['current/release-build-r1.json'])['build_sources'])
            assert replayed['original_attempts'] == 10000 and replayed['original_outcomes'] == ORIGINAL_OUTCOMES
            assert replayed['candidate_outcomes'] == {'passed':517}
            assert replayed['repaired_cases'] == 96 and replayed['passing_controls'] == 421
            assert replayed['complete_10k_performance_claim'] is False
        else:
            assert row['status'] == 'rejected'


def verify(packet,repo=None,require_controls=False):
    report,data = read_archive(packet)
    verify_sources(report,data,repo)
    if require_controls:
        verify_controls(packet,data)
    with tempfile.TemporaryDirectory(prefix='shardloom-dictionary-replay-') as folder:
        replayed = replay_saved(report,data,Path(folder))
    return dict(status='passed',source_fingerprint_sha256=report['source_fingerprint_sha256'],**replayed)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root',type=Path)
    parser.add_argument('--require-controls',action='store_true')
    args = parser.parse_args()
    assert __debug__
    print(json.dumps(verify(args.packet,args.repo_root,args.require_controls),sort_keys=True))
