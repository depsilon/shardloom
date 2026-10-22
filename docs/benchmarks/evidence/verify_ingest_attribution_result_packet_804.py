"""Reopen the compact result packet without original payloads or live source edits.

Requires the recorded Git base in --repository and a local-only --scratch root.
This replays source reconstruction and recorded aggregate decisions, not retired
event traces or physical Vortex outputs. Only frozen pure checker functions are
loaded from the hash-bound packet; no measurement helper is executed.
"""
import argparse
import ast
import base64
from fractions import Fraction
import gzip
import hashlib
import json
import lzma
from pathlib import Path
import statistics
import subprocess
import tempfile

EXPECTED_PACKET = '4ef023256c1d16fb5a90312b67d800c1fe3560d78dbc2dda11ae3ad430477432'
ORIGINAL_ROOT = '/Users/dylan/LocalData/shardloom/conditional-investigations-20261019/'
BASE = 'ce9428fc1239ce9b6a65d55d857e05d46c878f97'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    scratch = args.scratch.resolve(strict=True)
    assert scratch.is_dir() and not any(part in ('Desktop', 'Documents', 'CloudDocs', 'Mobile Documents') for part in scratch.parts)
    compressed = args.packet.read_bytes()
    assert sha(compressed) == EXPECTED_PACKET and len(compressed) == 4409808
    payload = json.loads(lzma.decompress(compressed))
    assert payload['schema_version'] == 'shardloom.ingest_attribution_results.v1'
    assert payload['base_commit'] == BASE and payload['scheduling_change_retained'] is False
    members = {}
    for entry in payload['members']:
        assert entry['path'] not in members
        raw = base64.b64decode(entry['base64'], validate=True)
        assert len(raw) == entry['bytes'] and sha(raw) == entry['sha256']
        members[entry['path']] = raw
    assert len(members) == 2151 and sum(map(len, members.values())) == 42905871
    traces = payload['event_trace_identities']
    assert len(traces) == len({entry['path'] for entry in traces}) == 184
    assert all(entry['path'] not in members for entry in traces)

    def raw_at(relative):
        return members[ORIGINAL_ROOT + relative]

    def read_at(relative):
        return json.loads(raw_at(relative))

    available = {sha(raw) for raw in members.values()}
    for label in payload['attempts']:
        freeze = read_at(label + '/measurement-freeze.json')
        assert all(ref['sha256'] in available for ref in freeze['helpers'])
        assert freeze['source_freeze_sha256'] in available
        assert freeze['artifact_binding_sha256'] in available

    # The entire packet is pinned above. Extract just four pure functions from
    # its frozen independent evaluator, avoiding all local-path module imports.
    names = {'natural', 'verify_admission', 'verify_activation', 'decide'}
    parsed = ast.parse(raw_at('verify_ingest_overlap_802.py'))
    selected = [node for node in parsed.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in selected} == names and len(selected) == len(names)
    namespace = dict(Fraction=Fraction, statistics=statistics, GIB=1 << 30)
    exec(compile(ast.Module(body=selected, type_ignores=[]), 'frozen_checker', 'exec'), namespace)

    base_hashes, reconstructed = {}, []
    with tempfile.TemporaryDirectory(prefix='ingest-result-reopen-', dir=scratch) as tmp:
        for label in payload['source_sets']:
            freeze = read_at(label + '/source-freeze.json')
            assert freeze['head'] == BASE
            patch = raw_at(label + '/tracked.patch')
            assert sha(patch) == freeze['patch_sha256']
            work = Path(tmp) / label
            work.mkdir()
            patch_path = work / 'frozen.patch'
            patch_path.write_bytes(patch)
            stats = subprocess.check_output(['git', 'apply', '--numstat', '--', str(patch_path)], cwd=work, text=True)
            changed = []
            for line in stats.splitlines():
                name = line.split('\t', 2)[2]
                relative = Path(name)
                assert not relative.is_absolute() and '..' not in relative.parts
                target = work / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=args.repository))
                changed.append(name)
            subprocess.run(['git', 'apply', '--check', '--', str(patch_path)], cwd=work, check=True)
            subprocess.run(['git', 'apply', '--', str(patch_path)], cwd=work, check=True)
            for name in freeze['extras']:
                relative = Path(name)
                assert not relative.is_absolute() and '..' not in relative.parts
                target = work / relative
                assert not target.exists()
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw_at(label + '/extra/' + name))
            for row in freeze['sources']:
                target = work / row['path']
                if target.exists():
                    raw = target.read_bytes()
                    actual = len(raw), sha(raw)
                else:
                    if row['path'] not in base_hashes:
                        raw = subprocess.check_output(['git', 'show', BASE + ':' + row['path']], cwd=args.repository)
                        base_hashes[row['path']] = len(raw), sha(raw)
                    actual = base_hashes[row['path']]
                assert actual == (row['bytes'], row['sha256']), (label, row['path'])
            reconstructed.append(dict(label=label, source_files=len(freeze['sources']),
                                      changed_tracked_files=len(changed), extra_files=len(freeze['extras'])))

    label = 'ingest-overlap-801-a1'
    freeze = read_at(label + '/measurement-freeze.json')
    decision = read_at(label + '/decision.json')
    assert sha(raw_at(label + '/decision.json')) == payload['decision_sha256']
    by_id = {row['case']['id']: row for row in decision['metrics']}
    assert len(by_id) == 84 and set(by_id) == {case['id'] for case in freeze['cases']}
    preflights = 0
    for case in [*freeze['preflight_cases'], *freeze['cases']]:
        result = json.loads(gzip.decompress(raw_at(label + '/' + case['id'] + '/result.json.gz')))
        assert result['config']['case'] == case and result['status'] == 'passed'
        assert result['fallback_attempted'] is False and result['external_engine_invoked'] is False
        namespace['verify_activation'](result, case['cell']['parallelism'], len(result['geometry']['batch_rows']))
        assert result['native_final_bytes'] == result['native_denied_reservations'] == result['owned_staging_remaining'] == 0
        if case['cohort'] == 'preflight':
            preflights += 1
            continue
        metric = by_id[case['id']]
        assert metric['complete_ingest_nanos'] == result['complete_ingest_nanos']
        assert metric['artifact_sha256'] == result['artifact_sha256']
        if case['role'] == 'observed':
            drivers = result['observation']['admitted_drivers']
            opportunity = sum((drivers - occupied) * duration
                for child in result['observation']['children']
                for occupied, duration in enumerate(child['ready_successor_cpu_queue_empty_wall_nanos']))
            assert opportunity == metric['opportunity_driver_nanos']
            assert drivers * result['complete_ingest_nanos'] == metric['denominator_driver_nanos']
    assert preflights == 14
    recomputed = namespace['decide'](decision['metrics'], read_at('ingest-overlap-attribution-protocol-797.json'))
    assert all(decision[key] == value for key, value in recomputed.items())
    assert recomputed['outcome'] == payload['current_outcome'] == 'inconclusive_observer_overhead_gate_failed'
    assert recomputed['candidate_design_admitted'] is False
    result = dict(status='passed', packet_sha256=EXPECTED_PACKET, members_verified=len(members),
        source_sets_reconstructed=reconstructed, unchanged_git_sources_hashed=len(base_hashes),
        preflight_records_verified=preflights, measured_records_recomputed=len(by_id),
        original_member_files_required=False, original_working_tree_changes_required=False,
        raw_event_traces_replayed=False, raw_event_trace_identities_indexed=len(traces),
        physical_artifacts_reopened=False, current_outcome=recomputed['outcome'],
        candidate_design_admitted=False, scheduling_change_retained=False,
        temporary_sources_removed=True)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
