#!/usr/bin/env python3
"""Replay saved attribution evidence without a native executable or source data."""
from __future__ import annotations
import argparse
import collections
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import tempfile


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def verify(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    assert manifest['schema'] == 'shardloom.current_attribution_archive.v1'
    mapping = {}
    for entry in manifest['files']:
        relative = PurePosixPath(entry['member'])
        assert not relative.is_absolute() and '..' not in relative.parts
        assert entry['original_path'] not in mapping
        path = root / relative
        raw = path.read_bytes()
        assert len(raw) == entry['bytes'] and digest(raw) == entry['sha256'], entry['member']
        mapping[entry['original_path']] = path

    def bound(reference):
        path = mapping[reference['path']]
        raw = path.read_bytes()
        assert digest(raw) == reference['sha256'], reference['path']
        if 'bytes' in reference:
            assert len(raw) == reference['bytes']
        return path

    def read(name):
        return json.loads((root / 'evidence' / name).read_text())

    sys.path.insert(0, str(root / 'repo' / 'scripts'))
    from clickbench_reference_packet import load_reference_packet, canonical_values_sha256
    from native_workflow_protocol import extract_result, report_fields, strict_json
    from run_clickbench_query_uat import equivalent
    from query_timing_report import query_phase_timings

    review = read('current-attribution-review-956.json')
    protocol = strict_json(bound(review['protocol']).read_text())
    result = strict_json(bound(review['result']).read_text())
    inventory = strict_json(bound(review['inventory']).read_text())
    independent = strict_json(bound(review['independent_tree_verification']).read_text())
    symbol_rows = inventory['demangler']['mapping']
    symbol_map = {row['original_symbol']: row['demangled_output'] for row in symbol_rows}
    assert len(symbol_rows) == len(symbol_map) == inventory['demangler']['input_symbol_count']
    assert list(symbol_map) == sorted(symbol_map)
    source = strict_json(bound(protocol['source']).read_text())
    runtime = strict_json(bound(protocol['runtime']).read_text())
    build = strict_json(bound(protocol['build']).read_text())
    assert source['compiled_source_equal'] and build['status'] == runtime['status'] == 'passed'
    assert build['source_before'] == build['source_after']
    assert protocol['binary'] == runtime['executable']
    assert review['binary'] == protocol['binary'] and review['input'] == protocol['input']
    assert review['source_binding'] == protocol['source']
    for row in source['source_files']:
        if row['relative_path'] in build['source_after']:
            assert row['sha256'] == build['source_after'][row['relative_path']]
    assert protocol['reference_kind'] == 'independent_reference'
    reference = load_reference_packet(bound(protocol['reference']), bound(protocol['queries']))
    assert reference['packet']['reference_kind'] == 'independent_reference'
    assert protocol['query_ids'] == [1, 3, 7, 19, 23, 29, 34, 41]
    assert protocol['total_native_calls'] == len(result['records']) == len(review['calls']) == 16
    assert protocol['memory_gb'] == 24 and protocol['requested_parallelism'] == 12
    assert protocol['calls_per_query'] == 2 and protocol['no_repeats_within_role'] is True
    assert protocol['roles'] == ['counter-control', 'sampled']
    assert result['status'] == 'passed' and result['independent_reference'] is True
    assert result['source_and_binary_unchanged'] and result['input_hash_before_after']
    assert result['uat_lock_released'] and result['complete_values_checks'] == 16
    for data in (result, review):
        assert data['fallback_attempted'] is False and data['external_engine_invoked'] is False
    assert review['runtime_changed'] is False and review['performance_claim'] is False
    expected_keys = [(query, sampled) for query in protocol['query_ids'] for sampled in (False, True)]
    actual_keys = []
    for record, summarized in zip(result['records'], review['calls']):
        key = (record['query'], record['sample_requested'])
        actual_keys.append(key)
        assert key == (summarized['query'], summarized['sampled'])
        assert record['status'] == 'passed' and record['complete_values_match'] is True
        assert record['process_group_drained'] and record['direct_child_reaped']
        assert record['returncode'] == 0 and not record['timed_out']
        assert record['interrupted_signal'] is None and not record['group_absence_unproved_at_cleanup']
        assert record['statement'] == reference['queries'][record['query'] - 1]
        members = [bound(item) for item in record['files']]
        output = next(path for path in members if path.name.endswith('.stdout.json'))
        envelope = strict_json(output.read_text())
        fields = report_fields(envelope)
        assert fields['fallback_attempted'] == 'false'
        assert fields['external_engine_invoked'] == 'false'
        values = extract_result(envelope)
        assert equivalent(values, reference['values'][record['query']]), key
        assert canonical_values_sha256(values) == record['result_sha256']
        assert len(values) == record['result_rows'] == summarized['result_rows']
        timing = strict_json(bound(record['native_timing']).read_text())
        assert timing['native_reaped'] and timing['sampler_reaped'] and timing['returncode'] == 0
        phase = query_phase_timings(fields, timing['seconds'], allow_missing=False)
        assert phase == record['timings']
        assert phase['engine_execution_seconds'] == summarized['engine_execution_seconds']
        for native, label in [('ri_instructions', 'whole_process_instructions'), ('ri_cycles', 'whole_process_cycles')]:
            assert timing['darwin_rusage_v4'][native] > 0
            assert timing['darwin_rusage_v4'][native] == summarized[label]
        assert timing['counter_scope'] == summarized['counter_scope']
        for member in timing.get('sampling_files', []):
            bound(member)
    assert actual_keys == expected_keys
    for binding in review['source_bindings']:
        path = bound(binding)
        text = '\n'.join(path.read_text().splitlines()[binding['first_line'] - 1:binding['last_line']])
        assert text == binding['excerpt']
        relative = path.relative_to(root / 'repo').as_posix()
        frozen = next(row for row in source['source_files'] if row['relative_path'] == relative)
        assert frozen['sha256'] == binding['sha256']

    # Reconstruct ancestry from original printed indentation. Do not trust an
    # inventory count merely because its producer recorded successful validation.
    total_frames = total_threads = total_roots = total_exclusive = 0
    summaries = {lane['query']: lane for lane in review['lanes']}
    assert set(summaries) == {3, 7, 19, 23, 29, 34, 41}
    assert [entry['file'] for entry in inventory['files']] == [f'q{q:02d}-sampled.sample.txt' for q in (3, 7, 19, 23, 29, 34, 41)]
    for sample in inventory['files']:
        path = mapping[sample['source_path']]
        assert digest(path.read_bytes()) == sample['source_sha256']
        lines = path.read_text().splitlines()
        start = lines.index('Call graph:') + 1
        stop = next(index for index in range(start, len(lines)) if lines[index].startswith('Total number in stack'))
        threads, frame_rows, active, ancestors = [], {}, None, []
        for index in range(start, stop):
            line = lines[index]
            if not line.strip():
                continue
            header = re.match(r'^\s*(\d+)\s+(Thread_\S+)(.*)$', line)
            if header:
                active = dict(count=int(header[1]), name=header[2], suffix=header[3], nodes=[])
                threads.append(active)
                ancestors = []
                continue
            match = re.search(r'(\d+)\s+(\S.*)$', line)
            assert active is not None and match is not None
            while ancestors and ancestors[-1]['column'] >= match.start(1):
                ancestors.pop()
            node = dict(line=index + 1, count=int(match[1]), column=match.start(1),
                        parent=ancestors[-1]['line'] if ancestors else None,
                        depth=len(ancestors), children=0)
            frame_rows[node['line']] = node
            active['nodes'].append(node)
            ancestors.append(node)
        for node in frame_rows.values():
            if node['parent'] is not None:
                frame_rows[node['parent']]['children'] += node['count']
        originals = {node['line']: node for thread in sample['threads'] for node in thread['frames']}
        assert set(originals) == set(frame_rows)
        for line, node in frame_rows.items():
            prior = originals[line]
            assert prior['original_line_text'] == lines[line - 1]
            raw_frame = re.search(r'(\d+)\s+(\S.*)$', lines[line - 1])[2]
            raw_symbol = raw_frame.split()[0]
            assert prior['full_frame_text'] == raw_frame and prior['function_symbol'] == raw_symbol
            assert prior['demangled_function_symbol'] == symbol_map[raw_symbol]
            assert prior['parent_line'] == node['parent'] and prior['depth'] == node['depth']
            assert prior['observation_count'] == node['count']
            assert prior['immediate_child_count_sum'] == node['children']
            node['exclusive'] = node['count'] - node['children']
            assert node['exclusive'] >= 0 and prior['exclusive_count'] == node['exclusive']
        roots = sum(node['count'] for node in frame_rows.values() if node['parent'] is None)
        exclusive = sum(node['exclusive'] for node in frame_rows.values())
        assert roots == exclusive == sample['exclusive_frame_count_sum'] == sample['root_thread_frame_count_sum']
        assert len(threads) == sample['thread_count'] and len(frame_rows) == sample['frame_count']
        query = int(sample['file'][1:3])
        summary = summaries[query]
        assert bound(summary['raw']) == path
        assert len(summary['threads']) == len(threads)
        for thread, recorded, reviewed in zip(threads, sample['threads'], summary['threads']):
            assert thread['name'] == recorded['thread_name'] == reviewed['name']
            assert thread['count'] == recorded['observation_count'] == reviewed['observations']
            assert thread['suffix'] == recorded['header_suffix'] == reviewed['suffix']
            assert reviewed['main_thread'] == ('com.apple.main-thread' in thread['suffix'])
            symbols = collections.Counter()
            for node in thread['nodes']:
                symbols[originals[node['line']]['demangled_function_symbol']] += node['exclusive']
            assert reviewed['top_exclusive'] == [dict(symbol=s, observations=n) for s, n in symbols.most_common(12)]
            def within(symbol, ancestor):
                amount = 0
                for node in thread['nodes']:
                    if symbol not in originals[node['line']]['function_symbol']:
                        continue
                    current = node
                    while current is not None:
                        if ancestor in originals[current['line']]['function_symbol']:
                            amount += node['exclusive']
                            break
                        current = frame_rows.get(current['parent'])
                return amount
            assert reviewed['zstd_exclusive_under_serial_aggregate'] == within('ZSTD_', 'update_compact_direct_from_chunk_profiled')
            assert reviewed['numeric_value_exclusive'] == within('aggregate_direct_numeric_value', 'update_direct_from_chunk')
            assert reviewed['numeric_kind_exclusive'] == within('numeric_input_is_integer', 'update_direct_from_chunk')
        assert sample['empty_graph'] == summary['empty_graph'] == (len(frame_rows) == 0)
        if query in (19, 23, 29, 34):
            main = [thread for thread in threads if 'com.apple.main-thread' in thread['suffix']]
            assert len(main) == 1 and main[0]['count'] >= 100
            assert any('shardloom_vortex' in node['demangled_function_symbol'] for node in originals.values())
        total_frames += len(frame_rows)
        total_threads += len(threads)
        total_roots += roots
        total_exclusive += exclusive
    assert total_frames == 15209 and total_threads == 147
    assert total_roots == total_exclusive == 244781
    assert independent['mismatch_count'] == 0 and independent['totals']['frames'] == total_frames
    assert inventory['missing_sample_files'][0]['file'] == 'q01-sampled.sample.txt'
    assert inventory['missing_sample_files'][0]['evidence_sha256'] == review['result']['sha256']
    assert result['sample_capture']['1']['sample_file'] is None
    assert result['sample_capture']['7']['main_thread_observations'] == 0
    assert result['attribution_lane_admission'] == {str(q): True for q in (19, 23, 29, 34)}
    return dict(status='passed', native_calls=16, complete_value_checks=16,
                raw_sample_files=7, frames=total_frames, threads=total_threads,
                exclusive_observations=total_exclusive, source_excerpts=len(review['source_bindings']),
                scope='Saved records replayed; no engine, source data or compiler executed.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='shardloom-attribution-replay-') as temporary:
        root = Path(temporary)
        with tarfile.open(args.archive, 'r:gz') as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            assert len(names) == len(set(names))
            for member in members:
                name = PurePosixPath(member.name)
                assert member.isfile() and not name.is_absolute() and '..' not in name.parts
                assert member.size <= 32 * 1024 * 1024
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                stream = archive.extractfile(member)
                assert stream is not None
                path.write_bytes(stream.read())
        print(json.dumps(verify(root), indent=2))


if __name__ == '__main__':
    main()
