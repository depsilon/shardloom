#!/usr/bin/env python3
"""Replay the accepted-engine restoration and its isolated Full43 from saved bytes."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import sys
import tarfile
import tempfile

if not __debug__:
    raise RuntimeError('optimized Python is unsupported: replay requires semantic assertions')

MAX_MEMBER_BYTES = 32 << 20
MAX_ARCHIVE_BYTES = 128 << 20
MAX_ARCHIVE_MEMBERS = 1024

REGION_SHA = 'd79e24b01ee9d04efa0167895fce6d41f072c4053c14f2a8674bf9db21c81c9d'
PROTOCOL_SHA = '5fdd1589bb3ec01799ba55b2a7f42c759ce84ac39d9a9cba82e1f0d2294507ab'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def authenticate_helpers(root):
    # Trust lives in this reviewed verifier, never in the packet's manifest.
    # Bind every file in both import roots, including transitive package imports
    # and unexpected files that could shadow a standard-library module.
    identities = {}
    for relative in ('repo/scripts', 'repo/python/src'):
        base = root / relative
        if not base.is_dir() or base.is_symlink():
            raise ValueError('archived helper identity mismatch')
        for path in base.rglob('*'):
            if path.is_symlink():
                raise ValueError('archived helper identity mismatch')
            if path.is_file():
                identities[path.relative_to(root).as_posix()] = digest(path.read_bytes())
    encoded = json.dumps(identities, sort_keys=True, separators=(',', ':')).encode()
    if digest(encoded) != 'd24ca1652f75b1f3585a22a5931c465eb83252aa7e472710470beb6b3a52d5c0':
        raise ValueError('archived helper identity mismatch')

    # Keep repeated in-process replay from adding unauthenticated .pyc files.
    sys.dont_write_bytecode = True


class LimitedArchiveStream:
    """Bound decompression, including extended headers parsed by tarfile."""
    def __init__(self, stream):
        self.stream = stream
        self.remaining = MAX_ARCHIVE_BYTES + MAX_ARCHIVE_MEMBERS * 8192 + 10240

    def read(self, size=-1):
        limit = self.remaining + 1
        raw = self.stream.read(limit if size < 0 else min(size, limit))
        self.remaining -= len(raw)
        if self.remaining < 0:
            raise ValueError('archive decompressed stream exceeds limit')
        return raw


def read_gzip_payload(path):
    with gzip.open(path, 'rb') as stream:
        raw = stream.read(MAX_MEMBER_BYTES + 1)
    if len(raw) > MAX_MEMBER_BYTES:
        raise ValueError('nested gzip payload exceeds limit')
    return raw


def unpack(packet, root):
    seen, total = set(), 0
    # Stream headers so the member limit is checked before allocating an
    # arbitrary complete index. All extraction guards remain active under -O.
    with gzip.open(packet, 'rb') as compressed, \
            tarfile.open(fileobj=LimitedArchiveStream(compressed), mode='r|') as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if (not member.isfile() or not path.parts or path.is_absolute()
                    or '..' in path.parts or chr(92) in member.name
                    or any(':' in part for part in path.parts) or len(member.name) > 4096):
                raise ValueError('unsafe archive member')
            name = path.as_posix()
            if name in seen:
                raise ValueError('duplicate archive member')
            if len(seen) >= MAX_ARCHIVE_MEMBERS:
                raise ValueError('archive member count exceeds limit')
            if not 0 <= member.size <= MAX_MEMBER_BYTES:
                raise ValueError('archive member size exceeds limit')
            if total + member.size > MAX_ARCHIVE_BYTES:
                raise ValueError('archive total size exceeds limit')
            seen.add(name)
            total += member.size
            raw = archive.extractfile(member).read()
            if len(raw) != member.size:
                raise ValueError('archive member length mismatch')
            destination = root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)


def members(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    mapping = {}
    names = {'manifest.json'}
    for entry in manifest['files']:
        path = PurePosixPath(entry['member'])
        assert not path.is_absolute() and '..' not in path.parts
        assert entry['member'] not in names and entry['original_path'] not in mapping
        raw = (root / path).read_bytes()
        assert len(raw) == entry['bytes'] and digest(raw) == entry['sha256']
        mapping[entry['original_path']] = root / path
        names.add(entry['member'])
    assert {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()} == names
    return manifest, mapping


def verify(root, region_root):
    manifest, mapping = members(root)
    authenticate_helpers(region_root)
    region_manifest, region_mapping = members(region_root)
    assert manifest['schema'] == 'shardloom.scalar_region_restoration_archive.v1'
    assert manifest['rejected_packet_sha256'] == REGION_SHA

    def bound(reference):
        path = mapping.get(reference['path'], region_mapping.get(reference['path']))
        assert path is not None, reference['path']
        raw = path.read_bytes()
        assert digest(raw) == reference['sha256']
        assert len(raw) == reference.get('bytes', len(raw))
        return path

    def read(reference):
        return json.loads(bound(reference).read_text())

    assert region_manifest['protocol']['sha256'] == PROTOCOL_SHA
    protocol = read(region_manifest['protocol'])
    baseline = protocol['baseline_runtime_sources']
    restored = read(manifest['restored'])
    runtime = read(restored['runtime'])
    build = read(runtime['build'])
    assert runtime['executable'] == protocol['baseline']['executable'] == restored['binary']
    assert build['status'] == 'passed' and build['process_group_drained']
    assert build['source_before'] == build['source_after'] == baseline
    assert len(baseline) == 1922
    restoration = read(restored['restoration'])
    assert restoration['status'] == 'passed' and restoration['source_exactly_equals_frozen_baseline']
    assert restoration['restored_native_and_cargo_source_files'] == len(baseline)
    assert restoration['no_candidate_retained'] and restoration['git_clean']
    assert restoration['baseline_executable'] == restored['binary']
    assert restoration['source_commit'] == restored['restored_source_commit']
    assert len(restored['restoration_checks']) == 3
    expected_commands = [
        ['cargo', 'fmt', '--all', '--', '--check'],
        ['cargo', 'clippy', '--workspace', '--all-targets', '--offline', '--', '-D', 'warnings'],
        ['cargo', 'test', '--workspace', '--all-targets', '--offline'],
    ]
    for reference, command in zip(restored['restoration_checks'], expected_commands):
        check = read(reference)
        assert check['command'] == command
        assert check['status'] == 'passed' and check['returncode'] == 0
        assert check['process_group_drained'] and check['direct_child_reaped']
        assert check['source_before'] == check['source_after'] == baseline
        log = mapping[str(Path(reference['path']).with_suffix('.log'))].read_bytes()
        assert digest(log) == check['log_sha256']
    plan = read(restored['plan'])
    assert plan['status'] == 'predeclared' and plan['query_ids'] == list(range(1, 44))
    assert plan['repeats'] == 1 and plan['expected_calls'] == 43
    assert not plan['parallel_work'] and not plan['extra_profiling'] and not plan['artificial_pacing']
    assert plan['restoration'] == restored['restoration']
    assert plan['restoration_checks'] == restored['restoration_checks']
    for key in ('helper', 'supervisor', 'workload_guard', 'oracle_acceptance'):
        bound(plan[key])
    assert plan['reference'] == protocol['full43_reference']
    assert plan['queries'] == protocol['full43_queries']
    assert plan['input'] == restored['input']
    assert plan['input_generation'] == restored['input_generation']

    # Only frozen parsers are shared with collection; coverage, source restoration,
    # comparisons and timing reconstruction are checked independently here.
    sys.path.insert(0, str(region_root / 'repo/scripts'))
    from clickbench_reference_packet import load_reference_packet
    from native_workflow_protocol import extract_result, report_fields, strict_json
    from query_timing_report import query_phase_timings
    from run_clickbench_query_uat import equivalent
    reference = load_reference_packet(bound(protocol['full43_reference']), bound(protocol['full43_queries']))
    cohorts = []
    for record in (read(manifest['prior']), restored):
        assert record['status'] == 'passed' and record['returncode'] == 0
        for key in ('pre_input_hash_verified', 'post_input_hash_verified', 'source_unchanged',
                    'uat_lock_released', 'process_group_drained', 'direct_child_reaped'):
            assert record[key] is True, key
        assert record['binary'] == runtime['executable'] and record['runtime'] == restored['runtime']
        assert record['input'] == restored['input'] and record['reference'] == plan['reference']
        summary = read(record['summary'])
        for key in ('complete', 'full_result_validation', 'completed_identity_check',
                    'reference_packet_identity_verified', 'harness_identity_verified'):
            assert summary[key] is True, key
        assert summary['selected_query_ids'] == list(range(1, 44)) and summary['repeats'] == 1
        assert summary['score_scope'] == 'full_43' and summary['reference_kind'] == 'independent_reference'
        assert summary['binary_sha256'] == runtime['executable']['sha256']
        assert summary['reference_packet']['sha256'] == plan['reference']['sha256']
        assert [(row['query'], row['run']) for row in summary['records']] == [(q, 1) for q in range(1, 44)]
        rows = 0
        timings = []
        for row in summary['records']:
            assert row['passed'] and row['returncode'] == 0 and row['guard_failures'] == []
            assert row['validation'] == 'complete_values' and row['engine_timing_reported']
            assert row['query_statement'] == reference['queries'][row['query'] - 1]
            prefix = str(Path(record['summary']['path']).parent / f"q{row['query']:02d}_run1")
            envelope = strict_json(read_gzip_payload(mapping[prefix + '.stdout.json.gz']).decode())
            fields = report_fields(envelope)
            assert fields['fallback_attempted'] == fields['external_engine_invoked'] == 'false'
            assert fields.get('local_primitive_aggregate_source_region_execution') not in ('true', True)
            actual = extract_result(envelope)
            assert equivalent(actual, reference['values'][row['query']])
            assert digest(json.dumps(actual, sort_keys=True, allow_nan=False).encode()) == row['result_sha256']
            native = strict_json(mapping[prefix + '.timing.json'].read_text())
            assert native['returncode'] == 0
            phase = query_phase_timings(fields, native['seconds'])
            for key, value in phase.items():
                assert row[key] == value, (row['query'], key)
            assert math.isfinite(row['engine_execution_seconds']) and row['engine_execution_seconds'] >= 0
            timings.append(row['engine_execution_seconds'])
            rows += len(actual)
        # Preserve the recorded compensated total across Python's pre/post-3.12
        # built-in float sum implementations without weakening exact equality.
        total = math.fsum(timings)
        assert rows == record['result_rows'] == 381
        assert total == record['engine_execution_seconds'] == summary['timing_totals']['totals']['engine_execution_seconds']
        cohorts.append(dict(queries=43, rows=rows, engine_execution_seconds=total, query_engine_seconds=timings))
    return dict(status='passed', restored_source_commit=restored['restored_source_commit'],
        source_files_restored=1922, required_checks=3, cohorts=cohorts,
        restored_engine_seconds=cohorts[1]['engine_execution_seconds'],
        prior_engine_seconds=cohorts[0]['engine_execution_seconds'],
        query_engine_deltas=[b - a for a, b in zip(cohorts[0]['query_engine_seconds'], cohorts[1]['query_engine_seconds'])],
        binary_reused=True, runtime_executed=False, binary_and_input_bytes_in_archive=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--region-packet', type=Path, default=Path(__file__).parent / 'packet.tar.gz')
    args = parser.parse_args()
    assert digest(args.region_packet.read_bytes()) == REGION_SHA
    with tempfile.TemporaryDirectory(prefix='shardloom-restoration-') as temporary:
        root = Path(temporary)
        unpack(args.region_packet, root / 'regions')
        if args.packet.is_dir():
            restored = args.packet
        else:
            restored = root / 'restored'
            unpack(args.packet, restored)
        result = verify(restored, root / 'regions')
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
