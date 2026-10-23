#!/usr/bin/env python3
"""Replay the frozen PGO investigation from saved records; execute no archived code before authentication."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
import lzma
import math
from pathlib import Path, PurePosixPath
import sys
import tarfile
import tempfile

if not __debug__:
    raise RuntimeError('optimized Python is unsupported: replay requires semantic assertions')

MAX_MEMBER_BYTES = 32 << 20
MAX_ARCHIVE_BYTES = 128 << 20
MAX_ARCHIVE_MEMBERS = 4096
PROTOCOL_SHA256 = 'a4a4794979bd3b2f487b9b23c595dc286a6daf0f2f1207e29b1334b03b791b44'

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


def read_nested_archive(path):
    # Limit both decoder memory and decoded tar bytes before tarfile sees an
    # extended header. LZMAFile does not expose a decoder memory limit.
    with path.open('rb') as stream:
        compressed = stream.read(MAX_MEMBER_BYTES + 1)
    if len(compressed) > MAX_MEMBER_BYTES:
        raise ValueError('nested xz compressed payload exceeds limit')
    limit = MAX_ARCHIVE_BYTES + MAX_ARCHIVE_MEMBERS * 8192 + 10240
    decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=128 << 20)
    try:
        decoded = decoder.decompress(compressed, max_length=limit + 1)
    except lzma.LZMAError as error:
        raise ValueError('nested xz decoding rejected') from error
    if len(decoded) > limit or not decoder.eof:
        raise ValueError('nested xz payload exceeds limit or is incomplete')
    if decoder.unused_data:
        raise ValueError('nested xz trailing data')
    result, total = {}, 0
    with tarfile.open(fileobj=io.BytesIO(decoded), mode='r|') as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            if (not member.isfile() or not name.parts or name.is_absolute()
                    or '..' in name.parts or chr(92) in member.name
                    or any(':' in part for part in name.parts) or len(member.name) > 4096):
                raise ValueError('unsafe nested archive member')
            key = name.as_posix()
            if key in result:
                raise ValueError('duplicate nested archive member')
            if len(result) >= MAX_ARCHIVE_MEMBERS:
                raise ValueError('nested archive member count exceeds limit')
            if not 0 <= member.size <= MAX_MEMBER_BYTES:
                raise ValueError('nested archive member size exceeds limit')
            if total + member.size > MAX_ARCHIVE_BYTES:
                raise ValueError('nested archive total size exceeds limit')
            raw = archive.extractfile(member).read()
            if len(raw) != member.size:
                raise ValueError('nested archive member length mismatch')
            total += member.size
            result[key] = raw
    return result


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


def exact(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(exact(actual[k], expected[k]) for k in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(exact(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, float):
        return math.isfinite(actual) and actual.hex() == expected.hex()
    return actual == expected


def ordered_values(actual, expected, ordered):
    if not ordered:
        key = lambda row: json.dumps(row, sort_keys=True, ensure_ascii=False)
        actual, expected = sorted(actual, key=key), sorted(expected, key=key)
    assert exact(actual, expected), 'complete result mismatch'


def safe_member(name):
    path = PurePosixPath(name)
    if (not path.parts or path.is_absolute() or '..' in path.parts or chr(92) in name
            or any(':' in part for part in path.parts) or len(name) > 4096):
        raise ValueError('unsafe manifest member')
    return path


class Replay:
    def __init__(self, root):
        self.root = root
        if root.is_symlink():
            raise ValueError('symlinks are unsupported in replay directories')
        count, total = 0, 0
        for path in root.rglob('*'):
            if path.is_symlink():
                raise ValueError('symlinks are unsupported in replay directories')
            if path.is_file():
                size = path.stat().st_size
                count, total = count + 1, total + size
                if size > MAX_MEMBER_BYTES or count > MAX_ARCHIVE_MEMBERS or total > MAX_ARCHIVE_BYTES:
                    raise ValueError('replay directory exceeds archive limits')
        authenticate_helpers(root)
        sys.path.insert(0, str(root / 'repo/scripts'))
        from clickbench_reference_packet import load_reference_packet
        from native_workflow_protocol import extract_result, report_fields, strict_json
        from query_timing_report import query_phase_timings
        from run_clickbench_query_uat import equivalent
        from run_clickbench_paired_query_uat import role_order
        self.strict_json, self.extract, self.fields = strict_json, extract_result, report_fields
        self.phases, self.equivalent, self.role_order = query_phase_timings, equivalent, role_order
        self.load_reference = load_reference_packet
        self.manifest = strict_json((root / 'packet-index.json').read_text())
        assert self.manifest['schema'] == 'shardloom.current_source_pgo_portable_packet.v1'
        self.mapping, members, total = {}, {'packet-index.json'}, 0
        for entry in self.manifest['files']:
            name = safe_member(entry['member']).as_posix()
            assert name not in members and entry['original_path'] not in self.mapping
            path = root / name
            assert not path.is_symlink() and path.is_file()
            raw = path.read_bytes()
            assert len(raw) == entry['bytes'] and digest(raw) == entry['sha256'], name
            assert len(raw) <= MAX_MEMBER_BYTES
            total += len(raw)
            assert total <= MAX_ARCHIVE_BYTES and len(members) < MAX_ARCHIVE_MEMBERS
            self.mapping[entry['original_path']] = path
            members.add(name)
        assert {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()} == members
        self.protocol = self.read(self.manifest['protocol'])
        assert digest(self.bound(self.manifest['protocol']).read_bytes()) == PROTOCOL_SHA256
        self.experiment = self.protocol['root']
        self.base = str(PurePosixPath(self.experiment).parent)
        self.builds = {}

    def bound(self, reference):
        path = self.mapping[reference['path']]
        raw = path.read_bytes()
        assert digest(raw) == reference['sha256'], reference['path']
        if 'bytes' in reference:
            assert len(raw) == reference['bytes']
        return path

    def read(self, reference):
        return self.strict_json(self.bound(reference).read_text())

    def named(self, name):
        return self.strict_json(self.mapping[self.base + '/' + name].read_text())

    def stage(self, name, *, status='passed'):
        data = self.named(PurePosixPath(self.experiment).name + '/' + name + '/summary.json')
        assert data['stage'] == name and data['status'] == status
        assert data['protocol'] == self.manifest['protocol']
        assert data['source_before'] == self.protocol['sources']
        if status == 'passed':
            assert data['source_after'] == data['source_before']
        if name == 'control':
            assert 'readback_amendment' not in data
        else:
            assert data['readback_amendment'] == self.manifest['amendment']
        assert data['prior_failed_configuration'] == self.manifest['failed_configuration']
        assert math.isfinite(data['stage_seconds']) and data['stage_seconds'] > 0
        return data

    def command(self, row, *, returncode=0):
        assert row['status'] == ('passed' if returncode == 0 else 'failed')
        assert row['returncode'] == returncode
        assert row['process_group_drained'] and row['direct_child_reaped']
        assert not row['timed_out'] and row['interrupted_signal'] is None
        if 'stderr' in row:
            self.bound(row['stderr'])
        if 'compressed_stdout' in row:
            raw = read_gzip_payload(self.bound(row['compressed_stdout']))
            assert len(raw) == row['stdout']['bytes'] and digest(raw) == row['stdout']['sha256']
            return raw
        return None

    def no_fallback(self, envelope, *, error=False, typed=False):
        fallback = envelope['fallback']
        assert fallback['attempted'] is False and fallback['allowed'] is False
        assert fallback['engine'] is None
        entries = list(envelope['fields'])
        if typed:
            for key in ('result', 'policy', 'lifecycle', 'capability_snapshot'):
                extra = envelope.get(key, {}).get('fields', [])
                assert isinstance(extra, list)
                entries.extend(extra)
        assert isinstance(entries, list)
        fields = {}
        for entry in entries:
            assert isinstance(entry, dict) and isinstance(entry.get('key'), str)
            assert entry['key'] not in fields or (typed and fields[entry['key']] == entry['value'])
            fields[entry['key']] = entry['value']
        assert all(v is False or (type(v) is str and v == 'false') for k, v in fields.items()
                   if k.endswith(('fallback_attempted', 'external_engine_invoked')))
        external = [v for k, v in fields.items() if k.endswith('external_engine_invoked')]
        assert all(v is False or (type(v) is str and v == 'false') for v in external)
        if not error:
            assert envelope['status'] == 'success' and external
        return fields

    def query(self, row, case, worker, binary, source):
        raw = self.command(row)
        envelope = self.strict_json(raw.decode())
        fields = self.no_fallback(envelope)
        actual = self.extract(envelope)
        ordered_values(actual, case['expected'], case['ordered'])
        assert row['complete_values'] and row['native_query']
        assert exact(row['values'], actual) and row['fields'] == fields
        command = [binary, 'run', 'sql', '--input', source, '--input-format', 'vortex',
                   '--sql', case['sql'], '--request', 'collect', '--bounded', 'true',
                   '--execution-policy', 'native_vortex', '--memory-gb', '4',
                   '--max-parallelism', str(worker), '--format', 'json']
        assert row['command'] == command
        timing = row['native_timing']
        assert timing['returncode'] == 0
        timing_path = row['stdout']['path'].removesuffix('.stdout.json') + '.timing.json'
        assert self.strict_json(self.mapping[timing_path].read_text()) == timing
        phases = self.phases(fields, timing['seconds'])
        for key, value in phases.items():
            assert row[key] == value
        assert row['native_peak_rss_bytes'] == timing['peak_rss_bytes'] > 0
        if case['expected_strategies']:
            assert fields['local_primitive_aggregate_update_strategy'] in case['expected_strategies']
        return phases['engine_execution_seconds']

    def python_training(self, reference, corpus, library, instrumented):
        data = self.read(reference)
        assert data['status'] == 'passed' and data['role'] == 'training_only'
        assert data['instrumented'] is instrumented
        assert data['manifest_sha256'] == self.protocol['training']['corpus']['sha256']
        assert data['native_library_sha256'] == library['sha256']
        assert data['native_operations'] == len(data['records']) == 20
        assert data['complete_value_checks'] == 19 and data['workers'] == 4
        assert data['no_external_execution'] and data['source_and_library_unchanged']
        assert data['native_child_processes_allowed'] is False and data['external_query_execution'] is False
        cases = [c for c in corpus['cases'] if c['role'] == 'train']
        assert [r['name'] for r in data['records']] == [c['name'] for c in cases] + [
            'arrow-filter-order', 'arrow-write-vortex', 'vortex-complete-reopen', 'vortex-nullable-aggregate']
        for row, case in zip(data['records'], cases):
            fields = self.no_fallback(row['envelope'], typed=True)
            assert fields['python_execution_transport'] == 'native' and row['complete_values']
            ordered_values(row['values'], case['expected'], case['ordered'])
            assert int(fields['output_row_count']) == len(row['values'])
            assert row['strategy'] == fields.get('local_primitive_aggregate_update_strategy')
            if case['expected_strategies']:
                assert row['strategy'] in case['expected_strategies']
        rows = [{'py_id': 20_000_000 + n,
                 'py_text': None if n % 13 == 0 else f'python-learn-{n % 71}-京都'} for n in range(4097)]
        extras = data['records'][16:]
        expected = [list(reversed(rows))[:257], None, rows,
                    [{'n': len(rows), 'lo': rows[0]['py_id'], 'hi': rows[-1]['py_id'],
                      'texts': sum(row['py_text'] is not None for row in rows)}]]
        for row, values in zip(extras, expected):
            fields = self.no_fallback(row['envelope'], typed=True)
            assert fields['python_execution_transport'] == 'native'
            if values is not None:
                assert row['complete_values'] and exact(row['values'], values)
                assert int(fields['output_row_count']) == len(values)
            else:
                assert len(row['output_sha256']) == 64
        return {'native_operations': 20, 'complete_value_checks': 19,
                'strategy_cases': sum(bool(c['expected_strategies']) for c in cases)}

    def pressure(self, reference, binary, mode):
        data = self.read(reference)
        assert data['status'] == 'passed' and data['mode'] == mode
        assert data['binary_sha256'] == binary['sha256']
        rows, start, step, column = ((500_000, 1 << 60, 1, 'train_priority') if mode == 'training'
                                     else (450_001, -(1 << 59), 3, 'evaluation_rank'))
        assert (data['rows'], data['start'], data['step'], data['column']) == (rows, start, step, column)
        assert data['offset'] == rows - 19 and data['limit'] == 11
        assert [r['name'] for r in data['records']] == ['prepare', 'route', 'dataframe', 'sql', 'quota-denial']
        parent = str(PurePosixPath(reference['path']).parent)
        expected = [{column: start + index * step} for index in range(18, 7, -1)]
        for row in data['records']:
            name = row['name']
            assert row['passed'] and row['command'][0] == binary['path']
            raw = self.mapping[parent + '/' + name + '.stdout.json'].read_text()
            envelope = self.strict_json(raw)
            fields = self.no_fallback(envelope, error=name == 'quota-denial')
            if name == 'quota-denial':
                assert row['returncode'] != 0 and envelope['status'] != 'success'
                assert 'quota' in raw.lower()
                assert not [g for g in row['guard_failures'] if g != 'profiled native command failed']
            else:
                assert row['returncode'] == 0 and row['guard_failures'] == []
            if name in ('dataframe', 'sql'):
                assert exact(self.extract(envelope), expected) and row['complete_values']
                prefix = 'local_primitive_native_sort_spill_'
                details = {k: int(fields[prefix + k]) for k in ('runs_written', 'runs_validated',
                    'merge_passes', 'peak_reserved_bytes', 'peak_disk_bytes')}
                details['owned_cleanup_completed'] = fields[prefix + 'owned_cleanup_completed'] == 'true'
                assert details == row['spill']
                assert details['runs_written'] == details['runs_validated'] > 8
                assert details['merge_passes'] > 0 and details['owned_cleanup_completed']
                assert details['peak_reserved_bytes'] <= 8_388_608 and details['peak_disk_bytes'] <= 33_554_432
        for key in ('complete_values', 'actual_spill', 'side_effect_free_route', 'quota_denial', 'cleanup'):
            assert data[key] is True
        assert data['fallback_attempted'] is data['external_engine_invoked'] is False
        return {'calls': 5, 'actual_spill': True, 'quota_denial': True, 'complete_value_checks': 2}

    def build(self, role):
        data = self.stage(role, status='failed' if role == 'control' else 'passed')
        original = data
        if role == 'control':
            assert data['error'] == 'AssertionError()' and len(data['records']) == 2
            recovered = self.stage('control-recovery')
            assert recovered['native_build_repeated'] is False
            assert recovered['original_control']['path'] == self.experiment + '/control/summary.json'
            assert self.read(recovered['original_control']) == original
            assert len(recovered['records']) == 1
            data = recovered
        assert data['target_directory'] == self.protocol['build']['target_directories'][role]
        assert data['fresh_empty_target'] and data['build_command'] == self.protocol['build']['command']
        assert data['rustflags'] == self.protocol['build']['flags'][role]
        assert data['environment'] == self.protocol['build']['environment']
        for record in original['records']:
            self.command(record)
        compiled = next(r for r in original['records'] if r['name'] == 'cargo-build')
        assert compiled['command'] == self.protocol['build']['command']
        messages = [self.strict_json(line) for line in self.command(compiled).decode().splitlines() if line.strip()]
        selected = [m for m in messages if m.get('reason') == 'compiler-artifact'
                    and m.get('target', {}).get('name') in ('shardloom', '_native')]
        assert {m['target']['name'] for m in selected} == {'shardloom', '_native'}
        assert all(m['fresh'] is False for m in selected)
        assert messages[-1]['reason'] == 'build-finished' and messages[-1]['success']
        if role == 'candidate':
            diagnostics = self.bound(compiled['stderr']).read_text()
            assert diagnostics.count('no profile data available for function') == 3922
            assert 'profile data may be out of date' not in diagnostics
            assert 'hash mismatch' not in diagnostics.lower()
        cli = next(m['executable'] for m in selected if m['target']['name'] == 'shardloom')
        assert cli.startswith(data['target_directory'] + '/')
        symbols = self.command(next(r for r in data['records'] if r['name'] == 'native-object-symbols')).decode()
        assert data['native_objects_uninstrumented']
        assert not any(marker in symbols for marker in ('__llvm_profile', '__llvm_prf', '__profc_', '__profd_'))
        assert len(data['native_objects']) == 39
        assert any('zstd-sys-' in x['path'] for x in data['native_objects'])
        assert all(x['path'].startswith(str(PurePosixPath(cli).parent / 'build') + '/')
                   for x in data['native_objects'])
        plan = self.named(PurePosixPath(self.experiment).name + '/' + role + '/cache-retirement-plan.json')
        retirement = self.named(PurePosixPath(self.experiment).name + '/' + role + '/cache-retirement.json')
        assert self.read(retirement['plan']) == plan
        assert plan['status'] == 'verified_before_retirement' and plan['role'] == role
        assert plan['open_handles'] is False and plan['backup_created'] is False
        assert retirement['status'] == 'passed' and retirement['cache_absent'] and retirement['retained_artifacts_verified']
        assert retirement['entries_retired'] == len(plan['inventory'])
        assert plan['retained_artifacts'] == data['artifacts']
        inventory = {item['path']: item for item in plan['inventory'] if 'path' in item}
        for item in data['native_objects']:
            assert all(inventory[item['path']][k] == v for k, v in item.items())
        assert set(data['artifacts']) == {'cli', 'library'}
        if role != 'control':
            assert all(data['artifacts'][k]['sha256'] != self.builds['control']['artifacts'][k]['sha256']
                       for k in data['artifacts'])
        self.builds[role] = data
        return {'fresh': True, 'native_objects': 39, 'artifacts': data['artifacts'],
                'compiler_stage_records': len(original['records']),
                'readback_recovery': role == 'control', 'cache_retired': True}

    def training(self):
        corpus = self.read(self.protocol['training']['corpus'])
        assert corpus['status'] == 'passed'
        cases = [c for c in corpus['cases'] if c['role'] == 'train']
        heldout = [c for c in corpus['cases'] if c['role'] == 'heldout']
        assert len(cases) == 16 and len(heldout) == 17
        assert {c['dataset'] for c in cases}.isdisjoint(c['dataset'] for c in heldout)
        assert len({d['file']['sha256'] for d in corpus['datasets']}) == 4
        admission = self.stage('control-admission')
        assert [r['name'] for r in admission['records']] == ['stage-native-package', 'native-python']
        for row in admission['records']:
            self.command(row)
        control_python = self.python_training(admission['native_python'], corpus,
                                             self.builds['control']['artifacts']['library'], False)
        data = self.stage('training')
        assert data['role'] == 'train' and data['profile_directory_empty_before']
        assert data['no_heldout_or_full43_training'] and data['selected_paths_observed']
        assert data['cli_queries'] == 48 and data['preparations'] == 2
        assert data['native_python_operations'] == 20 and data['pressure_calls'] == 5
        assert len(data['records']) == 53
        sources = {d['id']: d['file'] for d in data['datasets']}
        preflight = self.named('pgo-baseline-preflight-966b/summary.json')
        baseline_sources = {d['id']: d['file'] for d in preflight['datasets']}
        assert len(sources) == 2 and sources.keys() == baseline_sources.keys()
        for key, source in sources.items():
            assert source['sha256'] == baseline_sources[key]['sha256']
            assert source['bytes'] == baseline_sources[key]['bytes']
        cli = self.builds['instrumented']['artifacts']['cli']
        expected_order = []
        for dataset in (d for d in corpus['datasets'] if d['role'] == 'train'):
            expected_order.append(dataset['id'] + '-prepare')
            expected_order.extend(c['name'] + '-w' + str(w) for c in cases
                                  if c['dataset'] == dataset['id'] for w in (1, 4, 12))
        expected_order.extend(['stage-native-package', 'native-python', 'pressure'])
        assert [r['name'] for r in data['records']] == expected_order
        queries = [r for r in data['records'] if r['native_query']]
        assert [(r['case'], r['requested_workers']) for r in queries] == [
            (c['name'], w) for d in corpus['datasets'] if d['role'] == 'train'
            for c in cases if c['dataset'] == d['id'] for w in (1, 4, 12)]
        strategies = set()
        for row in data['records']:
            if row['native_query']:
                case = next(c for c in cases if c['name'] == row['case'])
                self.query(row, case, row['requested_workers'], cli['path'], sources[case['dataset']]['path'])
                if case['expected_strategies']:
                    strategies.add(row['fields']['local_primitive_aggregate_update_strategy'])
            else:
                raw = self.command(row)
                if row['name'].endswith('-prepare'):
                    self.no_fallback(self.strict_json(raw.decode()))
        assert len(strategies) == 9
        native = self.python_training(data['native_python'], corpus,
                                     self.builds['instrumented']['artifacts']['library'], True)
        pressure = self.pressure(data['pressure'], cli, 'training')
        profiles = data['raw_profiles']
        assert 1 <= len(profiles) <= 16
        assert len({i['path'] for i in profiles}) == len(profiles)
        assert all(i['path'].startswith(self.experiment + '/profiles/shardloom-')
                   and i['path'].endswith('.profraw') and i['bytes'] > 0 for i in profiles)
        assert sum(i['bytes'] for i in profiles) <= 1 << 30
        merge = self.stage('merge')
        assert merge['raw_profiles'] == profiles
        assert [r['name'] for r in merge['records']] == ['merge-profiles', 'profile-summary']
        assert merge['records'][0]['command'][-len(profiles):] == [i['path'] for i in profiles]
        for row in merge['records']:
            self.command(row)
        summary = self.command(merge['records'][1]).decode()
        assert 'Instrumentation level: IR' in summary and 'Total functions: 81892' in summary
        for key in ('bytes', 'sha256'):
            assert merge['merged_profile'][key] == merge['transferred_profile'][key]
        assert merge['transferred_profile']['path'] == self.experiment + '/profile-transfer/merged.profdata'
        return {'cli_queries': 48, 'preparations': 2, 'selected_strategy_families': sorted(strategies),
                'control_admission': control_python, 'native_python': native, 'pressure': pressure,
                'fresh_profiles': len(profiles), 'profile_merge_and_transfer': True}

    def full43(self, name):
        data = self.stage(name)
        paired = name != 'standard-full43'
        reverse = name == 'paired-b'
        assert data['input'] == self.protocol['full43_input']
        assert data['query_file'] == self.protocol['query_file']
        assert data['artifacts'] == {r: self.builds[r]['artifacts'] for r in ('control', 'candidate')}
        assert data['evaluation_binding'] == self.manifest['binding']
        binding = self.read(self.manifest['binding'])
        assert data['evaluator'] == binding['evaluator']
        self.bound(data['evaluator'])
        reference = self.load_reference(self.bound(data['reference']), self.bound(data['query_file']))
        assert reference['packet']['reference_kind'] == 'independent_reference'
        summary = self.read(data['summary'])
        assert summary['completed_identity_check'] and summary['reference_packet_identity_verified']
        assert summary['reference_kind'] == 'independent_reference'
        assert summary['reference_packet']['sha256'] == data['reference']['sha256']
        assert summary['repeats'] == 1 and summary['selected_query_ids'] == list(range(1, 44))
        if paired:
            assert summary['reverse_order'] is reverse
        expected = ([(q, role) for q in range(1, 44) for role in self.role_order(q, 1, reverse)] if paired
                    else [(q, 'candidate') for q in range(1, 44)])
        assert [(r['query'], r.get('role', 'candidate')) for r in summary['records']] == expected
        parent = str(PurePosixPath(data['summary']['path']).parent)
        archives = {}
        if paired:
            assert len(summary['archives']) == 43
            for q, item in zip(range(1, 44), summary['archives']):
                raw = read_nested_archive(self.bound(item))
                assert list(raw) == [m['name'] for m in item['members']]
                for member in item['members']:
                    assert len(raw[member['name']]) == member['bytes']
                    assert digest(raw[member['name']]) == member['sha256']
                archives[q] = raw
        observed = []
        for row in summary['records']:
            q, role = row['query'], row.get('role', 'candidate')
            assert row['run'] == 1 and row['passed'] and row['returncode'] == 0
            assert row['validation'] == 'complete_values' and row['engine_timing_reported']
            assert row['guard_failures'] == [] and row['query_statement'] == reference['queries'][q - 1]
            prefix = f'q{q:02d}_run1' + ('_' + role if paired else '')
            if paired:
                raw = archives[q][prefix + '.stdout.json']
                timing = self.strict_json(archives[q][prefix + '.timing.json'].decode())
            else:
                raw = read_gzip_payload(self.mapping[parent + '/' + prefix + '.stdout.json.gz'])
                timing = self.strict_json(self.mapping[parent + '/' + prefix + '.timing.json'].read_text())
            fields = self.no_fallback(self.strict_json(raw.decode()))
            actual = self.extract(self.strict_json(raw.decode()))
            assert self.equivalent(actual, reference['values'][q])
            assert timing['returncode'] == 0
            phases = self.phases(fields, timing['seconds'])
            for key, value in phases.items():
                assert row[key] == value
            observed.append({'query': q, 'role': role, 'engine_seconds': row['engine_execution_seconds'],
                             'process_seconds': row['process_wall_seconds'], 'rows': len(actual),
                             'raw_sha256': digest(raw)})
        assert observed == data['observations']
        totals = {role: {'engine_seconds': math.fsum(r['engine_seconds'] for r in observed if r['role'] == role),
                         'process_seconds': math.fsum(r['process_seconds'] for r in observed if r['role'] == role),
                         'rows': sum(r['rows'] for r in observed if r['role'] == role)}
                  for role in ('control', 'candidate') if any(r['role'] == role for r in observed)}
        assert totals == data['totals'] and all(t['rows'] == 381 for t in totals.values())
        by_query = {(r['query'], r['role']): r['engine_seconds'] for r in observed}
        if paired:
            controls = {q: by_query[q, 'control'] for q in range(1, 44)}
            assert data['engine_gate_passed'] == (totals['candidate']['engine_seconds'] < totals['control']['engine_seconds'])
        else:
            restored = self.named('full43-restored-run-964.json')
            controls = {r['query']: r['engine_execution_seconds'] for r in self.read(restored['summary'])['records']}
        deltas = [{'query': q, 'control_engine_seconds': controls[q],
                   'candidate_engine_seconds': by_query[q, 'candidate'],
                   'delta_seconds': by_query[q, 'candidate'] - controls[q],
                   'deviation_triggered': by_query[q, 'candidate'] > controls[q] * 1.10 + 0.020}
                  for q in range(1, 44)]
        assert data['query_deltas'] == deltas
        deviations = [r['query'] for r in deltas if r['deviation_triggered']]
        assert data['deviation_queries'] == deviations
        assert data['uat_lock_released'] and data['complete_values']
        assert len(data['records']) == 1
        workflow = data['records'][0]
        assert workflow['status'] == 'passed' and workflow['returncode'] == 0
        assert workflow['process_group_drained'] and workflow['direct_child_reaped']
        assert workflow['source_hash_before_after'] and workflow['native_query_workflow']
        self.bound(workflow['log'])
        return {'stage': name, 'query_calls': len(observed), 'complete_result_rows': sum(r['rows'] for r in observed),
                'engine_totals': {role: totals[role]['engine_seconds'] for role in totals},
                'engine_gate_passed': data.get('engine_gate_passed'), 'deviation_queries': deviations}

    def heldout(self, order):
        data = self.stage('heldout-' + order)
        corpus = self.read(self.protocol['training']['corpus'])
        cases = [c for c in corpus['cases'] if c['role'] == 'heldout']
        sources = {d['id']: d['file'] for d in self.protocol['heldout_inputs']}
        roles = ['control', 'candidate'] if order == 'a' else ['candidate', 'control']
        assert data['role'] == 'heldout' and data['datasets'] == self.protocol['heldout_inputs']
        assert data['artifacts'] == {r: self.builds[r]['artifacts'] for r in roles}
        assert len(cases) == 17 and len(data['records']) == 102
        expected = [(c['name'], w, r) for c in cases for w in (1, 4, 12) for r in roles]
        assert [(r['case'], r['workers'], r['role']) for r in data['records']] == expected
        for row in data['records']:
            case = next(c for c in cases if c['name'] == row['case'])
            self.query(row, case, row['workers'], data['artifacts'][row['role']]['cli']['path'],
                       sources[case['dataset']]['path'])
        totals = {r: math.fsum(x['engine_execution_seconds'] for x in data['records'] if x['role'] == r)
                  for r in roles}
        assert totals == data['engine_totals']
        assert data['heldout_gate_passed'] == (totals['candidate'] <= totals['control'] * 1.05 + 0.020)
        keyed = {(r['case'], r['workers'], r['role']): r['engine_execution_seconds'] for r in data['records']}
        deviations = [{'case': name, 'workers': worker, 'control_engine_seconds': value,
                       'candidate_engine_seconds': keyed[name, worker, 'candidate']}
                      for (name, worker, role), value in keyed.items() if role == 'control'
                      and keyed[name, worker, 'candidate'] > value * 1.10 + 0.020]
        assert deviations == data['deviations']
        return {'calls': 102, 'engine_totals': totals, 'gate_passed': data['heldout_gate_passed'],
                'deviations': deviations}

    def public_pressure(self, reference):
        data = self.read(reference)
        assert data['status'] == 'passed' and data['families'] == ['aggregate-pressure', 'ordering-pressure']
        assert data['performance_claim'] is False and data['whole_process_memory_ceiling'] is False
        expected_names = ['aggregate-pressure-grouped-' + str(g) + '-spill-' + str(s)
                          for g in (False, True) for s in (False, True)]
        expected_names += ['aggregate-pressure-write']
        expected_names += ['aggregate-pressure-failed-' + f for f in ('producer', 'quota', 'corrupt')]
        expected_names += ['aggregate-pressure-consumer-close', 'aggregate-pressure-consumer-failure']
        expected_names += ['order-pressure-' + n for n in ('resident', 'spill', 'write')]
        expected_names += ['order-pressure-failed-' + f for f in ('producer', 'quota', 'corrupt')]
        assert [c['name'] for c in data['cases']] == expected_names
        parent = str(PurePosixPath(reference['path']).parent)
        groups, aggregate_rows = 16381, 32769

        def aggregate_expected(grouped):
            if grouped:
                for ordinal in range(groups):
                    value, count = ordinal * 17 % groups, 2 + int(ordinal < 7)
                    present = value % 31 != 0
                    yield {'s': f'{value:09}' + 'x' * 503, 'rows': count,
                        'present': count if present else 0, 'unique': int(present),
                        'total': float(value * count) if present else None,
                        'mean': float(value) if present else None,
                        'smallest': value if present else None, 'largest': value if present else None}
            else:
                values = [n * 17 % groups for n in range(aggregate_rows) if (n * 17 % groups) % 31]
                total = sum(map(float, values))
                yield {'rows': aggregate_rows, 'present': len(values), 'unique': groups,
                       'total': total, 'mean': total / len(values), 'smallest': 1, 'largest': groups - 1}

        checked_rows = 0
        for case in data['cases']:
            assert case['status'] == 'passed'
            name = case['name']
            if 'complete_rows' in case:
                item = dict(case['complete_rows'])
                item['path'] = parent + '/' + safe_member(item['path']).as_posix()
                path = self.bound(item)
                ordered = name.startswith('order-')
                grouped = '-grouped-True-' in name or name == 'aggregate-pressure-write'
                expected = (({'n': n, 's': 'λ' * 256} for n in range(66567)) if ordered
                            else aggregate_expected(grouped))
                count, decoded = 0, 0
                sha = hashlib.sha256()
                with gzip.open(path, 'rb') as stream:
                    for wanted in expected:
                        raw = stream.readline(8193)
                        decoded += len(raw)
                        assert 0 < len(raw) <= 8192 and decoded <= 64 << 20
                        actual = self.strict_json(raw.decode())
                        assert exact(actual, wanted), (name, count)
                        canonical = (json.dumps(wanted, sort_keys=True, ensure_ascii=False,
                                                allow_nan=False, separators=(',', ':')) + '\n').encode()
                        assert raw == canonical
                        sha.update(raw)
                        count += 1
                    assert stream.read(1) == b''
                assert count == case.get('rows_verified', case.get('complete_output_rows'))
                assert sha.hexdigest() == case['all_output_rows_sha256']
                checked_rows += count
                assert case['performance_claim'] is False
                if 'native_grant_bytes' in case:
                    assert case['native_grant_bytes'] == 1 << 30
                if 'peak_reserved_bytes' in case:
                    assert 0 < case['peak_reserved_bytes'] <= 1 << 30
                    assert 0 < case['allocation_peak_reserved_bytes'] <= 1 << 30
                if 'larger_than_grant_public_claim' in case:
                    assert case['larger_than_grant_public_claim'] is False
            elif '-failed-' in name:
                assert case['source_closed'] and not case['published'] and case['spill_cleaned']
                if name.endswith('-corrupt'):
                    assert case['damaged_run']
            else:
                assert case['source_closed'] and case['child_exited'] and case['successful_prefix'] is False
        return {'cases': len(data['cases']), 'complete_rows_replayed': checked_rows,
                'larger_than_public_grant_claim': False}

    def pressure_role(self, role):
        data = self.stage('pressure-' + role)
        assert data['correctness_only'] and data['artifacts'] == self.builds[role]['artifacts']
        assert [r['name'] for r in data['records']] == ['heldout-ordering', 'streaming-pressure']
        for row in data['records']:
            self.command(row)
        return {'custom': self.pressure(data['custom'], data['artifacts']['cli'], 'evaluation'),
                'public': self.public_pressure(data['streaming'])}

    def package(self, role):
        data = self.stage('package-' + role)
        assert data['artifacts'] == self.builds[role]['artifacts'] and data['publication_attempted'] is False
        assert len(data['records']) == 1
        self.command(data['records'][0])
        proof = self.read(data['proof'])
        assert proof['status'] == 'passed' and proof['publication_attempted'] is False
        assert proof['performance_claim'] is False and proof['scope'] == 'current_host_native_wheel_install_and_binding_integration'
        assert proof['uninstall_status'] == 'passed' and proof['installed_load']['isolated_install']
        assert proof['integration'] == data['native_tests'] == {
            'tests': 121, 'failures': 0, 'errors': 0, 'skipped': 0, 'status': 'passed'}
        for section in ('stage', 'integrity'):
            row = proof[section]
            assert row['status'] == 'passed' and row['published'] is False
            assert row['native_library_sha256'] == data['artifacts']['library']['sha256']
            assert row['cli_sha256'] == data['artifacts']['cli']['sha256']
            assert row['licenses']['status'] == 'passed'
        assert proof['integrity']['tag'] == 'cp310-abi3-macosx_27_0_arm64'
        parent = str(PurePosixPath(data['proof']['path']).parent)
        names = ['build', 'create_environment', 'install_optional_consumer', 'install',
                 'load', 'integration', 'uninstall', 'verify_uninstalled']
        assert [s['name'] for s in proof['steps']] == names
        for step in proof['steps']:
            assert step['returncode'] == 0
            raw = self.bound({'path': parent + '/' + safe_member(step['log']).as_posix(),
                              'sha256': step['log_sha256']}).read_text()
            if step['name'] == 'load':
                assert step['command'][-2:] == [data['artifacts']['library']['sha256'],
                                                data['artifacts']['cli']['sha256']]
                assert self.strict_json(raw) == proof['installed_load']
            if step['name'] == 'integration':
                assert sum(line.endswith(' ... ok') for line in raw.splitlines()) == 121
                assert self.strict_json(self.mapping[parent + '/integration.json'].read_text()) == proof['integration']
        return {'tests': 121, 'failures': 0, 'errors': 0, 'skips': 0, 'isolated_install': True,
                'uninstalled': True, 'platform_scope': proof['scope']}

    def ingest_failure(self):
        failed_launch = self.stage('ingest-1', status='failed')
        assert len(failed_launch['records']) == 1
        self.command(failed_launch['records'][0], returncode=1)
        amendment = self.named('pgo-ingest-amendment-980.json')
        assert amendment['native_ingest_observations_before_amendment'] == 0
        assert amendment['protocol'] == self.manifest['protocol']
        assert amendment['original_evaluation_binding'] == self.manifest['binding']
        for item in amendment['preserved_failure']:
            self.bound(item)
        preflight = self.stage('ingest-interpreter-preflight')
        assert preflight['native_process_launched'] is False
        for row in preflight['records']:
            raw = self.command(row)
            assert self.strict_json(raw.decode())['timing_helper_imported']
        failed = self.stage('ingest-recovered-1', status='failed')
        assert failed['error'] == 'AssertionError()' and len(failed['records']) == 1
        self.command(failed['records'][0])
        assert failed['ingest_collection_amendment']['sha256'] == digest(self.mapping[self.base + '/pgo-ingest-amendment-980.json'].read_bytes())
        assert '--max-parallelism' in failed['records'][0]['command']
        command = failed['records'][0]['command']
        assert command[command.index('--max-parallelism') + 1] == '2'
        wrappers = [p for name, p in self.mapping.items()
                    if name.startswith(self.experiment + '/ingest-recovered-1/uat/logs/')
                    and name.endswith('/prepare_summary.json')]
        assert len(wrappers) == 1
        wrapper = self.strict_json(wrappers[0].read_text())
        assert wrapper['returncode'] == 0 and wrapper['stdout_json_ok'] and wrapper['target_exists']
        assert wrapper['max_parallelism'] == 2 and wrapper['memory_gb'] == 24
        fields = self.no_fallback(self.strict_json(self.mapping[wrapper['stdout_path']].read_text()))
        assert fields['public_workflow_preparation_source_state_record_batch_count'] == '423'
        assert fields['public_workflow_preparation_source_state_stream_batch_size'] == '262144'
        assert wrapper['target_bytes'] == 15748946385 != self.protocol['full43_input']['bytes']
        for i in (2, 3, 4):
            assert 'ingest-recovered-' + str(i) not in self.manifest['stages']
        diagnostic = self.stage('control-ingest-correctness')
        assert diagnostic['performance_comparison'] is False and diagnostic['frozen_retention_gate_still_failed']
        assert diagnostic['artifacts'] == self.builds['control']['artifacts']
        assert diagnostic['input']['bytes'] == wrapper['target_bytes']
        assert diagnostic['input']['sha256'] == '05082be7bd2c515904822678b957281605e98abca1e4f3472c3b766dcc869510'
        assert diagnostic['failed_ingest']['path'] == self.experiment + '/ingest-recovered-1/summary.json'
        self.read(diagnostic['failed_ingest'])
        summary = self.read(diagnostic['summary'])
        assert summary['completed_identity_check'] and summary['reference_packet_identity_verified']
        assert summary['selected_query_ids'] == list(range(1, 44)) and summary['repeats'] == 1
        reference = self.load_reference(self.bound(diagnostic['reference']), self.bound(diagnostic['query_file']))
        parent = str(PurePosixPath(diagnostic['summary']['path']).parent)
        observed = []
        for q, row in zip(range(1, 44), summary['records'], strict=True):
            assert row['query'] == q and row['run'] == 1 and row['passed'] and row['returncode'] == 0
            assert row['validation'] == 'complete_values' and row['guard_failures'] == []
            assert row['query_statement'] == reference['queries'][q - 1]
            raw = read_gzip_payload(self.mapping[parent + f'/q{q:02d}_run1.stdout.json.gz'])
            envelope = self.strict_json(raw.decode())
            fields = self.no_fallback(envelope)
            actual = self.extract(envelope)
            assert self.equivalent(actual, reference['values'][q])
            timing = self.strict_json(self.mapping[parent + f'/q{q:02d}_run1.timing.json'].read_text())
            assert timing['returncode'] == 0
            phases = self.phases(fields, timing['seconds'])
            assert phases['engine_execution_seconds'] == row['engine_execution_seconds']
            observed.append({'query': q, 'engine_seconds': phases['engine_execution_seconds'],
                             'rows': len(actual), 'raw_sha256': digest(raw)})
        assert diagnostic['observations'] == observed
        assert diagnostic['queries'] == 43 and diagnostic['rows'] == sum(r['rows'] for r in observed) == 381
        assert diagnostic['engine_seconds'] == math.fsum(r['engine_seconds'] for r in observed)
        return {'first_attempt_native_process_started': False, 'native_ingest_calls': 1,
                'candidate_ingest_calls': 0, 'output_byte_identity_gate': 'failed',
                'diagnostic_correctness_queries': 43, 'diagnostic_complete_rows': 381,
                'diagnostic_engine_seconds': diagnostic['engine_seconds'],
                'complete_workflow_comparison': 'unavailable', 'break_even_reuses': None}

    def bindings(self):
        for item in self.protocol['helpers'] + self.protocol['prerequisites']:
            self.bound(item)
        assert len(self.protocol['sources']) == 2243 and len(self.protocol['native_sources']) == 1922
        assert all(self.protocol['sources'][k] == v for k, v in self.protocol['native_sources'].items())
        amendment = self.read(self.manifest['amendment'])
        assert self.manifest['amendment']['sha256'] == 'bcd7b0896413ce0733fc39ea5df4ef11b91ee10b20b5764e3d44c5ff3f5a9027'
        assert amendment['protocol'] == self.manifest['protocol']
        assert amendment['instrumented_workspace_builds'] == amendment['training_runs'] == amendment['candidate_observations'] == 0
        for item in amendment['revised_helpers'] + amendment['preserved_evidence']:
            self.bound(item)
        binding = self.read(self.manifest['binding'])
        assert binding['protocol'] == self.manifest['protocol']
        assert binding['readback_amendment'] == self.manifest['amendment']
        assert binding['prior_failed_configuration'] == self.manifest['failed_configuration']
        assert binding['instrumented_build_started'] is False
        assert binding['training_runs'] == binding['candidate_observations'] == 0
        self.bound(binding['evaluator'])
        original = self.read(binding['original_binding'])
        assert original['protocol'] == self.manifest['protocol']
        self.bound(original['evaluator'])
        failed = self.read(self.manifest['failed_configuration'])
        assert failed['decision'] == 'drop' and failed['all_evaluation_stages_unrun']
        assert failed['training_calls_attempted'] == 1 and failed['training_calls_successful'] == 0
        assert failed['profiles_created'] == failed['candidate_observations'] == 0
        assert failed['profile_merge_performed'] is failed['profile_use_build_performed'] is False
        probe = self.read(self.manifest['compiler_probe'])
        assert probe['status'] == 'passed' and probe['broken_reproduces_sigsegv']
        assert probe['fixed_control_instrumented_profile_use_exact_results'] and probe['fixed_c_objects_uninstrumented']
        assert probe['compiler_isolation_flags'] == '-fno-profile-generate -fno-profile-use'
        records = {r['name']: r for r in probe['records']}
        assert len(records) == len(probe['records']) == 18
        for name, row in records.items():
            assert row['returncode'] == (-11 if name == 'broken-run' else 0)
            self.bound(row['stdout'])
            self.bound(row['stderr'])
        broken_symbols = self.bound(records['broken-symbols']['stdout']).read_text()
        assert '__llvm' in broken_symbols
        outputs = [self.bound(records[role + '-run']['stdout']).read_bytes()
                   for role in ('control', 'instrumented', 'candidate')]
        assert outputs[0] == outputs[1] == outputs[2] and outputs[0].strip()
        for role in ('control', 'instrumented', 'candidate'):
            symbols = self.bound(records[role + '-symbols']['stdout']).read_text()
            assert not any(marker in symbols for marker in ('__llvm_profile', '__llvm_prf', '__profc_', '__profd_'))
        return {'frozen_protocol': True, 'readback_recovery_bound': True,
                'prior_mixed_compiler_crash_preserved': True, 'native_sources': 1922}

    def admission(self):
        data = self.named('pgo-ingest-admission-replay-981.json')
        assert data['status'] == 'passed' and data['metadata_only']
        assert data['source'] == self.protocol['ingest_source']
        assert data['performance_comparison'] is data['all_column_value_equivalence_claim'] is False
        for item in data['source_policy'] + [data['helper'], data['supersedes']]:
            self.bound(item)
        groups = data['row_groups']
        assert len(groups) == 226 and data['columns'] == 105
        assert sum(g['rows'] for g in groups) == data['rows'] == 99997497
        metadata_width = max((g['uncompressed_bytes'] + g['rows'] - 1) // g['rows'] for g in groups)
        estimate = max(metadata_width, data['columns'] * 8, 1) * 4
        assert metadata_width == data['max_metadata_bytes_per_row'] == 1270
        assert estimate == data['estimated_bytes_per_row'] == 5080
        assert len(data['observations']) == 2
        for saved, requested, admitted in zip(data['observations'], (12, 2), (10, 2), strict=True):
            budget = (24 << 30) // 8 // admitted
            affordable = max(1, budget // estimate)
            batch = 262144 if affordable >= 262144 else 1 << (affordable.bit_length() - 1)
            assert saved['requested_parallelism'] == requested and saved['admitted_parallelism'] == admitted
            assert saved['memory_bytes'] == 24 << 30
            assert saved['source_batch_budget_bytes'] == budget and saved['affordable_rows'] == affordable
            assert saved['batch_rows'] == batch
            target, start, tasks = max(batch, 65536) * 4, 0, []
            while start < len(groups):
                end, rows = start, 0
                while end < len(groups):
                    if end > start and (rows >= target or end - start >= 64):
                        break
                    rows += max(groups[end]['rows'], 1)
                    end += 1
                    if rows >= target:
                        break
                tasks.append({'first_row_group': start, 'row_groups': end - start, 'rows': rows,
                              'batches': (rows + batch - 1) // batch})
                start = end
            assert tasks == saved['tasks'] and len(tasks) == saved['source_tasks']
            assert sum(t['batches'] for t in tasks) == saved['source_batches']
        old = self.named('pgo-ingest-layout-provenance-981b.json')
        # The detailed source inventory is data; saved old/current runner reports
        # below bind the actual resource settings independently of its prose.
        assert old
        old_acceptance_path = '/Users/dylan/LocalData/shardloom/native-python-surface-20261010/fresh-uat663-acceptance.json'
        accepted = self.strict_json(self.mapping[old_acceptance_path].read_text())
        assert accepted['accepted_commit'] == '6780e7df564c55f3e256822be1d0383cfabff0c9'
        wrapper = self.read(accepted['ingest_summary'])
        assert wrapper['max_parallelism'] == 12 and wrapper['memory_gb'] == 24
        assert wrapper['target_bytes'] == self.protocol['full43_input']['bytes']
        fields = self.no_fallback(self.read(accepted['ingest_stdout']))
        assert fields['public_workflow_preparation_source_state_record_batch_count'] == '3120'
        assert fields['public_workflow_preparation_source_state_stream_batch_size'] == '32768'
        return {'root_cause': 'reference prepared at different parallelism',
                'batch_rows': [o['batch_rows'] for o in data['observations']],
                'source_batches': [o['source_batches'] for o in data['observations']],
                'same_byte_identity_comparator_appropriate': False}

    def closeout(self, results):
        assert self.manifest['closeout'] is not None, 'closed investigation requires its closeout record'
        data = self.read(self.manifest['closeout'])
        assert data['schema'] == 'shardloom.current_source_pgo_closeout.v1'
        assert data['status'] == 'closed' and data['decision'] == 'drop'
        assert data['protocol'] == self.manifest['protocol']
        assert data['gate_changed'] is data['failed_observation_replaced'] is False
        assert data['complete_workflow_savings'] is data['break_even_reuses'] is None
        assert data['native_control_ingest_count'] == 1 and data['candidate_ingest_count'] == 0
        assert data['planned_native_ingests'] == 4
        assert data['unrun_ingest_roles'] == ['candidate', 'candidate', 'control']
        assert data['release_default_changed'] is data['publication_attempted'] is False
        assert data['competitive_gate_completion_claim'] is False
        assert data['sources'] == {'all_source_files': 2243, 'native_source_files': 1922, 'exactly_unchanged': True}
        for item in data['evidence']:
            self.bound(item)
        stages = {s: self.stage(s, status='failed' if s == 'control' else 'passed')['stage_seconds']
                  for s in ('control', 'instrumented', 'training', 'merge', 'candidate')}
        stages['control'] += self.stage('control-recovery')['stage_seconds']
        generation = self.named('pgo-generation-966/generation.json')['seconds']
        setup = data['setup_diagnostics']
        assert setup['stage_seconds'] == stages and setup['corpus_generation_seconds'] == generation
        assert setup['additional_setup_seconds'] == max(0.0, generation + stages['instrumented']
            + stages['training'] + stages['merge'] + stages['candidate'] - stages['control'])
        assert setup['reuse_count_evidenced'] is None
        assert data['standard_full43_engine_seconds'] == results['full43'][0]['engine_totals']['candidate']
        for row, result in zip(data['paired_full43'], results['full43'][1:], strict=True):
            assert row['engine_totals'] == result['engine_totals']
            totals = row['engine_totals']
            assert row['engine_gain_fraction'] == 1 - totals['candidate'] / totals['control']
            assert row['deviation_queries'] == result['deviation_queries']
        repeated = set(results['full43'][1]['deviation_queries']) & set(results['full43'][2]['deviation_queries'])
        assert sorted(repeated) == data['per_query_deviation_adjudication']['repeated_in_both_orders'] == [4]
        for order, row in zip(('a', 'b'), data['per_query_deviation_adjudication']['q4'], strict=True):
            pair = self.stage('paired-' + order)
            assert row == next(r for r in pair['query_deltas'] if r['query'] == 4)
        return {'status': 'closed', 'decision': 'drop', 'gate_unchanged': True,
                'complete_workflow_savings': None, 'break_even_reuses': None}


def verify(root):
    replay = Replay(root)
    results = {'bindings': replay.bindings(), 'builds': {}}
    for role in ('control', 'instrumented', 'candidate'):
        results['builds'][role] = replay.build(role)
    results['training'] = replay.training()
    results['full43'] = [replay.full43(name) for name in ('standard-full43', 'paired-a', 'paired-b')]
    results['heldout'] = {order: replay.heldout(order) for order in ('a', 'b')}
    results['pressure'] = {role: replay.pressure_role(role) for role in ('control', 'candidate')}
    results['package'] = {role: replay.package(role) for role in ('control', 'candidate')}
    results['ingest'] = replay.ingest_failure()
    results['admission'] = replay.admission()
    results['closeout'] = replay.closeout(results)
    results.update(status='passed', engine_executed=False, archived_code_authenticated=True,
                   decision='drop', retention_gate_failed=True, release_default_changed=False)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.packet.is_dir():
        result = verify(args.packet)
    else:
        with tempfile.TemporaryDirectory(prefix='shardloom-pgo-replay-') as directory:
            root = Path(directory)
            unpack(args.packet, root)
            result = verify(root)
    text = json.dumps(result, indent=2, allow_nan=False) + '\n'
    if args.output:
        args.output.write_text(text)
    print(text)


if __name__ == '__main__':
    main()
