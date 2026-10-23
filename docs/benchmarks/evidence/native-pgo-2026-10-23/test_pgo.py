"""Require semantic rejection after repairing saved evidence's outer identities."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from archive_trust_controls import check as check_archive_trust


def read(path):
    return json.loads(path.read_text())


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def repair(root, manifest, mapping):
    def refs(value, digests):
        changed = False
        if isinstance(value, dict):
            if value.get('path') in digests and 'sha256' in value:
                size, digest = digests[value['path']]
                if value['sha256'] != digest:
                    value['sha256'], changed = digest, True
                if 'bytes' in value and value['bytes'] != size:
                    value['bytes'], changed = size, True
            for item in value.values():
                changed = refs(item, digests) or changed
        elif isinstance(value, list):
            for item in value:
                changed = refs(item, digests) or changed
        return changed

    for _ in range(16):
        digests = {original: (path.stat().st_size, sha(path.read_bytes())) for original, path in mapping.items()}
        changes = 0
        for path in mapping.values():
            if path.suffix == '.json':
                try:
                    value = read(path)
                except json.JSONDecodeError:
                    # A preserved failed command may have empty/non-JSON stdout.
                    continue
                if refs(value, digests):
                    save(path, value)
                    changes += 1
        if changes == 0:
            break
    else:
        raise AssertionError('identity repair did not converge')
    refs(manifest, digests)
    for item in manifest['files']:
        item['bytes'], item['sha256'] = digests[item['original_path']]
    save(root / 'packet-index.json', manifest)


def mutate(root, name):
    manifest = read(root / 'packet-index.json')
    mapping = {item['original_path']: root / item['member'] for item in manifest['files']}
    experiment = read(mapping[manifest['protocol']['path']])['root']

    def stage(name):
        path = mapping[experiment + '/' + name + '/summary.json']
        return path, read(path)

    def envelope(path, transform):
        raw = gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()
        value = json.loads(raw)
        transform(value)
        changed = (json.dumps(value, allow_nan=False) + '\n').encode()
        path.write_bytes(gzip.compress(changed, mtime=0) if path.suffix == '.gz' else changed)
        return {item['key']: item['value'] for item in value['fields']}

    def field(value, key):
        return next(item for item in value['fields'] if item['key'] == key)

    def result_value(value, precise=False):
        item = field(value, 'result_jsonl')
        rows = [json.loads(line) for line in item['value'].splitlines()]
        key = next(k for k, v in rows[0].items() if isinstance(v, float))
        rows[0][key] = math.nextafter(rows[0][key], math.inf) if precise else rows[0][key] + 1000.0
        item['value'] = ''.join(json.dumps(row) + '\n' for row in rows)
        return rows

    standard_path, standard = stage('standard-full43')
    single_path = mapping[standard['summary']['path']]
    single = read(single_path)
    output = mapping[str(Path(standard['summary']['path']).parent / 'q03_run1.stdout.json.gz')]
    diagnostic = ''
    if name == 'standard_query_coverage':
        single['records'].pop()
        save(single_path, single)
        diagnostic = 'for r in summary[\'records\']] == expected'
    elif name == 'complete_result':
        envelope(output, result_value)
        diagnostic = 'self.equivalent(actual, reference'
    elif name == 'engine_clock':
        def change(value):
            item = field(value, 'native_query_execution_nanos')
            item['value'] = str(int(item['value']) + 1)
        envelope(output, change)
        diagnostic = 'row[key] == value'
    elif name == 'fallback_flag':
        envelope(output, lambda value: field(value, 'fallback_attempted').update(value='true'))
        diagnostic = "v is False or (type(v) is str and v == 'false')"
    elif name == 'pair_order':
        path, value = stage('paired-a')
        path = mapping[value['summary']['path']]
        value = read(path)
        value['records'][0], value['records'][1] = value['records'][1], value['records'][0]
        save(path, value)
        diagnostic = 'for r in summary[\'records\']] == expected'
    elif name in ('training_coverage', 'profile_freshness', 'process_drain'):
        path, value = stage('training')
        if name == 'training_coverage':
            value['records'].pop()
            diagnostic = "len(data['records']) == 53"
        elif name == 'profile_freshness':
            value['profile_directory_empty_before'] = False
            diagnostic = "data['profile_directory_empty_before']"
        else:
            value['records'][0]['process_group_drained'] = False
            diagnostic = "row['process_group_drained']"
        save(path, value)
    elif name in ('python_values', 'python_strategy', 'python_row_count', 'typed_fallback'):
        _, training = stage('training')
        path = mapping[training['native_python']['path']]
        value = read(path)
        row = value['records'][1]
        if name == 'python_values':
            row['values'][0]['total'] += 1
            diagnostic = 'complete result mismatch'
        elif name == 'python_strategy':
            row['strategy'] = 'invented_strategy'
            diagnostic = "row['strategy'] == fields.get"
        else:
            key = 'output_row_count' if name == 'python_row_count' else 'external_engine_invoked'
            entries = [item for section in ('result', 'policy', 'lifecycle', 'capability_snapshot')
                       for item in row['envelope'].get(section, {}).get('fields', []) if item['key'] == key]
            assert entries
            for entry in entries:
                entry['value'] = '2' if name == 'python_row_count' else 'true'
            diagnostic = "int(fields['output_row_count'])" if name == 'python_row_count' else "v is False or (type(v) is str and v == 'false')"
        save(path, value)
    elif name == 'heldout_exact_float':
        path, value = stage('heldout-a')
        row = next(r for r in value['records'] if r['case'] == 'scalar_numeric')
        raw_path = mapping[row['compressed_stdout']['path']]
        def change(raw):
            row['values'] = result_value(raw, True)
        row['fields'] = envelope(raw_path, change)
        decoded = gzip.decompress(raw_path.read_bytes())
        row['stdout'].update(bytes=len(decoded), sha256=sha(decoded))
        save(path, value)
        diagnostic = 'complete result mismatch'
    elif name in ('pressure_grant', 'pressure_coverage'):
        _, pressure = stage('pressure-candidate')
        path = mapping[pressure['streaming']['path']]
        value = read(path)
        if name == 'pressure_coverage':
            value['cases'].pop()
            diagnostic = "for c in data['cases']] == expected_names"
        else:
            row = next(c for c in value['cases'] if 'peak_reserved_bytes' in c)
            row['peak_reserved_bytes'] = (1 << 30) + 1
            diagnostic = "case['peak_reserved_bytes'] <= 1 << 30"
        save(path, value)
    elif name == 'package_test_coverage':
        path, value = stage('package-candidate')
        proof_path = mapping[value['proof']['path']]
        proof = read(proof_path)
        proof['integration']['skipped'] = value['native_tests']['skipped'] = 1
        save(proof_path, proof)
        save(path, value)
        diagnostic = "proof['integration'] == data['native_tests']"
    elif name == 'candidate_sources':
        path, value = stage('candidate')
        key = next(iter(value['source_before']))
        value['source_before'][key] = value['source_after'][key] = '0' * 64
        save(path, value)
        diagnostic = "data['source_before'] == self.protocol['sources']"
    elif name == 'compiler_failure_preserved':
        path = mapping[manifest['compiler_probe']['path']]
        value = read(path)
        next(r for r in value['records'] if r['name'] == 'broken-run')['returncode'] = 0
        save(path, value)
        # The tiny compiler probe was frozen into the protocol before training;
        # repairing all dependent references must still trip that external pin.
        diagnostic = 'PROTOCOL_SHA256'
    elif name == 'layout_admission':
        path = mapping[str(Path(experiment).parent / 'pgo-ingest-admission-replay-981.json')]
        value = read(path)
        value['observations'][0]['source_batches'] += 1
        save(path, value)
        diagnostic = "sum(t['batches'] for t in tasks) == saved['source_batches']"
    elif name in ('retention_decision', 'unrun_ingests'):
        path = mapping[manifest['closeout']['path']]
        value = read(path)
        if name == 'retention_decision':
            value['decision'] = 'retain'
            diagnostic = "data['decision'] == 'drop'"
        else:
            value['candidate_ingest_count'] = 2
            diagnostic = "data['candidate_ingest_count'] == 0"
        save(path, value)
    elif name == 'missing_closeout':
        manifest['closeout'] = None
        diagnostic = 'closed investigation requires its closeout record'
    else:
        raise AssertionError(name)
    repair(root, manifest, mapping)
    return diagnostic


def authentication_controls(packet, verifier):
    results = []
    for member in ('repo/scripts/clickbench_reference_packet.py',
                   'repo/python/src/shardloom/_result_schema.py', 'repo/scripts/csv.py'):
        with tempfile.TemporaryDirectory(prefix='pgo-authentication-') as temporary:
            parent = Path(temporary)
            root = parent / 'packet'
            with tarfile.open(packet, 'r:gz') as archive:
                archive.extractall(root, filter='data')
            target, marker = root / member, parent / 'untrusted-helper-executed'
            raw = target.read_bytes() if target.exists() else b''
            target.write_bytes(raw + ('\nfrom pathlib import Path as _MarkerPath\n'
                f'_MarkerPath({str(marker)!r}).write_text("executed")\n').encode())
            manifest = read(root / 'packet-index.json')
            entry = next((row for row in manifest['files'] if row['member'] == member), None)
            if entry is None:
                entry = {'member': member, 'original_path': '/untrusted/' + member}
                manifest['files'].append(entry)
            entry.update(bytes=target.stat().st_size, sha256=sha(target.read_bytes()))
            save(root / 'packet-index.json', manifest)
            result = subprocess.run([sys.executable, '-I', '-B', str(verifier), str(root)],
                capture_output=True, text=True, timeout=30, check=False)
            assert result.returncode != 0 and 'archived helper identity mismatch' in result.stderr
            assert not marker.exists(), 'archived helper executed before authentication'
            results.append({'member': member, 'rejected_before_execution': True,
                            'manifest_identity_repaired': True})
    with tempfile.TemporaryDirectory(prefix='pgo-link-control-') as temporary:
        parent = Path(temporary)
        root = parent / 'packet'
        with tarfile.open(packet, 'r:gz') as archive:
            archive.extractall(root, filter='data')
        (root / 'untrusted-link').symlink_to(parent / 'missing-target')
        result = subprocess.run([sys.executable, '-I', '-B', str(verifier), str(root)],
            capture_output=True, text=True, timeout=30, check=False)
        assert result.returncode != 0 and 'symlinks are unsupported' in result.stderr
        results.append({'member': 'untrusted-link', 'rejected_before_execution': True})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).parent / 'packet.tar.gz')
    parser.add_argument('--verifier', type=Path, default=Path(__file__).parent / 'verify_pgo.py')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    packet, verifier = args.packet.resolve(), args.verifier.resolve()
    def replay(path):
        return subprocess.run([sys.executable, '-B', str(verifier), str(path)],
                              text=True, capture_output=True, timeout=120)
    archive_trust = check_archive_trust(verifier)
    original = replay(packet)
    assert original.returncode == 0, original.stderr
    authentication = authentication_controls(packet, verifier)
    cases = ['standard_query_coverage', 'complete_result', 'engine_clock', 'fallback_flag',
             'pair_order', 'training_coverage', 'profile_freshness', 'process_drain',
             'python_values', 'python_strategy', 'python_row_count', 'typed_fallback',
             'heldout_exact_float', 'pressure_grant', 'pressure_coverage', 'package_test_coverage',
             'candidate_sources', 'compiler_failure_preserved', 'layout_admission',
             'retention_decision', 'unrun_ingests', 'missing_closeout']
    results = []
    for name in cases:
        with tempfile.TemporaryDirectory(prefix='pgo-negative-') as temporary:
            root = Path(temporary)
            with tarfile.open(packet, 'r:gz') as archive:
                archive.extractall(root, filter='data')
            diagnostic = mutate(root, name)
            result = replay(root)
            assert result.returncode != 0, name
            assert diagnostic in result.stderr, (name, result.stderr)
            results.append(dict(case=name, rejected=True, returncode=result.returncode,
                semantic_diagnostic=diagnostic, traceback=result.stderr, outer_identities_repaired=True))
            print(name + ': rejected', flush=True)
    after = replay(packet)
    assert after.returncode == 0, after.stderr
    assert json.loads(original.stdout) == json.loads(after.stdout)
    receipt = dict(status='passed', negative_cases=len(results), all_rejected=True,
        original_passed_before_and_after=True, packet_sha256=sha(packet.read_bytes()),
        verifier_sha256=sha(verifier.read_bytes()), results=results, native_queries_executed=0,
        source_data_required=False, archive_trust=archive_trust, helper_authentication=authentication)
    if args.output:
        with args.output.open('x') as output:
            json.dump(receipt, output, indent=2)
            output.write('\n')
    print(json.dumps(dict(status='passed', negative_cases=len(results), native_queries_executed=0,
        archive_trust_negative_controls=len(archive_trust['negative_controls']),
        helper_authentication_controls=len(authentication))))


if __name__ == '__main__':
    main()
