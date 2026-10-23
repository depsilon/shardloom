"""Corrupt saved experiment evidence, repair outer hashes, require rejection."""
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

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--packet', type=Path, default=Path(__file__).parent / 'packet.tar.gz')
parser.add_argument('--verifier', type=Path, default=Path(__file__).parent / 'verify_regions.py')
parser.add_argument('--output', type=Path)
args = parser.parse_args()
PACKET = args.packet.resolve()
VERIFY = args.verifier.resolve()


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
                    value['sha256'] = digest
                    changed = True
                if 'bytes' in value and value['bytes'] != size:
                    value['bytes'] = size
                    changed = True
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
                value = read(path)
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
    save(root / 'manifest.json', manifest)


def mutate(root, name):
    manifest = read(root / 'manifest.json')
    mapping = {item['original_path']: root / item['member'] for item in manifest['files']}
    standard_path = mapping[manifest['standard']['path']]
    standard = read(standard_path)
    single_path = mapping[standard['summary']['path']]
    single = read(single_path)
    full_path = mapping[manifest['stages']['full43-A']['path']]
    full = read(full_path)
    held_path = mapping[manifest['stages']['heldout-A']['path']]
    held = read(held_path)
    single_output = mapping[str(Path(standard['summary']['path']).parent / 'q03_run1.stdout.json.gz')]
    held_prefix = str(Path(manifest['stages']['heldout-A']['path']).parent / 't_signed-candidate')
    held_output = mapping[held_prefix + '.stdout.json']

    def mutate_envelope(path, transform):
        raw = gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()
        value = json.loads(raw)
        transform(value)
        changed = (json.dumps(value, allow_nan=False) + '\n').encode()
        path.write_bytes(gzip.compress(changed, mtime=0) if path.suffix == '.gz' else changed)
        return {item['key']: item['value'] for item in value['fields']}

    def field(value, key):
        return next(item for item in value['fields'] if item['key'] == key)

    def changed_value(value, next_float=False):
        item = field(value, 'result_jsonl')
        rows = [json.loads(line) for line in item['value'].splitlines()]
        key = next(k for k, v in rows[0].items() if isinstance(v, float))
        rows[0][key] = math.nextafter(rows[0][key], math.inf) if next_float else rows[0][key] + 1.0
        item['value'] = ''.join(json.dumps(row) + '\n' for row in rows)
        return rows

    if name == 'standard_query_coverage':
        single['records'].pop()
        save(single_path, single)
    elif name == 'complete_result':
        mutate_envelope(single_output, changed_value)
    elif name == 'engine_clock':
        def change(value):
            item = field(value, 'native_query_execution_nanos')
            item['value'] = str(int(item['value']) + 1)
        mutate_envelope(single_output, change)
    elif name == 'fallback_flag':
        def change(value):
            field(value, 'fallback_attempted')['value'] = 'true'
        mutate_envelope(single_output, change)
    elif name == 'pair_order':
        full['records'][0], full['records'][1] = full['records'][1], full['records'][0]
        save(full_path, full)
    elif name in ('target_activation', 'outstanding_regions', 'cpu_grant', 'heldout_exact_float'):
        row = next(r for r in held['records'] if (r['case'], r['role']) == ('t_signed', 'candidate'))
        def change(value):
            if name == 'heldout_exact_float':
                row['actual'] = changed_value(value, True)
                return
            key, replacement = {
                'target_activation': ('local_primitive_aggregate_source_region_execution', 'false'),
                'outstanding_regions': ('local_primitive_aggregate_source_region_outstanding', '1'),
                'cpu_grant': ('local_primitive_aggregate_source_region_peak_active_workers', '4'),
            }[name]
            field(value, key)['value'] = replacement
        row['fields'] = mutate_envelope(held_output, change)
        save(held_path, held)
    elif name == 'stage_score':
        full['score']['aggregate_passed'] = True
        full['retention_gate_passed'] = True
        save(full_path, full)
    elif name == 'candidate_source':
        source = next(x for x in manifest['candidate_sources'] if x['relative_path'].endswith('/scalar_region_workers.rs'))
        path = mapping[source['identity']['path']]
        path.write_bytes(path.read_bytes() + b'\n// deliberately changed archived source\n')
    elif name == 'public_regression_log':
        runtime = read(mapping[manifest['runtime']['path']])
        reference = next(r for r in runtime['checks'] if 'region-public-tests-' in r['path'])
        check_path = mapping[reference['path']]
        check = read(check_path)
        log = mapping[str(Path(reference['path']).with_suffix('.log'))]
        original = log.read_text()
        changed = '\n'.join(line for line in original.splitlines()
                            if 'source_region_observations_survive_public_summary_without_invented_counters' not in line) + '\n'
        assert changed != original
        log.write_text(changed)
        check['log_sha256'] = sha(log.read_bytes())
        save(check_path, check)
    elif name == 'process_drain':
        closeout_path = mapping[manifest['failure_closeout']['path']]
        closeout = read(closeout_path)
        closeout['all_launched_process_groups_drained'] = False
        save(closeout_path, closeout)
    else:
        raise AssertionError(name)
    repair(root, manifest, mapping)


def replay(path):
    return subprocess.run([sys.executable, '-B', str(VERIFY), str(path)],
                          text=True, capture_output=True, timeout=120)


archive_trust = check_archive_trust(VERIFY)
original = replay(PACKET)
assert original.returncode == 0, original.stderr
cases = ['standard_query_coverage', 'complete_result', 'engine_clock', 'fallback_flag',
         'pair_order', 'target_activation', 'outstanding_regions', 'cpu_grant',
         'heldout_exact_float', 'stage_score', 'candidate_source', 'public_regression_log',
         'process_drain']
results = []
for name in cases:
    with tempfile.TemporaryDirectory(prefix='region-negative-') as temporary:
        root = Path(temporary)
        with tarfile.open(PACKET, 'r:gz') as archive:
            archive.extractall(root, filter='data')
        mutate(root, name)
        result = replay(root)
        assert result.returncode != 0, name
        results.append(dict(case=name, rejected=True, returncode=result.returncode,
                            diagnostic=result.stderr.strip().splitlines()[-1],
                            traceback=result.stderr, outer_identities_repaired=True))
        print(name + ': rejected', flush=True)
after = replay(PACKET)
assert after.returncode == 0, after.stderr
assert json.loads(original.stdout) == json.loads(after.stdout)
receipt = dict(status='passed', negative_cases=len(results), all_rejected=True,
    original_passed_before_and_after=True, packet_sha256=sha(PACKET.read_bytes()),
    verifier_sha256=sha(VERIFY.read_bytes()), results=results,
    native_queries_executed=0, source_data_required=False, archive_trust=archive_trust)
if args.output:
    with args.output.open('x') as output:
        json.dump(receipt, output, indent=2)
        output.write('\n')
print(json.dumps(dict(status='passed', negative_cases=len(results), native_queries_executed=0,
    archive_trust_negative_controls=len(archive_trust['negative_controls']))))
