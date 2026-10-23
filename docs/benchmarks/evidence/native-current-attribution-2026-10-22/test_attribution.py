#!/usr/bin/env python3
"""Check semantic rejection after repairing outer member hashes."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile
import traceback
from archive_trust_controls import check as check_archive_trust

ROOT = Path(__file__).parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--packet', type=Path, default=ROOT / 'packet.tar.gz')
parser.add_argument('--verifier', type=Path, default=ROOT / 'verify_attribution.py')
parser.add_argument('--output', type=Path)
args = parser.parse_args()
ARCHIVE = args.packet.resolve()
spec = importlib.util.spec_from_file_location('attribution_verifier', args.verifier.resolve())
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def update_json(raw, change):
    value = json.loads(raw)
    change(value)
    return (json.dumps(value, indent=2) + '\n').encode()


def set_key(*keys, value):
    def change(data):
        parent = data
        for key in keys[:-1]:
            parent = parent[key]
        parent[keys[-1]] = value
    return change


cases = [
    ('incomplete_value_claim', 'evidence/attribution-result-953.json', set_key('records', 0, 'complete_values_match', value=False)),
    ('missing_native_call', 'evidence/attribution-result-953.json', lambda data: data['records'].pop()),
    ('query_identity_changed', 'evidence/attribution-result-953.json', set_key('records', 2, 'statement', value='SELECT 0')),
    ('external_execution_claim', 'evidence/current-attribution-review-956.json', set_key('external_engine_invoked', value=True)),
    ('counter_mismatch', 'evidence/current-attribution-review-956.json', set_key('calls', 2, 'whole_process_instructions', value=1)),
    ('engine_time_mismatch', 'evidence/current-attribution-review-956.json', set_key('calls', 2, 'engine_execution_seconds', value=999.0)),
    ('source_excerpt_changed', 'evidence/current-attribution-review-956.json', set_key('source_bindings', 0, 'excerpt', value='unbound source')),
    ('stack_exclusive_changed', 'evidence/attribution-cohort-953/stack-inventory-954.json', set_key('files', 0, 'threads', 0, 'frames', 0, 'exclusive_count', value=1)),
    ('raw_symbol_changed', 'evidence/attribution-cohort-953/stack-inventory-954.json', set_key('files', 0, 'threads', 0, 'frames', 0, 'function_symbol', value='invented_symbol')),
    ('demangled_symbol_changed', 'evidence/attribution-cohort-953/stack-inventory-954.json', set_key('files', 0, 'threads', 0, 'frames', 0, 'demangled_function_symbol', value='invented_symbol')),
    ('demangle_mapping_changed', 'evidence/attribution-cohort-953/stack-inventory-954.json', set_key('demangler', 'mapping', 0, 'demangled_output', value='invented_symbol')),
    ('insufficient_main_observations', 'evidence/current-attribution-review-956.json', set_key('lanes', 2, 'threads', 0, 'observations', value=99)),
    ('raw_counter_zero', 'evidence/attribution-cohort-953/q03-counter-control.timing.json', set_key('darwin_rusage_v4', 'ri_instructions', value=0)),
    ('undrained_call', 'evidence/attribution-result-953.json', set_key('records', 2, 'process_group_drained', value=False)),
    ('source_check_denied', 'evidence/attribution-result-953.json', set_key('source_and_binary_unchanged', value=False)),
]

with tarfile.open(ARCHIVE, 'r:gz') as archive:
    originals = {member.name: archive.extractfile(member).read() for member in archive.getmembers()}
manifest = json.loads(originals['manifest.json'])
names = {entry['original_path']: entry['member'] for entry in manifest['files']}


def reseal(blobs):
    """Repair the identity graph, so failures must reach semantic checks."""
    for _ in range(20):
        identities = {path: dict(bytes=len(blobs[name]), sha256=sha(blobs[name])) for path, name in names.items()}
        changed = False

        def walk(value):
            if isinstance(value, list):
                return [walk(item) for item in value]
            if not isinstance(value, dict):
                return value
            out = {key: walk(item) for key, item in value.items()}
            path = out.get('path') or out.get('original_path')
            if path in identities and 'sha256' in out:
                out['sha256'] = identities[path]['sha256']
                if 'bytes' in out:
                    out['bytes'] = identities[path]['bytes']
            # The missing-control record carries an intentionally explicit
            # result digest rather than a full identity object.
            if 'evidence_sha256' in out and out.get('file') == 'q01-sampled.sample.txt':
                out['evidence_sha256'] = sha(blobs['evidence/attribution-result-953.json'])
            return out

        for name, raw in list(blobs.items()):
            if not name.endswith('.json'):
                continue
            data = json.loads(raw)
            revised = walk(data)
            if revised != data:
                blobs[name] = (json.dumps(revised, indent=2) + '\n').encode()
                changed = True
        if not changed:
            return
    raise AssertionError('identity reseal did not converge')


archive_trust = check_archive_trust(args.verifier)
records = []
with tempfile.TemporaryDirectory(prefix='shardloom-attribution-negative-') as temporary:
    destination = Path(temporary)
    for name, raw in originals.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    positive = verifier.verify(destination)
    for name, member, mutation in cases:
        blobs = dict(originals)
        blobs[member] = update_json(blobs[member], mutation)
        reseal(blobs)
        for entry in json.loads(blobs['manifest.json'])['files']:
            assert sha(blobs[entry['member']]) == entry['sha256'], (name, entry['member'])
            assert len(blobs[entry['member']]) == entry['bytes']
        for relative, raw in blobs.items():
            (destination / relative).write_bytes(raw)
        try:
            verifier.verify(destination)
        except (AssertionError, ValueError, KeyError) as error:
            frames = traceback.extract_tb(error.__traceback__)
            records.append(dict(name=name, status='rejected', outer_hashes_repaired=True,
                                error_type=type(error).__name__, verifier_line=frames[-1].lineno,
                                statement=frames[-1].line))
        else:
            raise AssertionError(f'corruption accepted: {name}')
    for relative, raw in originals.items():
        (destination / relative).write_bytes(raw)
    assert verifier.verify(destination) == positive
receipt = dict(status='passed', archive=dict(path=str(ARCHIVE), bytes=ARCHIVE.stat().st_size,
                                           sha256=sha(ARCHIVE.read_bytes())),
               verifier_sha256=sha(args.verifier.read_bytes()),
               test_sha256=sha(Path(__file__).read_bytes()), positive=positive,
               negative_controls=records, archive_trust=archive_trust, restored_positive=True,
               runtime_executed=False, note='Every negative control has all outer member hashes repaired before replay.')
if args.output:
    with args.output.open('x') as output:
        output.write(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(dict(status='passed', negative_controls=len(records),
    archive_trust_negative_controls=len(archive_trust['negative_controls']), runtime_executed=False), indent=2))
