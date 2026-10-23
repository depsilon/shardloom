#!/usr/bin/env python3
"""Check restoration evidence rejection after repairing all outer identities."""
import argparse
import gzip
import importlib.util
import json
from pathlib import Path
import tempfile
import traceback
from archive_trust_controls import check as check_archive_trust

ROOT = Path(__file__).parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--packet', type=Path, default=ROOT / 'restoration.tar.gz')
parser.add_argument('--region-packet', type=Path, default=ROOT / 'packet.tar.gz')
parser.add_argument('--output', type=Path)
args = parser.parse_args()
spec = importlib.util.spec_from_file_location('restoration_replay', ROOT / 'verify_restoration.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def reseal(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    mapping = {item['original_path']: root / item['member'] for item in manifest['files']}
    for _ in range(20):
        identities = {original: dict(bytes=path.stat().st_size, sha256=verifier.digest(path.read_bytes()))
                      for original, path in mapping.items()}
        changed = False

        def repair(value):
            nonlocal changed
            if isinstance(value, dict):
                original = value.get('path') or value.get('original_path')
                if original in identities and 'sha256' in value:
                    for key, replacement in identities[original].items():
                        if key in value and value[key] != replacement:
                            value[key] = replacement
                            changed = True
                for item in value.values():
                    repair(item)
            elif isinstance(value, list):
                for item in value:
                    repair(item)

        for path in mapping.values():
            if path.suffix == '.json':
                value = json.loads(path.read_text())
                old = json.dumps(value, sort_keys=True)
                repair(value)
                if json.dumps(value, sort_keys=True) != old:
                    save(path, value)
        repair(manifest)
        save(root / 'manifest.json', manifest)
        if not changed:
            break
    else:
        raise AssertionError('identity repair did not converge')
    verifier.members(root)


archive_trust = check_archive_trust(ROOT / 'verify_restoration.py')
cases = ('retained_candidate', 'source_hash', 'check_drain', 'query_coverage',
         'complete_values', 'engine_clock', 'input_identity')
results = []
with tempfile.TemporaryDirectory(prefix='restoration-negative-') as temporary:
    root = Path(temporary)
    regions, restored = root / 'regions', root / 'restored'
    assert verifier.digest(args.region_packet.read_bytes()) == verifier.REGION_SHA
    verifier.unpack(args.region_packet, regions)
    verifier.unpack(args.packet, restored)
    originals = {p.relative_to(restored): p.read_bytes() for p in restored.rglob('*') if p.is_file()}
    positive = verifier.verify(restored, regions)
    for case in cases:
        for path, raw in originals.items():
            (restored / path).write_bytes(raw)
        manifest, mapping = verifier.members(restored)
        record_path = mapping[manifest['restored']['path']]
        record = json.loads(record_path.read_text())
        if case == 'retained_candidate':
            path = mapping[record['restoration']['path']]
            value = json.loads(path.read_text())
            value['no_candidate_retained'] = False
            save(path, value)
        elif case in ('source_hash', 'check_drain'):
            path = mapping[record['restoration_checks'][1]['path']]
            value = json.loads(path.read_text())
            if case == 'source_hash':
                value['source_after']['shardloom-cli/src/public_workflow_route.rs'] = '0' * 64
            else:
                value['process_group_drained'] = False
            save(path, value)
        elif case == 'query_coverage':
            path = mapping[record['summary']['path']]
            value = json.loads(path.read_text())
            value['records'].pop()
            save(path, value)
        elif case in ('complete_values', 'engine_clock'):
            path = mapping[str(Path(record['summary']['path']).parent / 'q03_run1.stdout.json.gz')]
            value = json.loads(gzip.decompress(path.read_bytes()))
            field = next(item for item in value['fields'] if item['key'] ==
                         ('result_jsonl' if case == 'complete_values' else 'native_query_execution_nanos'))
            if case == 'complete_values':
                values = [json.loads(line) for line in field['value'].splitlines()]
                key = next(k for k, v in values[0].items() if isinstance(v, float))
                values[0][key] += 1.0
                field['value'] = ''.join(json.dumps(row) + '\n' for row in values)
            else:
                field['value'] = str(int(field['value']) + 1)
            path.write_bytes(gzip.compress(json.dumps(value).encode(), mtime=0))
        elif case == 'input_identity':
            record['input']['sha256'] = '0' * 64
            save(record_path, record)
        reseal(restored)
        try:
            verifier.verify(restored, regions)
        except (AssertionError, ValueError, KeyError) as error:
            frame = traceback.extract_tb(error.__traceback__)[-1]
            results.append(dict(case=case, rejected=True, outer_identities_repaired=True,
                                verifier_line=frame.lineno, statement=frame.line))
        else:
            raise AssertionError('corruption accepted: ' + case)
    for path, raw in originals.items():
        (restored / path).write_bytes(raw)
    assert verifier.verify(restored, regions) == positive
receipt = dict(status='passed', negative_cases=len(results), results=results,
    original_passed_before_and_after=True, runtime_executed=False, archive_trust=archive_trust,
    packet_sha256=verifier.digest(args.packet.read_bytes()),
    verifier_sha256=verifier.digest((ROOT / 'verify_restoration.py').read_bytes()))
if args.output:
    with args.output.open('x') as output:
        output.write(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(dict(status='passed', negative_cases=len(results), runtime_executed=False,
    archive_trust_negative_controls=len(archive_trust['negative_controls']))))
