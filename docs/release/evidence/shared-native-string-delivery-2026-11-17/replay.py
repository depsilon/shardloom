"""Verify and replay the saved string observations; execute no native query."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repository', type=Path)
    args = parser.parse_args()
    packet = args.packet.resolve(strict=True)
    report = json.loads((packet / 'validation.json').read_text())
    assert report['schema_version'] == 'shardloom.string_delivery_acceptance.v1'
    assert report['status'] == 'local_checks_passed'
    assert report['archive']['path'] == 'observations.json.gz'
    raw = (packet / 'observations.json.gz').read_bytes()
    assert len(raw) == report['archive']['bytes'] and sha(raw) == report['archive']['sha256']
    payload = json.loads(gzip.decompress(raw))
    assert payload['schema_version'] == 'shardloom.string_delivery_raw.v1'
    files = payload['files']
    assert {name: sha(value.encode()) for name, value in files.items()} == payload['file_sha256']
    assert {name: sha(value.encode()) for name, value in payload['sources'].items()} == payload['source_sha256'] == report['source_sha256']
    if args.repository:
        for name, digest in report['source_sha256'].items():
            assert sha((args.repository / name).read_bytes()) == digest, name
    overlay = json.loads(files['string-attribution-source-overlay-r1.json'])
    assert {name: sha(value.encode()) for name, value in overlay['sources'].items()} == overlay['source_sha256'] == report['attribution_source_sha256']
    build = json.loads(files['string-queue-release-build-r1.json'])
    observation = json.loads(files['string-queue-attribution-r1/receipt.json'])
    assert observation['source_files'] == build['source_files']
    assert observation['build_receipt_sha256'] == payload['file_sha256']['string-queue-release-build-r1.json']
    assert observation['build_log_sha256'] == payload['file_sha256']['logs/string-queue-release-build-r1.log']
    assert observation['protocol_sha256'] == payload['file_sha256']['string-queue-attribution-design-r2.md']
    for name, digest in overlay['source_sha256'].items():
        assert build['source_files'][name]['sha256'] == digest
    for check in report['checks']:
        receipt_name = check['name'] + '.json'
        log_name = 'logs/' + check['name'] + '.log'
        assert payload['file_sha256'][receipt_name] == check['receipt_sha256']
        assert payload['file_sha256'][log_name] == check['log_sha256']
        receipt = json.loads(files[receipt_name])
        assert receipt['process_group_drained']
        if check['name'] == 'incorporation-native-suite-r1':
            supplement = json.loads(files['incorporation-native-debug-stop-verification-r1.json'])
            assert supplement['sources_unchanged_independently_verified']
            assert supplement['receipt_sha256'] == payload['file_sha256'][receipt_name]
            assert supplement['log_sha256'] == payload['file_sha256'][log_name]
            assert receipt['interrupted_signal'] == 2 and not supplement['complete_suite']
        else:
            assert receipt['sources_unchanged']
        assert receipt['status'] == check['status'] and receipt['command'] == check['command']
    assert sum(check['status'] == 'failed' for check in report['checks']) == 5
    # Restore only these known replay inputs, never arbitrary archived paths or executables.
    args.output.mkdir(parents=True, exist_ok=False)
    for name in ['string-queue-attribution-r1/receipt.json',
                 'string-queue-attribution-r1/observations.log', 'analyze_string_queue_attribution.py']:
        path = args.output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(files[name])
    result = subprocess.run([sys.executable, '-B', str((args.output / 'analyze_string_queue_attribution.py').resolve())],
                            cwd=args.output, check=True, text=True, capture_output=True, timeout=30)
    reanalysis = json.loads((args.output / 'string-queue-attribution-analysis-r1.json').read_text())
    assert reanalysis == json.loads(files['string-queue-attribution-analysis-r1.json']) == report['analysis']
    receipt = dict(schema_version='shardloom.string_delivery_replay.v1', status='passed',
                   archive_sha256=sha(raw), validation_sha256=sha((packet / 'validation.json').read_bytes()),
                   source_bound=args.repository is not None, files_verified=len(files),
                   final_sources_verified=len(payload['sources']), attribution_sources_verified=len(overlay['sources']),
                   observations_verified=reanalysis['observations'], native_queries_executed=0,
                   analyzer_stdout=result.stdout)
    (args.output / 'replay.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    print(json.dumps({key: receipt[key] for key in ['status', 'files_verified', 'observations_verified', 'native_queries_executed']}))


if __name__ == '__main__':
    main()
