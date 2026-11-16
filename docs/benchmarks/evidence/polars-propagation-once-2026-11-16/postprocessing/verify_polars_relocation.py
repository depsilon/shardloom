"""Check the exact portable packet outside every original evidence root."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

P = Path(__file__).resolve().parents[1]
PACKET = P / 'polars-packet-r4'


def content(path):
    before = path.stat()
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    after = path.stat()
    generation = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    assert generation(before) == generation(after)
    return dict(bytes=before.st_size, sha256=digest)


def final_result(path):
    return json.loads(path.read_text().splitlines()[-1])


def main():
    target = P / 'polars-relocation-verification-r1.json'
    assert not target.exists()
    original = final_result(P / 'portable-check-r4.log')
    assert original['status'] == 'passed_complete_portable_evidence_replay_with_retained_failures'
    original_guard = json.loads((P / 'portable-check-r4.json').read_bytes())
    assert original_guard['status'] == 'passed' and original_guard['returncode'] == 0
    assert original['queries_executed'] == 0
    assert original['polars_attempts_replayed'] == original['historical_native_results_replayed'] == 11579
    files = {p.name: content(p) for p in PACKET.iterdir()}
    with tempfile.TemporaryDirectory(prefix='shardloom-polars-relocated-', dir='/private/tmp') as temporary:
        root = Path(temporary)
        assert shutil.disk_usage(root).free >= (12 << 30) + sum(r['bytes'] for r in files.values())
        packet = root / 'packet'
        shutil.copytree(PACKET, packet)
        assert {p.name: content(p) for p in packet.iterdir()} == files
        subprocess.run([sys.executable, '-B', str(P / 'run_preparation.py'),
            '--name', 'relocation-check-r1', '--timeout', '1800', '--',
            sys.executable, '-I', '-B', str(packet / 'verify.py')], check=True, timeout=1860)
        receipt = json.loads((P / 'relocation-check-r1.json').read_bytes())
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        assert receipt['source_unchanged'] and receipt['direct_child_reaped'] and receipt['process_group_drained']
        relocated = final_result(P / 'relocation-check-r1.log')
        assert relocated == original
        assert {p.name: content(p) for p in packet.iterdir()} == files
    assert {p.name: content(p) for p in PACKET.iterdir()} == files
    result = dict(status='passed', identical_complete_replay=True, queries_executed=0,
        original_reads_engine_imports_processes_network_forbidden=True,
        relocated_outside_original_roots=True, temporary_copy_removed=True,
        result=relocated, packet_files=files,
        original_guard=content(P / 'portable-check-r4.json'),
        relocated_guard=content(P / 'relocation-check-r1.json'),
        original_log=content(P / 'portable-check-r4.log'),
        relocated_log=content(P / 'relocation-check-r1.log'), verifier=content(Path(__file__)))
    with target.open('x') as output:
        json.dump(result, output, indent=2, allow_nan=False); output.write('\n')
    print(json.dumps(dict(status='passed', polars_attempts_replayed=11579,
        historical_native_results_replayed=11579, queries_executed=0)), flush=True)


if __name__ == '__main__':
    main()
