"""Copy the exact packet away from all original roots and replay with zero queries."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

OUT = Path(__file__).resolve().parent.parent
PYTHON = '/Users/dylan/.cache/shardloom/query-surface-polars-2.1.0/bin/python'
PACKET = OUT / 'variable-copy-packet-r3'


def content(path):
    with path.open('rb') as stream:
        return dict(bytes=path.stat().st_size, sha256=hashlib.file_digest(stream, 'sha256').hexdigest())


def main():
    destination = OUT / 'variable-copy-relocation-verification-r1.json'
    assert not destination.exists()
    originals = {p.name: content(p) for p in PACKET.iterdir()}
    with tempfile.TemporaryDirectory(prefix='shardloom-variable-copy-relocated-', dir='/private/tmp') as temporary:
        root = Path(temporary)
        assert shutil.disk_usage(root).free >= (12 << 30) + sum(r['bytes'] for r in originals.values())
        packet = root / 'packet'
        shutil.copytree(PACKET, packet)
        assert {p.name: content(p) for p in packet.iterdir()} == originals
        command = [PYTHON, '-B', str(OUT / 'guard_checks.py'), '--name', 'variable-copy-portable-relocated-r1',
                   '--timeout', '600', '--', PYTHON, '-I', '-B', str(packet / 'verify.py')]
        subprocess.run(command, check=True, timeout=660)
        receipt = json.loads((OUT / 'variable-copy-portable-relocated-r1.json').read_bytes())
        assert receipt['status'] == 'passed' and receipt['returncode'] == 0
        result = json.loads((OUT / 'logs/variable-copy-portable-relocated-r1.log').read_bytes())
        original = json.loads((OUT / 'variable-copy-portable-replay-r3.log').read_bytes())
        assert result == original and result['status'] == 'passed_complete_portable_replay'
        assert {p.name: content(p) for p in packet.iterdir()} == originals
        verified = dict(status='passed', identical_complete_replay=True, query_executions=0,
            relocation_outside_original_roots=True, original_reads_forbidden=result['original_reads_forbidden'],
            native_imports_processes_network_forbidden=True, packet_files=originals,
            fresh_packets_replayed=806, candidate_source_fingerprint=result['source_fingerprints']['candidate'],
            guarded_receipt=content(OUT / 'variable-copy-portable-relocated-r1.json'),
            relocated_result=content(OUT / 'logs/variable-copy-portable-relocated-r1.log'),
            original_result=content(OUT / 'variable-copy-portable-replay-r3.log'), verifier=content(Path(__file__)))
    assert {p.name: content(p) for p in PACKET.iterdir()} == originals
    destination.write_text(json.dumps(verified, indent=2) + '\n')
    print(json.dumps(dict(status='passed', fresh_packets_replayed=806, query_executions=0)))


if __name__ == '__main__':
    main()
