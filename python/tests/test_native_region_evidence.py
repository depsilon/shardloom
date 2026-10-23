"""Portable replay of attribution, rejected regions, and restored-engine evidence."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'docs/benchmarks/evidence'


class NativeRegionEvidenceTests(unittest.TestCase):
    def relocated(self, name, destination):
        source = EVIDENCE / name
        if not (source / 'packet.tar.gz').exists():
            if (ROOT / '.git').exists():
                self.fail('tracked native prerequisite evidence is missing: ' + name)
            self.skipTest('historical evidence bundles are excluded from exported source archives')
        shutil.copytree(source, destination)
        manifest = json.loads((destination / 'manifest.json').read_text())
        for entry in manifest['files']:
            raw = (destination / entry['file']).read_bytes()
            self.assertEqual(len(raw), entry['bytes'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), entry['sha256'])
        return destination

    def run_replay(self, cwd, script, *arguments, sequential_sum=False):
        command = [sys.executable, '-B']
        if sequential_sum:
            # Reproduce Python <=3.11's sequential float accumulation even on
            # newer interpreters, which changed built-in sum in Python 3.12.
            command.extend(['-c',
                'import builtins, functools, operator, runpy, sys; '
                'builtins.sum = lambda values, start=0: functools.reduce(operator.add, values, start); '
                'sys.argv = sys.argv[1:]; runpy.run_path(sys.argv[0], run_name="__main__")'])
        command.extend([str(script), *map(str, arguments)])
        result = subprocess.run(command,
            cwd=cwd, capture_output=True, text=True, timeout=180, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def test_timing_replay_with_sequential_float_sum(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated('native-scalar-regions-2026-10-22', Path(temporary) / 'regions')
            for script, packet in [('verify_regions.py', 'packet.tar.gz'),
                                   ('verify_restoration.py', 'restoration.tar.gz')]:
                with self.subTest(script=script):
                    result = json.loads(self.run_replay(temporary, directory / script,
                        directory / packet, sequential_sum=True))
                    self.assertEqual(result['status'], 'passed')

    def test_archived_helpers_authenticated_before_import(self):
        variants = [
            ('native-current-attribution-2026-10-22', 'verify_attribution.py'),
            ('native-scalar-regions-2026-10-22', 'verify_regions.py'),
            ('native-scalar-regions-2026-10-22', 'verify_restoration.py'),
        ]
        members = ['repo/scripts/clickbench_reference_packet.py',
                   'repo/python/src/shardloom/_result_schema.py',
                   'repo/scripts/csv.py']
        for name, script in variants:
            for member in members:
                with self.subTest(script=script, member=member), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    directory = self.relocated(name, root / 'verifier')
                    extracted = root / 'packet'
                    with tarfile.open(directory / 'packet.tar.gz') as archive:
                        archive.extractall(extracted, filter='data')
                    marker = root / 'untrusted-helper-executed'
                    target = extracted / member
                    raw = target.read_bytes() if target.exists() else b''
                    target.write_bytes(raw + ('\nfrom pathlib import Path as _ProofPath\n'
                        f'_ProofPath({str(marker)!r}).write_text("executed")\n').encode())
                    manifest_path = extracted / 'manifest.json'
                    manifest = json.loads(manifest_path.read_text())
                    entry = next((row for row in manifest['files'] if row['member'] == member), None)
                    if entry is None:
                        entry = {'member': member, 'original_path': '/untrusted/' + member}
                        manifest['files'].append(entry)
                    entry.update(bytes=target.stat().st_size,
                                 sha256=hashlib.sha256(target.read_bytes()).hexdigest())
                    manifest_path.write_text(json.dumps(manifest))
                    arguments = [str(extracted)]
                    if script == 'verify_restoration.py':
                        restored = root / 'restoration'
                        with tarfile.open(directory / 'restoration.tar.gz') as archive:
                            archive.extractall(restored, filter='data')
                        arguments = [str(restored), str(extracted)]
                    result = subprocess.run([sys.executable, '-I', '-B', '-c',
                        'import importlib.util, pathlib, sys; '
                        'spec = importlib.util.spec_from_file_location("verifier", sys.argv[1]); '
                        'module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); '
                        'module.verify(*(pathlib.Path(p) for p in sys.argv[2:]))',
                        str(directory / script), *arguments], cwd=root,
                        capture_output=True, text=True, timeout=180, check=False)
                    self.assertFalse(marker.exists(), 'archive code executed before authentication')
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('archived helper identity mismatch', result.stderr)

    def test_current_attribution_and_semantic_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated('native-current-attribution-2026-10-22', Path(temporary) / 'attribution')
            result = json.loads(self.run_replay(temporary, directory / 'verify_attribution.py', directory / 'packet.tar.gz'))
            self.assertEqual(result['status'], 'passed')
            controls = json.loads(self.run_replay(temporary, directory / 'test_attribution.py'))
            self.assertEqual(controls['negative_controls'], 15)
            self.assertFalse(controls['runtime_executed'])

    def test_region_rejection_and_semantic_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated('native-scalar-regions-2026-10-22', Path(temporary) / 'regions')
            result = json.loads(self.run_replay(temporary, directory / 'verify_regions.py', directory / 'packet.tar.gz'))
            self.assertEqual(result['calls'], 226)
            self.assertEqual(result['decision'], 'drop')
            self.assertFalse(result['experiment_complete'])
            self.assertFalse(result['performance_comparison_admitted'])
            self.assertEqual(result['unexecuted_calls'], 41)
            self.assertFalse(result['native_execution_during_replay'])
            output = self.run_replay(temporary, directory / 'test_regions.py')
            controls = json.loads(output.splitlines()[-1])
            self.assertEqual(controls['negative_cases'], 13)
            self.assertEqual(controls['native_queries_executed'], 0)

    def test_restoration_and_semantic_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated('native-scalar-regions-2026-10-22', Path(temporary) / 'restoration')
            result = json.loads(self.run_replay(temporary, directory / 'verify_restoration.py', directory / 'restoration.tar.gz'))
            self.assertEqual(result['source_files_restored'], 1922)
            self.assertEqual(result['required_checks'], 3)
            self.assertEqual(result['restored_engine_seconds'], 48.479745297)
            self.assertFalse(result['runtime_executed'])
            controls = json.loads(self.run_replay(temporary, directory / 'test_restoration.py'))
            self.assertEqual(controls['negative_cases'], 7)
            self.assertFalse(controls['runtime_executed'])


if __name__ == '__main__':
    unittest.main()
