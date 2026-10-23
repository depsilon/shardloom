"""Portable replay of attribution, rejected regions, and restored-engine evidence."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
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

    def run_replay(self, cwd, script, *arguments):
        result = subprocess.run([sys.executable, '-B', str(script), *map(str, arguments)],
            cwd=cwd, capture_output=True, text=True, timeout=180, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

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
