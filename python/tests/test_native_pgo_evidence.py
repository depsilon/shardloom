"""Relocated, engine-free replay of the finite PGO drop and its failure controls."""
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
EVIDENCE = ROOT / 'docs/benchmarks/evidence/native-pgo-2026-10-23'


class NativePgoEvidenceTests(unittest.TestCase):
    def relocated(self, destination):
        if not (EVIDENCE / 'packet.tar.gz').exists():
            if (ROOT / '.git').exists():
                self.fail('tracked native PGO evidence is missing')
            self.skipTest('historical evidence bundles are excluded from exported source archives')
        shutil.copytree(EVIDENCE, destination)
        manifest = json.loads((destination / 'manifest.json').read_text())
        for entry in manifest['files']:
            raw = (destination / entry['file']).read_bytes()
            self.assertEqual(len(raw), entry['bytes'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), entry['sha256'])
        return destination

    def replay(self, directory, *, sequential_sum=False):
        command = [sys.executable, '-B']
        if sequential_sum:
            command.extend(['-c',
                'import builtins, functools, operator, runpy, sys; '
                'builtins.sum = lambda values, start=0: functools.reduce(operator.add, values, start); '
                'sys.argv = sys.argv[1:]; runpy.run_path(sys.argv[0], run_name="__main__")'])
        command.extend([str(directory / 'verify_pgo.py'), str(directory / 'packet.tar.gz')])
        result = subprocess.run(command, cwd=directory.parent, text=True,
                                capture_output=True, timeout=180, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return json.loads(result.stdout)

    def test_closed_drop_replays_without_engine_or_dataset(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated(Path(temporary) / 'evidence')
            result = self.replay(directory)
            self.assertEqual(result['status'], 'passed')
            self.assertEqual(result['decision'], 'drop')
            self.assertFalse(result['engine_executed'])
            self.assertFalse(result['release_default_changed'])
            self.assertTrue(result['retention_gate_failed'])
            self.assertEqual([r['query_calls'] for r in result['full43']], [43, 86, 86])
            self.assertEqual([r['calls'] for r in result['heldout'].values()], [102, 102])
            self.assertEqual(result['ingest']['candidate_ingest_calls'], 0)
            self.assertIsNone(result['closeout']['complete_workflow_savings'])
            self.assertIsNone(result['closeout']['break_even_reuses'])
            self.assertEqual(result['closeout']['status'], 'closed')

    def test_replay_with_sequential_float_sum(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated(Path(temporary) / 'evidence')
            result = self.replay(directory, sequential_sum=True)
            self.assertEqual(result['status'], 'passed')
            self.assertEqual(result['full43'][0]['engine_totals']['candidate'], 53.109073585)
            self.assertEqual(result['ingest']['diagnostic_engine_seconds'], 43.268958791)

    def test_semantic_and_archive_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated(Path(temporary) / 'evidence')
            result = subprocess.run([sys.executable, '-B', str(directory / 'test_pgo.py')],
                cwd=directory.parent, text=True, capture_output=True, timeout=600, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            controls = json.loads(result.stdout.splitlines()[-1])
            self.assertEqual(controls['negative_cases'], 22)
            self.assertEqual(controls['archive_trust_negative_controls'], 23)
            self.assertEqual(controls['helper_authentication_controls'], 4)
            self.assertEqual(controls['native_queries_executed'], 0)


if __name__ == '__main__':
    unittest.main()
