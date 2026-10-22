"""Keep the historical selected-guarantee proof replayable in ordinary CI."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'docs/benchmarks/evidence/perf12-guarantees-2026-10-22'


class Perf12GuaranteeEvidenceTests(unittest.TestCase):
    def test_retained_bindings_and_corruption_controls(self):
        if not (EVIDENCE / 'packet.json.xz').exists():
            if (ROOT / '.git').exists():
                self.fail('tracked guarantee packet is missing from the checkout')
            self.skipTest('historical evidence bundles are excluded from exported source archives')
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'proof.json'
            completed = subprocess.run(
                [sys.executable, '-B', str(EVIDENCE / 'test_guarantees.py'), '--output', str(output)],
                cwd=temporary, capture_output=True, text=True, timeout=120, check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            result = json.loads(output.read_text(encoding='utf-8'))
        self.assertEqual(result['status'], 'passed_guarantee_corruption_checks')
        self.assertEqual(result['negative_case_count'], 52)
        self.assertFalse(result['runtime_executed'])
        self.assertFalse(result['positive']['current_source_checked'])
        self.assertEqual(result['positive']['selected_installed_observations'], 60)


if __name__ == '__main__':
    unittest.main()
