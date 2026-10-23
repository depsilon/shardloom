"""Replay historical independent correctness evidence without running an engine."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "docs/benchmarks/evidence/native-independent-full43-2026-10-22"


class IndependentFull43EvidenceTests(unittest.TestCase):
    def test_complete_results_and_corruption_controls(self):
        if not (EVIDENCE / "records.json.xz").exists():
            if (ROOT / ".git").exists():
                self.fail("tracked independent Full43 packet is missing from the checkout")
            self.skipTest("historical evidence bundles are excluded from exported source archives")
        with tempfile.TemporaryDirectory() as temporary:
            # Replay from another checkout location even on the packet producer's
            # machine. Historical member identities must not use this root.
            repository = Path(temporary) / "relocated-repository"
            for directory in ("scripts", "python/src"):
                for source in (ROOT / directory).rglob("*.py"):
                    destination = repository / source.relative_to(ROOT)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, destination)
            environment = dict(
                os.environ,
                SHARDLOOM_CORRECTNESS_PACKET=str(EVIDENCE),
                SHARDLOOM_CORRECTNESS_REPOSITORY=str(repository),
            )
            replay = subprocess.run(
                [sys.executable, "-B", str(EVIDENCE / "verify_correctness_packet_950.py"),
                 "--review", str(EVIDENCE / "review.json"),
                 "--packet", str(EVIDENCE / "records.json.xz"), "--repository", str(repository)],
                cwd=temporary, env=environment, capture_output=True, text=True,
                timeout=120, check=False,
            )
            self.assertEqual(replay.returncode, 0, replay.stdout + replay.stderr)
            result = json.loads(replay.stdout)
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["oracle_cohorts"], 2)
            self.assertEqual(result["complete_native_passes"], 3)
            self.assertEqual(result["stopped_native_passes"], 1)
            self.assertEqual(result["queries_per_complete_pass"], 43)
            self.assertEqual(result["rows_per_complete_pass"], 381)
            controls = subprocess.run(
                [sys.executable, "-B", str(EVIDENCE / "test_correctness_packet_950.py")],
                cwd=temporary, env=environment, capture_output=True, text=True,
                timeout=120, check=False,
            )
            self.assertEqual(controls.returncode, 0, controls.stdout + controls.stderr)
            self.assertIn("Ran 18 tests", controls.stderr)
            self.assertIn("\nOK\n", controls.stderr)

    def test_corrected_runtime_workload_matrix_and_corruption_controls(self):
        if not (EVIDENCE / "matrix-records.json.xz").exists():
            if (ROOT / ".git").exists():
                self.fail("tracked corrected-runtime matrix is missing from the checkout")
            self.skipTest("historical evidence bundles are excluded from exported source archives")
        environment = dict(
            os.environ,
            SHARDLOOM_MATRIX_PACKET=str(EVIDENCE),
            SHARDLOOM_CORRECTNESS_REPOSITORY=str(ROOT),
        )
        with tempfile.TemporaryDirectory() as temporary:
            replay = subprocess.run(
                [sys.executable, "-B", str(EVIDENCE / "verify_matrix_packet_951.py"),
                 "--manifest", str(EVIDENCE / "matrix-manifest.json"),
                 "--repository", str(ROOT), "--output", str(Path(temporary) / "replay.json")],
                cwd=temporary, env=environment, capture_output=True, text=True,
                timeout=180, check=False,
            )
            self.assertEqual(replay.returncode, 0, replay.stdout + replay.stderr)
            result = json.loads(replay.stdout)
            self.assertEqual(result["status"], "passed_corrected_matrix_portable_replay")
            self.assertEqual(result["complete_result_pairs"], 704)
            self.assertEqual(result["logical_schema_statements"], 368)
            self.assertEqual(result["terminal_order_statements"], 48)
            self.assertTrue(result["independent_full43_runtime_binding_verified"])
            self.assertFalse(result["runtime_executed"])
            controls = subprocess.run(
                [sys.executable, "-B", str(EVIDENCE / "test_matrix_packet_951.py")],
                cwd=temporary, env=environment, capture_output=True, text=True,
                timeout=240, check=False,
            )
            self.assertEqual(controls.returncode, 0, controls.stdout + controls.stderr)
            self.assertIn("Ran 14 tests", controls.stderr)
            self.assertIn("\nOK\n", controls.stderr)


if __name__ == "__main__":
    unittest.main()
