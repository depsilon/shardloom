# SPDX-License-Identifier: Apache-2.0
"""Publication records are immutable inputs, never commands to execute."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import check_published_channel_proofs as checks
from release_channel_contract import selected_channel_rows


class PublishedChannelProofTests(unittest.TestCase):
    def matrix(self):
        return json.loads((ROOT / "docs/release/package-channel-readiness-matrix.json").read_text())

    def fixture(self, root):
        matrix = self.matrix()
        for row in selected_channel_rows(matrix):
            path = root / row["install_transcript_ref"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((ROOT / row["install_transcript_ref"]).read_bytes())
        return matrix

    def test_accepts_all_four_approved_publication_records(self):
        report = checks.validate_published_channel_proofs(ROOT, self.matrix())
        self.assertEqual(report["blockers"], [])
        self.assertEqual(len(report["verified_channels"]), 4)

    def test_rejects_changed_status_assets_source_and_nested_steps(self):
        mutations = (("proof_status", "failed"), ("smoke_check_status", "failed"),
                     ("install_transcript_status", "failed"), ("fallback_attempted", True),
                     ("target_commit", "f" * 40), ("source_commit", "f" * 40),
                     ("assets", {}), ("steps", []), ("formula_checksum", "f" * 64))
        for channel in ("github_prerelease", "homebrew_tap"):
            for field, value in mutations:
                with self.subTest(channel=channel, field=field), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    matrix = self.fixture(root)
                    row = next(item for item in selected_channel_rows(matrix) if item["channel_id"] == channel)
                    path = root / row["install_transcript_ref"]
                    proof = json.loads(path.read_bytes())
                    proof[field] = value
                    path.write_text(json.dumps(proof))
                    report = checks.validate_published_channel_proofs(root, matrix)
                    self.assertIn("SHA256 differs", "; ".join(report["blockers"]))

    def test_rejects_missing_malformed_and_reformatted_transcripts(self):
        for channel in ("github_prerelease", "homebrew_tap", "testpypi", "pypi"):
            for mutation in ("missing", "malformed", "reformatted"):
                with self.subTest(channel=channel, mutation=mutation), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    matrix = self.fixture(root)
                    row = next(item for item in selected_channel_rows(matrix) if item["channel_id"] == channel)
                    path = root / row["install_transcript_ref"]
                    if mutation == "missing":
                        path.unlink()
                    elif mutation == "malformed":
                        path.write_text("not JSON")
                    else:
                        path.write_bytes(path.read_bytes() + b"\n")
                    self.assertEqual(checks.validate_published_channel_proofs(root, matrix)["status"], "blocked")

    def test_rejects_unbound_matrix_refs_and_unapproved_release(self):
        matrix = self.matrix()
        for channel in ("github_prerelease", "homebrew_tap"):
            for field in ("install_transcript_ref", "uninstall_transcript_ref", "clean_install_transcript_ref",
                          "smoke_transcript_ref", "sbom_ref", "checksum_ref", "provenance_ref"):
                with self.subTest(channel=channel, field=field):
                    changed = copy.deepcopy(matrix)
                    next(row for row in selected_channel_rows(changed) if row["channel_id"] == channel)[field] = "https://unrelated.invalid/proof"
                    self.assertEqual(checks.validate_published_channel_proofs(ROOT, changed)["status"], "blocked")
        with patch.object(checks, "SELECTED_PACKAGE_RELEASE_VERSION", "0.2.5"):
            report = checks.validate_published_channel_proofs(ROOT, matrix)
        self.assertIn("requires an approved transcript", "; ".join(report["blockers"]))


if __name__ == "__main__":
    unittest.main()
