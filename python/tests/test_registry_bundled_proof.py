from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from check_registry_bundled_proof import bundled_registry_proof_blockers
from release_channel_contract import PUBLISHED_REGISTRY_BUILD_IDENTITIES, SELECTED_PACKAGE_RELEASE_VERSION


class RegistryBundledProofTests(unittest.TestCase):
    def proof(self, channel):
        return json.loads((ROOT / "docs/release/channel-proofs" /
                           f"{channel}-v{SELECTED_PACKAGE_RELEASE_VERSION}-transcript.json").read_text())

    def validate(self, proof, channel):
        return bundled_registry_proof_blockers(
            proof, channel_id=channel, package_version=SELECTED_PACKAGE_RELEASE_VERSION,
            runtime_source_commit=PUBLISHED_REGISTRY_BUILD_IDENTITIES[SELECTED_PACKAGE_RELEASE_VERSION]["testpypi"]["source_commit"],
            smoke_stdout=(ROOT / "docs/release/channel-proofs" /
                          f"{channel}-v{SELECTED_PACKAGE_RELEASE_VERSION}-bundled-smoke.stdout.json").read_bytes(),
        )

    def test_accepts_both_recorded_bundled_installations(self):
        for channel in ("testpypi", "pypi"):
            with self.subTest(channel=channel):
                self.assertEqual(self.validate(self.proof(channel), channel), [])

    def test_rejects_failed_missing_or_unbound_bundled_evidence(self):
        mutations = [
            (("proof_status",), "failed", "proof_status must be passed"),
            (("status",), "failed", "status must be passed"),
            (("uninstall_transcript_status",), "failed", "uninstall_transcript_status must be passed"),
            (("blockers",), ["failed"], "must have no blockers"),
            (("channel_id",), "wrong", "must match the registry channel"),
            (("source_commit",), "f" * 40, "must match the approved runtime source"),
            (("steps",), [], "requires all six ordered"),
            (("steps", 3, "returncode"), 1, "must exit successfully"),
            (("steps", 3, "returncode"), False, "must exit successfully"),
            (("steps", 3, "process_group_drained"), False, "process_group_drained must be true"),
            (("steps", 3, "direct_child_reaped"), False, "direct_child_reaped must be true"),
            (("steps", 3, "timed_out"), True, "timed_out must be false"),
            (("steps", 3, "interrupted_signal"), 15, "must not be interrupted"),
            (("steps", 3, "stdout_sha256"), None, "requires stdout_sha256"),
            (("steps", 3, "command"), None, "requires an isolated Python command"),
            (("steps", 3, "command", 3), "pass", "approved complete-value smoke program"),
            (("steps", 3, "stdout_sha256"), "a" * 64, "captured smoke stdout must match"),
            (("steps", 1, "command", 3), "pass", "must verify the package is absent"),
            (("steps", 5, "command", 3), "pass", "must verify the package is absent"),
            (("steps", 3, "command"), ["/usr/bin/python", "-I", "-c", "pass"], "must use the recorded clean venv interpreter"),
            (("steps", 2, "command"), ["python", "-I", "-m", "pip", "install", "other.whl"], "install command must use the verified wheel"),
            (("wheel_identity", "sha256"), "f" * 64, "wheel digest must match"),
            (("wheel_identity", "path"), "/tmp/other.whl", "wheel filename must match"),
            (("wheel_metadata",), "Root-Is-Purelib: true\nTag: py3-none-any\n", "wheel metadata must match"),
            (("result", "cli_version"), "0.0.0", "CLI version must match"),
            (("result", "exact_results"), [], "complete DataFrame and two SQL results"),
            (("result", "unsupported_blocker"), None, "expected unsupported diagnostic"),
            (("result", "fallback_attempted"), True, "result fallback_attempted must be false"),
            (("result", "external_engine_invoked"), True, "result external_engine_invoked must be false"),
            (("result", "resolved_cli_path"), "/opt/homebrew/bin/shardloom", "inside the same clean venv"),
            (("result", "package_path"), "/tmp/source/shardloom/__init__.py", "inside the same clean venv"),
        ]
        mutations.extend(((field,), True, f"{field} must be false") for field in (
            "external_cli_override", "source_python_path_override", "fallback_attempted",
            "external_engine_invoked", "secrets_required", "publication_attempted_by_this_tool",
            "registry_upload_attempted_by_this_tool", "package_upload_attempted_by_this_tool",
            "package_channel_submission_attempted_by_this_tool",
        ))
        for channel in ("testpypi", "pypi"):
            original = self.proof(channel)
            missing = copy.deepcopy(original)
            missing.pop("bundled_cli_supplemental_proof")
            self.assertIn("is required", "; ".join(self.validate(missing, channel)))
            for path, value, expected in mutations:
                with self.subTest(channel=channel, path=path):
                    proof = copy.deepcopy(original)
                    target = proof["bundled_cli_supplemental_proof"]
                    for key in path[:-1]:
                        target = target[key]
                    target[path[-1]] = value
                    self.assertIn(expected, "; ".join(self.validate(proof, channel)))

    def test_rejects_missing_changed_or_unrelated_captured_stdout(self):
        for channel in ("testpypi", "pypi"):
            proof = self.proof(channel)
            for raw in (None, b"{}\n", b"not JSON", b"x" * 65537):
                with self.subTest(channel=channel, raw_length=None if raw is None else len(raw)):
                    errors = bundled_registry_proof_blockers(
                        proof, channel_id=channel, package_version=SELECTED_PACKAGE_RELEASE_VERSION,
                        runtime_source_commit=proof["bundled_cli_supplemental_proof"]["source_commit"],
                        smoke_stdout=raw,
                    )
                    self.assertTrue(errors)


if __name__ == "__main__":
    unittest.main()
