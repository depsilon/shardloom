from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from check_registry_bundled_proof import bundled_registry_proof_blockers
from release_channel_contract import PUBLISHED_REGISTRY_BUILD_IDENTITIES, SELECTED_PACKAGE_RELEASE_VERSION


class RegistryBundledProofTests(unittest.TestCase):
    def test_030_and_031_source_field_requires_matching_release_commit(self):
        for version in ("0.3.0", "0.3.1"):
            original = self.proof_for_version("testpypi", version)
            smoke_stdout = (ROOT / "docs/release/channel-proofs" /
                            f"testpypi-v{version}-bundled-smoke.stdout.json").read_bytes()
            expected = original["bundled_cli_supplemental_proof"]["release_source_commit"]
            cases = ((expected, False), ("b" * 40, True), (None, True))
            for release_source_commit, should_block in cases:
                with self.subTest(version=version, release_source_commit=release_source_commit):
                    proof = copy.deepcopy(original)
                    supplement = proof["bundled_cli_supplemental_proof"]
                    # Newer registry proofs use release_source_commit; the obsolete
                    # source_commit must not affect either acceptance or rejection.
                    supplement["source_commit"] = expected if should_block else "c" * 40
                    if release_source_commit is None:
                        supplement.pop("release_source_commit", None)
                    else:
                        supplement["release_source_commit"] = release_source_commit
                    blockers = bundled_registry_proof_blockers(
                        proof, channel_id="testpypi", package_version=version,
                        runtime_source_commit=expected, smoke_stdout=smoke_stdout,
                    )
                    has_source_blocker = any(
                        "must match the approved runtime source" in blocker for blocker in blockers
                    )
                    self.assertEqual(has_source_blocker, should_block, blockers)

    def proof(self, channel):
        return self.proof_for_version(channel, SELECTED_PACKAGE_RELEASE_VERSION)

    def proof_for_version(self, channel, version):
        return json.loads((ROOT / "docs/release/channel-proofs" /
                           f"{channel}-v{version}-transcript.json").read_text())

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

    def test_preserves_all_historical_installation_proofs(self):
        base = ROOT / "docs/release/channel-proofs"
        for version in ("0.2.4", "0.3.0", "0.3.1", "0.3.2", "0.3.3", "0.4.0"):
            for channel in ("testpypi", "pypi"):
                with self.subTest(version=version, channel=channel):
                    proof = self.proof_for_version(channel, version)
                    self.assertEqual(bundled_registry_proof_blockers(
                        proof, channel_id=channel, package_version=version,
                        runtime_source_commit=PUBLISHED_REGISTRY_BUILD_IDENTITIES[version]["testpypi"]["source_commit"],
                        smoke_stdout=(base / f"{channel}-v{version}-bundled-smoke.stdout.json").read_bytes(),
                    ), [])

    def test_rejects_failed_missing_or_unbound_bundled_evidence(self):
        source_field = "source_commit" if SELECTED_PACKAGE_RELEASE_VERSION == "0.2.4" else "release_source_commit"
        version_field = "version" if SELECTED_PACKAGE_RELEASE_VERSION == "0.5.1" else "cli_version"
        cli_field = "bundled_cli" if SELECTED_PACKAGE_RELEASE_VERSION == "0.5.1" else "resolved_cli_path"
        mutations = [
            (("proof_status",), "failed", "proof_status must be passed"),
            (("status",), "failed", "status must be passed"),
            (("uninstall_transcript_status",), "failed", "uninstall_transcript_status must be passed"),
            (("blockers",), ["failed"], "must have no blockers"),
            (("channel_id",), "wrong", "must match the registry channel"),
            ((source_field,), "f" * 40, "must match the approved runtime source"),
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
            (("result", version_field), "0.0.0", "CLI version must match"),
            (("result", "exact_results"), [], "complete DataFrame and two SQL results"),
            (("result", "unsupported_blocker"), None, "expected unsupported diagnostic"),
            (("result", "fallback_attempted"), True, "result fallback_attempted must be false"),
            (("result", "external_engine_invoked"), True, "result external_engine_invoked must be false"),
            (("result", cli_field), "/opt/homebrew/bin/shardloom", "inside the same clean venv"),
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

    def test_051_rejects_rebound_incomplete_or_changed_workflow_results(self):
        """Alter both the result and its capture, so receipt self-consistency cannot hide drift."""
        mutations = [
            (("native_vortex_roundtrip_values",), [], "native_vortex_roundtrip_values"),
            (("analytic_frame_values", 1, "n"), 1, "analytic_frame_values"),
            (("correlated_scalar_subquery_values", 0, "scalar"), 0, "correlated_scalar_subquery_values"),
            (("repeated_sql_executions",), True, "repeated_sql_executions"),
            (("actual_disk_pressure_claimed_by_smoke",), True, "actual_disk_pressure_claimed_by_smoke"),
            (("native_vortex_output_sha256",), "bad", "native Vortex output digest"),
            (("cli_distribution",), "homebrew_source_build", "same bundled wheel CLI"),
            (("explicit_homebrew_cli_binding",), True, "same bundled wheel CLI"),
            (("verified_native_cli",), "/opt/homebrew/bin/shardloom", "same bundled wheel CLI"),
            (("cli_sha256",), "bad", "bundled CLI SHA256 argument"),
            (("exact_results", 0, 0, "id"), 2.0, "complete DataFrame and two SQL results"),
            (("incremental_complete_values",), {}, "all five complete incremental workflows"),
        ]
        names = ("vortex_incremental_results", "streamed_general_aggregate", "streamed_left_join",
                 "streamed_analytic_window", "sparse_pivot")
        for name in names:
            mutations.extend([
                (("incremental_complete_values", name), None, "complete typed rows"),
                (("incremental_complete_values", name, "rows"), [], "complete typed rows"),
                (("incremental_complete_values", name, "batches"), True, "acknowledged batch count"),
                (("incremental_complete_values", name, "batches"), 0, "acknowledged batch count"),
                (("incremental_complete_values", name, "batches"), 100, "acknowledged batch count"),
                (("incremental_complete_values", name, "final_report_success"), False, "final_report_success"),
                (("incremental_complete_values", name, "owned_workspace_empty"), False, "owned_workspace_empty"),
                (("incremental_complete_values", name, "explicit_spill_policy"),
                 name == "vortex_incremental_results", "approved spill policy"),
            ])
        mutations.append((("incremental_complete_values", "sparse_pivot", "rows", 0, "pivot_a"),
                          7, "complete typed rows"))
        for channel in ("testpypi", "pypi"):
            original = self.proof_for_version(channel, "0.5.1")
            for path, value, expected in mutations:
                with self.subTest(channel=channel, path=path, value=value):
                    proof = copy.deepcopy(original)
                    supplement = proof["bundled_cli_supplemental_proof"]
                    target = supplement["result"]
                    for key in path[:-1]:
                        target = target[key]
                    target[path[-1]] = value
                    capture = json.dumps(supplement["result"]).encode()
                    supplement["steps"][3]["stdout_sha256"] = hashlib.sha256(capture).hexdigest()
                    errors = bundled_registry_proof_blockers(
                        proof, channel_id=channel, package_version="0.5.1",
                        runtime_source_commit=PUBLISHED_REGISTRY_BUILD_IDENTITIES["0.5.1"]["testpypi"]["source_commit"],
                        smoke_stdout=capture,
                    )
                    self.assertNotIn("result must equal the captured", "; ".join(errors))
                    self.assertIn(expected, "; ".join(errors))

    def test_051_rejects_changed_smoke_program_arguments_and_digest(self):
        for channel in ("testpypi", "pypi"):
            original = self.proof_for_version(channel, "0.5.1")
            command = original["bundled_cli_supplemental_proof"]["steps"][3]["command"]
            mutations = [
                (command[:-1], "approved complete-value smoke program"),
                (command + ["/opt/homebrew/bin/shardloom"], "approved complete-value smoke program"),
                (command[:-1] + ["f" * 64], "bundled CLI SHA256 argument"),
                (command[:3] + ["pass"] + command[4:], "approved complete-value smoke program"),
            ]
            for changed, expected in mutations:
                with self.subTest(channel=channel, expected=expected):
                    proof = copy.deepcopy(original)
                    proof["bundled_cli_supplemental_proof"]["steps"][3]["command"] = changed
                    self.assertIn(expected, "; ".join(self.validate(proof, channel)))
            proof = copy.deepcopy(original)
            proof["bundled_cli_supplemental_proof"]["shared_smoke_sha256"] = "f" * 64
            self.assertIn("shared smoke digest", "; ".join(self.validate(proof, channel)))

    def test_rejects_missing_changed_or_unrelated_captured_stdout(self):
        for channel in ("testpypi", "pypi"):
            proof = self.proof(channel)
            for raw in (None, b"{}\n", b"not JSON", b"x" * 65537):
                with self.subTest(channel=channel, raw_length=None if raw is None else len(raw)):
                    errors = bundled_registry_proof_blockers(
                        proof, channel_id=channel, package_version=SELECTED_PACKAGE_RELEASE_VERSION,
                        runtime_source_commit=PUBLISHED_REGISTRY_BUILD_IDENTITIES[SELECTED_PACKAGE_RELEASE_VERSION]["testpypi"]["source_commit"],
                        smoke_stdout=raw,
                    )
                    self.assertTrue(errors)


if __name__ == "__main__":
    unittest.main()
