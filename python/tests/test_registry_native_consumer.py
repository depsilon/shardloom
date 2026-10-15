# SPDX-License-Identifier: Apache-2.0
"""Reject native registry evidence drift using inert, offline fixtures."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
if __package__:
    from .test_registry_release_evidence import evidence, native_fixture
else:
    from test_registry_release_evidence import evidence, native_fixture
import check_package_channel_readiness as consumer


class NativeRegistryConsumerTests(unittest.TestCase):
    def fixture(self):
        artifacts, rows = [], []
        programs = None
        for index, linux in enumerate((False, True)):
            files, distribution, programs = native_fixture(linux)
            artifacts.append(distribution)
            row = evidence.verify_native_proof(files, distribution, programs)
            row.update(workflow_artifact_id=index + 1,
                workflow_artifact_name="native-registry-proof-" + ("linux" if linux else "macos"),
                workflow_run_id=44, source_commit="d" * 40,
                workflow_artifact_archive_sha256="e" * 64, workflow_artifact_archive_size_bytes=100)
            rows.append(row)
        return artifacts, {"workflow_run_id": 44, "source_commit": "d" * 40,
                           "native_installed_wheel_proofs": rows}, {
            "0.6.0": {name: hashlib.sha256(text.encode()).hexdigest() for name, text in programs.items()}}

    def check(self, artifacts, provenance, policy):
        with patch.object(consumer, "SELECTED_PACKAGE_RELEASE_VERSION", "0.6.0"), \
                patch.object(consumer, "PUBLISHED_REGISTRY_NATIVE_PROOF_PROGRAM_SHA256", policy):
            return consumer.registry_native_installation_blockers("testpypi", artifacts, provenance)

    def test_exact_proofs_and_original_files_pass(self):
        artifacts, provenance, policy = self.fixture()
        self.assertEqual(self.check(artifacts, provenance, policy), [])

    def test_native_proof_mutations_and_missing_policy_are_blocked(self):
        artifacts, original, policy = self.fixture()
        self.assertTrue(self.check(artifacts, original, {}))
        for mutation in ("missing", "duplicate", "wrong_wheel", "unhashable_wheel", "run", "source", "artifact_id",
                         "archive_hash", "summary", "program", "file_hash", "file_size", "file_text", "duplicate_file"):
            provenance = copy.deepcopy(original)
            rows = provenance["native_installed_wheel_proofs"]
            if mutation == "missing": rows.pop()
            elif mutation == "duplicate": rows[1] = copy.deepcopy(rows[0])
            elif mutation == "wrong_wheel": rows[0]["wheel"] = "other.whl"
            elif mutation == "unhashable_wheel": rows[0]["wheel"] = {}
            elif mutation == "run": rows[0]["workflow_run_id"] = 45
            elif mutation == "source": rows[0]["source_commit"] = "f" * 40
            elif mutation == "artifact_id": rows[1]["workflow_artifact_id"] = rows[0]["workflow_artifact_id"]
            elif mutation == "archive_hash": rows[0]["workflow_artifact_archive_sha256"] = "bad"
            elif mutation == "summary": rows[0]["integration"]["tests"] = 1
            elif mutation == "program":
                next(step for step in rows[0]["proof_report"]["steps"] if step["name"] == "load")["command"][3] = "changed"
            elif mutation == "file_hash": rows[0]["original_files"][0]["sha256"] = "f" * 64
            elif mutation == "file_size": rows[0]["original_files"][0]["size_bytes"] += 1
            elif mutation == "file_text": rows[0]["original_files"][0]["text"] += "changed"
            elif mutation == "duplicate_file": rows[0]["original_files"].append(copy.deepcopy(rows[0]["original_files"][0]))
            with self.subTest(mutation=mutation):
                self.assertTrue(self.check(artifacts, provenance, policy))

    def test_malformed_original_json_shapes_are_blocked(self):
        artifacts, original, policy = self.fixture()
        cases = [(name, None) for name in (
            "native-build.json", "proof/report.json", "proof/integration.json")]
        cases.extend(("proof/report.json", field) for field in (
            "integrity", "stage", "installed_load", "linux_audit"))
        for name, field in cases:
            for malformed in ([], None, 7, "not an object"):
                with self.subTest(file=name, field=field, value=malformed):
                    provenance = copy.deepcopy(original)
                    # Select Linux so its audit shape is also checked. Rebind
                    # each original file's hash to exercise structural checking.
                    row = provenance["native_installed_wheel_proofs"][1]
                    member = next(item for item in row["original_files"]
                                  if item["name"] == name)
                    value = malformed
                    if field is not None:
                        value = json.loads(member["text"])
                        value[field] = malformed
                    member["text"] = json.dumps(value)
                    raw = member["text"].encode("utf-8")
                    member["size_bytes"] = len(raw)
                    member["sha256"] = hashlib.sha256(raw).hexdigest()
                    blockers = self.check(artifacts, provenance, policy)
                    self.assertTrue(blockers)
                    self.assertTrue(all("registry native installation" in item
                                        for item in blockers))

    def test_native_sbom_graph_requires_module_and_edge(self):
        artifacts, _, _ = self.fixture()
        # A component identifier is a content hash; give this inert Linux fixture
        # a different distribution hash, as actual platform wheels have.
        artifacts[1]["sha256"] = "f" * 64
        bom = evidence.cyclonedx("testpypi", artifacts, [], "2026-10-15T00:00:00Z")
        with patch.object(consumer, "SELECTED_PACKAGE_RELEASE_VERSION", "0.6.0"):
            self.assertEqual(consumer.registry_bundled_cli_inventory_blockers("testpypi", artifacts, bom), [])
            for field in ("components", "dependencies"):
                changed = copy.deepcopy(bom); changed[field].pop()
                self.assertTrue(consumer.registry_bundled_cli_inventory_blockers("testpypi", artifacts, changed))
            changed = copy.deepcopy(artifacts); del changed[0]["native_library"]
            self.assertTrue(consumer.registry_bundled_cli_inventory_blockers("testpypi", changed, bom))


if __name__ == "__main__":
    unittest.main()
