from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_MANIFEST = Path("website/assets/benchmarks/latest/manifest.json")
RETIRED_MIRROR_PATHS = (
    "website/assets/benchmarks/latest/benchmark-results.json",
    "website-public/assets/benchmarks/latest/benchmark-results.json",
    "website/assets/data/benchmark-evidence.json",
    "website-public/assets/data/benchmark-evidence.json",
    "website-src/src/data/benchmark-evidence.json",
    "website/assets/benchmarks/latest/manifest.json",
    "website-public/assets/benchmarks/latest/manifest.json",
    "website-src/src/data/benchmark-manifest.json",
)


def load_gate_module():
    scripts = str(REPO_ROOT / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    module_path = REPO_ROOT / "scripts" / "check_front_door_benchmark_publication.py"
    spec = importlib.util.spec_from_file_location(
        "check_front_door_benchmark_publication_for_test",
        module_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


class FrontDoorBenchmarkPublicationTests(unittest.TestCase):
    def test_absent_canonical_mirrors_passes_only_the_surface_check(self) -> None:
        module = load_gate_module()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = module.build_report(root, manifest_path=CANONICAL_MANIFEST)

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["schema_version"], "shardloom.front_door_benchmark_publication_gate.v1")
        self.assertEqual(report["evidence_class"], "public_surface_absence_check")
        self.assertEqual(report["retired_mirror_absence_check"]["inspected_path_count"], 8)
        self.assertEqual(report["retired_mirror_absence_check"]["present_retired_refs"], [])
        for field in (
            "runtime_execution_performed",
            "benchmark_run_performed",
            "performance_claim_allowed",
            "performance_equivalence_claim_allowed",
            "production_claim_allowed",
            "superiority_claim_allowed",
            "parity_claim_allowed",
            "publication_allowed",
            "fallback_attempted",
            "external_engine_invoked",
        ):
            self.assertIs(report[field], False)
        self.assertEqual(report["public_benchmark_surface"], "clickbench_handoff")
        for field in (
            "scoped_local_front_door_parity_supported",
            "sql_python_dataframe_parity_status",
            "public_front_door_benchmark_row_count",
            "front_door_equivalence_constitution_status",
            "measured_front_door_equivalence_artifact_present",
        ):
            self.assertNotIn(field, report)

    def test_each_retired_mirror_blocks_the_surface_check(self) -> None:
        module = load_gate_module()
        for retired_ref in RETIRED_MIRROR_PATHS:
            with self.subTest(retired_ref=retired_ref), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                stale = root / retired_ref
                stale.parent.mkdir(parents=True, exist_ok=True)
                stale.write_text("retired", encoding="utf-8")

                report = module.build_report(root, manifest_path=CANONICAL_MANIFEST)

                self.assertEqual(report["status"], "blocked")
                self.assertIn(retired_ref, report["retired_mirror_absence_check"]["present_retired_refs"])
                self.assertFalse(report["publication_allowed"])
                self.assertTrue(report["blockers"])

    def test_missing_custom_manifest_cannot_admit_local_publication(self) -> None:
        module = load_gate_module()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = module.build_report(root, manifest_path=Path("local-run.json"))

        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["publication_allowed"])
        self.assertTrue(any("not the canonical public site path" in item for item in report["blockers"]))
        self.assertTrue(any("custom manifest is missing" in item for item in report["blockers"]))


if __name__ == "__main__":
    unittest.main()
