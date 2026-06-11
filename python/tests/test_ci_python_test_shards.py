from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_script(script_name: str, module_name: str) -> object:
    module_path = REPO_ROOT / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PythonTestShardTests(unittest.TestCase):
    def test_discovery_matches_unittest_default_test_star_pattern(self) -> None:
        runner = load_script(
            "run_python_test_shard.py",
            "run_python_test_shard_pattern_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            test_root = root / "python" / "tests"
            test_root.mkdir(parents=True)
            (test_root / "testfoo.py").write_text("", encoding="utf-8")
            (test_root / "test_bar.py").write_text("", encoding="utf-8")
            (test_root / "helper_test.py").write_text("", encoding="utf-8")

            discovered = runner.discover_test_modules(root)

        self.assertEqual(discovered, ["test_bar", "testfoo"])

    def test_shards_cover_discovered_modules_exactly_once(self) -> None:
        runner = load_script(
            "run_python_test_shard.py",
            "run_python_test_shard_for_test",
        )
        discovered = {
            f"python.tests.{stem}" for stem in runner.discover_test_modules(REPO_ROOT)
        }
        observed: list[str] = []
        for shard in runner.SHARD_ORDER:
            modules = runner.module_names_for_shard(shard, REPO_ROOT)
            self.assertTrue(modules, shard)
            observed.extend(modules)

        self.assertEqual(set(observed), discovered)
        self.assertEqual(len(observed), len(set(observed)))
        self.assertIn("python.tests.test_release_scripts", observed)
        self.assertIn("python.tests.test_front_door_benchmark_publication", observed)

    def test_merge_rejects_missing_shard_evidence(self) -> None:
        runner = load_script(
            "run_python_test_shard.py",
            "run_python_test_shard_missing_for_test",
        )
        merger = load_script(
            "merge_python_test_shard_evidence.py",
            "merge_python_test_shard_missing_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for shard in runner.SHARD_ORDER[:-1]:
                payload = {
                    "schema_version": runner.SCHEMA_VERSION,
                    "status": "passed",
                    "shard_id": shard,
                    "modules": runner.module_names_for_shard(shard, REPO_ROOT),
                    "module_count": len(runner.module_names_for_shard(shard, REPO_ROOT)),
                    "test_count": 1,
                    "elapsed_seconds": 0.1,
                    "skipped_count": 0,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
                (root / f"{shard}.json").write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )

            report = merger.build_report(REPO_ROOT, root)

        self.assertEqual(report["status"], "failed")
        self.assertIn(
            "missing Python test shard evidence for release_scripts",
            report["blockers"],
        )

    def test_merge_rejects_failed_shard_status(self) -> None:
        runner = load_script(
            "run_python_test_shard.py",
            "run_python_test_shard_failed_status_for_test",
        )
        merger = load_script(
            "merge_python_test_shard_evidence.py",
            "merge_python_test_shard_failed_status_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for shard in runner.SHARD_ORDER:
                modules = runner.module_names_for_shard(shard, REPO_ROOT)
                payload = {
                    "schema_version": runner.SCHEMA_VERSION,
                    "status": "failed" if shard == "release_scripts" else "passed",
                    "shard_id": shard,
                    "modules": modules,
                    "module_count": len(modules),
                    "test_count": len(modules),
                    "elapsed_seconds": 0.1,
                    "skipped_count": 0,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
                (root / f"{shard}.json").write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )

            report = merger.build_report(REPO_ROOT, root)

        self.assertEqual(report["status"], "failed")
        self.assertIn("release_scripts: status=failed", report["blockers"])

    def test_merge_accepts_complete_shard_evidence(self) -> None:
        runner = load_script(
            "run_python_test_shard.py",
            "run_python_test_shard_complete_for_test",
        )
        merger = load_script(
            "merge_python_test_shard_evidence.py",
            "merge_python_test_shard_complete_for_test",
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for shard in runner.SHARD_ORDER:
                modules = runner.module_names_for_shard(shard, REPO_ROOT)
                payload = {
                    "schema_version": runner.SCHEMA_VERSION,
                    "status": "passed",
                    "shard_id": shard,
                    "modules": modules,
                    "module_count": len(modules),
                    "test_count": len(modules),
                    "elapsed_seconds": 0.1,
                    "skipped_count": 0,
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                }
                (root / f"{shard}.json").write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )

            report = merger.build_report(REPO_ROOT, root)

        self.assertEqual(report["status"], "passed")
        self.assertTrue(report["coverage_equivalent_to_discover"])
        self.assertEqual(report["shard_count"], len(runner.SHARD_ORDER))


if __name__ == "__main__":
    unittest.main()
