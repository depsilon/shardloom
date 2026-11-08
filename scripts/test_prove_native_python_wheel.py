"""Build-only and reused-wheel proof boundaries without replacing real load tests."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import prove_native_python_wheel as proof
import test_check_native_python_wheel as fixtures


class IntegrationSelectionTests(unittest.TestCase):
    def test_installed_proof_requires_connected_and_independent_client_suites(self):
        names = ("test_native_binding", "test_connected_programming", "test_native_function_catalog",
                 "test_native_observations", "test_native_standard_adapters", "test_query_surface_native",
                 "test_recursion_policy", "test_public_context")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in names:
                (root / (name + ".py")).write_text(
                    "import unittest\nclass Check(unittest.TestCase):\n"
                    "    def test_loaded(self): self.assertTrue(True)\n")
            report = root / "integration.json"
            command = [sys.executable, "-I", "-B", "-c", proof.RUN_TESTS, str(root), str(report)]
            result = subprocess.run(command, text=True, capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(report.read_text())["tests"], len(names))
            for name in ("test_native_standard_adapters", "test_public_context"):
                with self.subTest(required_suite=name):
                    required = root / (name + ".py")
                    original = required.read_text()
                    required.write_text("import unittest\nclass Check(unittest.TestCase):\n"
                        "    @unittest.skip('missing required coverage')\n    def test_loaded(self): pass\n")
                    result = subprocess.run(command, text=True, capture_output=True, check=False)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(json.loads(report.read_text())["status"], "failed")
                    required.unlink()
                    result = subprocess.run(command, text=True, capture_output=True, check=False)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("required native integration suite is absent: " + name + ".py",
                                  result.stderr)
                    required.write_text(original)


class WheelReuseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.wheel, self.library, self.binary = fixtures.NativePythonWheelIntegrityTests().fixture(self.root)
        self.wheelhouse = self.root / "wheelhouse"
        self.wheelhouse.mkdir()
        self.report = self.root / "build-report.json"
        self.report.write_text(json.dumps({"schema_version": "shardloom.native_python_wheel_proof.v1",
            "status": "built", "integrity": {"wheel_sha256": proof.sha(self.wheel)}}))

    def invoke(self, **kwargs):
        with patch.object(proof, "version", return_value="fixture"), patch.object(
                proof.platform, "platform", return_value="fixture host"), patch(
                "check_native_python_wheel.verify_notices", return_value={"status": "passed"}):
            return proof.prove(self.root, self.root / "output", self.library, self.binary,
                               self.wheelhouse, **kwargs)

    def simulate_step_work(self, slow_step: str, seconds: int):
        """Exercise real proof control flow with deterministic subprocess work."""
        def run(command, **kwargs):
            name = Path(kwargs["stdout"].name).stem
            if name == slow_step and seconds > kwargs["timeout"]:
                kwargs["stdout"].write(b"fixture step exceeded its bounded budget\n")
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])
            if name == "load":
                kwargs["stdout"].write(json.dumps({"native_library": str(self.library),
                    "cli": str(self.binary), "isolated_install": True}).encode())
            elif name == "integration":
                Path(command[-1]).write_text(json.dumps({"tests": 1, "failures": 0,
                    "errors": 0, "skipped": 0, "status": "passed"}))
            return subprocess.CompletedProcess(command, 0)

        with patch.object(proof.subprocess, "run", side_effect=run):
            return self.invoke(existing_wheel=self.wheel, build_report=self.report)

    def test_complete_integration_has_a_separate_finite_budget(self) -> None:
        result = self.simulate_step_work("integration", 900)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["uninstall_status"], "passed")
        self.assertEqual({row["name"]: row["timeout_seconds"] for row in result["steps"]}, {
            "create_environment": 600, "install_optional_consumer": 600, "install": 600,
            "load": 600, "integration": 1200, "uninstall": 600, "verify_uninstalled": 600})

    def test_integration_still_times_out_and_preserves_the_failed_observation(self) -> None:
        with self.assertRaises(subprocess.TimeoutExpired) as failure:
            self.simulate_step_work("integration", 1201)
        self.assertEqual(failure.exception.timeout, 1200)
        saved = json.loads((self.root / "output/report.json").read_text())
        self.assertEqual(saved["status"], "failed")
        self.assertFalse(saved["installation_performed"])
        self.assertNotIn("integration", saved)
        self.assertNotIn("uninstall_status", saved)
        step = saved["steps"][-1]
        self.assertEqual((step["name"], step["timeout_seconds"]), ("integration", 1200))
        self.assertNotIn("returncode", step)
        self.assertEqual(step["log_sha256"], proof.sha(self.root / "output/integration.log"))

    def test_setup_keeps_its_shorter_deadline(self) -> None:
        with self.assertRaises(subprocess.TimeoutExpired) as failure:
            self.simulate_step_work("create_environment", 601)
        self.assertEqual(failure.exception.timeout, 600)
        saved = json.loads((self.root / "output/report.json").read_text())
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(len(saved["steps"]), 1)
        self.assertEqual(saved["steps"][0]["timeout_seconds"], 600)

    def test_reuse_requires_report_pair_and_cannot_also_request_build(self) -> None:
        for args in ({"existing_wheel": self.wheel}, {"build_report": self.report},
                     {"existing_wheel": self.wheel, "build_report": self.report, "build_only": True}):
            with self.subTest(args=args), self.assertRaisesRegex(ValueError, "requires its build report"):
                self.invoke(**args)

    def test_changed_wheel_or_failed_build_report_never_reaches_install(self) -> None:
        for report in ({"status": "failed"}, {"integrity": {"wheel_sha256": "wrong"}}):
            original = json.loads(self.report.read_text())
            self.report.write_text(json.dumps({**original, **report}))
            with patch.object(proof.subprocess, "run") as run:
                with self.assertRaisesRegex(ValueError, "successful build report"):
                    self.invoke(existing_wheel=self.wheel, build_report=self.report)
                run.assert_not_called()
            saved = json.loads((self.root / "output/report.json").read_text())
            self.assertEqual(saved["status"], "failed")
            self.assertFalse(saved["installation_performed"])
            (self.root / "output/report.json").unlink()
            (self.root / "output").rmdir()
            self.report.write_text(json.dumps(original))

    def test_linux_audit_cannot_be_bypassed_by_reusing_unaudited_wheel(self) -> None:
        with patch.object(proof.sys, "platform", "linux"), patch.object(proof.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "reviewed compatibility audit"):
                self.invoke(existing_wheel=self.wheel, build_report=self.report, audit_linux=True)
            run.assert_not_called()
        self.assertEqual(json.loads((self.root / "output/report.json").read_text())["status"], "failed")

    def test_installed_proof_clears_inherited_source_runtime_overrides(self) -> None:
        overrides = {
            "SHARDLOOM_NATIVE_TEST_LIBRARY": "/unrelated/source/_native.abi3.so",
            "SHARDLOOM_BIN": "/unrelated/source/shardloom",
            "SHARDLOOM_SURFACE_TEST_BINARY": "/unrelated/source/surface-shardloom",
            "SHARDLOOM_REPO_ROOT": "/unrelated/source",
            "PYTHONPATH": "/unrelated/source/python/src",
        }
        observed = {}

        def inspect_environment(command, **kwargs):
            self.assertIn("venv", command)
            observed.update({name: kwargs["env"].get(name) for name in overrides})
            observed["SHARDLOOM_REQUIRE_NATIVE_PYTHON"] = kwargs["env"].get(
                "SHARDLOOM_REQUIRE_NATIVE_PYTHON")
            raise RuntimeError("stop before invoking environment creation")

        with patch.dict(proof.os.environ, overrides), patch.object(
                proof.subprocess, "run", side_effect=inspect_environment):
            with self.assertRaisesRegex(RuntimeError, "stop before invoking environment creation"):
                self.invoke(existing_wheel=self.wheel, build_report=self.report)
        self.assertEqual(observed["SHARDLOOM_REQUIRE_NATIVE_PYTHON"], "1")
        for name in overrides:
            self.assertIsNone(observed[name], name)

    def test_build_only_reports_no_installation_and_never_creates_a_virtualenv(self) -> None:
        commands = []

        def fake_run(command, **kwargs):
            commands.append([str(arg) for arg in command])
            # Model only the package builder. Real archive/integrity checks remain
            # in place; installed execution is separately required in CI.
            destination = Path(command[command.index("--wheel-dir") + 1])
            destination.mkdir()
            (destination / self.wheel.name).write_bytes(self.wheel.read_bytes())
            return type("Result", (), {"returncode": 0})()

        with patch.object(proof, "stage", return_value={"wheel_platform_tag": None}), \
                patch.object(proof.subprocess, "run", side_effect=fake_run):
            result = self.invoke(build_only=True)
        self.assertEqual(len(commands), 1)
        self.assertIn("wheel", commands[0])
        self.assertEqual(result["status"], "built")
        self.assertFalse(result["installation_performed"])
        self.assertEqual(result["scope"], "current_host_native_wheel_build_integrity")
        self.assertFalse((self.root / "output/venv").exists())


if __name__ == "__main__":
    unittest.main()
