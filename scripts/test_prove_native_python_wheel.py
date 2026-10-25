"""Build-only and reused-wheel proof boundaries without replacing real load tests."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import prove_native_python_wheel as proof
import test_check_native_python_wheel as fixtures


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
