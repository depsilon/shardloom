# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import contextlib
import io
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_PATH = REPO_ROOT / "examples" / "local-vortex-benchmark" / "run.py"
SPEC = importlib.util.spec_from_file_location("local_vortex_benchmark_example", EXAMPLE_PATH)
assert SPEC is not None and SPEC.loader is not None
EXAMPLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXAMPLE)


class LocalNativeBenchmarkExampleTests(unittest.TestCase):
    def test_defaults_delegate_to_shared_harness(self) -> None:
        args = EXAMPLE.parser().parse_args(
            ["--shardloom-binary", "target/debug/shardloom", "--workspace", "/tmp/bench"]
        )

        command, cwd = EXAMPLE.build_command(args)

        self.assertEqual(cwd, REPO_ROOT)
        self.assertEqual(command[1], str(REPO_ROOT / "benchmarks/traditional_analytics/run.py"))
        self.assertEqual(command[command.index("--engines") + 1], "shardloom")
        self.assertEqual(command[command.index("--reference-engine") + 1], "pandas")
        self.assertEqual(command[command.index("--scenarios") + 1], "selective filter")
        self.assertEqual(command[command.index("--rows") + 1], "64")
        self.assertEqual(command[command.index("--dim-rows") + 1], "8")
        self.assertEqual(command[command.index("--repeats") + 1], "1")
        self.assertEqual(command[command.index("--formats") + 1], "csv")
        self.assertEqual(command[command.index("--input-state") + 1], "raw")
        self.assertEqual(command[command.index("--output-format") + 1], "collect")
        self.assertNotIn("--run-id", command)
        self.assertNotIn("--data-dir", command)

    def test_user_choices_are_forwarded_and_wrapper_returns_harness_status(self) -> None:
        args = [
            "--shardloom-binary",
            "/opt/shardloom",
            "--workspace",
            "/tmp/local-data",
            "--repo-root",
            "/tmp/shardloom-checkout",
            "--rows",
            "100",
            "--dim-rows",
            "12",
            "--repeats",
            "3",
            "--formats",
            "csv",
            "jsonl",
            "--input-state",
            "prepared",
            "--output-format",
            "jsonl",
        ]

        result = type("Result", (), {"returncode": 7})()
        with patch.object(EXAMPLE.subprocess, "run", return_value=result) as run:
            status = EXAMPLE.main(args)

        command = run.call_args.args[0]
        self.assertEqual(status, 7)
        self.assertEqual(command[command.index("--shardloom-binary") + 1], "/opt/shardloom")
        self.assertEqual(command[command.index("--workspace") + 1], "/tmp/local-data")
        explicit_root = Path("/tmp/shardloom-checkout")
        resolved_root = explicit_root.resolve()
        self.assertEqual(run.call_args.kwargs["cwd"], resolved_root)
        self.assertFalse(run.call_args.kwargs["check"])
        self.assertEqual(
            command[1], str(resolved_root / "benchmarks/traditional_analytics/run.py")
        )
        self.assertEqual(command[command.index("--shardloom-binary") + 1], "/opt/shardloom")
        self.assertEqual(command[command.index("--workspace") + 1], "/tmp/local-data")
        self.assertEqual(command[command.index("--rows") + 1], "100")
        self.assertEqual(command[command.index("--dim-rows") + 1], "12")
        self.assertEqual(command[command.index("--repeats") + 1], "3")
        self.assertEqual(
            command[command.index("--formats") + 1 : command.index("--scenarios")],
            ["csv", "jsonl"],
        )
        self.assertEqual(command[command.index("--input-state") + 1], "prepared")
        self.assertEqual(command[command.index("--output-format") + 1], "jsonl")

    def test_retired_wrapper_options_are_rejected(self) -> None:
        for obsolete in ("--run-id", "--iterations", "--data-dir", "--profile"):
            with self.subTest(option=obsolete), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    EXAMPLE.parser().parse_args([
                        "--shardloom-binary", "/opt/shardloom", "--workspace", "/tmp/bench",
                        obsolete, "retired",
                    ])


if __name__ == "__main__":
    unittest.main()
