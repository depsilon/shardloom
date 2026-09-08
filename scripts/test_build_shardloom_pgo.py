# SPDX-License-Identifier: Apache-2.0
"""Small orchestration tests; no Cargo, compiler, profile tool or engine invocation."""
import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import pgo_local_experiment as pgo


class PgoTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "source"
        self.repo.mkdir()
        (self.repo / "Cargo.toml").write_text('[workspace.package]\nrust-version = "1.96"\n')
        self.args = pgo.parse_args(["--repo-root", str(self.repo), "--run-dir", str(self.root / "pgo")])

    def test_print_only_does_not_probe_toolchain_create_dirs_or_spawn(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(subprocess, "Popen", side_effect=AssertionError("spawned")), contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(pgo.main(["--repo-root", str(self.repo), "--run-dir", str(self.root / "plan")]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["status"], "print_only")
        self.assertEqual(report["steps"], [])
        self.assertIn("--target", report["build_command_template"])
        self.assertEqual(report["profile_pattern"], "shardloom-%m.profraw")
        self.assertFalse((self.root / "plan").exists())

    def test_rejects_existing_or_checkout_destination_preserves_sentinel(self):
        output = self.root / "pgo"
        output.mkdir()
        sentinel = output / "older.profraw"
        sentinel.write_bytes(b"original")
        with self.assertRaisesRegex(ValueError, "already exists"):
            pgo.make_plan(self.args, {})
        self.assertEqual(sentinel.read_bytes(), b"original")
        self.args.run_dir = self.repo / "target/pgo"
        with self.assertRaisesRegex(ValueError, "outside"):
            pgo.make_plan(self.args, {})

    def test_base_flags_are_explicit_identical_and_no_native_cpu_default(self):
        flags = pgo.base_flags({"CARGO_ENCODED_RUSTFLAGS": "-Copt-level=3\x1f--cfg=fixture"}, ["-Cdebuginfo=1"])
        self.assertEqual(flags, ["-Copt-level=3", "--cfg=fixture", "-Cdebuginfo=1"])
        for env in ({"RUSTFLAGS": "-Cprofile-use=old"}, {"RUSTFLAGS": "-C target-cpu=native"},
                    {"RUSTFLAGS": "a", "CARGO_ENCODED_RUSTFLAGS": "b"}):
            with self.assertRaises(ValueError):
                pgo.base_flags(env, [])

    def test_default_training_binds_both_exact_binary_slots_and_is_training(self):
        plan = pgo.make_plan(self.args, {})
        command = plan["training_command"]
        expected = str((self.root / "pgo/binaries/instrumented/shardloom").resolve())
        self.assertEqual(command[command.index("--baseline-binary") + 1], expected)
        self.assertEqual(command[command.index("--candidate-binary") + 1], expected)
        self.assertEqual(plan["train_fixture"]["cases"], list(pgo.TRAIN_CASES))
        self.assertIn("not comparison or heldout evaluation", plan["training_scope"])
        self.assertFalse(plan["performance_claim_allowed"])

    def test_custom_argv_token_is_not_shell_expansion_or_certification(self):
        command = pgo.training_command(self.repo, self.root, Path("/some path/cli"), 'python runner.py {instrumented_binary} "$(not-a-shell)"')
        self.assertEqual(command[-2:], ["/some path/cli", "$(not-a-shell)"])
        with self.assertRaisesRegex(ValueError, "standalone"):
            pgo.training_command(self.repo, self.root, Path("/cli"), "python runner.py --binary={instrumented_binary}")
        self.args.training_command = "python runner.py {instrumented_binary}"
        self.assertIsNone(pgo.make_plan(self.args, {})["external_engine_invoked"])

    def test_fresh_profiles_reject_empty_symlink_count_and_byte_overflow(self):
        directory = self.root / "profiles"
        directory.mkdir()
        with self.assertRaises(ValueError):
            pgo.fresh_profiles(directory, 100)
        raw = directory / "shardloom-123.profraw"
        raw.write_bytes(b"profile")
        self.assertEqual(pgo.fresh_profiles(directory, 7), [raw])
        with self.assertRaises(ValueError):
            pgo.fresh_profiles(directory, 6)
        alias = directory / "alias.profraw"
        alias.symlink_to(raw)
        with self.assertRaises(ValueError):
            pgo.fresh_profiles(directory, 100)
        alias.unlink()
        for i in range(16):
            (directory / f"extra-{i}.profraw").write_bytes(b"x")
        with self.assertRaises(ValueError):
            pgo.fresh_profiles(directory, 100)

    def test_cargo_artifact_resolution_uses_recorded_cli_and_rejects_escape(self):
        target = self.root / "cargo"
        binary = target / "host/release-pgo/shardloom"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b"cli")
        messages = self.root / "cargo.json"
        item = {"reason": "compiler-artifact", "target": {"name": "shardloom", "kind": ["bin"]}, "executable": str(binary)}
        messages.write_text("compiler line\n" + json.dumps(item) + "\n")
        self.assertEqual(pgo.executable_from_messages(messages, target), binary.resolve())
        item["executable"] = str(self.repo / "shardloom")
        messages.write_text(json.dumps(item))
        with self.assertRaises(ValueError):
            pgo.executable_from_messages(messages, target)

    def fake_runner(self, fail=None):
        report = pgo.make_plan(self.args, {})
        output = Path(report["run_dir"])
        output.mkdir()
        target = output / "cargo-target"
        target.mkdir()
        report["profdata_path"] = "profdata"
        calls = []
        runner = types.SimpleNamespace(root=self.repo, output=output, args=self.args, report=report, env={})
        self.args.training_command = "train {instrumented_binary}"
        def step(name, command, env=None, timeout=None):
            calls.append((name, list(command), dict(env or {})))
            if name == fail:
                raise ValueError("injected stage failure")
            stdout = output / (name + ".json")
            if name.startswith("build-"):
                binary = target / "host/release-pgo/shardloom"
                binary.parent.mkdir(parents=True, exist_ok=True)
                binary.write_bytes(name.encode())
                stdout.write_text(json.dumps({"reason": "compiler-artifact", "target": {"name": "shardloom", "kind": ["bin"]}, "executable": str(binary)}))
            elif name == "training":
                (output / "profiles/shardloom-signature.profraw").write_bytes(b"fresh")
            elif name == "merge":
                Path(command[command.index("-o") + 1]).write_bytes(b"merged")
            return stdout
        runner.step = step
        return runner, target, calls

    def test_failed_instrumented_build_never_trains_merges_or_uses(self):
        runner, target, calls = self.fake_runner("build-instrumented")
        with self.assertRaisesRegex(ValueError, "injected"):
            pgo.build_variants(runner, target, "host")
        self.assertEqual([c[0] for c in calls], ["build-control", "build-instrumented"])

    def test_failed_training_never_merges_or_builds_profile_use(self):
        runner, target, calls = self.fake_runner("training")
        with self.assertRaisesRegex(ValueError, "injected"):
            pgo.build_variants(runner, target, "host")
        self.assertEqual(calls[-1][0], "training")
        self.assertNotIn("merge", [c[0] for c in calls])

    def test_three_frozen_binaries_flags_and_fresh_merge_inputs(self):
        runner, target, calls = self.fake_runner()
        pgo.build_variants(runner, target, "host")
        builds = [c for c in calls if c[0].startswith("build-")]
        self.assertTrue(all(c[1] == builds[0][1] for c in builds))
        self.assertEqual(builds[0][2]["CARGO_ENCODED_RUSTFLAGS"], "")
        self.assertIn("profile-generate=", builds[1][2]["CARGO_ENCODED_RUSTFLAGS"])
        self.assertIn("profile-use=", builds[2][2]["CARGO_ENCODED_RUSTFLAGS"])
        self.assertEqual(len({b["sha256"] for b in runner.report["binaries"].values()}), 3)
        self.assertTrue(all(Path(b["path"]).read_bytes() == ("build-" + key).encode() for key, b in runner.report["binaries"].items()))
        merge = next(c[1] for c in calls if c[0] == "merge")
        self.assertEqual(merge[4:], [str(runner.output / "profiles/shardloom-signature.profraw")])
        train = next(c for c in calls if c[0] == "training")
        self.assertTrue(train[2]["LLVM_PROFILE_FILE"].endswith("shardloom-%m.profraw"))

    def test_default_training_rejects_wrong_binary_and_duplicate_matrix(self):
        directory = self.root / "training/logs/heldout_operators_fixture"
        directory.mkdir(parents=True)
        binary = {"path": "/exact/instrumented", "sha256": "exact-hash"}
        rows = [{"case": c, "requested_workers": w, "variant": v, "sample": s, "passed": True}
                for c in pgo.TRAIN_CASES for w in (1, 4) for v in ("baseline", "candidate") for s in (0, 1)]
        summary = {"status": "passed", "all_requested_acceptance_cases_passed": True,
                   "cases": [{"name": c} for c in pgo.TRAIN_CASES], "records": rows,
                   "fixture": {"rows": 4096}, "binaries": {v: dict(binary) for v in ("baseline", "candidate")}}
        path = directory / "summary.json"
        runner = types.SimpleNamespace(output=self.root, report={})
        path.write_text(json.dumps(summary))
        pgo.verify_training(runner, binary)
        self.assertEqual(runner.report["training_evidence"]["records"], 96)
        summary["binaries"]["candidate"]["sha256"] = "wrong"
        path.write_text(json.dumps(summary))
        with self.assertRaisesRegex(ValueError, "different CLI"):
            pgo.verify_training(runner, binary)
        summary["binaries"]["candidate"] = binary
        summary["records"][-1] = summary["records"][0]
        path.write_text(json.dumps(summary))
        with self.assertRaisesRegex(ValueError, "matrix"):
            pgo.verify_training(runner, binary)

    def test_profile_guard_fails_before_process_creation(self):
        plan = pgo.make_plan(self.args, {})
        output = Path(plan["run_dir"])
        (output / "profiles").mkdir(parents=True)
        (output / "profiles/raw.profraw").write_bytes(b"oversized")
        self.args.max_profile_mib = 1 / pgo.MIB
        runner = pgo.Runner(self.args, plan)
        with patch.object(pgo, "check_budgets", return_value={"log_bytes": 0}), patch.object(subprocess, "Popen", side_effect=AssertionError("spawned")):
            with self.assertRaisesRegex(ValueError, "profile byte"):
                runner.step("denied", [sys.executable, "-c", "raise AssertionError()"])

    @unittest.skipUnless(os.name == "posix", "native process groups require POSIX")
    def test_timeout_stops_a_sigterm_ignoring_owned_process_group(self):
        plan = pgo.make_plan(self.args, {})
        output = Path(plan["run_dir"])
        (output / "logs").mkdir(parents=True)
        runner = pgo.Runner(self.args, plan)
        runner.guard = lambda: None
        code = "import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print(os.getpid(),flush=True); time.sleep(60)"
        with self.assertRaises(TimeoutError):
            runner.step("timeout", [sys.executable, "-c", code], timeout=0.1)
        pid = int((output / "logs/timeout.stdout").read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        self.assertEqual(plan["steps"][0]["status"], "failed")
        self.assertEqual(plan["steps"][0]["returncode"], -signal.SIGKILL)

    @unittest.skipUnless(os.name == "posix", "native process groups require POSIX")
    def test_group_cleanup_stops_descendant_after_leader_exits(self):
        import time
        plan = pgo.make_plan(self.args, {})
        output = Path(plan["run_dir"])
        (output / "logs").mkdir(parents=True)
        runner = pgo.Runner(self.args, plan)
        runner.guard = lambda: None
        child = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)"
        leader = "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]]); print(p.pid,flush=True)"
        runner.step("orphan", [sys.executable, "-c", leader, child], timeout=2)
        pid = int((output / "logs/orphan.stdout").read_text())
        # A killed orphan can briefly await init's reap; a zombie is not live.
        for _ in range(50):
            observed = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False).stdout.strip()
            if not observed or observed.startswith("Z"):
                break
            time.sleep(0.02)
        else:
            self.fail("owned descendant remained live after group cleanup")


if __name__ == "__main__":
    unittest.main()
