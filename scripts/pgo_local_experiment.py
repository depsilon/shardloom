# SPDX-License-Identifier: Apache-2.0
"""Guarded local PGO orchestration, separate from release and runtime policy."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid

from local_uat_storage import GIB, MIB, accounted_bytes, check_budgets, require_local_path
from release_report_utils import rust_toolchain_version

TRAIN_CASES = ("metadata_count", "nullable_numeric_scalar", "exact_integer_extrema",
               "numeric_group_sum", "skewed_string_count", "nullable_string_count",
               "composite_group", "compound_count_topk", "nullable_group_distinct",
               "utf8_byte_length", "filtered_sort_offset", "nullable_projection")
PROFILE_PATTERN = "shardloom-%m.profraw"
SMOKE_SOURCE = '''fn main() {
    let n: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
    let value: u64 = (0..n).map(|i| if i % 3 == 0 { i * 2 } else { i }).sum();
    println!("{value}");
}
'''


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Print-only by default; --run executes guarded local PGO.", allow_abbrev=False)
    if sys.version_info < (3, 11):
        p.error("Python 3.11 or newer is required by this helper and its native training harness")
    p.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument("--run-dir", type=Path, help="Fresh, non-existing local directory; never reused or removed")
    p.add_argument("--target", help="Native host triple; otherwise resolved from rustc -vV during --run")
    p.add_argument("--llvm-profdata", default=shutil.which("llvm-profdata") or "llvm-profdata")
    p.add_argument("--features", default="release-user-surfaces")
    p.add_argument("--training-command", help="Shell-free argv string with standalone {instrumented_binary} token")
    p.add_argument("--rustflag", action="append", default=[])
    for name, default in (("max-run-gib", 40), ("max-profile-mib", 1024), ("max-log-mib", 256),
                          ("min-free-gib", 12), ("stage-timeout", 3600), ("training-timeout", 900)):
        p.add_argument("--" + name, type=float, default=default)
    p.add_argument("--smoke-only", action="store_true")
    p.add_argument("--run", action="store_true")
    args = p.parse_args(argv)
    for field in ("max_run_gib", "max_profile_mib", "max_log_mib", "min_free_gib", "stage_timeout", "training_timeout"):
        if not math.isfinite(getattr(args, field)) or getattr(args, field) <= 0:
            p.error(f"{field} must be finite and positive")
    return args


def base_flags(env, extra):
    if env.get("RUSTFLAGS") and env.get("CARGO_ENCODED_RUSTFLAGS"):
        raise ValueError("set only one of RUSTFLAGS and CARGO_ENCODED_RUSTFLAGS")
    flags = (env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f") if env.get("CARGO_ENCODED_RUSTFLAGS")
             else shlex.split(env.get("RUSTFLAGS", ""))) + extra
    if any(word in " ".join(flags) for word in ("profile-generate", "profile-use", "instrument-coverage", "target-cpu=native")):
        raise ValueError("base flags cannot contain existing instrumentation/PGO or target-cpu=native")
    return flags


def identity(path):
    info = path.stat()
    return {"device": info.st_dev, "inode": info.st_ino, "bytes": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def file_record(path):
    before = identity(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"expected owned regular file: {path}")
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if identity(path) != before:
        raise ValueError(f"file changed while hashing: {path}")
    return {"path": str(path), "sha256": digest, "generation": before}


def training_command(root, output, binary, custom):
    if custom is not None:
        argv = shlex.split(custom)
        if "{instrumented_binary}" not in argv:
            raise ValueError("custom training requires a standalone {instrumented_binary} argv token")
        return [str(binary) if arg == "{instrumented_binary}" else arg for arg in argv]
    return [sys.executable, str(root / "scripts/run_heldout_operator_uat.py"),
            "--baseline-binary", str(binary), "--candidate-binary", str(binary),
            "--uat-root", str(output / "training"), "--rows", "4096", "--samples", "1",
            "--workers", "1,4", "--cases", ",".join(TRAIN_CASES), "--timeout", "60"]


def make_plan(args, env):
    root = args.repo_root.resolve()
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    proposed = args.run_dir or Path.home() / "LocalData/shardloom/pgo" / f"{stamp}-{uuid.uuid4().hex[:8]}"
    output = require_local_path(proposed, Path.home(), sys.platform)
    if output == root or output.is_relative_to(root):
        raise ValueError("PGO outputs must be outside the source checkout")
    if output.exists():
        raise ValueError("run-dir already exists; profiles/artifacts are never reused or deleted")
    target = args.target or "<rustc-host-target>"
    if args.target and not re.fullmatch(r"[A-Za-z0-9_-]+", args.target):
        raise ValueError("invalid native target triple")
    return {"schema_version": "shardloom.pgo_build_helper.v2", "status": "print_only",
            "repo_root": str(root), "run_dir": str(output), "target": target,
            "cargo_target_dir": str(output / "cargo-target"), "profile_dir": str(output / "profiles"),
            "merged_profile": str(output / "merged.profdata"), "profile_pattern": PROFILE_PATTERN,
            "base_rustflags": base_flags(env, args.rustflag), "features": args.features, "profile": "release-pgo",
            "build_command_template": ["cargo", "build", "--locked", "--offline", "-p", "shardloom-cli", "--bin", "shardloom", "--features", args.features, "--profile", "release-pgo", "--target", target, "--message-format=json-render-diagnostics"],
            "training_command": training_command(root, output, output / "binaries/instrumented/shardloom", args.training_command),
            "training_scope": "training only; exact same instrumented CLI in both harness slots; not comparison or heldout evaluation" if args.training_command is None else "custom command; supplied binary token does not certify actual use, correctness, or external-engine policy",
            "train_fixture": {"rows": 4096, "workers": [1, 4], "samples": 1, "warmups": 1, "cases": list(TRAIN_CASES)} if args.training_command is None else None,
            "evaluation_required": "Separate untrained schema/distribution or case subset; full exact values and matched control/profile-use binaries. Training output is not evaluation.",
            "limits": {key: getattr(args, key) for key in ("max_run_gib", "max_profile_mib", "max_log_mib", "min_free_gib", "stage_timeout", "training_timeout")},
            "smoke_only": args.smoke_only, "benchmark_only_build": True,
            "portable_release_artifact": False, "target_cpu_native_enabled": False,
            "publication_attempted": False, "performance_claim_allowed": False,
            "external_engine_invoked": None if args.run or args.training_command else False,
            "fallback_attempted": None if args.run or args.training_command else False,
            "compatibility_status": "not_run", "steps": []}


def stop_group(process):
    """Stop ordinary descendants even when their leader already exited."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


class Runner:
    def __init__(self, args, report):
        self.args, self.report = args, report
        self.root, self.output = Path(report["repo_root"]), Path(report["run_dir"])
        self.env = os.environ.copy()
        self.env["RUSTUP_TOOLCHAIN"] = self.env.get("RUSTUP_TOOLCHAIN", rust_toolchain_version(self.root))
        self.env["CARGO_TARGET_DIR"] = report["cargo_target_dir"]
        for key in ("TMPDIR", "TMP", "TEMP"):
            self.env[key] = str(self.output / "tmp")
        for key in ("RUSTFLAGS", "LLVM_PROFILE_FILE", "SHARDLOOM_PGO_PROFILE"):
            self.env.pop(key, None)
        self.env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(report["base_rustflags"])

    def guard(self):
        a = self.args
        snapshot = check_budgets(self.output, self.output / "cargo-target", self.output / "logs",
                                min_free_bytes=int(a.min_free_gib * GIB), reserve_bytes=0,
                                max_workspace_bytes=int(a.max_run_gib * GIB), max_log_bytes=int(a.max_log_mib * MIB))
        profiles = accounted_bytes(self.output / "profiles") + accounted_bytes(self.output / "smoke/profiles")
        if profiles > a.max_profile_mib * MIB:
            raise ValueError("sampled profile byte budget exceeded")
        if snapshot["log_bytes"] + accounted_bytes(self.output / "training/logs") > a.max_log_mib * MIB:
            raise ValueError("sampled combined log budget exceeded")

    def step(self, name, command, env=None, timeout=None):
        self.guard()
        out, err = self.output / "logs" / (name + ".stdout"), self.output / "logs" / (name + ".stderr")
        record = {"stage": name, "command": command, "status": "running"}
        self.report["steps"].append(record)
        started = time.monotonic()
        with out.open("xb") as stdout, err.open("xb") as stderr:
            try:
                process = subprocess.Popen(command, cwd=self.root, env=env or self.env,
                                           stdout=stdout, stderr=stderr, start_new_session=True)
            except BaseException as error:
                record.update(status="failed", error=str(error), seconds=time.monotonic() - started)
                raise
            try:
                while process.poll() is None:
                    if time.monotonic() - started > (timeout or self.args.stage_timeout):
                        raise TimeoutError(f"PGO stage timeout: {name}")
                    self.guard()
                    time.sleep(0.25)
                if process.returncode:
                    raise ValueError(f"PGO stage failed: {name} (exit {process.returncode})")
                self.guard()
                record["status"] = "passed"
            except BaseException:
                record["status"] = "failed"
                raise
            finally:
                previous = {s: signal.signal(s, signal.SIG_IGN) for s in (signal.SIGINT, signal.SIGTERM)}
                try:
                    stop_group(process)
                finally:
                    for sig, handler in previous.items():
                        signal.signal(sig, handler)
                record.update(seconds=time.monotonic() - started, returncode=process.returncode,
                              stdout=file_record(out), stderr=file_record(err))
        return out


def fresh_profiles(directory, max_bytes):
    paths = sorted(directory.iterdir())
    if not paths or len(paths) > 16:
        raise ValueError("expected 1..16 fresh binary-signature profiles")
    for path in paths:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or path.suffix != ".profraw" or info.st_size == 0:
            raise ValueError(f"unexpected profile entry: {path}")
    if sum(path.stat().st_size for path in paths) > max_bytes:
        raise ValueError("profile bytes exceed declared cap")
    return paths


def executable_from_messages(path, target_dir):
    target_dir = target_dir.resolve()
    candidates = set()
    for line in path.read_text().splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        target = item.get("target", {})
        if item.get("reason") == "compiler-artifact" and target.get("name") == "shardloom" and "bin" in target.get("kind", []) and item.get("executable"):
            candidates.add(Path(item["executable"]).resolve())
    if len(candidates) != 1:
        raise ValueError("Cargo did not resolve exactly one shardloom CLI executable")
    binary = candidates.pop()
    if not binary.is_relative_to(target_dir) or not binary.is_file():
        raise ValueError("Cargo executable escaped the owned target directory")
    return binary


def run_smoke(runner, rustc, profdata, target):
    smoke = runner.output / "smoke"
    profiles = smoke / "profiles"
    profiles.mkdir(parents=True)
    source = smoke / "main.rs"
    source.write_text(SMOKE_SOURCE)
    generated, used, merged = smoke / "instrumented", smoke / "profile-use", smoke / "merged.profdata"
    common = [rustc, str(source), "--edition=2024", "--target", target, "-O", "-Clto=thin", "-Ccodegen-units=1", *runner.report["base_rustflags"]]
    runner.step("smoke-generate", [*common, f"-Cprofile-generate={profiles}", "-o", str(generated)])
    env = dict(runner.env, LLVM_PROFILE_FILE=str(profiles / PROFILE_PATTERN))
    expected = str(sum(i * 2 if i % 3 == 0 else i for i in range(1000)))
    if runner.step("smoke-train", [str(generated), "1000"], env).read_text().strip() != expected:
        raise ValueError("instrumented smoke result mismatch")
    inputs = fresh_profiles(profiles, runner.args.max_profile_mib * MIB)
    runner.step("smoke-merge", [profdata, "merge", "-o", str(merged), *map(str, inputs)])
    runner.step("smoke-use", [*common, f"-Cprofile-use={merged}", "-Cllvm-args=-pgo-warn-missing-function", "-o", str(used)])
    if runner.step("smoke-validate", [str(used), "1000"]).read_text().strip() != expected:
        raise ValueError("profile-use smoke result mismatch")
    runner.report["compatibility_status"] = "tiny_native_instrument_merge_use_exact_result_passed"
    runner.report["smoke_artifacts"] = [file_record(p) for p in [source, generated, used, merged, *inputs]]


def verify_training(runner, binary):
    paths = list((runner.output / "training/logs").glob("heldout_operators_*/summary.json"))
    if len(paths) != 1:
        raise ValueError("default training did not produce exactly one acceptance summary")
    summary = json.loads(paths[0].read_text())
    rows = summary["records"]
    if summary["status"] != "passed" or not summary["all_requested_acceptance_cases_passed"] or len(rows) != len(TRAIN_CASES) * 8 or not all(r["passed"] for r in rows):
        raise ValueError("default training incomplete or failed full-value checks")
    if {c["name"] for c in summary["cases"]} != set(TRAIN_CASES):
        raise ValueError("training cases differ from declared corpus")
    expected = {(case, worker, variant, sample) for case in TRAIN_CASES for worker in (1, 4)
                for variant in ("baseline", "candidate") for sample in (0, 1)}
    observed = {(row["case"], row["requested_workers"], row["variant"], row["sample"]) for row in rows}
    if observed != expected or summary["fixture"]["rows"] != 4096:
        raise ValueError("training matrix or fixture differs from declared corpus")
    if any(item["sha256"] != binary["sha256"] or item["path"] != binary["path"] for item in summary["binaries"].values()):
        raise ValueError("training used a different CLI binary")
    runner.report["training_evidence"] = {"summary": file_record(paths[0]), "fixture": summary["fixture"],
                                          "records": len(rows), "all_exact_values_passed": True,
                                          "role": "training_not_independent_evaluation"}
    runner.report.update(external_engine_invoked=False, fallback_attempted=False)


def build_variants(runner, target_dir, target):
    report, args = runner.report, runner.args
    command = list(report["build_command_template"])
    command[command.index("--target") + 1] = target
    report["build_command_template"], report["binaries"] = command, {}
    profiles, merged = Path(report["profile_dir"]), Path(report["merged_profile"])
    profiles.mkdir()
    for variant in ("control", "instrumented", "profile-use"):
        flags, env = list(report["base_rustflags"]), dict(runner.env)
        if variant == "instrumented":
            flags.append(f"-Cprofile-generate={profiles}")
        elif variant == "profile-use":
            flags += [f"-Cprofile-use={merged}", "-Cllvm-args=-pgo-warn-missing-function"]
            env["SHARDLOOM_PGO_PROFILE"] = str(merged)
        env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(flags)
        stdout = runner.step("build-" + variant, command, env)
        binary = executable_from_messages(stdout, target_dir)
        frozen = runner.output / "binaries" / variant / "shardloom"
        frozen.parent.mkdir(parents=True)
        shutil.copy2(binary, frozen)
        before = file_record(frozen)
        report["binaries"][variant] = {**before, "rustflags": flags}
        if variant == "instrumented":
            if list(profiles.iterdir()):
                raise ValueError("build unexpectedly produced profiles before training")
            train_env = dict(runner.env, LLVM_PROFILE_FILE=str(profiles / PROFILE_PATTERN), SHARDLOOM_PGO_TRAIN_BINARY=str(frozen))
            runner.step("training", training_command(runner.root, runner.output, frozen, args.training_command), train_env, args.training_timeout)
            if file_record(frozen) != before:
                raise ValueError("instrumented binary changed during training")
            if args.training_command is None:
                verify_training(runner, before)
            inputs = fresh_profiles(profiles, args.max_profile_mib * MIB)
            report["profile_inputs"] = [file_record(p) for p in inputs]
            runner.step("merge", [report["profdata_path"], "merge", "-o", str(merged), *map(str, inputs)])
            report["merged_profile_artifact"] = file_record(merged)


def execute(args, report):
    output = Path(report["run_dir"])
    reservation = MIB if args.smoke_only else int(args.max_run_gib * GIB)
    check_budgets(output, output / "cargo-target", output / "logs", min_free_bytes=int(args.min_free_gib * GIB),
                  reserve_bytes=reservation, max_workspace_bytes=int(args.max_run_gib * GIB), max_log_bytes=int(args.max_log_mib * MIB))
    output.mkdir(parents=True, exist_ok=False)
    (output / "logs").mkdir()
    (output / "tmp").mkdir()
    runner = Runner(args, report)
    report["status"] = "running"
    try:
        rustc = shutil.which("rustc") or "rustc"
        profdata = shutil.which(args.llvm_profdata) or args.llvm_profdata
        versions = runner.step("rustc-version", [rustc, "-vV"]).read_text()
        report["rustc_version"], report["profdata_path"] = versions, profdata
        report["profdata_version"] = runner.step("profdata-version", [profdata, "--version"]).read_text()
        host = next(line.removeprefix("host: ") for line in versions.splitlines() if line.startswith("host: "))
        target = args.target or host
        if target != host:
            raise ValueError("this executable local experiment requires the rustc host target")
        report["target"], report["toolchain"] = target, runner.env["RUSTUP_TOOLCHAIN"]
        run_smoke(runner, rustc, profdata, target)
        if args.smoke_only:
            report["status"] = "smoke_passed_no_workspace_build"
            return
        if runner.step("source-status", ["git", "status", "--porcelain"]).read_text():
            raise ValueError("freeze a clean source checkout before PGO workspace builds")
        report["source_commit"] = runner.step("source-commit", ["git", "rev-parse", "HEAD"]).read_text().strip()
        names = ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "scripts/build_shardloom_pgo.py", "scripts/pgo_local_experiment.py", "scripts/run_heldout_operator_uat.py", "scripts/run_resident_call_path_uat.py", "scripts/run_clickbench_query_uat.py", "scripts/timed_native_command.py", "scripts/local_uat_storage.py")
        report["source_files"] = [file_record(runner.root / p) for p in names if (runner.root / p).exists()]
        metadata = json.loads(runner.step("cargo-metadata", ["cargo", "metadata", "--offline", "--locked", "--no-deps", "--format-version", "1"]).read_text())
        target_dir = Path(metadata["target_directory"]).resolve()
        if target_dir != Path(report["cargo_target_dir"]):
            raise ValueError("resolved Cargo target does not match owned output")
        build_variants(runner, target_dir, target)
        if runner.step("source-status-final", ["git", "status", "--porcelain"]).read_text() or runner.step("source-commit-final", ["git", "rev-parse", "HEAD"]).read_text().strip() != report["source_commit"]:
            raise ValueError("source changed during PGO experiment")
        for record in [*report["source_files"], *report["profile_inputs"], report["merged_profile_artifact"]]:
            if file_record(Path(record["path"])) != record:
                raise ValueError("recorded build/source/profile generation changed")
        for record in report["binaries"].values():
            if file_record(Path(record["path"])) != {k: v for k, v in record.items() if k != "rustflags"}:
                raise ValueError("frozen binary generation changed")
        report["status"] = "built_and_trained_requires_independent_evaluation"
    except BaseException as error:
        report.update(status="failed", error=str(error) or type(error).__name__)
        raise
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")


def main(argv=None):
    args = parse_args(argv)
    report = make_plan(args, os.environ)
    if args.run:
        def interrupted(_signal, _frame):
            raise KeyboardInterrupt("PGO experiment interrupted")
        signal.signal(signal.SIGTERM, interrupted)
        try:
            execute(args, report)
        except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
            report.update(status="failed", error=str(error))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["status"] == "failed" else 0
