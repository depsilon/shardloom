#!/usr/bin/env python3
"""Partition one checked native test executable without changing Rust's runner."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import time

from ci_build_artifact import ROOT, sha, verify_bundle, write_json

BASELINE = ROOT / ".github/ci-native-baseline.json"
PARTITIONS = 4


def test_environment(root: Path = ROOT) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("RUST_TEST_")}
    env.update(CARGO_MANIFEST_DIR=str(root / "shardloom-vortex"),
               CARGO_MANIFEST_PATH=str(root / "shardloom-vortex/Cargo.toml"))
    return env


def parse_inventory(output: str) -> list[str]:
    names = []
    for line in output.splitlines():
        if line.endswith(": test"):
            names.append(line.removesuffix(": test"))
        elif line.strip() and not re.fullmatch(r"\d+ tests?, 0 benchmarks?", line):
            raise ValueError(f"unexpected native test inventory line: {line}")
    if len(names) != len(set(names)):
        raise ValueError("duplicate native test inventory entries")
    return sorted(names)


def discover(binary: Path, *, ignored: bool = False, root: Path = ROOT) -> list[str]:
    command = [str(binary.resolve()), "--list", "--format=terse", "--color=never"]
    if ignored:
        command += ["--ignored"]
    output = subprocess.check_output(command, cwd=root / "shardloom-vortex",
                                     env=test_environment(root), text=True, timeout=60)
    return parse_inventory(output)


def partition_tests(tests: list[str], ignored: list[str], baseline: dict,
                    count: int = PARTITIONS) -> list[dict]:
    if not tests or len(tests) != len(set(tests)) or count < 1:
        raise ValueError("empty/duplicate native inventory or invalid partition count")
    if sorted(ignored) != sorted(baseline["ignored_tests"]) or not set(ignored) <= set(tests):
        raise ValueError("native ignored-test policy changed")
    estimates = baseline["estimated_seconds"]
    default = baseline["unmeasured_test_weight_seconds"]
    if any(not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0
           for value in [default, *estimates.values()]):
        raise ValueError("native scheduling estimates must be finite positive numbers")
    ignored_set = set(ignored)
    weights = {name: 0.0 if name in ignored_set else estimates.get(name, default) for name in tests}
    partitions = [{"id": i, "estimated_seconds": 0.0, "tests": []} for i in range(count)]
    for name in sorted(tests, key=lambda name: (-weights[name], name)):
        target = min(partitions, key=lambda row: (row["estimated_seconds"], len(row["tests"]), row["id"]))
        target["tests"].append(name)
        target["estimated_seconds"] += weights[name]
    for row in partitions:
        row["tests"].sort()
        row["estimated_seconds"] = round(row["estimated_seconds"], 6)
    verify_coverage(tests, partitions)
    return partitions


def verify_coverage(tests: list[str], partitions: list[dict]) -> None:
    if len({row["id"] for row in partitions}) != len(partitions):
        raise ValueError("duplicate native partition IDs")
    assigned = Counter(name for row in partitions for name in row["tests"])
    if assigned != Counter(tests) or any(count != 1 for count in assigned.values()):
        raise ValueError("native partitions omit, duplicate or invent a test")
    if any(not row["tests"] for row in partitions):
        raise ValueError("empty native partition")


def prepare(directory: Path, *, root: Path = ROOT) -> dict:
    manifest = verify_bundle(directory, "native-tests", root=root)
    binary = directory / "native-tests"
    binary.chmod(0o755)
    tests = discover(binary, root=root)
    ignored = discover(binary, ignored=True, root=root)
    baseline_path = root / ".github/ci-native-baseline.json"
    baseline = json.loads(baseline_path.read_text())
    manifest["native_suite"] = {"tests": tests, "ignored": ignored,
        "baseline_sha256": sha(baseline_path), "partitions": partition_tests(tests, ignored, baseline),
        "test_threads": 1, "runner": "rust-libtest", "doctests": "not selected by --lib"}
    write_json(directory / "manifest.json", manifest)
    return manifest


def verify_plan(directory: Path, *, root: Path = ROOT) -> tuple[dict, dict]:
    manifest = verify_bundle(directory, "native-tests", root=root)
    binary = directory / "native-tests"
    binary.chmod(0o755)
    suite = manifest["native_suite"]
    tests = discover(binary, root=root)
    ignored = discover(binary, ignored=True, root=root)
    baseline_path = root / ".github/ci-native-baseline.json"
    expected = partition_tests(tests, ignored, json.loads(baseline_path.read_text()))
    if (suite["tests"] != tests or suite["ignored"] != ignored
            or suite["baseline_sha256"] != sha(baseline_path)
            or suite["partitions"] != expected or suite["test_threads"] != 1
            or suite["runner"] != "rust-libtest"):
        raise ValueError("native executable inventory/partition plan mismatch")
    return manifest, suite


def parse_results(output: str, expected: list[str], ignored: list[str]) -> dict:
    rows = re.findall(r"^test (.*?) \.\.\. (ok|FAILED|ignored)(?:[^\n]*)$", output, re.MULTILINE)
    normalized = [(name.removesuffix(" - should panic"), status) for name, status in rows]
    if Counter(name for name, _ in normalized) != Counter(expected):
        raise ValueError("native execution did not report every assigned test exactly once")
    if sorted(name for name, status in normalized if status == "ignored") != sorted(ignored):
        raise ValueError("native execution changed the ignored-test policy")
    if any(status == "FAILED" for _, status in normalized):
        raise ValueError("native test failed")
    summaries = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output)
    if len(summaries) != 1 or tuple(map(int, summaries[0])) != (
            len(expected) - len(ignored), 0, len(ignored)):
        raise ValueError("native test summary does not match its complete inventory")
    return {"passed": len(expected) - len(ignored), "ignored": len(ignored), "tests": expected}


def run_partition(directory: Path, partition: int, output: Path, *, root: Path = ROOT,
                  timeout: float = 3600) -> dict:
    manifest, suite = verify_plan(directory, root=root)
    selected = [row for row in suite["partitions"] if row["id"] == partition]
    if len(selected) != 1:
        raise ValueError("unknown native test partition")
    tests = selected[0]["tests"]
    ignored = sorted(set(tests) & set(suite["ignored"]))
    if output.exists() or output.is_symlink():
        raise ValueError("native partition output must be new")
    output.mkdir(parents=True)
    command = [str((directory / "native-tests").resolve()), "--exact", "--test-threads=1",
               "--format=pretty", "--color=never", *tests]
    receipt = {"schema_version": "shardloom.ci_native_partition.v1", "status": "running",
        "partition": partition, "artifact_manifest_sha256": sha(directory / "manifest.json"),
        "revision": manifest["identity"]["revision"], "test_threads": 1,
        "expected_tests": tests, "expected_ignored": ignored}
    log = output / "tests.log"
    started = time.monotonic()
    process = None
    try:
        with log.open("xb") as stream:
            process = subprocess.Popen(command, cwd=root / "shardloom-vortex",
                env=test_environment(root), stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            while process.poll() is None:
                if time.monotonic() - started > timeout or log.stat().st_size > 64 << 20:
                    raise RuntimeError("native partition exceeded its deadline or log budget")
                time.sleep(0.2)
        receipt["returncode"] = process.returncode
        if process.returncode:
            raise RuntimeError(f"native partition failed with {process.returncode}; see {log}")
        if log.stat().st_size > 64 << 20:
            raise RuntimeError("native partition exceeded its log budget")
        # A passing parent must not hide a leaked fault-injection/recovery child.
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            receipt["children_drained"] = True
        else:
            receipt["children_drained"] = False
            raise RuntimeError("native partition left a child process running")
        receipt["result"] = parse_results(log.read_text(), tests, ignored)
        receipt["status"] = "passed"
    except BaseException as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        if process is not None:
            # Include fault-injection children, even when the harness parent exited.
            try:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            except ProcessLookupError:
                pass
            process.wait()
        receipt["seconds"] = time.monotonic() - started
        if log.exists():
            receipt["log_sha256"] = sha(log)
        write_json(output / "report.json", receipt)
    return receipt


def aggregate(directory: Path, reports: Path, *, root: Path = ROOT) -> dict:
    manifest, suite = verify_plan(directory, root=root)
    paths = sorted(reports.glob("*/report.json"))
    rows = [json.loads(path.read_text()) for path in paths]
    if Counter(row["partition"] for row in rows) != Counter(row["id"] for row in suite["partitions"]):
        raise ValueError("missing, duplicate or unexpected native partition reports")
    for path, row in zip(paths, rows, strict=True):
        owned = suite["partitions"][row["partition"]]["tests"]
        ignored = sorted(set(owned) & set(suite["ignored"]))
        log = path.parent / "tests.log"
        if (log.is_symlink() or not log.is_file() or log.stat().st_size > 64 << 20
                or row.get("log_sha256") != sha(log)):
            raise ValueError("native partition log is missing or changed")
        actual = parse_results(log.read_text(), owned, ignored)
        if (row.get("schema_version") != "shardloom.ci_native_partition.v1"
                or row["status"] != "passed" or row["returncode"] != 0
                or row.get("children_drained") is not True or row.get("test_threads") != 1
                or row.get("revision") != manifest["identity"]["revision"]
                or row["artifact_manifest_sha256"] != sha(directory / "manifest.json")
                or row.get("expected_tests") != owned or row["expected_ignored"] != ignored
                or row["result"] != actual
                or not isinstance(row.get("seconds"), (int, float))
                or not math.isfinite(row["seconds"]) or row["seconds"] < 0):
            raise ValueError("native partition evidence does not satisfy the complete suite")
    return {"schema_version": "shardloom.ci_native_coverage.v1", "status": "passed",
        "tests": len(suite["tests"]), "passed": len(suite["tests"]) - len(suite["ignored"]),
        "ignored": len(suite["ignored"]), "partitions": len(rows),
        "summed_partition_seconds": sum(row["seconds"] for row in rows)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "run", "aggregate"))
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--partition", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.action == "prepare":
        result = prepare(args.bundle)
        result = {"tests": len(result["native_suite"]["tests"]), "partitions": PARTITIONS}
    elif args.action == "run":
        if args.partition is None or args.output is None:
            parser.error("run requires --partition and --output")
        result = run_partition(args.bundle, args.partition, args.output)
        result = {key: result[key] for key in ("status", "partition", "seconds")}
    else:
        if args.output is None:
            parser.error("aggregate requires --output containing partition reports")
        result = aggregate(args.bundle, args.output)
        write_json(args.output / "coverage.json", result)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
