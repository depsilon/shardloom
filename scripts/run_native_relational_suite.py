#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run complete public native acceptance as sequential, independently guarded families.

Family summaries remain immutable. The suite is their checked union, with exact
membership against an optional pre-execution case manifest. This avoids making
each small native operation rescan every preceding family's output directory.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

from local_uat_storage import MIB, StorageGuardError, accounted_bytes
from native_workflow_materialization import MATERIALIZATIONS
from native_workflow_protocol import strict_json
from run_clickbench_query_uat import file_sha256, stop_process

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "benchmarks/traditional_analytics"))
from resources import BenchmarkGuard

FAMILIES = ("base", "unary", "nested", "pivot", "typed", "memory", "frames")
SCHEMA = "shardloom.native_relational_suite.v1"
VARIABLE_FIELDS = {
    "schema_version", "acceptance_family", "cases", "envelope_files",
    "envelope_archives", "source_files", "source_sha256",
}


def source_identity(path: Path) -> list[int]:
    info = path.stat()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def combine_summaries(cohorts: list[dict], expected: dict[str, int] | None = None) -> dict:
    """Reject missing/duplicate families, stale provenance and incomplete values."""
    if expected is not None and (not isinstance(expected, dict) or any(
            not isinstance(name, str) or type(count) is not int or count < 0
            for name, count in expected.items())):
        raise ValueError("invalid frozen case manifest")
    if [item["family"] for item in cohorts] != list(FAMILIES):
        raise ValueError("suite requires each declared family exactly once, in order")
    common = None
    merged = {key: [] for key in ("cases", "source_files", "envelope_files", "envelope_archives")}
    for item in cohorts:
        path = Path(item["path"])
        if file_sha256(path) != item["sha256"]:
            raise ValueError(f"family summary changed: {path}")
        summary = strict_json(path.read_text())
        if (summary["schema_version"] != "shardloom.native_relational_python_acceptance.v1"
                or summary["status"] != "passed"
                or summary["acceptance_family"] != item["family"]
                or summary["fallback_attempted"] is not False
                or summary["external_engine_invoked"] is not False):
            raise ValueError(f"incomplete or incompatible family: {item['family']}")
        fields = {key: value for key, value in summary.items() if key not in VARIABLE_FIELDS}
        if common is None:
            common = fields
        elif common != fields:
            raise ValueError(f"family execution provenance differs: {item['family']}")
        for key in merged:
            merged[key].extend(summary[key])
        for source in summary["source_files"]:
            source_path = Path(source["path"])
            if (source_identity(source_path) != source["identity"]
                    or file_sha256(source_path) != source["sha256"]):
                raise ValueError(f"family source changed: {source_path}")
    cases = {}
    for case in merged["cases"]:
        if (case["name"] in cases or case["passed"] is not True
                or type(case["complete_rows_verified"]) is not int
                or case["complete_rows_verified"] < 0):
            raise ValueError("suite contains duplicate, invalid or failed result checks")
        cases[case["name"]] = case["complete_rows_verified"]
    if not cases or (expected is not None and cases != expected):
        raise ValueError("suite does not match the complete frozen case manifest")
    for key in ("source_files", "envelope_files", "envelope_archives"):
        paths = [item["path"] for item in merged[key]]
        if len(paths) != len(set(paths)):
            raise ValueError(f"suite contains duplicate {key} paths")
    return {**common, **merged, "schema_version": SCHEMA, "acceptance_family": "all",
            "cohort_summaries": cohorts, "complete_rows_verified": sum(cases.values()),
            "case_count": len(cases)}


def run_family(command: list[str], log: Path, guard, timeout: float) -> dict:
    started = time.monotonic()
    process = None
    receipt = {"command": command, "timeout_seconds": timeout, "status": "running"}
    try:
        guard()
        with log.open("x") as stream:
            process = subprocess.Popen(command, cwd=REPO, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            receipt["pid"] = process.pid
            sampled = started
            while process.poll() is None:
                if time.monotonic() - sampled >= 2:
                    guard()
                    sampled = time.monotonic()
                if time.monotonic() - started > timeout:
                    raise TimeoutError("native acceptance family exceeded its deadline")
                if log.stat().st_size > 8 * MIB:
                    raise StorageGuardError("native acceptance family log exceeded 8 MiB")
                time.sleep(0.25)
        receipt["returncode"] = process.returncode
        if process.returncode:
            raise ValueError(f"native family returned {process.returncode}; inspect {log}")
        guard()
        receipt["status"] = "passed"
        return receipt
    except BaseException as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        if process is not None:
            stop_process(process)
        raise
    finally:
        receipt["supervised_seconds"] = time.monotonic() - started
        if log.exists():
            receipt["log_sha256"] = file_sha256(log)
        with log.with_suffix(".json").open("x") as stream:
            json.dump(receipt, stream, indent=2)
            stream.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--build-commit", required=True)
    parser.add_argument("--nested-fixture-generator", type=Path, required=True)
    parser.add_argument("--typed-fixture-generator", type=Path, required=True)
    parser.add_argument("--materializations", nargs="+", choices=MATERIALIZATIONS,
                        default=list(MATERIALIZATIONS))
    parser.add_argument("--expected-manifest", type=Path,
                        help="JSON with a frozen name-to-row-count mapping in its cases field")
    parser.add_argument("--family-timeout", type=float, default=3000)
    args = parser.parse_args()
    if not 0 < args.family_timeout <= 3000:
        parser.error("family deadline must be positive and no greater than 3000 seconds")
    binary = args.binary.resolve(strict=True)
    expected_path = args.expected_manifest.resolve(strict=True) if args.expected_manifest else None
    expected_sha = file_sha256(expected_path) if expected_path else None
    expected = strict_json(expected_path.read_text())["cases"] if expected_path else None
    root = args.uat_root.resolve()
    if root.exists():
        parser.error("suite destination must be new; prior evidence is never replaced")
    runner = Path(__file__).with_name("run_native_relational_uat.py")
    frozen = {str(path): file_sha256(path) for path in (
        binary, runner, Path(__file__).resolve(),
        args.nested_fixture_generator.resolve(strict=True),
        args.typed_fixture_generator.resolve(strict=True),
    )}
    cohorts = []
    report = {"schema_version": SCHEMA, "status": "running", "cohort_summaries": cohorts}
    started = time.monotonic()

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    with BenchmarkGuard(root, binary=binary, memory_gb=24) as supervisor:
        logs = root / "logs"
        logs.mkdir()

        def guard():
            supervisor.check()
            log_bytes = accounted_bytes(logs) + sum(
                accounted_bytes(root / family / "logs") for family in FAMILIES)
            if log_bytes > 192 * MIB:
                raise StorageGuardError("combined native acceptance logs exceeded 192 MiB")

        try:
            for family in FAMILIES:
                command = [sys.executable, "-B", str(runner), "--binary", str(binary),
                           "--uat-root", str(root / family), "--build-commit", args.build_commit,
                           "--nested-fixture-generator", str(args.nested_fixture_generator.resolve()),
                           "--typed-fixture-generator", str(args.typed_fixture_generator.resolve()),
                           "--family", family, "--materializations", *args.materializations,
                           "--compress-logs", "--archive-logs"]
                print(f"Starting native acceptance family: {family}", flush=True)
                run_family(command, logs / f"{family}.log", guard, args.family_timeout)
                paths = list((root / family / "logs").glob("*/summary.json"))
                if len(paths) != 1:
                    raise ValueError(f"family must produce exactly one summary: {family}")
                path = paths[0]
                summary = strict_json(path.read_text())
                cohorts.append({"family": family, "path": str(path), "sha256": file_sha256(path)})
                print(json.dumps({"family": family, "status": summary["status"],
                                  "checks": len(summary["cases"])}), flush=True)
            if any(file_sha256(Path(path)) != digest for path, digest in frozen.items()):
                raise ValueError("native suite executable or controller changed")
            if expected_path and file_sha256(expected_path) != expected_sha:
                raise ValueError("frozen expected case manifest changed")
            report = combine_summaries(cohorts, expected)
            report["suite_controller_sha256"] = frozen[str(Path(__file__).resolve())]
            report["expected_manifest"] = ({"path": str(expected_path), "sha256": expected_sha}
                                           if expected_path else None)
            guard()
            return 0
        except BaseException as error:
            report.update(status="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            report["supervised_seconds"] = time.monotonic() - started
            report["supervisor_samples"] = supervisor.samples
            with (root / "summary.json").open("x") as stream:
                json.dump(report, stream, indent=2)
                stream.write("\n")
            print(root / "summary.json", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
