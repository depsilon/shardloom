#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Guarded public-CLI ClickBench runs with complete result regression checks.

This harness does not call an external engine. A retained-output comparison is
regression evidence, not an independent correctness oracle or an official rank.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import threading
import time

from local_uat_storage import GIB, MIB, StorageGuardError, check_budgets, require_local_path


def strict_json(text: str):
    def invalid(value):
        raise ValueError(f"nonfinite JSON value: {value}")
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            invalid(value)
        return number
    return json.loads(text, parse_constant=invalid, parse_float=finite_float)


def extract_result(envelope: dict):
    if envelope.get("status") != "success":
        raise ValueError("public operation did not report success")
    fields = envelope.get("fields", [])
    for required in ("public_workflow_fallback_attempted", "public_workflow_external_engine_invoked"):
        observed = [field["value"] for field in fields if field.get("key") == required]
        if not observed or any(not (value is False or value == "false") for value in observed):
            raise ValueError(f"missing or unsafe execution evidence: {required}")
    for field in fields:
        if field.get("key", "").endswith(("fallback_attempted", "external_engine_invoked")):
            if not (field.get("value") is False or field.get("value") == "false"):
                raise ValueError(f"unsafe execution evidence: {field['key']}")
    summaries = [line for line in envelope.get("human_text", "").splitlines()
                 if line.startswith(("result summary: ", "value summary: "))]
    if len(summaries) != 1:
        raise ValueError("expected one complete result summary")
    summary = summaries[0].split(": ", 1)[1]
    if " values=" in summary:
        payload = strict_json(summary.split(" values=", 1)[1])
        if "values" in payload:
            values = payload["values"]
            if isinstance(values, dict) and payload.get("rows") == 1:
                return [values]
            if not isinstance(values, list) or payload.get("rows") != len(values):
                raise ValueError("result preview is truncated or has an invalid row count")
            return values
        if "count" in payload:
            count = payload["count"]
            if type(count) is not int or count < 0:
                raise ValueError("invalid native count result")
            return count
        raise ValueError("result payload contains no complete values")
    result = strict_json(summary)
    if type(result) is not int or result < 0:
        raise ValueError("unsupported scalar result")
    return result


def equivalent(actual, expected) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(equivalent(actual[key], expected[key]) for key in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(equivalent(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, float):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    return actual == expected


def file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run_command(command: list[str], stdout: Path, stderr: Path, timeout: float, guard) -> dict:
    """Time through process exit, not through the watchdog's sampling interval."""
    stopped = threading.Event()
    failures: list[str] = []
    with stdout.open("xb") as out, stderr.open("xb") as err:
        started = time.perf_counter()
        process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)

        def watch():
            while not stopped.wait(0.25):
                try:
                    if time.perf_counter() - started > timeout:
                        raise ValueError("native command timeout")
                    if stdout.stat().st_size + stderr.stat().st_size > 8 * MIB:
                        raise ValueError("native command output exceeded 8 MiB")
                    guard()
                except (OSError, ValueError) as error:
                    failures.append(str(error))
                    stop_process(process)
                    return

        watcher = threading.Thread(target=watch, name="uat-storage-watchdog")
        watcher.start()
        try:
            returncode = process.wait()
            seconds = time.perf_counter() - started
        finally:
            stopped.set()
            stop_process(process)
            watcher.join()
    guard()
    if stdout.stat().st_size + stderr.stat().st_size > 8 * MIB:
        failures.append("native command output exceeded 8 MiB")
    return {"returncode": returncode, "seconds": seconds, "guard_failures": failures}


def score(records: list[dict], query_count: int) -> dict:
    complete = (len(records) == query_count * 3
                and all(record["passed"] for record in records)
                and {(record["query"], record["run"]) for record in records}
                == {(query, run) for query in range(1, query_count + 1) for run in range(1, 4)})
    if not complete:
        return {"complete": False, "runs_completed": len(records), "runs_passed": sum(record["passed"] for record in records)}
    timings = [[record["seconds"] for record in records if record["query"] == query] for query in range(1, query_count + 1)]
    best = [min(runs) for runs in timings]
    hot = [min(runs[1:]) for runs in timings]
    return {
        "complete": True, "queries_passed": query_count,
        "runs_completed": len(records), "runs_passed": len(records),
        "query_total_seconds": sum(best), "hot_total_seconds": sum(hot),
        "all_raw_run_seconds": sum(sum(runs) for runs in timings),
        "geomean_seconds": math.exp(sum(math.log(value) for value in best) / query_count),
        "hot_geomean_seconds": math.exp(sum(math.log(value) for value in hot) / query_count),
        "query_runs": timings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--uat-root", required=True, type=Path)
    parser.add_argument("--queries", type=Path, default=Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql")
    parser.add_argument("--reference-dir", required=True, type=Path)
    parser.add_argument("--reference-override", type=Path, help="independently generated query/values reference JSON")
    parser.add_argument("--allow-descriptor-baseline", action="store_true", help="measure old Q20 descriptor path; marks full result validation false")
    parser.add_argument("--build-commit", required=True)
    parser.add_argument("--memory-gb", type=int, default=24)
    parser.add_argument("--max-parallelism", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    if args.memory_gb <= 0 or args.max_parallelism <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("memory, parallelism and timeout must be positive")
    root = require_local_path(args.uat_root, Path.home(), os.sys.platform)
    source = require_local_path(args.input, Path.home(), os.sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    logs = root / "logs" / f"full43_{stamp}"

    def guard():
        check_budgets(root, source, logs, min_free_bytes=12 * GIB, reserve_bytes=0,
                      max_workspace_bytes=100 * GIB, max_log_bytes=256 * MIB)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    lock.mkdir()  # Deliberately shared with ingest: no overlapping large UAT.
    records = []
    summary = {}
    try:
        logs.mkdir(parents=True, exist_ok=False)
        text = "\n".join(line for line in args.queries.read_text().splitlines() if not line.lstrip().startswith("--"))
        queries = [query.strip() for query in text.split(";") if query.strip()]
        if len(queries) != 43:
            raise ValueError("expected the pinned 43-statement ClickBench query file")
        override = strict_json(args.reference_override.read_text()) if args.reference_override else None
        references = []
        for index in range(1, 44):
            if override and override["query"] == index:
                references.append(override["values"])
            else:
                references.append(extract_result(strict_json((args.reference_dir / f"q{index:02d}_run1.stdout.json").read_text())))
        identity = source.stat()
        summary = {
            "schema_version": "shardloom.clickbench.public_result_regression.v1",
            "build_commit": args.build_commit, "binary_sha256": file_sha256(args.binary),
            "queries_sha256": file_sha256(args.queries), "source": str(source),
            "source_bytes": identity.st_size, "platform": platform.platform(),
            "source_generation": {"device": identity.st_dev, "inode": identity.st_ino,
                                  "size_bytes": identity.st_size, "mtime_ns": identity.st_mtime_ns,
                                  "ctime_ns": identity.st_ctime_ns},
            "machine": platform.machine(), "cpu_count": os.cpu_count(),
            "memory_gb": args.memory_gb, "max_parallelism": args.max_parallelism,
            "timing_boundary": "process creation through completed public CLI output and process exit",
            "cache_policy": "new_process_per_run_os_page_cache_uncontrolled_no_answer_cache",
            "reference": str(args.reference_dir),
            "reference_override": override,
            "correctness_boundary": "complete returned values compared with retained ShardLoom outputs; not an independent oracle",
            "records": records,
        }
        for index, (query, expected) in enumerate(zip(queries, references), 1):
            for run in range(1, 4):
                prefix = logs / f"q{index:02d}_run{run}"
                command = [str(args.binary), "run", "sql", "--input", str(source), "--input-format", "vortex", "--sql", query,
                           "--request", "collect", "--bounded", "true", "--memory-gb", str(args.memory_gb),
                           "--max-parallelism", str(args.max_parallelism), "--format", "json"]
                result = run_command(command, prefix.with_suffix(".stdout.json"), prefix.with_suffix(".stderr.txt"), args.timeout, guard)
                result.update(query=index, run=run, passed=False)
                try:
                    if result["returncode"] != 0 or result["guard_failures"]:
                        raise ValueError("native command or watchdog failed")
                    envelope = strict_json(prefix.with_suffix(".stdout.json").read_text())
                    validation = "complete_values"
                    try:
                        actual = extract_result(envelope)
                    except ValueError:
                        if not (args.allow_descriptor_baseline and index == 20):
                            raise
                        match = re.search(r"^result summary: projected_columns=UserID rows=(\d+)$", envelope.get("human_text", ""), re.MULTILINE)
                        if not match or int(match[1]) != len(expected):
                            raise ValueError("legacy descriptor disagrees with independent row count") from None
                        # Validate success/no-fallback using the same fail-closed parser.
                        checked = dict(envelope, human_text="value summary: " + match[1])
                        extract_result(checked)
                        actual = expected
                        validation = "descriptor_count_only_not_returned_values"
                    if not equivalent(actual, expected):
                        raise ValueError("complete result differs from retained reference")
                    if source.stat() != identity:
                        # Access time is not a generation marker.
                        current = source.stat()
                        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns) != (identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns, identity.st_ctime_ns):
                            raise ValueError("source changed during UAT")
                    result["result_sha256"] = hashlib.sha256(json.dumps(actual, sort_keys=True, allow_nan=False).encode()).hexdigest()
                    result["passed"] = True
                    result["validation"] = validation
                except (OSError, ValueError) as error:
                    result["failure"] = str(error)
                records.append(result)
                summary.update(score(records, 43))
                summary["full_result_validation"] = len(records) == 129 and all(record.get("validation") == "complete_values" for record in records)
                (logs / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
                print(json.dumps(result), flush=True)
                if not result["passed"]:
                    return 1
        return 0
    finally:
        lock.rmdir()
        print(f"UAT evidence: {logs}", flush=True)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
