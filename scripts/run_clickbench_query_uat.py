#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Guarded public-CLI ClickBench runs with complete result regression checks.

This harness does not call an external engine. A retained-output comparison is
regression evidence, not an independent correctness oracle or an official rank.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import threading
import time

from local_uat_storage import GIB, MIB, StorageGuardError, check_budgets, require_local_path
from native_workflow_protocol import extract_result, public_workflow_command, report_fields, strict_json
from clickbench_reference_packet import load_reference_packet, query_statements


def extract_reference_result(envelope: dict):
    """Read immutable historical comparison evidence, never current executions.

    Earlier accepted reference packets stored complete values in diagnostic text.
    Their offline reader remains separate from the strict live result protocol.
    Descriptor-only references are insufficient for complete-value comparison.
    """
    entries = envelope.get("fields")
    if not isinstance(entries, list):
        raise ValueError("historical report fields must be an array")
    fields = {}
    for field in entries:
        if not isinstance(field, dict) or not isinstance(field.get("key"), str):
            raise ValueError("invalid historical report field")
        key, value = field["key"], field.get("value")
        if key in fields and (type(fields[key]) is not type(value) or fields[key] != value):
            raise ValueError(f"conflicting historical report field: {key}")
        fields[key] = value
    if any(key in fields for key in ("result_jsonl", "result_values_json", "result_payload_complete")):
        return extract_result(envelope)
    # Older immutable packets repeat identical certificate fields. Coalesce only
    # that historical representation; live responses retain unique-field checks.
    report_fields(envelope | {"fields": [{"key": key, "value": value}
                                        for key, value in fields.items()]})
    known = fields.get("result_known")
    if (not any(key in fields for key in ("result_schema_json", "result_schema_format"))
            and (known is True or known == "true") and "count" in fields):
        count = fields["count"]
        if type(count) not in (int, str) or not str(count).isascii() or not str(count).isdecimal():
            raise ValueError("invalid historical native count")
        return [{"count_all": int(count)}]
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
            return [{"count_all": count}]
        raise ValueError("result payload contains no complete values")
    result = strict_json(summary)
    if type(result) is not int or result < 0:
        raise ValueError("unsupported scalar result")
    return [{"count_all": result}]


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


def clickbench_harness_paths() -> tuple[Path, ...]:
    script = Path(__file__).resolve()
    scripts = tuple(
        script.with_name(name)
        for name in (
            script.name,
            "clickbench_reference_packet.py",
            "native_workflow_protocol.py",
            "timed_native_command.py",
            "local_uat_storage.py",
        )
    )
    package_sources = tuple(sorted(
        (script.parents[1] / "python" / "src" / "shardloom").rglob("*.py")
    ))
    return scripts + package_sources


def correctness_boundary(reference_kind: str, *, reference_override: bool = False) -> str:
    if reference_kind == "independent_reference":
        return (
            "complete returned values compared with an explicitly frozen independent reference; "
            "finite floats use 1e-12 tolerance"
        )
    if reference_override:
        return (
            "complete returned values compared with retained native regression outputs, with an "
            "optional query-specific reference override; this is not a full independent oracle"
        )
    return (
        "complete returned values compared with retained native regression outputs; "
        "finite floats use 1e-12 tolerance; not an independent oracle"
    )


def read_json_log(path: Path):
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        raw = gzip.decompress(path.with_suffix(path.suffix + ".gz").read_bytes())
    return strict_json(raw.decode("utf-8"))


def compress_completed_log(path: Path, guard) -> dict:
    """Archive a completed owned log after its timed operation and validation.

    Exclusive creation and a byte-for-byte readback preserve evidence before
    removing the uncompressed copy. This never raises the storage guard limits.
    """
    raw = path.read_bytes()
    compressed = gzip.compress(raw, mtime=0)
    target = path.with_suffix(path.suffix + ".gz")
    block_bytes = max(4096, os.statvfs(path.parent).f_frsize)
    reserved_bytes = math.ceil(len(compressed) / block_bytes) * block_bytes
    guard(reserved_bytes)
    with target.open("xb") as stream:
        stream.write(compressed)
    if gzip.decompress(target.read_bytes()) != raw:
        raise ValueError("completed log archive differs from original bytes")
    guard(0)
    evidence = {"path": str(target), "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "gzip_sha256": file_sha256(target), "raw_bytes": len(raw),
                "gzip_bytes": target.stat().st_size}
    path.unlink()
    guard(0)
    return evidence


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


def run_profiled_command(command: list[str], prefix: Path, timeout: float, guard) -> dict:
    """Use one supervisor per operation so CPU/RSS never include earlier queries."""
    timing = prefix.with_suffix(".timing.json")
    supervisor = [sys.executable, str(Path(__file__).with_name("timed_native_command.py")),
                  "--timing", str(timing), "--pid-file", str(prefix.with_suffix(".pid")),
                  "--shared-process-group", "--", *command]
    result = run_command(supervisor, prefix.with_suffix(".stdout.json"),
                         prefix.with_suffix(".stderr.txt"), timeout, guard)
    result["supervised_wall_seconds"] = result["seconds"]
    if timing.exists():
        native = strict_json(timing.read_text())
        result["seconds"] = native["seconds"]
        result["native_peak_rss_bytes"] = native["peak_rss_bytes"]
        result["user_cpu_seconds"] = native["user_cpu_seconds"]
        result["system_cpu_seconds"] = native["system_cpu_seconds"]
        for key in ("minor_page_faults", "major_page_faults", "input_block_operations",
                    "output_block_operations", "os_counter_scope"):
            if key in native:
                result[key] = native[key]
        if native["returncode"] != 0:
            result["guard_failures"].append("profiled native command failed")
    else:
        result["guard_failures"].append("native timing evidence is missing")
    return result


def score(records: list[dict], query_count: int, selected: list[int] | None = None) -> dict:
    queries = list(range(1, query_count + 1)) if selected is None else selected
    if not queries or len(set(queries)) != len(queries) or any(q < 1 or q > query_count for q in queries):
        raise ValueError("invalid selected query ids")
    complete = (len(records) == len(queries) * 3
                and all(record["passed"] for record in records)
                and {(record["query"], record["run"]) for record in records}
                == {(query, run) for query in queries for run in range(1, 4)})
    if not complete:
        return {"complete": False, "runs_completed": len(records), "runs_passed": sum(record["passed"] for record in records)}
    timings = [[record["seconds"] for record in sorted(records, key=lambda r: r["run"]) if record["query"] == query] for query in queries]
    best = [min(runs) for runs in timings]
    hot = [min(runs[1:]) for runs in timings]
    return {
        "complete": True, "queries_passed": len(queries), "query_ids": queries,
        "runs_completed": len(records), "runs_passed": len(records),
        "query_total_seconds": sum(best), "hot_total_seconds": sum(hot),
        "all_raw_run_seconds": sum(sum(runs) for runs in timings),
        "geomean_seconds": math.exp(sum(math.log(value) for value in best) / len(queries)),
        "hot_geomean_seconds": math.exp(sum(math.log(value) for value in hot) / len(queries)),
        "query_runs": timings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--uat-root", required=True, type=Path)
    parser.add_argument("--queries", type=Path, default=Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql")
    reference_group = parser.add_mutually_exclusive_group(required=True)
    reference_group.add_argument("--reference-dir", type=Path)
    reference_group.add_argument("--reference-packet", type=Path)
    parser.add_argument("--reference-override", type=Path, help="independently generated query/values reference JSON")
    parser.add_argument("--build-commit", required=True)
    parser.add_argument("--memory-gb", type=int, default=24)
    parser.add_argument("--max-parallelism", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--query-ids", help="comma-separated targeted query ids; never a full-suite score")
    parser.add_argument("--compress-logs", action="store_true", help="losslessly archive each completed stdout/stderr after validation, within the unchanged log budget")
    args = parser.parse_args()
    if args.reference_override is not None and args.reference_packet is not None:
        parser.error("--reference-override can only be used with --reference-dir")
    if args.memory_gb <= 0 or args.max_parallelism <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("memory, parallelism and timeout must be positive")
    try:
        selected = [int(value) for value in args.query_ids.split(",")] if args.query_ids else list(range(1, 44))
        score([], 43, selected)
    except ValueError as error:
        parser.error(str(error))
    root = require_local_path(args.uat_root, Path.home(), os.sys.platform)
    source = require_local_path(args.input, Path.home(), os.sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    logs = root / "logs" / f"full43_{stamp}"

    def guard(reserve_log_bytes=0):
        check_budgets(root, source, logs, min_free_bytes=12 * GIB, reserve_bytes=reserve_log_bytes,
                      max_workspace_bytes=100 * GIB, max_log_bytes=256 * MIB - reserve_log_bytes)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    lock.mkdir()  # Deliberately shared with ingest: no overlapping large UAT.
    records = []
    summary = {}
    try:
        logs.mkdir(parents=True, exist_ok=False)
        packet_receipt = None
        override = None
        if args.reference_packet is not None:
            loaded_packet = load_reference_packet(args.reference_packet, args.queries)
            queries = loaded_packet["queries"]
            queries_sha256 = loaded_packet["packet"]["queries_sha256"]
            references = [loaded_packet["values"][index] for index in range(1, 44)]
            packet_receipt = loaded_packet["packet"]
        else:
            query_bytes, queries = query_statements(args.queries)
            queries_sha256 = hashlib.sha256(query_bytes).hexdigest()
            override = strict_json(args.reference_override.read_text()) if args.reference_override else None
            references = []
            for index in range(1, 44):
                if override and override["query"] == index:
                    references.append(override["values"])
                else:
                    references.append(extract_reference_result(read_json_log(args.reference_dir / f"q{index:02d}_run1.stdout.json")))
        identity = source.stat()
        harness = {str(path): file_sha256(path) for path in clickbench_harness_paths()}
        reference_kind = (
            packet_receipt["reference_kind"]
            if packet_receipt is not None else "retained_native_regression"
        )
        summary_path = logs / "summary.json"

        def save_summary() -> None:
            summary_path.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")

        summary = {
            "schema_version": "shardloom.clickbench.public_result_regression.v1",
            "build_commit": args.build_commit, "binary_sha256": file_sha256(args.binary),
            "queries_sha256": queries_sha256, "source": str(source),
            "source_bytes": identity.st_size, "platform": platform.platform(),
            "source_generation": {"device": identity.st_dev, "inode": identity.st_ino,
                                  "size_bytes": identity.st_size, "mtime_ns": identity.st_mtime_ns,
                                  "ctime_ns": identity.st_ctime_ns},
            "machine": platform.machine(), "cpu_count": os.cpu_count(),
            "memory_gb": args.memory_gb, "max_parallelism": args.max_parallelism,
            "timing_boundary": "native process creation through completed public CLI output and process exit",
            "cpu_timing_boundary": "native child CPU work; overlaps wall time and other worker CPU",
            "selected_query_ids": selected,
            "score_scope": "full_43" if selected == list(range(1, 44)) else "targeted_queries_only",
            "cache_policy": "new_process_per_run_os_page_cache_uncontrolled_no_answer_cache",
            "reference": str(args.reference_dir) if args.reference_dir is not None else None,
            "reference_packet": packet_receipt,
            "reference_override": override,
            "reference_kind": reference_kind,
            "correctness_boundary": correctness_boundary(
                reference_kind, reference_override=override is not None
            ),
            "harness_sha256": harness,
            "completed_identity_check": False,
            "reference_packet_identity_verified": False if packet_receipt is not None else None,
            "harness_identity_verified": False,
            "records": records,
        }
        for index, (query, expected) in enumerate(zip(queries, references), 1):
            if index not in selected:
                continue
            for run in range(1, 4):
                prefix = logs / f"q{index:02d}_run{run}"
                command = public_workflow_command(
                    args.binary, query, input_path=source, input_format="vortex",
                    memory_gb=args.memory_gb, max_parallelism=args.max_parallelism,
                )
                result = run_profiled_command(command, prefix, args.timeout, guard)
                result.update(query=index, run=run, command=command, passed=False)
                validation_started = time.perf_counter()
                try:
                    if result["returncode"] != 0 or result["guard_failures"]:
                        raise ValueError("native command or watchdog failed")
                    envelope = strict_json(prefix.with_suffix(".stdout.json").read_text())
                    actual = extract_result(envelope)
                    if not equivalent(actual, expected):
                        raise ValueError("complete result differs from retained reference")
                    if source.stat() != identity:
                        # Access time is not a generation marker.
                        current = source.stat()
                        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns) != (identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns, identity.st_ctime_ns):
                            raise ValueError("source changed during UAT")
                    result["result_sha256"] = hashlib.sha256(json.dumps(actual, sort_keys=True, allow_nan=False).encode()).hexdigest()
                    result["passed"] = True
                    result["validation"] = "complete_values"
                except (OSError, ValueError) as error:
                    result["failure"] = str(error)
                result["validation_seconds"] = time.perf_counter() - validation_started
                result["stdout_bytes"] = prefix.with_suffix(".stdout.json").stat().st_size
                result["stderr_bytes"] = prefix.with_suffix(".stderr.txt").stat().st_size
                if args.compress_logs:
                    result["completed_log_archives"] = [
                        compress_completed_log(prefix.with_suffix(suffix), guard)
                        for suffix in (".stdout.json", ".stderr.txt")
                        if prefix.with_suffix(suffix).stat().st_size
                    ]
                records.append(result)
                summary.update(score(records, 43, selected))
                summary["full_result_validation"] = len(records) == 129 and all(record.get("validation") == "complete_values" for record in records)
                save_summary()
                guard()
                print(json.dumps(result), flush=True)
                if not result["passed"]:
                    return 1
        identity_errors: list[str] = []

        def verify_identity(label: str, path: Path, expected_sha256: str) -> bool:
            try:
                observed = file_sha256(path)
            except (OSError, ValueError) as error:
                identity_errors.append(f"{label} identity could not be verified: {error}")
                return False
            if observed != expected_sha256:
                identity_errors.append(f"{label} changed during UAT")
                return False
            return True

        binary_unchanged = verify_identity("binary", args.binary, summary["binary_sha256"])
        queries_unchanged = verify_identity("query file", args.queries, summary["queries_sha256"])
        packet_unchanged = None
        if packet_receipt is not None:
            packet_unchanged = verify_identity(
                "reference packet", Path(packet_receipt["path"]), packet_receipt["sha256"]
            )
        harness_checks = [
            verify_identity("harness file", Path(path), digest)
            for path, digest in harness.items()
        ]
        harness_unchanged = all(harness_checks)
        identity_verified = (
            binary_unchanged and queries_unchanged
            and (packet_unchanged is not False) and harness_unchanged
        )
        summary["reference_packet_identity_verified"] = packet_unchanged
        summary["harness_identity_verified"] = harness_unchanged
        summary["completed_identity_check"] = identity_verified
        if not identity_verified:
            summary.update(
                complete=False,
                full_result_validation=False,
                failure="identity verification failed: " + "; ".join(identity_errors),
            )
            save_summary()
            return 1
        save_summary()
        return 0
    finally:
        lock.rmdir()
        print(f"UAT evidence: {logs}", flush=True)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
