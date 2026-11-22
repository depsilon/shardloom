#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Guarded direct-Parquet ClickBench runs with complete result checks.

This harness does not call an external engine. A retained-output comparison is
regression evidence, not an independent correctness oracle or an official rank.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import subprocess
import sys
import threading
import time

from local_uat_storage import (
    GIB, MIB, StorageGuardError, add_log_budget_arguments, check_budgets, require_local_path,
)
from native_workflow_protocol import extract_result, public_workflow_command, report_fields, strict_json
from clickbench_reference_packet import load_reference_packet, query_statements
from clickbench_native_sql import native_query_statements
from query_timing_report import query_phase_timings, timing_totals, write_timing_csv


_EXECUTION_REPORT_PREFIXES = (
    "local_primitive_", "native_source_demand_", "native_source_ranked_",
    "native_source_metadata_count_", "native_input_", "resident_", "relational_",
)
_EXECUTION_REPORT_EXACT_KEYS = {
    "fallback_attempted",
    "external_engine_invoked",
    "public_workflow_fallback_attempted",
    "public_workflow_external_engine_invoked",
    "execution_backend",
    "execution_mode",
    "native_input_end_observed",
    "native_input_output_ownership_detached",
}


def execution_report(fields: dict) -> dict:
    """Retain selected execution evidence without result payloads or inference."""
    return {
        key: value
        for key, value in fields.items()
        if key.startswith(_EXECUTION_REPORT_PREFIXES) or key in _EXECUTION_REPORT_EXACT_KEYS
    }


def validate_input_execution(fields: dict, input_format: str) -> None:
    """Require completed direct-source evidence for the ordinary Parquet run."""
    if input_format != "parquet":
        return

    def number(key):
        value = fields.get(key)
        if (type(value) not in (int, str) or re.fullmatch(r"[0-9]+", str(value)) is None
                or int(value) > (1 << 64) - 1):
            raise ValueError("invalid direct-Parquet execution counter: " + key)
        return int(value)

    sources = number("native_source_demand_sources")
    if (sources == 0 or number("native_source_demand_parquet_sources") != sources
            or number("native_source_demand_files") < sources
            or fields.get("native_source_demand_formats") != "parquet"):
        raise ValueError("direct-Parquet source execution was not established")
    if number("native_source_demand_prepared_input_bytes_written") != 0:
        raise ValueError("direct-Parquet execution wrote prepared input")
    for key in ("native_input_end_observed", "native_input_output_ownership_detached"):
        if fields.get(key) is not True and fields.get(key) != "true":
            raise ValueError("direct-Parquet source lifecycle incomplete: " + key)


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
            "clickbench_native_sql.py",
            "native_workflow_protocol.py",
            "timed_native_command.py",
            "query_timing_report.py",
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
        result["supervision_overhead_seconds"] = max(0.0, result["supervised_wall_seconds"] - native["seconds"])
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


def score(records: list[dict], query_count: int, selected: list[int] | None = None,
          *, repeats: int = 1, timing_key: str = "seconds") -> dict:
    """Score one explicit clock; the legacy helper default remains process time."""
    boundaries = {
        "seconds": ("process_wall_seconds", "sum_of_best_complete_process_wall_times_not_engine_execution"),
        "engine_execution_seconds": (
            "engine_execution_seconds",
            "sum_of_best_prepared_native_execution_times_excluding_result_serialization",
        ),
    }
    if timing_key not in boundaries:
        raise ValueError("unsupported score timing key")
    metric, boundary = boundaries[timing_key]
    if type(repeats) is not int or repeats <= 0:
        raise ValueError("repeats must be a positive integer")
    queries = list(range(1, query_count + 1)) if selected is None else selected
    if not queries or len(set(queries)) != len(queries) or any(q < 1 or q > query_count for q in queries):
        raise ValueError("invalid selected query ids")
    complete = (len(records) == len(queries) * repeats
                and all(record["passed"] for record in records)
                and {(record["query"], record["run"]) for record in records}
                == {(query, run) for query in queries for run in range(1, repeats + 1)})
    result = {
        "complete": complete, "runs_completed": len(records),
        "runs_passed": sum(record["passed"] for record in records),
        "timing_metric": metric, "timing_available": False,
        "timing_unavailable_reason": "incomplete_results",
        "query_total_timing_boundary": boundary,
        "query_total_seconds": None, "hot_total_seconds": None,
        "all_raw_run_seconds": None, "geomean_seconds": None,
        "hot_geomean_seconds": None, "query_runs": None,
    }
    if not complete:
        return result
    timings = [[record.get(timing_key) for record in sorted(records, key=lambda r: r["run"])
                if record["query"] == query] for query in queries]
    values = [value for runs in timings for value in runs]
    if any(value is not None and (type(value) not in (int, float)
                                 or not math.isfinite(value) or value < 0) for value in values):
        raise ValueError(f"{metric} must contain finite nonnegative measurements")
    result.update(queries_passed=len(queries), query_ids=queries, query_runs=timings)
    if any(value is None for value in values):
        result["timing_unavailable_reason"] = "missing_measurements"
        return result

    def geomean(values):
        return 0.0 if 0 in values else math.exp(sum(math.log(value) for value in values) / len(values))

    best = [min(runs) for runs in timings]
    hot = [min(runs[1:]) for runs in timings] if repeats > 1 else None
    return {
        **result, "timing_available": True, "timing_unavailable_reason": None,
        "query_total_seconds": sum(best), "hot_total_seconds": sum(hot) if hot is not None else None,
        "all_raw_run_seconds": sum(sum(runs) for runs in timings),
        "geomean_seconds": geomean(best),
        "hot_geomean_seconds": geomean(hot) if hot is not None else None,
    }


def verify_zero_answer_cache(fields: dict) -> None:
    """Reject any query-answer cache reuse so warm timing reflects real execution."""
    for key in (
        "query_answer_cache_hit",
        "answer_cache_hit",
        "cached_result_reused",
        "resident_answer_cache_hit",
    ):
        value = fields.get(key)
        if value is True or (isinstance(value, str) and value.strip().lower() == "true"):
            raise ValueError(f"forbidden query answer cache hit reported via {key}")


class PersistentResidentWorker:
    """Long-lived worker client over newline-delimited JSON stdin/stdout."""

    def __init__(self, binary: Path, stderr_path: Path):
        self.binary = Path(binary)
        self.stderr_path = Path(stderr_path)
        self.error_file = self.stderr_path.open("xb")
        started = time.perf_counter()
        try:
            self.process = subprocess.Popen(
                [str(self.binary), "python-worker"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.error_file,
                start_new_session=True,
                bufsize=0,
            )
        except BaseException:
            self.error_file.close()
            raise
        self.start_seconds = time.perf_counter() - started
        self.buffer = b""

    def run_query(self, command: list[str], stdout_path: Path, timeout: float, guard) -> dict:
        started = time.perf_counter()
        failures: list[str] = []
        worker_args = (
            command[1:] if command and Path(command[0]).name == self.binary.name else list(command)
        )
        payload = (json.dumps({"args": worker_args}, separators=(",", ":")) + "\n").encode("utf-8")
        if len(payload) > 4096:
            raise ValueError("bounded persistent worker request exceeded 4096 bytes")
        assert self.process.stdin is not None and self.process.stdout is not None
        self.process.stdin.write(payload)
        self.process.stdin.flush()
        deadline = started + timeout
        next_guard = started + 0.25
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            while b"\n" not in self.buffer:
                now = time.perf_counter()
                if now >= deadline:
                    failures.append("persistent resident worker timeout")
                    stop_process(self.process)
                    break
                if len(self.buffer) > 8 * MIB or self.stderr_path.stat().st_size > 8 * MIB:
                    failures.append("persistent resident worker output exceeded 8 MiB")
                    stop_process(self.process)
                    break
                if now >= next_guard:
                    guard()
                    next_guard = time.perf_counter() + 0.25
                if not selector.select(min(0.1, max(0.0, deadline - now))):
                    continue
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    failures.append("persistent resident worker exited before completing response")
                    break
                self.buffer += chunk
        if b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            elapsed = time.perf_counter() - started
            if len(line) > 8 * MIB or self.buffer:
                failures.append("persistent resident worker emitted oversized or extra output")
            stdout_path.write_bytes(line + b"\n")
            guard()
            return {
                "returncode": 0 if not failures else 1,
                "seconds": elapsed,
                "guard_failures": failures,
            }
        elapsed = time.perf_counter() - started
        stdout_path.write_bytes(self.buffer)
        guard()
        return {"returncode": 1, "seconds": elapsed, "guard_failures": failures}

    def close(self) -> None:
        stop_process(self.process)
        for pipe in (self.process.stdin, self.process.stdout):
            if pipe is not None:
                pipe.close()
        self.error_file.close()


def _warm_surface_name(concurrent_workers: int) -> str:
    return "warm_resident_concurrent" if concurrent_workers > 1 else "warm_resident_session"


def _source_generation_dict(stat_result: os.stat_result) -> dict[str, int]:
    return {
        "device": stat_result.st_dev,
        "inode": stat_result.st_ino,
        "size_bytes": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
        "ctime_ns": stat_result.st_ctime_ns,
    }


def _write_summary_checked(summary: dict, summary_path: Path, guard) -> None:
    summary_path.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    try:
        guard()
    except StorageGuardError as error:
        reason = str(error)
        summary.update(
            complete=False,
            full_result_validation=False,
            status="failed",
            storage_error=reason,
            failure="storage budget rejected: " + reason,
        )
        summary_path.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
        raise


def _save_lane_summary(summary: dict, records: list[dict], summary_path: Path, logs: Path, guard) -> None:
    summary["timing_totals"] = timing_totals(records)
    summary["timing_csv"] = str(logs / "query-timings.csv")
    write_timing_csv(logs / "query-timings.csv", records)
    _write_summary_checked(summary, summary_path, guard)


def _execute_lane(
    *,
    lane_name: str,
    concurrent_workers: int,
    args,
    selected: list[int],
    queries: list[str],
    submitted_queries: list[str],
    references: list,
    queries_sha256: str,
    query_dialect: str,
    source: Path,
    identity: os.stat_result,
    logs: Path,
    packet_receipt,
    override,
    harness: dict[str, str],
    reference_kind: str,
    guard,
) -> dict:
    logs.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []
    is_cold = lane_name == "cold_process_cli"
    surface_name = "cold_process_cli" if is_cold else _warm_surface_name(concurrent_workers)
    cache_policy = (
        "new_process_per_run_os_page_cache_uncontrolled_no_answer_cache"
        if is_cold
        else "persistent_resident_worker_shared_session_no_answer_cache"
    )
    timing_boundary = (
        "native process creation through completed public CLI output and process exit"
        if is_cold
        else "resident worker JSON request through completed response line; worker startup measured separately"
    )
    summary = {
        "schema_version": "shardloom.clickbench.public_result_regression.v2",
        "build_commit": args.build_commit,
        "binary_sha256": file_sha256(args.binary),
        "queries_sha256": queries_sha256,
        "source": str(source),
        "input_format": args.input_format,
        "input_execution_contract": (
            "direct_original_parquet_no_preparation"
            if args.input_format == "parquet"
            else "existing_native_vortex_input"
        ),
        "query_dialect": query_dialect,
        "source_bytes": identity.st_size,
        "platform": platform.platform(),
        "source_generation": _source_generation_dict(identity),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "memory_gb": args.memory_gb,
        "max_parallelism": args.max_parallelism,
        "timing_surface": surface_name,
        "concurrent_workers": 1 if is_cold else concurrent_workers,
        "worker_startup_seconds": [],
        "zero_answer_cache_verified": False,
        "timing_boundary": timing_boundary,
        "engine_timing_required": not args.allow_missing_engine_timing,
        "cpu_timing_boundary": "native child CPU work; overlaps wall time and other worker CPU",
        "selected_query_ids": selected,
        "repeats": args.repeats,
        "score_scope": "full_43" if selected == list(range(1, 44)) else "targeted_queries_only",
        "cache_policy": cache_policy,
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
    summary_path = logs / "summary.json"
    tasks = [
        (index, query, expected, run)
        for index, (query, expected) in enumerate(zip(queries, references), 1)
        if index in selected
        for run in range(1, args.repeats + 1)
    ]

    state_lock = threading.Lock()
    stop_event = threading.Event()

    def process_result(index: int, run: int, query: str, expected, command: list[str], prefix: Path, result: dict, worker_id: int | None) -> bool:
        result.update(
            query=index,
            run=run,
            command=command,
            query_statement=query,
            executed_query_statement=submitted_queries[index - 1],
            timing_surface=surface_name,
            worker_id=worker_id,
            zero_answer_cache_verified=False,
            passed=False,
        )
        validation_started = time.perf_counter()
        try:
            if result["returncode"] != 0 or result["guard_failures"]:
                raise ValueError("native command or watchdog failed")
            envelope = strict_json(prefix.with_suffix(".stdout.json").read_text())
            fields = report_fields(envelope)
            verify_zero_answer_cache(fields)
            result["zero_answer_cache_verified"] = True
            result["execution_report"] = execution_report(fields)
            validate_input_execution(fields, args.input_format)
            result.update(
                query_phase_timings(
                    fields,
                    result["seconds"],
                    allow_missing=args.allow_missing_engine_timing,
                )
            )
            actual = extract_result(envelope)
            if not equivalent(actual, expected):
                raise ValueError("complete result differs from retained reference")
            if source.stat() != identity:
                current = source.stat()
                if _source_generation_dict(current) != _source_generation_dict(identity):
                    raise ValueError("source changed during UAT")
            result["result_sha256"] = hashlib.sha256(
                json.dumps(actual, sort_keys=True, allow_nan=False).encode()
            ).hexdigest()
            result["passed"] = True
            result["validation"] = "complete_values"
        except (OSError, ValueError) as error:
            result["failure"] = str(error)
        result["validation_seconds"] = time.perf_counter() - validation_started
        stdout_path = prefix.with_suffix(".stdout.json")
        stderr_path = prefix.with_suffix(".stderr.txt")
        result["stdout_bytes"] = stdout_path.stat().st_size if stdout_path.exists() else 0
        result["stderr_bytes"] = stderr_path.stat().st_size if stderr_path.exists() else 0
        with state_lock:
            if args.compress_logs:
                result["completed_log_archives"] = [
                    compress_completed_log(prefix.with_suffix(suffix), guard)
                    for suffix in (".stdout.json", ".stderr.txt")
                    if prefix.with_suffix(suffix).exists() and prefix.with_suffix(suffix).stat().st_size
                ]
            records.append(result)
            summary.update(
                score(records, 43, selected, repeats=args.repeats, timing_key="engine_execution_seconds")
            )
            summary["secondary_process_wall"] = score(records, 43, selected, repeats=args.repeats)
            summary["zero_answer_cache_verified"] = bool(records) and all(
                r.get("zero_answer_cache_verified") for r in records
            )
            summary["full_result_validation"] = (
                summary["complete"]
                and summary["zero_answer_cache_verified"]
                and selected == list(range(1, 44))
                and all(record.get("validation") == "complete_values" for record in records)
            )
            _save_lane_summary(summary, records, summary_path, logs, guard)
        print(json.dumps(result), flush=True)
        return bool(result["passed"])

    if is_cold:
        for index, query, expected, run in tasks:
            prefix = logs / f"q{index:02d}_run{run}"
            command = public_workflow_command(
                args.binary,
                submitted_queries[index - 1],
                input_path=source,
                input_format=args.input_format,
                memory_gb=args.memory_gb,
                max_parallelism=args.max_parallelism,
            )
            result = run_profiled_command(command, prefix, args.timeout, guard)
            if not process_result(index, run, query, expected, command, prefix, result, None):
                return summary
    else:
        workers: list[PersistentResidentWorker] = []
        try:
            for worker_idx in range(concurrent_workers):
                worker = PersistentResidentWorker(
                    args.binary,
                    logs / f"{surface_name}_worker_{worker_idx}.stderr.txt",
                )
                workers.append(worker)
            summary["worker_startup_seconds"] = [worker.start_seconds for worker in workers]
            buckets: list[list[tuple[int, str, object, int]]] = [[] for _ in range(concurrent_workers)]
            for task_idx, task in enumerate(tasks):
                buckets[task_idx % concurrent_workers].append(task)

            def run_bucket(worker_idx: int) -> bool:
                worker = workers[worker_idx]
                for index, query, expected, run in buckets[worker_idx]:
                    if stop_event.is_set():
                        return False
                    prefix = logs / f"q{index:02d}_run{run}"
                    command = public_workflow_command(
                        args.binary,
                        submitted_queries[index - 1],
                        input_path=source,
                        input_format=args.input_format,
                        memory_gb=args.memory_gb,
                        max_parallelism=args.max_parallelism,
                    )
                    prefix.with_suffix(".stderr.txt").write_text("")
                    res = worker.run_query(command, prefix.with_suffix(".stdout.json"), args.timeout, guard)
                    if not process_result(index, run, query, expected, command, prefix, res, worker_idx):
                        stop_event.set()
                        return False
                return True

            if concurrent_workers == 1:
                run_bucket(0)
            else:
                with ThreadPoolExecutor(max_workers=concurrent_workers) as pool:
                    futures = [pool.submit(run_bucket, idx) for idx in range(concurrent_workers)]
                    for future in futures:
                        future.result()
        finally:
            for worker in workers:
                worker.close()
        if not summary.get("complete"):
            return summary

    _save_lane_summary(summary, records, summary_path, logs, guard)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--input-format", choices=("parquet", "vortex"), default="parquet",
                        help="source format (default: parquet, read directly without ingestion)")
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
    parser.add_argument("--repeats", type=int, default=1,
                        help="executions per query (default: 1); repeated trials require an explicit value")
    parser.add_argument("--lane", choices=("cold_process_cli", "warm_resident_session", "paired"),
                        default="cold_process_cli",
                        help="execution lane: cold CLI spawns, warm persistent worker, or paired separate lanes")
    parser.add_argument("--concurrent-workers", type=int, default=1,
                        help="number of concurrent persistent workers on warm/paired lanes (default: 1)")
    parser.add_argument("--allow-missing-engine-timing", action="store_true",
                        help="historical binary comparison only; missing engine clocks remain unavailable")
    parser.add_argument("--compress-logs", action="store_true", help="losslessly archive each completed stdout/stderr after validation, within the unchanged log budget")
    add_log_budget_arguments(parser)
    args = parser.parse_args()
    if args.reference_override is not None and args.reference_packet is not None:
        parser.error("--reference-override can only be used with --reference-dir")
    if args.memory_gb <= 0 or args.max_parallelism <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("memory, parallelism and timeout must be positive")
    if args.concurrent_workers < 1:
        parser.error("--concurrent-workers must be positive")
    if args.lane == "cold_process_cli" and args.concurrent_workers > 1:
        parser.error("--concurrent-workers > 1 requires --lane warm_resident_session or --lane paired")
    try:
        selected = [int(value) for value in args.query_ids.split(",")] if args.query_ids else list(range(1, 44))
        score([], 43, selected, repeats=args.repeats)
    except ValueError as error:
        parser.error(str(error))
    root = require_local_path(args.uat_root, Path.home(), os.sys.platform)
    source = require_local_path(args.input, Path.home(), os.sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    logs = root / "logs" / f"full43_{stamp}"

    def guard(reserve_log_bytes=0):
        return check_budgets(
            root, source, logs, min_free_bytes=12 * GIB, reserve_bytes=reserve_log_bytes,
            max_workspace_bytes=100 * GIB, max_log_bytes=math.floor(args.max_log_mib * MIB),
            max_run_log_bytes=math.floor(args.max_run_log_mib * MIB),
            reserve_log_bytes=reserve_log_bytes,
        )

    storage_preflight = guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    lock.mkdir()  # Deliberately shared with ingest: no overlapping large UAT.
    summary: dict = {}
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
        submitted_queries, query_dialect = native_query_statements(
            queries, queries_sha256, input_format=args.input_format)
        identity = source.stat()
        harness = {str(path): file_sha256(path) for path in clickbench_harness_paths()}
        reference_kind = (
            packet_receipt["reference_kind"]
            if packet_receipt is not None else "retained_native_regression"
        )
        summary_path = logs / "summary.json"
        if args.lane == "paired":
            warm_name = _warm_surface_name(args.concurrent_workers)
            cold_summary = _execute_lane(
                lane_name="cold_process_cli",
                concurrent_workers=1,
                args=args,
                selected=selected,
                queries=queries,
                submitted_queries=submitted_queries,
                references=references,
                queries_sha256=queries_sha256,
                query_dialect=query_dialect,
                source=source,
                identity=identity,
                logs=logs / "cold_process_cli",
                packet_receipt=packet_receipt,
                override=override,
                harness=harness,
                reference_kind=reference_kind,
                guard=guard,
            )
            cold_summary["storage_preflight"] = storage_preflight
            cold_summary["log_budget_bytes"] = {
                "retained": math.floor(args.max_log_mib * MIB),
                "active_run": math.floor(args.max_run_log_mib * MIB),
            }
            warm_summary = _execute_lane(
                lane_name="warm_resident_session",
                concurrent_workers=args.concurrent_workers,
                args=args,
                selected=selected,
                queries=queries,
                submitted_queries=submitted_queries,
                references=references,
                queries_sha256=queries_sha256,
                query_dialect=query_dialect,
                source=source,
                identity=identity,
                logs=logs / warm_name,
                packet_receipt=packet_receipt,
                override=override,
                harness=harness,
                reference_kind=reference_kind,
                guard=guard,
            )
            warm_summary["storage_preflight"] = storage_preflight
            warm_summary["log_budget_bytes"] = cold_summary["log_budget_bytes"]
            summary = {
                "schema_version": "shardloom.clickbench.public_result_regression.v2",
                "build_commit": args.build_commit,
                "binary_sha256": cold_summary["binary_sha256"],
                "queries_sha256": queries_sha256,
                "source": str(source),
                "input_format": args.input_format,
                "lane": "paired",
                "timing_surface": "paired_separate_lanes",
                "cache_policy": "separate_cold_and_warm_lanes_no_answer_cache",
                "selected_query_ids": selected,
                "repeats": args.repeats,
                "concurrent_workers": args.concurrent_workers,
                "storage_preflight": storage_preflight,
                "log_budget_bytes": cold_summary["log_budget_bytes"],
                "complete": bool(cold_summary["complete"] and warm_summary["complete"]),
                "full_result_validation": bool(
                    cold_summary["full_result_validation"] and warm_summary["full_result_validation"]
                ),
                "zero_answer_cache_verified": bool(
                    cold_summary["zero_answer_cache_verified"] and warm_summary["zero_answer_cache_verified"]
                ),
                "timing_available": False,
                "timing_unavailable_reason": "see_separate_lane_scores",
                "lanes": {
                    "cold_process_cli": cold_summary,
                    warm_name: warm_summary,
                },
                "harness_sha256": harness,
                "completed_identity_check": False,
                "reference_packet_identity_verified": False if packet_receipt is not None else None,
                "harness_identity_verified": False,
            }
            if not summary["complete"]:
                _write_summary_checked(summary, summary_path, guard)
                return 1
        else:
            lane_summary = _execute_lane(
                lane_name=args.lane,
                concurrent_workers=args.concurrent_workers,
                args=args,
                selected=selected,
                queries=queries,
                submitted_queries=submitted_queries,
                references=references,
                queries_sha256=queries_sha256,
                query_dialect=query_dialect,
                source=source,
                identity=identity,
                logs=logs,
                packet_receipt=packet_receipt,
                override=override,
                harness=harness,
                reference_kind=reference_kind,
                guard=guard,
            )
            lane_summary["storage_preflight"] = storage_preflight
            lane_summary["log_budget_bytes"] = {
                "retained": math.floor(args.max_log_mib * MIB),
                "active_run": math.floor(args.max_run_log_mib * MIB),
            }
            summary = lane_summary
            summary["lanes"] = {lane_summary["timing_surface"]: dict(lane_summary)}
            if not summary.get("complete"):
                _write_summary_checked(summary, summary_path, guard)
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
        for target_summary in [summary, *summary.get("lanes", {}).values()]:
            target_summary["reference_packet_identity_verified"] = packet_unchanged
            target_summary["harness_identity_verified"] = harness_unchanged
            target_summary["completed_identity_check"] = identity_verified
        if not identity_verified:
            summary.update(
                complete=False,
                full_result_validation=False,
                failure="identity verification failed: " + "; ".join(identity_errors),
            )
            _write_summary_checked(summary, summary_path, guard)
            return 1
        _write_summary_checked(summary, summary_path, guard)
        return 0
    finally:
        lock.rmdir()
        print(f"UAT evidence: {logs}", flush=True)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
