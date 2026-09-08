#!/usr/bin/env python3
"""Root-run bounded three-surface experiment; no automatic build or package install.

Native Rust controls run sequentially, separately from the alternating worker /
in-process pairs. These controls do not establish cross-language speedup by
themselves. Every source value and actual native result is checked independently.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import time

from load_native import load, sha256

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import extract_result, run_command, strict_json
from run_resident_call_path_uat import (
    Worker, command_args, fixture_rows, generation, percentiles, validate,
    validate_candidate_count_where, validate_candidate_reuse, validate_preparation,
)


def verify(actual, expected):
    # Exact Python integer equality plus types: no float coercion above 2^53.
    if type(actual) is not type(expected):
        raise ValueError("native result type differs from independent oracle")
    if isinstance(expected, list):
        if len(actual) != len(expected):
            raise ValueError("native result row count differs")
        for got, want in zip(actual, expected, strict=True):
            verify(got, want)
    elif isinstance(expected, dict):
        if actual.keys() != expected.keys():
            raise ValueError("native result schema differs")
        for key in expected:
            verify(actual[key], expected[key])
    elif actual != expected:
        raise ValueError("native complete result differs")


def cases(rows):
    return [
        {"name": "metadata_count", "primitive": "count", "expected": len(rows)},
        {"name": "filtered_count", "primitive": "count_where", "predicate": "gte:cohort_key:24",
         "threshold": 24, "expected": sum(row["cohort_key"] >= 24 for row in rows)},
        {"name": "empty_filtered_count", "primitive": "count_where", "predicate": "gte:cohort_key:99",
         "threshold": 99, "expected": 0},
        {"name": "projection", "primitive": "project", "columns": ["nullable_label", "exact_identifier", "cohort_key"],
         "expected": rows},
    ]


def native_plan(session, source, case):
    if case["primitive"] == "count":
        return session.prepare_count(str(source))
    if case["primitive"] == "count_where":
        return session.prepare_count_where_i64(str(source), "cohort_key", "ge", case["threshold"])
    return session.prepare_projection(str(source), case["columns"], 32)


def native_call(plan, case):
    started = time.perf_counter_ns()
    if case["primitive"] != "project":
        actual = plan.execute_count()
        elapsed = time.perf_counter_ns() - started
        return actual, {"native_return_nanos": elapsed, "explicit_json_sink_nanos": 0,
                        "complete_python_return_nanos": elapsed, "result_drop_nanos": 0}
    batch = plan.execute_arrays()
    returned = time.perf_counter_ns() - started
    try:
        sink_started = time.perf_counter_ns()
        raw = batch.to_json()
        sink = time.perf_counter_ns() - sink_started
        complete = time.perf_counter_ns() - started
        # Parse and independent value verification are outside all reported native clocks.
        actual = strict_json(raw)
    finally:
        close_started = time.perf_counter_ns()
        batch.close()
        dropped = time.perf_counter_ns() - close_started
    return actual, {"native_return_nanos": returned, "explicit_json_sink_nanos": sink,
                    "complete_python_return_nanos": complete, "result_drop_nanos": dropped}


def run(args):
    output_root = require_local_path(args.uat_root, Path.home(), sys.platform)
    if not args.extension.is_absolute() or not args.cli.is_absolute() or not args.native_control.is_absolute():
        raise ValueError("all measured executable paths must be explicit and absolute")
    binaries = {"extension": args.extension.resolve(strict=True), "cli": args.cli.resolve(strict=True),
                "native_control": args.native_control.resolve(strict=True)}
    output = output_root / "logs" / ("native_python_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    source = output / "fixture.vortex"

    def guard():
        check_budgets(output_root, source, output, min_free_bytes=GIB, reserve_bytes=0,
                      max_workspace_bytes=100 * GIB, max_log_bytes=256 * MIB)
        if source.exists() and source.stat().st_size > 16 * MIB:
            raise ValueError("native binding fixture exceeds 16 MiB")

    guard()
    output_root.mkdir(parents=True, exist_ok=True)
    lock = output_root / ".ingest-uat.lock"
    lock.mkdir()
    summary = {"schema": "shardloom.native_python_experiment.v1", "status": "running",
               "samples": 30, "warmups": 1, "records": [], "preparation": [], "closes": [],
               "platform": platform.platform(), "python": sys.version,
               "binaries": {key: {"path": str(path), "sha256": sha256(path)} for key, path in binaries.items()},
               "resource_request": {"memory_gb": 1, "max_parallelism": 2},
               "scope": __doc__.strip(), "cache_scope": "uncontrolled OS cache; no query-answer cache",
               "clock_scope": {"native_rust": "direct native return, explicit JSON sink and result drop separately; export copy outside clocks",
                               "json_resident": "request JSON encoding through complete response bytes; JSON parsing outside clock",
                               "in_process": "CPython call through native owned array/scalar return; explicit native JSON plus Python Unicode copy separately"},
               "ownership_scope": "native owned-reservation counters only; Python objects, fixture values, and upstream uncharged allocations excluded; not RSS"}
    try:
        output.mkdir()
        rows = fixture_rows()
        raw_source = output / "fixture.jsonl"
        raw_source.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows))
        command = [str(binaries["cli"]), "prepare", "dataframe", "--input", str(raw_source),
                   "--input-format", "jsonl", "--output", str(source), "--memory-gb", "1",
                   "--max-parallelism", "2", "--format", "json"]
        prepared = run_command(command, output / "prepare.stdout.json", output / "prepare.stderr.txt", 60, guard)
        if prepared["returncode"] or prepared["guard_failures"]:
            raise ValueError("native fixture preparation failed")
        validate_preparation(strict_json((output / "prepare.stdout.json").read_text()))
        identity, source_hash = generation(source), sha256(source)
        summary["fixture"] = {"expected_rows": rows, "sha256": source_hash, "generation": identity,
                              "bytes": source.stat().st_size, "preparation": prepared,
                              "prepare_command": command, "input_sha256": sha256(raw_source)}
        imported = time.perf_counter_ns()
        native = load(binaries["extension"])
        summary["extension_import_nanos"] = time.perf_counter_ns() - imported
        summary["provider_version"] = native.PROVIDER_VERSION
        for case in cases(rows):
            guard()
            # A direct Rust control is preserved separately; no artificial pairing claim.
            prefix = output / (case["name"] + ".native")
            result = run_command([str(binaries["native_control"]), str(source), case["name"]],
                                 prefix.with_suffix(".stdout.jsonl"), prefix.with_suffix(".stderr.txt"), 60, guard)
            if result["returncode"] or result["guard_failures"]:
                raise ValueError("direct native control failed")
            raw = prefix.with_suffix(".stdout.jsonl").read_bytes()
            if len(raw) > 4 * MIB:
                raise ValueError("native control output exceeds 4 MiB")
            lines = [strict_json(line) for line in raw.splitlines()]
            controls = [line for line in lines if line.get("kind") == "sample"]
            closes = [line for line in lines if line.get("kind") == "close"]
            if len(closes) != 1 or closes[0]["native_owned_bytes"] != 0:
                raise ValueError("native control missing actual owner-release evidence")
            if len(controls) != 31 or [line["sample"] for line in controls] != list(range(31)):
                raise ValueError("direct native control sample matrix incomplete")
            for record in controls:
                verify(record["value"], case["expected"])
                if record["source_opens"] != 1 or record["completed_executions"] != record["sample"] + 1:
                    raise ValueError("native control did not retain actual prepared source")
                record["passed"] = True
                summary["records"].append(record)
            summary["preparation"].extend({"surface": "native_rust", **line} for line in lines if line["kind"] == "preparation")
            summary["closes"].extend({"surface": "native_rust", "case": case["name"], **line} for line in lines if line["kind"] == "close")
            with contextlib.ExitStack() as stack:
                worker = Worker(binaries["cli"], output / (case["name"] + ".worker.stderr.txt"))
                stack.callback(worker.close)
                started = time.perf_counter_ns()
                session = native.Session(memory_gb=1, max_parallelism=2)
                stack.callback(session.close)
                session_nanos = time.perf_counter_ns() - started
                started = time.perf_counter_ns()
                plan = native_plan(session, source, case)
                stack.callback(plan.close)
                summary["preparation"].append({"surface": "in_process", "case": case["name"],
                                               "session_nanos": session_nanos, "plan_nanos": time.perf_counter_ns() - started,
                                               "worker_spawn_seconds": worker.start_seconds,
                                               "worker_plan_preparation_scope": "included in warmup; no separate public prepare API"})
                for sample in range(31):
                    surfaces = ("in_process", "json_resident") if sample % 2 else ("json_resident", "in_process")
                    for surface in surfaces:
                        guard()
                        if generation(source) != identity:
                            raise ValueError("source generation changed")
                        record = {"surface": surface, "case": case["name"], "sample": sample,
                                  "warmup": sample == 0, "passed": False}
                        summary["records"].append(record)
                        if surface == "in_process":
                            actual, timing = native_call(plan, case)
                            verify(actual, case["expected"])
                            record.update(timing)
                            counters = session.snapshot()
                            if counters["prepared_source_opens"] != 1 or counters["completed_executions"] != sample + 1:
                                raise ValueError("in-process operation did not retain actual prepared source")
                            record["native_counters"] = counters
                            record["value"] = actual
                        else:
                            envelope, seconds, raw = worker.request(command_args(source, case), 30, guard)
                            verify(extract_result(envelope), case["expected"])
                            record["result_sha256"] = validate(envelope, case["expected"])
                            fields = {field["key"]: field["value"] for field in envelope.get("fields", [])}
                            validate_candidate_reuse(fields, "persistent_worker", sample)
                            if case["primitive"] == "count_where":
                                validate_candidate_count_where(fields, case["expected"])
                            if any(str(value).lower() != "false" for key, value in fields.items()
                                   if key.endswith(("fallback_attempted", "external_engine_invoked"))):
                                raise ValueError("worker fallback evidence disagrees")
                            record["complete_response_nanos"] = round(seconds * 1e9)
                            envelope_path = output / f"{case['name']}.worker.{sample:02}.json.gz"
                            compressed = gzip.compress(raw, mtime=0)
                            if gzip.decompress(compressed) != raw:
                                raise ValueError("lossless worker evidence compression failed")
                            envelope_path.write_bytes(compressed)
                            record["envelope"] = envelope_path.name
                            record["envelope_sha256"] = hashlib.sha256(raw).hexdigest()
                            record["resident_source_opens"] = fields.get("resident_source_opens")
                            record["resident_completed_executions"] = fields.get("resident_completed_executions")
                        record["passed"] = True
                plan.close()
                closed = session.close()
                if closed["native_owned_bytes"] != 0:
                    raise ValueError("in-process plan/native result owners failed to release")
                summary["closes"].append({"surface": "in_process", "case": case["name"], **closed})
        if len(summary["records"]) != 4 * 3 * 31 or not all(row["passed"] for row in summary["records"]):
            raise ValueError("three-surface sample matrix incomplete")
        summary["latencies"] = {}
        for surface in ("native_rust", "json_resident", "in_process"):
            for case in cases(rows):
                samples = [record for record in summary["records"] if record["surface"] == surface
                           and record["case"] == case["name"] and not record["warmup"]]
                if len(samples) != 30:
                    raise ValueError("performance sample count differs")
                clocks = [key for key in samples[0] if key.endswith("_nanos")]
                summary["latencies"][surface + "." + case["name"]] = {
                    clock: percentiles([record[clock] / 1e9 for record in samples]) for clock in clocks}
        if generation(source) != identity or sha256(source) != source_hash:
            raise ValueError("source changed through acceptance")
        for key, path in binaries.items():
            if sha256(path) != summary["binaries"][key]["sha256"]:
                raise ValueError("measured binary changed")
        guard()
        summary["status"] = "passed"
    except BaseException as error:
        summary["status"] = "failed"
        summary["error"] = str(error) or type(error).__name__
        raise
    finally:
        try:
            if output.is_dir():
                (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        finally:
            lock.rmdir()
    return output / "summary.json"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--native-control", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    print(run(parser.parse_args()))
