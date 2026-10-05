#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Parameterized, guarded benchmarks through ShardLoom's public native engine.

External engines are independent comparison processes. They never provide a
result to ShardLoom or take over a rejected native declaration.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(REPO / "scripts"))
from baseline_catalog import BASELINE_NAMES
from benchmark_models import DatasetPaths, FORMAT_ORDER, GENERATED_DATASET_PROFILES, comparison_input_format
from comparison import CORRECTNESS_FLOAT_DIGITS, round_float
from fixtures import dim_path, fact_part_paths, fact_path
from native_public_runner import NativeConfiguration, NativeDeclarationUnsupported, NativePublicRunner
from resources import BenchmarkGuard
from run_clickbench_query_uat import equivalent, file_sha256, run_profiled_command
from native_workflow_protocol import strict_json
from workloads import WORKLOADS

SCHEMA = "shardloom.public_native_benchmark.v1"


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def positive_seconds(value):
    import math
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("expected finite positive seconds")
    return number


def write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, default=str, allow_nan=False)
        stream.write("\n")


def input_inventory(root):
    return [{"path": str(path), "sha256": file_sha256(path), "bytes": path.stat().st_size}
            for path in sorted(root.rglob("*")) if path.is_file()]


def generations(inventory):
    return {row["path"]: tuple(getattr(Path(row["path"]).stat(), name)
                              for name in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"))
            for row in inventory}


def harness_source_inventory():
    paths = {*Path(__file__).parent.glob("*.py"), *(REPO / "python/src/shardloom").glob("*.py")}
    paths.update(REPO / "scripts" / name for name in (
        "native_workflow_protocol.py", "run_clickbench_query_uat.py", "clickbench_reference_packet.py", "local_uat_storage.py",
        "timed_native_command.py",
    ))
    return {str(path.relative_to(REPO)): file_sha256(path) for path in sorted(paths)}


def summarize(records, expected_keys):
    keys = [(row["engine"], row["format"], row["scenario"], row["repeat"]) for row in records]
    complete = (len(keys) == len(expected_keys) and set(keys) == expected_keys
                and all(row["status"] == "passed" for row in records))
    groups = {}
    for row in records:
        key = (row["engine"], row["format"], row["scenario"])
        groups.setdefault(key, []).append(row)
    timings = []
    for (engine, data_format, scenario), rows in groups.items():
        if all(row["status"] == "passed" for row in rows):
            timings.append({"engine": engine, "format": data_format, "scenario": scenario,
                            "input_format": data_format if engine == "shardloom" else comparison_input_format(data_format),
                            "seconds": [row["seconds"] for row in rows],
                            "median_seconds": statistics.median(row["seconds"] for row in rows),
                            "timing_boundary": rows[0]["timing_boundary"]})
    return {"complete": complete, "expected_cases": len(expected_keys),
            "recorded_cases": len(records),
            "passed_cases": sum(row["status"] == "passed" for row in records),
            "timings": timings, "performance_claim": False}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--list", action="store_true", help="show declarations without running engines")
    result.add_argument("--shardloom-binary", type=Path)
    result.add_argument("--workspace", type=Path,
                        default=Path.home() / "LocalData/shardloom/traditional-benchmarks")
    result.add_argument("--rows", type=positive_int, default=1000)
    result.add_argument("--dim-rows", type=positive_int, default=20)
    result.add_argument("--dataset-profile", choices=GENERATED_DATASET_PROFILES, default="tiny_smoke")
    result.add_argument("--formats", nargs="+", choices=FORMAT_ORDER, default=["csv"])
    result.add_argument("--scenarios", nargs="+", choices=tuple(WORKLOADS), default=list(WORKLOADS))
    result.add_argument("--engines", nargs="+", choices=("shardloom", *BASELINE_NAMES),
                        default=["shardloom", "pandas"])
    result.add_argument("--reference-engine", choices=BASELINE_NAMES, default="pandas")
    result.add_argument("--input-state", choices=("raw", "prepared"), default="raw")
    result.add_argument("--output-format", choices=("collect", "vortex", "csv", "json", "jsonl",
                                                   "parquet", "arrow_ipc", "avro", "orc"),
                        default="collect")
    result.add_argument("--repeats", type=positive_int, default=3)
    result.add_argument("--memory-gb", type=positive_int, default=1)
    result.add_argument("--max-parallelism", type=positive_int, default=2)
    result.add_argument("--timeout", type=positive_seconds, default=120)
    return result


def worker(job, prefix, args, guard):
    job_path = prefix.with_suffix(".job.json")
    write_json(job_path, job)
    receipt = run_profiled_command(
        [sys.executable, str(Path(__file__).with_name("worker.py")), "--job", str(job_path)],
        prefix, args.timeout, guard.check,
    )
    stdout = prefix.with_suffix(".stdout.json")
    receipt.update(stdout=str(stdout), stdout_sha256=file_sha256(stdout),
                   job=str(job_path), job_sha256=file_sha256(job_path))
    write_json(prefix.with_suffix(".receipt.json"), receipt)
    result = strict_json(stdout.read_text())
    if any(item != "profiled native command failed" for item in receipt["guard_failures"]):
        raise ValueError(f"benchmark worker resource guard failed: {receipt}")
    if receipt["returncode"] != 0 and result.get("status") == "passed":
        raise ValueError("worker reported success after a failed process")
    return result, receipt


def run(args):
    binary = args.shardloom_binary.resolve(strict=True)
    if not binary.is_file():
        raise ValueError("a built native executable is required")
    engines = list(dict.fromkeys([args.reference_engine, *args.engines]))
    if "shardloom" not in engines:
        raise ValueError("this harness requires the public ShardLoom candidate")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    report = {"schema_version": SCHEMA, "status": "running", "run_id": stamp,
              "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
              "configuration": vars(args), "engines": engines, "records": [],
              "reference_engine": args.reference_engine, "independent_reference": True,
              "comparison_input_formats": {fmt: comparison_input_format(fmt) for fmt in args.formats},
              "comparison_contract": {"floating_decimal_places": CORRECTNESS_FLOAT_DIGITS,
                                      "empty_metric_sum": 0.0,
                                      "complete_rows_required": True,
                                      "vortex_reference": "original CSV fixture; comparison adapters do not read Vortex"},
              "binary": {"path": str(binary), "sha256": file_sha256(binary)},
              "harness_sources": harness_source_inventory(),
              "workload_declarations": {name: asdict(WORKLOADS[name]) for name in args.scenarios},
              "host": {"platform": platform.platform(), "machine": platform.machine(),
                       "logical_cpu_count": os.cpu_count()},
              "timing_policy": {
                  "process": "one fresh executable process per complete request, through output and exit",
                  "native_preparation": "outside query timing when input_state=prepared",
                  "output_validation": "outside query timing; included in actual run wall time",
                  "fixture_generation": "outside query timing; included in actual run wall time",
                  "vortex_fixture_preparation": "public preparation from original CSV, before input freeze and query timing",
                  "source_hashing": "outside query timing; may warm filesystem cache",
                  "native_build": "caller supplies frozen executable; no build in harness",
                  "baseline_output": "each adapter's declared terminal result, independent comparison only",
                  "comparison_scope": "no speedup or superiority claim; sink/lifecycle differences remain explicit",
              },
              "resource_policy": {"native_memory_gb": args.memory_gb,
                                  "native_max_parallelism": args.max_parallelism,
                                  "sampled_process_tree_rss_limit_bytes": args.memory_gb * 1024**3 + 512 * 1024**2,
                                  "rss_is_sampled_not_an_allocator_guarantee": True},
              "query_answers_cached_by_native_adapter": False, "performance_claim": False}
    started = time.perf_counter()
    expected = {(engine, fmt, scenario, repeat)
                for engine in engines for fmt in args.formats for scenario in args.scenarios
                for repeat in range(1, args.repeats + 1)}
    destination = None
    try:
        with BenchmarkGuard(args.workspace, binary=binary, memory_gb=args.memory_gb) as guard:
            run_root = guard.root / "runs" / stamp
            run_root.mkdir(parents=True, exist_ok=False)
            logs = guard.root / "logs" / stamp
            logs.mkdir(parents=True, exist_ok=False)
            destination = run_root / "report.json"
            report["output_directory"] = str(run_root)
            fixture, receipt = worker(
                {"operation": "fixture", "root": str(run_root / "input"), "rows": args.rows,
                 "dim_rows": args.dim_rows, "formats": args.formats,
                 "dataset_profile": args.dataset_profile},
                logs / "fixture", args, guard,
            )
            report["fixture_receipt"] = receipt
            if fixture["status"] != "passed":
                raise ValueError(f"fixture generation failed: {fixture}")
            paths = replace(DatasetPaths.from_record(fixture["paths"]),
                            output_root=run_root / "baseline-output")
            native = NativePublicRunner(
                NativeConfiguration(binary, guard.root, args.input_state, args.output_format,
                                    args.memory_gb, args.max_parallelism, args.timeout),
                fact_path=fact_path, dim_path=dim_path, fact_part_paths=fact_part_paths,
                round_float=round_float, guard=guard.check,
            )
            report["native_fixture_preparation"] = native.fixture_preparation_receipts
            native.prepare_fixture_inputs(paths, args.formats)
            inventory = input_inventory(paths.root)
            initial_generations = generations(inventory)
            report["inputs"] = inventory
            references = {}
            prepare_error = None
            # Independent query results are frozen before candidate workloads.
            for engine in engines:
                if engine == "shardloom":
                    try:
                        native.prepare(paths, args.formats, args.scenarios)
                    except (NativeDeclarationUnsupported, ValueError, RuntimeError, OSError) as error:
                        prepare_error = f"{type(error).__name__}: {error}"
                    report["native_input_preparation"] = native.preparation_receipts
                for fmt in args.formats:
                    for scenario in args.scenarios:
                        for repeat in range(1, args.repeats + 1):
                            guard.check()
                            if generations(inventory) != initial_generations:
                                raise ValueError("input changed during benchmark")
                            record = {"engine": engine, "format": fmt, "scenario": scenario,
                                      "repeat": repeat, "status": "failed"}
                            try:
                                if engine == "shardloom":
                                    if prepare_error:
                                        raise RuntimeError(prepare_error)
                                    response = native.run(scenario, paths, fmt)
                                    value = response["__benchmark_result"]
                                    evidence = response["__shardloom_evidence"]
                                    record.update(evidence=evidence,
                                                  seconds=float(evidence["cli_process_wall_millis"]) / 1000,
                                                  timing_boundary=evidence["benchmark_timing_boundary"])
                                else:
                                    actual_format = comparison_input_format(fmt)
                                    record["comparison_input_format"] = actual_format
                                    response, receipt = worker(
                                        {"operation": "baseline", "engine": engine, "format": actual_format,
                                         "scenario": scenario, "paths": asdict(paths)},
                                        logs / f"comparison-{len(report['records']):06d}", args, guard,
                                    )
                                    record.update(receipt=receipt, seconds=receipt["seconds"],
                                                  timing_boundary="fresh comparison process through complete result and exit")
                                    if response["status"] != "passed":
                                        record.update(status=response["status"], error=response.get("error"))
                                        report["records"].append(record)
                                        continue
                                    if response.get("input_format") != actual_format:
                                        raise ValueError("comparison process used a different input format")
                                    value = response["result"]
                                    record["engine_version"] = response["version"]
                                key = (fmt, scenario)
                                if engine == args.reference_engine and key not in references:
                                    references[key] = value
                                    write_json(logs / f"reference-{len(references):04d}.json",
                                               {"format": fmt, "scenario": scenario, "result": value,
                                                "comparison_input_format": comparison_input_format(fmt),
                                                "engine": engine, "record_index": len(report["records"])})
                                record["result"] = value
                                record["result_sha256"] = hashlib.sha256(
                                    json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()
                                if key not in references:
                                    raise ValueError("independent reference is unavailable")
                                record["matches_reference"] = equivalent(value, references[key])
                                record["status"] = "passed" if record["matches_reference"] else "mismatch"
                            except NativeDeclarationUnsupported as error:
                                record.update(status="unsupported", error=str(error))
                            except Exception as error:
                                record.update(status="failed", error=f"{type(error).__name__}: {error}")
                            report["records"].append(record)
                            print(json.dumps({key: record[key] for key in
                                              ("engine", "format", "scenario", "repeat", "status")}), flush=True)
            final_files = input_inventory(paths.root)
            original_paths = {row["path"] for row in inventory}
            if [row for row in final_files if row["path"] in original_paths] != inventory:
                raise ValueError("fixture content changed during benchmark")
            derived = [row for row in final_files if row["path"] not in original_paths]
            if any(".shardloom/prepared" not in row["path"] or
                   not row["path"].endswith(".vortex") for row in derived):
                raise ValueError("unexpected output appeared inside the fixture directory")
            report["native_prepared_artifacts"] = derived
            if file_sha256(binary) != report["binary"]["sha256"]:
                raise ValueError("native executable changed during benchmark")
            if harness_source_inventory() != report["harness_sources"]:
                raise ValueError("benchmark harness source changed during execution")
            report["summary"] = summarize(report["records"], expected)
            report["status"] = "passed" if report["summary"]["complete"] else "incomplete"
            guard.check()
            report["resource_samples"] = guard.samples
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        report["summary"] = summarize(report["records"], expected)
    finally:
        report["actual_run_wall_seconds"] = time.perf_counter() - started
        if destination is not None:
            write_json(destination, report)
            print(json.dumps({"status": report["status"], "report": str(destination)}), flush=True)
        else:
            print(json.dumps({"status": report["status"], "error": report.get("error")}), file=sys.stderr)
    return 0 if report["status"] == "passed" else 1


def main():
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    options = parser()
    args = options.parse_args()
    for name in ("engines", "formats", "scenarios"):
        values = getattr(args, name)
        if len(values) != len(set(values)):
            options.error(f"--{name} must not repeat declarations")
    if args.list:
        print(json.dumps({"schema_version": SCHEMA, "engines": ["shardloom", *BASELINE_NAMES],
                          "input_states": ["raw", "prepared"], "formats": FORMAT_ORDER,
                          "scenarios": {name: asdict(workload) for name, workload in WORKLOADS.items()}}, indent=2))
        return 0
    if args.shardloom_binary is None:
        options.error("--shardloom-binary is required for execution")
    # Bound common comparison-library pools without claiming every engine honors
    # these variables; the process-tree watchdog independently enforces sampled RSS.
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                 "NUMEXPR_NUM_THREADS", "POLARS_MAX_THREADS"):
        os.environ[name] = str(args.max_parallelism)
    os.environ["SHARDLOOM_BENCHMARK_PARALLELISM"] = str(args.max_parallelism)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
