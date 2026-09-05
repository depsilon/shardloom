#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure complete native output, then reopen and verify every requested value.

Paired fresh processes use one immutable renamed/nullable source. Output write,
flush, validation and publication are timed; the independent post-run value
comparison is reported separately. This is a local acceptance sample, not rank.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import sys
import time

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import extract_result, file_sha256, run_profiled_command, strict_json
from run_heldout_operator_uat import archive_stdout, canonical, exact_equal, fixture_rows, validate_no_fallback
from run_resident_call_path_uat import generation, paired_order, percentiles, validate_preparation

HARNESS_FILES = ("run_native_output_uat.py", "run_heldout_operator_uat.py",
                 "run_resident_call_path_uat.py", "run_clickbench_query_uat.py",
                 "timed_native_command.py", "local_uat_storage.py")
COLUMNS = (("renamed_label", "optional_text"), ("shipment_sequence", "row_key"),
           ("exact_identifier", "exact_identifier"))


def export_args(source: Path, output: Path, limit: int, workers: int) -> list[str]:
    projection = {"structured_columns": [{"name": name, "source": source_name}
                                           for name, source_name in COLUMNS]}
    return ["run", "dataframe", "--input", str(source), "--input-format", "vortex",
            "--request", "write_vortex", "--output", str(output), "--bounded", "true",
            "--execution-policy", "native_vortex", "--vortex-primitive", "expression_project",
            "--vortex-columns", ",".join(source_name for _, source_name in COLUMNS),
            "--vortex-expression-projection", canonical(projection),
            "--vortex-source-order-limit", str(limit), "--memory-gb", "1",
            "--max-parallelism", str(workers), "--format", "json"]


def expected_output(rows: list[dict], limit: int) -> list[dict]:
    return [{name: row[source] for name, source in COLUMNS} for row in rows[:limit]]


def validate_output(envelope: dict, expected: list[dict]) -> None:
    validate_no_fallback(envelope)
    if not exact_equal(extract_result(envelope), expected):
        raise ValueError("reopened complete typed output differs from independent fixture")


def validate_array_evidence(fields: dict, output_digest: str) -> None:
    required = {
        "native_vortex_result_export_kind": "owned_native_array_stream",
        "native_vortex_array_sink_adapter_payload_bytes_copied": "0",
        "native_vortex_array_sink_scalar_values_materialized": "0",
        "native_vortex_array_sink_source_generation_validated": "true",
        "native_vortex_array_sink_dtype_and_row_count_validated": "true",
        "native_vortex_array_sink_output_sha256": output_digest,
        "arrow_converted": "false",
    }
    for key, expected in required.items():
        if fields.get(key) != expected:
            raise ValueError(f"candidate native array evidence mismatch: {key}")
    if int(fields.get("native_vortex_array_sink_arrays_submitted", "0")) < 1:
        raise ValueError("candidate did not submit executable native output arrays")
    if fields.get("native_vortex_array_sink_writer_input_batch_bound") != "3":
        raise ValueError("candidate native writer input bound was not reported")


def execute(args) -> Path:
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = root / "logs" / f"native_output_{stamp}"
    artifacts = root / "vortex" / f"native_output_{stamp}"
    source = artifacts / "fixture.vortex"
    binaries = {name: getattr(args, f"{name}_binary").resolve(strict=True)
                for name in ("baseline", "candidate")}
    rows = fixture_rows(args.rows)
    bounds = [args.rows, min(10_003, args.rows // 2)]

    def guard():
        check_budgets(root, source, output, min_free_bytes=12 * GIB, reserve_bytes=64 * MIB,
                      max_workspace_bytes=100 * GIB, max_log_bytes=250 * MIB)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    summary = {"schema_version": "shardloom.native_output_uat.v1", "status": "running",
               "claim_gate_status": "not_claim_grade", "scope": __doc__.strip(),
               "platform": platform.platform(), "logical_cpus": os.cpu_count(),
               "samples": args.samples, "warmups_per_case": 1, "requested_workers": args.workers,
               "candidate_native_array_required": args.require_native_array_candidate,
               "cache_policy": "OS page cache uncontrolled; no answer cache; fresh process per output",
               "ordering": "alternating baseline/candidate pairs; each output is newly created",
               "oracle": "literal projected values from independent generated nullable Unicode/exact-int fixture",
               "validation_reader": "frozen candidate native reader with logical-field lookup; every output value compared after publication against independent fixture; older baseline reader assumes physical Struct children for some valid native layouts",
               "binaries": {name: {"path": str(path), "sha256": file_sha256(path), "generation": generation(path)}
                            for name, path in binaries.items()},
               "harness_files": {name: file_sha256(Path(__file__).with_name(name)) for name in HARNESS_FILES},
               "records": []}
    lock.mkdir()
    try:
        output.mkdir(parents=True)
        artifacts.mkdir(parents=True)
        raw = artifacts / "fixture.jsonl"
        with raw.open("x") as stream:
            for row in rows:
                stream.write(canonical(row) + "\n")
        command = [str(binaries["baseline"]), "prepare", "dataframe", "--input", str(raw),
                   "--input-format", "jsonl", "--output", str(source), "--memory-gb", "1",
                   "--max-parallelism", str(args.workers), "--format", "json"]
        prep = output / "prepare"
        summary["preparation"] = {"command": command, **run_profiled_command(command, prep, args.timeout, guard)}
        if summary["preparation"]["returncode"] or summary["preparation"]["guard_failures"]:
            raise ValueError("fixture preparation failed")
        validate_preparation(strict_json(prep.with_suffix(".stdout.json").read_text()))
        summary["preparation"].update(archive_stdout(prep.with_suffix(".stdout.json")))
        source_identity, source_digest = generation(source), file_sha256(source)
        summary["fixture"] = {"rows": args.rows, "path": str(source), "sha256": source_digest,
                              "generation": source_identity, "input_sha256": file_sha256(raw)}
        for limit in bounds:
            expected = expected_output(rows, limit)
            for sample in range(args.samples + 1):
                for name in paired_order(sample):
                    guard()
                    if generation(source) != source_identity:
                        raise ValueError("source changed during paired output execution")
                    prefix = output / f"rows_{limit}_{name}_{sample:03d}"
                    artifact = artifacts / f"{prefix.name}.vortex"
                    command = [str(binaries[name]), *export_args(source, artifact, limit, args.workers)]
                    record = {"variant": name, "rows": limit, "sample": sample, "warmup": sample == 0,
                              "command": command, "passed": False,
                              **run_profiled_command(command, prefix, args.timeout, guard)}
                    summary["records"].append(record)
                    if record["returncode"] or record["guard_failures"]:
                        raise ValueError("native output failed")
                    envelope = strict_json(prefix.with_suffix(".stdout.json").read_text())
                    validate_no_fallback(envelope)
                    fields = {field["key"]: field["value"] for field in envelope["fields"]}
                    record["export_fields"] = {key: value for key, value in fields.items()
                                               if key.startswith("native_vortex_array_sink_") or key in
                                               ("native_vortex_result_export_kind", "decode_materialization_boundary",
                                                "data_decoded", "arrow_converted", "rows_written")}
                    record.update(archive_stdout(prefix.with_suffix(".stdout.json")))
                    identity = generation(artifact)
                    validation = prefix.with_name(prefix.name + "_reopen")
                    command = [str(binaries["candidate"]), "run", "dataframe", "--input", str(artifact),
                               "--input-format", "vortex", "--request", "collect", "--bounded", "true",
                               "--vortex-primitive", "project", "--vortex-columns", ",".join(name for name, _ in COLUMNS),
                               "--memory-gb", "1", "--max-parallelism", "1", "--format", "json"]
                    started = time.perf_counter()
                    record["reopen"] = {"command": command,
                                        **run_profiled_command(command, validation, args.timeout, guard)}
                    if record["reopen"]["returncode"] or record["reopen"]["guard_failures"]:
                        raise ValueError("native output reopening failed")
                    validate_output(strict_json(validation.with_suffix(".stdout.json").read_text()), expected)
                    if generation(artifact) != identity:
                        raise ValueError("published native output changed during validation")
                    record["output"] = {"path": str(artifact), "sha256": file_sha256(artifact), "generation": identity,
                                        "bytes": artifact.stat().st_size, "rows": limit}
                    if name == "candidate" and args.require_native_array_candidate:
                        validate_array_evidence(fields, record["output"]["sha256"])
                    record["validation_seconds"] = time.perf_counter() - started
                    record["reopen"].update(archive_stdout(validation.with_suffix(".stdout.json")))
                    record["passed"] = True
        for name, path in binaries.items():
            if generation(path) != summary["binaries"][name]["generation"] or file_sha256(path) != summary["binaries"][name]["sha256"]:
                raise ValueError("binary changed during output acceptance")
        if generation(source) != source_identity or file_sha256(source) != source_digest:
            raise ValueError("source changed during output acceptance")
        for name, digest in summary["harness_files"].items():
            if file_sha256(Path(__file__).with_name(name)) != digest:
                raise ValueError("acceptance harness changed during execution")
        summary["comparisons"] = {}
        for limit in bounds:
            records = [record for record in summary["records"] if record["rows"] == limit and not record["warmup"]]
            times = {name: percentiles([record["seconds"] for record in records if record["variant"] == name])
                     for name in binaries}
            ratios = [next(r["seconds"] for r in records if r["variant"] == "baseline" and r["sample"] == sample) /
                      next(r["seconds"] for r in records if r["variant"] == "candidate" and r["sample"] == sample)
                      for sample in range(1, args.samples + 1)]
            summary["comparisons"][str(limit)] = {**times, "paired_baseline_over_candidate_ratios": ratios,
                                                  "median_paired_ratio": statistics.median(ratios),
                                                  "uncertainty_scope": "observed paired spread; not a confidence interval"}
        summary["status"] = "passed"
    except BaseException as error:
        summary["status"] = "failed"
        summary["error"] = str(error) or type(error).__name__
        raise
    finally:
        try:
            if output.is_dir():
                (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
        finally:
            lock.rmdir()
    return output / "summary.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=40_000)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--require-native-array-candidate", action="store_true")
    args = parser.parse_args()
    if not 64 <= args.rows <= 40_000 or not 1 <= args.samples <= 10 or not 1 <= args.workers <= 12:
        parser.error("rows must be 64..=40000, samples 1..=10, workers 1..=12")
    if not 0 < args.timeout <= 600:
        parser.error("timeout must be finite and 0..=600 seconds")
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("native output acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    print(execute(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
