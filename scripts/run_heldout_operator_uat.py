#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Paired public native operator acceptance against an independent Python oracle.

The deterministic renamed-schema fixture checks operator semantics at requested
worker settings. Fresh-process timings include startup and complete output, but
exclude fixture preparation and Python validation. This bounded workload does
not establish production throughput, worker utilization, join coverage, or rank.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import datetime as dt
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import statistics
import sys

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import extract_result, file_sha256, run_command, run_profiled_command, strict_json
from run_resident_call_path_uat import generation, paired_order, percentiles, validate_preparation

WORKERS = (1, 2, 4, 8, 12)
HARNESS_FILES = ("run_heldout_operator_uat.py", "run_resident_call_path_uat.py",
                 "run_clickbench_query_uat.py", "timed_native_command.py", "local_uat_storage.py")
SUMMARY_COUNTER_PREFIXES = (
    "resident_", "local_primitive_aggregate_first_pass_", "local_primitive_aggregate_result_finalization_",
    "local_primitive_aggregate_fused_string_count_",
    "local_primitive_aggregate_workers_",
    "local_primitive_aggregate_provider_",
    "local_primitive_aggregate_native_numeric_accessor_",
    "local_primitive_aggregate_encoded_numeric_reduction_",
    "local_primitive_scan_segment_reuse_",
    "local_primitive_native_sort_spill_", "local_primitive_sort_spill_", "local_primitive_resource_",
    "local_primitive_physical_policy_selected_", "local_primitive_memory_",
)
SUMMARY_COUNTER_KEYS = {
    "fallback_attempted", "external_engine_invoked", "public_workflow_fallback_attempted",
    "public_workflow_external_engine_invoked", "local_primitive_rows_scanned", "local_primitive_rows_selected",
    "local_primitive_spill_state", "local_primitive_spill_io_performed", "local_primitive_aggregate_update_strategy",
    "local_primitive_compact_group_state_strategy", "local_primitive_group_state_mode",
    "local_primitive_estimated_group_key_storage_bytes", "local_primitive_estimated_group_string_storage_bytes",
    "local_primitive_candidate_groups", "local_primitive_retained_candidate_groups",
}


def fixture_rows(count: int) -> list[dict]:
    if not 64 <= count <= 131072:
        raise ValueError("fixture row count must be 64..=131072")
    labels = ["", "東京", "λ\"\n", "café", "not a URL", "a/b", "🙂", "plain"]
    rows = []
    for index in range(count):
        category = "hot-set" if index % 20 < 18 else labels[(index // 20) % len(labels)]
        rows.append({
            "row_key": index,
            "cohort_code": index % 7 - 3,
            "category_text": category,
            "optional_text": None if index % 11 == 0 else labels[index % len(labels)],
            "metric_units": index % 19 - 9,
            "optional_units": None if index % 7 == 0 else index % 31 - 15,
            "unique_text": f"item-{index:06d}",
            "exact_identifier": (-1 if index % 2 == 0 else 1) * (2**60 + index),
        })
    rows[0]["exact_identifier"] = -(2**63)
    rows[-1]["exact_identifier"] = 2**63 - 1
    return rows


def projected(rows: list[dict], columns: tuple[str, ...]) -> list[dict]:
    return [{column: row[column] for column in columns} for row in rows]


def scalar_oracle(rows: list[dict], column: str) -> dict:
    values = [row[column] for row in rows if row[column] is not None]
    # Inputs and sums fit exactly in binary64. No approximate comparison masks
    # reassociation or a rounded integer identifier in the public result.
    return {"n": len(rows), "present": len(values),
            "total": float(sum(values)) if values else None,
            "average": float(sum(values)) / len(values) if values else None,
            "smallest": min(values) if values else None,
            "largest": max(values) if values else None}


def group_oracle(rows: list[dict], keys: tuple[str, ...], measure=None) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[key] for key in keys)].append(row)
    return [{**dict(zip(keys, key)), **(measure(members) if measure else {"n": len(members)})}
            for key, members in groups.items()]


def cases(rows: list[dict]) -> list[dict]:
    def case(name, family, sql, expected, ordered=False):
        return {"name": name, "family": family, "sql": sql, "expected": expected,
                "comparison": "ordered_exact_typed_values" if ordered else "exact_typed_multiset"}

    numeric_measures = "COUNT(*) AS n, COUNT(optional_units) AS present, SUM(optional_units) AS total, AVG(optional_units) AS average, MIN(optional_units) AS smallest, MAX(optional_units) AS largest"
    numeric_groups = group_oracle(rows, ("cohort_code",), lambda members: {
        "n": len(members), "total": float(sum(row["metric_units"] for row in members))})
    distinct_groups = group_oracle(rows, ("cohort_code",), lambda members: {
        "n": len(members), "different": len({row["optional_text"] for row in members
                                              if row["optional_text"] is not None})})
    nonempty = [row for row in rows if row["category_text"] != ""]
    lengths = group_oracle(nonempty, ("cohort_code",), lambda members: {
        "n": len(members), "bytes_total": float(sum(len(row["category_text"].encode("utf-8")) for row in members))})
    unique = sorted(group_oracle(rows, ("unique_text",)), key=lambda row: row["unique_text"])[:12]
    compound_topk = sorted(group_oracle(rows, ("cohort_code", "category_text")),
                           key=lambda row: (-row["n"], row["cohort_code"], row["category_text"]))[:12]
    filtered = [row for row in rows if row["metric_units"] >= 5 and row["cohort_code"] < 2]
    filtered = sorted(filtered, key=lambda row: (-row["metric_units"], row["row_key"]))[3:15]
    tail = rows[-12:]
    result = [
        case("metadata_count", "scalar", "SELECT COUNT(*) FROM heldout", len(rows)),
        case("nullable_numeric_scalar", "scalar", f"SELECT {numeric_measures} FROM heldout", [scalar_oracle(rows, "optional_units")]),
        case("exact_integer_extrema", "scalar", "SELECT MIN(exact_identifier) AS smallest, MAX(exact_identifier) AS largest FROM heldout",
             [{"smallest": -(2**63), "largest": 2**63 - 1}]),
        case("scalar_distinct", "distinct", "SELECT COUNT(DISTINCT category_text) AS labels, COUNT(DISTINCT optional_text) AS optional_labels, COUNT(DISTINCT unique_text) AS unique_labels FROM heldout",
             [{"labels": len({r["category_text"] for r in rows}), "optional_labels": len({r["optional_text"] for r in rows if r["optional_text"] is not None}), "unique_labels": len(rows)}]),
        case("numeric_group_sum", "numeric_group", "SELECT cohort_code, COUNT(*) AS n, SUM(metric_units) AS total FROM heldout GROUP BY cohort_code", numeric_groups),
        case("skewed_string_count", "string_group", "SELECT category_text, COUNT(*) AS n FROM heldout GROUP BY category_text", group_oracle(rows, ("category_text",))),
        case("nullable_string_count", "string_group", "SELECT optional_text, COUNT(*) AS n FROM heldout GROUP BY optional_text", group_oracle(rows, ("optional_text",))),
        case("composite_group", "composite_group", "SELECT cohort_code, category_text, COUNT(*) AS n FROM heldout GROUP BY cohort_code, category_text", group_oracle(rows, ("cohort_code", "category_text"))),
        case("compound_count_topk", "composite_group", "SELECT cohort_code, category_text, COUNT(*) AS n FROM heldout GROUP BY cohort_code, category_text ORDER BY n DESC LIMIT 12", compound_topk, True),
        case("nullable_group_distinct", "distinct", "SELECT cohort_code, COUNT(*) AS n, COUNT(DISTINCT optional_text) AS different FROM heldout GROUP BY cohort_code", distinct_groups),
        case("utf8_byte_length", "string_transform", "SELECT cohort_code, COUNT(*) AS n, SUM(length(category_text)) AS bytes_total FROM heldout WHERE category_text <> '' GROUP BY cohort_code", lengths),
        case("all_unique_group_topk", "string_group", "SELECT unique_text, COUNT(*) AS n FROM heldout GROUP BY unique_text ORDER BY n DESC, unique_text ASC LIMIT 12", unique, True),
        case("filtered_sort_offset", "relational_sort", "SELECT row_key, metric_units, optional_text, exact_identifier FROM heldout WHERE metric_units >= 5 AND cohort_code < 2 ORDER BY metric_units DESC, row_key ASC LIMIT 12 OFFSET 3", projected(filtered, ("row_key", "metric_units", "optional_text", "exact_identifier")), True),
        case("nullable_projection", "relational_collect", f"SELECT optional_text, exact_identifier, row_key FROM heldout WHERE row_key >= {len(rows) - 12}", projected(tail, ("optional_text", "exact_identifier", "row_key"))),
        case("empty_numeric_scalar", "scalar", f"SELECT {numeric_measures} FROM heldout WHERE row_key >= {len(rows)}", [scalar_oracle([], "optional_units")]),
        case("empty_filtered_sort", "relational_sort", f"SELECT row_key, optional_text FROM heldout WHERE row_key >= {len(rows)} ORDER BY row_key ASC LIMIT 12", [], True),
    ]
    result.append({"name": "checked_signed_group_overflow", "family": "overflow_diagnostic",
                   "sql": "SELECT exact_identifier - 1 AS shifted, COUNT(*) AS n FROM heldout GROUP BY shifted",
                   "expected_diagnostic_terms": ["overflow", "no fallback execution was attempted"],
                   "expected_diagnostic_alternatives": ["direct int64 offset key overflowed", "additive group expression overflowed int64"],
                   "comparison": "explicit_native_overflow_diagnostic"})
    return result


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def exact_equal(actual, expected) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(exact_equal(actual[key], expected[key]) for key in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(exact_equal(a, b) for a, b in zip(actual, expected))
    return actual == expected and (not isinstance(actual, float) or math.isfinite(actual))


def validate_values(envelope: dict, case: dict) -> str:
    validate_no_fallback(envelope)
    actual, expected = extract_result(envelope), case["expected"]
    if case["comparison"] == "exact_typed_multiset" and isinstance(expected, list):
        if not isinstance(actual, list):
            raise ValueError("complete native rows were not returned")
        actual, expected = sorted(actual, key=canonical), sorted(expected, key=canonical)
    if not exact_equal(actual, expected):
        raise ValueError(f"complete exact typed values disagree: expected={canonical(expected)[:2048]} actual={canonical(actual)[:2048]}")
    return hashlib.sha256(canonical(actual).encode()).hexdigest()


def validate_no_fallback(envelope: dict) -> None:
    if envelope.get("fallback", {}).get("attempted") is not False:
        raise ValueError("envelope lacks structured no-fallback evidence")
    for diagnostic in envelope.get("diagnostics", []):
        if diagnostic.get("fallback", {}).get("attempted") is not False:
            raise ValueError("diagnostic lacks structured no-fallback evidence")


def validate_diagnostic(envelope: dict, case: dict, returncode: int) -> list[str]:
    if returncode <= 0 or envelope.get("status") not in ("error", "blocked"):
        raise ValueError("expected a normal nonzero exit with an explicit error envelope")
    text = canonical(envelope).lower()
    if not all(term in text for term in case["expected_diagnostic_terms"]):
        raise ValueError("native failure did not report the expected checked overflow")
    if not any(term in text for term in case["expected_diagnostic_alternatives"]):
        raise ValueError("failure was not checked signed group-key arithmetic overflow")
    validate_no_fallback(envelope)
    diagnostics = envelope.get("diagnostics", [])
    if not diagnostics or any(diagnostic.get("fallback", {}).get("attempted") is not False
                              for diagnostic in diagnostics):
        raise ValueError("diagnostic lacks explicit structured no-fallback evidence")
    for field in envelope.get("fields", []):
        if field.get("key", "").endswith(("fallback_attempted", "external_engine_invoked")):
            if not (field.get("value") is False or field.get("value") == "false"):
                raise ValueError("unsafe execution evidence in diagnostic")
    codes = [diagnostic.get("code") for diagnostic in diagnostics]
    if not all(isinstance(code, str) and code for code in codes):
        raise ValueError("expected stable diagnostic codes")
    return codes


def command_args(source: Path, case: dict, workers: int) -> list[str]:
    return ["run", "sql", "--input", str(source), "--input-format", "vortex", "--sql", case["sql"],
            "--request", "collect", "--execution-policy", "native_vortex", "--bounded", "true",
            "--memory-gb", "1", "--max-parallelism", str(workers), "--format", "json"]


def archive_stdout(path: Path) -> dict:
    """Retain exact newly generated output, verifying gzip before unlinking it."""
    digest = file_sha256(path)
    original_bytes = path.stat().st_size
    archive = path.with_suffix(path.suffix + ".gz")
    with archive.open("xb") as destination:
        with gzip.GzipFile(filename="", fileobj=destination, mode="wb", mtime=0) as compressed:
            with path.open("rb") as source:
                while chunk := source.read(MIB):
                    compressed.write(chunk)
    actual_digest = hashlib.sha256()
    actual_bytes = 0
    with gzip.open(archive, "rb") as source:
        while chunk := source.read(MIB):
            actual_digest.update(chunk)
            actual_bytes += len(chunk)
    if actual_bytes != original_bytes or actual_digest.hexdigest() != digest or file_sha256(path) != digest:
        raise ValueError("lossless stdout archive verification failed; original output retained")
    path.unlink()
    return {"envelope": archive.name, "stdout_raw_sha256": digest,
            "stdout_raw_bytes": original_bytes, "stdout_gzip_bytes": archive.stat().st_size,
            "stdout_encoding": "gzip_lossless_verified"}


def concise_execution_fields(envelope: dict) -> dict:
    """Bound summary duplication; complete evidence remains in hashed raw gzip."""
    selected = {}
    for field in envelope.get("fields", []):
        key, value = field.get("key", ""), field.get("value")
        if key in SUMMARY_COUNTER_KEYS or key.startswith(SUMMARY_COUNTER_PREFIXES):
            if len(key) <= 128 and isinstance(value, (str, int, float, bool)) and len(str(value)) <= 192:
                selected[key] = value
    if len(selected) > 128 or len(canonical(selected).encode()) > 24 * 1024:
        raise ValueError("concise execution counter summary exceeded its bound")
    return selected


def comparisons(records: list[dict], selected: list[dict], workers: list[int], samples: int) -> dict:
    result = {}
    for case in selected:
        for count in workers:
            matching = [r for r in records if r["case"] == case["name"] and r["requested_workers"] == count]
            identities = {(r["variant"], r["sample"]) for r in matching}
            complete = (len(matching) == 2 * (samples + 1) and all(r["passed"] for r in matching)
                        and identities == {(name, sample) for name in ("baseline", "candidate") for sample in range(samples + 1)})
            row = {"complete": complete, "passed_records": sum(r["passed"] for r in matching), "records": len(matching)}
            if complete and "expected" in case:
                values = {name: [r["seconds"] for r in matching if r["variant"] == name and not r["warmup"]]
                          for name in ("baseline", "candidate")}
                row.update({name: percentiles(value) for name, value in values.items()})
                ratios = [next(r["seconds"] for r in matching if r["variant"] == "baseline" and r["sample"] == sample) /
                          next(r["seconds"] for r in matching if r["variant"] == "candidate" and r["sample"] == sample)
                          for sample in range(1, samples + 1)]
                row["paired_baseline_over_candidate_ratios"] = ratios
                row["median_paired_ratio"] = statistics.median(ratios)
                row["min_paired_ratio"], row["max_paired_ratio"] = min(ratios), max(ratios)
                row["paired_ratio_sample_stdev"] = statistics.stdev(ratios) if len(ratios) > 1 else None
                row["uncertainty_scope"] = "observed paired spread; not a confidence interval or a production speedup claim"
            result[f"{case['name']}.workers_{count}"] = row
    return result


def execute(args) -> Path:
    if os.name != "posix":
        raise ValueError("this process-group harness requires a POSIX host")
    rows = fixture_rows(args.rows)
    all_cases = cases(rows)
    selected = all_cases if not args.cases else [case for case in all_cases if case["name"] in args.cases]
    if not selected or (args.cases and set(args.cases) != {case["name"] for case in selected}):
        raise ValueError("unknown or empty selected cases")
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    binaries = {name: getattr(args, f"{name}_binary").resolve(strict=True) for name in ("baseline", "candidate")}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = root / "logs" / f"heldout_operators_{stamp}"
    source = output / "fixture.vortex"
    # Keep space for the final readable summary rather than consuming the full
    # log allowance with raw operation evidence before the summary is written.
    summary_reserve_bytes = (2 * (args.samples + 1) * len(selected) * len(args.workers) * 32 * 1024) + 2 * MIB
    if summary_reserve_bytes >= 256 * MIB:
        raise ValueError("requested matrix cannot reserve its bounded summary within the existing log quota")

    def guard():
        check_budgets(root, source, output, min_free_bytes=GIB, reserve_bytes=0,
                      max_workspace_bytes=100 * GIB, max_log_bytes=256 * MIB - summary_reserve_bytes)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    summary = {"schema_version": "shardloom.heldout_native_operators.v1", "status": "running",
               "claim_gate_status": "not_claim_grade", "scope": __doc__.strip(),
               "oracle": "independent Python integer, set, grouping and ordering operations over generated rows; no external engine",
               "float_contract": "small integer sums exact in binary64; exact typed equality including averages; no tolerance",
               "timing_boundary": "native process creation through complete stdout and process exit; preparation and Python validation excluded",
               "cpu_boundary": "native child CPU work, which can overlap wall time",
               "rss_boundary": "per-native-child peak RSS; not allocator-reserved bytes",
               "cache_policy": "OS page cache uncontrolled; one immutable native artifact; fresh process for every query",
               "worker_scope": "requested worker ceilings; observed counters retained separately; no utilization or scaling claim",
               "execution_evidence": "selected bounded counters in summary; complete original envelope in verified gzip",
               "summary_reserved_bytes": summary_reserve_bytes,
               "ordering": "sequential alternating baseline/candidate pairs; one warmup per case and worker setting",
               "samples": args.samples, "workers": args.workers, "all_workers": WORKERS,
               "all_case_names": [case["name"] for case in all_cases], "cases": selected,
               "platform": platform.platform(), "python_version": platform.python_version(), "logical_cpus": os.cpu_count(),
               "binaries": {name: {"path": str(binary), "sha256": file_sha256(binary), "generation": generation(binary)} for name, binary in binaries.items()},
               "harness_files": {name: file_sha256(Path(__file__).with_name(name)) for name in HARNESS_FILES},
               "records": []}
    lock.mkdir()
    try:
        output.mkdir(parents=True)
        raw_source = output / "fixture.jsonl"
        with raw_source.open("x") as stream:
            for row in rows:
                stream.write(canonical(row) + "\n")
        prepare = [str(binaries["candidate"]), "prepare", "dataframe", "--input", str(raw_source),
                   "--input-format", "jsonl", "--output", str(source), "--memory-gb", "1",
                   "--max-parallelism", "2", "--format", "json"]
        summary["fixture_prepare_command"] = prepare
        preparation = run_command(prepare, output / "prepare.stdout.json", output / "prepare.stderr.txt", args.timeout, guard)
        if preparation["returncode"] or preparation["guard_failures"] or not source.is_file():
            raise ValueError("native fixture preparation failed")
        validate_preparation(strict_json((output / "prepare.stdout.json").read_text()))
        summary["fixture_prepare_output"] = archive_stdout(output / "prepare.stdout.json")
        identity, source_hash = generation(source), file_sha256(source)
        summary["fixture"] = {"rows": len(rows), "bytes": source.stat().st_size, "source": str(source),
                              "sha256": source_hash, "generation": identity, "input_sha256": file_sha256(raw_source),
                              "preparation": preparation}
        for count in args.workers:
            for case in selected:
                failed_variants = set()
                for sample in range(args.samples + 1):
                    for name in paired_order(sample):
                        if name in failed_variants:
                            continue
                        guard()
                        if generation(source) != identity or generation(binaries[name]) != summary["binaries"][name]["generation"]:
                            raise ValueError("source or binary changed during acceptance")
                        prefix = output / f"{case['name']}_w{count}_{name}_{sample:03d}"
                        command = [str(binaries[name]), *command_args(source, case, count)]
                        record = {"case": case["name"], "family": case["family"], "requested_workers": count,
                                  "variant": name, "sample": sample, "warmup": sample == 0,
                                  "command": command, "envelope": prefix.with_suffix(".stdout.json").name, "passed": False}
                        summary["records"].append(record)
                        record.update(run_profiled_command(command, prefix, args.timeout, guard))
                        # A checked error is an acceptance case, not a watchdog
                        # failure or a successful query eligible for a speed ratio.
                        unexpected_guards = [failure for failure in record["guard_failures"]
                                             if failure != "profiled native command failed"]
                        try:
                            if unexpected_guards:
                                raise ValueError(f"guarded native operation failed: {unexpected_guards}")
                            try:
                                envelope = strict_json(prefix.with_suffix(".stdout.json").read_text())
                                if "expected_diagnostic_terms" in case:
                                    record["diagnostic_codes"] = validate_diagnostic(envelope, case, record["returncode"])
                                else:
                                    if record["returncode"]:
                                        raise ValueError("native operation failed")
                                    record["result_sha256"] = validate_values(envelope, case)
                                if not math.isfinite(record["seconds"]) or record["seconds"] <= 0:
                                    raise ValueError("native timing must be finite and positive")
                                record["execution_fields"] = concise_execution_fields(envelope)
                                record["passed"] = True
                            except (ValueError, KeyError, TypeError) as error:
                                record["error"] = str(error)
                                failed_variants.add(name)
                        finally:
                            record.update(archive_stdout(prefix.with_suffix(".stdout.json")))
        if generation(source) != identity or file_sha256(source) != source_hash:
            raise ValueError("native source was not immutable through the paired run")
        for name, binary in binaries.items():
            if generation(binary) != summary["binaries"][name]["generation"] or file_sha256(binary) != summary["binaries"][name]["sha256"]:
                raise ValueError(f"{name} binary changed during the paired run")
        for name, digest in summary["harness_files"].items():
            if file_sha256(Path(__file__).with_name(name)) != digest:
                raise ValueError(f"harness component changed during the paired run: {name}")
        summary["comparisons"] = comparisons(summary["records"], selected, args.workers, args.samples)
        passed = all(row["complete"] for row in summary["comparisons"].values())
        summary["status"] = "passed" if passed else "failed"
        summary["full_matrix_complete"] = passed and len(selected) == len(all_cases) and tuple(args.workers) == WORKERS
        summary["all_requested_acceptance_cases_passed"] = passed
    except BaseException as error:
        summary["status"] = "failed"
        summary["error"] = str(error) or type(error).__name__
        raise
    finally:
        try:
            if output.is_dir():
                payload = (json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()
                if len(payload) > summary_reserve_bytes:
                    raise ValueError("readable summary exceeded its reserved bound; raw operation evidence retained")
                (output / "summary.json").write_bytes(payload)
        finally:
            lock.rmdir()
    return output / "summary.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--workers", type=lambda text: [int(value) for value in text.split(",")], default=list(WORKERS))
    parser.add_argument("--cases", type=lambda text: text.split(","))
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    if not 1 <= args.samples <= 100 or not 64 <= args.rows <= 131072:
        parser.error("samples must be 1..=100 and rows 64..=131072")
    if not args.workers or len(set(args.workers)) != len(args.workers) or any(value not in WORKERS for value in args.workers):
        parser.error("workers must be a unique subset of 1,2,4,8,12")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be finite and positive")
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("heldout acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    path = execute(args)
    print(path)
    return 0 if strict_json(path.read_text())["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
