# SPDX-License-Identifier: Apache-2.0
"""Fresh-process reuse jobs and frozen lifecycle gates for native output UAT."""

from __future__ import annotations

import math
from pathlib import Path
import statistics

from run_clickbench_query_uat import extract_result, run_profiled_command, strict_json
from run_heldout_operator_uat import archive_stdout, command_args, exact_equal, validate_no_fallback
from run_resident_call_path_uat import generation, percentiles

REPETITIONS = 3
LIFECYCLE_MAX_RATIO = 1.05
WRITE_MAX_RATIO = 1.10
PROCESS_NOISE_SECONDS = 0.005


def reuse_cases(rows: list[dict]) -> list[dict]:
    """Oracles use complete fixture values, never either engine's answers."""
    sequence = [row["shipment_sequence"] for row in rows]
    cutoff = max(sequence) - 6
    tail = [{"shipment_sequence": row["shipment_sequence"], "renamed_label": row["renamed_label"]}
            for row in reversed(rows) if row["shipment_sequence"] >= cutoff]
    return [
        {"name": "integer_aggregate", "sql": "SELECT COUNT(*) AS n, MIN(shipment_sequence) AS lo, "
         "MAX(shipment_sequence) AS hi, COUNT(exact_identifier) AS present FROM heldout",
         "expected": [{"n": len(rows), "lo": min(sequence), "hi": max(sequence),
                       "present": sum(row["exact_identifier"] is not None for row in rows)}]},
        {"name": "range_prune", "sql": "SELECT shipment_sequence, renamed_label FROM heldout "
         "WHERE shipment_sequence < 0", "expected": []},
        {"name": "scan_control", "sql": "SELECT shipment_sequence, renamed_label FROM heldout "
         f"WHERE shipment_sequence >= {cutoff} ORDER BY shipment_sequence DESC", "expected": tail},
    ]


def bind_reuse_case(case: dict, artifact: Path) -> dict:
    """Bind every SQL route to the exact file, including the relational sorter."""
    marker = "FROM heldout"
    if case["sql"].count(marker) != 1:
        raise ValueError("persisted reuse requires one explicit source binding")
    source = str(artifact).replace("'", "''")
    return {**case, "sql": case["sql"].replace(marker, f"FROM '{source}'")}


def validate_reuse(envelope: dict, case: dict, *, candidate: bool) -> dict:
    validate_no_fallback(envelope)
    if not exact_equal(extract_result(envelope), case["expected"]):
        raise ValueError(f"persisted reuse result differs from independent oracle: {case['name']}")
    fields = {field["key"]: field["value"] for field in
              [*envelope.get("fields", []), *envelope.get("result", {}).get("fields", [])]}
    if case["name"] == "integer_aggregate":
        count = fields.get("local_primitive_arrays_read_count")
        if not isinstance(count, str) or not count.isascii() or not count.isdecimal():
            raise ValueError("persisted aggregate is missing its returned-array count")
        if candidate and (count != "0" or fields.get("local_primitive_aggregate_update_strategy")
                          != "exact_file_footer_scalar_completion"):
            raise ValueError("persisted aggregate did not complete from its file statistics")
        if not candidate and int(count) == 0:
            raise ValueError("baseline already skips aggregate arrays; this case cannot establish new metadata reuse")
    elif case["name"] == "scan_control":
        rows = fields.get("relational_scan_rows_delivered")
        if not isinstance(rows, str) or not rows.isascii() or not rows.isdecimal() or int(rows) == 0:
            raise ValueError("non-metadata control did not deliver its source payload rows")
    elif case["name"] == "range_prune":
        # This public projection does not publish a payload-request counter.
        # An empty result alone is not I/O proof. The matching resident-provider
        # regression separately observes real segment requests and a scan control.
        if (fields.get("public_workflow_route_id") != "native_vortex_filter_project"
                or fields.get("resident_completed_executions") != "1"):
            raise ValueError("persisted range projection did not complete its native execution")
    else:
        raise ValueError("unknown persisted reuse job")
    return {key: value for key, value in fields.items()
            if key.startswith(("local_primitive_", "resident_", "relational_", "execution_resource_observed_"))
            or key in ("data_decoded", "arrow_converted", "decode_materialization_boundary")}


def measure_reuse(binary, artifact, expected, prefix, workers, timeout, guard, records, *, candidate):
    identity = generation(artifact)
    for repeat in range(REPETITIONS):
        for case in reuse_cases(expected):
            guard()
            if generation(artifact) != identity:
                raise ValueError("persisted output changed before its next job")
            stem = prefix.with_name(prefix.name + f"_{case['name']}_{repeat:02d}")
            command = [str(binary), *command_args(artifact, bind_reuse_case(case, artifact), workers)]
            record = {"job": case["name"], "repeat": repeat, "first_use": repeat == 0,
                      "command": command, "passed": False,
                      **run_profiled_command(command, stem, timeout, guard)}
            records.append(record)
            if record["returncode"] or record["guard_failures"]:
                raise ValueError("persisted reuse job failed")
            envelope = strict_json(stem.with_suffix(".stdout.json").read_text())
            record["fields"] = validate_reuse(envelope, case, candidate=candidate)
            record.update(archive_stdout(stem.with_suffix(".stdout.json")))
            record["passed"] = True
    if generation(artifact) != identity:
        raise ValueError("persisted output changed during later computations")
    return records


def reuse_comparisons(records: list[dict], bounds: list[int]) -> dict:
    comparisons = {}
    for rows in bounds:
        cohort = [record for record in records if record["rows"] == rows and not record["warmup"]]
        samples = sorted({record["sample"] for record in cohort})
        if len(samples) < 5 or len(cohort) != 2 * len(samples):
            raise ValueError("persisted cost gate requires five complete paired samples")
        paired = []
        for sample in samples:
            pair = {record["variant"]: record for record in cohort if record["sample"] == sample}
            if set(pair) != {"baseline", "candidate"}:
                raise ValueError("persisted cost gate has a missing or duplicate variant")
            values = {}
            for variant, record in pair.items():
                expected_jobs = {(case, repeat) for case in ("integer_aggregate", "range_prune", "scan_control")
                                 for repeat in range(REPETITIONS)}
                if ({(job["job"], job["repeat"]) for job in record["reuse"]} != expected_jobs
                        or len(record["reuse"]) != len(expected_jobs)
                        or not record["passed"] or not all(job["passed"] for job in record["reuse"])):
                    raise ValueError("persisted cost gate has incomplete or failed reuse jobs")
                measurements = [record["seconds"], record["output"]["bytes"],
                                *(job["seconds"] for job in record["reuse"])]
                if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0
                       for value in measurements):
                    raise ValueError("persisted cost gate requires finite positive measurements")
                values[variant] = {
                    "write": record["seconds"],
                    "first_lifecycle": record["seconds"] + sum(job["seconds"] for job in record["reuse"]
                                                               if job["repeat"] == 0),
                    "repeated_lifecycle": record["seconds"] + sum(job["seconds"] for job in record["reuse"]),
                    "scan_control": statistics.median(job["seconds"] for job in record["reuse"]
                                                       if job["job"] == "scan_control"),
                    "output_bytes": record["output"]["bytes"],
                }
            paired.append(values)
        metrics = {}
        for metric in ("write", "first_lifecycle", "repeated_lifecycle", "scan_control", "output_bytes"):
            baseline = [pair["baseline"][metric] for pair in paired]
            candidate = [pair["candidate"][metric] for pair in paired]
            differences = [right - left for left, right in zip(baseline, candidate, strict=True)]
            limit = WRITE_MAX_RATIO if metric == "write" else LIFECYCLE_MAX_RATIO
            if metric == "output_bytes":
                margins = [right - (left + max(4096, left * 0.01))
                           for left, right in zip(baseline, candidate, strict=True)]
            else:
                allowance = PROCESS_NOISE_SECONDS if metric in {"write", "scan_control"} else 0
                margins = [right - max(left * limit, left + allowance)
                           for left, right in zip(baseline, candidate, strict=True)]
            metrics[metric] = {"baseline": percentiles(baseline), "candidate": percentiles(candidate),
                               "paired_differences": differences,
                               "median_gate_margin": statistics.median(margins),
                               "passed": statistics.median(margins) <= 0}
        comparisons[str(rows)] = {"metrics": metrics, "passed": all(value["passed"] for value in metrics.values()),
                                  "timing_scope": "sum of complete fresh-process clocks; validation/hash/archive excluded",
                                  "uncertainty_scope": "paired observations; not a confidence interval"}
    return comparisons
