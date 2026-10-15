# SPDX-License-Identifier: Apache-2.0
"""Shared, explicitly separated native query clocks for UAT and comparisons."""
from __future__ import annotations

import csv
import math
from pathlib import Path


TIMING_CLOCK = "monotonic_wall_not_cpu"
TIMING_BOUNDARY = (
    "prepared_native_execution_including_io_generation_validation_and_certification_"
    "excluding_result_serialization"
)
TIMING_METRICS = (
    "engine_execution_seconds", "result_serialization_seconds", "process_other_seconds",
    "process_wall_seconds", "supervision_overhead_seconds", "supervised_wall_seconds",
    "user_cpu_seconds", "system_cpu_seconds", "validation_seconds",
)


def query_phase_timings(fields: dict, process_seconds: float, *, allow_missing: bool = False) -> dict:
    """Validate engine-owned clocks; never infer engine time from process time."""
    if (type(process_seconds) not in (int, float) or not math.isfinite(process_seconds)
            or process_seconds < 0):
        raise ValueError("process timing must be a finite nonnegative number")
    reported = fields.get("native_query_timing_reported")
    if reported != "true":
        if reported not in (None, "false"):
            raise ValueError("invalid native query timing presence flag")
        if not allow_missing:
            raise ValueError("native query timing is missing; historical binaries require --allow-missing-engine-timing")
        return dict(engine_timing_reported=False, engine_execution_seconds=None,
                    result_serialization_seconds=None, process_other_seconds=None,
                    process_wall_seconds=process_seconds)
    if (fields.get("native_query_timing_clock") != TIMING_CLOCK
            or fields.get("native_query_timing_boundary") != TIMING_BOUNDARY):
        raise ValueError("unsupported native query timing clock or boundary")

    def nanos(key):
        value = fields.get(key)
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError(f"invalid native timing: {key}")
        # A bounded counter also rejects untrusted oversized integer strings.
        if len(value) > 20 or int(value) > (1 << 64) - 1:
            raise ValueError(f"native timing is out of range: {key}")
        return int(value) / 1_000_000_000

    engine = nanos("native_query_execution_nanos")
    sink = nanos("native_result_serialization_nanos")
    if engine + sink > process_seconds + 1e-6:
        raise ValueError("engine and result clocks exceed enclosing process wall time")
    return dict(engine_timing_reported=True, engine_execution_seconds=engine,
                result_serialization_seconds=sink,
                process_other_seconds=max(0.0, process_seconds - engine - sink),
                process_wall_seconds=process_seconds)


def timing_totals(records: list[dict]) -> dict:
    """Sum raw calls only; absent observations stay unavailable, never zero."""
    totals = {}
    for key in TIMING_METRICS:
        values = [record.get(key) for record in records]
        totals[key] = (sum(values) if values and all(
            type(value) in (int, float) and math.isfinite(value) and value >= 0
            for value in values) else None)
    return dict(raw_calls=len(records),
                calls_with_engine_timing=sum(record.get("engine_timing_reported") is True for record in records),
                aggregation="sum_of_every_raw_call_not_best_of_repeats_or_workflow_elapsed",
                process_other_boundary="startup;parsing;preparation;control_plane;report_rendering;exit;unattributed_process_work",
                cpu_boundary="whole_process_cpu_overlaps_wall_time_includes_parallel_workers",
                totals=totals)


def write_timing_csv(path: Path, records: list[dict]) -> None:
    columns = ("query", "run", "role", "passed", "engine_timing_reported", *TIMING_METRICS,
               "native_peak_rss_bytes", "minor_page_faults", "major_page_faults",
               "input_block_operations", "output_block_operations", "query_statement")
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
