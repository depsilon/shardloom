#!/usr/bin/env python3
"""Bounded analytical screen of numeric original-Parquet-column residual pairs.

No dataset values are written. The byte estimates are a deliberately small
For/Dict/RunEnd model, not measurements of Vortex output or runtime.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SOURCE: Path
ROW_GROUPS = (0, 113, 225)
ROWS_PER_REGION = 65_536
BLOCK_ROWS = 8_192
PROBE_ROWS = 2_048
MAX_SELECTED_ROWS = 196_608
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1
EXPLICIT_PAIRS = (
    ("EventTime", "ClientEventTime"),
    ("EventTime", "LocalEventTime"),
    ("ResolutionWidth", "WindowClientWidth"),
    ("ResolutionHeight", "WindowClientHeight"),
    ("ClientIP", "RemoteIP"),
    ("ClientIP", "IPNetworkID"),
    ("ResponseStartTiming", "ResponseEndTiming"),
    ("FetchTiming", "ResponseStartTiming"),
    ("ConnectTiming", "SendTiming"),
)


def for_bytes(values: list[int]) -> int:
    """8-byte minimum, 1-byte width, packed offsets at range.bit_length()."""
    if not values:
        return 9
    low = min(values)
    bits = (max(values) - low).bit_length()
    return 8 + 1 + (len(values) * bits + 7) // 8


def sequence_cost(values: list[int]) -> dict[str, int | str]:
    """Return the minimum of the specified For, Dict, and RunEnd byte models."""
    if not values:
        raise ValueError("empty sequences are outside this screen's model")
    if any(value < I64_MIN or value > I64_MAX for value in values):
        raise OverflowError("modeled values must be representable as int64")

    unique = sorted(set(values))
    code_by_value = {value: index for index, value in enumerate(unique)}
    codes = [code_by_value[value] for value in values]
    runends: list[int] = []
    runvalues: list[int] = []
    previous = values[0]
    for index, value in enumerate(values[1:], 1):
        if value != previous:
            runends.append(index)
            runvalues.append(previous)
            previous = value
    runends.append(len(values))
    runvalues.append(previous)

    costs = {
        "For": for_bytes(values),
        "Dict": 8 + for_bytes(unique) + for_bytes(codes),
        "RunEnd": 8 + for_bytes(runends) + for_bytes(runvalues),
    }
    encoding = min(costs, key=lambda name: (costs[name], name))
    return {
        "rows": len(values),
        "for_bytes": costs["For"],
        "dict_bytes": costs["Dict"],
        "runend_bytes": costs["RunEnd"],
        "minimum_bytes": costs[encoding],
        "minimum_encoding": encoding,
    }


def checked_residual(source: list[int], target: list[int]) -> tuple[list[int] | None, dict[str, int] | None]:
    """Compute target-source with Python integers; return None if int64 unsafe."""
    if len(source) != len(target):
        raise ValueError("source and target lengths differ")
    residual = [y - x for x, y in zip(source, target)]
    low, high = min(residual), max(residual)
    if low < I64_MIN or high > I64_MAX:
        return None, {"minimum": low, "maximum": high}
    return residual, {"minimum": low, "maximum": high}


def probe_indices(rows: int, count: int) -> list[int]:
    if rows <= 0 or count <= 0:
        raise ValueError("probe dimensions must be positive")
    n = min(rows, count)
    if n == 1:
        return [0]
    return [(index * (rows - 1)) // (n - 1) for index in range(n)]


def assert_roundtrip(source: list[int], target: list[int], residual: list[int]) -> None:
    if len(source) != len(target) or len(source) != len(residual):
        raise AssertionError("roundtrip sequences have different lengths")
    for x, y, delta in zip(source, target, residual):
        if x + delta != y:
            raise AssertionError("exact target != source + residual")


def _self_test() -> None:
    assert for_bytes([7, 7, 7, 7]) == 9
    assert sequence_cost([7, 7, 7, 7])["minimum_bytes"] == 9
    source = [10, 20, 30, 40]
    target = [9, 18, 27, 36]
    residual, bounds = checked_residual(source, target)
    assert residual == [-1, -2, -3, -4]
    assert bounds == {"minimum": -4, "maximum": -1}
    assert_roundtrip(source, target, residual)
    assert sequence_cost([2, 2, 2, 5])["runend_bytes"] == 8 + for_bytes([3, 4]) + for_bytes([2, 5])
    unsafe, extremes = checked_residual([I64_MIN], [I64_MAX])
    assert unsafe is None and extremes == {"minimum": (1 << 64) - 1, "maximum": (1 << 64) - 1}
    try:
        sequence_cost([I64_MIN - 1])
    except OverflowError:
        pass
    else:
        raise AssertionError("sequence model accepted a value outside int64")
    tiny = _exact_pair("x", "y", [{"x": [10, 20, 30], "y": [9, 18, 27]}])
    assert tiny["saving_bytes"] == tiny["baseline_bytes"] - tiny["joint_bytes"]
    assert tiny["regions"][0]["blocks"][0]["residual_minimum"] == -3
    global BLOCK_ROWS
    original_block_rows = BLOCK_ROWS
    try:
        BLOCK_ROWS = 2
        mixed = _exact_pair(
            "x", "y",
            [{"x": [0, 0, I64_MIN], "y": [1, 2, I64_MAX]}],
        )
    finally:
        BLOCK_ROWS = original_block_rows
    assert mixed["complete"] is False
    assert mixed["modeled_rows"] == 2 and mixed["unsupported_rows"] == 1
    assert mixed["regions"][0]["modeled_rows"] == 2
    assert mixed["regions"][0]["unsupported_rows"] == 1
    assert mixed["saving_bytes"] is None
    assert mixed["dependent_only_read_ratio_vs_original_target"] is None
    partial = mixed["supported_blocks_only"]
    assert isinstance(partial["saving_bytes"], int)
    assert isinstance(partial["dependent_only_read_ratio_vs_original_target_supported_blocks_only"], float)
    assert mixed["regions"][0]["blocks"][0].get("saving_bytes") is not None
    assert mixed["regions"][0]["blocks"][1].get("unsupported")
    assert probe_indices(65_536, 2_048)[0] == 0
    assert probe_indices(65_536, 2_048)[-1] == 65_535
    assert len(set(probe_indices(65_536, 2_048))) == 2_048
    print(json.dumps({"self_test": "passed", "source_rows_read": 0, "dataset_opened": False}))


def _generation(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ctime_ns": stat.st_ctime_ns,
    }


def _read_region(parquet: pq.ParquetFile, row_group: int, columns: list[str]) -> tuple[dict[str, list[int]], int]:
    values = {name: [] for name in columns}
    for batch in parquet.iter_batches(
        batch_size=ROWS_PER_REGION,
        row_groups=[row_group],
        columns=columns,
        use_threads=False,
        use_pandas_metadata=False,
    ):
        remaining = ROWS_PER_REGION - len(next(iter(values.values()))) if values else ROWS_PER_REGION
        if remaining <= 0:
            break
        if batch.num_rows > remaining:
            batch = batch.slice(0, remaining)
        for index, name in enumerate(columns):
            array = batch.column(index)
            if array.null_count:
                raise ValueError(f"nulls in selected integer column {name!r} row_group={row_group}")
            values[name].extend(array.to_pylist())
        if len(next(iter(values.values()))) >= ROWS_PER_REGION:
            break
    rows = len(next(iter(values.values()))) if values else 0
    for name, sequence in values.items():
        if len(sequence) != rows:
            raise AssertionError(f"column row-count mismatch for {name!r}")
    return values, rows


def _int64_columns(regions: list[dict[str, list[int]]], columns: list[str]) -> tuple[list[str], dict[str, str]]:
    allowed: list[str] = []
    exclusions: dict[str, str] = {}
    for name in columns:
        if any(value < I64_MIN or value > I64_MAX for region in regions for value in region[name]):
            exclusions[name] = "observed input value outside signed int64 model width"
        else:
            allowed.append(name)
    return allowed, exclusions


def _screen(regions: list[dict[str, list[int]]], columns: list[str]) -> tuple[list[dict], set[tuple[str, str]], dict[str, list[dict]]]:
    retained: set[tuple[str, str]] = set(EXPLICIT_PAIRS)
    rankings: list[dict] = []
    overflow_exclusions: dict[str, list[dict]] = {}
    if not columns:
        return rankings, retained, overflow_exclusions
    for region_index, region in enumerate(regions):
        row_count = len(region[columns[0]])
        indexes = probe_indices(row_count, PROBE_ROWS)
        probes = {name: [region[name][index] for index in indexes] for name in columns}
        for target_name in columns:
            candidates: list[dict] = []
            for source_name in columns:
                if source_name == target_name:
                    continue
                residual, bounds = checked_residual(probes[source_name], probes[target_name])
                if residual is None:
                    overflow_exclusions.setdefault(f"{source_name}->{target_name}", []).append(
                        {"region_index": region_index, "row_group": ROW_GROUPS[region_index], "residual_bounds": bounds}
                    )
                    continue
                saving = for_bytes(probes[target_name]) - for_bytes(residual)
                if saving > 0:
                    candidates.append({"source": source_name, "target": target_name, "probe_for_saving_bytes": saving})
            candidates.sort(key=lambda item: (-item["probe_for_saving_bytes"], item["source"]))
            top = candidates[:2]
            retained.update((item["source"], item["target"]) for item in top)
            rankings.append({
                "region_index": region_index,
                "row_group": ROW_GROUPS[region_index],
                "target": target_name,
                "probe_rows": len(indexes),
                "top_positive_for_reference_sources": top,
            })
    return rankings, retained, overflow_exclusions


def _exact_pair(source: str, target: str, regions: list[dict[str, list[int]]]) -> dict:
    pair = {"source": source, "target": target, "residual": f"{target}-{source}", "regions": [], "unsupported": []}
    totals = {"baseline_bytes": 0, "joint_bytes": 0, "saving_bytes": 0, "charged_source_plus_residual_bytes": 0, "original_target_bytes": 0}
    modeled_rows = 0
    unsupported_rows = 0
    for region_index, region in enumerate(regions):
        source_values, target_values = region[source], region[target]
        block_records = []
        region_totals = {"baseline_bytes": 0, "joint_bytes": 0, "saving_bytes": 0, "charged_source_plus_residual_bytes": 0, "original_target_bytes": 0}
        region_modeled_rows = 0
        region_unsupported_rows = 0
        for block_start in range(0, len(source_values), BLOCK_ROWS):
            x = source_values[block_start:block_start + BLOCK_ROWS]
            y = target_values[block_start:block_start + BLOCK_ROWS]
            residual, bounds = checked_residual(x, y)
            block = {"block_start": block_start, "rows": len(x)}
            if residual is None:
                block["unsupported"] = "residual exceeds signed int64 width"
                block["residual_bounds"] = bounds
                pair["unsupported"].append({"region_index": region_index, "row_group": ROW_GROUPS[region_index], **block})
                region_unsupported_rows += len(x)
                unsupported_rows += len(x)
                block_records.append(block)
                continue
            assert_roundtrip(x, y, residual)
            source_cost = sequence_cost(x)
            target_cost = sequence_cost(y)
            residual_cost = sequence_cost(residual)
            baseline = int(source_cost["minimum_bytes"]) + int(target_cost["minimum_bytes"])
            joint = int(source_cost["minimum_bytes"]) + int(residual_cost["minimum_bytes"]) + 16
            saving = baseline - joint
            charged = int(source_cost["minimum_bytes"]) + int(residual_cost["minimum_bytes"])
            original = int(target_cost["minimum_bytes"])
            block.update({
                "residual_minimum": bounds["minimum"], "residual_maximum": bounds["maximum"],
                "source_cost": source_cost, "target_cost": target_cost, "residual_cost": residual_cost,
                "baseline_source_plus_target_bytes": baseline,
                "joint_source_plus_residual_plus_16_byte_relation_bytes": joint,
                "saving_bytes": saving,
                "dependent_only_source_plus_residual_bytes": charged,
                "dependent_only_read_ratio_vs_original_target": (charged / original) if original else None,
            })
            for name, value in (("baseline_bytes", baseline), ("joint_bytes", joint), ("saving_bytes", saving),
                                ("charged_source_plus_residual_bytes", charged), ("original_target_bytes", original)):
                region_totals[name] += value
                totals[name] += value
            region_modeled_rows += len(x)
            modeled_rows += len(x)
            block_records.append(block)
        region_complete = region_unsupported_rows == 0
        region_ratio = (region_totals["charged_source_plus_residual_bytes"] / region_totals["original_target_bytes"]
                        if region_complete and region_totals["original_target_bytes"] else None)
        pair["regions"].append({
            "region_index": region_index, "row_group": ROW_GROUPS[region_index], "rows": len(source_values),
            "complete": region_complete,
            "modeled_rows": region_modeled_rows,
            "unsupported_rows": region_unsupported_rows,
            "blocks": block_records,
            "supported_blocks_only": {
                **region_totals,
                "dependent_only_read_ratio_vs_original_target_supported_blocks_only": (
                    region_totals["charged_source_plus_residual_bytes"] / region_totals["original_target_bytes"]
                    if region_totals["original_target_bytes"] else None
                ),
            },
            "saving_bytes": region_totals["saving_bytes"] if region_complete else None,
            "dependent_only_read_ratio_vs_original_target": region_ratio,
        })
    pair_complete = unsupported_rows == 0
    pair.update({
        "complete": pair_complete,
        "modeled_rows": modeled_rows,
        "unsupported_rows": unsupported_rows,
        "supported_blocks_only": {
            **totals,
            "dependent_only_read_ratio_vs_original_target_supported_blocks_only": (
                totals["charged_source_plus_residual_bytes"] / totals["original_target_bytes"]
                if totals["original_target_bytes"] else None
            ),
        },
        "baseline_bytes": totals["baseline_bytes"] if pair_complete else None,
        "joint_bytes": totals["joint_bytes"] if pair_complete else None,
        "saving_bytes": totals["saving_bytes"] if pair_complete else None,
        "dependent_only_read_ratio_vs_original_target": (
            totals["charged_source_plus_residual_bytes"] / totals["original_target_bytes"]
            if pair_complete and totals["original_target_bytes"] else None
        ),
        "whole_pair_comparison_unsupported_reason": (
            None if pair_complete else "one or more selected blocks have residuals outside signed int64 width"
        ),
    })
    return pair


def _run_screen() -> dict:
    if not SOURCE.is_file():
        raise FileNotFoundError(f"fixed Parquet input not found: {SOURCE}")
    before = _generation(SOURCE)
    parquet = pq.ParquetFile(SOURCE)
    schema = parquet.schema_arrow
    names = schema.names
    if len(names) != len(set(names)):
        raise ValueError("duplicate top-level Parquet field names are unsupported")
    integer_columns = [field.name for field in schema if pa.types.is_integer(field.type)]
    integer_schema = [{"name": field.name, "type": str(field.type)} for field in schema if pa.types.is_integer(field.type)]
    if not integer_columns:
        raise ValueError("no integer columns found")
    if any(index >= parquet.metadata.num_row_groups for index in ROW_GROUPS):
        raise ValueError(f"required row group missing; file has {parquet.metadata.num_row_groups}")

    regions: list[dict[str, list[int]]] = []
    region_info = []
    selected_rows = 0
    for row_group in ROW_GROUPS:
        values, rows = _read_region(parquet, row_group, integer_columns)
        if rows > ROWS_PER_REGION:
            raise AssertionError("per-region row cap exceeded")
        selected_rows += rows
        if selected_rows > MAX_SELECTED_ROWS:
            raise AssertionError("total selected-row cap exceeded")
        regions.append(values)
        region_info.append({"row_group": row_group, "rows_selected": rows, "block_rows": BLOCK_ROWS})
    int64_columns, type_exclusions = _int64_columns(regions, integer_columns)
    rankings, retained, probe_overflow = _screen(regions, int64_columns)

    explicit_presence = []
    for source, target in EXPLICIT_PAIRS:
        exists = source in integer_columns and target in integer_columns
        explicit_presence.append({"source": source, "target": target, "present_as_integer_columns": exists})
        if exists and (source not in int64_columns or target not in int64_columns):
            continue
    available_pairs = sorted((source, target) for source, target in retained
                             if source in int64_columns and target in int64_columns and source != target)
    results = [_exact_pair(source, target, regions) for source, target in available_pairs]
    after = _generation(SOURCE)
    output = {
        "schema_version": "shardloom.r7_numeric_residual_screen.v1",
        "analysis_provider": "PyArrow Parquet reference reader; not ShardLoom execution",
        "pyarrow_version": pa.__version__,
        "source": str(SOURCE),
        "source_generation_before": before,
        "source_generation_after": after,
        "source_generation_unchanged": before == after,
        "source_parquet_schema": integer_schema,
        "row_group_count": parquet.metadata.num_row_groups,
        "regions": region_info,
        "selected_rows_total": selected_rows,
        "selected_rows_assertion_max": MAX_SELECTED_ROWS,
        "nulls_present_in_selected_integer_inputs": False,
        "dataset_values_written": False,
        "query_executed": False,
        "all_ordered_distinct_integer_column_pairs": max(0, len(integer_columns) * (len(integer_columns) - 1)),
        "int64_screened_ordered_distinct_pairs_per_region": max(0, len(int64_columns) * (len(int64_columns) - 1)),
        "input_width_excluded_ordered_pair_count_per_region": max(
            0, len(integer_columns) * (len(integer_columns) - 1) - len(int64_columns) * (len(int64_columns) - 1)
        ),
        "probe_rows_per_region": PROBE_ROWS,
        "screen_rankings_per_target_region": rankings,
        "retained_pair_count": len(results),
        "explicit_pairs": explicit_presence,
        "type_exclusions": type_exclusions,
        "probe_overflow_exclusions": probe_overflow,
        "retained_pairs": results,
        "model": {
            "For": "9 bytes + ceil(n * bit_length(max-min) / 8); 8-byte minimum and 1-byte bit width",
            "Dict": "8-byte length + For(sorted unique int64 values) + For(exact integer codes)",
            "RunEnd": "8-byte length + For(exact exclusive run-end positions) + For(run representative values)",
            "candidate_joint": "minimum(source) + minimum(residual) + 16 bytes per block relationship metadata",
            "baseline": "minimum(source) + minimum(target)",
            "dependent_only_read_ratio": "(minimum(source) + minimum(residual)) / minimum(original target); excludes relation metadata",
            "interpretation": "analytical serialized byte model only; not actual Vortex encoded size, IO bytes, elapsed performance, or a full-artifact claim",
        },
        "model_exclusions": [
            "Vortex sparse/patch encodings and other physical encodings are not modeled.",
            "Vortex headers, segment framing, statistics, alignment, and encoding overheads differ from this model.",
            "No native residual encoding, scan/provider, or query path is implemented by this script.",
            "Probe selection may be optimistic; each region samples only its first 65536 rows from one row group.",
            "Selected regions are a sample, not representative proof for the whole Parquet artifact.",
            "Only signed-int64 representable inputs and int64-safe residuals are modeled; unsafe width pairs are excluded.",
        ],
    }
    if before != after:
        raise RuntimeError("Parquet source generation changed during screening")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run tiny in-memory model tests; never opens the source")
    parser.add_argument("--source", type=Path, help="Parquet source path (required unless --self-test)")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    if args.source is None:
        parser.error("--source is required unless --self-test")
    global SOURCE
    SOURCE = args.source
    result = _run_screen()
    encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > 8 * 1024 * 1024:
        raise RuntimeError("result exceeds 8 MiB stdout cap")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
