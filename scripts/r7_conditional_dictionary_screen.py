#!/usr/bin/env python3
"""Bounded analytical screen of conditional dictionaries in original Parquet columns.

The screen uses selected source regions only. Zstd buffer sizes are real for the
chosen PyArrow codec; every other byte total is an analytical model, not Vortex
physical size, runtime, or a full-artifact support claim.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SOURCE: Path
NUMERIC_HELPER = Path(__file__).with_name("r7_numeric_residual_screen.py")
ROW_GROUPS = (0, 113, 225)
ROWS_PER_REGION = 65_536
BLOCK_ROWS = 8_192
MAX_SELECTED_ROWS = 196_608
MAX_STDOUT_BYTES = 8 * 1024 * 1024
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1
PAIRS = (
    ("RegionID", "IPNetworkID"),
    ("RegionID", "ClientIP"),
    ("RefererCategoryID", "RefererRegionID"),
    ("URLCategoryID", "URLRegionID"),
    ("UserAgent", "UserAgentMajor"),
    ("UserAgentMajor", "UserAgentMinor"),
    ("MobilePhone", "MobilePhoneModel"),
    ("BrowserCountry", "BrowserLanguage"),
    ("URL", "URLHash"),
    ("Referer", "RefererHash"),
    ("URL", "Title"),
    ("URL", "OriginalURL"),
    ("URL", "Referer"),
)
SHARED_UNION_PAIRS = (("URL", "OriginalURL"), ("URL", "Referer"))


def _load_numeric_helper():
    if not NUMERIC_HELPER.is_file():
        raise FileNotFoundError(f"read-only numeric cost helper is missing: {NUMERIC_HELPER}")
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("r7_numeric_residual_screen_helper", NUMERIC_HELPER)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load numeric cost helper")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


NUMERIC = _load_numeric_helper()


def _is_utf8(dtype: pa.DataType) -> bool:
    return pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype)


def _supported(dtype: pa.DataType) -> bool:
    return pa.types.is_integer(dtype) or _is_utf8(dtype)


def _min_cost(values: list[int]) -> dict:
    if not values:
        raise ValueError("empty numeric sequence is unsupported")
    return NUMERIC.sequence_cost(values)


def _for_cost(values: list[int]) -> int:
    return NUMERIC.for_bytes(values)


def _zstd_size(raw: bytes) -> int:
    return int(pa.Codec("zstd", compression_level=3).compress(raw).size)


def _utf8_raw_cost(values: list[str]) -> dict:
    encoded = [value.encode("utf-8") for value in values]
    lengths = [len(value) for value in encoded]
    compressed_bytes = _zstd_size(b"".join(encoded))
    lengths_for_bytes = _for_cost(lengths)
    return {
        "zstd_level": 3,
        "zstd_compressed_utf8_bytes": compressed_bytes,
        "for_row_utf8_lengths_bytes": lengths_for_bytes,
        "minimum_bytes": compressed_bytes + lengths_for_bytes,
    }


def _domain_cost(domain: list, kind: str) -> dict:
    if not domain:
        raise ValueError("empty domain is unsupported")
    if kind == "integer":
        seq = _min_cost(domain)
        return {"kind": kind, "domain_length_bytes": 8, "integer_sequence_cost": seq,
                "minimum_bytes": int(seq["minimum_bytes"]) + 8}
    if kind == "utf8":
        encoded = [value.encode("utf-8") for value in domain]
        lengths = [len(value) for value in encoded]
        compressed = _zstd_size(b"".join(encoded))
        lengths_cost = _for_cost(lengths)
        return {"kind": kind, "domain_length_bytes": 8, "zstd_level": 3,
                "zstd_compressed_domain_utf8_bytes": compressed,
                "for_domain_utf8_lengths_bytes": lengths_cost,
                "minimum_bytes": compressed + lengths_cost + 8}
    raise ValueError(f"unsupported domain kind: {kind}")


def _kind(dtype: pa.DataType) -> str:
    if pa.types.is_integer(dtype):
        return "integer"
    if _is_utf8(dtype):
        return "utf8"
    raise ValueError(f"unsupported type: {dtype}")


def _code_domain(values: list) -> tuple[list, list[int]]:
    domain = sorted(set(values))
    id_by_value = {value: index for index, value in enumerate(domain)}
    return domain, [id_by_value[value] for value in values]


def _check_i64(values: list[int]) -> None:
    if any(value < I64_MIN or value > I64_MAX for value in values):
        raise OverflowError("observed integer value is outside signed int64 model width")


def _independent_cost(values: list, kind: str) -> dict:
    if kind == "integer":
        _check_i64(values)
        sequence = _min_cost(values)
        return {"chosen": "numeric_sequence", "numeric_sequence_cost": sequence,
                "minimum_bytes": int(sequence["minimum_bytes"])}
    domain, codes = _code_domain(values)
    domain_model = _domain_cost(domain, "utf8")
    code_cost = _min_cost(codes)
    dictionary_bytes = int(domain_model["minimum_bytes"]) + int(code_cost["minimum_bytes"]) + 8
    raw_model = _utf8_raw_cost(values)
    chosen = "raw_utf8" if int(raw_model["minimum_bytes"]) <= dictionary_bytes else "dictionary"
    return {
        "chosen": chosen,
        "raw_utf8_cost": raw_model,
        "dictionary_cost": {"domain_cost": domain_model, "row_code_cost": code_cost,
                            "dictionary_container_length_bytes": 8, "minimum_bytes": dictionary_bytes},
        "minimum_bytes": min(int(raw_model["minimum_bytes"]), dictionary_bytes),
    }


def _dictionary_cost(values: list, kind: str) -> dict:
    domain, codes = _code_domain(values)
    domain_model = _domain_cost(domain, kind)
    code_cost = _min_cost(codes)
    return {
        "domain_count": len(domain),
        "domain_cost": domain_model,
        "row_code_cost": code_cost,
        "dictionary_container_length_bytes": 8,
        "minimum_bytes": int(domain_model["minimum_bytes"]) + int(code_cost["minimum_bytes"]) + 8,
    }


def _conditional_block(a_values: list, b_values: list, a_kind: str, b_kind: str) -> dict:
    if not a_values or len(a_values) != len(b_values):
        raise ValueError("conditional block must contain matching nonempty columns")
    if any(value is None for value in a_values) or any(value is None for value in b_values):
        raise ValueError("null values are unsupported in conditional dictionary inputs")
    if a_kind == "integer":
        _check_i64(a_values)
    if b_kind == "integer":
        _check_i64(b_values)

    a_domain, a_codes = _code_domain(a_values)
    b_domain, b_codes = _code_domain(b_values)
    parent_dictionary = _dictionary_cost(a_values, a_kind)
    b_domain_model = _domain_cost(b_domain, b_kind)

    b_ids_by_a: list[set[int]] = [set() for _ in a_domain]
    for a_code, b_code in zip(a_codes, b_codes):
        b_ids_by_a[a_code].add(b_code)
    sorted_b_ids = [sorted(ids) for ids in b_ids_by_a]
    offsets = [0]
    flat_b_ids: list[int] = []
    local_code_by_a: list[dict[int, int]] = []
    max_fanout = 0
    for b_ids in sorted_b_ids:
        local_code_by_a.append({global_id: index for index, global_id in enumerate(b_ids)})
        flat_b_ids.extend(b_ids)
        offsets.append(len(flat_b_ids))
        max_fanout = max(max_fanout, len(b_ids))
    local_codes = [local_code_by_a[a_code][b_code] for a_code, b_code in zip(a_codes, b_codes)]
    for row_index, (a_code, b_code, local_code) in enumerate(zip(a_codes, b_codes, local_codes)):
        reconstructed_b_code = sorted_b_ids[a_code][local_code]
        if reconstructed_b_code != b_code or b_domain[reconstructed_b_code] != b_values[row_index]:
            raise AssertionError("conditional code/value roundtrip failed")

    offsets_cost = _min_cost(offsets)
    flat_ids_cost = _min_cost(flat_b_ids)
    local_codes_cost = _min_cost(local_codes)
    relationship_bytes = 16
    dependent_bytes = (int(b_domain_model["minimum_bytes"]) + int(offsets_cost["minimum_bytes"])
                       + int(flat_ids_cost["minimum_bytes"]) + int(local_codes_cost["minimum_bytes"])
                       + relationship_bytes)
    independent_a = _independent_cost(a_values, a_kind)
    independent_b = _independent_cost(b_values, b_kind)
    baseline = int(independent_a["minimum_bytes"]) + int(independent_b["minimum_bytes"])
    joint = int(parent_dictionary["minimum_bytes"]) + dependent_bytes
    b_baseline = int(independent_b["minimum_bytes"])
    return {
        "rows": len(a_values),
        "A_domain_count": len(a_domain),
        "B_domain_count": len(b_domain),
        "distinct_A_B_pair_count": len(flat_b_ids),
        "max_B_fanout_per_A": max_fanout,
        "parent_A_full_dictionary": parent_dictionary,
        "independent_A_baseline": independent_a,
        "independent_B_baseline": independent_b,
        "conditional_dependent_components": {
            "B_domain": b_domain_model,
            "A_to_B_offsets": offsets_cost,
            "flattened_global_B_ids": flat_ids_cost,
            "per_row_local_B_codes": local_codes_cost,
            "relationship_metadata_bytes": relationship_bytes,
            "minimum_bytes": dependent_bytes,
        },
        "baseline_independent_A_plus_B_bytes": baseline,
        "candidate_joint_parent_dictionary_plus_dependent_bytes": joint,
        "joint_saving_bytes": baseline - joint,
        "dependent_only_read_ratio_full_parent_dictionary_plus_dependent_vs_independent_B": (
            (int(parent_dictionary["minimum_bytes"]) + dependent_bytes) / b_baseline if b_baseline else None
        ),
    }


def _shared_union_block(a_values: list[str], b_values: list[str]) -> dict:
    if not a_values or len(a_values) != len(b_values):
        raise ValueError("shared-union block must contain matching nonempty columns")
    if any(value is None for value in a_values) or any(value is None for value in b_values):
        raise ValueError("null values are unsupported in shared-union inputs")
    union = sorted(set(a_values) | set(b_values))
    id_by_value = {value: index for index, value in enumerate(union)}
    a_codes = [id_by_value[value] for value in a_values]
    b_codes = [id_by_value[value] for value in b_values]
    for index, (a_code, b_code) in enumerate(zip(a_codes, b_codes)):
        if union[a_code] != a_values[index] or union[b_code] != b_values[index]:
            raise AssertionError("shared union dictionary roundtrip failed")
    union_cost = _domain_cost(union, "utf8")
    a_code_cost = _min_cost(a_codes)
    b_code_cost = _min_cost(b_codes)
    relation_bytes = 16
    candidate = int(union_cost["minimum_bytes"]) + int(a_code_cost["minimum_bytes"]) + int(b_code_cost["minimum_bytes"]) + relation_bytes
    independent_a = _independent_cost(a_values, "utf8")
    independent_b = _independent_cost(b_values, "utf8")
    baseline = int(independent_a["minimum_bytes"]) + int(independent_b["minimum_bytes"])
    return {
        "rows": len(a_values),
        "union_domain_count": len(union),
        "A_domain_count": len(set(a_values)),
        "B_domain_count": len(set(b_values)),
        "union_domain_cost": union_cost,
        "A_remapped_code_cost": a_code_cost,
        "B_remapped_code_cost": b_code_cost,
        "relationship_metadata_bytes": relation_bytes,
        "baseline_independent_A_plus_B_bytes": baseline,
        "candidate_union_domain_plus_both_remapped_streams_bytes": candidate,
        "joint_saving_bytes": baseline - candidate,
    }


def _self_test() -> None:
    repeated = _conditional_block(["parent"] * 12, ["b", "a"] * 6, "utf8", "utf8")
    assert repeated["A_domain_count"] == 1
    assert repeated["B_domain_count"] == 2
    assert repeated["distinct_A_B_pair_count"] == 2
    assert repeated["max_B_fanout_per_A"] == 2
    unicode_values = ["é", "\x00", "", "é"]
    unicode_result = _conditional_block(unicode_values, ["x", "y", "z", "x"], "utf8", "utf8")
    assert unicode_result["rows"] == 4
    assert unicode_result["parent_A_full_dictionary"]["minimum_bytes"] > 0
    negative = _conditional_block([-3, -3, 5, 5], [-8, -7, 0, 1], "integer", "integer")
    assert negative["A_domain_count"] == 2 and negative["B_domain_count"] == 4
    raw_parent = _independent_cost(["constant"] * 16, "utf8")
    forced_parent = _dictionary_cost(["constant"] * 16, "utf8")
    assert raw_parent["chosen"] == "raw_utf8"
    assert forced_parent["minimum_bytes"] > raw_parent["minimum_bytes"]
    shared = _shared_union_block(["é", "", "é"], ["", "z", "z"])
    assert shared["union_domain_count"] == 3
    assert shared["candidate_union_domain_plus_both_remapped_streams_bytes"] > 0
    try:
        _conditional_block(["a", None], ["x", "y"], "utf8", "utf8")
    except ValueError:
        pass
    else:
        raise AssertionError("null value was not rejected")
    modeled_block = {"status": "modeled", **_conditional_block(["p", "p"], ["x", "y"], "utf8", "utf8")}
    mixed_summary = _summarize_block_evidence(
        [{"region_index": 0, "row_group": 0, "block": modeled_block},
         {"region_index": 0, "row_group": 0, "block": {"status": "unsupported", "rows": 1}}],
        [{"region_index": 0, "row_group": 0, "rows_selected": 3}],
        _aggregate_regions,
    )
    assert mixed_summary["complete"] is False
    assert mixed_summary["modeled_rows"] == 2 and mixed_summary["unsupported_rows"] == 1
    assert mixed_summary["totals"] is None and mixed_summary["supported_blocks_only"] is not None
    assert len(mixed_summary["blocks"]) == 2
    complete_summary = _summarize_block_evidence(
        [{"region_index": 0, "row_group": 0, "block": modeled_block}],
        [{"region_index": 0, "row_group": 0, "rows_selected": 2}],
        _aggregate_regions,
    )
    assert complete_summary["complete"] is True
    assert complete_summary["modeled_rows"] == 2 and complete_summary["unsupported_rows"] == 0
    assert complete_summary["totals"] is not None
    empty_summary = _summarize_block_evidence(
        [], [{"region_index": 0, "row_group": 0, "rows_selected": 0}], _aggregate_regions
    )
    assert empty_summary["complete"] is False and empty_summary["expected_rows"] == 0
    assert empty_summary["empty_regions"]
    print(json.dumps({"self_test": "passed", "source_rows_read": 0, "dataset_opened": False}))


def _generation(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"device": stat.st_dev, "inode": stat.st_ino, "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns, "ctime_ns": stat.st_ctime_ns}


def _read_region(parquet: pq.ParquetFile, row_group: int, columns: list[str]) -> tuple[pa.Table, dict[str, int], int]:
    batches = []
    null_counts = {name: 0 for name in columns}
    rows = 0
    for batch in parquet.iter_batches(batch_size=ROWS_PER_REGION, row_groups=[row_group], columns=columns,
                                      use_threads=False, use_pandas_metadata=False):
        remaining = ROWS_PER_REGION - rows
        if remaining <= 0:
            break
        if batch.num_rows > remaining:
            batch = batch.slice(0, remaining)
        for index, name in enumerate(columns):
            null_counts[name] += batch.column(index).null_count
        batches.append(batch)
        rows += batch.num_rows
        if rows >= ROWS_PER_REGION:
            break
    if not batches:
        return pa.table({name: pa.array([], type=pa.null()) for name in columns}), null_counts, 0
    return pa.Table.from_batches(batches), null_counts, rows


def _aggregate_regions(blocks: list[dict]) -> dict:
    cost_keys = (
        "baseline_independent_A_plus_B_bytes",
        "candidate_joint_parent_dictionary_plus_dependent_bytes",
        "joint_saving_bytes",
    )
    result = {key: sum(int(block[key]) for block in blocks) for key in cost_keys}
    result.update({
        "rows_sum_across_blocks": sum(block["rows"] for block in blocks),
        "A_domain_count_sum_across_blocks": sum(block["A_domain_count"] for block in blocks),
        "B_domain_count_sum_across_blocks": sum(block["B_domain_count"] for block in blocks),
        "distinct_A_B_pair_count_sum_across_blocks": sum(block["distinct_A_B_pair_count"] for block in blocks),
        "max_B_fanout_per_A_across_blocks": max((block["max_B_fanout_per_A"] for block in blocks), default=0),
    })
    baseline_b = sum(int(block["independent_B_baseline"]["minimum_bytes"]) for block in blocks)
    charged = sum(int(block["parent_A_full_dictionary"]["minimum_bytes"])
                  + int(block["conditional_dependent_components"]["minimum_bytes"]) for block in blocks)
    result["dependent_only_read_ratio_full_parent_dictionary_plus_dependent_vs_independent_B"] = charged / baseline_b if baseline_b else None
    return result


def _aggregate_shared(blocks: list[dict]) -> dict:
    return {
        "rows_sum_across_blocks": sum(block["rows"] for block in blocks),
        "union_domain_count_sum_across_blocks": sum(block["union_domain_count"] for block in blocks),
        "A_domain_count_sum_across_blocks": sum(block["A_domain_count"] for block in blocks),
        "B_domain_count_sum_across_blocks": sum(block["B_domain_count"] for block in blocks),
        "baseline_independent_A_plus_B_bytes": sum(block["baseline_independent_A_plus_B_bytes"] for block in blocks),
        "candidate_union_domain_plus_both_remapped_streams_bytes": sum(block["candidate_union_domain_plus_both_remapped_streams_bytes"] for block in blocks),
        "joint_saving_bytes": sum(block["joint_saving_bytes"] for block in blocks),
    }


def _summarize_block_evidence(block_entries: list[dict], regions: list[dict], aggregate) -> dict:
    """Keep supported subtotal separate from whole-result totals unless coverage is complete."""
    expected_rows = sum(region["rows_selected"] for region in regions)
    modeled_blocks = [entry["block"] for entry in block_entries if entry["block"]["status"] == "modeled"]
    unsupported_blocks = [entry["block"] for entry in block_entries if entry["block"]["status"] == "unsupported"]
    modeled_rows = sum(block["rows"] for block in modeled_blocks)
    unsupported_rows = max(0, expected_rows - modeled_rows)
    empty_regions = []
    per_region = []
    for region in regions:
        index = region["region_index"]
        expected = region["rows_selected"]
        entries = [entry for entry in block_entries if entry["region_index"] == index]
        region_modeled = [entry["block"] for entry in entries if entry["block"]["status"] == "modeled"]
        region_unsupported = [entry["block"] for entry in entries if entry["block"]["status"] == "unsupported"]
        region_modeled_rows = sum(block["rows"] for block in region_modeled)
        region_unsupported_rows = max(0, expected - region_modeled_rows)
        region_complete = expected > 0 and region_modeled_rows == expected and region_unsupported_rows == 0
        if expected == 0 or not entries:
            empty_regions.append({"region_index": index, "row_group": region["row_group"],
                                  "expected_rows": expected, "recorded_blocks": len(entries)})
        supported_totals = aggregate(region_modeled) if region_modeled else None
        per_region.append({
            "region_index": index,
            "row_group": region["row_group"],
            "expected_rows": expected,
            "modeled_rows": region_modeled_rows,
            "unsupported_rows": region_unsupported_rows,
            "modeled_blocks": len(region_modeled),
            "unsupported_blocks": len(region_unsupported),
            "recorded_blocks": len(entries),
            "complete": region_complete,
            "supported_blocks_only": supported_totals,
            "totals": supported_totals if region_complete else None,
        })
    complete = (
        expected_rows > 0
        and all(region["rows_selected"] > 0 for region in regions)
        and modeled_rows == expected_rows
        and unsupported_rows == 0
        and not empty_regions
        and len(modeled_blocks) + len(unsupported_blocks) == len(block_entries)
    )
    supported_totals = aggregate(modeled_blocks) if modeled_blocks else None
    return {
        "expected_rows": expected_rows,
        "modeled_rows": modeled_rows,
        "unsupported_rows": unsupported_rows,
        "modeled_blocks": len(modeled_blocks),
        "unsupported_blocks": len(unsupported_blocks),
        "empty_regions": empty_regions,
        "complete": complete,
        "per_region": per_region,
        "supported_blocks_only": supported_totals,
        "totals": supported_totals if complete else None,
        "blocks": block_entries,
    }


def _run() -> dict:
    if not SOURCE.is_file():
        raise FileNotFoundError(f"fixed Parquet source missing: {SOURCE}")
    before = _generation(SOURCE)
    parquet = pq.ParquetFile(SOURCE)
    schema = parquet.schema_arrow
    fields = {field.name: field for field in schema}
    if len(fields) != len(schema.names):
        raise ValueError("duplicate top-level Parquet names are unsupported")
    expected_columns = sorted({column for pair in PAIRS for column in pair})
    supported_columns = [name for name in expected_columns if name in fields and _supported(fields[name].type)]
    schema_report = [{"name": name, "type": str(fields[name].type), "supported_type": _supported(fields[name].type)}
                     for name in expected_columns if name in fields]
    missing_columns = [name for name in expected_columns if name not in fields]
    for rg in ROW_GROUPS:
        if rg >= parquet.metadata.num_row_groups:
            raise ValueError(f"required row group {rg} unavailable; file has {parquet.metadata.num_row_groups}")

    pair_results = {pair: {"source": pair[0], "target": pair[1], "regions": []} for pair in PAIRS}
    shared_results = {pair: {"source": pair[0], "target": pair[1], "regions": []} for pair in SHARED_UNION_PAIRS}
    selected_rows = 0
    null_counts_by_region = []
    unsupported_by_column: dict[str, list[str]] = {}

    for region_index, row_group in enumerate(ROW_GROUPS):
        table, null_counts, region_rows = _read_region(parquet, row_group, supported_columns)
        selected_rows += region_rows
        if selected_rows > MAX_SELECTED_ROWS:
            raise AssertionError("selected row cap exceeded")
        null_counts_by_region.append({"region_index": region_index, "row_group": row_group,
                                      "rows_selected": region_rows,
                                      "null_counts_by_column": null_counts})
        for name, count in null_counts.items():
            if count:
                unsupported_by_column.setdefault(name, []).append(f"nulls observed in row_group {row_group}: {count}")

        for block_start in range(0, region_rows, BLOCK_ROWS):
            block_rows = min(BLOCK_ROWS, region_rows - block_start)
            a_cache: dict[str, list] = {}
            for source, target in PAIRS:
                result = pair_results[(source, target)]
                block_record = {"block_start": block_start, "rows": block_rows}
                if source in missing_columns or target in missing_columns:
                    block_record.update({"status": "unsupported", "reason": "required column missing"})
                elif source not in fields or target not in fields or not _supported(fields[source].type) or not _supported(fields[target].type):
                    block_record.update({"status": "unsupported", "reason": "only integer and UTF8 fields are modeled"})
                elif source in unsupported_by_column or target in unsupported_by_column:
                    block_record.update({"status": "unsupported", "reason": "null observed in selected input column"})
                elif region_rows == 0:
                    block_record.update({"status": "unsupported", "reason": "selected row group has zero rows"})
                else:
                    if source not in a_cache:
                        a_cache[source] = table.column(source).slice(block_start, block_rows).to_pylist()
                    if target not in a_cache:
                        a_cache[target] = table.column(target).slice(block_start, block_rows).to_pylist()
                    try:
                        block_record.update({"status": "modeled", **_conditional_block(
                            a_cache[source], a_cache[target], _kind(fields[source].type), _kind(fields[target].type)
                        )})
                    except (OverflowError, TypeError, ValueError) as error:
                        block_record.update({"status": "unsupported", "reason": f"{type(error).__name__}: {error}"})
                result["regions"].append({"region_index": region_index, "row_group": row_group, "block": block_record})

            for source, target in SHARED_UNION_PAIRS:
                result = shared_results[(source, target)]
                block_record = {"block_start": block_start, "rows": block_rows}
                if source in missing_columns or target in missing_columns:
                    block_record.update({"status": "unsupported", "reason": "required column missing"})
                elif source not in fields or target not in fields or not _is_utf8(fields[source].type) or not _is_utf8(fields[target].type):
                    block_record.update({"status": "unsupported", "reason": "shared union only models UTF8/UTF8 pairs"})
                elif source in unsupported_by_column or target in unsupported_by_column:
                    block_record.update({"status": "unsupported", "reason": "null observed in selected input column"})
                elif region_rows == 0:
                    block_record.update({"status": "unsupported", "reason": "selected row group has zero rows"})
                else:
                    if source not in a_cache:
                        a_cache[source] = table.column(source).slice(block_start, block_rows).to_pylist()
                    if target not in a_cache:
                        a_cache[target] = table.column(target).slice(block_start, block_rows).to_pylist()
                    try:
                        block_record.update({"status": "modeled", **_shared_union_block(a_cache[source], a_cache[target])})
                    except (TypeError, ValueError) as error:
                        block_record.update({"status": "unsupported", "reason": f"{type(error).__name__}: {error}"})
                result["regions"].append({"region_index": region_index, "row_group": row_group, "block": block_record})

    for pair, result in pair_results.items():
        entries = result.pop("regions")
        result.update(_summarize_block_evidence(entries, null_counts_by_region, _aggregate_regions))

    for pair, result in shared_results.items():
        entries = result.pop("regions")
        result.update(_summarize_block_evidence(entries, null_counts_by_region, _aggregate_shared))

    after = _generation(SOURCE)
    if before != after:
        raise RuntimeError("source generation changed during the conditional dictionary screen")
    output = {
        "schema_version": "shardloom.r7_conditional_dictionary_screen.v1",
        "analysis_provider": "PyArrow Parquet reference reader; not ShardLoom execution",
        "source_path": str(SOURCE),
        "source_generation_before": before,
        "source_generation_after": after,
        "source_generation_unchanged": True,
        "pyarrow_version": pa.__version__,
        "schema": schema_report,
        "missing_columns": missing_columns,
        "unsupported_columns": unsupported_by_column,
        "row_group_count": parquet.metadata.num_row_groups,
        "regions": null_counts_by_region,
        "selected_rows_total": selected_rows,
        "selected_rows_maximum": MAX_SELECTED_ROWS,
        "block_rows": BLOCK_ROWS,
        "threaded_reads": False,
        "pairs": list(pair_results.values()),
        "shared_union_estimates": list(shared_results.values()),
        "model_scope": {
            "conditional_parent_reference": "always charges the full independent dictionary A (A domain + exact A row codes + 8-byte container length), even if independent A baseline chooses a cheaper raw/For representation",
            "conditional_dependent": "B domain + A-to-B offsets + flattened sorted global B IDs + exact per-row local B codes + 16-byte relationship metadata",
            "offsets": "CSR offsets: zero followed by exclusive cumulative flattened-ID lengths",
            "utf8_domain": "actual PyArrow Zstd level 3 compressed concatenated UTF8 bytes + For byte lengths + 8-byte domain length",
            "utf8_independent_raw": "actual PyArrow Zstd level 3 compressed concatenated row UTF8 bytes + For row byte lengths",
            "utf8_independent_dictionary": "domain cost + exact code sequence cost + 8-byte dictionary container length",
            "integer_baseline": "reuses r7_numeric_residual_screen.py sequence_cost",
            "shared_union": "pairwise UTF8 union domain for URL->OriginalURL and URL->Referer + both remapped row-code streams + 16-byte relationship metadata; separate estimate, not a persistence claim",
            "aggregation": "totals are pair-local sums across blocks; pairs overlap and must not be summed together",
        },
        "exclusions": [
            "No native Vortex conditional dictionary encoding, persistence, or provider exists in this measurement.",
            "This is not actual Vortex serialized size, elapsed performance, whole-artifact evidence, or a support claim.",
            "UTF8 Zstd compressed buffer sizes are actual for PyArrow Zstd level 3; For/Dict code and relationship byte costs are analytical.",
            "Vortex sparse/patch/other encodings, framing, statistics, alignment, and headers are not modeled.",
            "Only first 65536 rows of each of row groups 0, 113, and 225 are read; sample domain counts are not global.",
            "Nulls, unsupported physical types, missing fields, and integers outside signed int64 are reported unsupported.",
        ],
    }
    packed = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    if len(packed.encode("utf-8")) > MAX_STDOUT_BYTES:
        raise RuntimeError("compact JSON output exceeds 8 MiB cap")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run tiny synthetic tests; never opens Parquet source")
    parser.add_argument("--source", type=Path, help="Parquet source path (required unless --self-test)")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    if args.source is None:
        parser.error("--source is required unless --self-test")
    global SOURCE
    SOURCE = args.source
    print(json.dumps(_run(), ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
