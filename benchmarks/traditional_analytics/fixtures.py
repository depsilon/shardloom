# SPDX-License-Identifier: Apache-2.0
"""Deterministic comparison fixtures, independent of any query execution."""
from __future__ import annotations
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmark_models import (
    BenchmarkUnsupported, DatasetPaths, FORMAT_ORDER, GENERATED_DATASET_PROFILES,
)
from local_uat_storage import require_local_path


FIXTURE_COLUMN_DTYPES = {
    "id": "int64",
    "group_key": "int64",
    "dim_key": "int64",
    "value": "int64",
    "flag": "int64",
    "partition_year": "int64",
    "partition_month": "int64",
    "cluster_bucket": "int64",
    "file_bucket": "int64",
    "cdc_sequence": "int64",
    "metric": "float64",
    "weight": "float64",
    "nested_score": "float64",
    "optional_metric_v2": "float64",
    "renamed_metric_candidate": "float64",
    "is_deleted": "boolean",
    "category": "utf8",
    "dim_label": "utf8",
    "event_date": "utf8",
    "schema_version_tag": "utf8",
    "raw_event_time": "utf8",
    "dirty_numeric": "utf8",
    "dirty_flag": "utf8",
    "effective_ts": "utf8",
    "nested_payload": "utf8",
    "nested_group": "utf8",
    "cdc_op": "utf8",
    "op": "utf8",
}


def fixture_column_dtype(column: str) -> str:
    """Return the declared logical type for a generated fixture column."""
    dtype = FIXTURE_COLUMN_DTYPES.get(column)
    if dtype is not None:
        return dtype
    for prefix, prefix_dtype in (
        ("extra_metric_", "float64"),
        ("nullable_metric_", "float64"),
        ("nullable_category_", "utf8"),
    ):
        suffix = column.removeprefix(prefix) if column.startswith(prefix) else ""
        if suffix.isdigit():
            return prefix_dtype
    raise ValueError(f"unknown generated fixture column: {column}")


def fixture_columns_for_role(paths: DatasetPaths, role: str) -> tuple[str, ...]:
    if role in {"fact", "parts"}:
        return (
            "id", "group_key", "dim_key", "value", "metric", "flag", "category",
            *paths.fact_extra_columns,
        )
    if role == "dim":
        return ("dim_key", "dim_label", "weight")
    if role == "delta":
        return ("id", "op", "value", "metric", "effective_ts")
    raise ValueError(f"unknown generated fixture source role: {role}")


def fixture_schema_for_columns(columns: tuple[str, ...] | list[str]) -> str:
    if len(set(columns)) != len(columns):
        raise ValueError("generated fixture schema contains duplicate columns")
    return ",".join(f"{column}:{fixture_column_dtype(column)}" for column in columns)


def fixture_schema_for_role(paths: DatasetPaths, role: str) -> str:
    return fixture_schema_for_columns(fixture_columns_for_role(paths, role))


def fixture_columns_from_csv(path: Path) -> tuple[str, ...]:
    with path.open("r", newline="", encoding="utf-8") as source:
        try:
            return tuple(next(csv.reader(source)))
        except StopIteration as exc:
            raise ValueError(f"generated fixture CSV is missing a header: {path}") from exc


def fixture_arrow_column_types(pa: Any, columns: tuple[str, ...] | list[str]) -> dict[str, Any]:
    arrow_types = {
        "int64": pa.int64(),
        "float64": pa.float64(),
        "boolean": pa.bool_(),
        "utf8": pa.string(),
    }
    return {column: arrow_types[fixture_column_dtype(column)] for column in columns}


def fixture_arrow_csv_read(arrow_csv: Any, pa: Any, path: Path) -> Any:
    columns = fixture_columns_from_csv(path)
    options = arrow_csv.ConvertOptions(column_types=fixture_arrow_column_types(pa, columns))
    return arrow_csv.read_csv(path, convert_options=options)


def fixture_avro_schema(columns: tuple[str, ...] | list[str], record_name: str) -> dict[str, Any]:
    avro_types = {"int64": "long", "float64": "double", "boolean": "boolean", "utf8": "string"}
    return {
        "type": "record",
        "name": record_name,
        "fields": [
            {
                "name": column,
                "type": ["null", avro_types[fixture_column_dtype(column)]],
                "default": None,
            }
            for column in columns
        ],
    }


def fixture_scalar_from_text(value: str, dtype: str) -> Any:
    if value == "":
        return None
    if dtype == "int64":
        return int(value)
    if dtype == "float64":
        return float(value)
    if dtype == "boolean":
        normalized = value.strip().lower()
        if normalized not in {"true", "false"}:
            raise ValueError(f"invalid boolean fixture value: {value!r}")
        return normalized == "true"
    if dtype == "utf8":
        return value
    raise ValueError(f"unknown generated fixture dtype: {dtype}")

def dataset_profile_supports_all_executable_scenarios(dataset_profile: str) -> bool:
    return dataset_profile == "tiny_smoke"

def ensure_dataset(
    root: Path,
    rows: int,
    dim_rows: int,
    requested_formats: tuple[str, ...],
    dataset_profile: str,
) -> DatasetPaths:
    """Create an immutable fixture in a new local directory; never replace data."""
    root = require_local_path(root, Path.home(), sys.platform)
    if type(rows) is not int or rows < 1 or type(dim_rows) is not int or dim_rows < 1:
        raise ValueError("fixture row counts must be positive integers")
    if dataset_profile not in GENERATED_DATASET_PROFILES:
        raise ValueError("unknown dataset profile")
    if not requested_formats or set(requested_formats) - set(FORMAT_ORDER):
        raise ValueError("unknown or empty fixture format selection")
    root.mkdir(parents=True, exist_ok=False)
    fact_csv = root / "fact.csv"
    dim_csv = root / "dim.csv"
    fact_jsonl = root / "fact.jsonl"
    dim_jsonl = root / "dim.jsonl"
    fact_parquet = root / "fact.parquet"
    dim_parquet = root / "dim.parquet"
    fact_arrow_ipc = root / "fact.arrow"
    dim_arrow_ipc = root / "dim.arrow"
    fact_avro = root / "fact.avro"
    dim_avro = root / "dim.avro"
    fact_orc = root / "fact.orc"
    dim_orc = root / "dim.orc"
    fact_csv_parts_dir = root / "fact_csv_parts"
    fact_jsonl_parts_dir = root / "fact_jsonl_parts"
    fact_parquet_parts_dir = root / "fact_parquet_parts"
    fact_arrow_ipc_parts_dir = root / "fact_arrow_ipc_parts"
    fact_avro_parts_dir = root / "fact_avro_parts"
    fact_orc_parts_dir = root / "fact_orc_parts"
    cdc_delta_csv = root / "cdc_delta.csv"
    nested_jsonl = root / "nested_fact.jsonl"
    metadata_json = root / "dataset.json"
    fact_extra_columns = generated_fact_extra_columns(dataset_profile)
    expected_metadata = {
        "rows": rows,
        "dim_rows": dim_rows,
        "schema_version": 7,
        "dataset_profile": dataset_profile,
        "dataset_file_shape": dataset_file_shape(dataset_profile),
        "fact_extra_columns": list(fact_extra_columns),
        "fact_file_part_count": fact_file_part_count(dataset_profile, rows),
        "formats": sorted(requested_formats),
    }
    with fact_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        fact_columns = [
            "id",
            "group_key",
            "dim_key",
            "value",
            "metric",
            "flag",
            "category",
            *fact_extra_columns,
        ]
        writer.writerow(fact_columns)
        for idx in range(rows):
            group_key = generated_group_key(idx, dataset_profile)
            dim_key = generated_dim_key(idx, dim_rows, dataset_profile)
            value = (idx * 17) % 10_000
            metric = ((idx * 13) % 100_000) / 100.0
            flag = 1 if idx % 7 == 0 else 0
            category = generated_category(idx, group_key, dataset_profile)
            writer.writerow(
                [
                    idx,
                    group_key,
                    dim_key,
                    value,
                    f"{metric:.2f}",
                    flag,
                    category,
                    *generated_extra_fact_values(
                        idx,
                        group_key,
                        dim_key,
                        value,
                        metric,
                        flag,
                        category,
                        dataset_profile,
                        fact_extra_columns,
                    ),
                ]
            )

    with dim_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dim_key", "dim_label", "weight"])
        for idx in range(dim_rows):
            writer.writerow([idx, f"d{idx % 50}", (idx * 3) % 100])

    if "jsonl" in requested_formats:
        write_jsonl_copies(fact_csv, dim_csv, fact_jsonl, dim_jsonl)

    write_profile_sidecars(
        fact_csv,
        dataset_profile,
        rows,
        requested_formats,
        fact_csv_parts_dir,
        fact_jsonl_parts_dir,
        fact_parquet_parts_dir,
        fact_arrow_ipc_parts_dir,
        fact_avro_parts_dir,
        fact_orc_parts_dir,
        cdc_delta_csv,
        nested_jsonl,
    )

    if {"parquet", "arrow-ipc", "orc"} & set(requested_formats):
        write_arrow_family_copies(
            fact_csv,
            dim_csv,
            fact_parquet if "parquet" in requested_formats else None,
            dim_parquet if "parquet" in requested_formats else None,
            fact_arrow_ipc if "arrow-ipc" in requested_formats else None,
            dim_arrow_ipc if "arrow-ipc" in requested_formats else None,
            fact_orc if "orc" in requested_formats else None,
            dim_orc if "orc" in requested_formats else None,
        )
    if "avro" in requested_formats:
        write_avro_copies(fact_csv, dim_csv, fact_avro, dim_avro)

    with metadata_json.open("x", encoding="utf-8") as handle:
        json.dump(expected_metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return DatasetPaths(
        root,
        fact_csv,
        dim_csv,
        fact_jsonl,
        dim_jsonl,
        fact_parquet,
        dim_parquet,
        fact_arrow_ipc,
        dim_arrow_ipc,
        fact_avro,
        dim_avro,
        fact_orc,
        dim_orc,
        rows,
        dim_rows,
        dataset_profile,
        fact_extra_columns,
        fact_csv_parts_dir,
        fact_jsonl_parts_dir,
        fact_parquet_parts_dir,
        fact_arrow_ipc_parts_dir,
        fact_avro_parts_dir,
        fact_orc_parts_dir,
        cdc_delta_csv,
        nested_jsonl,
    )

def dataset_file_shape(dataset_profile: str) -> str:
    if dataset_profile_supports_all_executable_scenarios(dataset_profile):
        return "complete_local_smoke_fixture"
    if dataset_profile == "many_small_files":
        return "many_small_csv_parts"
    if dataset_profile == "few_large_files":
        return "few_large_csv_parts"
    if dataset_profile == "cdc_delta_overlay":
        return "base_plus_small_change_overlay"
    if dataset_profile in {"schema_drift", "dirty_csv", "nested_json"}:
        return dataset_profile
    return "single_local_files"

def fact_file_part_count(dataset_profile: str, rows: int) -> int:
    if dataset_profile_supports_all_executable_scenarios(dataset_profile):
        return max(1, min(rows, 8))
    if dataset_profile == "many_small_files":
        return max(1, min(rows, 32))
    if dataset_profile == "few_large_files":
        return max(1, min(rows, 2))
    return 0

def dataset_profile_has_cdc_overlay(dataset_profile: str) -> bool:
    return dataset_profile in {"cdc_delta_overlay"} or (
        dataset_profile_supports_all_executable_scenarios(dataset_profile)
    )

def dataset_profile_has_nested_json_fixture(dataset_profile: str) -> bool:
    return dataset_profile in {"nested_json"} or (
        dataset_profile_supports_all_executable_scenarios(dataset_profile)
    )

def write_profile_sidecars(
    fact_csv: Path,
    dataset_profile: str,
    rows: int,
    requested_formats: tuple[str, ...],
    fact_csv_parts_dir: Path,
    fact_jsonl_parts_dir: Path,
    fact_parquet_parts_dir: Path,
    fact_arrow_ipc_parts_dir: Path,
    fact_avro_parts_dir: Path,
    fact_orc_parts_dir: Path,
    cdc_delta_csv: Path,
    nested_jsonl: Path,
) -> None:
    part_count = fact_file_part_count(dataset_profile, rows)
    if part_count > 0:
        write_csv_parts(fact_csv, fact_csv_parts_dir, part_count)
        if "jsonl" in requested_formats:
            write_jsonl_part_copies(fact_csv_parts_dir, fact_jsonl_parts_dir)
        if {"parquet", "arrow-ipc", "orc"} & set(requested_formats):
            write_arrow_fact_part_copies(
                fact_csv_parts_dir,
                fact_parquet_parts_dir if "parquet" in requested_formats else None,
                fact_arrow_ipc_parts_dir if "arrow-ipc" in requested_formats else None,
                fact_orc_parts_dir if "orc" in requested_formats else None,
            )
        if "avro" in requested_formats:
            write_avro_fact_part_copies(fact_csv_parts_dir, fact_avro_parts_dir)
    if dataset_profile_has_cdc_overlay(dataset_profile):
        write_cdc_delta_overlay(fact_csv, cdc_delta_csv)
    if dataset_profile_has_nested_json_fixture(dataset_profile):
        write_nested_json_fixture(fact_csv, nested_jsonl)

def write_csv_parts(source_csv: Path, target_dir: Path, part_count: int) -> None:
    target_dir.mkdir(parents=True, exist_ok=False)
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        reader = csv.reader(source)
        header = next(reader)
        writers: list[tuple[Any, Any]] = []
        try:
            for index in range(part_count):
                target = (target_dir / f"part-{index:05d}.csv").open(
                    "w", newline="", encoding="utf-8"
                )
                writer = csv.writer(target)
                writer.writerow(header)
                writers.append((target, writer))
            for row_index, row in enumerate(reader):
                writers[row_index % part_count][1].writerow(row)
        finally:
            for handle, _writer in writers:
                handle.close()

def write_jsonl_part_copies(source_dir: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=False)
    for source_csv in sorted(source_dir.glob("part-*.csv")):
        write_jsonl_copy(source_csv, target_dir / f"{source_csv.stem}.jsonl")

def write_arrow_fact_part_copies(
    source_dir: Path,
    parquet_dir: Path | None,
    arrow_ipc_dir: Path | None,
    orc_dir: Path | None,
) -> None:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.csv as arrow_csv  # type: ignore
        import pyarrow.ipc as ipc  # type: ignore
        import pyarrow.orc as orc  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise BenchmarkUnsupported(
            "pyarrow is required to generate split Arrow-family benchmark inputs"
        ) from exc

    for target_dir in (parquet_dir, arrow_ipc_dir, orc_dir):
        if target_dir is None:
            continue
        target_dir.mkdir(parents=True, exist_ok=False)

    for source_csv in sorted(source_dir.glob("part-*.csv")):
        table = fixture_arrow_csv_read(arrow_csv, pa, source_csv)
        if parquet_dir is not None:
            pq.write_table(table, parquet_dir / f"{source_csv.stem}.parquet")
        if arrow_ipc_dir is not None:
            write_arrow_ipc_table(ipc, table, arrow_ipc_dir / f"{source_csv.stem}.arrow")
        if orc_dir is not None:
            orc.write_table(table, orc_dir / f"{source_csv.stem}.orc")

def write_avro_fact_part_copies(source_dir: Path, target_dir: Path) -> None:
    try:
        import fastavro  # type: ignore
    except ImportError as exc:
        raise BenchmarkUnsupported(
            "fastavro is required to generate split Avro benchmark inputs"
        ) from exc

    target_dir.mkdir(parents=True, exist_ok=False)
    for source_csv in sorted(source_dir.glob("part-*.csv")):
        schema = fastavro.parse_schema(fixture_avro_schema(
            fixture_columns_from_csv(source_csv), "fact"
        ))
        write_avro_copy(
            fastavro,
            source_csv,
            target_dir / f"{source_csv.stem}.avro",
            schema,
        )

def write_cdc_delta_overlay(source_csv: Path, target_csv: Path) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    overlay_size = max(1, min(len(rows), 24))
    with target_csv.open("w", newline="", encoding="utf-8") as target:
        fieldnames = ["id", "op", "value", "metric", "effective_ts"]
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        for index, row in enumerate(rows[:overlay_size]):
            op = "delete" if index % 7 == 0 else "update"
            writer.writerow(
                {
                    "id": row["id"],
                    "op": op,
                    "value": "" if op == "delete" else str(int(row["value"]) + 101),
                    "metric": "" if op == "delete" else f"{float(row['metric']) + 1.25:.2f}",
                    "effective_ts": f"2024-12-{(index % 28) + 1:02d}T00:00:00Z",
                }
            )
        for offset in range(max(1, overlay_size // 4)):
            writer.writerow(
                {
                    "id": len(rows) + offset,
                    "op": "insert",
                    "value": 9000 + offset,
                    "metric": f"{250.0 + offset:.2f}",
                    "effective_ts": f"2024-12-{(offset % 28) + 1:02d}T12:00:00Z",
                }
            )

def write_nested_json_fixture(source_csv: Path, target_jsonl: Path) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        with target_jsonl.open("w", encoding="utf-8") as target:
            for row in csv.DictReader(source):
                payload = json.loads(row["nested_payload"])
                target.write(
                    json.dumps(
                        {
                            "id": int(row["id"]),
                            "group_key": int(row["group_key"]),
                            "metric": float(row["metric"]),
                            "nested_payload": payload,
                        },
                        separators=(",", ":"),
                    )
                )
                target.write("\n")

def generated_group_key(idx: int, dataset_profile: str) -> int:
    if dataset_profile == "skewed_keys":
        return 0 if idx % 10 < 7 else idx % 100
    if dataset_profile == "well_clustered":
        return (idx // 32) % 100
    if dataset_profile == "poorly_clustered":
        return (idx * 37) % 100
    return idx % 100

def generated_dim_key(idx: int, dim_rows: int, dataset_profile: str) -> int:
    if dataset_profile == "skewed_keys":
        return 0 if idx % 10 < 6 else idx % dim_rows
    if dataset_profile == "well_clustered":
        return (idx // 32) % dim_rows
    if dataset_profile == "poorly_clustered":
        return (idx * 7919) % dim_rows
    return idx % dim_rows

def generated_category(idx: int, group_key: int, dataset_profile: str) -> str:
    if dataset_profile == "high_cardinality_strings" or (
        dataset_profile_supports_all_executable_scenarios(dataset_profile)
    ):
        return f"c{idx % 10_000}"
    if dataset_profile == "schema_drift":
        return f"c{group_key % 10}_v{1 + (idx % 3)}"
    return f"c{group_key % 10}"

def generated_fact_extra_columns(dataset_profile: str) -> tuple[str, ...]:
    if dataset_profile_supports_all_executable_scenarios(dataset_profile):
        return (
            tuple(f"nullable_metric_{index:02d}" for index in range(16))
            + tuple(f"nullable_category_{index:02d}" for index in range(4))
            + (
                "event_date",
                "partition_year",
                "partition_month",
                "cluster_bucket",
                "file_bucket",
                "schema_version_tag",
                "optional_metric_v2",
                "renamed_metric_candidate",
                "raw_event_time",
                "dirty_numeric",
                "dirty_flag",
                "nested_payload",
                "nested_group",
                "nested_score",
                "cdc_op",
                "cdc_sequence",
                "effective_ts",
                "is_deleted",
            )
        )
    if dataset_profile == "wide_table":
        return tuple(f"extra_metric_{index:02d}" for index in range(16))
    if dataset_profile == "very_wide_table":
        return tuple(f"extra_metric_{index:02d}" for index in range(64))
    if dataset_profile == "null_heavy":
        return tuple(f"nullable_metric_{index:02d}" for index in range(16)) + tuple(
            f"nullable_category_{index:02d}" for index in range(4)
        )
    if dataset_profile in {"many_small_files", "few_large_files"}:
        return ("file_bucket", "event_date")
    if dataset_profile == "partitioned_by_date":
        return ("event_date", "partition_year", "partition_month")
    if dataset_profile in {"poorly_clustered", "well_clustered"}:
        return ("cluster_bucket", "event_date")
    if dataset_profile == "schema_drift":
        return ("schema_version_tag", "optional_metric_v2", "renamed_metric_candidate")
    if dataset_profile == "dirty_csv":
        return ("raw_event_time", "dirty_numeric", "dirty_flag")
    if dataset_profile == "nested_json":
        return ("nested_payload", "nested_group", "nested_score")
    if dataset_profile == "cdc_delta_overlay":
        return ("cdc_op", "cdc_sequence", "effective_ts", "is_deleted")
    return ()

def generated_extra_fact_values(
    idx: int,
    group_key: int,
    dim_key: int,
    value: int,
    metric: float,
    flag: int,
    category: str,
    dataset_profile: str,
    fact_extra_columns: tuple[str, ...],
) -> list[str]:
    values = []
    for column in fact_extra_columns:
        if column.startswith("extra_metric_"):
            column_index = int(column.rsplit("_", 1)[1])
            values.append(f"{((idx + 1) * (column_index + 3)) % 100_000 / 100.0:.2f}")
        elif column.startswith("nullable_metric_"):
            column_index = int(column.rsplit("_", 1)[1])
            if (idx + column_index) % 3 == 0:
                values.append("")
            else:
                values.append(f"{(metric + column_index + (value % 17)):.2f}")
        elif column.startswith("nullable_category_"):
            column_index = int(column.rsplit("_", 1)[1])
            values.append("" if (idx + column_index) % 4 == 0 else category)
        elif column == "event_date":
            values.append(generated_event_date(idx))
        elif column == "partition_year":
            values.append(generated_event_date(idx)[:4])
        elif column == "partition_month":
            values.append(generated_event_date(idx)[5:7])
        elif column == "cluster_bucket":
            cluster_source = group_key if dataset_profile == "well_clustered" else dim_key
            values.append(str(cluster_source % 16))
        elif column == "file_bucket":
            if dataset_profile_supports_all_executable_scenarios(dataset_profile):
                bucket_count = 8
            else:
                bucket_count = 32 if dataset_profile == "many_small_files" else 2
            values.append(str(idx % bucket_count))
        elif column == "schema_version_tag":
            values.append(f"schema_v{1 + (idx % 3)}")
        elif column == "optional_metric_v2":
            values.append("" if idx % 5 == 0 else f"{metric * 1.1:.2f}")
        elif column == "renamed_metric_candidate":
            values.append(f"{metric:.2f}")
        elif column == "raw_event_time":
            values.append(
                "not-a-timestamp" if idx % 11 == 0 else f"{generated_event_date(idx)}T00:00:00Z"
            )
        elif column == "dirty_numeric":
            values.append("bad-number" if idx % 13 == 0 else str(value))
        elif column == "dirty_flag":
            values.append("Y" if flag else ("?" if idx % 17 == 0 else "N"))
        elif column == "nested_payload":
            values.append(
                json.dumps(
                    {
                        "event": {"date": generated_event_date(idx), "flag": bool(flag)},
                        "metrics": {"value": value, "score": round(metric / 10.0, 4)},
                        "labels": [category, f"g{group_key % 5}"],
                    },
                    separators=(",", ":"),
                )
            )
        elif column == "nested_group":
            values.append(f"g{group_key % 5}")
        elif column == "nested_score":
            values.append(f"{metric / 10.0:.4f}")
        elif column == "cdc_op":
            values.append("base")
        elif column == "cdc_sequence":
            values.append(str(idx))
        elif column == "effective_ts":
            values.append(f"{generated_event_date(idx)}T00:00:00Z")
        elif column == "is_deleted":
            values.append("false")
        else:
            values.append("" if flag else str(value))
    return values

def generated_event_date(idx: int) -> str:
    month = ((idx // 28) % 12) + 1
    day = (idx % 28) + 1
    return f"2024-{month:02d}-{day:02d}"

def write_jsonl_copies(fact_csv: Path, dim_csv: Path, fact_jsonl: Path, dim_jsonl: Path) -> None:
    write_jsonl_copy(fact_csv, fact_jsonl)
    write_jsonl_copy(dim_csv, dim_jsonl)

def write_jsonl_copy(source_csv: Path, target_jsonl: Path) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        with target_jsonl.open("w", encoding="utf-8") as target:
            for row in reader:
                typed = {}
                for key, value in row.items():
                    if key is None or value is None:
                        continue
                    typed[key] = fixture_scalar_from_text(value, fixture_column_dtype(key))
                target.write(json.dumps(typed, separators=(",", ":")))
                target.write("\n")

def write_arrow_family_copies(
    fact_csv: Path,
    dim_csv: Path,
    fact_parquet: Path | None,
    dim_parquet: Path | None,
    fact_arrow_ipc: Path | None,
    dim_arrow_ipc: Path | None,
    fact_orc: Path | None,
    dim_orc: Path | None,
) -> None:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.csv as arrow_csv  # type: ignore
        import pyarrow.ipc as ipc  # type: ignore
        import pyarrow.orc as orc  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise BenchmarkUnsupported(
            "pyarrow is required to generate Arrow-family benchmark inputs"
        ) from exc

    fact_table = fixture_arrow_csv_read(arrow_csv, pa, fact_csv)
    dim_table = fixture_arrow_csv_read(arrow_csv, pa, dim_csv)
    if fact_parquet is not None and dim_parquet is not None:
        pq.write_table(fact_table, fact_parquet)
        pq.write_table(dim_table, dim_parquet)
    if fact_arrow_ipc is not None and dim_arrow_ipc is not None:
        write_arrow_ipc_table(ipc, fact_table, fact_arrow_ipc)
        write_arrow_ipc_table(ipc, dim_table, dim_arrow_ipc)
    if fact_orc is not None and dim_orc is not None:
        orc.write_table(fact_table, fact_orc)
        orc.write_table(dim_table, dim_orc)
    _ = pa

def write_arrow_ipc_table(ipc: Any, table: Any, path: Path) -> None:
    with path.open("wb") as handle:
        with ipc.new_file(handle, table.schema) as writer:
            writer.write_table(table)

def write_avro_copies(fact_csv: Path, dim_csv: Path, fact_avro: Path, dim_avro: Path) -> None:
    try:
        import fastavro  # type: ignore
    except ImportError as exc:
        raise BenchmarkUnsupported(
            "fastavro is required to generate Avro benchmark inputs"
        ) from exc

    write_avro_copy(
        fastavro,
        fact_csv,
        fact_avro,
        fastavro.parse_schema(fixture_avro_schema(fixture_columns_from_csv(fact_csv), "fact")),
    )
    write_avro_copy(
        fastavro,
        dim_csv,
        dim_avro,
        fastavro.parse_schema(fixture_avro_schema(fixture_columns_from_csv(dim_csv), "dim")),
    )

def write_avro_copy(
    fastavro: Any,
    source_csv: Path,
    target_avro: Path,
    schema: dict[str, Any],
) -> None:
    schema_fields = [field["name"] for field in schema["fields"]]
    nullable_fields = {
        field["name"]
        for field in schema["fields"]
        if isinstance(field.get("type"), list) and "null" in field["type"]
    }
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        records = []
        for row in csv.DictReader(source):
            record = {}
            for key in schema_fields:
                if key not in row:
                    continue
                value = row[key]
                if value == "" and key in nullable_fields:
                    record[key] = None
                    continue
                record[key] = fixture_scalar_from_text(value, fixture_column_dtype(key))
            records.append(record)
    with target_avro.open("wb") as target:
        fastavro.writer(target, schema, records)

def fact_path(paths: DatasetPaths, data_format: str) -> Path:
    if data_format == "csv":
        return paths.fact_csv
    if data_format == "jsonl":
        return paths.fact_jsonl
    if data_format == "parquet":
        return paths.fact_parquet
    if data_format == "arrow-ipc":
        return paths.fact_arrow_ipc
    if data_format == "avro":
        return paths.fact_avro
    if data_format == "orc":
        return paths.fact_orc
    raise BenchmarkUnsupported(f"unsupported fact storage format: {data_format}")

def dim_path(paths: DatasetPaths, data_format: str) -> Path:
    if data_format == "csv":
        return paths.dim_csv
    if data_format == "jsonl":
        return paths.dim_jsonl
    if data_format == "parquet":
        return paths.dim_parquet
    if data_format == "arrow-ipc":
        return paths.dim_arrow_ipc
    if data_format == "avro":
        return paths.dim_avro
    if data_format == "orc":
        return paths.dim_orc
    raise BenchmarkUnsupported(f"unsupported dimension storage format: {data_format}")

def fact_part_paths(paths: DatasetPaths, data_format: str) -> tuple[Path, ...]:
    if data_format == "csv" and paths.fact_csv_parts_dir is not None:
        return tuple(sorted(paths.fact_csv_parts_dir.glob("part-*.csv")))
    if data_format == "jsonl" and paths.fact_jsonl_parts_dir is not None:
        return tuple(sorted(paths.fact_jsonl_parts_dir.glob("part-*.jsonl")))
    if data_format == "parquet" and paths.fact_parquet_parts_dir is not None:
        return tuple(sorted(paths.fact_parquet_parts_dir.glob("part-*.parquet")))
    if data_format == "arrow-ipc" and paths.fact_arrow_ipc_parts_dir is not None:
        return tuple(sorted(paths.fact_arrow_ipc_parts_dir.glob("part-*.arrow")))
    if data_format == "avro" and paths.fact_avro_parts_dir is not None:
        return tuple(sorted(paths.fact_avro_parts_dir.glob("part-*.avro")))
    if data_format == "orc" and paths.fact_orc_parts_dir is not None:
        return tuple(sorted(paths.fact_orc_parts_dir.glob("part-*.orc")))
    return ()
