# SPDX-License-Identifier: Apache-2.0
"""Independent comparison engines. These adapters never execute ShardLoom work."""
from __future__ import annotations
import csv
import importlib
import json
import math
import os
import re
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from benchmark_models import BenchmarkUnsupported, DatasetPaths, EngineRunner, FORMAT_ORDER
from fixtures import fact_path, dim_path, fact_part_paths
from comparison import (
    round_float, normalize_scalar_result, normalize_group_rows, normalize_top_rows,
    normalize_multi_group_rows, normalize_rank_rows, normalize_top_group_rows,
    normalize_complex_etl_rows,
)

DASK_BLOCKSIZE = "16MB"

DASK_SCHEDULER = "threads"

def module_version(name: str) -> str:
    module = importlib.import_module(name)
    return str(getattr(module, "__version__", "unknown"))

def scenario_slug(scenario: str) -> str:
    return (
        scenario.lower()
        .replace("/", "-")
        .replace(" ", "-")
        .replace("_", "-")
    )

def sql_literal(path: Path) -> str:
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"

def scenario_output_path(
    paths: DatasetPaths, engine: str, data_format: str, scenario: str, extension: str
) -> Path:
    if paths.output_root is None:
        raise ValueError("benchmark output directory must be declared separately from input")
    output_dir = paths.output_root / engine / data_format / scenario_slug(scenario) / uuid.uuid4().hex
    output_dir.mkdir(parents=True, exist_ok=False)
    return output_dir / f"part-00000.{extension}"

def write_rows_as_csv(path: Path, rows: list[dict[str, Any]], fieldnames: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def pyarrow_rows(batches: list[Any]) -> list[dict[str, Any]]:
    import pyarrow as pa  # type: ignore

    if not batches:
        return []
    return pa.Table.from_batches(batches).to_pylist()

def pyarrow_table_for_format(path: Path, data_format: str) -> Any:
    import pyarrow as pa  # type: ignore
    import pyarrow.csv as arrow_csv  # type: ignore
    import pyarrow.ipc as ipc  # type: ignore
    import pyarrow.json as arrow_json  # type: ignore
    import pyarrow.orc as orc  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    if data_format == "csv":
        return arrow_csv.read_csv(path)
    if data_format == "jsonl":
        return arrow_json.read_json(path)
    if data_format == "parquet":
        return pq.read_table(path)
    if data_format == "arrow-ipc":
        with path.open("rb") as handle:
            return ipc.open_file(handle).read_all()
    if data_format == "avro":
        try:
            import fastavro  # type: ignore
        except ImportError as exc:
            raise BenchmarkUnsupported(
                "fastavro is required to read Avro benchmark inputs"
            ) from exc
        with path.open("rb") as handle:
            records = list(fastavro.reader(handle))
        return pa.Table.from_pylist(records)
    if data_format == "orc":
        return orc.read_table(path)
    raise BenchmarkUnsupported(f"unsupported storage format for Arrow read: {data_format}")

def pandas_frame_for_format(path: Path, data_format: str) -> Any:
    import pandas as pd  # type: ignore

    if data_format == "csv":
        return pd.read_csv(path)
    if data_format == "jsonl":
        return pd.read_json(path, lines=True)
    if data_format == "parquet":
        return pd.read_parquet(path)
    if data_format == "orc":
        return pd.read_orc(path)
    return pyarrow_table_for_format(path, data_format).to_pandas()

def configure_java_home() -> None:
    if shutil.which("java") is not None and os.environ.get("JAVA_HOME"):
        return
    candidates = []
    env_java_home = os.environ.get("JAVA_HOME")
    if env_java_home:
        candidates.append(Path(env_java_home))
    if os.name == "nt":
        adoptium_root = Path("C:/Program Files/Eclipse Adoptium")
        if adoptium_root.exists():
            candidates.extend(sorted(adoptium_root.glob("jdk-*-hotspot"), reverse=True))
        java_root = Path("C:/Program Files/Java")
        if java_root.exists():
            candidates.extend(sorted(java_root.glob("jdk-*"), reverse=True))
    for candidate in candidates:
        java_exe = candidate / "bin" / ("java.exe" if os.name == "nt" else "java")
        if java_exe.exists():
            os.environ["JAVA_HOME"] = str(candidate)
            os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")
            return

def pandas_runner() -> EngineRunner:
    import pandas as pd  # type: ignore

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        return pandas_frame_for_format(fact_path(paths, data_format), data_format)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        return pandas_frame_for_format(dim_path(paths, data_format), data_format)

    def read_fact_parts(paths: DatasetPaths, data_format: str) -> Any:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        frames = []
        for part in parts:
            frames.append(pandas_frame_for_format(part, data_format))
        return pd.concat(frames, ignore_index=True)

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame[(frame["flag"] == 1) & (frame["value"] >= 5000)]
        return normalize_scalar_result(len(filtered), filtered["metric"].sum())

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        grouped = (
            frame.groupby("group_key", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort_values(["metric", "id"], ascending=[False, True])
            .head(10)[["id", "metric"]]
            .to_dict("records")
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact.merge(dim, on="dim_key", how="inner")
        grouped = (
            joined.groupby("dim_label", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        projected = frame[["id", "group_key", "category"]]
        return normalize_scalar_result(len(projected), projected["group_key"].sum())

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return {"distinct_category_count": int(frame["category"].nunique())}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        limited = (
            frame[(frame["flag"] == 1) & (frame["value"] >= 5000)][["id", "value", "category"]]
            .sort_values(["id"])
            .head(100)
        )
        return normalize_scalar_result(len(limited), limited["value"].sum())

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.groupby(["group_key", "category"], as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact[fact["value"] >= 2500]
            .merge(dim, on="dim_key", how="inner")
            .groupby(["dim_label", "category"], as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] == 1][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_rank_rows(rows)

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "event_date" not in frame.columns:
            raise BenchmarkUnsupported("partition pruning requires an event_date fixture column")
        filtered = frame[(frame["event_date"] >= "2024-03-01") & (frame["event_date"] < "2024-06-01")]
        return normalize_scalar_result(len(filtered), filtered["metric"].sum())

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact_parts(paths, data_format)
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "nullable_metric_00" not in frame.columns:
            raise BenchmarkUnsupported("null-heavy aggregate requires nullable_metric_00")
        series = pd.to_numeric(frame["nullable_metric_00"], errors="coerce")
        return {
            "row_count": int(series.notna().sum()),
            "metric_sum": round_float(series.sum(skipna=True)),
        }

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.groupby("category", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return {
            "distinct_category_count": int(frame["category"].nunique()),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] <= 3][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_top_group_rows(rows)

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        required = {"raw_event_time", "dirty_numeric", "dirty_flag"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise BenchmarkUnsupported(
                "clean/cast/filter/write requires dirty fixture columns: "
                + ",".join(missing)
            )
        parsed = pd.to_datetime(
            frame["raw_event_time"],
            format="%Y-%m-%dT%H:%M:%SZ",
            errors="coerce",
            utc=True,
        )
        numeric = pd.to_numeric(frame["dirty_numeric"], errors="coerce")
        valid = parsed.notna() & numeric.notna() & (frame["dirty_flag"].astype(str) == "Y")
        filtered = frame[valid & (numeric >= 500)].copy()
        filtered["clean_numeric"] = numeric[filtered.index]
        output_path = scenario_output_path(
            paths, "pandas", data_format, "clean/cast/filter/write", "csv"
        )
        filtered[["id", "raw_event_time", "clean_numeric", "category"]].to_csv(
            output_path, index=False
        )
        return normalize_scalar_result(len(filtered), filtered["clean_numeric"].sum())

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "raw_event_time" not in frame.columns:
            raise BenchmarkUnsupported("dirty CSV scenario requires raw_event_time")
        parsed = pd.to_datetime(
            frame["raw_event_time"],
            format="%Y-%m-%dT%H:%M:%SZ",
            errors="coerce",
            utc=True,
        )
        numeric = pd.to_numeric(frame["dirty_numeric"], errors="coerce")
        valid = parsed.notna() & numeric.notna()
        return normalize_scalar_result(int(valid.sum()), numeric[valid].sum())

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        frame = read_fact(paths, data_format).set_index("id", drop=False)
        overlay = pd.read_csv(paths.cdc_delta_csv)
        for row in overlay.to_dict("records"):
            row_id = int(row["id"])
            op = str(row["op"])
            if op == "delete":
                frame = frame.drop(index=row_id, errors="ignore")
            else:
                frame.loc[row_id, "id"] = row_id
                frame.loc[row_id, "value"] = int(row["value"])
                frame.loc[row_id, "metric"] = float(row["metric"])
                frame.loc[row_id, "flag"] = 1
                frame.loc[row_id, "category"] = f"cdc_{op}"
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "nested_payload" not in frame.columns:
            raise BenchmarkUnsupported("nested JSON scenario requires nested_payload")
        scores = []
        flagged = 0
        for value in frame["nested_payload"]:
            payload = json.loads(value) if isinstance(value, str) else value
            scores.append(float(payload["metrics"]["score"]))
            flagged += 1 if payload["event"]["flag"] else 0
        return {"row_count": len(scores), "metric_sum": round_float(sum(scores)), "flagged": flagged}

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        expanded = fact.merge(dim, on="dim_key", how="inner")
        expanded["skew_key"] = expanded["group_key"] % 10
        grouped = (
            expanded.groupby("skew_key", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact[fact["value"] >= 2500].merge(dim, on="dim_key", how="inner")
        joined["bucket"] = joined["group_key"] % 10
        joined["weighted_metric"] = joined["metric"] * (joined["weight"] + 1)
        rows = (
            joined.groupby(["dim_label", "bucket"], as_index=False)
            .agg(
                row_count=("id", "count"),
                metric_sum=("metric", "sum"),
                weighted_sum=("weighted_metric", "sum"),
            )
            .sort_values(["weighted_sum", "dim_label", "bucket"], ascending=[False, True, True])
            .head(20)
            .to_dict("records")
        )
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "pandas",
        module_version("pandas"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=FORMAT_ORDER,
    )

def polars_eager_runner() -> EngineRunner:
    import polars as pl  # type: ignore

    def read_frame(path: Path, data_format: str) -> Any:
        if data_format == "parquet":
            return pl.read_parquet(path)
        if data_format == "jsonl":
            return pl.read_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.read_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path)
        if data_format == "orc":
            return pl.from_arrow(pyarrow_table_for_format(path, data_format))
        return pl.read_csv(path)

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        return read_frame(fact_path(paths, data_format), data_format)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        return read_frame(dim_path(paths, data_format), data_format)

    def read_fact_parts(paths: DatasetPaths, data_format: str) -> Any:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        return pl.concat(
            [read_frame(part, data_format) for part in parts],
            how="vertical_relaxed",
        )

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return normalize_scalar_result(frame.height, frame["metric"].sum())

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame.filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
        return normalize_scalar_result(filtered.height, filtered["metric"].sum())

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.group_by("group_key")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort(["metric", "id"], descending=[True, False])
            .head(10)
            .select(["id", "metric"])
            .to_dicts()
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.join(dim, on="dim_key", how="inner")
            .group_by("dim_label")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        projected = frame.select(["id", "group_key", "category"])
        return normalize_scalar_result(projected.height, projected["group_key"].sum())

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        return {"distinct_category_count": int(frame["category"].n_unique())}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        limited = (
            frame.filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
            .select(["id", "value", "category"])
            .sort("id")
            .head(100)
        )
        return normalize_scalar_result(limited.height, limited["value"].sum())

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.group_by(["group_key", "category"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.filter(pl.col("value") >= 2500)
            .join(dim, on="dim_key", how="inner")
            .group_by(["dim_label", "category"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort(["group_key", "metric", "id"], descending=[False, True, False])
            .with_columns((pl.col("id").cum_count().over("group_key")).alias("rank"))
            .filter(pl.col("rank") == 1)
            .select(["group_key", "id", "metric", "rank"])
            .to_dicts()
        )
        return normalize_rank_rows(rows)

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "event_date" not in frame.columns:
            raise BenchmarkUnsupported("partition pruning requires an event_date fixture column")
        filtered = frame.filter(
            (pl.col("event_date") >= "2024-03-01")
            & (pl.col("event_date") < "2024-06-01")
        )
        return normalize_scalar_result(filtered.height, filtered["metric"].sum())

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact_parts(paths, data_format)
        return normalize_scalar_result(frame.height, frame["metric"].sum())

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "nullable_metric_00" not in frame.columns:
            raise BenchmarkUnsupported("null-heavy aggregate requires nullable_metric_00")
        result = frame.select(
            pl.col("nullable_metric_00")
            .cast(pl.Float64, strict=False)
            .drop_nulls()
            .count()
            .alias("row_count"),
            pl.col("nullable_metric_00")
            .cast(pl.Float64, strict=False)
            .sum()
            .alias("metric_sum"),
        ).to_dicts()[0]
        return normalize_scalar_result(result["row_count"], result["metric_sum"])

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.group_by("category")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return {
            "distinct_category_count": int(frame["category"].n_unique()),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            frame.sort(["group_key", "metric", "id"], descending=[False, True, False])
            .with_columns((pl.col("id").cum_count().over("group_key")).alias("rank"))
            .filter(pl.col("rank") <= 3)
            .select(["group_key", "id", "metric", "rank"])
            .to_dicts()
        )
        return normalize_top_group_rows(rows)

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        required = {"raw_event_time", "dirty_numeric", "dirty_flag"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise BenchmarkUnsupported(
                "clean/cast/filter/write requires dirty fixture columns: "
                + ",".join(missing)
            )
        filtered = (
            frame.with_columns(
                [
                    pl.col("raw_event_time")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .alias("parsed_event_time"),
                    pl.col("dirty_numeric")
                    .cast(pl.Float64, strict=False)
                    .alias("clean_numeric"),
                ]
            )
            .filter(
                pl.col("parsed_event_time").is_not_null()
                & pl.col("clean_numeric").is_not_null()
                & (pl.col("dirty_flag").cast(pl.Utf8) == "Y")
                & (pl.col("clean_numeric") >= 500)
            )
        )
        output_path = scenario_output_path(
            paths, "polars-eager", data_format, "clean/cast/filter/write", "csv"
        )
        filtered.select(["id", "raw_event_time", "clean_numeric", "category"]).write_csv(
            output_path
        )
        return normalize_scalar_result(filtered.height, filtered["clean_numeric"].sum())

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        required = {"raw_event_time", "dirty_numeric"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise BenchmarkUnsupported(
                "dirty CSV scenario requires fixture columns: " + ",".join(missing)
            )
        filtered = (
            frame.with_columns(
                [
                    pl.col("raw_event_time")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .alias("parsed_event_time"),
                    pl.col("dirty_numeric")
                    .cast(pl.Float64, strict=False)
                    .alias("clean_numeric"),
                ]
            )
            .filter(
                pl.col("parsed_event_time").is_not_null()
                & pl.col("clean_numeric").is_not_null()
            )
        )
        return normalize_scalar_result(filtered.height, filtered["clean_numeric"].sum())

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        frame = read_fact(paths, data_format)
        overlay = pl.read_csv(paths.cdc_delta_csv).with_columns(
            [
                pl.col("id").cast(pl.Int64),
                pl.col("metric").cast(pl.Float64, strict=False),
            ]
        )
        changed_ids = overlay.select("id")
        base_kept = frame.join(changed_ids, on="id", how="anti")
        delta_rows = overlay.filter(pl.col("op") != "delete")
        metric_sum = (base_kept["metric"].sum() or 0.0) + (
            delta_rows["metric"].sum() or 0.0
        )
        return normalize_scalar_result(base_kept.height + delta_rows.height, metric_sum)

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        if "nested_payload" not in frame.columns:
            raise BenchmarkUnsupported("nested JSON scenario requires nested_payload")
        result = frame.select(
            pl.len().alias("row_count"),
            pl.col("nested_payload")
            .str.json_path_match("$.metrics.score")
            .cast(pl.Float64, strict=False)
            .sum()
            .alias("metric_sum"),
            (
                pl.col("nested_payload").str.json_path_match("$.event.flag") == "true"
            )
            .cast(pl.Int64)
            .sum()
            .alias("flagged"),
        ).to_dicts()[0]
        return {
            "row_count": int(result["row_count"]),
            "metric_sum": round_float(result["metric_sum"]),
            "flagged": int(result["flagged"]),
        }

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.join(dim, on="dim_key", how="inner")
            .with_columns((pl.col("group_key") % 10).alias("skew_key"))
            .group_by("skew_key")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .to_dicts()
        )
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        rows = (
            fact.filter(pl.col("value") >= 2500)
            .join(dim, on="dim_key", how="inner")
            .with_columns(
                [
                    (pl.col("group_key") % 10).alias("bucket"),
                    (pl.col("metric") * (pl.col("weight") + 1)).alias("weighted_metric"),
                ]
            )
            .group_by(["dim_label", "bucket"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                    pl.col("weighted_metric").sum().alias("weighted_sum"),
                ]
            )
            .sort(["weighted_sum", "dim_label", "bucket"], descending=[True, False, False])
            .head(20)
            .to_dicts()
        )
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "polars-eager",
        module_version("polars"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=FORMAT_ORDER,
    )

def polars_lazy_runner() -> EngineRunner:
    import polars as pl  # type: ignore

    def scan_fact(paths: DatasetPaths, data_format: str) -> Any:
        path = fact_path(paths, data_format)
        if data_format == "parquet":
            return pl.scan_parquet(path)
        if data_format == "jsonl":
            return pl.scan_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.scan_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path).lazy()
        if data_format == "orc":
            return pl.from_arrow(pyarrow_table_for_format(path, data_format)).lazy()
        if data_format == "csv":
            return pl.scan_csv(path)
        raise BenchmarkUnsupported(f"polars-lazy does not support {data_format} in this harness")

    def scan_dim(paths: DatasetPaths, data_format: str) -> Any:
        path = dim_path(paths, data_format)
        if data_format == "parquet":
            return pl.scan_parquet(path)
        if data_format == "jsonl":
            return pl.scan_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.scan_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path).lazy()
        if data_format == "orc":
            return pl.from_arrow(pyarrow_table_for_format(path, data_format)).lazy()
        if data_format == "csv":
            return pl.scan_csv(path)
        raise BenchmarkUnsupported(f"polars-lazy does not support {data_format} in this harness")

    def scan_fact_parts(paths: DatasetPaths, data_format: str) -> Any:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        if data_format == "parquet":
            return pl.scan_parquet(list(parts))
        if data_format == "jsonl":
            return pl.scan_ndjson(list(parts))
        if data_format == "arrow-ipc":
            return pl.scan_ipc(list(parts))
        if data_format == "avro":
            return pl.concat(
                [pl.read_avro(part) for part in parts],
                how="vertical_relaxed",
            ).lazy()
        if data_format == "orc":
            return pl.concat(
                [
                    pl.from_arrow(pyarrow_table_for_format(part, data_format))
                    for part in parts
                ],
                how="vertical_relaxed",
            ).lazy()
        if data_format == "csv":
            return pl.scan_csv(list(parts))
        raise BenchmarkUnsupported(f"polars-lazy does not support {data_format} fact parts")

    def collect_one(query: Any) -> dict[str, Any]:
        rows = query.collect().to_dicts()
        if not rows:
            return {"row_count": 0, "metric_sum": 0.0}
        return rows[0]

    def scalar_from_lazy(query: Any, metric_column: str = "metric_sum") -> Any:
        row = collect_one(query)
        return normalize_scalar_result(row["row_count"], row[metric_column])

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format).select(
                pl.len().alias("row_count"),
                pl.col("metric").sum().alias("metric_sum"),
            )
        )

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format)
            .filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
            .select(
                pl.len().alias("row_count"),
                pl.col("metric").sum().alias("metric_sum"),
            )
        )

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .group_by("group_key")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .collect()
            .to_dicts()
        )
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .sort(["metric", "id"], descending=[True, False])
            .head(10)
            .select(["id", "metric"])
            .collect()
            .to_dicts()
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .join(scan_dim(paths, data_format), on="dim_key", how="inner")
            .group_by("dim_label")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .collect()
            .to_dicts()
        )
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format)
            .select(["id", "group_key", "category"])
            .select(
                pl.len().alias("row_count"),
                pl.col("group_key").sum().alias("metric_sum"),
            )
        )

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        row = collect_one(
            scan_fact(paths, data_format).select(
                pl.col("category").n_unique().alias("distinct_category_count")
            )
        )
        return {"distinct_category_count": int(row["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format)
            .filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
            .select(["id", "value", "category"])
            .sort("id")
            .head(100)
            .select(
                pl.len().alias("row_count"),
                pl.col("value").sum().alias("metric_sum"),
            )
        )

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .group_by(["group_key", "category"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .collect()
            .to_dicts()
        )
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .filter(pl.col("value") >= 2500)
            .join(scan_dim(paths, data_format), on="dim_key", how="inner")
            .group_by(["dim_label", "category"])
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .collect()
            .to_dicts()
        )
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .sort(["group_key", "metric", "id"], descending=[False, True, False])
            .with_columns((pl.col("id").cum_count().over("group_key")).alias("rank"))
            .filter(pl.col("rank") == 1)
            .select(["group_key", "id", "metric", "rank"])
            .collect()
            .to_dicts()
        )
        return normalize_rank_rows(rows)

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format)
            .filter(
                (pl.col("event_date") >= "2024-03-01")
                & (pl.col("event_date") < "2024-06-01")
            )
            .select(
                pl.len().alias("row_count"),
                pl.col("metric").sum().alias("metric_sum"),
            )
        )

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact_parts(paths, data_format).select(
                pl.len().alias("row_count"),
                pl.col("metric").sum().alias("metric_sum"),
            )
        )

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format).select(
                pl.col("nullable_metric_00")
                .cast(pl.Float64, strict=False)
                .drop_nulls()
                .count()
                .alias("row_count"),
                pl.col("nullable_metric_00")
                .cast(pl.Float64, strict=False)
                .sum()
                .alias("metric_sum"),
            )
        )

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        frame = scan_fact(paths, data_format)
        rows = (
            frame.group_by("category")
            .agg(
                [
                    pl.len().alias("row_count"),
                    pl.col("metric").sum().alias("metric_sum"),
                ]
            )
            .collect()
            .to_dicts()
        )
        distinct_row = collect_one(
            scan_fact(paths, data_format).select(
                pl.col("category").n_unique().alias("distinct_category_count")
            )
        )
        return {
            "distinct_category_count": int(distinct_row["distinct_category_count"]),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        rows = (
            scan_fact(paths, data_format)
            .sort(["group_key", "metric", "id"], descending=[False, True, False])
            .with_columns((pl.col("id").cum_count().over("group_key")).alias("rank"))
            .filter(pl.col("rank") <= 3)
            .select(["group_key", "id", "metric", "rank"])
            .collect()
            .to_dicts()
        )
        return normalize_top_group_rows(rows)

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        filtered = (
            scan_fact(paths, data_format)
            .with_columns(
                [
                    pl.col("raw_event_time")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .alias("parsed_event_time"),
                    pl.col("dirty_numeric")
                    .cast(pl.Float64, strict=False)
                    .alias("clean_numeric"),
                ]
            )
            .filter(
                pl.col("parsed_event_time").is_not_null()
                & pl.col("clean_numeric").is_not_null()
                & (pl.col("dirty_flag").cast(pl.Utf8) == "Y")
                & (pl.col("clean_numeric") >= 500)
            )
        )
        output = filtered.collect()
        output_path = scenario_output_path(
            paths, "polars-lazy", data_format, "clean/cast/filter/write", "csv"
        )
        output.select(["id", "raw_event_time", "clean_numeric", "category"]).write_csv(
            output_path
        )
        return normalize_scalar_result(output.height, output["clean_numeric"].sum())

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        return scalar_from_lazy(
            scan_fact(paths, data_format)
            .with_columns(
                [
                    pl.col("raw_event_time")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .alias("parsed_event_time"),
                    pl.col("dirty_numeric")
                    .cast(pl.Float64, strict=False)
                    .alias("clean_numeric"),
                ]
            )
            .filter(
                pl.col("parsed_event_time").is_not_null()
                & pl.col("clean_numeric").is_not_null()
            )
            .select(
                pl.len().alias("row_count"),
                pl.col("clean_numeric").sum().alias("metric_sum"),
            )
        )

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        overlay = pl.scan_csv(paths.cdc_delta_csv).with_columns(
            [
                pl.col("id").cast(pl.Int64),
                pl.col("metric").cast(pl.Float64, strict=False),
            ]
        )
        base_kept = scan_fact(paths, data_format).join(
            overlay.select("id"), on="id", how="anti"
        )
        base_row = collect_one(
            base_kept.select(
                pl.len().alias("row_count"),
                pl.col("metric").sum().alias("metric_sum"),
            )
        )
        delta_row = collect_one(
            overlay.filter(pl.col("op") != "delete").select(
                pl.len().alias("row_count"),
                pl.col("metric").sum().alias("metric_sum"),
            )
        )
        return normalize_scalar_result(
            int(base_row["row_count"]) + int(delta_row["row_count"]),
            round_float(base_row["metric_sum"]) + round_float(delta_row["metric_sum"]),
        )

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        row = collect_one(
            scan_fact(paths, data_format).select(
                pl.len().alias("row_count"),
                pl.col("nested_payload")
                .str.json_path_match("$.metrics.score")
                .cast(pl.Float64, strict=False)
                .sum()
                .alias("metric_sum"),
                (
                    pl.col("nested_payload").str.json_path_match("$.event.flag") == "true"
                )
                .cast(pl.Int64)
                .sum()
                .alias("flagged"),
            )
        )
        return {
            "row_count": int(row["row_count"]),
            "metric_sum": round_float(row["metric_sum"]),
            "flagged": int(row["flagged"]),
        }

    return EngineRunner(
        "polars-lazy",
        f"{module_version('polars')} (lazy scan API)",
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
        },
        formats=FORMAT_ORDER,
    )

def duckdb_runner() -> EngineRunner:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")
    arrow_backed_formats = {"arrow-ipc", "avro", "orc"}

    def table_expr(paths: DatasetPaths, table: str, data_format: str) -> str:
        path = fact_path(paths, data_format) if table == "fact" else dim_path(paths, data_format)
        if data_format == "parquet":
            function = "read_parquet"
        elif data_format == "jsonl":
            function = "read_json_auto"
        elif data_format in arrow_backed_formats:
            return table
        else:
            function = "read_csv_auto"
        return f"{function}({sql_literal(path)})"

    def list_sql_literal(paths: tuple[Path, ...]) -> str:
        return "[" + ", ".join(sql_literal(path) for path in paths) + "]"

    def register_arrow_table(name: str, path: Path, data_format: str) -> None:
        try:
            con.unregister(name)
        except Exception:
            pass
        con.register(name, pyarrow_table_for_format(path, data_format))

    def register_arrow_parts_table(
        name: str, part_paths: tuple[Path, ...], data_format: str
    ) -> None:
        import pyarrow as pa  # type: ignore

        try:
            con.unregister(name)
        except Exception:
            pass
        con.register(
            name,
            pa.concat_tables(
                [pyarrow_table_for_format(path, data_format) for path in part_paths],
                promote_options="default",
            ),
        )

    def fact_parts_expr(paths: DatasetPaths, data_format: str) -> str:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        if data_format == "parquet":
            return f"read_parquet({list_sql_literal(parts)})"
        if data_format == "jsonl":
            return f"read_json_auto({list_sql_literal(parts)})"
        if data_format in arrow_backed_formats:
            register_arrow_parts_table("fact_parts", parts, data_format)
            return "fact_parts"
        return f"read_csv_auto({list_sql_literal(parts)})"

    def register_arrow_backed_tables(paths: DatasetPaths, data_format: str) -> None:
        for name, path in (
            ("fact", fact_path(paths, data_format)),
            ("dim", dim_path(paths, data_format)),
        ):
            register_arrow_table(name, path, data_format)

    def query(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        if data_format in arrow_backed_formats:
            register_arrow_backed_tables(paths, data_format)
        sql = sql.replace("{fact}", table_expr(paths, "fact", data_format)).replace(
            "{dim}", table_expr(paths, "dim", data_format)
        )
        columns = [column[0] for column in con.execute(sql).description]
        return [dict(zip(columns, row)) for row in con.fetchall()]

    def execute(paths: DatasetPaths, data_format: str, sql: str) -> None:
        if data_format in arrow_backed_formats:
            register_arrow_backed_tables(paths, data_format)
        sql = sql.replace("{fact}", table_expr(paths, "fact", data_format)).replace(
            "{dim}", table_expr(paths, "dim", data_format)
        )
        con.execute(sql)

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum from {fact}",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from {fact} where flag = 1 and value >= 5000",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select group_key, count(*) as row_count, sum(metric) as metric_sum "
                "from {fact} group by group_key",
            ),
            "group_key",
        )

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_top_rows(
            query(
                paths,
                data_format,
                "select id, metric from {fact} "
                "order by metric desc, id asc limit 10",
            )
        )

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, count(*) as row_count, sum(f.metric) as metric_sum "
                "from {fact} f join {dim} d "
                "on f.dim_key = d.dim_key group by d.dim_label",
            ),
            "dim_label",
        )

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(group_key) as metric_sum "
            "from (select id, group_key, category from {fact})",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(distinct category) as distinct_category_count from {fact}",
        )
        return {"distinct_category_count": int(rows[0]["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(value) as metric_sum "
            "from (select id, value, category from {fact} "
            "where flag = 1 and value >= 5000 order by id asc limit 100)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select group_key, category, count(*) as row_count, sum(metric) as metric_sum "
                "from {fact} group by group_key, category",
            ),
            ("group_key", "category"),
        )

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.category, count(*) as row_count, sum(f.metric) as metric_sum "
                "from {fact} f join {dim} d on f.dim_key = d.dim_key "
                "where f.value >= 2500 group by d.dim_label, f.category",
            ),
            ("dim_label", "category"),
        )

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_rank_rows(
            query(
                paths,
                data_format,
                "select group_key, id, metric, rank from ("
                "select group_key, id, metric, "
                "row_number() over (partition by group_key order by metric desc, id asc) as rank "
                "from {fact}) where rank = 1",
            )
        )

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from {fact} where event_date >= '2024-03-01' and event_date < '2024-06-01'",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            f"from {fact_parts_expr(paths, data_format)}",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(try_cast(nullable_metric_00 as double)) as row_count, "
            "sum(try_cast(nullable_metric_00 as double)) as metric_sum from {fact}",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select category, count(*) as row_count, sum(metric) as metric_sum "
            "from {fact} group by category",
        )
        distinct = query(
            paths,
            data_format,
            "select count(distinct category) as distinct_category_count from {fact}",
        )
        return {
            "distinct_category_count": int(distinct[0]["distinct_category_count"]),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_top_group_rows(
            query(
                paths,
                data_format,
                "select group_key, id, metric, rank from ("
                "select group_key, id, metric, "
                "row_number() over (partition by group_key order by metric desc, id asc) as rank "
                "from {fact}) where rank <= 3",
            )
        )

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        filtered_sql = (
            "select id, raw_event_time, try_cast(dirty_numeric as double) as clean_numeric, "
            "category from {fact} "
            "where try_strptime(raw_event_time, '%Y-%m-%dT%H:%M:%SZ') is not null "
            "and try_cast(dirty_numeric as double) is not null "
            "and cast(dirty_flag as varchar) = 'Y' "
            "and try_cast(dirty_numeric as double) >= 500"
        )
        output_path = scenario_output_path(
            paths, "duckdb", data_format, "clean/cast/filter/write", "csv"
        )
        execute(
            paths,
            data_format,
            f"COPY ({filtered_sql}) TO {sql_literal(output_path)} (HEADER, DELIMITER ',')",
        )
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(clean_numeric) as metric_sum "
            f"from ({filtered_sql})",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(try_cast(dirty_numeric as double)) as metric_sum "
            "from {fact} "
            "where try_strptime(raw_event_time, '%Y-%m-%dT%H:%M:%SZ') is not null "
            "and try_cast(dirty_numeric as double) is not null",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        cdc_delta_expr = f"read_csv_auto({sql_literal(paths.cdc_delta_csv)})"
        rows = query(
            paths,
            data_format,
            "with overlay as ("
            f"select id, op, try_cast(metric as double) as metric from {cdc_delta_expr}"
            "), base_kept as ("
            "select f.id, f.metric from {fact} f left join overlay o on f.id = o.id "
            "where o.id is null"
            "), merged as ("
            "select id, metric from base_kept "
            "union all select id, metric from overlay where op <> 'delete'"
            ") select count(*) as row_count, sum(metric) as metric_sum from merged",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, "
            "sum(cast(json_extract(nested_payload, '$.metrics.score') as double)) as metric_sum, "
            "sum(case when cast(json_extract(nested_payload, '$.event.flag') as boolean) "
            "then 1 else 0 end) as flagged from {fact}",
        )
        return {
            "row_count": int(rows[0]["row_count"]),
            "metric_sum": round_float(rows[0]["metric_sum"]),
            "flagged": int(rows[0]["flagged"]),
        }

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select f.group_key % 10 as skew_key, count(*) as row_count, sum(f.metric) as metric_sum "
                "from {fact} f join {dim} d "
                "on f.dim_key = d.dim_key group by skew_key",
            ),
            "skew_key",
        )

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_complex_etl_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.group_key % 10 as bucket, count(*) as row_count, "
                "sum(f.metric) as metric_sum, sum(f.metric * (d.weight + 1)) as weighted_sum "
                "from {fact} f join {dim} d "
                "on f.dim_key = d.dim_key where f.value >= 2500 "
                "group by d.dim_label, bucket "
                "order by weighted_sum desc, d.dim_label asc, bucket asc limit 20",
            )
        )

    return EngineRunner(
        "duckdb",
        module_version("duckdb"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=FORMAT_ORDER,
        close=con.close,
    )

def spark_runner(profile: str) -> EngineRunner:
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        raise BenchmarkUnsupported(
            "Spark/PySpark requires a local JDK. Install JDK 17 or newer, set JAVA_HOME, "
            "and ensure java is on PATH before running Spark benchmark rows."
        )
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    import pyspark  # type: ignore
    from pyspark.sql import SparkSession, functions as F  # type: ignore
    from pyspark.sql.window import Window  # type: ignore

    builder = SparkSession.builder.master("local[*]").appName(
        f"shardloom-traditional-analytics-benchmark-{profile}"
    )
    builder = builder.config("spark.ui.enabled", "false")
    builder = builder.config("spark.pyspark.python", sys.executable).config(
        "spark.pyspark.driver.python", sys.executable
    )
    profile_notes = [
        "master=local[*]",
        "spark.ui.enabled=false",
        f"spark.pyspark.python={sys.executable}",
    ]
    if profile == "local-tuned":
        local_threads = os.cpu_count() or 1
        shuffle_partitions = max(1, min(local_threads, 8))
        builder = (
            builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.default.parallelism", str(shuffle_partitions))
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
        profile_notes.extend(
            [
                f"spark.sql.shuffle.partitions={shuffle_partitions}",
                f"spark.default.parallelism={shuffle_partitions}",
                "spark.sql.adaptive.enabled=true",
                "spark.sql.adaptive.coalescePartitions.enabled=true",
            ]
        )
    elif profile not in {"default", "pyspark"}:
        raise BenchmarkUnsupported(f"unknown Spark benchmark profile: {profile}")

    spark_session: Any | None = None

    def spark_instance() -> Any:
        nonlocal spark_session
        if spark_session is None:
            spark_session = builder.getOrCreate()
            spark_session.sparkContext.setLogLevel("ERROR")
        return spark_session

    def close_spark() -> None:
        nonlocal spark_session
        if spark_session is not None:
            spark_session.stop()
            spark_session = None

    def warmup_spark() -> None:
        spark_instance()

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return spark_instance().read.parquet(str(paths.fact_parquet))
        if data_format == "jsonl":
            return spark_instance().read.json(str(paths.fact_jsonl))
        if data_format == "orc":
            return spark_instance().read.orc(str(paths.fact_orc))
        if data_format in {"arrow-ipc", "avro"}:
            return spark_instance().createDataFrame(
                pandas_frame_for_format(fact_path(paths, data_format), data_format)
            )
        return spark_instance().read.option("header", True).option("inferSchema", True).csv(
            str(paths.fact_csv)
        )

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return spark_instance().read.parquet(str(paths.dim_parquet))
        if data_format == "jsonl":
            return spark_instance().read.json(str(paths.dim_jsonl))
        if data_format == "orc":
            return spark_instance().read.orc(str(paths.dim_orc))
        if data_format in {"arrow-ipc", "avro"}:
            return spark_instance().createDataFrame(
                pandas_frame_for_format(dim_path(paths, data_format), data_format)
            )
        return spark_instance().read.option("header", True).option("inferSchema", True).csv(
            str(paths.dim_csv)
        )

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        row = frame.agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum")).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format).where((F.col("flag") == 1) & (F.col("value") >= 5000))
        row = frame.agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum")).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .groupBy("group_key")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .orderBy(F.col("metric").desc(), F.col("id").asc())
            .select("id", "metric")
            .limit(10)
            .collect()
        ]
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .groupBy("dim_label")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format).select("id", "group_key", "category")
        row = frame.agg(
            F.count("*").alias("row_count"), F.sum("group_key").alias("metric_sum")
        ).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        row = read_fact(paths, data_format).agg(F.countDistinct("category").alias("distinct_category_count")).first()
        return {"distinct_category_count": int(row["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = (
            read_fact(paths, data_format)
            .where((F.col("flag") == 1) & (F.col("value") >= 5000))
            .select("id", "value", "category")
            .orderBy(F.col("id").asc())
            .limit(100)
        )
        row = frame.agg(
            F.count("*").alias("row_count"), F.sum("value").alias("metric_sum")
        ).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .groupBy("group_key", "category")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .where(F.col("value") >= 2500)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .groupBy("dim_label", "category")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        window = Window.partitionBy("group_key").orderBy(F.col("metric").desc(), F.col("id").asc())
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") == 1)
            .select("group_key", "id", "metric", "rank")
            .collect()
        ]
        return normalize_rank_rows(rows)

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        window = Window.partitionBy("group_key").orderBy(F.col("metric").desc(), F.col("id").asc())
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") <= 3)
            .select("group_key", "id", "metric", "rank")
            .collect()
        ]
        return normalize_top_group_rows(rows)

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths, data_format)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .withColumn("skew_key", F.col("group_key") % F.lit(10))
            .groupBy("skew_key")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        joined = (
            read_fact(paths, data_format)
            .where(F.col("value") >= 2500)
            .join(read_dim(paths, data_format), on="dim_key", how="inner")
            .withColumn("bucket", F.col("group_key") % F.lit(10))
            .withColumn("weighted_metric", F.col("metric") * (F.col("weight") + F.lit(1)))
        )
        rows = [
            row.asDict()
            for row in joined.groupBy("dim_label", "bucket")
            .agg(
                F.count("*").alias("row_count"),
                F.sum("metric").alias("metric_sum"),
                F.sum("weighted_metric").alias("weighted_sum"),
            )
            .orderBy(F.col("weighted_sum").desc(), F.col("dim_label").asc(), F.col("bucket").asc())
            .limit(20)
            .collect()
        ]
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        {
            "default": "spark-default",
            "local-tuned": "spark-local-tuned",
            "pyspark": "pyspark",
        }[profile],
        (
            f"{module_version('pyspark')} ({'; '.join(profile_notes)}; "
            "arrow-ipc/avro local DataFrame adapter)"
        ),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "top-N per group": top_n_per_group,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=FORMAT_ORDER,
        warmup=warmup_spark,
        close=close_spark,
    )

def spark_default_runner() -> EngineRunner:
    return spark_runner("default")

def pyspark_runner() -> EngineRunner:
    return spark_runner("pyspark")

def spark_local_tuned_runner() -> EngineRunner:
    return spark_runner("local-tuned")

def datafusion_runner() -> EngineRunner:
    import datafusion  # type: ignore

    def register_arrow_table(ctx: Any, name: str, path: Path, data_format: str) -> None:
        table = pyarrow_table_for_format(path, data_format)
        ctx.register_record_batches(name, [table.to_batches()])

    def register_arrow_parts_table(
        ctx: Any, name: str, part_paths: tuple[Path, ...], data_format: str
    ) -> None:
        import pyarrow as pa  # type: ignore

        table = pa.concat_tables(
            [pyarrow_table_for_format(path, data_format) for path in part_paths],
            promote_options="default",
        )
        ctx.register_record_batches(name, [table.to_batches()])

    def query(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        ctx = datafusion.SessionContext()
        if data_format == "parquet":
            ctx.register_parquet("fact", paths.fact_parquet)
            ctx.register_parquet("dim", paths.dim_parquet)
        elif data_format == "jsonl":
            ctx.register_json("fact", paths.fact_jsonl, file_extension=".jsonl")
            ctx.register_json("dim", paths.dim_jsonl, file_extension=".jsonl")
        elif data_format == "arrow-ipc":
            register_arrow_table(ctx, "fact", paths.fact_arrow_ipc, data_format)
            register_arrow_table(ctx, "dim", paths.dim_arrow_ipc, data_format)
        elif data_format == "avro":
            ctx.register_avro("fact", paths.fact_avro)
            ctx.register_avro("dim", paths.dim_avro)
        elif data_format == "orc":
            register_arrow_table(ctx, "fact", paths.fact_orc, data_format)
            register_arrow_table(ctx, "dim", paths.dim_orc, data_format)
        else:
            ctx.register_csv("fact", paths.fact_csv, has_header=True)
            ctx.register_csv("dim", paths.dim_csv, has_header=True)
        return pyarrow_rows(ctx.sql(sql).collect())

    def query_fact_parts(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        ctx = datafusion.SessionContext()
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        register_arrow_parts_table(ctx, "fact", parts, data_format)
        return pyarrow_rows(ctx.sql(sql).collect())

    def query_with_cdc_delta(
        paths: DatasetPaths, data_format: str, sql: str
    ) -> list[dict[str, Any]]:
        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        ctx = datafusion.SessionContext()
        if data_format == "parquet":
            ctx.register_parquet("fact", paths.fact_parquet)
        elif data_format == "jsonl":
            ctx.register_json("fact", paths.fact_jsonl, file_extension=".jsonl")
        elif data_format == "arrow-ipc":
            register_arrow_table(ctx, "fact", paths.fact_arrow_ipc, data_format)
        elif data_format == "avro":
            ctx.register_avro("fact", paths.fact_avro)
        elif data_format == "orc":
            register_arrow_table(ctx, "fact", paths.fact_orc, data_format)
        else:
            ctx.register_csv("fact", paths.fact_csv, has_header=True)
        ctx.register_csv("cdc_delta", paths.cdc_delta_csv, has_header=True)
        return pyarrow_rows(ctx.sql(sql).collect())

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(paths, data_format, "select count(*) as row_count, sum(metric) as metric_sum from fact")
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from fact where flag = 1 and value >= 5000",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select group_key, count(*) as row_count, sum(metric) as metric_sum "
                "from fact group by group_key",
            ),
            "group_key",
        )

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_top_rows(
            query(paths, data_format, "select id, metric from fact order by metric desc, id asc limit 10")
        )

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key group by d.dim_label",
            ),
            "dim_label",
        )

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(group_key) as metric_sum "
            "from (select id, group_key, category from fact)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(paths, data_format, "select count(distinct category) as distinct_category_count from fact")
        return {"distinct_category_count": int(rows[0]["distinct_category_count"])}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(value) as metric_sum "
            "from (select id, value, category from fact "
            "where flag = 1 and value >= 5000 order by id asc limit 100)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select group_key, category, count(*) as row_count, sum(metric) as metric_sum "
                "from fact group by group_key, category",
            ),
            ("group_key", "category"),
        )

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_multi_group_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.category, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key "
                "where f.value >= 2500 group by d.dim_label, f.category",
            ),
            ("dim_label", "category"),
        )

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_rank_rows(
            query(
                paths,
                data_format,
                "select group_key, id, metric, rank from ("
                "select group_key, id, metric, "
                "row_number() over (partition by group_key order by metric desc, id asc) as rank "
                "from fact) where rank = 1",
            )
        )

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from fact where event_date >= '2024-03-01' and event_date < '2024-06-01'",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        rows = query_fact_parts(
            paths,
            data_format,
            "select count(*) as row_count, sum(metric) as metric_sum from fact",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(try_cast(nullable_metric_00 as double)) as row_count, "
            "sum(try_cast(nullable_metric_00 as double)) as metric_sum from fact",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select category, count(*) as row_count, sum(metric) as metric_sum "
            "from fact group by category",
        )
        distinct = query(
            paths,
            data_format,
            "select count(distinct category) as distinct_category_count from fact",
        )
        return {
            "distinct_category_count": int(distinct[0]["distinct_category_count"]),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_top_group_rows(
            query(
                paths,
                data_format,
                "select group_key, id, metric, rank from ("
                "select group_key, id, metric, "
                "row_number() over (partition by group_key order by metric desc, id asc) as rank "
                "from fact) where rank <= 3",
            )
        )

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select id, raw_event_time, try_cast(dirty_numeric as double) as clean_numeric, "
            "category from fact "
            "where regexp_like(raw_event_time, '^\\\\d{4}-\\\\d{2}-\\\\d{2}T\\\\d{2}:\\\\d{2}:\\\\d{2}Z$') "
            "and try_cast(dirty_numeric as double) is not null "
            "and cast(dirty_flag as varchar) = 'Y' "
            "and try_cast(dirty_numeric as double) >= 500",
        )
        output_path = scenario_output_path(
            paths, "datafusion", data_format, "clean/cast/filter/write", "csv"
        )
        write_rows_as_csv(output_path, rows, ("id", "raw_event_time", "clean_numeric", "category"))
        return normalize_scalar_result(
            len(rows),
            sum(float(row["clean_numeric"] or 0.0) for row in rows),
        )

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        rows = query(
            paths,
            data_format,
            "select count(*) as row_count, sum(try_cast(dirty_numeric as double)) as metric_sum "
            "from fact "
            "where regexp_like(raw_event_time, '^\\\\d{4}-\\\\d{2}-\\\\d{2}T\\\\d{2}:\\\\d{2}:\\\\d{2}Z$') "
            "and try_cast(dirty_numeric as double) is not null",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        rows = query_with_cdc_delta(
            paths,
            data_format,
            "with overlay as ("
            "select id, op, try_cast(metric as double) as metric from cdc_delta"
            "), base_kept as ("
            "select f.id, f.metric from fact f left join overlay o on f.id = o.id "
            "where o.id is null"
            "), merged as ("
            "select id, metric from base_kept "
            "union all select id, metric from overlay where op <> 'delete'"
            ") select count(*) as row_count, sum(metric) as metric_sum from merged",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        raise BenchmarkUnsupported(
            "DataFusion 50.1.0 Python SQL function registry exposes no JSON extraction "
            "functions in this benchmark profile"
        )

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_group_rows(
            query(
                paths,
                data_format,
                "select f.group_key % 10 as skew_key, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key group by skew_key",
            ),
            "skew_key",
        )

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        return normalize_complex_etl_rows(
            query(
                paths,
                data_format,
                "select d.dim_label, f.group_key % 10 as bucket, count(*) as row_count, "
                "sum(f.metric) as metric_sum, sum(f.metric * (d.weight + 1)) as weighted_sum "
                "from fact f join dim d on f.dim_key = d.dim_key "
                "where f.value >= 2500 group by d.dim_label, bucket "
                "order by weighted_sum desc, d.dim_label asc, bucket asc limit 20",
            )
        )

    return EngineRunner(
        "datafusion",
        module_version("datafusion"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=FORMAT_ORDER,
    )

def dask_runner() -> EngineRunner:
    import dask  # type: ignore
    import dask.dataframe as dd  # type: ignore

    blocksize = None if DASK_BLOCKSIZE == "default" else DASK_BLOCKSIZE

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return dd.read_parquet(paths.fact_parquet)
        if data_format == "jsonl":
            return dd.read_json(paths.fact_jsonl, lines=True, blocksize=blocksize)
        if data_format in {"arrow-ipc", "avro", "orc"}:
            return dd.from_pandas(
                pandas_frame_for_format(fact_path(paths, data_format), data_format),
                npartitions=1,
            )
        return dd.read_csv(paths.fact_csv, blocksize=blocksize)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return dd.read_parquet(paths.dim_parquet)
        if data_format == "jsonl":
            return dd.read_json(paths.dim_jsonl, lines=True, blocksize=blocksize)
        if data_format in {"arrow-ipc", "avro", "orc"}:
            return dd.from_pandas(
                pandas_frame_for_format(dim_path(paths, data_format), data_format),
                npartitions=1,
            )
        return dd.read_csv(paths.dim_csv, blocksize=blocksize)

    def read_fact_parts(paths: DatasetPaths, data_format: str) -> Any:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        import pandas as pd  # type: ignore

        return dd.from_pandas(
            pd.concat(
                [pandas_frame_for_format(path, data_format) for path in parts],
                ignore_index=True,
            ),
            npartitions=max(1, min(len(parts), 8)),
        )

    def compute_one(*values: Any) -> tuple[Any, ...]:
        return dask.compute(*values, scheduler=DASK_SCHEDULER)

    def compute_frame(value: Any) -> Any:
        return value.compute(scheduler=DASK_SCHEDULER)

    def ingest(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        row_count, metric_sum = compute_one(frame.id.count(), frame.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def selective_filter(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame[(frame.flag == 1) & (frame.value >= 5000)]
        row_count, metric_sum = compute_one(filtered.id.count(), filtered.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        counts = frame.groupby("group_key").id.count().rename("row_count")
        sums = frame.groupby("group_key").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        rows = (
            compute_frame(frame.nlargest(10, "metric")[["id", "metric"]])
            .sort_values(["metric", "id"], ascending=[False, True])
            .to_dict("records")
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact.merge(dim, on="dim_key", how="inner")
        counts = joined.groupby("dim_label").id.count().rename("row_count")
        sums = joined.groupby("dim_label").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)[["id", "group_key", "category"]]
        row_count, metric_sum = compute_one(frame.id.count(), frame.group_key.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def distinct_count(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        distinct = compute_frame(frame.category.nunique())
        return {"distinct_category_count": int(distinct)}

    def filter_projection_limit(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        limited = compute_frame(
            frame[(frame.flag == 1) & (frame.value >= 5000)][["id", "value", "category"]]
        ).sort_values("id").head(100)
        return normalize_scalar_result(len(limited), limited["value"].sum())

    def multi_key_group_by(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        groups = frame.groupby(["group_key", "category"])
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_multi_group_rows(rows, ("group_key", "category"))

    def join_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact[fact.value >= 2500].merge(dim, on="dim_key", how="inner")
        groups = joined.groupby(["dim_label", "category"])
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_multi_group_rows(rows, ("dim_label", "category"))

    def row_number_window(paths: DatasetPaths, data_format: str) -> Any:
        frame = compute_frame(read_fact(paths, data_format))
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] == 1][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_rank_rows(rows)

    def partition_pruning(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        filtered = frame[(frame.event_date >= "2024-03-01") & (frame.event_date < "2024-06-01")]
        row_count, metric_sum = compute_one(filtered.id.count(), filtered.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def many_small_files_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact_parts(paths, data_format)
        row_count, metric_sum = compute_one(frame.id.count(), frame.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def null_heavy_aggregate(paths: DatasetPaths, data_format: str) -> Any:
        import dask.dataframe as dd  # type: ignore

        frame = read_fact(paths, data_format)
        numeric = dd.to_numeric(frame.nullable_metric_00, errors="coerce")
        row_count, metric_sum = compute_one(numeric.count(), numeric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def high_cardinality_string_group_distinct(paths: DatasetPaths, data_format: str) -> Any:
        frame = read_fact(paths, data_format)
        groups = frame.groupby("category")
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        distinct = compute_frame(frame.category.nunique())
        return {
            "distinct_category_count": int(distinct),
            "groups": normalize_multi_group_rows(rows, ("category",))[:100],
        }

    def top_n_per_group(paths: DatasetPaths, data_format: str) -> Any:
        frame = compute_frame(read_fact(paths, data_format))
        ranked = frame.sort_values(["group_key", "metric", "id"], ascending=[True, False, True])
        ranked["rank"] = ranked.groupby("group_key").cumcount() + 1
        rows = ranked[ranked["rank"] <= 3][["group_key", "id", "metric", "rank"]].to_dict(
            "records"
        )
        return normalize_top_group_rows(rows)

    def clean_cast_filter_write(paths: DatasetPaths, data_format: str) -> Any:
        import pandas as pd  # type: ignore

        frame = compute_frame(read_fact(paths, data_format)).reset_index(drop=True)
        parsed = pd.to_datetime(
            frame["raw_event_time"],
            format="%Y-%m-%dT%H:%M:%SZ",
            errors="coerce",
            utc=True,
        )
        numeric = pd.to_numeric(frame["dirty_numeric"], errors="coerce")
        valid = parsed.notna() & numeric.notna() & (frame["dirty_flag"].astype(str) == "Y")
        filtered = frame[valid & (numeric >= 500)].copy()
        filtered["clean_numeric"] = numeric.loc[filtered.index].to_numpy()
        output_path = scenario_output_path(
            paths, "dask", data_format, "clean/cast/filter/write", "csv"
        )
        filtered[["id", "raw_event_time", "clean_numeric", "category"]].to_csv(
            output_path, index=False
        )
        return normalize_scalar_result(len(filtered), filtered["clean_numeric"].sum())

    def malformed_timestamp_dirty_csv(paths: DatasetPaths, data_format: str) -> Any:
        import pandas as pd  # type: ignore

        frame = compute_frame(read_fact(paths, data_format)).reset_index(drop=True)
        parsed = pd.to_datetime(
            frame["raw_event_time"],
            format="%Y-%m-%dT%H:%M:%SZ",
            errors="coerce",
            utc=True,
        )
        numeric = pd.to_numeric(frame["dirty_numeric"], errors="coerce")
        valid = parsed.notna() & numeric.notna()
        return normalize_scalar_result(int(valid.sum()), numeric[valid].sum())

    def small_change_over_large_base(paths: DatasetPaths, data_format: str) -> Any:
        import pandas as pd  # type: ignore

        if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
            raise BenchmarkUnsupported("CDC overlay scenario requires cdc_delta.csv")
        frame = compute_frame(read_fact(paths, data_format)).set_index("id", drop=False)
        overlay = pd.read_csv(paths.cdc_delta_csv)
        for row in overlay.to_dict("records"):
            row_id = int(row["id"])
            op = str(row["op"])
            if op == "delete":
                frame = frame.drop(index=row_id, errors="ignore")
            else:
                frame.loc[row_id, "id"] = row_id
                frame.loc[row_id, "value"] = int(row["value"])
                frame.loc[row_id, "metric"] = float(row["metric"])
                frame.loc[row_id, "flag"] = 1
                frame.loc[row_id, "category"] = f"cdc_{op}"
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def nested_json_field_scan(paths: DatasetPaths, data_format: str) -> Any:
        frame = compute_frame(read_fact(paths, data_format))
        if "nested_payload" not in frame.columns:
            raise BenchmarkUnsupported("nested JSON scenario requires nested_payload")
        scores = []
        flagged = 0
        for value in frame["nested_payload"]:
            payload = json.loads(value) if isinstance(value, str) else value
            scores.append(float(payload["metrics"]["score"]))
            flagged += 1 if payload["event"]["flag"] else 0
        return {"row_count": len(scores), "metric_sum": round_float(sum(scores)), "flagged": flagged}

    def scale_stress(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact.merge(dim, on="dim_key", how="inner")
        joined = joined.assign(skew_key=joined.group_key % 10)
        counts = joined.groupby("skew_key").id.count().rename("row_count")
        sums = joined.groupby("skew_key").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths, data_format: str) -> Any:
        fact = read_fact(paths, data_format)
        dim = read_dim(paths, data_format)
        joined = fact[fact.value >= 2500].merge(dim, on="dim_key", how="inner")
        joined = joined.assign(
            bucket=joined.group_key % 10,
            weighted_metric=joined.metric * (joined["weight"] + 1),
        )
        groups = joined.groupby(["dim_label", "bucket"])
        counts = groups.id.count().rename("row_count")
        sums = groups.metric.sum().rename("metric_sum")
        weighted_sums = groups.weighted_metric.sum().rename("weighted_sum")
        rows = (
            compute_frame(dd.concat([counts, sums, weighted_sums], axis=1).reset_index())
            .sort_values(["weighted_sum", "dim_label", "bucket"], ascending=[False, True, True])
            .head(20)
            .to_dict("records")
        )
        return normalize_complex_etl_rows(rows)

    return EngineRunner(
        "dask",
        module_version("dask"),
        {
            "csv/file ingest": ingest,
            "selective filter": selective_filter,
            "group by aggregation": group_by,
            "sort and top-k": top_k,
            "hash join": hash_join,
            "wide projection": wide_projection,
            "distinct count": distinct_count,
            "filter + projection + limit": filter_projection_limit,
            "multi-key group by": multi_key_group_by,
            "join + aggregate": join_aggregate,
            "row number window": row_number_window,
            "partition pruning": partition_pruning,
            "many-small-files scan": many_small_files_scan,
            "null-heavy aggregate": null_heavy_aggregate,
            "high-cardinality string group/distinct": high_cardinality_string_group_distinct,
            "top-N per group": top_n_per_group,
            "clean/cast/filter/write": clean_cast_filter_write,
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=FORMAT_ORDER,
    )

ENGINE_FACTORIES = {
    "pandas": pandas_runner,
    "polars-eager": polars_eager_runner,
    "polars-lazy": polars_lazy_runner,
    "duckdb": duckdb_runner,
    "datafusion": datafusion_runner,
    "dask": dask_runner,
    "pyspark": pyspark_runner,
    "spark-default": spark_default_runner,
    "spark-local-tuned": spark_local_tuned_runner,
}
