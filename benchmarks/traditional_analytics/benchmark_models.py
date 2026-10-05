# SPDX-License-Identifier: Apache-2.0
"""Shared declarations for benchmark tooling; never imported by runtime code."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

DEFAULT_DATASET_PROFILE = "narrow_fact_dim"

GENERATED_DATASET_PROFILES = (
    "tiny_smoke",
    "narrow_fact_dim",
    "skewed_keys",
    "high_cardinality_strings",
    "wide_table",
    "very_wide_table",
    "null_heavy",
    "many_small_files",
    "few_large_files",
    "partitioned_by_date",
    "poorly_clustered",
    "well_clustered",
    "schema_drift",
    "dirty_csv",
    "nested_json",
    "cdc_delta_overlay",
)

FORMAT_ORDER = ("csv", "json", "jsonl", "vortex", "parquet", "arrow-ipc", "avro", "orc")
COMPARISON_FORMATS = tuple(fmt for fmt in FORMAT_ORDER if fmt != "vortex")


def comparison_input_format(candidate_format: str) -> str:
    """Use the original CSV fixture as the independent Vortex query oracle.

    This does not claim that a comparison adapter reads Vortex. The report and
    retained worker job record the actual comparison input format separately.
    """
    if candidate_format not in FORMAT_ORDER:
        raise ValueError(f"unknown candidate input format: {candidate_format}")
    return "csv" if candidate_format == "vortex" else candidate_format

@dataclass(frozen=True)
class DatasetPaths:
    root: Path
    fact_csv: Path
    dim_csv: Path
    fact_jsonl: Path
    dim_jsonl: Path
    fact_parquet: Path
    dim_parquet: Path
    fact_arrow_ipc: Path
    dim_arrow_ipc: Path
    fact_avro: Path
    dim_avro: Path
    fact_orc: Path
    dim_orc: Path
    rows: int
    dim_rows: int
    dataset_profile: str = DEFAULT_DATASET_PROFILE
    fact_extra_columns: tuple[str, ...] = ()
    fact_csv_parts_dir: Path | None = None
    fact_jsonl_parts_dir: Path | None = None
    fact_parquet_parts_dir: Path | None = None
    fact_arrow_ipc_parts_dir: Path | None = None
    fact_avro_parts_dir: Path | None = None
    fact_orc_parts_dir: Path | None = None
    cdc_delta_csv: Path | None = None
    nested_jsonl: Path | None = None
    output_root: Path | None = None
    fact_json: Path | None = None
    dim_json: Path | None = None
    fact_json_parts_dir: Path | None = None
    fact_vortex: Path | None = None
    dim_vortex: Path | None = None
    fact_vortex_parts_dir: Path | None = None

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> DatasetPaths:
        values = dict(record)
        for key, value in values.items():
            if key not in {"rows", "dim_rows", "dataset_profile", "fact_extra_columns"}:
                values[key] = Path(value) if value is not None else None
        values["fact_extra_columns"] = tuple(values.get("fact_extra_columns", ()))
        return cls(**values)

class BenchmarkUnsupported(RuntimeError):
    """Raised when an engine cannot execute a benchmark scenario yet."""

@dataclass(frozen=True)
class EngineRunner:
    name: str
    version: str
    scenarios: dict[str, Callable[[DatasetPaths, str], Any]]
    formats: tuple[str, ...] = ("csv",)
    prepare: Callable[[DatasetPaths, tuple[str, ...]], None] | None = None
    warmup: Callable[[], dict[str, Any] | None] | None = None
    close: Callable[[], None] | None = None
