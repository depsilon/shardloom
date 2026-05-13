#!/usr/bin/env python
"""Run traditional analytics benchmarks against external baseline engines.

This script is benchmark tooling only. It must not be imported by ShardLoom
runtime code and must not be used as fallback execution for unsupported
ShardLoom plans.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import math
import os
import platform
import shutil
import statistics
import sys
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ENGINE_ORDER = (
    "shardloom",
    "shardloom-vortex",
    "pandas",
    "polars",
    "duckdb",
    "spark-default",
    "spark-local-tuned",
    "datafusion",
    "dask",
)
ENGINE_ALIASES = {"spark": ("spark-default", "spark-local-tuned")}
BENCHMARK_SUITE = "local_analytics"
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
SCENARIO_ORDER = (
    "csv/file ingest",
    "selective filter",
    "group by aggregation",
    "sort and top-k",
    "hash join",
    "wide projection",
    "distinct count",
)
TAXONOMY_EXTRA_SCENARIO_ORDER = (
    "filter + projection + limit",
    "multi-key group by",
    "join + aggregate",
    "row number window",
    "partition pruning",
    "many-small-files scan",
    "null-heavy aggregate",
    "high-cardinality string group/distinct",
    "top-N per group",
    "malformed timestamp / dirty CSV",
    "small change over large base",
    "nested JSON field scan",
)
FORMAT_ORDER = ("csv", "jsonl", "parquet", "arrow-ipc", "avro", "orc")
DEFAULT_FORMAT_ORDER = ("csv", "parquet")
SHARDLOOM_VORTEX_FORMAT = "vortex"
STRESS_SCENARIO_ORDER = (
    "scale stress skewed join aggregation",
    "scale stress multi-stage etl",
)
SHARDLOOM_TRADITIONAL_SCENARIOS = SCENARIO_ORDER + STRESS_SCENARIO_ORDER
SHARDLOOM_TAXONOMY_EXTRA_SCENARIOS = (
    "filter + projection + limit",
    "multi-key group by",
    "join + aggregate",
    "row number window",
    "high-cardinality string group/distinct",
    "top-N per group",
)
SHARDLOOM_EXECUTABLE_SCENARIOS = (
    SCENARIO_ORDER + SHARDLOOM_TAXONOMY_EXTRA_SCENARIOS + STRESS_SCENARIO_ORDER
)
SCENARIO_BYTES = {
    "csv/file ingest": ("fact",),
    "selective filter": ("fact",),
    "group by aggregation": ("fact",),
    "sort and top-k": ("fact",),
    "hash join": ("fact", "dim"),
    "wide projection": ("fact",),
    "distinct count": ("fact",),
    "filter + projection + limit": ("fact",),
    "multi-key group by": ("fact",),
    "join + aggregate": ("fact", "dim"),
    "row number window": ("fact",),
    "partition pruning": ("fact",),
    "many-small-files scan": ("fact",),
    "null-heavy aggregate": ("fact",),
    "high-cardinality string group/distinct": ("fact",),
    "top-N per group": ("fact",),
    "malformed timestamp / dirty CSV": ("fact",),
    "small change over large base": ("fact",),
    "nested JSON field scan": ("fact",),
    "scale stress skewed join aggregation": ("fact", "dim"),
    "scale stress multi-stage etl": ("fact", "dim"),
}
DASK_BLOCKSIZE = "16MB"
DASK_SCHEDULER = "threads"
SHARDLOOM_BUILD_PROFILE = "release"
SHARDLOOM_RESULT_SINK = False
SHARDLOOM_CLAIM_GRADE_REQUIRED_EVIDENCE = (
    ("workload_scorecard_status", "workload_certified"),
    ("native_io_certificate_status", "certified"),
    ("output_replay_verified", "true"),
    ("computed_result_sink_replay_verified", "true"),
    ("computed_result_sink_native_io_certificate_status", "certified"),
    ("runtime_execution_certificate_status", "certified"),
    ("runtime_fallback_attempted", "false"),
    ("runtime_external_query_engine_invoked", "false"),
    ("layout_advisor_fallback_attempted", "false"),
    ("layout_advisor_external_engine_invoked", "false"),
    ("materialization_boundary_report_emitted", "true"),
    ("native_io_materializing_transitions_have_boundaries", "true"),
)
CORRECTNESS_FLOAT_DIGITS = 4


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
    cdc_delta_csv: Path | None = None
    nested_jsonl: Path | None = None


@dataclass(frozen=True)
class EngineRunner:
    name: str
    version: str
    scenarios: dict[str, Callable[[DatasetPaths, str], Any]]
    formats: tuple[str, ...] = ("csv",)
    prepare: Callable[[DatasetPaths], None] | None = None
    warmup: Callable[[], None] | None = None
    close: Callable[[], None] | None = None
    startup_time_millis: float | None = None


@dataclass(frozen=True)
class ScenarioMetadata:
    scenario_id: str
    name: str
    suite: str
    category: str
    default: bool
    stress: bool
    executable: bool
    dataset_profiles: tuple[str, ...]
    description: str


class BenchmarkUnsupported(RuntimeError):
    """Raised when an engine cannot execute a benchmark scenario yet."""


def scenario_catalog_path() -> Path:
    return Path(__file__).resolve().parents[1] / "common" / "scenario_catalog.json"


def load_scenario_catalog() -> dict[str, Any]:
    with scenario_catalog_path().open("r", encoding="utf-8") as handle:
        return json.load(handle)


def scenario_metadata_from_catalog(catalog: dict[str, Any]) -> dict[str, ScenarioMetadata]:
    metadata = {}
    for row in catalog["scenarios"]:
        metadata[row["name"]] = ScenarioMetadata(
            scenario_id=row["id"],
            name=row["name"],
            suite=row["suite"],
            category=row["category"],
            default=bool(row["default"]),
            stress=bool(row["stress"]),
            executable=bool(row["executable"]),
            dataset_profiles=tuple(row.get("dataset_profiles", [])),
            description=row.get("description", ""),
        )
    return metadata


SCENARIO_CATALOG = load_scenario_catalog()
SCENARIO_METADATA = scenario_metadata_from_catalog(SCENARIO_CATALOG)
EXECUTABLE_SCENARIO_ORDER = tuple(
    scenario["name"] for scenario in SCENARIO_CATALOG["scenarios"] if scenario["executable"]
)


def scenario_metadata(scenario: str) -> ScenarioMetadata:
    return SCENARIO_METADATA.get(
        scenario,
        ScenarioMetadata(
            scenario_id=scenario_slug(scenario),
            name=scenario,
            suite=BENCHMARK_SUITE,
            category="unknown",
            default=False,
            stress=False,
            executable=False,
            dataset_profiles=(DEFAULT_DATASET_PROFILE,),
            description="scenario is not present in the benchmark catalog",
        ),
    )


def taxonomy_default_scenarios(include_extra: bool, include_stress: bool) -> tuple[str, ...]:
    scenarios = list(SCENARIO_ORDER)
    if include_extra:
        scenarios.extend(TAXONOMY_EXTRA_SCENARIO_ORDER)
    if include_stress:
        scenarios.extend(STRESS_SCENARIO_ORDER)
    return tuple(scenario for scenario in scenarios if scenario in EXECUTABLE_SCENARIO_ORDER)


def engine_role(engine: str) -> str:
    if engine.startswith("shardloom"):
        return "shardloom_native"
    return "local_baseline"


def is_shardloom_engine(engine: str) -> bool:
    return engine.startswith("shardloom")


def expand_engine_aliases(engine_names: tuple[str, ...]) -> tuple[str, ...]:
    expanded: list[str] = []
    for engine in engine_names:
        for name in ENGINE_ALIASES.get(engine, (engine,)):
            if name not in expanded:
                expanded.append(name)
    return tuple(expanded)


class MemorySampler:
    def __init__(self) -> None:
        self._running = False
        self._thread: threading.Thread | None = None
        self.peak_bytes: int | None = None
        try:
            import psutil  # type: ignore
        except ImportError:
            self._psutil = None
            self._process = None
        else:
            self._psutil = psutil
            self._process = psutil.Process(os.getpid())

    def __enter__(self) -> "MemorySampler":
        if self._process is None:
            return self
        self._running = True
        self._sample()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._sample()

    def _sample_loop(self) -> None:
        while self._running:
            self._sample()
            time.sleep(0.01)

    def _sample(self) -> None:
        if self._process is None:
            return
        try:
            rss = self._process.memory_info().rss
            for child in self._process.children(recursive=True):
                try:
                    rss += child.memory_info().rss
                except Exception:
                    continue
        except Exception:
            return
        self.peak_bytes = rss if self.peak_bytes is None else max(self.peak_bytes, rss)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--dim-rows", type=int, default=1_000)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument(
        "--engines",
        default=",".join(ENGINE_ORDER),
        help="Comma-separated engines: shardloom,shardloom-vortex,pandas,polars,duckdb,spark-default,spark-local-tuned,datafusion,dask. Alias: spark expands to both Spark profiles.",
    )
    parser.add_argument(
        "--formats",
        default=",".join(DEFAULT_FORMAT_ORDER),
        help="Comma-separated external storage formats to run where supported: csv,jsonl,parquet,arrow-ipc,avro,orc. ShardLoom native Vortex rows use the shardloom-vortex engine.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=EXECUTABLE_SCENARIO_ORDER,
        help="Run one scenario. Repeat to run multiple scenarios.",
    )
    parser.add_argument(
        "--dataset-profile",
        default=DEFAULT_DATASET_PROFILE,
        choices=GENERATED_DATASET_PROFILES,
        help="Generated local dataset profile. Some advanced profiles emit fixture sidecars and remain claim-blocked until comparative coverage is promoted.",
    )
    parser.add_argument(
        "--include-taxonomy-extra",
        action="store_true",
        help="Include opt-in taxonomy scenarios beyond the default local analytics suite.",
    )
    parser.add_argument(
        "--include-stress",
        action="store_true",
        help="Include opt-in scale/shuffle stress scenarios. These are intended for Spark/Dask-style scale testing and may be inappropriate for small local runs.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent / ".generated" / "data",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path. Defaults to benchmarks/traditional_analytics/results/<timestamp>.json.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=None,
        help="Output Markdown report path. Defaults to the JSON path with .md extension.",
    )
    parser.add_argument("--no-markdown", action="store_true")
    parser.add_argument("--regenerate", action="store_true")
    parser.add_argument(
        "--dask-blocksize",
        default=DASK_BLOCKSIZE,
        help="Dask CSV blocksize, for example 16MB or 64MB. Use 'default' for Dask defaults.",
    )
    parser.add_argument(
        "--dask-scheduler",
        default=DASK_SCHEDULER,
        choices=("threads", "processes", "synchronous"),
        help="Dask scheduler used for compute calls.",
    )
    parser.add_argument(
        "--skip-shardloom-native",
        action="store_true",
        help="Skip ShardLoom native encoded microbenchmarks in the report.",
    )
    parser.add_argument(
        "--shardloom-build-profile",
        default=SHARDLOOM_BUILD_PROFILE,
        choices=("debug", "release"),
        help="Build profile for the ShardLoom CLI used by benchmark rows. Build time is excluded from per-scenario timing.",
    )
    parser.add_argument(
        "--cache-mode",
        default="warm-ish-local-filesystem",
        help="Declared cache mode for the report. The harness does not clear OS file cache.",
    )
    parser.add_argument(
        "--timing-scope",
        default="per-scenario operation only; engine initialization excluded",
        help="Human-readable timing scope recorded in the report.",
    )
    parser.add_argument(
        "--shardloom-native-iterations",
        type=int,
        default=None,
        help="Iterations for ShardLoom native microbenchmarks. Defaults to --iterations.",
    )
    parser.add_argument(
        "--shardloom-result-sink",
        action="store_true",
        help="For shardloom rows, also write and replay the computed result as a native Vortex result artifact.",
    )
    parser.add_argument(
        "--require-all-engines",
        action="store_true",
        help="Return nonzero after writing artifacts if any selected engine dependency is missing.",
    )
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be greater than zero")
    if args.dim_rows <= 0:
        parser.error("--dim-rows must be greater than zero")
    if args.iterations <= 0:
        parser.error("--iterations must be greater than zero")
    requested_engines = tuple(
        engine.strip().lower() for engine in args.engines.split(",") if engine.strip()
    )
    engines = expand_engine_aliases(requested_engines)
    unknown = sorted(set(engines) - set(ENGINE_ORDER))
    if unknown:
        parser.error(f"unknown engines: {','.join(unknown)}")
    args.engine_list = engines
    requested_formats = tuple(
        data_format.strip().lower()
        for data_format in args.formats.split(",")
        if data_format.strip()
    )
    unknown_formats = sorted(set(requested_formats) - set(FORMAT_ORDER))
    if unknown_formats:
        parser.error(f"unknown formats: {','.join(unknown_formats)}")
    if not requested_formats:
        parser.error("--formats must include at least one format")
    args.format_list = requested_formats
    if args.scenario:
        args.scenario_list = tuple(args.scenario)
    else:
        args.scenario_list = taxonomy_default_scenarios(
            args.include_taxonomy_extra, args.include_stress
        )
    args.shardloom_native_iterations = args.shardloom_native_iterations or args.iterations
    if args.shardloom_native_iterations <= 0:
        parser.error("--shardloom-native-iterations must be greater than zero")
    return args


def ensure_dataset(
    root: Path,
    rows: int,
    dim_rows: int,
    regenerate: bool,
    requested_formats: tuple[str, ...],
    dataset_profile: str,
) -> DatasetPaths:
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
    cdc_delta_csv = root / "cdc_delta.csv"
    nested_jsonl = root / "nested_fact.jsonl"
    metadata_json = root / "dataset.json"
    if regenerate and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    fact_extra_columns = generated_fact_extra_columns(dataset_profile)
    expected_metadata = {
        "rows": rows,
        "dim_rows": dim_rows,
        "schema_version": 6,
        "dataset_profile": dataset_profile,
        "dataset_file_shape": dataset_file_shape(dataset_profile),
        "fact_extra_columns": list(fact_extra_columns),
        "fact_file_part_count": fact_file_part_count(dataset_profile, rows),
        "formats": sorted(requested_formats),
    }
    required_paths = [fact_csv, dim_csv]
    if "jsonl" in requested_formats:
        required_paths.extend([fact_jsonl, dim_jsonl])
    if "parquet" in requested_formats:
        required_paths.extend([fact_parquet, dim_parquet])
    if "arrow-ipc" in requested_formats:
        required_paths.extend([fact_arrow_ipc, dim_arrow_ipc])
    if "avro" in requested_formats:
        required_paths.extend([fact_avro, dim_avro])
    if "orc" in requested_formats:
        required_paths.extend([fact_orc, dim_orc])
    if fact_file_part_count(dataset_profile, rows) > 0:
        required_paths.append(fact_csv_parts_dir)
        if "jsonl" in requested_formats:
            required_paths.append(fact_jsonl_parts_dir)
    if dataset_profile == "cdc_delta_overlay":
        required_paths.append(cdc_delta_csv)
    if dataset_profile == "nested_json":
        required_paths.append(nested_jsonl)
    if (
        all(path.exists() for path in required_paths)
        and metadata_json.exists()
    ):
        with metadata_json.open("r", encoding="utf-8") as handle:
            if json.load(handle) == expected_metadata:
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
                    cdc_delta_csv,
                    nested_jsonl,
                )

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
        cdc_delta_csv,
        nested_jsonl,
    )

    with metadata_json.open("w", encoding="utf-8") as handle:
        json.dump(expected_metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")

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
        cdc_delta_csv,
        nested_jsonl,
    )


def dataset_file_shape(dataset_profile: str) -> str:
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
    if dataset_profile == "many_small_files":
        return max(4, min(rows, 32))
    if dataset_profile == "few_large_files":
        return max(1, min(rows, 2))
    return 0


def write_profile_sidecars(
    fact_csv: Path,
    dataset_profile: str,
    rows: int,
    requested_formats: tuple[str, ...],
    fact_csv_parts_dir: Path,
    fact_jsonl_parts_dir: Path,
    cdc_delta_csv: Path,
    nested_jsonl: Path,
) -> None:
    part_count = fact_file_part_count(dataset_profile, rows)
    if part_count > 0:
        write_csv_parts(fact_csv, fact_csv_parts_dir, part_count)
        if "jsonl" in requested_formats:
            write_jsonl_part_copies(fact_csv_parts_dir, fact_jsonl_parts_dir)
    if dataset_profile == "cdc_delta_overlay":
        write_cdc_delta_overlay(fact_csv, cdc_delta_csv)
    if dataset_profile == "nested_json":
        write_nested_json_fixture(fact_csv, nested_jsonl)


def write_csv_parts(source_csv: Path, target_dir: Path, part_count: int) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
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
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_csv in sorted(source_dir.glob("part-*.csv")):
        write_jsonl_copy(
            source_csv,
            target_dir / f"{source_csv.stem}.jsonl",
            {
                "id": int,
                "group_key": int,
                "dim_key": int,
                "value": int,
                "metric": float,
                "flag": int,
                "category": str,
            },
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
    if dataset_profile == "high_cardinality_strings":
        return f"c{idx % 10_000}"
    if dataset_profile == "schema_drift":
        return f"c{group_key % 10}_v{1 + (idx % 3)}"
    return f"c{group_key % 10}"


def generated_fact_extra_columns(dataset_profile: str) -> tuple[str, ...]:
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
            values.append(str(idx % (32 if dataset_profile == "many_small_files" else 2)))
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
    write_jsonl_copy(
        fact_csv,
        fact_jsonl,
        {
            "id": int,
            "group_key": int,
            "dim_key": int,
            "value": int,
            "metric": float,
            "flag": int,
            "category": str,
        },
    )
    write_jsonl_copy(
        dim_csv,
        dim_jsonl,
        {"dim_key": int, "dim_label": str, "weight": float},
    )


def write_jsonl_copy(source_csv: Path, target_jsonl: Path, converters: dict[str, Callable[[str], Any]]) -> None:
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        with target_jsonl.open("w", encoding="utf-8") as target:
            for row in reader:
                typed = {}
                for key, value in row.items():
                    if key is None or value is None:
                        continue
                    converter = converters.get(key)
                    if converter is None:
                        typed[key] = None if value == "" else value
                    elif value == "":
                        typed[key] = None
                    else:
                        typed[key] = converter(value)
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

    fact_table = arrow_csv.read_csv(fact_csv)
    dim_table = arrow_csv.read_csv(dim_csv)
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

    fact_schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "fact",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "group_key", "type": "int"},
                {"name": "dim_key", "type": "int"},
                {"name": "value", "type": "int"},
                {"name": "metric", "type": "double"},
                {"name": "flag", "type": "int"},
                {"name": "category", "type": "string"},
            ],
        }
    )
    dim_schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "dim",
            "fields": [
                {"name": "dim_key", "type": "int"},
                {"name": "dim_label", "type": "string"},
                {"name": "weight", "type": "double"},
            ],
        }
    )
    write_avro_copy(
        fastavro,
        fact_csv,
        fact_avro,
        fact_schema,
        {
            "id": int,
            "group_key": int,
            "dim_key": int,
            "value": int,
            "metric": float,
            "flag": int,
            "category": str,
        },
    )
    write_avro_copy(
        fastavro,
        dim_csv,
        dim_avro,
        dim_schema,
        {"dim_key": int, "dim_label": str, "weight": float},
    )


def write_avro_copy(
    fastavro: Any,
    source_csv: Path,
    target_avro: Path,
    schema: dict[str, Any],
    converters: dict[str, Callable[[str], Any]],
) -> None:
    schema_fields = {field["name"] for field in schema["fields"]}
    with source_csv.open("r", newline="", encoding="utf-8") as source:
        records = [
            {
                key: converters[key](value)
                for key, value in row.items()
                if key in schema_fields and value is not None
            }
            for row in csv.DictReader(source)
        ]
    with target_avro.open("wb") as target:
        fastavro.writer(target, schema, records)


def module_version(name: str) -> str:
    module = importlib.import_module(name)
    return str(getattr(module, "__version__", "unknown"))


def shardloom_runner() -> EngineRunner:
    root = workspace_root()
    binary = build_shardloom_cli(
        root,
        "vortex-traditional-analytics-benchmark",
        SHARDLOOM_BUILD_PROFILE,
    )
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")

    def run_scenario(scenario: str, paths: DatasetPaths, data_format: str) -> Any:
        workspace = (
            paths.root / "shardloom_universal_io" / data_format / scenario_slug(scenario)
        )
        command = [
            str(binary),
            "traditional-analytics-run",
            scenario,
            str(fact_path(paths, data_format)),
            str(dim_path(paths, data_format)),
            "--workspace",
            str(workspace),
            "--input-format",
            data_format,
            "--format",
            "json",
        ]
        if SHARDLOOM_RESULT_SINK:
            command.extend(["--verify-native-replay", "--write-result-vortex"])
        completed = subprocess_run(command, root, env)
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            if completed["returncode"] != 0:
                raise RuntimeError(
                    completed["stderr"] or completed["stdout"] or "unknown failure"
                ) from exc
            raise RuntimeError(f"ShardLoom emitted invalid JSON: {exc}") from exc
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        fields = parse_output_fields(payload)
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        required_true_fields = [
            "native_work_envelope_created",
            "native_work_stream_created",
            "native_result_stream_created",
            "native_io_certificate_emitted",
            "compatibility_source_adapter_used",
            "compatibility_to_vortex_import_performed",
            "resource_auto_sizing_enabled",
            "dynamic_sizing_applied",
            "partitioning_auto_derived",
            "vortex_file_written",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "materialization_boundary_report_emitted",
            "native_io_per_path_certificate_emitted",
            "native_io_materializing_transitions_have_boundaries",
            "runtime_task_graph_created",
            "runtime_task_graph_executed",
            "runtime_queue_limit_enforced",
            "runtime_backpressure_bounded",
            "runtime_cancellation_testable",
            "runtime_retry_testable",
            "runtime_fail_before_oom_enforced",
            "layout_advisor_report_emitted",
        ]
        missing_evidence = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_evidence:
            raise RuntimeError(
                "ShardLoom universal I/O evidence was missing: "
                + ", ".join(missing_evidence)
            )
        if fields.get("native_io_certificate_status") != "certified":
            raise RuntimeError(
                "ShardLoom NativeIoCertificate was not certified: "
                + str(fields.get("native_io_certificate_status", "missing"))
            )
        if SHARDLOOM_RESULT_SINK:
            for field in (
                "computed_result_sink_requested",
                "computed_result_sink_written",
                "computed_result_sink_replay_verified",
            ):
                if fields.get(field) != "true":
                    raise RuntimeError(
                        "ShardLoom result sink evidence was missing: " + field
                    )
            if (
                fields.get("computed_result_sink_native_io_certificate_status")
                != "certified"
            ):
                raise RuntimeError(
                    "ShardLoom result sink NativeIoCertificate was not certified: "
                    + str(
                        fields.get(
                            "computed_result_sink_native_io_certificate_status", "missing"
                        )
                    )
                )
            if fields.get("runtime_execution_certificate_status") != "certified":
                raise RuntimeError(
                    "ShardLoom runtime execution certificate was not certified: "
                    + str(fields.get("runtime_execution_certificate_status", "missing"))
                )
            if fields.get("runtime_fallback_attempted") != "false":
                raise RuntimeError("ShardLoom runtime fallback_attempted was not false")
            if fields.get("runtime_external_query_engine_invoked") != "false":
                raise RuntimeError(
                    "ShardLoom runtime external query engine invocation was not false"
                )
            if fields.get("runtime_memory_reservations_requested") != fields.get(
                "runtime_memory_reservations_released"
            ):
                raise RuntimeError(
                    "ShardLoom runtime memory reservations were not released"
                )
            if fields.get("layout_advisor_status") != "report_only":
                raise RuntimeError(
                    "ShardLoom layout advisor status was not report_only: "
                    + str(fields.get("layout_advisor_status", "missing"))
                )
            if fields.get("layout_advisor_improvement_claim_allowed") != "false":
                raise RuntimeError("ShardLoom layout advisor allowed an improvement claim")
            if fields.get("layout_advisor_write_layout_execution_allowed") != "false":
                raise RuntimeError(
                    "ShardLoom layout advisor allowed write-layout execution"
                )
            if fields.get("layout_advisor_fallback_attempted") != "false":
                raise RuntimeError("ShardLoom layout advisor fallback_attempted was not false")
            if fields.get("layout_advisor_external_engine_invoked") != "false":
                raise RuntimeError(
                    "ShardLoom layout advisor external engine invocation was not false"
                )
        if (
            fields.get("native_io_certificate_path_id")
            != "compatibility_source_to_native_vortex_sink"
        ):
            raise RuntimeError(
                "ShardLoom NativeIoCertificate path was unexpected: "
                + str(fields.get("native_io_certificate_path_id", "missing"))
            )
        if fields.get("source_format") != shardloom_source_format(data_format):
            raise RuntimeError(
                "ShardLoom source format was unexpected: "
                + str(fields.get("source_format", "missing"))
            )
        result_json = fields.get("result_json")
        if result_json is None:
            raise RuntimeError("ShardLoom result_json field was missing")
        return {
            "__benchmark_result": json.loads(result_json),
            "__shardloom_evidence": fields,
        }

    return EngineRunner(
        "shardloom",
        shardloom_version(root, SHARDLOOM_BUILD_PROFILE),
        {
            scenario: (
                lambda paths, data_format, scenario=scenario: run_scenario(
                    scenario, paths, data_format
                )
            )
            for scenario in SHARDLOOM_EXECUTABLE_SCENARIOS
        },
        formats=FORMAT_ORDER,
    )


def shardloom_vortex_runner() -> EngineRunner:
    root = workspace_root()
    binary = build_shardloom_cli(
        root,
        "vortex-traditional-analytics-benchmark",
        SHARDLOOM_BUILD_PROFILE,
    )
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    prepared_paths: dict[str, Path] = {}

    def prepare(paths: DatasetPaths) -> None:
        if prepared_paths:
            return
        workspace = paths.root / "shardloom_native_vortex_inputs"
        command = [
            str(binary),
            "traditional-analytics-run",
            "csv/file ingest",
            str(paths.fact_csv),
            str(paths.dim_csv),
            "--workspace",
            str(workspace),
            "--format",
            "json",
        ]
        completed = subprocess_run(command, root, env)
        if completed["returncode"] != 0:
            raise BenchmarkUnsupported(
                completed["stderr"] or completed["stdout"] or "native Vortex input setup failed"
            )
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            raise BenchmarkUnsupported(
                f"ShardLoom native Vortex setup emitted invalid JSON: {exc}"
            ) from exc
        fields = parse_output_fields(payload)
        fact_vortex = Path(fields.get("fact_vortex_path", ""))
        dim_vortex = Path(fields.get("dim_vortex_path", ""))
        if not fact_vortex.exists() or not dim_vortex.exists():
            raise BenchmarkUnsupported(
                "ShardLoom native Vortex setup did not produce fact/dim .vortex files"
            )
        prepared_paths["fact"] = fact_vortex
        prepared_paths["dim"] = dim_vortex

    def run_scenario(scenario: str, paths: DatasetPaths, data_format: str) -> Any:
        if data_format != SHARDLOOM_VORTEX_FORMAT:
            raise BenchmarkUnsupported("shardloom-vortex only runs native .vortex inputs")
        prepare(paths)
        command = [
            str(binary),
            "traditional-analytics-vortex-run",
            scenario,
            str(prepared_paths["fact"]),
            str(prepared_paths["dim"]),
            "--format",
            "json",
        ]
        completed = subprocess_run(command, root, env)
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            if completed["returncode"] != 0:
                raise RuntimeError(
                    completed["stderr"] or completed["stdout"] or "unknown failure"
                ) from exc
            raise RuntimeError(f"ShardLoom emitted invalid JSON: {exc}") from exc
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        fields = parse_output_fields(payload)
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        required_true_fields = [
            "native_work_envelope_created",
            "native_work_stream_created",
            "native_result_stream_created",
            "native_io_certificate_emitted",
            "vortex_source_adapter_used",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "materialization_boundary_report_emitted",
            "native_io_per_path_certificate_emitted",
            "native_io_materializing_transitions_have_boundaries",
        ]
        missing_evidence = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_evidence:
            raise RuntimeError(
                "ShardLoom native Vortex evidence was missing: "
                + ", ".join(missing_evidence)
            )
        if fields.get("native_io_certificate_status") != "certified":
            raise RuntimeError(
                "ShardLoom NativeIoCertificate was not certified: "
                + str(fields.get("native_io_certificate_status", "missing"))
            )
        if (
            fields.get("native_io_certificate_path_id")
            != "native_vortex_source_to_native_runtime_result"
        ):
            raise RuntimeError(
                "ShardLoom NativeIoCertificate path was unexpected: "
                + str(fields.get("native_io_certificate_path_id", "missing"))
            )
        result_json = fields.get("result_json")
        if result_json is None:
            raise RuntimeError("ShardLoom result_json field was missing")
        return {
            "__benchmark_result": json.loads(result_json),
            "__shardloom_evidence": fields,
        }

    return EngineRunner(
        "shardloom-vortex",
        shardloom_version(root, SHARDLOOM_BUILD_PROFILE),
        {
            scenario: (
                lambda paths, data_format, scenario=scenario: run_scenario(
                    scenario, paths, data_format
                )
            )
            for scenario in SHARDLOOM_TRADITIONAL_SCENARIOS
        },
        formats=(SHARDLOOM_VORTEX_FORMAT,),
        prepare=prepare,
    )


def available_runners(engine_names: tuple[str, ...]) -> tuple[dict[str, EngineRunner], dict[str, str]]:
    runners: dict[str, EngineRunner] = {}
    missing: dict[str, str] = {}
    for engine in engine_names:
        try:
            started = time.perf_counter()
            runner = ENGINE_FACTORIES[engine]()
            startup_time = (time.perf_counter() - started) * 1000.0
            runners[engine] = replace(runner, startup_time_millis=round(startup_time, 4))
        except Exception as exc:
            missing[engine] = f"{type(exc).__name__}: {exc}"
    return runners, missing


def warmup_runner(runner: EngineRunner) -> EngineRunner:
    if runner.warmup is None:
        return runner
    started = time.perf_counter()
    runner.warmup()
    warmup_time = (time.perf_counter() - started) * 1000.0
    startup_time = (runner.startup_time_millis or 0.0) + warmup_time
    return replace(runner, startup_time_millis=round(startup_time, 4))


def prepare_runner(runner: EngineRunner, paths: DatasetPaths) -> EngineRunner:
    if runner.prepare is None:
        return runner
    started = time.perf_counter()
    runner.prepare(paths)
    prepare_time = (time.perf_counter() - started) * 1000.0
    startup_time = (runner.startup_time_millis or 0.0) + prepare_time
    return replace(runner, startup_time_millis=round(startup_time, 4))


def round_float(value: Any) -> float:
    if value is None:
        return 0.0
    number = float(value)
    if math.isnan(number):
        return 0.0
    return round(number, CORRECTNESS_FLOAT_DIGITS)


def normalize_scalar_result(row_count: Any, metric_sum: Any) -> dict[str, Any]:
    return {"row_count": int(row_count), "metric_sum": round_float(metric_sum)}


def parse_output_fields(payload: dict[str, Any]) -> dict[str, str]:
    return {
        str(field.get("key")): str(field.get("value"))
        for field in payload.get("fields", [])
        if isinstance(field, dict) and "key" in field
    }


def parse_optional_int(value: Any) -> int | None:
    if value is None or value == "none" or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_optional_bool(value: Any) -> bool | None:
    if value is None or value == "none" or value == "":
        return None
    text = str(value).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    return None


def first_meaningful_field(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "" and value != "none":
            return value
    return None


def unwrap_engine_value(value: Any) -> tuple[Any, dict[str, str]]:
    if (
        isinstance(value, dict)
        and "__benchmark_result" in value
        and isinstance(value.get("__shardloom_evidence"), dict)
    ):
        return value["__benchmark_result"], {
            str(key): str(field_value)
            for key, field_value in value["__shardloom_evidence"].items()
        }
    return value, {}


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def shardloom_binary_path(root: Path, profile: str) -> Path:
    binary_name = "shardloom.exe" if os.name == "nt" else "shardloom"
    target_profile = "release" if profile == "release" else "debug"
    return root / "target" / target_profile / binary_name


def build_shardloom_cli(root: Path, features: str, profile: str) -> Path:
    cargo = shutil.which("cargo")
    if cargo is None:
        raise BenchmarkUnsupported("cargo was not found on PATH, so ShardLoom could not be built")
    build_command = [
        cargo,
        "build",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        features,
    ]
    if profile == "release":
        build_command.append("--release")
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    completed = subprocess_run(build_command, root, env)
    if completed["returncode"] != 0:
        raise BenchmarkUnsupported(
            "ShardLoom CLI build failed before benchmark timing began: "
            + (completed["stderr"] or completed["stdout"] or "unknown failure")
        )
    binary = shardloom_binary_path(root, profile)
    if not binary.exists():
        raise BenchmarkUnsupported(f"ShardLoom binary was not found after build: {binary}")
    return binary


def shardloom_version(root: Path, profile: str) -> str:
    git = shutil.which("git")
    if git is None:
        return f"workspace-local-{profile}"
    completed = subprocess_run([git, "rev-parse", "--short", "HEAD"], root, os.environ.copy())
    if completed["returncode"] != 0:
        return f"workspace-local-{profile}"
    version = f"workspace-local-{profile}-{completed['stdout'].strip()}"
    dirty = subprocess_run(
        [git, "status", "--short", "--untracked-files=no"], root, os.environ.copy()
    )
    if dirty["returncode"] == 0 and dirty["stdout"].strip():
        version += "-dirty"
    return version


def scenario_slug(scenario: str) -> str:
    return (
        scenario.lower()
        .replace("/", "-")
        .replace(" ", "-")
        .replace("_", "-")
    )


def normalize_group_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                key: str(row[key]) if key == "dim_label" else int(row[key]),
                "row_count": int(row["row_count"]),
                "metric_sum": round_float(row["metric_sum"]),
            }
        )
    return sorted(normalized, key=lambda row: row[key])


def normalize_top_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {"id": int(row["id"]), "metric": round_float(row["metric"])} for row in rows
    ]
    return sorted(normalized, key=lambda row: (-row["metric"], row["id"]))[:10]


def normalize_multi_group_rows(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized_row = {
            key: str(row[key]) if key in {"category", "dim_label"} else int(row[key])
            for key in keys
        }
        normalized_row["row_count"] = int(row["row_count"])
        normalized_row["metric_sum"] = round_float(row["metric_sum"])
        normalized.append(normalized_row)
    return sorted(normalized, key=lambda row: tuple(row[key] for key in keys))


def normalize_rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "group_key": int(row["group_key"]),
            "id": int(row["id"]),
            "metric": round_float(row["metric"]),
            "rank": int(row.get("rank", row.get("row_number", 1))),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: (row["group_key"], row["rank"], row["id"]))


def normalize_top_group_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "group_key": int(row["group_key"]),
            "id": int(row["id"]),
            "metric": round_float(row["metric"]),
            "rank": int(row["rank"]),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: (row["group_key"], row["rank"], row["id"]))


def normalize_complex_etl_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "dim_label": str(row["dim_label"]),
            "bucket": int(row["bucket"]),
            "row_count": int(row["row_count"]),
            "metric_sum": round_float(row["metric_sum"]),
            "weighted_sum": round_float(row["weighted_sum"]),
        }
        for row in rows
    ]
    return sorted(
        normalized,
        key=lambda row: (-row["weighted_sum"], row["dim_label"], row["bucket"]),
    )[:20]


def canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sql_literal(path: Path) -> str:
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


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
    return ()


def scenario_display_name(data_format: str, scenario: str) -> str:
    return f"{data_format}: {scenario}"


def shardloom_source_format(data_format: str) -> str:
    return "arrow_ipc" if data_format == "arrow-ipc" else data_format


def pyarrow_rows(batches: list[Any]) -> list[dict[str, Any]]:
    import pyarrow as pa  # type: ignore

    if not batches:
        return []
    return pa.Table.from_batches(batches).to_pylist()


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
        path = fact_path(paths, data_format)
        if data_format == "parquet":
            return pd.read_parquet(path)
        if data_format == "jsonl":
            return pd.read_json(path, lines=True)
        if data_format == "arrow-ipc":
            return pd.read_feather(path)
        if data_format == "orc":
            return pd.read_orc(path)
        return pd.read_csv(path)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        path = dim_path(paths, data_format)
        if data_format == "parquet":
            return pd.read_parquet(path)
        if data_format == "jsonl":
            return pd.read_json(path, lines=True)
        if data_format == "arrow-ipc":
            return pd.read_feather(path)
        if data_format == "orc":
            return pd.read_orc(path)
        return pd.read_csv(path)

    def read_fact_parts(paths: DatasetPaths, data_format: str) -> Any:
        parts = fact_part_paths(paths, data_format)
        if not parts:
            raise BenchmarkUnsupported(
                f"{paths.dataset_profile} does not have {data_format} fact parts"
            )
        frames = []
        for part in parts:
            if data_format == "jsonl":
                frames.append(pd.read_json(part, lines=True))
            else:
                frames.append(pd.read_csv(part))
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
            "malformed timestamp / dirty CSV": malformed_timestamp_dirty_csv,
            "small change over large base": small_change_over_large_base,
            "nested JSON field scan": nested_json_field_scan,
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet", "arrow-ipc", "orc"),
    )


def polars_runner() -> EngineRunner:
    import polars as pl  # type: ignore

    def read_fact(paths: DatasetPaths, data_format: str) -> Any:
        path = fact_path(paths, data_format)
        if data_format == "parquet":
            return pl.read_parquet(path)
        if data_format == "jsonl":
            return pl.read_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.read_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path)
        return pl.read_csv(path)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        path = dim_path(paths, data_format)
        if data_format == "parquet":
            return pl.read_parquet(path)
        if data_format == "jsonl":
            return pl.read_ndjson(path)
        if data_format == "arrow-ipc":
            return pl.read_ipc(path)
        if data_format == "avro":
            return pl.read_avro(path)
        return pl.read_csv(path)

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
        "polars",
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet", "arrow-ipc", "avro"),
    )


def duckdb_runner() -> EngineRunner:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")

    def table_expr(paths: DatasetPaths, table: str, data_format: str) -> str:
        path = fact_path(paths, data_format) if table == "fact" else dim_path(paths, data_format)
        if data_format == "parquet":
            function = "read_parquet"
        elif data_format == "jsonl":
            function = "read_json_auto"
        else:
            function = "read_csv_auto"
        return f"{function}({sql_literal(path)})"

    def query(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        sql = sql.replace("{fact}", table_expr(paths, "fact", data_format)).replace(
            "{dim}", table_expr(paths, "dim", data_format)
        )
        columns = [column[0] for column in con.execute(sql).description]
        return [dict(zip(columns, row)) for row in con.fetchall()]

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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet"),
        close=con.close,
    )


def spark_runner(profile: str) -> EngineRunner:
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        raise BenchmarkUnsupported(
            "Spark/PySpark requires a local JDK. Install JDK 17 or newer, set JAVA_HOME, "
            "and ensure java is on PATH before running Spark benchmark rows."
        )
    import pyspark  # type: ignore
    from pyspark.sql import SparkSession, functions as F  # type: ignore
    from pyspark.sql.window import Window  # type: ignore

    builder = SparkSession.builder.master("local[*]").appName(
        f"shardloom-traditional-analytics-benchmark-{profile}"
    )
    builder = builder.config("spark.ui.enabled", "false")
    profile_notes = ["master=local[*]", "spark.ui.enabled=false"]
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
    elif profile != "default":
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
        "spark-default" if profile == "default" else "spark-local-tuned",
        f"{module_version('pyspark')} ({'; '.join(profile_notes)})",
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet", "orc"),
        warmup=warmup_spark,
        close=close_spark,
    )


def spark_default_runner() -> EngineRunner:
    return spark_runner("default")


def spark_local_tuned_runner() -> EngineRunner:
    return spark_runner("local-tuned")


def datafusion_runner() -> EngineRunner:
    import datafusion  # type: ignore

    def query(paths: DatasetPaths, data_format: str, sql: str) -> list[dict[str, Any]]:
        ctx = datafusion.SessionContext()
        if data_format == "parquet":
            ctx.register_parquet("fact", paths.fact_parquet)
            ctx.register_parquet("dim", paths.dim_parquet)
        else:
            ctx.register_csv("fact", paths.fact_csv, has_header=True)
            ctx.register_csv("dim", paths.dim_csv, has_header=True)
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "parquet"),
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
        return dd.read_csv(paths.fact_csv, blocksize=blocksize)

    def read_dim(paths: DatasetPaths, data_format: str) -> Any:
        if data_format == "parquet":
            return dd.read_parquet(paths.dim_parquet)
        if data_format == "jsonl":
            return dd.read_json(paths.dim_jsonl, lines=True, blocksize=blocksize)
        return dd.read_csv(paths.dim_csv, blocksize=blocksize)

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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        formats=("csv", "jsonl", "parquet"),
    )


ENGINE_FACTORIES: dict[str, Callable[[], EngineRunner]] = {
    "shardloom": shardloom_runner,
    "shardloom-vortex": shardloom_vortex_runner,
    "pandas": pandas_runner,
    "polars": polars_runner,
    "duckdb": duckdb_runner,
    "spark-default": spark_default_runner,
    "spark-local-tuned": spark_local_tuned_runner,
    "datafusion": datafusion_runner,
    "dask": dask_runner,
}


def maybe_path_size(path: Path) -> int | None:
    return path.stat().st_size if path.exists() else None


def scenario_bytes(paths: DatasetPaths, scenario: str, data_format: str) -> int:
    if data_format == SHARDLOOM_VORTEX_FORMAT:
        return 0
    if scenario == "many-small-files scan":
        parts = fact_part_paths(paths, data_format)
        if parts:
            return sum(path.stat().st_size for path in parts)
    total = 0
    for name in SCENARIO_BYTES[scenario]:
        path = fact_path(paths, data_format) if name == "fact" else dim_path(paths, data_format)
        total += path.stat().st_size
    return total


def rows_scanned(paths: DatasetPaths, scenario: str) -> int:
    if scenario in {
        "hash join",
        "join + aggregate",
        "scale stress skewed join aggregation",
        "scale stress multi-stage etl",
    }:
        return paths.rows + paths.dim_rows
    return paths.rows


def rows_materialized(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        return int(value.get("row_count", 1))
    return 1


def materialization_policy(engine: str, data_format: str) -> str:
    if engine == "shardloom-vortex" or data_format == SHARDLOOM_VORTEX_FORMAT:
        return "native_vortex_input_prepared_before_scenario_timing"
    if engine == "shardloom":
        return "compatibility_source_to_local_vortex_import_included"
    return "engine_local_compatibility_reader_policy"


def native_vortex_or_compatibility_import(engine: str, data_format: str) -> str:
    if data_format == SHARDLOOM_VORTEX_FORMAT:
        return "native_vortex"
    if engine == "shardloom":
        return "compatibility_import_to_vortex"
    return "compatibility_format_baseline"


def shardloom_claim_grade_missing_evidence(result: dict[str, Any]) -> list[str]:
    evidence = result.get("shardloom_evidence", {})
    missing: list[str] = []
    for field, expected in SHARDLOOM_CLAIM_GRADE_REQUIRED_EVIDENCE:
        actual = str(evidence.get(field, "missing")).lower()
        if actual != expected:
            missing.append(f"{field}!={expected} (actual={actual})")
    if not evidence.get("benchmark_row_ref"):
        missing.append("benchmark_row_ref missing")
    if not evidence.get("coverage_row_ref"):
        missing.append("coverage_row_ref missing")
    if result.get("fallback_attempted", False):
        missing.append("result fallback_attempted was true")
    return missing


def claim_grade_readiness(result: dict[str, Any]) -> dict[str, Any]:
    engine = result["engine"]
    status = result["status"]
    if not is_shardloom_engine(engine):
        return {
            "claim_gate_status": "external_baseline_only",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [
                "external baseline rows are comparison-only"
            ],
        }
    if status != "success":
        return {
            "claim_gate_status": "unsupported" if status == "unsupported" else "blocked",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [result.get("reason", status)],
        }
    if engine == "shardloom-vortex":
        return {
            "claim_gate_status": "fixture_smoke_only",
            "claim_grade_requirements_met": False,
            "claim_grade_missing_evidence": [
                "native Vortex lane lacks workload scorecard/result-sink replay evidence"
            ],
        }
    missing = shardloom_claim_grade_missing_evidence(result)
    claim_grade = not missing
    return {
        "claim_gate_status": "claim_grade" if claim_grade else "not_claim_grade",
        "claim_grade_requirements_met": claim_grade,
        "claim_grade_missing_evidence": missing,
    }


def benchmark_constitution(
    result: dict[str, Any],
    cache_mode: str,
    dataset_profile: str,
) -> dict[str, Any]:
    metadata = scenario_metadata(result["scenario_base"])
    engine = result["engine"]
    data_format = result["storage_format"]
    return {
        "constitution_id": (
            f"{metadata.scenario_id}:{engine}:{data_format}:{dataset_profile}"
        ),
        "scenario_id": metadata.scenario_id,
        "scenario_category": metadata.category,
        "dataset_profile": dataset_profile,
        "engine_role": engine_role(engine),
        "input_format": data_format,
        "table_format": "none",
        "storage_mode": "local_filesystem",
        "native_vortex_or_compatibility_import": native_vortex_or_compatibility_import(
            engine, data_format
        ),
        "startup_included": False,
        "conversion_included": engine == "shardloom" and data_format != SHARDLOOM_VORTEX_FORMAT,
        "staging_included": engine.startswith("shardloom"),
        "result_delivery_included": True,
        "write_included": engine == "shardloom" and SHARDLOOM_RESULT_SINK,
        "cache_mode": cache_mode,
        "iterations": result["iterations"],
        "warmup_policy": "engine startup/warmup recorded separately",
        "correctness_oracle": "first successful digest per formatted scenario",
        "materialization_policy": materialization_policy(engine, data_format),
        "resource_policy": "engine defaults; ShardLoom auto sizing recorded in evidence",
        "claim_level": result.get("claim_gate_status", "not_claim_grade"),
    }


def annotate_result(
    result: dict[str, Any],
    cache_mode: str,
    dataset_profile: str,
) -> dict[str, Any]:
    metadata = scenario_metadata(result["scenario_base"])
    readiness = claim_grade_readiness(result)
    result["benchmark_suite"] = metadata.suite
    result["scenario_id"] = metadata.scenario_id
    result["scenario_category"] = metadata.category
    result["dataset_profile"] = dataset_profile
    result["engine_role"] = engine_role(result["engine"])
    result["claim_gate_status"] = readiness["claim_gate_status"]
    result["claim_grade_requirements_met"] = readiness["claim_grade_requirements_met"]
    result["claim_grade_missing_evidence"] = readiness["claim_grade_missing_evidence"]
    result["benchmark_constitution"] = benchmark_constitution(
        result, cache_mode, dataset_profile
    )
    return result


def coverage_status(result: dict[str, Any]) -> str:
    if result["status"] != "success":
        if result["status"] == "unsupported":
            return "unsupported"
        if result["status"] == "missing_dependency":
            return "blocked"
        return "blocked"
    if result["engine"] == "shardloom-vortex":
        return "fixture_smoke_only"
    if result["engine"] == "shardloom":
        return str(result.get("claim_gate_status", "not_claim_grade"))
    return "external_baseline_only"


def coverage_table(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for result in results:
        constitution = result["benchmark_constitution"]
        rows.append(
            {
                "scenario_name": result["scenario_name"],
                "scenario_id": result["scenario_id"],
                "scenario_category": result["scenario_category"],
                "dataset_profile": result["dataset_profile"],
                "engine": result["engine"],
                "engine_role": result["engine_role"],
                "status": coverage_status(result),
                "timing_row_present": result["metrics"]["query_runtime_millis"] is not None,
                "claim_gate_status": result["claim_gate_status"],
                "claim_grade_requirements_met": result["claim_grade_requirements_met"],
                "claim_grade_missing_evidence": result["claim_grade_missing_evidence"],
                "timing_row_claim_grade": (
                    result["metrics"]["query_runtime_millis"] is not None
                    and result["claim_grade_requirements_met"]
                ),
                "benchmark_constitution_id": constitution["constitution_id"],
                "certificate_status": result.get("shardloom_evidence", {}).get(
                    "native_io_certificate_status"
                ),
                "native_io_status_required": is_shardloom_engine(result["engine"]),
                "materialization_policy": constitution["materialization_policy"],
                "fallback_attempted": result.get("fallback_attempted", False),
                "external_engine_invoked": (
                    not is_shardloom_engine(result["engine"])
                    and result["status"] == "success"
                ),
            }
        )
    return rows


def catalog_coverage_summary(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": scenario["id"],
            "scenario_name": scenario["name"],
            "suite": scenario["suite"],
            "scenario_category": scenario["category"],
            "executable_in_local_runner": bool(scenario["executable"]),
            "default": bool(scenario["default"]),
            "stress": bool(scenario["stress"]),
            "dataset_profiles": scenario.get("dataset_profiles", []),
        }
        for scenario in catalog["scenarios"]
    ]


def failed_result(
    engine: str,
    scenario: str,
    data_format: str,
    status: str,
    reason: str,
    paths: DatasetPaths,
    iterations: int,
    elapsed_millis: float | None = None,
) -> dict[str, Any]:
    metrics = {
        "wall_time_millis": round(elapsed_millis, 4) if elapsed_millis is not None else None,
        "query_runtime_millis": round(elapsed_millis, 4) if elapsed_millis is not None else None,
        "peak_memory_bytes": None,
        "bytes_read": scenario_bytes(paths, scenario, data_format),
        "bytes_written": None,
        "rows_scanned": rows_scanned(paths, scenario),
        "rows_materialized": 0,
        "data_decoded": None,
        "data_materialized": None,
        "row_read": None,
        "arrow_converted": None,
        "object_store_io": None,
        "write_io": None,
        "spill_io_performed": None,
        "object_store_requests": 0,
        "spill_required_bytes": None,
    }
    return {
        "scenario_name": scenario_display_name(data_format, scenario),
        "scenario_base": scenario,
        "storage_format": data_format,
        "engine": engine,
        "status": status,
        "reason": reason,
        "iterations": iterations,
        "iteration_wall_time_millis": [] if elapsed_millis is None else [round(elapsed_millis, 4)],
        "metrics": metrics,
        "correctness_digest": None,
        "output_preview": None,
        "shardloom_evidence": {},
        "fallback_attempted": False,
        "external_baseline_only": not is_shardloom_engine(engine),
    }


def run_one(
    runner: EngineRunner,
    paths: DatasetPaths,
    scenario: str,
    data_format: str,
    iterations: int,
) -> dict[str, Any]:
    scenario_fn = runner.scenarios.get(scenario)
    if scenario_fn is None:
        return failed_result(
            runner.name,
            scenario,
            data_format,
            "unsupported",
            f"{runner.name} does not implement benchmark scenario: {scenario}",
            paths,
            iterations,
        )
    values = []
    evidence_rows = []
    timings = []
    peak_memory = []
    for _ in range(iterations):
        started = time.perf_counter()
        with MemorySampler() as sampler:
            try:
                value, evidence = unwrap_engine_value(scenario_fn(paths, data_format))
            except BenchmarkUnsupported as exc:
                elapsed = (time.perf_counter() - started) * 1000.0
                return failed_result(
                    runner.name,
                    scenario,
                    data_format,
                    "unsupported",
                    str(exc),
                    paths,
                    iterations,
                    elapsed,
                )
            except Exception as exc:
                elapsed = (time.perf_counter() - started) * 1000.0
                return failed_result(
                    runner.name,
                    scenario,
                    data_format,
                    "execution_error",
                    f"{type(exc).__name__}: {exc}",
                    paths,
                    iterations,
                    elapsed,
                )
            else:
                elapsed = time.perf_counter() - started
        values.append(value)
        evidence_rows.append(evidence)
        timings.append(elapsed * 1000.0)
        if sampler.peak_bytes is not None:
            peak_memory.append(sampler.peak_bytes)

    digest = canonical_digest(values[-1])
    stable = all(canonical_digest(value) == digest for value in values)
    evidence = evidence_rows[-1] if evidence_rows else {}
    bytes_written = None
    if evidence:
        fact_vortex_bytes = parse_optional_int(evidence.get("fact_vortex_bytes"))
        dim_vortex_bytes = parse_optional_int(evidence.get("dim_vortex_bytes"))
        if fact_vortex_bytes is not None or dim_vortex_bytes is not None:
            bytes_written = (fact_vortex_bytes or 0) + (dim_vortex_bytes or 0)
    bytes_read = parse_optional_int(evidence.get("source_bytes_read")) if evidence else None
    return {
        "scenario_name": scenario_display_name(data_format, scenario),
        "scenario_base": scenario,
        "storage_format": data_format,
        "engine": runner.name,
        "status": "success" if stable else "unstable_output",
        "iterations": iterations,
        "iteration_wall_time_millis": [round(value, 4) for value in timings],
        "metrics": {
            "wall_time_millis": round(sum(timings), 4),
            "query_runtime_millis": round(statistics.mean(timings), 4),
            "peak_memory_bytes": max(peak_memory) if peak_memory else None,
            "bytes_read": bytes_read
            if bytes_read is not None
            else scenario_bytes(paths, scenario, data_format),
            "bytes_written": bytes_written,
            "rows_scanned": rows_scanned(paths, scenario),
            "rows_materialized": parse_optional_int(evidence.get("rows_materialized"))
            if evidence
            else rows_materialized(values[-1]),
            "data_decoded": parse_optional_bool(evidence.get("data_decoded")),
            "data_materialized": parse_optional_bool(evidence.get("data_materialized")),
            "row_read": parse_optional_bool(evidence.get("row_read")),
            "arrow_converted": parse_optional_bool(evidence.get("arrow_converted")),
            "object_store_io": parse_optional_bool(evidence.get("object_store_io")),
            "write_io": parse_optional_bool(evidence.get("write_io")),
            "spill_io_performed": parse_optional_bool(evidence.get("spill_io_performed")),
            "object_store_requests": 0,
            "spill_required_bytes": None,
        },
        "correctness_digest": digest,
        "output_preview": values[-1] if not isinstance(values[-1], list) else values[-1][:5],
        "shardloom_evidence": evidence,
        "fallback_attempted": False,
        "external_baseline_only": not is_shardloom_engine(runner.name),
    }


def run_shardloom_native_microbenchmarks(iterations: int) -> list[dict[str, Any]]:
    root = workspace_root()
    fixture = root / "shardloom-vortex" / "tests" / "fixtures" / "metadata_footer_u64_20000.vortex"
    if not fixture.exists():
        return [
            {
                "name": "local encoded CountAll",
                "status": "missing_fixture",
                "reason": f"Vortex fixture was not found at {fixture}",
            }
        ]
    try:
        binary = build_shardloom_cli(
            root,
            "vortex-traditional-analytics-benchmark",
            SHARDLOOM_BUILD_PROFILE,
        )
    except BenchmarkUnsupported as exc:
        return [
            {
                "name": "local encoded CountAll",
                "status": "build_error",
                "reason": str(exc),
            }
        ]
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    rows = [
        run_shardloom_count_microbenchmark(root, env, binary, fixture, iterations),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive count",
            "count",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive projection",
            "project:value",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive validity count",
            "count-where:is_not_null:value",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive comparison count",
            "count-where:gte:value:10000",
        ),
        run_shardloom_vortex_run_microbenchmark(
            root,
            env,
            binary,
            fixture,
            iterations,
            "local primitive filter projection",
            "filter-project:gte:value:10000|value",
        ),
        run_shardloom_commit_microbenchmark(root, env, binary, iterations),
    ]
    return rows


def run_shardloom_count_microbenchmark(
    root: Path,
    env: dict[str, str],
    binary: Path,
    fixture: Path,
    iterations: int,
) -> dict[str, Any]:
    command = [
        str(binary),
        "vortex-count-benchmark",
        str(fixture),
        "1",
        "1",
        "--iterations",
        str(iterations),
        "--format",
        "json",
    ]
    started = time.perf_counter()
    completed = subprocess_run(command, root, env)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if completed["returncode"] != 0:
        return native_microbenchmark_error(
            "local encoded CountAll",
            "execution_error",
            completed["stderr"] or completed["stdout"] or "unknown failure",
            command,
            elapsed_ms,
        )
    try:
        payload = json.loads(completed["stdout"].splitlines()[0])
    except (json.JSONDecodeError, IndexError) as exc:
        return native_microbenchmark_error(
            "local encoded CountAll",
            "invalid_output",
            f"{type(exc).__name__}: {exc}",
            command,
            elapsed_ms,
        )
    fields = parse_output_fields(payload)
    return {
        "name": "local encoded CountAll",
        "status": payload.get("status", "unknown"),
        "dataset": str(fixture),
        "primitive": "count",
        "rows": fields.get("count"),
        "iterations": fields.get("iterations_completed"),
        "query_runtime_millis": fields.get("avg_query_runtime_millis"),
        "query_runtime_micros": fields.get("avg_query_runtime_micros"),
        "timing_scope": "in-command repeated local encoded count",
        "comparison_status": fields.get("comparison_status"),
        "claim_gate_status": fields.get("claim_gate_status"),
        "data_read": fields.get("data_read"),
        "data_decoded": fields.get("data_decoded"),
        "data_materialized": fields.get("data_materialized"),
        "row_read": fields.get("row_read"),
        "arrow_converted": fields.get("arrow_converted"),
        "materialization_boundary_reported": fields.get(
            "materialization_boundary_reported", "false"
        ),
        "fallback_attempted": fields.get("fallback_attempted"),
        "performance_claim_allowed": fields.get("performance_claim_allowed"),
        "command": command,
    }


def run_shardloom_vortex_run_microbenchmark(
    root: Path,
    env: dict[str, str],
    binary: Path,
    fixture: Path,
    iterations: int,
    name: str,
    primitive: str,
) -> dict[str, Any]:
    command = [
        str(binary),
        "vortex-run",
        str(fixture),
        primitive,
        "1",
        "1",
        "--format",
        "json",
    ]
    timings: list[float] = []
    payload: dict[str, Any] | None = None
    for _ in range(iterations):
        started = time.perf_counter()
        completed = subprocess_run(command, root, env)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        timings.append(elapsed_ms)
        if completed["returncode"] != 0:
            return native_microbenchmark_error(
                name,
                "execution_error",
                completed["stderr"] or completed["stdout"] or "unknown failure",
                command,
                elapsed_ms,
            )
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            return native_microbenchmark_error(
                name,
                "invalid_output",
                f"{type(exc).__name__}: {exc}",
                command,
                elapsed_ms,
            )
        if payload.get("status") != "success":
            return native_microbenchmark_error(
                name,
                str(payload.get("status", "unsupported")),
                payload.get("human_text") or "ShardLoom native primitive did not succeed",
                command,
                elapsed_ms,
            )
    fields = parse_output_fields(payload or {})
    return {
        "name": name,
        "status": (payload or {}).get("status", "unknown"),
        "dataset": str(fixture),
        "primitive": primitive,
        "rows": first_meaningful_field(
            fields.get("local_primitive_rows_selected"),
            fields.get("local_primitive_rows_scanned"),
        ),
        "iterations": str(iterations),
        "query_runtime_millis": round(statistics.mean(timings), 4),
        "timing_scope": "average CLI process wall time",
        "comparison_status": "not_applicable",
        "claim_gate_status": "not_claim_grade",
        "result_known": fields.get("result_known"),
        "projected_columns": fields.get("local_primitive_projected_columns"),
        "filter_pushdown_applied": fields.get("local_primitive_filter_pushdown_applied"),
        "projection_pushdown_applied": fields.get(
            "local_primitive_projection_pushdown_applied"
        ),
        "upstream_filter_expression_used": fields.get(
            "local_primitive_upstream_filter_expression_used"
        ),
        "upstream_projection_expression_used": fields.get(
            "local_primitive_upstream_projection_expression_used"
        ),
        "data_read": fields.get("data_read"),
        "data_decoded": fields.get("data_decoded"),
        "data_materialized": fields.get("data_materialized"),
        "row_read": fields.get("row_read"),
        "arrow_converted": fields.get("arrow_converted"),
        "materialization_boundary_reported": fields.get(
            "local_primitive_materialization_boundary_reported"
        ),
        "work_avoided_metrics": fields.get("work_avoided_metrics"),
        "work_avoided_known_metrics": fields.get("work_avoided_known_metrics"),
        "work_avoided_unknown_metrics": fields.get("work_avoided_unknown_metrics"),
        "work_avoided_decode_avoided": fields.get("work_avoided_decode_avoided"),
        "work_avoided_materialization_avoided": fields.get(
            "work_avoided_materialization_avoided"
        ),
        "work_avoided_rows_not_scanned": fields.get("work_avoided_rows_not_scanned"),
        "work_avoided_rows_not_scanned_known": fields.get(
            "work_avoided_rows_not_scanned_known"
        ),
        "work_avoided_segments_pruned": fields.get("work_avoided_segments_pruned"),
        "work_avoided_segments_pruned_known": fields.get(
            "work_avoided_segments_pruned_known"
        ),
        "work_avoided_bytes_not_read": fields.get("work_avoided_bytes_not_read"),
        "work_avoided_bytes_not_read_known": fields.get("work_avoided_bytes_not_read_known"),
        "work_avoided_spill_avoided": fields.get("work_avoided_spill_avoided"),
        "work_avoided_fallback_blocked": fields.get("work_avoided_fallback_blocked"),
        "decision_trace_entries": fields.get("decision_trace_entries"),
        "why_claim_gate_status": fields.get("why_claim_gate_status"),
        "why_primary_reason": fields.get("why_primary_reason"),
        "why_blocker_count": fields.get("why_blocker_count"),
        "why_blockers": fields.get("why_blockers"),
        "why_next_actions": fields.get("why_next_actions"),
        "fallback_attempted": str(
            (payload or {}).get("fallback", {}).get("attempted", False)
        ).lower(),
        "performance_claim_allowed": "false",
        "command": command,
    }


def native_microbenchmark_error(
    name: str,
    status: str,
    reason: str,
    command: list[str] | None = None,
    elapsed_millis: float | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": name,
        "status": status,
        "reason": reason,
    }
    if command is not None:
        result["command"] = command
    if elapsed_millis is not None:
        result["elapsed_millis"] = round(elapsed_millis, 4)
    return result


def run_shardloom_commit_microbenchmark(
    root: Path,
    env: dict[str, str],
    binary: Path,
    iterations: int,
) -> dict[str, Any]:
    command_template = [
        str(binary),
        "vortex-local-commit-execute",
        "<target-uri>",
        "<workspace>",
        "commit-protocol-ready,finalized-manifest-written,commit-marker-written,output-payload-written,local-workspace,feature-gate-enabled",
        "--format",
        "json",
    ]
    timings: list[float] = []
    bytes_written: list[int] = []
    commit_latencies: list[int] = []
    payload: dict[str, Any] | None = None
    generated_root = root / "benchmarks" / "traditional_analytics" / ".generated"
    generated_root.mkdir(parents=True, exist_ok=True)
    for iteration in range(iterations):
        workspace = generated_root / f"commit-{os.getpid()}-{time.time_ns()}-{iteration}"
        workspace.mkdir(parents=True, exist_ok=False)
        try:
            prepare_shardloom_commit_workspace(workspace, iteration)
            target_uri = (workspace / "target.vortex").resolve().as_uri()
            command = [
                str(binary),
                "vortex-local-commit-execute",
                target_uri,
                str(workspace),
                command_template[4],
                "--format",
                "json",
            ]
            started = time.perf_counter()
            completed = subprocess_run(command, root, env)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            timings.append(elapsed_ms)
            if completed["returncode"] != 0:
                return native_microbenchmark_error(
                    "local commit manifest",
                    "execution_error",
                    completed["stderr"] or completed["stdout"] or "unknown failure",
                    command,
                    elapsed_ms,
                )
            try:
                payload = json.loads(completed["stdout"].splitlines()[0])
            except (json.JSONDecodeError, IndexError) as exc:
                return native_microbenchmark_error(
                    "local commit manifest",
                    "invalid_output",
                    f"{type(exc).__name__}: {exc}",
                    command,
                    elapsed_ms,
                )
            if payload.get("status") != "success":
                return native_microbenchmark_error(
                    "local commit manifest",
                    str(payload.get("status", "unsupported")),
                    payload.get("human_text") or "ShardLoom local commit did not succeed",
                    command,
                    elapsed_ms,
                )
            fields = parse_output_fields(payload)
            bytes_written_value = parse_optional_int(fields.get("bytes_written"))
            latency_value = parse_optional_int(fields.get("write_commit_latency_micros"))
            if bytes_written_value is not None:
                bytes_written.append(bytes_written_value)
            if latency_value is not None:
                commit_latencies.append(latency_value)
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    fields = parse_output_fields(payload or {})
    avg_commit_latency_micros = (
        int(round(statistics.mean(commit_latencies))) if commit_latencies else None
    )
    return {
        "name": "local commit manifest",
        "status": (payload or {}).get("status", "unknown"),
        "dataset": "synthetic local staged workspace",
        "primitive": "local_commit",
        "rows": "n/a",
        "iterations": str(iterations),
        "query_runtime_millis": round(statistics.mean(timings), 4),
        "timing_scope": "average CLI process wall time",
        "comparison_status": "not_applicable",
        "claim_gate_status": "not_claim_grade",
        "commit_executed": fields.get("commit_executed"),
        "manifest_committed": fields.get("manifest_committed"),
        "bytes_written": str(sum(bytes_written)) if bytes_written else fields.get("bytes_written"),
        "write_commit_latency_micros": str(avg_commit_latency_micros)
        if avg_commit_latency_micros is not None
        else fields.get("write_commit_latency_micros"),
        "write_commit_latency_millis": str(round(avg_commit_latency_micros / 1000.0, 4))
        if avg_commit_latency_micros is not None
        else fields.get("write_commit_latency_millis"),
        "data_read": "false",
        "data_decoded": "false",
        "data_materialized": "false",
        "row_read": "false",
        "arrow_converted": "false",
        "materialization_boundary_reported": "false",
        "fallback_attempted": str((payload or {}).get("fallback", {}).get("attempted", False)).lower(),
        "performance_claim_allowed": "false",
        "command": command_template,
    }


def prepare_shardloom_commit_workspace(workspace: Path, iteration: int) -> None:
    (workspace / "_shardloom_finalized_manifest.json").write_text(
        json.dumps({"finalized": True, "iteration": iteration}, sort_keys=True),
        encoding="utf-8",
    )
    (workspace / ".shardloom-commit-marker").write_text("marker=true\n", encoding="utf-8")
    (workspace / "_shardloom_output_payload.vortex").write_bytes(b"payload")


def subprocess_run(command: list[str], cwd: Path, env: dict[str, str]) -> dict[str, Any]:
    import subprocess

    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def universal_io_lanes() -> list[dict[str, Any]]:
    return [
        {
            "name": "CSV/JSONL/Parquet/Arrow IPC/Avro/ORC -> NativeWorkStream -> Vortex",
            "status": "smoke_supported",
            "reason": "ShardLoom benchmark rows use deterministic local compatibility source adapters, emit native work/native result evidence fields, write local Vortex files, reopen them through Vortex, and scan Vortex arrays. The path still materializes Vortex-derived arrays for the temporary operators.",
            "expected_report": "per-path NativeIoCertificate with SourceCapabilityReport, SourcePushdownReport, SinkRequirementReport, AdapterFidelityReport, MaterializationBoundaryReport, and side-effect evidence",
        },
        {
            "name": "Compatibility source -> Vortex import -> encoded CountAll",
            "status": "partial_smoke_supported",
            "reason": "Compatibility-to-Vortex import and Vortex scan are exercised by ShardLoom traditional rows. The native microbenchmark lane separately exercises local Vortex scan filter/projection pushdown. Fully integrated compatibility-to-Vortex encoded operator execution over imported artifacts remains a CG-2/CG-13/CG-19 follow-up.",
            "expected_report": "NativeIoCertificate plus encoded-count execution certificate",
        },
    ]


def correctness_summary(
    results: list[dict[str, Any]], scenarios: tuple[str, ...]
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for scenario in scenarios:
        successful = [
            result
            for result in results
            if result["scenario_name"] == scenario and result["status"] == "success"
        ]
        if not successful:
            summary[scenario] = {
                "status": "missing",
                "reference_engine": None,
                "matching_engines": [],
                "mismatching_engines": [],
            }
            continue
        reference = successful[0]
        matching = [
            result["engine"]
            for result in successful
            if result["correctness_digest"] == reference["correctness_digest"]
        ]
        mismatching = [
            result["engine"]
            for result in successful
            if result["correctness_digest"] != reference["correctness_digest"]
        ]
        summary[scenario] = {
            "status": "passed" if not mismatching else "mismatch",
            "reference_engine": reference["engine"],
            "reference_digest": reference["correctness_digest"],
            "matching_engines": matching,
            "mismatching_engines": mismatching,
        }
    return summary


def environment_report() -> dict[str, Any]:
    total_memory = None
    try:
        import psutil  # type: ignore
    except ImportError:
        pass
    else:
        total_memory = psutil.virtual_memory().total
    return {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "total_memory_bytes": total_memory,
    }


def fairness_parameters(args: argparse.Namespace, paths: DatasetPaths) -> dict[str, Any]:
    return {
        "status": "local_smoke_not_claim_grade",
        "rows": paths.rows,
        "dim_rows": paths.dim_rows,
        "storage_format": "CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC where supported; ShardLoom compatibility source adapters import into local Vortex files; shardloom-vortex native .vortex rows",
        "benchmark_suite": BENCHMARK_SUITE,
        "scenario_catalog_schema": SCENARIO_CATALOG["schema_version"],
        "dataset_profile": args.dataset_profile,
        "generated_dataset_profiles": list(GENERATED_DATASET_PROFILES),
        "formats_requested": list(args.format_list),
        "formats_reported": list(report_format_order(args)),
        "compression": "engine defaults; Parquet uses pyarrow defaults; ShardLoom uses upstream Vortex writer defaults",
        "iterations": args.iterations,
        "stress_lane_included": any(
            scenario in STRESS_SCENARIO_ORDER for scenario in args.scenario_list
        ),
        "cache_mode": args.cache_mode,
        "timing_scope": args.timing_scope,
        "engines_requested": list(args.engine_list),
        "scenarios_requested": list(args.scenario_list),
        "taxonomy_extra_included": args.include_taxonomy_extra,
        "shardloom_build_profile": args.shardloom_build_profile,
        "shardloom_build_time_excluded": True,
        "shardloom_feature_gate": "vortex-traditional-analytics-benchmark",
        "shardloom_result_sink_enabled": args.shardloom_result_sink,
        "dask_blocksize": args.dask_blocksize,
        "dask_scheduler": args.dask_scheduler,
        "spark_requires_java": True,
        "spark_profiles": "spark-default local[*] with Spark defaults; spark-local-tuned local[*] with shuffle/default parallelism capped to local CPU count and AQE enabled",
        "java_on_path": shutil.which("java") is not None,
        "java_home_set": bool(os.environ.get("JAVA_HOME")),
        "object_store_included": False,
        "compatibility_to_vortex_included": True,
        "csv_to_vortex_included": "csv" in args.format_list,
        "parquet_included": "parquet" in args.format_list,
        "jsonl_included": "jsonl" in args.format_list,
        "arrow_ipc_included": "arrow-ipc" in args.format_list,
        "avro_included": "avro" in args.format_list,
        "orc_included": "orc" in args.format_list,
        "shardloom_resource_sizing": "auto by default; optional --memory-gb and --max-parallelism caps are reflected in ShardLoom evidence fields",
        "native_vortex_included": "shardloom-vortex" in args.engine_list,
        "shardloom_universal_io_smoke_included": True,
        "shardloom_native_microbenchmarks_included": not args.skip_shardloom_native,
        "claim_grade_requirements": [
            "pin engine versions",
            "declare hardware profile",
            "separate cold-cache and warm-cache runs",
            "use larger-than-memory and object-store datasets where relevant",
            "record ShardLoom native and universal-I/O rows separately from external compatibility-file baselines",
            "run multiple repetitions under the same process isolation policy",
        ],
    }


def default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(__file__).resolve().parent / "results" / f"traditional_analytics_{timestamp}.json"


def report_format_order(args: argparse.Namespace) -> tuple[str, ...]:
    formats = list(args.format_list) if any(
        engine != "shardloom-vortex" for engine in args.engine_list
    ) else []
    if "shardloom-vortex" in args.engine_list and SHARDLOOM_VORTEX_FORMAT not in formats:
        formats.append(SHARDLOOM_VORTEX_FORMAT)
    return tuple(formats)


def formats_for_engine_report(
    engine: str, runner: EngineRunner | None, report_formats: tuple[str, ...]
) -> tuple[str, ...]:
    if engine == "shardloom-vortex":
        return (SHARDLOOM_VORTEX_FORMAT,)
    if runner is not None and runner.formats == (SHARDLOOM_VORTEX_FORMAT,):
        return (SHARDLOOM_VORTEX_FORMAT,)
    return tuple(data_format for data_format in report_formats if data_format != SHARDLOOM_VORTEX_FORMAT)


def expanded_scenario_order(
    formats: tuple[str, ...], scenarios: tuple[str, ...]
) -> list[str]:
    return [
        scenario_display_name(data_format, scenario)
        for data_format in formats
        for scenario in scenarios
    ]


def markdown_output_path(json_path: Path, requested: Path | None) -> Path:
    if requested is not None:
        return requested
    return json_path.with_suffix(".md")


def format_metric(value: Any, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}{suffix}"
    return f"{value}{suffix}"


def format_bytes(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit = units[0]
    for unit in units:
        if abs(number) < 1024.0 or unit == units[-1]:
            break
        number /= 1024.0
    return f"{number:.2f} {unit}"


def format_bool(value: Any) -> str:
    if value is None:
        return "n/a"
    return str(value).lower()


def result_lookup(results: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(result["scenario_name"], result["engine"]): result for result in results}


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    output = ["| " + " | ".join(headers) + " |"]
    output.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return "\n".join(output)


def render_engine_overview(artifact: dict[str, Any]) -> str:
    results = artifact["results"]
    rows = []
    for engine in artifact["engine_order"]:
        engine_results = [result for result in results if result["engine"] == engine]
        version_info = artifact["engine_versions"].get(engine, {})
        rows.append(
            [
                engine,
                "yes" if version_info.get("available") else "no",
                str(version_info.get("version") or version_info.get("reason") or "n/a"),
                format_metric(version_info.get("startup_time_millis"), " ms"),
                str(sum(1 for result in engine_results if result["status"] == "success")),
                str(sum(1 for result in engine_results if result["status"] != "success")),
            ]
        )
    return markdown_table(
        [
            "Engine",
            "Available",
            "Version / reason",
            "Startup / warmup",
            "Successful scenarios",
            "Failed scenarios",
        ],
        rows,
    )


def render_fairness_parameters(artifact: dict[str, Any]) -> str:
    params = artifact["fairness_parameters"]
    rows = [
        ["Status", str(params["status"])],
        ["Rows", f"{params['rows']} fact / {params['dim_rows']} dimension"],
        ["Storage", f"{params['storage_format']} ({params['compression']})"],
        ["Benchmark suite", str(params["benchmark_suite"])],
        ["Scenario catalog", str(params["scenario_catalog_schema"])],
        ["Dataset profile", str(params["dataset_profile"])],
        ["Generated profiles", ", ".join(params["generated_dataset_profiles"])],
        ["Formats requested", ", ".join(params["formats_requested"])],
        ["Formats reported", ", ".join(params["formats_reported"])],
        ["Iterations", str(params["iterations"])],
        ["Stress lane included", str(params["stress_lane_included"])],
        ["Taxonomy extras included", str(params["taxonomy_extra_included"])],
        [
            "ShardLoom build",
            f"profile={params['shardloom_build_profile']}, feature={params['shardloom_feature_gate']}, build_time_excluded={params['shardloom_build_time_excluded']}",
        ],
        ["Cache mode", str(params["cache_mode"])],
        ["Timing scope", str(params["timing_scope"])],
        ["Dask mode", f"blocksize={params['dask_blocksize']}, scheduler={params['dask_scheduler']}"],
        [
            "Spark prerequisite",
            f"requires Java; java_on_path={params['java_on_path']}, JAVA_HOME={params['java_home_set']}",
        ],
        ["Spark profiles", str(params["spark_profiles"])],
        ["Object store included", str(params["object_store_included"])],
        ["Compatibility to Vortex included", str(params["compatibility_to_vortex_included"])],
        ["CSV to Vortex included", str(params["csv_to_vortex_included"])],
        ["Parquet included", str(params["parquet_included"])],
        ["JSONL included", str(params["jsonl_included"])],
        ["Arrow IPC included", str(params["arrow_ipc_included"])],
        ["Avro included", str(params["avro_included"])],
        ["ORC included", str(params["orc_included"])],
        ["ShardLoom resource sizing", str(params["shardloom_resource_sizing"])],
        ["Native Vortex included", str(params["native_vortex_included"])],
        [
            "ShardLoom universal I/O smoke",
            str(params["shardloom_universal_io_smoke_included"]),
        ],
        [
            "ShardLoom native microbenchmarks",
            str(params["shardloom_native_microbenchmarks_included"]),
        ],
    ]
    return markdown_table(["Parameter", "Value"], rows)


def render_read_this_first(artifact: dict[str, Any]) -> str:
    notes = [
        "This is a local smoke/bring-up report, not a claim-grade benchmark.",
        "External baseline rows measure each engine's local compatibility-file paths where supported. Unsupported format rows are captured explicitly instead of blocking the report.",
        "ShardLoom rows use compatibility source adapters into local Vortex files, reopen those files through Vortex, scan Vortex arrays, and then run the temporary benchmark operators over Vortex-derived arrays.",
        "ShardLoom native Vortex rows start timing from existing `.vortex` inputs prepared before scenario timing; they still use temporary benchmark operators and are not mature SQL/DataFrame/API evidence.",
        "ShardLoom's current traditional rows report a concrete per-path NativeIoCertificate and a compatibility-format materialization boundary; they prove universal I/O viability, not mature encoded-native SQL/operator coverage.",
        "Coverage rows now carry claim_gate_status so fixture-smoke, unsupported, external-baseline-only, not-claim-grade, and claim-grade rows stay distinct from timing rows.",
        "ShardLoom derives resource sizing automatically by default. Evidence fields show policy mode, detected/applied parallelism, batch rows, target partition bytes, and target partition count.",
        "Dask results depend heavily on partitioning, scheduler, file count, and dataset size; small single-file CSV tests can make scheduler overhead dominate.",
        "Spark rows are split into spark-default and spark-local-tuned so default behavior is not mixed with local tuning; each Spark profile starts and warms its own session immediately before its scenario rows.",
        "Spark rows require Java/JDK. Missing Spark rows mean local setup is incomplete, not that Spark failed the workload.",
        "Stress rows are opt-in; they become meaningful Spark-style scale tests only with larger-than-memory data, stable cache policy, and explicit hardware/runtime settings.",
        "ShardLoom benchmark build time is excluded from per-scenario timing; compatibility-to-Vortex import, Vortex file write/read, scan, and the temporary benchmark operator are included.",
    ]
    return "\n".join(f"- {note}" for note in notes)


def render_scenario_matrix(artifact: dict[str, Any]) -> str:
    lookup = result_lookup(artifact["results"])
    headers = ["Scenario", *artifact["engine_order"]]
    rows = []
    for scenario in artifact["scenario_order"]:
        row = [scenario]
        for engine in artifact["engine_order"]:
            result = lookup.get((scenario, engine))
            if result is None:
                row.append("missing")
                continue
            if result["status"] == "success":
                millis = result["metrics"]["query_runtime_millis"]
                row.append(f"{format_metric(millis, ' ms')}")
            else:
                row.append(result["status"])
        rows.append(row)
    return markdown_table(headers, rows)


def render_coverage_table(artifact: dict[str, Any]) -> str:
    rows = []
    for row in artifact["coverage_table"]:
        rows.append(
            [
                row["scenario_name"],
                row["engine"],
                row["scenario_category"],
                row["engine_role"],
                row["status"],
                str(row["timing_row_present"]),
                row["claim_gate_status"],
                str(row["claim_grade_requirements_met"]),
                str(row["timing_row_claim_grade"]),
                "; ".join(row["claim_grade_missing_evidence"][:2])
                if row["claim_grade_missing_evidence"]
                else "none",
                str(row["native_io_status_required"]),
                str(row["certificate_status"] or "n/a"),
                str(row["fallback_attempted"]),
                str(row["external_engine_invoked"]),
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Category",
            "Role",
            "Coverage",
            "Timing row",
            "Claim gate",
            "Claim-grade",
            "Timing claim-grade",
            "Missing claim evidence",
            "Native I/O req",
            "Certificate",
            "Fallback",
            "External engine invoked",
        ],
        rows,
    )


def render_resource_metrics_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact["results"]:
        metrics = result["metrics"]
        rows.append(
            [
                result["scenario_name"],
                result["engine"],
                result["status"],
                format_metric(metrics.get("query_runtime_millis"), " ms"),
                format_bytes(metrics.get("peak_memory_bytes")),
                format_bytes(metrics.get("bytes_read")),
                format_bytes(metrics.get("bytes_written")),
                format_metric(metrics.get("rows_scanned")),
                format_metric(metrics.get("rows_materialized")),
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Engine",
            "Status",
            "Runtime",
            "Peak RSS",
            "Bytes read",
            "Bytes written",
            "Rows scanned",
            "Rows materialized",
        ],
        rows,
    )


def render_shardloom_effects_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact["results"]:
        if not str(result["engine"]).startswith("shardloom"):
            continue
        metrics = result["metrics"]
        evidence = result.get("shardloom_evidence", {})
        rows.append(
            [
                result["scenario_name"],
                result["status"],
                format_bool(metrics.get("data_decoded")),
                format_bool(metrics.get("data_materialized")),
                format_bool(metrics.get("row_read")),
                format_bool(metrics.get("arrow_converted")),
                format_bool(metrics.get("object_store_io")),
                format_bool(metrics.get("write_io")),
                format_bool(metrics.get("spill_io_performed")),
                str(evidence.get("native_io_certificate_path_id", "n/a")),
                str(evidence.get("native_io_certificate_emitted", "n/a")),
                str(evidence.get("native_io_certificate_status", "n/a")),
                str(evidence.get("resource_policy_mode", "n/a")),
                str(evidence.get("detected_parallelism", "n/a")),
                str(evidence.get("applied_max_parallelism", "n/a")),
                str(evidence.get("applied_batch_rows", "n/a")),
                format_bytes(parse_optional_int(evidence.get("target_partition_bytes"))),
                str(evidence.get("target_partition_count", "n/a")),
                str(evidence.get("materialization_boundary_rows", "n/a")),
                format_bytes(parse_optional_int(evidence.get("source_bytes_read"))),
            ]
        )
    if not rows:
        rows.append(
            [
                "not run",
                "missing",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
            ]
        )
    return markdown_table(
        [
            "Scenario",
            "Status",
            "Decoded",
            "Materialized",
            "Row read",
            "Arrow",
            "Object store",
            "Write IO",
            "Spill IO",
            "Native I/O path",
            "Native I/O cert",
            "Cert status",
            "Sizing",
            "Detected par",
            "Applied par",
            "Batch rows",
            "Target part bytes",
            "Target parts",
            "Boundary rows",
            "Source bytes",
        ],
        rows,
    )


def render_correctness_table(artifact: dict[str, Any]) -> str:
    rows = []
    for scenario, summary in artifact["correctness"].items():
        rows.append(
            [
                scenario,
                summary["status"],
                str(summary.get("reference_engine") or "n/a"),
                ", ".join(summary.get("matching_engines", [])) or "n/a",
                ", ".join(summary.get("mismatching_engines", [])) or "n/a",
            ]
        )
    return markdown_table(
        ["Scenario", "Status", "Reference", "Matching engines", "Mismatching engines"],
        rows,
    )


def render_shardloom_native_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        rows.append(
            [
                result.get("name", "n/a"),
                str(result.get("status", "n/a")),
                str(result.get("primitive", "n/a")),
                str(result.get("rows", "n/a")),
                format_metric(result.get("query_runtime_millis"), " ms"),
                str(result.get("timing_scope", "n/a")),
                str(result.get("data_decoded", "n/a")),
                str(result.get("data_materialized", "n/a")),
                str(result.get("filter_pushdown_applied", "n/a")),
                str(result.get("projection_pushdown_applied", "n/a")),
                str(result.get("materialization_boundary_reported", "n/a")),
                str(result.get("fallback_attempted", "n/a")),
                str(result.get("claim_gate_status", "n/a")),
            ]
        )
    if not rows:
        rows.append(
            [
                "not run",
                "skipped",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
            ]
        )
    return markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Primitive",
            "Rows",
            "Avg runtime",
            "Timing scope",
            "Decoded",
            "Materialized",
            "Filter pushdown",
            "Projection pushdown",
            "Boundary",
            "Fallback",
            "Claim gate",
        ],
        rows,
    )


def render_shardloom_work_avoidance_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        rows.append(
            [
                result.get("name", "n/a"),
                str(result.get("status", "n/a")),
                str(result.get("primitive", "n/a")),
                str(result.get("work_avoided_metrics", "n/a")),
                str(result.get("work_avoided_decode_avoided", "n/a")),
                str(result.get("work_avoided_materialization_avoided", "n/a")),
                str(result.get("work_avoided_rows_not_scanned", "n/a")),
                str(result.get("work_avoided_segments_pruned", "n/a")),
                str(result.get("work_avoided_bytes_not_read", "n/a")),
                str(result.get("work_avoided_spill_avoided", "n/a")),
                str(result.get("work_avoided_fallback_blocked", "n/a")),
            ]
        )
    if not rows:
        rows.append(
            [
                "not run",
                "skipped",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                "n/a",
            ]
        )
    return markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Primitive",
            "Metrics",
            "Decode",
            "Materialize",
            "Rows skipped",
            "Segments pruned",
            "Bytes avoided",
            "Spill",
            "Fallback",
        ],
        rows,
    )


def render_shardloom_why_table(artifact: dict[str, Any]) -> str:
    rows = []
    details = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        if result.get("why_claim_gate_status") is None:
            continue
        name = str(result.get("name", "n/a"))
        rows.append(
            [
                name,
                str(result.get("status", "n/a")),
                str(result.get("why_claim_gate_status", "n/a")),
                str(result.get("decision_trace_entries", "n/a")),
                str(result.get("why_blocker_count", "n/a")),
                str(result.get("why_primary_reason", "n/a")).replace("|", "\\|"),
                final_summary_item(result.get("why_next_actions")),
            ]
        )
        details.append(
            f"- **{name}** blockers: {summary_list_text(result.get('why_blockers'))}"
        )
        details.append(
            f"  next: {summary_list_text(result.get('why_next_actions'))}"
        )
    if not rows:
        rows.append(["not run", "skipped", "n/a", "n/a", "n/a", "n/a", "n/a"])
    table = markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Claim gate",
            "Trace entries",
            "Blockers",
            "Primary reason",
            "Next focus",
        ],
        rows,
    )
    if details:
        return table + "\n\n" + "\n".join(details)
    return table


def final_summary_item(value: Any) -> str:
    text = str(value or "n/a")
    return text.rsplit(" | ", maxsplit=1)[-1].replace("|", "\\|")


def summary_list_text(value: Any) -> str:
    text = str(value or "n/a")
    return text.replace(" | ", "; ").replace("|", "\\|")


def render_shardloom_commit_table(artifact: dict[str, Any]) -> str:
    rows = []
    for result in artifact.get("shardloom_native_microbenchmarks", []):
        if result.get("primitive") != "local_commit":
            continue
        rows.append(
            [
                result.get("name", "n/a"),
                str(result.get("status", "n/a")),
                str(result.get("iterations", "n/a")),
                str(result.get("commit_executed", "n/a")),
                str(result.get("manifest_committed", "n/a")),
                format_bytes(parse_optional_int(result.get("bytes_written"))),
                str(result.get("write_commit_latency_micros", "n/a")),
                str(result.get("fallback_attempted", "n/a")),
            ]
        )
    if not rows:
        rows.append(["not run", "skipped", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
    return markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Iterations",
            "Commit",
            "Manifest committed",
            "Bytes written",
            "Avg commit us",
            "Fallback",
        ],
        rows,
    )


def render_universal_io_table(artifact: dict[str, Any]) -> str:
    rows = []
    for lane in artifact.get("universal_io_lanes", []):
        rows.append(
            [
                lane.get("name", "n/a"),
                lane.get("status", "n/a"),
                str(lane.get("expected_report", "n/a")),
                str(lane.get("reason", "n/a")).replace("|", "\\|"),
            ]
        )
    return markdown_table(["Lane", "Status", "Expected evidence", "Reason"], rows)


def render_fastest_table(artifact: dict[str, Any]) -> str:
    lookup = result_lookup(artifact["results"])
    rows = []
    for scenario in artifact["scenario_order"]:
        successful = [
            lookup[(scenario, engine)]
            for engine in artifact["engine_order"]
            if (scenario, engine) in lookup and lookup[(scenario, engine)]["status"] == "success"
        ]
        if not successful:
            rows.append([scenario, "n/a", "n/a", "n/a", "n/a"])
            continue
        ordered = sorted(successful, key=lambda result: result["metrics"]["query_runtime_millis"])
        fastest = ordered[0]
        slowest = ordered[-1]
        fastest_ms = fastest["metrics"]["query_runtime_millis"]
        slowest_ms = slowest["metrics"]["query_runtime_millis"]
        rows.append(
            [
                scenario,
                fastest["engine"],
                format_metric(fastest_ms, " ms"),
                slowest["engine"],
                f"{slowest_ms / fastest_ms:.2f}x" if fastest_ms else "n/a",
            ]
        )
    return markdown_table(
        ["Scenario", "Fastest engine", "Fastest time", "Slowest engine", "Slowest / fastest"],
        rows,
    )


def render_timing_bars(artifact: dict[str, Any]) -> str:
    sections = []
    lookup = result_lookup(artifact["results"])
    for scenario in artifact["scenario_order"]:
        successful = [
            lookup[(scenario, engine)]
            for engine in artifact["engine_order"]
            if (scenario, engine) in lookup and lookup[(scenario, engine)]["status"] == "success"
        ]
        if not successful:
            sections.append(f"### {scenario}\n\nNo successful timing rows.")
            continue
        max_ms = max(result["metrics"]["query_runtime_millis"] for result in successful) or 1.0
        lines = [f"### {scenario}", "", "```text"]
        for engine in artifact["engine_order"]:
            result = lookup.get((scenario, engine))
            if result is None:
                lines.append(f"{engine:<12} missing")
                continue
            if result["status"] != "success":
                lines.append(f"{engine:<12} {result['status']}")
                continue
            millis = result["metrics"]["query_runtime_millis"]
            width = max(1, int((millis / max_ms) * 40))
            lines.append(f"{engine:<12} {millis:>10.2f} ms | {'#' * width}")
        lines.append("```")
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


def render_errors_table(artifact: dict[str, Any]) -> str:
    rows = []
    for error in artifact["errors"]:
        rows.append(
            [
                error.get("engine", "n/a"),
                error.get("scenario", "n/a"),
                error.get("status", "n/a"),
                str(error.get("reason", "n/a")).replace("|", "\\|"),
            ]
        )
    if not rows:
        rows.append(["none", "none", "none", "none"])
    return markdown_table(["Engine", "Scenario", "Status", "Reason"], rows)


def render_markdown_report(artifact: dict[str, Any]) -> str:
    dataset = artifact["dataset"]
    env = artifact["environment"]
    lines = [
        "# Traditional Analytics Benchmark Results",
        "",
        "These are raw local benchmark measurements. They are not ShardLoom performance, superiority, or best-choice claims.",
        "",
        f"- Generated: `{artifact['generated_at_utc']}`",
        f"- Scope: `{artifact['benchmark_scope']}`",
        f"- Dataset profile: `{dataset['dataset_profile']}`",
        f"- Rows: `{dataset['rows']}` fact rows, `{dataset['dim_rows']}` dimension rows",
        f"- CSV files: `{dataset['fact_csv_bytes']}` fact bytes, `{dataset['dim_csv_bytes']}` dimension bytes",
        f"- Parquet files: `{dataset['fact_parquet_bytes']}` fact bytes, `{dataset['dim_parquet_bytes']}` dimension bytes",
        f"- JSONL files: `{dataset['fact_jsonl_bytes']}` fact bytes, `{dataset['dim_jsonl_bytes']}` dimension bytes",
        f"- Arrow IPC files: `{dataset['fact_arrow_ipc_bytes']}` fact bytes, `{dataset['dim_arrow_ipc_bytes']}` dimension bytes",
        f"- Avro files: `{dataset['fact_avro_bytes']}` fact bytes, `{dataset['dim_avro_bytes']}` dimension bytes",
        f"- ORC files: `{dataset['fact_orc_bytes']}` fact bytes, `{dataset['dim_orc_bytes']}` dimension bytes",
        f"- Python: `{env['python_version']}`",
        f"- Platform: `{env['platform']}`",
        f"- CPU count: `{env['cpu_count']}`",
        f"- Fallback execution allowed: `{artifact['fallback_execution_allowed']}`",
        f"- Performance claim allowed: `{artifact['performance_claim_allowed']}`",
        "",
        "## Read This First",
        "",
        render_read_this_first(artifact),
        "",
        "## Fairness Parameters",
        "",
        render_fairness_parameters(artifact),
        "",
        "## Engine Overview",
        "",
        render_engine_overview(artifact),
        "",
        "## Scenario Timing Matrix",
        "",
        "Values are mean per-iteration query/runtime milliseconds for successful rows. Failed rows show their status.",
        "",
        render_scenario_matrix(artifact),
        "",
        "## Support And Coverage Matrix",
        "",
        "Coverage is separate from timing. External engines are comparison baselines only, and ShardLoom rows must keep certificate, Native I/O, materialization, and no-fallback evidence visible.",
        "",
        render_coverage_table(artifact),
        "",
        "## Resource Metrics",
        "",
        "Memory is sampled process RSS when `psutil` is available. Bytes read and written are declared local file bytes for the scenario; ShardLoom bytes written include temporary Vortex artifacts from the universal-I/O smoke path.",
        "",
        render_resource_metrics_table(artifact),
        "",
        "## ShardLoom Runtime Effects",
        "",
        "These fields come from ShardLoom's CLI evidence and make decode, materialization, row-read, Arrow, object-store, write, spill, and native-I/O-certificate status explicit.",
        "",
        render_shardloom_effects_table(artifact),
        "",
        "## Fastest Successful Rows",
        "",
        render_fastest_table(artifact),
        "",
        "## ShardLoom Native Microbenchmarks",
        "",
        "These rows are not directly comparable to compatibility-file engine rows. They show the current native encoded/Vortex path that ShardLoom can execute today.",
        "",
        render_shardloom_native_table(artifact),
        "",
        "## ShardLoom Decision / Why Evidence",
        "",
        "These fields explain why each native runtime row is or is not claim-grade. They are derived from `vortex-run` DecisionTrace/WhyReport evidence.",
        "",
        render_shardloom_why_table(artifact),
        "",
        "## ShardLoom Work-Avoidance Evidence",
        "",
        "These fields come from `vortex-run` runtime effects. Unknown segment-prune and bytes-not-read values stay explicit until the runtime can measure them safely.",
        "",
        render_shardloom_work_avoidance_table(artifact),
        "",
        "## ShardLoom Write/Commit Evidence",
        "",
        "This local-only smoke row measures the current committed-manifest step. It is not an object-store or table-format commit benchmark.",
        "",
        render_shardloom_commit_table(artifact),
        "",
        "## Universal I/O And Compatibility-To-Vortex Lanes",
        "",
        "These lanes make the ShardLoom universal-I/O boundary explicit instead of hiding compatibility-format import behind external baseline rows.",
        "",
        render_universal_io_table(artifact),
        "",
        "## Timing Bars",
        "",
        render_timing_bars(artifact),
        "",
        "## Correctness",
        "",
        render_correctness_table(artifact),
        "",
        "## Errors And Unsupported Rows",
        "",
        render_errors_table(artifact),
        "",
        "## Limitations",
        "",
    ]
    for limitation in artifact["limitations"]:
        lines.append(f"- {limitation}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    global DASK_BLOCKSIZE, DASK_SCHEDULER, SHARDLOOM_BUILD_PROFILE, SHARDLOOM_RESULT_SINK
    args = parse_args()
    DASK_BLOCKSIZE = args.dask_blocksize
    DASK_SCHEDULER = args.dask_scheduler
    SHARDLOOM_BUILD_PROFILE = args.shardloom_build_profile
    SHARDLOOM_RESULT_SINK = args.shardloom_result_sink
    configure_java_home()
    paths = ensure_dataset(
        args.data_dir,
        args.rows,
        args.dim_rows,
        args.regenerate,
        args.format_list,
        args.dataset_profile,
    )
    report_formats = report_format_order(args)
    scenario_order = expanded_scenario_order(report_formats, args.scenario_list)
    runners, missing = available_runners(args.engine_list)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    def record_result(result: dict[str, Any]) -> None:
        annotate_result(result, args.cache_mode, args.dataset_profile)
        results.append(result)
        if result["status"] != "success":
            errors.append(
                {
                    "engine": result["engine"],
                    "scenario": result["scenario_name"],
                    "status": result["status"],
                    "reason": result.get("reason", "scenario did not complete"),
                }
            )

    for engine in args.engine_list:
        runner = runners.get(engine)
        engine_formats = formats_for_engine_report(engine, runner, report_formats)
        if runner is None:
            reason = missing.get(engine, "engine was not initialized")
            for data_format in engine_formats:
                for scenario in args.scenario_list:
                    result = failed_result(
                        engine,
                        scenario,
                        data_format,
                        "missing_dependency",
                        reason,
                        paths,
                        args.iterations,
                    )
                    record_result(result)
            continue
        try:
            try:
                runner = warmup_runner(runner)
                runner = prepare_runner(runner, paths)
                runners[engine] = runner
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                for data_format in engine_formats:
                    for scenario in args.scenario_list:
                        result = failed_result(
                            engine,
                            scenario,
                            data_format,
                            "engine_startup_error",
                            reason,
                            paths,
                            args.iterations,
                        )
                        record_result(result)
                continue
            for data_format in engine_formats:
                for scenario in args.scenario_list:
                    if data_format not in runner.formats:
                        result = failed_result(
                            engine,
                            scenario,
                            data_format,
                            "unsupported_format",
                            f"{engine} does not support {data_format} in this harness",
                            paths,
                            args.iterations,
                        )
                    else:
                        result = run_one(runner, paths, scenario, data_format, args.iterations)
                    record_result(result)
        finally:
            if runner.close is not None:
                runner.close()

    engine_versions = {
        engine: {
            "available": engine in runners,
            "version": runners[engine].version,
            "startup_time_millis": runners[engine].startup_time_millis,
        }
        for engine in runners
    }
    for engine, reason in missing.items():
        engine_versions[engine] = {
            "available": False,
            "version": None,
            "reason": reason,
            "startup_time_millis": None,
        }

    artifact = {
        "schema_version": "shardloom.traditional_analytics_benchmark.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_scope": "traditional_analytics_comparative_harness",
        "fallback_execution_allowed": False,
        "external_engines_are_fallback": False,
        "performance_claim_allowed": False,
        "dataset": {
            "rows": paths.rows,
            "dim_rows": paths.dim_rows,
            "dataset_profile": args.dataset_profile,
            "dataset_file_shape": dataset_file_shape(args.dataset_profile),
            "fact_extra_columns": list(paths.fact_extra_columns),
            "fact_csv": str(paths.fact_csv),
            "dim_csv": str(paths.dim_csv),
            "fact_jsonl": str(paths.fact_jsonl),
            "dim_jsonl": str(paths.dim_jsonl),
            "fact_parquet": str(paths.fact_parquet),
            "dim_parquet": str(paths.dim_parquet),
            "fact_arrow_ipc": str(paths.fact_arrow_ipc),
            "dim_arrow_ipc": str(paths.dim_arrow_ipc),
            "fact_avro": str(paths.fact_avro),
            "dim_avro": str(paths.dim_avro),
            "fact_orc": str(paths.fact_orc),
            "dim_orc": str(paths.dim_orc),
            "fact_csv_parts_dir": str(paths.fact_csv_parts_dir)
            if paths.fact_csv_parts_dir is not None
            else None,
            "fact_jsonl_parts_dir": str(paths.fact_jsonl_parts_dir)
            if paths.fact_jsonl_parts_dir is not None
            else None,
            "fact_csv_part_count": len(fact_part_paths(paths, "csv")),
            "fact_jsonl_part_count": len(fact_part_paths(paths, "jsonl")),
            "cdc_delta_csv": str(paths.cdc_delta_csv)
            if paths.cdc_delta_csv is not None and paths.cdc_delta_csv.exists()
            else None,
            "nested_jsonl": str(paths.nested_jsonl)
            if paths.nested_jsonl is not None and paths.nested_jsonl.exists()
            else None,
            "fact_csv_bytes": paths.fact_csv.stat().st_size,
            "dim_csv_bytes": paths.dim_csv.stat().st_size,
            "fact_jsonl_bytes": maybe_path_size(paths.fact_jsonl),
            "dim_jsonl_bytes": maybe_path_size(paths.dim_jsonl),
            "fact_parquet_bytes": maybe_path_size(paths.fact_parquet),
            "dim_parquet_bytes": maybe_path_size(paths.dim_parquet),
            "fact_arrow_ipc_bytes": maybe_path_size(paths.fact_arrow_ipc),
            "dim_arrow_ipc_bytes": maybe_path_size(paths.dim_arrow_ipc),
            "fact_avro_bytes": maybe_path_size(paths.fact_avro),
            "dim_avro_bytes": maybe_path_size(paths.dim_avro),
            "fact_orc_bytes": maybe_path_size(paths.fact_orc),
            "dim_orc_bytes": maybe_path_size(paths.dim_orc),
            "deterministic_generator": "benchmarks/traditional_analytics/run.py",
        },
        "environment": environment_report(),
        "fairness_parameters": fairness_parameters(args, paths),
        "engine_order": list(args.engine_list),
        "engine_versions": engine_versions,
        "format_order": list(report_formats),
        "scenario_order": scenario_order,
        "scenario_catalog_path": str(scenario_catalog_path()),
        "scenario_catalog": catalog_coverage_summary(SCENARIO_CATALOG),
        "coverage_table": coverage_table(results),
        "results": results,
        "shardloom_native_microbenchmarks": []
        if args.skip_shardloom_native
        else run_shardloom_native_microbenchmarks(args.shardloom_native_iterations),
        "universal_io_lanes": universal_io_lanes(),
        "correctness": correctness_summary(results, tuple(scenario_order)),
        "errors": errors,
        "limitations": [
            "Compatibility-file workloads include local file read cost and do not represent object-store behavior.",
            "Parquet, Arrow IPC, Avro, and ORC workloads use generated local files with engine-default read settings; they do not represent tuned lakehouse/table-format layouts.",
            "ShardLoom traditional rows include local compatibility-to-Vortex import and Vortex scan, but current temporary operators materialize Vortex-derived arrays instead of executing the full mature encoded SQL/operator surface.",
            "ShardLoom native Vortex rows exclude compatibility-to-Vortex setup from scenario timing but still use the current temporary benchmark operators after Vortex scan.",
            "ShardLoom native microbenchmark rows separately expose local Vortex scan filter/projection pushdown evidence; those rows are not a mature SQL/DataFrame/API benchmark surface.",
            "Dask performance is sensitive to partitioning and scheduler settings; this report records the selected blocksize and scheduler.",
            "Engine startup/warmup time is recorded separately from per-scenario timing. Spark profiles warm an isolated Spark session before their scenario rows and are closed before the next engine runs.",
            "Peak memory is sampled process RSS when psutil is available and may miss short-lived spikes.",
            "ShardLoom traditional rows use the native Rust benchmark command, not the future SQL parser/dataframe API.",
            "This artifact is benchmark evidence only and does not permit performance or superiority claims by itself.",
        ],
    }

    output_path = args.output or default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(artifact, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(output_path)
    if not args.no_markdown:
        report_path = markdown_output_path(output_path, args.markdown_output)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(render_markdown_report(artifact), encoding="utf-8")
        print(report_path)

    if args.require_all_engines and any(
        error["status"] == "missing_dependency" for error in errors
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
