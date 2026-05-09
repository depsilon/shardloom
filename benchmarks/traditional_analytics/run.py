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
    "pandas",
    "polars",
    "duckdb",
    "spark-default",
    "spark-local-tuned",
    "datafusion",
    "dask",
)
ENGINE_ALIASES = {"spark": ("spark-default", "spark-local-tuned")}
SCENARIO_ORDER = (
    "csv/file ingest",
    "selective filter",
    "group by aggregation",
    "sort and top-k",
    "hash join",
    "wide projection",
    "distinct count",
)
STRESS_SCENARIO_ORDER = (
    "scale stress skewed join aggregation",
    "scale stress multi-stage etl",
)
SCENARIO_BYTES = {
    "csv/file ingest": ("fact",),
    "selective filter": ("fact",),
    "group by aggregation": ("fact",),
    "sort and top-k": ("fact",),
    "hash join": ("fact", "dim"),
    "wide projection": ("fact",),
    "distinct count": ("fact",),
    "scale stress skewed join aggregation": ("fact", "dim"),
    "scale stress multi-stage etl": ("fact", "dim"),
}
DASK_BLOCKSIZE = "16MB"
DASK_SCHEDULER = "threads"
SHARDLOOM_BUILD_PROFILE = "release"


@dataclass(frozen=True)
class DatasetPaths:
    root: Path
    fact_csv: Path
    dim_csv: Path
    rows: int
    dim_rows: int


@dataclass(frozen=True)
class EngineRunner:
    name: str
    version: str
    scenarios: dict[str, Callable[[DatasetPaths], Any]]
    warmup: Callable[[], None] | None = None
    close: Callable[[], None] | None = None
    startup_time_millis: float | None = None


class BenchmarkUnsupported(RuntimeError):
    """Raised when an engine cannot execute a benchmark scenario yet."""


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
        help="Comma-separated engines: shardloom,pandas,polars,duckdb,spark-default,spark-local-tuned,datafusion,dask. Alias: spark expands to both Spark profiles.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=SCENARIO_ORDER + STRESS_SCENARIO_ORDER,
        help="Run one scenario. Repeat to run multiple scenarios.",
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
    if args.scenario:
        args.scenario_list = tuple(args.scenario)
    elif args.include_stress:
        args.scenario_list = SCENARIO_ORDER + STRESS_SCENARIO_ORDER
    else:
        args.scenario_list = SCENARIO_ORDER
    args.shardloom_native_iterations = args.shardloom_native_iterations or args.iterations
    if args.shardloom_native_iterations <= 0:
        parser.error("--shardloom-native-iterations must be greater than zero")
    return args


def ensure_dataset(root: Path, rows: int, dim_rows: int, regenerate: bool) -> DatasetPaths:
    fact_csv = root / "fact.csv"
    dim_csv = root / "dim.csv"
    metadata_json = root / "dataset.json"
    if regenerate and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    expected_metadata = {"rows": rows, "dim_rows": dim_rows, "schema_version": 1}
    if fact_csv.exists() and dim_csv.exists() and metadata_json.exists():
        with metadata_json.open("r", encoding="utf-8") as handle:
            if json.load(handle) == expected_metadata:
                return DatasetPaths(root, fact_csv, dim_csv, rows, dim_rows)

    with fact_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "group_key", "dim_key", "value", "metric", "flag", "category"])
        for idx in range(rows):
            group_key = idx % 100
            dim_key = idx % dim_rows
            value = (idx * 17) % 10_000
            metric = ((idx * 13) % 100_000) / 100.0
            flag = 1 if idx % 7 == 0 else 0
            writer.writerow(
                [
                    idx,
                    group_key,
                    dim_key,
                    value,
                    f"{metric:.2f}",
                    flag,
                    f"c{group_key % 10}",
                ]
            )

    with dim_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dim_key", "dim_label", "weight"])
        for idx in range(dim_rows):
            writer.writerow([idx, f"d{idx % 50}", (idx * 3) % 100])

    with metadata_json.open("w", encoding="utf-8") as handle:
        json.dump(expected_metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return DatasetPaths(root, fact_csv, dim_csv, rows, dim_rows)


def module_version(name: str) -> str:
    module = importlib.import_module(name)
    return str(getattr(module, "__version__", "unknown"))


def shardloom_runner() -> EngineRunner:
    root = workspace_root()
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
        "vortex-traditional-analytics-benchmark",
    ]
    if SHARDLOOM_BUILD_PROFILE == "release":
        build_command.append("--release")
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    completed = subprocess_run(build_command, root, env)
    if completed["returncode"] != 0:
        raise BenchmarkUnsupported(
            "ShardLoom CLI build failed before benchmark timing began: "
            + (completed["stderr"] or completed["stdout"] or "unknown failure")
        )
    binary = shardloom_binary_path(root, SHARDLOOM_BUILD_PROFILE)
    if not binary.exists():
        raise BenchmarkUnsupported(f"ShardLoom binary was not found after build: {binary}")

    def run_scenario(scenario: str, paths: DatasetPaths) -> Any:
        workspace = paths.root / "shardloom_universal_io" / scenario_slug(scenario)
        command = [
            str(binary),
            "traditional-analytics-run",
            scenario,
            str(paths.fact_csv),
            str(paths.dim_csv),
            "--workspace",
            str(workspace),
            "--format",
            "json",
        ]
        completed = subprocess_run(command, root, env)
        if completed["returncode"] != 0:
            raise RuntimeError(completed["stderr"] or completed["stdout"] or "unknown failure")
        try:
            payload = json.loads(completed["stdout"].splitlines()[0])
        except (json.JSONDecodeError, IndexError) as exc:
            raise RuntimeError(f"ShardLoom emitted invalid JSON: {exc}") from exc
        fields = parse_output_fields(payload)
        if payload.get("status") != "success":
            reason = fields.get("reason") or payload.get("human_text") or "unsupported"
            raise BenchmarkUnsupported(str(reason))
        required_true_fields = [
            "native_work_envelope_created",
            "native_work_stream_created",
            "native_result_stream_created",
            "native_io_certificate_emitted",
            "csv_source_adapter_used",
            "csv_to_vortex_import_performed",
            "vortex_file_written",
            "vortex_file_read",
            "upstream_vortex_scan_called",
            "materialization_boundary_report_emitted",
        ]
        missing_evidence = [
            field for field in required_true_fields if fields.get(field) != "true"
        ]
        if missing_evidence:
            raise RuntimeError(
                "ShardLoom universal I/O evidence was missing: "
                + ", ".join(missing_evidence)
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
            scenario: (lambda paths, scenario=scenario: run_scenario(scenario, paths))
            for scenario in SCENARIO_ORDER + STRESS_SCENARIO_ORDER
        },
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


def round_float(value: Any) -> float:
    if value is None:
        return 0.0
    number = float(value)
    if math.isnan(number):
        return 0.0
    return round(number, 6)


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


def shardloom_version(root: Path, profile: str) -> str:
    git = shutil.which("git")
    if git is None:
        return f"workspace-local-{profile}"
    completed = subprocess_run([git, "rev-parse", "--short", "HEAD"], root, os.environ.copy())
    if completed["returncode"] != 0:
        return f"workspace-local-{profile}"
    return f"workspace-local-{profile}-{completed['stdout'].strip()}"


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

    def ingest(paths: DatasetPaths) -> Any:
        frame = pd.read_csv(paths.fact_csv)
        return normalize_scalar_result(len(frame), frame["metric"].sum())

    def selective_filter(paths: DatasetPaths) -> Any:
        frame = pd.read_csv(paths.fact_csv)
        filtered = frame[(frame["flag"] == 1) & (frame["value"] >= 5000)]
        return normalize_scalar_result(len(filtered), filtered["metric"].sum())

    def group_by(paths: DatasetPaths) -> Any:
        frame = pd.read_csv(paths.fact_csv)
        grouped = (
            frame.groupby("group_key", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "group_key")

    def top_k(paths: DatasetPaths) -> Any:
        frame = pd.read_csv(paths.fact_csv)
        rows = (
            frame.sort_values(["metric", "id"], ascending=[False, True])
            .head(10)[["id", "metric"]]
            .to_dict("records")
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths) -> Any:
        fact = pd.read_csv(paths.fact_csv)
        dim = pd.read_csv(paths.dim_csv)
        joined = fact.merge(dim, on="dim_key", how="inner")
        grouped = (
            joined.groupby("dim_label", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "dim_label")

    def wide_projection(paths: DatasetPaths) -> Any:
        frame = pd.read_csv(paths.fact_csv)
        projected = frame[["id", "group_key", "category"]]
        return normalize_scalar_result(len(projected), projected["group_key"].sum())

    def distinct_count(paths: DatasetPaths) -> Any:
        frame = pd.read_csv(paths.fact_csv)
        return {"distinct_category_count": int(frame["category"].nunique())}

    def scale_stress(paths: DatasetPaths) -> Any:
        fact = pd.read_csv(paths.fact_csv)
        dim = pd.read_csv(paths.dim_csv)
        expanded = fact.merge(dim, on="dim_key", how="inner")
        expanded["skew_key"] = expanded["group_key"] % 10
        grouped = (
            expanded.groupby("skew_key", as_index=False)
            .agg(row_count=("id", "count"), metric_sum=("metric", "sum"))
            .to_dict("records")
        )
        return normalize_group_rows(grouped, "skew_key")

    def complex_etl(paths: DatasetPaths) -> Any:
        fact = pd.read_csv(paths.fact_csv)
        dim = pd.read_csv(paths.dim_csv)
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
    )


def polars_runner() -> EngineRunner:
    import polars as pl  # type: ignore

    def ingest(paths: DatasetPaths) -> Any:
        frame = pl.read_csv(paths.fact_csv)
        return normalize_scalar_result(frame.height, frame["metric"].sum())

    def selective_filter(paths: DatasetPaths) -> Any:
        frame = pl.read_csv(paths.fact_csv)
        filtered = frame.filter((pl.col("flag") == 1) & (pl.col("value") >= 5000))
        return normalize_scalar_result(filtered.height, filtered["metric"].sum())

    def group_by(paths: DatasetPaths) -> Any:
        frame = pl.read_csv(paths.fact_csv)
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

    def top_k(paths: DatasetPaths) -> Any:
        frame = pl.read_csv(paths.fact_csv)
        rows = (
            frame.sort(["metric", "id"], descending=[True, False])
            .head(10)
            .select(["id", "metric"])
            .to_dicts()
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths) -> Any:
        fact = pl.read_csv(paths.fact_csv)
        dim = pl.read_csv(paths.dim_csv)
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

    def wide_projection(paths: DatasetPaths) -> Any:
        frame = pl.read_csv(paths.fact_csv)
        projected = frame.select(["id", "group_key", "category"])
        return normalize_scalar_result(projected.height, projected["group_key"].sum())

    def distinct_count(paths: DatasetPaths) -> Any:
        frame = pl.read_csv(paths.fact_csv)
        return {"distinct_category_count": int(frame["category"].n_unique())}

    def scale_stress(paths: DatasetPaths) -> Any:
        fact = pl.read_csv(paths.fact_csv)
        dim = pl.read_csv(paths.dim_csv)
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

    def complex_etl(paths: DatasetPaths) -> Any:
        fact = pl.read_csv(paths.fact_csv)
        dim = pl.read_csv(paths.dim_csv)
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
    )


def duckdb_runner() -> EngineRunner:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")

    def query(paths: DatasetPaths, sql: str) -> list[dict[str, Any]]:
        sql = sql.replace("{fact}", sql_literal(paths.fact_csv)).replace(
            "{dim}", sql_literal(paths.dim_csv)
        )
        columns = [column[0] for column in con.execute(sql).description]
        return [dict(zip(columns, row)) for row in con.fetchall()]

    def ingest(paths: DatasetPaths) -> Any:
        rows = query(
            paths,
            "select count(*) as row_count, sum(metric) as metric_sum from read_csv_auto({fact})",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def selective_filter(paths: DatasetPaths) -> Any:
        rows = query(
            paths,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from read_csv_auto({fact}) where flag = 1 and value >= 5000",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def group_by(paths: DatasetPaths) -> Any:
        return normalize_group_rows(
            query(
                paths,
                "select group_key, count(*) as row_count, sum(metric) as metric_sum "
                "from read_csv_auto({fact}) group by group_key",
            ),
            "group_key",
        )

    def top_k(paths: DatasetPaths) -> Any:
        return normalize_top_rows(
            query(
                paths,
                "select id, metric from read_csv_auto({fact}) "
                "order by metric desc, id asc limit 10",
            )
        )

    def hash_join(paths: DatasetPaths) -> Any:
        return normalize_group_rows(
            query(
                paths,
                "select d.dim_label, count(*) as row_count, sum(f.metric) as metric_sum "
                "from read_csv_auto({fact}) f join read_csv_auto({dim}) d "
                "on f.dim_key = d.dim_key group by d.dim_label",
            ),
            "dim_label",
        )

    def wide_projection(paths: DatasetPaths) -> Any:
        rows = query(
            paths,
            "select count(*) as row_count, sum(group_key) as metric_sum "
            "from (select id, group_key, category from read_csv_auto({fact}))",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def distinct_count(paths: DatasetPaths) -> Any:
        rows = query(
            paths,
            "select count(distinct category) as distinct_category_count from read_csv_auto({fact})",
        )
        return {"distinct_category_count": int(rows[0]["distinct_category_count"])}

    def scale_stress(paths: DatasetPaths) -> Any:
        return normalize_group_rows(
            query(
                paths,
                "select f.group_key % 10 as skew_key, count(*) as row_count, sum(f.metric) as metric_sum "
                "from read_csv_auto({fact}) f join read_csv_auto({dim}) d "
                "on f.dim_key = d.dim_key group by skew_key",
            ),
            "skew_key",
        )

    def complex_etl(paths: DatasetPaths) -> Any:
        return normalize_complex_etl_rows(
            query(
                paths,
                "select d.dim_label, f.group_key % 10 as bucket, count(*) as row_count, "
                "sum(f.metric) as metric_sum, sum(f.metric * (d.weight + 1)) as weighted_sum "
                "from read_csv_auto({fact}) f join read_csv_auto({dim}) d "
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
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

    def read_fact(paths: DatasetPaths) -> Any:
        return spark_instance().read.option("header", True).option("inferSchema", True).csv(
            str(paths.fact_csv)
        )

    def read_dim(paths: DatasetPaths) -> Any:
        return spark_instance().read.option("header", True).option("inferSchema", True).csv(
            str(paths.dim_csv)
        )

    def ingest(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)
        row = frame.agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum")).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def selective_filter(paths: DatasetPaths) -> Any:
        frame = read_fact(paths).where((F.col("flag") == 1) & (F.col("value") >= 5000))
        row = frame.agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum")).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def group_by(paths: DatasetPaths) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths)
            .groupBy("group_key")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths)
            .orderBy(F.col("metric").desc(), F.col("id").asc())
            .select("id", "metric")
            .limit(10)
            .collect()
        ]
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths)
            .join(read_dim(paths), on="dim_key", how="inner")
            .groupBy("dim_label")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths) -> Any:
        frame = read_fact(paths).select("id", "group_key", "category")
        row = frame.agg(
            F.count("*").alias("row_count"), F.sum("group_key").alias("metric_sum")
        ).first()
        return normalize_scalar_result(row["row_count"], row["metric_sum"])

    def distinct_count(paths: DatasetPaths) -> Any:
        row = read_fact(paths).agg(F.countDistinct("category").alias("distinct_category_count")).first()
        return {"distinct_category_count": int(row["distinct_category_count"])}

    def scale_stress(paths: DatasetPaths) -> Any:
        rows = [
            row.asDict()
            for row in read_fact(paths)
            .join(read_dim(paths), on="dim_key", how="inner")
            .withColumn("skew_key", F.col("group_key") % F.lit(10))
            .groupBy("skew_key")
            .agg(F.count("*").alias("row_count"), F.sum("metric").alias("metric_sum"))
            .collect()
        ]
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths) -> Any:
        joined = (
            read_fact(paths)
            .where(F.col("value") >= 2500)
            .join(read_dim(paths), on="dim_key", how="inner")
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
        warmup=warmup_spark,
        close=close_spark,
    )


def spark_default_runner() -> EngineRunner:
    return spark_runner("default")


def spark_local_tuned_runner() -> EngineRunner:
    return spark_runner("local-tuned")


def datafusion_runner() -> EngineRunner:
    import datafusion  # type: ignore

    def query(paths: DatasetPaths, sql: str) -> list[dict[str, Any]]:
        ctx = datafusion.SessionContext()
        ctx.register_csv("fact", paths.fact_csv, has_header=True)
        ctx.register_csv("dim", paths.dim_csv, has_header=True)
        return pyarrow_rows(ctx.sql(sql).collect())

    def ingest(paths: DatasetPaths) -> Any:
        rows = query(paths, "select count(*) as row_count, sum(metric) as metric_sum from fact")
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def selective_filter(paths: DatasetPaths) -> Any:
        rows = query(
            paths,
            "select count(*) as row_count, sum(metric) as metric_sum "
            "from fact where flag = 1 and value >= 5000",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def group_by(paths: DatasetPaths) -> Any:
        return normalize_group_rows(
            query(
                paths,
                "select group_key, count(*) as row_count, sum(metric) as metric_sum "
                "from fact group by group_key",
            ),
            "group_key",
        )

    def top_k(paths: DatasetPaths) -> Any:
        return normalize_top_rows(
            query(paths, "select id, metric from fact order by metric desc, id asc limit 10")
        )

    def hash_join(paths: DatasetPaths) -> Any:
        return normalize_group_rows(
            query(
                paths,
                "select d.dim_label, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key group by d.dim_label",
            ),
            "dim_label",
        )

    def wide_projection(paths: DatasetPaths) -> Any:
        rows = query(
            paths,
            "select count(*) as row_count, sum(group_key) as metric_sum "
            "from (select id, group_key, category from fact)",
        )
        return normalize_scalar_result(rows[0]["row_count"], rows[0]["metric_sum"])

    def distinct_count(paths: DatasetPaths) -> Any:
        rows = query(paths, "select count(distinct category) as distinct_category_count from fact")
        return {"distinct_category_count": int(rows[0]["distinct_category_count"])}

    def scale_stress(paths: DatasetPaths) -> Any:
        return normalize_group_rows(
            query(
                paths,
                "select f.group_key % 10 as skew_key, count(*) as row_count, sum(f.metric) as metric_sum "
                "from fact f join dim d on f.dim_key = d.dim_key group by skew_key",
            ),
            "skew_key",
        )

    def complex_etl(paths: DatasetPaths) -> Any:
        return normalize_complex_etl_rows(
            query(
                paths,
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
    )


def dask_runner() -> EngineRunner:
    import dask  # type: ignore
    import dask.dataframe as dd  # type: ignore

    blocksize = None if DASK_BLOCKSIZE == "default" else DASK_BLOCKSIZE

    def read_fact(paths: DatasetPaths) -> Any:
        return dd.read_csv(paths.fact_csv, blocksize=blocksize)

    def read_dim(paths: DatasetPaths) -> Any:
        return dd.read_csv(paths.dim_csv, blocksize=blocksize)

    def compute_one(*values: Any) -> tuple[Any, ...]:
        return dask.compute(*values, scheduler=DASK_SCHEDULER)

    def compute_frame(value: Any) -> Any:
        return value.compute(scheduler=DASK_SCHEDULER)

    def ingest(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)
        row_count, metric_sum = compute_one(frame.id.count(), frame.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def selective_filter(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)
        filtered = frame[(frame.flag == 1) & (frame.value >= 5000)]
        row_count, metric_sum = compute_one(filtered.id.count(), filtered.metric.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def group_by(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)
        counts = frame.groupby("group_key").id.count().rename("row_count")
        sums = frame.groupby("group_key").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "group_key")

    def top_k(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)
        rows = (
            compute_frame(frame.nlargest(10, "metric")[["id", "metric"]])
            .sort_values(["metric", "id"], ascending=[False, True])
            .to_dict("records")
        )
        return normalize_top_rows(rows)

    def hash_join(paths: DatasetPaths) -> Any:
        fact = read_fact(paths)
        dim = read_dim(paths)
        joined = fact.merge(dim, on="dim_key", how="inner")
        counts = joined.groupby("dim_label").id.count().rename("row_count")
        sums = joined.groupby("dim_label").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "dim_label")

    def wide_projection(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)[["id", "group_key", "category"]]
        row_count, metric_sum = compute_one(frame.id.count(), frame.group_key.sum())
        return normalize_scalar_result(row_count, metric_sum)

    def distinct_count(paths: DatasetPaths) -> Any:
        frame = read_fact(paths)
        distinct = compute_frame(frame.category.nunique())
        return {"distinct_category_count": int(distinct)}

    def scale_stress(paths: DatasetPaths) -> Any:
        fact = read_fact(paths)
        dim = read_dim(paths)
        joined = fact.merge(dim, on="dim_key", how="inner")
        joined = joined.assign(skew_key=joined.group_key % 10)
        counts = joined.groupby("skew_key").id.count().rename("row_count")
        sums = joined.groupby("skew_key").metric.sum().rename("metric_sum")
        rows = compute_frame(dd.concat([counts, sums], axis=1).reset_index()).to_dict("records")
        return normalize_group_rows(rows, "skew_key")

    def complex_etl(paths: DatasetPaths) -> Any:
        fact = read_fact(paths)
        dim = read_dim(paths)
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
            "scale stress skewed join aggregation": scale_stress,
            "scale stress multi-stage etl": complex_etl,
        },
    )


ENGINE_FACTORIES: dict[str, Callable[[], EngineRunner]] = {
    "shardloom": shardloom_runner,
    "pandas": pandas_runner,
    "polars": polars_runner,
    "duckdb": duckdb_runner,
    "spark-default": spark_default_runner,
    "spark-local-tuned": spark_local_tuned_runner,
    "datafusion": datafusion_runner,
    "dask": dask_runner,
}


def scenario_bytes(paths: DatasetPaths, scenario: str) -> int:
    total = 0
    for name in SCENARIO_BYTES[scenario]:
        total += (paths.fact_csv if name == "fact" else paths.dim_csv).stat().st_size
    return total


def rows_scanned(paths: DatasetPaths, scenario: str) -> int:
    if scenario in {
        "hash join",
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


def failed_result(
    engine: str,
    scenario: str,
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
        "bytes_read": scenario_bytes(paths, scenario),
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
        "scenario_name": scenario,
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
        "external_baseline_only": engine != "shardloom",
    }


def run_one(
    runner: EngineRunner,
    paths: DatasetPaths,
    scenario: str,
    iterations: int,
) -> dict[str, Any]:
    scenario_fn = runner.scenarios[scenario]
    values = []
    evidence_rows = []
    timings = []
    peak_memory = []
    for _ in range(iterations):
        started = time.perf_counter()
        with MemorySampler() as sampler:
            try:
                value, evidence = unwrap_engine_value(scenario_fn(paths))
            except BenchmarkUnsupported as exc:
                elapsed = (time.perf_counter() - started) * 1000.0
                return failed_result(
                    runner.name,
                    scenario,
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
    return {
        "scenario_name": scenario,
        "engine": runner.name,
        "status": "success" if stable else "unstable_output",
        "iterations": iterations,
        "iteration_wall_time_millis": [round(value, 4) for value in timings],
        "metrics": {
            "wall_time_millis": round(sum(timings), 4),
            "query_runtime_millis": round(statistics.mean(timings), 4),
            "peak_memory_bytes": max(peak_memory) if peak_memory else None,
            "bytes_read": scenario_bytes(paths, scenario),
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
        "external_baseline_only": runner.name != "shardloom",
    }


def run_shardloom_native_microbenchmarks(iterations: int) -> list[dict[str, Any]]:
    root = workspace_root()
    fixture = root / "shardloom-vortex" / "tests" / "fixtures" / "metadata_footer_u64_20000.vortex"
    cargo = shutil.which("cargo")
    if cargo is None:
        return [
            {
                "name": "local encoded CountAll",
                "status": "missing_cargo",
                "reason": "cargo was not found on PATH, so the feature-gated ShardLoom microbenchmark could not run.",
            }
        ]
    if not fixture.exists():
        return [
            {
                "name": "local encoded CountAll",
                "status": "missing_fixture",
                "reason": f"Vortex fixture was not found at {fixture}",
            }
        ]
    command = [
        cargo,
        "run",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        "vortex-encoded-read-spike",
        "--",
        "vortex-count-benchmark",
        str(fixture),
        "1",
        "1",
        "--iterations",
        str(iterations),
        "--format",
        "json",
    ]
    env = os.environ.copy()
    env["RUSTUP_TOOLCHAIN"] = env.get("RUSTUP_TOOLCHAIN", "1.91.1")
    started = time.perf_counter()
    completed = subprocess_run(command, root, env)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if completed["returncode"] != 0:
        return [
            {
                "name": "local encoded CountAll",
                "status": "execution_error",
                "reason": completed["stderr"] or completed["stdout"] or "unknown failure",
                "command": command,
                "elapsed_millis": round(elapsed_ms, 4),
            }
        ]
    try:
        payload = json.loads(completed["stdout"].splitlines()[0])
    except (json.JSONDecodeError, IndexError) as exc:
        return [
            {
                "name": "local encoded CountAll",
                "status": "invalid_output",
                "reason": f"{type(exc).__name__}: {exc}",
                "command": command,
                "elapsed_millis": round(elapsed_ms, 4),
            }
        ]
    fields = parse_output_fields(payload)
    return [
        {
            "name": "local encoded CountAll",
            "status": payload.get("status", "unknown"),
            "dataset": str(fixture),
            "rows": fields.get("count"),
            "iterations": fields.get("iterations_completed"),
            "query_runtime_millis": fields.get("avg_query_runtime_millis"),
            "query_runtime_micros": fields.get("avg_query_runtime_micros"),
            "comparison_status": fields.get("comparison_status"),
            "claim_gate_status": fields.get("claim_gate_status"),
            "data_read": fields.get("data_read"),
            "data_decoded": fields.get("data_decoded"),
            "data_materialized": fields.get("data_materialized"),
            "row_read": fields.get("row_read"),
            "arrow_converted": fields.get("arrow_converted"),
            "fallback_attempted": fields.get("fallback_attempted"),
            "performance_claim_allowed": fields.get("performance_claim_allowed"),
            "command": command,
        }
    ]


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
            "name": "CSV -> ShardLoom NativeWorkStream -> Vortex",
            "status": "smoke_supported",
            "reason": "ShardLoom benchmark rows use a deterministic CSV source adapter/import, emit native work/native result evidence fields, write local Vortex files, reopen them through Vortex, and scan Vortex arrays. The path still materializes Vortex-derived arrays for the temporary operators.",
            "expected_report": "SourceCapabilityReport plus NativeIoCertificate evidence fields",
        },
        {
            "name": "CSV -> Vortex import -> encoded CountAll",
            "status": "partial_smoke_supported",
            "reason": "CSV-to-Vortex import and Vortex scan are exercised by ShardLoom traditional rows. Fully encoded CountAll over the imported artifact remains a separate CG-2/CG-19 follow-up because current traditional rows materialize Vortex-derived arrays for operator evaluation.",
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
        "storage_format": "csv baselines; ShardLoom CSV source adapter into local Vortex files",
        "compression": "engine defaults; ShardLoom uses upstream Vortex writer defaults",
        "iterations": args.iterations,
        "stress_lane_included": any(
            scenario in STRESS_SCENARIO_ORDER for scenario in args.scenario_list
        ),
        "cache_mode": args.cache_mode,
        "timing_scope": args.timing_scope,
        "engines_requested": list(args.engine_list),
        "scenarios_requested": list(args.scenario_list),
        "shardloom_build_profile": args.shardloom_build_profile,
        "shardloom_build_time_excluded": True,
        "shardloom_feature_gate": "vortex-traditional-analytics-benchmark",
        "dask_blocksize": args.dask_blocksize,
        "dask_scheduler": args.dask_scheduler,
        "spark_requires_java": True,
        "spark_profiles": "spark-default local[*] with Spark defaults; spark-local-tuned local[*] with shuffle/default parallelism capped to local CPU count and AQE enabled",
        "java_on_path": shutil.which("java") is not None,
        "java_home_set": bool(os.environ.get("JAVA_HOME")),
        "object_store_included": False,
        "csv_to_vortex_included": True,
        "shardloom_universal_io_smoke_included": True,
        "shardloom_native_microbenchmarks_included": not args.skip_shardloom_native,
        "claim_grade_requirements": [
            "pin engine versions",
            "declare hardware profile",
            "separate cold-cache and warm-cache runs",
            "use larger-than-memory and object-store datasets where relevant",
            "record ShardLoom native and universal-I/O rows separately from external CSV baselines",
            "run multiple repetitions under the same process isolation policy",
        ],
    }


def default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(__file__).resolve().parent / "results" / f"traditional_analytics_{timestamp}.json"


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
        ["Iterations", str(params["iterations"])],
        ["Stress lane included", str(params["stress_lane_included"])],
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
        ["CSV to Vortex included", str(params["csv_to_vortex_included"])],
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
        "External baseline rows measure each engine's local CSV path. ShardLoom rows use a CSV source adapter into local Vortex files, reopen those files through Vortex, scan Vortex arrays, and then run the temporary benchmark operators over Vortex-derived arrays.",
        "ShardLoom's current traditional rows report a materialization boundary; they prove universal I/O viability, not mature encoded-native SQL/operator coverage.",
        "Dask results depend heavily on partitioning, scheduler, file count, and dataset size; small single-file CSV tests can make scheduler overhead dominate.",
        "Spark rows are split into spark-default and spark-local-tuned so default behavior is not mixed with local tuning; each Spark profile starts and warms its own session immediately before its scenario rows.",
        "Spark rows require Java/JDK. Missing Spark rows mean local setup is incomplete, not that Spark failed the workload.",
        "Stress rows are opt-in; they become meaningful Spark-style scale tests only with larger-than-memory data, stable cache policy, and explicit hardware/runtime settings.",
        "ShardLoom benchmark build time is excluded from per-scenario timing; CSV-to-Vortex import, Vortex file write/read, scan, and the temporary benchmark operator are included.",
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
        if result["engine"] != "shardloom":
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
                str(evidence.get("native_io_certificate_emitted", "n/a")),
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
            "Native I/O cert",
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
                str(result.get("rows", "n/a")),
                format_metric(result.get("query_runtime_millis"), " ms"),
                str(result.get("data_decoded", "n/a")),
                str(result.get("data_materialized", "n/a")),
                str(result.get("fallback_attempted", "n/a")),
                str(result.get("claim_gate_status", "n/a")),
            ]
        )
    if not rows:
        rows.append(["not run", "skipped", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
    return markdown_table(
        [
            "Microbenchmark",
            "Status",
            "Rows",
            "Avg runtime",
            "Decoded",
            "Materialized",
            "Fallback",
            "Claim gate",
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
        f"- Rows: `{dataset['rows']}` fact rows, `{dataset['dim_rows']}` dimension rows",
        f"- Files: `{dataset['fact_csv_bytes']}` fact CSV bytes, `{dataset['dim_csv_bytes']}` dimension CSV bytes",
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
        "These rows are not directly comparable to CSV engine rows. They show the current native encoded/Vortex path that ShardLoom can execute today.",
        "",
        render_shardloom_native_table(artifact),
        "",
        "## Universal I/O And CSV-To-Vortex Lanes",
        "",
        "These lanes make the missing ShardLoom universal-I/O work explicit instead of hiding it behind the CSV comparison.",
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
    global DASK_BLOCKSIZE, DASK_SCHEDULER, SHARDLOOM_BUILD_PROFILE
    args = parse_args()
    DASK_BLOCKSIZE = args.dask_blocksize
    DASK_SCHEDULER = args.dask_scheduler
    SHARDLOOM_BUILD_PROFILE = args.shardloom_build_profile
    configure_java_home()
    paths = ensure_dataset(args.data_dir, args.rows, args.dim_rows, args.regenerate)
    runners, missing = available_runners(args.engine_list)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for engine in args.engine_list:
        runner = runners.get(engine)
        if runner is None:
            reason = missing.get(engine, "engine was not initialized")
            for scenario in args.scenario_list:
                result = failed_result(
                    engine, scenario, "missing_dependency", reason, paths, args.iterations
                )
                results.append(result)
                errors.append(
                    {
                        "engine": engine,
                        "scenario": scenario,
                        "status": "missing_dependency",
                        "reason": reason,
                    }
                )
            continue
        try:
            try:
                runner = warmup_runner(runner)
                runners[engine] = runner
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                for scenario in args.scenario_list:
                    result = failed_result(
                        engine,
                        scenario,
                        "engine_startup_error",
                        reason,
                        paths,
                        args.iterations,
                    )
                    results.append(result)
                    errors.append(
                        {
                            "engine": engine,
                            "scenario": scenario,
                            "status": "engine_startup_error",
                            "reason": reason,
                        }
                    )
                continue
            for scenario in args.scenario_list:
                result = run_one(runner, paths, scenario, args.iterations)
                results.append(result)
                if result["status"] != "success":
                    errors.append(
                        {
                            "engine": engine,
                            "scenario": scenario,
                            "status": result["status"],
                            "reason": result.get("reason", "scenario did not complete"),
                        }
                    )
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
            "fact_csv": str(paths.fact_csv),
            "dim_csv": str(paths.dim_csv),
            "fact_csv_bytes": paths.fact_csv.stat().st_size,
            "dim_csv_bytes": paths.dim_csv.stat().st_size,
            "deterministic_generator": "benchmarks/traditional_analytics/run.py",
        },
        "environment": environment_report(),
        "fairness_parameters": fairness_parameters(args, paths),
        "engine_order": list(args.engine_list),
        "engine_versions": engine_versions,
        "scenario_order": list(args.scenario_list),
        "results": results,
        "shardloom_native_microbenchmarks": []
        if args.skip_shardloom_native
        else run_shardloom_native_microbenchmarks(args.shardloom_native_iterations),
        "universal_io_lanes": universal_io_lanes(),
        "correctness": correctness_summary(results, args.scenario_list),
        "errors": errors,
        "limitations": [
            "CSV workloads include local file read cost and do not represent object-store behavior.",
            "ShardLoom traditional rows include local CSV-to-Vortex import and Vortex scan, but current temporary operators materialize Vortex-derived arrays instead of executing the full mature encoded SQL/operator surface.",
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
