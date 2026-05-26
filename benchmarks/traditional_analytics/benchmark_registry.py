#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Benchmark lane and profile registry for static benchmark publishing.

This module is benchmark tooling only. It is not imported by ShardLoom runtime
code and must not be used as fallback execution for unsupported ShardLoom plans.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SCHEMA_VERSION = "shardloom.benchmark_registry.v1"
MANIFEST_SCHEMA_VERSION = "shardloom.website_benchmark_manifest.v1"


@dataclass(frozen=True)
class Lane:
    name: str
    group: str
    external_baseline_only: bool
    module: str | None = None
    version_attribute: str = "__version__"
    executable: str | None = None
    requires_java: bool = False
    requires_gpu: bool = False
    adapter_backend: str | None = None
    availability_hint: str = ""


@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    required_lanes: tuple[str, ...]
    optional_lanes: tuple[str, ...]
    required_formats: tuple[str, ...]
    optional_formats: tuple[str, ...]
    required_scenarios: tuple[str, ...]
    optional_scenarios: tuple[str, ...]
    hardware_class: str
    object_store_runtime_required: bool
    claim_boundary: str


LANES: dict[str, Lane] = {
    "shardloom": Lane(
        "shardloom",
        "shardloom",
        external_baseline_only=False,
        availability_hint="workspace-local ShardLoom benchmark lane",
    ),
    "shardloom-prepared-vortex": Lane(
        "shardloom-prepared-vortex",
        "shardloom",
        external_baseline_only=False,
        availability_hint="workspace-local prepared Vortex benchmark lane",
    ),
    "shardloom-prepare-batch": Lane(
        "shardloom-prepare-batch",
        "shardloom",
        external_baseline_only=False,
        availability_hint=(
            "workspace-local single-process compatibility prepare plus prepared/native "
            "batch benchmark lane"
        ),
    ),
    "shardloom-vortex": Lane(
        "shardloom-vortex",
        "shardloom",
        external_baseline_only=False,
        availability_hint="workspace-local native Vortex benchmark lane",
    ),
    "native-vortex": Lane(
        "native-vortex",
        "shardloom",
        external_baseline_only=False,
        availability_hint="alias vocabulary for native Vortex benchmark rows",
    ),
    "pandas": Lane("pandas", "core_local_baseline", True, module="pandas"),
    "polars-eager": Lane("polars-eager", "core_local_baseline", True, module="polars"),
    "polars-lazy": Lane("polars-lazy", "core_local_baseline", True, module="polars"),
    "duckdb": Lane("duckdb", "core_local_baseline", True, module="duckdb"),
    "datafusion": Lane("datafusion", "core_local_baseline", True, module="datafusion"),
    "dask": Lane("dask", "core_local_baseline", True, module="dask"),
    "spark-default": Lane(
        "spark-default",
        "spark_baseline",
        True,
        module="pyspark",
        requires_java=True,
    ),
    "spark-local-tuned": Lane(
        "spark-local-tuned",
        "spark_baseline",
        True,
        module="pyspark",
        requires_java=True,
    ),
    "pyarrow-dataset": Lane(
        "pyarrow-dataset",
        "extended_optional_baseline",
        True,
        module="pyarrow.dataset",
    ),
    "pyarrow-acero": Lane(
        "pyarrow-acero",
        "extended_optional_baseline",
        True,
        module="pyarrow",
        availability_hint="pyarrow installed; Acero execution support remains profile-gated",
    ),
    "clickhouse-local": Lane(
        "clickhouse-local",
        "extended_optional_baseline",
        True,
        executable="clickhouse-local",
    ),
    "daft": Lane("daft", "extended_optional_baseline", True, module="daft"),
    "ray-data": Lane("ray-data", "extended_optional_baseline", True, module="ray"),
    "ibis-duckdb": Lane(
        "ibis-duckdb",
        "extended_optional_baseline",
        True,
        module="ibis",
        adapter_backend="duckdb",
    ),
    "ibis-datafusion": Lane(
        "ibis-datafusion",
        "extended_optional_baseline",
        True,
        module="ibis",
        adapter_backend="datafusion",
    ),
    "ibis-polars": Lane(
        "ibis-polars",
        "extended_optional_baseline",
        True,
        module="ibis",
        adapter_backend="polars",
    ),
    "cudf-gpu": Lane(
        "cudf-gpu",
        "gpu_optional_baseline",
        True,
        module="cudf",
        requires_gpu=True,
    ),
}


PROFILES: dict[str, BenchmarkProfile] = {
    "smoke": BenchmarkProfile(
        name="smoke",
        required_lanes=(
            "shardloom",
            "shardloom-prepared-vortex",
            "shardloom-prepare-batch",
            "shardloom-vortex",
        ),
        optional_lanes=(),
        required_formats=("csv",),
        optional_formats=("parquet",),
        required_scenarios=("selective filter",),
        optional_scenarios=("group by aggregation", "hash join", "top-N per group"),
        hardware_class="cpu_local",
        object_store_runtime_required=False,
        claim_boundary="local_smoke_not_claim_grade",
    ),
    "full_local": BenchmarkProfile(
        name="full_local",
        required_lanes=(
            "shardloom",
            "shardloom-prepared-vortex",
            "shardloom-prepare-batch",
            "shardloom-vortex",
            "pandas",
            "polars-eager",
            "polars-lazy",
            "duckdb",
            "datafusion",
            "dask",
        ),
        optional_lanes=(),
        required_formats=("csv", "parquet"),
        optional_formats=("jsonl", "arrow-ipc", "avro", "orc"),
        required_scenarios=(
            "selective filter",
            "filter + projection + limit",
            "group by aggregation",
            "hash join",
            "top-N per group",
        ),
        optional_scenarios=(
            "dirty CSV / clean-cast",
            "nested JSON field scan",
            "CDC overlay",
            "null-heavy aggregate",
        ),
        hardware_class="cpu_local",
        object_store_runtime_required=False,
        claim_boundary="local_comparative_not_claim_grade",
    ),
    "full_local_plus_spark": BenchmarkProfile(
        name="full_local_plus_spark",
        required_lanes=(
            "shardloom",
            "shardloom-prepared-vortex",
            "shardloom-prepare-batch",
            "shardloom-vortex",
            "pandas",
            "polars-eager",
            "polars-lazy",
            "duckdb",
            "datafusion",
            "dask",
            "spark-default",
            "spark-local-tuned",
        ),
        optional_lanes=(),
        required_formats=("csv", "parquet"),
        optional_formats=("jsonl", "arrow-ipc", "avro", "orc"),
        required_scenarios=(
            "selective filter",
            "filter + projection + limit",
            "group by aggregation",
            "hash join",
            "top-N per group",
        ),
        optional_scenarios=("dirty CSV / clean-cast", "nested JSON field scan", "CDC overlay"),
        hardware_class="cpu_local_jvm",
        object_store_runtime_required=False,
        claim_boundary="local_comparative_with_spark_not_claim_grade",
    ),
    "extended_local": BenchmarkProfile(
        name="extended_local",
        required_lanes=(
            "shardloom",
            "shardloom-prepared-vortex",
            "shardloom-prepare-batch",
            "shardloom-vortex",
        ),
        optional_lanes=(
            "pandas",
            "polars-eager",
            "polars-lazy",
            "duckdb",
            "datafusion",
            "dask",
            "pyarrow-dataset",
            "pyarrow-acero",
            "clickhouse-local",
            "daft",
            "ray-data",
            "ibis-duckdb",
            "ibis-datafusion",
            "ibis-polars",
        ),
        required_formats=("csv", "parquet"),
        optional_formats=("jsonl", "arrow-ipc", "avro", "orc"),
        required_scenarios=("selective filter", "group by aggregation"),
        optional_scenarios=("nested JSON field scan", "dirty CSV / clean-cast", "CDC overlay"),
        hardware_class="cpu_local_extended",
        object_store_runtime_required=False,
        claim_boundary="extended_local_context_not_claim_grade",
    ),
    "gpu_optional": BenchmarkProfile(
        name="gpu_optional",
        required_lanes=("shardloom",),
        optional_lanes=("cudf-gpu",),
        required_formats=("csv", "parquet"),
        optional_formats=(),
        required_scenarios=("selective filter", "group by aggregation"),
        optional_scenarios=(),
        hardware_class="gpu_optional",
        object_store_runtime_required=False,
        claim_boundary="gpu_context_not_cpu_local_claim_grade",
    ),
    "object_store_optional": BenchmarkProfile(
        name="object_store_optional",
        required_lanes=("shardloom",),
        optional_lanes=(),
        required_formats=(),
        optional_formats=("s3", "gcs", "adls"),
        required_scenarios=(),
        optional_scenarios=("object-store read", "object-store write"),
        hardware_class="network_object_store",
        object_store_runtime_required=True,
        claim_boundary="object_store_report_only_until_runtime_admitted",
    ),
    "io_reuse_and_fanout": BenchmarkProfile(
        name="io_reuse_and_fanout",
        required_lanes=(
            "shardloom",
            "shardloom-prepared-vortex",
            "shardloom-prepare-batch",
            "shardloom-vortex",
        ),
        optional_lanes=(),
        required_formats=("csv", "parquet", "vortex"),
        optional_formats=("jsonl", "arrow-ipc", "avro", "orc"),
        required_scenarios=("io_reuse_and_fanout", "source_state_reuse"),
        optional_scenarios=("output_plan_reuse", "generated_source_output"),
        hardware_class="cpu_local",
        object_store_runtime_required=False,
        claim_boundary="io_reuse_and_fanout_not_performance_claim",
    ),
}


def profile_names() -> tuple[str, ...]:
    return tuple(PROFILES)


def lane_names() -> tuple[str, ...]:
    return tuple(LANES)


def profile_dict(profile_name: str) -> dict[str, Any]:
    profile = PROFILES[profile_name]
    return {
        "benchmark_profile": profile.name,
        "required_lanes": list(profile.required_lanes),
        "optional_lanes": list(profile.optional_lanes),
        "required_formats": list(profile.required_formats),
        "optional_formats": list(profile.optional_formats),
        "required_scenarios": list(profile.required_scenarios),
        "optional_scenarios": list(profile.optional_scenarios),
        "hardware_class": profile.hardware_class,
        "object_store_runtime_required": profile.object_store_runtime_required,
        "claim_boundary": profile.claim_boundary,
    }


def expected_lanes_for_profile(profile_name: str) -> tuple[str, ...]:
    profile = PROFILES[profile_name]
    seen: list[str] = []
    for lane in profile.required_lanes + profile.optional_lanes:
        if lane not in seen:
            seen.append(lane)
    return tuple(seen)


def lane_required_for_profile(profile_name: str, lane: str) -> bool:
    return lane in PROFILES[profile_name].required_lanes


def registry_document() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "profiles": {name: profile_dict(name) for name in PROFILES},
        "lanes": {
            name: {
                "group": lane.group,
                "external_baseline_only": lane.external_baseline_only,
                "module": lane.module,
                "executable": lane.executable,
                "requires_java": lane.requires_java,
                "requires_gpu": lane.requires_gpu,
                "adapter_backend": lane.adapter_backend,
                "availability_hint": lane.availability_hint,
            }
            for name, lane in LANES.items()
        },
    }
