#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Generate targeted ShardLoom prepare-batch role-repair evidence rows."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_RUNNER = ROOT / "benchmarks" / "traditional_analytics" / "run.py"
DEFAULT_OUTPUT = (
    ROOT
    / "website"
    / "assets"
    / "benchmarks"
    / "latest"
    / "prepare-batch-role-repair-evidence.json"
)
DEFAULT_PUBLIC_OUTPUT = (
    ROOT
    / "website-public"
    / "assets"
    / "benchmarks"
    / "latest"
    / "prepare-batch-role-repair-evidence.json"
)
DEFAULT_DATA_DIR = ROOT / "target" / "prepare-batch-role-repair-evidence" / "data"
EVIDENCE_SCHEMA_VERSION = "shardloom.prepare_batch_role_repair_evidence.v1"
SCENARIOS = ("hash join", "small change over large base")
EVIDENCE_TOP_LEVEL_KEYS = (
    "engine",
    "status",
    "scenario",
    "operation",
    "storage_format",
    "selected_execution_mode",
    "actual_evidence_tier",
    "timing_surface",
    "claim_gate_status",
    "route_lane_id",
    "fallback_attempted",
    "external_engine_invoked",
)
EVIDENCE_METRIC_KEYS = (
    "prepare_batch_runtime_status",
    "prepare_batch_route",
    "prepare_batch_lifecycle_write_reopen_status",
    "prepare_batch_fallback_attempted",
    "prepare_batch_external_engine_invoked",
    "prepare_batch_prepared_state_dependency_fallback_attempted",
    "prepare_batch_prepared_state_dependency_external_engine_invoked",
    "prepare_batch_prepared_state_optimization_strategy",
    "prepare_batch_prepared_state_optimization_status",
    "prepare_batch_prepared_state_optimization_repaired_roles",
    "prepare_batch_prepared_state_optimization_no_fallback_policy_status",
    "prepare_batch_prepared_state_optimization_fallback_attempted",
    "prepare_batch_prepared_state_optimization_external_engine_invoked",
    "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed",
    "prepare_batch_prepared_state_partial_repair_status",
    "prepare_batch_prepared_state_partial_repair_reused_roles",
    "prepare_batch_prepared_state_partial_repair_repaired_roles",
    "prepare_batch_prepared_state_partial_repair_regeneration_performed",
    "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed",
    "prepare_batch_prepared_state_partial_repair_replay_proof",
    "prepare_batch_prepared_state_partial_repair_micros",
    "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_write_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros",
)


def load_benchmark_runner() -> Any:
    spec = importlib.util.spec_from_file_location(
        "shardloom_traditional_analytics_runner_for_role_repair",
        BENCHMARK_RUNNER,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load benchmark runner at {BENCHMARK_RUNNER}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def git_sha() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
    ).strip()


def repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def append_fact_row(bench: Any, paths: Any, row_id: int) -> None:
    with paths.fact_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
    if not fieldnames:
        raise RuntimeError("fact CSV has no header")
    group_key = row_id % 100
    dim_key = row_id % max(1, paths.dim_rows)
    value = (row_id * 17) % 10_000
    metric = ((row_id * 13) % 100_000) / 100.0
    flag = 1 if row_id % 7 == 0 else 0
    category = bench.generated_category(row_id, group_key, paths.dataset_profile)
    base = {
        "id": str(row_id),
        "group_key": str(group_key),
        "dim_key": str(dim_key),
        "value": str(value),
        "metric": f"{metric:.2f}",
        "flag": str(flag),
        "category": category,
    }
    extra_columns = tuple(column for column in fieldnames if column not in base)
    extra_values = bench.generated_extra_fact_values(
        row_id,
        group_key,
        dim_key,
        value,
        metric,
        flag,
        category,
        paths.dataset_profile,
        extra_columns,
    )
    row = {column: "" for column in fieldnames}
    row.update(base)
    row.update(dict(zip(extra_columns, extra_values)))
    with paths.fact_csv.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writerow(row)


def append_dim_row(paths: Any, row_id: int) -> None:
    with paths.dim_csv.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([row_id, f"role_repair_dim_{row_id}", "1.0"])


def append_cdc_delta_row(paths: Any, row_id: int) -> None:
    if paths.cdc_delta_csv is None or not paths.cdc_delta_csv.exists():
        raise RuntimeError("CDC delta CSV is required for CDC role-repair evidence")
    with paths.cdc_delta_csv.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["id", "op", "value", "metric", "effective_ts"],
        )
        writer.writerow(
            {
                "id": row_id,
                "op": "insert",
                "value": 10_000 + row_id,
                "metric": f"{row_id / 10.0:.2f}",
                "effective_ts": "2026-06-12T00:00:00Z",
            }
        )


def field_map(row: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    evidence = row.get("shardloom_evidence")
    if isinstance(evidence, dict):
        fields.update(evidence)
    metrics = row.get("metrics")
    if isinstance(metrics, dict):
        fields.update(metrics)
    fields.update(row)
    return fields


def field_text(row: dict[str, Any], key: str) -> str:
    value = field_map(row).get(key)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def validate_case(rows: list[dict[str, Any]], case_id: str, strategy: str, role: str) -> None:
    if not rows:
        raise RuntimeError(f"{case_id} produced no benchmark rows")
    for row in rows:
        if row.get("status") != "success":
            raise RuntimeError(f"{case_id} row did not succeed: {row.get('status')}")
        if row.get("engine") != "shardloom-prepare-batch":
            raise RuntimeError(f"{case_id} row used unexpected engine: {row.get('engine')}")
        actual_strategy = field_text(
            row,
            "prepare_batch_prepared_state_optimization_strategy",
        )
        if actual_strategy != strategy:
            raise RuntimeError(
                f"{case_id} expected strategy {strategy}, got {actual_strategy or 'missing'}"
            )
        if strategy == "role_scoped_repair":
            repaired = field_text(
                row,
                "prepare_batch_prepared_state_partial_repair_repaired_roles",
            )
            if role not in {part.strip() for part in repaired.split(",")}:
                raise RuntimeError(f"{case_id} did not repair expected role {role}")
            for key in (
                "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros",
                "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros",
                "prepare_batch_prepared_state_partial_repair_vortex_write_micros",
                "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros",
            ):
                if field_text(row, key) == "":
                    raise RuntimeError(f"{case_id} omitted {key}")
        for key in (
            "fallback_attempted",
            "external_engine_invoked",
            "prepare_batch_prepared_state_optimization_fallback_attempted",
            "prepare_batch_prepared_state_optimization_external_engine_invoked",
        ):
            if field_text(row, key).lower() != "false":
                raise RuntimeError(f"{case_id} reported {key}!={False}")


def evidence_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = field_map(row)
    compact = {
        key: fields[key]
        for key in EVIDENCE_TOP_LEVEL_KEYS
        if key in fields
    }
    compact["metrics"] = {
        key: fields[key]
        for key in EVIDENCE_METRIC_KEYS
        if key in fields
    }
    return compact


def run_case(
    bench: Any,
    runner: Any,
    paths: Any,
    *,
    case_id: str,
    expected_strategy: str,
    expected_repaired_role: str,
    cache_mode: str,
) -> dict[str, Any]:
    rows = bench.run_batch(runner, paths, SCENARIOS, "csv", 1)
    annotated: list[dict[str, Any]] = []
    for row in rows:
        row["prepare_batch_role_repair_evidence_case_id"] = case_id
        row["prepare_batch_role_repair_expected_strategy"] = expected_strategy
        row["prepare_batch_role_repair_expected_repaired_role"] = expected_repaired_role
        bench.annotate_result(row, cache_mode, paths.dataset_profile)
        annotated.append(evidence_row(row))
    validate_case(annotated, case_id, expected_strategy, expected_repaired_role)
    return {
        "case_id": case_id,
        "expected_strategy": expected_strategy,
        "expected_repaired_role": expected_repaired_role,
        "scenario_order": list(SCENARIOS),
        "row_count": len(annotated),
        "rows": annotated,
    }


def generate(args: argparse.Namespace) -> dict[str, Any]:
    bench = load_benchmark_runner()
    bench.SHARDLOOM_BUILD_PROFILE = args.shardloom_build_profile
    bench.SHARDLOOM_RESULT_SINK = False
    bench.SHARDLOOM_EVIDENCE_LEVEL = args.shardloom_evidence_level
    bench.SHARDLOOM_EVIDENCE_TIER = args.shardloom_evidence_tier
    if not args.reuse_data_dir and args.data_dir.exists():
        shutil.rmtree(args.data_dir)
    paths = bench.ensure_dataset(
        args.data_dir,
        args.rows,
        args.dim_rows,
        True,
        ("csv",),
        "tiny_smoke",
    )
    runners, missing = bench.available_runners(("shardloom-prepare-batch",))
    if "shardloom-prepare-batch" in missing:
        raise RuntimeError(missing["shardloom-prepare-batch"])
    runner = bench.warmup_runner(runners["shardloom-prepare-batch"])

    runs: list[dict[str, Any]] = []
    runs.append(
        run_case(
            bench,
            runner,
            paths,
            case_id="full_prepare_register",
            expected_strategy="full_prepare_register",
            expected_repaired_role="all_prepared_artifacts_created",
            cache_mode=args.cache_mode,
        )
    )
    runs.append(
        run_case(
            bench,
            runner,
            paths,
            case_id="manifest_reuse",
            expected_strategy="manifest_reuse",
            expected_repaired_role="none",
            cache_mode=args.cache_mode,
        )
    )
    append_fact_row(bench, paths, args.rows + 10_001)
    runs.append(
        run_case(
            bench,
            runner,
            paths,
            case_id="fact_role_repair",
            expected_strategy="role_scoped_repair",
            expected_repaired_role="fact_input",
            cache_mode=args.cache_mode,
        )
    )
    append_dim_row(paths, args.dim_rows + 20_001)
    runs.append(
        run_case(
            bench,
            runner,
            paths,
            case_id="dim_role_repair",
            expected_strategy="role_scoped_repair",
            expected_repaired_role="dim_input",
            cache_mode=args.cache_mode,
        )
    )
    append_cdc_delta_row(paths, args.rows + 30_001)
    runs.append(
        run_case(
            bench,
            runner,
            paths,
            case_id="cdc_delta_role_repair",
            expected_strategy="role_scoped_repair",
            expected_repaired_role="cdc_delta_input",
            cache_mode=args.cache_mode,
        )
    )

    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_git_sha": git_sha(),
        "generator": repo_relative(Path(__file__)),
        "benchmark_runner": repo_relative(BENCHMARK_RUNNER),
        "data_dir": repo_relative(args.data_dir),
        "dataset_profile": "tiny_smoke",
        "rows": args.rows,
        "dim_rows": args.dim_rows,
        "input_format": "csv",
        "engine": "shardloom-prepare-batch",
        "scenario_order": list(SCENARIOS),
        "case_order": [run["case_id"] for run in runs],
        "role_repair_required_roles": ["fact_input", "dim_input", "cdc_delta_input"],
        "role_repair_observed_roles": [
            "fact_input",
            "dim_input",
            "cdc_delta_input",
        ],
        "run_count": len(runs),
        "row_count": sum(int(run["row_count"]) for run in runs),
        "runs": runs,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "performance_claim_allowed": False,
        "claim_boundary": (
            "Targeted prepare-batch role-repair evidence only. Rows prove real "
            "workspace manifest reuse and role-scoped repair over an isolated local "
            "CSV tiny-smoke dataset; they do not refresh the full public benchmark, "
            "authorize performance claims, or claim broad production readiness."
        ),
    }


def write_outputs(payload: dict[str, Any], output: Path, public_output: Path | None) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")
    if public_output is not None:
        public_output.parent.mkdir(parents=True, exist_ok=True)
        public_output.write_text(text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--public-output", type=Path, default=DEFAULT_PUBLIC_OUTPUT)
    parser.add_argument("--no-public-output", action="store_true")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--rows", type=int, default=96)
    parser.add_argument("--dim-rows", type=int, default=24)
    parser.add_argument(
        "--reuse-data-dir",
        action="store_true",
        help="Reuse an existing isolated data dir instead of regenerating it.",
    )
    parser.add_argument(
        "--shardloom-build-profile",
        default="release",
        choices=("debug", "release", "release-lto", "release-pgo", "release-native-benchmark"),
    )
    parser.add_argument(
        "--shardloom-evidence-level",
        default="certified",
        choices=("minimal_runtime", "certified", "full_replay"),
    )
    parser.add_argument(
        "--shardloom-evidence-tier",
        default="metadata_sink",
        choices=(
            "runtime_minimal",
            "metadata_sink",
            "full_vortex_replay",
            "publication_full",
        ),
    )
    parser.add_argument("--cache-mode", default="warm-ish-local-filesystem")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = generate(args)
    public_output = None if args.no_public_output else args.public_output
    write_outputs(payload, args.output, public_output)
    print(args.output)
    if public_output is not None:
        print(public_output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
