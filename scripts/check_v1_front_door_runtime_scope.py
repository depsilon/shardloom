#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the v1 front-door runtime scope contract."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_report_utils import (
    fail_closed_fields,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_front_door_runtime_scope_report.v1"
DOC_PATH = Path("docs/architecture/v1-front-door-runtime-scope.md")
SCENARIO_SUPPORT_PATH = Path("examples/local-python-benchmark-scenarios/scenario_support.py")

SUPPORTED_PARITY_ROWS = {
    "local_file_filter_project_limit",
    "local_file_join_aggregate_sort_window",
    "generated_source_output",
    "schema_quality_preview",
    "local_vortex_primitive_runtime",
    "typed_nested_compatibility_sink",
    "native_vortex_general_runtime",
    "decoded_materialization_interop",
    "arbitrary_sql_python_dataframe_breadth",
    "performance_equivalence",
}

EXPORT_PENDING_PARITY_ROWS: set[str] = set()

BROAD_PENDING_PARITY_ROWS = {
    "object_store_lakehouse_catalog",
}

EXPECTED_EXAMPLE_SCENARIOS = {
    "selective_filter",
    "filter_projection_limit",
    "group_by_aggregation",
    "hash_join",
    "global_top_n",
    "clean_cast_filter_write",
    "malformed_timestamp_cast",
    "null_heavy_aggregate",
    "nested_json_field_scan",
}

EXPECTED_ERROR_SCENARIOS: set[str] = set()

DOC_MARKERS = (
    "shardloom.v1_front_door_runtime_scope.v1",
    "ShardLoomContext.front_door_parity_matrix()",
    "ShardLoomContext.front_door_semantic_surface_matrix()",
    "ShardLoomContext.user_route_capability_report()",
    "examples/local-python-benchmark-scenarios/run.py",
    "selective_filter",
    "malformed_timestamp_cast",
    "runtime_execution=false",
    "data_read=false",
    "write_io=false",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "Dynamic admission",
    "metadata-first",
    "Capillary work units",
    "PulseWeave-style controls",
    "timing_surface",
    "claim_gate_status",
    "broad SQL/DataFrame parity",
    "front-door performance equivalence",
)

REQUIRED_SEMANTIC_ROWS = {
    "dataframe_construction_read_apis",
    "dataframe_selection_projection",
    "dataframe_filtering",
    "dataframe_type_system",
    "dataframe_casts_coercion",
    "dataframe_missing_data",
    "dataframe_aggregation",
    "dataframe_joins",
    "dataframe_ordering_window",
    "dataframe_reshaping",
    "dataframe_materialization",
    "dataframe_index_semantics",
    "dataframe_expression_callable_apis",
    "dataframe_determinism",
    "dataframe_errors_blockers",
    "dataframe_fallback_boundary",
    "sql_parser_grammar_scope",
    "sql_binder_name_resolution",
    "sql_type_system",
    "sql_casts_coercion",
    "sql_null_semantics",
    "sql_relational_semantics",
    "sql_operator_semantics",
    "sql_aggregates",
    "sql_joins",
    "sql_subqueries",
    "sql_windows",
    "sql_ordering_collation",
    "sql_errors_edge_cases",
    "sql_fallback_boundary",
    "shared_claim_vocabulary",
}

PUBLIC_DOC_MARKERS = {
    "README.md": (
        DOC_PATH.as_posix(),
        "examples/local-python-benchmark-scenarios/run.py",
        "fallback_attempted",
        "external_engine_invoked",
    ),
    "python/README.md": (
        DOC_PATH.as_posix(),
        "front_door_parity_matrix",
        "front_door_semantic_surface_matrix",
        "performance_equivalence_claim_allowed",
    ),
    "docs/release/public-status-matrix.md": (
        DOC_PATH.as_posix(),
        "Scoped local CSV, JSON/JSONL/NDJSON, generated rows, local Vortex",
    ),
    "website-src/src/pages/benchmarks.astro": (
        "ClickBench",
        "No local leaderboard",
        "public comparison surface",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-front-door-runtime-scope-report.json"),
    )
    return parser.parse_args()


def load_context_reports(repo_root: Path) -> tuple[Any, Any, Any]:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    ctx = ShardLoomContext(client=None)
    return (
        ctx.front_door_parity_matrix(),
        ctx.front_door_semantic_surface_matrix(),
        ctx.user_route_capability_report(),
    )


def load_scenario_support(repo_root: Path) -> Any:
    path = repo_root / SCENARIO_SUPPORT_PATH
    spec = importlib.util.spec_from_file_location(
        "shardloom_v1_front_door_scenario_support",
        path,
    )
    if spec is None or spec.loader is None:
        raise FileNotFoundError(path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def validate_parity_matrix(matrix: Any) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    rows = [
        {
            "row_id": row.row_id,
            "support_status": row.support_status,
            "runtime_gap_status": row.runtime_gap_status,
            "parity_status": row.parity_status,
            "shared_runtime_path": row.shared_runtime_path,
            "blocker_id": row.blocker_id,
            "fallback_attempted": row.fallback_attempted,
            "external_engine_invoked": row.external_engine_invoked,
            "claim_boundary": row.claim_boundary,
        }
        for row in matrix.rows
    ]
    by_id = {str(row["row_id"]): row for row in rows}

    missing_supported = sorted(SUPPORTED_PARITY_ROWS - by_id.keys())
    if missing_supported:
        blockers.append("v1 parity matrix missing supported rows: " + ",".join(missing_supported))
    missing_pending = sorted(BROAD_PENDING_PARITY_ROWS - by_id.keys())
    if missing_pending:
        blockers.append("v1 parity matrix missing pending rows: " + ",".join(missing_pending))
    missing_export_pending = sorted(EXPORT_PENDING_PARITY_ROWS - by_id.keys())
    if missing_export_pending:
        blockers.append(
            "v1 parity matrix missing export-pending rows: "
            + ",".join(missing_export_pending)
        )

    for row_id in sorted(SUPPORTED_PARITY_ROWS):
        row = by_id.get(row_id)
        if not row:
            continue
        if row.get("parity_status") != "equivalent_admitted_scope":
            blockers.append(f"{row_id}: v1 row must be equivalent_admitted_scope")
        if row.get("runtime_gap_status") != "admitted_scope":
            blockers.append(f"{row_id}: v1 row must use runtime_gap_status=admitted_scope")
        if row.get("blocker_id") is not None:
            blockers.append(f"{row_id}: v1 row must not carry blocker_id")

    for row_id in sorted(EXPORT_PENDING_PARITY_ROWS):
        row = by_id.get(row_id)
        if not row:
            continue
        if row.get("parity_status") != "deterministic_blocker_until_native_export_contract":
            blockers.append(f"{row_id}: export-pending row must use native export blocker parity")
        if row.get("runtime_gap_status") != "native_compatibility_export_contract_missing":
            blockers.append(
                f"{row_id}: export-pending row must use native compatibility export gap status"
            )
        if not row.get("blocker_id"):
            blockers.append(f"{row_id}: export-pending row must carry blocker_id")

    for row_id in sorted(BROAD_PENDING_PARITY_ROWS):
        row = by_id.get(row_id)
        if not row:
            continue
        if row.get("parity_status") == "equivalent_admitted_scope":
            blockers.append(f"{row_id}: pending row must not be equivalent_admitted_scope")
        if row_id in {
            "object_store_lakehouse_catalog",
            "arbitrary_sql_python_dataframe_breadth",
        }:
            if row.get("parity_status") != "front_door_gap":
                blockers.append(f"{row_id}: broad pending row must remain front_door_gap")
            if not row.get("blocker_id"):
                blockers.append(f"{row_id}: broad pending row must expose blocker_id")
        if str(row.get("runtime_gap_status", "")) in {"unsupported", "blocked", ""}:
            blockers.append(f"{row_id}: pending row must use a precise runtime_gap_status")

    if matrix.scoped_local_front_door_parity_supported is not True:
        blockers.append("scoped_local_front_door_parity_supported must be true")
    if matrix.flexible_anything_claim_allowed is not False:
        blockers.append("flexible_anything_claim_allowed must remain false")
    if matrix.performance_equivalence_claim_allowed is not False:
        blockers.append("performance_equivalence_claim_allowed must remain false")
    if matrix.all_no_fallback_no_external_engine is not True:
        blockers.append("front-door parity rows must preserve no fallback and no external engine")
    return rows, blockers


def validate_semantic_surface(matrix: Any) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    rows = [
        {
            "row_id": row.row_id,
            "surface": row.surface,
            "semantic_family": row.semantic_family,
            "fallback_attempted": row.fallback_attempted,
            "external_engine_invoked": row.external_engine_invoked,
            "deterministic_blockers": row.deterministic_blockers,
            "claim_boundary": row.claim_boundary,
        }
        for row in matrix.rows
    ]
    row_ids = {str(row["row_id"]) for row in rows}
    missing = sorted(REQUIRED_SEMANTIC_ROWS - row_ids)
    if missing:
        blockers.append("front-door semantic matrix missing rows: " + ",".join(missing))
    if matrix.pandas_compatible_claim_allowed is not False:
        blockers.append("pandas compatibility label must not be claimable")
    if matrix.polars_compatible_claim_allowed is not False:
        blockers.append("Polars compatibility label must not be claimable")
    if matrix.broad_dataframe_compatible_claim_allowed is not False:
        blockers.append("broad DataFrame compatibility label must not be claimable")
    if matrix.ansi_sql_compliant_claim_allowed is not False:
        blockers.append("broad SQL-standard/ANSI-style compliance label must not be claimable")
    if matrix.all_no_fallback_no_external_engine is not True:
        blockers.append("semantic rows must preserve no fallback and no external engine")
    if matrix.all_deterministic_blockers is not True:
        blockers.append("semantic rows must require deterministic blockers")
    return rows, blockers


def validate_route_report(report: Any) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    public_rows = [
        {
            "front_door_id": row.front_door_id,
            "route_runtime_status": row.route_runtime_status,
            "fallback_attempted": row.fallback_attempted,
            "external_engine_invoked": row.external_engine_invoked,
            "public_user_surface": row.public_user_surface,
            "claim_boundary": row.claim_boundary,
        }
        for row in report.public_front_door_route_rows
    ]
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("user route report must preserve no fallback and no external engine")
    if report.unsupported_local_benchmark_route_ids:
        blockers.append(
            "user route report must not contain unsupported local benchmark routes: "
            + ",".join(report.unsupported_local_benchmark_route_ids)
        )
    if len(public_rows) < 2:
        blockers.append("public front-door route report must expose prepared route rows")
    for row in public_rows:
        front_door_id = str(row["front_door_id"])
        if row.get("route_runtime_status") != "global_runtime_supported":
            blockers.append(f"{front_door_id}: public front door must be runtime supported")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{front_door_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{front_door_id}: external_engine_invoked must be false")
        surface = str(row.get("public_user_surface", ""))
        if "prepare_vortex" not in surface:
            blockers.append(f"{front_door_id}: public surface must name prepare_vortex")
    return public_rows, blockers


def validate_scenario_contract(module: Any, repo_root: Path) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    expected_errors = set(getattr(module, "EXPECTED_ERROR_SCENARIOS", set()))
    if expected_errors != EXPECTED_ERROR_SCENARIOS:
        blockers.append("expected-error scenario set mismatch")

    class DummyContext:
        def read_csv(self, path: str, schema: dict[str, str]) -> str:
            return f"csv:{path}:{','.join(schema)}"

        def read_json(self, path: str, schema: dict[str, str]) -> str:
            return f"json:{path}:{','.join(schema)}"

    class DummySl:
        @staticmethod
        def col(name: str) -> str:
            return f"col:{name}"

    # Scenario names are collected without running the actions. This keeps the validator
    # side-effect-free while the dedicated example tests exercise execution through a fake CLI.
    actions = module.scenario_actions(DummyContext(), DummySl())
    names = [name for name, _ in actions]
    name_set = set(names)
    if name_set != EXPECTED_EXAMPLE_SCENARIOS:
        blockers.append(
            "v1 example scenario ids mismatch: "
            + json.dumps({"expected": sorted(EXPECTED_EXAMPLE_SCENARIOS), "actual": names})
        )
    if len(names) != len(name_set):
        blockers.append("v1 example scenario ids must be unique")

    text = read_text(repo_root / SCENARIO_SUPPORT_PATH)
    for token in [
        "profile_order: Sequence[str] = (\"release\", \"debug\")",
        "fallback_attempted",
        "external_engine_invoked",
        "timing_components",
        "python_wall_millis",
    ]:
        if token not in text:
            blockers.append(f"{SCENARIO_SUPPORT_PATH}: missing marker {token!r}")
    return names, blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    blockers: list[str] = []
    checked_docs: list[str] = []

    blockers.extend(
        require_markers(
            DOC_PATH.as_posix(),
            read_text(resolve_path(repo_root, DOC_PATH)),
            DOC_MARKERS,
        )
    )
    checked_docs.append(DOC_PATH.as_posix())

    for rel_path, markers in PUBLIC_DOC_MARKERS.items():
        blockers.extend(
            require_markers(rel_path, read_text(resolve_path(repo_root, rel_path)), markers)
        )
        checked_docs.append(rel_path)

    parity, semantic_surface, route_report = load_context_reports(repo_root)
    parity_rows, parity_blockers = validate_parity_matrix(parity)
    semantic_rows, semantic_blockers = validate_semantic_surface(semantic_surface)
    route_rows, route_blockers = validate_route_report(route_report)
    blockers.extend(parity_blockers)
    blockers.extend(semantic_blockers)
    blockers.extend(route_blockers)

    scenario_module = load_scenario_support(repo_root)
    scenario_names, scenario_blockers = validate_scenario_contract(scenario_module, repo_root)
    blockers.extend(scenario_blockers)

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "scope_doc": DOC_PATH.as_posix(),
        "checked_docs": checked_docs,
        "checked_doc_count": len(checked_docs),
        "supported_parity_row_ids": sorted(SUPPORTED_PARITY_ROWS),
        "broad_pending_parity_row_ids": sorted(BROAD_PENDING_PARITY_ROWS),
        "parity_rows": parity_rows,
        "front_door_semantic_surface_schema_version": semantic_surface.schema_version,
        "semantic_surface_rows": semantic_rows,
        "semantic_surface_row_ids": list(semantic_surface.row_order),
        "dataframe_claim_statement": semantic_surface.dataframe_claim_statement,
        "dataframe_subset_claim_statement": (
            semantic_surface.dataframe_subset_claim_statement
        ),
        "sql_claim_statement": semantic_surface.sql_claim_statement,
        "pandas_compatible_claim_allowed": (
            semantic_surface.pandas_compatible_claim_allowed
        ),
        "polars_compatible_claim_allowed": (
            semantic_surface.polars_compatible_claim_allowed
        ),
        "broad_dataframe_compatible_claim_allowed": (
            semantic_surface.broad_dataframe_compatible_claim_allowed
        ),
        "ansi_sql_compliant_claim_allowed": (
            semantic_surface.ansi_sql_compliant_claim_allowed
        ),
        "public_front_door_rows": route_rows,
        "example_scenario_ids": scenario_names,
        "expected_error_scenario_ids": sorted(EXPECTED_ERROR_SCENARIOS),
        "scoped_local_front_door_parity_supported": (
            parity.scoped_local_front_door_parity_supported
        ),
        "flexible_anything_claim_allowed": parity.flexible_anything_claim_allowed,
        "performance_equivalence_claim_allowed": parity.performance_equivalence_claim_allowed,
        "all_no_fallback_no_external_engine": (
            parity.all_no_fallback_no_external_engine
            and semantic_surface.all_no_fallback_no_external_engine
            and route_report.all_no_fallback_no_external_engine
        ),
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve_path(repo_root, args.output)
    report = build_report(repo_root)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
