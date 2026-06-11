#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate SQL/Python/DataFrame front-door parity posture.

This gate does not claim broad parity. It fails when the repo cannot clearly
say which front-door workflows share a Vortex-normalized ShardLoom-native
runtime path and which ones still block the user's "build anything, same
behavior/performance" goal.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.sql_python_dataframe_parity_gate.v1"
GATE_ID = "gar-runtime-impl-6c.sql_python_dataframe_parity"

REQUIRED_ROWS = {
    "local_file_filter_project_limit",
    "local_file_join_aggregate_sort_window",
    "generated_source_output",
    "schema_quality_preview",
    "local_vortex_primitive_runtime",
    "typed_nested_compatibility_sink",
    "native_vortex_general_runtime",
    "decoded_materialization_interop",
    "object_store_lakehouse_catalog",
    "arbitrary_sql_python_dataframe_breadth",
    "performance_equivalence",
}

REQUIRED_ADMITTED_ROWS = {
    "local_file_filter_project_limit",
    "local_file_join_aggregate_sort_window",
    "generated_source_output",
    "schema_quality_preview",
    "local_vortex_primitive_runtime",
    "typed_nested_compatibility_sink",
    "decoded_materialization_interop",
}

REQUIRED_GAP_ROWS = REQUIRED_ROWS - REQUIRED_ADMITTED_ROWS
ADMITTED_RUNTIME_GAP_STATUS = "admitted_scope"
PRECISE_RUNTIME_GAP_STATUSES = {
    "front_door_connection_pending",
    "output_route_pending",
    "claim_evidence_pending",
    "benchmark_publication_pending",
    "runtime_expansion_pending",
}
GENERIC_GAP_TERMS = {"unsupported", "blocked", "not complete", "not_complete"}

REQUIRED_SOURCE_MARKERS = {
    "python/src/shardloom/context.py": [
        "class FrontDoorParityMatrix",
        "FRONT_DOOR_PARITY_ROWS",
        "def front_door_parity_matrix(",
        "typed_nested_compatibility_sink",
        "arbitrary_sql_python_dataframe_breadth",
        "performance_equivalence",
    ],
    "python/src/shardloom/query.py": [
        "class SqlWorkflow",
        "class LazyFrame",
        "def collect(",
        "def write_vortex(",
        "def to_pandas(",
        "def sql(",
        "def any_source(",
        "def all_source(",
        "def outer(",
        "def intersect(",
        "def except_rows(",
        "def unhex(",
        "def from_base64(",
        "def is_distinct_from(",
        "def is_not_distinct_from(",
        "def query(",
        "def dropna(",
        "def astype(",
        "def nlargest(",
        "def nsmallest(",
        "decimal128(p,s)",
        "group_by: object | None = None",
        "having: object | None = None",
        "source_alias: object | None = None",
        "def _normalize_sort_nulls(",
        "def _sql_local_subquery_source(",
        "def _vortex_sql_primitive_shape(",
    ],
    "python/src/shardloom/client.py": [
        "def _split_field_list(",
        "def decimal_cast_runtime_execution(",
        "def decimal_cast_target_dtypes(",
        "def decimal_cast_output_boundary(",
        "def decimal_cast_typed_decimal_sink_formats(",
        "def decimal_cast_blocked_typed_decimal_sink_formats(",
        "def complex_projection_output_boundary(",
        "def complex_projection_typed_nested_sink_formats(",
        "def complex_projection_blocked_typed_nested_sink_formats(",
        "def typed_nested_child_schema_evidence_status(",
        "def typed_nested_child_schema_blocker(",
        "def unsupported_reasons(",
        "def sql_set_operation_runtime_execution(",
        "def source_qualified_subquery_runtime_execution(",
        "def source_qualified_subquery_source_qualifiers(",
    ],
    "python/tests/test_query_builder.py": [
        "test_local_csv_query_builder_join_invokes_sql_smoke",
        "test_local_csv_query_builder_predicate_or_join_condition_invokes_sql_smoke",
        "test_local_csv_query_builder_group_by_aggregate_invokes_sql_smoke",
        "test_local_csv_query_builder_distinct_projection_invokes_sql_smoke",
        "test_local_csv_query_builder_union_all_invokes_sql_smoke",
        "test_local_csv_query_builder_intersect_invokes_sql_smoke",
        "test_local_csv_query_builder_except_invokes_sql_smoke",
        "test_local_csv_query_builder_distinct_aggregate_having_invokes_sql_smoke",
        "test_local_csv_query_builder_distinct_join_invokes_sql_smoke",
        "test_local_csv_query_builder_distinct_window_invokes_sql_smoke",
        "test_local_csv_query_builder_window_rank_dense_rank_invokes_sql_smoke",
        "test_local_csv_query_builder_quantified_subquery_filter_invokes_sql_smoke",
        "test_local_csv_query_builder_correlated_subquery_invokes_sql_smoke",
        "test_context_sql_source_qualified_exists_subquery_exposes_report_fields",
        "test_local_csv_query_builder_outer_correlation_unsupported_diagnostics_passthrough",
        "test_local_csv_query_builder_grouped_projected_subquery_helpers_invoke_sql_smoke",
        "test_local_csv_query_builder_with_column_cast_invokes_sql_smoke",
        "cast_projection_target_dtype",
        "test_local_csv_query_builder_with_decimal_cast_invokes_sql_smoke",
        "decimal_cast_output_boundary",
        "decimal_cast_typed_decimal_sink_formats",
        "decimal_cast_blocked_typed_decimal_sink_formats",
        "test_local_csv_query_builder_write_parquet_exposes_typed_nested_sink_boundary",
        "typed_nested_compatibility_sink_with_result_jsonl_evidence",
        "test_local_csv_query_builder_binary_helper_projection_invokes_sql_smoke",
        "binary_helper_projection_runtime_execution",
        "binary_helper_projection_operator",
        "test_schema_declared_dataframe_query_dropna_astype_lowers_to_sql_smoke",
        "test_local_csv_query_builder_top_n_dataframe_aliases_lower_to_sort_limit",
        "workflow.duplicated(subset=[\"id\"])",
        "workflow.mask(\"amount < 0\", other=0)",
        "workflow.set_index(\"id\")",
        "projected_subquery_group_by_runtime_execution",
        "projected_subquery_having_runtime_execution",
        "source_alias=\"allowed\"",
        "test_context_sql_schema_quality_helpers_invoke_sql_smoke",
        "test_sql_vortex_collect_uses_local_filter_project_primitive",
        "test_local_csv_query_builder_decoded_materialization_helpers",
        "test_materialized_input_boundaries_create_generated_rows",
        "test_context_sql_source_free_write_invokes_generated_source_sql_smoke",
        "test_local_csv_query_builder_order_by_explicit_nulls_invokes_sql_smoke",
    ],
    "shardloom-cli/src/sql_local_source_runtime.rs": [
        "fn validate_sql_cte_policy_boundary(",
        "fn parse_null_safe_comparison_predicate(",
        "fn parse_sort_null_ordering(",
        "NULLS FIRST",
        "SQL common table expressions (WITH/RECURSIVE) are not admitted",
        "fn parser_blocks_common_table_expressions_without_fallback(",
        "fn contains_all_null_complex_dtype_without_child_schema(",
        "typed_complex_child_schema_not_admitted",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/sql-python-dataframe-parity-gate.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def missing_marker_blockers(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    for rel_path, markers in REQUIRED_SOURCE_MARKERS.items():
        text = read_text(repo_root / rel_path)
        if not text:
            blockers.append(f"missing required file: {rel_path}")
            continue
        for marker in markers:
            if marker not in text:
                blockers.append(f"{rel_path} missing marker: {marker}")
    return blockers


def load_matrix(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).front_door_parity_matrix()


def row_payload(row: Any) -> dict[str, Any]:
    return {
        "row_id": row.row_id,
        "workflow": row.workflow,
        "support_status": row.support_status,
        "runtime_gap_status": row.runtime_gap_status,
        "sql_surface": row.sql_surface,
        "python_surface": row.python_surface,
        "dataframe_surface": row.dataframe_surface,
        "shared_runtime_path": row.shared_runtime_path,
        "parity_status": row.parity_status,
        "performance_equivalence_status": row.performance_equivalence_status,
        "runtime_execution": row.runtime_execution,
        "data_read": row.data_read,
        "write_io": row.write_io,
        "materialization_required": row.materialization_required,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "blocker_id": row.blocker_id,
        "required_evidence": list(row.required_evidence),
        "claim_boundary": row.claim_boundary,
    }


def validate_matrix(matrix: Any) -> tuple[list[dict[str, Any]], list[str]]:
    rows = [row_payload(row) for row in matrix.rows]
    blockers: list[str] = []
    by_id = {str(row["row_id"]): row for row in rows}

    missing = sorted(REQUIRED_ROWS - by_id.keys())
    if missing:
        blockers.append("front-door parity matrix missing rows: " + ",".join(missing))

    extra = sorted(by_id.keys() - REQUIRED_ROWS)
    if extra:
        blockers.append("front-door parity matrix has unclassified extra rows: " + ",".join(extra))

    for row in rows:
        row_id = str(row["row_id"])
        if row["fallback_attempted"] is not False:
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if row["external_engine_invoked"] is not False:
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if not str(row["claim_boundary"]).strip():
            blockers.append(f"{row_id}: claim_boundary is required")
        if not row["required_evidence"]:
            blockers.append(f"{row_id}: required_evidence is required")
        if row_id in REQUIRED_ADMITTED_ROWS:
            if row["parity_status"] != "equivalent_admitted_scope":
                blockers.append(f"{row_id}: admitted row must be equivalent_admitted_scope")
            if row["runtime_gap_status"] != ADMITTED_RUNTIME_GAP_STATUS:
                blockers.append(f"{row_id}: admitted row must use runtime_gap_status=admitted_scope")
            if row["runtime_execution"] is not True:
                blockers.append(f"{row_id}: admitted row must have runtime_execution=true")
            if "no_benchmark_claim" not in str(row["performance_equivalence_status"]):
                blockers.append(f"{row_id}: admitted row must avoid benchmarked performance claim")
            if row["blocker_id"] is not None:
                blockers.append(f"{row_id}: admitted scoped row must not carry blocker_id")
        if row_id in REQUIRED_GAP_ROWS:
            if row["parity_status"] != "front_door_gap":
                blockers.append(f"{row_id}: gap row must be front_door_gap")
            runtime_gap_status = str(row["runtime_gap_status"])
            if runtime_gap_status not in PRECISE_RUNTIME_GAP_STATUSES:
                blockers.append(
                    f"{row_id}: gap row must use a precise runtime_gap_status, got "
                    f"{runtime_gap_status!r}"
                )
            if runtime_gap_status.lower() in GENERIC_GAP_TERMS:
                blockers.append(f"{row_id}: gap row must not use generic runtime_gap_status")
            support_status = str(row["support_status"]).lower()
            if support_status in GENERIC_GAP_TERMS:
                blockers.append(f"{row_id}: gap support_status must be a concrete pending label")
            shared_runtime_path = str(row["shared_runtime_path"]).lower()
            if shared_runtime_path.strip() in GENERIC_GAP_TERMS:
                blockers.append(f"{row_id}: shared_runtime_path must not be generic unsupported prose")
            if not row["blocker_id"]:
                blockers.append(f"{row_id}: gap row must name blocker_id")

    if matrix.flexible_anything_claim_allowed is not False:
        blockers.append("flexible_anything_claim_allowed must remain false")
    if matrix.performance_equivalence_claim_allowed is not False:
        blockers.append("performance_equivalence_claim_allowed must remain false")
    if matrix.scoped_local_front_door_parity_supported is not True:
        blockers.append("scoped_local_front_door_parity_supported must be true")
    if matrix.all_no_fallback_no_external_engine is not True:
        blockers.append("all_no_fallback_no_external_engine must be true")
    if matrix.all_broad_gaps_have_precise_runtime_status is not True:
        blockers.append("all_broad_gaps_have_precise_runtime_status must be true")

    return rows, blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    matrix = load_matrix(repo_root)
    rows, matrix_blockers = validate_matrix(matrix)
    marker_blockers = missing_marker_blockers(repo_root)
    blockers = [*matrix_blockers, *marker_blockers]
    remaining_gaps = [
        row
        for row in rows
        if row["parity_status"] == "front_door_gap" or row["blocker_id"]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "claim_gate_status": "not_claim_grade",
        "covered_phase_items": ["GAR-RUNTIME-IMPL-6C", "CG-20", "CG-21", "CG-22"],
        "scoped_local_front_door_parity_supported": matrix.scoped_local_front_door_parity_supported,
        "flexible_anything_claim_allowed": matrix.flexible_anything_claim_allowed,
        "performance_equivalence_claim_allowed": matrix.performance_equivalence_claim_allowed,
        "all_no_fallback_no_external_engine": matrix.all_no_fallback_no_external_engine,
        "row_count": len(rows),
        "runtime_gap_status_vocabulary": [
            ADMITTED_RUNTIME_GAP_STATUS,
            *sorted(PRECISE_RUNTIME_GAP_STATUSES),
        ],
        "runtime_gap_status_counts": dict(matrix.runtime_gap_status_counts),
        "admitted_row_count": len(
            [row for row in rows if row["parity_status"] == "equivalent_admitted_scope"]
        ),
        "remaining_gap_count": len(remaining_gaps),
        "remaining_gap_row_ids": [str(row["row_id"]) for row in remaining_gaps],
        "rows": rows,
        "all_broad_gaps_have_precise_runtime_status": matrix.all_broad_gaps_have_precise_runtime_status,
        "vortex_normalization_contract": (
            "User inputs are front doors into ShardLoom's Vortex-backed runtime path: native "
            ".vortex sources start at the Vortex boundary, while compatibility files, generated "
            "rows, and materialized Python/Arrow snapshots must expose their adapter-to-Vortex "
            "normalization or preparation boundary before broad runtime-ready claims."
        ),
        "claim_boundary": (
            "ShardLoom has scoped local SQL/Python/DataFrame parity where rows declare "
            "equivalent_admitted_scope and a shared ShardLoom runtime path. Broad arbitrary "
            "SQL/Python/DataFrame flexibility and performance equivalence remain not-claim-grade "
            "until the GAR-RUNTIME-IMPL-6D runtime-expansion checklist rows are closed with "
            "explicit Vortex-normalization, correctness, execution-certificate, native-I/O, and "
            "benchmark evidence."
        ),
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"front-door parity blocker: {blocker}")
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
