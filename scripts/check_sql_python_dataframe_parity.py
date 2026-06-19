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
from types import SimpleNamespace
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
    "native_vortex_general_runtime",
    "decoded_materialization_interop",
}

REQUIRED_EXPORT_PENDING_ROWS: set[str] = set()

REQUIRED_GAP_ROWS = REQUIRED_ROWS - REQUIRED_ADMITTED_ROWS - REQUIRED_EXPORT_PENDING_ROWS
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
        "class FrontDoorSemanticSurfaceMatrix",
        "FRONT_DOOR_PARITY_ROWS",
        "FRONT_DOOR_SEMANTIC_SURFACE_ROWS",
        "def front_door_parity_matrix(",
        "def front_door_semantic_surface_matrix(",
        "typed_nested_compatibility_sink",
        "arbitrary_sql_python_dataframe_breadth",
        "performance_equivalence",
        "pandas_compatible_claim_allowed",
        "ansi_sql_compliant_claim_allowed",
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
        "test_local_csv_query_builder_binary_byte_length_invokes_sql_smoke",
        "binary_byte_length_projection_runtime_execution",
        "binary_byte_length_predicate_runtime_execution",
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
    "shardloom-cli/src/workflow_planning.rs": [
        "cg21.workflow.sample.weighted_or_rng_contract_missing",
        "cg21.workflow.explode.nested_expansion_unsupported",
        "cg21.workflow.pivot.broad_reshape_contract_missing",
        "cg21.workflow.pivot_table.broad_aggregate_reshape_contract_missing",
        "cg21.workflow.melt.reshape_semantics_unsupported",
        "cg21.workflow.rolling.broad_window_semantics_unsupported",
        "cg21.workflow.duplicated.nullable_nested_or_index_contract_missing",
        "cg21.workflow.drop_duplicates.subset_keep_or_null_equality_contract_missing",
        "cg21.workflow.mask.null_callable_or_alignment_contract_missing",
        "cg21.workflow.replace.null_regex_method_or_mixed_dtype_contract_missing",
        "cg21.workflow.set_index.hidden_index_materialization_contract_missing",
        "cg21.workflow.reset_index.row_number_or_hidden_index_contract_missing",
        "cg21.workflow.sort_index.hidden_index_order_contract_missing",
        "cg21.workflow.apply.python_callable_unsupported",
        "cg21.workflow.pipe.python_callable_unsupported",
        "cg21.workflow.transform.python_callable_unsupported",
        "cg21.workflow.applymap.python_callable_unsupported",
        "cg21.workflow.map.python_callable_unsupported",
        "cg21.workflow.map_rows.python_callable_or_row_udf_unsupported",
        "cg21.workflow.eval.expression_engine_unsupported",
        "cg21.workflow.fanout.multi_sink_atomicity_contract_missing",
    ],
    "shardloom-cli/src/status_capabilities.rs": [
        "cg21.workflow.sample.weighted_or_rng_contract_missing",
        "cg21.workflow.explode.nested_expansion_unsupported",
        "cg21.workflow.pivot.broad_reshape_contract_missing",
        "cg21.workflow.pivot_table.broad_aggregate_reshape_contract_missing",
        "cg21.workflow.melt.reshape_semantics_unsupported",
        "cg21.workflow.rolling.broad_window_semantics_unsupported",
        "cg21.workflow.duplicated.nullable_nested_or_index_contract_missing",
        "cg21.workflow.drop_duplicates.subset_keep_or_null_equality_contract_missing",
        "cg21.workflow.mask.null_callable_or_alignment_contract_missing",
        "cg21.workflow.replace.null_regex_method_or_mixed_dtype_contract_missing",
        "cg21.workflow.set_index.hidden_index_materialization_contract_missing",
        "cg21.workflow.reset_index.row_number_or_hidden_index_contract_missing",
        "cg21.workflow.sort_index.hidden_index_order_contract_missing",
        "cg21.workflow.apply.python_callable_unsupported",
        "cg21.workflow.pipe.python_callable_unsupported",
        "cg21.workflow.transform.python_callable_unsupported",
        "cg21.workflow.applymap.python_callable_unsupported",
        "cg21.workflow.map.python_callable_unsupported",
        "cg21.workflow.map_rows.python_callable_or_row_udf_unsupported",
        "cg21.workflow.eval.expression_engine_unsupported",
        "cg21.workflow.fanout.multi_sink_atomicity_contract_missing",
    ],
}

REQUIRED_DATAFRAME_SEMANTIC_ROW_IDS = {
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
}

REQUIRED_SQL_SEMANTIC_ROW_IDS = {
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
}

REQUIRED_SHARED_SEMANTIC_ROW_IDS = {"shared_claim_vocabulary"}

REQUIRED_DATAFRAME_METHOD_STATUSES = {
    "sample": {"production_admitted_local_workflow"},
    "set_index": {"scoped_runtime_supported"},
    "reset_index": {"scoped_runtime_supported"},
    "sort_index": {"production_admitted_local_workflow"},
    "pivot": {"production_admitted_local_workflow"},
    "pivot_table": {"production_admitted_local_workflow"},
    "melt": {"production_admitted_local_workflow"},
    "explode": {"production_admitted_local_workflow"},
    "rolling": {"production_admitted_local_workflow"},
    "mask": {"production_admitted_local_workflow"},
    "replace": {"production_admitted_local_workflow"},
    "dropna": {"production_admitted_local_workflow"},
    "fillna": {"production_admitted_local_workflow"},
    "fill_null": {"production_admitted_local_workflow"},
    "isna": {"production_admitted_local_workflow"},
    "isnull": {"production_admitted_local_workflow"},
    "notna": {"production_admitted_local_workflow"},
    "notnull": {"production_admitted_local_workflow"},
    "eval": {"production_admitted_local_workflow"},
    "transform": {"production_admitted_local_workflow"},
    "applymap": {"production_admitted_local_workflow"},
    "map": {"production_admitted_local_workflow"},
    "map_rows": {"production_admitted_local_workflow"},
    "apply": {"lazy_plan_supported"},
    "pipe": {"lazy_plan_supported"},
    "write": {"production_admitted_local_workflow"},
    "write_jsonl": {"production_admitted_local_workflow"},
    "write_csv": {"production_admitted_local_workflow"},
    "fanout": {"production_admitted_local_workflow"},
    "distinct": {"production_admitted_local_workflow"},
    "drop_duplicates": {"production_admitted_local_workflow"},
    "unique": {"production_admitted_local_workflow"},
}

PLAN_TRANSFORM_ONLY_METHODS = {"apply", "pipe"}

REQUIRED_DATAFRAME_METHOD_FUTURE_CONTRACT_BLOCKERS = {
    "sample": {"cg21.workflow.sample.weighted_or_rng_contract_missing"},
    "explode": {"cg21.workflow.explode.nested_expansion_unsupported"},
    "pivot": {"cg21.workflow.pivot.broad_reshape_contract_missing"},
    "pivot_table": {"cg21.workflow.pivot_table.broad_aggregate_reshape_contract_missing"},
    "melt": {"cg21.workflow.melt.reshape_semantics_unsupported"},
    "rolling": {"cg21.workflow.rolling.broad_window_semantics_unsupported"},
    "dropna": {"cg21.workflow.dropna.null_cleanup_semantics_contract_missing"},
    "fillna": {"cg21.workflow.fillna.null_fill_semantics_unsupported"},
    "fill_null": {"cg21.workflow.fillna.null_fill_semantics_unsupported"},
    "isna": {"cg21.workflow.isna.null_mask_semantics_unsupported"},
    "isnull": {"cg21.workflow.isna.null_mask_semantics_unsupported"},
    "notna": {"cg21.workflow.notna.null_mask_semantics_unsupported"},
    "notnull": {"cg21.workflow.notna.null_mask_semantics_unsupported"},
    "duplicated": {"cg21.workflow.duplicated.nullable_nested_or_index_contract_missing"},
    "drop_duplicates": {
        "cg21.workflow.drop_duplicates.subset_keep_or_null_equality_contract_missing"
    },
    "unique": {
        "cg21.workflow.drop_duplicates.subset_keep_or_null_equality_contract_missing"
    },
    "mask": {"cg21.workflow.mask.null_callable_or_alignment_contract_missing"},
    "replace": {
        "cg21.workflow.replace.null_regex_method_or_mixed_dtype_contract_missing"
    },
    "set_index": {
        "cg21.workflow.set_index.hidden_index_materialization_contract_missing"
    },
    "reset_index": {
        "cg21.workflow.reset_index.row_number_or_hidden_index_contract_missing"
    },
    "sort_index": {"cg21.workflow.sort_index.hidden_index_order_contract_missing"},
    "apply": {"cg21.workflow.apply.python_callable_unsupported"},
    "pipe": {"cg21.workflow.pipe.python_callable_unsupported"},
    "transform": {"cg21.workflow.transform.python_callable_unsupported"},
    "applymap": {"cg21.workflow.applymap.python_callable_unsupported"},
    "map": {"cg21.workflow.map.python_callable_unsupported"},
    "map_rows": {"cg21.workflow.map_rows.python_callable_or_row_udf_unsupported"},
    "eval": {"cg21.workflow.eval.expression_engine_unsupported"},
    "fanout": {"cg21.workflow.fanout.multi_sink_atomicity_contract_missing"},
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


def load_dataframe_method_matrix(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomBinaryNotFoundError, ShardLoomContext
    from shardloom.context import DATAFRAME_METHOD_CAPABILITY_ROWS

    try:
        return ShardLoomContext(client=None).dataframe_method_matrix()
    except ShardLoomBinaryNotFoundError:
        return SimpleNamespace(rows=DATAFRAME_METHOD_CAPABILITY_ROWS)


def load_semantic_surface_matrix(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).front_door_semantic_surface_matrix()


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


def dataframe_method_payload(row: Any) -> dict[str, Any]:
    return {
        "method": row.method,
        "family": row.family,
        "support_status": row.support_status,
        "runtime_execution": row.runtime_execution,
        "data_read": row.data_read,
        "write_io": row.write_io,
        "materialization_required": row.materialization_required,
        "diagnostic_operation": row.diagnostic_operation,
        "blocker_id": row.blocker_id,
        "future_contract_blocker_ids": list(row.future_contract_blocker_ids),
        "required_evidence": list(row.required_evidence),
        "claim_boundary": row.claim_boundary,
    }


def semantic_surface_payload(row: Any) -> dict[str, Any]:
    return {
        "row_id": row.row_id,
        "surface": row.surface,
        "semantic_family": row.semantic_family,
        "admitted_scope": row.admitted_scope,
        "unsupported_scope": row.unsupported_scope,
        "deterministic_blockers": row.deterministic_blockers,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "required_evidence": list(row.required_evidence),
        "claim_boundary": row.claim_boundary,
    }


def validate_dataframe_method_surface(
    matrix: Any,
) -> tuple[list[dict[str, Any]], dict[str, int], list[str]]:
    rows = [dataframe_method_payload(row) for row in matrix.rows]
    by_method = {str(row["method"]): row for row in rows}
    blockers: list[str] = []
    status_counts: dict[str, int] = {}
    for row in rows:
        status = str(row["support_status"])
        status_counts[status] = status_counts.get(status, 0) + 1
        if row["blocker_id"]:
            blockers.append(f"{row['method']}: method matrix must not carry active blocker_id")

    for method, expected_blockers in REQUIRED_DATAFRAME_METHOD_FUTURE_CONTRACT_BLOCKERS.items():
        row = by_method.get(method)
        if row is None:
            blockers.append(f"dataframe method matrix missing required method {method}")
            continue
        observed = set(row.get("future_contract_blocker_ids") or [])
        missing = expected_blockers - observed
        if missing:
            blockers.append(
                f"{method}: missing future_contract_blocker_ids {sorted(missing)!r}"
            )

    for method, allowed_statuses in REQUIRED_DATAFRAME_METHOD_STATUSES.items():
        row = by_method.get(method)
        if row is None:
            blockers.append(f"dataframe method matrix missing required method {method}")
            continue
        status = str(row["support_status"])
        if status not in allowed_statuses:
            blockers.append(
                f"{method}: support_status {status!r} not in {sorted(allowed_statuses)!r}"
            )
        if method not in PLAN_TRANSFORM_ONLY_METHODS and row["runtime_execution"] is not True:
            blockers.append(f"{method}: feasible method shape must have runtime_execution=true")
        if method in PLAN_TRANSFORM_ONLY_METHODS and row["runtime_execution"] is not False:
            blockers.append(f"{method}: plan-transform method must remain plan-only")
        if not row["required_evidence"]:
            blockers.append(f"{method}: required_evidence is required")
        if not str(row["claim_boundary"]).strip():
            blockers.append(f"{method}: claim_boundary is required")

    return rows, status_counts, blockers


def validate_semantic_surface(matrix: Any) -> tuple[list[dict[str, Any]], list[str]]:
    rows = [semantic_surface_payload(row) for row in matrix.rows]
    blockers: list[str] = []
    by_id = {str(row["row_id"]): row for row in rows}
    required_ids = (
        REQUIRED_DATAFRAME_SEMANTIC_ROW_IDS
        | REQUIRED_SQL_SEMANTIC_ROW_IDS
        | REQUIRED_SHARED_SEMANTIC_ROW_IDS
    )

    missing = sorted(required_ids - by_id.keys())
    if missing:
        blockers.append("semantic surface matrix missing rows: " + ",".join(missing))

    extra = sorted(by_id.keys() - required_ids)
    if extra:
        blockers.append(
            "semantic surface matrix has unclassified extra rows: " + ",".join(extra)
        )

    for row in rows:
        row_id = str(row["row_id"])
        if row["fallback_attempted"] is not False:
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if row["external_engine_invoked"] is not False:
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if row["deterministic_blockers"] is not True:
            blockers.append(f"{row_id}: deterministic_blockers must be true")
        if not str(row["admitted_scope"]).strip():
            blockers.append(f"{row_id}: admitted_scope is required")
        if not str(row["unsupported_scope"]).strip():
            blockers.append(f"{row_id}: unsupported_scope is required")
        if not str(row["claim_boundary"]).strip():
            blockers.append(f"{row_id}: claim_boundary is required")
        if not row["required_evidence"]:
            blockers.append(f"{row_id}: required_evidence is required")
        surface = str(row["surface"])
        if surface not in {"dataframe", "sql", "shared"}:
            blockers.append(f"{row_id}: unknown semantic surface {surface!r}")
        if row_id.startswith("dataframe_") and surface != "dataframe":
            blockers.append(f"{row_id}: dataframe row must use surface=dataframe")
        if row_id.startswith("sql_") and surface != "sql":
            blockers.append(f"{row_id}: sql row must use surface=sql")

    if matrix.pandas_compatible_claim_allowed is not False:
        blockers.append("pandas_compatible_claim_allowed must remain false")
    if matrix.polars_compatible_claim_allowed is not False:
        blockers.append("polars_compatible_claim_allowed must remain false")
    if matrix.broad_dataframe_compatible_claim_allowed is not False:
        blockers.append("broad_dataframe_compatible_claim_allowed must remain false")
    if matrix.ansi_sql_compliant_claim_allowed is not False:
        blockers.append("ansi_sql_compliant_claim_allowed must remain false")
    if matrix.all_no_fallback_no_external_engine is not True:
        blockers.append("semantic all_no_fallback_no_external_engine must be true")
    if matrix.all_deterministic_blockers is not True:
        blockers.append("semantic all_deterministic_blockers must be true")
    if set(matrix.dataframe_row_ids) != REQUIRED_DATAFRAME_SEMANTIC_ROW_IDS:
        blockers.append("dataframe semantic row ids do not match required surface")
    if set(matrix.sql_row_ids) != REQUIRED_SQL_SEMANTIC_ROW_IDS:
        blockers.append("sql semantic row ids do not match required surface")
    if set(matrix.shared_row_ids) != REQUIRED_SHARED_SEMANTIC_ROW_IDS:
        blockers.append("shared semantic row ids do not match required surface")

    return rows, blockers


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
        if row_id in REQUIRED_EXPORT_PENDING_ROWS:
            if row["parity_status"] != "deterministic_blocker_until_native_export_contract":
                blockers.append(
                    f"{row_id}: export-pending row must use native export blocker parity"
                )
            if row["runtime_gap_status"] != "native_compatibility_export_contract_missing":
                blockers.append(
                    f"{row_id}: export-pending row must use native compatibility export gap"
                )
            if row["runtime_execution"] is not False:
                blockers.append(f"{row_id}: export-pending row must have runtime_execution=false")
            if row["write_io"] is not False:
                blockers.append(f"{row_id}: export-pending row must have write_io=false")
            if not row["blocker_id"]:
                blockers.append(f"{row_id}: export-pending row must name blocker_id")
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
    dataframe_methods, dataframe_status_counts, dataframe_blockers = (
        validate_dataframe_method_surface(load_dataframe_method_matrix(repo_root))
    )
    semantic_matrix = load_semantic_surface_matrix(repo_root)
    semantic_rows, semantic_blockers = validate_semantic_surface(semantic_matrix)
    marker_blockers = missing_marker_blockers(repo_root)
    blockers = [
        *matrix_blockers,
        *dataframe_blockers,
        *semantic_blockers,
        *marker_blockers,
    ]
    remaining_gaps = [
        row
        for row in rows
        if row["parity_status"] == "front_door_gap" or row["blocker_id"]
    ]
    dataframe_named_surface_rows = [
        row
        for row in dataframe_methods
        if row["method"] in REQUIRED_DATAFRAME_METHOD_STATUSES
    ]
    dataframe_pending_or_unsupported_rows = [
        row
        for row in dataframe_methods
        if "pending" in str(row["support_status"]).lower()
        or "unsupported" in str(row["support_status"]).lower()
    ]
    dataframe_future_contract_blocker_ids = sorted(
        {
            blocker_id
            for row in dataframe_methods
            for blocker_id in row.get("future_contract_blocker_ids", [])
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "claim_gate_status": "not_claim_grade",
        "covered_phase_items": ["GAR-RUNTIME-IMPL-6C", "CG-20", "CG-21", "CG-22"],
        "scoped_local_front_door_parity_supported": matrix.scoped_local_front_door_parity_supported,
        "v1_scope_document": matrix.v1_scope_document,
        "v1_scope_ready": matrix.v1_scope_ready,
        "v1_supported_row_ids": list(matrix.v1_supported_row_ids),
        "v1_pending_row_ids": list(matrix.v1_pending_row_ids),
        "v1_example_scenario_ids": list(matrix.v1_example_scenario_ids),
        "v1_expected_error_scenario_ids": list(matrix.v1_expected_error_scenario_ids),
        "flexible_anything_claim_allowed": matrix.flexible_anything_claim_allowed,
        "performance_equivalence_claim_allowed": matrix.performance_equivalence_claim_allowed,
        "all_no_fallback_no_external_engine": matrix.all_no_fallback_no_external_engine,
        "row_count": len(rows),
        "dataframe_method_row_count": len(dataframe_methods),
        "dataframe_method_support_status_counts": dataframe_status_counts,
        "dataframe_method_blocker_count": len(
            [row for row in dataframe_methods if row["blocker_id"]]
        ),
        "dataframe_future_contract_blocker_count": len(
            dataframe_future_contract_blocker_ids
        ),
        "dataframe_future_contract_blocker_ids": dataframe_future_contract_blocker_ids,
        "dataframe_method_pending_or_unsupported_count": len(
            dataframe_pending_or_unsupported_rows
        ),
        "dataframe_named_runtime_surface_status": (
            "passed" if not dataframe_blockers else "blocked"
        ),
        "dataframe_named_runtime_surface_ids": [
            str(row["method"]) for row in dataframe_named_surface_rows
        ],
        "dataframe_named_runtime_surface_rows": dataframe_named_surface_rows,
        "dataframe_plan_transform_only_method_ids": sorted(PLAN_TRANSFORM_ONLY_METHODS),
        "semantic_surface_status": "passed" if not semantic_blockers else "blocked",
        "front_door_semantic_surface_schema_version": semantic_matrix.schema_version,
        "semantic_surface_row_count": len(semantic_rows),
        "dataframe_semantic_surface_row_ids": list(semantic_matrix.dataframe_row_ids),
        "sql_semantic_surface_row_ids": list(semantic_matrix.sql_row_ids),
        "shared_semantic_surface_row_ids": list(semantic_matrix.shared_row_ids),
        "dataframe_claim_statement": semantic_matrix.dataframe_claim_statement,
        "dataframe_subset_claim_statement": (
            semantic_matrix.dataframe_subset_claim_statement
        ),
        "sql_claim_statement": semantic_matrix.sql_claim_statement,
        "pandas_compatible_claim_allowed": (
            semantic_matrix.pandas_compatible_claim_allowed
        ),
        "polars_compatible_claim_allowed": (
            semantic_matrix.polars_compatible_claim_allowed
        ),
        "broad_dataframe_compatible_claim_allowed": (
            semantic_matrix.broad_dataframe_compatible_claim_allowed
        ),
        "ansi_sql_compliant_claim_allowed": (
            semantic_matrix.ansi_sql_compliant_claim_allowed
        ),
        "semantic_surface_all_no_fallback_no_external_engine": (
            semantic_matrix.all_no_fallback_no_external_engine
        ),
        "semantic_surface_all_deterministic_blockers": (
            semantic_matrix.all_deterministic_blockers
        ),
        "semantic_surface_rows": semantic_rows,
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
            "ShardLoom has scoped local SQL/Python/DataFrame-style parity where rows declare "
            "equivalent_admitted_scope and a shared ShardLoom runtime path. It exposes a "
            "documented subset of pandas/Polars-style DataFrame operations and a documented "
            "SQL-standard-inspired SELECT-query subset for admitted routes. Broad arbitrary "
            "DataFrame compatibility, broad pandas/Polars compatibility, broad SQL-standard/ANSI-style "
            "compliance, and performance equivalence remain disallowed until the matching "
            "runtime-expansion checklist rows are closed with explicit Vortex-normalization, "
            "correctness, execution-certificate, native-I/O, and benchmark evidence."
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
