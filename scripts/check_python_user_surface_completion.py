#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the scoped Python user-surface completion contract.

This gate answers a narrow release-readiness question: can ShardLoom describe a
simple PySpark-like Python front door for its admitted local runtime scope
without overclaiming Spark compatibility, broad SQL/DataFrame production
support, package publication, or performance?
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.python_user_surface_completion_gate.v1"
GATE_ID = "gar-user-surface-1d.python_user_surface_completion"

FALSE_SAFETY_FIELDS = [
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "fallback_attempted",
    "external_engine_invoked",
]

REQUIRED_DRY_RUN_TRUE_FIELDS = [
    "wheel_import_and_client_smoke_performed",
    "local_python_example_smoke_performed",
    "local_python_user_surface_quickstart_performed",
    "local_python_result_and_evidence_printed",
    "local_python_unsupported_path_evidence_printed",
    "generated_output_proof_distinct_from_no_dataset_smoke",
    "generated_source_user_rows_smoke_performed",
    "generated_source_range_smoke_performed",
]

REQUIRED_DRY_RUN_STEPS = [
    "wheel_import_and_client_smoke",
    "example_local_python_smoke",
    "generated_source_user_rows_local_output_smoke",
    "generated_source_range_local_output_smoke",
]

REQUIRED_QUERY_BUILDER_METHODS = [
    "read_csv",
    "read_json",
    "filter",
    "select",
    "limit",
    "with_column",
    "join",
    "group_by",
    "agg",
    "sort",
    "window",
    "collect",
    "write",
    "write_jsonl",
    "fanout",
    "to_python_objects",
    "profile",
    "describe",
    "quarantine",
]

REQUIRED_ALIAS_METHODS = [
    "project",
    "query",
    "with_columns",
    "assign",
    "groupby",
    "order_by",
    "sort_by",
    "sort_values",
    "distinct",
    "drop_duplicates",
    "unique",
    "schema_contract",
]

REQUIRED_GENERATED_METHODS = [
    "from_rows",
    "range",
    "literal_table",
    "calendar",
    "sequence",
    "sql_values",
    "sql_literal_select",
    "object_store_generated_output",
    "foundry_generated_output",
]

REQUIRED_MATERIALIZATION_METHODS = [
    "to_pandas",
    "to_arrow",
    "to_arrow_table",
    "to_arrow_ipc",
    "to_numpy",
    "from_pandas",
    "from_arrow_table",
    "from_arrow_ipc",
    "display",
]

OPTIONAL_DECODED_OUTPUT_CONTAINER_METHODS = {
    "to_pandas",
    "to_arrow",
    "to_arrow_table",
    "to_arrow_ipc",
    "to_numpy",
}

DISPLAY_MATERIALIZATION_METHODS = {
    "display",
}

MATERIALIZED_INPUT_REENTRY_METHODS = {
    "from_pandas",
    "from_arrow_table",
    "from_arrow_ipc",
}

REQUIRED_TRANSFORM_RUNTIME_METHODS = [
    "rename",
    "rename_columns",
    "drop",
    "drop_columns",
    "astype",
]

REQUIRED_SUMMARY_RUNTIME_METHODS = [
    "nunique",
    "value_counts",
]

REQUIRED_METADATA_PROFILE_METHODS = [
    "profile",
    "describe",
]

REQUIRED_TOP_N_RUNTIME_METHODS = [
    "nlargest",
    "nsmallest",
]

REQUIRED_SOURCE_ORDER_RUNTIME_METHODS = [
    "tail",
]

REQUIRED_SAMPLING_RUNTIME_METHODS = [
    "sample",
]

REQUIRED_DEDUP_RUNTIME_METHODS = [
    "duplicated",
]

REQUIRED_RESHAPE_RUNTIME_METHODS = [
    "melt",
    "explode",
    "pivot",
    "pivot_table",
]

REQUIRED_RESHAPE_RUNTIME_EVIDENCE = {
    "melt": [
        "native_vortex_melt_primitive",
        "explicit_id_value_column_contract",
        "same_typed_value_columns",
        "explicit_decode_materialization_boundary",
    ],
    "explode": [
        "native_vortex_explode_primitive",
        "typed_list_projection",
        "list_element_scalar_contract",
        "explicit_decode_materialization_boundary",
    ],
    "pivot": [
        "native_vortex_pivot_primitive",
        "single_index_column_contract",
        "single_pivot_column_contract",
        "single_value_column_contract",
        "duplicate_cell_fail_closed_policy",
        "explicit_decode_materialization_boundary",
    ],
    "pivot_table": [
        "native_vortex_pivot_primitive",
        "single_index_column_contract",
        "single_pivot_column_contract",
        "single_value_column_contract",
        "explicit_aggregate_kernel",
        "explicit_decode_materialization_boundary",
    ],
}

REQUIRED_WINDOW_RUNTIME_METHODS = [
    "rolling",
]

REQUIRED_PLAN_TRANSFORM_METHODS = [
    "apply",
    "pipe",
]

REQUIRED_EXPRESSION_RUNTIME_METHODS = [
    "applymap",
    "eval",
    "map",
    "map_rows",
    "transform",
]

REQUIRED_COMBINE_RUNTIME_METHODS = [
    "merge",
    "concat",
]

REQUIRED_NULL_RUNTIME_METHODS = [
    "fillna",
    "fill_null",
    "dropna",
    "isna",
    "isnull",
    "notna",
    "notnull",
]

REQUIRED_INDEX_METADATA_METHODS = [
    "set_index",
    "reset_index",
    "sort_index",
]

REQUIRED_UNSUPPORTED_METHODS: list[str] = []

REQUIRED_DOC_MARKERS = {
    "README.md": [
        "prepared = ctx.prepare_vortex(",
        "prepared.query(\"selective filter\").collect()",
        "prepared.query(\"clean/cast/filter/write\").collect()",
        "scenario_selective-filter_fallback_attempted",
        "materialization_report.blocker_id",
        "fallback_attempted, result.external_engine_invoked",
    ],
    "python/README.md": [
        "DataFrame-style surface",
        "ctx.session",
        "ctx.sql(...)",
        "docs/reference/shardloom-user-surface-index.md",
        "blocked.required_evidence",
        "matrix.all_no_fallback_no_external_engine",
    ],
    "docs/reference/shardloom-user-surface-index.md": [
        "shardloom.user_surface_index.v1",
        "shardloom command-metadata --format json",
        "ctx.read(path)",
        "ctx.sql(\"SELECT ...\")",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ],
    "docs/getting-started/first-10-minutes.md": [
        "python examples\\local-python-smoke\\run.py --repo-root .",
        "ctx.read(...).filter(...).select(...).write_jsonl(...)` path blocks",
        "quickstart_local_file_blocker_id",
        "quickstart_generated_output_row_count",
        "quickstart_unsupported_blocker_id",
    ],
    "website-src/src/pages/start.astro": [
        "examples\\local-python-smoke\\run.py --repo-root .",
        "ctx.read(\"data/orders.csv\")",
        "sl.col(\"status\") == \"paid\"",
        "print(result.claim_summary.claim_gate_status)",
        "check_python_user_surface_completion.py",
    ],
}

REQUIRED_SOURCE_MARKERS = {
    "examples/local-python-smoke/run.py": [
        "quickstart_user_surface_status=passed",
        "quickstart_local_file_blocker_id=",
        "quickstart_local_file_runtime_execution=",
        "quickstart_generated_output_row_count=",
        "quickstart_unsupported_blocker_id=",
        "quickstart_unsupported_external_engine_invoked=",
        ".filter(",
        ".select(",
        ".write_jsonl(",
        ".with_column(",
    ],
    "python/src/shardloom/context.py": [
        "class ShardLoomContext",
        "def context(",
        "def session(",
        "DATAFRAME_METHOD_CAPABILITY_ROWS",
    ],
    "python/src/shardloom/session.py": [
        "class ShardLoomSession",
        "class SessionSqlResult",
        "def sql(",
    ],
    "python/src/shardloom/query.py": [
        "class UnsupportedWorkflowOperationReport",
        "def to_pandas(",
        "def from_rows(",
        "def sql(",
        "external_engine_invoked",
    ],
}

REQUIRED_TEST_MARKERS = {
    "python/tests/test_query_builder.py": [
        "test_context_sql_local_source_collect_invokes_sql_smoke",
        "test_context_sql_local_source_write_invokes_sql_smoke",
        "test_context_sql_source_free_write_invokes_generated_source_sql_smoke",
        "test_schema_declared_dataframe_rename_blocks_until_alias_preserving_vortex_projection",
        "test_schema_declared_dataframe_drop_lowers_to_prepared_vortex_projection",
        "test_local_csv_query_builder_value_counts_lowers_to_grouped_count",
        "test_local_csv_query_builder_concat_lowers_to_union_all",
        "test_local_csv_query_builder_merge_lowers_to_join",
        "test_local_csv_query_builder_nunique_lowers_to_count_distinct",
        "test_schema_declared_dataframe_fillna_lowers_to_coalesce_projection",
        "test_schema_declared_dataframe_null_masks_lower_to_boolean_projections",
        "test_schema_declared_dataframe_query_dropna_astype_lowers_to_sql_smoke",
        "test_local_csv_query_builder_top_n_dataframe_aliases_lower_to_sort_limit",
        "test_missing_dataframe_affordances_return_report_only_unsupported",
        "workflow.rename({\"amount\": \"order_amount\"})",
        "workflow.drop(columns=[\"unused\"])",
        "workflow.astype({\"amount\": \"int64\"})",
        "workflow.dropna(subset=[\"label\"])",
        "source.nlargest(5, \"amount\")",
        "test_local_csv_query_builder_sample_collect_routes_through_prepared_vortex",
        "native_vortex_sample",
        "test_local_csv_query_builder_explode_routes_through_prepared_vortex_explode",
        "test_local_csv_query_builder_pivot_routes_through_prepared_vortex_pivot",
        "test_local_csv_query_builder_pivot_table_routes_through_prepared_vortex_pivot",
        "workflow.pipe(\"workflow_udf\", \"arg1\", config=\"strict\")",
        "workflow.transform(\"column_udf\")",
        "workflow.applymap(\"cell_udf\")",
        "workflow.eval(\"amount + tax\", engine=\"python\")",
    ],
    "python/tests/test_cli_client.py": [
        "test_context_session_reuses_prepared_vortex_state_when_fingerprints_match",
        "test_context_session_reuses_local_query_output_when_fingerprints_match",
        "test_context_session_reuses_local_fanout_outputs_when_fingerprints_match",
        "test_context_capabilities_collects_typed_views_without_dataset_commands",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--release-dry-run-transcript",
        type=Path,
        default=Path("target/release-dry-run-proof/transcript.json"),
    )
    parser.add_argument(
        "--runs-today-matrix",
        type=Path,
        default=Path("docs/status/runs-today-support-matrix.json"),
    )
    parser.add_argument(
        "--production-usability-report",
        type=Path,
        default=Path("target/production-usability-gate.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/python-user-surface-completion-gate.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def step_map(dry_run: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if dry_run is None:
        return {}
    steps = dry_run.get("steps")
    if not isinstance(steps, list):
        return {}
    return {
        str(step.get("name")): step
        for step in steps
        if isinstance(step, dict) and isinstance(step.get("name"), str)
    }


def step_passed(steps: dict[str, dict[str, Any]], name: str) -> bool:
    return steps.get(name, {}).get("returncode") == 0


def _row(
    row_id: str,
    surface: str,
    blockers: list[str],
    evidence_refs: list[str],
    *,
    status_when_passed: str = "passed",
    claim_gate_status: str = "not_claim_grade",
) -> dict[str, Any]:
    return {
        "row_id": row_id,
        "surface": surface,
        "status": status_when_passed if not blockers else "blocked",
        "claim_gate_status": claim_gate_status,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "evidence_refs": evidence_refs,
        "blockers": blockers,
    }


def _missing_marker_blockers(repo_root: Path, marker_map: dict[str, list[str]]) -> list[str]:
    blockers: list[str] = []
    for rel_path, markers in marker_map.items():
        text = read_text(repo_root / rel_path)
        if not text:
            blockers.append(f"missing required file: {rel_path}")
            continue
        for marker in markers:
            if marker not in text:
                blockers.append(f"{rel_path} missing marker: {marker}")
    return blockers


def _static_value(node: ast.AST, constants: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, (ast.Tuple, ast.List)):
        return tuple(_static_value(elt, constants) for elt in node.elts)
    return None


def _constant_name(target: ast.AST) -> str | None:
    return target.id if isinstance(target, ast.Name) else None


def _load_dataframe_method_rows_from_source(context_path: Path) -> tuple[dict[str, Any], ...]:
    tree = ast.parse(read_text(context_path), filename=str(context_path))
    constants: dict[str, Any] = {}
    dataframe_rows_node: ast.AST | None = None

    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            name = _constant_name(node.targets[0])
            if name is not None:
                constants[name] = _static_value(node.value, constants)
                if name == "DATAFRAME_METHOD_CAPABILITY_ROWS":
                    dataframe_rows_node = node.value
        elif isinstance(node, ast.AnnAssign):
            name = _constant_name(node.target)
            if name is not None:
                constants[name] = _static_value(node.value, constants) if node.value else None
                if name == "DATAFRAME_METHOD_CAPABILITY_ROWS":
                    dataframe_rows_node = node.value

    if not isinstance(dataframe_rows_node, ast.Tuple):
        return ()

    rows: list[dict[str, Any]] = []
    for row_node in dataframe_rows_node.elts:
        if not (
            isinstance(row_node, ast.Call)
            and isinstance(row_node.func, ast.Name)
            and row_node.func.id == "_df_method"
            and len(row_node.args) >= 3
        ):
            continue
        method = _static_value(row_node.args[0], constants)
        family = _static_value(row_node.args[1], constants)
        support_status = _static_value(row_node.args[2], constants)
        keywords = {
            keyword.arg: _static_value(keyword.value, constants)
            for keyword in row_node.keywords
            if keyword.arg is not None
        }
        required_evidence = keywords.get("required_evidence", ())
        if required_evidence is None:
            required_evidence = ()
        rows.append(
            {
                "method": method,
                "family": family,
                "support_status": support_status,
                "claim_gate_status": "not_claim_grade",
                "diagnostic_operation": keywords.get("diagnostic_operation"),
                "blocker_id": keywords.get("blocker_id"),
                "required_evidence": list(required_evidence),
                "runtime_execution": bool(keywords.get("runtime_execution", False)),
                "data_read": bool(keywords.get("data_read", False)),
                "write_io": bool(keywords.get("write_io", False)),
                "materialization_required": bool(
                    keywords.get("materialization_required", False)
                ),
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_boundary": keywords.get("claim_boundary") or "",
            }
        )
    return tuple(rows)


def _load_dataframe_method_rows(repo_root: Path) -> tuple[dict[str, Any], ...]:
    src = repo_root / "python" / "src"
    inserted = False
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
        inserted = True
    try:
        try:
            from shardloom.context import DATAFRAME_METHOD_CAPABILITY_ROWS
        except TypeError as exc:
            if sys.version_info >= (3, 10) or "dataclass" not in str(exc) or "slots" not in str(exc):
                raise
            return _load_dataframe_method_rows_from_source(src / "shardloom" / "context.py")

        return tuple(
            {
                "method": row.method,
                "family": row.family,
                "support_status": row.support_status,
                "claim_gate_status": row.claim_gate_status,
                "diagnostic_operation": row.diagnostic_operation,
                "blocker_id": row.blocker_id,
                "required_evidence": list(row.required_evidence),
                "runtime_execution": row.runtime_execution,
                "data_read": row.data_read,
                "write_io": row.write_io,
                "materialization_required": row.materialization_required,
                "fallback_attempted": row.fallback_attempted,
                "external_engine_invoked": row.external_engine_invoked,
                "claim_boundary": row.claim_boundary,
            }
            for row in DATAFRAME_METHOD_CAPABILITY_ROWS
        )
    finally:
        if inserted:
            sys.path.remove(str(src))


def validate_release_dry_run(dry_run: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if dry_run is None:
        return {"status": "missing"}, ["missing release dry-run transcript"]
    if dry_run.get("schema_version") != "shardloom.release_dry_run_proof.v1":
        blockers.append("release dry-run schema_version mismatch")
    for field in REQUIRED_DRY_RUN_TRUE_FIELDS:
        if dry_run.get(field) is not True:
            blockers.append(f"release dry-run {field} must be true")
    for field in [
        *FALSE_SAFETY_FIELDS,
        "external_runtime_dependencies_added",
        "fallback_engine_dependency_added",
        "public_package_release_claim_allowed",
    ]:
        if dry_run.get(field) is not False:
            blockers.append(f"release dry-run {field} must be false")
    steps = step_map(dry_run)
    for step_name in REQUIRED_DRY_RUN_STEPS:
        if not step_passed(steps, step_name):
            blockers.append(f"release dry-run step did not pass: {step_name}")
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "required_true_fields": REQUIRED_DRY_RUN_TRUE_FIELDS,
            "required_steps": REQUIRED_DRY_RUN_STEPS,
        },
        blockers,
    )


def validate_method_matrix(rows: tuple[dict[str, Any], ...]) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    by_method = {str(row.get("method")): row for row in rows}

    for method in [
        *REQUIRED_QUERY_BUILDER_METHODS,
        *REQUIRED_ALIAS_METHODS,
        *REQUIRED_GENERATED_METHODS,
        "sql",
        *REQUIRED_MATERIALIZATION_METHODS,
        *REQUIRED_TRANSFORM_RUNTIME_METHODS,
        *REQUIRED_SUMMARY_RUNTIME_METHODS,
        *REQUIRED_TOP_N_RUNTIME_METHODS,
        *REQUIRED_SOURCE_ORDER_RUNTIME_METHODS,
        *REQUIRED_SAMPLING_RUNTIME_METHODS,
        *REQUIRED_DEDUP_RUNTIME_METHODS,
        *REQUIRED_RESHAPE_RUNTIME_METHODS,
        *REQUIRED_WINDOW_RUNTIME_METHODS,
        *REQUIRED_PLAN_TRANSFORM_METHODS,
        *REQUIRED_EXPRESSION_RUNTIME_METHODS,
        *REQUIRED_COMBINE_RUNTIME_METHODS,
        *REQUIRED_NULL_RUNTIME_METHODS,
        *REQUIRED_UNSUPPORTED_METHODS,
    ]:
        if method not in by_method:
            blockers.append(f"DataFrame method matrix missing method: {method}")

    for row in rows:
        method = str(row.get("method"))
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{method}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{method}: external_engine_invoked must be false")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"{method}: claim_gate_status must be not_claim_grade")
        if not isinstance(row.get("claim_boundary"), str) or not row["claim_boundary"].strip():
            blockers.append(f"{method}: missing claim_boundary")

    for method in [
        *REQUIRED_QUERY_BUILDER_METHODS,
        *REQUIRED_ALIAS_METHODS,
        *REQUIRED_GENERATED_METHODS,
        *REQUIRED_TRANSFORM_RUNTIME_METHODS,
        *REQUIRED_SUMMARY_RUNTIME_METHODS,
        *REQUIRED_TOP_N_RUNTIME_METHODS,
        *REQUIRED_SOURCE_ORDER_RUNTIME_METHODS,
        *REQUIRED_SAMPLING_RUNTIME_METHODS,
        *REQUIRED_DEDUP_RUNTIME_METHODS,
        *REQUIRED_RESHAPE_RUNTIME_METHODS,
        *REQUIRED_WINDOW_RUNTIME_METHODS,
        *REQUIRED_PLAN_TRANSFORM_METHODS,
        *REQUIRED_EXPRESSION_RUNTIME_METHODS,
        *REQUIRED_COMBINE_RUNTIME_METHODS,
        "sql",
    ]:
        row = by_method.get(method)
        if not row:
            continue
        support_status = str(row.get("support_status", ""))
        if "unsupported" in support_status:
            blockers.append(f"{method}: must not be an unsupported row")
        if support_status == "fixture_smoke_supported" and not row.get("required_evidence"):
            blockers.append(f"{method}: fixture support requires evidence names")

    for method in REQUIRED_TRANSFORM_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "fixture_smoke_supported":
            blockers.append(f"{method}: scoped transform support must be fixture_smoke_supported")
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: scoped transform support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: scoped transform support requires data_read true")
        if row.get("blocker_id"):
            blockers.append(f"{method}: scoped transform row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        if "declared_schema_projection_rewrite" not in required_evidence:
            blockers.append(
                f"{method}: scoped transform row missing declared_schema_projection_rewrite"
            )

    for method in REQUIRED_SUMMARY_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "fixture_smoke_supported":
            blockers.append(f"{method}: scoped summary support must be fixture_smoke_supported")
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: scoped summary support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: scoped summary support requires data_read true")
        if row.get("blocker_id"):
            blockers.append(f"{method}: scoped summary row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        expected_evidence = (
            ["distinct_count_semantics", "dropna_policy"]
            if method == "nunique"
            else ["grouped_count_semantics", "ordering_contract"]
        )
        for evidence in expected_evidence:
            if evidence not in required_evidence:
                blockers.append(f"{method}: scoped summary row missing {evidence}")

    for method in REQUIRED_METADATA_PROFILE_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: metadata profile support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: metadata profile support requires runtime_execution true")
        if row.get("data_read") is not False:
            blockers.append(f"{method}: metadata profile support must not claim data_read")
        if row.get("blocker_id"):
            blockers.append(f"{method}: metadata profile row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        if "native_vortex_metadata_profile_route" not in required_evidence:
            blockers.append(
                f"{method}: metadata profile row missing native_vortex_metadata_profile_route"
            )

    for method in REQUIRED_TOP_N_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "fixture_smoke_supported":
            blockers.append(f"{method}: scoped top-N support must be fixture_smoke_supported")
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: scoped top-N support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: scoped top-N support requires data_read true")
        if row.get("blocker_id"):
            blockers.append(f"{method}: scoped top-N row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in ["sort_operator", "top_n_contract", "ordering_contract"]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: scoped top-N row missing {evidence}")

    for method in REQUIRED_SOURCE_ORDER_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: source-order runtime support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(
                f"{method}: source-order runtime support requires runtime_execution true"
            )
        if row.get("data_read") is not True:
            blockers.append(f"{method}: source-order runtime support requires data_read true")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: source-order runtime must report materialization_required")
        if row.get("blocker_id"):
            blockers.append(f"{method}: source-order runtime row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in [
            "native_vortex_tail_primitive",
            "source_order_semantics",
            "explicit_decode_materialization_boundary",
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: source-order runtime row missing {evidence}")

    for method in REQUIRED_SAMPLING_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: sampling runtime support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: sampling runtime support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: sampling runtime support requires data_read true")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: sampling runtime must report materialization_required")
        if row.get("blocker_id"):
            blockers.append(f"{method}: sampling runtime row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in [
            "native_vortex_sample_primitive",
            "deterministic_seed_policy",
            "bounded_result_contract",
            "explicit_decode_materialization_boundary",
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: sampling runtime row missing {evidence}")

    for method in REQUIRED_DEDUP_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: deduplication runtime support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: deduplication runtime support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: deduplication runtime support requires data_read true")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: deduplication runtime must report materialization_required")
        if row.get("blocker_id"):
            blockers.append(f"{method}: deduplication runtime row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in [
            "native_vortex_duplicate_mask_primitive",
            "duplicate_mask_semantics",
            "keep_first_subset_contract",
            "explicit_decode_materialization_boundary",
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: deduplication runtime row missing {evidence}")

    for method in REQUIRED_RESHAPE_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: reshape runtime support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: reshape runtime support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: reshape runtime support requires data_read true")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: reshape runtime must report materialization_required")
        if row.get("blocker_id"):
            blockers.append(f"{method}: reshape runtime row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in REQUIRED_RESHAPE_RUNTIME_EVIDENCE.get(method, []):
            if evidence not in required_evidence:
                blockers.append(f"{method}: reshape runtime row missing {evidence}")

    for method in REQUIRED_WINDOW_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: window runtime support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: window runtime support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: window runtime support requires data_read true")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: window runtime must report materialization_required")
        if row.get("blocker_id"):
            blockers.append(f"{method}: window runtime row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in [
            "native_vortex_rolling_window_primitive",
            "source_order_window_contract",
            "bounded_window_state",
            "complete_window_sum_semantics",
            "explicit_decode_materialization_boundary",
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: window runtime row missing {evidence}")

    for method in REQUIRED_PLAN_TRANSFORM_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "lazy_plan_supported":
            blockers.append(f"{method}: plan-transform support must be lazy_plan_supported")
        if row.get("runtime_execution") is not False:
            blockers.append(f"{method}: plan-transform support must not claim direct runtime execution")
        if row.get("data_read") is not False:
            blockers.append(f"{method}: plan-transform support must not claim data_read")
        if row.get("materialization_required") is not False:
            blockers.append(f"{method}: plan-transform support must not require materialization")
        if row.get("blocker_id"):
            blockers.append(f"{method}: plan-transform row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in [
            "explicit_shardloom_plan_transform_wrapper",
            "lazy_plan_return_type_contract",
            "terminal_route_preserves_execution_certificate",
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: plan-transform row missing {evidence}")

    for method in REQUIRED_EXPRESSION_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "production_admitted_local_workflow":
            blockers.append(
                f"{method}: expression runtime support must be production_admitted_local_workflow"
            )
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: expression runtime support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: expression runtime support requires data_read true")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: expression runtime must report materialization_required")
        if row.get("blocker_id"):
            blockers.append(f"{method}: expression runtime row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        for evidence in [
            "native_vortex_expression_project_primitive",
            "numeric_scalar_assignment_contract",
            "typed_expression_contract",
            "explicit_decode_materialization_boundary",
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: expression runtime row missing {evidence}")

    for method in REQUIRED_COMBINE_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "fixture_smoke_supported":
            blockers.append(f"{method}: scoped combine support must be fixture_smoke_supported")
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: scoped combine support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: scoped combine support requires data_read true")
        if row.get("blocker_id"):
            blockers.append(f"{method}: scoped combine row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        expected_evidence = (
            ["join_alias_semantics", "join_operator_capability"]
            if method == "merge"
            else ["schema_alignment_contract", "set_operation_semantics"]
        )
        for evidence in expected_evidence:
            if evidence not in required_evidence:
                blockers.append(f"{method}: scoped combine row missing {evidence}")

    for method in REQUIRED_NULL_RUNTIME_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        if row.get("support_status") != "fixture_smoke_supported":
            blockers.append(f"{method}: scoped null support must be fixture_smoke_supported")
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: scoped null support requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: scoped null support requires data_read true")
        if row.get("blocker_id"):
            blockers.append(f"{method}: scoped null row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        if method in {"fillna", "fill_null"}:
            expected_evidence = [
                "null_fill_semantics",
                "projection_rewrite_semantics",
                "dtype_coercion_policy",
            ]
        elif method == "dropna":
            expected_evidence = [
                "declared_schema_filter_rewrite",
                "null_filter_semantics",
                "projection_rewrite_semantics",
            ]
        elif method in {"notna", "notnull"}:
            expected_evidence = [
                "projection_result_shape",
                "three_valued_logic_policy",
                "not_null_mask_semantics",
            ]
        else:
            expected_evidence = [
                "projection_result_shape",
                "three_valued_logic_policy",
                "null_mask_semantics",
            ]
        for evidence in expected_evidence:
            if evidence not in required_evidence:
                blockers.append(f"{method}: scoped null row missing {evidence}")

    for method in REQUIRED_MATERIALIZATION_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        support_status = str(row.get("support_status", ""))
        if method in OPTIONAL_DECODED_OUTPUT_CONTAINER_METHODS:
            if support_status != "optional_dependency_container_supported":
                blockers.append(
                    f"{method}: optional decoded output materialization must be optional_dependency_container_supported"
                )
            if row.get("blocker_id"):
                blockers.append(
                    f"{method}: optional decoded output materialization must not carry blocker_id"
                )
            required_evidence = set(row.get("required_evidence") or [])
            for evidence in [
                "vortex_prepared_state_or_native_vortex_input",
                "bounded_materialization_contract",
                "decoded_materialization_policy",
                "optional_dependency_policy",
                "no_fallback_evidence",
            ]:
                if evidence not in required_evidence:
                    blockers.append(f"{method}: optional materialization row missing {evidence}")
            if row.get("runtime_execution") is not True:
                blockers.append(f"{method}: optional materialization requires runtime_execution true")
            if row.get("data_read") is not True:
                blockers.append(f"{method}: optional materialization requires data_read true")
        elif method in DISPLAY_MATERIALIZATION_METHODS:
            if support_status != "production_admitted_local_workflow":
                blockers.append(
                    f"{method}: display materialization must be production_admitted_local_workflow"
                )
            if row.get("blocker_id"):
                blockers.append(f"{method}: display materialization must not carry blocker_id")
            required_evidence = set(row.get("required_evidence") or [])
            for evidence in [
                "vortex_prepared_state_or_native_vortex_input",
                "bounded_materialization_contract",
                "decoded_materialization_policy",
                "notebook_display_contract",
                "no_fallback_evidence",
            ]:
                if evidence not in required_evidence:
                    blockers.append(f"{method}: display materialization row missing {evidence}")
            if row.get("runtime_execution") is not True:
                blockers.append(f"{method}: display materialization requires runtime_execution true")
            if row.get("data_read") is not True:
                blockers.append(f"{method}: display materialization requires data_read true")
        elif method in MATERIALIZED_INPUT_REENTRY_METHODS:
            if "unsupported" in support_status or support_status == "runtime_expansion_pending":
                blockers.append(
                    f"{method}: materialized input re-entry row must be scoped supported"
                )
            if row.get("blocker_id"):
                blockers.append(f"{method}: materialized input re-entry row must not carry blocker_id")
        if row.get("materialization_required") is not True:
            blockers.append(f"{method}: materialization_required must be true")
        if not row.get("required_evidence"):
            blockers.append(f"{method}: missing materialization evidence")

    for method in REQUIRED_UNSUPPORTED_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        support_status = str(row.get("support_status", ""))
        if "unsupported" not in support_status:
            blockers.append(f"{method}: must be an unsupported diagnostic row")
        if not row.get("blocker_id"):
            blockers.append(f"{method}: missing blocker_id")
        if not row.get("required_evidence"):
            blockers.append(f"{method}: missing required_evidence")
        for field in ["runtime_execution", "data_read", "write_io"]:
            if row.get(field) is not False:
                blockers.append(f"{method}: unsupported row {field} must be false")

    for method in REQUIRED_INDEX_METADATA_METHODS:
        row = by_method.get(method)
        if not row:
            continue
        support_status = str(row.get("support_status", ""))
        if support_status != "scoped_runtime_supported":
            blockers.append(f"{method}: index metadata row must be scoped_runtime_supported")
        if row.get("blocker_id"):
            blockers.append(f"{method}: index metadata row must not carry blocker_id")
        required_evidence = set(row.get("required_evidence") or [])
        expected_evidence = [
            "source_order_preservation",
            "execution_certificate",
            "no_fallback_evidence",
        ]
        if method == "set_index":
            expected_evidence.extend(
                [
                    "explicit_index_state_metadata",
                    "encoded_row_data_preserved",
                ]
            )
        else:
            expected_evidence.append("no_explicit_index_state_contract")
        for evidence in [
            *expected_evidence,
        ]:
            if evidence not in required_evidence:
                blockers.append(f"{method}: index metadata row missing {evidence}")
        if row.get("runtime_execution") is not True:
            blockers.append(f"{method}: index metadata row requires runtime_execution true")
        if row.get("data_read") is not True:
            blockers.append(f"{method}: index metadata row requires data_read true")

    completion_rows = [
        _row(
            "dataframe_query_builder",
            "DataFrame/query-builder",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_QUERY_BUILDER_METHODS)
                or any(f"{method}:" in blocker for method in REQUIRED_ALIAS_METHODS)
                or "DataFrame method matrix missing method" in blocker
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_or_lazy_rows_present",
        ),
        _row(
            "generated_output",
            "source-free generated output",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_GENERATED_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "ctx_sql",
            "ctx.sql local-source/source-free bridge",
            [blocker for blocker in blockers if blocker.startswith("sql:")],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_row_present",
        ),
        _row(
            "materialization_interop",
            "bounded materialization and materialized input boundaries",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_MATERIALIZATION_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_materialization_rows_present",
        ),
        _row(
            "schema_declared_transforms",
            "schema-declared DataFrame projection transforms",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_TRANSFORM_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_summary_methods",
            "scoped DataFrame summary methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_SUMMARY_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "metadata_profile_methods",
            "metadata-first DataFrame profile methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_METADATA_PROFILE_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="metadata_first_profile_rows_present",
        ),
        _row(
            "scoped_top_n_methods",
            "scoped DataFrame top-N selection methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_TOP_N_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_source_order_methods",
            "scoped DataFrame source-order selection methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_SOURCE_ORDER_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_sampling_methods",
            "scoped DataFrame deterministic sampling methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_SAMPLING_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_deduplication_methods",
            "scoped DataFrame duplicate-mask methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_DEDUP_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_reshape_methods",
            "scoped DataFrame reshape runtime methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_RESHAPE_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_window_methods",
            "scoped DataFrame window runtime methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_WINDOW_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_plan_transform_methods",
            "scoped DataFrame plan-transform methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_PLAN_TRANSFORM_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_lazy_plan_rows_present",
        ),
        _row(
            "scoped_expression_runtime_methods",
            "scoped DataFrame expression runtime methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_EXPRESSION_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_combine_methods",
            "scoped DataFrame combine methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_COMBINE_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_null_methods",
            "scoped DataFrame null cleanup and mask methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_NULL_RUNTIME_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "scoped_index_metadata_methods",
            "scoped DataFrame index metadata no-op methods",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_INDEX_METADATA_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="scoped_runtime_rows_present",
        ),
        _row(
            "unsupported_paths",
            "deterministic unsupported broad/unsafe paths",
            [
                blocker
                for blocker in blockers
                if any(f"{method}:" in blocker for method in REQUIRED_UNSUPPORTED_METHODS)
            ],
            ["python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS"],
            status_when_passed="deterministic_blockers_present",
        ),
    ]
    return completion_rows, blockers


def validate_runs_today(runs_today: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if runs_today is None:
        return {"status": "missing"}, ["missing runs-today support matrix"]
    if runs_today.get("schema_version") != "shardloom.runs_today_support_matrix.v1":
        blockers.append("runs-today support matrix schema mismatch")
    if runs_today.get("all_rows_no_fallback_no_external_engine") is not True:
        blockers.append("runs-today all_rows_no_fallback_no_external_engine must be true")
    if runs_today.get("performance_claim_allowed") is not False:
        blockers.append("runs-today performance_claim_allowed must be false")
    rows = runs_today.get("rows")
    if not isinstance(rows, list):
        return {"status": "blocked", "row_count": 0}, blockers + ["runs-today rows must be a list"]
    by_id = {row.get("id"): row for row in rows if isinstance(row, dict)}
    for row_id in [
        "python_status_capabilities",
        "python_local_query_builder",
        "python_generated_source_helpers",
    ]:
        if row_id not in by_id:
            blockers.append(f"runs-today missing Python surface row: {row_id}")
    for row_id in [
        "claim_production_readiness",
        "claim_object_store_lakehouse_foundry_production",
        "claim_performance_superiority",
    ]:
        row = by_id.get(row_id)
        if not isinstance(row, dict):
            blockers.append(f"runs-today missing blocked claim row: {row_id}")
            continue
        if row.get("support_state") != "blocked":
            blockers.append(f"runs-today {row_id} support_state must be blocked")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"runs-today {row_id} claim_gate_status must be not_claim_grade")
        if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
            blockers.append(f"runs-today {row_id} fallback/external fields must be false")
    package_row = by_id.get("claim_package_publication")
    if not isinstance(package_row, dict):
        blockers.append("runs-today missing selected package access row: claim_package_publication")
    else:
        if package_row.get("support_state") != "executable":
            blockers.append("runs-today claim_package_publication support_state must be executable")
        if package_row.get("claim_gate_status") != "package_access_only":
            blockers.append(
                "runs-today claim_package_publication claim_gate_status must be package_access_only"
            )
        if (
            package_row.get("fallback_attempted") is not False
            or package_row.get("external_engine_invoked") is not False
        ):
            blockers.append("runs-today claim_package_publication fallback/external fields must be false")
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "row_count": len(rows),
            "support_state_counts": runs_today.get("support_state_counts"),
        },
        blockers,
    )


def validate_production_usability_claims(
    production_usability: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str]]:
    if production_usability is None:
        return {"status": "not_attached"}, []
    blockers: list[str] = []
    if production_usability.get("schema_version") != "shardloom.production_usability_gate.v1":
        blockers.append("production usability schema_version mismatch")
    for field in [
        "production_claim_allowed",
        "performance_claim_allowed",
        "public_release_claim_allowed",
        "public_package_claim_allowed",
        "publication_attempted",
        "tag_created",
        "secrets_required",
        "fallback_attempted",
        "external_engine_invoked",
    ]:
        if production_usability.get(field) is not False:
            blockers.append(f"production usability {field} must be false")
    return {"status": "passed" if not blockers else "blocked"}, blockers


def build_report(
    *,
    repo_root: Path,
    release_dry_run_ref: str,
    runs_today_matrix_ref: str,
    production_usability_ref: str,
    dry_run: dict[str, Any] | None,
    runs_today: dict[str, Any] | None,
    production_usability: dict[str, Any] | None,
) -> dict[str, Any]:
    release_summary, release_blockers = validate_release_dry_run(dry_run)
    method_rows = _load_dataframe_method_rows(repo_root)
    method_completion_rows, method_blockers = validate_method_matrix(method_rows)
    docs_blockers = _missing_marker_blockers(repo_root, REQUIRED_DOC_MARKERS)
    source_blockers = _missing_marker_blockers(repo_root, REQUIRED_SOURCE_MARKERS)
    test_blockers = _missing_marker_blockers(repo_root, REQUIRED_TEST_MARKERS)
    runs_today_summary, runs_today_blockers = validate_runs_today(runs_today)
    production_summary, production_blockers = validate_production_usability_claims(
        production_usability
    )

    matrix_rows = [
        _row(
            "install_import_context",
            "import, context, and first local workflow",
            release_blockers,
            [release_dry_run_ref, "examples/local-python-smoke/run.py"],
            status_when_passed="runnable_proof_present",
        ),
        *method_completion_rows,
        _row(
            "session",
            "caller-owned session reuse surface",
            [
                blocker
                for blocker in [*source_blockers, *test_blockers]
                if "session" in blocker.lower() or "Session" in blocker
            ],
            [
                "python/src/shardloom/session.py",
                "python/tests/test_cli_client.py",
                "python/README.md",
            ],
            status_when_passed="session_surface_documented_and_tested",
        ),
        _row(
            "docs_website_claim_boundary",
            "README, Python docs, first-10-minutes, and website claim boundary",
            docs_blockers,
            list(REQUIRED_DOC_MARKERS.keys()),
            status_when_passed="claim_boundary_documented",
        ),
        _row(
            "source_and_tests",
            "source and test markers for user-surface proof",
            [*source_blockers, *test_blockers],
            [*REQUIRED_SOURCE_MARKERS.keys(), *REQUIRED_TEST_MARKERS.keys()],
            status_when_passed="source_and_tests_present",
        ),
        _row(
            "runs_today_claim_boundary",
            "status matrix and public-claim blockers",
            runs_today_blockers,
            [runs_today_matrix_ref],
            status_when_passed="claim_boundary_rows_present",
        ),
        _row(
            "production_usability_overclaim_guard",
            "attached production-usability overclaim guard",
            production_blockers,
            [production_usability_ref],
            status_when_passed=production_summary["status"],
        ),
    ]

    blockers = [
        *release_blockers,
        *method_blockers,
        *docs_blockers,
        *source_blockers,
        *test_blockers,
        *runs_today_blockers,
        *production_blockers,
    ]
    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if passed else "blocked",
        "covered_phase_items": ["GAR-USER-SURFACE-1D"],
        "claim_gate_status": "not_claim_grade",
        "claim_scope": "scoped_admitted_local_python_front_door",
        "scoped_python_front_door_claim_allowed": passed,
        "production_sql_dataframe_claim_allowed": False,
        "spark_compatibility_claim_allowed": False,
        "package_publication_claim_allowed": False,
        "performance_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_boundary": (
            "Only a PySpark-like simple Python front door for admitted local runtime smokes: "
            "import/context/session, scoped local-source SQL/DataFrame/query-builder, "
            "source-free generated output, and deterministic unsupported diagnostics. This is not "
            "Spark compatibility, broad SQL/DataFrame production support, package publication, "
            "distributed execution, object-store/lakehouse, Foundry production, or performance evidence."
        ),
        "release_dry_run_transcript_ref": release_dry_run_ref,
        "runs_today_matrix_ref": runs_today_matrix_ref,
        "production_usability_ref": production_usability_ref,
        "release_dry_run": release_summary,
        "runs_today_support_matrix": runs_today_summary,
        "production_usability_overclaim_guard": production_summary,
        "method_matrix_row_count": len(method_rows),
        "method_matrix_rows": method_rows,
        "completion_matrix": matrix_rows,
        "remaining_non_parity_gaps": [
            "Spark internals and PySpark API parity",
            "broad SQL/DataFrame production runtime",
            "decoded pandas/Arrow/NumPy materialization",
            "object-store/lakehouse/table production IO",
            "package publication and channel install claims",
            "performance or Spark-displacement claims",
        ],
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    dry_run_path = resolve(repo_root, args.release_dry_run_transcript)
    runs_today_path = resolve(repo_root, args.runs_today_matrix)
    production_usability_path = resolve(repo_root, args.production_usability_report)
    output = resolve(repo_root, args.output)
    report = build_report(
        repo_root=repo_root,
        release_dry_run_ref=rel(repo_root, dry_run_path),
        runs_today_matrix_ref=rel(repo_root, runs_today_path),
        production_usability_ref=rel(repo_root, production_usability_path),
        dry_run=load_json(dry_run_path),
        runs_today=load_json(runs_today_path),
        production_usability=load_json(production_usability_path),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"python user-surface completion blocker: {blocker}")
        return 1
    print(f"python user-surface completion gate passed: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
