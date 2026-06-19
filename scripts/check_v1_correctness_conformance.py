#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the v1 correctness and conformance closeout evidence.

This is an aggregate gate over the checked-in v1 correctness matrix and existing
reports. It does not execute runtime work, invoke external engines, publish
packages, create tags, or make release claims. Producer jobs must generate the
input reports first.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, load_json, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_correctness_conformance_report.v1"
MATRIX_SCHEMA_VERSION = "shardloom.v1_correctness_conformance_matrix.v1"
GATE_ID = "prod-v1-2b.correctness_conformance"
DEFAULT_MATRIX = Path("docs/release/v1-correctness-conformance-matrix.json")

EXPECTED_FRONT_DOOR_SUPPORTED_ROWS = 7
EXPECTED_FRONT_DOOR_PENDING_ROWS = 4
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

EXPECTED_VORTEX_PRIMITIVE_ROUTES = 11
EXPECTED_VORTEX_LOCAL_FILE_ROUTES = 15
EXPECTED_SOURCE_INPUT_FORMATS = 6
EXPECTED_SOURCE_PREPARED_ROUTE_IDS = 4
EXPECTED_SOURCE_DIRECT_ROUTE_IDS = 1
EXPECTED_SOURCE_GENERATED_ROUTE_IDS = 1
EXPECTED_SOURCE_INVALIDATION_CASES = 9
EXPECTED_OUTPUT_FORMATS = 7
EXPECTED_OUTPUT_WRITE_METHODS = 9
EXPECTED_OUTPUT_ROUTE_IDS = 7
EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS = 113
EXPECTED_EXAMPLE_REPLAY_DOC_SOURCES = 6
EXPECTED_EXAMPLE_REPLAY_RUNTIME_COMMANDS = 3
EXPECTED_EXAMPLE_REPLAY_SCENARIOS = len(EXPECTED_EXAMPLE_SCENARIOS)
EXPECTED_EXAMPLE_REPLAY_ERROR_SCENARIOS = len(EXPECTED_ERROR_SCENARIOS)
EXPECTED_EXAMPLE_REPLAY_UNSUPPORTED_FIXTURES = 1

EXPECTED_GOLDEN_WORKFLOWS = {
    "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output",
    "generated_source_to_local_vortex_output_replay_fidelity",
    "prepared_native_vortex_count_filter_project_execution_certificates",
}
EXPECTED_GOLDEN_STAGE_COUNT_MIN = 9

EXPECTED_EXECUTABLE_FIXTURES = 117
EXPECTED_DIAGNOSTIC_CASES = 25
EXPECTED_UNSUPPORTED_DIAGNOSTICS = 23
EXPECTED_RUNTIME_ERROR_DIAGNOSTICS = 1
EXPECTED_INVALID_SHAPE_DIAGNOSTICS = 1
EXPECTED_PROPERTY_LANE_COUNT = 10
EXPECTED_DETERMINISTIC_FUZZ_CASES = 5
EXPECTED_ADMITTED_STAGE_COUNT_MIN = 144
EXPECTED_ADMITTED_VALIDATOR_CASES = EXPECTED_EXECUTABLE_FIXTURES + EXPECTED_DIAGNOSTIC_CASES
EXPECTED_ADMITTED_REQUIRED_RUNTIME_ROWS = EXPECTED_ADMITTED_VALIDATOR_CASES
EXPECTED_ADMITTED_SUPPORT_REPORT_ROWS = 2
EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS = EXPECTED_DIAGNOSTIC_CASES
ADMITTED_ARTIFACT_REF_PREFIX = "target/admitted-semantics-matrix/artifacts/"
SEMANTIC_EXPECTED_OUTPUT_DIGEST_SOURCES = {
    "decoded_reference_result_jsonl",
    "decoded_reference_output_artifact",
}
SEMANTIC_OBSERVED_OUTPUT_DIGEST_SOURCES = {
    "envelope_result_jsonl",
    "sink_output_artifact",
}
UNSUPPORTED_STAGE_KINDS = {
    "unsupported_diagnostic",
    "runtime_error_diagnostic",
    "invalid_shape_diagnostic",
}
REQUIRED_SEMANTIC_CASE_IDS = {
    "numeric_generic_property_seed_20260521",
    "filter_project_limit_property_seed_20260618",
    "join_property_seed_20260619",
    "aggregate_topn_property_seed_20260620",
    "in_subquery_property_seed_20260621",
    "string_function_property_seed_20260622",
    "temporal_property_seed_20260623",
    "decimal_property_seed_20260624",
    "binary_property_seed_20260625",
    "output_jsonl_property_seed_20260626",
    "try_cast_projection_null_on_invalid",
    "string_transform_length_utf8",
    "regex_predicate_utf8",
    "like_predicate_utf8",
    "like_escape_predicate_utf8",
    "temporal_extract_utc_date32_timestamp",
    "null_coalesce_nullif",
    "predicate_projection_three_valued",
    "null_safe_comparison_predicate_semantics",
    "order_by_explicit_null_ordering",
    "subquery_predicate_projection_semantics",
    "aggregate_having_output_rows",
    "string_function_composition_utf8",
    "temporal_arithmetic_difference_utc",
    "interval_literal_temporal_arithmetic",
    "conditional_projection_case_when",
    "binary_hex_literal_projection",
    "binary_text_literal_projection",
    "binary_cast_projection_predicate",
    "binary_cast_ordering_predicate",
    "decimal_cast_projection_predicate",
    "decimal_arithmetic_projection",
    "in_subquery_scalar_semantics",
    "row_value_in_subquery_semantics",
    "exists_subquery_semantics",
    "quantified_subquery_semantics",
    "join_multi_key_expression_condition",
    "join_scalar_expression_condition",
    "join_logical_or_condition",
    "distinct_count_grouped",
    "select_distinct_projection",
    "window_rank_offset_distribution",
    "sql_parser_surface_fuzz_seed_20260613",
    "expression_parser_fuzz_seed_20260614",
    "route_selection_join_fuzz_seed_20260615",
    "route_selection_aggregate_topn_fuzz_seed_20260616",
    "output_writer_policy_fuzz_seed_20260617",
}
REQUIRED_UNSUPPORTED_CASE_IDS = {
    "unsupported_timezone_database_policy",
    "unsupported_timestamptz_policy",
    "unsupported_locale_collation",
    "unsupported_list_array_access_cast",
    "unsupported_struct_access_cast",
    "unsupported_complex_join_key",
    "unsupported_variant_access",
    "unsupported_union_dtype_cast",
    "invalid_shape_scalar_multi_column_in_subquery",
    "runtime_error_numeric_division_by_zero",
    "unsupported_output_no_overwrite_policy",
}
REQUIRED_FUZZ_CASE_IDS = {
    "sql_parser_surface_fuzz_seed_20260613",
    "expression_parser_fuzz_seed_20260614",
    "route_selection_join_fuzz_seed_20260615",
    "route_selection_aggregate_topn_fuzz_seed_20260616",
    "output_writer_policy_fuzz_seed_20260617",
}
REQUIRED_PROPERTY_CASE_IDS = {
    "numeric_generic_property_seed_20260521",
    "filter_project_limit_property_seed_20260618",
    "join_property_seed_20260619",
    "aggregate_topn_property_seed_20260620",
    "in_subquery_property_seed_20260621",
    "string_function_property_seed_20260622",
    "temporal_property_seed_20260623",
    "decimal_property_seed_20260624",
    "binary_property_seed_20260625",
    "output_jsonl_property_seed_20260626",
}
REQUIRED_SOURCE_INVALIDATION_CASE_IDS = {
    "cold_prepare_no_manifest",
    "warm_reuse_manifest_match",
    "source_changed",
    "artifact_changed",
    "schema_changed",
    "policy_changed",
    "version_changed",
    "missing_artifact",
    "corrupted_manifest",
}

REQUIRED_OPERATION_COVERAGE_ROWS = {
    "selective_filter": {
        "semantic_case_ids": (
            "predicate_projection_three_valued",
            "null_safe_comparison_predicate_semantics",
            "filter_project_limit_property_seed_20260618",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "filter", "select", "limit", "collect"),
    },
    "filter_projection_limit": {
        "semantic_case_ids": (
            "predicate_projection_three_valued",
            "order_by_explicit_null_ordering",
            "filter_project_limit_property_seed_20260618",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "filter", "select", "limit", "collect"),
    },
    "group_by_aggregation": {
        "semantic_case_ids": (
            "aggregate_having_output_rows",
            "distinct_count_grouped",
            "aggregate_topn_property_seed_20260620",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "filter", "group_by", "agg", "limit", "collect"),
    },
    "hash_join": {
        "semantic_case_ids": (
            "join_scalar_expression_condition",
            "join_multi_key_expression_condition",
            "join_property_seed_20260619",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "join", "select", "limit", "collect"),
    },
    "global_top_n": {
        "semantic_case_ids": (
            "order_by_explicit_null_ordering",
            "filter_project_limit_property_seed_20260618",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "select", "nlargest", "collect"),
    },
    "clean_cast_filter_write": {
        "semantic_case_ids": (
            "try_cast_projection_null_on_invalid",
            "decimal_cast_projection_predicate",
            "output_writer_policy_fuzz_seed_20260617",
            "decimal_property_seed_20260624",
            "output_jsonl_property_seed_20260626",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "with_column", "filter", "limit", "write_vortex"),
    },
    "malformed_timestamp_cast": {
        "semantic_case_ids": (
            "try_cast_projection_null_on_invalid",
            "temporal_property_seed_20260623",
        ),
        "unsupported_case_ids": (
            "unsupported_timezone_database_policy",
            "unsupported_timestamptz_policy",
        ),
        "python_methods": ("read_csv", "with_column", "limit", "collect"),
    },
    "null_heavy_aggregate": {
        "semantic_case_ids": (
            "null_coalesce_nullif",
            "distinct_count_grouped",
            "aggregate_having_output_rows",
            "aggregate_topn_property_seed_20260620",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_csv", "dropna", "group_by", "agg", "limit", "collect"),
    },
    "nested_json_field_scan": {
        "semantic_case_ids": (
            "string_transform_length_utf8",
            "like_predicate_utf8",
            "string_function_property_seed_20260622",
        ),
        "unsupported_case_ids": (),
        "python_methods": ("read_json", "filter", "select", "limit", "collect"),
    },
}

REQUIRED_OPERATION_COVERAGE_ROW_COUNT = len(REQUIRED_OPERATION_COVERAGE_ROWS)
REQUIRED_OPERATION_SEMANTIC_LINK_COUNT = sum(
    len(row["semantic_case_ids"]) for row in REQUIRED_OPERATION_COVERAGE_ROWS.values()
)
REQUIRED_OPERATION_UNSUPPORTED_LINK_COUNT = sum(
    len(row["unsupported_case_ids"]) for row in REQUIRED_OPERATION_COVERAGE_ROWS.values()
)
REQUIRED_OPERATION_PYTHON_METHOD_LINK_COUNT = sum(
    len(row["python_methods"]) for row in REQUIRED_OPERATION_COVERAGE_ROWS.values()
)
REQUIRED_OPERATION_UNIQUE_PYTHON_METHOD_COUNT = len(
    {
        method
        for row in REQUIRED_OPERATION_COVERAGE_ROWS.values()
        for method in row["python_methods"]
    }
)

FALSE_REPORT_FIELDS = (
    "public_release_claim_allowed",
    "public_package_claim_allowed",
    "performance_claim_allowed",
    "production_claim_allowed",
    "spark_replacement_claim_allowed",
    "publication_attempted",
    "tag_created",
    "package_upload_attempted",
    "fallback_attempted",
    "external_engine_invoked",
)


@dataclass(frozen=True)
class ReportPaths:
    golden_workflow: Path = Path("target/golden-workflow-report.json")
    admitted_semantics: Path = Path("target/admitted-semantics-matrix-report.json")
    front_door: Path = Path("target/v1-front-door-runtime-scope-report.json")
    vortex_runtime: Path = Path("target/v1-vortex-runtime-scope-report.json")
    source_prepared_state: Path = Path("target/v1-source-prepared-state-scope-report.json")
    local_output_sink: Path = Path("target/v1-local-output-sink-scope-report.json")
    python_user_surface: Path = Path("target/python-user-surface-completion-gate.json")
    example_replay: Path = Path("target/v1-example-replay-report.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--golden-workflow-report", type=Path, default=ReportPaths.golden_workflow)
    parser.add_argument(
        "--admitted-semantics-report",
        type=Path,
        default=ReportPaths.admitted_semantics,
    )
    parser.add_argument("--front-door-report", type=Path, default=ReportPaths.front_door)
    parser.add_argument("--vortex-runtime-report", type=Path, default=ReportPaths.vortex_runtime)
    parser.add_argument(
        "--source-prepared-state-report",
        type=Path,
        default=ReportPaths.source_prepared_state,
    )
    parser.add_argument(
        "--local-output-sink-report",
        type=Path,
        default=ReportPaths.local_output_sink,
    )
    parser.add_argument(
        "--python-user-surface-report",
        type=Path,
        default=ReportPaths.python_user_surface,
    )
    parser.add_argument(
        "--example-replay-report",
        type=Path,
        default=ReportPaths.example_replay,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-correctness-conformance-report.json"),
    )
    return parser.parse_args()


def _load_report(repo_root: Path, path: Path) -> tuple[dict[str, Any] | None, list[str], str]:
    resolved = resolve_path(repo_root, path)
    if not resolved.exists():
        return None, [f"missing report: {path.as_posix()}"], path.as_posix()
    payload = load_json(resolved)
    if not isinstance(payload, dict):
        return None, [f"{path.as_posix()}: report is not an object"], path.as_posix()
    return payload, [], path.as_posix()


def _load_matrix(repo_root: Path, path: Path) -> tuple[dict[str, Any] | None, list[str], str]:
    resolved = resolve_path(repo_root, path)
    if not resolved.exists():
        return None, [f"missing matrix: {path.as_posix()}"], path.as_posix()
    payload = load_json(resolved)
    if not isinstance(payload, dict):
        return None, [f"{path.as_posix()}: matrix is not an object"], path.as_posix()
    return payload, [], path.as_posix()


def _matrix_set(payload: dict[str, Any], field: str) -> set[str]:
    value = payload.get(field, [])
    return {str(item) for item in value} if isinstance(value, list) else set()


def _validate_matrix(matrix: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if matrix.get("schema_version") != MATRIX_SCHEMA_VERSION:
        blockers.append(
            "matrix schema_version="
            + str(matrix.get("schema_version", "missing"))
        )
    if matrix.get("matrix_id") != GATE_ID:
        blockers.append("matrix matrix_id=" + str(matrix.get("matrix_id", "missing")))
    if matrix.get("status") != "v1_correctness_scope_declared":
        blockers.append("matrix status=" + str(matrix.get("status", "missing")))
    for field in [
        "public_release_claim_allowed",
        "public_package_claim_allowed",
        "performance_claim_allowed",
        "production_claim_allowed",
        "spark_replacement_claim_allowed",
        "runtime_execution",
        "publication_attempted",
        "tag_created",
        "package_upload_attempted",
        "fallback_attempted",
        "external_engine_invoked",
    ]:
        if matrix.get(field) is not False:
            blockers.append(f"matrix {field} must be false")
    if matrix.get("correctness_claim_requires_report") is not True:
        blockers.append("matrix correctness_claim_requires_report must be true")
    if matrix.get("external_engines_allowed_as_oracles_only") is not True:
        blockers.append("matrix external_engines_allowed_as_oracles_only must be true")
    if matrix.get("external_oracle_required_for_v1") is not False:
        blockers.append("matrix external_oracle_required_for_v1 must be false")

    expected_counts = matrix.get("expected_counts")
    if not isinstance(expected_counts, dict):
        blockers.append("matrix expected_counts must be an object")
        expected_counts = {}
    for field, expected in {
        "front_door_supported_rows": EXPECTED_FRONT_DOOR_SUPPORTED_ROWS,
        "front_door_pending_rows": EXPECTED_FRONT_DOOR_PENDING_ROWS,
        "front_door_example_scenarios": len(EXPECTED_EXAMPLE_SCENARIOS),
        "front_door_expected_error_scenarios": len(EXPECTED_ERROR_SCENARIOS),
        "vortex_primitive_routes": EXPECTED_VORTEX_PRIMITIVE_ROUTES,
        "vortex_local_file_routes": EXPECTED_VORTEX_LOCAL_FILE_ROUTES,
        "source_input_formats": EXPECTED_SOURCE_INPUT_FORMATS,
        "source_prepared_routes": EXPECTED_SOURCE_PREPARED_ROUTE_IDS,
        "source_direct_routes": EXPECTED_SOURCE_DIRECT_ROUTE_IDS,
        "source_generated_routes": EXPECTED_SOURCE_GENERATED_ROUTE_IDS,
        "source_invalidation_cases": EXPECTED_SOURCE_INVALIDATION_CASES,
        "output_formats": EXPECTED_OUTPUT_FORMATS,
        "output_write_methods": EXPECTED_OUTPUT_WRITE_METHODS,
        "output_routes": EXPECTED_OUTPUT_ROUTE_IDS,
        "python_user_surface_method_rows": EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS,
        "example_replay_doc_sources": EXPECTED_EXAMPLE_REPLAY_DOC_SOURCES,
        "example_replay_runtime_commands": EXPECTED_EXAMPLE_REPLAY_RUNTIME_COMMANDS,
        "example_replay_scenarios": EXPECTED_EXAMPLE_REPLAY_SCENARIOS,
        "example_replay_expected_error_scenarios": EXPECTED_EXAMPLE_REPLAY_ERROR_SCENARIOS,
        "example_replay_unsupported_failure_fixtures": (
            EXPECTED_EXAMPLE_REPLAY_UNSUPPORTED_FIXTURES
        ),
        "golden_workflows": len(EXPECTED_GOLDEN_WORKFLOWS),
        "golden_stage_count_min": EXPECTED_GOLDEN_STAGE_COUNT_MIN,
        "executable_fixtures": EXPECTED_EXECUTABLE_FIXTURES,
        "diagnostic_cases": EXPECTED_DIAGNOSTIC_CASES,
        "unsupported_diagnostics": EXPECTED_UNSUPPORTED_DIAGNOSTICS,
        "runtime_error_diagnostics": EXPECTED_RUNTIME_ERROR_DIAGNOSTICS,
        "invalid_shape_diagnostics": EXPECTED_INVALID_SHAPE_DIAGNOSTICS,
        "property_lanes": EXPECTED_PROPERTY_LANE_COUNT,
        "deterministic_fuzz_cases": EXPECTED_DETERMINISTIC_FUZZ_CASES,
        "admitted_stage_count_min": EXPECTED_ADMITTED_STAGE_COUNT_MIN,
        "admitted_validator_cases": EXPECTED_ADMITTED_VALIDATOR_CASES,
        "admitted_required_runtime_rows": EXPECTED_ADMITTED_REQUIRED_RUNTIME_ROWS,
        "admitted_support_report_rows": EXPECTED_ADMITTED_SUPPORT_REPORT_ROWS,
        "admitted_deterministic_unsupported_rows": EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS,
    }.items():
        if expected_counts.get(field) != expected:
            blockers.append(f"matrix expected_counts.{field}={expected_counts.get(field)}")

    for field, expected in [
        ("front_door_example_scenario_ids", EXPECTED_EXAMPLE_SCENARIOS),
        ("front_door_expected_error_scenario_ids", EXPECTED_ERROR_SCENARIOS),
        ("golden_workflow_ids", EXPECTED_GOLDEN_WORKFLOWS),
        ("required_semantic_case_ids", REQUIRED_SEMANTIC_CASE_IDS),
        ("required_unsupported_case_ids", REQUIRED_UNSUPPORTED_CASE_IDS),
    ]:
        observed = _matrix_set(matrix, field)
        if observed != expected:
            blockers.append(
                f"matrix {field} mismatch: missing={sorted(expected - observed)} "
                + f"extra={sorted(observed - expected)}"
            )

    expected_inputs = {
        "golden_workflow": {
            "path": "target/golden-workflow-report.json",
            "schema_version": "shardloom.golden_workflow_validation_report.v1",
            "required_status": "passed",
        },
        "admitted_semantics": {
            "path": "target/admitted-semantics-matrix-report.json",
            "schema_version": "shardloom.admitted_semantics_matrix_report.v1",
            "required_status": "passed",
        },
        "front_door": {
            "path": "target/v1-front-door-runtime-scope-report.json",
            "schema_version": "shardloom.v1_front_door_runtime_scope_report.v1",
            "required_status": "passed",
        },
        "vortex_runtime": {
            "path": "target/v1-vortex-runtime-scope-report.json",
            "schema_version": "shardloom.v1_vortex_runtime_scope_report.v1",
            "required_status": "passed",
        },
        "source_prepared_state": {
            "path": "target/v1-source-prepared-state-scope-report.json",
            "schema_version": "shardloom.v1_source_prepared_state_scope_report.v1",
            "required_status": "passed",
        },
        "local_output_sink": {
            "path": "target/v1-local-output-sink-scope-report.json",
            "schema_version": "shardloom.v1_local_output_sink_scope_report.v1",
            "required_status": "passed",
        },
        "python_user_surface": {
            "path": "target/python-user-surface-completion-gate.json",
            "schema_version": "shardloom.python_user_surface_completion_gate.v1",
            "required_status": "passed",
        },
        "example_replay": {
            "path": "target/v1-example-replay-report.json",
            "schema_version": "shardloom.v1_example_replay_report.v1",
            "required_status": "passed",
        },
    }
    report_inputs = matrix.get("report_inputs")
    if not isinstance(report_inputs, list):
        blockers.append("matrix report_inputs must be a list")
        report_inputs = []
    observed_inputs = {
        str(row.get("report_id")): {
            "path": str(row.get("path")),
            "schema_version": str(row.get("schema_version")),
            "required_status": str(row.get("required_status")),
        }
        for row in report_inputs
        if isinstance(row, dict)
    }
    if observed_inputs != expected_inputs:
        blockers.append(
            "matrix report_inputs mismatch: "
            + f"observed={observed_inputs}"
        )

    residual_gaps = matrix.get("residual_gap_dispositions")
    if not isinstance(residual_gaps, list):
        blockers.append("matrix residual_gap_dispositions must be a list")
        residual_gaps = []
    expected_gap_ids = {
        "broad_ansi_subquery_parity_beyond_admitted_v1_scope",
        "external_oracle_result_artifact_population",
        "general_fuzz_beyond_deterministic_v1_property_fuzz_lanes",
    }
    observed_gap_ids = {
        str(row.get("gap_id"))
        for row in residual_gaps
        if isinstance(row, dict)
    }
    if observed_gap_ids != expected_gap_ids:
        blockers.append(
            "matrix residual_gap_dispositions mismatch: "
            + f"missing={sorted(expected_gap_ids - observed_gap_ids)} "
            + f"extra={sorted(observed_gap_ids - expected_gap_ids)}"
        )
    for row in residual_gaps:
        if not isinstance(row, dict):
            continue
        if row.get("v1_closeout_status") not in {
            "outside_declared_v1_scope",
            "not_required_for_current_v1_correctness_claim",
        }:
            blockers.append(
                "matrix residual gap "
                + str(row.get("gap_id", "missing"))
                + " has invalid v1_closeout_status"
            )
        if not row.get("reason"):
            blockers.append(
                "matrix residual gap "
                + str(row.get("gap_id", "missing"))
                + " missing reason"
            )

    return {
        "schema_version": matrix.get("schema_version"),
        "matrix_id": matrix.get("matrix_id"),
        "expected_count_field_count": len(expected_counts),
        "required_semantic_case_count": len(_matrix_set(matrix, "required_semantic_case_ids")),
        "required_unsupported_case_count": len(
            _matrix_set(matrix, "required_unsupported_case_ids")
        ),
        "report_input_count": len(observed_inputs),
        "residual_gap_count": len(observed_gap_ids),
    }, blockers


def _expect_false(
    payload: dict[str, Any],
    label: str,
    fields: tuple[str, ...] = FALSE_REPORT_FIELDS,
) -> list[str]:
    blockers: list[str] = []
    for field in fields:
        if field in payload and payload.get(field) is not False:
            blockers.append(f"{label}: {field} must be false")
    return blockers


def _expect_status(payload: dict[str, Any], label: str, schema_version: str) -> list[str]:
    blockers: list[str] = []
    if payload.get("schema_version") != schema_version:
        blockers.append(f"{label}: schema_version={payload.get('schema_version', 'missing')}")
    if payload.get("status") != "passed":
        blockers.extend(payload.get("blockers", [f"{label}: status is not passed"]))
    blockers.extend(_expect_false(payload, label))
    return blockers


def _missing(observed: Any, expected: set[str]) -> list[str]:
    return sorted(expected - {str(value) for value in observed or []})


def _is_hex_digest(value: Any, prefix: str, hex_chars: int) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    digest = value[len(prefix) :]
    return len(digest) == hex_chars and all(char in "0123456789abcdef" for char in digest)


def _expected_stage_artifact_ref(case_id: str) -> str:
    return ADMITTED_ARTIFACT_REF_PREFIX + case_id + ".json"


def _validate_stage_contract(
    stage: dict[str, Any],
    case_id: str,
    *,
    require_semantic_digests: bool,
    require_diagnostic_fields: bool,
) -> list[str]:
    blockers: list[str] = []
    if stage.get("status") != "passed":
        blockers.append(f"admitted_semantics: {case_id} stage status must be passed")
    if stage.get("artifact_ref") != _expected_stage_artifact_ref(case_id):
        blockers.append(f"admitted_semantics: {case_id} missing artifact_ref")
    if stage.get("fallback_attempted") is not False:
        blockers.append(f"admitted_semantics: {case_id} fallback_attempted must be false")
    if stage.get("external_engine_invoked") is not False:
        blockers.append(
            f"admitted_semantics: {case_id} external_engine_invoked must be false"
        )
    if stage.get("blockers") != []:
        blockers.append(f"admitted_semantics: {case_id} blockers must be empty")
    if require_semantic_digests:
        if not _is_hex_digest(stage.get("decoded_reference_digest"), "sha256:", 64):
            blockers.append(
                f"admitted_semantics: {case_id} decoded_reference_digest must be sha256-prefixed"
            )
        if not _is_hex_digest(stage.get("correctness_digest"), "fnv64:", 16):
            blockers.append(
                f"admitted_semantics: {case_id} correctness_digest must be fnv64-prefixed"
            )
        if not _is_hex_digest(stage.get("result_digest"), "fnv64:", 16):
            blockers.append(
                f"admitted_semantics: {case_id} result_digest must be fnv64-prefixed"
            )
        if not _is_hex_digest(stage.get("expected_output_digest"), "sha256:", 64):
            blockers.append(
                f"admitted_semantics: {case_id} expected_output_digest must be sha256-prefixed"
            )
        if not _is_hex_digest(stage.get("observed_output_digest"), "sha256:", 64):
            blockers.append(
                f"admitted_semantics: {case_id} observed_output_digest must be sha256-prefixed"
            )
        if (
            _is_hex_digest(stage.get("expected_output_digest"), "sha256:", 64)
            and _is_hex_digest(stage.get("observed_output_digest"), "sha256:", 64)
            and stage.get("expected_output_digest") != stage.get("observed_output_digest")
        ):
            blockers.append(
                f"admitted_semantics: {case_id} expected/observed output digests must match"
            )
        if (
            stage.get("expected_output_digest_source")
            not in SEMANTIC_EXPECTED_OUTPUT_DIGEST_SOURCES
        ):
            blockers.append(
                f"admitted_semantics: {case_id} expected_output_digest_source is invalid"
            )
        if (
            stage.get("observed_output_digest_source")
            not in SEMANTIC_OBSERVED_OUTPUT_DIGEST_SOURCES
        ):
            blockers.append(
                f"admitted_semantics: {case_id} observed_output_digest_source is invalid"
            )
    if require_diagnostic_fields:
        if stage.get("kind") not in UNSUPPORTED_STAGE_KINDS:
            blockers.append(f"admitted_semantics: {case_id} diagnostic kind is invalid")
        if not isinstance(stage.get("diagnostic_code"), str) or not stage.get("diagnostic_code"):
            blockers.append(f"admitted_semantics: {case_id} diagnostic_code is required")
        if (
            not isinstance(stage.get("diagnostic_fragment"), str)
            or not stage.get("diagnostic_fragment")
        ):
            blockers.append(f"admitted_semantics: {case_id} diagnostic_fragment is required")
    return blockers


def _validate_required_stage_evidence(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    stages = payload.get("stages")
    if not isinstance(stages, list):
        return {
            "semantic_fixture_evidence_status": "failed",
            "required_executable_stage_evidence_count": 0,
            "required_unsupported_stage_evidence_count": 0,
            "required_stage_artifact_ref_count": 0,
            "required_stage_decoded_reference_digest_count": 0,
            "required_stage_expected_output_digest_count": 0,
            "required_stage_observed_output_digest_count": 0,
            "required_stage_output_digest_match_count": 0,
            "required_stage_expected_output_digest_source_count": 0,
            "required_stage_observed_output_digest_source_count": 0,
            "required_stage_correctness_digest_count": 0,
            "required_stage_result_digest_count": 0,
            "required_unsupported_stage_diagnostic_field_count": 0,
            "required_stage_no_fallback_count": 0,
            "required_stage_no_external_engine_count": 0,
        }, ["admitted_semantics: stages must be a list"]

    stage_by_case_id: dict[str, dict[str, Any]] = {}
    for index, stage in enumerate(stages):
        if not isinstance(stage, dict):
            blockers.append(f"admitted_semantics: stages[{index}] must be an object")
            continue
        case_id = stage.get("case_id")
        if not isinstance(case_id, str) or not case_id:
            blockers.append(f"admitted_semantics: stages[{index}] missing case_id")
            continue
        if case_id in stage_by_case_id:
            blockers.append(f"admitted_semantics: duplicate stage case_id {case_id}")
            continue
        stage_by_case_id[case_id] = stage

    required_stage_ids = REQUIRED_SEMANTIC_CASE_IDS | REQUIRED_UNSUPPORTED_CASE_IDS
    executable_stage_count = 0
    unsupported_stage_count = 0
    artifact_ref_count = 0
    decoded_digest_count = 0
    expected_output_digest_count = 0
    observed_output_digest_count = 0
    output_digest_match_count = 0
    expected_output_digest_source_count = 0
    observed_output_digest_source_count = 0
    correctness_digest_count = 0
    result_digest_count = 0
    diagnostic_field_count = 0
    no_fallback_count = 0
    no_external_count = 0

    for case_id in sorted(REQUIRED_SEMANTIC_CASE_IDS):
        stage = stage_by_case_id.get(case_id)
        if stage is None:
            blockers.append(f"admitted_semantics: missing required stage {case_id}")
            continue
        executable_stage_count += 1
        blockers.extend(
            _validate_stage_contract(
                stage,
                case_id,
                require_semantic_digests=True,
                require_diagnostic_fields=False,
            )
        )

    for case_id in sorted(REQUIRED_UNSUPPORTED_CASE_IDS):
        stage = stage_by_case_id.get(case_id)
        if stage is None:
            blockers.append(f"admitted_semantics: missing required stage {case_id}")
            continue
        unsupported_stage_count += 1
        blockers.extend(
            _validate_stage_contract(
                stage,
                case_id,
                require_semantic_digests=False,
                require_diagnostic_fields=True,
            )
        )

    for case_id in sorted(required_stage_ids):
        stage = stage_by_case_id.get(case_id)
        if not isinstance(stage, dict):
            continue
        if stage.get("artifact_ref") == _expected_stage_artifact_ref(case_id):
            artifact_ref_count += 1
        if _is_hex_digest(stage.get("decoded_reference_digest"), "sha256:", 64):
            decoded_digest_count += 1
        if _is_hex_digest(stage.get("expected_output_digest"), "sha256:", 64):
            expected_output_digest_count += 1
        if _is_hex_digest(stage.get("observed_output_digest"), "sha256:", 64):
            observed_output_digest_count += 1
        if (
            _is_hex_digest(stage.get("expected_output_digest"), "sha256:", 64)
            and _is_hex_digest(stage.get("observed_output_digest"), "sha256:", 64)
            and stage.get("expected_output_digest") == stage.get("observed_output_digest")
        ):
            output_digest_match_count += 1
        if (
            stage.get("expected_output_digest_source")
            in SEMANTIC_EXPECTED_OUTPUT_DIGEST_SOURCES
        ):
            expected_output_digest_source_count += 1
        if (
            stage.get("observed_output_digest_source")
            in SEMANTIC_OBSERVED_OUTPUT_DIGEST_SOURCES
        ):
            observed_output_digest_source_count += 1
        if _is_hex_digest(stage.get("correctness_digest"), "fnv64:", 16):
            correctness_digest_count += 1
        if _is_hex_digest(stage.get("result_digest"), "fnv64:", 16):
            result_digest_count += 1
        if (
            stage.get("kind") in UNSUPPORTED_STAGE_KINDS
            and isinstance(stage.get("diagnostic_code"), str)
            and bool(stage.get("diagnostic_code"))
            and isinstance(stage.get("diagnostic_fragment"), str)
            and bool(stage.get("diagnostic_fragment"))
        ):
            diagnostic_field_count += 1
        if stage.get("fallback_attempted") is False:
            no_fallback_count += 1
        if stage.get("external_engine_invoked") is False:
            no_external_count += 1

    return {
        "semantic_fixture_evidence_status": "passed" if not blockers else "failed",
        "required_executable_stage_evidence_count": executable_stage_count,
        "required_unsupported_stage_evidence_count": unsupported_stage_count,
        "required_stage_artifact_ref_count": artifact_ref_count,
        "required_stage_decoded_reference_digest_count": decoded_digest_count,
        "required_stage_expected_output_digest_count": expected_output_digest_count,
        "required_stage_observed_output_digest_count": observed_output_digest_count,
        "required_stage_output_digest_match_count": output_digest_match_count,
        "required_stage_expected_output_digest_source_count": expected_output_digest_source_count,
        "required_stage_observed_output_digest_source_count": observed_output_digest_source_count,
        "required_stage_correctness_digest_count": correctness_digest_count,
        "required_stage_result_digest_count": result_digest_count,
        "required_unsupported_stage_diagnostic_field_count": diagnostic_field_count,
        "required_stage_no_fallback_count": no_fallback_count,
        "required_stage_no_external_engine_count": no_external_count,
    }, blockers


def _report_status(inputs: dict[str, dict[str, Any] | None], key: str) -> str:
    payload = inputs.get(key)
    return str(payload.get("status", "missing")) if isinstance(payload, dict) else "missing"


def _report_list(inputs: dict[str, dict[str, Any] | None], key: str, field: str) -> list[Any]:
    payload = inputs.get(key)
    if not isinstance(payload, dict):
        return []
    value = payload.get(field, [])
    return value if isinstance(value, list) else []


def _report_bool(inputs: dict[str, dict[str, Any] | None], key: str, field: str) -> bool:
    payload = inputs.get(key)
    return bool(payload.get(field)) if isinstance(payload, dict) else False


def _validate_front_door(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "front_door",
        "shardloom.v1_front_door_runtime_scope_report.v1",
    )
    if payload.get("scoped_local_front_door_parity_supported") is not True:
        blockers.append("front_door: scoped_local_front_door_parity_supported must be true")
    if len(payload.get("supported_parity_row_ids", [])) != EXPECTED_FRONT_DOOR_SUPPORTED_ROWS:
        blockers.append("front_door: supported_parity_row_ids count mismatch")
    if len(payload.get("broad_pending_parity_row_ids", [])) != EXPECTED_FRONT_DOOR_PENDING_ROWS:
        blockers.append("front_door: broad_pending_parity_row_ids count mismatch")
    missing_scenarios = _missing(payload.get("example_scenario_ids"), EXPECTED_EXAMPLE_SCENARIOS)
    if missing_scenarios:
        blockers.append("front_door: missing example scenarios " + ",".join(missing_scenarios))
    missing_errors = _missing(payload.get("expected_error_scenario_ids"), EXPECTED_ERROR_SCENARIOS)
    if missing_errors:
        blockers.append("front_door: missing expected error scenarios " + ",".join(missing_errors))
    if payload.get("all_no_fallback_no_external_engine") is not True:
        blockers.append("front_door: all_no_fallback_no_external_engine must be true")
    if payload.get("performance_equivalence_claim_allowed") is not False:
        blockers.append("front_door: performance_equivalence_claim_allowed must be false")
    return {
        "supported_parity_row_count": len(payload.get("supported_parity_row_ids", [])),
        "broad_pending_parity_row_count": len(payload.get("broad_pending_parity_row_ids", [])),
        "example_scenario_count": len(payload.get("example_scenario_ids", [])),
        "expected_error_scenario_count": len(payload.get("expected_error_scenario_ids", [])),
    }, blockers


def _validate_vortex(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "vortex_runtime",
        "shardloom.v1_vortex_runtime_scope_report.v1",
    )
    for field, expected in [
        ("local_vortex_primitive_route_count", EXPECTED_VORTEX_PRIMITIVE_ROUTES),
        ("local_file_benchmark_route_count", EXPECTED_VORTEX_LOCAL_FILE_ROUTES),
    ]:
        if payload.get(field) != expected:
            blockers.append(f"vortex_runtime: {field}={payload.get(field, 'missing')}")
    if payload.get("local_vortex_primitive_v1_scope_ready") is not True:
        blockers.append("vortex_runtime: local_vortex_primitive_v1_scope_ready must be true")
    if payload.get("user_route_v1_vortex_scope_ready") is not True:
        blockers.append("vortex_runtime: user_route_v1_vortex_scope_ready must be true")
    return {
        "primitive_route_count": payload.get("local_vortex_primitive_route_count"),
        "local_file_benchmark_route_count": payload.get("local_file_benchmark_route_count"),
    }, blockers


def _validate_source(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "source_prepared_state",
        "shardloom.v1_source_prepared_state_scope_report.v1",
    )
    expected_counts = {
        "supported_input_formats": EXPECTED_SOURCE_INPUT_FORMATS,
        "prepared_route_ids": EXPECTED_SOURCE_PREPARED_ROUTE_IDS,
        "direct_transient_route_ids": EXPECTED_SOURCE_DIRECT_ROUTE_IDS,
        "generated_route_ids": EXPECTED_SOURCE_GENERATED_ROUTE_IDS,
        "invalidation_case_ids": EXPECTED_SOURCE_INVALIDATION_CASES,
    }
    for field, expected in expected_counts.items():
        if len(payload.get(field, [])) != expected:
            blockers.append(f"source_prepared_state: {field} count mismatch")
    observed_invalidation = {
        str(value) for value in payload.get("invalidation_case_ids", [])
    }
    if observed_invalidation != REQUIRED_SOURCE_INVALIDATION_CASE_IDS:
        blockers.append(
            "source_prepared_state: invalidation_case_ids mismatch: "
            + f"missing={sorted(REQUIRED_SOURCE_INVALIDATION_CASE_IDS - observed_invalidation)} "
            + f"extra={sorted(observed_invalidation - REQUIRED_SOURCE_INVALIDATION_CASE_IDS)}"
        )
    if payload.get("source_prepared_benchmark_required_fields_ready") is not True:
        blockers.append("source_prepared_state: benchmark required fields must be ready")
    return {
        "supported_input_format_count": len(payload.get("supported_input_formats", [])),
        "prepared_route_count": len(payload.get("prepared_route_ids", [])),
        "invalidation_case_count": len(payload.get("invalidation_case_ids", [])),
        "invalidation_case_ids": sorted(observed_invalidation),
        "benchmark_rows_with_required_fields": payload.get(
            "source_prepared_benchmark_rows_with_required_fields"
        ),
    }, blockers


def _validate_output(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "local_output_sink",
        "shardloom.v1_local_output_sink_scope_report.v1",
    )
    expected_counts = {
        "supported_output_formats": EXPECTED_OUTPUT_FORMATS,
        "user_write_methods": EXPECTED_OUTPUT_WRITE_METHODS,
        "output_route_ids": EXPECTED_OUTPUT_ROUTE_IDS,
    }
    for field, expected in expected_counts.items():
        if len(payload.get(field, [])) != expected:
            blockers.append(f"local_output_sink: {field} count mismatch")
    if payload.get("local_output_sink_benchmark_required_fields_ready") is not True:
        blockers.append("local_output_sink: benchmark required fields must be ready")
    if payload.get("local_output_sink_benchmark_replay_ready") is not True:
        blockers.append("local_output_sink: benchmark replay must be ready")
    return {
        "supported_output_format_count": len(payload.get("supported_output_formats", [])),
        "write_method_count": len(payload.get("user_write_methods", [])),
        "output_route_count": len(payload.get("output_route_ids", [])),
        "benchmark_rows_with_required_fields": payload.get(
            "local_output_sink_benchmark_rows_with_required_fields"
        ),
    }, blockers


def _validate_python_user_surface(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "python_user_surface",
        "shardloom.python_user_surface_completion_gate.v1",
    )
    if payload.get("scoped_python_front_door_claim_allowed") is not True:
        blockers.append("python_user_surface: scoped_python_front_door_claim_allowed must be true")
    if payload.get("method_matrix_row_count") != EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS:
        blockers.append(
            "python_user_surface: method_matrix_row_count="
            + str(payload.get("method_matrix_row_count", "missing"))
        )
    rows = payload.get("method_matrix_rows", [])
    if not isinstance(rows, list):
        blockers.append("python_user_surface: method_matrix_rows must be a list")
        rows = []
    if len(rows) != EXPECTED_PYTHON_USER_SURFACE_METHOD_ROWS:
        blockers.append(
            "python_user_surface: method_matrix_rows count="
            + str(len(rows))
        )
    method_rows = {
        str(row.get("method")): row
        for row in rows
        if isinstance(row, dict) and row.get("method")
    }
    required_methods = {
        method
        for row in REQUIRED_OPERATION_COVERAGE_ROWS.values()
        for method in row["python_methods"]
    }
    missing_methods = sorted(required_methods - method_rows.keys())
    if missing_methods:
        blockers.append(
            "python_user_surface: missing required operation methods "
            + ",".join(missing_methods)
        )
    for method in sorted(required_methods & method_rows.keys()):
        row = method_rows[method]
        if row.get("fallback_attempted") is not False:
            blockers.append(f"python_user_surface: {method} fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(
                f"python_user_surface: {method} external_engine_invoked must be false"
            )
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(
                f"python_user_surface: {method} claim_gate_status="
                + str(row.get("claim_gate_status", "missing"))
            )
        if not str(row.get("support_status", "")).strip():
            blockers.append(f"python_user_surface: {method} support_status is required")
    return {
        "method_matrix_row_count": payload.get("method_matrix_row_count"),
        "method_matrix_row_list_count": len(rows),
        "required_operation_method_count": len(required_methods),
        "required_operation_method_rows_present": len(required_methods & method_rows.keys()),
    }, blockers


def _validate_example_replay(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "example_replay",
        "shardloom.v1_example_replay_report.v1",
    )
    expected_values = {
        "docs_marker_source_count": EXPECTED_EXAMPLE_REPLAY_DOC_SOURCES,
        "runtime_command_count": EXPECTED_EXAMPLE_REPLAY_RUNTIME_COMMANDS,
        "golden_workflow_replay_verified_count": len(EXPECTED_GOLDEN_WORKFLOWS),
        "benchmark_scenario_count": EXPECTED_EXAMPLE_REPLAY_SCENARIOS,
        "benchmark_expected_error_scenario_count": (
            EXPECTED_EXAMPLE_REPLAY_ERROR_SCENARIOS
        ),
        "unsupported_failure_fixture_count": (
            EXPECTED_EXAMPLE_REPLAY_UNSUPPORTED_FIXTURES
        ),
    }
    for field, expected in expected_values.items():
        if payload.get(field) != expected:
            blockers.append(f"example_replay: {field}={payload.get(field, 'missing')}")
    for field in [
        "docs_marker_status",
        "runtime_command_status",
        "golden_workflow_replay_status",
        "docs_example_execution_status",
        "python_readme_example_execution_status",
        "website_example_execution_status",
        "quickstart_smoke_status",
        "benchmark_scenario_execution_status",
        "timing_review_status",
        "unsupported_failure_fixture_status",
    ]:
        if payload.get(field) != "passed":
            blockers.append(f"example_replay: {field}={payload.get(field, 'missing')}")
    if payload.get("all_no_fallback_no_external_engine") is not True:
        blockers.append("example_replay: all_no_fallback_no_external_engine must be true")
    if payload.get("correctness_claim_allowed") is not True:
        blockers.append("example_replay: correctness_claim_allowed must be true")
    if payload.get("runtime_support_claim_allowed") is not False:
        blockers.append("example_replay: runtime_support_claim_allowed must be false")
    if payload.get("claim_gate_status") != "not_claim_grade":
        blockers.append(
            "example_replay: claim_gate_status="
            + str(payload.get("claim_gate_status", "missing"))
        )
    observed_errors = {str(value) for value in payload.get("expected_error_scenario_ids", [])}
    if observed_errors != EXPECTED_ERROR_SCENARIOS:
        blockers.append(
            "example_replay: expected_error_scenario_ids mismatch: "
            + f"missing={sorted(EXPECTED_ERROR_SCENARIOS - observed_errors)} "
            + f"extra={sorted(observed_errors - EXPECTED_ERROR_SCENARIOS)}"
        )
    return {
        "docs_marker_source_count": payload.get("docs_marker_source_count"),
        "docs_marker_count": payload.get("docs_marker_count"),
        "docs_marker_pass_count": payload.get("docs_marker_pass_count"),
        "runtime_command_count": payload.get("runtime_command_count"),
        "golden_workflow_replay_verified_count": payload.get(
            "golden_workflow_replay_verified_count"
        ),
        "golden_workflow_stage_count": payload.get("golden_workflow_stage_count"),
        "benchmark_scenario_count": payload.get("benchmark_scenario_count"),
        "benchmark_expected_error_scenario_count": payload.get(
            "benchmark_expected_error_scenario_count"
        ),
        "unsupported_failure_fixture_count": payload.get(
            "unsupported_failure_fixture_count"
        ),
        "all_no_fallback_no_external_engine": payload.get(
            "all_no_fallback_no_external_engine"
        ),
    }, blockers


def _validate_golden(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "golden_workflow",
        "shardloom.golden_workflow_validation_report.v1",
    )
    if payload.get("workflow_count") != len(EXPECTED_GOLDEN_WORKFLOWS):
        blockers.append("golden_workflow: workflow_count mismatch")
    if (
        not isinstance(payload.get("stage_count"), int)
        or payload["stage_count"] < EXPECTED_GOLDEN_STAGE_COUNT_MIN
    ):
        blockers.append("golden_workflow: stage_count below v1 minimum")
    missing_workflows = _missing(payload.get("workflow_ids"), EXPECTED_GOLDEN_WORKFLOWS)
    if missing_workflows:
        blockers.append("golden_workflow: missing workflows " + ",".join(missing_workflows))
    if payload.get("support_matrix_status") != "passed":
        blockers.append("golden_workflow: support_matrix_status must be passed")
    return {
        "workflow_count": payload.get("workflow_count"),
        "stage_count": payload.get("stage_count"),
        "workflow_ids": payload.get("workflow_ids", []),
    }, blockers


def _validate_admitted(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers = _expect_status(
        payload,
        "admitted_semantics",
        "shardloom.admitted_semantics_matrix_report.v1",
    )
    expected_values = {
        "executable_fixture_count": EXPECTED_EXECUTABLE_FIXTURES,
        "diagnostic_case_count": EXPECTED_DIAGNOSTIC_CASES,
        "unsupported_diagnostic_count": EXPECTED_UNSUPPORTED_DIAGNOSTICS,
        "runtime_error_diagnostic_count": EXPECTED_RUNTIME_ERROR_DIAGNOSTICS,
        "invalid_shape_diagnostic_count": EXPECTED_INVALID_SHAPE_DIAGNOSTICS,
    }
    for field, expected in expected_values.items():
        if payload.get(field) != expected:
            blockers.append(f"admitted_semantics: {field}={payload.get(field, 'missing')}")
    scope_expected_values = {
        "remaining_matrix_gap_status": "passed",
        "v1_runtime_scope_status": "passed",
        "v1_expected_validator_case_count": EXPECTED_ADMITTED_VALIDATOR_CASES,
        "v1_required_runtime_row_count": EXPECTED_ADMITTED_REQUIRED_RUNTIME_ROWS,
        "v1_missing_validator_case_count": 0,
        "v1_unexpected_required_runtime_row_count": 0,
        "v1_support_report_row_count": EXPECTED_ADMITTED_SUPPORT_REPORT_ROWS,
        "deterministic_unsupported_scope_status": "passed",
        "deterministic_unsupported_row_count": EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS,
        "deterministic_unsupported_oracle_row_count": EXPECTED_DETERMINISTIC_UNSUPPORTED_ROWS,
    }
    for field, expected in scope_expected_values.items():
        if payload.get(field) != expected:
            blockers.append(f"admitted_semantics: {field}={payload.get(field, 'missing')}")
    if (
        not isinstance(payload.get("stage_count"), int)
        or payload["stage_count"] < EXPECTED_ADMITTED_STAGE_COUNT_MIN
    ):
        blockers.append("admitted_semantics: stage_count below v1 minimum")
    if payload.get("property_execution_performed") is not True:
        blockers.append("admitted_semantics: property_execution_performed must be true")
    if payload.get("property_lane_count") != EXPECTED_PROPERTY_LANE_COUNT:
        blockers.append(
            "admitted_semantics: property_lane_count="
            + str(payload.get("property_lane_count", "missing"))
        )
    missing_property_cases = _missing(
        payload.get("property_case_ids"),
        REQUIRED_PROPERTY_CASE_IDS,
    )
    if missing_property_cases:
        blockers.append(
            "admitted_semantics: missing deterministic property cases "
            + ",".join(missing_property_cases)
        )
    if payload.get("deterministic_fuzz_execution_performed") is not True:
        blockers.append(
            "admitted_semantics: deterministic_fuzz_execution_performed must be true"
        )
    if payload.get("deterministic_fuzz_case_count") != EXPECTED_DETERMINISTIC_FUZZ_CASES:
        blockers.append(
            "admitted_semantics: deterministic_fuzz_case_count="
            + str(payload.get("deterministic_fuzz_case_count", "missing"))
        )
    missing_fuzz_cases = _missing(payload.get("fuzz_case_ids"), REQUIRED_FUZZ_CASE_IDS)
    if missing_fuzz_cases:
        blockers.append(
            "admitted_semantics: missing deterministic fuzz cases "
            + ",".join(missing_fuzz_cases)
        )
    if payload.get("decoded_reference_differential_execution_performed") is not True:
        blockers.append(
            "admitted_semantics: decoded_reference_differential_execution_performed must be true"
        )
    if payload.get("semantic_conformance_suite_status") != "passed":
        blockers.append("admitted_semantics: semantic_conformance_suite_status must be passed")
    if payload.get("correctness_harness_boundary_status") != "passed":
        blockers.append("admitted_semantics: correctness_harness_boundary_status must be passed")
    missing_cases = _missing(payload.get("executable_case_ids"), REQUIRED_SEMANTIC_CASE_IDS)
    if missing_cases:
        blockers.append(
            "admitted_semantics: missing required executable cases "
            + ",".join(missing_cases)
        )
    observed_diagnostics = set(str(value) for value in payload.get("unsupported_case_ids", []))
    observed_diagnostics.update(str(value) for value in payload.get("runtime_error_case_ids", []))
    observed_diagnostics.update(str(value) for value in payload.get("invalid_shape_case_ids", []))
    missing_unsupported = sorted(REQUIRED_UNSUPPORTED_CASE_IDS - observed_diagnostics)
    if missing_unsupported:
        blockers.append(
            "admitted_semantics: missing required unsupported/error cases "
            + ",".join(missing_unsupported)
        )
    stage_summary, stage_blockers = _validate_required_stage_evidence(payload)
    blockers.extend(stage_blockers)
    return {
        "executable_fixture_count": payload.get("executable_fixture_count"),
        "diagnostic_case_count": payload.get("diagnostic_case_count"),
        "unsupported_diagnostic_count": payload.get("unsupported_diagnostic_count"),
        "property_lane_count": payload.get("property_lane_count"),
        "deterministic_fuzz_execution_performed": payload.get(
            "deterministic_fuzz_execution_performed"
        ),
        "property_case_ids": payload.get("property_case_ids", []),
        "deterministic_fuzz_case_count": payload.get("deterministic_fuzz_case_count"),
        "fuzz_case_ids": payload.get("fuzz_case_ids", []),
        "stage_count": payload.get("stage_count"),
        "remaining_matrix_gap_status": payload.get("remaining_matrix_gap_status"),
        "v1_runtime_scope_status": payload.get("v1_runtime_scope_status"),
        "v1_expected_validator_case_count": payload.get("v1_expected_validator_case_count"),
        "v1_required_runtime_row_count": payload.get("v1_required_runtime_row_count"),
        "v1_missing_validator_case_count": payload.get("v1_missing_validator_case_count"),
        "v1_unexpected_required_runtime_row_count": payload.get(
            "v1_unexpected_required_runtime_row_count"
        ),
        "v1_support_report_row_count": payload.get("v1_support_report_row_count"),
        "deterministic_unsupported_scope_status": payload.get(
            "deterministic_unsupported_scope_status"
        ),
        "deterministic_unsupported_row_count": payload.get(
            "deterministic_unsupported_row_count"
        ),
        "deterministic_unsupported_oracle_row_count": payload.get(
            "deterministic_unsupported_oracle_row_count"
        ),
        "required_semantic_case_count": len(REQUIRED_SEMANTIC_CASE_IDS),
        "required_unsupported_case_count": len(REQUIRED_UNSUPPORTED_CASE_IDS),
        "remaining_matrix_gaps": payload.get("remaining_matrix_gaps", []),
        **stage_summary,
    }, blockers


def _stage_by_case_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stages = payload.get("stages", [])
    return {
        str(stage.get("case_id")): stage
        for stage in stages
        if isinstance(stage, dict) and stage.get("case_id")
    }


def _python_method_rows(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("method_matrix_rows", [])
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("method")): row
        for row in rows
        if isinstance(row, dict) and row.get("method")
    }


def _semantic_stage_has_output_digest(stage: dict[str, Any]) -> bool:
    return (
        _is_hex_digest(stage.get("expected_output_digest"), "sha256:", 64)
        and _is_hex_digest(stage.get("observed_output_digest"), "sha256:", 64)
        and stage.get("expected_output_digest") == stage.get("observed_output_digest")
        and stage.get("expected_output_digest_source")
        in SEMANTIC_EXPECTED_OUTPUT_DIGEST_SOURCES
        and stage.get("observed_output_digest_source")
        in SEMANTIC_OBSERVED_OUTPUT_DIGEST_SOURCES
    )


def _unsupported_stage_has_diagnostic(stage: dict[str, Any]) -> bool:
    return (
        stage.get("kind") in UNSUPPORTED_STAGE_KINDS
        and isinstance(stage.get("diagnostic_code"), str)
        and bool(stage.get("diagnostic_code"))
        and isinstance(stage.get("diagnostic_fragment"), str)
        and bool(stage.get("diagnostic_fragment"))
    )


def _validate_operation_coverage(
    *,
    admitted_semantics: dict[str, Any] | None,
    front_door: dict[str, Any] | None,
    python_user_surface: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    front_door_scenarios = set(
        str(value)
        for value in (front_door or {}).get("example_scenario_ids", [])
    )
    expected_error_scenarios = set(
        str(value)
        for value in (front_door or {}).get("expected_error_scenario_ids", [])
    )
    stage_by_id = _stage_by_case_id(admitted_semantics or {})
    method_rows = _python_method_rows(python_user_surface or {})

    semantic_link_count = 0
    unsupported_link_count = 0
    python_method_link_count = 0
    output_digest_row_count = 0
    diagnostic_row_count = 0
    no_fallback_row_count = 0
    python_method_rows_present = 0

    for operation_id, spec in sorted(REQUIRED_OPERATION_COVERAGE_ROWS.items()):
        semantic_case_ids = tuple(spec["semantic_case_ids"])
        unsupported_case_ids = tuple(spec["unsupported_case_ids"])
        python_methods = tuple(spec["python_methods"])
        semantic_link_count += len(semantic_case_ids)
        unsupported_link_count += len(unsupported_case_ids)
        python_method_link_count += len(python_methods)

        row_blockers: list[str] = []
        if operation_id not in front_door_scenarios:
            row_blockers.append("missing front-door example scenario")
        if operation_id in EXPECTED_ERROR_SCENARIOS and operation_id not in expected_error_scenarios:
            row_blockers.append("missing expected-error scenario marker")
        if operation_id not in EXPECTED_ERROR_SCENARIOS and operation_id in expected_error_scenarios:
            row_blockers.append("unexpected expected-error scenario marker")

        semantic_stages = [stage_by_id.get(case_id) for case_id in semantic_case_ids]
        missing_semantics = [
            case_id
            for case_id, stage in zip(semantic_case_ids, semantic_stages)
            if stage is None
        ]
        if missing_semantics:
            row_blockers.append("missing semantic cases " + ",".join(missing_semantics))
        semantic_output_ok = bool(semantic_stages) and all(
            isinstance(stage, dict)
            and stage.get("fallback_attempted") is False
            and stage.get("external_engine_invoked") is False
            and _semantic_stage_has_output_digest(stage)
            for stage in semantic_stages
        )
        if not semantic_output_ok:
            row_blockers.append("semantic output digest evidence incomplete")

        unsupported_stages = [stage_by_id.get(case_id) for case_id in unsupported_case_ids]
        missing_unsupported = [
            case_id
            for case_id, stage in zip(unsupported_case_ids, unsupported_stages)
            if stage is None
        ]
        if missing_unsupported:
            row_blockers.append(
                "missing unsupported/error cases " + ",".join(missing_unsupported)
            )
        unsupported_ok = all(
            isinstance(stage, dict)
            and stage.get("fallback_attempted") is False
            and stage.get("external_engine_invoked") is False
            and _unsupported_stage_has_diagnostic(stage)
            for stage in unsupported_stages
        )
        if unsupported_case_ids and unsupported_ok:
            diagnostic_row_count += 1
        if unsupported_case_ids and not unsupported_ok:
            row_blockers.append("unsupported/error diagnostic evidence incomplete")

        missing_methods = [method for method in python_methods if method not in method_rows]
        if missing_methods:
            row_blockers.append("missing Python methods " + ",".join(missing_methods))
        method_ok_count = 0
        for method in python_methods:
            method_row = method_rows.get(method)
            if not isinstance(method_row, dict):
                continue
            if (
                method_row.get("fallback_attempted") is False
                and method_row.get("external_engine_invoked") is False
                and method_row.get("claim_gate_status") == "not_claim_grade"
                and str(method_row.get("support_status", "")).strip()
            ):
                method_ok_count += 1
        python_method_rows_present += method_ok_count
        if method_ok_count != len(python_methods):
            row_blockers.append("Python accessor evidence incomplete")

        no_fallback_ok = (
            (front_door or {}).get("all_no_fallback_no_external_engine") is True
            and (python_user_surface or {}).get("fallback_attempted") is False
            and (python_user_surface or {}).get("external_engine_invoked") is False
            and all(
                isinstance(stage, dict)
                and stage.get("fallback_attempted") is False
                and stage.get("external_engine_invoked") is False
                for stage in [*semantic_stages, *unsupported_stages]
                if stage is not None
            )
        )
        if no_fallback_ok:
            no_fallback_row_count += 1
        else:
            row_blockers.append("no-fallback evidence incomplete")
        if semantic_output_ok:
            output_digest_row_count += 1

        rows.append(
            {
                "operation_id": operation_id,
                "front_door_scenario_id": operation_id,
                "expected_error": operation_id in EXPECTED_ERROR_SCENARIOS,
                "semantic_case_ids": list(semantic_case_ids),
                "unsupported_case_ids": list(unsupported_case_ids),
                "python_methods": list(python_methods),
                "semantic_output_digest_evidence_complete": semantic_output_ok,
                "unsupported_diagnostic_evidence_complete": (
                    unsupported_ok if unsupported_case_ids else None
                ),
                "python_accessor_evidence_complete": method_ok_count == len(python_methods),
                "no_fallback_evidence_complete": no_fallback_ok,
                "status": "passed" if not row_blockers else "failed",
                "blockers": row_blockers,
            }
        )
        blockers.extend(
            f"operation_coverage: {operation_id}: {blocker}" for blocker in row_blockers
        )

    summary = {
        "operation_coverage_status": "passed" if not blockers else "failed",
        "operation_coverage_row_count": len(rows),
        "operation_coverage_semantic_link_count": semantic_link_count,
        "operation_coverage_unsupported_link_count": unsupported_link_count,
        "operation_coverage_python_method_link_count": python_method_link_count,
        "operation_coverage_unique_python_method_count": REQUIRED_OPERATION_UNIQUE_PYTHON_METHOD_COUNT,
        "operation_coverage_output_digest_row_count": output_digest_row_count,
        "operation_coverage_diagnostic_row_count": diagnostic_row_count,
        "operation_coverage_python_method_rows_present": python_method_rows_present,
        "operation_coverage_no_fallback_row_count": no_fallback_row_count,
    }
    return summary, rows, blockers


def build_report(
    repo_root: Path,
    paths: ReportPaths,
    matrix_path: Path = DEFAULT_MATRIX,
) -> dict[str, Any]:
    inputs: dict[str, dict[str, Any] | None] = {}
    refs: dict[str, str] = {}
    blockers: list[str] = []
    matrix, matrix_blockers, matrix_ref = _load_matrix(repo_root, matrix_path)
    blockers.extend(f"matrix: {blocker}" for blocker in matrix_blockers)
    matrix_summary: dict[str, Any] = {}
    matrix_validation_blockers: list[str] = []
    if matrix is not None:
        matrix_summary, matrix_validation_blockers = _validate_matrix(matrix)
        blockers.extend(matrix_validation_blockers)
    for key, path in [
        ("golden_workflow", paths.golden_workflow),
        ("admitted_semantics", paths.admitted_semantics),
        ("front_door", paths.front_door),
        ("vortex_runtime", paths.vortex_runtime),
        ("source_prepared_state", paths.source_prepared_state),
        ("local_output_sink", paths.local_output_sink),
        ("python_user_surface", paths.python_user_surface),
        ("example_replay", paths.example_replay),
    ]:
        payload, report_blockers, ref = _load_report(repo_root, path)
        inputs[key] = payload
        refs[key] = ref
        blockers.extend(f"{key}: {blocker}" for blocker in report_blockers)

    summaries: dict[str, Any] = {}
    validators = {
        "front_door": _validate_front_door,
        "vortex_runtime": _validate_vortex,
        "source_prepared_state": _validate_source,
        "local_output_sink": _validate_output,
        "golden_workflow": _validate_golden,
        "admitted_semantics": _validate_admitted,
        "python_user_surface": _validate_python_user_surface,
        "example_replay": _validate_example_replay,
    }
    for key, validator in validators.items():
        payload = inputs[key]
        if payload is None:
            continue
        summary, report_blockers = validator(payload)
        summaries[key] = summary
        blockers.extend(report_blockers)

    operation_coverage_summary, operation_coverage_rows, operation_coverage_blockers = (
        _validate_operation_coverage(
            admitted_semantics=inputs.get("admitted_semantics"),
            front_door=inputs.get("front_door"),
            python_user_surface=inputs.get("python_user_surface"),
        )
    )
    summaries["operation_coverage"] = operation_coverage_summary
    blockers.extend(operation_coverage_blockers)

    matrix_passed = matrix is not None and not matrix_blockers and not matrix_validation_blockers
    passed = not blockers
    report = {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if passed else "failed",
        "blockers": blockers,
        "matrix_ref": matrix_ref,
        "matrix_status": "passed" if matrix_passed else "failed",
        "matrix_summary": matrix_summary,
        "input_report_refs": refs,
        "input_report_count": len(refs),
        "v1_correctness_matrix_status": "passed" if passed else "failed",
        "scope_report_status": "passed"
        if all(_report_status(inputs, key) == "passed" for key in (
            "front_door",
            "vortex_runtime",
            "source_prepared_state",
            "local_output_sink",
        ))
        else "failed",
        "golden_workflow_validator_status": _report_status(inputs, "golden_workflow"),
        "admitted_semantics_validator_status": _report_status(inputs, "admitted_semantics"),
        "example_replay_validator_status": _report_status(inputs, "example_replay"),
        "docs_example_execution_status": "passed" if not blockers else "blocked",
        "unsupported_path_test_status": "passed" if not blockers else "blocked",
        "decoded_reference_differential_execution_performed": _report_bool(
            inputs,
            "admitted_semantics",
            "decoded_reference_differential_execution_performed",
        ),
        "property_execution_performed": _report_bool(
            inputs,
            "admitted_semantics",
            "property_execution_performed",
        ),
        "deterministic_fuzz_execution_performed": _report_bool(
            inputs,
            "admitted_semantics",
            "deterministic_fuzz_execution_performed",
        ),
        "summaries": summaries,
        "operation_coverage_rows": operation_coverage_rows,
        "residual_unsupported_rows": {
            "front_door": _report_list(inputs, "front_door", "broad_pending_parity_row_ids"),
            "vortex_runtime": _report_list(inputs, "vortex_runtime", "unsupported_boundary_ids"),
            "source_prepared_state": _report_list(
                inputs,
                "source_prepared_state",
                "unsupported_boundary_ids",
            ),
            "local_output_sink": _report_list(
                inputs,
                "local_output_sink",
                "unsupported_boundary_ids",
            ),
            "admitted_semantics": _report_list(
                inputs,
                "admitted_semantics",
                "remaining_matrix_gaps",
            ),
        },
        "claim_gate_status": "not_claim_grade",
        "runtime_support_claim_allowed": False,
        "correctness_claim_allowed": passed,
        "external_engines_allowed_as_oracles_only": True,
        "external_oracle_used": False,
        **fail_closed_fields(),
    }
    for field in FALSE_REPORT_FIELDS:
        if report.get(field) is not False:
            report.setdefault("blockers", []).append(f"{field} must be false")
            report["status"] = "failed"
    return report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    paths = ReportPaths(
        golden_workflow=args.golden_workflow_report,
        admitted_semantics=args.admitted_semantics_report,
        front_door=args.front_door_report,
        vortex_runtime=args.vortex_runtime_report,
        source_prepared_state=args.source_prepared_state_report,
        local_output_sink=args.local_output_sink_report,
        python_user_surface=args.python_user_surface_report,
        example_replay=args.example_replay_report,
    )
    report = build_report(repo_root, paths, args.matrix)
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
