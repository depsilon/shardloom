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
EXPECTED_ERROR_SCENARIOS = {"malformed_timestamp_cast"}

EXPECTED_VORTEX_PRIMITIVE_ROUTES = 9
EXPECTED_VORTEX_LOCAL_FILE_ROUTES = 15
EXPECTED_SOURCE_INPUT_FORMATS = 6
EXPECTED_SOURCE_PREPARED_ROUTE_IDS = 4
EXPECTED_SOURCE_DIRECT_ROUTE_IDS = 1
EXPECTED_SOURCE_GENERATED_ROUTE_IDS = 1
EXPECTED_SOURCE_INVALIDATION_CASES = 9
EXPECTED_OUTPUT_FORMATS = 7
EXPECTED_OUTPUT_WRITE_METHODS = 9
EXPECTED_OUTPUT_ROUTE_IDS = 8

EXPECTED_GOLDEN_WORKFLOWS = {
    "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output",
    "generated_source_to_local_vortex_output_replay_fidelity",
    "prepared_native_vortex_count_filter_project_execution_certificates",
}
EXPECTED_GOLDEN_STAGE_COUNT_MIN = 9

EXPECTED_EXECUTABLE_FIXTURES = 103
EXPECTED_DIAGNOSTIC_CASES = 24
EXPECTED_UNSUPPORTED_DIAGNOSTICS = 22
EXPECTED_RUNTIME_ERROR_DIAGNOSTICS = 1
EXPECTED_INVALID_SHAPE_DIAGNOSTICS = 1
EXPECTED_ADMITTED_STAGE_COUNT_MIN = 129
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
}

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
        "golden_workflows": len(EXPECTED_GOLDEN_WORKFLOWS),
        "golden_stage_count_min": EXPECTED_GOLDEN_STAGE_COUNT_MIN,
        "executable_fixtures": EXPECTED_EXECUTABLE_FIXTURES,
        "diagnostic_cases": EXPECTED_DIAGNOSTIC_CASES,
        "unsupported_diagnostics": EXPECTED_UNSUPPORTED_DIAGNOSTICS,
        "runtime_error_diagnostics": EXPECTED_RUNTIME_ERROR_DIAGNOSTICS,
        "invalid_shape_diagnostics": EXPECTED_INVALID_SHAPE_DIAGNOSTICS,
        "admitted_stage_count_min": EXPECTED_ADMITTED_STAGE_COUNT_MIN,
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
        "general_fuzz_beyond_seeded_property_lane",
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
    if payload.get("source_prepared_benchmark_required_fields_ready") is not True:
        blockers.append("source_prepared_state: benchmark required fields must be ready")
    return {
        "supported_input_format_count": len(payload.get("supported_input_formats", [])),
        "prepared_route_count": len(payload.get("prepared_route_ids", [])),
        "invalidation_case_count": len(payload.get("invalidation_case_ids", [])),
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
    if (
        not isinstance(payload.get("stage_count"), int)
        or payload["stage_count"] < EXPECTED_ADMITTED_STAGE_COUNT_MIN
    ):
        blockers.append("admitted_semantics: stage_count below v1 minimum")
    if payload.get("property_execution_performed") is not True:
        blockers.append("admitted_semantics: property_execution_performed must be true")
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
        "stage_count": payload.get("stage_count"),
        "required_semantic_case_count": len(REQUIRED_SEMANTIC_CASE_IDS),
        "required_unsupported_case_count": len(REQUIRED_UNSUPPORTED_CASE_IDS),
        "remaining_matrix_gaps": payload.get("remaining_matrix_gaps", []),
        **stage_summary,
    }, blockers


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
    }
    for key, validator in validators.items():
        payload = inputs[key]
        if payload is None:
            continue
        summary, report_blockers = validator(payload)
        summaries[key] = summary
        blockers.extend(report_blockers)

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
        "docs_example_execution_status": "covered_by_front_door_scenarios_and_golden_workflows"
        if not blockers
        else "blocked",
        "unsupported_path_test_status": "covered_by_admitted_semantics_diagnostics"
        if not blockers
        else "blocked",
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
        "summaries": summaries,
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
    )
    report = build_report(repo_root, paths, args.matrix)
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
