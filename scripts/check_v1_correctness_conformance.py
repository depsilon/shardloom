#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the v1 correctness and conformance closeout evidence.

This is an aggregate gate over existing reports. It does not execute runtime
work, invoke external engines, publish packages, create tags, or make release
claims. Producer jobs must generate the input reports first.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, load_json, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_correctness_conformance_report.v1"
GATE_ID = "prod-v1-2b.correctness_conformance"

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
    return {
        "executable_fixture_count": payload.get("executable_fixture_count"),
        "diagnostic_case_count": payload.get("diagnostic_case_count"),
        "unsupported_diagnostic_count": payload.get("unsupported_diagnostic_count"),
        "property_lane_count": payload.get("property_lane_count"),
        "stage_count": payload.get("stage_count"),
        "required_semantic_case_count": len(REQUIRED_SEMANTIC_CASE_IDS),
        "required_unsupported_case_count": len(REQUIRED_UNSUPPORTED_CASE_IDS),
        "remaining_matrix_gaps": payload.get("remaining_matrix_gaps", []),
    }, blockers


def build_report(repo_root: Path, paths: ReportPaths) -> dict[str, Any]:
    inputs: dict[str, dict[str, Any] | None] = {}
    refs: dict[str, str] = {}
    blockers: list[str] = []
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

    passed = not blockers
    report = {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if passed else "failed",
        "blockers": blockers,
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
    report = build_report(repo_root, paths)
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
