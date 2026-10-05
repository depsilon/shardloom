#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the declarative v1 local output and sink scope contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_report_utils import (  # noqa: E402
    fail_closed_fields,
    load_json,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_local_output_sink_scope_report.v1"
DOC_PATH = Path("docs/architecture/v1-local-output-sink-scope.md")
EVIDENCE_CLASS = "declarative_contract"

DOC_MARKERS = (
    "shardloom.v1_local_output_sink_scope.v1",
    "ShardLoomContext.local_output_sink_scope_report()",
    "output-evidence-fields-golden.json",
    "native_vortex_query",
    "shared_native_workflow",
    "write_json",
    "jsonl",
    "arrow-ipc",
    "write_vortex",
    "error_if_exists_by_default",
    "explicit_allow_overwrite",
    "append_mode_unsupported",
    "atomic_rename_same_directory",
    "partial_write_cleanup_reported",
    "Vortex-first provider check",
    "use_vortex_native_provider",
    "wrap_vortex_concept",
    "blocked_until_vortex_or_shardloom_evidence",
    "object_store_output_paths",
    "table_catalog_writes",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "runtime_evidence_verified=false",
    "create-if-absent",
    "no answer cache",
)

PUBLIC_DOC_MARKERS = {
    "README.md": (
        DOC_PATH.as_posix(),
        "Local output/sink scope",
        "write_vortex",
    ),
    "python/README.md": (
        DOC_PATH.as_posix(),
        "local_output_sink_scope_report",
        "write_vortex",
    ),
    "docs/release/public-status-matrix.md": (
        DOC_PATH.as_posix(),
        "local output/sink scope",
        "append remains unsupported",
    ),
    "docs/release/v1-inclusion-scope-matrix.md": (
        "`PROD-V1-1D`",
        "closed_local_output_sink_scope",
        DOC_PATH.as_posix(),
    ),
    "website-src/src/content/docs/field-guide/benchmark-methodology.mdx": (
        "ClickBench",
        "does not present a public ranking.",
        "no-fallback evidence",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-local-output-sink-scope-report.json"),
    )
    return parser.parse_args()


def load_context_report(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).local_output_sink_scope_report()


def _value(row: Any, field: str) -> Any:
    return row.get(field) if isinstance(row, dict) else getattr(row, field, None)


def _sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


def method_payload(row: Any) -> dict[str, Any]:
    return {
        "method": _value(row, "method"),
        "family": _value(row, "family"),
        "support_status": _value(row, "support_status"),
        "required_evidence": _sequence(_value(row, "required_evidence")),
        "runtime_execution": _value(row, "runtime_execution"),
        "data_read": _value(row, "data_read"),
        "write_io": _value(row, "write_io"),
        "materialization_required": _value(row, "materialization_required"),
        "fallback_attempted": _value(row, "fallback_attempted"),
        "external_engine_invoked": _value(row, "external_engine_invoked"),
        "claim_gate_status": _value(row, "claim_gate_status"),
        "claim_boundary": _value(row, "claim_boundary"),
    }


def route_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": _value(row, "route_id"),
        "owner": _value(row, "owner"),
        "execution_mode": _value(row, "execution_mode"),
        "route_display_name": _value(row, "route_display_name"),
        "desired_outputs": _sequence(_value(row, "desired_outputs")),
        "output_route": _value(row, "output_route"),
        "evidence_route": _value(row, "evidence_route"),
        "materialization_decode_boundary": _value(row, "materialization_decode_boundary"),
        "route_runtime_status": _value(row, "route_runtime_status"),
        "required_evidence": _sequence(_value(row, "required_evidence")),
        "fallback_attempted": _value(row, "fallback_attempted"),
        "external_engine_invoked": _value(row, "external_engine_invoked"),
        "claim_gate_status": _value(row, "claim_gate_status"),
        "claim_boundary": _value(row, "claim_boundary"),
    }


def validate_context_report(report: Any) -> list[str]:
    blockers: list[str] = []
    if report.schema_version != "shardloom.v1_local_output_sink_scope.v1":
        blockers.append("local output/sink scope report schema mismatch")
    if report.scope_document != DOC_PATH.as_posix():
        blockers.append("local output/sink scope document mismatch")
    if report.report_id != "prod-v1-1d.local_output_sink_scope":
        blockers.append("local output/sink scope report id mismatch")
    if report.v1_scope_ready is not True:
        blockers.append("declarative local output/sink contract must be ready")
    if report.all_write_methods_registered is not True:
        blockers.append("all write methods must be registered")
    if report.all_write_methods_no_fallback_no_external_engine is not True:
        blockers.append("all write methods must preserve no fallback")
    if report.all_output_routes_no_fallback_no_external_engine is not True:
        blockers.append("all output routes must preserve no fallback")
    if report.all_output_routes_emit_sink_evidence is not True:
        blockers.append("all output routes must declare sink evidence")
    if report.all_feature_gated_formats_labeled is not True:
        blockers.append("feature-gated output formats must be labeled")
    if report.write_policy_contract_ready is not True:
        blockers.append("write policy contract must be ready")

    expected_formats = {"json", "jsonl", "csv", "parquet", "arrow-ipc", "avro", "orc", "vortex"}
    expected_default_formats = {"json", "jsonl", "csv"}
    expected_gated_formats = {"parquet", "arrow-ipc", "avro", "orc", "vortex"}
    expected_methods = {
        "write", "write_json", "write_jsonl", "write_csv", "write_parquet",
        "write_arrow_ipc", "write_avro", "write_orc", "write_vortex", "fanout",
    }
    expected_policies = {
        "error_if_exists_by_default", "explicit_allow_overwrite", "append_mode_unsupported",
        "atomic_rename_same_directory", "partial_write_cleanup_reported",
    }
    if set(report.supported_output_formats) != expected_formats:
        blockers.append("supported output format set mismatch")
    if set(report.default_output_formats) != expected_default_formats:
        blockers.append("default output format set mismatch")
    if set(report.feature_gated_output_formats) != expected_gated_formats:
        blockers.append("feature-gated output format set mismatch")
    if set(report.user_write_methods) != expected_methods:
        blockers.append("write method set mismatch")
    if tuple(report.output_route_ids) != ("native_vortex_query",):
        blockers.append("output route ids must contain only native_vortex_query")
    if set(report.write_policy_ids) != expected_policies:
        blockers.append("write policy id set mismatch")
    if len(report.golden_fixture_paths) != 3 or not any(
        path.endswith("output-evidence-fields-golden.json") for path in report.golden_fixture_paths
    ):
        blockers.append("golden fixtures must include the three current scope fixtures")
    boundaries = set(report.unsupported_boundary_ids)
    for boundary in ("append_mode", "object_store_output_paths", "table_catalog_writes"):
        if boundary not in boundaries:
            blockers.append(f"unsupported boundaries must include {boundary}")
    for field in ("performance_claim_allowed", "production_claim_allowed", "spark_replacement_claim_allowed"):
        if getattr(report, field) is not False:
            blockers.append(f"{field} must be false")
    if report.claim_gate_status != "not_claim_grade":
        blockers.append("claim_gate_status must remain not_claim_grade")

    rows = tuple(report.write_method_rows)
    if {row.method for row in rows} != expected_methods:
        blockers.append("write method capability rows must exactly match the declared methods")
    route_rows = tuple(report.output_user_route_rows)
    if len(route_rows) != 1:
        blockers.append("exactly one output user route must be declared")
    for row in route_rows:
        if _value(row, "route_id") != "native_vortex_query":
            blockers.append("output route must be native_vortex_query")
        if _value(row, "owner") != "shared_native_workflow":
            blockers.append("output route owner must be shared_native_workflow")
        if _value(row, "execution_mode") != "native_vortex":
            blockers.append("output route execution_mode must be native_vortex")
    for row in (*rows, *route_rows):
        row_id = _value(row, "method") or _value(row, "route_id")
        if _value(row, "fallback_attempted") is not False:
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if _value(row, "external_engine_invoked") is not False:
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if _value(row, "claim_gate_status") != "not_claim_grade":
            blockers.append(f"{row_id}: claim_gate_status must remain not_claim_grade")
    return blockers


def validate_docs(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    blockers.extend(
        require_markers(DOC_PATH.as_posix(), read_text(resolve_path(repo_root, DOC_PATH)), DOC_MARKERS)
    )
    for rel_path, markers in PUBLIC_DOC_MARKERS.items():
        blockers.extend(require_markers(rel_path, read_text(resolve_path(repo_root, rel_path)), markers))
    return blockers


def validate_fixtures(repo_root: Path, report: Any) -> tuple[list[str], list[dict[str, Any]]]:
    blockers: list[str] = []
    fixtures: list[dict[str, Any]] = []
    expected_policies = set(report.write_policy_ids)
    expected_fields = set(report.required_runtime_fields)
    expected_formats = set(report.supported_output_formats)
    expected_methods = set(report.user_write_methods)
    for rel_path in report.golden_fixture_paths:
        try:
            payload = load_json(resolve_path(repo_root, rel_path))
        except (OSError, ValueError) as exc:
            blockers.append(f"{rel_path}: fixture unreadable: {exc.__class__.__name__}")
            continue
        fixtures.append({"path": rel_path, "schema_version": payload.get("schema_version")})
        for field, expected in (
            ("scope_document", DOC_PATH.as_posix()),
            ("claim_gate_status", "not_claim_grade"),
            ("evidence_class", EVIDENCE_CLASS),
            ("runtime_evidence_verified", False),
            ("fallback_attempted", False),
            ("external_engine_invoked", False),
        ):
            if (payload.get(field) is not expected if isinstance(expected, bool) else payload.get(field) != expected):
                blockers.append(f"{rel_path}: {field} must equal {expected!r}")
        if rel_path.endswith("output-scope-golden.json"):
            for field, expected in (
                ("output_route_ids", ["native_vortex_query"]),
                ("supported_output_formats", sorted(expected_formats)),
                ("default_output_formats", sorted(report.default_output_formats)),
                ("feature_gated_output_formats", sorted(report.feature_gated_output_formats)),
                ("user_write_methods", sorted(expected_methods)),
            ):
                actual = payload.get(field, [])
                if not isinstance(actual, list) or len(actual) != len(set(actual)) or set(actual) != set(expected):
                    blockers.append(f"{rel_path}: {field} mismatch")
        elif rel_path.endswith("output-policy-matrix.json"):
            policies = payload.get("policies", [])
            policy_ids = {str(item.get("policy_id")) for item in policies if isinstance(item, dict)}
            if not isinstance(policies, list) or policy_ids != expected_policies:
                blockers.append(f"{rel_path}: policy ids mismatch")
            expected_policy_rows = {
                "error_if_exists_by_default": ("supported_default_local_policy", False, True),
                "explicit_allow_overwrite": (
                    "existing_targets_denied_for_shared_native_computed_writes_even_when_requested",
                    False,
                    True,
                ),
                "append_mode_unsupported": ("blocked_unsupported_v1", True, False),
                "atomic_rename_same_directory": (
                    "reported_when_writer_supports_same_directory_commit",
                    False,
                    True,
                ),
                "partial_write_cleanup_reported": (
                    "cleanup_or_not_required_status_reported",
                    False,
                    True,
                ),
            }
            for item in policies if isinstance(policies, list) else []:
                if not isinstance(item, dict):
                    continue
                policy_id = item.get("policy_id")
                expected = expected_policy_rows.get(policy_id)
                if expected is None:
                    continue
                actual = (
                    item.get("runtime_posture"),
                    item.get("deterministic_diagnostic_required"),
                    item.get("write_io_allowed"),
                )
                if actual != expected:
                    blockers.append(f"{rel_path}: policy contract mismatch for {policy_id}")
                if policy_id == "explicit_allow_overwrite" and item.get("existing_target_allowed") is not False:
                    blockers.append(f"{rel_path}: existing target overwrite must remain denied")
        elif rel_path.endswith("output-evidence-fields-golden.json"):
            fields = set(str(field) for field in payload.get("declared_fields", []))
            if fields != expected_fields:
                blockers.append(f"{rel_path}: declared fields mismatch")
            if payload.get("readback_source") != "live_tests_or_harness":
                blockers.append(f"{rel_path}: readback_source must identify live tests or harness")
            if payload.get("replay_result") is not None:
                blockers.append(f"{rel_path}: declarative fixture must not claim a replay result")
    return blockers, fixtures


def build_report(repo_root: Path) -> dict[str, Any]:
    scope_report = load_context_report(repo_root)
    write_method_rows = [method_payload(row) for row in scope_report.write_method_rows]
    output_route_rows = [route_payload(row) for row in scope_report.output_user_route_rows]
    blockers = validate_context_report(scope_report)
    blockers.extend(validate_docs(repo_root))
    fixture_blockers, fixtures = validate_fixtures(repo_root, scope_report)
    blockers.extend(fixture_blockers)
    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "report_id": scope_report.report_id,
        "v1_scope_document": scope_report.scope_document,
        "evidence_class": EVIDENCE_CLASS,
        "runtime_evidence_verified": False,
        "declarative_contract_ready": passed,
        "supported_output_formats": list(scope_report.supported_output_formats),
        "default_output_formats": list(scope_report.default_output_formats),
        "feature_gated_output_formats": list(scope_report.feature_gated_output_formats),
        "user_write_methods": list(scope_report.user_write_methods),
        "output_route_ids": list(scope_report.output_route_ids),
        "write_policy_ids": list(scope_report.write_policy_ids),
        "golden_fixture_paths": list(scope_report.golden_fixture_paths),
        "required_runtime_fields": list(scope_report.required_runtime_fields),
        "unsupported_boundary_ids": list(scope_report.unsupported_boundary_ids),
        "write_method_rows": write_method_rows,
        "output_user_route_rows": output_route_rows,
        "fixture_rows": fixtures,
        "all_no_fallback_no_external_engine": (
            scope_report.all_write_methods_no_fallback_no_external_engine
            and scope_report.all_output_routes_no_fallback_no_external_engine
        ),
        "all_write_methods_registered": scope_report.all_write_methods_registered,
        "all_write_methods_no_fallback_no_external_engine": scope_report.all_write_methods_no_fallback_no_external_engine,
        "all_output_routes_no_fallback_no_external_engine": scope_report.all_output_routes_no_fallback_no_external_engine,
        "all_output_routes_emit_sink_evidence": scope_report.all_output_routes_emit_sink_evidence,
        "all_feature_gated_formats_labeled": scope_report.all_feature_gated_formats_labeled,
        "write_policy_contract_ready": scope_report.write_policy_contract_ready,
        "v1_scope_ready": scope_report.v1_scope_ready,
        "claim_gate_status": scope_report.claim_gate_status,
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
