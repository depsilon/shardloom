#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Build a static, side-effect-free user route capability report."""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.user_route_capability_report.v1"
GATE_ID = "gar-runtime-impl-6d.user_route_capability_report"
PUBLIC_FRONT_DOOR_ROUTE_SCHEMA_VERSION = "shardloom.public_front_door_route_rows.v1"
PUBLIC_ROUTE_REUSE_MATRIX_SCHEMA_VERSION = "shardloom.public_route_reuse_matrix.v1"
REQUIRED_ROUTE_IDS = {"native_vortex_query", "object_store_lakehouse_runtime"}
SUPPORTED_ROUTE_ID = "native_vortex_query"
REQUIRED_FRONT_DOOR_IDS = {
    "local_source_vortex_middle_front_door",
    "native_vortex_front_door",
    "declared_memory_front_door",
    "source_free_sql_front_door",
}
REQUIRED_FRONT_DOOR_FAMILIES = {
    "local_source_vortex_middle_front_door": "local_compat_file",
    "native_vortex_front_door": "native_vortex_file",
    "declared_memory_front_door": "declared_memory",
    "source_free_sql_front_door": "source_free",
}
REQUIRED_REUSE_ROWS = {
    "filter_project_limit", "group_aggregate", "join", "ordered_rows", "distinct",
    "string_expressions", "casts_and_nulls", "declared_sinks", "memory_and_source_free",
}
REQUIRED_EVIDENCE = {
    "native_vortex_plan_route_family", "native_vortex_operation_family",
    "public_workflow_fallback_attempted", "public_workflow_external_engine_invoked",
}
REQUIRED_RUNTIME_SPINE = (
    "declared input or source-free expression -> native Vortex admission -> "
    "native_vortex_unified_plan -> typed result or declared sink"
)
FORBIDDEN_REUSE_FIELDS = {
    "alternate_route_ids", "source_state_required", "prepared_state_required",
    "prepared_olap_state_reused_when_available", "prepared_state_reuse_manifest_path",
    "prepared_state_reuse_policy",
}
FORBIDDEN_RUNTIME_LABELS = {
    "sql-local-source-smoke", "direct_compatibility_transient", "direct_transient",
    "internal_local_source_smoke",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/user-route-capability-report.json"))
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_report(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).user_route_capability_report()


def row_payload(row: Any) -> dict[str, Any]:
    return {
        "route_id": row.route_id,
        "route_display_name": row.route_display_name,
        "input_family": row.input_family,
        "input_examples": list(row.input_examples),
        "front_doors": list(row.front_doors),
        "desired_outputs": list(row.desired_outputs),
        "recommended_user_surface": row.recommended_user_surface,
        "start_state": row.start_state,
        "vortex_normalization_point": row.vortex_normalization_point,
        "source_route": row.source_route,
        "preparation_route": row.preparation_route,
        "execution_mode": row.execution_mode,
        "execution_route": row.execution_route,
        "output_route": row.output_route,
        "evidence_route": row.evidence_route,
        "materialization_decode_boundary": row.materialization_decode_boundary,
        "nearest_runnable_route": row.nearest_runnable_route,
        "required_feature_gate": row.required_feature_gate,
        "runtime_blocker_code": row.runtime_blocker_code,
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "blocker_id": row.blocker_id or "none",
        "owner": row.owner,
        "required_evidence": list(row.required_evidence),
        "claim_gate_status": row.claim_gate_status,
        "performance_claim_allowed": row.performance_claim_allowed,
        "production_claim_allowed": row.production_claim_allowed,
        "spark_replacement_claim_allowed": row.spark_replacement_claim_allowed,
        "claim_boundary": row.claim_boundary,
    }


def public_front_door_row_payload(row: Any) -> dict[str, Any]:
    return {
        "front_door_id": row.front_door_id,
        "owning_route_id": row.owning_route_id,
        "input_family": row.input_family,
        "public_user_surface": row.public_user_surface,
        "vortex_normalization_point": row.vortex_normalization_point,
        "execution_mode": row.execution_mode,
        "output_route": row.output_route,
        "required_evidence": list(row.required_evidence),
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "claim_boundary": row.claim_boundary,
    }


def public_route_reuse_matrix_row_payload(row: Any) -> dict[str, Any]:
    return {
        "row_id": row.row_id,
        "operation_family": row.operation_family,
        "public_surfaces": list(row.public_surfaces),
        "source_variants": list(row.source_variants),
        "primary_route_id": row.primary_route_id,
        "shared_runtime_spine": row.shared_runtime_spine,
        "native_plan_route_family": row.native_plan_route_family,
        "native_plan_payload_kind": row.native_plan_payload_kind,
        "materialization_decode_boundary": row.materialization_decode_boundary,
        "typed_result_or_sink_contract": row.typed_result_or_sink_contract,
        "evidence_fields": list(row.evidence_fields),
        "route_runtime_status": row.route_runtime_status,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
        "claim_boundary": row.claim_boundary,
    }


def validate_rows(report: Any, rows: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    by_id = {row.get("route_id"): row for row in rows}
    if set(by_id) != REQUIRED_ROUTE_IDS or len(rows) != len(REQUIRED_ROUTE_IDS):
        blockers.append("route ids must contain the native query and object-store boundary exactly once")
    native = by_id.get(SUPPORTED_ROUTE_ID, {})
    if native.get("owner") != "shared_native_workflow":
        blockers.append("native_vortex_query must be owned by shared_native_workflow")
    if native.get("route_runtime_status") != "global_runtime_supported":
        blockers.append("native_vortex_query must be globally runtime supported")
    if native.get("execution_mode") != "native_vortex":
        blockers.append("native_vortex_query must use native_vortex execution")
    examples = " ".join(native.get("input_examples", []))
    for example in (".vortex", "from_rows(", "SELECT 1 AS id"):
        if example not in examples:
            blockers.append(f"native_vortex_query is missing input example {example!r}")
    outputs = set(native.get("desired_outputs", []))
    for output in ("complete_typed_rows", "native_vortex_output", "compatibility_output", "fanout"):
        if output not in outputs:
            blockers.append(f"native_vortex_query is missing output contract {output!r}")
    if "complete typed result" not in native.get("output_route", "") or "committed requested output" not in native.get("output_route", ""):
        blockers.append("native_vortex_query output route must cover complete results and committed declared outputs")
    if not REQUIRED_EVIDENCE.issubset(set(native.get("required_evidence", []))):
        blockers.append("native_vortex_query is missing native workflow evidence fields")
    if native.get("fallback_attempted") is not False or native.get("external_engine_invoked") is not False:
        blockers.append("native_vortex_query must preserve no-fallback and no-external-engine")
    for field in ("performance_claim_allowed", "production_claim_allowed", "spark_replacement_claim_allowed"):
        if native.get(field) is not False:
            blockers.append(f"native_vortex_query {field} must remain false")
    external = by_id.get("object_store_lakehouse_runtime", {})
    if external.get("route_runtime_status") != "external_environment_gate_pending":
        blockers.append("object-store runtime must remain behind its external-environment gate")
    if external.get("blocker_id") != "cg9.cg10.cg21.production_io_front_door_missing":
        blockers.append("object-store runtime must retain the production I/O blocker")
    if external.get("fallback_attempted") is not False or external.get("external_engine_invoked") is not False:
        blockers.append("object-store boundary must preserve no-fallback and no-external-engine")
    if getattr(report, "claim_gate_status", None) != "not_claim_grade":
        blockers.append("route capability discovery must remain not claim grade")
    for field in ("flexible_anything_claim_allowed", "performance_equivalence_claim_allowed", "production_claim_allowed", "spark_replacement_claim_allowed"):
        if getattr(report, field, None) is not False:
            blockers.append(f"route capability report {field} must remain false")
    if getattr(report, "all_no_fallback_no_external_engine", None) is not True:
        blockers.append("route capability report must preserve no-fallback and no-external-engine")
    return blockers


def public_context_methods(repo_root: Path) -> set[str]:
    source = (repo_root / "python/src/shardloom/context.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "ShardLoomContext":
            return {
                item.name
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and not item.name.startswith("_")
            }
    return set()


def validate_public_front_door_routes(
    rows: list[dict[str, Any]],
    route_rows: list[dict[str, Any]],
    repo_root: Path = ROOT,
) -> list[str]:
    blockers: list[str] = []
    by_id = {row.get("front_door_id"): row for row in rows}
    context_methods = public_context_methods(repo_root)
    if set(by_id) != REQUIRED_FRONT_DOOR_IDS or len(rows) != len(REQUIRED_FRONT_DOOR_IDS):
        blockers.append("public front doors must include the four declared input families exactly once")
    supported = {row.get("route_id") for row in route_rows if row.get("route_runtime_status") == "global_runtime_supported"}
    for identifier, family in REQUIRED_FRONT_DOOR_FAMILIES.items():
        row = by_id.get(identifier, {})
        if row.get("input_family") != family:
            blockers.append(f"{identifier} has an unexpected input family")
        if row.get("owning_route_id") not in supported or row.get("owning_route_id") != SUPPORTED_ROUTE_ID:
            blockers.append(f"{identifier} must use the shared native query route")
        if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
            blockers.append(f"{identifier} must preserve no-fallback and no-external-engine")
        if not REQUIRED_EVIDENCE.issubset(set(row.get("required_evidence", []))):
            blockers.append(f"{identifier} is missing route evidence fields")
        if not row.get("vortex_normalization_point") or not row.get("output_route"):
            blockers.append(f"{identifier} must expose normalization and output contracts")
        surface = row.get("public_user_surface", "")
        named_methods = {
            match.group(1)
            for match in re.finditer(r"\bctx\.([A-Za-z_][A-Za-z0-9_]*)\s*\(", surface)
        }
        unknown_methods = named_methods - context_methods
        if unknown_methods:
            blockers.append(
                f"{identifier} references missing Context methods: "
                + ",".join(sorted(unknown_methods))
            )
    return blockers


def validate_public_route_reuse_matrix(rows: list[dict[str, Any]], route_rows: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    by_id = {row.get("row_id"): row for row in rows}
    if set(by_id) != REQUIRED_REUSE_ROWS or len(rows) != len(REQUIRED_REUSE_ROWS):
        blockers.append("public route reuse matrix must include all nine operation families exactly once")
    for identifier, row in by_id.items():
        if row.get("operation_family") != identifier:
            blockers.append(f"{identifier} operation family must match its row id")
        if row.get("primary_route_id") != SUPPORTED_ROUTE_ID:
            blockers.append(f"{identifier} must use native_vortex_query as its primary route")
        if row.get("native_plan_route_family") != "native_vortex_unified_plan" or row.get("native_plan_payload_kind") != "native_query_plan":
            blockers.append(f"{identifier} must use the native Vortex unified plan")
        if row.get("shared_runtime_spine") != REQUIRED_RUNTIME_SPINE:
            blockers.append(f"{identifier} must preserve the shared native runtime spine")
        if row.get("typed_result_or_sink_contract") != "complete_typed_rows_or_committed_declared_output":
            blockers.append(f"{identifier} must preserve the typed result or declared sink contract")
        if row.get("route_runtime_status") != "global_runtime_supported":
            blockers.append(f"{identifier} must be globally runtime supported")
        if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
            blockers.append(f"{identifier} must preserve no-fallback and no-external-engine")
        if FORBIDDEN_REUSE_FIELDS.intersection(row):
            blockers.append(f"{identifier} contains retired alternate-route or prepared-state fields")
        if not REQUIRED_EVIDENCE.issubset(set(row.get("evidence_fields", []))):
            blockers.append(f"{identifier} is missing route evidence fields")
        variants = set(row.get("source_variants", []))
        if not {"vortex", "declared_memory", "source_free"}.issubset(variants):
            blockers.append(f"{identifier} must cover Vortex, declared-memory, and source-free inputs")
    return blockers


def validate_no_stale_public_runtime_labels(rows: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for row in rows:
        text = json.dumps(row, sort_keys=True)
        for label in FORBIDDEN_RUNTIME_LABELS:
            if label in text:
                blockers.append(f"public route row {row.get('route_id')} contains stale runtime label {label!r}")
    return blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    route_report = load_report(repo_root)
    rows = [row_payload(row) for row in route_report.rows]
    doors = [public_front_door_row_payload(row) for row in route_report.public_front_door_route_rows]
    reuse = [public_route_reuse_matrix_row_payload(row) for row in route_report.public_route_reuse_matrix_rows]
    blockers = validate_rows(route_report, rows)
    blockers.extend(validate_public_front_door_routes(doors, rows, repo_root))
    blockers.extend(validate_public_route_reuse_matrix(reuse, rows))
    blockers.extend(validate_no_stale_public_runtime_labels(rows))
    blockers = sorted(set(blockers))
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "report_kind": "static_capability_discovery",
        "claim_gate_status": route_report.claim_gate_status,
        "runtime_execution_performed": False,
        "performance_evidence_produced": False,
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "route_count": len(rows),
        "route_ids": [row["route_id"] for row in rows],
        "rows": rows,
        "public_front_door_route_schema_version": PUBLIC_FRONT_DOOR_ROUTE_SCHEMA_VERSION,
        "public_front_door_route_count": len(doors),
        "public_front_door_route_rows": doors,
        "public_route_reuse_matrix_schema_version": PUBLIC_ROUTE_REUSE_MATRIX_SCHEMA_VERSION,
        "public_route_reuse_matrix_count": len(reuse),
        "public_route_reuse_matrix_rows": reuse,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "all_no_fallback_no_external_engine": route_report.all_no_fallback_no_external_engine,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "acceptance_summary": {
            "shared_native_route_owns_public_front_doors": not blockers,
            "public_route_reuse_matrix_complete": len(reuse) == len(REQUIRED_REUSE_ROWS),
            "static_discovery_is_not_runtime_or_performance_proof": True,
            "no_fallback_no_external_engine": True,
        },
    }


def main() -> int:
    args = parse_args()
    report = build_report(args.repo_root.resolve())
    output = resolve(args.repo_root.resolve(), args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "blockers": report["blockers"], "output": str(output)}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
