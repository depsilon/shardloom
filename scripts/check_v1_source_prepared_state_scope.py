#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the declarative v1 native source-state scope contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PYTHON_SRC = ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

DOC_PATH = Path("docs/architecture/v1-source-prepared-state-scope.md")
FIXTURE_DIR = Path("docs/architecture/fixtures/v1-source-prepared-state")
FIXTURE_PATHS = (
    FIXTURE_DIR / "source-state-golden.json",
    FIXTURE_DIR / "vortex-prepared-state-golden.json",
    FIXTURE_DIR / "reuse-invalidation-matrix.json",
)
SCHEMA_VERSION = "shardloom.v1_source_prepared_state_scope_report.v1"
CANONICAL_ROUTE = (
    "declared input or source-free expression -> native Vortex admission -> "
    "native_vortex_unified_plan -> typed result or declared sink"
)
SUPPORTED_FORMATS = ("csv", "json", "jsonl", "parquet", "arrow-ipc", "avro", "orc")
ROUTE_IDS = ("native_vortex_query",)
INVALIDATION_CASE_IDS = (
    "first_request",
    "same_source_same_declaration",
    "source_changed",
    "memory_declaration_changed",
    "resource_policy_changed",
    "missing_artifact",
    "artifact_changed",
)
REQUIRED_RUNTIME_FIELDS = (
    "native_vortex_plan_route_family",
    "native_vortex_operation_family",
    "public_workflow_fallback_attempted",
    "public_workflow_external_engine_invoked",
)
UNSUPPORTED_BOUNDARIES = (
    "global_hidden_cache",
    "external_cache_service",
    "object_store_prepared_state_reuse",
    "table_catalog_prepared_state_reuse",
    "broad_non_local_preparation",
)
STATE_OWNER = "ResidentVortexSession"
REUSE_SCOPE = "native_session_or_explicit_vortex_artifact"
REUSE_POLICY = "validate_source_generation_and_declaration_before_each_execution"

DOC_MARKERS = (
    "shardloom.v1_source_prepared_state_scope.v1",
    "native_vortex_query",
    "ResidentVortexSession",
    "validate_source_generation_and_declaration_before_each_execution",
    "does not cache query",
    "memory and",
    "Compatibility inputs normalize through Vortex",
    "vortex-prepare",
    "source-free execution does not require publication",
    "first_request",
    "memory_declaration_changed",
    "artifact_changed",
    "global_hidden_cache",
    "external cache service",
    "object-store prepared-state",
    "table/catalog prepared-state reuse",
    "broad non-local preparation",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "test_native_session_execution.py",
    "resident_worker.rs",
    "declarative specification",
    "not runtime evidence",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-source-prepared-state-scope-report.json"),
    )
    return parser.parse_args()


def load_context_report(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).source_prepared_state_scope_report()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("fixture root must be an object")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_context_report(report: Any) -> list[str]:
    blockers: list[str] = []
    if report.schema_version != "shardloom.v1_source_prepared_state_scope.v1":
        blockers.append("source/prepared scope report schema mismatch")
    if report.scope_document != DOC_PATH.as_posix():
        blockers.append("source/prepared scope document mismatch")
    if report.report_id != "prod-v1-1c.source_prepared_state_scope":
        blockers.append("source/prepared scope report id mismatch")
    if report.canonical_route != CANONICAL_ROUTE:
        blockers.append("canonical route must be native_vortex_query")
    if tuple(report.prepared_route_ids) != ROUTE_IDS:
        blockers.append("prepared routes must contain only native_vortex_query")
    if tuple(report.supported_input_formats) != SUPPORTED_FORMATS:
        blockers.append("supported input format contract mismatch")
    if tuple(report.invalidation_case_ids) != INVALIDATION_CASE_IDS:
        blockers.append("invalidation case contract mismatch")
    if tuple(report.required_runtime_fields) != REQUIRED_RUNTIME_FIELDS:
        blockers.append("required runtime field contract mismatch")
    if tuple(report.unsupported_boundary_ids) != UNSUPPORTED_BOUNDARIES:
        blockers.append("unsupported boundary contract mismatch")
    if report.state_owner != STATE_OWNER:
        blockers.append("state owner must be ResidentVortexSession")
    if report.reuse_scope != REUSE_SCOPE:
        blockers.append("reuse scope must be native session or explicit Vortex artifact")
    if report.reuse_policy != REUSE_POLICY:
        blockers.append("reuse must validate source generation and declaration per execution")
    if report.query_answers_cached is not False:
        blockers.append("query answers must not be cached")
    if report.v1_scope_ready is not True:
        blockers.append("source/prepared v1_scope_ready must be true")
    if report.all_no_fallback_no_external_engine is not True:
        blockers.append("native_vortex_query must preserve no-fallback policy")
    for field in (
        "performance_claim_allowed",
        "production_claim_allowed",
        "spark_replacement_claim_allowed",
    ):
        if getattr(report, field, None) is not False:
            blockers.append(f"{field} must be false")
    if report.claim_gate_status != "not_claim_grade":
        blockers.append("claim_gate_status must remain not_claim_grade")
    rows = tuple(report.prepared_user_route_rows)
    if len(rows) != 1 or getattr(rows[0], "route_id", None) != "native_vortex_query":
        blockers.append("native_vortex_query route row is missing or mismatched")
    for row in rows:
        if getattr(row, "fallback_attempted", None) is not False:
            blockers.append("native_vortex_query fallback_attempted must be false")
        if getattr(row, "external_engine_invoked", None) is not False:
            blockers.append("native_vortex_query external_engine_invoked must be false")
        if getattr(row, "claim_gate_status", None) != "not_claim_grade":
            blockers.append("native_vortex_query claim gate must be not_claim_grade")
    return blockers


def _base_fixture_blockers(payload: dict[str, Any], path: Path) -> list[str]:
    prefix = path.as_posix()
    blockers: list[str] = []
    expected = {
        "scope_document": DOC_PATH.as_posix(),
        "canonical_route": CANONICAL_ROUTE,
        "evidence_class": "declarative_specification",
        "state_owner": STATE_OWNER,
        "reuse_scope": REUSE_SCOPE,
        "reuse_policy": REUSE_POLICY,
        "query_answers_cached": False,
        "claim_gate_status": "not_claim_grade",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            blockers.append(f"{prefix}: {key} must equal {value!r}")
    if "runtime_evidence" in payload:
        blockers.append(f"{prefix}: declarative fixtures cannot claim runtime evidence")
    return blockers


def _source_state_spec() -> dict[str, Any]:
    return {
        "schema_version": "shardloom.v1_source_state_golden.v2",
        "fixture_id": "v1_source_state_scope.native_query_contract",
        "scope_document": DOC_PATH.as_posix(),
        "canonical_route": CANONICAL_ROUTE,
        "evidence_class": "declarative_specification",
        "route_id": "native_vortex_query",
        "state_owner": STATE_OWNER,
        "reuse_scope": REUSE_SCOPE,
        "reuse_policy": REUSE_POLICY,
        "query_answers_cached": False,
        "source_inputs": {
            "native_vortex": "direct",
            "memory": "direct",
            "compatibility_formats": list(SUPPORTED_FORMATS),
            "compatibility_normalization": "through_vortex_before_shared_query",
            "source_free": "shared_engine_without_mandatory_publication",
        },
        "durable_vortex_prepare": "optional",
        "claim_gate_status": "not_claim_grade",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def _prepared_state_spec() -> dict[str, Any]:
    return {
        "schema_version": "shardloom.v1_vortex_prepared_state_golden.v2",
        "fixture_id": "v1_vortex_prepared_state_scope.session_or_artifact_reuse",
        "scope_document": DOC_PATH.as_posix(),
        "canonical_route": CANONICAL_ROUTE,
        "evidence_class": "declarative_specification",
        "route_id": "native_vortex_query",
        "state_owner": STATE_OWNER,
        "reuse_scope": REUSE_SCOPE,
        "reuse_policy": REUSE_POLICY,
        "query_answers_cached": False,
        "artifact_contract": {
            "format": "vortex",
            "reuse_requires_explicit_artifact": True,
            "validate_source_generation": True,
            "validate_declaration": True,
            "durable_preparation_required": False,
        },
        "claim_gate_status": "not_claim_grade",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def _matrix_spec(case_ids: tuple[str, ...]) -> dict[str, Any]:
    details = {
        "first_request": (False, "no_validated_resident_state", "execute_native_query"),
        "same_source_same_declaration": (True, "resident_state_validated", "execute_native_query"),
        "source_changed": (False, "source_generation_changed", "execute_native_query"),
        "memory_declaration_changed": (False, "memory_declaration_changed", "execute_native_query"),
        "resource_policy_changed": (False, "resource_policy_changed", "execute_native_query"),
        "missing_artifact": (False, "explicit_vortex_artifact_missing", "execute_native_query"),
        "artifact_changed": (False, "explicit_vortex_artifact_changed", "execute_native_query"),
    }
    return {
        "schema_version": "shardloom.v1_source_prepared_state_reuse_invalidation_matrix.v2",
        "fixture_id": "v1_source_prepared_state_scope.invalidation_contract",
        "scope_document": DOC_PATH.as_posix(),
        "canonical_route": CANONICAL_ROUTE,
        "evidence_class": "declarative_specification",
        "state_owner": STATE_OWNER,
        "reuse_scope": REUSE_SCOPE,
        "reuse_policy": REUSE_POLICY,
        "query_answers_cached": False,
        "cases": [
            {
                "case_id": case_id,
                "reuse_hit": details[case_id][0],
                "reuse_reason": details[case_id][1],
                "invalidation_reason": "none" if details[case_id][0] else details[case_id][1],
                "query_execution": details[case_id][2],
                "query_answer_cached": False,
            }
            for case_id in case_ids
        ],
        "claim_gate_status": "not_claim_grade",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def expected_fixtures(report: Any) -> dict[str, dict[str, Any]]:
    paths = FIXTURE_PATHS
    return {
        paths[0].as_posix(): _source_state_spec(),
        paths[1].as_posix(): _prepared_state_spec(),
        paths[2].as_posix(): _matrix_spec(tuple(report.invalidation_case_ids)),
    }


def validate_fixtures(repo_root: Path, report: Any) -> tuple[list[str], list[dict[str, str]]]:
    blockers: list[str] = []
    fixtures: list[dict[str, str]] = []
    expected = expected_fixtures(report)
    report_paths = tuple(Path(path).as_posix() for path in report.golden_fixture_paths)
    if report_paths != tuple(expected):
        blockers.append("golden fixture paths must match the three canonical scope fixtures")
    if tuple(report.invalidation_case_ids) != INVALIDATION_CASE_IDS:
        blockers.append("invalidation case ids do not match the static v1 contract")
    for rel_path, spec in expected.items():
        path = repo_root / rel_path
        try:
            payload = _read_json(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            blockers.append(f"{rel_path}: fixture unreadable or missing ({exc.__class__.__name__})")
            continue
        fixtures.append({"path": rel_path, "schema_version": str(payload.get("schema_version", ""))})
        blockers.extend(_base_fixture_blockers(payload, Path(rel_path)))
        for key, value in spec.items():
            if payload.get(key) != value:
                blockers.append(f"{rel_path}: {key} does not match the declarative v1 contract")
        if payload != spec:
            missing = sorted(set(spec) - set(payload))
            extra = sorted(set(payload) - set(spec))
            if missing:
                blockers.append(f"{rel_path}: missing contract fields: {', '.join(missing)}")
            if extra:
                blockers.append(f"{rel_path}: unexpected fields: {', '.join(extra)}")
    return blockers, fixtures


def validate_docs(repo_root: Path) -> list[str]:
    path = repo_root / DOC_PATH
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"{DOC_PATH.as_posix()}: unreadable ({exc.__class__.__name__})"]
    return [f"{DOC_PATH.as_posix()}: missing marker {marker!r}" for marker in DOC_MARKERS if marker not in text]


def build_report(repo_root: Path) -> dict[str, Any]:
    scope = load_context_report(repo_root)
    blockers = validate_context_report(scope)
    blockers.extend(validate_docs(repo_root))
    fixture_blockers, fixture_rows = validate_fixtures(repo_root, scope)
    blockers.extend(fixture_blockers)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "evidence_class": "declarative_specification",
        "runtime_execution_performed": False,
        "performance_evidence_produced": False,
        "report_id": scope.report_id,
        "v1_scope_document": scope.scope_document,
        "canonical_route": scope.canonical_route,
        "route_ids": list(scope.prepared_route_ids),
        "supported_input_formats": list(scope.supported_input_formats),
        "state_owner": scope.state_owner,
        "reuse_scope": scope.reuse_scope,
        "reuse_policy": scope.reuse_policy,
        "query_answers_cached": scope.query_answers_cached,
        "invalidation_case_ids": list(scope.invalidation_case_ids),
        "golden_fixture_paths": list(scope.golden_fixture_paths),
        "required_runtime_fields": list(scope.required_runtime_fields),
        "unsupported_boundary_ids": list(scope.unsupported_boundary_ids),
        "fixture_specs": fixture_rows,
        "all_no_fallback_no_external_engine": scope.all_no_fallback_no_external_engine,
        "v1_scope_ready": scope.v1_scope_ready and not blockers,
        "claim_gate_status": scope.claim_gate_status,
        "performance_claim_allowed": scope.performance_claim_allowed,
        "production_claim_allowed": scope.production_claim_allowed,
        "spark_replacement_claim_allowed": scope.spark_replacement_claim_allowed,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "package_upload_attempted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    report = build_report(repo_root)
    _write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
