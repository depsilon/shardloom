#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Build and validate the user-surface runtime gap inventory.

This gate is the first GAR-RUNTIME-IMPL-6D child slice. It does not close any
runtime gaps by itself; it prevents broad "unsupported" language from hiding
whether a row needs a front door, an output route, claim evidence, true runtime
implementation, or policy rejection.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_artifact_completeness import result_rows as benchmark_result_rows
from check_python_user_surface_completion import _load_dataframe_method_rows
from check_sql_python_dataframe_parity import build_report as build_parity_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.user_surface_runtime_gap_inventory.v1"
GATE_ID = "gar-runtime-impl-6d.user_surface_runtime_gap_inventory"

CLASSIFICATIONS = {
    "runtime_available_needs_front_door",
    "runtime_available_needs_output_route",
    "runtime_available_needs_claim_evidence",
    "true_runtime_expansion_item",
    "policy_rejected",
}

INVENTORIED_RUNS_TODAY_STATES = {"blocked", "future", "report_only"}
INVENTORIED_WEBSITE_STATUSES = {"blocked", "report_only"}
STATUS_TERMS = (
    "unsupported",
    "blocked",
    "not complete",
    "front_door_gap",
    "report-only",
    "report_only",
)

DOC_STATUS_FILES = (
    Path("README.md"),
    Path("docs/getting-started/first-10-minutes.md"),
    Path("docs/architecture/sql-python-dataframe-front-door-parity.md"),
    Path("website-src/src/pages/start.astro"),
    Path("examples/local-vortex-benchmark/README.md"),
)


FRONT_DOOR_GAP_ROUTES: dict[str, dict[str, str]] = {
    "native_vortex_general_runtime": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "native_vortex_boundary",
        "runtime_route": (
            "scoped local Vortex primitives exist; broad native Vortex read-transform-write "
            "runtime remains a GAR-RUNTIME-IMPL-6D expansion item"
        ),
        "output_or_evidence_route": (
            "primitive report rows and Native I/O evidence today; broad native Vortex output route "
            "requires vortex reader/writer/operator coverage"
        ),
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.native_vortex_general_runtime",
    },
    "object_store_lakehouse_catalog": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "object_store_or_table_source_to_vortex_pending",
        "runtime_route": "object-store/table runtime, commit, rollback, and catalog work pending",
        "output_or_evidence_route": "blocked diagnostic or report-only evidence until runtime lands",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_catalog",
    },
    "arbitrary_sql_python_dataframe_breadth": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "front_door_expression_to_vortex_plan_pending",
        "runtime_route": "broad SQL grammar, expression registry, DataFrame API, UDF, and effect policy pending",
        "output_or_evidence_route": "deterministic diagnostic until broad semantic/runtime evidence lands",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    },
    "performance_equivalence": {
        "classification": "runtime_available_needs_claim_evidence",
        "vortex_normalization_point": "route_specific_vortex_boundary_required_in_benchmark_manifest",
        "runtime_route": "scoped shared runtime paths exist for admitted rows",
        "output_or_evidence_route": "front-door equivalent benchmark manifest and claim evidence pending",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.performance_equivalence",
    },
}

DATAFRAME_METHOD_FRONT_DOOR_GAPS = (
    "sample",
    "explode",
    "pivot",
    "pivot_table",
    "melt",
    "rolling",
    "tail",
    "describe",
    "duplicated",
    "mask",
    "replace",
    "set_index",
    "reset_index",
    "sort_index",
    "apply",
    "pipe",
    "transform",
    "applymap",
    "map",
    "map_rows",
)

DATAFRAME_METHOD_FRONT_DOOR_ROUTE = {
    "classification": "true_runtime_expansion_item",
    "vortex_normalization_point": "dataframe_front_door_to_vortex_plan_pending",
    "runtime_route": (
        "Python LazyFrame method exists as a deterministic fail-closed front door; "
        "admitted native runtime remains pending"
    ),
    "output_or_evidence_route": (
        "workflow-unsupported-plan diagnostic until the method has semantic, runtime, "
        "and output evidence"
    ),
    "owner": "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
}

DATAFRAME_METHOD_EXPRESSION_ROUTE = {
    "classification": "true_runtime_expansion_item",
    "vortex_normalization_point": "dataframe_expression_to_vortex_plan_pending",
    "runtime_route": (
        "Python LazyFrame expression front door exists as a deterministic fail-closed "
        "diagnostic; admitted native expression parsing and execution remain pending"
    ),
    "output_or_evidence_route": (
        "workflow-unsupported-plan diagnostic until typed expression semantics, native "
        "execution, and no-fallback evidence land"
    ),
    "owner": "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
}

DATAFRAME_METHOD_GAP_ROUTES: dict[str, dict[str, str]] = {
    method: DATAFRAME_METHOD_FRONT_DOOR_ROUTE
    for method in DATAFRAME_METHOD_FRONT_DOOR_GAPS
}
DATAFRAME_METHOD_GAP_ROUTES.update(
    {
        "eval": DATAFRAME_METHOD_EXPRESSION_ROUTE,
        "schema_contract": {
            "classification": "true_runtime_expansion_item",
            "vortex_normalization_point": (
                "local_source_or_vortex_schema_to_contract_runtime_pending"
            ),
            "runtime_route": "schema contract enforcement runtime pending",
            "output_or_evidence_route": (
                "diagnostic report until contract enforcement evidence lands"
            ),
            "owner": "GAR-RUNTIME-IMPL-6D:last_order.schema_contract_runtime",
        },
    }
)

RUNS_TODAY_GAP_ROUTES: dict[str, dict[str, str]] = {
    "input_object_store_cloud": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "credentialed_object_store_source_to_vortex_pending",
        "runtime_route": "cloud object-store runtime and credential policy pending",
        "output_or_evidence_route": "blocked diagnostic until credentialed runtime lands",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.object_store_runtime",
    },
    "execution_report_only_surfaces": {
        "classification": "runtime_available_needs_claim_evidence",
        "vortex_normalization_point": "route_specific_or_not_applicable_for_report_only_surface",
        "runtime_route": "planning/report surfaces require execution admission before runtime use",
        "output_or_evidence_route": "report-only evidence; execution certificate pending",
        "owner": "GAR-RUNTIME-IMPL-6D:execution_admission",
    },
    "execution_live_hybrid_remote_distributed": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "live_hybrid_remote_source_to_vortex_state_pending",
        "runtime_route": "live/hybrid/remote/distributed execution fabric pending",
        "output_or_evidence_route": "future runtime evidence pending",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_remote_distributed",
    },
    "claim_production_readiness": {
        "classification": "runtime_available_needs_claim_evidence",
        "vortex_normalization_point": "all_claimed_routes_must_name_vortex_boundary",
        "runtime_route": "scoped local runtime exists; production gates remain blocked",
        "output_or_evidence_route": "release/production usability evidence pending",
        "owner": "release.production_readiness_gate",
    },
    "claim_performance_superiority": {
        "classification": "runtime_available_needs_claim_evidence",
        "vortex_normalization_point": "benchmark_route_specific",
        "runtime_route": "benchmark rows exist; superiority claim evidence pending",
        "output_or_evidence_route": "CG-5/CG-6 correctness and benchmark evidence pending",
        "owner": "cg5.cg6.claim_grade_correctness_and_benchmark_evidence_required",
    },
    "claim_package_publication": {
        "classification": "runtime_available_needs_claim_evidence",
        "vortex_normalization_point": "not_applicable_package_distribution_gate",
        "runtime_route": "selected package channels provide proof-backed package access only",
        "output_or_evidence_route": (
            "production, performance, broad runtime, and future channel claims remain blocked"
        ),
        "owner": "release.package_publication_gate",
    },
    "claim_object_store_lakehouse_foundry_production": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "platform_or_table_source_to_vortex_pending",
        "runtime_route": "object-store/lakehouse/Foundry production runtime pending",
        "output_or_evidence_route": "platform production evidence pending",
        "owner": "platform.production_integration_evidence_required",
    },
}

WEBSITE_STATUS_ROUTES: dict[str, dict[str, str]] = {
    "iceberg-delta-hudi": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "lakehouse_table_source_to_vortex_pending",
        "runtime_route": "Iceberg/Delta/Hudi production runtime and commits pending",
        "output_or_evidence_route": "blocked diagnostic and local rehearsal evidence only",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.lakehouse_table_runtime",
    },
    "package-release": {
        "classification": "runtime_available_needs_claim_evidence",
        "vortex_normalization_point": "not_applicable_package_distribution_gate",
        "runtime_route": "local source checkout runtime only",
        "output_or_evidence_route": "release readiness report pending public-package evidence",
        "owner": "release.package_publication_gate",
    },
    "public-workflow-route-facade": {
        "classification": "runtime_available_needs_output_route",
        "vortex_normalization_point": "route_specific_or_public_prepare_vortex_target",
        "runtime_route": (
            "side-effect-free route admission plus scoped run/prepare wrappers attach route "
            "metadata to admitted runtime envelopes; bounded local-source collect, general "
            "local-source writes, generated-source direct writes, source-free SQL writes, and "
            "admitted local/generated fanout helpers and explicit native Vortex primitive "
            "collect/local-execution helpers now use the public run facade"
        ),
        "output_or_evidence_route": (
            "typed public route and execution-facade envelopes today; lower smoke/runtime/primitive "
            "commands are client_only evidence surfaces, while future helper families and future "
            "native Vortex write-helper payloads remain deferred until their owning runtime items "
            "define explicit payload contracts"
        ),
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar",
    },
    "sql-dataframe": {
        "classification": "true_runtime_expansion_item",
        "vortex_normalization_point": "front_door_expression_to_vortex_plan_pending",
        "runtime_route": "scoped local runtime exists; broad SQL/DataFrame parity pending",
        "output_or_evidence_route": "diagnostic/evidence/local fanout today; broad route evidence pending",
        "owner": "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    },
}

DOC_STATUS_PATTERNS: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "v1 supported/unsupported surface",
        "runtime_available_needs_claim_evidence",
        "route_specific_v1_support_boundary",
        "generated v1 supported/unsupported surface",
        "release.v1_docs_productization_gate",
    ),
    (
        "v1-supported-unsupported.md",
        "runtime_available_needs_claim_evidence",
        "route_specific_v1_support_boundary",
        "generated v1 supported/unsupported surface",
        "release.v1_docs_productization_gate",
    ),
    (
        "package user install status",
        "runtime_available_needs_claim_evidence",
        "not_applicable_package_distribution_gate",
        "package-channel release evidence pending",
        "release.package_publication_gate",
    ),
    (
        "report-only or blocked status for broader",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "status matrix row",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "unsupported work must emit",
        "policy_rejected",
        "not_applicable_policy_diagnostic",
        "deterministic blocker diagnostic",
        "no_fallback_policy",
    ),
    (
        "unbounded convenience materializations return deterministic evidence",
        "runtime_available_needs_output_route",
        "local_source_to_bounded_decoded_materialization_boundary",
        "bounded materialization works; unbounded output route blocks deterministically",
        "GAR-RUNTIME-IMPL-6D:last_order.materialization_output_route",
    ),
    (
        "unsupported = ctx.read",
        "runtime_available_needs_output_route",
        "local_source_to_bounded_decoded_materialization_boundary",
        "unbounded materialization blocker evidence",
        "GAR-RUNTIME-IMPL-6D:last_order.materialization_output_route",
    ),
    (
        "unsupported-claim rows",
        "runtime_available_needs_claim_evidence",
        "route_specific_vortex_boundary_required",
        "production usability status report",
        "release.production_usability_gate",
    ),
    (
        "quickstart_unsupported_blocker_id",
        "policy_rejected",
        "not_applicable_policy_diagnostic",
        "quickstart deterministic unsupported blocker",
        "quickstart.no_fallback_diagnostic",
    ),
    (
        "unsupported path lacks a blocker",
        "policy_rejected",
        "not_applicable_policy_diagnostic",
        "quickstart deterministic unsupported blocker",
        "quickstart.no_fallback_diagnostic",
    ),
    (
        "remains blocked for public release",
        "runtime_available_needs_claim_evidence",
        "not_applicable_package_distribution_gate",
        "public release evidence pending",
        "release.public_package_gate",
    ),
    (
        "that remains blocked",
        "runtime_available_needs_claim_evidence",
        "not_applicable_package_distribution_gate",
        "public release evidence pending",
        "release.public_package_gate",
    ),
    (
        "unsupported shapes",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "GAR-RUNTIME-IMPL-6D runtime-expansion checklist",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "front_door_gap",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "front-door parity matrix gap row",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "runtime_gap_status",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "precise front-door runtime gap status",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "benchmark_publication_pending",
        "runtime_available_needs_claim_evidence",
        "route_specific_vortex_boundary_required",
        "front-door benchmark publication evidence pending",
        "GAR-RUNTIME-IMPL-6D:last_order.claim_evidence",
    ),
    (
        "generic `unsupported`",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "generic unsupported language rejected by validator",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "generic unsupported or",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "generic unsupported language rejected by validator",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "blocked posture",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "generic blocked posture rejected by validator",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "claims remain blocked until evidence exists",
        "runtime_available_needs_claim_evidence",
        "route_specific_vortex_boundary_required",
        "claim evidence pending",
        "GAR-RUNTIME-IMPL-6D:last_order.claim_evidence",
    ),
    (
        "--allow-blocked",
        "runtime_available_needs_claim_evidence",
        "route_specific_vortex_boundary_required",
        "blocked release architecture/final rehearsal report",
        "release.claim_evidence_gate",
    ),
    (
        "blocked, and unsupported paths",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "status matrix route explanation",
        "GAR-RUNTIME-IMPL-6D:last_order.runtime_expansion",
    ),
    (
        "python exposes the cli status",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "Python/CLI deterministic unsupported diagnostics for non-admitted runtime shapes",
        "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    ),
    (
        "diagnostics, and deduplicated `unsupported_reasons`",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "Python/CLI deterministic unsupported diagnostics for non-admitted runtime shapes",
        "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    ),
    (
        "runtime unsupported diagnostics",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "Python/CLI deterministic unsupported diagnostics for non-admitted runtime shapes",
        "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    ),
    (
        "correlated subquery shapes directly as `status`",
        "true_runtime_expansion_item",
        "route_specific_vortex_boundary_required",
        "Python/CLI deterministic unsupported diagnostics for non-admitted runtime shapes",
        "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    ),
    (
        "fail closed through `workflow-unsupported-plan`",
        "true_runtime_expansion_item",
        "dataframe_front_door_to_vortex_plan_pending",
        "Python/DataFrame deterministic unsupported diagnostics for non-admitted runtime shapes",
        "GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--benchmark-results",
        type=Path,
        default=Path("website/assets/benchmarks/latest/benchmark-results.json"),
    )
    parser.add_argument(
        "--runs-today-matrix",
        type=Path,
        default=Path("docs/status/runs-today-support-matrix.json"),
    )
    parser.add_argument(
        "--website-status-dir",
        type=Path,
        default=Path("website-src/src/content/status"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/user-surface-runtime-gap-inventory.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def is_local_duplicate_copy(path: Path) -> bool:
    stem, separator, suffix = path.stem.rpartition(" ")
    return bool(stem and separator and suffix.isdecimal())


def common_row(
    *,
    source: str,
    row_id: str,
    surface: str,
    observed_status: str,
    classification: str,
    vortex_normalization_point: str,
    runtime_route: str,
    output_or_evidence_route: str,
    owner: str,
    blocker_id: str | None,
    claim_gate_status: str,
    fallback_attempted: bool,
    external_engine_invoked: bool,
    runtime_execution: bool,
    benchmark_range: bool,
    user_visible_refs: list[str],
    required_evidence: list[str],
    claim_boundary: str,
) -> dict[str, Any]:
    return {
        "source": source,
        "row_id": row_id,
        "surface": surface,
        "observed_status": observed_status,
        "classification": classification,
        "vortex_normalization_point": vortex_normalization_point,
        "runtime_route": runtime_route,
        "output_or_evidence_route": output_or_evidence_route,
        "owner": owner,
        "blocker_id": blocker_id or "none",
        "claim_gate_status": claim_gate_status,
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "runtime_execution": runtime_execution,
        "benchmark_range": benchmark_range,
        "user_visible_refs": user_visible_refs,
        "required_evidence": required_evidence,
        "claim_boundary": claim_boundary,
    }


def front_door_gap_rows(repo_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    report = build_parity_report(repo_root)
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for row in report["rows"]:
        row_id = str(row["row_id"])
        if row["parity_status"] != "front_door_gap" and not row["blocker_id"]:
            continue
        route = FRONT_DOOR_GAP_ROUTES.get(row_id)
        if route is None:
            blockers.append(f"front-door parity gap row is unclassified: {row_id}")
            continue
        rows.append(
            common_row(
                source="front_door_parity_matrix",
                row_id=row_id,
                surface=str(row["workflow"]),
                observed_status=str(row.get("runtime_gap_status") or row["parity_status"]),
                classification=route["classification"],
                vortex_normalization_point=route["vortex_normalization_point"],
                runtime_route=route["runtime_route"],
                output_or_evidence_route=route["output_or_evidence_route"],
                owner=route["owner"],
                blocker_id=row["blocker_id"],
                claim_gate_status="not_claim_grade",
                fallback_attempted=row["fallback_attempted"],
                external_engine_invoked=row["external_engine_invoked"],
                runtime_execution=bool(row["runtime_execution"]),
                benchmark_range=row_id in {
                    "native_vortex_general_runtime",
                    "arbitrary_sql_python_dataframe_breadth",
                    "performance_equivalence",
                },
                user_visible_refs=[
                    "python/src/shardloom/context.py:FRONT_DOOR_PARITY_ROWS",
                    "docs/architecture/sql-python-dataframe-front-door-parity.md",
                ],
                required_evidence=list(row["required_evidence"]),
                claim_boundary=str(row["claim_boundary"]),
            )
        )
    if report["status"] != "passed":
        blockers.extend(f"front-door parity gate blocker: {blocker}" for blocker in report["blockers"])
    return rows, blockers


def dataframe_method_gap_rows(repo_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for row in _load_dataframe_method_rows(repo_root):
        support_status = str(row.get("support_status", ""))
        blocker_id = row.get("blocker_id")
        if "unsupported" not in support_status and not blocker_id:
            continue
        method = str(row.get("method"))
        route = DATAFRAME_METHOD_GAP_ROUTES.get(method)
        if route is None:
            blockers.append(f"DataFrame method gap row is unclassified: {method}")
            continue
        rows.append(
            common_row(
                source="dataframe_method_capability_matrix",
                row_id=method,
                surface=f"{row.get('family')}::{method}",
                observed_status=support_status,
                classification=route["classification"],
                vortex_normalization_point=route["vortex_normalization_point"],
                runtime_route=route["runtime_route"],
                output_or_evidence_route=route["output_or_evidence_route"],
                owner=route["owner"],
                blocker_id=str(blocker_id) if blocker_id else None,
                claim_gate_status=str(row.get("claim_gate_status") or "not_claim_grade"),
                fallback_attempted=bool(row.get("fallback_attempted")),
                external_engine_invoked=bool(row.get("external_engine_invoked")),
                runtime_execution=bool(row.get("runtime_execution")),
                benchmark_range=False,
                user_visible_refs=[
                    "python/src/shardloom/context.py:DATAFRAME_METHOD_CAPABILITY_ROWS",
                    "python/README.md",
                ],
                required_evidence=list(row.get("required_evidence") or []),
                claim_boundary=str(row.get("claim_boundary") or ""),
            )
        )
    return rows, blockers


def runs_today_gap_rows(repo_root: Path, matrix_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    payload = load_json(matrix_path)
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for raw in payload.get("rows", []):
        if not isinstance(raw, dict):
            continue
        support_state = str(raw.get("support_state", ""))
        if support_state not in INVENTORIED_RUNS_TODAY_STATES:
            continue
        row_id = str(raw.get("id"))
        route = RUNS_TODAY_GAP_ROUTES.get(row_id)
        if route is None:
            blockers.append(f"runs-today support row is unclassified: {row_id}")
            continue
        rows.append(
            common_row(
                source="runs_today_support_matrix",
                row_id=row_id,
                surface=",".join(str(item) for item in raw.get("surface", [])),
                observed_status=support_state,
                classification=route["classification"],
                vortex_normalization_point=route["vortex_normalization_point"],
                runtime_route=route["runtime_route"],
                output_or_evidence_route=route["output_or_evidence_route"],
                owner=route["owner"],
                blocker_id=str(raw.get("blocker_id") or "none"),
                claim_gate_status=str(raw.get("claim_gate_status") or "not_claim_grade"),
                fallback_attempted=bool(raw.get("fallback_attempted")),
                external_engine_invoked=bool(raw.get("external_engine_invoked")),
                runtime_execution=bool(raw.get("runtime_execution")),
                benchmark_range=row_id in {
                    "execution_report_only_surfaces",
                    "claim_performance_superiority",
                },
                user_visible_refs=[rel(repo_root, matrix_path)],
                required_evidence=list(raw.get("evidence_refs") or []),
                claim_boundary=str(raw.get("claim_boundary") or ""),
            )
        )
    return rows, blockers


def website_status_gap_rows(repo_root: Path, status_dir: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for path in sorted(status_dir.glob("*.json")):
        if is_local_duplicate_copy(path):
            continue
        payload = load_json(path)
        status = str(payload.get("status", ""))
        if status not in INVENTORIED_WEBSITE_STATUSES:
            continue
        row_id = path.stem
        route = WEBSITE_STATUS_ROUTES.get(row_id)
        if route is None:
            blockers.append(f"website status row is unclassified: {row_id}")
            continue
        rows.append(
            common_row(
                source="website_status_content",
                row_id=row_id,
                surface=str(payload.get("capability") or row_id),
                observed_status=status,
                classification=route["classification"],
                vortex_normalization_point=route["vortex_normalization_point"],
                runtime_route=route["runtime_route"],
                output_or_evidence_route=route["output_or_evidence_route"],
                owner=route["owner"],
                blocker_id="none",
                claim_gate_status="not_claim_grade",
                fallback_attempted=False,
                external_engine_invoked=False,
                runtime_execution=status == "runtime_supported",
                benchmark_range=row_id == "sql-dataframe",
                user_visible_refs=[rel(repo_root, path)],
                required_evidence=list(payload.get("evidence") or []),
                claim_boundary=str(payload.get("blocked") or ""),
            )
        )
    return rows, blockers


def classify_doc_line(line: str) -> tuple[str, str, str, str, str] | None:
    lower = line.lower()
    for pattern, classification, normalization, output_route, owner in DOC_STATUS_PATTERNS:
        if pattern in lower:
            return classification, normalization, "documentation_reference", output_route, owner
    return None


def doc_status_rows(repo_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for rel_path in DOC_STATUS_FILES:
        path = repo_root / rel_path
        if not path.exists():
            blockers.append(f"user-facing status doc missing: {rel_path.as_posix()}")
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            lowered = line.lower()
            if not any(term in lowered for term in STATUS_TERMS):
                continue
            classified = classify_doc_line(line)
            if classified is None:
                blockers.append(
                    "user-facing status text is unclassified: "
                    f"{rel_path.as_posix()}:{line_number}: {line.strip()}"
                )
                continue
            classification, normalization, runtime_route, output_route, owner = classified
            rows.append(
                common_row(
                    source="user_facing_doc_status_text",
                    row_id=f"{rel_path.as_posix()}:{line_number}",
                    surface=line.strip(),
                    observed_status="status_text_reference",
                    classification=classification,
                    vortex_normalization_point=normalization,
                    runtime_route=runtime_route,
                    output_or_evidence_route=output_route,
                    owner=owner,
                    blocker_id="doc_status_reference",
                    claim_gate_status="not_claim_grade",
                    fallback_attempted=False,
                    external_engine_invoked=False,
                    runtime_execution=False,
                    benchmark_range=rel_path.as_posix()
                    in {
                        "README.md",
                        "docs/architecture/sql-python-dataframe-front-door-parity.md",
                        "examples/local-vortex-benchmark/README.md",
                    },
                    user_visible_refs=[rel_path.as_posix()],
                    required_evidence=["status_text_classification"],
                    claim_boundary=(
                        "Documentation status language must map to a concrete runtime gap, claim "
                        "gate, output route, or policy rejection instead of generic unsupported prose."
                    ),
                )
            )
    return rows, blockers


def benchmark_support_summary(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    rows = benchmark_result_rows(payload)
    shardloom_rows = [
        row for row in rows if str(row.get("engine", "")).startswith("shardloom")
    ]
    shardloom_unsupported = [
        row
        for row in shardloom_rows
        if str(row.get("status", "")).lower() in {"unsupported", "blocked"}
        or str(row.get("route_runtime_status", "")).lower() == "unsupported"
    ]
    external_unsupported = [
        row
        for row in rows
        if not str(row.get("engine", "")).startswith("shardloom")
        and str(row.get("status", "")).lower().startswith("unsupported")
    ]
    blockers: list[str] = []
    expected_row_count = payload.get("published_benchmark_row_count")
    if isinstance(expected_row_count, int) and len(rows) != expected_row_count:
        blockers.append(
            "benchmark artifact loaded row count does not match published_benchmark_row_count: "
            f"{len(rows)} != {expected_row_count}"
        )
    if not shardloom_rows:
        blockers.append("benchmark artifact loaded no ShardLoom rows")
    if shardloom_unsupported:
        blockers.append(
            "benchmark artifact has ShardLoom unsupported rows: "
            f"{len(shardloom_unsupported)}"
        )
    external_blockers: list[dict[str, Any]] = []
    for row in external_unsupported:
        identity = (
            f"{row.get('engine')}:{row.get('storage_format') or row.get('format')}:"
            f"{row.get('scenario_id') or row.get('scenario')}"
        )
        missing: list[str] = []
        if row.get("external_baseline_only") is not True:
            missing.append("external_baseline_only=true")
        if row.get("fallback_attempted") is not False:
            missing.append("fallback_attempted=false")
        if row.get("external_engine_invoked") is not False:
            missing.append("external_engine_invoked=false")
        missing_evidence = row.get("claim_grade_missing_evidence")
        if not missing_evidence:
            missing.append("claim_grade_missing_evidence")
        if missing:
            external_blockers.append({"row": identity, "missing": missing})
    if external_blockers:
        blockers.append(
            "external unsupported benchmark rows are missing external-baseline classification: "
            f"{len(external_blockers)}"
        )

    shardloom_route_counts = Counter(str(row.get("route_runtime_status")) for row in shardloom_rows)
    external_route_counts = Counter(
        str(row.get("route_runtime_status"))
        for row in rows
        if not str(row.get("engine", "")).startswith("shardloom")
    )
    return (
        {
            "published_row_count": len(rows),
            "shardloom_row_count": len(shardloom_rows),
            "shardloom_unsupported_row_count": len(shardloom_unsupported),
            "shardloom_route_runtime_status_counts": dict(sorted(shardloom_route_counts.items())),
            "external_baseline_unsupported_row_count": len(external_unsupported),
            "external_route_runtime_status_counts": dict(sorted(external_route_counts.items())),
            "external_baseline_unsupported_rows": [
                {
                    "engine": row.get("engine"),
                    "storage_format": row.get("storage_format") or row.get("format"),
                    "scenario_id": row.get("scenario_id") or row.get("scenario"),
                    "status": row.get("status"),
                    "route_runtime_status": row.get("route_runtime_status"),
                    "classification": "external_baseline_limitation",
                    "claim_grade_missing_evidence": row.get("claim_grade_missing_evidence"),
                    "fallback_attempted": row.get("fallback_attempted"),
                    "external_engine_invoked": row.get("external_engine_invoked"),
                }
                for row in external_unsupported
            ],
            "external_baseline_classification_blockers": external_blockers,
        },
        blockers,
    )


def validate_inventory(rows: list[dict[str, Any]], benchmark: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        identity = (str(row.get("source")), str(row.get("row_id")))
        if identity in seen:
            blockers.append(f"duplicate inventory row: {identity[0]}:{identity[1]}")
        seen.add(identity)
        classification = str(row.get("classification", ""))
        if classification not in CLASSIFICATIONS:
            blockers.append(f"{identity[0]}:{identity[1]} has invalid classification {classification!r}")
        for field in (
            "observed_status",
            "vortex_normalization_point",
            "runtime_route",
            "output_or_evidence_route",
            "owner",
            "claim_boundary",
        ):
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                blockers.append(f"{identity[0]}:{identity[1]} missing {field}")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{identity[0]}:{identity[1]} must preserve fallback_attempted=false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{identity[0]}:{identity[1]} must preserve external_engine_invoked=false")
        if classification != "policy_rejected" and row.get("claim_gate_status") == "claim_grade":
            blockers.append(f"{identity[0]}:{identity[1]} must not claim claim_grade while inventoried")
        observed_status = str(row.get("observed_status", "")).lower()
        if (
            row.get("benchmark_range") is True
            and "unsupported" in observed_status
            and classification
            in {"runtime_available_needs_front_door", "runtime_available_needs_output_route"}
        ):
            blockers.append(
                f"{identity[0]}:{identity[1]} is benchmark-range and should not use generic "
                "unsupported for a front-door/output-route gap"
            )
        if (
            row.get("source") == "front_door_parity_matrix"
            and observed_status in {"unsupported", "blocked", "not complete", "not_complete"}
        ):
            blockers.append(
                f"{identity[0]}:{identity[1]} front-door gap must use a precise runtime gap "
                "status instead of generic unsupported/blocked language"
            )
    if benchmark["shardloom_unsupported_row_count"] != 0:
        blockers.append("benchmark summary must report zero ShardLoom unsupported rows")
    if benchmark["external_baseline_classification_blockers"]:
        blockers.append("external benchmark unsupported rows must remain external-baseline-only")
    return blockers


def build_report(
    *,
    repo_root: Path,
    benchmark_results: Path,
    runs_today_matrix: Path,
    website_status_dir: Path,
) -> dict[str, Any]:
    inventory_rows: list[dict[str, Any]] = []
    blockers: list[str] = []

    for rows, row_blockers in (
        front_door_gap_rows(repo_root),
        dataframe_method_gap_rows(repo_root),
        runs_today_gap_rows(repo_root, runs_today_matrix),
        website_status_gap_rows(repo_root, website_status_dir),
        doc_status_rows(repo_root),
    ):
        inventory_rows.extend(rows)
        blockers.extend(row_blockers)

    benchmark, benchmark_blockers = benchmark_support_summary(load_json(benchmark_results))
    blockers.extend(benchmark_blockers)

    inventory_blockers = validate_inventory(inventory_rows, benchmark)
    blockers.extend(inventory_blockers)

    classification_counts = Counter(str(row["classification"]) for row in inventory_rows)
    source_counts = Counter(str(row["source"]) for row in inventory_rows)
    observed_status_counts = Counter(str(row["observed_status"]) for row in inventory_rows)

    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "covered_phase_items": ["GAR-RUNTIME-IMPL-6D"],
        "classification_vocabulary": sorted(CLASSIFICATIONS),
        "inventory_row_count": len(inventory_rows),
        "classification_counts": dict(sorted(classification_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "observed_status_counts": dict(sorted(observed_status_counts.items())),
        "benchmark_support_summary": benchmark,
        "inventory_rows": inventory_rows,
        "acceptance_summary": {
            "shardloom_benchmark_unsupported_rows": benchmark["shardloom_unsupported_row_count"],
            "external_baseline_unsupported_rows": benchmark[
                "external_baseline_unsupported_row_count"
            ],
            "all_inventory_rows_classified": not any(
                row["classification"] not in CLASSIFICATIONS for row in inventory_rows
            ),
            "all_inventory_rows_no_fallback_no_external_engine": all(
                row["fallback_attempted"] is False and row["external_engine_invoked"] is False
                for row in inventory_rows
            ),
            "claim_gate_status": "not_claim_grade",
            "fallback_attempted": False,
            "external_engine_invoked": False,
        },
        "claim_boundary": (
            "This inventory classifies current user-surface gaps. It does not widen runtime "
            "support, authorize broad SQL/Python/DataFrame flexibility, make performance or "
            "production claims, publish packages, or permit fallback execution."
        ),
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(
        repo_root=repo_root,
        benchmark_results=resolve(repo_root, args.benchmark_results),
        runs_today_matrix=resolve(repo_root, args.runs_today_matrix),
        website_status_dir=resolve(repo_root, args.website_status_dir),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"user-surface runtime gap inventory blocker: {blocker}")
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
