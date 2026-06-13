#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate public status, claim-boundary, and compute-flow doc anchors."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from release_report_utils import (
    fail_closed_fields,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)
from check_public_claim_language import (
    SCHEMA_VERSION as PUBLIC_CLAIM_LANGUAGE_SCHEMA_VERSION,
)
from check_public_claim_language import build_report as build_public_claim_language_report
from check_v1_inclusion_scope import SCHEMA_VERSION as V1_INCLUSION_SCOPE_SCHEMA_VERSION
from check_v1_inclusion_scope import build_report as build_v1_inclusion_scope_report
from check_v1_front_door_runtime_scope import (
    SCHEMA_VERSION as V1_FRONT_DOOR_RUNTIME_SCOPE_SCHEMA_VERSION,
)
from check_v1_front_door_runtime_scope import (
    build_report as build_v1_front_door_runtime_scope_report,
)
from check_v1_vortex_runtime_scope import (
    SCHEMA_VERSION as V1_VORTEX_RUNTIME_SCOPE_SCHEMA_VERSION,
)
from check_v1_vortex_runtime_scope import build_report as build_v1_vortex_runtime_scope_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.public_status_docs_report.v1"
PUBLIC_STATUS_REF = Path("docs/release/public-status-matrix.md")

CANONICAL_PUBLIC_STATUS_MARKERS = (
    "shardloom.public_status_matrix.v1",
    "canonical public status and claim-boundary owner",
    "docs/release/finished-product-scope.md",
    "docs/release/v1-inclusion-scope-matrix.md",
    "public_release_claim_allowed=false",
    "public_package_claim_allowed=false",
    "performance_claim_allowed=false",
    "performance_superiority_claim_allowed=false",
    "production_claim_allowed=false",
    "spark_replacement_claim_allowed=false",
    "broad_engine_replacement_claim_allowed=false",
    "drop_in_replacement_claim_allowed=false",
    "production_platform_claim_allowed=false",
    "publication_attempted=false",
    "tag_created=false",
    "package_upload_attempted=false",
    "fallback_attempted=false",
    "external_engine_invoked=false",
)

PUBLIC_DOC_MARKERS = {
    "README.md": (
        PUBLIC_STATUS_REF.as_posix(),
        "docs/release/finished-product-scope.md",
        "Current Support Posture",
        "package-channel evidence is still gated",
    ),
    "docs/getting-started/install.md": (
        PUBLIC_STATUS_REF.as_posix(),
        "Public status is owned by",
        "not a PyPI, Conda, Homebrew, GHCR, crates.io, production, or performance claim",
    ),
    "docs/getting-started/first-10-minutes.md": (
        PUBLIC_STATUS_REF.as_posix(),
        "Public status is owned by",
        "local technical-preview evidence only",
    ),
    "docs/getting-started/examples.md": (
        PUBLIC_STATUS_REF.as_posix(),
        "Public status is owned by",
        "local and no-fallback by default",
    ),
    "python/README.md": (
        PUBLIC_STATUS_REF.as_posix(),
        "Public status is owned by",
        "pre-release",
    ),
    "docs/release/public-technical-preview-readiness.md": (
        PUBLIC_STATUS_REF.as_posix(),
        "Current public posture is owned by",
        "Historical validation snapshot",
    ),
    "docs/release/v1-inclusion-scope-matrix.md": (
        "shardloom.v1_inclusion_scope_matrix.v1",
        "v1_inclusion_scope_required_rows_cannot_be_report_only=true",
        "v1_inclusion_scope_deferred_rows_require_unsupported_diagnostics=true",
    ),
    "docs/architecture/v1-front-door-runtime-scope.md": (
        "shardloom.v1_front_door_runtime_scope.v1",
        "ShardLoomContext.front_door_parity_matrix()",
        "ShardLoomContext.user_route_capability_report()",
        "runtime_execution=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ),
    "docs/architecture/v1-vortex-runtime-scope.md": (
        "shardloom.v1_vortex_runtime_scope.v1",
        "ShardLoomContext.local_vortex_primitive_route_report()",
        "ShardLoomContext.user_route_capability_report()",
        "feature_gated_local_vortex_runtime",
        "object_store_vortex_io",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ),
}

COMPUTE_FLOW_MARKERS = {
    "docs/architecture/compute-engine-flow-reference.md": (
        "canonical ShardLoom compute-flow reference",
        "Historical alignment review",
        "This reference, not the overhaul review, owns current compute-flow vocabulary",
    ),
    "docs/architecture/compute-engine-flow-overhaul-review.md": (
        "Status: historical alignment review",
        "This file is historical only",
        "Historical Next Move At Completion",
    ),
    "docs/use-cases/reference-backlinks.md": (
        "docs/architecture/compute-engine-flow-reference.md",
        PUBLIC_STATUS_REF.as_posix(),
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/public-status-docs-report.json"),
    )
    return parser.parse_args()


def build_report(repo_root: Path) -> dict[str, Any]:
    blockers: list[str] = []
    checked_docs: list[str] = []

    canonical_text = read_text(resolve_path(repo_root, PUBLIC_STATUS_REF))
    blockers.extend(
        require_markers(
            PUBLIC_STATUS_REF.as_posix(),
            canonical_text,
            CANONICAL_PUBLIC_STATUS_MARKERS,
        )
    )
    checked_docs.append(PUBLIC_STATUS_REF.as_posix())

    for rel_path, markers in PUBLIC_DOC_MARKERS.items():
        blockers.extend(
            require_markers(rel_path, read_text(resolve_path(repo_root, rel_path)), markers)
        )
        checked_docs.append(rel_path)

    for rel_path, markers in COMPUTE_FLOW_MARKERS.items():
        blockers.extend(
            require_markers(rel_path, read_text(resolve_path(repo_root, rel_path)), markers)
        )
        checked_docs.append(rel_path)

    claim_language_report = build_public_claim_language_report(repo_root)
    if claim_language_report.get("schema_version") != PUBLIC_CLAIM_LANGUAGE_SCHEMA_VERSION:
        blockers.append("public claim-language report schema mismatch")
    if claim_language_report.get("status") != "passed":
        blockers.extend(
            f"public claim language: {blocker}"
            for blocker in claim_language_report.get("blockers", [])
        )
    v1_inclusion_scope_report = build_v1_inclusion_scope_report(repo_root)
    if v1_inclusion_scope_report.get("schema_version") != V1_INCLUSION_SCOPE_SCHEMA_VERSION:
        blockers.append("v1 inclusion-scope report schema mismatch")
    if v1_inclusion_scope_report.get("status") != "passed":
        blockers.extend(
            f"v1 inclusion scope: {blocker}"
            for blocker in v1_inclusion_scope_report.get("blockers", [])
        )
    v1_front_door_runtime_scope_report = build_v1_front_door_runtime_scope_report(repo_root)
    if (
        v1_front_door_runtime_scope_report.get("schema_version")
        != V1_FRONT_DOOR_RUNTIME_SCOPE_SCHEMA_VERSION
    ):
        blockers.append("v1 front-door runtime-scope report schema mismatch")
    if v1_front_door_runtime_scope_report.get("status") != "passed":
        blockers.extend(
            f"v1 front-door runtime scope: {blocker}"
            for blocker in v1_front_door_runtime_scope_report.get("blockers", [])
        )
    v1_vortex_runtime_scope_report = build_v1_vortex_runtime_scope_report(repo_root)
    if (
        v1_vortex_runtime_scope_report.get("schema_version")
        != V1_VORTEX_RUNTIME_SCOPE_SCHEMA_VERSION
    ):
        blockers.append("v1 Vortex runtime-scope report schema mismatch")
    if v1_vortex_runtime_scope_report.get("status") != "passed":
        blockers.extend(
            f"v1 Vortex runtime scope: {blocker}"
            for blocker in v1_vortex_runtime_scope_report.get("blockers", [])
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "canonical_public_status_matrix": PUBLIC_STATUS_REF.as_posix(),
        "checked_docs": checked_docs,
        "checked_doc_count": len(checked_docs),
        "public_claim_language_report": claim_language_report,
        "public_claim_language_status": claim_language_report.get("status", "missing"),
        "v1_inclusion_scope_report": v1_inclusion_scope_report,
        "v1_inclusion_scope_status": v1_inclusion_scope_report.get("status", "missing"),
        "v1_front_door_runtime_scope_report": v1_front_door_runtime_scope_report,
        "v1_front_door_runtime_scope_status": v1_front_door_runtime_scope_report.get(
            "status",
            "missing",
        ),
        "v1_vortex_runtime_scope_report": v1_vortex_runtime_scope_report,
        "v1_vortex_runtime_scope_status": v1_vortex_runtime_scope_report.get(
            "status",
            "missing",
        ),
        "claim_gate_status": "not_claim_grade",
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
