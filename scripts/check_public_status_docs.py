#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate public status, claim-boundary, and compute-flow doc anchors."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.public_status_docs_report.v1"
PUBLIC_STATUS_REF = Path("docs/release/public-status-matrix.md")

CANONICAL_PUBLIC_STATUS_MARKERS = (
    "shardloom.public_status_matrix.v1",
    "canonical public status and claim-boundary owner",
    "public_release_claim_allowed=false",
    "public_package_claim_allowed=false",
    "performance_claim_allowed=false",
    "production_claim_allowed=false",
    "spark_replacement_claim_allowed=false",
    "publication_attempted=false",
    "tag_created=false",
    "package_upload_attempted=false",
    "fallback_attempted=false",
    "external_engine_invoked=false",
)

PUBLIC_DOC_MARKERS = {
    "README.md": (
        PUBLIC_STATUS_REF.as_posix(),
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


def resolve(repo_root: Path, path: Path | str) -> Path:
    path = Path(path)
    return path if path.is_absolute() else repo_root / path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def require_markers(label: str, text: str, markers: tuple[str, ...]) -> list[str]:
    blockers: list[str] = []
    if not text:
        return [f"{label}: missing file or empty text"]
    for marker in markers:
        if marker not in text:
            blockers.append(f"{label}: missing marker {marker!r}")
    return blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    blockers: list[str] = []
    checked_docs: list[str] = []

    canonical_text = read_text(resolve(repo_root, PUBLIC_STATUS_REF))
    blockers.extend(
        require_markers(
            PUBLIC_STATUS_REF.as_posix(),
            canonical_text,
            CANONICAL_PUBLIC_STATUS_MARKERS,
        )
    )
    checked_docs.append(PUBLIC_STATUS_REF.as_posix())

    for rel_path, markers in PUBLIC_DOC_MARKERS.items():
        blockers.extend(require_markers(rel_path, read_text(resolve(repo_root, rel_path)), markers))
        checked_docs.append(rel_path)

    for rel_path, markers in COMPUTE_FLOW_MARKERS.items():
        blockers.extend(require_markers(rel_path, read_text(resolve(repo_root, rel_path)), markers))
        checked_docs.append(rel_path)

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "canonical_public_status_matrix": PUBLIC_STATUS_REF.as_posix(),
        "checked_docs": checked_docs,
        "checked_doc_count": len(checked_docs),
        "claim_gate_status": "not_claim_grade",
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "package_upload_attempted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root)
    write_json(output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
