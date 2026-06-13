#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate public claim wording and finished-product v1 claim rows.

The validator permits external-engine names in no-fallback policy, unsupported diagnostics,
benchmark baseline labels, migration/oracle references, and historical RFC/ledger context. It
blocks positive public wording that implies broad replacement, superiority, production platform
support, or broad SQL/DataFrame parity without a closed claim row.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, read_text, require_markers, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.public_claim_language_report.v1"

FINISHED_PRODUCT_SCOPE = Path("docs/release/finished-product-scope.md")
PER_CLAIM_MATRIX = Path("docs/release/per-claim-evidence-attachment-matrix.md")
PUBLIC_STATUS_MATRIX = Path("docs/release/public-status-matrix.md")
KNOWN_UNSUPPORTED_PATHS = Path("docs/release/known-unsupported-paths.md")

REQUIRED_V1_CLAIM_ROWS = (
    "local_runtime_product_claim",
    "api_schema_stability_claim",
    "supported_front_door_scope_claim",
    "supported_vortex_route_claim",
    "supported_output_sink_claim",
    "security_supply_chain_claim",
    "external_baseline_comparison_claim",
)

REQUIRED_RELEASE_CLAIM_ROWS = (
    "public_release_claim",
    "public_package_claim",
)

OUT_OF_V1_CLAIM_ROWS = (
    "performance_superiority_claim",
    "spark_displacement_claim",
    "engine_replacement_claim",
    "production_sql_dataframe_claim",
    "object_store_lakehouse_claim",
    "foundry_platform_claim",
)

FINISHED_SCOPE_MARKERS = (
    "shardloom.finished_product_scope.v1",
    "Vortex-first",
    "no-fallback",
    "Required V1 Claim Rows",
    "Out-of-V1 Claim Rows",
    "Allowed External Engine Contexts",
    "PulseWeave",
    "capillary",
    "dynamic admission",
    "timing-surface",
    "evidence-tier",
)

PUBLIC_STATUS_MARKERS = (
    "docs/release/finished-product-scope.md",
    "performance_superiority_claim_allowed=false",
    "spark_replacement_claim_allowed=false",
    "broad_engine_replacement_claim_allowed=false",
    "drop_in_replacement_claim_allowed=false",
    "production_platform_claim_allowed=false",
)

KNOWN_UNSUPPORTED_MARKERS = (
    "outside the current finished-product v1 support boundary",
    "unsupported surfaces are explicit boundaries",
    "fallback_attempted=false",
    "external_engine_invoked=false",
)

PER_CLAIM_MARKERS = (
    "per_claim_evidence_attachment_matrix_required_v1_row_count=7",
    "per_claim_evidence_attachment_matrix_out_of_v1_row_count=6",
    "per_claim_evidence_attachment_matrix_external_baseline_context_allowed=true",
    "per_claim_evidence_attachment_matrix_performance_superiority_claim_allowed=false",
    "per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false",
    "per_claim_evidence_attachment_matrix_engine_replacement_claim_allowed=false",
)

PUBLIC_SCAN_GLOBS = (
    "README.md",
    "python/README.md",
    "docs/getting-started/**/*.md",
    "docs/release/*.md",
    "docs/use-cases/**/*.md",
    "website-src/src/pages/**/*.astro",
    "website-src/src/content/docs/**/*.mdx",
)

SKIP_PARTS = {
    ".git",
    "node_modules",
    "target",
    "__pycache__",
}

HISTORICAL_CONTEXT_PATHS = (
    "docs/rfcs/",
    "docs/architecture/phased-execution-completed-ledger.md",
)

ALLOWED_CONTEXT_RE = re.compile(
    r"\b("
    r"not|no|without|blocked|gated|unsupported|fail-closed|must not|does not|"
    r"do not|cannot|claim boundary|claim-gate|claim gate|allowed=false|"
    r"claim_allowed=false|fallback_attempted=false|external_engine_invoked=false|"
    r"baseline|baselines|oracle|oracles|migration reference|migration references|"
    r"historical|rfc|ledger|policy|non-goal|non-goals"
    r")\b",
    re.IGNORECASE,
)

RISK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "external_engine_replacement",
        re.compile(
            r"\bShardLoom\b.{0,120}\b"
            r"(Spark|PySpark|DuckDB|Polars|DataFusion|pandas)\b.{0,80}\b"
            r"(replacement|displacement|alternative|successor|parity)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "drop_in_replacement",
        re.compile(r"\bdrop[- ]in\s+(replacement|alternative|parity)\b", re.IGNORECASE),
    ),
    (
        "performance_superiority",
        re.compile(
            r"\b(fastest|faster than|outperforms|beats|superior to|best default)\b"
            r".{0,120}\b(Spark|PySpark|DuckDB|Polars|DataFusion|pandas|baseline|competitor)",
            re.IGNORECASE,
        ),
    ),
    (
        "production_platform_support",
        re.compile(
            r"\bShardLoom\b.{0,120}\b("
            r"production[- ]ready|production readiness|production support|"
            r"object-store production|lakehouse production|Foundry production|"
            r"live/hybrid production"
            r")\b",
            re.IGNORECASE,
        ),
    ),
    (
        "broad_sql_dataframe_parity",
        re.compile(
            r"\bShardLoom\b.{0,120}\b"
            r"(broad|full|complete|arbitrary)\s+(SQL|DataFrame|SQL/DataFrame)\s+"
            r"(parity|support|runtime|execution)\b",
            re.IGNORECASE,
        ),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--scan-path",
        action="append",
        default=[],
        help="Optional repo-relative public file path to scan. May be repeated.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/public-claim-language-report.json"),
    )
    return parser.parse_args()


def repo_relative(path: Path, repo_root: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def skip_path(path: Path) -> bool:
    return any(part in SKIP_PARTS for part in path.parts)


def iter_scan_files(repo_root: Path, scan_paths: tuple[str, ...] | None = None) -> list[Path]:
    if scan_paths:
        return [
            (repo_root / path).resolve()
            for path in scan_paths
            if (repo_root / path).is_file() and not skip_path(repo_root / path)
        ]
    files: set[Path] = set()
    for pattern in PUBLIC_SCAN_GLOBS:
        for path in repo_root.glob(pattern):
            if path.is_file() and not skip_path(path):
                files.add(path.resolve())
    return sorted(files)


def historical_context_path(rel_path: str) -> bool:
    return any(rel_path == marker or rel_path.startswith(marker) for marker in HISTORICAL_CONTEXT_PATHS)


def line_context(lines: list[str], index: int) -> str:
    start = max(index - 1, 0)
    end = min(index + 2, len(lines))
    return " ".join(line.strip() for line in lines[start:end])


def validate_claim_rows(label: str, text: str, blockers: list[str]) -> None:
    for row in REQUIRED_RELEASE_CLAIM_ROWS:
        if row not in text:
            blockers.append(f"{label}: missing required release claim row {row}")
    for row in REQUIRED_V1_CLAIM_ROWS:
        if row not in text:
            blockers.append(f"{label}: missing required v1 claim row {row}")
    for row in OUT_OF_V1_CLAIM_ROWS:
        if row not in text:
            blockers.append(f"{label}: missing out-of-v1 claim row {row}")


def claim_language_blockers_for_file(path: Path, repo_root: Path) -> list[str]:
    rel_path = repo_relative(path, repo_root)
    if historical_context_path(rel_path):
        return []
    blockers: list[str] = []
    lines = read_text(path).splitlines()
    for index, line in enumerate(lines, start=1):
        context = line_context(lines, index - 1)
        for pattern_name, pattern in RISK_PATTERNS:
            if not pattern.search(line):
                continue
            if ALLOWED_CONTEXT_RE.search(context):
                continue
            blockers.append(
                f"{rel_path}:{index}: forbidden positive public claim wording "
                f"({pattern_name})"
            )
    return blockers


def build_report(
    repo_root: Path,
    *,
    scan_paths: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    blockers: list[str] = []

    finished_scope_text = read_text(repo_root / FINISHED_PRODUCT_SCOPE)
    public_status_text = read_text(repo_root / PUBLIC_STATUS_MATRIX)
    unsupported_text = read_text(repo_root / KNOWN_UNSUPPORTED_PATHS)
    per_claim_text = read_text(repo_root / PER_CLAIM_MATRIX)

    blockers.extend(
        require_markers(
            FINISHED_PRODUCT_SCOPE.as_posix(),
            finished_scope_text,
            FINISHED_SCOPE_MARKERS,
        )
    )
    blockers.extend(
        require_markers(
            PUBLIC_STATUS_MATRIX.as_posix(),
            public_status_text,
            PUBLIC_STATUS_MARKERS,
        )
    )
    blockers.extend(
        require_markers(
            KNOWN_UNSUPPORTED_PATHS.as_posix(),
            unsupported_text,
            KNOWN_UNSUPPORTED_MARKERS,
        )
    )
    blockers.extend(
        require_markers(PER_CLAIM_MATRIX.as_posix(), per_claim_text, PER_CLAIM_MARKERS)
    )
    validate_claim_rows(PER_CLAIM_MATRIX.as_posix(), per_claim_text, blockers)
    validate_claim_rows(FINISHED_PRODUCT_SCOPE.as_posix(), finished_scope_text, blockers)

    checked_files: list[str] = []
    for path in iter_scan_files(repo_root, scan_paths):
        checked_files.append(repo_relative(path, repo_root))
        blockers.extend(claim_language_blockers_for_file(path, repo_root))

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "claim_gate_status": "not_claim_grade",
        "finished_product_scope": FINISHED_PRODUCT_SCOPE.as_posix(),
        "per_claim_evidence_matrix": PER_CLAIM_MATRIX.as_posix(),
        "required_v1_claim_rows": list(REQUIRED_V1_CLAIM_ROWS),
        "required_release_claim_rows": list(REQUIRED_RELEASE_CLAIM_ROWS),
        "out_of_v1_claim_rows": list(OUT_OF_V1_CLAIM_ROWS),
        "checked_files": checked_files,
        "checked_file_count": len(checked_files),
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    report = build_report(repo_root, scan_paths=tuple(args.scan_path) or None)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
