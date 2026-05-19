#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate architecture tracker state for the hard release-readiness gate.

This report is intentionally fail-closed. Unchecked global architecture review or phased-plan rows
block public release/package claims until their evidence is attached or the row is explicitly moved
to the completed ledger.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_architecture_tracker_report.v1"
CHECKBOX_RE = re.compile(r"^\s*-\s+\[\s\]\s+(?P<text>.+?)\s*$")
GAR_ID_RE = re.compile(r"\bGAR-[A-Z0-9]+(?:-[A-Z0-9]+)*\b")
RFC_RE = re.compile(r"\bRFC\s+00[0-4][0-9]\b")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/release-architecture-tracker-report.json"),
    )
    parser.add_argument("--allow-blocked", action="store_true")
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def read_required_text(path: Path, label: str, blockers: list[str]) -> str:
    if not path.exists():
        blockers.append(f"missing required architecture tracker input: {label} ({path.as_posix()})")
        return ""
    return path.read_text(encoding="utf-8")


def unchecked_items(text: str) -> list[str]:
    return [
        match.group("text")
        for line in text.splitlines()
        if (match := CHECKBOX_RE.match(line))
    ]


def gar_ids(items: list[str]) -> set[str]:
    return {match.group(0) for item in items for match in GAR_ID_RE.finditer(item)}


def gar_ids_in_text(text: str) -> set[str]:
    return {match.group(0) for match in GAR_ID_RE.finditer(text)}


def mirrored_gar_ids(source_ids: set[str], known_ids: set[str]) -> set[str]:
    mirrored = set()
    for source_id in source_ids:
        if source_id in known_ids or any(item.startswith(source_id) for item in known_ids):
            mirrored.add(source_id)
    return mirrored


def rfc_mentions(text: str) -> list[str]:
    return sorted({" ".join(match.group(0).split()) for match in RFC_RE.finditer(text)})


def check_contains(text: str, required: list[str]) -> list[str]:
    return [item for item in required if item not in text]


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)

    global_review_ref = Path("docs/architecture/global-architecture-review.md")
    phase_plan_ref = Path("docs/architecture/phased-execution-plan.md")
    traceability_ref = Path("docs/architecture/rfc-phase-traceability.md")
    unsupported_ref = Path("docs/release/known-unsupported-paths.md")
    security_ref = Path("docs/security/release-security-gate.md")
    provenance_ref = Path("docs/release/release-provenance-dry-run.md")
    per_claim_matrix_ref = Path("docs/release/per-claim-evidence-attachment-matrix.md")
    completed_ref = Path("docs/architecture/phased-execution-completed-ledger.md")

    blockers: list[str] = []
    global_review = read_required_text(repo_root / global_review_ref, "global architecture review", blockers)
    phase_plan = read_required_text(repo_root / phase_plan_ref, "phased execution plan", blockers)
    traceability = read_required_text(repo_root / traceability_ref, "RFC phase traceability", blockers)
    unsupported = read_required_text(repo_root / unsupported_ref, "known unsupported paths", blockers)
    security = read_required_text(repo_root / security_ref, "release security gate", blockers)
    provenance = read_required_text(repo_root / provenance_ref, "release provenance dry run", blockers)
    per_claim_matrix = read_required_text(repo_root / per_claim_matrix_ref, "per-claim evidence matrix", blockers)
    completed = read_required_text(repo_root / completed_ref, "completed ledger", blockers)

    global_unchecked = unchecked_items(global_review)
    phase_unchecked = unchecked_items(phase_plan)
    global_gar_ids = gar_ids(global_unchecked)
    phase_gar_ids = gar_ids(phase_unchecked)
    mirrored_phase_gar_ids = gar_ids_in_text(phase_plan)
    completed_gar_ids = gar_ids_in_text(completed)
    known_mirrored_ids = mirrored_phase_gar_ids | completed_gar_ids
    mirrored_missing = sorted(global_gar_ids - mirrored_gar_ids(global_gar_ids, known_mirrored_ids))

    if global_unchecked:
        blockers.append(f"global architecture review has unchecked items: {len(global_unchecked)}")
    if phase_unchecked:
        blockers.append(f"phased execution plan has unchecked items: {len(phase_unchecked)}")
    if mirrored_missing:
        blockers.append("unchecked GAR ids missing from phase plan or completed ledger: " + ",".join(mirrored_missing))

    traceability_missing = check_contains(
        traceability,
        [
            "RFC 0043",
            "P8.0",
            "P8.4",
            "GAR-0043",
            "No package publication",
            "fallback execution",
        ],
    )
    if traceability_missing:
        blockers.append("missing traceability markers: " + ",".join(traceability_missing))

    unsupported_missing = check_contains(
        unsupported,
        [
            "broad SQL/DataFrame execution",
            "object-store runtime",
            "Foundry proof-of-use",
            "fallback_attempted=false",
            "external_engine_invoked=false",
        ],
    )
    if unsupported_missing:
        blockers.append("missing known-unsupported markers: " + ",".join(unsupported_missing))

    security_missing = check_contains(
        security,
        [
            "target/release-security-gate-report.json",
            "public release claims cannot pass",
            "`fallback_attempted=true`",
            "`external_engine_invoked=true`",
        ],
    )
    if security_missing:
        blockers.append("missing release-security markers: " + ",".join(security_missing))

    provenance_missing = check_contains(
        provenance,
        [
            "SupplyChainReleaseEvidence",
            "target/release-provenance-dry-run/manifest.json",
            "checksums.sha256",
            "publication_attempted=false",
            "tag_created=false",
        ],
    )
    if provenance_missing:
        blockers.append("missing provenance markers: " + ",".join(provenance_missing))

    per_claim_matrix_missing = check_contains(
        per_claim_matrix,
        [
            "shardloom.per_claim_evidence_attachment_matrix.v1",
            "per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade",
            "per_claim_evidence_attachment_matrix_public_release_claim_allowed=false",
            "per_claim_evidence_attachment_matrix_public_package_claim_allowed=false",
            "per_claim_evidence_attachment_matrix_fallback_attempted=false",
            "per_claim_evidence_attachment_matrix_external_engine_invoked=false",
            "required_no_fallback_evidence",
            "required_release_approval",
        ],
    )
    if per_claim_matrix_missing:
        blockers.append("missing per-claim matrix markers: " + ",".join(per_claim_matrix_missing))

    traceability_rfc_mentions = rfc_mentions(traceability)
    passed = not blockers
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "claim_gate_status": "claim_grade" if passed else "not_claim_grade",
        "public_release_claim_allowed": passed,
        "public_package_claim_allowed": passed,
        "architecture_tracker_status": "passed" if passed else "blocked",
        "global_architecture_review_ref": str(global_review_ref).replace("\\", "/"),
        "phased_execution_plan_ref": str(phase_plan_ref).replace("\\", "/"),
        "rfc_phase_traceability_ref": str(traceability_ref).replace("\\", "/"),
        "known_unsupported_paths_ref": str(unsupported_ref).replace("\\", "/"),
        "release_security_gate_ref": str(security_ref).replace("\\", "/"),
        "release_provenance_ref": str(provenance_ref).replace("\\", "/"),
        "per_claim_evidence_matrix_ref": str(per_claim_matrix_ref).replace("\\", "/"),
        "unchecked_global_architecture_review_count": len(global_unchecked),
        "unchecked_phase_plan_count": len(phase_unchecked),
        "unchecked_global_architecture_review_items": global_unchecked,
        "unchecked_phase_plan_items": phase_unchecked,
        "unchecked_global_gar_ids": sorted(global_gar_ids),
        "unchecked_phase_gar_ids": sorted(phase_gar_ids),
        "mirrored_phase_gar_ids": sorted(mirrored_phase_gar_ids),
        "completed_ledger_gar_ids": sorted(completed_gar_ids),
        "unchecked_global_gar_ids_missing_from_phase_or_ledger": mirrored_missing,
        "traceability_rfc_mentions": traceability_rfc_mentions,
        "traceability_matrix_present": not traceability_missing,
        "known_unsupported_paths_present": not unsupported_missing,
        "release_security_refs_present": not security_missing,
        "release_provenance_refs_present": not provenance_missing,
        "per_claim_evidence_matrix_present": not per_claim_matrix_missing,
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if passed or args.allow_blocked else 1


if __name__ == "__main__":
    raise SystemExit(main())
