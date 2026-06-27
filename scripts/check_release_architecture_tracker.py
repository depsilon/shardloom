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
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_runtime_gap_family_burn_down import (
    SCHEMA_VERSION as RUNTIME_GAP_FAMILY_BURN_DOWN_SCHEMA_VERSION,
)
from check_runtime_gap_family_burn_down import (
    build_report as build_runtime_gap_family_burn_down_report,
)


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


def read_required_text(
    path: Path,
    label: str,
    blockers: list[str],
    missing_required_inputs: list[str],
) -> str:
    if not path.exists():
        blockers.append(f"missing required architecture tracker input: {label} ({path.as_posix()})")
        missing_required_inputs.append(str(path).replace("\\", "/"))
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


def runtime_gap_family_burn_down_blockers(
    report: dict[str, Any],
    *,
    expected_global_unchecked_count: int,
) -> list[str]:
    blockers: list[str] = []
    if report.get("schema_version") != RUNTIME_GAP_FAMILY_BURN_DOWN_SCHEMA_VERSION:
        blockers.append(
            "runtime gap family burn-down schema_version="
            + str(report.get("schema_version", "missing"))
        )
    if report.get("status") != "passed":
        blockers.extend(
            "runtime gap family burn-down: " + str(blocker)
            for blocker in report.get("blockers", ["status is not passed"])
        )
    if report.get("global_review_unchecked_count") != expected_global_unchecked_count:
        blockers.append(
            "runtime gap family burn-down global_review_unchecked_count mismatch: "
            + f"{report.get('global_review_unchecked_count', 'missing')} "
            + f"!= {expected_global_unchecked_count}"
        )
    if report.get("mapped_gap_count") != expected_global_unchecked_count:
        blockers.append(
            "runtime gap family burn-down mapped_gap_count mismatch: "
            + f"{report.get('mapped_gap_count', 'missing')} "
            + f"!= {expected_global_unchecked_count}"
        )
    acceptance = report.get("acceptance_summary")
    if not isinstance(acceptance, dict):
        blockers.append("runtime gap family burn-down missing acceptance_summary")
    else:
        for field in (
            "all_unchecked_global_review_rows_mapped",
            "all_families_have_phase_items",
            "all_families_have_active_phase_owner",
            "all_families_have_evidence_and_validators",
            "all_no_fallback_invariants_named",
            "all_claim_boundaries_named",
        ):
            if acceptance.get(field) is not True:
                blockers.append(f"runtime gap family burn-down {field} must be true")
    for field in (
        "fallback_attempted",
        "external_engine_invoked",
        "runtime_support_claim_allowed",
        "performance_claim_allowed",
        "production_claim_allowed",
    ):
        if report.get(field) is not False:
            blockers.append(f"runtime gap family burn-down {field} must be false")
    if report.get("claim_gate_status") != "not_claim_grade":
        blockers.append(
            "runtime gap family burn-down claim_gate_status="
            + str(report.get("claim_gate_status", "missing"))
        )
    return blockers


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
    missing_required_inputs: list[str] = []
    global_review = read_required_text(
        repo_root / global_review_ref,
        "global architecture review",
        blockers,
        missing_required_inputs,
    )
    phase_plan = read_required_text(
        repo_root / phase_plan_ref,
        "phased execution plan",
        blockers,
        missing_required_inputs,
    )
    traceability = read_required_text(
        repo_root / traceability_ref,
        "RFC phase traceability",
        blockers,
        missing_required_inputs,
    )
    unsupported = read_required_text(
        repo_root / unsupported_ref,
        "known unsupported paths",
        blockers,
        missing_required_inputs,
    )
    security = read_required_text(
        repo_root / security_ref,
        "release security gate",
        blockers,
        missing_required_inputs,
    )
    provenance = read_required_text(
        repo_root / provenance_ref,
        "release provenance dry run",
        blockers,
        missing_required_inputs,
    )
    per_claim_matrix = read_required_text(
        repo_root / per_claim_matrix_ref,
        "per-claim evidence matrix",
        blockers,
        missing_required_inputs,
    )
    completed = read_required_text(
        repo_root / completed_ref,
        "completed ledger",
        blockers,
        missing_required_inputs,
    )

    global_unchecked = unchecked_items(global_review)
    phase_unchecked = unchecked_items(phase_plan)
    runtime_gap_family_burn_down = build_runtime_gap_family_burn_down_report(repo_root)
    runtime_gap_family_blockers = runtime_gap_family_burn_down_blockers(
        runtime_gap_family_burn_down,
        expected_global_unchecked_count=len(global_unchecked),
    )
    global_gar_ids = gar_ids(global_unchecked)
    phase_gar_ids = gar_ids(phase_unchecked)
    mirrored_phase_gar_ids = gar_ids_in_text(phase_plan)
    completed_gar_ids = gar_ids_in_text(completed)
    known_mirrored_ids = mirrored_phase_gar_ids | completed_gar_ids
    mirrored_missing = sorted(global_gar_ids - mirrored_gar_ids(global_gar_ids, known_mirrored_ids))

    global_review_mapping_status = "no_unchecked_global_review_rows"
    if global_unchecked and runtime_gap_family_blockers:
        blockers.append(f"global architecture review has unchecked items: {len(global_unchecked)}")
        blockers.extend(runtime_gap_family_blockers)
        global_review_mapping_status = "blocked_unmapped_or_invalid"
    elif global_unchecked:
        global_review_mapping_status = "mapped_to_runtime_gap_family_claim_boundaries"
    phase_plan_queue_status = (
        "open_development_queue_blocks_release_claims"
        if phase_unchecked
        else "no_unchecked_phase_plan_rows"
    )
    if phase_unchecked:
        blockers.append(f"phase plan has unchecked items: {len(phase_unchecked)}")
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
    claim_grade = passed and not global_unchecked and not phase_unchecked
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "claim_gate_status": "claim_grade" if claim_grade else "not_claim_grade",
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
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
        "global_review_mapping_status": global_review_mapping_status,
        "global_review_unchecked_rows_block_release": bool(runtime_gap_family_blockers),
        "phase_plan_queue_status": phase_plan_queue_status,
        "phase_plan_unchecked_rows_block_release_claims": bool(phase_unchecked),
        "runtime_gap_family_burn_down_schema_version": runtime_gap_family_burn_down.get(
            "schema_version"
        ),
        "runtime_gap_family_burn_down_status": runtime_gap_family_burn_down.get("status"),
        "runtime_gap_family_burn_down_blocker_count": len(runtime_gap_family_blockers),
        "runtime_gap_family_burn_down_blockers": runtime_gap_family_blockers,
        "runtime_gap_family_burn_down_mapped_gap_count": runtime_gap_family_burn_down.get(
            "mapped_gap_count"
        ),
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
        "missing_required_input_count": len(missing_required_inputs),
        "missing_required_inputs": missing_required_inputs,
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
    if missing_required_inputs:
        return 1
    return 0 if passed or args.allow_blocked else 1


if __name__ == "__main__":
    raise SystemExit(main())
