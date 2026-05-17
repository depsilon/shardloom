#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate Use Case Atlas capability-family coverage."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from check_use_case_index import INDEX_PATH, REPO_ROOT, load_index, validate_index


EXPECTED_CAPABILITY_FAMILIES = {
    "onboarding_first_10_minutes",
    "local_file_etl",
    "compatibility_import_certified",
    "prepared_native_vortex",
    "python_wrapper_client",
    "sql_dataframe_report_only",
    "source_free_generated_output",
    "messy_data_dirty_json_cdc",
    "query_scenario_cookbook",
    "output_and_fanout",
    "object_store_boundaries",
    "table_lakehouse_boundaries",
    "foundry_dev_stack_local_proof",
    "evidence_audit_claim_gates",
    "benchmark_interpretation",
    "package_release_install_channels",
}
EXPECTED_EXECUTION_MODES = {
    "no_dataset_smoke",
    "compatibility_import_certified",
    "prepared_vortex",
    "native_vortex",
    "report_only",
    "planned_generated_source",
    "report_only_blocked",
    "mixed_by_row",
    "local_release_dry_run",
}
EXPECTED_ENGINE_MODES = {"batch_status", "batch", "none"}
BLOCKED_OR_REPORT_ONLY_FAMILIES = {
    "sql_dataframe_report_only",
    "source_free_generated_output",
    "object_store_boundaries",
    "table_lakehouse_boundaries",
    "package_release_install_channels",
}
EXPECTED_EVIDENCE_CONCEPTS = {
    "fallback_attempted=false": "no-fallback evidence",
    "external_engine_invoked=false": "external engine evidence",
    "native_io_certificate_status": "Native I/O certificate",
    "materialization_boundary": "materialization boundary",
    "claim_gate_status": "claim gate",
    "result_replay_verified": "result-sink replay",
    "generated_source_certificate_status": "generated-source certificate",
    "source_state_reuse_hit": "source-state reuse",
    "source_read_millis": "benchmark timing",
}
SUPPORTED_STATUSES = {"ready_local", "smoke_supported"}


def values(use_case: dict[str, object], field: str) -> list[str]:
    value = use_case.get(field)
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]


def mode_tokens(value: object) -> set[str]:
    text = str(value)
    return {token for token in text.replace("|", "/").replace(",", "/").split("/") if token}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--index", type=Path, default=INDEX_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    index_path = args.index if args.index.is_absolute() else repo_root / args.index
    data = load_index(index_path)
    blockers = validate_index(data, repo_root)

    declared = {
        family["id"]
        for family in data.get("capability_families", [])
        if isinstance(family, dict) and isinstance(family.get("id"), str)
    }
    if declared != EXPECTED_CAPABILITY_FAMILIES:
        blockers.append(
            "capability_families must match expected atlas families; "
            f"missing={sorted(EXPECTED_CAPABILITY_FAMILIES - declared)} "
            f"extra={sorted(declared - EXPECTED_CAPABILITY_FAMILIES)}"
        )

    covered = {
        use_case["capability_family"]
        for use_case in data.get("use_cases", [])
        if isinstance(use_case, dict) and isinstance(use_case.get("capability_family"), str)
    }
    missing_coverage = EXPECTED_CAPABILITY_FAMILIES - covered
    if missing_coverage:
        blockers.append(f"capability families without a use case: {sorted(missing_coverage)}")

    use_cases = [use_case for use_case in data.get("use_cases", []) if isinstance(use_case, dict)]
    execution_modes = {token for use_case in use_cases for token in mode_tokens(use_case.get("execution_mode"))}
    missing_execution_modes = EXPECTED_EXECUTION_MODES - execution_modes
    if missing_execution_modes:
        blockers.append(f"execution modes without a use case: {sorted(missing_execution_modes)}")

    engine_modes = {str(use_case.get("engine_mode")) for use_case in use_cases}
    missing_engine_modes = EXPECTED_ENGINE_MODES - engine_modes
    if missing_engine_modes:
        blockers.append(f"engine modes without a use case: {sorted(missing_engine_modes)}")

    supported_inputs = {
        value
        for use_case in use_cases
        if use_case.get("status") in SUPPORTED_STATUSES
        for value in values(use_case, "inputs")
    }
    supported_outputs = {
        value
        for use_case in use_cases
        if use_case.get("status") in SUPPORTED_STATUSES
        for value in values(use_case, "outputs")
    }
    if "none" not in supported_inputs or "local_csv" not in supported_inputs or "local_parquet" not in supported_inputs:
        blockers.append("supported input families must include none, local_csv, and local_parquet")
    if "status_report" not in supported_outputs or "local_result_sink_evidence" not in supported_outputs:
        blockers.append("supported output families must include status_report and local_result_sink_evidence")

    families_with_blockers = {
        str(use_case.get("capability_family"))
        for use_case in use_cases
        if use_case.get("status") in {"report_only", "planned", "blocked", "unsupported"}
        and use_case.get("blocked_explanation")
    }
    missing_blocked_major = BLOCKED_OR_REPORT_ONLY_FAMILIES - families_with_blockers
    if missing_blocked_major:
        blockers.append(f"blocked/report-only major families without blocker docs: {sorted(missing_blocked_major)}")

    evidence_fields = {
        field
        for use_case in use_cases
        for field in values(use_case, "evidence_fields")
    }
    for expected_field, concept in EXPECTED_EVIDENCE_CONCEPTS.items():
        if not any(expected_field in field for field in evidence_fields):
            blockers.append(f"evidence concept missing from use-case index: {concept} ({expected_field})")

    referenced_files = {
        reference
        for use_case in use_cases
        for reference in values(use_case, "references")
    }
    for readme in sorted((repo_root / "examples").glob("*/README.md")):
        relative = readme.relative_to(repo_root).as_posix()
        if relative not in referenced_files:
            blockers.append(f"example directory is not linked from any use case: {relative}")

    family_counts = {family_id: 0 for family_id in EXPECTED_CAPABILITY_FAMILIES}
    for use_case in data.get("use_cases", []):
        if isinstance(use_case, dict):
            family_id = use_case.get("capability_family")
            if family_id in family_counts:
                family_counts[family_id] += 1

    if blockers:
        print("use-case coverage validation failed:", file=sys.stderr)
        for blocker in blockers:
            print(f"- {blocker}", file=sys.stderr)
        return 1

    print("use-case coverage ok:")
    for family_id in sorted(family_counts):
        print(f"- {family_id}: {family_counts[family_id]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
