#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate targeted ShardLoom prepare-batch role-repair evidence."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


EVIDENCE_SCHEMA_VERSION = "shardloom.prepare_batch_role_repair_evidence.v1"
REPORT_SCHEMA_VERSION = "shardloom.prepare_batch_role_repair_evidence_report.v1"
REQUIRED_CASES = {
    "full_prepare_register": {
        "strategy": "full_prepare_register",
        "repaired_role": "all_prepared_artifacts_created",
        "partial_repair_status": "blocked_missing_base_manifest_full_prepare_required",
    },
    "manifest_reuse": {
        "strategy": "manifest_reuse",
        "repaired_role": "none",
        "partial_repair_status": "not_needed_manifest_hit",
    },
    "fact_role_repair": {
        "strategy": "role_scoped_repair",
        "repaired_role": "fact_input",
        "partial_repair_status": "admitted_role_repair_completed",
    },
    "dim_role_repair": {
        "strategy": "role_scoped_repair",
        "repaired_role": "dim_input",
        "partial_repair_status": "admitted_role_repair_completed",
    },
    "cdc_delta_role_repair": {
        "strategy": "role_scoped_repair",
        "repaired_role": "cdc_delta_input",
        "partial_repair_status": "admitted_role_repair_completed",
    },
}
ROLE_REPAIR_STAGE_MICROS_FIELDS = (
    "prepare_batch_prepared_state_partial_repair_micros",
    "prepare_batch_prepared_state_partial_repair_source_to_columnar_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_array_build_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_write_micros",
    "prepare_batch_prepared_state_partial_repair_vortex_reopen_verify_micros",
)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def row_fields(row: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    evidence = row.get("shardloom_evidence")
    if isinstance(evidence, dict):
        fields.update(evidence)
    metrics = row.get("metrics")
    if isinstance(metrics, dict):
        fields.update(metrics)
    fields.update(row)
    return fields


def field_text(fields: dict[str, Any], key: str) -> str:
    value = fields.get(key)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def field_bool(fields: dict[str, Any], key: str) -> bool | None:
    value = fields.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def field_number(fields: dict[str, Any], key: str) -> float | None:
    value = fields.get(key)
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def csv_values(value: str) -> set[str]:
    return {part.strip() for part in value.split(",") if part.strip() and part != "none"}


def validate_no_fallback_fields(
    fields: dict[str, Any],
    blockers: list[str],
    *,
    case_id: str,
    row_index: int,
) -> None:
    required_false = (
        "fallback_attempted",
        "external_engine_invoked",
        "prepare_batch_fallback_attempted",
        "prepare_batch_external_engine_invoked",
        "prepare_batch_prepared_state_dependency_fallback_attempted",
        "prepare_batch_prepared_state_dependency_external_engine_invoked",
        "prepare_batch_prepared_state_optimization_fallback_attempted",
        "prepare_batch_prepared_state_optimization_external_engine_invoked",
        "prepare_batch_prepared_state_optimization_stale_artifact_reuse_allowed",
        "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed",
    )
    for key in required_false:
        if field_bool(fields, key) is not False:
            blockers.append(
                f"{case_id} row {row_index} must report {key}=false "
                f"(actual={field_text(fields, key) or 'missing'})"
            )
    if (
        field_text(
            fields,
            "prepare_batch_prepared_state_optimization_no_fallback_policy_status",
        )
        != "passed_fallback_false_external_engine_false"
    ):
        blockers.append(
            f"{case_id} row {row_index} must pass prepared-state optimization "
            "no-fallback policy"
        )


def validate_case_row(
    row: dict[str, Any],
    *,
    case_id: str,
    expected: dict[str, str],
    row_index: int,
) -> None:
    blockers: list[str] = []
    fields = row_fields(row)
    if row.get("engine") != "shardloom-prepare-batch":
        blockers.append(f"{case_id} row {row_index} engine must be shardloom-prepare-batch")
    if row.get("status") != "success":
        blockers.append(f"{case_id} row {row_index} status must be success")
    if row.get("storage_format") != "csv":
        blockers.append(f"{case_id} row {row_index} storage_format must be csv")
    if row.get("selected_execution_mode") != "prepared_vortex":
        blockers.append(f"{case_id} row {row_index} selected mode must be prepared_vortex")
    validate_no_fallback_fields(fields, blockers, case_id=case_id, row_index=row_index)

    strategy = field_text(
        fields,
        "prepare_batch_prepared_state_optimization_strategy",
    )
    if strategy != expected["strategy"]:
        blockers.append(
            f"{case_id} row {row_index} strategy mismatch: "
            f"expected {expected['strategy']}, got {strategy or 'missing'}"
        )

    partial_status = field_text(
        fields,
        "prepare_batch_prepared_state_partial_repair_status",
    )
    if partial_status != expected["partial_repair_status"]:
        blockers.append(
            f"{case_id} row {row_index} partial repair status mismatch: "
            f"expected {expected['partial_repair_status']}, got {partial_status or 'missing'}"
        )

    repaired_role = expected["repaired_role"]
    repaired_roles = csv_values(
        field_text(fields, "prepare_batch_prepared_state_optimization_repaired_roles")
    ) | csv_values(
        field_text(fields, "prepare_batch_prepared_state_partial_repair_repaired_roles")
    )
    if repaired_role not in {"all_prepared_artifacts_created", "none"}:
        if repaired_role not in repaired_roles:
            blockers.append(
                f"{case_id} row {row_index} must report repaired role {repaired_role}"
            )
        if field_bool(
            fields,
            "prepare_batch_prepared_state_partial_repair_regeneration_performed",
        ) is not True:
            blockers.append(
                f"{case_id} row {row_index} admitted repair must report regeneration"
            )
        reused_roles = csv_values(
            field_text(fields, "prepare_batch_prepared_state_partial_repair_reused_roles")
        )
        if not reused_roles:
            blockers.append(f"{case_id} row {row_index} must report reused roles")
        for key in ROLE_REPAIR_STAGE_MICROS_FIELDS:
            value = field_number(fields, key)
            if value is None or value < 0:
                blockers.append(
                    f"{case_id} row {row_index} must report numeric {key}"
                )
        proof = field_text(
            fields,
            "prepare_batch_prepared_state_partial_repair_replay_proof",
        )
        if not proof or proof in {"not_reported", "not_executed", "none"}:
            blockers.append(f"{case_id} row {row_index} must report repair replay proof")
    elif repaired_role == "none":
        if field_bool(
            fields,
            "prepare_batch_prepared_state_partial_repair_regeneration_performed",
        ) is not False:
            blockers.append(
                f"{case_id} row {row_index} manifest reuse must not regenerate repair"
            )

    if blockers:
        raise ValueError("; ".join(blockers))


def validate_artifact_payload(payload: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    if payload.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        blockers.append("prepare-batch role-repair evidence schema_version mismatch")
    if payload.get("fallback_attempted") is not False:
        blockers.append("prepare-batch role-repair evidence fallback_attempted must be false")
    if payload.get("external_engine_invoked") is not False:
        blockers.append(
            "prepare-batch role-repair evidence external_engine_invoked must be false"
        )
    runs = payload.get("runs")
    if not isinstance(runs, list):
        blockers.append("prepare-batch role-repair evidence runs must be a list")
        runs = []
    by_case: dict[str, dict[str, Any]] = {}
    for run in runs:
        if isinstance(run, dict):
            case_id = str(run.get("case_id") or "")
            if case_id:
                by_case[case_id] = run
    missing_cases = sorted(set(REQUIRED_CASES) - set(by_case))
    if missing_cases:
        blockers.append(
            "prepare-batch role-repair evidence missing cases: "
            + ", ".join(missing_cases)
        )
    row_count = 0
    strategy_counts: Counter[str] = Counter()
    repaired_roles: set[str] = set()
    for case_id, expected in REQUIRED_CASES.items():
        run = by_case.get(case_id)
        if not run:
            continue
        rows = run.get("rows")
        if not isinstance(rows, list) or not rows:
            blockers.append(f"{case_id} must contain one or more rows")
            continue
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                blockers.append(f"{case_id} row {row_index} must be an object")
                continue
            try:
                validate_case_row(
                    row,
                    case_id=case_id,
                    expected=expected,
                    row_index=row_index,
                )
            except ValueError as exc:
                blockers.append(str(exc))
            row_count += 1
            fields = row_fields(row)
            strategy = field_text(
                fields,
                "prepare_batch_prepared_state_optimization_strategy",
            )
            strategy_counts[strategy or "missing"] += 1
            if strategy == "role_scoped_repair":
                repaired_roles.update(
                    csv_values(
                        field_text(
                            fields,
                            "prepare_batch_prepared_state_partial_repair_repaired_roles",
                        )
                    )
                )
    summary = {
        "case_count": len(by_case),
        "row_count": row_count,
        "strategy_counts": dict(sorted(strategy_counts.items())),
        "repaired_roles": sorted(repaired_roles),
    }
    return blockers, summary


def build_report(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "status": "blocked",
            "artifact": str(path),
            "blockers": ["prepare-batch role-repair evidence artifact must be an object"],
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
    blockers, summary = validate_artifact_payload(payload)
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "artifact": str(path),
        "artifact_schema_version": payload.get("schema_version"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "blockers": blockers,
        "fallback_attempted": payload.get("fallback_attempted") is True,
        "external_engine_invoked": payload.get("external_engine_invoked") is True,
        "performance_claim_allowed": False,
        "claim_boundary": payload.get("claim_boundary"),
        **summary,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact",
        type=Path,
        default=Path("website/assets/benchmarks/latest/prepare-batch-role-repair-evidence.json"),
    )
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args.artifact)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
