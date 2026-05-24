#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom runtime-envelope evidence contracts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python" / "src"))

from shardloom import OutputEnvelope  # noqa: E402

SCHEMA_VERSION = "shardloom.runtime_execution_envelope_validation_report.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/runtime-execution-envelope-validation-report.json"),
    )
    return parser.parse_args()


def envelope(fields: list[dict[str, str]], *, command: str = "sql-local-source-smoke") -> OutputEnvelope:
    return OutputEnvelope.from_json(
        {
            "schema_version": "shardloom.output.v2",
            "command": command,
            "status": "success",
            "summary": "runtime envelope validation fixture",
            "human_text": "runtime envelope validation fixture",
            "fallback": {
                "attempted": False,
                "allowed": False,
                "engine": None,
                "reason": "disabled",
            },
            "diagnostics": [],
            "result": {"fields": fields},
            "result_refs": [],
            "artifacts": [],
            "artifact_refs": [],
            "certificates": [],
            "policy": {
                "fields": [
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"},
                    {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                ]
            },
            "lifecycle": {"fields": []},
            "capability_snapshot": {"fields": []},
            "fields": [],
        }
    )


def fixture_rows() -> list[dict[str, Any]]:
    complete = envelope(
        [
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_digest", "value": "fnv64:source"},
            {"key": "source_state_materialization_layout", "value": "scalar_row_map"},
            {"key": "execution_certificate_ref", "value": "sql-local-source.execution.v1"},
        ]
    ).runtime_execution_validation(surface_id="complete_sql_local_source")

    missing_certificate = envelope(
        [
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_materialization_layout", "value": "scalar_row_map"},
        ]
    ).runtime_execution_validation(surface_id="missing_execution_certificate")

    prepared_missing_state = envelope(
        [
            {"key": "execution_mode", "value": "prepared_vortex"},
            {"key": "source_state_materialization_layout", "value": "encoded_vortex"},
            {"key": "execution_certificate_ref", "value": "prepared-vortex.execution.v1"},
        ],
        command="traditional-analytics-vortex-batch-run",
    ).runtime_execution_validation(surface_id="prepared_vortex_missing_state")

    certified_timing_drift = envelope(
        [
            {"key": "execution_mode", "value": "compatibility_import_certified"},
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_materialization_layout", "value": "columnar_source_state"},
            {"key": "execution_certificate_ref", "value": "compat-certified.execution.v1"},
            {"key": "timing_scope", "value": "warm_query_only"},
            {"key": "preparation_included", "value": "false"},
        ],
        command="traditional-analytics-run",
    ).runtime_execution_validation(surface_id="compatibility_import_certified_timing_drift")

    invalid_no_fallback_flags = OutputEnvelope.from_json(
        {
            "schema_version": "shardloom.output.v2",
            "command": "sql-local-source-smoke",
            "status": "success",
            "summary": "runtime envelope validation fixture",
            "human_text": "runtime envelope validation fixture",
            "fallback": {
                "attempted": False,
                "allowed": False,
                "engine": None,
                "reason": "disabled",
            },
            "diagnostics": [],
            "result": {
                "fields": [
                    {"key": "source_state_id", "value": "source-state-fixture"},
                    {
                        "key": "source_state_materialization_layout",
                        "value": "scalar_row_map",
                    },
                    {
                        "key": "execution_certificate_ref",
                        "value": "sql-local-source.execution.v1",
                    },
                ]
            },
            "result_refs": [],
            "artifacts": [],
            "artifact_refs": [],
            "certificates": [],
            "policy": {
                "fields": [
                    {"key": "fallback_attempted", "value": "maybe"},
                    {"key": "external_engine_invoked", "value": "false"},
                    {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                ]
            },
            "lifecycle": {"fields": []},
            "capability_snapshot": {"fields": []},
            "fields": [],
        }
    ).runtime_execution_validation(surface_id="invalid_no_fallback_flags")

    return [
        complete.as_dict(),
        missing_certificate.as_dict(),
        prepared_missing_state.as_dict(),
        certified_timing_drift.as_dict(),
        invalid_no_fallback_flags.as_dict(),
    ]


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    rows = fixture_rows()
    expected = {
        "complete_sql_local_source": "passed",
        "missing_execution_certificate": "blocked",
        "prepared_vortex_missing_state": "blocked",
        "compatibility_import_certified_timing_drift": "blocked",
        "invalid_no_fallback_flags": "blocked",
    }
    blockers = [
        f"{row['surface_id']} status={row['status']} expected={expected[row['surface_id']]}"
        for row in rows
        if row["status"] != expected[row["surface_id"]]
    ]
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "validator_schema_version": "shardloom.runtime_execution_envelope_validation.v1",
        "row_count": len(rows),
        "passed_row_count": sum(1 for row in rows if row["status"] == "passed"),
        "blocked_row_count": sum(1 for row in rows if row["status"] == "blocked"),
        "rows": rows,
        "blockers": blockers,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
