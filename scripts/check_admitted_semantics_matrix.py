#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate admitted SQL expression semantics against decoded references.

The validator executes only local ShardLoom CLI paths. It does not invoke external engines,
publish packages, probe networks, or authorize production/ANSI/performance claims.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.admitted_semantics_matrix_report.v1"
MATRIX_SCHEMA_VERSION = "shardloom.admitted_semantics_fixture_matrix.v1"
DEFAULT_FEATURES = "vortex-write,vortex-local-primitives"
PROPERTY_SEED = 20260521

REQUIRED_ROW_FIELDS = (
    "id",
    "operator_family",
    "support_state",
    "runtime_validation",
    "source_format",
    "input_dtype",
    "output_dtype",
    "null_policy",
    "coercion_policy",
    "invalid_input_behavior",
    "unsupported_diagnostic_code",
    "unsupported_diagnostic_message",
    "decoded_reference_kind",
    "oracle_boundary",
    "property_seed",
    "claim_boundary",
    "fallback_attempted",
    "external_engine_invoked",
)

FALSE_REPORT_FIELDS = (
    "production_claim_allowed",
    "ansi_sql_claim_allowed",
    "performance_claim_allowed",
    "public_release_claim_allowed",
    "public_package_claim_allowed",
    "package_publication_performed",
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "fallback_attempted",
    "external_engine_invoked",
)


@dataclass(frozen=True)
class SqlFixtureCase:
    case_id: str
    source_name: str
    source_text: str
    statement_template: str
    expected_jsonl: str
    expected_fields: dict[str, str]
    property_seed: int | None = None


@dataclass(frozen=True)
class UnsupportedCase:
    case_id: str
    source_name: str
    source_text: str
    statement_template: str
    diagnostic_code: str
    diagnostic_fragment: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--matrix",
        type=Path,
        default=Path("docs/status/admitted-semantics-matrix.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/admitted-semantics-matrix-report.json"),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("target/admitted-semantics-matrix"),
    )
    parser.add_argument("--features", default=DEFAULT_FEATURES)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def command_text(command: list[str]) -> str:
    return " ".join(command).replace(str(sys.executable), "python")


def tail(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[-limit:]


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def jsonl(rows: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)


def digest_text(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def bool_field(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
    return None


def collect_field_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    direct = payload.get("fields")
    if isinstance(direct, list):
        rows.extend(row for row in direct if isinstance(row, dict))
    for key in ("result", "policy", "lifecycle", "capability_snapshot"):
        child = payload.get(key)
        if isinstance(child, dict):
            rows.extend(collect_field_rows(child))
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, list):
        for artifact in artifacts:
            if isinstance(artifact, dict):
                rows.extend(collect_field_rows(artifact.get("payload")))
    return rows


def field_map(payload: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for row in collect_field_rows(payload):
        key = row.get("key")
        value = row.get("value")
        if isinstance(key, str):
            fields[key] = "" if value is None else str(value)
    return fields


def no_fallback_blockers(payload: dict[str, Any], label: str) -> list[str]:
    blockers: list[str] = []
    fallback = payload.get("fallback")
    if isinstance(fallback, dict):
        if fallback.get("attempted") is not False:
            blockers.append(f"{label}: envelope fallback.attempted must be false")
        if fallback.get("allowed") is not False:
            blockers.append(f"{label}: envelope fallback.allowed must be false")
    for key, value in field_map(payload).items():
        lowered = key.lower()
        bool_value = bool_field(value)
        if "fallback_attempted" in lowered and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
        if "external_engine_invoked" in lowered and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
        if "external_query_engine_invoked" in lowered and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
        if lowered.endswith("fallback_execution_allowed") and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
    return blockers


def run_subprocess(*, repo_root: Path, command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=repo_root, text=True, capture_output=True, check=False)


def locate_binary(repo_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return resolve(repo_root, explicit).resolve()
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repo_root / "target"))
    if not target_root.is_absolute():
        target_root = repo_root / target_root
    suffix = ".exe" if os.name == "nt" else ""
    return (target_root / "debug" / f"shardloom{suffix}").resolve()


def build_binary(repo_root: Path, features: str, skip_build: bool, binary: Path) -> dict[str, Any]:
    if skip_build:
        blockers = [] if binary.exists() else [f"binary does not exist: {binary}"]
        return {"command": "skipped", "status": "passed" if not blockers else "failed", "blockers": blockers}
    command = [
        "cargo",
        "build",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        features,
    ]
    completed = run_subprocess(repo_root=repo_root, command=command)
    blockers = []
    if completed.returncode != 0:
        blockers.append("feature-gated CLI build failed")
    if not binary.exists():
        blockers.append(f"built binary missing: {binary}")
    return {
        "command": command_text(command),
        "argv": command,
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "stdout_tail": tail(completed.stdout),
        "stderr_tail": tail(completed.stderr),
        "features": features,
        "blockers": blockers,
    }


def property_numeric_case() -> SqlFixtureCase:
    rng = random.Random(PROPERTY_SEED)
    csv_rows = ["id,amount,tax"]
    expected: list[dict[str, Any]] = []
    for row_id in range(1, 25):
        amount: int | None = rng.randint(-6, 32)
        if row_id % 7 == 0:
            amount = None
        tax: int | None = rng.randint(-3, 8)
        if row_id % 5 == 0:
            tax = None
        csv_rows.append(
            f"{row_id},{'' if amount is None else amount},{'' if tax is None else tax}"
        )
        if amount is not None and amount >= 10:
            if tax is None:
                expected.append({"id": row_id, "gross": None, "spread": None})
            else:
                expected.append(
                    {"id": row_id, "gross": (amount + tax) * 2, "spread": abs(amount - tax)}
                )
    return SqlFixtureCase(
        case_id="numeric_generic_property_seed_20260521",
        source_name="numeric-property.csv",
        source_text="\n".join(csv_rows) + "\n",
        statement_template=(
            "SELECT id,(amount + tax) * 2 AS gross,ABS(amount - tax) AS spread "
            "FROM '{source}' WHERE amount >= 10 LIMIT 100"
        ),
        expected_jsonl=jsonl(expected),
        expected_fields={
            "sql_statement_kind": "local_source_computed_projection_filter_limit",
            "generic_expression_projection_runtime_execution": "true",
            "generic_expression_projection_output_column": "gross,spread",
            "projected_columns": "id,gross,spread",
            "claim_gate_status": "fixture_smoke_only",
            "production_claim_allowed": "false",
            "performance_claim_allowed": "false",
        },
        property_seed=PROPERTY_SEED,
    )


def executable_cases() -> list[SqlFixtureCase]:
    return [
        property_numeric_case(),
        SqlFixtureCase(
            case_id="try_cast_projection_null_on_invalid",
            source_name="try-cast.csv",
            source_text="id,raw_amount\n1,8\n2,not_an_int\n3,15\n",
            statement_template=(
                "SELECT id,TRY_CAST(raw_amount AS int64) AS amount_i64 "
                "FROM '{source}' WHERE id >= 1 LIMIT 10"
            ),
            expected_jsonl=(
                '{"id":1,"amount_i64":8}\n'
                '{"id":2,"amount_i64":null}\n'
                '{"id":3,"amount_i64":15}\n'
            ),
            expected_fields={
                "cast_projection_runtime_execution": "true",
                "cast_projection_source_column": "raw_amount",
                "cast_projection_output_column": "amount_i64",
                "cast_projection_target_dtype": "int64",
                "cast_projection_mode": "try",
                "projected_columns": "id,amount_i64",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        SqlFixtureCase(
            case_id="string_transform_length_utf8",
            source_name="strings.csv",
            source_text="id,label\n1, Alpha \n2,BETA\n3,gamma\n",
            statement_template=(
                "SELECT id,LOWER(label) AS lowered,UPPER(label) AS raised,"
                "TRIM(label) AS trimmed,LENGTH(label) AS label_len "
                "FROM '{source}' WHERE id >= 1 LIMIT 3"
            ),
            expected_jsonl=(
                '{"id":1,"lowered":" alpha ","raised":" ALPHA ","trimmed":"Alpha","label_len":7}\n'
                '{"id":2,"lowered":"beta","raised":"BETA","trimmed":"BETA","label_len":4}\n'
                '{"id":3,"lowered":"gamma","raised":"GAMMA","trimmed":"gamma","label_len":5}\n'
            ),
            expected_fields={
                "string_transform_projection_runtime_execution": "true",
                "string_transform_projection_operator": "lower,upper,trim",
                "string_length_projection_runtime_execution": "true",
                "string_length_projection_output_column": "label_len",
                "projected_columns": "id,lowered,raised,trimmed,label_len",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        SqlFixtureCase(
            case_id="temporal_extract_utc_date32_timestamp",
            source_name="temporal.csv",
            source_text=(
                "id,event_date,event_ts\n"
                "1,2026-05-19,2026-05-19T12:34:56Z\n"
                "2,2027-01-02,2027-01-02T03:04:05Z\n"
            ),
            statement_template=(
                "SELECT id,DATE_YEAR(CAST(event_date AS date32)) AS event_year,"
                "DATE_MONTH(event_date) AS event_month,"
                "TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) AS event_hour,"
                "TIMESTAMP_SECOND(event_ts) AS event_second "
                "FROM '{source}' WHERE id >= 1 LIMIT 2"
            ),
            expected_jsonl=(
                '{"id":1,"event_year":2026,"event_month":5,"event_hour":12,"event_second":56}\n'
                '{"id":2,"event_year":2027,"event_month":1,"event_hour":3,"event_second":5}\n'
            ),
            expected_fields={
                "date_extract_projection_runtime_execution": "true",
                "date_extract_projection_operator": "date_year,date_month",
                "timestamp_extract_projection_runtime_execution": "true",
                "timestamp_extract_projection_operator": "timestamp_hour,timestamp_second",
                "projected_columns": "id,event_year,event_month,event_hour,event_second",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        SqlFixtureCase(
            case_id="null_coalesce_nullif",
            source_name="nulls.csv",
            source_text=(
                "id,label,amount,event_date\n"
                "1,alpha,8,2026-05-19\n"
                "2,missing,0,2026-01-01\n"
                "3,beta,15,2027-01-02\n"
                "4,,,\n"
            ),
            statement_template=(
                "SELECT id,COALESCE(label, 'unknown') AS label_clean,"
                "NULLIF(amount, 0) AS amount_nonzero "
                "FROM '{source}' WHERE id >= 1 LIMIT 4"
            ),
            expected_jsonl=(
                '{"id":1,"label_clean":"alpha","amount_nonzero":8}\n'
                '{"id":2,"label_clean":"missing","amount_nonzero":null}\n'
                '{"id":3,"label_clean":"beta","amount_nonzero":15}\n'
                '{"id":4,"label_clean":"unknown","amount_nonzero":null}\n'
            ),
            expected_fields={
                "null_coalesce_projection_runtime_execution": "true",
                "null_coalesce_projection_fallback_dtype": "utf8",
                "nullif_projection_runtime_execution": "true",
                "nullif_projection_sentinel_dtype": "int64",
                "projected_columns": "id,label_clean,amount_nonzero",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        SqlFixtureCase(
            case_id="predicate_projection_three_valued",
            source_name="predicate-projection.csv",
            source_text=(
                "id,label,amount,active,event_date\n"
                "1,alpha,8,true,2025-12-31\n"
                "2,,15,false,2026-05-19\n"
                "3,gamma,,,\n"
            ),
            statement_template=(
                "SELECT id,amount >= 10 AS is_large,label IS NULL AS missing_label,"
                "active IS NOT TRUE AS inactive_or_unknown,"
                "event_date >= DATE '2026-01-01' AS current_year "
                "FROM '{source}' WHERE id >= 1 LIMIT 3"
            ),
            expected_jsonl=(
                '{"id":1,"is_large":false,"missing_label":false,'
                '"inactive_or_unknown":false,"current_year":false}\n'
                '{"id":2,"is_large":true,"missing_label":true,'
                '"inactive_or_unknown":true,"current_year":true}\n'
                '{"id":3,"is_large":null,"missing_label":false,'
                '"inactive_or_unknown":true,"current_year":null}\n'
            ),
            expected_fields={
                "predicate_projection_runtime_execution": "true",
                "predicate_projection_predicate_family": "comparison,null_predicate,boolean_predicate,comparison",
                "predicate_projection_source_column": "amount,label,active,event_date",
                "projected_columns": "id,is_large,missing_label,inactive_or_unknown,current_year",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
    ]


def unsupported_cases() -> list[UnsupportedCase]:
    return [
        UnsupportedCase(
            case_id="unsupported_numeric_division_by_zero",
            source_name="numeric-unsupported.csv",
            source_text="id,amount\n1,8\n",
            statement_template="SELECT id,amount / 0 AS broken FROM '{source}' LIMIT 10",
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="numeric arithmetic projection division by zero is not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_cast_decimal128",
            source_name="cast-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template="SELECT id,CAST(label AS decimal128) AS unsupported FROM '{source}' LIMIT 10",
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment=(
                "CAST target dtype must be one of int64, float64, utf8, boolean, "
                "date32, or timestamp_micros"
            ),
        ),
    ]


def validate_matrix_manifest(
    payload: dict[str, Any] | None,
    expected_case_ids: set[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    blockers: list[str] = []
    if payload is None:
        return {}, {
            "status": "failed",
            "blockers": ["missing admitted semantics matrix manifest"],
            "row_count": 0,
            "row_ids": [],
        }
    if payload.get("schema_version") != MATRIX_SCHEMA_VERSION:
        blockers.append(
            "matrix schema_version="
            + str(payload.get("schema_version", "missing"))
            + f", expected {MATRIX_SCHEMA_VERSION}"
        )
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        rows = []
        blockers.append("matrix rows must be a list")
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            blockers.append("matrix row must be an object")
            continue
        row_id = row.get("id")
        if not isinstance(row_id, str) or not row_id:
            blockers.append("matrix row missing string id")
            continue
        if row_id in by_id:
            blockers.append(f"duplicate matrix row id {row_id}")
        by_id[row_id] = row
        for field in REQUIRED_ROW_FIELDS:
            if field not in row:
                blockers.append(f"{row_id}: missing field {field}")
            elif row[field] in ("", None, []):
                blockers.append(f"{row_id}: empty field {field}")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if row.get("oracle_boundary") not in {
            "decoded_reference_only",
            "deterministic_unsupported_diagnostic",
        }:
            blockers.append(f"{row_id}: invalid oracle_boundary={row.get('oracle_boundary')}")
        if row.get("support_state") == "unsupported_diagnostic" and row.get(
            "unsupported_diagnostic_code"
        ) == "not_applicable_executable":
            blockers.append(f"{row_id}: unsupported rows must name a diagnostic code")
    row_order = payload.get("row_order")
    if row_order != [row.get("id") for row in rows if isinstance(row, dict)]:
        blockers.append("matrix row_order must match row order")
    missing = sorted(expected_case_ids - set(by_id))
    if missing:
        blockers.append("matrix missing executable validator rows: " + ",".join(missing))
    stale = sorted(
        row_id
        for row_id, row in by_id.items()
        if row.get("runtime_validation") == "required" and row_id not in expected_case_ids
    )
    if stale:
        blockers.append("matrix required runtime rows without validator cases: " + ",".join(stale))
    summary = {
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "row_count": len(by_id),
        "row_ids": sorted(by_id),
        "remaining_matrix_gaps": payload.get("remaining_matrix_gaps", []),
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    return by_id, summary


def parse_json_output(completed: subprocess.CompletedProcess[str], label: str) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    try:
        payload = json.loads(completed.stdout)
        if not isinstance(payload, dict):
            raise ValueError("envelope is not an object")
    except Exception as exc:  # noqa: BLE001 - surfaced in report.
        payload = {}
        blockers.append(f"{label}: failed to parse JSON output: {exc}")
    return payload, blockers


def run_cli_json(
    *,
    repo_root: Path,
    binary: Path,
    args: list[str],
) -> subprocess.CompletedProcess[str]:
    return run_subprocess(repo_root=repo_root, command=[str(binary), *args, "--format", "json"])


def materialize_source(work_dir: Path, case_id: str, source_name: str, source_text: str) -> Path:
    case_dir = work_dir / "sources" / case_id
    case_dir.mkdir(parents=True, exist_ok=True)
    source_path = case_dir / source_name
    source_path.write_text(source_text, encoding="utf-8")
    return source_path


def run_executable_case(
    *,
    repo_root: Path,
    binary: Path,
    work_dir: Path,
    case: SqlFixtureCase,
    matrix_row: dict[str, Any] | None,
) -> dict[str, Any]:
    source_path = materialize_source(work_dir, case.case_id, case.source_name, case.source_text)
    statement = case.statement_template.format(source=source_path)
    completed = run_cli_json(
        repo_root=repo_root,
        binary=binary,
        args=["sql-local-source-smoke", statement],
    )
    payload, blockers = parse_json_output(completed, case.case_id)
    artifact_ref = work_dir / "artifacts" / f"{case.case_id}.json"
    write_json(
        artifact_ref,
        payload
        if payload
        else {
            "stdout_tail": tail(completed.stdout),
            "stderr_tail": tail(completed.stderr),
        },
    )
    expected_digest = digest_text(case.expected_jsonl)
    if completed.returncode != 0:
        blockers.append(f"{case.case_id}: returncode={completed.returncode}")
    if payload:
        if payload.get("status") != "success":
            blockers.append(f"{case.case_id}: status={payload.get('status')!r}, expected 'success'")
        blockers.extend(no_fallback_blockers(payload, case.case_id))
        fields = field_map(payload)
        observed_jsonl = fields.get("result_jsonl")
        if observed_jsonl != case.expected_jsonl:
            blockers.append(f"{case.case_id}: result_jsonl does not match decoded reference")
        for key, value in case.expected_fields.items():
            observed = fields.get(key)
            if observed != value:
                blockers.append(f"{case.case_id}: {key}={observed!r}, expected {value!r}")
        correctness_digest = fields.get("correctness_digest", "")
        if not correctness_digest.startswith("fnv64:"):
            blockers.append(f"{case.case_id}: correctness_digest must be fnv64-prefixed")
    else:
        fields = {}

    if matrix_row is None:
        blockers.append(f"{case.case_id}: missing matrix row")
    else:
        if matrix_row.get("support_state") not in {"executable", "property_executed"}:
            blockers.append(
                f"{case.case_id}: support_state={matrix_row.get('support_state')} is not executable"
            )
        if case.property_seed is not None and matrix_row.get("property_seed") != case.property_seed:
            blockers.append(
                f"{case.case_id}: property_seed={matrix_row.get('property_seed')} "
                f"expected {case.property_seed}"
            )
        if matrix_row.get("decoded_reference_kind") != "jsonl_inline_reference":
            blockers.append(
                f"{case.case_id}: decoded_reference_kind={matrix_row.get('decoded_reference_kind')}"
            )

    return {
        "case_id": case.case_id,
        "kind": "sql_local_source_decoded_reference",
        "command": command_text([str(binary), "sql-local-source-smoke", statement, "--format", "json"]),
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "source_ref": rel(repo_root, source_path),
        "decoded_reference_digest": expected_digest,
        "correctness_digest": fields.get("correctness_digest", "") if payload else "",
        "result_digest": fields.get("result_digest", "") if payload else "",
        "property_seed": case.property_seed,
        "selected_fields": {
            key: fields[key]
            for key in sorted(
                set(case.expected_fields)
                | {
                    "correctness_digest",
                    "fallback_attempted",
                    "external_engine_invoked",
                    "claim_gate_status",
                    "production_claim_allowed",
                    "performance_claim_allowed",
                }
            )
            if key in fields
        },
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }


def run_unsupported_case(
    *,
    repo_root: Path,
    binary: Path,
    work_dir: Path,
    case: UnsupportedCase,
    matrix_row: dict[str, Any] | None,
) -> dict[str, Any]:
    source_path = materialize_source(work_dir, case.case_id, case.source_name, case.source_text)
    statement = case.statement_template.format(source=source_path)
    completed = run_cli_json(
        repo_root=repo_root,
        binary=binary,
        args=["sql-local-source-smoke", statement],
    )
    payload, blockers = parse_json_output(completed, case.case_id)
    artifact_ref = work_dir / "artifacts" / f"{case.case_id}.json"
    write_json(
        artifact_ref,
        payload
        if payload
        else {
            "stdout_tail": tail(completed.stdout),
            "stderr_tail": tail(completed.stderr),
        },
    )
    combined = completed.stdout + completed.stderr
    if completed.returncode == 0:
        blockers.append(f"{case.case_id}: unsupported case unexpectedly succeeded")
    if payload:
        if payload.get("status") != "error":
            blockers.append(f"{case.case_id}: status={payload.get('status')!r}, expected 'error'")
        diagnostics = payload.get("diagnostics")
        if not isinstance(diagnostics, list) or not diagnostics:
            blockers.append(f"{case.case_id}: missing diagnostic row")
        else:
            codes = {str(row.get("code")) for row in diagnostics if isinstance(row, dict)}
            if case.diagnostic_code not in codes:
                blockers.append(f"{case.case_id}: diagnostic code {case.diagnostic_code} missing")
        blockers.extend(no_fallback_blockers(payload, case.case_id))
    if case.diagnostic_fragment not in combined:
        blockers.append(f"{case.case_id}: missing diagnostic fragment {case.diagnostic_fragment!r}")
    if "external_engine_invoked=false" not in combined:
        blockers.append(f"{case.case_id}: missing external_engine_invoked=false diagnostic text")
    if matrix_row is None:
        blockers.append(f"{case.case_id}: missing matrix row")
    else:
        if matrix_row.get("support_state") != "unsupported_diagnostic":
            blockers.append(f"{case.case_id}: support_state={matrix_row.get('support_state')}")
        if matrix_row.get("unsupported_diagnostic_code") != case.diagnostic_code:
            blockers.append(
                f"{case.case_id}: unsupported_diagnostic_code="
                f"{matrix_row.get('unsupported_diagnostic_code')}"
            )
        if case.diagnostic_fragment not in str(matrix_row.get("unsupported_diagnostic_message")):
            blockers.append(f"{case.case_id}: matrix diagnostic message does not include fragment")

    return {
        "case_id": case.case_id,
        "kind": "unsupported_diagnostic",
        "command": command_text([str(binary), "sql-local-source-smoke", statement, "--format", "json"]),
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "source_ref": rel(repo_root, source_path),
        "diagnostic_code": case.diagnostic_code,
        "diagnostic_fragment": case.diagnostic_fragment,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }


def run_support_report_stage(
    *,
    repo_root: Path,
    binary: Path,
    work_dir: Path,
    stage_id: str,
    cli_args: list[str],
    expected_fields: dict[str, str],
    integer_minimums: dict[str, int] | None = None,
) -> dict[str, Any]:
    completed = run_cli_json(repo_root=repo_root, binary=binary, args=cli_args)
    payload, blockers = parse_json_output(completed, stage_id)
    artifact_ref = work_dir / "artifacts" / f"{stage_id}.json"
    write_json(
        artifact_ref,
        payload
        if payload
        else {
            "stdout_tail": tail(completed.stdout),
            "stderr_tail": tail(completed.stderr),
        },
    )
    if completed.returncode != 0:
        blockers.append(f"{stage_id}: returncode={completed.returncode}")
    if payload:
        if payload.get("status") != "success":
            blockers.append(f"{stage_id}: status={payload.get('status')!r}, expected 'success'")
        blockers.extend(no_fallback_blockers(payload, stage_id))
        fields = field_map(payload)
        for key, value in expected_fields.items():
            observed = fields.get(key)
            if observed != value:
                blockers.append(f"{stage_id}: {key}={observed!r}, expected {value!r}")
        for key, minimum in (integer_minimums or {}).items():
            try:
                observed_int = int(fields.get(key, ""))
            except ValueError:
                blockers.append(f"{stage_id}: {key}={fields.get(key)!r} is not an integer")
                continue
            if observed_int < minimum:
                blockers.append(f"{stage_id}: {key}={observed_int}, expected >= {minimum}")
    else:
        fields = {}
    return {
        "case_id": stage_id,
        "kind": "support_report",
        "command": command_text([str(binary), *cli_args, "--format", "json"]),
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "selected_fields": {
            key: fields[key]
            for key in sorted(set(expected_fields) | set((integer_minimums or {})))
            if key in fields
        },
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    args = parse_args()
    started = time.perf_counter()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    work_dir = resolve(repo_root, args.work_dir)
    matrix_path = resolve(repo_root, args.matrix)
    work_dir.mkdir(parents=True, exist_ok=True)
    binary = locate_binary(repo_root, args.binary)

    build = build_binary(repo_root, args.features, args.skip_build, binary)
    cases = executable_cases()
    unsupported = unsupported_cases()
    expected_case_ids = {case.case_id for case in cases} | {case.case_id for case in unsupported}
    matrix_rows, matrix_summary = validate_matrix_manifest(load_json(matrix_path), expected_case_ids)

    stages: list[dict[str, Any]] = []
    if build["status"] == "passed":
        for case in cases:
            stages.append(
                run_executable_case(
                    repo_root=repo_root,
                    binary=binary,
                    work_dir=work_dir,
                    case=case,
                    matrix_row=matrix_rows.get(case.case_id),
                )
            )
        for case in unsupported:
            stages.append(
                run_unsupported_case(
                    repo_root=repo_root,
                    binary=binary,
                    work_dir=work_dir,
                    case=case,
                    matrix_row=matrix_rows.get(case.case_id),
                )
            )
        stages.append(
            run_support_report_stage(
                repo_root=repo_root,
                binary=binary,
                work_dir=work_dir,
                stage_id="semantic_conformance_suite",
                cli_args=["semantic-conformance-suite"],
                expected_fields={
                    "semantic_profile": "ShardLoomNative",
                    "failed_fixture_count": "0",
                    "external_oracle_used": "false",
                    "fallback_attempted": "false",
                    "external_engine_invoked": "false",
                    "in_memory_fixture_execution": "true",
                    "query_execution": "false",
                    "runtime_execution": "false",
                },
                integer_minimums={"executed_fixture_count": 8, "passed_fixture_count": 8},
            )
        )
        stages.append(
            run_support_report_stage(
                repo_root=repo_root,
                binary=binary,
                work_dir=work_dir,
                stage_id="correctness_harness_boundary",
                cli_args=["correctness-harness-plan"],
                expected_fields={
                    "schema_version": "shardloom.correctness_differential_harness.v1",
                    "harness_status": "needs_evidence",
                    "property_fuzz_execution_performed": "false",
                    "decoded_reference_execution_performed": "false",
                    "external_engine_execution": "false",
                    "fallback_attempted": "false",
                    "production_claim_allowed": "false",
                },
                integer_minimums={"generated_property_fixture_count": 4, "fuzz_seed_count": 4},
            )
        )

    operator_families = sorted(
        {
            str(row.get("operator_family"))
            for row in matrix_rows.values()
            if str(row.get("operator_family", "")).strip()
        }
    )
    blockers = list(build.get("blockers", []))
    blockers.extend(matrix_summary["blockers"])
    blockers.extend(
        f"{stage['case_id']}: {blocker}" for stage in stages for blocker in stage["blockers"]
    )
    passed = not blockers
    property_stages = [stage for stage in stages if stage.get("property_seed") is not None]
    executable_stage_ids = [case.case_id for case in cases]
    unsupported_stage_ids = [case.case_id for case in unsupported]
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "admitted_semantics_validator_status": "passed" if passed else "failed",
        "matrix_schema_version": MATRIX_SCHEMA_VERSION,
        "matrix_ref": rel(repo_root, matrix_path),
        "matrix_status": matrix_summary["status"],
        "matrix_row_count": matrix_summary["row_count"],
        "matrix_row_ids": matrix_summary["row_ids"],
        "covered_operator_families": operator_families,
        "covered_operator_family_count": len(operator_families),
        "executable_fixture_count": len(cases),
        "unsupported_diagnostic_count": len(unsupported),
        "property_lane_count": len(property_stages),
        "property_seed_order": [
            stage["property_seed"] for stage in property_stages if stage.get("property_seed") is not None
        ],
        "decoded_reference_differential_execution_performed": bool(cases),
        "property_execution_performed": bool(property_stages),
        "semantic_conformance_suite_status": next(
            (stage["status"] for stage in stages if stage["case_id"] == "semantic_conformance_suite"),
            "not_run",
        ),
        "correctness_harness_boundary_status": next(
            (stage["status"] for stage in stages if stage["case_id"] == "correctness_harness_boundary"),
            "not_run",
        ),
        "executable_case_ids": executable_stage_ids,
        "unsupported_case_ids": unsupported_stage_ids,
        "stage_count": len(stages),
        "stages": stages,
        "build": build,
        "blockers": blockers,
        "remaining_matrix_gaps": matrix_summary["remaining_matrix_gaps"],
        "claim_gate_status": "admitted_semantics_fixture_matrix_only",
        "oracle_boundary": "decoded_reference_only_no_external_engine",
        "external_oracle_used": False,
        "external_engines_allowed_as_oracles_only": True,
        "production_claim_allowed": False,
        "ansi_sql_claim_allowed": False,
        "performance_claim_allowed": False,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "package_publication_performed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "elapsed_millis": round((time.perf_counter() - started) * 1000.0, 4),
    }
    for field in FALSE_REPORT_FIELDS:
        if report.get(field) is not False:
            report.setdefault("blockers", []).append(f"{field} must be false")
            report["status"] = "failed"
            report["admitted_semantics_validator_status"] = "failed"
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
