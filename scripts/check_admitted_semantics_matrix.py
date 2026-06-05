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
    auxiliary_sources: tuple[tuple[str, str, str], ...] = ()


@dataclass(frozen=True)
class UnsupportedCase:
    case_id: str
    source_name: str
    source_text: str
    statement_template: str
    diagnostic_code: str
    diagnostic_fragment: str
    support_state: str = "unsupported_diagnostic"
    oracle_boundary: str = "deterministic_unsupported_diagnostic"
    stage_kind: str = "unsupported_diagnostic"


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
    env = os.environ.copy()
    if command and command[0] == "cargo":
        env.setdefault("CARGO_INCREMENTAL", "0")
    return subprocess.run(
        command,
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


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


def string_function_composition_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="string_function_composition_utf8",
        source_name="string-functions.csv",
        source_text="id,label,segment\n1,alpha,north\n2,beta,east\n3,alpaca,north\n4,,west\n",
        statement_template=(
            "SELECT id,CONCAT(label, '-', segment) AS label_key,SUBSTR(label, 2, 3) AS middle,"
            "LEFT(label, 2) AS prefix,RIGHT(label, 2) AS suffix,REPLACE(label, 'a', '') AS scrubbed "
            "FROM '{source}' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label_key":"alpha-north","middle":"lph","prefix":"al",'
            '"suffix":"ha","scrubbed":"lph"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "string_function",
            "string_function_runtime_execution": "true",
            "string_function_operator": "concat",
            "string_function_source_column": "label+segment",
            "string_function_literal_count": "2",
            "string_function_projection_runtime_execution": "true",
            "string_function_projection_operator": "concat,substr,left,right,replace",
            "string_function_projection_source_column": "label+segment,label,label,label,label",
            "string_function_projection_output_column": "label_key,middle,prefix,suffix,scrubbed",
            "string_function_projection_literal_count": "1,2,1,1,2",
            "projected_columns": "id,label_key,middle,prefix,suffix,scrubbed",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def temporal_arithmetic_difference_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="temporal_arithmetic_difference_utc",
        source_name="temporal-arithmetic.csv",
        source_text=(
            "id,start_date,end_date,start_ts,end_ts\n"
            "1,2026-05-19,2026-05-23,2026-05-19T12:34:45Z,2026-05-19T12:37:50Z\n"
            "2,2026-01-01,2026-01-10,2026-01-01T00:00:00Z,2026-01-01T00:01:30Z\n"
        ),
        statement_template=(
            "SELECT id,DATE_ADD_DAYS(CAST(start_date AS date32), 3) AS plus_three,"
            "DATE_SUB_DAYS(end_date, 2) AS end_minus_two,"
            "DATE_DIFF_DAYS(CAST(end_date AS date32), start_date) AS span_days,"
            "TIMESTAMP_ADD_SECONDS(CAST(start_ts AS timestamp_micros), 90) AS shifted_ts,"
            "TIMESTAMP_DIFF_SECONDS(CAST(end_ts AS timestamp_micros), start_ts) AS elapsed_seconds "
            "FROM '{source}' WHERE DATE_DIFF_DAYS(end_date, start_date) >= 4 LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"plus_three":"2026-05-22","end_minus_two":"2026-05-21",'
            '"span_days":4,"shifted_ts":"2026-05-19T12:36:15Z","elapsed_seconds":185}\n'
            '{"id":2,"plus_three":"2026-01-04","end_minus_two":"2026-01-08",'
            '"span_days":9,"shifted_ts":"2026-01-01T00:01:30Z","elapsed_seconds":90}\n'
        ),
        expected_fields={
            "predicate_operator_family": "generic_expression",
            "generic_expression_predicate_runtime_execution": "true",
            "generic_expression_predicate_operator_family": "temporal_difference",
            "date_arithmetic_projection_runtime_execution": "true",
            "date_arithmetic_projection_operator": "date_add_days,date_sub_days",
            "date_arithmetic_projection_output_column": "plus_three,end_minus_two",
            "timestamp_arithmetic_projection_runtime_execution": "true",
            "timestamp_arithmetic_projection_operator": "timestamp_add_seconds",
            "timestamp_arithmetic_projection_output_column": "shifted_ts",
            "generic_expression_projection_runtime_execution": "true",
            "generic_expression_projection_output_column": "span_days,elapsed_seconds",
            "projected_columns": "id,plus_three,end_minus_two,span_days,shifted_ts,elapsed_seconds",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def interval_literal_temporal_arithmetic_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="interval_literal_temporal_arithmetic",
        source_name="interval-temporal-arithmetic.csv",
        source_text=(
            "id,event_date,event_ts\n"
            "1,2026-05-19,2026-05-19T12:34:45Z\n"
            "2,2026-01-01,2026-01-01T00:00:00Z\n"
            "3,,\n"
        ),
        statement_template=(
            "SELECT id,DATE_ADD_DAYS(event_date, INTERVAL '1' DAY) AS next_day,"
            "DATE_SUB_DAYS(event_date, INTERVAL '2' DAYS) AS prior_two,"
            "TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '90' SECOND) AS shifted_ts,"
            "TIMESTAMP_SUB_SECONDS(event_ts, INTERVAL '1' MINUTE) AS prior_minute "
            "FROM '{source}' WHERE TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' HOUR) "
            ">= TIMESTAMP '2026-01-01T01:00:00Z' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"next_day":"2026-05-20","prior_two":"2026-05-17",'
            '"shifted_ts":"2026-05-19T12:36:15Z","prior_minute":"2026-05-19T12:33:45Z"}\n'
            '{"id":2,"next_day":"2026-01-02","prior_two":"2025-12-30",'
            '"shifted_ts":"2026-01-01T00:01:30Z","prior_minute":"2025-12-31T23:59:00Z"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "timestamp_arithmetic",
            "timestamp_arithmetic_runtime_execution": "true",
            "timestamp_arithmetic_operator": "timestamp_add_seconds",
            "timestamp_arithmetic_seconds": "3600",
            "timestamp_arithmetic_source_column": "event_ts",
            "date_arithmetic_projection_runtime_execution": "true",
            "date_arithmetic_projection_operator": "date_add_days,date_sub_days",
            "date_arithmetic_projection_days": "1,2",
            "date_arithmetic_projection_output_column": "next_day,prior_two",
            "timestamp_arithmetic_projection_runtime_execution": "true",
            "timestamp_arithmetic_projection_operator": "timestamp_add_seconds,timestamp_sub_seconds",
            "timestamp_arithmetic_projection_seconds": "90,60",
            "timestamp_arithmetic_projection_output_column": "shifted_ts,prior_minute",
            "projected_columns": "id,next_day,prior_two,shifted_ts,prior_minute",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def timestamp_offset_literal_normalization_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="timestamp_offset_literal_normalization",
        source_name="timestamp-offset-normalization.csv",
        source_text=(
            "id,event_ts\n"
            "1,2026-05-19T17:34:55Z\n"
            "2,2026-05-19T12:34:56-05:00\n"
            "3,2026-05-19T17:35:00Z\n"
            "4,\n"
        ),
        statement_template=(
            "SELECT id,CAST(event_ts AS timestamp_micros) AS event_ts_utc "
            "FROM '{source}' WHERE CAST(event_ts AS timestamp_micros) "
            ">= TIMESTAMP '2026-05-19T12:34:56-05:00' ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":2,"event_ts_utc":"2026-05-19T17:34:56Z"}\n'
            '{"id":3,"event_ts_utc":"2026-05-19T17:35:00Z"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "cast",
            "cast_runtime_execution": "true",
            "cast_source_column": "event_ts",
            "cast_target_dtype": "timestamp_micros",
            "cast_mode": "strict",
            "cast_projection_runtime_execution": "true",
            "cast_projection_source_column": "event_ts",
            "cast_projection_output_column": "event_ts_utc",
            "cast_projection_target_dtype": "timestamp_micros",
            "cast_projection_mode": "strict",
            "sort_keys": "id",
            "sort_direction": "asc",
            "projected_columns": "id,event_ts_utc",
            "fallback_attempted": "false",
            "external_engine_invoked": "false",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def conditional_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="conditional_projection_case_when",
        source_name="conditional.csv",
        source_text=(
            "id,label,amount,event_date,preferred_label,fallback_label\n"
            "1,alpha,8,2025-12-31,preferred-alpha,fallback-alpha\n"
            "2,beta,15,2026-05-19,preferred-beta,fallback-beta\n"
            "3,gamma,,2026-06-01,preferred-gamma,fallback-gamma\n"
        ),
        statement_template=(
            "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 'small' END AS size_band,"
            "CASE WHEN event_date >= DATE '2026-01-01' THEN DATE '2026-12-31' ELSE DATE '2025-12-31' END AS cutoff_day,"
            "CASE WHEN amount >= 10 THEN preferred_label ELSE fallback_label END AS label_choice "
            "FROM '{source}' WHERE id >= 1 LIMIT 3"
        ),
        expected_jsonl=(
            '{"id":1,"size_band":"small","cutoff_day":"2025-12-31","label_choice":"fallback-alpha"}\n'
            '{"id":2,"size_band":"large","cutoff_day":"2026-12-31","label_choice":"preferred-beta"}\n'
            '{"id":3,"size_band":"small","cutoff_day":"2026-12-31","label_choice":"fallback-gamma"}\n'
        ),
        expected_fields={
            "conditional_projection_runtime_execution": "true",
            "conditional_projection_predicate_family": "comparison,comparison,comparison",
            "conditional_projection_source_column": "amount,event_date,amount+fallback_label+preferred_label",
            "conditional_projection_output_column": "size_band,cutoff_day,label_choice",
            "conditional_projection_then_dtype": "utf8,date32,utf8",
            "conditional_projection_else_dtype": "utf8,date32,utf8",
            "projected_columns": "id,size_band,cutoff_day,label_choice",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def binary_hex_literal_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_hex_literal_projection",
        source_name="binary-hex-literal.csv",
        source_text="id,label\n1,alpha\n2,beta\n",
        statement_template="SELECT id,X'00ff10' AS payload FROM '{source}' LIMIT 10",
        expected_jsonl=(
            '{"id":1,"payload":"binary[hex=00ff10]"}\n'
            '{"id":2,"payload":"binary[hex=00ff10]"}\n'
        ),
        expected_fields={
            "literal_projection_runtime_execution": "true",
            "literal_projection_columns": "payload",
            "literal_projection_count": "1",
            "literal_projection_dtype": "binary",
            "binary_literal_projection_runtime_execution": "true",
            "binary_literal_projection_columns": "payload",
            "binary_literal_projection_byte_count": "3",
            "binary_literal_projection_hex_value": "00ff10",
            "projected_columns": "id,payload",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def binary_text_literal_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_text_literal_projection",
        source_name="binary-text-literal.csv",
        source_text="id,label\n1,alpha\n2,beta\n",
        statement_template=(
            "SELECT id,BINARY 'ok' AS marker,BLOB 'raw' AS payload "
            "FROM '{source}' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"marker":"binary[hex=6f6b]","payload":"binary[hex=726177]"}\n'
            '{"id":2,"marker":"binary[hex=6f6b]","payload":"binary[hex=726177]"}\n'
        ),
        expected_fields={
            "literal_projection_runtime_execution": "true",
            "literal_projection_columns": "marker,payload",
            "literal_projection_count": "2",
            "literal_projection_dtype": "binary,binary",
            "binary_literal_projection_runtime_execution": "true",
            "binary_literal_projection_columns": "marker,payload",
            "binary_literal_projection_byte_count": "2,3",
            "binary_literal_projection_hex_value": "6f6b,726177",
            "projected_columns": "id,marker,payload",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def complex_array_literal_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="complex_array_literal_projection",
        source_name="complex-array-literal.csv",
        source_text="id,label\n1,alpha\n2,beta\n",
        statement_template="SELECT id,ARRAY[1,2,NULL] AS values FROM '{source}' LIMIT 10",
        expected_jsonl=(
            '{"id":1,"values":[1,2,null]}\n'
            '{"id":2,"values":[1,2,null]}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_complex_projection_limit",
            "complex_projection_runtime_execution": "true",
            "complex_projection_columns": "values",
            "complex_projection_count": "1",
            "complex_projection_kind": "array_literal",
            "complex_projection_output_dtype": "list",
            "complex_projection_source_column": "not_applicable",
            "complex_projection_output_boundary": "jsonl_nested_result_boundary_only",
            "complex_projection_equality_semantics": "admitted_distinct_projection_and_union_distinct_result_boundary_values_only",
            "projected_columns": "id,values",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def complex_struct_source_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="complex_struct_source_projection",
        source_name="complex-struct-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,\n",
        statement_template="SELECT id,STRUCT(label, amount) AS payload FROM '{source}' LIMIT 10",
        expected_jsonl=(
            '{"id":1,"payload":{"label":"alpha","amount":8}}\n'
            '{"id":2,"payload":{"label":"beta","amount":null}}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_complex_projection_limit",
            "complex_projection_runtime_execution": "true",
            "complex_projection_columns": "payload",
            "complex_projection_count": "1",
            "complex_projection_kind": "struct_source_columns",
            "complex_projection_output_dtype": "struct",
            "complex_projection_source_column": "label,amount",
            "complex_projection_output_boundary": "jsonl_nested_result_boundary_only",
            "complex_projection_equality_semantics": "admitted_distinct_projection_and_union_distinct_result_boundary_values_only",
            "projected_columns": "id,payload",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def complex_distinct_projection_equality_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="complex_distinct_projection_equality",
        source_name="complex-distinct.csv",
        source_text="id,label,amount\n1,alpha,8\n2,alpha,8\n3,beta,\n",
        statement_template=(
            "SELECT DISTINCT label,STRUCT(label, amount) AS payload,ARRAY[1,2,NULL] AS values "
            "FROM '{source}' LIMIT 10"
        ),
        expected_jsonl=(
            '{"label":"alpha","payload":{"label":"alpha","amount":8},"values":[1,2,null]}\n'
            '{"label":"beta","payload":{"label":"beta","amount":null},"values":[1,2,null]}\n'
        ),
        expected_fields={
            "distinct_projection_runtime_execution": "true",
            "distinct_projection_output_columns": "label,payload,values",
            "distinct_projection_input_row_count": "3",
            "distinct_projection_output_row_count": "2",
            "complex_projection_runtime_execution": "true",
            "complex_projection_columns": "payload,values",
            "complex_projection_count": "2",
            "complex_projection_kind": "struct_source_columns,array_literal",
            "complex_projection_output_dtype": "struct,list",
            "complex_projection_source_column": "label,amount",
            "complex_projection_output_boundary": "jsonl_nested_result_boundary_only",
            "complex_projection_equality_semantics": "admitted_distinct_projection_and_union_distinct_result_boundary_values_only",
            "projected_columns": "label,payload,values",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def complex_order_by_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="complex_order_by_projection",
        source_name="complex-order.csv",
        source_text="id,label,amount\n1,gamma,13\n2,alpha,8\n3,beta,\n",
        statement_template=(
            "SELECT id,STRUCT(label, amount) AS payload FROM '{source}' "
            "ORDER BY payload ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":2,"payload":{"label":"alpha","amount":8}}\n'
            '{"id":3,"payload":{"label":"beta","amount":null}}\n'
            '{"id":1,"payload":{"label":"gamma","amount":13}}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_complex_projection_order_by_topn_limit",
            "order_by_runtime_execution": "true",
            "top_n_runtime_execution": "true",
            "sort_operator_family": "single_key_complex_result_boundary_topn",
            "sort_keys": "payload",
            "sort_direction": "asc",
            "complex_projection_runtime_execution": "true",
            "complex_projection_columns": "payload",
            "complex_projection_count": "1",
            "complex_projection_kind": "struct_source_columns",
            "complex_projection_output_dtype": "struct",
            "complex_projection_source_column": "label,amount",
            "complex_projection_output_boundary": "jsonl_nested_result_boundary_only",
            "complex_projection_ordering_columns": "payload",
            "complex_projection_ordering_semantics": "admitted_canonical_structural_result_boundary_values_only",
            "projected_columns": "id,payload",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def sql_union_complex_distinct_equality_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="sql_union_complex_distinct_equality",
        source_name="complex-union-left.csv",
        source_text="id,label\n1,alpha\n2,beta\n",
        auxiliary_sources=(
            (
                "right",
                "complex-union-right.csv",
                "id,label\n1,alpha\n3,gamma\n",
            ),
        ),
        statement_template=(
            "SELECT id,ARRAY[1] AS values,STRUCT(label) AS payload FROM '{source}' "
            "UNION SELECT id,ARRAY[1] AS values,STRUCT(label) AS payload FROM '{right}' "
            "ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"values":[1],"payload":{"label":"alpha"}}\n'
            '{"id":2,"values":[1],"payload":{"label":"beta"}}\n'
            '{"id":3,"values":[1],"payload":{"label":"gamma"}}\n'
        ),
        expected_fields={
            "sql_union_runtime_execution": "true",
            "sql_union_mode": "distinct",
            "sql_union_branch_count": "2",
            "sql_union_input_row_count": "4",
            "sql_union_distinct_input_row_count": "3",
            "sql_union_output_row_count": "3",
            "sql_union_order_by_runtime_execution": "true",
            "sort_keys": "id",
            "sort_direction": "asc",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def sql_union_complex_ordering_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="sql_union_complex_ordering",
        source_name="complex-union-order-left.csv",
        source_text="id,label\n1,alpha\n2,beta\n",
        auxiliary_sources=(
            (
                "right",
                "complex-union-order-right.csv",
                "id,label\n3,gamma\n4,delta\n",
            ),
        ),
        statement_template=(
            "SELECT id,STRUCT(label) AS payload FROM '{source}' "
            "UNION ALL SELECT id,STRUCT(label) AS payload FROM '{right}' "
            "ORDER BY payload DESC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":3,"payload":{"label":"gamma"}}\n'
            '{"id":4,"payload":{"label":"delta"}}\n'
            '{"id":2,"payload":{"label":"beta"}}\n'
            '{"id":1,"payload":{"label":"alpha"}}\n'
        ),
        expected_fields={
            "sql_union_runtime_execution": "true",
            "sql_union_mode": "all",
            "sql_union_branch_count": "2",
            "sql_union_input_row_count": "4",
            "sql_union_output_row_count": "4",
            "sql_union_order_by_runtime_execution": "true",
            "sort_operator_family": "single_key_complex_result_boundary_topn",
            "sort_keys": "payload",
            "sort_direction": "desc",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def binary_cast_projection_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_cast_projection_predicate",
        source_name="binary-cast.csv",
        source_text="id,label,amount\n1,alpha,42\n2,beta,7\n3,,0\n",
        statement_template=(
            "SELECT id,CAST(label AS binary) AS label_bytes,"
            "TRY_CAST(amount AS varbinary) AS amount_bytes FROM '{source}' "
            "WHERE CAST(label AS binary) = X'616c706861' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label_bytes":"binary[hex=616c706861]",'
            '"amount_bytes":"binary[hex=3432]"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "cast",
            "cast_runtime_execution": "true",
            "cast_source_column": "label",
            "cast_target_dtype": "binary",
            "cast_mode": "strict",
            "cast_projection_runtime_execution": "true",
            "cast_projection_source_column": "label,amount",
            "cast_projection_output_column": "label_bytes,amount_bytes",
            "cast_projection_target_dtype": "binary,binary",
            "cast_projection_mode": "strict,try",
            "projected_columns": "id,label_bytes,amount_bytes",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def binary_cast_ordering_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_cast_ordering_predicate",
        source_name="binary-cast-ordering.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,alp\n4,\n5,gamma\n",
        statement_template=(
            "SELECT id,CAST(label AS binary) AS label_bytes FROM '{source}' "
            "WHERE CAST(label AS binary) > BINARY 'alpha' ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":2,"label_bytes":"binary[hex=62657461]"}\n'
            '{"id":5,"label_bytes":"binary[hex=67616d6d61]"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "cast",
            "cast_runtime_execution": "true",
            "cast_source_column": "label",
            "cast_target_dtype": "binary",
            "cast_mode": "strict",
            "cast_projection_runtime_execution": "true",
            "cast_projection_source_column": "label",
            "cast_projection_output_column": "label_bytes",
            "cast_projection_target_dtype": "binary",
            "cast_projection_mode": "strict",
            "projected_columns": "id,label_bytes",
            "fallback_attempted": "false",
            "external_engine_invoked": "false",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def decimal_cast_projection_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="decimal_cast_projection_predicate",
        source_name="decimal-cast.csv",
        source_text=(
            "id,amount,raw_amount\n"
            "1,12.34,12.30\n"
            "2,8.00,bad\n"
            "3,,7.50\n"
        ),
        statement_template=(
            "SELECT id,CAST(amount AS decimal128(10,2)) AS amount_decimal,"
            "TRY_CAST(raw_amount AS decimal(10,2)) AS raw_decimal FROM '{source}' "
            "WHERE CAST(amount AS numeric(10,2)) >= 10.00 LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"amount_decimal":"12.34","raw_decimal":"12.30"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "cast",
            "cast_runtime_execution": "true",
            "cast_source_column": "amount",
            "cast_target_dtype": "decimal128(10,2)",
            "cast_mode": "strict",
            "cast_projection_runtime_execution": "true",
            "cast_projection_source_column": "amount,raw_amount",
            "cast_projection_output_column": "amount_decimal,raw_decimal",
            "cast_projection_target_dtype": "decimal128(10,2),decimal128(10,2)",
            "cast_projection_mode": "strict,try",
            "decimal_cast_runtime_execution": "true",
            "decimal_cast_source_column": "amount,amount,raw_amount",
            "decimal_cast_output_column": "amount_decimal,raw_decimal",
            "decimal_cast_target_dtype": "decimal128(10,2),decimal128(10,2),decimal128(10,2)",
            "decimal_cast_precision": "10,10,10",
            "decimal_cast_scale": "2,2,2",
            "decimal_cast_mode": "strict,strict,try",
            "decimal_cast_output_boundary": "jsonl_exact_decimal_string_csv_exact_decimal_text_parquet_arrow_avro_vortex_typed_decimal_orc_blocked",
            "projected_columns": "id,amount_decimal,raw_decimal",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def decimal_arithmetic_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="decimal_arithmetic_projection",
        source_name="decimal-arithmetic.csv",
        source_text="id,amount\n1,12.34\n2,15.50\n3,21.25\n",
        statement_template=(
            "SELECT id,CAST(amount AS decimal128(10,2)) + "
            "CAST('1.25' AS decimal128(10,2)) AS adjusted,"
            "CAST(amount AS decimal128(10,2)) / 2 AS half,"
            "CAST(amount AS decimal128(10,2)) * CAST('1.50' AS decimal128(3,2)) AS scaled "
            "FROM '{source}' "
            "WHERE CAST(amount AS decimal128(10,2)) + 0 >= CAST('12.34' AS decimal128(10,2)) "
            "LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"adjusted":"13.59","half":"6.170000","scaled":"18.5100"}\n'
            '{"id":2,"adjusted":"16.75","half":"7.750000","scaled":"23.2500"}\n'
            '{"id":3,"adjusted":"22.50","half":"10.625000","scaled":"31.8750"}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_computed_projection_filter_limit",
            "generic_expression_predicate_runtime_execution": "true",
            "generic_expression_projection_runtime_execution": "true",
            "generic_expression_projection_source_column": "amount,amount,amount",
            "generic_expression_projection_output_column": "adjusted,half,scaled",
            "generic_expression_projection_operator_family": "cast+numeric_binary,cast+numeric_binary,cast+numeric_binary",
            "generic_expression_projection_binary_operator_count": "3",
            "projected_columns": "id,adjusted,half,scaled",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def binary_helper_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_helper_projection",
        source_name="binary-helper-projection.csv",
        source_text=(
            "id,hex_payload,b64_payload\n"
            "1,00ff10,AP8Q\n"
            "2,616c706861,YWxwaGE=\n"
            "3,,\n"
        ),
        statement_template=(
            "SELECT id,UNHEX(hex_payload) AS payload_hex,"
            "FROM_BASE64(b64_payload) AS payload_b64 FROM '{source}' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"payload_hex":"binary[hex=00ff10]",'
            '"payload_b64":"binary[hex=00ff10]"}\n'
            '{"id":2,"payload_hex":"binary[hex=616c706861]",'
            '"payload_b64":"binary[hex=616c706861]"}\n'
            '{"id":3,"payload_hex":null,"payload_b64":null}\n'
        ),
        expected_fields={
            "binary_helper_projection_runtime_execution": "true",
            "binary_helper_projection_operator": "unhex,from_base64",
            "binary_helper_projection_source_column": "hex_payload,b64_payload",
            "binary_helper_projection_output_column": "payload_hex,payload_b64",
            "binary_helper_projection_output_dtype": "binary",
            "binary_helper_projection_null_semantics": "null_propagating_utf8_decode",
            "projected_columns": "id,payload_hex,payload_b64",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def in_predicate_literal_null_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="in_predicate_literal_null_semantics",
        source_name="in-null.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,,21\n4,gamma,13\n",
        statement_template="SELECT id,label FROM '{source}' WHERE label IN ('alpha', NULL) LIMIT 10",
        expected_jsonl='{"id":1,"label":"alpha"}\n',
        expected_fields={
            "predicate_operator_family": "in_predicate",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "2",
            "in_list_null_value_count": "1",
            "in_predicate_null_semantics": "sql_three_valued_where_filter",
            "selected_row_count": "1",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def row_value_in_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="row_value_in_predicate_semantics",
        source_name="row-value-in.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,alpha,13\n5,,34\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) "
            "IN ((1,'alpha'),(3,'gamma'),(5,NULL)) LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":3,"label":"gamma"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "row_value_in_predicate",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "3",
            "in_list_null_value_count": "1",
            "row_value_in_predicate_runtime_execution": "true",
            "row_value_in_source_columns": "id,label",
            "row_value_in_column_groups": "id+label",
            "row_value_in_column_count": "2",
            "row_value_in_tuple_count": "3",
            "row_value_in_null_value_count": "1",
            "row_value_in_null_semantics": "sql_row_value_three_valued_where_filter",
            "in_predicate_null_semantics": "sql_three_valued_where_filter",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="row_value_in_subquery_semantics",
        source_name="row-value-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n5,,34\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) IN ("
            "SELECT allowed_id,allowed_label FROM '{allowed}' "
            "WHERE active IS TRUE ORDER BY score DESC LIMIT 3"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":3,"label":"gamma"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "row_value_in_subquery",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "3",
            "in_list_null_value_count": "1",
            "row_value_in_predicate_runtime_execution": "true",
            "row_value_in_source_columns": "id,label",
            "row_value_in_column_groups": "id+label",
            "row_value_in_column_count": "2",
            "row_value_in_tuple_count": "3",
            "row_value_in_null_value_count": "1",
            "row_value_in_null_semantics": "sql_row_value_three_valued_where_filter",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_order_by_runtime_execution": "true",
            "in_subquery_limit_runtime_execution": "true",
            "in_subquery_source_column": "allowed_id,allowed_label",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "5",
            "in_subquery_filtered_row_count": "4",
            "in_subquery_materialization_bound": "32",
            "in_subquery_materialized_value_count": "3",
            "in_subquery_materialized_null_value_count": "1",
            "in_predicate_null_semantics": "sql_three_valued_where_filter",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "row-value-in-subquery-allowed.csv",
                "allowed_id,allowed_label,active,score\n1,alpha,true,20\n3,gamma,true,40\n5,NULL,true,50\n4,delta,false,60\n2,beta,true,10\n",
            ),
        ),
    )


def exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="exists_subquery_semantics",
        source_name="exists-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT * FROM '{allowed}' WHERE active IS TRUE ORDER BY score DESC LIMIT 1"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "exists_subquery",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_projection_kind": "wildcard",
            "exists_subquery_source_column": "not_applicable",
            "exists_subquery_source_format": "csv",
            "exists_subquery_filter_runtime_execution": "true",
            "exists_subquery_order_by_runtime_execution": "true",
            "exists_subquery_limit_runtime_execution": "true",
            "exists_subquery_input_row_count": "3",
            "exists_subquery_filtered_row_count": "2",
            "exists_subquery_bounded_row_count": "1",
            "exists_subquery_scan_bound": "50000",
            "exists_subquery_result": "true",
            "exists_subquery_null_semantics": "sql_exists_two_valued_presence_test",
            "selected_row_count": "3",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "exists-subquery-allowed.csv",
                "active,score\nfalse,10\ntrue,30\ntrue,20\n",
            ),
        ),
    )


def quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="quantified_subquery_semantics",
        source_name="quantified-subquery-source.csv",
        source_text=(
            "id,label,amount\n"
            "1,alpha,8\n"
            "2,beta,15\n"
            "3,gamma,21\n"
            "4,delta,13\n"
            "5,epsilon,34\n"
        ),
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount > ALL ("
            "SELECT threshold FROM '{thresholds}' "
            "WHERE active IS TRUE ORDER BY score DESC LIMIT 2"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":3,"label":"gamma"}\n'
            '{"id":5,"label":"epsilon"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "quantified_subquery",
            "quantified_subquery_runtime_execution": "true",
            "quantified_subquery_quantifier": "all",
            "quantified_subquery_comparison_operator": "gt",
            "quantified_subquery_source_column": "threshold",
            "quantified_subquery_source_format": "csv",
            "quantified_subquery_filter_runtime_execution": "true",
            "quantified_subquery_order_by_runtime_execution": "true",
            "quantified_subquery_limit_runtime_execution": "true",
            "quantified_subquery_input_row_count": "3",
            "quantified_subquery_filtered_row_count": "2",
            "quantified_subquery_materialization_bound": "32",
            "quantified_subquery_materialized_value_count": "2",
            "quantified_subquery_materialized_null_value_count": "0",
            "quantified_subquery_null_semantics": "sql_all_three_valued_where_filter",
            "having_quantified_subquery_runtime_execution": "false",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "thresholds",
                "quantified-subquery-thresholds.csv",
                "threshold,active,score\n10,true,10\n20,true,20\n99,false,30\n",
            ),
        ),
    )


def sql_union_composition_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="sql_union_composition_semantics",
        source_name="union-left.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n",
        auxiliary_sources=(
            (
                "right",
                "union-right.csv",
                "id,label,amount\n2,beta,20\n4,delta,40\n5,epsilon,5\n",
            ),
        ),
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount >= 10 "
            "UNION SELECT id,label FROM '{right}' WHERE amount >= 10 "
            "ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        expected_fields={
            "sql_union_runtime_execution": "true",
            "sql_union_mode": "distinct",
            "sql_union_branch_count": "2",
            "sql_union_input_row_count": "5",
            "sql_union_distinct_input_row_count": "4",
            "sql_union_output_row_count": "4",
            "sql_union_order_by_runtime_execution": "true",
            "sort_keys": "id",
            "sort_direction": "asc",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def in_subquery_scalar_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="in_subquery_scalar_semantics",
        source_name="in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN (SELECT id FROM '{allowed}') LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "3",
            "in_list_null_value_count": "1",
            "in_subquery_runtime_execution": "true",
            "in_subquery_source_column": "id",
            "in_subquery_source_format": "csv",
            "in_subquery_materialized_value_count": "3",
            "in_subquery_materialized_null_value_count": "1",
            "in_predicate_null_semantics": "sql_three_valued_where_filter",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(("allowed", "in-subquery-allowed.csv", "id\n1\n3\nNULL\n"),),
    )


def in_subquery_filtered_ordered_limited_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="in_subquery_filtered_ordered_limited_semantics",
        source_name="in-subquery-filtered-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT id FROM '{allowed}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":3,"label":"gamma"}\n{"id":4,"label":"delta"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "2",
            "in_list_null_value_count": "0",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_order_by_runtime_execution": "true",
            "in_subquery_limit_runtime_execution": "true",
            "in_subquery_source_column": "id",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "4",
            "in_subquery_filtered_row_count": "3",
            "in_subquery_materialization_bound": "32",
            "in_subquery_materialized_value_count": "2",
            "in_subquery_materialized_null_value_count": "0",
            "in_predicate_null_semantics": "not_applicable",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "in-subquery-filtered-allowed.csv",
                "id,active,score\n1,true,10\n2,false,30\n3,true,20\n4,true,40\n",
            ),
        ),
    )


def correlated_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_in_subquery_semantics",
        source_name="correlated-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT id FROM '{allowed}' WHERE id = outer.id AND active IS TRUE "
            "AND outer.amount >= min_amount ORDER BY min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "correlated-in-subquery-allowed.csv",
                (
                    "id,min_amount,active\n"
                    "1,5,true\n"
                    "1,99,true\n"
                    "2,25,true\n"
                    "3,25,false\n"
                    "3,20,true\n"
                    "5,1,true\n"
                ),
            ),
        ),
    )


def source_qualified_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_in_subquery_semantics",
        source_name="source-qualified-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT allowed.id FROM '{allowed}' AS allowed "
            "WHERE allowed.id = outer.id AND allowed.active IS TRUE "
            "AND outer.amount >= allowed.min_amount "
            "ORDER BY allowed.min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_order_by_runtime_execution": "true",
            "in_subquery_source_column": "id",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "source-qualified-in-subquery-allowed.csv",
                (
                    "id,min_amount,active\n"
                    "1,5,true\n"
                    "1,99,true\n"
                    "2,25,true\n"
                    "3,25,false\n"
                    "3,20,true\n"
                    "5,1,true\n"
                ),
            ),
        ),
    )


def correlated_row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_row_value_in_subquery_semantics",
        source_name="correlated-row-value-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) IN ("
            "SELECT id,label FROM '{allowed}' WHERE id = outer.id AND active IS TRUE "
            "AND min_amount <= outer.amount ORDER BY min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "row_value_in_subquery",
            "row_value_in_predicate_runtime_execution": "true",
            "row_value_in_source_columns": "id,label",
            "row_value_in_column_groups": "id+label",
            "row_value_in_column_count": "2",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "correlated-row-value-in-subquery-allowed.csv",
                (
                    "id,label,min_amount,active\n"
                    "1,alpha,5,true\n"
                    "1,alpha,99,true\n"
                    "2,beta,25,true\n"
                    "3,gamma,25,false\n"
                    "3,gamma,20,true\n"
                    "5,epsilon,1,true\n"
                ),
            ),
        ),
    )


def correlated_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_exists_subquery_semantics",
        source_name="correlated-exists-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT * FROM '{allowed}' WHERE id = outer.id AND active IS TRUE "
            "AND min_amount <= outer.amount LIMIT 1"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "exists_subquery",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_filter_runtime_execution": "true",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "correlated-exists-subquery-allowed.csv",
                (
                    "id,min_amount,active\n"
                    "1,5,true\n"
                    "1,99,true\n"
                    "2,25,true\n"
                    "3,25,false\n"
                    "3,20,true\n"
                    "5,1,true\n"
                ),
            ),
        ),
    )


def correlated_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_quantified_subquery_semantics",
        source_name="correlated-quantified-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount > ALL ("
            "SELECT min_amount FROM '{thresholds}' WHERE id = outer.id "
            "ORDER BY min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "quantified_subquery",
            "quantified_subquery_runtime_execution": "true",
            "quantified_subquery_quantifier": "all",
            "quantified_subquery_comparison_operator": "gt",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "3",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "thresholds",
                "correlated-quantified-subquery-thresholds.csv",
                "id,min_amount\n1,5\n1,9\n2,25\n3,20\n3,29\n5,1\n",
            ),
        ),
    )


def joined_projected_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_in_subquery_semantics",
        source_name="joined-projected-in-source.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT a.id FROM '{source}' AS s INNER JOIN '{allowed}' AS a "
            "ON s.id = a.id WHERE a.active IS TRUE ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_subquery_runtime_execution": "true",
            "in_subquery_source_column": "a.id",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "4",
            "in_subquery_filtered_row_count": "2",
            "in_subquery_materialized_value_count": "2",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_group_by_runtime_execution": "false",
            "projected_subquery_having_runtime_execution": "false",
            "projected_subquery_output_column_count": "1",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "joined-projected-in-allowed.csv",
                "id,active,score\n1,true,30\n2,false,20\n3,true,40\n5,true,50\n",
            ),
        ),
    )


def joined_projected_row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_row_value_in_subquery_semantics",
        source_name="joined-projected-row-value-in-source.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) IN ("
            "SELECT s.id,s.label FROM '{source}' AS s INNER JOIN '{allowed}' AS a "
            "ON s.id = a.id WHERE a.active IS TRUE ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "row_value_in_subquery",
            "row_value_in_predicate_runtime_execution": "true",
            "row_value_in_source_columns": "id,label",
            "row_value_in_column_groups": "id+label",
            "row_value_in_column_count": "2",
            "row_value_in_tuple_count": "2",
            "in_subquery_runtime_execution": "true",
            "in_subquery_source_column": "s.id,s.label",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "4",
            "in_subquery_filtered_row_count": "2",
            "in_subquery_materialized_value_count": "2",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_group_by_runtime_execution": "false",
            "projected_subquery_having_runtime_execution": "false",
            "projected_subquery_output_column_count": "2",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "joined-projected-row-value-in-allowed.csv",
                "id,active,score\n1,true,30\n2,false,20\n3,true,40\n5,true,50\n",
            ),
        ),
    )


def grouped_having_projected_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="grouped_having_projected_in_subquery_semantics",
        source_name="grouped-projected-in-source.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_subquery_runtime_execution": "true",
            "in_subquery_source_column": "id",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "6",
            "in_subquery_filtered_row_count": "2",
            "in_subquery_materialized_value_count": "2",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "false",
            "projected_subquery_group_by_runtime_execution": "true",
            "projected_subquery_having_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "grouped",
                "grouped-projected-in-values.csv",
                "id,amount\n1,10\n1,20\n2,5\n3,7\n3,9\n4,1\n",
            ),
        ),
    )


def joined_projected_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_exists_subquery_semantics",
        source_name="joined-projected-exists-source.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT c.id FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "exists_subquery",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_projection_kind": "column_list",
            "exists_subquery_source_column": "c.id",
            "exists_subquery_filter_runtime_execution": "true",
            "exists_subquery_order_by_runtime_execution": "true",
            "exists_subquery_limit_runtime_execution": "true",
            "exists_subquery_input_row_count": "4",
            "exists_subquery_filtered_row_count": "2",
            "exists_subquery_bounded_row_count": "2",
            "exists_subquery_result": "true",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "selected_row_count": "4",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "candidates",
                "joined-projected-exists-candidates.csv",
                "id,min_amount\n1,5\n2,25\n3,20\n5,1\n",
            ),
            (
                "allowed",
                "joined-projected-exists-allowed.csv",
                "id,active,score\n1,true,30\n2,false,20\n3,true,40\n5,false,50\n",
            ),
        ),
    )


def grouped_having_projected_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="grouped_having_projected_exists_subquery_semantics",
        source_name="grouped-projected-exists-source.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "exists_subquery",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_projection_kind": "column_list",
            "exists_subquery_source_column": "id",
            "exists_subquery_filter_runtime_execution": "false",
            "exists_subquery_order_by_runtime_execution": "true",
            "exists_subquery_limit_runtime_execution": "true",
            "exists_subquery_input_row_count": "6",
            "exists_subquery_filtered_row_count": "2",
            "exists_subquery_bounded_row_count": "2",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "false",
            "projected_subquery_group_by_runtime_execution": "true",
            "projected_subquery_having_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "selected_row_count": "4",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "grouped",
                "grouped-projected-exists-values.csv",
                "id,amount\n1,10\n1,20\n2,5\n3,7\n3,9\n4,1\n",
            ),
        ),
    )


def joined_projected_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_quantified_subquery_semantics",
        source_name="joined-projected-quantified-source.csv",
        source_text=(
            "id,label,amount\n"
            "1,alpha,8\n"
            "2,beta,15\n"
            "3,gamma,21\n"
            "4,delta,13\n"
            "5,epsilon,34\n"
        ),
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount > ALL ("
            "SELECT t.threshold FROM '{thresholds}' AS t INNER JOIN '{allowed}' AS a "
            "ON t.threshold_id = a.threshold_id WHERE a.enabled IS TRUE "
            "ORDER BY t.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":3,"label":"gamma"}\n{"id":5,"label":"epsilon"}\n',
        expected_fields={
            "predicate_operator_family": "quantified_subquery",
            "quantified_subquery_runtime_execution": "true",
            "quantified_subquery_quantifier": "all",
            "quantified_subquery_comparison_operator": "gt",
            "quantified_subquery_source_column": "t.threshold",
            "quantified_subquery_source_format": "csv",
            "quantified_subquery_filter_runtime_execution": "true",
            "quantified_subquery_order_by_runtime_execution": "true",
            "quantified_subquery_limit_runtime_execution": "true",
            "quantified_subquery_input_row_count": "3",
            "quantified_subquery_filtered_row_count": "2",
            "quantified_subquery_materialization_bound": "32",
            "quantified_subquery_materialized_value_count": "2",
            "quantified_subquery_materialized_null_value_count": "0",
            "quantified_subquery_null_semantics": "sql_all_three_valued_where_filter",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_group_by_runtime_execution": "false",
            "projected_subquery_having_runtime_execution": "false",
            "projected_subquery_output_column_count": "1",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "thresholds",
                "joined-projected-quantified-thresholds.csv",
                "threshold_id,threshold,score\n10,10,20\n20,20,30\n30,99,40\n",
            ),
            (
                "allowed",
                "joined-projected-quantified-allowed.csv",
                "threshold_id,enabled\n10,true\n20,true\n30,false\n",
            ),
        ),
    )


def correlated_joined_projected_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_in_subquery_semantics",
        source_name="correlated-joined-projected-in-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT c.id FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE AND c.id = outer.id "
            "AND c.min_amount <= outer.amount ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_source_column": "c.id",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "candidates",
                "correlated-joined-projected-in-candidates.csv",
                "id,min_amount\n1,5\n1,99\n2,25\n3,20\n5,1\n",
            ),
            (
                "allowed",
                "correlated-joined-projected-in-allowed.csv",
                "id,active,score\n1,true,30\n2,true,20\n3,true,40\n5,false,50\n",
            ),
        ),
    )


def correlated_joined_projected_row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_row_value_in_subquery_semantics",
        source_name="correlated-joined-projected-row-value-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) IN ("
            "SELECT c.id,c.label FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE AND c.id = outer.id "
            "AND c.min_amount <= outer.amount ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "row_value_in_subquery",
            "row_value_in_predicate_runtime_execution": "true",
            "row_value_in_source_columns": "id,label",
            "row_value_in_column_count": "2",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_source_column": "c.id,c.label",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_output_column_count": "2",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "candidates",
                "correlated-joined-projected-row-value-candidates.csv",
                "id,label,min_amount\n1,alpha,5\n1,alpha,99\n2,beta,25\n3,gamma,20\n5,epsilon,1\n",
            ),
            (
                "allowed",
                "correlated-joined-projected-row-value-allowed.csv",
                "id,active,score\n1,true,30\n2,true,20\n3,true,40\n5,false,50\n",
            ),
        ),
    )


def correlated_joined_projected_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_quantified_subquery_semantics",
        source_name="correlated-joined-projected-quantified-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount > ALL ("
            "SELECT t.threshold FROM '{thresholds}' AS t INNER JOIN '{allowed}' AS a "
            "ON t.threshold_id = a.threshold_id WHERE a.enabled IS TRUE AND t.id = outer.id "
            "ORDER BY t.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "quantified_subquery",
            "quantified_subquery_runtime_execution": "true",
            "quantified_subquery_filter_runtime_execution": "true",
            "quantified_subquery_quantifier": "all",
            "quantified_subquery_comparison_operator": "gt",
            "quantified_subquery_source_column": "t.threshold",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "id",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "3",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "thresholds",
                "correlated-joined-projected-quantified-thresholds.csv",
                (
                    "id,threshold_id,threshold,score\n"
                    "1,10,5,30\n"
                    "1,20,9,20\n"
                    "2,30,25,20\n"
                    "3,40,20,40\n"
                    "3,50,29,10\n"
                    "5,60,1,50\n"
                ),
            ),
            (
                "allowed",
                "correlated-joined-projected-quantified-allowed.csv",
                "threshold_id,enabled\n10,true\n20,true\n30,true\n40,true\n50,true\n60,false\n",
            ),
        ),
    )


def correlated_joined_projected_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_exists_subquery_semantics",
        source_name="correlated-joined-projected-exists-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT c.id FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE AND c.id = outer.id "
            "AND c.min_amount <= outer.amount ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "exists_subquery",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_projection_kind": "column_list",
            "exists_subquery_source_column": "c.id",
            "exists_subquery_filter_runtime_execution": "true",
            "exists_subquery_order_by_runtime_execution": "true",
            "exists_subquery_limit_runtime_execution": "true",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "candidates",
                "correlated-joined-projected-exists-candidates.csv",
                "id,min_amount\n1,5\n1,99\n2,25\n3,20\n5,1\n",
            ),
            (
                "allowed",
                "correlated-joined-projected-exists-allowed.csv",
                "id,active,score\n1,true,30\n2,true,20\n3,true,40\n5,false,50\n",
            ),
        ),
    )


CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT = (
    "id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n"
)

CORRELATED_GROUPED_PROJECTED_VALUES_TEXT = (
    "id,label,min_amount,threshold\n"
    "1,alpha,5,7\n"
    "1,alpha,9,8\n"
    "2,beta,25,18\n"
    "2,beta,30,21\n"
    "3,gamma,20,25\n"
    "3,gamma,29,26\n"
    "5,epsilon,1,2\n"
    "5,epsilon,2,3\n"
)


def correlated_grouped_having_projected_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_in_subquery_semantics",
        source_name="correlated-grouped-projected-in-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "AND id = outer.id AND min(min_amount) <= outer.amount "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "false",
            "in_subquery_source_column": "id",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "false",
            "projected_subquery_group_by_runtime_execution": "true",
            "projected_subquery_having_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-in-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def correlated_grouped_having_projected_row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_row_value_in_subquery_semantics",
        source_name="correlated-grouped-projected-row-value-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) IN ("
            "SELECT id,label FROM '{grouped}' GROUP BY id,label HAVING count(*) >= 2 "
            "AND id = outer.id AND min(min_amount) <= outer.amount "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "row_value_in_subquery",
            "row_value_in_predicate_runtime_execution": "true",
            "row_value_in_source_columns": "id,label",
            "row_value_in_column_count": "2",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "false",
            "in_subquery_source_column": "id,label",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "false",
            "projected_subquery_group_by_runtime_execution": "true",
            "projected_subquery_having_runtime_execution": "true",
            "projected_subquery_output_column_count": "2",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-row-value-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def correlated_grouped_having_projected_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_quantified_subquery_semantics",
        source_name="correlated-grouped-projected-quantified-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount > ALL ("
            "SELECT threshold FROM '{grouped}' GROUP BY threshold "
            "HAVING min(id) = outer.id AND count(*) >= 1 "
            "ORDER BY threshold ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        expected_fields={
            "predicate_operator_family": "quantified_subquery",
            "quantified_subquery_runtime_execution": "true",
            "quantified_subquery_filter_runtime_execution": "false",
            "quantified_subquery_quantifier": "all",
            "quantified_subquery_comparison_operator": "gt",
            "quantified_subquery_source_column": "threshold",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "false",
            "projected_subquery_group_by_runtime_execution": "true",
            "projected_subquery_having_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "id",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "3",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-quantified-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def correlated_grouped_having_projected_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_exists_subquery_semantics",
        source_name="correlated-grouped-projected-exists-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "AND id = outer.id AND min(min_amount) <= outer.amount "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "exists_subquery",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_projection_kind": "column_list",
            "exists_subquery_source_column": "id",
            "exists_subquery_filter_runtime_execution": "false",
            "projected_subquery_runtime_execution": "true",
            "projected_subquery_join_runtime_execution": "false",
            "projected_subquery_group_by_runtime_execution": "true",
            "projected_subquery_having_runtime_execution": "true",
            "projected_subquery_output_column_count": "1",
            "correlated_subquery_runtime_execution": "true",
            "correlated_subquery_outer_alias": "outer",
            "correlated_subquery_outer_column": "amount,id",
            "correlated_subquery_evaluation_strategy": "per_outer_row_bounded_subquery_materialization",
            "correlated_subquery_outer_row_evaluation_count": "4",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-exists-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def nested_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="nested_in_subquery_semantics",
        source_name="nested-in-subquery-source.csv",
        source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT allowed_id FROM '{allowed}' WHERE allowed_id IN ("
            "SELECT id FROM '{nested}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2"
            ") ORDER BY priority DESC LIMIT 3"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        expected_fields={
            "predicate_operator_family": "in_subquery",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "2",
            "in_list_null_value_count": "0",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_order_by_runtime_execution": "true",
            "in_subquery_limit_runtime_execution": "true",
            "in_subquery_source_column": "allowed_id",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "4",
            "in_subquery_filtered_row_count": "2",
            "in_subquery_materialization_bound": "32",
            "in_subquery_materialized_value_count": "2",
            "in_subquery_materialized_null_value_count": "0",
            "nested_subquery_runtime_execution": "true",
            "nested_subquery_predicate_count": "1",
            "nested_subquery_max_depth": "1",
            "nested_subquery_materialization_order": "inner_first_depth_first",
            "in_predicate_null_semantics": "not_applicable",
            "selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "nested-in-subquery-allowed.csv",
                "allowed_id,priority\n1,10\n2,30\n3,20\n5,40\n",
            ),
            (
                "nested",
                "nested-in-subquery-nested.csv",
                "id,active,score\n1,true,20\n2,true,10\n3,true,40\n4,false,50\n",
            ),
        ),
    )


def having_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_in_subquery_semantics",
        source_name="having-in-subquery-source.csv",
        source_text=(
            "region,id,amount\n"
            "east,1,10\n"
            "east,2,13\n"
            "west,3,20\n"
            "north,4,12\n"
            "north,5,15\n"
            "north,6,18\n"
        ),
        statement_template=(
            "SELECT region,count(*) AS rows,sum(amount) AS total FROM '{source}' "
            "GROUP BY region HAVING rows IN ("
            "SELECT rows FROM '{allowed}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2"
            ") ORDER BY total DESC LIMIT 10"
        ),
        expected_jsonl=(
            '{"region":"north","rows":3,"total":45}\n'
            '{"region":"east","rows":2,"total":23}\n'
        ),
        expected_fields={
            "aggregate_runtime_execution": "true",
            "aggregate_operator_family": "grouped_aggregate",
            "group_by_runtime_execution": "true",
            "having_runtime_execution": "true",
            "having_operator_family": "in_subquery",
            "having_source_column": "rows",
            "having_in_subquery_runtime_execution": "true",
            "in_predicate_runtime_execution": "true",
            "in_list_value_count": "2",
            "in_list_null_value_count": "0",
            "in_subquery_runtime_execution": "true",
            "in_subquery_filter_runtime_execution": "true",
            "in_subquery_order_by_runtime_execution": "true",
            "in_subquery_limit_runtime_execution": "true",
            "in_subquery_source_column": "rows",
            "in_subquery_source_format": "csv",
            "in_subquery_input_row_count": "3",
            "in_subquery_filtered_row_count": "2",
            "in_subquery_materialization_bound": "32",
            "in_subquery_materialized_value_count": "2",
            "in_subquery_materialized_null_value_count": "0",
            "having_input_row_count": "3",
            "having_selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "having-in-subquery-allowed.csv",
                "rows,active,score\n2,true,10\n3,true,20\n1,false,30\n",
            ),
        ),
    )


def having_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_exists_subquery_semantics",
        source_name="having-exists-subquery-source.csv",
        source_text=(
            "region,id,amount\n"
            "east,1,10\n"
            "east,2,13\n"
            "west,3,20\n"
            "north,4,12\n"
            "north,5,15\n"
            "north,6,18\n"
        ),
        statement_template=(
            "SELECT region,count(*) AS rows,sum(amount) AS total FROM '{source}' "
            "GROUP BY region HAVING EXISTS ("
            "SELECT * FROM '{allowed}' WHERE active IS TRUE ORDER BY score DESC LIMIT 1"
            ") ORDER BY total DESC LIMIT 10"
        ),
        expected_jsonl=(
            '{"region":"north","rows":3,"total":45}\n'
            '{"region":"east","rows":2,"total":23}\n'
            '{"region":"west","rows":1,"total":20}\n'
        ),
        expected_fields={
            "aggregate_runtime_execution": "true",
            "aggregate_operator_family": "grouped_aggregate",
            "group_by_runtime_execution": "true",
            "having_runtime_execution": "true",
            "having_operator_family": "exists_subquery",
            "having_exists_subquery_runtime_execution": "true",
            "exists_subquery_runtime_execution": "true",
            "exists_subquery_projection_kind": "wildcard",
            "exists_subquery_source_format": "csv",
            "exists_subquery_filter_runtime_execution": "true",
            "exists_subquery_order_by_runtime_execution": "true",
            "exists_subquery_limit_runtime_execution": "true",
            "exists_subquery_input_row_count": "3",
            "exists_subquery_filtered_row_count": "2",
            "exists_subquery_bounded_row_count": "1",
            "exists_subquery_result": "true",
            "exists_subquery_null_semantics": "sql_exists_two_valued_presence_test",
            "having_input_row_count": "3",
            "having_selected_row_count": "3",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "allowed",
                "having-exists-subquery-allowed.csv",
                "active,score\nfalse,10\ntrue,30\ntrue,20\n",
            ),
        ),
    )


def having_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_quantified_subquery_semantics",
        source_name="having-quantified-subquery-source.csv",
        source_text=(
            "region,id,amount\n"
            "east,1,10\n"
            "east,2,13\n"
            "west,3,20\n"
            "north,4,12\n"
            "north,5,15\n"
            "north,6,18\n"
        ),
        statement_template=(
            "SELECT region,count(*) AS rows,sum(amount) AS total FROM '{source}' "
            "GROUP BY region HAVING total > ALL ("
            "SELECT threshold FROM '{thresholds}' WHERE active IS TRUE "
            "ORDER BY score DESC LIMIT 2"
            ") ORDER BY total DESC LIMIT 10"
        ),
        expected_jsonl=(
            '{"region":"north","rows":3,"total":45}\n'
            '{"region":"east","rows":2,"total":23}\n'
        ),
        expected_fields={
            "aggregate_runtime_execution": "true",
            "aggregate_operator_family": "grouped_aggregate",
            "group_by_runtime_execution": "true",
            "having_runtime_execution": "true",
            "having_operator_family": "quantified_subquery",
            "having_source_column": "total",
            "having_quantified_subquery_runtime_execution": "true",
            "quantified_subquery_runtime_execution": "true",
            "quantified_subquery_quantifier": "all",
            "quantified_subquery_comparison_operator": "gt",
            "quantified_subquery_source_column": "threshold",
            "quantified_subquery_source_format": "csv",
            "quantified_subquery_filter_runtime_execution": "true",
            "quantified_subquery_order_by_runtime_execution": "true",
            "quantified_subquery_limit_runtime_execution": "true",
            "quantified_subquery_input_row_count": "3",
            "quantified_subquery_filtered_row_count": "2",
            "quantified_subquery_materialization_bound": "32",
            "quantified_subquery_materialized_value_count": "2",
            "quantified_subquery_materialized_null_value_count": "0",
            "quantified_subquery_null_semantics": "sql_all_three_valued_where_filter",
            "having_input_row_count": "3",
            "having_selected_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "thresholds",
                "having-quantified-subquery-thresholds.csv",
                "threshold,active,score\n20,true,10\n22,true,20\n99,false,30\n",
            ),
        ),
    )


def distinct_count_grouped_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="distinct_count_grouped",
        source_name="count-distinct.csv",
        source_text=(
            "id,region,customer_id,amount\n"
            "1,east,c1,10\n"
            "2,east,c1,12\n"
            "3,east,c2,14\n"
            "4,east,,16\n"
            "5,west,c3,7\n"
            "6,west,c4,8\n"
            "7,west,c3,9\n"
        ),
        statement_template=(
            "SELECT region,count(DISTINCT customer_id) AS unique_customers,count(*) AS rows "
            "FROM '{source}' WHERE amount >= 8 GROUP BY region LIMIT 10"
        ),
        expected_jsonl=(
            '{"region":"east","unique_customers":2,"rows":4}\n'
            '{"region":"west","unique_customers":2,"rows":2}\n'
        ),
        expected_fields={
            "aggregate_runtime_execution": "true",
            "aggregate_operator_family": "grouped_aggregate",
            "aggregate_functions": "count(DISTINCT customer_id),count(*)",
            "distinct_aggregate_runtime_execution": "true",
            "distinct_aggregate_function": "count(DISTINCT customer_id)",
            "distinct_aggregate_column": "customer_id",
            "distinct_aggregate_null_semantics": "sql_count_distinct_ignores_nulls",
            "group_by_runtime_execution": "true",
            "group_by_group_count": "2",
            "projected_columns": "region,unique_customers,rows",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def select_distinct_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="select_distinct_projection",
        source_name="select-distinct.csv",
        source_text=(
            "id,region,label,amount\n"
            "1,east,alpha,10\n"
            "2,east,alpha,12\n"
            "3,west,beta,8\n"
            "4,west,beta,14\n"
            "5,north,gamma,20\n"
        ),
        statement_template=(
            "SELECT DISTINCT region,label FROM '{source}' "
            "WHERE amount >= 8 ORDER BY region,label LIMIT 2"
        ),
        expected_jsonl=(
            '{"region":"east","label":"alpha"}\n'
            '{"region":"north","label":"gamma"}\n'
        ),
        expected_fields={
            "distinct_projection_runtime_execution": "true",
            "distinct_projection_output_columns": "region,label",
            "distinct_projection_input_row_count": "5",
            "distinct_projection_output_row_count": "2",
            "distinct_projection_limit_applied_after_deduplication": "true",
            "distinct_projection_null_semantics": "sql_select_distinct_groups_nulls",
            "projected_columns": "region,label",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def select_distinct_aggregate_having_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="select_distinct_aggregate_having",
        source_name="select-distinct-aggregate.csv",
        source_text=(
            "id,region,amount\n"
            "1,east,10\n"
            "2,east,12\n"
            "3,west,8\n"
            "4,west,14\n"
            "5,north,3\n"
        ),
        statement_template=(
            "SELECT DISTINCT region,count(*) AS rows FROM '{source}' "
            "GROUP BY region HAVING count(*) >= 2 LIMIT 5"
        ),
        expected_jsonl=(
            '{"region":"east","rows":2}\n'
            '{"region":"west","rows":2}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_distinct_group_by_aggregate_limit_having",
            "aggregate_runtime_execution": "true",
            "group_by_runtime_execution": "true",
            "having_runtime_execution": "true",
            "distinct_projection_runtime_execution": "true",
            "distinct_projection_output_columns": "region,rows",
            "distinct_projection_input_row_count": "2",
            "distinct_projection_output_row_count": "2",
            "distinct_projection_limit_applied_after_deduplication": "true",
            "projected_columns": "region,rows",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def having_hidden_aggregate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_hidden_aggregate_expression",
        source_name="having-hidden.csv",
        source_text="id,region,amount\n1,east,10\n2,west,5\n3,east,12\n4,west,14\n5,north,3\n",
        statement_template=(
            "SELECT region,count(*) AS rows FROM '{source}' WHERE amount >= 0 GROUP BY region "
            "HAVING sum(amount) >= 10 AND count(*) >= 2 AND count(DISTINCT id) >= 2 "
            "ORDER BY rows DESC LIMIT 10"
        ),
        expected_jsonl='{"region":"east","rows":2}\n{"region":"west","rows":2}\n',
        expected_fields={
            "having_runtime_execution": "true",
            "having_operator_family": "logical_predicate",
            "having_source_column": "sum(amount),count(*),count(DISTINCT id)",
            "having_aggregate_runtime_execution": "true",
            "having_aggregate_function": "sum(amount),count(*),count(DISTINCT id)",
            "having_aggregate_output_column": "__having_sum_amount_1,__having_count_all_2,__having_count_distinct_id_3",
            "output_row_count": "2",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def window_mixed_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="window_rank_offset_distribution",
        source_name="windows.csv",
        source_text=(
            "id,region,amount\n"
            "1,east,30\n"
            "2,east,20\n"
            "3,east,10\n"
            "4,west,15\n"
            "5,west,5\n"
        ),
        statement_template=(
            "SELECT id,region,amount,"
            "ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,"
            "RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS r,"
            "LAG(amount) OVER (PARTITION BY region ORDER BY amount DESC) AS previous_amount,"
            "NTILE(2) OVER (PARTITION BY region ORDER BY amount DESC) AS bucket "
            "FROM '{source}' LIMIT 5"
        ),
        expected_jsonl=(
            '{"id":1,"region":"east","amount":30,"rn":1,"r":1,"previous_amount":null,"bucket":1}\n'
            '{"id":2,"region":"east","amount":20,"rn":2,"r":2,"previous_amount":30,"bucket":1}\n'
            '{"id":3,"region":"east","amount":10,"rn":3,"r":3,"previous_amount":20,"bucket":2}\n'
            '{"id":4,"region":"west","amount":15,"rn":1,"r":1,"previous_amount":null,"bucket":1}\n'
            '{"id":5,"region":"west","amount":5,"rn":2,"r":2,"previous_amount":15,"bucket":2}\n'
        ),
        expected_fields={
            "window_runtime_execution": "true",
            "window_operator_family": "mixed",
            "window_function": "row_number,rank,lag,ntile",
            "window_partition_columns": "region;region;region;region",
            "window_order_by_columns": "amount;amount;amount;amount",
            "window_order_by_directions": "desc;desc;desc;desc",
            "window_output_columns": "rn,r,previous_amount,bucket",
            "window_row_number_runtime_execution": "true",
            "window_rank_runtime_execution": "true",
            "window_lag_runtime_execution": "true",
            "window_ntile_runtime_execution": "true",
            "projected_columns": "id,region,amount,rn,r,previous_amount,bucket",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def select_distinct_window_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="select_distinct_window",
        source_name="select-distinct-window.csv",
        source_text=(
            "id,region,amount\n"
            "1,east,10\n"
            "2,east,10\n"
            "3,east,5\n"
            "4,west,7\n"
            "5,west,7\n"
        ),
        statement_template=(
            "SELECT DISTINCT region,RANK() OVER "
            "(PARTITION BY region ORDER BY amount DESC) AS r "
            "FROM '{source}' LIMIT 2"
        ),
        expected_jsonl=(
            '{"region":"east","r":1}\n'
            '{"region":"east","r":3}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_distinct_window_limit",
            "window_runtime_execution": "true",
            "window_rank_runtime_execution": "true",
            "distinct_projection_runtime_execution": "true",
            "distinct_projection_output_columns": "region,r",
            "distinct_projection_input_row_count": "5",
            "distinct_projection_output_row_count": "2",
            "distinct_projection_limit_applied_after_deduplication": "true",
            "projected_columns": "region,r",
            "claim_gate_status": "fixture_smoke_only",
        },
    )


def join_multi_key_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="join_multi_key_expression_condition",
        source_name="join-fact.csv",
        source_text=(
            "id,customer_id,region,amount\n"
            "1,10,east,8\n"
            "2,20,west,15\n"
            "3,20,east,21\n"
            "4,30,east,22\n"
            "5,30,west,23\n"
        ),
        statement_template=(
            "SELECT f.id,d.segment FROM '{source}' AS f INNER JOIN '{dim}' AS d "
            "ON f.customer_id = d.customer_id AND f.region = d.region "
            "WHERE f.amount >= 10 LIMIT 10"
        ),
        expected_jsonl=(
            '{"f.id":2,"d.segment":"enterprise"}\n'
            '{"f.id":3,"d.segment":"consumer"}\n'
            '{"f.id":5,"d.segment":"startup"}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_inner_equi_join_filter_limit",
            "join_runtime_execution": "true",
            "join_type": "inner_equi",
            "join_left_key": "f.customer_id,f.region",
            "join_right_key": "d.customer_id,d.region",
            "join_key_arity": "2",
            "join_multi_key_runtime_execution": "true",
            "join_matched_row_count": "3",
            "join_rows_output": "3",
            "projected_columns": "f.id,d.segment",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "dim",
                "join-dim.csv",
                "customer_id,region,segment\n20,west,enterprise\n20,east,consumer\n30,west,startup\n99,east,orphan\n",
            ),
        ),
    )


def join_scalar_expression_condition_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="join_scalar_expression_condition",
        source_name="join-expression-fact.csv",
        source_text="id,amount\n1,8\n2,15\n3,21\n",
        statement_template=(
            "SELECT f.id,d.segment FROM '{source}' AS f INNER JOIN '{dim}' AS d "
            "ON f.amount + d.discount >= 25 LIMIT 10"
        ),
        expected_jsonl=(
            '{"f.id":2,"d.segment":"large"}\n'
            '{"f.id":3,"d.segment":"small"}\n'
            '{"f.id":3,"d.segment":"large"}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_inner_expression_join_limit",
            "join_runtime_execution": "true",
            "join_type": "inner_expression",
            "join_on_predicate_runtime_execution": "true",
            "join_on_predicate_operator_family": "generic_expression",
            "join_on_predicate_source_column": "d.discount,f.amount",
            "join_key_arity": "0",
            "join_candidate_row_count": "6",
            "join_matched_row_count": "3",
            "join_rows_output": "3",
            "projected_columns": "f.id,d.segment",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "dim",
                "join-expression-dim.csv",
                "segment,discount\nsmall,4\nlarge,10\n",
            ),
        ),
    )


def select_distinct_join_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="select_distinct_join",
        source_name="select-distinct-join-fact.csv",
        source_text=(
            "id,customer_id,region,amount\n"
            "1,10,east,5\n"
            "2,10,east,7\n"
            "3,20,west,9\n"
        ),
        statement_template=(
            "SELECT DISTINCT f.region,d.segment FROM '{source}' AS f "
            "INNER JOIN '{dim}' AS d ON f.customer_id = d.customer_id LIMIT 2"
        ),
        expected_jsonl=(
            '{"f.region":"east","d.segment":"retail"}\n'
            '{"f.region":"west","d.segment":"enterprise"}\n'
        ),
        expected_fields={
            "sql_statement_kind": "local_source_inner_equi_join_distinct_limit",
            "join_runtime_execution": "true",
            "distinct_projection_runtime_execution": "true",
            "distinct_projection_output_columns": "f.region,d.segment",
            "distinct_projection_input_row_count": "3",
            "distinct_projection_output_row_count": "2",
            "distinct_projection_limit_applied_after_deduplication": "true",
            "projected_columns": "f.region,d.segment",
            "claim_gate_status": "fixture_smoke_only",
        },
        auxiliary_sources=(
            (
                "dim",
                "select-distinct-join-dim.csv",
                "customer_id,segment\n10,retail\n20,enterprise\n",
            ),
        ),
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
            case_id="regex_predicate_utf8",
            source_name="regex-predicate.csv",
            source_text="id,label\n1,alpha\n2,beta\n3,gamma\n4,\n",
            statement_template=(
                "SELECT id,label,REGEXP_LIKE(label, '^a') AS starts_with_a "
                "FROM '{source}' WHERE label RLIKE '^(alpha|gamma)$' LIMIT 10"
            ),
            expected_jsonl=(
                '{"id":1,"label":"alpha","starts_with_a":true}\n'
                '{"id":3,"label":"gamma","starts_with_a":false}\n'
            ),
            expected_fields={
                "predicate_operator_family": "string_predicate",
                "string_predicate_runtime_execution": "true",
                "string_predicate_operator": "regex_match",
                "predicate_projection_runtime_execution": "true",
                "predicate_projection_predicate_family": "string_predicate",
                "predicate_projection_source_column": "label",
                "projected_columns": "id,label,starts_with_a",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        SqlFixtureCase(
            case_id="like_predicate_utf8",
            source_name="like-predicate.csv",
            source_text="id,label\n1,alpha\n2,beta\n3,alpine\n4,\n5,delta\n",
            statement_template=(
                "SELECT id,label FROM '{source}' WHERE label LIKE '_l%' LIMIT 10"
            ),
            expected_jsonl=(
                '{"id":1,"label":"alpha"}\n'
                '{"id":3,"label":"alpine"}\n'
            ),
            expected_fields={
                "predicate_operator_family": "string_predicate",
                "string_predicate_runtime_execution": "true",
                "string_predicate_operator": "like_pattern",
                "projected_columns": "id,label",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        SqlFixtureCase(
            case_id="like_escape_predicate_utf8",
            source_name="like-escape-predicate.csv",
            source_text="id,label\n1,alpha\n2,al_pha\n3,al%pha\n4,alxpha\n5,\n",
            statement_template=(
                "SELECT id,label FROM '{source}' WHERE label LIKE 'al!_%' ESCAPE '!' LIMIT 10"
            ),
            expected_jsonl='{"id":2,"label":"al_pha"}\n',
            expected_fields={
                "predicate_operator_family": "string_predicate",
                "string_predicate_runtime_execution": "true",
                "string_predicate_operator": "like_pattern",
                "string_predicate_like_escape_runtime_execution": "true",
                "string_predicate_like_escape_character": "!",
                "projected_columns": "id,label",
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
        SqlFixtureCase(
            case_id="subquery_predicate_projection_semantics",
            source_name="subquery-predicate-projection-source.csv",
            source_text=(
                "id,label,amount\n"
                "1,alpha,10\n"
                "2,beta,20\n"
                "3,gamma,30\n"
                "4,delta,40\n"
            ),
            statement_template=(
                "SELECT id,"
                "id IN (SELECT id FROM '{allowed}' WHERE id = outer.id "
                "AND active IS TRUE AND outer.amount >= min_amount "
                "ORDER BY min_amount ASC LIMIT 10) AS matched,"
                "CASE WHEN id IN (SELECT id FROM '{allowed}' WHERE id = outer.id "
                "AND active IS TRUE AND outer.amount >= min_amount "
                "ORDER BY min_amount ASC LIMIT 10) THEN 'allowed' ELSE 'blocked' END AS status "
                "FROM '{source}' ORDER BY id ASC LIMIT 4"
            ),
            expected_jsonl=(
                '{"id":1,"matched":true,"status":"allowed"}\n'
                '{"id":2,"matched":false,"status":"blocked"}\n'
                '{"id":3,"matched":true,"status":"allowed"}\n'
                '{"id":4,"matched":false,"status":"blocked"}\n'
            ),
            expected_fields={
                "predicate_operator_family": "none",
                "predicate_projection_runtime_execution": "true",
                "predicate_projection_predicate_family": "in_subquery",
                "predicate_projection_source_column": "amount+id",
                "conditional_projection_runtime_execution": "true",
                "conditional_projection_predicate_family": "in_subquery",
                "conditional_projection_source_column": "amount+id",
                "in_subquery_runtime_execution": "true",
                "in_subquery_source_column": "id,id",
                "in_subquery_filter_runtime_execution": "true",
                "in_subquery_order_by_runtime_execution": "true",
                "in_subquery_limit_runtime_execution": "true",
                "correlated_subquery_runtime_execution": "true",
                "correlated_subquery_outer_alias": "outer",
                "correlated_subquery_outer_column": "amount,id",
                "correlated_subquery_outer_row_evaluation_count": "4",
                "fallback_attempted": "false",
                "external_engine_invoked": "false",
                "claim_gate_status": "fixture_smoke_only",
            },
            auxiliary_sources=(
                (
                    "allowed",
                    "subquery-predicate-projection-allowed.csv",
                    (
                        "id,min_amount,active\n"
                        "1,5,true\n"
                        "1,99,true\n"
                        "2,25,true\n"
                        "3,25,false\n"
                        "3,20,true\n"
                        "5,1,true\n"
                    ),
                ),
            ),
        ),
        SqlFixtureCase(
            case_id="aggregate_having_output_rows",
            source_name="aggregate-having.csv",
            source_text=(
                "region,amount\n"
                "east,10\n"
                "east,12\n"
                "west,9\n"
                "west,10\n"
                "central,3\n"
            ),
            statement_template=(
                "SELECT region,count(*) AS rows,sum(amount) AS total_amount "
                "FROM '{source}' WHERE amount >= 0 GROUP BY region "
                "HAVING total_amount >= 10 AND rows >= 2 "
                "ORDER BY total_amount DESC LIMIT 10"
            ),
            expected_jsonl=(
                '{"region":"east","rows":2,"total_amount":22}\n'
                '{"region":"west","rows":2,"total_amount":19}\n'
            ),
            expected_fields={
                "aggregate_runtime_execution": "true",
                "aggregate_operator_family": "grouped_aggregate",
                "group_by_runtime_execution": "true",
                "having_runtime_execution": "true",
                "having_operator_family": "logical_predicate",
                "having_source_column": "total_amount,rows",
                "having_input_row_count": "3",
                "having_selected_row_count": "2",
                "claim_gate_status": "fixture_smoke_only",
            },
        ),
        string_function_composition_case(),
        temporal_arithmetic_difference_case(),
        interval_literal_temporal_arithmetic_case(),
        timestamp_offset_literal_normalization_case(),
        conditional_projection_case(),
        binary_hex_literal_projection_case(),
        binary_text_literal_projection_case(),
        complex_array_literal_projection_case(),
        complex_struct_source_projection_case(),
        complex_distinct_projection_equality_case(),
        complex_order_by_projection_case(),
        sql_union_complex_distinct_equality_case(),
        sql_union_complex_ordering_case(),
        binary_cast_projection_predicate_case(),
        binary_cast_ordering_predicate_case(),
        decimal_cast_projection_predicate_case(),
        decimal_arithmetic_projection_case(),
        binary_helper_projection_case(),
        in_predicate_literal_null_case(),
        row_value_in_predicate_case(),
        row_value_in_subquery_case(),
        exists_subquery_case(),
        quantified_subquery_case(),
        sql_union_composition_case(),
        in_subquery_scalar_case(),
        in_subquery_filtered_ordered_limited_case(),
        correlated_in_subquery_case(),
        source_qualified_in_subquery_case(),
        correlated_row_value_in_subquery_case(),
        correlated_exists_subquery_case(),
        correlated_quantified_subquery_case(),
        joined_projected_in_subquery_case(),
        joined_projected_row_value_in_subquery_case(),
        grouped_having_projected_in_subquery_case(),
        joined_projected_exists_subquery_case(),
        grouped_having_projected_exists_subquery_case(),
        joined_projected_quantified_subquery_case(),
        correlated_joined_projected_in_subquery_case(),
        correlated_joined_projected_row_value_in_subquery_case(),
        correlated_joined_projected_quantified_subquery_case(),
        correlated_joined_projected_exists_subquery_case(),
        correlated_grouped_having_projected_in_subquery_case(),
        correlated_grouped_having_projected_row_value_in_subquery_case(),
        correlated_grouped_having_projected_quantified_subquery_case(),
        correlated_grouped_having_projected_exists_subquery_case(),
        nested_in_subquery_case(),
        having_in_subquery_case(),
        having_exists_subquery_case(),
        having_quantified_subquery_case(),
        distinct_count_grouped_case(),
        select_distinct_projection_case(),
        select_distinct_aggregate_having_case(),
        having_hidden_aggregate_case(),
        window_mixed_case(),
        select_distinct_window_case(),
        join_multi_key_case(),
        join_scalar_expression_condition_case(),
        select_distinct_join_case(),
    ]


def unsupported_cases() -> list[UnsupportedCase]:
    return [
        UnsupportedCase(
            case_id="runtime_error_numeric_division_by_zero",
            source_name="numeric-unsupported.csv",
            source_text="id,amount\n1,8\n",
            statement_template="SELECT id,amount / 0 AS broken FROM '{source}' LIMIT 10",
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="numeric arithmetic projection division by zero is a runtime data error",
            support_state="runtime_error_diagnostic",
            oracle_boundary="deterministic_runtime_error_diagnostic",
            stage_kind="runtime_error_diagnostic",
        ),
        UnsupportedCase(
            case_id="unsupported_timezone_database_policy",
            source_name="timezone-db-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id,TIMESTAMP '2026-05-19T12:34:56Z' AT TIME ZONE "
                "'America/Chicago' AS unsupported FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="timezone database semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_timezone_database_function_policy",
            source_name="timezone-db-function-unsupported.csv",
            source_text="id,event_ts\n1,2026-05-19T17:34:56Z\n",
            statement_template=(
                "SELECT id,TIMEZONE('America/Chicago', event_ts) AS unsupported "
                "FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="timezone database semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_timestamptz_policy",
            source_name="timestamptz-unsupported.csv",
            source_text="id,event_ts\n1,2026-05-19T17:34:56Z\n",
            statement_template=(
                "SELECT id,CAST(event_ts AS timestamptz) AS unsupported "
                "FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="timezone database semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_locale_collation",
            source_name="collation-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id,label COLLATE nocase AS folded FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_locale_case_insensitive_predicate",
            source_name="locale-casefold-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE label ILIKE 'a%' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_nonbinary_source_binary_literal_predicate",
            source_name="binary-literal-predicate-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE label = X'616c706861' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="comparison operands are not admitted together: utf8 and binary",
        ),
        UnsupportedCase(
            case_id="unsupported_nonbinary_source_binary_ordering_predicate",
            source_name="binary-source-ordering-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE label > BINARY 'alpha' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="comparison operands are not admitted together: utf8 and binary",
        ),
        UnsupportedCase(
            case_id="unsupported_list_array_access_cast",
            source_name="list-array-unsupported.csv",
            source_text="id,payload\n1,alpha\n",
            statement_template=(
                "SELECT id,LIST_EXTRACT(payload, 1) AS item FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="list and array accessors, function constructors, casts, and equality semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_struct_access_cast",
            source_name="struct-unsupported.csv",
            source_text="id,label,amount\n1,alpha,8\n",
            statement_template=(
                "SELECT id,ROW(label, amount) AS payload FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="row constructors plus struct casts, equality, and access semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_complex_subquery_membership",
            source_name="complex-subquery-membership-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE id IN "
                "(SELECT ARRAY[1] AS value_list FROM '{source}') LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="projected subqueries do not admit ARRAY or STRUCT projection outputs for membership materialization",
        ),
        UnsupportedCase(
            case_id="unsupported_variant_access",
            source_name="variant-unsupported.csv",
            source_text="id,payload\n1,alpha\n",
            statement_template=(
                "SELECT id,VARIANT_GET(payload, 'field') AS field FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="variant access semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_union_dtype_cast",
            source_name="union-dtype-unsupported.csv",
            source_text="id,payload\n1,alpha\n",
            statement_template="SELECT CAST(payload AS union) AS payload FROM '{source}' LIMIT 10",
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="union dtype casts are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_arbitrary_interval_arithmetic",
            source_name="arbitrary-interval-arithmetic-unsupported.csv",
            source_text=(
                "id,event_date,event_ts,interval\n"
                "1,2026-05-19,2026-05-19T12:34:45Z,1\n"
            ),
            statement_template=(
                "SELECT id,event_date + INTERVAL '1' DAY AS next_day "
                "FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="arbitrary ANSI INTERVAL arithmetic is not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_complex_join_key",
            source_name="complex-join-key-fact.csv",
            source_text="id,customer_id\n1,10\n",
            statement_template=(
                "SELECT f.id,d.segment FROM '{source}' AS f "
                "JOIN '{source}' AS d ON ARRAY[f.customer_id] = ARRAY[d.customer_id] LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="JOIN ON complex key expressions are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_join_or_predicate",
            source_name="join-or-predicate-fact.csv",
            source_text="id,customer_id,region\n1,10,east\n",
            statement_template=(
                "SELECT f.id,d.id FROM '{source}' AS f JOIN '{source}' AS d "
                "ON f.customer_id = d.customer_id OR f.region = d.region LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="JOIN ON OR predicates are not admitted",
        ),
        UnsupportedCase(
            case_id="invalid_shape_scalar_multi_column_in_subquery",
            source_name="scalar-multi-column-subquery-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE id IN "
                "(SELECT id,label FROM '{source}') LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="multi-column IN subqueries require row-value source columns",
            support_state="invalid_shape_diagnostic",
            oracle_boundary="deterministic_invalid_shape_diagnostic",
            stage_kind="invalid_shape_diagnostic",
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
            "remaining_matrix_gaps": sorted(expected_case_ids),
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
            "deterministic_runtime_error_diagnostic",
            "deterministic_invalid_shape_diagnostic",
        }:
            blockers.append(f"{row_id}: invalid oracle_boundary={row.get('oracle_boundary')}")
        if row.get("support_state") in {
            "unsupported_diagnostic",
            "runtime_error_diagnostic",
            "invalid_shape_diagnostic",
        } and row.get("unsupported_diagnostic_code") == "not_applicable_executable":
            blockers.append(f"{row_id}: diagnostic rows must name a diagnostic code")
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
    format_paths: dict[str, Path] = {"source": source_path}
    auxiliary_refs: list[Path] = []
    for placeholder, source_name, source_text in case.auxiliary_sources:
        auxiliary_path = materialize_source(work_dir, case.case_id, source_name, source_text)
        format_paths[placeholder] = auxiliary_path
        auxiliary_refs.append(auxiliary_path)
    statement = case.statement_template.format(**format_paths)
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
        "auxiliary_source_refs": [rel(repo_root, path) for path in auxiliary_refs],
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
        if matrix_row.get("support_state") != case.support_state:
            blockers.append(
                f"{case.case_id}: support_state={matrix_row.get('support_state')}, "
                f"expected {case.support_state}"
            )
        if matrix_row.get("oracle_boundary") != case.oracle_boundary:
            blockers.append(
                f"{case.case_id}: oracle_boundary={matrix_row.get('oracle_boundary')}, "
                f"expected {case.oracle_boundary}"
            )
        if matrix_row.get("unsupported_diagnostic_code") != case.diagnostic_code:
            blockers.append(
                f"{case.case_id}: unsupported_diagnostic_code="
                f"{matrix_row.get('unsupported_diagnostic_code')}"
            )
        if case.diagnostic_fragment not in str(matrix_row.get("unsupported_diagnostic_message")):
            blockers.append(f"{case.case_id}: matrix diagnostic message does not include fragment")

    return {
        "case_id": case.case_id,
        "kind": case.stage_kind,
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
                integer_minimums={"executed_fixture_count": 16, "passed_fixture_count": 16},
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
    unsupported_stage_ids = [
        case.case_id for case in unsupported if case.support_state == "unsupported_diagnostic"
    ]
    runtime_error_stage_ids = [
        case.case_id for case in unsupported if case.support_state == "runtime_error_diagnostic"
    ]
    invalid_shape_stage_ids = [
        case.case_id for case in unsupported if case.support_state == "invalid_shape_diagnostic"
    ]
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
        "diagnostic_case_count": len(unsupported),
        "unsupported_diagnostic_count": len(unsupported_stage_ids),
        "runtime_error_diagnostic_count": len(runtime_error_stage_ids),
        "invalid_shape_diagnostic_count": len(invalid_shape_stage_ids),
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
        "runtime_error_case_ids": runtime_error_stage_ids,
        "invalid_shape_case_ids": invalid_shape_stage_ids,
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
