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

from native_workflow_protocol import extract_result, public_workflow_command, report_fields, strict_json
from release_feature_contract import RELEASE_USER_SURFACE_EXAMPLE_FEATURES
from run_clickbench_query_uat import equivalent


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.admitted_semantics_matrix_report.v1"
MATRIX_SCHEMA_VERSION = "shardloom.admitted_semantics_fixture_matrix.v1"
DEFAULT_FEATURES = RELEASE_USER_SURFACE_EXAMPLE_FEATURES
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

EXPECTED_REMAINING_MATRIX_GAPS = (
    "broad SQL-standard subquery parity beyond the admitted bounded local scalar/row-value IN/NOT IN, "
    "EXISTS/NOT EXISTS, quantified ANY/ALL, nested scalar IN, projected joined/grouped "
    "scalar/row-value IN/NOT IN/EXISTS/NOT EXISTS, projected quantified, source-qualified "
    "scalar/row-value IN/NOT IN/EXISTS/NOT EXISTS/quantified local subquery references, "
    "correlated outer.<column> subquery filter, subquery-backed predicate/CASE projection, "
    "HAVING-level scalar/row-value IN/NOT IN, EXISTS/NOT EXISTS, and correlated quantified "
    "variants, and deterministic outer-reference diagnostics",
    "external-oracle result artifact population",
    "general fuzz execution beyond the deterministic v1 property/fuzz lanes",
)
DIAGNOSTIC_SUPPORT_STATES = {
    "unsupported_diagnostic",
    "runtime_error_diagnostic",
    "invalid_shape_diagnostic",
}
DIAGNOSTIC_ORACLE_BY_SUPPORT_STATE = {
    "unsupported_diagnostic": "deterministic_unsupported_diagnostic",
    "runtime_error_diagnostic": "deterministic_runtime_error_diagnostic",
    "invalid_shape_diagnostic": "deterministic_invalid_shape_diagnostic",
}


@dataclass(frozen=True)
class SqlFixtureCase:
    case_id: str
    source_name: str
    source_text: str
    statement_template: str
    expected_jsonl: str
    property_seed: int | None = None
    fuzz_seed: int | None = None
    fuzz_surface: str | None = None
    auxiliary_sources: tuple[tuple[str, str, str], ...] = ()
    output_format: str | None = None
    output_name: str | None = None
    expected_output_text: str | None = None


@dataclass(frozen=True)
class UnsupportedCase:
    case_id: str
    source_name: str
    source_text: str
    statement_template: str
    diagnostic_code: str
    diagnostic_fragment: str
    output_format: str | None = None
    output_name: str | None = None
    allow_overwrite: bool = True
    preexisting_output_text: str | None = None
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
            '{"id":1,"plus_three":20595,"end_minus_two":20594,'
            '"span_days":4,"shifted_ts":1779194175000000,"elapsed_seconds":185}\n'
            '{"id":2,"plus_three":20457,"end_minus_two":20461,'
            '"span_days":9,"shifted_ts":1767225690000000,"elapsed_seconds":90}\n'
        ),
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
            '{"id":1,"next_day":20593,"prior_two":20590,'
            '"shifted_ts":1779194175000000,"prior_minute":1779194025000000}\n'
            '{"id":2,"next_day":20455,"prior_two":20452,'
            '"shifted_ts":1767225690000000,"prior_minute":1767225540000000}\n'
        ),
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
            '{"id":2,"event_ts_utc":1779212096000000}\n'
            '{"id":3,"event_ts_utc":1779212100000000}\n'
        ),
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
            '{"id":1,"size_band":"small","cutoff_day":20453,"label_choice":"fallback-alpha"}\n'
            '{"id":2,"size_band":"large","cutoff_day":20818,"label_choice":"preferred-beta"}\n'
            '{"id":3,"size_band":"small","cutoff_day":20818,"label_choice":"fallback-gamma"}\n'
        ),
    )


def binary_hex_literal_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_hex_literal_projection",
        source_name="binary-hex-literal.csv",
        source_text="id,label\n1,alpha\n2,beta\n",
        statement_template="SELECT id,X'00ff10' AS payload FROM '{source}' LIMIT 10",
        expected_jsonl=(
            '{"id":1,"payload":"00ff10"}\n'
            '{"id":2,"payload":"00ff10"}\n'
        ),
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
            '{"id":1,"marker":"6f6b","payload":"726177"}\n'
            '{"id":2,"marker":"6f6b","payload":"726177"}\n'
        ),
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
    )


def complex_csv_output_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="complex_csv_output_projection",
        source_name="complex-csv-output.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,\n",
        statement_template=(
            "SELECT id,ARRAY[1,2] AS values,STRUCT(label, amount) AS payload "
            "FROM '{source}' LIMIT 2"
        ),
        expected_jsonl=(
            '{"id":1,"values":[1,2],"payload":{"label":"alpha","amount":8}}\n'
            '{"id":2,"values":[1,2],"payload":{"label":"beta","amount":null}}\n'
        ),
        output_format="csv",
        output_name="complex-csv-output-result.csv",
        expected_output_text=(
            'id,values,payload\n'
            '1,"[1,2]","{""label"":""alpha"",""amount"":8}"\n'
            '2,"[1,2]","{""label"":""beta"",""amount"":null}"\n'
        ),
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
    )


def binary_cast_projection_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_cast_projection_predicate",
        source_name="binary-cast.csv",
        source_text=(
            "id,label_prefix,label_suffix,amount_text\n"
            "1,al,pha, 42A \n"
            "2,be,ta,7b\n"
            "3,,,\n"
        ),
        statement_template=(
            "SELECT id,CAST(CONCAT(label_prefix,label_suffix) AS binary) AS label_bytes,"
            "TRY_CAST(LOWER(TRIM(amount_text)) AS varbinary) AS amount_bytes FROM '{source}' "
            "WHERE CAST(CONCAT(label_prefix,label_suffix) AS binary) = X'616c706861' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label_bytes":"616c706861",'
            '"amount_bytes":"343261"}\n'
        ),
    )


def binary_cast_ordering_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_cast_ordering_predicate",
        source_name="binary-cast-ordering.csv",
        source_text="id,label\n1, Alpha \n2,Beta\n3,Alp\n4,\n5,Gamma\n",
        statement_template=(
            "SELECT id,CAST(LOWER(TRIM(label)) AS binary) AS label_bytes FROM '{source}' "
            "WHERE CAST(LOWER(TRIM(label)) AS binary) > BINARY 'alpha' ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":2,"label_bytes":"62657461"}\n'
            '{"id":5,"label_bytes":"67616d6d61"}\n'
        ),
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
            '{"id":1,"amount_decimal":"decimal128(10,2):1234",'
            '"raw_decimal":"decimal128(10,2):1230"}\n'
        ),
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
            '{"id":1,"adjusted":"decimal128(11,2):1359",'
            '"half":"decimal128(38,6):6170000","scaled":"decimal128(13,4):185100"}\n'
            '{"id":2,"adjusted":"decimal128(11,2):1675",'
            '"half":"decimal128(38,6):7750000","scaled":"decimal128(13,4):232500"}\n'
            '{"id":3,"adjusted":"decimal128(11,2):2250",'
            '"half":"decimal128(38,6):10625000","scaled":"decimal128(13,4):318750"}\n'
        ),
    )


def binary_helper_projection_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_helper_projection",
        source_name="binary-helper-projection.csv",
        source_text=(
            "id,hex_payload,b64_prefix,b64_suffix\n"
            "1, 00FF10 ,AP,8Q\n"
            "2, 616C706861 ,YWxw,aGE=\n"
            "3,,,\n"
        ),
        statement_template=(
            "SELECT id,UNHEX(LOWER(TRIM(hex_payload))) AS payload_hex,"
            "FROM_BASE64(CONCAT(b64_prefix,b64_suffix)) AS payload_b64 "
            "FROM '{source}' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"payload_hex":"00ff10",'
            '"payload_b64":"00ff10"}\n'
            '{"id":2,"payload_hex":"616c706861",'
            '"payload_b64":"616c706861"}\n'
            '{"id":3,"payload_hex":null,"payload_b64":null}\n'
        ),
    )


def binary_helper_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_helper_predicate",
        source_name="binary-helper-predicate.csv",
        source_text=(
            "id,hex_payload,b64_prefix,b64_suffix\n"
            "1, 00FF10 ,AP,8Q\n"
            "2, 616C706861 ,YWxw,aGE=\n"
            "3,726177ff,cmF3,/w==\n"
            "4,,,\n"
        ),
        statement_template=(
            "SELECT id FROM '{source}' WHERE "
            "FROM_BASE64(CONCAT(b64_prefix,b64_suffix)) = X'00ff10' "
            "OR UNHEX(LOWER(TRIM(hex_payload))) != BINARY 'alpha' LIMIT 10"
        ),
        expected_jsonl='{"id":1}\n{"id":3}\n',
    )


def binary_byte_length_projection_predicate_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_byte_length_projection_predicate",
        source_name="binary-byte-length.csv",
        source_text=(
            "id,hex_payload,b64_prefix,b64_suffix,label_prefix,label_suffix\n"
            "1, 00FF10 ,AP,8Q,al,pha\n"
            "2, 616C706861 ,YWxw,aGE=,be,ta\n"
            "3,,,,,\n"
        ),
        statement_template=(
            "SELECT id,BYTE_LENGTH(UNHEX(LOWER(TRIM(hex_payload)))) AS payload_len,"
            "OCTET_LENGTH(CAST(CONCAT(label_prefix,label_suffix) AS binary)) AS label_len "
            "FROM '{source}' "
            "WHERE BYTE_LENGTH(FROM_BASE64(CONCAT(b64_prefix,b64_suffix))) >= 4 LIMIT 10"
        ),
        expected_jsonl='{"id":2,"payload_len":5,"label_len":4}\n',
    )


def in_predicate_literal_null_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="in_predicate_literal_null_semantics",
        source_name="in-null.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,,21\n4,gamma,13\n",
        statement_template="SELECT id,label FROM '{source}' WHERE label IN ('alpha', NULL) LIMIT 10",
        expected_jsonl='{"id":1,"label":"alpha"}\n',
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
        auxiliary_sources=(
            (
                "allowed",
                "row-value-in-subquery-allowed.csv",
                "allowed_id,allowed_label,active,score\n1,alpha,true,20\n3,gamma,true,40\n5,NULL,true,50\n4,delta,false,60\n2,beta,true,10\n",
            ),
        ),
    )


def not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="not_in_subquery_semantics",
        source_name="not-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id NOT IN ("
            "SELECT id FROM '{allowed}' WHERE active IS TRUE ORDER BY score ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "not-in-subquery-allowed.csv",
                "id,active,score\n1,true,10\n3,true,20\n4,false,30\n",
            ),
        ),
    )


def row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="row_value_not_in_subquery_semantics",
        source_name="row-value-not-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) NOT IN ("
            "SELECT allowed_id,allowed_label FROM '{allowed}' "
            "WHERE active IS TRUE ORDER BY score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "row-value-not-in-subquery-allowed.csv",
                (
                    "allowed_id,allowed_label,active,score\n"
                    "1,alpha,true,20\n"
                    "3,gamma,true,40\n"
                    "4,delta,false,60\n"
                ),
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
    )


def sql_intersect_composition_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="sql_intersect_composition_semantics",
        source_name="intersect-left.csv",
        source_text="id,label\n1,alpha\n2,beta\n2,beta\n3,gamma\n",
        auxiliary_sources=(
            (
                "right",
                "intersect-right.csv",
                "id,label\n2,beta\n3,gamma\n4,delta\n",
            ),
        ),
        statement_template=(
            "SELECT id,label FROM '{source}' "
            "INTERSECT SELECT id,label FROM '{right}' "
            "ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":3,"label":"gamma"}\n',
    )


def sql_except_composition_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="sql_except_composition_semantics",
        source_name="except-left.csv",
        source_text="id,label\n1,alpha\n2,beta\n2,beta\n3,gamma\n",
        auxiliary_sources=(
            (
                "right",
                "except-right.csv",
                "id,label\n2,beta\n4,delta\n",
            ),
        ),
        statement_template=(
            "SELECT id,label FROM '{source}' "
            "EXCEPT SELECT id,label FROM '{right}' "
            "ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
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


def source_qualified_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_not_in_subquery_semantics",
        source_name="source-qualified-not-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id NOT IN ("
            "SELECT allowed.id FROM '{allowed}' AS allowed "
            "WHERE allowed.active IS TRUE AND outer.amount >= allowed.min_amount "
            "ORDER BY allowed.min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "source-qualified-not-in-subquery-allowed.csv",
                (
                    "id,label,min_amount,active\n"
                    "1,alpha,5,true\n"
                    "1,alpha,99,true\n"
                    "2,beta,25,true\n"
                    "3,gamma,20,true\n"
                    "5,epsilon,1,true\n"
                ),
            ),
        ),
    )


def source_qualified_row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_row_value_in_subquery_semantics",
        source_name="source-qualified-row-value-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) IN ("
            "SELECT allowed.id,allowed.label FROM '{allowed}' AS allowed "
            "WHERE allowed.id = outer.id AND allowed.active IS TRUE "
            "AND outer.amount >= allowed.min_amount "
            "ORDER BY allowed.min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "source-qualified-row-value-in-subquery-allowed.csv",
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


def source_qualified_row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_row_value_not_in_subquery_semantics",
        source_name="source-qualified-row-value-not-in-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) NOT IN ("
            "SELECT allowed.id,allowed.label FROM '{allowed}' AS allowed "
            "WHERE allowed.active IS TRUE AND outer.amount >= allowed.min_amount "
            "ORDER BY allowed.min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "source-qualified-row-value-not-in-subquery-allowed.csv",
                (
                    "id,label,min_amount,active\n"
                    "1,alpha,5,true\n"
                    "1,alpha,99,true\n"
                    "2,beta,25,true\n"
                    "3,gamma,20,true\n"
                    "5,epsilon,1,true\n"
                ),
            ),
        ),
    )


def source_qualified_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_exists_subquery_semantics",
        source_name="source-qualified-exists-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE EXISTS ("
            "SELECT allowed.id FROM '{allowed}' AS allowed "
            "WHERE allowed.id = outer.id AND allowed.active IS TRUE "
            "AND allowed.min_amount <= outer.amount "
            "ORDER BY allowed.min_amount ASC LIMIT 1"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":3,"label":"gamma"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "source-qualified-exists-subquery-allowed.csv",
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


def source_qualified_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_not_exists_subquery_semantics",
        source_name="source-qualified-not-exists-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE NOT EXISTS ("
            "SELECT allowed.id FROM '{allowed}' AS allowed "
            "WHERE allowed.id = outer.id AND allowed.active IS TRUE "
            "AND allowed.min_amount <= outer.amount LIMIT 1"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "source-qualified-not-exists-subquery-allowed.csv",
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


def source_qualified_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="source_qualified_quantified_subquery_semantics",
        source_name="source-qualified-quantified-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE amount > ALL ("
            "SELECT thresholds.min_amount FROM '{thresholds}' AS thresholds "
            "WHERE thresholds.id = outer.id "
            "ORDER BY thresholds.min_amount ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        auxiliary_sources=(
            (
                "thresholds",
                "source-qualified-quantified-subquery-thresholds.csv",
                "id,min_amount\n1,5\n1,9\n2,25\n3,20\n3,29\n5,1\n",
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


def correlated_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_not_exists_subquery_semantics",
        source_name="correlated-not-exists-subquery-source.csv",
        source_text="id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE NOT EXISTS ("
            "SELECT * FROM '{allowed}' WHERE id = outer.id AND active IS TRUE "
            "AND min_amount <= outer.amount LIMIT 1"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "correlated-not-exists-subquery-allowed.csv",
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
        auxiliary_sources=(
            (
                "grouped",
                "grouped-projected-in-values.csv",
                "id,amount\n1,10\n1,20\n2,5\n3,7\n3,9\n4,1\n",
            ),
        ),
    )


PROJECTED_NEGATIVE_SOURCE_TEXT = "id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n"

PROJECTED_NEGATIVE_ALLOWED_TEXT = (
    "id,label,active,score\n"
    "1,alpha,true,30\n"
    "2,beta,false,20\n"
    "3,gamma,true,40\n"
    "5,epsilon,true,50\n"
)

PROJECTED_NEGATIVE_GROUPED_TEXT = (
    "id,label,amount\n"
    "1,alpha,10\n"
    "1,alpha,20\n"
    "2,beta,5\n"
    "3,gamma,7\n"
    "3,gamma,9\n"
    "4,delta,1\n"
)


def joined_projected_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_not_in_subquery_semantics",
        source_name="joined-projected-not-in-source.csv",
        source_text=PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id NOT IN ("
            "SELECT a.id FROM '{source}' AS s INNER JOIN '{allowed}' AS a "
            "ON s.id = a.id WHERE a.active IS TRUE ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            ("allowed", "joined-projected-not-in-allowed.csv", PROJECTED_NEGATIVE_ALLOWED_TEXT),
        ),
    )


def grouped_having_projected_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="grouped_having_projected_not_in_subquery_semantics",
        source_name="grouped-projected-not-in-source.csv",
        source_text=PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id NOT IN ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            ("grouped", "grouped-projected-not-in-values.csv", PROJECTED_NEGATIVE_GROUPED_TEXT),
        ),
    )


def joined_projected_row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_row_value_not_in_subquery_semantics",
        source_name="joined-projected-row-value-not-in-source.csv",
        source_text=PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) NOT IN ("
            "SELECT s.id,s.label FROM '{source}' AS s INNER JOIN '{allowed}' AS a "
            "ON s.id = a.id WHERE a.active IS TRUE ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "joined-projected-row-value-not-in-allowed.csv",
                PROJECTED_NEGATIVE_ALLOWED_TEXT,
            ),
        ),
    )


def grouped_having_projected_row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="grouped_having_projected_row_value_not_in_subquery_semantics",
        source_name="grouped-projected-row-value-not-in-source.csv",
        source_text=PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) NOT IN ("
            "SELECT id,label FROM '{grouped}' GROUP BY id,label HAVING count(*) >= 2 "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "grouped",
                "grouped-projected-row-value-not-in-values.csv",
                PROJECTED_NEGATIVE_GROUPED_TEXT,
            ),
        ),
    )


def joined_projected_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="joined_projected_not_exists_subquery_semantics",
        source_name="joined-projected-not-exists-source.csv",
        source_text=PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE NOT EXISTS ("
            "SELECT a.id FROM '{source}' AS s INNER JOIN '{allowed}' AS a "
            "ON s.id = a.id WHERE a.active IS TRUE AND a.score > 100 "
            "ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        auxiliary_sources=(
            (
                "allowed",
                "joined-projected-not-exists-allowed.csv",
                PROJECTED_NEGATIVE_ALLOWED_TEXT,
            ),
        ),
    )


def grouped_having_projected_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="grouped_having_projected_not_exists_subquery_semantics",
        source_name="grouped-projected-not-exists-source.csv",
        source_text=PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE NOT EXISTS ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 3 "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"label":"alpha"}\n'
            '{"id":2,"label":"beta"}\n'
            '{"id":3,"label":"gamma"}\n'
            '{"id":4,"label":"delta"}\n'
        ),
        auxiliary_sources=(
            (
                "grouped",
                "grouped-projected-not-exists-values.csv",
                PROJECTED_NEGATIVE_GROUPED_TEXT,
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


CORRELATED_JOINED_PROJECTED_NEGATIVE_SOURCE_TEXT = (
    "id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n"
)

CORRELATED_JOINED_PROJECTED_NEGATIVE_CANDIDATES_TEXT = (
    "id,label,min_amount\n"
    "1,alpha,5\n"
    "1,alpha,99\n"
    "2,beta,25\n"
    "3,gamma,20\n"
    "5,epsilon,1\n"
)

CORRELATED_JOINED_PROJECTED_NEGATIVE_ALLOWED_TEXT = (
    "id,active,score\n1,true,30\n2,true,20\n3,true,40\n5,false,50\n"
)


def correlated_joined_projected_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_not_in_subquery_semantics",
        source_name="correlated-joined-projected-not-in-source.csv",
        source_text=CORRELATED_JOINED_PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id NOT IN ("
            "SELECT c.id FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE AND c.id = outer.id "
            "AND c.min_amount <= outer.amount ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "candidates",
                "correlated-joined-projected-not-in-candidates.csv",
                CORRELATED_JOINED_PROJECTED_NEGATIVE_CANDIDATES_TEXT,
            ),
            (
                "allowed",
                "correlated-joined-projected-not-in-allowed.csv",
                CORRELATED_JOINED_PROJECTED_NEGATIVE_ALLOWED_TEXT,
            ),
        ),
    )


def correlated_joined_projected_row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_row_value_not_in_subquery_semantics",
        source_name="correlated-joined-projected-row-value-not-in-source.csv",
        source_text=CORRELATED_JOINED_PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) NOT IN ("
            "SELECT c.id,c.label FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE AND c.id = outer.id "
            "AND c.min_amount <= outer.amount ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "candidates",
                "correlated-joined-projected-row-value-not-in-candidates.csv",
                CORRELATED_JOINED_PROJECTED_NEGATIVE_CANDIDATES_TEXT,
            ),
            (
                "allowed",
                "correlated-joined-projected-row-value-not-in-allowed.csv",
                CORRELATED_JOINED_PROJECTED_NEGATIVE_ALLOWED_TEXT,
            ),
        ),
    )


def correlated_joined_projected_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_joined_projected_not_exists_subquery_semantics",
        source_name="correlated-joined-projected-not-exists-source.csv",
        source_text=CORRELATED_JOINED_PROJECTED_NEGATIVE_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE NOT EXISTS ("
            "SELECT c.id FROM '{candidates}' AS c INNER JOIN '{allowed}' AS a "
            "ON c.id = a.id WHERE a.active IS TRUE AND c.id = outer.id "
            "AND c.min_amount <= outer.amount ORDER BY a.score DESC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "candidates",
                "correlated-joined-projected-not-exists-candidates.csv",
                CORRELATED_JOINED_PROJECTED_NEGATIVE_CANDIDATES_TEXT,
            ),
            (
                "allowed",
                "correlated-joined-projected-not-exists-allowed.csv",
                CORRELATED_JOINED_PROJECTED_NEGATIVE_ALLOWED_TEXT,
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
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-exists-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def correlated_grouped_having_projected_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_not_in_subquery_semantics",
        source_name="correlated-grouped-projected-not-in-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id NOT IN ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "AND id = outer.id AND min(min_amount) <= outer.amount "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-not-in-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def correlated_grouped_having_projected_row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_row_value_not_in_subquery_semantics",
        source_name="correlated-grouped-projected-row-value-not-in-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE (id,label) NOT IN ("
            "SELECT id,label FROM '{grouped}' GROUP BY id,label HAVING count(*) >= 2 "
            "AND id = outer.id AND min(min_amount) <= outer.amount "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-row-value-not-in-values.csv",
                CORRELATED_GROUPED_PROJECTED_VALUES_TEXT,
            ),
        ),
    )


def correlated_grouped_having_projected_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="correlated_grouped_having_projected_not_exists_subquery_semantics",
        source_name="correlated-grouped-projected-not-exists-source.csv",
        source_text=CORRELATED_GROUPED_PROJECTED_SOURCE_TEXT,
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE NOT EXISTS ("
            "SELECT id FROM '{grouped}' GROUP BY id HAVING count(*) >= 2 "
            "AND id = outer.id AND min(min_amount) <= outer.amount "
            "ORDER BY id ASC LIMIT 10"
            ") LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":4,"label":"delta"}\n',
        auxiliary_sources=(
            (
                "grouped",
                "correlated-grouped-projected-not-exists-values.csv",
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
            '{"region":"north","rows":3,"total":45.0}\n'
            '{"region":"east","rows":2,"total":23.0}\n'
        ),
        auxiliary_sources=(
            (
                "allowed",
                "having-in-subquery-allowed.csv",
                "rows,active,score\n2,true,10\n3,true,20\n1,false,30\n",
            ),
        ),
    )


def having_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_not_in_subquery_semantics",
        source_name="having-not-in-subquery-source.csv",
        source_text=(
            "region,amount,active\n"
            "north,10,true\n"
            "north,15,true\n"
            "south,5,false\n"
            "south,7,true\n"
            "east,30,true\n"
        ),
        statement_template=(
            "SELECT region,sum(amount) AS total FROM '{source}' "
            "GROUP BY region HAVING total NOT IN ("
            "SELECT allowed.min_amount FROM '{allowed}' AS allowed "
            "WHERE allowed.enabled IS TRUE ORDER BY allowed.min_amount ASC LIMIT 10"
            ") ORDER BY region ASC LIMIT 10"
        ),
        expected_jsonl='{"region":"east","total":30.0}\n{"region":"south","total":12.0}\n',
        auxiliary_sources=(
            (
                "allowed",
                "having-not-in-subquery-allowed.csv",
                "region,enabled,min_amount\nnorth,true,25\neast,true,40\nwest,true,1\n",
            ),
        ),
    )


def having_row_value_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_row_value_in_subquery_semantics",
        source_name="having-row-value-in-subquery-source.csv",
        source_text=(
            "region,label,amount\n"
            "north,A,10\n"
            "north,A,15\n"
            "south,B,5\n"
            "south,B,7\n"
            "east,C,30\n"
        ),
        statement_template=(
            "SELECT region,label,sum(amount) AS total FROM '{source}' "
            "GROUP BY region,label HAVING (region,label) IN ("
            "SELECT allowed.region,allowed.label FROM '{allowed}' AS allowed "
            "WHERE allowed.enabled IS TRUE ORDER BY allowed.min_amount ASC LIMIT 10"
            ") ORDER BY region ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"region":"east","label":"C","total":30.0}\n'
            '{"region":"north","label":"A","total":25.0}\n'
        ),
        auxiliary_sources=(
            (
                "allowed",
                "having-row-value-in-subquery-allowed.csv",
                "region,label,enabled,min_amount\nnorth,A,true,25\neast,C,true,40\nwest,Z,true,1\n",
            ),
        ),
    )


def having_row_value_not_in_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_row_value_not_in_subquery_semantics",
        source_name="having-row-value-not-in-subquery-source.csv",
        source_text=(
            "region,label,amount\n"
            "north,A,10\n"
            "north,A,15\n"
            "south,B,5\n"
            "south,B,7\n"
            "east,C,30\n"
        ),
        statement_template=(
            "SELECT region,label,sum(amount) AS total FROM '{source}' "
            "GROUP BY region,label HAVING (region,label) NOT IN ("
            "SELECT allowed.region,allowed.label FROM '{allowed}' AS allowed "
            "WHERE allowed.enabled IS TRUE ORDER BY allowed.min_amount ASC LIMIT 10"
            ") ORDER BY region ASC LIMIT 10"
        ),
        expected_jsonl='{"region":"south","label":"B","total":12.0}\n',
        auxiliary_sources=(
            (
                "allowed",
                "having-row-value-not-in-subquery-allowed.csv",
                "region,label,enabled,min_amount\nnorth,A,true,25\neast,C,true,40\nwest,Z,true,1\n",
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
            '{"region":"north","rows":3,"total":45.0}\n'
            '{"region":"east","rows":2,"total":23.0}\n'
            '{"region":"west","rows":1,"total":20.0}\n'
        ),
        auxiliary_sources=(
            (
                "allowed",
                "having-exists-subquery-allowed.csv",
                "active,score\nfalse,10\ntrue,30\ntrue,20\n",
            ),
        ),
    )


def having_not_exists_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_not_exists_subquery_semantics",
        source_name="having-not-exists-subquery-source.csv",
        source_text=(
            "region,amount,active\n"
            "north,10,true\n"
            "north,15,true\n"
            "south,5,false\n"
            "south,7,true\n"
            "east,30,true\n"
        ),
        statement_template=(
            "SELECT region,sum(amount) AS total FROM '{source}' "
            "GROUP BY region HAVING NOT EXISTS ("
            "SELECT allowed.region FROM '{allowed}' AS allowed "
            "WHERE allowed.enabled IS TRUE AND allowed.region = outer.region "
            "ORDER BY allowed.min_amount ASC LIMIT 10"
            ") ORDER BY region ASC LIMIT 10"
        ),
        expected_jsonl='{"region":"south","total":12.0}\n',
        auxiliary_sources=(
            (
                "allowed",
                "having-not-exists-subquery-allowed.csv",
                "region,enabled,min_amount\nnorth,true,25\neast,true,40\nwest,true,1\n",
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
            '{"region":"north","rows":3,"total":45.0}\n'
            '{"region":"east","rows":2,"total":23.0}\n'
        ),
        auxiliary_sources=(
            (
                "thresholds",
                "having-quantified-subquery-thresholds.csv",
                "threshold,active,score\n20,true,10\n22,true,20\n99,false,30\n",
            ),
        ),
    )


def having_correlated_quantified_subquery_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="having_correlated_quantified_subquery_semantics",
        source_name="having-correlated-quantified-subquery-source.csv",
        source_text=(
            "region,amount,active\n"
            "north,10,true\n"
            "north,15,true\n"
            "south,5,false\n"
            "south,7,true\n"
            "east,30,true\n"
        ),
        statement_template=(
            "SELECT region,sum(amount) AS total FROM '{source}' "
            "GROUP BY region HAVING total > ALL ("
            "SELECT allowed.min_amount FROM '{allowed}' AS allowed "
            "WHERE allowed.region = outer.region "
            "ORDER BY allowed.min_amount ASC LIMIT 10"
            ") ORDER BY region ASC LIMIT 10"
        ),
        expected_jsonl='{"region":"south","total":12.0}\n',
        auxiliary_sources=(
            (
                "allowed",
                "having-correlated-quantified-subquery-allowed.csv",
                "region,enabled,min_amount\nnorth,true,25\neast,true,40\nwest,true,1\n",
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
    )


def window_analytic_frames_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="window_analytic_frames",
        source_name="window-frames.csv",
        source_text="id,region,priority,amount\n1,east,2,20\n2,east,1,\n3,east,1,10\n4,west,3,30\n",
        statement_template=(
            "SELECT id,"
            "SUM(amount) OVER (PARTITION BY region ORDER BY priority ROWS UNBOUNDED PRECEDING) AS total,"
            "COUNT(*) OVER (PARTITION BY region ORDER BY priority GROUPS CURRENT ROW) AS peers,"
            "COUNT(*) OVER (PARTITION BY region ORDER BY priority RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS nearby,"
            "FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY priority ROWS CURRENT ROW EXCLUDE CURRENT ROW) AS absent,"
            "LAST_VALUE(amount) OVER (PARTITION BY region ORDER BY priority) AS last,"
            "NTH_VALUE(amount,2) OVER (PARTITION BY region) AS second "
            "FROM '{source}'"
        ),
        expected_jsonl=(
            '{"id":1,"total":30.0,"peers":1,"nearby":3,"absent":null,"last":20,"second":null}\n'
            '{"id":2,"total":null,"peers":2,"nearby":2,"absent":null,"last":10,"second":null}\n'
            '{"id":3,"total":10.0,"peers":2,"nearby":2,"absent":null,"last":10,"second":null}\n'
            '{"id":4,"total":30.0,"peers":1,"nearby":1,"absent":null,"last":30,"second":null}\n'
        ),
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
        auxiliary_sources=(
            (
                "dim",
                "join-expression-dim.csv",
                "segment,discount\nsmall,4\nlarge,10\n",
            ),
        ),
    )


def join_logical_or_condition_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="join_logical_or_condition",
        source_name="join-or-fact.csv",
        source_text=(
            "id,customer_id,region\n"
            "1,10,east\n"
            "2,20,west\n"
            "3,30,north\n"
        ),
        statement_template=(
            "SELECT f.id,d.segment FROM '{source}' AS f INNER JOIN '{dim}' AS d "
            "ON f.customer_id = d.customer_id OR f.region = d.region LIMIT 10"
        ),
        expected_jsonl=(
            '{"f.id":1,"d.segment":"by_customer"}\n'
            '{"f.id":1,"d.segment":"cross_match"}\n'
            '{"f.id":2,"d.segment":"by_region"}\n'
            '{"f.id":2,"d.segment":"cross_match"}\n'
            '{"f.id":3,"d.segment":"both"}\n'
        ),
        auxiliary_sources=(
            (
                "dim",
                "join-or-dim.csv",
                (
                    "id,customer_id,region,segment\n"
                    "100,10,south,by_customer\n"
                    "101,99,west,by_region\n"
                    "102,30,north,both\n"
                    "103,20,east,cross_match\n"
                ),
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
        auxiliary_sources=(
            (
                "dim",
                "select-distinct-join-dim.csv",
                "customer_id,segment\n10,retail\n20,enterprise\n",
            ),
        ),
    )


def sql_parser_surface_fuzz_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="sql_parser_surface_fuzz_seed_20260613",
        source_name="sql-parser-fuzz.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,tax,label\n"
            "1,10,100,8,5,2,alpha\n"
            "2,10,200,12,7,3,beta\n"
            "3,20,100,22,10,4,gamma\n"
            "4,30,300,,6,5,delta\n"
        ),
        statement_template=(
            " SeLeCt id,label FROM '{source}' WHERE value >= 10 ORDER BY id DESC LIMIT 2"
        ),
        expected_jsonl='{"id":3,"label":"gamma"}\n{"id":2,"label":"beta"}\n',
        fuzz_seed=20260613,
        fuzz_surface="sql_parsing_subset",
    )


def expression_parser_fuzz_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="expression_parser_fuzz_seed_20260614",
        source_name="expression-parser-fuzz.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,tax,label\n"
            "1,10,100,8,5,2,alpha\n"
            "2,10,200,12,7,3,beta\n"
            "3,20,100,22,10,4,gamma\n"
            "4,30,300,,6,5,delta\n"
        ),
        statement_template=(
            "SELECT id,((amount + 2) * (tax - 1)) AS score "
            "FROM '{source}' WHERE amount >= 5 ORDER BY id ASC LIMIT 3"
        ),
        expected_jsonl='{"id":1,"score":7}\n{"id":2,"score":18}\n{"id":3,"score":36}\n',
        fuzz_seed=20260614,
        fuzz_surface="expression_parsing",
    )


def route_selection_join_fuzz_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="route_selection_join_fuzz_seed_20260615",
        source_name="route-selection-join-fact.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,tax,label\n"
            "1,10,100,8,5,2,alpha\n"
            "2,10,200,12,7,3,beta\n"
            "3,20,100,22,10,4,gamma\n"
            "4,30,300,,6,5,delta\n"
        ),
        statement_template=(
            "SELECT f.id,d.segment FROM '{source}' AS f JOIN '{dim}' AS d "
            "ON f.dim_key = d.dim_key WHERE f.value >= 10 ORDER BY f.id ASC LIMIT 10"
        ),
        expected_jsonl='{"f.id":2,"d.segment":"edge"}\n{"f.id":3,"d.segment":"core"}\n',
        auxiliary_sources=(
            ("dim", "route-selection-join-dim.csv", "dim_key,segment\n100,core\n200,edge\n"),
        ),
        fuzz_seed=20260615,
        fuzz_surface="route_selection",
    )


def route_selection_aggregate_topn_fuzz_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="route_selection_aggregate_topn_fuzz_seed_20260616",
        source_name="route-selection-aggregate-fuzz.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,tax,label\n"
            "1,10,100,8,5,2,alpha\n"
            "2,10,200,12,7,3,beta\n"
            "3,20,100,22,10,4,gamma\n"
            "4,30,300,,6,5,delta\n"
        ),
        statement_template=(
            "SELECT group_key,count(*) AS rows,sum(value) AS total "
            "FROM '{source}' WHERE value >= 0 GROUP BY group_key "
            "ORDER BY total DESC LIMIT 2"
        ),
        expected_jsonl='{"group_key":20,"rows":1,"total":22.0}\n{"group_key":10,"rows":2,"total":20.0}\n',
        fuzz_seed=20260616,
        fuzz_surface="route_selection",
    )


def output_writer_policy_fuzz_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="output_writer_policy_fuzz_seed_20260617",
        source_name="output-writer-policy-fuzz.csv",
        source_text="id,label,value\n1,alpha,8\n2,beta,12\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE value >= 8 ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl='{"id":1,"label":"alpha"}\n{"id":2,"label":"beta"}\n',
        output_format="csv",
        output_name="output-writer-policy.csv",
        expected_output_text="id,label\n1,alpha\n2,beta\n",
        fuzz_seed=20260617,
        fuzz_surface="output_writer_policy",
    )


def filter_project_limit_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="filter_project_limit_property_seed_20260618",
        source_name="filter-project-limit-property.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,rate,label,event_date,hex_payload\n"
            "1,10,100,8,5,2,alpha,2024-01-01,616c706861\n"
            "2,10,200,12,7,3,beta,2024-01-03,62657461\n"
            "3,20,100,22,10,4,gamma,2024-01-02,67616d6d61\n"
            "4,30,300,,6,5,delta,2024-01-05,64656c7461\n"
            "5,20,200,18,9,1,omega,2024-01-04,6f6d656761\n"
        ),
        statement_template=(
            "SELECT id,label,value FROM '{source}' "
            "WHERE value >= 10 ORDER BY value DESC LIMIT 3"
        ),
        expected_jsonl=(
            '{"id":3,"label":"gamma","value":22}\n'
            '{"id":5,"label":"omega","value":18}\n'
            '{"id":2,"label":"beta","value":12}\n'
        ),
        property_seed=20260618,
    )


def join_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="join_property_seed_20260619",
        source_name="join-property-fact.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,rate,label,event_date,hex_payload\n"
            "1,10,100,8,5,2,alpha,2024-01-01,616c706861\n"
            "2,10,200,12,7,3,beta,2024-01-03,62657461\n"
            "3,20,100,22,10,4,gamma,2024-01-02,67616d6d61\n"
            "4,30,300,,6,5,delta,2024-01-05,64656c7461\n"
            "5,20,200,18,9,1,omega,2024-01-04,6f6d656761\n"
        ),
        statement_template=(
            "SELECT f.id,d.segment,f.value FROM '{source}' AS f JOIN '{dim}' AS d "
            "ON f.dim_key = d.dim_key WHERE f.value >= 10 ORDER BY f.id ASC LIMIT 10"
        ),
        expected_jsonl=(
            '{"f.id":2,"d.segment":"edge","f.value":12}\n'
            '{"f.id":3,"d.segment":"core","f.value":22}\n'
            '{"f.id":5,"d.segment":"edge","f.value":18}\n'
        ),
        auxiliary_sources=(
            (
                "dim",
                "join-property-dim.csv",
                "dim_key,segment\n100,core\n200,edge\n300,cold\n",
            ),
        ),
        property_seed=20260619,
    )


def aggregate_topn_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="aggregate_topn_property_seed_20260620",
        source_name="aggregate-topn-property.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,rate,label,event_date,hex_payload\n"
            "1,10,100,8,5,2,alpha,2024-01-01,616c706861\n"
            "2,10,200,12,7,3,beta,2024-01-03,62657461\n"
            "3,20,100,22,10,4,gamma,2024-01-02,67616d6d61\n"
            "4,30,300,,6,5,delta,2024-01-05,64656c7461\n"
            "5,20,200,18,9,1,omega,2024-01-04,6f6d656761\n"
        ),
        statement_template=(
            "SELECT group_key,count(*) AS rows,sum(value) AS total "
            "FROM '{source}' WHERE value >= 0 GROUP BY group_key "
            "ORDER BY total DESC LIMIT 2"
        ),
        expected_jsonl=(
            '{"group_key":20,"rows":2,"total":40.0}\n'
            '{"group_key":10,"rows":2,"total":20.0}\n'
        ),
        property_seed=20260620,
    )


def subquery_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="in_subquery_property_seed_20260621",
        source_name="in-subquery-property-source.csv",
        source_text=(
            "id,group_key,dim_key,value,amount,rate,label,event_date,hex_payload\n"
            "1,10,100,8,5,2,alpha,2024-01-01,616c706861\n"
            "2,10,200,12,7,3,beta,2024-01-03,62657461\n"
            "3,20,100,22,10,4,gamma,2024-01-02,67616d6d61\n"
            "4,30,300,,6,5,delta,2024-01-05,64656c7461\n"
            "5,20,200,18,9,1,omega,2024-01-04,6f6d656761\n"
        ),
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE id IN ("
            "SELECT id FROM '{allowed}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2"
            ") ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":3,"label":"gamma"}\n',
        auxiliary_sources=(
            (
                "allowed",
                "in-subquery-property-allowed.csv",
                "id,active,score\n2,true,20\n3,true,10\n5,false,30\n",
            ),
        ),
        property_seed=20260621,
    )


def string_function_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="string_function_property_seed_20260622",
        source_name="string-function-property.csv",
        source_text="id,label,segment\n1,alpha,north\n2,beta,east\n3,gamma,north\n4,omega,west\n",
        statement_template=(
            "SELECT id,CONCAT(label, '-', segment) AS label_key,SUBSTR(label, 2, 3) AS middle,"
            "LEFT(label, 2) AS prefix,RIGHT(label, 2) AS suffix,REPLACE(label, 'a', '') AS scrubbed "
            "FROM '{source}' WHERE CONCAT(label, '-', segment) = 'gamma-north' LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":3,"label_key":"gamma-north","middle":"amm","prefix":"ga",'
            '"suffix":"ma","scrubbed":"gmm"}\n'
        ),
        property_seed=20260622,
    )


def temporal_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="temporal_property_seed_20260623",
        source_name="temporal-property.csv",
        source_text=(
            "id,start_date,end_date,start_ts,end_ts\n"
            "1,2026-06-01,2026-06-04,2026-06-01T00:00:00Z,2026-06-01T00:02:00Z\n"
            "2,2026-06-10,2026-06-16,2026-06-10T12:00:00Z,2026-06-10T12:04:30Z\n"
        ),
        statement_template=(
            "SELECT id,DATE_ADD_DAYS(CAST(start_date AS date32), 3) AS plus_three,"
            "DATE_SUB_DAYS(end_date, 2) AS end_minus_two,"
            "DATE_DIFF_DAYS(CAST(end_date AS date32), start_date) AS span_days,"
            "TIMESTAMP_ADD_SECONDS(CAST(start_ts AS timestamp_micros), 90) AS shifted_ts,"
            "TIMESTAMP_DIFF_SECONDS(CAST(end_ts AS timestamp_micros), start_ts) AS elapsed_seconds "
            "FROM '{source}' WHERE DATE_DIFF_DAYS(end_date, start_date) >= 3 LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":1,"plus_three":20608,"end_minus_two":20606,'
            '"span_days":3,"shifted_ts":1780272090000000,"elapsed_seconds":120}\n'
            '{"id":2,"plus_three":20617,"end_minus_two":20618,'
            '"span_days":6,"shifted_ts":1781092890000000,"elapsed_seconds":270}\n'
        ),
        property_seed=20260623,
    )


def decimal_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="decimal_property_seed_20260624",
        source_name="decimal-property.csv",
        source_text="id,amount\n1,10.00\n2,12.50\n3,20.25\n",
        statement_template=(
            "SELECT id,CAST(amount AS decimal128(10,2)) + "
            "CAST('0.75' AS decimal128(10,2)) AS adjusted,"
            "CAST(amount AS decimal128(10,2)) / 2 AS half,"
            "CAST(amount AS decimal128(10,2)) * CAST('1.25' AS decimal128(3,2)) AS scaled "
            "FROM '{source}' "
            "WHERE CAST(amount AS decimal128(10,2)) + 0 >= CAST('12.50' AS decimal128(10,2)) "
            "LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":2,"adjusted":"decimal128(11,2):1325",'
            '"half":"decimal128(38,6):6250000","scaled":"decimal128(13,4):156250"}\n'
            '{"id":3,"adjusted":"decimal128(11,2):2100",'
            '"half":"decimal128(38,6):10125000","scaled":"decimal128(13,4):253125"}\n'
        ),
        property_seed=20260624,
    )


def binary_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="binary_property_seed_20260625",
        source_name="binary-property.csv",
        source_text=(
            "id,hex_payload,b64_prefix,b64_suffix,label_prefix,label_suffix\n"
            "1, 00FF10 ,AP,8Q,al,pha\n"
            "2, 616C706861 ,YWxw,aGE=,be,ta\n"
            "3,6f6d656761,b21l,Zw==,om,ega\n"
        ),
        statement_template=(
            "SELECT id,BYTE_LENGTH(UNHEX(LOWER(TRIM(hex_payload)))) AS payload_len,"
            "OCTET_LENGTH(CAST(CONCAT(label_prefix,label_suffix) AS binary)) AS label_len "
            "FROM '{source}' "
            "WHERE BYTE_LENGTH(FROM_BASE64(CONCAT(b64_prefix,b64_suffix))) >= 4 LIMIT 10"
        ),
        expected_jsonl=(
            '{"id":2,"payload_len":5,"label_len":4}\n'
            '{"id":3,"payload_len":5,"label_len":5}\n'
        ),
        property_seed=20260625,
    )


def output_jsonl_property_case() -> SqlFixtureCase:
    return SqlFixtureCase(
        case_id="output_jsonl_property_seed_20260626",
        source_name="output-jsonl-property.csv",
        source_text="id,label,value\n1,alpha,8\n2,beta,12\n3,gamma,18\n",
        statement_template=(
            "SELECT id,label FROM '{source}' WHERE value >= 10 ORDER BY id ASC LIMIT 10"
        ),
        expected_jsonl='{"id":2,"label":"beta"}\n{"id":3,"label":"gamma"}\n',
        output_format="jsonl",
        output_name="output-jsonl-property.jsonl",
        expected_output_text='{"id":2,"label":"beta"}\n{"id":3,"label":"gamma"}\n',
        property_seed=20260626,
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
        ),
        SqlFixtureCase(
            case_id="like_escape_predicate_utf8",
            source_name="like-escape-predicate.csv",
            source_text="id,label\n1,alpha\n2,al_pha\n3,al%pha\n4,alxpha\n5,\n",
            statement_template=(
                "SELECT id,label FROM '{source}' WHERE label LIKE 'al!_%' ESCAPE '!' LIMIT 10"
            ),
            expected_jsonl='{"id":2,"label":"al_pha"}\n',
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
        ),
        SqlFixtureCase(
            case_id="null_safe_comparison_predicate_semantics",
            source_name="null-safe-comparison.csv",
            source_text=(
                "id,label,peer\n"
                "1,alpha,alpha\n"
                "2,alpha,beta\n"
                "3,,beta\n"
                "4,beta,\n"
                "5,,\n"
            ),
            statement_template=(
                "SELECT id,label IS NOT DISTINCT FROM peer AS same_null_safe "
                "FROM '{source}' WHERE label IS DISTINCT FROM peer LIMIT 10"
            ),
            expected_jsonl=(
                '{"id":2,"same_null_safe":false}\n'
                '{"id":3,"same_null_safe":false}\n'
                '{"id":4,"same_null_safe":false}\n'
            ),
        ),
        SqlFixtureCase(
            case_id="order_by_explicit_null_ordering",
            source_name="order-by-null-ordering.csv",
            source_text=(
                "id,label,amount\n"
                "1,missing_a,\n"
                "2,beta,10\n"
                "3,gamma,7\n"
                "4,missing_b,\n"
                "5,delta,12\n"
            ),
            statement_template=(
                "SELECT id,label FROM '{source}' ORDER BY amount ASC NULLS FIRST LIMIT 4"
            ),
            expected_jsonl=(
                '{"id":1,"label":"missing_a"}\n'
                '{"id":4,"label":"missing_b"}\n'
                '{"id":3,"label":"gamma"}\n'
                '{"id":2,"label":"beta"}\n'
            ),
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
                '{"region":"east","rows":2,"total_amount":22.0}\n'
                '{"region":"west","rows":2,"total_amount":19.0}\n'
            ),
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
        complex_csv_output_projection_case(),
        complex_distinct_projection_equality_case(),
        complex_order_by_projection_case(),
        sql_union_complex_distinct_equality_case(),
        sql_union_complex_ordering_case(),
        binary_cast_projection_predicate_case(),
        binary_cast_ordering_predicate_case(),
        decimal_cast_projection_predicate_case(),
        decimal_arithmetic_projection_case(),
        binary_helper_projection_case(),
        binary_helper_predicate_case(),
        binary_byte_length_projection_predicate_case(),
        in_predicate_literal_null_case(),
        row_value_in_predicate_case(),
        row_value_in_subquery_case(),
        not_in_subquery_case(),
        row_value_not_in_subquery_case(),
        exists_subquery_case(),
        quantified_subquery_case(),
        sql_union_composition_case(),
        sql_intersect_composition_case(),
        sql_except_composition_case(),
        in_subquery_scalar_case(),
        in_subquery_filtered_ordered_limited_case(),
        correlated_in_subquery_case(),
        source_qualified_in_subquery_case(),
        source_qualified_not_in_subquery_case(),
        source_qualified_row_value_in_subquery_case(),
        source_qualified_row_value_not_in_subquery_case(),
        source_qualified_exists_subquery_case(),
        source_qualified_not_exists_subquery_case(),
        source_qualified_quantified_subquery_case(),
        correlated_row_value_in_subquery_case(),
        correlated_exists_subquery_case(),
        correlated_not_exists_subquery_case(),
        correlated_quantified_subquery_case(),
        joined_projected_in_subquery_case(),
        joined_projected_not_in_subquery_case(),
        joined_projected_row_value_in_subquery_case(),
        joined_projected_row_value_not_in_subquery_case(),
        grouped_having_projected_in_subquery_case(),
        grouped_having_projected_not_in_subquery_case(),
        grouped_having_projected_row_value_not_in_subquery_case(),
        joined_projected_exists_subquery_case(),
        joined_projected_not_exists_subquery_case(),
        grouped_having_projected_exists_subquery_case(),
        grouped_having_projected_not_exists_subquery_case(),
        joined_projected_quantified_subquery_case(),
        correlated_joined_projected_in_subquery_case(),
        correlated_joined_projected_not_in_subquery_case(),
        correlated_joined_projected_row_value_in_subquery_case(),
        correlated_joined_projected_row_value_not_in_subquery_case(),
        correlated_joined_projected_quantified_subquery_case(),
        correlated_joined_projected_exists_subquery_case(),
        correlated_joined_projected_not_exists_subquery_case(),
        correlated_grouped_having_projected_in_subquery_case(),
        correlated_grouped_having_projected_not_in_subquery_case(),
        correlated_grouped_having_projected_row_value_in_subquery_case(),
        correlated_grouped_having_projected_row_value_not_in_subquery_case(),
        correlated_grouped_having_projected_quantified_subquery_case(),
        correlated_grouped_having_projected_exists_subquery_case(),
        correlated_grouped_having_projected_not_exists_subquery_case(),
        nested_in_subquery_case(),
        having_in_subquery_case(),
        having_not_in_subquery_case(),
        having_row_value_in_subquery_case(),
        having_row_value_not_in_subquery_case(),
        having_exists_subquery_case(),
        having_not_exists_subquery_case(),
        having_quantified_subquery_case(),
        having_correlated_quantified_subquery_case(),
        distinct_count_grouped_case(),
        select_distinct_projection_case(),
        select_distinct_aggregate_having_case(),
        having_hidden_aggregate_case(),
        window_mixed_case(),
        window_analytic_frames_case(),
        select_distinct_window_case(),
        join_multi_key_case(),
        join_scalar_expression_condition_case(),
        join_logical_or_condition_case(),
        select_distinct_join_case(),
        sql_parser_surface_fuzz_case(),
        expression_parser_fuzz_case(),
        route_selection_join_fuzz_case(),
        route_selection_aggregate_topn_fuzz_case(),
        output_writer_policy_fuzz_case(),
        filter_project_limit_property_case(),
        join_property_case(),
        aggregate_topn_property_case(),
        subquery_property_case(),
        string_function_property_case(),
        temporal_property_case(),
        decimal_property_case(),
        binary_property_case(),
        output_jsonl_property_case(),
    ]


def unsupported_cases() -> list[UnsupportedCase]:
    return [
        UnsupportedCase(
            case_id="runtime_error_numeric_division_by_zero",
            source_name="numeric-unsupported.csv",
            source_text="id,amount\n1,8\n",
            statement_template="SELECT id,amount / 0 AS broken FROM '{source}' LIMIT 10",
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment='division by zero',
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
            diagnostic_code='SL_UNSUPPORTED_SQL',
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
            diagnostic_code='SL_UNSUPPORTED_SQL',
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
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="timezone database semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_locale_collation",
            source_name="collation-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id,label COLLATE nocase AS folded FROM '{source}' LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_locale_case_insensitive_predicate",
            source_name="locale-casefold-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE label ILIKE 'a%' LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
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
            diagnostic_fragment='incompatible key types require an explicit lossless cast',
        ),
        UnsupportedCase(
            case_id="unsupported_nonbinary_source_binary_ordering_predicate",
            source_name="binary-source-ordering-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE label > BINARY 'alpha' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment='incompatible key types require an explicit lossless cast',
        ),
        UnsupportedCase(
            case_id="unsupported_list_array_access_cast",
            source_name="list-array-unsupported.csv",
            source_text="id,payload\n1,alpha\n",
            statement_template=(
                "SELECT id,LIST_EXTRACT(payload, 1) AS item FROM '{source}' LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="list and array accessors, function constructors, casts, and equality semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_struct_access_cast",
            source_name="struct-unsupported.csv",
            source_text="id,label,amount\n1,alpha,8\n",
            statement_template=(
                "SELECT id,ROW(label, amount) AS payload FROM '{source}' LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
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
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="projected subqueries do not admit ARRAY or STRUCT projection outputs for membership materialization",
        ),
        UnsupportedCase(
            case_id="unsupported_orc_nested_output_preservation",
            source_name="orc-nested-output-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id,ARRAY[1,2] AS values FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment='ORC does not admit nested output',
            output_format="orc",
            output_name="nested.orc",
        ),
        UnsupportedCase(
            case_id="unsupported_orc_typed_decimal_sink_preservation",
            source_name="orc-decimal-output-unsupported.csv",
            source_text="id,amount\n1,12.34\n",
            statement_template=(
                "SELECT id,CAST(amount AS decimal128(10,2)) AS amount_decimal "
                "FROM '{source}' LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment='ORC does not admit decimal or temporal output',
            output_format="orc",
            output_name="decimal.orc",
        ),
        UnsupportedCase(
            case_id="unsupported_variant_access",
            source_name="variant-unsupported.csv",
            source_text="id,payload\n1,alpha\n",
            statement_template=(
                "SELECT id,VARIANT_GET(payload, 'field') AS field FROM '{source}' LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="variant access semantics are not admitted",
        ),
        UnsupportedCase(
            case_id="unsupported_union_dtype_cast",
            source_name="union-dtype-unsupported.csv",
            source_text="id,payload\n1,alpha\n",
            statement_template="SELECT CAST(payload AS union) AS payload FROM '{source}' LIMIT 10",
            diagnostic_code='SL_UNSUPPORTED_SQL',
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
            diagnostic_code='SL_UNSUPPORTED_SQL',
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
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="JOIN ON complex key expressions are not admitted",
        ),
        UnsupportedCase(
            case_id="invalid_shape_scalar_multi_column_in_subquery",
            source_name="scalar-multi-column-subquery-unsupported.csv",
            source_text="id,label\n1,alpha\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE id IN "
                "(SELECT id,label FROM '{source}') LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment="multi-column IN subqueries require row-value source columns",
            support_state="invalid_shape_diagnostic",
            oracle_boundary="deterministic_invalid_shape_diagnostic",
            stage_kind="invalid_shape_diagnostic",
        ),
        UnsupportedCase(
            case_id="unsupported_unbound_source_qualified_in_subquery_select",
            source_name="unbound-source-qualified-in-subquery-select.csv",
            source_text="id,label,amount,active\n1,alpha,10,true\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE id IN "
                "(SELECT missing.id FROM '{source}' AS allowed) LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment=(
                "qualified IN subquery selected columns references admit only the subquery source qualifier"
            ),
        ),
        UnsupportedCase(
            case_id="unsupported_unbound_source_qualified_row_value_subquery_filter",
            source_name="unbound-source-qualified-row-value-filter.csv",
            source_text="id,label,amount,active\n1,alpha,10,true\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE (id,label) IN "
                "(SELECT allowed.id,allowed.label FROM '{source}' AS allowed "
                "WHERE missing.id = outer.id) LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment=(
                "qualified IN subquery predicates admit only outer.<column> references or the subquery source qualifier"
            ),
        ),
        UnsupportedCase(
            case_id="unsupported_unbound_source_qualified_exists_projection",
            source_name="unbound-source-qualified-exists-projection.csv",
            source_text="id,label,amount,active\n1,alpha,10,true\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE EXISTS "
                "(SELECT missing.id FROM '{source}' AS allowed LIMIT 1) LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment=(
                "qualified EXISTS subquery projection references admit only the subquery source qualifier"
            ),
        ),
        UnsupportedCase(
            case_id="unsupported_unbound_source_qualified_quantified_order_by",
            source_name="unbound-source-qualified-quantified-order-by.csv",
            source_text="id,label,amount,active\n1,alpha,10,true\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE amount > ALL "
                "(SELECT allowed.amount FROM '{source}' AS allowed "
                "ORDER BY missing.amount LIMIT 10) LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment=(
                "qualified local subquery ORDER BY references admit only the subquery source qualifier"
            ),
        ),
        UnsupportedCase(
            case_id="unsupported_outer_reference_non_column_comparison",
            source_name="outer-reference-noncomparison-unsupported.csv",
            source_text="id,label,amount\n1,alpha,10\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE id IN "
                "(SELECT id FROM '{source}' WHERE outer.amount > 10) LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment=(
                "correlated IN subquery predicates admit outer.<column> references only in "
                "column-to-column comparisons"
            ),
        ),
        UnsupportedCase(
            case_id="unsupported_outer_to_outer_subquery_comparison",
            source_name="outer-to-outer-comparison-unsupported.csv",
            source_text="id,label,amount\n1,alpha,10\n",
            statement_template=(
                "SELECT id FROM '{source}' WHERE id IN "
                "(SELECT id FROM '{source}' WHERE outer.id = outer.amount) LIMIT 10"
            ),
            diagnostic_code='SL_UNSUPPORTED_SQL',
            diagnostic_fragment=(
                "correlated IN subquery predicates require exactly one outer.<column> reference "
                "per column comparison"
            ),
        ),
        UnsupportedCase(
            case_id="unsupported_output_no_overwrite_policy",
            source_name="output-no-overwrite-policy.csv",
            source_text="id,label,value\n1,alpha,8\n2,beta,12\n",
            statement_template=(
                "SELECT id,label FROM '{source}' WHERE value >= 8 ORDER BY id ASC LIMIT 10"
            ),
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="output target already exists and overwrite is disabled",
            output_format="csv",
            output_name="existing-output.csv",
            allow_overwrite=False,
            preexisting_output_text="id,label\nexisting,row\n",
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
            "remaining_matrix_gap_status": "failed",
            "v1_runtime_scope_status": "failed",
            "v1_expected_validator_case_count": len(expected_case_ids),
            "v1_required_runtime_row_count": 0,
            "v1_missing_validator_case_count": len(expected_case_ids),
            "v1_unexpected_required_runtime_row_count": 0,
            "v1_support_report_row_count": 0,
            "deterministic_unsupported_scope_status": "failed",
            "deterministic_unsupported_row_count": 0,
            "deterministic_unsupported_oracle_row_count": 0,
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
    required_runtime_row_ids = {
        row_id
        for row_id, row in by_id.items()
        if row.get("runtime_validation") == "required"
    }
    stale = sorted(required_runtime_row_ids - expected_case_ids)
    if stale:
        blockers.append("matrix required runtime rows without validator cases: " + ",".join(stale))
    support_report_row_ids = sorted(
        row_id
        for row_id, row in by_id.items()
        if row.get("runtime_validation") != "required"
    )
    diagnostic_row_ids = sorted(
        row_id
        for row_id, row in by_id.items()
        if row.get("support_state") in DIAGNOSTIC_SUPPORT_STATES
    )
    deterministic_diagnostic_row_ids: list[str] = []
    for row_id in diagnostic_row_ids:
        row = by_id[row_id]
        support_state = str(row.get("support_state"))
        expected_oracle = DIAGNOSTIC_ORACLE_BY_SUPPORT_STATE[support_state]
        if row.get("oracle_boundary") != expected_oracle:
            blockers.append(
                f"{row_id}: diagnostic oracle_boundary={row.get('oracle_boundary')}, "
                f"expected {expected_oracle}"
            )
            continue
        if row.get("runtime_validation") != "required":
            blockers.append(f"{row_id}: diagnostic runtime_validation must be required")
            continue
        if row.get("unsupported_diagnostic_code") == "not_applicable_executable":
            blockers.append(f"{row_id}: deterministic diagnostic code is required")
            continue
        if not str(row.get("unsupported_diagnostic_message", "")).strip():
            blockers.append(f"{row_id}: deterministic diagnostic message is required")
            continue
        deterministic_diagnostic_row_ids.append(row_id)
    remaining_matrix_gaps = payload.get("remaining_matrix_gaps", [])
    if not isinstance(remaining_matrix_gaps, list):
        blockers.append("matrix remaining_matrix_gaps must be a list")
        remaining_matrix_gaps = []
    else:
        remaining_matrix_gaps = [str(value) for value in remaining_matrix_gaps]
    remaining_gap_status = "passed"
    if tuple(remaining_matrix_gaps) != EXPECTED_REMAINING_MATRIX_GAPS:
        remaining_gap_status = "failed"
        blockers.append(
            "matrix remaining_matrix_gaps changed: observed="
            + repr(remaining_matrix_gaps)
        )
    summary = {
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "row_count": len(by_id),
        "row_ids": sorted(by_id),
        "remaining_matrix_gaps": remaining_matrix_gaps,
        "remaining_matrix_gap_status": remaining_gap_status,
        "v1_runtime_scope_status": "passed" if not missing and not stale else "failed",
        "v1_expected_validator_case_count": len(expected_case_ids),
        "v1_required_runtime_row_count": len(required_runtime_row_ids),
        "v1_missing_validator_case_count": len(missing),
        "v1_unexpected_required_runtime_row_count": len(stale),
        "v1_support_report_row_count": len(support_report_row_ids),
        "deterministic_unsupported_scope_status": (
            "passed"
            if len(diagnostic_row_ids) == len(deterministic_diagnostic_row_ids)
            else "failed"
        ),
        "deterministic_unsupported_row_count": len(diagnostic_row_ids),
        "deterministic_unsupported_oracle_row_count": len(deterministic_diagnostic_row_ids),
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    return by_id, summary


def parse_json_output(completed: subprocess.CompletedProcess[str], label: str) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    try:
        payload = strict_json(completed.stdout)
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


def canonical_rows_digest(rows: list[dict[str, Any]]) -> str:
    return digest_text(json.dumps(rows, sort_keys=True, allow_nan=False))


def expected_rows(expected_jsonl: str) -> list[dict[str, Any]]:
    return [strict_json(line) for line in expected_jsonl.splitlines()]


def workflow_source_bindings(
    source_path: Path,
    auxiliary_refs: list[Path] | tuple[Path, ...] = (),
) -> dict[str, dict[str, str]]:
    return {
        str(path): {"input_format": "csv"}
        for path in (source_path, *auxiliary_refs)
    }


def selected_workflow_fields(fields: dict[str, Any]) -> dict[str, Any]:
    evidence_keys = {
        "public_workflow_requested_output",
        "result_payload_complete",
        "output_row_count",
        "result_schema_json",
        "result_schema_format",
        "output_path",
        "output_commit_status",
        "output_io_performed",
        "native_vortex_result_export_path",
        "native_vortex_result_export_format",
        "native_vortex_result_export_rows_written",
        "native_vortex_result_export_all_targets_committed",
    }
    return {
        key: fields[key]
        for key in sorted(fields)
        if key in evidence_keys
        or key.endswith(("fallback_attempted", "external_engine_invoked"))
    }


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
    output_path: Path | None = None
    if case.output_format is not None:
        output_name = case.output_name or f"{case.case_id}.{case.output_format}"
        output_path = work_dir / case.case_id / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
    cli_args = public_workflow_command(
        binary,
        statement,
        source_bindings=workflow_source_bindings(source_path, auxiliary_refs),
        requested_output="collect" if output_path is None else f"write_{case.output_format}",
        output=output_path,
        allow_overwrite=output_path is not None,
    )
    completed = run_subprocess(repo_root=repo_root, command=cli_args)
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
    observed_output_digest = ""
    observed_output_digest_source = ""
    expected_output_digest = ""
    expected_output_digest_source = ""
    fields: dict[str, Any] = {}
    expected = []
    try:
        expected = expected_rows(case.expected_jsonl)
    except Exception as exc:  # noqa: BLE001 - invalid reference is reported with the case.
        blockers.append(f"{case.case_id}: decoded reference JSONL is invalid: {exc}")

    if output_path is None:
        expected_output_digest = canonical_rows_digest(expected)
        expected_output_digest_source = "canonical_decoded_reference_rows"
    elif case.expected_output_text is None:
        blockers.append(f"{case.case_id}: writer case is missing expected_output_text")
    else:
        expected_output_digest = digest_text(case.expected_output_text)
        expected_output_digest_source = "decoded_reference_output_artifact"

    if completed.returncode != 0:
        blockers.append(f"{case.case_id}: returncode={completed.returncode}")
    if payload:
        if payload.get("status") != "success":
            blockers.append(f"{case.case_id}: status={payload.get('status')!r}, expected 'success'")
        blockers.extend(no_fallback_blockers(payload, case.case_id))
        try:
            fields = report_fields(payload)
        except Exception as exc:  # noqa: BLE001 - malformed evidence becomes a case blocker.
            blockers.append(f"{case.case_id}: invalid public workflow evidence: {exc}")

    if output_path is not None:
        committed = fields.get("native_vortex_result_export_all_targets_committed")
        if not (committed is True or committed == "true"):
            blockers.append(
                f"{case.case_id}: native Vortex export did not report all targets committed"
            )
        for key, expected_value in {
            "public_workflow_requested_output": f"write_{case.output_format}",
            "native_vortex_result_export_path": str(output_path),
            "native_vortex_result_export_format": case.output_format,
            "native_vortex_result_export_rows_written": str(len(expected)),
        }.items():
            if fields.get(key) != expected_value:
                blockers.append(
                    f"{case.case_id}: {key}={fields.get(key)!r}, expected {expected_value!r}"
                )
        if not output_path.exists():
            blockers.append(f"{case.case_id}: expected output artifact was not written")
        else:
            observed_output_bytes = output_path.read_bytes()
            observed_output_digest = "sha256:" + hashlib.sha256(observed_output_bytes).hexdigest()
            observed_output_digest_source = "sink_output_artifact"
            if case.expected_output_text is not None and observed_output_bytes != case.expected_output_text.encode("utf-8"):
                blockers.append(f"{case.case_id}: output artifact does not match decoded reference")
    elif payload and fields:
        try:
            observed_rows = extract_result(payload)
            if not equivalent(observed_rows, expected):
                blockers.append(f"{case.case_id}: complete native rows do not match decoded reference")
            observed_output_digest = canonical_rows_digest(observed_rows)
            observed_output_digest_source = "complete_native_result_rows"
        except Exception as exc:  # noqa: BLE001 - malformed/truncated result is a case blocker.
            blockers.append(f"{case.case_id}: complete native result is invalid: {exc}")
    if observed_output_digest and observed_output_digest != expected_output_digest:
        blockers.append(f"{case.case_id}: observed output digest does not match expected")

    if matrix_row is None:
        blockers.append(f"{case.case_id}: missing matrix row")
    else:
        if matrix_row.get("support_state") not in {
            "executable",
            "property_executed",
            "fuzz_executed",
        }:
            blockers.append(
                f"{case.case_id}: support_state={matrix_row.get('support_state')} is not executable"
            )
        if case.property_seed is not None and matrix_row.get("property_seed") != case.property_seed:
            blockers.append(
                f"{case.case_id}: property_seed={matrix_row.get('property_seed')} "
                f"expected {case.property_seed}"
            )
        if case.fuzz_seed is not None and matrix_row.get("fuzz_seed") != case.fuzz_seed:
            blockers.append(
                f"{case.case_id}: fuzz_seed={matrix_row.get('fuzz_seed')} "
                f"expected {case.fuzz_seed}"
            )
        if case.fuzz_surface is not None and matrix_row.get("fuzz_surface") != case.fuzz_surface:
            blockers.append(
                f"{case.case_id}: fuzz_surface={matrix_row.get('fuzz_surface')} "
                f"expected {case.fuzz_surface}"
            )
        if matrix_row.get("decoded_reference_kind") != "jsonl_inline_reference":
            blockers.append(
                f"{case.case_id}: decoded_reference_kind={matrix_row.get('decoded_reference_kind')}"
            )

    return {
        "case_id": case.case_id,
        "kind": "sql_native_decoded_reference",
        "command": command_text(cli_args),
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "source_ref": rel(repo_root, source_path),
        "auxiliary_source_refs": [rel(repo_root, path) for path in auxiliary_refs],
        "output_ref": rel(repo_root, output_path) if output_path is not None else "",
        "decoded_reference_digest": expected_digest,
        "expected_output_digest": expected_output_digest or "",
        "expected_output_digest_source": expected_output_digest_source,
        "observed_output_digest": observed_output_digest,
        "observed_output_digest_source": observed_output_digest_source,
        "property_seed": case.property_seed,
        "fuzz_seed": case.fuzz_seed,
        "fuzz_surface": case.fuzz_surface or "",
        "selected_fields": selected_workflow_fields(fields),
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
    output_path: Path | None = None
    if case.output_format is not None:
        output_name = case.output_name or f"{case.case_id}.{case.output_format}"
        output_path = work_dir / case.case_id / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if case.preexisting_output_text is not None:
            output_path.write_text(case.preexisting_output_text, encoding="utf-8")
    cli_args = public_workflow_command(
        binary,
        statement,
        source_bindings=workflow_source_bindings(source_path),
        requested_output="collect" if output_path is None else f"write_{case.output_format}",
        output=output_path,
        allow_overwrite=output_path is not None and case.allow_overwrite,
    )
    completed = run_subprocess(repo_root=repo_root, command=cli_args)
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
    if (
        output_path is not None
        and case.preexisting_output_text is None
        and output_path.exists()
    ):
        blockers.append(f"{case.case_id}: unsupported sink wrote output artifact")
    if (
        output_path is not None
        and case.preexisting_output_text is not None
        and (
            not output_path.is_file()
            or output_path.read_bytes() != case.preexisting_output_text.encode("utf-8")
        )
    ):
        blockers.append(f"{case.case_id}: unsupported sink modified existing output artifact")
    if payload:
        if payload.get("status") not in {"error", "unsupported"}:
            blockers.append(
                f"{case.case_id}: status={payload.get('status')!r}, expected an error or unsupported result"
            )
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
    if payload:
        fields = field_map(payload)
        has_no_external_engine_evidence = bool_field(fields.get("external_engine_invoked")) is False
        fallback = payload.get("fallback")
        if isinstance(fallback, dict):
            has_no_external_engine_evidence = (
                has_no_external_engine_evidence
                or (
                    fallback.get("attempted") is False
                    and fallback.get("allowed") is False
                    and fallback.get("engine") is None
                )
            )
    else:
        has_no_external_engine_evidence = False
    if (
        "external_engine_invoked=false" not in combined
        and not has_no_external_engine_evidence
    ):
        blockers.append(f"{case.case_id}: missing no-external-engine diagnostic evidence")
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
        "command": command_text(cli_args),
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "source_ref": rel(repo_root, source_path),
        "output_ref": rel(repo_root, output_path) if output_path is not None else "",
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
    fuzz_stages = [stage for stage in stages if stage.get("fuzz_seed") is not None]
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
        "remaining_matrix_gap_status": matrix_summary["remaining_matrix_gap_status"],
        "v1_runtime_scope_status": matrix_summary["v1_runtime_scope_status"],
        "v1_expected_validator_case_count": matrix_summary["v1_expected_validator_case_count"],
        "v1_required_runtime_row_count": matrix_summary["v1_required_runtime_row_count"],
        "v1_missing_validator_case_count": matrix_summary["v1_missing_validator_case_count"],
        "v1_unexpected_required_runtime_row_count": matrix_summary[
            "v1_unexpected_required_runtime_row_count"
        ],
        "v1_support_report_row_count": matrix_summary["v1_support_report_row_count"],
        "deterministic_unsupported_scope_status": matrix_summary[
            "deterministic_unsupported_scope_status"
        ],
        "deterministic_unsupported_row_count": matrix_summary[
            "deterministic_unsupported_row_count"
        ],
        "deterministic_unsupported_oracle_row_count": matrix_summary[
            "deterministic_unsupported_oracle_row_count"
        ],
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
        "property_case_ids": [stage["case_id"] for stage in property_stages],
        "deterministic_fuzz_execution_performed": bool(fuzz_stages),
        "deterministic_fuzz_case_count": len(fuzz_stages),
        "deterministic_fuzz_seed_order": [
            stage["fuzz_seed"] for stage in fuzz_stages if stage.get("fuzz_seed") is not None
        ],
        "deterministic_fuzz_surface_order": [
            stage["fuzz_surface"] for stage in fuzz_stages if stage.get("fuzz_surface")
        ],
        "fuzz_case_ids": [stage["case_id"] for stage in fuzz_stages],
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
