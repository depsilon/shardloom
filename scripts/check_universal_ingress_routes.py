"""Validate ShardLoom UniversalIngress / vortex_ingest route taxonomy.

The taxonomy is intentionally documentation-facing, but it protects a runtime contract:
``prepared_vortex`` executes from ``VortexPreparedState`` and must not be modeled as reading
non-Vortex sources directly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TAXONOMY_PATH = ROOT / "docs" / "architecture" / "universal-ingress-route-taxonomy.json"

REQUIRED_ALIASES = {
    "local_csv",
    "local_jsonl",
    "local_ndjson",
    "local_json",
    "local_parquet",
    "local_arrow_ipc",
    "local_avro",
    "local_orc",
    "local_excel",
    "sqlite",
    "local_database_file",
    "postgres",
    "mysql",
    "jdbc",
    "odbc",
    "snowflake",
    "bigquery",
    "databricks_sql",
    "s3",
    "gcs",
    "adls",
    "iceberg",
    "delta",
    "hudi",
    "existing_vortex",
    "local_vortex",
    "generated_source",
    "user_rows",
    "range",
    "sequence",
    "literal_table",
    "calendar",
    "python_user_rows",
    "dataframe_api_request",
    "sql_values",
    "sql_literal_select",
    "sql_generate_series",
    "rest",
    "flight",
    "adbc",
    "api_event_saas_adapter",
    "unstructured_media_reference",
    "foundry_dataset",
}

NON_SUPPORTED_STATUSES = {
    "report_only",
    "blocked",
    "unsupported",
    "not_planned",
    "report_only_optional_generated_vortex_ingest",
}


def _load_taxonomy() -> dict[str, Any]:
    return json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))


def _require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def _blocker_present(value: Any) -> bool:
    return isinstance(value, str) and value not in {"", "missing", "none"}


def validate_taxonomy(taxonomy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    _require(
        taxonomy.get("schema_version") == "shardloom.universal_ingress_route_taxonomy.v1",
        "unexpected or missing taxonomy schema_version",
        errors,
    )
    _require(taxonomy.get("fallback_attempted") is False, "taxonomy fallback_attempted must be false", errors)
    _require(
        taxonomy.get("external_engine_invoked") is False,
        "taxonomy external_engine_invoked must be false",
        errors,
    )

    rows = taxonomy.get("rows")
    _require(isinstance(rows, list) and rows, "taxonomy rows must be a non-empty list", errors)
    if not isinstance(rows, list):
        return errors

    observed_aliases: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            errors.append("taxonomy row is not an object")
            continue
        row_id = str(row.get("source_surface_id") or "unknown")
        aliases = row.get("source_aliases") or []
        if not isinstance(aliases, list) or not all(isinstance(alias, str) for alias in aliases):
            errors.append(f"{row_id}: source_aliases must be a list of strings")
            aliases = []
        observed_aliases.update(aliases)

        for field in (
            "universal_ingress_status",
            "source_adapter_status",
            "vortex_ingest_status",
            "compatibility_import_certified_status",
            "prepared_vortex_input_contract",
            "prepared_vortex_timing_scope",
            "fallback_attempted",
            "external_engine_invoked",
            "claim_gate_status",
            "claim_boundary",
        ):
            _require(field in row, f"{row_id}: missing {field}", errors)

        _require(
            row.get("prepared_vortex_direct_source_input_allowed") is False,
            f"{row_id}: prepared_vortex must not allow direct source input",
            errors,
        )
        _require(
            row.get("prepared_vortex_requires_prepared_state") is True,
            f"{row_id}: prepared_vortex must require VortexPreparedState",
            errors,
        )
        _require(row.get("fallback_attempted") is False, f"{row_id}: fallback_attempted must be false", errors)
        _require(
            row.get("external_engine_invoked") is False,
            f"{row_id}: external_engine_invoked must be false",
            errors,
        )

        for status_field, blocker_field in (
            ("universal_ingress_status", "source_adapter_blocker_id"),
            ("vortex_ingest_status", "vortex_ingest_blocker_id"),
            (
                "compatibility_import_certified_status",
                "compatibility_import_certified_blocker_id",
            ),
        ):
            status = str(row.get(status_field) or "")
            if status in NON_SUPPORTED_STATUSES:
                _require(
                    _blocker_present(row.get(blocker_field)),
                    f"{row_id}: {status_field}={status} requires {blocker_field}",
                    errors,
                )

        if row.get("source_kind") == "non_vortex_source":
            _require(
                row.get("vortex_ingest_status") == row.get("compatibility_import_certified_status"),
                f"{row_id}: non-Vortex source must project same support status to vortex_ingest and certified route",
                errors,
            )
            _require(
                row.get("compatibility_import_certified_timing_scope")
                == "cold_certified_end_to_end",
                f"{row_id}: certified route timing must be cold_certified_end_to_end",
                errors,
            )
            _require(
                row.get("prepared_vortex_timing_scope") == "warm_prepared_query",
                f"{row_id}: prepared route timing must be warm_prepared_query",
                errors,
            )

    missing_aliases = sorted(REQUIRED_ALIASES - observed_aliases)
    _require(not missing_aliases, f"missing source universe aliases: {', '.join(missing_aliases)}", errors)
    return errors


def main() -> int:
    errors = validate_taxonomy(_load_taxonomy())
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"validated {TAXONOMY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
