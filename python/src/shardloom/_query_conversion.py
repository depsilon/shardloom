"""Deferred implementations for query conversion.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _workflow_quarantine_checks(schema_report, checks):
    if checks:
        normalized_checks = _api._normalize_columns(checks)
        parsed = _api._parse_data_quality_checks(normalized_checks)
        if parsed is None:
            raise ValueError(
                "quarantine checks must use supported data-quality forms such as "
                "'not_null:column', 'unique:column', or 'regex:column:pattern'"
            )
        return parsed
    return tuple(
        _api._WorkflowDataQualityCheckSpec("not_null", field.name, f"not_null:{field.name}")
        for field in schema_report.fields
    )


def _workflow_quarantine_rows(schema_report, checks):
    rows = schema_report.smoke_report.result_rows
    if not rows or not checks:
        return ()
    field_names = set(schema_report.field_names)
    unique_value_counts: dict[str, dict[str, int]] = {}
    regex_patterns = {
        spec.raw: _api.re.compile(spec.pattern or "")
        for spec in checks
        if spec.kind == "regex" and spec.pattern is not None
    }
    for spec in checks:
        if spec.kind != "unique" or spec.column not in field_names:
            continue
        counts: dict[str, int] = {}
        for row in rows:
            key = _api._stable_quality_value_key(row.get(spec.column))
            counts[key] = counts.get(key, 0) + 1
        unique_value_counts[spec.column] = counts

    quarantined: list[Mapping[str, Any]] = []
    for row in rows:
        failed = False
        for spec in checks:
            kind = spec.kind
            column = spec.column
            if column not in field_names:
                failed = True
            elif kind == "not_null":
                failed = row.get(column) is None
            elif kind == "unique":
                key = _api._stable_quality_value_key(row.get(column))
                failed = unique_value_counts.get(column, {}).get(key, 0) > 1
            elif kind == "regex":
                pattern = regex_patterns.get(spec.raw)
                value = row.get(column)
                failed = (
                    not isinstance(value, str)
                    or pattern is None
                    or pattern.search(value) is None
                )
            if failed:
                quarantined.append(row)
                break
    return tuple(quarantined)


def _quarantine_pushdown_predicate(checks):
    if not checks or any(spec.kind not in {"not_null", "regex"} for spec in checks):
        return None
    predicates = []
    for spec in checks:
        column = spec.column
        if not _api._is_sql_identifier(column):
            return None
        if spec.kind == "not_null":
            predicates.append(f"{column} IS NULL")
        elif spec.kind == "regex" and spec.pattern is not None:
            predicates.append(
                f"({column} IS NULL OR {column} NOT RLIKE {_api._sql_string_literal(spec.pattern)})"
            )
        else:
            return None
    return " OR ".join(predicates) if predicates else None


def _normalize_optional_quarantine_output_format(target_uri, output_format):
    if output_format is not None:
        return _api._normalize_local_output_format(output_format)
    if target_uri is None:
        return None
    suffix = _api.Path(str(target_uri)).suffix.lower()
    if suffix in {".vortex", ".vtx"}:
        return "vortex"
    if suffix == ".csv":
        return "csv"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".arrow", ".ipc", ".feather"}:
        return "arrow-ipc"
    if suffix == ".avro":
        return "avro"
    if suffix == ".orc":
        return "orc"
    return "jsonl"


def _read_arrow_ipc_from_seekable(source, ipc):
    try:
        return ipc.open_stream(source).read_all()
    except Exception as stream_error:
        seek = getattr(source, "seek", None)
        if callable(seek):
            seek(0)
        open_file = getattr(ipc, "open_file", None)
        if not callable(open_file):
            raise stream_error
        try:
            return open_file(source).read_all()
        except Exception:
            raise stream_error


def _normalize_local_output_format(value):
    normalized = value.strip().lower()
    if normalized in {"json", "json-array"}:
        return "json"
    if normalized in {"jsonl", "json-lines", "ndjson", "inline-jsonl"}:
        return "jsonl"
    if normalized == "csv":
        return "csv"
    if normalized == "parquet":
        return "parquet"
    if normalized in {"arrow", "arrow-ipc", "arrow_ipc", "ipc", "feather"}:
        return "arrow-ipc"
    if normalized == "avro":
        return "avro"
    if normalized == "orc":
        return "orc"
    if normalized in {"vortex", "vtx"}:
        return "vortex"
    raise ValueError(
        "scoped local writes currently support local JSON arrays, JSONL, CSV, and feature-gated "
        "Parquet/Arrow IPC/Avro/ORC/Vortex only"
    )


def _public_write_request_for_format(output_format):
    normalized = _api._normalize_local_output_format(output_format)
    return {
        "json": "write_json",
        "jsonl": "write_jsonl",
        "csv": "write_csv",
        "parquet": "write_parquet",
        "arrow-ipc": "write_arrow_ipc",
        "avro": "write_avro",
        "orc": "write_orc",
        "vortex": "write_vortex",
    }[normalized]


def _normalize_fanout_outputs(outputs):
    if isinstance(outputs, _api.Mapping):
        items = outputs.items()
    elif _api._is_non_string_sequence(outputs):
        items = outputs
    else:
        raise TypeError("fanout outputs must be a mapping or sequence of (format, path) pairs")

    normalized: list[tuple[str, CommandPart]] = []
    for item in items:
        if not _api._is_non_string_sequence(item) or len(item) != 2:
            raise ValueError("fanout outputs must contain (format, path) pairs")
        output_format, output_path = item
        if not isinstance(output_format, str):
            raise TypeError("fanout output format names must be strings")
        if not isinstance(output_path, (str, _api.os.PathLike)):
            raise TypeError("fanout output paths must be strings or path-like objects")
        normalized.append(
            (
                _api._normalize_local_output_format(output_format),
                _api._require_non_empty("fanout output path", output_path),
            )
        )
    if not normalized:
        raise ValueError("fanout outputs must not be empty")
    return tuple(normalized)
