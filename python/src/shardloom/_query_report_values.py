"""Deferred implementations for query report values.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _sql_identifier_rewrite_boundary(statement, start, length):
    before = statement[start - 1] if start > 0 else ""
    after_index = start + length
    after = statement[after_index] if after_index < len(statement) else ""
    return (
        not _api._is_identifier_char(before)
        and not _api._is_identifier_char(after)
        and before != "."
        and after != "."
    )


def _workflow_schema_report(workflow, smoke_report):
    rows = smoke_report.result_rows
    declared = dict(workflow.source.schema)
    fields = tuple(
        _api.WorkflowSchemaField(
            name=name, dtype=dtype.label, nullable=dtype.nullable,
            declared_dtype=declared.get(name),
            observed_non_null_count=_api.builtins.sum(row[name] is not None for row in rows),
            null_count=_api.builtins.sum(row[name] is None for row in rows),
        ) for name, dtype in smoke_report.result_schema
    )
    return _api.WorkflowSchemaReport(workflow=workflow, smoke_report=smoke_report, fields=fields)


def _infer_python_scalar_dtype(value):
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    if isinstance(value, str):
        return "utf8"
    if value is None:
        return "null"
    return "json"


def _normalize_schema_dtype_token(value):
    if value is None:
        return None
    if temporal := _api.parse_temporal_declaration(value):
        return temporal
    normalized = value.strip().lower().replace("-", "_")
    aliases = {
        "boolean": "bool",
        "bool": "bool",
        "int": "int64",
        "integer": "int64",
        "i64": "int64",
        "int64": "int64",
        "long": "int64",
        "float": "float64",
        "double": "float64",
        "f64": "float64",
        "float64": "float64",
        "str": "utf8",
        "string": "utf8",
        "utf8": "utf8",
        "date": "date32",
        "date32": "date32",
        "timestamp": "timestamp_micros",
        "timestamp_micros": "timestamp_micros",
    }
    return aliases.get(normalized, normalized)


def _validate_workflow_schema(report, expected_schema):
    observed = report.schema_map
    expected = {
        name: _api._normalize_schema_dtype_token(dtype) or dtype
        for name, dtype in expected_schema
    }
    missing_fields = tuple(name for name in expected if name not in observed)
    unexpected_fields = tuple(name for name in observed if name not in expected)
    mismatches: list[WorkflowSchemaMismatch] = []
    for name, expected_dtype in expected.items():
        observed_dtype = observed.get(name)
        if observed_dtype is not None and observed_dtype != expected_dtype:
            mismatches.append(
                _api.WorkflowSchemaMismatch(
                    field=name,
                    expected_dtype=expected_dtype,
                    observed_dtype=observed_dtype,
                )
            )
    return _api.WorkflowSchemaValidationReport(
        schema_report=report,
        expected_schema=expected_schema,
        missing_fields=missing_fields,
        unexpected_fields=unexpected_fields,
        dtype_mismatches=tuple(mismatches),
    )


def _parse_data_quality_checks(checks):
    if not checks:
        raise ValueError("data-quality checks must not be empty")
    parsed: list[_WorkflowDataQualityCheckSpec] = []
    for check in checks:
        parts = check.split(":", 2)
        if len(parts) < 2:
            return None
        kind = parts[0].strip().lower().replace("-", "_").replace(" ", "_")
        column = parts[1].strip()
        if not column:
            return None
        if kind in {"not_null", "non_null", "required"}:
            if len(parts) != 2:
                return None
            parsed.append(_api._WorkflowDataQualityCheckSpec("not_null", column, check))
        elif kind == "unique":
            if len(parts) != 2:
                return None
            parsed.append(_api._WorkflowDataQualityCheckSpec("unique", column, check))
        elif kind in {"regex", "matches"}:
            if len(parts) != 3:
                return None
            pattern = parts[2].strip()
            if not pattern:
                return None
            try:
                _api.re.compile(pattern)
            except _api.re.error:
                return None
            parsed.append(_api._WorkflowDataQualityCheckSpec("regex", column, check, pattern))
        else:
            return None
    return tuple(parsed)


def _workflow_data_quality_report(schema_report, checks):
    rows = schema_report.smoke_report.result_rows
    field_names = set(schema_report.field_names)
    schema_map = schema_report.schema_map
    results: list[WorkflowDataQualityCheckResult] = []
    for spec in checks:
        kind = spec.kind
        column = spec.column
        raw_check = spec.raw
        if column not in field_names:
            results.append(
                _api.WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=False,
                    failing_row_count=len(rows),
                    message=f"column {column!r} was not observed",
                )
            )
            continue
        if kind == "not_null":
            failing = sum(1 for row in rows if row.get(column) is None)
            results.append(
                _api.WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=failing == 0,
                    failing_row_count=failing,
                    message="all rows are non-null" if failing == 0 else "null values observed",
                )
            )
            continue
        if kind == "unique":
            seen: set[str] = set()
            duplicate_count = 0
            for row in rows:
                key = _api._stable_quality_value_key(row.get(column))
                if key in seen:
                    duplicate_count += 1
                else:
                    seen.add(key)
            results.append(
                _api.WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=duplicate_count == 0,
                    failing_row_count=duplicate_count,
                    message="all values are unique"
                    if duplicate_count == 0
                    else "duplicate values observed",
                )
            )
            continue
        if kind == "regex":
            if schema_map.get(column) != "utf8":
                results.append(
                    _api.WorkflowDataQualityCheckResult(
                        check=raw_check,
                        column=column,
                        passed=False,
                        failing_row_count=len(rows),
                        message="regex data-quality checks require utf8 values",
                    )
                )
                continue
            pattern = _api.re.compile(spec.pattern or "")
            failing = sum(
                1
                for row in rows
                if not isinstance(row.get(column), str) or pattern.search(row[column]) is None
            )
            results.append(
                _api.WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=failing == 0,
                    failing_row_count=failing,
                    message="all values match regex"
                    if failing == 0
                    else "regex mismatches or null values observed",
                )
            )
    return _api.WorkflowDataQualityReport(
        schema_report=schema_report,
        checks=tuple(results),
    )
