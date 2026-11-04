"""Deferred implementations for query report views.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def WorkflowNotebookPreview_to_html(self):

    columns = _api._row_field_order(self.rows)
    if not columns:
        return "<table><tbody></tbody></table>"
    header = "".join(f"<th>{_api.html.escape(column)}</th>" for column in columns)
    body_rows = []
    for row in self.rows:
        cells = "".join(
            f"<td>{_api.html.escape(_api._display_cell(row.get(column)))}</td>"
            for column in columns
        )
        body_rows.append(f"<tr>{cells}</tr>")
    body = "".join(body_rows)
    return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"


def LazyFrame_describe(self, *columns, check, **kwargs):

    target_ref = _api._normalize_describe_target(columns, kwargs)
    if not kwargs:
        if not columns:
            return self.profile(check=check)
        normalized_columns = _api._normalize_columns(columns)
        if self.source.source_format == "vortex":
            projection_columns = self._declared_projection_columns()
            if projection_columns is None:
                return self._unsupported_operation("describe", target_ref, check=check)
            missing = tuple(
                column for column in normalized_columns if column not in projection_columns
            )
            if missing:
                return self._unsupported_operation("describe", target_ref, check=check)
        if all(_api._is_sql_identifier(column) for column in normalized_columns):
            return self.select(*normalized_columns).profile(check=check)
    return self._unsupported_operation("describe", target_ref, check=check)


def LazyFrame_profile(self, limit, *, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    _api._validate_positive_row_count("profile limit", limit)
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api.WorkflowProfileReport(
        workflow=self,
        smoke_report=report,
        schema_report=_api._workflow_schema_report(self, report),
        limit=limit,
    )


def LazyFrame_schema(self, *, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    self.client._ensure_open()
    if getattr(self.client, "transport", "subprocess") != "native":
        raise ValueError("metadata schema inspection requires the in-process native transport")
    return self.client._native_backend_for().schema(self, allocation)


def LazyFrame_validate_schema(self, schema, *, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    normalized = _api._normalize_schema(schema)
    if not normalized:
        raise ValueError("schema validation contract must not be empty")
    report = self._bounded_schema_report(check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._validate_workflow_schema(report, normalized)


def LazyFrame_data_quality_check(self, *checks, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    normalized_checks = _api._normalize_columns(checks)
    parsed_checks = _api._parse_data_quality_checks(normalized_checks)
    if parsed_checks is not None:
        report = self._bounded_schema_report(check=check, resources=allocation)
        if isinstance(report, _api.UnsupportedWorkflowOperationReport):
            return report
        return _api._workflow_data_quality_report(report, parsed_checks)
    return self._unsupported_operation(
        "data-quality",
        ",".join(normalized_checks),
        check=check,
    )


def LazyFrame_data_quality_summary(self, *, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    report = self._bounded_schema_report(check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api.WorkflowDataQualityReport(schema_report=report)


def LazyFrame_quarantine(self, target_uri, *checks, output_format, limit, allow_overwrite, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    _api._validate_positive_row_count("quarantine limit", limit)
    parsed_checks: tuple[_WorkflowDataQualityCheckSpec, ...] | None = None
    if checks:
        normalized_checks = _api._normalize_columns(checks)
        parsed_checks = _api._parse_data_quality_checks(normalized_checks)
        if parsed_checks is None:
            return self._unsupported_operation(
                "quarantine",
                ",".join(normalized_checks),
                check=check,
            )
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    schema_report = _api._workflow_schema_report(self, report)
    parsed_checks = parsed_checks or _api._workflow_quarantine_checks(schema_report, ())
    quality_report = _api._workflow_data_quality_report(schema_report, parsed_checks)
    rows = _api._workflow_quarantine_rows(schema_report, parsed_checks)
    normalized_output_format = _api._normalize_optional_quarantine_output_format(
        target_uri,
        output_format,
    )
    sink_report: Result | None = None
    if target_uri is not None and rows:
        pushdown_statement = self._quarantine_pushdown_statement(
            parsed_checks,
            limit=limit,
        )
        if pushdown_statement is not None and normalized_output_format is not None:
            sink = _api.LazyFrame.from_sql(
                statement=pushdown_statement,
                client=self.client,
                source_bindings=self._declared_sources(),
                resources=self.resources, resource_limits=self.resource_limits,
            ).write(
                target_uri,
                output_format=normalized_output_format,
                allow_overwrite=allow_overwrite,
                check=check,
                resources=allocation,
            )
            if sink.envelope.status == "success":
                sink_report = sink
    return _api.WorkflowQuarantineReport(
        workflow=self,
        quality_report=quality_report,
        checks=tuple(spec.raw for spec in parsed_checks),
        rows=rows,
        limit=limit,
        target_uri=None if target_uri is None else str(target_uri),
        output_format=normalized_output_format,
        sink_report=sink_report,
    )


def LazyFrame__projection_columns_for_schema_or_explicit_selection(self, *, allowed_operations):
    if self.source.schema:
        return self._declared_projection_columns(allowed_operations=allowed_operations)
    projection_columns: tuple[str, ...] | None = None
    for operation in self.operations:
        if operation.kind == "select":
            if any(not _api._is_sql_identifier(value) for value in operation.values):
                return None
            projection_columns = operation.values
        elif operation.kind == "set_index":
            continue
        elif allowed_operations is not None and operation.kind in allowed_operations:
            continue
        else:
            return None
    if not projection_columns:
        return None
    return projection_columns


def LazyFrame__schema_declared_rename_projection(self, items):
    projection_columns = self._schema_declared_projection_columns()
    if projection_columns is None:
        return None
    rename_map = dict(items)
    if any(not _api._is_sql_identifier(source) for source in rename_map):
        raise ValueError("rename source column names admit only bare SQL identifiers")
    missing = tuple(source for source in rename_map if source not in projection_columns)
    if missing:
        raise ValueError(
            "rename referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    output_names = tuple(rename_map.get(column, column) for column in projection_columns)
    if len(set(output_names)) != len(output_names):
        raise ValueError("rename output column names must be unique")
    return tuple(
        column if column == output_name else f"{column} AS {output_name}"
        for column, output_name in zip(projection_columns, output_names)
    )


def LazyFrame__schema_declared_drop_projection(self, columns):
    projection_columns = self._schema_declared_projection_columns()
    if projection_columns is None:
        return None
    if any(not _api._is_sql_identifier(column) for column in columns):
        return None
    missing = tuple(column for column in columns if column not in projection_columns)
    if missing:
        raise ValueError(
            "drop referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    remaining = tuple(column for column in projection_columns if column not in set(columns))
    if not remaining:
        raise ValueError("drop must leave at least one projected column")
    return remaining


def LazyFrame__schema_declared_fillna_projection(self, value):
    projection_columns = self._schema_declared_projection_columns()
    if projection_columns is None or value is None:
        return None
    if isinstance(value, _api.Mapping):
        if not value:
            return None
        fill_values: dict[str, str] = {}
        for raw_column, raw_value in value.items():
            column = _api._require_non_empty("fillna column", raw_column)
            if not _api._is_sql_identifier(column):
                raise ValueError("fillna column names admit only bare SQL identifiers")
            fill_literal = _api._sql_fillna_literal(raw_value)
            if fill_literal is None:
                return None
            fill_values[column] = fill_literal
        missing = tuple(column for column in fill_values if column not in projection_columns)
        if missing:
            raise ValueError(
                "fillna referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
    else:
        fill_literal = _api._sql_fillna_literal(value)
        if fill_literal is None:
            return None
        fill_values = {column: fill_literal for column in projection_columns}
    return tuple(
        f"COALESCE({column}, {fill_values[column]}) AS {column}"
        if column in fill_values
        else column
        for column in projection_columns
    )


def LazyFrame__schema_declared_null_mask_projection(self, columns, *, is_not):
    projection_columns = self._schema_declared_projection_columns()
    if projection_columns is None and not columns:
        return None
    target_columns = (
        _api._normalize_columns(columns)
        if columns
        else projection_columns
    )
    if any(not _api._is_sql_identifier(column) for column in target_columns):
        return None
    missing = (
        tuple(column for column in target_columns if column not in projection_columns)
        if projection_columns is not None
        else ()
    )
    if missing:
        raise ValueError(
            "null-mask referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    null_operator = "IS NOT NULL" if is_not else "IS NULL"
    return tuple(f"{column} {null_operator} AS {column}" for column in target_columns)


def LazyFrame__schema_declared_melt_value_columns(self, id_vars):
    projection_columns = self._unary_projection_columns()
    if projection_columns is None or projection_columns == ("*",):
        return None
    try:
        id_columns = _api._normalize_optional_columns(id_vars)
    except (TypeError, ValueError):
        return None
    if any(not _api._is_sql_identifier(column) for column in id_columns):
        return None
    missing = tuple(column for column in id_columns if column not in projection_columns)
    if missing:
        raise ValueError(
            "melt id_vars referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    value_columns = tuple(
        column for column in projection_columns if column not in set(id_columns)
    )
    if not value_columns:
        return None
    return value_columns


def LazyFrame__schema_declared_dropna_predicate(self, subset, *, how, axis, thresh, kwargs):
    if kwargs:
        return None
    if _api._normalize_dropna_axis(axis) != "rows":
        return None
    if any(operation.kind == "limit" for operation in self.operations):
        return None
    normalized_how = _api._normalize_dropna_how(how)
    projection_columns = self._schema_declared_projection_columns()
    if projection_columns is None and subset is None:
        return None
    target_columns = _api._normalize_columns((subset,)) if subset is not None else projection_columns
    if any(not _api._is_sql_identifier(column) for column in target_columns):
        return None
    missing = (
        tuple(column for column in target_columns if column not in projection_columns)
        if projection_columns is not None
        else ()
    )
    if missing:
        raise ValueError(
            "dropna referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    if thresh is not None:
        threshold = _api._normalize_non_negative_int("dropna thresh", thresh)
        if threshold == 0:
            return ""
        return _api._dropna_threshold_predicate(target_columns, threshold)
    joiner = " AND " if normalized_how == "any" else " OR "
    return joiner.join(f"{column} IS NOT NULL" for column in target_columns)


def LazyFrame__schema_declared_astype_projection(self, dtype, *, errors, kwargs):
    projection_columns = self._schema_declared_projection_columns()
    if projection_columns is None or kwargs:
        return None
    normalized_errors = _api._normalize_astype_errors(errors)
    if normalized_errors != "raise":
        return None
    dtype_map = _api._normalize_astype_dtype_map(dtype, projection_columns)
    if dtype_map is None:
        return None
    missing = tuple(column for column in dtype_map if column not in projection_columns)
    if missing:
        raise ValueError(
            "astype referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    return tuple(
        f"CAST({column} AS {dtype_map[column]}) AS {column}"
        if column in dtype_map
        else column
        for column in projection_columns
    )


def LazyFrame__bounded_schema_report(self, *, check, memory_gb, memory_bytes, max_parallelism, resources):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    report = self._bounded_materialization_report(limit=100, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._workflow_schema_report(self, report)


def LazyFrame__quarantine_pushdown_statement(self, checks, *, limit):
    predicate = _api._quarantine_pushdown_predicate(checks)
    if predicate is None:
        return None
    if _api._workflow_has_offset(self):
        # The legacy quarantine helper rebuilt source/filter stages and
        # could not preserve an offset. Keep the complete relation as the
        # input to the quarantine filter instead of dropping that stage.
        relation = self._relation_statement()
        if relation is None:
            return None
        return (
            f"SELECT * FROM ({relation}) AS _sl_quarantine "
            f"WHERE {predicate} LIMIT {limit}"
        )
    operations: list[WorkflowOperation] = []
    filters: list[str] = []
    saw_select = False
    for operation in self.operations:
        if operation.kind == "select" and not saw_select:
            operations.append(operation)
            saw_select = True
        elif operation.kind == "filter":
            filters.append(operation.values[0])
        elif operation.kind == "limit":
            continue
        else:
            return None
    filters.append(predicate)
    operations.append(
        _api.WorkflowOperation(
            "filter",
            (" AND ".join(f"({value})" for value in filters),),
        )
    )
    operations.append(_api.WorkflowOperation("limit", (str(limit),)))
    return _api.LazyFrame(
        source=self.source,
        client=self.client,
        operations=tuple(operations),
        engine_mode=self.engine_mode,
        resources=self.resources, resource_limits=self.resource_limits,
    )._relation_statement()
