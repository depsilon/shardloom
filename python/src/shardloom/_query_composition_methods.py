"""Deferred implementations for query composition methods.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def RollingFrame__aggregate(self, aggregate, column, *, alias, check, **kwargs):
    target_ref = _api._normalize_rolling_aggregate_target(
        aggregate,
        column,
        alias=alias,
        kwargs=kwargs,
    )
    if kwargs:
        return self.frame._unsupported_operation(
            f"rolling-{aggregate}",
            target_ref,
            check=check,
        )
    payload = _api._vortex_rolling_window_payload(
        column,
        output_column=alias,
        window_size=self.window,
        min_periods=self.min_periods,
        aggregate=aggregate,
        center=self.center,
    )
    if payload is None:
        return self.frame._unsupported_operation(
            f"rolling-{aggregate}",
            target_ref,
            check=check,
        )
    return self.frame._append(_api.WorkflowOperation("rolling_window", (payload,)))


def LazyFrame_join(self, other, *, on, condition, how, check):

    normalized_how = _api._normalize_join_how(how)
    if on is not None and condition is not None:
        raise ValueError("join() accepts either on= equi keys or condition=, not both")
    normalized_condition = (
        None if condition is None else _api._normalize_join_condition(condition)
    )
    if normalized_how == "cross" and normalized_condition is not None:
        raise ValueError("cross joins do not accept condition=; use filter() after join()")
    normalized_columns = (
        ()
        if on is None
        else tuple(
            _api._normalize_output_column_name(column)
            for column in _api._normalize_columns((on,))
        )
    )
    columns = ",".join(normalized_columns)
    right_uri: str
    right_summary: str
    right_operations: tuple[WorkflowOperation, ...] = ()
    right_statement: str | None = None
    right_source_local = False
    if isinstance(other, _api.LazyFrame):
        if other.client is not self.client:
            raise ValueError("joined relation belongs to another context")
        right_uri = other.source.uri
        right_summary = other.operation_summary
        right_operations = other.operations
        if self.client.transport == "native":
            right_statement = f"SELECT * FROM {other._bound_relation_source().uri}"
        elif right_operations or other.source.relation is not None:
            right_statement = other._relation_statement()
        right_source_local = other._relation_statement() is not None
        right_source_vortex = other.source.source_format == "vortex"
    else:
        right_uri = _api._require_non_empty("join right source", other)
        right_summary = right_uri
        right_source_local = _api._source_format_for_local_source_ref(right_uri) is not None
        right_source_vortex = _api._is_local_vortex_source_ref(right_uri)
    target = f"{normalized_how}:{columns}:{normalized_condition or ''}:{right_summary}"
    if (
        (
            self._relation_statement() is not None
            and (right_source_local or right_source_vortex)
        )
        and (not right_operations or right_statement is not None)
        and (normalized_columns or normalized_condition is not None or normalized_how == "cross")
    ):
        return self._append(
            _api.WorkflowOperation(
                "join",
                (
                    right_uri,
                    columns,
                    columns,
                    normalized_how,
                    "f",
                    "d",
                    normalized_condition or "",
                ),
                source_bindings=_api._predicate_sources(other, condition),
                right_statement=right_statement,
                resource_limits=_api._predicate_limits(other, condition),
                right_frame=other if isinstance(other, _api.LazyFrame) else None,
            )
        )
    if self.source.source_format == "vortex" or right_source_vortex:
        return self._unsupported_operation("native-vortex-join", target, check=check)
    return self._unsupported_operation("join", target, check=check)


def LazyFrame_resample(self, time, every, unit, *, by, origin, month_end, ambiguity):

    time_column = _api._normalize_temporal_grid_column(time, "time")
    keys = _api._normalize_temporal_grid_keys(by, time_column)
    spacing = _api._normalize_temporal_grid_int("every", every, required=True)
    if spacing <= 0:
        raise ValueError("resample spacing must be a positive integer")
    normalized_unit = _api._normalize_temporal_grid_unit(unit)
    normalized_origin = _api._normalize_temporal_grid_int("origin", origin)
    normalized_month_end, normalized_ambiguity = _api._normalize_temporal_grid_policies(
        month_end, ambiguity,
    )
    bucket = _api.col(time_column).temporal_bucket(
        spacing, normalized_unit, origin=normalized_origin,
        month_end=normalized_month_end, ambiguity=normalized_ambiguity,
    )
    bucketed = self.with_column(time_column, bucket)
    return bucketed.group_by(*keys, time_column)


def LazyFrame_gap_fill(self, time, every, unit, *, by, start, end, origin, month_end, ambiguity):

    time_column = _api._normalize_temporal_grid_column(time, "time")
    keys = _api._normalize_temporal_grid_keys(by, time_column)
    spacing = _api._normalize_temporal_grid_int("every", every, required=True)
    if spacing <= 0:
        raise ValueError("gap fill spacing must be a positive integer")
    normalized_unit = _api._normalize_temporal_grid_unit(unit)
    normalized_start = _api._normalize_temporal_grid_int("start", start)
    normalized_end = _api._normalize_temporal_grid_int("end", end)
    normalized_origin = _api._normalize_temporal_grid_int("origin", origin)
    normalized_month_end, normalized_ambiguity = _api._normalize_temporal_grid_policies(
        month_end, ambiguity,
    )
    return self._append(_api.WorkflowOperation("gap_fill", (
        time_column,
        str(spacing),
        normalized_unit,
        ",".join(keys),
        "NULL" if normalized_start is None else str(normalized_start),
        "NULL" if normalized_end is None else str(normalized_end),
        "NULL" if normalized_origin is None else str(normalized_origin),
        normalized_month_end,
        normalized_ambiguity,
    )))


def LazyFrame_aggregate(self, *expressions, check):

    values = _api._normalize_columns(expressions)
    target = ",".join(values)
    if self._can_append_scalar_aggregate():
        return self._append(_api.WorkflowOperation(
            "aggregate", values, _api._predicate_sources(expressions),
            resource_limits=_api._predicate_limits(expressions),
        ))
    if self.source.source_format == "vortex":
        return self._unsupported_operation("native-vortex-aggregate", target, check=check)
    return self._unsupported_operation("aggregate", target, check=check)


def LazyFrame_observe(self, name, *expressions, **named_expressions):
    from ._connected import identifier
    name = identifier(name, label="observation name")
    if getattr(self.client, "transport", "subprocess") != "native":
        raise ValueError("observations require the in-process native transport")
    values = list(_api._normalize_columns(expressions)) if expressions else []
    values.extend(_api._format_named_aggregate(alias, expression) for alias, expression in named_expressions.items())
    if not values:
        raise ValueError("observation requires at least one aggregate")
    if _api._predicate_sources(expressions, named_expressions):
        raise ValueError("observation metrics cannot read another relation")
    frame = self._append(_api.WorkflowOperation("observe", (name, *values),
        resource_limits=_api._predicate_limits(expressions, named_expressions)))
    self.client._native_backend_for().declaration(frame)
    return frame


def LazyFrame_agg(self, *expressions, check, **named_expressions):

    values = list(_api._normalize_columns(expressions)) if expressions else []
    target_values = list(values)
    target_values.extend(
        f"{_api._require_non_empty('aggregate name', name)}={_api._require_non_empty('aggregate expression', expression)}"
        for name, expression in named_expressions.items()
    )
    values.extend(
        _api._format_named_aggregate(name, expression)
        for name, expression in named_expressions.items()
    )
    if not values:
        raise ValueError("aggregate expressions must not be empty")
    if self._can_append_scalar_aggregate():
        return self._append(_api.WorkflowOperation(
            "aggregate", tuple(values),
            _api._predicate_sources(expressions, named_expressions),
            resource_limits=_api._predicate_limits(expressions, named_expressions),
        ))
    if self.source.source_format == "vortex":
        return self._unsupported_operation(
            "native-vortex-aggregate",
            ",".join(target_values),
            check=check,
        )
    return self._unsupported_operation("agg", ",".join(target_values), check=check)


def LazyFrame_sort(self, *columns, descending, nulls, check):

    normalized_columns = _api._normalize_columns(columns)
    direction = "desc" if descending else "asc"
    null_ordering = _api._normalize_sort_nulls(nulls)
    target = f"{direction}:{','.join(normalized_columns)}"
    if null_ordering is not None:
        target = f"{target}:nulls_{null_ordering}"
    if self._can_append_sort(normalized_columns):
        return self._append(
            _api.WorkflowOperation(
                "sort",
                _api._format_sort_operation_values(
                    direction,
                    normalized_columns,
                    null_ordering,
                ),
                _api._predicate_sources(columns),
                resource_limits=_api._predicate_limits(columns),
            )
        )
    return self._unsupported_operation("sort", target, check=check)


def LazyFrame_display(self, limit, *, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    _api._validate_positive_row_count("display limit", limit)
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api.WorkflowNotebookPreview(
        workflow=self,
        smoke_report=report,
        limit=limit,
    )


def LazyFrame__set_operation(self, other, *, operation, keyword, check):
    if isinstance(other, _api.LazyFrame):
        if getattr(self.client, "transport", "subprocess") == "native":
            frame = _api.LazyFrame.from_sql(
                f"SELECT * FROM __sl_left {keyword} SELECT * FROM __sl_right",
                self.client, relations={"__sl_left": self, "__sl_right": other},
                resources=self.resources, resource_limits=_api._predicate_limits(self, other),
            )
            if self.source._session is not None:
                from .session import _session_frame
                frame = _session_frame(self.source._session, frame)
            return frame
        left = self._sql_local_source_union_branch_statement()
        right = other._sql_local_source_union_branch_statement()
        if left is None and (complete_left := self._relation_statement()) is not None:
            left = f"SELECT * FROM ({complete_left}) AS _sl_set_left"
        if right is None and (complete_right := other._relation_statement()) is not None:
            right = f"SELECT * FROM ({complete_right}) AS _sl_set_right"
        if left is not None and right is not None:
            frame = _api.LazyFrame.from_sql(
                statement=f"{left} {keyword} {right}",
                client=self.client,
                source_bindings=(*self._declared_sources(), *other._declared_sources()),
                resources=self.resources, resource_limits=_api._predicate_limits(self, other),
            )
            if self.source._session is not None:
                from .session import _session_frame
                frame = _session_frame(self.source._session, frame)
            return frame
        target = f"{self.operation_summary};{other.operation_summary}"
    else:
        target = str(other)
    return self._unsupported_operation(operation, target, check=check)


def LazyFrame__unsupported_operation(self, operation, target_ref, *, check):
    operation = self._native_vortex_blocker_operation(operation, target_ref)
    envelope = self.client.workflow_unsupported_plan(
        operation,
        self.operation_summary,
        target_ref,
        check=check,
    )
    return _api.UnsupportedWorkflowOperationReport(
        workflow=self,
        operation=operation,
        envelope=envelope,
    )


def LazyFrame__native_vortex_blocker_operation(self, operation, target_ref):
    if self.source.source_format != "vortex":
        return operation
    normalized = operation.replace("_", "-")
    if normalized.startswith("native-vortex-"):
        return normalized
    if normalized in {
        "write-vortex",
        "write-parquet",
        "write-arrow-ipc",
        "write-arrow",
        "write-ipc",
        "write-avro",
        "write-orc",
        "write-jsonl",
        "write-csv",
        "fanout",
    }:
        return "native-vortex-sink"
    if normalized in {"join", "merge"} or any(
        op.kind == "join" for op in self.operations
    ):
        return "native-vortex-join"
    if normalized in {"aggregate", "aggregation", "agg", "group-by", "groupby"} or any(
        op.kind in {"aggregate", "group_by"} for op in self.operations
    ):
        return "native-vortex-aggregate"
    if normalized in {"nlargest", "nsmallest"} or _api._workflow_has_top_n_shape(
        self.operations
    ):
        return "native-vortex-top-n"
    if normalized in {"astype", "cast", "try-cast"} or any(
        op.kind == "with_column"
        and any(_api._sql_text_looks_like_cast(value) for value in op.values)
        for op in self.operations
    ):
        return "native-vortex-cast"
    if target_ref is not None and _api._sql_text_looks_like_cast(target_ref):
        return "native-vortex-cast"
    if any(
        op.kind == "filter" and _api._sql_filter_looks_like_substring_contains(op.values[0])
        for op in self.operations
    ):
        return "native-vortex-string-contains"
    return operation


def GroupedLazyFrame_agg(self, *expressions, check, **named_expressions):

    values = list(_api._normalize_columns(expressions)) if expressions else []
    target_values = list(values)
    target_values.extend(
        f"{_api._require_non_empty('aggregate name', name)}={_api._require_non_empty('aggregate expression', expression)}"
        for name, expression in named_expressions.items()
    )
    values.extend(
        _api._format_named_aggregate(name, expression)
        for name, expression in named_expressions.items()
    )
    if not values:
        raise ValueError("aggregate expressions must not be empty")
    target = f"group_by:{','.join(self.columns)};agg:{','.join(target_values)}"
    source_bindings = (*self.source_bindings, *_api._predicate_sources(expressions, named_expressions))
    resource_limits = _api.merge_resource_limits(
        self.resource_limits, _api._predicate_limits(expressions, named_expressions),
    )
    if self.workflow._can_append_group_by_aggregate(self.columns):
        return self.workflow._append_group_by_aggregate(
            self.columns, tuple(values), source_bindings=source_bindings,
            resource_limits=resource_limits,
            grouping_clause=self.grouping_clause,
        )
    if self.workflow.source.source_format == "vortex":
        return self.workflow._append_group_by_aggregate(
            self.columns, tuple(values), source_bindings=source_bindings,
            resource_limits=resource_limits,
            grouping_clause=self.grouping_clause,
        )
    envelope = self.workflow.client.workflow_unsupported_plan(
        "agg",
        self.operation_summary,
        target,
        check=check,
    )
    return _api.UnsupportedWorkflowOperationReport(
        workflow=self.workflow,
        operation="agg",
        envelope=envelope,
    )
