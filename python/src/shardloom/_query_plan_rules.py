"""Admission rules for ordinary immutable query declarations."""

from __future__ import annotations

from . import query as _api


def LazyFrame__can_append_scalar_aggregate(self):
    if self._relation_statement() is not None:
        return True
    if _api._workflow_has_offset(self):
        return False
    if not _api._is_declared_local_source(self.source) and self.source.source_format != "vortex":
        return False
    if self.source.source_format == "vortex" and any(
        operation.kind == "limit" for operation in self.operations
    ):
        return False
    return all(
        operation.kind not in {"select", "aggregate", "group_by", "sort"}
        for operation in self.operations
    )


def LazyFrame__can_append_group_by_aggregate(self, columns):
    if self._relation_statement() is not None:
        return True
    if _api._workflow_has_offset(self):
        return False
    if not _api._is_declared_local_source(self.source):
        return False
    return all(
        operation.kind not in {"select", "aggregate", "group_by", "sort"}
        for operation in self.operations
    )


def LazyFrame__explicit_set_operation_projection_columns(self):
    projection: tuple[str, ...] | None = None
    for operation in self.operations:
        if operation.kind == "filter":
            continue
        if operation.kind == "select" and projection is None:
            if any(not _api._is_sql_identifier(column) for column in operation.values):
                return None
            projection = operation.values
            continue
        return None
    return projection


def LazyFrame__can_append_sort(self, columns):
    if not columns:
        return False
    if len(set(columns)) != len(columns):
        return False
    if self._relation_statement() is not None:
        return True
    if any(operation.kind == "limit" for operation in self.operations):
        return False
    if _api._is_declared_local_source(self.source):
        return all(operation.kind != "sort" for operation in self.operations)
    if self.source.source_format == "vortex":
        return all(
            operation.kind in {"filter", "select", "set_index", "group_by", "aggregate", "having", "window", "join"}
            for operation in self.operations
        )
    return False


def LazyFrame__can_append_window(self, expressions):
    if expressions and self._relation_statement() is not None:
        return True
    if (not (_api._is_declared_local_source(self.source) or self.source.source_format == "vortex")
            or not expressions):
        return False
    for operation in self.operations:
        if operation.kind in {"select", "filter", "window"}:
            continue
        return False
    return True


def LazyFrame__can_append_having(self):
    if self._relation_statement() is not None:
        return bool(self.operations and self.operations[-1].kind == "aggregate")
    if not _api._is_declared_local_source(self.source) and self.source.source_format != "vortex":
        return False
    saw_aggregate = False
    for operation in self.operations:
        if operation.kind == "aggregate":
            saw_aggregate = True
            continue
        if saw_aggregate and operation.kind in {"filter", "having", "sort", "limit"}:
            return False
    return saw_aggregate


def LazyFrame__can_append_projection_column(self, column_name, *, allow_vortex):
    if self._relation_statement() is not None:
        return True
    if (not _api._is_declared_local_source(self.source)
            and not (allow_vortex and self.source.source_format == "vortex")):
        return False
    saw_join = False
    saw_projection = False
    for operation in self.operations:
        if operation.kind == "join":
            saw_join = True
            continue
        if operation.kind == "select":
            saw_projection = True
            if column_name in operation.values:
                return False
        elif operation.kind == "filter":
            continue
        elif operation.kind == "with_column":
            if column_name == operation.values[0]:
                return False
            continue
        elif operation.kind == "window":
            return False
        elif operation.kind == "having":
            return False
        else:
            return False
    if saw_join and not saw_projection:
        return False
    return True


def LazyFrame__can_lower_merge_to_join(self, other, *, on, how):
    if self._relation_statement() is None:
        return False
    try:
        normalized_how = _api._normalize_join_how(how)
        normalized_columns = tuple(
            _api._normalize_output_column_name(column)
            for column in _api._normalize_columns((on,))
        )
    except (TypeError, ValueError):
        return False
    if not normalized_columns:
        return False
    if isinstance(other, _api.LazyFrame):
        return other._relation_statement() is not None
    return _api._source_format_for_local_source_ref(str(other)) is not None


def LazyFrame__append_group_by_aggregate(self, columns, expressions, *, source_bindings, resource_limits, grouping_clause):
    return _api.LazyFrame(
        source=self.source,
        client=self.client,
        operations=(
            *self.operations,
            _api.WorkflowOperation("group_by", columns, grouping_clause=grouping_clause),
            _api.WorkflowOperation("aggregate", expressions, source_bindings, resource_limits=resource_limits),
        ),
        engine_mode=self.engine_mode,
        resources=self.resources, resource_limits=self.resource_limits,
    )
