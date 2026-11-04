"""Deferred implementations for query statement.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def LazyFrame__relation_statement(self):
    from ._relational_sql import flat_order_is_safe, render_frame

    if self.source.relation is not None:
        from ._relational_sql import render_stages
        return render_stages(self.source.relation.statement, self.operations)

    if flat_order_is_safe(self.operations):
        statement = self._sql_local_source_statement(allow_native_source=True, require_limit=False)
        if statement is not None:
            return statement
    return render_frame(self)


def LazyFrame__sql_local_source_statement(self, *, default_limit, allow_native_source, require_limit):
    if not _api._is_declared_local_source(self.source) and not (
        allow_native_source and (
            self.source.source_format == "vortex" or _api._is_declared_local_source(self.source)
        )
    ):
        return None
    projection_list: tuple[str, ...] | None = None
    aggregate_list: tuple[str, ...] | None = None
    group_by_list: tuple[str, ...] | None = None
    literal_columns: list[tuple[str, str]] = []
    window_expressions: list[str] = []
    join_info: tuple[str, ...] | None = None
    sort_key: tuple[str, tuple[str, ...], str | None] | None = None
    distinct_requested = False
    predicate: str | None = None
    having: str | None = None
    limit: str | None = None
    limit_offset = "0"
    for operation in self.operations:
        if operation.kind == "select" and projection_list is None:
            if window_expressions or distinct_requested:
                return None
            if any(
                not _api._sql_fragment_admitted_for_local_source_statement(
                    "select projection", value
                )
                for value in operation.values
            ):
                return None
            projection_list = operation.values
        elif operation.kind == "aggregate" and aggregate_list is None:
            if distinct_requested:
                return None
            if any(
                not _api._sql_fragment_admitted_for_local_source_statement(
                    "aggregate expression", value
                )
                for value in operation.values
            ):
                return None
            aggregate_list = operation.values
        elif operation.kind == "group_by" and group_by_list is None:
            if operation.grouping_clause is not None:
                return None  # The stage renderer owns grouping-set grammar.
            if distinct_requested:
                return None
            if any(
                not _api._sql_fragment_admitted_for_local_source_statement(
                    "GROUP BY column", value
                )
                for value in operation.values
            ):
                return None
            group_by_list = operation.values
        elif operation.kind == "with_column":
            if window_expressions or distinct_requested:
                return None
            if not _api._sql_fragment_admitted_for_local_source_statement(
                "computed-column expression", operation.values[1]
            ):
                return None
            literal_columns.append((operation.values[0], operation.values[1]))
        elif operation.kind == "window":
            if (
                aggregate_list is not None
                or group_by_list is not None
                or literal_columns
                or join_info is not None
                or sort_key is not None
                or distinct_requested
                or having is not None
                or limit is not None
            ):
                return None
            if any(
                not _api._sql_fragment_admitted_for_local_source_statement(
                    "window expression",
                    value,
                    allowed_keywords=("order by",),
                )
                for value in operation.values
            ):
                return None
            window_expressions.extend(operation.values)
        elif operation.kind == "sort" and sort_key is None:
            sort_key = _api._parse_sort_operation_values(operation.values)
            if any(
                not _api._sql_fragment_admitted_for_local_source_statement(
                    "ORDER BY column", value
                )
                for value in sort_key[1]
            ):
                return None
        elif operation.kind == "join" and join_info is None:
            if operation.right_statement is not None:
                return None
            if (aggregate_list is not None or group_by_list is not None or distinct_requested
                    or predicate is not None or sort_key is not None or limit is not None
                    or projection_list is not None or literal_columns or window_expressions):
                # This flat SQL renderer cannot move a preceding input stage
                # across a join; doing so can change rows and null extension.
                return None
            join_info = operation.values  # type: ignore[assignment]
        elif operation.kind == "distinct" and not distinct_requested:
            if limit is not None:
                return None
            distinct_requested = True
        elif operation.kind == "filter" and predicate is None:
            if (
                aggregate_list is not None
                or group_by_list is not None
                or distinct_requested
                or having is not None
                or sort_key is not None
                or window_expressions
                or limit is not None
            ):
                return None
            predicate = _api._rewrite_predicate_with_computed_columns(
                operation.values[0],
                tuple(literal_columns),
            )
        elif operation.kind == "having" and having is None:
            if aggregate_list is None or sort_key is not None or limit is not None:
                return None
            having = operation.values[0]
        elif operation.kind == "limit" and limit is None:
            limit_parts = _api._limit_operation_parts(operation.values)
            if limit_parts is None:
                return None
            limit, limit_offset = limit_parts
        else:
            return None
    if limit is None:
        if default_limit is None and require_limit:
            return None
        if default_limit is not None:
            limit = str(default_limit)
            limit_offset = "0"
    limit_clause = (
        f" LIMIT {limit}"
        + (f" OFFSET {limit_offset}" if limit is not None and limit_offset != "0" else "")
        if limit is not None
        else ""
    )
    if group_by_list is not None and aggregate_list is None:
        return None
    if join_info is not None:
        if len(join_info) == 6:
            right_uri, left_key, right_key, how, left_alias, right_alias = join_info
            join_condition = ""
        elif len(join_info) == 7:
            (
                right_uri,
                left_key,
                right_key,
                how,
                left_alias,
                right_alias,
                join_condition,
            ) = join_info
        else:
            return None
        left_keys = tuple(column for column in left_key.split(",") if column)
        right_keys = tuple(column for column in right_key.split(",") if column)
        if how == "cross":
            if left_keys or right_keys or join_condition:
                return None
            on_clause = ""
        elif join_condition:
            if left_keys or right_keys:
                return None
            on_clause = f" ON {join_condition}"
        elif len(left_keys) != len(right_keys) or not left_keys:
            return None
        else:
            on_clause = " ON " + " AND ".join(
                f"{left_alias}.{left_column} = {right_alias}.{right_column}"
                for left_column, right_column in zip(left_keys, right_keys)
            )
        if aggregate_list is not None:
            if projection_list is not None or literal_columns or window_expressions:
                return None
            if group_by_list is not None:
                select_clause = ",".join((*group_by_list, *aggregate_list))
                group_by_clause = f" GROUP BY {','.join(group_by_list)}"
            else:
                select_clause = ",".join(aggregate_list)
                group_by_clause = ""
        else:
            if (
                projection_list is None
                or group_by_list is not None
                or having is not None
                or window_expressions
            ):
                return None
            select_values = list(projection_list)
            select_values.extend(
                f"{literal} AS {column}" for column, literal in literal_columns
            )
            select_clause = ",".join(select_values)
            group_by_clause = ""
        order_by_clause = ""
        if sort_key is not None:
            direction, columns, null_ordering = sort_key
            order_by_clause = _api._format_order_by_clause(
                columns,
                direction,
                null_ordering,
            )
        source_uri = _api._quote_sql_local_source_path(self.source.uri)
        right_source_uri = _api._quote_sql_local_source_path(right_uri)
        join_keyword = _api._sql_join_keyword(how)
        select_keyword = "SELECT DISTINCT" if distinct_requested else "SELECT"
        return (
            f"{select_keyword} {select_clause} FROM {source_uri} AS {left_alias} "
            f"{join_keyword} {right_source_uri} AS {right_alias}"
            f"{on_clause}"
            f"{_api._optional_sql_where_clause(predicate)}{group_by_clause}"
            f"{_api._optional_sql_having_clause(having)}{order_by_clause}{limit_clause}"
        )
    if projection_list is not None:
        if aggregate_list is not None or group_by_list is not None:
            return None
        select_values = list(projection_list)
        select_values.extend(
            f"{literal} AS {column}" for column, literal in literal_columns
        )
        select_values.extend(window_expressions)
        select_clause = ",".join(select_values)
        group_by_clause = ""
    elif aggregate_list is not None:
        if literal_columns or window_expressions:
            return None
        if group_by_list is not None:
            select_clause = ",".join((*group_by_list, *aggregate_list))
            group_by_clause = f" GROUP BY {','.join(group_by_list)}"
        else:
            select_clause = ",".join(aggregate_list)
            group_by_clause = ""
    else:
        if having is not None:
            return None
        if literal_columns or window_expressions:
            select_values = ["*"]
            select_values.extend(
                f"{literal} AS {column}" for column, literal in literal_columns
            )
            select_values.extend(window_expressions)
            select_clause = ",".join(select_values)
        else:
            select_clause = "*"
        group_by_clause = ""
    order_by_clause = ""
    if sort_key is not None:
        direction, columns, null_ordering = sort_key
        order_by_clause = _api._format_order_by_clause(columns, direction, null_ordering)
    source_uri = _api._quote_sql_local_source_path(self.source.uri)
    select_keyword = "SELECT DISTINCT" if distinct_requested else "SELECT"
    return (
        f"{select_keyword} {select_clause} FROM {source_uri}"
        f"{_api._optional_sql_where_clause(predicate)}{group_by_clause}"
        f"{_api._optional_sql_having_clause(having)}{order_by_clause}{limit_clause}"
    )


def LazyFrame__sql_local_source_union_branch_statement(self):
    from ._relational_sql import flat_order_is_safe

    if not flat_order_is_safe(self.operations):
        return None
    if any(operation.kind in {"limit", "sort"} for operation in self.operations):
        return None
    statement = self._sql_local_source_statement(default_limit=1, allow_native_source=True)
    suffix = " LIMIT 1"
    if statement is None or not statement.endswith(suffix):
        return None
    return statement[: -len(suffix)]
