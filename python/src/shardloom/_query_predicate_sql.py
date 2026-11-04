"""Deferred implementations for query predicate sql.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _normalize_substring_bound(name, value, *, minimum):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer literal")
    if isinstance(value, int):
        parsed = value
    else:
        text = _api._require_non_empty(name, value)
        if text in {"+", "-"} or not all(
            ch.isdigit() or (index == 0 and ch in {"+", "-"})
            for index, ch in enumerate(text)
        ):
            raise ValueError(f"{name} must be an integer literal")
        parsed = int(text)
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


def _normalize_temporal_extract_column(expression, dtype):
    text = _api._require_non_empty("temporal extract column expression", expression)
    if text.upper().startswith("CAST("):
        if not text.endswith(")"):
            raise ValueError("temporal extract CAST expression must be closed")
        inner = text[5:-1].strip()
        upper_inner = inner.upper()
        marker = " AS "
        marker_index = upper_inner.find(marker)
        if marker_index < 0:
            raise ValueError("temporal extract CAST expression must use CAST(column AS dtype)")
        column = inner[:marker_index].strip()
        target = inner[marker_index + len(marker) :].strip().lower()
        if target == "timestamp":
            target = "timestamp_micros"
        if target != dtype:
            raise ValueError(f"temporal extract CAST target must be {dtype}")
        return f"CAST({_api._normalize_expression_column(column)} AS {dtype})"
    return _api._normalize_expression_column(text)


def _normalize_nullable_projection_column(expression):
    text = _api._require_non_empty("COALESCE column expression", expression)
    if text.upper().startswith("CAST("):
        if not text.endswith(")"):
            raise ValueError("COALESCE CAST expression must be closed")
        inner = text[5:-1].strip()
        upper_inner = inner.upper()
        marker = " AS "
        marker_index = upper_inner.find(marker)
        if marker_index < 0:
            raise ValueError("COALESCE CAST expression must use CAST(column AS dtype)")
        column = _api._normalize_expression_column(inner[:marker_index].strip())
        dtype = _api._normalize_cast_dtype(inner[marker_index + len(marker) :].strip())
        if dtype not in {"date32", "timestamp_micros"}:
            raise ValueError("COALESCE CAST target must be date32 or timestamp_micros")
        return f"CAST({column} AS {dtype})"
    return _api._normalize_expression_column(text)


def _parse_numeric_literal_token(value):
    try:
        return int(value)
    except ValueError:
        try:
            parsed = float(value)
        except ValueError as exc:
            raise ValueError("numeric arithmetic projection literal must be numeric") from exc
        if not _api.math.isfinite(parsed):
            raise ValueError("numeric arithmetic projection float literal must be finite")
        return parsed


def _normalize_timestamp_literal(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            "SQL predicate datetime literals must be timezone-aware; scoped timestamp_micros admits UTC ISO timestamps only"
        )
    value = value.astimezone(_api.timezone.utc)
    if value.microsecond:
        text = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        text = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return text


def _predicate_sources(*values):
    from ._window import WindowSpec
    sources: list[WorkflowSource] = []
    for value in values:
        if isinstance(value, (tuple, list)):
            sources.extend(_api._predicate_sources(*value))
        elif isinstance(value, _api.Mapping):
            sources.extend(_api._predicate_sources(*value.values()))
        elif isinstance(value, (_api.PredicateExpression, _api.ColumnExpression, _api.WindowExpression, WindowSpec)):
            sources.extend(value.source_bindings)
        elif isinstance(value, _api.LazyFrame):
            if value.client.transport == "native":
                sources.append(value._bound_relation_source())
            else:
                sources.extend(value._declared_sources())
    return tuple(sources)


def _predicate_limits(*values):

    from ._window import WindowSpec
    limits = []
    for value in values:
        if isinstance(value, (tuple, list)):
            limits.append(_api._predicate_limits(*value))
        elif isinstance(value, _api.Mapping):
            limits.append(_api._predicate_limits(*value.values()))
        elif isinstance(value, (_api.PredicateExpression, _api.ColumnExpression, _api.WindowExpression,
                                WindowSpec, _api.WorkflowOperation, _api.LazyFrame)):
            limits.append(value.resource_limits)
    return _api.merge_resource_limits(*limits)


def _validate_raw_sql_fragment(name, value, *, allowed_keywords, allow_nested_queries):
    _api._validate_sql_fragment_quotes(name, value)
    if allow_nested_queries:
        _api._validate_balanced_expression_parentheses(value)
    for token in _api._RAW_SQL_FRAGMENT_BREAKOUT_TOKENS:
        if _api._contains_sql_token_outside_quotes(value, token):
            raise ValueError(
                f"{name} must stay inside a scoped expression; SQL statement separators "
                "and comments are not admitted"
            )
    for keyword in _api._RAW_SQL_FRAGMENT_BREAKOUT_KEYWORDS:
        if keyword in allowed_keywords:
            continue
        found = (
            _api._find_top_level_sql_keyword_outside_quotes(value, keyword) is not None
            if allow_nested_queries else _api._contains_sql_keyword_outside_quotes(value, keyword)
        )
        if found:
            raise ValueError(
                f"{name} must stay inside a scoped expression; SQL clause keyword "
                f"{keyword!r} is not admitted in raw fragments"
            )


def _sql_fragment_admitted_for_local_source_statement(name, value, *, allowed_keywords):
    try:
        _api._validate_raw_sql_fragment(
            name,
            value,
            allowed_keywords=allowed_keywords,
            # Scalar SELECTs are bounded expression children. Clause breakouts,
            # separators and comments remain denied before native SQL binding.
            allow_nested_queries=True,
        )
    except ValueError:
        return False
    return True


def _validate_sql_fragment_quotes(name, value):
    in_quote = False
    index = 0
    while index < len(value):
        if value[index] == "'":
            if in_quote and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
        index += 1
    if in_quote:
        raise ValueError(f"{name} SQL string literal is not closed")


def _contains_sql_token_outside_quotes(statement, token):
    in_quote = False
    index = 0
    while index <= len(statement) - len(token):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote and statement.startswith(token, index):
            return True
        index += 1
    return False


def _normalize_in_values(values):
    if len(values) == 1 and _api._is_non_string_sequence(values[0]):
        normalized = tuple(values[0])
    else:
        normalized = values
    if not normalized:
        raise ValueError("IN predicates require at least one value")
    if len(normalized) > 32:
        raise ValueError("IN predicates admit at most 32 values")
    return normalized


def _row_value_in_predicate(columns, rows, *, negated):
    normalized_columns = _api._normalize_row_value_columns(columns)
    normalized_rows = _api._normalize_row_value_in_rows(rows, arity=len(normalized_columns))
    column_sql = ",".join(normalized_columns)
    row_sql = ",".join(
        "(" + ",".join(_api._sql_in_literal(value) for value in row) + ")"
        for row in normalized_rows
    )
    operator = "NOT IN" if negated else "IN"
    return _api.PredicateExpression(f"({column_sql}) {operator} ({row_sql})")


def _row_value_in_source_predicate(columns, source, source_columns, *, source_alias, where, group_by, having, order_by, descending, limit, negated):
    normalized_columns = _api._normalize_row_value_columns(columns)
    normalized_source_columns = _api._normalize_row_value_columns(source_columns)
    if len(normalized_source_columns) != len(normalized_columns):
        raise ValueError(
            "row-value IN subquery selected-column arity must match the source column count"
        )
    column_sql = ",".join(normalized_columns)
    source_column_sql = ",".join(normalized_source_columns)
    source_ref = _api._sql_in_subquery_source(source, source_alias=source_alias)
    tail = _api._sql_in_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
    )
    operator = "NOT IN" if negated else "IN"
    return _api.PredicateExpression(
        f"({column_sql}) {operator} (SELECT {source_column_sql} FROM {source_ref}{tail})",
        _api._predicate_sources(source, where, having),
        resource_limits=_api._predicate_limits(source, where, having),
    )


def _exists_source_predicate(source, *, source_alias, select, where, group_by, having, order_by, descending, limit, negated):
    projection_sql = _api._normalize_exists_subquery_projection(select)
    source_ref = _api._sql_local_subquery_source(
        source, "EXISTS subquery source", source_alias=source_alias
    )
    tail = _api._sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="EXISTS subquery limit",
        positive_limit=False,
    )
    operator = "NOT EXISTS" if negated else "EXISTS"
    return _api.PredicateExpression(
        f"{operator} (SELECT {projection_sql} FROM {source_ref}{tail})",
        _api._predicate_sources(source, where, having),
        resource_limits=_api._predicate_limits(source, where, having),
    )


def _quantified_source_predicate(column_sql, comparison, quantifier, source, source_column, *, source_alias, where, group_by, having, order_by, descending, limit):
    operator = _api._normalize_quantified_comparison_operator(comparison)
    source_column_sql = _api._normalize_expression_column(source_column)
    source_ref = _api._sql_local_subquery_source(
        source, "ANY/ALL subquery source", source_alias=source_alias
    )
    tail = _api._sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="ANY/ALL subquery limit",
        positive_limit=True,
    )
    return _api.PredicateExpression(
        f"{column_sql} {operator} {quantifier} "
        f"(SELECT {source_column_sql} FROM {source_ref}{tail})",
        _api._predicate_sources(source, where, having),
        resource_limits=_api._predicate_limits(source, where, having),
    )


def _normalize_quantified_comparison_operator(operator):
    text = _api._require_non_empty("ANY/ALL comparison operator", operator).lower()
    operators = {
        "=": "=",
        "==": "=",
        "eq": "=",
        "!=": "!=",
        "<>": "!=",
        "ne": "!=",
        "neq": "!=",
        "<": "<",
        "lt": "<",
        "<=": "<=",
        "le": "<=",
        "lte": "<=",
        ">": ">",
        "gt": ">",
        ">=": ">=",
        "ge": ">=",
        "gte": ">=",
    }
    try:
        return operators[text]
    except KeyError as exc:
        raise ValueError(
            "ANY/ALL comparison operator must be one of =, !=, <>, <, <=, >, >=, "
            "eq, ne, lt, le, gt, or ge"
        ) from exc


def _normalize_exists_subquery_projection(select):
    if isinstance(select, str) and select.strip() == "*":
        return "*"
    if isinstance(select, _api.ColumnExpression):
        return _api._normalize_expression_column(select.sql)
    if _api._is_non_string_sequence(select):
        columns = tuple(_api._normalize_expression_column(item) for item in select)
        if not columns:
            raise ValueError("EXISTS subquery projection columns must not be empty")
        if len(set(columns)) != len(columns):
            raise ValueError("EXISTS subquery projection columns must be unique")
        return ",".join(columns)
    if isinstance(select, str):
        return _api._normalize_expression_column(select)
    return _api._sql_literal(select)


def _normalize_row_value_columns(columns):
    if _api._is_non_string_sequence(columns):
        raw_columns = tuple(columns)
    else:
        raw_columns = (columns,)
    normalized = tuple(_api._normalize_expression_column(column) for column in raw_columns)
    if len(normalized) < 2:
        raise ValueError("row-value IN predicates require at least two columns")
    if len(set(normalized)) != len(normalized):
        raise ValueError("row-value IN predicate columns must be unique")
    return normalized


def _normalize_row_value_in_rows(rows, *, arity):
    if not _api._is_non_string_sequence(rows):
        raise TypeError("row-value IN predicates require a sequence of literal rows")
    normalized_rows: list[tuple[object, ...]] = []
    for row in rows:
        if not _api._is_non_string_sequence(row):
            raise TypeError("row-value IN literal rows must be sequences")
        normalized_row = tuple(row)
        if len(normalized_row) != arity:
            raise ValueError(
                "row-value IN literal row arity must match the source column count"
            )
        normalized_rows.append(normalized_row)
    if not normalized_rows:
        raise ValueError("row-value IN predicates require at least one literal row")
    if len(normalized_rows) > 32:
        raise ValueError("row-value IN predicates admit at most 32 literal rows")
    return tuple(normalized_rows)


def _sql_local_subquery_source(source, name, *, source_alias):
    if isinstance(source, _api.LazyFrame):
        if source.client.transport == "native":
            source_ref = source._bound_relation_source().uri
        elif source.operations or source.source.relation is not None:
            statement = source._relation_statement()
            if statement is None:
                raise ValueError("source subquery contains an operation without native relational lowering")
            source_ref = f"({statement})"
            if source_alias is None:
                source_alias = "_sl_subquery"
        else:
            source_ref = _api._quote_sql_local_source_path(source.source.uri)
    else:
        source_ref = _api._quote_sql_local_source_path(_api._require_non_empty(name, source))
    if source_alias is None:
        return source_ref
    alias = _api._normalize_output_column_name(source_alias)
    if alias.lower() == "outer":
        raise ValueError("local subquery source alias 'outer' is reserved")
    return f"{source_ref} AS {alias}"


def _sql_in_subquery_tail(*, where, group_by, having, order_by, descending, limit):
    return _api._sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="IN subquery limit",
        positive_limit=True,
    )


def _sql_local_subquery_tail(*, where, group_by, having, order_by, descending, limit, limit_name, positive_limit):
    tail = ""
    if where is not None:
        tail = f"{tail} WHERE {_api._predicate_sql(where)}"
    group_columns = _api._normalize_in_subquery_order_by(group_by)
    if having is not None and not group_columns:
        raise ValueError("source subquery HAVING requires group_by in this scoped helper")
    if group_columns:
        tail = f"{tail} GROUP BY {','.join(group_columns)}"
    if having is not None:
        tail = f"{tail} HAVING {_api._predicate_sql(having)}"
    order_columns = _api._normalize_in_subquery_order_by(order_by)
    if order_columns:
        direction = "desc" if descending else "asc"
        tail = f"{tail}{_api._format_order_by_clause(order_columns, direction)}"
    if limit is not None:
        if positive_limit:
            normalized_limit = _api._normalize_positive_int(limit_name, limit)
        else:
            normalized_limit = _api._normalize_non_negative_int(limit_name, limit)
        tail = f"{tail} LIMIT {normalized_limit}"
    return tail


def _is_source_free_sql_statement(statement):
    normalized = statement.strip().rstrip(";").strip()
    if _api._starts_with_sql_keyword(normalized, "values"):
        return True
    if _api._is_source_free_sql_generator_statement(normalized):
        return True
    return _api._starts_with_sql_keyword(normalized, "select") and not _api._contains_sql_keyword_outside_quotes(
        normalized,
        "from",
    )


def _is_source_free_sql_generator_statement(statement):
    if not _api._starts_with_sql_keyword(statement, "select"):
        return False
    select_body = statement[len("select") :].strip()
    from_position = _api._find_sql_keyword_outside_quotes(select_body, "from")
    if from_position is None:
        return False
    source_ref = select_body[from_position + len("from") :].strip().lower()
    clause_positions = tuple(
        position
        for position in (
            _api._find_sql_keyword_outside_quotes(source_ref, "where"),
            _api._find_sql_keyword_outside_quotes(source_ref, "limit"),
        )
        if position is not None
    )
    if clause_positions:
        source_ref = source_ref[: min(clause_positions)].strip()
    return (
        source_ref.startswith("generate_series(")
        or source_ref.startswith("generate_series (")
        or source_ref.startswith("range(")
        or source_ref.startswith("range (")
    ) and source_ref.endswith(")")
