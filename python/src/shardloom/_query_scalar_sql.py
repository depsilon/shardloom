"""Deferred implementations for query scalar sql.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _sql_numeric_literal(value):
    if isinstance(value, bool):
        raise ValueError("numeric arithmetic literals must be int or finite float values")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not _api.math.isfinite(value):
            raise ValueError("numeric arithmetic float literals must be finite")
        return str(value)
    if isinstance(value, _api.Decimal):
        return _api._sql_decimal_literal(value)
    raise TypeError("numeric arithmetic literals must be int, finite float, or finite Decimal values")


def _sql_numeric_arithmetic_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    parts = text.split()
    if len(parts) != 3 or parts[1] not in {"+", "-", "*", "/"}:
        raise ValueError(
            "computed with_column currently admits sl.col(...) numeric arithmetic "
            "expressions of the form column (+|-|*|/) literal"
        )
    _api._normalize_expression_column(parts[0])
    literal = _api._parse_numeric_literal_token(parts[2])
    _api._sql_numeric_literal(literal)
    if parts[1] == "/" and literal == 0:
        raise ValueError("numeric arithmetic projection division by zero is not admitted")
    return text


def _sql_generic_expression_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    function = text.split("(", 1)[0].strip().upper() if "(" in text else ""
    scalar_call = function in {
        "CAST", "TRY_CAST", "ABS", "FLOOR", "CEIL", "CEILING", "ROUND",
        "LOWER", "UPPER", "TRIM", "LENGTH", "CONCAT", "SUBSTR", "SUBSTRING",
        "LEFT", "RIGHT", "REPLACE", "BYTE_LENGTH", "OCTET_LENGTH", "UNHEX",
        "FROM_BASE64", "COALESCE", "NULLIF", "DATE_YEAR", "DATE_MONTH", "DATE_DAY",
        "YEAR", "MONTH", "DAY", "TIMESTAMP_YEAR", "TIMESTAMP_MONTH", "TIMESTAMP_DAY",
        "TIMESTAMP_HOUR", "TIMESTAMP_MINUTE", "TIMESTAMP_SECOND", "DATE_ADD_DAYS",
        "DATE_SUB_DAYS", "TIMESTAMP_ADD_SECONDS", "TIMESTAMP_SUB_SECONDS",
        "DATE_DIFF_DAYS", "TIMESTAMP_DIFF_SECONDS",
        "TEMPORAL_BUCKET",
        "EXTRACT_MINUTE", "MINUTE", "DATE_TRUNC_MINUTE", "TRUNCATE_MINUTE",
        "URL_DOMAIN", "REGEX_DOMAIN", "REGEX_REPLACE_DOMAIN", "REGEXP_REPLACE", "REGEX_REPLACE",
    }
    if not (
        _api._expression_has_numeric_operator(text)
        or scalar_call
        or _api._is_scalar_subquery_expression(text)
        or text.upper().startswith(("-", "+", "CASE "))
    ):
        raise ValueError(
            "computed with_column requires an admitted scalar expression"
        )
    _api._validate_balanced_expression_parentheses(text)
    return text


def _expression_has_numeric_operator(value):
    in_quote = False
    depth = 0
    for index, char in enumerate(value):
        if char == "'":
            in_quote = not in_quote
            continue
        if in_quote:
            continue
        if char == "(":
            depth += 1
            continue
        if char == ")":
            depth -= 1
            continue
        if char in {"*", "/"}:
            return True
        if char in {"+", "-"} and not _api._is_unary_numeric_sign(value, index, char):
            return True
    return False


def _expression_has_temporal_difference_call(value):
    text = value.strip()
    while text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if not inner:
            return False
        text = inner
    upper = text.upper()
    return upper.startswith("DATE_DIFF_DAYS(") or upper.startswith(
        "TIMESTAMP_DIFF_SECONDS("
    )


def _is_unary_numeric_sign(value, index, char):
    if char not in {"+", "-"}:
        return False
    before = next(
        (candidate for candidate in reversed(value[:index]) if not candidate.isspace()),
        None,
    )
    after = next(
        (
            candidate
            for candidate in value[index + len(char) :]
            if not candidate.isspace()
        ),
        None,
    )
    return (before is None or before in "(,+-*/") and (
        after is not None and (after.isdigit() or after == ".")
    )


def _validate_balanced_expression_parentheses(value):
    in_quote = False
    depth = 0
    for char in value:
        if char == "'":
            in_quote = not in_quote
            continue
        if in_quote:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("computed with_column expression has unbalanced parentheses")
    if in_quote:
        raise ValueError("computed with_column expression has an unclosed string literal")
    if depth != 0:
        raise ValueError("computed with_column expression has unbalanced parentheses")


def _sql_numeric_abs_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits ABS column expressions")
    function = text[:open_index].strip().upper()
    if function != "ABS":
        raise ValueError("computed with_column currently admits ABS column expressions")
    column = text[open_index + 1 : -1].strip()
    normalized = _api._normalize_expression_column(column)
    return f"ABS({normalized})"


def _sql_numeric_rounding_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits numeric rounding column expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"FLOOR", "CEIL", "ROUND"}:
        raise ValueError(
            "computed with_column currently admits numeric rounding column expressions"
        )
    column = text[open_index + 1 : -1].strip()
    normalized = _api._normalize_expression_column(column)
    return f"{function}({normalized})"


def _sql_computed_projection_expression(expression):
    if isinstance(expression, _api.ColumnExpression):
        if expression.source_bindings:
            return expression.sql
        try:
            return _api._normalize_expression_column(expression.sql)
        except (TypeError, ValueError):
            pass
    parsers = (
        _api._sql_complex_projection_expression,
        _api._sql_cast_projection_expression,
        _api._sql_null_coalesce_projection_expression,
        _api._sql_nullif_projection_expression,
        _api._sql_conditional_projection_expression,
        _api._sql_predicate_projection_expression,
        _api._sql_numeric_arithmetic_projection_expression,
        _api._sql_numeric_abs_projection_expression,
        _api._sql_numeric_rounding_projection_expression,
        _api._sql_generic_expression_projection_expression,
        _api._sql_date_arithmetic_projection_expression,
        _api._sql_timestamp_arithmetic_projection_expression,
        _api._sql_string_length_projection_expression,
        _api._sql_string_transform_projection_expression,
        _api._sql_string_function_projection_expression,
        _api._sql_binary_helper_projection_expression,
        _api._sql_binary_byte_length_projection_expression,
        _api._sql_temporal_extract_projection_expression,
    )
    last_error: TypeError | ValueError | None = None
    for parser in parsers:
        try:
            return parser(expression)
        except (TypeError, ValueError) as error:
            last_error = error
    if last_error is None:
        raise ValueError("computed with_column expression is not admitted")
    raise last_error


def _sql_cast_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    upper_text = text.upper()
    if not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits CAST/TRY_CAST column expressions"
        )
    if upper_text.startswith("TRY_CAST("):
        function = "TRY_CAST"
        inner = text[len("TRY_CAST(") : -1].strip()
    elif upper_text.startswith("CAST("):
        function = "CAST"
        inner = text[len("CAST(") : -1].strip()
    else:
        raise ValueError(
            "computed with_column currently admits CAST/TRY_CAST column expressions"
        )
    source, dtype = _api._split_cast_source_and_dtype(
        inner,
        "CAST/TRY_CAST column expressions must use CAST(column AS dtype) syntax",
    )
    _api._validate_balanced_expression_parentheses(source)
    return f"{function}({source} AS {dtype})"


def _sql_null_coalesce_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits COALESCE column expressions")
    function = text[:open_index].strip().upper()
    if function != "COALESCE":
        raise ValueError("computed with_column currently admits COALESCE column expressions")
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError("COALESCE with_column expressions require exactly two arguments")
    column = _api._normalize_nullable_projection_column(args[0])
    fallback = args[1].strip()
    if fallback.upper() == "NULL":
        raise ValueError("COALESCE with_column expressions require a non-NULL fallback")
    return f"COALESCE({column}, {fallback})"


def _sql_nullif_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits NULLIF column expressions")
    function = text[:open_index].strip().upper()
    if function != "NULLIF":
        raise ValueError("computed with_column currently admits NULLIF column expressions")
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError("NULLIF with_column expressions require exactly two arguments")
    column = _api._normalize_nullable_projection_column(args[0])
    sentinel = args[1].strip()
    if sentinel.upper() == "NULL":
        raise ValueError("NULLIF with_column expressions require a non-NULL sentinel")
    return f"NULLIF({column}, {sentinel})"


def _sql_conditional_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    upper = text.upper()
    if not upper.startswith("CASE "):
        raise ValueError("computed with_column currently admits CASE WHEN expressions")
    when_marker = "WHEN "
    then_marker = " THEN "
    else_marker = " ELSE "
    end_marker = " END"
    when_index = upper.find(when_marker)
    then_index = upper.find(then_marker)
    else_index = upper.find(else_marker)
    end_index = upper.rfind(end_marker)
    if not (0 <= when_index < then_index < else_index < end_index):
        raise ValueError(
            "CASE with_column expressions must use CASE WHEN <predicate> THEN <literal-or-column> ELSE <literal-or-column> END"
        )
    if upper[:when_index].strip() != "CASE" or upper[end_index + len(end_marker) :].strip():
        raise ValueError(
            "CASE with_column expressions must be a single CASE WHEN expression"
        )
    predicate = _api._predicate_sql(text[when_index + len(when_marker) : then_index].strip())
    then_literal = text[then_index + len(then_marker) : else_index].strip()
    else_literal = text[else_index + len(else_marker) : end_index].strip()
    if not then_literal or not else_literal:
        raise ValueError("CASE with_column expressions require THEN and ELSE branches")
    return f"CASE WHEN {predicate} THEN {then_literal} ELSE {else_literal} END"


def _sql_case_branch(value):
    if value is None:
        return "NULL"
    if isinstance(value, (_api.ColumnExpression, _api.PredicateExpression)):
        text = value.sql.strip()
        if not text:
            kind = "column" if isinstance(value, _api.ColumnExpression) else "predicate"
            raise ValueError(f"CASE branch {kind} expression must not be empty")
        return text
    return _api._sql_literal(value)


def _sql_date_arithmetic_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits DATE_ADD_DAYS/DATE_SUB_DAYS expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"DATE_ADD_DAYS", "DATE_SUB_DAYS"}:
        raise ValueError(
            "computed with_column currently admits DATE_ADD_DAYS/DATE_SUB_DAYS expressions"
        )
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError(
            "date arithmetic with_column expressions require exactly two arguments"
        )
    column = _api._normalize_temporal_extract_column(args[0], "date32")
    days = _api._normalize_date_arithmetic_days(args[1])
    return f"{function}({column}, {days})"


def _sql_timestamp_arithmetic_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits TIMESTAMP_ADD_SECONDS/TIMESTAMP_SUB_SECONDS expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"TIMESTAMP_ADD_SECONDS", "TIMESTAMP_SUB_SECONDS"}:
        raise ValueError(
            "computed with_column currently admits TIMESTAMP_ADD_SECONDS/TIMESTAMP_SUB_SECONDS expressions"
        )
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError(
            "timestamp arithmetic with_column expressions require exactly two arguments"
        )
    column = _api._normalize_temporal_extract_column(args[0], "timestamp_micros")
    seconds = _api._normalize_timestamp_arithmetic_seconds(args[1])
    return f"{function}({column}, {seconds})"


def _sql_string_transform_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits LOWER/UPPER/TRIM column expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"LOWER", "UPPER", "TRIM"}:
        raise ValueError(
            "computed with_column currently admits LOWER/UPPER/TRIM column expressions"
        )
    column = text[open_index + 1 : -1].strip()
    normalized, has_source_column = _api._normalize_string_scalar_expression_sql(column)
    if not has_source_column:
        raise ValueError(
            "string transform with_column expressions require at least one source column"
        )
    return f"{function}({normalized})"


def _sql_string_length_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits LENGTH column expressions")
    function = text[:open_index].strip().upper()
    if function != "LENGTH":
        raise ValueError("computed with_column currently admits LENGTH column expressions")
    column = text[open_index + 1 : -1].strip()
    normalized, has_source_column = _api._normalize_string_scalar_expression_sql(column)
    if not has_source_column:
        raise ValueError("LENGTH with_column expressions require at least one source column")
    return f"LENGTH({normalized})"


def _sql_string_function_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits CONCAT/SUBSTR/LEFT/RIGHT/REPLACE expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"CONCAT", "SUBSTR", "SUBSTRING", "LEFT", "RIGHT", "REPLACE"}:
        raise ValueError(
            "computed with_column currently admits CONCAT/SUBSTR/LEFT/RIGHT/REPLACE expressions"
        )
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if function == "CONCAT":
        if len(args) < 2:
            raise ValueError("CONCAT with_column expressions require at least two arguments")
        normalized_args: list[str] = []
        has_source_column = False
        for arg in args:
            normalized, is_source_column = _api._normalize_string_scalar_expression_sql(arg)
            normalized_args.append(normalized)
            has_source_column = has_source_column or is_source_column
        if not has_source_column:
            raise ValueError(
                "CONCAT with_column expressions require at least one source column"
            )
        return f"CONCAT({', '.join(normalized_args)})"
    if function in {"SUBSTR", "SUBSTRING"}:
        if len(args) != 3:
            raise ValueError(
                "SUBSTR/SUBSTRING with_column expressions require exactly three arguments"
            )
        value_arg, is_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        if not is_source_column:
            raise ValueError(
                "SUBSTR/SUBSTRING with_column expressions require a source column argument"
            )
        start = _api._normalize_substring_bound("substring start", args[1], minimum=1)
        length = _api._normalize_substring_bound("substring length", args[2], minimum=0)
        return f"SUBSTR({value_arg}, {start}, {length})"
    if function in {"LEFT", "RIGHT"}:
        if len(args) != 2:
            raise ValueError(
                "LEFT/RIGHT with_column expressions require exactly two arguments"
            )
        value_arg, is_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        if not is_source_column:
            raise ValueError(
                "LEFT/RIGHT with_column expressions require a source column argument"
            )
        count = _api._normalize_substring_bound("left/right count", args[1], minimum=0)
        return f"{function}({value_arg}, {count})"
    if len(args) != 3:
        raise ValueError("REPLACE with_column expressions require exactly three arguments")
    value_arg, is_source_column = _api._normalize_string_scalar_expression_sql(args[0])
    if not is_source_column:
        raise ValueError("REPLACE with_column expressions require a source column argument")
    needle = _api._parse_sql_string_literal_token(args[1])
    if needle == "":
        raise ValueError("REPLACE with_column expressions require a non-empty search literal")
    replacement = _api._parse_sql_string_literal_token(args[2])
    return (
        f"REPLACE({value_arg}, "
        f"{_api._sql_string_function_literal('replace search literal', needle, allow_empty=False)}, "
        f"{_api._sql_string_function_literal('replace replacement literal', replacement, allow_empty=True)})"
    )


def _vortex_string_replace_expression_parts(expression_sql):
    text = expression_sql.strip()
    upper = text.upper()
    if not upper.startswith("REPLACE(") or not text.endswith(")"):
        return None
    args = _api._split_projection_function_args(text[len("REPLACE(") : -1].strip())
    if len(args) != 3:
        return None
    try:
        column, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        if not has_source_column or not _api._is_sql_identifier(column):
            return None
        needle = _api._parse_sql_string_literal_token(args[1])
        replacement = _api._parse_sql_string_literal_token(args[2])
    except (TypeError, ValueError):
        return None
    if needle == "":
        return None
    return column, needle, replacement


def _sql_binary_helper_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits UNHEX/FROM_BASE64 binary helper expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"UNHEX", "FROM_BASE64"}:
        raise ValueError(
            "computed with_column currently admits UNHEX/FROM_BASE64 binary helper expressions"
        )
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 1:
        raise ValueError("binary helper with_column expressions require exactly one argument")
    expression_sql, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
    if not has_source_column:
        raise ValueError("binary helper with_column expressions require a source column argument")
    return f"{function}({expression_sql})"


def _sql_binary_byte_length_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits BYTE_LENGTH/OCTET_LENGTH binary expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"BYTE_LENGTH", "OCTET_LENGTH"}:
        raise ValueError(
            "computed with_column currently admits BYTE_LENGTH/OCTET_LENGTH binary expressions"
        )
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 1:
        raise ValueError("binary byte length with_column expressions require exactly one argument")
    expression_sql, has_source_column = _api._normalize_binary_scalar_expression_sql(args[0])
    if not has_source_column:
        raise ValueError(
            "binary byte length with_column expressions require a source-backed binary expression"
        )
    return f"{function}({expression_sql})"


def _sql_temporal_extract_projection_expression(expression):
    if not isinstance(expression, _api.ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits DATE/TIMESTAMP extract column expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {
        "DATE_YEAR",
        "DATE_MONTH",
        "DATE_DAY",
        "TIMESTAMP_YEAR",
        "TIMESTAMP_MONTH",
        "TIMESTAMP_DAY",
        "TIMESTAMP_HOUR",
        "TIMESTAMP_MINUTE",
        "TIMESTAMP_SECOND",
    }:
        raise ValueError(
            "computed with_column currently admits DATE/TIMESTAMP extract column expressions"
        )
    column = text[open_index + 1 : -1].strip()
    if function.startswith("DATE_"):
        normalized = _api._normalize_temporal_extract_column(column, "date32")
    else:
        normalized = _api._normalize_temporal_extract_column(column, "timestamp_micros")
    return f"{function}({normalized})"


def _split_projection_function_args(expression):
    args: list[str] = []
    start = 0
    depth = 0
    in_quote = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            if in_quote and index + 1 < len(expression) and expression[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
        elif char == "(" and not in_quote:
            depth += 1
        elif char == ")" and not in_quote:
            depth -= 1
            if depth < 0:
                raise ValueError("computed with_column expression has unbalanced parentheses")
        elif char == "," and not in_quote and depth == 0:
            args.append(expression[start:index].strip())
            start = index + 1
        index += 1
    if in_quote:
        raise ValueError("computed with_column expression has an unclosed string literal")
    if depth != 0:
        raise ValueError("computed with_column expression has unbalanced parentheses")
    args.append(expression[start:].strip())
    if any(not arg for arg in args):
        raise ValueError("computed with_column expression has an empty argument")
    return tuple(args)


def _normalize_string_function_text_arg_sql(raw):
    text = _api._require_non_empty("string function argument", raw)
    if text.startswith("'"):
        value = _api._parse_sql_string_literal_token(text)
        return (
            _api._sql_string_function_literal(
                "string function literal", value, allow_empty=True
            ),
            False,
        )
    return _api._normalize_string_scalar_expression_sql(text)


def _normalize_string_scalar_expression_sql(raw):
    text = _api._require_non_empty("string expression", raw)
    if _api._is_scalar_subquery_expression(text):
        # The native binder admits the inner query and its output dtype.
        return text, True
    if text.startswith("'"):
        value = _api._parse_sql_string_literal_token(text)
        return (
            _api._sql_string_function_literal(
                "string function literal", value, allow_empty=True
            ),
            False,
        )
    open_index = text.find("(")
    if open_index < 0:
        return _api._normalize_expression_column(text), True
    if not text.endswith(")"):
        raise ValueError("string expression function call must be closed")
    function = text[:open_index].strip().upper()
    args = _api._split_projection_function_args(text[open_index + 1 : -1].strip())
    if function in {"LOWER", "UPPER", "TRIM"}:
        if len(args) != 1:
            raise ValueError("string transform expressions require exactly one argument")
        arg_sql, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        return f"{function}({arg_sql})", has_source_column
    if function == "CONCAT":
        if len(args) < 2:
            raise ValueError("CONCAT string expressions require at least two arguments")
        normalized_args: list[str] = []
        has_source_column = False
        for arg in args:
            arg_sql, arg_has_source = _api._normalize_string_scalar_expression_sql(arg)
            normalized_args.append(arg_sql)
            has_source_column = has_source_column or arg_has_source
        return f"CONCAT({', '.join(normalized_args)})", has_source_column
    if function in {"SUBSTR", "SUBSTRING"}:
        if len(args) != 3:
            raise ValueError("SUBSTR/SUBSTRING string expressions require exactly three arguments")
        value_arg, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        start = _api._normalize_substring_bound("substring start", args[1], minimum=1)
        length = _api._normalize_substring_bound("substring length", args[2], minimum=0)
        return f"SUBSTR({value_arg}, {start}, {length})", has_source_column
    if function in {"LEFT", "RIGHT"}:
        if len(args) != 2:
            raise ValueError("LEFT/RIGHT string expressions require exactly two arguments")
        value_arg, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        count = _api._normalize_substring_bound("left/right count", args[1], minimum=0)
        return f"{function}({value_arg}, {count})", has_source_column
    if function == "REPLACE":
        if len(args) != 3:
            raise ValueError("REPLACE string expressions require exactly three arguments")
        value_arg, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        needle = _api._parse_sql_string_literal_token(args[1])
        if needle == "":
            raise ValueError("REPLACE string expressions require a non-empty search literal")
        replacement = _api._parse_sql_string_literal_token(args[2])
        return (
            f"REPLACE({value_arg}, "
            f"{_api._sql_string_function_literal('replace search literal', needle, allow_empty=False)}, "
            f"{_api._sql_string_function_literal('replace replacement literal', replacement, allow_empty=True)})",
            has_source_column,
        )
    raise ValueError(
        "string expressions currently admit columns, string literals, LOWER/UPPER/TRIM, CONCAT, SUBSTR/SUBSTRING, LEFT/RIGHT, and REPLACE"
    )


def _normalize_binary_scalar_expression_sql(raw):
    text = _api._require_non_empty("binary expression", raw)
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "binary byte length expressions admit UNHEX(...), FROM_BASE64(...), or CAST(... AS binary)"
        )
    function = text[:open_index].strip().upper()
    inner = text[open_index + 1 : -1].strip()
    if function in {"UNHEX", "FROM_BASE64"}:
        args = _api._split_projection_function_args(inner)
        if len(args) != 1:
            raise ValueError("binary helper expressions require exactly one argument")
        expression_sql, has_source_column = _api._normalize_string_scalar_expression_sql(args[0])
        if not has_source_column:
            raise ValueError(
                "binary helper expressions require a source-backed string expression"
            )
        return f"{function}({expression_sql})", True
    if function in {"CAST", "TRY_CAST"}:
        source, dtype = _api._split_cast_source_and_dtype(
            inner,
            "binary byte length CAST expressions must use CAST(expression AS binary)",
        )
        if dtype != "binary":
            raise ValueError(
                "binary byte length CAST expressions must target binary, blob, or varbinary"
            )
        expression_sql, has_source_column = _api._normalize_string_scalar_expression_sql(source)
        if not has_source_column:
            raise ValueError(
                "binary byte length CAST expressions require a source-backed string expression"
            )
        return f"{function}({expression_sql} AS {dtype})", True
    raise ValueError(
        "binary byte length expressions admit UNHEX(...), FROM_BASE64(...), or CAST(... AS binary)"
    )


def _parse_sql_string_literal_token(raw):
    text = raw.strip()
    if not text.startswith("'") or not text.endswith("'") or len(text) < 2:
        raise ValueError("string function literals must be single quoted")
    body = text[1:-1]
    output: list[str] = []
    index = 0
    while index < len(body):
        char = body[index]
        if char == "'":
            if index + 1 < len(body) and body[index + 1] == "'":
                output.append("'")
                index += 2
                continue
            raise ValueError(
                "single quotes inside string function literals must be escaped as doubled quotes"
            )
        output.append(char)
        index += 1
    return "".join(output)
