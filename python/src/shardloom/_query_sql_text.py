"""Deferred implementations for query sql text.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _cap_top_level_sql_limit(statement, limit_index, count):
    limit_end = limit_index + len("limit")
    tail = statement[limit_end:].lstrip()
    if not tail:
        return statement
    digit_count = 0
    while digit_count < len(tail) and tail[digit_count].isdigit():
        digit_count += 1
    if digit_count == 0:
        return statement
    existing_limit = int(tail[:digit_count])
    capped_limit = min(existing_limit, count)
    return f"{statement[:limit_index].rstrip()} LIMIT {capped_limit}{tail[digit_count:]}"


def _duplicate_mask_operation_parts(values):
    if not values:
        return None, "first"
    keep = "first"
    columns: list[str] = []
    for value in values:
        if value.startswith("keep="):
            keep = value.removeprefix("keep=")
        else:
            columns.append(value)
    if keep not in {"first", "last", "false"}:
        return None, "first"
    if not columns or (columns != ["*"] and any(not _api._is_sql_identifier(column) for column in columns)):
        return None, keep
    return tuple(columns), keep


def _vortex_tiny_predicate_from_sql(predicate):

    text = predicate.strip()
    if not text:
        return None
    if _api._VORTEX_TINY_COMPACT_RE.fullmatch(text):
        return text
    null_match = _api._VORTEX_TINY_NULL_PREDICATE_RE.fullmatch(text)
    if null_match:
        column = null_match.group(1)
        return f"is_not_null:{column}" if null_match.group(2) else f"is_null:{column}"
    compare_match = _api._VORTEX_TINY_INT_COMPARE_RE.fullmatch(text)
    if compare_match is None:
        return None
    column, operator, value = compare_match.groups()
    op = {
        "=": "eq",
        ">": "gt",
        ">=": "gte",
        "<": "lt",
        "<=": "lte",
    }[operator]
    return f"{op}:{column}:{value}"


def _vortex_tiny_predicate_column(predicate):
    if predicate is None:
        return None
    text = predicate.strip()
    null_match = _api._VORTEX_TINY_NULL_PREDICATE_RE.fullmatch(text)
    if null_match:
        return null_match.group(1)
    compact_match = _api._VORTEX_TINY_COMPACT_RE.fullmatch(text)
    if compact_match:
        return compact_match.group(2) or compact_match.group(4)
    compare_match = _api._VORTEX_TINY_INT_COMPARE_RE.fullmatch(text)
    if compare_match:
        return compare_match.group(1)
    return None


def _is_local_source_sql_ref(value):
    lower = value.strip().lower()
    if "://" in lower or lower.startswith(("s3:", "gs:", "abfs:", "abfss:")):
        return False
    suffix = _api._source_suffix_for_local_ref(lower)
    return suffix in {
            ".csv",
            ".tsv",
            ".json",
            ".jsonl",
            ".ndjson",
            ".parquet",
            ".arrow",
            ".ipc",
            ".feather",
            ".avro",
            ".orc",
        }


def _find_top_level_sql_phrase_span_outside_quotes(statement, phrase):
    tokens = phrase.lower().split()
    if not tokens:
        return None
    lower = statement.lower()
    in_quote = False
    depth = 0
    index = 0
    first = tokens[0]
    while index <= len(statement) - len(first):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote:
            if char == "(":
                depth += 1
                index += 1
                continue
            if char == ")" and depth > 0:
                depth -= 1
                index += 1
                continue
            if depth == 0 and lower.startswith(first, index):
                before = statement[index - 1] if index > 0 else ""
                after_index = index + len(first)
                after = statement[after_index] if after_index < len(statement) else ""
                if _api._is_identifier_char(before) or _api._is_identifier_char(after):
                    index += 1
                    continue
                cursor = after_index
                matched = True
                for token in tokens[1:]:
                    if cursor >= len(statement) or not statement[cursor].isspace():
                        matched = False
                        break
                    while cursor < len(statement) and statement[cursor].isspace():
                        cursor += 1
                    if not lower.startswith(token, cursor):
                        matched = False
                        break
                    token_end = cursor + len(token)
                    next_char = statement[token_end] if token_end < len(statement) else ""
                    if _api._is_identifier_char(next_char):
                        matched = False
                        break
                    cursor = token_end
                if matched:
                    return index, cursor
        index += 1
    return None


def _sql_source_refs(statement):
    refs: list[str] = []
    lower = statement.lower()
    in_quote = False
    index = 0
    while index < len(statement):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if in_quote:
            index += 1
            continue
        keyword_len = 0
        for keyword in ("from", "join"):
            if lower.startswith(keyword, index):
                before = statement[index - 1] if index > 0 else ""
                after_index = index + len(keyword)
                after = statement[after_index] if after_index < len(statement) else ""
                if not _api._is_identifier_char(before) and not _api._is_identifier_char(after):
                    keyword_len = len(keyword)
                    break
        if keyword_len == 0:
            index += 1
            continue
        ref_start = index + keyword_len
        while ref_start < len(statement) and statement[ref_start].isspace():
            ref_start += 1
        if ref_start < len(statement) and statement[ref_start] == "'":
            ref_end = ref_start + 1
            current: list[str] = []
            while ref_end < len(statement):
                if statement[ref_end] == "'":
                    if ref_end + 1 < len(statement) and statement[ref_end + 1] == "'":
                        current.append("'")
                        ref_end += 2
                        continue
                    refs.append("".join(current))
                    index = ref_end + 1
                    break
                current.append(statement[ref_end])
                ref_end += 1
            else:
                index = ref_end
        else:
            index = ref_start
    return tuple(refs)


def _find_sql_keyword_outside_quotes(statement, keyword):
    lower = statement.lower()
    needle = keyword.lower()
    in_quote = False
    index = 0
    while index <= len(statement) - len(needle):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote and lower.startswith(needle, index):
            before = statement[index - 1] if index > 0 else ""
            after_index = index + len(needle)
            after = statement[after_index] if after_index < len(statement) else ""
            if not _api._is_identifier_char(before) and not _api._is_identifier_char(after):
                return index
        index += 1
    return None


def _find_top_level_sql_keyword_outside_quotes(statement, keyword):
    lower = statement.lower()
    needle = keyword.lower()
    in_quote = False
    depth = 0
    index = 0
    while index <= len(statement) - len(needle):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote:
            if char == "(":
                depth += 1
                index += 1
                continue
            if char == ")" and depth > 0:
                depth -= 1
                index += 1
                continue
            if depth == 0 and lower.startswith(needle, index):
                before = statement[index - 1] if index > 0 else ""
                after_index = index + len(needle)
                after = statement[after_index] if after_index < len(statement) else ""
                if not _api._is_identifier_char(before) and not _api._is_identifier_char(after):
                    return index
        index += 1
    return None


def _normalize_sort_nulls(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("sort nulls must be one of 'first' or 'last'")
    normalized = value.strip().lower().replace("_", "-")
    if normalized in {"first", "nulls-first"}:
        return "first"
    if normalized in {"last", "nulls-last"}:
        return "last"
    raise ValueError("sort nulls must be one of 'first' or 'last'")


def _sql_join_keyword(how):
    return {
        "inner": "INNER JOIN",
        "left": "LEFT JOIN",
        "right": "RIGHT JOIN",
        "full": "FULL JOIN",
        "semi": "LEFT SEMI JOIN",
        "anti": "LEFT ANTI JOIN",
        "cross": "CROSS JOIN",
    }[how]


def _rewrite_predicate_with_computed_columns(predicate, computed_columns):
    expanded_columns: dict[str, str] = {}
    for alias, expression in computed_columns:
        expanded = expression
        for prior_alias, prior_expression in expanded_columns.items():
            expanded = _api._replace_sql_identifier_outside_quotes(
                expanded,
                prior_alias,
                f"({prior_expression})",
            )
        expanded_columns[alias] = expanded

    rewritten = predicate
    for alias, expression in expanded_columns.items():
        rewritten = _api._replace_sql_identifier_outside_quotes(
            rewritten,
            alias,
            f"({expression})",
        )
    return rewritten


def _replace_sql_identifier_outside_quotes(statement, identifier, replacement):
    if not identifier or identifier not in statement:
        return statement
    rewritten: list[str] = []
    in_quote = False
    index = 0
    while index < len(statement):
        char = statement[index]
        if char == "'":
            rewritten.append(char)
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                rewritten.append(statement[index + 1])
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if (
            not in_quote
            and statement.startswith(identifier, index)
            and _api._sql_identifier_rewrite_boundary(statement, index, len(identifier))
        ):
            rewritten.append(replacement)
            index += len(identifier)
            continue
        rewritten.append(char)
        index += 1
    return "".join(rewritten)
