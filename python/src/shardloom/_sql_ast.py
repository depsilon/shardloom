# SPDX-License-Identifier: Apache-2.0
"""Unified SQL lexer and optional native AST bridge for ShardLoom Python planning.

Retires ad-hoc character loops across ``shardloom.query`` in favor of a single
scope- and comment-aware SQL tokenizer (stdlib only) paired with the canonical
Rust ``shardloom._native.inspect_sql_json`` hook when the native extension is loaded.
"""

from __future__ import annotations

import importlib
import json
import sys
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SqlToken:
    """Single lexed SQL token with source byte/character offsets and nesting depth."""

    kind: str  # "word", "single_quote", "double_quote", "punct"
    value: str
    start: int
    end: int
    paren_depth: int
    bracket_depth: int


def inspect_sql_native(
    statement: str,
    *,
    allow_import: bool = False,
) -> dict[str, Any] | None:
    """Return canonical AST metadata via ``shardloom._native.inspect_sql_json`` if loaded."""
    module = sys.modules.get("shardloom._native")
    if module is None and allow_import:
        try:
            module = importlib.import_module("shardloom._native")
        except ImportError:
            module = None
    if module is None:
        return None
    fn = getattr(module, "inspect_sql_json", None)
    if fn is None:
        return None
    try:
        payload = json.loads(fn(statement))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def tokenize_sql(
    statement: str,
    *,
    error_prefix: str = "computed with_column expression",
    require_balanced: bool = False,
) -> tuple[SqlToken, ...]:
    """Tokenize SQL handling comments, escaped quotes, brackets, and parentheses."""
    tokens: list[SqlToken] = []
    length = len(statement)
    index = 0
    paren_depth = 0
    bracket_depth = 0

    while index < length:
        ch = statement[index]
        if ch.isspace():
            index += 1
            continue
        if ch == "-" and index + 1 < length and statement[index + 1] == "-":
            newline = statement.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if ch == "/" and index + 1 < length and statement[index + 1] == "*":
            close = statement.find("*/", index + 2)
            if close < 0:
                if require_balanced:
                    raise ValueError(f"{error_prefix} has an unclosed block comment")
                break
            index = close + 2
            continue
        if ch in ("'", '"'):
            quote_char = ch
            cursor = index + 1
            chars: list[str] = []
            closed = False
            while cursor < length:
                current = statement[cursor]
                if current == quote_char:
                    if cursor + 1 < length and statement[cursor + 1] == quote_char:
                        chars.append(quote_char)
                        cursor += 2
                        continue
                    cursor += 1
                    closed = True
                    break
                chars.append(current)
                cursor += 1
            if not closed:
                if require_balanced:
                    raise ValueError(f"{error_prefix} has an unclosed string literal")
                break
            tokens.append(
                SqlToken(
                    kind="single_quote" if quote_char == "'" else "double_quote",
                    value="".join(chars),
                    start=index,
                    end=cursor,
                    paren_depth=paren_depth,
                    bracket_depth=bracket_depth,
                )
            )
            index = cursor
            continue
        if ch.isalnum() or ch == "_":
            cursor = index + 1
            while cursor < length and (statement[cursor].isalnum() or statement[cursor] == "_"):
                cursor += 1
            tokens.append(
                SqlToken(
                    kind="word",
                    value=statement[index:cursor],
                    start=index,
                    end=cursor,
                    paren_depth=paren_depth,
                    bracket_depth=bracket_depth,
                )
            )
            index = cursor
            continue
        if ch == "(":
            tokens.append(SqlToken("punct", "(", index, index + 1, paren_depth, bracket_depth))
            paren_depth += 1
            index += 1
            continue
        if ch == ")":
            paren_depth -= 1
            if paren_depth < 0:
                if require_balanced:
                    raise ValueError(f"{error_prefix} has unbalanced parentheses")
                paren_depth = 0
            tokens.append(SqlToken("punct", ")", index, index + 1, paren_depth, bracket_depth))
            index += 1
            continue
        if ch == "[":
            tokens.append(SqlToken("punct", "[", index, index + 1, paren_depth, bracket_depth))
            bracket_depth += 1
            index += 1
            continue
        if ch == "]":
            bracket_depth -= 1
            if bracket_depth < 0:
                if require_balanced:
                    raise ValueError(f"{error_prefix} has unbalanced brackets")
                bracket_depth = 0
            tokens.append(SqlToken("punct", "]", index, index + 1, paren_depth, bracket_depth))
            index += 1
            continue
        tokens.append(SqlToken("punct", ch, index, index + 1, paren_depth, bracket_depth))
        index += 1

    if require_balanced:
        if paren_depth != 0:
            raise ValueError(f"{error_prefix} has unbalanced parentheses")
        if bracket_depth != 0:
            raise ValueError(f"{error_prefix} has unbalanced brackets")
    return tuple(tokens)


def validate_balanced_parentheses(
    value: str,
    *,
    error_prefix: str = "computed with_column expression",
) -> None:
    """Validate that quotes, comments, parentheses, and brackets are balanced."""
    tokenize_sql(value, error_prefix=error_prefix, require_balanced=True)


def split_top_level_csv(
    expression: str,
    *,
    error_prefix: str = "computed with_column expression",
) -> tuple[str, ...]:
    """Split a top-level comma-separated SQL argument list."""
    tokens = tokenize_sql(expression, error_prefix=error_prefix, require_balanced=True)
    args: list[str] = []
    start = 0
    for token in tokens:
        if (
            token.kind == "punct"
            and token.value == ","
            and token.paren_depth == 0
            and token.bracket_depth == 0
        ):
            args.append(expression[start : token.start].strip())
            start = token.end
    args.append(expression[start:].strip())
    if any(not arg for arg in args):
        raise ValueError(f"{error_prefix} has an empty argument")
    return tuple(args)


def _word_eq(token: SqlToken, expected: str) -> bool:
    return token.kind == "word" and token.value.lower() == expected


def _preceding_is_distinct(tokens: tuple[SqlToken, ...], idx: int) -> bool:
    if idx >= 2 and _word_eq(tokens[idx - 1], "distinct") and _word_eq(tokens[idx - 2], "is"):
        return True
    return (
        idx >= 3
        and _word_eq(tokens[idx - 1], "distinct")
        and _word_eq(tokens[idx - 2], "not")
        and _word_eq(tokens[idx - 3], "is")
    )


def find_top_level_phrase_span(statement: str, phrase: str) -> tuple[int, int] | None:
    """Return ``(start, end)`` of the first top-level SQL keyword/phrase outside quotes and comments."""
    parts = phrase.lower().split()
    if not parts:
        return None
    tokens = tokenize_sql(statement, require_balanced=False)
    search_and = parts == ["and"]
    search_from = parts == ["from"]
    skip_next_and_for_between = False
    limit = len(tokens) - len(parts)
    for idx in range(max(0, limit + 1)):
        first = tokens[idx]
        if first.paren_depth != 0 or first.bracket_depth != 0:
            continue
        if search_and and _word_eq(first, "between"):
            skip_next_and_for_between = True
            continue
        if all(
            tokens[idx + offset].paren_depth == 0
            and tokens[idx + offset].bracket_depth == 0
            and _word_eq(tokens[idx + offset], part)
            for offset, part in enumerate(parts)
        ):
            if search_and and skip_next_and_for_between:
                skip_next_and_for_between = False
                continue
            if search_from and _preceding_is_distinct(tokens, idx):
                continue
            return first.start, tokens[idx + len(parts) - 1].end
    return None


def find_top_level_keyword(statement: str, keyword: str) -> int | None:
    """Return the start offset of the first top-level keyword/phrase, if present."""
    span = find_top_level_phrase_span(statement, keyword)
    return None if span is None else span[0]


def find_keyword_outside_quotes(statement: str, keyword: str) -> int | None:
    """Return the start offset of the first keyword/phrase outside quotes and comments (any depth)."""
    parts = keyword.lower().split()
    if not parts:
        return None
    tokens = tokenize_sql(statement, require_balanced=False)
    limit = len(tokens) - len(parts)
    for idx in range(max(0, limit + 1)):
        if all(_word_eq(tokens[idx + offset], part) for offset, part in enumerate(parts)):
            return tokens[idx].start
    return None


def contains_keyword_outside_quotes(statement: str, keyword: str) -> bool:
    """Return True when ``keyword`` appears outside quotes and comments."""
    return find_keyword_outside_quotes(statement, keyword) is not None


def starts_with_keyword(statement: str, keyword: str) -> bool:
    """Return True when the first SQL token (after leading comments/whitespace) is ``keyword``."""
    parts = keyword.lower().split()
    if not parts:
        return False
    tokens = tokenize_sql(statement, require_balanced=False)
    if len(tokens) < len(parts):
        return False
    return all(_word_eq(tokens[offset], part) for offset, part in enumerate(parts))


@dataclass
class _SourceScope:
    in_select: bool = False
    in_from_clause: bool = False
    expect_source: bool = False


def extract_source_refs(statement: str) -> tuple[str, ...]:
    """Extract quoted ``FROM``/``JOIN`` source refs via native AST inspection or scope-aware lexer."""
    native = inspect_sql_native(statement)
    if native is not None and isinstance(native.get("source_refs"), list):
        native_refs = [
            entry["reference"]
            for entry in native["source_refs"]
            if isinstance(entry, dict)
            and entry.get("quoted_path") is True
            and isinstance(entry.get("reference"), str)
        ]
        if native_refs:
            return tuple(native_refs)

    tokens = tokenize_sql(statement, require_balanced=False)
    refs: list[str] = []
    scopes: list[_SourceScope] = [_SourceScope()]
    for idx, token in enumerate(tokens):
        if token.kind == "punct" and token.value == "(":
            scopes[-1].expect_source = False
            scopes.append(_SourceScope())
            continue
        if token.kind == "punct" and token.value == ")":
            if len(scopes) > 1:
                scopes.pop()
            else:
                scopes[-1].expect_source = False
            continue
        scope = scopes[-1]
        if token.kind == "single_quote":
            if scope.expect_source:
                refs.append(token.value)
            scope.expect_source = False
        elif token.kind == "word":
            upper = token.value.upper()
            if scope.expect_source:
                scope.expect_source = False
            elif upper == "SELECT":
                scope.in_select = True
                scope.in_from_clause = False
            elif scope.in_select and upper == "FROM":
                if not _preceding_is_distinct(tokens, idx):
                    scope.in_from_clause = True
                    scope.expect_source = True
            elif scope.in_select and scope.in_from_clause and upper == "JOIN":
                scope.expect_source = True
            elif upper in {"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET"}:
                scope.in_select = False
                scope.in_from_clause = False
                scope.expect_source = False
        else:
            scope.expect_source = False
    return tuple(refs)
