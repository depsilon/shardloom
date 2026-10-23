#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Independent, baseline-only equivalents of the pinned 43 ClickBench tasks.

This module is never imported by the engine. DuckDB is an optional test oracle
installed in a separate environment. Result values are computed from Parquet;
neither a ShardLoom binary nor a native result packet is an input.

The finite dialect map makes existing native semantics explicit: byte lengths,
integer epoch dates/times, F64 SUM/AVG with exact integer accumulation,
declared group-key tie breaking,
and absolute source-row order for raw ties and unordered limited groups.
It is deliberately not a general SQL translator.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys

from clickbench_reference_packet import query_statements


QUERIES_SHA256 = "4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b"
ORDINAL = "__shardloom_reference_source_row"
INTEGER_AGGREGATE_COLUMNS = {"AdvEngineID", "ResolutionWidth", "UserID", "IsRefresh"}
INTEGER_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
                 "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"}
EPOCH_MINUTE = "(EventTime // 60 - CASE WHEN EventTime < 0 AND EventTime % 60 <> 0 THEN 1 ELSE 0 END)"
GROUP_KEYS = {
    8: ["AdvEngineID"], 9: ["RegionID"], 10: ["RegionID"],
    11: ["MobilePhoneModel"], 12: ["MobilePhone", "MobilePhoneModel"],
    13: ["SearchPhrase"], 14: ["SearchPhrase"],
    15: ["SearchEngineID", "SearchPhrase"], 16: ["UserID"],
    17: ["UserID", "SearchPhrase"], 18: ["UserID", "SearchPhrase"],
    19: ["UserID", "m", "SearchPhrase"], 22: ["SearchPhrase"],
    23: ["SearchPhrase"], 28: ["CounterID"], 29: ["k"],
    31: ["SearchEngineID", "ClientIP"], 32: ["WatchID", "ClientIP"],
    33: ["WatchID", "ClientIP"], 34: ["URL"], 35: ["literal_1", "URL"],
    36: ["ClientIP", "ClientIP_minus1", "ClientIP_minus2", "ClientIP_minus3"],
    37: ["URL"], 38: ["Title"], 39: ["URL"],
    40: ["TraficSourceID", "SearchEngineID", "AdvEngineID", "Src", "Dst"],
    41: ["URLHash", "EventDate"], 42: ["WindowClientWidth", "WindowClientHeight"],
    43: ["M"],
}


def quote_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
        raise ValueError(f"unsupported reference identifier: {value!r}")
    return '"' + value + '"'


def validate_arithmetic_schema(source_types: dict[str, str]) -> None:
    """The pinned numeric aggregates require integral inputs, not rounded floats."""
    invalid = {name: source_types.get(name) for name in sorted(INTEGER_AGGREGATE_COLUMNS)
               if source_types.get(name) not in INTEGER_TYPES}
    if invalid:
        raise ValueError(f"independent integer aggregate schema differs: {invalid}")


def split_projection(value: str) -> list[str]:
    """Split only top-level commas, preserving SQL literal bytes."""
    parts, start, depth, quote, index = [], 0, 0, None, 0
    while index < len(value):
        char = value[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("unbalanced reference projection")
        elif char == "," and depth == 0:
            parts.append(value[start:index].strip())
            start = index + 1
        index += 1
    if quote is not None or depth:
        raise ValueError("unbalanced reference projection")
    parts.append(value[start:].strip())
    if any(not part for part in parts):
        raise ValueError("empty reference projection")
    return parts


def dialect_expression(expression: str) -> str:
    expression = re.sub(r"length\((\w+)\)", r"octet_length(encode(\1))",
                        expression, flags=re.IGNORECASE)
    # Integer input columns are epoch seconds and days, not SQL timestamps/dates.
    # Use floor, including for negative epochs, independently of native helpers.
    expression = re.sub(
        r"extract\(minute FROM EventTime\)",
        f"(({EPOCH_MINUTE} % 60 + 60) % 60)",
        expression, flags=re.IGNORECASE)
    expression = re.sub(
        r"DATE_TRUNC\('minute', EventTime\)",
        f"({EPOCH_MINUTE} * 60)",
        expression, flags=re.IGNORECASE)
    # Native arithmetic on this signed input is widened before subtraction.
    expression = re.sub(r"\bClientIP - ([123])\b", r"CAST(ClientIP AS BIGINT) - \1", expression)
    return expression


def projection_item(item: str, index: int, query_id: int) -> tuple[str, str]:
    match = re.fullmatch(r"(.*) AS (\w+)", item, flags=re.IGNORECASE | re.DOTALL)
    expression, alias = (match.group(1), match.group(2)) if match else (item, None)
    aggregate = re.fullmatch(r"(SUM|COUNT|AVG|MIN|MAX)\((.*)\)", expression,
                             flags=re.IGNORECASE | re.DOTALL)
    if aggregate:
        function, argument = aggregate.group(1).lower(), aggregate.group(2)
        distinct = argument.startswith("DISTINCT ")
        column = argument.removeprefix("DISTINCT ")
        length = re.fullmatch(r"length\((\w+)\)", column, re.IGNORECASE)
        if length:
            column = length.group(1)
        column = re.sub(r" [+-] \d+$", "", column)
        if alias is None:
            if function == "count" and argument == "*":
                alias = "count_all" if query_id in (1, 2, 21) else f"count_all_{index}"
            else:
                alias = f"{function}_{'distinct_' if distinct else ''}{column}_{index}"
        if function in ("sum", "avg"):
            # Preserve per-input F64 conversion and widen before addition.
            # Every admitted operand in these pinned queries is integral. Sum
            # those converted values exactly in HUGEINT, then round the total
            # once to DOUBLE. Ordinary floating AVG itself drifted beyond the
            # acceptance tolerance on Q4; an oracle must not impose that error.
            # Overflow is an explicit baseline failure, never a wrapped total.
            additive = re.fullmatch(r"(\w+) ([+-]) (\d+)", argument)
            argument = (f"CAST({additive[1]} AS DOUBLE) {additive[2]} {additive[3]}"
                        if additive else f"CAST({dialect_expression(argument)} AS DOUBLE)")
            expression = f"CAST(SUM(CAST({argument} AS HUGEINT)) AS DOUBLE)"
            if function == "avg":
                expression += f" / COUNT({argument})"
        else:
            expression = dialect_expression(expression)
    else:
        if alias is None:
            additive = re.fullmatch(r"(\w+) - ([123])", expression)
            if additive:
                alias = f"{additive[1]}_minus{additive[2]}"
            elif expression == "1" and query_id == 35:
                alias = "literal_1"
            elif re.fullmatch(r"\w+", expression):
                alias = expression
            else:
                raise ValueError(f"unmapped reference projection: {expression}")
        expression = dialect_expression(expression)
    return expression, alias


def query_plan(query_path: Path) -> list[dict]:
    raw, statements = query_statements(query_path)
    if hashlib.sha256(raw).hexdigest() != QUERIES_SHA256:
        raise ValueError("independent equivalents require the exact pinned query file")
    plans = []
    for query_id, statement in enumerate(statements, 1):
        projection, tail = statement.removeprefix("SELECT ").split(" FROM hits", 1)
        names = []
        if projection == "*":
            selected = f"* EXCLUDE ({quote_identifier(ORDINAL)})"
        else:
            selected_parts = []
            for index, item in enumerate(split_projection(projection)):
                expression, alias = projection_item(item, index, query_id)
                names.append(alias)
                selected_parts.append(f"{expression} AS {quote_identifier(alias)}")
            if len(set(names)) != len(names):
                raise ValueError("duplicate independent output names")
            selected = ", ".join(selected_parts)
        tail = dialect_expression(tail)
        tail = re.sub(r"'2013-\d{2}-\d{2}'", lambda match: str(
            (dt.date.fromisoformat(match[0][1:-1]) - dt.date(1970, 1, 1)).days), tail)
        # Canonical expressions in ORDER BY/HAVING still bind directly. Their
        # aggregate aliases are unchanged when explicitly present in the query.
        before_limit, limit = tail.split(" LIMIT ", 1) if " LIMIT " in tail else (tail, None)
        if query_id == 18:
            before_limit += f" ORDER BY MIN({quote_identifier(ORDINAL)})"
        elif query_id in GROUP_KEYS:
            if " ORDER BY " not in before_limit:
                raise ValueError("missing grouped reference ordering")
            before_limit += ", " + ", ".join(
                quote_identifier(key) + " ASC NULLS FIRST" for key in GROUP_KEYS[query_id])
        elif query_id in (24, 25, 26, 27):
            before_limit += f", {quote_identifier(ORDINAL)} ASC"
        elif query_id == 20:
            before_limit += f" ORDER BY {quote_identifier(ORDINAL)} ASC"
        sql = "SELECT " + selected + " FROM hits" + before_limit
        if limit is not None:
            sql += " LIMIT " + limit
        plans.append(dict(query_id=query_id, statement=statement, equivalent_sql=sql,
                          output_names=names or None,
                          group_tie_keys=GROUP_KEYS.get(query_id, []),
                          source_order=query_id in (18, 20, 24, 25, 26, 27)))
    return plans


def execute_plan(connection, plan: dict) -> dict:
    """Execute one independent SELECT, retaining all rows and baseline types."""
    result = connection.execute(plan["equivalent_sql"])
    columns = [(entry[0], str(entry[1])) for entry in result.description]
    names = [entry[0] for entry in columns]
    if len(set(names)) != len(names) or (plan["output_names"] is not None and names != plan["output_names"]):
        raise ValueError("independent result schema does not match the frozen projection")
    values = [dict(zip(names, row, strict=True)) for row in result.fetchall()]
    # No rounding, type coercion, result repair, truncation or native-value input.
    json.dumps(values, allow_nan=False)
    return dict(query_id=plan["query_id"], statement=plan["statement"],
                equivalent_sql=plan["equivalent_sql"], columns=columns, values=values)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--query-id", type=int, required=True, choices=range(1, 44))
    parser.add_argument("--temp-directory", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-gib", type=int, default=6)
    parser.add_argument("--temp-gib", type=int, default=32)
    parser.add_argument("--supervisor-pid", type=int, required=True)
    parser.add_argument("--lock-directory", type=Path, required=True)
    args = parser.parse_args()
    # This is the finite worker, not a standalone large-data runner. Its caller
    # must own the shared lock, storage/concurrency watch, deadline and process
    # group drain. The accepted packet retains that exact supervisor and protocol.
    lock_owner = json.loads((args.lock_directory / "owner.json").read_text())
    if (args.supervisor_pid != os.getppid()
            or lock_owner.get("pid") != args.supervisor_pid
            or lock_owner.get("kind") != "independent_clickbench_reference"):
        parser.error("the recorded independent-reference supervisor must own this worker")
    if min(args.threads, args.memory_gib, args.temp_gib) <= 0:
        parser.error("positive baseline resource limits are required")
    from local_uat_storage import require_local_path
    parquet = require_local_path(args.parquet, Path.home(), sys.platform)
    temporary = require_local_path(args.temp_directory, Path.home(), sys.platform)
    if not parquet.is_file() or temporary.exists() or not temporary.parent.is_dir():
        parser.error("a resident input and a new owned temporary directory are required")
    plan = query_plan(args.queries)[args.query_id - 1]
    import duckdb  # optional oracle only; never a ShardLoom runtime dependency
    settings = dict(threads=args.threads, memory_limit=f"{args.memory_gib}GiB",
                    temp_directory=str(temporary), max_temp_directory_size=f"{args.temp_gib}GiB",
                    preserve_insertion_order=True, default_null_order="NULLS_FIRST",
                    autoinstall_known_extensions=False, autoload_known_extensions=False)
    with duckdb.connect(config=settings) as connection:
        # file_row_number is absolute within this single immutable source file.
        connection.from_parquet(str(parquet), file_row_number=True).create_view("reference_input")
        description = connection.execute("DESCRIBE reference_input").fetchall()
        source_columns = [entry[0] for entry in description]
        source_types = {entry[0]: entry[1] for entry in description}
        validate_arithmetic_schema(source_types)
        if ORDINAL in source_columns or source_columns.count("file_row_number") != 1:
            raise ValueError("ambiguous independent source-row identity")
        connection.execute(f"CREATE VIEW hits AS SELECT * EXCLUDE (file_row_number), "
                           f"file_row_number AS {quote_identifier(ORDINAL)} FROM reference_input")
        record = execute_plan(connection, plan)
        record.update(oracle="duckdb", oracle_version=duckdb.__version__,
                      engine_version=connection.execute("SELECT version()").fetchone()[0],
                      settings=settings, source_columns=source_columns, source_types=source_types,
                      baseline_only=True, native_runtime_invoked=False)
    print(json.dumps(record, allow_nan=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
