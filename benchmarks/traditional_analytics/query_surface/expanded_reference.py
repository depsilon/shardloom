# SPDX-License-Identifier: Apache-2.0
"""Independent, test-only SQL semantics over original Arrow values in stdlib SQLite.

The benchmark's native and Polars renderers have no dependency on this module.
Only the admitted expansion domains are handled. Original cases retain their
independent seeded-Python expectations.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
import json
import math
from pathlib import Path
import sqlite3

from .semantics import TypedResult
from .specification import identifier


def q(name):
    return '"' + identifier(name) + '"'


def expr(e):
    if e.op == "column":
        return q(e.value)
    if e.op == "literal":
        if e.value is None:
            return "NULL"
        if type(e.value) is bool:
            return "1" if e.value else "0"
        if type(e.value) in (int, float) and math.isfinite(e.value):
            return str(e.value)
        if isinstance(e.value, str):
            return "'" + e.value.replace("'", "''") + "'"
        raise ValueError(f"no oracle literal for {e.dtype}")
    args = [expr(a) for a in e.args]
    operators = dict(
        add="+",
        sub="-",
        mul="*",
        mod="%",
        eq="=",
        ne="<>",
        lt="<",
        le="<=",
        gt=">",
        ge=">=",
        **{"and": "AND", "or": "OR"},
    )
    if e.op in operators:
        return f"({args[0]} {operators[e.op]} {args[1]})"
    if e.op in ("is_null", "is_not_null"):
        return f"({args[0]} IS {'NOT ' if e.op == 'is_not_null' else ''}NULL)"
    if e.op == "call":
        names = {
            "coalesce": "COALESCE",
            "nullif": "NULLIF",
            "upper": "u_upper",
            "lower": "u_lower",
            "length": "u_len",
            "trim": "u_trim",
            "octet_length": "u_octets",
        }
        name = names.get(e.value)
        if name is None and e.value in {
            a + "_" + b for a in ("date", "timestamp") for b in ("year", "month", "day")
        }:
            name = e.value
        if name is not None:
            return f"{name}({','.join(args)})"
    raise ValueError(f"no independent oracle for expression {e.op}/{e.value}")


def reducer(m):
    f = m.function
    a = "*" if m.argument is None else expr(m.argument)
    if f in ("sum", "avg", "min", "max", "count"):
        return f"{f}({a})"
    if f in ("count_distinct", "sum_distinct", "avg_distinct"):
        return f"{f.split('_')[0]}(DISTINCT {a})"
    if f == "median":
        return f"exact_median({a})"
    if f in ("var_pop", "var_samp", "stddev_pop", "stddev_samp"):
        # Inputs here are bounded integers. Cast before squaring the sum, which
        # may exceed 64 bits. n=0/1 and sample division are explicit.
        n = f"COUNT({a})"
        square = f"SUM({a} * {a})"
        delta = f"({square} - (1.0 * SUM({a}) * SUM({a})) / {n})"
        ddof = int(f.endswith("samp"))
        value = f"({delta} / ({n} - {ddof}))"
        if f.startswith("stddev"):
            value = f"ref_sqrt({value})"
        return f"CASE WHEN {n} > {ddof} THEN {value} END"
    raise ValueError("no independent oracle reducer: " + f)


def statement(node):
    op, args = node.op, node.args
    if op == "scan":
        return "SELECT * FROM " + q(args[0])
    children = [statement(c) for c in node.inputs]
    source = "(" + children[0] + ") AS l"
    if op == "project":
        return "SELECT " + ",".join(f"{expr(e)} AS {q(n)}" for n, e in args) + " FROM " + source
    if op == "filter":
        return "SELECT * FROM " + source + " WHERE " + expr(args[0])
    if op == "aggregate":
        keys, ms = args
        fields = [q(k) for k in keys] + [f"{reducer(m)} AS {q(m.alias)}" for m in ms]
        return (
            "SELECT "
            + ",".join(fields)
            + " FROM "
            + source
            + (" GROUP BY " + ",".join(map(q, keys)) if keys else "")
        )
    if op == "sort":
        keys = [f"{q(k.column)} {'DESC' if k.descending else 'ASC'} NULLS {k.nulls.upper()}" for k in args]
        return "SELECT * FROM " + source + " ORDER BY " + ",".join(keys)
    if op == "limit":
        return children[0] + f" LIMIT {args[0]} OFFSET {args[1]}"
    if op == "distinct":
        return "SELECT DISTINCT * FROM " + source
    if op == "set":
        verb = {
            "union_all": "UNION ALL",
            "union_distinct": "UNION",
            "intersect": "INTERSECT",
            "except": "EXCEPT",
        }[args[0]]
        return "SELECT * FROM " + source + " " + verb + " SELECT * FROM (" + children[1] + ")"
    if op == "join":
        keys, columns, kind = args
        right = "(" + children[1] + ") AS r"
        on = " AND ".join(f"l.{q(a)}=r.{q(b)}" for a, b in keys)
        fields = ",".join(
            f"{'l' if side == 'left' else 'r'}.{q(n)} AS {q(alias)}" for side, n, alias in columns
        )
        if kind in ("semi", "anti"):
            verb = "EXISTS" if kind == "semi" else "NOT EXISTS"
            return f"SELECT {fields} FROM {source} WHERE {verb} (SELECT 1 FROM {right} WHERE {on})"
        if kind == "right":
            return f"SELECT {fields} FROM {right} LEFT JOIN {source} ON {on}"
        if kind == "full":
            # SQLite can repeatedly scan an unindexed materialized RHS for a
            # filtered FULL JOIN. Keep all left matches, then emit each right
            # row with no SQL-equal left key exactly once, including NULLs.
            unmatched = ",".join(
                f"{'NULL' if side == 'left' else 'r.' + q(n)} AS {q(alias)}" for side, n, alias in columns
            )
            return (
                f"SELECT {fields} FROM {source} LEFT JOIN {right} ON {on} UNION ALL "
                f"SELECT {unmatched} FROM {right} WHERE NOT EXISTS (SELECT 1 FROM {source} WHERE {on})"
            )
        if kind not in ("inner", "left"):
            raise ValueError(kind)
        return f"SELECT {fields} FROM {source} {kind.upper()} JOIN {right} ON {on}"
    if op == "window_case":
        fn, part, arg, descending, dist = args
        over = f"PARTITION BY {q(part)} ORDER BY row_id {'DESC' if descending else 'ASC'}"
        if fn == "row_number":
            formula = "ROW_NUMBER()"
        elif fn in ("lag", "lead"):
            formula = f"{fn}({q(arg)},{dist})"
        else:
            formula = f"SUM({q(arg)})"
            over += f" ROWS BETWEEN {dist} PRECEDING AND CURRENT ROW"
        return f"SELECT row_id,{formula} OVER ({over}) AS value FROM {source}"
    if op == "asof_case":
        lk, rk, direction, exact, tolerance = args
        compare = ("<" if direction == "backward" else ">") + ("=" if exact else "")
        ordered = "DESC" if direction == "backward" else "ASC"
        condition = f"r.{q(rk)} {compare} l.{q(lk)}"
        if tolerance is not None:
            condition += f" AND ABS(r.{q(rk)}-l.{q(lk)})<={tolerance}"
        nearest = f"FROM ({children[1]}) AS r WHERE {condition} ORDER BY r.{q(rk)} {ordered} LIMIT 1"
        return (
            f"SELECT l.row_id,(SELECT r.day_index {nearest}) AS matched_day_index,"
            f"(SELECT r.week_index {nearest}) AS week_index FROM {source}"
        )
    if op == "explode_case":
        columns = [f"element.value AS items" if n == "items" else f"l.{q(n)}" for n, _ in node.schema]
        return (
            "SELECT "
            + ",".join(columns)
            + " FROM "
            + source
            + " CROSS JOIN json_each(COALESCE(l.items,'[null]')) AS element"
        )
    if op == "membership_case":
        left, right, negated = args
        verb = "NOT IN" if negated else "IN"
        return f"SELECT l.* FROM {source} WHERE l.{q(left)} {verb} (SELECT {q(right)} FROM ({children[1]}))"
    if op == "materialized_case":
        return (
            f"WITH twice AS MATERIALIZED ({children[0]}) SELECT * FROM twice UNION ALL "
            f"SELECT * FROM twice WHERE value>{args[0]}"
        )
    if op == "recursive_case":
        step, depth = args
        # Closed-form multiset oracle for repeated, bounded affine frontiers.
        # Depth comes from a literal relation, independent of the native CTE.
        levels = " UNION ALL ".join(f"SELECT {i} AS step_number" for i in range(depth + 1))
        return (
            f"SELECT l.bucket, l.value + ({step}) * step_number AS value FROM {source} CROSS JOIN ({levels})"
        )
    raise ValueError("no independent oracle for " + op)


class Median:
    def __init__(self):
        self.values = []

    def step(self, value):
        if value is not None:
            self.values.append(value)

    def finalize(self):
        if self.values:
            import statistics

            return float(statistics.median(self.values))
        return None


def open_database(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    db.execute("PRAGMA cache_size=-131072")
    db.execute("PRAGMA temp_store=FILE")
    for name, f in {
        "u_upper": str.upper,
        "u_lower": str.lower,
        "u_trim": str.strip,
        "u_len": len,
        "u_octets": lambda v: len(v.encode()),
        "ref_sqrt": lambda v: math.sqrt(max(0, v)),
    }.items():
        db.create_function(name, 1, lambda v, f=f: None if v is None else f(v), deterministic=True)
    for name, epoch, make_delta in (
        ("date", dt.date(1970, 1, 1), lambda v: dt.timedelta(days=v)),
        ("timestamp", dt.datetime(1970, 1, 1), lambda v: dt.timedelta(microseconds=v)),
    ):
        for attr in ("year", "month", "day"):
            db.create_function(
                name + "_" + attr,
                1,
                lambda v, e=epoch, f=make_delta, a=attr: None if v is None else getattr(e + f(v), a),
                deterministic=True,
            )
    db.create_aggregate("exact_median", 1, Median)
    return db


def normalize(value, dtype):
    if value is None:
        return None
    if dtype.name == "bool":
        if value not in (0, 1):
            raise ValueError("oracle produced a nonboolean")
        return bool(value)
    if dtype.name == "decimal128":
        return Decimal(value)
    if dtype.name in ("list", "struct"):
        return json.loads(value)
    if dtype.name.startswith("float"):
        return float(value)
    return value


def expected(db, case, max_output_rows=10_000):
    query = statement(case.relation)
    cursor = db.execute(query)
    names = tuple(n for n, _ in case.contract.schema)
    if tuple(d[0] for d in cursor.description) != names:
        raise ValueError(f"independent result names differ for {case.id}")
    result = cursor.fetchmany(max_output_rows + 1)
    if len(result) > max_output_rows:
        raise ValueError(f"independent result exceeds frozen limit for {case.id}")
    rows = [{n: normalize(v, t) for (n, t), v in zip(case.contract.schema, row)} for row in result]
    return TypedResult(case.contract.schema, rows)
