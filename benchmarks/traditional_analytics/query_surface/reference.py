# SPDX-License-Identifier: Apache-2.0
"""Independent bounded Python expectations for reviewed corpus declarations.

Test-only calculations over regenerated records, never candidate execution or
an engine fallback. No query text is parsed and no native output is reused.
Only the deliberately enumerated semantic subset below has an oracle here.
"""
from __future__ import annotations

from collections import defaultdict
import datetime as dt
from decimal import Decimal, localcontext
from functools import cmp_to_key
from itertools import islice
import math
import operator
import heapq

from .datasets import RowGenerator, encoded_row
from .semantics import TypedResult


class ReferenceGap(NotImplementedError):
    pass


def scalar(expr, row):
    if expr.op == "column":
        return row[expr.value]
    if expr.op == "literal":
        return expr.value
    if expr.op == "call" and expr.value == "coalesce":
        for arg in expr.args:
            value = scalar(arg, row)
            if value is not None:
                return value
        return None
    args = [scalar(arg, row) for arg in expr.args]
    if expr.op == "is_null":
        return args[0] is None
    if expr.op == "is_not_null":
        return args[0] is not None
    if expr.op == "and":
        return False if False in args else None if None in args else True
    if expr.op == "or":
        return True if True in args else None if None in args else False
    if expr.op == "call" and expr.value == "nullif":
        return None if args[0] == args[1] else args[0]
    if any(value is None for value in args):
        return None
    binary = {"add": operator.add, "sub": operator.sub, "mul": operator.mul,
              "eq": operator.eq, "ne": operator.ne, "lt": operator.lt, "le": operator.le,
              "gt": operator.gt, "ge": operator.ge}
    if expr.op in binary:
        return binary[expr.op](*args)
    if expr.op == "mod":
        # SQL remainder truncates toward zero; Python % follows the divisor.
        a, b = args
        quotient = abs(a) // abs(b)
        quotient = -quotient if (a < 0) != (b < 0) else quotient
        return a - quotient * b
    if expr.op == "not":
        return not args[0]
    if expr.op == "neg":
        return -args[0]
    if expr.op in {"cast", "try_cast"}:
        constructors = {"int64": int, "uint64": int, "float64": float, "utf8": str}
        if expr.dtype.name not in constructors:
            raise ReferenceGap(f"reference cast has no reviewed contract: {expr.dtype}")
        try:
            return constructors[expr.dtype.name](args[0])
        except (ValueError, OverflowError):
            if expr.op == "try_cast":
                return None
            raise
    if expr.op == "call":
        unary = {"abs": abs, "length": len, "octet_length": lambda x: len(x.encode("utf-8")),
                 "upper": str.upper, "lower": str.lower, "trim": str.strip,
                 "floor": lambda x: float(math.floor(x)), "ceil": lambda x: float(math.ceil(x))}
        if expr.value in unary:
            return unary[expr.value](*args)
        if expr.value == "concat":
            return "".join(args)
        if expr.value in {"date_year", "date_month", "date_day"}:
            value = dt.date(1970, 1, 1) + dt.timedelta(days=args[0])
            return getattr(value, expr.value.removeprefix("date_"))
        if expr.value.startswith("timestamp_"):
            part = expr.value.removeprefix("timestamp_")
            if part in {"year", "month", "day", "hour", "minute", "second"}:
                return getattr(dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=args[0]), part)
    raise ReferenceGap(f"reference expression is not reviewed: {expr.op}/{expr.value}")


def _compare_rows(keys):
    def compare(a, b):
        for key in keys:
            x, y = a[key.column], b[key.column]
            if x is None or y is None:
                order = (x is None) - (y is None)
                order *= 1 if key.nulls == "last" else -1
            else:
                order = (x > y) - (x < y)
                order *= -1 if key.descending else 1
            if order:
                return order
        return 0
    return compare


def _aggregate(relation, spec):
    keys, measures = relation.args
    needed = set(keys).union(*(m.argument.columns() for m in measures if m.argument is not None))
    # Independent expected sums retain Python's exact integer arithmetic. Floating
    # sums use fsum over each state's values rather than the engine's reduction.
    def new_state():
        return [{"count": 0, "values": [], "distinct": set(), "min": None, "max": None} for _ in measures]
    groups = {} if keys else {(): new_state()}
    for row in records(relation.inputs[0], spec, needed):
        key = tuple(row[name] for name in keys)
        if key not in groups:
            groups[key] = new_state()
        for measure, state in zip(measures, groups[key]):
            value = 1 if measure.argument is None else scalar(measure.argument, row)
            if value is None:
                continue
            state["count"] += 1
            if measure.function == "count_distinct":
                state["distinct"].add(encoded_row(value))
            elif measure.function in {"sum", "avg"}:
                state["values"].append(value)
            elif measure.function in {"min", "max"}:
                current = state[measure.function]
                state[measure.function] = value if current is None else (min if measure.function == "min" else max)(value, current)
            elif measure.function != "count":
                raise ReferenceGap(f"unreviewed aggregate reference: {measure.function}")
    for key, states in groups.items():
        result = dict(zip(keys, key))
        for measure, state in zip(measures, states):
            function = measure.function
            if function == "count":
                value = state["count"]
            elif function == "count_distinct":
                value = len(state["distinct"])
            elif function in {"min", "max"}:
                value = state[function]
            elif not state["count"]:
                value = None
            else:
                values = state["values"]
                with localcontext() as context:
                    context.prec = 100
                    total = math.fsum(values) if any(type(v) is float for v in values) else sum(values)
                    value = total / state["count"] if function == "avg" else total
                    if measure.dtype.name.startswith("float"):
                        value = float(value)
                    elif measure.dtype.name == "decimal128":
                        value = Decimal(value)
            result[measure.alias] = value
        yield result


def records(relation, spec, requested=None):
    """Stream reference inputs; only stateful operations retain projected rows."""
    names = set(dict(relation.schema)) if requested is None else set(requested)
    op, args = relation.op, relation.args
    def project(row):
        return {name: row[name] for name in dict(relation.schema) if name in names}
    if op == "scan":
        yield from RowGenerator(spec, args[0]).rows(tuple(name for name, _ in relation.schema if name in names))
    elif op == "project":
        expressions = [(name, expr) for name, expr in args if name in names]
        needed = set().union(*(expr.columns() for _, expr in expressions))
        for row in records(relation.inputs[0], spec, needed):
            yield {name: scalar(expr, row) for name, expr in expressions}
    elif op == "filter":
        for row in records(relation.inputs[0], spec, names | args[0].columns()):
            if scalar(args[0], row) is True:
                yield project(row)
    elif op == "aggregate":
        for row in _aggregate(relation, spec):
            yield project(row)
    elif op == "sort":
        needed = names | {key.column for key in args}
        for row in sorted(records(relation.inputs[0], spec, needed), key=cmp_to_key(_compare_rows(args))):
            yield project(row)
    elif op == "limit":
        child = relation.inputs[0]
        if child.op == "sort":
            needed = names | {key.column for key in child.args}
            bounded = heapq.nsmallest(args[0] + args[1], records(child.inputs[0], spec, needed),
                                      key=cmp_to_key(_compare_rows(child.args)))
            yield from (project(row) for row in bounded[args[1]:])
        else:
            yield from islice(records(child, spec, names), args[1], args[1] + args[0])
    elif op == "distinct":
        seen = set()
        for row in records(relation.inputs[0], spec):
            key = encoded_row(row)
            if key not in seen:
                seen.add(key)
                yield project(row)
    elif op == "set":
        if args[0] == "union_all":
            for child in relation.inputs:
                yield from records(child, spec, names)
        else:
            left = {encoded_row(row): row for row in records(relation.inputs[0], spec)}
            right = {encoded_row(row): row for row in records(relation.inputs[1], spec)}
            selected = {"union_distinct": left.keys() | right.keys(), "intersect": left.keys() & right.keys(),
                        "except": left.keys() - right.keys()}[args[0]]
            for key in sorted(selected):
                yield project(left[key] if key in left else right[key])
    elif op == "join":
        keys, columns, kind = args
        left_names = {key[0] for key in keys} | {source for side, source, alias in columns if side == "left" and alias in names}
        right_names = {key[1] for key in keys} | {source for side, source, alias in columns if side == "right" and alias in names}
        right = list(records(relation.inputs[1], spec, right_names))
        index, matched = defaultdict(list), set()
        for i, row in enumerate(right):
            key = tuple(row[b] for _, b in keys)
            if None not in key:
                index[key].append(i)
        def output(left, right):
            return {alias: (left if side == "left" else right).get(source)
                    for side, source, alias in columns if alias in names}
        for left in records(relation.inputs[0], spec, left_names):
            key = tuple(left[a] for a, _ in keys)
            matches = [] if None in key else index[key]
            if kind in {"semi", "anti"}:
                if bool(matches) == (kind == "semi"):
                    yield output(left, {})
            elif matches:
                for i in matches:
                    matched.add(i)
                    yield output(left, right[i])
            elif kind in {"left", "full"}:
                yield output(left, {})
        if kind in {"right", "full"}:
            for i, row in enumerate(right):
                if i not in matched:
                    yield output({}, row)
    else:
        raise ReferenceGap(f"reference relation is not reviewed: {op}")


def expected(case, spec, *, max_output_rows=100_000):
    if case.relation is None:
        raise ReferenceGap("proposed capability has no independent expectation yet")
    rows = list(islice(records(case.relation, spec), max_output_rows + 1))
    if len(rows) > max_output_rows:
        raise ReferenceGap("reference output exceeds the declared complete-result budget")
    return TypedResult(case.contract.schema, rows)
