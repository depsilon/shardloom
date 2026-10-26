# SPDX-License-Identifier: Apache-2.0
"""Explicit external baseline using idiomatic Polars lazy expressions.

This module is benchmark-only and is never imported by ShardLoom execution.
SQL null sums, non-null distinct counts, count widths and signed remainder are
declared in the lazy request, not repaired in results after execution.
"""
from __future__ import annotations

from dataclasses import replace
import os
import operator
import time

from .adapters import AdapterGap
from .semantics import ResultMismatch, TypedResult, arrow_schema, compare, schema_record


def dtype(value, pl):
    scalars = {"int8": pl.Int8, "int16": pl.Int16, "int32": pl.Int32, "int64": pl.Int64,
        "uint8": pl.UInt8, "uint16": pl.UInt16, "uint32": pl.UInt32, "uint64": pl.UInt64,
        "float32": pl.Float32, "float64": pl.Float64, "bool": pl.Boolean, "utf8": pl.String,
        "binary": pl.Binary, "date32": pl.Date, "timestamp_micros": pl.Datetime("us"), "null": pl.Null}
    if value.name in scalars:
        return scalars[value.name]
    if value.name == "decimal128":
        return pl.Decimal(*value.parameters)
    raise AdapterGap(f"Polars adapter has no reviewed dtype declaration: {value}")


def expression(expr, pl):
    if expr.op == "column":
        return pl.col(expr.value)
    if expr.op == "literal":
        return pl.lit(expr.value, dtype=dtype(expr.dtype, pl))
    args = [expression(arg, pl) for arg in expr.args]
    binary = {"add": operator.add, "sub": operator.sub, "mul": operator.mul,
              "eq": operator.eq, "ne": operator.ne, "lt": operator.lt, "le": operator.le,
              "gt": operator.gt, "ge": operator.ge, "and": operator.and_, "or": operator.or_}
    if expr.op in binary:
        return binary[expr.op](*args)
    if expr.op == "mod":
        a, b = args
        remainder = a % b
        # Polars/Python floor remainder has the divisor's sign. SQL's declared
        # remainder truncates toward zero. No float conversion loses int64 bits.
        return pl.when(((a < 0) != (b < 0)) & (remainder != 0)).then(remainder - b).otherwise(remainder)
    if expr.op in {"is_null", "is_not_null"}:
        return getattr(args[0], expr.op)()
    if expr.op == "not":
        return ~args[0]
    if expr.op == "neg":
        return -args[0]
    if expr.op in {"cast", "try_cast"}:
        return args[0].cast(dtype(expr.dtype, pl), strict=expr.op == "cast")
    if expr.op == "call":
        name = expr.value
        if name == "coalesce":
            return pl.coalesce(args)
        if name == "nullif":
            return pl.when(args[0] == args[1]).then(None).otherwise(args[0])
        if name == "concat":
            return pl.concat_str(args, separator="", ignore_nulls=False)
        if name in {"length", "octet_length", "trim", "upper", "lower"}:
            method = {"length": "len_chars", "octet_length": "len_bytes", "trim": "strip_chars",
                      "upper": "to_uppercase", "lower": "to_lowercase"}[name]
            return getattr(args[0].str, method)().cast(dtype(expr.dtype, pl))
        if name.startswith(("date_", "timestamp_")):
            method = name.split("_", 1)[1]
            if method in {"year", "month", "day", "hour", "minute", "second"}:
                return getattr(args[0].dt, method)().cast(dtype(expr.dtype, pl))
        if name in {"abs", "floor", "ceil"}:
            return getattr(args[0], name)()
    raise AdapterGap(f"Polars adapter has no reviewed expression: {expr.op}/{expr.value}")


def aggregate(measure, pl):
    arg = expression(measure.argument, pl) if measure.argument is not None else None
    if measure.function == "count":
        value = pl.len() if arg is None else arg.count()
    elif measure.function == "count_distinct":
        value = arg.drop_nulls().n_unique()
    elif measure.function == "sum":
        value = pl.when(arg.count() > 0).then(arg.sum()).otherwise(None)
    elif measure.function in {"avg", "min", "max"}:
        value = getattr(arg, "mean" if measure.function == "avg" else measure.function)()
    else:
        raise AdapterGap(f"Polars adapter has no reviewed aggregate: {measure.function}")
    return value.cast(dtype(measure.dtype, pl)).alias(measure.alias)


def declaration(relation, sources, pl):
    op, args = relation.op, relation.args
    if op == "scan":
        return pl.scan_ipc(sources[args[0]])
    inputs = [declaration(child, sources, pl) for child in relation.inputs]
    source = inputs[0]
    if op == "project":
        return source.select(*(expression(expr, pl).alias(name) for name, expr in args))
    if op == "filter":
        return source.filter(expression(args[0], pl))
    if op == "aggregate":
        keys, measures = args
        values = [aggregate(measure, pl) for measure in measures]
        return source.group_by(list(keys)).agg(values) if keys else source.select(values)
    if op == "sort":
        return source.sort([key.column for key in args], descending=[key.descending for key in args],
                           nulls_last=[key.nulls == "last" for key in args], maintain_order=True)
    if op == "limit":
        return source.slice(args[1], args[0])
    if op == "distinct":
        return source.unique()
    if op == "set":
        if args[0] in {"union_all", "union_distinct"}:
            combined = pl.concat(inputs)
            return combined if args[0] == "union_all" else combined.unique()
        return source.unique().join(inputs[1].unique(), on=list(dict(relation.schema)),
            how="semi" if args[0] == "intersect" else "anti", nulls_equal=True)
    if op == "join":
        keys, columns, kind = args
        left = source.rename({name: "left_" + name for name, _ in relation.inputs[0].schema})
        right = inputs[1].rename({name: "right_" + name for name, _ in relation.inputs[1].schema})
        joined = left.join(right, left_on=["left_" + a for a, _ in keys],
            right_on=["right_" + b for _, b in keys], how=kind, nulls_equal=False, coalesce=False)
        return joined.select(*(pl.col(side + "_" + name).alias(alias) for side, name, alias in columns))
    raise AdapterGap(f"Polars adapter has no reviewed relation: {op}")


def _without_nullability(value):
    parameters = tuple(_without_nullability(p) if hasattr(p, "nullable") else
                       (p[0], _without_nullability(p[1])) if isinstance(p, tuple) and len(p) == 2
                       and hasattr(p[1], "nullable") else p for p in value.parameters)
    return replace(value, nullable=True, parameters=parameters)


def compare_result(wanted, actual, contract):
    def logical(schema):
        return tuple((name, _without_nullability(value)) for name, value in schema)
    if logical(wanted.schema) != logical(actual.schema):
        raise ResultMismatch("Polars observable field names/order/logical dtypes differ")
    # Polars does not expose a nonnullable field contract. Check all values under
    # our declared null rules after validating every observable logical dtype.
    compare(wanted, TypedResult(wanted.schema, actual.rows), contract)


def execute(relation, sources, *, parallelism):
    os.environ["POLARS_MAX_THREADS"] = str(parallelism)
    import polars as pl

    if pl.thread_pool_size() != parallelism:
        raise ValueError("Polars was initialized before the declared thread limit")
    started = time.perf_counter()
    plan = declaration(relation, sources, pl)
    constructed = time.perf_counter()
    frame = plan.collect(engine="auto")
    collected = time.perf_counter()
    output = frame.to_arrow()
    schema, rows = arrow_schema(output.schema), output.to_pylist()
    consumed = time.perf_counter()
    return TypedResult(schema, rows), {"execution_engine": "polars", "version": pl.__version__,
        "thread_pool_size": pl.thread_pool_size(), "selected_engine": "auto",
        "complete_consumption": True, "execution_mode": "first_query_fresh_session",
        "input_format": "original_arrow_ipc", "public_declaration": relation.record(),
        "logical_plan": plan.explain(optimized=False), "optimized_plan": plan.explain(optimized=True),
        "plan_capture": "after timed complete consumption; no second collect",
        "observed_schema": schema_record(schema),
        "schema_comparison": "exact names/order/logical types; nullability metadata is not exposed by Polars",
        "clocks": {"setup_and_declaration_seconds": constructed - started,
                   "collect_call_seconds": collected - constructed,
                   "delivery_seconds": consumed - collected,
                   "complete_workflow_seconds": consumed - started,
                   "engine_execution_seconds": None},
        "clock_boundary": "explicit pre-clock engine import excluded; lazy declaration through complete Arrow and Python-row output included; "
                          "initialization/imports triggered inside those APIs are included; "
                          "no engine-only Polars clock is claimed"}
