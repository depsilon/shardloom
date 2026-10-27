# SPDX-License-Identifier: Apache-2.0
"""Thin, genuine public-interface declarations over one typed workload."""
from __future__ import annotations

from contextlib import ExitStack
import datetime as dt
from decimal import Decimal
import math
import operator
from pathlib import Path
import time

from shardloom._result_schema import schema_fields
from .semantics import TypedResult
from .specification import Expression, Relation, SURFACES, identifier


class AdapterGap(NotImplementedError):
    """No equivalent declaration in this adapter; never an engine rejection."""


BINARY = {"add": ("+", operator.add), "sub": ("-", operator.sub), "mul": ("*", operator.mul),
          "div": ("/", operator.truediv), "mod": ("%", operator.mod),
          "eq": ("=", operator.eq), "ne": ("<>", operator.ne), "lt": ("<", operator.lt),
          "le": ("<=", operator.le), "gt": (">", operator.gt), "ge": (">=", operator.ge),
          "and": ("AND", operator.and_), "or": ("OR", operator.or_)}


def quoted(name):
    # The corpus's admitted identifiers are validated ASCII names. Quoted and
    # reserved identifiers are separate capability cases, not silently rewritten.
    return identifier(name)


def sql_type(dtype):
    if dtype.name == "decimal128":
        return f"DECIMAL({dtype.parameters[0]},{dtype.parameters[1]})"
    return {"int64": "BIGINT", "uint64": "UBIGINT", "float64": "DOUBLE", "utf8": "VARCHAR",
            "bool": "BOOLEAN", "date32": "DATE", "timestamp_micros": "TIMESTAMP"}.get(dtype.name, dtype.name.upper())


def sql_literal(expression):
    value, dtype = expression.value, expression.dtype
    if value is None:
        return "NULL" if dtype.name == "null" else f"CAST(NULL AS {sql_type(dtype)})"
    if dtype.name == "date32":
        if type(value) is int:
            value = dt.date(1970, 1, 1) + dt.timedelta(days=value)
        return f"DATE '{value.isoformat()}'"
    if dtype.name == "timestamp_micros":
        if type(value) is int:
            value = dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=value)
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if dtype.name == "decimal128":
        return f"CAST('{value}' AS {sql_type(dtype)})"
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    if type(value) in (int, float):
        if type(value) is float and not math.isfinite(value):
            raise AdapterGap("nonfinite SQL literal requires an explicit native numeric policy")
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise AdapterGap(f"SQL literal is unimplemented for {dtype!r}")


def expression_sql(expression: Expression):
    op, args = expression.op, expression.args
    if op == "column":
        return quoted(expression.value)
    if op == "literal":
        return sql_literal(expression)
    if op in BINARY:
        return f"({expression_sql(args[0])} {BINARY[op][0]} {expression_sql(args[1])})"
    if op in {"is_null", "is_not_null"}:
        return f"({expression_sql(args[0])} IS {'NOT ' if op == 'is_not_null' else ''}NULL)"
    if op in {"not", "neg"}:
        return f"({'NOT ' if op == 'not' else '-'}{expression_sql(args[0])})"
    if op in {"cast", "try_cast"}:
        return f"{op.upper()}({expression_sql(args[0])} AS {sql_type(expression.dtype)})"
    if op == "call":
        return f"{identifier(expression.value).upper()}({','.join(map(expression_sql, args))})"
    raise AdapterGap(f"SQL expression adapter is absent: {op}")


def expression_native(expression: Expression, native):
    E, op = native.Expression, expression.op
    if op == "column":
        return E.column(expression.value)
    if op == "literal":
        value, dtype = expression.value, expression.dtype
        if dtype.name == "decimal128":
            parts = Decimal(value).as_tuple()
            shift = parts.exponent + dtype.parameters[1]
            coefficient = int("".join(map(str, parts.digits)))
            if shift < 0:
                if coefficient % 10 ** -shift:
                    raise ValueError("decimal literal discards digits")
                coefficient //= 10 ** -shift
            else:
                coefficient *= 10 ** shift
            return E.decimal128(-coefficient if parts.sign else coefficient, *dtype.parameters)
        if dtype.name in {"date32", "timestamp_micros", "uint64"} and value is not None:
            return getattr(E, dtype.name)(value)
        return E.literal(value)
    args = [expression_native(arg, native) for arg in expression.args]
    if op in BINARY:
        return BINARY[op][1](*args)
    if op in {"is_null", "is_not_null"}:
        return getattr(args[0], op)()
    if op in {"not", "neg"}:
        return ~args[0] if op == "not" else -args[0]
    if op in {"cast", "try_cast"}:
        return args[0].cast(expression.dtype.label, tolerant=op == "try_cast")
    if op == "call":
        return E.call(expression.value, args)
    raise AdapterGap(f"typed expression adapter is absent: {op}")


def expression_dataframe(expression: Expression, sl):
    from shardloom import functions as F
    op = expression.op
    if op == "column":
        return sl.col(expression.value)
    if op == "literal":
        value = expression.value
        if expression.dtype.name == "date32" and type(value) is int:
            value = dt.date(1970, 1, 1) + dt.timedelta(days=value)
        if expression.dtype.name == "timestamp_micros" and type(value) is int:
            value = dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=value)
        return F.lit(value)
    args = [expression_dataframe(arg, sl) for arg in expression.args]
    if op in BINARY:
        return BINARY[op][1](*args)
    if op in {"is_null", "is_not_null"}:
        return getattr(args[0], op)()
    if op in {"not", "neg"}:
        return ~args[0] if op == "not" else -args[0]
    if op in {"cast", "try_cast"}:
        return getattr(args[0], op)(expression.dtype)
    if op == "call":
        name = {"octet_length": "byte_length", "nullif": "null_if"}.get(expression.value, expression.value)
        function = getattr(F, name, None)
        if function is None:
            raise AdapterGap(f"DataFrame scalar constructor is absent: {name}")
        return function(*args)
    raise AdapterGap(f"DataFrame expression adapter is absent: {op}")


def _paths(sources, name):
    paths = sources.get(name)
    if not paths:
        raise ValueError(f"source binding is absent: {name}")
    return tuple(Path(path) for path in paths)


def source_format(path):
    """Explicit fixture format, shared by real native and reference adapters."""
    value = {".vortex": "vortex", ".vtx": "vortex", ".parquet": "parquet",
             ".arrow": "arrow-ipc", ".ipc": "arrow-ipc", ".feather": "arrow-ipc"}.get(Path(path).suffix.lower())
    if value is None:
        raise AdapterGap(f"fixture format is not declared by its supported extension: {path}")
    return value


def relation_sql(relation: Relation, sources):
    op, args = relation.op, relation.args
    if op == "scan":
        selects = ["SELECT * FROM '" + str(path).replace("'", "''") + "'" for path in _paths(sources, args[0])]
        return " UNION ALL ".join(selects)
    inputs = [relation_sql(child, sources) for child in relation.inputs]
    source = f"({inputs[0]}) AS q"
    if op == "project":
        return f"SELECT {','.join(expression_sql(expr) + ' AS ' + quoted(name) for name, expr in args)} FROM {source}"
    if op == "filter":
        return f"SELECT * FROM {source} WHERE {expression_sql(args[0])}"
    if op == "aggregate":
        keys, measures = args
        columns = [quoted(key) for key in keys]
        for measure in measures:
            arg = "*" if measure.argument is None else expression_sql(measure.argument)
            function = measure.function.upper()
            if function.endswith("_DISTINCT"):
                function, arg = function.removesuffix("_DISTINCT"), "DISTINCT " + arg
            columns.append(f"{function}({arg}) AS {quoted(measure.alias)}")
        return f"SELECT {','.join(columns)} FROM {source}" + (" GROUP BY " + ",".join(map(quoted, keys)) if keys else "")
    if op == "sort":
        return f"SELECT * FROM {source} ORDER BY " + ",".join(
            f"{quoted(key.column)} {'DESC' if key.descending else 'ASC'} NULLS {key.nulls.upper()}" for key in args)
    if op == "limit":
        return inputs[0] + f" LIMIT {args[0]} OFFSET {args[1]}"
    if op == "distinct":
        return f"SELECT DISTINCT * FROM {source}"
    if op == "set":
        kind = {"union_all": "UNION ALL", "union_distinct": "UNION", "intersect": "INTERSECT", "except": "EXCEPT"}[args[0]]
        return f"SELECT * FROM ({inputs[0]}) AS l {kind} SELECT * FROM ({inputs[1]}) AS r"
    if op == "join":
        keys, columns, kind = args
        projection = ",".join(f"{'f' if side == 'left' else 'd'}.{quoted(name)} AS {quoted(alias)}"
                              for side, name, alias in columns)
        on = " AND ".join(f"f.{quoted(a)} = d.{quoted(b)}" for a, b in keys)
        return f"SELECT {projection} FROM ({inputs[0]}) AS f {kind.upper()} JOIN ({inputs[1]}) AS d ON {on}"
    raise AdapterGap(f"SQL relation adapter is absent: {op}")


def relation_native(relation: Relation, sources, native):
    op, args = relation.op, relation.args
    if op == "scan":
        parts = [native.Relation.scan(str(path)) for path in _paths(sources, args[0])]
        result = parts[0]
        for part in parts[1:]:
            result = result.set_operation(part, kind="union_all")
        return result
    inputs = [relation_native(child, sources, native) for child in relation.inputs]
    source = inputs[0]
    if op == "project":
        return source.project([(name, expression_native(expr, native)) for name, expr in args])
    if op == "filter":
        return source.filter(expression_native(args[0], native))
    if op == "aggregate":
        keys, measures = args
        bindings, projection = [], [(key, native.Expression.column(key)) for key in keys]
        needs_projection = any(m.argument is not None and m.argument.op != "column" for m in measures)
        for i, measure in enumerate(measures):
            name = None
            if measure.argument is not None:
                name = f"__measure_{i}" if needs_projection else measure.argument.value
                if needs_projection:
                    projection.append((name, expression_native(measure.argument, native)))
            bindings.append((measure.function, name, measure.alias))
        if needs_projection:
            source = source.project(projection)
        return source.aggregate(list(keys), bindings)
    if op == "sort":
        return source.sort([(key.column, key.descending, key.nulls) for key in args])
    if op == "limit":
        return source.limit(args[0], offset=args[1])
    if op == "distinct":
        return source.unary("distinct_rows")
    if op == "set":
        return source.set_operation(inputs[1], kind=args[0])
    if op == "join":
        kind = {"semi": "left_semi", "anti": "left_anti"}.get(args[2], args[2])
        return source.join(inputs[1], list(args[0]), list(args[1]), kind=kind)
    raise AdapterGap(f"typed relation adapter is absent: {op}")


def relation_dataframe(relation: Relation, sources, sl, client):
    op, args = relation.op, relation.args
    if op == "scan":
        readers = {"vortex": sl.read_vortex, "parquet": sl.read_parquet,
                   "arrow-ipc": sl.read_arrow_ipc}
        parts = [readers[source_format(path)](path, client=client) for path in _paths(sources, args[0])]
        result = parts[0]
        for part in parts[1:]:
            result = result.union_all(part)
        return result
    inputs = [relation_dataframe(child, sources, sl, client) for child in relation.inputs]
    source = inputs[0]
    if op == "project":
        return source.select(*(expression_dataframe(expr, sl).alias(name) for name, expr in args))
    if op == "filter":
        return source.filter(expression_dataframe(args[0], sl))
    if op == "aggregate":
        keys, measures = args
        # Aggregate strings are the current public DataFrame aggregate contract;
        # the relation is still constructed through DataFrame operations.
        declarations = []
        for measure in measures:
            arg = "*" if measure.argument is None else expression_sql(measure.argument)
            function = measure.function.upper()
            if function.endswith("_DISTINCT"):
                function, arg = function.removesuffix("_DISTINCT"), "DISTINCT " + arg
            declarations.append(f"{function}({arg}) AS {quoted(measure.alias)}")
        return (source.group_by(*keys) if keys else source).agg(*declarations)
    if op == "sort":
        if len({(key.descending, key.nulls) for key in args}) != 1:
            raise AdapterGap("DataFrame sort adapter requires a shared direction and null placement")
        return source.sort(*(key.column for key in args), descending=args[0].descending, nulls=args[0].nulls)
    if op == "limit":
        return source.limit(args[0], offset=args[1])
    if op == "distinct":
        return source.distinct()
    if op == "set":
        method = {"union_all": "union_all", "union_distinct": "union", "intersect": "intersect", "except": "except_"}[args[0]]
        return getattr(source, method)(inputs[1])
    if op == "join":
        keys, columns, kind = args
        condition = " AND ".join(f"f.{quoted(a)} = d.{quoted(b)}" for a, b in keys)
        return source.join(inputs[1], condition=condition, how=kind).select(*(
            f"{'f' if side == 'left' else 'd'}.{quoted(name)} AS {quoted(alias)}" for side, name, alias in columns))
    raise AdapterGap(f"DataFrame relation adapter is absent: {op}")


def execute(relation, sources, surface, *, native, binary, memory_bytes=1 << 30,
            parallelism=1, timeout=60):
    """Consume one full result. Outer supervisor owns deadlines/process cleanup.

    Each call currently measures a fresh public session. Persistent/prepared
    lifecycle modes must be declared separately; they are not inferred here.
    """
    import shardloom as sl

    if surface not in SURFACES:
        raise ValueError("unknown public surface")
    started = time.perf_counter()
    sql = relation_sql(relation, sources)
    files = [(str(path), source_format(path), None)
             for name in sorted(relation.sources()) for path in _paths(sources, name)]
    with ExitStack() as stack:
        if surface in {"dataframe", "cli_sql"}:
            client = sl.ShardLoomClient(binary=str(binary), transport="subprocess" if surface == "cli_sql" else "native",
                                        use_persistent_worker=False, memory_bytes=memory_bytes,
                                        max_parallelism=parallelism, timeout=timeout)
            stack.callback(client.close)
            plan = sl.sql(sql, client=client) if surface == "cli_sql" else relation_dataframe(relation, sources, sl, client)
            constructed = time.perf_counter()
            result = plan.collect(check=True)
            collected = time.perf_counter()
            legacy = {entry.key: entry.value for entry in result.envelope.fields}
            fields = dict(result.envelope.field_map)
            if (len(legacy) != len(result.envelope.fields) or result.envelope.fallback.attempted
                    or any(fields[key] != value for key, value in legacy.items())):
                raise ValueError("ambiguous or unsafe public result envelope")
            schema, rows = result.schema, result.to_rows()
            report = result.envelope.raw
        else:
            session = native.Session(native.ExecutionResources(memory_bytes, parallelism))
            stack.callback(session.close)
            if surface == "typed_relation":
                plan = session.plan(relation_native(relation, sources, native), files=files)
            elif surface == "python_sql":
                plan = session.sql(sql, files=files)
            else:
                plan = session.workflow(["sql", "--sql", sql, "--request", "collect", "--bounded", "true",
                                         "--memory-bytes", str(memory_bytes), "--max-parallelism", str(parallelism)])
            constructed = time.perf_counter()
            result = plan.collect(timeout=timeout)
            collected = time.perf_counter()
            fields = dict(result.report)
            schema = schema_fields(result.schema_json, "vortex.dtype.serde.v1")
            rows, export = result.row_export(temporal_objects=False)
            report = {"fields": fields, "row_export": export}
        from .evidence import no_fallback
        no_fallback(fields)
        consumed = time.perf_counter()
        clocks = {"setup_and_declaration_seconds": constructed - started,
                  "collect_call_seconds": collected - constructed,
                  "delivery_seconds": consumed - collected,
                  "complete_workflow_seconds": consumed - started}
        # Use the engine-owned report through the existing clock validator. A
        # missing native clock stays unavailable; collect-call time is not a proxy.
        from query_timing_report import query_phase_timings
        from .diagnostics import extract
        clocks.update(query_phase_timings(fields, consumed - started, allow_missing=True))
        return TypedResult(schema, rows), {"surface": surface, "sql_declaration": sql,
            "public_declaration": relation.record(), "fields": fields, "raw_report": report,
            "clocks": clocks, "execution_mode": "first_query_fresh_session",
            "diagnostics": extract(fields),
            "complete_consumption": True}
