# SPDX-License-Identifier: Apache-2.0
"""Local ADBC 1.1 access to an explicitly selected ShardLoom context.

Install ``shardloom[adbc]``. Queries use the same native plans and resource
grants as the context. Bind one row of named scalar parameters. Transactions,
SQL writes, remote partitions and parameter streams are explicitly unsupported.
Closing a driver handle cancels its streams; it does not close the borrowed
context. Arrow batches are an explicit result-delivery boundary.
"""

from __future__ import annotations

from contextlib import contextmanager
import re
import secrets
import threading
import weakref

__all__ = ("database", "driver_options")


def _context(context):
    from .context import ShardLoomContext
    if not isinstance(context, ShardLoomContext):
        raise TypeError("ADBC requires an explicitly selected ShardLoom context")
    context.client._ensure_open()
    if context.client.transport != "native":
        raise ValueError("local ADBC requires the in-process native transport")
    return context.client._native_backend_for().module


@contextmanager
def driver_options(context):
    """Yield one-use options for an independent ADBC manager's database open.

    Consume the options inside the with block. The capability expires when the
    block exits, including failed initialization. No default context is created.
    """
    module = _context(context)
    token = secrets.token_hex(32)
    pointer = module._adbc_register(token, _Database(context))
    try:
        yield {"driver": module.__file__, "entrypoint": "AdbcShardloomInit",
               "init_func": pointer, "shardloom.context": token}
    finally:
        module._adbc_discard(token)


def database(context):
    """Open the independent ADBC manager's database handle on this context."""
    from adbc_driver_manager import AdbcDatabase
    with driver_options(context) as options:
        return AdbcDatabase(**options)


def _arrow_schema(frame):
    import pyarrow as pa
    from ._result_schema import _arrow_type
    return pa.schema([pa.field(name, _arrow_type(dtype, pa), nullable=dtype.nullable)
                      for name, dtype in frame.schema()])


def _parameter_values(batch):
    import pyarrow as pa
    from .types import scalar
    from ._temporal import canonical_temporal_name

    def temporal_dtype(dtype):
        if pa.types.is_date32(dtype):
            return "date32"
        if pa.types.is_date64(dtype):
            return "date64"
        if pa.types.is_time32(dtype) or pa.types.is_time64(dtype):
            return canonical_temporal_name(f"time({dtype.unit})")
        if pa.types.is_timestamp(dtype):
            if dtype.unit == "us" and dtype.tz is None:
                return "timestamp_micros"
            zone = f",{dtype.tz}" if dtype.tz is not None else ""
            return canonical_temporal_name(f"timestamp({dtype.unit}{zone})")
        if pa.types.is_duration(dtype):
            return canonical_temporal_name(f"duration({dtype.unit})")
        return None

    values = {}
    supported = {pa.bool_(): "bool", pa.int64(): "int64", pa.uint64(): "uint64",
                 pa.float64(): "float64", pa.string(): "utf8", pa.binary(): "binary"}
    for index, field in enumerate(batch.schema):
        dtype = field.type
        if pa.types.is_null(dtype):
            values[field.name] = None
            continue
        temporal = temporal_dtype(dtype)
        if temporal is not None:
            native = temporal
        elif pa.types.is_decimal128(dtype):
            native = f"decimal128({dtype.precision},{dtype.scale})"
        else:
            native = supported.get(dtype)
        if native is None:
            raise ValueError(
                f"ADBC parameter type {dtype} has no exact native scalar contract; "
                "cast it explicitly before binding"
            )
        value = batch.column(index)[0]
        if temporal is not None:
            # PyArrow's Python conversion can truncate sub-microsecond timestamps
            # and cannot represent the full signed 64-bit temporal domain.
            value = value.value if value.is_valid else None
        else:
            value = value.as_py()
        values[field.name] = scalar(value, {"type": native, "nullable": field.nullable})
    return values


def _pattern(value, pattern):
    if pattern is None:
        return True
    pieces, escaped = [], False
    for character in pattern:
        if escaped:
            pieces.append(re.escape(character))
            escaped = False
        elif character == "\\":
            escaped = True
        else:
            pieces.append(".*" if character == "%" else "." if character == "_" else re.escape(character))
    if escaped:
        raise ValueError("ADBC metadata pattern ends with an escape")
    return re.fullmatch("".join(pieces), value, flags=re.DOTALL) is not None


class _Database:
    def __init__(self, context):
        self.context = context

    def new_connection(self):
        _context(self.context)
        return _Connection(self.context)


class _Connection:
    def __init__(self, context):
        self.context = context
        self.closed = False
        self.streams = weakref.WeakSet()
        self.lock = threading.RLock()

    def ensure_open(self):
        if self.closed:
            raise ValueError("ADBC connection is closed")
        self.context.client._ensure_open()

    def cancel(self):
        with self.lock:
            streams = tuple(self.streams)
        for stream in streams:
            stream.cancel()

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.closed = True
        self.cancel()

    def prepare(self, statement):
        self.ensure_open()
        return _Prepared(self, statement)

    def table_schema(self, catalog, schema, name):
        self.ensure_open()
        if catalog not in (None, "") or schema not in (None, ""):
            raise ValueError("local ADBC has only the unnamed context catalog/schema")
        return _arrow_schema(self.context.table(name))

    def get_info(self, schema, codes):
        import pyarrow as pa
        from ._version import __version__
        self.ensure_open()
        values = {0: "ShardLoom", 1: __version__, 3: True, 4: False,
                  100: "ShardLoom local ADBC", 101: __version__, 102: "59.3.1", 103: 1_001_000}
        selected = sorted(values if codes is None else set(codes).intersection(values))
        union_type = schema.field("info_value").type
        children = [[] for _ in union_type]
        type_ids, offsets = [], []
        for code in selected:
            value = values[code]
            index = 1 if isinstance(value, bool) else 2 if isinstance(value, int) else 0
            type_ids.append(index)
            offsets.append(len(children[index]))
            children[index].append(value)
        union = pa.UnionArray.from_dense(pa.array(type_ids, type=pa.int8()), pa.array(offsets, type=pa.int32()),
            [pa.array(values, type=field.type) for values, field in zip(children, union_type)],
            field_names=[field.name for field in union_type], type_codes=list(union_type.type_codes))
        return pa.RecordBatch.from_arrays([pa.array(selected, type=pa.uint32()), union], schema=schema)

    def get_objects(self, schema, depth, catalog, db_schema, table_name, table_type, column_name):
        import pyarrow as pa
        self.ensure_open()
        if not _pattern("", catalog):
            return pa.RecordBatch.from_pylist([], schema=schema)
        schemas = None
        if depth != 1:
            schemas = []
            if _pattern("", db_schema):
                tables = None
                if depth != 2:
                    tables = []
                    if table_type is None or "VIEW" in table_type:
                        for name in self.context.list_tables():
                            if not _pattern(name, table_name):
                                continue
                            columns, constraints = None, None
                            if depth == 0:
                                columns = [{"column_name": field.name, "ordinal_position": position,
                                            "xdbc_type_name": str(field.type),
                                            "xdbc_nullable": 1 if field.nullable else 0,
                                            "xdbc_is_nullable": "YES" if field.nullable else "NO"}
                                           for position, field in enumerate(self.table_schema(None, None, name), 1)
                                           if _pattern(field.name, column_name)]
                                constraints = []
                            tables.append({"table_name": name, "table_type": "VIEW", "table_columns": columns,
                                           "table_constraints": constraints})
                schemas.append({"db_schema_name": "", "db_schema_tables": tables})
        return pa.RecordBatch.from_pylist([{"catalog_name": "", "catalog_db_schemas": schemas}], schema=schema)


class _Prepared:
    def __init__(self, connection, statement):
        self.connection = connection
        self.statement = statement
        context = connection.context
        self.relations = context.catalog.bindings(None)
        self.names = tuple(_context(context).Plan.parameter_names(statement))
        # Validate syntax and snapshot relation ownership without executing or
        # inspecting payload. Actual parameter types bind separately per action.
        context.sql(statement, parameters=dict.fromkeys(self.names), relations=self.relations)

    def validate_parameters(self, batch):
        self.connection.ensure_open()
        if batch.num_rows != 1 or len(batch.schema.names) != len(set(batch.schema.names)):
            raise ValueError("ADBC parameters require one row with unique named fields")
        if set(batch.schema.names) != set(self.names):
            raise ValueError(f"ADBC parameter names must be exactly {self.names!r}")
        _parameter_values(batch)

    def parameter_schema(self):
        import pyarrow as pa
        return pa.schema([pa.field(name, pa.null()) for name in self.names])

    def frame(self, batch):
        self.connection.ensure_open()
        if batch is None:
            if self.names:
                raise ValueError(f"missing named scalar parameters {self.names!r}")
            values = {}
        else:
            self.validate_parameters(batch)
            # One bounded parameter row is an explicit scalar binding boundary.
            # Query result rows are never converted here.
            values = _parameter_values(batch)
        return self.connection.context.sql(self.statement, parameters=values, relations=self.relations)

    def schema(self, batch):
        return _arrow_schema(self.frame(batch))

    def execute(self, batch):
        frame = self.frame(batch)
        schema = _arrow_schema(frame)
        stream = frame.iter_batches()
        with self.connection.lock:
            self.connection.ensure_open()
            self.connection.streams.add(stream)
        return schema, stream
