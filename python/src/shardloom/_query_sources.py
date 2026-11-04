"""Deferred implementations for query sources.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _source_suffix_for_local_ref(uri):

    value = _api.os.fspath(uri).lower()
    if "://" in value or value.startswith(("s3:", "gs:", "abfs:", "abfss:")):
        return _api.Path(value).suffix.lower()
    compression_suffixes = (".gz", ".zst", ".zstd")
    text_suffixes = {".csv", ".tsv", ".json", ".jsonl", ".ndjson"}
    for compression in compression_suffixes:
        if value.endswith(compression):
            base_suffix = _api.Path(value[: -len(compression)]).suffix.lower()
            if base_suffix in text_suffixes:
                return base_suffix
            break
    return _api.Path(value).suffix.lower()


def read(uri, *, input_format, schema, snapshot_id, version, instant, client, engine_mode, **client_config):

    source_kind = _api._source_kind_from_path(uri) if input_format is None else input_format
    source_kind = source_kind.strip().lower().replace("_", "-")
    if source_kind in {"iceberg", "delta", "hudi"}:
        selectors = {"snapshot_id": snapshot_id, "version": version, "instant": instant}
        expected = {"iceberg": "snapshot_id", "delta": "version", "hudi": "instant"}[source_kind]
        if schema is not None:
            raise ValueError("table sources use their authoritative schema; schema= is unsupported")
        if any(value is not None and name != expected for name, value in selectors.items()):
            raise ValueError(f"{source_kind} sources accept only {expected}= as a snapshot selector")
        selected = selectors[expected]
        if expected == "snapshot_id" and selected is not None:
            if isinstance(selected, bool) or not isinstance(selected, int):
                raise TypeError("snapshot_id must be an integer")
            if selected < -(1 << 63) or selected >= (1 << 63):
                raise ValueError("snapshot_id must fit in a signed 64-bit integer")
        if expected == "version" and selected is not None:
            if isinstance(selected, bool) or not isinstance(selected, int):
                raise TypeError("version must be an integer")
            if selected < 0 or selected >= (1 << 64):
                raise ValueError("version must fit in an unsigned 64-bit integer")
        if expected == "instant" and selected is not None:
            if not isinstance(selected, str):
                raise TypeError("instant must be a string")
            if not selected or len(selected) > 32 or not selected.isascii() or not selected.isdigit():
                raise ValueError("instant must contain 1 to 32 ASCII digits")
        table_snapshot = () if selected is None else ((expected, selected),)
    else:
        if any(value is not None for value in (snapshot_id, version, instant)):
            raise ValueError("snapshot selectors require input_format='iceberg', 'delta', or 'hudi'")
        table_snapshot = ()
    if source_kind == "vortex":
        return _api.read_vortex(
            uri,
            schema=schema,
            client=client,
            engine_mode=engine_mode,
            **client_config,
        )
    return _api._read_source(
        source_kind,
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        table_snapshot=table_snapshot,
        **client_config,
    )


def from_batches(batches, *, schema, streaming, client, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)
    from ._batches import BatchInput
    from ._input_schema import normalize_schema, schema_hints, wire_schema
    from uuid import uuid4

    if not isinstance(streaming, bool):
        raise TypeError("streaming must be a bool")
    fields = normalize_schema(schema)
    if not fields:
        raise ValueError("batch schema requires nonempty distinct fields")
    declared = wire_schema(fields)
    source = BatchInput(batches, declared, types=fields)
    return _api.LazyFrame(
        source=_api.WorkflowSource("memory", "memory://batches/" + uuid4().hex, schema_hints(fields),
                              (("kind", "batches"), ("schema", declared), ("streaming", streaming)), source),
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
    )


def from_rows(rows, *, client, schema, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)

    return _api._memory_rows_source(
        rows,
        client=resolved_client,
        schema=schema,
        resources=resources,
        resource_limits=resource_limits,
    )


def dataframe_generated_with_column(name, expression, *, client, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)

    column = _api._require_non_empty("generated DataFrame column name", name)
    literal = _api._generated_literal_expression(expression)
    return _api._memory_rows_source(
        [{column: literal}],
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
    )


def range(start, end, *, step, column, client, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)

    normalized_start = _api._require_range_int("start", start)
    normalized_end = _api._require_range_int("end", end)
    normalized_step = _api._require_range_int("step", step)
    if normalized_step == 0:
        raise ValueError("range step must not be zero")
    normalized_column = _api._require_non_empty("range column", column)
    return _api._native_memory_frame(
        {"kind": "range", "start": normalized_start, "end": normalized_end,
         "step": normalized_step, "column": normalized_column, "inclusive": False},
        schema=((normalized_column, "int64"),),
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
    )


def sql_values(values_clause, *, client, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)

    statement = _api._require_non_empty("SQL VALUES clause", values_clause)
    return _api.LazyFrame.from_sql(
        statement=statement,
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
    )


def sql_literal_select(expression, *, client, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)

    statement = _api._require_non_empty("SQL literal SELECT expression", expression)
    return _api.LazyFrame.from_sql(
        statement=statement,
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
    )


def sql(statement, *, input, input_format, client, parameters, relations, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)

    input_uri, normalized_input_format = _api._normalize_sql_workflow_input(
        input,
        input_format,
    )
    return _api.LazyFrame.from_sql(
        statement=_api._require_non_empty("SQL statement", statement),
        client=resolved_client,
        input_uri=input_uri,
        input_format=normalized_input_format,
        parameters=parameters, relations=relations,
        resources=resources,
        resource_limits=resource_limits,
    )


def _normalize_sql_workflow_input(input_ref, input_format):

    if input_ref is None:
        if input_format is not None:
            raise ValueError("input_format requires input")
        return None, None
    input_uri = _api._require_non_empty("SQL input", _api.os.fspath(input_ref))
    if input_format is None:
        inferred = _api._source_format_for_sql_input_ref(input_uri)
        if inferred is None:
            raise ValueError(
                "input_format is required when SQL input format cannot be inferred"
            )
        return input_uri, inferred
    normalized = input_format.strip().lower().replace("_", "-")
    admitted_formats = set(_api.SUPPORTED_SOURCE_FORMATS) | {"jsonl", "ndjson"}
    if normalized not in admitted_formats:
        raise ValueError(
            f"input_format must be one of {tuple(sorted(admitted_formats))}; got {input_format!r}"
        )
    return input_uri, normalized


def _source_format_for_sql_input_ref(value):

    if _api._is_local_vortex_source_ref(value):
        return "vortex"
    # Keep path punctuation intact for local inputs: in particular, ``?`` can
    # be a glob wildcard and is not a URL query delimiter here. The local
    # suffix helper recognizes compressed text suffixes while refusing to
    # strip compression wrappers from columnar formats. Remote URI handling
    # remains outside local SQL input inference.
    local_format = _api._source_format_for_local_source_ref(value)
    if local_format is not None:
        if local_format == "json":
            suffix = _api._source_suffix_for_local_ref(value)
            if suffix in {".jsonl", ".ndjson"}:
                return suffix[1:]
        return local_format
    if "://" in value or value.lower().startswith(("s3:", "gs:", "abfs:", "abfss:")):
        return None
    return _api._source_format_for_local_source_ref(repr(value))


def calendar(start, end, *, column, include_parts, client, **client_config):

    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)
    start_date = _api._normalize_date("calendar start", start)
    end_date = _api._normalize_date("calendar end", end)
    if start_date >= end_date:
        raise ValueError("calendar start must be before end")
    column_name = _api._require_non_empty("calendar column", column)
    rows = []
    current = start_date
    while current < end_date:
        row: dict[str, object] = {column_name: current.isoformat()}
        if include_parts:
            row.update(
                {
                    "year": current.year,
                    "month": current.month,
                    "day": current.day,
                    "day_of_week": current.isoweekday(),
                }
            )
        rows.append(row)
        current += _api.timedelta(days=1)
    return _api.from_rows(
        rows,
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
    )


def from_pandas(dataframe, *, schema, client, engine_mode, check, **client_config):

    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)
    workflow = _api._materialized_boundary_workflow(
        "pandas",
        _api._python_object_boundary_ref("pandas", dataframe),
        client=resolved_client,
        engine_mode=engine_mode,
        resources=resources,
        resource_limits=resource_limits,
    )
    rows = _api._pandas_like_records(dataframe)
    if rows is None:
        return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)
    try:
        return _api._memory_rows_source(
            rows,
            client=resolved_client,
            schema=schema,
            engine_mode=engine_mode,
            resources=resources,
            resource_limits=resource_limits,
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)


def _native_input_frame(owner, *, client, engine_mode, resources, resource_limits, schema):
    from ._input_schema import normalize_schema, schema_hints
    from uuid import uuid4

    fields = _api.schema_fields(owner.schema_json, "vortex.dtype.serde.v1")
    if schema is not None and normalize_schema(schema) != fields:
        raise ValueError("declared schema differs from the native columnar input schema")
    return _api.LazyFrame(
        source=_api.WorkflowSource("memory", "memory://native/" + uuid4().hex,
                              schema_hints(fields), native_input=owner),
        client=client, engine_mode=_api._normalize_engine_mode(engine_mode),
        resources=resources, resource_limits=resource_limits,
    )


def from_vortex_ipc(array_message, *, dtype_message, client, engine_mode, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)
    if resolved_client.transport != "native":
        raise ValueError("from_vortex_ipc requires transport='native'; use read_vortex for a native file")
    if type(array_message) is not bytes or type(dtype_message) is not bytes:
        raise TypeError("from_vortex_ipc requires complete array_message and dtype_message bytes")
    mode = _api._normalize_engine_mode(engine_mode)
    owner = resolved_client._native_backend_for(resources).from_vortex_ipc(
        array_message, dtype_message, resources,
    )
    return _api._native_input_frame(
        owner, client=resolved_client, engine_mode=mode,
        resources=resources, resource_limits=resource_limits,
    )


def from_result(result, *, client, engine_mode, **client_config):
    from ._batches import ResultBatch
    from ._native_backend import NativeResultBatch
    from .session import SessionSqlResult

    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)
    mode = _api._normalize_engine_mode(engine_mode)
    if type(result) is SessionSqlResult:
        result = result.report
    if resolved_client.transport == "native":
        backend = resolved_client._native_backend_for(resources)
        if type(result) is backend.module.Result or type(result) is backend.module.ColumnarBatch:
            owner = backend.from_arrow(result, resources)
            return _api._native_input_frame(owner, client=resolved_client, engine_mode=mode,
                                       resources=resources, resource_limits=resource_limits)
        if (type(result) is _api.Result or type(result) is NativeResultBatch) and result.native_result is not None:
            owner = backend.from_arrow(result, resources)
            return _api._native_input_frame(owner, client=resolved_client, engine_mode=mode,
                                       resources=resources, resource_limits=resource_limits)
    if not any(type(result) is candidate for candidate in (_api.Result, ResultBatch, NativeResultBatch)):
        raise TypeError("from_result requires a completed ShardLoom Result or result batch")
    if isinstance(result, _api.Result) and (result.is_error or result.operation != "collect"):
        raise ValueError("from_result requires successful collected data, not a write or error receipt")
    schema = dict(result.result_schema)
    return _api._memory_rows_source(result.python_objects, schema=schema, client=resolved_client,
                               engine_mode=mode, resources=resources, resource_limits=resource_limits)


def from_arrow_table(table, *, schema, client, engine_mode, check, **client_config):

    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)
    if getattr(resolved_client, "transport", "subprocess") == "native":
        owner = resolved_client._native_backend_for(resources).from_arrow(table, resources)
        return _api._native_input_frame(
            owner, schema=schema,
            client=resolved_client, engine_mode=_api._normalize_engine_mode(engine_mode),
            resources=resources, resource_limits=resource_limits,
        )
    workflow = _api._materialized_boundary_workflow(
        "arrow_table",
        _api._python_object_boundary_ref("arrow_table", table),
        client=resolved_client,
        engine_mode=engine_mode,
        resources=resources,
        resource_limits=resource_limits,
    )
    rows = _api._arrow_table_like_records(table)
    if rows is None:
        return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)
    try:
        return _api._memory_rows_source(
            rows,
            client=resolved_client,
            schema=schema,
            engine_mode=engine_mode,
            resources=resources,
            resource_limits=resource_limits,
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)


def from_arrow_ipc(source, *, schema, client, engine_mode, check, **client_config):

    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=True)
    target = (
        str(source)
        if isinstance(source, (str, _api.os.PathLike))
        else _api._python_object_boundary_ref("arrow_ipc", source)
    )
    workflow = _api._materialized_boundary_workflow(
        "arrow_ipc",
        target,
        client=resolved_client,
        engine_mode=engine_mode,
        resources=resources,
        resource_limits=resource_limits,
    )
    pyarrow = _api._optional_module("pyarrow")
    if pyarrow is None:
        return workflow._unsupported_operation(
            "from-arrow-ipc",
            "missing optional dependency: pyarrow",
            check=check,
        )
    try:
        rows = _api._arrow_table_like_records(_api._read_arrow_ipc_table(source, pyarrow))
    except Exception:
        rows = None
    if rows is None:
        return workflow._unsupported_operation("from-arrow-ipc", workflow.uri, check=check)
    try:
        return _api._memory_rows_source(
            rows,
            client=resolved_client,
            schema=schema,
            engine_mode=engine_mode,
            resources=resources,
            resource_limits=resource_limits,
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-arrow-ipc", workflow.uri, check=check)


def _memory_rows_source(rows, *, client, schema, engine_mode, resources, resource_limits):
    limits = _api.merge_resource_limits(resource_limits, getattr(client, "resource_limits", None))
    resources = _api.resolve_resources(
        resources=resources, inherited=getattr(client, "resources", None), limits=limits,
    )
    limits = _api.merge_resource_limits(limits, resources.limits)
    if getattr(client, "transport", "subprocess") == "native":
        owner = client._native_backend_for(resources).from_rows(rows, schema, resources)
        from ._input_schema import schema_hints
        from uuid import uuid4
        fields = _api.schema_fields(owner.schema_json, "vortex.dtype.serde.v1")
        return _api.LazyFrame(
            source=_api.WorkflowSource("memory", "memory://native/" + uuid4().hex,
                                  schema_hints(fields), native_input=owner),
            client=client, engine_mode=_api._normalize_engine_mode(engine_mode),
            resources=resources, resource_limits=limits,
        )
    from ._input_schema import encode_cell, normalize_schema, schema_hints, wire_schema
    from ._result_schema import ResultType
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, _api.Sequence):
        raise TypeError("rows must be a sequence of mappings")
    if len(rows) > 65_536:
        raise ValueError("native row input exceeds 65,536 rows")
    declared = normalize_schema(schema)
    if declared:
        columns = tuple(name for name, _ in declared)
    elif rows and isinstance(rows[0], _api.Mapping):
        columns = tuple(rows[0])
    elif rows:
        raise TypeError("rows must contain mappings")
    else:
        raise ValueError("empty row input requires an explicit schema")
    if not columns:
        raise ValueError("native row input requires at least one column")
    if any(not isinstance(name, str) for name in columns):
        raise TypeError("row column names must be strings")
    if any(not name or len(name.encode("utf-8")) > 256 for name in columns):
        raise ValueError("row column names must contain 1..=256 UTF8 bytes")
    keys = set(columns)
    for index, row in enumerate(rows):
        if not isinstance(row, _api.Mapping):
            raise TypeError(f"row {index} is not a mapping")
        if set(row) != keys:
            raise ValueError("all rows must match the declared column names")
    if declared:
        fields = declared
    else:
        fields = tuple((name, ResultType(_api._infer_memory_column_type(row[name] for row in rows), True))
                       for name in columns)
    declared_fields = wire_schema(fields)
    encoded_rows = tuple(tuple(encode_cell(dtype, row[name]) for name, dtype in fields) for row in rows)
    return _api._native_memory_frame(
        {"kind": "rows", "schema": declared_fields, "rows": encoded_rows},
        schema=schema_hints(fields), client=client, engine_mode=engine_mode, resources=resources,
        resource_limits=limits,
    )


def _native_memory_frame(declaration, *, schema, client, engine_mode, resources, resource_limits):
    payload = _api.json.dumps(declaration, sort_keys=True, ensure_ascii=False).encode("utf-8")
    if len(payload) > 8 * 1024 * 1024:
        raise ValueError("native memory declaration exceeds 8 MiB")
    uri = "memory://input/" + _api.hashlib.sha256(payload).hexdigest()
    return _api.LazyFrame(
        source=_api.WorkflowSource("memory", uri, schema, tuple(declaration.items())),
        client=client, engine_mode=_api._normalize_engine_mode(engine_mode), resources=resources,
        resource_limits=resource_limits,
    )


def _memory_value_type(value):
    if value is None or isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError("native row integers must fit int64")
        return "int64"
    if isinstance(value, float):
        if not _api.math.isfinite(value):
            raise ValueError("float native row values must be finite")
        return "float64"
    if isinstance(value, str):
        return "utf8"
    raise TypeError(
        "native row values must be None, bool, int, float, or str"
    )


def _memory_value(value_type, value):
    if value_type == "bool":
        if not isinstance(value, bool):
            raise TypeError("native bool columns must contain only bool values")
        return "true" if value else "false"
    if value_type == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("native int64 columns must contain only int values")
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError("native row integers must fit int64")
        return str(value)
    if value_type == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("native float64 columns must contain only numeric values")
        if isinstance(value, int) and _api.builtins.abs(value) > (1 << 53):
            raise ValueError("integer-to-float row conversion exceeds the exact integer range")
        numeric = float(value)
        if not _api.math.isfinite(numeric):
            raise ValueError("float native row values must be finite")
        return str(numeric)
    if value_type == "utf8":
        if not isinstance(value, str):
            raise TypeError("native utf8 columns must contain only str values")
        return value
    raise ValueError(f"unsupported native input value type {value_type!r}")


def _dataframe_source_free_projection_row(expressions):
    if not expressions:
        raise ValueError("DataFrame source-free projection must include at least one expression")
    if len(expressions) == 1 and isinstance(expressions[0], _api.Mapping):
        row: dict[str, object] = {}
        for raw_name, raw_value in expressions[0].items():
            name = _api._normalize_output_column_name(raw_name)
            if name in row:
                raise ValueError("DataFrame source-free projection aliases must be unique")
            _api._memory_value_type(raw_value)
            row[name] = raw_value
        if not row:
            raise ValueError("DataFrame source-free projection mapping must not be empty")
        return row

    row = {}
    for expression in expressions:
        name, value = _api._dataframe_source_free_projection_item(expression)
        if name in row:
            raise ValueError("DataFrame source-free projection aliases must be unique")
        row[name] = value
    return row


def _dataframe_source_free_projection_item(expression):
    if (
        isinstance(expression, _api.Sequence)
        and not isinstance(expression, (str, bytes, bytearray))
        and len(expression) == 2
    ):
        name = _api._normalize_output_column_name(expression[0])
        value = expression[1]
        if isinstance(value, str) and value.strip().startswith("lit("):
            value = _api._generated_literal_expression(value)
        else:
            _api._memory_value_type(value)
        return name, value
    if isinstance(expression, str):
        return _api._parse_dataframe_literal_alias_expression(expression)
    raise TypeError(
        "DataFrame source-free projection expressions must be mappings, "
        "(alias, literal) pairs, or lit(...).alias(...) strings"
    )


def _parse_dataframe_literal_alias_expression(expression):
    text = expression.strip()
    if not text:
        raise ValueError("DataFrame source-free projection expression must not be empty")
    try:
        parsed = _api.ast.parse(text, mode="eval").body
    except SyntaxError as exc:
        raise ValueError(
            "DataFrame source-free projection strings must use lit(...).alias('name')"
        ) from exc
    if not (
        isinstance(parsed, _api.ast.Call)
        and isinstance(parsed.func, _api.ast.Attribute)
        and parsed.func.attr == "alias"
        and isinstance(parsed.func.value, _api.ast.Call)
        and isinstance(parsed.func.value.func, _api.ast.Name)
        and parsed.func.value.func.id == "lit"
        and len(parsed.func.value.args) == 1
        and not parsed.func.value.keywords
        and len(parsed.args) == 1
        and not parsed.keywords
    ):
        raise ValueError(
            "DataFrame source-free projection strings must use lit(...).alias('name')"
        )
    alias_node = parsed.args[0]
    if not isinstance(alias_node, _api.ast.Constant) or not isinstance(alias_node.value, str):
        raise ValueError("DataFrame source-free projection alias must be a string literal")
    try:
        value = _api.ast.literal_eval(parsed.func.value.args[0])
    except (SyntaxError, ValueError) as exc:
        raise ValueError(
            "DataFrame source-free projection lit(...) must contain a bool, int, float, or string literal"
        ) from exc
    _api._memory_value_type(value)
    return _api._normalize_output_column_name(alias_node.value), value


def _generated_literal_expression(expression):
    if isinstance(expression, str):
        text = expression.strip()
        if not text:
            raise ValueError("literal with_column expression must not be empty")
        if not (text.startswith("lit(") and text.endswith(")")):
            raise ValueError(
                "literal with_column currently supports only lit(...) expressions "
                "or direct Python bool/int/float literals"
            )
        inner = text[4:-1].strip()
        if not inner:
            raise ValueError("lit(...) expression must include a value")
        lowered = inner.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        if lowered in {"null", "none"}:
            return None
        try:
            parsed = _api.ast.literal_eval(inner)
        except (SyntaxError, ValueError) as exc:
            raise ValueError(
                "lit(...) expression must contain a bool, int, float, or quoted string"
            ) from exc
        _api._memory_value_type(parsed)
        return parsed
    _api._memory_value_type(expression)
    return expression


def _normalize_date(name, value):
    if isinstance(value, _api.datetime):
        return value.date()
    if isinstance(value, _api.date):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a date or ISO date string")
    text = value.strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    try:
        return _api.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date string like YYYY-MM-DD") from exc


def _normalize_limit_range(count, offset):

    normalized_count = _api._normalize_non_negative_int("limit count", count)
    normalized_offset = _api._normalize_non_negative_int("limit offset", offset)
    if (
        normalized_count > _api._NATIVE_USIZE_MAX
        or normalized_offset > _api._NATIVE_USIZE_MAX
        or normalized_offset > _api._NATIVE_USIZE_MAX - normalized_count
    ):
        raise ValueError("limit offset + count must fit in a 64-bit native usize")
    return normalized_count, normalized_offset


def _limit_operation_parts(values):

    if len(values) not in {1, 2}:
        return None
    try:
        count, offset = _api._normalize_limit_range(
            int(values[0]), int(values[1]) if len(values) == 2 else 0,
        )
    except (TypeError, ValueError):
        return None
    return str(count), str(offset)


def _first_string_field(envelope, keys):
    for key in keys:
        value = envelope.field(key)
        if value is None:
            continue
        normalized = value.strip()
        if not normalized or normalized.lower() in {"none", "unknown"}:
            continue
        return normalized
    return None


def _first_int_field(envelope, keys):
    for key in keys:
        value = envelope.field(key)
        if value is None:
            continue
        normalized = value.strip().lower()
        if not normalized or normalized in {"none", "unknown"}:
            continue
        try:
            return int(normalized)
        except ValueError:
            continue
    return None


def _read_source(source_format, uri, *, schema, client, engine_mode, table_snapshot, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)
    normalized = source_format.strip().lower().replace("_", "-")
    if normalized == "ndjson":
        normalized = "jsonl"
    if normalized not in _api.SUPPORTED_SOURCE_FORMATS:
        raise ValueError(
            f"source_format must be one of {_api.SUPPORTED_SOURCE_FORMATS}; got {source_format!r}"
        )
    return _api.LazyFrame(
        source=_api.WorkflowSource(
            source_format=normalized,
            uri=str(uri),
            schema=_api._normalize_schema(schema),
            table_snapshot=table_snapshot,
        ),
        client=resolved_client,
        engine_mode=_api._normalize_engine_mode(engine_mode),
        resources=resources,
        resource_limits=resource_limits,
    )


def _materialized_boundary_workflow(source_format, uri, *, client, engine_mode, **client_config):
    resolved_client, resources, resource_limits = _api._source_configuration(client, client_config, required=False)
    return _api.LazyFrame(
        source=_api.WorkflowSource(
            source_format=source_format,
            uri=uri,
        ),
        client=resolved_client,
        engine_mode=_api._normalize_engine_mode(engine_mode),
        resources=resources,
        resource_limits=resource_limits,
    )


def _source_configuration(client, client_config, *, required):

    config = dict(client_config)
    allocation = {name: config.pop(name, None) for name in (
        "resources", "memory_gb", "memory_bytes", "max_parallelism",
    )}
    limits = config.pop("resource_limits", None)
    resolved_client = _api._client_from_config(client, config)
    limits = _api.merge_resource_limits(limits, getattr(resolved_client, "resource_limits", None))
    resolver = _api.resolve_resources if required else _api.optional_resources
    resources = resolver(
        **allocation, limits=limits, inherited=getattr(resolved_client, "resources", None),
    )
    limits = _api.merge_resource_limits(limits, resources.limits if resources is not None else None)
    return resolved_client, resources, limits
