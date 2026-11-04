"""Deferred implementations for client arguments.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def _iter_fanout_outputs(fanout_outputs):
    if fanout_outputs is None:
        return ()
    if isinstance(fanout_outputs, _api.Mapping):
        items = fanout_outputs.items()
    else:
        items = fanout_outputs
    normalized: list[tuple[str, str]] = []
    for item in items:
        fanout_format, fanout_path = item
        normalized.append((str(fanout_format), str(fanout_path)))
    return tuple(normalized)


def _schema_command_arg(schema):
    if schema is None:
        return None
    if isinstance(schema, str):
        value = schema.strip()
        return value or None
    items = schema.items() if isinstance(schema, _api.Mapping) else schema
    parts: list[str] = []
    for name, dtype in items:
        name_text = str(name).strip()
        dtype_text = str(dtype).strip()
        if not name_text or not dtype_text:
            raise ValueError("schema entries must have non-empty column names and dtype values")
        parts.append(f"{name_text}:{dtype_text}")
    return ",".join(parts) if parts else None


def _source_bindings_command_arg(source_bindings):
    if source_bindings is None:
        return None
    if not isinstance(source_bindings, _api.Mapping):
        raise TypeError("source_bindings must be a mapping")
    if not source_bindings:
        return None
    if len(source_bindings) > 128:
        raise ValueError("source_bindings must contain at most 128 entries")

    normalized: dict[str, dict[str, object]] = {}
    for uri, binding in source_bindings.items():
        if not isinstance(uri, str):
            raise TypeError("source binding URI must be a string")
        if not isinstance(binding, _api.Mapping):
            raise TypeError("each source binding must be a mapping")
        keys = set(binding)
        if keys - {"input_format", "source_schema", "memory_input", "table_snapshot"} or "input_format" not in keys:
            raise ValueError(
                "each source binding requires input_format and optional source_schema, memory_input or table_snapshot"
            )
        input_format = binding["input_format"]
        if not isinstance(input_format, str) or not input_format.strip():
            raise ValueError("source binding input_format must be a non-empty string")
        entry: dict[str, object] = {"input_format": input_format}
        if "table_snapshot" in binding:
            selection = binding["table_snapshot"]
            if input_format not in {"iceberg", "delta", "hudi"} or not isinstance(selection, _api.Mapping):
                raise ValueError("table_snapshot requires an Iceberg, Delta or Hudi selection mapping")
            if set(selection) - {"snapshot_id", "version", "instant"}:
                raise ValueError("unknown table snapshot selector")
            entry["table_snapshot"] = dict(selection)
        schema_arg = _api._schema_command_arg(binding.get("source_schema"))
        if schema_arg is not None:
            entry["source_schema"] = schema_arg
        memory_input = binding.get("memory_input")
        if input_format == "memory":
            if not uri.startswith("memory://") or schema_arg is not None or not isinstance(memory_input, _api.Mapping):
                raise ValueError("memory sources require a memory URI and a typed memory_input declaration")
            entry["memory_input"] = dict(memory_input)
        elif memory_input is not None:
            raise ValueError("memory_input cannot be attached to a file source")
        normalized[uri] = entry

    value = _api.json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
    limit = 8 * 1024 * 1024 if any("memory_input" in entry for entry in normalized.values()) else 256 * 1024
    if len(value.encode("utf-8")) > limit:
        raise ValueError(f"serialized source_bindings exceed the {limit}-byte input bound")
    return value


def _signals_arg(signals):
    if isinstance(signals, str):
        value = signals
    else:
        values = [str(signal).strip() for signal in signals]
        if not values:
            raise ValueError("signals must not be empty")
        value = ",".join(values)
    if value.strip() == "":
        raise ValueError("signals must not be empty")
    if any(part.strip() == "" for part in value.split(",")):
        raise ValueError("signals must not contain empty tokens")
    return value


def _envelope_reports_blocked_or_incomplete(envelope):
    if envelope.is_error or envelope.has_error_diagnostics:
        return True
    for key in _api.BLOCKER_FIELDS:
        if envelope.field(key) is None:
            continue
        value = envelope.field_bool(key)
        if key.endswith("_allowed"):
            if value is False:
                return True
        elif value is True:
            return True
    status = envelope.field("claim_evidence_status") or envelope.field("claim_gate_status")
    return status in {"needs_evidence", "evidence_missing", "blocked"}


def _columns_arg(columns):
    if isinstance(columns, str):
        value = columns
    else:
        values = [str(column) for column in columns]
        if not values:
            raise ValueError("columns must not be empty")
        value = ",".join(values)
    if value.strip() == "":
        raise ValueError("columns must not be empty")
    return value


def _append_resource_execution_args(args, *, option, enabled, memory_gb, max_parallelism, memory_bytes, resources):
    if enabled:
        allocation = _api.resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
            resources=resources,
        )
        args.append(option)
        args.extend(_api.resource_command_args(allocation))
        return
    if any(value is not None for value in (memory_gb, memory_bytes, max_parallelism, resources)):
        raise ValueError(
            "memory_gb and max_parallelism require explicit local execution"
        )


def _append_public_vortex_payload_args(args, *, native_vortex_operation_family, native_vortex_right_input, vortex_primitive, vortex_predicate, vortex_columns, vortex_source_order_limit, vortex_sample_seed, vortex_sample_fraction, vortex_sample_replacement, vortex_sample_weight_column, vortex_duplicate_keep, vortex_deduplicate_key_columns, vortex_expression_projection, vortex_melt_projection, vortex_explode_projection, vortex_pivot_projection, vortex_rolling_window, vortex_sort_rows, vortex_aggregate, resources, spill):
    if spill is not None:
        args.extend(["--spill", _api.json.dumps(dict(spill)) if isinstance(spill, _api.Mapping) else spill])
    if native_vortex_operation_family is not None:
        args.extend(["--native-vortex-operation-family", native_vortex_operation_family])
    if native_vortex_right_input is not None:
        args.extend(["--native-vortex-right-input", str(native_vortex_right_input)])
    if vortex_primitive is not None:
        args.extend(["--vortex-primitive", vortex_primitive])
    if vortex_predicate is not None:
        args.extend(["--vortex-predicate", vortex_predicate])
    if vortex_columns is not None:
        args.extend(["--vortex-columns", _api._columns_arg(vortex_columns)])
    if vortex_source_order_limit is not None:
        args.extend(
            [
                "--vortex-source-order-limit",
                str(_api._positive_int("vortex_source_order_limit", vortex_source_order_limit)),
            ]
        )
    if vortex_sample_seed is not None:
        args.extend(
            [
                "--vortex-sample-seed",
                str(_api._non_negative_int("vortex_sample_seed", vortex_sample_seed)),
            ]
        )
    if vortex_sample_fraction is not None:
        args.extend(
            [
                "--vortex-sample-fraction",
                _api._sample_fraction_arg(vortex_sample_fraction),
            ]
        )
    if vortex_sample_replacement:
        args.append("--vortex-sample-replacement")
    if vortex_sample_weight_column is not None:
        column = str(vortex_sample_weight_column).strip()
        if not column:
            raise ValueError("vortex_sample_weight_column must not be empty")
        args.extend(["--vortex-sample-weight-column", column])
    if vortex_duplicate_keep is not None:
        args.extend(["--vortex-duplicate-keep", _api._duplicate_keep_arg(vortex_duplicate_keep)])
    if vortex_deduplicate_key_columns is not None:
        args.extend(
            [
                "--vortex-deduplicate-key-columns",
                _api._columns_arg(vortex_deduplicate_key_columns),
            ]
        )
    if vortex_expression_projection is not None:
        payload = str(vortex_expression_projection).strip()
        if not payload:
            raise ValueError("vortex_expression_projection must not be empty")
        args.extend(["--vortex-expression-projection", payload])
    if vortex_melt_projection is not None:
        payload = str(vortex_melt_projection).strip()
        if not payload:
            raise ValueError("vortex_melt_projection must not be empty")
        args.extend(["--vortex-melt-projection", payload])
    if vortex_explode_projection is not None:
        payload = str(vortex_explode_projection).strip()
        if not payload:
            raise ValueError("vortex_explode_projection must not be empty")
        args.extend(["--vortex-explode-projection", payload])
    if vortex_pivot_projection is not None:
        payload = str(vortex_pivot_projection).strip()
        if not payload:
            raise ValueError("vortex_pivot_projection must not be empty")
        args.extend(["--vortex-pivot-projection", payload])
    if vortex_rolling_window is not None:
        payload = str(vortex_rolling_window).strip()
        if not payload:
            raise ValueError("vortex_rolling_window must not be empty")
        args.extend(["--vortex-rolling-window", payload])
    if vortex_sort_rows is not None:
        payload = str(vortex_sort_rows).strip()
        if not payload:
            raise ValueError("vortex_sort_rows must not be empty")
        args.extend(["--vortex-sort-rows", payload])
    if vortex_aggregate is not None:
        payload = str(vortex_aggregate).strip()
        if not payload:
            raise ValueError("vortex_aggregate must not be empty")
        args.extend(["--vortex-aggregate", payload])
    args.extend(_api.resource_command_args(resources))
