"""Deferred implementations for query bindings.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _source_format_for_local_source_ref(value):
    if not _api._is_local_source_sql_ref(value):
        return None
    suffix = _api._source_suffix_for_local_ref(value)
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    if suffix in {".json", ".jsonl", ".ndjson"}:
        return "json"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".arrow", ".ipc", ".feather"}:
        return "arrow-ipc"
    if suffix == ".avro":
        return "avro"
    if suffix == ".orc":
        return "orc"
    return None


def _public_workflow_input_format(source):

    if source.relation is not None:
        return None
    if source.source_format == "json":
        suffix = _api._source_suffix_for_local_ref(source.uri)
        if suffix == ".jsonl":
            return "jsonl"
        if suffix == ".ndjson":
            return "ndjson"
    return source.source_format


def _workflow_source_bindings(sources):
    bindings: dict[str, dict[str, object]] = {}
    for source in sources:
        if source._bound_frame is not None:
            continue
        binding: dict[str, object] = {"input_format": _api._public_workflow_input_format(source)}
        if source.source_format in {"iceberg", "delta", "hudi"}:
            binding["table_snapshot"] = dict(source.table_snapshot)
        if source.native_input is not None:
            binding["native_input"] = source.native_input
        elif source.memory_input:
            binding["memory_input"] = dict(source.memory_input)
        elif schema := _api._prepare_vortex_schema_hints(source):
            binding["source_schema"] = schema
        previous = bindings.setdefault(source.uri, binding)
        if previous != binding:
            raise ValueError(f"conflicting format or schema declarations for source {source.uri!r}")
    return bindings


def _single_quoted_sql_strings(statement):
    values: list[str] = []
    in_quote = False
    current: list[str] = []
    index = 0
    while index < len(statement):
        char = statement[index]
        if char != "'":
            if in_quote:
                current.append(char)
            index += 1
            continue
        if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
            current.append("'")
            index += 2
            continue
        if in_quote:
            values.append("".join(current))
            current = []
            in_quote = False
        else:
            current = []
            in_quote = True
        index += 1
    return tuple(values)


def _terminal_resource_kwargs(memory_gb, max_parallelism, spill, *, memory_bytes, resources, inherited, limits, required):

    resolver = _api.resolve_resources if required else _api.optional_resources
    allocation = resolver(memory_gb=memory_gb, memory_bytes=memory_bytes,
                          max_parallelism=max_parallelism, resources=resources,
                          inherited=inherited, limits=limits)
    kwargs: dict[str, object] = {
        "resources": allocation,
    }
    if spill is not None:
        kwargs["spill"] = spill
    return kwargs


def _frame_action(frame, operation, execute, *, reuse):
    if not isinstance(reuse, bool):
        raise TypeError("reuse must be a bool")
    session = frame.source._session
    if session is not None:
        session._ensure_open()
        return session._sql_result(operation=operation, execute=execute, reuse=reuse)
    if not reuse:
        frame.client.release_prepared()
    return execute()


def _collect_native_relational(client, statement, *, surface, plan_summary, input_kwargs, check, memory_gb, max_parallelism, memory_bytes, resources, inherited, limits, spill, input_batches, native_plan, native_frame):
    return client.public_workflow_run(
        surface, sql_statement=statement, plan_summary=plan_summary,
        requested_output="collect", execution_policy="vortex_middle",
        materialization_policy="bounded", evidence_level="production_admitted_local_workflow",
        bounded=True,
        **_api._terminal_resource_kwargs(memory_gb, max_parallelism, spill,
                                    memory_bytes=memory_bytes, resources=resources,
                                    inherited=inherited, limits=limits),
        check=check, **input_kwargs,
        input_batches=input_batches,
        **({"_native_plan": native_plan, "_native_frame": native_frame} if getattr(client, "transport", "subprocess") == "native" else {}),
    ).envelope
