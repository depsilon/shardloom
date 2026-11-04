"""Deferred implementations for client workflow.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def ShardLoomClient_public_workflow_route(self, surface, *, input_uri, input_format, source_schema, source_bindings, sql_statement, plan_summary, requested_output, output_ref, execution_policy, materialization_policy, evidence_level, bounded, fanout_outputs, native_vortex_operation_family, native_vortex_right_input, vortex_primitive, vortex_predicate, vortex_columns, vortex_source_order_limit, vortex_sample_seed, vortex_sample_fraction, vortex_sample_replacement, vortex_sample_weight_column, vortex_duplicate_keep, vortex_deduplicate_key_columns, vortex_expression_projection, vortex_melt_projection, vortex_explode_projection, vortex_pivot_projection, vortex_rolling_window, vortex_sort_rows, vortex_aggregate, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    resources = _api.optional_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )
    args: list[CommandPart] = ["route", surface]
    if input_uri is not None:
        args.extend(["--input", str(input_uri)])
    if input_format is not None:
        args.extend(["--input-format", input_format])
    source_schema_arg = _api._schema_command_arg(source_schema)
    if source_schema_arg is not None:
        args.extend(["--source-schema", source_schema_arg])
    source_bindings_arg = _api._source_bindings_command_arg(source_bindings)
    if source_bindings_arg is not None:
        args.extend(["--source-bindings", source_bindings_arg])
    if sql_statement is not None:
        args.extend(["--sql", sql_statement])
    if plan_summary is not None:
        args.extend(["--plan", plan_summary])
    args.extend(["--request", requested_output])
    if output_ref is not None:
        args.extend(["--output", str(output_ref)])
    for fanout_format, fanout_path in _api._iter_fanout_outputs(fanout_outputs):
        args.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
    args.extend(["--execution-policy", execution_policy])
    args.extend(["--materialization-policy", materialization_policy])
    args.extend(["--evidence-level", evidence_level])
    if bounded is not None:
        args.extend(["--bounded", "true" if bounded else "false"])
    _api._append_public_vortex_payload_args(
        args,
        native_vortex_operation_family=native_vortex_operation_family,
        native_vortex_right_input=native_vortex_right_input,
        vortex_primitive=vortex_primitive,
        vortex_predicate=vortex_predicate,
        vortex_columns=vortex_columns,
        vortex_source_order_limit=vortex_source_order_limit,
        vortex_sample_seed=vortex_sample_seed,
        vortex_sample_fraction=vortex_sample_fraction,
        vortex_sample_replacement=vortex_sample_replacement,
        vortex_sample_weight_column=vortex_sample_weight_column,
        vortex_duplicate_keep=vortex_duplicate_keep,
        vortex_deduplicate_key_columns=vortex_deduplicate_key_columns,
        vortex_expression_projection=vortex_expression_projection,
        vortex_melt_projection=vortex_melt_projection,
        vortex_explode_projection=vortex_explode_projection,
        vortex_pivot_projection=vortex_pivot_projection,
        vortex_rolling_window=vortex_rolling_window,
        vortex_sort_rows=vortex_sort_rows,
        vortex_aggregate=vortex_aggregate,
        resources=resources,
        spill=spill,
    )
    return _api.PublicWorkflowRoute(self.run(args, check=check))


def ShardLoomClient_public_workflow_run(self, surface, *, input_uri, input_format, source_schema, source_bindings, input_batches, sql_statement, plan_summary, requested_output, output_ref, execution_policy, materialization_policy, evidence_level, bounded, allow_overwrite, fanout_outputs, native_vortex_operation_family, native_vortex_right_input, vortex_primitive, vortex_predicate, vortex_columns, vortex_source_order_limit, vortex_sample_seed, vortex_sample_fraction, vortex_sample_replacement, vortex_sample_weight_column, vortex_duplicate_keep, vortex_deduplicate_key_columns, vortex_expression_projection, vortex_melt_projection, vortex_explode_projection, vortex_pivot_projection, vortex_rolling_window, vortex_sort_rows, vortex_aggregate, memory_gb, memory_bytes, resources, max_parallelism, spill, check, _native_plan, _native_frame):

    if self.transport == "native":
        request = dict(locals())
        request.pop("self")
        allocation = _api.resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        return self._native_backend_for(allocation).execute(request, allocation, check=check)
    args = self._public_workflow_facade_args(
        "run",
        surface,
        input_uri=input_uri,
        input_format=input_format,
        source_schema=source_schema,
        source_bindings=source_bindings,
        sql_statement=sql_statement,
        plan_summary=plan_summary,
        requested_output=requested_output,
        output_ref=output_ref,
        execution_policy=execution_policy,
        materialization_policy=materialization_policy,
        evidence_level=evidence_level,
        bounded=bounded,
        allow_overwrite=allow_overwrite,
        fanout_outputs=fanout_outputs,
        native_vortex_operation_family=native_vortex_operation_family,
        native_vortex_right_input=native_vortex_right_input,
        vortex_primitive=vortex_primitive,
        vortex_predicate=vortex_predicate,
        vortex_columns=vortex_columns,
        vortex_source_order_limit=vortex_source_order_limit,
        vortex_sample_seed=vortex_sample_seed,
        vortex_sample_fraction=vortex_sample_fraction,
        vortex_sample_replacement=vortex_sample_replacement,
        vortex_sample_weight_column=vortex_sample_weight_column,
        vortex_duplicate_keep=vortex_duplicate_keep,
        vortex_deduplicate_key_columns=vortex_deduplicate_key_columns,
        vortex_expression_projection=vortex_expression_projection,
        vortex_melt_projection=vortex_melt_projection,
        vortex_explode_projection=vortex_explode_projection,
        vortex_pivot_projection=vortex_pivot_projection,
        vortex_rolling_window=vortex_rolling_window,
        vortex_sort_rows=vortex_sort_rows,
        vortex_aggregate=vortex_aggregate,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
    )
    if input_batches:
        from ._batches import ResultBatchIterator, validate_inputs
        validate_inputs(source_bindings, input_batches)
        with ResultBatchIterator(self, args, inputs=input_batches, stream_results=False, check=check) as batches:
            for _ in batches:
                raise _api.ShardLoomProtocolError("non-streaming transaction emitted a result batch")
            if batches._terminal_envelope is None:
                raise _api.ShardLoomProtocolError("native batch transaction omitted its final report")
            return _api.PublicWorkflowExecution(batches._terminal_envelope)
    return _api.PublicWorkflowExecution(self.run(args, check=check))


def ShardLoomClient_public_workflow_batches(self, surface, *, batch_rows, input_batches, output_window_bytes, **kwargs):
    self._ensure_open()
    from ._native_backend import validate_output_window_bytes
    output_window_bytes = validate_output_window_bytes(output_window_bytes)
    if self.transport == "native":
        request = dict(kwargs, surface=surface, input_batches=input_batches)
        if output_window_bytes:
            request["output_window_bytes"] = output_window_bytes
        allocation = _api.resolve_resources(
            memory_gb=kwargs.get("memory_gb"), memory_bytes=kwargs.get("memory_bytes"),
            max_parallelism=kwargs.get("max_parallelism"), resources=kwargs.get("resources"),
            inherited=self.resources, limits=self.resource_limits,
        )
        iterator = self._native_backend_for(allocation).batches(request, allocation, batch_rows)
        with self._native_backend_lock:
            self._ensure_open()
            iterator._client = self
            self._batch_streams.add(iterator)
        return iterator
    if output_window_bytes:
        raise ValueError("output_window_bytes requires transport='native'")
    kwargs.pop("_native_plan", None)
    kwargs.pop("_native_frame", None)
    from ._batches import ResultBatchIterator, validate_inputs
    if kwargs.get("requested_output", "collect") != "collect":
        raise ValueError("incremental result consumption requires collect output")
    kwargs.setdefault("bounded", True)
    args = self._public_workflow_facade_args("run", surface, **kwargs)
    validate_inputs(kwargs.get("source_bindings"), input_batches)
    return ResultBatchIterator(self, args, inputs=input_batches, batch_rows=batch_rows)


def ShardLoomClient_public_workflow_prepare(self, surface, *, input_uri, output_ref, input_format, source_schema, plan_summary, evidence_level, memory_gb, memory_bytes, resources, max_parallelism, check):

    args = self._public_workflow_facade_args(
        "prepare",
        surface,
        input_uri=input_uri,
        input_format=input_format,
        source_schema=source_schema,
        plan_summary=plan_summary,
        requested_output="prepare",
        output_ref=output_ref,
        execution_policy="prepare_once",
        materialization_policy="bounded",
        evidence_level=evidence_level,
        bounded=True,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
    )
    return _api.PublicWorkflowExecution(self.run(args, check=check))


def ShardLoomClient__public_workflow_facade_args(self, command, surface, *, input_uri, input_format, source_schema, source_bindings, sql_statement, plan_summary, requested_output, output_ref, execution_policy, materialization_policy, evidence_level, bounded, allow_overwrite, fanout_outputs, native_vortex_operation_family, native_vortex_right_input, vortex_primitive, vortex_predicate, vortex_columns, vortex_source_order_limit, vortex_sample_seed, vortex_sample_fraction, vortex_sample_replacement, vortex_sample_weight_column, vortex_duplicate_keep, vortex_deduplicate_key_columns, vortex_expression_projection, vortex_melt_projection, vortex_explode_projection, vortex_pivot_projection, vortex_rolling_window, vortex_sort_rows, vortex_aggregate, memory_gb, memory_bytes, resources, max_parallelism, spill):
    resources = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )
    args: list[CommandPart] = [command, surface]
    if input_uri is not None:
        args.extend(["--input", str(input_uri)])
    if input_format is not None:
        args.extend(["--input-format", input_format])
    source_schema_arg = _api._schema_command_arg(source_schema)
    if source_schema_arg is not None:
        args.extend(["--source-schema", source_schema_arg])
    source_bindings_arg = _api._source_bindings_command_arg(source_bindings)
    if source_bindings_arg is not None:
        args.extend(["--source-bindings", source_bindings_arg])
    if sql_statement is not None:
        args.extend(["--sql", sql_statement])
    if plan_summary is not None:
        args.extend(["--plan", plan_summary])
    args.extend(["--request", requested_output])
    if output_ref is not None:
        args.extend(["--output", str(output_ref)])
    for fanout_format, fanout_path in _api._iter_fanout_outputs(fanout_outputs):
        args.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
    args.extend(["--execution-policy", execution_policy])
    args.extend(["--materialization-policy", materialization_policy])
    args.extend(["--evidence-level", evidence_level])
    if bounded is not None:
        args.extend(["--bounded", "true" if bounded else "false"])
    if allow_overwrite:
        args.append("--allow-overwrite")
    _api._append_public_vortex_payload_args(
        args,
        native_vortex_operation_family=native_vortex_operation_family,
        native_vortex_right_input=native_vortex_right_input,
        vortex_primitive=vortex_primitive,
        vortex_predicate=vortex_predicate,
        vortex_columns=vortex_columns,
        vortex_source_order_limit=vortex_source_order_limit,
        vortex_sample_seed=vortex_sample_seed,
        vortex_sample_fraction=vortex_sample_fraction,
        vortex_sample_replacement=vortex_sample_replacement,
        vortex_sample_weight_column=vortex_sample_weight_column,
        vortex_duplicate_keep=vortex_duplicate_keep,
        vortex_deduplicate_key_columns=vortex_deduplicate_key_columns,
        vortex_expression_projection=vortex_expression_projection,
        vortex_melt_projection=vortex_melt_projection,
        vortex_explode_projection=vortex_explode_projection,
        vortex_pivot_projection=vortex_pivot_projection,
        vortex_rolling_window=vortex_rolling_window,
        vortex_sort_rows=vortex_sort_rows,
        vortex_aggregate=vortex_aggregate,
        resources=resources,
        spill=spill,
    )
    return args
