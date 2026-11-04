"""Deferred implementations for query action methods.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def LazyFrame_route(self, *, requested_output, output_ref, execution_policy, materialization_policy, evidence_level, bounded, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    normalized_bounded = (
        self._has_declared_limit()
        if bounded is None and requested_output == "collect"
        else bounded
    )
    effective_evidence_level = evidence_level
    if (
        evidence_level == "runtime_smoke"
        and _api._is_declared_local_source(self.source)
    ):
        effective_evidence_level = "production_admitted_local_workflow"
    effective_execution_policy = (
        _api._public_workflow_default_execution_policy(self.source)
        if execution_policy is None
        else execution_policy
    )
    relational_statement = self._native_relational_statement()
    return self.client.public_workflow_route(
        self._surface,
        **self._declared_input_kwargs(),
        sql_statement=relational_statement,
        plan_summary=self.operation_summary,
        requested_output=requested_output,
        output_ref=output_ref,
        execution_policy=effective_execution_policy,
        materialization_policy=materialization_policy,
        evidence_level=effective_evidence_level,
        bounded=normalized_bounded,
        check=check,
        **_api._terminal_resource_kwargs(memory_gb, max_parallelism, spill, inherited=self.resources, limits=self.resource_limits, memory_bytes=memory_bytes, resources=resources, required=False),
    )


def LazyFrame_run(self, *, requested_output, output_ref, execution_policy, materialization_policy, evidence_level, bounded, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    normalized_bounded = (
        self._has_declared_limit()
        if bounded is None and requested_output == "collect"
        else bounded
    )
    effective_execution_policy = (
        _api._public_workflow_default_execution_policy(self.source)
        if execution_policy is None
        else execution_policy
    )
    relational_statement = self._native_relational_statement()
    return self.client.public_workflow_run(
        self._surface,
        **self._declared_input_kwargs(),
        sql_statement=relational_statement,
        plan_summary=self.operation_summary,
        requested_output=requested_output,
        output_ref=output_ref,
        execution_policy=effective_execution_policy,
        materialization_policy=materialization_policy,
        evidence_level=evidence_level,
        bounded=normalized_bounded,
        **_api._native_plan_kwargs(self),
        **_api._terminal_resource_kwargs(memory_gb, max_parallelism, spill, inherited=self.resources, limits=self.resource_limits, memory_bytes=memory_bytes, resources=resources),
        check=check,
    )


def LazyFrame_prepare(self, target_vortex_path, *, evidence_level, memory_gb, memory_bytes, max_parallelism, resources, check):

    source = self.source
    if source.relation is not None:
        relation = source.relation
        if relation.input_uri is not None:
            source = next((item for item in relation.source_bindings
                           if item.uri == relation.input_uri),
                          _api.WorkflowSource(relation.input_format or
                              _api._source_format_for_local_source_ref(relation.input_uri) or "auto",
                              relation.input_uri))
        elif len(relation.source_bindings) == 1 and not relation.relations:
            source = relation.source_bindings[0]
        else:
            raise ValueError("prepare() requires one explicit file source; use write_vortex() to execute a composed query")
    return self.client.public_workflow_prepare(
        self._surface,
        input_uri=source.uri,
        input_format=_api._public_workflow_input_format(source),
        source_schema=_api._prepare_vortex_schema_hints(source),
        output_ref=target_vortex_path,
        plan_summary=self.operation_summary,
        evidence_level=evidence_level,
        **_api._terminal_resource_kwargs(memory_gb, max_parallelism,
            memory_bytes=memory_bytes, resources=resources,
            inherited=self.resources, limits=self.resource_limits),
        check=check,
    )


def LazyFrame_iter_batches(self, *, batch_rows, memory_gb, memory_bytes, resources, max_parallelism, spill, output_window_bytes):
    statement = self._relation_statement()
    if statement is None:
        raise ValueError("batch consumption requires an admitted native SQL declaration")
    return self.client.public_workflow_batches(
        self._surface, batch_rows=batch_rows, sql_statement=statement,
        output_window_bytes=output_window_bytes,
        **_api._native_plan_kwargs(self),
        **self._declared_input_kwargs(),
        input_batches=_api._workflow_batch_inputs(self._declared_sources()),
        plan_summary=self.operation_summary, evidence_level="production_admitted_local_workflow",
        **_api._terminal_resource_kwargs(memory_gb, max_parallelism, spill, inherited=self.resources, limits=self.resource_limits, memory_bytes=memory_bytes, resources=resources),
    )


def LazyFrame_write_jsonl(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="jsonl",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_write_json(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="json",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_write_csv(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="csv",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_write_parquet(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="parquet",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_write_arrow_ipc(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="arrow-ipc",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_write_avro(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="avro",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_write_orc(self, target_uri, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):

    return self.write(
        target_uri,
        output_format="orc",
        allow_overwrite=allow_overwrite,
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        spill=spill,
        check=check,
    )


def LazyFrame_fanout(self, outputs, *, allow_overwrite, reuse, memory_gb, memory_bytes, resources, max_parallelism, spill, check):
    normalized_outputs = _api._normalize_fanout_outputs(outputs)
    output_format, output_path = normalized_outputs[0]
    if self._relation_statement() is None:
        return self._unsupported_operation("fanout", str(output_path), check=check)
    return self._public_workflow_write_report(
        output_path, requested_output=_api._public_write_request_for_format(output_format),
        allow_overwrite=allow_overwrite, fanout_outputs=normalized_outputs[1:],
        reuse=reuse,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check,
    )


def LazyFrame_to_pandas(self, *, limit, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    pandas = _api._optional_module("pandas")
    if pandas is None:
        return self._unsupported_operation(
            "to-pandas",
            "missing optional dependency: pandas",
            check=check,
        )
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._result_to_pandas(report, pandas)


def LazyFrame_to_arrow(self, *, limit, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    pyarrow = _api._optional_module("pyarrow")
    if pyarrow is None:
        return self._unsupported_operation(
            "to-arrow",
            "missing optional dependency: pyarrow",
            check=check,
        )
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._result_to_arrow_table(report, pyarrow)


def LazyFrame_to_arrow_table(self, *, limit, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    pyarrow = _api._optional_module("pyarrow")
    if pyarrow is None:
        return self._unsupported_operation(
            "to-arrow-table",
            "missing optional dependency: pyarrow",
            check=check,
        )
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._result_to_arrow_table(report, pyarrow)


def LazyFrame_to_arrow_ipc(self, *, limit, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    pyarrow = _api._optional_module("pyarrow")
    if pyarrow is None:
        return self._unsupported_operation(
            "to-arrow-ipc",
            "missing optional dependency: pyarrow",
            check=check,
        )
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._result_to_arrow_ipc(report, pyarrow)


def LazyFrame_to_numpy(self, *, limit, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    numpy = _api._optional_module("numpy")
    if numpy is None:
        return self._unsupported_operation(
            "to-numpy",
            "missing optional dependency: numpy",
            check=check,
        )
    report = self._bounded_materialization_report(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return _api._result_to_numpy(report, numpy)


def LazyFrame_to_python_objects(self, *, check, memory_gb, memory_bytes, max_parallelism, resources):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    report = self._bounded_materialization_report(limit=None, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    return report.python_objects


def LazyFrame_prepare_vortex(self, target_vortex_path, *, workspace, input_format, memory_gb, memory_bytes, resources, max_parallelism, allow_overwrite, certification_level, check):

    resources = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )
    if self.engine_mode not in {"auto", "batch"}:
        raise ValueError(
            "LazyFrame.prepare_vortex currently supports engine_mode='auto' or 'batch' "
            "for scoped local batch preparation; live/hybrid preparation remains gated"
        )
    if self.source.memory_input:
        target = _api._generated_prepared_vortex_target_path(
            self.source.uri.rsplit("/", 1)[-1],
            target_vortex_path=target_vortex_path, workspace=workspace,
        )
        return self.write_vortex(
            target, allow_overwrite=allow_overwrite, check=check,
            resources=resources,
        )
    if self.source.source_format == "vortex":
        raise ValueError(
            "LazyFrame.prepare_vortex starts from raw compatibility input; "
            "read_vortex(...) sources are already Vortex-native"
        )
    if not _api._is_declared_local_source(self.source):
        raise ValueError(
            "LazyFrame.prepare_vortex requires a local CSV, JSON/JSONL/NDJSON, Parquet, "
            "Arrow IPC, Avro, or ORC source"
        )
    if self.operations:
        raise ValueError(
            "LazyFrame.prepare_vortex prepares the raw local source before query operators; "
            "call it directly on read_*(...) or use write_vortex(...) for a query-result sink"
        )
    target = _api._prepared_vortex_target_path(
        self.source.uri,
        target_vortex_path=target_vortex_path,
        workspace=workspace,
    )
    return self.client.vortex_prepare(
        self.source.uri,
        target,
        input_format=input_format,
        schema=_api._prepare_vortex_schema_hints(self.source),
        allow_overwrite=allow_overwrite,
        certification_level=certification_level,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        check=check,
    )
