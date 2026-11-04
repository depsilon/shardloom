"""Deferred implementations for client commands.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def ShardLoomClient_engine_selection_plan(self, engine, *, boundedness, update_mode, output_mode, check):

    return _api.EngineSelectionPlan(
        self.run(
            [
                "engine-selection-plan",
                engine,
                boundedness,
                update_mode,
                output_mode,
            ],
            check=check,
        )
    )


def ShardLoomClient_live_fixture_run(self, operator, argument, *, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    args = ["live-fixture-run", operator]
    if argument is not None:
        args.append(str(argument) if isinstance(argument, str) else _api._columns_arg(argument))
    args.extend(_api.resource_command_args(allocation))
    return _api.LiveFixtureRunReport(self.run(args, check=check))


def ShardLoomClient_hybrid_overlay_run(self, operator, argument, *, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    args = ["hybrid-overlay-run", operator]
    if argument is not None:
        args.append(str(argument) if isinstance(argument, str) else _api._columns_arg(argument))
    args.extend(_api.resource_command_args(allocation))
    return _api.HybridOverlayRunReport(self.run(args, check=check))


def ShardLoomClient_live_hybrid_state_transition_smoke(self, *, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    return _api.LiveHybridStateTransitionReport(
        self.run(["live-hybrid-state-transition-smoke", *_api.resource_command_args(allocation)], check=check)
    )


def ShardLoomClient_live_hybrid_durable_checkpoint_smoke(self, checkpoint_dir, *, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    return _api.LiveHybridDurableCheckpointReport(
        self.run(
            ["live-hybrid-durable-checkpoint-smoke", str(checkpoint_dir), *_api.resource_command_args(allocation)],
            check=check,
        )
    )


def ShardLoomClient_distributed_local_fixture_run(self, worker_count, fault_mode, *, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    return _api.LocalDistributedFixtureRunReport(
        self.run(
            ["distributed-local-fixture-run", str(worker_count), fault_mode, *_api.resource_command_args(allocation)],
            check=check,
        )
    )


def ShardLoomClient_vortex_prepare(self, source_path, target_vortex_path, *, input_format, schema, allow_overwrite, certification_level, memory_gb, memory_bytes, resources, max_parallelism, delta_source_path, delta_target_vortex_path, delta_update_mode, check):

    resources = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )
    command: list[CommandPart] = [
        "vortex-prepare",
        str(source_path),
        str(target_vortex_path),
    ]
    if input_format is not None:
        command.extend(["--input-format", input_format])
    schema_arg = _api._schema_command_arg(schema)
    if schema_arg is not None:
        command.extend(["--schema", schema_arg])
    if allow_overwrite:
        command.append("--allow-overwrite")
    if certification_level != "ingest_certified":
        command.extend(["--certification-level", certification_level])
    command.extend(_api.resource_command_args(resources))
    if delta_source_path is not None:
        command.extend(["--delta-source", str(delta_source_path)])
    if delta_target_vortex_path is not None:
        command.extend(["--delta-target", str(delta_target_vortex_path)])
    if delta_update_mode != "append-only":
        command.extend(["--delta-update-mode", delta_update_mode])
    return _api.VortexIngestSmokeReport(self.run(command, check=check))


def ShardLoomClient_vortex_run(self, dataset_uri, primitive, *, memory_gb, memory_bytes, resources, max_parallelism, check):

    if primitive.strip().lower() not in {"count", "count_all"}:
        allocation = _api.resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism, inherited=self.resources, limits=self.resource_limits,
        )
        return self.run(
            ["vortex-run", str(dataset_uri), primitive, *_api.resource_command_args(allocation)],
            check=check,
        )
    return self.public_workflow_run(
        "cli", input_uri=dataset_uri, input_format="vortex",
        requested_output="collect", execution_policy="native_vortex",
        materialization_policy="zero_decode", evidence_level="runtime_smoke",
        bounded=True, vortex_primitive=primitive,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
        check=check,
    ).envelope


def ShardLoomClient_vortex_count(self, dataset_uri, *, execute_local_encoded_count, memory_gb, memory_bytes, resources, max_parallelism, check):

    args = ["vortex-count", str(dataset_uri)]
    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism, inherited=self.resources, limits=self.resource_limits,
    )
    if execute_local_encoded_count:
        args.append("--execute-local-encoded-count")
    args.extend(_api.resource_command_args(allocation))
    return self.run(args, check=check)


def ShardLoomClient_vortex_count_where(self, dataset_uri, predicate, *, execute_local_primitive, memory_gb, memory_bytes, resources, max_parallelism, check):

    if execute_local_primitive:
        return self.public_workflow_run(
            "cli",
            input_uri=dataset_uri,
            input_format="vortex",
            requested_output="collect",
            execution_policy="native_vortex",
            materialization_policy="zero_decode",
            evidence_level="runtime_smoke",
            bounded=True,
            vortex_primitive="count_where",
            vortex_predicate=predicate,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ).envelope
    args = ["vortex-count-where", str(dataset_uri), predicate]
    _api._append_resource_execution_args(
        args,
        option="--execute-local-primitive",
        enabled=execute_local_primitive,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
    )
    return self.run(args, check=check)


def ShardLoomClient_vortex_filter(self, dataset_uri, predicate, *, source_order_limit, execute_local_primitive, memory_gb, memory_bytes, resources, max_parallelism, check):

    if execute_local_primitive:
        return self.public_workflow_run(
            "cli",
            input_uri=dataset_uri,
            input_format="vortex",
            requested_output="collect",
            execution_policy="native_vortex",
            materialization_policy="zero_decode",
            evidence_level="runtime_smoke",
            bounded=True,
            vortex_primitive="filter",
            vortex_predicate=predicate,
            vortex_source_order_limit=source_order_limit,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ).envelope
    args = ["vortex-filter", str(dataset_uri), predicate]
    if source_order_limit is not None:
        args.extend(
            [
                "--limit",
                str(_api._positive_int("source_order_limit", source_order_limit)),
            ]
        )
    _api._append_resource_execution_args(
        args,
        option="--execute-local-primitive",
        enabled=execute_local_primitive,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
    )
    return self.run(args, check=check)


def ShardLoomClient_vortex_project(self, dataset_uri, columns, *, source_order_limit, execute_local_primitive, memory_gb, memory_bytes, resources, max_parallelism, check):

    if execute_local_primitive:
        return self.public_workflow_run(
            "cli",
            input_uri=dataset_uri,
            input_format="vortex",
            requested_output="collect",
            execution_policy="native_vortex",
            materialization_policy="zero_decode",
            evidence_level="runtime_smoke",
            bounded=True,
            vortex_primitive="project",
            vortex_columns=columns,
            vortex_source_order_limit=source_order_limit,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ).envelope
    args = ["vortex-project", str(dataset_uri), _api._columns_arg(columns)]
    if source_order_limit is not None:
        args.extend(
            [
                "--limit",
                str(_api._positive_int("source_order_limit", source_order_limit)),
            ]
        )
    _api._append_resource_execution_args(
        args,
        option="--execute-local-primitive",
        enabled=execute_local_primitive,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
    )
    return self.run(args, check=check)


def ShardLoomClient_vortex_filter_project(self, dataset_uri, predicate, columns, *, source_order_limit, execute_local_primitive, memory_gb, memory_bytes, resources, max_parallelism, check):

    if execute_local_primitive:
        return self.public_workflow_run(
            "cli",
            input_uri=dataset_uri,
            input_format="vortex",
            requested_output="collect",
            execution_policy="native_vortex",
            materialization_policy="zero_decode",
            evidence_level="runtime_smoke",
            bounded=True,
            vortex_primitive="filter_project",
            vortex_predicate=predicate,
            vortex_columns=columns,
            vortex_source_order_limit=source_order_limit,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ).envelope
    args = [
        "vortex-filter-project",
        str(dataset_uri),
        predicate,
        _api._columns_arg(columns),
    ]
    if source_order_limit is not None:
        args.extend(
            [
                "--limit",
                str(_api._positive_int("source_order_limit", source_order_limit)),
            ]
        )
    _api._append_resource_execution_args(
        args,
        option="--execute-local-primitive",
        enabled=execute_local_primitive,
        memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
        max_parallelism=max_parallelism,
    )
    return self.run(args, check=check)


def ShardLoomClient_local_vortex_primitive_smoke(self, dataset_uri, *, predicate, columns, memory_gb, memory_bytes, resources, max_parallelism, check):

    resources = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )
    return _api.LocalVortexPrimitiveSmokeReport(
        count=self.vortex_run(
            dataset_uri,
            "count",
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ),
        count_where=self.vortex_count_where(
            dataset_uri,
            predicate,
            execute_local_primitive=True,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ),
        filter=self.vortex_filter(
            dataset_uri,
            predicate,
            execute_local_primitive=True,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ),
        project=self.vortex_project(
            dataset_uri,
            columns,
            execute_local_primitive=True,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ),
        filter_project=self.vortex_filter_project(
            dataset_uri,
            predicate,
            columns,
            execute_local_primitive=True,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
            check=check,
        ),
    )


def ShardLoomClient_vortex_output_payload_plan(self, target_uri, workspace_path, signals, *, check):

    return self.run(
        [
            "vortex-output-payload-plan",
            str(target_uri),
            str(workspace_path),
            _api._signals_arg(signals),
        ],
        check=check,
    )


def ShardLoomClient_vortex_commit_protocol_plan(self, target_uri, current_state, transition, signals, *, check):

    return self.run(
        [
            "vortex-commit-protocol-plan",
            str(target_uri),
            current_state,
            transition,
            _api._signals_arg(signals),
        ],
        check=check,
    )


def ShardLoomClient_vortex_local_commit_recovery_plan(self, target_uri, workspace_path, signals, *, check):

    return self.run(
        [
            "vortex-local-commit-recovery-plan",
            str(target_uri),
            str(workspace_path),
            _api._signals_arg(signals),
        ],
        check=check,
    )


def ShardLoomClient_local_table_metadata_read_smoke(self, *, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = ["local-table-metadata-read-smoke"]
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_object_store_read_smoke(self, local_object_path, *, profile, byte_range, public_fixture_path, fixture_listing, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = ["object-store-read-smoke", str(local_object_path), "--profile", profile]
    if public_fixture_path is not None:
        command.extend(["--public-fixture-path", str(public_fixture_path)])
    if fixture_listing:
        command.append("--fixture-listing")
    if byte_range is not None:
        offset, length = byte_range
        command.extend(["--range", f"{offset}:{length}"])
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_object_store_partition_discovery_smoke(self, local_partition_root, *, profile, partition_columns, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = [
        "object-store-partition-discovery-smoke",
        str(local_partition_root),
        "--profile",
        profile,
    ]
    if partition_columns is not None:
        columns = [
            str(column).strip()
            for column in partition_columns
            if str(column).strip()
        ]
        if columns:
            command.extend(["--partition-columns", ",".join(columns)])
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_object_store_write_smoke(self, source_path, target_object_path, *, profile, idempotency_key, allow_overwrite, rollback_after_commit, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = [
        "object-store-write-smoke",
        str(source_path),
        str(target_object_path),
        "--profile",
        profile,
    ]
    if idempotency_key is not None:
        command.extend(["--idempotency-key", idempotency_key])
    if allow_overwrite:
        command.append("--allow-overwrite")
    if rollback_after_commit:
        command.append("--rollback-after-commit")
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_local_table_append_commit_rehearsal_smoke(self, target_manifest_path, *, profile, idempotency_key, allow_overwrite, rollback_after_commit, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = [
        "local-table-append-commit-rehearsal-smoke",
        str(target_manifest_path),
        "--profile",
        profile,
    ]
    if idempotency_key is not None:
        command.extend(["--idempotency-key", idempotency_key])
    if allow_overwrite:
        command.append("--allow-overwrite")
    if rollback_after_commit:
        command.append("--rollback-after-commit")
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_object_store_write_recovery_smoke(self, target_object_path, *, profile, idempotency_key, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = [
        "object-store-write-recovery-smoke",
        str(target_object_path),
        "--profile",
        profile,
    ]
    if idempotency_key is not None:
        command.extend(["--idempotency-key", idempotency_key])
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_local_table_commit_recovery_smoke(self, target_manifest_path, *, profile, idempotency_key, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command = [
        "local-table-commit-recovery-smoke",
        str(target_manifest_path),
        "--profile",
        profile,
    ]
    if idempotency_key is not None:
        command.extend(["--idempotency-key", idempotency_key])
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_workflow_readiness_smoke(self, *, target_uri, workspace_path, compatibility_target_uri, remote_sources, check):

    remote_items = _api._workflow_remote_source_items(remote_sources)
    output_commit = (
        _api.WorkflowReadinessPlan(
            "vortex_output_target",
            self.vortex_output_plan(target_uri, check=check),
        ),
        _api.WorkflowReadinessPlan(
            "compatibility_export_target",
            self.translation_plan(compatibility_target_uri, check=check),
        ),
        _api.WorkflowReadinessPlan(
            "native_plan_export",
            self.plan_export("native", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_write_intent",
            self.vortex_write_intent_plan(
                target_uri,
                _api.DEFAULT_VORTEX_WRITE_INTENT_SIGNALS,
                check=check,
            ),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_output_payload",
            self.vortex_output_payload_plan(
                target_uri,
                workspace_path,
                _api.DEFAULT_VORTEX_OUTPUT_PAYLOAD_SIGNALS,
                check=check,
            ),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_staged_manifest",
            self.vortex_staged_manifest_file_plan(
                workspace_path,
                _api.DEFAULT_VORTEX_STAGED_MANIFEST_SIGNALS,
                check=check,
            ),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_commit_marker",
            self.vortex_commit_marker_plan(
                workspace_path,
                _api.DEFAULT_VORTEX_COMMIT_MARKER_SIGNALS,
                check=check,
            ),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_commit_intent",
            self.vortex_commit_intent_plan(
                target_uri,
                _api.DEFAULT_VORTEX_COMMIT_INTENT_SIGNALS,
                check=check,
            ),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_commit_protocol",
            self.vortex_commit_protocol_plan(
                target_uri,
                "not-started",
                "validate-intent",
                _api.DEFAULT_VORTEX_COMMIT_PROTOCOL_SIGNALS,
                check=check,
            ),
        ),
        _api.WorkflowReadinessPlan(
            "vortex_local_commit_recovery",
            self.vortex_local_commit_recovery_plan(
                target_uri,
                workspace_path,
                _api.DEFAULT_VORTEX_LOCAL_COMMIT_RECOVERY_SIGNALS,
                check=check,
            ),
        ),
    )
    table_remote = (
        _api.WorkflowReadinessPlan(
            "table_intelligence",
            self.table_intelligence_plan(check=check),
        ),
        _api.WorkflowReadinessPlan(
            "table_compat_iceberg",
            self.table_compat_plan("iceberg", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "table_compat_delta",
            self.table_compat_plan("delta", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "layout_health",
            self.layout_health_plan("healthy", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "compaction",
            self.compaction_plan("small-files", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "catalog_metadata_gate",
            self.catalog_metadata_gate(check=check),
        ),
        _api.WorkflowReadinessPlan(
            "object_store_request",
            self.object_store_request_plan("ready", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "object_store_range",
            self.object_store_range_plan("s3-ranges", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "object_store_coalesce",
            self.object_store_coalesce_plan("s3-ranges", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "object_store_schedule",
            self.object_store_schedule_plan("s3-ranges", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "object_store_checkpoint_retry",
            self.object_store_checkpoint_retry_plan("ready", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "object_store_commit",
            self.object_store_commit_plan("ready", check=check),
        ),
        *(
            _api.WorkflowReadinessPlan(
                f"remote_input_{name}",
                self.input_plan(uri, check=check),
            )
            for name, uri in remote_items
        ),
    )
    evidence = (
        _api.WorkflowReadinessPlan(
            "migration_capabilities",
            self.capabilities("migration", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "correctness_plan",
            self.correctness_plan(check=check),
        ),
        _api.WorkflowReadinessPlan(
            "benchmark_claim_evidence",
            self.benchmark_claim_evidence_plan("foundation", check=check),
        ),
        _api.WorkflowReadinessPlan(
            "world_class_sufficiency",
            self.world_class_sufficiency_plan(check=check),
        ),
    )
    return _api.WorkflowReadinessSmokeReport(
        output_commit=output_commit,
        table_remote=table_remote,
        evidence=evidence,
    )


def ShardLoomClient_udf_local_scalar_fixture_smoke(self, values, *, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )

    if isinstance(values, str):
        encoded_values = values
    else:
        encoded_values = ",".join("null" if value is None else str(value) for value in values)
    return self.run(["udf-local-scalar-fixture-smoke", encoded_values, *_api.resource_command_args(allocation)], check=check)


def ShardLoomClient_embedding_vector_local_fixture_smoke(self, texts, *, query, memory_gb, memory_bytes, max_parallelism, resources, check):

    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, inherited=self.resources, limits=self.resource_limits,
    )

    if isinstance(texts, str):
        encoded_texts = texts
    else:
        encoded_texts = ";".join(str(text) for text in texts)
    command: list[CommandPart] = ["embedding-vector-local-fixture-smoke", encoded_texts]
    if query is not None:
        command.extend(["--query", query])
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_sqlite_local_import_export_smoke(self, database_path, *, table, export_jsonl, roundtrip_db, order_by, allow_overwrite, memory_gb, memory_bytes, max_parallelism, resources, check):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb,
        memory_bytes=memory_bytes,
        max_parallelism=max_parallelism,
        resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )

    command: list[CommandPart] = [
        "sqlite-local-import-export-smoke",
        str(database_path),
        "--table",
        table,
        "--export-jsonl",
        str(export_jsonl),
        "--roundtrip-db",
        str(roundtrip_db),
    ]
    if order_by is not None:
        command.extend(["--order-by", order_by])
    if allow_overwrite:
        command.append("--allow-overwrite")
    command.extend(_api.resource_command_args(allocation))
    return self.run(command, check=check)


def ShardLoomClient_compatibility_source_smoke(self, sources, *, check):

    source_items = _api._compatibility_source_items(sources)
    return _api.CompatibilitySourceSmokeReport(
        input_adapters=self.input_adapters(check=check),
        native_io_envelope=self.native_io_envelope_plan(check=check),
        sources=tuple(
            _api.CompatibilitySourcePlan(
                source_name=name,
                dataset_uri=str(uri),
                plan=self.input_plan(uri, check=check),
            )
            for name, uri in source_items
        ),
    )
