//! Report the native providers, explicit materialization and observation limits.

use super::{Metrics, Result};
use shardloom_core::{
    NativeIoAdapterFidelityReport, NativeIoCertificate, NativeIoMaterializationBoundaryReport,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, RepresentationState,
};

#[allow(clippy::too_many_lines)] // One declarative certificate covers the same execution.
pub(super) fn certificate(
    metrics: &Metrics,
    rows: u64,
    batch_rows: usize,
    sources: usize,
    memory_sources: usize,
    spill: Option<&crate::relational_query::VortexRelationalSpillReport>,
) -> Result<NativeIoCertificate> {
    let file_sources = sources;
    let sources = file_sources + memory_sources;
    let data_work = metrics.data_scans.get() > 0;
    let spilled = spill.is_some_and(|report| report.runs_written > 0);
    NativeIoCertificate::new(
        "native_relational.execution.v1",
        "native_vortex_sources_to_relational_batches",
        NativeIoSourceCapabilityReport {
            source_kind: "vortex".into(),
            adapter_id: "shardloom.native_relational.vortex_0_85".into(),
            schema_discovery_status: if metrics.schema_discovery_stages.get() == 0 {
                format!("bound_before_execution_sources={sources}")
            } else {
                format!(
                    "source_schemas_prepared_before_execution_sources={sources};dynamic_schemas_bound_in_execution={}",
                    metrics.schema_discovery_stages.get()
                )
            },
            statistics_availability: format!(
                "held_native_file_metadata_sources={file_sources};immutable_native_array_sources={memory_sources}"
            ),
            pushdown_capabilities: "bound_projection_and_native_predicates".into(),
            encoded_representation_preserved: true,
            range_read_capability: file_sources > 0,
            streaming_capability: true,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: vec!["native_bound_scan".into()],
            rejected_operations: vec![],
            guarantee: "bound_types_and_generation_checked_before_and_after_final_consumer".into(),
            proof_basis: format!(
                "scans_started={} scans_metadata_pruned={} native_batches={} delivered_scan_rows={} unary_stages={} unary_retained_state_items={} unary_stages_retaining_complete_population={} dynamic_schema_stages={}",
                metrics.scans_started.get(),
                metrics.scans_pruned.get(),
                metrics.scan_batches.get(),
                metrics.scan_rows.get(),
                metrics.unary_stages.get(),
                metrics.unary_state_items.get(),
                metrics.unary_population_retention.get(),
                metrics.schema_discovery_stages.get()
            ),
            residual_expression: (metrics.residual_batches.get() > 0).then(|| {
                format!(
                    "bound ShardLoom native residual evaluation; batches={}",
                    metrics.residual_batches.get()
                )
            }),
            conservative_false_positive_policy: true,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        if data_work {
            vec![NativeIoRepresentationTransition::new(
                RepresentationState::VortexEncoded,
                RepresentationState::DecodedColumnar,
                true,
            )]
        } else {
            vec![]
        },
        NativeIoSinkRequirementReport {
            target_format: "native_relational_result_batches".into(),
            accepts_encoded: false,
            requires_decoded_columnar: true,
            requires_rows: false,
            preserves_metadata: false,
            requires_ordering: true,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: true,
            max_chunk_size: Some(batch_rows as u64),
            backpressure_policy: "synchronous_consumer_controls_next_batch".into(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "shardloom.native_relational.vortex_0_85".into(),
            source_kind: "vortex".into(),
            sink_kind: "native_relational_result_batches".into(),
            metadata_preserved: false,
            statistics_preserved: false,
            encoded_representation_preserved: false,
            materialization_required: data_work,
            fidelity_loss: "none_for_bound_native_values_schema_and_nulls".into(),
            metadata_loss: "computed_columns_do_not_inherit_source_layouts_or_statistics".into(),
            fallback_attempted: false,
        },
        if data_work {
            vec![NativeIoMaterializationBoundaryReport {
                boundary_id: "native_relational.keys_and_selected_payload".into(),
                from_state: RepresentationState::VortexEncoded,
                to_state: RepresentationState::DecodedColumnar,
                required_by: "typed_keys_and_bounded_native_result_consumption".into(),
                reason: format!(
                    "native key execution and native take followed by explicit compact output copies; unary scalar access and retained row state use the shared operation kernels and native output builder; unary stages retaining the complete population={}; native ordering run materialization performed={spilled}; physical decoder bytes are unobserved, so the legacy numeric bytes field is not a zero-decode claim; final output buffer bytes are reported separately; reservations exclude upstream provider scratch and are not an RSS bound",
                    metrics.unary_population_retention.get()
                ),
                bytes_decoded: 0,
                rows_materialized: rows,
                fidelity_loss: "none_for_bound_scalar_values".into(),
                fallback_attempted: false,
            }]
        } else {
            vec![]
        },
        NativeIoSideEffectReport {
            data_read: data_work,
            data_decoded: data_work,
            data_materialized: data_work,
            row_read: data_work,
            arrow_converted: false,
            object_store_io: false,
            write_io: spilled,
            spill_io_performed: spilled,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
        },
        vec![],
    )
}
