//! Shared single-source scan loop over one resident native file generation.
//!
//! `CountWhere`/filter/project execution uses the resident allocator and
//! generation gate without changing projection, predicate, or result semantics.
//! The ordinary reader prepares a source for each call. Callers that already
//! retain a source can use `read_prepared`; neither path caches query answers.
//! Cache admission/evidence is deliberately a separate next integration: the
//! existing count projection pins predicate fields and has no measured saving.

use super::{
    DatasetUri, Instant, LocalVortexScan, LocalVortexScanPlan, MaterializedPredicateEvaluator,
    ResidualPredicateMaterialization, Result, ShardLoomError, UniversalInputSource,
    VortexLocalPrimitiveEmbeddedLayoutReport, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveKind, VortexReaderBackedSplitEvidence, bind_vortex_scan_expr,
    native_numeric_execution_ctx, plan_vortex_reader_generated_prepared_batch_envelopes,
    row_export_columns_from_chunk, row_export_materialized_row_count, vortex_error,
};
use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};
use shardloom_exec::compute_pool::CancellationToken;

/// Preserve the existing planner callback and reports while owning the source,
/// provider drivers, and native allocator for the complete operation.
pub(super) fn read(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    configure: impl FnOnce(&vortex::array::dtype::DType) -> Result<LocalVortexScanPlan>,
) -> Result<LocalVortexScan> {
    let preparation_started = Instant::now();
    let resident = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.resource_envelope.max_parallelism,
    )?;
    let source = resident.prepare_file(path)?;
    let preparation_micros = preparation_started.elapsed().as_micros();
    let mut scan = read_prepared(source_uri, &source, primitive_kind, policy, configure)?;
    scan.control_plane_micros = scan.control_plane_micros.saturating_add(preparation_micros);
    Ok(scan)
}

/// Execute against the caller-retained file and allocator. Only this call's
/// planning/control work is timed here; opening the source belongs to its owner.
/// The source generation is checked before and after actual execution, including
/// a metadata-pruned result. A source with a wider resource grant is rejected.
/// The caller supplies the admitted URI associated with this retained source;
/// it identifies reader evidence and is never reopened by this function.
pub(super) fn read_prepared(
    source_uri: &DatasetUri,
    source: &PreparedVortexSource,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    configure: impl FnOnce(&vortex::array::dtype::DType) -> Result<LocalVortexScanPlan>,
) -> Result<LocalVortexScan> {
    read_prepared_controlled(
        source_uri,
        source,
        primitive_kind,
        policy,
        &CancellationToken::default(),
        configure,
    )
    .map(|(scan, _)| scan)
}

/// Execute against a caller-retained source with its operation cancellation token.
/// The source generation, provider I/O drain, and execution admission remain owned
/// by the source's existing resident session.
///
/// # Errors
/// Rejects cancellation, resource-policy disagreement, source mutation, and native
/// provider failures without reopening the source or changing scan semantics.
pub(super) fn read_prepared_controlled(
    source_uri: &DatasetUri,
    source: &PreparedVortexSource,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    cancellation: &CancellationToken,
    configure: impl FnOnce(&vortex::array::dtype::DType) -> Result<LocalVortexScanPlan>,
) -> Result<(LocalVortexScan, usize)> {
    cancellation.check()?;
    source.with_native_execution_controlled(cancellation, |_, context| {
        read_prepared_in_context(
            source_uri,
            source,
            primitive_kind,
            policy,
            context,
            configure,
        )
    })
}

/// Reuse the complete operation's grant for the same scan, pruning and evidence.
pub(super) fn read_prepared_in_context(
    source_uri: &DatasetUri,
    source: &PreparedVortexSource,
    primitive_kind: VortexQueryPrimitiveKind,
    mut policy: VortexLocalPrimitiveExecutionPolicy,
    context: &crate::resident_session::NativeExecutionContext<'_>,
    configure: impl FnOnce(&vortex::array::dtype::DType) -> Result<LocalVortexScanPlan>,
) -> Result<(LocalVortexScan, usize)> {
    let cancellation = context.cancellation();
    cancellation.check()?;
    let preparation_started = Instant::now();
    let (memory_bytes, parallelism) = source.resource_limits();
    if memory_bytes > policy.resource_envelope.memory_budget_bytes
        || parallelism > policy.resource_envelope.max_parallelism
    {
        return Err(ShardLoomError::InvalidOperation(
            "prepared local Vortex source exceeds the scan resource policy; prepare a source within the requested memory and CPU bounds; no fallback execution was attempted".to_string(),
        ));
    }
    // Keep scan task admission and its evidence within the same grant as the
    // held native source, including over-host requests and reused sources.
    policy.max_parallelism = parallelism;
    policy.resource_envelope.max_parallelism = parallelism;
    policy.resource_envelope.scan_concurrency_per_worker = policy
        .resource_envelope
        .scan_concurrency_per_worker
        .min(parallelism);
    let plan = configure(source.dtype())?;
    let preparation_micros = preparation_started.elapsed().as_micros();
    cancellation.check()?;
    source.with_admitted_native_execution(context, |file, context| {
        let started = Instant::now();
        let metadata_pruned = consult_metadata_pruning(file, &plan, cancellation)?;
        let preparation_micros = preparation_micros.saturating_add(started.elapsed().as_micros());
        // This scan owns no other compute pool. Restore drivers only when
        // metadata cannot answer it, within the same source and call grant.
        source.retained_session().with_owned_execution_drivers(
            context,
            metadata_pruned != Some(true),
            || {
                execute_after_pruning(
                    source_uri,
                    primitive_kind,
                    policy,
                    file,
                    context.runtime(),
                    context.native_session(),
                    plan,
                    preparation_micros,
                    cancellation,
                    metadata_pruned,
                )
            },
        )
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute(
    source_uri: &DatasetUri,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file: &vortex::file::VortexFile,
    runtime: &impl vortex::io::runtime::BlockingRuntime,
    session: &vortex::session::VortexSession,
    plan: LocalVortexScanPlan,
    preparation_micros: u128,
    cancellation: &CancellationToken,
) -> Result<LocalVortexScan> {
    let started = Instant::now();
    let metadata_pruned = consult_metadata_pruning(file, &plan, cancellation)?;
    execute_after_pruning(
        source_uri,
        primitive_kind,
        policy,
        file,
        runtime,
        session,
        plan,
        preparation_micros.saturating_add(started.elapsed().as_micros()),
        cancellation,
        metadata_pruned,
    )
}

fn consult_metadata_pruning(
    file: &vortex::file::VortexFile,
    plan: &LocalVortexScanPlan,
    cancellation: &CancellationToken,
) -> Result<Option<bool>> {
    cancellation.check()?;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex source-order limit must be >= 1".to_string(),
        ));
    }
    let metadata_pruned = plan
        .filter
        .as_ref()
        .map(|filter| file.can_prune(filter).map_err(vortex_error))
        .transpose()?;
    cancellation.check()?;
    Ok(metadata_pruned)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn execute_after_pruning(
    source_uri: &DatasetUri,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file: &vortex::file::VortexFile,
    runtime: &impl vortex::io::runtime::BlockingRuntime,
    session: &vortex::session::VortexSession,
    mut plan: LocalVortexScanPlan,
    preparation_micros: u128,
    cancellation: &CancellationToken,
    metadata_pruned: Option<bool>,
) -> Result<LocalVortexScan> {
    cancellation.check()?;
    let mut ctx = native_numeric_execution_ctx(session);
    let control_plane_started = Instant::now();
    let mut control_plane_micros = preparation_micros;
    let mut evidence_collection_micros = 0_u128;
    let source_row_count = file.row_count();
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let residual_predicate = plan.residual_predicate.clone();
    let source_order_limit = plan.source_order_limit;
    let output_columns = plan
        .output_columns
        .clone()
        .unwrap_or_else(|| plan.projected_columns.clone());
    let mut embedded_layout = VortexLocalPrimitiveEmbeddedLayoutReport::from_file(
        file,
        primitive_kind,
        filter_pushdown_applied,
        projection_pushdown_applied,
    );
    if let Some(metadata_pruned) = metadata_pruned {
        embedded_layout.mark_pruning_consulted(metadata_pruned);
        if metadata_pruned {
            let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
            let reader_splits = Vec::new();
            let evidence_started = Instant::now();
            let reader_generated_prepared_batch_report =
                plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits);
            cancellation.check()?;
            evidence_collection_micros =
                evidence_collection_micros.saturating_add(evidence_started.elapsed().as_micros());
            control_plane_micros =
                control_plane_micros.saturating_add(control_plane_started.elapsed().as_micros());
            let scan = LocalVortexScan {
                source_row_count,
                result_row_count: 0,
                pre_limit_result_row_count: 0,
                arrays_read_count: 0,
                reader_splits,
                reader_generated_prepared_batch_report,
                control_plane_micros,
                evidence_collection_micros,
                max_chunk_rows: 0,
                resource_envelope: policy.resource_envelope(),
                max_parallelism_requested: policy.max_parallelism,
                scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
                projected_columns: output_columns,
                filter_pushdown_applied,
                projection_pushdown_applied,
                residual_predicate_materialization: ResidualPredicateMaterialization::from_flags(
                    residual_predicate.is_some(),
                    false,
                ),
                source_order_limit,
                embedded_layout,
                embedded_derived_column_rewrites: plan.embedded_derived_column_rewrites.clone(),
            };
            cancellation.check()?;
            return Ok(scan);
        }
    }
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = plan.filter.take() {
        scan = scan.with_filter(bind_vortex_scan_expr(file, &filter)?);
    }
    if let Some(projection) = plan.projection.take() {
        scan = scan.with_projection(bind_vortex_scan_expr(file, &projection)?);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());
    let residual_evaluator = residual_predicate
        .as_ref()
        .map(|predicate| {
            MaterializedPredicateEvaluator::compile(predicate, &plan.projected_columns)
        })
        .transpose()?;
    control_plane_micros =
        control_plane_micros.saturating_add(control_plane_started.elapsed().as_micros());
    let mut result_row_count = 0usize;
    let mut pre_limit_result_row_count = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut max_chunk_rows = 0usize;
    let mut residual_predicate_materialized = false;
    let mut chunks = scan.into_array_iter(runtime).map_err(vortex_error)?;
    loop {
        cancellation.check()?;
        let Some(chunk) = chunks.next() else {
            break;
        };
        cancellation.check()?;
        let chunk = chunk.map_err(vortex_error)?;
        cancellation.check()?;
        let rows = chunk.len();
        let evidence_started = Instant::now();
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            rows,
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )?;

        reader_splits.push(split);
        evidence_collection_micros =
            evidence_collection_micros.saturating_add(evidence_started.elapsed().as_micros());
        let chunk_result_rows = if let Some(predicate) = residual_evaluator.as_ref() {
            if let Some(selected) =
                predicate.fast_count_matches_in_chunk(&chunk, &plan.projected_columns, &mut ctx)?
            {
                selected
            } else {
                residual_predicate_materialized = true;
                let columns =
                    row_export_columns_from_chunk(&chunk, &plan.projected_columns, &mut ctx)?;
                let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
                let mut selected = 0usize;
                for row_index in 0..materialized_rows {
                    if row_index % 4096 == 0 {
                        cancellation.check()?;
                    }
                    if predicate.matches(&columns, row_index)? {
                        selected = selected.checked_add(1).ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "local Vortex primitive residual selected row count overflowed usize"
                                    .to_string(),
                            )
                        })?;
                    }
                }
                selected
            }
        } else {
            rows
        };
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(chunk_result_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex primitive pre-limit result row count overflowed usize"
                        .to_string(),
                )
            })?;
        let output_rows = source_order_limit.map_or(chunk_result_rows, |limit| {
            limit
                .saturating_sub(result_row_count)
                .min(chunk_result_rows)
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex primitive result row count overflowed usize".to_string(),
            )
        })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
        if source_order_limit.is_some_and(|limit| result_row_count >= limit) {
            break;
        }
    }
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let evidence_started = Instant::now();
    // Certify the reader boundary without copying executable values for reporting.
    let reader_generated_prepared_batch_report =
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits);
    cancellation.check()?;
    evidence_collection_micros =
        evidence_collection_micros.saturating_add(evidence_started.elapsed().as_micros());
    let result = LocalVortexScan {
        source_row_count,
        result_row_count,
        pre_limit_result_row_count,
        arrays_read_count,
        reader_splits,
        reader_generated_prepared_batch_report,
        control_plane_micros,
        evidence_collection_micros,
        max_chunk_rows,
        resource_envelope: policy.resource_envelope(),
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
        projected_columns: output_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        residual_predicate_materialization: ResidualPredicateMaterialization::from_flags(
            residual_predicate.is_some(),
            residual_predicate_materialized,
        ),
        source_order_limit,
        embedded_layout,
        embedded_derived_column_rewrites: plan.embedded_derived_column_rewrites,
    };
    cancellation.check()?;
    Ok(result)
}

#[cfg(test)]
#[path = "local_primitive_prepared_scan_tests.rs"]
mod tests;
