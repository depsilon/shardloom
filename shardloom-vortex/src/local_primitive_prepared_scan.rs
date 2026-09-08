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
    plan_vortex_reader_generated_prepared_batch_envelopes,
    plan_vortex_reader_generated_prepared_batch_kernel_inputs,
    reader_generated_encoded_kernel_inputs_from_vortex_chunk, row_export_columns_from_chunk,
    row_export_materialized_row_count, vortex_error,
};
use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};

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
    let preparation_started = Instant::now();
    let (memory_bytes, parallelism) = source.resource_limits();
    if memory_bytes > policy.resource_envelope.memory_budget_bytes
        || parallelism > policy.resource_envelope.max_parallelism
    {
        return Err(ShardLoomError::InvalidOperation(
            "prepared local Vortex source exceeds the scan resource policy; prepare a source within the requested memory and CPU bounds; no fallback execution was attempted".to_string(),
        ));
    }
    let plan = configure(source.dtype())?;
    let preparation_micros = preparation_started.elapsed().as_micros();
    source.with_native_execution(|file, _, runtime| {
        execute(
            source_uri,
            primitive_kind,
            policy,
            file,
            runtime,
            plan,
            preparation_micros,
        )
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn execute(
    source_uri: &DatasetUri,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file: &vortex::file::VortexFile,
    runtime: &impl vortex::io::runtime::BlockingRuntime,
    mut plan: LocalVortexScanPlan,
    preparation_micros: u128,
) -> Result<LocalVortexScan> {
    let control_plane_started = Instant::now();
    let mut control_plane_micros = preparation_micros;
    let mut evidence_collection_micros = 0_u128;
    let source_row_count = file.row_count();
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex source-order limit must be >= 1".to_string(),
        ));
    }
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
    if let Some(filter) = plan.filter.as_ref() {
        let metadata_pruned = file.can_prune(filter).map_err(vortex_error)?;
        embedded_layout.mark_pruning_consulted(metadata_pruned);
        if metadata_pruned {
            let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
            let reader_splits = Vec::new();
            let evidence_started = Instant::now();
            let reader_generated_prepared_batch_report =
                plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits);
            evidence_collection_micros =
                evidence_collection_micros.saturating_add(evidence_started.elapsed().as_micros());
            control_plane_micros =
                control_plane_micros.saturating_add(control_plane_started.elapsed().as_micros());
            return Ok(LocalVortexScan {
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
            });
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
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    let mut residual_predicate_materialized = false;
    for chunk in scan.into_array_iter(runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
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
        encoded_kernel_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            source_uri,
            &split.split_ref,
            &chunk,
        )?);
        reader_splits.push(split);
        evidence_collection_micros =
            evidence_collection_micros.saturating_add(evidence_started.elapsed().as_micros());
        let chunk_result_rows = if let Some(predicate) = residual_evaluator.as_ref() {
            if let Some(selected) =
                predicate.fast_count_matches_in_chunk(&chunk, &plan.projected_columns)?
            {
                selected
            } else {
                residual_predicate_materialized = true;
                let columns = row_export_columns_from_chunk(&chunk, &plan.projected_columns)?;
                let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
                let mut selected = 0usize;
                for row_index in 0..materialized_rows {
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
    let reader_generated_prepared_batch_report = if encoded_kernel_inputs.is_empty() {
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits)
    } else {
        plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits,
            &encoded_kernel_inputs,
        )
    };
    evidence_collection_micros =
        evidence_collection_micros.saturating_add(evidence_started.elapsed().as_micros());
    Ok(LocalVortexScan {
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
    })
}

#[cfg(test)]
#[path = "local_primitive_prepared_scan_tests.rs"]
mod tests;
