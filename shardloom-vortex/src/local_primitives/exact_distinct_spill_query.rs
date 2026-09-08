//! Exact aggregate spill over one held source generation. The returned native
//! owner stays alive until the enclosing prepared-source validation succeeds.

use super::super::{
    GroupedAggregateStates, LocalVortexAggregateScan, LocalVortexScan,
    ResidualPredicateMaterialization, UniversalInputSource,
    VortexLocalPrimitiveEmbeddedLayoutReport, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveRequest, VortexReaderBackedSplitEvidence,
    annotate_simple_aggregate_layout_correlation_summary,
    annotate_simple_aggregate_rewrite_summary, bind_vortex_scan_expr,
    native_numeric_accessor::NativeNumericAccessorWork,
    plan_vortex_reader_generated_prepared_batch_envelopes,
    plan_vortex_reader_generated_prepared_batch_kernel_inputs, predicate_to_vortex_expr,
    projection_scan_plan, reader_generated_encoded_kernel_inputs_from_vortex_chunk,
    required_simple_aggregate, rewrite_simple_aggregate_for_embedded_derived_columns,
    split_predicate_for_vortex_pushdown, vortex_error,
};
use super::{
    spill_accumulator::{OwnedSpillResult, SpillAccumulator},
    workers,
};
use shardloom_core::{DatasetUri, Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;
use shardloom_plan::ProjectionRequest;
use std::sync::Arc;
use vortex::{file::VortexFile, io::runtime::BlockingRuntime, session::VortexSession};

#[cfg(test)]
thread_local! {
    static AFTER_FINISH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One held-source attempt; all owners unwind together.
pub(in super::super) fn execute(
    source_uri: &DatasetUri,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file: &VortexFile,
    session: &VortexSession,
    runtime: &impl BlockingRuntime,
    memory: &LiveMemoryPool,
) -> Result<(LocalVortexAggregateScan, Arc<OwnedSpillResult>)> {
    if !workers::request_may_be_admitted(request)
        || !workers::request_schema_may_be_admitted(request, file.dtype())
    {
        return Err(failed(
            "requires nonnullable identity integer group/value columns, native pushdown-only predicate, and bounded count-descending output",
        ));
    }
    let aggregate_plan = rewrite_simple_aggregate_for_embedded_derived_columns(
        file.dtype(),
        required_simple_aggregate(request)?,
        request.predicate.as_ref(),
    )?;
    let aggregate = &aggregate_plan.aggregate;
    let spill_policy = aggregate
        .spill
        .as_ref()
        .ok_or_else(|| failed("policy is absent"))?;
    let limit = request
        .source_order_limit
        .filter(|limit| *limit != 0)
        .ok_or_else(|| failed("requires positive output limit"))?;
    let retained = aggregate
        .offset
        .checked_add(limit)
        .ok_or_else(|| failed("output offset plus limit overflowed"))?;
    let columns = [
        aggregate.group_by[0].as_str().to_owned(),
        aggregate.measures[0]
            .column
            .as_ref()
            .ok_or_else(|| failed("distinct column is absent"))?
            .as_str()
            .to_owned(),
    ];
    let fields = file
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| failed("requires struct source"))?;
    let dtypes = [
        fields
            .field(&columns[0])
            .ok_or_else(|| failed("group column is absent"))?,
        fields
            .field(&columns[1])
            .ok_or_else(|| failed("distinct column is absent"))?,
    ];
    let mut accumulator =
        SpillAccumulator::new(spill_policy, columns, dtypes, retained, memory, session)?;
    let (pushdown, residual) = aggregate_plan
        .predicate
        .as_ref()
        .map_or((None, None), |predicate| {
            split_predicate_for_vortex_pushdown(predicate, request.kind)
        });
    if residual.is_some() {
        return Err(failed("residual predicate is not admitted"));
    }
    let mut plan = projection_scan_plan(
        file.dtype(),
        &ProjectionRequest::columns(aggregate.projected_columns()),
        request.kind,
    )?;
    if let Some(predicate) = pushdown.as_ref() {
        plan.filter = Some(predicate_to_vortex_expr(
            predicate,
            file.dtype(),
            request.kind,
        )?);
    }
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let mut embedded_layout = VortexLocalPrimitiveEmbeddedLayoutReport::from_file(
        file,
        request.kind,
        filter_pushdown_applied,
        projection_pushdown_applied,
    );
    if let Some(filter) = plan.filter.as_ref() {
        embedded_layout.mark_pruning_consulted(file.can_prune(filter).map_err(vortex_error)?);
    }
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        aggregate,
        Some(limit),
        &plan.projected_columns,
        false,
        false,
        policy.resource_envelope(),
    )?;
    let mut numeric_work = NativeNumericAccessorWork::default();
    let mut reader_splits = Vec::new();
    let mut encoded_inputs = Vec::new();
    let mut rows = 0_usize;
    let mut max_chunk_rows = 0;
    if !embedded_layout.metadata_pruned_entire_input {
        let mut scan = file.scan().map_err(vortex_error)?;
        if let Some(filter) = plan.filter {
            scan = scan.with_filter(bind_vortex_scan_expr(file, &filter)?);
        }
        if let Some(projection) = plan.projection {
            scan = scan.with_projection(bind_vortex_scan_expr(file, &projection)?);
        }
        scan = scan.with_concurrency(policy.scan_concurrency_per_worker());
        for chunk in scan.into_array_iter(runtime).map_err(vortex_error)? {
            let chunk = chunk.map_err(vortex_error)?;
            numeric_work.add(&accumulator.push(&chunk, runtime)?)?;
            rows = rows
                .checked_add(chunk.len())
                .ok_or_else(|| failed("input rows overflowed"))?;
            max_chunk_rows = max_chunk_rows.max(chunk.len());
            let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
                source_uri.clone(),
                reader_splits.len(),
                chunk.len(),
                chunk.dtype().to_string(),
                chunk.encoding_id().to_string(),
                chunk.nchildren(),
                chunk.nbuffers(),
            )?;
            encoded_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
                source_uri,
                &split.split_ref,
                &chunk,
            )?);
            reader_splits.push(split);
        }
    }
    let owner = Arc::new(accumulator.finish(runtime)?);
    states.finalized_distinct_counts =
        Some(workers::ExactDistinctResult::from_spill(Arc::clone(&owner)));
    let (result_row_count, mut result_summary) =
        states.result_row_count_and_summary(Some(limit))?;
    let mut state_budget = states.state_budget_report(aggregate, rows, result_row_count)?;
    numeric_work.annotate(&mut result_summary)?;
    annotate_simple_aggregate_rewrite_summary(&mut result_summary, &aggregate_plan)?;
    annotate_simple_aggregate_layout_correlation_summary(&mut result_summary, &embedded_layout)?;
    let evidence = owner.result.evidence;
    state_budget.state_family =
        "grouped_count_distinct+complete_native_integer_pair_runs+finalized_distinct_counts".into();
    state_budget.state_budget_status = "admitted_owned_operator_envelope".into();
    state_budget.spill_policy = "explicit_caller_workspace_native_exact_integer_distinct".into();
    state_budget.spill_supported = true;
    state_budget.spill_required = evidence.runs_written != 0;
    state_budget.spill_io_performed = evidence.runs_written != 0;
    state_budget.fail_closed_if_spill_required = true;
    state_budget.budget_scope = "query_reserved_operator_envelope;native_run_metadata_conversion_merge_final_selection;source_provider_and_JSON_allocations_separate".into();
    state_budget.diagnostic_code = "none".into();
    state_budget.next_action =
        "none;generic aggregate and join spill families remain unadmitted".into();
    state_budget.native_aggregate_spill = Some(crate::VortexAggregateSpillReport {
        family: "exact_integer_grouped_count_distinct".into(),
        workspace: spill_policy.workspace.clone(),
        quota_bytes: spill_policy.quota_bytes,
        memory_bytes: spill_policy.memory_bytes,
        peak_reserved_bytes: evidence.peak_reserved_bytes,
        peak_disk_bytes: evidence.peak_disk_bytes,
        runs_written: evidence.runs_written,
        runs_validated: evidence.runs_validated,
        merge_passes: evidence.merge_passes,
        source_rows: evidence.rows,
        complete_pairs: evidence.complete_pairs,
        groups: evidence.groups,
        buffer_capacity_pairs: evidence.buffer_capacity_pairs,
        run_block_rows: 1024,
        merge_fan_in: 4,
        owned_cleanup_completed: true,
    });
    let mut summary: serde_json::Value =
        serde_json::from_str(&result_summary).map_err(|error| failed(&error.to_string()))?;
    summary["aggregate_spill_runs_written"] = evidence.runs_written.into();
    summary["aggregate_spill_runs_validated"] = evidence.runs_validated.into();
    summary["aggregate_spill_merge_passes"] = evidence.merge_passes.into();
    summary["aggregate_spill_source_rows"] = evidence.rows.into();
    summary["aggregate_spill_complete_pairs"] = evidence.complete_pairs.into();
    summary["aggregate_spill_peak_reserved_bytes"] = evidence.peak_reserved_bytes.into();
    summary["aggregate_spill_peak_disk_bytes"] = evidence.peak_disk_bytes.into();
    summary["aggregate_spill_owned_cleanup_completed"] = true.into();
    summary["aggregate_spill_scope"] = "one_retained_source_generation;one_runtime;one_run_registry;complete_pairs_then_global_EOF_order;declared_operator_envelope_reserved_from_query_pool_through_native_result;source_provider_and_JSON_allocations_separate;not_RSS;blocking_IO_not_synchronously_interruptible".into();
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let prepared_report = if encoded_inputs.is_empty() {
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits)
    } else {
        plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits,
            &encoded_inputs,
        )
    };
    #[cfg(test)]
    AFTER_FINISH.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    Ok((
        LocalVortexAggregateScan {
            scan: LocalVortexScan {
                source_row_count: file.row_count(),
                result_row_count,
                pre_limit_result_row_count: rows,
                arrays_read_count: reader_splits.len(),
                reader_splits,
                reader_generated_prepared_batch_report: prepared_report,
                control_plane_micros: 0,
                evidence_collection_micros: 0,
                max_chunk_rows,
                resource_envelope: policy.resource_envelope(),
                max_parallelism_requested: policy.max_parallelism,
                scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
                projected_columns: aggregate.output_columns(),
                filter_pushdown_applied,
                projection_pushdown_applied,
                residual_predicate_materialization: ResidualPredicateMaterialization::from_flags(
                    false, false,
                ),
                source_order_limit: Some(limit),
                embedded_layout,
                embedded_derived_column_rewrites: aggregate_plan.rewritten_columns,
            },
            result_summary: summary.to_string(),
            state_budget,
            restored_provider_background_workers: 0,
        },
        owner,
    ))
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native exact integer COUNT DISTINCT spill {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "exact_distinct_spill_public_tests.rs"]
mod tests;
