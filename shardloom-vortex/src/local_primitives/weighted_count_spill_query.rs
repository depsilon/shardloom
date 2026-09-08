//! Source-bound weighted COUNT query adapter.
//! Return the owner through `PreparedVortexSource::with_native_execution` so the
//! final generation check precedes public report/certificate construction.

use super::{LocalVortexAggregateScan, VortexLocalPrimitiveStateBudgetReport};
use super::{
    LocalVortexScan, ResidualPredicateMaterialization, UniversalInputSource,
    VortexLocalPrimitiveEmbeddedLayoutReport, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveRequest, VortexReaderBackedSplitEvidence, bind_vortex_scan_expr,
    integer_key_json_value, plan_vortex_reader_generated_prepared_batch_envelopes,
    plan_vortex_reader_generated_prepared_batch_kernel_inputs, predicate_to_vortex_expr,
    projection_scan_plan, reader_generated_encoded_kernel_inputs_from_vortex_chunk,
    split_predicate_for_vortex_pushdown, vortex_error,
    weighted_count_spill_accumulator::{Accumulator, OwnedResult},
    weighted_count_spill_admission::{self, failed},
};
use crate::{VortexAggregateSpillPolicy, VortexWeightedCountSpillReport};
use shardloom_core::{DatasetUri, Result};
use shardloom_exec::live_memory::LiveMemoryPool;
use shardloom_plan::ProjectionRequest;
use std::sync::Arc;
use vortex::{file::VortexFile, io::runtime::BlockingRuntime, session::VortexSession};

#[cfg(test)]
thread_local! {
    static AFTER_FINISH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// The native result owner survives the caller's final source-generation check.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn execute(
    source_uri: &DatasetUri,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file: &VortexFile,
    session: &VortexSession,
    runtime: &impl BlockingRuntime,
    memory: &LiveMemoryPool,
) -> Result<(LocalVortexAggregateScan, Arc<OwnedResult>)> {
    let contract = weighted_count_spill_admission::admit(request, file.dtype())?;
    let aggregate = request
        .simple_aggregate
        .as_ref()
        .expect("admitted aggregate");
    let spill = aggregate.spill.as_ref().expect("admitted policy");
    let limit = contract.limit;
    let mut accumulator = Accumulator::new(spill, contract, memory, session)?;
    let (pushdown, residual) = request
        .predicate
        .as_ref()
        .map_or((None, None), |predicate| {
            split_predicate_for_vortex_pushdown(predicate, request.kind)
        });
    if residual.is_some() {
        return Err(failed("residual predicate is outside this family"));
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
            accumulator.push_source(&chunk, runtime)?;
            rows = rows
                .checked_add(chunk.len())
                .ok_or_else(|| failed("source rows exceed address space"))?;
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
    let (result_row_count, mut summary) = result_summary(&owner, request)?;
    owner.source_work.numeric.annotate(&mut summary)?;
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
    let scan = LocalVortexScan {
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
        embedded_derived_column_rewrites: Vec::new(),
    };
    let state_budget = state_budget(&owner, spill);
    #[cfg(test)]
    AFTER_FINISH.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    Ok((
        LocalVortexAggregateScan {
            scan,
            result_summary: summary,
            state_budget,
            restored_provider_background_workers: 0,
        },
        owner,
    ))
}

fn state_budget(
    owner: &OwnedResult,
    policy: &VortexAggregateSpillPolicy,
) -> VortexLocalPrimitiveStateBudgetReport {
    let e = owner.result.evidence;
    let mut budget = VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
        "grouped_count+complete_weighted_utf8_native_runs",
        vec![
            "bounded_complete_key_buffer",
            "native_weighted_runs",
            "global_eof_selection",
        ],
        vec![
            "owned_operator_reservation",
            "utf8_arena_bytes",
            "native_run_disk_bytes",
        ],
        e.groups,
        Some(e.groups),
        "query_reserved_operator_envelope;native_records_utf8_arena_run_metadata_conversion_merge_final_selection;source_provider_and_JSON_allocations_separate",
    );
    budget.state_budget_status = "admitted_owned_operator_envelope".into();
    budget.state_pressure_class = "reserved_within_operator_budget".into();
    budget.spill_policy = "explicit_caller_workspace_native_weighted_complete_key_count".into();
    budget.spill_supported = true;
    budget.spill_required = e.runs_written != 0;
    budget.spill_io_performed = e.runs_written != 0;
    budget.diagnostic_code = "none".into();
    budget.next_action = "none;generic aggregate and join spill remain unadmitted".into();
    let key_order = match owner.contract.order {
        super::weighted_count_spill::KeyOrder::Text => "utf8",
        super::weighted_count_spill::KeyOrder::IntegerText { .. } => "integer_utf8",
        super::weighted_count_spill::KeyOrder::TextInteger { .. } => "utf8_integer",
    };
    budget.native_weighted_count_spill = Some(VortexWeightedCountSpillReport {
        family: "weighted_complete_utf8_grouped_count".into(),
        key_order: key_order.into(),
        workspace: policy.workspace.clone(),
        quota_bytes: policy.quota_bytes,
        memory_bytes: policy.memory_bytes,
        peak_reserved_bytes: e.peak_reserved_bytes,
        peak_disk_bytes: e.peak_disk_bytes,
        runs_written: e.runs_written,
        runs_validated: e.runs_validated,
        merge_passes: e.merge_passes,
        source_rows: e.source_weight,
        source_records: e.source_records,
        initial_run_records: e.initial_run_records,
        native_records_written: e.native_records_written,
        native_bytes_written: e.native_bytes_written,
        groups: e.groups,
        buffer_capacity_records: e.buffer_rows,
        buffer_capacity_text_bytes: e.buffer_bytes,
        min_run_block_rows: e.block_rows,
        max_run_block_rows: e.max_block_rows,
        max_run_key_bytes: e.max_run_key_bytes,
        max_admitted_key_bytes: weighted_count_spill_admission::MAX_KEY_BYTES,
        merge_fan_in: 4,
        source_text_bytes_copied: e.source_text_bytes_copied,
        encoded_text_bytes_copied: e.encoded_text_bytes_copied,
        merge_head_text_bytes_copied: e.merge_head_text_bytes_copied,
        selection_text_bytes_copied: e.selection_text_bytes_copied,
        owned_cleanup_completed: true,
    });
    budget
}

pub(super) fn result_summary(
    owner: &OwnedResult,
    request: &VortexQueryPrimitiveRequest,
) -> Result<(usize, String)> {
    let mut rows = Vec::new();
    let contract = &owner.contract;
    owner.result.visit(contract.offset, |key, text, count| {
        if rows.len() == contract.limit {
            return Ok(());
        }
        let mut row = serde_json::Map::new();
        for (index, column) in contract.groups.iter().enumerate() {
            let value = if index == contract.text_index {
                text.into()
            } else {
                let key = key.ok_or_else(|| failed("final numeric key disappeared"))?;
                integer_key_json_value(key.bits, key.signed)
            };
            row.insert(column.clone(), value);
        }
        row.insert(contract.count_alias.clone(), count.into());
        rows.push(serde_json::Value::Object(row));
        Ok(())
    })?;
    let count = rows.len();
    let evidence = owner.result.evidence;
    let spill_json = serde_json::json!({
        "source_records": evidence.source_records, "source_weight": evidence.source_weight, "initial_run_records": evidence.initial_run_records,
        "native_records_written": evidence.native_records_written, "native_bytes_written": evidence.native_bytes_written,
        "runs_written": evidence.runs_written, "runs_validated": evidence.runs_validated, "merge_passes": evidence.merge_passes,
        "peak_disk_bytes": evidence.peak_disk_bytes, "peak_reserved_bytes": evidence.peak_reserved_bytes,
        "buffer_rows": evidence.buffer_rows, "buffer_bytes": evidence.buffer_bytes, "min_run_block_rows": evidence.block_rows,
        "max_run_block_rows": evidence.max_block_rows, "max_run_key_bytes": evidence.max_run_key_bytes,
        "max_admitted_key_bytes": weighted_count_spill_admission::MAX_KEY_BYTES,
        "source_text_bytes_copied": evidence.source_text_bytes_copied, "encoded_text_bytes_copied": evidence.encoded_text_bytes_copied,
        "merge_head_text_bytes_copied": evidence.merge_head_text_bytes_copied, "selection_text_bytes_copied": evidence.selection_text_bytes_copied,
        "source_batches": owner.source_work.source_batches, "dictionary_batches": owner.source_work.dictionary_batches,
        "drained_epochs": owner.source_work.drained_epochs, "committed_weight": owner.source_work.committed_weight, "deferred_weight": owner.source_work.deferred_weight,
        "utf8_native_value_bytes": owner.source_work.utf8_native_value_bytes, "owned_cleanup_completed": true,
    });
    let summary = serde_json::json!({
        "rows": count, "values": rows, "group_by": contract.groups.join(","), "functions": "count",
        "aggregate_update_strategy": "complete_weighted_native_runs", "group_output_strategy": "bounded_heap_after_complete_weighted_key_merge",
        "candidate_groups": evidence.groups, "offset": contract.offset,
        "order_by": request.simple_aggregate.as_ref().expect("admitted request").order_by.iter().map(crate::VortexAggregateOrderExpr::summary).collect::<Vec<_>>().join(","),
        "group_key_storage": "complete_native_utf8_and_optional_exact_integer_bits", "group_key_comparison_strategy": "complete_declared_key_order",
        "weighted_count_final_reserved_bytes": owner.reserved_bytes(), "weighted_count_selected_reserved_bytes": owner.selected_reserved_bytes(),
        "spill_state": if evidence.runs_written == 0 { "admitted_no_spill_needed" } else { "native_runs_cleaned" },
        "weighted_count_spill": spill_json,
        "weighted_count_spill_scope": "one_retained_source_generation;one_runtime;one_run_registry;all_positive_weights_before_global_selection;full_query_reserved_operator_envelope_through_native_result;source_provider_and_JSON_allocations_separate;not_RSS;blocking_IO_not_synchronously_interruptible",
    });
    Ok((count, summary.to_string()))
}

#[cfg(test)]
#[path = "weighted_count_spill_query_tests.rs"]
mod tests;
