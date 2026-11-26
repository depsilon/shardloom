// SPDX-License-Identifier: Apache-2.0
//! Bounded worker-pool range execution for multi-row-group demanded Parquet reads.

use super::{
    Consumption, Execution, OwnedSourceBatch, Source, SourceState, checked_add, failed,
    verify_released,
};
use crate::local_primitives::native_relational_probe_filter::ProbeRuntimeFilter;
use crate::resident_session::{
    NativeExecutionContext, ResidentVortexSession, WorkerExecutionDelta,
};
use crate::universal_format_io::demanded::{ColumnarReadReport, PreparedColumnarSource};
use crate::vortex_ingest::ArrowCopyReasons;
use shardloom_core::{PredicateExpr, Result};
use shardloom_exec::{
    compute_pool::{CancellationToken, ComputePool, ComputeTask, WorkerContext},
    live_memory::{Budgeted, MemoryLease},
};
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::AtomicBool,
    mpsc::{self, Receiver},
};

const TASK_RESERVATION_BYTES: u64 = 4096;
const BOUNDED_CHANNEL_DEPTH: usize = 2;
const MIN_PARALLEL_ROWS: u64 = 16_384;

pub(super) struct ParallelAdmission {
    pub(super) pool: Arc<ComputePool>,
    pub(super) workers: usize,
    pub(super) ranges: Vec<std::ops::Range<usize>>,
}

#[derive(Default)]
pub(in crate::local_primitives) struct WorkerSourceStats {
    pub(in crate::local_primitives) batches: u64,
    pub(in crate::local_primitives) rows: u64,
    pub(in crate::local_primitives) input_logical_bytes: u64,
    pub(in crate::local_primitives) intake_payload_bytes_copied: u64,
    pub(in crate::local_primitives) intake_columnar_buffer_bytes_copied: u64,
    pub(in crate::local_primitives) intake_columnar_copy_reasons: ArrowCopyReasons,
    pub(in crate::local_primitives) intake_columnar_buffer_bytes_transferred: u64,
    pub(in crate::local_primitives) intake_columnar_buffer_regions_transferred: u64,
    pub(in crate::local_primitives) intake_columnar_allocation_bytes_transferred: u64,
    pub(in crate::local_primitives) max_batch_rows: usize,
    pub(in crate::local_primitives) max_batch_logical_bytes: u64,
}

impl WorkerSourceStats {
    pub(in crate::local_primitives) fn observe(
        &mut self,
        source: &crate::resident_memory_source::ResidentMemorySource,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let rows = source.row_count();
        let bytes = source.input_logical_bytes() as u64;
        checked_add(&mut self.batches, 1)?;
        checked_add(&mut self.rows, rows as u64)?;
        checked_add(&mut self.input_logical_bytes, bytes)?;
        checked_add(
            &mut self.intake_payload_bytes_copied,
            source.intake_payload_bytes_copied(),
        )?;
        checked_add(
            &mut self.intake_columnar_buffer_bytes_copied,
            source.intake_columnar_buffer_bytes_copied(),
        )?;
        self.intake_columnar_copy_reasons
            .merge(source.intake_columnar_copy_reasons());
        checked_add(
            &mut self.intake_columnar_buffer_bytes_transferred,
            source.intake_columnar_buffer_bytes_transferred(),
        )?;
        checked_add(
            &mut self.intake_columnar_buffer_regions_transferred,
            source.intake_columnar_buffer_regions_transferred(),
        )?;
        checked_add(
            &mut self.intake_columnar_allocation_bytes_transferred,
            source.intake_columnar_allocation_bytes_transferred(),
        )?;
        let transferred = source.intake_columnar_buffer_bytes_transferred();
        let copied = source.intake_columnar_buffer_bytes_copied();
        if transferred > 0 {
            context.record_capability_decision(
                shardloom_core::native_io_decisions::NativeIoCapability::RepresentationRetention,
                shardloom_core::native_io_decisions::NativeIoDecisionOutcome::Executed,
                shardloom_core::native_io_decisions::NativeIoDecisionReason::OwnedRepresentation,
                shardloom_core::native_io_decisions::NativeIoObservedWork {
                    transferred_buffer_bytes: Some(transferred),
                    ..shardloom_core::native_io_decisions::NativeIoObservedWork::default()
                },
            )?;
        }
        if copied > 0 {
            context.record_capability_decision(
                shardloom_core::native_io_decisions::NativeIoCapability::RepresentationRetention,
                shardloom_core::native_io_decisions::NativeIoDecisionOutcome::Declined,
                shardloom_core::native_io_decisions::NativeIoDecisionReason::RequiredRepresentationCopy,
                shardloom_core::native_io_decisions::NativeIoObservedWork {
                    copied_buffer_bytes: Some(copied),
                    ..shardloom_core::native_io_decisions::NativeIoObservedWork::default()
                },
            )?;
        }
        self.max_batch_rows = self.max_batch_rows.max(rows);
        self.max_batch_logical_bytes = self.max_batch_logical_bytes.max(bytes);
        Ok(())
    }

    fn merge_into(
        &self,
        report: &mut super::ExecutedVortexBatchInput,
        next_batch: &std::cell::Cell<u64>,
    ) -> Result<()> {
        checked_add(&mut report.payload_batches, self.batches)?;
        checked_add(&mut report.rows, self.rows)?;
        checked_add(&mut report.input_logical_bytes, self.input_logical_bytes)?;
        checked_add(
            &mut report.intake_payload_bytes_copied,
            self.intake_payload_bytes_copied,
        )?;
        checked_add(
            &mut report.intake_columnar_buffer_bytes_copied,
            self.intake_columnar_buffer_bytes_copied,
        )?;
        report
            .intake_columnar_copy_reasons
            .merge(&self.intake_columnar_copy_reasons);
        checked_add(
            &mut report.intake_columnar_buffer_bytes_transferred,
            self.intake_columnar_buffer_bytes_transferred,
        )?;
        checked_add(
            &mut report.intake_columnar_buffer_regions_transferred,
            self.intake_columnar_buffer_regions_transferred,
        )?;
        checked_add(
            &mut report.intake_columnar_allocation_bytes_transferred,
            self.intake_columnar_allocation_bytes_transferred,
        )?;
        report.max_batch_rows = report.max_batch_rows.max(self.max_batch_rows);
        report.max_retained_input_batches = report.max_retained_input_batches.max(1);
        report.max_retained_input_logical_bytes = report
            .max_retained_input_logical_bytes
            .max(self.max_batch_logical_bytes);
        next_batch.set(
            next_batch
                .get()
                .checked_add(self.batches)
                .ok_or_else(|| failed("input batch ordinal overflow"))?,
        );
        Ok(())
    }
}

pub(super) fn admit_parallel_parquet(
    prepared: &PreparedColumnarSource,
    usage: super::Usage,
    consumption: Consumption,
    session: &ResidentVortexSession,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ParallelAdmission>> {
    admit_parallel_parquet_with_cap(prepared, usage, consumption, session, 2, context)
}

fn admit_parallel_parquet_with_cap(
    prepared: &PreparedColumnarSource,
    usage: super::Usage,
    consumption: Consumption,
    session: &ResidentVortexSession,
    max_ranges_per_worker: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ParallelAdmission>> {
    if consumption != Consumption::Query
        || !usage.admits_independent_read()
        || !prepared.is_parquet()
        || prepared.max_rows().is_some()
        || context.cpu_lanes() <= 1
    {
        return Ok(None);
    }
    let row_groups = prepared.row_group_count();
    if row_groups < 2 || prepared.total_selected_rows() < MIN_PARALLEL_ROWS {
        return Ok(None);
    }
    let Some(pool) = session.shared_compute_pool(context.cpu_lanes())? else {
        return Ok(None);
    };
    let snap = context.memory().snapshot();
    let available = snap.limit_bytes.saturating_sub(snap.reserved_bytes);
    let rg_estimate = prepared
        .max_selected_row_group_workspace_estimate()
        .max(64 * 1024);
    let per_worker = rg_estimate.saturating_mul(4);
    let max_by_memory = usize::try_from(available / per_worker).unwrap_or(usize::MAX);
    let workers = context.cpu_lanes().min(row_groups).min(max_by_memory);
    if workers < 2 {
        return Ok(None);
    }
    let range_count = workers
        .saturating_mul(max_ranges_per_worker.max(1))
        .min(row_groups);
    let ranges = partition_ranges(row_groups, range_count);
    Ok(Some(ParallelAdmission {
        pool,
        workers,
        ranges,
    }))
}

fn partition_ranges(total: usize, count: usize) -> Vec<std::ops::Range<usize>> {
    let base = total / count;
    let rem = total % count;
    let mut ranges = Vec::with_capacity(count);
    let mut start = 0;
    for index in 0..count {
        let len = base + usize::from(index < rem);
        let end = start + len;
        ranges.push(start..end);
        start = end;
    }
    ranges
}

type StreamTaskResult = (ColumnarReadReport, WorkerExecutionDelta);

struct ActiveStreamTask {
    rx: Receiver<Result<OwnedSourceBatch>>,
    task: ComputeTask<StreamTaskResult>,
}

fn teardown_stream_tasks(
    child_token: &CancellationToken,
    current_rx: Option<Receiver<Result<OwnedSourceBatch>>>,
    current_task: Option<ComputeTask<StreamTaskResult>>,
    mut pending: VecDeque<ActiveStreamTask>,
) {
    child_token.cancel();
    drop(current_rx);
    for active in &mut pending {
        // Replace receiver with closed dummy receiver so all senders unblock before join!
        let (_, dummy_rx) = mpsc::sync_channel(0);
        let real_rx = std::mem::replace(&mut active.rx, dummy_rx);
        drop(real_rx);
    }
    if let Some(task) = current_task {
        let _ = task.join();
    }
    while let Some(active) = pending.pop_front() {
        drop(active.rx);
        let _ = active.task.join();
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn run_streaming_ranges(
    execution: &Execution<'_, '_>,
    index: usize,
    declaration: &Source,
    state: &SourceState,
    prepared: &Arc<PreparedColumnarSource>,
    admission: ParallelAdmission,
    mandatory_predicate: Option<&(PredicateExpr, bool)>,
    runtime_predicate: Option<&PredicateExpr>,
    empty_short_circuit: bool,
    probe_filters: &Arc<[ProbeRuntimeFilter]>,
    residual_present: bool,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(super::SourceFilterSatisfaction, &super::ArrayRef) -> Result<()>,
) -> Result<()> {
    let ParallelAdmission {
        pool,
        workers,
        ranges,
    } = admission;
    let total_ranges = ranges.len();
    let child_flag = Arc::new(AtomicBool::new(false));
    let child_token =
        CancellationToken::from_shared_flag_with_parent(child_flag, context.cancellation());
    let scope = execution
        .prepared
        .session
        .worker_execution_scope(context, child_token.clone())?;

    let submit_range = |range: std::ops::Range<usize>| -> Result<ActiveStreamTask> {
        let (tx, rx) = mpsc::sync_channel::<Result<OwnedSourceBatch>>(BOUNDED_CHANNEL_DEPTH);
        let lease = context.memory().reserve(TASK_RESERVATION_BYTES)?;
        let range_scope = scope.clone();
        let range_prepared = Arc::clone(prepared);
        let range_session = execution.prepared.session.clone();
        let range_mandatory = mandatory_predicate.cloned();
        let range_pred = runtime_predicate.cloned();
        let range_probe_filters = Arc::clone(probe_filters);
        let submit_cancel = child_token.clone();
        let task = pool.submit(
            Budgeted::new(
                move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                    range_scope.with_context(|wctx| {
                        let mut reader = range_prepared.open_row_group_range_with_filters(
                            range,
                            range_mandatory,
                            range_pred,
                            empty_short_circuit,
                            range_probe_filters,
                            residual_present,
                            false,
                            wctx,
                        )?;
                        loop {
                            match reader.next_native_with_selection(&range_session, wctx) {
                                Ok(Some((source, selection, satisfaction))) => {
                                    let first_row = selection.first_row();
                                    let owned = OwnedSourceBatch {
                                        source,
                                        transport_bounds: false,
                                        first_row,
                                        filter_status: satisfaction.into(),
                                    };
                                    if tx.send(Ok(owned)).is_err() {
                                        return reader.report();
                                    }
                                }
                                Ok(None) => return reader.report(),
                                Err(error) => {
                                    let _ = tx.send(Err(error.clone()));
                                    return Err(error);
                                }
                            }
                        }
                    })
                },
                lease,
            ),
            submit_cancel,
        )?;
        Ok(ActiveStreamTask { rx, task })
    };

    let mut active_tasks: VecDeque<ActiveStreamTask> = VecDeque::new();
    let mut range_iter = ranges.into_iter();
    for _ in 0..workers {
        let Some(range) = range_iter.next() else {
            break;
        };
        match submit_range(range) {
            Ok(task) => active_tasks.push_back(task),
            Err(error) => {
                teardown_stream_tasks(&child_token, None, None, active_tasks);
                return Err(error);
            }
        }
    }

    let mut merged_report: Option<ColumnarReadReport> = None;
    let mut min_first_row = 0_u64;
    while let Some(active) = active_tasks.pop_front() {
        let ActiveStreamTask { rx, task } = active;
        if let Some(next_range) = range_iter.next() {
            match submit_range(next_range) {
                Ok(next_task) => active_tasks.push_back(next_task),
                Err(error) => {
                    teardown_stream_tasks(&child_token, Some(rx), Some(task), active_tasks);
                    return Err(error);
                }
            }
        }
        loop {
            if let Err(error) = context.check_cancelled() {
                teardown_stream_tasks(&child_token, Some(rx), Some(task), active_tasks);
                return Err(error);
            }
            let Ok(owner) = execution.original_owner.try_borrow_mut() else {
                teardown_stream_tasks(&child_token, Some(rx), Some(task), active_tasks);
                return Err(failed(
                    "nested input demand requires releasing the original source batch first",
                ));
            };
            match rx.recv() {
                Ok(Ok(OwnedSourceBatch {
                    source,
                    transport_bounds,
                    first_row,
                    filter_status,
                })) => {
                    let step: Result<()> = (|| {
                        if first_row < min_first_row {
                            return Err(failed(
                                "parallel range stream violated monotonic source row coordinates",
                            ));
                        }
                        min_first_row = first_row;
                        execution.validate_input(declaration, &source, transport_bounds)?;
                        let released = source.batch_release_witness()?;
                        execution.record_input(&source, context)?;
                        let array = source.batch_array(context)?;
                        let res = consume(filter_status, array);
                        drop(source);
                        res?;
                        verify_released(&released)?;
                        drop(owner);
                        state.next_batch.set(
                            state
                                .next_batch
                                .get()
                                .checked_add(1)
                                .ok_or_else(|| failed("input batch ordinal overflow"))?,
                        );
                        Ok(())
                    })();
                    if let Err(error) = step {
                        teardown_stream_tasks(&child_token, Some(rx), Some(task), active_tasks);
                        return Err(error);
                    }
                }
                Ok(Err(error)) => {
                    teardown_stream_tasks(&child_token, Some(rx), Some(task), active_tasks);
                    return Err(error);
                }
                Err(_) => break,
            }
        }
        drop(rx);
        match task.join() {
            Ok(done) => {
                let ((report, delta), lease) = done.into_parts();
                drop(lease);
                if let Err(error) = context.merge_worker_delta(delta) {
                    teardown_stream_tasks(&child_token, None, None, active_tasks);
                    return Err(error);
                }
                match &mut merged_report {
                    Some(existing) => {
                        if let Err(error) = existing.merge_range(&report) {
                            teardown_stream_tasks(&child_token, None, None, active_tasks);
                            return Err(error);
                        }
                    }
                    None => merged_report = Some(report),
                }
            }
            Err(error) => {
                teardown_stream_tasks(&child_token, None, None, active_tasks);
                return Err(error);
            }
        }
    }

    let report = merged_report.ok_or_else(|| failed("parallel range scan yielded no report"))?;
    let _ = index;
    prepared.complete_region_selection(context)?;
    {
        let mut execution_report = execution.report.borrow_mut();
        execution_report.parallel_range_workers =
            execution_report.parallel_range_workers.max(workers as u64);
        execution_report.source_workers = execution_report.source_workers.max(workers as u64);
        execution_report.operator_workers = execution_report.operator_workers.max(1);
        checked_add(
            &mut execution_report.parallel_range_tasks,
            total_ranges as u64,
        )?;
    }
    execution.record_columnar_input(report)?;
    execution.finish_producer(state, None, Consumption::Query)
}

type RangeComputeTask<T> = ComputeTask<(
    (T, ColumnarReadReport, WorkerSourceStats),
    WorkerExecutionDelta,
)>;

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(in crate::local_primitives) fn run_parallel_range_tasks<T, W, M>(
    execution: &Execution<'_, '_>,
    index: usize,
    worker_fn: W,
    mut merge_fn: M,
    persistent_states_count: u64,
    context: &NativeExecutionContext<'_>,
) -> Result<bool>
where
    T: Send + 'static,
    W: Fn(
            usize,
            std::ops::Range<usize>,
            Arc<PreparedColumnarSource>,
            ResidentVortexSession,
            &NativeExecutionContext<'_>,
        ) -> Result<(T, ColumnarReadReport, WorkerSourceStats)>
        + Send
        + Sync
        + 'static,
    M: FnMut(usize, T) -> Result<()>,
{
    let Some(state) = execution.states.values.get(index) else {
        return Ok(false);
    };
    let Some(declaration) = execution.prepared.batch_sources.get(index) else {
        return Ok(false);
    };
    let Some(prepared_parquet) = &declaration.local else {
        return Ok(false);
    };
    if state.started.get() {
        return Ok(false);
    }
    let Some(admission) = admit_parallel_parquet_with_cap(
        prepared_parquet,
        state.usage.get(),
        Consumption::Query,
        &execution.prepared.session,
        1,
        context,
    )?
    else {
        return Ok(false);
    };
    state.started.set(true);

    let ParallelAdmission {
        pool,
        workers,
        ranges,
    } = admission;
    let total_ranges = ranges.len();
    let child_flag = Arc::new(AtomicBool::new(false));
    let child_token =
        CancellationToken::from_shared_flag_with_parent(child_flag, context.cancellation());
    let scope = execution
        .prepared
        .session
        .worker_execution_scope(context, child_token.clone())?;
    let worker_fn = Arc::new(worker_fn);

    let submit_one =
        |range_idx: usize, range: std::ops::Range<usize>| -> Result<(usize, RangeComputeTask<T>)> {
            let lease = context.memory().reserve(TASK_RESERVATION_BYTES)?;
            let range_scope = scope.clone();
            let range_prepared = Arc::clone(prepared_parquet);
            let range_session = execution.prepared.session.clone();
            let range_fn = Arc::clone(&worker_fn);
            let cancel = child_token.clone();
            let task = pool.submit(
                Budgeted::new(
                    move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                        range_scope.with_context(|wctx| {
                            range_fn(range_idx, range, range_prepared, range_session, wctx)
                        })
                    },
                    lease,
                ),
                cancel,
            )?;
            Ok((range_idx, task))
        };

    let mut active = VecDeque::new();
    let mut next_idx = 0_usize;
    let mut range_iter = ranges.into_iter().enumerate();
    for _ in 0..workers {
        if let Some((r_idx, r)) = range_iter.next() {
            match submit_one(r_idx, r) {
                Ok(task) => active.push_back(task),
                Err(error) => {
                    child_token.cancel();
                    while let Some((_, t)) = active.pop_front() {
                        let _ = t.join();
                    }
                    return Err(error);
                }
            }
        }
    }

    let mut merged_report: Option<ColumnarReadReport> = None;
    let mut merged_stats = WorkerSourceStats::default();
    let mut total_batches_consumed = 0_u64;
    while let Some((r_idx, task)) = active.pop_front() {
        if let Some((n_idx, n_r)) = range_iter.next() {
            match submit_one(n_idx, n_r) {
                Ok(next_task) => active.push_back(next_task),
                Err(error) => {
                    child_token.cancel();
                    let _ = task.join();
                    while let Some((_, t)) = active.pop_front() {
                        let _ = t.join();
                    }
                    return Err(error);
                }
            }
        }
        if let Err(error) = context.check_cancelled() {
            child_token.cancel();
            let _ = task.join();
            while let Some((_, t)) = active.pop_front() {
                let _ = t.join();
            }
            return Err(error);
        }
        let joined = match task.join() {
            Ok(done) => done,
            Err(error) => {
                child_token.cancel();
                while let Some((_, t)) = active.pop_front() {
                    let _ = t.join();
                }
                return Err(error);
            }
        };
        let (((value, rpt, stats), delta), lease) = joined.into_parts();
        drop(lease);
        let step: Result<()> = (|| {
            context.merge_worker_delta(delta)?;
            match &mut merged_report {
                Some(existing) => existing.merge_range(&rpt)?,
                None => merged_report = Some(rpt),
            }
            checked_add(&mut total_batches_consumed, stats.batches)?;
            checked_add(&mut merged_stats.batches, stats.batches)?;
            checked_add(&mut merged_stats.rows, stats.rows)?;
            checked_add(
                &mut merged_stats.input_logical_bytes,
                stats.input_logical_bytes,
            )?;
            checked_add(
                &mut merged_stats.intake_payload_bytes_copied,
                stats.intake_payload_bytes_copied,
            )?;
            checked_add(
                &mut merged_stats.intake_columnar_buffer_bytes_copied,
                stats.intake_columnar_buffer_bytes_copied,
            )?;
            merged_stats
                .intake_columnar_copy_reasons
                .merge(&stats.intake_columnar_copy_reasons);
            checked_add(
                &mut merged_stats.intake_columnar_buffer_bytes_transferred,
                stats.intake_columnar_buffer_bytes_transferred,
            )?;
            checked_add(
                &mut merged_stats.intake_columnar_buffer_regions_transferred,
                stats.intake_columnar_buffer_regions_transferred,
            )?;
            checked_add(
                &mut merged_stats.intake_columnar_allocation_bytes_transferred,
                stats.intake_columnar_allocation_bytes_transferred,
            )?;
            merged_stats.max_batch_rows = merged_stats.max_batch_rows.max(stats.max_batch_rows);
            merged_stats.max_batch_logical_bytes = merged_stats
                .max_batch_logical_bytes
                .max(stats.max_batch_logical_bytes);
            debug_assert_eq!(r_idx, next_idx);
            next_idx += 1;
            merge_fn(r_idx, value)
        })();
        if let Err(error) = step {
            child_token.cancel();
            while let Some((_, t)) = active.pop_front() {
                let _ = t.join();
            }
            return Err(error);
        }
    }

    let report =
        merged_report.ok_or_else(|| failed("parallel range task execution yielded no report"))?;
    prepared_parquet.complete_region_selection(context)?;
    {
        let mut execution_report = execution.report.borrow_mut();
        merged_stats.merge_into(&mut execution_report, &state.next_batch)?;
        execution_report.parallel_range_workers =
            execution_report.parallel_range_workers.max(workers as u64);
        execution_report.source_workers = execution_report.source_workers.max(workers as u64);
        execution_report.operator_workers =
            execution_report
                .operator_workers
                .max(if persistent_states_count > 0 {
                    workers as u64
                } else {
                    1
                });
        checked_add(
            &mut execution_report.parallel_range_tasks,
            total_ranges as u64,
        )?;
        checked_add(
            &mut execution_report.persistent_aggregate_states,
            if persistent_states_count > 0 {
                total_ranges as u64
            } else {
                0
            },
        )?;
        checked_add(
            &mut execution_report.aggregate_batches_consumed,
            total_batches_consumed,
        )?;
    }
    execution.record_columnar_input(report)?;
    execution.finish_producer(state, None, Consumption::Query)?;
    Ok(true)
}

#[allow(clippy::too_many_lines)]
pub(in crate::local_primitives::prepared_relational) fn run_parallel_parquet_aggregate(
    execution: &Execution<'_, '_>,
    plan: &Arc<super::super::scan_parallel::ParallelAggregatePlan>,
    context: &NativeExecutionContext<'_>,
    metrics: &super::Metrics<'_>,
) -> Result<Option<crate::local_primitives::native_relational_aggregate::Aggregate<'static>>> {
    use crate::local_primitives::native_relational_aggregate::Aggregate;
    use crate::native_work_sizing::{Owner, Rows};
    let Some(declaration) = execution.prepared.batch_sources.get(plan.source_index) else {
        return Ok(None);
    };
    let expected_dtype = declaration.schema.dtype().clone();
    let source_index = plan.source_index;
    let plan_for_worker = Arc::clone(plan);
    let mut merged_agg: Option<Aggregate<'static>> = None;
    let mut merged_scans = super::super::scan::ScanBatchCounts::default();
    let mut partial_groups_emitted = 0_u64;
    let mut partial_groups_merged = 0_u64;
    let executed = run_parallel_range_tasks(
        execution,
        source_index,
        move |_range_idx, range, prepared_parquet, range_session, wctx| {
            let mut reader = prepared_parquet.open_row_group_range_with_filters(
                range,
                plan_for_worker.mandatory_predicate.clone(),
                None,
                false,
                Arc::from([]),
                plan_for_worker.residual.is_some(),
                false,
                wctx,
            )?;
            let mut agg = Aggregate::new_shared(Arc::clone(&plan_for_worker.spec), wctx.memory())?;
            let mut sizing = Rows::from_fields(
                Owner::Scan,
                &plan_for_worker.scan_fields,
                plan_for_worker.scan_rows,
                2,
                wctx,
            )?;
            let mut stats = WorkerSourceStats::default();
            let mut counts = super::super::scan::ScanBatchCounts::default();
            while let Some((source, _selection, satisfaction)) =
                reader.next_native_with_selection(&range_session, wctx)?
            {
                if !source.belongs_to_session(&range_session) || source.dtype() != &expected_dtype {
                    return Err(failed(
                        "parallel range input changed its declared schema or session",
                    ));
                }
                let released = source.batch_release_witness()?;
                stats.observe(&source, wctx)?;
                let source_array = source.batch_array(wctx)?;
                let active_residual = if satisfaction.mandatory_predicate_satisfied {
                    None
                } else {
                    plan_for_worker.residual.as_ref()
                };
                let execute_source =
                    |source_array: &super::ArrayRef, range: std::ops::Range<usize>| {
                        if satisfaction.mandatory_predicate_satisfied {
                            plan_for_worker
                                .projection
                                .execute_batch_array_without_filter(source_array, range, wctx)
                        } else {
                            plan_for_worker.projection.execute_batch_array(
                                source_array,
                                range,
                                wctx,
                            )
                        }
                    };
                counts.scans_started = counts
                    .scans_started
                    .checked_add(1)
                    .ok_or_else(|| failed("scan counter overflow"))?;
                if source_array.is_empty() {
                    let projected = execute_source(source_array, 0..0)?;
                    super::super::scan::accept_scan_batch_counts(
                        &projected,
                        &plan_for_worker.columns,
                        active_residual,
                        &[],
                        &plan_for_worker.scan_fields,
                        wctx,
                        &mut counts,
                        plan_for_worker.batch_rows,
                        &mut |array| {
                            super::super::scan_parallel::apply_parallel_transforms(
                                &plan_for_worker.transforms,
                                0,
                                array,
                                wctx,
                                &mut |final_batch| {
                                    if plan_for_worker.count_partial {
                                        agg.consume_reordered_count(
                                            &final_batch,
                                            wctx,
                                            plan_for_worker.batch_rows,
                                        )
                                    } else {
                                        agg.consume(&final_batch, wctx, plan_for_worker.batch_rows)
                                    }
                                },
                            )
                        },
                    )?;
                } else {
                    counts.data_scans = counts
                        .data_scans
                        .checked_add(1)
                        .ok_or_else(|| failed("data scan counter overflow"))?;
                    let mut start = 0;
                    while start < source_array.len() {
                        let selected = sizing.select((source_array.len() - start) as u64, wctx)?;
                        let end = start + selected.rows;
                        let projected = execute_source(source_array, start..end)?;
                        super::super::scan::accept_scan_batch_counts(
                            &projected,
                            &plan_for_worker.columns,
                            active_residual,
                            &[],
                            &plan_for_worker.scan_fields,
                            wctx,
                            &mut counts,
                            plan_for_worker.batch_rows,
                            &mut |array| {
                                super::super::scan_parallel::apply_parallel_transforms(
                                    &plan_for_worker.transforms,
                                    0,
                                    array,
                                    wctx,
                                    &mut |final_batch| {
                                        if plan_for_worker.count_partial {
                                            agg.consume_reordered_count(
                                                &final_batch,
                                                wctx,
                                                plan_for_worker.batch_rows,
                                            )
                                        } else {
                                            agg.consume(
                                                &final_batch,
                                                wctx,
                                                plan_for_worker.batch_rows,
                                            )
                                        }
                                    },
                                )
                            },
                        )?;
                        sizing.complete(selected, None, wctx);
                        start = end;
                    }
                }
                drop(source);
                verify_released(&released)?;
            }
            let report = reader.report()?;
            Ok(((agg, counts), report, stats))
        },
        |_range_idx, (worker_agg, worker_counts)| {
            merged_scans.scan_rows = merged_scans
                .scan_rows
                .checked_add(worker_counts.scan_rows)
                .ok_or_else(|| failed("scan rows overflow"))?;
            merged_scans.scan_batches = merged_scans
                .scan_batches
                .checked_add(worker_counts.scan_batches)
                .ok_or_else(|| failed("scan batches overflow"))?;
            merged_scans.scans_started = merged_scans
                .scans_started
                .checked_add(worker_counts.scans_started)
                .ok_or_else(|| failed("scans started overflow"))?;
            merged_scans.data_scans = merged_scans
                .data_scans
                .checked_add(worker_counts.data_scans)
                .ok_or_else(|| failed("data scans overflow"))?;
            merged_scans.residual_batches = merged_scans
                .residual_batches
                .checked_add(worker_counts.residual_batches)
                .ok_or_else(|| failed("residual batches overflow"))?;
            checked_add(
                &mut partial_groups_emitted,
                worker_agg.partial_group_count(),
            )?;
            match &mut merged_agg {
                None => merged_agg = Some(worker_agg),
                Some(existing) => {
                    let merged = existing.merge_from(worker_agg, context, plan.batch_rows)?;
                    checked_add(&mut partial_groups_merged, merged)?;
                }
            }
            Ok(())
        },
        1,
        context,
    )?;
    if !executed {
        return Ok(None);
    }
    let final_groups = merged_agg
        .as_ref()
        .map_or(0, Aggregate::partial_group_count);
    {
        let mut execution_report = execution.report.borrow_mut();
        checked_add(
            &mut execution_report.aggregate_partial_groups_emitted,
            partial_groups_emitted,
        )?;
        checked_add(
            &mut execution_report.aggregate_partial_groups_merged,
            partial_groups_merged,
        )?;
        checked_add(&mut execution_report.aggregate_final_groups, final_groups)?;
    }
    merged_scans.merge_into(metrics)?;
    if plan.finite_float_predicate {
        super::super::add(&metrics.pipeline.finite_float_scans, 1)?;
    }
    Ok(merged_agg)
}

#[allow(clippy::type_complexity)]
fn admit_parallel_parquet_join_source<'a>(
    execution: &'a Execution<'_, '_>,
    source_index: usize,
    residual_is_none: bool,
    probe_filters: &[ProbeRuntimeFilter],
    context: &NativeExecutionContext<'_>,
) -> Result<
    Option<(
        &'a SourceState,
        &'a Arc<PreparedColumnarSource>,
        vortex::array::dtype::DType,
        ParallelAdmission,
    )>,
> {
    let Some(state) = execution.states.values.get(source_index) else {
        return Ok(None);
    };
    let Some(declaration) = execution.prepared.batch_sources.get(source_index) else {
        return Ok(None);
    };
    let Some(prepared_parquet) = &declaration.local else {
        return Ok(None);
    };
    if state.started.get() || !state.usage.get().admits_independent_read() {
        return Ok(None);
    }
    let dtype = declaration.schema.dtype().clone();
    let empty_short_circuit = residual_is_none
        && super::super::scan::has_infallible_empty_build(probe_filters, &mut |name| {
            dtype.as_struct_fields_opt()?.field(name)
        });
    if empty_short_circuit {
        return Ok(None);
    }
    let Some(admission) = admit_parallel_parquet_with_cap(
        prepared_parquet,
        state.usage.get(),
        Consumption::Query,
        &execution.prepared.session,
        1,
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some((state, prepared_parquet, dtype, admission)))
}

pub(in crate::local_primitives::prepared_relational) fn can_admit_parallel_parquet_join(
    execution: &Execution<'_, '_>,
    source_index: usize,
    residual_is_none: bool,
    probe_filters: &[ProbeRuntimeFilter],
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    Ok(admit_parallel_parquet_join_source(
        execution,
        source_index,
        residual_is_none,
        probe_filters,
        context,
    )?
    .is_some())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(in crate::local_primitives::prepared_relational) fn run_parallel_parquet_join(
    execution: &Execution<'_, '_>,
    source_index: usize,
    projection: &Arc<super::super::PreparedMemoryProjection>,
    mandatory_predicate: Option<&(PredicateExpr, bool)>,
    residual: Option<&super::super::MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    columns: &[String],
    scan_fields: &[(String, vortex::array::dtype::DType)],
    transforms: &Arc<Vec<super::super::scan_parallel::ParallelAggregateTransform>>,
    sealed_join: &Arc<crate::local_primitives::native_relational_join::SealedJoin>,
    scan_rows: usize,
    batch_rows: usize,
    finite_float_predicate: bool,
    context: &NativeExecutionContext<'_>,
    metrics: &super::Metrics<'_>,
    consume: &mut dyn FnMut(super::ArrayRef) -> Result<()>,
) -> Result<Option<Vec<crate::local_primitives::native_capacity::ReservedVec<bool>>>> {
    use crate::native_work_sizing::{Owner, Rows};
    use vortex::array::dtype::DType;

    type JoinWorkerMatched = Option<crate::local_primitives::native_capacity::ReservedVec<bool>>;
    type JoinTaskDone = (
        (
            super::super::scan::ScanBatchCounts,
            JoinWorkerMatched,
            ColumnarReadReport,
            WorkerSourceStats,
        ),
        WorkerExecutionDelta,
    );

    struct ActiveJoinTask {
        rx: Receiver<Result<super::ArrayRef>>,
        task: ComputeTask<JoinTaskDone>,
    }

    fn teardown_join_tasks(
        child_token: &CancellationToken,
        current_rx: Option<Receiver<Result<super::ArrayRef>>>,
        current_task: Option<ComputeTask<JoinTaskDone>>,
        mut pending: VecDeque<ActiveJoinTask>,
    ) {
        child_token.cancel();
        drop(current_rx);
        for active in &mut pending {
            let (_, dummy_rx) = mpsc::sync_channel(0);
            let real_rx = std::mem::replace(&mut active.rx, dummy_rx);
            drop(real_rx);
        }
        if let Some(task) = current_task {
            let _ = task.join();
        }
        while let Some(active) = pending.pop_front() {
            drop(active.rx);
            let _ = active.task.join();
        }
    }

    let Some((state, prepared_parquet, dtype, admission)) = admit_parallel_parquet_join_source(
        execution,
        source_index,
        residual.is_none(),
        probe_filters,
        context,
    )?
    else {
        return Ok(None);
    };
    state.started.set(true);
    let runtime_predicate = super::super::scan::admitted_runtime_predicate(probe_filters, &dtype);
    let probe_filters_arc: Arc<[ProbeRuntimeFilter]> = Arc::from(probe_filters);
    let ParallelAdmission {
        pool,
        workers,
        ranges,
    } = admission;
    let total_ranges = ranges.len();
    let child_flag = Arc::new(AtomicBool::new(false));
    let child_token =
        CancellationToken::from_shared_flag_with_parent(child_flag, context.cancellation());
    let scope = execution
        .prepared
        .session
        .worker_execution_scope(context, child_token.clone())?;

    let projection = Arc::clone(projection);
    let mandatory_predicate = mandatory_predicate.cloned();
    let residual = residual.cloned();
    let columns: Arc<[String]> = Arc::from(columns);
    let scan_fields: Arc<[(String, DType)]> = Arc::from(scan_fields);
    let transforms = Arc::clone(transforms);
    let expected_dtype = dtype;

    let submit_range = |range: std::ops::Range<usize>| -> Result<ActiveJoinTask> {
        let lease = context.memory().reserve(TASK_RESERVATION_BYTES)?;
        let (tx, rx) = mpsc::sync_channel::<Result<super::ArrayRef>>(BOUNDED_CHANNEL_DEPTH);
        let range_scope = scope.clone();
        let range_prepared = Arc::clone(prepared_parquet);
        let range_session = execution.prepared.session.clone();
        let range_mandatory = mandatory_predicate.clone();
        let range_runtime = runtime_predicate.clone();
        let range_filters = Arc::clone(&probe_filters_arc);
        let range_projection = Arc::clone(&projection);
        let range_residual = residual.clone();
        let range_columns = Arc::clone(&columns);
        let range_fields = Arc::clone(&scan_fields);
        let range_transforms = Arc::clone(&transforms);
        let range_join = Arc::clone(sealed_join);
        let expected_dtype = expected_dtype.clone();
        let cancel = child_token.clone();
        let residual_present = range_residual.is_some();
        let task = pool.submit(
            Budgeted::new(
                move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                    range_scope.with_context(|wctx| {
                        let res: Result<(super::super::scan::ScanBatchCounts, JoinWorkerMatched, ColumnarReadReport, WorkerSourceStats)> = (|| {
                            let mut worker_matched = range_join.new_worker_matched(wctx.memory())?;
                            let mut reader = range_prepared.open_row_group_range_with_filters(
                                range,
                                range_mandatory,
                                range_runtime,
                                false,
                                Arc::clone(&range_filters),
                                residual_present,
                                false,
                                wctx,
                            )?;
                            let mut sizing =
                                Rows::from_fields(Owner::Scan, &range_fields, scan_rows, 2, wctx)?;
                            let mut stats = WorkerSourceStats::default();
                            let mut counts = super::super::scan::ScanBatchCounts::default();
                            while let Some((source, _selection, satisfaction)) =
                                reader.next_native_with_selection(&range_session, wctx)?
                            {
                                if !source.belongs_to_session(&range_session)
                                    || source.dtype() != &expected_dtype
                                {
                                    return Err(failed(
                                        "parallel range input changed its declared schema or session",
                                    ));
                                }
                                let released = source.batch_release_witness()?;
                                stats.observe(&source, wctx)?;
                                let source_array = source.batch_array(wctx)?;
                                let active_residual = if satisfaction.mandatory_predicate_satisfied {
                                    None
                                } else {
                                    range_residual.as_ref()
                                };
                                let active_filters: &[ProbeRuntimeFilter] =
                                    if satisfaction.probe_filters_satisfied {
                                        &[]
                                    } else {
                                        &range_filters
                                    };
                                let execute_source =
                                    |source_array: &super::ArrayRef, range: std::ops::Range<usize>| {
                                        if satisfaction.mandatory_predicate_satisfied {
                                            range_projection
                                                .execute_batch_array_without_filter(source_array, range, wctx)
                                        } else {
                                            range_projection
                                                .execute_batch_array(source_array, range, wctx)
                                        }
                                    };
                                counts.scans_started = counts
                                    .scans_started
                                    .checked_add(1)
                                    .ok_or_else(|| failed("scan counter overflow"))?;
                                let mut emit_probe = |final_batch: super::ArrayRef| -> Result<()> {
                                    range_join.consume_sealed(
                                        final_batch,
                                        worker_matched.as_mut().map(|v| v.values.as_mut_slice()),
                                        wctx,
                                        batch_rows,
                                        &mut |out_batch| {
                                            tx.send(Ok(out_batch)).map_err(|_| {
                                                failed("parallel join consumer stopped early")
                                            })
                                        },
                                    )?;
                                    Ok(())
                                };
                                if source_array.is_empty() {
                                    let projected = execute_source(source_array, 0..0)?;
                                    super::super::scan::accept_scan_batch_counts(
                                        &projected,
                                        &range_columns,
                                        active_residual,
                                        active_filters,
                                        &range_fields,
                                        wctx,
                                        &mut counts,
                                        batch_rows,
                                        &mut |array| {
                                            super::super::scan_parallel::apply_parallel_transforms(
                                                &range_transforms,
                                                0,
                                                array,
                                                wctx,
                                                &mut emit_probe,
                                            )
                                        },
                                    )?;
                                } else {
                                    counts.data_scans = counts
                                        .data_scans
                                        .checked_add(1)
                                        .ok_or_else(|| failed("data scan counter overflow"))?;
                                    let mut start = 0;
                                    while start < source_array.len() {
                                        let selected =
                                            sizing.select((source_array.len() - start) as u64, wctx)?;
                                        let end = start + selected.rows;
                                        let projected = execute_source(source_array, start..end)?;
                                        super::super::scan::accept_scan_batch_counts(
                                            &projected,
                                            &range_columns,
                                            active_residual,
                                            active_filters,
                                            &range_fields,
                                            wctx,
                                            &mut counts,
                                            batch_rows,
                                            &mut |array| {
                                                super::super::scan_parallel::apply_parallel_transforms(
                                                    &range_transforms,
                                                    0,
                                                    array,
                                                    wctx,
                                                    &mut emit_probe,
                                                )
                                            },
                                        )?;
                                        sizing.complete(selected, None, wctx);
                                        start = end;
                                    }
                                }
                                drop(source);
                                verify_released(&released)?;
                            }
                            let report = reader.report()?;
                            Ok((counts, worker_matched, report, stats))
                        })();
                        match res {
                            Ok(done) => Ok(done),
                            Err(error) => {
                                let _ = tx.send(Err(error.clone()));
                                Err(error)
                            }
                        }
                    })
                },
                lease,
            ),
            cancel,
        )?;
        Ok(ActiveJoinTask { rx, task })
    };

    let mut active: VecDeque<ActiveJoinTask> = VecDeque::with_capacity(workers);
    let mut range_iter = ranges.into_iter();
    for _ in 0..workers {
        let Some(range) = range_iter.next() else {
            break;
        };
        match submit_range(range) {
            Ok(task) => active.push_back(task),
            Err(error) => {
                teardown_join_tasks(&child_token, None, None, active);
                return Err(error);
            }
        }
    }

    let mut merged_report: Option<ColumnarReadReport> = None;
    let mut merged_stats = WorkerSourceStats::default();
    let mut merged_scans = super::super::scan::ScanBatchCounts::default();
    let mut matched_vecs = Vec::with_capacity(total_ranges);
    while let Some(ActiveJoinTask { rx, task }) = active.pop_front() {
        if let Some(next_range) = range_iter.next() {
            match submit_range(next_range) {
                Ok(next_task) => active.push_back(next_task),
                Err(error) => {
                    teardown_join_tasks(&child_token, Some(rx), Some(task), active);
                    return Err(error);
                }
            }
        }
        loop {
            if let Err(error) = context.check_cancelled() {
                teardown_join_tasks(&child_token, Some(rx), Some(task), active);
                return Err(error);
            }
            match rx.recv() {
                Ok(Ok(batch)) => {
                    if let Err(error) = consume(batch) {
                        teardown_join_tasks(&child_token, Some(rx), Some(task), active);
                        return Err(error);
                    }
                }
                Ok(Err(error)) => {
                    teardown_join_tasks(&child_token, Some(rx), Some(task), active);
                    return Err(error);
                }
                Err(_) => break,
            }
        }
        let joined = match task.join() {
            Ok(done) => done,
            Err(error) => {
                teardown_join_tasks(&child_token, None, None, active);
                return Err(error);
            }
        };
        let (((worker_counts, worker_matched, rpt, stats), delta), lease) = joined.into_parts();
        drop(lease);
        let step = (|| -> Result<()> {
            context.merge_worker_delta(delta)?;
            match &mut merged_report {
                Some(existing) => existing.merge_range(&rpt)?,
                None => merged_report = Some(rpt),
            }
            checked_add(&mut merged_scans.scan_rows, worker_counts.scan_rows)?;
            checked_add(&mut merged_scans.scan_batches, worker_counts.scan_batches)?;
            checked_add(&mut merged_scans.scans_started, worker_counts.scans_started)?;
            checked_add(&mut merged_scans.data_scans, worker_counts.data_scans)?;
            checked_add(
                &mut merged_scans.residual_batches,
                worker_counts.residual_batches,
            )?;
            checked_add(&mut merged_stats.batches, stats.batches)?;
            checked_add(&mut merged_stats.rows, stats.rows)?;
            checked_add(
                &mut merged_stats.input_logical_bytes,
                stats.input_logical_bytes,
            )?;
            checked_add(
                &mut merged_stats.intake_payload_bytes_copied,
                stats.intake_payload_bytes_copied,
            )?;
            checked_add(
                &mut merged_stats.intake_columnar_buffer_bytes_copied,
                stats.intake_columnar_buffer_bytes_copied,
            )?;
            merged_stats
                .intake_columnar_copy_reasons
                .merge(&stats.intake_columnar_copy_reasons);
            checked_add(
                &mut merged_stats.intake_columnar_buffer_bytes_transferred,
                stats.intake_columnar_buffer_bytes_transferred,
            )?;
            checked_add(
                &mut merged_stats.intake_columnar_buffer_regions_transferred,
                stats.intake_columnar_buffer_regions_transferred,
            )?;
            checked_add(
                &mut merged_stats.intake_columnar_allocation_bytes_transferred,
                stats.intake_columnar_allocation_bytes_transferred,
            )?;
            merged_stats.max_batch_rows = merged_stats.max_batch_rows.max(stats.max_batch_rows);
            merged_stats.max_batch_logical_bytes = merged_stats
                .max_batch_logical_bytes
                .max(stats.max_batch_logical_bytes);
            if let Some(m) = worker_matched {
                matched_vecs.push(m);
            }
            Ok(())
        })();
        if let Err(error) = step {
            teardown_join_tasks(&child_token, None, None, active);
            return Err(error);
        }
    }

    let report = merged_report
        .ok_or_else(|| failed("parallel join range task execution yielded no report"))?;
    prepared_parquet.complete_region_selection(context)?;
    {
        let mut execution_report = execution.report.borrow_mut();
        merged_stats.merge_into(&mut execution_report, &state.next_batch)?;
        execution_report.parallel_range_workers =
            execution_report.parallel_range_workers.max(workers as u64);
        execution_report.source_workers = execution_report.source_workers.max(workers as u64);
        execution_report.operator_workers = execution_report.operator_workers.max(workers as u64);
        checked_add(
            &mut execution_report.parallel_range_tasks,
            total_ranges as u64,
        )?;
    }
    execution.record_columnar_input(report)?;
    execution.finish_producer(state, None, Consumption::Query)?;
    merged_scans.merge_into(metrics)?;
    metrics
        .source_workers
        .set(metrics.source_workers.get().max(workers as u64));
    metrics
        .operator_workers
        .set(metrics.operator_workers.get().max(workers as u64));
    if finite_float_predicate {
        super::super::add(&metrics.pipeline.finite_float_scans, 1)?;
    }
    Ok(Some(matched_vecs))
}
