//! Bounded, ordered transport for the existing mixed integer chunk recipe.
//! The caller prepares accessors; workers build partials; the caller merges in
//! source order. Only a completed winner-count proof admits this mode.

use super::{
    AggregateCountDistinctPairPreunionKey, AggregateDistinctValue, AggregateValueTransform,
    GroupedAggregateStates, SimpleAggregateFunction, SimpleAggregateState, SimpleAggregateStates,
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, SubmitOutcome},
    aggregate_direct_column_accessors_from_chunk,
    mixed_distinct_partial::MixedDistinctPartial,
    required_simple_aggregate, winner_distinct,
};
use crate::VortexSimpleAggregateRequest;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{sync::Arc, time::Instant};
use vortex::array::ArrayRef;

const MAX_ROWS: usize = 262_144;
const MAX_MEASURES: usize = 8;

#[cfg(test)]
type WorkerStartHook = Box<dyn FnOnce(&super::aggregate_chunk_jobs::ChunkWorkerContext) + Send>;
#[cfg(test)]
thread_local! {
    pub(super) static WORKER_START_TEST_HOOK: std::cell::RefCell<Option<WorkerStartHook>> =
        const { std::cell::RefCell::new(None) };
}

struct Recipe {
    request: VortexSimpleAggregateRequest,
    columns: Vec<String>,
    limit: Option<usize>,
    groups: usize,
    policy: VortexLocalPrimitiveExecutionPolicy,
}

struct Completed {
    partial: MixedDistinctPartial,
    rows: usize,
    build_nanos: u128,
}

pub(super) struct MixedDistinctWorkers {
    jobs: AggregateChunkJobs<Completed>,
    recipe: Arc<Recipe>,
    active: bool,
    rows: u64,
    accessor_nanos: u128,
    build_nanos: u128,
    merge_nanos: u128,
    peak_task_reservation: u64,
    parallelism: usize,
    _metadata: MemoryLease,
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native mixed DISTINCT workers {reason}; no fallback execution was attempted"
    ))
}

/// CPU-ownership precheck only. Actual admission needs the completed R3.a proof.
pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    if request.predicate.is_some()
        || aggregate.group_by.len() != 1
        || !aggregate.group_expressions.is_empty()
        || !aggregate.having.is_empty()
        || aggregate.spill.is_some()
        || !(2..=MAX_MEASURES).contains(&aggregate.measures.len())
        || request
            .source_order_limit
            .and_then(|limit| aggregate.offset.checked_add(limit))
            .is_none_or(|cap| !(1..=128).contains(&cap))
    {
        return false;
    }
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    let Ok(states) = SimpleAggregateStates::new(aggregate, &columns) else {
        return false;
    };
    let Some((distinct, _)) = states.single_count_distinct_state_index_and_column() else {
        return false;
    };
    let Some(count) = states.states.iter().find(|state| {
        state.function == SimpleAggregateFunction::Count && state.column_index.is_none()
    }) else {
        return false;
    };
    super::exact_distinct_pairs::workers::order_admitted(
        &aggregate.order_by,
        &count.alias,
        aggregate.group_by[0].as_str(),
    ) && states.states.iter().enumerate().all(|(index, state)| {
        state.argument_offset.is_none()
            && matches!(state.value_transform, AggregateValueTransform::Identity)
            && (index == distinct
                || matches!(
                    (state.function, state.column_index),
                    (SimpleAggregateFunction::Count, None)
                        | (
                            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg,
                            Some(_)
                        )
                ))
    })
}

impl MixedDistinctWorkers {
    pub(super) fn admit(
        states: &GroupedAggregateStates<'_>,
        proof: &winner_distinct::Report,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Self>> {
        let Some(groups) = proof.selected_group_bound() else {
            return Ok(None);
        };
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        if parallelism < 2 || states.state_template.states.len() > MAX_MEASURES {
            return Ok(None);
        }
        // This admitted request has no expressions, HAVING or spill owners.
        let request_bytes = states
            .request
            .measures
            .iter()
            .try_fold(0usize, |bytes, measure| {
                bytes
                    .checked_add(size_of::<crate::VortexSimpleAggregateMeasure>())?
                    .checked_add(measure.function.len())?
                    .checked_add(measure.alias.len())?
                    .checked_add(
                        measure
                            .column
                            .as_ref()
                            .map_or(0, |column| column.as_str().len()),
                    )?
                    .checked_add(measure.value_transform.as_ref().map_or(0, String::len))
            })
            .and_then(|bytes| {
                states
                    .request
                    .group_by
                    .iter()
                    .try_fold(bytes, |bytes, column| {
                        bytes
                            .checked_add(size_of::<shardloom_core::ColumnRef>())?
                            .checked_add(column.as_str().len())
                    })
            })
            .and_then(|bytes| {
                states
                    .request
                    .order_by
                    .iter()
                    .try_fold(bytes, |bytes, order| {
                        bytes
                            .checked_add(size_of::<crate::VortexAggregateOrderExpr>())?
                            .checked_add(order.column.len())
                    })
            })
            .ok_or_else(|| failed("request size overflow"))?;
        let metadata = columns
            .iter()
            .try_fold(request_bytes, |bytes, column| {
                bytes
                    .checked_add(column.len())?
                    .checked_add(size_of::<String>())
            })
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(size_of::<Recipe>()))
            .ok_or_else(|| failed("metadata size overflow"))?;
        let Ok(lease) =
            memory.reserve(u64::try_from(metadata).map_err(|_| failed("metadata size overflow"))?)
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            jobs: AggregateChunkJobs::with_cancellation(
                parallelism,
                parallelism.min(8),
                memory.snapshot().limit_bytes,
                memory.clone(),
                // Observe the operation while keeping retirement/error cleanup
                // local to this stage; dropping successful jobs must not cancel
                // a caller-owned prepared execution token.
                cancellation.map_or_else(CancellationToken::default, |parent| {
                    CancellationToken::from_shared_flag_with_parent(Arc::default(), parent)
                }),
            )?,
            recipe: Arc::new(Recipe {
                request: states.request.clone(),
                columns: columns.to_vec(),
                limit: states.result_limit,
                groups,
                policy,
            }),
            active: true,
            rows: 0,
            accessor_nanos: 0,
            build_nanos: 0,
            merge_nanos: 0,
            peak_task_reservation: 0,
            parallelism,
            _metadata: lease,
        }))
    }

    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.jobs.is_full() {
            self.merge_next(states)?;
        }
        Ok(())
    }

    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        if !self.active {
            return Ok(false);
        }
        self.jobs.check_cancelled()?;
        if chunk.len() > MAX_ROWS {
            self.retire(states)?;
            return Ok(false);
        }
        let started = Instant::now();
        let accessors = aggregate_direct_column_accessors_from_chunk(
            chunk,
            &self.recipe.columns,
            &mut states.native_execution_ctx,
        )?;
        self.accessor_nanos += started.elapsed().as_nanos();
        states
            .native_numeric_accessor_work
            .add(&accessors.numeric_work)?;
        states.observe_aggregate_accessors(&self.recipe.columns, &accessors);
        if states
            .grouped_count_distinct_integer_pair_preunion_inputs_for_accessors(
                &accessors,
                None,
                chunk.len(),
            )?
            .is_none()
        {
            self.retire(states)?;
            return states.update_compact_direct_from_accessors(
                &accessors,
                &self.recipe.columns,
                None,
                chunk.len(),
            );
        }
        let bytes = reservation_bytes(
            chunk.len(),
            self.recipe.groups,
            &states.state_template,
            self.recipe.columns.len(),
        )?;
        while self.jobs.outstanding() != 0 && self.available_bytes() < bytes {
            self.merge_next(states)?;
        }
        if self.available_bytes() < bytes {
            self.retire(states)?;
            return states.update_compact_direct_from_accessors(
                &accessors,
                &self.recipe.columns,
                None,
                chunk.len(),
            );
        }
        let recipe = Arc::clone(&self.recipe);
        let rows = chunk.len();
        #[cfg(test)]
        let start_hook = WORKER_START_TEST_HOOK.with(|hook| hook.borrow_mut().take());
        let outcome = self.jobs.try_submit(bytes, move |worker, _lease| {
            worker.check_cancelled()?;
            #[cfg(test)]
            if let Some(hook) = start_hook {
                hook(worker);
            }
            let started = Instant::now();
            let local = GroupedAggregateStates::new_with_resource_envelope(
                &recipe.request,
                recipe.limit,
                &recipe.columns,
                false,
                false,
                recipe.policy.resource_envelope(),
            )?;
            let partial = local
                .prepare_mixed_distinct_partial(
                    &accessors,
                    None,
                    rows,
                    Some(recipe.groups),
                    &|| worker.check_cancelled(),
                )?
                .ok_or_else(|| failed("immutable accessor admission changed inside the job"))?;
            if partial.retained_capacity_bytes()? > bytes {
                return Err(failed("partial capacity exceeded its admitted model"));
            }
            Ok(Completed {
                partial,
                rows,
                build_nanos: started.elapsed().as_nanos(),
            })
        })?;
        if let SubmitOutcome::InitialCapacityDenied(_) = outcome {
            // The unexecuted closure releases its prepared owners. Drain older
            // work before the original caller reprocesses this current chunk.
            self.retire(states)?;
            return Ok(false);
        }
        self.peak_task_reservation = self.peak_task_reservation.max(bytes);
        Ok(true)
    }

    fn available_bytes(&self) -> u64 {
        let memory = self.jobs.memory().snapshot();
        memory.limit_bytes - memory.reserved_bytes
    }

    fn merge_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let expected = self.jobs.joined();
        let Some(completed) = self.jobs.join_next()? else {
            return Ok(());
        };
        if completed.ordinal() != expected {
            self.jobs.cancel();
            return Err(failed("completion order changed"));
        }
        let started = Instant::now();
        completed.consume_owned(|completed| {
            self.rows = self
                .rows
                .checked_add(completed.rows as u64)
                .ok_or_else(|| failed("row count overflow"))?;
            self.build_nanos += completed.build_nanos;
            completed.partial.merge_into(states)
        })?;
        self.merge_nanos += started.elapsed().as_nanos();
        Ok(())
    }

    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.merge_next(states)?;
        }
        self.jobs.check_cancelled()
    }

    fn retire(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.finish(states)?;
        self.jobs.retire()?;
        self.active = false;
        Ok(())
    }

    pub(super) fn retired(&self) -> bool {
        !self.active
    }
    pub(super) fn cancel(&self) {
        self.jobs.cancel();
    }
    #[cfg(test)]
    pub(super) fn has_committed_groups(&self) -> bool {
        self.rows != 0
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value =
            serde_json::from_str(summary).map_err(|error| failed(&error.to_string()))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        object.insert("aggregate_mixed_distinct_workers".into(), serde_json::json!({
            "rows": self.rows, "submitted_chunks": self.jobs.submitted(), "completed_chunks": self.jobs.joined(),
            "peak_outstanding_chunks": self.jobs.peak_outstanding(), "cpu_ceiling": self.parallelism,
            "retired_to_same_serial_consumer": !self.active,
            "proven_group_bound": self.recipe.groups, "max_chunk_rows": MAX_ROWS,
            "peak_task_reservation_bytes": self.peak_task_reservation,
            "accessor_caller_nanos": u64::try_from(self.accessor_nanos).unwrap_or(u64::MAX),
            "partial_build_worker_nanos": u64::try_from(self.build_nanos).unwrap_or(u64::MAX),
            "ordered_merge_caller_nanos": u64::try_from(self.merge_nanos).unwrap_or(u64::MAX),
            "join_wait_caller_nanos": u64::try_from(self.jobs.join_wait_nanos()).unwrap_or(u64::MAX),
            "scope": "bounded_complete_chunk_partials_with_existing_integer_recipe;ordinary_rows_not_deduplicated;source_order_merge;worker_spans_may_overlap_not_CPU_or_complete_wall;task_capacity_model_excludes_upstream_source_provider_allocations_and_global_aggregate_state_not_RSS_bound"
        }));
        *summary = value.to_string();
        Ok(())
    }
}

fn reservation_bytes(
    rows: usize,
    groups: usize,
    template: &SimpleAggregateStates,
    columns: usize,
) -> Result<u64> {
    let rows = rows
        .max(1)
        .checked_next_power_of_two()
        .ok_or_else(|| failed("row capacity overflow"))?;
    let alias_bytes = template
        .states
        .iter()
        .try_fold(0usize, |total, state| total.checked_add(state.alias.len()))
        .ok_or_else(|| failed("alias size overflow"))?;
    let group = size_of::<SimpleAggregateStates>()
        .checked_add(size_of::<super::AggregateCountDistinctPreunionGroupKey>())
        .and_then(|bytes| bytes.checked_add(64))
        .ok_or_else(|| failed("group capacity overflow"))?;
    let row = columns
        .checked_mul(size_of::<u64>())
        .and_then(|bytes| bytes.checked_add(size_of::<AggregateCountDistinctPairPreunionKey>()))
        .and_then(|bytes| bytes.checked_add(size_of::<AggregateDistinctValue>()))
        .and_then(|bytes| bytes.checked_add(2))
        .ok_or_else(|| failed("row width overflow"))?;
    let state = template
        .states
        .len()
        .checked_mul(size_of::<SimpleAggregateState>())
        .and_then(|size| size.checked_add(alias_bytes))
        .and_then(|size| size.checked_add(group))
        .ok_or_else(|| failed("state capacity overflow"))?;
    let bytes = rows
        .checked_mul(row)
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| {
            groups
                .checked_mul(state)
                .and_then(|extra| extra.checked_mul(8))
                .and_then(|extra| bytes.checked_add(extra))
        })
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| failed("task capacity overflow"))?;
    u64::try_from(bytes).map_err(|_| failed("task capacity exceeds u64"))
}
