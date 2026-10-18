//! Prepare source-backed UTF8 dictionaries and admitted dense partials in a
//! bounded queue; merge through the original aggregate consumer in source order.

use super::{
    AggregateUtf8DictionarySource, AggregateValueTransform, GroupedAggregateStates,
    NativeNumericAccessorWork, TransformedDictionaryDenseGeneralPlan,
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveResourceEnvelope,
    VortexQueryPrimitiveRequest, aggregate_canonical_utf8_chunk,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext, SubmitOutcome},
    aggregate_utf8_accessor_from_canonical,
    dictionary_group_partials::{self, PreparedDictionaryPartials},
    logical_field_from_native_array, required_simple_aggregate,
    utf8_chunk_dictionary::Utf8ChunkDictionary,
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{sync::Arc, time::Instant};
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        arrays::Dict,
        dtype::{DType, Nullability},
    },
    session::VortexSession,
};

const MAX_ROWS: usize = 262_144;
#[cfg(test)]
type WorkerStartHook = Box<dyn FnOnce(&super::aggregate_chunk_jobs::ChunkWorkerContext) + Send>;
#[cfg(test)]
thread_local! {
    pub(super) static WORKER_START_TEST_HOOK: std::cell::RefCell<Option<WorkerStartHook>> =
        const { std::cell::RefCell::new(None) };
    pub(super) static SUBMIT_TEST_PRESSURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct Recipe {
    columns: Vec<String>,
    dense_plan: Option<TransformedDictionaryDenseGeneralPlan>,
}

struct Completed {
    accessor: super::AggregateDirectColumnAccessor,
    work: NativeNumericAccessorWork,
    rows: usize,
    partials: Option<PreparedDictionaryPartials>,
    reduction_nanos: u128,
}

impl Recipe {
    fn prepare(
        &self,
        input: &vortex::array::arrays::VarBinViewArray,
        source: AggregateUtf8DictionarySource,
        mut work: NativeNumericAccessorWork,
        session: &VortexSession,
        worker: &ChunkWorkerContext,
    ) -> Result<Completed> {
        worker.check_cancelled()?;
        let allocation_started = Instant::now();
        let dictionary = Utf8ChunkDictionary::with_row_capacity(input.len())?;
        work.utf8.dictionary_nanos += allocation_started.elapsed().as_nanos();
        let accessor = aggregate_utf8_accessor_from_canonical(
            &self.columns[0],
            input,
            source,
            &mut work.utf8,
            dictionary,
            &|| worker.check_cancelled(),
            &mut session.create_execution_ctx(),
        )?;
        let reduction_started = Instant::now();
        let partials = self
            .dense_plan
            .map(|plan| {
                dictionary_group_partials::prepare(&accessor, plan, &|| worker.check_cancelled())
            })
            .transpose()?
            .flatten();
        let reduction_nanos = if self.dense_plan.is_some() {
            reduction_started.elapsed().as_nanos()
        } else {
            0
        };
        Ok(Completed {
            accessor,
            work,
            rows: input.len(),
            partials,
            reduction_nanos,
        })
    }
}

pub(super) struct DictionaryPrepareWorkers {
    jobs: AggregateChunkJobs<Completed>,
    recipe: Arc<Recipe>,
    session: VortexSession,
    active: bool,
    rows: u64,
    provider_nanos: u128,
    dictionary_nanos: u128,
    consume_nanos: u128,
    peak_task_bytes: u64,
    provider_progress: bool,
    background_workers: usize,
    window: usize,
    reduction_nanos: u128,
    partial_chunks: u64,
    partial_values: u64,
    partial_groups: u64,
    _metadata: MemoryLease,
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native dictionary preparation {reason}; no fallback execution was attempted"
    ))
}

fn shape(states: &GroupedAggregateStates<'_>, columns: &[String]) -> bool {
    columns.len() == 1
        && states.group_key_indices.len() == 1
        && states.group_columns.len() == 1
        && states.group_columns[0].column_index == 0
        && states.group_columns[0].extra_column_indices.is_empty()
        && matches!(
            states.group_columns[0].transform,
            AggregateValueTransform::UrlDomain
        )
        && !states.request.order_by.is_empty()
        && states.request.spill.is_none()
        && states.source_order_group_admission_limit().is_none()
        && !states.state_template.is_count_star_only()
        && TransformedDictionaryDenseGeneralPlan::from_template(&states.state_template, 0).is_some()
}

pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    let Ok(envelope) = VortexLocalPrimitiveResourceEnvelope::new(1, 1) else {
        return false;
    };
    GroupedAggregateStates::new_with_resource_envelope(
        aggregate,
        request.source_order_limit,
        &columns,
        false,
        false,
        envelope,
        None,
    )
    .is_ok_and(|states| shape(&states, &columns))
}

impl DictionaryPrepareWorkers {
    pub(super) fn admit(
        states: &GroupedAggregateStates<'_>,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Self>> {
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        if parallelism < 2 || !shape(states, columns) {
            return Ok(None);
        }
        let DType::Struct(fields, Nullability::NonNullable) = dtype else {
            return Ok(None);
        };
        if fields.field(columns[0].as_str()) != Some(DType::Utf8(Nullability::NonNullable)) {
            return Ok(None);
        }
        let dense_plan = (parallelism >= 4 && states.compact_measure_specs.is_none())
            .then(|| {
                TransformedDictionaryDenseGeneralPlan::from_template(&states.state_template, 0)
            })
            .flatten();
        let background_workers = if dense_plan.is_some() { 2 } else { 1 };
        let window = background_workers + 1;
        let metadata = columns[0]
            .len()
            .checked_mul(4)
            .and_then(|bytes| {
                bytes.checked_add(size_of::<Self>() + size_of::<Recipe>() + 4 * size_of::<String>())
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("metadata size overflow"))?;
        let Ok(lease) = memory.reserve(metadata) else {
            return Ok(None);
        };
        Ok(Some(Self {
            jobs: AggregateChunkJobs::with_cancellation(
                background_workers + 1,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
                cancellation.map_or_else(CancellationToken::default, |parent| {
                    CancellationToken::from_shared_flag_with_parent(Arc::default(), parent)
                }),
            )?,
            recipe: Arc::new(Recipe {
                columns: columns.to_vec(),
                dense_plan,
            }),
            // The grouped state already owns the query's admitted context.
            // Preserve its allocator and provider registry in caller and jobs.
            session: states
                .native_execution_ctx
                .as_ref()
                .ok_or_else(|| {
                    failed("native preparation requires the admitted execution context")
                })?
                .session()
                .clone(),
            active: true,
            rows: 0,
            provider_nanos: 0,
            dictionary_nanos: 0,
            consume_nanos: 0,
            peak_task_bytes: 0,
            provider_progress: parallelism >= 3,
            background_workers,
            window,
            reduction_nanos: 0,
            partial_chunks: 0,
            partial_values: 0,
            partial_groups: 0,
            _metadata: lease,
        }))
    }

    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.jobs.is_full() {
            self.consume_next(states)?;
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
        let array = logical_field_from_native_array(chunk, &self.recipe.columns[0])?;
        // Keep native dictionary admission and its ordering intact. The normal
        // caller consumes this chunk after all older prepared chunks finish.
        if array.as_opt::<Dict>().is_some() {
            self.finish(states)?;
            return Ok(false);
        }
        let source = if array.is_host() {
            AggregateUtf8DictionarySource::HostUtf8ChunkDictionary
        } else {
            AggregateUtf8DictionarySource::DecodedUtf8ChunkDictionary
        };
        let mut work = NativeNumericAccessorWork::default();
        let utf8 = aggregate_canonical_utf8_chunk(
            &array,
            &mut work.utf8,
            &mut self.session.create_execution_ctx(),
        )?;
        self.provider_nanos += work.utf8.provider_nanos;
        if utf8.len() != chunk.len() || utf8.dtype() != array.dtype() {
            return Err(failed("canonical source changed rows or dtype"));
        }
        let rows = utf8.len();
        let partial_bytes = if self.recipe.dense_plan.is_some() {
            dictionary_group_partials::bounded_capacity_bytes(rows)?
        } else {
            0
        };
        let bytes = Utf8ChunkDictionary::bounded_capacity_bytes(rows)?
            .checked_add(utf8.nbytes())
            .and_then(|bytes| bytes.checked_add(partial_bytes))
            .and_then(|bytes| bytes.checked_add(size_of::<Completed>() as u64))
            .ok_or_else(|| failed("task byte size overflow"))?;
        #[cfg(test)]
        let _test_pressure = SUBMIT_TEST_PRESSURE.with(std::cell::Cell::take).then(|| {
            self.jobs
                .memory()
                .reserve(self.available_bytes().saturating_sub(1024))
                .unwrap()
        });
        while self.jobs.outstanding() != 0 && self.available_bytes() < bytes {
            self.consume_next(states)?;
        }
        if self.available_bytes() < bytes {
            self.retire(states)?;
            return self.consume_canonical(states, &utf8, source, work);
        }
        let recipe = Arc::clone(&self.recipe);
        let session = self.session.clone();
        let input = utf8.clone();
        let worker_work = work.clone();
        #[cfg(test)]
        let start_hook = WORKER_START_TEST_HOOK.with(|hook| hook.borrow_mut().take());
        let outcome = self.jobs.try_submit(bytes, move |worker, _lease| {
            worker.check_cancelled()?;
            #[cfg(test)]
            if let Some(hook) = start_hook {
                hook(worker);
            }
            recipe.prepare(&input, source, worker_work, &session, worker)
        })?;
        if let SubmitOutcome::InitialCapacityDenied(_) = outcome {
            self.retire(states)?;
            return self.consume_canonical(states, &utf8, source, work);
        }
        self.peak_task_bytes = self.peak_task_bytes.max(bytes);
        Ok(true)
    }

    fn consume_canonical(
        &mut self,
        states: &mut GroupedAggregateStates<'_>,
        utf8: &vortex::array::arrays::VarBinViewArray,
        source: AggregateUtf8DictionarySource,
        mut work: NativeNumericAccessorWork,
    ) -> Result<bool> {
        let accessor = aggregate_utf8_accessor_from_canonical(
            &self.recipe.columns[0],
            utf8,
            source,
            &mut work.utf8,
            Utf8ChunkDictionary::default(),
            &|| self.jobs.check_cancelled(),
            &mut self.session.create_execution_ctx(),
        )?;
        states.native_numeric_accessor_work.add(&work)?;
        if !states.update_compact_direct_from_accessors(
            &[accessor],
            &self.recipe.columns,
            None,
            utf8.len(),
        )? {
            return Err(failed("serial accessor lost the admitted consumer"));
        }
        Ok(true)
    }

    fn available_bytes(&self) -> u64 {
        let memory = self.jobs.memory().snapshot();
        memory.limit_bytes - memory.reserved_bytes
    }

    fn consume_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
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
            self.dictionary_nanos += completed.work.utf8.dictionary_nanos;
            self.reduction_nanos += completed.reduction_nanos;
            if let Some(partials) = completed.partials.as_ref() {
                self.partial_chunks = self.partial_chunks.saturating_add(1);
                self.partial_values = self
                    .partial_values
                    .saturating_add(partials.active_values() as u64);
                self.partial_groups = self
                    .partial_groups
                    .saturating_add(partials.group_count() as u64);
            }
            states.native_numeric_accessor_work.add(&completed.work)?;
            if !states.update_compact_direct_from_accessors_with_partials(
                &[completed.accessor],
                &self.recipe.columns,
                None,
                completed.rows,
                completed.partials.as_ref(),
            )? {
                return Err(failed("prepared accessor lost the admitted consumer"));
            }
            Ok(())
        })?;
        self.consume_nanos += started.elapsed().as_nanos();
        Ok(())
    }

    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.consume_next(states)?;
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
    /// One provider lane shares the grant with the caller and one or two
    /// preparation workers. `provider_drivers` counts caller + drivers.
    pub(super) fn provider_parallelism(&self) -> Option<usize> {
        self.provider_progress.then_some(2)
    }
    pub(super) fn provider_cpu_scope(&self) -> &'static str {
        match (self.retired(), self.background_workers) {
            (true, 2) => {
                "same_prepared_source;dictionary_worker_joined_before_serial_consumer;existing_provider_driver_retained;shared_four_lane_CPU_grant;no_source_reopen_or_replay"
            }
            (false, 2) => {
                "same_prepared_source;one_caller_two_dictionary_workers_one_provider_driver;shared_four_lane_CPU_grant;no_source_reopen_or_replay"
            }
            (true, _) => {
                "same_prepared_source;dictionary_worker_joined_before_serial_consumer;existing_provider_driver_retained;shared_three_lane_CPU_grant;no_source_reopen_or_replay"
            }
            (false, _) => {
                "same_prepared_source;one_caller_one_dictionary_worker_one_provider_driver;shared_three_lane_CPU_grant;no_source_reopen_or_replay"
            }
        }
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
        object.insert("aggregate_timing_scope".into(), "disjoint_caller_scan_evidence_and_serial_accessor_updates_plus_finalization;dictionary_preparation_and_dense_reduction_workers_and_ordered_consumer_reported_separately;not_exclusive_CPU_or_complete_wall".into());
        if let Some(utf8) = object
            .get_mut("aggregate_utf8_chunk_accessor")
            .and_then(serde_json::Value::as_object_mut)
        {
            utf8.insert("scope".into(), "provider_caller_elapsed_plus_dictionary_worker_or_serial_elapsed;stages_may_overlap;provider_includes_deferred_IO_decompression_filter_and_canonicalization;source_backed_bytes_are_cumulative_logical_string_lengths;not_CPU_unique_allocation_or_RSS_bound".into());
        }
        object.insert("aggregate_dictionary_preparation_workers".into(), serde_json::json!({
            "rows": self.rows, "submitted_chunks": self.jobs.submitted(), "completed_chunks": self.jobs.joined(),
            "peak_outstanding_chunks": self.jobs.peak_outstanding(), "cpu_ceiling": 1 + self.background_workers + usize::from(self.provider_progress), "max_chunk_rows": MAX_ROWS,
            "dictionary_background_workers": self.background_workers, "window_limit_chunks": self.window, "provider_background_worker_grant": usize::from(self.provider_progress),
            "retired_to_same_serial_consumer": !self.active, "peak_task_reservation_bytes": self.peak_task_bytes,
            "provider_caller_nanos": u64::try_from(self.provider_nanos).unwrap_or(u64::MAX),
            "dictionary_worker_nanos": u64::try_from(self.dictionary_nanos).unwrap_or(u64::MAX),
            "reduction_worker_nanos": u64::try_from(self.reduction_nanos).unwrap_or(u64::MAX),
            "dictionary_partial_recipe_admitted": self.recipe.dense_plan.is_some(),
            "worker_dense_partial_chunks": self.partial_chunks,
            "worker_dense_partial_values": self.partial_values,
            "worker_dense_partial_groups": self.partial_groups,
            "ordered_consumer_caller_nanos": u64::try_from(self.consume_nanos).unwrap_or(u64::MAX),
            "join_wait_caller_nanos": u64::try_from(self.jobs.join_wait_nanos()).unwrap_or(u64::MAX),
            "scope": "same_first_seen_dictionary_builder_and_ordered_aggregate_consumer;dense_partial_keeps_dictionary_ID_update_order_and_extremum_source_indices;window_and_task_lease_retained_through_consumption;bounded_metadata_capacity_plus_native_nbytes_estimate;provider_allocations_and_global_state_separate;not_RSS_bound_or_exclusive_CPU"
        }));
        *summary = value.to_string();
        Ok(())
    }
}
