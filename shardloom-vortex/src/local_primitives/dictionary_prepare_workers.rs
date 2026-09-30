//! Prepare source-backed UTF8 dictionaries in a bounded queue; consume unchanged
//! aggregate accessors in source order on the caller.

use super::{
    AggregateUtf8DictionarySource, AggregateValueTransform, GroupedAggregateStates,
    NativeNumericAccessorWork, TransformedDictionaryDenseGeneralPlan,
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveResourceEnvelope,
    VortexQueryPrimitiveRequest, aggregate_canonical_utf8_chunk,
    aggregate_chunk_jobs::{AggregateChunkJobs, SubmitOutcome},
    aggregate_utf8_accessor_from_canonical, logical_field_from_native_array,
    required_simple_aggregate,
    utf8_chunk_dictionary::Utf8ChunkDictionary,
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{sync::Arc, time::Instant};
use vortex::array::{
    ArrayRef,
    arrays::Dict,
    dtype::{DType, Nullability},
};

const MAX_ROWS: usize = 262_144;
const WINDOW: usize = 2;

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
}

struct Completed {
    accessor: super::AggregateDirectColumnAccessor,
    work: NativeNumericAccessorWork,
    rows: usize,
}

pub(super) struct DictionaryPrepareWorkers {
    jobs: AggregateChunkJobs<Completed>,
    recipe: Arc<Recipe>,
    active: bool,
    rows: u64,
    provider_nanos: u128,
    dictionary_nanos: u128,
    consume_nanos: u128,
    peak_task_bytes: u64,
    provider_progress: bool,
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
                2,
                WINDOW,
                memory.snapshot().limit_bytes,
                memory.clone(),
                cancellation.map_or_else(CancellationToken::default, |parent| {
                    CancellationToken::from_shared_flag_with_parent(Arc::default(), parent)
                }),
            )?,
            recipe: Arc::new(Recipe {
                columns: columns.to_vec(),
            }),
            active: true,
            rows: 0,
            provider_nanos: 0,
            dictionary_nanos: 0,
            consume_nanos: 0,
            peak_task_bytes: 0,
            provider_progress: parallelism >= 3,
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
        let utf8 = aggregate_canonical_utf8_chunk(&array, &mut work.utf8)?;
        self.provider_nanos += work.utf8.provider_nanos;
        if utf8.len() != chunk.len() || utf8.dtype() != array.dtype() {
            return Err(failed("canonical source changed rows or dtype"));
        }
        let rows = utf8.len();
        let bytes = Utf8ChunkDictionary::bounded_capacity_bytes(rows)?
            .checked_add(utf8.nbytes())
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
        let input = utf8.clone();
        let mut worker_work = work.clone();
        #[cfg(test)]
        let start_hook = WORKER_START_TEST_HOOK.with(|hook| hook.borrow_mut().take());
        let outcome = self.jobs.try_submit(bytes, move |worker, _lease| {
            worker.check_cancelled()?;
            #[cfg(test)]
            if let Some(hook) = start_hook {
                hook(worker);
            }
            worker.check_cancelled()?;
            let allocation_started = Instant::now();
            let dictionary = Utf8ChunkDictionary::with_row_capacity(rows)?;
            worker_work.utf8.dictionary_nanos += allocation_started.elapsed().as_nanos();
            let accessor = aggregate_utf8_accessor_from_canonical(
                &recipe.columns[0],
                &input,
                source,
                &mut worker_work.utf8,
                dictionary,
                &|| worker.check_cancelled(),
            )?;
            Ok(Completed {
                accessor,
                work: worker_work,
                rows,
            })
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
            states.native_numeric_accessor_work.add(&completed.work)?;
            if !states.update_compact_direct_from_accessors(
                &[completed.accessor],
                &self.recipe.columns,
                None,
                completed.rows,
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
    /// One native provider lane shares the same three-lane admission: caller,
    /// dictionary worker, provider. `provider_drivers` counts caller + drivers.
    pub(super) fn provider_parallelism(&self) -> Option<usize> {
        self.provider_progress.then_some(2)
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
        object.insert("aggregate_timing_scope".into(), "disjoint_caller_scan_evidence_and_serial_accessor_updates_plus_finalization;dictionary_preparation_worker_and_ordered_consumer_reported_separately;not_exclusive_CPU_or_complete_wall".into());
        if let Some(utf8) = object
            .get_mut("aggregate_utf8_chunk_accessor")
            .and_then(serde_json::Value::as_object_mut)
        {
            utf8.insert("scope".into(), "provider_caller_elapsed_plus_dictionary_worker_or_serial_elapsed;stages_may_overlap;provider_includes_deferred_IO_decompression_filter_and_canonicalization;source_backed_bytes_are_cumulative_logical_string_lengths;not_CPU_unique_allocation_or_RSS_bound".into());
        }
        object.insert("aggregate_dictionary_preparation_workers".into(), serde_json::json!({
            "rows": self.rows, "submitted_chunks": self.jobs.submitted(), "completed_chunks": self.jobs.joined(),
            "peak_outstanding_chunks": self.jobs.peak_outstanding(), "cpu_ceiling": 2 + usize::from(self.provider_progress), "max_chunk_rows": MAX_ROWS,
            "dictionary_background_workers": 1, "provider_background_worker_grant": usize::from(self.provider_progress),
            "retired_to_same_serial_consumer": !self.active, "peak_task_reservation_bytes": self.peak_task_bytes,
            "provider_caller_nanos": u64::try_from(self.provider_nanos).unwrap_or(u64::MAX),
            "dictionary_worker_nanos": u64::try_from(self.dictionary_nanos).unwrap_or(u64::MAX),
            "ordered_consumer_caller_nanos": u64::try_from(self.consume_nanos).unwrap_or(u64::MAX),
            "join_wait_caller_nanos": u64::try_from(self.jobs.join_wait_nanos()).unwrap_or(u64::MAX),
            "scope": "same_first_seen_dictionary_builder_and_ordered_aggregate_consumer;window_and_task_lease_retained_through_consumption;exact_metadata_capacity_plus_native_nbytes_estimate;provider_allocations_and_global_state_separate;not_RSS_bound_or_exclusive_CPU"
        }));
        *summary = value.to_string();
        Ok(())
    }
}
