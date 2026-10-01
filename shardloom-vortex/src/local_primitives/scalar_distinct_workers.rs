//! Bounded scalar COUNT(DISTINCT UTF8) ingestion and exact partition union.
//! The caller owns source progress; workers never retain provider strings in
//! global state. An admitted failure aborts; no partial cardinality is exposed.

use super::{
    AggregateDirectColumnAccessor, AggregateUtf8DictionarySource, AggregateValueTransform,
    SimpleAggregateStates, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext, SubmitOutcome},
    scalar_distinct_partitions::{Partitions, failed},
    utf8_chunk_dictionary::{Utf8ChunkDictionary, exact_row_vec},
    vortex_error,
};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{hash::Hasher as _, sync::Arc, time::Instant};
use vortex::array::{
    ArrayRef,
    arrays::{
        Dict, VarBinViewArray, dict::DictArraySlotsExt as _, varbinview::VarBinViewArrayExt as _,
    },
    dtype::{DType, Nullability},
};
use vortex::session::VortexSession;

#[derive(Default)]
struct Work {
    rows: u64,
    entries: u64,
    copied_bytes: u64,
    dictionary_chunks: u64,
    provider_nanos: u64,
    dictionary_nanos: u64,
    mark_nanos: u64,
    union_nanos: u64,
}

impl Work {
    fn add(&mut self, other: &Self) -> Result<()> {
        macro_rules! add {
            ($($field:ident),+ $(,)?) => { $(self.$field = self.$field.checked_add(other.$field)
                .ok_or_else(|| failed("work counter overflow"))?;)+ };
        }
        add!(
            rows,
            entries,
            copied_bytes,
            dictionary_chunks,
            provider_nanos,
            dictionary_nanos,
            mark_nanos,
            union_nanos
        );
        Ok(())
    }
}

enum Input {
    Canonical(VarBinViewArray),
    Dictionary {
        values: VarBinViewArray,
        codes: Vec<u32>,
        nulls: Option<Vec<bool>>,
    },
}

impl Input {
    fn values(&self) -> &VarBinViewArray {
        match self {
            Self::Canonical(values) | Self::Dictionary { values, .. } => values,
        }
    }
    fn capacity_bytes(&self) -> Result<u64> {
        let rows = self.values().len();
        let scratch = Utf8ChunkDictionary::bounded_capacity_bytes(rows)?;
        let entries = rows
            .checked_mul(size_of::<(u64, usize)>() + size_of::<bool>())
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("task scratch overflow"))?;
        let code_bytes = match self {
            Self::Canonical(_) => 0,
            Self::Dictionary { codes, nulls, .. } => codes
                .capacity()
                .checked_mul(size_of::<u32>())
                .and_then(|n| n.checked_add(nulls.as_ref().map_or(0, Vec::capacity)))
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| failed("code capacity overflow"))?,
        };
        scratch
            .checked_add(entries)
            .and_then(|n| n.checked_add(code_bytes))
            .and_then(|n| n.checked_add(self.values().nbytes()))
            .and_then(|n| n.checked_add(size_of::<Work>() as u64))
            .ok_or_else(|| failed("task bytes overflow"))
    }
}

pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    if request.predicate.is_some()
        || request.source_order_limit.is_some()
        || !aggregate.group_by.is_empty()
        || !aggregate.group_expressions.is_empty()
        || aggregate.spill.is_some()
    {
        return false;
    }
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|v| v.as_str().to_owned())
        .collect::<Vec<_>>();
    SimpleAggregateStates::new(aggregate, &columns).is_ok_and(|states| shape(&states, &columns))
}

fn shape(states: &SimpleAggregateStates, columns: &[String]) -> bool {
    columns.len() == 1
        && states
            .single_count_distinct_alias_and_column()
            .is_some_and(|(_, index)| index == 0)
        && states.states[0].argument_offset.is_none()
        && matches!(
            states.states[0].value_transform,
            AggregateValueTransform::Identity
        )
        && states.states[0].distinct_values.is_empty()
        && !states.partition_distinct_completed
}

pub(super) struct ScalarDistinctWorkers {
    jobs: AggregateChunkJobs<Work>,
    partitions: Option<Arc<Partitions>>,
    column: String,
    session: VortexSession,
    parallelism: usize,
    work: Work,
    cardinality: Option<u64>,
    peak_reserved: u64,
    _metadata: MemoryLease,
}

impl ScalarDistinctWorkers {
    pub(super) fn admit(
        states: &SimpleAggregateStates,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Self>> {
        if !shape(states, columns) {
            return Ok(None);
        }
        let DType::Struct(fields, Nullability::NonNullable) = dtype else {
            return Ok(None);
        };
        if !fields
            .field(columns[0].as_str())
            .is_some_and(|dtype| matches!(dtype, DType::Utf8(_)))
        {
            return Ok(None);
        }
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        if parallelism < 2 {
            return Ok(None);
        }
        let window = parallelism.saturating_mul(2).clamp(2, 24);
        let metadata_bytes = size_of::<Self>()
            .checked_add(window * 256)
            .and_then(|n| n.checked_add(columns[0].len()))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("worker metadata overflow"))?;
        let Ok(metadata) = memory.reserve(metadata_bytes) else {
            return Ok(None);
        };
        let Ok(partitions) = Partitions::new(memory) else {
            return Ok(None);
        };
        let token = cancellation.map_or_else(CancellationToken::default, |parent| {
            CancellationToken::from_shared_flag_with_parent(Arc::default(), parent)
        });
        Ok(Some(Self {
            jobs: AggregateChunkJobs::with_cancellation(
                parallelism,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
                token,
            )?,
            partitions: Some(Arc::new(partitions)),
            column: columns[0].clone(),
            session: session.clone(),
            parallelism,
            work: Work::default(),
            cardinality: None,
            peak_reserved: 0,
            _metadata: metadata,
        }))
    }

    pub(super) fn before_next(&mut self) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.jobs.is_full() {
            self.consume_next()?;
        }
        Ok(())
    }

    pub(super) fn submit(&mut self, chunk: &ArrayRef) -> Result<()> {
        self.jobs.check_cancelled()?;
        let started = Instant::now();
        let array = super::logical_field_from_native_array(chunk, &self.column)?;
        let mut ctx = super::native_numeric_execution_ctx(&self.session);
        let array =
            super::encoded_numeric_reduction::resolve_structural_projection(&array, &mut ctx)?;
        if !matches!(array.dtype(), DType::Utf8(_)) || array.len() != chunk.len() {
            return Err(failed("source changed admitted dtype or row count"));
        }
        // Bound code/remapping vectors while the caller prepares the native
        // owner. Keep this lease through task admission, including its overlap.
        let preparation_bytes = array
            .len()
            .checked_mul(32)
            .and_then(|n| n.checked_add(size_of::<Input>()))
            .and_then(|n| n.checked_add(self.column.len()))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("preparation capacity overflow"))?;
        while self.jobs.outstanding() != 0 && self.available_bytes() < preparation_bytes {
            self.consume_next()?;
        }
        let preparation_lease = self.jobs.memory().reserve(preparation_bytes)?;
        let input = if let Some(dict) = array.as_opt::<Dict>()
            && let Some((mut codes, nulls)) =
                super::direct_u32_codes_with_nulls_from_vortex_array(dict.codes())
        {
            let selected = super::dictionary_handoff::referenced_values(
                dict.values(),
                &mut codes,
                nulls.as_deref(),
            )?;
            let values = selected
                .execute::<VarBinViewArray>(&mut ctx)
                .map_err(vortex_error)?;
            Input::Dictionary {
                values,
                codes,
                nulls,
            }
        } else {
            let values = array
                .clone()
                .execute::<VarBinViewArray>(&mut ctx)
                .map_err(vortex_error)?;
            if values.len() != chunk.len() {
                return Err(failed("canonical source changed row count"));
            }
            Input::Canonical(values)
        };
        let work = Work {
            rows: chunk.len() as u64,
            provider_nanos: nanos(started),
            ..Work::default()
        };
        let bytes = input
            .capacity_bytes()?
            .checked_add(self.column.len() as u64)
            .ok_or_else(|| failed("task column metadata overflow"))?;
        // Completed receipts still own their task leases. Release them before
        // admitting another task, never hold a partition lock while joining.
        while self.jobs.outstanding() != 0 && self.available_bytes() < bytes {
            self.consume_next()?;
        }
        let partitions = self
            .partitions
            .as_ref()
            .ok_or_else(|| failed("union already finalized"))?
            .clone();
        let column = self.column.clone();
        let session = self.session.clone();
        match self.jobs.try_submit(bytes, move |worker, _lease| {
            process(input, &column, &session, &partitions, worker, work)
        })? {
            SubmitOutcome::Submitted(_) => {}
            SubmitOutcome::InitialCapacityDenied(error) => return Err(error),
        }
        drop(preparation_lease);
        self.peak_reserved = self
            .peak_reserved
            .max(self.jobs.memory().snapshot().peak_reserved_bytes);
        Ok(())
    }

    fn consume_next(&mut self) -> Result<()> {
        let Some(done) = self.jobs.join_next()? else {
            return Err(failed("missing completed chunk"));
        };
        done.consume(|work| self.work.add(work))
    }

    fn available_bytes(&self) -> u64 {
        let snapshot = self.jobs.memory().snapshot();
        snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes)
    }

    pub(super) fn finish(&mut self, states: &mut SimpleAggregateStates) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.consume_next()?;
        }
        self.jobs.retire()?;
        if !shape(states, std::slice::from_ref(&self.column)) {
            return Err(failed("scalar state changed before finalization"));
        }
        let parts = self
            .partitions
            .take()
            .ok_or_else(|| failed("union already finalized"))?;
        let cardinality = parts.cardinality()?;
        self.peak_reserved = self
            .peak_reserved
            .max(self.jobs.memory().snapshot().peak_reserved_bytes);
        drop(parts);
        states.states[0].count = cardinality;
        states.partition_distinct_completed = true;
        states.direct_scalar_updates = true;
        states.direct_distinct_updates = true;
        self.cardinality = Some(cardinality);
        Ok(())
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(vortex_error)?;
        value["aggregate_scalar_distinct_workers"] = serde_json::json!({
            "strategy": "bounded_native_utf8_partition_exact", "chunks": self.jobs.joined(),
            "rows": self.work.rows, "partial_entries": self.work.entries,
            "cardinality": self.cardinality, "copied_persistent_bytes": self.work.copied_bytes,
            "copy_scope": "copied_persistent_bytes_counts_unique_key_bytes_on_insert;arena_resize_copies_excluded",
            "native_dictionary_chunks": self.work.dictionary_chunks,
            "provider_nanos": self.work.provider_nanos, "dictionary_nanos": self.work.dictionary_nanos,
            "used_code_mark_nanos": self.work.mark_nanos, "union_nanos": self.work.union_nanos,
            "parallelism_including_caller": self.parallelism, "peak_outstanding": self.jobs.peak_outstanding(),
            "worker_busy_nanos": self.jobs.worker_busy_nanos(), "join_wait_nanos": self.jobs.join_wait_nanos(),
            "shared_pool_peak_reserved_bytes": self.peak_reserved,
            "memory_scope": "owned_native_buffers_and_reserved_task_partition_capacity;provider_allocations_outside_HostAllocator_excluded;not_RSS",
            "cpu_scope": "one_source_caller_plus_admitted_compute_workers;no_provider_background_pool",
            "timing_scope": "provider_nanos_is_caller_preparation_including_admission;dictionary_mark_union_are_summed_worker_elapsed_and_can_overlap_caller_and_each_other;not_CPU_or_additive_query_wall_time",
        });
        *summary = value.to_string();
        Ok(())
    }
}

fn process(
    input: Input,
    column: &str,
    session: &VortexSession,
    parts: &Partitions,
    worker: &ChunkWorkerContext,
    mut work: Work,
) -> Result<Work> {
    worker.check_cancelled()?;
    match input {
        Input::Canonical(values) => {
            let started = Instant::now();
            let mut dictionary_work = super::native_numeric_accessor::Utf8AccessorWork::default();
            let dictionary = Utf8ChunkDictionary::with_row_capacity(values.len())?;
            let accessor = super::aggregate_utf8_accessor_from_canonical(
                column,
                &values,
                AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
                &mut dictionary_work,
                dictionary,
                &|| worker.check_cancelled(),
            )?;
            work.dictionary_nanos = nanos(started);
            let AggregateDirectColumnAccessor::Utf8Dictionary { values, .. } = accessor else {
                return Err(failed("chunk dictionary missing"));
            };
            // Constructed dictionaries contain only referenced nonnull values;
            // no second row/code pass or per-key occurrence count is needed.
            let mut entries =
                hashed_entries(values.len(), |index| values[index].as_bytes(), worker)?;
            work.entries = entries.len() as u64;
            let started = Instant::now();
            work.copied_bytes =
                parts.union(&mut entries, |index| values[index].as_bytes(), worker)?;
            work.union_nanos = nanos(started);
        }
        Input::Dictionary {
            values,
            codes,
            nulls,
        } => {
            work.dictionary_chunks = 1;
            let started = Instant::now();
            let mut ctx = super::native_numeric_execution_ctx(session);
            let valid = values
                .varbinview_validity()
                .execute_mask(values.len(), &mut ctx)
                .map_err(vortex_error)?;
            if valid.len() != values.len() || nulls.as_ref().is_some_and(|v| v.len() != codes.len())
            {
                return Err(failed("dictionary validity length mismatch"));
            }
            let mut used = exact_row_vec::<bool>(values.len())?;
            used.resize(values.len(), false);
            for (row, &code) in codes.iter().enumerate() {
                if row.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if nulls.as_ref().is_some_and(|v| v[row]) {
                    continue;
                }
                let index = code as usize;
                let slot = used
                    .get_mut(index)
                    .ok_or_else(|| failed("dictionary code exceeds values"))?;
                if valid.value(index) {
                    *slot = true;
                }
            }
            work.mark_nanos = nanos(started);
            let mut entries = exact_row_vec(values.len())?;
            for (index, used) in used.into_iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if used {
                    let bytes = super::native_utf8::borrowed_bytes(&values, index);
                    std::str::from_utf8(bytes).map_err(|error| {
                        failed(&format!("used dictionary value has invalid UTF8: {error}"))
                    })?;
                    entries.push((hash(bytes), index));
                }
            }
            work.entries = entries.len() as u64;
            let started = Instant::now();
            work.copied_bytes = parts.union(
                &mut entries,
                |index| super::native_utf8::borrowed_bytes(&values, index),
                worker,
            )?;
            work.union_nanos = nanos(started);
        }
    }
    worker.check_cancelled()?;
    Ok(work)
}

fn hashed_entries<'a>(
    len: usize,
    bytes: impl Fn(usize) -> &'a [u8],
    worker: &ChunkWorkerContext,
) -> Result<Vec<(u64, usize)>> {
    let mut entries = exact_row_vec(len)?;
    for index in 0..len {
        if index.is_multiple_of(4096) {
            worker.check_cancelled()?;
        }
        entries.push((hash(bytes(index)), index));
    }
    Ok(entries)
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    hasher.write(bytes);
    hasher.finish()
}
fn nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "scalar_distinct_workers_tests.rs"]
mod tests;
