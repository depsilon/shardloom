//! Exact COUNT over complete triple-key partitions. Equality sorting is private;
//! final ordering always uses the existing typed numeric/minute/UTF8 comparator.

use super::{
    AggregateNumericMinuteStringKey as Key, AggregateStringInterner, GroupedAggregateStates,
    NumericMinuteStringAggregateOrderCandidate as Candidate,
    NumericMinuteStringGroupRoles as Roles, VortexLocalPrimitiveExecutionPolicy,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    aggregate_direct_column_accessors_from_chunk, aggregate_direct_utf8_dictionary_interner_ids,
    compare_numeric_minute_string_candidates, numeric_minute_string_worst_retained_candidate_index,
    triple_count_workers::{PARTITIONS, fill_keys, partition, roles, table_bytes},
};
use rustc_hash::FxHashMap;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{sync::Arc, time::Instant};
use vortex::array::{ArrayRef, dtype::DType};

#[cfg(test)]
type WorkerStartHook = Box<dyn FnOnce(&ChunkWorkerContext) + Send>;
#[cfg(test)]
thread_local! {
    pub(super) static WORKER_START_TEST_HOOK: std::cell::RefCell<Option<WorkerStartHook>> =
        const { std::cell::RefCell::new(None) };
}

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native triple-key partition reduction: {message}; no fallback execution was attempted"
    ))
}

fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("capacity byte overflow"))
}

struct Partition {
    keys: Vec<Key>,
    lease: MemoryLease,
    growths: u64,
    overlap_peak: u64,
    #[cfg(test)]
    deny_next_growth: bool,
}

impl Partition {
    fn reserve(&mut self, additional: usize) -> Result<()> {
        let need = self
            .keys
            .len()
            .checked_add(additional)
            .ok_or_else(|| failed("key length overflow"))?;
        if need <= self.keys.capacity() {
            return Ok(());
        }
        #[cfg(test)]
        if std::mem::take(&mut self.deny_next_growth) {
            self.lease.resize(u64::MAX)?;
        }
        // The existing numeric-pair policy: reserve old plus new capacity before
        // the allocator may move storage, then keep the observed vector capacity.
        let growth = self
            .keys
            .capacity()
            .checked_add(self.keys.capacity() / 4)
            .ok_or_else(|| failed("key growth overflow"))?;
        let capacity = need.max(growth).max(64);
        let overlap = self
            .lease
            .bytes()
            .checked_add(bytes::<Key>(capacity)?)
            .ok_or_else(|| failed("growth overlap overflow"))?;
        self.lease.resize(overlap)?;
        self.overlap_peak = self.overlap_peak.max(overlap);
        self.keys
            .try_reserve_exact(capacity - self.keys.len())
            .map_err(failed)?;
        if self.keys.capacity() > capacity {
            return Err(failed("vector exceeds admitted capacity"));
        }
        self.lease.resize(bytes::<Key>(self.keys.capacity())?)?;
        self.growths += 1;
        Ok(())
    }
}

struct Partial {
    retained: Vec<Candidate>,
    groups: usize,
    sort_nanos: u128,
    reduce_nanos: u128,
}

pub(super) struct SortedWorkers {
    // Cancel/join jobs before any remaining persistent input owners are dropped.
    jobs: AggregateChunkJobs<Partial>,
    partitions: Vec<Partition>,
    columns: Vec<String>,
    roles: Roles,
    cap: usize,
    _owner: MemoryLease,
    rows: u64,
    source_chunks: u64,
    groups: usize,
    accessor_nanos: u128,
    binding_nanos: u128,
    route_nanos: u128,
    sort_nanos: u128,
    reduce_nanos: u128,
    finish_nanos: u128,
    buffer_peak: u64,
    growths: u64,
    overlap_peak: u64,
    partition_rows: Vec<usize>,
    partition_capacities: Vec<usize>,
    partition_groups: Vec<usize>,
}

impl SortedWorkers {
    pub(super) fn admit(
        states: &GroupedAggregateStates<'_>,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Self>> {
        let Some(cap) = states
            .result_limit
            .filter(|&n| n > 0)
            .and_then(|n| states.request.offset.checked_add(n))
            .filter(|&n| n <= 128)
        else {
            return Ok(None);
        };
        let Some(roles) = roles(states, dtype, columns) else {
            return Ok(None);
        };
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let owner_bytes = size_of::<Self>()
            + PARTITIONS * (size_of::<Partition>() + 3 * size_of::<usize>())
            + columns
                .iter()
                .map(|c| c.len() + size_of::<String>())
                .sum::<usize>();
        let Ok(owner) = memory.reserve(u64::try_from(owner_bytes).map_err(failed)?) else {
            return Ok(None);
        };
        let mut partitions = Vec::new();
        partitions.try_reserve_exact(PARTITIONS).map_err(failed)?;
        for _ in 0..PARTITIONS {
            partitions.push(Partition {
                keys: Vec::new(),
                lease: memory.reserve(0)?,
                growths: 0,
                overlap_peak: 0,
                #[cfg(test)]
                deny_next_growth: false,
            });
        }
        let cancellation = cancellation.map_or_else(CancellationToken::default, |parent| {
            CancellationToken::from_shared_flag_with_parent(Arc::default(), parent)
        });
        Ok(Some(Self {
            jobs: AggregateChunkJobs::with_cancellation(
                parallelism,
                parallelism.saturating_mul(2).clamp(1, 24),
                memory.snapshot().limit_bytes,
                memory.clone(),
                cancellation,
            )?,
            partitions,
            columns: columns.to_vec(),
            roles,
            cap,
            _owner: owner,
            rows: 0,
            source_chunks: 0,
            groups: 0,
            accessor_nanos: 0,
            binding_nanos: 0,
            route_nanos: 0,
            sort_nanos: 0,
            reduce_nanos: 0,
            finish_nanos: 0,
            buffer_peak: 0,
            growths: 0,
            overlap_peak: 0,
            partition_rows: Vec::new(),
            partition_capacities: Vec::new(),
            partition_groups: Vec::new(),
        }))
    }

    pub(super) fn before_next(&self) -> Result<()> {
        self.jobs.check_cancelled()
    }
    pub(super) fn cancel(&self) {
        self.jobs.cancel();
    }
    #[cfg(test)]
    pub(super) fn has_committed_groups(&self) -> bool {
        self.rows > 0
    }
    #[cfg(test)]
    pub(super) fn deny_next_state_growth_for_test(&mut self) {
        for partition in &mut self.partitions {
            partition.deny_next_growth = true;
        }
    }

    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        self.jobs.check_cancelled()?;
        let started = Instant::now();
        let accessors = aggregate_direct_column_accessors_from_chunk(
            chunk,
            &self.columns,
            &mut states.native_execution_ctx,
        )?;
        self.accessor_nanos += started.elapsed().as_nanos();
        states
            .native_numeric_accessor_work
            .add(&accessors.numeric_work)?;
        states.observe_aggregate_accessors(&self.columns, &accessors);
        let key_accessors = [
            &accessors[self.roles.numeric_column],
            &accessors[self.roles.minute_column],
            &accessors[self.roles.string_column],
        ];
        if key_accessors
            .iter()
            .any(|column| column.len() != chunk.len())
        {
            return Err(failed("key columns have different lengths"));
        }
        let started = Instant::now();
        let ids = aggregate_direct_utf8_dictionary_interner_ids(
            key_accessors[2],
            &mut states.string_interner,
        )?;
        self.binding_nanos += started.elapsed().as_nanos();
        let started = Instant::now();
        let staging_bytes = bytes::<Key>(chunk.len())?
            .checked_add((PARTITIONS * size_of::<usize>()) as u64)
            .ok_or_else(|| failed("staging byte overflow"))?;
        let staging_lease = self.jobs.memory().reserve(staging_bytes)?;
        self.buffer_peak = self.buffer_peak.max(staging_bytes);
        let mut keys = Vec::new();
        keys.try_reserve_exact(chunk.len()).map_err(failed)?;
        if keys.capacity() > chunk.len() {
            return Err(failed("staging exceeds admitted capacity"));
        }
        let mut counts = [0; PARTITIONS];
        fill_keys(
            key_accessors,
            ids.as_deref(),
            self.roles.minute_column_prepared,
            &mut states.string_interner,
            &mut keys,
            &mut counts,
        )?;
        self.jobs.check_cancelled()?;
        for (partition, count) in self.partitions.iter_mut().zip(counts) {
            partition.reserve(count)?;
        }
        for (index, &key) in keys.iter().enumerate() {
            if index % 1024 == 0 {
                self.jobs.check_cancelled()?;
            }
            self.partitions[partition(key)].keys.push(key);
        }
        self.rows = self
            .rows
            .checked_add(u64::try_from(keys.len()).map_err(failed)?)
            .ok_or_else(|| failed("row count overflow"))?;
        self.source_chunks += 1;
        drop(keys);
        drop(staging_lease);
        self.route_nanos += started.elapsed().as_nanos();
        states.numeric_minute_string_group_roles = Some(self.roles);
        states.numeric_minute_string_dictionary_code_reuse |= ids.is_some();
        states.numeric_minute_string_count_direct_updates = true;
        states.count_star_direct_updates = true;
        Ok(true)
    }

    fn join_next(
        &mut self,
        retained: &mut Vec<Candidate>,
        worst: &mut Option<usize>,
        interner: &AggregateStringInterner,
    ) -> Result<()> {
        if let Some(completed) = self.jobs.join_next()? {
            completed.consume_owned(|partial| {
                self.groups = self
                    .groups
                    .checked_add(partial.groups)
                    .ok_or_else(|| failed("group count overflow"))?;
                self.partition_groups.push(partial.groups);
                self.sort_nanos += partial.sort_nanos;
                self.reduce_nanos += partial.reduce_nanos;
                for candidate in partial.retained {
                    consider(retained, worst, candidate, self.cap, interner);
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.jobs.check_cancelled()?;
        let started = Instant::now();
        let mut selected_lease = self.jobs.memory().reserve(
            table_bytes(self.cap)?
                .checked_add(bytes::<Candidate>(self.cap)?)
                .ok_or_else(|| failed("selection byte overflow"))?,
        )?;
        let mut retained = Vec::new();
        retained.try_reserve_exact(self.cap).map_err(failed)?;
        if retained.capacity() > self.cap {
            return Err(failed("selection exceeds admitted capacity"));
        }
        let mut worst = None;
        // Freeze the single producer domain, sharing its existing owner. Neither
        // string values nor the interner directory are cloned or reconstructed.
        let interner = Arc::new(std::mem::take(&mut states.string_interner));
        for partition in std::mem::take(&mut self.partitions) {
            self.jobs.check_cancelled()?;
            self.partition_rows.push(partition.keys.len());
            self.partition_capacities.push(partition.keys.capacity());
            self.growths += partition.growths;
            self.overlap_peak = self.overlap_peak.max(partition.overlap_peak);
            while self.jobs.is_full() {
                self.join_next(&mut retained, &mut worst, &interner)?;
            }
            let cap = self.cap.min(partition.keys.len());
            let domain = Arc::clone(&interner);
            #[cfg(test)]
            let start_hook = WORKER_START_TEST_HOOK.with(|hook| hook.borrow_mut().take());
            self.jobs.submit(
                bytes::<Candidate>(cap)?
                    .checked_add(size_of::<Partial>() as u64)
                    .ok_or_else(|| failed("partial byte overflow"))?,
                move |worker, _| {
                    #[cfg(test)]
                    if let Some(hook) = start_hook {
                        hook(worker);
                    }
                    reduce(partition, cap, &domain, worker)
                },
            )?;
        }
        while self.jobs.outstanding() > 0 {
            self.join_next(&mut retained, &mut worst, &interner)?;
        }
        self.jobs.check_cancelled()?;
        self.jobs.retire()?;
        states.string_interner = Arc::try_unwrap(interner)
            .map_err(|_| failed("completed workers retained string domain"))?;
        let mut selected = FxHashMap::default();
        selected.try_reserve(retained.len()).map_err(failed)?;
        for candidate in &retained {
            selected.insert(candidate.key, candidate.count);
        }
        drop(retained);
        selected_lease.resize(table_bytes(self.cap)?)?;
        states.complete_key_partition_group_count = Some(self.groups);
        states.numeric_minute_string_count_groups = Some(selected);
        states.numeric_minute_string_group_roles = Some(self.roles);
        states.complete_key_selected_lease = Some(selected_lease);
        states.numeric_minute_string_count_direct_updates = true;
        states.count_star_direct_updates = true;
        self.finish_nanos += started.elapsed().as_nanos();
        Ok(())
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(failed)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        let memory = self.jobs.memory().snapshot();
        object.extend(serde_json::json!({
            "aggregate_workers_family": "complete_numeric_minute_string_partition_sort_reduce",
            "aggregate_workers_triple_source_chunks": self.source_chunks,
            "aggregate_workers_triple_submitted_partition_tasks": self.jobs.submitted(),
            "aggregate_workers_triple_joined_partition_tasks": self.jobs.joined(),
            "aggregate_workers_shared_live_peak_bytes": memory.peak_reserved_bytes,
            "aggregate_workers_shared_live_bytes": memory.reserved_bytes,
            "aggregate_workers_shared_live_limit_bytes": memory.limit_bytes,
            "aggregate_workers_shared_live_denied_reservations": memory.denied_reservations,
            "aggregate_workers_triple_rows": self.rows,
            "aggregate_workers_triple_inserted_keys": self.groups,
            "aggregate_workers_triple_existing_key_updates": self.rows.saturating_sub(self.groups as u64),
            "aggregate_workers_triple_partition_rows": self.partition_rows,
            "aggregate_workers_triple_partition_groups": self.partition_groups,
            "aggregate_workers_triple_partition_capacities": self.partition_capacities,
            "aggregate_workers_triple_growths": self.growths,
            "aggregate_workers_triple_growth_overlap_peak_bytes": self.overlap_peak,
            "aggregate_workers_triple_buffer_peak_bytes": self.buffer_peak,
            "aggregate_workers_triple_accessor_nanos": self.accessor_nanos,
            "aggregate_workers_triple_binding_nanos": self.binding_nanos,
            "aggregate_workers_triple_routing_submit_nanos": self.route_nanos,
            "aggregate_workers_triple_sort_nanos": self.sort_nanos,
            "aggregate_workers_triple_reduce_nanos": self.reduce_nanos,
            "aggregate_workers_triple_join_wait_nanos": self.jobs.join_wait_nanos(),
            "aggregate_workers_triple_finish_nanos": self.finish_nanos,
            "aggregate_workers_triple_memory_scope": "persistent_vector_capacity_growth_overlap_staging_and_selected_candidates;producer_interner_and_provider_accessor_allocations_separate;not_RSS_or_allocator_enforcement",
            "aggregate_workers_triple_pressure_policy": "cancel_drain_fail;no_serial_or_spill_replay",
            "aggregate_workers_triple_counter_scope": "complete_keys_sorted_and_adjacent_equality_counted;worker_spans_overlap;source_chunks_exclude_partition_jobs;sort_cancellation_checked_before_and_after_each_partition"
        }).as_object().ok_or_else(|| failed("evidence is not an object"))?.clone());
        *summary = value.to_string();
        Ok(())
    }
}

fn consider(
    retained: &mut Vec<Candidate>,
    worst: &mut Option<usize>,
    candidate: Candidate,
    cap: usize,
    interner: &AggregateStringInterner,
) {
    if retained.len() < cap {
        retained.push(candidate);
        if retained.len() == cap {
            *worst = numeric_minute_string_worst_retained_candidate_index(retained, interner);
        }
    } else if let Some(index) = *worst
        && compare_numeric_minute_string_candidates(&candidate, &retained[index], interner).is_lt()
    {
        retained[index] = candidate;
        *worst = numeric_minute_string_worst_retained_candidate_index(retained, interner);
    }
}

fn reduce(
    mut partition: Partition,
    cap: usize,
    interner: &AggregateStringInterner,
    worker: &ChunkWorkerContext,
) -> Result<Partial> {
    worker.check_cancelled()?;
    let started = Instant::now();
    partition
        .keys
        .sort_unstable_by_key(|key| (key.key_kinds, key.numeric_bits, key.string_id));
    let sort_nanos = started.elapsed().as_nanos();
    worker.check_cancelled()?;
    let started = Instant::now();
    let mut retained = Vec::new();
    retained.try_reserve_exact(cap).map_err(failed)?;
    if retained.capacity() > cap {
        return Err(failed("partial selection exceeds admitted capacity"));
    }
    let (mut worst, mut groups, mut begin) = (None, 0usize, 0usize);
    while begin < partition.keys.len() {
        if groups % 1024 == 0 {
            worker.check_cancelled()?;
        }
        let key = partition.keys[begin];
        let mut end = begin + 1;
        while end < partition.keys.len() && partition.keys[end] == key {
            if end % 1024 == 0 {
                worker.check_cancelled()?;
            }
            end += 1;
        }
        let count = u64::try_from(end - begin).map_err(failed)?;
        consider(
            &mut retained,
            &mut worst,
            Candidate { key, count },
            cap,
            interner,
        );
        groups += 1;
        begin = end;
    }
    Ok(Partial {
        retained,
        groups,
        sort_nanos,
        reduce_nanos: started.elapsed().as_nanos(),
    })
}
