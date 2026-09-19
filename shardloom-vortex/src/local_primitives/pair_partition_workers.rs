//! Exact complete-partition counts for the existing numeric-pair late-measure
//! route. Sampling chooses ownership, never an answer or a uniqueness proof.

use super::{
    AggregateNumericPairKey as Key, AggregateValueTransform, GroupedAggregateStates,
    NumericPairAggregateOrderCandidate as Candidate, NumericPairNearUniqueCountDirectory,
    SimpleAggregateFunction, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveResourceEnvelope, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    aggregate_direct_column_accessors_from_chunk, aggregate_numeric_pair_direct_key_slices,
    numeric_pair_near_unique_directory_admitted, required_simple_aggregate,
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::time::Instant;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
};

const PARTITIONS: usize = 64;

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native numeric-pair partition reduction: {message}; no fallback execution was attempted"
    ))
}

fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("capacity byte overflow"))
}

fn retained_cap(states: &GroupedAggregateStates<'_>) -> Option<usize> {
    states
        .result_limit
        .filter(|&n| n > 0)
        .and_then(|n| states.request.offset.checked_add(n))
        .filter(|&n| n <= 128)
}

fn shape(states: &GroupedAggregateStates<'_>) -> bool {
    let Some(specs) = states.compact_measure_specs.as_ref() else {
        return false;
    };
    let Some(alias) = states.state_template.count_star_measure_alias() else {
        return false;
    };
    states.numeric_pair_late_measure_enabled
        && states.request.spill.is_none()
        && states.request.having.is_empty()
        && states.request.group_expressions.is_empty()
        && states.group_key_indices.len() == 2
        && states.group_columns.len() == 2
        && retained_cap(states).is_some()
        && matches!(states.request.order_by.as_slice(), [order] if order.descending && order.column == alias)
        && specs.iter().all(|s| {
            matches!(s.value_transform, AggregateValueTransform::Identity)
                && matches!(
                    s.function,
                    SimpleAggregateFunction::Count
                        | SimpleAggregateFunction::Sum
                        | SimpleAggregateFunction::Avg
                )
        })
        && specs.iter().any(|s| {
            matches!(
                s.function,
                SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
            )
        })
        && states.groups.is_empty()
        && states.group_order.is_empty()
        && states.numeric_pair_late_measure_count_groups.is_none()
        && states
            .numeric_pair_late_measure_near_unique_directory
            .is_none()
        && states.numeric_pair_late_measure_retained_keys.is_none()
}

fn roles(
    states: &GroupedAggregateStates<'_>,
    dtype: &DType,
    columns: &[String],
) -> Option<[usize; 2]> {
    if !shape(states) {
        return None;
    }
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return None;
    };
    let mut result = [0; 2];
    for (slot, &group) in states.group_key_indices.iter().enumerate() {
        let column = states.group_columns.get(group)?;
        if !matches!(column.transform, AggregateValueTransform::Identity)
            || !column.extra_column_indices.is_empty()
        {
            return None;
        }
        if !matches!(fields.field(columns.get(column.column_index)?.as_str()),
            Some(DType::Primitive(p, Nullability::NonNullable)) if p.is_int())
        {
            return None;
        }
        result[slot] = column.column_index;
    }
    Some(result)
}

pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    if request.predicate.is_some() {
        return false;
    }
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    if aggregate.group_by.len() != 2 || !aggregate.group_expressions.is_empty() {
        return false;
    }
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|c| c.as_str().to_owned())
        .collect::<Vec<_>>();
    let Ok(envelope) = VortexLocalPrimitiveResourceEnvelope::new(1, 1) else {
        return false;
    };
    GroupedAggregateStates::new_with_resource_envelope(
        aggregate,
        request.source_order_limit,
        &columns,
        true,
        false,
        envelope,
    )
    .is_ok_and(|states| shape(&states))
}

pub(super) fn request_schema_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
) -> bool {
    if !request_may_be_admitted(request) {
        return false;
    }
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|c| c.as_str().to_owned())
        .collect::<Vec<_>>();
    let Ok(envelope) = VortexLocalPrimitiveResourceEnvelope::new(1, 1) else {
        return false;
    };
    GroupedAggregateStates::new_with_resource_envelope(
        aggregate,
        request.source_order_limit,
        &columns,
        true,
        false,
        envelope,
    )
    .is_ok_and(|states| roles(&states, dtype, &columns).is_some())
}

fn partition(key: Key) -> usize {
    let mut value =
        key.first_bits ^ key.second_bits.rotate_left(23) ^ u64::from(key.key_kinds).rotate_left(47);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    usize::try_from((value ^ (value >> 31)) & (PARTITIONS as u64 - 1)).expect("six-bit partition")
}

struct Partition {
    keys: Vec<Key>,
    // The payload drops before its capacity credit.
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
        let growth = self
            .keys
            .capacity()
            .checked_add(self.keys.capacity() / 4)
            .ok_or_else(|| failed("key growth overflow"))?;
        let capacity = need.max(growth).max(64);
        let next = bytes::<Key>(capacity)?;
        let overlap = self
            .lease
            .bytes()
            .checked_add(next)
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

pub(super) struct PairSelection {
    pub(super) groups: usize,
    pub(super) retained: Vec<Candidate>,
    pub(super) lease: MemoryLease,
}

struct Partial {
    retained: Vec<Candidate>,
    groups: usize,
    duplicates: usize,
    sort_nanos: u128,
    reduce_nanos: u128,
}

pub(super) struct PairWorkers {
    // Jobs cancel/join before persistent input owners are dropped on failure.
    jobs: AggregateChunkJobs<Partial>,
    partitions: Vec<Partition>,
    columns: Vec<String>,
    roles: [usize; 2],
    cap: usize,
    decision: Option<bool>,
    retired: bool,
    _owner: MemoryLease,
    rows: u64,
    source_chunks: u64,
    groups: usize,
    duplicates: usize,
    accessor_nanos: u128,
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

impl PairWorkers {
    pub(super) fn admit(
        states: &GroupedAggregateStates<'_>,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        memory: &LiveMemoryPool,
    ) -> Result<Option<Self>> {
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
        Ok(Some(Self {
            jobs: AggregateChunkJobs::new(
                parallelism,
                parallelism.saturating_mul(2).clamp(1, 24),
                memory.snapshot().limit_bytes,
                memory.clone(),
            )?,
            partitions,
            columns: columns.to_vec(),
            roles,
            cap: retained_cap(states).ok_or_else(|| failed("missing retained bound"))?,
            decision: None,
            retired: false,
            _owner: owner,
            rows: 0,
            source_chunks: 0,
            groups: 0,
            duplicates: 0,
            accessor_nanos: 0,
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

    pub(super) fn provider_restore_requested(&self) -> bool {
        self.retired
    }
    pub(super) fn before_next(&mut self) -> Result<()> {
        self.jobs.check_cancelled()
    }
    pub(super) fn cancel(&self) {
        self.jobs.cancel();
    }

    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        self.jobs.check_cancelled()?;
        if self.decision == Some(false) {
            return Ok(false);
        }
        if chunk.is_empty() {
            return Ok(true);
        }
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
        let Some((first, second)) = aggregate_numeric_pair_direct_key_slices(
            &accessors[self.roles[0]],
            &accessors[self.roles[1]],
        ) else {
            if self.decision.is_some() {
                return Err(failed("committed input lost integer-slice contract"));
            }
            self.decline()?;
            return Ok(false);
        };
        if first.len() != chunk.len() || second.len() != chunk.len() {
            return Err(failed("key column lengths differ"));
        }
        if self.decision.is_none() {
            // Charge the existing bounded sample's table/control capacity too.
            let sample_lease = self.jobs.memory().reserve(8192 * 32)?;
            let admitted = numeric_pair_near_unique_directory_admitted(
                first,
                second,
                states.result_limit,
                states.request.offset,
            )?;
            drop(sample_lease);
            if !admitted {
                self.decline()?;
                return Ok(false);
            }
            self.decision = Some(true);
        }
        let started = Instant::now();
        let reserved = bytes::<Key>(chunk.len())?
            .checked_add((PARTITIONS * size_of::<usize>()) as u64)
            .ok_or_else(|| failed("staging overflow"))?;
        let buffer_lease = self.jobs.memory().reserve(reserved)?;
        self.buffer_peak = self.buffer_peak.max(reserved);
        let mut keys = Vec::new();
        keys.try_reserve_exact(chunk.len()).map_err(failed)?;
        if keys.capacity() > chunk.len() {
            return Err(failed("staging exceeds admitted capacity"));
        }
        let mut counts = [0usize; PARTITIONS];
        first.for_each_pair(second, None, |row, key| {
            if row % 1024 == 0 {
                self.jobs.check_cancelled()?;
            }
            counts[partition(key)] += 1;
            keys.push(key);
            Ok(())
        })?;
        for (partition, count) in self.partitions.iter_mut().zip(counts) {
            partition.reserve(count)?;
        }
        for key in &keys {
            self.partitions[partition(*key)].keys.push(*key);
        }
        self.rows = self
            .rows
            .checked_add(u64::try_from(keys.len()).map_err(failed)?)
            .ok_or_else(|| failed("row count overflow"))?;
        self.source_chunks += 1;
        drop(keys);
        drop(buffer_lease);
        self.route_nanos += started.elapsed().as_nanos();
        states.numeric_pair_direct_key_slice_updates = true;
        states.compact_measure_direct_updates = true;
        states.numeric_pair_late_measure_direct_updates = true;
        Ok(true)
    }

    fn decline(&mut self) -> Result<()> {
        self.jobs.retire()?;
        self.partitions.clear();
        self.retired = true;
        self.decision = Some(false);
        Ok(())
    }

    fn join_next(
        &mut self,
        retained: &mut Vec<Candidate>,
        worst: &mut Option<usize>,
    ) -> Result<()> {
        if let Some(completed) = self.jobs.join_next()? {
            completed.consume(|partial| {
                self.groups = self
                    .groups
                    .checked_add(partial.groups)
                    .ok_or_else(|| failed("group count overflow"))?;
                self.duplicates = self
                    .duplicates
                    .checked_add(partial.duplicates)
                    .ok_or_else(|| failed("duplicate count overflow"))?;
                self.partition_groups.push(partial.groups);
                self.sort_nanos += partial.sort_nanos;
                self.reduce_nanos += partial.reduce_nanos;
                for &candidate in &partial.retained {
                    NumericPairNearUniqueCountDirectory::consider_retained_candidate(
                        retained, worst, candidate, self.cap,
                    )?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.decision == Some(false) {
            return Ok(());
        }
        let started = Instant::now();
        let selected_lease = self.jobs.memory().reserve(bytes::<Candidate>(self.cap)?)?;
        let mut retained = Vec::new();
        retained.try_reserve_exact(self.cap).map_err(failed)?;
        if retained.capacity() > self.cap {
            return Err(failed("selection exceeds admitted capacity"));
        }
        let mut worst = None;
        let partitions = std::mem::take(&mut self.partitions);
        for partition in partitions {
            self.jobs.check_cancelled()?;
            self.partition_rows.push(partition.keys.len());
            self.partition_capacities.push(partition.keys.capacity());
            self.growths += partition.growths;
            self.overlap_peak = self.overlap_peak.max(partition.overlap_peak);
            while self.jobs.is_full() {
                self.join_next(&mut retained, &mut worst)?;
            }
            let cap = self.cap.min(partition.keys.len());
            // Persistent input credit travels in Partition. This task credit
            // covers the selected output until the caller consumes its receipt.
            self.jobs.submit(
                bytes::<Candidate>(cap)?
                    .checked_add(size_of::<Partial>() as u64)
                    .ok_or_else(|| failed("result capacity overflow"))?,
                move |worker, _| reduce(partition, cap, worker),
            )?;
        }
        while self.jobs.outstanding() > 0 {
            self.join_next(&mut retained, &mut worst)?;
        }
        self.jobs.check_cancelled()?;
        self.jobs.retire()?;
        self.retired = true;
        self.finish_nanos += started.elapsed().as_nanos();
        states.numeric_pair_partition_selection = Some(PairSelection {
            groups: self.groups,
            retained,
            lease: selected_lease,
        });
        Ok(())
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(failed)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        let memory = self.jobs.memory().snapshot();
        object.extend(serde_json::json!({
            "aggregate_workers_family": if self.decision == Some(false) { "numeric_pair_partition_sample_declined" } else { "complete_numeric_pair_partition_sort_reduce" },
            "aggregate_workers_pair_source_chunks": self.source_chunks,
            "aggregate_workers_pair_submitted_partition_tasks": self.jobs.submitted(),
            "aggregate_workers_pair_joined_partition_tasks": self.jobs.joined(),
            "aggregate_workers_shared_live_peak_bytes": memory.peak_reserved_bytes,
            "aggregate_workers_shared_live_bytes": memory.reserved_bytes,
            "aggregate_workers_shared_live_limit_bytes": memory.limit_bytes,
            "aggregate_workers_shared_live_denied_reservations": memory.denied_reservations,
            "aggregate_workers_pair_rows": self.rows,
            "aggregate_workers_pair_groups": self.groups,
            "aggregate_workers_pair_duplicate_keys": self.duplicates,
            "aggregate_workers_pair_partition_rows": self.partition_rows,
            "aggregate_workers_pair_partition_capacities": self.partition_capacities,
            "aggregate_workers_pair_partition_groups": self.partition_groups,
            "aggregate_workers_pair_growths": self.growths,
            "aggregate_workers_pair_growth_overlap_peak_bytes": self.overlap_peak,
            "aggregate_workers_pair_buffer_peak_bytes": self.buffer_peak,
            "aggregate_workers_pair_accessor_nanos": self.accessor_nanos,
            "aggregate_workers_pair_routing_nanos": self.route_nanos,
            "aggregate_workers_pair_sort_nanos": self.sort_nanos,
            "aggregate_workers_pair_reduce_nanos": self.reduce_nanos,
            "aggregate_workers_pair_finish_nanos": self.finish_nanos,
            "aggregate_workers_pair_retired_before_provider_resume": self.retired,
            "aggregate_workers_pair_memory_scope": "persistent_vector_capacity_growth_overlap_staging_and_selected_candidates;provider_accessor_and_existing_second_pass_states_separate;not_RSS_or_allocator_enforcement",
            "aggregate_workers_pair_counter_scope": "complete_keys_sorted_and_adjacent_equality_counted;sample_not_a_uniqueness_proof;worker_spans_overlap;sort_cancellation_checked_before_and_after_each_complete_partition",
            "aggregate_workers_pair_pressure_policy": "cancel_drain_fail_after_commit;no_serial_or_spill_replay"
        }).as_object().ok_or_else(|| failed("evidence is not an object"))?.clone());
        *summary = value.to_string();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn force_partitions_for_test(&mut self) {
        assert_eq!(self.rows, 0);
        self.decision = Some(true);
    }
    #[cfg(test)]
    pub(super) fn cancel_for_test(&self) {
        self.cancel();
    }
    #[cfg(test)]
    pub(super) fn has_committed_input(&self) -> bool {
        self.rows > 0
    }
    #[cfg(test)]
    pub(super) fn deny_next_growth_for_test(&mut self) {
        for p in &mut self.partitions {
            p.deny_next_growth = true;
        }
    }
}

fn reduce(mut partition: Partition, cap: usize, worker: &ChunkWorkerContext) -> Result<Partial> {
    worker.check_cancelled()?;
    let started = Instant::now();
    partition
        .keys
        .sort_unstable_by_key(|key| (key.key_kinds, key.first_bits, key.second_bits));
    let sort_nanos = started.elapsed().as_nanos();
    worker.check_cancelled()?;
    let started = Instant::now();
    let mut retained = Vec::new();
    retained.try_reserve_exact(cap).map_err(failed)?;
    if retained.capacity() > cap {
        return Err(failed("partial selection exceeds admitted capacity"));
    }
    let mut worst = None;
    let (mut groups, mut duplicates, mut begin) = (0usize, 0usize, 0usize);
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
        NumericPairNearUniqueCountDirectory::consider_retained_candidate(
            &mut retained,
            &mut worst,
            Candidate { key, count },
            cap,
        )?;
        groups += 1;
        duplicates += usize::from(count > 1);
        begin = end;
    }
    Ok(Partial {
        retained,
        groups,
        duplicates,
        sort_nanos,
        reduce_nanos: started.elapsed().as_nanos(),
    })
}
