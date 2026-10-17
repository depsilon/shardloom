//! Complete typed keys share one producer-owned string domain. This bounded
//! candidate reuses native accessors and query jobs; committed pressure is fatal.

use super::{
    AggregateDirectColumnAccessor, AggregateDirectIntegerKeySlice, AggregateIntegerKeyPart,
    AggregateNumericMinuteStringKey as Key, AggregateNumericPairKey, AggregateValueTransform,
    GroupedAggregateStates, NumericMinuteStringAggregateOrderCandidate as Candidate,
    NumericMinuteStringGroupRoles as Roles, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveResourceEnvelope, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    aggregate_direct_column_accessors_from_chunk, aggregate_direct_integer_key_part,
    aggregate_direct_integer_key_slice, aggregate_direct_minute_u8,
    aggregate_direct_prepared_minute_u8, aggregate_direct_utf8_dictionary_bound_id,
    aggregate_direct_utf8_dictionary_bound_id_for_code,
    aggregate_direct_utf8_dictionary_interner_ids, aggregate_prepared_minute_i64_value,
    aggregate_prepared_minute_u64_value, aggregate_raw_minute_i64_value,
    aggregate_raw_minute_u64_value, aggregate_utf8_dictionary_value_nulls_are_absent,
    compare_numeric_minute_string_candidates, is_shardloom_extract_minute_derived_column,
    numeric_minute_string_worst_retained_candidate_index, required_simple_aggregate,
};
use rustc_hash::FxHashMap;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
};

const PARTITIONS: usize = 64;

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native triple-key count: {message}; no fallback execution was attempted"
    ))
}

fn shape(states: &GroupedAggregateStates<'_>) -> bool {
    states.group_key_indices.len() == 3
        && states.group_columns.len() == 3
        && states.state_template.is_count_star_only()
        && states.single_numeric_count_order_alias().is_ok()
        && states.request.having.is_empty()
        && states.request.spill.is_none()
        && states
            .result_limit
            .is_some_and(|limit| limit > 0 && states.request.offset.checked_add(limit).is_some())
        && states.groups.is_empty()
        && states.group_order.is_empty()
}

pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    aggregate.group_by.len() + aggregate.group_expressions.len() == 3
        && aggregate.having.is_empty()
        && aggregate.spill.is_none()
        && request.source_order_limit.is_some_and(|limit| limit > 0)
        && aggregate.measures.len() == 1
}

pub(super) fn request_schema_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    envelope: VortexLocalPrimitiveResourceEnvelope,
) -> bool {
    let Ok(lowering) = super::aggregate_lowering::AggregateLowering::new(request, dtype, envelope)
    else {
        return false;
    };
    if lowering.residual.is_some() {
        return false;
    }
    aggregate_roles(
        &lowering.rewrite.aggregate,
        request.source_order_limit,
        dtype,
        envelope,
    )
    .is_some()
}

fn aggregate_roles(
    aggregate: &crate::VortexSimpleAggregateRequest,
    limit: Option<usize>,
    dtype: &DType,
    envelope: VortexLocalPrimitiveResourceEnvelope,
) -> Option<Roles> {
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    GroupedAggregateStates::new_with_resource_envelope(
        aggregate, limit, &columns, false, false, envelope, None,
    )
    .ok()
    .and_then(|states| roles(&states, dtype, &columns))
}

/// A nullable prepared field does not prove a nonnullable worker key. Keep the
/// original integer expression when it already satisfies the complete-key
/// contract; charge its actual scan/decode cost in the comparison.
pub(super) fn preserve_raw_minute_input(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    envelope: VortexLocalPrimitiveResourceEnvelope,
) -> bool {
    if request.predicate.is_some() {
        return false;
    }
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    let Some(roles) = aggregate_roles(aggregate, request.source_order_limit, dtype, envelope)
    else {
        return false;
    };
    if roles.minute_column_prepared {
        return false;
    }
    let columns = aggregate.projected_columns();
    let Some(source) = columns.get(roles.minute_column) else {
        return false;
    };
    let DType::Struct(fields, _) = dtype else {
        return false;
    };
    matches!(
        fields.field(super::shardloom_extract_minute_derived_column(source.as_str()).as_str()),
        Some(DType::Primitive(_, Nullability::Nullable))
    )
}

fn roles(states: &GroupedAggregateStates<'_>, dtype: &DType, columns: &[String]) -> Option<Roles> {
    if !shape(states) {
        return None;
    }
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return None;
    };
    let (mut numeric, mut minute, mut text) = (None, None, None);
    let mut prepared = false;
    for &index in &states.group_key_indices {
        let column = states.group_columns.get(index)?;
        if !column.extra_column_indices.is_empty() {
            return None;
        }
        let dtype = fields.field(columns.get(column.column_index)?.as_str())?;
        match (&column.transform, dtype) {
            (AggregateValueTransform::Identity, DType::Utf8(Nullability::NonNullable))
                if text.is_none() =>
            {
                text = Some(index);
            }
            (
                AggregateValueTransform::ExtractMinute,
                DType::Primitive(ptype, Nullability::NonNullable),
            ) if ptype.is_int() && minute.is_none() => minute = Some(index),
            (
                AggregateValueTransform::Identity,
                DType::Primitive(ptype, Nullability::NonNullable),
            ) if ptype.is_int()
                && is_shardloom_extract_minute_derived_column(&column.source_column)
                && minute.is_none() =>
            {
                minute = Some(index);
                prepared = true;
            }
            (
                AggregateValueTransform::Identity,
                DType::Primitive(ptype, Nullability::NonNullable),
            ) if ptype.is_int() && numeric.is_none() => numeric = Some(index),
            _ => return None,
        }
    }
    let (numeric, minute, text) = (numeric?, minute?, text?);
    Some(Roles {
        numeric_group: numeric,
        numeric_column: states.group_columns[numeric].column_index,
        minute_group: minute,
        minute_column: states.group_columns[minute].column_index,
        minute_column_prepared: prepared,
        string_group: text,
        string_column: states.group_columns[text].column_index,
    })
}

// Independent routing mixer: selecting on FxHashMap's bucket bits would make
// every key within a partition collide on those same bits in its inner table.
fn partition(key: Key) -> usize {
    let mut value =
        key.numeric_bits ^ key.string_id.rotate_left(23) ^ u64::from(key.key_kinds).rotate_left(47);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    usize::try_from((value ^ (value >> 31)) & (PARTITIONS as u64 - 1))
        .expect("six-bit partition index fits usize")
}

// A conservative capacity model, not allocator/RSS accounting. Reserve old and
// new table capacity simultaneously before try_reserve can move the allocation.
fn table_bytes(entries: usize) -> Result<u64> {
    if entries == 0 {
        return Ok(0);
    }
    let buckets = entries
        .checked_mul(2)
        .and_then(usize::checked_next_power_of_two)
        .ok_or_else(|| failed("table capacity overflow"))?;
    buckets
        .checked_mul(size_of::<(Key, u64)>() + 1)
        .and_then(|bytes| bytes.checked_add(64))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("table byte capacity overflow"))
}

struct Partition {
    groups: FxHashMap<Key, u64>,
    lease: MemoryLease,
    rows: u64,
    growths: u64,
    growth_overlap_peak: u64,
    #[cfg(test)]
    deny_next_growth: bool,
}
impl Partition {
    fn reserve(&mut self, rows: usize) -> Result<()> {
        let needed = self
            .groups
            .len()
            .checked_add(rows)
            .ok_or_else(|| failed("group count overflow"))?;
        if needed <= self.groups.capacity() {
            return Ok(());
        }
        #[cfg(test)]
        if std::mem::take(&mut self.deny_next_growth) {
            self.lease.resize(u64::MAX)?;
        }
        let old = self.lease.bytes();
        let next = table_bytes(needed)?;
        let overlap = old
            .checked_add(next)
            .ok_or_else(|| failed("growth overlap overflow"))?;
        self.lease.resize(overlap)?;
        self.growth_overlap_peak = self.growth_overlap_peak.max(overlap);
        self.groups.try_reserve(rows).map_err(failed)?;
        // The pinned table implementation returns at most twice the next power
        // of two of the requested entries. Reject an unexpected capacity rather
        // than silently represent the model as exact allocation enforcement.
        let observed = u64::try_from(self.groups.capacity())
            .map_err(failed)?
            .checked_mul((size_of::<(Key, u64)>() + 1) as u64)
            .ok_or_else(|| failed("observed capacity overflow"))?;
        if observed > next {
            return Err(failed("table capacity exceeds admitted model"));
        }
        self.lease.resize(next)?;
        self.growths += 1;
        Ok(())
    }
}

struct RoutedKeys {
    keys: Vec<Key>,
    ends: [usize; PARTITIONS],
}
#[derive(Default)]
struct Work {
    rows: u64,
    update: u128,
    lock_wait: u128,
}

pub(super) struct TripleWorkers {
    jobs: AggregateChunkJobs<Work>,
    partitions: Vec<Arc<Mutex<Partition>>>,
    roles: Roles,
    columns: Vec<String>,
    _owner: MemoryLease,
    accessor: u128,
    binding: u128,
    routing: u128,
    selection: u128,
    work: Work,
    partition_rows: Vec<u64>,
    partition_groups: Vec<usize>,
    partition_capacities: Vec<usize>,
    growths: u64,
    growth_overlap_peak: u64,
    buffer_peak: u64,
    total_key_bytes: u64,
}

impl TripleWorkers {
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
        let window = parallelism.saturating_mul(2).clamp(1, 24);
        let owner_bytes = size_of::<Self>()
            + PARTITIONS
                * (size_of::<Partition>()
                    + size_of::<Mutex<Partition>>()
                    + size_of::<Arc<Mutex<Partition>>>()
                    + 3 * size_of::<usize>()
                    + 64)
            + columns
                .iter()
                .map(|column| column.len() + size_of::<String>())
                .sum::<usize>();
        let Ok(owner) = memory.reserve(u64::try_from(owner_bytes).map_err(failed)?) else {
            return Ok(None);
        };
        let mut partitions = Vec::new();
        partitions.try_reserve_exact(PARTITIONS).map_err(failed)?;
        for _ in 0..PARTITIONS {
            partitions.push(Arc::new(Mutex::new(Partition {
                groups: FxHashMap::default(),
                lease: memory.reserve(0)?,
                rows: 0,
                growths: 0,
                growth_overlap_peak: 0,
                #[cfg(test)]
                deny_next_growth: false,
            })));
        }
        Ok(Some(Self {
            jobs: AggregateChunkJobs::new(
                parallelism,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
            )?,
            partitions,
            roles,
            columns: columns.to_vec(),
            _owner: owner,
            accessor: 0,
            binding: 0,
            routing: 0,
            selection: 0,
            work: Work::default(),
            partition_rows: Vec::new(),
            partition_groups: Vec::new(),
            partition_capacities: Vec::new(),
            growths: 0,
            growth_overlap_peak: 0,
            buffer_peak: 0,
            total_key_bytes: 0,
        }))
    }
    pub(super) fn cancel(&self) {
        self.jobs.cancel();
    }
    #[cfg(test)]
    pub(super) fn cancel_for_test(&self) {
        self.cancel();
    }
    #[cfg(test)]
    pub(super) fn drain(&mut self, _states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() > 0 {
            self.join_next()?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn deny_next_state_growth_for_test(&self) {
        for partition in &self.partitions {
            partition.lock().unwrap().deny_next_growth = true;
        }
    }
    #[cfg(test)]
    pub(super) fn has_committed_groups(&self) -> bool {
        self.partitions.iter().any(|partition| {
            partition
                .lock()
                .is_ok_and(|partition| !partition.groups.is_empty())
        })
    }
    pub(super) fn before_next(&mut self) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.jobs.is_full() {
            self.join_next()?;
        }
        Ok(())
    }
    fn join_next(&mut self) -> Result<()> {
        if let Some(completed) = self.jobs.join_next()? {
            completed.consume(|work| {
                self.work.rows += work.rows;
                self.work.update += work.update;
                self.work.lock_wait += work.lock_wait;
                Ok(())
            })?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_lines)]
    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        let started = Instant::now();
        let accessors = aggregate_direct_column_accessors_from_chunk(
            chunk,
            &self.columns,
            super::required_native_execution_ctx(&mut states.native_execution_ctx)?,
        )?;
        self.accessor += started.elapsed().as_nanos();
        states
            .native_numeric_accessor_work
            .add(&accessors.numeric_work)?;
        states.observe_aggregate_accessors(&self.columns, &accessors);
        let numeric = &accessors[self.roles.numeric_column];
        let minute = &accessors[self.roles.minute_column];
        let text = &accessors[self.roles.string_column];
        if [numeric.len(), minute.len(), text.len()]
            .iter()
            .any(|&rows| rows != chunk.len())
        {
            return Err(failed("key columns have different lengths"));
        }
        let started = Instant::now();
        let ids = aggregate_direct_utf8_dictionary_interner_ids(text, &mut states.string_interner)?;
        self.binding += started.elapsed().as_nanos();
        let started = Instant::now();
        let direct = match ids.as_deref() {
            Some(ids) => DirectKeys::admit(
                [numeric, minute, text],
                ids,
                self.roles.minute_column_prepared,
            )?,
            None => None,
        };
        let buffers = if direct.is_some() { 1 } else { 2 };
        let bytes = chunk
            .len()
            .checked_mul(size_of::<Key>() * buffers)
            .and_then(|bytes| {
                bytes.checked_add(
                    size_of::<RoutedKeys>()
                        + 2 * PARTITIONS * size_of::<usize>()
                        + PARTITIONS * size_of::<Arc<Mutex<Partition>>>(),
                )
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("key buffer size overflow"))?;
        while self.jobs.outstanding() > 0
            && self
                .jobs
                .memory()
                .snapshot()
                .limit_bytes
                .saturating_sub(self.jobs.memory().snapshot().reserved_bytes)
                < bytes
        {
            self.join_next()?;
        }
        // The captured buffer owns its own lease; submit(0) must not charge it
        // again. Window permits plus this per-buffer check bound captured inputs.
        let lease = self.jobs.memory().reserve(bytes)?;
        self.buffer_peak = self.buffer_peak.max(bytes);
        let routed = if let Some(direct) = direct {
            direct.route(|| self.jobs.check_cancelled())?
        } else {
            let mut keys = Vec::new();
            keys.try_reserve_exact(chunk.len()).map_err(failed)?;
            if keys.capacity() > chunk.len() {
                return Err(failed("key buffer exceeds admitted capacity"));
            }
            let mut counts = [0usize; PARTITIONS];
            for row in 0..chunk.len() {
                let key = if let Some(ids) = ids.as_deref() {
                    Key::from_parts(
                        aggregate_direct_integer_key_part(numeric, row, "triple count")?,
                        if self.roles.minute_column_prepared {
                            aggregate_direct_prepared_minute_u8(minute, row)?
                        } else {
                            aggregate_direct_minute_u8(minute, row)?
                        },
                        aggregate_direct_utf8_dictionary_bound_id(text, ids, row)?,
                    )
                } else {
                    Key::from_accessors(
                        numeric,
                        minute,
                        text,
                        self.roles.minute_column_prepared,
                        row,
                        &mut states.string_interner,
                    )?
                };
                counts[partition(key)] += 1;
                keys.push(key);
            }
            let mut ends = [0usize; PARTITIONS];
            let mut next = [0usize; PARTITIONS];
            let mut total = 0;
            for index in 0..PARTITIONS {
                next[index] = total;
                total += counts[index];
                ends[index] = total;
            }
            let mut routed = Vec::new();
            routed.try_reserve_exact(keys.len()).map_err(failed)?;
            if routed.capacity() > keys.len() {
                return Err(failed("routed key buffer exceeds admitted capacity"));
            }
            routed.resize(
                keys.len(),
                Key::from_parts(
                    AggregateIntegerKeyPart {
                        bits: 0,
                        signed: false,
                    },
                    0,
                    0,
                ),
            );
            for key in &keys {
                let index = partition(*key);
                routed[next[index]] = *key;
                next[index] += 1;
            }
            drop(keys);
            RoutedKeys { keys: routed, ends }
        };
        self.total_key_bytes +=
            u64::try_from(routed.keys.len() * size_of::<Key>()).map_err(failed)?;
        let input = Budgeted::new(routed, lease);
        let partitions = self.partitions.clone();
        self.jobs.submit(0, move |worker, _| {
            update_partitions(&partitions, input.value(), worker)
        })?;
        self.routing += started.elapsed().as_nanos();
        states.numeric_minute_string_group_roles = Some(self.roles);
        states.numeric_minute_string_dictionary_code_reuse |= ids.is_some();
        states.numeric_minute_string_count_direct_updates = true;
        states.count_star_direct_updates = true;
        Ok(true)
    }
    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.jobs.check_cancelled()?;
        while self.jobs.outstanding() > 0 {
            self.join_next()?;
        }
        let started = Instant::now();
        let requested_cap = states
            .request
            .offset
            .checked_add(states.result_limit.unwrap_or(0))
            .ok_or_else(|| failed("retained window overflow"))?;
        let total_groups = self
            .partitions
            .iter()
            .try_fold(0usize, |total, partition| {
                total
                    .checked_add(partition.lock().map_err(failed)?.groups.len())
                    .ok_or_else(|| failed("group count overflow"))
            })?;
        let retained_cap = requested_cap.min(total_groups);
        let mut selected_lease = self.jobs.memory().reserve(
            table_bytes(retained_cap)?
                .checked_add(
                    u64::try_from(
                        retained_cap
                            .checked_mul(size_of::<Candidate>())
                            .ok_or_else(|| failed("selection capacity overflow"))?,
                    )
                    .map_err(failed)?,
                )
                .ok_or_else(|| failed("selection bytes overflow"))?,
        )?;
        let mut retained = Vec::<Candidate>::new();
        retained.try_reserve_exact(retained_cap).map_err(failed)?;
        if retained.capacity() > retained_cap {
            return Err(failed("selection exceeds admitted capacity"));
        }
        let mut worst = None;
        let mut groups = 0usize;
        for partition in &self.partitions {
            let partition = partition.lock().map_err(failed)?;
            groups = groups
                .checked_add(partition.groups.len())
                .ok_or_else(|| failed("group count overflow"))?;
            self.partition_rows.push(partition.rows);
            self.partition_groups.push(partition.groups.len());
            self.partition_capacities.push(partition.groups.capacity());
            self.growths += partition.growths;
            self.growth_overlap_peak = self.growth_overlap_peak.max(partition.growth_overlap_peak);
            for (&key, &count) in &partition.groups {
                let candidate = Candidate { key, count };
                if retained.len() < retained_cap {
                    retained.push(candidate);
                    if retained.len() == retained_cap {
                        worst = numeric_minute_string_worst_retained_candidate_index(
                            &retained,
                            &states.string_interner,
                        );
                    }
                } else if let Some(index) = worst
                    && compare_numeric_minute_string_candidates(
                        &candidate,
                        &retained[index],
                        &states.string_interner,
                    )
                    .is_lt()
                {
                    retained[index] = candidate;
                    worst = numeric_minute_string_worst_retained_candidate_index(
                        &retained,
                        &states.string_interner,
                    );
                }
            }
        }
        self.jobs.check_cancelled()?;
        let mut selected = FxHashMap::default();
        selected.try_reserve(retained.len()).map_err(failed)?;
        for candidate in &retained {
            selected.insert(candidate.key, candidate.count);
        }
        drop(retained);
        selected_lease.resize(table_bytes(retained_cap)?)?;
        states.complete_key_partition_group_count = Some(groups);
        states.numeric_minute_string_count_groups = Some(selected);
        states.numeric_minute_string_group_roles = Some(self.roles);
        states.complete_key_selected_lease = Some(selected_lease);
        states.numeric_minute_string_count_direct_updates = true;
        states.count_star_direct_updates = true;
        self.partitions.clear();
        self.selection += started.elapsed().as_nanos();
        Ok(())
    }
    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(failed)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        let inserted: usize = self.partition_groups.iter().sum();
        let memory = self.jobs.memory().snapshot();
        object.extend(serde_json::json!({
            "aggregate_workers_family": "complete_numeric_minute_string_partitions",
            "aggregate_workers_submitted_chunks": self.jobs.submitted(),
            "aggregate_workers_joined_chunks": self.jobs.joined(),
            "aggregate_workers_shared_live_peak_bytes": memory.peak_reserved_bytes,
            "aggregate_workers_shared_live_bytes": memory.reserved_bytes,
            "aggregate_workers_shared_live_limit_bytes": memory.limit_bytes,
            "aggregate_workers_shared_live_denied_reservations": memory.denied_reservations,
            "aggregate_workers_triple_rows": self.work.rows,
            "aggregate_workers_triple_inserted_keys": inserted,
            "aggregate_workers_triple_existing_key_updates": self.work.rows.saturating_sub(inserted as u64),
            "aggregate_workers_triple_partition_rows": self.partition_rows,
            "aggregate_workers_triple_partition_groups": self.partition_groups,
            "aggregate_workers_triple_partition_capacities": self.partition_capacities,
            "aggregate_workers_triple_growths": self.growths,
            "aggregate_workers_triple_growth_overlap_peak_bytes": self.growth_overlap_peak,
            "aggregate_workers_triple_buffer_peak_bytes": self.buffer_peak,
            "aggregate_workers_triple_key_slot_payload_bytes": self.total_key_bytes,
            "aggregate_workers_triple_accessor_nanos": self.accessor,
            "aggregate_workers_triple_binding_nanos": self.binding,
            "aggregate_workers_triple_routing_submit_nanos": self.routing,
            "aggregate_workers_triple_update_nanos": self.work.update,
            "aggregate_workers_triple_lock_wait_nanos": self.work.lock_wait,
            "aggregate_workers_triple_join_wait_nanos": self.jobs.join_wait_nanos(),
            "aggregate_workers_triple_selection_nanos": self.selection,
            "aggregate_workers_triple_memory_scope": "leased_key_buffers_and_conservative_table_capacity_with_growth_overlap;producer_interner_and_accessor_allocations_remain_separate;not_RSS_or_allocator_enforcement",
            "aggregate_workers_triple_pressure_policy": "cancel_drain_fail;no_serial_or_spill_replay",
            "aggregate_workers_triple_counter_scope": "inserted_existing_counts_are_not_bucket_probes_or_local_duplicate_counts;worker_nanos_overlap"
        }).as_object().ok_or_else(|| failed("evidence is not an object"))?.clone());
        *summary = value.to_string();
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct DirectKeys<'a> {
    numeric: AggregateDirectIntegerKeySlice<'a>,
    minute: AggregateDirectIntegerKeySlice<'a>,
    row_ids: &'a [u32],
    ids: &'a [u64],
    prepared: bool,
}

impl<'a> DirectKeys<'a> {
    fn admit(
        accessors: [&'a AggregateDirectColumnAccessor; 3],
        ids: &'a [u64],
        prepared: bool,
    ) -> Result<Option<Self>> {
        let [numeric, minute, text] = accessors;
        let AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids,
            values,
            value_nulls,
            row_nulls,
            ..
        } = text
        else {
            return Ok(None);
        };
        if !aggregate_utf8_dictionary_value_nulls_are_absent(
            value_nulls.as_deref(),
            row_nulls.as_deref(),
        ) {
            return Ok(None);
        }
        if ids.len() != values.len() {
            return Err(failed("dictionary ID domain length mismatch"));
        }
        let (Some(numeric), Some(minute)) = (
            aggregate_direct_integer_key_slice(numeric),
            aggregate_direct_integer_key_slice(minute),
        ) else {
            return Ok(None);
        };
        if numeric.len() != minute.len() || numeric.len() != row_ids.len() {
            return Err(failed("direct key columns have different lengths"));
        }
        Ok(Some(Self {
            numeric,
            minute,
            row_ids,
            ids,
            prepared,
        }))
    }

    fn visit(&self, visit: impl FnMut(usize, Key) -> Result<()>) -> Result<()> {
        match (self.minute.signed(), self.prepared) {
            (false, true) => direct_key_slices(
                self.numeric,
                self.minute,
                self.row_ids,
                self.ids,
                aggregate_prepared_minute_u64_value,
                visit,
            ),
            (false, false) => direct_key_slices(
                self.numeric,
                self.minute,
                self.row_ids,
                self.ids,
                aggregate_raw_minute_u64_value,
                visit,
            ),
            (true, true) => direct_key_slices(
                self.numeric,
                self.minute,
                self.row_ids,
                self.ids,
                |bits| aggregate_prepared_minute_i64_value(bits.cast_signed()),
                visit,
            ),
            (true, false) => direct_key_slices(
                self.numeric,
                self.minute,
                self.row_ids,
                self.ids,
                |bits| aggregate_raw_minute_i64_value(bits.cast_signed()),
                visit,
            ),
        }
    }

    /// Immutable native slices and prebound dictionary identities make both
    /// passes pure. Only final partition slots receive a complete stored key.
    fn route(&self, mut check_cancelled: impl FnMut() -> Result<()>) -> Result<RoutedKeys> {
        check_cancelled()?;
        let mut counts = [0_usize; PARTITIONS];
        self.visit(|row, key| {
            if row.is_multiple_of(1024) {
                check_cancelled()?;
            }
            counts[partition(key)] += 1;
            Ok(())
        })?;
        let mut ends = [0_usize; PARTITIONS];
        let mut next = [0_usize; PARTITIONS];
        let mut total = 0_usize;
        for index in 0..PARTITIONS {
            next[index] = total;
            total = total
                .checked_add(counts[index])
                .ok_or_else(|| failed("routing histogram size overflow"))?;
            ends[index] = total;
        }
        if total != self.numeric.len() {
            return Err(failed("routing histogram differs from key count"));
        }
        check_cancelled()?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(total).map_err(failed)?;
        if keys.capacity() > total {
            return Err(failed("routed key buffer exceeds admitted capacity"));
        }
        keys.resize(
            total,
            Key::from_parts(
                AggregateIntegerKeyPart {
                    bits: 0,
                    signed: false,
                },
                0,
                0,
            ),
        );
        self.visit(|row, key| {
            if row.is_multiple_of(1024) {
                check_cancelled()?;
            }
            let index = partition(key);
            debug_assert!(next[index] < ends[index]);
            keys[next[index]] = key;
            next[index] += 1;
            Ok(())
        })?;
        check_cancelled()?;
        debug_assert_eq!(next, ends);
        Ok(RoutedKeys { keys, ends })
    }
}

fn direct_key_slices(
    numeric: AggregateDirectIntegerKeySlice<'_>,
    minute: AggregateDirectIntegerKeySlice<'_>,
    row_ids: &[u32],
    ids: &[u64],
    mut minute_for_bits: impl FnMut(u64) -> Result<u8>,
    mut visit: impl FnMut(usize, Key) -> Result<()>,
) -> Result<()> {
    numeric.for_each_pair(minute, None, |row, pair| {
        let key = Key::from_parts(
            AggregateIntegerKeyPart {
                bits: pair.first_bits,
                signed: pair.key_kinds & AggregateNumericPairKey::FIRST_SIGNED != 0,
            },
            minute_for_bits(pair.second_bits)?,
            aggregate_direct_utf8_dictionary_bound_id_for_code(ids, row_ids[row], "triple count")?,
        );
        visit(row, key)
    })
}

fn update_partitions(
    partitions: &[Arc<Mutex<Partition>>],
    input: &RoutedKeys,
    worker: &ChunkWorkerContext,
) -> Result<Work> {
    let mut work = Work {
        rows: input.keys.len() as u64,
        ..Work::default()
    };
    let mut begin = 0;
    for (index, &end) in input.ends.iter().enumerate() {
        worker.check_cancelled()?;
        if begin == end {
            continue;
        }
        let started = Instant::now();
        let mut partition = partitions[index].lock().map_err(failed)?;
        work.lock_wait += started.elapsed().as_nanos();
        let started = Instant::now();
        partition.reserve(end - begin)?;
        for (offset, key) in input.keys[begin..end].iter().enumerate() {
            if offset % 1024 == 0 {
                worker.check_cancelled()?;
            }
            let count = partition.groups.entry(*key).or_insert(0);
            *count = count
                .checked_add(1)
                .ok_or_else(|| failed("count overflow"))?;
        }
        partition.rows += (end - begin) as u64;
        work.update += started.elapsed().as_nanos();
        begin = end;
    }
    Ok(work)
}

#[cfg(test)]
mod direct_routing_tests {
    use super::*;

    #[test]
    fn typed_scatter_preserves_complete_keys_and_source_order_in_each_partition() {
        let ids = [u64::MAX - 9, 7, 0, 19, 3, 88, 1];
        for rows in [0_usize, 1, 65, 4099] {
            let signed = (0..rows)
                .map(|row| i64::try_from(row % 103).unwrap() - 51)
                .collect::<Vec<_>>();
            let unsigned = (0..rows)
                .map(|row| u64::MAX - u64::try_from(row % 103).unwrap())
                .collect::<Vec<_>>();
            let minutes = (0..rows)
                .map(|row| u8::try_from(row % 60).unwrap())
                .collect::<Vec<_>>();
            let seconds = minutes
                .iter()
                .map(|&value| u64::from(value) * 60)
                .collect::<Vec<_>>();
            let row_ids = (0..rows)
                .map(|row| u32::try_from(row % ids.len()).unwrap())
                .collect::<Vec<_>>();
            for is_signed in [false, true] {
                for prepared in [false, true] {
                    let direct = DirectKeys {
                        numeric: if is_signed {
                            AggregateDirectIntegerKeySlice::Int64(&signed)
                        } else {
                            AggregateDirectIntegerKeySlice::UInt64(&unsigned)
                        },
                        minute: if prepared {
                            AggregateDirectIntegerKeySlice::UInt8(&minutes)
                        } else {
                            AggregateDirectIntegerKeySlice::UInt64(&seconds)
                        },
                        row_ids: &row_ids,
                        ids: &ids,
                        prepared,
                    };
                    let expected = (0..rows)
                        .map(|row| {
                            Key::from_parts(
                                AggregateIntegerKeyPart {
                                    bits: if is_signed {
                                        signed[row].cast_unsigned()
                                    } else {
                                        unsigned[row]
                                    },
                                    signed: is_signed,
                                },
                                minutes[row],
                                ids[row % ids.len()],
                            )
                        })
                        .collect::<Vec<_>>();
                    let routed = direct.route(|| Ok(())).unwrap();
                    assert_eq!(routed.keys.len(), rows);
                    assert_eq!(routed.keys.capacity(), rows);
                    let mut start = 0;
                    for (index, end) in routed.ends.into_iter().enumerate() {
                        let expected = expected
                            .iter()
                            .copied()
                            .filter(|key| partition(*key) == index)
                            .collect::<Vec<_>>();
                        assert_eq!(routed.keys[start..end], expected);
                        start = end;
                    }
                    assert_eq!(start, rows);
                }
            }
        }
    }

    #[test]
    fn typed_scatter_checks_cancellation_in_both_passes() {
        let numeric = vec![7_u64; 8193];
        let minutes = vec![1_u8; numeric.len()];
        let row_ids = vec![0_u32; numeric.len()];
        let direct = DirectKeys {
            numeric: AggregateDirectIntegerKeySlice::UInt64(&numeric),
            minute: AggregateDirectIntegerKeySlice::UInt8(&minutes),
            row_ids: &row_ids,
            ids: &[9],
            prepared: true,
        };
        for fail_at in [1, 3, 12] {
            let mut calls = 0;
            let error = direct
                .route(|| {
                    calls += 1;
                    if calls == fail_at {
                        Err(failed("fixture cancelled"))
                    } else {
                        Ok(())
                    }
                })
                .err()
                .unwrap();
            assert!(error.to_string().contains("fixture cancelled"));
            assert_eq!(calls, fail_at);
        }
    }

    #[test]
    fn typed_scatter_rejects_an_unbound_dictionary_code() {
        let direct = DirectKeys {
            numeric: AggregateDirectIntegerKeySlice::Int8(&[0, -1]),
            minute: AggregateDirectIntegerKeySlice::UInt8(&[0, 59]),
            row_ids: &[0, 1],
            ids: &[7],
            prepared: true,
        };
        let error = direct.route(|| Ok(())).err().unwrap().to_string();
        assert!(error.contains("dictionary"), "{error}");
        assert!(error.contains("no fallback execution was attempted"));
    }
}
