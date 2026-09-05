//! Exact all-key native string counts for one deterministic source chunk.
//!
//! Canonical strings retain one native owner plus an index/count hash table; no
//! copied string dictionary or per-row dictionary IDs are constructed. Native
//! Dict codes stay bound to this partial's own values. Every occupied key merges.

use super::aggregate_chunk_jobs::ChunkWorkerContext;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::MemoryLease;
use std::{hash::Hasher as _, sync::Arc, time::Instant};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{
        Constant, Dict, PrimitiveArray, VarBinViewArray, dict::DictArraySlotsExt as _,
        varbinview::VarBinViewArrayExt as _,
    },
    dtype::{DType, Nullability, PType},
    validity::Validity,
};

#[derive(Clone, Copy, Default)]
struct CountSlot {
    hash: u64,
    value_index: usize,
    count: u64,
}

#[derive(Clone, Default)]
pub(super) struct StringCountPartialWork {
    pub rows: u64,
    pub partial_entries: u64,
    pub dictionary_values: u64,
    pub utf8_bytes_hashed: u64,
    pub equality_comparisons: u64,
    pub canonicalization_nanos: u128,
    pub count_nanos: u128,
    pub partial_capacity_bytes: u64,
    pub native_dictionary: bool,
    pub native_constant: bool,
}

pub(super) struct StringCountPartial {
    values: VarBinViewArray,
    counts: Vec<CountSlot>,
    pub work: StringCountPartialWork,
    preserves_existing_key_order: bool,
    // Retained through coordinator merge, even if task metadata is dropped.
    _counts_lease: MemoryLease,
    _deferred_metadata_lease: Option<MemoryLease>,
}

impl StringCountPartial {
    pub(super) fn deferred_metadata_bytes() -> u64 {
        (size_of::<Self>() + 2 * size_of::<usize>()) as u64
    }

    pub(super) fn retain_deferred_metadata(&mut self, lease: &mut MemoryLease) -> Result<()> {
        self._deferred_metadata_lease = Some(lease.split(Self::deferred_metadata_bytes())?);
        Ok(())
    }
    /// Linear, allocation-free grouping of all exact chunk keys by content hash.
    /// Dictionary codes remain bound to `values`; only referenced values hash.
    pub(super) fn arrange_partitions<const N: usize>(
        &mut self,
        worker: &ChunkWorkerContext,
    ) -> Result<[usize; N]> {
        if !N.is_power_of_two() {
            return Err(failed("partition count must be a power of two"));
        }
        let mut sizes = [0_usize; N];
        for (index, entry) in self.counts.iter_mut().enumerate() {
            if index % 4096 == 0 {
                worker.check_cancelled()?;
            }
            if self.work.native_dictionary || self.work.native_constant {
                let bytes = self.values.bytes_at(entry.value_index);
                let mut hasher = rustc_hash::FxHasher::default();
                hasher.write(bytes.as_slice());
                entry.hash = hasher.finish();
                self.work.utf8_bytes_hashed = self
                    .work
                    .utf8_bytes_hashed
                    .checked_add(u64_count(bytes.len())?)
                    .ok_or_else(|| failed("partition hash byte count overflowed"))?;
            }
            sizes[partition_index::<N>(entry.hash)] += 1;
        }
        let mut starts = [0_usize; N];
        let mut ends = [0_usize; N];
        let mut end = 0;
        for ((start, stop), size) in starts.iter_mut().zip(ends.iter_mut()).zip(sizes) {
            *start = end;
            end += size;
            *stop = end;
        }
        let mut next = starts;
        for (partition, end) in ends.iter().copied().enumerate() {
            while next[partition] < end {
                if next[partition] % 4096 == 0 {
                    worker.check_cancelled()?;
                }
                let target = partition_index::<N>(self.counts[next[partition]].hash);
                if target == partition {
                    next[partition] += 1;
                } else {
                    self.counts.swap(next[partition], next[target]);
                    next[target] += 1;
                }
            }
        }
        self.preserves_existing_key_order = false;
        Ok(ends)
    }

    pub(super) fn entry(&self, index: usize) -> Result<(vortex::buffer::ByteBuffer, u64, u64)> {
        let entry = self
            .counts
            .get(index)
            .ok_or_else(|| failed("partial index is absent"))?;
        Ok((
            self.values.bytes_at(entry.value_index),
            entry.hash,
            entry.count,
        ))
    }

    pub(super) fn retain_unconsumed(&mut self, start: usize, rows: u64) {
        self.counts.drain(..start);
        self.work.rows = rows;
        self.work.partial_entries = self.counts.len() as u64;
    }
    /// The ordinary count path records canonical keys in first-occurrence order.
    /// Native Dict already enumerates referenced values in its own domain order.
    pub(super) fn preserve_existing_key_order(&mut self) {
        if !self.preserves_existing_key_order {
            let started = Instant::now();
            self.counts.sort_unstable_by_key(|entry| entry.value_index);
            self.work.count_nanos += started.elapsed().as_nanos();
            self.preserves_existing_key_order = true;
        }
    }

    pub(super) fn for_each_count(
        &self,
        mut visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        for entry in &self.counts {
            let bytes = self.values.bytes_at(entry.value_index);
            let value = std::str::from_utf8(bytes.as_slice())
                .map_err(|error| failed(&format!("invalid UTF-8 during exact merge: {error}")))?;
            visit(value, entry.count)?;
        }
        Ok(())
    }
}

pub(super) fn partition_index<const N: usize>(hash: u64) -> usize {
    usize::try_from(hash.rotate_right(32) & ((N - 1) as u64))
        .expect("masked partition hash fits usize")
}

/// Shape admission is independent of names and the histogram row threshold.
/// Source schema nullability and the key's native UTF8 dtype are caller checks;
/// every worker revalidates its actual leaf before contributing any partial.
pub(super) fn admitted_group_index(states: &super::GroupedAggregateStates<'_>) -> Option<usize> {
    let [group_index] = states.group_key_indices.as_slice() else {
        return None;
    };
    let key = states.group_columns.get(*group_index)?;
    if !states.state_template.is_count_star_only()
        || !matches!(key.transform, super::AggregateValueTransform::Identity)
        || !key.extra_column_indices.is_empty()
        || !states.can_reconstruct_count_star_group_values_from_key()
        || states.source_order_group_admission_limit().is_some()
        || states
            .group_columns
            .iter()
            .enumerate()
            .any(|(index, column)| {
                index != *group_index
                    && !matches!(
                        column.transform,
                        super::AggregateValueTransform::ConstantInt(_)
                    )
            })
        || states.string_count_topk_candidate_ids.is_some()
        || states.string_count_topk_exact_counts.is_some()
        || states.single_numeric_count_groups.is_some()
        || states.numeric_pair_compact_groups.is_some()
        || states.numeric_pair_late_measure_count_groups.is_some()
        || states.numeric_minute_string_count_groups.is_some()
        || states
            .string_count_distinct_topk_heavy_hitter_sketch
            .is_some()
        || states.numeric_utf8_topk_heavy_hitter_sketch.is_some()
        || (states.string_count_topk_heavy_hitter_enabled
            && !states.string_count_topk_first_pass_exact_histogram_enabled)
    {
        return None;
    }
    Some(*group_index)
}

/// Caller-only reducer state. The borrowed request/schema/global group state
/// never crosses a worker boundary. This accounting describes the existing
/// conservative exact-state estimate separately from owned partial capacity.
#[derive(Default)]
pub(super) struct StringCountMerge {
    observed_interner_values: usize,
    retained_utf8_bytes: u64,
    pub rows: u64,
    pub entries: u64,
    pub new_global_strings: u64,
    pub peak_estimated_global_bytes: u64,
    pub pressure_transitions: u64,
    pub released_histogram_entries: u64,
    pub retained_interner_values_on_pressure: u64,
    pub merge_nanos: u128,
    poisoned: bool,
}

impl StringCountMerge {
    pub(super) fn merge(
        &mut self,
        states: &mut super::GroupedAggregateStates<'_>,
        group_index: usize,
        partial: &StringCountPartial,
    ) -> Result<()> {
        if self.poisoned {
            return Err(failed(
                "ordered reducer was invalidated by an earlier error",
            ));
        }
        let started = Instant::now();
        let result = self.merge_inner(states, group_index, partial);
        self.merge_nanos += started.elapsed().as_nanos();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    #[allow(clippy::too_many_lines)]
    fn merge_inner(
        &mut self,
        states: &mut super::GroupedAggregateStates<'_>,
        group_index: usize,
        partial: &StringCountPartial,
    ) -> Result<()> {
        if admitted_group_index(states) != Some(group_index) {
            return Err(failed(
                "ordered reducer lost its admitted exact count state",
            ));
        }
        if !states.string_count_topk_heavy_hitter_enabled {
            return self.merge_ordinary(states, partial);
        }
        self.refresh_interner(states)?;
        let next_total = states
            .string_count_topk_total_weight
            .checked_add(partial.work.rows)
            .ok_or_else(|| failed("total row count overflowed u64"))?;
        let mut visited_rows = 0_u64;
        partial.for_each_count(|value, count| {
            visited_rows = visited_rows
                .checked_add(count)
                .ok_or_else(|| failed("partial weight overflowed u64"))?;
            if count == 0 {
                return Err(failed("completed partial contains zero-weight entry"));
            }
            if let Some(histogram) = states
                .string_count_topk_first_pass_exact_histogram_counts
                .as_mut()
            {
                let id = states.string_interner.id(value);
                if let Some(id) = id
                    && let Some(previous) = histogram.get_mut(&id)
                {
                    *previous = previous
                        .checked_add(count)
                        .ok_or_else(|| failed("global exact group count overflowed u64"))?;
                } else {
                    let extra_keys = usize::from(id.is_none());
                    let next_keys = states
                        .string_interner
                        .len()
                        .checked_add(extra_keys)
                        .ok_or_else(|| failed("global key count overflowed"))?;
                    let extra_bytes = if id.is_none() {
                        u64_count(value.len())?
                    } else {
                        0
                    };
                    let estimated_bytes = u64_count(next_keys)?
                        .checked_mul(
                            super::STRING_COUNT_TOPK_FIRST_PASS_EXACT_HISTOGRAM_BYTES_PER_ENTRY,
                        )
                        .and_then(|bytes| bytes.checked_add(self.retained_utf8_bytes))
                        .and_then(|bytes| bytes.checked_add(extra_bytes))
                        .ok_or_else(|| failed("global state estimate overflowed"))?;
                    if histogram.len()
                        >= states.string_count_topk_first_pass_exact_histogram_entry_budget
                        || estimated_bytes > states.resource_envelope.memory_budget_bytes
                    {
                        self.enter_native_pressure_route(states)?;
                        self.merge_sketch_value(states, value, count)?;
                    } else {
                        let id = if let Some(id) = id {
                            id
                        } else {
                            states
                                .string_interner
                                .reserve(1, "parallel exact string identity")?;
                            let id = states.string_interner.intern(value)?;
                            self.new_global_strings = self
                                .new_global_strings
                                .checked_add(1)
                                .ok_or_else(|| failed("new global string counter overflowed"))?;
                            self.retained_utf8_bytes = self
                                .retained_utf8_bytes
                                .checked_add(extra_bytes)
                                .ok_or_else(|| failed("retained string byte counter overflowed"))?;
                            self.observed_interner_values = states.string_interner.len();
                            id
                        };
                        let histogram = states
                            .string_count_topk_first_pass_exact_histogram_counts
                            .as_mut()
                            .ok_or_else(|| failed("exact histogram disappeared"))?;
                        super::reserve_hash_map_capacity(
                            histogram,
                            1,
                            "parallel exact string counts",
                        )?;
                        histogram.insert(id, count);
                        self.peak_estimated_global_bytes =
                            self.peak_estimated_global_bytes.max(estimated_bytes);
                    }
                }
            } else {
                self.merge_sketch_value(states, value, count)?;
            }
            self.entries = self
                .entries
                .checked_add(1)
                .ok_or_else(|| failed("merged entry counter overflowed"))?;
            Ok(())
        })?;
        if visited_rows != partial.work.rows {
            return Err(failed(
                "completed partial weight differs from its source row count",
            ));
        }
        self.refresh_interner(states)?;
        self.rows = self
            .rows
            .checked_add(visited_rows)
            .ok_or_else(|| failed("merged row counter overflowed"))?;
        states.string_count_topk_total_weight = next_total;
        states.string_count_topk_first_pass_exact_histogram_input_rows = states
            .string_count_topk_first_pass_exact_histogram_input_rows
            .checked_add(visited_rows)
            .ok_or_else(|| failed("histogram input row counter overflowed"))?;
        states.string_count_topk_string_group_index = Some(group_index);
        states.count_star_direct_updates = true;
        states.string_count_topk_heavy_hitter_direct_updates = true;
        if partial.work.native_dictionary {
            states.chunk_dictionary_direct_updates = true;
            states.string_count_topk_dictionary_code_reuse = true;
        }
        states
            .aggregate_accessor_summary
            .insert(if partial.work.native_dictionary {
                "native_dictionary_owned_all_key_count_partial".into()
            } else {
                "native_canonical_utf8_owned_all_key_count_partial".into()
            });
        Ok(())
    }

    fn merge_ordinary(
        &mut self,
        states: &mut super::GroupedAggregateStates<'_>,
        partial: &StringCountPartial,
    ) -> Result<()> {
        let record_order = states.request.order_by.is_empty();
        if record_order && !partial.preserves_existing_key_order {
            return Err(failed(
                "ordinary source-order merge requires ordered partial keys",
            ));
        }
        if states
            .string_count_topk_first_pass_exact_histogram_counts
            .is_some()
            || states.string_count_topk_heavy_hitter_sketch.is_some()
        {
            return Err(failed(
                "ordinary count state cannot mix with exact histogram/sketch state",
            ));
        }
        let mut rows = 0_u64;
        partial.for_each_count(|value, count| {
            rows = rows
                .checked_add(count)
                .ok_or_else(|| failed("ordinary partial weight overflowed"))?;
            if count == 0 {
                return Err(failed("ordinary partial contains zero-weight entry"));
            }
            let id = if let Some(id) = states.string_interner.id(value) {
                id
            } else {
                states
                    .string_interner
                    .reserve(1, "parallel ordinary string identity")?;
                let id = states.string_interner.intern(value)?;
                self.new_global_strings = self
                    .new_global_strings
                    .checked_add(1)
                    .ok_or_else(|| failed("ordinary new string counter overflowed"))?;
                id
            };
            let key =
                super::AggregateGroupKey::single(super::AggregateDistinctValue::Utf8Interned(id));
            if !states.groups.contains_key(&key) {
                super::reserve_hash_map_capacity(
                    &mut states.groups,
                    1,
                    "parallel ordinary string count groups",
                )?;
                if record_order {
                    states.group_order.try_reserve(1).map_err(|error| {
                        failed(&format!("ordinary group order allocation failed: {error}"))
                    })?;
                }
            }
            let group = match states.groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if record_order {
                        states.group_order.push(entry.key().clone());
                    }
                    entry.insert(super::GroupedAggregateState::new_compact_count_star(None))
                }
            };
            group.increment_count_star_by(count)?;
            self.entries = self
                .entries
                .checked_add(1)
                .ok_or_else(|| failed("ordinary merged entry counter overflowed"))?;
            Ok(())
        })?;
        if rows != partial.work.rows {
            return Err(failed("ordinary partial weight differs from source rows"));
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| failed("ordinary merged row counter overflowed"))?;
        self.refresh_interner(states)?;
        states.count_star_direct_updates = true;
        states.chunk_dictionary_direct_updates |= partial.work.native_dictionary;
        states
            .aggregate_accessor_summary
            .insert(if partial.work.native_dictionary {
                "native_dictionary_owned_all_key_count_partial".into()
            } else {
                "native_canonical_utf8_owned_all_key_count_partial".into()
            });
        Ok(())
    }

    fn refresh_interner(&mut self, states: &super::GroupedAggregateStates<'_>) -> Result<()> {
        let appended = states
            .string_interner
            .values
            .get(self.observed_interner_values..)
            .ok_or_else(|| failed("global interner identity changed during ordered merge"))?;
        for value in appended {
            self.retained_utf8_bytes = self
                .retained_utf8_bytes
                .checked_add(u64_count(value.len())?)
                .ok_or_else(|| failed("global retained string bytes overflowed"))?;
        }
        self.observed_interner_values = states.string_interner.values.len();
        Ok(())
    }

    fn enter_native_pressure_route(
        &mut self,
        states: &mut super::GroupedAggregateStates<'_>,
    ) -> Result<()> {
        let previous = states
            .string_count_topk_first_pass_exact_histogram_counts
            .take()
            .ok_or_else(|| failed("pressure transition lost its exact consumed prefix"))?;
        let mut sketch = super::StringCountTopKHeavyHitterSketch::new_with_exact_mirror(
            states.string_count_topk_heavy_hitter_capacity(),
            0,
        );
        super::replay_string_count_topk_sketch_from_exact_counts(&mut sketch, &previous)?;
        self.released_histogram_entries = self
            .released_histogram_entries
            .checked_add(u64_count(previous.len())?)
            .ok_or_else(|| failed("released histogram counter overflowed"))?;
        drop(previous);
        states.string_count_topk_first_pass_exact_histogram_disabled = true;
        states.string_count_topk_heavy_hitter_sketch = Some(sketch);
        self.pressure_transitions = self
            .pressure_transitions
            .checked_add(1)
            .ok_or_else(|| failed("pressure counter overflowed"))?;
        self.retained_interner_values_on_pressure = u64_count(states.string_interner.len())?;
        Ok(())
    }

    #[allow(clippy::unused_self)]
    fn merge_sketch_value(
        &mut self,
        states: &mut super::GroupedAggregateStates<'_>,
        value: &str,
        count: u64,
    ) -> Result<()> {
        let owned = states.string_interner.id(value).map_or_else(
            || Ok(Arc::<str>::from(value)),
            |id| states.string_interner.value_arc(id),
        )?;
        let sketch = states
            .string_count_topk_heavy_hitter_sketch
            .as_mut()
            .ok_or_else(|| failed("pressure route lost its native sketch"))?;
        sketch.update_lazy_utf8_value(&owned, count, &mut states.string_interner)
    }
}

/// Known table capacity is admitted before the chunk enters the work queue.
pub(super) fn partial_bytes(array: &ArrayRef) -> Result<u64> {
    let slots = if array.as_opt::<Constant>().is_some() {
        usize::from(!array.is_empty())
    } else if let Some(dictionary) = array.as_opt::<Dict>() {
        dictionary.values().len()
    } else {
        hash_slot_count(array.len())?
    };
    slot_bytes(slots)
}

fn hash_slot_count(rows: usize) -> Result<usize> {
    rows.checked_mul(2)
        .and_then(|count| count.max(1).checked_next_power_of_two())
        .ok_or_else(|| failed("exact hash-table capacity overflowed"))
}

fn slot_bytes(slots: usize) -> Result<u64> {
    slots
        .checked_mul(std::mem::size_of::<CountSlot>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("partial allocation size overflowed"))
}

/// The caller admits a non-null identity string key and reserves `partial_bytes`
/// before submitting any chunk. `ctx` must use the same native budget as `lease`.
/// Provider allocations bypassing `HostAllocator` are outside this capacity scope.
#[allow(clippy::too_many_lines)]
pub(super) fn count_string_chunk(
    array: &ArrayRef,
    mut ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
) -> Result<StringCountPartial> {
    worker.check_cancelled()?;
    if !matches!(array.dtype(), DType::Utf8(Nullability::NonNullable)) {
        return Err(failed("lost its admitted nonnullable UTF-8 key contract"));
    }
    if lease.bytes() < partial_bytes(array)? {
        return Err(failed(
            "task did not pre-reserve its exact partial capacity",
        ));
    }
    let started = Instant::now();
    if array.as_opt::<Constant>().is_some() {
        let value_count = usize::from(!array.is_empty());
        let values = array
            .slice(0..value_count)
            .map_err(super::vortex_error)?
            .execute::<VarBinViewArray>(&mut ctx)
            .map_err(super::vortex_error)?;
        validate_values(&values)?;
        if values.len() != value_count {
            return Err(failed(
                "constant canonicalization changed the admitted value length",
            ));
        }
        let canonicalization_nanos = started.elapsed().as_nanos();
        let started = Instant::now();
        let (mut counts, counts_lease) = allocate_slots(value_count, lease)?;
        let rows = u64_count(array.len())?;
        if value_count == 1 {
            let bytes = values.bytes_at(0);
            std::str::from_utf8(bytes.as_slice())
                .map_err(|error| failed(&format!("constant key has invalid UTF-8: {error}")))?;
            counts[0] = CountSlot {
                hash: 0,
                value_index: 0,
                count: rows,
            };
        }
        worker.check_cancelled()?;
        return Ok(StringCountPartial {
            values,
            counts,
            work: StringCountPartialWork {
                rows,
                partial_entries: u64_count(value_count)?,
                canonicalization_nanos,
                count_nanos: started.elapsed().as_nanos(),
                partial_capacity_bytes: counts_lease.bytes(),
                native_constant: true,
                ..Default::default()
            },
            preserves_existing_key_order: true,
            _counts_lease: counts_lease,
            _deferred_metadata_lease: None,
        });
    }
    if let Some(dictionary) = array.as_opt::<Dict>() {
        let values = dictionary
            .values()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .map_err(super::vortex_error)?;
        let codes = dictionary
            .codes()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(super::vortex_error)?;
        validate_values(&values)?;
        if !matches!(
            vortex::array::arrays::primitive::PrimitiveArrayExt::validity(&codes),
            Validity::NonNullable | Validity::AllValid
        ) {
            return Err(failed("native dictionary codes are nullable"));
        }
        if codes.len() != array.len() {
            return Err(failed(
                "native dictionary code length differs from chunk rows",
            ));
        }
        let canonicalization_nanos = started.elapsed().as_nanos();
        let started = Instant::now();
        let (mut counts, counts_lease) = allocate_slots(values.len(), lease)?;
        macro_rules! count_codes {
            ($ty:ty) => {{
                for (row, code) in codes.as_slice::<$ty>().iter().copied().enumerate() {
                    if row % 4096 == 0 {
                        worker.check_cancelled()?;
                    }
                    let index = usize::try_from(code).map_err(|_| {
                        failed("native dictionary code is negative or exceeds usize")
                    })?;
                    let slot = counts.get_mut(index).ok_or_else(|| {
                        failed("native dictionary code exceeds its own values domain")
                    })?;
                    slot.value_index = index;
                    slot.count = slot
                        .count
                        .checked_add(1)
                        .ok_or_else(|| failed("native dictionary group count overflowed u64"))?;
                }
            }};
        }
        match codes.ptype() {
            PType::U8 => count_codes!(u8),
            PType::U16 => count_codes!(u16),
            PType::U32 => count_codes!(u32),
            PType::U64 => count_codes!(u64),
            PType::I8 => count_codes!(i8),
            PType::I16 => count_codes!(i16),
            PType::I32 => count_codes!(i32),
            PType::I64 => count_codes!(i64),
            PType::F16 | PType::F32 | PType::F64 => {
                return Err(failed("native dictionary code dtype is not an integer"));
            }
        }
        counts.retain(|slot| slot.count != 0);
        // Check referenced strings only: unused dictionary entries never acquire
        // a global identity and do not contribute to native group counts.
        for entry in &counts {
            let bytes = values.bytes_at(entry.value_index);
            std::str::from_utf8(bytes.as_slice()).map_err(|error| {
                failed(&format!(
                    "native dictionary value has invalid UTF-8: {error}"
                ))
            })?;
        }
        let work = StringCountPartialWork {
            rows: u64_count(array.len())?,
            partial_entries: u64_count(counts.len())?,
            dictionary_values: u64_count(values.len())?,
            canonicalization_nanos,
            count_nanos: started.elapsed().as_nanos(),
            partial_capacity_bytes: counts_lease.bytes(),
            native_dictionary: true,
            ..Default::default()
        };
        worker.check_cancelled()?;
        return Ok(StringCountPartial {
            values,
            counts,
            work,
            preserves_existing_key_order: true,
            _counts_lease: counts_lease,
            _deferred_metadata_lease: None,
        });
    }
    let values = array
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(super::vortex_error)?;
    validate_values(&values)?;
    if values.len() != array.len() {
        return Err(failed("canonical key length differs from chunk rows"));
    }
    let canonicalization_nanos = started.elapsed().as_nanos();
    let started = Instant::now();
    let slot_count = hash_slot_count(values.len())?;
    let (mut counts, counts_lease) = allocate_slots(slot_count, lease)?;
    let mask = slot_count - 1;
    let hash_mask = u64_count(mask)?;
    let mut work = StringCountPartialWork {
        rows: u64_count(array.len())?,
        canonicalization_nanos,
        partial_capacity_bytes: counts_lease.bytes(),
        ..Default::default()
    };
    for row in 0..values.len() {
        if row % 4096 == 0 {
            worker.check_cancelled()?;
        }
        let bytes = values.bytes_at(row);
        std::str::from_utf8(bytes.as_slice())
            .map_err(|error| failed(&format!("canonical key has invalid UTF-8: {error}")))?;
        work.utf8_bytes_hashed = work
            .utf8_bytes_hashed
            .checked_add(u64_count(bytes.len())?)
            .ok_or_else(|| failed("hashed byte counter overflowed"))?;
        let mut hasher = rustc_hash::FxHasher::default();
        hasher.write(bytes.as_slice());
        let hash = hasher.finish();
        let mut bucket =
            usize::try_from(hash & hash_mask).map_err(|_| failed("hash bucket exceeds usize"))?;
        loop {
            let slot = &mut counts[bucket];
            if slot.count == 0 {
                *slot = CountSlot {
                    hash,
                    value_index: row,
                    count: 1,
                };
                break;
            }
            if slot.hash == hash {
                work.equality_comparisons = work
                    .equality_comparisons
                    .checked_add(1)
                    .ok_or_else(|| failed("equality counter overflowed"))?;
                let previous = values.bytes_at(slot.value_index);
                if previous.as_slice() == bytes.as_slice() {
                    slot.count = slot
                        .count
                        .checked_add(1)
                        .ok_or_else(|| failed("exact group count overflowed u64"))?;
                    break;
                }
            }
            bucket = (bucket + 1) & mask;
        }
    }
    counts.retain(|slot| slot.count != 0);
    work.partial_entries = u64_count(counts.len())?;
    work.count_nanos = started.elapsed().as_nanos();
    worker.check_cancelled()?;
    Ok(StringCountPartial {
        values,
        counts,
        work,
        preserves_existing_key_order: false,
        _counts_lease: counts_lease,
        _deferred_metadata_lease: None,
    })
}

fn validate_values(values: &VarBinViewArray) -> Result<()> {
    if !matches!(
        values.varbinview_validity(),
        Validity::NonNullable | Validity::AllValid
    ) {
        return Err(failed("canonical values are nullable"));
    }
    Ok(())
}

fn allocate_slots(
    count: usize,
    task_lease: &mut MemoryLease,
) -> Result<(Vec<CountSlot>, MemoryLease)> {
    let bytes = slot_bytes(count)?;
    let lease = task_lease.split(bytes)?;
    let mut slots = Vec::new();
    slots
        .try_reserve_exact(count)
        .map_err(|error| failed(&format!("partial allocation failed: {error}")))?;
    if slots.capacity() > count {
        return Err(failed(
            "allocator returned capacity beyond the admitted partial bound",
        ));
    }
    slots.resize(count, CountSlot::default());
    Ok((slots, lease))
}

fn u64_count(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| failed("row/value count exceeds u64"))
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex parallel exact string count {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "string_count_partial_tests.rs"]
mod tests;
