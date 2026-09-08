//! Native complete integer group/value pairs for admitted grouped COUNT DISTINCT.
//! Original-width numeric owners feed exact keys; no partial group top-K occurs.

use super::{
    AggregateIntegerKeyPart, NativeNumericOwner, aggregate_chunk_jobs::ChunkWorkerContext,
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{hash::Hasher as _, time::Instant};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::PrimitiveArray,
    dtype::{DType, Nullability},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub(super) struct Pair {
    pub group_bits: u64,
    pub value_bits: u64,
    pub signedness: u8,
}
impl Pair {
    pub(super) fn new(group: AggregateIntegerKeyPart, value: AggregateIntegerKeyPart) -> Self {
        Self {
            group_bits: group.bits,
            value_bits: value.bits,
            signedness: u8::from(group.signed) | (u8::from(value.signed) << 1),
        }
    }
    pub(super) fn group(self) -> AggregateIntegerKeyPart {
        AggregateIntegerKeyPart {
            bits: self.group_bits,
            signed: self.signedness & 1 != 0,
        }
    }
    pub(super) fn value(self) -> AggregateIntegerKeyPart {
        AggregateIntegerKeyPart {
            bits: self.value_bits,
            signed: self.signedness & 2 != 0,
        }
    }
    pub(super) fn hash(self) -> u64 {
        let mut hash = rustc_hash::FxHasher::default();
        hash.write_u64(self.group_bits);
        hash.write_u64(self.value_bits);
        hash.write_u8(self.signedness);
        hash.finish()
    }
}
#[derive(Clone, Copy, Default)]
struct Slot {
    pair: Pair,
    hash: u64,
    weight: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Insert {
    Applied { new_pair: bool },
    NeedsEntryCredit,
    BytePressure,
}

/// The caller grants new-entry admission separately from byte reservations.
/// Existing-key updates never consume another entry credit.
pub(super) struct PairSet {
    slots: Vec<Slot>,
    memory: LiveMemoryPool,
    lease: MemoryLease,
    metadata: MemoryLease,
    pub pairs: usize,
    pub rows: u64,
    pub comparisons: u64,
}
impl PairSet {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        let metadata = size_of::<Self>()
            .max(size_of::<PairPartial>())
            .checked_add(2 * size_of::<usize>())
            .ok_or_else(|| failed("pair owner metadata overflowed"))?;
        Ok(Self {
            slots: Vec::new(),
            memory: memory.clone(),
            lease: memory.reserve(0)?,
            metadata: memory.reserve(metadata as u64)?,
            pairs: 0,
            rows: 0,
            comparisons: 0,
        })
    }
    pub(super) fn insert(
        &mut self,
        pair: Pair,
        weight: u64,
        new_entry_admitted: bool,
        worker: &ChunkWorkerContext,
    ) -> Result<Insert> {
        self.insert_hashed(pair, pair.hash(), weight, new_entry_admitted, worker)
    }
    fn insert_hashed(
        &mut self,
        pair: Pair,
        hash: u64,
        weight: u64,
        new_entry_admitted: bool,
        worker: &ChunkWorkerContext,
    ) -> Result<Insert> {
        if weight == 0 {
            return Err(failed("zero-weight pair contribution"));
        }
        let rows = self
            .rows
            .checked_add(weight)
            .ok_or_else(|| failed("pair input weight overflowed"))?;
        if let Some(index) = self.find(pair, hash, &mut || worker.check_cancelled())? {
            self.slots[index].weight = self.slots[index]
                .weight
                .checked_add(weight)
                .ok_or_else(|| failed("pair weight overflowed"))?;
            self.rows = rows;
            return Ok(Insert::Applied { new_pair: false });
        }
        if !new_entry_admitted {
            return Ok(Insert::NeedsEntryCredit);
        }
        if (self.slots.is_empty() || self.pairs + 1 > self.slots.len() / 2)
            && !self.grow(&mut || worker.check_cancelled())?
        {
            return Ok(Insert::BytePressure);
        }
        let mut index = bucket(hash, self.slots.len());
        let mut probes = 0_usize;
        while self.slots[index].weight != 0 {
            if probes.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            probes += 1;
            index = (index + 1) & (self.slots.len() - 1);
        }
        self.slots[index] = Slot { pair, hash, weight };
        self.pairs += 1;
        self.rows = rows;
        Ok(Insert::Applied { new_pair: true })
    }
    fn find(
        &mut self,
        pair: Pair,
        hash: u64,
        check_cancelled: &mut impl FnMut() -> Result<()>,
    ) -> Result<Option<usize>> {
        if self.slots.is_empty() {
            return Ok(None);
        }
        let mut index = bucket(hash, self.slots.len());
        let mut probes = 0_usize;
        loop {
            if probes.is_multiple_of(4096) {
                check_cancelled()?;
            }
            probes += 1;
            let slot = self.slots[index];
            if slot.weight == 0 {
                return Ok(None);
            }
            if slot.hash == hash {
                self.comparisons = self
                    .comparisons
                    .checked_add(1)
                    .ok_or_else(|| failed("pair probe counter overflowed"))?;
                if slot.pair == pair {
                    return Ok(Some(index));
                }
            }
            index = (index + 1) & (self.slots.len() - 1);
        }
    }
    fn grow(&mut self, check_cancelled: &mut impl FnMut() -> Result<()>) -> Result<bool> {
        let capacity = self
            .slots
            .len()
            .max(8)
            .checked_mul(2)
            .ok_or_else(|| failed("pair table capacity overflowed"))?;
        let bytes = capacity
            .checked_mul(size_of::<Slot>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("pair table bytes overflowed"))?;
        let Ok(lease) = self.memory.reserve(bytes) else {
            return Ok(false);
        };
        let mut slots = Vec::new();
        if slots.try_reserve_exact(capacity).is_err() || slots.capacity() > capacity {
            return Ok(false);
        }
        slots.resize(capacity, Slot::default());
        for (position, slot) in self.slots.iter().copied().enumerate() {
            if position.is_multiple_of(4096) {
                check_cancelled()?;
            }
            if slot.weight == 0 {
                continue;
            }
            let mut index = bucket(slot.hash, capacity);
            let mut probes = 0_usize;
            while slots[index].weight != 0 {
                probes += 1;
                if probes.is_multiple_of(4096) {
                    check_cancelled()?;
                }
                index = (index + 1) & (capacity - 1);
            }
            slots[index] = slot;
        }
        self.slots = slots;
        self.lease = lease;
        Ok(true)
    }
    pub(super) fn visit(&self, mut visit: impl FnMut(Pair, u64) -> Result<()>) -> Result<()> {
        for slot in &self.slots {
            if slot.weight != 0 {
                visit(slot.pair, slot.weight)?;
            }
        }
        Ok(())
    }
    /// Exact pair identities survive handoff. Only the final EOF reducer may
    /// turn each pair into a contribution of one for its group.
    pub(super) fn clear(&mut self) -> Result<()> {
        self.slots = Vec::new();
        self.lease.resize(0)?;
        self.pairs = 0;
        self.rows = 0;
        Ok(())
    }
}

pub(super) struct PairPartial {
    slots: Vec<Slot>,
    pub rows: u64,
    pub comparisons: u64,
    pub capacity_bytes: u64,
    pub native_execution_nanos: u128,
    pub count_nanos: u128,
    pub numeric_work: [NumericWork; 2],
    _lease: MemoryLease,
    _metadata: MemoryLease,
}
#[derive(Clone, Copy, Default)]
pub(super) struct NumericWork {
    pub required_execution: bool,
    pub source_bytes: u64,
    pub canonical_bytes: u64,
    pub elapsed_nanos: u128,
}
impl PairPartial {
    fn from_set(
        mut pairs: PairSet,
        native_execution_nanos: u128,
        count_nanos: u128,
        numeric_work: [NumericWork; 2],
    ) -> Self {
        // Retain capacity and its owner; compaction copies only private slot
        // metadata and never widens or copies the native numeric payload.
        pairs.slots.retain(|slot| slot.weight != 0);
        Self {
            slots: pairs.slots,
            rows: pairs.rows,
            comparisons: pairs.comparisons,
            capacity_bytes: pairs.lease.bytes(),
            native_execution_nanos,
            count_nanos,
            numeric_work,
            _lease: pairs.lease,
            _metadata: pairs.metadata,
        }
    }
    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }
    pub(super) fn entry(&self, index: usize) -> (Pair, u64, u64) {
        let slot = self.slots[index];
        (slot.pair, slot.weight, slot.hash)
    }
    pub(super) fn visit(&self, mut visit: impl FnMut(Pair, u64) -> Result<()>) -> Result<()> {
        for slot in &self.slots {
            visit(slot.pair, slot.weight)?;
        }
        Ok(())
    }
    pub(super) fn arrange(
        &mut self,
        worker: &ChunkWorkerContext,
    ) -> Result<[(usize, usize); partitions::PARTITIONS]> {
        const N: usize = partitions::PARTITIONS;
        let mut counts = [0_usize; N];
        for (index, slot) in self.slots.iter().enumerate() {
            if index.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            counts[partitions::partition(slot.hash)] += 1;
        }
        let mut next = [0_usize; N];
        let mut ranges = [(0, 0); N];
        let mut end = 0;
        for index in 0..N {
            next[index] = end;
            ranges[index].0 = end;
            end += counts[index];
            ranges[index].1 = end;
        }
        let mut swaps = 0_usize;
        for index in 0..N {
            while next[index] < ranges[index].1 {
                if swaps.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                swaps += 1;
                let target = partitions::partition(self.slots[next[index]].hash);
                if target == index {
                    next[index] += 1;
                } else {
                    self.slots.swap(next[index], next[target]);
                    next[target] += 1;
                }
            }
        }
        Ok(ranges)
    }
    pub(super) fn retain_suffix(&mut self, start: usize) -> Result<()> {
        self.slots.drain(..start);
        self.rows = self.slots.iter().try_fold(0_u64, |rows, slot| {
            rows.checked_add(slot.weight)
                .ok_or_else(|| failed("deferred pair weight overflowed"))
        })?;
        Ok(())
    }
}
pub(super) enum CountOutcome {
    Counted(PairPartial),
    RetryCapacity(ShardLoomError),
}

#[cfg(test)]
pub(super) fn count(
    group: &ArrayRef,
    value: &ArrayRef,
    ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    memory: &LiveMemoryPool,
) -> Result<PairPartial> {
    match count_admitted(group, value, ctx, worker, memory)? {
        CountOutcome::Counted(partial) => Ok(partial),
        CountOutcome::RetryCapacity(error) => Err(error),
    }
}

pub(super) fn count_admitted(
    group: &ArrayRef,
    value: &ArrayRef,
    mut ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    memory: &LiveMemoryPool,
) -> Result<CountOutcome> {
    worker.check_cancelled()?;
    if group.len() != value.len() || ![group, value].iter().all(|array| matches!(array.dtype(), DType::Primitive(ptype, Nullability::NonNullable) if ptype.is_int())) {
        return Err(failed("requires aligned nonnullable integer group/value arrays"));
    }
    let mut pairs = match PairSet::new(memory) {
        Ok(pairs) => pairs,
        Err(error) => return Ok(CountOutcome::RetryCapacity(error)),
    };
    let started = Instant::now();
    #[cfg(test)]
    if let Err(error) = injected_provider_error(&ctx) {
        return if crate::owned_buffers::is_owned_reservation_denial(&error) {
            Ok(CountOutcome::RetryCapacity(super::vortex_error(error)))
        } else {
            Err(super::vortex_error(error))
        };
    }
    let mut owners: [Option<NativeNumericOwner>; 2] = [None, None];
    let mut numeric_work = [NumericWork::default(); 2];
    for (index, array) in [group, value].into_iter().enumerate() {
        let owner_started = Instant::now();
        let primitive = match array.clone().execute::<PrimitiveArray>(&mut ctx) {
            Ok(primitive) => primitive,
            Err(error) if crate::owned_buffers::is_owned_reservation_denial(&error) => {
                return Ok(CountOutcome::RetryCapacity(super::vortex_error(error)));
            }
            Err(error) => return Err(super::vortex_error(error)),
        };
        if primitive.len() != array.len() || primitive.dtype() != array.dtype() {
            return Err(failed("native pair owner changed dtype or row count"));
        }
        let canonical_bytes = primitive.nbytes();
        owners[index] = Some(NativeNumericOwner::new(primitive, &mut ctx)?);
        numeric_work[index] = NumericWork {
            required_execution: !array.is::<vortex::array::arrays::Primitive>(),
            source_bytes: array.nbytes(),
            canonical_bytes,
            elapsed_nanos: owner_started.elapsed().as_nanos(),
        };
    }
    let [Some(group_owner), Some(value_owner)] = &owners else {
        unreachable!("two input owners");
    };
    let groups = group_owner
        .integer_key_slice()
        .ok_or_else(|| failed("group lost its all-valid integer contract"))?;
    let values = value_owner
        .integer_key_slice()
        .ok_or_else(|| failed("distinct value lost its all-valid integer contract"))?;
    if groups.len() != group.len() || values.len() != value.len() {
        return Err(failed("native pair execution changed row count"));
    }
    let native_execution_nanos = started.elapsed().as_nanos();
    let started = Instant::now();
    let mut capacity_denied = false;
    let result = groups.for_each_pair(values, None, |row, key| {
        if row.is_multiple_of(4096) {
            worker.check_cancelled()?;
        }
        let pair = Pair {
            group_bits: key.first_bits,
            value_bits: key.second_bits,
            signedness: key.key_kinds,
        };
        match pairs.insert(pair, 1, true, worker)? {
            Insert::Applied { .. } => Ok(()),
            Insert::BytePressure => {
                capacity_denied = true;
                Err(failed(
                    "native pair partial exceeds its byte budget before global mutation",
                ))
            }
            Insert::NeedsEntryCredit => Err(failed(
                "source-bounded pair partial unexpectedly lacked entry admission",
            )),
        }
    });
    match result {
        Err(error) if capacity_denied => Ok(CountOutcome::RetryCapacity(error)),
        Err(error) => Err(error),
        Ok(()) => Ok(CountOutcome::Counted(PairPartial::from_set(
            pairs,
            native_execution_nanos,
            started.elapsed().as_nanos(),
            numeric_work,
        ))),
    }
}
fn bucket(hash: u64, capacity: usize) -> usize {
    usize::try_from(hash & ((capacity - 1) as u64)).expect("masked pair hash fits usize")
}
fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native exact grouped distinct {reason}; no fallback execution was attempted"
    ))
}

#[path = "exact_distinct_partitions.rs"]
pub(super) mod partitions;

#[path = "exact_distinct_workers.rs"]
pub(super) mod workers;

#[cfg(test)]
#[derive(Debug)]
pub(super) struct ProviderFault {
    pub memory: LiveMemoryPool,
    pub calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub corruption: bool,
    pub repeat: bool,
}
#[cfg(test)]
impl vortex::session::SessionVar for ProviderFault {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
#[cfg(test)]
impl vortex::session::VortexSessionVar for ProviderFault {}
#[cfg(test)]
fn injected_provider_error(ctx: &ExecutionCtx) -> vortex::error::VortexResult<()> {
    use vortex::{array::memory::HostAllocator as _, session::SessionExt as _};
    let Some(fault) = ctx.session().get_opt::<ProviderFault>() else {
        return Ok(());
    };
    let call = fault
        .calls
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        + 1;
    if call != 2 && !(fault.repeat && call > 2) {
        return Ok(());
    }
    let snapshot = fault.memory.snapshot();
    let bytes = usize::try_from(snapshot.limit_bytes - snapshot.reserved_bytes)
        .expect("bounded fixture memory fits usize");
    let error = crate::owned_buffers::ReservedHostAllocator::new(fault.memory.clone())
        .allocate(bytes, vortex::buffer::Alignment::DEFAULT_ALIGNMENT)
        .expect_err("alignment capacity must exceed remaining fixture budget");
    if fault.corruption {
        Err(
            vortex::error::vortex_err!(InvalidArgument: "injected exact distinct provider corruption"),
        )
    } else {
        Err(error)
    }
}

#[cfg(test)]
#[path = "exact_distinct_pairs_tests.rs"]
mod tests;

#[cfg(feature = "vortex-write")]
#[path = "exact_distinct_spill.rs"]
mod spill;

#[cfg(feature = "vortex-write")]
#[path = "exact_distinct_spill_accumulator.rs"]
pub(super) mod spill_accumulator;

#[cfg(all(feature = "vortex-write", unix))]
#[path = "exact_distinct_spill_query.rs"]
pub(super) mod spill_query;
