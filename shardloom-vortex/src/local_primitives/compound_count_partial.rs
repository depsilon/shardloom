//! Exact all-key integer/UTF8 partials. Dictionary codes never leave their
//! retained native value domain; equality includes integer signedness and bytes.

use super::{
    AggregateIntegerKeyPart, NativeNumericOwner, aggregate_chunk_jobs::ChunkWorkerContext,
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::MemoryLease;
use std::{hash::Hasher as _, time::Instant};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{
        Dict, PrimitiveArray, VarBinViewArray, dict::DictArraySlotsExt as _,
        varbinview::VarBinViewArrayExt as _,
    },
    dtype::{DType, Nullability},
    validity::Validity,
};

#[derive(Clone, Copy, Default)]
pub(super) struct Key {
    pub bits: u64,
    pub signed: bool,
}

impl Key {
    pub(super) fn part(self) -> AggregateIntegerKeyPart {
        AggregateIntegerKeyPart {
            bits: self.bits,
            signed: self.signed,
        }
    }
    pub(super) fn cmp(self, other: Self) -> std::cmp::Ordering {
        // A column has one signedness; the cross-signed comparison also remains
        // exact for test/defensive callers without narrowing unsigned extrema.
        let integer = |key: Self| {
            if key.signed {
                i128::from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
            } else {
                i128::from(key.bits)
            }
        };
        integer(self)
            .cmp(&integer(other))
            .then(self.signed.cmp(&other.signed))
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    hash: u64,
    key: Key,
    value: usize,
    count: u64,
}

#[derive(Clone, Default)]
pub(super) struct Work {
    pub rows: u64,
    pub entries: u64,
    pub bytes_hashed: u64,
    pub comparisons: u64,
    pub canonicalization_nanos: u128,
    pub numeric_execution_nanos: u128,
    pub numeric_source_bytes: u64,
    pub numeric_canonical_bytes: u64,
    pub numeric_required_execution: bool,
    pub count_nanos: u128,
    pub capacity_bytes: u64,
    pub dictionary: bool,
}

pub(super) struct CompoundPartial {
    values: VarBinViewArray,
    slots: Vec<Slot>,
    pub work: Work,
    _lease: MemoryLease,
}

impl CompoundPartial {
    #[cfg(test)]
    pub(super) fn force_collision_hashes(&mut self) {
        for slot in &mut self.slots {
            slot.hash = 0;
        }
    }
    pub(super) fn bytes(rows: usize) -> Result<u64> {
        capacity(rows)?
            .checked_mul(size_of::<Slot>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() + 2 * size_of::<usize>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("partial capacity overflowed"))
    }
    pub(super) fn dictionary_hash_bytes(text: &ArrayRef) -> Result<u64> {
        text.as_opt::<Dict>()
            .map_or(Some(0), |dict| {
                dict.values()
                    .len()
                    .checked_mul(size_of::<Option<u64>>())
                    .and_then(|bytes| u64::try_from(bytes).ok())
            })
            .ok_or_else(|| failed("dictionary hash capacity overflowed"))
    }
    pub(super) fn arrange<const N: usize>(
        &mut self,
        worker: &ChunkWorkerContext,
    ) -> Result<[usize; N]> {
        let mut sizes = [0_usize; N];
        for entry in &self.slots {
            sizes[partition::<N>(entry.hash)] += 1;
        }
        let mut next = [0_usize; N];
        let mut ends = [0_usize; N];
        let mut end = 0;
        for index in 0..N {
            next[index] = end;
            end += sizes[index];
            ends[index] = end;
        }
        for (index, end) in ends.iter().copied().enumerate() {
            while next[index] < end {
                if next[index] % 4096 == 0 {
                    worker.check_cancelled()?;
                }
                let target = partition::<N>(self.slots[next[index]].hash);
                if target == index {
                    next[index] += 1;
                } else {
                    self.slots.swap(next[index], next[target]);
                    next[target] += 1;
                }
            }
        }
        Ok(ends)
    }
    pub(super) fn entry(&self, index: usize) -> (Key, vortex::buffer::ByteBuffer, u64, u64) {
        let slot = self.slots[index];
        (
            slot.key,
            self.values.bytes_at(slot.value),
            slot.hash,
            slot.count,
        )
    }
    pub(super) fn retain_suffix(&mut self, cursor: usize, rows: u64) {
        self.slots.drain(..cursor);
        self.work.rows = rows;
        self.work.entries = self.slots.len() as u64;
    }
    pub(super) fn visit(&self, mut visit: impl FnMut(Key, &str, u64) -> Result<()>) -> Result<()> {
        for slot in &self.slots {
            let bytes = self.values.bytes_at(slot.value);
            visit(
                slot.key,
                std::str::from_utf8(bytes.as_slice())
                    .map_err(|error| failed(&error.to_string()))?,
                slot.count,
            )?;
        }
        Ok(())
    }
}

pub(super) fn partition<const N: usize>(hash: u64) -> usize {
    super::string_count_partial::partition_index::<N>(hash)
}

pub(super) fn string_hash(bytes: &[u8]) -> u64 {
    let mut hash = rustc_hash::FxHasher::default();
    hash.write(bytes);
    hash.finish()
}

fn hash_key(key: Key, text_hash: u64) -> u64 {
    let mut hash = rustc_hash::FxHasher::default();
    hash.write_u64(key.bits);
    hash.write_u8(u8::from(key.signed));
    hash.write_u64(text_hash);
    hash.finish()
}

fn capacity(rows: usize) -> Result<usize> {
    rows.max(1)
        .checked_mul(2)
        .and_then(usize::checked_next_power_of_two)
        .ok_or_else(|| failed("partial table capacity overflowed"))
}

pub(super) enum CountOutcome {
    Counted(CompoundPartial),
    OwnedAllocationDenied(ShardLoomError),
}

pub(super) fn count_admitted(
    numeric: &ArrayRef,
    text: &ArrayRef,
    ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
) -> Result<CountOutcome> {
    let mut owned_denial = false;
    let result = count_with_provider_error(numeric, text, ctx, worker, lease, &mut |error| {
        // Preserve the actual typed provider error before the public diagnostic
        // converts it to text. An unrelated denial elsewhere is never enough.
        owned_denial = crate::owned_buffers::is_owned_reservation_denial(&error);
        super::vortex_error(error)
    });
    match result {
        Ok(partial) => Ok(CountOutcome::Counted(partial)),
        Err(error) if owned_denial => Ok(CountOutcome::OwnedAllocationDenied(error)),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(super) fn count(
    numeric: &ArrayRef,
    text: &ArrayRef,
    ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
) -> Result<CompoundPartial> {
    count_with_provider_error(numeric, text, ctx, worker, lease, &mut super::vortex_error)
}

// Keep native owners, charged scratch, the borrowed dictionary domain, and the
// single dispatch into typed loops in one checked resource-lifetime boundary.
#[allow(clippy::too_many_lines)]
fn count_with_provider_error(
    numeric: &ArrayRef,
    text: &ArrayRef,
    mut ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
    native_error: &mut impl FnMut(vortex::error::VortexError) -> ShardLoomError,
) -> Result<CompoundPartial> {
    worker.check_cancelled()?;
    if numeric.len() != text.len()
        || !matches!(numeric.dtype(), DType::Primitive(ptype, Nullability::NonNullable) if ptype.is_int())
        || !matches!(text.dtype(), DType::Utf8(Nullability::NonNullable))
    {
        return Err(failed("lost nonnullable integer/UTF8 row contract"));
    }
    let required = CompoundPartial::bytes(text.len())?
        .checked_add(CompoundPartial::dictionary_hash_bytes(text)?)
        .ok_or_else(|| failed("partial reservation overflowed"))?;
    if lease.bytes() < required {
        return Err(failed(
            "task did not reserve compound partial capacity before native execution",
        ));
    }
    let started = Instant::now();
    #[cfg(test)]
    injected_provider_error(&ctx).map_err(&mut *native_error)?;
    let primitive = numeric
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(&mut *native_error)?;
    if primitive.len() != numeric.len() || primitive.dtype() != numeric.dtype() {
        return Err(failed("numeric execution changed dtype or row count"));
    }
    let numeric_canonical_bytes = primitive.nbytes();
    let owner = NativeNumericOwner::new(primitive, &mut ctx)?;
    let numeric_execution_nanos = started.elapsed().as_nanos();
    let keys = owner
        .integer_key_slice()
        .ok_or_else(|| failed("numeric key is not an all-valid integer"))?;
    let dictionary = text.as_opt::<Dict>();
    let values = if let Some(dict) = dictionary {
        dict.values().clone()
    } else {
        text.clone()
    }
    .execute::<VarBinViewArray>(&mut ctx)
    .map_err(&mut *native_error)?;
    if !matches!(
        values.varbinview_validity(),
        Validity::NonNullable | Validity::AllValid
    ) {
        return Err(failed("string value domain contains nulls"));
    }
    let codes = dictionary
        .map(|dict| {
            let primitive = dict
                .codes()
                .clone()
                .execute::<PrimitiveArray>(&mut ctx)
                .map_err(&mut *native_error)?;
            NativeNumericOwner::new(primitive, &mut ctx)
        })
        .transpose()?;
    if keys.len() != text.len()
        || codes
            .as_ref()
            .is_some_and(|codes| codes.len() != text.len())
        || (codes.is_none() && values.len() != text.len())
    {
        return Err(failed("native canonicalization changed row count"));
    }
    let canonicalization_nanos = started.elapsed().as_nanos();
    let bytes = CompoundPartial::bytes(text.len())?;
    let owned_lease = lease.split(bytes)?;
    let mut slots = Vec::new();
    let capacity = capacity(text.len())?;
    slots
        .try_reserve_exact(capacity)
        .map_err(|error| failed(&error.to_string()))?;
    if slots.capacity() > capacity {
        return Err(failed("partial allocator exceeded reserved capacity"));
    }
    slots.resize(capacity, Slot::default());
    let hash_bytes = CompoundPartial::dictionary_hash_bytes(text)?;
    let _hash_lease = lease.split(hash_bytes)?;
    let mut hashes = Vec::new();
    if dictionary.is_some() {
        hashes
            .try_reserve_exact(values.len())
            .map_err(|error| failed(&error.to_string()))?;
        if hashes.capacity() > values.len() {
            return Err(failed("dictionary hash capacity exceeded reservation"));
        }
        hashes.resize(values.len(), None);
    }
    let started = Instant::now();
    let mut work = Work {
        rows: text.len() as u64,
        canonicalization_nanos,
        numeric_execution_nanos,
        numeric_source_bytes: numeric.nbytes(),
        numeric_canonical_bytes,
        numeric_required_execution: !numeric.is::<vortex::array::arrays::Primitive>(),
        capacity_bytes: bytes,
        dictionary: dictionary.is_some(),
        ..Work::default()
    };
    let mut insert = |row: usize, numeric: AggregateIntegerKeyPart, value: usize| -> Result<()> {
        if row.is_multiple_of(4096) {
            worker.check_cancelled()?;
        }
        if value >= values.len() {
            return Err(failed("dictionary code exceeds its own value domain"));
        }
        let bytes = values.bytes_at(value);
        let text_hash = if let Some(hash) = hashes.get(value).copied().flatten() {
            hash
        } else {
            std::str::from_utf8(bytes.as_slice()).map_err(|error| failed(&error.to_string()))?;
            work.bytes_hashed = work
                .bytes_hashed
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| failed("hash byte count overflowed"))?;
            let hash = string_hash(bytes.as_slice());
            if let Some(slot) = hashes.get_mut(value) {
                *slot = Some(hash);
            }
            hash
        };
        let key = Key {
            bits: numeric.bits,
            signed: numeric.signed,
        };
        let hash = hash_key(key, text_hash);
        let mut bucket = bucket(hash, slots.len());
        loop {
            let slot = &mut slots[bucket];
            if slot.count == 0 {
                *slot = Slot {
                    hash,
                    key,
                    value,
                    count: 1,
                };
                break;
            }
            if slot.hash == hash && slot.key.bits == key.bits && slot.key.signed == key.signed {
                work.comparisons = work
                    .comparisons
                    .checked_add(1)
                    .ok_or_else(|| failed("comparison count overflowed"))?;
                if values.bytes_at(slot.value).as_slice() == bytes.as_slice() {
                    slot.count = slot
                        .count
                        .checked_add(1)
                        .ok_or_else(|| failed("partial count overflowed"))?;
                    break;
                }
            }
            bucket = (bucket + 1) & (slots.len() - 1);
        }
        Ok(())
    };
    if let Some(codes) = codes.as_ref() {
        let codes = codes
            .integer_key_slice()
            .ok_or_else(|| failed("dictionary codes contain null or noninteger values"))?;
        keys.for_each_pair(codes, None, |row, pair| {
            let key = AggregateIntegerKeyPart {
                bits: pair.first_bits,
                signed: pair.key_kinds & 1 != 0,
            };
            let code = AggregateIntegerKeyPart {
                bits: pair.second_bits,
                signed: pair.key_kinds & 2 != 0,
            };
            if code.signed && i64::from_ne_bytes(code.bits.to_ne_bytes()) < 0 {
                return Err(failed("dictionary code is negative"));
            }
            insert(
                row,
                key,
                usize::try_from(code.bits).map_err(|_| failed("dictionary code exceeds usize"))?,
            )
        })?;
    } else {
        keys.for_each(None, |row, key| insert(row, key, row))?;
    }
    slots.retain(|slot| slot.count != 0);
    work.entries = slots.len() as u64;
    work.count_nanos = started.elapsed().as_nanos();
    Ok(CompoundPartial {
        values,
        slots,
        work,
        _lease: owned_lease,
    })
}

pub(super) fn bucket(hash: u64, capacity: usize) -> usize {
    usize::try_from(hash & ((capacity - 1) as u64)).expect("masked native table hash fits usize")
}
pub(super) fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex compound count {reason}; no fallback execution was attempted"
    ))
}

// A session-local test fault at the actual provider-execution boundary. It
// creates the real typed allocator error; no codec or allocation coverage is
// emulated. Production has no hook, global state, or error-string matching.
#[cfg(test)]
#[derive(Debug)]
pub(super) struct ProviderFault {
    pub memory: shardloom_exec::live_memory::LiveMemoryPool,
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
    let len = usize::try_from(snapshot.limit_bytes - snapshot.reserved_bytes)
        .expect("bounded fixture memory fits usize");
    let error = crate::owned_buffers::ReservedHostAllocator::new(fault.memory.clone())
        .allocate(len, vortex::buffer::Alignment::DEFAULT_ALIGNMENT)
        .expect_err("alignment capacity must exceed the remaining budget");
    if fault.corruption {
        Err(vortex::error::vortex_err!(InvalidArgument: "injected compound provider corruption"))
    } else {
        Err(error)
    }
}
