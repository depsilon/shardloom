//! Scan-local reuse of compressed native segments with known allocation owners.
//! Logical segment completions here are not completed filesystem-read bytes.

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use futures::{
    FutureExt as _, TryFutureExt as _,
    future::{BoxFuture, Shared, WeakShared},
};
use shardloom_core::{ColumnRef, PredicateExpr, Result};
use shardloom_exec::live_memory::{LiveMemoryPool, LiveMemorySnapshot, MemoryLease};
use vortex::{
    array::{buffer::BufferHandle, dtype::DType, memory::HostAllocatorRef},
    buffer::{Alignment, ByteBuffer},
    error::{SharedVortexResult, VortexError, VortexResult, vortex_err},
    layout::{
        DynLayout,
        layouts::struct_::Struct,
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
};

use super::resident_error;
use crate::owned_buffers::{ReservedHostAllocator, is_owned_reservation_denial};

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_field_names)] // Each independent bound has explicit maximum semantics.
pub(crate) struct SegmentReusePolicy {
    pub(crate) max_retained_bytes: u64,
    pub(crate) max_segment_bytes: usize,
    pub(crate) max_entries: usize,
}

impl Default for SegmentReusePolicy {
    fn default() -> Self {
        Self {
            max_retained_bytes: 16 << 20,
            max_segment_bytes: 8 << 20,
            max_entries: 128,
        }
    }
}

impl SegmentReusePolicy {
    /// Admit repeated filter-only scalar fields behind native Struct readers.
    /// Inspect only the root and bounded schema/predicate/projection metadata;
    /// never materialize child layouts. This is an opportunity, not an I/O proof.
    pub(crate) fn for_scan(
        predicate: &PredicateExpr,
        projected_columns: &[ColumnRef],
        layout: &dyn DynLayout,
        memory_bytes: u64,
    ) -> Option<Self> {
        let root = layout.as_opt::<Struct>()?;
        let fields = root.struct_fields();
        let retained = (memory_bytes / 16).min(64 << 20);
        if fields.nfields() > 256
            || projected_columns.len() > 64
            || retained < 1 << 20
            || !matches!(predicate, PredicateExpr::And(_))
        {
            return None;
        }
        let field_index = |name: &str| {
            fields
                .names()
                .iter()
                .position(|field| field.as_ref() == name)
        };
        for column in projected_columns {
            field_index(column.as_str())?;
        }
        let mut columns = [None; 64];
        let mut repeated = [false; 64];
        let mut used = 0;
        let mut remaining_nodes = 128;
        collect_columns(
            predicate,
            &mut columns,
            &mut repeated,
            &mut used,
            &mut remaining_nodes,
        )?;
        let mut eligible = false;
        for (index, name) in columns[..used].iter().enumerate() {
            let name = (*name)?;
            let field = fields.field_by_index(field_index(name)?)?;
            eligible |= repeated[index]
                && !projected_columns
                    .iter()
                    .any(|column| column.as_str() == name)
                && matches!(
                    field,
                    DType::Bool(_) | DType::Primitive(..) | DType::Utf8(_)
                );
        }
        eligible.then_some(Self {
            max_retained_bytes: retained,
            max_segment_bytes: usize::try_from(retained.min(16 << 20)).ok()?,
            max_entries: 128,
        })
    }
}

fn collect_columns<'a>(
    predicate: &'a PredicateExpr,
    columns: &mut [Option<&'a str>; 64],
    repeated: &mut [bool; 64],
    used: &mut usize,
    remaining_nodes: &mut usize,
) -> Option<()> {
    *remaining_nodes = remaining_nodes.checked_sub(1)?;
    if let Some(column) = predicate.column() {
        if let Some(index) = columns[..*used]
            .iter()
            .position(|name| *name == Some(column.as_str()))
        {
            repeated[index] = true;
        } else {
            *columns.get_mut(*used)? = Some(column.as_str());
            *used += 1;
        }
    } else if let PredicateExpr::And(children) = predicate {
        for child in children {
            collect_columns(child, columns, repeated, used, remaining_nodes)?;
        }
    }
    Some(())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentReuseCounters {
    pub(crate) hits: u64,
    pub(crate) shared_requests: u64,
    pub(crate) downstream_requests: u64,
    pub(crate) completed_segments: u64,
    pub(crate) completed_segment_bytes: u64,
    pub(crate) copied_bytes: u64,
    pub(crate) copy_nanos: u64,
    pub(crate) evictions: u64,
    pub(crate) pressure_bypasses: u64,
    pub(crate) entry_bypasses: u64,
    pub(crate) oversized_bypasses: u64,
    pub(crate) cancelled_requests: u64,
    pub(crate) failed_requests: u64,
    pub(crate) invalidations: u64,
    pub(crate) peak_entries: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentReuseSnapshot {
    pub(crate) counters: SegmentReuseCounters,
    pub(crate) admission_skipped: bool,
    pub(crate) uncached_replays: u64,
    pub(crate) discarded_attempt_nanos: u64,
    pub(crate) uncached_replay_nanos: u64,
    pub(crate) provider_background_workers: usize,
    pub(crate) closed: bool,
    pub(crate) retained_entries: usize,
    pub(crate) in_flight: usize,
    pub(crate) table_reserved_bytes: u64,
    /// Includes evicted buffers still owned by consumers, not just table entries.
    pub(crate) retention: LiveMemorySnapshot,
    /// Shared concurrent session scope; this is not cache-exclusive or process RSS.
    pub(crate) session: LiveMemorySnapshot,
}

impl SegmentReuseSnapshot {
    pub(crate) fn skipped(policy: SegmentReusePolicy, memory: &LiveMemoryPool) -> Self {
        Self {
            counters: SegmentReuseCounters::default(),
            admission_skipped: true,
            uncached_replays: 0,
            discarded_attempt_nanos: 0,
            uncached_replay_nanos: 0,
            provider_background_workers: 0,
            closed: true,
            retained_entries: 0,
            in_flight: 0,
            table_reserved_bytes: 0,
            retention: LiveMemorySnapshot {
                limit_bytes: policy.max_retained_bytes,
                reserved_bytes: 0,
                peak_reserved_bytes: 0,
                denied_reservations: 0,
            },
            session: memory.snapshot(),
        }
    }

    pub(crate) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary)
            .map_err(|error| resident_error(&format!("segment reuse summary: {error}")))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| resident_error("segment reuse summary is not an object"))?;
        let c = self.counters;
        object.insert("scan_segment_reuse".into(), serde_json::json!({
            "scope": "one_prepared_execution;serialized_native_segment_bytes;known_owned_copy;no_decoded_or_answer_cache;generation_validated;optional_retention_bypasses_under_pressure",
            "admission_skipped": self.admission_skipped,
            "uncached_replays": self.uncached_replays,
            "discarded_attempt_nanos": self.discarded_attempt_nanos,
            "uncached_replay_nanos": self.uncached_replay_nanos,
            "provider_background_workers": self.provider_background_workers,
            "provider_background_workers_scope": "completed_native_scan_and_wrapper_owned_drivers;excludes_inner_drivers_in_failed_outer_cache_attempts;not_total_threads_or_CPU_time",
            "replay_scope": "at_most_once_on_explicit_typed_owned_allocation_denial;callback_state_and_cache_dropped;same_prepared_generation;pending_native_IO_owners_remain_charged;not_an_OS_read_drain",
            "hits": c.hits, "shared_requests": c.shared_requests,
            "downstream_requests": c.downstream_requests, "completed_segments": c.completed_segments,
            "completed_segment_bytes": c.completed_segment_bytes,
            "completed_segment_bytes_scope": "logical_segment_completions_not_filesystem_or_device_reads",
            "copied_bytes": c.copied_bytes, "copy_nanos": c.copy_nanos,
            "copy_timing_scope": "direct_memcpy_only;allocation_and_admission_remain_in_query_wall",
            "evictions": c.evictions, "pressure_bypasses": c.pressure_bypasses,
            "entry_bypasses": c.entry_bypasses, "oversized_or_empty_bypasses": c.oversized_bypasses,
            "cancelled_requests": c.cancelled_requests, "failed_requests": c.failed_requests,
            "invalidations": c.invalidations, "peak_entries": c.peak_entries,
            "closed": self.closed, "retained_entries": self.retained_entries, "in_flight": self.in_flight,
            "table_reserved_bytes": self.table_reserved_bytes,
            "retention_limit_bytes": self.retention.limit_bytes,
            "retention_live_owned_bytes": self.retention.reserved_bytes,
            "retention_peak_owned_bytes": self.retention.peak_reserved_bytes,
            "retention_denied_reservations": self.retention.denied_reservations,
            "session_limit_bytes": self.session.limit_bytes,
            "session_live_owned_bytes": self.session.reserved_bytes,
            "session_peak_owned_bytes": self.session.peak_reserved_bytes,
            "session_denied_reservations": self.session.denied_reservations,
            "memory_scope": "retention_is_full_allocation_capacity_including_live_evicted_slices;session_is_shared_concurrent_owner_scope_not_cache_exclusive_or_RSS;bounded_future_metadata_excluded",
        }));
        *summary = value.to_string();
        Ok(())
    }
}

type ReadFuture = BoxFuture<'static, SharedVortexResult<BufferHandle>>;
type Validator = dyn Fn() -> VortexResult<()> + Send + Sync;

enum EntryValue {
    Ready(ByteBuffer),
    Loading {
        token: u64,
        future: WeakShared<ReadFuture>,
    },
}

struct Entry {
    id: SegmentId,
    accessed: u64,
    value: EntryValue,
}

struct State {
    entries: Vec<Entry>,
    // Reservation outlives the table allocation (field drop order).
    table_lease: MemoryLease,
    clock: u64,
    active_requests: usize,
    closed: bool,
    counters: SegmentReuseCounters,
}

impl State {
    fn tick(&mut self) -> VortexResult<u64> {
        self.clock = self
            .clock
            .checked_add(1)
            .ok_or_else(|| vortex_err!("segment reuse request counter overflow"))?;
        Ok(self.clock)
    }

    fn evict(&mut self) -> bool {
        let index = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| matches!(entry.value, EntryValue::Ready(_)))
            .min_by_key(|(_, entry)| entry.accessed)
            .map(|(index, _)| index);
        if let Some(index) = index {
            self.entries.remove(index);
            // At most one eviction per admitted request or copied-byte reservation.
            self.counters.evictions += 1;
            true
        } else {
            false
        }
    }
}

struct Inner {
    source: Arc<dyn SegmentSource>,
    validator: Arc<Validator>,
    allocator: HostAllocatorRef,
    memory: LiveMemoryPool,
    retention: LiveMemoryPool,
    policy: SegmentReusePolicy,
    state: Mutex<State>,
}

/// Construction requires the prepared source's generation validator and session
/// pool. This never retains an unobservable source allocation by its slice size.
#[derive(Clone)]
pub(crate) struct ScanSegmentReuse(Arc<Inner>);

impl ScanSegmentReuse {
    #[cfg(test)]
    pub(crate) fn new(
        source: Arc<dyn SegmentSource>,
        memory: LiveMemoryPool,
        policy: SegmentReusePolicy,
        validator: impl Fn() -> VortexResult<()> + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::try_new(source, memory, policy, validator)?
            .ok_or_else(|| resident_error("test segment reuse table admission denied"))
    }

    pub(crate) fn try_new(
        source: Arc<dyn SegmentSource>,
        memory: LiveMemoryPool,
        policy: SegmentReusePolicy,
        validator: impl Fn() -> VortexResult<()> + Send + Sync + 'static,
    ) -> Result<Option<Self>> {
        if policy.max_entries == 0
            || policy.max_entries > 65_536
            || policy.max_segment_bytes == 0
            || policy.max_retained_bytes == 0
        {
            return Err(resident_error(
                "segment reuse requires positive bounded entry and segment limits",
            ));
        }
        let table_bytes = policy
            .max_entries
            .checked_mul(std::mem::size_of::<Entry>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| resident_error("segment reuse table size overflow"))?;
        let Ok(table_lease) = memory.reserve(table_bytes) else {
            return Ok(None);
        };
        let mut entries = Vec::new();
        if entries.try_reserve_exact(policy.max_entries).is_err()
            || entries.capacity() > policy.max_entries
        {
            return Ok(None);
        }
        let retention = LiveMemoryPool::new(policy.max_retained_bytes)?;
        let allocator = Arc::new(ReservedHostAllocator::new(memory.clone()));
        Ok(Some(Self(Arc::new(Inner {
            source,
            validator: Arc::new(validator),
            allocator,
            memory,
            retention,
            policy,
            state: Mutex::new(State {
                entries,
                table_lease,
                clock: 0,
                active_requests: 0,
                closed: false,
                counters: SegmentReuseCounters::default(),
            }),
        }))))
    }

    pub(crate) fn snapshot(&self) -> VortexResult<SegmentReuseSnapshot> {
        let state = self.0.lock()?;
        Ok(SegmentReuseSnapshot {
            counters: state.counters,
            admission_skipped: false,
            uncached_replays: 0,
            discarded_attempt_nanos: 0,
            uncached_replay_nanos: 0,
            provider_background_workers: 0,
            closed: state.closed,
            retained_entries: state
                .entries
                .iter()
                .filter(|entry| matches!(entry.value, EntryValue::Ready(_)))
                .count(),
            in_flight: state.active_requests,
            table_reserved_bytes: state.table_lease.bytes(),
            retention: self.0.retention.snapshot(),
            session: self.0.memory.snapshot(),
        })
    }

    pub(crate) fn close(&self) -> VortexResult<SegmentReuseSnapshot> {
        {
            let mut state = self.0.lock()?;
            state.closed = true;
            state.entries.clear();
        }
        self.snapshot()
    }
}

impl Inner {
    fn lock(&self) -> VortexResult<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| vortex_err!("segment reuse state poisoned"))
    }

    fn validate(&self) -> VortexResult<()> {
        if let Err(error) = (self.validator)() {
            let mut state = self.lock()?;
            state.closed = true;
            state.entries.clear();
            state.counters.invalidations += 1;
            return Err(error);
        }
        if self.lock()?.closed {
            return Err(vortex_err!("scan segment reuse is closed"));
        }
        Ok(())
    }

    fn request(self: &Arc<Self>, id: SegmentId) -> VortexResult<Shared<ReadFuture>> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(vortex_err!("scan segment reuse is closed"));
        }
        let accessed = state.tick()?;
        if let Ok(index) = state.entries.binary_search_by_key(&id, |entry| entry.id) {
            state.entries[index].accessed = accessed;
            match &state.entries[index].value {
                EntryValue::Ready(buffer) => {
                    let result = BufferHandle::new_host(buffer.clone());
                    state.counters.hits += 1;
                    return Ok(futures::future::ready(Ok(result)).boxed().shared());
                }
                EntryValue::Loading { future, .. } => {
                    if let Some(future) = future.upgrade() {
                        state.counters.shared_requests += 1;
                        return Ok(future);
                    }
                }
            }
            state.entries.remove(index);
        }
        if state.entries.len() == self.policy.max_entries && !state.evict() {
            state.counters.entry_bypasses += 1;
            state.active_requests += 1;
            let inner = Arc::clone(self);
            let guard = FlightGuard {
                inner: Arc::clone(&inner),
                id,
                token: accessed,
                finished: false,
            };
            return Ok(async move {
                let mut guard = guard;
                let result = inner.read(id).await;
                guard.finish(result.is_err());
                result
            }
            .map_err(Arc::new)
            .boxed()
            .shared());
        }
        state.active_requests += 1;
        let inner = Arc::clone(self);
        // Construct before the future so dropping even an unpolled future
        // removes its weak entry. Never upgrade-and-drop other futures while
        // holding this lock: their last owner may run this same cleanup.
        let guard = FlightGuard {
            inner: Arc::clone(&inner),
            id,
            token: accessed,
            finished: false,
        };
        let future = async move {
            let mut guard = guard;
            let result = async {
                let result = inner.read(id).await?;
                let copied = inner.copy_for_retention(&result)?;
                inner.validate()?;
                if let Some(buffer) = copied {
                    let mut state = inner.lock()?;
                    if !state.closed && let Ok(index) = state.entries.binary_search_by_key(&id, |entry| entry.id)
                        && matches!(state.entries[index].value, EntryValue::Loading { token, .. } if token == accessed)
                    {
                        state.entries[index].value = EntryValue::Ready(buffer.clone());
                    }
                    Ok(BufferHandle::new_host(buffer))
                } else {
                    Ok(result)
                }
            }.await;
            guard.finish(result.is_err());
            result
        }.map_err(Arc::new).boxed().shared();
        let weak = future
            .downgrade()
            .expect("a newly constructed unpolled shared future is live");
        let index = state
            .entries
            .binary_search_by_key(&id, |entry| entry.id)
            .unwrap_err();
        state.entries.insert(
            index,
            Entry {
                id,
                accessed,
                value: EntryValue::Loading {
                    token: accessed,
                    future: weak,
                },
            },
        );
        state.counters.peak_entries = state.counters.peak_entries.max(state.entries.len());
        Ok(future)
    }

    async fn read(&self, id: SegmentId) -> VortexResult<BufferHandle> {
        self.validate()?;
        // Registration happens only after a miss is established and polled.
        self.lock()?.counters.downstream_requests += 1;
        let result = self.source.request(id).await?;
        self.validate()?;
        let mut state = self.lock()?;
        state.counters.completed_segments += 1;
        if let Some(buffer) = result.as_host_opt() {
            state.counters.completed_segment_bytes = state
                .counters
                .completed_segment_bytes
                .checked_add(buffer.len() as u64)
                .ok_or_else(|| vortex_err!("segment completion byte counter overflow"))?;
        }
        Ok(result)
    }

    fn copy_for_retention(&self, result: &BufferHandle) -> VortexResult<Option<ByteBuffer>> {
        let Some(buffer) = result.as_host_opt() else {
            return Ok(None);
        };
        // bytes::Bytes may discard empty owners; do not make an empty-owner claim.
        if buffer.is_empty() || buffer.len() > self.policy.max_segment_bytes {
            self.lock()?.counters.oversized_bypasses += 1;
            return Ok(None);
        }
        let capacity = buffer
            .len()
            .checked_add(*buffer.alignment().max(Alignment::DEFAULT_ALIGNMENT))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| vortex_err!("segment retained allocation size overflow"))?;
        let lease = loop {
            if let Ok(lease) = self.retention.reserve(capacity) {
                break lease;
            }
            let mut state = self.lock()?;
            if !state.evict() {
                state.counters.pressure_bypasses += 1;
                return Ok(None);
            }
        };
        let mut copied = loop {
            match self.allocator.allocate(buffer.len(), buffer.alignment()) {
                Ok(buffer) => break buffer,
                Err(error) if is_owned_reservation_denial(&error) => {
                    let mut state = self.lock()?;
                    if !state.evict() {
                        state.counters.pressure_bypasses += 1;
                        return Ok(None);
                    }
                }
                Err(error) => return Err(error),
            }
        };
        let started = Instant::now();
        copied.as_mut_slice().copy_from_slice(buffer.as_slice());
        let elapsed = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| vortex_err!("segment copy timing overflow"))?;
        let alignment = copied.alignment();
        let owner = RetainedBytes {
            buffer: copied.freeze(),
            _retention: lease,
        };
        let copied = ByteBuffer::from_bytes_aligned(bytes::Bytes::from_owner(owner), alignment);
        let mut state = self.lock()?;
        state.counters.copied_bytes = state
            .counters
            .copied_bytes
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| vortex_err!("segment copy byte counter overflow"))?;
        state.counters.copy_nanos = state
            .counters
            .copy_nanos
            .checked_add(elapsed)
            .ok_or_else(|| vortex_err!("segment copy timing counter overflow"))?;
        Ok(Some(copied))
    }
}

// Both credits follow the final clone/slice, including after table eviction.
struct RetainedBytes {
    buffer: ByteBuffer,
    _retention: MemoryLease,
}

impl AsRef<[u8]> for RetainedBytes {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

struct FlightGuard {
    inner: Arc<Inner>,
    id: SegmentId,
    token: u64,
    finished: bool,
}

impl FlightGuard {
    fn finish(&mut self, failed: bool) {
        self.finished = true;
        if failed {
            self.inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .counters
                .failed_requests += 1;
        }
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_requests -= 1;
        if !self.finished {
            state.counters.cancelled_requests += 1;
        }
        if let Ok(index) = state
            .entries
            .binary_search_by_key(&self.id, |entry| entry.id)
            && matches!(state.entries[index].value, EntryValue::Loading { token, .. } if token == self.token)
        {
            state.entries.remove(index);
        }
    }
}

impl SegmentSource for ScanSegmentReuse {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let inner = Arc::clone(&self.0);
        async move {
            inner.validate()?;
            let result = inner.request(id)?.await.map_err(VortexError::from)?;
            inner.validate()?;
            Ok(result)
        }
        .boxed()
    }
}

#[cfg(test)]
#[path = "resident_segment_reuse_tests.rs"]
mod tests;
