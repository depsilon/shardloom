//! Session-owned immutable native segments, independent of an operation reader.

use super::{SourceIdentity, resident_error};
use crate::owned_buffers::ReservedHostAllocator;
use shardloom_core::{Result, native_file_scan::NativeSourceSegmentCacheSnapshot};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease, OptionalMemoryReclaimer};
use std::sync::{
    Arc, Mutex, MutexGuard, Weak,
    atomic::{AtomicBool, Ordering},
};
use vortex::{
    array::buffer::BufferHandle,
    buffer::{Alignment, ByteBuffer},
    error::{VortexResult, vortex_err},
    layout::segments::SegmentId,
};

const MAX_ENTRIES: usize = 128;
const MAX_BYTES: u64 = 32 << 20;
const MAX_SEGMENT_BYTES: usize = 8 << 20;

struct Entry {
    source: Weak<SourceIdentity>,
    segment: SegmentId,
    buffer: ByteBuffer,
    accessed: u64,
    operation: u64,
    completed: bool,
    accessed_operation: u64,
}

#[derive(Default)]
struct State {
    entries: Vec<Entry>,
    metadata: Option<MemoryLease>,
    clock: u64,
    epoch: u64,
    evictions: u64,
    invalidations: u64,
    reclamations: u64,
    overflow: bool,
}

impl State {
    fn advance(&mut self) -> VortexResult<u64> {
        self.clock = self
            .clock
            .checked_add(1)
            .ok_or_else(|| vortex_err!("native segment cache access counter overflow"))?;
        Ok(self.clock)
    }

    fn clear(&mut self, invalidate: bool, reclaim: bool) {
        self.overflow |= self
            .evictions
            .checked_add(self.entries.len() as u64)
            .is_none();
        self.evictions = self.evictions.saturating_add(self.entries.len() as u64);
        self.entries = Vec::new();
        self.metadata = None;
        self.overflow |= self.epoch.checked_add(1).is_none();
        self.epoch = self.epoch.saturating_add(1);
        if invalidate {
            self.overflow |= self.invalidations.checked_add(1).is_none();
            self.invalidations = self.invalidations.saturating_add(1);
        }
        if reclaim {
            self.overflow |= self.reclamations.checked_add(1).is_none();
            self.reclamations = self.reclamations.saturating_add(1);
        }
    }

    fn evict_one(&mut self, operation: u64) -> VortexResult<bool> {
        let Some(position) = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.accessed_operation != operation)
            .min_by_key(|(_, entry)| entry.accessed)
            .map(|(position, _)| position)
        else {
            return Ok(false);
        };
        self.evictions = self
            .evictions
            .checked_add(1)
            .ok_or_else(|| vortex_err!("native segment cache eviction counter overflow"))?;
        self.entries.remove(position);
        Ok(true)
    }
}

pub(super) enum Lookup {
    Disabled,
    Hit {
        buffer: ByteBuffer,
        cross_call: bool,
    },
    Miss {
        epoch: u64,
    },
}

#[derive(Default)]
pub(super) struct CopyOutcome {
    pub(super) copied_bytes: u64,
    pub(super) stored: bool,
}

pub(super) struct Store {
    enabled: AtomicBool,
    memory: LiveMemoryPool,
    retention: LiveMemoryPool,
    state: Mutex<State>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeSourceSegmentStore")
            .field("enabled", &self.enabled.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Store {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Arc<Self>> {
        let store = Arc::new(Self {
            enabled: AtomicBool::new(false),
            memory: memory.clone(),
            retention: LiveMemoryPool::new(MAX_BYTES)?,
            state: Mutex::default(),
        });
        let endpoint: Arc<dyn OptionalMemoryReclaimer> = store.clone();
        let enabled = memory.try_install_optional_reclaimer(Arc::downgrade(&endpoint))?;
        store.enabled.store(enabled, Ordering::Release);
        Ok(store)
    }

    fn lock(&self) -> VortexResult<MutexGuard<'_, State>> {
        let state = self
            .state
            .lock()
            .map_err(|_| vortex_err!("native segment cache poisoned"))?;
        if state.overflow {
            return Err(vortex_err!("native segment cache counter overflow"));
        }
        Ok(state)
    }

    pub(super) fn lookup(
        &self,
        source: &Arc<SourceIdentity>,
        segment: SegmentId,
        operation: u64,
    ) -> VortexResult<Lookup> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(Lookup::Disabled);
        }
        let mut state = self.lock()?;
        let accessed = state.advance()?;
        let owner = Arc::downgrade(source);
        // Same-call requests preserve provider live-future sharing and the
        // action-local reuse policy. Only earlier calls serve cache hits.
        if let Some(entry) = state.entries.iter_mut().find(|entry| {
            entry.completed
                && entry.source.ptr_eq(&owner)
                && entry.segment == segment
                && entry.operation != operation
        }) {
            entry.accessed = accessed;
            entry.accessed_operation = operation;
            return Ok(Lookup::Hit {
                buffer: entry.buffer.clone(),
                cross_call: entry.operation != operation,
            });
        }
        Ok(Lookup::Miss { epoch: state.epoch })
    }

    /// Retain a known-capacity copy. This never stores or borrows a producer,
    /// reader, scope or future; the current operation still returns its input.
    pub(super) fn retain(
        &self,
        source: &Arc<SourceIdentity>,
        segment: SegmentId,
        operation: u64,
        epoch: u64,
        input: &BufferHandle,
    ) -> VortexResult<CopyOutcome> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(CopyOutcome::default());
        }
        let Some(input) = input.as_host_opt() else {
            return Ok(CopyOutcome::default());
        };
        if input.is_empty() || input.len() > MAX_SEGMENT_BYTES {
            return Ok(CopyOutcome::default());
        }
        let capacity = input
            .len()
            .checked_add(
                input
                    .alignment()
                    .max(Alignment::DEFAULT_ALIGNMENT)
                    .as_usize(),
            )
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| vortex_err!("native segment cache allocation overflow"))?;
        let Some(retained) = self.reserve_copy(source, segment, operation, epoch, capacity)? else {
            return Ok(CopyOutcome::default());
        };
        // Pool reclamation can run concurrently. Never hold the store lock
        // while allocating/copying; the epoch below prevents stale publication.
        let Some(mut output) = ReservedHostAllocator::new(self.memory.clone())
            .try_allocate(input.len(), input.alignment())?
        else {
            return Ok(CopyOutcome::default());
        };
        output.as_mut_slice().copy_from_slice(input.as_slice());
        let alignment = output.alignment();
        let buffer = ByteBuffer::from_bytes_aligned_with_owner(
            bytes::Bytes::from_owner(Retained {
                buffer: output.freeze(),
                _retention: retained,
            }),
            alignment,
        );
        let mut outcome = CopyOutcome {
            copied_bytes: input.len() as u64,
            stored: false,
        };
        let mut state = self.lock()?;
        if state.epoch != epoch {
            return Ok(outcome);
        }
        let owner = Arc::downgrade(source);
        if state
            .entries
            .iter()
            .any(|entry| entry.source.ptr_eq(&owner) && entry.segment == segment)
        {
            return Ok(outcome);
        }
        // Another completion may have filled the table during the copy.
        while state.entries.len() >= MAX_ENTRIES {
            if !state.evict_one(operation)? {
                return Ok(outcome);
            }
        }
        let accessed = state.advance()?;
        state.entries.push(Entry {
            source: owner,
            segment,
            buffer,
            accessed,
            operation,
            completed: false,
            accessed_operation: operation,
        });
        outcome.stored = true;
        Ok(outcome)
    }

    /// Admit both table storage and full retained capacity before the copy.
    /// This lock never spans native allocation or copying.
    fn reserve_copy(
        &self,
        source: &Arc<SourceIdentity>,
        segment: SegmentId,
        operation: u64,
        epoch: u64,
        capacity: u64,
    ) -> VortexResult<Option<MemoryLease>> {
        let grant = self.memory.snapshot();
        let limit = (grant.limit_bytes / 16).min(MAX_BYTES);
        if capacity > limit
            || capacity > grant.limit_bytes.saturating_sub(grant.reserved_bytes) / 16
        {
            return Ok(None);
        }
        let mut state = self.lock()?;
        if state.epoch != epoch {
            return Ok(None);
        }
        let owner = Arc::downgrade(source);
        if state
            .entries
            .iter()
            .any(|entry| entry.source.ptr_eq(&owner) && entry.segment == segment)
        {
            return Ok(None);
        }
        if state.metadata.is_none() {
            let bytes = (MAX_ENTRIES * size_of::<Entry>() + 4096) as u64;
            if bytes > grant.limit_bytes.saturating_sub(grant.reserved_bytes) / 16 {
                return Ok(None);
            }
            let Some(credit) = self
                .memory
                .try_reserve(bytes)
                .map_err(|error| vortex_err!("{error}"))?
            else {
                return Ok(None);
            };
            state
                .entries
                .try_reserve_exact(MAX_ENTRIES)
                .map_err(|error| vortex_err!("{error}"))?;
            if state.entries.capacity() > MAX_ENTRIES {
                state.entries = Vec::new();
                return Err(vortex_err!("native segment cache table capacity changed"));
            }
            state.metadata = Some(credit);
        }
        while state.entries.len() >= MAX_ENTRIES
            || self
                .retention
                .snapshot()
                .reserved_bytes
                .checked_add(capacity)
                .is_none_or(|bytes| bytes > limit)
        {
            if !state.evict_one(operation)? {
                return Ok(None);
            }
        }
        self.retention
            .try_reserve(capacity)
            .map_err(|error| vortex_err!("{error}"))
    }

    /// Publish only after the call's final consumer, generation and cancellation
    /// checks. Concurrent and failed operations cannot warm another call.
    pub(super) fn complete_operation(&self, operation: u64) -> Result<()> {
        let mut state = self
            .lock()
            .map_err(|error| resident_error(&error.to_string()))?;
        for entry in &mut state.entries {
            if entry.operation == operation {
                entry.completed = true;
            }
        }
        Ok(())
    }

    pub(super) fn discard_uncompleted_operation(&self, operation: u64) {
        if let Ok(mut state) = self.state.lock() {
            let before = state.entries.len();
            state
                .entries
                .retain(|entry| entry.completed || entry.operation != operation);
            let evicted = (before - state.entries.len()) as u64;
            state.overflow |= state.evictions.checked_add(evicted).is_none();
            state.evictions = state.evictions.saturating_add(evicted);
            if state.entries.is_empty() {
                // Another serving operation can have admitted a copy while
                // the table is still empty. Refunding the table must revoke
                // that admission before it can publish without its credit.
                state.clear(false, false);
            }
        }
    }

    pub(super) fn invalidate_source(&self, source: &Arc<SourceIdentity>, invalidated: bool) {
        if let Ok(mut state) = self.state.lock() {
            let owner = Arc::downgrade(source);
            let before = state.entries.len();
            state.entries.retain(|entry| !entry.source.ptr_eq(&owner));
            let evicted = (before - state.entries.len()) as u64;
            state.overflow |= state.evictions.checked_add(evicted).is_none()
                || state
                    .invalidations
                    .checked_add(u64::from(invalidated))
                    .is_none()
                || state.epoch.checked_add(1).is_none();
            state.evictions = state.evictions.saturating_add(evicted);
            state.invalidations = state.invalidations.saturating_add(u64::from(invalidated));
            state.epoch = state.epoch.saturating_add(1);
            if state.entries.is_empty() {
                state.entries = Vec::new();
                state.metadata = None;
            }
        }
    }

    pub(super) fn snapshot(&self) -> NativeSourceSegmentCacheSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        NativeSourceSegmentCacheSnapshot {
            enabled: self.enabled.load(Ordering::Acquire),
            retained_entries: state.entries.len() as u64,
            table_reserved_bytes: state.metadata.as_ref().map_or(0, MemoryLease::bytes),
            retained_capacity_bytes: self.retention.snapshot().reserved_bytes,
            retention_limit_bytes: (self.memory.snapshot().limit_bytes / 16).min(MAX_BYTES),
            evictions: state.evictions,
            invalidations: state.invalidations,
            pressure_reclamations: state.reclamations,
            counters_overflowed: state.overflow,
        }
    }

    pub(super) fn validate_evidence(&self) -> Result<NativeSourceSegmentCacheSnapshot> {
        let snapshot = self.snapshot();
        if snapshot.counters_overflowed {
            return Err(resident_error("native segment cache counter overflow"));
        }
        Ok(snapshot)
    }
}

impl OptionalMemoryReclaimer for Store {
    fn reclaim_optional_memory(&self) {
        if let Ok(mut state) = self.state.try_lock() {
            state.clear(false, true);
        }
    }
}

struct Retained {
    buffer: ByteBuffer,
    _retention: MemoryLease,
}
impl AsRef<[u8]> for Retained {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

#[cfg(test)]
#[path = "resident_source_segment_cache_tests.rs"]
mod tests;
