//! Bounded coalesced byte-range prefetch for selective `SegmentSource` scans (`E2E-ARCH-03`).
//!
//! Used by wide `SortRows` late materialization (`ScanBuilder::with_row_indices`)
//! to coalesce nearby projected column segments into fewer bounded byte-range
//! reads under explicit [`LiveMemoryPool`] headroom and cooperative cancellation,
//! releasing all retained prefetch buffers on scope exit.

#[cfg(unix)]
use crate::resident_session::{ReadWork, SourceIdentity};
use futures::FutureExt as _;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use vortex::{
    array::buffer::BufferHandle,
    buffer::{Alignment, ByteBufferMut},
    error::vortex_err,
    file::SegmentSpec,
    layout::{
        LayoutChildType, LayoutRef,
        layouts::struct_::Struct,
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
};

const DEFAULT_GAP_LIMIT_BYTES: u64 = 64 * 1024;
const DEFAULT_COALESCED_RANGE_LIMIT_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_PREFETCH_BUFFER_LIMIT_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_AMPLIFICATION_NUMERATOR: u64 = 4;
const DEFAULT_AMPLIFICATION_DENOMINATOR: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentPrefetchPolicy {
    pub(crate) gap_limit_bytes: u64,
    pub(crate) coalesced_range_limit_bytes: u64,
    pub(crate) read_amplification_numerator: u64,
    pub(crate) read_amplification_denominator: u64,
    pub(crate) prefetch_buffer_limit_bytes: u64,
}

impl Default for SegmentPrefetchPolicy {
    fn default() -> Self {
        Self {
            gap_limit_bytes: DEFAULT_GAP_LIMIT_BYTES,
            coalesced_range_limit_bytes: DEFAULT_COALESCED_RANGE_LIMIT_BYTES,
            read_amplification_numerator: DEFAULT_AMPLIFICATION_NUMERATOR,
            read_amplification_denominator: DEFAULT_AMPLIFICATION_DENOMINATOR,
            prefetch_buffer_limit_bytes: DEFAULT_PREFETCH_BUFFER_LIMIT_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentByteSpan {
    pub(crate) id: SegmentId,
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) alignment: Alignment,
}

impl SegmentByteSpan {
    fn end_offset(self) -> Option<u64> {
        self.offset.checked_add(u64::from(self.length))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoalescedByteRange {
    pub(crate) start_offset: u64,
    pub(crate) end_offset: u64,
    pub(crate) useful_bytes: u64,
    pub(crate) segments: Vec<SegmentByteSpan>,
}

impl CoalescedByteRange {
    #[must_use]
    pub(crate) const fn span_bytes(&self) -> u64 {
        self.end_offset.saturating_sub(self.start_offset)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrefetchStats {
    pub(crate) coalesced_reads: u64,
    pub(crate) prefetched_segments: u64,
    pub(crate) prefetch_hits: u64,
    pub(crate) fallback_reads: u64,
}

/// Plan sorted, bounded coalesced byte ranges from segment spans while strictly
/// enforcing `gap_limit_bytes`, `coalesced_range_limit_bytes`, and `<= 4x` read
/// amplification (`span_bytes / useful_bytes`).
pub(crate) fn plan_coalesced_byte_ranges(
    segments: &[SegmentByteSpan],
    policy: &SegmentPrefetchPolicy,
) -> Vec<CoalescedByteRange> {
    if policy.coalesced_range_limit_bytes == 0 || policy.read_amplification_denominator == 0 {
        return Vec::new();
    }
    let mut seen_ids = BTreeSet::new();
    let mut normalized = segments
        .iter()
        .copied()
        .filter(|span| {
            span.length > 0
                && span.end_offset().is_some()
                && u64::from(span.length) <= policy.coalesced_range_limit_bytes
                && seen_ids.insert(span.id)
        })
        .collect::<Vec<_>>();
    normalized.sort_by_key(|span| (span.offset, span.length, span.id));

    let num = u128::from(policy.read_amplification_numerator);
    let denom = u128::from(policy.read_amplification_denominator);
    let mut ranges: Vec<CoalescedByteRange> = Vec::new();

    for span in normalized {
        let Some(seg_end) = span.end_offset() else {
            continue;
        };
        let seg_len = u64::from(span.length);
        if let Some(current) = ranges.last_mut() {
            let gap = span.offset.saturating_sub(current.end_offset);
            let new_end = current.end_offset.max(seg_end);
            let new_span = new_end.saturating_sub(current.start_offset);
            let overlap_bytes = current.end_offset.saturating_sub(span.offset).min(seg_len);
            let new_useful = current
                .useful_bytes
                .saturating_add(seg_len.saturating_sub(overlap_bytes));
            let amplification_ok = new_useful > 0
                && u128::from(new_span).saturating_mul(denom)
                    <= u128::from(new_useful).saturating_mul(num);
            if gap <= policy.gap_limit_bytes
                && new_span <= policy.coalesced_range_limit_bytes
                && amplification_ok
            {
                current.end_offset = new_end;
                current.useful_bytes = new_useful;
                current.segments.push(span);
                continue;
            }
        }
        ranges.push(CoalescedByteRange {
            start_offset: span.offset,
            end_offset: seg_end,
            useful_bytes: seg_len,
            segments: vec![span],
        });
    }

    ranges
}

/// Collect segment IDs referenced by `projected_columns` without decoding arrays.
pub(crate) fn collect_projected_segment_ids(
    layout: &LayoutRef,
    projected_columns: &[String],
) -> Result<Vec<SegmentId>> {
    fn visit(
        layout: &LayoutRef,
        wanted_fields: Option<&BTreeSet<&str>>,
        seen: &mut BTreeSet<SegmentId>,
    ) -> Result<()> {
        for segment_id in layout.segment_ids() {
            seen.insert(segment_id);
        }
        if layout.is::<Struct>() {
            for slot in 0..layout.nslots() {
                let keep = match layout.slot_type(slot) {
                    Some(LayoutChildType::Field(field_name)) => {
                        wanted_fields.is_none_or(|fields| fields.contains(field_name.as_ref()))
                    }
                    Some(LayoutChildType::Auxiliary(_)) => true,
                    Some(LayoutChildType::Transparent(_) | LayoutChildType::Chunk(_)) | None => {
                        false
                    }
                };
                if !keep {
                    continue;
                }
                if let Some(child) = layout.slot(slot).map_err(prefetch_error)? {
                    // Nested struct columns inside an already-selected field include all children.
                    visit(&child, None, seen)?;
                }
            }
            return Ok(());
        }
        for slot in 0..layout.nslots() {
            if let Some(child) = layout.slot(slot).map_err(prefetch_error)? {
                visit(&child, wanted_fields, seen)?;
            }
        }
        Ok(())
    }

    let wanted = projected_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let filter = (!wanted.is_empty()).then_some(&wanted);
    visit(layout, filter, &mut seen)?;
    Ok(seen.into_iter().collect())
}

#[derive(Default)]
struct PrefetchState {
    entries: BTreeMap<SegmentId, (BufferHandle, Option<MemoryLease>)>,
    retained_bytes: u64,
    closed: bool,
}

pub(crate) struct BoundedPrefetchSegmentSource {
    fallback: Arc<dyn SegmentSource>,
    path: PathBuf,
    segment_map: Vec<SegmentSpec>,
    policy: SegmentPrefetchPolicy,
    memory: Option<LiveMemoryPool>,
    cancellation: CancellationToken,
    #[cfg(unix)]
    held_source: Option<(Arc<SourceIdentity>, Arc<ReadWork>)>,
    state: Mutex<PrefetchState>,
    coalesced_reads: AtomicU64,
    prefetched_segments: AtomicU64,
    prefetch_hits: AtomicU64,
    fallback_reads: AtomicU64,
}

pub(crate) struct PrefetchScanGuard(Arc<BoundedPrefetchSegmentSource>);

impl PrefetchScanGuard {
    #[must_use]
    pub(crate) fn new(source: Arc<BoundedPrefetchSegmentSource>) -> Self {
        Self(source)
    }
}

impl Drop for PrefetchScanGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl BoundedPrefetchSegmentSource {
    pub(crate) fn new(
        fallback: Arc<dyn SegmentSource>,
        path: PathBuf,
        segment_map: &[SegmentSpec],
        policy: SegmentPrefetchPolicy,
        memory: Option<LiveMemoryPool>,
        cancellation: CancellationToken,
        #[cfg(unix)] held_source: Option<(Arc<SourceIdentity>, Arc<ReadWork>)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            fallback,
            path,
            segment_map: segment_map.to_vec(),
            policy,
            memory,
            cancellation,
            #[cfg(unix)]
            held_source,
            state: Mutex::default(),
            coalesced_reads: AtomicU64::new(0),
            prefetched_segments: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            fallback_reads: AtomicU64::new(0),
        })
    }

    pub(crate) fn prefetch_segments(&self, segment_ids: &[SegmentId]) -> Result<PrefetchStats> {
        self.cancellation.check()?;
        if segment_ids.is_empty()
            || self.policy.prefetch_buffer_limit_bytes == 0
            || self.is_closed()?
        {
            return Ok(self.stats());
        }
        let spans = self.collect_segment_spans(segment_ids);
        let ranges = plan_coalesced_byte_ranges(&spans, &self.policy);
        if ranges.is_empty() {
            return Ok(self.stats());
        }
        let buffer_cap = self.buffer_cap_bytes();
        if buffer_cap == 0 {
            return Ok(self.stats());
        }
        let reopened_file = self.open_reopened_file_if_needed()?;
        for range in ranges {
            self.cancellation.check()?;
            if !self.prefetch_one_range(&range, buffer_cap, reopened_file.as_ref())? {
                break;
            }
        }
        Ok(self.stats())
    }

    fn is_closed(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| prefetch_error("segment prefetch state poisoned"))?
            .closed)
    }

    fn collect_segment_spans(&self, segment_ids: &[SegmentId]) -> Vec<SegmentByteSpan> {
        let mut spans = Vec::with_capacity(segment_ids.len());
        for &id in segment_ids {
            let Some(&spec) = self.segment_map.get(*id as usize) else {
                continue;
            };
            spans.push(SegmentByteSpan {
                id,
                offset: spec.offset,
                length: spec.length,
                alignment: spec.alignment,
            });
        }
        spans
    }

    fn buffer_cap_bytes(&self) -> u64 {
        match &self.memory {
            Some(pool) => {
                let snapshot = pool.snapshot();
                let available = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
                self.policy.prefetch_buffer_limit_bytes.min(available / 4)
            }
            None => self.policy.prefetch_buffer_limit_bytes,
        }
    }

    fn open_reopened_file_if_needed(&self) -> Result<Option<File>> {
        #[cfg(unix)]
        if self.held_source.is_some() {
            return Ok(None);
        }
        File::open(&self.path).map(Some).map_err(prefetch_error)
    }

    fn read_coalesced_range(
        &self,
        range: &CoalescedByteRange,
        reopened_file: Option<&File>,
        span_usize: usize,
    ) -> Result<Vec<u8>> {
        #[cfg(unix)]
        let file_ref: &File = if let Some((identity, _)) = &self.held_source {
            identity.validate()?;
            if range.end_offset > identity.generation.len {
                return Err(prefetch_error(
                    "coalesced segment prefetch exceeded held source generation length",
                ));
            }
            &identity.file
        } else {
            reopened_file.expect("reopened file active")
        };
        #[cfg(not(unix))]
        let file_ref: &File = reopened_file.expect("reopened file active");

        let mut scratch = vec![0_u8; span_usize];
        vortex::io::std_file::read_exact_at(file_ref, &mut scratch, range.start_offset)
            .map_err(prefetch_error)?;

        #[cfg(unix)]
        if let Some((identity, read_work)) = &self.held_source {
            identity.validate()?;
            read_work.read_completed(span_usize)?;
        }
        Ok(scratch)
    }

    fn prefetch_one_range(
        &self,
        range: &CoalescedByteRange,
        buffer_cap: u64,
        reopened_file: Option<&File>,
    ) -> Result<bool> {
        let current_retained = {
            let state = self
                .state
                .lock()
                .map_err(|_| prefetch_error("segment prefetch state poisoned"))?;
            if state.closed {
                return Ok(false);
            }
            state.retained_bytes
        };
        if current_retained.saturating_add(range.useful_bytes) > buffer_cap {
            return Ok(false);
        }
        let span_u64 = range.span_bytes();
        let Ok(span_usize) = usize::try_from(span_u64) else {
            return Ok(false);
        };
        if span_usize == 0 {
            return Ok(true);
        }
        let range_lease = match &self.memory {
            Some(pool) => {
                let Some(lease) = pool.try_reserve(span_u64)? else {
                    return Ok(false);
                };
                Some(lease)
            }
            None => None,
        };

        let scratch = self.read_coalesced_range(range, reopened_file, span_usize)?;
        self.coalesced_reads.fetch_add(1, Ordering::Relaxed);

        let mut staged = Vec::with_capacity(range.segments.len());
        let mut memory_exhausted = false;
        for seg in &range.segments {
            self.cancellation.check()?;
            let Some(rel_start_u64) = seg.offset.checked_sub(range.start_offset) else {
                continue;
            };
            let Ok(rel_start) = usize::try_from(rel_start_u64) else {
                continue;
            };
            let seg_len = seg.length as usize;
            let Some(rel_end) = rel_start.checked_add(seg_len) else {
                continue;
            };
            if rel_end > scratch.len() {
                continue;
            }
            let seg_lease = match &self.memory {
                Some(pool) => {
                    if let Some(lease) = pool.try_reserve(u64::from(seg.length))? {
                        Some(lease)
                    } else {
                        memory_exhausted = true;
                        break;
                    }
                }
                None => None,
            };
            let buffer =
                ByteBufferMut::copy_from_aligned(&scratch[rel_start..rel_end], seg.alignment)
                    .freeze();
            staged.push((seg.id, BufferHandle::new_host(buffer), seg_lease));
        }
        drop(scratch);
        drop(range_lease);

        let mut state = self
            .state
            .lock()
            .map_err(|_| prefetch_error("segment prefetch state poisoned"))?;
        if state.closed {
            return Ok(false);
        }
        for (id, handle, lease) in staged {
            let entry_bytes = lease
                .as_ref()
                .map_or(handle.len() as u64, MemoryLease::bytes);
            if !state.entries.contains_key(&id) {
                state.retained_bytes = state.retained_bytes.saturating_add(entry_bytes);
                self.prefetched_segments.fetch_add(1, Ordering::Relaxed);
            }
            state.entries.insert(id, (handle, lease));
        }
        Ok(!memory_exhausted)
    }

    pub(crate) fn close(&self) {
        let drained = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.retained_bytes = 0;
            std::mem::take(&mut state.entries)
        };
        drop(drained);
    }

    #[must_use]
    pub(crate) fn stats(&self) -> PrefetchStats {
        PrefetchStats {
            coalesced_reads: self.coalesced_reads.load(Ordering::Relaxed),
            prefetched_segments: self.prefetched_segments.load(Ordering::Relaxed),
            prefetch_hits: self.prefetch_hits.load(Ordering::Relaxed),
            fallback_reads: self.fallback_reads.load(Ordering::Relaxed),
        }
    }
}

impl SegmentSource for BoundedPrefetchSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        if let Err(error) = self.cancellation.check() {
            return futures::future::ready(Err(vortex_err!("{error}"))).boxed();
        }
        let hit = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                None
            } else {
                state.entries.get(&id).map(|(handle, _)| handle.clone())
            }
        };
        if let Some(handle) = hit {
            self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
            #[cfg(unix)]
            if let Some((identity, read_work)) = &self.held_source
                && let Err(error) = identity
                    .validate()
                    .and_then(|()| read_work.segment_requested())
            {
                return futures::future::ready(Err(vortex_err!("{error}"))).boxed();
            }
            return futures::future::ready(Ok(handle)).boxed();
        }
        self.fallback_reads.fetch_add(1, Ordering::Relaxed);
        self.fallback.request(id)
    }
}

fn prefetch_error(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native coalesced segment prefetch failed: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "resident_segment_prefetch_tests.rs"]
mod tests;
