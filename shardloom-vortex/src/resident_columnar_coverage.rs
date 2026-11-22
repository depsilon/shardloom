//! Optional, completed native column coverage for one held local generation.
//! The session's existing segment owner delegates reclamation here. No record
//! retains a session, source descriptor, decoder, future or consumer cursor.

use crate::{
    resident_memory_source::DetachedColumnarBatch,
    source_identity::{FileGeneration, SourceIdentity},
};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

const MAX_RECORDS: usize = 128;
const MAX_FRAGMENTS: usize = 128;
const MAX_BYTES: u64 = 32 << 20;

#[derive(Clone, Copy)]
pub(crate) struct Region {
    pub(crate) index: usize,
    pub(crate) first_row: u64,
    pub(crate) rows: u64,
}

pub(crate) struct Fragment {
    pub(crate) first_row: u64,
    pub(crate) batch: DetachedColumnarBatch,
}

pub(crate) struct Coverage {
    path: PathBuf,
    generation: FileGeneration,
    pub(crate) region: Region,
    pub(crate) fields: Vec<usize>,
    pub(crate) fragments: Vec<Fragment>,
    filled: u64,
    bytes: u64,
    validated: bool,
    _metadata: MemoryLease,
}

impl Coverage {
    fn matches(&self, source: &SourceIdentity, region: Region) -> bool {
        self.path == source.path()
            && self.generation == source.generation
            && self.region.index == region.index
            && self.region.first_row == region.first_row
            && self.region.rows == region.rows
    }
}

struct Entry {
    id: u64,
    operation: u64,
    completed: bool,
    data: Arc<Coverage>,
}

#[derive(Default)]
struct State {
    entries: Vec<Entry>,
    metadata: Option<MemoryLease>,
    epoch: u64,
    next_id: u64,
    disabled: bool,
}

impl State {
    fn bytes(&self) -> u64 {
        self.entries.iter().fold(
            self.metadata.as_ref().map_or(0, MemoryLease::bytes),
            |sum, entry| sum.saturating_add(entry.data.bytes),
        )
    }

    fn clear(&mut self) {
        self.entries = Vec::new();
        self.metadata = None;
        if let Some(epoch) = self.epoch.checked_add(1) {
            self.epoch = epoch;
        } else {
            self.disabled = true;
        }
    }

    fn remove(&mut self, id: u64) {
        self.entries.retain(|entry| entry.id != id);
        if self.entries.is_empty() {
            self.clear();
        }
    }

    fn make_room(&mut self, bytes: u64, limit: u64, preserve: Option<u64>) -> bool {
        while self
            .bytes()
            .checked_add(bytes)
            .is_none_or(|sum| sum > limit)
            || (preserve.is_none() && self.entries.len() >= MAX_RECORDS)
        {
            let Some(index) = self
                .entries
                .iter()
                .position(|entry| Some(entry.id) != preserve)
            else {
                return false;
            };
            // Bounded FIFO avoids uncredited access maps and never drops an
            // acquired consumer's references or its original allocation credits.
            self.entries.remove(index);
        }
        true
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Ticket {
    epoch: u64,
    id: u64,
    operation: u64,
}

pub(crate) struct Hit {
    pub(crate) coverage: Arc<Coverage>,
    pub(crate) field: usize,
}

pub(crate) struct Acquisition {
    /// Positions match the exact requested source field order.
    pub(crate) hits: Vec<Option<Hit>>,
    pub(crate) missing: Vec<usize>,
    _metadata: MemoryLease,
}

pub(crate) struct Store {
    memory: LiveMemoryPool,
    state: Mutex<State>,
}

impl Store {
    pub(super) fn new(memory: &LiveMemoryPool) -> Self {
        Self {
            memory: memory.clone(),
            state: Mutex::default(),
        }
    }

    /// Caller validates the current held generation before and after use.
    /// Contention/admission refusal declines optional reuse before any decoding.
    pub(crate) fn acquire(
        &self,
        source: &SourceIdentity,
        region: Region,
        fields: &[usize],
        operation: u64,
    ) -> Option<Acquisition> {
        if fields.is_empty() || fields.len() > 128 || region.rows == 0 {
            return None;
        }
        let bytes = 4096_u64.checked_add((fields.len() as u64).checked_mul(128)?)?;
        let credit = self.memory.try_reserve(bytes).ok()??;
        let grant = self.memory.snapshot();
        let limit = (grant.limit_bytes / 16).min(MAX_BYTES);
        let mut state = self.state.try_lock().ok()?;
        if state.disabled {
            return None;
        }
        state.entries.retain(|entry| {
            entry.data.path != source.path() || entry.data.generation == source.generation
        });
        if state.bytes() > limit {
            state.clear();
        }
        let mut hits = Vec::new();
        let mut missing = Vec::new();
        hits.try_reserve_exact(fields.len()).ok()?;
        missing.try_reserve_exact(fields.len()).ok()?;
        if hits.capacity() > fields.len() || missing.capacity() > fields.len() {
            return None;
        }
        for &field in fields {
            let hit = state.entries.iter().find_map(|entry| {
                (entry.completed
                    && entry.operation != operation
                    && entry.data.matches(source, region))
                .then(|| entry.data.fields.iter().position(|&index| index == field))
                .flatten()
                .map(|field| Hit {
                    coverage: Arc::clone(&entry.data),
                    field,
                })
            });
            if hit.is_none() {
                missing.push(field);
            }
            hits.push(hit);
        }
        Some(Acquisition {
            hits,
            missing,
            _metadata: credit,
        })
    }

    pub(crate) fn begin(
        &self,
        source: &SourceIdentity,
        region: Region,
        fields: &[usize],
        operation: u64,
    ) -> Option<Ticket> {
        if fields.is_empty() || fields.len() > 128 || region.rows == 0 {
            return None;
        }
        let path_bytes = source.path().as_os_str().len();
        let bytes = 4096_u64
            .checked_add((MAX_FRAGMENTS * size_of::<Fragment>()) as u64)?
            .checked_add(std::mem::size_of_val(fields) as u64)?
            .checked_add(path_bytes as u64)?;
        let grant = self.memory.snapshot();
        let limit = (grant.limit_bytes / 16).min(MAX_BYTES);
        if bytes > limit || bytes > grant.limit_bytes.saturating_sub(grant.reserved_bytes) / 16 {
            return None;
        }
        let mut state = self.state.try_lock().ok()?;
        if state.disabled || !state.make_room(bytes, limit, None) {
            return None;
        }
        if state.metadata.is_none() {
            let table_bytes = (MAX_RECORDS * size_of::<Entry>() + 1024) as u64;
            if table_bytes.checked_add(bytes)? > limit {
                return None;
            }
            let credit = self.memory.try_reserve(table_bytes).ok()??;
            state.entries.try_reserve_exact(MAX_RECORDS).ok()?;
            if state.entries.capacity() > MAX_RECORDS {
                state.clear();
                return None;
            }
            state.metadata = Some(credit);
        }
        if !state.make_room(bytes, limit, None) {
            return None;
        }
        let metadata = self.memory.try_reserve(bytes).ok()??;
        let mut path = PathBuf::new();
        path.try_reserve_exact(path_bytes).ok()?;
        path.push(source.path());
        let mut owned_fields = Vec::new();
        owned_fields.try_reserve_exact(fields.len()).ok()?;
        owned_fields.extend_from_slice(fields);
        let mut fragments = Vec::new();
        fragments.try_reserve_exact(MAX_FRAGMENTS).ok()?;
        if path.capacity() > path_bytes
            || owned_fields.capacity() > fields.len()
            || fragments.capacity() > MAX_FRAGMENTS
        {
            return None;
        }
        let id = state.next_id.checked_add(1)?;
        state.next_id = id;
        let epoch = state.epoch;
        state.entries.push(Entry {
            id,
            operation,
            completed: false,
            data: Arc::new(Coverage {
                path,
                generation: source.generation.clone(),
                region,
                fields: owned_fields,
                fragments,
                filled: 0,
                bytes,
                validated: false,
                _metadata: metadata,
            }),
        });
        Some(Ticket {
            epoch,
            id,
            operation,
        })
    }

    pub(crate) fn append(&self, ticket: Ticket, first_row: u64, batch: DetachedColumnarBatch) {
        let limit = (self.memory.snapshot().limit_bytes / 16).min(MAX_BYTES);
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        if state.disabled || state.epoch != ticket.epoch {
            return;
        }
        if !state.entries.iter().any(|entry| {
            entry.id == ticket.id && entry.operation == ticket.operation && !entry.completed
        }) {
            return;
        }
        let bytes = batch.retained_bytes();
        if !state.make_room(bytes, limit, Some(ticket.id)) {
            state.remove(ticket.id);
            return;
        }
        let Some(entry) = state.entries.iter_mut().find(|entry| {
            entry.id == ticket.id && entry.operation == ticket.operation && !entry.completed
        }) else {
            return;
        };
        let Some(data) = Arc::get_mut(&mut entry.data) else {
            return;
        };
        let rows = batch.rows() as u64;
        let end = data.filled.checked_add(rows);
        if data.validated
            || rows == 0
            || data.fragments.len() == MAX_FRAGMENTS
            || data.region.first_row.checked_add(data.filled) != Some(first_row)
            || end.is_none_or(|end| end > data.region.rows)
            || batch.columns() != data.fields.len()
            || data
                .fragments
                .first()
                .is_some_and(|first| first.batch.dtype() != batch.dtype())
            || data.bytes.checked_add(bytes).is_none()
        {
            state.remove(ticket.id);
            return;
        }
        data.filled = end.expect("checked region extent");
        data.bytes += bytes;
        data.fragments.push(Fragment { first_row, batch });
    }

    /// Decoder EOF/extent and current source generation must already be checked.
    pub(crate) fn validate(&self, ticket: Ticket) {
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        if state.epoch != ticket.epoch {
            return;
        }
        let Some(entry) = state.entries.iter_mut().find(|entry| {
            entry.id == ticket.id && entry.operation == ticket.operation && !entry.completed
        }) else {
            return;
        };
        let Some(data) = Arc::get_mut(&mut entry.data) else {
            return;
        };
        if data.filled == data.region.rows && !data.fragments.is_empty() {
            data.validated = true;
        } else {
            state.remove(ticket.id);
        }
    }

    pub(crate) fn discard(&self, ticket: Ticket) {
        if let Ok(mut state) = self.state.try_lock()
            && state.epoch == ticket.epoch
        {
            state.remove(ticket.id);
        }
    }

    pub(super) fn complete_operation(&self, operation: u64) {
        if let Ok(mut state) = self.state.try_lock() {
            state
                .entries
                .retain(|entry| entry.operation != operation || entry.data.validated);
            for entry in &mut state.entries {
                if entry.operation == operation {
                    entry.completed = true;
                }
            }
            if state.entries.is_empty() {
                state.clear();
            }
        }
    }

    pub(super) fn discard_uncompleted_operation(&self, operation: u64) {
        if let Ok(mut state) = self.state.try_lock() {
            state
                .entries
                .retain(|entry| entry.completed || entry.operation != operation);
            if state.entries.is_empty() {
                state.clear();
            }
        }
    }

    pub(crate) fn invalidate(&self, source: &SourceIdentity) {
        if let Ok(mut state) = self.state.try_lock() {
            state
                .entries
                .retain(|entry| entry.data.path != source.path());
            if state.entries.is_empty() {
                state.clear();
            }
        }
    }

    pub(super) fn reclaim(&self) {
        if let Ok(mut state) = self.state.try_lock() {
            state.clear();
        }
    }

    pub(crate) fn retained_bytes(&self) -> Option<u64> {
        // Observation must not turn brief lock contention into zero retention.
        self.state.lock().ok().map(|state| state.bytes())
    }
}
