// SPDX-License-Identifier: Apache-2.0
// ShardLoom modification: optional, operation-owned exact FSST matcher tables.

//! One positive-contains table per explicitly exclusive native operation.
//!
//! Keys contain exact symbols, lengths and pattern bytes. Row results, backing
//! rejection, offsets and validity never enter this store. Caller-held tables
//! retain their own admission credit after the operation registry is cleared.

#![expect(
    clippy::disallowed_types,
    reason = "Poisoned operation state must decline optional matcher reuse"
)]

use std::any::Any;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use fsst::Symbol;
use vortex_array::memory::{HostAllocatorRef, HostMemoryReservation, HostMemorySessionExt};
use vortex_error::{VortexResult, vortex_ensure};
use vortex_session::{SessionExt, SessionVar, VortexSession};

use crate::dfa::FlatContainsDfa;

/// Conservative fixed registry storage, excluding the existing session type map.
pub const REGISTRY_BYTES: usize = 512;
/// Fixed operation state, inline identity and Arc/owner control storage.
pub const SCOPE_BYTES: usize = 4096;
/// Maximum contains construction plus retained tables and owner control storage.
/// Two 256*255 byte tables, 255*255 symbol transitions and a 254-byte failure
/// vector total less than this bound, including their small fixed headers.
pub const TABLE_BYTES: usize = 256 * 1024;
const MAX_SYMBOLS: usize = 255;
const MAX_PATTERN_BYTES: usize = 512;

/// Optional session marker; only a weak operation pointer survives between calls.
#[derive(Clone, Default)]
pub struct MatcherReuse(Option<Arc<Registry>>);

struct Registry {
    active: Mutex<Weak<State>>,
    allocator: HostAllocatorRef,
    _credit: HostMemoryReservation,
}

struct State {
    data: Mutex<Data>,
    _credit: HostMemoryReservation,
}

struct Data {
    active: bool,
    entry: Option<Entry>,
    counts: MatcherReuseSnapshot,
    overflowed: bool,
}

struct Entry {
    key: Key,
    table: Arc<CompiledContains>,
}

struct Key {
    symbols: [u64; MAX_SYMBOLS],
    lengths: [u8; MAX_SYMBOLS],
    pattern: [u8; MAX_PATTERN_BYTES],
    symbols_len: usize,
    pattern_len: usize,
}

/// A retained immutable DFA; its own credit outlives every borrowed use.
pub(crate) struct CompiledContains {
    pub(crate) dfa: FlatContainsDfa,
    _credit: HostMemoryReservation,
}

/// Exclusive native-operation owner. Drop clears the registry before refunding.
pub struct MatcherReuseScope {
    registry: Arc<Registry>,
    state: Arc<State>,
    session: Option<VortexSession>,
}

/// Per-operation provider lookups; these are not row or decoded-byte counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MatcherReuseSnapshot {
    /// Positive contains calls reaching table lookup after backing rejection.
    pub lookups: u64,
    /// Exact table/length/pattern hits in the admitted positive-contains domain.
    pub hits: u64,
    /// Empty or different-identity entries observed during lookup.
    pub misses: u64,
    /// Actual admitted DFA constructions, including concurrent transient users.
    pub constructions: u64,
    /// Table reservations denied before cache-owned table allocation.
    pub denied: u64,
    /// Actual vector capacity of the single retained entry, if present.
    pub retained_table_capacity_bytes: usize,
    /// Retained reservation, including the conservative construction allowance.
    pub retained_reservation_bytes: usize,
}

impl std::fmt::Debug for MatcherReuse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatcherReuse")
            .field(
                "active",
                &self.0.as_ref().is_some_and(|registry| {
                    registry
                        .active
                        .lock()
                        .is_ok_and(|state| state.strong_count() != 0)
                }),
            )
            .finish_non_exhaustive()
    }
}

impl SessionVar for MatcherReuse {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl MatcherReuse {
    /// Admit the marker before installing it into the existing native session.
    pub fn try_new(allocator: &HostAllocatorRef) -> VortexResult<Self> {
        const {
            assert!(size_of::<Registry>() + 128 <= REGISTRY_BYTES);
        }
        let credit = allocator.reserve_external(REGISTRY_BYTES)?;
        Ok(Self(Some(Arc::new(Registry {
            active: Mutex::new(Weak::new()),
            allocator: Arc::clone(allocator),
            _credit: credit,
        }))))
    }

    /// Install charged state only for an exclusively admitted operation. The
    /// scope restores an empty session marker before its ownership is released.
    /// Session type-map storage remains ordinary provider infrastructure.
    pub fn begin_in(session: &VortexSession) -> Option<MatcherReuseScope> {
        if let Some(current) = session.get_opt::<Self>()
            && let Some(registry) = &current.0
            && let Some(previous) = registry.active.lock().ok()?.upgrade()
        {
            previous.disable();
            return None;
        }
        let reuse = Self::try_new(&session.host_allocator()).ok()?;
        let mut scope = reuse.begin()?;
        session.register(reuse);
        scope.session = Some(session.clone());
        Some(scope)
    }

    /// Begin an exclusively admitted operation. Denial allocates no cache state.
    /// Overlap disables reuse in the prior scope and declines the new scope.
    pub fn begin(&self) -> Option<MatcherReuseScope> {
        const {
            assert!(size_of::<State>() + 128 <= SCOPE_BYTES);
        }
        let registry = self.0.as_ref()?;
        let previous = registry.active.lock().ok()?.upgrade();
        if let Some(previous) = previous {
            previous.disable();
            return None;
        }
        let credit = registry.allocator.reserve_external(SCOPE_BYTES).ok()?;
        let mut active = registry.active.lock().ok()?;
        if let Some(previous) = active.upgrade() {
            drop(active);
            previous.disable();
            return None;
        }
        let state = Arc::new(State {
            data: Mutex::new(Data {
                active: true,
                entry: None,
                counts: MatcherReuseSnapshot::default(),
                overflowed: false,
            }),
            _credit: credit,
        });
        *active = Arc::downgrade(&state);
        Some(MatcherReuseScope {
            registry: Arc::clone(registry),
            state,
            session: None,
        })
    }

    /// Only positive, case-sensitive, nonempty literal contains reaches here.
    /// A distinct allocator cannot borrow another native owner's admission.
    pub(crate) fn get_or_compile(
        &self,
        symbols: &[Symbol],
        lengths: &[u8],
        pattern: &[u8],
        needle: &[u8],
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Option<(Arc<CompiledContains>, bool)>> {
        let Some(registry) = &self.0 else {
            return Ok(None);
        };
        if !Arc::ptr_eq(allocator, &registry.allocator)
            || symbols.len() > MAX_SYMBOLS
            || symbols.len() != lengths.len()
            || lengths.iter().any(|len| !(1..=8).contains(len))
            || pattern.len() > MAX_PATTERN_BYTES
            || needle.is_empty()
            || needle.len() > FlatContainsDfa::MAX_NEEDLE_LEN
        {
            return Ok(None);
        }
        let Some(state) = registry
            .active
            .lock()
            .ok()
            .and_then(|active| active.upgrade())
        else {
            return Ok(None);
        };
        {
            let Ok(mut data) = state.data.lock() else {
                return Ok(None);
            };
            if !data.active {
                return Ok(None);
            }
            data.increment(|counts| &mut counts.lookups);
            let hit = data
                .entry
                .as_ref()
                .filter(|entry| entry.key.matches(symbols, lengths, pattern))
                .map(|entry| Arc::clone(&entry.table));
            if let Some(table) = hit {
                data.increment(|counts| &mut counts.hits);
                return Ok(Some((table, false)));
            }
            data.increment(|counts| &mut counts.misses);
            if data.entry.is_some() {
                return Ok(None);
            }
        }
        // No lock is held while invoking an allocator or constructing tables.
        // Concurrent constructors each own credit; only one entry is retained.
        let reservation_bytes = table_reservation_bytes(symbols.len(), needle.len());
        let Ok(credit) = allocator.reserve_external(reservation_bytes) else {
            if let Ok(mut data) = state.data.lock() {
                data.increment(|counts| &mut counts.denied);
            }
            return Ok(None);
        };
        let dfa = FlatContainsDfa::new(symbols, lengths, needle)?;
        let capacity = dfa.heap_capacity_bytes();
        vortex_ensure!(
            capacity <= 2 * 256 * (needle.len() + 1),
            "FSST matcher table capacity exceeds admitted bound"
        );
        let table = Arc::new(CompiledContains {
            dfa,
            _credit: credit,
        });
        if let Ok(mut data) = state.data.lock() {
            data.increment(|counts| &mut counts.constructions);
            if data.active && data.entry.is_none() {
                data.entry = Some(Entry {
                    key: Key::new(symbols, lengths, pattern),
                    table: Arc::clone(&table),
                });
                data.counts.retained_table_capacity_bytes = capacity;
                data.counts.retained_reservation_bytes = reservation_bytes;
            }
        }
        Ok(Some((table, true)))
    }
}

fn table_reservation_bytes(symbols: usize, needle: usize) -> usize {
    // Existing constructors allocate exact-length u8 vectors: two 256-wide
    // tables, one symbol table, and the KMP failure vector. Their lifetimes
    // overlap by less than this sum. 256 bytes cover the fixed Arc/owner headers.
    let bytes = (2 * 256 + symbols) * (needle + 1) + needle + 256;
    debug_assert!(bytes <= TABLE_BYTES);
    bytes
}

impl Key {
    fn new(symbols: &[Symbol], lengths: &[u8], pattern: &[u8]) -> Self {
        let mut key = Self {
            symbols: [0; MAX_SYMBOLS],
            lengths: [0; MAX_SYMBOLS],
            pattern: [0; MAX_PATTERN_BYTES],
            symbols_len: symbols.len(),
            pattern_len: pattern.len(),
        };
        for (target, symbol) in key.symbols.iter_mut().zip(symbols) {
            *target = symbol.to_u64();
        }
        key.lengths[..lengths.len()].copy_from_slice(lengths);
        key.pattern[..pattern.len()].copy_from_slice(pattern);
        key
    }

    fn matches(&self, symbols: &[Symbol], lengths: &[u8], pattern: &[u8]) -> bool {
        self.symbols_len == symbols.len()
            && self.pattern_len == pattern.len()
            && self.lengths[..self.symbols_len] == *lengths
            && self.pattern[..self.pattern_len] == *pattern
            && self.symbols[..self.symbols_len]
                .iter()
                .zip(symbols)
                .all(|(stored, symbol)| *stored == symbol.to_u64())
    }
}

impl Data {
    fn increment(&mut self, field: impl FnOnce(&mut MatcherReuseSnapshot) -> &mut u64) {
        let count = field(&mut self.counts);
        match count.checked_add(1) {
            Some(next) => *count = next,
            None => self.overflowed = true,
        }
    }
}

impl State {
    fn disable(&self) {
        let retired = {
            let mut data = self.data.lock().unwrap_or_else(PoisonError::into_inner);
            data.active = false;
            data.entry.take()
        };
        drop(retired);
    }
}

impl MatcherReuseScope {
    /// Read only after the operation's provider work has drained.
    pub fn snapshot(&self) -> Option<MatcherReuseSnapshot> {
        let data = self.state.data.lock().ok()?;
        (data.active && !data.overflowed).then_some(data.counts)
    }
}

impl Drop for MatcherReuseScope {
    fn drop(&mut self) {
        let mut active = self
            .registry
            .active
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if Weak::ptr_eq(&active, &Arc::downgrade(&self.state)) {
            *active = Weak::new();
        }
        drop(active);
        self.state.disable();
        if let Some(session) = &self.session {
            let matches = session.get_opt::<MatcherReuse>().is_some_and(|current| {
                current
                    .0
                    .as_ref()
                    .is_some_and(|registry| Arc::ptr_eq(registry, &self.registry))
            });
            if matches {
                session.register(MatcherReuse::default());
            }
        }
    }
}

#[cfg(test)]
mod tests;
