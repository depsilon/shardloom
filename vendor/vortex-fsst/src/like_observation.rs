// SPDX-License-Identifier: Apache-2.0
// ShardLoom modification: optional, debug-only session observation.

//! Explicit diagnostic observation of successful FSST LIKE calls.
//!
//! Register a fresh observer on a fresh session for each diagnostic request.
//! Snapshot only after the request drains. These logical operand counts neither
//! measure decoding nor account provider allocations. No global state is used.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use vortex_session::SessionVar;

#[derive(Debug, Default)]
struct Counts {
    counters: [AtomicU64; 9],
    overflowed: AtomicBool,
}

/// A diagnostic handle shared only by explicit clones and session registrations.
#[derive(Clone, Debug, Default)]
pub struct LikeObservation(Arc<Counts>);

/// Successful call counts read after all work in the observed request drains.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LikeObservationSnapshot {
    /// Successful FSST LIKE calls, including empty operands if executed.
    pub calls: u64,
    /// Logical operand rows, not unique rows or decoded bytes.
    pub rows: u64,
    /// Positive, supported, nonempty contains calls considered for filtering.
    pub eligible_contains_calls: u64,
    /// Calls admitting a necessary-byte filter from the actual symbol table.
    pub prefilter_calls: u64,
    /// Logical rows passed through an admitted filter, including null rows.
    pub prefilter_rows: u64,
    /// Rows rejected by the necessary-byte filter before exact matching.
    pub rejected_rows: u64,
    /// Actual row calls to the exact prefix/contains DFA.
    pub exact_rows: u64,
    /// Exact prefix/contains DFA constructions (not match-all or block rejection).
    pub matcher_constructions: u64,
    /// Calls whose active compressed span contained no possible candidate.
    pub block_rejections: u64,
    /// Whether either counter could not represent an increment.
    pub overflowed: bool,
}

impl LikeObservation {
    /// Read diagnostic counters after all callers have completed.
    pub fn snapshot(&self) -> LikeObservationSnapshot {
        LikeObservationSnapshot {
            calls: self.0.counters[0].load(Ordering::Relaxed),
            rows: self.0.counters[1].load(Ordering::Relaxed),
            eligible_contains_calls: self.0.counters[2].load(Ordering::Relaxed),
            prefilter_calls: self.0.counters[3].load(Ordering::Relaxed),
            prefilter_rows: self.0.counters[4].load(Ordering::Relaxed),
            rejected_rows: self.0.counters[5].load(Ordering::Relaxed),
            exact_rows: self.0.counters[6].load(Ordering::Relaxed),
            matcher_constructions: self.0.counters[7].load(Ordering::Relaxed),
            block_rejections: self.0.counters[8].load(Ordering::Relaxed),
            overflowed: self.0.overflowed.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record(&self, work: LikeObservationSnapshot) {
        if work.overflowed {
            self.0.overflowed.store(true, Ordering::Relaxed);
            return;
        }
        for (counter, increment) in self.0.counters.iter().zip([
            work.calls,
            work.rows,
            work.eligible_contains_calls,
            work.prefilter_calls,
            work.prefilter_rows,
            work.rejected_rows,
            work.exact_rows,
            work.matcher_constructions,
            work.block_rejections,
        ]) {
            let previous = counter.fetch_add(increment, Ordering::Relaxed);
            if previous.checked_add(increment).is_none() {
                self.0.overflowed.store(true, Ordering::Relaxed);
            }
        }
    }
}

impl SessionVar for LikeObservation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
