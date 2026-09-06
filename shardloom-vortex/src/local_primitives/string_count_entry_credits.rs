//! Distinct-entry admission, independent of the table's byte reservations.
//! Only block claims and returns synchronize. A block's per-entry consumption
//! is local; its used count becomes visible when the block returns.

use super::string_count_partitions::failed;
use shardloom_core::Result;
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

pub(super) const BLOCK_ENTRIES: usize = 1024;
const CANCEL_POLL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Default)]
pub(super) struct CreditEvidence {
    pub committed: usize,
    pub reserved: usize,
    pub claim_calls: u64,
    pub granted_entries: u64,
    pub return_calls: u64,
    pub refunded_entries: u64,
    pub wait_calls: u64,
}

#[derive(Default)]
struct State {
    evidence: CreditEvidence,
    fault: Option<&'static str>,
}

pub(super) struct EntryCredits {
    limit: usize,
    state: Mutex<State>,
    available: Condvar,
    committed: AtomicUsize,
}

pub(super) enum Claim<'a> {
    Block(EntryBlock<'a>),
    Exhausted,
    Stopped,
}

pub(super) struct EntryBlock<'a> {
    owner: &'a EntryCredits,
    granted: usize,
    remaining: usize,
}

impl EntryCredits {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::new(State::default()),
            available: Condvar::new(),
            committed: AtomicUsize::new(0),
        }
    }

    /// Exact after all reducers drain. Active blocks have not published their
    /// used entries yet; this is a committed-progress lower bound while running.
    pub(super) fn committed(&self) -> usize {
        self.committed.load(Ordering::Acquire)
    }

    pub(super) fn evidence(&self) -> Result<CreditEvidence> {
        let state = self
            .state
            .lock()
            .map_err(|_| failed("entry credit lock poisoned"))?;
        if let Some(fault) = state.fault {
            return Err(failed(fault));
        }
        Ok(state.evidence)
    }

    pub(super) fn wake(&self) {
        self.available.notify_all();
    }

    /// The caller must hold neither a partition guard nor another entry block.
    /// `continue_work` checks cancellation and the existing byte-pressure flag.
    pub(super) fn claim(
        &self,
        requested: usize,
        mut continue_work: impl FnMut() -> Result<bool>,
    ) -> Result<Claim<'_>> {
        if requested == 0 {
            return Err(failed("zero-sized entry credit claim"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| failed("entry credit lock poisoned"))?;
        loop {
            if !continue_work()? {
                return Ok(Claim::Stopped);
            }
            if let Some(fault) = state.fault {
                return Err(failed(fault));
            }
            let evidence = &mut state.evidence;
            let free = self
                .limit
                .checked_sub(evidence.committed)
                .and_then(|free| free.checked_sub(evidence.reserved))
                .ok_or_else(|| failed("entry credit limit invariant failed"))?;
            if free != 0 {
                let granted = requested.min(BLOCK_ENTRIES).min(free);
                let claim_calls = evidence
                    .claim_calls
                    .checked_add(1)
                    .ok_or_else(|| failed("entry credit claim counter overflowed"))?;
                let granted_entries = evidence
                    .granted_entries
                    .checked_add(
                        u64::try_from(granted).map_err(|_| failed("entry grant exceeds u64"))?,
                    )
                    .ok_or_else(|| failed("entry credit grant counter overflowed"))?;
                // `granted <= free` proves this sum remains within the limit.
                evidence.reserved += granted;
                evidence.claim_calls = claim_calls;
                evidence.granted_entries = granted_entries;
                return Ok(Claim::Block(EntryBlock {
                    owner: self,
                    granted,
                    remaining: granted,
                }));
            }
            if evidence.reserved == 0 {
                return Ok(Claim::Exhausted);
            }
            // Outstanding blocks can still refund capacity. Treating their
            // ownership as exhaustion would spuriously enter the handoff path.
            evidence.wait_calls = evidence
                .wait_calls
                .checked_add(1)
                .ok_or_else(|| failed("entry credit wait counter overflowed"))?;
            #[cfg(test)]
            if evidence.wait_calls == 1 {
                self.available.notify_all();
            }
            state = self
                .available
                .wait_timeout(state, CANCEL_POLL)
                .map_err(|_| failed("entry credit wait lock poisoned"))?
                .0;
        }
    }

    #[cfg(test)]
    pub(super) fn wait_until_blocked(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut state = self.state.lock().unwrap();
        while state.evidence.wait_calls == 0 {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .expect("entry claimant never waited");
            state = self.available.wait_timeout(state, remaining).unwrap().0;
        }
    }
}

impl EntryBlock<'_> {
    pub(super) fn remaining(&self) -> usize {
        self.remaining
    }

    pub(super) fn consume_one(&mut self) -> Result<()> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or_else(|| failed("entry inserted without an admitted credit"))?;
        Ok(())
    }
}

impl Drop for EntryBlock<'_> {
    fn drop(&mut self) {
        // Recover the lock for cleanup even if another thread unwound. Admission
        // and final evidence still reject a poisoned mutex; credits never leak.
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let used = self.granted - self.remaining;
        let next = state
            .evidence
            .reserved
            .checked_sub(self.granted)
            .zip(state.evidence.committed.checked_add(used))
            .filter(|(reserved, committed)| {
                committed
                    .checked_add(*reserved)
                    .is_some_and(|total| total <= self.owner.limit)
            });
        if let Some((reserved, committed)) = next {
            state.evidence.reserved = reserved;
            state.evidence.committed = committed;
            self.owner.committed.store(committed, Ordering::Release);
        } else {
            state.fault = Some("entry credit return invariant failed");
        }
        let counters = state.evidence.return_calls.checked_add(1).zip(
            u64::try_from(self.remaining)
                .ok()
                .and_then(|unused| state.evidence.refunded_entries.checked_add(unused)),
        );
        if let Some((calls, refunded)) = counters {
            state.evidence.return_calls = calls;
            state.evidence.refunded_entries = refunded;
        } else {
            state.fault = Some("entry credit return counter overflowed");
        }
        drop(state);
        self.owner.wake();
    }
}

#[cfg(test)]
#[path = "string_count_entry_credits_tests.rs"]
mod tests;
