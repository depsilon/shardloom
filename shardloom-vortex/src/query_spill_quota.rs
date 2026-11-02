//! One explicit primitive spill declaration shares its live run-byte ceiling.
//!
//! Stores retain committed bytes until owned unlink. A declaration keeps only a
//! weak scope, so a later completed action starts fresh rather than accumulating
//! history or retaining run state. This is not a filesystem-wide usage monitor.

use shardloom_core::{Result, ShardLoomError};
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone, Debug, Default)]
pub(crate) struct Owner(Arc<Mutex<Weak<Quota>>>);

impl Owner {
    pub(crate) fn bind(&self, limit_bytes: u64) -> Result<Arc<Quota>> {
        if limit_bytes == 0 {
            return Err(failed("byte ceiling must be positive"));
        }
        let mut scope = self.0.lock().map_err(|_| failed("owner is poisoned"))?;
        if let Some(active) = scope.upgrade() {
            if active.limit_bytes != limit_bytes {
                return Err(failed("byte ceiling changed while spill owners are active"));
            }
            return Ok(active);
        }
        let active = Arc::new(Quota {
            limit_bytes,
            state: Mutex::new(Snapshot::default()),
        });
        *scope = Arc::downgrade(&active);
        Ok(active)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(clippy::struct_field_names)] // Explicit byte units distinguish quota evidence from run counts.
pub(crate) struct Snapshot {
    pub(crate) live_bytes: u64,
    pub(crate) pending_bytes: u64,
    pub(crate) peak_live_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct Quota {
    limit_bytes: u64,
    state: Mutex<Snapshot>,
}

impl Quota {
    pub(crate) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<PendingWrite> {
        let mut state = self.state.lock().map_err(|_| failed("scope is poisoned"))?;
        let accounted = state
            .live_bytes
            .checked_add(state.pending_bytes)
            .ok_or_else(|| failed("byte accounting overflow"))?;
        if bytes > self.limit_bytes.saturating_sub(accounted) {
            return Err(failed(
                "byte quota exhausted by live runs and pending writes",
            ));
        }
        state.pending_bytes += bytes;
        Ok(PendingWrite {
            quota: Arc::clone(self),
            bytes,
        })
    }

    /// Call only after the corresponding owned payload/marker has been removed.
    pub(crate) fn removed(&self, bytes: u64) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| failed("scope is poisoned"))?;
        state.live_bytes = state
            .live_bytes
            .checked_sub(bytes)
            .ok_or_else(|| failed("removed bytes exceed owned live runs"))?;
        Ok(())
    }

    #[cfg(any(test, all(feature = "vortex-write", unix)))]
    pub(crate) fn snapshot(&self) -> Result<Snapshot> {
        self.state
            .lock()
            .map(|state| *state)
            .map_err(|_| failed("scope is poisoned"))
    }
}

pub(crate) struct PendingWrite {
    quota: Arc<Quota>,
    bytes: u64,
}

impl PendingWrite {
    /// Commit only the bytes actually written. Short writes refund their unused
    /// admission; failed writes drop the admission without claiming disk work.
    pub(crate) fn commit(mut self, written: u64) -> Result<()> {
        if written > self.bytes {
            return Err(failed("write exceeded its admitted bytes"));
        }
        let mut state = self
            .quota
            .state
            .lock()
            .map_err(|_| failed("scope is poisoned"))?;
        state.pending_bytes -= self.bytes;
        state.live_bytes += written;
        state.peak_live_bytes = state.peak_live_bytes.max(state.live_bytes);
        self.bytes = 0;
        Ok(())
    }
}

impl Drop for PendingWrite {
    fn drop(&mut self) {
        if self.bytes != 0
            && let Ok(mut state) = self.quota.state.lock()
        {
            state.pending_bytes -= self.bytes;
        }
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native query spill quota: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_runs_and_pending_writes_share_one_ceiling_and_actual_peak() {
        let owner = Owner::default();
        let first = owner.bind(100).unwrap();
        let second = owner.clone().bind(100).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        first.reserve(32).unwrap().commit(32).unwrap();
        let writing = second.reserve(60).unwrap();
        assert!(first.reserve(9).is_err());
        assert_eq!(first.snapshot().unwrap().peak_live_bytes, 32);
        writing.commit(17).unwrap();
        assert_eq!(first.snapshot().unwrap().live_bytes, 49);
        assert_eq!(first.snapshot().unwrap().peak_live_bytes, 49);
        assert_eq!(first.snapshot().unwrap().pending_bytes, 0);
        drop(second.reserve(51).unwrap());
        assert_eq!(first.snapshot().unwrap().pending_bytes, 0);
        assert!(second.reserve(52).is_err());
        first.removed(32).unwrap();
        second.reserve(83).unwrap().commit(83).unwrap();
        assert_eq!(first.snapshot().unwrap().peak_live_bytes, 100);
        second.removed(100).unwrap();
        assert_eq!(first.snapshot().unwrap().live_bytes, 0);
        assert!(first.removed(1).is_err());
    }

    #[test]
    fn declarations_rebind_only_after_active_scopes_end() {
        let owner = Owner::default();
        let scope = owner.bind(100).unwrap();
        scope.reserve(37).unwrap().commit(37).unwrap();
        assert!(owner.bind(200).is_err());
        let held = owner.bind(100).unwrap();
        drop(scope);
        assert_eq!(held.snapshot().unwrap().live_bytes, 37);
        held.removed(37).unwrap();
        assert!(
            owner.bind(200).is_err(),
            "a live empty scope retains its ceiling"
        );
        drop(held);
        let fresh = owner.bind(200).unwrap();
        assert_eq!(fresh.snapshot().unwrap(), Snapshot::default());
        fresh.reserve(200).unwrap().commit(200).unwrap();
        assert!(fresh.reserve(1).is_err());
        assert!(Owner::default().bind(0).is_err());
    }

    #[test]
    fn invalid_write_commit_refunds_only_its_pending_admission() {
        let scope = Owner::default().bind(100).unwrap();
        scope.reserve(12).unwrap().commit(12).unwrap();
        assert!(scope.reserve(8).unwrap().commit(9).is_err());
        assert_eq!(scope.snapshot().unwrap().live_bytes, 12);
        assert_eq!(scope.snapshot().unwrap().pending_bytes, 0);
        assert_eq!(scope.snapshot().unwrap().peak_live_bytes, 12);
    }
}
