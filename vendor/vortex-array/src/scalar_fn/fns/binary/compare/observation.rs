// SPDX-License-Identifier: Apache-2.0
//! Optional, fixed-size observations of successful encoded comparison dispatch.

// Poisoning is part of the observer's fail-closed attribution contract: a panic
// while holding the registry makes counts unavailable. parking_lot does not
// provide this signal, so this module deliberately uses the standard mutex.
#![expect(
    clippy::disallowed_types,
    reason = "Observer attribution requires mutex poisoning"
)]

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use vortex_session::{SessionExt, SessionVar, VortexSession};

/// A session-local weak observer registry. Cloned sessions share the registry;
/// consumers must establish exclusive operation ownership before starting a scope.
#[derive(Clone, Default)]
pub struct CompareWork(Arc<Mutex<Weak<State>>>);

impl std::fmt::Debug for CompareWork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompareWork")
            .field(
                "active",
                &self.0.lock().is_ok_and(|state| state.strong_count() != 0),
            )
            .finish_non_exhaustive()
    }
}

struct State {
    counts: [AtomicU64; 6],
    overflow: AtomicBool,
    _ownership: Arc<dyn Any + Send + Sync>,
}

/// Operation-owned counters and resource credit. Dropping the scope clears the
/// session's weak registry before releasing the counted state allocation.
pub struct CompareWorkScope {
    state: Arc<State>,
    registry: Arc<Mutex<Weak<State>>>,
}

impl SessionVar for CompareWork {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl CompareWork {
    /// Start an exclusively owned operation with the caller's resource credit.
    /// An overlapping scope invalidates attribution for both operations.
    pub fn begin(&self, ownership: Arc<dyn Any + Send + Sync>) -> Option<CompareWorkScope> {
        let mut active = self.0.lock().ok()?;
        if let Some(previous) = active.upgrade() {
            previous.overflow.store(true, Ordering::Relaxed);
            return None;
        }
        let state = Arc::new(State {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            overflow: AtomicBool::new(false),
            _ownership: ownership,
        });
        *active = Arc::downgrade(&state);
        Some(CompareWorkScope {
            state,
            registry: Arc::clone(&self.0),
        })
    }
}

impl CompareWorkScope {
    /// Snapshot calls and logical operand rows in ALP, BitPacked, FoR order.
    /// No decode/physical-I/O inference is supported. Overflow is unavailable.
    pub fn snapshot(&self) -> Option<[u64; 6]> {
        let values = std::array::from_fn(|index| self.state.counts[index].load(Ordering::Relaxed));
        (!self.state.overflow.load(Ordering::Relaxed)).then_some(values)
    }
}

impl Drop for CompareWorkScope {
    fn drop(&mut self) {
        // Weak pointers retain the Arc allocation after its value is dropped.
        // Clear the registry while the counted state and its credit are alive,
        // including when an earlier observer access poisoned the registry.
        let mut active = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
        if Weak::ptr_eq(&active, &Arc::downgrade(&self.state)) {
            *active = Weak::new();
        }
    }
}

impl State {
    fn record(&self, family: usize, rows: u64) {
        for (offset, value) in [(0, 1), (1, rows)] {
            if self.counts[family * 2 + offset]
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    count.checked_add(value)
                })
                .is_err()
            {
                self.overflow.store(true, Ordering::Relaxed);
            }
        }
    }
}

pub(super) fn family(id: &str) -> Option<usize> {
    match id {
        "vortex.alp" => Some(0),
        "fastlanes.bitpacked.v2" => Some(1),
        "fastlanes.for.v2" => Some(2),
        _ => None,
    }
}

pub(super) fn record(session: &VortexSession, family: usize, rows: usize) {
    if let Some(observer) = session.get_opt::<CompareWork>() {
        let active = match observer.0.lock() {
            Ok(active) => active,
            Err(poisoned) => {
                if let Some(state) = poisoned.into_inner().upgrade() {
                    state.overflow.store(true, Ordering::Relaxed);
                }
                return;
            }
        };
        let Some(state) = active.upgrade() else {
            return;
        };
        drop(active);
        if let Ok(rows) = u64::try_from(rows) {
            state.record(family, rows);
        } else {
            state.overflow.store(true, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_work_is_optional_shared_and_overflow_is_unavailable() {
        let session = VortexSession::empty();
        record(&session, 0, 10);
        assert!(session.get_opt::<CompareWork>().is_none());
        let observer = CompareWork::default();
        let work = observer.begin(Arc::new(())).unwrap();
        let session = session.with_some(observer.clone());
        record(&session.clone(), 0, 10);
        record(&session, 1, 20);
        record(&session, 2, 30);
        assert_eq!(work.snapshot(), Some([1, 10, 1, 20, 1, 30]));
        work.state.counts[0].store(u64::MAX, Ordering::Relaxed);
        record(&session, 0, 1);
        assert_eq!(work.snapshot(), None);
        drop(work);
        assert!(Weak::ptr_eq(&observer.0.lock().unwrap(), &Weak::new()));
        record(&session, 0, 99);
        let fresh = observer.begin(Arc::new(())).unwrap();
        assert_eq!(fresh.snapshot(), Some([0; 6]));
        assert!(observer.begin(Arc::new(())).is_none());
        assert_eq!(fresh.snapshot(), None);
    }

    #[test]
    fn compare_work_releases_ownership_and_poison_never_reports_partial_counts() {
        let observer = CompareWork::default();
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let work = observer.begin(owner).unwrap();
        let session = VortexSession::empty().with_some(observer.clone());
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = observer.0.lock().unwrap();
                panic!("test poisoned observer");
            })
            .is_err()
        );
        record(&session, 0, 10);
        assert_eq!(work.snapshot(), None);
        drop(work);
        assert!(weak.upgrade().is_none());
        assert!(Weak::ptr_eq(
            &observer.0.lock().unwrap_or_else(PoisonError::into_inner),
            &Weak::new()
        ));
    }

    #[test]
    fn compare_work_clears_registry_while_inflight_record_retains_credit() {
        let observer = CompareWork::default();
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let work = observer.begin(owner).unwrap();
        let inflight = Arc::clone(&work.state);
        drop(work);
        assert!(Weak::ptr_eq(&observer.0.lock().unwrap(), &Weak::new()));
        assert!(weak.upgrade().is_some());
        let fresh = observer.begin(Arc::new(())).unwrap();
        inflight.record(0, 9);
        assert_eq!(fresh.snapshot(), Some([0; 6]));
        drop(inflight);
        assert!(weak.upgrade().is_none());
        drop(fresh);
        assert!(Weak::ptr_eq(&observer.0.lock().unwrap(), &Weak::new()));
    }
}
