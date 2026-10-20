//! Serial opt-in width observations, shared unchanged by both frozen binaries.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);
static OWNERS: AtomicU64 = AtomicU64::new(0);
static INPUT_ROWS: AtomicU64 = AtomicU64::new(0);
static MAX_ROWS: AtomicU64 = AtomicU64::new(0);
static TAKE_ARRAYS: AtomicU64 = AtomicU64::new(0);
static TAKE_ROWS: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)] // The original binary has no compact-owner calls by design.
pub(super) fn owner(rows: usize) {
    if ACTIVE.load(Ordering::Relaxed) {
        OWNERS.fetch_add(1, Ordering::Relaxed);
        INPUT_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
        MAX_ROWS.fetch_max(rows as u64, Ordering::Relaxed);
    }
}

#[allow(dead_code)] // The original binary has no U16 selection arrays by design.
pub(super) fn take(rows: usize) {
    if ACTIVE.load(Ordering::Relaxed) {
        TAKE_ARRAYS.fetch_add(1, Ordering::Relaxed);
        TAKE_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
    }
}

pub(super) struct Session;

impl Session {
    pub(super) fn start() -> Self {
        assert!(
            !ACTIVE.swap(true, Ordering::SeqCst),
            "compact selection observation requires serial execution"
        );
        for counter in [&OWNERS, &INPUT_ROWS, &MAX_ROWS, &TAKE_ARRAYS, &TAKE_ROWS] {
            counter.store(0, Ordering::Relaxed);
        }
        Self
    }

    pub(super) fn finish(self) -> serde_json::Value {
        let result = serde_json::json!({
            "compact_owners":OWNERS.load(Ordering::Relaxed),
            "compact_input_rows":INPUT_ROWS.load(Ordering::Relaxed),
            "maximum_local_extent":MAX_ROWS.load(Ordering::Relaxed),
            "u16_take_arrays":TAKE_ARRAYS.load(Ordering::Relaxed),
            "u16_take_rows":TAKE_ROWS.load(Ordering::Relaxed),
        });
        drop(self);
        result
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::SeqCst);
    }
}
