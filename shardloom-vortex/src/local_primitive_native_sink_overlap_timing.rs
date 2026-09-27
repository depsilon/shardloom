//! Test-only caller attribution; provider work may overlap these elapsed spans.

use std::{cell::RefCell, collections::BTreeMap, time::Instant};

#[derive(Default)]
pub(super) struct Timings {
    nanos: BTreeMap<&'static str, u128>,
    calls: BTreeMap<&'static str, u64>,
    provider_buffered_bytes_at_push_peak: u64,
}

impl Timings {
    pub(super) fn into_json(self) -> serde_json::Value {
        serde_json::json!({
            "nanos": self.nanos,
            "calls": self.calls,
            "provider_buffered_bytes_at_push_peak": self.provider_buffered_bytes_at_push_peak,
        })
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Timings>> = const { RefCell::new(None) };
}

pub(super) fn measure<T>(operation: impl FnOnce() -> T) -> (T, Timings) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            CURRENT.with(|current| current.borrow_mut().take());
        }
    }
    CURRENT.with(|current| {
        assert!(current.borrow().is_none());
        *current.borrow_mut() = Some(Timings::default());
    });
    let _reset = Reset;
    let result = operation();
    let timings = CURRENT.with(|current| current.borrow_mut().take().unwrap());
    (result, timings)
}

pub(super) fn clock() -> Option<Instant> {
    CURRENT.with(|current| current.borrow().is_some().then(Instant::now))
}

pub(super) fn record(name: &'static str, start: Option<Instant>) {
    if let Some(start) = start {
        let nanos = start.elapsed().as_nanos();
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            let timing = current.as_mut().unwrap();
            *timing.nanos.entry(name).or_default() += nanos;
            *timing.calls.entry(name).or_default() += 1;
        });
    }
}

pub(super) fn buffered(bytes: u64) {
    CURRENT.with(|current| {
        if let Some(timing) = current.borrow_mut().as_mut() {
            timing.provider_buffered_bytes_at_push_peak =
                timing.provider_buffered_bytes_at_push_peak.max(bytes);
        }
    });
}

pub(super) fn instrument<I: Iterator>(input: I) -> impl Iterator<Item = I::Item> {
    struct Measured<I>(I);
    impl<I: Iterator> Iterator for Measured<I> {
        type Item = I::Item;
        fn next(&mut self) -> Option<Self::Item> {
            let start = clock();
            let item = self.0.next();
            record("scan_next", start);
            item
        }
    }
    Measured(input)
}
