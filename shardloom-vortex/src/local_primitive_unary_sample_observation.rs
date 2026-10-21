//! Opt-in, calling-thread-only counts around the unchanged standard partition search.
//! This module is absent from ordinary builds and makes no timing claim.

use std::{cell::RefCell, marker::PhantomData, rc::Rc};

#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Counts {
    pub(crate) populations: u64,
    pub(crate) population_entries: u64,
    pub(crate) logical_draws: u64,
    pub(crate) searches: u64,
    pub(crate) comparisons: u64,
    pub(crate) maximum_comparisons: u64,
    pub(crate) calibrations: u64,
    pub(crate) calibration_entries: u64,
    pub(crate) models_admitted: u64,
    pub(crate) bound_rejections: u64,
    pub(crate) credit_bypasses: u64,
    pub(crate) model_searches: u64,
    pub(crate) maximum_radius: u64,
    pub(crate) maximum_model_credit: u64,
    pub(crate) model_reuses: u64,
    pub(crate) refusal_reuses: u64,
    pub(crate) maximum_owner_credit: u64,
}

thread_local! {
    static ACTIVE: RefCell<Option<Counts>> = const { RefCell::new(None) };
}

pub(crate) struct Session(PhantomData<Rc<()>>);

impl Session {
    pub(crate) fn start() -> Self {
        ACTIVE.with_borrow_mut(|slot| {
            assert!(slot.is_none(), "sample observation must not be nested");
            *slot = Some(Counts::default());
        });
        Self(PhantomData)
    }

    pub(crate) fn finish(self) -> Counts {
        let counts = ACTIVE.with_borrow_mut(|slot| slot.take().expect("active sample observation"));
        drop(self);
        counts
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        ACTIVE.with_borrow_mut(|slot| *slot = None);
    }
}

pub(crate) fn population(entries: usize, draws: usize) {
    ACTIVE.with_borrow_mut(|slot| {
        if let Some(counts) = slot {
            counts.populations += 1;
            counts.population_entries += entries as u64;
            counts.logical_draws += draws as u64;
        }
    });
}

pub(crate) fn partition_point<T>(values: &[T], mut predicate: impl FnMut(&T) -> bool) -> usize {
    if !ACTIVE.with_borrow(Option::is_some) {
        return values.partition_point(predicate);
    }
    let mut comparisons = 0;
    let index = values.partition_point(|value| {
        comparisons += 1;
        predicate(value)
    });
    ACTIVE.with_borrow_mut(|slot| {
        let counts = slot.as_mut().expect("active sample observation");
        counts.searches += 1;
        counts.comparisons += comparisons;
        counts.maximum_comparisons = counts.maximum_comparisons.max(comparisons);
    });
    index
}

#[allow(dead_code)] // The ordinary baseline deliberately has no model calls.
pub(crate) fn calibrated(entries: usize, radius: Option<usize>, credited_bytes: u64) {
    ACTIVE.with_borrow_mut(|slot| {
        if let Some(counts) = slot {
            counts.calibrations += 1;
            counts.calibration_entries += entries as u64;
            counts.maximum_model_credit = counts.maximum_model_credit.max(credited_bytes);
            if let Some(radius) = radius {
                counts.models_admitted += 1;
                counts.maximum_radius = counts.maximum_radius.max(radius as u64);
            } else {
                counts.bound_rejections += 1;
            }
        }
    });
}

#[allow(dead_code)] // The ordinary baseline deliberately has no model calls.
pub(crate) fn credit_bypass() {
    ACTIVE.with_borrow_mut(|slot| {
        if let Some(counts) = slot {
            counts.credit_bypasses += 1;
        }
    });
}

#[allow(dead_code)] // The ordinary baseline deliberately has no model calls.
pub(crate) fn model_search() {
    ACTIVE.with_borrow_mut(|slot| {
        if let Some(counts) = slot {
            counts.model_searches += 1;
        }
    });
}

#[test]
fn sample_observation_preserves_empty_duplicate_and_gap_boundaries() {
    for values in [vec![], vec![1.0], vec![1.0, 1.0, 1.0, 4.0, 9.0]] {
        for threshold in [0.0, 1.0, 2.0, 4.0, 8.0, 9.0, 10.0] {
            let expected = values
                .iter()
                .position(|&v| v >= threshold)
                .unwrap_or(values.len());
            assert_eq!(partition_point(&values, |&v| v < threshold), expected);
            assert!(ACTIVE.with_borrow(Option::is_none));
            let session = Session::start();
            let mut actual_calls = 0;
            assert_eq!(
                partition_point(&values, |&v| {
                    actual_calls += 1;
                    v < threshold
                }),
                expected
            );
            let counts = session.finish();
            assert_eq!(counts.searches, 1);
            assert_eq!(counts.comparisons, actual_calls);
            assert_eq!(counts.maximum_comparisons, actual_calls);
            assert!(ACTIVE.with_borrow(Option::is_none));
        }
    }
}

#[test]
fn sample_observation_clears_on_success_error_and_unwind() {
    let session = Session::start();
    population(4, 7);
    assert_eq!(session.finish().logical_draws, 7);
    let failure = || -> Result<(), &'static str> {
        let _session = Session::start();
        population(99, 999);
        Err("deliberate early return")
    };
    assert_eq!(failure(), Err("deliberate early return"));
    assert!(
        std::panic::catch_unwind(|| {
            let _session = Session::start();
            partition_point(&[1, 2], |_| panic!("deliberate predicate unwind"));
        })
        .is_err()
    );
    assert!(ACTIVE.with_borrow(Option::is_none));
    assert_eq!(Session::start().finish(), Counts::default());
}

#[test]
fn sample_observation_is_thread_local_and_rejects_nested_sessions() {
    let session = Session::start();
    population(4, 7);
    assert!(std::panic::catch_unwind(Session::start).is_err());
    let other = std::thread::spawn(|| {
        let session = Session::start();
        population(8, 2);
        session.finish()
    })
    .join()
    .unwrap();
    assert_eq!(other.population_entries, 8);
    let counts = session.finish();
    assert_eq!(counts.population_entries, 4);
    assert_eq!(counts.logical_draws, 7);
}

#[allow(dead_code)] // Baseline owns no preparation model.
pub(crate) fn reused(radius: Option<usize>, owner_bytes: u64, model_bytes: u64) {
    ACTIVE.with_borrow_mut(|slot| {
        if let Some(counts) = slot {
            counts.maximum_owner_credit = counts.maximum_owner_credit.max(owner_bytes);
            counts.maximum_model_credit = counts.maximum_model_credit.max(model_bytes);
            if let Some(radius) = radius {
                counts.model_reuses += 1;
                counts.maximum_radius = counts.maximum_radius.max(radius as u64);
            } else {
                counts.refusal_reuses += 1;
            }
        }
    });
}

#[allow(dead_code)] // Baseline owns no preparation model.
pub(crate) fn owner_credit(bytes: u64) {
    ACTIVE.with_borrow_mut(|slot| {
        if let Some(counts) = slot {
            counts.maximum_owner_credit = counts.maximum_owner_credit.max(bytes);
        }
    });
}
