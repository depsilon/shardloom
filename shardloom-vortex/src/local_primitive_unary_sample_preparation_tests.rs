use super::super::{sample::Score, sample_observation};
use super::*;

fn population(cumulative: impl IntoIterator<Item = f64>) -> Vec<Candidate> {
    cumulative
        .into_iter()
        .enumerate()
        .map(|(ordinal, cumulative_weight)| Candidate {
            score: Score::Integer(0),
            ordinal,
            cumulative_weight,
            row: None,
        })
        .collect()
}

fn weighted(weights: impl IntoIterator<Item = f64>) -> Vec<Candidate> {
    let mut total = 0.0;
    population(weights.into_iter().map(|weight| {
        total += weight;
        total
    }))
}

fn uniform() -> Vec<Candidate> {
    population((1..=4096).map(f64::from))
}

fn owner(rows: usize) -> (LiveMemoryPool, Box<Preparation>) {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let preparation = Preparation::new(rows, &memory).unwrap().unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, OWNER_BYTES);
    (memory, preparation)
}

fn boundaries(model: Model, values: &[Candidate]) {
    let expected = |threshold| values.partition_point(|entry| entry.cumulative_weight < threshold);
    let mut previous = 0.0;
    for entry in values {
        let value = entry.cumulative_weight;
        for threshold in [
            value.next_down(),
            value,
            value.next_up(),
            previous + (value - previous) / 2.0,
        ] {
            assert_eq!(
                model.search(values, threshold),
                expected(threshold),
                "{threshold:?}"
            );
        }
        previous = value;
    }
    for threshold in [
        f64::NEG_INFINITY,
        -1.0,
        -0.0,
        0.0,
        f64::from_bits(1),
        f64::INFINITY,
        f64::NAN,
    ] {
        assert_eq!(
            model.search(values, threshold),
            expected(threshold),
            "{threshold:?}"
        );
    }
}

#[test]
fn prepared_sampling_empty_singleton_and_small_exact_boundaries() {
    for values in [
        population([]),
        population([1.0]),
        population([1.0, 1.0, 4.0]),
    ] {
        for threshold in [-1.0, 0.0, 1.0, 2.0, 4.0, f64::INFINITY, f64::NAN] {
            assert_eq!(
                exact_search(&values, threshold),
                values.partition_point(|entry| entry.cumulative_weight < threshold)
            );
        }
    }
}

#[test]
fn prepared_sampling_uniform_and_periodic_reuse_every_boundary() {
    for (values, radius) in [
        (uniform(), 1),
        (weighted((0..4096).map(|i| f64::from(1 + i % 16))), 5),
    ] {
        let (memory, preparation) = owner(values.len());
        for call in 0..3 {
            let observed = sample_observation::Session::start();
            let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
            let model = preparation
                .prepare(&values, &memory, || Ok(()))
                .unwrap()
                .unwrap();
            assert_eq!(model.radius, radius);
            boundaries(model, &values);
            guard.complete();
            let counts = observed.finish();
            assert_eq!(counts.calibrations, u64::from(call == 0));
            assert_eq!(counts.model_reuses, u64::from(call > 0));
            assert_eq!(counts.maximum_model_credit, MODEL_BYTES);
            assert_eq!(memory.snapshot().reserved_bytes, OWNER_BYTES + MODEL_BYTES);
        }
        drop(preparation);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn prepared_sampling_admitted_rounded_duplicates_keep_first_equal_on_reuse() {
    let values = weighted((0..4096).map(|i| if i % 2 == 0 { 1.0 } else { f64::from_bits(1) }));
    assert_eq!(
        values[0].cumulative_weight.to_bits(),
        values[1].cumulative_weight.to_bits()
    );
    let (memory, preparation) = owner(values.len());
    for _ in 0..2 {
        let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
        let model = preparation
            .prepare(&values, &memory, || Ok(()))
            .unwrap()
            .unwrap();
        boundaries(model, &values);
        for index in (0..values.len()).step_by(2) {
            assert_eq!(
                model.search(&values, values[index].cumulative_weight),
                index
            );
        }
        guard.complete();
    }
    drop(preparation);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_tiny_and_maximum_finite_totals_and_underflow() {
    let tiny = weighted(std::iter::repeat_n(f64::from_bits(1), 4096));
    let large = population((1..=4096).map(|i| f64::from(i) / 4096.0 * f64::MAX));
    for values in [tiny, large] {
        let (memory, preparation) = owner(values.len());
        for _ in 0..2 {
            let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
            let model = preparation
                .prepare(&values, &memory, || Ok(()))
                .unwrap()
                .unwrap();
            boundaries(model, &values);
            assert_eq!(model.search(&values, 0.0), 0);
            guard.complete();
        }
        drop(preparation);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_refusal_is_scalar_and_reused_without_calibration() {
    let shifted = weighted((0..8192).map(|i| if i < 4096 { 1.0 } else { 1024.0 }));
    let tail = weighted((0..8192).map(|i| if i == 8191 { 2_f64.powi(40) } else { 1.0 }));
    for values in [shifted, tail] {
        let (memory, preparation) = owner(values.len());
        for call in 0..3 {
            let observed = sample_observation::Session::start();
            let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
            assert!(
                preparation
                    .prepare(&values, &memory, || Ok(()))
                    .unwrap()
                    .is_none()
            );
            guard.complete();
            let counts = observed.finish();
            assert_eq!(counts.calibrations, u64::from(call == 0));
            assert_eq!(counts.bound_rejections, u64::from(call == 0));
            assert_eq!(counts.refusal_reuses, u64::from(call > 0));
            assert_eq!(counts.model_reuses, 0);
            assert_eq!(memory.snapshot().reserved_bytes, OWNER_BYTES + MODEL_BYTES);
        }
        drop(preparation);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_pressure_reclaims_only_model_then_recalibrates() {
    let values = uniform();
    let (memory, preparation) = owner(values.len());
    let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
    preparation
        .prepare(&values, &memory, || Ok(()))
        .unwrap()
        .unwrap();
    guard.complete();
    let pressure = memory.reserve(available(&memory) - HEADROOM + 1).unwrap();
    let before = memory.snapshot().reserved_bytes;
    let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, before - MODEL_BYTES);
    assert!(
        preparation
            .prepare(&values, &memory, || panic!("pressure bypass"))
            .unwrap()
            .is_none()
    );
    assert!(preparation.retained.borrow().is_none());
    guard.complete();
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, OWNER_BYTES);
    let observed = sample_observation::Session::start();
    let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
    preparation
        .prepare(&values, &memory, || Ok(()))
        .unwrap()
        .unwrap();
    guard.complete();
    assert_eq!(observed.finish().calibrations, 1);
    drop(preparation);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn prepared_sampling_optional_owner_and_model_credit_bypass() {
    let small = LiveMemoryPool::new(HEADROOM).unwrap();
    assert!(Preparation::new(4096, &small).unwrap().is_none());
    assert_eq!(small.snapshot().reserved_bytes, 0);
    let values = uniform();
    let (memory, preparation) = owner(values.len());
    let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
    let pressure = memory
        .reserve(available(&memory) - HEADROOM - MODEL_BYTES + 1)
        .unwrap();
    assert!(
        preparation
            .prepare(&values, &memory, || Ok(()))
            .unwrap()
            .is_none()
    );
    assert!(preparation.retained.borrow().is_none());
    guard.complete();
    drop((pressure, preparation));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn prepared_sampling_cancelled_calibration_and_unwind_clear_every_model() {
    let values = uniform();
    for cancel_at in [1, 2, 3] {
        let (memory, preparation) = owner(values.len());
        let cancellation = shardloom_exec::compute_pool::CancellationToken::default();
        let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
        let mut polls = 0;
        let result = preparation.prepare(&values, &memory, || {
            polls += 1;
            if polls == cancel_at {
                cancellation.cancel();
            }
            cancellation.check()
        });
        assert!(result.is_err());
        assert_eq!(polls, cancel_at);
        drop(guard);
        assert!(preparation.retained.borrow().is_none());
        assert_eq!(memory.snapshot().reserved_bytes, OWNER_BYTES);
        drop(preparation);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let (memory, preparation) = owner(values.len());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
            preparation
                .prepare(&values, &memory, || panic!("calibration unwind"))
                .unwrap();
        }))
        .is_err()
    );
    assert!(preparation.retained.borrow().is_none());
    assert_eq!(memory.snapshot().reserved_bytes, OWNER_BYTES);
    drop(preparation);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_defensive_mismatch_clears_and_does_not_recalibrate() {
    for values in [
        population((1..=4095).map(f64::from)),
        weighted(std::iter::repeat_n(2.0, 4096)),
    ] {
        let (memory, preparation) = owner(4096);
        let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
        preparation
            .prepare(&uniform(), &memory, || Ok(()))
            .unwrap()
            .unwrap();
        guard.complete();
        let observed = sample_observation::Session::start();
        let guard = CallGuard::begin(Some(&preparation), &memory).unwrap();
        assert!(preparation.prepare(&values, &memory, || Ok(())).is_err());
        assert!(preparation.retained.borrow().is_none());
        assert_eq!(observed.finish().calibrations, 0);
        drop(guard);
        drop(preparation);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_failed_completion_and_recursive_attempt_clear_provisional_state() {
    let values = uniform();
    let (memory, preparation) = owner(values.len());
    let outer = FailureGuard::new(Some(&preparation));
    let call = CallGuard::begin(Some(&preparation), &memory).unwrap();
    preparation
        .prepare(&values, &memory, || Ok(()))
        .unwrap()
        .unwrap();
    call.complete();
    assert!(preparation.retained.borrow().is_some());
    drop(outer);
    assert!(preparation.retained.borrow().is_none());
    let call = CallGuard::begin(Some(&preparation), &memory).unwrap();
    preparation
        .prepare(&values, &memory, || Ok(()))
        .unwrap()
        .unwrap();
    let error = CallGuard::begin(Some(&preparation), &memory).err().unwrap();
    assert!(error.to_string().contains("cannot execute recursively"));
    assert!(preparation.retained.borrow().is_none());
    assert!(
        preparation
            .prepare(&values, &memory, || panic!("recursive bypass"))
            .unwrap()
            .is_none()
    );
    call.complete();
    drop(preparation);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
