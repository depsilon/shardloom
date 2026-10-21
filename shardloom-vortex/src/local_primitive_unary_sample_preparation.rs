//! Scalar calibration owned by one immutable, admitted prepared sampling root.
//!
//! Population buffers and rows are never retained here. The relational binder
//! proves source/plan identity; length and total bits are defensive checks only.

use super::{
    Result, failed,
    sample::{Candidate, exact_search},
};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::cell::{Cell, RefCell};

const HEADROOM: u64 = 1 << 20;
pub(super) const POINTER_BYTES: u64 = size_of::<Option<Box<Preparation>>>() as u64;
const OWNER_BYTES: u64 = size_of::<Preparation>() as u64;
const MODEL_BYTES: u64 = size_of::<Stored>() as u64;
const _: () = assert!(POINTER_BYTES + OWNER_BYTES + MODEL_BYTES <= 128);

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Idle,
    Active,
    Bypass,
}

pub(in crate::local_primitives) struct Preparation {
    retained: RefCell<Option<Box<Stored>>>,
    state: Cell<State>,
    rows: usize,
    _metadata: MemoryLease,
}

struct Stored {
    model: Model,
    #[cfg_attr(not(test), allow(dead_code))] // Drop retains and releases the model's credit.
    metadata: MemoryLease,
}

#[derive(Clone, Copy)]
pub(super) struct Model {
    population: usize,
    total: f64,
    // Zero remembers a complete decision to refuse prediction, not a radius.
    radius: usize,
}

fn available(memory: &LiveMemoryPool) -> u64 {
    let snapshot = memory.snapshot();
    snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes)
}

impl Preparation {
    pub(super) fn new(rows: usize, memory: &LiveMemoryPool) -> Result<Option<Box<Self>>> {
        if available(memory) < HEADROOM + OWNER_BYTES {
            return Ok(None);
        }
        let metadata = memory.reserve(OWNER_BYTES)?;
        Ok(Some(Box::new(Self {
            retained: RefCell::new(None),
            state: Cell::new(State::Idle),
            rows,
            _metadata: metadata,
        })))
    }

    fn clear(&self) {
        // Drop the allocation and its own lease together. The owner shell's
        // separate credit remains live until the bound plan is dropped.
        self.retained.borrow_mut().take();
    }

    #[cfg(test)]
    pub(in crate::local_primitives) fn retained_bytes(&self) -> u64 {
        self.retained
            .borrow()
            .as_ref()
            .map_or(0, |entry| entry.metadata.bytes())
    }

    fn pressure(&self, memory: &LiveMemoryPool, additional: u64) -> bool {
        if available(memory) >= HEADROOM + additional {
            return false;
        }
        self.clear();
        self.state.set(State::Bypass);
        #[cfg(test)]
        super::sample_observation::credit_bypass();
        true
    }

    pub(super) fn prepare(
        &self,
        population: &[Candidate],
        memory: &LiveMemoryPool,
        mut check_cancelled: impl FnMut() -> Result<()>,
    ) -> Result<Option<Model>> {
        match self.state.get() {
            State::Idle => return Err(failed("prepared sampling requires an active execution")),
            State::Bypass => return Ok(None),
            State::Active => {}
        }
        if population.len() != self.rows {
            self.clear();
            return Err(failed("immutable prepared sampling population changed"));
        }
        let total = population
            .last()
            .map_or(0.0, |entry| entry.cumulative_weight);
        if !total.is_finite() || total <= 0.0 {
            self.clear();
            return Err(failed("immutable prepared sampling total is invalid"));
        }
        check_cancelled()?;
        #[cfg(test)]
        super::sample_observation::owner_credit(POINTER_BYTES + OWNER_BYTES);
        if self.pressure(memory, 0) {
            return Ok(None);
        }
        let cached = self.retained.borrow().as_ref().map(|entry| entry.model);
        if let Some(model) = cached {
            if model.population != population.len() || model.total.to_bits() != total.to_bits() {
                self.clear();
                return Err(failed(
                    "immutable prepared sampling cumulative weights changed",
                ));
            }
            #[cfg(test)]
            super::sample_observation::reused(
                (model.radius != 0).then_some(model.radius),
                POINTER_BYTES + OWNER_BYTES,
                MODEL_BYTES,
            );
            return Ok((model.radius != 0).then_some(model));
        }
        if self.pressure(memory, MODEL_BYTES) {
            return Ok(None);
        }
        let metadata = memory.reserve(MODEL_BYTES)?;
        let mut model = Model {
            population: population.len(),
            total,
            radius: 0,
        };
        let mut maximum_error = 0;
        let mut visits = 0;
        for (ordinal, entry) in population.iter().enumerate() {
            if ordinal.is_multiple_of(256) {
                #[cfg(test)]
                CALIBRATION_POLL.with_borrow_mut(|slot| {
                    if let Some(poll) = slot {
                        poll(ordinal);
                    }
                });
                check_cancelled()?;
            }
            visits += 1;
            let error = model.predict(entry.cumulative_weight).abs_diff(ordinal);
            if error > 8 {
                break;
            }
            maximum_error = maximum_error.max(error);
            if ordinal + 1 == population.len() {
                model.radius = maximum_error + 1;
            }
        }
        check_cancelled()?;
        #[cfg(test)]
        super::sample_observation::calibrated(
            visits,
            (model.radius != 0).then_some(model.radius),
            MODEL_BYTES,
        );
        #[cfg(not(test))]
        let _ = visits;
        // Publication remains provisional until the outer call guard completes.
        // Recursive entry clears this record before any session-lock attempt.
        *self.retained.borrow_mut() = Some(Box::new(Stored { model, metadata }));
        Ok((model.radius != 0).then_some(model))
    }
}

#[cfg(test)]
type CalibrationPoll = Box<dyn FnMut(usize)>;

#[cfg(test)]
thread_local! {
    pub(in crate::local_primitives) static BEFORE_MODEL_PREPARATION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    pub(in crate::local_primitives) static CALIBRATION_POLL: RefCell<Option<CalibrationPoll>> = const { RefCell::new(None) };
}

impl Model {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn predict(self, threshold: f64) -> usize {
        (((threshold / self.total) * (self.population - 1) as f64) as usize)
            .min(self.population - 1)
    }

    pub(super) fn search(self, population: &[Candidate], threshold: f64) -> usize {
        debug_assert_eq!(population.len(), self.population);
        if !threshold.is_finite() || !(0.0..=self.total).contains(&threshold) {
            return exact_search(population, threshold);
        }
        let prediction = self.predict(threshold);
        let start = prediction.saturating_sub(self.radius);
        let end = prediction
            .saturating_add(self.radius)
            .min(population.len() - 1);
        #[cfg(test)]
        super::sample_observation::model_search();
        start + exact_search(&population[start..=end], threshold)
    }
}

/// This guard begins before native session admission, which is not reentrant.
pub(in crate::local_primitives) struct CallGuard<'a> {
    preparation: Option<&'a Preparation>,
    complete: bool,
}

impl<'a> CallGuard<'a> {
    pub(in crate::local_primitives) fn begin(
        preparation: Option<&'a Preparation>,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        if let Some(preparation) = preparation {
            if preparation.state.get() != State::Idle {
                preparation.clear();
                preparation.state.set(State::Bypass);
                return Err(failed("prepared sampling cannot execute recursively"));
            }
            preparation.state.set(State::Active);
            preparation.pressure(memory, 0);
        }
        Ok(Self {
            preparation,
            complete: false,
        })
    }

    pub(in crate::local_primitives) fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for CallGuard<'_> {
    fn drop(&mut self) {
        if let Some(preparation) = self.preparation {
            if !self.complete {
                preparation.clear();
            }
            preparation.state.set(State::Idle);
        }
    }
}

/// Collection and writer finalization can fail after the native call succeeds.
pub(in crate::local_primitives) struct FailureGuard<'a> {
    preparation: Option<&'a Preparation>,
    complete: bool,
}

impl<'a> FailureGuard<'a> {
    pub(in crate::local_primitives) fn new(preparation: Option<&'a Preparation>) -> Self {
        Self {
            preparation,
            complete: false,
        }
    }

    pub(in crate::local_primitives) fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for FailureGuard<'_> {
    fn drop(&mut self) {
        if !self.complete
            && let Some(preparation) = self.preparation
        {
            preparation.clear();
        }
    }
}

#[cfg(test)]
#[path = "local_primitive_unary_sample_preparation_tests.rs"]
mod tests;
