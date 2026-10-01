//! Seed-compatible bounded top-k sampling and explicitly budgeted replacement population.

use super::{
    BATCH_ROWS, NativeBatch, NativeExecutionContext, PreparedVortexUnary, ReservedVec, Result,
    UnaryOutput, Value, failed, values::OwnedRow,
};
use std::{borrow::Borrow as _, cmp::Ordering};

#[derive(Clone, Copy)]
enum Score {
    Integer(u64),
    Weighted(f64),
}
impl Score {
    fn cmp(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(&b),
            (Self::Weighted(a), Self::Weighted(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        }
    }
}

struct Candidate {
    score: Score,
    slot: usize,
    ordinal: usize,
    cumulative_weight: f64,
    row: Option<OwnedRow>,
}

pub(super) struct Sample {
    candidates: ReservedVec<Candidate>,
    cap: usize,
    seen: usize,
    total_weight: f64,
}

impl Sample {
    pub(super) fn usage(&self) -> super::report::StateUsage {
        super::report::StateUsage {
            items: self.candidates.values.len(),
            all_input_retained: self.seen > 0
                && self
                    .candidates
                    .values
                    .iter()
                    .filter(|entry| entry.row.is_some())
                    .count()
                    == self.seen,
        }
    }
    pub(super) fn new(
        plan: &PreparedVortexUnary,
        context: &NativeExecutionContext<'_>,
        rows: u64,
    ) -> Result<Self> {
        Ok(Self {
            candidates: ReservedVec::new(context.memory())?,
            cap: super::super::sample_target_count(
                &plan.request,
                usize::try_from(rows).map_err(super::vortex_error)?,
            )?,
            seen: 0,
            total_weight: 0.0,
        })
    }

    pub(super) fn consume(
        &mut self,
        plan: &PreparedVortexUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        payload: bool,
    ) -> Result<()> {
        let seed = plan.request.sample_seed.unwrap_or(0);
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            let ordinal = self.seen;
            self.seen = self
                .seen
                .checked_add(1)
                .ok_or_else(|| failed("sample population overflow"))?;
            let weight = if let Some(column) = plan.weight_index {
                super::super::sample_weight_value(batch.stat(column, row)?.borrow())?
            } else {
                1.0
            };
            let score = if plan.weight_index.is_some() {
                Score::Weighted(super::super::deterministic_weighted_sample_score(
                    seed, ordinal, weight,
                ))
            } else {
                Score::Integer(super::super::deterministic_sample_score(seed, ordinal))
            };
            if plan.request.sample_with_replacement {
                self.total_weight += weight;
                if !self.total_weight.is_finite() {
                    return Err(failed("sample total weight must remain finite"));
                }
                self.candidates.reserve_one()?;
                self.candidates.values.push(Candidate {
                    score,
                    slot: ordinal,
                    ordinal,
                    cumulative_weight: self.total_weight,
                    row: payload
                        .then(|| batch.row(&plan.output_indices, row))
                        .transpose()?,
                });
            } else if self.candidates.values.len() < self.cap {
                self.candidates.reserve_one()?;
                let slot = self.candidates.values.len();
                self.candidates.values.push(Candidate {
                    score,
                    slot,
                    ordinal,
                    cumulative_weight: 0.0,
                    row: payload
                        .then(|| batch.row(&plan.output_indices, row))
                        .transpose()?,
                });
                self.sift_up(slot);
            } else if self.cap > 0
                && score.cmp(self.candidates.values[0].score) == Ordering::Greater
            {
                let slot = self.candidates.values[0].slot;
                self.candidates.values[0] = Candidate {
                    score,
                    slot,
                    ordinal,
                    cumulative_weight: 0.0,
                    row: payload
                        .then(|| batch.row(&plan.output_indices, row))
                        .transpose()?,
                };
                self.sift_down();
            }
        }
        Ok(())
    }

    // Original selection replaces the first lowest-scoring slot. Carry that
    // slot through heap swaps so ties preserve the existing exact seed contract.
    fn less(&self, a: usize, b: usize) -> bool {
        let a = &self.candidates.values[a];
        let b = &self.candidates.values[b];
        a.score.cmp(b.score).then(a.slot.cmp(&b.slot)) == Ordering::Less
    }
    fn sift_up(&mut self, mut child: usize) {
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.less(child, parent) {
                break;
            }
            self.candidates.values.swap(child, parent);
            child = parent;
        }
    }
    fn sift_down(&mut self) {
        let mut parent = 0;
        loop {
            let left = parent * 2 + 1;
            if left >= self.candidates.values.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.candidates.values.len() && self.less(right, left) {
                right
            } else {
                left
            };
            if !self.less(child, parent) {
                break;
            }
            self.candidates.values.swap(child, parent);
            parent = child;
        }
    }

    pub(super) fn finish(
        mut self,
        plan: &PreparedVortexUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        let target = super::super::sample_target_count(&plan.request, self.seen)?;
        if !plan.request.sample_with_replacement {
            self.candidates
                .values
                .sort_unstable_by(|a, b| b.score.cmp(a.score).then(a.slot.cmp(&b.slot)));
            self.candidates.values.truncate(target);
            self.candidates
                .values
                .sort_unstable_by_key(|candidate| candidate.ordinal);
        }
        let seed = plan.request.sample_seed.unwrap_or(0);
        for start in (0..target).step_by(BATCH_ROWS) {
            context.check_cancelled()?;
            output.emit((target - start).min(BATCH_ROWS), |row, column| {
                let draw = start + row;
                let index = if !plan.request.sample_with_replacement {
                    draw
                } else if plan.weight_index.is_some() {
                    let threshold =
                        super::super::deterministic_sample_unit(seed ^ 0xa076_1d64_78bd_642f, draw)
                            * self.total_weight;
                    self.candidates
                        .values
                        .partition_point(|candidate| candidate.cumulative_weight < threshold)
                        .min(self.candidates.values.len() - 1)
                } else {
                    super::super::deterministic_sample_replacement_index(
                        seed,
                        draw,
                        self.candidates.values.len(),
                    )
                };
                let row = self.candidates.values[index]
                    .row
                    .as_ref()
                    .ok_or_else(|| failed("sample retained row is absent"))?;
                Ok(Value::from(&row.values()[column]))
            })?;
        }
        Ok(self.seen)
    }
}
