//! Charged statistical state shared by compatible measures of one input.

use super::{Function, GroupOrdinals, Spec, failed};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec,
        native_relational_keys::KeyColumn,
        native_statistical_moments::{Moments, PairedMoments},
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;

#[derive(Clone, Copy)]
struct Slot {
    owner: usize,
    index: usize,
    paired: bool,
}

pub(super) struct Table {
    slots: ReservedVec<Option<Slot>>,
    width: usize,
    paired_width: usize,
    values: ReservedVec<Moments>,
    pairs: ReservedVec<PairedMoments>,
}

impl Table {
    pub(super) fn new(spec: &Spec, memory: &LiveMemoryPool) -> Result<Option<Self>> {
        if !spec.measures.iter().any(|m| m.function.is_moment()) {
            return Ok(None);
        }
        let mut slots = ReservedVec::<Option<Slot>>::new(memory)?;
        slots.reserve(spec.measures.len())?;
        let mut width = 0;
        let mut paired_width = 0;
        for (index, measure) in spec.measures.iter().enumerate() {
            let slot = if measure.function.is_moment() {
                let existing = slots
                    .values
                    .iter()
                    .flatten()
                    .find(|prior| {
                        let prior = &spec.measures[prior.owner];
                        prior.column == measure.column
                            && prior.secondary_column == measure.secondary_column
                            && prior.filter_column == measure.filter_column
                    })
                    .copied();
                Some(existing.unwrap_or_else(|| {
                    let paired = measure.function.is_pair();
                    let count = if paired {
                        &mut paired_width
                    } else {
                        &mut width
                    };
                    let slot = Slot {
                        owner: index,
                        index: *count,
                        paired,
                    };
                    *count += 1;
                    slot
                }))
            } else {
                None
            };
            slots.values.push(slot);
        }
        let mut table = Self {
            slots,
            width,
            paired_width,
            values: ReservedVec::new(memory)?,
            pairs: ReservedVec::new(memory)?,
        };
        if spec.groups.is_empty() {
            table.reserve_groups(1)?;
        }
        Ok(Some(table))
    }

    pub(super) fn needs_input(&self, measure: usize) -> bool {
        self.slots.values[measure].is_some_and(|slot| slot.owner == measure)
    }

    pub(super) fn reserve_groups(&mut self, groups: usize) -> Result<()> {
        let count = groups
            .checked_mul(self.width)
            .ok_or_else(|| failed("statistical state cardinality overflow"))?;
        self.values.reserve(count - self.values.values.len())?;
        self.values.values.resize(count, Moments::default());
        let count = groups
            .checked_mul(self.paired_width)
            .ok_or_else(|| failed("paired statistical state cardinality overflow"))?;
        self.pairs.reserve(count - self.pairs.values.len())?;
        self.pairs.values.resize(count, PairedMoments::default());
        Ok(())
    }

    pub(super) fn consume(
        &mut self,
        measure: usize,
        ordinals: &GroupOrdinals,
        values: &KeyColumn,
        secondary: Option<&KeyColumn>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let slot = self.slots.values[measure]
            .ok_or_else(|| failed("statistical measure state is absent"))?;
        if slot.owner != measure {
            return Ok(());
        }
        if slot.paired {
            let secondary =
                secondary.ok_or_else(|| failed("paired statistical input is absent"))?;
            if matches!(ordinals, GroupOrdinals::Scalar(_)) {
                let mut partial = PairedMoments::default();
                for row in 0..ordinals.len() {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if values.is_null(row)? || secondary.is_null(row)? {
                        continue;
                    }
                    partial.observe_with_policy(
                        &values.raw_cell(row)?,
                        &secondary.raw_cell(row)?,
                        context.float_policy(),
                    )?;
                }
                self.pairs.values[slot.index].merge(partial)?;
            } else {
                for (row, group) in ordinals.iter() {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if values.is_null(row)? || secondary.is_null(row)? {
                        continue;
                    }
                    self.pairs.values[group * self.paired_width + slot.index].observe_with_policy(
                        &values.raw_cell(row)?,
                        &secondary.raw_cell(row)?,
                        context.float_policy(),
                    )?;
                }
            }
            return Ok(());
        }
        if matches!(ordinals, GroupOrdinals::Scalar(_)) {
            let mut partial = Moments::default();
            for row in 0..ordinals.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                partial.observe_with_policy(&values.raw_cell(row)?, context.float_policy())?;
            }
            self.values.values[slot.index].merge(partial)?;
        } else {
            for (row, group) in ordinals.iter() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                self.values.values[group * self.width + slot.index]
                    .observe_with_policy(&values.raw_cell(row)?, context.float_policy())?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        &self,
        measure: usize,
        group: usize,
        function: Function,
    ) -> Result<Option<f64>> {
        let slot = self.slots.values[measure]
            .ok_or_else(|| failed("statistical measure state is absent"))?;
        if slot.paired {
            self.pairs.values[group * self.paired_width + slot.index]
                .finish(function.sample(), function == Function::Corr)
        } else {
            self.values.values[group * self.width + slot.index]
                .finish(function.sample(), function.standard_deviation())
        }
    }
}
