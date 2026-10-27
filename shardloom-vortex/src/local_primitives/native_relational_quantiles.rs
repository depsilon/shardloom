//! Exact quantile state shares one ordered domain between levels of an input.
//! Resident domains own charged balanced indexes. Ordered spill keeps counts
//! and selected endpoints only, independent of the number of input rows.

use super::{Function, GroupOrdinals, Measure, Spec, failed};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec,
        native_order_statistic::{Arena, Root},
        native_quantile::{self, Position},
        native_relational_keys::{Cell, KeyColumn},
        result_batch::Value,
        vortex_error,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::ops::Range;

#[derive(Clone, Copy)]
struct Slot {
    owner: usize,
    index: usize,
}

#[derive(Default)]
struct Endpoints {
    lower: Option<Cell>,
    upper: Option<Cell>,
}

enum Mode {
    Resident {
        arena: Arena,
        roots: ReservedVec<Root>,
        ordinal: usize,
    },
    Ordered {
        counts: ReservedVec<u64>,
        seen: ReservedVec<u64>,
        endpoints: ReservedVec<Endpoints>,
    },
}

pub(super) struct Table {
    slots: ReservedVec<Option<Slot>>,
    width: usize,
    mode: Mode,
}

impl Table {
    pub(super) fn new(spec: &Spec, memory: &LiveMemoryPool, ordered: bool) -> Result<Option<Self>> {
        if !spec
            .measures
            .iter()
            .any(|measure| measure.function.is_quantile())
        {
            return Ok(None);
        }
        let mut slots = ReservedVec::<Option<Slot>>::new(memory)?;
        slots.reserve(spec.measures.len())?;
        let mut width = 0;
        for (owner, measure) in spec.measures.iter().enumerate() {
            let slot = if measure.function.is_quantile() {
                let existing = slots
                    .values
                    .iter()
                    .flatten()
                    .find(|slot| {
                        let prior = &spec.measures[slot.owner];
                        prior.column == measure.column
                            && prior.filter_column == measure.filter_column
                    })
                    .copied();
                Some(existing.unwrap_or_else(|| {
                    let index = width;
                    width += 1;
                    Slot { owner, index }
                }))
            } else {
                None
            };
            slots.values.push(slot);
        }
        let mode = if ordered {
            let mut endpoints = ReservedVec::new(memory)?;
            endpoints.reserve(spec.measures.len())?;
            endpoints
                .values
                .resize_with(spec.measures.len(), Endpoints::default);
            Mode::Ordered {
                counts: ReservedVec::new(memory)?,
                seen: ReservedVec::new(memory)?,
                endpoints,
            }
        } else {
            Mode::Resident {
                arena: Arena::new(memory)?,
                roots: ReservedVec::new(memory)?,
                ordinal: 0,
            }
        };
        let mut table = Self { slots, width, mode };
        if spec.groups.is_empty() {
            table.reserve_groups(1)?;
        }
        Ok(Some(table))
    }

    pub(super) fn needs_input(&self, measure: usize) -> bool {
        self.slots.values[measure].is_some_and(|slot| slot.owner == measure)
    }

    pub(super) fn reserve_groups(&mut self, groups: usize) -> Result<()> {
        match &mut self.mode {
            Mode::Resident { roots, .. } => {
                let count = groups
                    .checked_mul(self.width)
                    .ok_or_else(|| failed("quantile group count overflow"))?;
                roots.reserve(count - roots.values.len())?;
                roots.values.resize(count, None);
            }
            Mode::Ordered { counts, seen, .. } => {
                if groups != 1 {
                    return Err(failed("ordered quantile state requires one current group"));
                }
                for values in [counts, seen] {
                    values.reserve(self.width - values.values.len())?;
                    values.values.resize(self.width, 0);
                }
            }
        }
        Ok(())
    }

    pub(super) fn consume(
        &mut self,
        measure: usize,
        ordinals: &GroupOrdinals,
        values: &KeyColumn,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let slot =
            self.slots.values[measure].ok_or_else(|| failed("quantile state slot is absent"))?;
        if slot.owner != measure {
            return Ok(());
        }
        for (row, group) in ordinals.iter() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if values.is_null(row)? {
                continue;
            }
            let value = values.raw_cell(row)?;
            match &mut self.mode {
                Mode::Resident {
                    arena,
                    roots,
                    ordinal,
                } => {
                    let index = group * self.width + slot.index;
                    let next = ordinal
                        .checked_add(1)
                        .ok_or_else(|| failed("quantile observation ordinal overflow"))?;
                    roots.values[index] = arena.insert(
                        roots.values[index],
                        value,
                        *ordinal,
                        context.cancellation(),
                    )?;
                    *ordinal = next;
                }
                Mode::Ordered { counts, .. } => {
                    native_quantile::validate(&value)?;
                    counts.values[slot.index] = counts.values[slot.index]
                        .checked_add(1)
                        .ok_or_else(|| failed("quantile observation count overflow"))?;
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn consume_ordered(
        &mut self,
        spec: &Spec,
        measures: &[usize],
        values: &KeyColumn,
        rows: Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let Mode::Ordered {
            counts,
            seen,
            endpoints,
        } = &mut self.mode
        else {
            return Err(failed("ordered quantile values reached a resident index"));
        };
        let mut input = None;
        for &index in measures {
            let Some(slot) = self.slots.values[index] else {
                continue;
            };
            if input.is_some_and(|input| input != slot.index) {
                return Err(failed("ordered quantiles disagree on their shared input"));
            }
            input = Some(slot.index);
            let first = seen.values[slot.index];
            let last = first
                .checked_add(u64::try_from(rows.len()).map_err(vortex_error)?)
                .ok_or_else(|| failed("ordered quantile count overflow"))?;
            let count = counts.values[slot.index];
            if last > count {
                return Err(failed(
                    "ordered quantile values exceed their counted base rows",
                ));
            }
            let measure = &spec.measures[index];
            let position = Position::new(
                measure
                    .quantile_level
                    .ok_or_else(|| failed("quantile level is absent"))?,
                count,
                measure.function == Function::QuantileDisc,
            )?;
            let endpoints = &mut endpoints.values[index];
            for (rank, target) in [
                (position.lower, &mut endpoints.lower),
                (position.upper, &mut endpoints.upper),
            ] {
                context.check_cancelled()?;
                if first <= rank && rank < last {
                    let row = rows.start + usize::try_from(rank - first).map_err(vortex_error)?;
                    let value = values.raw_cell(row)?;
                    native_quantile::validate(&value)?;
                    *target = Some(value);
                }
            }
        }
        if let Some(input) = input {
            seen.values[input] = seen.values[input]
                .checked_add(u64::try_from(rows.len()).map_err(vortex_error)?)
                .ok_or_else(|| failed("ordered quantile count overflow"))?;
        }
        Ok(())
    }

    pub(super) fn finish(
        &self,
        index: usize,
        group: usize,
        measure: &Measure,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Value<'static>> {
        let slot =
            self.slots.values[index].ok_or_else(|| failed("quantile state slot is absent"))?;
        let count = match &self.mode {
            Mode::Resident { arena, roots, .. } => {
                arena.count(roots.values[group * self.width + slot.index])
            }
            Mode::Ordered { counts, seen, .. } => {
                if counts.values[slot.index] != seen.values[slot.index] {
                    return Err(failed(
                        "ordered quantile values do not cover all counted base rows",
                    ));
                }
                counts.values[slot.index]
            }
        };
        if count == 0 {
            return Ok(Value::Null);
        }
        let discrete = measure.function == Function::QuantileDisc;
        let position = Position::new(
            measure
                .quantile_level
                .ok_or_else(|| failed("quantile level is absent"))?,
            count,
            discrete,
        )?;
        let (lower, upper) = match &self.mode {
            Mode::Resident { arena, roots, .. } => {
                let root = roots.values[group * self.width + slot.index];
                (
                    arena
                        .select(root, position.lower, context.cancellation())?
                        .0,
                    arena
                        .select(root, position.upper, context.cancellation())?
                        .0,
                )
            }
            Mode::Ordered { endpoints, .. } => {
                let endpoints = &endpoints.values[index];
                (
                    endpoints
                        .lower
                        .as_ref()
                        .ok_or_else(|| failed("ordered quantile lower endpoint is absent"))?,
                    endpoints
                        .upper
                        .as_ref()
                        .ok_or_else(|| failed("ordered quantile upper endpoint is absent"))?,
                )
            }
        };
        if discrete {
            native_quantile::numeric_value(lower, &measure.dtype)
        } else {
            position.interpolate(lower, upper, &measure.dtype)
        }
    }
}
