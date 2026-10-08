//! Bounded native binary summaries retain exact first-tied extrema positions.

use super::{
    NativeExecutionContext, Report, Result, State, StoredOrder, add,
    bounds::{self, Bounds, Inversion},
    failed, frame,
    lookup::{Lookup, Partition},
    records::{ResultLayout, VALUE, Values},
};
use crate::local_primitives::{
    SimpleAggregateFunction as Aggregate, native_capacity::ReservedVec,
    native_relational_records::ORDINAL, native_relational_window_frame::Input as _,
};
use std::{cmp::Ordering, ops::Range};

#[allow(clippy::too_many_arguments)] // One exact extremum shares its partition, coverage, store and result sink.
pub(super) fn run(
    spec: &frame::Spec,
    input: &mut Partition<'_, '_>,
    bounds: &Bounds<'_>,
    spill: &State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    output: &mut Values<'_, '_>,
    report: &mut Report,
) -> Result<()> {
    let key = spec
        .key
        .ok_or_else(|| failed("window extremum has no measure key"))?;
    let maximum = spec.function == frame::Function::Aggregate(Aggregate::Max);
    let _metadata = context.memory().reserve(8192)?;
    let layout = ResultLayout::positions()?;
    let names = [ORDINAL.into(), VALUE.into()];
    let levels = summaries(
        input, bounds, key, maximum, &layout, &names, spill, context, limit, report,
    )?;
    let mut lookup = ReservedVec::new(context.memory())?;
    lookup.reserve(levels.values.len())?;
    for level in &levels.values {
        lookup
            .values
            .push(Lookup::<2>::new(level, &names, context)?);
    }
    let mut frames = Lookup::<2>::new(&bounds.store, &bounds.names, context)?;
    for row in 0..input.range.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let mut value = None;
        for range in bounds::ranges(&mut frames, row, context)? {
            let candidate = query(range, &mut lookup.values, input, key, maximum, context)?;
            value = choose(value, candidate, input, key, maximum, context)?;
        }
        output.push(frame::Value::Source(value), context)?;
        #[cfg(test)]
        if (row + 1).is_multiple_of(limit) {
            super::progress(super::Progress::ExtremaResults, row + 1);
        }
    }
    add(&mut report.lookup_blocks, frames.blocks)?;
    for level in &lookup.values {
        add(&mut report.lookup_blocks, level.blocks)?;
    }
    drop((frames, lookup));
    for level in levels.values {
        level.finish(context)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Bottom-up summaries use the same exact comparator and native position record at every level.
fn summaries<'s>(
    input: &mut Partition<'_, '_>,
    bounds: &Bounds<'_>,
    key: usize,
    maximum: bool,
    layout: &ResultLayout,
    names: &[String],
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    report: &mut Report,
) -> Result<ReservedVec<StoredOrder<'s>>> {
    let rows = input.range.len();
    let mut levels = ReservedVec::new(context.memory())?;
    if rows < 2 {
        return Ok(levels);
    }
    levels.reserve(rows.ilog2() as usize + 1)?;
    let mut inverse = Inversion::new(bounds, context)?;
    let mut output = Values::new(layout, spill, limit, context)?;
    let mut selected = None;
    for row in 0..rows {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let spans = inverse.spans(row, context)?;
        let candidate = if spans.iter().any(|range| !range.is_empty())
            && !input.key_is_null(row, key, context)?
        {
            Some(row)
        } else {
            None
        };
        selected = choose(selected, candidate, input, key, maximum, context)?;
        if row % 2 == 1 || row + 1 == rows {
            output.push(frame::Value::Source(selected), context)?;
            selected = None;
            #[cfg(test)]
            if (row + 1).is_multiple_of(2 * limit) {
                super::progress(super::Progress::ExtremaSummaries, row.div_ceil(2));
            }
        }
    }
    inverse.record(report)?;
    drop(inverse);
    let mut length = rows.div_ceil(2);
    levels.values.push(output.finish(length, context)?);
    add(&mut report.extrema_summary_rows, length as u64)?;
    while length > 1 {
        context.check_cancelled()?;
        let prior = levels
            .values
            .last()
            .ok_or_else(|| failed("window extremum level is absent"))?;
        let mut input_level = Lookup::<2>::new(prior, names, context)?;
        let mut output = Values::new(layout, spill, limit, context)?;
        for left in (0..length).step_by(2) {
            let first = node(&mut input_level, left, context)?;
            let second = if left + 1 < length {
                node(&mut input_level, left + 1, context)?
            } else {
                None
            };
            output.push(
                frame::Value::Source(choose(first, second, input, key, maximum, context)?),
                context,
            )?;
        }
        add(&mut report.lookup_blocks, input_level.blocks)?;
        drop(input_level);
        length = length.div_ceil(2);
        levels.values.push(output.finish(length, context)?);
        add(&mut report.extrema_summary_rows, length as u64)?;
    }
    Ok(levels)
}

fn node(
    level: &mut Lookup<'_, '_>,
    position: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    if level.unsigned(position, 0, context)? != position {
        return Err(failed("window extremum summary ordinal changed"));
    }
    level.nullable_unsigned(position, 1, context)
}

fn choose(
    left: Option<usize>,
    right: Option<usize>,
    input: &mut Partition<'_, '_>,
    key: usize,
    maximum: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(left.or(right));
    };
    let order = input.compare_key(left, right, key, context)?;
    Ok(Some(if order.is_eq() {
        left.min(right)
    } else if order
        == if maximum {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    {
        left
    } else {
        right
    }))
}

fn query(
    range: Range<usize>,
    levels: &mut [Lookup<'_, '_>],
    input: &mut Partition<'_, '_>,
    key: usize,
    maximum: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    if range.start > range.end || range.end > input.range.len() {
        return Err(failed("window extremum range exceeds its partition"));
    }
    let mut position = range.start;
    let mut value = None;
    while position < range.end {
        let level = (range.end - position)
            .ilog2()
            .min(position.trailing_zeros()) as usize;
        let candidate = if level == 0 {
            (!input.key_is_null(position, key, context)?).then_some(position)
        } else {
            let summary = levels
                .get_mut(level - 1)
                .ok_or_else(|| failed("window extremum summary level is absent"))?;
            node(summary, position >> level, context)?
        };
        value = choose(value, candidate, input, key, maximum, context)?;
        position = position
            .checked_add(1usize << level)
            .ok_or_else(|| failed("window extremum range position overflow"))?;
    }
    Ok(value)
}
