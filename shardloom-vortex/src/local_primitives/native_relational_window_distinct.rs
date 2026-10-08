//! Exact DISTINCT counts from native value-interval unions and boundary events.

use super::{
    Layout, NativeExecutionContext, Report, Result, State, StoredOrder, add,
    bounds::{Bounds, Inversion},
    failed, frame,
    lookup::{Lookup, Partition},
    records::Values,
};
use crate::local_primitives::{
    native_capacity::ReservedVec,
    native_payload,
    native_relational_keys::Cell,
    native_relational_records::{self as records, ORDINAL},
    native_relational_sort,
    native_relational_spill::Ordering,
    native_relational_window_frame::Input as _,
    result_batch::{self, Value},
    vortex_error,
};
use std::ops::Range;
use vortex::array::{
    dtype::{DType, Nullability, PType},
    memory::MemorySessionExt as _,
};

const VALUE: &str = "value";
const END: &str = "end";
const DELTA: &str = "delta";

#[allow(clippy::too_many_arguments)] // One distinct function shares partition, frame membership, store and result sink.
pub(super) fn run(
    spec: &frame::Spec,
    layout: &Layout,
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
        .ok_or_else(|| failed("window DISTINCT has no key"))?;
    let dtype = &layout.group_fields[key].1;
    let metadata = native_payload::metadata_bytes(dtype)?
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| failed("window distinct schema capacity overflow"))?;
    let _metadata = context.memory().reserve(metadata)?;
    let spec = records::order(
        vec![
            (ORDINAL.into(), records::u64_type()),
            (END.into(), records::u64_type()),
            (VALUE.into(), dtype.clone()),
        ],
        vec![VALUE.into(), ORDINAL.into()],
    )?;
    let mut intervals = Ordering::new(&spec, spill, limit, context)?;
    let mut inverse = Inversion::new(bounds, context)?;
    let mut buffer = ReservedVec::new(context.memory())?;
    buffer.reserve(limit)?;
    for position in 0..input.range.len() {
        if position.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let spans = inverse.spans(position, context)?;
        if spans.iter().all(Range::is_empty) || input.key_is_null(position, key, context)? {
            continue;
        }
        for span in spans.into_iter().filter(|span| !span.is_empty()) {
            buffer.values.push((position, span));
            add(&mut report.distinct_intervals, 1)?;
            if buffer.values.len() == limit {
                flush_intervals(
                    &mut buffer,
                    input,
                    &layout.group_names[key],
                    &spec,
                    &mut intervals,
                    context,
                )?;
                #[cfg(test)]
                super::progress(super::Progress::DistinctIntervals, position + 1);
            }
        }
    }
    inverse.record(report)?;
    drop(inverse);
    flush_intervals(
        &mut buffer,
        input,
        &layout.group_names[key],
        &spec,
        &mut intervals,
        context,
    )?;
    drop(buffer);
    let intervals = intervals.retain(context)?;
    let event_spec = records::order(
        vec![
            (ORDINAL.into(), records::u64_type()),
            (
                DELTA.into(),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            ),
        ],
        vec![ORDINAL.into(), DELTA.into()],
    )?;
    let events = unions(&intervals, &event_spec, spill, context, limit, report)?;
    intervals.finish(context)?;
    counts(
        &events,
        &event_spec.names,
        input.range.len(),
        output,
        context,
        report,
    )?;
    events.finish(context)
}

fn flush_intervals(
    buffer: &mut ReservedVec<(usize, Range<usize>)>,
    input: &mut Partition<'_, '_>,
    name: &str,
    spec: &native_relational_sort::Spec,
    output: &mut Ordering<'_, '_>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if buffer.values.is_empty() {
        return Ok(());
    }
    let rows = buffer.values.len();
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(3)?;
    columns.values.push(records::unsigned(rows, context, |row| {
        u64::try_from(buffer.values[row].1.start).map_err(vortex_error)
    })?);
    columns.values.push(records::unsigned(rows, context, |row| {
        u64::try_from(buffer.values[row].1.end).map_err(vortex_error)
    })?);
    let mut positions = ReservedVec::new(context.memory())?;
    positions.reserve(rows)?;
    for (position, _) in &buffer.values {
        positions.values.push(Some(input.range.start + position));
    }
    columns.values.push(input.lookup.gather(
        &positions.values,
        None,
        name,
        &spec.fields[2].1,
        context,
    )?);
    output.build(records::structure(&spec.fields, columns, rows)?, context)?;
    buffer.values.clear();
    Ok(())
}

struct Events<'p, 's> {
    spec: &'p native_relational_sort::Spec,
    order: Ordering<'p, 's>,
    buffer: ReservedVec<(usize, i64)>,
    limit: usize,
}

impl Events<'_, '_> {
    fn push(
        &mut self,
        edge: usize,
        delta: i64,
        context: &NativeExecutionContext<'_>,
        report: &mut Report,
    ) -> Result<()> {
        self.buffer.values.push((edge, delta));
        add(&mut report.distinct_events, 1)?;
        if self.buffer.values.len() == self.limit {
            self.flush(context)?;
            #[cfg(test)]
            super::progress(
                super::Progress::DistinctEvents,
                usize::try_from(report.distinct_events).map_err(vortex_error)?,
            );
        }
        Ok(())
    }
    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.buffer.values.is_empty() {
            return Ok(());
        }
        let rows = self.buffer.values.len();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(2)?;
        columns.values.push(records::unsigned(rows, context, |row| {
            u64::try_from(self.buffer.values[row].0).map_err(vortex_error)
        })?);
        columns.values.push(result_batch::build_column(
            &self.spec.fields[1].1,
            rows,
            &context.native_session().allocator(),
            |row| Ok(Value::Int(self.buffer.values[row].1)),
        )?);
        self.order.build(
            records::structure(&self.spec.fields, columns, rows)?,
            context,
        )?;
        self.buffer.values.clear();
        Ok(())
    }
    fn interval(
        &mut self,
        range: Range<usize>,
        context: &NativeExecutionContext<'_>,
        report: &mut Report,
    ) -> Result<()> {
        self.push(range.start, 1, context, report)?;
        self.push(range.end, -1, context, report)
    }
}

fn unions<'s>(
    intervals: &StoredOrder<'_>,
    spec: &native_relational_sort::Spec,
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    report: &mut Report,
) -> Result<StoredOrder<'s>> {
    let mut buffer = ReservedVec::new(context.memory())?;
    buffer.reserve(limit)?;
    let mut events = Events {
        spec,
        order: Ordering::new(spec, spill, limit, context)?,
        buffer,
        limit,
    };
    let names = [VALUE.into(), ORDINAL.into(), END.into()];
    let mut input = Lookup::<2>::new(intervals, &names, context)?;
    let mut current: Option<(usize, Range<usize>)> = None;
    for row in 0..input.rows()? {
        let range = input.unsigned(row, 1, context)?..input.unsigned(row, 2, context)?;
        if range.is_empty() {
            return Err(failed("window distinct union received an empty interval"));
        }
        if let Some((key, merged)) = &mut current {
            let order = input.compare(*key, row, 0, context)?;
            if order.is_gt() {
                return Err(failed("window distinct values are not in native order"));
            }
            if order.is_eq() && range.start <= merged.end {
                if range.start < merged.start {
                    return Err(failed("window distinct interval starts moved backwards"));
                }
                merged.end = merged.end.max(range.end);
                continue;
            }
            events.interval(merged.clone(), context, report)?;
        }
        current = Some((row, range));
    }
    if let Some((_, range)) = current {
        events.interval(range, context, report)?;
    }
    add(&mut report.lookup_blocks, input.blocks)?;
    events.flush(context)?;
    events.order.retain(context)
}

fn counts(
    events: &StoredOrder<'_>,
    names: &[String],
    rows: usize,
    output: &mut Values<'_, '_>,
    context: &NativeExecutionContext<'_>,
    report: &mut Report,
) -> Result<()> {
    let mut events = Lookup::<2>::new(events, names, context)?;
    let length = events.rows()?;
    let mut next = 0;
    let mut count = 0u64;
    for row in 0..=rows {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        while next < length {
            let position = events.unsigned(next, 0, context)?;
            if position < row || position > rows {
                return Err(failed(
                    "window distinct event exceeds ordered output positions",
                ));
            }
            if position > row {
                break;
            }
            count = match events.raw(next, 1, context)? {
                Cell::NegativeInteger(-1) => count.checked_sub(1),
                Cell::NonnegativeInteger(1) => count.checked_add(1),
                _ => return Err(failed("window distinct event has an invalid direction")),
            }
            .ok_or_else(|| failed("window distinct count overflow or underflow"))?;
            next += 1;
        }
        if row < rows {
            output.push(frame::Value::Count(count), context)?;
        }
    }
    if next != length || count != 0 {
        return Err(failed(
            "window distinct events did not balance at partition end",
        ));
    }
    add(&mut report.lookup_blocks, events.blocks)
}
