//! Exact DISTINCT reductions from native value-interval unions and boundary events.

use super::{
    Layout, NativeExecutionContext, Report, Result, State, StoredOrder, add,
    bounds::{Bounds, Inversion},
    failed, frame,
    lookup::{Lookup, Partition},
    records::Values,
};
use crate::local_primitives::{
    native_capacity::ReservedVec,
    native_decimal_reduce, native_float_total, native_payload,
    native_relational_aggregate::{Function as Aggregate, number},
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
    memory::HostMemorySessionExt as _,
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
    outputs: Range<usize>,
    spill: &State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    output: &mut Values<'_, '_>,
    report: &mut Report,
) -> Result<()> {
    let frame_spec = spec;
    let additive = spec.function != frame::Function::Aggregate(Aggregate::CountDistinct);
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
    let mut inverse = Inversion::new(bounds, outputs.clone(), context)?;
    let mut buffer = ReservedVec::new(context.memory())?;
    buffer.reserve(limit)?;
    for position in 0..input.range.len() {
        if position.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let spans = inverse.spans(position, context)?;
        if spans.iter().all(Range::is_empty)
            || !frame::admitted(position, frame_spec, input, context)?
            || input.key_is_null(position, key, context)?
        {
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
    let mut event_fields = vec![
        (ORDINAL.into(), records::u64_type()),
        (
            DELTA.into(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
    ];
    if additive {
        event_fields.push((VALUE.into(), dtype.clone()));
    }
    let event_spec = records::order(event_fields, vec![ORDINAL.into(), DELTA.into()])?;
    let events = unions(&intervals, &event_spec, spill, context, limit, report)?;
    intervals.finish(context)?;
    let mut event_names = vec![ORDINAL.into(), DELTA.into()];
    if additive {
        event_names.push(VALUE.into());
    }
    counts(
        frame_spec,
        &events,
        &event_names,
        outputs.len(),
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
    values: Option<ReservedVec<Cell>>,
    limit: usize,
}

impl Events<'_, '_> {
    fn push(
        &mut self,
        edge: usize,
        delta: i64,
        value: Option<&Cell>,
        context: &NativeExecutionContext<'_>,
        report: &mut Report,
    ) -> Result<()> {
        if let Some(values) = &mut self.values {
            values.values.push(
                value
                    .ok_or_else(|| failed("distinct numeric event has no value"))?
                    .clone(),
            );
        }
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
        columns.reserve(self.spec.fields.len())?;
        columns.values.push(records::unsigned(rows, context, |row| {
            u64::try_from(self.buffer.values[row].0).map_err(vortex_error)
        })?);
        columns
            .values
            .push(result_batch::build_column_with_float_policy(
                &self.spec.fields[1].1,
                rows,
                &context.native_session().host_allocator(),
                context.float_policy(),
                |row| Ok(Value::Int(self.buffer.values[row].1)),
            )?);
        if let Some(values) = &self.values {
            columns
                .values
                .push(result_batch::build_column_with_float_policy(
                    &self.spec.fields[2].1,
                    rows,
                    &context.native_session().host_allocator(),
                    context.float_policy(),
                    |row| numeric_value(&values.values[row], &self.spec.fields[2].1),
                )?);
        }
        self.order.build(
            records::structure(&self.spec.fields, columns, rows)?,
            context,
        )?;
        self.buffer.values.clear();
        if let Some(values) = &mut self.values {
            values.values.clear();
        }
        Ok(())
    }
    fn interval(
        &mut self,
        range: Range<usize>,
        value: Option<&Cell>,
        context: &NativeExecutionContext<'_>,
        report: &mut Report,
    ) -> Result<()> {
        self.push(range.start, 1, value, context, report)?;
        self.push(range.end, -1, value, context, report)
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
    let values = if spec.fields.len() > 2 {
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(limit)?;
        Some(values)
    } else {
        None
    };
    let mut events = Events {
        spec,
        order: Ordering::new(spec, spill, limit, context)?,
        buffer,
        values,
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
            let value = if events.values.is_some() {
                Some(input.raw(*key, 0, context)?)
            } else {
                None
            };
            events.interval(merged.clone(), value.as_ref(), context, report)?;
        }
        current = Some((row, range));
    }
    if let Some((key, range)) = current {
        let value = if events.values.is_some() {
            Some(input.raw(key, 0, context)?)
        } else {
            None
        };
        events.interval(range, value.as_ref(), context, report)?;
    }
    add(&mut report.lookup_blocks, input.blocks)?;
    events.flush(context)?;
    events.order.retain(context)
}

fn counts(
    spec: &frame::Spec,
    events: &StoredOrder<'_>,
    names: &[String],
    rows: usize,
    output: &mut Values<'_, '_>,
    context: &NativeExecutionContext<'_>,
    report: &mut Report,
) -> Result<()> {
    let frame::Function::Aggregate(function) = spec.function else {
        return Err(failed("distinct event reducer has no aggregate function"));
    };
    let mut floats = native_float_total::Total::default();
    let mut decimals = native_decimal_reduce::Total::default();
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
            let add = match events.raw(next, 1, context)? {
                Cell::NegativeInteger(-1) => false,
                Cell::NonnegativeInteger(1) => true,
                _ => return Err(failed("window distinct event has an invalid direction")),
            };
            count = if add {
                count.checked_add(1)
            } else {
                count.checked_sub(1)
            }
            .ok_or_else(|| failed("window distinct count overflow or underflow"))?;
            if function != Aggregate::CountDistinct {
                let value = events.raw(next, 2, context)?;
                if let Some(dtype) = spec.decimal {
                    let Cell::Decimal(value, actual) = value else {
                        return Err(failed("distinct window event changed decimal type"));
                    };
                    if actual != dtype {
                        return Err(failed("distinct window event changed decimal metadata"));
                    }
                    if add {
                        decimals.add(value, dtype)?;
                    } else {
                        decimals.remove(value, dtype)?;
                    }
                } else {
                    let value = number(&value)?;
                    if add {
                        floats.add(value, context.float_policy())?;
                    } else {
                        floats.remove(value)?;
                    }
                }
            }
            next += 1;
        }
        if row < rows {
            let value = if function == Aggregate::CountDistinct {
                frame::Value::Count(count)
            } else if let Some(dtype) = spec.decimal {
                frame::Value::Decimal(decimals.finish_with(
                    dtype,
                    function.is_average(),
                    spec.decimal_output,
                )?)
            } else {
                frame::Value::Float(floats.finish(function.is_average())?)
            };
            output.push(value, context)?;
        }
    }
    if next != length || count != 0 {
        return Err(failed(
            "window distinct events did not balance at partition end",
        ));
    }
    add(&mut report.lookup_blocks, events.blocks)
}

fn numeric_value(value: &Cell, dtype: &DType) -> Result<Value<'static>> {
    Ok(match value {
        Cell::NegativeInteger(value) => Value::Int(*value),
        Cell::NonnegativeInteger(value) if matches!(dtype, DType::Primitive(ptype, _) if ptype.is_signed_int()) => {
            Value::Int(i64::try_from(*value).map_err(vortex_error)?)
        }
        Cell::NonnegativeInteger(value) => Value::UInt(*value),
        Cell::Float(value) => Value::Float(f64::from_bits(*value)),
        Cell::Decimal(value, dtype) => Value::Decimal(*value, *dtype),
        _ => return Err(failed("distinct numeric event contains a nonnumeric value")),
    })
}
