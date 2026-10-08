//! Explicit bounded analytic windows over the query's shared native run store.

use super::{Function, Group, Spec, frame};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec,
        native_relational_batch::{Batch, failed},
        native_relational_records as private,
        native_relational_spill::{Ordering, State, StoredOrder},
        result_batch::{self, Value},
        vortex_error,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, VortexSessionExecute as _, arrays::PrimitiveArray, dtype::DType,
    memory::MemorySessionExt as _,
};

#[path = "native_relational_window_bounds.rs"]
mod bounds;
#[path = "native_relational_window_distinct.rs"]
mod distinct;
#[path = "native_relational_window_extrema.rs"]
mod extrema;
#[path = "native_relational_window_lookup.rs"]
mod lookup;
#[path = "native_relational_window_partition.rs"]
mod partition;
#[path = "native_relational_window_records.rs"]
mod records;

use lookup::Lookup;
use records::{DATA, Kind, Layout};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    Bounds,
    DistinctIntervals,
    DistinctEvents,
    ExtremaSummaries,
    ExtremaResults,
}
#[cfg(test)]
type ProgressHook = Box<dyn FnMut(Progress, usize) -> bool>;
#[cfg(test)]
type CacheHitHook = Box<dyn FnOnce(&std::path::Path)>;
#[cfg(test)]
thread_local! {
    pub(crate) static WINDOW_PROGRESS: std::cell::RefCell<Option<ProgressHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static BEFORE_CACHE_HIT: std::cell::RefCell<Option<CacheHitHook>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn progress(phase: Progress, rows: usize) {
    WINDOW_PROGRESS.with(|slot| {
        let Some(mut hook) = slot.borrow_mut().take() else {
            return;
        };
        if !hook(phase, rows) {
            *slot.borrow_mut() = Some(hook);
        }
    });
}

#[derive(Default)]
pub(in crate::local_primitives) struct Report {
    pub(in crate::local_primitives) input_rows: u64,
    pub(in crate::local_primitives) input_batches_detached: u64,
    pub(in crate::local_primitives) groups: u64,
    pub(in crate::local_primitives) partitions: u64,
    pub(in crate::local_primitives) peer_records: u64,
    pub(in crate::local_primitives) bounds_rows: u64,
    pub(in crate::local_primitives) distinct_intervals: u64,
    pub(in crate::local_primitives) distinct_events: u64,
    pub(in crate::local_primitives) extrema_summary_rows: u64,
    pub(in crate::local_primitives) lookup_blocks: u64,
    pub(in crate::local_primitives) output_rows: u64,
}

fn add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| failed("stored window counter overflow"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One bound window shares its caller's input, grant, run store and consumer.
pub(in crate::local_primitives) fn run(
    spec: &Spec,
    input_fields: &[(String, DType)],
    spill: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<Report> {
    if batch_rows == 0 {
        return Err(failed("window output batch rows must be positive"));
    }
    let limit = batch_rows.min(1024);
    let layout = Layout::new(spec, input_fields, context)?;
    let mut report = Report::default();
    let source = build(spec, &layout, spill, context, limit, input, &mut report)?;
    let mut groups = ReservedVec::new(context.memory())?;
    groups.reserve(spec.groups.len())?;
    for group in &spec.groups {
        context.check_cancelled()?;
        groups.values.push(evaluate_group(
            spec,
            group,
            &layout,
            &source,
            spill,
            context,
            limit,
            &mut report,
        )?);
    }
    output(
        spec,
        &layout,
        &source,
        &groups.values,
        context,
        limit,
        consume,
        &mut report,
    )?;
    for group in groups.values {
        group.finish(context)?;
    }
    source.finish(context)?;
    context.check_cancelled()?;
    Ok(report)
}

#[allow(clippy::too_many_arguments)] // Build once; the producer is completed before any analytic result.
fn build<'s>(
    spec: &Spec,
    layout: &Layout,
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    report: &mut Report,
) -> Result<StoredOrder<'s>> {
    let mut order = Ordering::new(&layout.source, spill, limit, context)?;
    input(&mut |array| {
        context.check_cancelled()?;
        let ordinal = report.input_rows;
        add(&mut report.input_rows, array.len() as u64)?;
        let input = Batch::new(array, &spec.keys, context)?;
        for row in 0..input.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            input.hash_prefix(row, spec.ordering_keys, true)?;
        }
        for start in (0..input.array.len()).step_by(limit) {
            let end = start.saturating_add(limit).min(input.array.len());
            order.build(
                layout.source_record(&input, start..end, ordinal, context)?,
                context,
            )?;
        }
        if !input.array.is_empty() {
            add(&mut report.input_batches_detached, 1)?;
        }
        context.check_cancelled()
    })?;
    order.retain(context)
}

#[allow(clippy::too_many_arguments)] // Group semantics, original store and shared policy are one execution boundary.
fn evaluate_group<'s>(
    spec: &Spec,
    group: &Group,
    layout: &Layout,
    source: &StoredOrder<'s>,
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    report: &mut Report,
) -> Result<StoredOrder<'s>> {
    let order_spec = layout.group_order(group)?;
    let mut order = Ordering::new(&order_spec, spill, limit, context)?;
    let mut start = 0;
    while let Some(block) = source.read_block_at(start, context)? {
        let record = layout.group_record(spec, block.array(), context)?;
        let batch = Batch::new(record.clone(), &layout.group_names, context)?;
        for row in 0..record.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            for key in &group.order {
                if key.nulls.is_none() && batch.key_is_null(row, key.key)? {
                    return Err(failed(
                        "window ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
                    ));
                }
            }
        }
        add(&mut start, record.len() as u64)?;
        drop(batch);
        order.build(record, context)?;
    }
    if start != source.rows() {
        return Err(failed("window source changed its completed length"));
    }
    source.validate()?;
    let ordered = order.retain(context)?;
    let result_spec = layout.group_results(group)?;
    let mut results = Ordering::new(&result_spec, spill, limit, context)?;
    let rows =
        usize::try_from(ordered.rows()).map_err(|_| failed("window partition count overflow"))?;
    let mut start = 0;
    while start < rows {
        let mut scan = Lookup::<2>::new(&ordered, &layout.group_names, context)?;
        let mut end = start + 1;
        while end < rows && same(&mut scan, &group.partition, start, end, context)? {
            if end.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            end += 1;
        }
        add(&mut report.lookup_blocks, scan.blocks)?;
        drop(scan);
        let mut input = lookup::Partition {
            lookup: Lookup::new(&ordered, &layout.group_names, context)?,
            range: start..end,
        };
        partition::run(
            spec,
            group,
            layout,
            &mut input,
            spill,
            context,
            limit,
            &result_spec,
            &mut results,
            report,
        )?;
        add(&mut report.lookup_blocks, input.lookup.blocks)?;
        add(&mut report.partitions, 1)?;
        start = end;
    }
    ordered.finish(context)?;
    add(&mut report.groups, 1)?;
    results.retain(context)
}

fn same(
    input: &mut Lookup<'_, '_>,
    keys: &[usize],
    left: usize,
    right: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    for &key in keys {
        if !input.compare(left, right, key, context)?.is_eq() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn selected_column(spec: &Spec, index: usize) -> Result<&str> {
    match &spec.functions[index] {
        Function::Lag { column, .. } | Function::Lead { column, .. } => Ok(column.as_str()),
        Function::Framed(_) => spec.frames[index]
            .as_ref()
            .and_then(|frame| frame.column.as_deref())
            .ok_or_else(|| failed("stored window selection has no source column")),
        _ => Err(failed("stored window selection has no source function")),
    }
}

#[allow(clippy::too_many_arguments)] // Final assembly joins only certified original ordinals, then detaches public buffers.
fn output(
    spec: &Spec,
    layout: &Layout,
    source: &StoredOrder<'_>,
    groups: &[StoredOrder<'_>],
    context: &NativeExecutionContext<'_>,
    limit: usize,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    report: &mut Report,
) -> Result<()> {
    let mut input = Lookup::<2>::new(source, &layout.source.names, context)?;
    let rows = input.rows()?;
    let mut results = ReservedVec::new(context.memory())?;
    results.reserve(groups.len())?;
    for store in groups {
        if store.rows() != source.rows() {
            return Err(failed("window group produced a different source length"));
        }
        results
            .values
            .push(Lookup::<2>::new(store, &layout.source.names, context)?);
    }
    let mut positions = ReservedVec::new(context.memory())?;
    positions.reserve(limit)?;
    for start in (0..rows).step_by(limit) {
        let end = start.saturating_add(limit).min(rows);
        positions.values.clear();
        positions.values.extend((start..end).map(Some));
        for row in start..end {
            if input.unsigned(row, 0, context)? != row {
                return Err(failed("window original input ordinal changed"));
            }
            for group in &mut results.values {
                if group.unsigned(row, 0, context)? != row {
                    return Err(failed("window group result ordinal changed"));
                }
            }
        }
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(spec.fields.len())?;
        for (name, (_, dtype)) in spec.columns.iter().zip(&spec.fields) {
            columns.values.push(input.gather(
                &positions.values,
                Some(DATA),
                name,
                dtype,
                context,
            )?);
        }
        for (index, result) in layout.results.iter().enumerate() {
            let group = spec
                .groups
                .iter()
                .position(|group| group.functions.contains(&index))
                .ok_or_else(|| failed("window function has no ordering group"))?;
            let values = results.values[group].gather(
                &positions.values,
                None,
                &result.name,
                &result.kind.dtype(),
                context,
            )?;
            columns.values.push(result_column(
                spec,
                index,
                result.kind,
                values,
                &mut input,
                context,
            )?);
        }
        consume(private::delivered(
            &spec.fields,
            columns,
            end - start,
            context,
        )?)?;
        add(&mut report.output_rows, (end - start) as u64)?;
        context.check_cancelled()?;
    }
    add(&mut report.lookup_blocks, input.blocks)?;
    for group in &results.values {
        add(&mut report.lookup_blocks, group.blocks)?;
    }
    source.validate()?;
    for group in groups {
        group.validate()?;
    }
    Ok(())
}

fn result_column(
    spec: &Spec,
    index: usize,
    kind: Kind,
    values: ArrayRef,
    input: &mut Lookup<'_, '_>,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let dtype = &spec.fields[spec.columns.len() + index].1;
    match kind {
        Kind::Source => {
            let mut execution = context.native_session().create_execution_ctx();
            let values = values
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            let valid = values
                .validity()
                .map_err(vortex_error)?
                .execute_mask(values.len(), &mut execution)
                .map_err(vortex_error)?;
            let mut selected = ReservedVec::new(context.memory())?;
            selected.reserve(values.len())?;
            let ordinals = values.to_buffer::<u64>();
            for row in 0..values.len() {
                selected.values.push(if valid.value(row) {
                    Some(usize::try_from(ordinals[row]).map_err(vortex_error)?)
                } else {
                    None
                });
            }
            input.gather(
                &selected.values,
                Some(DATA),
                selected_column(spec, index)?,
                dtype,
                context,
            )
        }
        Kind::Integer => {
            let mut execution = context.native_session().create_execution_ctx();
            let values = values
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            let values = values.to_buffer::<u64>();
            result_batch::build_column(
                dtype,
                values.len(),
                &context.native_session().allocator(),
                |row| {
                    Ok(Value::Int(
                        i64::try_from(values[row]).map_err(vortex_error)?,
                    ))
                },
            )
        }
        _ => Ok(values),
    }
}
