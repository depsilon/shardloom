//! Explicit native join pressure strategy using the shared stable run machinery.

use super::{
    ArrayRef, Batch, DType, Kind, NativeExecutionContext, Pairs, ReservedVec, Result, Side, Spec,
    Table, failed,
};
use crate::local_primitives::{
    native_payload,
    native_relational_spill::{Ordering, State, StoredOrder},
};

#[path = "native_relational_join_lookup.rs"]
mod lookup;
#[path = "native_relational_join_outer.rs"]
mod outer;
#[path = "native_relational_join_records.rs"]
mod records;

#[derive(Default)]
pub(in crate::local_primitives) struct Report {
    pub(in crate::local_primitives) build_rows: u64,
    pub(in crate::local_primitives) build_batches_detached: u64,
    pub(in crate::local_primitives) probe_rows: u64,
    pub(in crate::local_primitives) candidate_rows: u64,
    pub(in crate::local_primitives) match_records: u64,
    pub(in crate::local_primitives) lookup_blocks: u64,
    pub(in crate::local_primitives) output_rows: u64,
}

fn add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| failed("ordered join counter overflow"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One bound join, shared grant/store and two already admitted inputs.
pub(in crate::local_primitives) fn run(
    spec: &Spec,
    right_fields: &[(String, DType)],
    spill: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    right_input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    left_input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<Report> {
    if batch_rows == 0 {
        return Err(failed("output batch rows must be positive"));
    }
    let layout = records::Layout::new(spec, right_fields, context)?;
    let mut report = Report::default();
    let right = build(
        spec,
        &layout,
        spill,
        context,
        batch_rows,
        right_input,
        &mut report,
    )?;
    let mut search = lookup::Search::new(&right, spec);
    let mut matches = outer::Matches::new(spec, &layout, spill, batch_rows, context)?;
    let empty = Table::new(context.memory())?;
    let mut pairs = Pairs::new(batch_rows, context.memory())?;
    let mut rights = ReservedVec::new(context.memory())?;
    rights.reserve(batch_rows)?;
    // The shared join builder compacts values. This final native copy also ties
    // the public parent-schema credit to every independently retained child.
    let mut delivered = |array: ArrayRef| consume(native_payload::detach(&array, context)?);
    left_input(&mut |array| {
        add(&mut report.probe_rows, array.len() as u64)?;
        let left = Batch::new(array, &spec.left_keys, context)?;
        for row in 0..left.array.len() {
            context.check_cancelled()?;
            let mut range = search.range(&left, row, context)?;
            let mut matched = false;
            while range.start < range.end {
                let candidates = search.candidates(&left, row, &mut range, batch_rows, context)?;
                if candidates.table.rows() == 0 {
                    break;
                }
                add(&mut report.candidate_rows, candidates.table.rows() as u64)?;
                rights.values.clear();
                rights.values.extend(0..candidates.table.rows());
                let selected = spec.select_candidates(
                    &left.array,
                    &candidates.table,
                    row,
                    &rights.values,
                    context,
                )?;
                matched |= !selected.values.is_empty();
                if matched && spec.short_circuit() {
                    break;
                }
                for &candidate in &selected.values {
                    if let Some(matches) = &mut matches {
                        matches.push(candidates.positions.values[candidate], context)?;
                    }
                    pairs.left.values.push(Some(row));
                    pairs.right.values.push(Some(candidate));
                }
                spec.flush(
                    &candidates.table,
                    Some(&left.array),
                    &mut pairs,
                    &mut report.output_rows,
                    context,
                    &mut delivered,
                )?;
            }
            if spec.keep_unpaired_left(matched) {
                pairs.left.values.push(Some(row));
                pairs.right.values.push(None);
                spec.flush(
                    &empty,
                    Some(&left.array),
                    &mut pairs,
                    &mut report.output_rows,
                    context,
                    &mut delivered,
                )?;
            }
        }
        context.check_cancelled()
    })?;
    report.lookup_blocks = search.blocks;
    drop((search, rights, pairs, empty));
    if let Some(matches) = matches {
        report.match_records = matches.records;
        outer::finish(
            matches,
            right,
            spec,
            &layout,
            spill,
            context,
            batch_rows,
            &mut report.output_rows,
            &mut delivered,
        )?;
    } else {
        right.finish(context)?;
    }
    context.check_cancelled()?;
    Ok(report)
}

fn build<'a>(
    spec: &Spec,
    layout: &'a records::Layout,
    spill: &'a State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    report: &mut Report,
) -> Result<StoredOrder<'a>> {
    let mut build = Ordering::new(&layout.build, spill, batch_rows, context)?;
    input(&mut |array| {
        context.check_cancelled()?;
        let ordinal = report.build_rows;
        add(&mut report.build_rows, array.len() as u64)?;
        if !array.is_empty() {
            add(&mut report.build_batches_detached, 1)?;
        }
        let input = Batch::new(array, &spec.right_keys, context)?;
        for start in (0..input.array.len()).step_by(batch_rows.min(1024)) {
            let end = start
                .saturating_add(batch_rows.min(1024))
                .min(input.array.len());
            build.build(
                layout.record(&input, start..end, ordinal, context)?,
                context,
            )?;
        }
        context.check_cancelled()
    })?;
    build.retain(context)
}
