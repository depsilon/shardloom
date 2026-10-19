//! Predicate populations over the existing quota-accounted native run store.

use super::{ArrayRef, Batch, DType, Kind, NativeExecutionContext, Report, Result, Spec};
use crate::local_primitives::{
    native_relational_batch::failed,
    native_relational_spill::{Ordering, State},
};

#[path = "native_relational_subquery_lookup.rs"]
mod lookup;
#[path = "native_relational_subquery_records.rs"]
mod records;

#[cfg(test)]
pub(crate) use lookup::BEFORE_CACHE_HIT;

pub(in crate::local_primitives) struct Predicate<'a> {
    pub(super) spec: &'a Spec,
    search: lookup::Search<'a>,
    report: Report,
}

fn add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| failed("subquery predicate counter overflow"))?;
    Ok(())
}

pub(super) fn build<'a>(
    spec: &'a Spec,
    fields: &[(String, DType)],
    state: &'a State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
) -> Result<Predicate<'a>> {
    if batch_rows == 0 || matches!(spec.kind, Kind::Scalar { .. }) {
        return Err(failed(
            "predicate population requires positive batches and a predicate",
        ));
    }
    let layout = records::Layout::new(spec, fields, context)?;
    // Bound simultaneous lookup/merge payloads; this is block geometry, not a
    // population or record-width limit. The shared sorter handles large rows.
    let block_rows = batch_rows.min(64);
    let mut order = Ordering::new(&layout.order, state, block_rows, context)?;
    // Parameterized builds can coexist with source replay and its merge readers.
    // Retained keys are only one owner of the grant; preserve transient headroom.
    order.limit_buffer(context.memory().snapshot().limit_bytes / 8)?;
    let mut report = Report {
        stages: 1,
        ..Report::default()
    };
    input(&mut |array| {
        context.check_cancelled()?;
        let ordinal = report.input_rows;
        add(&mut report.input_rows, array.len() as u64)?;
        let input = Batch::new(array, &spec.right_keys, context)?;
        for row in 0..input.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            // Validate every key, even after a null prefix or an eventual
            // predicate short circuit. Only then may irrelevant rows be omitted.
            input.hash(row, true)?;
        }
        for start in (0..input.array.len()).step_by(block_rows) {
            let end = start.saturating_add(block_rows).min(input.array.len());
            let record = layout.record(spec, &input, start..end, ordinal, context)?;
            add(&mut report.indexed_rows, record.len() as u64)?;
            if !record.is_empty() {
                order.build(record, context)?;
            }
        }
        context.check_cancelled()
    })?;
    let source = order.retain(context)?;
    report.spilled_stages = u64::from(source.is_spilled());
    Ok(Predicate {
        spec,
        search: lookup::Search::new(source, spec),
        report,
    })
}

impl Predicate<'_> {
    pub(super) fn result(
        &mut self,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        context.check_cancelled()?;
        left.hash(row, true)?;
        add(&mut self.report.probe_rows, 1)?;
        let value = self.search.evaluate(left, row, context)?;
        Ok(if self.spec.negated {
            value.map(|value| !value)
        } else {
            value
        })
    }

    pub(super) fn consume_selected(
        &mut self,
        array: ArrayRef,
        selected: &[usize],
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let left = Batch::new(array, &self.spec.left_keys, context)?;
        let mut values =
            crate::local_primitives::native_capacity::ReservedVec::new(context.memory())?;
        values.reserve(left.array.len())?;
        values
            .values
            .resize(left.array.len(), self.spec.inactive_value());
        for &row in selected {
            values.values[row] = self.result(&left, row, context)?;
        }
        consume(self.spec.output(&left.array, &values.values, context)?)?;
        context.check_cancelled()
    }

    pub(super) fn finish(mut self, context: &NativeExecutionContext<'_>) -> Result<Report> {
        self.report.candidate_rows = self.search.candidates;
        self.report.lookup_blocks = self.search.blocks;
        self.search.finish(context)?;
        Ok(self.report)
    }
}
