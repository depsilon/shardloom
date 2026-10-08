//! Exact general aggregates using the shared stable native ordering/run store.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_payload, native_relational_aggregate as aggregate,
    native_relational_batch::{Batch, failed, index_array, take_batch},
    native_relational_expression::keys,
    native_relational_keys::{Cell, KeyColumn},
    native_relational_spill::{Ordering, State},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{ArrayRef, dtype::DType};

#[path = "native_relational_aggregate_records.rs"]
mod records;
use records::{Layout, ORDINAL};
#[path = "native_relational_aggregate_output.rs"]
mod output;
use output::Results;

#[derive(Default)]
pub(super) struct Report {
    pub(super) input_rows: u64,
    pub(super) distinct_rows: u64,
}

#[allow(clippy::too_many_arguments)] // One bound aggregate shares its caller's input, grant, run store and consumer.
pub(super) fn run(
    spec: &aggregate::Spec,
    input_fields: &[(String, DType)],
    spill: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<Report> {
    let layout = Layout::new(spec, input_fields, context)?;
    let mut order = Ordering::new(&layout.records, spill, batch_rows, context)?;
    let mut report = Report::default();
    input(&mut |array| {
        context.check_cancelled()?;
        let ordinal = report.input_rows;
        report.input_rows = report
            .input_rows
            .checked_add(array.len() as u64)
            .ok_or_else(|| failed("aggregate input ordinal overflow"))?;
        if array.is_empty() {
            return Ok(());
        }
        order.build(layout.base(&array, ordinal, context)?, context)?;
        for kind in 0..layout.distinct.len() {
            context.check_cancelled()?;
            if let Some(record) = layout.distinct_record(&array, kind, context)? {
                report.distinct_rows = report
                    .distinct_rows
                    .checked_add(record.len() as u64)
                    .ok_or_else(|| failed("aggregate distinct record count overflow"))?;
                order.build(record, context)?;
            }
        }
        context.check_cancelled()
    })?;
    let mut output = Results::new(&layout.results, spill, batch_rows, context)?;
    let mut reducer = Reducer {
        layout: &layout,
        spec,
        group: None,
    };
    order.finish(context, batch_rows, &mut |array| {
        reducer.consume(array, context, batch_rows, &mut output)
    })?;
    reducer.finish(context, batch_rows, &mut output)?;
    drop(reducer);
    output.finish(context, batch_rows, &mut |array| {
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(spec.fields.len())?;
        for (name, _) in &layout.results.fields[..spec.fields.len()] {
            columns
                .values
                .push(logical_field_from_native_array(&array, name)?);
        }
        consume(records::delivered(
            &spec.fields,
            columns,
            array.len(),
            context,
        )?)
    })?;
    Ok(report)
}

struct PreviousDistinct {
    kind: usize,
    _array: ArrayRef,
    key: KeyColumn,
}

struct Group<'a> {
    key: Batch,
    first: u64,
    aggregate: aggregate::Aggregate<'a>,
    previous: Option<PreviousDistinct>,
}

struct Reducer<'a> {
    layout: &'a Layout,
    spec: &'a aggregate::Spec,
    group: Option<Group<'a>>,
}

fn integer(values: &KeyColumn, row: usize) -> Result<u64> {
    match values.cell(row)? {
        Cell::NonnegativeInteger(value) => Ok(value),
        _ => Err(failed(
            "ordered aggregate ordinal or record kind is invalid",
        )),
    }
}

impl Reducer<'_> {
    fn consume(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        output: &mut Results<'_>,
    ) -> Result<()> {
        let batch = Batch::new(array, &self.layout.group_names, context)?;
        let kinds = records::kind(&batch.array, context)?;
        let ordinals = keys(
            &logical_field_from_native_array(&batch.array, ORDINAL)?,
            context,
        )?;
        let mut start = 0;
        while start < batch.array.len() {
            context.check_cancelled()?;
            let kind = integer(&kinds, start)?;
            if let Some(group) = &self.group
                && !group.key.equal(0, &batch, start, true)?
            {
                self.finish_group(context, batch_rows, output)?;
            }
            if self.group.is_none() {
                if kind != 0 {
                    return Err(failed("ordered aggregate group has no base records"));
                }
                let key = take_batch(&batch.array, &self.layout.group_fields, &[start], context)?;
                self.group = Some(Group {
                    key: Batch::new(key, &self.layout.group_names, context)?,
                    first: integer(&ordinals, start)?,
                    aggregate: aggregate::Aggregate::new_ordered(
                        &self.layout.scalar,
                        context.memory(),
                    )?,
                    previous: None,
                });
            }
            let mut end = start + 1;
            while end < batch.array.len()
                && integer(&kinds, end)? == kind
                && batch.equal(start, &batch, end, true)?
            {
                if end.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                end += 1;
            }
            let group = self
                .group
                .as_mut()
                .ok_or_else(|| failed("ordered aggregate group is absent"))?;
            if kind == 0 {
                if group.previous.is_some() {
                    return Err(failed("aggregate base records follow distinct records"));
                }
                let selected = batch.array.slice(start..end).map_err(vortex_error)?;
                group
                    .aggregate
                    .consume_ordered_base(&selected, context, batch_rows)?;
            } else {
                let kind = usize::try_from(kind - 1).map_err(vortex_error)?;
                consume_distinct(group, self.layout, &batch.array, start, end, kind, context)?;
            }
            start = end;
        }
        context.check_cancelled()
    }

    fn finish(
        &mut self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        output: &mut Results<'_>,
    ) -> Result<()> {
        if self.group.is_none() && self.layout.group_names.is_empty() {
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(1)?;
            columns
                .values
                .push(records::unsigned(1, context, |_| Ok(0))?);
            let key = records::structure(&self.layout.group_fields, columns, 1)?;
            self.group = Some(Group {
                key: Batch::new(key, &[], context)?,
                first: 0,
                aggregate: aggregate::Aggregate::new_ordered(
                    &self.layout.scalar,
                    context.memory(),
                )?,
                previous: None,
            });
        }
        self.finish_group(context, batch_rows, output)
    }

    fn finish_group(
        &mut self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        output: &mut Results<'_>,
    ) -> Result<()> {
        let Some(group) = self.group.take() else {
            return Ok(());
        };
        let mut emit = |values: Option<ArrayRef>| {
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(self.layout.results.fields.len())?;
            let indices = index_array(1, false, context, |_| Ok(Some(0)))?;
            for (index, (_, dtype)) in self.spec.groups.iter().enumerate() {
                let value = logical_field_from_native_array(
                    &group.key.array,
                    &self.layout.group_names[index],
                )?;
                columns
                    .values
                    .push(native_payload::take(&value, &indices, dtype, context)?);
            }
            for (name, _) in &self.layout.scalar.fields {
                let array = values
                    .as_ref()
                    .ok_or_else(|| failed("ordered aggregate measures are absent"))?;
                columns
                    .values
                    .push(logical_field_from_native_array(array, name)?);
            }
            columns
                .values
                .push(records::unsigned(1, context, |_| Ok(group.first))?);
            output.push(
                records::structure(&self.layout.results.fields, columns, 1)?,
                context,
            )
        };
        if self.layout.scalar.measures.is_empty() {
            emit(None)
        } else {
            group
                .aggregate
                .finish(context, batch_rows, &mut |array| emit(Some(array)))
        }
    }
}

#[allow(clippy::too_many_arguments)] // One contiguous kind/group span updates the existing exact scalar reducer.
fn consume_distinct(
    group: &mut Group<'_>,
    layout: &Layout,
    array: &ArrayRef,
    start: usize,
    end: usize,
    kind: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let entry = layout
        .distinct
        .get(kind)
        .ok_or_else(|| failed("ordered aggregate distinct kind is out of bounds"))?;
    if group
        .previous
        .as_ref()
        .is_some_and(|previous| previous.kind > kind)
    {
        return Err(failed("aggregate distinct record kinds are out of order"));
    }
    let column = logical_field_from_native_array(array, &entry.name)?;
    let values = keys(&column, context)?;
    let mut count = 0_u64;
    for row in start..end {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if values.is_null(row)? {
            return Err(failed("ordered distinct record contains a null value"));
        }
        let duplicate = if row > start {
            values.equals_at(row, &values, row - 1, false)?
        } else {
            match &group.previous {
                Some(previous) if previous.kind == kind => {
                    values.equals_at(row, &previous.key, 0, false)?
                }
                _ => false,
            }
        };
        if !duplicate {
            count = count
                .checked_add(1)
                .ok_or_else(|| failed("distinct count overflow"))?;
        }
    }
    for &measure in &entry.measures {
        group.aggregate.add_ordered_distinct(measure, count)?;
    }
    // Keep only the final value across a batch boundary, while the prior owner
    // remains credited during replacement. Do not retain the whole run block.
    let indices = index_array(1, false, context, |_| Ok(Some(end - 1)))?;
    let selected = native_payload::take(&column, &indices, &entry.dtype, context)?;
    let key = keys(&selected, context)?;
    group.previous = Some(PreviousDistinct {
        kind,
        _array: selected,
        key,
    });
    Ok(())
}
