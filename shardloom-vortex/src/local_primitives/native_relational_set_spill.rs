//! Exact null-equal set membership through bounded native runs and stable ordinals.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::{Batch, failed, index_array, take_batch},
    native_relational_expression::keys,
    native_relational_keys::Cell,
    native_relational_record_output::Results,
    native_relational_records::{self as records, ORDINAL},
    native_relational_sort::Spec,
    native_relational_spill::{Ordering, State},
};
use crate::{
    relational_query::VortexRelationalSetKind as Kind, resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{ArrayRef, dtype::DType};

const SIDE: &str = "side";
// Key reduction and first-occurrence ordering overlap. Their live run blocks
// must stay smaller than the generic single-order maximum even when callers
// request large output batches. This limits work granularity, not input size;
// every actual payload/reader owner still needs admission from the same grant.
const BLOCK_ROWS: usize = 256;

#[derive(Default)]
pub(super) struct Report {
    pub(super) input_rows: u64,
    pub(super) groups: u64,
    pub(super) output_rows: u64,
}

fn add(value: &mut u64, amount: u64) -> Result<()> {
    *value = value
        .checked_add(amount)
        .ok_or_else(|| failed("ordered set counter overflow"))?;
    Ok(())
}

/// The caller supplies each already-aligned source exactly once. The original
/// resident traversal is preserved: union left then right; other sets right
/// then left. The ordinal is global to that traversal, with explicit side tags.
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    kind: Kind,
    fields: &[(String, DType)],
    state: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    mut input: impl FnMut(bool, &mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<Report> {
    if kind == Kind::UnionAll || batch_rows == 0 {
        return Err(failed(
            "ordered set requires a distinct kind and positive batch size",
        ));
    }
    let block_rows = batch_rows.min(BLOCK_ROWS);
    let layout = Layout::new(fields, context)?;
    let mut order = Ordering::new(&layout.keys, state, block_rows, context)?;
    let mut report = Report::default();
    let sides = if kind == Kind::UnionDistinct {
        [true, false]
    } else {
        [false, true]
    };
    for left in sides {
        input(left, &mut |array| {
            context.check_cancelled()?;
            let ordinal = report.input_rows;
            add(&mut report.input_rows, array.len() as u64)?;
            for start in (0..array.len()).step_by(block_rows) {
                let end = start.saturating_add(block_rows).min(array.len());
                let record = layout.record(&array, fields, start..end, ordinal, left, context)?;
                order.build(record, context)?;
            }
            context.check_cancelled()
        })?;
    }
    let mut output = Results::new(&layout.output, state, block_rows, context)?;
    let mut reducer = Reducer {
        kind,
        layout: &layout,
        group: None,
        report: &mut report,
    };
    order.finish(context, block_rows, &mut |array| {
        reducer.consume(array, context, &mut output)
    })?;
    reducer.finish(context, &mut output)?;
    drop(reducer);
    output.finish(context, block_rows, &mut |array| {
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(fields.len())?;
        for name in &layout.names {
            columns
                .values
                .push(logical_field_from_native_array(&array, name)?);
        }
        consume(records::delivered(fields, columns, array.len(), context)?)
    })?;
    context.check_cancelled()?;
    Ok(report)
}

struct Layout {
    keys: Spec,
    output: Spec,
    names: Vec<String>,
    _metadata: MemoryLease,
}

impl Layout {
    fn new(fields: &[(String, DType)], context: &NativeExecutionContext<'_>) -> Result<Self> {
        // Covers both cloned sort descriptions, private names and logical types.
        // Payload/key owners and their growth still reserve separately.
        let bytes = fields.iter().try_fold(16_384_u64, |total, (_, dtype)| {
            native_payload::metadata_bytes(dtype)?
                .checked_add(2048)
                .and_then(|field| field.checked_mul(8))
                .and_then(|field| total.checked_add(field))
                .ok_or_else(|| failed("ordered set descriptor capacity overflow"))
        })?;
        let metadata = context.memory().reserve(bytes)?;
        let names = (0..fields.len())
            .map(|index| format!("key_{index}"))
            .collect::<Vec<_>>();
        let mut private = fields
            .iter()
            .zip(&names)
            .map(|((_, dtype), name)| (name.clone(), dtype.clone()))
            .collect::<Vec<_>>();
        private.push((SIDE.into(), records::u64_type()));
        private.push((ORDINAL.into(), records::u64_type()));
        let mut key_order = names.clone();
        key_order.push(ORDINAL.into());
        Ok(Self {
            keys: records::order(private.clone(), key_order)?,
            output: records::order(private, vec![ORDINAL.into()])?,
            names,
            _metadata: metadata,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        array: &ArrayRef,
        fields: &[(String, DType)],
        range: std::ops::Range<usize>,
        ordinal: u64,
        left: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let rows = range.len();
        let indices = index_array(rows, false, context, |row| Ok(Some(range.start + row)))?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.keys.fields.len())?;
        for (name, dtype) in fields {
            let column = logical_field_from_native_array(array, name)?;
            columns
                .values
                .push(native_payload::take(&column, &indices, dtype, context)?);
        }
        columns
            .values
            .push(records::unsigned(rows, context, |_| Ok(u64::from(!left)))?);
        columns.values.push(records::unsigned(rows, context, |row| {
            ordinal
                .checked_add((range.start + row) as u64)
                .ok_or_else(|| failed("ordered set input ordinal overflow"))
        })?);
        records::structure(&self.keys.fields, columns, rows)
    }
}

struct Group {
    first: Batch,
    left: bool,
    right: bool,
}

struct Reducer<'a> {
    kind: Kind,
    layout: &'a Layout,
    group: Option<Group>,
    report: &'a mut Report,
}

impl Reducer<'_> {
    fn consume(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        output: &mut Results<'_>,
    ) -> Result<()> {
        let batch = Batch::new(array, &self.layout.names, context)?;
        let sides = keys(
            &logical_field_from_native_array(&batch.array, SIDE)?,
            context,
        )?;
        for row in 0..batch.array.len() {
            context.check_cancelled()?;
            let left = match sides.cell(row)? {
                Cell::NonnegativeInteger(0) => true,
                Cell::NonnegativeInteger(1) => false,
                _ => return Err(failed("ordered set record has an invalid source side")),
            };
            if let Some(group) = &self.group
                && !group.first.equal(0, &batch, row, true)?
            {
                self.finish(context, output)?;
            }
            if self.group.is_none() {
                let first = take_batch(&batch.array, &self.layout.keys.fields, &[row], context)?;
                self.group = Some(Group {
                    first: Batch::new(first, &self.layout.names, context)?,
                    left,
                    right: !left,
                });
                add(&mut self.report.groups, 1)?;
                continue;
            }
            let group = self
                .group
                .as_mut()
                .ok_or_else(|| failed("ordered set group is absent"))?;
            if left && !group.left {
                // INTERSECT/EXCEPT must return the actual first left payload,
                // even when an equal right row has a distinct signed-zero bit.
                let first = take_batch(&batch.array, &self.layout.keys.fields, &[row], context)?;
                group.first = Batch::new(first, &self.layout.names, context)?;
            }
            group.left |= left;
            group.right |= !left;
        }
        context.check_cancelled()
    }

    fn finish(
        &mut self,
        context: &NativeExecutionContext<'_>,
        output: &mut Results<'_>,
    ) -> Result<()> {
        let Some(group) = self.group.take() else {
            return Ok(());
        };
        let keep = match self.kind {
            Kind::UnionDistinct => true,
            Kind::Intersect => group.left && group.right,
            Kind::Except => group.left && !group.right,
            Kind::UnionAll => return Err(failed("ordered set received UNION ALL")),
        };
        if keep {
            output.push(group.first.array, context)?;
            add(&mut self.report.output_rows, 1)?;
        }
        context.check_cancelled()
    }
}
