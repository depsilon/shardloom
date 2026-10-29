//! As-of lookup over the existing bounded native ordering and lookup owners.

use super::{
    Batch, NativeExecutionContext, Pairs, Result, Search, Spec, candidate, compare, options,
    select, validate,
};
use crate::{
    local_primitives::{
        native_payload, native_relational_sort,
        native_relational_spill::{Ordering, State},
        native_relational_window::spill::lookup::Lookup,
        vortex_error,
    },
    relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey},
};
use shardloom_core::ColumnRef;
use std::cmp::Ordering as Comparison;
use vortex::array::{ArrayRef, dtype::DType};

use super::super::spill::Report;

fn add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| super::failed("as-of counter overflow"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One bound join shares the admitted store, grant, inputs and consumer.
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
    let policy = options(spec)?;
    let mut report = Report::default();
    let _metadata = context.memory().reserve(
        u64::try_from(right_fields.len().saturating_add(spec.right_keys.len()))
            .map_err(vortex_error)?
            .checked_mul(8192)
            .ok_or_else(|| super::failed("as-of sort metadata overflow"))?,
    )?;
    let sort_spec = native_relational_sort::Spec {
        fields: right_fields.to_vec(),
        keys: spec
            .right_keys
            .iter()
            .map(|name| {
                Ok(VortexRelationalOrderKey {
                    column: ColumnRef::new(name.clone())?,
                    descending: false,
                    nulls: Some(VortexRelationalNullOrder::First),
                })
            })
            .collect::<Result<_>>()?,
        names: spec.right_keys.clone(),
        copy_policy: native_payload::CopyPolicy::PreserveUnobserved,
    };
    let mut order = Ordering::new(&sort_spec, spill, batch_rows, context)?;
    right_input(&mut |array| {
        context.check_cancelled()?;
        add(&mut report.build_rows, array.len() as u64)?;
        if !array.is_empty() {
            add(&mut report.build_batches_detached, 1)?;
        }
        order.build(
            native_payload::detach_with_policy(
                &array,
                native_payload::CopyPolicy::PreserveUnobserved,
                context,
            )?,
            context,
        )
    })?;
    let stored = order.retain(context)?;
    let mut search = StoredSearch {
        lookup: Lookup::new(&stored, &spec.right_keys, context)?,
        candidates: 0,
    };
    let mut pairs = Pairs::new(batch_rows, context.memory())?;
    // Bind schema and parent metadata to independently retained child outputs,
    // as on the existing ordered join path.
    let mut delivered = |array: ArrayRef| consume(native_payload::detach(&array, context)?);
    left_input(&mut |array| {
        add(&mut report.probe_rows, array.len() as u64)?;
        let left = Batch::for_ordering(array, &spec.left_keys, context)?;
        validate(&left, context)?;
        for row in 0..left.array.len() {
            context.check_cancelled()?;
            let selected = select(
                policy,
                spec.left_keys.len(),
                &left,
                row,
                &mut search,
                context,
            )?;
            if selected.is_some() {
                add(&mut report.match_records, 1)?;
            }
            pairs.left.values.push(Some(row));
            pairs.right.values.push(selected);
            if pairs.left.values.len() == pairs.limit {
                spec.flush_with_right(
                    Some(&left.array),
                    &mut pairs,
                    &mut report.output_rows,
                    context,
                    &mut delivered,
                    |name, dtype, rows| search.lookup.gather(rows, None, name, dtype, context),
                )?;
            }
        }
        spec.flush_with_right(
            Some(&left.array),
            &mut pairs,
            &mut report.output_rows,
            context,
            &mut delivered,
            |name, dtype, rows| search.lookup.gather(rows, None, name, dtype, context),
        )?;
        context.check_cancelled()
    })?;
    report.lookup_blocks = search.lookup.blocks;
    report.candidate_rows = search.candidates;
    drop(search);
    stored.finish(context)?;
    Ok(report)
}

struct StoredSearch<'a, 's> {
    lookup: Lookup<'a, 's>,
    candidates: u64,
}
impl Search for StoredSearch<'_, '_> {
    fn rows(&self) -> Result<usize> {
        self.lookup.rows()
    }
    fn compare(
        &mut self,
        position: usize,
        left: &Batch,
        row: usize,
        keys: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Comparison> {
        self.lookup.with_batch(position, context, |right, local| {
            compare(right, local, left, row, keys)
        })
    }
    fn candidate(
        &mut self,
        position: usize,
        left: &Batch,
        row: usize,
        keys: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<i128>> {
        add(&mut self.candidates, 1)?;
        self.lookup.with_batch(position, context, |right, local| {
            candidate(right, local, left, row, keys)
        })
    }
}
