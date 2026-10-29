//! Exact one-pass sampling populations over the shared native ordering store.

use super::{
    BoundUnary, NativeBatch, NativeExecutionContext, ReservedVec, Result, UnaryOutput, Value,
    VortexQueryPrimitiveKind as Kind, failed, report::StateUsage, values,
};
use crate::local_primitives::{
    completed_result::CompletedRows,
    native_relational_spill::{Ordering, State},
};
use std::borrow::Borrow as _;
use vortex::array::ArrayRef;

#[path = "local_primitive_unary_sampling_access.rs"]
mod access;
#[path = "local_primitive_unary_sampling_records.rs"]
mod records;
use records::Schema;

const BLOCK_ROWS: usize = 256;
const PENDING_BYTES: u64 = 256 * 1024;

#[cfg(test)]
type BeforeCacheHit = Box<dyn FnOnce(&std::path::Path)>;
#[cfg(test)]
thread_local! {
    pub(in crate::local_primitives) static BEFORE_POPULATION_CACHE_HIT: std::cell::RefCell<Option<BeforeCacheHit>> = const { std::cell::RefCell::new(None) };
}

#[derive(Default)]
pub(in crate::local_primitives) struct Report {
    pub(in crate::local_primitives) input_rows: u64,
    pub(in crate::local_primitives) population_rows: u64,
    pub(in crate::local_primitives) output_rows: u64,
    pub(in crate::local_primitives) replacement_draws: u64,
    pub(in crate::local_primitives) lookup_blocks: u64,
}

struct Record {
    ordinal: u64,
    score: u64,
    cumulative_weight: u64,
    row: values::OwnedRow,
}

struct Population<'p, 's> {
    schema: &'p Schema,
    order: Ordering<'p, 's>,
    pending: ReservedVec<Record>,
    bytes: u64,
    block_rows: usize,
}

impl<'p, 's> Population<'p, 's> {
    fn new(
        schema: &'p Schema,
        state: &'s State,
        block_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            order: Ordering::new(&schema.population, state, block_rows, context)?,
            pending: ReservedVec::new(context.memory())?,
            bytes: 0,
            block_rows,
        })
    }

    fn push(&mut self, record: Record, context: &NativeExecutionContext<'_>) -> Result<()> {
        let bytes = record
            .row
            .retained_bytes()?
            .checked_add(std::mem::size_of::<Record>() as u64)
            .ok_or_else(|| failed("sampling population buffer overflow"))?;
        if self.pending.values.len() >= self.block_rows
            || bytes > PENDING_BYTES.saturating_sub(self.bytes)
        {
            self.flush(context)?;
        }
        self.pending.push(record)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| failed("sampling population buffer overflow"))?;
        if self.bytes >= PENDING_BYTES {
            self.flush(context)?;
        }
        Ok(())
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.pending.values.is_empty() {
            return Ok(());
        }
        let array = self.schema.build(&self.pending.values, context)?;
        self.pending.values.clear();
        self.bytes = 0;
        self.order.build(array, context)
    }

    fn complete(mut self, context: &NativeExecutionContext<'_>) -> Result<Ordering<'p, 's>> {
        self.flush(context)?;
        Ok(self.order)
    }
}

fn add(value: &mut u64, count: u64) -> Result<()> {
    *value = value
        .checked_add(count)
        .ok_or_else(|| failed("sampling population counter overflow"))?;
    Ok(())
}

pub(super) fn admitted(plan: &BoundUnary, input_rows: Option<u64>) -> bool {
    plan.request.kind == Kind::SampleRows
        && (plan.request.sample_with_replacement
            || (plan.request.sample_fraction.is_some() && input_rows.is_none()))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One population observes one producer and completes before output.
pub(super) fn run(
    plan: &BoundUnary,
    state: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    produce: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<(StateUsage, Report)> {
    if !admitted(plan, None) || batch_rows == 0 {
        return Err(failed(
            "sampling spill requires an admitted selector and positive batch bound",
        ));
    }
    let block_rows = batch_rows.min(BLOCK_ROWS);
    let schema = Schema::new(plan, context)?;
    let mut population = Population::new(&schema, state, block_rows, context)?;
    let mut report = Report::default();
    let mut seen = 0_usize;
    let mut total_weight = 0.0;
    let seed = plan.request.sample_seed.unwrap_or(0);
    produce(&mut |array| {
        context.check_cancelled()?;
        add(&mut report.input_rows, array.len() as u64)?;
        for start in (0..array.len()).step_by(block_rows) {
            let end = start.saturating_add(block_rows).min(array.len());
            let array = array.slice(start..end).map_err(super::vortex_error)?;
            let mut batch = NativeBatch::new(&array, &plan.columns, context)?;
            for row in 0..array.len() {
                context.check_cancelled()?;
                if let Some(predicate) = &plan.predicate
                    && !predicate.matches_with(&mut |column| batch.stat(column, row))?
                {
                    continue;
                }
                let ordinal = seen;
                seen = seen
                    .checked_add(1)
                    .ok_or_else(|| failed("sample population overflow"))?;
                let weight = if let Some(column) = plan.weight_index {
                    super::super::sample_weight_value(batch.stat(column, row)?.borrow())?
                } else {
                    1.0
                };
                let score = if plan.weight_index.is_some() {
                    records::ordered_score(super::super::deterministic_weighted_sample_score(
                        seed, ordinal, weight,
                    ))?
                } else {
                    super::super::deterministic_sample_score(seed, ordinal)
                };
                if plan.request.sample_with_replacement {
                    total_weight += weight;
                    if !total_weight.is_finite() {
                        return Err(failed("sample total weight must remain finite"));
                    }
                }
                population.push(
                    Record {
                        ordinal: ordinal as u64,
                        score,
                        cumulative_weight: total_weight.to_bits(),
                        row: batch.row(&plan.output_indices, row, context)?,
                    },
                    context,
                )?;
            }
        }
        context.check_cancelled()
    })?;
    let target = super::super::sample_target_count(&plan.request, seen)?;
    report.population_rows = seen as u64;
    let payload = CompletedRows::streaming_native(
        plan.fields.clone(),
        context.memory(),
        batch_rows,
        context.cancellation().clone(),
        context.float_policy(),
        consume,
    )?;
    let mut output = UnaryOutput {
        columns: &plan.output_columns,
        rows: 0,
        payload: Some(payload),
    };
    let order = population.complete(context)?;
    if plan.request.sample_with_replacement {
        report.lookup_blocks = replacement(
            order,
            &schema,
            plan,
            target,
            seen,
            total_weight,
            block_rows,
            context,
            &mut output,
        )?;
        report.replacement_draws = target as u64;
    } else {
        let mut selected = Ordering::new(&schema.output, state, block_rows, context)?;
        let mut remaining = target;
        order.finish(context, block_rows, &mut |array| {
            let rows = array.len().min(remaining);
            if rows != 0 {
                selected.build(array.slice(0..rows).map_err(super::vortex_error)?, context)?;
                remaining -= rows;
            }
            Ok(())
        })?;
        if remaining != 0 {
            return Err(failed(
                "sampling ordered population ended before its target",
            ));
        }
        selected.finish(context, block_rows, &mut |array| {
            output.emit_native(array.len(), context, || {
                schema.deliver(&array, plan, context)
            })
        })?;
    }
    output.finish(context)?;
    report.output_rows = output.rows as u64;
    context.check_cancelled()?;
    Ok((
        StateUsage {
            items: seen,
            all_input_retained: seen != 0,
        },
        report,
    ))
}

#[allow(clippy::too_many_arguments)] // The finalized population and exact resident draw parameters travel together.
fn replacement(
    order: Ordering<'_, '_>,
    schema: &Schema,
    plan: &BoundUnary,
    target: usize,
    seen: usize,
    total_weight: f64,
    block_rows: usize,
    context: &NativeExecutionContext<'_>,
    output: &mut UnaryOutput<'_, '_>,
) -> Result<u64> {
    let population = order.retain(context)?;
    if population.rows() != seen as u64 {
        return Err(failed("sampling population row count changed"));
    }
    let mut access = access::Access::new(population, schema);
    let seed = plan.request.sample_seed.unwrap_or(0);
    let mut pending = ReservedVec::new(context.memory())?;
    let mut bytes = 0_u64;
    for draw in 0..target {
        context.check_cancelled()?;
        let position = if plan.weight_index.is_some() {
            let threshold =
                super::super::deterministic_sample_unit(seed ^ 0xa076_1d64_78bd_642f, draw)
                    * total_weight;
            access.weighted_index(threshold, context)?
        } else {
            super::super::deterministic_sample_replacement_index(seed, draw, seen) as u64
        };
        let row = access.row(position, context)?;
        let weight = row.retained_bytes()?;
        if pending.values.len() >= block_rows || weight > PENDING_BYTES.saturating_sub(bytes) {
            emit(&mut pending, plan, context, output)?;
            bytes = 0;
        }
        pending.push(row)?;
        bytes = bytes
            .checked_add(weight)
            .ok_or_else(|| failed("sampling draw buffer overflow"))?;
        if bytes >= PENDING_BYTES {
            emit(&mut pending, plan, context, output)?;
            bytes = 0;
        }
    }
    emit(&mut pending, plan, context, output)?;
    access.finish(context)
}

fn emit(
    pending: &mut ReservedVec<values::OwnedRow>,
    plan: &BoundUnary,
    context: &NativeExecutionContext<'_>,
    output: &mut UnaryOutput<'_, '_>,
) -> Result<()> {
    output.emit_retained(
        &plan.fields,
        pending.values.len(),
        context,
        |row| Ok(&pending.values[row]),
        |column| column,
    )?;
    pending.values.clear();
    Ok(())
}
