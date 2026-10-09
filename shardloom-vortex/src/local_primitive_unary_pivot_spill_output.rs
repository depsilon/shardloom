//! Late bounded output over sparse native state; no retained dense cell matrix.

use super::super::{
    BATCH_ROWS, BoundUnary, NativeExecutionContext, Plan, Result, SpillReport, Value,
    VortexPivotProjectionRequest, failed, native_payload,
};
use super::{Completed, Kind, Margin, StoredValue, aggregate_at, cell_at, pivot};
use crate::local_primitives::{
    completed_result::CompletedRows, native_capacity::ReservedVec, native_relational_spill::State,
    result_batch,
};
use std::borrow::Cow;
use vortex::array::{ArrayRef, memory::MemorySessionExt as _};

impl Completed {
    pub(in crate::local_primitives::prepared_unary::pivot) fn emit(
        self,
        bound: &BoundUnary,
        state: &State,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<SpillReport> {
        let mut report = SpillReport::default();
        let plan = bound
            .pivot
            .as_ref()
            .ok_or_else(|| failed("pivot is not bound"))?;
        let projection = crate::local_primitives::required_pivot_projection(&bound.request)?;
        let mut output = CompletedRows::streaming_native(
            self.fields.clone(),
            context.memory(),
            batch_rows,
            context.cancellation().clone(),
            consume,
        )?;
        if self.rows == 0 {
            output.push_empty_native(&self.columns, context)?;
            output.finish_stream()?;
        } else {
            let run = self
                .run
                .as_ref()
                .ok_or_else(|| failed("nonempty pivot lost its native state"))?;
            let mut access = run.open(state, &self.schema.spec, context)?;
            let mut indices = ReservedVec::new(context.memory())?;
            indices.reserve(BATCH_ROWS)?;
            let mut position = 0;
            let mut offset = 0;
            let native = self
                .fields
                .iter()
                .any(|(_, dtype)| native_payload::is_nested(dtype));
            while offset < self.rows {
                context.check_cancelled()?;
                let count = (self.rows - offset).min(BATCH_ROWS);
                indices.values.clear();
                for row in 0..count {
                    indices.values.push(if offset + row < self.index_rows {
                        Some(
                            access
                                .next_index(&mut position, context)?
                                .ok_or_else(|| failed("pivot output index prefix is truncated"))?,
                        )
                    } else {
                        None
                    });
                }
                if native {
                    let array =
                        self.native_batch(&mut access, &indices.values, plan, projection, context)?;
                    output.push_native(array, context)?;
                } else {
                    output.push_values(&self.columns, count, |row, column| {
                        self.value(
                            &mut access,
                            indices.values[row],
                            column,
                            plan,
                            projection,
                            context,
                        )
                    })?;
                }
                offset += count;
            }
            output.finish_stream()?;
            access.validate()?;
            report.lookup_blocks = report
                .lookup_blocks
                .checked_add(access.blocks())
                .ok_or_else(|| failed("pivot lookup block count overflow"))?;
            report.reader_opens = report
                .reader_opens
                .checked_add(1)
                .ok_or_else(|| failed("pivot reader count overflow"))?;
        }
        // Access and cache owners are gone before exact owned-file removal.
        if let Some(run) = self.run {
            run.finish(state, context)?;
        }
        Ok(report)
    }

    fn row_margin(
        &self,
        access: &mut pivot::Access<'_, '_>,
        index: u64,
        plan: &Plan,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Margin> {
        let mut margin = Margin::for_kind(Kind::new(plan.decimal_source, plan.nested_extrema));
        for domain in &self.domains {
            let cell = aggregate_at(&self.schema, access, index, domain, context)?;
            margin.push_cell(cell.as_ref(), &plan.aggregate)?;
        }
        Ok(margin)
    }

    #[allow(clippy::too_many_arguments)]
    fn value<'a>(
        &self,
        access: &mut pivot::Access<'_, '_>,
        index: Option<u64>,
        column: usize,
        plan: &'a Plan,
        projection: &'a VortexPivotProjectionRequest,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Value<'a>> {
        context.check_cancelled()?;
        let Some(index) = index else {
            if column == 0 {
                return Ok(Value::Text(Cow::Borrowed(&projection.margins_name)));
            }
            return plan.fill(if column <= self.domains.len() {
                self.column_margins.values[column - 1].value(&plan.aggregate)?
            } else {
                self.grand_margin.value(&plan.aggregate)?
            });
        };
        if column == 0 {
            return self
                .schema
                .scalar(&access.row(index, context)?, true, context);
        }
        if column > self.domains.len() {
            return plan.fill(
                self.row_margin(access, index, plan, context)?
                    .value(&plan.aggregate)?,
            );
        }
        let row = cell_at(access, index, &self.domains[column - 1], context)?;
        let value = if matches!(plan.aggregate.as_str(), "first" | "first_unique") {
            row.as_ref()
                .map(|row| self.schema.scalar(row, false, context))
                .transpose()?
        } else {
            row.as_ref()
                .map(|row| match self.schema.read(row, context)?.value {
                    StoredValue::Aggregate(cell) => cell.value(&plan.aggregate),
                    _ => Err(failed("pivot output selected a non-aggregate cell")),
                })
                .transpose()?
                .flatten()
        };
        plan.fill(value)
    }

    fn native_value(
        &self,
        access: &mut pivot::Access<'_, '_>,
        index: Option<u64>,
        column: usize,
        plan: &Plan,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        context.check_cancelled()?;
        let Some(index) = index else {
            return if column > 0 && column <= self.domains.len() {
                self.column_margins.values[column - 1].native()
            } else if column > self.domains.len() {
                self.grand_margin.native()
            } else {
                Err(failed("pivot margins cannot label a nested index"))
            };
        };
        if column == 0 {
            let entry = self.schema.read(&access.row(index, context)?, context)?;
            return match entry.value {
                StoredValue::Index(value) => value.native().map(Some),
                _ => Err(failed("pivot index selected a cell record")),
            };
        }
        if column > self.domains.len() {
            return self.row_margin(access, index, plan, context)?.native();
        }
        cell_at(access, index, &self.domains[column - 1], context)?
            .map(|row| match self.schema.read(&row, context)?.value {
                StoredValue::First(value) => value.native().map(Some),
                StoredValue::Aggregate(cell) => cell.native(),
                StoredValue::Index(_) => Err(failed("pivot cell selected an index record")),
            })
            .transpose()
            .map(Option::flatten)
    }

    fn native_batch(
        &self,
        access: &mut pivot::Access<'_, '_>,
        indices: &[Option<u64>],
        plan: &Plan,
        projection: &VortexPivotProjectionRequest,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut arrays = ReservedVec::new(context.memory())?;
        arrays.reserve(self.fields.len())?;
        for (column, (_, dtype)) in self.fields.iter().enumerate() {
            context.check_cancelled()?;
            let array = if native_payload::is_nested(dtype) {
                native_payload::retained_column(dtype, indices.len(), context, |row| {
                    self.native_value(access, indices[row], column, plan, context)
                })?
            } else {
                result_batch::build_column(
                    dtype,
                    indices.len(),
                    &context.native_session().allocator(),
                    |row| self.value(access, indices[row], column, plan, projection, context),
                )?
            };
            arrays.values.push(array);
        }
        super::super::super::values::native_struct(&self.fields, indices.len(), arrays)
    }
}
