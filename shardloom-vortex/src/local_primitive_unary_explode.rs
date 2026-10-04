//! Expand native list coordinates in source order without an expanded row table.

use super::super::native_relational_batch::{index_array, take_column};
use super::{
    BATCH_ROWS, BoundUnary, DType, NativeBatch, NativeExecutionContext, ReservedVec, Result,
    UnaryOutput, VortexQueryPrimitiveRequest, failed, vortex_error,
};
use vortex::array::{
    VortexSessionExecute as _, arrays::StructArray, dtype::FieldNames, validity::Validity,
};

pub(super) struct Plan {
    pub(super) fields: Vec<(String, DType)>,
    columns: Vec<Column>,
}

enum Column {
    Scalar(usize),
    List {
        source: usize,
        field: Option<String>,
    },
}

impl Plan {
    pub(super) fn bind(
        request: &VortexQueryPrimitiveRequest,
        dtype: &DType,
        columns: &[String],
        selected: &[String],
    ) -> Result<Self> {
        let projection = super::super::required_explode_projection(request)?;
        let explode = projection.explode_columns();
        if explode.is_empty() || (projection.element_field.is_some() && explode.len() != 1) {
            return Err(failed(
                "explode element-field projection requires exactly one list",
            ));
        }
        for (index, name) in explode.iter().enumerate() {
            if explode[..index].contains(name)
                || !selected.iter().any(|column| column == name.as_str())
            {
                return Err(failed("explode requires distinct selected list columns"));
            }
        }
        let names = projection.output_columns(selected);
        let mut output = Vec::with_capacity(selected.len());
        let mut fields = Vec::with_capacity(selected.len());
        for (name, output_name) in selected.iter().zip(names) {
            let source = columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| failed("explode source column is absent"))?;
            let source_dtype = super::schema::source_field(dtype, name)?;
            let output_dtype = if explode.iter().any(|column| column.as_str() == name) {
                let (element, nullable) = match &source_dtype {
                    DType::List(element, nullability)
                    | DType::FixedSizeList(element, _, nullability) => (
                        element.as_ref(),
                        *nullability == super::Nullability::Nullable,
                    ),
                    _ => return Err(failed("explode requires a list or fixed-size-list source")),
                };
                let element = if let Some(field) = &projection.element_field {
                    super::schema::source_field(element, field)?
                } else {
                    element.clone()
                };
                output.push(Column::List {
                    source,
                    field: projection.element_field.clone(),
                });
                if nullable {
                    element.as_nullable()
                } else {
                    element
                }
            } else {
                output.push(Column::Scalar(source));
                source_dtype
            };
            fields.push((output_name, output_dtype));
        }
        Ok(Self {
            fields,
            columns: output,
        })
    }
}

use super::super::native_list::Column as ListColumn;

#[derive(Default)]
pub(super) struct Explode {
    pub(super) produced: usize,
}

impl Explode {
    pub(super) fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        let compiled = plan
            .explode
            .as_ref()
            .ok_or_else(|| failed("explode is not bound"))?;
        let mut execution = context.native_session().create_execution_ctx();
        let _metadata = context
            .memory()
            .reserve(compiled.columns.len() as u64 * 2048)?;
        let lists = compiled
            .columns
            .iter()
            .map(|column| match column {
                Column::Scalar(_) => Ok(None),
                Column::List { source, field } => {
                    let mut list = ListColumn::new(batch.column(*source)?, &mut execution)?;
                    if let Some(field) = field {
                        list.elements =
                            super::super::logical_field_from_native_array(&list.elements, field)?;
                    }
                    Ok(Some(list))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mut selected = ReservedVec::new(context.memory())?;
        let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
        if output.rows >= limit {
            return Ok(true);
        }
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            let mut count = None;
            for list in lists.iter().flatten() {
                let length = list
                    .coordinates(row, &mut execution)?
                    .map_or(1, |(_, count)| count);
                if count.is_some_and(|count| count != length) {
                    return Err(failed(
                        "multi-column explode requires equal list lengths per selected source row",
                    ));
                }
                count = Some(length);
            }
            let count = count.ok_or_else(|| failed("explode has no list column"))?;
            self.produced = self
                .produced
                .checked_add(count)
                .ok_or_else(|| failed("explode row count overflow"))?;
            for element in 0..count {
                selected.push((row, element))?;
                if selected.values.len() == BATCH_ROWS
                    || output.rows.saturating_add(selected.values.len()) >= limit
                {
                    emit(compiled, batch, &lists, context, &mut selected, output)?;
                    context.check_cancelled()?;
                    if output.rows >= limit {
                        return Ok(true);
                    }
                }
            }
        }
        emit(compiled, batch, &lists, context, &mut selected, output)?;
        Ok(false)
    }
}

fn emit(
    plan: &Plan,
    batch: &mut NativeBatch,
    lists: &[Option<ListColumn>],
    context: &NativeExecutionContext<'_>,
    selected: &mut ReservedVec<(usize, usize)>,
    output: &mut UnaryOutput<'_, '_>,
) -> Result<()> {
    let rows = selected.values.len();
    output.emit_native(rows, context, || {
        let parent = index_array(rows, false, context, |row| Ok(Some(selected.values[row].0)))?;
        let mut execution = context.native_session().create_execution_ctx();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(plan.columns.len())?;
        for (index, (column, (_, dtype))) in plan.columns.iter().zip(&plan.fields).enumerate() {
            context.check_cancelled()?;
            let array = match column {
                Column::Scalar(source) => {
                    take_column(&batch.column(*source)?, &parent, dtype, context)?
                }
                Column::List { .. } => {
                    let list = lists[index]
                        .as_ref()
                        .ok_or_else(|| failed("native list is absent"))?;
                    let indices = index_array(rows, dtype.is_nullable(), context, |row| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        let (source, element) = selected.values[row];
                        Ok(list
                            .coordinates(source, &mut execution)?
                            .map(|(start, _)| start + element))
                    })?;
                    take_column(&list.elements, &indices, dtype, context)?
                }
            };
            columns.values.push(array);
        }
        let (columns, _ownership) = columns.into_parts();
        StructArray::try_new(
            plan.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            rows,
            Validity::NonNullable,
        )
        .map(vortex::array::IntoArray::into_array)
        .map_err(vortex_error)
    })?;
    selected.values.clear();
    Ok(())
}
