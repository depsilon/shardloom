//! Expand native list coordinates in source order without an expanded row table.

use super::{
    BATCH_ROWS, BoundUnary, DType, NativeBatch, NativeExecutionContext, ReservedVec, Result,
    UnaryOutput, Value, VortexQueryPrimitiveRequest, failed, vortex_error,
};
use vortex::array::{
    ArrayRef, ExecutionCtx, VortexSessionExecute as _,
    arrays::fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
    arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
    arrays::{FixedSizeListArray, ListViewArray},
    validity::Validity,
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

struct ListColumn {
    elements: ArrayRef,
    validity: Validity,
    offsets: Option<ArrayRef>,
    sizes: Option<ArrayRef>,
    fixed: usize,
}

impl ListColumn {
    fn new(array: ArrayRef, field: Option<&str>, context: &mut ExecutionCtx) -> Result<Self> {
        let mut result = match array.dtype() {
            DType::List(_, _) => {
                let list = array
                    .execute::<ListViewArray>(context)
                    .map_err(vortex_error)?;
                Self {
                    elements: list.elements().clone(),
                    validity: list.listview_validity(),
                    offsets: Some(list.offsets().clone()),
                    sizes: Some(list.sizes().clone()),
                    fixed: 0,
                }
            }
            DType::FixedSizeList(_, _, _) => {
                let list = array
                    .execute::<FixedSizeListArray>(context)
                    .map_err(vortex_error)?;
                Self {
                    elements: list.elements().clone(),
                    validity: list.fixed_size_list_validity(),
                    offsets: None,
                    sizes: None,
                    fixed: list.list_size() as usize,
                }
            }
            _ => return Err(failed("explode source changed its list dtype")),
        };
        if let Some(field) = field {
            result.elements =
                super::super::logical_field_from_native_array(&result.elements, field)?;
        }
        Ok(result)
    }

    fn coordinates(
        &self,
        row: usize,
        context: &mut ExecutionCtx,
    ) -> Result<Option<(usize, usize)>> {
        if !self
            .validity
            .execute_is_valid(row, context)
            .map_err(vortex_error)?
        {
            return Ok(None);
        }
        let index = |array: &ArrayRef, context: &mut ExecutionCtx| -> Result<usize> {
            array
                .execute_scalar(row, context)
                .map_err(vortex_error)?
                .as_primitive()
                .as_::<usize>()
                .ok_or_else(|| failed("list coordinate is not a nonnegative platform index"))
        };
        let (start, count) = match (&self.offsets, &self.sizes) {
            (Some(offsets), Some(sizes)) => (index(offsets, context)?, index(sizes, context)?),
            _ => (
                row.checked_mul(self.fixed)
                    .ok_or_else(|| failed("list offset overflow"))?,
                self.fixed,
            ),
        };
        if start
            .checked_add(count)
            .is_none_or(|end| end > self.elements.len())
        {
            return Err(failed("list coordinates exceed native elements"));
        }
        Ok(Some((start, count)))
    }
}

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
                    ListColumn::new(batch.column(*source)?, field.as_deref(), &mut execution)
                        .map(Some)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mut selected = ReservedVec::new(context.memory())?;
        let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
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
                    emit(
                        compiled,
                        batch,
                        &lists,
                        &mut execution,
                        &mut selected,
                        output,
                    )?;
                    context.check_cancelled()?;
                    if output.rows >= limit {
                        return Ok(true);
                    }
                }
            }
        }
        emit(
            compiled,
            batch,
            &lists,
            &mut execution,
            &mut selected,
            output,
        )?;
        Ok(false)
    }
}

fn emit(
    plan: &Plan,
    batch: &mut NativeBatch,
    lists: &[Option<ListColumn>],
    context: &mut ExecutionCtx,
    selected: &mut ReservedVec<(usize, usize)>,
    output: &mut UnaryOutput<'_, '_>,
) -> Result<()> {
    output.emit(selected.values.len(), |row, column| {
        let (source, element) = selected.values[row];
        match &plan.columns[column] {
            Column::Scalar(index) => batch.value(*index, source),
            Column::List { .. } => {
                let list = lists[column]
                    .as_ref()
                    .ok_or_else(|| failed("native list is absent"))?;
                match list.coordinates(source, context)? {
                    None => Ok(Value::Null),
                    Some((start, _)) => {
                        super::values::scalar_value(&list.elements, start + element, context)
                    }
                }
            }
        }
    })?;
    selected.values.clear();
    Ok(())
}
