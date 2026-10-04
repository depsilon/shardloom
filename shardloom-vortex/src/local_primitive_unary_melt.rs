//! Row-major native reshape. Mixed scalar values keep their types in Variant;
//! a text or compatibility writer remains a separate, explicit boundary.

use super::{
    BATCH_ROWS, BoundUnary, DType, NativeBatch, NativeExecutionContext, Nullability, ReservedVec,
    Result, UnaryOutput, Value, VortexQueryPrimitiveRequest, failed,
};
use vortex::array::dtype::PType;

pub(super) struct Plan {
    pub(super) fields: Vec<(String, DType)>,
    ids: Vec<usize>,
    values: Vec<usize>,
}

impl Plan {
    pub(super) fn bind(
        request: &VortexQueryPrimitiveRequest,
        source: &DType,
        columns: &[String],
    ) -> Result<Self> {
        let melt = super::super::required_melt_projection(request)?;
        let position = |name: &str| {
            columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| failed("melt source column is absent from projection"))
        };
        let ids = melt
            .id_columns
            .iter()
            .map(|name| position(name.as_str()))
            .collect::<Result<Vec<_>>>()?;
        let values = melt
            .value_columns
            .iter()
            .map(|name| position(name.as_str()))
            .collect::<Result<Vec<_>>>()?;
        let mut fields = melt
            .id_columns
            .iter()
            .map(|name| {
                Ok((
                    name.as_str().to_owned(),
                    super::schema::source_field(source, name.as_str())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let dtypes = melt
            .value_columns
            .iter()
            .map(|name| super::schema::source_field(source, name.as_str()))
            .collect::<Result<Vec<_>>>()?;
        for dtype in &dtypes {
            super::super::native_payload::metadata_bytes(dtype)?;
        }
        fields.push((
            melt.variable_column.clone(),
            DType::Utf8(Nullability::NonNullable),
        ));
        fields.push((melt.value_column.clone(), common_dtype(&dtypes)?));
        Ok(Self {
            fields,
            ids,
            values,
        })
    }
}

pub(super) fn common_dtype(dtypes: &[DType]) -> Result<DType> {
    let first = dtypes
        .first()
        .ok_or_else(|| failed("melt has no value types"))?;
    let nullability = if dtypes.iter().any(DType::is_nullable) {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };
    if dtypes
        .iter()
        .all(|dtype| dtype.as_nonnullable() == first.as_nonnullable())
    {
        return Ok(first.with_nullability(nullability));
    }
    if dtypes
        .iter()
        .any(|dtype| matches!(dtype, DType::Decimal(..))) && dtypes.iter().all(|dtype| {
        matches!(dtype, DType::Decimal(..))
            || matches!(dtype, DType::Primitive(p, _) if p.is_signed_int() || p.is_unsigned_int())
    }) {
        let decimal = super::super::native_relational_expression::scalar::decimal_operand;
        let mut common = first.clone();
        for dtype in &dtypes[1..] {
            let (precision, scale) = decimal(0, &common)?.common_type(decimal(0, dtype)?)?;
            common = DType::Decimal(
                vortex::array::dtype::DecimalDType::new(
                    precision,
                    i8::try_from(scale).map_err(super::vortex_error)?,
                ),
                nullability,
            );
        }
        return Ok(common);
    }
    if dtypes.iter().any(|dtype| {
        super::super::native_payload::is_nested(dtype)
            || matches!(
                dtype,
                DType::Binary(_) | DType::Decimal(..) | DType::Extension(_)
            )
    }) {
        return Err(failed(
            "melt requires one lossless common scalar domain; typed values cannot be mixed with storage integers or unrelated types",
        ));
    }
    let signed = dtypes
        .iter()
        .all(|dtype| matches!(dtype, DType::Primitive(p, _) if p.is_signed_int()));
    let unsigned = dtypes
        .iter()
        .all(|dtype| matches!(dtype, DType::Primitive(p, _) if p.is_unsigned_int()));
    let float = dtypes
        .iter()
        .all(|dtype| matches!(dtype, DType::Primitive(PType::F32 | PType::F64, _)));
    Ok(if signed {
        DType::Primitive(PType::I64, nullability)
    } else if unsigned {
        DType::Primitive(PType::U64, nullability)
    } else if float {
        DType::Primitive(PType::F64, nullability)
    } else {
        DType::Variant(nullability)
    })
}

#[derive(Default)]
pub(super) struct Melt {
    pub(super) produced: usize,
}

impl Melt {
    pub(super) fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        let compiled = plan
            .melt
            .as_ref()
            .ok_or_else(|| failed("melt is not bound"))?;
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
            self.produced = self
                .produced
                .checked_add(compiled.values.len())
                .ok_or_else(|| failed("melt row count overflow"))?;
            for value in 0..compiled.values.len() {
                selected.push((row, value))?;
                if selected.values.len() == BATCH_ROWS
                    || output.rows.saturating_add(selected.values.len()) >= limit
                {
                    emit(plan, compiled, batch, &mut selected, context, output)?;
                    if output.rows >= limit {
                        return Ok(true);
                    }
                }
            }
        }
        emit(plan, compiled, batch, &mut selected, context, output)?;
        Ok(false)
    }
}

fn emit(
    plan: &BoundUnary,
    compiled: &Plan,
    batch: &mut NativeBatch,
    selected: &mut ReservedVec<(usize, usize)>,
    context: &NativeExecutionContext<'_>,
    output: &mut UnaryOutput<'_, '_>,
) -> Result<()> {
    let request = super::super::required_melt_projection(&plan.request)?;
    if compiled
        .fields
        .iter()
        .any(|(_, dtype)| super::super::native_payload::is_nested(dtype))
    {
        output.emit_native(selected.values.len(), context, || {
            native_melt(
                compiled,
                batch,
                &selected.values,
                &request.value_columns,
                context,
            )
        })?;
        selected.values.clear();
        return Ok(());
    }
    output.emit(selected.values.len(), |row, column| {
        let (source, value) = selected.values[row];
        match column.cmp(&compiled.ids.len()) {
            std::cmp::Ordering::Less => batch.value(compiled.ids[column], source),
            std::cmp::Ordering::Equal => Ok(Value::Text(std::borrow::Cow::Borrowed(
                request.value_columns[value].as_str(),
            ))),
            std::cmp::Ordering::Greater => {
                let value = batch.value(compiled.values[value], source)?;
                let target = &compiled
                    .fields
                    .last()
                    .ok_or_else(|| failed("melt value type is absent"))?
                    .1;
                super::values::common_value(value, target)
            }
        }
    })?;
    selected.values.clear();
    Ok(())
}

fn native_melt(
    compiled: &Plan,
    batch: &mut NativeBatch,
    selected: &[(usize, usize)],
    names: &[shardloom_core::ColumnRef],
    context: &NativeExecutionContext<'_>,
) -> Result<vortex::array::ArrayRef> {
    use super::super::{
        native_relational_batch::{index_array, take_column},
        result_batch,
    };
    use vortex::array::{
        IntoArray as _,
        arrays::{ChunkedArray, StructArray},
        builtins::ArrayBuiltins as _,
        dtype::FieldNames,
        memory::MemorySessionExt as _,
        validity::Validity,
    };
    let rows = selected.len();
    let indices = index_array(rows, false, context, |row| Ok(Some(selected[row].0)))?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(compiled.fields.len())?;
    for (index, &column) in compiled.ids.iter().enumerate() {
        context.check_cancelled()?;
        columns.values.push(take_column(
            &batch.column(column)?,
            &indices,
            &compiled.fields[index].1,
            context,
        )?);
    }
    columns.values.push(result_batch::build_column(
        &DType::Utf8(Nullability::NonNullable),
        rows,
        &context.native_session().allocator(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            Ok(Value::Text(std::borrow::Cow::Borrowed(
                names[selected[row].1].as_str(),
            )))
        },
    )?);
    let dtype = &compiled
        .fields
        .last()
        .ok_or_else(|| failed("melt output dtype absent"))?
        .1;
    let value = if super::super::native_payload::is_nested(dtype) {
        let mut arrays = ReservedVec::new(context.memory())?;
        arrays.reserve(compiled.values.len())?;
        let source_rows = batch.column(compiled.values[0])?.len();
        for &column in &compiled.values {
            context.check_cancelled()?;
            let array = batch.column(column)?;
            if array.len() != source_rows {
                return Err(failed("melt source columns changed row count"));
            }
            arrays
                .values
                .push(array.cast(dtype.clone()).map_err(super::vortex_error)?);
        }
        let (arrays, _ownership) = arrays.into_parts();
        let array = ChunkedArray::try_new(arrays, dtype.clone())
            .map_err(super::vortex_error)?
            .into_array();
        let indices = index_array(rows, false, context, |row| {
            let (source, value) = selected[row];
            value
                .checked_mul(source_rows)
                .and_then(|index| index.checked_add(source))
                .map(Some)
                .ok_or_else(|| failed("melt source ordinal overflow"))
        })?;
        take_column(&array, &indices, dtype, context)?
    } else {
        result_batch::build_column(dtype, rows, &context.native_session().allocator(), |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let (source, value) = selected[row];
            super::values::common_value(batch.value(compiled.values[value], source)?, dtype)
        })?
    };
    columns.values.push(value);
    let (columns, _ownership) = columns.into_parts();
    StructArray::try_new(
        compiled
            .fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        rows,
        Validity::NonNullable,
    )
    .map(vortex::array::IntoArray::into_array)
    .map_err(super::vortex_error)
}
