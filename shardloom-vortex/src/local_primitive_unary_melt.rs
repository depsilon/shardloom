//! Row-major native reshape. Mixed scalar values keep their types in Variant;
//! a text or compatibility writer remains a separate, explicit boundary.

use super::{
    BATCH_ROWS, DType, NativeBatch, NativeExecutionContext, Nullability, PreparedVortexUnary,
    ReservedVec, Result, UnaryOutput, Value, VortexQueryPrimitiveRequest, failed,
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
            if !matches!(dtype, DType::Bool(_) | DType::Utf8(_))
                && !matches!(dtype, DType::Primitive(p, _) if *p != PType::F16)
            {
                return Err(failed("melt values require admitted scalar source types"));
            }
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
        plan: &PreparedVortexUnary,
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
                    emit(plan, compiled, batch, &mut selected, output)?;
                    if output.rows >= limit {
                        return Ok(true);
                    }
                }
            }
        }
        emit(plan, compiled, batch, &mut selected, output)?;
        Ok(false)
    }
}

fn emit(
    plan: &PreparedVortexUnary,
    compiled: &Plan,
    batch: &mut NativeBatch,
    selected: &mut ReservedVec<(usize, usize)>,
    output: &mut UnaryOutput<'_, '_>,
) -> Result<()> {
    let request = super::super::required_melt_projection(&plan.request)?;
    output.emit(selected.values.len(), |row, column| {
        let (source, value) = selected.values[row];
        match column.cmp(&compiled.ids.len()) {
            std::cmp::Ordering::Less => batch.value(compiled.ids[column], source),
            std::cmp::Ordering::Equal => Ok(Value::Text(std::borrow::Cow::Borrowed(
                request.value_columns[value].as_str(),
            ))),
            std::cmp::Ordering::Greater => batch.value(compiled.values[value], source),
        }
    })?;
    selected.values.clear();
    Ok(())
}
