//! Schema and ordinal binding happens once, including typed empty results.

use super::{
    DType, LocalVortexScanPlan, Nullability, Result, VortexQueryPrimitiveKind as Kind,
    VortexQueryPrimitiveRequest, expression, failed,
};

pub(super) struct Binding {
    pub(super) columns: Vec<String>,
    pub(super) output_columns: Vec<String>,
    pub(super) output_indices: Vec<usize>,
    pub(super) key_indices: Vec<usize>,
    pub(super) weight_index: Option<usize>,
    pub(super) fields: Vec<(String, DType)>,
    pub(super) expression: Option<expression::Plan>,
    pub(super) melt: Option<super::melt::Plan>,
    pub(super) explode: Option<super::explode::Plan>,
    pub(super) pivot: Option<super::pivot::Plan>,
}

pub(super) fn source_field(dtype: &DType, name: &str) -> Result<DType> {
    if dtype.is_struct() {
        super::super::completed_result::source_field(dtype, name)
    } else {
        Ok(dtype.clone())
    }
}

/// Select the retained provider from schema before executing any rows. Existing
/// nested native providers remain available until their typed state is admitted.
pub(super) fn retained_source_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    plan: &LocalVortexScanPlan,
) -> Result<bool> {
    let columns = if plan.projected_columns.is_empty() {
        super::super::local_field_names(dtype, request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    if columns.is_empty() || columns.len() > 128 {
        return Ok(false);
    }
    let flat = |dtype: &DType| {
        matches!(
            dtype,
            DType::Bool(_) | DType::Utf8(_) | DType::Primitive(_, _)
        ) && !matches!(dtype, DType::Primitive(vortex::array::dtype::PType::F16, _))
    };
    for name in &columns {
        if name.is_empty() || name.len() > 256 {
            return Ok(false);
        }
        let field = source_field(dtype, name)?;
        if request.kind == Kind::ExplodeRows {
            let projection = super::super::required_explode_projection(request)?;
            if projection
                .explode_columns()
                .iter()
                .any(|column| column.as_str() == name)
            {
                let (DType::List(element, _) | DType::FixedSizeList(element, _, _)) = &field else {
                    return Err(failed("explode requires a list or fixed-size-list source"));
                };
                let element = if let Some(name) = &projection.element_field {
                    source_field(element, name)?
                } else {
                    element.as_ref().clone()
                };
                if !flat(&element) {
                    return Ok(false);
                }
                continue;
            }
        }
        if !flat(&field) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn bind(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    plan: &LocalVortexScanPlan,
    memory: &shardloom_exec::live_memory::LiveMemoryPool,
) -> Result<Binding> {
    let columns = if plan.projected_columns.is_empty() {
        super::super::local_field_names(dtype, request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let selected = match request.kind {
        Kind::DropDuplicateRows => super::super::drop_duplicate_output_columns(dtype, request)?,
        Kind::SampleRows => super::super::sample_output_columns(dtype, request)?,
        _ => plan
            .output_columns
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| columns.clone()),
    };
    let keys = if request.kind == Kind::DropDuplicateRows {
        super::super::drop_duplicate_key_columns(dtype, request)?
    } else {
        selected.clone()
    };
    let position = |name: &str| {
        columns
            .iter()
            .position(|column| column == name)
            .ok_or_else(|| failed("required column is absent from native projection"))
    };
    let output_indices = selected
        .iter()
        .map(|name| position(name))
        .collect::<Result<Vec<_>>>()?;
    let key_indices = keys
        .iter()
        .map(|name| position(name))
        .collect::<Result<Vec<_>>>()?;
    let weight_index = request
        .sample_weight_column
        .as_ref()
        .map(|name| position(name.as_str()))
        .transpose()?;
    let expression = if request.kind == Kind::ExpressionProjectRows {
        Some(expression::Plan::bind(
            request, dtype, &columns, &selected, memory,
        )?)
    } else {
        None
    };
    let melt = if request.kind == Kind::MeltRows {
        Some(super::melt::Plan::bind(request, dtype, &columns)?)
    } else {
        None
    };
    let explode = if request.kind == Kind::ExplodeRows {
        Some(super::explode::Plan::bind(
            request, dtype, &columns, &selected,
        )?)
    } else {
        None
    };
    let pivot = if request.kind == Kind::PivotRows {
        Some(super::pivot::Plan::bind(request, dtype, &columns)?)
    } else {
        None
    };
    let fields = if let Some(pivot) = &pivot {
        vec![pivot.index_field.clone()]
    } else if let Some(explode) = &explode {
        explode.fields.clone()
    } else if let Some(melt) = &melt {
        melt.fields.clone()
    } else if let Some(expression) = &expression {
        expression.fields.clone()
    } else {
        fixed_fields(request, dtype, selected)?
    };
    let output_columns = fields.iter().map(|(name, _)| name.clone()).collect();
    Ok(Binding {
        columns,
        output_columns,
        output_indices,
        key_indices,
        weight_index,
        fields,
        expression,
        melt,
        explode,
        pivot,
    })
}

fn fixed_fields(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    selected: Vec<String>,
) -> Result<Vec<(String, DType)>> {
    let columns = match request.kind {
        Kind::RollingWindowRows => rolling_columns(request, dtype, &selected)?,
        Kind::DuplicateMaskRows => vec!["duplicated".into()],
        _ => selected,
    };
    columns
        .into_iter()
        .map(|name| {
            let field = match request.kind {
                Kind::RollingWindowRows => super::rolling::dtype(request)?,
                Kind::DuplicateMaskRows => DType::Bool(Nullability::NonNullable),
                _ => source_field(dtype, &name)?,
            };
            Ok((name, field))
        })
        .collect()
}

fn rolling_columns(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    selected: &[String],
) -> Result<Vec<String>> {
    let rolling = super::super::required_rolling_window(request)?;
    if selected.len() != 1 || selected[0] != rolling.source_column.as_str() {
        return Err(failed("rolling requires its declared source column"));
    }
    let input = source_field(dtype, &selected[0])?;
    if rolling.aggregate != "count"
        && !matches!(input, DType::Primitive(p, _) if p != vortex::array::dtype::PType::F16)
    {
        return Err(failed("rolling numeric aggregate requires numeric input"));
    }
    Ok(rolling.output_columns())
}
