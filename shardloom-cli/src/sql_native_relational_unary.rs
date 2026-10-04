//! Bind frontend column spellings to the preceding relation's visible fields.

use super::{ColumnRef, Lowered, NativeResult, ProjectionRequest};
use shardloom_core::PredicateExpr;
use shardloom_vortex::{VortexExpressionRewrite as Rewrite, VortexQueryPrimitiveRequest};

pub(super) fn resolve(
    mut request: VortexQueryPrimitiveRequest,
    input: &Lowered,
) -> NativeResult<VortexQueryPrimitiveRequest> {
    projection(&mut request.projection, input)?;
    if let Some(keys) = &mut request.deduplicate_key_projection {
        projection(keys, input)?;
    }
    if let Some(weight) = &mut request.sample_weight_column {
        column(weight, input)?;
    }
    if let Some(melt) = &mut request.melt_projection {
        for name in melt.id_columns.iter_mut().chain(&mut melt.value_columns) {
            column(name, input)?;
        }
    }
    if let Some(rolling) = &mut request.rolling_window {
        column(&mut rolling.source_column, input)?;
    }
    if let Some(pivot) = &mut request.pivot_projection {
        column(&mut pivot.index_column, input)?;
        column(&mut pivot.pivot_column, input)?;
        column(&mut pivot.value_column, input)?;
    }
    if let Some(explode) = &mut request.explode_projection {
        column(&mut explode.column, input)?;
        for name in &mut explode.columns {
            column(name, input)?;
        }
    }
    if let Some(expressions) = &mut request.expression_projection {
        for rewrite in &mut expressions.rewrites {
            match rewrite {
                Rewrite::RowNumber { .. } => {} // This declares an output name.
                Rewrite::MaskScalar {
                    target_column,
                    predicate: filter,
                    ..
                } => {
                    column(target_column, input)?;
                    predicate(filter, input)?;
                }
                Rewrite::ReplaceScalar { target_column, .. }
                | Rewrite::StringReplaceScalar { target_column, .. }
                | Rewrite::RegexReplaceScalar { target_column, .. }
                | Rewrite::NumericScalarArithmetic { target_column, .. }
                | Rewrite::ForwardFillNull { target_column, .. } => column(target_column, input)?,
            }
        }
    }
    Ok(request)
}

fn projection(projection: &mut ProjectionRequest, input: &Lowered) -> NativeResult<()> {
    if let ProjectionRequest::Columns(columns) = projection {
        for name in columns {
            column(name, input)?;
        }
    }
    Ok(())
}

fn column(name: &mut ColumnRef, input: &Lowered) -> NativeResult<()> {
    *name = ColumnRef::new(input.resolve(name.as_str())?)?;
    Ok(())
}

fn predicate(value: &mut PredicateExpr, input: &Lowered) -> NativeResult<()> {
    match value {
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => {}
        PredicateExpr::And(children) => {
            for child in children {
                predicate(child, input)?;
            }
        }
        PredicateExpr::IsNull { column: name }
        | PredicateExpr::IsNotNull { column: name }
        | PredicateExpr::Compare { column: name, .. }
        | PredicateExpr::StringContains { column: name, .. }
        | PredicateExpr::InList { column: name, .. } => column(name, input)?,
    }
    Ok(())
}
