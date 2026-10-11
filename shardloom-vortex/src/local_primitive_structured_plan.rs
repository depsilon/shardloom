//! Structured primitive declarations use the existing native expression plan.
//! Vortex arrays and every writer retain the admitted source's session.

use super::{CancellationToken, PreparedVortexSource, Result, VortexLocalPrimitiveExecutionPolicy};
use crate::{
    VortexStructuredProjectionExpr,
    local_primitives::prepared_relational::{
        PreparedVortexRelational, prepare_relational_from_source_controlled,
    },
    query_primitive::VortexQueryPrimitiveRequest,
    relational_query::{
        VortexRelationalLimit, VortexRelationalPlan, VortexRelationalProject, VortexRelationalScan,
    },
};
use shardloom_core::{ExprId, Expression, ExpressionKind};

pub(super) fn annotate_report(
    request: &VortexQueryPrimitiveRequest,
    source_rows: u64,
    report: &mut crate::local_primitives::VortexLocalPrimitiveRowExportReport,
) -> Result<()> {
    report.primitive_kind = request.kind;
    report.source_order_limit_requested = request
        .source_order_limit
        .map(super::super::usize_to_u64)
        .transpose()?;
    report.evidence.pushdown.source_order_limit_applied = request.source_order_limit.is_some();
    // Projection preserves cardinality. The retained footer proves an
    // unfiltered pre-limit count; filtered early termination proves a lower
    // bound only, without a second scan to manufacture an exact observation.
    report.pre_limit_result_row_count = if request.predicate.is_none() {
        source_rows
    } else {
        report.rows_written
    };
    if let Some(evidence) = &mut report.evidence.native_array_sink {
        evidence.pre_limit_result_row_count = report.pre_limit_result_row_count;
        evidence.pre_limit_result_row_count_exact = request.predicate.is_none()
            || report
                .source_order_limit_requested
                .is_none_or(|limit| report.rows_written < limit);
    }
    Ok(())
}

pub(super) fn prepare(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    cancellation.check()?;
    let uri = request
        .source_uri
        .clone()
        .ok_or_else(|| super::failed("structured source URI is absent"))?;
    prepare_relational_from_source_controlled(
        uri.clone(),
        source,
        policy,
        |_| {
            plan(
                request,
                VortexRelationalPlan::Scan(VortexRelationalScan {
                    source_uri: uri,
                    projection: request.projection.clone(),
                    predicate: request.predicate.clone(),
                }),
            )
        },
        cancellation,
    )
}

pub(super) fn plan(
    request: &VortexQueryPrimitiveRequest,
    input: VortexRelationalPlan,
) -> Result<VortexRelationalPlan> {
    let projection = request
        .structured_projection
        .as_ref()
        .ok_or_else(|| super::failed("structured projection is absent"))?;
    if projection.is_empty() || projection.source_columns().is_empty() {
        return Err(super::failed(
            "structured projection requires output columns and a referenced native source column",
        ));
    }
    if request.source_order_limit == Some(0)
        || request
            .expression_projection
            .as_ref()
            .is_some_and(|value| !value.is_empty())
        || request.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            ) || diagnostic.fallback.attempted
        })
    {
        return Err(super::failed(
            "structured projection parameters are not admitted",
        ));
    }
    let input = match request.source_order_limit {
        None => input,
        Some(count) => VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input,
            offset: 0,
            count,
        })),
    };
    let expressions = projection
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Ok((
                column.output_column.clone(),
                expression(index, &column.expr)?,
            ))
        })
        .collect::<Result<_>>()?;
    Ok(VortexRelationalPlan::Project(Box::new(
        VortexRelationalProject { input, expressions },
    )))
}

fn expression(index: usize, value: &VortexStructuredProjectionExpr) -> Result<Expression> {
    let id = ExprId::new(format!("structured-{index}"))?;
    Ok(match value {
        VortexStructuredProjectionExpr::SourceColumn(column) => {
            Expression::column(id, column.clone())
        }
        VortexStructuredProjectionExpr::ArrayLiteral(values) => Expression::new(
            id,
            ExpressionKind::List {
                values: values
                    .iter()
                    .enumerate()
                    .map(|(item, value)| {
                        Ok(Expression::literal(
                            ExprId::new(format!("structured-{index}-{item}"))?,
                            value.clone(),
                        ))
                    })
                    .collect::<Result<_>>()?,
            },
        ),
        VortexStructuredProjectionExpr::StructColumns(columns) => Expression::new(
            id,
            ExpressionKind::Struct {
                fields: columns
                    .iter()
                    .enumerate()
                    .map(|(item, column)| {
                        Ok((
                            column.as_str().to_owned(),
                            Expression::column(
                                ExprId::new(format!("structured-{index}-{item}"))?,
                                column.clone(),
                            ),
                        ))
                    })
                    .collect::<Result<_>>()?,
            },
        ),
    })
}
