//! Immutable aggregate rewrite/projection lowering, independent of a runtime.
//! Fresh scans still bind provider expressions and consult file statistics.

use super::{
    AggregateEmbeddedDerivedRewrite, LocalVortexScanPlan, PredicateExpr, Result,
    VortexQueryPrimitiveRequest, append_predicate_columns, predicate_to_vortex_expr,
    projection_scan_plan, required_simple_aggregate,
    rewrite_simple_aggregate_for_embedded_derived_columns, split_predicate_for_vortex_pushdown,
};
use shardloom_plan::ProjectionRequest;
use vortex::array::dtype::DType;

pub(super) struct AggregateLowering {
    pub(super) rewrite: AggregateEmbeddedDerivedRewrite,
    pub(super) plan: LocalVortexScanPlan,
    pub(super) pushdown: Option<PredicateExpr>,
    pub(super) residual: Option<PredicateExpr>,
}

impl AggregateLowering {
    pub(super) fn new(request: &VortexQueryPrimitiveRequest, dtype: &DType) -> Result<Self> {
        let rewrite = rewrite_simple_aggregate_for_embedded_derived_columns(
            dtype,
            required_simple_aggregate(request)?,
            request.predicate.as_ref(),
        )?;
        let mut projected_columns = rewrite.aggregate.projected_columns();
        let (pushdown, residual) = rewrite
            .predicate
            .as_ref()
            .map_or((None, None), |predicate| {
                split_predicate_for_vortex_pushdown(predicate, request.kind)
            });
        if let Some(predicate) = &residual {
            append_predicate_columns(predicate, &mut projected_columns);
        }
        let projection = ProjectionRequest::columns(projected_columns);
        let mut plan = projection_scan_plan(dtype, &projection, request.kind)?;
        if let Some(predicate) = &pushdown {
            plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
        }
        Ok(Self {
            rewrite,
            plan,
            pushdown,
            residual,
        })
    }
}
