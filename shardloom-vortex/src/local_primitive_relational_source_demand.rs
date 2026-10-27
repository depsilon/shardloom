//! Derive physical source demand from the shared, fully bound native tree.

use super::super::{ScanSource, SubqueryRelation};
use super::{Binder, Node, NodeKind, Result, failed};
use shardloom_core::ColumnRef;
use shardloom_plan::{ProjectionRequest, SourceDemand};

impl Binder<'_> {
    pub(in crate::local_primitives::prepared_relational) fn apply_source_demand(
        &mut self,
        root: &mut Node,
    ) -> Result<()> {
        if !self
            .batch_sources
            .values
            .iter()
            .any(|source| source.local.is_some())
        {
            return Ok(());
        }
        // Temporary copies are bounded by the admitted plan, including literals.
        // Physical descriptors retain only column/region selections afterwards.
        let _scratch = self.session.reserve_input_scratch(self.metadata.bytes())?;
        let mut demands: Vec<_> = self
            .batch_sources
            .values
            .iter()
            .map(|_| SourceDemand {
                projection: ProjectionRequest::Columns(Vec::new()),
                predicates: Vec::new(),
            })
            .collect();
        let complete = scans(root, &mut |source, columns, predicate, _| {
            if let ScanSource::Batch { index, .. } = source {
                let demand = &mut demands[*index];
                let ProjectionRequest::Columns(wanted) = &mut demand.projection else {
                    unreachable!()
                };
                for name in columns {
                    if !wanted.iter().any(|column| column.as_str() == name) {
                        wanted.push(ColumnRef::new(name)?);
                    }
                }
                demand.predicates.push(predicate.clone());
            }
            Ok(())
        })?;
        // Deferred/correlated discovery may introduce additional requirements.
        // Preserve complete admitted source schemas until that work is known.
        if !complete {
            return Ok(());
        }
        for (source, demand) in self.batch_sources.values.iter_mut().zip(demands) {
            if let Some(local) = &source.local {
                self.cancellation.check()?;
                if demand.predicates.is_empty() {
                    return Err(failed(
                        "declared local source is not referenced by the bound plan",
                    ));
                }
                let local = local
                    .source
                    .prepare(&demand, self.session, &self.cancellation)?;
                source.schema = local.empty_schema(self.session, &self.cancellation)?;
                source.local = Some(local);
            }
        }
        scans(root, &mut |source, columns, predicate, plan| {
            if let ScanSource::Batch { index, projection } = source {
                // Bound Vortex expressions encode source field positions. A
                // narrowed physical schema requires fresh binding, not just a
                // smaller payload underneath the original expression.
                plan.filter = predicate
                    .as_ref()
                    .map(|predicate| {
                        crate::local_primitives::predicate_to_vortex_expr(
                            predicate,
                            self.batch_sources.values[*index].schema.dtype(),
                            crate::local_primitives::VortexQueryPrimitiveKind::FilterAndProject,
                        )
                    })
                    .transpose()?;
                **projection = self.batch_sources.values[*index]
                    .schema
                    .prepare_projection(
                        &columns.iter().map(String::as_str).collect::<Vec<_>>(),
                        plan.filter.clone(),
                        None,
                    )?;
            }
            Ok(())
        })?;
        Ok(())
    }
}

fn scans(
    node: &mut Node,
    visit: &mut impl FnMut(
        &mut ScanSource,
        &[String],
        &Option<shardloom_core::PredicateExpr>,
        &mut super::super::LocalVortexScanPlan,
    ) -> Result<()>,
) -> Result<bool> {
    match &mut node.kind {
        NodeKind::Scan {
            source,
            columns,
            predicate,
            plan,
            ..
        } => {
            visit(source, columns, predicate, plan)?;
            Ok(true)
        }
        NodeKind::Pipeline { input, .. }
        | NodeKind::Unary { input, .. }
        | NodeKind::Aggregate { input, .. }
        | NodeKind::Project { input, .. }
        | NodeKind::Filter { input, .. }
        | NodeKind::Sort { input, .. }
        | NodeKind::Limit { input, .. }
        | NodeKind::Window { input, .. } => scans(input, visit),
        NodeKind::Join { left, right, .. } | NodeKind::Set { left, right, .. } => {
            let left = scans(left, visit)?;
            let right = scans(right, visit)?;
            Ok(left && right)
        }
        NodeKind::Subquery {
            input, relation, ..
        } => {
            let input = scans(input, visit)?;
            let relation = match relation {
                SubqueryRelation::Bound(node) => scans(node, visit)?,
                SubqueryRelation::Dynamic(_) => false,
            };
            Ok(input && relation)
        }
        NodeKind::CompletedPivot { .. } | NodeKind::Outer => Ok(true),
    }
}
