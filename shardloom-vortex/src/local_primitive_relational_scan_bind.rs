//! Normalize fully validated, infallible file projections and predicates into
//! the existing Vortex scan. Computation, range and residual barriers remain.

use super::{Binder, Node, NodeKind, Result, failed, field};
use crate::local_primitives::{
    self as native,
    native_relational_expression::{Expression as BoundExpression, Kind},
    prepared_relational::{ScanSource, provider},
};
use shardloom_core::{ColumnRef, Expression};
use shardloom_plan::ProjectionRequest;
use std::collections::{BTreeMap, BTreeSet};

impl Binder<'_> {
    pub(super) fn narrow_file_projection(
        &mut self,
        input: &mut Node,
        expressions: &[BoundExpression],
    ) -> Result<()> {
        let NodeKind::Scan {
            source: ScanSource::File(index),
            plan,
            columns,
            residual,
        } = &mut input.kind
        else {
            return Ok(());
        };
        if residual.is_some()
            || plan.source_order_limit.is_some()
            || expressions
                .iter()
                .any(|expression| !matches!(expression.kind, Kind::Column(_)))
        {
            return Ok(());
        }
        let bytes = (expressions.len() as u64)
            .checked_mul(4096)
            .ok_or_else(|| failed("scan projection metadata overflow"))?;
        let capacity = self.session.memory().snapshot();
        if bytes > capacity.limit_bytes.saturating_sub(capacity.reserved_bytes) / 2 {
            return Ok(());
        }
        self.charge(usize::try_from(bytes).map_err(native::vortex_error)?)?;
        let mut projected = Vec::new();
        let mut seen = BTreeSet::new();
        for expression in expressions {
            let Kind::Column(name) = &expression.kind else {
                unreachable!("checked column-only projection")
            };
            if seen.insert(name.as_str()) {
                projected.push(ColumnRef::new(name)?);
            }
        }
        let mut narrowed = native::projection_scan_plan(
            self.sources.values[*index].dtype(),
            &ProjectionRequest::Columns(projected),
            native::VortexQueryPrimitiveKind::ProjectColumns,
        )?;
        let fields = narrowed
            .projected_columns
            .iter()
            .map(|name| Ok((name.clone(), field(&input.fields, name)?.clone())))
            .collect::<Result<Vec<_>>>()?;
        narrowed.filter = plan.filter.take();
        narrowed.embedded_derived_column_rewrites =
            std::mem::take(&mut plan.embedded_derived_column_rewrites);
        columns.clone_from(&narrowed.projected_columns);
        *plan = narrowed;
        input.fields = fields;
        Ok(())
    }

    pub(super) fn push_file_filter(
        &mut self,
        input: &mut Node,
        predicate: &Expression,
        predicate_metadata: u64,
    ) -> Result<bool> {
        let Some(width) = alias_file_scan_width(input) else {
            return Ok(false);
        };
        // Reserve the lowered predicate before cloning it, including literal
        // bytes already counted by the ordinary expression binder. Low
        // headroom retains the fully bound general evaluator.
        let bytes = predicate_metadata
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add((width as u64).checked_mul(4096)?))
            .ok_or_else(|| failed("scan predicate metadata overflow"))?;
        let capacity = self.session.memory().snapshot();
        if bytes > capacity.limit_bytes.saturating_sub(capacity.reserved_bytes) / 2 {
            return Ok(false);
        }
        self.charge(usize::try_from(bytes).map_err(native::vortex_error)?)?;
        let mut aliases: BTreeMap<_, _> = input
            .fields
            .iter()
            .map(|(name, _)| (name.clone(), name.clone()))
            .collect();
        let mut node = input;
        loop {
            match &mut node.kind {
                NodeKind::Project { input, expressions } => {
                    // Even an unused computed column may fail. Only traverse
                    // encoded column views, never a computed projection.
                    if expressions
                        .iter()
                        .any(|expr| !matches!(expr.kind, Kind::Column(_)))
                    {
                        return Ok(false);
                    }
                    let columns = node
                        .fields
                        .iter()
                        .zip(expressions.iter())
                        .map(|((name, _), expression)| {
                            let Kind::Column(source) = &expression.kind else {
                                unreachable!("checked column-only projection")
                            };
                            (name.as_str(), source.as_str())
                        })
                        .collect::<BTreeMap<_, _>>();
                    for name in aliases.values_mut() {
                        let Some(source) = columns.get(name.as_str()) else {
                            return Ok(false);
                        };
                        name.clear();
                        name.push_str(source);
                    }
                    node = input;
                }
                NodeKind::Scan {
                    source: ScanSource::File(index),
                    plan,
                    residual,
                    ..
                } if residual.is_none() && plan.source_order_limit.is_none() => {
                    let dtype = self.sources.values[*index].dtype();
                    let Some(predicate) = provider::scan_predicate(predicate, &aliases, dtype)
                    else {
                        return Ok(false);
                    };
                    let filter = native::predicate_to_vortex_expr(
                        &predicate,
                        self.sources.values[*index].dtype(),
                        native::VortexQueryPrimitiveKind::FilterAndProject,
                    )?;
                    plan.filter = Some(match plan.filter.take() {
                        Some(prior) => vortex::array::expr::and(prior, filter),
                        None => filter,
                    });
                    return Ok(true);
                }
                _ => return Ok(false),
            }
        }
    }
}

fn alias_file_scan_width(node: &Node) -> Option<usize> {
    match &node.kind {
        NodeKind::Project { input, expressions }
            if expressions
                .iter()
                .all(|expression| matches!(expression.kind, Kind::Column(_))) =>
        {
            Some(node.fields.len().max(alias_file_scan_width(input)?))
        }
        NodeKind::Scan {
            source: ScanSource::File(_),
            plan,
            residual,
            ..
        } if residual.is_none() && plan.source_order_limit.is_none() => Some(node.fields.len()),
        _ => None,
    }
}
