//! Narrow fully bound operator inputs and normalize safe predicates into the
//! native file/batch scan. Expressions keep their original evaluation order.

use super::{Binder, Node, NodeKind, Result, Side, failed, field};
use crate::local_primitives::{
    self as native,
    native_relational_expression::{Expression as BoundExpression, Kind},
    prepared_relational::{ScanSource, provider},
};
use shardloom_core::{ColumnRef, PredicateExpr};
use shardloom_plan::ProjectionRequest;
use std::collections::{BTreeMap, BTreeSet};

impl Binder<'_> {
    pub(super) fn narrow_join_inputs(
        &mut self,
        left: &mut Node,
        right: &mut Node,
        spec: &native::native_relational_join::Spec,
    ) -> Result<()> {
        for (side, input, keys) in [
            (Side::Left, left, &spec.left_keys),
            (Side::Right, right, &spec.right_keys),
        ] {
            let mut needed = keys
                .iter()
                .map(String::as_str)
                .chain(
                    spec.columns
                        .iter()
                        .chain(
                            spec.condition
                                .iter()
                                .flat_map(|condition| &condition.columns),
                        )
                        .filter_map(|(column_side, name)| {
                            (*column_side == side).then_some(name.as_str())
                        }),
                )
                .peekable();
            // A join may consume only this side's row population. Keep one
            // already-bound input field for native record ownership and spill.
            // Bind it here so filters/windows keep its real dependencies, while
            // computed projections still control their own input demand.
            let row_field = if needed.peek().is_none() && self.scan_accepts_source_demand(input) {
                self.charge_fields(1)?;
                input.fields.first().map(|(name, _)| name.clone())
            } else {
                None
            };
            self.narrow_source_demand(input, needed.chain(row_field.as_deref()))?;
        }
        Ok(())
    }

    pub(super) fn narrow_source_demand<'name>(
        &mut self,
        input: &mut Node,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        if !self.scan_accepts_source_demand(input) {
            return Ok(());
        }
        match &mut input.kind {
            NodeKind::Filter {
                input: child,
                predicate,
            } => {
                // The predicate still executes in the same place on every
                // admitted row. Its hidden columns remain in the child shape.
                self.charge_fields(child.fields.len())?;
                let mut columns: BTreeSet<String> = needed.into_iter().map(str::to_owned).collect();
                predicate.visit_columns(&mut |name| {
                    columns.insert(name.to_owned());
                    Ok(())
                })?;
                self.narrow_source_demand(child, columns.iter().map(String::as_str))?;
                input.fields.clone_from(&child.fields);
            }
            NodeKind::Limit { input: child, .. } => {
                self.narrow_source_demand(child, needed)?;
                input.fields.clone_from(&child.fields);
            }
            NodeKind::Project {
                input: child,
                expressions,
            } => {
                self.charge_fields(input.fields.len())?;
                let mut retained: BTreeSet<String> =
                    needed.into_iter().map(str::to_owned).collect();
                for ((name, _), expression) in input.fields.iter().zip(expressions.iter()) {
                    // Every computed expression retains its original execution
                    // and error order, even when its output is not requested.
                    if !matches!(expression.kind, Kind::Column(_)) {
                        retained.insert(name.clone());
                    }
                }
                let mut index = 0;
                expressions.retain(|_| {
                    let keep = retained.contains(&input.fields[index].0);
                    index += 1;
                    keep
                });
                input.fields.retain(|(name, _)| retained.contains(name));
                BoundExpression::bind_projection_reuse(expressions);
                self.narrow_scan_projection(child, expressions)?;
            }
            NodeKind::Window { input: child, spec } => {
                self.charge_fields(input.fields.len())?;
                let needed: BTreeSet<_> = needed.into_iter().collect();
                let columns = spec.columns.len();
                let mut index = 0;
                spec.fields.retain(|(name, _)| {
                    let keep = index >= columns || needed.contains(name.as_str());
                    index += 1;
                    keep
                });
                spec.columns.retain(|name| needed.contains(name.as_str()));
                input.fields.clone_from(&spec.fields);
                self.narrow_source_demand(child, super::window::input_columns(spec))?;
            }
            NodeKind::Join { left, right, spec } => {
                self.charge_fields(spec.fields.len())?;
                let needed: BTreeSet<_> = needed.into_iter().collect();
                // COUNT(*) still consumes actual matches. Preserve one declared
                // output instead of introducing an empty native row record.
                let keep = |index: usize, name: &str| {
                    needed.contains(name) || (needed.is_empty() && index == 0)
                };
                let mut index = 0;
                spec.columns.retain(|_| {
                    let retain = keep(index, &spec.fields[index].0);
                    index += 1;
                    retain
                });
                let mut index = 0;
                spec.fields.retain(|(name, _)| {
                    let retain = keep(index, name);
                    index += 1;
                    retain
                });
                input.fields.clone_from(&spec.fields);
                self.narrow_join_inputs(left, right, spec)?;
            }
            NodeKind::Scan { .. } => self.narrow_scan_columns(input, needed)?,
            _ => {}
        }
        Ok(())
    }

    fn scan_accepts_source_demand(&self, input: &Node) -> bool {
        match &input.kind {
            NodeKind::Scan {
                source: ScanSource::Batch { index, .. },
                ..
            } => !self.batch_sources.values[*index].external(),
            NodeKind::Filter { input, .. }
            | NodeKind::Limit { input, .. }
            | NodeKind::Project { input, .. }
            | NodeKind::Window { input, .. } => self.scan_accepts_source_demand(input),
            NodeKind::Join { left, right, .. } => {
                self.scan_accepts_source_demand(left) && self.scan_accepts_source_demand(right)
            }
            _ => false,
        }
    }

    pub(super) fn narrow_scan_projection(
        &mut self,
        input: &mut Node,
        expressions: &[BoundExpression],
    ) -> Result<()> {
        // Binding has already checked every expression, including unused or
        // fallible computations. Only narrow its input; do not remove or move
        // the computation across a predicate or range.
        let demanded = self.scan_accepts_source_demand(input);
        if !demanded
            && !matches!(
                input.kind,
                NodeKind::Scan {
                    source: ScanSource::File(_) | ScanSource::Batch { .. },
                    ..
                }
            )
        {
            return Ok(());
        }
        // Prepared files and caller batches keep their existing validation
        // scope. Only owned demanded sources may exclude fields behind a
        // computation; ordinary encoded column views remain unchanged.
        if !demanded
            && expressions
                .iter()
                .any(|expression| !matches!(expression.kind, Kind::Column(_)))
        {
            return Ok(());
        }
        let bytes = (input.fields.len() as u64)
            .checked_mul(4096)
            .ok_or_else(|| failed("scan projection metadata overflow"))?;
        let capacity = self.session.memory().snapshot();
        if bytes > capacity.limit_bytes.saturating_sub(capacity.reserved_bytes) / 2 {
            return Ok(());
        }
        let _scratch = self.session.reserve_input_scratch(bytes)?;
        let mut needed = BTreeSet::new();
        for expression in expressions {
            expression.visit_columns(&mut |name| {
                needed.insert(name.to_owned());
                Ok(())
            })?;
        }
        if demanded {
            self.narrow_source_demand(input, needed.iter().map(String::as_str))
        } else {
            self.narrow_scan_columns(input, needed.iter().map(String::as_str))
        }
    }

    pub(super) fn narrow_scan_columns<'name>(
        &mut self,
        input: &mut Node,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        let NodeKind::Scan {
            source,
            plan,
            columns,
            residual,
            ..
        } = &mut input.kind
        else {
            return Ok(());
        };
        if self.scan_dtype(source).is_none() {
            return Ok(());
        }
        if residual.is_some() || plan.source_order_limit.is_some() {
            return Ok(());
        }
        let bytes = (input.fields.len() as u64)
            .checked_mul(4096)
            .ok_or_else(|| failed("scan projection metadata overflow"))?;
        let capacity = self.session.memory().snapshot();
        if bytes > capacity.limit_bytes.saturating_sub(capacity.reserved_bytes) / 2 {
            return Ok(());
        }
        self.charge(usize::try_from(bytes).map_err(native::vortex_error)?)?;
        let mut projected = Vec::new();
        let mut seen = BTreeSet::new();
        for name in needed {
            if seen.insert(name) {
                projected.push(ColumnRef::new(name)?);
            }
        }
        // COUNT(*) and constant projections still consume the real source row
        // population. Retain one admitted field rather than invent row counts.
        if projected.is_empty() {
            projected.push(ColumnRef::new(
                &input
                    .fields
                    .first()
                    .ok_or_else(|| failed("scan has no row-bearing field"))?
                    .0,
            )?);
        }
        let mut narrowed = native::projection_scan_plan(
            self.scan_dtype(source).expect("supported scan source"),
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
        self.refresh_batch_projection(source, columns, plan)?;
        Ok(())
    }

    pub(super) fn push_scan_filter(
        &mut self,
        input: &mut Node,
        predicate: &BoundExpression,
        predicate_metadata: u64,
    ) -> Result<bool> {
        let Some(width) = alias_scan_width(input) else {
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
                    source,
                    predicate: prior_predicate,
                    plan,
                    residual,
                    columns,
                    ..
                } if residual.is_none() && plan.source_order_limit.is_none() => {
                    let Some(dtype) = self.scan_dtype(source) else {
                        return Ok(false);
                    };
                    let Some(predicate) = provider::scan_predicate(predicate, &aliases, dtype)
                    else {
                        return Ok(false);
                    };
                    let filter = native::predicate_to_vortex_expr(
                        &predicate,
                        dtype,
                        native::VortexQueryPrimitiveKind::FilterAndProject,
                    )?;
                    plan.filter = Some(match plan.filter.take() {
                        Some(prior) => vortex::array::expr::and(prior, filter),
                        None => filter,
                    });
                    *prior_predicate = Some(match prior_predicate.take() {
                        Some(prior) => PredicateExpr::And(vec![prior, predicate]),
                        None => predicate,
                    });
                    self.refresh_batch_projection(source, columns, plan)?;
                    return Ok(true);
                }
                _ => return Ok(false),
            }
        }
    }

    fn scan_dtype(&self, source: &ScanSource) -> Option<&vortex::array::dtype::DType> {
        match source {
            ScanSource::File(index) => Some(self.sources.values[*index].dtype()),
            ScanSource::Batch { index, .. } => {
                Some(self.batch_sources.values[*index].schema.dtype())
            }
            ScanSource::Memory(_) => None,
        }
    }

    fn refresh_batch_projection(
        &self,
        source: &mut ScanSource,
        columns: &[String],
        plan: &native::LocalVortexScanPlan,
    ) -> Result<()> {
        if let ScanSource::Batch { index, projection } = source {
            **projection = self.batch_sources.values[*index]
                .schema
                .prepare_projection(
                    &columns.iter().map(String::as_str).collect::<Vec<_>>(),
                    plan.filter.clone(),
                    None,
                )?;
        }
        Ok(())
    }
}

fn alias_scan_width(node: &Node) -> Option<usize> {
    match &node.kind {
        NodeKind::Project { input, expressions }
            if expressions
                .iter()
                .all(|expression| matches!(expression.kind, Kind::Column(_))) =>
        {
            Some(node.fields.len().max(alias_scan_width(input)?))
        }
        NodeKind::Scan {
            source: ScanSource::File(_) | ScanSource::Batch { .. },
            plan,
            residual,
            ..
        } if residual.is_none() && plan.source_order_limit.is_none() => Some(node.fields.len()),
        _ => None,
    }
}
