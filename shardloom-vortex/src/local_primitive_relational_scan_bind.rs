//! Narrow fully bound operator inputs and normalize safe predicates into the
//! native file/batch scan. Expressions keep their original evaluation order.

use super::super::{PipelineKind, PipelineStage, SubqueryRelation};
use super::{Binder, DType, Node, NodeKind, Result, Side, failed, field};
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
            return self.narrow_validated_join_payload(input, needed);
        }
        match &mut input.kind {
            NodeKind::Filter {
                input: child,
                predicate,
            } => {
                self.narrow_filter_demand(child, predicate, needed)?;
                input.fields.clone_from(&child.fields);
            }
            NodeKind::Limit { input: child, .. } => {
                self.narrow_source_demand(child, needed)?;
                input.fields.clone_from(&child.fields);
            }
            NodeKind::Sort {
                input: child, spec, ..
            } => {
                self.narrow_sort_demand(child, spec, needed)?;
                input.fields.clone_from(&child.fields);
            }
            NodeKind::Unary {
                input: child,
                operation,
            } => {
                self.narrow_unary_demand(child, operation, needed)?;
                input.fields.clear();
                input.fields.extend_from_slice(operation.fields());
            }
            NodeKind::Project {
                input: child,
                expressions,
            } => {
                self.charge_named_fields(&input.fields)?;
                retain_projection(
                    &mut input.fields,
                    expressions,
                    needed.into_iter().map(str::to_owned).collect(),
                );
                self.narrow_scan_projection(child, expressions)?;
            }
            NodeKind::Pipeline {
                input: child,
                stages,
            } => {
                self.narrow_pipeline_demand(child, stages, needed)?;
                input
                    .fields
                    .clone_from(stages.last().map_or(&child.fields, |stage| &stage.fields));
            }
            NodeKind::Window { input: child, spec } => {
                self.charge_named_fields(&input.fields)?;
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
                self.charge_named_fields(&spec.fields)?;
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
            NodeKind::Subquery {
                input: outer,
                relation: SubqueryRelation::Bound(inner),
                spec,
                ..
            } => {
                self.narrow_membership_inputs(outer, inner, spec, needed)?;
                input.fields.clone_from(&spec.fields);
            }
            NodeKind::Scan { .. } => self.narrow_scan_columns(input, needed)?,
            _ => {}
        }
        Ok(())
    }

    fn narrow_filter_demand<'name>(
        &mut self,
        input: &mut Node,
        predicate: &BoundExpression,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        // The predicate still executes in the same place on every admitted
        // row. Its hidden columns remain in the child shape.
        self.charge_named_fields(&input.fields)?;
        let mut columns: BTreeSet<String> = needed.into_iter().map(str::to_owned).collect();
        predicate.visit_columns(&mut |name| {
            columns.insert(name.to_owned());
            Ok(())
        })?;
        self.narrow_source_demand(input, columns.iter().map(String::as_str))
    }

    fn narrow_unary_demand<'name>(
        &mut self,
        input: &mut Node,
        operation: &mut native::prepared_unary::BoundUnary,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        self.charge_named_fields(operation.fields())?;
        let needed: BTreeSet<_> = needed.into_iter().collect();
        let Some(narrowed) = operation.narrow_explode_output(
            &needed,
            &DType::struct_(
                input.fields.clone(),
                vortex::array::dtype::Nullability::NonNullable,
            ),
            self.schema_scope(),
        )?
        else {
            return Ok(());
        };
        self.narrow_source_demand(input, narrowed.input_columns().iter().map(String::as_str))?;
        *operation = narrowed;
        Ok(())
    }

    fn narrow_sort_demand<'name>(
        &mut self,
        input: &mut Node,
        spec: &mut native::native_relational_sort::Spec,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        // Ordering needs every key and its complete row population, but an
        // owned demanded source need not read unused payload. Child projections
        // retain computed expressions, including their observable errors.
        self.charge_named_fields(&input.fields)?;
        let columns: BTreeSet<String> = needed
            .into_iter()
            .map(str::to_owned)
            .chain(spec.names.iter().cloned())
            .collect();
        self.narrow_source_demand(input, columns.iter().map(String::as_str))?;
        spec.unobserved_fields.clear();
        for (index, (name, _)) in input.fields.iter().enumerate() {
            if input.carries_unobserved(name) {
                spec.unobserved_fields.push(index);
            }
        }
        spec.fields.clone_from(&input.fields);
        Ok(())
    }

    fn narrow_pipeline_demand<'name>(
        &mut self,
        input: &mut Node,
        stages: &mut [PipelineStage],
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        // Apply the same projection/filter/range rules in reverse dataflow
        // order, without allocating temporary nodes for a flat declaration.
        self.charge_named_fields(stages.last().map_or(&input.fields, |stage| &stage.fields))?;
        let mut needed: BTreeSet<String> = needed.into_iter().map(str::to_owned).collect();
        if stages
            .last()
            .map_or(&input.fields, |stage| &stage.fields)
            .iter()
            .all(|(name, _)| needed.contains(name))
        {
            // Earlier binding already propagated this complete demand. Merely
            // renaming the same fields must not recharge every previous stage.
            return Ok(());
        }
        for stage in stages.iter_mut().rev() {
            self.cancellation.check()?;
            self.charge_named_fields(&stage.fields)?;
            match &mut stage.kind {
                PipelineKind::Project(expressions) => {
                    retain_projection(&mut stage.fields, expressions, needed);
                    needed = BTreeSet::new();
                    for expression in expressions {
                        expression.visit_columns(&mut |name| {
                            needed.insert(name.to_owned());
                            Ok(())
                        })?;
                    }
                }
                PipelineKind::Filter(predicate) => {
                    predicate.visit_columns(&mut |name| {
                        needed.insert(name.to_owned());
                        Ok(())
                    })?;
                }
                PipelineKind::Limit { .. } => {}
                PipelineKind::Observe(_) | PipelineKind::ApplicationBatch(_) => {
                    unreachable!("source-demand admission excludes effect/completion barriers")
                }
            }
        }
        self.narrow_source_demand(input, needed.iter().map(String::as_str))?;
        // Pass-through stages use the narrowed upstream schema. Project stages
        // already retain their own aliases and all computed outputs.
        let mut fields = &input.fields;
        for stage in stages {
            if !matches!(stage.kind, PipelineKind::Project(_)) {
                stage.fields.clone_from(fields);
            }
            fields = &stage.fields;
        }
        Ok(())
    }

    fn narrow_validated_join_payload<'name>(
        &mut self,
        input: &mut Node,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        let NodeKind::Join { spec, .. } = &mut input.kind else {
            return Ok(());
        };
        self.charge_named_fields(&spec.fields)?;
        let needed: BTreeSet<_> = needed.into_iter().collect();
        let first = (0..spec.fields.len()).find(|&index| spec.retains(index));
        let retained = spec
            .fields
            .iter()
            .enumerate()
            .map(|(index, (name, dtype))| {
                Ok(spec.retains(index)
                    && (needed.contains(name.as_str())
                        || (needed.is_empty() && first == Some(index))
                        || !crate::native_payload_schema::supported(dtype, self.schema_scope())?))
            })
            .collect::<Result<Vec<_>>>()?;
        if retained.iter().all(|retained| *retained) {
            return Ok(());
        }
        input.fields = spec
            .fields
            .iter()
            .zip(&retained)
            .filter_map(|(field, &retained)| retained.then_some(field.clone()))
            .collect();
        spec.retained = Some(retained);
        // Input scans, match population and column visitation stay with their
        // original owners. Only validated selected output crosses this mask.
        Ok(())
    }

    fn narrow_membership_inputs<'name>(
        &mut self,
        outer: &mut Node,
        inner: &mut Node,
        spec: &mut native::native_relational_subquery::Spec,
        needed: impl IntoIterator<Item = &'name str>,
    ) -> Result<()> {
        // The caller admits only bound, owned inputs. Both batch keys and
        // guard expressions address logical fields by name. The final
        // subquery field is synthesized after probing, never scanned.
        self.charge_named_fields(&spec.fields)?;
        let marker = spec
            .fields
            .last()
            .ok_or_else(|| failed("missing subquery result field"))?
            .clone();
        let mut columns: BTreeSet<String> = needed
            .into_iter()
            .filter(|name| *name != marker.0)
            .map(str::to_owned)
            .chain(spec.left_keys.iter().cloned())
            .collect();
        if let Some(guard) = &spec.guard {
            guard.visit_columns(&mut |name| {
                columns.insert(name.to_owned());
                Ok(())
            })?;
        }
        self.narrow_source_demand(outer, columns.iter().map(String::as_str))?;
        self.narrow_source_demand(inner, spec.right_keys.iter().map(String::as_str))?;
        spec.fields.clone_from(&outer.fields);
        spec.fields.push(marker);
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
            | NodeKind::Sort { input, .. }
            | NodeKind::Window { input, .. } => self.scan_accepts_source_demand(input),
            NodeKind::Unary { input, operation } if operation.accepts_output_demand() => {
                self.scan_accepts_source_demand(input)
            }
            NodeKind::Pipeline { input, stages } => {
                stages.iter().all(|stage| {
                    matches!(
                        stage.kind,
                        PipelineKind::Project(_)
                            | PipelineKind::Filter(_)
                            | PipelineKind::Limit { .. }
                    )
                }) && self.scan_accepts_source_demand(input)
            }
            NodeKind::Join { left, right, .. } => {
                self.scan_accepts_source_demand(left) && self.scan_accepts_source_demand(right)
            }
            NodeKind::Subquery {
                input: outer,
                relation: SubqueryRelation::Bound(inner),
                spec,
                parameterized: false,
                initialize_on_demand: false,
            } if matches!(spec.kind, native::native_relational_subquery::Kind::In) => {
                self.scan_accepts_source_demand(outer) && self.scan_accepts_source_demand(inner)
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
                } | NodeKind::Join { .. }
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
        if demanded || matches!(input.kind, NodeKind::Join { .. }) {
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
            predicate,
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
        // population. Prefer a field already read by the predicate, retaining
        // its nullable values as a row carrier rather than counting them.
        let mut hidden_carrier = false;
        if projected.is_empty() {
            let owned = matches!(source, ScanSource::Batch { index, .. }
                if !self.batch_sources.values[*index].external());
            let name = input
                .fields
                .iter()
                .find(|(name, _)| {
                    predicate
                        .as_ref()
                        .is_some_and(|predicate| predicate_uses_column(predicate, name))
                })
                .map(|(name, _)| name.as_str())
                .or_else(|| {
                    owned
                        .then(|| predicate.as_ref().and_then(first_predicate_column))
                        .flatten()
                })
                .or_else(|| input.fields.first().map(|(name, _)| name.as_str()))
                .ok_or_else(|| failed("scan has no row-bearing field"))?;
            hidden_carrier = !input.fields.iter().any(|(field, _)| field == name);
            projected.push(ColumnRef::new(name)?);
        }
        let mut narrowed = native::projection_scan_plan(
            self.scan_dtype(source).expect("supported scan source"),
            &ProjectionRequest::Columns(projected),
            native::VortexQueryPrimitiveKind::ProjectColumns,
        )?;
        let fields = narrowed
            .projected_columns
            .iter()
            .map(|name| {
                let dtype = if hidden_carrier {
                    // The original declaration and predicate were already
                    // validated. Only an owned source's value-free consumer
                    // can expose this required predicate field as its carrier.
                    native::completed_result::source_field(
                        self.scan_dtype(source).expect("supported scan source"),
                        name,
                    )?
                } else {
                    field(&input.fields, name)?.clone()
                };
                Ok((name.clone(), dtype))
            })
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
                    if !remap_aliases(&node.fields, expressions, &mut aliases) {
                        return Ok(false);
                    }
                    node = input;
                }
                NodeKind::Pipeline { input, stages } => {
                    for stage in stages.iter().rev() {
                        let PipelineKind::Project(expressions) = &stage.kind else {
                            return Ok(false);
                        };
                        if !remap_aliases(&stage.fields, expressions, &mut aliases) {
                            return Ok(false);
                        }
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
                    // Keep existing physical rewrites, while using Vortex's
                    // conjunct extraction and balanced builder. Flat programs
                    // must not create an unbounded recursive AND chain. The
                    // original predicates' binding credits cover these borrowed
                    // expression handles and the overlapping balanced tree.
                    plan.filter = Some(match plan.filter.take() {
                        Some(prior) => vortex::array::expr::and_collect(
                            vortex::expr::forms::conjuncts(&prior)
                                .into_iter()
                                .chain(std::iter::once(filter)),
                        )
                        .expect("a new filter is always present"),
                        None => filter,
                    });
                    *prior_predicate = Some(provider::conjoin(prior_predicate.take(), predicate));
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

fn predicate_uses_column(predicate: &PredicateExpr, name: &str) -> bool {
    predicate
        .column()
        .is_some_and(|column| column.as_str() == name)
        || matches!(predicate, PredicateExpr::And(children) if children.iter().any(|child| predicate_uses_column(child, name)))
}

fn first_predicate_column(predicate: &PredicateExpr) -> Option<&str> {
    predicate
        .column()
        .map(ColumnRef::as_str)
        .or_else(|| match predicate {
            PredicateExpr::And(children) => children.iter().find_map(first_predicate_column),
            _ => None,
        })
}

fn retain_projection(
    fields: &mut Vec<(String, DType)>,
    expressions: &mut Vec<BoundExpression>,
    mut retained: BTreeSet<String>,
) {
    // Every computed expression retains its original execution and error
    // order, even when its output is not requested by the consumer.
    for ((name, _), expression) in fields.iter().zip(expressions.iter()) {
        if !matches!(expression.kind, Kind::Column(_)) {
            retained.insert(name.clone());
        }
    }
    let mut index = 0;
    expressions.retain(|_| {
        let keep = retained.contains(&fields[index].0);
        index += 1;
        keep
    });
    fields.retain(|(name, _)| retained.contains(name));
    BoundExpression::bind_projection_reuse(expressions);
}

fn remap_aliases(
    fields: &[(String, DType)],
    expressions: &[BoundExpression],
    aliases: &mut BTreeMap<String, String>,
) -> bool {
    let mut columns = BTreeMap::new();
    for ((name, _), expression) in fields.iter().zip(expressions) {
        let Kind::Column(source) = &expression.kind else {
            return false;
        };
        columns.insert(name.as_str(), source.as_str());
    }
    for name in aliases.values_mut() {
        let Some(source) = columns.get(name.as_str()) else {
            return false;
        };
        name.clear();
        name.push_str(source);
    }
    true
}

fn alias_scan_width(mut node: &Node) -> Option<usize> {
    let mut width = 0;
    loop {
        width = width.max(node.fields.len());
        node = match &node.kind {
            NodeKind::Project { input, expressions }
                if super::super::transform::column_projection(expressions) =>
            {
                input
            }
            NodeKind::Pipeline { input, stages } => {
                for stage in stages {
                    let PipelineKind::Project(expressions) = &stage.kind else {
                        return None;
                    };
                    if !super::super::transform::column_projection(expressions) {
                        return None;
                    }
                    width = width.max(stage.fields.len());
                }
                input
            }
            NodeKind::Scan {
                source: ScanSource::File(_) | ScanSource::Batch { .. },
                plan,
                residual,
                ..
            } if residual.is_none() && plan.source_order_limit.is_none() => return Some(width),
            _ => return None,
        };
    }
}
