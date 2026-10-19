//! Explicit native singleton parameters, with fresh inner operator state per row.

use super::super::native_relational_batch::{Batch, take_batch};
use super::{
    ArrayRef, Metrics, NativeExecutionContext, Node, NodeKind, PreparedVortexRelational, Result,
    SubqueryRelation, VortexRelationalPreparation, bind, failed, native_relational_subquery,
};

impl PreparedVortexRelational {
    pub(super) fn run_subquery(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let NodeKind::Subquery {
            input,
            relation,
            spec,
            parameterized,
        } = &node.kind
        else {
            return Err(failed("subquery dispatch received another operator"));
        };
        if !parameterized {
            return self
                .run_uncorrelated_subquery(node, context, metrics, batch_rows, parameter, consume);
        }
        self.run(
            input,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| {
                let left = Batch::new(array, &spec.left_keys, context)?;
                let selected = spec.selected_rows(&left.array, context)?;
                let mut values = native_relational_subquery::Results::new(
                    spec,
                    left.array.len(),
                    context.memory(),
                )?;
                for &row in &selected.values {
                    context.check_cancelled()?;
                    let parameter = take_batch(&left.array, &input.fields, &[row], context)?;
                    let mut subquery = match relation {
                        SubqueryRelation::Bound(relation) => self.complete_subquery(
                            spec,
                            relation,
                            context,
                            metrics,
                            batch_rows,
                            Some(&parameter),
                        )?,
                        SubqueryRelation::Dynamic(declaration) => {
                            let mut preparation = VortexRelationalPreparation {
                                binding: bind::Binder::for_execution(
                                    self,
                                    context,
                                    metrics,
                                    Some(&parameter),
                                )?,
                            };
                            preparation.binding.restrict_subquery_sources(
                                declaration.source_scope.clone(),
                                declaration.source_metadata.clone(),
                            );
                            let plan = (declaration.lower)(&mut preparation)?;
                            let root = preparation.binding.bind(&plan, 0)?;
                            preparation.binding.ensure_consumed()?;
                            bind::validate_subquery_relation(spec, &input.fields, &root.fields)?;
                            self.complete_subquery(
                                spec,
                                &root,
                                context,
                                metrics,
                                batch_rows,
                                Some(&parameter),
                            )?
                        }
                    };
                    subquery.push_result(&mut values, &left, row, context)?;
                    // The next parameter must not retain this inner reader/run.
                    metrics.record_subquery(&subquery.finish(context)?)?;
                }
                consume(values.finish(spec, &left.array, context)?)?;
                context.check_cancelled()
            },
        )
    }

    fn run_uncorrelated_subquery(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let NodeKind::Subquery {
            input,
            relation: SubqueryRelation::Bound(relation),
            spec,
            parameterized: false,
        } = &node.kind
        else {
            return Err(failed("uncorrelated subquery requires a bound relation"));
        };
        let mut subquery = native_relational_subquery::Ready::empty(spec, context)?;
        if spec.guard.is_some()
            || matches!(spec.kind, native_relational_subquery::Kind::Scalar { .. })
        {
            let mut initialized = false;
            self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let selected = spec.selected_rows(&array, context)?;
                    if !initialized && !selected.values.is_empty() {
                        subquery = self.complete_subquery(
                            spec, relation, context, metrics, batch_rows, parameter,
                        )?;
                        initialized = true;
                    }
                    subquery.consume_selected(array, &selected.values, context, consume)
                },
            )?;
        } else {
            subquery =
                self.complete_subquery(spec, relation, context, metrics, batch_rows, parameter)?;
            self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| subquery.consume(array, context, consume),
            )?;
        }
        metrics.record_subquery(&subquery.finish(context)?)
    }

    #[allow(clippy::too_many_arguments)] // One bound inner node and its existing execution context.
    fn complete_subquery<'a>(
        &self,
        spec: &'a native_relational_subquery::Spec,
        relation: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &'a Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
    ) -> Result<native_relational_subquery::Ready<'a>> {
        #[cfg(feature = "vortex-write")]
        let detaches_keys = metrics.spill.is_some()
            && !matches!(spec.kind, native_relational_subquery::Kind::Scalar { .. });
        #[cfg(not(feature = "vortex-write"))]
        let detaches_keys = false;
        native_relational_subquery::Ready::build(
            spec,
            &relation.fields,
            context,
            batch_rows,
            #[cfg(feature = "vortex-write")]
            metrics.spill.as_ref(),
            |accept| {
                self.run(
                    relation,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        if detaches_keys {
                            // The predicate builder validates all keys and copies
                            // its private key records before releasing this batch.
                            // A full-row copy here would duplicate that ownership
                            // and overlap input replay plus native merge readers.
                            let rows = array.len();
                            accept(array)?;
                            metrics.record_detached_subquery_build(rows)
                        } else {
                            accept(metrics.detach_subquery_build(array, context)?)
                        }
                    },
                )
            },
        )
    }
}
