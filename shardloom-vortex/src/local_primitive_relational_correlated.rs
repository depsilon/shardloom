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
        metrics: &Metrics,
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
                    let mut subquery =
                        native_relational_subquery::Subquery::new(spec, context.memory())?;
                    let mut build = |array| subquery.build(array, context);
                    match relation {
                        SubqueryRelation::Bound(relation) => self.run(
                            relation,
                            context,
                            metrics,
                            batch_rows,
                            Some(&parameter),
                            &mut build,
                        )?,
                        SubqueryRelation::Dynamic(lower) => {
                            let mut preparation = VortexRelationalPreparation {
                                binding: bind::Binder::for_execution(
                                    self,
                                    context,
                                    metrics,
                                    Some(&parameter),
                                )?,
                            };
                            let plan = lower(&mut preparation)?;
                            let root = preparation.binding.bind(&plan, 0)?;
                            preparation.binding.ensure_consumed()?;
                            bind::validate_subquery_relation(spec, &input.fields, &root.fields)?;
                            self.run(
                                &root,
                                context,
                                metrics,
                                batch_rows,
                                Some(&parameter),
                                &mut build,
                            )?;
                        }
                    }
                    values.push(&subquery, &left, row, context)?;
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
        metrics: &Metrics,
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
        let mut subquery = native_relational_subquery::Subquery::new(spec, context.memory())?;
        if spec.guard.is_some()
            || matches!(spec.kind, native_relational_subquery::Kind::Scalar { .. })
        {
            let mut initialized = false;
            return self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let selected = spec.selected_rows(&array, context)?;
                    if !initialized && !selected.values.is_empty() {
                        self.run(
                            relation,
                            context,
                            metrics,
                            batch_rows,
                            parameter,
                            &mut |array| subquery.build(array, context),
                        )?;
                        initialized = true;
                    }
                    subquery.consume_selected(array, &selected.values, context, consume)
                },
            );
        }
        self.run(
            relation,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| subquery.build(array, context),
        )?;
        self.run(
            input,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| subquery.consume(array, context, consume),
        )
    }
}
