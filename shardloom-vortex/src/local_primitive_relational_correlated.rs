//! Explicit native singleton parameters, with fresh inner operator state per row.

use super::super::native_relational_batch::{Batch, take_batch};
use super::{
    ArrayRef, Metrics, NativeExecutionContext, Node, NodeKind, PreparedVortexRelational,
    ReservedVec, Result, SubqueryRelation, VortexRelationalPreparation, bind, failed,
    native_relational_subquery,
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
            let SubqueryRelation::Bound(relation) = relation else {
                return Err(failed("dynamic subquery requires an outer singleton"));
            };
            let mut subquery = native_relational_subquery::Subquery::new(spec, context.memory())?;
            self.run(
                relation,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| subquery.build(array, context),
            )?;
            return self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| subquery.consume(array, context, consume),
            );
        }
        self.run(
            input,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| {
                let left = Batch::new(array, &spec.left_keys, context)?;
                let mut values = ReservedVec::new(context.memory())?;
                values.reserve(left.array.len())?;
                for row in 0..left.array.len() {
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
                    values.values.push(subquery.result(&left, row, context)?);
                }
                consume(spec.output(&left.array, &values.values, context)?)?;
                context.check_cancelled()
            },
        )
    }
}
