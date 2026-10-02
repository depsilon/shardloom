//! Explicit native singleton parameters, with fresh inner operator state per row.

use super::super::native_relational_batch::{Batch, take_batch};
use super::*;

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
                    self.run(
                        relation,
                        context,
                        metrics,
                        batch_rows,
                        Some(&parameter),
                        &mut |array| subquery.build(array, context),
                    )?;
                    values.values.push(subquery.result(&left, row, context)?);
                }
                consume(spec.output(&left.array, &values.values, context)?)?;
                context.check_cancelled()
            },
        )
    }
}
