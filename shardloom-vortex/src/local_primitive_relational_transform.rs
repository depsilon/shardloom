//! Streaming expression/projection/filter/range and retained native ordering.

use super::super::native_relational_keys::Cell as KeyCell;
use super::{
    ArrayRef, FieldNames, LocalSelection, Metrics, NativeExecutionContext, Node, NodeKind,
    PreparedVortexRelational, ReservedVec, Result, StructArray, Validity, failed,
    native_relational_expression, native_relational_sort, vortex_error,
};

impl PreparedVortexRelational {
    pub(super) fn run_transform(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        match &node.kind {
            NodeKind::Project { input, expressions } => self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let mut columns = ReservedVec::new(context.memory())?;
                    columns.reserve(expressions.len())?;
                    for expression in expressions {
                        columns.values.push(expression.evaluate(&array, context)?);
                    }
                    let (columns, _ownership) = columns.into_parts();
                    let output = StructArray::try_new(
                        node.fields
                            .iter()
                            .map(|(name, _)| name.as_str())
                            .collect::<FieldNames>(),
                        columns,
                        array.len(),
                        Validity::NonNullable,
                    )
                    .map_err(vortex_error)?;
                    consume(vortex::array::IntoArray::into_array(output))
                },
            ),
            NodeKind::Filter { input, predicate } => {
                let recipe = native_relational_expression::FilterRecipe::compile(predicate);
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        let rows = filter_rows(predicate, recipe.as_ref(), &array, context)?;
                        rows.for_each_batch(
                            &array,
                            &node.fields,
                            array.len().max(1),
                            context,
                            consume,
                        )
                    },
                )
            }
            NodeKind::Sort { .. } => {
                self.run_sort(node, context, metrics, batch_rows, parameter, consume)
            }
            NodeKind::Limit {
                input,
                offset,
                count,
            } => {
                // A finite streaming source still owes complete validation,
                // including late errors, even when no rows are requested.
                if *count == 0 && metrics.input.is_none() {
                    return Ok(());
                }
                let mut skip = *offset;
                let mut remaining = *count;
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        let start = skip.min(array.len());
                        skip -= start;
                        let count = remaining.min(array.len() - start);
                        remaining -= count;
                        if count == 0 {
                            return Ok(());
                        }
                        consume(array.slice(start..start + count).map_err(vortex_error)?)
                    },
                )
            }
            _ => Err(failed("transform dispatch received another operator")),
        }
    }

    fn run_sort(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let NodeKind::Sort {
            input,
            spec,
            output_prefix,
        } = &node.kind
        else {
            return Err(failed("sort dispatch received another operator"));
        };
        #[cfg(feature = "vortex-write")]
        if let Some(spill) = &metrics.spill {
            let mut sort = super::super::native_relational_spill::Ordering::new(
                spec, spill, batch_rows, context,
            )?;
            self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| sort.build(metrics.detach_ordering_input(array, context)?, context),
            )?;
            return sort.finish(context, batch_rows, consume);
        }
        let mut sort = native_relational_sort::Sort::new(spec, context.memory())?;
        self.run(
            input,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| sort.build(metrics.detach_ordering_input(array, context)?, context),
        )?;
        sort.finish_prefix(
            context,
            batch_rows,
            *output_prefix,
            metrics.input.is_some(),
            consume,
        )
    }
}

fn filter_rows(
    predicate: &native_relational_expression::Expression,
    recipe: Option<&native_relational_expression::FilterRecipe<'_>>,
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<LocalSelection> {
    if let Some(recipe) = recipe {
        let mask = recipe.evaluate_mask(array, context)?;
        return LocalSelection::from_mask(&mask, context);
    }
    // Keep the existing evaluation and ownership of unadmitted expressions,
    // including lazy/fallible branches and nullable Boolean projections.
    let predicate =
        native_relational_expression::keys(&predicate.evaluate(array, context)?, context)?;
    let mut rows = LocalSelection::new(array.len(), context)?;
    for row in 0..array.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if predicate.cell(row)? == KeyCell::Boolean(true) {
            rows.push(row)?;
        }
    }
    Ok(rows)
}
