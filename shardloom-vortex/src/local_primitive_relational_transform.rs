//! Streaming expression/projection/filter/range and retained native ordering.

use super::super::native_relational_keys::Cell as KeyCell;
use super::*;

impl PreparedVortexRelational {
    pub(super) fn run_transform(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics,
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
            NodeKind::Filter { input, predicate } => self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let predicate = native_relational_expression::keys(
                        &predicate.evaluate(&array, context)?,
                        context,
                    )?;
                    let mut rows = ReservedVec::new(context.memory())?;
                    rows.reserve(array.len())?;
                    for row in 0..array.len() {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        if predicate.cell(row)? == KeyCell::Boolean(true) {
                            rows.values.push(row);
                        }
                    }
                    if rows.values.is_empty() {
                        return Ok(());
                    }
                    consume(select_batch(&array, &node.fields, &rows.values, context)?)
                },
            ),
            NodeKind::Sort { .. } => {
                self.run_sort(node, context, metrics, batch_rows, parameter, consume)
            }
            NodeKind::Limit {
                input,
                offset,
                count,
            } => {
                if *count == 0 {
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
        metrics: &Metrics,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let NodeKind::Sort { input, spec } = &node.kind else {
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
                &mut |array| sort.build(array, context),
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
            &mut |array| sort.build(array, context),
        )?;
        sort.finish(context, batch_rows, consume)
    }
}
