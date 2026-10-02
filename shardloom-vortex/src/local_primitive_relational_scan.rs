//! Ordered source delivery preserves native payload until an operator consumes it.

use super::{
    ArrayRef, DType, LocalVortexScanPlan, MaterializedPredicateEvaluator, Metrics,
    NativeExecutionContext, PreparedVortexSource, ReservedVec, Result, add, failed, select_batch,
    vortex_error,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    source: &PreparedVortexSource,
    plan: &LocalVortexScanPlan,
    columns: &[String],
    residual: Option<&MaterializedPredicateEvaluator>,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics,
    batch_rows: usize,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    source.with_admitted_native_execution(context, |file, context| {
        add(&metrics.scans_started, 1)?;
        if plan
            .filter
            .as_ref()
            .map(|filter| file.can_prune(filter).map_err(vortex_error))
            .transpose()?
            .unwrap_or(false)
        {
            add(&metrics.scans_pruned, 1)?;
            return Ok(());
        }
        if file.row_count() == 0 {
            return Ok(());
        }
        add(&metrics.data_scans, 1)?;
        let mut scan = file
            .scan()
            .map_err(vortex_error)?
            .with_ordered(true)
            .with_concurrency(1)
            .with_split_by(vortex::layout::scan::split_by::SplitBy::RowCount(8192));
        if let Some(filter) = &plan.filter {
            scan = scan.with_filter(super::super::bind_vortex_scan_expr(file, filter)?);
        }
        if let Some(projection) = &plan.projection {
            scan = scan.with_projection(super::super::bind_vortex_scan_expr(file, projection)?);
        }
        for array in scan
            .into_array_iter(context.runtime())
            .map_err(vortex_error)?
        {
            context.check_cancelled()?;
            let array = array.map_err(vortex_error)?;
            if array.len() > 8192 {
                return Err(failed("scan exceeded its admitted batch size"));
            }
            add(&metrics.scan_rows, array.len() as u64)?;
            add(&metrics.scan_batches, 1)?;
            let mut selection = ReservedVec::new(context.memory())?;
            selection.reserve(array.len())?;
            let mut values = residual
                .map(|_| {
                    add(&metrics.residual_batches, 1)?;
                    super::super::prepared_unary::values::NativeBatch::new(&array, columns, context)
                })
                .transpose()?;
            for row in 0..array.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if let (Some(predicate), Some(values)) = (residual, values.as_mut())
                    && !predicate.matches_with(&mut |column| values.stat(column, row))?
                {
                    continue;
                }
                selection.values.push(row);
            }
            for rows in selection.values.chunks(batch_rows) {
                consume(select_batch(&array, fields, rows, context)?)?;
            }
        }
        Ok(())
    })
}
