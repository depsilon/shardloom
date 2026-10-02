//! Ordered source delivery preserves native payload until an operator consumes it.

use super::{
    ArrayRef, DType, LocalVortexScanPlan, MaterializedPredicateEvaluator, Metrics,
    NativeExecutionContext, PreparedVortexSource, ReservedVec, Result, add, failed, select_batch,
    vortex_error,
};
use vortex::io::runtime::BlockingRuntime as _;

const SCAN_ROWS: usize = 8192;

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
            .with_split_by(vortex::layout::scan::split_by::SplitBy::RowCount(SCAN_ROWS));
        if let Some(filter) = &plan.filter {
            scan = scan.with_filter(super::super::bind_vortex_scan_expr(file, filter)?);
        }
        if let Some(projection) = &plan.projection {
            scan = scan.with_projection(super::super::bind_vortex_scan_expr(file, projection)?);
        }
        // Upstream stream concurrency is per host worker. Drive one task at a
        // time instead, so retained order/join state cannot overlap with host-
        // sized speculative reads. Bind once and execute bounded row ranges.
        let split_bytes = file
            .row_count()
            .div_ceil(SCAN_ROWS as u64)
            .checked_add(1)
            .and_then(|splits| splits.checked_mul(16))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| failed("native scan split metadata capacity overflow"))?;
        let _split_metadata = context.memory().reserve(split_bytes)?;
        let scan = scan.prepare().map_err(vortex_error)?;
        for start in (0..file.row_count()).step_by(SCAN_ROWS) {
            context.check_cancelled()?;
            let end = start.saturating_add(SCAN_ROWS as u64).min(file.row_count());
            let mut tasks = scan.execute(Some(start..end)).map_err(vortex_error)?;
            if tasks.len() != 1 {
                return Err(failed("native scan range changed its admitted task count"));
            }
            let task = tasks
                .pop()
                .ok_or_else(|| failed("native scan task is absent"))?;
            let Some(array) = context.runtime().block_on(task).map_err(vortex_error)? else {
                continue;
            };
            if array.len() > SCAN_ROWS {
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
