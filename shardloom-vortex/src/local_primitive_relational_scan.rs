//! Ordered source delivery preserves native payload until an operator consumes it.

use super::{
    ArrayRef, DType, LocalVortexScanPlan, MaterializedPredicateEvaluator, Metrics,
    NativeExecutionContext, PreparedVortexSource, ReservedVec, Result, ScanSource, add, failed,
    select_batch, vortex_error,
};
use vortex::io::runtime::BlockingRuntime as _;

const SCAN_ROWS: usize = 8192;

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    source: &ScanSource,
    files: &[PreparedVortexSource],
    plan: &LocalVortexScanPlan,
    columns: &[String],
    residual: Option<&MaterializedPredicateEvaluator>,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    batch_rows: usize,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let mut accept = |array: ArrayRef| {
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
        Ok(())
    };
    match source {
        ScanSource::File(index) => run_file(
            &files[*index],
            plan,
            context,
            metrics,
            match metrics.file_scan_batching.get() {
                super::FileScanBatching::Native => SCAN_ROWS,
                super::FileScanBatching::Output => batch_rows.min(SCAN_ROWS),
            },
            &mut accept,
        ),
        ScanSource::Memory(projection) => {
            add(&metrics.scans_started, 1)?;
            if projection.source_rows() == 0 {
                return Ok(());
            }
            add(&metrics.data_scans, 1)?;
            for start in (0..projection.source_rows()).step_by(SCAN_ROWS) {
                let end = start
                    .saturating_add(SCAN_ROWS)
                    .min(projection.source_rows());
                accept(projection.execute_range(start..end, context)?)?;
            }
            Ok(())
        }
        ScanSource::Batch { index, projection } => {
            let input = metrics.input.ok_or_else(|| {
                super::batch_input::failed("streaming scan has no input provider")
            })?;
            input.run(*index, context, metrics, &mut |source| {
                add(&metrics.scans_started, 1)?;
                if source.is_empty() {
                    return accept(projection.execute_batch_array(source, 0..0, context)?);
                }
                add(&metrics.data_scans, 1)?;
                // Producer batches and native scan work units are different
                // boundaries. Slice before projection/filter work so a large
                // admitted columnar input does not expand scan scratch.
                for start in (0..source.len()).step_by(SCAN_ROWS) {
                    let end = start.saturating_add(SCAN_ROWS).min(source.len());
                    accept(projection.execute_batch_array(source, start..end, context)?)?;
                }
                Ok(())
            })
        }
    }
}

fn run_file(
    source: &PreparedVortexSource,
    plan: &LocalVortexScanPlan,
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    scan_rows: usize,
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
        // Execution supplies one bounded range at a time. Retain only the two
        // file endpoints instead of expanding a full-file split vector at the
        // output batch size (which may be a single row).
        let _split_metadata = context.memory().reserve(4096 + 2 * 16)?;
        let mut scan = file
            .scan()
            .map_err(vortex_error)?
            .with_ordered(true)
            .with_concurrency(1)
            .with_natural_splits([0, file.row_count()].into());
        if let Some(filter) = &plan.filter {
            scan = scan.with_filter(super::super::bind_vortex_scan_expr(file, filter)?);
        }
        if let Some(projection) = &plan.projection {
            scan = scan.with_projection(super::super::bind_vortex_scan_expr(file, projection)?);
        }
        // Upstream stream concurrency is per host worker. Drive one task at a
        // time instead, so retained order/join state cannot overlap with host-
        // sized speculative reads. Bind once and execute bounded row ranges.
        // Honor downstream batch demand before reading a wide source, rather
        // than allocating an 8192-row range and only then splitting the output.
        let scan = scan.prepare().map_err(vortex_error)?;
        for start in (0..file.row_count()).step_by(scan_rows) {
            context.check_cancelled()?;
            let end = start.saturating_add(scan_rows as u64).min(file.row_count());
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
            consume(array)?;
        }
        Ok(())
    })
}
