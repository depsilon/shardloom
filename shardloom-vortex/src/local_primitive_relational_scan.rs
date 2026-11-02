//! Ordered source delivery preserves native payload until an operator consumes it.

use super::{
    ArrayRef, DType, LocalSelection, LocalVortexScanPlan, MaterializedPredicateEvaluator, Metrics,
    NativeExecutionContext, PreparedVortexSource, Result, ScanSource, add, failed, vortex_error,
};
use crate::native_work_sizing::{Owner, Rows};
use shardloom_core::native_io_decisions::{
    NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
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
        let mut selection = LocalSelection::new(array.len(), context)?;
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
            selection.push(row)?;
        }
        selection.for_each_batch(&array, fields, batch_rows, context, consume)
    };
    match source {
        ScanSource::File(index) => run_file(
            &files[*index],
            plan,
            fields,
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
            let mut sizing = Rows::from_fields(Owner::Scan, fields, SCAN_ROWS, 2)?;
            let mut start = 0;
            while start < projection.source_rows() {
                let selected = sizing.select((projection.source_rows() - start) as u64, context)?;
                let end = start + selected.rows;
                accept(projection.execute_range(start..end, context)?)?;
                sizing.complete(selected, None, context);
                start = end;
            }
            Ok(())
        }
        ScanSource::Batch { index, projection } => {
            let input = metrics.input.ok_or_else(|| {
                super::batch_input::failed("streaming scan has no input provider")
            })?;
            let mut sizing = Rows::from_fields(Owner::Scan, fields, SCAN_ROWS, 2)?;
            input.run(*index, context, metrics, &mut |source| {
                add(&metrics.scans_started, 1)?;
                if source.is_empty() {
                    return accept(projection.execute_batch_array(source, 0..0, context)?);
                }
                add(&metrics.data_scans, 1)?;
                // Producer batches and native scan work units are different
                // boundaries. Slice before projection/filter work so a large
                // admitted columnar input does not expand scan scratch.
                let mut start = 0;
                while start < source.len() {
                    let selected = sizing.select((source.len() - start) as u64, context)?;
                    let end = start + selected.rows;
                    accept(projection.execute_batch_array(source, start..end, context)?)?;
                    sizing.complete(selected, None, context);
                    start = end;
                }
                Ok(())
            })
        }
    }
}

fn run_file(
    source: &PreparedVortexSource,
    plan: &LocalVortexScanPlan,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    scan_rows: usize,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    source.with_admitted_native_execution(context, |file, context| {
        add(&metrics.scans_started, 1)?;
        if let Some(filter) = &plan.filter {
            let excluded = file.can_prune(filter).map_err(vortex_error)?;
            context.record_capability_decision(
                NativeIoCapability::RegionExclusion,
                if excluded {
                    NativeIoDecisionOutcome::Executed
                } else {
                    NativeIoDecisionOutcome::Declined
                },
                if excluded {
                    NativeIoDecisionReason::ExclusionProved
                } else {
                    NativeIoDecisionReason::ExclusionNotProved
                },
                NativeIoObservedWork {
                    regions: Some(1),
                    excluded_regions: Some(u64::from(excluded)),
                    ..NativeIoObservedWork::default()
                },
            )?;
            if excluded {
                add(&metrics.scans_pruned, 1)?;
                return Ok(());
            }
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
        let mut sizing = Rows::from_fields(Owner::Scan, fields, scan_rows, 2)?;
        let mut start = 0;
        while start < file.row_count() {
            context.check_cancelled()?;
            let selected = sizing.select(file.row_count() - start, context)?;
            let end = start + selected.rows as u64;
            let mut tasks = scan.execute(Some(start..end)).map_err(vortex_error)?;
            if tasks.len() != 1 {
                return Err(failed("native scan range changed its admitted task count"));
            }
            let task = tasks
                .pop()
                .ok_or_else(|| failed("native scan task is absent"))?;
            if let Some(array) = context.runtime().block_on(task).map_err(vortex_error)? {
                consume(array)?;
            }
            sizing.complete(selected, None, context);
            start = end;
        }
        Ok(())
    })
}
