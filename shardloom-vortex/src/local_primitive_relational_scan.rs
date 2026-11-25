//! Ordered source delivery preserves native payload until an operator consumes it.

use super::super::native_relational_probe_filter::{
    ProbeRuntimeFilter, apply_bloom_mask, is_infallible_key_dtype,
};
use super::{
    ArrayRef, DType, LocalSelection, LocalVortexScanPlan, MaterializedPredicateEvaluator, Metrics,
    NativeExecutionContext, PreparedVortexSource, Result, ScanSource, add, failed, vortex_error,
};
use crate::native_work_sizing::{Owner, Rows};
use shardloom_core::{
    PredicateExpr,
    native_io_decisions::{
        NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
    },
};
use vortex::{io::runtime::BlockingRuntime as _, mask::Mask};

const SCAN_ROWS: usize = 8192;

pub(super) fn has_infallible_empty_build(
    probe_filters: &[ProbeRuntimeFilter],
    field_dtype: &mut impl FnMut(&str) -> Option<DType>,
) -> bool {
    probe_filters.iter().any(|filter| {
        filter.empty_build
            && !filter.probe_columns.is_empty()
            && filter
                .probe_columns
                .iter()
                .all(|name| field_dtype(name).is_some_and(|dtype| is_infallible_key_dtype(&dtype)))
    })
}

pub(super) fn admitted_runtime_predicate(
    probe_filters: &[ProbeRuntimeFilter],
    source_dtype: &DType,
) -> Option<PredicateExpr> {
    let mut combined: Option<PredicateExpr> = None;
    for filter in probe_filters {
        let Some(pred) = &filter.key_predicate else {
            continue;
        };
        if super::provider::predicate_preserves_typed_semantics(pred, source_dtype)
            && super::super::predicate_to_vortex_expr(
                pred,
                source_dtype,
                super::super::VortexQueryPrimitiveKind::FilterAndProject,
            )
            .is_ok()
        {
            combined = Some(super::provider::conjoin(combined, pred.clone()));
        }
    }
    combined
}

fn plan_with_runtime_predicate(
    plan: &LocalVortexScanPlan,
    predicate: PredicateExpr,
    source_dtype: &DType,
) -> Result<LocalVortexScanPlan> {
    let expr = super::super::predicate_to_vortex_expr(
        &predicate,
        source_dtype,
        super::super::VortexQueryPrimitiveKind::FilterAndProject,
    )?;
    let mut updated = plan.clone();
    updated.filter = Some(match updated.filter.take() {
        Some(prior) => vortex::array::expr::and_collect(
            vortex::expr::forms::conjuncts(&prior)
                .into_iter()
                .chain(std::iter::once(expr)),
        )
        .expect("runtime filter conjunction always contains an expression"),
        None => expr,
    });
    updated.pushdown_predicate = Some(super::provider::conjoin(
        updated.pushdown_predicate.take(),
        predicate,
    ));
    Ok(updated)
}

#[allow(clippy::too_many_arguments)]
fn accept_scan_batch(
    array: &ArrayRef,
    columns: &[String],
    residual: Option<&MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    batch_rows: usize,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    if array.len() > SCAN_ROWS {
        return Err(failed("scan exceeded its admitted batch size"));
    }
    add(&metrics.scan_rows, array.len() as u64)?;
    add(&metrics.scan_batches, 1)?;
    let bloom_mask = if probe_filters.is_empty() {
        None
    } else {
        apply_bloom_mask(array, probe_filters, context)?
    };
    if bloom_mask.as_ref().is_some_and(Mask::all_false) {
        return Ok(());
    }
    if residual.is_none() {
        if let Some(mask) = bloom_mask {
            return LocalSelection::from_mask(&mask, context)?
                .for_each_batch(array, fields, batch_rows, context, consume);
        }
        return emit_unfiltered_scan_batch(array, fields, batch_rows, context, consume);
    }
    let mut selection = LocalSelection::new(array.len(), context)?;
    let mut values = residual
        .map(|_| {
            add(&metrics.residual_batches, 1)?;
            super::super::prepared_unary::values::NativeBatch::new(array, columns, context)
        })
        .transpose()?;
    for row in 0..array.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if bloom_mask.as_ref().is_some_and(|mask| !mask.value(row)) {
            continue;
        }
        if let (Some(predicate), Some(values)) = (residual, values.as_mut())
            && !predicate.matches_with(&mut |column| values.stat(column, row))?
        {
            continue;
        }
        selection.push(row)?;
    }
    selection.for_each_batch(array, fields, batch_rows, context, consume)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    source: &ScanSource,
    files: &[PreparedVortexSource],
    #[cfg(feature = "vortex-write")] owned_array_sources: &[(
        shardloom_core::DatasetUri,
        crate::owned_array_source::OwnedArraySource,
    )],
    plan: &LocalVortexScanPlan,
    columns: &[String],
    residual: Option<&MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    batch_rows: usize,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let mut accept = |array: ArrayRef| {
        accept_scan_batch(
            &array,
            columns,
            residual,
            probe_filters,
            fields,
            context,
            metrics,
            batch_rows,
            consume,
        )
    };
    let scan_batches = match metrics.file_scan_batching.get() {
        super::FileScanBatching::Native => SCAN_ROWS,
        super::FileScanBatching::Output => batch_rows.min(SCAN_ROWS),
    };
    match source {
        ScanSource::File(index) => {
            let source_file = &files[*index];
            if residual.is_none()
                && has_infallible_empty_build(probe_filters, &mut |name| {
                    source_file.dtype().as_struct_fields_opt()?.field(name)
                })
            {
                prune_empty_file(source_file, plan, context, metrics)
            } else if let Some(pred) =
                admitted_runtime_predicate(probe_filters, source_file.dtype())
            {
                let updated = plan_with_runtime_predicate(plan, pred, source_file.dtype())?;
                run_file(
                    source_file,
                    &updated,
                    fields,
                    context,
                    metrics,
                    scan_batches,
                    &mut accept,
                )
            } else {
                run_file(
                    source_file,
                    plan,
                    fields,
                    context,
                    metrics,
                    scan_batches,
                    &mut accept,
                )
            }
        }
        ScanSource::Memory(projection) => run_memory_scan(
            projection,
            residual.is_some(),
            probe_filters,
            fields,
            context,
            metrics,
            &mut accept,
        ),
        #[cfg(feature = "vortex-write")]
        ScanSource::OwnedArray { index, .. } => {
            let owned = &owned_array_sources[*index].1;
            run_owned_array_with_probe_filters(
                owned,
                plan,
                residual.is_some(),
                true,
                probe_filters,
                fields,
                context,
                metrics,
                scan_batches,
                &mut accept,
            )
        }
        ScanSource::Batch { index, projection } => run_batch_scan(
            *index,
            projection,
            residual.is_some(),
            probe_filters,
            fields,
            context,
            metrics,
            &mut accept,
        ),
    }
}

fn run_memory_scan(
    projection: &crate::resident_memory_source::PreparedMemoryProjection,
    residual_present: bool,
    probe_filters: &[ProbeRuntimeFilter],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    accept: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    add(&metrics.scans_started, 1)?;
    if projection.source_rows() == 0
        || (!residual_present
            && has_infallible_empty_build(probe_filters, &mut |name| {
                fields
                    .iter()
                    .find(|(col, _)| col == name)
                    .map(|(_, dtype)| dtype.clone())
            }))
    {
        return Ok(());
    }
    add(&metrics.data_scans, 1)?;
    let mut sizing = Rows::from_fields(Owner::Scan, fields, SCAN_ROWS, 2, context)?;
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

#[allow(clippy::too_many_arguments)]
fn run_batch_scan(
    index: usize,
    projection: &crate::resident_memory_source::PreparedMemoryProjection,
    residual_present: bool,
    probe_filters: &[ProbeRuntimeFilter],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    accept: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let input = metrics
        .input
        .ok_or_else(|| super::batch_input::failed("streaming scan has no input provider"))?;
    let mut sizing = Rows::from_fields(Owner::Scan, fields, SCAN_ROWS, 2, context)?;
    input.run_with_probe_filters(
        index,
        probe_filters,
        residual_present,
        context,
        metrics,
        &mut |source| {
            add(&metrics.scans_started, 1)?;
            if source.is_empty() {
                return accept(projection.execute_batch_array(source, 0..0, context)?);
            }
            add(&metrics.data_scans, 1)?;
            let mut start = 0;
            while start < source.len() {
                let selected = sizing.select((source.len() - start) as u64, context)?;
                let end = start + selected.rows;
                accept(projection.execute_batch_array(source, start..end, context)?)?;
                sizing.complete(selected, None, context);
                start = end;
            }
            Ok(())
        },
    )
}

fn prune_empty_file(
    source: &PreparedVortexSource,
    plan: &LocalVortexScanPlan,
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
) -> Result<()> {
    source.with_admitted_native_execution(context, |_file, context| {
        let observation = super::super::native_file_scan_evidence::Observation::plan(
            source,
            plan,
            shardloom_core::native_file_scan::NativeFileScanRoute::RelationalScan,
            context,
        )?;
        add(&metrics.scans_started, 1)?;
        context.record_capability_decision(
            NativeIoCapability::RegionExclusion,
            NativeIoDecisionOutcome::Executed,
            NativeIoDecisionReason::ExclusionProved,
            NativeIoObservedWork {
                regions: Some(1),
                excluded_regions: Some(1),
                ..NativeIoObservedWork::default()
            },
        )?;
        add(&metrics.scans_pruned, 1)?;
        observation.complete(source, context, 0, 0, 1, 1)
    })
}

fn emit_unfiltered_scan_batch(
    array: &ArrayRef,
    fields: &[(String, DType)],
    batch_rows: usize,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    if batch_rows == 0 {
        return Err(failed(
            "selection source extent or output batch size changed",
        ));
    }
    if array.is_empty() {
        return Ok(());
    }
    let mut start = 0;
    while start < array.len() {
        context.check_cancelled()?;
        let end = array.len().min(start.saturating_add(batch_rows));
        let batch = if start == 0 && end == array.len() {
            super::project_selected_batch(array, fields, context)?
        } else {
            let sliced = array.slice(start..end).map_err(vortex_error)?;
            super::project_selected_batch(&sliced, fields, context)?
        };
        consume(batch)?;
        start = end;
    }
    Ok(())
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
        let observation = super::super::native_file_scan_evidence::Observation::plan(
            source,
            plan,
            shardloom_core::native_file_scan::NativeFileScanRoute::RelationalScan,
            context,
        )?;
        let pruning_checks = u64::from(plan.filter.is_some());
        add(&metrics.scans_started, 1)?;
        if let Some(filter) = &plan.filter {
            let excluded = crate::local_primitives::file_pruning::can_prune_file_with_predicate(
                file,
                filter,
                plan.pushdown_predicate.as_ref(),
            )?;
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
                return observation.complete(source, context, 0, 0, 1, 1);
            }
        }
        if file.row_count() == 0 {
            return observation.complete(source, context, 0, 0, pruning_checks, 0);
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
        let mut sizing = Rows::from_fields(Owner::Scan, fields, scan_rows, 2, context)?;
        let mut start = 0;
        let mut delivered = 0_u64;
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
                delivered = delivered
                    .checked_add(array.len() as u64)
                    .ok_or_else(|| failed("native scan delivered row count overflow"))?;
                consume(array)?;
            }
            sizing.complete(selected, None, context);
            start = end;
        }
        observation.complete(source, context, delivered, 1, pruning_checks, 0)
    })
}

#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_arguments)]
fn run_owned_array_with_probe_filters(
    owned: &crate::owned_array_source::OwnedArraySource,
    plan: &LocalVortexScanPlan,
    residual_present: bool,
    count_source_scan: bool,
    probe_filters: &[ProbeRuntimeFilter],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    scan_batches: usize,
    accept: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let _ = fields;
    if !residual_present
        && has_infallible_empty_build(probe_filters, &mut |name| {
            owned.dtype().as_struct_fields_opt()?.field(name)
        })
    {
        if count_source_scan {
            add(&metrics.scans_started, 1)?;
        }
        return Ok(());
    }
    if let Some(pred) = admitted_runtime_predicate(probe_filters, owned.dtype()) {
        let updated = plan_with_runtime_predicate(plan, pred, owned.dtype())?;
        run_owned_array(
            owned,
            &updated,
            count_source_scan,
            context,
            metrics,
            scan_batches,
            accept,
        )
    } else {
        run_owned_array(
            owned,
            plan,
            count_source_scan,
            context,
            metrics,
            scan_batches,
            accept,
        )
    }
}

#[cfg(feature = "vortex-write")]
fn collect_pushable_conjuncts(
    expr: &super::native_relational_expression::Expression,
    aliases: &std::collections::BTreeMap<String, String>,
    dtype: &DType,
    out: &mut Vec<PredicateExpr>,
) {
    use super::native_relational_expression::Kind as ExprKind;
    if let ExprKind::Binary(left, shardloom_core::BinaryOp::And, right) = &expr.kind {
        collect_pushable_conjuncts(left, aliases, dtype, out);
        collect_pushable_conjuncts(right, aliases, dtype, out);
    } else if let Some((pred, false)) = super::provider::scan_predicate(expr, aliases, dtype, None)
    {
        out.push(pred);
    }
}

#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_arguments)]
pub(super) fn run_owned_source_pushdown(
    owned: &crate::owned_array_source::OwnedArraySource,
    filter: Option<&super::native_relational_expression::Expression>,
    probe_filters: &[ProbeRuntimeFilter],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    batch_rows: usize,
    consume: &mut dyn FnMut(bool, ArrayRef) -> Result<()>,
) -> Result<bool> {
    let mut filter_satisfied = filter.is_none();
    let mut plan = LocalVortexScanPlan::passthrough();
    if let Some(expr) = filter {
        let aliases: std::collections::BTreeMap<String, String> = fields
            .iter()
            .map(|(name, _)| (name.clone(), name.clone()))
            .collect();
        if let Some((pred, false)) =
            super::provider::scan_predicate(expr, &aliases, owned.dtype(), None)
        {
            plan = plan_with_runtime_predicate(&plan, pred, owned.dtype())?;
            filter_satisfied = true;
        } else if expr.is_infallible_for_join_pushdown(true) {
            let mut conjuncts = Vec::new();
            collect_pushable_conjuncts(expr, &aliases, owned.dtype(), &mut conjuncts);
            if let Some(first) = conjuncts
                .into_iter()
                .reduce(|a, b| super::provider::conjoin(Some(a), b))
            {
                plan = plan_with_runtime_predicate(&plan, first, owned.dtype())?;
            }
        }
    }
    if plan.pushdown_predicate.is_none() && probe_filters.is_empty() {
        return Ok(false);
    }
    let scan_batches = match metrics.file_scan_batching.get() {
        super::FileScanBatching::Native => SCAN_ROWS,
        super::FileScanBatching::Output => batch_rows.min(SCAN_ROWS),
    };
    let mut accept = |array: ArrayRef| {
        if array.len() > SCAN_ROWS {
            return Err(failed("scan exceeded its admitted batch size"));
        }
        let bloom_mask = if probe_filters.is_empty() {
            None
        } else {
            apply_bloom_mask(&array, probe_filters, context)?
        };
        if bloom_mask.as_ref().is_some_and(Mask::all_false) {
            return Ok(());
        }
        if let Some(mask) = bloom_mask {
            return LocalSelection::from_mask(&mask, context)?.for_each_batch(
                &array,
                fields,
                batch_rows,
                context,
                &mut |filtered| consume(filter_satisfied, filtered),
            );
        }
        emit_unfiltered_scan_batch(&array, fields, batch_rows, context, &mut |filtered| {
            consume(filter_satisfied, filtered)
        })
    };
    run_owned_array_with_probe_filters(
        owned,
        &plan,
        !filter_satisfied,
        false,
        probe_filters,
        fields,
        context,
        metrics,
        scan_batches,
        &mut accept,
    )?;
    Ok(true)
}

#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_arguments)]
fn run_owned_array(
    source: &crate::owned_array_source::OwnedArraySource,
    plan: &LocalVortexScanPlan,
    count_source_scan: bool,
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    scan_rows: usize,
    accept: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    use vortex::layout::scan::split_by::SplitBy;

    if count_source_scan {
        add(&metrics.scans_started, 1)?;
    }
    if source.row_count() == 0 {
        return Ok(());
    }
    let (mut scan, scan_stats) = source.scan_with_stats(context.native_session());
    scan = scan
        .with_ordered(true)
        .with_concurrency(1)
        .with_split_by(SplitBy::RowCount(scan_rows.max(1)));
    if let Some(filter) = &plan.filter {
        let bound = filter
            .bind(source.dtype())
            .and_then(|expr| expr.optimize_recursive())
            .map_err(vortex_error)?;
        scan = scan.with_filter(bound);
    }
    if let Some(projection) = &plan.projection {
        let bound = projection
            .bind(source.dtype())
            .and_then(|expr| expr.optimize_recursive())
            .map_err(vortex_error)?;
        scan = scan.with_projection(bound);
    }
    let mut delivered = 0_u64;
    let iter = scan
        .into_array_iter(context.runtime())
        .map_err(vortex_error)?;
    for chunk in iter {
        context.check_cancelled()?;
        let array = chunk.map_err(vortex_error)?;
        if !array.is_empty() {
            delivered = delivered
                .checked_add(array.len() as u64)
                .ok_or_else(|| failed("owned array scan delivered row count overflow"))?;
            accept(array)?;
        }
    }
    let pruned_splits = scan_stats.excluded_zones();
    if plan.filter.is_some() && source.has_zone_maps() {
        let regions = source.row_count().div_ceil(8192);
        debug_assert!(pruned_splits <= regions);
        context.record_capability_decision(
            NativeIoCapability::RegionExclusion,
            if pruned_splits > 0 {
                NativeIoDecisionOutcome::Executed
            } else {
                NativeIoDecisionOutcome::Declined
            },
            if pruned_splits > 0 {
                NativeIoDecisionReason::ExclusionProved
            } else {
                NativeIoDecisionReason::ExclusionNotProved
            },
            NativeIoObservedWork {
                regions: Some(regions),
                excluded_regions: Some(pruned_splits),
                ..NativeIoObservedWork::default()
            },
        )?;
    }
    if count_source_scan && pruned_splits > 0 {
        add(&metrics.scans_pruned, pruned_splits)?;
    }
    if count_source_scan && (delivered > 0 || pruned_splits == 0) {
        add(&metrics.data_scans, 1)?;
    }
    Ok(())
}
