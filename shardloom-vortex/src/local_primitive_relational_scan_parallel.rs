//! Shared bounded parallel execution across Parquet, Vortex file, and owned-array sources.

use super::super::native_relational_aggregate::{Aggregate, Spec as AggregateSpec};
use super::super::native_relational_probe_filter::ProbeRuntimeFilter;
use super::{
    ArrayRef, DType, FileScanBatching, LocalVortexScanPlan, MaterializedPredicateEvaluator,
    Metrics, NativeExecutionContext, Node, NodeKind, PipelineKind, PreparedVortexRelational,
    PreparedVortexSource, Result, ScanSource, add, batch_input, failed, scan, transform,
    vortex_error,
};
use crate::native_work_sizing::{Owner, Rows};
use crate::resident_session::WorkerExecutionDelta;
use shardloom_core::{
    PredicateExpr,
    native_file_scan::NativeFileScanRoute,
    native_io_decisions::{
        NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
    },
};
use shardloom_exec::{
    compute_pool::{CancellationToken, ComputePool, ComputeTask, WorkerContext},
    live_memory::{Budgeted, MemoryLease},
};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{
    Arc,
    atomic::AtomicBool,
    mpsc::{self, Receiver},
};
use vortex::io::runtime::BlockingRuntime as _;

const MIN_PARALLEL_ROWS: u64 = 16_384;
const SPLIT_ROWS: u64 = 8_192;
const TASK_RESERVATION_BYTES: u64 = 4096;
const BOUNDED_CHANNEL_DEPTH: usize = 2;
const PER_WORKER_VORTEX_BUDGET_BYTES: u64 = 512 * 1024;

pub(super) enum ParallelTransformKind {
    Project(Vec<super::native_relational_expression::Expression>),
    Filter(super::native_relational_expression::Expression),
}

pub(super) struct ParallelAggregateTransform {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) kind: ParallelTransformKind,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub(super) struct ParallelAggregatePlan {
    pub(super) source_index: usize,
    pub(super) projection: Arc<super::PreparedMemoryProjection>,
    pub(super) mandatory_predicate: Option<(PredicateExpr, bool)>,
    pub(super) residual: Option<MaterializedPredicateEvaluator>,
    pub(super) columns: Vec<String>,
    pub(super) scan_fields: Vec<(String, DType)>,
    pub(super) transforms: Vec<ParallelAggregateTransform>,
    pub(super) spec: Arc<AggregateSpec>,
    pub(super) count_partial: bool,
    pub(super) batch_rows: usize,
    pub(super) scan_rows: usize,
    pub(super) finite_float_predicate: bool,
}

pub(super) enum ParallelScanTarget {
    File {
        source_index: usize,
        plan: LocalVortexScanPlan,
        columns: Vec<String>,
        scan_fields: Vec<(String, DType)>,
        residual: Option<MaterializedPredicateEvaluator>,
        finite_float_predicate: bool,
    },
    #[cfg(feature = "vortex-write")]
    OwnedArray {
        source_index: usize,
        plan: LocalVortexScanPlan,
        columns: Vec<String>,
        scan_fields: Vec<(String, DType)>,
        residual: Option<MaterializedPredicateEvaluator>,
        finite_float_predicate: bool,
    },
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    Batch {
        source_index: usize,
        projection: Arc<super::PreparedMemoryProjection>,
        mandatory_predicate: Option<(PredicateExpr, bool)>,
        residual: Option<MaterializedPredicateEvaluator>,
        columns: Vec<String>,
        scan_fields: Vec<(String, DType)>,
        finite_float_predicate: bool,
    },
}

pub(super) struct ParallelSourceChain {
    pub(super) target: ParallelScanTarget,
    pub(super) transforms: Vec<ParallelAggregateTransform>,
    pub(super) unwrapped_file_kernels: Vec<&'static str>,
    pub(super) batch_rows: usize,
    pub(super) scan_rows: usize,
}

type VortexParallelRangePlan = Option<(Arc<ComputePool>, usize, Vec<Range<u64>>)>;
type ParallelVortexRangeResult = (
    (Aggregate<'static>, scan::ScanBatchCounts, u64),
    WorkerExecutionDelta,
);

#[allow(clippy::large_enum_variant)]
pub(super) enum ParallelAggregateOutcome {
    Completed(Aggregate<'static>),
    ConsumedIntoCaller,
    NotAdmitted,
}

impl ParallelSourceChain {
    #[allow(clippy::too_many_lines)]
    pub(super) fn build(input: &Node, batch_rows: usize, scan_rows: usize) -> Option<Self> {
        let mut reversed = Vec::new();
        let mut unwrapped_file_kernels = Vec::new();
        let (mut current_fields, mut current_kind) = (input.fields.as_slice(), &input.kind);
        loop {
            match current_kind {
                NodeKind::FileKernel(kernel) if kernel.operation.is_projection() => {
                    unwrapped_file_kernels.push(kernel.admission_reason);
                    current_kind = &kernel.declaration;
                }
                NodeKind::Project { input, expressions } => {
                    let mut cloned = Vec::with_capacity(expressions.len());
                    for expr in expressions {
                        cloned.push(expr.try_clone_for_parallel()?);
                    }
                    reversed.push(ParallelAggregateTransform {
                        fields: current_fields.to_vec(),
                        kind: ParallelTransformKind::Project(cloned),
                    });
                    current_fields = input.fields.as_slice();
                    current_kind = &input.kind;
                }
                NodeKind::Filter { input, predicate } => {
                    reversed.push(ParallelAggregateTransform {
                        fields: current_fields.to_vec(),
                        kind: ParallelTransformKind::Filter(predicate.try_clone_for_parallel()?),
                    });
                    current_fields = input.fields.as_slice();
                    current_kind = &input.kind;
                }
                NodeKind::Pipeline { input, stages } => {
                    for stage in stages.iter().rev() {
                        let kind = match &stage.kind {
                            PipelineKind::Project(expressions) => {
                                let mut cloned = Vec::with_capacity(expressions.len());
                                for expr in expressions {
                                    cloned.push(expr.try_clone_for_parallel()?);
                                }
                                ParallelTransformKind::Project(cloned)
                            }
                            PipelineKind::Filter(predicate) => {
                                ParallelTransformKind::Filter(predicate.try_clone_for_parallel()?)
                            }
                            _ => return None,
                        };
                        reversed.push(ParallelAggregateTransform {
                            fields: stage.fields.clone(),
                            kind,
                        });
                    }
                    current_fields = input.fields.as_slice();
                    current_kind = &input.kind;
                }
                NodeKind::Scan {
                    source,
                    plan,
                    columns,
                    residual,
                    finite_float_predicate,
                    ..
                } => {
                    reversed.reverse();
                    let scan_fields = current_fields.to_vec();
                    let target = match source {
                        ScanSource::File(index) => ParallelScanTarget::File {
                            source_index: *index,
                            plan: plan.clone(),
                            columns: columns.clone(),
                            scan_fields,
                            residual: residual.clone(),
                            finite_float_predicate: *finite_float_predicate,
                        },
                        #[cfg(feature = "vortex-write")]
                        ScanSource::OwnedArray { index, .. } => ParallelScanTarget::OwnedArray {
                            source_index: *index,
                            plan: plan.clone(),
                            columns: columns.clone(),
                            scan_fields,
                            residual: residual.clone(),
                            finite_float_predicate: *finite_float_predicate,
                        },
                        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
                        ScanSource::Batch { index, projection } => ParallelScanTarget::Batch {
                            source_index: *index,
                            projection: Arc::clone(projection),
                            mandatory_predicate: plan
                                .pushdown_predicate
                                .as_ref()
                                .map(|pred| (pred.clone(), residual.is_none())),
                            residual: residual.clone(),
                            columns: columns.clone(),
                            scan_fields,
                            finite_float_predicate: *finite_float_predicate,
                        },
                        _ => return None,
                    };
                    return Some(Self {
                        target,
                        transforms: reversed,
                        unwrapped_file_kernels,
                        batch_rows,
                        scan_rows,
                    });
                }
                _ => return None,
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn build_for_join(
        input: &Node,
        probe_filters: &[ProbeRuntimeFilter],
        batch_rows: usize,
        scan_rows: usize,
    ) -> Option<(Self, Vec<ProbeRuntimeFilter>)> {
        let mut active_filters = probe_filters.to_vec();
        let mut reversed = Vec::new();
        let mut unwrapped_file_kernels = Vec::new();
        let (mut current_fields, mut current_kind) = (input.fields.as_slice(), &input.kind);
        loop {
            match current_kind {
                NodeKind::FileKernel(kernel) if kernel.operation.is_projection() => {
                    unwrapped_file_kernels.push(kernel.admission_reason);
                    current_kind = &kernel.declaration;
                }
                NodeKind::Project { input, expressions } => {
                    let mut cloned = Vec::with_capacity(expressions.len());
                    for expr in expressions {
                        cloned.push(expr.try_clone_for_parallel()?);
                    }
                    if !active_filters.is_empty() {
                        active_filters = active_filters
                            .into_iter()
                            .filter_map(|filter| {
                                filter.remap(|out_name| {
                                    let pos = current_fields
                                        .iter()
                                        .position(|(name, _)| name == out_name)?;
                                    match &expressions.get(pos)?.kind {
                                        super::native_relational_expression::Kind::Column(
                                            src_name,
                                        ) => Some(src_name.clone()),
                                        _ => None,
                                    }
                                })
                            })
                            .collect();
                    }
                    reversed.push(ParallelAggregateTransform {
                        fields: current_fields.to_vec(),
                        kind: ParallelTransformKind::Project(cloned),
                    });
                    current_fields = input.fields.as_slice();
                    current_kind = &input.kind;
                }
                NodeKind::Filter { input, predicate } => {
                    if !predicate.is_infallible_for_join_pushdown(true) {
                        active_filters.clear();
                    }
                    reversed.push(ParallelAggregateTransform {
                        fields: current_fields.to_vec(),
                        kind: ParallelTransformKind::Filter(predicate.try_clone_for_parallel()?),
                    });
                    current_fields = input.fields.as_slice();
                    current_kind = &input.kind;
                }
                NodeKind::Pipeline { input, stages } => {
                    for stage in stages.iter().rev() {
                        let kind = match &stage.kind {
                            PipelineKind::Project(expressions) => {
                                let mut cloned = Vec::with_capacity(expressions.len());
                                for expr in expressions {
                                    cloned.push(expr.try_clone_for_parallel()?);
                                }
                                if !active_filters.is_empty() {
                                    let fields = &stage.fields;
                                    active_filters = active_filters
                                        .into_iter()
                                        .filter_map(|filter| {
                                            filter.remap(|out_name| {
                                                let pos = fields
                                                    .iter()
                                                    .position(|(name, _)| name == out_name)?;
                                                match &expressions.get(pos)?.kind {
                                                    super::native_relational_expression::Kind::Column(src_name) => Some(src_name.clone()),
                                                    _ => None,
                                                }
                                            })
                                        })
                                        .collect();
                                }
                                ParallelTransformKind::Project(cloned)
                            }
                            PipelineKind::Filter(predicate) => {
                                if !predicate.is_infallible_for_join_pushdown(true) {
                                    active_filters.clear();
                                }
                                ParallelTransformKind::Filter(predicate.try_clone_for_parallel()?)
                            }
                            _ => return None,
                        };
                        reversed.push(ParallelAggregateTransform {
                            fields: stage.fields.clone(),
                            kind,
                        });
                    }
                    current_fields = input.fields.as_slice();
                    current_kind = &input.kind;
                }
                NodeKind::Scan {
                    source,
                    plan,
                    columns,
                    residual,
                    finite_float_predicate,
                    ..
                } => {
                    reversed.reverse();
                    let scan_fields = current_fields.to_vec();
                    let target = match source {
                        ScanSource::File(index) => ParallelScanTarget::File {
                            source_index: *index,
                            plan: plan.clone(),
                            columns: columns.clone(),
                            scan_fields,
                            residual: residual.clone(),
                            finite_float_predicate: *finite_float_predicate,
                        },
                        #[cfg(feature = "vortex-write")]
                        ScanSource::OwnedArray { index, .. } => ParallelScanTarget::OwnedArray {
                            source_index: *index,
                            plan: plan.clone(),
                            columns: columns.clone(),
                            scan_fields,
                            residual: residual.clone(),
                            finite_float_predicate: *finite_float_predicate,
                        },
                        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
                        ScanSource::Batch { index, projection } => ParallelScanTarget::Batch {
                            source_index: *index,
                            projection: Arc::clone(projection),
                            mandatory_predicate: plan
                                .pushdown_predicate
                                .as_ref()
                                .map(|pred| (pred.clone(), residual.is_none())),
                            residual: residual.clone(),
                            columns: columns.clone(),
                            scan_fields,
                            finite_float_predicate: *finite_float_predicate,
                        },
                        _ => return None,
                    };
                    return Some((
                        Self {
                            target,
                            transforms: reversed,
                            unwrapped_file_kernels,
                            batch_rows,
                            scan_rows,
                        },
                        active_filters,
                    ));
                }
                _ => return None,
            }
        }
    }
}

pub(in crate::local_primitives::prepared_relational) fn apply_parallel_transforms(
    transforms: &[ParallelAggregateTransform],
    index: usize,
    array: ArrayRef,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let Some(stage) = transforms.get(index) else {
        return consume(array);
    };
    match &stage.kind {
        ParallelTransformKind::Project(expressions) => {
            let projected = transform::project_array(&stage.fields, expressions, &array, context)?;
            apply_parallel_transforms(transforms, index + 1, projected, context, consume)
        }
        ParallelTransformKind::Filter(predicate) => {
            transform::apply_filter(predicate, &stage.fields, &array, context, &mut |filtered| {
                apply_parallel_transforms(transforms, index + 1, filtered, context, consume)
            })
        }
    }
}

fn admit_vortex_parallel(
    row_count: u64,
    prepared: &PreparedVortexRelational,
    context: &NativeExecutionContext<'_>,
) -> Result<VortexParallelRangePlan> {
    if row_count < MIN_PARALLEL_ROWS || context.cpu_lanes() <= 1 {
        return Ok(None);
    }
    let Some(pool) = prepared.session.shared_compute_pool(context.cpu_lanes())? else {
        return Ok(None);
    };
    let snap = context.memory().snapshot();
    let available = snap.limit_bytes.saturating_sub(snap.reserved_bytes);
    let max_by_memory =
        usize::try_from(available / PER_WORKER_VORTEX_BUDGET_BYTES).unwrap_or(usize::MAX);
    let chunks = usize::try_from(row_count.div_ceil(SPLIT_ROWS)).unwrap_or(usize::MAX);
    let workers = context.cpu_lanes().min(chunks).min(max_by_memory);
    if workers < 2 {
        return Ok(None);
    }
    let ranges = partition_row_ranges(row_count, workers);
    if ranges.len() < 2 {
        return Ok(None);
    }
    Ok(Some((pool, workers, ranges)))
}

fn partition_row_ranges(row_count: u64, workers: usize) -> Vec<Range<u64>> {
    let chunks = usize::try_from(row_count.div_ceil(SPLIT_ROWS)).unwrap_or(usize::MAX);
    let count = workers.min(chunks).max(1);
    let base = chunks / count;
    let rem = chunks % count;
    let mut ranges = Vec::with_capacity(count);
    let mut chunk_start = 0usize;
    for idx in 0..count {
        let len = base + usize::from(idx < rem);
        let chunk_end = chunk_start + len;
        let start = (chunk_start as u64) * SPLIT_ROWS;
        let end = ((chunk_end as u64) * SPLIT_ROWS).min(row_count);
        if start < end {
            ranges.push(start..end);
        }
        chunk_start = chunk_end;
    }
    ranges
}

fn resolve_file_plan(
    source_file: &PreparedVortexSource,
    base_plan: &LocalVortexScanPlan,
    probe_filters: &[ProbeRuntimeFilter],
) -> Result<LocalVortexScanPlan> {
    if let Some(pred) = scan::admitted_runtime_predicate(probe_filters, source_file.dtype())
        && crate::local_primitives::file_pruning::runtime_predicate_is_selective_on_file(
            source_file.file(),
            &pred,
        )
    {
        scan::plan_with_runtime_predicate(base_plan, pred, source_file.dtype())
    } else {
        Ok(base_plan.clone())
    }
}

#[allow(clippy::too_many_arguments)]
fn run_vortex_file_worker_batches(
    file: &vortex::file::VortexFile,
    plan: &LocalVortexScanPlan,
    range: Range<u64>,
    columns: &[String],
    residual: Option<&MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    scan_fields: &[(String, DType)],
    scan_rows: usize,
    batch_rows: usize,
    wctx: &NativeExecutionContext<'_>,
    counts: &mut scan::ScanBatchCounts,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<u64> {
    let mut builder = file
        .scan()
        .map_err(vortex_error)?
        .with_ordered(true)
        .with_concurrency(1)
        .with_natural_splits([0, file.row_count()].into());
    if let Some(filter) = &plan.filter {
        builder = builder.with_filter(super::super::bind_vortex_scan_expr(file, filter)?);
    }
    if let Some(projection) = &plan.projection {
        builder = builder.with_projection(super::super::bind_vortex_scan_expr(file, projection)?);
    }
    let prepared_scan = builder.prepare().map_err(vortex_error)?;
    let mut sizing = Rows::from_fields(Owner::Scan, scan_fields, scan_rows, 2, wctx)?;
    let mut pos = range.start;
    let mut delivered = 0_u64;
    while pos < range.end {
        wctx.check_cancelled()?;
        let selected = sizing.select(range.end - pos, wctx)?;
        let sub_end = pos + selected.rows as u64;
        let mut tasks = prepared_scan
            .execute(Some(pos..sub_end))
            .map_err(vortex_error)?;
        if tasks.len() != 1 {
            return Err(failed("native scan range changed its admitted task count"));
        }
        let task = tasks
            .pop()
            .ok_or_else(|| failed("native scan task is absent"))?;
        if let Some(array) = wctx.runtime().block_on(task).map_err(vortex_error)? {
            delivered = delivered
                .checked_add(array.len() as u64)
                .ok_or_else(|| failed("native scan delivered row count overflow"))?;
            scan::accept_scan_batch_counts(
                &array,
                columns,
                residual,
                probe_filters,
                scan_fields,
                wctx,
                counts,
                batch_rows,
                consume,
            )?;
        }
        sizing.complete(selected, None, wctx);
        pos = sub_end;
    }
    Ok(delivered)
}

#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_arguments)]
fn run_owned_array_worker_batches(
    owned: &crate::owned_array_source::OwnedArraySource,
    plan: &LocalVortexScanPlan,
    range: Range<u64>,
    columns: &[String],
    residual: Option<&MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    scan_fields: &[(String, DType)],
    scan_rows: usize,
    batch_rows: usize,
    wctx: &NativeExecutionContext<'_>,
    counts: &mut scan::ScanBatchCounts,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<u64> {
    use vortex::layout::scan::split_by::SplitBy;

    let (mut builder, scan_stats) = owned.scan_with_stats(wctx.native_session());
    builder = builder
        .with_ordered(true)
        .with_concurrency(1)
        .with_row_range(range)
        .with_split_by(SplitBy::RowCount(scan_rows.max(1)));
    if let Some(filter) = &plan.filter {
        let bound = filter
            .bind(owned.dtype())
            .and_then(|expr| expr.optimize_recursive())
            .map_err(vortex_error)?;
        builder = builder.with_filter(bound);
    }
    if let Some(projection) = &plan.projection {
        let bound = projection
            .bind(owned.dtype())
            .and_then(|expr| expr.optimize_recursive())
            .map_err(vortex_error)?;
        builder = builder.with_projection(bound);
    }
    let iter = builder
        .into_array_iter(wctx.runtime())
        .map_err(vortex_error)?;
    for chunk in iter {
        wctx.check_cancelled()?;
        let array = chunk.map_err(vortex_error)?;
        if !array.is_empty() {
            scan::accept_scan_batch_counts(
                &array,
                columns,
                residual,
                probe_filters,
                scan_fields,
                wctx,
                counts,
                batch_rows,
                consume,
            )?;
        }
    }
    Ok(scan_stats.excluded_zones())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn run_parallel_aggregate(
    prepared: &PreparedVortexRelational,
    input: &Node,
    spec: &AggregateSpec,
    aggregate: &mut Aggregate<'_>,
    count_partial: bool,
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    batch_rows: usize,
) -> Result<ParallelAggregateOutcome> {
    let scan_rows = match metrics.file_scan_batching.get() {
        FileScanBatching::Native => scan::SCAN_ROWS,
        FileScanBatching::Output => batch_rows.min(scan::SCAN_ROWS),
    };
    let Some(chain) = ParallelSourceChain::build(input, batch_rows, scan_rows) else {
        return Ok(ParallelAggregateOutcome::NotAdmitted);
    };
    let shared_spec = spec.try_clone_for_parallel_merge();
    match &chain.target {
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        ParallelScanTarget::Batch {
            source_index,
            projection,
            mandatory_predicate,
            residual,
            columns,
            scan_fields,
            finite_float_predicate,
        } => {
            let Some(shared_spec) = shared_spec else {
                return Ok(ParallelAggregateOutcome::NotAdmitted);
            };
            let Some(input_provider) = metrics.input else {
                return Ok(ParallelAggregateOutcome::NotAdmitted);
            };
            let plan = Arc::new(ParallelAggregatePlan {
                source_index: *source_index,
                projection: Arc::clone(projection),
                mandatory_predicate: mandatory_predicate.clone(),
                residual: residual.clone(),
                columns: columns.clone(),
                scan_fields: scan_fields.clone(),
                transforms: chain.transforms,
                spec: shared_spec,
                count_partial,
                batch_rows: chain.batch_rows,
                scan_rows: chain.scan_rows,
                finite_float_predicate: *finite_float_predicate,
            });
            let mut completed = None;
            input_provider.with_execution(&mut |execution| {
                completed = batch_input::parallel::run_parallel_parquet_aggregate(
                    execution, &plan, context, metrics,
                )?;
                Ok(())
            })?;
            match completed {
                Some(completed) => Ok(ParallelAggregateOutcome::Completed(completed)),
                None => Ok(ParallelAggregateOutcome::NotAdmitted),
            }
        }
        ParallelScanTarget::File {
            source_index,
            plan,
            columns,
            scan_fields,
            residual,
            finite_float_predicate,
        } => {
            let source_file = &prepared.sources[*source_index];
            let Some((pool, workers, ranges)) =
                admit_vortex_parallel(source_file.file().row_count(), prepared, context)?
            else {
                return Ok(ParallelAggregateOutcome::NotAdmitted);
            };
            let transforms = Arc::new(chain.transforms);
            let columns: Arc<[String]> = Arc::from(columns.as_slice());
            let scan_fields: Arc<[(String, DType)]> = Arc::from(scan_fields.as_slice());
            let residual = residual.clone();
            let effective_plan = resolve_file_plan(source_file, plan, &[])?;
            if let Some(shared_spec) = shared_spec {
                let merged =
                    source_file.with_admitted_native_execution(context, |file, context| {
                        let observation =
                            super::super::native_file_scan_evidence::Observation::plan(
                                source_file,
                                &effective_plan,
                                NativeFileScanRoute::RelationalScan,
                                context,
                            )?;
                        let pruning_checks = u64::from(effective_plan.filter.is_some());
                        add(&metrics.scans_started, 1)?;
                        if let Some(filter) = &effective_plan.filter {
                            let excluded =
                            crate::local_primitives::file_pruning::can_prune_file_with_predicate(
                                file,
                                filter,
                                effective_plan.pushdown_predicate.as_ref(),
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
                                observation.complete(source_file, context, 0, 0, 1, 1)?;
                                metrics
                                    .source_workers
                                    .set(metrics.source_workers.get().max(1));
                                metrics
                                    .operator_workers
                                    .set(metrics.operator_workers.get().max(1));
                                return Aggregate::new_shared(shared_spec, context.memory());
                            }
                        }
                        add(&metrics.data_scans, 1)?;
                        let total_ranges = ranges.len();
                        let child_flag = Arc::new(AtomicBool::new(false));
                        let child_token = CancellationToken::from_shared_flag_with_parent(
                            child_flag,
                            context.cancellation(),
                        );
                        let scope = prepared
                            .session
                            .worker_execution_scope(context, child_token.clone())?;
                        let _worker_limit = pool.scope_active_worker_limit(workers);
                        let mut active: VecDeque<ComputeTask<ParallelVortexRangeResult>> =
                            VecDeque::with_capacity(total_ranges);
                        for range in ranges {
                            let lease = match context.memory().reserve(TASK_RESERVATION_BYTES) {
                                Ok(lease) => lease,
                                Err(error) => {
                                    child_token.cancel();
                                    while let Some(task) = active.pop_front() {
                                        let _ = task.join();
                                    }
                                    return Err(error);
                                }
                            };
                            let task_scope = scope.clone();
                            let task_file = file.clone();
                            let task_plan = effective_plan.clone();
                            let task_columns = Arc::clone(&columns);
                            let task_residual = residual.clone();
                            let task_fields = Arc::clone(&scan_fields);
                            let task_transforms = Arc::clone(&transforms);
                            let task_spec = Arc::clone(&shared_spec);
                            let cancel = child_token.clone();
                            let submitted = pool.submit(
                                Budgeted::new(
                                    move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                                        task_scope.with_context(|wctx| {
                                            let mut worker_agg =
                                                Aggregate::new_shared(task_spec, wctx.memory())?;
                                            let mut counts = scan::ScanBatchCounts::default();
                                            let delivered = run_vortex_file_worker_batches(
                                                &task_file,
                                                &task_plan,
                                                range,
                                                &task_columns,
                                                task_residual.as_ref(),
                                                &[],
                                                &task_fields,
                                                scan_rows,
                                                batch_rows,
                                                wctx,
                                                &mut counts,
                                                &mut |array| {
                                                    apply_parallel_transforms(
                                                        &task_transforms,
                                                        0,
                                                        array,
                                                        wctx,
                                                        &mut |final_batch| {
                                                            if count_partial {
                                                                worker_agg.consume_reordered_count(
                                                                    &final_batch,
                                                                    wctx,
                                                                    batch_rows,
                                                                )
                                                            } else {
                                                                worker_agg.consume(
                                                                    &final_batch,
                                                                    wctx,
                                                                    batch_rows,
                                                                )
                                                            }
                                                        },
                                                    )
                                                },
                                            )?;
                                            Ok((worker_agg, counts, delivered))
                                        })
                                    },
                                    lease,
                                ),
                                cancel,
                            );
                            match submitted {
                                Ok(task) => active.push_back(task),
                                Err(error) => {
                                    child_token.cancel();
                                    while let Some(task) = active.pop_front() {
                                        let _ = task.join();
                                    }
                                    return Err(error);
                                }
                            }
                        }
                        let mut merged_agg: Option<Aggregate<'static>> = None;
                        let mut merged_counts = scan::ScanBatchCounts::default();
                        let mut total_delivered = 0_u64;
                        while let Some(task) = active.pop_front() {
                            if let Err(error) = context.check_cancelled() {
                                child_token.cancel();
                                let _ = task.join();
                                while let Some(rest) = active.pop_front() {
                                    let _ = rest.join();
                                }
                                return Err(error);
                            }
                            let joined = match task.join() {
                                Ok(done) => done,
                                Err(error) => {
                                    child_token.cancel();
                                    while let Some(rest) = active.pop_front() {
                                        let _ = rest.join();
                                    }
                                    return Err(error);
                                }
                            };
                            let (((worker_agg, worker_counts, delivered), delta), lease) =
                                joined.into_parts();
                            drop(lease);
                            let step = (|| -> Result<()> {
                                context.merge_worker_delta(delta)?;
                                total_delivered =
                                    total_delivered.checked_add(delivered).ok_or_else(|| {
                                        failed("native scan delivered row count overflow")
                                    })?;
                                merge_scan_counts(&mut merged_counts, worker_counts)?;
                                match &mut merged_agg {
                                    None => merged_agg = Some(worker_agg),
                                    Some(existing) => {
                                        existing.merge_from(worker_agg, context, batch_rows)?;
                                    }
                                }
                                Ok(())
                            })();
                            if let Err(error) = step {
                                child_token.cancel();
                                while let Some(rest) = active.pop_front() {
                                    let _ = rest.join();
                                }
                                return Err(error);
                            }
                        }
                        observation.complete(
                            source_file,
                            context,
                            total_delivered,
                            1,
                            pruning_checks,
                            0,
                        )?;
                        merged_counts.merge_into(metrics)?;
                        if *finite_float_predicate {
                            add(&metrics.pipeline.finite_float_scans, 1)?;
                        }
                        metrics
                            .source_workers
                            .set(metrics.source_workers.get().max(workers as u64));
                        metrics
                            .operator_workers
                            .set(metrics.operator_workers.get().max(workers as u64));
                        merged_agg.ok_or_else(|| failed("parallel file aggregate yielded no state"))
                    })?;
                record_unwrapped_file_kernels(&chain.unwrapped_file_kernels, metrics, context)?;
                Ok(ParallelAggregateOutcome::Completed(merged))
            } else {
                // Float SUM/AVG or non-mergeable aggregate: ordered parallel source decode -> serial operator.
                source_file.with_admitted_native_execution(context, |file, context| {
                    let observation = super::super::native_file_scan_evidence::Observation::plan(
                        source_file,
                        &effective_plan,
                        NativeFileScanRoute::RelationalScan,
                        context,
                    )?;
                    let pruning_checks = u64::from(effective_plan.filter.is_some());
                    add(&metrics.scans_started, 1)?;
                    if let Some(filter) = &effective_plan.filter {
                        let excluded =
                            crate::local_primitives::file_pruning::can_prune_file_with_predicate(
                                file,
                                filter,
                                effective_plan.pushdown_predicate.as_ref(),
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
                            observation.complete(source_file, context, 0, 0, 1, 1)?;
                            metrics
                                .source_workers
                                .set(metrics.source_workers.get().max(1));
                            metrics
                                .operator_workers
                                .set(metrics.operator_workers.get().max(1));
                            return Ok(());
                        }
                    }
                    add(&metrics.data_scans, 1)?;
                    let mut merged_counts = scan::ScanBatchCounts::default();
                    let mut total_delivered = 0_u64;
                    stream_vortex_file_ranges(
                        file,
                        &effective_plan,
                        &pool,
                        workers,
                        ranges,
                        &columns,
                        residual.as_ref(),
                        &[],
                        &scan_fields,
                        &transforms,
                        scan_rows,
                        batch_rows,
                        prepared,
                        context,
                        &mut |counts, delivered| {
                            total_delivered =
                                total_delivered.checked_add(delivered).ok_or_else(|| {
                                    failed("native scan delivered row count overflow")
                                })?;
                            merge_scan_counts(&mut merged_counts, counts)
                        },
                        &mut |final_batch| {
                            if count_partial {
                                aggregate.consume_reordered_count(&final_batch, context, batch_rows)
                            } else {
                                aggregate.consume(&final_batch, context, batch_rows)
                            }
                        },
                    )?;
                    observation.complete(
                        source_file,
                        context,
                        total_delivered,
                        1,
                        pruning_checks,
                        0,
                    )?;
                    merged_counts.merge_into(metrics)?;
                    if *finite_float_predicate {
                        add(&metrics.pipeline.finite_float_scans, 1)?;
                    }
                    metrics
                        .source_workers
                        .set(metrics.source_workers.get().max(workers as u64));
                    metrics
                        .operator_workers
                        .set(metrics.operator_workers.get().max(1));
                    Ok(())
                })?;
                record_unwrapped_file_kernels(&chain.unwrapped_file_kernels, metrics, context)?;
                Ok(ParallelAggregateOutcome::ConsumedIntoCaller)
            }
        }
        #[cfg(feature = "vortex-write")]
        ParallelScanTarget::OwnedArray {
            source_index,
            plan,
            columns,
            scan_fields,
            residual,
            finite_float_predicate,
        } => {
            let owned = &prepared.owned_array_sources[*source_index].1;
            let Some((pool, workers, ranges)) =
                admit_vortex_parallel(owned.row_count(), prepared, context)?
            else {
                return Ok(ParallelAggregateOutcome::NotAdmitted);
            };
            let transforms = Arc::new(chain.transforms);
            let columns: Arc<[String]> = Arc::from(columns.as_slice());
            let scan_fields: Arc<[(String, DType)]> = Arc::from(scan_fields.as_slice());
            let residual = residual.clone();
            add(&metrics.scans_started, 1)?;
            add(&metrics.data_scans, 1)?;
            if let Some(shared_spec) = shared_spec {
                let total_ranges = ranges.len();
                let child_flag = Arc::new(AtomicBool::new(false));
                let child_token = CancellationToken::from_shared_flag_with_parent(
                    child_flag,
                    context.cancellation(),
                );
                let scope = prepared
                    .session
                    .worker_execution_scope(context, child_token.clone())?;
                let _worker_limit = pool.scope_active_worker_limit(workers);
                let mut active: VecDeque<ComputeTask<ParallelVortexRangeResult>> =
                    VecDeque::with_capacity(total_ranges);
                for range in ranges {
                    let lease = match context.memory().reserve(TASK_RESERVATION_BYTES) {
                        Ok(lease) => lease,
                        Err(error) => {
                            child_token.cancel();
                            while let Some(task) = active.pop_front() {
                                let _ = task.join();
                            }
                            return Err(error);
                        }
                    };
                    let task_scope = scope.clone();
                    let task_owned = owned.clone();
                    let task_plan = plan.clone();
                    let task_columns = Arc::clone(&columns);
                    let task_residual = residual.clone();
                    let task_fields = Arc::clone(&scan_fields);
                    let task_transforms = Arc::clone(&transforms);
                    let task_spec = Arc::clone(&shared_spec);
                    let cancel = child_token.clone();
                    let submitted = pool.submit(
                        Budgeted::new(
                            move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                                task_scope.with_context(|wctx| {
                                    let mut worker_agg =
                                        Aggregate::new_shared(task_spec, wctx.memory())?;
                                    let mut counts = scan::ScanBatchCounts::default();
                                    let pruned = run_owned_array_worker_batches(
                                        &task_owned,
                                        &task_plan,
                                        range,
                                        &task_columns,
                                        task_residual.as_ref(),
                                        &[],
                                        &task_fields,
                                        scan_rows,
                                        batch_rows,
                                        wctx,
                                        &mut counts,
                                        &mut |array| {
                                            apply_parallel_transforms(
                                                &task_transforms,
                                                0,
                                                array,
                                                wctx,
                                                &mut |final_batch| {
                                                    if count_partial {
                                                        worker_agg.consume_reordered_count(
                                                            &final_batch,
                                                            wctx,
                                                            batch_rows,
                                                        )
                                                    } else {
                                                        worker_agg.consume(
                                                            &final_batch,
                                                            wctx,
                                                            batch_rows,
                                                        )
                                                    }
                                                },
                                            )
                                        },
                                    )?;
                                    Ok((worker_agg, counts, pruned))
                                })
                            },
                            lease,
                        ),
                        cancel,
                    );
                    match submitted {
                        Ok(task) => active.push_back(task),
                        Err(error) => {
                            child_token.cancel();
                            while let Some(task) = active.pop_front() {
                                let _ = task.join();
                            }
                            return Err(error);
                        }
                    }
                }
                let mut merged_agg: Option<Aggregate<'static>> = None;
                let mut merged_counts = scan::ScanBatchCounts::default();
                let mut total_pruned = 0_u64;
                while let Some(task) = active.pop_front() {
                    if let Err(error) = context.check_cancelled() {
                        child_token.cancel();
                        let _ = task.join();
                        while let Some(rest) = active.pop_front() {
                            let _ = rest.join();
                        }
                        return Err(error);
                    }
                    let joined = match task.join() {
                        Ok(done) => done,
                        Err(error) => {
                            child_token.cancel();
                            while let Some(rest) = active.pop_front() {
                                let _ = rest.join();
                            }
                            return Err(error);
                        }
                    };
                    let (((worker_agg, worker_counts, pruned), delta), lease) = joined.into_parts();
                    drop(lease);
                    let step = (|| -> Result<()> {
                        context.merge_worker_delta(delta)?;
                        total_pruned += pruned;
                        merge_scan_counts(&mut merged_counts, worker_counts)?;
                        match &mut merged_agg {
                            None => merged_agg = Some(worker_agg),
                            Some(existing) => {
                                existing.merge_from(worker_agg, context, batch_rows)?;
                            }
                        }
                        Ok(())
                    })();
                    if let Err(error) = step {
                        child_token.cancel();
                        while let Some(rest) = active.pop_front() {
                            let _ = rest.join();
                        }
                        return Err(error);
                    }
                }
                if plan.filter.is_some() && owned.has_zone_maps() {
                    let regions = owned.row_count().div_ceil(SPLIT_ROWS);
                    context.record_capability_decision(
                        NativeIoCapability::RegionExclusion,
                        if total_pruned > 0 {
                            NativeIoDecisionOutcome::Executed
                        } else {
                            NativeIoDecisionOutcome::Declined
                        },
                        if total_pruned > 0 {
                            NativeIoDecisionReason::ExclusionProved
                        } else {
                            NativeIoDecisionReason::ExclusionNotProved
                        },
                        NativeIoObservedWork {
                            regions: Some(regions),
                            excluded_regions: Some(total_pruned),
                            ..NativeIoObservedWork::default()
                        },
                    )?;
                }
                merged_counts.merge_into(metrics)?;
                if *finite_float_predicate {
                    add(&metrics.pipeline.finite_float_scans, 1)?;
                }
                metrics
                    .source_workers
                    .set(metrics.source_workers.get().max(workers as u64));
                metrics
                    .operator_workers
                    .set(metrics.operator_workers.get().max(workers as u64));
                Ok(ParallelAggregateOutcome::Completed(merged_agg.ok_or_else(
                    || failed("parallel owned-array aggregate yielded no state"),
                )?))
            } else {
                let mut merged_counts = scan::ScanBatchCounts::default();
                let mut total_pruned = 0_u64;
                stream_owned_array_ranges(
                    owned,
                    plan,
                    &pool,
                    workers,
                    ranges,
                    &columns,
                    residual.as_ref(),
                    &[],
                    &scan_fields,
                    &transforms,
                    scan_rows,
                    batch_rows,
                    prepared,
                    context,
                    &mut |counts, pruned| {
                        total_pruned += pruned;
                        merge_scan_counts(&mut merged_counts, counts)
                    },
                    &mut |final_batch| {
                        if count_partial {
                            aggregate.consume_reordered_count(&final_batch, context, batch_rows)
                        } else {
                            aggregate.consume(&final_batch, context, batch_rows)
                        }
                    },
                )?;
                if plan.filter.is_some() && owned.has_zone_maps() {
                    let regions = owned.row_count().div_ceil(SPLIT_ROWS);
                    context.record_capability_decision(
                        NativeIoCapability::RegionExclusion,
                        if total_pruned > 0 {
                            NativeIoDecisionOutcome::Executed
                        } else {
                            NativeIoDecisionOutcome::Declined
                        },
                        if total_pruned > 0 {
                            NativeIoDecisionReason::ExclusionProved
                        } else {
                            NativeIoDecisionReason::ExclusionNotProved
                        },
                        NativeIoObservedWork {
                            regions: Some(regions),
                            excluded_regions: Some(total_pruned),
                            ..NativeIoObservedWork::default()
                        },
                    )?;
                }
                merged_counts.merge_into(metrics)?;
                if *finite_float_predicate {
                    add(&metrics.pipeline.finite_float_scans, 1)?;
                }
                metrics
                    .source_workers
                    .set(metrics.source_workers.get().max(workers as u64));
                metrics
                    .operator_workers
                    .set(metrics.operator_workers.get().max(1));
                Ok(ParallelAggregateOutcome::ConsumedIntoCaller)
            }
        }
    }
}

fn merge_scan_counts(dst: &mut scan::ScanBatchCounts, src: scan::ScanBatchCounts) -> Result<()> {
    dst.scan_rows = dst
        .scan_rows
        .checked_add(src.scan_rows)
        .ok_or_else(|| failed("scan row counter overflow"))?;
    dst.scan_batches = dst
        .scan_batches
        .checked_add(src.scan_batches)
        .ok_or_else(|| failed("scan batch counter overflow"))?;
    dst.scans_started = dst
        .scans_started
        .checked_add(src.scans_started)
        .ok_or_else(|| failed("scan counter overflow"))?;
    dst.data_scans = dst
        .data_scans
        .checked_add(src.data_scans)
        .ok_or_else(|| failed("data scan counter overflow"))?;
    dst.residual_batches = dst
        .residual_batches
        .checked_add(src.residual_batches)
        .ok_or_else(|| failed("residual batch counter overflow"))?;
    Ok(())
}

type StreamTask<S> = (
    Receiver<Result<ArrayRef>>,
    ComputeTask<((scan::ScanBatchCounts, S), WorkerExecutionDelta)>,
);

fn teardown_ordered_streams<S: Send + 'static>(
    child_token: &CancellationToken,
    current_rx: Option<Receiver<Result<ArrayRef>>>,
    current_task: Option<ComputeTask<((scan::ScanBatchCounts, S), WorkerExecutionDelta)>>,
    mut pending: VecDeque<StreamTask<S>>,
) {
    child_token.cancel();
    drop(current_rx);
    for (rx, _) in &mut pending {
        let (_, dummy_rx) = mpsc::sync_channel(0);
        let real_rx = std::mem::replace(rx, dummy_rx);
        drop(real_rx);
    }
    if let Some(task) = current_task {
        let _ = task.join();
    }
    while let Some((rx, task)) = pending.pop_front() {
        drop(rx);
        let _ = task.join();
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_ordered_streams<S: Send + 'static>(
    child_token: &CancellationToken,
    mut active: VecDeque<StreamTask<S>>,
    context: &NativeExecutionContext<'_>,
    on_summary: &mut dyn FnMut(scan::ScanBatchCounts, S) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    while let Some((rx, task)) = active.pop_front() {
        loop {
            if let Err(error) = context.check_cancelled() {
                teardown_ordered_streams(child_token, Some(rx), Some(task), active);
                return Err(error);
            }
            match rx.recv() {
                Ok(Ok(batch)) => {
                    if let Err(error) = consume(batch) {
                        teardown_ordered_streams(child_token, Some(rx), Some(task), active);
                        return Err(error);
                    }
                }
                Ok(Err(error)) => {
                    teardown_ordered_streams(child_token, Some(rx), Some(task), active);
                    return Err(error);
                }
                Err(_) => break,
            }
        }
        let joined = match task.join() {
            Ok(done) => done,
            Err(error) => {
                teardown_ordered_streams(child_token, None, None, active);
                return Err(error);
            }
        };
        let (((counts, summary), delta), lease) = joined.into_parts();
        drop(lease);
        if let Err(error) = context
            .merge_worker_delta(delta)
            .and_then(|()| on_summary(counts, summary))
        {
            teardown_ordered_streams(child_token, None, None, active);
            return Err(error);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn stream_vortex_file_ranges(
    file: &vortex::file::VortexFile,
    plan: &LocalVortexScanPlan,
    pool: &ComputePool,
    workers: usize,
    ranges: Vec<Range<u64>>,
    columns: &Arc<[String]>,
    residual: Option<&MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    scan_fields: &Arc<[(String, DType)]>,
    transforms: &Arc<Vec<ParallelAggregateTransform>>,
    scan_rows: usize,
    batch_rows: usize,
    prepared: &PreparedVortexRelational,
    context: &NativeExecutionContext<'_>,
    on_summary: &mut dyn FnMut(scan::ScanBatchCounts, u64) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let child_flag = Arc::new(AtomicBool::new(false));
    let child_token =
        CancellationToken::from_shared_flag_with_parent(child_flag, context.cancellation());
    let scope = prepared
        .session
        .worker_execution_scope(context, child_token.clone())?;
    let _worker_limit = pool.scope_active_worker_limit(workers);
    let mut active: VecDeque<StreamTask<u64>> = VecDeque::with_capacity(ranges.len());
    let shared_filters: Arc<[ProbeRuntimeFilter]> = Arc::from(probe_filters);
    for range in ranges {
        let lease = match context.memory().reserve(TASK_RESERVATION_BYTES) {
            Ok(lease) => lease,
            Err(error) => {
                teardown_ordered_streams(&child_token, None, None, active);
                return Err(error);
            }
        };
        let (tx, rx) = mpsc::sync_channel::<Result<ArrayRef>>(BOUNDED_CHANNEL_DEPTH);
        let task_scope = scope.clone();
        let task_file = file.clone();
        let task_plan = plan.clone();
        let task_columns = Arc::clone(columns);
        let task_residual = residual.cloned();
        let task_filters = Arc::clone(&shared_filters);
        let task_fields = Arc::clone(scan_fields);
        let task_transforms = Arc::clone(transforms);
        let cancel = child_token.clone();
        let submitted = pool.submit(
            Budgeted::new(
                move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                    task_scope.with_context(|wctx| {
                        let mut counts = scan::ScanBatchCounts::default();
                        let res = run_vortex_file_worker_batches(
                            &task_file,
                            &task_plan,
                            range,
                            &task_columns,
                            task_residual.as_ref(),
                            &task_filters,
                            &task_fields,
                            scan_rows,
                            batch_rows,
                            wctx,
                            &mut counts,
                            &mut |array| {
                                apply_parallel_transforms(
                                    &task_transforms,
                                    0,
                                    array,
                                    wctx,
                                    &mut |final_batch| {
                                        if tx.send(Ok(final_batch)).is_err() {
                                            return Err(failed(
                                                "parallel scan consumer stopped early",
                                            ));
                                        }
                                        Ok(())
                                    },
                                )
                            },
                        );
                        match res {
                            Ok(delivered) => Ok((counts, delivered)),
                            Err(error) => {
                                let _ = tx.send(Err(failed(&error.to_string())));
                                Err(error)
                            }
                        }
                    })
                },
                lease,
            ),
            cancel,
        );
        match submitted {
            Ok(task) => active.push_back((rx, task)),
            Err(error) => {
                teardown_ordered_streams(&child_token, Some(rx), None, active);
                return Err(error);
            }
        }
    }
    drain_ordered_streams(&child_token, active, context, on_summary, consume)
}

#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_arguments)]
fn stream_owned_array_ranges(
    owned: &crate::owned_array_source::OwnedArraySource,
    plan: &LocalVortexScanPlan,
    pool: &ComputePool,
    workers: usize,
    ranges: Vec<Range<u64>>,
    columns: &Arc<[String]>,
    residual: Option<&MaterializedPredicateEvaluator>,
    probe_filters: &[ProbeRuntimeFilter],
    scan_fields: &Arc<[(String, DType)]>,
    transforms: &Arc<Vec<ParallelAggregateTransform>>,
    scan_rows: usize,
    batch_rows: usize,
    prepared: &PreparedVortexRelational,
    context: &NativeExecutionContext<'_>,
    on_summary: &mut dyn FnMut(scan::ScanBatchCounts, u64) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let child_flag = Arc::new(AtomicBool::new(false));
    let child_token =
        CancellationToken::from_shared_flag_with_parent(child_flag, context.cancellation());
    let scope = prepared
        .session
        .worker_execution_scope(context, child_token.clone())?;
    let _worker_limit = pool.scope_active_worker_limit(workers);
    let mut active: VecDeque<StreamTask<u64>> = VecDeque::with_capacity(ranges.len());
    let shared_filters: Arc<[ProbeRuntimeFilter]> = Arc::from(probe_filters);
    for range in ranges {
        let lease = match context.memory().reserve(TASK_RESERVATION_BYTES) {
            Ok(lease) => lease,
            Err(error) => {
                teardown_ordered_streams(&child_token, None, None, active);
                return Err(error);
            }
        };
        let (tx, rx) = mpsc::sync_channel::<Result<ArrayRef>>(BOUNDED_CHANNEL_DEPTH);
        let task_scope = scope.clone();
        let task_owned = owned.clone();
        let task_plan = plan.clone();
        let task_columns = Arc::clone(columns);
        let task_residual = residual.cloned();
        let task_filters = Arc::clone(&shared_filters);
        let task_fields = Arc::clone(scan_fields);
        let task_transforms = Arc::clone(transforms);
        let cancel = child_token.clone();
        let submitted = pool.submit(
            Budgeted::new(
                move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                    task_scope.with_context(|wctx| {
                        let mut counts = scan::ScanBatchCounts::default();
                        let res = run_owned_array_worker_batches(
                            &task_owned,
                            &task_plan,
                            range,
                            &task_columns,
                            task_residual.as_ref(),
                            &task_filters,
                            &task_fields,
                            scan_rows,
                            batch_rows,
                            wctx,
                            &mut counts,
                            &mut |array| {
                                apply_parallel_transforms(
                                    &task_transforms,
                                    0,
                                    array,
                                    wctx,
                                    &mut |final_batch| {
                                        if tx.send(Ok(final_batch)).is_err() {
                                            return Err(failed(
                                                "parallel scan consumer stopped early",
                                            ));
                                        }
                                        Ok(())
                                    },
                                )
                            },
                        );
                        match res {
                            Ok(pruned) => Ok((counts, pruned)),
                            Err(error) => {
                                let _ = tx.send(Err(failed(&error.to_string())));
                                Err(error)
                            }
                        }
                    })
                },
                lease,
            ),
            cancel,
        );
        match submitted {
            Ok(task) => active.push_back((rx, task)),
            Err(error) => {
                teardown_ordered_streams(&child_token, Some(rx), None, active);
                return Err(error);
            }
        }
    }
    drain_ordered_streams(&child_token, active, context, on_summary, consume)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn try_run_parallel_join<'a>(
    prepared: &PreparedVortexRelational,
    left: &Node,
    join: super::native_relational_join::Join<'a>,
    context: &NativeExecutionContext<'_>,
    metrics: &Metrics<'_>,
    batch_rows: usize,
    left_filters: &[ProbeRuntimeFilter],
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<Option<super::native_relational_join::Join<'a>>> {
    use crate::local_primitives::native_capacity::ReservedVec;

    let scan_rows = match metrics.file_scan_batching.get() {
        FileScanBatching::Native => scan::SCAN_ROWS,
        FileScanBatching::Output => batch_rows.min(scan::SCAN_ROWS),
    };
    let Some((chain, scan_filters)) =
        ParallelSourceChain::build_for_join(left, left_filters, batch_rows, scan_rows)
    else {
        return Ok(Some(join));
    };
    // Avoid spawning worker pools when an empty-build short-circuit applies immediately.
    match &chain.target {
        ParallelScanTarget::File {
            source_index,
            residual,
            ..
        } => {
            let source_file = &prepared.sources[*source_index];
            if residual.is_none()
                && scan::has_infallible_empty_build(&scan_filters, &mut |name| {
                    source_file.dtype().as_struct_fields_opt()?.field(name)
                })
            {
                return Ok(Some(join));
            }
            if source_file.file().row_count() < MIN_PARALLEL_ROWS || context.cpu_lanes() <= 1 {
                return Ok(Some(join));
            }
        }
        #[cfg(feature = "vortex-write")]
        ParallelScanTarget::OwnedArray {
            source_index,
            residual,
            ..
        } => {
            let owned = &prepared.owned_array_sources[*source_index].1;
            if residual.is_none()
                && scan::has_infallible_empty_build(&scan_filters, &mut |name| {
                    owned.dtype().as_struct_fields_opt()?.field(name)
                })
            {
                return Ok(Some(join));
            }
            if owned.row_count() < MIN_PARALLEL_ROWS || context.cpu_lanes() <= 1 {
                return Ok(Some(join));
            }
        }
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        ParallelScanTarget::Batch { .. } => {
            if metrics.input.is_none() || context.cpu_lanes() <= 1 {
                return Ok(Some(join));
            }
        }
    }

    let mut sealed_join = match join.try_into_sealed_parallel(&left.fields, context)? {
        Ok(sealed) => Arc::new(sealed),
        Err(join) => return Ok(Some(join)),
    };

    match &chain.target {
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        ParallelScanTarget::Batch {
            source_index,
            projection,
            mandatory_predicate,
            residual,
            columns,
            scan_fields,
            finite_float_predicate,
        } => {
            let Some(input_provider) = metrics.input else {
                unreachable!("verified above");
            };
            let transforms = Arc::new(chain.transforms);
            let mut matched_vecs = None;
            input_provider.with_execution(&mut |execution| {
                matched_vecs = batch_input::parallel::run_parallel_parquet_join(
                    execution,
                    *source_index,
                    projection,
                    mandatory_predicate.as_ref(),
                    residual.as_ref(),
                    &scan_filters,
                    columns,
                    scan_fields,
                    &transforms,
                    &sealed_join,
                    chain.scan_rows,
                    chain.batch_rows,
                    *finite_float_predicate,
                    context,
                    metrics,
                    consume,
                )?;
                Ok(())
            })?;
            if let Some(matched_vecs) = matched_vecs {
                let sealed_mut = Arc::get_mut(&mut sealed_join)
                    .ok_or_else(|| failed("parallel join workers still retain sealed state"))?;
                for m in matched_vecs {
                    sealed_mut.merge_matched(&m.values)?;
                }
                sealed_join.finish(context, batch_rows, consume)?;
                context.record_native_join_work(&sealed_join.join_work()?)?;
                return Ok(None);
            }
            // Declined inside batch_parallel (e.g., tight headroom, single RG, replay).
            // Run remaining left stream directly through `consume_sealed`!
            let sealed_mut = Arc::get_mut(&mut sealed_join)
                .ok_or_else(|| failed("parallel join state unexpectedly shared"))?;
            let mut local_matched = sealed_mut.new_worker_matched(context.memory())?;
            prepared.run_with_probe_filters(
                left,
                context,
                metrics,
                batch_rows,
                None,
                left_filters,
                &mut |array| {
                    sealed_mut
                        .consume_sealed(
                            array,
                            local_matched.as_mut().map(|v| v.values.as_mut_slice()),
                            context,
                            batch_rows,
                            consume,
                        )
                        .map(|_| ())
                },
            )?;
            if let Some(m) = local_matched {
                sealed_mut.merge_matched(&m.values)?;
            }
            sealed_join.finish(context, batch_rows, consume)?;
            context.record_native_join_work(&sealed_join.join_work()?)?;
            Ok(None)
        }
        ParallelScanTarget::File {
            source_index,
            plan,
            columns,
            scan_fields,
            residual,
            finite_float_predicate,
        } => {
            let source_file = &prepared.sources[*source_index];
            let Some((pool, workers, ranges)) =
                admit_vortex_parallel(source_file.file().row_count(), prepared, context)?
            else {
                let sealed_mut = Arc::get_mut(&mut sealed_join)
                    .ok_or_else(|| failed("parallel join state unexpectedly shared"))?;
                let mut local_matched = sealed_mut.new_worker_matched(context.memory())?;
                prepared.run_with_probe_filters(
                    left,
                    context,
                    metrics,
                    batch_rows,
                    None,
                    left_filters,
                    &mut |array| {
                        sealed_mut
                            .consume_sealed(
                                array,
                                local_matched.as_mut().map(|v| v.values.as_mut_slice()),
                                context,
                                batch_rows,
                                consume,
                            )
                            .map(|_| ())
                    },
                )?;
                if let Some(m) = local_matched {
                    sealed_mut.merge_matched(&m.values)?;
                }
                sealed_join.finish(context, batch_rows, consume)?;
                context.record_native_join_work(&sealed_join.join_work()?)?;
                return Ok(None);
            };
            let transforms = Arc::new(chain.transforms);
            let columns: Arc<[String]> = Arc::from(columns.as_slice());
            let scan_fields: Arc<[(String, DType)]> = Arc::from(scan_fields.as_slice());
            let shared_filters: Arc<[ProbeRuntimeFilter]> = Arc::from(scan_filters.as_slice());
            let residual = residual.clone();
            let effective_plan = resolve_file_plan(source_file, plan, &shared_filters)?;

            let matched_vecs: Vec<ReservedVec<bool>> = source_file.with_admitted_native_execution(
                context,
                |file, context| {
                    let observation = super::super::native_file_scan_evidence::Observation::plan(
                        source_file,
                        &effective_plan,
                        NativeFileScanRoute::RelationalScan,
                        context,
                    )?;
                    let pruning_checks = u64::from(effective_plan.filter.is_some());
                    add(&metrics.scans_started, 1)?;
                    if let Some(filter) = &effective_plan.filter {
                        let excluded =
                            crate::local_primitives::file_pruning::can_prune_file_with_predicate(
                                file,
                                filter,
                                effective_plan.pushdown_predicate.as_ref(),
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
                            observation.complete(source_file, context, 0, 0, 1, 1)?;
                            return Ok(Vec::new());
                        }
                    }
                    add(&metrics.data_scans, 1)?;
                    let child_flag = Arc::new(AtomicBool::new(false));
                    let child_token = CancellationToken::from_shared_flag_with_parent(
                        child_flag,
                        context.cancellation(),
                    );
                    let scope = prepared
                        .session
                        .worker_execution_scope(context, child_token.clone())?;
                    let _worker_limit = pool.scope_active_worker_limit(workers);
                    let mut active: VecDeque<
                        StreamTask<(u64, Option<ReservedVec<bool>>)>,
                    > = VecDeque::with_capacity(ranges.len());
                    for range in ranges {
                        let lease = match context.memory().reserve(TASK_RESERVATION_BYTES) {
                            Ok(lease) => lease,
                            Err(error) => {
                                teardown_ordered_streams(&child_token, None, None, active);
                                return Err(error);
                            }
                        };
                        let (tx, rx) =
                            mpsc::sync_channel::<Result<ArrayRef>>(BOUNDED_CHANNEL_DEPTH);
                        let task_scope = scope.clone();
                        let task_file = file.clone();
                        let task_plan = effective_plan.clone();
                        let task_columns = Arc::clone(&columns);
                        let task_residual = residual.clone();
                        let task_filters = Arc::clone(&shared_filters);
                        let task_fields = Arc::clone(&scan_fields);
                        let task_transforms = Arc::clone(&transforms);
                        let task_join = Arc::clone(&sealed_join);
                        let cancel = child_token.clone();
                        let submitted = pool.submit(
                            Budgeted::new(
                                move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                                    task_scope.with_context(|wctx| {
                                        let mut counts = scan::ScanBatchCounts::default();
                                        let mut worker_matched =
                                            task_join.new_worker_matched(wctx.memory())?;
                                        let res = run_vortex_file_worker_batches(
                                            &task_file,
                                            &task_plan,
                                            range,
                                            &task_columns,
                                            task_residual.as_ref(),
                                            &task_filters,
                                            &task_fields,
                                            scan_rows,
                                            batch_rows,
                                            wctx,
                                            &mut counts,
                                            &mut |array| {
                                                apply_parallel_transforms(
                                                    &task_transforms,
                                                    0,
                                                    array,
                                                    wctx,
                                                    &mut |final_batch| {
                                                        task_join.consume_sealed(
                                                            final_batch,
                                                            worker_matched
                                                                .as_mut()
                                                                .map(|v| v.values.as_mut_slice()),
                                                            wctx,
                                                            batch_rows,
                                                            &mut |out_batch| {
                                                                tx.send(Ok(out_batch)).map_err(|_| {
                                                                    failed(
                                                                        "parallel join consumer stopped early",
                                                                    )
                                                                })
                                                            },
                                                        )?;
                                                        Ok(())
                                                    },
                                                )
                                            },
                                        );
                                        match res {
                                            Ok(delivered) => Ok((counts, (delivered, worker_matched))),
                                            Err(error) => {
                                                let _ = tx.send(Err(failed(&error.to_string())));
                                                Err(error)
                                            }
                                        }
                                    })
                                },
                                lease,
                            ),
                            cancel,
                        );
                        match submitted {
                            Ok(task) => active.push_back((rx, task)),
                            Err(error) => {
                                teardown_ordered_streams(&child_token, Some(rx), None, active);
                                return Err(error);
                            }
                        }
                    }
                    let mut merged_counts = scan::ScanBatchCounts::default();
                    let mut total_delivered = 0_u64;
                    let mut collected_matched = Vec::new();
                    drain_ordered_streams(
                        &child_token,
                        active,
                        context,
                        &mut |counts, (delivered, worker_matched)| {
                            total_delivered = total_delivered
                                .checked_add(delivered)
                                .ok_or_else(|| failed("native scan delivered row count overflow"))?;
                            if let Some(m) = worker_matched {
                                collected_matched.push(m);
                            }
                            merge_scan_counts(&mut merged_counts, counts)
                        },
                        consume,
                    )?;
                    observation.complete(
                        source_file,
                        context,
                        total_delivered,
                        1,
                        pruning_checks,
                        0,
                    )?;
                    merged_counts.merge_into(metrics)?;
                    if *finite_float_predicate {
                        add(&metrics.pipeline.finite_float_scans, 1)?;
                    }
                    metrics
                        .source_workers
                        .set(metrics.source_workers.get().max(workers as u64));
                    metrics
                        .operator_workers
                        .set(metrics.operator_workers.get().max(workers as u64));
                    Ok(collected_matched)
                },
            )?;
            let sealed_mut = Arc::get_mut(&mut sealed_join)
                .ok_or_else(|| failed("parallel join workers still retain sealed state"))?;
            for m in matched_vecs {
                sealed_mut.merge_matched(&m.values)?;
            }
            sealed_join.finish(context, batch_rows, consume)?;
            context.record_native_join_work(&sealed_join.join_work()?)?;
            record_unwrapped_file_kernels(&chain.unwrapped_file_kernels, metrics, context)?;
            Ok(None)
        }
        #[cfg(feature = "vortex-write")]
        ParallelScanTarget::OwnedArray {
            source_index,
            plan,
            columns,
            scan_fields,
            residual,
            finite_float_predicate,
        } => {
            let owned = &prepared.owned_array_sources[*source_index].1;
            let Some((pool, workers, ranges)) =
                admit_vortex_parallel(owned.row_count(), prepared, context)?
            else {
                let sealed_mut = Arc::get_mut(&mut sealed_join)
                    .ok_or_else(|| failed("parallel join state unexpectedly shared"))?;
                let mut local_matched = sealed_mut.new_worker_matched(context.memory())?;
                prepared.run_with_probe_filters(
                    left,
                    context,
                    metrics,
                    batch_rows,
                    None,
                    left_filters,
                    &mut |array| {
                        sealed_mut
                            .consume_sealed(
                                array,
                                local_matched.as_mut().map(|v| v.values.as_mut_slice()),
                                context,
                                batch_rows,
                                consume,
                            )
                            .map(|_| ())
                    },
                )?;
                if let Some(m) = local_matched {
                    sealed_mut.merge_matched(&m.values)?;
                }
                sealed_join.finish(context, batch_rows, consume)?;
                context.record_native_join_work(&sealed_join.join_work()?)?;
                return Ok(None);
            };
            let transforms = Arc::new(chain.transforms);
            let columns: Arc<[String]> = Arc::from(columns.as_slice());
            let scan_fields: Arc<[(String, DType)]> = Arc::from(scan_fields.as_slice());
            let shared_filters: Arc<[ProbeRuntimeFilter]> = Arc::from(scan_filters.as_slice());
            let residual = residual.clone();
            let effective_plan = if let Some(pred) =
                scan::admitted_runtime_predicate(&shared_filters, owned.dtype())
            {
                scan::plan_with_runtime_predicate(plan, pred, owned.dtype())?
            } else {
                plan.clone()
            };
            add(&metrics.scans_started, 1)?;
            add(&metrics.data_scans, 1)?;
            let child_flag = Arc::new(AtomicBool::new(false));
            let child_token =
                CancellationToken::from_shared_flag_with_parent(child_flag, context.cancellation());
            let scope = prepared
                .session
                .worker_execution_scope(context, child_token.clone())?;
            let _worker_limit = pool.scope_active_worker_limit(workers);
            let mut active: VecDeque<StreamTask<(u64, Option<ReservedVec<bool>>)>> =
                VecDeque::with_capacity(ranges.len());
            for range in ranges {
                let lease = match context.memory().reserve(TASK_RESERVATION_BYTES) {
                    Ok(lease) => lease,
                    Err(error) => {
                        teardown_ordered_streams(&child_token, None, None, active);
                        return Err(error);
                    }
                };
                let (tx, rx) = mpsc::sync_channel::<Result<ArrayRef>>(BOUNDED_CHANNEL_DEPTH);
                let task_scope = scope.clone();
                let task_owned = owned.clone();
                let task_plan = effective_plan.clone();
                let task_columns = Arc::clone(&columns);
                let task_residual = residual.clone();
                let task_filters = Arc::clone(&shared_filters);
                let task_fields = Arc::clone(&scan_fields);
                let task_transforms = Arc::clone(&transforms);
                let task_join = Arc::clone(&sealed_join);
                let cancel = child_token.clone();
                let submitted = pool.submit(
                    Budgeted::new(
                        move |_worker: &WorkerContext, _lease: &mut MemoryLease| {
                            task_scope.with_context(|wctx| {
                                let mut counts = scan::ScanBatchCounts::default();
                                let mut worker_matched =
                                    task_join.new_worker_matched(wctx.memory())?;
                                let res = run_owned_array_worker_batches(
                                    &task_owned,
                                    &task_plan,
                                    range,
                                    &task_columns,
                                    task_residual.as_ref(),
                                    &task_filters,
                                    &task_fields,
                                    scan_rows,
                                    batch_rows,
                                    wctx,
                                    &mut counts,
                                    &mut |array| {
                                        apply_parallel_transforms(
                                            &task_transforms,
                                            0,
                                            array,
                                            wctx,
                                            &mut |final_batch| {
                                                task_join.consume_sealed(
                                                    final_batch,
                                                    worker_matched
                                                        .as_mut()
                                                        .map(|v| v.values.as_mut_slice()),
                                                    wctx,
                                                    batch_rows,
                                                    &mut |out_batch| {
                                                        tx.send(Ok(out_batch)).map_err(|_| {
                                                            failed(
                                                                "parallel join consumer stopped early",
                                                            )
                                                        })
                                                    },
                                                )?;
                                                Ok(())
                                            },
                                        )
                                    },
                                );
                                match res {
                                    Ok(pruned) => Ok((counts, (pruned, worker_matched))),
                                    Err(error) => {
                                        let _ = tx.send(Err(failed(&error.to_string())));
                                        Err(error)
                                    }
                                }
                            })
                        },
                        lease,
                    ),
                    cancel,
                );
                match submitted {
                    Ok(task) => active.push_back((rx, task)),
                    Err(error) => {
                        teardown_ordered_streams(&child_token, Some(rx), None, active);
                        return Err(error);
                    }
                }
            }
            let mut merged_counts = scan::ScanBatchCounts::default();
            let mut total_pruned = 0_u64;
            let mut matched_vecs = Vec::new();
            drain_ordered_streams(
                &child_token,
                active,
                context,
                &mut |counts, (pruned, worker_matched)| {
                    total_pruned += pruned;
                    if let Some(m) = worker_matched {
                        matched_vecs.push(m);
                    }
                    merge_scan_counts(&mut merged_counts, counts)
                },
                consume,
            )?;
            if effective_plan.filter.is_some() && owned.has_zone_maps() {
                let regions = owned.row_count().div_ceil(SPLIT_ROWS);
                context.record_capability_decision(
                    NativeIoCapability::RegionExclusion,
                    if total_pruned > 0 {
                        NativeIoDecisionOutcome::Executed
                    } else {
                        NativeIoDecisionOutcome::Declined
                    },
                    if total_pruned > 0 {
                        NativeIoDecisionReason::ExclusionProved
                    } else {
                        NativeIoDecisionReason::ExclusionNotProved
                    },
                    NativeIoObservedWork {
                        regions: Some(regions),
                        excluded_regions: Some(total_pruned),
                        ..NativeIoObservedWork::default()
                    },
                )?;
            }
            merged_counts.merge_into(metrics)?;
            if *finite_float_predicate {
                add(&metrics.pipeline.finite_float_scans, 1)?;
            }
            metrics
                .source_workers
                .set(metrics.source_workers.get().max(workers as u64));
            metrics
                .operator_workers
                .set(metrics.operator_workers.get().max(workers as u64));
            let sealed_mut = Arc::get_mut(&mut sealed_join)
                .ok_or_else(|| failed("parallel join workers still retain sealed state"))?;
            for m in matched_vecs {
                sealed_mut.merge_matched(&m.values)?;
            }
            sealed_join.finish(context, batch_rows, consume)?;
            context.record_native_join_work(&sealed_join.join_work()?)?;
            Ok(None)
        }
    }
}

fn record_unwrapped_file_kernels(
    reasons: &[&'static str],
    metrics: &Metrics<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    for &reason in reasons {
        metrics.pipeline.record(
            super::provider::Executed {
                report: None,
                certificate: None,
            },
            reason,
            context,
        )?;
    }
    Ok(())
}
