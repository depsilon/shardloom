//! Original Vortex ranges with the scalar consumer inside each admitted job.
//! Only integer arithmetic proved exact in the existing F64 accumulator may be
//! regrouped. Native batches, one source generation and one CPU grant survive.

use super::{
    AggregateValueTransform, NativeNumericAccessorWork, PredicateExpr, SimpleAggregateFunction,
    SimpleAggregateState, SimpleAggregateStates, StatValue, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveRequest, VortexReaderBackedSplitEvidence,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    aggregate_lowering::AggregateLowering,
    native_numeric_execution_ctx, vortex_error,
};
use shardloom_core::{DatasetUri, Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{ops::Range, sync::Arc, time::Instant};
use vortex::{
    array::{
        ArrayRef,
        dtype::{DType, Nullability, PType},
    },
    expr::BoundExpression,
    file::VortexFile,
    io::runtime::{BlockingRuntime, current::CurrentThreadRuntime},
    layout::{
        LayoutReaderRef, LayoutRef,
        scan::{
            repeated_scan::RepeatedScan,
            scan_builder::{ScanBuilder, referenced_field_masks},
        },
    },
    mask::Mask,
    session::VortexSession,
};

const MIN_ROWS: u64 = 131_072;
const MAX_COLUMNS: usize = 16;
const MAX_MEASURES: usize = 16;
const MAX_DEPTH: usize = 64;
const MAX_LAYOUT_NODES: usize = 262_144;
// Pinned Vortex 0.85.0 SplitBy::Layout uses this subdivision unchanged.
const PROVIDER_MAX_SPLIT_ROWS: u64 = 100_000;

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex scalar source regions {message}; no fallback execution was attempted"
    ))
}

pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    request.source_order_limit.is_none()
        && aggregate.group_by.is_empty()
        && aggregate.group_expressions.is_empty()
        && aggregate.spill.is_none()
        && aggregate.offset == 0
        && (1..=MAX_MEASURES).contains(&aggregate.measures.len())
        && request.predicate.as_ref().is_none_or(|predicate| {
            matches!(
                predicate,
                PredicateExpr::Compare {
                    value: StatValue::Int64(_) | StatValue::UInt64(_),
                    ..
                }
            )
        })
        && aggregate.measures.iter().all(|measure| {
            measure.argument_offset.is_none()
                && measure.value_transform.is_none()
                && matches!(
                    measure.function.to_ascii_lowercase().as_str(),
                    "count" | "sum" | "avg" | "min" | "max"
                )
        })
}

fn integer_magnitude(dtype: &DType) -> Option<u128> {
    let DType::Primitive(ptype, _) = dtype else {
        return None;
    };
    Some(match ptype {
        PType::I8 => 1 << 7,
        PType::I16 => 1 << 15,
        PType::I32 => 1 << 31,
        PType::I64 => 1 << 63,
        PType::U8 => u128::from(u8::MAX),
        PType::U16 => u128::from(u16::MAX),
        PType::U32 => u128::from(u32::MAX),
        PType::U64 => u128::from(u64::MAX),
        _ => return None,
    })
}

pub(super) fn request_schema_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
) -> bool {
    if !request_may_be_admitted(request) {
        return false;
    }
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return false;
    };
    let aggregate = super::required_simple_aggregate(request).expect("shape checked");
    aggregate.projected_columns().len() <= MAX_COLUMNS
        && aggregate.measures.iter().all(|measure| {
            measure.column.as_ref().map_or(
                measure.function.eq_ignore_ascii_case("count"),
                |column| {
                    fields
                        .field(column.as_str())
                        .is_some_and(|dtype| integer_magnitude(&dtype).is_some())
                },
            )
        })
        && request.predicate.as_ref().is_none_or(|predicate| {
            predicate.column().is_some_and(|column| {
                fields
                    .field(column.as_str())
                    .is_some_and(|dtype| integer_magnitude(&dtype).is_some())
            })
        })
}

fn states_admitted(
    states: &SimpleAggregateStates,
    dtype: &DType,
    columns: &[String],
    rows: u64,
) -> bool {
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return false;
    };
    columns.len() <= MAX_COLUMNS
        && (1..=MAX_MEASURES).contains(&states.states.len())
        && columns.iter().all(|column| {
            fields
                .field(column.as_str())
                .is_some_and(|dtype| integer_magnitude(&dtype).is_some())
        })
        && states.states.iter().all(|state| {
            if state.argument_offset.is_some()
                || !matches!(state.value_transform, AggregateValueTransform::Identity)
                || !state.distinct_values.is_empty()
            {
                return false;
            }
            let Some(index) = state.column_index else {
                return state.function == SimpleAggregateFunction::Count;
            };
            let Some(magnitude) = columns
                .get(index)
                .and_then(|column| fields.field(column.as_str()))
                .and_then(|dtype| integer_magnitude(&dtype))
            else {
                return false;
            };
            match state.function {
                SimpleAggregateFunction::Count
                | SimpleAggregateFunction::Min
                | SimpleAggregateFunction::Max => true,
                SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => u128::from(rows)
                    .checked_mul(magnitude)
                    .is_some_and(|bound| bound <= (1_u128 << 53)),
                SimpleAggregateFunction::CountDistinct => false,
            }
        })
}

fn known_layout(layout: &LayoutRef) -> bool {
    matches!(
        layout.encoding_id().as_ref(),
        "vortex.flat" | "vortex.chunked" | "vortex.struct" | "vortex.zoned" | "vortex.dict"
    )
}

/// Only ancestor slots are retained; a wide footer never becomes a second tree.
fn layout_node_bound(file: &VortexFile, memory: &LiveMemoryPool) -> Result<Option<usize>> {
    let bytes = (MAX_DEPTH * size_of::<(LayoutRef, usize)>()) as u64;
    let Ok(_stack_lease) = memory.reserve(bytes) else {
        return Ok(None);
    };
    let root = file.footer().layout();
    if !known_layout(root) || root.nslots() > MAX_LAYOUT_NODES {
        return Ok(None);
    }
    let mut stack = Vec::with_capacity(MAX_DEPTH);
    stack.push((Arc::clone(root), 0));
    let mut count = 1;
    while let Some((parent, slot)) = stack.last_mut() {
        if *slot == parent.nslots() {
            stack.pop();
            continue;
        }
        let child = parent.slot(*slot).map_err(vortex_error)?;
        *slot += 1;
        if let Some(child) = child {
            count += 1;
            if count > MAX_LAYOUT_NODES
                || stack.len() == MAX_DEPTH
                || child.nslots() > MAX_LAYOUT_NODES
                || !known_layout(&child)
            {
                return Ok(None);
            }
            stack.push((child, 0));
        }
    }
    Ok(Some(count))
}

fn directory_bound(nodes: usize, rows: u64) -> Result<(u64, u64)> {
    let raw = u64::try_from(nodes)
        .ok()
        .and_then(|n| n.checked_mul(2 * (MAX_COLUMNS as u64 + 1)))
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| failed("directory bound overflow"))?
        .max(128);
    let added = rows.div_ceil(PROVIDER_MAX_SPLIT_ROWS);
    let bytes = raw
        .checked_mul(5)
        .and_then(|n| added.checked_mul(3).and_then(|extra| n.checked_add(extra)))
        .and_then(|n| n.checked_mul(size_of::<u64>() as u64))
        .ok_or_else(|| failed("directory byte bound overflow"))?;
    Ok((
        bytes,
        raw.checked_add(added)
            .ok_or_else(|| failed("directory length overflow"))?,
    ))
}

struct Recipe {
    template: SimpleAggregateStates,
    columns: Vec<String>,
    session: VortexSession,
    // Declared state/column/accessor metadata only. Provider arrays and allocator
    // exclusions retain their existing owner; this is not an RSS reservation.
    partial_bytes: u64,
}

fn recipe_byte_bounds(
    states: &SimpleAggregateStates,
    columns: &[String],
    source_uri: &DatasetUri,
    window: usize,
) -> Result<(u64, u64)> {
    let text_bytes = columns
        .iter()
        .map(String::len)
        .chain(states.states.iter().map(|state| state.alias.len()))
        .try_fold(source_uri.as_str().len(), usize::checked_add)
        .ok_or_else(|| failed("recipe string bound overflow"))?;
    // Fixed-width numeric owners/accessors, at most sixteen state aliases,
    // and bounded BTreeSet column-name evidence. No distinct/string payload.
    let partial_bytes = text_bytes
        .checked_mul(16)
        .and_then(|n| {
            n.checked_add(
                MAX_COLUMNS * (size_of::<super::AggregateDirectColumnAccessor>() * 4 + 4096),
            )
        })
        .and_then(|n| {
            n.checked_add(
                MAX_MEASURES * size_of::<SimpleAggregateState>() * 2 + size_of::<Partial>() + 8192,
            )
        })
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("partial capacity bound overflow"))?;
    let recipe_bytes = partial_bytes
        .checked_add(size_of::<Regions>() as u64)
        .and_then(|n| n.checked_add(u64::try_from(window).ok()?.checked_mul(1024)?))
        .ok_or_else(|| failed("recipe capacity bound overflow"))?;
    Ok((partial_bytes, recipe_bytes))
}

struct Partial {
    range: Range<u64>,
    states: SimpleAggregateStates,
    work: NativeNumericAccessorWork,
    split: Option<Split>,
    metadata_pruned: bool,
    elapsed_nanos: u128,
}

struct Split {
    rows: usize,
    dtype: String,
    encoding: String,
    children: usize,
    buffers: usize,
}

pub(super) struct Regions {
    // Drop/drain the jobs before any scan/directory/recipe credit disappears.
    jobs: AggregateChunkJobs<Partial>,
    scan: Arc<RepeatedScan<ArrayRef>>,
    reader: LayoutReaderRef,
    filter: Option<BoundExpression>,
    runtime: CurrentThreadRuntime,
    boundaries: Arc<[u64]>,
    recipe: Arc<Recipe>,
    source_uri: DatasetUri,
    parallelism: usize,
    directory_peak_bytes: u64,
    directory_lease: MemoryLease,
    recipe_lease: MemoryLease,
}

#[derive(Default)]
pub(super) struct Report {
    pub(super) selected_rows: usize,
    pub(super) arrays: usize,
    source_rows: u64,
    submitted: u64,
    completed: u64,
    pruned: u64,
    peak_outstanding: usize,
    peak_active_workers: usize,
    parallelism: usize,
    directory_peak_bytes: u64,
    directory_retained_bytes: u64,
    recipe_bytes: u64,
    partial_bytes: u64,
    consumer_nanos: u128,
    merge_nanos: u128,
}

impl Regions {
    // Keep pre-input admission and its reservation lifetimes in one transaction.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) fn admit(
        file: &VortexFile,
        source_uri: &DatasetUri,
        request: &VortexQueryPrimitiveRequest,
        lowering: &AggregateLowering,
        states: &SimpleAggregateStates,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        runtime: CurrentThreadRuntime,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Self>> {
        if file.row_count() < MIN_ROWS
            || !request_schema_may_be_admitted(request, file.dtype())
            || lowering.residual.is_some()
            || !states_admitted(states, file.dtype(), columns, file.row_count())
        {
            return Ok(None);
        }
        let Some(nodes) = layout_node_bound(file, memory)? else {
            return Ok(None);
        };
        let (directory_peak_bytes, max_boundaries) = directory_bound(nodes, file.row_count())?;
        let Ok(mut directory_lease) = memory.reserve(directory_peak_bytes) else {
            return Ok(None);
        };
        let parallelism = policy.resource_envelope().max_parallelism;
        let window = parallelism
            .checked_mul(2)
            .ok_or_else(|| failed("window overflow"))?;
        let (partial_bytes, recipe_bytes) =
            recipe_byte_bounds(states, columns, source_uri, window)?;
        let Ok(recipe_lease) = memory.reserve(recipe_bytes) else {
            return Ok(None);
        };
        // Confirm one partial can be admitted before creating workers or input
        // requests. Later denials abort and drain; they never replay the source.
        let Ok(partial_probe) = memory.reserve(partial_bytes) else {
            return Ok(None);
        };
        drop(partial_probe);
        let reader = file.layout_reader().map_err(vortex_error)?;
        let filter = lowering
            .plan
            .filter
            .as_ref()
            .map(|expression| {
                expression
                    .optimize_recursive(file.dtype())
                    .and_then(|expression| expression.bind(file.dtype()))
                    .map_err(vortex_error)
            })
            .transpose()?;
        let projection = lowering
            .plan
            .projection
            .as_ref()
            .map(|expression| {
                expression
                    .optimize_recursive(file.dtype())
                    .and_then(|expression| expression.bind(file.dtype()))
                    .map_err(vortex_error)
            })
            .transpose()?
            .unwrap_or_else(|| BoundExpression::new_root(file.dtype().clone()));
        if referenced_field_masks(&projection, filter.as_ref())
            .map_err(vortex_error)?
            .len()
            > MAX_COLUMNS + 1
        {
            return Ok(None);
        }
        let mut scan = ScanBuilder::new(session.clone(), Arc::clone(&reader))
            .with_projection(projection)
            .with_concurrency(1);
        if let Some(filter) = &filter {
            scan = scan.with_filter(filter.clone());
        }
        let boundaries = scan.full_file_splits().map_err(vortex_error)?;
        if boundaries.len() < 3 {
            return Ok(None);
        }
        if u64::try_from(boundaries.len()).map_err(vortex_error)? > max_boundaries
            || u64::try_from(boundaries.capacity()).map_err(vortex_error)?
                > max_boundaries.saturating_mul(2)
            || boundaries.first() != Some(&0)
            || boundaries.last() != Some(&file.row_count())
            || !boundaries.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(failed(
                "provider directory exceeded its admitted capacity or coverage",
            ));
        }
        let boundaries: Arc<[u64]> = boundaries.into();
        let scan = Arc::new(
            scan.with_natural_splits(Arc::clone(&boundaries))
                .prepare()
                .map_err(vortex_error)?,
        );
        directory_lease.resize(
            (boundaries.len() as u64)
                .checked_mul(8)
                .and_then(|n| n.checked_add(64))
                .ok_or_else(|| failed("retained directory overflow"))?,
        )?;
        let recipe = Arc::new(Recipe {
            template: states.clone(),
            columns: columns.to_vec(),
            session: session.clone(),
            partial_bytes,
        });
        let token = cancellation.map_or_else(CancellationToken::default, |parent| {
            CancellationToken::from_shared_flag_with_parent(Arc::default(), parent)
        });
        let jobs = AggregateChunkJobs::with_cancellation(
            parallelism,
            window,
            memory.snapshot().limit_bytes,
            memory.clone(),
            token,
        )?;
        Ok(Some(Self {
            jobs,
            scan,
            reader,
            filter,
            runtime,
            boundaries,
            recipe,
            source_uri: source_uri.clone(),
            parallelism,
            directory_peak_bytes,
            directory_lease,
            recipe_lease,
        }))
    }

    pub(super) fn run(
        mut self,
        states: &mut SimpleAggregateStates,
        work: &mut NativeNumericAccessorWork,
        reader_splits: &mut Vec<VortexReaderBackedSplitEvidence>,
        max_chunk_rows: &mut usize,
    ) -> Result<Report> {
        let mut report = Report {
            source_rows: *self.boundaries.last().expect("validated directory"),
            parallelism: self.parallelism,
            directory_peak_bytes: self.directory_peak_bytes,
            directory_retained_bytes: self.directory_lease.bytes(),
            recipe_bytes: self.recipe_lease.bytes(),
            partial_bytes: self.recipe.partial_bytes,
            ..Report::default()
        };
        let mut next_row = 0;
        for index in 0..self.boundaries.len() - 1 {
            self.jobs.check_cancelled()?;
            if self.jobs.is_full() {
                self.merge_next(
                    states,
                    work,
                    reader_splits,
                    max_chunk_rows,
                    &mut report,
                    &mut next_row,
                )?;
            }
            let range = self.boundaries[index]..self.boundaries[index + 1];
            let recipe = Arc::clone(&self.recipe);
            let scan = Arc::clone(&self.scan);
            let reader = Arc::clone(&self.reader);
            let filter = self.filter.clone();
            let runtime = self.runtime.clone();
            let ordinal = self
                .jobs
                .submit(recipe.partial_bytes, move |context, lease| {
                    execute_region(
                        context,
                        lease,
                        &recipe,
                        &scan,
                        &reader,
                        filter.as_ref(),
                        &runtime,
                        range,
                    )
                })?;
            if ordinal != index as u64 {
                return Err(failed("submission ordinal changed"));
            }
        }
        while self.jobs.outstanding() != 0 {
            self.merge_next(
                states,
                work,
                reader_splits,
                max_chunk_rows,
                &mut report,
                &mut next_row,
            )?;
        }
        if next_row != report.source_rows {
            return Err(failed("ordered source coverage is incomplete"));
        }
        report.submitted = self.jobs.submitted();
        report.completed = self.jobs.joined();
        report.peak_outstanding = self.jobs.peak_outstanding();
        report.peak_active_workers = self
            .jobs
            .pool_snapshot()
            .map_or(0, |snapshot| snapshot.peak_active_workers);
        self.jobs.retire()?;
        Ok(report)
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_next(
        &mut self,
        states: &mut SimpleAggregateStates,
        work: &mut NativeNumericAccessorWork,
        reader_splits: &mut Vec<VortexReaderBackedSplitEvidence>,
        max_chunk_rows: &mut usize,
        report: &mut Report,
        next_row: &mut u64,
    ) -> Result<()> {
        let partial = self
            .jobs
            .join_next()?
            .ok_or_else(|| failed("missing ordered completion"))?;
        let ordinal = usize::try_from(partial.ordinal()).map_err(vortex_error)?;
        let started = Instant::now();
        partial.consume_owned(|partial| {
            if partial.range.start != *next_row || self.boundaries.get(ordinal) != Some(next_row) {
                return Err(failed("completion coverage or ordinal changed"));
            }
            *next_row = partial.range.end;
            merge_states(states, &partial.states)?;
            work.add(&partial.work)?;
            report.consumer_nanos = report
                .consumer_nanos
                .checked_add(partial.elapsed_nanos)
                .ok_or_else(|| failed("consumer clock overflow"))?;
            report.pruned += u64::from(partial.metadata_pruned);
            if let Some(split) = partial.split {
                report.selected_rows = report
                    .selected_rows
                    .checked_add(split.rows)
                    .ok_or_else(|| failed("selected row count overflow"))?;
                *max_chunk_rows = (*max_chunk_rows).max(split.rows);
                reader_splits.push(VortexReaderBackedSplitEvidence::local_scan_chunk(
                    self.source_uri.clone(),
                    report.arrays,
                    split.rows,
                    split.dtype,
                    split.encoding,
                    split.children,
                    split.buffers,
                )?);
                report.arrays += 1;
            }
            Ok(())
        })?;
        report.merge_nanos += started.elapsed().as_nanos();
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_region(
    context: &ChunkWorkerContext,
    lease: &mut MemoryLease,
    recipe: &Recipe,
    scan: &RepeatedScan<ArrayRef>,
    reader: &LayoutReaderRef,
    filter: Option<&BoundExpression>,
    runtime: &CurrentThreadRuntime,
    range: Range<u64>,
) -> Result<Partial> {
    context.check_cancelled()?;
    let started = Instant::now();
    let mut partial = Partial {
        range: range.clone(),
        states: recipe.template.clone(),
        work: NativeNumericAccessorWork::default(),
        split: None,
        metadata_pruned: false,
        elapsed_nanos: 0,
    };
    let rows = usize::try_from(range.end - range.start).map_err(vortex_error)?;
    if let Some(filter) = filter {
        let pruning = reader
            .pruning_evaluation(&range, filter, Mask::new_true(rows))
            .map_err(vortex_error)?;
        if runtime.block_on(pruning).map_err(vortex_error)?.all_false() {
            context.check_cancelled()?;
            partial.metadata_pruned = true;
            partial.elapsed_nanos = started.elapsed().as_nanos();
            return Ok(partial);
        }
    }
    context.check_cancelled()?;
    let mut tasks = scan.execute(Some(range)).map_err(vortex_error)?;
    if tasks.len() != 1 {
        return Err(failed(
            "original range did not produce exactly one native task",
        ));
    }
    let chunk = runtime
        .block_on(tasks.pop().expect("one task"))
        .map_err(vortex_error)?;
    context.check_cancelled()?;
    if let Some(chunk) = chunk {
        let mut execution = native_numeric_execution_ctx(&recipe.session);
        if !partial.states.update_direct_from_chunk(
            &chunk,
            &recipe.columns,
            None,
            &mut partial.work,
            &mut execution,
        )? {
            return Err(failed(
                "admitted integer scalar consumer declined an actual native batch",
            ));
        }
        context.check_cancelled()?;
        // Bound the rendered metadata before its allocation. Numeric Struct
        // dtypes contain only the admitted field names and fixed primitive names.
        let names = recipe.columns.iter().map(String::len).sum::<usize>();
        let dtype_bound = names
            .checked_mul(6)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| failed("dtype evidence bound overflow"))?;
        let encoding_bytes = chunk.encoding_id().as_ref().len();
        let extra = u64::try_from(
            dtype_bound
                .checked_add(encoding_bytes)
                .ok_or_else(|| failed("split metadata overflow"))?,
        )
        .map_err(vortex_error)?;
        lease.resize(
            recipe
                .partial_bytes
                .checked_add(extra)
                .ok_or_else(|| failed("partial metadata credit overflow"))?,
        )?;
        let dtype = chunk.dtype().to_string();
        let encoding = chunk.encoding_id().to_string();
        if dtype.capacity() > dtype_bound || encoding.capacity() > encoding_bytes {
            return Err(failed(
                "rendered split metadata exceeded its preallocation bound",
            ));
        }
        partial.split = Some(Split {
            rows: chunk.len(),
            dtype,
            encoding,
            children: chunk.nchildren(),
            buffers: chunk.nbuffers(),
        });
    }
    context.check_cancelled()?;
    partial.elapsed_nanos = started.elapsed().as_nanos();
    Ok(partial)
}

fn merge_states(target: &mut SimpleAggregateStates, source: &SimpleAggregateStates) -> Result<()> {
    if source.direct_distinct_updates
        || source.dictionary_arc_distinct_updates
        || source.dense_integer_distinct_preunion_updates
        || source.fused_utf8_dictionary_transform_updates
        || source.lazy_utf8_dictionary_minmax_updates
        || source.partition_distinct_completed
    {
        return Err(failed("partial contains an excluded state family"));
    }
    target.merge_preaggregated_from(source)?;
    target.direct_scalar_updates |= source.direct_scalar_updates;
    target.fused_numeric_additive_updates |= source.fused_numeric_additive_updates;
    Ok(())
}

impl Report {
    pub(super) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(vortex_error)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        object.insert("aggregate_source_region_execution".into(), true.into());
        object.insert(
            "aggregate_source_region_coverage_complete".into(),
            true.into(),
        );
        object.insert("aggregate_provider_background_workers".into(), 0.into());
        for (name, count) in [
            ("source_rows", self.source_rows),
            ("submitted", self.submitted),
            ("completed", self.completed),
            ("metadata_pruned", self.pruned),
            ("outstanding", 0),
            ("peak_outstanding", self.peak_outstanding as u64),
            ("parallelism", self.parallelism as u64),
            ("peak_active_workers", self.peak_active_workers as u64),
            ("directory_peak_bytes", self.directory_peak_bytes),
            ("directory_retained_bytes", self.directory_retained_bytes),
            ("recipe_bytes", self.recipe_bytes),
            ("partial_initial_bytes", self.partial_bytes),
        ] {
            object.insert(format!("aggregate_source_region_{name}"), count.into());
        }
        object.insert(
            "aggregate_source_region_consumer_nanos".into(),
            self.consumer_nanos.to_string().into(),
        );
        object.insert(
            "aggregate_source_region_merge_nanos".into(),
            self.merge_nanos.to_string().into(),
        );
        object.insert("aggregate_source_region_scope".into(), "original_natural_ranges;original_native_batches;integer_exact_bound;ordered_compact_partial_merge;single_cpu_grant;precredited_directory_recipe_partial;provider_allocations_keep_existing_exclusions;not_RSS_bound;no_source_replay".into());
        *summary = value.to_string();
        Ok(())
    }
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "scalar_region_workers_tests.rs"]
mod tests;
