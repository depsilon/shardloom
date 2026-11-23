//! Bounded parallel chunk extraction and local Top-K reduction for `SortRows`.

use super::{
    SortRowCandidate, StatValue, VortexReaderBackedSplitEvidence, VortexSortTiePolicy,
    aggregate_chunk_jobs::AggregateChunkJobs, check_local_vortex_context,
    materialized_sort_row_values_by_indices, native_sort_block,
    parallel_distinct_rows::submit_with_backpressure, retain_sort_top_window,
    row_export_columns_from_chunk, row_export_materialized_row_count,
    sort_candidate_source_ordinal, sort_output_stream, vortex_error,
};
use crate::{
    DatasetUri, ShardLoomError, VortexAggregateOrderExpr, resident_session::NativeExecutionContext,
};
use shardloom_exec::{
    compute_pool::{CancellationToken, ComputePool},
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::sync::Arc;
use vortex::{
    array::ArrayRef, io::runtime::BlockingRuntime, layout::scan::scan_builder::ScanBuilder,
    session::VortexSession,
};

const MAX_PARALLEL_SORT_WINDOW: usize = 8;

pub(in crate::local_primitives) struct SortScanState<'a> {
    pub(in crate::local_primitives) candidates: &'a mut Vec<SortRowCandidate>,
    pub(in crate::local_primitives) candidate_grant: &'a mut Option<MemoryLease>,
    pub(in crate::local_primitives) selected_rows: &'a mut usize,
    pub(in crate::local_primitives) source_rows_seen: &'a mut usize,
    pub(in crate::local_primitives) source_local_rows_seen: &'a mut usize,
    pub(in crate::local_primitives) arrays_read_count: &'a mut usize,
    pub(in crate::local_primitives) reader_splits: &'a mut Vec<VortexReaderBackedSplitEvidence>,
    pub(in crate::local_primitives) max_chunk_rows: &'a mut usize,
    pub(in crate::local_primitives) topk_threshold_pruned_chunks: &'a mut usize,
    pub(in crate::local_primitives) topk_threshold_pruned_rows: &'a mut usize,
    pub(in crate::local_primitives) native_sort_work: &'a mut native_sort_block::Work,
}

struct ExtractedSortChunk {
    rows: usize,
    dtype: String,
    encoding: String,
    nchildren: usize,
    nbuffers: usize,
    scratch_bytes: u64,
    candidates: Vec<SortRowCandidate>,
    native_work: Option<native_sort_block::Work>,
}

struct SortMergeSpec<'a> {
    source_uri: &'a DatasetUri,
    candidate_order_column_indices: &'a [usize],
    order_by: &'a [VortexAggregateOrderExpr],
    tie_policy: VortexSortTiePolicy,
    retained_cap: usize,
    retention_flush_threshold: usize,
}

#[allow(clippy::too_many_arguments)]
fn extract_fallback_sort_chunk(
    lease: &mut MemoryLease,
    chunk: &ArrayRef,
    declared_columns: &[String],
    candidate_value_column_indices: &[usize],
    candidate_order_column_indices: &[usize],
    order_by: &[VortexAggregateOrderExpr],
    tie_policy: VortexSortTiePolicy,
    retained_cap: usize,
    chunk_ordinal_base: usize,
    source_partition_index: usize,
    chunk_source_base: usize,
    source_row_id_column_index: Option<usize>,
    exec_ctx: &mut vortex::array::ExecutionCtx,
    cancellation: &CancellationToken,
) -> super::Result<(u64, Vec<SortRowCandidate>)> {
    let rows = chunk.len();
    let bytes = sort_output_stream::scratch_bytes(chunk, declared_columns, exec_ctx)?;
    lease.resize(bytes)?;
    let columns = row_export_columns_from_chunk(chunk, declared_columns, exec_ctx)?;
    let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
    let mut local_candidates = Vec::with_capacity(materialized_rows);
    let mut has_nan = false;
    for row_index in 0..materialized_rows {
        if row_index.is_multiple_of(4096) {
            cancellation.check()?;
        }
        let ordinal = chunk_ordinal_base.checked_add(row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex sort selected row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let source_ordinal = sort_candidate_source_ordinal(
            &columns,
            row_index,
            chunk_source_base,
            row_index,
            source_row_id_column_index,
        )?;
        let values = materialized_sort_row_values_by_indices(
            &columns,
            row_index,
            candidate_value_column_indices,
        )?;
        if !has_nan {
            has_nan = candidate_order_column_indices.iter().any(|&index| {
                matches!(values.get(index), Some(StatValue::Float64(value)) if value.is_nan())
            });
        }
        local_candidates.push(SortRowCandidate {
            ordinal,
            source_partition_index,
            source_ordinal,
            values,
        });
    }
    if !has_nan && tie_policy != VortexSortTiePolicy::All && local_candidates.len() > retained_cap {
        retain_sort_top_window(
            &mut local_candidates,
            order_by,
            candidate_order_column_indices,
            retained_cap,
            tie_policy,
        );
    }
    Ok((bytes, local_candidates))
}

#[allow(clippy::too_many_arguments)]
fn extract_sort_chunk(
    worker: &super::aggregate_chunk_jobs::ChunkWorkerContext,
    lease: &mut MemoryLease,
    chunk: &ArrayRef,
    declared_columns: &[String],
    candidate_value_column_indices: &[usize],
    candidate_order_column_indices: &[usize],
    order_by: &[VortexAggregateOrderExpr],
    tie_policy: VortexSortTiePolicy,
    retained_cap: usize,
    chunk_ordinal_base: usize,
    source_partition_index: usize,
    chunk_source_base: usize,
    source_row_id_column_index: Option<usize>,
    allow_native_sort_block: bool,
    session: &VortexSession,
    cancellation: &CancellationToken,
) -> super::Result<ExtractedSortChunk> {
    worker.check_cancelled()?;
    let rows = chunk.len();
    let dtype = chunk.dtype().to_string();
    let encoding = chunk.encoding_id().to_string();
    let nchildren = chunk.nchildren();
    let nbuffers = chunk.nbuffers();
    let mut exec_ctx = vortex::array::VortexSessionExecute::create_execution_ctx(session);
    if allow_native_sort_block
        && let Some(prepared) = native_sort_block::prepare(
            chunk,
            declared_columns,
            tie_policy,
            retained_cap,
            &mut exec_ctx,
        )?
    {
        let bytes = prepared.scratch_bytes()?;
        lease.resize(bytes)?;
        let mut local_candidates = Vec::new();
        let work = prepared.append(
            candidate_value_column_indices,
            candidate_order_column_indices,
            order_by,
            tie_policy,
            retained_cap,
            chunk_ordinal_base,
            source_partition_index,
            chunk_source_base,
            source_row_id_column_index,
            &mut local_candidates,
        )?;
        return Ok(ExtractedSortChunk {
            rows,
            dtype,
            encoding,
            nchildren,
            nbuffers,
            scratch_bytes: bytes,
            candidates: local_candidates,
            native_work: Some(work),
        });
    }

    let (bytes, local_candidates) = extract_fallback_sort_chunk(
        lease,
        chunk,
        declared_columns,
        candidate_value_column_indices,
        candidate_order_column_indices,
        order_by,
        tie_policy,
        retained_cap,
        chunk_ordinal_base,
        source_partition_index,
        chunk_source_base,
        source_row_id_column_index,
        &mut exec_ctx,
        cancellation,
    )?;
    Ok(ExtractedSortChunk {
        rows,
        dtype,
        encoding,
        nchildren,
        nbuffers,
        scratch_bytes: bytes,
        candidates: local_candidates,
        native_work: None,
    })
}

fn merge_completed_chunk(
    extracted: ExtractedSortChunk,
    spec: &SortMergeSpec<'_>,
    state: &mut SortScanState<'_>,
) -> super::Result<()> {
    let ExtractedSortChunk {
        rows,
        dtype,
        encoding,
        nchildren,
        nbuffers,
        scratch_bytes,
        mut candidates,
        native_work,
    } = extracted;
    if let Some(grant) = state.candidate_grant.as_mut() {
        sort_output_stream::reserve_candidates(
            grant,
            state.candidates,
            scratch_bytes,
            *state.arrays_read_count,
        )?;
    }
    let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
        spec.source_uri.clone(),
        *state.arrays_read_count,
        rows,
        dtype,
        encoding,
        nchildren,
        nbuffers,
    )?;
    state.reader_splits.push(split);
    *state.selected_rows = state.selected_rows.checked_add(rows).ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex sort selected row count overflowed usize; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    if let Some(work) = native_work {
        if work.candidate_rows == 0 && rows != 0 {
            *state.topk_threshold_pruned_chunks += 1;
            *state.topk_threshold_pruned_rows += rows;
        }
        state.native_sort_work.add(&work)?;
    }
    state.candidates.append(&mut candidates);
    if spec.tie_policy != VortexSortTiePolicy::All
        && state.candidates.len() > spec.retention_flush_threshold
    {
        retain_sort_top_window(
            state.candidates,
            spec.order_by,
            spec.candidate_order_column_indices,
            spec.retained_cap,
            spec.tie_policy,
        );
    }
    *state.max_chunk_rows = (*state.max_chunk_rows).max(rows);
    *state.source_rows_seen = state.source_rows_seen.checked_add(rows).ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex sort source row count overflowed usize; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    *state.source_local_rows_seen =
        state.source_local_rows_seen.checked_add(rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex sort local source row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
    *state.arrays_read_count += 1;
    Ok(())
}

fn drain_sort_headroom(
    jobs: &mut AggregateChunkJobs<ExtractedSortChunk>,
    memory: &LiveMemoryPool,
    estimate: u64,
    spec: &SortMergeSpec<'_>,
    state: &mut SortScanState<'_>,
) -> super::Result<()> {
    // Under tight memory budgets, drain in-flight workers before queuing more
    // work so speculative worker leases do not trigger false OOM denials.
    while jobs.outstanding() > 0 {
        let snapshot = memory.snapshot();
        let headroom = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
        if headroom >= estimate.saturating_mul(2) {
            break;
        }
        let Some(completed) = jobs.join_next()? else {
            break;
        };
        let extracted = completed.consume_owned(Ok)?;
        merge_completed_chunk(extracted, spec, state)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::local_primitives) fn run_parallel_sort_chunks<R: BlockingRuntime>(
    scan: ScanBuilder<ArrayRef>,
    runtime: &R,
    session: &VortexSession,
    source_uri: &DatasetUri,
    source_partition_index: usize,
    declared_columns: &[String],
    candidate_value_column_indices: &[usize],
    candidate_order_column_indices: &[usize],
    order_by: &[VortexAggregateOrderExpr],
    tie_policy: VortexSortTiePolicy,
    retained_cap: usize,
    retention_flush_threshold: usize,
    source_row_id_column_index: Option<usize>,
    allow_native_sort_block: bool,
    workers: usize,
    shared_pool: Option<&Arc<ComputePool>>,
    memory: &LiveMemoryPool,
    context: Option<&NativeExecutionContext<'_>>,
    state: &mut SortScanState<'_>,
) -> super::Result<()> {
    let window_limit = workers.saturating_mul(2).clamp(2, MAX_PARALLEL_SORT_WINDOW);
    let queue_byte_limit = memory.snapshot().limit_bytes;
    let mut jobs = AggregateChunkJobs::<ExtractedSortChunk>::for_operation_with_pool(
        workers,
        window_limit,
        queue_byte_limit,
        memory.clone(),
        context.map(NativeExecutionContext::cancellation),
        shared_pool,
    )?;
    let job_cancellation = jobs.cancellation().clone();
    let declared_columns: Arc<[String]> = Arc::from(declared_columns);
    let candidate_value_indices: Arc<[usize]> = Arc::from(candidate_value_column_indices);
    let candidate_order_indices: Arc<[usize]> = Arc::from(candidate_order_column_indices);
    let order_by_arc: Arc<[VortexAggregateOrderExpr]> = Arc::from(order_by);
    let merge_spec = SortMergeSpec {
        source_uri,
        candidate_order_column_indices: &candidate_order_indices,
        order_by: &order_by_arc,
        tie_policy,
        retained_cap,
        retention_flush_threshold,
    };
    let mut next_ordinal = *state.selected_rows;
    let mut next_source_base = *state.source_local_rows_seen;

    for chunk in scan.into_array_iter(runtime).map_err(vortex_error)? {
        check_local_vortex_context(context)?;
        let chunk = chunk.map_err(vortex_error)?;
        let rows = chunk.len();
        let chunk_ordinal_base = next_ordinal;
        let chunk_source_base = next_source_base;
        next_ordinal = next_ordinal.checked_add(rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex sort selected row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        next_source_base = next_source_base.checked_add(rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex sort source row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let estimate = sort_output_stream::scalar_scratch_bytes(rows, declared_columns.len())
            .min(queue_byte_limit);
        drain_sort_headroom(&mut jobs, memory, estimate, &merge_spec, state)?;
        let _ = submit_with_backpressure(
            &mut jobs,
            estimate,
            || {
                let chunk = chunk.clone();
                let declared_columns = Arc::clone(&declared_columns);
                let candidate_value_indices = Arc::clone(&candidate_value_indices);
                let candidate_order_indices = Arc::clone(&candidate_order_indices);
                let order_by_arc = Arc::clone(&order_by_arc);
                let session = session.clone();
                let cancellation = job_cancellation.clone();
                move |worker, lease| {
                    extract_sort_chunk(
                        worker,
                        lease,
                        &chunk,
                        &declared_columns,
                        &candidate_value_indices,
                        &candidate_order_indices,
                        &order_by_arc,
                        tie_policy,
                        retained_cap,
                        chunk_ordinal_base,
                        source_partition_index,
                        chunk_source_base,
                        source_row_id_column_index,
                        allow_native_sort_block,
                        &session,
                        &cancellation,
                    )
                }
            },
            |completed| {
                let extracted = completed.consume_owned(Ok)?;
                merge_completed_chunk(extracted, &merge_spec, state)?;
                Ok(false)
            },
        )?;
    }
    while let Some(completed) = jobs.join_next()? {
        let extracted = completed.consume_owned(Ok)?;
        merge_completed_chunk(extracted, &merge_spec, state)?;
    }
    jobs.retire()?;
    Ok(())
}

#[cfg(all(test, feature = "vortex-write"))]
mod tests {
    use super::super::local_vortex_runtime;
    use super::super::native_flat_layout::SequentialNativeFlatLayout;
    use crate::{
        VortexAggregateOrderExpr, VortexLocalPrimitiveExecutionPolicy,
        VortexLocalPrimitiveExecutionStatus, VortexQueryPrimitiveRequest, VortexSortRowsRequest,
        VortexSortTiePolicy, execute_vortex_local_partitioned_primitive_with_policy,
        execute_vortex_local_primitive_with_policy,
    };
    use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use shardloom_plan::ProjectionRequest;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vortex::VortexSessionDefault as _;
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{
        BoolArray, DictArray, PrimitiveArray, StructArray, VarBinViewArray,
    };
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

    struct ChunkedSortFixture(Vec<PathBuf>);

    impl ChunkedSortFixture {
        fn write_wide_native(rows: usize, chunk_rows: usize, seed: i64) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "shardloom-parallel-sort-wide-{}-{}",
                std::process::id(),
                NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&dir).unwrap();
            let path = dir.join("wide.vortex");
            let mut key_vals = Vec::with_capacity(rows);
            let mut label_vals = Vec::with_capacity(rows);
            for i in 0..rows {
                let row_i64 = i64::try_from(i).unwrap();
                let k = ((row_i64 * 17 + seed) % 50) - 10;
                key_vals.push(k);
                label_vals.push(format!("lbl_{:03}", (row_i64 + seed) % 40));
            }
            let mut cols = vec![
                PrimitiveArray::new(key_vals, Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_str(label_vals).into_array(),
            ];
            for c in 0..8_i64 {
                let data = (0..rows)
                    .map(|i| seed + i64::try_from(i).unwrap() * 10 + c)
                    .collect::<Vec<_>>();
                cols.push(PrimitiveArray::new(data, Validity::NonNullable).into_array());
            }
            let array = StructArray::try_new(
                FieldNames::from([
                    "key", "label", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7",
                ]),
                cols,
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            write_chunked_file(&path, &array, chunk_rows);
            Self(vec![path, dir])
        }

        fn write_fallback_types(rows: usize, chunk_rows: usize) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "shardloom-parallel-sort-fb-{}-{}",
                std::process::id(),
                NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&dir).unwrap();
            let path = dir.join("fallback.vortex");
            let floats = (0..rows)
                .map(|i| match i % 23 {
                    5 => -0.0,
                    9 => 0.0,
                    _ => f64::from(u32::try_from((rows - i) % 31).unwrap()) + 0.25,
                })
                .collect::<Vec<_>>();
            let codes = (0..rows)
                .map(|i| u32::try_from(i % 4).unwrap())
                .collect::<Vec<_>>();
            let dict_values = VarBinViewArray::from_iter_str(["d", "b", "c", "a"]).into_array();
            let dict = DictArray::try_new(
                PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
                dict_values,
            )
            .unwrap()
            .into_array();
            let flags = BoolArray::from_iter((0..rows).map(|i| i % 2 == 0)).into_array();
            let seq = (0..rows)
                .map(|i| i64::try_from(i).unwrap())
                .collect::<Vec<_>>();
            let array = StructArray::try_new(
                FieldNames::from(["f", "d", "b", "seq"]),
                vec![
                    PrimitiveArray::new(floats, Validity::NonNullable).into_array(),
                    dict,
                    flags,
                    PrimitiveArray::new(seq, Validity::NonNullable).into_array(),
                ],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            write_chunked_file(&path, &array, chunk_rows);
            Self(vec![path, dir])
        }

        fn path(&self) -> &PathBuf {
            &self.0[0]
        }

        fn uri(&self) -> DatasetUri {
            DatasetUri::new(self.path().display().to_string()).unwrap()
        }
    }

    impl Drop for ChunkedSortFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0[1]);
        }
    }

    fn write_chunked_file(path: &PathBuf, array: &vortex::array::ArrayRef, chunk_rows: usize) {
        let rows = array.len();
        let runtime = local_vortex_runtime(
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 2).unwrap(),
        );
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut file = fs::File::create(path).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(
                rows.div_ceil(chunk_rows).max(1),
            ))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, array.dtype().clone());
        for start in (0..rows).step_by(chunk_rows) {
            writer
                .push(array.slice(start..rows.min(start + chunk_rows)).unwrap())
                .unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
    }

    fn summary_values(summary: Option<&str>) -> serde_json::Value {
        let text = summary.expect("sort result summary");
        let start = text.find('{').expect("JSON start");
        serde_json::from_str::<serde_json::Value>(&text[start..]).expect("JSON object")["values"]
            .clone()
    }

    #[test]
    fn parallel_sort_topk_single_file_parity_native_fallback_ties_and_wide_pushdown() {
        let wide = ChunkedSortFixture::write_wide_native(600, 150, 7);
        let fallback = ChunkedSortFixture::write_fallback_types(480, 120);
        let p1 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        let p4 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
        assert_eq!(p4.max_parallelism, 4);

        for tie in [VortexSortTiePolicy::First, VortexSortTiePolicy::Last] {
            let req = VortexQueryPrimitiveRequest::sort_rows(
                wide.uri(),
                ProjectionRequest::columns(vec![
                    ColumnRef::new("key").unwrap(),
                    ColumnRef::new("label").unwrap(),
                    ColumnRef::new("c0").unwrap(),
                ]),
                None,
                VortexSortRowsRequest::new(vec![
                    VortexAggregateOrderExpr::new("key", true),
                    VortexAggregateOrderExpr::new("label", false),
                ])
                .with_offset(5)
                .with_tie_policy(tie),
                12,
            );
            let r1 = execute_vortex_local_primitive_with_policy(&req, p1).unwrap();
            let r4 = execute_vortex_local_primitive_with_policy(&req, p4).unwrap();
            assert_eq!(r4.status, VortexLocalPrimitiveExecutionStatus::Executed);
            assert!(!r4.fallback_execution_allowed);
            assert_eq!(r1.rows_scanned, r4.rows_scanned);
            assert_eq!(r1.rows_selected, r4.rows_selected);
            assert_eq!(
                summary_values(r1.result_summary.as_deref()),
                summary_values(r4.result_summary.as_deref())
            );
        }

        // Wide projection + pushdown predicate -> second-pass row_idx late materialization.
        let wide_req = VortexQueryPrimitiveRequest::sort_rows(
            wide.uri(),
            ProjectionRequest::All,
            Some(PredicateExpr::Compare {
                column: ColumnRef::new("key").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(10),
            }),
            VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new("key", false)])
                .with_offset(3),
            8,
        );
        let w1 = execute_vortex_local_primitive_with_policy(&wide_req, p1).unwrap();
        let w4 = execute_vortex_local_primitive_with_policy(&wide_req, p4).unwrap();
        assert_eq!(
            summary_values(w1.result_summary.as_deref()),
            summary_values(w4.result_summary.as_deref())
        );

        // Fallback dtypes: Float64 (with NaN & +-0.0), Dict, Bool, plus TiePolicy::All.
        for (order_col, tie, limit) in [
            ("f", VortexSortTiePolicy::First, 15),
            ("d", VortexSortTiePolicy::Last, 10),
            ("b", VortexSortTiePolicy::All, 4),
        ] {
            let fb_req = VortexQueryPrimitiveRequest::sort_rows(
                fallback.uri(),
                ProjectionRequest::All,
                None,
                VortexSortRowsRequest::new(vec![
                    VortexAggregateOrderExpr::new(order_col, false),
                    VortexAggregateOrderExpr::new("seq", false),
                ])
                .with_offset(2)
                .with_tie_policy(tie),
                limit,
            );
            let f1 = execute_vortex_local_primitive_with_policy(&fb_req, p1).unwrap();
            let f4 = execute_vortex_local_primitive_with_policy(&fb_req, p4).unwrap();
            assert_eq!(f1.rows_selected, f4.rows_selected);
            assert_eq!(
                summary_values(f1.result_summary.as_deref()),
                summary_values(f4.result_summary.as_deref())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn parallel_sort_topk_partitioned_and_streaming_parity_and_memory_release() {
        use super::super::execution::execute_partitioned_in_session;
        use super::super::partitioned_native::PartitionedNativeOperation;
        use crate::resident_session::ResidentVortexSession;
        use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
        use shardloom_exec::compute_pool::CancellationToken;
        use vortex::array::VortexSessionExecute as _;

        let left = ChunkedSortFixture::write_wide_native(450, 150, 3);
        let right = ChunkedSortFixture::write_wide_native(450, 150, 11);
        let sources = vec![left.uri(), right.uri()];
        let req = VortexQueryPrimitiveRequest::sort_rows(
            left.uri(),
            ProjectionRequest::All,
            None,
            VortexSortRowsRequest::new(vec![
                VortexAggregateOrderExpr::new("key", true),
                VortexAggregateOrderExpr::new("c0", false),
            ])
            .with_offset(2),
            5,
        );
        let p1 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        let p4 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
        assert_eq!(p4.max_parallelism, 4);
        let s1 =
            execute_vortex_local_partitioned_primitive_with_policy(&req, &sources, p1).unwrap();
        let s4 =
            execute_vortex_local_partitioned_primitive_with_policy(&req, &sources, p4).unwrap();
        assert_eq!(
            summary_values(s1.result_summary.as_deref()),
            summary_values(s4.result_summary.as_deref())
        );

        let ext_policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
            ExecutionResources::from_bytes(16 << 20, 4, ExecutionResourceOrigin::ExecutionCall)
                .unwrap(),
        )
        .unwrap();
        let ext_session = ResidentVortexSession::for_external_cpu_pool(16 << 20, 4).unwrap();
        let ext_report = execute_partitioned_in_session(
            &req,
            &sources,
            ext_policy,
            &ext_session,
            &CancellationToken::default(),
        )
        .unwrap();
        assert_eq!(
            summary_values(s1.result_summary.as_deref()),
            summary_values(ext_report.result_summary.as_deref())
        );
        assert_eq!(
            ext_session
                .snapshot()
                .reserved_without_source_segment_cache(),
            0
        );

        let prepared_sources = vec![
            ext_session.prepare_file(left.path()).unwrap(),
            ext_session.prepare_file(right.path()).unwrap(),
        ];
        let op = PartitionedNativeOperation::new(
            &req,
            ext_policy,
            &sources,
            &prepared_sources,
            &CancellationToken::default(),
        )
        .unwrap();
        let mut streamed_keys = Vec::new();
        op.for_each_batch(&CancellationToken::default(), |batch, context| {
            let mut exec_ctx = context.native_session().create_execution_ctx();
            let col = super::super::logical_field_from_native_array(&batch, "key")?;
            for row in 0..batch.len() {
                let scalar = col
                    .execute_scalar(row, &mut exec_ctx)
                    .map_err(super::vortex_error)?;
                streamed_keys.push(serde_json::json!(
                    scalar.as_primitive().typed_value::<i64>().unwrap()
                ));
            }
            Ok(())
        })
        .unwrap();
        let expected_keys = summary_values(s1.result_summary.as_deref())
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["key"].clone())
            .collect::<Vec<_>>();
        assert_eq!(streamed_keys, expected_keys);
        drop(op);
        drop(prepared_sources);
        assert_eq!(
            ext_session
                .snapshot()
                .reserved_without_source_segment_cache(),
            0
        );
    }

    #[cfg(unix)]
    #[test]
    fn parallel_sort_topk_tight_memory_drains_without_leaks() {
        use super::super::execution::execute_in_session;
        use crate::resident_session::ResidentVortexSession;
        use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
        use shardloom_exec::compute_pool::CancellationToken;

        let left = ChunkedSortFixture::write_wide_native(450, 150, 3);
        let p1 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        // Tight-memory single-file execution in external-pool session must drain
        // cleanly without false OOMs or leaked leases.
        let tight_policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
            ExecutionResources::from_bytes(2 << 20, 4, ExecutionResourceOrigin::ExecutionCall)
                .unwrap(),
        )
        .unwrap();
        let tight_session = ResidentVortexSession::for_external_cpu_pool(2 << 20, 4).unwrap();
        let tight_req = VortexQueryPrimitiveRequest::sort_rows(
            left.uri(),
            ProjectionRequest::columns(vec![
                ColumnRef::new("key").unwrap(),
                ColumnRef::new("c0").unwrap(),
            ]),
            None,
            VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new("key", true)]),
            6,
        );
        let tight_report = execute_in_session(
            &tight_req,
            tight_policy,
            &tight_session,
            &CancellationToken::default(),
        )
        .unwrap();
        let tight_seq = execute_vortex_local_primitive_with_policy(&tight_req, p1).unwrap();
        assert_eq!(
            summary_values(tight_seq.result_summary.as_deref()),
            summary_values(tight_report.result_summary.as_deref())
        );
        assert_eq!(
            tight_session
                .snapshot()
                .reserved_without_source_segment_cache(),
            0
        );
    }
}
