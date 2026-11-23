//! Bounded parallel key extraction for `DistinctRows` and `DropDuplicateRows`.

#[cfg(unix)]
use super::VortexLocalPrimitiveExecutionPolicy;
use super::{
    LocalVortexRuntime, Result, ShardLoomError, StatValue, VortexReaderBackedSplitEvidence,
    aggregate_chunk_jobs::{AggregateChunkJobs, CompletedChunk, SubmitOutcome},
    check_local_vortex_context, row_export_columns_from_chunk,
    row_export_materialized_projected_row, row_export_materialized_row_count,
    row_key_columns_from_chunk, row_key_from_key_columns, row_key_materialized_row_count,
    vortex_error,
};
use crate::{DatasetUri, resident_session::NativeExecutionContext};
#[cfg(unix)]
use shardloom_core::FloatPolicy;
#[cfg(unix)]
use shardloom_exec::live_memory::Budgeted;
use shardloom_exec::{
    compute_pool::{CancellationToken, ComputePool},
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::sync::Arc;
use vortex::{
    array::{ArrayRef, ExecutionCtx},
    layout::scan::scan_builder::ScanBuilder,
    session::VortexSession,
};

#[cfg(unix)]
use super::native_capacity::ReservedVec;
#[cfg(unix)]
use super::prepared_unary::values::NativeBatch;

const MAX_PARALLEL_DISTINCT_WINDOW: usize = 16;

fn window_limit_for(workers: usize) -> usize {
    workers
        .saturating_mul(2)
        .clamp(2, MAX_PARALLEL_DISTINCT_WINDOW)
}

fn initial_chunk_estimate(rows: usize, columns: usize, queue_byte_limit: u64) -> u64 {
    let estimate = u64::try_from(rows)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(columns.max(1)).unwrap_or(1))
        .saturating_mul(32)
        .clamp(512, 1 << 20);
    estimate.min(queue_byte_limit)
}

/// Shared bounded-window chunk submitter with FIFO backpressure drain.
/// Returns `Ok(true)` if an already-completed older chunk triggered early stop
/// before or during submission.
pub(super) fn submit_with_backpressure<T, F>(
    jobs: &mut AggregateChunkJobs<T>,
    initial_bytes: u64,
    mut make_job: impl FnMut() -> F,
    mut on_completed: impl FnMut(CompletedChunk<T>) -> Result<bool>,
) -> Result<bool>
where
    T: Send + 'static,
    F: FnOnce(&super::aggregate_chunk_jobs::ChunkWorkerContext, &mut MemoryLease) -> Result<T>
        + Send
        + 'static,
{
    let mut attempt_bytes = initial_bytes;
    loop {
        while jobs.is_full() {
            let Some(completed) = jobs.join_next()? else {
                break;
            };
            if on_completed(completed)? {
                return Ok(true);
            }
        }
        match jobs.try_submit(attempt_bytes, make_job())? {
            SubmitOutcome::Submitted(_) => return Ok(false),
            SubmitOutcome::InitialCapacityDenied(error) => {
                if let Some(completed) = jobs.join_next()? {
                    if on_completed(completed)? {
                        return Ok(true);
                    }
                    continue;
                }
                if attempt_bytes > 0 {
                    attempt_bytes = 0;
                    continue;
                }
                return Err(error);
            }
        }
    }
}

#[cfg(unix)]
pub(in crate::local_primitives) struct ExtractedUnaryChunk {
    pub(in crate::local_primitives) rows: usize,
    pub(in crate::local_primitives) dtype: String,
    pub(in crate::local_primitives) encoding: String,
    pub(in crate::local_primitives) nchildren: usize,
    pub(in crate::local_primitives) nbuffers: usize,
    pub(in crate::local_primitives) batch: NativeBatch,
    pub(in crate::local_primitives) keys: ReservedVec<Budgeted<String>>,
}

#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
fn extract_prepared_unary_chunk(
    worker: &super::aggregate_chunk_jobs::ChunkWorkerContext,
    initial_lease: &mut MemoryLease,
    chunk: &ArrayRef,
    columns: &[String],
    key_indices: &[usize],
    session: &VortexSession,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
    float_policy: FloatPolicy,
) -> Result<ExtractedUnaryChunk> {
    worker.check_cancelled()?;
    // Release the heuristic backpressure estimate before reserving exact NativeBatch
    // and string-key leases so tight memory budgets are never double-charged.
    initial_lease.resize(0)?;
    let rows = chunk.len();
    let mut batch =
        NativeBatch::new_detached(chunk, columns, session, memory, cancellation, float_policy)?;
    let keys = batch.extract_keys(key_indices, rows)?;
    Ok(ExtractedUnaryChunk {
        rows,
        dtype: chunk.dtype().to_string(),
        encoding: chunk.encoding_id().to_string(),
        nchildren: chunk.nchildren(),
        nbuffers: chunk.nbuffers(),
        batch,
        keys,
    })
}

#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
pub(in crate::local_primitives) fn run_prepared_unary(
    scan: ScanBuilder<ArrayRef>,
    columns: &[String],
    key_indices: &[usize],
    policy: &VortexLocalPrimitiveExecutionPolicy,
    context: &NativeExecutionContext<'_>,
    mut consume_chunk: impl FnMut(ExtractedUnaryChunk) -> Result<bool>,
) -> Result<usize> {
    let workers = shardloom_exec::compute_pool::bounded_cpu_parallelism(
        policy.resource_envelope.max_parallelism,
    );
    let shared_pool = context.shared_compute_pool(workers)?;
    let window_limit = window_limit_for(workers);
    let queue_byte_limit = context.memory().snapshot().limit_bytes;
    let mut jobs = AggregateChunkJobs::<ExtractedUnaryChunk>::for_operation_with_pool(
        workers,
        window_limit,
        queue_byte_limit,
        context.memory().clone(),
        Some(context.cancellation()),
        shared_pool.as_ref(),
    )?;
    let job_cancellation = jobs.cancellation().clone();
    let columns: Arc<[String]> = Arc::from(columns);
    let key_indices: Arc<[usize]> = Arc::from(key_indices);
    let session = context.native_session().clone();
    let memory = context.memory().clone();
    let float_policy = context.float_policy();
    let mut max_chunk_rows = 0usize;
    let mut early_stopped = false;

    let mut consume_completed = |completed: CompletedChunk<ExtractedUnaryChunk>| -> Result<bool> {
        completed.consume_owned(|extracted| {
            max_chunk_rows = max_chunk_rows.max(extracted.rows);
            consume_chunk(extracted)
        })
    };

    for chunk in scan
        .into_array_iter(context.runtime())
        .map_err(vortex_error)?
    {
        context.check_cancelled()?;
        let chunk = chunk.map_err(vortex_error)?;
        let estimate = initial_chunk_estimate(chunk.len(), columns.len(), queue_byte_limit);
        let cancelled_early = submit_with_backpressure(
            &mut jobs,
            estimate,
            || {
                let chunk = chunk.clone();
                let columns = Arc::clone(&columns);
                let key_indices = Arc::clone(&key_indices);
                let session = session.clone();
                let memory = memory.clone();
                let cancellation = job_cancellation.clone();
                move |worker, lease| {
                    extract_prepared_unary_chunk(
                        worker,
                        lease,
                        &chunk,
                        &columns,
                        &key_indices,
                        &session,
                        &memory,
                        &cancellation,
                        float_policy,
                    )
                }
            },
            &mut consume_completed,
        )?;
        if cancelled_early {
            early_stopped = true;
            break;
        }
    }
    if !early_stopped {
        while let Some(completed) = jobs.join_next()? {
            if consume_completed(completed)? {
                early_stopped = true;
                break;
            }
        }
    }
    if !early_stopped {
        jobs.retire()?;
    }
    Ok(max_chunk_rows)
}

struct DirectDistinctChunk {
    rows: usize,
    dtype: String,
    encoding: String,
    nchildren: usize,
    nbuffers: usize,
    keys: Vec<String>,
}

fn extract_direct_distinct_chunk(
    worker: &super::aggregate_chunk_jobs::ChunkWorkerContext,
    initial_lease: &mut MemoryLease,
    chunk: &ArrayRef,
    declared_columns: &[String],
    output_columns: &[String],
    session: &VortexSession,
    cancellation: &CancellationToken,
) -> Result<DirectDistinctChunk> {
    worker.check_cancelled()?;
    initial_lease.resize(0)?;
    let rows = chunk.len();
    let mut ctx: ExecutionCtx = vortex::array::VortexSessionExecute::create_execution_ctx(session);
    let row_key_columns =
        row_key_columns_from_chunk(chunk, declared_columns, output_columns, &mut ctx)?;
    let key_rows = if row_key_columns.is_empty() {
        rows
    } else {
        row_key_materialized_row_count(&row_key_columns)?
    };
    let mut keys = Vec::with_capacity(key_rows);
    for row_index in 0..key_rows {
        if row_index.is_multiple_of(4096) {
            cancellation.check()?;
        }
        keys.push(row_key_from_key_columns(&row_key_columns, row_index)?);
    }
    Ok(DirectDistinctChunk {
        rows,
        dtype: chunk.dtype().to_string(),
        encoding: chunk.encoding_id().to_string(),
        nchildren: chunk.nchildren(),
        nbuffers: chunk.nbuffers(),
        keys,
    })
}

pub(in crate::local_primitives) type DirectDistinctResult = (
    usize,
    rustc_hash::FxHashSet<String>,
    usize,
    Vec<VortexReaderBackedSplitEvidence>,
    usize,
);

struct DirectDistinctState<'a> {
    source_uri: &'a DatasetUri,
    source_order_limit: Option<usize>,
    seen: rustc_hash::FxHashSet<String>,
    result_row_count: usize,
    arrays_read_count: usize,
    reader_splits: Vec<VortexReaderBackedSplitEvidence>,
    max_chunk_rows: usize,
}

fn consume_direct_distinct_chunk(
    completed: CompletedChunk<DirectDistinctChunk>,
    state: &mut DirectDistinctState<'_>,
) -> Result<bool> {
    completed.consume_owned(|extracted| {
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            state.source_uri.clone(),
            state.arrays_read_count,
            extracted.rows,
            extracted.dtype,
            extracted.encoding,
            extracted.nchildren,
            extracted.nbuffers,
        )?;
        state.reader_splits.push(split);
        let remaining_limit = state
            .source_order_limit
            .map(|limit| limit.saturating_sub(state.result_row_count));
        if remaining_limit != Some(0) {
            let mut added = 0usize;
            for key in extracted.keys {
                if state.seen.insert(key) {
                    added = added.checked_add(1).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex distinct result row count overflowed usize".to_string(),
                        )
                    })?;
                    if remaining_limit.is_some_and(|limit| added >= limit) {
                        break;
                    }
                }
            }
            state.result_row_count =
                state.result_row_count.checked_add(added).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex distinct result row count overflowed usize".to_string(),
                    )
                })?;
        }
        state.max_chunk_rows = state.max_chunk_rows.max(extracted.rows);
        state.arrays_read_count += 1;
        Ok(state
            .source_order_limit
            .is_some_and(|limit| state.result_row_count >= limit))
    })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::local_primitives) fn run_direct_distinct_scan(
    scan: ScanBuilder<ArrayRef>,
    source_uri: &DatasetUri,
    declared_columns: &[String],
    output_columns: &[String],
    source_order_limit: Option<usize>,
    session: &VortexSession,
    runtime: &LocalVortexRuntime,
    memory: &LiveMemoryPool,
    workers: usize,
    shared_pool: Option<&Arc<ComputePool>>,
    context: Option<&NativeExecutionContext<'_>>,
) -> Result<DirectDistinctResult> {
    let window_limit = window_limit_for(workers);
    let queue_byte_limit = memory.snapshot().limit_bytes;
    let mut jobs = AggregateChunkJobs::<DirectDistinctChunk>::for_operation_with_pool(
        workers,
        window_limit,
        queue_byte_limit,
        memory.clone(),
        context.map(NativeExecutionContext::cancellation),
        shared_pool,
    )?;
    let job_cancellation = jobs.cancellation().clone();
    let declared_columns: Arc<[String]> = Arc::from(declared_columns);
    let output_columns: Arc<[String]> = Arc::from(output_columns);

    let mut state = DirectDistinctState {
        source_uri,
        source_order_limit,
        seen: rustc_hash::FxHashSet::default(),
        result_row_count: 0,
        arrays_read_count: 0,
        reader_splits: Vec::new(),
        max_chunk_rows: 0,
    };
    let mut early_stopped = false;

    for chunk in scan.into_array_iter(runtime).map_err(vortex_error)? {
        check_local_vortex_context(context)?;
        let chunk = chunk.map_err(vortex_error)?;
        let estimate = initial_chunk_estimate(chunk.len(), output_columns.len(), queue_byte_limit);
        let stopped = submit_with_backpressure(
            &mut jobs,
            estimate,
            || {
                let chunk = chunk.clone();
                let declared_columns = Arc::clone(&declared_columns);
                let output_columns = Arc::clone(&output_columns);
                let session = session.clone();
                let cancellation = job_cancellation.clone();
                move |worker, lease| {
                    extract_direct_distinct_chunk(
                        worker,
                        lease,
                        &chunk,
                        &declared_columns,
                        &output_columns,
                        &session,
                        &cancellation,
                    )
                }
            },
            |completed| consume_direct_distinct_chunk(completed, &mut state),
        )?;
        if stopped {
            early_stopped = true;
            break;
        }
    }
    if !early_stopped {
        while let Some(completed) = jobs.join_next()? {
            if consume_direct_distinct_chunk(completed, &mut state)? {
                early_stopped = true;
                break;
            }
        }
    }
    if !early_stopped {
        jobs.retire()?;
    }
    Ok((
        state.result_row_count,
        state.seen,
        state.arrays_read_count,
        state.reader_splits,
        state.max_chunk_rows,
    ))
}

struct DirectDropDuplicateChunk {
    chunk_rows: usize,
    dtype: String,
    encoding: String,
    nchildren: usize,
    nbuffers: usize,
    rows: Vec<(String, Vec<StatValue>)>,
}

#[allow(clippy::too_many_arguments)]
fn extract_direct_drop_duplicate_chunk(
    worker: &super::aggregate_chunk_jobs::ChunkWorkerContext,
    initial_lease: &mut MemoryLease,
    chunk: &ArrayRef,
    materialization_columns: &[String],
    declared_columns: &[String],
    key_columns: &[String],
    output_columns: &[String],
    session: &VortexSession,
    cancellation: &CancellationToken,
) -> Result<DirectDropDuplicateChunk> {
    worker.check_cancelled()?;
    initial_lease.resize(0)?;
    let chunk_rows = chunk.len();
    let mut ctx: ExecutionCtx = vortex::array::VortexSessionExecute::create_execution_ctx(session);
    let materialized_columns =
        row_export_columns_from_chunk(chunk, materialization_columns, &mut ctx)?;
    let row_key_columns =
        row_key_columns_from_chunk(chunk, declared_columns, key_columns, &mut ctx)?;
    let materialized_rows = row_key_materialized_row_count(&row_key_columns)?;
    let output_rows = row_export_materialized_row_count(&materialized_columns, chunk_rows)?;
    if output_rows != materialized_rows {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex drop_duplicates row-key and output columns had mismatched row counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut rows = Vec::with_capacity(materialized_rows);
    for row_index in 0..materialized_rows {
        if row_index.is_multiple_of(4096) {
            cancellation.check()?;
        }
        let key = row_key_from_key_columns(&row_key_columns, row_index)?;
        let row = row_export_materialized_projected_row(
            materialization_columns,
            &materialized_columns,
            output_columns,
            row_index,
        )?;
        rows.push((key, row));
    }
    Ok(DirectDropDuplicateChunk {
        chunk_rows,
        dtype: chunk.dtype().to_string(),
        encoding: chunk.encoding_id().to_string(),
        nchildren: chunk.nchildren(),
        nbuffers: chunk.nbuffers(),
        rows,
    })
}

pub(in crate::local_primitives) struct DirectDropDuplicateResult {
    pub(in crate::local_primitives) counts: rustc_hash::FxHashMap<String, usize>,
    pub(in crate::local_primitives) first_positions: rustc_hash::FxHashMap<String, usize>,
    pub(in crate::local_primitives) last_positions: rustc_hash::FxHashMap<String, usize>,
    pub(in crate::local_primitives) rows: Vec<(usize, String, Vec<StatValue>)>,
    pub(in crate::local_primitives) arrays_read_count: usize,
    pub(in crate::local_primitives) reader_splits: Vec<VortexReaderBackedSplitEvidence>,
    pub(in crate::local_primitives) max_chunk_rows: usize,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::local_primitives) fn run_direct_drop_duplicate_scan(
    scan: ScanBuilder<ArrayRef>,
    source_uri: &DatasetUri,
    materialization_columns: &[String],
    declared_columns: &[String],
    key_columns: &[String],
    output_columns: &[String],
    session: &VortexSession,
    runtime: &LocalVortexRuntime,
    memory: &LiveMemoryPool,
    workers: usize,
    shared_pool: Option<&Arc<ComputePool>>,
    context: Option<&NativeExecutionContext<'_>>,
) -> Result<DirectDropDuplicateResult> {
    let window_limit = window_limit_for(workers);
    let queue_byte_limit = memory.snapshot().limit_bytes;
    let mut jobs = AggregateChunkJobs::<DirectDropDuplicateChunk>::for_operation_with_pool(
        workers,
        window_limit,
        queue_byte_limit,
        memory.clone(),
        context.map(NativeExecutionContext::cancellation),
        shared_pool,
    )?;
    let job_cancellation = jobs.cancellation().clone();
    let materialization_columns: Arc<[String]> = Arc::from(materialization_columns);
    let declared_columns: Arc<[String]> = Arc::from(declared_columns);
    let key_columns: Arc<[String]> = Arc::from(key_columns);
    let output_columns: Arc<[String]> = Arc::from(output_columns);

    let mut counts = rustc_hash::FxHashMap::<String, usize>::default();
    let mut first_positions = rustc_hash::FxHashMap::<String, usize>::default();
    let mut last_positions = rustc_hash::FxHashMap::<String, usize>::default();
    let mut rows = Vec::<(usize, String, Vec<StatValue>)>::new();
    let mut global_index = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut max_chunk_rows = 0usize;

    let mut consume_completed =
        |completed: CompletedChunk<DirectDropDuplicateChunk>| -> Result<bool> {
            completed.consume_owned(|extracted| {
                let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
                    source_uri.clone(),
                    arrays_read_count,
                    extracted.chunk_rows,
                    extracted.dtype,
                    extracted.encoding,
                    extracted.nchildren,
                    extracted.nbuffers,
                )?;
                reader_splits.push(split);
                for (key, row) in extracted.rows {
                    *counts.entry(key.clone()).or_insert(0) += 1;
                    first_positions.entry(key.clone()).or_insert(global_index);
                    last_positions.insert(key.clone(), global_index);
                    rows.push((global_index, key, row));
                    global_index = global_index.checked_add(1).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex drop_duplicates row ordinal overflowed usize".to_string(),
                        )
                    })?;
                }
                max_chunk_rows = max_chunk_rows.max(extracted.chunk_rows);
                arrays_read_count += 1;
                Ok(false)
            })
        };

    for chunk in scan.into_array_iter(runtime).map_err(vortex_error)? {
        check_local_vortex_context(context)?;
        let chunk = chunk.map_err(vortex_error)?;
        let estimate =
            initial_chunk_estimate(chunk.len(), materialization_columns.len(), queue_byte_limit);
        let _ = submit_with_backpressure(
            &mut jobs,
            estimate,
            || {
                let chunk = chunk.clone();
                let materialization_columns = Arc::clone(&materialization_columns);
                let declared_columns = Arc::clone(&declared_columns);
                let key_columns = Arc::clone(&key_columns);
                let output_columns = Arc::clone(&output_columns);
                let session = session.clone();
                let cancellation = job_cancellation.clone();
                move |worker, lease| {
                    extract_direct_drop_duplicate_chunk(
                        worker,
                        lease,
                        &chunk,
                        &materialization_columns,
                        &declared_columns,
                        &key_columns,
                        &output_columns,
                        &session,
                        &cancellation,
                    )
                }
            },
            &mut consume_completed,
        )?;
    }
    while let Some(completed) = jobs.join_next()? {
        let _ = consume_completed(completed)?;
    }
    jobs.retire()?;
    Ok(DirectDropDuplicateResult {
        counts,
        first_positions,
        last_positions,
        rows,
        arrays_read_count,
        reader_splits,
        max_chunk_rows,
    })
}

#[cfg(all(test, feature = "vortex-write"))]
mod tests {
    use super::super::local_vortex_runtime;
    use super::super::native_flat_layout::SequentialNativeFlatLayout;
    use crate::{
        VortexDuplicateKeepPolicy, VortexLocalPrimitiveExecutionPolicy,
        VortexLocalPrimitiveExecutionStatus, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
        execute_vortex_local_primitive_with_policy,
    };
    use shardloom_core::{ColumnRef, DatasetUri};
    use shardloom_plan::ProjectionRequest;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vortex::VortexSessionDefault as _;
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

    struct ChunkedDistinctFixture(PathBuf);

    impl ChunkedDistinctFixture {
        fn new(keys: &[Option<u64>], vals: &[i64], chunk_rows: usize) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "shardloom-parallel-distinct-{}-{}",
                std::process::id(),
                NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&dir).unwrap();
            let fixture = Self(dir);
            let array = StructArray::try_new(
                FieldNames::from(["key", "val"]),
                vec![
                    PrimitiveArray::new(
                        keys.iter().map(|v| v.unwrap_or(0)).collect::<Vec<_>>(),
                        Validity::from_iter(keys.iter().map(Option::is_some)),
                    )
                    .into_array(),
                    PrimitiveArray::new(vals.to_vec(), Validity::NonNullable).into_array(),
                ],
                keys.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            let rows = array.len();
            let runtime = local_vortex_runtime(
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 2).unwrap(),
            );
            let session = VortexSession::default().with_handle(runtime.handle());
            let mut file = fs::File::create(fixture.path()).unwrap();
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
            fixture
        }

        fn path(&self) -> PathBuf {
            self.0.join("input.vortex")
        }

        fn uri(&self) -> DatasetUri {
            DatasetUri::new(self.path().display().to_string()).unwrap()
        }
    }

    impl Drop for ChunkedDistinctFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn proj(names: &[&str]) -> ProjectionRequest {
        ProjectionRequest::columns(
            names
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
        )
    }

    #[cfg(unix)]
    fn verify_distinct_sessions(
        limited_req: &VortexQueryPrimitiveRequest,
        p4: VortexLocalPrimitiveExecutionPolicy,
        expected_limited_jsonl: &str,
    ) {
        use super::super::prepared_unary::prepare_unary_in_session;
        use crate::resident_session::ResidentVortexSession;
        use shardloom_exec::compute_pool::CancellationToken;

        // Verify external-pool cancellation releases all leases and provider-worker
        // sessions preserve exact sequential parity without creating a second pool.
        let (effective, _) = p4.with_physical_policy_for_request(limited_req);
        let cancel_token = CancellationToken::default();
        let ext_session = ResidentVortexSession::for_external_cpu_pool(
            effective.resource_envelope.memory_budget_bytes,
            effective.max_parallelism,
        )
        .unwrap();
        let ext_prepared = prepare_unary_in_session(limited_req, p4, &ext_session).unwrap();
        let prepared_baseline = ext_session
            .snapshot()
            .reserved_without_source_segment_cache();
        cancel_token.cancel();
        assert!(ext_prepared.collect_jsonl(&cancel_token).is_err());
        assert_eq!(
            ext_session
                .snapshot()
                .reserved_without_source_segment_cache(),
            prepared_baseline
        );
        let ok_ext = ext_prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(ok_ext.result_jsonl.value(), expected_limited_jsonl);
        drop(ok_ext);
        drop(ext_prepared);
        assert_eq!(
            ext_session
                .snapshot()
                .reserved_without_source_segment_cache(),
            0
        );

        let prov_session = ResidentVortexSession::new(
            effective.resource_envelope.memory_budget_bytes,
            effective.max_parallelism,
        )
        .unwrap();
        assert!(prov_session.snapshot().provider_background_workers > 0);
        let prov_prepared = prepare_unary_in_session(limited_req, p4, &prov_session).unwrap();
        let prov_out = prov_prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(prov_out.result_jsonl.value(), expected_limited_jsonl);
        drop(prov_out);
        drop(prov_prepared);
        assert_eq!(
            prov_session
                .snapshot()
                .reserved_without_source_segment_cache(),
            0
        );
    }

    #[cfg(unix)]
    #[test]
    fn parallel_prepared_unary_distinct_and_drop_duplicates_parity_limit_cancel_and_memory() {
        use super::super::prepared_unary::prepare_unary;
        use shardloom_exec::compute_pool::CancellationToken;

        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for i in 0..900_u64 {
            let key = if i % 17 == 0 { None } else { Some(i % 150) };
            keys.push(key);
            vals.push(i64::try_from(i).unwrap());
        }
        let fixture = ChunkedDistinctFixture::new(&keys, &vals, 300);
        let p1 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        let p4 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
        assert_eq!(p4.max_parallelism, 4);

        let limited_req =
            VortexQueryPrimitiveRequest::distinct_rows(fixture.uri(), proj(&["key"]), None)
                .with_source_order_limit(80);
        let r1 = prepare_unary(&limited_req, p1)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let r4 = prepare_unary(&limited_req, p4)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(r4.execution.runtime.provider_background_workers, 0);
        let expected_limited_jsonl = r1.result_jsonl.value().clone();
        assert_eq!(expected_limited_jsonl, *r4.result_jsonl.value());
        assert_eq!(
            r1.execution.report.arrays_read_count,
            r4.execution.report.arrays_read_count
        );
        assert_eq!(
            r1.execution.report.reader_splits.len(),
            r4.execution.report.reader_splits.len()
        );
        drop(r1);
        drop(r4);

        for keep in [
            VortexDuplicateKeepPolicy::First,
            VortexDuplicateKeepPolicy::Last,
            VortexDuplicateKeepPolicy::AllDuplicates,
        ] {
            let req = VortexQueryPrimitiveRequest::drop_duplicate_rows(
                fixture.uri(),
                proj(&["key", "val"]),
                proj(&["key"]),
            )
            .with_duplicate_keep(keep);
            let seq = prepare_unary(&req, p1)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let par = prepare_unary(&req, p4)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(par.execution.runtime.provider_background_workers, 0);
            assert_eq!(seq.result_jsonl.value(), par.result_jsonl.value());
        }

        verify_distinct_sessions(&limited_req, p4, &expected_limited_jsonl);
    }

    #[test]
    fn parallel_direct_distinct_and_drop_duplicate_scans_match_sequential() {
        let keys = (0..900_u64)
            .map(|i| if i % 19 == 0 { None } else { Some(i % 140) })
            .collect::<Vec<_>>();
        let vals = (0..900_i64).collect::<Vec<_>>();
        let fixture = ChunkedDistinctFixture::new(&keys, &vals, 300);
        let p1 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        let p4 = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
        assert_eq!(p4.max_parallelism, 4);

        let distinct_req =
            VortexQueryPrimitiveRequest::distinct_rows(fixture.uri(), proj(&["key"]), None)
                .with_source_order_limit(75);
        let d1 = execute_vortex_local_primitive_with_policy(&distinct_req, p1).unwrap();
        let d4 = execute_vortex_local_primitive_with_policy(&distinct_req, p4).unwrap();
        assert_eq!(d1.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(d4.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert!(!d4.fallback_execution_allowed);
        assert_eq!(d1.rows_selected, d4.rows_selected);
        assert_eq!(d1.rows_projected, d4.rows_projected);
        assert_eq!(d1.arrays_read_count, d4.arrays_read_count);
        assert_eq!(d1.reader_splits.len(), d4.reader_splits.len());

        for keep in [
            VortexDuplicateKeepPolicy::First,
            VortexDuplicateKeepPolicy::Last,
            VortexDuplicateKeepPolicy::AllDuplicates,
        ] {
            let req = VortexQueryPrimitiveRequest::drop_duplicate_rows(
                fixture.uri(),
                proj(&["key", "val"]),
                proj(&["key"]),
            )
            .with_duplicate_keep(keep);
            let r1 = execute_vortex_local_primitive_with_policy(&req, p1).unwrap();
            let r4 = execute_vortex_local_primitive_with_policy(&req, p4).unwrap();
            assert_eq!(
                r1.primitive_kind,
                VortexQueryPrimitiveKind::DropDuplicateRows
            );
            assert_eq!(r1.rows_selected, r4.rows_selected);
            assert_eq!(r1.rows_projected, r4.rows_projected);
            assert_eq!(r1.result_summary, r4.result_summary);
        }
    }
}
