use super::*;
use crate::resident_session::ResidentVortexSession;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::properties::WriterProperties;
use shardloom_core::{
    ColumnRef, FloatPolicy,
    native_io_decisions::{
        NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
    },
};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::Budgeted};
use shardloom_plan::{ProjectionRequest, SourceDemand};
use std::{fs, path::PathBuf, sync::mpsc};
use vortex::array::VortexSessionExecute as _;

struct ParquetFixture {
    dir: PathBuf,
    path: PathBuf,
}

impl ParquetFixture {
    fn new(name: &str, rows: usize, group_rows: usize) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "shardloom-demanded-contract-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("input.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| i64::try_from(i).unwrap()),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("row-{i:06}-{}", "x".repeat(32))),
                )),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_rows))
            .build();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self { dir, path }
    }
}

impl Drop for ParquetFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn worker_execution_scope_shares_policy_merges_delta_and_preserves_chrysalis_coverage() {
    let fixture = ParquetFixture::new("worker-scope", 2048, 512);
    let session = ResidentVortexSession::new_with_policies(
        64 << 20,
        2,
        FloatPolicy::Extended,
        shardloom_core::RecursionLimit::new(321).unwrap(),
    )
    .unwrap();
    let memory = session.shared_memory_pool();
    let parent_token = CancellationToken::default();

    session
        .with_native_execution_context(&parent_token, |ctx| {
            let worker_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let child_token = CancellationToken::from_shared_flag_with_parent(
                Arc::clone(&worker_flag),
                ctx.cancellation(),
            );
            let scope = session.worker_execution_scope(ctx, child_token)?;
            let pool = session
                .shared_compute_pool(2)?
                .expect("shared compute pool available");

            let source =
                LocalColumnarSource::admit(&fixture.path, "parquet", &memory, ctx.cancellation())?;
            let demand = SourceDemand {
                projection: ProjectionRequest::Columns(vec![
                    ColumnRef::new("k")?,
                    ColumnRef::new("v")?,
                ]),
                ..SourceDemand::default()
            };
            let prepared = source.prepare(&demand, &session, ctx.cancellation())?;

            let tasks = [(0..2), (2..4)]
                .into_iter()
                .map(|range| {
                    let s = scope.clone();
                    let p = Arc::clone(&prepared);
                    let sess = session.clone();
                    let lease = memory.reserve(4096)?;
                    pool.submit(
                        Budgeted::new(
                            move |_worker: &shardloom_exec::compute_pool::WorkerContext,
                                  _lease: &mut shardloom_exec::live_memory::MemoryLease| {
                                s.with_context(|wctx| {
                                    assert_eq!(wctx.float_policy(), FloatPolicy::Extended);
                                    assert_eq!(wctx.recursion_limit().get(), 321);
                                    wctx.record_capability_decision(
                                        NativeIoCapability::RegionExclusion,
                                        NativeIoDecisionOutcome::Executed,
                                        NativeIoDecisionReason::ExclusionProved,
                                        NativeIoObservedWork {
                                            regions: Some(2),
                                            excluded_regions: Some(0),
                                            ..NativeIoObservedWork::default()
                                        },
                                    )?;
                                    let mut reader =
                                        p.open_row_group_range(range, None, false, false, wctx)?;
                                    let mut rows = 0_u64;
                                    while let Some(batch) = reader.next_native(&sess, wctx)? {
                                        rows += batch.row_count() as u64;
                                    }
                                    let rpt = reader.report()?;
                                    Ok((rows, rpt))
                                })
                            },
                            lease,
                        ),
                        ctx.cancellation().clone(),
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            let mut total_rows = 0_u64;
            for task in tasks {
                let done = task.join()?;
                let (((rows, _rpt), delta), _lease) = done.into_parts();
                total_rows += rows;
                ctx.merge_worker_delta(delta)?;
            }
            prepared.complete_region_selection(ctx)?;
            assert_eq!(total_rows, 2048);
            ctx.finish_opened_sources()?;
            // Complete the session operation so Chrysalis retains the 2-worker published regions.
            Ok(())
        })
        .unwrap();

    // Verify a subsequent reader reuses complete Chrysalis coverage without reading Parquet rows!
    session
        .with_native_execution_context(&parent_token, |ctx| {
            let source =
                LocalColumnarSource::admit(&fixture.path, "parquet", &memory, ctx.cancellation())?;
            let demand = SourceDemand {
                projection: ProjectionRequest::Columns(vec![ColumnRef::new("k")?]),
                ..SourceDemand::default()
            };
            let prepared = source.prepare(&demand, &session, ctx.cancellation())?;
            let mut reader = prepared.open(ctx)?;
            let mut rows = 0_u64;
            while let Some(batch) = reader.next_native(&session, ctx)? {
                rows += batch.row_count() as u64;
            }
            let rpt = reader.report()?;
            assert_eq!(rows, 2048);
            assert_eq!(rpt.rows_decoded, 0);
            assert_eq!(rpt.native_coverage_field_rows, 2048);
            Ok(())
        })
        .unwrap();

    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn sparse_selection_preserves_chrysalis_integrity_and_zero_survivors() {
    let fixture = ParquetFixture::new("sparse-chrysalis", 1024, 512);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let memory = session.shared_memory_pool();
    let token = CancellationToken::default();

    session
        .with_native_execution_context(&token, |ctx| {
            let source = LocalColumnarSource::admit(&fixture.path, "parquet", &memory, &token)?;
            let demand = SourceDemand {
                projection: ProjectionRequest::Columns(vec![
                    ColumnRef::new("k")?,
                    ColumnRef::new("v")?,
                ]),
                ..SourceDemand::default()
            };
            let prepared = source.prepare(&demand, &session, &token)?;
            let mut reader = prepared.open(ctx)?;
            // Manually inspect open_decoder_with_selection on sparse selection and empty selection:
            reader.input = Some((
                0,
                ctx.local_source_reader(
                    Arc::clone(&prepared.source.files[0].identity),
                    Arc::clone(&reader.metrics),
                )?,
            ));
            let group0 = prepared.row_groups[0];
            reader.coverage = coverage::ReaderCoverage::begin_with_selection(
                &prepared,
                group0,
                u64::MAX,
                true,
                Some(2),
                ctx,
            );
            let sel = RowSelection::from(vec![
                RowSelector::skip(10),
                RowSelector::select(1),
                RowSelector::skip(500),
                RowSelector::select(1),
            ]);
            reader.open_decoder_with_selection(
                group0,
                &prepared.projection,
                Some((sel, 2)),
                ctx,
            )?;
            reader.next_group = 1;
            reader.end_group = 1;
            let mut keys = Vec::new();
            let mut exec = ctx.native_session().create_execution_ctx();
            while let Some(batch) = reader.next_native(&session, ctx)? {
                let arr = batch.batch_array(ctx)?;
                let col_k = crate::local_primitives::logical_field_from_native_array(arr, "k")?;
                for i in 0..arr.len() {
                    keys.push(
                        col_k
                            .execute_scalar(i, &mut exec)
                            .unwrap()
                            .as_primitive()
                            .as_::<i64>()
                            .unwrap(),
                    );
                }
            }
            assert_eq!(keys, vec![10, 511]);
            // Sparse read MUST NOT have published any Chrysalis coverage fragments!
            let rpt = reader.report()?;
            assert_eq!(rpt.native_coverage_field_rows, 0);
            assert_eq!(rpt.native_coverage_retained_bytes, 0);
            Ok(())
        })
        .unwrap();

    // Unfiltered subsequent read STILL decodes full 1024 rows cleanly without poisoned fragments!
    session
        .with_native_execution_context(&token, |ctx| {
            let source = LocalColumnarSource::admit(&fixture.path, "parquet", &memory, &token)?;
            let demand = SourceDemand {
                projection: ProjectionRequest::Columns(vec![
                    ColumnRef::new("k")?,
                    ColumnRef::new("v")?,
                ]),
                ..SourceDemand::default()
            };
            let prepared = source.prepare(&demand, &session, &token)?;
            let mut reader = prepared.open(ctx)?;
            let mut count = 0_u64;
            while let Some(batch) = reader.next_native(&session, ctx)? {
                count += batch.row_count() as u64;
            }
            let rpt = reader.report()?;
            assert_eq!(count, 1024);
            assert_eq!(rpt.rows_decoded, 1024);
            assert_eq!(rpt.native_coverage_field_rows, 0);
            Ok(())
        })
        .unwrap();

    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn bounded_channel_worker_makes_progress_under_single_worker_and_cancellation() {
    let fixture = ParquetFixture::new("bounded-wip", 4096, 512);
    // Modest memory budget to verify bounded channels do not buffer all extents at once.
    let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
    let memory = session.shared_memory_pool();
    let token = CancellationToken::default();

    session
        .with_native_execution_context(&token, |ctx| {
            let pool = session
                .shared_compute_pool(2)?
                .expect("compute pool available");
            let _limit_guard = pool.scope_active_worker_limit(1);
            let child_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let child_token =
                CancellationToken::from_shared_flag_with_parent(child_flag, ctx.cancellation());
            let scope = session.worker_execution_scope(ctx, child_token.clone())?;

            let source = LocalColumnarSource::admit(&fixture.path, "parquet", &memory, &token)?;
            let demand = SourceDemand {
                projection: ProjectionRequest::Columns(vec![
                    ColumnRef::new("k")?,
                    ColumnRef::new("v")?,
                ]),
                ..SourceDemand::default()
            };
            let prepared = source.prepare(&demand, &session, &token)?;
            let (tx, rx) = mpsc::sync_channel::<ResidentMemorySource>(1);
            let lease = memory.reserve(4096)?;
            let sess = session.clone();
            let p = Arc::clone(&prepared);
            let task = pool.submit(
                Budgeted::new(
                    move |_worker: &shardloom_exec::compute_pool::WorkerContext,
                          _lease: &mut shardloom_exec::live_memory::MemoryLease| {
                        scope.with_context(|wctx| {
                            let mut reader =
                                p.open_row_group_range(0..8, None, false, true, wctx)?;
                            while let Some(batch) = reader.next_native(&sess, wctx)? {
                                if tx.send(batch).is_err() {
                                    break;
                                }
                            }
                            Ok(())
                        })
                    },
                    lease,
                ),
                child_token.clone(),
            )?;

            // Slow consumer reads first batch then cancels/drops receiver early!
            let first = rx.recv().unwrap();
            assert_eq!(first.row_count(), 512);
            drop(first);
            drop(rx);
            child_token.cancel();
            let _ = task.join();
            Ok(())
        })
        .unwrap();

    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
