//! Real source values, exact coverage, operation completion and final ownership.

use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use shardloom_core::ColumnRef;
use std::{fs, path::PathBuf, sync::atomic::AtomicU64};
use vortex::array::{ArrayRef, VortexSessionExecute as _};

mod boundaries;

struct Fixture {
    directory: PathBuf,
    rows: usize,
}

impl Fixture {
    fn new(rows: usize, group_rows: usize) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "shardloom-column-coverage-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let fixture = Self { directory, rows };
        fixture.write(group_rows, 0);
        fixture
    }
    fn path(&self) -> PathBuf {
        self.directory.join("input.parquet")
    }
    fn write(&self, group_rows: usize, delta: i64) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..self.rows).map(|row| i64::try_from(row).unwrap() + delta),
                )),
                Arc::new(Int64Array::from_iter((0..self.rows).map(|row| {
                    (!row.is_multiple_of(7)).then_some(-(i64::try_from(row).unwrap()))
                }))),
                Arc::new(Float64Array::from_iter((0..self.rows).map(|row| {
                    (!row.is_multiple_of(5)).then_some(f64::from(u32::try_from(row).unwrap()) + 0.5)
                }))),
                Arc::new(StringArray::from_iter((0..self.rows).map(|row| {
                    (!row.is_multiple_of(3)).then(|| format!("row-{row}-λ"))
                }))),
            ],
        )
        .unwrap();
        self.write_batch(&batch, group_rows);
    }
    fn write_batch(&self, batch: &RecordBatch, group_rows: usize) {
        let mut writer = ArrowWriter::try_new(
            fs::File::create(self.path()).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(group_rows))
                    .build(),
            ),
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn demand(names: &[&str], prefix: Option<u64>) -> SourceDemand {
    SourceDemand {
        projection: ProjectionRequest::Columns(
            names
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
        ),
        max_rows: prefix,
        ..SourceDemand::default()
    }
}

fn validate(
    array: &ArrayRef,
    first: u64,
    names: &[&str],
    delta: i64,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut execution = context.native_session().create_execution_ctx();
    for &name in names {
        let field = crate::local_primitives::logical_field_from_native_array(array, name)?;
        for offset in 0..array.len() {
            let row = usize::try_from(first).unwrap() + offset;
            let value = field
                .execute_scalar(offset, &mut execution)
                .map_err(failed)?;
            match name {
                "a" => assert_eq!(
                    value.as_primitive().as_::<i64>(),
                    Some(i64::try_from(row).unwrap() + delta)
                ),
                "b" => assert_eq!(
                    value.as_primitive().as_::<i64>(),
                    (!row.is_multiple_of(7)).then_some(-(i64::try_from(row).unwrap()))
                ),
                "c" => assert_eq!(
                    value.as_primitive().as_::<f64>(),
                    (!row.is_multiple_of(5))
                        .then_some(f64::from(u32::try_from(row).unwrap()) + 0.5)
                ),
                "text" => assert_eq!(
                    value.as_utf8().value().map(|text| text.as_str().to_owned()),
                    (!row.is_multiple_of(3)).then(|| format!("row-{row}-λ"))
                ),
                _ => panic!("unknown fixture field"),
            }
        }
    }
    Ok(())
}

fn read_in(
    session: &ResidentVortexSession,
    fixture: &Fixture,
    names: &[&str],
    prefix: Option<u64>,
    delta: i64,
    context: &NativeExecutionContext<'_>,
    mut after_batch: impl FnMut(usize) -> Result<()>,
) -> Result<ColumnarReadReport> {
    // New metadata admission every time: pointer identity cannot provide a hit.
    let source = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        context.memory(),
        context.cancellation(),
    )?;
    let prepared = source.prepare(&demand(names, prefix), session, context.cancellation())?;
    let mut reader = prepared.open(context)?;
    let mut rows = 0;
    let mut batches = 0;
    while let Some((native, first)) = reader.next_native_located(session, context)? {
        assert_eq!(first, rows);
        let witness = native.batch_release_witness()?;
        validate(native.batch_array(context)?, first, names, delta, context)?;
        rows += native.row_count() as u64;
        drop(native);
        assert_eq!(
            witness.strong_count(),
            0,
            "delivery metadata must not become cache metadata"
        );
        batches += 1;
        after_batch(batches)?;
    }
    assert_eq!(rows, (fixture.rows as u64).min(prefix.unwrap_or(u64::MAX)));
    source.validate_generation_controlled(context.cancellation())?;
    reader.report()
}

fn read(
    session: &ResidentVortexSession,
    fixture: &Fixture,
    names: &[&str],
    prefix: Option<u64>,
) -> ColumnarReadReport {
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            read_in(session, fixture, names, prefix, 0, context, |_| Ok(()))
        })
        .unwrap()
}

#[test]
fn columnar_coverage_fresh_bindings_align_nullable_native_fragments() {
    let fixture = Fixture::new(17_003, 9_001);
    let session = ResidentVortexSession::new(512 << 20, 1).unwrap();
    let pool = session.shared_memory_pool();
    let cold = read(&session, &fixture, &["a", "b"], None);
    assert_eq!(
        (cold.rows_decoded, cold.native_coverage_field_rows),
        (17_003, 0)
    );
    let partial = read(&session, &fixture, &["b", "text"], None);
    assert_eq!(partial.rows_decoded, 17_003);
    assert_eq!(
        (
            partial.native_coverage_regions,
            partial.native_coverage_field_rows
        ),
        (2, 17_003)
    );
    let hit = read(&session, &fixture, &["text", "a", "b"], None);
    assert_eq!(
        (hit.rows_decoded, hit.row_groups_read, hit.data_read_bytes),
        (0, 0, 0)
    );
    assert_eq!(hit.native_coverage_field_rows, 17_003 * 3);
    assert!(hit.native_coverage_retained_bytes <= 32 << 20);
    drop(session);
    assert_eq!(
        pool.snapshot().reserved_bytes,
        0,
        "optional arrays must not retain the session"
    );
}

#[test]
fn columnar_coverage_prefix_does_not_publish_incomplete_regions() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let prefix = read(&session, &fixture, &["a"], Some(5));
    assert_eq!(
        (prefix.rows_decoded, prefix.native_coverage_field_rows),
        (5, 0)
    );
    let full = read(&session, &fixture, &["a"], None);
    assert_eq!(
        (full.rows_decoded, full.native_coverage_field_rows),
        (37, 0)
    );
    let reused = read(&session, &fixture, &["a"], Some(22));
    assert_eq!(
        (reused.rows_decoded, reused.native_coverage_field_rows),
        (0, 22)
    );
    let empty = read(&session, &fixture, &["a"], Some(0));
    assert_eq!(
        (
            empty.rows_decoded,
            empty.native_coverage_field_rows,
            empty.data_read_bytes
        ),
        (0, 0, 0)
    );
}

#[test]
fn columnar_coverage_failed_and_cancelled_operations_publish_nothing() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let cancelled = session.with_native_execution_context(&cancellation, |context| {
        read_in(&session, &fixture, &["a"], None, 0, context, |batch| {
            if batch == 2 {
                cancellation.cancel();
            }
            Ok(())
        })
    });
    assert!(cancelled.is_err());
    assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 0);
    let failed: Result<()> =
        session.with_native_execution_context(&CancellationToken::default(), |context| {
            let report = read_in(&session, &fixture, &["a"], None, 0, context, |_| Ok(()))?;
            assert_eq!(report.native_coverage_field_rows, 0);
            Err(failed("deliberate final-consumer failure"))
        });
    assert!(failed.is_err());
    assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 0);
    assert_eq!(
        read(&session, &fixture, &["a"], None).native_coverage_field_rows,
        0
    );
    assert_eq!(
        read(&session, &fixture, &["a"], None).native_coverage_field_rows,
        37
    );
}

#[test]
fn columnar_coverage_pressure_revokes_pending_publication() {
    let fixture = Fixture::new(17_003, 17_003);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let pool = session.shared_memory_pool();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            read_in(&session, &fixture, &["a", "c"], None, 0, context, |batch| {
                if batch == 1 {
                    pool.reclaim_optional_memory();
                }
                Ok(())
            })
        })
        .unwrap();
    let after = read(&session, &fixture, &["a", "c"], None);
    assert_eq!(
        (after.rows_decoded, after.native_coverage_field_rows),
        (17_003, 0)
    );
    assert_eq!(read(&session, &fixture, &["a", "c"], None).rows_decoded, 0);
}

#[test]
fn columnar_coverage_held_native_slice_survives_eviction_and_session_drop() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let pool = session.shared_memory_pool();
    read(&session, &fixture, &["a", "text"], None);
    let retained = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                context.memory(),
                context.cancellation(),
            )?;
            let prepared = source.prepare(
                &demand(&["a", "text"], None),
                &session,
                context.cancellation(),
            )?;
            let mut reader = prepared.open(context)?;
            let (native, first) = reader.next_native_located(&session, context)?.unwrap();
            assert_eq!(first, 0);
            let array = native.batch_array(context)?.slice(3..9).map_err(failed)?;
            pool.reclaim_optional_memory();
            validate(&array, 3, &["a", "text"], 0, context)?;
            Ok(array)
        })
        .unwrap();
    drop(session);
    assert!(
        pool.snapshot().reserved_bytes > 0,
        "retained result still owns backing and metadata"
    );
    drop(retained);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn columnar_coverage_source_replacement_and_new_session_miss() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    read(&session, &fixture, &["a"], None);
    fs::rename(fixture.path(), fixture.directory.join("old.parquet")).unwrap();
    fixture.write(17, 1_000);
    let changed = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            read_in(&session, &fixture, &["a"], None, 1_000, context, |_| Ok(()))
        })
        .unwrap();
    assert_eq!(
        (changed.rows_decoded, changed.native_coverage_field_rows),
        (37, 0)
    );
    let fresh = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let reopened = fresh
        .with_native_execution_context(&CancellationToken::default(), |context| {
            read_in(&fresh, &fixture, &["a"], None, 1_000, context, |_| Ok(()))
        })
        .unwrap();
    assert_eq!(reopened.native_coverage_field_rows, 0);
}

#[test]
fn columnar_coverage_serving_sessions_keep_independent_readers() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::with_serving_policy(
        64 << 20,
        2,
        crate::resident_session::ResidentServingPolicy::default(),
    )
    .unwrap();
    for _ in 0..2 {
        let report = read(&session, &fixture, &["a"], None);
        assert_eq!(
            (
                report.rows_decoded,
                report.native_coverage_field_rows,
                report.native_coverage_retained_bytes
            ),
            (37, 0, 0)
        );
    }
}
