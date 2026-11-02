//! Actual task ordering, shared-owner lifetime and failure controls.

#[path = "complete_parquet_batch_tests.rs"]
mod batch_tests;

use super::*;
use crate::universal_format_io::demanded::LocalColumnarSource;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    fs,
    io::Write as _,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "shardloom-complete-parquet-tasks-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }

    fn write(&self, name: &str, start: i64, rows: i64) -> PathBuf {
        let path = self.0.join(name);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(start..start + rows))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(2))
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn assert_task_metadata(ingest: &IngestColumnarStreamSource, parallelism: usize) {
    assert_eq!(
        ingest.source_stream_unit_hint_kind,
        "parquet_adaptive_row_group_task_count"
    );
    assert_eq!(ingest.source_stream_unit_count_hint, Some(3));
    assert_eq!(
        ingest.source_stream_unit_row_ranges.as_deref(),
        Some([(0, 128), (128, 256), (256, 260)].as_slice())
    );
    assert_eq!(
        ingest.ingest_executor_applied_parallelism,
        shardloom_exec::compute_pool::bounded_cpu_parallelism(parallelism).min(3)
    );
    assert_eq!(
        ingest
            .ingest_runtime
            .as_ref()
            .unwrap()
            .requested_parallelism(),
        parallelism
    );
}

#[test]
fn complete_parquet_tasks_preserve_single_and_collection_order_and_retained_owners() {
    for collection in [false, true] {
        let fixture = Fixture::new();
        let input = if collection {
            fixture.write("a.parquet", 0, 140);
            fixture.write("b.parquet", 140, 120);
            fixture.0.clone()
        } else {
            fixture.write("input.parquet", 0, 260)
        };
        for parallelism in [1, 2, 4] {
            let memory = LiveMemoryPool::new(16 << 20).unwrap();
            let cancellation = CancellationToken::default();
            let source = LocalColumnarSource::admit_complete(
                &input,
                "parquet",
                false,
                &memory,
                &cancellation,
            )
            .unwrap();
            let weak = Arc::downgrade(&source);
            let ingest = source
                .complete_ingest_stream_with_parallelism(
                    Some(260),
                    parallelism,
                    &memory,
                    &cancellation,
                )
                .unwrap()
                .with_prefetch(parallelism, &cancellation)
                .unwrap();
            if parallelism > 1 {
                assert_task_metadata(&ingest, parallelism);
            }
            if !collection {
                assert!(
                    ingest
                        .source_stream_policy
                        .contains("parquet_extent_row_groups=130;")
                );
                assert!(
                    ingest
                        .source_stream_policy
                        .contains("source_unit_byte_range_count=")
                );
            }
            drop(source);
            let (metadata, _, mut reader) = ingest.into_parts();
            let drivers = metadata
                .ingest_runtime
                .as_ref()
                .unwrap()
                .start_drivers()
                .unwrap();
            let mut values = Vec::new();
            let mut retained = None;
            for batch in reader.by_ref() {
                let batch = batch.unwrap();
                values.extend_from_slice(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values(),
                );
                retained = Some(batch);
            }
            assert!(reader.next().is_none());
            drop(reader);
            drop(drivers);
            drop(metadata);
            assert_eq!(values, (0..260_i64).collect::<Vec<_>>());
            assert!(weak.upgrade().is_some());
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(retained);
            assert!(weak.upgrade().is_none());
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(memory.snapshot().denied_reservations, 0);
        }
    }
}

#[test]
fn complete_parquet_tasks_reject_limits_and_credit_denial_before_consumption() {
    let fixture = Fixture::new();
    let input = fixture.write("input.parquet", 0, 260);
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let cancellation = CancellationToken::default();
    let source =
        LocalColumnarSource::admit_complete(&input, "parquet", false, &memory, &cancellation)
            .unwrap();
    let held = memory.snapshot().reserved_bytes;
    for (limit, parallelism) in [(Some(259), 4), (None, 0)] {
        assert!(
            source
                .complete_ingest_stream_with_parallelism(limit, parallelism, &memory, &cancellation)
                .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, held);
    }
    let pressure = memory
        .reserve(memory.snapshot().limit_bytes - held - 1)
        .unwrap();
    let pressured = memory.snapshot().reserved_bytes;
    assert!(
        source
            .complete_ingest_stream_with_parallelism(None, 4, &memory, &cancellation)
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, pressured);
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, held);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn complete_parquet_tasks_drain_after_cancellation_or_changed_generation() {
    for cancel in [true, false] {
        let fixture = Fixture::new();
        let input = fixture.write("input.parquet", 0, 260);
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let source =
            LocalColumnarSource::admit_complete(&input, "parquet", false, &memory, &cancellation)
                .unwrap();
        let ingest = source
            .complete_ingest_stream_with_parallelism(None, 4, &memory, &cancellation)
            .unwrap()
            .with_prefetch(4, &cancellation)
            .unwrap();
        let (metadata, _, mut reader) = ingest.into_parts();
        let first = reader.next().unwrap().unwrap();
        if cancel {
            cancellation.cancel();
        } else {
            fs::OpenOptions::new()
                .append(true)
                .open(&input)
                .unwrap()
                .write_all(b"changed")
                .unwrap();
        }
        // Reject a changed source even when a valid batch was already queued.
        let error = reader.next().unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "changed" })
        );
        assert!(reader.next().is_none());
        drop(reader);
        drop(metadata);
        drop(source);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 1]
        );
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(first);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn complete_parquet_tasks_empty_source_does_not_invent_work() {
    let fixture = Fixture::new();
    let input = fixture.write("empty.parquet", 0, 0);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let cancellation = CancellationToken::default();
    let source =
        LocalColumnarSource::admit_complete(&input, "parquet", false, &memory, &cancellation)
            .unwrap();
    let ingest = source
        .complete_ingest_stream_with_parallelism(Some(0), 4, &memory, &cancellation)
        .unwrap();
    assert_eq!(ingest.source_stream_unit_count_hint, Some(0));
    assert!(ingest.ingest_runtime.is_none());
    let (metadata, _, mut reader) = ingest.into_parts();
    assert!(reader.next().is_none());
    drop(reader);
    drop(metadata);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
