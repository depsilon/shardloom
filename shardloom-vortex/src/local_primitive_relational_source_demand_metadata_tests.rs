//! Admission scales with retained metadata and one parsing region, not a
//! multiplier of every serialized statistic in the source footer.

use super::*;
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    errors::ParquetError,
    file::metadata::MetadataMemoryReservation,
};
use serde_json::json;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{io::Read, path::Path, sync::Mutex};

#[derive(Debug)]
struct RecordingAdmission {
    credit: Mutex<MemoryLease>,
    successful_sizes: Mutex<Vec<usize>>,
    cancel_after: Option<usize>,
}

impl RecordingAdmission {
    fn new(memory: &LiveMemoryPool, cancel_after: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            credit: Mutex::new(memory.reserve(0).unwrap()),
            successful_sizes: Mutex::new(Vec::new()),
            cancel_after,
        })
    }
}

impl MetadataMemoryReservation for RecordingAdmission {
    fn resize(&self, bytes: usize) -> parquet::errors::Result<()> {
        let mut sizes = self.successful_sizes.lock().unwrap();
        if self.cancel_after.is_some_and(|after| sizes.len() == after) {
            return Err(ParquetError::General("test metadata cancellation".into()));
        }
        self.credit
            .lock()
            .unwrap()
            .resize(bytes as u64)
            .map_err(|error| ParquetError::External(Box::new(error)))?;
        if bytes != 0 {
            sizes.push(bytes);
        }
        Ok(())
    }
}

fn wide_statistics(rows: usize, width: usize, group_rows: usize) -> ParquetFixture {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| i64::try_from(i).unwrap()),
            )),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 19 != 0).then(|| format!("{i:08}:{}λ", "p".repeat(width)))
            }))),
        ],
    )
    .unwrap();
    ParquetFixture::with_properties(
        batch,
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_rows))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_statistics_truncate_length(None)
            .build(),
    )
}

fn footer_bytes(path: &Path) -> u64 {
    let mut file = fs::File::open(path).unwrap();
    file.seek(SeekFrom::End(-8)).unwrap();
    let mut tail = [0; 8];
    file.read_exact(&mut tail).unwrap();
    u64::from(u32::from_le_bytes(tail[..4].try_into().unwrap()))
}

#[test]
fn parquet_metadata_admission_preserves_complete_metadata_under_the_original_ceiling() {
    for (rows, width, group_rows) in [(257, 1025, 1), (513, 1025, 2), (257, 7, 1)] {
        let fixture = wide_statistics(rows, width, group_rows);
        let reference = ArrowReaderMetadata::load(
            &fs::File::open(fixture.path()).unwrap(),
            ArrowReaderOptions::new(),
        )
        .unwrap();
        let prior_charge = footer_bytes(&fixture.path()) * 32 + 8192;
        if width > 7 {
            assert!(
                prior_charge > 16 << 20,
                "fixture must reject the old whole-footer reservation: {prior_charge}"
            );
        }
        for ceiling in [4 << 20, 16 << 20] {
            let memory = LiveMemoryPool::new(ceiling).unwrap();
            let owner = RecordingAdmission::new(&memory, None);
            let metadata = ArrowReaderMetadata::load(
                &fs::File::open(fixture.path()).unwrap(),
                ArrowReaderOptions::new().with_metadata_memory_reservation(owner.clone()),
            )
            .unwrap();
            assert_eq!(metadata.metadata(), reference.metadata());
            assert_eq!(metadata.schema(), reference.schema());
            let sizes = owner.successful_sizes.lock().unwrap();
            assert!(sizes.windows(2).any(|pair| pair[1] < pair[0]));
            assert!(memory.snapshot().reserved_bytes >= metadata.metadata().memory_size() as u64);
            assert!(memory.snapshot().peak_reserved_bytes <= ceiling);
            drop(sizes);
            drop(metadata);
            drop(owner);
            assert_eq!(memory.snapshot().reserved_bytes, 0);

            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                &memory,
                &CancellationToken::default(),
            )
            .unwrap();
            assert!(memory.snapshot().reserved_bytes < ceiling);
            assert!(memory.snapshot().peak_reserved_bytes <= ceiling);
            drop(source);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn parquet_metadata_admission_failure_cancellation_and_retry_refund_shared_credits() {
    let fixture = wide_statistics(257, 1025, 1);
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let held = memory.reserve((4 << 20) - (64 << 10)).unwrap();
    let before = memory.snapshot().reserved_bytes;
    let failure = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        &memory,
        &CancellationToken::default(),
    );
    assert!(failure.is_err());
    assert_eq!(memory.snapshot().reserved_bytes, before);
    drop(held);
    let owner = RecordingAdmission::new(&memory, Some(3));
    let failure = ArrowReaderMetadata::load(
        &fs::File::open(fixture.path()).unwrap(),
        ArrowReaderOptions::new().with_metadata_memory_reservation(owner.clone()),
    );
    assert!(
        failure
            .unwrap_err()
            .to_string()
            .contains("test metadata cancellation")
    );
    drop(owner);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(LocalColumnarSource::admit(&fixture.path(), "parquet", &memory, &cancelled).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let source = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        &memory,
        &CancellationToken::default(),
    )
    .unwrap();
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn parquet_metadata_admission_rejects_malformed_footer_without_retained_credit() {
    let fixture = wide_statistics(257, 1025, 1);
    let footer = footer_bytes(&fixture.path());
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.path())
        .unwrap();
    let start = file.metadata().unwrap().len() - 8 - footer;
    file.seek(SeekFrom::Start(start)).unwrap();
    let mut original = [0];
    file.read_exact(&mut original).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();
    file.write_all(&[0x0f]).unwrap();
    file.sync_all().unwrap();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let result = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        &memory,
        &CancellationToken::default(),
    );
    assert!(result.is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    file.seek(SeekFrom::Start(start)).unwrap();
    file.write_all(&original).unwrap();
    file.sync_all().unwrap();
    let source = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        &memory,
        &CancellationToken::default(),
    )
    .unwrap();
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn parquet_metadata_admission_keeps_complete_values_and_pruning_with_small_grants() {
    let rows = 513;
    let width = 1025;
    let fixture = wide_statistics(rows, width, 2);
    for ceiling in [4 << 20, 16 << 20] {
        for predicate in [None, Some(equal("n", 381))] {
            let mut resources = policy();
            resources.resource_envelope.memory_budget_bytes = ceiling;
            let prepared = prepare_relational_with_schema(resources, |preparation| {
                let source = LocalColumnarSource::admit(
                    &fixture.path(),
                    "parquet",
                    &preparation.shared_memory_pool(),
                    &preparation.cancellation_token(),
                )?;
                preparation.register_local_source(
                    DatasetUri::new(fixture.path().display().to_string())?,
                    source,
                )?;
                Ok(fixture.scan(&["n", "text"], predicate.clone()))
            })
            .unwrap();
            let memory = prepared.session.shared_memory_pool();
            let baseline = memory.snapshot().reserved_bytes;
            let output = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected = (0..rows)
                .filter(|&n| predicate.is_none() || n == 381)
                .map(|n| {
                    json!({
                        "n": n,
                        "text": (n % 19 != 0).then(|| format!("{n:08}:{}λ", "p".repeat(width)))
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(json_rows(&output), expected);
            if predicate.is_some() {
                let report = output.execution.input.as_ref().unwrap();
                assert!(report.columnar_row_groups_read < report.columnar_row_groups_total);
            }
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            assert!(memory.snapshot().peak_reserved_bytes <= ceiling);
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
