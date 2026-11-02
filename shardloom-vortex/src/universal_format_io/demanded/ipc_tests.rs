use super::*;
use crate::source_identity::SourceIdentity;
use arrow_array::{
    ArrayRef, DictionaryArray, Int8Array, Int64Array, NullArray, RecordBatch, StringArray,
    types::Int8Type,
};
use arrow_ipc::{
    CompressionType,
    writer::{DictionaryHandling, FileWriter, IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new(bytes: &[u8]) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-owned-ipc-{}-{}.arrow",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, bytes).unwrap();
        Self(path)
    }

    fn open(&self, pool: &LiveMemoryPool, token: &CancellationToken) -> OwnedLocalRead {
        OwnedLocalRead::metadata(
            Arc::new(SourceIdentity::capture(&self.0).unwrap()),
            pool,
            token,
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_file(&self.0).unwrap();
    }
}

fn batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("empty", DataType::Null, true),
    ]));
    [0, 32, 64]
        .map(|start| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(start..start + 32)) as ArrayRef,
                    Arc::new(StringArray::from(
                        (start..start + 32)
                            .map(|n| {
                                (n % 3 != 0).then(|| format!("{n}/λ/{}", "repeat".repeat(512)))
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(NullArray::new(32)),
                ],
            )
            .unwrap()
        })
        .to_vec()
}

fn encode(
    batches: &[RecordBatch],
    stream: bool,
    compression: Option<CompressionType>,
    delta: bool,
) -> Vec<u8> {
    let options = IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap()
        .with_dictionary_handling(if delta {
            DictionaryHandling::Delta
        } else {
            DictionaryHandling::Resend
        });
    let mut output = Vec::new();
    if stream {
        let mut writer =
            StreamWriter::try_new_with_options(&mut output, &batches[0].schema(), options).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    } else {
        let mut writer =
            FileWriter::try_new_with_options(&mut output, &batches[0].schema(), options).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    output
}

fn open(
    fixture: &Fixture,
    pool: &LiveMemoryPool,
    token: &CancellationToken,
    projection: &[usize],
) -> Reader {
    let input = fixture.open(pool, token);
    let metadata = discover(&input, pool, token).unwrap();
    Reader::new(input, metadata, projection, pool, token).unwrap()
}

#[test]
fn native_owned_ipc_file_stream_codecs_and_bufferless_projection_keep_complete_values() {
    let expected = batches();
    for compression in [
        None,
        Some(CompressionType::LZ4_FRAME),
        Some(CompressionType::ZSTD),
    ] {
        for stream in [false, true] {
            let fixture = Fixture::new(&encode(&expected, stream, compression, false));
            for projection in [vec![1, 0, 2], vec![2], vec![]] {
                let pool = LiveMemoryPool::new(32 << 20).unwrap();
                let mut reader = open(&fixture, &pool, &CancellationToken::default(), &projection);
                let mut actual = Vec::new();
                while let Some(batch) = reader.next().unwrap() {
                    actual.push(batch);
                }
                assert!(reader.next().unwrap().is_none());
                drop(reader);
                assert_eq!(actual.len(), expected.len());
                for (actual, expected) in actual.iter().zip(&expected) {
                    assert_eq!(&**actual, &expected.project(&projection).unwrap());
                }
                assert!(pool.snapshot().reserved_bytes > 0);
                drop(actual);
                assert_eq!(pool.snapshot().reserved_bytes, 0);
            }
        }
    }
}

fn dictionary(values: &[&str], keys: Vec<Option<i8>>) -> RecordBatch {
    let array = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(keys),
        Arc::new(StringArray::from(values.to_vec())),
    )
    .unwrap();
    RecordBatch::try_from_iter([("label", Arc::new(array) as ArrayRef)]).unwrap()
}

#[test]
fn native_owned_ipc_dictionary_replacement_and_deltas_survive_reader_drop() {
    let expected = vec![
        dictionary(&["alpha", "β"], vec![Some(1), None, Some(0)]),
        dictionary(&["alpha", "β", "γ"], vec![Some(2), Some(0), None]),
        dictionary(&["replacement", "tail"], vec![Some(0), None, Some(1)]),
    ];
    for delta in [false, true] {
        for compression in [
            None,
            Some(CompressionType::LZ4_FRAME),
            Some(CompressionType::ZSTD),
        ] {
            let fixture = Fixture::new(&encode(&expected, true, compression, delta));
            let pool = LiveMemoryPool::new(32 << 20).unwrap();
            let mut reader = open(&fixture, &pool, &CancellationToken::default(), &[0]);
            let mut actual = Vec::new();
            while let Some(batch) = reader.next().unwrap() {
                actual.push(batch);
            }
            drop(reader);
            for (actual, expected) in actual.iter().zip(&expected) {
                assert_eq!(&**actual, expected);
            }
            assert_eq!(actual.len(), expected.len());
            drop(actual);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
    // File dictionaries are loaded from the retained footer index.
    let fixture = Fixture::new(&encode(&expected[..1], false, None, false));
    let pool = LiveMemoryPool::new(1 << 20).unwrap();
    let mut reader = open(&fixture, &pool, &CancellationToken::default(), &[0]);
    let batch = reader.next().unwrap().unwrap();
    drop(reader);
    assert_eq!(&*batch, &expected[0]);
    drop(batch);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn native_owned_ipc_denial_cancellation_and_source_change_are_terminal_and_refund() {
    let expected = batches();
    for stream in [false, true] {
        for cause in ["denial", "cancellation", "mutation"] {
            let fixture = Fixture::new(&encode(
                &expected,
                stream,
                Some(CompressionType::ZSTD),
                false,
            ));
            let pool = LiveMemoryPool::new(8 << 20).unwrap();
            let token = CancellationToken::default();
            let mut reader = open(&fixture, &pool, &token, &[0, 1, 2]);
            let retained = reader.next().unwrap().unwrap();
            let pressure = if cause == "denial" {
                Some(
                    pool.reserve(pool.snapshot().limit_bytes - pool.snapshot().reserved_bytes)
                        .unwrap(),
                )
            } else {
                None
            };
            if cause == "cancellation" {
                token.cancel();
            }
            if cause == "mutation" {
                fs::write(&fixture.0, b"changed").unwrap();
            }
            let error = reader.next().unwrap_err();
            if cause == "denial" {
                assert!(error.to_string().contains("memory reservation denied"));
            }
            if cause == "cancellation" {
                assert_eq!(error, token.check().unwrap_err());
            }
            assert!(reader.next().unwrap().is_none());
            drop(pressure);
            drop(reader);
            assert_eq!(&*retained, &expected[0]);
            assert!(pool.snapshot().reserved_bytes > 0);
            drop(retained);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_owned_ipc_discovery_and_truncated_message_fail_without_leaked_credits() {
    let expected = batches();
    let bytes = encode(&expected, true, None, false);
    let fixture = Fixture::new(&bytes);
    let pool = LiveMemoryPool::new(4 << 20).unwrap();
    let token = CancellationToken::default();
    let input = fixture.open(&pool, &token);
    let metadata = discover(&input, &pool, &token).unwrap();
    let Layout::Stream { start } = metadata.layout else {
        unreachable!()
    };
    let (_, _, body_start) = header(&input, start).unwrap().unwrap();
    drop(metadata);
    drop(input);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    for length in [
        0,
        1,
        4,
        7,
        usize::try_from(start + 1).unwrap(),
        usize::try_from(body_start + 1).unwrap(),
    ] {
        let truncated = Fixture::new(&bytes[..length]);
        let input = truncated.open(&pool, &token);
        if let Ok(metadata) = discover(&input, &pool, &token) {
            let mut reader = Reader::new(input, metadata, &[0], &pool, &token).unwrap();
            assert!(reader.next().is_err());
            assert!(reader.next().unwrap().is_none());
        } else {
            drop(input);
        }
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
    let input = fixture.open(&pool, &token);
    let pressure = pool
        .reserve(pool.snapshot().limit_bytes - pool.snapshot().reserved_bytes)
        .unwrap();
    assert!(discover(&input, &pool, &token).is_err());
    drop(pressure);
    drop(input);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn native_owned_ipc_complete_ingestion_keeps_schema_and_payload_through_prefetch() {
    let expected = batches()
        .iter()
        .map(|batch| batch.project(&[0, 1]).unwrap())
        .collect::<Vec<_>>();
    for stream in [false, true] {
        let fixture = Fixture::new(&encode(
            &expected,
            stream,
            Some(CompressionType::ZSTD),
            false,
        ));
        for parallelism in [1, 4] {
            let pool = LiveMemoryPool::new(16 << 20).unwrap();
            let token = CancellationToken::default();
            let source = super::super::LocalColumnarSource::admit_complete(
                &fixture.0,
                "arrow-ipc",
                false,
                &pool,
                &token,
            )
            .unwrap();
            let weak = Arc::downgrade(&source);
            let ingest = source
                .complete_ingest_stream(None, &pool, &token)
                .unwrap()
                .with_prefetch(parallelism, &token)
                .unwrap();
            drop(source);
            let (metadata, _, mut reader) = ingest.into_parts();
            let first = reader.next().unwrap().unwrap();
            drop(reader);
            drop(metadata);
            assert!(weak.upgrade().is_some());
            assert_eq!(&*first, &expected[0]);
            drop(first);
            assert!(weak.upgrade().is_none());
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_owned_ipc_prefix_reports_full_physical_decode_and_zero_prefix_does_no_read() {
    let expected = batches()
        .iter()
        .map(|batch| batch.project(&[0, 1]).unwrap())
        .collect::<Vec<_>>();
    for stream in [false, true] {
        let fixture = Fixture::new(&encode(&expected, stream, None, false));
        for limit in [0, 2] {
            let pool = LiveMemoryPool::new(16 << 20).unwrap();
            let token = CancellationToken::default();
            let source = super::super::LocalColumnarSource::admit_complete(
                &fixture.0,
                "arrow-ipc",
                false,
                &pool,
                &token,
            )
            .unwrap();
            let prepared = super::super::PreparedColumnarSource {
                region_selection: None,
                source,
                projection: vec![0],
                row_groups: vec![],
                max_rows: Some(limit),
                metadata: pool.reserve(0).unwrap(),
            };
            let metrics = Arc::new(super::super::LocalReadMetrics::default());
            let mut reader = super::super::generic::Reader::new();
            let actual = reader
                .next_preparation(&prepared, &metrics, &pool, &token)
                .unwrap();
            if limit == 0 {
                assert!(actual.is_none());
            } else {
                assert_eq!(
                    &*actual.unwrap().0,
                    &expected[0].project(&[0]).unwrap().slice(0, 2)
                );
            }
            assert!(
                reader
                    .next_preparation(&prepared, &metrics, &pool, &token)
                    .unwrap()
                    .is_none()
            );
            let report = reader.report(&prepared, &metrics).unwrap();
            assert_eq!(report.rows_decoded, if limit == 0 { 0 } else { 32 });
            assert_eq!(report.adaptive_decode.completed_windows, 0);
            if limit == 0 {
                assert_eq!(report.data_read_bytes, 0);
            }
            drop(reader);
            drop(prepared);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_owned_ipc_shared_reader_admits_small_messages_and_rejects_oversized_physical_units() {
    let small = RecordBatch::try_from_iter([(
        "n",
        Arc::new(Int64Array::from_iter_values(0..16)) as ArrayRef,
    )])
    .unwrap();
    let large = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec!["x".repeat(1 << 20)])) as ArrayRef,
    )])
    .unwrap();
    for stream in [false, true] {
        for (expected, admitted) in [(&small, true), (&large, false)] {
            let fixture =
                Fixture::new(&encode(std::slice::from_ref(expected), stream, None, false));
            let pool = LiveMemoryPool::new(256 << 10).unwrap();
            let token = CancellationToken::default();
            let source = super::super::LocalColumnarSource::admit_complete(
                &fixture.0,
                "arrow-ipc",
                false,
                &pool,
                &token,
            )
            .unwrap();
            let ingest = source.complete_ingest_stream(None, &pool, &token).unwrap();
            let (metadata, _, mut reader) = ingest.into_parts();
            if admitted {
                let batch = reader.next().unwrap().unwrap();
                assert_eq!(&*batch, expected);
                assert!(reader.next().is_none());
            } else {
                let error = reader.next().unwrap().unwrap_err();
                assert!(
                    source_error(error)
                        .to_string()
                        .contains("memory reservation denied")
                );
                assert!(reader.next().is_none());
            }
            drop(reader);
            drop(metadata);
            drop(source);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
            assert!(pool.snapshot().peak_reserved_bytes <= pool.snapshot().limit_bytes);
        }
    }
}
