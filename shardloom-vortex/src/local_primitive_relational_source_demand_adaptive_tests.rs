//! Provider limits applied through the shared demanded source and native intake.

use super::*;
use orc_rust::compression::CompressionType;

const ROUTES: [&str; 8] = [
    "parquet",
    "mapped-parquet",
    "orc",
    "orc-zlib",
    "orc-snappy",
    "orc-lz4",
    "orc-zstd",
    "avro",
];

fn fixture(rows: usize) -> ParquetFixture {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(
            0..i64::try_from(rows).unwrap(),
        ))],
    )
    .unwrap();
    let fixture =
        ParquetFixture::with_batch_rows(batch.clone(), EnabledStatistics::Chunk, rows.max(1));
    for (name, compression) in [
        ("orc", None),
        ("orc-zlib", Some(CompressionType::Zlib)),
        ("orc-snappy", Some(CompressionType::Snappy)),
        ("orc-lz4", Some(CompressionType::Lz4)),
        ("orc-zstd", Some(CompressionType::Zstd)),
    ] {
        let mut builder = orc_rust::ArrowWriterBuilder::new(
            fs::File::create(fixture.0.join(format!("input.{name}"))).unwrap(),
            batch.schema(),
        )
        .with_compression_block_size(4096);
        if let Some(compression) = compression {
            builder = builder.with_compression(compression);
        }
        let mut writer = builder.try_build().unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let mut writer = arrow_avro::writer::AvroWriter::new(
        fs::File::create(fixture.0.join("input.avro")).unwrap(),
        batch.schema().as_ref().clone(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    fixture
}

fn source(
    fixture: &ParquetFixture,
    route: &str,
    session: &ResidentVortexSession,
    token: &CancellationToken,
) -> Result<Arc<LocalColumnarSource>> {
    let memory = session.shared_memory_pool();
    match route {
        "parquet" => LocalColumnarSource::admit(&fixture.path(), "parquet", &memory, token),
        "mapped-parquet" => {
            LocalColumnarSource::admit_complete(&fixture.path(), "parquet", false, &memory, token)
        }
        "orc" | "orc-zlib" | "orc-snappy" | "orc-lz4" | "orc-zstd" => LocalColumnarSource::admit(
            &fixture.0.join(format!("input.{route}")),
            "orc",
            &memory,
            token,
        ),
        "avro" => LocalColumnarSource::admit(&fixture.0.join("input.avro"), "avro", &memory, token),
        _ => unreachable!(),
    }
}

fn check_values(
    source: &ResidentMemorySource,
    context: &NativeExecutionContext<'_>,
    seen: &mut i64,
) -> Result<()> {
    let array = source.batch_array(context)?;
    let mut execution = context.native_session().create_execution_ctx();
    let values = crate::local_primitives::logical_field_from_native_array(array, "n")?
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(|error| failed(&error.to_string()))?;
    for value in values.as_slice::<i64>() {
        assert_eq!(*value, *seen);
        *seen += 1;
    }
    Ok(())
}

#[test]
fn columnar_adaptive_decoder_shrinks_regrows_and_retains_complete_native_outputs() {
    let fixture = fixture(40_003);
    for route in ROUTES {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.shared_memory_pool();
        let token = CancellationToken::default();
        let held = session
            .with_native_execution_context(&token, |context| {
                let source = source(&fixture, route, &session, &token)?;
                let prepared =
                    source.prepare(&shardloom_plan::SourceDemand::default(), &session, &token)?;
                let mut reader = prepared.open(context)?;
                let (first, offset) = reader.next_native_located(&session, context)?.unwrap();
                assert_eq!(offset, 0);
                assert_eq!(first.row_count(), 8192, "{route}");
                let mut seen = 0;
                check_values(&first, context, &mut seen)?;
                let snapshot = memory.snapshot();
                let pressure =
                    memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (96 << 10))?;
                let (second, offset) = reader.next_native_located(&session, context)?.unwrap();
                assert_eq!(offset, u64::try_from(seen).unwrap());
                let shrunk = second.row_count();
                assert!(shrunk > 0 && shrunk < first.row_count(), "{route}");
                check_values(&second, context, &mut seen)?;
                drop((second, pressure));
                let mut regrown = false;
                while let Some((batch, offset)) = reader.next_native_located(&session, context)? {
                    assert_eq!(offset, u64::try_from(seen).unwrap());
                    regrown |= batch.row_count() > shrunk;
                    check_values(&batch, context, &mut seen)?;
                }
                assert!(regrown, "{route}");
                assert_eq!(seen, 40_003);
                check_values(&first, context, &mut 0)?;
                let report = reader.report()?;
                assert_eq!(report.rows_decoded, 40_003);
                assert!(report.adaptive_decode.shrunk_windows > 0, "{route}");
                assert!(report.adaptive_decode.grown_windows > 0, "{route}");
                assert!(
                    report.adaptive_decode.headroom_limited_windows > 0,
                    "{route}"
                );
                assert_eq!(report.adaptive_decode.maximum_in_flight, 1);
                assert_eq!(report.adaptive_decode.ceiling_bytes, 16 << 20);
                Ok(first)
            })
            .unwrap();
        assert!(memory.snapshot().reserved_bytes > 0);
        assert!(memory.snapshot().peak_reserved_bytes <= 16 << 20);
        drop((session, held));
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{route}");
    }
}

#[test]
fn columnar_adaptive_owned_providers_start_below_the_old_fixed_envelope() {
    let fixture = fixture(20_003);
    for route in [
        "orc",
        "orc-zlib",
        "orc-snappy",
        "orc-lz4",
        "orc-zstd",
        "avro",
    ] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.shared_memory_pool();
        let token = CancellationToken::default();
        let held = session
            .with_native_execution_context(&token, |context| {
                let source = source(&fixture, route, &session, &token)?;
                let prepared =
                    source.prepare(&shardloom_plan::SourceDemand::default(), &session, &token)?;
                let snapshot = memory.snapshot();
                let _pressure =
                    memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (384 << 10))?;
                let mut reader = prepared.open(context)?;
                let first = reader.next_native(&session, context)?.unwrap();
                let mut seen = 0;
                check_values(&first, context, &mut seen)?;
                while let Some(batch) = reader.next_native(&session, context)? {
                    check_values(&batch, context, &mut seen)?;
                }
                assert_eq!(seen, 20_003, "{route}");
                assert_eq!(reader.report()?.rows_decoded, 20_003);
                check_values(&first, context, &mut 0)?;
                Ok(first)
            })
            .unwrap();
        assert!(memory.snapshot().peak_reserved_bytes <= 8 << 20);
        drop((session, held));
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{route}");
    }
}

fn compressed_fixture() -> (ParquetFixture, Vec<i64>) {
    let expected = (0_u64..40_003)
        .map(|index| {
            if index.is_multiple_of(2) {
                0
            } else {
                i64::try_from((index.wrapping_mul(0x9e37_79b1) ^ (index >> 9)) & 0xffff).unwrap()
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(expected.clone()))],
    )
    .unwrap();
    let fixture = ParquetFixture::new();
    let path = fixture.0.join("input.orc-zstd");
    let mut writer =
        orc_rust::ArrowWriterBuilder::new(fs::File::create(&path).unwrap(), batch.schema())
            .with_compression(CompressionType::Zstd)
            .with_compression_block_size(4096)
            .try_build()
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Verify the fixture actually crosses many compressed physical blocks;
    // monotonic integers can otherwise collapse to one tiny RLE stream.
    let bytes = fs::read(path).unwrap();
    let reader = orc_rust::ArrowReaderBuilder::try_new(bytes::Bytes::from(bytes.clone())).unwrap();
    let metadata = reader.shared_file_metadata();
    assert_eq!(metadata.stripe_metadatas().len(), 1);
    let stripe = &metadata.stripe_metadatas()[0];
    let mut offset = usize::try_from(stripe.offset() + stripe.index_length()).unwrap();
    let end = offset + usize::try_from(stripe.data_length()).unwrap();
    let mut compressed = 0;
    while offset < end {
        let header = u32::from_le_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], 0]);
        compressed += usize::from(header & 1 == 0);
        offset += 3 + usize::try_from(header >> 1).unwrap();
    }
    assert_eq!(offset, end);
    assert!(
        compressed >= 8,
        "fixture has only {compressed} compressed blocks"
    );
    (fixture, expected)
}

#[test]
fn columnar_adaptive_orc_zstd_crosses_compressed_blocks_under_retained_pressure() {
    let (fixture, expected) = compressed_fixture();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.shared_memory_pool();
    let token = CancellationToken::default();
    let held = session
        .with_native_execution_context(&token, |context| {
            let source = source(&fixture, "orc-zstd", &session, &token)?;
            let prepared =
                source.prepare(&shardloom_plan::SourceDemand::default(), &session, &token)?;
            let mut reader = prepared.open(context)?;
            let first = reader.next_native(&session, context)?.unwrap();
            let mut seen = 0;
            let mut check = |batch: &ResidentMemorySource| -> Result<()> {
                let array = batch.batch_array(context)?;
                let mut execution = context.native_session().create_execution_ctx();
                let values = crate::local_primitives::logical_field_from_native_array(array, "n")?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(|error| failed(&error.to_string()))?;
                assert_eq!(
                    values.as_slice::<i64>(),
                    &expected[seen..seen + batch.row_count()]
                );
                seen += batch.row_count();
                Ok(())
            };
            check(&first)?;
            let snapshot = memory.snapshot();
            let _pressure =
                memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (96 << 10))?;
            while let Some(batch) = reader.next_native(&session, context)? {
                assert!(batch.row_count() < first.row_count());
                check(&batch)?;
            }
            assert_eq!(seen, expected.len());
            assert_eq!(reader.report()?.rows_decoded, expected.len() as u64);
            Ok(first)
        })
        .unwrap();
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(memory.snapshot().peak_reserved_bytes <= 8 << 20);
    drop((session, held));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn columnar_adaptive_decoder_known_end_and_zero_prefix_need_no_fake_batch_credit() {
    let fixture = fixture(17);
    for route in ROUTES {
        for prefix in [None, Some(0)] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let memory = session.shared_memory_pool();
            let token = CancellationToken::default();
            session
                .with_native_execution_context(&token, |context| {
                    let source = source(&fixture, route, &session, &token)?;
                    let demand = shardloom_plan::SourceDemand {
                        max_rows: prefix,
                        ..Default::default()
                    };
                    let prepared = source.prepare(&demand, &session, &token)?;
                    let mut reader = prepared.open(context)?;
                    if prefix.is_none() {
                        let batch = reader.next_native(&session, context)?.unwrap();
                        assert_eq!(batch.row_count(), 17);
                        check_values(&batch, context, &mut 0)?;
                    }
                    let snapshot = memory.snapshot();
                    let pressure =
                        memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?;
                    assert!(reader.next_native(&session, context)?.is_none());
                    let report = reader.report()?;
                    assert_eq!(
                        report.adaptive_decode.completed_windows,
                        u64::from(prefix.is_none())
                    );
                    assert_eq!(report.rows_decoded, if prefix.is_some() { 0 } else { 17 });
                    drop(pressure);
                    Ok(())
                })
                .unwrap();
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn columnar_adaptive_decoder_denial_or_cancellation_refunds_and_preserves_the_error() {
    let fixture = fixture(20_003);
    for route in ROUTES {
        for cancel in [false, true] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let memory = session.shared_memory_pool();
            let token = CancellationToken::default();
            let error = session
                .with_native_execution_context(&token, |context| {
                    let source = source(&fixture, route, &session, &token)?;
                    let prepared = source.prepare(
                        &shardloom_plan::SourceDemand::default(),
                        &session,
                        &token,
                    )?;
                    let mut reader = prepared.open(context)?;
                    let first = reader.next_native(&session, context)?.unwrap();
                    check_values(&first, context, &mut 0)?;
                    let snapshot = memory.snapshot();
                    let _pressure =
                        memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1)?;
                    if cancel {
                        token.cancel();
                    }
                    reader.next_native(&session, context).map(|_| ())
                })
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(if cancel {
                    "cancelled"
                } else {
                    "minimum workspace does not fit"
                }),
                "{route}: {error}"
            );
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0, "{route}");
        }
    }
}
