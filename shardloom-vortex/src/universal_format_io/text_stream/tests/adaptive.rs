use super::*;

#[test]
fn streaming_text_adaptive_shared_alignment_preserves_complete_values_with_live_pressure() {
    use crate::universal_format_io::demanded::LocalColumnarSource;
    let fixture = numbered_fixture(20_003);
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let token = CancellationToken::default();
    let source = LocalColumnarSource::admit_text_with(
        &fixture.0,
        "json",
        false,
        &memory,
        &token,
        &|path, pool, cancellation| {
            TextSource::admit(
                path,
                TextFraming::JsonDocument,
                false,
                decoder(),
                None,
                None,
                pool,
                cancellation,
            )
        },
    )
    .unwrap();
    let mut complete = source.complete_stream(None, &memory, &token).unwrap();
    let first = complete.reader.next().unwrap().unwrap();
    let mut seen = 0;
    ids(&first, &mut seen);
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (160 << 10))
        .unwrap();
    let second = complete.reader.next().unwrap().unwrap();
    assert!(second.num_rows() > 0 && second.num_rows() < first.num_rows());
    ids(&second, &mut seen);
    drop((second, pressure));
    for batch in complete.reader.by_ref() {
        ids(&batch.unwrap(), &mut seen);
    }
    assert_eq!(seen, 20_003);
    drop((complete, source));
    ids(&first, &mut 0);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(first);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn ids(batch: &RecordBatch, seen: &mut i64) {
    for id in batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
    {
        assert_eq!(*id, *seen);
        *seen += 1;
    }
}

fn numbered_fixture(rows: usize) -> Fixture {
    Fixture::new(&format!(
        "[{}]",
        (0..rows)
            .map(|id| format!("{{\"id\":{id}}}"))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

#[test]
fn streaming_text_adaptive_shrinks_and_regrows_with_retained_owners() {
    let fixture = numbered_fixture(40_000);
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let first = reader.next().unwrap().unwrap();
    assert_eq!(first.num_rows(), 8192);
    let mut seen = 0;
    ids(&first, &mut seen);
    // This owner overlaps the retained first output and the source/reader.
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (256 << 10))
        .unwrap();
    let second = reader.next().unwrap().unwrap();
    let shrunk_rows = second.num_rows();
    assert!(shrunk_rows > 0 && shrunk_rows < first.num_rows());
    // A complete-record boundary can close a pressured batch before its
    // selected capacity. Regrowth applies to that capacity, not the short row
    // count; every emitted row is still checked below.
    let shrunk_capacity = usize::try_from(reader.adaptive_work_report().minimum_units).unwrap();
    assert!(shrunk_rows <= shrunk_capacity && shrunk_capacity < first.num_rows());
    ids(&second, &mut seen);
    drop(second);
    drop(pressure);
    let mut recovered = Vec::new();
    for _ in 0..4 {
        let batch = reader.next().unwrap().unwrap();
        recovered.push(batch.num_rows());
        ids(&batch, &mut seen);
    }
    assert_eq!(recovered[0], shrunk_capacity);
    assert_eq!(recovered[1], shrunk_capacity);
    assert!(recovered[2] > shrunk_capacity && recovered[2] <= 2 * shrunk_capacity);
    for batch in reader.by_ref() {
        ids(&batch.unwrap(), &mut seen);
    }
    assert_eq!(seen, 40_000);
    let report = reader.adaptive_work_report();
    assert!(
        report.headroom_limited_windows > 0
            && report.shrunk_windows > 0
            && report.grown_windows > 0
    );
    assert_eq!(report.maximum_in_flight, 1);
    assert_eq!(report.ceiling_bytes, 8 << 20);
    assert!(memory.snapshot().peak_reserved_bytes <= report.ceiling_bytes);
    drop(reader);
    drop(source);
    assert!(memory.snapshot().reserved_bytes > 0);
    ids(&first, &mut 0);
    drop(first);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_text_adaptive_wide_schemas_reduce_batches_and_keep_every_value() {
    let mut first_rows = Vec::new();
    for width in [1, 24] {
        let content = format!(
            "[{}]",
            (0..2000)
                .map(|row| {
                    format!(
                        "{{{}}}",
                        (0..width)
                            .map(|col| format!("\"c{col:02}\":{}", row + col))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        let fixture = Fixture::new(&content);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let token = CancellationToken::default();
        let source = fixture.source(&memory, &token).unwrap();
        let projection = (0..width).collect::<Vec<_>>();
        let mut reader = source
            .open(
                &projection,
                8192,
                &token,
                Arc::new(TextReadMetrics::default()),
            )
            .unwrap();
        let mut seen = 0_i64;
        for batch in reader.by_ref() {
            let batch = batch.unwrap();
            if seen == 0 {
                first_rows.push(batch.num_rows());
            }
            for col in 0..width {
                let values = batch
                    .column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert_eq!(values.value(row), seen + i64::try_from(row + col).unwrap());
                }
            }
            seen += i64::try_from(batch.num_rows()).unwrap();
        }
        assert_eq!(seen, 2000);
        assert!(memory.snapshot().peak_reserved_bytes <= 1 << 20);
        drop(reader);
        drop(source);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert!(first_rows[1] > 0 && first_rows[1] < first_rows[0]);
}

#[test]
fn streaming_text_adaptive_long_records_close_batches_before_pressure() {
    let text = "東京🙂".repeat(6554);
    let content = format!(
        "[{}]",
        (0..6)
            .map(|_| format!("{{\"text\":\"{text}\"}}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let fixture = Fixture::new(&content);
    let memory = LiveMemoryPool::new(3 << 20).unwrap();
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let batches = reader
        .by_ref()
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), 6);
    assert_eq!(reader.adaptive_work_report().completed_windows, 6);
    drop(reader);
    drop(source);
    for batch in &batches {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            text
        );
    }
    assert!(memory.snapshot().peak_reserved_bytes <= 3 << 20);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(batches);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_text_adaptive_late_large_record_does_not_block_earlier_prefix() {
    let text = "x".repeat(128 << 10);
    let fixture = Fixture::new(&format!("[{{\"text\":\"ok\"}},{{\"text\":\"{text}\"}}]"));
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (256 << 10))
        .unwrap();
    let first = reader.next().unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "ok"
    );
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("reservation denied")
    );
    assert!(reader.next().is_none());
    assert_eq!(reader.adaptive_work_report().completed_windows, 1);
    drop(first);
    drop(reader);
    drop(source);
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_text_adaptive_minimum_empty_and_cancellation_refund() {
    let fixture = numbered_fixture(3);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 4096)
        .unwrap();
    let error = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        error.contains("owner=text_batch")
            && error.contains("required_additional_bytes=")
            && error.contains("accounted_headroom_bytes=")
    );
    assert_eq!(reader.adaptive_work_report().completed_windows, 0);
    assert!(reader.next().is_none());
    drop(reader);
    drop(source);
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let empty = Fixture::new("[]");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let source = TextSource::admit_adapted(
        &empty.0,
        TextFraming::JsonDocument,
        false,
        decoder(),
        Some(schema),
        None,
        &["id".into()],
        None,
        &memory,
        &token,
    )
    .unwrap();
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
        .unwrap();
    assert!(reader.next().is_none());
    assert_eq!(reader.adaptive_work_report().completed_windows, 0);
    drop(reader);
    drop(pressure);
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    token.cancel();
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("cancel")
    );
    assert_eq!(reader.adaptive_work_report().completed_windows, 0);
    drop(reader);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_text_adaptive_failed_decoder_does_not_complete_or_retry_batch() {
    use std::sync::atomic::AtomicBool;
    let fixture = numbered_fixture(3);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let token = CancellationToken::default();
    let calls = Arc::new(AtomicU64::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let callback = {
        let calls = Arc::clone(&calls);
        let fail = Arc::clone(&fail);
        let decode = decoder();
        Arc::new(move |raw: &str| {
            calls.fetch_add(1, Ordering::Relaxed);
            if fail.load(Ordering::Relaxed) && raw == "{\"id\":1}" {
                return Err(failed("deliberate decoder failure"));
            }
            decode(raw)
        }) as TextRecordDecoder
    };
    let source = TextSource::admit(
        &fixture.0,
        TextFraming::JsonDocument,
        false,
        callback,
        None,
        None,
        &memory,
        &token,
    )
    .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 3);
    fail.store(true, Ordering::Relaxed);
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("deliberate decoder failure")
    );
    assert!(reader.next().is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 5);
    assert_eq!(reader.adaptive_work_report().completed_windows, 0);
    drop(reader);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
