use super::*;
use arrow_array::{Array as _, Int64Array, StringArray};
use std::{fs, path::PathBuf, sync::atomic::AtomicU64};

mod adaptive;

struct Fixture(PathBuf);
impl Fixture {
    fn new(content: &str) -> Self {
        Self::bytes("json", content.as_bytes())
    }
    fn bytes(extension: &str, content: &[u8]) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-text-stream-{}-{}.{extension}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, content).unwrap();
        Self(path)
    }
    fn source(
        &self,
        memory: &LiveMemoryPool,
        token: &CancellationToken,
    ) -> Result<Arc<TextSource>> {
        TextSource::admit(
            &self.0,
            TextFraming::JsonDocument,
            false,
            decoder(),
            None,
            None,
            memory,
            token,
        )
    }
}

fn compressed(codec: &str, raw: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    match codec {
        "gz" => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(raw).unwrap();
            encoder.finish().unwrap()
        }
        "zst" | "zstd" => {
            let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
            encoder.include_checksum(true).unwrap();
            encoder.write_all(raw).unwrap();
            encoder.finish().unwrap()
        }
        _ => unreachable!(),
    }
}

#[test]
fn compressed_text_reads_all_members_counts_physical_bytes_and_preserves_values() {
    let raw = "\u{feff}[{\"id\":1,\"text\":\"東京🙂\"},{\"id\":2,\"text\":null}]";
    for codec in ["gz", "zst", "zstd"] {
        // A record crosses a member/frame boundary. Framing belongs to the
        // decoded stream, not to each compressed member.
        let split = raw.len() / 2;
        let mut bytes = compressed(codec, &raw.as_bytes()[..split]);
        bytes.extend(compressed(codec, &raw.as_bytes()[split..]));
        let fixture = Fixture::bytes(&format!("json.{codec}"), &bytes);
        let memory = LiveMemoryPool::new(32 << 20).unwrap();
        let token = CancellationToken::default();
        let source = fixture.source(&memory, &token).unwrap();
        assert_eq!(source.discovery_bytes(), bytes.len() as u64, "{codec}");
        let metrics = Arc::new(TextReadMetrics::default());
        let batches = source
            .open(&[0, 1], 1, &token, Arc::clone(&metrics))
            .unwrap()
            .map(|batch| batch.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 2, "{codec}");
        assert_eq!(
            metrics.bytes.load(Ordering::Relaxed),
            bytes.len() as u64,
            "{codec}"
        );
        assert!(metrics.requests.load(Ordering::Relaxed) > 0);
        let text = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "東京🙂");
        assert!(batches[1].column(1).is_null(0));
        drop(source);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(batches);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn compressed_text_rejects_truncation_checksums_and_unadmitted_workspace() {
    let raw = br#"[{"id":1},{"id":2}]"#;
    for codec in ["gz", "zst"] {
        let original = compressed(codec, raw);
        let mut truncated = original.clone();
        truncated.truncate(truncated.len() - 3);
        let mut checksum = original.clone();
        // Both fixtures use checksummed formats. Mutating the final checksum
        // (or gzip size trailer) must fail even after valid rows were decoded.
        *checksum.last_mut().unwrap() ^= 0x40;
        for invalid in [truncated, checksum, b"not compressed".to_vec()] {
            let fixture = Fixture::bytes(&format!("json.{codec}"), &invalid);
            let memory = LiveMemoryPool::new(32 << 20).unwrap();
            assert!(
                fixture
                    .source(&memory, &CancellationToken::default())
                    .is_err(),
                "{codec}"
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
        let fixture = Fixture::bytes(&format!("json.{codec}"), &original);
        let denied = LiveMemoryPool::new(512 << 10).unwrap();
        assert!(
            fixture
                .source(&denied, &CancellationToken::default())
                .err()
                .unwrap()
                .to_string()
                .contains("reservation denied")
        );
        assert_eq!(denied.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn compressed_text_cancellation_and_changed_generation_poison_replay() {
    for codec in ["gz", "zst"] {
        let fixture = Fixture::bytes(
            &format!("json.{codec}"),
            &compressed(codec, br#"[{"id":1},{"id":2}]"#),
        );
        let memory = LiveMemoryPool::new(32 << 20).unwrap();
        let token = CancellationToken::default();
        let source = fixture.source(&memory, &token).unwrap();
        let mut reader = source
            .open(&[0], 1, &token, Arc::new(TextReadMetrics::default()))
            .unwrap();
        drop(reader.next().unwrap().unwrap());
        token.cancel();
        assert!(
            reader
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("cancel")
        );
        assert!(reader.next().is_none());
        drop(reader);
        let token = CancellationToken::default();
        let mut reader = source
            .open(&[0], 1, &token, Arc::new(TextReadMetrics::default()))
            .unwrap();
        drop(reader.next().unwrap().unwrap());
        fs::write(&fixture.0, compressed(codec, br#"[{"id":3},{"id":4}]"#)).unwrap();
        assert!(
            reader
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("changed")
        );
        assert!(reader.next().is_none());
        drop(reader);
        drop(source);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_file(&self.0).unwrap();
    }
}

fn decoder() -> TextRecordDecoder {
    Arc::new(|raw| {
        let value: serde_json::Value = serde_json::from_str(raw).map_err(failed)?;
        value
            .as_object()
            .ok_or_else(|| failed("expected object"))?
            .iter()
            .map(|(name, value)| {
                let value = match value {
                    serde_json::Value::Null => ScalarValue::Null,
                    serde_json::Value::Bool(value) => ScalarValue::Boolean(*value),
                    serde_json::Value::Number(value) => {
                        ScalarValue::Int64(value.as_i64().ok_or_else(|| failed("expected int64"))?)
                    }
                    serde_json::Value::String(value) => ScalarValue::Utf8(value.clone()),
                    _ => ScalarValue::Utf8(serde_json::to_string(value).map_err(failed)?),
                };
                Ok((name.clone(), value))
            })
            .collect()
    })
}

#[test]
fn streaming_json_discovers_late_fields_and_retains_credited_output_lifetime() {
    let fixture =
        Fixture::new(r#"[{"id":1,"text":"東京🙂"},{"id":2,"late":true},{"id":3,"text":null}]"#);
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    assert_eq!(source.rows(), 3);
    assert_eq!(
        source.discovery_bytes(),
        fs::metadata(&fixture.0).unwrap().len()
    );
    let names = source
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["id", "text", "late"]);
    assert!(source.schema().field(1).is_nullable());
    assert!(source.schema().field(2).is_nullable());
    let mut reader = source
        .open(&[0, 1, 2], 2, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let first = reader.next().unwrap().unwrap();
    let second = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    assert_eq!(second.num_rows(), 1);
    drop(reader);
    drop(source);
    assert!(memory.snapshot().reserved_bytes > 0);
    let text = first
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(text.value(0), "東京🙂");
    assert!(text.is_null(1));
    assert!(second.column(1).is_null(0));
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values().as_ref(), &[1, 2]);
    drop(first);
    drop(second);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_json_framing_keeps_nested_strings_unicode_and_rejects_suffixes() {
    let token = CancellationToken::default();
    for content in [
        "\u{feff}[{\"id\":1,\"text\":\"[ } \\\" 東京\"},{\"id\":2,\"text\":\"🙂\"},]",
        r#"{"id":1,"text":{"a":[1,{"q":"} ["}]}}"#,
    ] {
        let fixture = Fixture::new(content);
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let source = fixture.source(&memory, &token).unwrap();
        let rows = source
            .open(&[0, 1], 1, &token, Arc::new(TextReadMetrics::default()))
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>();
        assert_eq!(rows, source.rows());
    }
    for content in [
        r#"[{"id":1}] {}"#,
        r#"[{"id":1} {"id":2}]"#,
        r#"[{"id":1},{"id":2]"#,
        r#"{"id":1} []"#,
        r#"[{"id":1},invalid]"#,
        r#"[{"id":1},{"id":"changed"}]"#,
    ] {
        let fixture = Fixture::new(content);
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        assert!(fixture.source(&memory, &token).is_err(), "{content}");
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn streaming_json_many_batches_do_not_retain_document_rows() {
    let content = format!(
        "[{}]",
        (0..20_001)
            .map(|id| format!("{{\"id\":{id}}}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let fixture = Fixture::new(&content);
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    assert!(memory.snapshot().reserved_bytes < 64 << 10);
    let mut seen = 0_i64;
    let mut batches = 0;
    for batch in source
        .open(&[0], 4096, &token, Arc::new(TextReadMetrics::default()))
        .unwrap()
    {
        let batch = batch.unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for id in ids.values() {
            assert_eq!(*id, seen);
            seen += 1;
        }
        batches += 1;
    }
    assert_eq!(seen, 20_001);
    assert!(batches > 4);
    assert!(memory.snapshot().peak_reserved_bytes < 8 << 20);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_json_large_record_has_explicit_admission_and_owned_complete_value() {
    let text = "z".repeat(9 << 20);
    let fixture = Fixture::new(&format!("[{{\"text\":\"{text}\"}}]"));
    let token = CancellationToken::default();
    let denied = LiveMemoryPool::new(64 << 20).unwrap();
    assert!(
        fixture
            .source(&denied, &token)
            .err()
            .unwrap()
            .to_string()
            .contains("reservation denied")
    );
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(512 << 20).unwrap();
    let source = fixture.source(&memory, &token).unwrap();
    let mut reader = source
        .open(&[0], 8192, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    drop(reader);
    drop(source);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        text
    );
    drop(batch);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streaming_json_cancel_change_and_decode_errors_release_all_owners() {
    let fixture = Fixture::new(r#"[{"id":1},{"id":2}]"#);
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let token = CancellationToken::default();
    token.cancel();
    assert!(
        fixture
            .source(&memory, &token)
            .err()
            .unwrap()
            .to_string()
            .contains("cancel")
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let token = CancellationToken::default();
    let source = fixture.source(&memory, &token).unwrap();
    let mut reader = source
        .open(&[0], 1, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    drop(reader.next().unwrap().unwrap());
    fs::write(&fixture.0, r#"[{"id":3},{"id":4}]"#).unwrap();
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("changed")
    );
    assert!(reader.next().is_none());
    assert!(
        source
            .open(&[0], 1, &token, Arc::new(TextReadMetrics::default()))
            .is_err()
    );
    drop(reader);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn csv_profile_decoder(
    headers: &'static [&'static str],
    int_column: &'static str,
) -> TextRecordDecoder {
    Arc::new(move |raw| {
        let fields = raw
            .trim_end_matches(['\r', '\n'])
            .split(',')
            .collect::<Vec<_>>();
        if fields.len() != headers.len() {
            return Err(failed("unexpected csv column count"));
        }
        headers
            .iter()
            .zip(fields)
            .map(|(&name, val)| {
                let scalar = if name == int_column {
                    ScalarValue::Int64(val.parse().map_err(failed)?)
                } else {
                    ScalarValue::Utf8(val.to_owned())
                };
                Ok((name.to_owned(), scalar))
            })
            .collect()
    })
}

fn assert_positive_profile_and_specs(label: &str, source: &TextSource) {
    use crate::universal_format_io::{
        EmbeddedDerivedColumnKind, EmbeddedDerivedColumnMode,
        embedded_derived_column_specs_with_profile,
    };

    let profile = source
        .first_batch_profile()
        .unwrap_or_else(|| panic!("{label} must attach first_batch_profile"));
    assert!(
        profile.column("landing_link").unwrap().looks_like_url,
        "{label}"
    );
    assert!(
        profile
            .column("observed_at")
            .unwrap()
            .looks_like_epoch_seconds,
        "{label}"
    );
    assert!(
        profile.column("iso_ts").unwrap().looks_like_timestamp_text,
        "{label}"
    );
    assert!(
        profile.column("raw_note").unwrap().has_varied_or_long_utf8,
        "{label}"
    );
    assert!(
        !profile.column("raw_note").unwrap().looks_like_url,
        "{label}"
    );
    assert!(!profile.column("status").unwrap().looks_like_url, "{label}");
    assert!(
        !profile.column("status").unwrap().has_varied_or_long_utf8,
        "{label}"
    );

    let specs = embedded_derived_column_specs_with_profile(
        source.schema().as_ref(),
        Some(profile),
        EmbeddedDerivedColumnMode::FullAdapter,
    );
    let names = specs
        .iter()
        .map(|spec| (spec.source_column.as_str(), spec.kind))
        .collect::<Vec<_>>();
    assert!(
        names.contains(&("landing_link", EmbeddedDerivedColumnKind::Utf8Length)),
        "{label}"
    );
    assert!(
        names.contains(&("landing_link", EmbeddedDerivedColumnKind::UrlDomain)),
        "{label}"
    );
    assert!(
        names.contains(&("observed_at", EmbeddedDerivedColumnKind::ExtractMinute)),
        "{label}"
    );
    assert!(
        names.contains(&("observed_at", EmbeddedDerivedColumnKind::DateTruncMinute)),
        "{label}"
    );
    assert!(
        names.contains(&("iso_ts", EmbeddedDerivedColumnKind::ExtractMinute)),
        "{label}"
    );
    assert!(
        names.contains(&("raw_note", EmbeddedDerivedColumnKind::Utf8Length)),
        "{label}"
    );
    assert!(
        !names.contains(&("raw_note", EmbeddedDerivedColumnKind::UrlDomain)),
        "{label}"
    );
    assert!(!names.iter().any(|(col, _)| *col == "status"), "{label}");
}

#[test]
fn first_batch_profile_populates_across_csv_jsonl_and_json_without_clickbench_names() {
    let positive_csv = "\
landing_link,observed_at,iso_ts,raw_note,status\n\
https://shardloom.io/docs/vortex,1700002861,2024-05-01T12:34:56Z,a long unindexed payload message,ok\n\
https://docs.rs/arrow/index.html,1700002920,2024-05-01T12:35:00Z,tiny,ko\n";
    let positive_jsonl = concat!(
        r#"{"landing_link":"https://shardloom.io/docs/vortex","observed_at":1700002861,"iso_ts":"2024-05-01T12:34:56Z","raw_note":"a long unindexed payload message","status":"ok"}"#,
        "\n",
        r#"{"landing_link":"https://docs.rs/arrow/index.html","observed_at":1700002920,"iso_ts":"2024-05-01T12:35:00Z","raw_note":"tiny","status":"ko"}"#,
        "\n"
    );
    let positive_json = concat!(
        r#"[{"landing_link":"https://shardloom.io/docs/vortex","observed_at":1700002861,"iso_ts":"2024-05-01T12:34:56Z","raw_note":"a long unindexed payload message","status":"ok"},"#,
        r#"{"landing_link":"https://docs.rs/arrow/index.html","observed_at":1700002920,"iso_ts":"2024-05-01T12:35:00Z","raw_note":"tiny","status":"ko"}]"#
    );
    let csv_headers: &[&str] = &[
        "landing_link",
        "observed_at",
        "iso_ts",
        "raw_note",
        "status",
    ];
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let token = CancellationToken::default();

    for (label, framing, skip_first, bytes, dec) in [
        (
            "csv",
            TextFraming::Csv,
            true,
            positive_csv.as_bytes(),
            csv_profile_decoder(csv_headers, "observed_at"),
        ),
        (
            "jsonl",
            TextFraming::JsonLines,
            false,
            positive_jsonl.as_bytes(),
            decoder(),
        ),
        (
            "json",
            TextFraming::JsonDocument,
            false,
            positive_json.as_bytes(),
            decoder(),
        ),
    ] {
        let fixture = Fixture::bytes(label, bytes);
        let source = TextSource::admit(
            &fixture.0, framing, skip_first, dec, None, None, &memory, &token,
        )
        .unwrap();
        assert_positive_profile_and_specs(label, &source);
    }
}

#[test]
fn first_batch_profile_rejects_misnamed_clickbench_csv_columns() {
    use crate::universal_format_io::{
        EmbeddedDerivedColumnMode, embedded_derived_column_specs_with_profile,
    };

    let misnamed_csv = "URL,Referer,EventTime\nblue,cyan,1\npink,gold,2\n";
    let misnamed_headers: &[&str] = &["URL", "Referer", "EventTime"];
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let token = CancellationToken::default();
    let misnamed_fixture = Fixture::bytes("csv", misnamed_csv.as_bytes());
    let misnamed_src = TextSource::admit(
        &misnamed_fixture.0,
        TextFraming::Csv,
        true,
        csv_profile_decoder(misnamed_headers, "EventTime"),
        None,
        None,
        &memory,
        &token,
    )
    .unwrap();
    let misnamed_profile = misnamed_src.first_batch_profile().unwrap();
    let misnamed_specs = embedded_derived_column_specs_with_profile(
        misnamed_src.schema().as_ref(),
        Some(misnamed_profile),
        EmbeddedDerivedColumnMode::FullAdapter,
    );
    assert!(misnamed_specs.is_empty(), "{misnamed_specs:?}");
}

#[test]
fn projected_decoder_skips_unprojected_fields_and_preserves_undiscovered_field_errors() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let csv_fixture = Fixture::bytes("csv", b"a,b,c\n1,alpha,100\n2,beta,200\n");
    let header = ["a", "b", "c"];
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let token = CancellationToken::default();
    let full_parses = Arc::new(AtomicUsize::new(0));
    let full_decoder: TextRecordDecoder = Arc::new(move |raw| {
        let cells: Vec<&str> = raw.trim_end_matches(['\r', '\n']).split(',').collect();
        if cells.len() != 3 {
            return Err(failed("bad csv width"));
        }
        Ok(header
            .iter()
            .zip(cells)
            .map(|(k, v)| ((*k).to_string(), ScalarValue::Utf8(v.to_string())))
            .collect())
    });
    let mut source = TextSource::admit_adapted(
        &csv_fixture.0,
        TextFraming::Csv,
        true,
        full_decoder,
        None,
        None,
        &["a".into(), "b".into(), "c".into()],
        None,
        &memory,
        &token,
    )
    .unwrap();
    let parse_counter = Arc::clone(&full_parses);
    TextSource::set_projected_decoder(
        &mut source,
        Arc::new(move |raw, projection| {
            let cells: Vec<&str> = raw.trim_end_matches(['\r', '\n']).split(',').collect();
            if cells.len() != 3 {
                return Err(failed("bad csv width"));
            }
            parse_counter.fetch_add(projection.len(), Ordering::Relaxed);
            Ok(projection
                .iter()
                .map(|&idx| {
                    (
                        header[idx].to_string(),
                        ScalarValue::Utf8(cells[idx].to_string()),
                    )
                })
                .collect())
        }),
    )
    .unwrap();

    let mut reader = source
        .open(&[1], 16, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "alpha");
    assert_eq!(col.value(1), "beta");
    // Only 1 projected column parsed per row across 2 rows = 2 parses (not 6).
    assert_eq!(full_parses.load(Ordering::Relaxed), 2);
    assert!(reader.next().is_none());
    drop((batch, reader, source));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // Undiscovered field check remains active even with a projected decoder installed.
    let jsonl_fixture = Fixture::bytes("jsonl", b"{\"a\":1,\"b\":\"x\"}\n");
    let mut jsonl_source = TextSource::admit(
        &jsonl_fixture.0,
        TextFraming::JsonLines,
        false,
        decoder(),
        None,
        None,
        &memory,
        &token,
    )
    .unwrap();
    TextSource::set_projected_decoder(
        &mut jsonl_source,
        Arc::new(|_, _| {
            Ok(vec![(
                "injected_unknown".to_string(),
                ScalarValue::Int64(9),
            )])
        }),
    )
    .unwrap();
    let mut bad_reader = jsonl_source
        .open(&[0], 16, &token, Arc::new(TextReadMetrics::default()))
        .unwrap();
    assert!(bad_reader.next().unwrap().is_err());
    drop((bad_reader, jsonl_source));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
