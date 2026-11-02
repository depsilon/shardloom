//! Incremental text preparation with explicit complete schema-discovery replay.

#[cfg(not(all(feature = "vortex-local-primitives", unix)))]
use super::{
    Arc, Instant, LocalInputAdapterSelection, LocalSourceFormat, LocalSourceReadPlan,
    ShardLoomError, VortexIngestReport, VortexIngestRequest, VortexIngestSourceData,
    fingerprint_local_source_file_with_budget_report, finish_text_streaming_vortex_prepare,
    fnv64_digest, text_stream_record_batch_size,
};
#[cfg(all(test, feature = "vortex-local-primitives", unix))]
use super::{Arc, LocalSourceReadPlan, ShardLoomError};
#[cfg(test)]
use super::{
    CancellationToken, RecordBatch, parse_flat_json_object_with_plan, unsupported_sql_error,
};
use shardloom_vortex::universal_format_io::text_stream::TextReadMetrics;
#[cfg(test)]
use shardloom_vortex::universal_format_io::text_stream::{
    TextFraming, TextRecordDecoder, TextSource,
};

#[cfg(test)]
pub(super) fn decoder() -> TextRecordDecoder {
    Arc::new(|record| {
        parse_flat_json_object_with_plan(
            record,
            &LocalSourceReadPlan::full("streaming_json_record"),
        )
    })
}

#[allow(clippy::too_many_lines)]
#[cfg(not(all(feature = "vortex-local-primitives", unix)))]
pub(super) fn prepare(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
    schema_hints: &[(String, shardloom_core::LogicalDType)],
) -> Result<VortexIngestReport, ShardLoomError> {
    request.cancellation.check()?;
    let prepare_start = Instant::now();
    let limits = request.runtime_profile.read_limits();
    let read_start = Instant::now();
    let format = source_adapter.source_format;
    let scout = fingerprint_local_source_file_with_budget_report(
        &request.source_path,
        format.row_label(),
        limits.source_bytes,
        request.source_fingerprint_policy,
        &request.cancellation,
    )?;
    let read_millis = read_start.elapsed().as_millis();
    let build_start = Instant::now();
    let memory = request.native_memory_pool()?;
    let text = super::direct_text::prepare_single(
        &request.source_path,
        format.as_str(),
        schema_hints,
        limits.input_rows,
        &memory,
        &request.cancellation,
    )?;
    let schema = text.schema();
    let header = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let source_schema_digest = fnv64_digest(&header.join(","));
    let column_arrow_dtypes = schema
        .fields()
        .iter()
        .map(|field| Some(field.data_type().clone()))
        .collect::<Vec<_>>();
    let rows = text.rows();
    let batch_size = text_stream_record_batch_size(limits.input_rows).min(8192);
    let reader = text.open(
        &(0..header.len()).collect::<Vec<_>>(),
        batch_size,
        &request.cancellation,
        Arc::new(TextReadMetrics::default()),
    )?;
    let columnar_source = shardloom_vortex::FlatLocalColumnarStreamSource {
        column_dtypes: vec![None; header.len()],
        column_arrow_dtypes,
        materialized_columns: header.clone(),
        reader_projection_columns: header.clone(),
        header,
        row_count_hint: Some(rows),
        record_batch_count_hint: None,
        source_stream_batch_size: batch_size,
        source_stream_unit_count_hint: None,
        source_stream_unit_row_ranges: None,
        source_stream_unit_hint_kind: "width_bounded_text_record_batches".into(),
        source_stream_policy: format!(
            "incremental_{}_schema_discovery_replay;schema_discovery_read_bytes={};record_batch_stream_batch_size_{batch_size};single_record_and_credited_batch",
            format.as_str(),
            text.discovery_bytes(),
        ),
        source_dictionary_preservation_status: "text_typed_builders_no_source_dictionary".into(),
        ingest_executor_status: "streaming_text_schema_discovered".into(),
        ingest_executor_kind: "incremental_text_record_reader".into(),
        ingest_executor_requested_parallelism: request.resources.max_parallelism(),
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: None,
        source_identities: vec![text.identity()],
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        reader: Box::new(reader),
    };
    let columnar_source =
        shardloom_vortex::universal_format_io::with_embedded_derived_columns_columnar_stream_source(
            columnar_source,
        );
    let columnar_source = shardloom_vortex::universal_format_io::with_capillary_prefetch_columnar_stream_source_controlled(
        columnar_source,
        request.resources.max_parallelism(),
        &request.cancellation,
    )?;
    let mut source = VortexIngestSourceData::from_columnar_stream_source(
        source_adapter,
        &columnar_source,
        scout,
        read_millis,
        build_start.elapsed().as_millis(),
    );
    source.read_plan = LocalSourceReadPlan::full("incremental_text_schema_replay");
    source.projection_pushdown_status = format.projection_pushdown_status(&source.read_plan);
    source.materialization_layout = if format == LocalSourceFormat::Json {
        "incremental_json_to_streaming_arrow_record_batch_source_state"
    } else {
        "incremental_text_to_streaming_arrow_record_batch_source_state"
    };
    source.parse_normalization = "text_incremental_records_with_explicit_schema_replay";
    finish_text_streaming_vortex_prepare(
        request,
        source,
        columnar_source,
        source_schema_digest,
        prepare_start,
    )
}

#[cfg(test)]
fn parse(
    content: &str,
    max_rows: Option<usize>,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, ShardLoomError> {
    parse_controlled(content, max_rows, batch_size, &CancellationToken::default())
}

#[cfg(test)]
fn parse_controlled(
    content: &str,
    max_rows: Option<usize>,
    batch_size: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<RecordBatch>, ShardLoomError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    cancellation.check()?;
    let path = std::env::temp_dir().join(format!(
        "shardloom-json-stream-{}-{}.json",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, content).map_err(|e| unsupported_sql_error(&e.to_string()))?;
    let result = (|| {
        let memory = shardloom_exec::live_memory::LiveMemoryPool::new(64 << 20)?;
        let source = TextSource::admit(
            &path,
            TextFraming::JsonDocument,
            false,
            decoder(),
            None,
            max_rows,
            &memory,
            cancellation,
        )?;
        source
            .open(
                &(0..source.schema().fields().len()).collect::<Vec<_>>(),
                batch_size,
                cancellation,
                Arc::new(TextReadMetrics::default()),
            )?
            .map(|batch| batch.map_err(|e| unsupported_sql_error(&e.to_string())))
            .collect()
    })();
    std::fs::remove_file(&path).map_err(|e| unsupported_sql_error(&e.to_string()))?;
    result
}

#[cfg(test)]
mod tests {
    use super::super::{
        fnv64_digest_bytes_controlled, ordered_source_rows, parse_json_source_content,
        read_local_source_bytes_with_budget_report_controlled,
        visit_json_source_rows_with_plan_controlled,
    };
    use super::*;

    #[test]
    fn whole_json_controlled_preparation_stops_before_io_and_between_rows() {
        let token = CancellationToken::default();
        token.cancel();
        let error = read_local_source_bytes_with_budget_report_controlled(
            std::path::Path::new("must-not-be-opened.json"),
            "JSON",
            None,
            &token,
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancel"), "{error}");
        assert!(
            parse_controlled("invalid JSON", None, 1, &token)
                .unwrap_err()
                .to_string()
                .contains("cancel")
        );
        let token = CancellationToken::default();
        let mut rows = 0;
        let error = visit_json_source_rows_with_plan_controlled(
            r#"[{"id":1},{"id":2},{"id":3}]"#,
            &LocalSourceReadPlan::full("controlled_json_test"),
            &token,
            |_| {
                rows += 1;
                token.cancel();
                Ok(())
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancel"), "{error}");
        assert_eq!(rows, 1);
    }

    #[test]
    fn whole_json_controlled_digest_preserves_bytes_and_parent_deadline_scope() {
        let parent = CancellationToken::default();
        let bytes = "東京🙂\\n".repeat(8193).into_bytes();
        assert_eq!(
            fnv64_digest_bytes_controlled(&bytes, &parent).unwrap(),
            super::super::fnv64_digest_bytes(&bytes),
        );
        let child = parent.with_deadline(std::time::Instant::now());
        assert!(
            fnv64_digest_bytes_controlled(&bytes, &child)
                .unwrap_err()
                .to_string()
                .contains("deadline")
        );
        assert!(
            parse_controlled(r#"{"id":1}"#, None, 1, &child)
                .unwrap_err()
                .to_string()
                .contains("deadline")
        );
        parent.check().unwrap();
    }

    fn reference(content: &str) -> Result<RecordBatch, ShardLoomError> {
        let (header, rows) = parse_json_source_content(content)?;
        let rows = ordered_source_rows(&header, &rows)?;
        let mut source =
            shardloom_vortex::universal_format_io::stream_flat_text_rows_columnar_source(
                header.clone(),
                vec![None; header.len()],
                vec![None; header.len()],
                header.clone(),
                header,
                rows,
                262_144,
                "JSON",
            )?;
        source
            .reader
            .next()
            .expect("nonempty source")
            .map_err(|error| unsupported_sql_error(&error.to_string()))
    }

    #[test]
    fn whole_json_typed_preserves_complete_values_schema_and_duplicates() {
        for content in [
            "\u{feff}{\"id\":9,\"label\":\"東京🙂\\n\",\"n\":null}",
            r#"[{"id":"overwritten","id":1,"n":null},{"n":null,"late":true,"id":2},{"id":3,"late":null,}]"#,
            r#"[{"first":null},{"second":null,"first":"value"},{"first":null,"second":7},]"#,
            r#"[{"x":9223372036854775807,"f":1.25,"obj":{"b":[1,null],"a":"λ"}},{"x":-9223372036854775808,"f":-0.0,"obj":[{},true]}]"#,
            r#"[{"n":null},{"n":null}]"#,
            r#"{"id":18446744073709551615}"#,
        ] {
            let reference = reference(content).unwrap();
            for size in [1, 2, 3, 65_536] {
                let actual = parse(content, None, size).unwrap();
                assert_eq!(actual.len(), reference.num_rows().div_ceil(size));
                for (i, batch) in actual.iter().enumerate() {
                    assert_eq!(
                        batch,
                        &reference.slice(i * size, size.min(reference.num_rows() - i * size)),
                        "{content}"
                    );
                }
            }
        }
    }

    #[test]
    fn whole_json_typed_rejects_legacy_invalid_and_unstable_types() {
        for content in [
            "",
            "[]",
            "{}",
            "[{}]",
            "[1]",
            "null",
            "{\"x\":1} {}",
            "[{\"x\":1}] []",
            "[{\"x\":1}",
            "[{\"x\":1} {\"x\":2}]",
            r#"{"x":falsee,"x":1}"#,
            r#"{"bad name":1}"#,
            r#"{"x":{"a":1,}}"#,
            r#"{"x":"\uZZZZ"}"#,
            r#"[{"x":1},{"x":1.25}]"#,
            r#"[{"x":true},{"x":"true"}]"#,
        ] {
            assert!(reference(content).is_err(), "reference admits {content}");
            assert!(
                parse(content, None, 2).is_err(),
                "candidate admits {content}"
            );
        }
        assert!(
            parse(r#"[{"x":1},{"x":2}]"#, Some(1), 1)
                .unwrap_err()
                .to_string()
                .contains("at most 1")
        );
        assert!(parse(r#"{"x":1}"#, Some(0), 1).is_err());
    }
}
