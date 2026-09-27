//! Whole-document JSON construction without retained named scalar rows.
//!
//! Input text and typed columns remain whole-document allocations. Only writer
//! batches are bounded; this is not a streaming JSON parser or an RSS bound.

use super::*;
use arrow_array::RecordBatchIterator;
use shardloom_vortex::universal_format_io::InferredTextColumnBuilder;

const CONTEXT: &str = "JSON Universal Ingest typed text RecordBatch";

#[allow(clippy::too_many_lines)]
pub(super) fn prepare(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
) -> Result<VortexIngestReport, ShardLoomError> {
    let prepare_start = Instant::now();
    let limits = request.runtime_profile.read_limits();
    let byte_read = read_local_source_bytes_with_budget_report(
        &request.source_path,
        "JSON",
        limits.source_bytes,
    )?;
    let read_millis = byte_read.total_read_millis();
    let scout = ColumnarSourceScoutEvidence {
        bytes: u64::try_from(byte_read.bytes.len())
            .map_err(|_| unsupported_sql_error("JSON source length does not fit in u64"))?,
        digest: fnv64_digest_bytes(&byte_read.bytes),
        fingerprint_kind: "local_file_content_digest".to_string(),
        fingerprint_policy: "content_digest".to_string(),
        identity_source: "local_file_explicit_proof_digest".to_string(),
        content_fingerprint_requested: true,
        content_fingerprint_performed: true,
        metadata_scout_millis: byte_read.source_metadata_scout_millis,
        byte_acquisition_millis: byte_read.source_byte_acquisition_millis,
        full_body_millis: byte_read.source_full_body_millis,
    };
    let build_start = Instant::now();
    let content = decode_local_text_source(
        &request.source_path,
        LocalSourceFormat::Json,
        byte_read.bytes,
    )?;
    let batch = parse(&content, limits.input_rows)?;
    drop(content);
    let header = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let column_arrow_dtypes = batch
        .schema()
        .fields()
        .iter()
        .map(|field| Some(field.data_type().clone()))
        .collect::<Vec<_>>();
    let schema = batch.schema();
    let rows = batch.num_rows();
    let batch_size = text_stream_record_batch_size(limits.input_rows);
    let batch_count = rows.div_ceil(batch_size);
    let reader = RecordBatchIterator::new(
        (0..rows)
            .step_by(batch_size)
            .map(move |start| Ok(batch.slice(start, batch_size.min(rows - start)))),
        schema,
    );
    let columnar_source = shardloom_vortex::FlatLocalColumnarStreamSource {
        column_dtypes: vec![None; header.len()],
        column_arrow_dtypes,
        materialized_columns: header.clone(),
        reader_projection_columns: header.clone(),
        header,
        row_count_hint: Some(rows),
        record_batch_count_hint: Some(batch_count),
        source_stream_batch_size: batch_size,
        source_stream_unit_count_hint: Some(batch_count),
        source_stream_unit_row_ranges: Some(
            (0..rows)
                .step_by(batch_size)
                .map(|start| (start, start + batch_size.min(rows - start)))
                .collect(),
        ),
        source_stream_unit_hint_kind: "text_record_batch_count".to_string(),
        source_stream_policy: format!(
            "whole_json_typed_columns_writer_batch_size_{batch_size}_rows"
        ),
        source_dictionary_preservation_status: "json_typed_builders_no_source_dictionary"
            .to_string(),
        ingest_executor_status: "whole_json_typed_columns_ready".to_string(),
        ingest_executor_kind: "whole_json_typed_columns_to_record_batch_slices".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(batch_count),
        source_identities: Vec::new(),
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        reader: Box::new(reader),
    };
    let columnar_source =
        shardloom_vortex::universal_format_io::with_embedded_derived_columns_columnar_stream_source(
            columnar_source,
        );
    let columnar_source = shardloom_vortex::with_capillary_prefetch_columnar_stream_source(
        columnar_source,
        request.max_parallelism,
    );
    let mut source = VortexIngestSourceData::from_columnar_stream_source(
        source_adapter,
        &columnar_source,
        scout,
        read_millis,
        build_start.elapsed().as_millis(),
    );
    source.read_plan = LocalSourceReadPlan::full("whole_json_typed_vortex_ingest_source_state");
    source.projection_pushdown_status =
        LocalSourceProjectionPushdownStatus::TextParserColumnPruning;
    source.materialization_layout = "whole_json_typed_columns_with_batched_writer";
    source.parse_normalization = "json_adapter_to_whole_typed_columns";
    // Like the prior JSON route, infer types without explicit schema hints.
    let source_schema_digest = fnv64_digest(&source.header.join(","));
    finish_text_streaming_vortex_prepare(
        request,
        source,
        columnar_source,
        source_schema_digest,
        prepare_start,
    )
}

fn parse(content: &str, max_rows: Option<usize>) -> Result<RecordBatch, ShardLoomError> {
    let read_plan = LocalSourceReadPlan::full("whole_json_typed_columns");
    let mut indices = BTreeMap::<String, usize>::new();
    let mut values = Vec::<ScalarValue>::new();
    let mut builders = Vec::<InferredTextColumnBuilder>::new();
    let mut row_count = 0;
    visit_json_source_rows_with_plan(content, &read_plan, |fields| {
        row_count += 1;
        enforce_local_source_row_budget(row_count, max_rows, "JSON")?;
        values.fill(ScalarValue::Null);
        for (name, value) in fields {
            let index = if let Some(&index) = indices.get(&name) {
                index
            } else {
                let index = builders.len();
                builders.push(InferredTextColumnBuilder::new(
                    &name,
                    row_count - 1,
                    CONTEXT,
                )?);
                values.push(ScalarValue::Null);
                indices.insert(name, index);
                index
            };
            values[index] = value;
        }
        for (builder, value) in builders.iter_mut().zip(&values) {
            builder.append(value)?;
        }
        Ok(())
    })?;
    if row_count == 0 {
        return Err(unsupported_sql_error(
            "JSON source must include at least one object row",
        ));
    }
    let (fields, arrays): (Vec<_>, Vec<_>) = builders
        .into_iter()
        .map(InferredTextColumnBuilder::finish)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        unsupported_sql_error(&format!("{CONTEXT} failed to construct columns: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(
                parse(content, None).unwrap(),
                reference(content).unwrap(),
                "{content}"
            );
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
            assert!(parse(content, None).is_err(), "candidate admits {content}");
        }
        assert!(
            parse(r#"[{"x":1},{"x":2}]"#, Some(1))
                .unwrap_err()
                .to_string()
                .contains("at most 1")
        );
        assert!(parse(r#"{"x":1}"#, Some(0)).is_err());
    }
}
