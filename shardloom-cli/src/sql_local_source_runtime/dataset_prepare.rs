//! Complete preparation uses the same held collection and format source owner.

use super::{
    ColumnarSourceScoutEvidence, Instant, LocalInputAdapterSelection, LocalSourceFormat,
    LocalSourceReadPlan, LogicalDType, Path, ShardLoomError, VortexIngestReport,
    VortexIngestRequest, VortexIngestSourceData, direct_text, finish_text_streaming_vortex_prepare,
    fnv64_digest, public_preparation_source_binding, unsupported_sql_error,
    vortex_ingest_schema_digest_from_parts,
};
use shardloom_vortex::universal_format_io::demanded::LocalColumnarSource;

pub(super) fn is_collection(path: &Path) -> bool {
    path.is_dir()
        || path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("manifest"))
        || path
            .as_os_str()
            .as_encoded_bytes()
            .iter()
            .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b']'))
}

// One complete-input lifecycle binds the collection, reader and publication proof.
#[allow(clippy::too_many_lines)]
pub(super) fn prepare(
    mut request: VortexIngestRequest,
    adapter: LocalInputAdapterSelection,
    hints: &[(String, LogicalDType)],
) -> Result<VortexIngestReport, ShardLoomError> {
    request.cancellation.check()?;
    let started = Instant::now();
    let memory = request.native_memory_pool()?;
    let format = adapter.source_format;
    let text = matches!(
        format,
        LocalSourceFormat::Csv
            | LocalSourceFormat::Tsv
            | LocalSourceFormat::Json
            | LocalSourceFormat::JsonLines
    );
    if !text && !hints.is_empty() {
        return Err(unsupported_sql_error(
            "columnar sources use their embedded schema; text schema hints are not admitted",
        ));
    }
    let schema = (!hints.is_empty()).then(|| {
        hints
            .iter()
            .map(|(name, dtype)| format!("{name}:{}", dtype.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    });
    if request.prepared_source_binding.is_none() {
        request.prepared_source_binding = Some(public_preparation_source_binding(
            &request.source_path,
            Some(format),
            request.source_fingerprint_policy,
            schema.as_deref(),
            &request.cancellation,
            request.float_policy,
            &memory,
        )?);
    }
    let limits = request.runtime_profile.read_limits();
    let read_start = Instant::now();
    let content = request
        .source_fingerprint_policy
        .content_fingerprint_requested();
    let source = if text {
        direct_text::admit_parsed(
            &request.source_path,
            format.as_str(),
            hints,
            limits.input_rows,
            content,
            &memory,
            &request.cancellation,
        )?
    } else {
        LocalColumnarSource::admit_complete(
            &request.source_path,
            format.as_str(),
            content,
            &memory,
            &request.cancellation,
        )?
    };
    let bytes = source.source_byte_size()?;
    if limits.source_bytes.is_some_and(|limit| bytes > limit) {
        return Err(unsupported_sql_error(
            "local dataset exceeds the configured source byte budget",
        ));
    }
    let digest = if content {
        fnv64_digest(&source.content_fingerprints().join(";"))
    } else {
        fnv64_digest(
            request
                .prepared_source_binding
                .as_deref()
                .expect("binding established"),
        )
    };
    let scout = ColumnarSourceScoutEvidence {
        bytes,
        digest,
        fingerprint_kind: format!(
            "local_dataset_{}",
            request.source_fingerprint_policy.fingerprint_kind()
        ),
        fingerprint_policy: request.source_fingerprint_policy.as_str().into(),
        identity_source: "held_local_dataset_file_generations_and_manifest".into(),
        content_fingerprint_requested: content,
        content_fingerprint_performed: content,
        metadata_scout_millis: read_start.elapsed().as_millis(),
        byte_acquisition_millis: 0,
        full_body_millis: 0,
    };
    let mut stream = source.complete_stream(limits.input_rows, &memory, &request.cancellation)?;
    stream.ingest_executor_requested_parallelism = request.resources.max_parallelism();
    let stream =
        shardloom_vortex::universal_format_io::with_embedded_derived_columns_columnar_stream_source(
            stream,
        );
    let stream = shardloom_vortex::universal_format_io::with_capillary_prefetch_columnar_stream_source_controlled(
        stream,
        request.resources.max_parallelism(),
        &request.cancellation,
    )?;
    let schema_digest = fnv64_digest(&vortex_ingest_schema_digest_from_parts(
        &stream.header,
        hints,
    ));
    let mut evidence = VortexIngestSourceData::from_columnar_stream_source(
        adapter,
        &stream,
        scout,
        read_start.elapsed().as_millis(),
        0,
    );
    evidence.read_plan = LocalSourceReadPlan::full("complete_local_dataset_stream");
    evidence.materialization_layout =
        "complete_local_dataset_to_streaming_arrow_record_batch_source_state";
    evidence.parse_normalization = "explicit_format_decode_and_union_by_name_alignment";
    // The source owner keeps all generations and membership alive through the
    // full reader; the writer checks its embedded binding again before commit.
    source.validate_output_destination(&request.target_path)?;
    finish_text_streaming_vortex_prepare(request, evidence, stream, schema_digest, started)
}
