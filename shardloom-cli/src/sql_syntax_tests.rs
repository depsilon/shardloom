//! Extracted unit test suite for `sql_syntax` (`E2E-ARCH-01`).

use super::*;

#[test]
fn unaliased_parameter_expressions_have_positional_names() {
    let parsed = parse_projection_list("id,value+:extra,LENGTH(label)").unwrap();
    assert_eq!(parsed.projection_order.len(), 3);
    assert_eq!(
        parsed.generic_expression_projections[0].alias,
        "expression_2"
    );
    assert_eq!(
        parsed.generic_expression_projections[1].alias,
        "expression_3"
    );
    assert_eq!(
        parsed.generic_expression_projections[0].source_columns,
        ["value"]
    );
    assert!(parse_projection_list("value+:extra; DROP TABLE facts").is_err());
}

#[test]
fn compact_comparisons_preserve_operators_and_quoted_literals() {
    for spelling in ["=", "!=", "<>", "<", "<=", ">", ">="] {
        let expected = parse_comparison_op(spelling).unwrap();
        assert!(matches!(
            parse_predicate(&format!("value{spelling}-3")).unwrap(),
            ParsedPredicate::Compare { column, op, value: ScalarValue::Int64(-3) }
                if column == "value" && op == expected
        ));
        assert!(is_explicit_predicate_projection_shape(&format!("value{spelling}-3")).unwrap());
    }
    assert!(matches!(
        parse_predicate("label='a>=b''<c'").unwrap(),
        ParsedPredicate::Compare { column, op: ComparisonOp::Eq, value: ScalarValue::Utf8(value) }
            if column == "label" && value == "a>=b'<c"
    ));
    for compound in [
        "value=1 AND metric>=2",
        "value=1 OR metric>=2",
        "(value>=2)",
    ] {
        assert!(is_explicit_predicate_projection_shape(compound).unwrap());
        assert!(parse_predicate(compound).is_ok(), "{compound}");
    }
    for invalid in ["value==3", "value<3>1", "value>=", "=3"] {
        assert!(parse_predicate(invalid).is_err(), "{invalid}");
    }
}

static SQL_LOCAL_SOURCE_TEST_PATH_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn sql_local_source_test_path(extension: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let counter =
        SQL_LOCAL_SOURCE_TEST_PATH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!(
        "shardloom-sql-local-source-{}-{nanos}-{counter}.{extension}",
        std::process::id(),
    ));
    path
}

fn field_map(fields: Vec<(String, String)>) -> BTreeMap<String, String> {
    fields.into_iter().collect()
}

fn assert_field_eq(fields: &BTreeMap<String, String>, key: &str, expected: &str) {
    assert_eq!(
        fields.get(key).map(String::as_str),
        Some(expected),
        "field {key}"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn assert_text_discovery_policy(fields: &BTreeMap<String, String>, expected: &str) {
    let policy = &fields["source_state_stream_policy"];
    let parts = policy.split(';').collect::<Vec<_>>();
    assert_eq!(parts[0], expected);
    assert!(parts.contains(&"record_batch_stream_batch_size_8192"));
    assert!(parts.contains(&"single_record_and_credited_batch"));
    let bytes = parts
        .iter()
        .filter_map(|part| part.strip_prefix("schema_discovery_read_bytes="))
        .map(|bytes| bytes.parse::<u64>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(bytes.len(), 1);
    assert!(
        bytes[0] > 0,
        "complete discovery must report its source reads"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn assert_ingest_stream_lane_recipe(fields: &BTreeMap<String, String>, requested: usize) {
    // Small finite-budget fixtures run ready source, conversion, and
    // provider tasks on the shared native runtime. Task-window capacity
    // does not imply dedicated threads.
    let (source, conversion, provider, prefetch) = match requested {
        2 => (0, 0, 1, 2),
        4 => (0, 0, 3, 4),
        _ => panic!("fixture has no independently specified lane recipe"),
    };
    let topology = fields["vortex_writer_physical_design_writer_queue_topology"]
        .split(';')
        .filter_map(|item| item.split_once('='))
        .collect::<BTreeMap<_, _>>();
    for (key, expected) in [
        ("ingest_cpu_requested", requested),
        ("ingest_cpu_configured", requested),
        ("ingest_cpu_caller", 1),
        ("ingest_cpu_source_drivers", source),
        ("ingest_cpu_conversion_drivers", conversion),
        ("ingest_cpu_provider_drivers", provider),
        ("array_prefetch_window", prefetch),
    ] {
        assert_eq!(topology[key].parse::<usize>().unwrap(), expected, "{key}");
    }
    assert_eq!(1 + source + conversion + provider, requested);
    assert_eq!(
        topology["driver_lifetime"],
        "joined_before_artifact_call_returns"
    );
    assert_eq!(
        topology["ingest_cpu_scope"],
        "shardloom_owned_drivers_excludes_blocking_io_and_source_library_internal_threads"
    );
    assert_eq!(
        topology["lane_reassignment"],
        "ready_source_conversion_and_provider_tasks_share_executor"
    );
    assert_eq!(
        topology["array_build_workers_scope"],
        "concurrent_tasks_not_dedicated_threads"
    );
    assert_field_eq(
        fields,
        "vortex_writer_runtime_requested_parallelism",
        &requested.to_string(),
    );
    assert_field_eq(
        fields,
        "vortex_writer_runtime_applied_parallelism",
        &requested.to_string(),
    );
    assert_field_eq(
        fields,
        "vortex_writer_runtime_background_workers",
        &provider.to_string(),
    );
    assert_field_eq(
        fields,
        "vortex_writer_physical_design_source_executor_applied_parallelism",
        "1",
    );
    assert_field_eq(
        fields,
        "vortex_writer_physical_design_array_build_worker_count",
        &prefetch.to_string(),
    );
    assert_field_eq(
        fields,
        "vortex_array_build_prefetch_window",
        &prefetch.to_string(),
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn assert_field_contains(fields: &BTreeMap<String, String>, key: &str, expected: &str) {
    let value = fields
        .get(key)
        .unwrap_or_else(|| panic!("field {key} must be present"));
    assert!(
        value.contains(expected),
        "field {key} value {value:?} must contain {expected:?}"
    );
}

#[test]
fn local_source_read_budget_rejects_oversized_regular_file() {
    let path = sql_local_source_test_path("csv");
    let file = fs::File::create(&path).expect("create sparse local source");
    file.set_len(MAX_LOCAL_SOURCE_BYTES + 1)
        .expect("set sparse source length");

    let error = read_local_source_bytes_with_budget(&path, "csv", Some(MAX_LOCAL_SOURCE_BYTES))
        .expect_err("oversized read blocked");
    let _ = fs::remove_file(&path);

    assert!(
        error
            .to_string()
            .contains("scoped local-source evidence reads admit at most"),
        "{error}"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn product_columnar_source_fingerprint_is_metadata_only_by_default() {
    let path = sql_local_source_test_path("parquet");
    fs::write(&path, b"same-sized content variant one").expect("write source");

    let first = fingerprint_local_source_file_with_budget_report(
        &path,
        "Parquet",
        None,
        SourceFingerprintPolicy::MetadataOnly,
        &CancellationToken::default(),
    )
    .expect("fingerprint first source");
    fs::write(&path, b"same-sized content variant two").expect("rewrite source");
    let second = fingerprint_local_source_file_with_budget_report(
        &path,
        "Parquet",
        None,
        SourceFingerprintPolicy::MetadataOnly,
        &CancellationToken::default(),
    )
    .expect("fingerprint second source");

    assert_eq!(first.bytes, second.bytes);
    assert!(first.digest.starts_with("fnv64:"));
    assert!(second.digest.starts_with("fnv64:"));
    assert_eq!(first.fingerprint_kind, "local_file_metadata_size_mtime");
    assert_eq!(first.fingerprint_policy, "metadata_only");
    assert_eq!(
        first.identity_source,
        "local_file_metadata_fast_prepare_identity"
    );
    assert!(!first.content_fingerprint_requested);
    assert!(!first.content_fingerprint_performed);
    assert_eq!(first.byte_acquisition_millis, 0);
    assert_eq!(first.full_body_millis, 0);
    assert!(!second.content_fingerprint_performed);

    fs::remove_file(&path).expect("remove source");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn product_columnar_source_content_fingerprint_is_explicit_proof() {
    let path = sql_local_source_test_path("parquet");
    fs::write(&path, b"same-sized content variant one").expect("write source");

    let first = fingerprint_local_source_file_with_budget_report(
        &path,
        "Parquet",
        None,
        SourceFingerprintPolicy::ContentDigest,
        &CancellationToken::default(),
    )
    .expect("fingerprint first source");
    fs::write(&path, b"same-sized content variant two").expect("rewrite source");
    let second = fingerprint_local_source_file_with_budget_report(
        &path,
        "Parquet",
        None,
        SourceFingerprintPolicy::ContentDigest,
        &CancellationToken::default(),
    )
    .expect("fingerprint second source");

    assert_eq!(first.bytes, second.bytes);
    assert!(first.digest.starts_with("fnv64:"));
    assert!(second.digest.starts_with("fnv64:"));
    assert_eq!(first.fingerprint_kind, "local_file_content_digest");
    assert_eq!(first.fingerprint_policy, "content_digest");
    assert_eq!(first.identity_source, "local_file_explicit_proof_digest");
    assert!(first.content_fingerprint_requested);
    assert!(first.content_fingerprint_performed);
    assert_ne!(first.digest, second.digest);
    assert_eq!(first.full_body_millis, 0);
    assert_eq!(second.full_body_millis, 0);

    fs::remove_file(&path).expect("remove source");
}

#[test]
fn product_local_workflow_profile_disables_synthetic_caps() {
    let plan = LocalSourceReadPlan::required(
        BTreeSet::from(["id".to_string()]),
        "test_product_local_required_columns",
    );
    let mut content = String::from("id,label\n");
    for row_id in 0..=SMOKE_MAX_INPUT_ROWS {
        writeln!(&mut content, "{row_id},label-{row_id}").expect("write csv row");
    }

    let smoke_error =
        parse_csv_source_content_with_plan(&content, &plan, Some(SMOKE_MAX_INPUT_ROWS))
            .expect_err("smoke row cap remains enforced");
    let (header, rows) = parse_csv_source_content_with_plan(&content, &plan, None)
        .expect("product local workflow has no synthetic row cap");

    assert!(
        smoke_error
            .to_string()
            .contains("supports at most 50000 CSV data rows"),
        "{smoke_error}"
    );
    assert_eq!(header, vec!["id", "label"]);
    assert_eq!(rows.len(), SMOKE_MAX_INPUT_ROWS + 1);
    assert_eq!(
        SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.input_row_cap_label(),
        "none_synthetic_row_cap_disabled"
    );

    assert!(!SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_input_row_cap_enabled());
    assert!(!SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_output_row_cap_enabled());
    assert!(
        !SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_source_byte_cap_enabled()
    );
    assert!(
        !SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_join_candidate_cap_enabled()
    );
    assert_eq!(
        SqlLocalSourceRuntimeProfile::ProductLocalWorkflow
            .read_limits()
            .output_rows,
        None
    );
    assert_eq!(
        SqlLocalSourceRuntimeProfile::ProductLocalWorkflow
            .read_limits()
            .join_candidate_rows,
        None
    );
    assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_input_row_cap_enabled());
    assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_output_row_cap_enabled());
    assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_source_byte_cap_enabled());
    assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_join_candidate_cap_enabled());

    assert_eq!(
        SqlLocalSourceRuntimeProfile::Smoke
            .read_limits()
            .output_rows,
        Some(SMOKE_MAX_OUTPUT_ROWS)
    );
    assert_eq!(
        SqlLocalSourceRuntimeProfile::Smoke
            .read_limits()
            .join_candidate_rows,
        Some(SMOKE_MAX_JOIN_CANDIDATE_ROWS)
    );
}

#[test]
fn source_unit_split_row_ranges_use_exact_source_units_only() {
    assert_eq!(
        source_unit_split_row_ranges(10, Some(&[(0, 2), (2, 7), (7, 10)])),
        vec![(0, 2), (2, 7), (7, 10)]
    );
    assert_eq!(source_unit_split_row_ranges(10, None), vec![(0, 10)]);
    assert_eq!(
        source_unit_split_row_ranges(10, Some(&[(0, 3), (4, 10)])),
        vec![(0, 10)]
    );
    assert_eq!(
        source_unit_split_row_ranges(10, Some(&[(0, 3), (3, 9)])),
        vec![(0, 10)]
    );
    assert_eq!(
        source_unit_split_row_ranges(0, Some(&[(0, 0)])),
        Vec::<(usize, usize)>::new()
    );
}

#[test]
fn public_workflow_preparation_preserves_observed_stage_scopes_without_invented_zeros() {
    let raw_fields = vec![
        (
            "vortex_ingest_ownership_copy_reasons".to_string(),
            r#"{"nonzero_pointer_offset":{"regions":2,"visible_bytes":19}}"#.to_string(),
        ),
        (
            "vortex_ingest_conversion_prefetch_shrunk_windows".to_string(),
            "3".to_string(),
        ),
        (
            "vortex_ingest_stream_validation_work_nanos".to_string(),
            "452".to_string(),
        ),
        (
            "vortex_ingest_stage_time_scope".to_string(),
            "overlapping_elapsed_work".to_string(),
        ),
        (
            "vortex_shared_native_memory_peak_reserved_bytes".to_string(),
            "8192".to_string(),
        ),
        (
            "vortex_shared_native_memory_exclusions".to_string(),
            "provider_bypass;rss".to_string(),
        ),
        (
            "vortex_ingest_unapproved_internal_field".to_string(),
            "private".to_string(),
        ),
    ];
    let fields = field_map(public_workflow_preparation_fields(&raw_fields));
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_ingest_ownership_copy_reasons",
        r#"{"nonzero_pointer_offset":{"regions":2,"visible_bytes":19}}"#,
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_ingest_conversion_prefetch_shrunk_windows",
        "3",
    );
    assert!(!fields.contains_key(
        "public_workflow_preparation_vortex_ingest_conversion_prefetch_grown_windows"
    ));
    assert!(
        !fields
            .contains_key("public_workflow_preparation_vortex_ingest_ownership_copy_reason_scope")
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_ingest_stream_validation_work_nanos",
        "452",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_ingest_stage_time_scope",
        "overlapping_elapsed_work",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_shared_native_memory_peak_reserved_bytes",
        "8192",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_shared_native_memory_exclusions",
        "provider_bypass;rss",
    );
    assert!(!fields.contains_key("public_workflow_preparation_vortex_ingest_text_zstd_work_nanos"));
    assert!(
        !fields.contains_key("public_workflow_preparation_vortex_ingest_unapproved_internal_field")
    );
}

#[test]
fn public_workflow_preparation_fields_keep_product_stream_source_evidence() {
    let raw_fields = vec![
        ("source_state_stream_batch_size".to_string(), "262144".to_string()),
        (
            "source_state_stream_unit_count_hint".to_string(),
            "8".to_string(),
        ),
        (
            "source_state_stream_unit_hint_kind".to_string(),
            "parquet_adaptive_row_group_task_count".to_string(),
        ),
        (
            "source_state_stream_policy".to_string(),
            "product_columnar_stream_batch_size_262144_rows;source_unit_interface=capillary_source_extents;source_unit_byte_range_count=8;source_unit_physical_bytes=8192;source_unit_byte_range_sample=0..1024|1024..2048;source_unit_physical_source=file_local_range_read_eligible;source_unit_scheduler_wait_status=bounded_ordered_delivery;source_unit_decode_wait_status=measured_in_stream_source_pull_micros;source_unit_writer_starvation_status=surfaced_as_prepare_writer_backpressure_or_timer_overlap;source_unit_physical_bandwidth_status=derivable_from_source_bytes_and_prepare_source_hydration_millis;parquet_extent_row_groups=8;parquet_extent_row_groups_with_byte_ranges=8;parquet_extent_column_chunks=64;parquet_extent_column_chunks_with_byte_ranges=64;parquet_extent_compressed_bytes=8192;parquet_extent_uncompressed_bytes=16384;parquet_extent_codec_summary=SNAPPY:64".to_string(),
        ),
        (
            "source_state_dictionary_preservation_status".to_string(),
            "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary;parquet_extent_dictionary_pages=8;parquet_extent_statistics=64;parquet_extent_row_group_summary=rg=0,rows=2,chunks=8,compressed_bytes=1024,uncompressed_bytes=2048,start=0,len=1024"
                .to_string(),
        ),
        (
            "not_selected_internal_field".to_string(),
            "should_drop".to_string(),
        ),
    ];
    let fields = field_map(public_workflow_preparation_fields(&raw_fields));

    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_batch_size",
        "262144",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_unit_count_hint",
        "8",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_unit_hint_kind",
        "parquet_adaptive_row_group_task_count",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_policy",
        "product_columnar_stream_batch_size_262144_rows;source_unit_interface=capillary_source_extents;source_unit_byte_range_count=8;source_unit_physical_bytes=8192;source_unit_byte_range_sample=0..1024|1024..2048;source_unit_physical_source=file_local_range_read_eligible;source_unit_scheduler_wait_status=bounded_ordered_delivery;source_unit_decode_wait_status=measured_in_stream_source_pull_micros;source_unit_writer_starvation_status=surfaced_as_prepare_writer_backpressure_or_timer_overlap;source_unit_physical_bandwidth_status=derivable_from_source_bytes_and_prepare_source_hydration_millis;parquet_extent_row_groups=8;parquet_extent_row_groups_with_byte_ranges=8;parquet_extent_column_chunks=64;parquet_extent_column_chunks_with_byte_ranges=64;parquet_extent_compressed_bytes=8192;parquet_extent_uncompressed_bytes=16384;parquet_extent_codec_summary=SNAPPY:64",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_unit_interface",
        "capillary_source_extents",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_unit_byte_range_count",
        "8",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_unit_physical_bytes",
        "8192",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_stream_unit_physical_source",
        "file_local_range_read_eligible",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_parquet_extent_row_group_count",
        "8",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_parquet_extent_column_chunk_count",
        "64",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_parquet_extent_compressed_bytes",
        "8192",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_parquet_extent_codec_summary",
        "SNAPPY:64",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_dictionary_preservation_status",
        "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary;parquet_extent_dictionary_pages=8;parquet_extent_statistics=64;parquet_extent_row_group_summary=rg=0,rows=2,chunks=8,compressed_bytes=1024,uncompressed_bytes=2048,start=0,len=1024",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_parquet_extent_dictionary_page_count",
        "8",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_source_state_parquet_extent_statistics_count",
        "64",
    );
    assert!(!fields.contains_key("public_workflow_preparation_not_selected_internal_field"));
}

#[test]
fn layout_writer_provider_uses_batch_presence_for_zero_row_columnar_sources() {
    let source_adapter =
        LocalInputAdapterSelection::infer_from_path(Path::new("schema-only.parquet"))
            .expect("infer parquet adapter");
    let source = VortexIngestSourceData {
        source_format: source_adapter.source_format,
        source_adapter,
        header: vec!["id".to_string()],
        column_arrow_dtypes: vec![Some(DataType::Int64)],
        #[cfg(feature = "universal-format-io")]
        first_batch_profile: None,
        read_plan: LocalSourceReadPlan::full("test_zero_row_columnar_source"),
        materialized_columns: vec!["id".to_string()],
        reader_projection_columns: vec!["id".to_string()],
        projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
        source_bytes: 128,
        source_digest: "fnv64:test".to_string(),
        source_fingerprint: SourceFingerprintEvidence::metadata_only(),
        row_count: 0,
        row_count_known: true,
        source_split_row_ranges: vec![(0, 0)],
        source_metadata_scout_millis: 0,
        source_byte_acquisition_millis: 0,
        source_full_body_millis: 0,
        read_millis: 0,
        compatibility_parse_millis: 0,
        source_to_columnar_millis: 0,
        record_batch_count: 1,
        source_stream_batch_size: 0,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
        source_stream_policy: "test_source_defined_record_batches".to_string(),
        source_dictionary_preservation_status: "test_not_applicable".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        materialization_layout: "arrow_record_batch_columnar_source_state",
        parse_normalization: "structured_reader_to_arrow_record_batches",
        columnar_source_preserved: true,
    };

    assert_eq!(layout_writer_provider_kind(&source), "vortex_array_kernel");
    assert_eq!(
        layout_writer_provider_surface(&source),
        "ArrayRef::from_arrow(RecordBatch);VortexSession::write_options().write(ArrayStream)"
    );
}

#[test]
fn layout_writer_provider_uses_streaming_vortex_provider_for_non_empty_columnar_source() {
    let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("hits.parquet"))
        .expect("infer parquet adapter");
    let source = VortexIngestSourceData {
        source_format: source_adapter.source_format,
        source_adapter,
        header: vec!["URL".to_string()],
        column_arrow_dtypes: vec![Some(DataType::Utf8)],
        #[cfg(feature = "universal-format-io")]
        first_batch_profile: None,
        read_plan: LocalSourceReadPlan::full("test_streaming_columnar_source"),
        materialized_columns: vec!["URL".to_string()],
        reader_projection_columns: vec!["URL".to_string()],
        projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
        source_bytes: 1024,
        source_digest: "fnv64:test".to_string(),
        source_fingerprint: SourceFingerprintEvidence::metadata_only(),
        row_count: 100_000_000,
        row_count_known: true,
        source_split_row_ranges: vec![(0, 100_000_000)],
        source_metadata_scout_millis: 0,
        source_byte_acquisition_millis: 0,
        source_full_body_millis: 0,
        read_millis: 0,
        compatibility_parse_millis: 0,
        source_to_columnar_millis: 0,
        record_batch_count: 0,
        source_stream_batch_size: 262_144,
        source_stream_unit_count_hint: Some(8),
        source_stream_unit_hint_kind: "parquet_adaptive_row_group_task_count".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status:
            "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary"
                .to_string(),
        ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
        ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
        ingest_executor_requested_parallelism: 2,
        ingest_executor_applied_parallelism: 2,
        ingest_executor_unit_count_hint: None,
        materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
        parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
        columnar_source_preserved: true,
    };

    assert_eq!(layout_writer_provider_kind(&source), "vortex_array_kernel");
    assert_eq!(
        layout_writer_provider_surface(&source),
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)"
    );
    assert_eq!(
        layout_workload_constitution(&source),
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_text_olap;layout_family=url_domain_dictionary_length_layout;text_domain=true;time_bucket=false;counter=false;key_profile=dictionary_text_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
    );
    assert_eq!(
        layout_expected_read_tradeoff(&source),
        "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution"
    );
    assert_eq!(
        layout_expected_write_tradeoff(&source),
        "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
    );
}

#[test]
fn layout_writer_compression_candidates_use_source_dtypes_before_name_heuristics() {
    let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("hits.parquet"))
        .expect("infer parquet adapter");
    let source = VortexIngestSourceData {
        source_format: source_adapter.source_format,
        source_adapter,
        header: vec![
            "URL".to_string(),
            "Title".to_string(),
            "ClientIP".to_string(),
            "__shardloom_derived_utf8_len_URL".to_string(),
            "SearchPhrase".to_string(),
            "BrowserCountry".to_string(),
            "ParamCurrency".to_string(),
            "Params".to_string(),
        ],
        column_arrow_dtypes: vec![
            Some(DataType::Utf8),
            Some(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            )),
            Some(DataType::UInt32),
            Some(DataType::UInt32),
            None,
            Some(DataType::Utf8),
            Some(DataType::Utf8),
            Some(DataType::Utf8),
        ],
        #[cfg(feature = "universal-format-io")]
        first_batch_profile: None,
        read_plan: LocalSourceReadPlan::full("test_writer_candidates"),
        materialized_columns: vec!["URL".to_string(), "Title".to_string()],
        reader_projection_columns: vec!["URL".to_string(), "Title".to_string()],
        projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
        source_bytes: 1024,
        source_digest: "fnv64:test".to_string(),
        source_fingerprint: SourceFingerprintEvidence::metadata_only(),
        row_count: 100_000_000,
        row_count_known: true,
        source_split_row_ranges: vec![(0, 100_000_000)],
        source_metadata_scout_millis: 0,
        source_byte_acquisition_millis: 0,
        source_full_body_millis: 0,
        read_millis: 0,
        compatibility_parse_millis: 0,
        source_to_columnar_millis: 0,
        record_batch_count: 0,
        source_stream_batch_size: 262_144,
        source_stream_unit_count_hint: Some(8),
        source_stream_unit_hint_kind: "test_units".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status:
            "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary"
                .to_string(),
        ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
        ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
        ingest_executor_requested_parallelism: 2,
        ingest_executor_applied_parallelism: 2,
        ingest_executor_unit_count_hint: None,
        materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
        parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
        columnar_source_preserved: true,
    };

    assert_eq!(
        layout_writer_compression_candidate_fields(&source),
        vec![
            "URL".to_string(),
            "Title".to_string(),
            "BrowserCountry".to_string(),
            "ParamCurrency".to_string(),
            "Params".to_string(),
        ]
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn large_text_domain_columnar_source_does_not_synthesize_full_url_metadata() {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchReader, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::collections::VecDeque;
    use std::sync::Arc;

    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front().map(Ok)
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("Referer", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("https://www.google.com/search"),
                Some("https://example.com/page"),
                None,
            ])),
        ],
    )
    .expect("record batch");
    let first_batch_profile = Some(
        shardloom_vortex::universal_format_io::first_batch_profile::FirstBatchProfile::from_record_batch(&batch),
    );
    let source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header: vec!["id".to_string(), "Referer".to_string()],
        column_dtypes: vec![Some(LogicalDType::Int64), Some(LogicalDType::Utf8)],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
        materialized_columns: vec!["id".to_string(), "Referer".to_string()],
        reader_projection_columns: vec!["id".to_string(), "Referer".to_string()],
        row_count_hint: Some(100_000_000),
        record_batch_count_hint: Some(1),
        source_stream_batch_size:
            shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_row_ranges: Some(vec![(0, 3)]),
        source_stream_unit_hint_kind: "test_large_text_domain_record_batch".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status:
            "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary"
                .to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_large_text_domain_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        first_batch_profile,
        reader: Box::new(TestRecordBatchReader {
            schema,
            batches: VecDeque::from([batch]),
        }),
    };

    let mut source = with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

    assert!(
        source.source_dictionary_preservation_status.contains(
            "source_native_embedded_derived_columns=not_available_for_current_arrow_layout"
        ),
        "{}",
        source.source_dictionary_preservation_status
    );
    assert!(
        !source
            .source_dictionary_preservation_status
            .contains("__shardloom_derived_url_domain_Referer"),
        "{}",
        source.source_dictionary_preservation_status
    );
    let names = source
        .reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["id".to_string(), "Referer".to_string()]);
    let batch = source.reader.next().expect("batch").expect("batch ok");
    assert_eq!(batch.num_columns(), 2);
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn large_plain_utf8_parquet_guard_uses_lean_runtime_metadata_profile() {
    large_utf8_parquet_guard_prepared_batch(false);
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn large_utf8_view_parquet_guard_uses_same_lean_runtime_metadata() {
    use arrow_array::{StringArray, StringViewArray};

    let plain = large_utf8_parquet_guard_prepared_batch(false);
    let views = large_utf8_parquet_guard_prepared_batch(true);
    assert_eq!(plain.num_rows(), views.num_rows());
    assert_eq!(plain.num_columns(), views.num_columns());
    for (expected, actual) in plain.columns().iter().zip(views.columns()) {
        if let Some(expected) = expected.as_any().downcast_ref::<StringArray>() {
            let actual = actual
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("string views");
            assert_eq!(
                expected.iter().collect::<Vec<_>>(),
                actual.iter().collect::<Vec<_>>()
            );
        } else {
            assert_eq!(expected.to_data(), actual.to_data());
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn large_utf8_parquet_guard_prepared_batch(use_views: bool) -> arrow_array::RecordBatch {
    use arrow_array::{
        ArrayRef, Int64Array, RecordBatch, RecordBatchReader, StringArray, StringViewArray,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::collections::VecDeque;
    use std::sync::Arc;

    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front().map(Ok)
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let text_type = if use_views {
        DataType::Utf8View
    } else {
        DataType::Utf8
    };
    let text_array = |values: Vec<Option<&str>>| -> ArrayRef {
        if use_views {
            Arc::new(StringViewArray::from(values))
        } else {
            Arc::new(StringArray::from(values))
        }
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("URL", text_type.clone(), true),
        Field::new("SearchPhrase", text_type.clone(), true),
        Field::new("Title", text_type, true),
        Field::new("EventTime", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            text_array(vec![
                Some("https://www.google.com/a"),
                Some("https://docs.rs/b"),
                None,
                Some("https://example.com/日本語"),
            ]),
            text_array(vec![Some("rust"), Some("shardloom"), None, Some("日本語")]),
            text_array(vec![Some("Tag01"), Some("Tag02"), None, Some("Tag03")]),
            Arc::new(Int64Array::from(vec![
                1_700_002_860_i64,
                1_700_002_921,
                1_700_002_980,
                1_700_003_041,
            ])) as ArrayRef,
        ],
    )
    .expect("record batch");
    let first_batch_profile = Some(
        shardloom_vortex::universal_format_io::first_batch_profile::FirstBatchProfile::from_record_batch(&batch),
    );
    let source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header: vec![
            "URL".to_string(),
            "SearchPhrase".to_string(),
            "Title".to_string(),
            "EventTime".to_string(),
        ],
        column_dtypes: vec![
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Int64),
        ],
        column_arrow_dtypes: schema
            .fields()
            .iter()
            .map(|field| Some(field.data_type().clone()))
            .collect(),
        materialized_columns: vec![
            "URL".to_string(),
            "SearchPhrase".to_string(),
            "Title".to_string(),
            "EventTime".to_string(),
        ],
        reader_projection_columns: vec![
            "URL".to_string(),
            "SearchPhrase".to_string(),
            "Title".to_string(),
            "EventTime".to_string(),
        ],
        row_count_hint: Some(100_000_000),
        record_batch_count_hint: Some(1),
        source_stream_batch_size:
            shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_row_ranges: Some(vec![(0, 4)]),
        source_stream_unit_hint_kind: "test_plain_utf8_large_parquet_guard".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status: if use_views {
            "parquet_arrow_reader_uses_utf8_view_for_large_olap_text_zstd_artifact_size_guard"
        } else {
            "parquet_arrow_reader_uses_plain_utf8_for_large_olap_text_zstd_artifact_size_guard"
        }
        .to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_plain_utf8_large_parquet_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        first_batch_profile,
        reader: Box::new(TestRecordBatchReader {
            schema,
            batches: VecDeque::from([batch]),
        }),
    };

    let mut source = with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

    assert!(
        source.source_dictionary_preservation_status.contains(
            "embedded_derived_column_mode=source_native_lean_runtime_dictionary_or_typed_time_only"
        ),
        "{}",
        source.source_dictionary_preservation_status
    );
    let names = source
        .reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert!(names.contains(&"__shardloom_derived_utf8_len_URL".to_string()));
    assert!(names.contains(&"__shardloom_derived_url_domain_URL".to_string()));
    assert!(names.contains(&"__shardloom_derived_utf8_len_SearchPhrase".to_string()));
    assert!(names.contains(&"__shardloom_derived_extract_minute_EventTime".to_string()));
    assert!(names.contains(&"__shardloom_derived_date_trunc_minute_EventTime".to_string()));
    assert!(!names.contains(&"__shardloom_derived_utf8_len_Title".to_string()));
    let batch = source.reader.next().expect("batch").expect("batch ok");
    assert_eq!(batch.num_columns(), 9);
    assert_eq!(batch.num_rows(), 4);
    batch
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn large_dictionary_columnar_source_uses_lean_runtime_metadata_profile() {
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::collections::VecDeque;
    use std::sync::Arc;

    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front().map(Ok)
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let dictionary_array = |values: &[Option<&str>]| -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for value in values {
            if let Some(value) = value {
                builder.append(value).expect("append dictionary value");
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish()) as ArrayRef
    };
    let dictionary_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("URL", dictionary_type.clone(), true),
        Field::new("Referer", dictionary_type.clone(), true),
        Field::new("SearchPhrase", dictionary_type.clone(), true),
        Field::new("Title", dictionary_type.clone(), true),
        Field::new("OriginalURL", dictionary_type, true),
        Field::new("EventTime", DataType::Int64, false),
        Field::new("ClientEventTime", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            dictionary_array(&[Some("https://www.google.com/a"), Some("https://docs.rs/b")]),
            dictionary_array(&[Some("https://ref.example/a"), Some("https://google.com/r")]),
            dictionary_array(&[Some("rust"), Some("shardloom")]),
            dictionary_array(&[Some("Tag01"), Some("Tag02")]),
            dictionary_array(&[Some("home"), Some("page")]),
            Arc::new(Int64Array::from(vec![1_700_002_860_i64, 1_700_002_921])) as ArrayRef,
            Arc::new(Int64Array::from(vec![300_i64, 360])) as ArrayRef,
        ],
    )
    .expect("record batch");
    let first_batch_profile = Some(
        shardloom_vortex::universal_format_io::first_batch_profile::FirstBatchProfile::from_record_batch(&batch),
    );
    let source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header: vec![
            "URL".to_string(),
            "Referer".to_string(),
            "SearchPhrase".to_string(),
            "Title".to_string(),
            "OriginalURL".to_string(),
            "EventTime".to_string(),
            "ClientEventTime".to_string(),
        ],
        column_dtypes: vec![
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Int64),
            Some(LogicalDType::Int64),
        ],
        column_arrow_dtypes: schema
            .fields()
            .iter()
            .map(|field| Some(field.data_type().clone()))
            .collect(),
        materialized_columns: vec![
            "URL".to_string(),
            "Referer".to_string(),
            "SearchPhrase".to_string(),
            "Title".to_string(),
            "OriginalURL".to_string(),
            "EventTime".to_string(),
            "ClientEventTime".to_string(),
        ],
        reader_projection_columns: vec![
            "URL".to_string(),
            "Referer".to_string(),
            "SearchPhrase".to_string(),
            "Title".to_string(),
            "OriginalURL".to_string(),
            "EventTime".to_string(),
            "ClientEventTime".to_string(),
        ],
        row_count_hint: Some(100_000_000),
        record_batch_count_hint: Some(1),
        source_stream_batch_size:
            shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_row_ranges: Some(vec![(0, 2)]),
        source_stream_unit_hint_kind: "test_lean_dictionary_record_batch".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status:
            "parquet_arrow_reader_requested_dictionary_preservation_for_string_derived_columns"
                .to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_dictionary_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        first_batch_profile,
        reader: Box::new(TestRecordBatchReader {
            schema,
            batches: VecDeque::from([batch]),
        }),
    };

    let mut source = with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

    assert!(
        source.source_dictionary_preservation_status.contains(
            "embedded_derived_column_mode=source_native_lean_runtime_dictionary_or_typed_time_only"
        ),
        "{}",
        source.source_dictionary_preservation_status
    );
    let names = source
        .reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert!(names.contains(&"__shardloom_derived_utf8_len_URL".to_string()));
    assert!(names.contains(&"__shardloom_derived_url_domain_URL".to_string()));
    assert!(names.contains(&"__shardloom_derived_utf8_len_Referer".to_string()));
    assert!(names.contains(&"__shardloom_derived_url_domain_Referer".to_string()));
    assert!(names.contains(&"__shardloom_derived_utf8_len_SearchPhrase".to_string()));
    assert!(names.contains(&"__shardloom_derived_extract_minute_EventTime".to_string()));
    assert!(names.contains(&"__shardloom_derived_date_trunc_minute_EventTime".to_string()));
    assert!(!names.contains(&"__shardloom_derived_utf8_len_Title".to_string()));
    assert!(!names.contains(&"__shardloom_derived_utf8_len_OriginalURL".to_string()));
    assert!(!names.contains(&"__shardloom_derived_url_domain_OriginalURL".to_string()));
    assert!(!names.contains(&"__shardloom_derived_extract_minute_ClientEventTime".to_string()));
    assert!(!names.contains(&"__shardloom_derived_date_trunc_minute_ClientEventTime".to_string()));
    assert_eq!(
        source
            .reader
            .next()
            .expect("batch")
            .expect("batch ok")
            .num_columns(),
        14
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn source_native_time_metadata_does_not_force_full_text_domain_metadata() {
    use arrow_array::{Array as _, Int64Array, RecordBatch, RecordBatchReader, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::collections::VecDeque;
    use std::sync::Arc;

    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front().map(Ok)
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("EventTime", DataType::Int64, false),
        Field::new("Referer", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![
                1_700_002_860_i64,
                1_700_002_921,
                1_700_002_980,
            ])),
            Arc::new(StringArray::from(vec![
                Some("https://www.google.com/search"),
                Some("https://example.com/page"),
                None,
            ])),
        ],
    )
    .expect("record batch");
    let first_batch_profile = Some(
        shardloom_vortex::universal_format_io::first_batch_profile::FirstBatchProfile::from_record_batch(&batch),
    );
    let source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header: vec!["EventTime".to_string(), "Referer".to_string()],
        column_dtypes: vec![Some(LogicalDType::Int64), Some(LogicalDType::Utf8)],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
        materialized_columns: vec!["EventTime".to_string(), "Referer".to_string()],
        reader_projection_columns: vec!["EventTime".to_string(), "Referer".to_string()],
        row_count_hint: Some(100_000_000),
        record_batch_count_hint: Some(1),
        source_stream_batch_size:
            shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_row_ranges: Some(vec![(0, 3)]),
        source_stream_unit_hint_kind: "test_time_plus_text_record_batch".to_string(),
        source_stream_policy: "test_time_plus_text_units".to_string(),
        source_dictionary_preservation_status:
            "parquet_arrow_reader_preserves_source_native_time_metadata".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_time_plus_text_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        first_batch_profile,
        reader: Box::new(TestRecordBatchReader {
            schema,
            batches: VecDeque::from([batch]),
        }),
    };

    let mut source = with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

    assert!(
        source
            .source_dictionary_preservation_status
            .contains("embedded_derived_column_mode=source_native_dictionary_or_typed_time_only"),
        "{}",
        source.source_dictionary_preservation_status
    );
    let names = source
        .reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert!(names.contains(&"__shardloom_derived_extract_minute_EventTime".to_string()));
    assert!(names.contains(&"__shardloom_derived_date_trunc_minute_EventTime".to_string()));
    assert!(!names.contains(&"__shardloom_derived_utf8_len_Referer".to_string()));
    assert!(!names.contains(&"__shardloom_derived_url_domain_Referer".to_string()));

    let batch = source.reader.next().expect("batch").expect("batch ok");
    let event_minutes = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::UInt8Array>()
        .expect("minute uint8");
    assert_eq!(event_minutes.value(0), 1);
    assert_eq!(event_minutes.value(1), 2);
    assert_eq!(event_minutes.value(2), 3);
    let event_minute_buckets = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("minute bucket int64");
    assert_eq!(event_minute_buckets.value(0), 1_700_002_860);
    assert_eq!(event_minute_buckets.value(1), 1_700_002_920);
    assert_eq!(event_minute_buckets.value(2), 1_700_002_980);
}

#[test]
fn public_workflow_preparation_keeps_layout_profile_fields() {
    let raw_fields = vec![
        (
            "vortex_layout_write_advisor_source_scale".to_string(),
            "large_olap".to_string(),
        ),
        (
            "vortex_layout_write_advisor_profile_family".to_string(),
            "url_time_counter_olap".to_string(),
        ),
        (
            "vortex_layout_write_advisor_prepared_layout_family".to_string(),
            "url_time_counter_dictionary_stats_layout".to_string(),
        ),
        (
            "vortex_layout_write_advisor_text_domain_columns".to_string(),
            "true".to_string(),
        ),
        (
            "vortex_layout_write_advisor_time_bucket_columns".to_string(),
            "true".to_string(),
        ),
        (
            "vortex_layout_write_advisor_counter_columns".to_string(),
            "true".to_string(),
        ),
        (
            "vortex_layout_write_advisor_key_profile".to_string(),
            "high_cardinality_numeric_text_time_keys".to_string(),
        ),
        (
            "vortex_layout_write_advisor_dictionary_profile".to_string(),
            "source_dictionary_or_derived_dictionary_evidence".to_string(),
        ),
        (
            "vortex_layout_write_advisor_expected_read_tradeoff".to_string(),
            "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution"
                .to_string(),
        ),
        (
            "vortex_layout_write_advisor_expected_write_tradeoff".to_string(),
            "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
                .to_string(),
        ),
        (
            "vortex_writer_compression_field_count".to_string(),
            "3".to_string(),
        ),
        (
            "vortex_writer_compression_field_names".to_string(),
            "URL,Referer,SearchPhrase".to_string(),
        ),
        (
            "vortex_writer_compression_decision_count".to_string(),
            "3".to_string(),
        ),
        (
            "vortex_writer_compression_decisions".to_string(),
            "field=URL;decision=compress;codec=zstd|field=Referer;decision=compress;codec=zstd|field=SearchPhrase;decision=compress;codec=zstd".to_string(),
        ),
    ];
    let fields = field_map(public_workflow_preparation_fields(&raw_fields));

    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_source_scale",
        "large_olap",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_profile_family",
        "url_time_counter_olap",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_prepared_layout_family",
        "url_time_counter_dictionary_stats_layout",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_text_domain_columns",
        "true",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_time_bucket_columns",
        "true",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_counter_columns",
        "true",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_key_profile",
        "high_cardinality_numeric_text_time_keys",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_dictionary_profile",
        "source_dictionary_or_derived_dictionary_evidence",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_expected_read_tradeoff",
        "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_layout_write_advisor_expected_write_tradeoff",
        "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_writer_compression_field_count",
        "3",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_writer_compression_field_names",
        "URL,Referer,SearchPhrase",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_writer_compression_decision_count",
        "3",
    );
    assert_field_eq(
        &fields,
        "public_workflow_preparation_vortex_writer_compression_decisions",
        "field=URL;decision=compress;codec=zstd|field=Referer;decision=compress;codec=zstd|field=SearchPhrase;decision=compress;codec=zstd",
    );
}

#[test]
fn layout_writer_provider_preserves_unknown_streaming_row_count_as_vortex_provider() {
    let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("hits.orc"))
        .expect("infer ORC adapter");
    let source = VortexIngestSourceData {
        source_format: source_adapter.source_format,
        source_adapter,
        header: vec!["URL".to_string()],
        column_arrow_dtypes: vec![Some(DataType::Utf8)],
        #[cfg(feature = "universal-format-io")]
        first_batch_profile: None,
        read_plan: LocalSourceReadPlan::full("test_unknown_streaming_columnar_source"),
        materialized_columns: vec!["URL".to_string()],
        reader_projection_columns: vec!["URL".to_string()],
        projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
        source_bytes: 1024,
        source_digest: "fnv64:test".to_string(),
        source_fingerprint: SourceFingerprintEvidence::metadata_only(),
        row_count: 0,
        row_count_known: false,
        source_split_row_ranges: vec![(0, 0)],
        source_metadata_scout_millis: 0,
        source_byte_acquisition_millis: 0,
        source_full_body_millis: 0,
        read_millis: 0,
        compatibility_parse_millis: 0,
        source_to_columnar_millis: 0,
        record_batch_count: 0,
        source_stream_batch_size: 262_144,
        source_stream_unit_count_hint: None,
        source_stream_unit_hint_kind: "orc_stream_record_batches_unknown_before_read".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status:
            "orc_arrow_reader_typed_batches_preserved_dictionary_contract_not_declared".to_string(),
        ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
        ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
        ingest_executor_requested_parallelism: 2,
        ingest_executor_applied_parallelism: 2,
        ingest_executor_unit_count_hint: None,
        materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
        parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
        columnar_source_preserved: true,
    };

    assert_eq!(layout_writer_provider_kind(&source), "vortex_array_kernel");
    assert_eq!(
        layout_writer_provider_surface(&source),
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn partitioned_columnar_stream_reader_enforces_global_row_budget() {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front().map(Ok)
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let first_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("first batch");
    let second_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 4]))],
    )
    .expect("second batch");
    let mut readers: VecDeque<Box<dyn RecordBatchReader + Send>> = VecDeque::new();
    readers.push_back(Box::new(TestRecordBatchReader {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([first_batch]),
    }));
    readers.push_back(Box::new(TestRecordBatchReader {
        schema: Arc::clone(&schema),
        batches: VecDeque::from([second_batch]),
    }));
    let mut reader = PartitionedColumnarStreamReader {
        schema,
        readers,
        max_rows: 3,
        row_count: 0,
        source_label: "Parquet",
        path: "target/parts".to_string(),
        failed: false,
        cancellation: CancellationToken::default(),
    };

    let first = reader
        .next()
        .expect("first partition batch")
        .expect("batch");
    assert_eq!(first.num_rows(), 2);
    let error = reader
        .next()
        .expect("global row budget error")
        .expect_err("second partition exceeds aggregate budget");
    assert!(
        error
            .to_string()
            .contains("exceeds the configured local source row budget of 3 across partition files")
    );
    assert!(reader.next().is_none());
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn vortex_ingest_reuse_test_root(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let counter =
        SQL_LOCAL_SOURCE_TEST_PATH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!(
        "shardloom-vortex-ingest-reuse-{name}-{}-{nanos}-{counter}",
        std::process::id()
    ));
    fs::create_dir(&path).expect("create unique vortex ingest reuse root");
    path
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn vortex_ingest_reuse_request(
    source_path: PathBuf,
    target_path: PathBuf,
    allow_overwrite: bool,
) -> VortexIngestRequest {
    VortexIngestRequest {
        source_path,
        source_format_override: None,
        target_path,
        allow_overwrite,
        certification_level: shardloom_vortex::VortexIngestCertificationLevel::IngestCertified,
        runtime_profile: SqlLocalSourceRuntimeProfile::Smoke,
        resources: shardloom_core::ExecutionResources::from_gib(
            4,
            2,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
        shared_memory_pool: None,
        cancellation: CancellationToken::default(),
        float_policy: shardloom_core::FloatPolicy::Strict,
        source_fingerprint_policy: SourceFingerprintPolicy::DEFAULT_PUBLIC_PREPARE,
        delta: None,
        prepared_source_binding: None,
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn prepared_vortex_ingest_report(outcome: VortexIngestOutcome) -> Box<VortexIngestReport> {
    let VortexIngestOutcome::Prepared(report) = outcome;
    report
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_ingest_public_prepare_writes_only_single_vortex_artifact() {
    let root = vortex_ingest_reuse_test_root("single-artifact");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    fs::write(&source, "id,label,amount\n1,alpha,10\n2,beta,20\n").expect("write reuse source");

    let first = run_vortex_prepare(vortex_ingest_reuse_request(
        source.clone(),
        target.clone(),
        false,
    ))
    .expect("first vortex_ingest run writes artifact");
    let first_report = prepared_vortex_ingest_report(first);
    let first_fields = field_map(first_report.fields());
    assert_field_eq(&first_fields, "vortex_ingest_performed", "true");
    assert_field_eq(
        &first_fields,
        "vortex_writer_layout_block_target_bytes",
        "1048576",
    );
    assert_field_eq(&first_fields, "prepared_state_reuse_hit", "false");
    assert_field_eq(
        &first_fields,
        "prepared_state_invalidation_reason",
        "not_applicable_single_vortex_artifact",
    );
    assert_field_eq(
        &first_fields,
        "prepared_state_reuse_scope",
        "single_vortex_artifact_no_sidecar",
    );
    assert_field_eq(
        &first_fields,
        "prepared_state_reuse_manifest_path",
        "not_applicable_single_vortex_artifact",
    );
    assert_field_eq(
        &first_fields,
        "prepared_olap_state_status",
        "prepared_olap_state_ready",
    );
    assert_field_eq(
        &first_fields,
        "prepared_olap_state_query_time_contract",
        "single_vortex_artifact_native_runtime_no_query_answer_sidecar",
    );
    assert_field_eq(&first_fields, "prepared_olap_state_blocker_id", "none");
    assert!(
        first_fields
            .get("prepared_olap_state_embedded_layout_statistics_contract")
            .is_some_and(|value| value
                .contains("metadata_first_when_vortex_layout_statistics_are_available"))
    );
    assert!(
        first_fields
            .get("prepared_state_reuse_manifest_digest")
            .is_some_and(|value| value == "not_applicable_single_vortex_artifact")
    );
    let manifest_path = shardloom_vortex::vortex_prepared_state_reuse_manifest_path(&target)
        .expect("manifest path");
    assert!(
        !manifest_path.exists(),
        "public Vortex prepare must not create artifact-adjacent prepared-state reuse manifest"
    );
    assert!(
        !target
            .parent()
            .expect("target parent")
            .join(".shardloom")
            .join("prepared.vortex.prepared-olap-state.manifest")
            .exists(),
        "public Vortex prepare must not create prepared OLAP sidecar manifests"
    );
    let artifact_digest = first_fields
        .get("vortex_artifact_digest")
        .expect("artifact digest")
        .clone();

    let second = run_vortex_prepare(vortex_ingest_reuse_request(
        source.clone(),
        target.clone(),
        true,
    ))
    .expect("second vortex_ingest run rewrites artifact when overwrite is explicit");
    let second_report = prepared_vortex_ingest_report(second);
    let second_fields = field_map(second_report.fields());
    assert_field_eq(&second_fields, "vortex_ingest_performed", "true");
    assert_field_eq(
        &second_fields,
        "vortex_ingest_status",
        "prepared_state_created",
    );
    assert_field_eq(&second_fields, "prepared_state_created", "true");
    assert_field_eq(&second_fields, "prepared_state_reused", "false");
    assert_field_eq(&second_fields, "prepared_state_reuse_hit", "false");
    assert_field_eq(
        &second_fields,
        "prepared_state_reuse_scope",
        "single_vortex_artifact_no_sidecar",
    );
    assert_field_eq(
        &second_fields,
        "prepared_olap_state_status",
        "prepared_olap_state_ready",
    );
    assert_field_eq(
        &second_fields,
        "prepared_olap_state_query_time_contract",
        "single_vortex_artifact_native_runtime_no_query_answer_sidecar",
    );
    assert_field_eq(&second_fields, "prepared_olap_state_blocker_id", "none");
    assert_field_eq(&second_fields, "fallback_attempted", "false");
    assert_field_eq(&second_fields, "external_engine_invoked", "false");
    assert_eq!(
        second_fields.get("vortex_artifact_digest"),
        Some(&artifact_digest)
    );
    assert!(
        !manifest_path.exists(),
        "public Vortex overwrite must still leave no prepared-state reuse manifest"
    );

    fs::remove_dir_all(root).expect("remove reuse hit root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_text_sources_use_typed_record_batch_stream() {
    let root = vortex_ingest_reuse_test_root("text-record-batch-stream");
    let cases = [
        (
            "csv",
            "input.csv",
            "id,label,amount\n1,alpha,10\n2,beta,20\n",
            "incremental_csv_schema_discovery_replay",
        ),
        (
            "jsonl",
            "input.jsonl",
            "{\"id\":1,\"label\":\"alpha\",\"amount\":10}\n{\"id\":2,\"label\":\"beta\",\"amount\":20}\n",
            "incremental_jsonl_schema_discovery_replay",
        ),
    ];

    for (label, file_name, body, stream_policy) in cases {
        let source = root.join(file_name);
        let target = root.join(format!("{label}.vortex"));
        fs::write(&source, body).expect("write text source");

        let outcome = run_vortex_prepare(vortex_ingest_reuse_request(source, target, false))
            .expect("text source writes Vortex artifact through Universal Ingest");
        let report = prepared_vortex_ingest_report(outcome);
        let fields = field_map(report.fields());

        assert_field_eq(&fields, "vortex_ingest_performed", "true");
        assert_field_eq(
            &fields,
            "source_state_materialization_layout",
            "incremental_text_to_streaming_arrow_record_batch_source_state",
        );
        assert_field_eq(
            &fields,
            "source_state_parse_normalization",
            "text_incremental_records_with_explicit_schema_replay",
        );
        assert_field_eq(&fields, "source_state_columnar_preserved", "true");
        assert_field_eq(&fields, "source_state_record_batch_count", "1");
        assert_text_discovery_policy(&fields, stream_policy);
        assert_field_eq(
            &fields,
            "source_state_stream_unit_hint_kind",
            "width_bounded_text_record_batches",
        );
        assert_field_eq(
            &fields,
            "source_state_dictionary_preservation_status",
            "text_typed_builders_no_source_dictionary",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_status",
            "bounded_shared_runtime_source",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_kind",
            "ordered_source_tasks_on_shared_native_ingest_runtime",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_requested_parallelism",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_applied_parallelism",
            "1",
        );
        assert_field_eq(&fields, "source_state_ingest_executor_unit_count_hint", "1");
        assert_ingest_stream_lane_recipe(&fields, 2);
        assert_field_eq(
            &fields,
            "vortex_array_build_provider_surface",
            "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
        );
        assert_field_eq(
            &fields,
            "vortex_array_build_strategy",
            "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
        );
        assert_field_eq(&fields, "vortex_array_build_prefetch_window", "2");
        assert_field_eq(
            &fields,
            "vortex_array_build_input_layout",
            "streaming_arrow_record_batch_columnar_source_state",
        );
        assert_field_eq(&fields, "vortex_array_build_record_batch_count", "1");
        assert_field_eq(
            &fields,
            "vortex_writer_coalescing_policy_status",
            "native_within_source_batch_only;cross_batch_coalescing_disabled",
        );
        assert_field_eq(
            &fields,
            "universal_ingest_timing_split_schema_version",
            "shardloom.universal_ingest_timing_split.v1",
        );
        assert_field_eq(
            &fields,
            "universal_ingest_timing_split_status",
            "streaming_source_pull_decode_derive_and_arrow_to_vortex_convert_timing_recorded",
        );
        assert_field_eq(
            &fields,
            "universal_ingest_stream_timing_overlap_policy",
            "capillary_prefetch_may_overlap_decode_derive_with_encode_write_wall_time",
        );
        for field in [
            "universal_ingest_source_read_millis",
            "universal_ingest_decode_derive_millis",
            "universal_ingest_decode_millis",
            "universal_ingest_derived_metadata_build_millis",
            "universal_ingest_arrow_to_vortex_convert_millis",
            "universal_ingest_encode_write_wall_millis",
            "universal_ingest_footer_register_millis",
            "universal_ingest_reopen_verify_millis",
            "universal_ingest_prepare_source_hydration_millis",
            "universal_ingest_prepare_nested_source_batch_production_millis",
            "universal_ingest_prepare_route_bookkeeping_millis",
            "universal_ingest_prepare_scheduler_wait_millis",
            "universal_ingest_prepare_writer_backpressure_or_timer_overlap_millis",
            "universal_ingest_prepare_evidence_emit_millis",
            "vortex_writer_runtime_requested_parallelism",
            "vortex_writer_runtime_applied_parallelism",
            "vortex_writer_runtime_background_workers",
            "vortex_compression_millis",
            "vortex_encode_write_millis",
            "vortex_final_commit_millis",
        ] {
            fields
                .get(field)
                .unwrap_or_else(|| panic!("{field} should be emitted"))
                .parse::<u128>()
                .unwrap_or_else(|_| panic!("{field} should be numeric"));
        }
        assert_field_eq(
            &fields,
            "vortex_array_build_manual_scalar_copy_avoided",
            "true",
        );
        assert_field_eq(
            &fields,
            "vortex_preparation_spine_decode_boundary_status",
            "no_scalar_row_decode_for_streamed_batches",
        );
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
    }

    fs::remove_dir_all(root).expect("remove text stream root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_max_parallelism_propagates_to_public_prepare_evidence() {
    let root = vortex_ingest_reuse_test_root("max-parallelism-propagation");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    fs::write(&source, "id,label\n1,alpha\n2,beta\n").expect("write csv source");

    let mut request = vortex_ingest_reuse_request(source, target, false);
    request.resources = shardloom_core::ExecutionResources::from_gib(
        6,
        4,
        shardloom_core::ExecutionResourceOrigin::ExecutionCall,
    )
    .expect("explicit fixture allocation");

    let outcome = run_vortex_prepare(request)
        .expect("csv source writes Vortex artifact with explicit max parallelism");
    let report = prepared_vortex_ingest_report(outcome);
    let raw_fields = report.fields();
    let fields = field_map(raw_fields.clone());
    let public_fields = field_map(public_workflow_preparation_fields(&raw_fields));

    assert_field_eq(&fields, "vortex_ingest_requested_memory_gb", "6");
    assert_field_eq(&fields, "vortex_ingest_requested_max_parallelism", "4");
    assert_field_eq(
        &fields,
        "vortex_layout_write_advisor_writer_parallelism_budget",
        "4",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_status",
        "bounded_shared_runtime_source",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_requested_parallelism",
        "4",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_applied_parallelism",
        "1",
    );
    assert_field_eq(&fields, "vortex_array_build_prefetch_window", "4");
    assert_ingest_stream_lane_recipe(&fields, 4);
    assert_field_eq(
        &fields,
        "vortex_array_build_strategy",
        "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
    );
    assert_field_eq(&fields, "vortex_writer_physical_design_status", "applied");
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_provider_decision",
        "use_vortex_native_provider",
    );
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_source_executor_requested_parallelism",
        "4",
    );
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_source_executor_applied_parallelism",
        "1",
    );
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_array_build_prefetch_window",
        "4",
    );
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_array_build_worker_count",
        "4",
    );
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_writer_backpressure_policy",
        "reserved_inflight_window_bounds_queued_active_and_ordered_results",
    );
    assert_field_eq(
        &fields,
        "vortex_writer_physical_design_no_fallback_policy",
        "native_vortex_writer_provider_only_no_spark_datafusion_duckdb_polars_velox_fallback",
    );
    assert_field_eq(
        &fields,
        "vortex_segment_metadata_status",
        "admitted_footer_segment_metadata",
    );
    assert_field_eq(&fields, "vortex_segment_metadata_row_count", "2");
    assert_field_eq(&fields, "vortex_segment_metadata_row_count_proven", "true");
    assert_field_eq(
        &fields,
        "vortex_segment_metadata_metadata_count_admission",
        "admitted_metadata_count_from_prepared_footer_or_writer_summary",
    );
    assert_field_eq(
        &fields,
        "vortex_segment_metadata_predicate_pruning_admission",
        "admitted_conservative_metadata_pruning_for_predicate_ranges_and_domain_absence",
    );
    assert_field_eq(
        &fields,
        "vortex_segment_metadata_no_false_negative_policy",
        "unknown_or_inconclusive_segments_are_read_no_false_negative_pruning",
    );
    assert_field_eq(
        &fields,
        "vortex_segment_metadata_query_answer_sidecar_status",
        "disabled_rejected_for_public_default_runtime",
    );
    assert_field_eq(&fields, "vortex_capillary_preparation_max_parallelism", "4");
    assert_field_eq(
        &fields,
        "vortex_capillary_preparation_memory_budget_bytes",
        &(6_u64 * 1024 * 1024 * 1024).to_string(),
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_ingest_requested_memory_gb",
        "6",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_layout_write_advisor_writer_parallelism_budget",
        "4",
    );
    assert!(
        public_fields
            .contains_key("public_workflow_preparation_universal_ingest_timing_split_status"),
        "public workflow preparation projection should expose ingest timing split status"
    );
    assert!(
        public_fields.contains_key(
            "public_workflow_preparation_universal_ingest_prepare_known_component_millis"
        ),
        "public workflow preparation projection should expose known prepare attribution"
    );
    assert!(
        public_fields.contains_key(
            "public_workflow_preparation_universal_ingest_prepare_unattributed_millis"
        ),
        "public workflow preparation projection should expose residual prepare attribution"
    );
    assert!(
        public_fields.contains_key(
            "public_workflow_preparation_universal_ingest_prepare_attribution_status"
        ),
        "public workflow preparation projection should expose prepare attribution status"
    );
    assert!(
        public_fields.contains_key(
            "public_workflow_preparation_universal_ingest_prepare_source_hydration_millis"
        ),
        "public workflow preparation projection should expose source hydration attribution"
    );
    assert!(
        public_fields.contains_key(
            "public_workflow_preparation_universal_ingest_prepare_nested_source_batch_production_millis"
        ),
        "public workflow preparation projection should expose nested source batch production timing"
    );
    assert!(
        public_fields.contains_key(
            "public_workflow_preparation_universal_ingest_prepare_writer_backpressure_or_timer_overlap_millis"
        ),
        "public workflow preparation projection should expose residual writer/backpressure attribution"
    );
    let prepare_total_millis = fields
        .get("prepare_once_millis")
        .expect("prepare total field")
        .parse::<u128>()
        .expect("prepare total millis");
    let known_prepare_millis = fields
        .get("universal_ingest_prepare_known_component_millis")
        .expect("known prepare attribution")
        .parse::<u128>()
        .expect("known prepare attribution millis");
    let unattributed_prepare_millis = fields
        .get("universal_ingest_prepare_unattributed_millis")
        .expect("unattributed prepare attribution")
        .parse::<u128>()
        .expect("unattributed prepare attribution millis");
    assert!(known_prepare_millis <= prepare_total_millis);
    assert!(unattributed_prepare_millis <= prepare_total_millis);
    assert_eq!(
        known_prepare_millis.saturating_add(unattributed_prepare_millis),
        prepare_total_millis
    );
    assert!(
        public_fields.contains_key("public_workflow_preparation_vortex_writer_stats_concurrency"),
        "public workflow preparation projection should expose writer stats concurrency"
    );
    assert!(
        public_fields
            .contains_key("public_workflow_preparation_vortex_writer_compression_decision_count"),
        "public workflow preparation projection should expose writer compression decisions"
    );
    assert!(
        public_fields
            .contains_key("public_workflow_preparation_vortex_writer_compression_decisions"),
        "public workflow preparation projection should expose writer compression decision details"
    );
    assert!(
        public_fields
            .contains_key("public_workflow_preparation_vortex_writer_profile_selection_reason"),
        "public workflow preparation projection should expose writer profile reason"
    );
    assert!(
        public_fields
            .contains_key("public_workflow_preparation_vortex_writer_profile_regression_guard"),
        "public workflow preparation projection should expose writer profile regression guard"
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_array_build_prefetch_window",
        "4",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_array_build_strategy",
        "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_writer_physical_design_status",
        "applied",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_writer_physical_design_array_build_prefetch_window",
        "4",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_writer_physical_design_array_build_worker_count",
        "4",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_writer_physical_design_no_fallback_policy",
        "native_vortex_writer_provider_only_no_spark_datafusion_duckdb_polars_velox_fallback",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_segment_metadata_status",
        "admitted_footer_segment_metadata",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_segment_metadata_metadata_count_admission",
        "admitted_metadata_count_from_prepared_footer_or_writer_summary",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_segment_metadata_predicate_pruning_admission",
        "admitted_conservative_metadata_pruning_for_predicate_ranges_and_domain_absence",
    );
    assert_field_eq(
        &public_fields,
        "public_workflow_preparation_vortex_segment_metadata_no_false_negative_policy",
        "unknown_or_inconclusive_segments_are_read_no_false_negative_pruning",
    );
    assert_field_eq(&fields, "fallback_attempted", "false");
    assert_field_eq(&fields, "external_engine_invoked", "false");

    fs::remove_dir_all(root).expect("remove max parallelism root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_parquet_public_prepare_uses_row_group_capillary_executor() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let root = vortex_ingest_reuse_test_root("parquet-row-group-public-prepare");
    let source = root.join("input.parquet");
    let target = root.join("prepared.vortex");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("URL", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("https://example.com/a"),
                Some("https://example.com/b"),
                Some("https://example.net/c"),
                None,
            ])),
        ],
    )
    .expect("parquet batch");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let file = fs::File::create(&source).expect("create parquet source");
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    let request = vortex_ingest_reuse_request(source, target, false);
    let outcome = run_vortex_prepare(request)
        .expect("parquet source writes Vortex artifact through public prepare");
    let report = prepared_vortex_ingest_report(outcome);
    let fields = field_map(report.fields());

    assert_field_eq(&fields, "vortex_ingest_requested_max_parallelism", "2");
    assert_field_eq(
        &fields,
        "vortex_layout_write_advisor_writer_parallelism_budget",
        "2",
    );
    assert_field_eq(
        &fields,
        "source_state_stream_unit_hint_kind",
        "parquet_adaptive_row_group_task_count",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_status",
        "bounded_shared_runtime_source",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_kind",
        "parquet_ordered_source_tasks_on_shared_native_ingest_runtime",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_requested_parallelism",
        "2",
    );
    assert_field_eq(
        &fields,
        "source_state_ingest_executor_applied_parallelism",
        "1",
    );
    assert_field_eq(
        &fields,
        "vortex_array_build_strategy",
        "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
    );
    assert_ingest_stream_lane_recipe(&fields, 2);
    assert_field_eq(&fields, "vortex_writer_stats_concurrency", "1");
    assert_field_eq(&fields, "fallback_attempted", "false");
    assert_field_eq(&fields, "external_engine_invoked", "false");

    fs::remove_dir_all(root).expect("remove parquet public prepare root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_inferred_text_sources_stream_without_scalar_row_bridge() {
    let root = vortex_ingest_reuse_test_root("inferred-text-stream");
    let cases = [
        (
            "csv",
            "input.csv",
            "id,label,amount\n1,alpha,10\n2,beta,20\n",
            "incremental_csv_schema_discovery_replay",
        ),
        (
            "jsonl",
            "input.jsonl",
            "{\"id\":1,\"label\":\"alpha\",\"amount\":10}\n{\"id\":2,\"label\":99,\"amount\":20.5}\n",
            "incremental_jsonl_schema_discovery_replay",
        ),
    ];

    for (label, file_name, body, stream_policy) in cases {
        let source = root.join(file_name);
        let target = root.join(format!("{label}.vortex"));
        fs::write(&source, body).expect("write inferred text source");

        let outcome = run_vortex_prepare(vortex_ingest_reuse_request(source, target, false))
            .expect("inferred text source writes Vortex artifact through direct stream");
        let report = prepared_vortex_ingest_report(outcome);
        let fields = field_map(report.fields());

        assert_eq!(report.vortex_report.row_count, 2);
        assert_field_eq(&fields, "vortex_ingest_performed", "true");
        assert_field_eq(
            &fields,
            "source_state_materialization_layout",
            "incremental_text_to_streaming_arrow_record_batch_source_state",
        );
        assert_field_eq(
            &fields,
            "source_state_parse_normalization",
            "text_incremental_records_with_explicit_schema_replay",
        );
        assert_field_eq(&fields, "source_state_columnar_preserved", "true");
        assert_text_discovery_policy(&fields, stream_policy);
        assert_field_eq(
            &fields,
            "source_state_dictionary_preservation_status",
            "text_typed_builders_no_source_dictionary",
        );
        assert_field_eq(&fields, "source_state_record_batch_count", "1");
        assert_field_eq(&fields, "compatibility_parse_millis", "0");
        assert_field_eq(
            &fields,
            "source_read_buffer_carry_status",
            "incremental_schema_discovery_then_generation_checked_record_replay",
        );
        assert_field_eq(
            &fields,
            "source_fingerprint_kind",
            "local_file_metadata_size_mtime",
        );
        assert_field_eq(&fields, "source_fingerprint_policy", "metadata_only");
        assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
        assert_ingest_stream_lane_recipe(&fields, 2);
        assert_field_eq(
            &fields,
            "vortex_array_build_provider_surface",
            "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
        );
        assert_field_eq(&fields, "vortex_array_build_prefetch_window", "2");
        assert_field_eq(
            &fields,
            "vortex_array_build_strategy",
            "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
        );
        assert_field_eq(
            &fields,
            "vortex_preparation_spine_decode_boundary_status",
            "no_scalar_row_decode_for_streamed_batches",
        );
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
    }

    fs::remove_dir_all(root).expect("remove inferred text stream root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn direct_jsonl_test_reader(
    body: &str,
    columns: &[(&str, LogicalDType)],
    batch_size: usize,
    max_input_rows: Option<usize>,
) -> (PathBuf, SchemaDeclaredTextRecordBatchReader) {
    let root = vortex_ingest_reuse_test_root("direct-jsonl");
    let path = root.join("input.jsonl");
    fs::write(&path, body).unwrap();
    let header = columns
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, dtype)| {
                Field::new(
                    *name,
                    schema_declared_text_arrow_dtype(dtype, name, "test").unwrap(),
                    true,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let reader = SchemaDeclaredTextRecordBatchReader::new(
        LocalSourceFormat::JsonLines,
        schema,
        header,
        columns
            .iter()
            .map(|(_, dtype)| Some(dtype.clone()))
            .collect(),
        BufReader::new(fs::File::open(path).unwrap()),
        TextRecordBatchReaderConfig {
            max_input_rows,
            batch_size,
            cancellation: CancellationToken::default(),
            memory: shardloom_exec::live_memory::LiveMemoryPool::new(1 << 20).unwrap(),
        },
        "direct JSONL test".into(),
    )
    .unwrap();
    (root, reader)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn direct_jsonl_builder_preserves_duplicate_coercion_missing_and_batch_values() {
    let columns = [
        ("id", LogicalDType::Int64),
        ("unsigned", LogicalDType::UInt64),
        ("ratio", LogicalDType::Float64),
        ("truth", LogicalDType::Boolean),
        ("label", LogicalDType::Utf8),
        ("bin", LogicalDType::Binary),
        ("date", LogicalDType::Date32),
        ("time", LogicalDType::TimestampMicros),
        ("nested", LogicalDType::Utf8),
    ];
    let body = concat!(
        "\n\u{feff}",
        r#"{"id":"discard","id":9223372036854775807,"unsigned":12,"ratio":3,"truth":true,"label":"é\n\uD834\uDD1E","bin":"ab","date":"1970-01-02","time":"1970-01-01T00:00:01Z","nested":{"b":[true,null],"a":1},"ignored":[{"a":"escaped\""}]}"#,
        "\n\n",
        r#"{"ignored":null}"#,
        "\n",
        r#"{"label":false,"id":-9223372036854775808,}"#,
        "\n"
    );
    let (root, mut reader) = direct_jsonl_test_reader(body, &columns, 2, None);
    let expected = [
        vec![
            ScalarValue::Int64(i64::MAX),
            ScalarValue::UInt64(12),
            ScalarValue::Float64(3.0),
            ScalarValue::Boolean(true),
            ScalarValue::Utf8("é\n𝄞".into()),
            ScalarValue::Binary(b"ab".to_vec()),
            ScalarValue::Date32(1),
            ScalarValue::TimestampMicros(1_000_000),
            ScalarValue::Utf8(r#"{"a":1,"b":[true,null]}"#.into()),
        ],
        vec![ScalarValue::Null; columns.len()],
        vec![
            ScalarValue::Int64(i64::MIN),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Utf8("false".into()),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
        ],
    ];
    for rows in expected.chunks(2) {
        let named_rows = rows
            .iter()
            .map(|row| {
                reader
                    .header
                    .iter()
                    .cloned()
                    .zip(row.iter().cloned())
                    .collect()
            })
            .collect::<Vec<_>>();
        let expected_batch =
            shardloom_vortex::universal_format_io::flat_rows_to_record_batch_with_schema(
                Arc::clone(&reader.schema),
                &reader.header,
                &named_rows,
                "expected",
            )
            .unwrap();
        assert_eq!(reader.next_record_batch().unwrap().unwrap(), expected_batch);
    }
    assert!(reader.next_record_batch().unwrap().is_none());
    assert_eq!(reader.next_row_number, 3);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn direct_jsonl_temporal_schema_preserves_exact_ticks_and_zone_metadata() {
    use shardloom_core::temporal::TemporalDType;
    let declarations = [
        "timestamp(ns,America/New_York)",
        "time(ns)",
        "duration(ns)",
        "date64",
    ];
    let names = ["instant", "clock", "elapsed", "day"];
    let columns = names
        .iter()
        .zip(declarations)
        .map(|(name, dtype)| (*name, TemporalDType::parse(dtype).unwrap().logical()))
        .collect::<Vec<_>>();
    let (root, mut reader) = direct_jsonl_test_reader(
        concat!(
            r#"{"instant":9223372036854775807,"clock":"23:59:59.999999999","elapsed":"-17ns","day":"1969-12-31"}"#,
            "\n",
            r#"{"instant":null,"clock":null,"elapsed":null,"day":null}"#,
            "\n"
        ),
        &columns,
        2,
        None,
    );
    let batch = reader.next().unwrap().unwrap();
    for (index, (declaration, expected)) in declarations
        .into_iter()
        .zip([i64::MAX, 86_399_999_999_999, -17, -86_400_000])
        .enumerate()
    {
        let column = batch.column(index);
        assert_eq!(
            shardloom_vortex::temporal_arrow::dtype(column.data_type())
                .unwrap()
                .as_str(),
            declaration
        );
        assert_eq!(
            shardloom_vortex::temporal_arrow::value(column.as_ref(), 0).unwrap(),
            Some(expected)
        );
        assert_eq!(
            shardloom_vortex::temporal_arrow::value(column.as_ref(), 1).unwrap(),
            None
        );
    }
    assert!(reader.next().is_none());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn temporal_text_schema_admission_and_csv_json_semantics_agree() {
    use shardloom_core::temporal::TemporalDType;
    use shardloom_exec::live_memory::LiveMemoryPool;
    let declarations = parse_vortex_ingest_schema_hints(
        "instant:TIMESTAMP( NS, 'America/New_York' ), elapsed:duration(ns), day:date64",
    )
    .unwrap();
    assert_eq!(declarations.len(), 3);
    assert_eq!(declarations[0].1.as_str(), "timestamp(ns,America/New_York)");
    assert!(parse_vortex_ingest_schema_hints("a:timestamp(ns,UTC").is_err());
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let dtype = &declarations[0].1;
    assert!(BoundTextTemporalColumn::bind(dtype, &LiveMemoryPool::new(1).unwrap()).is_err());
    let column = BoundTextTemporalColumn::bind(dtype, &memory)
        .unwrap()
        .unwrap();
    assert!(memory.snapshot().reserved_bytes > 0);
    let raw = "1970-01-01T00:00:00";
    let csv = parse_schema_declared_text_scalar(raw, dtype, "instant", "test", Some(&column.codec))
        .unwrap();
    let json = coerce_schema_declared_json_scalar(
        ScalarValue::Utf8(raw.into()),
        dtype,
        "instant",
        "test",
        Some(&column.codec),
    )
    .unwrap();
    assert_eq!(csv, json);
    assert_eq!(
        csv,
        shardloom_core::temporal::TemporalValue::new(
            18_000_000_000_000,
            TemporalDType::parse("timestamp(ns,America/New_York)").unwrap()
        )
        .unwrap()
        .into_scalar()
    );
    for raw in ["2024-03-10T02:30:00", "2024-11-03T01:30:00"] {
        assert!(
            parse_schema_declared_text_scalar(raw, dtype, "instant", "test", Some(&column.codec))
                .is_err()
        );
    }
    drop(column);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(
        BoundTextTemporalColumn::bind(
            &TemporalDType::parse("timestamp(ns,No/Such_Zone)")
                .unwrap()
                .logical(),
            &memory
        )
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn direct_jsonl_builder_rejects_invalid_rows_before_emitting_a_batch() {
    let cases = [
        (r#"{"v":-1}"#, LogicalDType::UInt64),
        (r#"{"v":18446744073709551616}"#, LogicalDType::UInt64),
        (r#"{"v":1.5}"#, LogicalDType::Int64),
        (r#"{"v":1}"#, LogicalDType::Boolean),
        (r#"{"v":true}"#, LogicalDType::Float64),
        (r#"{"v":1}"#, LogicalDType::Binary),
        (r#"{"v":"not-date"}"#, LogicalDType::Date32),
        (r#"{"v":"not-time"}"#, LogicalDType::TimestampMicros),
        (
            r#"{"v":1}"#,
            shardloom_core::temporal::TemporalDType::parse("date64")
                .unwrap()
                .logical(),
        ),
        (
            r#"{"v":86400}"#,
            shardloom_core::temporal::TemporalDType::parse("time(s)")
                .unwrap()
                .logical(),
        ),
        (
            r#"{"v":"00:00:00.000000001"}"#,
            shardloom_core::temporal::TemporalDType::parse("time(us)")
                .unwrap()
                .logical(),
        ),
        (r#"{"v":"\x","v":"okay"}"#, LogicalDType::Utf8),
        (r#"{"v":1} trailing"#, LogicalDType::Int64),
        (r#"{"ignored":[1,}"#, LogicalDType::Int64),
        (r#"{"ignored":"\x"}"#, LogicalDType::Int64),
        ("{}", LogicalDType::Int64),
    ];
    for (invalid, dtype) in cases {
        let (root, mut reader) = direct_jsonl_test_reader(
            &format!("{{\"v\":null}}\n{invalid}\n"),
            &[("v", dtype)],
            3,
            None,
        );
        assert!(reader.next_record_batch().is_err(), "{invalid}");
        fs::remove_dir_all(root).unwrap();
    }
    let (root, mut reader) = direct_jsonl_test_reader(
        "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n",
        &[("v", LogicalDType::Int64)],
        3,
        Some(2),
    );
    assert!(reader.next_record_batch().is_err());
    fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn direct_jsonl_builder_handles_empty_and_exact_batch_boundaries() {
    for row_count in 0..=5 {
        let (root, mut reader) = direct_jsonl_test_reader(
            &"{\"v\":null}\n".repeat(row_count),
            &[("v", LogicalDType::Int64)],
            2,
            None,
        );
        let mut observed = Vec::new();
        while let Some(batch) = reader.next_record_batch().unwrap() {
            assert_eq!(batch.column(0).null_count(), batch.num_rows());
            observed.push(batch.num_rows());
        }
        assert_eq!(observed.iter().sum::<usize>(), row_count);
        assert_eq!(observed.len(), row_count.div_ceil(2));
        assert!(reader.next_record_batch().unwrap().is_none());
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_schema_declared_text_sources_stream_without_scalar_row_bridge() {
    let root = vortex_ingest_reuse_test_root("schema-declared-text-stream");
    let cases = [
        (
            "csv",
            "input.csv",
            "id,label,amount\n1,alpha,10\n2,beta,20\n",
            "incremental_csv_schema_discovery_replay",
        ),
        (
            "jsonl",
            "input.jsonl",
            "{\"id\":1,\"label\":\"alpha\",\"amount\":10}\n{\"id\":2,\"label\":\"beta\",\"amount\":20}\n",
            "incremental_jsonl_schema_discovery_replay",
        ),
    ];
    let hints = vec![
        ("id".to_string(), LogicalDType::Int64),
        ("label".to_string(), LogicalDType::Utf8),
        ("amount".to_string(), LogicalDType::Int64),
    ];

    for (label, file_name, body, stream_policy) in cases {
        let source = root.join(file_name);
        let target = root.join(format!("{label}.vortex"));
        fs::write(&source, body).expect("write text source");

        let outcome = run_vortex_ingest_prepare_once_with_schema(
            vortex_ingest_reuse_request(source, target, false),
            &hints,
        )
        .expect("schema-declared text source writes Vortex artifact through direct stream");
        let report = prepared_vortex_ingest_report(outcome);
        let fields = field_map(report.fields());

        assert_field_eq(&fields, "vortex_ingest_performed", "true");
        assert_field_eq(
            &fields,
            "source_state_materialization_layout",
            "incremental_text_to_streaming_arrow_record_batch_source_state",
        );
        assert_field_eq(
            &fields,
            "source_state_parse_normalization",
            "text_incremental_records_with_explicit_schema_replay",
        );
        assert_field_eq(&fields, "source_state_columnar_preserved", "true");
        assert_text_discovery_policy(&fields, stream_policy);
        assert_field_eq(
            &fields,
            "source_read_buffer_carry_status",
            "incremental_schema_discovery_then_generation_checked_record_replay",
        );
        assert_field_eq(
            &fields,
            "source_fingerprint_kind",
            "local_file_metadata_size_mtime",
        );
        assert_field_eq(&fields, "source_fingerprint_policy", "metadata_only");
        assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
        assert_field_eq(
            &fields,
            "source_state_dictionary_preservation_status",
            "text_typed_builders_no_source_dictionary",
        );
        assert_field_eq(&fields, "source_state_record_batch_count", "1");
        assert_field_eq(&fields, "compatibility_parse_millis", "0");
        assert_ingest_stream_lane_recipe(&fields, 2);
        assert_field_eq(
            &fields,
            "vortex_array_build_provider_surface",
            "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
        );
        assert_field_eq(&fields, "vortex_array_build_prefetch_window", "2");
        assert_field_eq(
            &fields,
            "vortex_array_build_strategy",
            "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
        );
        assert_field_eq(
            &fields,
            "vortex_preparation_spine_decode_boundary_status",
            "no_scalar_row_decode_for_streamed_batches",
        );
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
    }

    fs::remove_dir_all(root).expect("remove schema declared text stream root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn whole_json_typed_prepare_preserves_derived_output_and_failed_overwrite() {
    let root = vortex_ingest_reuse_test_root("whole-json-typed");
    let source = root.join("input.json");
    let target = root.join("prepared.vortex");
    fs::write(&source, r#"[{"id":1,"URL":"https://example.com/a","n":null},{"URL":"https://example.org/b","id":2,"n":null}]"#).unwrap();
    let request = vortex_ingest_reuse_request(source.clone(), target.clone(), false);
    let report = prepared_vortex_ingest_report(run_vortex_prepare(request).unwrap());
    assert_eq!(report.vortex_report.row_count, 2);
    let fields = field_map(report.fields());
    assert_field_eq(
        &fields,
        "source_state_projection_pushdown_status",
        "not_requested_full_read",
    );
    assert_field_eq(
        &fields,
        "source_read_buffer_carry_status",
        "incremental_schema_discovery_then_generation_checked_record_replay",
    );
    assert_field_eq(
        &fields,
        "source_read_mmap_eligibility_status",
        "not_used_columnar_reader_owns_buffer_lifetime",
    );
    assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
    assert_eq!(
        report.source.materialization_layout,
        "incremental_json_to_streaming_arrow_record_batch_source_state"
    );
    assert!(
        report
            .vortex_report
            .column_family_summary()
            .contains("__shardloom_derived_url_domain_URL:")
    );
    let original = fs::read(&target).unwrap();
    for invalid in [
        r#"[{"id":1},{"id":2}] garbage"#,
        r#"[{"id":1},{"id":"changed"}]"#,
    ] {
        fs::write(&source, invalid).unwrap();
        assert!(
            run_vortex_prepare(vortex_ingest_reuse_request(
                source.clone(),
                target.clone(),
                true
            ))
            .is_err()
        );
        assert_eq!(fs::read(&target).unwrap(), original);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
    }
    fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_streaming_text_routes_preserve_hidden_derived_columns() {
    let root = vortex_ingest_reuse_test_root("streaming-derived-columns");
    let inferred_source = root.join("inferred.csv");
    let inferred_target = root.join("inferred.vortex");
    fs::write(
        &inferred_source,
        "id,URL,SearchPhrase\n1,https://www.example.com/a,encoded columnar search query\n2,https://[2001:db8::1]:8443/b,beta\n",
    )
    .expect("write inferred csv source");

    let inferred = run_vortex_prepare(vortex_ingest_reuse_request(
        inferred_source,
        inferred_target,
        false,
    ))
    .expect("inferred text stream writes Vortex artifact");
    let inferred_report = prepared_vortex_ingest_report(inferred);
    let inferred_fields = field_map(inferred_report.fields());
    assert!(
        inferred_report
            .vortex_report
            .column_family_summary()
            .contains("__shardloom_derived_utf8_len_URL:"),
        "{}",
        inferred_report.vortex_report.column_family_summary()
    );
    assert!(
        inferred_report
            .vortex_report
            .column_family_summary()
            .contains("__shardloom_derived_url_domain_URL:"),
        "{}",
        inferred_report.vortex_report.column_family_summary()
    );
    assert_field_contains(
        &inferred_fields,
        "source_state_dictionary_preservation_status",
        "__shardloom_derived_url_domain_URL",
    );

    let schema_source = root.join("schema.csv");
    let schema_target = root.join("schema.vortex");
    fs::write(
        &schema_source,
        "id,URL,SearchPhrase\n1,https://www.example.net/a,encoded columnar search query\n2,https://www.example.org/b,beta\n",
    )
    .expect("write schema csv source");
    let full_hints = vec![
        ("id".to_string(), LogicalDType::Int64),
        ("URL".to_string(), LogicalDType::Utf8),
        ("SearchPhrase".to_string(), LogicalDType::Utf8),
    ];
    let schema_declared = run_vortex_ingest_prepare_once_with_schema(
        vortex_ingest_reuse_request(schema_source, schema_target, false),
        &full_hints,
    )
    .expect("schema-declared text stream writes Vortex artifact");
    let schema_report = prepared_vortex_ingest_report(schema_declared);
    let schema_fields = field_map(schema_report.fields());
    assert!(
        schema_report
            .vortex_report
            .column_family_summary()
            .contains("__shardloom_derived_url_domain_URL:"),
        "{}",
        schema_report.vortex_report.column_family_summary()
    );
    assert_field_contains(
        &schema_fields,
        "source_state_dictionary_preservation_status",
        "__shardloom_derived_utf8_len_SearchPhrase",
    );

    fs::remove_dir_all(root).expect("remove streaming derived columns root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_partial_jsonl_schema_hints_preserve_undeclared_columns() {
    let root = vortex_ingest_reuse_test_root("partial-jsonl-schema-hints");
    let source = root.join("input.jsonl");
    let target = root.join("prepared.vortex");
    fs::write(
        &source,
        "{\"id\":1,\"URL\":\"https://www.example.com/a\",\"amount\":10}\n{\"id\":2,\"URL\":\"https://www.example.net/b\",\"amount\":20}\n",
    )
    .expect("write partial schema jsonl source");
    let hints = vec![("id".to_string(), LogicalDType::Int64)];

    let outcome = run_vortex_ingest_prepare_once_with_schema(
        vortex_ingest_reuse_request(source, target, false),
        &hints,
    )
    .expect("schema-hinted JSONL preserves inferred fields");
    let report = prepared_vortex_ingest_report(outcome);
    let fields = field_map(report.fields());
    let summary = report.vortex_report.column_family_summary();
    assert!(summary.contains("id:int64"), "{summary}");
    assert!(summary.contains("URL:utf8"), "{summary}");
    assert!(summary.contains("amount:int64"), "{summary}");
    assert!(
        summary.contains("__shardloom_derived_url_domain_URL:"),
        "{summary}"
    );
    assert_text_discovery_policy(&fields, "incremental_jsonl_schema_discovery_replay");
    assert_field_contains(
        &fields,
        "source_state_dictionary_preservation_status",
        "__shardloom_derived_url_domain_URL",
    );
    assert_field_eq(&fields, "fallback_attempted", "false");
    assert_field_eq(&fields, "external_engine_invoked", "false");

    fs::remove_dir_all(root).expect("remove partial jsonl schema root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_partial_csv_schema_hints_preserve_undeclared_columns() {
    let root = vortex_ingest_reuse_test_root("partial-csv-schema-hints");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    fs::write(
        &source,
        "id,URL,event_date,amount\n1,https://www.example.com/a,2026-06-22,10\n2,https://www.example.net/b,2026-06-23,20\n",
    )
    .expect("write partial schema csv source");
    let hints = vec![
        ("event_date".to_string(), LogicalDType::Date32),
        ("id".to_string(), LogicalDType::Int64),
    ];

    let outcome = run_vortex_ingest_prepare_once_with_schema(
        vortex_ingest_reuse_request(source, target, false),
        &hints,
    )
    .expect("schema-hinted CSV preserves inferred fields");
    let report = prepared_vortex_ingest_report(outcome);
    let fields = field_map(report.fields());
    let summary = report.vortex_report.column_family_summary();
    assert!(summary.contains("id:int64"), "{summary}");
    assert!(summary.contains("URL:utf8"), "{summary}");
    assert!(summary.contains("event_date:date32"), "{summary}");
    assert!(summary.contains("amount:int64"), "{summary}");
    assert!(
        summary.contains("__shardloom_derived_url_domain_URL:"),
        "{summary}"
    );
    assert_text_discovery_policy(&fields, "incremental_csv_schema_discovery_replay");
    assert_field_contains(
        &fields,
        "source_state_dictionary_preservation_status",
        "__shardloom_derived_url_domain_URL",
    );
    assert_field_eq(&fields, "fallback_attempted", "false");
    assert_field_eq(&fields, "external_engine_invoked", "false");

    fs::remove_dir_all(root).expect("remove partial csv schema root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn schema_declared_text_stream_preserves_smoke_cap_and_product_no_cap() {
    let root = vortex_ingest_reuse_test_root("schema-declared-product-cap");
    let source = root.join("large.csv");
    let smoke_target = root.join("smoke.vortex");
    let product_target = root.join("product.vortex");
    let inferred_smoke_target = root.join("inferred-smoke.vortex");
    let inferred_product_target = root.join("inferred-product.vortex");
    let hints = vec![
        ("id".to_string(), LogicalDType::Int64),
        ("label".to_string(), LogicalDType::Utf8),
    ];
    let mut body = String::from("id,label\n");
    for row_id in 0..=SMOKE_MAX_INPUT_ROWS {
        writeln!(&mut body, "{row_id},label-{row_id}").expect("write csv row");
    }
    fs::write(&source, body).expect("write large schema-declared csv");

    let smoke_error = run_vortex_ingest_prepare_once_with_schema(
        vortex_ingest_reuse_request(source.clone(), smoke_target, false),
        &hints,
    )
    .expect_err("smoke schema-declared stream preserves the diagnostic row cap");
    assert!(
        smoke_error
            .to_string()
            .contains("supports at most 50000 text data rows"),
        "{smoke_error}"
    );

    let mut product_request =
        vortex_ingest_reuse_request(source.clone(), product_target.clone(), false);
    product_request.runtime_profile = SqlLocalSourceRuntimeProfile::ProductLocalWorkflow;
    let product = run_vortex_ingest_prepare_once_with_schema(product_request, &hints)
        .expect("product schema-declared stream has no synthetic row cap");
    let product_report = prepared_vortex_ingest_report(product);
    let fields = field_map(product_report.fields());
    assert_eq!(
        product_report.vortex_report.row_count,
        u64::try_from(SMOKE_MAX_INPUT_ROWS + 1).expect("test row count fits u64")
    );
    assert_field_eq(
        &fields,
        "source_state_materialization_layout",
        "incremental_text_to_streaming_arrow_record_batch_source_state",
    );
    assert_field_eq(&fields, "source_state_stream_batch_size", "8192");
    assert_text_discovery_policy(&fields, "incremental_csv_schema_discovery_replay");
    assert_field_eq(
        &fields,
        "source_read_buffer_carry_status",
        "incremental_schema_discovery_then_generation_checked_record_replay",
    );
    assert_field_eq(&fields, "source_fingerprint_policy", "metadata_only");
    assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
    assert_field_eq(&fields, "fallback_attempted", "false");
    assert_field_eq(&fields, "external_engine_invoked", "false");
    assert!(product_target.exists());

    let inferred_smoke_error = run_vortex_prepare(vortex_ingest_reuse_request(
        source.clone(),
        inferred_smoke_target,
        false,
    ))
    .expect_err("smoke inferred stream preserves the diagnostic row cap");
    assert!(
        inferred_smoke_error
            .to_string()
            .contains("supports at most 50000 text data rows"),
        "{inferred_smoke_error}"
    );

    let mut inferred_product_request =
        vortex_ingest_reuse_request(source.clone(), inferred_product_target.clone(), false);
    inferred_product_request.runtime_profile = SqlLocalSourceRuntimeProfile::ProductLocalWorkflow;
    let inferred_product = run_vortex_prepare(inferred_product_request)
        .expect("product inferred stream has no synthetic row cap");
    let inferred_product_report = prepared_vortex_ingest_report(inferred_product);
    let inferred_fields = field_map(inferred_product_report.fields());
    assert_eq!(
        inferred_product_report.vortex_report.row_count,
        u64::try_from(SMOKE_MAX_INPUT_ROWS + 1).expect("test row count fits u64")
    );
    assert_field_eq(
        &inferred_fields,
        "source_state_materialization_layout",
        "incremental_text_to_streaming_arrow_record_batch_source_state",
    );
    assert_field_eq(&inferred_fields, "source_state_stream_batch_size", "8192");
    assert_text_discovery_policy(&inferred_fields, "incremental_csv_schema_discovery_replay");
    assert_field_eq(
        &inferred_fields,
        "source_read_buffer_carry_status",
        "incremental_schema_discovery_then_generation_checked_record_replay",
    );
    assert_field_eq(
        &inferred_fields,
        "source_fingerprint_policy",
        "metadata_only",
    );
    assert_field_eq(
        &inferred_fields,
        "source_content_fingerprint_performed",
        "false",
    );
    assert_field_eq(&inferred_fields, "fallback_attempted", "false");
    assert_field_eq(&inferred_fields, "external_engine_invoked", "false");
    assert!(inferred_product_target.exists());

    fs::remove_dir_all(root).expect("remove schema declared product cap root");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn vortex_ingest_source_drift_with_overwrite_rewrites_single_artifact_without_sidecars() {
    let root = vortex_ingest_reuse_test_root("drift-rewrite");
    let source_dir = root.join("source");
    let target_dir = root.join("prepared");
    fs::create_dir_all(&source_dir).expect("create source dir");
    fs::create_dir_all(&target_dir).expect("create target dir");
    let source = source_dir.join("input.csv");
    let target = target_dir.join("prepared.vortex");
    fs::write(&source, "id,label,amount\n1,alpha,10\n").expect("write initial source");

    let first = run_vortex_prepare(vortex_ingest_reuse_request(
        source.clone(),
        target.clone(),
        false,
    ))
    .expect("first vortex_ingest run writes artifact");
    assert!(matches!(first, VortexIngestOutcome::Prepared(_)));
    let base_artifact = fs::read(&target).expect("read base artifact");
    let manifest_path = shardloom_vortex::vortex_prepared_state_reuse_manifest_path(&target)
        .expect("manifest path");
    assert!(!manifest_path.exists());

    fs::write(&source, "id,label,amount\n1,alpha,10\n2,beta,99\n").expect("mutate source");
    let second = run_vortex_prepare(vortex_ingest_reuse_request(
        source.clone(),
        target.clone(),
        true,
    ))
    .expect("changed source rewrites single Vortex artifact");
    let second_report = prepared_vortex_ingest_report(second);
    let second_fields = field_map(second_report.fields());
    assert_ne!(
        fs::read(&target).expect("read rewritten artifact"),
        base_artifact,
        "changed source with allow_overwrite must replace the single .vortex artifact"
    );
    assert_field_eq(&second_fields, "vortex_ingest_performed", "true");
    assert_field_eq(
        &second_fields,
        "vortex_ingest_status",
        "prepared_state_created",
    );
    assert_field_eq(&second_fields, "prepared_state_created", "true");
    assert_field_eq(&second_fields, "prepared_state_reused", "false");
    assert_field_eq(&second_fields, "prepared_state_reuse_hit", "false");
    assert_field_eq(
        &second_fields,
        "prepared_state_invalidation_reason",
        "not_applicable_single_vortex_artifact",
    );
    assert_field_eq(&second_fields, "fallback_attempted", "false");
    assert_field_eq(&second_fields, "external_engine_invoked", "false");
    assert!(!manifest_path.exists());
    assert!(
        fs::read_dir(target_dir.join(".shardloom"))
            .map_or(true, |mut entries| entries.next().is_none()),
        "public source drift rewrite must not leave target-adjacent sidecar files"
    );

    fs::remove_dir_all(root).expect("remove reuse drift root");
}

#[test]
fn differential_preparation_schema_mismatch_rejects_overlay_without_fallback() {
    let report = shardloom_vortex::evaluate_vortex_differential_preparation(
        shardloom_vortex::VortexDifferentialPreparationInput {
            update_mode: shardloom_vortex::VortexDifferentialUpdateMode::AppendOnly,
            base_source_state_id: "base-source".to_string(),
            base_source_state_digest: "fnv64:base-source".to_string(),
            base_prepared_state_id: "base-prepared".to_string(),
            base_prepared_state_digest: "fnv64:base-prepared".to_string(),
            base_row_count: 1,
            base_schema_digest: "fnv64:base-schema".to_string(),
            base_column_family_summary: "id:int64,label:utf8".to_string(),
            delta_source_state_id: "delta-source".to_string(),
            delta_source_state_digest: "fnv64:delta-source".to_string(),
            delta_row_count: 1,
            delta_schema_digest: "fnv64:changed-schema".to_string(),
            delta_column_family_summary: "id:int64,label:utf8,extra:utf8".to_string(),
            delta_manifest_digest: "fnv64:delta-manifest".to_string(),
            changed_byte_range_refs: "target/input.csv#bytes=10..20".to_string(),
            changed_row_range_refs: "target/input.csv#rows=1..2".to_string(),
            changed_segment_refs: "delta-segment".to_string(),
            delta_artifact_ref: "target/input.delta.vortex".to_string(),
            delta_artifact_digest: "fnv64:delta-artifact".to_string(),
            native_io_certificate_refs: "base=base-prepared;delta=delta-prepared".to_string(),
        },
    );

    assert!(!report.is_admitted());
    assert_eq!(report.status, "blocked_schema_mismatch");
    assert_eq!(
        report.schema_compatibility_status,
        "blocked_source_schema_or_column_family_mismatch"
    );
    assert!(!report.overlay_applied);
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn parses_scoped_sql_local_source_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
    )
    .expect("statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
    assert_eq!(parsed.group_by, [] as [String; 0]);
    assert!(parsed.order_by.is_none());
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/input.csv")
    );
    assert_eq!(parsed.limit, 5);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Compare {
            ref column,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Int64(10)
        } if column == "amount"
    ));
}

#[test]
fn parses_scoped_sql_local_source_statement_without_predicate() {
    let parsed =
        parse_sql_local_source_statement("SELECT id,label FROM 'target/input.csv' LIMIT 5")
            .expect("statement parses without a predicate");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
    assert_eq!(parsed.group_by, [] as [String; 0]);
    assert!(parsed.order_by.is_none());
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/input.csv")
    );
    assert_eq!(parsed.limit, 5);
    assert!(parsed.predicate.is_all());
}

#[test]
fn parses_scoped_select_distinct_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT DISTINCT region,label FROM 'target/input.csv' WHERE amount >= 10 ORDER BY region,label LIMIT 5",
    )
    .expect("SELECT DISTINCT statement parses");

    assert_eq!(parsed.projections, vec!["region", "label"]);
    assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
}

#[test]
fn parses_scoped_literal_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label,'north' AS segment,DATE '2026-05-19' AS batch_date,X'00ff10' AS payload,BINARY 'ok' AS text_payload,BLOB 'raw' AS blob_payload FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
    )
    .expect("literal projection statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert_eq!(parsed.literal_projections.len(), 5);
    assert_eq!(parsed.literal_projections[0].alias, "segment");
    assert_eq!(
        parsed.literal_projections[0].value,
        ScalarValue::Utf8("north".to_string())
    );
    assert_eq!(parsed.literal_projections[1].alias, "batch_date");
    assert!(matches!(
        parsed.literal_projections[1].value,
        ScalarValue::Date32(_)
    ));
    assert_eq!(parsed.literal_projections[2].alias, "payload");
    assert_eq!(
        parsed.literal_projections[2].value,
        ScalarValue::Binary(vec![0x00, 0xff, 0x10])
    );
    assert_eq!(parsed.literal_projections[3].alias, "text_payload");
    assert_eq!(
        parsed.literal_projections[3].value,
        ScalarValue::Binary(b"ok".to_vec())
    );
    assert_eq!(parsed.literal_projections[4].alias, "blob_payload");
    assert_eq!(
        parsed.literal_projections[4].value,
        ScalarValue::Binary(b"raw".to_vec())
    );
}

#[test]
fn binary_hex_literal_projection_blocks_malformed_literals_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id,X'0' AS payload FROM 'target/input.csv' LIMIT 5",
            "binary hex literals require an even number of hexadecimal digits",
        ),
        (
            "SELECT id,X'00xz' AS payload FROM 'target/input.csv' LIMIT 5",
            "binary hex literals admit hexadecimal digits only",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("malformed binary hex literal remains blocked");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn binary_text_literal_projection_blocks_malformed_literals_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id,BINARY alpha AS payload FROM 'target/input.csv' LIMIT 5",
            "SQL string literals must be single quoted",
        ),
        (
            "SELECT id,BLOB 'raw AS payload FROM 'target/input.csv' LIMIT 5",
            "SQL string literal is not closed",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("malformed binary text literal remains blocked");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn binary_literals_are_admitted_in_direct_predicates() {
    for (statement, expected_op, expected_value) in [
        (
            "SELECT id FROM 'target/input.csv' WHERE payload = X'616c706861' LIMIT 5",
            ComparisonOp::Eq,
            b"alpha".to_vec(),
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE payload > BINARY 'alpha' LIMIT 5",
            ComparisonOp::Gt,
            b"alpha".to_vec(),
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE payload <= BLOB 'raw' LIMIT 5",
            ComparisonOp::LtEq,
            b"raw".to_vec(),
        ),
    ] {
        let parsed = parse_sql_local_source_statement(statement)
            .expect("direct binary predicate statement parses");

        assert!(
            matches!(
                parsed.predicate,
                ParsedPredicate::Compare {
                    ref column,
                    op,
                    value: ScalarValue::Binary(ref value),
                } if column == "payload"
                    && op == expected_op
                    && value.as_slice() == expected_value.as_slice()
            ),
            "unexpected predicate for {statement}: {:?}",
            parsed.predicate
        );
    }

    let error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE label = X'0' LIMIT 5",
    )
    .expect_err("malformed binary hex predicate literal remains blocked");
    assert!(
        error
            .to_string()
            .contains("binary hex literals require an even number")
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));

    for statement in [
        "SELECT id,CASE WHEN id = 1 THEN X'00' ELSE X'ff' END AS payload FROM 'target/input.csv' LIMIT 5",
        "SELECT id,CASE WHEN id = 1 THEN BINARY 'x' ELSE BLOB 'y' END AS payload FROM 'target/input.csv' LIMIT 5",
    ] {
        let parsed = parse_sql_local_source_statement(statement)
            .expect("binary conditional declarations lower to the native expression binder");
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(parsed.generic_expression_projections[0].alias, "payload");
    }
}

#[test]
fn binary_helper_predicates_are_admitted() {
    for (statement, expected_op, expected_comparison, expected_value, expected_source_columns) in [
        (
            "SELECT id FROM 'target/input.csv' WHERE UNHEX(hex_payload) = X'00ff10' LIMIT 5",
            BinaryHelperOp::Unhex,
            ComparisonOp::Eq,
            vec![0x00, 0xff, 0x10],
            vec!["hex_payload"],
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE FROM_BASE64(b64_payload) >= BINARY 'alpha' LIMIT 5",
            BinaryHelperOp::FromBase64,
            ComparisonOp::GtEq,
            b"alpha".to_vec(),
            vec!["b64_payload"],
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE FROM_BASE64(b64_payload) < BLOB 'raw' LIMIT 5",
            BinaryHelperOp::FromBase64,
            ComparisonOp::Lt,
            b"raw".to_vec(),
            vec!["b64_payload"],
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE FROM_BASE64(CONCAT(b64_prefix,b64_suffix)) = BINARY 'alpha' LIMIT 5",
            BinaryHelperOp::FromBase64,
            ComparisonOp::Eq,
            b"alpha".to_vec(),
            vec!["b64_prefix", "b64_suffix"],
        ),
    ] {
        let parsed =
            parse_sql_local_source_statement(statement).expect("binary helper predicate parses");

        assert!(
            matches!(
                &parsed.predicate,
                ParsedPredicate::BinaryHelperCompare(ParsedBinaryHelperPredicate {
                    source_columns,
                    op,
                    comparison,
                    value,
                    ..
                }) if source_columns
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    == expected_source_columns
                    && *op == expected_op
                    && *comparison == expected_comparison
                    && value.as_slice() == expected_value.as_slice()
            ),
            "unexpected helper predicate for {statement}: {:?}",
            parsed.predicate
        );
    }

    let error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE UNHEX(hex_payload) = NULL LIMIT 5",
    )
    .expect_err("NULL helper predicate literal remains blocked");
    assert!(
        error
            .to_string()
            .contains("binary helper predicates admit X'<hex>'"),
        "{error}"
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parses_scoped_complex_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,ARRAY[1,2,NULL] AS values,STRUCT(label, amount) AS payload FROM 'target/input.csv' LIMIT 5",
    )
    .expect("scoped complex projections parse");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.complex_projections.len(), 2);
    assert_eq!(parsed.complex_projections[0].alias, "values");
    assert_eq!(
        parsed.complex_projections[0].kind,
        ParsedComplexProjectionKind::ArrayLiteral(vec![
            ScalarValue::Int64(1),
            ScalarValue::Int64(2),
            ScalarValue::Null
        ])
    );
    assert_eq!(parsed.complex_projections[1].alias, "payload");
    assert_eq!(
        parsed.complex_projections[1].kind,
        ParsedComplexProjectionKind::StructColumns(vec!["label".to_string(), "amount".to_string()])
    );
    assert!(parsed.has_complex_projection());
}

#[test]
fn parses_scoped_binary_helper_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,UNHEX(LOWER(TRIM(hex_payload))) AS payload_hex,FROM_BASE64(CONCAT(b64_prefix,b64_suffix)) AS payload_b64 FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("binary helper projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.binary_helper_projections.len(), 2);
    assert_eq!(parsed.binary_helper_projections[0].alias, "payload_hex");
    assert_eq!(
        parsed.binary_helper_projections[0].source_columns,
        vec!["hex_payload"]
    );
    assert_eq!(
        parsed.binary_helper_projections[0].op,
        BinaryHelperOp::Unhex
    );
    assert_eq!(parsed.binary_helper_projections[1].alias, "payload_b64");
    assert_eq!(
        parsed.binary_helper_projections[1].source_columns,
        vec!["b64_prefix", "b64_suffix"]
    );
    assert_eq!(
        parsed.binary_helper_projections[1].op,
        BinaryHelperOp::FromBase64
    );
}

#[test]
fn parses_scoped_binary_byte_length_projection_and_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,BYTE_LENGTH(UNHEX(LOWER(TRIM(hex_payload)))) AS payload_len,OCTET_LENGTH(CAST(CONCAT(label_prefix,label_suffix) AS binary)) AS label_len FROM 'target/input.csv' WHERE BYTE_LENGTH(FROM_BASE64(CONCAT(b64_prefix,b64_suffix))) >= 4 LIMIT 5",
    )
    .expect("binary byte length statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.binary_byte_length_projections.len(), 2);
    assert_eq!(
        parsed.binary_byte_length_projections[0].alias,
        "payload_len"
    );
    assert_eq!(
        parsed.binary_byte_length_projections[0].argument_family,
        BinaryByteLengthArgumentFamily::Helper(BinaryHelperOp::Unhex)
    );
    assert_eq!(
        parsed.binary_byte_length_projections[0].source_columns,
        vec!["hex_payload"]
    );
    assert_eq!(
        parsed.binary_byte_length_projections[1].argument_family,
        BinaryByteLengthArgumentFamily::Cast(CastMode::Strict)
    );
    assert_eq!(
        parsed.binary_byte_length_projections[1].source_columns,
        vec!["label_prefix", "label_suffix"]
    );

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::BinaryByteLengthCompare(ParsedBinaryByteLengthPredicate {
            argument_family: BinaryByteLengthArgumentFamily::Helper(BinaryHelperOp::FromBase64),
            comparison: ComparisonOp::GtEq,
            value: ScalarValue::Int64(4),
            ..
        })
    ));
}

#[test]
fn parses_scoped_numeric_arithmetic_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount + 5 AS adjusted,ratio * 2.0 AS doubled FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
    )
    .expect("numeric arithmetic projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.literal_projections,
        [] as [ParsedLiteralProjection; 0]
    );
    assert_eq!(parsed.numeric_arithmetic_projections.len(), 2);
    assert_eq!(parsed.numeric_arithmetic_projections[0].alias, "adjusted");
    assert_eq!(parsed.numeric_arithmetic_projections[0].column, "amount");
    assert_eq!(
        parsed.numeric_arithmetic_projections[0].op,
        NumericArithmeticOp::Add
    );
    assert_eq!(
        parsed.numeric_arithmetic_projections[0].rhs,
        ScalarValue::Int64(5)
    );
    assert_eq!(parsed.numeric_arithmetic_projections[1].alias, "doubled");
}

#[test]
fn parses_scoped_decimal_generic_arithmetic_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CAST(amount AS decimal128(10,2)) + CAST('1.25' AS decimal128(10,2)) AS adjusted FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("decimal generic arithmetic projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.numeric_arithmetic_projections,
        [] as [ParsedNumericArithmeticProjection; 0]
    );
    assert_eq!(parsed.generic_expression_projections.len(), 1);
    assert_eq!(parsed.generic_expression_projections[0].alias, "adjusted");
}

#[test]
fn parses_star_plus_computed_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT *,amount + 5 AS adjusted,LOWER(label) AS normalized FROM 'target/input.jsonl' WHERE amount >= 10 LIMIT 5",
    )
    .expect("star plus computed projection statement parses");

    assert_eq!(parsed.projections, vec!["*"]);
    assert_eq!(parsed.numeric_arithmetic_projections.len(), 1);
    assert_eq!(parsed.numeric_arithmetic_projections[0].alias, "adjusted");
    assert_eq!(parsed.string_transform_projections.len(), 1);
    assert_eq!(parsed.string_transform_projections[0].alias, "normalized");
}

#[test]
fn parses_computed_projection_order_by_topn_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount + 5 AS adjusted FROM 'target/input.csv' WHERE amount >= 10 ORDER BY adjusted DESC LIMIT 2",
    )
    .expect("computed projection order-by statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.numeric_arithmetic_projections.len(), 1);
    assert_eq!(parsed.numeric_arithmetic_projections[0].alias, "adjusted");
    let order_by = parsed.order_by.as_ref().expect("order by parsed");
    assert_eq!(order_by.keys.len(), 1);
    assert_eq!(order_by.keys[0].column, "adjusted");
    assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
}

#[test]
fn parses_scoped_window_row_number_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,region,amount,ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM 'target/input.csv' WHERE amount >= 10 LIMIT 4",
    )
    .expect("window row-number statement parses");

    assert_eq!(parsed.projections, vec!["id", "region", "amount"]);
    assert_eq!(parsed.window_projections.len(), 1);
    let window = &parsed.window_projections[0];
    assert_eq!(window.alias, "rn");
    assert_eq!(window.function, WindowFunction::RowNumber);
    assert_eq!(window.partition_by, vec!["region".to_string()]);
}

#[test]
fn parses_scoped_window_ranking_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,region,RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS r,DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS dr FROM 'target/input.csv' LIMIT 6",
    )
    .expect("window ranking statement parses");

    assert_eq!(parsed.projections, vec!["id", "region"]);
    assert_eq!(parsed.window_projections.len(), 2);
    assert_eq!(parsed.window_projections[0].alias, "r");
    assert_eq!(parsed.window_projections[0].function, WindowFunction::Rank);
    assert_eq!(parsed.window_projections[1].alias, "dr");
    assert_eq!(
        parsed.window_projections[1].function,
        WindowFunction::DenseRank
    );
}

#[test]
fn parses_scoped_window_offset_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,region,LAG(label) OVER (PARTITION BY region ORDER BY amount ASC) AS previous_label,LEAD(label, 2) OVER (PARTITION BY region ORDER BY amount ASC) AS next2_label FROM 'target/input.csv' LIMIT 6",
    )
    .expect("window offset statement parses");

    assert_eq!(parsed.projections, vec!["id", "region"]);
    assert_eq!(parsed.window_projections.len(), 2);
    assert_eq!(parsed.window_projections[0].alias, "previous_label");
    assert_eq!(
        parsed.window_projections[0].function,
        WindowFunction::Lag {
            column: "label".to_string(),
            offset: 1,
        }
    );
    assert_eq!(parsed.window_projections[1].alias, "next2_label");
    assert_eq!(
        parsed.window_projections[1].function,
        WindowFunction::Lead {
            column: "label".to_string(),
            offset: 2,
        }
    );
}

#[test]
fn parses_scoped_window_distribution_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,region,NTILE(4) OVER (PARTITION BY region ORDER BY amount DESC) AS bucket,PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS percent_rank,CUME_DIST() OVER (PARTITION BY region ORDER BY amount DESC) AS cume_dist FROM 'target/input.csv' LIMIT 6",
    )
    .expect("window distribution statement parses");

    assert_eq!(parsed.projections, vec!["id", "region"]);
    assert_eq!(parsed.window_projections.len(), 3);
    assert_eq!(parsed.window_projections[0].alias, "bucket");
    assert_eq!(
        parsed.window_projections[0].function,
        WindowFunction::Ntile { bucket_count: 4 }
    );
    assert_eq!(parsed.window_projections[1].alias, "percent_rank");
    assert_eq!(
        parsed.window_projections[1].function,
        WindowFunction::PercentRank
    );
    assert_eq!(parsed.window_projections[2].alias, "cume_dist");
    assert_eq!(
        parsed.window_projections[2].function,
        WindowFunction::CumeDist
    );
}

#[test]
fn parser_blocks_invalid_window_offset_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id,LAG(label, 0) OVER (ORDER BY amount ASC) AS previous_label FROM 'target/input.csv' LIMIT 6",
    )
    .expect_err("zero offset is not admitted");

    assert!(
        error
            .to_string()
            .contains("LAG window offset must be at least 1")
    );
}

#[test]
fn parser_blocks_invalid_window_bucket_count_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id,NTILE(0) OVER (ORDER BY amount ASC) AS bucket FROM 'target/input.csv' LIMIT 6",
    )
    .expect_err("zero bucket count is not admitted");

    assert!(
        error
            .to_string()
            .contains("NTILE window bucket count must be at least 1")
    );
}

#[test]
fn parser_blocks_star_plus_raw_projection_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT *,id FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
    )
    .expect_err("star plus raw projection is not admitted");

    assert!(error.to_string().contains(
        "SELECT * can be mixed only with computed, literal, or window projections in this scoped smoke"
    ));
}

#[test]
fn parser_blocks_repeated_wildcard_projection_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT *,* FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
    )
    .expect_err("repeated wildcard projection is not admitted");

    assert!(
        error
            .to_string()
            .contains("SELECT * may appear only once in this scoped smoke")
    );
}

#[test]
fn parser_admits_left_literal_first_argument() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,LEFT('literal', 2) AS prefix FROM 'target/input.csv' LIMIT 5",
    )
    .expect("constant scalar calls lower to the native expression binder");
    assert_eq!(parsed.generic_expression_projections.len(), 1);
    assert_eq!(parsed.generic_expression_projections[0].alias, "prefix");
}

#[test]
fn parser_uses_earliest_join_keyword_for_join_type() {
    let raw = "'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.id = d.id LEFT JOIN tail";
    let (index, keyword_len, join_type) =
        find_join_keyword(raw).unwrap().expect("join keyword found");

    assert_eq!(&raw[index..index + keyword_len], "JOIN");
    assert_eq!(join_type, ParsedJoinType::InnerEqui);
}

#[test]
fn parses_generic_expression_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,(amount + tax) * 2 AS gross,ABS(amount - tax) AS spread FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
    )
    .expect("generic expression projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.numeric_arithmetic_projections,
        [] as [ParsedNumericArithmeticProjection; 0]
    );
    assert_eq!(parsed.generic_expression_projections.len(), 2);
    assert_eq!(parsed.generic_expression_projections[0].alias, "gross");
    assert_eq!(
        parsed.generic_expression_projections[0].source_columns,
        vec!["amount".to_string(), "tax".to_string()]
    );
}

#[test]
fn parses_temporal_difference_generic_expression_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,DATE_DIFF_DAYS(CAST(end_date AS date32), start_date) AS age_days,TIMESTAMP_DIFF_SECONDS(CAST(end_ts AS timestamp_micros), start_ts) AS elapsed_seconds FROM 'target/input.csv' WHERE DATE_DIFF_DAYS(end_date, DATE '2026-05-19') >= 2 LIMIT 5",
    )
    .expect("temporal difference generic expression statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.generic_expression_projections.len(), 2);

    assert!(parsed.predicate.uses_generic_expression());

    assert_eq!(parsed.predicate.columns(), vec!["end_date"]);
}

#[test]
fn parses_scoped_numeric_abs_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,ABS(amount) AS magnitude FROM 'target/input.csv' WHERE ABS(amount) >= 4 LIMIT 5",
    )
    .expect("numeric abs projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.literal_projections,
        [] as [ParsedLiteralProjection; 0]
    );
    assert_eq!(parsed.numeric_abs_projections.len(), 1);
    assert_eq!(parsed.numeric_abs_projections[0].alias, "magnitude");
    assert_eq!(parsed.numeric_abs_projections[0].column, "amount");
}

#[test]
fn parses_scoped_numeric_rounding_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,FLOOR(amount) AS bucket,CEIL(ratio) AS upper FROM 'target/input.csv' WHERE ROUND(amount) >= 4 LIMIT 5",
    )
    .expect("numeric rounding projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.numeric_rounding_projections.len(), 2);
    assert_eq!(parsed.numeric_rounding_projections[0].alias, "bucket");
    assert_eq!(parsed.numeric_rounding_projections[0].column, "amount");
    assert_eq!(
        parsed.numeric_rounding_projections[0].op,
        NumericRoundingOp::Floor
    );
    assert_eq!(parsed.numeric_rounding_projections[1].alias, "upper");
    assert_eq!(parsed.numeric_rounding_projections[1].column, "ratio");
    assert_eq!(
        parsed.numeric_rounding_projections[1].op,
        NumericRoundingOp::Ceil
    );
}

#[test]
fn parses_scoped_cast_projection_statement() {
    let statement = concat!(
        "SELECT id,CAST(amount AS float64) AS amount_float,",
        "CAST(event_date AS date32) AS event_day,",
        "CAST(CONCAT(label_prefix,label_suffix) AS binary) AS label_bytes ",
        "FROM 'target/input.csv' WHERE id >= 1 LIMIT 5"
    );
    let parsed =
        parse_sql_local_source_statement(statement).expect("cast projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.literal_projections,
        [] as [ParsedLiteralProjection; 0]
    );
    assert_eq!(parsed.cast_projections.len(), 3);
    assert_eq!(parsed.cast_projections[0].alias, "amount_float");
    assert_eq!(parsed.cast_projections[0].column, "amount");
    assert_eq!(
        parsed.cast_projections[0].target_dtype,
        LogicalDType::Float64
    );
    assert_eq!(parsed.cast_projections[0].mode, CastMode::Strict);
    assert_eq!(parsed.cast_projections[1].alias, "event_day");
    assert_eq!(parsed.cast_projections[1].column, "event_date");
    assert_eq!(
        parsed.cast_projections[1].target_dtype,
        LogicalDType::Date32
    );
    assert_eq!(parsed.cast_projections[1].mode, CastMode::Strict);
    assert_eq!(parsed.cast_projections[2].alias, "label_bytes");
    assert_eq!(parsed.cast_projections[2].column, "label_prefix");
    assert_eq!(
        parsed.cast_projections[2].source_columns,
        vec!["label_prefix".to_string(), "label_suffix".to_string()]
    );
    assert_eq!(
        parsed.cast_projections[2].target_dtype,
        LogicalDType::Binary
    );
    assert_eq!(parsed.cast_projections[2].mode, CastMode::Strict);
}

#[test]
fn parses_scoped_try_cast_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,TRY_CAST(raw_amount AS int64) AS amount_i64 FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("try_cast projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.cast_projections.len(), 1);
    assert_eq!(parsed.cast_projections[0].alias, "amount_i64");
    assert_eq!(parsed.cast_projections[0].column, "raw_amount");
    assert_eq!(parsed.cast_projections[0].target_dtype, LogicalDType::Int64);
    assert_eq!(parsed.cast_projections[0].mode, CastMode::Try);
}

#[test]
fn parses_scoped_date_arithmetic_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,DATE_ADD_DAYS(CAST(event_date AS date32), 7) AS next_week,DATE_SUB_DAYS(event_date, 1) AS prior_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("date arithmetic projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.date_arithmetic_projections.len(), 2);
    assert_eq!(parsed.date_arithmetic_projections[0].alias, "next_week");
    assert_eq!(parsed.date_arithmetic_projections[0].column, "event_date");
    assert_eq!(
        parsed.date_arithmetic_projections[0].op,
        DateArithmeticOp::AddDays
    );
    assert_eq!(parsed.date_arithmetic_projections[0].day_count, 7);
    assert_eq!(parsed.date_arithmetic_projections[1].alias, "prior_day");
    assert_eq!(parsed.date_arithmetic_projections[1].column, "event_date");
    assert_eq!(
        parsed.date_arithmetic_projections[1].op,
        DateArithmeticOp::SubDays
    );
    assert_eq!(parsed.date_arithmetic_projections[1].day_count, 1);
}

#[test]
fn parses_scoped_timestamp_arithmetic_projection_and_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,TIMESTAMP_ADD_SECONDS(CAST(event_ts AS timestamp_micros), 90) AS shifted_ts,TIMESTAMP_SUB_SECONDS(event_ts, 45) AS prior_ts FROM 'target/input.csv' WHERE TIMESTAMP_ADD_SECONDS(event_ts, 60) >= TIMESTAMP '2026-05-19T12:35:45Z' LIMIT 5",
    )
    .expect("timestamp arithmetic projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.timestamp_arithmetic_projections.len(), 2);
    assert_eq!(
        parsed.timestamp_arithmetic_projections[0].alias,
        "shifted_ts"
    );
    assert_eq!(
        parsed.timestamp_arithmetic_projections[0].column,
        "event_ts"
    );
    assert_eq!(
        parsed.timestamp_arithmetic_projections[0].op,
        TimestampArithmeticOp::AddSeconds
    );
    assert_eq!(parsed.timestamp_arithmetic_projections[0].second_count, 90);
    assert_eq!(parsed.timestamp_arithmetic_projections[1].alias, "prior_ts");
    assert_eq!(
        parsed.timestamp_arithmetic_projections[1].op,
        TimestampArithmeticOp::SubSeconds
    );

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::TimestampArithmeticCompare {
            ref column,
            op: TimestampArithmeticOp::AddSeconds,
            second_count: 60,
            comparison: ComparisonOp::GtEq,
            value: ScalarValue::TimestampMicros(_),
        } if column == "event_ts"
    ));
}

#[test]
fn parses_scoped_interval_literal_temporal_arithmetic_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,DATE_ADD_DAYS(event_date, INTERVAL '1' DAY) AS next_day,DATE_SUB_DAYS(event_date, INTERVAL '2' DAYS) AS prior_two,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '90' SECOND) AS shifted_ts,TIMESTAMP_SUB_SECONDS(event_ts, INTERVAL '1' MINUTE) AS prior_minute FROM 'target/input.csv' WHERE TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' HOUR) >= TIMESTAMP '2026-05-19T13:34:45Z' LIMIT 5",
    )
    .expect("scoped interval literal temporal arithmetic parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.date_arithmetic_projections.len(), 2);

    assert_eq!(parsed.timestamp_arithmetic_projections.len(), 2);

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::TimestampArithmeticCompare {
            ref column,
            op: TimestampArithmeticOp::AddSeconds,
            second_count: 3600,
            comparison: ComparisonOp::GtEq,
            value: ScalarValue::TimestampMicros(_),
        } if column == "event_ts"
    ));
}

#[test]
fn parses_scoped_null_coalesce_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,COALESCE(label, 'unknown') AS label_clean,COALESCE(event_date, DATE '2026-01-01') AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("null coalesce projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.null_coalesce_projections.len(), 2);
    assert_eq!(parsed.null_coalesce_projections[0].alias, "label_clean");
    assert_eq!(parsed.null_coalesce_projections[0].column, "label");
    assert_eq!(
        parsed.null_coalesce_projections[0].fallback,
        ScalarValue::Utf8("unknown".to_string())
    );
    assert_eq!(parsed.null_coalesce_projections[1].alias, "event_day");
    assert_eq!(parsed.null_coalesce_projections[1].column, "event_date");
    assert!(matches!(
        parsed.null_coalesce_projections[1].fallback,
        ScalarValue::Date32(_)
    ));
}

#[test]
fn parses_scoped_nullif_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,NULLIF(label, 'missing') AS label_clean,NULLIF(CAST(event_date AS date32), DATE '2026-01-01') AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("nullif projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.nullif_projections.len(), 2);
    assert_eq!(parsed.nullif_projections[0].alias, "label_clean");
    assert_eq!(parsed.nullif_projections[0].column, "label");
    assert_eq!(
        parsed.nullif_projections[0].sentinel,
        ScalarValue::Utf8("missing".to_string())
    );
    assert_eq!(parsed.nullif_projections[1].alias, "event_day");
    assert_eq!(parsed.nullif_projections[1].column, "event_date");
    assert_eq!(
        parsed.nullif_projections[1].source_cast_dtype,
        Some(LogicalDType::Date32)
    );
    assert!(matches!(
        parsed.nullif_projections[1].sentinel,
        ScalarValue::Date32(_)
    ));
}

#[test]
fn parses_scoped_conditional_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 'small' END AS size_band,CASE WHEN event_date >= DATE '2026-01-01' THEN DATE '2026-12-31' ELSE DATE '2025-12-31' END AS cutoff_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("conditional projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.conditional_projections.len(), 2);
    assert_eq!(parsed.conditional_projections[0].alias, "size_band");

    assert_eq!(
        parsed.conditional_projections[0].then_branch,
        ParsedConditionalBranch::Literal(ScalarValue::Utf8("large".to_string()))
    );
    assert_eq!(
        parsed.conditional_projections[0].else_branch,
        ParsedConditionalBranch::Literal(ScalarValue::Utf8("small".to_string()))
    );
    assert_eq!(parsed.conditional_projections[1].alias, "cutoff_day");
    assert!(matches!(
        parsed.conditional_projections[1].then_branch,
        ParsedConditionalBranch::Literal(ScalarValue::Date32(_))
    ));
}

#[test]
fn parses_scoped_conditional_projection_source_column_branches() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CASE WHEN amount >= 10 THEN preferred_label ELSE fallback_label END AS label_out FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("conditional projection column branch statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.conditional_projections.len(), 1);
    assert_eq!(parsed.conditional_projections[0].alias, "label_out");
    assert_eq!(
        parsed.conditional_projections[0].then_branch,
        ParsedConditionalBranch::Column("preferred_label".to_string())
    );
    assert_eq!(
        parsed.conditional_projections[0].else_branch,
        ParsedConditionalBranch::Column("fallback_label".to_string())
    );
}

#[test]
fn parses_scoped_predicate_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount >= 10 AS is_large,label IS NULL AS missing_label,active IS NOT TRUE AS inactive_or_unknown FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("predicate projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.predicate_projections.len(), 3);
    assert_eq!(parsed.predicate_projections[0].alias, "is_large");
}

#[test]
fn parses_scoped_correlated_predicate_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,id IN (SELECT id FROM 'target/allowed.csv' WHERE id = outer.id LIMIT 5) AS matched FROM 'target/input.csv' LIMIT 5",
    )
    .expect("correlated predicate projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.predicate_projections.len(), 1);
    assert_eq!(parsed.predicate_projections[0].alias, "matched");

    assert!(parsed.uses_outer_correlation());
}

#[test]
fn projected_exists_subqueries_use_predicate_lowering() {
    for (expression, correlated) in [
        ("EXISTS (SELECT * FROM 'inner.vortex')", false),
        ("(eXiStS (SELECT 1 FROM 'inner.vortex'))", false),
        ("NOT EXISTS (SELECT id FROM 'inner.vortex')", false),
        (
            "EXISTS (SELECT id FROM 'inner.vortex' WHERE id = outer.id)",
            true,
        ),
    ] {
        let statement = format!("SELECT id,{expression} AS present FROM 'outer.vortex' LIMIT 5");
        let parsed = parse_sql_local_source_statement(&statement).unwrap();
        assert_eq!(parsed.predicate_projections.len(), 1, "{expression}");
        assert_eq!(parsed.predicate_projections[0].alias, "present");
        assert_eq!(
            parsed.generic_expression_projections,
            [] as [ParsedGenericExpressionProjection; 0]
        );
        assert_eq!(parsed.uses_outer_correlation(), correlated, "{expression}");
    }
    for expression in [
        "exists_value",
        "'exists'",
        "(SELECT id FROM 'inner.vortex' WHERE EXISTS (SELECT 1 FROM 'other.vortex'))",
    ] {
        assert!(!is_explicit_predicate_projection_shape(expression).unwrap());
    }
}

#[test]
fn parses_scoped_correlated_conditional_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CASE WHEN id IN (SELECT id FROM 'target/allowed.csv' WHERE id = outer.id LIMIT 5) THEN 'yes' ELSE 'no' END AS matched FROM 'target/input.csv' LIMIT 5",
    )
    .expect("correlated CASE projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.conditional_projections.len(), 1);
    assert_eq!(parsed.conditional_projections[0].alias, "matched");

    assert!(parsed.uses_outer_correlation());
}

#[test]
fn parses_generic_expression_compare_projection_as_predicate_projection() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount + fee >= 10 AS is_profitable FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("generic expression predicate projection parses");

    assert_eq!(parsed.predicate_projections.len(), 1);
    assert_eq!(parsed.predicate_projections[0].alias, "is_profitable");

    assert_eq!(
        parsed.generic_expression_projections,
        [] as [ParsedGenericExpressionProjection; 0]
    );
}

#[test]
fn parses_scoped_string_transform_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,LOWER(label) AS lowered,UPPER(label) AS raised,TRIM(label) AS trimmed FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("string transform projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.literal_projections,
        [] as [ParsedLiteralProjection; 0]
    );
    assert_eq!(
        parsed.numeric_arithmetic_projections,
        [] as [ParsedNumericArithmeticProjection; 0]
    );
    assert_eq!(parsed.string_transform_projections.len(), 3);
    assert_eq!(parsed.string_transform_projections[0].alias, "lowered");
    assert_eq!(
        parsed.string_transform_projections[0].source_columns,
        vec!["label".to_string()]
    );
    assert_eq!(
        parsed.string_transform_projections[0].op,
        StringTransformOp::Lower
    );
}

#[test]
fn parses_scoped_string_length_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,LENGTH(label) AS label_len FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("string length projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(
        parsed.literal_projections,
        [] as [ParsedLiteralProjection; 0]
    );
    assert_eq!(
        parsed.string_transform_projections,
        [] as [ParsedStringTransformProjection; 0]
    );
    assert_eq!(parsed.string_length_projections.len(), 1);
    assert_eq!(parsed.string_length_projections[0].alias, "label_len");
    assert_eq!(
        parsed.string_length_projections[0].source_columns,
        vec!["label".to_string()]
    );
}

#[test]
fn parses_scoped_string_function_projection_and_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CONCAT(label, '-', segment) AS label_key,SUBSTR(label, 2, 3) AS middle,LEFT(label, 2) AS prefix,RIGHT(label, 2) AS suffix,REPLACE(label, 'a', '') AS scrubbed FROM 'target/input.csv' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 5",
    )
    .expect("string function projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.string_function_projections.len(), 5);
    assert_eq!(parsed.string_function_projections[0].alias, "label_key");
    assert_eq!(
        parsed.string_function_projections[0].op,
        StringFunctionOp::Concat
    );
    assert_eq!(
        parsed.string_function_projections[0].source_columns,
        vec!["label".to_string(), "segment".to_string()]
    );
    assert_eq!(parsed.string_function_projections[0].literal_count, 1);
    assert_eq!(
        parsed.string_function_projections[1].op,
        StringFunctionOp::Substr
    );
    assert_eq!(parsed.string_function_projections[1].literal_count, 2);
    assert_eq!(
        parsed.string_function_projections[2].op,
        StringFunctionOp::Left
    );
    assert_eq!(parsed.string_function_projections[2].literal_count, 1);
    assert_eq!(
        parsed.string_function_projections[3].op,
        StringFunctionOp::Right
    );
    assert_eq!(parsed.string_function_projections[3].literal_count, 1);
    assert_eq!(
        parsed.string_function_projections[4].op,
        StringFunctionOp::Replace
    );
    assert_eq!(parsed.string_function_projections[4].literal_count, 2);

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::StringFunctionCompare {
            op: StringFunctionOp::Concat,
            ref source_columns,
            literal_count: 2,
            value: ScalarValue::Utf8(ref value),
            ..
        } if source_columns == &vec!["label".to_string(), "segment".to_string()]
            && value == "alpha-north"
    ));
}

#[test]
fn parses_composed_string_expression_projection_and_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CONCAT(LOWER(TRIM(label)), '-', UPPER(segment)) AS label_key,LENGTH(REPLACE(TRIM(label), ' ', '')) AS compact_len FROM 'target/input.csv' WHERE CONCAT(LOWER(TRIM(label)), '-', UPPER(segment)) = 'alpha-north' AND LENGTH(REPLACE(TRIM(label), ' ', '')) >= 10 LIMIT 5",
    )
    .expect("composed string expression statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.string_function_projections.len(), 1);
    assert_eq!(parsed.string_function_projections[0].alias, "label_key");
    assert_eq!(
        parsed.string_function_projections[0].source_columns,
        vec!["label".to_string(), "segment".to_string()]
    );
    assert_eq!(parsed.string_function_projections[0].literal_count, 1);
    assert_eq!(parsed.string_length_projections.len(), 1);
    assert_eq!(parsed.string_length_projections[0].alias, "compact_len");
    assert_eq!(
        parsed.string_length_projections[0].source_columns,
        vec!["label".to_string()]
    );
}

#[test]
fn parses_scoped_temporal_extract_projection_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,DATE_YEAR(CAST(event_date AS date32)) AS event_year,TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) AS event_hour FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
    )
    .expect("temporal extract projection statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.date_extract_projections.len(), 1);
    assert_eq!(parsed.date_extract_projections[0].alias, "event_year");
    assert_eq!(parsed.date_extract_projections[0].column, "event_date");
    assert_eq!(parsed.date_extract_projections[0].op, DateExtractOp::Year);
    assert_eq!(parsed.timestamp_extract_projections.len(), 1);
    assert_eq!(parsed.timestamp_extract_projections[0].alias, "event_hour");
    assert_eq!(parsed.timestamp_extract_projections[0].column, "event_ts");
    assert_eq!(
        parsed.timestamp_extract_projections[0].op,
        TimestampExtractOp::Hour
    );
}

#[test]
fn null_coalesce_projection_null_fallback_is_admitted() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,COALESCE(label, NULL) AS label_clean FROM 'target/input.csv' LIMIT 5",
    )
    .expect("NULL coalesce declarations lower to the native expression binder");
    assert_eq!(parsed.generic_expression_projections.len(), 1);
    assert_eq!(
        parsed.generic_expression_projections[0].alias,
        "label_clean"
    );
}

#[test]
fn nullif_projection_null_sentinel_is_admitted() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,NULLIF(label, NULL) AS label_clean FROM 'target/input.csv' LIMIT 5",
    )
    .expect("NULL sentinel declarations lower to the native expression binder");
    assert_eq!(parsed.generic_expression_projections.len(), 1);
    assert_eq!(
        parsed.generic_expression_projections[0].alias,
        "label_clean"
    );
}

#[test]
fn conditional_projection_null_branch_is_admitted() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CASE WHEN amount >= 10 THEN NULL ELSE 'small' END AS size_band FROM 'target/input.csv' LIMIT 5",
    )
    .expect("NULL CASE branch declarations lower to the native expression binder");
    assert_eq!(parsed.generic_expression_projections.len(), 1);
    assert_eq!(parsed.generic_expression_projections[0].alias, "size_band");
}

#[test]
fn conditional_projection_mixed_branch_dtype_is_blocked() {
    let error = parse_sql_local_source_statement(
        "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 0 END AS size_band FROM 'target/input.csv' LIMIT 5",
    )
    .expect_err("mixed CASE branch dtypes are blocked during parsing");

    assert!(
        error
            .to_string()
            .contains("CASE projection THEN/ELSE branches must have matching dtypes"),
        "{error}"
    );
}

#[test]
fn numeric_arithmetic_divide_by_zero_remains_a_runtime_value_check() {
    parse_sql_local_source_statement(
        "SELECT id,amount / 0 AS ratio FROM 'target/input.csv' LIMIT 5",
    )
    .expect("projection syntax does not evaluate rows");
    parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE amount / 0 > 1 LIMIT 5",
    )
    .expect("predicate syntax does not evaluate rows");
}

#[test]
fn parses_scoped_scalar_aggregate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
    )
    .expect("aggregate statement parses");

    assert_eq!(parsed.projections, [] as [String; 0]);
    assert_eq!(parsed.aggregates.len(), 5);

    assert_eq!(parsed.aggregates[2].output_name(), "avg_amount");
    assert_eq!(parsed.group_by, [] as [String; 0]);
    assert!(parsed.order_by.is_none());
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/input.csv")
    );
}

#[test]
fn parses_scoped_aggregate_alias_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
    )
    .expect("aggregate alias statement parses");

    assert_eq!(parsed.projections, [] as [String; 0]);
    assert_eq!(parsed.aggregates.len(), 2);

    assert_eq!(parsed.aggregates[0].output_name(), "rows");
    assert_eq!(parsed.aggregates[0].alias.as_deref(), Some("rows"));

    assert_eq!(parsed.aggregates[1].output_name(), "total_amount");
    assert_eq!(parsed.group_by, [] as [String; 0]);
}

#[test]
fn parses_scoped_count_distinct_aggregate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(DISTINCT customer_id) AS unique_customers,count(*) AS rows FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region LIMIT 10",
    )
    .expect("count distinct aggregate statement parses");

    assert_eq!(parsed.projections, vec!["region"]);
    assert_eq!(parsed.group_by, vec!["region"]);
    assert_eq!(parsed.aggregates.len(), 2);

    assert_eq!(parsed.aggregates[0].output_name(), "unique_customers");
    assert!(parsed.aggregates[0].distinct);
}

#[test]
fn count_distinct_unsupported_shapes_are_blocked() {
    let sum_distinct = parse_sql_local_source_statement(
        "SELECT sum(DISTINCT amount) FROM 'target/input.csv' LIMIT 1",
    )
    .expect("SUM DISTINCT is admitted");
    assert!(sum_distinct.aggregates[0].distinct);
    assert_eq!(sum_distinct.aggregates[0].function, AggregateFunction::Sum);
    assert!(
        parse_sql_local_source_statement("SELECT var_pop(DISTINCT amount) FROM 'target/input.csv'")
            .is_err()
    );

    let count_distinct_star = parse_sql_local_source_statement(
        "SELECT count(DISTINCT *) FROM 'target/input.csv' LIMIT 1",
    )
    .expect_err("COUNT DISTINCT star is blocked");
    assert!(
        count_distinct_star
            .to_string()
            .contains("COUNT(DISTINCT *) is not admitted"),
        "{count_distinct_star}"
    );
}

#[test]
fn parses_scoped_group_by_aggregate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(*),sum(amount) FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region LIMIT 10",
    )
    .expect("group-by aggregate statement parses");

    assert_eq!(parsed.projections, vec!["region"]);
    assert_eq!(parsed.group_by, vec!["region"]);
    assert!(parsed.order_by.is_none());
    assert_eq!(parsed.aggregates.len(), 2);
}

#[test]
fn parses_scoped_aggregate_having_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region HAVING total_amount >= 10 AND rows >= 2 ORDER BY total_amount DESC LIMIT 10",
    )
    .expect("aggregate HAVING statement parses");
    assert_eq!(parsed.group_by, vec!["region"]);
    assert_eq!(parsed.aggregates.len(), 2);

    assert_eq!(parsed.having.columns(), vec!["total_amount", "rows"]);
}

#[test]
fn parses_scoped_having_exists_subquery_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(*) AS rows,sum(amount) AS total FROM 'target/input.csv' GROUP BY region HAVING EXISTS (SELECT * FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 1) ORDER BY total DESC LIMIT 10",
    )
    .expect("HAVING EXISTS subquery statement parses");

    assert_eq!(parsed.group_by, vec!["region"]);
    assert_eq!(parsed.aggregates.len(), 2);
}

#[test]
fn parses_scoped_having_quantified_subquery_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(*) AS rows,sum(amount) AS total FROM 'target/input.csv' GROUP BY region HAVING total > ALL (SELECT threshold FROM 'target/thresholds.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) ORDER BY total DESC LIMIT 10",
    )
    .expect("HAVING quantified subquery statement parses");

    assert_eq!(parsed.group_by, vec!["region"]);
    assert_eq!(parsed.aggregates.len(), 2);
}

#[test]
fn parses_scoped_having_unprojected_aggregate_functions() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(*) AS rows FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region HAVING sum(amount) >= 10 AND count(*) >= 2 AND count(DISTINCT id) >= 2 LIMIT 10",
    )
    .expect("aggregate HAVING statement with unprojected aggregate functions parses");
    assert_eq!(parsed.group_by, vec!["region"]);
    assert_eq!(parsed.aggregates.len(), 1);

    assert_eq!(parsed.aggregates[0].output_name(), "rows");
    assert_eq!(parsed.having_aggregates.len(), 2);
}

#[test]
fn having_hidden_aggregate_aliases_do_not_collide_with_visible_outputs() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,count(*) AS __having_sum_amount_1 FROM 'target/input.csv' GROUP BY region HAVING sum(amount) >= 10 LIMIT 10",
    )
    .expect("statement parses");

    assert_eq!(parsed.aggregates[0].output_name(), "__having_sum_amount_1");
    assert_eq!(parsed.having_aggregates.len(), 1);
    assert_eq!(
        parsed.having_aggregates[0].output_name(),
        "__having_sum_amount_2"
    );
}

#[test]
fn parses_scoped_multi_key_group_by_aggregate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT region,segment,count(*),sum(amount) FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region,segment LIMIT 10",
    )
    .expect("multi-key group-by aggregate statement parses");

    assert_eq!(parsed.projections, vec!["region", "segment"]);
    assert_eq!(parsed.group_by, vec!["region", "segment"]);
    assert!(parsed.order_by.is_none());
    assert_eq!(parsed.aggregates.len(), 2);
}

#[test]
fn parses_scoped_order_by_topn_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC LIMIT 3",
    )
    .expect("order-by statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
    assert_eq!(parsed.group_by, [] as [String; 0]);
    let order_by = parsed.order_by.as_ref().expect("order by parsed");

    assert_eq!(order_by.keys[0].column, "amount");
    assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
    assert_eq!(parsed.limit, 3);
}

#[test]
fn parses_scoped_explicit_null_order_by_topn_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' ORDER BY amount ASC NULLS FIRST,id DESC NULLS LAST LIMIT 4",
    )
    .expect("explicit null-order statement parses");

    let order_by = parsed.order_by.as_ref().expect("order by parsed");

    assert_eq!(
        order_by.keys[0].null_ordering,
        Some(SortNullOrdering::First)
    );
    assert_eq!(order_by.keys[1].null_ordering, Some(SortNullOrdering::Last));
}

#[test]
fn parses_scoped_multi_key_order_by_topn_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC,id ASC LIMIT 3",
    )
    .expect("multi-key order-by statement parses");

    let order_by = parsed.order_by.as_ref().expect("order by parsed");
    assert_eq!(
        order_by
            .keys
            .iter()
            .map(|key| (key.column.as_str(), key.direction))
            .collect::<Vec<_>>(),
        [("amount", SortDirection::Desc), ("id", SortDirection::Asc)]
    );

    assert_eq!(parsed.limit, 3);
}

#[test]
fn csv_scalar_inexact_exponent_decimals_fall_through_to_float_parser() {
    match parse_csv_scalar("1e-1") {
        ScalarValue::Float64(value) => assert!((value - 0.1).abs() < f64::EPSILON),
        other => panic!("expected 1e-1 to parse as Float64, got {other:?}"),
    }
    match parse_csv_scalar("1.5e0") {
        ScalarValue::Float64(value) => assert!((value - 1.5).abs() < f64::EPSILON),
        other => panic!("expected 1.5e0 to parse as Float64, got {other:?}"),
    }
    match parse_csv_scalar("2e0") {
        ScalarValue::Int64(value) => assert_eq!(value, 2),
        other => panic!("expected exact exponent integer to parse as Int64, got {other:?}"),
    }
}

#[test]
fn parses_scoped_cast_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount FROM 'target/input.jsonl' WHERE CAST(amount AS int64) >= 10 LIMIT 5",
    )
    .expect("cast predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "amount"]);
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/input.jsonl")
    );
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            target_dtype: LogicalDType::Int64,
            mode: CastMode::Strict,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Int64(10),
            ..
        } if column == "amount"
    ));
}

#[test]
fn parses_parenthesized_cast_predicate_expression_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CAST(raw_amount AS float64) AS amount_float FROM 'target/input.csv' WHERE (CAST(raw_amount AS float64)) >= 0 LIMIT 5",
    )
    .expect("parenthesized cast predicate expression statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            target_dtype: LogicalDType::Float64,
            mode: CastMode::Strict,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Int64(0),
            ..
        } if column == "raw_amount"
    ));
}

#[test]
fn parses_scoped_try_cast_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,raw_amount FROM 'target/input.csv' WHERE TRY_CAST(raw_amount AS int64) >= 10 LIMIT 5",
    )
    .expect("try_cast predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            target_dtype: LogicalDType::Int64,
            mode: CastMode::Try,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Int64(10),
            ..
        } if column == "raw_amount"
    ));
}

#[test]
fn parses_scoped_cast_date_literal_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,event_date FROM 'target/input.jsonl' WHERE CAST(event_date AS date32) >= DATE '2026-05-19' LIMIT 5",
    )
    .expect("cast date literal predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            target_dtype: LogicalDType::Date32,
            mode: CastMode::Strict,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Date32(_),
            ..
        } if column == "event_date"
    ));
}

#[test]
fn parses_scoped_binary_cast_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label_prefix,label_suffix FROM 'target/input.csv' WHERE CAST(CONCAT(label_prefix,label_suffix) AS binary) = BINARY 'alpha' LIMIT 5",
    )
    .expect("binary cast predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            ref source_columns,
            target_dtype: LogicalDType::Binary,
            mode: CastMode::Strict,
            op: ComparisonOp::Eq,
            value: ScalarValue::Binary(ref value),
            ..
        } if column == "label_prefix"
            && source_columns == &vec!["label_prefix".to_string(), "label_suffix".to_string()]
            && value == b"alpha"
    ));
}

#[test]
fn parses_scoped_decimal_cast_projection_and_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CAST(amount AS decimal128(10,2)) AS amount_decimal,TRY_CAST(raw_amount AS numeric(10,2)) AS raw_decimal FROM 'target/input.csv' WHERE CAST(amount AS decimal(10,2)) >= 10.00 LIMIT 5",
    )
    .expect("decimal cast projection and predicate statement parses");

    assert_eq!(parsed.projections, vec!["id"]);
    assert_eq!(parsed.cast_projections.len(), 2);
    assert_eq!(parsed.cast_projections[0].alias, "amount_decimal");
    assert_eq!(
        parsed.cast_projections[0].target_dtype,
        decimal128_dtype(10, 2)
    );
    assert_eq!(parsed.cast_projections[1].alias, "raw_decimal");
    assert_eq!(
        parsed.cast_projections[1].target_dtype,
        decimal128_dtype(10, 2)
    );
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            ref target_dtype,
            mode: CastMode::Strict,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Decimal128 {
                value: 1000,
                precision: 10,
                scale: 2
            },
            ..
        } if column == "amount" && target_dtype == &decimal128_dtype(10, 2)
    ));
}

#[test]
fn parses_scoped_binary_cast_ordering_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE CAST(LOWER(TRIM(label)) AS binary) > BINARY 'alpha' LIMIT 5",
    )
    .expect("binary cast ordering predicate parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::CastCompare {
            ref column,
            target_dtype: LogicalDType::Binary,
            mode: CastMode::Strict,
            op: ComparisonOp::Gt,
            value: ScalarValue::Binary(ref value),
            ..
        } if column == "label" && value == b"alpha"
    ));
}

#[test]
fn parses_scoped_string_length_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE LENGTH(label) >= 4 LIMIT 5",
    )
    .expect("string length predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::StringLengthCompare {
            ref source_columns,
            comparison: ComparisonOp::GtEq,
            value: ScalarValue::Int64(4),
            ..
        } if source_columns == &vec!["label".to_string()]
    ));
}

#[test]
fn parses_scoped_numeric_abs_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount FROM 'target/input.csv' WHERE ABS(amount) >= 4 LIMIT 5",
    )
    .expect("numeric abs predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "amount"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::NumericAbsCompare {
            ref column,
            comparison: ComparisonOp::GtEq,
            value: ScalarValue::Int64(4)
        } if column == "amount"
    ));
}

#[test]
fn parses_scoped_numeric_rounding_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount FROM 'target/input.csv' WHERE FLOOR(amount) >= 4 LIMIT 5",
    )
    .expect("numeric rounding predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "amount"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::NumericRoundingCompare {
            ref column,
            op: NumericRoundingOp::Floor,
            comparison: ComparisonOp::GtEq,
            value: ScalarValue::Int64(4)
        } if column == "amount"
    ));
}

#[test]
fn parses_generic_expression_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,amount FROM 'target/input.csv' WHERE (amount + tax) * 2 >= 40 AND ABS(amount - tax) > 8 LIMIT 5",
    )
    .expect("generic expression predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "amount"]);

    assert!(parsed.predicate.uses_generic_expression());

    assert_eq!(
        parsed.predicate.columns(),
        vec!["amount", "tax", "amount", "tax"]
    );
}

#[test]
fn parses_scoped_in_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label IN ('alpha','gamma') LIMIT 5",
    )
    .expect("IN predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/input.csv")
    );
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::InList {
            ref column,
            ref values,
        } if column == "label"
            && values == &vec![
                ScalarValue::Utf8("alpha".to_string()),
                ScalarValue::Utf8("gamma".to_string()),
            ]
    ));

    assert_eq!(parsed.predicate.columns(), vec!["label"]);
}

#[test]
fn parses_scoped_not_in_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label NOT IN ('alpha','gamma') LIMIT 5",
    )
    .expect("NOT IN predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Not { ref inner }
            if matches!(
                inner.as_ref(),
                ParsedPredicate::InList {
                    column,
                    values,
                } if column == "label"
                    && values == &vec![
                        ScalarValue::Utf8("alpha".to_string()),
                        ScalarValue::Utf8("gamma".to_string()),
                    ]
            )
    ));

    assert_eq!(parsed.predicate.columns(), vec!["label"]);
}

#[test]
fn parses_scoped_row_value_in_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE (id,label) IN ((1,'alpha'),(3,'gamma'),(5,NULL)) LIMIT 5",
    )
    .expect("row-value IN predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::RowValueInList {
            ref columns,
            ref tuples,
        } if columns == &vec!["id".to_string(), "label".to_string()]
            && tuples == &vec![
                vec![ScalarValue::Int64(1), ScalarValue::Utf8("alpha".to_string())],
                vec![ScalarValue::Int64(3), ScalarValue::Utf8("gamma".to_string())],
                vec![ScalarValue::Int64(5), ScalarValue::Null],
            ]
    ));

    assert_eq!(parsed.predicate.columns(), vec!["id", "label"]);
}

#[test]
fn parses_scoped_row_value_not_in_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE (id,label) NOT IN ((1,'alpha'),(3,'gamma')) LIMIT 5",
    )
    .expect("row-value NOT IN predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Not { ref inner }
            if matches!(
                inner.as_ref(),
                ParsedPredicate::RowValueInList {
                    columns,
                    tuples,
                } if columns == &vec!["id".to_string(), "label".to_string()]
                    && tuples.len() == 2
            )
    ));
}

#[test]
fn parses_scoped_date_in_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,event_date FROM 'target/input.csv' WHERE event_date IN (DATE '2026-05-18', DATE '2026-05-20') LIMIT 5",
    )
    .expect("DATE IN predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::InList {
            ref column,
            ref values,
        } if column == "event_date"
            && values.len() == 2
            && values.iter().all(|value| matches!(value, ScalarValue::Date32(_)))
    ));
}

#[test]
fn parses_scoped_in_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv') LIMIT 5",
    )
    .expect("IN subquery predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::InSubquery {
            ref column,
            ref subquery
        } if column == "id"
            && subquery.source_column == "id"
            && subquery.source.local_path().unwrap() == Path::new("target/allowed.csv")
            && subquery.values.is_empty()
    ));
}

#[test]
fn parses_filtered_ordered_limited_in_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) LIMIT 5",
    )
    .expect("filtered ordered IN subquery predicate statement parses");
    let ParsedPredicate::InSubquery { subquery, .. } = &parsed.predicate else {
        panic!("expected membership subquery");
    };
    assert_eq!(subquery.limit, Some(2));
    assert!(!matches!(*subquery.predicate, ParsedPredicate::All));
    assert_eq!(subquery.order_by.as_ref().unwrap().keys[0].column, "score");
}

#[test]
fn parses_nested_local_in_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT allowed_id FROM 'target/allowed.csv' WHERE allowed_id IN (SELECT id FROM 'target/nested.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) ORDER BY priority DESC LIMIT 3) LIMIT 5",
    )
    .expect("nested local IN subquery predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::InSubquery {
            ref column,
            ref subquery
        } if column == "id"
            && subquery.source_column == "allowed_id"
            && subquery.source.local_path().unwrap() == Path::new("target/allowed.csv")
            && matches!(
                subquery.predicate.as_ref(),
                ParsedPredicate::InSubquery {
                    column,
                    subquery
                } if column == "allowed_id"
                    && subquery.source_column == "id"
                    && subquery.source.local_path().unwrap() == Path::new("target/nested.csv")
            )
    ));
}

#[test]
fn parses_scoped_quantified_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= ALL (SELECT threshold FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) LIMIT 5",
    )
    .expect("quantified subquery predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
}

#[test]
fn parses_scoped_exists_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE EXISTS (SELECT * FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 1) LIMIT 5",
    )
    .expect("EXISTS subquery predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
}

#[test]
fn parses_scoped_not_exists_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE NOT EXISTS (SELECT 1 FROM 'target/blocked.csv' WHERE active IS TRUE LIMIT 1) LIMIT 5",
    )
    .expect("NOT EXISTS subquery predicate statement parses");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Not { ref inner }
            if matches!(
                inner.as_ref(),
                ParsedPredicate::ExistsSubquery { subquery }
                    if subquery.projection_kind == ParsedExistsSubqueryProjectionKind::Literal
                        && subquery.source.local_path().unwrap() == Path::new("target/blocked.csv")
                        && subquery.limit == Some(1)
            )
    ));
}

#[test]
fn parses_scoped_row_value_in_subquery_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE (id,label) IN (SELECT allowed_id,allowed_label FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 3) LIMIT 5",
    )
    .expect("row-value IN subquery predicate statement parses");

    assert_eq!(parsed.projections, vec!["id", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::RowValueInSubquery {
            ref columns,
            ref subquery
        } if columns == &vec!["id".to_string(), "label".to_string()]
            && subquery.source_columns
                == vec!["allowed_id".to_string(), "allowed_label".to_string()]
            && subquery.source.local_path().unwrap() == Path::new("target/allowed.csv")
            && subquery.tuples.is_empty()
    ));
}

#[test]
fn in_subquery_admits_source_qualifier_from_file_stem() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT allowed.id FROM 'target/allowed.csv' WHERE allowed.active IS TRUE ORDER BY allowed.id DESC LIMIT 5) LIMIT 5",
    )
    .expect("file-stem source qualifier is admitted");

    match parsed.predicate {
        ParsedPredicate::InSubquery { subquery, .. } => {
            assert_eq!(subquery.source_column, "id");
            assert!(matches!(
                subquery.predicate.as_ref(),
                ParsedPredicate::BooleanPredicate { column, expected: true, .. }
                if column == "active"
            ));
            let order_by = subquery.order_by.expect("source-qualified order by");
            assert_eq!(order_by.keys[0].column, "id");
            assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
        }
        other => panic!("expected source-qualified IN subquery, got {other:?}"),
    }
}

#[test]
fn in_subquery_rejects_reserved_outer_file_stem_qualifier_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT outer.id FROM 'target/outer.csv' WHERE outer.active IS TRUE LIMIT 5) LIMIT 5",
    )
    .expect_err("reserved inferred outer qualifier remains blocked");

    assert!(
        error
            .to_string()
            .contains("inferred source qualifier 'outer' is reserved"),
        "got {error}"
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn in_predicate_blocks_unadmitted_literal_lists_without_fallback() {
    let empty_error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE label IN () LIMIT 5",
    )
    .expect_err("empty IN list remains blocked");
    assert!(
        empty_error
            .to_string()
            .contains("IN predicates require at least one literal value")
    );
    assert!(
        empty_error
            .to_string()
            .contains("external_engine_invoked=false")
    );

    let null_admitted = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE label IN ('alpha', NULL) LIMIT 5",
    )
    .expect("NULL IN list values use SQL three-valued semantics");

    assert!(matches!(
        null_admitted.predicate,
        ParsedPredicate::InList {
            ref column,
            ref values,
        } if column == "label"
            && values == &vec![
                ScalarValue::Utf8("alpha".to_string()),
                ScalarValue::Null,
            ]
    ));

    let mixed_date_error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE label IN (DATE '2026-05-19', 'alpha') LIMIT 5",
    )
    .expect_err("mixed DATE/non-DATE IN lists remain blocked");
    assert!(
        mixed_date_error
            .to_string()
            .contains("IN predicates do not admit mixed DATE and non-DATE literal lists")
    );
    assert!(
        mixed_date_error
            .to_string()
            .contains("external_engine_invoked=false")
    );

    let trailing_error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE label IN ('alpha',) LIMIT 5",
    )
    .expect_err("trailing empty IN list values remain blocked");
    assert!(
        trailing_error
            .to_string()
            .contains("IN predicates require non-empty literal values")
    );
    assert!(
        trailing_error
            .to_string()
            .contains("external_engine_invoked=false")
    );
}

#[test]
fn in_subquery_blocks_unadmitted_advanced_shapes_without_fallback() {
    parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE id > ANY (SELECT id FROM 'target/allowed.csv' WHERE id IN (SELECT id FROM 'target/nested.csv')) LIMIT 5",
    )
    .expect("quantified nested IN subquery predicate is admitted");

    for (statement, expected) in [
        (
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id,label FROM 'target/allowed.csv') LIMIT 5",
            "multi-column IN subqueries require row-value source columns",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE (id,label) IN (SELECT id FROM 'target/allowed.csv') LIMIT 5",
            "row-value IN subquery selected-column arity must match the source column count",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' AS allowed WHERE blocked.id = id) LIMIT 5",
            "qualified IN subquery predicates admit only outer.<column> references or the subquery source qualifier",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' GROUP BY id HAVING outer.amount > 10 LIMIT 5) LIMIT 5",
            "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("unsupported advanced subquery shape remains blocked");
        assert!(
            error.to_string().contains(expected),
            "error {error:?} did not contain {expected:?}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn correlated_subquery_outer_references_outside_column_comparisons_block_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' WHERE outer.amount > 10) LIMIT 5",
            "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' WHERE outer.id = outer.amount) LIMIT 5",
            "correlated IN subquery predicates require exactly one outer.<column> reference per column comparison",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE EXISTS (SELECT * FROM 'target/allowed.csv' WHERE outer.amount IS NOT NULL LIMIT 1) LIMIT 5",
            "correlated EXISTS subquery predicates admit outer.<column> references only in column-to-column comparisons",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE (id,label) IN (SELECT id,label FROM 'target/allowed.csv' WHERE outer.amount > 10) LIMIT 5",
            "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE amount > ALL (SELECT min_amount FROM 'target/thresholds.csv' WHERE outer.amount > 10 LIMIT 5) LIMIT 5",
            "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/grouped.csv' GROUP BY id HAVING outer.amount > 10 LIMIT 5) LIMIT 5",
            "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("outer correlation outside admitted column comparisons remains blocked");
        assert!(
            error.to_string().contains(expected),
            "error {error:?} did not contain {expected:?}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn row_value_in_predicate_blocks_malformed_literal_tuples_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id FROM 'target/input.csv' WHERE (id,label) IN () LIMIT 5",
            "row-value IN predicates require at least one literal tuple",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE (id,label) IN ((1,'alpha'),) LIMIT 5",
            "row-value IN predicates require non-empty literal tuples",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE (id,label) IN ((1,'alpha',10)) LIMIT 5",
            "row-value IN literal tuple arity must match the source column count",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE (id,label) IN (1,'alpha') LIMIT 5",
            "row-value IN literal values must be parenthesized tuples",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE (event_date,label) IN ((DATE '2026-05-19','alpha'),('2026-05-20','beta')) LIMIT 5",
            "row-value IN predicates do not admit mixed DATE and non-DATE literals",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("malformed row-value IN shape remains blocked");
        assert!(
            error.to_string().contains(expected),
            "error {error:?} did not contain {expected:?}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn parses_scoped_logical_and_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND label LIKE '%ta' LIMIT 5",
    )
    .expect("logical AND statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["amount", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::And,
            ..
        }
    ));
}

#[test]
fn parses_scoped_logical_or_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 OR label LIKE '%ta' LIMIT 5",
    )
    .expect("logical OR statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["amount", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::Or,
            ..
        }
    ));
}

#[test]
fn logical_or_preserves_and_precedence_without_fallback() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE id >= 1 OR amount >= 10 AND label LIKE '%ta' LIMIT 5",
    )
    .expect("logical OR/AND statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["id", "amount", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::Or,
            ..
        }
    ));
}

#[test]
fn parses_scoped_logical_not_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE NOT label LIKE '%ta' LIMIT 5",
    )
    .expect("logical NOT statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["label"]);
    assert!(matches!(parsed.predicate, ParsedPredicate::Not { .. }));
}

#[test]
fn logical_not_preserves_or_precedence_without_fallback() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE NOT id >= 1 OR amount >= 10 LIMIT 5",
    )
    .expect("logical NOT/OR statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["id", "amount"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::Or,
            ..
        }
    ));
}

#[test]
fn parses_parenthesized_scoped_logical_predicate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%') LIMIT 5",
    )
    .expect("parenthesized logical statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["amount", "label", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::And,
            right,
            ..
        } if matches!(
            *right,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::Or,
                ..
            }
        )
    ));
}

#[test]
fn parenthesized_logical_predicates_override_default_precedence_without_fallback() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE (id >= 1 OR amount >= 10) AND label LIKE '%ta' LIMIT 5",
    )
    .expect("parenthesized logical statement parses");

    assert_eq!(parsed.predicate.columns(), vec!["id", "amount", "label"]);
    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::And,
            left,
            ..
        } if matches!(
            *left,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::Or,
                ..
            }
        )
    ));
}

#[test]
fn parser_blocks_unbalanced_predicate_parentheses_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE (id >= 1 OR amount >= 10 LIMIT 5",
    )
    .expect_err("unbalanced predicate parentheses remain blocked");

    assert!(error.to_string().contains("parentheses must be balanced"));
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn cast_predicate_blocks_unadmitted_dtype() {
    let error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.jsonl' WHERE CAST(amount AS decimal128(39,2)) >= 10 LIMIT 5",
    )
    .expect_err("invalid decimal cast target remains blocked");

    assert!(
        error
            .to_string()
            .contains("decimal CAST precision/scale must satisfy 1 <= precision <= 38")
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parser_blocks_advanced_scalar_policy_constructs_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id,TIMESTAMP '2026-05-19T12:34:56Z' AT TIME ZONE 'America/Chicago' AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,TIMEZONE('America/Chicago', event_ts) AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,CONVERT_TIMEZONE('UTC','America/Chicago',event_ts) AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,CAST(event_ts AS TIMESTAMPTZ) AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,TIMESTAMPTZ '2026-05-19 12:34:56 America/Chicago' AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,CAST(event_ts AS TIMESTAMP_TZ) AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,CAST(event_ts AS TIMESTAMP WITH LOCAL TIME ZONE) AS local_ts FROM 'target/input.csv' LIMIT 5",
            "timezone database semantics are not admitted",
        ),
        (
            "SELECT id,label COLLATE nocase AS folded FROM 'target/input.csv' LIMIT 5",
            "SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE label ILIKE 'a%' LIMIT 5",
            "SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
        ),
        (
            "SELECT id,TRY_CAST(label AS decimal128(39,2)) AS unsupported FROM 'target/input.csv' LIMIT 5",
            "decimal CAST precision/scale must satisfy 1 <= precision <= 38",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("advanced scalar policy construct remains blocked");

        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    let parsed = parse_sql_local_source_statement(
        "SELECT id,timezone FROM 'target/input.csv' WHERE timezone = 'UTC' LIMIT 5",
    )
    .expect("timezone column name is not a timezone database function");
    assert_eq!(parsed.projections, vec!["id", "timezone"]);

    let parsed = parse_sql_local_source_statement(
        "SELECT id,timestamptz,timestamp_tz FROM 'target/input.csv' WHERE timestamptz = 'UTC' LIMIT 5",
    )
    .expect("timezone dtype spellings are only blocked as type syntax");
    assert_eq!(
        parsed.projections,
        vec!["id", "timestamptz", "timestamp_tz"]
    );
}

#[test]
fn parser_blocks_unscoped_interval_literal_shapes_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id,event_date + INTERVAL '1' DAY AS next_day FROM 'target/input.csv' LIMIT 5",
            "arbitrary ANSI INTERVAL arithmetic is not admitted",
        ),
        (
            "SELECT id FROM 'target/input.csv' WHERE event_ts >= TIMESTAMP '2026-05-19T12:00:00Z' - INTERVAL '1' HOUR LIMIT 5",
            "arbitrary ANSI INTERVAL arithmetic is not admitted",
        ),
        (
            "SELECT id,DATE_ADD_DAYS(event_date, INTERVAL '1' HOUR) AS next_hour FROM 'target/input.csv' LIMIT 5",
            "date arithmetic interval literals admit DAY units only",
        ),
        (
            "SELECT id,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1.5' SECOND) AS shifted FROM 'target/input.csv' LIMIT 5",
            "ANSI INTERVAL literal value must be a signed integer string literal",
        ),
        (
            "SELECT id,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' MONTH) AS shifted FROM 'target/input.csv' LIMIT 5",
            "ANSI INTERVAL literals in scoped temporal arithmetic admit DAY, HOUR, MINUTE, or SECOND units only",
        ),
        (
            "SELECT id,DATE_ADD_DAYS(event_date, intervaé) AS shifted FROM 'target/input.csv' LIMIT 5",
            "SQL identifiers may contain only ASCII letters, numbers, and underscores",
        ),
    ] {
        let error =
            parse_sql_local_source_statement(statement).expect_err("interval shape is blocked");

        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    let parsed = parse_sql_local_source_statement(
        "SELECT id,interval FROM 'target/input.csv' WHERE interval = 1 LIMIT 5",
    )
    .expect("interval is allowed as an ordinary column name");
    assert_eq!(parsed.projections, vec!["id", "interval"]);
}

#[test]
fn parser_allows_regex_named_columns_without_treating_identifiers_as_regex() {
    let parsed = parse_sql_local_source_statement(
        "SELECT regex, regexp FROM 'target/input.csv' WHERE regex = 'alpha' LIMIT 5",
    )
    .expect("regex-like identifiers are ordinary columns");

    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/input.csv")
    );
    assert_eq!(parsed.limit, 5);
}

#[test]
fn parser_admits_scoped_like_wildcard_predicates_without_fallback() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a%a' LIMIT 5",
    )
    .expect("scoped LIKE multi-wildcard predicate is admitted");
    match parsed.predicate {
        ParsedPredicate::StringMatch {
            column, op, value, ..
        } => {
            assert_eq!(column, "label");
            assert_eq!(op, StringPredicateOp::LikePattern);
            assert_eq!(value, r"\Aa(?s:.*)a\z");
        }
        other => panic!("expected LIKE StringMatch predicate, got {other:?}"),
    }

    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label LIKE '_l%' LIMIT 5",
    )
    .expect("scoped LIKE single-character wildcard predicate is admitted");
    match parsed.predicate {
        ParsedPredicate::StringMatch {
            column, op, value, ..
        } => {
            assert_eq!(column, "label");
            assert_eq!(op, StringPredicateOp::LikePattern);
            assert_eq!(value, r"\A(?s:.)l(?s:.*)\z");
        }
        other => panic!("expected LIKE StringMatch predicate, got {other:?}"),
    }

    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label NOT LIKE 'a_p%' LIMIT 5",
    )
    .expect("scoped NOT LIKE wildcard predicate is admitted");
    match parsed.predicate {
        ParsedPredicate::Not { inner } => match *inner {
            ParsedPredicate::StringMatch {
                column, op, value, ..
            } => {
                assert_eq!(column, "label");
                assert_eq!(op, StringPredicateOp::LikePattern);
                assert_eq!(value, r"\Aa(?s:.)p(?s:.*)\z");
            }
            other => panic!("expected inner LIKE StringMatch predicate, got {other:?}"),
        },
        other => panic!("expected negated LIKE predicate, got {other:?}"),
    }
}

#[test]
fn parser_admits_scoped_like_escape_predicates_without_fallback() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!_%' ESCAPE '!' LIMIT 5",
    )
    .expect("scoped LIKE ESCAPE predicate is admitted");
    match parsed.predicate {
        ParsedPredicate::StringMatch {
            column,
            op,
            value,
            like_escape,
        } => {
            assert_eq!(column, "label");
            assert_eq!(op, StringPredicateOp::LikePattern);
            assert_eq!(value, r"\Aa_(?s:.*)\z");
            assert_eq!(like_escape, Some('!'));
        }
        other => panic!("expected LIKE ESCAPE StringMatch predicate, got {other:?}"),
    }

    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label NOT LIKE 'a!_%' ESCAPE '!' LIMIT 5",
    )
    .expect("scoped NOT LIKE ESCAPE predicate is admitted");
    match parsed.predicate {
        ParsedPredicate::Not { inner } => match *inner {
            ParsedPredicate::StringMatch {
                column,
                op,
                value,
                like_escape,
            } => {
                assert_eq!(column, "label");
                assert_eq!(op, StringPredicateOp::LikePattern);
                assert_eq!(value, r"\Aa_(?s:.*)\z");
                assert_eq!(like_escape, Some('!'));
            }
            other => panic!("expected inner LIKE ESCAPE StringMatch predicate, got {other:?}"),
        },
        other => panic!("expected negated LIKE ESCAPE predicate, got {other:?}"),
    }
}

#[test]
fn parser_blocks_malformed_like_escape_clauses_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!_%' ESCAPE '' LIMIT 5",
            "LIKE ESCAPE clause requires a single-character string literal",
        ),
        (
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!_%' ESCAPE '!!' LIMIT 5",
            "LIKE ESCAPE clause requires a single-character string literal",
        ),
        (
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!' ESCAPE '!' LIMIT 5",
            "LIKE ESCAPE pattern cannot end with the escape character",
        ),
        (
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!x%' ESCAPE '!' LIMIT 5",
            "LIKE ESCAPE may only escape %, _, or the escape character",
        ),
    ] {
        let error =
            parse_sql_local_source_statement(statement).expect_err("malformed ESCAPE blocks");

        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn parser_admits_scoped_regex_predicates_without_fallback() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label RLIKE '^(alpha|gamma)$' LIMIT 5",
    )
    .expect("scoped RLIKE predicate is admitted");
    match parsed.predicate {
        ParsedPredicate::StringMatch {
            column, op, value, ..
        } => {
            assert_eq!(column, "label");
            assert_eq!(op, StringPredicateOp::RegexMatch);
            assert_eq!(value, "^(alpha|gamma)$");
        }
        other => panic!("expected regex StringMatch predicate, got {other:?}"),
    }

    let parsed = parse_sql_local_source_statement(
        "SELECT id,REGEXP_LIKE(label, '^a') AS matched FROM 'target/input.csv' LIMIT 5",
    )
    .expect("scoped REGEXP_LIKE predicate projection is admitted");
    assert_eq!(parsed.predicate_projections.len(), 1);
    assert_eq!(parsed.predicate_projections[0].alias, "matched");
    match &parsed.predicate_projections[0].predicate {
        ParsedPredicate::StringMatch {
            column, op, value, ..
        } => {
            assert_eq!(column, "label");
            assert_eq!(*op, StringPredicateOp::RegexMatch);
            assert_eq!(value, "^a");
        }
        other => panic!("expected regex predicate projection, got {other:?}"),
    }
}

#[test]
fn parser_blocks_invalid_regex_patterns_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id,label FROM 'target/input.csv' WHERE label REGEXP '[' LIMIT 5",
    )
    .expect_err("invalid regex pattern remains blocked");

    assert!(error.to_string().contains("regex pattern is invalid"));
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parser_allows_decimal_policy_words_outside_cast_targets() {
    let parsed = parse_sql_local_source_statement(
        "SELECT id,CAST(amount AS float64) AS numeric FROM 'target/input.csv' LIMIT 5",
    )
    .expect("numeric alias is not a decimal cast target");

    assert_eq!(parsed.cast_projections[0].alias, "numeric");
    assert_eq!(
        parsed.cast_projections[0].target_dtype,
        LogicalDType::Float64
    );
}

#[test]
fn parser_blocks_unadmitted_complex_dtype_policy_constructs_without_fallback() {
    for (statement, expected) in [
        (
            "SELECT id,ARRAY(1,2) AS values FROM 'target/input.csv' LIMIT 5",
            "list and array accessors, function constructors, casts, and equality semantics are not admitted",
        ),
        (
            "SELECT id,LIST_EXTRACT(payload, 1) AS item FROM 'target/input.csv' LIMIT 5",
            "list and array accessors, function constructors, casts, and equality semantics are not admitted",
        ),
        (
            "SELECT CAST(payload AS list) AS payload FROM 'target/input.csv' LIMIT 5",
            "list and array accessors, function constructors, casts, and equality semantics are not admitted",
        ),
        (
            "SELECT id,ROW(label, amount) AS payload FROM 'target/input.csv' LIMIT 5",
            "row constructors plus struct casts, equality, and access semantics are not admitted",
        ),
        (
            "SELECT TRY_CAST(payload AS struct) AS payload FROM 'target/input.csv' LIMIT 5",
            "row constructors plus struct casts, equality, and access semantics are not admitted",
        ),
        (
            "SELECT CAST(payload AS variant) AS payload FROM 'target/input.csv' LIMIT 5",
            "Variant constructors and casts are not admitted",
        ),
        (
            "SELECT CAST(payload AS union) AS payload FROM 'target/input.csv' LIMIT 5",
            "union dtype casts are not admitted",
        ),
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("complex dtype policy construct remains blocked");

        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn parser_variant_field_functions_reach_native_binding() {
    for expression in [
        "VARIANT_GET(payload, '$.field')",
        "TRY_VARIANT_GET(payload, '$.field', 'int64')",
        "IS_VARIANT_NULL(VARIANT_GET(payload, '$[0]'))",
    ] {
        validate_complex_dtype_policy_boundaries_with_sql_union(
            &format!("SELECT {expression} FROM 'input.vortex'"),
            false,
        )
        .expect("field functions are admitted by the shared native binder");
    }
}

#[test]
fn parser_blocks_complex_subquery_membership_materialization_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT ARRAY[1] AS value_list FROM 'target/input.csv') LIMIT 5",
    )
    .expect_err("complex projected subquery membership remains blocked");

    assert!(
        error.to_string().contains(
            "projected subqueries do not admit ARRAY or STRUCT projection outputs for membership materialization"
        ),
        "unexpected error: {error}"
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parses_scoped_sql_union_all_statement() {
    let parsed = parse_sql_local_source_union_statement(
        "SELECT id,label FROM 'target/left.csv' WHERE amount >= 10 UNION ALL SELECT id,label FROM 'target/right.csv' WHERE amount >= 20 ORDER BY id DESC LIMIT 3",
    )
    .expect("scoped union statement parses");

    assert_eq!(parsed.mode, SqlUnionMode::All);
    assert_eq!(parsed.branches.len(), 2);
    assert_eq!(parsed.branch_statements.len(), 2);
    assert!(parsed.branch_statements[0].ends_with(&format!("LIMIT {}", usize::MAX)));
    assert_eq!(parsed.limit, 3);
    let order_by = parsed.order_by.expect("global order by parsed");
    assert_eq!(order_by.keys[0].column, "id");
    assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
}

#[test]
fn parses_scoped_sql_intersect_and_except_statements() {
    let intersect = parse_sql_local_source_union_statement(
        "SELECT id,label FROM 'target/left.csv' INTERSECT DISTINCT SELECT id,label FROM 'target/right.csv' ORDER BY id ASC LIMIT 3",
    )
    .expect("scoped intersect statement parses");
    assert_eq!(intersect.mode, SqlUnionMode::IntersectDistinct);
    assert_eq!(intersect.branches.len(), 2);
    assert_eq!(intersect.limit, 3);
    let order_by = intersect.order_by.expect("global order by parsed");
    assert_eq!(order_by.keys[0].column, "id");
    assert_eq!(order_by.keys[0].direction, SortDirection::Asc);

    let except = parse_sql_local_source_union_statement(
        "SELECT id,label FROM 'target/left.csv' EXCEPT SELECT id,label FROM 'target/right.csv' LIMIT 5",
    )
    .expect("scoped except statement parses");
    assert_eq!(except.mode, SqlUnionMode::ExceptDistinct);
    assert_eq!(except.branches.len(), 2);
    assert_eq!(except.limit, 5);
    assert!(except.order_by.is_none());
}

#[test]
fn parser_blocks_common_table_expressions_without_fallback() {
    for statement in [
        "WITH recent AS (SELECT id FROM 'target/input.csv' LIMIT 5) SELECT id FROM recent LIMIT 5",
        "WITH RECURSIVE r AS (SELECT id FROM 'target/input.csv' LIMIT 5) SELECT id FROM r LIMIT 5",
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("CTE syntax remains a deterministic parser blocker");
        assert!(
            error
                .to_string()
                .contains("SQL common table expressions (WITH/RECURSIVE) are not admitted"),
            "got {error}"
        );
        assert!(error.to_string().contains("cte_plan_nodes"));
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    let error = parse_sql_local_source_union_statement(
        "WITH recent AS (SELECT id FROM 'target/input.csv' LIMIT 5) SELECT id FROM recent UNION SELECT id FROM 'target/other.csv' LIMIT 5",
    )
    .expect_err("CTE set-operation syntax remains blocked");
    assert!(
        error
            .to_string()
            .contains("SQL common table expressions (WITH/RECURSIVE) are not admitted"),
        "got {error}"
    );
    assert!(
        error
            .to_string()
            .contains("no fallback execution was attempted")
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parser_blocks_intersect_all_and_except_all_without_fallback() {
    for statement in [
        "SELECT id FROM 'target/left.csv' INTERSECT ALL SELECT id FROM 'target/right.csv' LIMIT 5",
        "SELECT id FROM 'target/left.csv' EXCEPT ALL SELECT id FROM 'target/right.csv' LIMIT 5",
    ] {
        let error = parse_sql_local_source_union_statement(statement)
            .expect_err("ALL set operation remains blocked");
        assert!(
            error
                .to_string()
                .contains("INTERSECT ALL and EXCEPT ALL are not admitted"),
            "got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn parser_blocks_union_branch_local_limit_without_fallback() {
    let error = parse_sql_local_source_union_statement(
        "SELECT id FROM 'target/left.csv' LIMIT 2 UNION SELECT id FROM 'target/right.csv' LIMIT 5",
    )
    .expect_err("branch-local limit remains blocked");

    assert!(
        error
            .to_string()
            .contains("SQL set-operation branch-local LIMIT clauses are not admitted"),
        "got {error}"
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn timestamp_literal_blocks_named_timezones_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT id,TIMESTAMP '2026-05-19T12:34:56Z' AT TIME ZONE 'America/Chicago' AS local_ts FROM 'target/input.csv' LIMIT 5",
    )
    .expect_err("named timezone semantics remain blocked");

    assert!(
        error
            .to_string()
            .contains("timezone database semantics are not admitted")
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parses_scoped_inner_equi_join_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 3",
    )
    .expect("join statement parses");

    assert_eq!(parsed.projections, vec!["f.id", "d.segment"]);
    assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
    assert_eq!(parsed.group_by, [] as [String; 0]);
    assert!(parsed.order_by.is_none());
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/fact.csv")
    );
    assert_eq!(parsed.source_alias.as_deref(), Some("f"));
    let join = parsed.join.as_ref().expect("join parsed");
    assert_eq!(
        join.right_source.local_path().unwrap(),
        Path::new("target/dim.csv")
    );
    assert_eq!(join.right_alias, "d");

    assert!(matches!(
        parsed.predicate,
        ParsedPredicate::Compare {
            ref column,
            op: ComparisonOp::GtEq,
            value: ScalarValue::Int64(10)
        } if column == "f.amount"
    ));
}

#[test]
fn parses_scoped_multi_key_inner_equi_join_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 3",
    )
    .expect("multi-key join statement parses");

    let join = parsed.join.as_ref().expect("join parsed");
    assert_eq!(join.key_pairs.len(), 2);
    assert_eq!(join.join_type, ParsedJoinType::InnerEqui);
    assert_eq!(join.key_pairs[0].left.column, "customer_id");
    assert_eq!(join.key_pairs[1].right.column, "region");
}

#[test]
fn parses_scoped_join_computed_projection_topn_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT f.id,d.segment,f.amount + d.discount AS adjusted,CONCAT(d.segment,'-',f.region) AS segment_region FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 3",
    )
    .expect("join computed projection top-N statement parses");

    assert_eq!(parsed.projections, vec!["f.id", "d.segment"]);
    assert_eq!(parsed.generic_expression_projections.len(), 1);
    assert_eq!(
        parsed.generic_expression_projections[0].source_columns,
        vec!["d.discount", "f.amount"]
    );

    assert_eq!(parsed.string_function_projections.len(), 1);

    let order_by = parsed.order_by.as_ref().expect("order by parsed");

    assert_eq!(order_by.keys[0].column, "f.amount");
    assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
}

#[test]
fn parses_scoped_join_scalar_expression_on_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.amount + d.discount >= 25 LIMIT 5",
    )
    .expect("scalar expression join statement parses");

    let join = parsed.join.as_ref().expect("join parsed");
    assert_eq!(join.key_pairs, [] as [ParsedJoinKeyPair; 0]);
    assert_eq!(
        join.on_predicate_family,
        ParsedJoinOnPredicateFamily::GenericExpression
    );
    assert!(join.on_predicate.is_some());
}

#[test]
fn parses_scoped_join_logical_or_on_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id OR f.region = d.region LIMIT 5",
    )
    .expect("logical OR join statement parses");

    let join = parsed.join.as_ref().expect("join parsed");

    assert!(matches!(
        join.on_predicate.as_ref().expect("ON predicate"),
        ParsedPredicate::Logical {
            op: LogicalPredicateOp::Or,
            ..
        }
    ));
}

#[test]
fn parser_blocks_logical_or_on_non_inner_join_without_fallback() {
    let error = parse_sql_local_source_statement(
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f LEFT JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id OR f.region = d.region LIMIT 5",
    )
    .expect_err("outer OR join remains blocked");

    assert!(
        error
            .to_string()
            .contains("logical OR JOIN ON predicates are admitted only for INNER JOIN"),
        "got {error}"
    );
    assert!(error.to_string().contains("external_engine_invoked=false"));
}

#[test]
fn parses_scoped_join_group_by_aggregate_statement() {
    let parsed = parse_sql_local_source_statement(
        "SELECT d.segment,sum(f.amount) AS total_amount,count(*) AS rows FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 GROUP BY d.segment LIMIT 10",
    )
    .expect("join group-by aggregate statement parses");

    assert_eq!(parsed.projections, vec!["d.segment"]);
    assert_eq!(parsed.group_by, vec!["d.segment"]);
    assert!(parsed.order_by.is_none());
    assert_eq!(
        parsed.source.local_path().unwrap(),
        Path::new("target/fact.csv")
    );
    assert_eq!(parsed.source_alias.as_deref(), Some("f"));
    let join = parsed.join.as_ref().expect("join parsed");
    assert_eq!(
        join.right_source.local_path().unwrap(),
        Path::new("target/dim.csv")
    );
    assert_eq!(join.right_alias, "d");

    assert_eq!(parsed.aggregates.len(), 2);

    assert_eq!(parsed.aggregates[0].output_name(), "total_amount");
    assert_eq!(parsed.aggregates[0].column(), Some("f.amount"));

    assert_eq!(parsed.aggregates[1].output_name(), "rows");
}

#[test]
fn join_parser_blocks_complex_on_keys_without_fallback() {
    for statement in [
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON ARRAY[f.customer_id] = ARRAY[d.customer_id] LIMIT 5",
        "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON STRUCT(f.customer_id, f.region) = STRUCT(d.customer_id, d.region) LIMIT 5",
    ] {
        let error = parse_sql_local_source_statement(statement)
            .expect_err("complex join ON keys remain blocked");

        assert!(
            error
                .to_string()
                .contains("JOIN ON complex key expressions are not admitted"),
            "unexpected error for {statement}: {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }
}

#[test]
fn parser_blocks_unbounded_or_remote_sql() {
    assert!(parse_sql_local_source_statement("SELECT id FROM 'target/input.csv'").is_err());
    assert!(
        parse_sql_local_source_statement(
            "SELECT id FROM 's3://bucket/input.csv' WHERE id = 1 LIMIT 5"
        )
        .is_ok(),
        "URI blocking happens at source admission"
    );
    assert!(reject_remote_source_path(Path::new("s3://bucket/input.csv")).is_err());
}

#[test]
fn csv_parser_handles_basic_quoted_fields() {
    let row = split_csv_record("id,label").expect("record parses");
    assert_eq!(row, vec!["id", "label"]);
    let row = split_csv_record("1,\"hello, world\"").expect("record parses");
    assert_eq!(row, vec!["1", "hello, world"]);
}

#[test]
fn csv_source_read_plan_materializes_required_columns_only() {
    let plan = LocalSourceReadPlan::required(
        BTreeSet::from(["id".to_string(), "amount".to_string()]),
        "test_required_columns",
    );

    let (header, rows) = parse_csv_source_content_with_plan(
        "id,label,amount\n1,alpha,8\n",
        &plan,
        Some(SMOKE_MAX_INPUT_ROWS),
    )
    .expect("CSV parses with read plan");

    assert_eq!(header, vec!["id", "label", "amount"]);
    assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(rows[0].get("amount"), Some(&ScalarValue::Int64(8)));
    assert!(!rows[0].contains_key("label"));
}

#[test]
fn jsonl_source_read_plan_skips_unselected_nested_and_escaped_values() {
    let plan = LocalSourceReadPlan::required(
        BTreeSet::from(["id".to_string(), "label".to_string()]),
        "test_projected_jsonl_columns",
    );

    let (header, rows) = parse_jsonl_source_content_with_plan(
        "{\"id\":1,\"payload\":{\"nested\":[1,2]},\"label\":\"alpha\",\"tail\":\"\\u263A\"}\n",
        &plan,
        Some(SMOKE_MAX_INPUT_ROWS),
    )
    .expect("projected JSONL parse skips unselected values");

    assert_eq!(header, vec!["id", "payload", "label", "tail"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(
        rows[0].get("label"),
        Some(&ScalarValue::Utf8("alpha".into()))
    );
    assert!(!rows[0].contains_key("payload"));
    assert!(!rows[0].contains_key("tail"));
}

#[test]
fn jsonl_source_read_plan_normalizes_selected_nested_values_as_utf8_json_payload() {
    let plan = LocalSourceReadPlan::required(
        BTreeSet::from(["id".to_string(), "payload".to_string()]),
        "test_selected_jsonl_nested_column",
    );

    let (header, rows) = parse_jsonl_source_content_with_plan(
        "{\"id\":1,\"payload\":{\"nested\":[1,2]}}\n",
        &plan,
        Some(SMOKE_MAX_INPUT_ROWS),
    )
    .expect("selected nested JSONL value is normalized as UTF-8 JSON payload");

    assert_eq!(header, vec!["id", "payload"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("payload"),
        Some(&ScalarValue::Utf8("{\"nested\":[1,2]}".to_string()))
    );
}

#[test]
fn json_parser_handles_flat_array_and_missing_fields() {
    let (header, rows) =
        parse_json_source_content("[{\"id\":1,\"label\":\"alpha\"},{\"id\":2,\"score\":2.5}]")
            .expect("json array parses");

    assert_eq!(header, vec!["id", "label", "score"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    assert_eq!(
        rows[0].get("label"),
        Some(&ScalarValue::Utf8("alpha".into()))
    );
    assert_eq!(rows[0].get("score"), Some(&ScalarValue::Null));
    assert_eq!(rows[1].get("label"), Some(&ScalarValue::Null));
    assert_eq!(rows[1].get("score"), Some(&ScalarValue::Float64(2.5)));
}

#[test]
fn json_parser_handles_single_flat_object() {
    let (header, rows) =
        parse_json_source_content("{\"id\":1,\"label\":\"alpha\"}").expect("json parses");

    assert_eq!(header, vec!["id", "label"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
}

#[test]
fn json_parser_normalizes_nested_values_as_utf8_json_payloads() {
    let (header, rows) = parse_json_source_content("[{\"id\":1,\"payload\":{\"x\":1}}]")
        .expect("nested JSON values normalize");
    assert_eq!(header, vec!["id", "payload"]);
    assert_eq!(
        rows[0].get("payload"),
        Some(&ScalarValue::Utf8("{\"x\":1}".to_string()))
    );
}

#[cfg(feature = "universal-format-io")]
#[test]
fn layout_advisor_classifies_arbitrary_names_from_first_batch_profile_and_rejects_misnamed_clickbench()
 {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_vortex::universal_format_io::first_batch_profile::FirstBatchProfile;
    use std::sync::Arc;

    let pos_schema = Arc::new(Schema::new(vec![
        Field::new("landing_link", DataType::Utf8, true),
        Field::new("observed_at", DataType::Int64, false),
        Field::new("req_counter", DataType::Int64, false),
        Field::new("short_tag", DataType::Utf8, false),
    ]));
    let pos_batch = RecordBatch::try_new(
        Arc::clone(&pos_schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("https://shardloom.io/docs/vortex"),
                Some("https://docs.rs/arrow"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_700_002_861_i64, 1_700_002_920])) as ArrayRef,
            Arc::new(Int64Array::from(vec![101_i64, 202])) as ArrayRef,
            Arc::new(StringArray::from(vec!["aa", "bb"])) as ArrayRef,
        ],
    )
    .expect("positive advisor batch");
    let pos_profile = FirstBatchProfile::from_record_batch(&pos_batch);

    let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("events.parquet"))
        .expect("infer parquet adapter");
    let pos_source = VortexIngestSourceData {
        source_format: source_adapter.source_format,
        source_adapter: source_adapter.clone(),
        header: vec![
            "landing_link".to_string(),
            "observed_at".to_string(),
            "req_counter".to_string(),
            "short_tag".to_string(),
        ],
        column_arrow_dtypes: vec![None, None, None, None],
        first_batch_profile: Some(pos_profile),
        read_plan: LocalSourceReadPlan::full("test_content_profile_positive"),
        materialized_columns: vec!["landing_link".to_string()],
        reader_projection_columns: vec!["landing_link".to_string()],
        projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
        source_bytes: 1024,
        source_digest: "fnv64:pos".to_string(),
        source_fingerprint: SourceFingerprintEvidence::metadata_only(),
        row_count: 100_000_000,
        row_count_known: true,
        source_split_row_ranges: vec![(0, 100_000_000)],
        source_metadata_scout_millis: 0,
        source_byte_acquisition_millis: 0,
        source_full_body_millis: 0,
        read_millis: 0,
        compatibility_parse_millis: 0,
        source_to_columnar_millis: 0,
        record_batch_count: 0,
        source_stream_batch_size: 262_144,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_hint_kind: "test_units".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status:
            "parquet_arrow_reader_requested_dictionary_preservation_for_string_derived_columns"
                .to_string(),
        ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
        ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
        ingest_executor_requested_parallelism: 2,
        ingest_executor_applied_parallelism: 2,
        ingest_executor_unit_count_hint: None,
        materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
        parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
        columnar_source_preserved: true,
    };
    assert_eq!(layout_source_profile(&pos_source), "url_time_counter_olap");
    assert_eq!(
        layout_prepared_layout_family(&pos_source),
        "url_time_counter_dictionary_stats_layout"
    );
    assert_eq!(
        layout_writer_compression_candidate_fields(&pos_source),
        vec!["landing_link".to_string(), "short_tag".to_string()]
    );

    let mis_schema = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, false),
        Field::new("EventTime", DataType::Int64, false),
        Field::new("WatchID", DataType::Int64, false),
    ]));
    let mis_batch = RecordBatch::try_new(
        mis_schema,
        vec![
            Arc::new(StringArray::from(vec!["blue", "pink"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10_i64, 20])) as ArrayRef,
        ],
    )
    .expect("misnamed batch");
    let mis_source = VortexIngestSourceData {
        source_format: source_adapter.source_format,
        source_adapter,
        header: vec![
            "URL".to_string(),
            "EventTime".to_string(),
            "WatchID".to_string(),
        ],
        column_arrow_dtypes: vec![
            Some(DataType::Utf8),
            Some(DataType::Int64),
            Some(DataType::Int64),
        ],
        first_batch_profile: Some(FirstBatchProfile::from_record_batch(&mis_batch)),
        read_plan: LocalSourceReadPlan::full("test_content_profile_misnamed"),
        materialized_columns: vec!["URL".to_string()],
        reader_projection_columns: vec!["URL".to_string()],
        projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
        source_bytes: 1024,
        source_digest: "fnv64:mis".to_string(),
        source_fingerprint: SourceFingerprintEvidence::metadata_only(),
        row_count: 100,
        row_count_known: true,
        source_split_row_ranges: vec![(0, 100)],
        source_metadata_scout_millis: 0,
        source_byte_acquisition_millis: 0,
        source_full_body_millis: 0,
        read_millis: 0,
        compatibility_parse_millis: 0,
        source_to_columnar_millis: 0,
        record_batch_count: 0,
        source_stream_batch_size: 262_144,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_hint_kind: "test_units".to_string(),
        source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
        source_dictionary_preservation_status: "test".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: None,
        materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
        parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
        columnar_source_preserved: true,
    };
    assert!(!layout_source_has_text_domain_columns(&mis_source));
    assert!(!layout_source_has_time_bucket_columns(&mis_source));
    assert_eq!(layout_source_profile(&mis_source), "counter_olap");
}
