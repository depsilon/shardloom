use super::*;
use arrow_array::{Int64Array, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use vortex::{
    VortexSessionDefault as _,
    array::VortexSessionExecute as _,
    arrow::ArrowSessionExt as _,
    file::OpenOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shardloom-ingest-cpu-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn label(row: usize) -> Option<&'static str> {
    match row % 4 {
        0 => None,
        1 => Some("a non-URL label"),
        2 => Some("東京-λ"),
        _ => Some(""),
    }
}

fn fixture(root: &Path) -> (PathBuf, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("renamed_identifier", DataType::Int64, false),
        Field::new("renamed_text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..17_i64).map(|row| (1_i64 << 60) + row),
            )),
            Arc::new(StringArray::from((0..17).map(label).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let path = root.join("input.parquet");
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create(&path).unwrap(),
        Arc::clone(&schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    (path, schema)
}

fn verify_complete(path: &Path, schema: &Schema) {
    let runtime = CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .unwrap();
    assert_eq!(file.row_count(), 17);
    let expected_dtype =
        arrow_record_batch_to_vortex_array(RecordBatch::new_empty(Arc::new(schema.clone())))
            .unwrap()
            .dtype()
            .clone();
    assert_eq!(file.dtype(), &expected_dtype);
    let target = Field::new("", DataType::Struct(schema.fields.clone()), false);
    let mut execution = session.create_execution_ctx();
    let mut row = 0_usize;
    for array in file.scan().unwrap().into_array_iter(&runtime).unwrap() {
        let decoded = session
            .arrow()
            .execute_arrow(array.unwrap(), Some(&target), &mut execution)
            .unwrap();
        let fields = decoded.as_any().downcast_ref::<StructArray>().unwrap();
        let identifiers = fields
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = fields
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for local in 0..fields.len() {
            assert_eq!(
                identifiers.value(local),
                (1_i64 << 60) + i64::try_from(row).unwrap()
            );
            assert_eq!(text.is_null(local), label(row).is_none());
            if let Some(expected) = label(row) {
                assert_eq!(text.value(local), expected);
            }
            row += 1;
        }
    }
    assert_eq!(row, 17);
}

#[test]
fn ingest_cpu_grants_preserve_native_values_across_parquet_and_single_prefetch_sources() {
    let root = Directory::new();
    let (input, schema) = fixture(&root.0);
    for grant in [1, 2, 4, 8] {
        for single_prefetch in [false, true] {
            let source =
                crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
                    &input,
                    100,
                    if single_prefetch { 1 } else { grant },
                )
                .unwrap();
            let source = if single_prefetch {
                crate::universal_format_io::with_capillary_prefetch_columnar_stream_source(
                    source, grant,
                )
            } else {
                source
            };
            assert_eq!(source.ingest_executor_requested_parallelism, grant);
            assert_eq!(source.ingest_executor_applied_parallelism, 1);
            let path = root
                .0
                .join(format!("grant-{grant}-prefetch-{single_prefetch}.vortex"));
            let request = VortexPreparedStateColumnarStreamWriteRequest::new(&path, source)
                .shared_native_memory_budget_bytes(32 << 20);
            let report = write_flat_columnar_vortex_prepared_state_streaming(request).unwrap();
            let lanes = crate::ingest_cpu_lanes::IngestCpuLanes::pipeline(grant, 6);
            let design = &report.writer_physical_design;
            assert_eq!(report.row_count, 17);
            assert_eq!(design.array_build_worker_count, lanes.conversion_workers());
            assert_eq!(design.array_build_prefetch_window, lanes.prefetch_slots());
            assert_eq!(
                report.writer_runtime_background_workers,
                lanes.provider_drivers()
            );
            assert_eq!(
                report.writer_runtime_applied_parallelism,
                1 + lanes.provider_drivers()
            );
            assert_eq!(report.writer_runtime_requested_parallelism, grant);
            assert_eq!(
                1 + lanes.source_workers()
                    + design.array_build_worker_count
                    + report.writer_runtime_background_workers,
                grant
            );
            assert!(
                design
                    .writer_queue_topology
                    .contains(&format!("ingest_cpu_configured={grant};"))
            );
            assert!(
                design
                    .writer_queue_topology
                    .contains("excludes_blocking_io_and_source_library_internal_threads")
            );
            assert!(!design.fallback_attempted);
            assert!(!design.external_engine_invoked);
            verify_complete(&path, &schema);
        }
    }
}

#[test]
fn parallel_codec_writer_preserves_complete_values_and_admitted_owners_across_grants() {
    let root = Directory::new();
    let (input, schema) = fixture(&root.0);
    for grant in [1, 2, 3, 4, 5, 8] {
        let source =
            crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
                &input, 100, grant,
            )
            .unwrap();
        // Select the production large-source codec policy on tiny deterministic
        // data. Source/writer row-count verification still requires all 17 rows.
        let mut advice = super::tests::layout_advisor_input(true, "none");
        advice.source_format = "parquet".to_string();
        advice.writer_provider_kind = "vortex_array_kernel".to_string();
        advice.writer_provider_surface =
            "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)".to_string();
        advice.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
        advice.writer_parallelism_budget = grant;
        advice.writer_compression_candidate_fields = vec!["renamed_text".to_string()];
        let path = root.0.join(format!("parallel-codec-{grant}.vortex"));
        let report = write_flat_columnar_vortex_prepared_state_streaming(
            VortexPreparedStateColumnarStreamWriteRequest::new(&path, source)
                .layout_write_advisor(evaluate_vortex_layout_write_advisor(advice))
                .shared_native_memory_budget_bytes(32 << 20),
        )
        .unwrap();
        let lanes = crate::ingest_cpu_lanes::IngestCpuLanes::pipeline(grant, 6);
        let design = &report.writer_physical_design;
        assert_eq!(design.array_build_worker_count, lanes.conversion_workers());
        assert_eq!(design.array_build_prefetch_window, lanes.prefetch_slots());
        assert_eq!(
            report.writer_runtime_background_workers,
            lanes.provider_drivers()
        );
        assert_eq!(report.writer_runtime_requested_parallelism, grant);
        assert_eq!(
            1 + lanes.source_workers()
                + design.array_build_worker_count
                + report.writer_runtime_background_workers,
            grant
        );
        assert_eq!(report.writer_compression_concurrency, grant);
        assert_eq!(report.row_count, 17);
        assert_eq!(
            report
                .shared_native_memory
                .as_ref()
                .unwrap()
                .final_reserved_bytes,
            0
        );
        assert!(!design.fallback_attempted);
        assert!(!design.external_engine_invoked);
        verify_complete(&path, &schema);
    }
}

#[test]
fn repeated_writer_grant_changes_drop_owned_drivers_before_the_next_artifact() {
    let context = LocalVortexWriteContext::open();
    for grant in [8, 1, 4, 2, 8, 1] {
        let mut source = VortexWriterPhysicalDesignSourceInput::writer_only();
        source.cpu_lanes = Some(crate::ingest_cpu_lanes::IngestCpuLanes::pipeline(grant, 1));
        let decision = VortexLayoutWriteRuntimeDecision::not_requested_for_source(
            "vortex_array_kernel",
            "test",
            VortexIngestCertificationLevel::IngestCertified,
            source,
        );
        let (policy, drivers) = context.apply_runtime_policy(&decision).unwrap();
        assert_eq!(policy.background_workers, grant.saturating_sub(3));
        // Group Drop joins every owned CPU driver. The reusable context holds
        // no worker group or detached provider pool between these boundaries.
        drop(drivers);
    }
}

#[test]
fn narrower_request_never_silently_reuses_an_oversized_source_grant() {
    let root = Directory::new();
    let (input, _) = fixture(&root.0);
    let source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
        &input, 100, 8,
    )
    .unwrap();
    let source =
        crate::universal_format_io::with_capillary_prefetch_columnar_stream_source(source, 1);
    assert_eq!(source.ingest_executor_requested_parallelism, 1);
    assert_eq!(source.ingest_executor_applied_parallelism, 1);
    let path = root.0.join("denied.vortex");
    let error = write_flat_columnar_vortex_prepared_state_streaming(
        VortexPreparedStateColumnarStreamWriteRequest::new(&path, source),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("existing source workers exceed the requested grant")
    );
    assert!(!path.exists());
}
