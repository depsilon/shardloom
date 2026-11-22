//! Extracted unit test suite for `vortex_ingest` (`E2E-ARCH-01`).

use super::*;

#[test]
fn buffered_scalar_writer_shares_retained_source_credits_and_retries_after_release() {
    let root = temp_test_root("buffered-shared-source-owner");
    std::fs::create_dir_all(&root).unwrap();
    let existing = root.join("existing.vortex");
    let absent = root.join("absent").join("new.vortex");
    std::fs::write(&existing, b"keep existing output").unwrap();
    let resources = shardloom_core::ExecutionResources::from_bytes(
        16 << 20,
        1,
        shardloom_core::ExecutionResourceOrigin::ExecutionCall,
    )
    .unwrap();
    let memory = LiveMemoryPool::new(resources.memory_bytes()).unwrap();
    let retained_source = memory.reserve(resources.memory_bytes() - 4096).unwrap();
    let request = |path: &std::path::Path| {
        VortexPreparedStateWriteRequest::new(
            path,
            vec!["value".into()],
            vec![vec![("value".into(), ScalarValue::UInt64(37))]],
            resources,
        )
        .allow_overwrite(true)
    };
    for output in [&existing, &absent] {
        let error = write_flat_scalar_vortex_prepared_state_with_memory(
            request(output),
            retained_source.pool(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("memory reservation denied"),
            "{error}"
        );
        assert_eq!(memory.snapshot().reserved_bytes, retained_source.bytes());
        assert_eq!(std::fs::read(&existing).unwrap(), b"keep existing output");
        assert!(!absent.parent().unwrap().exists());
    }
    drop(retained_source);
    let other_source = memory.reserve(128).unwrap();
    let report =
        write_flat_scalar_vortex_prepared_state_with_memory(request(&absent), other_source.pool())
            .unwrap();
    assert_eq!(report.row_count, 1);
    assert_eq!(report.reopen_row_count, 1);
    let ownership = report.shared_native_memory.unwrap();
    assert_eq!(ownership.final_reserved_bytes, 128);
    assert_eq!(ownership.denied_reservations, 2);
    assert!(ownership.peak_reserved_bytes <= resources.memory_bytes());
    assert_eq!(memory.snapshot().reserved_bytes, 128);
    drop(other_source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn buffered_scalar_ingest_reserves_before_validation_and_destination_changes() {
    let root = temp_test_root("buffered-resource-owner");
    std::fs::create_dir_all(&root).unwrap();
    let existing = root.join("existing.vortex");
    let absent = root.join("absent").join("new.vortex");
    std::fs::write(&existing, b"keep existing output").unwrap();
    for path in [&existing, &absent] {
        for (bytes, payload) in [(1, "small".to_string()), (128 << 10, "x".repeat(128 << 10))] {
            let request = VortexPreparedStateWriteRequest::new(
                path,
                vec!["value".into()],
                vec![vec![("value".into(), ScalarValue::Utf8(payload))]],
                shardloom_core::ExecutionResources::from_bytes(
                    bytes,
                    1,
                    shardloom_core::ExecutionResourceOrigin::ExecutionCall,
                )
                .unwrap(),
            )
            .allow_overwrite(true);
            let error = write_flat_scalar_vortex_prepared_state(request).unwrap_err();
            assert!(error.to_string().contains("memory"), "{error}");
            assert_eq!(std::fs::read(&existing).unwrap(), b"keep existing output");
            assert!(!absent.parent().unwrap().exists());
        }
    }
    let request = VortexPreparedStateWriteRequest::new(
        &absent,
        vec!["value".into()],
        vec![vec![("value".into(), ScalarValue::Utf8("admitted".into()))]],
        shardloom_core::ExecutionResources::from_bytes(
            16 << 20,
            1,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .unwrap(),
    );
    let report = write_flat_scalar_vortex_prepared_state(request).unwrap();
    assert_eq!(report.row_count, 1);
    assert_eq!(report.reopen_row_count, 1);
    let memory = report.shared_native_memory.unwrap();
    assert!(memory.buffered_conversion);
    assert!(
        memory.peak_reserved_bytes
            >= shardloom_core::WorkspaceSafeLocalStagingWriter::buffer_capacity_bytes()
    );
    assert!(memory.peak_reserved_bytes <= memory.limit_bytes);
    assert_eq!(memory.final_reserved_bytes, 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn reopened_inventory_schema_is_compact_and_digest_is_deterministic() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-compact-inventory-{}.vortex",
        std::process::id()
    ));
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".into(), "payload".into()],
        vec![vec![
            ("id".into(), ScalarValue::Int64(1)),
            ("payload".into(), ScalarValue::Utf8("alpha".into())),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );
    write_flat_scalar_vortex_prepared_state(request).unwrap();
    let first = read_prepared_vortex_artifact_layout_inventory(&path).unwrap();
    let second = read_prepared_vortex_artifact_layout_inventory(&path).unwrap();
    assert_eq!(first.dtype_summary, "{id=i64, payload=utf8}");
    assert!(first.dtype_summary.len() < 256);
    assert_eq!(first.inventory_digest, second.inventory_digest);
    assert_eq!(first.row_count, Some(1));
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "universal-format-io")]
fn reopen_vortex_artifact_as_arrow_struct(
    path: &Path,
    schema: &arrow_schema::Schema,
) -> arrow_array::ArrayRef {
    use vortex::VortexSessionDefault as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::stream::ArrayStreamExt as _;
    use vortex::arrow::ArrowSessionExt as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let bytes = std::fs::read(path).expect("read vortex artifact");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session
        .open_options()
        .open_buffer(bytes)
        .expect("open vortex artifact");
    let array = runtime
        .block_on(
            file.scan()
                .expect("scan vortex artifact")
                .into_array_stream()
                .expect("array stream")
                .read_all(),
        )
        .expect("read vortex array");
    let mut ctx = session.create_execution_ctx();
    let target = arrow_schema::Field::new(
        "",
        arrow_schema::DataType::Struct(schema.fields.clone()),
        false,
    );
    session
        .arrow()
        .execute_arrow(array, Some(&target), &mut ctx)
        .expect("execute vortex artifact to arrow struct")
}

fn temp_test_root(name: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    root.push(format!(
        "shardloom-vortex-ingest-{name}-{}-{nanos}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

fn legacy_prepared_olap_state_manifest_path(target: &Path) -> PathBuf {
    target
        .parent()
        .expect("target parent")
        .join(".shardloom")
        .join(format!(
            "{}.prepared-olap-state.manifest",
            target
                .file_name()
                .and_then(|value| value.to_str())
                .expect("target file name")
        ))
}

fn legacy_prepared_olap_state_sidecar_dir(target: &Path) -> PathBuf {
    target
        .parent()
        .expect("target parent")
        .join(".shardloom")
        .join(format!(
            "{}.prepared-olap-state.d",
            target
                .file_name()
                .and_then(|value| value.to_str())
                .expect("target file name")
        ))
}

fn write_test_prepared_vortex_artifact(target: &Path, seed: i64) -> VortexPreparedStateWriteReport {
    let seed_f64 = f64::from(i32::try_from(seed).expect("test seed fits f64 exactly"));
    let request = VortexPreparedStateWriteRequest::new(
        target,
        vec![
            "id".to_string(),
            "group_key".to_string(),
            "metric".to_string(),
        ],
        vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(seed)),
                ("group_key".to_string(), ScalarValue::Int64(seed % 3)),
                ("metric".to_string(), ScalarValue::Float64(seed_f64 + 0.5)),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(seed + 1)),
                ("group_key".to_string(), ScalarValue::Int64((seed + 1) % 3)),
                ("metric".to_string(), ScalarValue::Float64(seed_f64 + 1.5)),
            ],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .allow_overwrite(true);
    let report = write_flat_scalar_vortex_prepared_state(request).expect("write test vortex");
    assert_eq!(
        report.prepared_olap_layout_inventory.status,
        "opened_single_vortex_artifact_footer"
    );
    assert_eq!(report.prepared_olap_layout_inventory.row_count, Some(2));
    assert!(
        report
            .prepared_olap_layout_inventory
            .metadata_persisted_in_artifact
    );
    report
}

#[test]
fn native_vortex_artifact_prepare_admits_same_artifact_without_rewrite() {
    let root = temp_test_root("native-vortex-pass-through");
    let source = root.join("prepared.vortex");
    let written = write_test_prepared_vortex_artifact(&source, 10);
    let request = VortexNativeArtifactPrepareRequest::new_local(
        &source,
        &source,
        false,
        shardloom_core::ExecutionResources::from_bytes(
            1024 * 1024,
            2,
            shardloom_core::ExecutionResourceOrigin::Context,
        )
        .unwrap(),
        "test-vortex-provider",
        "vortex-write",
        "ingest_certified",
    )
    .expect("native request");

    let report = prepare_native_vortex_artifact(&request).expect("native prepare report");

    assert_eq!(report.row_count, written.row_count);
    assert_eq!(report.status, "native_vortex_artifact_admitted");
    assert_eq!(report.policy, "vortex_native_prepared_state_pass_through");
    assert!(report.encoded_layout_preserved);
    assert!(report.same_artifact_passthrough);
    assert!(!report.workspace_copy_performed);
    assert!(!report.reencode_performed);
    assert!(!report.upstream_vortex_scan_called);
    assert!(!report.upstream_vortex_write_called);
    assert!(report.workspace_write_report.is_none());
    assert_eq!(
        report.target_digest_source,
        "native_vortex_metadata_footer_fingerprint_no_byte_read"
    );
    assert_eq!(
        report.segment_metadata_primitive.status,
        "admitted_footer_segment_metadata"
    );
    assert_eq!(
        report
            .segment_metadata_primitive
            .admission_for_query_family(VortexSegmentMetadataQueryFamily::MetadataCount),
        "admitted_metadata_count_from_prepared_footer_or_writer_summary"
    );
    assert!(
        report
            .segment_metadata_primitive
            .is_predicate_pruning_admitted()
    );
    assert!(
        report
            .segment_metadata_primitive
            .must_read_inconclusive_segments()
    );
    let fields = report.evidence_fields();
    assert_eq!(
        fields
            .iter()
            .find(|(key, _)| key == "vortex_to_vortex_reencode_performed")
            .map(|(_, value)| value.as_str()),
        Some("false")
    );
    assert!(!legacy_prepared_olap_state_manifest_path(&source).exists());
    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn native_vortex_artifact_prepare_copies_bytes_without_scan_stream_rewrite() {
    let root = temp_test_root("native-vortex-copy");
    let source = root.join("prepared.vortex");
    let target = root.join("copied.vortex");
    write_test_prepared_vortex_artifact(&source, 20);
    let request = VortexNativeArtifactPrepareRequest::new_local(
        &source,
        &target,
        false,
        shardloom_core::ExecutionResources::from_bytes(
            1024 * 1024,
            2,
            shardloom_core::ExecutionResourceOrigin::Context,
        )
        .unwrap(),
        "test-vortex-provider",
        "vortex-write",
        "ingest_certified",
    )
    .expect("native request");

    let report = prepare_native_vortex_artifact(&request).expect("native prepare report");

    assert_eq!(report.status, "native_vortex_artifact_copied");
    assert_eq!(report.policy, "vortex_native_workspace_safe_byte_copy");
    assert!(report.encoded_layout_preserved);
    assert!(report.workspace_copy_performed);
    assert!(!report.same_artifact_passthrough);
    assert!(!report.reencode_performed);
    assert!(!report.upstream_vortex_scan_called);
    assert!(!report.upstream_vortex_write_called);
    assert!(report.workspace_write_report.is_some());
    assert_eq!(
        std::fs::read(&source).expect("read source"),
        std::fs::read(&target).expect("read target")
    );
    assert_eq!(
        report.source_size_bytes,
        report
            .workspace_write_report
            .as_ref()
            .unwrap()
            .bytes_written
    );
    assert_eq!(
        report.target_digest_source,
        "workspace_safe_byte_copy_streaming_sha256_digest"
    );
    assert!(!legacy_prepared_olap_state_manifest_path(&target).exists());
    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn native_vortex_artifact_resources_refuse_before_input_and_preserve_shared_ownership() {
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
    use shardloom_exec::live_memory::LiveMemoryPool;
    let root = temp_test_root("native-vortex-resources");
    let source = root.join("source.vortex");
    let target = root.join("target.vortex");
    let resources = |bytes| {
        ExecutionResources::from_bytes(bytes, 3, ExecutionResourceOrigin::Platform).unwrap()
    };
    let mut request = VortexNativeArtifactPrepareRequest::new_local(
        &source,
        &target,
        true,
        resources(1),
        "test-provider",
        "vortex-write",
        "ingest_certified",
    )
    .unwrap();
    // A missing source would return a stat error if any inspection ran first.
    let error = prepare_native_vortex_artifact(&request).unwrap_err();
    assert!(error.to_string().contains("memory reservation denied"));
    assert!(!source.exists());
    assert!(!target.exists());

    write_test_prepared_vortex_artifact(&source, 41);
    fs::write(&target, b"preserve existing output").unwrap();
    let pool = LiveMemoryPool::new(1024 * 1024).unwrap();
    request.resources = resources(1024 * 1024);
    request.shared_memory_pool = Some(pool.clone());
    let retained = pool.reserve(1024 * 1024 - 4096).unwrap();
    assert!(
        prepare_native_vortex_artifact(&request)
            .unwrap_err()
            .to_string()
            .contains("memory reservation denied")
    );
    assert_eq!(fs::read(&target).unwrap(), b"preserve existing output");
    assert_eq!(pool.snapshot().reserved_bytes, retained.bytes());
    drop(retained);

    // The copy slot fits, but the provider footer does not. It must use the
    // same pool and still fail before replacing the existing target.
    let retained = pool.reserve(1024 * 1024 - 8192).unwrap();
    assert!(prepare_native_vortex_artifact(&request).is_err());
    assert_eq!(pool.snapshot().reserved_bytes, retained.bytes());
    assert_eq!(fs::read(&target).unwrap(), b"preserve existing output");
    drop(retained);

    request.resources = resources(1024 * 1024 - 1);
    assert!(
        prepare_native_vortex_artifact(&request)
            .unwrap_err()
            .to_string()
            .contains("exceeds the declared allocation")
    );
    request.resources = resources(1024 * 1024);
    let retained = pool.reserve(2048).unwrap();
    for pass_through in [false, true] {
        request.target_path = if pass_through {
            source.clone()
        } else {
            target.clone()
        };
        let report = prepare_native_vortex_artifact(&request).unwrap();
        assert_eq!(report.memory.reserved_bytes, retained.bytes());
        assert_eq!(pool.snapshot().reserved_bytes, retained.bytes());
        assert_eq!(report.resources, request.resources);
        assert!(report.memory.peak_reserved_bytes <= report.memory.limit_bytes);
        let fields: BTreeMap<_, _> = report.evidence_fields().into_iter().collect();
        assert_eq!(fields["execution_resource_memory_origin"], "platform");
        assert_eq!(fields["execution_resource_declared_max_parallelism"], "3");
        assert_eq!(fields["execution_resource_admitted_max_parallelism"], "1");
        assert_eq!(
            fields["execution_resource_admitted_memory_bytes"],
            "1048576"
        );
        assert_eq!(
            fields["execution_resource_whole_process_memory_limit_enforced"],
            "false"
        );
        assert_eq!(
            fields["execution_resource_observed_spill_io_performed"],
            "false"
        );
    }
    assert_eq!(fs::read(&source).unwrap(), fs::read(&target).unwrap());
    drop(retained);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn native_vortex_artifact_cancel_refuses_before_inspection_and_preserves_retained_credit() {
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
    use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
    let root = temp_test_root("native-vortex-cancel");
    let source = root.join("missing.vortex");
    let target = root.join("absent").join("target.vortex");
    let pool = LiveMemoryPool::new(1024 * 1024).unwrap();
    let retained = pool.reserve(17).unwrap();
    let mut request = VortexNativeArtifactPrepareRequest::new_local(
        &source,
        &target,
        true,
        ExecutionResources::from_bytes(1024 * 1024, 1, ExecutionResourceOrigin::Context).unwrap(),
        "test-provider",
        "vortex-write",
        "ingest_certified",
    )
    .unwrap();
    request.shared_memory_pool = Some(pool.clone());
    let token = CancellationToken::default();
    token.cancel();
    let error = prepare_native_vortex_artifact_controlled(&request, &token).unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(!source.exists());
    assert!(!target.parent().unwrap().exists());
    assert_eq!(pool.snapshot().reserved_bytes, 17);
    assert_eq!(pool.snapshot().peak_reserved_bytes, 17);
    drop(retained);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn native_vortex_copy_cancel_discards_staged_prefix_and_keeps_destination() {
    use shardloom_exec::compute_pool::CancellationToken;
    struct CancellingReader {
        cursor: std::io::Cursor<Vec<u8>>,
        token: CancellationToken,
        reads: usize,
    }
    impl std::io::Read for CancellingReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            self.reads += 1;
            let count = std::io::Read::read(&mut self.cursor, output)?;
            if self.reads == 2 {
                self.token.cancel();
            }
            Ok(count)
        }
    }
    let root = temp_test_root("native-vortex-copy-cancel");
    let target = root.join("target.vortex");
    fs::write(&target, b"preserve destination").unwrap();
    let token = CancellationToken::default();
    let mut source = CancellingReader {
        cursor: std::io::Cursor::new(vec![42_u8; 17_000]),
        token: token.clone(),
        reads: 0,
    };
    let error = shardloom_core::write_workspace_safe_bytes_with_validated_producer(
        &root,
        &target,
        true,
        "cancelled native copy fixture",
        |writer| {
            copy_native_artifact_bytes(&mut source, writer, &token)
                .map_err(|error| ShardLoomError::new(error.to_string()))
        },
        |_| token.check(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert_eq!(source.reads, 2);
    assert_eq!(fs::read(&target).unwrap(), b"preserve destination");
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn native_vortex_artifact_copy_reserves_workspace_staging_capacity() {
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
    let root = temp_test_root("native-vortex-staging-resources");
    let source = root.join("source.vortex");
    let target = root.join("target.vortex");
    write_test_prepared_vortex_artifact(&source, 41);
    fs::write(&target, b"preserve existing output").unwrap();
    let request = VortexNativeArtifactPrepareRequest::new_local(
        &source,
        &target,
        true,
        ExecutionResources::from_bytes(128 * 1024, 1, ExecutionResourceOrigin::Context).unwrap(),
        "test-provider",
        "vortex-write",
        "ingest_certified",
    )
    .unwrap();
    // Footer reads and the copy buffer fit, but the staged writer must also
    // obtain credits before output creation or replacement.
    let error = prepare_native_vortex_artifact(&request).unwrap_err();
    let writer_bytes = shardloom_core::WorkspaceSafeLocalStagingWriter::buffer_capacity_bytes();
    assert!(error.to_string().contains(&format!(
        "memory reservation denied: requested={writer_bytes}, reserved=8192, limit=131072"
    )));
    assert_eq!(fs::read(&target).unwrap(), b"preserve existing output");
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
    fs::remove_dir_all(root).unwrap();
}

fn reuse_request_for_test(
    source: &Path,
    target: &Path,
    manifest_path: &Path,
) -> VortexPreparedStateReuseRequest {
    VortexPreparedStateReuseRequest::new_local(
        source,
        target,
        manifest_path.to_path_buf(),
        "csv",
        None,
        "fnv64:parse-plan",
        "all_columns",
        "caller_owned_local_vortex_artifact",
        "test-provider",
        "vortex-write",
        "ingest_certified",
    )
    .expect("reuse request")
}

#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_scalar_rows_write_and_reopen_vortex_artifact() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec![
            "id".to_string(),
            "label".to_string(),
            "metric".to_string(),
            "active".to_string(),
        ],
        vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                ("metric".to_string(), ScalarValue::Float64(1.5)),
                ("active".to_string(), ScalarValue::Boolean(true)),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(2)),
                ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
                ("metric".to_string(), ScalarValue::Float64(2.5)),
                ("active".to_string(), ScalarValue::Boolean(false)),
            ],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(
        report.reopen_verification_status,
        "reopen_metadata_row_count_verified"
    );
    assert!(report.artifact_digest.starts_with("sha256:"));
    assert_eq!(
        report.artifact_digest_source,
        "workspace_safe_streaming_sha256_digest"
    );
    assert_eq!(report.digest_micros, 0);
    assert_eq!(
        report.workspace_write_report.bytes_written,
        report.bytes_written
    );
    assert!(
        report
            .workspace_write_report
            .output_digest
            .starts_with("sha256:")
    );
    assert_eq!(
        report.artifact_digest,
        report.workspace_write_report.output_digest
    );
    assert_eq!(report.workspace_write_report.commit_status, "committed");
    assert_eq!(
        report.workspace_write_report.commit_mode,
        "atomic_create_if_absent_hard_link_same_directory"
    );
    assert!(
        matches!(
            report.writer_context_reuse_status.as_str(),
            "thread_local_write_context_opened_for_first_artifact"
                | "thread_local_write_context_reused_for_artifact"
        ),
        "unexpected writer context reuse status: {}",
        report.writer_context_reuse_status
    );
    assert!(report.write_micros >= report.vortex_segment_write_micros);
    assert_eq!(
        report.workspace_stage_micros,
        report
            .write_micros
            .saturating_sub(report.vortex_segment_write_micros)
    );
    assert!(!report.workspace_write_report.staging_path.exists());
    assert!(report.workspace_write_report.no_fallback_invariant_holds());
    assert_eq!(report.timing_scope, "vortex_ingest_prepare_once");
    assert_eq!(report.certification_level, "ingest_certified");
    assert_eq!(
        report.column_family_summary(),
        "id:int64,label:utf8,metric:float64,active:boolean"
    );
    assert!(report.preparation_included);
    assert!(!report.query_timing_starts_after_preparation);
    assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "shardloom_scalar_rows_to_vortex_struct"
    );
    assert_eq!(report.array_build_strategy, "scalar_rows_to_vortex_struct");
    assert_eq!(report.array_build_input_layout, "materialized_rows");
    assert_eq!(report.array_build_record_batch_count, 0);
    assert!(!report.manual_scalar_copy_avoided);
    assert!(!report.layout_write_decision.runtime_decision_applied);
    assert_eq!(
        report.layout_write_decision.blocker,
        "layout_write_advisor_not_attached_to_writer"
    );
    assert_eq!(
        report.preparation_spine.schema_version,
        VORTEX_PREPARATION_SPINE_SCHEMA_VERSION
    );
    assert_eq!(
        report.preparation_spine.status,
        "admitted_local_preparation_spine"
    );
    assert_eq!(
        report.preparation_spine.vortex_first_decision,
        "implement_shardloom_kernel"
    );
    assert_eq!(report.preparation_spine.provider_kind, "shardloom_kernel");
    assert_eq!(
        report.preparation_spine.source_surface,
        "local_text_source_state_scalar_rows"
    );
    assert_eq!(report.preparation_spine.split_count, 1);
    assert_eq!(
        report.preparation_spine.native_io_certificate_status,
        "certified_local_vortex_preparation_spine"
    );
    assert!(!report.preparation_spine.fallback_attempted);
    assert!(!report.preparation_spine.external_engine_invoked);
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_vortex_write_removes_legacy_prepared_olap_sidecars_for_target() {
    let root = temp_test_root("legacy-sidecar-cleanup");
    let target = root.join("prepared.vortex");
    let legacy_manifest = legacy_prepared_olap_state_manifest_path(&target);
    let legacy_sidecar_dir = legacy_prepared_olap_state_sidecar_dir(&target);
    std::fs::create_dir_all(
        legacy_manifest
            .parent()
            .expect("legacy manifest parent exists"),
    )
    .expect("create legacy sidecar root");
    std::fs::write(&legacy_manifest, b"legacy exact sidecar manifest")
        .expect("write legacy manifest");
    std::fs::create_dir_all(&legacy_sidecar_dir).expect("create legacy sidecar dir");
    std::fs::write(legacy_sidecar_dir.join("query-summary.json"), b"{}")
        .expect("write legacy sidecar payload");

    let report = write_test_prepared_vortex_artifact(&target, 90);

    assert!(target.exists());
    assert_eq!(report.workspace_write_report.commit_status, "committed");
    assert!(
        !legacy_manifest.exists(),
        "legacy prepared OLAP manifest should be removed after successful artifact write"
    );
    assert!(
        !legacy_sidecar_dir.exists(),
        "legacy prepared OLAP sidecar directory should be removed after successful artifact write"
    );
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_scalar_binary_rows_write_reopens_exact_bytes() {
    use arrow_array::{BinaryArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-binary-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "payload".to_string(), "label".to_string()];
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        columns,
        vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                (
                    "payload".to_string(),
                    ScalarValue::Binary(vec![0x00, 0xff, 0x10]),
                ),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(2)),
                ("payload".to_string(), ScalarValue::Binary(Vec::new())),
                ("label".to_string(), ScalarValue::Utf8("empty".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(3)),
                ("payload".to_string(), ScalarValue::Binary(b"raw".to_vec())),
                ("label".to_string(), ScalarValue::Utf8("omega".to_string())),
            ],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,payload:binary,label:utf8"
    );
    assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "shardloom_scalar_rows_to_vortex_struct"
    );
    assert!(report.upstream_vortex_write_called);
    assert!(!report.upstream_vortex_scan_called);

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("label", DataType::Utf8, false),
    ]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let payload = struct_array
        .column_by_name("payload")
        .expect("payload column")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary payload column");
    assert_eq!(payload.value(0), &[0x00, 0xff, 0x10]);
    assert_eq!(payload.value(1), &[] as &[u8]);
    assert_eq!(payload.value(2), b"raw");
    assert_eq!(payload.null_count(), 0);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_leading_null_rows_infer_family_from_later_values() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-leading-null-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string()],
        vec![
            vec![("id".to_string(), ScalarValue::Null)],
            vec![("id".to_string(), ScalarValue::Int64(42))],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(report.column_family_summary(), "id:int64");
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_all_null_rows_without_dtype_default_to_nullable_utf8() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-default-null-utf8-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["value".to_string()],
        vec![vec![("value".to_string(), ScalarValue::Null)]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 1);
    assert_eq!(report.reopen_row_count, 1);
    assert_eq!(report.column_family_summary(), "value:utf8");
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_mixed_integer_and_float_rows_promote_to_float64() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-mixed-numeric-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["metric".to_string()],
        vec![
            vec![("metric".to_string(), ScalarValue::Int64(1))],
            vec![("metric".to_string(), ScalarValue::Float64(2.5))],
            vec![("metric".to_string(), ScalarValue::UInt64(3))],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(report.column_family_summary(), "metric:float64");
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_scalar_all_null_binary_rows_write_reopens_binary() {
    use arrow_array::{Array as _, BinaryArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-binary-null-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["payload".to_string()],
        vec![
            vec![("payload".to_string(), ScalarValue::Null)],
            vec![("payload".to_string(), ScalarValue::Null)],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .column_dtypes(vec![Some(LogicalDType::Binary)]);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(report.column_family_summary(), "payload:binary");
    let schema = Schema::new(vec![Field::new("payload", DataType::Binary, true)]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let payload = struct_array
        .column_by_name("payload")
        .expect("payload column")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary payload column");
    assert_eq!(payload.len(), 2);
    assert!(payload.is_null(0));
    assert!(payload.is_null(1));
    assert_eq!(payload.null_count(), 2);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_scalar_all_null_typed_rows_write_reopens_nullable_scalars() {
    use arrow_array::{
        Array as _, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, StructArray,
        TimestampMicrosecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-scalar-null-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec![
        "active".to_string(),
        "id".to_string(),
        "count".to_string(),
        "metric".to_string(),
        "label".to_string(),
        "event_day".to_string(),
        "event_ts".to_string(),
    ];
    let null_row = || {
        vec![
            ("active".to_string(), ScalarValue::Null),
            ("id".to_string(), ScalarValue::Null),
            ("count".to_string(), ScalarValue::Null),
            ("metric".to_string(), ScalarValue::Null),
            ("label".to_string(), ScalarValue::Null),
            ("event_day".to_string(), ScalarValue::Null),
            ("event_ts".to_string(), ScalarValue::Null),
        ]
    };
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        columns,
        vec![null_row(), null_row()],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .column_dtypes(vec![
        Some(LogicalDType::Boolean),
        Some(LogicalDType::Int64),
        Some(LogicalDType::UInt64),
        Some(LogicalDType::Float64),
        Some(LogicalDType::Utf8),
        Some(LogicalDType::Date32),
        Some(LogicalDType::TimestampMicros),
    ]);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(
        report.column_family_summary(),
        "active:boolean,id:int64,count:uint64,metric:float64,label:utf8,event_day:date32,event_ts:timestamp_micros"
    );
    let schema = Schema::new(vec![
        Field::new("active", DataType::Boolean, true),
        Field::new("id", DataType::Int64, true),
        Field::new("count", DataType::UInt64, true),
        Field::new("metric", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("event_day", DataType::Date32, true),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");

    assert_eq!(
        struct_array
            .column_by_name("active")
            .expect("active column")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column")
            .null_count(),
        2
    );
    assert_eq!(
        struct_array
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column")
            .null_count(),
        2
    );
    assert_eq!(
        struct_array
            .column_by_name("count")
            .expect("count column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64 column")
            .null_count(),
        2
    );
    assert_eq!(
        struct_array
            .column_by_name("metric")
            .expect("metric column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 column")
            .null_count(),
        2
    );
    assert_eq!(
        struct_array
            .column_by_name("label")
            .expect("label column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column")
            .null_count(),
        2
    );
    assert_eq!(
        struct_array
            .column_by_name("event_day")
            .expect("event_day column")
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 column")
            .null_count(),
        2
    );
    assert_eq!(
        struct_array
            .column_by_name("event_ts")
            .expect("event_ts column")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp_micros column")
            .null_count(),
        2
    );

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_scalar_mixed_null_rows_write_reopens_nullable_scalars() {
    use arrow_array::{
        Array as _, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, StructArray,
        TimestampMicrosecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-scalar-mixed-null-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec![
            "active".to_string(),
            "id".to_string(),
            "count".to_string(),
            "metric".to_string(),
            "label".to_string(),
            "event_day".to_string(),
            "event_ts".to_string(),
        ],
        vec![
            vec![
                ("active".to_string(), ScalarValue::Boolean(true)),
                ("id".to_string(), ScalarValue::Int64(-10)),
                ("count".to_string(), ScalarValue::UInt64(7)),
                ("metric".to_string(), ScalarValue::Float64(1.5)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                ("event_day".to_string(), ScalarValue::Date32(20_000)),
                (
                    "event_ts".to_string(),
                    ScalarValue::TimestampMicros(1_700_000_000_000_000),
                ),
            ],
            vec![
                ("active".to_string(), ScalarValue::Null),
                ("id".to_string(), ScalarValue::Null),
                ("count".to_string(), ScalarValue::Null),
                ("metric".to_string(), ScalarValue::Null),
                ("label".to_string(), ScalarValue::Null),
                ("event_day".to_string(), ScalarValue::Null),
                ("event_ts".to_string(), ScalarValue::Null),
            ],
            vec![
                ("active".to_string(), ScalarValue::Boolean(false)),
                ("id".to_string(), ScalarValue::Int64(20)),
                ("count".to_string(), ScalarValue::UInt64(8)),
                ("metric".to_string(), ScalarValue::Float64(2.5)),
                ("label".to_string(), ScalarValue::Utf8("omega".to_string())),
                ("event_day".to_string(), ScalarValue::Date32(20_001)),
                (
                    "event_ts".to_string(),
                    ScalarValue::TimestampMicros(1_700_000_000_001_000),
                ),
            ],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(
        report.column_family_summary(),
        "active:boolean,id:int64,count:uint64,metric:float64,label:utf8,event_day:date32,event_ts:timestamp_micros"
    );
    let schema = Schema::new(vec![
        Field::new("active", DataType::Boolean, true),
        Field::new("id", DataType::Int64, true),
        Field::new("count", DataType::UInt64, true),
        Field::new("metric", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("event_day", DataType::Date32, true),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let active = struct_array
        .column_by_name("active")
        .expect("active column")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean column");
    assert!(active.value(0));
    assert!(active.is_null(1));
    assert!(!active.value(2));
    assert_eq!(active.null_count(), 1);
    let id = struct_array
        .column_by_name("id")
        .expect("id column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(id.value(0), -10);
    assert!(id.is_null(1));
    assert_eq!(id.value(2), 20);
    let count = struct_array
        .column_by_name("count")
        .expect("count column")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("uint64 column");
    assert_eq!(count.value(0), 7);
    assert!(count.is_null(1));
    assert_eq!(count.value(2), 8);
    let metric = struct_array
        .column_by_name("metric")
        .expect("metric column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float64 column");
    assert!((metric.value(0) - 1.5).abs() < f64::EPSILON);
    assert!(metric.is_null(1));
    assert!((metric.value(2) - 2.5).abs() < f64::EPSILON);
    let label = struct_array
        .column_by_name("label")
        .expect("label column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 column");
    assert_eq!(label.value(0), "alpha");
    assert!(label.is_null(1));
    assert_eq!(label.value(2), "omega");
    let event_day = struct_array
        .column_by_name("event_day")
        .expect("event_day column")
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date32 column");
    assert_eq!(event_day.value(0), 20_000);
    assert!(event_day.is_null(1));
    assert_eq!(event_day.value(2), 20_001);
    let event_ts = struct_array
        .column_by_name("event_ts")
        .expect("event_ts column")
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("timestamp_micros column");
    assert_eq!(event_ts.value(0), 1_700_000_000_000_000);
    assert!(event_ts.is_null(1));
    assert_eq!(event_ts.value(2), 1_700_000_000_001_000);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_scalar_decimal_rows_write_reopens_decimal128() {
    use arrow_array::{Decimal128Array, StructArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-decimal-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "amount".to_string()],
        vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                (
                    "amount".to_string(),
                    ScalarValue::Decimal128 {
                        value: 1234,
                        precision: 10,
                        scale: 2,
                    },
                ),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(2)),
                (
                    "amount".to_string(),
                    ScalarValue::Decimal128 {
                        value: -800,
                        precision: 10,
                        scale: 2,
                    },
                ),
            ],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,amount:decimal128(10,2)"
    );
    assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "shardloom_scalar_rows_to_vortex_struct"
    );
    assert!(report.upstream_vortex_write_called);
    assert!(!report.upstream_vortex_scan_called);

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
    ]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let amount = struct_array
        .column_by_name("amount")
        .expect("amount column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal amount column");
    assert_eq!(amount.value(0), 1234);
    assert_eq!(amount.value(1), -800);
    assert_eq!(amount.null_count(), 0);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_scalar_all_null_decimal_rows_write_reopens_decimal128() {
    use arrow_array::{Array as _, Decimal128Array, StructArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-decimal-null-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["amount".to_string()],
        vec![
            vec![("amount".to_string(), ScalarValue::Null)],
            vec![("amount".to_string(), ScalarValue::Null)],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .column_dtypes(vec![Some(LogicalDType::Extension(
        "decimal128(10,2)".to_string(),
    ))]);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(report.column_family_summary(), "amount:decimal128(10,2)");
    let schema = Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        true,
    )]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let amount = struct_array
        .column_by_name("amount")
        .expect("amount column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal amount column");
    assert_eq!(amount.len(), 2);
    assert!(amount.is_null(0));
    assert!(amount.is_null(1));
    assert_eq!(amount.null_count(), 2);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_decimal_rows_reject_mixed_precision_scale() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-decimal-mixed-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["amount".to_string()],
        vec![
            vec![(
                "amount".to_string(),
                ScalarValue::Decimal128 {
                    value: 1234,
                    precision: 10,
                    scale: 2,
                },
            )],
            vec![(
                "amount".to_string(),
                ScalarValue::Decimal128 {
                    value: 1234,
                    precision: 12,
                    scale: 2,
                },
            )],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_scalar_vortex_prepared_state(request)
        .expect_err("mixed decimal precision/scale should block");

    assert!(
        error.to_string().contains(
            "local vortex_ingest column 'amount' mixes scalar families decimal128(10,2) and decimal128(12,2)"
        ),
        "{error}"
    );
    assert!(!path.exists());
}

#[test]
fn local_flat_scalar_rows_apply_layout_write_advisor_before_write() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-layout-advisor-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let advisor = evaluate_vortex_layout_write_advisor(layout_advisor_input(true, "none"));
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "label".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .layout_write_advisor(advisor);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert!(report.layout_write_decision.runtime_decision_applied);
    assert_eq!(
        report.layout_write_decision.selected_strategy,
        "single_vortex_artifact_embedded_olap_layout_statistics"
    );
    assert!(
        report
            .layout_write_decision
            .strategy_decision_digest
            .starts_with("fnv64:")
    );
    assert!(report.layout_write_decision.provider_admitted);
    assert_eq!(
        report.layout_write_decision.strategy_decision_digest,
        report.writer_physical_design.strategy_decision_digest
    );
    assert_eq!(report.writer_physical_design.status, "applied");
    assert_eq!(
        report.writer_physical_design.provider_decision,
        "use_shardloom_native_kernel_then_vortex_writer"
    );
    assert_eq!(
        report.writer_physical_design.source_stage_plan,
        "validated_flat_scalar_rows_already_materialized"
    );
    assert_eq!(report.writer_physical_design.array_build_prefetch_window, 0);
    assert_eq!(
        report.writer_physical_design.writer_row_block_size,
        VORTEX_PREPARED_OLAP_WRITER_FINE_ROW_BLOCK_SIZE
    );
    assert_eq!(
        report.writer_physical_design.writer_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.writer_physical_design.no_fallback_policy,
        "native_vortex_writer_provider_only_no_spark_datafusion_duckdb_polars_velox_fallback"
    );
    assert_eq!(report.layout_write_decision.blocker, "none");
    assert_eq!(
        report.writer_layout_strategy_applied,
        "vortex_write_strategy_row_block_4096_embedded_olap_layout_statistics"
    );
    assert_eq!(
        report.writer_layout_row_block_size,
        VORTEX_PREPARED_OLAP_WRITER_FINE_ROW_BLOCK_SIZE
    );
    assert_eq!(
        report.writer_layout_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.writer_profile_selection_reason,
        "small_source_fine_row_blocks_default_writer_profile"
    );
    assert_eq!(
        report.writer_profile_regression_guard,
        "small_source_default_profile_preserves_fixture_latency"
    );
    assert_eq!(report.reopen_row_count, 1);
    assert_ne!(
        report.prepared_olap_layout_inventory.root_layout_encoding,
        "not_available"
    );
    assert_ne!(
        report
            .prepared_olap_layout_inventory
            .layout_encoding_inventory,
        "not_available"
    );
    assert!(
        report
            .prepared_olap_layout_inventory
            .segment_membership_status
            .starts_with("segment_map_available")
    );
    assert_eq!(
        report
            .prepared_olap_layout_inventory
            .layout_reader_cache_status,
        "enabled_for_local_metadata_open"
    );
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn large_writer_summary_defers_layout_inventory_until_query_open() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-large-deferred-inventory-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    std::fs::write(&path, b"placeholder").expect("write placeholder artifact");
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
    advisor_input.source_byte_count = VORTEX_PREPARED_OLAP_LARGE_REOPEN_DEFER_BYTES;
    advisor_input.writer_parallelism_budget = 2;
    advisor_input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_time_counter_olap;layout_family=url_time_counter_dictionary_stats_layout;text_domain=true;time_bucket=true;counter=true;key_profile=high_cardinality_numeric_text_time_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
            .to_string();
    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);
    assert_eq!(
        advisor.writer_admission_policy,
        VORTEX_PRODUCT_LOCAL_INGEST_PREPARE_ONCE_ADMISSION_POLICY
    );
    let decision = VortexLayoutWriteRuntimeDecision::applied(
        &advisor,
        &path,
        "vortex_array_kernel",
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator",
        VortexIngestCertificationLevel::IngestCertified,
    );

    assert!(should_defer_prepared_vortex_artifact_layout_inventory(
        VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD,
        VORTEX_PREPARED_OLAP_LARGE_REOPEN_DEFER_BYTES,
        &decision
    ));
    let inventory = VortexPreparedOlapLayoutInventory::deferred_from_writer_summary(
        &path,
        VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD,
        VORTEX_PREPARED_OLAP_LARGE_REOPEN_DEFER_BYTES,
        &decision,
    );

    assert!(inventory.is_admitted());
    assert_eq!(
        inventory.status,
        VORTEX_PREPARED_OLAP_LAYOUT_INVENTORY_DEFERRED_STATUS
    );
    assert_eq!(
        inventory.row_count,
        Some(VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD)
    );
    assert_eq!(
        inventory.layout_reader_cache_status,
        "deferred_for_large_artifact_public_prepare"
    );
    assert!(inventory.metadata_persisted_in_artifact);
    let primitive = VortexSegmentMetadataPrimitiveReport::from_inventory(
        &inventory,
        "test_large_writer_summary_deferred",
        decision
            .writer_physical_design
            .strategy_decision_digest
            .clone(),
    );
    assert_eq!(
        primitive.status,
        "deferred_until_query_open_writer_summary_row_count_only"
    );
    assert!(primitive.requires_query_open());
    assert!(primitive.is_metadata_count_admitted());
    assert!(!primitive.is_predicate_pruning_admitted());
    assert_eq!(
        primitive.admission_for_query_family(VortexSegmentMetadataQueryFamily::PredicatePruning),
        "deferred_until_query_open"
    );
    assert_eq!(primitive.segment_count_field(), "unknown");
    assert!(!primitive.fallback_attempted);
    assert!(!primitive.external_engine_invoked);
    assert!(reopen_verification_is_certified(
        VORTEX_PREPARED_OLAP_REOPEN_DEFERRED_STATUS
    ));
    assert!(reopen_verification_uses_writer_summary(
        VORTEX_PREPARED_OLAP_REOPEN_DEFERRED_STATUS
    ));

    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn source_text_large_source_writer_profile_uses_advisor_candidate_fields() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-layout-advisor-field-list-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
    advisor_input.source_byte_count = 1_073_741_824;
    advisor_input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_time_counter_olap;layout_family=url_time_counter_dictionary_stats_layout;text_domain=true;time_bucket=true;counter=true;key_profile=high_cardinality_numeric_text_time_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
            .to_string();
    advisor_input.writer_compression_candidate_fields = vec![
        "URL".to_string(),
        "Title".to_string(),
        "__shardloom_derived_utf8_len_URL".to_string(),
        "Params".to_string(),
        "URL".to_string(),
    ];
    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);
    assert_eq!(
        advisor.writer_admission_policy,
        VORTEX_PRODUCT_LOCAL_INGEST_PREPARE_ONCE_ADMISSION_POLICY
    );
    let decision = VortexLayoutWriteRuntimeDecision::applied(
        &advisor,
        &path,
        "vortex_array_kernel",
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator",
        VortexIngestCertificationLevel::IngestCertified,
    );

    assert_eq!(
        decision.writer_compression_field_names,
        vec!["Params".to_string(), "Title".to_string(), "URL".to_string()]
    );
    assert_eq!(decision.writer_compression_field_decisions.len(), 4);
    assert!(
        decision
            .writer_compression_field_decisions
            .iter()
            .any(|decision| decision.contains(
                "field=URL;decision=compress;codec=zstd;level=-3;reason=large_source_artifact_size_guard_query_hot_text"
            ))
    );
    assert!(
        decision
            .writer_compression_field_decisions
            .iter()
            .any(|decision| decision.contains(
                "field=Title;decision=compress;codec=zstd;level=-3;reason=large_source_artifact_size_guard_query_hot_text"
            ))
    );
    assert!(
        decision
            .writer_compression_field_decisions
            .iter()
            .any(|decision| decision.contains(
                "field=Params;decision=compress;codec=zstd;level=-3;reason=payload_text_storage_benefit"
            ))
    );
    assert!(
        decision
            .writer_compression_field_decisions
            .iter()
            .any(|decision| decision
                .contains("field=__shardloom_derived_utf8_len_URL;decision=skip;codec=none"))
    );
}

#[test]
fn layout_writer_rejects_legacy_scoped_public_admission_policy() {
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.writer_admission_policy = "scoped_local_vortex_ingest_prepare_once".to_string();

    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);

    assert_eq!(
        advisor.status,
        "vortex_layout_write_advisor.unsupported_writer_admission_policy"
    );
    assert_eq!(
        advisor.blocker,
        "vortex_layout_write_advisor.unsupported_writer_admission_policy"
    );
    assert!(!advisor.provider_admitted);
}

#[test]
fn local_flat_scalar_rows_use_source_text_large_source_layout_row_blocks_when_advised() {
    let applied = shardloom_exec::compute_pool::bounded_cpu_parallelism(2);
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-layout-advisor-large-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
    advisor_input.source_byte_count = 1_073_741_824;
    advisor_input.writer_parallelism_budget = 2;
    advisor_input.writer_compression_candidate_fields = vec!["payload".to_string()];
    advisor_input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_time_counter_olap;layout_family=url_time_counter_dictionary_stats_layout;text_domain=true;time_bucket=true;counter=true;key_profile=high_cardinality_numeric_text_time_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
            .to_string();
    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "payload".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            (
                "payload".to_string(),
                ScalarValue::Utf8("alpha".to_string()),
            ),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .layout_write_advisor(advisor);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert!(report.layout_write_decision.runtime_decision_applied);
    assert_eq!(
        report.layout_write_decision.writer_row_block_size,
        VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_BLOCK_SIZE
    );
    assert_eq!(
        report.layout_write_decision.writer_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.layout_write_decision.writer_compression_policy,
        VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_LARGE_SOURCE_COMPRESSION_POLICY
    );
    assert_eq!(
        report.layout_write_decision.writer_compression_concurrency,
        2
    );
    assert_eq!(report.layout_write_decision.writer_stats_concurrency, 2);
    assert_eq!(
        report.writer_layout_strategy_applied,
        "vortex_write_strategy_row_block_262144_target_1mb_source_text_fast_zstd_no_dict_numeric_btrblocks_embedded_olap_layout_statistics"
    );
    assert_eq!(
        report.writer_layout_row_block_size,
        VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_BLOCK_SIZE
    );
    assert_eq!(
        report.writer_layout_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.writer_compression_policy,
        VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_LARGE_SOURCE_COMPRESSION_POLICY
    );
    assert_eq!(report.writer_compression_field_count(), 1);
    assert_eq!(report.writer_compression_field_names(), "payload");
    assert_eq!(report.writer_compression_decision_count(), 1);
    assert!(
        report
            .writer_compression_decisions()
            .contains("field=payload;decision=compress;codec=zstd")
    );
    assert!(
        report
            .encoding_summary()
            .contains("compression_field_count=1")
    );
    assert!(
        report
            .encoding_summary()
            .contains("compression_decision_count=1")
    );
    assert_eq!(report.writer_runtime_requested_parallelism, 2);
    assert_eq!(report.writer_runtime_applied_parallelism, applied);
    assert_eq!(report.writer_runtime_background_workers, applied - 1);
    assert_eq!(report.writer_runtime_kind, writer_runtime_kind(applied - 1));
    assert_eq!(report.writer_compression_concurrency, applied);
    assert_eq!(report.writer_stats_concurrency, applied);
    assert_eq!(
        report.writer_profile_selection_reason,
        "large_text_source_fast_zstd_profile"
    );
    assert_eq!(
        report.writer_profile_regression_guard,
        "text_zstd_policy_prior_uat_backed_numeric_btrblocks_requires_lifecycle_acceptance"
    );
    assert_eq!(report.reopen_row_count, 1);
    let fields: BTreeMap<_, _> = report.stage_work.evidence_fields().into_iter().collect();
    assert_eq!(fields["vortex_ingest_text_packing_completed_calls"], "1");
    assert_eq!(fields["vortex_ingest_text_packing_valid_values"], "1");
    assert_eq!(fields["vortex_ingest_text_packing_payload_bytes"], "9");
    assert_eq!(
        fields["vortex_ingest_text_packing_frame_index_entries"],
        "1"
    );
    assert_eq!(
        fields["vortex_ingest_text_packing_frame_index_bytes"],
        size_of::<usize>().to_string()
    );
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_rows_coalesce_very_large_high_cardinality_text_blocks() {
    let applied = shardloom_exec::compute_pool::bounded_cpu_parallelism(2);
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-layout-advisor-very-large-text-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.row_count = 100_000_000;
    advisor_input.source_byte_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_TEXT_COALESCED_SOURCE_BYTES;
    advisor_input.writer_parallelism_budget = 2;
    advisor_input.writer_compression_candidate_fields = vec!["URL".to_string()];
    advisor_input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_time_counter_olap;layout_family=url_time_counter_dictionary_stats_layout;text_domain=true;time_bucket=true;counter=true;key_profile=high_cardinality_numeric_text_time_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
            .to_string();
    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "URL".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            (
                "URL".to_string(),
                ScalarValue::Utf8("https://example.test".to_string()),
            ),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .layout_write_advisor(advisor);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(
        report.layout_write_decision.writer_row_block_size,
        VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_BLOCK_SIZE
    );
    assert_eq!(
        report.layout_write_decision.writer_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_FAST_LOAD_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.writer_layout_strategy_applied,
        "vortex_write_strategy_row_block_262144_target_8mb_source_text_fast_zstd_no_dict_numeric_btrblocks_embedded_olap_layout_statistics"
    );
    assert_eq!(report.writer_compression_concurrency, applied);
    assert_eq!(report.writer_compression_field_count(), 1);
    assert_eq!(report.writer_compression_field_names(), "URL");
    assert!(
        report
            .writer_compression_decisions()
            .contains("field=URL;decision=compress;codec=zstd;level=-3;reason=large_source_artifact_size_guard_query_hot_text")
    );
    assert_eq!(report.writer_runtime_requested_parallelism, 2);
    assert_eq!(report.writer_runtime_applied_parallelism, applied);
    assert_eq!(report.writer_runtime_background_workers, applied - 1);
    assert_eq!(report.writer_stats_concurrency, applied);
    assert_eq!(
        report.writer_layout_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_FAST_LOAD_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.writer_profile_selection_reason,
        "large_text_high_cardinality_source_coalesced_fast_zstd_profile"
    );
    assert_eq!(
        report.writer_profile_regression_guard,
        "text_zstd_policy_prior_uat_backed_numeric_btrblocks_requires_lifecycle_acceptance"
    );
    assert_eq!(report.reopen_row_count, 1);
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_rows_use_fast_load_large_source_layout_when_not_text_domain() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-layout-advisor-large-numeric-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
    advisor_input.source_byte_count = 1_073_741_824;
    advisor_input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=counter_olap;layout_family=counter_typed_stats_layout;text_domain=false;time_bucket=false;counter=true;key_profile=high_cardinality_numeric_keys;dictionary=columnar_dictionary_status_provider_dependent"
            .to_string();
    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "metric".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            ("metric".to_string(), ScalarValue::Float64(2.5)),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .layout_write_advisor(advisor);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert!(report.layout_write_decision.runtime_decision_applied);
    assert_eq!(
        report.layout_write_decision.writer_row_block_size,
        VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_BLOCK_SIZE
    );
    assert_eq!(
        report.layout_write_decision.writer_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_FAST_LOAD_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.layout_write_decision.writer_compression_policy,
        VORTEX_PREPARED_OLAP_WRITER_FAST_LOAD_LARGE_SOURCE_COMPRESSION_POLICY
    );
    assert_eq!(
        report.layout_write_decision.writer_compression_concurrency,
        VORTEX_PREPARED_OLAP_WRITER_FAST_LOAD_LARGE_SOURCE_COMPRESSION_CONCURRENCY
    );
    assert_eq!(
        report.writer_layout_strategy_applied,
        "vortex_write_strategy_row_block_262144_target_8mb_fast_load_numeric_btrblocks_embedded_olap_layout_statistics"
    );
    assert_eq!(
        report.writer_layout_block_target_bytes,
        VORTEX_PREPARED_OLAP_WRITER_FAST_LOAD_BLOCK_TARGET_BYTES
    );
    assert_eq!(
        report.writer_profile_selection_reason,
        "large_non_text_source_fast_load_numeric_btrblocks_layout_statistics"
    );
    assert_eq!(
        report.writer_profile_regression_guard,
        "non_text_large_source_numeric_btrblocks_requires_lifecycle_acceptance"
    );
    assert_eq!(report.reopen_row_count, 1);
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_rows_block_unsupported_layout_strategy_before_write() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-layout-advisor-blocked-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let advisor = evaluate_vortex_layout_write_advisor(layout_advisor_input(
        false,
        "vortex_layout_write_advisor.unsupported_layout_strategy",
    ));
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "label".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .layout_write_advisor(advisor);

    let error = write_flat_scalar_vortex_prepared_state(request)
        .expect_err("unsupported layout strategy must block");

    assert!(
        error.to_string().contains(
            "vortex_layout_write_advisor.status_not_admitted:blocked_layout_write_strategy"
        )
    );
    assert!(
        !path.exists(),
        "blocked layout/write advisor must not create {}",
        path.display()
    );
}

#[test]
fn local_flat_scalar_minimal_ingest_reads_layout_without_reopen_scan() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-minimal-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "label".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .certification_level(VortexIngestCertificationLevel::IngestMinimal);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 1);
    assert_eq!(report.writer_row_count, 1);
    assert_eq!(report.reopen_row_count, 1);
    assert_eq!(report.reopen_scan_micros, 0);
    assert_eq!(
        report.reopen_verification_status,
        "not_performed_ingest_minimal"
    );
    assert_eq!(report.certification_level, "ingest_minimal");
    assert!(report.upstream_vortex_write_called);
    assert!(!report.upstream_vortex_scan_called);
    assert_ne!(
        report.prepared_olap_layout_inventory.root_layout_encoding,
        "not_available"
    );
    assert!(
        report
            .prepared_olap_layout_inventory
            .layout_reader_cache_status
            .contains("enabled")
    );
    assert_eq!(
        report.preparation_spine.native_io_certificate_status,
        "minimal_local_vortex_preparation_spine_digest_only"
    );
    assert_eq!(
        report.preparation_spine.reopen_provider_surface,
        "not_invoked_ingest_minimal"
    );
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[test]
fn local_flat_scalar_rows_validate_manifest_without_claiming_scheduler_execution() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-capillary-prewrite-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "label".to_string()],
        vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(2)),
                ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
            ],
        ],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 2, 2));

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert!(report.capillary_prewrite_control.task_manifest_admitted);
    assert!(!report.capillary_prewrite_control.scheduler_applied);
    assert_eq!(
        report.capillary_prewrite_control.status,
        "task_manifest_admitted_before_array_build"
    );
    assert_eq!(
        report.capillary_prewrite_control.array_build_gate_status,
        "validated_task_role"
    );
    assert_eq!(
        report.capillary_prewrite_control.write_gate_status,
        "validated_task_role"
    );
    assert_eq!(
        report.capillary_prewrite_control.reopen_gate_status,
        "validated_task_role"
    );
    assert_eq!(
        report.capillary_prewrite_control.sink_evidence_gate_status,
        "validated_task_role"
    );
    assert_eq!(report.capillary_prewrite_control.execution_window_count, 0);
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_columnar_source_writes_and_reopens_vortex_artifact() {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec![
        "id".to_string(),
        "label".to_string(),
        "metric".to_string(),
        "active".to_string(),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("metric", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![None; columns.len()],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 2,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 2, 4));

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,label:utf8,metric:float64,active:boolean"
    );
    assert_eq!(
        report.reopen_verification_status,
        "reopen_metadata_row_count_verified"
    );
    assert!(report.upstream_vortex_write_called);
    assert!(!report.upstream_vortex_scan_called);
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert_eq!(
        report.array_build_strategy,
        "vortex_from_arrow_record_batch"
    );
    assert_eq!(
        report.array_build_input_layout,
        "arrow_record_batch_columnar_source_state"
    );
    assert_eq!(report.array_build_record_batch_count, 1);
    assert_eq!(report.stream_source_pull_micros, 0);
    assert_eq!(report.stream_array_convert_micros, 0);
    assert_eq!(
        report.stream_timing_split_status,
        "not_streaming_buffered_or_scalar_vortex_prepare"
    );
    assert_eq!(
        report.writer_coalescing_policy_status,
        "upstream_vortex_default_writer_coalescing_policy"
    );
    assert!(report.manual_scalar_copy_avoided);
    assert_eq!(
        report.preparation_spine.vortex_first_decision,
        "use_vortex_native_provider"
    );
    assert_eq!(
        report.preparation_spine.provider_kind,
        "vortex_array_kernel"
    );
    assert_eq!(report.preparation_spine.provider_crate, "vortex");
    assert_eq!(
        report.preparation_spine.provider_version,
        VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION
    );
    assert_eq!(
        report.preparation_spine.source_surface,
        "local_columnar_source_state_arrow_record_batches"
    );
    assert_eq!(report.preparation_spine.split_count, 1);
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "columnar_source_state_preserved_to_vortex_array_provider"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "no_scalar_row_decode_for_non_empty_batches"
    );
    assert!(report.capillary_prewrite_control.task_manifest_admitted);
    assert!(!report.capillary_prewrite_control.scheduler_applied);
    assert_eq!(
        report.capillary_prewrite_control.array_build_gate_status,
        "validated_task_role"
    );
    assert_eq!(
        report.capillary_prewrite_control.write_gate_status,
        "validated_task_role"
    );
    assert_eq!(
        report.capillary_prewrite_control.reopen_gate_status,
        "validated_task_role"
    );
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_columnar_stream_source_writes_without_buffered_batch_source() {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

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

    impl arrow_array::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-stream-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "label".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch_1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        ],
    )
    .expect("first record batch");
    let batch_2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["gamma"])),
        ],
    )
    .expect("second record batch");
    let reader = TestRecordBatchReader {
        schema,
        batches: VecDeque::from([batch_1, batch_2]),
    };
    let source = FlatLocalColumnarStreamSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        row_count_hint: Some(3),
        record_batch_count_hint: Some(2),
        source_stream_batch_size: 0,
        source_stream_unit_count_hint: Some(2),
        source_stream_unit_row_ranges: Some(vec![(0, 2), (2, 3)]),
        source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
        source_stream_policy: "test_source_defined_record_batches".to_string(),
        source_dictionary_preservation_status: "test_not_applicable".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(2),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros:
            crate::universal_format_io::new_embedded_derived_build_micros_counter(),
        reader: Box::new(reader),
    };
    let request = VortexPreparedStateColumnarStreamWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 3, 2));

    let report =
        write_flat_columnar_vortex_prepared_state_streaming(request).expect("stream report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(report.array_build_record_batch_count, 2);
    let stage_fields: BTreeMap<_, _> = report.stage_work.evidence_fields().into_iter().collect();
    assert_eq!(stage_fields["vortex_ingest_stream_validation_calls"], "2");
    assert_eq!(
        stage_fields["vortex_ingest_stream_arrow_conversion_calls"],
        "2"
    );
    assert_eq!(
        stage_fields["vortex_ingest_stream_arrow_conversion_rows"],
        "3"
    );
    assert_eq!(
        stage_fields["vortex_ingest_stream_arrow_ownership_calls"],
        "2"
    );
    assert_eq!(
        stage_fields["vortex_ingest_stream_arrow_ownership_rows"],
        "3"
    );
    assert_eq!(stage_fields["vortex_ingest_ownership_buffer_regions"], "6");
    // Two numeric value buffers, two UTF-8 offset buffers and all 14 text
    // bytes cross the real writer's credited-copy boundary.
    assert_eq!(
        stage_fields["vortex_ingest_ownership_copied_buffer_bytes"],
        "58"
    );
    assert_eq!(
        stage_fields["vortex_ingest_ownership_copied_allocation_bytes"],
        (58 + 6 * vortex::buffer::Alignment::DEFAULT_ALIGNMENT.as_usize()).to_string()
    );
    assert_eq!(
        stage_fields["vortex_ingest_ownership_repeated_buffer_bytes"],
        "0"
    );
    assert_eq!(
        stage_fields["vortex_ingest_identity_projection_batches"],
        "2"
    );
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator"
    );
    assert_eq!(
        report.array_build_strategy,
        "vortex_from_arrow_record_batch_stream"
    );
    assert_eq!(report.array_build_prefetch_window, 0);
    assert_eq!(
        report.array_build_input_layout,
        "streaming_arrow_record_batch_columnar_source_state"
    );
    assert_eq!(
        report.stream_timing_split_status,
        "streaming_source_pull_decode_derive_and_arrow_to_vortex_convert_timing_recorded"
    );
    assert!(
        report.stream_source_pull_micros > 0,
        "streaming source pull timing should be recorded"
    );
    assert!(
        report.stream_array_convert_micros > 0,
        "streaming Arrow-to-Vortex conversion timing should be recorded"
    );
    assert_eq!(
        report.stream_derived_metadata_build_micros, 0,
        "no embedded derived columns were attached in this stream"
    );
    assert_eq!(
        report.writer_coalescing_policy_status,
        "native_within_source_batch_only;cross_batch_coalescing_disabled"
    );
    assert_eq!(
        report.preparation_spine.source_surface,
        "streaming_local_columnar_source_state_arrow_record_batches"
    );
    assert_eq!(
        report.preparation_spine.split_surface,
        "streaming_arrow_record_batch_source_splits"
    );
    assert_eq!(report.preparation_spine.split_count, 2);
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "streaming_columnar_source_state_preserved_to_vortex_array_provider"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "no_scalar_row_decode_for_streamed_batches"
    );
    assert_eq!(report.column_family_summary(), "id:int64,label:utf8");
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn streaming_vortex_array_prefetch_window_is_bounded_by_parallelism() {
    use crate::ingest_cpu_lanes::IngestCpuLanes;
    for (requested, expected) in [(0, 0), (1, 0), (2, 0), (4, 3), (16, 4)] {
        let lanes = IngestCpuLanes::pipeline(requested, 8);
        assert_eq!(lanes.prefetch_slots(), expected);
        assert!(lanes.prefetch_slots() <= VORTEX_STREAM_ARRAY_PREFETCH_MAX_WINDOW);
        assert_eq!(lanes.configured_cpu_lanes(), requested.max(1));
    }
    assert_eq!(
        IngestCpuLanes::pipeline(2, 0).prefetch_slots(),
        1,
        "a synchronous source leaves one conversion lane"
    );
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_stream_source_prefetches_vortex_arrays_when_parallelism_available() {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

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

    impl arrow_array::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-stream-prefetch-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "label".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch_1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        ],
    )
    .expect("first record batch");
    let batch_2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["gamma"])),
        ],
    )
    .expect("second record batch");
    let source = FlatLocalColumnarStreamSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        row_count_hint: Some(3),
        record_batch_count_hint: Some(2),
        source_stream_batch_size: 0,
        source_stream_unit_count_hint: Some(2),
        source_stream_unit_row_ranges: Some(vec![(0, 2), (2, 3)]),
        source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
        source_stream_policy: "test_source_defined_record_batches".to_string(),
        source_dictionary_preservation_status: "test_not_applicable".to_string(),
        ingest_executor_status: "serial_pull_reader_writer_slot_reserved".to_string(),
        ingest_executor_kind: "single_source_record_batch_reader_with_writer_slot_reserved"
            .to_string(),
        ingest_executor_requested_parallelism: 2,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(2),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros:
            crate::universal_format_io::new_embedded_derived_build_micros_counter(),
        reader: Box::new(TestRecordBatchReader {
            schema,
            batches: VecDeque::from([batch_1, batch_2]),
        }),
    };
    let request = VortexPreparedStateColumnarStreamWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 3, 2));

    let report =
        write_flat_columnar_vortex_prepared_state_streaming(request).expect("stream report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(report.array_build_record_batch_count, 2);
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch);capillary_vortex_array_prefetch_window;streaming ArrayIterator"
    );
    assert_eq!(
        report.array_build_strategy,
        "capillary_vortex_array_prefetch_window_from_arrow_record_batch_stream"
    );
    assert_eq!(report.array_build_prefetch_window, 1);
    assert_eq!(
        report.writer_physical_design.provider_decision,
        "use_vortex_native_provider"
    );
    assert_eq!(
        report.writer_physical_design.source_stage_plan,
        "bounded_streaming_source_reader_feeds_vortex_array_iterator"
    );
    assert_eq!(
        report.writer_physical_design.array_build_stage_plan,
        "prefetch_streaming_arrow_record_batches_as_vortex_arrays_before_writer_pull"
    );
    assert_eq!(
        report.writer_physical_design.array_build_prefetch_window,
        report.array_build_prefetch_window
    );
    assert_eq!(
        report.writer_physical_design.writer_backpressure_policy,
        "reserved_inflight_window_bounds_queued_active_and_ordered_results"
    );
    assert!(report.writer_physical_design.evidence_fields().contains(&(
        "vortex_writer_physical_design_external_engine_invoked".to_string(),
        "false".to_string()
    )));
    assert_eq!(
        report.array_build_input_layout,
        "streaming_arrow_record_batch_columnar_source_state"
    );
    assert_eq!(report.column_family_summary(), "id:int64,label:utf8");
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn streaming_vortex_write_preserves_compact_embedded_derived_columns() {
    use arrow_array::{
        Array as _, ArrayAccessor as _, DictionaryArray, StringArray, UInt8Array, UInt32Array,
        types::Int32Type,
    };
    use arrow_schema::DataType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-embedded-derived-stream-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let header = vec![
        "URL".to_string(),
        "Referer".to_string(),
        "SearchPhrase".to_string(),
        "EventTime".to_string(),
    ];
    let rows = vec![
        vec![
            (
                "URL".to_string(),
                ScalarValue::Utf8("https://shardloom.io/docs".to_string()),
            ),
            (
                "Referer".to_string(),
                ScalarValue::Utf8("http://www.google.com/search".to_string()),
            ),
            ("SearchPhrase".to_string(), ScalarValue::Utf8(String::new())),
            ("EventTime".to_string(), ScalarValue::UInt64(61)),
        ],
        vec![
            (
                "URL".to_string(),
                ScalarValue::Utf8("https://shardloom.io/blog".to_string()),
            ),
            (
                "Referer".to_string(),
                ScalarValue::Utf8("https://docs.rs/crate".to_string()),
            ),
            (
                "SearchPhrase".to_string(),
                ScalarValue::Utf8("rust".to_string()),
            ),
            ("EventTime".to_string(), ScalarValue::UInt64(120)),
        ],
    ];
    let source = crate::universal_format_io::stream_flat_text_rows_columnar_source(
        header.clone(),
        vec![None, None, None, None],
        vec![None, None, None, None],
        header.clone(),
        header,
        rows,
        64,
        "JSONL",
    )
    .expect("text stream with embedded derived columns");
    let schema = source.reader.schema();
    assert!(
        schema
            .field_with_name("__shardloom_derived_utf8_len_Referer")
            .is_ok()
    );
    assert_eq!(
        schema
            .field_with_name("__shardloom_derived_utf8_len_Referer")
            .expect("referer len")
            .data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::UInt32))
    );
    assert_eq!(
        schema
            .field_with_name("__shardloom_derived_url_domain_Referer")
            .expect("referer domain")
            .data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    );
    assert_eq!(
        schema
            .field_with_name("__shardloom_derived_extract_minute_EventTime")
            .expect("event minute")
            .data_type(),
        &DataType::UInt8
    );
    assert_eq!(
        schema
            .field_with_name("__shardloom_derived_date_trunc_minute_EventTime")
            .expect("event minute bucket")
            .data_type(),
        &DataType::Int64
    );
    let request = VortexPreparedStateColumnarStreamWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 2, 1));

    let report =
        write_flat_columnar_vortex_prepared_state_streaming(request).expect("stream report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert!(
        report.stream_derived_metadata_build_micros > 0,
        "embedded derived metadata build timing should be recorded"
    );
    assert_eq!(
        report
            .prepared_olap_layout_inventory
            .domain_dictionary_status,
        "embedded_utf8_domain_columns_present_with_length_stats_dictionary_layout_not_observed"
    );
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .expect("arrow struct");
    let referer_lengths = struct_array
        .column_by_name("__shardloom_derived_utf8_len_Referer")
        .expect("referer length column")
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("referer length dictionary");
    let referer_lengths = referer_lengths
        .downcast_dict::<UInt32Array>()
        .expect("referer length dictionary values");
    assert_eq!(
        referer_lengths.value(0),
        u32::try_from("http://www.google.com/search".len()).expect("len")
    );
    assert_eq!(
        referer_lengths.value(1),
        u32::try_from("https://docs.rs/crate".len()).expect("len")
    );
    let referer_domains = struct_array
        .column_by_name("__shardloom_derived_url_domain_Referer")
        .expect("referer domain column")
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("referer domain dictionary");
    let referer_domains = referer_domains
        .downcast_dict::<StringArray>()
        .expect("referer domain dictionary values");
    assert_eq!(referer_domains.value(0), "google.com");
    assert_eq!(referer_domains.value(1), "docs.rs");
    let event_minutes = struct_array
        .column_by_name("__shardloom_derived_extract_minute_EventTime")
        .expect("event minute column")
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("event minute uint8");
    assert_eq!(event_minutes.value(0), 1);
    assert_eq!(event_minutes.value(1), 2);
    let event_minute_buckets = struct_array
        .column_by_name("__shardloom_derived_date_trunc_minute_EventTime")
        .expect("event minute bucket column")
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("event minute bucket int64");
    assert_eq!(event_minute_buckets.value(0), 60);
    assert_eq!(event_minute_buckets.value(1), 120);
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn streaming_vortex_write_preserves_dictionary_backed_embedded_length_metadata() {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow_array::{
        Array as _, ArrayAccessor as _, ArrayRef, DictionaryArray, RecordBatch, StringArray,
        UInt32Array, builder::StringDictionaryBuilder, types::UInt8Type,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

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

    impl arrow_array::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-dictionary-derived-length-stream-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
    builder
        .append("https://example.com/a")
        .expect("append example");
    builder
        .append("https://docs.rs/crate")
        .expect("append docs");
    builder
        .append("https://example.com/a")
        .expect("append example repeat");
    builder.append_null();
    let url_dictionary = Arc::new(builder.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "URL",
        url_dictionary.data_type().clone(),
        true,
    )]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![url_dictionary]).expect("dictionary batch");
    let source = FlatLocalColumnarStreamSource {
        header: vec!["URL".to_string()],
        column_dtypes: vec![Some(shardloom_core::LogicalDType::Utf8)],
        column_arrow_dtypes: vec![Some(schema.field(0).data_type().clone())],
        materialized_columns: vec!["URL".to_string()],
        reader_projection_columns: vec!["URL".to_string()],
        row_count_hint: Some(4),
        record_batch_count_hint: Some(1),
        source_stream_batch_size: 4,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_row_ranges: Some(vec![(0, 4)]),
        source_stream_unit_hint_kind: "test_dictionary_record_batch".to_string(),
        source_stream_policy: "test_dictionary_source_native_units".to_string(),
        source_dictionary_preservation_status: "test_dictionary_preservation_available".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_dictionary_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros:
            crate::universal_format_io::new_embedded_derived_build_micros_counter(),
        reader: Box::new(TestRecordBatchReader {
            schema,
            batches: VecDeque::from([batch]),
        }),
    };
    let source =
        crate::universal_format_io::with_source_native_embedded_derived_columns_columnar_stream_source(source);
    let schema = source.reader.schema();
    assert_eq!(
        schema
            .field_with_name("__shardloom_derived_utf8_len_URL")
            .expect("length field")
            .data_type(),
        &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32))
    );
    assert_eq!(
        schema
            .field_with_name("__shardloom_derived_url_domain_URL")
            .expect("domain field")
            .data_type(),
        &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
    );
    let request = VortexPreparedStateColumnarStreamWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 2, 1));

    let report =
        write_flat_columnar_vortex_prepared_state_streaming(request).expect("stream report");

    assert_eq!(report.row_count, 4);
    assert_eq!(report.reopen_row_count, 4);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .expect("arrow struct");
    let lengths = struct_array
        .column_by_name("__shardloom_derived_utf8_len_URL")
        .expect("length column")
        .as_any()
        .downcast_ref::<DictionaryArray<UInt8Type>>()
        .expect("u8 length dictionary");
    let lengths = lengths
        .downcast_dict::<UInt32Array>()
        .expect("length values");
    assert_eq!(
        lengths.value(0),
        u32::try_from("https://example.com/a".len()).expect("len")
    );
    assert_eq!(
        lengths.value(1),
        u32::try_from("https://docs.rs/crate".len()).expect("len")
    );
    assert_eq!(lengths.value(2), lengths.value(0));
    assert!(lengths.is_null(3));
    let domains = struct_array
        .column_by_name("__shardloom_derived_url_domain_URL")
        .expect("domain column")
        .as_any()
        .downcast_ref::<DictionaryArray<UInt8Type>>()
        .expect("u8 domain dictionary");
    let domains = domains
        .downcast_dict::<StringArray>()
        .expect("domain values");
    assert_eq!(domains.value(0), "example.com");
    assert_eq!(domains.value(1), "docs.rs");
    assert_eq!(domains.value(2), "example.com");
    assert!(domains.is_null(3));
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_empty_flat_columnar_stream_source_uses_admitted_native_array_provider() {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

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

    impl arrow_array::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-empty-columnar-stream-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "label".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let reader = TestRecordBatchReader {
        schema,
        batches: VecDeque::new(),
    };
    let source = FlatLocalColumnarStreamSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        row_count_hint: Some(0),
        record_batch_count_hint: Some(0),
        source_stream_batch_size: 0,
        source_stream_unit_count_hint: Some(0),
        source_stream_unit_row_ranges: Some(Vec::new()),
        source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
        source_stream_policy: "test_source_defined_record_batches".to_string(),
        source_dictionary_preservation_status: "test_not_applicable".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(0),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros:
            crate::universal_format_io::new_embedded_derived_build_micros_counter(),
        reader: Box::new(reader),
    };
    let mut advisor_input = layout_advisor_input(true, "none");
    advisor_input.row_count = 0;
    advisor_input.writer_provider_kind = "vortex_array_kernel".to_string();
    advisor_input.writer_provider_surface =
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)"
            .to_string();
    advisor_input.materialization_boundary_status =
        "empty_columnar_source_state_preserved_to_vortex_struct".to_string();
    advisor_input.decode_boundary_status =
        "no_scalar_row_decode_for_empty_columnar_source".to_string();
    let advisor = evaluate_vortex_layout_write_advisor(advisor_input);
    let request = VortexPreparedStateColumnarStreamWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .layout_write_advisor(advisor)
    .capillary_prewrite_input(capillary_prewrite_test_input(&path, 0, 0));

    let report =
        write_flat_columnar_vortex_prepared_state_streaming(request).expect("stream report");

    assert_eq!(report.row_count, 0);
    assert_eq!(report.reopen_row_count, 0);
    assert_eq!(report.array_build_record_batch_count, 1);
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert!(
        report
            .array_build_provider_surface
            .starts_with("ArrayRef::from_arrow(RecordBatch)")
    );
    assert_eq!(
        report.preparation_spine.source_surface,
        "streaming_local_columnar_source_state_arrow_record_batches"
    );
    let memory = report
        .shared_native_memory
        .expect("empty input owns its declared grant");
    assert!(memory.peak_reserved_bytes > 0);
    assert_eq!(memory.final_reserved_bytes, 0);
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_binary_source_writes_reopens_exact_bytes() {
    use std::sync::Arc;

    use arrow_array::{BinaryArray, Int64Array, RecordBatch, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-binary-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "payload".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0x00, 0xff, 0x10][..]),
                Some(&[][..]),
                Some(&b"raw"[..]),
            ])),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![None, Some(LogicalDType::Binary)],
        column_arrow_dtypes: vec![None, Some(arrow_schema::DataType::Binary)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 3,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(report.column_family_summary(), "id:int64,payload:binary");
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "columnar_source_state_preserved_to_vortex_array_provider"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "no_scalar_row_decode_for_non_empty_batches"
    );
    assert!(report.manual_scalar_copy_avoided);

    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let payload = struct_array
        .column_by_name("payload")
        .expect("payload column")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary payload column");
    assert_eq!(payload.value(0), &[0x00, 0xff, 0x10]);
    assert_eq!(payload.value(1), &[] as &[u8]);
    assert_eq!(payload.value(2), b"raw");
    assert_eq!(payload.null_count(), 0);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::float_cmp, clippy::too_many_lines)]
fn local_flat_columnar_nullable_source_writes_reopens_validity() {
    use std::sync::Arc;

    use arrow_array::{
        Array as _, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-nullable-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec![
        "id".to_string(),
        "label".to_string(),
        "metric".to_string(),
        "active".to_string(),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("metric", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.5)])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![
            Some(LogicalDType::Int64),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Float64),
            Some(LogicalDType::Boolean),
        ],
        column_arrow_dtypes: vec![
            Some(DataType::Int64),
            Some(DataType::Utf8),
            Some(DataType::Float64),
            Some(DataType::Boolean),
        ],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 3,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.reopen_row_count, 3);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,label:utf8,metric:float64,active:boolean"
    );
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "columnar_source_state_preserved_to_vortex_array_provider"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "no_scalar_row_decode_for_non_empty_batches"
    );
    assert!(report.manual_scalar_copy_avoided);
    assert!(!report.preparation_spine.fallback_attempted);
    assert!(!report.preparation_spine.external_engine_invoked);

    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let id = struct_array
        .column_by_name("id")
        .expect("id column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id int64 column");
    assert_eq!(id.value(0), 1);
    assert!(id.is_null(1));
    assert_eq!(id.value(2), 3);
    let label = struct_array
        .column_by_name("label")
        .expect("label column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("label string column");
    assert_eq!(label.null_count(), 3);
    let metric = struct_array
        .column_by_name("metric")
        .expect("metric column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("metric float64 column");
    assert_eq!(metric.value(0), 1.5);
    assert!(metric.is_null(1));
    assert_eq!(metric.value(2), 2.5);
    let active = struct_array
        .column_by_name("active")
        .expect("active column")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("active bool column");
    assert!(active.value(0));
    assert!(active.is_null(1));
    assert!(!active.value(2));

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_columnar_decimal_source_writes_reopens_precision_scale() {
    use std::sync::Arc;

    use arrow_array::{
        Array as _, Decimal128Array, Int64Array, RecordBatch, StructArray,
        builder::Decimal128Builder,
    };
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-decimal-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "amount".to_string()];
    let decimal_dtype = DataType::Decimal128(10, 2);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", decimal_dtype.clone(), true),
    ]));
    let mut amount = Decimal128Builder::with_capacity(4);
    amount.append_value(1234);
    amount.append_null();
    amount.append_value(-800);
    amount.append_value(0);
    let amount = amount
        .finish()
        .with_precision_and_scale(10, 2)
        .expect("decimal precision and scale");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(amount),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![
            Some(LogicalDType::Int64),
            Some(LogicalDType::Extension("decimal128(10,2)".to_string())),
        ],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(decimal_dtype)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 4,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 4);
    assert_eq!(report.reopen_row_count, 4);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,amount:decimal128(10,2)"
    );
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "columnar_source_state_preserved_to_vortex_array_provider"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "no_scalar_row_decode_for_non_empty_batches"
    );
    assert!(report.manual_scalar_copy_avoided);
    assert!(!report.preparation_spine.fallback_attempted);
    assert!(!report.preparation_spine.external_engine_invoked);

    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let amount = struct_array
        .column_by_name("amount")
        .expect("amount column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal amount column");
    assert_eq!(amount.precision(), 10);
    assert_eq!(amount.scale(), 2);
    assert_eq!(amount.value(0), 1234);
    assert!(amount.is_null(1));
    assert_eq!(amount.value(2), -800);
    assert_eq!(amount.value(3), 0);
    assert_eq!(amount.null_count(), 1);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_empty_buffered_columnar_preserves_canonical_scalar_schema_hints() {
    use arrow_schema::DataType;
    use vortex::array::dtype::{DType, Nullability, PType};

    let cases = [
        (DataType::Boolean, DType::Bool(Nullability::NonNullable)),
        (
            DataType::Int64,
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
        (
            DataType::UInt64,
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ),
        (
            DataType::Float64,
            DType::Primitive(PType::F64, Nullability::NonNullable),
        ),
        (DataType::Utf8, DType::Utf8(Nullability::NonNullable)),
        (DataType::Binary, DType::Binary(Nullability::NonNullable)),
    ];
    let columns = (0..cases.len())
        .map(|index| format!("field_{index}"))
        .collect::<Vec<_>>();
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: cases.iter().map(|(dtype, _)| Some(dtype.clone())).collect(),
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: Vec::new(),
        row_count: 0,
    };
    let shape = validate_flat_columnar_source_shape(&source).unwrap();
    let built = flat_columnar_source_to_vortex_struct(&source, &shape).unwrap();
    let DType::Struct(fields, Nullability::NonNullable) = built.array.dtype() else {
        panic!("empty columnar source must retain its native struct");
    };
    assert_eq!(built.array.len(), 0);
    for (actual, (_, expected)) in fields.fields().zip(cases) {
        assert_eq!(actual, expected);
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_empty_flat_columnar_decimal_source_writes_reopens_schema() {
    use arrow_array::{Decimal128Array, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-empty-columnar-decimal-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "amount".to_string()];
    let decimal_dtype = DataType::Decimal128(12, 4);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", decimal_dtype.clone(), true),
    ]);
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![
            Some(LogicalDType::Int64),
            Some(LogicalDType::Extension("decimal128(12,4)".to_string())),
        ],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(decimal_dtype)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: Vec::new(),
        row_count: 0,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 0);
    assert_eq!(report.reopen_row_count, 0);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,amount:decimal128(12,4)"
    );
    assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "shardloom_empty_columnar_struct_builder"
    );
    assert_eq!(
        report.array_build_strategy,
        "empty_columnar_schema_to_vortex_struct"
    );
    assert_eq!(report.array_build_record_batch_count, 0);
    assert!(!report.manual_scalar_copy_avoided);
    assert_eq!(
        report.preparation_spine.vortex_first_decision,
        "implement_shardloom_kernel"
    );
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "empty_columnar_schema_materialized_by_shardloom_kernel"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "empty_source_no_data_decode"
    );
    assert!(!report.preparation_spine.fallback_attempted);
    assert!(!report.preparation_spine.external_engine_invoked);

    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    assert_eq!(struct_array.len(), 0);
    let amount = struct_array
        .column_by_name("amount")
        .expect("amount column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal amount column");
    assert_eq!(amount.precision(), 12);
    assert_eq!(amount.scale(), 4);
    assert_eq!(amount.len(), 0);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_columnar_dictionary_source_writes_reopens_values() {
    use std::sync::Arc;

    use arrow_array::{
        Array as _, ArrayAccessor as _, BinaryArray, DictionaryArray, Int32Array, Int64Array,
        RecordBatch, StringArray, StructArray, types::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-dictionary-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec![
        "id".to_string(),
        "category".to_string(),
        "payload".to_string(),
    ];
    let dictionary_utf8_dtype =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let dictionary_binary_dtype =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", dictionary_utf8_dtype.clone(), true),
        Field::new("payload", dictionary_binary_dtype.clone(), true),
    ]));
    let category =
        DictionaryArray::<Int32Type>::from_iter([Some("alpha"), Some("beta"), None, Some("alpha")]);
    let payload = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1), None, Some(0)]),
        Arc::new(BinaryArray::from(vec![
            Some(&b"raw"[..]),
            Some(&[0x00, 0xff][..]),
        ])),
    )
    .expect("binary dictionary");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(category),
            Arc::new(payload),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![
            Some(LogicalDType::Int64),
            Some(LogicalDType::Utf8),
            Some(LogicalDType::Binary),
        ],
        column_arrow_dtypes: vec![
            Some(DataType::Int64),
            Some(dictionary_utf8_dtype),
            Some(dictionary_binary_dtype),
        ],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 4,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 4);
    assert_eq!(report.reopen_row_count, 4);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,category:dictionary_utf8,payload:dictionary_binary"
    );
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "columnar_source_state_preserved_to_vortex_array_provider"
    );
    assert_eq!(
        report.preparation_spine.decode_boundary_status,
        "no_scalar_row_decode_for_non_empty_batches"
    );
    assert!(report.manual_scalar_copy_avoided);
    assert!(!report.preparation_spine.fallback_attempted);
    assert!(!report.preparation_spine.external_engine_invoked);

    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let category = struct_array
        .column_by_name("category")
        .expect("category column")
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("dictionary category column");
    let typed = category
        .downcast_dict::<StringArray>()
        .expect("utf8 dictionary values");
    assert_eq!(typed.value(0), "alpha");
    assert_eq!(typed.value(1), "beta");
    assert!(typed.is_null(2));
    assert_eq!(typed.value(3), "alpha");
    assert_eq!(category.null_count(), 1);
    let payload = struct_array
        .column_by_name("payload")
        .expect("payload column")
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("dictionary payload column");
    let typed_payload = payload
        .downcast_dict::<BinaryArray>()
        .expect("binary dictionary values");
    assert_eq!(typed_payload.value(0), b"raw");
    assert_eq!(typed_payload.value(1), &[0x00, 0xff]);
    assert!(typed_payload.is_null(2));
    assert_eq!(typed_payload.value(3), b"raw");
    assert_eq!(payload.null_count(), 1);

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_scalar_typed_nested_rows_write_via_vortex_arrow_provider() {
    use std::sync::Arc;

    use arrow_array::{Array as _, Int64Array, ListArray, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::{LogicalDType, ScalarValue};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-scalar-nested-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec![
        "id".to_string(),
        "values".to_string(),
        "payload".to_string(),
    ];
    let rows = vec![
        vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            (
                "values".to_string(),
                ScalarValue::List(vec![ScalarValue::Int64(7), ScalarValue::Int64(8)]),
            ),
            (
                "payload".to_string(),
                ScalarValue::Struct(vec![
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                    ("amount".to_string(), ScalarValue::Int64(42)),
                ]),
            ),
        ],
        vec![
            ("id".to_string(), ScalarValue::Int64(2)),
            (
                "values".to_string(),
                ScalarValue::List(vec![ScalarValue::Int64(9), ScalarValue::Null]),
            ),
            (
                "payload".to_string(),
                ScalarValue::Struct(vec![
                    ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
                    ("amount".to_string(), ScalarValue::Null),
                ]),
            ),
        ],
    ];
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        columns.clone(),
        rows,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .column_dtypes(vec![
        Some(LogicalDType::Int64),
        Some(LogicalDType::List),
        Some(LogicalDType::Struct),
    ]);

    let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.reopen_row_count, 2);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,values:list,payload:struct"
    );
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert_eq!(
        report.array_build_strategy,
        "scalar_rows_to_arrow_record_batch_to_vortex"
    );
    assert_eq!(
        report.array_build_input_layout,
        "materialized_rows_to_arrow_record_batch"
    );
    assert_eq!(report.array_build_record_batch_count, 1);
    assert!(!report.manual_scalar_copy_avoided);
    assert_eq!(
        report.preparation_spine.vortex_first_decision,
        "use_vortex_native_provider"
    );
    assert_eq!(
        report.preparation_spine.provider_kind,
        "vortex_array_kernel"
    );
    assert_eq!(
        report.preparation_spine.materialization_boundary_status,
        "materialized_scalar_rows_to_arrow_record_batch_before_vortex_array_provider"
    );

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            false,
        ),
        Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Field::new("label", DataType::Utf8, true),
                    Field::new("amount", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        ),
    ]);
    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let values = struct_array
        .column_by_name("values")
        .expect("values column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("values list column");
    assert_eq!(values.len(), 2);
    assert_eq!(values.value_length(0), 2);
    assert_eq!(values.value_length(1), 2);
    let second_values = values.value(1);
    let second_values = second_values
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("second list int values");
    assert_eq!(second_values.value(0), 9);
    assert!(second_values.is_null(1));

    let payload = struct_array
        .column_by_name("payload")
        .expect("payload column")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("payload struct column");
    let labels = payload
        .column_by_name("label")
        .expect("label field")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("label string field");
    let amounts = payload
        .column_by_name("amount")
        .expect("amount field")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("amount int field");
    assert_eq!(labels.value(0), "alpha");
    assert_eq!(labels.value(1), "beta");
    assert_eq!(amounts.value(0), 42);
    assert!(amounts.is_null(1));

    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)]
fn local_flat_columnar_dictionary_primitive_source_writes_reopens_values() {
    use std::sync::Arc;

    use arrow_array::{
        Array as _, ArrayAccessor as _, DictionaryArray, Float64Array, Int16Array, Int64Array,
        RecordBatch, StructArray, UInt8Array,
        types::{Int16Type, UInt8Type},
    };
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-dictionary-primitive-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "bucket".to_string(), "score".to_string()];
    let dictionary_int_dtype =
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int64));
    let dictionary_float_dtype =
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Float64));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("bucket", dictionary_int_dtype.clone(), true),
        Field::new("score", dictionary_float_dtype.clone(), true),
    ]));
    let bucket = DictionaryArray::<UInt8Type>::try_new(
        UInt8Array::from(vec![Some(0), Some(1), None, Some(0)]),
        Arc::new(Int64Array::from(vec![10, 20])),
    )
    .expect("int dictionary");
    let score = DictionaryArray::<Int16Type>::try_new(
        Int16Array::from(vec![Some(0), Some(1), Some(0), None]),
        Arc::new(Float64Array::from(vec![1.5, 2.25])),
    )
    .expect("float dictionary");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(bucket),
            Arc::new(score),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![
            Some(LogicalDType::Int64),
            Some(LogicalDType::Int64),
            Some(LogicalDType::Float64),
        ],
        column_arrow_dtypes: vec![
            Some(DataType::Int64),
            Some(dictionary_int_dtype),
            Some(dictionary_float_dtype),
        ],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 4,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

    assert_eq!(report.row_count, 4);
    assert_eq!(report.reopen_row_count, 4);
    assert_eq!(
        report.column_family_summary(),
        "id:int64,bucket:dictionary_int64,score:dictionary_float64"
    );
    assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
    assert_eq!(
        report.array_build_provider_surface,
        "ArrayRef::from_arrow(RecordBatch)"
    );
    assert!(report.manual_scalar_copy_avoided);
    assert!(!report.preparation_spine.fallback_attempted);
    assert!(!report.preparation_spine.external_engine_invoked);

    let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
    let struct_array = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("arrow struct");
    let bucket = struct_array
        .column_by_name("bucket")
        .expect("bucket column")
        .as_any()
        .downcast_ref::<DictionaryArray<UInt8Type>>()
        .expect("dictionary bucket column");
    let bucket_values = bucket
        .downcast_dict::<Int64Array>()
        .expect("int dictionary values");
    assert_eq!(bucket_values.value(0), 10);
    assert_eq!(bucket_values.value(1), 20);
    assert!(bucket_values.is_null(2));
    assert_eq!(bucket_values.value(3), 10);
    let score = struct_array
        .column_by_name("score")
        .expect("score column")
        .as_any()
        .downcast_ref::<DictionaryArray<Int16Type>>()
        .expect("dictionary score column");
    let score_values = score
        .downcast_dict::<Float64Array>()
        .expect("float dictionary values");
    assert_eq!(score_values.value(0).to_bits(), 1.5_f64.to_bits());
    assert_eq!(score_values.value(1).to_bits(), 2.25_f64.to_bits());
    assert_eq!(score_values.value(2).to_bits(), 1.5_f64.to_bits());
    assert!(score_values.is_null(3));
    assert!(path.exists());
    std::fs::remove_file(path).expect("remove artifact");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_dictionary_float_rejects_non_finite_values_before_provider_path() {
    use std::sync::Arc;

    use arrow_array::{
        DictionaryArray, Float64Array, Int64Array, RecordBatch, UInt8Array, types::UInt8Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-dictionary-non-finite-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "score".to_string()];
    let dictionary_float_dtype =
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Float64));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", dictionary_float_dtype.clone(), true),
    ]));
    let score = DictionaryArray::<UInt8Type>::try_new(
        UInt8Array::from(vec![Some(0), Some(0)]),
        Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
    )
    .expect("float dictionary");
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(score)],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![Some(LogicalDType::Int64), Some(LogicalDType::Float64)],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(dictionary_float_dtype)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 2,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_columnar_vortex_prepared_state(request).expect_err("blocked");

    assert!(
        error
            .to_string()
            .contains("non-finite dictionary float value at dictionary index 1")
    );
    let _ = std::fs::remove_file(path);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_dictionary_boolean_blocks_before_provider_path() {
    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, DictionaryArray, Int64Array, RecordBatch, UInt8Array, types::UInt8Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_core::LogicalDType;

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-dictionary-boolean-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "active".to_string()];
    let dictionary_bool_dtype =
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Boolean));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("active", dictionary_bool_dtype.clone(), true),
    ]));
    let active = DictionaryArray::<UInt8Type>::try_new(
        UInt8Array::from(vec![Some(0), None]),
        Arc::new(BooleanArray::from(vec![true])),
    )
    .expect("boolean dictionary");
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(active)],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![Some(LogicalDType::Int64), Some(LogicalDType::Boolean)],
        column_arrow_dtypes: vec![Some(DataType::Int64), Some(dictionary_bool_dtype)],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 2,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_columnar_vortex_prepared_state(request)
        .expect_err("boolean dictionary blocked")
        .to_string();

    assert!(error.contains("unsupported Arrow type Dictionary(UInt8, Boolean)"));
    assert!(error.contains("no fallback execution was attempted"));
    let _ = std::fs::remove_file(path);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_record_batch_rejects_non_finite_float_before_provider_path() {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-non-finite-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "metric".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![None; columns.len()],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 2,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_columnar_vortex_prepared_state(request)
        .expect_err("non-finite float should be rejected");

    assert!(error.to_string().contains("non-finite float64"));
    assert!(!path.exists());
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_stream_rejects_non_finite_float_before_provider_path() {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

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

    impl arrow_array::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-stream-non-finite-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "metric".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
        ],
    )
    .expect("record batch");
    let reader = TestRecordBatchReader {
        schema,
        batches: VecDeque::from([batch]),
    };
    let source = FlatLocalColumnarStreamSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![None; columns.len()],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        row_count_hint: Some(2),
        record_batch_count_hint: Some(1),
        source_stream_batch_size: 0,
        source_stream_unit_count_hint: Some(1),
        source_stream_unit_row_ranges: Some(vec![(0, 2)]),
        source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
        source_stream_policy: "test_source_defined_record_batches".to_string(),
        source_dictionary_preservation_status: "test_not_applicable".to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "test_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(1),
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros:
            crate::universal_format_io::new_embedded_derived_build_micros_counter(),
        reader: Box::new(reader),
    };
    let request = VortexPreparedStateColumnarStreamWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_columnar_vortex_prepared_state_streaming(request)
        .expect_err("non-finite streaming float should be rejected");

    assert!(error.to_string().contains("non-finite float64"));
    assert!(!path.exists());
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_source_rejects_short_batches_before_column_access() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-short-batch-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        ],
    )
    .expect("record batch");
    let columns = vec!["id".to_string(), "label".to_string(), "metric".to_string()];
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![None; columns.len()],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 2,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_columnar_vortex_prepared_state(request)
        .expect_err("short batch must be rejected before column access");

    assert!(
        error
            .to_string()
            .contains("columnar SourceState batch 1 has 2 projected columns, expected 3")
    );
    assert!(
        error
            .to_string()
            .contains("no fallback execution was attempted")
    );
    assert!(!path.exists());
}

#[cfg(feature = "universal-format-io")]
#[test]
fn local_flat_columnar_source_rejects_row_count_mismatch() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-columnar-row-count-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let columns = vec!["id".to_string(), "label".to_string()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        ],
    )
    .expect("record batch");
    let source = FlatLocalColumnarSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: vec![None; columns.len()],
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        batches: vec![batch],
        row_count: 3,
    };
    let request = VortexPreparedStateColumnarWriteRequest::new(
        &path,
        source,
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    );

    let error = write_flat_columnar_vortex_prepared_state(request)
        .expect_err("row count mismatch must be rejected");

    assert!(
        error
            .to_string()
            .contains("columnar SourceState row count is 2, expected 3")
    );
    assert!(
        error
            .to_string()
            .contains("no fallback execution was attempted")
    );
    assert!(!path.exists());
}

#[test]
fn local_flat_scalar_full_replay_is_blocked_without_output_replay() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-vortex-ingest-full-replay-{}-{}.vortex",
        std::process::id(),
        1
    ));
    let _ = std::fs::remove_file(&path);
    let request = VortexPreparedStateWriteRequest::new(
        &path,
        vec!["id".to_string(), "label".to_string()],
        vec![vec![
            ("id".to_string(), ScalarValue::Int64(1)),
            ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
        ]],
        shardloom_core::ExecutionResources::from_gib(
            4,
            8,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
    .certification_level(VortexIngestCertificationLevel::IngestFullReplay);

    let error = write_flat_scalar_vortex_prepared_state(request)
        .expect_err("full replay requires downstream output evidence");

    assert!(
        error
            .to_string()
            .contains("ingest_full_replay requires downstream result replay/output evidence")
    );
    assert!(!path.exists());
}

#[test]
fn differential_preparation_admits_append_only_delta_overlay() {
    let input = differential_input(VortexDifferentialUpdateMode::AppendOnly);

    let report = evaluate_vortex_differential_preparation(input);
    let fields = report.evidence_fields();

    assert!(report.is_admitted());
    assert_eq!(report.status, "admitted_append_only_delta_overlay");
    assert_eq!(
        report.schema_compatibility_status,
        "compatible_source_schema_and_column_families"
    );
    assert_eq!(
        report.prepared_state_reuse_status,
        "base_prepared_state_reused_for_delta_overlay"
    );
    assert_eq!(
        report.native_io_certificate_status,
        "certified_local_vortex_differential_preparation_overlay"
    );
    assert!(report.overlay_manifest_digest.starts_with("fnv64:"));
    assert!(report.correctness_digest.starts_with("fnv64:"));
    assert!(fields.contains(&(
        "vortex_differential_preparation_overlay_applied".to_string(),
        "true".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_differential_preparation_fallback_attempted".to_string(),
        "false".to_string()
    )));
}

#[test]
fn capillary_manifest_cannot_certify_scheduler_execution_or_pressure() {
    let input = capillary_input("certified");

    let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");
    let fields = report.evidence_fields();

    assert_eq!(report.status, "planned_capillary_task_manifest");
    assert_eq!(report.task_count, 6);
    assert!(report.task_manifest_digest.starts_with("fnv64:"));
    assert_eq!(report.native_io_certificate_status, "certified");
    assert_eq!(report.pulseweave_report.status, "blocked");
    assert!(!report.pulseweave_report.runtime_decision_applied);
    assert!(!report.pulseweave_report.endopulse.adjustment_applied);
    assert!(!report.pulseweave_report.proofbound.claim_allowed);
    assert!(!report.scheduler_applied);
    assert_eq!(report.execution_certificate_status, "not_observed");
    assert_eq!(report.peak_memory_bytes, None);
    assert_eq!(
        report.scheduler_application_reason,
        "task_manifest_only_no_scheduler_application"
    );
    assert_eq!(report.execution_window_size, 0);
    assert_eq!(report.execution_window_count, 0);
    assert_eq!(report.execution_window_ids, "none");
    assert_eq!(report.execution_window_task_ids, "none");
    assert_eq!(report.execution_window_digests, "none");
    assert_eq!(
        report.pulseweave_report.application_scope,
        "vortex_cold_preparation_local_capillary_io"
    );
    assert!(fields.contains(&(
        "vortex_capillary_preparation_task_roles".to_string(),
        "source_split_discovery,read_chunk,columnarize_encode,vortex_segment_write,reopen_verify,sink_evidence".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_pulseweave_runtime_decision_applied".to_string(),
        "false".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_scheduler_applied".to_string(),
        "false".to_string()
    )));
    assert!(fields.iter().any(|(key, value)| {
        key == "vortex_capillary_preparation_execution_window_count" && value == "0"
    }));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_fallback_attempted".to_string(),
        "false".to_string()
    )));
}

#[test]
fn capillary_manifest_preserves_role_checks_without_schedule_claims() {
    let report =
        evaluate_vortex_capillary_preparation(capillary_input("certified")).expect("manifest");
    let mut control = VortexCapillaryPreWriteControlReport::from_preparation_report(&report);
    assert!(control.task_manifest_admitted);
    assert!(!control.scheduler_applied);
    control
        .apply_task_role_gate("read_chunk", "read_chunk")
        .expect("role");
    assert_eq!(control.read_chunk_gate_status, "validated_task_role");
    control.controlled_task_roles = "read_chunk".to_string();
    let error = control
        .apply_task_role_gate("vortex_segment_write", "write")
        .expect_err("missing write role must still fail");
    assert!(
        error
            .to_string()
            .contains("task role 'vortex_segment_write' was missing")
    );
    assert!(!control.scheduler_applied);
    assert_eq!(control.execution_window_count, 0);
    assert_eq!(control.write_gate_status, "pending");
}

#[test]
fn capillary_stages_do_not_grant_independent_work_permissions() {
    for task in capillary_preparation_tasks(&capillary_input("certified")) {
        let shape = task.to_pulseweave_shape().expect("shape");
        assert!(!shape.can_reorder);
        assert!(!shape.can_split);
        assert!(!shape.can_coalesce);
    }
}

#[test]
fn capillary_preparation_skips_small_input_below_threshold() {
    let mut input = capillary_input("certified");
    input.capillary_claim_evidence_requested = false;

    let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");
    let fields = report.evidence_fields();

    assert_eq!(report.status, "not_requested_below_threshold");
    assert_eq!(report.activation_result, "skipped");
    assert_eq!(
        report.activation_reason,
        "below_threshold_small_local_fixture"
    );
    assert_eq!(report.task_count, 0);
    assert_eq!(report.task_manifest_id, "none");
    assert_eq!(report.execution_window_count, 0);
    assert_eq!(report.execution_window_size, 0);
    assert_eq!(report.execution_window_ids, "none");
    assert!(!report.scheduler_applied);
    assert_eq!(
        report.scheduler_application_reason,
        "not_requested_below_threshold"
    );
    assert_eq!(report.pulseweave_report.status, "not_requested");
    assert!(!report.pulseweave_report.runtime_decision_applied);
    assert_eq!(report.execution_certificate_status, "not_requested");
    assert_eq!(report.claim_gate_status, "not_claim_grade");
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
    assert!(fields.contains(&(
        "vortex_capillary_preparation_activation_policy".to_string(),
        "dynamic_size_complexity_gate.v1".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_activation_result".to_string(),
        "skipped".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_task_count".to_string(),
        "0".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_pulseweave_status".to_string(),
        "not_requested".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_scheduler_applied".to_string(),
        "false".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_capillary_preparation_execution_window_count".to_string(),
        "0".to_string()
    )));
}

#[test]
fn capillary_preparation_activates_large_input_by_threshold() {
    let mut input = capillary_input("certified");
    input.capillary_claim_evidence_requested = false;
    input.source_byte_count = VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_BYTES;

    let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");

    assert_eq!(report.status, "planned_capillary_task_manifest");
    assert_eq!(report.activation_result, "activated");
    assert_eq!(report.activation_reason, "source_bytes_above_threshold");
    assert_eq!(report.task_count, 6);
    assert!(!report.scheduler_applied);
    assert_eq!(report.execution_window_count, 0);
    assert_eq!(report.pulseweave_report.status, "blocked");
}

#[test]
fn capillary_preparation_blocks_none_source_split_refs_as_missing_manifest() {
    let mut input = capillary_input("certified");
    input.source_split_refs = "none".to_string();

    let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");

    assert_eq!(report.status, "blocked_missing_capillary_task_manifest");
    assert_eq!(
        report.execution_certificate_status,
        "missing_capillary_task_manifest"
    );
    assert_eq!(report.pulseweave_report.status, "blocked");
    assert!(!report.pulseweave_report.runtime_decision_applied);
    assert!(!report.scheduler_applied);
    assert_eq!(
        report.scheduler_application_reason,
        "blocked_missing_capillary_task_manifest"
    );
    assert_eq!(report.execution_window_count, 0);
    assert!(
        report
            .pulseweave_report
            .blocker
            .contains("execution_certificate")
    );
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn scout_ingress_admits_clean_local_source() {
    let input = scout_input(0, "none", "not_detected", false);

    let report = evaluate_vortex_scout_ingress(input);
    let fields = report.evidence_fields();

    assert_eq!(report.status, "admitted_scout_ingress_clean");
    assert_eq!(
        report.no_standalone_lane_status,
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    );
    assert!(!report.quarantine_required);
    assert_eq!(report.claim_gate_status, "not_claim_grade");
    assert!(fields.contains(&(
        "vortex_scout_ingress_anomaly_count".to_string(),
        "0".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_scout_ingress_fallback_attempted".to_string(),
        "false".to_string()
    )));
}

#[test]
fn scout_ingress_blocks_nested_shapes_with_quarantine_plan() {
    let input = scout_input(
        1,
        "unsupported_nested_shape",
        "blocked_unsupported_nested_shape",
        true,
    );

    let report = evaluate_vortex_scout_ingress(input);

    assert_eq!(report.status, "blocked_unsupported_nested_shape");
    assert_eq!(report.anomaly_families, "unsupported_nested_shape");
    assert_eq!(
        report.unsupported_shape_status,
        "blocked_unsupported_nested_shape"
    );
    assert!(report.quarantine_required);
    assert_eq!(
        report.quarantine_output_plan_status,
        "planned_not_emitted_no_quarantine_sink_requested"
    );
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn layout_write_advisor_admits_scoped_local_strategy() {
    let report = evaluate_vortex_layout_write_advisor(layout_advisor_input(true, "none"));
    let fields = report.evidence_fields();

    assert_eq!(report.status, "admitted_local_layout_write_strategy");
    assert!(report.strategy_admitted);
    assert!(!report.runtime_decision_applied);
    assert_eq!(
        report.selected_strategy,
        "single_vortex_artifact_embedded_olap_layout_statistics"
    );
    assert!(report.strategy_decision_digest.starts_with("fnv64:"));
    assert!(report.provider_admitted);
    assert_eq!(report.blocker, "pending_runtime_write_decision");
    assert_eq!(
        report.no_standalone_lane_status,
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    );
    assert_eq!(report.claim_gate_status, "not_claim_grade");
    assert!(!report.fallback_attempted);
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_role".to_string(),
        "universal_ingest_optimizer".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_public_runtime_posture".to_string(),
        "admitted_local_product_workflow_not_smoke".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_single_artifact_required".to_string(),
        "true".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_query_answer_sidecar_allowed".to_string(),
        "false".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_surface_unification_policy".to_string(),
        "sql_python_dataframe_cli_share_vortex_normalized_physical_runtime".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_source_scale".to_string(),
        "unknown".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_profile_family".to_string(),
        "unknown".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_prepared_layout_family".to_string(),
        "unknown".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_key_profile".to_string(),
        "unknown".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_dictionary_profile".to_string(),
        "unknown".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_writer_parallelism_budget".to_string(),
        "1".to_string()
    )));
}

#[test]
fn layout_write_advisor_exposes_source_profile_fields_for_large_text_olap() {
    let mut input = layout_advisor_input(true, "none");
    input.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
    input.source_byte_count = 1_073_741_824;
    input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_time_counter_olap;layout_family=url_time_counter_dictionary_stats_layout;text_domain=true;time_bucket=true;counter=true;key_profile=high_cardinality_numeric_text_time_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
            .to_string();
    input.expected_read_tradeoff =
        "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution".to_string();
    input.expected_write_tradeoff =
        "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
            .to_string();

    let report = evaluate_vortex_layout_write_advisor(input);
    let fields = report.evidence_fields();

    assert_eq!(report.status, "admitted_local_layout_write_strategy");
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_writer_parallelism_budget".to_string(),
        "1".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_metadata_payload_policy".to_string(),
        "embedded_generic_layout_statistics_no_query_answer_sidecars".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_compact_derived_metadata_policy".to_string(),
        "dictionary_derived_metadata_preferred_over_full_hidden_columns".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_membership_sketch_policy".to_string(),
        "generic_segment_membership_only_not_query_result_cache".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_row_position_locality_policy".to_string(),
        "embedded_order_key_row_position_locality_when_available".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_source_scale".to_string(),
        "large_olap".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_profile_family".to_string(),
        "url_time_counter_olap".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_prepared_layout_family".to_string(),
        "url_time_counter_dictionary_stats_layout".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_text_domain_columns".to_string(),
        "true".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_time_bucket_columns".to_string(),
        "true".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_counter_columns".to_string(),
        "true".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_key_profile".to_string(),
        "high_cardinality_numeric_text_time_keys".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_dictionary_profile".to_string(),
        "source_dictionary_or_derived_dictionary_evidence".to_string()
    )));
    assert!(fields.contains(&(
        "vortex_layout_write_advisor_expected_read_tradeoff".to_string(),
        "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution".to_string()
    )));
    assert!(
        fields.contains(&(
            "vortex_layout_write_advisor_expected_write_tradeoff".to_string(),
            "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
                .to_string()
        ))
    );
}

#[test]
fn layout_write_advisor_blocks_unsupported_strategy() {
    let report = evaluate_vortex_layout_write_advisor(layout_advisor_input(
        false,
        "vortex_layout_write_advisor.unsupported_layout_strategy",
    ));

    assert_eq!(report.status, "blocked_layout_write_strategy");
    assert!(!report.strategy_admitted);
    assert!(!report.runtime_decision_applied);
    assert_eq!(report.selected_strategy, "not_admitted");
    assert!(!report.provider_admitted);
    assert_eq!(
        report.blocker,
        "vortex_layout_write_advisor.unsupported_layout_strategy"
    );
    assert_eq!(
        report.unsupported_diagnostic_code,
        "vortex_layout_write_advisor.unsupported_layout_strategy"
    );
    assert!(!report.external_engine_invoked);
}

#[test]
fn copy_budget_reports_unmeasured_segments_and_blocks_unsafe_reuse() {
    let report = evaluate_vortex_copy_budget(copy_budget_input(
        "reported_with_not_measured_segments",
        "blocked_no_unsafe_lifetime_shortcuts",
        "blocked_until_correctness_parity",
    ));

    assert_eq!(
        report.status,
        "reported_copy_budget_with_unmeasured_segments"
    );
    assert_eq!(report.buffer_reuse_count, 0);
    assert_eq!(report.claim_gate_status, "not_claim_grade");
    assert!(!report.fallback_attempted);

    let blocked = evaluate_vortex_copy_budget(copy_budget_input(
        "reported_with_not_measured_segments",
        "unsafe_lifetime_shortcut_requested",
        "blocked_unsafe_lifetime_shortcut",
    ));
    assert_eq!(blocked.status, "blocked_unsafe_lifetime_shortcut");

    let admitted = evaluate_vortex_copy_budget(copy_budget_input(
        "reported_with_not_measured_segments",
        "blocked_no_unsafe_lifetime_shortcuts",
        "admitted_read_once_source_buffer_carry_with_digest_and_row_count_proof",
    ));
    assert_eq!(
        admitted.status,
        "admitted_scoped_buffer_reuse_with_unmeasured_segments"
    );
}

#[test]
fn capillary_preparation_blocks_pulseweave_without_native_io_certificate() {
    let input = capillary_input("missing");

    let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");

    assert_eq!(
        report.status,
        "report_only_blocked_missing_native_io_certificate"
    );
    assert_eq!(report.execution_certificate_status, "not_observed");
    assert_eq!(report.pulseweave_report.status, "blocked");
    assert!(!report.pulseweave_report.runtime_decision_applied);
    assert!(!report.scheduler_applied);
    assert_eq!(
        report.scheduler_application_reason,
        "blocked_missing_native_io_certificate"
    );
    assert_eq!(report.execution_window_count, 0);
    assert!(
        report
            .pulseweave_report
            .blocker
            .contains("execution_certificate")
    );
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn differential_preparation_blocks_update_mode_without_overlay() {
    let input = differential_input(VortexDifferentialUpdateMode::Update);

    let report = evaluate_vortex_differential_preparation(input);

    assert!(!report.is_admitted());
    assert_eq!(report.status, "blocked_update_mode_policy");
    assert_eq!(
        report.update_mode_policy,
        "update_delete_upsert_blocked_until_row_identity_and_rewrite_semantics_are_certified"
    );
    assert_eq!(
        report.replay_verification_status,
        "blocked_before_overlay_replay"
    );
    assert!(!report.base_reprepare_performed);
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn differential_preparation_blocks_schema_mismatch_without_overlay() {
    let mut input = differential_input(VortexDifferentialUpdateMode::AppendOnly);
    input.delta_schema_digest = "fnv64:other".to_string();

    let report = evaluate_vortex_differential_preparation(input);

    assert!(!report.is_admitted());
    assert_eq!(report.status, "blocked_schema_mismatch");
    assert_eq!(
        report.schema_compatibility_status,
        "blocked_source_schema_or_column_family_mismatch"
    );
    assert_eq!(
        report.prepared_state_reuse_status,
        "blocked_before_prepared_state_reuse"
    );
    assert!(report.delta_artifact_written);
    assert!(!report.overlay_applied);
}

#[test]
fn append_only_refinement_admits_verified_source_prefix_delta() {
    let root = temp_test_root("append-only-refinement-admit");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, "id,label\n1,alpha\n2,beta\n").expect("write base source");
    std::fs::write(&target, b"prepared-vortex-artifact").expect("write prepared artifact");
    let manifest_path = vortex_prepared_state_reuse_manifest_path(&target).expect("manifest path");
    let base_request = reuse_request_for_test(&source, &target, &manifest_path);
    let base_decision =
        evaluate_vortex_prepared_state_reuse(&base_request).expect("base reuse decision");
    let artifact_digest = sha256_file_digest(&target, "test artifact").expect("artifact digest");
    write_vortex_prepared_state_reuse_manifest(
        &base_request,
        &base_decision,
        VortexPreparedStateReuseWriteEvidence {
            source_state_id: "source-state-base".to_string(),
            source_state_digest: "fnv64:source-base".to_string(),
            source_schema_digest: "fnv64:schema".to_string(),
            source_row_count: 2,
            source_column_family_summary: "id:int64,label:utf8".to_string(),
            prepared_state_id: "prepared-base".to_string(),
            prepared_state_digest: "fnv64:prepared-base".to_string(),
            prepared_artifact_digest: artifact_digest,
            certificate_refs: "reopen_row_count_scan".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .expect("write reuse manifest");

    std::fs::write(&source, "id,label\n1,alpha\n2,beta\n3,gamma\n").expect("append source");
    let current_request = reuse_request_for_test(&source, &target, &manifest_path);
    let decision = evaluate_vortex_prepared_state_append_only_refinement(&current_request)
        .expect("refinement decision");

    assert!(decision.is_admitted());
    assert_eq!(decision.status, "admitted_append_only_refinement");
    assert_eq!(
        decision.automatic_detection_status,
        "append_only_delta_detected"
    );
    assert_eq!(
        decision.source_prefix_verification_status,
        "verified_old_source_bytes_are_current_prefix"
    );
    assert_eq!(decision.base_source_row_count, 2);
    assert_eq!(
        decision.base_source_column_family_summary,
        "id:int64,label:utf8"
    );
    assert!(decision.changed_byte_range_refs.contains("#bytes="));
    assert!(!decision.fallback_attempted);
    assert!(!decision.external_engine_invoked);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn prepared_state_reuse_missing_artifact_is_cache_miss_not_error() {
    let root = temp_test_root("reuse-manifest-missing-artifact");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, "id,label\n1,alpha\n").expect("write source");
    std::fs::write(&target, b"prepared-vortex-artifact").expect("write prepared artifact");
    let manifest_path = vortex_prepared_state_reuse_manifest_path(&target).expect("manifest path");
    let request = reuse_request_for_test(&source, &target, &manifest_path);
    let first_decision =
        evaluate_vortex_prepared_state_reuse(&request).expect("first reuse decision");
    let artifact_digest = sha256_file_digest(&target, "test artifact").expect("artifact digest");
    write_vortex_prepared_state_reuse_manifest(
        &request,
        &first_decision,
        VortexPreparedStateReuseWriteEvidence {
            source_state_id: "source-state-base".to_string(),
            source_state_digest: "fnv64:source-base".to_string(),
            source_schema_digest: "fnv64:schema".to_string(),
            source_row_count: 1,
            source_column_family_summary: "id:int64,label:utf8".to_string(),
            prepared_state_id: "prepared-base".to_string(),
            prepared_state_digest: "fnv64:prepared-base".to_string(),
            prepared_artifact_digest: artifact_digest,
            certificate_refs: "reopen_row_count_scan".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .expect("write reuse manifest");

    std::fs::remove_file(&target).expect("remove stale artifact");
    let decision =
        evaluate_vortex_prepared_state_reuse(&request).expect("missing artifact is a miss");
    assert!(!decision.hit);
    assert_eq!(decision.status, "prepared_state_reuse_miss");
    assert_eq!(decision.reason, "prepared_artifact_missing");
    assert_eq!(decision.invalidation_reason, "prepared_artifact_missing");
    assert!(!decision.fallback_attempted);
    assert!(!decision.external_engine_invoked);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
#[allow(clippy::too_many_lines)]
fn prepared_olap_state_report_admits_single_vortex_artifact_without_sidecars() {
    let root = temp_test_root("prepared-olap-single-artifact");
    let source = root.join("source.parquet");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, b"source bytes").expect("write source");
    write_test_prepared_vortex_artifact(&target, 10);
    let source_digest = sha256_file_digest(&source, "test source").expect("source digest");
    let legacy_manifest_path = legacy_prepared_olap_state_manifest_path(&target);

    let request = VortexPreparedOlapStateWriteRequest::new_local(
        "source-state-hits",
        "sha256:source-state-digest",
        "parquet",
        source_digest,
        12,
        "sha256:schema-digest",
        2,
        &target,
        &target,
    )
    .expect("olap request")
    .with_prepared_layout_policy(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_LAYOUT_POLICY)
    .with_segment_map_status(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_SEGMENT_MAP_STATUS)
    .with_certification_level("single_vortex_artifact_olap_contract")
    .with_certificate_refs("local-prepared-olap-state-test");

    let report =
        publish_vortex_prepared_olap_single_artifact_state(&request).expect("publish olap state");

    assert_eq!(report.status, "prepared_olap_state_ready");
    assert!(!legacy_manifest_path.exists());
    assert!(!report.manifest_written);
    assert!(report.manifest_digest.starts_with("sha256:"));
    assert!(report.state_digest.starts_with("sha256:"));
    assert_eq!(report.exact_sidecar_family_count, 0);
    assert!(!report.sub_second_candidate);
    assert_eq!(
        report.artifact_model,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_MODEL
    );
    assert_eq!(
        report.writer_layout_strategy,
        VORTEX_PREPARED_OLAP_WRITER_LAYOUT_STRATEGY
    );
    assert_eq!(
        report.metadata_pruning_contract,
        VORTEX_PREPARED_OLAP_METADATA_PRUNING_CONTRACT
    );
    assert_eq!(
        report.query_answer_sidecar_status,
        VORTEX_PREPARED_OLAP_QUERY_ANSWER_SIDECAR_STATUS
    );
    assert_eq!(
        report.query_time_contract,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_QUERY_TIME_CONTRACT
    );
    assert_eq!(
        report.layout_inventory_status,
        "opened_single_vortex_artifact_footer"
    );
    assert!(report.layout_inventory_digest.starts_with("fnv64:"));
    assert_eq!(report.layout_footer_row_count, "2");
    assert!(report.layout_footer_segment_count != "unknown");
    assert!(report.layout_artifact_size_bytes != "unknown");
    assert!(report.layout_footer_approx_bytes != "unknown");
    assert!(report.layout_size_attribution.contains("artifact_bytes="));
    assert!(report.layout_size_attribution.contains("source_columns="));
    assert!(report.layout_size_attribution.contains("derived_columns="));
    assert!(report.layout_size_attribution.contains("footer_bytes="));
    assert!(
        report
            .layout_size_attribution
            .contains("column_byte_attribution=not_available_from_current_vortex_footer")
    );
    assert!(report.layout_metadata_persisted_in_artifact);
    assert_eq!(report.blocker_id, "none");
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
    assert!(
        report
            .admitted_query_families
            .contains("native_vortex_runtime")
    );

    let fields = report
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_schema_version")
            .map(String::as_str),
        Some(VORTEX_PREPARED_OLAP_STATE_SCHEMA_VERSION)
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_query_time_contract")
            .map(String::as_str),
        Some(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_QUERY_TIME_CONTRACT)
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_artifact_model")
            .map(String::as_str),
        Some(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_MODEL)
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_metadata_pruning_contract")
            .map(String::as_str),
        Some(VORTEX_PREPARED_OLAP_METADATA_PRUNING_CONTRACT)
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_query_answer_sidecar_status")
            .map(String::as_str),
        Some(VORTEX_PREPARED_OLAP_QUERY_ANSWER_SIDECAR_STATUS)
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_evidence_persistence")
            .map(String::as_str),
        Some("embedded_in_single_prepared_vortex_artifact")
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_external_manifest_written")
            .map(String::as_str),
        Some("false")
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_layout_inventory_status")
            .map(String::as_str),
        Some("opened_single_vortex_artifact_footer")
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_layout_footer_row_count")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        fields
            .get("vortex_prepared_olap_state_layout_metadata_persisted_in_artifact")
            .map(String::as_str),
        Some("true")
    );

    let hit =
        evaluate_vortex_prepared_olap_single_artifact_state(&request).expect("evaluate olap state");
    assert_eq!(hit.status, "prepared_olap_state_read_through_hit");
    assert!(!hit.manifest_written);
    assert_eq!(hit.manifest_digest, report.manifest_digest);
    assert_eq!(hit.state_digest, report.state_digest);
    assert_eq!(hit.exact_sidecar_family_count, 0);
    assert_eq!(
        hit.metadata_pruning_contract,
        VORTEX_PREPARED_OLAP_METADATA_PRUNING_CONTRACT
    );
    assert_eq!(
        hit.query_answer_sidecar_status,
        VORTEX_PREPARED_OLAP_QUERY_ANSWER_SIDECAR_STATUS
    );
    assert_eq!(
        hit.query_time_contract,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_QUERY_TIME_CONTRACT
    );
    assert_eq!(
        hit.layout_inventory_status,
        "opened_single_vortex_artifact_footer"
    );
    assert_eq!(hit.layout_footer_row_count, "2");
    assert!(hit.layout_metadata_persisted_in_artifact);
    assert_eq!(hit.blocker_id, "none");
    assert!(!hit.fallback_attempted);
    assert!(!hit.external_engine_invoked);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn prepared_olap_state_artifact_report_updates_digest_without_external_manifest() {
    let root = temp_test_root("prepared-olap-artifact-drift");
    let source = root.join("source.parquet");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, b"source bytes").expect("write source");
    write_test_prepared_vortex_artifact(&target, 20);
    let source_digest = sha256_file_digest(&source, "test source").expect("source digest");
    let legacy_manifest_path = legacy_prepared_olap_state_manifest_path(&target);
    let request = VortexPreparedOlapStateWriteRequest::new_local(
        "source-state-hits",
        "sha256:source-state-digest",
        "parquet",
        source_digest,
        12,
        "sha256:schema-digest",
        2,
        &target,
        &target,
    )
    .expect("olap request")
    .with_prepared_layout_policy(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_LAYOUT_POLICY)
    .with_segment_map_status(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_SEGMENT_MAP_STATUS);
    let initial =
        publish_vortex_prepared_olap_single_artifact_state(&request).expect("publish olap state");
    std::thread::sleep(std::time::Duration::from_millis(5));
    write_test_prepared_vortex_artifact(&target, 30);

    let decision = evaluate_vortex_prepared_olap_single_artifact_state(&request)
        .expect("evaluate mutated olap artifact");

    assert_eq!(decision.status, "prepared_olap_state_read_through_hit");
    assert_eq!(decision.blocker_id, "none");
    assert_eq!(
        decision.query_time_contract,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_QUERY_TIME_CONTRACT
    );
    assert!(!decision.manifest_written);
    assert!(!legacy_manifest_path.exists());
    assert!(!decision.fallback_attempted);
    assert!(!decision.external_engine_invoked);
    assert_ne!(
        decision.prepared_artifact_digest,
        initial.prepared_artifact_digest
    );
    assert_ne!(decision.state_digest, initial.state_digest);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn prepared_olap_artifact_fingerprint_tracks_same_size_artifact_mutation() {
    let root = temp_test_root("prepared-olap-artifact-fingerprint");
    let source = root.join("source.parquet");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, b"source bytes").expect("write source");
    write_test_prepared_vortex_artifact(&target, 40);
    let source_digest = sha256_file_digest(&source, "test source").expect("source digest");
    let request = VortexPreparedOlapStateWriteRequest::new_local(
        "source-state-hits",
        "sha256:source-state-digest",
        "parquet",
        source_digest,
        12,
        "sha256:schema-digest",
        2,
        &target,
        &target,
    )
    .expect("olap request")
    .with_prepared_layout_policy(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_LAYOUT_POLICY)
    .with_segment_map_status(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_SEGMENT_MAP_STATUS);
    let initial =
        publish_vortex_prepared_olap_single_artifact_state(&request).expect("publish olap state");

    std::thread::sleep(std::time::Duration::from_millis(5));
    write_test_prepared_vortex_artifact(&target, 50);
    let current_digest = sha256_file_digest(&target, "test artifact").expect("current digest");
    assert_ne!(current_digest, initial.prepared_artifact_digest);

    let fingerprint =
        LocalReuseFileFingerprint::from_path(&target, "test artifact").expect("fingerprint");
    assert_eq!(fingerprint.content_digest, current_digest);
    assert_ne!(fingerprint.content_digest, initial.prepared_artifact_digest);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn prepared_olap_state_admits_plain_vortex_artifact_with_embedded_layout_status() {
    let root = temp_test_root("prepared-olap-plain-single-artifact");
    let source = root.join("source.parquet");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, b"source bytes").expect("write source");
    write_test_prepared_vortex_artifact(&target, 60);
    let source_digest = sha256_file_digest(&source, "test source").expect("source digest");
    let legacy_manifest_path = legacy_prepared_olap_state_manifest_path(&target);
    let request = VortexPreparedOlapStateWriteRequest::new_local(
        "source-state-hits",
        "sha256:source-state-digest",
        "parquet",
        source_digest,
        12,
        "sha256:schema-digest",
        2,
        &target,
        &target,
    )
    .expect("olap request")
    .with_prepared_layout_policy(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_LAYOUT_POLICY)
    .with_segment_map_status(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_SEGMENT_MAP_STATUS);

    let report =
        publish_vortex_prepared_olap_single_artifact_state(&request).expect("publish ready state");

    assert_eq!(report.status, "prepared_olap_state_ready");
    assert!(!legacy_manifest_path.exists());
    assert!(!report.manifest_written);
    assert_eq!(report.exact_sidecar_family_count, 0);
    assert!(!report.sub_second_candidate);
    assert_eq!(
        report.query_time_contract,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_QUERY_TIME_CONTRACT
    );
    assert_eq!(
        report.layout_inventory_status,
        "opened_single_vortex_artifact_footer"
    );
    assert_eq!(report.layout_footer_row_count, "2");
    assert_ne!(report.layout_artifact_size_bytes, "unknown");
    assert_ne!(report.layout_root_encoding, "not_available");
    assert_ne!(report.layout_encoding_inventory, "not_available");
    assert!(report.layout_size_attribution.contains("source_columns="));
    assert!(report.layout_size_attribution.contains("derived_columns="));
    assert!(
        report
            .layout_size_attribution
            .contains("dictionary_status=")
    );
    assert!(
        report
            .layout_per_column_metadata_contract
            .contains(":logical=")
    );
    assert!(
        report
            .layout_per_column_metadata_contract
            .contains("roles=")
    );
    assert_ne!(
        report.layout_operator_selection_metadata_status,
        "per_column_metadata_not_available"
    );
    assert!(
        report
            .layout_segment_membership_status
            .starts_with("segment_map_available")
    );
    assert_eq!(
        report.layout_reader_cache_status,
        "enabled_for_local_metadata_open"
    );
    assert!(report.layout_metadata_persisted_in_artifact);
    assert_eq!(report.blocker_id, "none");
    assert!(
        report
            .admitted_query_families
            .contains("native_vortex_runtime")
    );

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn prepared_olap_state_bundle_reports_single_artifact_without_adjacent_olap_files() {
    let root = temp_test_root("prepared-olap-bundle-single-artifact");
    let source = root.join("source.parquet");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, b"source bytes").expect("write source");
    write_test_prepared_vortex_artifact(&target, 70);
    let source_digest = sha256_file_digest(&source, "test source").expect("source digest");
    let legacy_manifest_path = legacy_prepared_olap_state_manifest_path(&target);
    let request = VortexPreparedOlapStateWriteRequest::new_local(
        "source-state-hits",
        "sha256:source-state-digest",
        "parquet",
        source_digest,
        12,
        "sha256:schema-digest",
        2,
        &target,
        &target,
    )
    .expect("olap request")
    .with_prepared_layout_policy(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_LAYOUT_POLICY)
    .with_segment_map_status(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_SEGMENT_MAP_STATUS);

    let fingerprint_calls = LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get);
    let report =
        write_vortex_prepared_olap_state_bundle(request.clone()).expect("write olap bundle");
    assert_eq!(
        LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get) - fingerprint_calls,
        1,
        "publication must perform one complete artifact checksum, not two"
    );
    assert_eq!(
        report.prepared_artifact_digest,
        sha256_file_digest(&target, "published artifact").expect("artifact checksum")
    );

    assert_eq!(report.status, "prepared_olap_state_ready");
    assert!(!report.manifest_written);
    assert!(!legacy_manifest_path.exists());
    assert_eq!(report.exact_sidecar_family_count, 0);
    assert_eq!(
        report.artifact_model,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_MODEL
    );
    assert_eq!(
        report.metadata_pruning_contract,
        VORTEX_PREPARED_OLAP_METADATA_PRUNING_CONTRACT
    );
    assert_eq!(
        report.layout_inventory_status,
        "opened_single_vortex_artifact_footer"
    );
    assert_eq!(report.layout_footer_row_count, "2");
    assert!(report.layout_metadata_persisted_in_artifact);
    assert!(
        report
            .admitted_query_families
            .contains("native_vortex_runtime")
    );
    let sidecar_dir = target
        .parent()
        .expect("target parent")
        .join(".shardloom")
        .join("prepared.vortex.prepared-olap-state.d");
    assert!(
        !sidecar_dir.exists(),
        "single-artifact prepared OLAP state must not create a .prepared-olap-state.d directory"
    );

    let hit = evaluate_vortex_prepared_olap_single_artifact_state(&request)
        .expect("evaluate olap bundle");
    assert_eq!(hit.status, "prepared_olap_state_read_through_hit");
    assert_eq!(hit.blocker_id, "none");
    assert_eq!(
        hit.query_answer_sidecar_status,
        VORTEX_PREPARED_OLAP_QUERY_ANSWER_SIDECAR_STATUS
    );
    assert!(!hit.fallback_attempted);
    assert!(!hit.external_engine_invoked);

    std::fs::remove_file(&target).expect("remove published artifact");
    assert!(
        write_vortex_prepared_olap_state_bundle(request)
            .expect_err("publication still requires a readable artifact")
            .to_string()
            .contains("failed to stat prepared OLAP state artifact")
    );
    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn prepared_olap_state_bundle_rejects_then_clears_old_sidecar_request_fields() {
    let root = temp_test_root("prepared-olap-clears-sidecar-fields");
    let source = root.join("source.parquet");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, b"source bytes").expect("write source");
    write_test_prepared_vortex_artifact(&target, 80);
    let source_digest = sha256_file_digest(&source, "test source").expect("source digest");
    let legacy_manifest_path = legacy_prepared_olap_state_manifest_path(&target);
    let request = VortexPreparedOlapStateWriteRequest::new_local(
        "source-state-hits",
        "sha256:source-state-digest",
        "parquet",
        source_digest,
        12,
        "sha256:schema-digest",
        2,
        &target,
        &target,
    )
    .expect("olap request")
    .with_prepared_layout_policy(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_LAYOUT_POLICY)
    .with_segment_map_status(VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_SEGMENT_MAP_STATUS)
    .with_substring_index_manifest("sidecar:old-query-summary")
    .with_aggregate_summary_manifest("sidecar:old-aggregate-summary")
    .with_row_position_manifest("sidecar:old-row-position")
    .with_dictionary_union_manifest("sidecar:old-dictionary-union")
    .with_derived_column_manifest("sidecar|derived_column|sha256:copied|copied.sidecar");
    let blocked = publish_vortex_prepared_olap_single_artifact_state(&request)
        .expect("reject sidecar request");
    assert_eq!(
        blocked.status,
        "blocked_query_answer_sidecar_not_public_runtime"
    );
    assert_eq!(
        blocked.blocker_id,
        "prepared_olap_state.query_answer_sidecar_not_public_runtime"
    );
    assert_eq!(
        blocked.query_answer_sidecar_status,
        "rejected_declared_query_answer_sidecar_not_public_runtime"
    );
    assert_eq!(
        blocked.layout_inventory_status,
        "opened_single_vortex_artifact_footer"
    );

    let report =
        write_vortex_prepared_olap_state_bundle(request.clone()).expect("write olap bundle");

    let still_blocked = evaluate_vortex_prepared_olap_single_artifact_state(&request)
        .expect("evaluate original sidecar fields");
    let sanitized_request = request
        .clone()
        .with_substring_index_manifest("none")
        .with_aggregate_summary_manifest("none")
        .with_row_position_manifest("none")
        .with_dictionary_union_manifest("none")
        .with_derived_column_manifest("none");
    let decision = evaluate_vortex_prepared_olap_single_artifact_state(&sanitized_request)
        .expect("evaluate cleared sidecar fields");

    assert_eq!(report.status, "prepared_olap_state_ready");
    assert!(!report.manifest_written);
    assert!(!legacy_manifest_path.exists());
    assert_eq!(report.exact_sidecar_family_count, 0);
    assert_eq!(
        report.query_answer_sidecar_status,
        VORTEX_PREPARED_OLAP_QUERY_ANSWER_SIDECAR_STATUS
    );
    assert_eq!(
        report.layout_inventory_status,
        "opened_single_vortex_artifact_footer"
    );
    assert_eq!(
        still_blocked.status,
        "blocked_query_answer_sidecar_not_public_runtime"
    );
    assert_eq!(still_blocked.exact_sidecar_family_count, 5);
    assert_eq!(decision.status, "prepared_olap_state_read_through_hit");
    assert_eq!(decision.exact_sidecar_family_count, 0);
    assert_eq!(
        decision.query_time_contract,
        VORTEX_PREPARED_OLAP_SINGLE_ARTIFACT_QUERY_TIME_CONTRACT
    );
    assert!(!decision.fallback_attempted);
    assert!(!decision.external_engine_invoked);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[cfg(unix)]
#[test]
fn prepared_state_reuse_manifest_rejects_symlink_sidecar_directory() {
    use std::os::unix::fs::symlink;

    let root = temp_test_root("reuse-manifest-symlink");
    let outside = temp_test_root("reuse-manifest-symlink-outside");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, "id,label\n1,alpha\n").expect("write source");
    std::fs::write(&target, b"prepared-vortex-artifact").expect("write prepared artifact");
    symlink(&outside, root.join(".shardloom")).expect("sidecar symlink");
    let manifest_path = vortex_prepared_state_reuse_manifest_path(&target).expect("manifest path");
    let request = reuse_request_for_test(&source, &target, &manifest_path);
    let decision = evaluate_vortex_prepared_state_reuse(&request).expect("reuse decision");
    let artifact_digest = sha256_file_digest(&target, "test artifact").expect("artifact digest");

    let error = write_vortex_prepared_state_reuse_manifest(
        &request,
        &decision,
        VortexPreparedStateReuseWriteEvidence {
            source_state_id: "source-state-base".to_string(),
            source_state_digest: "fnv64:source-base".to_string(),
            source_schema_digest: "fnv64:schema".to_string(),
            source_row_count: 1,
            source_column_family_summary: "id:int64,label:utf8".to_string(),
            prepared_state_id: "prepared-base".to_string(),
            prepared_state_digest: "fnv64:prepared-base".to_string(),
            prepared_artifact_digest: artifact_digest,
            certificate_refs: "reopen_row_count_scan".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .expect_err("workspace-safe manifest writer rejects symlink sidecars");

    assert!(error.to_string().contains("symlink"));
    assert!(
        !outside
            .join("prepared.vortex.prepared-state-reuse.manifest")
            .exists()
    );
    let _ = std::fs::remove_file(root.join(".shardloom"));
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(outside);
}

#[test]
fn append_only_refinement_blocks_prefix_mismatch() {
    let root = temp_test_root("append-only-refinement-prefix");
    let source = root.join("input.csv");
    let target = root.join("prepared.vortex");
    std::fs::write(&source, "id,label\n1,alpha\n2,beta\n").expect("write base source");
    std::fs::write(&target, b"prepared-vortex-artifact").expect("write prepared artifact");
    let manifest_path = vortex_prepared_state_reuse_manifest_path(&target).expect("manifest path");
    let base_request = reuse_request_for_test(&source, &target, &manifest_path);
    let base_decision =
        evaluate_vortex_prepared_state_reuse(&base_request).expect("base reuse decision");
    let artifact_digest = sha256_file_digest(&target, "test artifact").expect("artifact digest");
    write_vortex_prepared_state_reuse_manifest(
        &base_request,
        &base_decision,
        VortexPreparedStateReuseWriteEvidence {
            source_state_id: "source-state-base".to_string(),
            source_state_digest: "fnv64:source-base".to_string(),
            source_schema_digest: "fnv64:schema".to_string(),
            source_row_count: 2,
            source_column_family_summary: "id:int64,label:utf8".to_string(),
            prepared_state_id: "prepared-base".to_string(),
            prepared_state_digest: "fnv64:prepared-base".to_string(),
            prepared_artifact_digest: artifact_digest,
            certificate_refs: "reopen_row_count_scan".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .expect("write reuse manifest");

    std::fs::write(&source, "id,label\n1,ALPHA\n2,beta\n3,gamma\n")
        .expect("rewrite and append source");
    let current_request = reuse_request_for_test(&source, &target, &manifest_path);
    let decision = evaluate_vortex_prepared_state_append_only_refinement(&current_request)
        .expect("refinement decision");

    assert!(!decision.is_admitted());
    assert_eq!(decision.status, "blocked_source_prefix_digest_mismatch");
    assert_eq!(decision.blocker_id, "source_prefix_digest_mismatch");
    assert!(!decision.fallback_attempted);
    assert!(!decision.external_engine_invoked);

    std::fs::remove_dir_all(root).expect("remove temp root");
}

#[test]
fn differential_refinement_manifest_is_digest_backed() {
    let root = temp_test_root("append-only-refinement-manifest");
    let manifest_path = root.join("refinement.manifest");
    let decision = VortexPreparedStateAppendOnlyRefinementDecision {
        schema_version: VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION,
        status: "admitted_append_only_refinement".to_string(),
        policy: VORTEX_DIFFERENTIAL_REFINEMENT_POLICY,
        reason: "source_prefix_verified_and_delta_bytes_detected".to_string(),
        blocker_id: "none".to_string(),
        automatic_detection_status: "append_only_delta_detected".to_string(),
        manifest_path: root.join("reuse.manifest"),
        reuse_manifest_digest: "fnv64:reuse".to_string(),
        source_path: root.join("input.csv"),
        source_format: "csv".to_string(),
        base_source_content_digest: "fnv64:base-source".to_string(),
        current_source_content_digest: "fnv64:current-source".to_string(),
        base_source_size_bytes: 10,
        current_source_size_bytes: 20,
        delta_byte_start: 10,
        delta_byte_end: 20,
        changed_byte_range_refs: "input.csv#bytes=10..20".to_string(),
        source_prefix_verification_status: "verified_old_source_bytes_are_current_prefix"
            .to_string(),
        base_source_state_id: "source-state-base".to_string(),
        base_source_state_digest: "fnv64:source-base".to_string(),
        base_source_row_count: 2,
        base_source_schema_digest: "fnv64:schema".to_string(),
        base_source_column_family_summary: "id:int64,label:utf8".to_string(),
        base_prepared_state_id: "prepared-base".to_string(),
        base_prepared_state_digest: "fnv64:prepared-base".to_string(),
        prepared_artifact_ref: root.join("prepared.vortex"),
        prepared_artifact_digest: "fnv64:artifact".to_string(),
        prepared_artifact_size_bytes: 64,
        certificate_refs: "reopen_row_count_scan".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    };
    let differential = evaluate_vortex_differential_preparation(differential_input(
        VortexDifferentialUpdateMode::AppendOnly,
    ));

    let report = write_vortex_differential_refinement_manifest(
        &manifest_path,
        &decision,
        &differential,
        "refined-prepared",
        "fnv64:refined",
        "count",
        "admitted_base_manifest_plus_delta_reopen_row_count",
        "fnv64:consumer",
    )
    .expect("write refinement manifest");

    assert!(report.manifest_written);
    assert!(report.manifest_digest.starts_with("sha256:"));
    let manifest = std::fs::read_to_string(&manifest_path).expect("read manifest");
    assert!(
        manifest.contains("schema_version=shardloom.vortex_differential_refinement_manifest.v1")
    );
    assert!(manifest.contains("overlay_consumer_family=count"));
    assert!(manifest.contains("manifest_digest=sha256:"));

    std::fs::remove_dir_all(root).expect("remove temp root");
}

fn differential_input(
    update_mode: VortexDifferentialUpdateMode,
) -> VortexDifferentialPreparationInput {
    VortexDifferentialPreparationInput {
        update_mode,
        base_source_state_id: "local-csv-base".to_string(),
        base_source_state_digest: "fnv64:base-source".to_string(),
        base_prepared_state_id: "vortex-prepared-state-base".to_string(),
        base_prepared_state_digest: "fnv64:base-prepared".to_string(),
        base_row_count: 2,
        base_schema_digest: "fnv64:schema".to_string(),
        base_column_family_summary: "id:int64,label:utf8".to_string(),
        delta_source_state_id: "local-csv-delta".to_string(),
        delta_source_state_digest: "fnv64:delta-source".to_string(),
        delta_row_count: 1,
        delta_schema_digest: "fnv64:schema".to_string(),
        delta_column_family_summary: "id:int64,label:utf8".to_string(),
        delta_manifest_digest: "fnv64:delta-manifest".to_string(),
        changed_byte_range_refs: "local-csv-delta:split=1:bytes=0..32".to_string(),
        changed_row_range_refs: "local-csv-delta:split=1:rows=0..1".to_string(),
        changed_segment_refs: "vortex-prepared-state-delta:rows=0..1:digest=fnv64:delta"
            .to_string(),
        delta_artifact_ref: "target/delta.vortex".to_string(),
        delta_artifact_digest: "fnv64:delta-artifact".to_string(),
        native_io_certificate_refs: "base_prepared_state,delta_artifact,reopen_row_count_scan"
            .to_string(),
    }
}

fn scout_input(
    anomaly_count: u64,
    anomaly_families: &str,
    unsupported_shape_status: &str,
    quarantine_required: bool,
) -> VortexScoutIngressInput {
    VortexScoutIngressInput {
        source_state_id: "local-jsonl-source".to_string(),
        source_state_digest: "fnv64:source".to_string(),
        source_format: "jsonl".to_string(),
        source_path: "target/source.jsonl".to_string(),
        source_schema_digest: "fnv64:schema".to_string(),
        row_count: 2,
        source_byte_count: 128,
        column_count: 2,
        read_plan: "full_columns".to_string(),
        metadata_range_refs: "local-jsonl-source:split=1:bytes=0..128".to_string(),
        sampled_row_range_refs: "local-jsonl-source:split=1:rows=0..2".to_string(),
        anomaly_count,
        anomaly_families: anomaly_families.to_string(),
        malformed_row_refs: "none".to_string(),
        schema_drift_status: "not_detected_no_prior_schema_baseline".to_string(),
        unsupported_shape_status: unsupported_shape_status.to_string(),
        nullability_status: "nullable_fields_admitted_as_scalar_nulls".to_string(),
        small_file_pathology_status: "observed_tiny_local_fixture_not_blocking".to_string(),
        quarantine_required,
        quarantine_output_plan_status: "planned_not_emitted_no_quarantine_sink_requested"
            .to_string(),
        quarantine_output_ref: "not_emitted".to_string(),
        quarantine_output_digest: "not_emitted".to_string(),
        redaction_status: "malformed_row_refs_are_row_numbers_only".to_string(),
        unsupported_diagnostic_code: "none".to_string(),
        correctness_policy: "fail_closed_no_silent_repair_or_row_drop".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

pub(super) fn layout_advisor_input(
    strategy_admitted: bool,
    unsupported_diagnostic_code: &str,
) -> VortexLayoutWriteAdvisorInput {
    VortexLayoutWriteAdvisorInput {
        source_state_id: "local-csv-source".to_string(),
        source_state_digest: "fnv64:source".to_string(),
        source_format: "csv".to_string(),
        source_schema_digest: "fnv64:schema".to_string(),
        row_count: 2,
        source_byte_count: 128,
        column_count: 2,
        workload_constitution: "prepare_once_local_fixture".to_string(),
        source_statistics_status: "local_source_file_stats_only".to_string(),
        requested_pushdown_requirements: "none_prepare_once".to_string(),
        sink_requirements: "workspace_safe_local_vortex_file_sink".to_string(),
        layout_strategy: "single_vortex_artifact_embedded_olap_layout_statistics".to_string(),
        chunking_strategy: "upstream_vortex_writer_default_zoned_row_blocks".to_string(),
        segmentation_strategy: "upstream_vortex_writer_default_zoned_segments".to_string(),
        dictionary_strategy:
            "preserve_vortex_dictionary_and_encoding_metadata_when_writer_emits_it".to_string(),
        statistics_policy:
            "preserve_vortex_layout_footer_statistics_for_metadata_pruning".to_string(),
        writer_provider_kind: "shardloom_kernel".to_string(),
        writer_provider_surface:
            "shardloom_scalar_rows_to_vortex_struct;VortexSession::write_options().write(ArrayStream)"
                .to_string(),
        writer_admission_policy: VORTEX_PRODUCT_LOCAL_INGEST_PREPARE_ONCE_ADMISSION_POLICY
            .to_string(),
        writer_parallelism_budget: 1,
        writer_compression_candidate_fields: Vec::new(),
        write_reopen_verification_depth: "writer_and_reopen_row_count".to_string(),
        materialization_boundary_status: "materialized_scalar_rows_before_write".to_string(),
        decode_boundary_status: "compatibility_parse_to_scalar_values".to_string(),
        expected_read_tradeoff: "not_claimed_fixture_layout".to_string(),
        expected_write_tradeoff: "not_claimed_fixture_layout".to_string(),
        strategy_admitted,
        unsupported_diagnostic_code: unsupported_diagnostic_code.to_string(),
        correctness_refs: "writer_reopen_row_count".to_string(),
        benchmark_refs: "not_claim_grade_no_benchmark_refresh".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn copy_budget_input(
    measurement_status: &str,
    unsafe_lifetime_shortcut_status: &str,
    buffer_reuse_status: &str,
) -> VortexCopyBudgetInput {
    VortexCopyBudgetInput {
        source_state_id: "local-csv-source".to_string(),
        source_state_digest: "fnv64:source".to_string(),
        prepared_state_id: "vortex-prepared-state-fnv64-prepared".to_string(),
        prepared_state_digest: "fnv64:prepared".to_string(),
        source_format: "csv".to_string(),
        row_count: 2,
        source_byte_count: 128,
        column_count: 2,
        allocation_scope: "vortex_ingest_local_prepare_once".to_string(),
        copy_scope: "source_read,parse_normalization,vortex_array_build,writer,reopen,evidence"
            .to_string(),
        measurement_status: measurement_status.to_string(),
        source_read_copy_bytes: "128".to_string(),
        parse_normalization_copy_bytes: "not_measured".to_string(),
        columnar_handoff_copy_bytes: "not_applicable_scalar_rows".to_string(),
        vortex_array_build_copy_bytes: "not_measured".to_string(),
        writer_buffer_bytes: "256".to_string(),
        reopen_verify_copy_bytes: "not_measured".to_string(),
        evidence_render_copy_bytes: "not_measured".to_string(),
        total_measured_copy_bytes: "384".to_string(),
        buffer_family: "source_bytes,scalar_rows,vortex_writer_buffer".to_string(),
        ownership_policy: "owned_buffers_no_borrowed_lifetime_reuse".to_string(),
        writer_buffering_status: "writer_buffer_bytes_reported".to_string(),
        buffer_reuse_status: buffer_reuse_status.to_string(),
        buffer_reuse_count: 0,
        unsafe_lifetime_shortcut_status: unsafe_lifetime_shortcut_status.to_string(),
        correctness_parity_refs: "writer_reopen_row_count".to_string(),
        materialization_boundary_status: "materialized_scalar_rows_before_write".to_string(),
        decode_boundary_status: "compatibility_parse_to_scalar_values".to_string(),
        unsupported_diagnostic_code: "none".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn capillary_input(native_io_certificate_status: &str) -> VortexCapillaryPreparationInput {
    VortexCapillaryPreparationInput {
        source_state_id: "local-csv-source".to_string(),
        source_state_digest: "fnv64:source".to_string(),
        prepared_state_id: "vortex-prepared-state-source".to_string(),
        prepared_state_digest: "fnv64:prepared".to_string(),
        source_surface: "local_text_source_state_scalar_rows".to_string(),
        sink_surface: "workspace_safe_local_vortex_file_sink".to_string(),
        format_family: "csv".to_string(),
        operation_class: "vortex_ingest_prepare_once".to_string(),
        certification_depth: "ingest_certified".to_string(),
        row_count: 4,
        source_byte_count: 128,
        column_count: 2,
        source_split_refs: "local-csv-source:split=0".to_string(),
        source_byte_range_refs: "local-csv-source:split=0:bytes=0..128".to_string(),
        source_row_range_refs: "local-csv-source:split=0:rows=0..4".to_string(),
        projection_mask: "id,label".to_string(),
        filter_mask_status: "none".to_string(),
        prepared_artifact_ref: "target/source.vortex".to_string(),
        prepared_artifact_digest: "fnv64:artifact".to_string(),
        prepared_artifact_segment_refs:
            "vortex-prepared-state-source:rows=0..4:digest=fnv64:artifact".to_string(),
        writer_sink_refs: "target/source.vortex".to_string(),
        materialization_boundary_status: "scalar_rows_materialized_before_vortex_array_build"
            .to_string(),
        decode_boundary_status: "local_text_parse_to_scalar_values".to_string(),
        native_io_certificate_status: native_io_certificate_status.to_string(),
        native_io_certificate_refs: "source_split_refs,prepared_artifact_ref,reopen_row_count_scan"
            .to_string(),
        correctness_digest: "fnv64:correct".to_string(),
        memory_budget_bytes: 16 * 1024 * 1024,
        max_parallelism: 2,
        result_sink_requested: false,
        result_sink_replay_verified: false,
        capillary_claim_evidence_requested: true,
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn capillary_prewrite_test_input(
    target_path: &Path,
    row_count: u64,
    column_count: usize,
) -> VortexCapillaryPreparationInput {
    let mut input = capillary_input("certified");
    input.prepared_artifact_ref = target_path.display().to_string();
    input.prepared_artifact_digest = "prewrite_pending_until_vortex_writer_certificate".to_string();
    input.writer_sink_refs = target_path.display().to_string();
    input.row_count = row_count;
    input.column_count = column_count;
    input.certification_depth = VortexIngestCertificationLevel::IngestCertified
        .as_str()
        .to_string();
    input.capillary_claim_evidence_requested = true;
    input
}
