use shardloom_core::{
    AgentContractPack, ByteRange, ColumnRef, DatasetFormat, DatasetManifest, DatasetRef,
    DatasetUri, EffectBudgetReport, EncodedSegment, EncodingKind, ExtensionId,
    ExtensionLicenseKind, ExtensionManifest, ExtensionProvenance, ExtensionVersion, FileDescriptor,
    FileRole, LayoutKind, LogicalDType, ManifestId, ManifestSegment, Nullability, OutputTarget,
    SecurityPlan, SegmentId, SegmentLayout, SegmentStats, SnapshotId, SnapshotRef,
    TableIntelligenceReport,
};
use shardloom_exec::{RecoveryPlan, RecoveryReport, RuntimePlanSkeleton, StreamingPlanSkeleton};
use shardloom_plan::{
    ObjectStoreCheckpointRetryInput, ObjectStoreCommitProtocolInput,
    ObjectStoreDistributedSchedulingPolicy, ObjectStoreRangePlanningPolicy,
    plan_object_store_checkpoint_retry, plan_object_store_commit_protocol,
    plan_object_store_distributed_scheduling, plan_object_store_ranges,
    plan_object_store_request_coalescing, plan_object_store_request_planner,
};
use shardloom_plan::{ScanMode, ScanRequest};
use shardloom_vortex::{VortexFileRef, VortexReadPlan, VortexWriteOptions, VortexWritePlan};

fn object_store_manifest_fixture() -> DatasetManifest {
    let uri = DatasetUri::new("s3://bucket/table.vortex").expect("uri");
    let mut manifest = DatasetManifest::new(
        ManifestId::new("object-store-plan").expect("manifest id"),
        DatasetRef::from_uri(uri.clone()).expect("dataset ref"),
        SnapshotRef::new(SnapshotId::new("object-store-snapshot").expect("snapshot id")),
    );
    let file = FileDescriptor::new(uri, DatasetFormat::Vortex, FileRole::NativeVortexData)
        .with_size_bytes(128 * 1024 * 1024);

    for (index, start) in [0, 8192, 16_384].into_iter().enumerate() {
        let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
        layout.byte_ranges = vec![ByteRange::new(start, 1024)];
        layout.physical_size_bytes = Some(1024);
        let segment = EncodedSegment::new(
            SegmentId::new(format!("s{index}")).expect("segment id"),
            ColumnRef::new("c").expect("column"),
            LogicalDType::Int64,
            Nullability::Nullable,
            layout,
            SegmentStats::with_row_count(1024),
        );
        manifest.add_segment(ManifestSegment::new(segment, file.clone()));
    }

    manifest.add_file(file);
    manifest
}

#[test]
fn plan_only_types_do_not_imply_execution_or_side_effects() {
    let vortex_dataset =
        DatasetRef::from_uri(DatasetUri::new("file://tmp/in.vortex").expect("uri")).expect("ds");
    let scan = ScanRequest::new(vortex_dataset.clone());
    assert_eq!(scan.mode, ScanMode::PlanOnly);
    assert!(!scan.requires_execution());

    let file = VortexFileRef::new(vortex_dataset.clone()).expect("vortex");
    let read = VortexReadPlan::metadata_only(file.clone());
    assert_eq!(read.status.as_str(), "metadata_only");
    assert!(!read.has_errors());

    let write = VortexWritePlan::planned(file, VortexWriteOptions::native_defaults());
    assert_eq!(write.fidelity.as_str(), "native_full_fidelity");
    assert_eq!(write.status.as_str(), "planned");

    let runtime = RuntimePlanSkeleton::for_dataset(vortex_dataset.clone()).expect("runtime plan");
    assert_eq!(runtime.status.as_str(), "planned");
    assert!(
        !runtime
            .to_human_text()
            .contains("fallback_execution=enabled")
    );

    let target_vortex =
        OutputTarget::from_uri(DatasetUri::new("file://tmp/out.vortex").expect("uri"));
    let target_parquet =
        OutputTarget::from_uri(DatasetUri::new("file://tmp/out.parquet").expect("uri"));

    let stream_vortex =
        StreamingPlanSkeleton::for_vortex_to_target(vortex_dataset.clone(), target_vortex);
    assert!(!stream_vortex.requires_materialization());

    let stream_parquet =
        StreamingPlanSkeleton::for_vortex_to_target(vortex_dataset, target_parquet);
    assert!(stream_parquet.requires_materialization());
    assert!(!stream_parquet.sink.requirement.preserves_metadata);

    let recovery = RecoveryReport::from_plan(&RecoveryPlan::diagnostic_only());
    assert_eq!(recovery.status.as_str(), "diagnostic_only");
    assert_eq!(recovery.actions_completed, 0);

    let obs = shardloom_core::RuntimeObservabilityReport::from_plan(
        &shardloom_core::ObservabilityPlan::default_foundation_plan(),
    );
    assert_eq!(obs.metrics.len(), 0);

    let sec = SecurityPlan::default_safe();
    assert!(!sec.allows_external_effects());

    let effect_budget = EffectBudgetReport::planning_default();
    assert!(effect_budget.side_effect_free());
    assert_eq!(effect_budget.approved_scope_count(), 0);
    assert!(!effect_budget.fallback_execution_allowed);

    let agent_contract = AgentContractPack::default_pack();
    assert!(agent_contract.side_effect_free());
    assert_eq!(agent_contract.fallback_allowed_surface_count(), 0);
    assert!(!agent_contract.text_is_authoritative);

    let table_intelligence = TableIntelligenceReport::report_only_foundation();
    assert!(table_intelligence.side_effect_free());
    assert_eq!(table_intelligence.required_cg9_surface_count(), 10);
    assert_eq!(table_intelligence.report_only_available_surface_count(), 7);
    assert!(!table_intelligence.catalog_io_performed);
    assert!(!table_intelligence.table_metadata_io_performed);
    assert!(!table_intelligence.fallback_execution_allowed);

    let object_store_manifest = object_store_manifest_fixture();
    let range_policy = ObjectStoreRangePlanningPolicy {
        max_coalesce_gap_bytes: 0,
        ..ObjectStoreRangePlanningPolicy::default()
    };
    let range_report = plan_object_store_ranges(object_store_manifest.clone(), range_policy);
    let coalescing_report =
        plan_object_store_request_coalescing(object_store_manifest, range_policy);
    let scheduling_report = plan_object_store_distributed_scheduling(
        coalescing_report.clone(),
        ObjectStoreDistributedSchedulingPolicy {
            max_requests_per_task: 1,
            max_task_count: 4,
        },
    );
    let checkpoint_retry_report = plan_object_store_checkpoint_retry(
        ObjectStoreCheckpointRetryInput::new(scheduling_report.clone())
            .with_retry_policy(true)
            .with_checkpoint_plan(true)
            .with_idempotency_keys(true)
            .with_attempt_record(true)
            .with_cleanup_policy(true),
    );
    let commit_report = plan_object_store_commit_protocol(
        ObjectStoreCommitProtocolInput::new(
            DatasetUri::new("s3://bucket/table/_commit").expect("uri"),
        )
        .with_staging_prefix(true)
        .with_manifest_pointer_update(true)
        .with_commit_record(true)
        .with_idempotency_key(true)
        .with_cleanup_plan(true)
        .with_provider_atomic_commit(true),
    );
    let object_store_request = plan_object_store_request_planner(
        range_report,
        coalescing_report,
        scheduling_report,
        checkpoint_retry_report,
        commit_report,
    );
    assert!(object_store_request.side_effect_free());
    assert_eq!(object_store_request.planned_surface_count, 5);
    assert!(!object_store_request.object_store_io);
    assert!(!object_store_request.write_io);
    assert!(!object_store_request.fallback_execution_allowed);

    let manifest = ExtensionManifest::new(
        ExtensionId::new("ext.sample").expect("id"),
        "sample",
        ExtensionVersion::new(0, 1, 0),
        shardloom_core::ExtensionCategory::Connector,
        ExtensionProvenance::new(ExtensionLicenseKind::Apache2),
    )
    .expect("manifest");
    let inspection = shardloom_core::ExtensionInspectionReport::metadata_only(manifest);
    assert!(!inspection.code_executed);

    let release = shardloom_core::ReleaseReport::from_plan(
        shardloom_core::ReleasePlan::default_foundation_plan(),
    );
    assert!(!release.published);
    assert_eq!(release.artifacts_published, 0);
}

#[test]
fn encoded_read_surfaces_do_not_materialize_or_arrow_convert() {
    let target_uri = DatasetUri::new("file://tmp/cg13.vortex").expect("uri");

    let boundary = shardloom_vortex::plan_vortex_encoded_read_boundary(
        shardloom_vortex::VortexEncodedReadBoundaryRequest::new(target_uri.clone())
            .upstream_open_options_available(true)
            .upstream_footer_available(true)
            .upstream_metadata_surface_available(true)
            .upstream_scan_surface_deferred(true)
            .local_path_only(true)
            .feature_gate_enabled(true),
    )
    .expect("boundary");
    assert!(!boundary.data_read());
    assert!(!boundary.array_decoded());
    assert!(!boundary.values_materialized());
    assert!(!boundary.arrow_converted());
    assert!(!boundary.object_store_io());
    assert!(!boundary.data_written());
    assert!(!boundary.upstream_scan_called());
    assert!(!boundary.fallback_execution_allowed());
    assert!(boundary.is_side_effect_free());

    let fixture_ref = shardloom_vortex::VortexEncodedReadFixtureRef::new("fixtures/missing.vortex")
        .expect("fixture ref");
    let fixture = shardloom_vortex::plan_vortex_encoded_read_fixture(
        shardloom_vortex::encoded_read_fixture_request_from_boundary_report(
            target_uri.clone(),
            fixture_ref,
            &boundary,
        ),
    )
    .expect("fixture");
    assert!(!fixture.encoded_data_read());
    assert!(!fixture.row_read());
    assert!(!fixture.array_decoded());
    assert!(!fixture.values_materialized());
    assert!(!fixture.arrow_converted());
    assert!(!fixture.object_store_io());
    assert!(!fixture.data_written());
    assert!(!fixture.upstream_scan_called());
    assert!(!fixture.fallback_execution_allowed());
    assert!(fixture.is_side_effect_free());

    let probe = shardloom_vortex::probe_vortex_encoded_read_metadata(
        shardloom_vortex::encoded_read_metadata_probe_request_from_fixture_report(
            target_uri.clone(),
            shardloom_vortex::VortexEncodedReadFixtureRef::new("fixtures/missing.vortex")
                .expect("fixture ref"),
            &fixture,
        ),
    )
    .expect("probe");
    assert!(!probe.encoded_data_read());
    assert!(!probe.row_read());
    assert!(!probe.array_decoded());
    assert!(!probe.values_materialized());
    assert!(!probe.arrow_converted());
    assert!(!probe.object_store_io());
    assert!(!probe.data_written());
    assert!(!probe.upstream_scan_called());
    assert!(!probe.fallback_execution_allowed());
    assert!(probe.is_side_effect_free());
}

#[test]
fn query_readiness_surfaces_do_not_materialize_or_arrow_convert() {
    let target_uri = DatasetUri::new("file://tmp/cg13q.vortex").expect("uri");

    let probe_report = shardloom_vortex::probe_vortex_encoded_read_metadata(
        shardloom_vortex::VortexEncodedReadMetadataProbeRequest::new(
            target_uri.clone(),
            shardloom_vortex::VortexEncodedReadFixtureRef::new("fixtures/missing.vortex")
                .expect("fixture ref"),
        )
        .fixture_ready(true)
        .fixture_ref_provided(true)
        .local_path_only(true)
        .feature_gate_enabled(true),
    )
    .expect("probe report");

    let boundary = shardloom_vortex::plan_vortex_metadata_async_boundary(
        shardloom_vortex::metadata_async_boundary_request_from_metadata_probe_report(
            target_uri.clone(),
            shardloom_vortex::VortexEncodedReadFixtureRef::new("fixtures/missing.vortex")
                .expect("fixture ref"),
            &probe_report,
        ),
    )
    .expect("metadata async boundary");
    assert!(!boundary.encoded_data_read());
    assert!(!boundary.row_read());
    assert!(!boundary.array_decoded());
    assert!(!boundary.values_materialized());
    assert!(!boundary.arrow_converted());
    assert!(!boundary.object_store_io());
    assert!(!boundary.data_written());
    assert!(!boundary.upstream_scan_called());
    assert!(!boundary.fallback_execution_allowed());
    assert!(boundary.is_side_effect_free());

    let invocation = shardloom_vortex::VortexMetadataAsyncInvocationReport {
        status: shardloom_vortex::VortexMetadataAsyncInvocationStatus::BlockedByBoundary,
        boundary_report: boundary.clone(),
        effects_performed: vec![],
        metadata_summary: None,
        metadata_summary_report: None,
        footer_summary: None,
        diagnostics: vec![],
    };
    assert!(!invocation.metadata_opened());
    assert!(!invocation.footer_inspected());
    assert!(!invocation.async_runtime_started());
    assert!(!invocation.metadata_footer_opened());
    assert!(!invocation.encoded_data_read());
    assert!(!invocation.row_read());
    assert!(!invocation.array_decoded());
    assert!(!invocation.values_materialized());
    assert!(!invocation.arrow_converted());
    assert!(!invocation.object_store_io());
    assert!(!invocation.data_written());
    assert!(!invocation.upstream_scan_called());
    assert!(!invocation.fallback_execution_allowed());
    assert!(invocation.is_side_effect_free());

    let primitive = shardloom_vortex::plan_vortex_query_primitive(
        shardloom_vortex::query_primitive_request_from_metadata_async_invocation(
            target_uri,
            shardloom_vortex::VortexQueryPrimitiveBoundaryKind::Count,
            &invocation,
        )
        .encoded_data_path_ready(false),
    )
    .expect("query primitive");
    assert!(!primitive.query_executed());
    assert!(!primitive.encoded_data_read());
    assert!(!primitive.row_read());
    assert!(!primitive.array_decoded());
    assert!(!primitive.values_materialized());
    assert!(!primitive.arrow_converted());
    assert!(!primitive.object_store_io());
    assert!(!primitive.data_written());
    assert!(!primitive.upstream_scan_called());
    assert!(!primitive.fallback_execution_allowed());
    assert!(primitive.is_side_effect_free());
}
