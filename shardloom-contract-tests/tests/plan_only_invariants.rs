use shardloom_core::{
    DatasetRef, DatasetUri, ExtensionId, ExtensionLicenseKind, ExtensionManifest,
    ExtensionProvenance, ExtensionVersion, OutputTarget, SecurityPlan,
};
use shardloom_exec::{RecoveryPlan, RecoveryReport, RuntimePlanSkeleton, StreamingPlanSkeleton};
use shardloom_plan::{ScanMode, ScanRequest};
use shardloom_vortex::{VortexFileRef, VortexReadPlan, VortexWriteOptions, VortexWritePlan};

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
