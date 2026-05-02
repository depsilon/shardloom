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
