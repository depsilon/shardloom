use shardloom_core::{
    CompatibilityOutputWriterMatrixReport, CompatibilityOutputWriterSupportStatus, DatasetFormat,
    DatasetRef, DatasetUri, FidelityLevel, MetadataPreservationStatus, OutputTarget,
    OutputTargetKind, TranslationPlan, TranslationReport,
};
use shardloom_vortex::VortexOutputFidelity;

#[test]
fn output_target_default_fidelity_is_stable() {
    assert_eq!(
        OutputTargetKind::Vortex.default_fidelity(),
        FidelityLevel::NativeFullFidelity
    );
    assert_eq!(
        OutputTargetKind::Parquet.default_fidelity(),
        FidelityLevel::CompatibilityLossyPhysical
    );
    assert_ne!(
        OutputTargetKind::ArrowIpc.default_fidelity(),
        FidelityLevel::NativeFullFidelity
    );
    assert!(OutputTargetKind::IcebergCompatible.is_compatibility_output());
    assert!(OutputTargetKind::DeltaCompatible.is_compatibility_output());
}

#[test]
fn vortex_fidelity_mapping_is_stable() {
    assert_eq!(
        VortexOutputFidelity::NativeFullFidelity.to_core_fidelity(),
        FidelityLevel::NativeFullFidelity
    );
    assert_eq!(
        VortexOutputFidelity::NativePartialFidelity.to_core_fidelity(),
        FidelityLevel::NativePartialFidelity
    );
    assert_eq!(
        VortexOutputFidelity::Unsupported.to_core_fidelity(),
        FidelityLevel::Unsupported
    );
}

#[test]
fn translation_plan_and_report_respect_native_vs_compatibility_contract() {
    let vortex = OutputTarget::from_uri(DatasetUri::new("file://tmp/out.vortex").expect("uri"));
    let parquet = OutputTarget::from_uri(DatasetUri::new("file://tmp/out.parquet").expect("uri"));

    let vortex_plan = TranslationPlan::for_target(vortex);
    assert_eq!(vortex_plan.fidelity, FidelityLevel::NativeFullFidelity);

    let parquet_plan = TranslationPlan::for_target(parquet);
    assert_eq!(
        parquet_plan.fidelity,
        FidelityLevel::CompatibilityLossyPhysical
    );
    assert!(
        parquet_plan
            .metadata
            .iter()
            .any(|m| m.status != MetadataPreservationStatus::Preserved)
    );

    let report = TranslationReport::from_plan(&parquet_plan);
    assert!(report.has_metadata_loss());
    assert_ne!(vortex_plan.fidelity, parquet_plan.fidelity);

    assert_eq!(
        OutputTargetKind::from_dataset_format(&DatasetFormat::Vortex),
        OutputTargetKind::Vortex
    );
    assert_eq!(
        OutputTargetKind::from_dataset_format(&DatasetFormat::Parquet),
        OutputTargetKind::Parquet
    );

    let _dataset = DatasetRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("uri"))
        .expect("dataset");
}

#[test]
fn compatibility_output_writer_matrix_respects_claim_boundaries() {
    let matrix = CompatibilityOutputWriterMatrixReport::current();
    let arrow_ipc = matrix
        .row_for_kind(&OutputTargetKind::ArrowIpc)
        .expect("arrow ipc writer row");
    let iceberg = matrix
        .row_for_kind(&OutputTargetKind::IcebergCompatible)
        .expect("iceberg writer row");

    assert_eq!(
        arrow_ipc.support_status,
        CompatibilityOutputWriterSupportStatus::LocalCompatibilityExportAdmitted
    );
    assert_eq!(
        arrow_ipc.support_status.claim_gate_status(),
        "not_claim_grade"
    );
    assert!(arrow_ipc.local_file_output);
    assert!(!arrow_ipc.object_store_output);
    assert!(!arrow_ipc.table_commit_semantics);

    assert_eq!(
        iceberg.support_status,
        CompatibilityOutputWriterSupportStatus::ReportOnlyBlocked
    );
    assert_eq!(
        iceberg.support_status.claim_gate_status(),
        "not_claim_grade"
    );
    assert!(!iceberg.local_file_output);
    assert!(!iceberg.object_store_output);
    assert!(!iceberg.table_commit_semantics);

    assert!(!matrix.fallback_attempted);
    assert!(!matrix.external_engine_invoked);
    assert!(!matrix.performance_claim_allowed);
    assert!(!matrix.production_output_claim_allowed);
    assert!(!matrix.lakehouse_table_commit_claim_allowed);
}
