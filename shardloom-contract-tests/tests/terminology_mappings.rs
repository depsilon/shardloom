use shardloom_core::{DatasetFormat, FidelityLevel, MaterializationRequirement, OutputTargetKind};
use shardloom_exec::DataWorkLevel;

#[test]
fn canonical_terminology_mappings_are_stable() {
    assert_eq!(
        DataWorkLevel::ZeroDecode.to_execution_state(),
        shardloom_core::ExecutionState::EncodedEvaluation
    );
    assert_eq!(
        DataWorkLevel::FullMaterialization.to_execution_state(),
        shardloom_core::ExecutionState::FullMaterialization
    );

    assert_eq!(
        MaterializationRequirement::None.canonical_label(),
        "no_materialization"
    );
    assert_eq!(
        MaterializationRequirement::Partial { reason: "x".into() }.canonical_label(),
        "partial_materialization"
    );
    assert_eq!(
        MaterializationRequirement::Full { reason: "x".into() }.canonical_label(),
        "full_materialization"
    );
    assert_eq!(
        MaterializationRequirement::Unknown { reason: "x".into() }.canonical_label(),
        "unknown_materialization"
    );

    assert_eq!(
        FidelityLevel::NativeFullFidelity.canonical_label(),
        "native_full_fidelity"
    );
    assert_eq!(
        OutputTargetKind::Vortex.canonical_label(),
        "native_vortex_output"
    );
    assert_eq!(
        OutputTargetKind::Parquet.canonical_label(),
        "compatibility_output"
    );

    assert_eq!(
        OutputTargetKind::from_dataset_format(&DatasetFormat::Vortex),
        OutputTargetKind::Vortex
    );
    assert_eq!(
        OutputTargetKind::from_dataset_format(&DatasetFormat::Parquet),
        OutputTargetKind::Parquet
    );
}
