use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, KernelKind, OperatorCertificationStatus,
    OperatorMemoryCertification, PhysicalKernelAdmissionReport, PhysicalKernelAdmissionStatus,
    PhysicalKernelRegistryPlan, PhysicalKernelRequirement, PhysicalKernelRequirementStatus,
    PhysicalOperatorContract, PhysicalOperatorExecutionLevel, PhysicalOperatorKind,
    PhysicalOperatorPlan, PhysicalOperatorReadinessStatus,
};

fn safe_streaming_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[test]
fn cg7_foundation_plan_declares_initial_operator_kernel_blockers() {
    let plan = PhysicalOperatorPlan::cg7_foundation();

    assert_eq!(plan.schema_version, "shardloom.physical_operator_plan.v1");
    assert!(plan.has_operator_kind(PhysicalOperatorKind::Filter));
    assert!(plan.has_operator_kind(PhysicalOperatorKind::Project));
    assert!(plan.has_operator_kind(PhysicalOperatorKind::CountAggregate));
    assert_eq!(plan.ready_for_native_planning_count(), 0);
    assert_eq!(plan.missing_kernel_count(), 3);
    assert_eq!(plan.unsupported_count(), 0);
    assert!(!plan.all_ready_for_native_planning());
    assert!(!plan.fallback_execution_allowed());
    assert!(!plan.diagnostics.is_empty());
    assert!(
        plan.to_human_text()
            .contains("schema_version: shardloom.physical_operator_plan.v1")
    );
    assert!(plan.to_human_text().contains("missing kernels: 3"));

    for operator in &plan.operators {
        assert_eq!(
            operator.readiness_status,
            PhysicalOperatorReadinessStatus::MissingKernel
        );
        assert_eq!(
            operator.certification_status,
            OperatorCertificationStatus::Planned
        );
        assert_eq!(operator.memory, OperatorMemoryCertification::unsupported());
        assert!(!operator.can_plan_native());
        assert!(!operator.fallback_execution_allowed());
        assert!(
            operator
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}

#[test]
fn reference_only_kernel_requirement_cannot_satisfy_native_operator() {
    let requirement = PhysicalKernelRequirement::present(KernelKind::DecodedReference);

    assert_eq!(
        requirement.status,
        PhysicalKernelRequirementStatus::ReferenceOnlyRejected
    );
    assert!(!requirement.is_satisfied());

    let unsupported_requirement = PhysicalKernelRequirement::present(KernelKind::Unsupported);
    assert_eq!(
        unsupported_requirement.status,
        PhysicalKernelRequirementStatus::Missing
    );
    assert!(!unsupported_requirement.is_satisfied());
    assert_eq!(PhysicalOperatorKind::Unsupported.operator_family(), None);
}

#[test]
fn cg7_kernel_registry_plan_names_required_missing_kernel_slots() {
    let registry = PhysicalKernelRegistryPlan::cg7_foundation();

    assert_eq!(
        registry.schema_version,
        "shardloom.physical_kernel_registry_plan.v1"
    );
    assert_eq!(
        registry.registry_id,
        "cg7.1-physical-operator-foundation.kernel-registry"
    );
    assert_eq!(registry.required_slot_count(), 6);
    assert_eq!(registry.present_slot_count(), 0);
    assert_eq!(registry.missing_slot_count(), 6);
    assert_eq!(registry.reference_only_rejected_count(), 0);
    assert!(!registry.all_required_slots_satisfied());
    assert!(!registry.fallback_execution_allowed());
    assert!(!registry.runtime_execution_allowed());
    assert!(!registry.diagnostics.is_empty());
    assert!(
        registry
            .required_slots
            .iter()
            .any(|slot| slot.slot_id == "cg7.1.filter.kernel.metadata")
    );
    assert!(
        registry
            .required_slots
            .iter()
            .all(|slot| !slot.fallback_execution_allowed())
    );
    assert!(registry.to_human_text().contains("missing slots: 6"));
    assert!(
        registry
            .to_human_text()
            .contains("runtime execution: disabled")
    );
}

#[test]
fn physical_kernel_admission_blocks_reference_fallback_and_missing_evidence() {
    let registry = PhysicalKernelRegistryPlan::cg7_foundation();
    let slot = registry
        .required_slots
        .iter()
        .find(|slot| slot.slot_id == "cg7.1.filter.kernel.metadata")
        .expect("slot exists");

    let reference = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::DecodedReference,
        BenchmarkEvidenceState::Present,
        BenchmarkEvidenceState::Present,
        safe_streaming_memory(),
        BenchmarkFallbackState::NotAttempted,
    );
    assert_eq!(
        reference.status,
        PhysicalKernelAdmissionStatus::BlockedReferenceOnlyKernel
    );
    assert_eq!(
        reference.schema_version,
        "shardloom.physical_kernel_admission.v1"
    );
    assert!(!reference.can_mark_kernel_present());

    let mismatch = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::Encoded,
        BenchmarkEvidenceState::Present,
        BenchmarkEvidenceState::Present,
        safe_streaming_memory(),
        BenchmarkFallbackState::NotAttempted,
    );
    assert_eq!(
        mismatch.status,
        PhysicalKernelAdmissionStatus::BlockedKernelKindMismatch
    );

    let fallback = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::Metadata,
        BenchmarkEvidenceState::Present,
        BenchmarkEvidenceState::Present,
        safe_streaming_memory(),
        BenchmarkFallbackState::Attempted,
    );
    assert_eq!(
        fallback.status,
        PhysicalKernelAdmissionStatus::BlockedFallbackAttempted
    );
    assert!(fallback.fallback_attempted());
    assert!(!fallback.fallback_execution_allowed());

    let missing_correctness = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::Metadata,
        BenchmarkEvidenceState::Missing,
        BenchmarkEvidenceState::Present,
        safe_streaming_memory(),
        BenchmarkFallbackState::NotAttempted,
    );
    assert_eq!(
        missing_correctness.status,
        PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
    );
    assert!(!missing_correctness.diagnostics.is_empty());

    let unsafe_memory = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::Metadata,
        BenchmarkEvidenceState::Present,
        BenchmarkEvidenceState::Present,
        OperatorMemoryCertification::unsupported(),
        BenchmarkFallbackState::NotAttempted,
    );
    assert_eq!(
        unsafe_memory.status,
        PhysicalKernelAdmissionStatus::BlockedMissingMemorySafety
    );
}

#[test]
fn physical_kernel_admission_allows_registry_before_production_claims() {
    let registry = PhysicalKernelRegistryPlan::cg7_foundation();
    let slot = registry
        .required_slots
        .iter()
        .find(|slot| slot.slot_id == "cg7.1.filter.kernel.metadata")
        .expect("slot exists");

    let registry_ready = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::Metadata,
        BenchmarkEvidenceState::Present,
        BenchmarkEvidenceState::Missing,
        safe_streaming_memory(),
        BenchmarkFallbackState::NotAttempted,
    );
    assert_eq!(
        registry_ready.status,
        PhysicalKernelAdmissionStatus::RegistryReady
    );
    assert!(registry_ready.can_mark_kernel_present());
    assert!(!registry_ready.can_satisfy_production_claim());
    assert!(registry_ready.diagnostics.is_empty());
    assert!(
        registry_ready
            .to_human_text()
            .contains("fallback execution: disabled")
    );

    let production_ready = PhysicalKernelAdmissionReport::evaluate(
        slot,
        KernelKind::Metadata,
        BenchmarkEvidenceState::Present,
        BenchmarkEvidenceState::Present,
        safe_streaming_memory(),
        BenchmarkFallbackState::NotAttempted,
    );
    assert_eq!(
        production_ready.status,
        PhysicalKernelAdmissionStatus::ProductionReady
    );
    assert!(production_ready.can_mark_kernel_present());
    assert!(production_ready.can_satisfy_production_claim());
}

#[test]
fn native_kernel_requirements_can_reach_planning_ready_without_execution() {
    let operator = PhysicalOperatorContract::new(
        "cg7.synthetic.filter",
        PhysicalOperatorKind::Filter,
        PhysicalOperatorExecutionLevel::EncodedNative,
        vec![
            PhysicalKernelRequirement::present(KernelKind::Metadata),
            PhysicalKernelRequirement::present(KernelKind::Encoded),
        ],
    )
    .expect("valid operator");

    assert_eq!(
        operator.readiness_status,
        PhysicalOperatorReadinessStatus::ReadyForNativePlanning
    );
    assert!(operator.can_plan_native());
    assert!(operator.diagnostics.is_empty());
    assert!(!operator.fallback_execution_allowed());
}

#[test]
fn unsupported_operator_level_blocks_native_planning() {
    let operator = PhysicalOperatorContract::new(
        "cg7.synthetic.reference",
        PhysicalOperatorKind::Filter,
        PhysicalOperatorExecutionLevel::TestReferenceOnly,
        vec![PhysicalKernelRequirement::present(
            KernelKind::DecodedReference,
        )],
    )
    .expect("valid operator");

    assert_eq!(
        operator.readiness_status,
        PhysicalOperatorReadinessStatus::Unsupported
    );
    assert!(!operator.can_plan_native());
    assert!(!operator.diagnostics[0].fallback.attempted);
}
