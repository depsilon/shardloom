use shardloom_core::{
    KernelKind, OperatorCertificationStatus, OperatorMemoryCertification,
    PhysicalKernelRequirement, PhysicalKernelRequirementStatus, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorKind, PhysicalOperatorPlan,
    PhysicalOperatorReadinessStatus,
};

#[test]
fn cg7_foundation_plan_declares_initial_operator_kernel_blockers() {
    let plan = PhysicalOperatorPlan::cg7_foundation();

    assert!(plan.has_operator_kind(PhysicalOperatorKind::Filter));
    assert!(plan.has_operator_kind(PhysicalOperatorKind::Project));
    assert!(plan.has_operator_kind(PhysicalOperatorKind::CountAggregate));
    assert!(!plan.all_ready_for_native_planning());
    assert!(!plan.fallback_execution_allowed());
    assert!(!plan.diagnostics.is_empty());

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
