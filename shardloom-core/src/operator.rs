//! Physical operator and kernel planning contracts.
//!
//! This module defines CG-7 report-only contracts for native physical
//! operators. It does not implement kernels, evaluate expressions, run plans, or
//! invoke external fallback engines.

use crate::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, KernelKind,
    OperatorCertificationStatus, OperatorFamily, OperatorMemoryCertification, Result,
    ShardLoomError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorKind {
    Scan,
    Filter,
    Project,
    Limit,
    CountAggregate,
    Aggregate,
    Join,
    TopK,
    Sort,
    Window,
    Repartition,
    Write,
    Unsupported,
}

impl PhysicalOperatorKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Filter => "filter",
            Self::Project => "project",
            Self::Limit => "limit",
            Self::CountAggregate => "count_aggregate",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::TopK => "top_k",
            Self::Sort => "sort",
            Self::Window => "window",
            Self::Repartition => "repartition",
            Self::Write => "write",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn operator_family(&self) -> Option<OperatorFamily> {
        match self {
            Self::Scan => Some(OperatorFamily::Scan),
            Self::Filter => Some(OperatorFamily::Filter),
            Self::Project => Some(OperatorFamily::Project),
            Self::Limit => Some(OperatorFamily::Limit),
            Self::CountAggregate | Self::Aggregate => Some(OperatorFamily::Aggregate),
            Self::Join => Some(OperatorFamily::Join),
            Self::TopK => Some(OperatorFamily::TopK),
            Self::Sort => Some(OperatorFamily::Sort),
            Self::Window => Some(OperatorFamily::Window),
            Self::Repartition => Some(OperatorFamily::Repartition),
            Self::Write => Some(OperatorFamily::Write),
            Self::Unsupported => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorExecutionLevel {
    MetadataOnly,
    EncodedNative,
    HybridNative,
    NativeDecoded,
    TestReferenceOnly,
    Unsupported,
}

impl PhysicalOperatorExecutionLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedNative => "encoded_native",
            Self::HybridNative => "hybrid_native",
            Self::NativeDecoded => "native_decoded",
            Self::TestReferenceOnly => "test_reference_only",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn can_satisfy_native_execution(&self) -> bool {
        matches!(
            self,
            Self::MetadataOnly | Self::EncodedNative | Self::HybridNative | Self::NativeDecoded
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalKernelRequirementStatus {
    Missing,
    Present,
    ReferenceOnlyRejected,
}

impl PhysicalKernelRequirementStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Present => "present",
            Self::ReferenceOnlyRejected => "reference_only_rejected",
        }
    }

    #[must_use]
    pub const fn is_satisfied(&self) -> bool {
        matches!(self, Self::Present)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalKernelRequirement {
    pub kind: KernelKind,
    pub status: PhysicalKernelRequirementStatus,
}

impl PhysicalKernelRequirement {
    #[must_use]
    pub const fn missing(kind: KernelKind) -> Self {
        Self {
            kind,
            status: PhysicalKernelRequirementStatus::Missing,
        }
    }

    #[must_use]
    pub const fn present(kind: KernelKind) -> Self {
        let status = if kind.is_reference_only() {
            PhysicalKernelRequirementStatus::ReferenceOnlyRejected
        } else if matches!(kind, KernelKind::Unsupported) {
            PhysicalKernelRequirementStatus::Missing
        } else {
            PhysicalKernelRequirementStatus::Present
        };
        Self { kind, status }
    }

    #[must_use]
    pub const fn is_satisfied(&self) -> bool {
        self.status.is_satisfied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorReadinessStatus {
    MissingKernel,
    ReadyForNativePlanning,
    Unsupported,
}

impl PhysicalOperatorReadinessStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MissingKernel => "missing_kernel",
            Self::ReadyForNativePlanning => "ready_for_native_planning",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn can_plan_native(&self) -> bool {
        matches!(self, Self::ReadyForNativePlanning)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalOperatorContract {
    pub operator_id: String,
    pub kind: PhysicalOperatorKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub kernel_requirements: Vec<PhysicalKernelRequirement>,
    pub memory: OperatorMemoryCertification,
    pub certification_status: OperatorCertificationStatus,
    pub readiness_status: PhysicalOperatorReadinessStatus,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalOperatorContract {
    /// Creates a physical operator contract and computes readiness.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `operator_id` is empty.
    pub fn new(
        operator_id: impl Into<String>,
        kind: PhysicalOperatorKind,
        execution_level: PhysicalOperatorExecutionLevel,
        kernel_requirements: Vec<PhysicalKernelRequirement>,
    ) -> Result<Self> {
        let operator_id = operator_id.into();
        if operator_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "physical operator id must not be empty".to_string(),
            ));
        }
        let mut contract = Self {
            operator_id,
            kind,
            execution_level,
            kernel_requirements,
            memory: OperatorMemoryCertification::unsupported(),
            certification_status: OperatorCertificationStatus::Planned,
            readiness_status: PhysicalOperatorReadinessStatus::Unsupported,
            diagnostics: Vec::new(),
        };
        contract.refresh_readiness();
        Ok(contract)
    }

    #[must_use]
    pub fn planned_foundation(kind: PhysicalOperatorKind) -> Self {
        let kernel_requirements = match kind {
            PhysicalOperatorKind::Filter
            | PhysicalOperatorKind::Project
            | PhysicalOperatorKind::CountAggregate => vec![
                PhysicalKernelRequirement::missing(KernelKind::Metadata),
                PhysicalKernelRequirement::missing(KernelKind::Encoded),
            ],
            _ => vec![PhysicalKernelRequirement::missing(KernelKind::Unsupported)],
        };
        let mut contract = Self {
            operator_id: format!("cg7.1.{}", kind.as_str()),
            kind,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            kernel_requirements,
            memory: OperatorMemoryCertification::unsupported(),
            certification_status: OperatorCertificationStatus::Planned,
            readiness_status: PhysicalOperatorReadinessStatus::Unsupported,
            diagnostics: Vec::new(),
        };
        contract.refresh_readiness();
        contract
    }

    #[must_use]
    pub fn current_runtime_supported(
        kind: PhysicalOperatorKind,
        execution_level: PhysicalOperatorExecutionLevel,
        kernel_requirements: Vec<PhysicalKernelRequirement>,
        memory: OperatorMemoryCertification,
        certification_status: OperatorCertificationStatus,
    ) -> Self {
        let mut contract = Self {
            operator_id: format!("runtime.5g-f1.{}", kind.as_str()),
            kind,
            execution_level,
            kernel_requirements,
            memory,
            certification_status,
            readiness_status: PhysicalOperatorReadinessStatus::Unsupported,
            diagnostics: Vec::new(),
        };
        contract.refresh_readiness();
        contract
    }

    #[must_use]
    pub fn current_runtime_blocked(kind: PhysicalOperatorKind) -> Self {
        let mut contract = Self {
            operator_id: format!("runtime.5g-f1.{}", kind.as_str()),
            kind,
            execution_level: PhysicalOperatorExecutionLevel::Unsupported,
            kernel_requirements: vec![PhysicalKernelRequirement::missing(KernelKind::Unsupported)],
            memory: OperatorMemoryCertification::unsupported(),
            certification_status: OperatorCertificationStatus::Unsupported,
            readiness_status: PhysicalOperatorReadinessStatus::Unsupported,
            diagnostics: Vec::new(),
        };
        contract.refresh_readiness();
        contract
    }

    pub fn refresh_readiness(&mut self) {
        self.diagnostics.clear();
        self.readiness_status = if !self.execution_level.can_satisfy_native_execution()
            || self.kind == PhysicalOperatorKind::Unsupported
        {
            PhysicalOperatorReadinessStatus::Unsupported
        } else if self.kernel_requirements.is_empty() {
            PhysicalOperatorReadinessStatus::MissingKernel
        } else if self
            .kernel_requirements
            .iter()
            .all(PhysicalKernelRequirement::is_satisfied)
        {
            PhysicalOperatorReadinessStatus::ReadyForNativePlanning
        } else {
            PhysicalOperatorReadinessStatus::MissingKernel
        };

        if !self.readiness_status.can_plan_native() {
            self.diagnostics.push(Diagnostic::not_implemented(
                format!("physical operator {}", self.kind.as_str()),
                "Native physical operator planning is blocked until required ShardLoom kernels are present and reference-only kernels are rejected.",
                "Add native metadata, encoded, or hybrid kernels in a later CG-7 step before enabling operator execution.",
            ));
        }
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn can_plan_native(&self) -> bool {
        self.readiness_status.can_plan_native()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical operator contract\nid: {}\nkind: {}\nreadiness: {}\nexecution level: {}\nkernels: {}\nfallback execution: disabled",
            self.operator_id,
            self.kind.as_str(),
            self.readiness_status.as_str(),
            self.execution_level.as_str(),
            self.kernel_requirements.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_kernel_requirements_cannot_ready_native_operator() {
        let contract = PhysicalOperatorContract::new(
            "test.empty-kernels",
            PhysicalOperatorKind::CountAggregate,
            PhysicalOperatorExecutionLevel::EncodedNative,
            Vec::new(),
        )
        .expect("operator contract");

        assert_eq!(
            contract.readiness_status,
            PhysicalOperatorReadinessStatus::MissingKernel
        );
        assert!(!contract.can_plan_native());
        assert!(!contract.diagnostics.is_empty());
        assert!(!contract.fallback_execution_allowed());
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalOperatorPlan {
    pub schema_version: &'static str,
    pub plan_id: String,
    pub operators: Vec<PhysicalOperatorContract>,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalOperatorPlan {
    #[must_use]
    pub fn cg7_foundation() -> Self {
        let mut plan = Self {
            schema_version: "shardloom.physical_operator_plan.v1",
            plan_id: "cg7.1-physical-operator-foundation".to_string(),
            operators: vec![
                PhysicalOperatorContract::planned_foundation(PhysicalOperatorKind::Filter),
                PhysicalOperatorContract::planned_foundation(PhysicalOperatorKind::Project),
                PhysicalOperatorContract::planned_foundation(PhysicalOperatorKind::CountAggregate),
            ],
            diagnostics: Vec::new(),
        };
        plan.refresh_diagnostics();
        plan
    }

    #[must_use]
    pub fn current_runtime() -> Self {
        let mut plan = Self {
            schema_version: "shardloom.physical_operator_plan.v1",
            plan_id: "runtime.5g-f1-physical-operator-kernel-coverage".to_string(),
            operators: current_runtime_operator_contracts(),
            diagnostics: Vec::new(),
        };
        plan.refresh_diagnostics();
        plan
    }

    pub fn refresh_diagnostics(&mut self) {
        self.diagnostics = self
            .operators
            .iter()
            .flat_map(|operator| operator.diagnostics.clone())
            .collect();
    }

    #[must_use]
    pub fn has_operator_kind(&self, kind: PhysicalOperatorKind) -> bool {
        self.operators.iter().any(|operator| operator.kind == kind)
    }

    #[must_use]
    pub fn all_ready_for_native_planning(&self) -> bool {
        !self.operators.is_empty()
            && self
                .operators
                .iter()
                .all(PhysicalOperatorContract::can_plan_native)
    }

    #[must_use]
    pub fn readiness_count(&self, status: PhysicalOperatorReadinessStatus) -> usize {
        self.operators
            .iter()
            .filter(|operator| operator.readiness_status == status)
            .count()
    }

    #[must_use]
    pub fn ready_for_native_planning_count(&self) -> usize {
        self.readiness_count(PhysicalOperatorReadinessStatus::ReadyForNativePlanning)
    }

    #[must_use]
    pub fn missing_kernel_count(&self) -> usize {
        self.readiness_count(PhysicalOperatorReadinessStatus::MissingKernel)
    }

    #[must_use]
    pub fn unsupported_count(&self) -> usize {
        self.readiness_count(PhysicalOperatorReadinessStatus::Unsupported)
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical operator plan\nschema_version: {}\nplan: {}\noperators: {}\nready: {}\nmissing kernels: {}\nunsupported: {}\nall ready: {}\nfallback execution: disabled",
            self.schema_version,
            self.plan_id,
            self.operators.len(),
            self.ready_for_native_planning_count(),
            self.missing_kernel_count(),
            self.unsupported_count(),
            self.all_ready_for_native_planning(),
        )
    }
}

fn current_runtime_operator_contracts() -> Vec<PhysicalOperatorContract> {
    let mut operators = current_runtime_encoded_operator_contracts();
    operators.extend(current_runtime_residual_operator_contracts());
    operators.extend([
        PhysicalOperatorContract::current_runtime_blocked(PhysicalOperatorKind::Repartition),
        PhysicalOperatorContract::current_runtime_blocked(PhysicalOperatorKind::Write),
    ]);
    operators
}

fn current_runtime_encoded_operator_contracts() -> Vec<PhysicalOperatorContract> {
    vec![
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Scan,
            PhysicalOperatorExecutionLevel::MetadataOnly,
            metadata_kernel_requirements(),
            streaming_memory(),
            OperatorCertificationStatus::EncodedCapable,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Filter,
            PhysicalOperatorExecutionLevel::EncodedNative,
            encoded_kernel_requirements(),
            streaming_memory(),
            OperatorCertificationStatus::EncodedCapable,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Project,
            PhysicalOperatorExecutionLevel::EncodedNative,
            encoded_kernel_requirements(),
            streaming_memory(),
            OperatorCertificationStatus::EncodedCapable,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::CountAggregate,
            PhysicalOperatorExecutionLevel::EncodedNative,
            encoded_kernel_requirements(),
            streaming_memory(),
            OperatorCertificationStatus::EncodedCapable,
        ),
    ]
}

fn current_runtime_residual_operator_contracts() -> Vec<PhysicalOperatorContract> {
    vec![
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Limit,
            PhysicalOperatorExecutionLevel::NativeDecoded,
            partial_decode_kernel_requirements(),
            bounded_residual_memory(),
            OperatorCertificationStatus::NativeDecoded,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Aggregate,
            PhysicalOperatorExecutionLevel::HybridNative,
            hybrid_kernel_requirements(),
            residual_state_memory(),
            OperatorCertificationStatus::NativeDecoded,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Join,
            PhysicalOperatorExecutionLevel::HybridNative,
            hybrid_kernel_requirements(),
            residual_state_memory(),
            OperatorCertificationStatus::NativeDecoded,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::TopK,
            PhysicalOperatorExecutionLevel::NativeDecoded,
            partial_decode_kernel_requirements(),
            residual_state_memory(),
            OperatorCertificationStatus::NativeDecoded,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Sort,
            PhysicalOperatorExecutionLevel::NativeDecoded,
            partial_decode_kernel_requirements(),
            residual_state_memory(),
            OperatorCertificationStatus::NativeDecoded,
        ),
        PhysicalOperatorContract::current_runtime_supported(
            PhysicalOperatorKind::Window,
            PhysicalOperatorExecutionLevel::HybridNative,
            hybrid_kernel_requirements(),
            residual_state_memory(),
            OperatorCertificationStatus::NativeDecoded,
        ),
    ]
}

fn metadata_kernel_requirements() -> Vec<PhysicalKernelRequirement> {
    vec![PhysicalKernelRequirement::present(KernelKind::Metadata)]
}

fn encoded_kernel_requirements() -> Vec<PhysicalKernelRequirement> {
    vec![
        PhysicalKernelRequirement::present(KernelKind::Metadata),
        PhysicalKernelRequirement::present(KernelKind::Encoded),
    ]
}

fn partial_decode_kernel_requirements() -> Vec<PhysicalKernelRequirement> {
    vec![
        PhysicalKernelRequirement::present(KernelKind::Metadata),
        PhysicalKernelRequirement::present(KernelKind::PartialDecode),
    ]
}

fn hybrid_kernel_requirements() -> Vec<PhysicalKernelRequirement> {
    vec![
        PhysicalKernelRequirement::present(KernelKind::Metadata),
        PhysicalKernelRequirement::present(KernelKind::Encoded),
        PhysicalKernelRequirement::present(KernelKind::PartialDecode),
    ]
}

#[must_use]
const fn streaming_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[must_use]
const fn bounded_residual_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[must_use]
const fn residual_state_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: false,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: false,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalKernelSlot {
    pub slot_id: String,
    pub operator_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub required_kernel_kind: KernelKind,
    pub status: PhysicalKernelRequirementStatus,
}

impl PhysicalKernelSlot {
    #[must_use]
    pub fn from_requirement(
        operator: &PhysicalOperatorContract,
        requirement: PhysicalKernelRequirement,
    ) -> Self {
        Self {
            slot_id: format!(
                "{}.kernel.{}",
                operator.operator_id,
                requirement.kind.as_str()
            ),
            operator_id: operator.operator_id.clone(),
            operator_kind: operator.kind,
            required_kernel_kind: requirement.kind,
            status: requirement.status,
        }
    }

    #[must_use]
    pub const fn is_satisfied(&self) -> bool {
        self.status.is_satisfied()
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "{} [{} -> {} / {}]",
            self.slot_id,
            self.operator_kind.as_str(),
            self.required_kernel_kind.as_str(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalKernelRegistryPlan {
    pub schema_version: &'static str,
    pub registry_id: String,
    pub required_slots: Vec<PhysicalKernelSlot>,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalKernelRegistryPlan {
    #[must_use]
    pub fn cg7_foundation() -> Self {
        Self::from_operator_plan(&PhysicalOperatorPlan::cg7_foundation())
    }

    #[must_use]
    pub fn from_operator_plan(plan: &PhysicalOperatorPlan) -> Self {
        let required_slots = plan
            .operators
            .iter()
            .flat_map(|operator| {
                operator
                    .kernel_requirements
                    .iter()
                    .copied()
                    .map(|requirement| PhysicalKernelSlot::from_requirement(operator, requirement))
            })
            .collect::<Vec<_>>();
        let mut registry = Self {
            schema_version: "shardloom.physical_kernel_registry_plan.v1",
            registry_id: format!("{}.kernel-registry", plan.plan_id),
            required_slots,
            diagnostics: Vec::new(),
        };
        registry.refresh_diagnostics();
        registry
    }

    pub fn refresh_diagnostics(&mut self) {
        self.diagnostics.clear();
        if !self.all_required_slots_satisfied() {
            self.diagnostics.push(Diagnostic::not_implemented(
                "physical kernel registry",
                "Physical kernel registry planning is blocked until all required native kernel slots are present.",
                "Add native metadata, encoded, or hybrid kernels in later CG-7 steps before enabling operator execution.",
            ));
        }
    }

    #[must_use]
    pub fn required_slot_count(&self) -> usize {
        self.required_slots.len()
    }

    #[must_use]
    pub fn present_slot_count(&self) -> usize {
        self.required_slots
            .iter()
            .filter(|slot| slot.is_satisfied())
            .count()
    }

    #[must_use]
    pub fn missing_slot_count(&self) -> usize {
        self.required_slots
            .iter()
            .filter(|slot| slot.status == PhysicalKernelRequirementStatus::Missing)
            .count()
    }

    #[must_use]
    pub fn reference_only_rejected_count(&self) -> usize {
        self.required_slots
            .iter()
            .filter(|slot| slot.status == PhysicalKernelRequirementStatus::ReferenceOnlyRejected)
            .count()
    }

    #[must_use]
    pub fn all_required_slots_satisfied(&self) -> bool {
        !self.required_slots.is_empty()
            && self
                .required_slots
                .iter()
                .all(PhysicalKernelSlot::is_satisfied)
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn runtime_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical kernel registry plan\nschema_version: {}\nregistry: {}\nrequired slots: {}\npresent slots: {}\nmissing slots: {}\nreference-only rejected: {}\nall slots satisfied: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.registry_id,
            self.required_slot_count(),
            self.present_slot_count(),
            self.missing_slot_count(),
            self.reference_only_rejected_count(),
            self.all_required_slots_satisfied(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalKernelAdmissionStatus {
    BlockedKernelKindMismatch,
    BlockedUnsupportedKernel,
    BlockedReferenceOnlyKernel,
    BlockedFallbackAttempted,
    BlockedMissingCorrectness,
    BlockedMissingMemorySafety,
    RegistryReady,
    ProductionReady,
}

impl PhysicalKernelAdmissionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BlockedKernelKindMismatch => "blocked_kernel_kind_mismatch",
            Self::BlockedUnsupportedKernel => "blocked_unsupported_kernel",
            Self::BlockedReferenceOnlyKernel => "blocked_reference_only_kernel",
            Self::BlockedFallbackAttempted => "blocked_fallback_attempted",
            Self::BlockedMissingCorrectness => "blocked_missing_correctness",
            Self::BlockedMissingMemorySafety => "blocked_missing_memory_safety",
            Self::RegistryReady => "registry_ready",
            Self::ProductionReady => "production_ready",
        }
    }

    #[must_use]
    pub const fn can_enter_registry(&self) -> bool {
        matches!(self, Self::RegistryReady | Self::ProductionReady)
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        matches!(self, Self::ProductionReady)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalKernelAdmissionReport {
    pub schema_version: &'static str,
    pub slot_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub required_kernel_kind: KernelKind,
    pub candidate_kernel_kind: KernelKind,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub memory: OperatorMemoryCertification,
    pub fallback: BenchmarkFallbackState,
    pub status: PhysicalKernelAdmissionStatus,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalKernelAdmissionReport {
    #[must_use]
    pub fn evaluate(
        slot: &PhysicalKernelSlot,
        candidate_kernel_kind: KernelKind,
        correctness_evidence: BenchmarkEvidenceState,
        benchmark_evidence: BenchmarkEvidenceState,
        memory: OperatorMemoryCertification,
        fallback: BenchmarkFallbackState,
    ) -> Self {
        let status = Self::admission_status(
            slot,
            candidate_kernel_kind,
            correctness_evidence,
            benchmark_evidence,
            memory,
            fallback,
        );
        let mut report = Self {
            schema_version: "shardloom.physical_kernel_admission.v1",
            slot_id: slot.slot_id.clone(),
            operator_kind: slot.operator_kind,
            required_kernel_kind: slot.required_kernel_kind,
            candidate_kernel_kind,
            correctness_evidence,
            benchmark_evidence,
            memory,
            fallback,
            status,
            diagnostics: Vec::new(),
        };
        report.refresh_diagnostics();
        report
    }

    fn admission_status(
        slot: &PhysicalKernelSlot,
        candidate_kernel_kind: KernelKind,
        correctness_evidence: BenchmarkEvidenceState,
        benchmark_evidence: BenchmarkEvidenceState,
        memory: OperatorMemoryCertification,
        fallback: BenchmarkFallbackState,
    ) -> PhysicalKernelAdmissionStatus {
        if matches!(candidate_kernel_kind, KernelKind::Unsupported) {
            return PhysicalKernelAdmissionStatus::BlockedUnsupportedKernel;
        }
        if candidate_kernel_kind.is_reference_only() {
            return PhysicalKernelAdmissionStatus::BlockedReferenceOnlyKernel;
        }
        if candidate_kernel_kind != slot.required_kernel_kind {
            return PhysicalKernelAdmissionStatus::BlockedKernelKindMismatch;
        }
        if fallback.attempted() {
            return PhysicalKernelAdmissionStatus::BlockedFallbackAttempted;
        }
        if !correctness_evidence.is_present() {
            return PhysicalKernelAdmissionStatus::BlockedMissingCorrectness;
        }
        if !Self::memory_safety_evidence_present(memory) {
            return PhysicalKernelAdmissionStatus::BlockedMissingMemorySafety;
        }
        if benchmark_evidence.is_present() {
            PhysicalKernelAdmissionStatus::ProductionReady
        } else {
            PhysicalKernelAdmissionStatus::RegistryReady
        }
    }

    #[must_use]
    pub const fn memory_safety_evidence_present(memory: OperatorMemoryCertification) -> bool {
        memory.oom_safe
            && !memory.requires_full_materialization
            && (memory.streaming || memory.bounded_memory || memory.spillable)
    }

    pub fn refresh_diagnostics(&mut self) {
        self.diagnostics.clear();
        if !self.status.can_enter_registry() {
            self.diagnostics.push(Diagnostic::not_implemented(
                format!("physical kernel admission {}", self.slot_id),
                format!(
                    "Native kernel admission is blocked with status {}.",
                    self.status.as_str()
                ),
                "Provide a matching native kernel kind, correctness evidence, memory-safety evidence, and no-fallback proof before marking the slot present.",
            ));
        }
    }

    #[must_use]
    pub const fn can_mark_kernel_present(&self) -> bool {
        self.status.can_enter_registry()
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        self.status.can_satisfy_production_claim()
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn fallback_attempted(&self) -> bool {
        self.fallback.attempted()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical kernel admission\nschema_version: {}\nslot: {}\nrequired kernel: {}\ncandidate kernel: {}\nstatus: {}\ncorrectness: {}\nbenchmark: {}\nfallback attempted: {}\nfallback execution: disabled",
            self.schema_version,
            self.slot_id,
            self.required_kernel_kind.as_str(),
            self.candidate_kernel_kind.as_str(),
            self.status.as_str(),
            self.correctness_evidence.as_str(),
            self.benchmark_evidence.as_str(),
            self.fallback_attempted(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalOperatorExecutionProfile {
    pub profile_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub preferred_level: PhysicalOperatorExecutionLevel,
    pub allowed_levels: Vec<PhysicalOperatorExecutionLevel>,
    pub required_kernel_kinds: Vec<KernelKind>,
    pub row_materialization_allowed: bool,
    pub arrow_conversion_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl PhysicalOperatorExecutionProfile {
    #[must_use]
    pub fn cg7_foundation(operator_kind: PhysicalOperatorKind) -> Self {
        let (preferred_level, allowed_levels, required_kernel_kinds) = match operator_kind {
            PhysicalOperatorKind::Filter
            | PhysicalOperatorKind::Project
            | PhysicalOperatorKind::CountAggregate => (
                PhysicalOperatorExecutionLevel::EncodedNative,
                vec![
                    PhysicalOperatorExecutionLevel::MetadataOnly,
                    PhysicalOperatorExecutionLevel::EncodedNative,
                    PhysicalOperatorExecutionLevel::HybridNative,
                    PhysicalOperatorExecutionLevel::NativeDecoded,
                ],
                vec![
                    KernelKind::Metadata,
                    KernelKind::Encoded,
                    KernelKind::PartialDecode,
                ],
            ),
            _ => (
                PhysicalOperatorExecutionLevel::Unsupported,
                vec![PhysicalOperatorExecutionLevel::Unsupported],
                vec![KernelKind::Unsupported],
            ),
        };
        Self {
            profile_id: format!("cg7.execution.{}", operator_kind.as_str()),
            operator_kind,
            preferred_level,
            allowed_levels,
            required_kernel_kinds,
            row_materialization_allowed: false,
            arrow_conversion_allowed: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub fn current_runtime(operator_kind: PhysicalOperatorKind) -> Self {
        let (preferred_level, allowed_levels, required_kernel_kinds) = match operator_kind {
            PhysicalOperatorKind::Scan => (
                PhysicalOperatorExecutionLevel::MetadataOnly,
                vec![PhysicalOperatorExecutionLevel::MetadataOnly],
                vec![KernelKind::Metadata],
            ),
            PhysicalOperatorKind::Filter | PhysicalOperatorKind::Project => (
                PhysicalOperatorExecutionLevel::EncodedNative,
                vec![
                    PhysicalOperatorExecutionLevel::MetadataOnly,
                    PhysicalOperatorExecutionLevel::EncodedNative,
                    PhysicalOperatorExecutionLevel::HybridNative,
                    PhysicalOperatorExecutionLevel::NativeDecoded,
                ],
                vec![
                    KernelKind::Metadata,
                    KernelKind::Encoded,
                    KernelKind::PartialDecode,
                ],
            ),
            PhysicalOperatorKind::CountAggregate => (
                PhysicalOperatorExecutionLevel::EncodedNative,
                vec![
                    PhysicalOperatorExecutionLevel::MetadataOnly,
                    PhysicalOperatorExecutionLevel::EncodedNative,
                ],
                vec![KernelKind::Metadata, KernelKind::Encoded],
            ),
            PhysicalOperatorKind::Aggregate
            | PhysicalOperatorKind::Join
            | PhysicalOperatorKind::Window => (
                PhysicalOperatorExecutionLevel::HybridNative,
                vec![
                    PhysicalOperatorExecutionLevel::HybridNative,
                    PhysicalOperatorExecutionLevel::NativeDecoded,
                ],
                vec![
                    KernelKind::Metadata,
                    KernelKind::Encoded,
                    KernelKind::PartialDecode,
                ],
            ),
            PhysicalOperatorKind::Limit
            | PhysicalOperatorKind::TopK
            | PhysicalOperatorKind::Sort => (
                PhysicalOperatorExecutionLevel::NativeDecoded,
                vec![PhysicalOperatorExecutionLevel::NativeDecoded],
                vec![KernelKind::Metadata, KernelKind::PartialDecode],
            ),
            PhysicalOperatorKind::Repartition
            | PhysicalOperatorKind::Write
            | PhysicalOperatorKind::Unsupported => (
                PhysicalOperatorExecutionLevel::Unsupported,
                vec![PhysicalOperatorExecutionLevel::Unsupported],
                vec![KernelKind::Unsupported],
            ),
        };
        Self {
            profile_id: format!("runtime.5g-f1.execution.{}", operator_kind.as_str()),
            operator_kind,
            preferred_level,
            allowed_levels,
            required_kernel_kinds,
            row_materialization_allowed: false,
            arrow_conversion_allowed: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub fn allows_level(&self, level: PhysicalOperatorExecutionLevel) -> bool {
        self.allowed_levels.contains(&level)
    }

    #[must_use]
    pub fn required_kernel_kinds_for_level(
        &self,
        level: PhysicalOperatorExecutionLevel,
    ) -> Vec<KernelKind> {
        if !self.allows_level(level) {
            return self.required_kernel_kinds.clone();
        }
        match level {
            PhysicalOperatorExecutionLevel::MetadataOnly => vec![KernelKind::Metadata],
            PhysicalOperatorExecutionLevel::EncodedNative => {
                vec![KernelKind::Metadata, KernelKind::Encoded]
            }
            PhysicalOperatorExecutionLevel::HybridNative => self.required_kernel_kinds.clone(),
            PhysicalOperatorExecutionLevel::NativeDecoded => {
                vec![KernelKind::Metadata, KernelKind::PartialDecode]
            }
            PhysicalOperatorExecutionLevel::TestReferenceOnly => vec![KernelKind::DecodedReference],
            PhysicalOperatorExecutionLevel::Unsupported => vec![KernelKind::Unsupported],
        }
    }

    #[must_use]
    pub fn allows_reference_only(&self) -> bool {
        self.allowed_levels
            .contains(&PhysicalOperatorExecutionLevel::TestReferenceOnly)
    }

    #[must_use]
    pub fn allows_unsupported(&self) -> bool {
        self.allowed_levels
            .contains(&PhysicalOperatorExecutionLevel::Unsupported)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "{} [{} preferred={} levels={} fallback={}]",
            self.profile_id,
            self.operator_kind.as_str(),
            self.preferred_level.as_str(),
            self.allowed_levels.len(),
            self.fallback_execution_allowed
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalOperatorExecutionProfileMatrix {
    pub schema_version: &'static str,
    pub matrix_id: String,
    pub profiles: Vec<PhysicalOperatorExecutionProfile>,
}

impl PhysicalOperatorExecutionProfileMatrix {
    #[must_use]
    pub fn cg7_foundation() -> Self {
        Self {
            schema_version: "shardloom.physical_operator_execution_profiles.v1",
            matrix_id: "cg7-foundation-execution-profile-matrix".to_string(),
            profiles: vec![
                PhysicalOperatorExecutionProfile::cg7_foundation(PhysicalOperatorKind::Filter),
                PhysicalOperatorExecutionProfile::cg7_foundation(PhysicalOperatorKind::Project),
                PhysicalOperatorExecutionProfile::cg7_foundation(
                    PhysicalOperatorKind::CountAggregate,
                ),
            ],
        }
    }

    #[must_use]
    pub fn current_runtime() -> Self {
        Self {
            schema_version: "shardloom.physical_operator_execution_profiles.v1",
            matrix_id: "runtime-5g-f1-execution-profile-matrix".to_string(),
            profiles: vec![
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Scan),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Filter),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Project),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Limit),
                PhysicalOperatorExecutionProfile::current_runtime(
                    PhysicalOperatorKind::CountAggregate,
                ),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Aggregate),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Join),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::TopK),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Sort),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Window),
                PhysicalOperatorExecutionProfile::current_runtime(
                    PhysicalOperatorKind::Repartition,
                ),
                PhysicalOperatorExecutionProfile::current_runtime(PhysicalOperatorKind::Write),
            ],
        }
    }

    #[must_use]
    pub fn profile_count(&self) -> usize {
        self.profiles.len()
    }

    #[must_use]
    pub fn allowed_level_count(&self, level: PhysicalOperatorExecutionLevel) -> usize {
        self.profiles
            .iter()
            .filter(|profile| profile.allows_level(level))
            .count()
    }

    #[must_use]
    pub fn native_execution_level_count(&self) -> usize {
        [
            PhysicalOperatorExecutionLevel::MetadataOnly,
            PhysicalOperatorExecutionLevel::EncodedNative,
            PhysicalOperatorExecutionLevel::HybridNative,
            PhysicalOperatorExecutionLevel::NativeDecoded,
        ]
        .into_iter()
        .filter(|level| {
            self.profiles
                .iter()
                .any(|profile| profile.allows_level(*level))
        })
        .count()
    }

    #[must_use]
    pub fn reference_only_allowed_count(&self) -> usize {
        self.profiles
            .iter()
            .filter(|profile| profile.allows_reference_only())
            .count()
    }

    #[must_use]
    pub fn unsupported_allowed_count(&self) -> usize {
        self.profiles
            .iter()
            .filter(|profile| profile.allows_unsupported())
            .count()
    }

    #[must_use]
    pub fn row_materialization_allowed_count(&self) -> usize {
        self.profiles
            .iter()
            .filter(|profile| profile.row_materialization_allowed)
            .count()
    }

    #[must_use]
    pub fn arrow_conversion_allowed_count(&self) -> usize {
        self.profiles
            .iter()
            .filter(|profile| profile.arrow_conversion_allowed)
            .count()
    }

    #[must_use]
    pub fn fallback_allowed_count(&self) -> usize {
        self.profiles
            .iter()
            .filter(|profile| profile.fallback_execution_allowed)
            .count()
    }

    #[must_use]
    pub fn profile_for(
        &self,
        operator_kind: PhysicalOperatorKind,
    ) -> Option<&PhysicalOperatorExecutionProfile> {
        self.profiles
            .iter()
            .find(|profile| profile.operator_kind == operator_kind)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical operator execution profiles\nschema_version: {}\nmatrix: {}\nprofiles: {}\nreference-only allowed: {}\nrow materialization allowed: {}\narrow conversion allowed: {}\nfallback allowed: {}",
            self.schema_version,
            self.matrix_id,
            self.profile_count(),
            self.reference_only_allowed_count(),
            self.row_materialization_allowed_count(),
            self.arrow_conversion_allowed_count(),
            self.fallback_allowed_count(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalKernelSelectionStatus {
    OperatorProfileMissing,
    ExecutionLevelRejected,
    RequiredKernelMissing,
    ReadyForAdmissionReview,
}

impl PhysicalKernelSelectionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OperatorProfileMissing => "operator_profile_missing",
            Self::ExecutionLevelRejected => "execution_level_rejected",
            Self::RequiredKernelMissing => "required_kernel_missing",
            Self::ReadyForAdmissionReview => "ready_for_admission_review",
        }
    }

    #[must_use]
    pub const fn can_select_kernel(&self) -> bool {
        matches!(self, Self::ReadyForAdmissionReview)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalKernelSelectionReport {
    pub schema_version: &'static str,
    pub operator_kind: PhysicalOperatorKind,
    pub requested_level: PhysicalOperatorExecutionLevel,
    pub required_kernel_kinds: Vec<KernelKind>,
    pub missing_slot_ids: Vec<String>,
    pub status: PhysicalKernelSelectionStatus,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalKernelSelectionReport {
    #[must_use]
    pub fn evaluate(
        operator_kind: PhysicalOperatorKind,
        requested_level: PhysicalOperatorExecutionLevel,
        profiles: &PhysicalOperatorExecutionProfileMatrix,
        registry: &PhysicalKernelRegistryPlan,
    ) -> Self {
        let Some(profile) = profiles.profile_for(operator_kind) else {
            return Self::blocked(
                operator_kind,
                requested_level,
                Vec::new(),
                Vec::new(),
                PhysicalKernelSelectionStatus::OperatorProfileMissing,
            );
        };
        if !profile.allows_level(requested_level) {
            return Self::blocked(
                operator_kind,
                requested_level,
                profile.required_kernel_kinds.clone(),
                Vec::new(),
                PhysicalKernelSelectionStatus::ExecutionLevelRejected,
            );
        }
        let required_kernel_kinds = profile.required_kernel_kinds_for_level(requested_level);
        let missing_slot_ids = required_kernel_kinds
            .iter()
            .flat_map(|kernel_kind| {
                let matching_slots = registry
                    .required_slots
                    .iter()
                    .filter(|slot| {
                        slot.operator_kind == operator_kind
                            && slot.required_kernel_kind == *kernel_kind
                    })
                    .collect::<Vec<_>>();
                if matching_slots.is_empty() {
                    vec![format!(
                        "{}.kernel.{}.missing",
                        operator_kind.as_str(),
                        kernel_kind.as_str()
                    )]
                } else {
                    matching_slots
                        .into_iter()
                        .filter(|slot| !slot.is_satisfied())
                        .map(|slot| slot.slot_id.clone())
                        .collect::<Vec<_>>()
                }
            })
            .collect::<Vec<_>>();
        let status = if missing_slot_ids.is_empty() {
            PhysicalKernelSelectionStatus::ReadyForAdmissionReview
        } else {
            PhysicalKernelSelectionStatus::RequiredKernelMissing
        };
        let mut report = Self {
            schema_version: "shardloom.physical_kernel_selection.v1",
            operator_kind,
            requested_level,
            required_kernel_kinds,
            missing_slot_ids,
            status,
            diagnostics: Vec::new(),
        };
        report.refresh_diagnostics();
        report
    }

    fn blocked(
        operator_kind: PhysicalOperatorKind,
        requested_level: PhysicalOperatorExecutionLevel,
        required_kernel_kinds: Vec<KernelKind>,
        missing_slot_ids: Vec<String>,
        status: PhysicalKernelSelectionStatus,
    ) -> Self {
        let mut report = Self {
            schema_version: "shardloom.physical_kernel_selection.v1",
            operator_kind,
            requested_level,
            required_kernel_kinds,
            missing_slot_ids,
            status,
            diagnostics: Vec::new(),
        };
        report.refresh_diagnostics();
        report
    }

    pub fn refresh_diagnostics(&mut self) {
        self.diagnostics.clear();
        if !self.status.can_select_kernel() {
            self.diagnostics.push(Diagnostic::not_implemented(
                format!("physical kernel selection {}", self.operator_kind.as_str()),
                format!(
                    "Physical kernel selection is blocked with status {}.",
                    self.status.as_str()
                ),
                "Provide an allowed execution level and present native kernel slots before selecting a kernel for execution.",
            ));
        }
    }

    #[must_use]
    pub const fn can_select_kernel(&self) -> bool {
        self.status.can_select_kernel()
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn runtime_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical kernel selection\nschema_version: {}\noperator: {}\nrequested level: {}\nstatus: {}\nmissing slots: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.operator_kind.as_str(),
            self.requested_level.as_str(),
            self.status.as_str(),
            self.missing_slot_ids.len(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorPlanningCertificateStatus {
    OperatorPlanBlocked,
    KernelRegistryBlocked,
    KernelSelectionBlocked,
    KernelAdmissionBlocked,
    ReadyForNativePlanning,
    ProductionCertified,
}

impl PhysicalOperatorPlanningCertificateStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OperatorPlanBlocked => "operator_plan_blocked",
            Self::KernelRegistryBlocked => "kernel_registry_blocked",
            Self::KernelSelectionBlocked => "kernel_selection_blocked",
            Self::KernelAdmissionBlocked => "kernel_admission_blocked",
            Self::ReadyForNativePlanning => "ready_for_native_planning",
            Self::ProductionCertified => "production_certified",
        }
    }

    #[must_use]
    pub const fn can_plan_native(&self) -> bool {
        matches!(
            self,
            Self::ReadyForNativePlanning | Self::ProductionCertified
        )
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        matches!(self, Self::ProductionCertified)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalOperatorPlanningCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub plan_id: String,
    pub operator_count: usize,
    pub ready_operator_count: usize,
    pub missing_kernel_operator_count: usize,
    pub unsupported_operator_count: usize,
    pub required_slot_count: usize,
    pub missing_slot_count: usize,
    pub selection_blocked_count: usize,
    pub admission_blocked_count: usize,
    pub registry_ready_slot_count: usize,
    pub production_ready_slot_count: usize,
    pub fallback_attempted: bool,
    pub selection_reports: Vec<PhysicalKernelSelectionReport>,
    pub admission_reports: Vec<PhysicalKernelAdmissionReport>,
    pub status: PhysicalOperatorPlanningCertificateStatus,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalOperatorPlanningCertificate {
    #[must_use]
    pub fn evaluate(
        plan: &PhysicalOperatorPlan,
        profiles: &PhysicalOperatorExecutionProfileMatrix,
        correctness_evidence: BenchmarkEvidenceState,
        benchmark_evidence: BenchmarkEvidenceState,
        memory: OperatorMemoryCertification,
        fallback: BenchmarkFallbackState,
    ) -> Self {
        let registry = PhysicalKernelRegistryPlan::from_operator_plan(plan);
        let selection_reports = plan
            .operators
            .iter()
            .map(|operator| {
                PhysicalKernelSelectionReport::evaluate(
                    operator.kind,
                    operator.execution_level,
                    profiles,
                    &registry,
                )
            })
            .collect::<Vec<_>>();
        let admission_reports = registry
            .required_slots
            .iter()
            .map(|slot| {
                PhysicalKernelAdmissionReport::evaluate(
                    slot,
                    slot.required_kernel_kind,
                    correctness_evidence,
                    benchmark_evidence,
                    memory,
                    fallback,
                )
            })
            .collect::<Vec<_>>();
        let mut certificate = Self {
            schema_version: "shardloom.physical_operator_planning_certificate.v1",
            certificate_id: format!("{}.physical-operator-planning-certificate", plan.plan_id),
            plan_id: plan.plan_id.clone(),
            operator_count: plan.operators.len(),
            ready_operator_count: plan.ready_for_native_planning_count(),
            missing_kernel_operator_count: plan.missing_kernel_count(),
            unsupported_operator_count: plan.unsupported_count(),
            required_slot_count: registry.required_slot_count(),
            missing_slot_count: registry.missing_slot_count(),
            selection_blocked_count: selection_reports
                .iter()
                .filter(|report| !report.can_select_kernel())
                .count(),
            admission_blocked_count: admission_reports
                .iter()
                .filter(|report| !report.can_mark_kernel_present())
                .count(),
            registry_ready_slot_count: admission_reports
                .iter()
                .filter(|report| report.can_mark_kernel_present())
                .count(),
            production_ready_slot_count: admission_reports
                .iter()
                .filter(|report| report.can_satisfy_production_claim())
                .count(),
            fallback_attempted: admission_reports
                .iter()
                .any(PhysicalKernelAdmissionReport::fallback_attempted),
            selection_reports,
            admission_reports,
            status: PhysicalOperatorPlanningCertificateStatus::OperatorPlanBlocked,
            diagnostics: Vec::new(),
        };
        certificate.status = certificate.compute_status();
        certificate.refresh_diagnostics();
        certificate
    }

    fn compute_status(&self) -> PhysicalOperatorPlanningCertificateStatus {
        if self.operator_count == 0
            || self.missing_kernel_operator_count > 0
            || self.unsupported_operator_count > 0
        {
            return PhysicalOperatorPlanningCertificateStatus::OperatorPlanBlocked;
        }
        if self.missing_slot_count > 0 {
            return PhysicalOperatorPlanningCertificateStatus::KernelRegistryBlocked;
        }
        if self.selection_blocked_count > 0 {
            return PhysicalOperatorPlanningCertificateStatus::KernelSelectionBlocked;
        }
        if self.admission_blocked_count > 0 {
            return PhysicalOperatorPlanningCertificateStatus::KernelAdmissionBlocked;
        }
        if self.required_slot_count > 0
            && self.production_ready_slot_count == self.required_slot_count
        {
            PhysicalOperatorPlanningCertificateStatus::ProductionCertified
        } else {
            PhysicalOperatorPlanningCertificateStatus::ReadyForNativePlanning
        }
    }

    pub fn refresh_diagnostics(&mut self) {
        self.diagnostics.clear();
        if !self.status.can_plan_native() {
            self.diagnostics.push(Diagnostic::not_implemented(
                format!("physical operator planning certificate {}", self.plan_id),
                format!(
                    "Physical operator planning certificate is blocked with status {}.",
                    self.status.as_str()
                ),
                "Resolve operator readiness, registry slots, selection, and kernel admission evidence before enabling physical operator execution.",
            ));
        }
    }

    #[must_use]
    pub const fn can_plan_native(&self) -> bool {
        self.status.can_plan_native()
    }

    #[must_use]
    pub const fn can_satisfy_production_claim(&self) -> bool {
        self.status.can_satisfy_production_claim()
    }

    #[must_use]
    pub const fn runtime_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical operator planning certificate\nschema_version: {}\ncertificate: {}\nplan: {}\noperators: {}\nrequired slots: {}\nmissing slots: {}\nselection blocked: {}\nadmission blocked: {}\nstatus: {}\nruntime execution: disabled\nfallback attempted: {}\nfallback execution: disabled",
            self.schema_version,
            self.certificate_id,
            self.plan_id,
            self.operator_count,
            self.required_slot_count,
            self.missing_slot_count,
            self.selection_blocked_count,
            self.admission_blocked_count,
            self.status.as_str(),
            self.fallback_attempted,
        )
    }
}
