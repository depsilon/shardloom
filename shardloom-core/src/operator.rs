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

    pub fn refresh_readiness(&mut self) {
        self.diagnostics.clear();
        self.readiness_status = if !self.execution_level.can_satisfy_native_execution()
            || self.kind == PhysicalOperatorKind::Unsupported
        {
            PhysicalOperatorReadinessStatus::Unsupported
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
                vec![KernelKind::Metadata, KernelKind::Encoded],
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
    pub fn allows_level(&self, level: PhysicalOperatorExecutionLevel) -> bool {
        self.allowed_levels.contains(&level)
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
    pub fn profile_count(&self) -> usize {
        self.profiles.len()
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
