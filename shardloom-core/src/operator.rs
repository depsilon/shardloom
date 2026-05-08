//! Physical operator and kernel planning contracts.
//!
//! This module defines CG-7 report-only contracts for native physical
//! operators. It does not implement kernels, evaluate expressions, run plans, or
//! invoke external fallback engines.

use crate::{
    Diagnostic, KernelKind, OperatorCertificationStatus, OperatorFamily,
    OperatorMemoryCertification, Result, ShardLoomError,
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
    pub plan_id: String,
    pub operators: Vec<PhysicalOperatorContract>,
    pub diagnostics: Vec<Diagnostic>,
}

impl PhysicalOperatorPlan {
    #[must_use]
    pub fn cg7_foundation() -> Self {
        let mut plan = Self {
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
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "physical operator plan\nplan: {}\noperators: {}\nall ready: {}\nfallback execution: disabled",
            self.plan_id,
            self.operators.len(),
            self.all_ready_for_native_planning(),
        )
    }
}
