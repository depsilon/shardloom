//! Execution facade for `ShardLoom`.
//!
//! This crate owns provider-neutral execution orchestration contracts with
//! explicit unsupported-path failures and no fallback delegation. The neutral
//! `execute` path returns report-only results or deterministic
//! provider-required blockers; it never reports no-op success for executable
//! plans. Provider crates attach concrete execution through the
//! `ShardLoomExecutionProvider` trait to avoid reversing crate dependencies.
//!
//! Memory, recovery, sizing, spill, runtime, and streaming exports are mostly
//! planning or promotion-gate surfaces. Narrow feature-gated local helpers stay
//! explicit and do not authorize object-store I/O, distributed execution,
//! external engine invocation, or fallback execution.

use shardloom_core::{
    CommandStatus, Diagnostic, DiagnosticCode, ExecutionProviderKind, FallbackStatus,
    OutputEnvelope, OutputTypedArtifact, OutputTypedRef, Result,
};
use shardloom_plan::{Plan, PlanKind};

pub mod memory;
pub mod recovery;
pub mod runtime;
pub mod sizing;
pub mod spill_lifecycle;
pub mod spill_payload;
pub mod streaming;

/// Reported status for the execution subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecStatus {
    /// Human-readable status line for `CLI` output.
    pub summary: String,
}

/// Top-level execution result status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomExecutionStatus {
    Executed,
    ReportOnly,
    BlockedProviderDispatchRequired,
    BlockedUnsupported,
}

impl ShardLoomExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Executed => "executed",
            Self::ReportOnly => "report_only",
            Self::BlockedProviderDispatchRequired => "blocked_provider_dispatch_required",
            Self::BlockedUnsupported => "blocked_unsupported",
        }
    }

    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Executed | Self::ReportOnly)
    }
}

/// Explicit status for one evidence slot on a top-level execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEvidenceSlotStatus {
    Present,
    NotRequired,
    EvidenceIncomplete,
}

impl ExecutionEvidenceSlotStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::NotRequired => "not_required",
            Self::EvidenceIncomplete => "evidence_incomplete",
        }
    }

    #[must_use]
    pub const fn is_incomplete(self) -> bool {
        matches!(self, Self::EvidenceIncomplete)
    }
}

/// Named evidence slots preserved by [`ShardLoomExecutionResult`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEvidenceSlotKind {
    ResultRefs,
    ArtifactRefs,
    InlineArtifacts,
    ExecutionCertificateRefs,
    NativeIoCertificateRefs,
    MaterializationBoundaryRefs,
    ResidualBoundaryRefs,
    RepresentationTransitions,
    ProviderVersion,
    SourceRefs,
    SplitRefs,
    LifecycleStatus,
    FallbackStatus,
}

impl ExecutionEvidenceSlotKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResultRefs => "result_refs",
            Self::ArtifactRefs => "artifact_refs",
            Self::InlineArtifacts => "inline_artifacts",
            Self::ExecutionCertificateRefs => "execution_certificate_refs",
            Self::NativeIoCertificateRefs => "native_io_certificate_refs",
            Self::MaterializationBoundaryRefs => "materialization_boundary_refs",
            Self::ResidualBoundaryRefs => "residual_boundary_refs",
            Self::RepresentationTransitions => "representation_transitions",
            Self::ProviderVersion => "provider_version",
            Self::SourceRefs => "source_refs",
            Self::SplitRefs => "split_refs",
            Self::LifecycleStatus => "lifecycle_status",
            Self::FallbackStatus => "fallback_status",
        }
    }
}

/// Evidence completeness record for a top-level execution result slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomExecutionEvidenceSlot {
    pub kind: ExecutionEvidenceSlotKind,
    pub status: ExecutionEvidenceSlotStatus,
    pub refs: Vec<String>,
    pub detail: String,
}

impl ShardLoomExecutionEvidenceSlot {
    #[must_use]
    pub fn new(
        kind: ExecutionEvidenceSlotKind,
        status: ExecutionEvidenceSlotStatus,
        refs: Vec<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            status,
            refs,
            detail: detail.into(),
        }
    }
}

/// Inline artifact payload preserved with a top-level execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomExecutionInlineArtifact {
    pub artifact_id: String,
    pub artifact_kind: String,
    pub status: String,
    pub fields: Vec<(String, String)>,
}

impl ShardLoomExecutionInlineArtifact {
    #[must_use]
    pub fn new(
        artifact_id: impl Into<String>,
        artifact_kind: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            artifact_kind: artifact_kind.into(),
            status: status.into(),
            fields: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    #[must_use]
    fn to_output_artifact(&self) -> OutputTypedArtifact {
        self.fields.iter().fold(
            OutputTypedArtifact::new(
                self.artifact_id.clone(),
                self.artifact_kind.clone(),
                self.status.clone(),
            ),
            |artifact, (key, value)| artifact.with_field(key.clone(), value.clone()),
        )
    }
}

/// Typed top-level execution result returned by the execution facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomExecutionResult {
    pub status: ShardLoomExecutionStatus,
    pub plan_id: String,
    pub plan_kind: String,
    pub engine_mode: String,
    pub execution_provider_kind: Option<ExecutionProviderKind>,
    pub provider_api_surface: Option<String>,
    pub provider_version: Option<String>,
    pub source_refs: Vec<String>,
    pub split_refs: Vec<String>,
    pub result_refs: Vec<String>,
    pub artifact_refs: Vec<String>,
    pub inline_artifacts: Vec<ShardLoomExecutionInlineArtifact>,
    pub execution_certificate_refs: Vec<String>,
    pub native_io_certificate_refs: Vec<String>,
    pub materialization_boundary_refs: Vec<String>,
    pub residual_boundary_refs: Vec<String>,
    pub representation_transitions: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
    pub lifecycle_status: String,
    pub fallback: FallbackStatus,
    pub external_engine_invoked: bool,
}

impl ShardLoomExecutionResult {
    #[must_use]
    pub fn from_plan(plan: &Plan, status: ShardLoomExecutionStatus) -> Self {
        Self {
            status,
            plan_id: plan.id.as_str().to_string(),
            plan_kind: plan.kind.as_str().to_string(),
            engine_mode: "batch".to_string(),
            execution_provider_kind: plan.provider_kind(),
            provider_api_surface: plan.provider_api_surface().map(str::to_string),
            provider_version: None,
            source_refs: plan.source_refs(),
            split_refs: plan.split_refs(),
            result_refs: vec![],
            artifact_refs: vec![],
            inline_artifacts: vec![],
            execution_certificate_refs: vec![],
            native_io_certificate_refs: vec![],
            materialization_boundary_refs: vec![],
            residual_boundary_refs: plan.residual_boundary_refs(),
            representation_transitions: vec![],
            diagnostics: plan.diagnostics(),
            lifecycle_status: status.as_str().to_string(),
            fallback: FallbackStatus::disabled_by_policy(),
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn report_only(plan: &Plan) -> Self {
        Self::from_plan(plan, ShardLoomExecutionStatus::ReportOnly)
    }

    #[must_use]
    pub fn blocked_provider_dispatch_required(plan: &Plan) -> Self {
        let mut result = Self::from_plan(
            plan,
            ShardLoomExecutionStatus::BlockedProviderDispatchRequired,
        );
        result.diagnostics.push(plan.unsupported_diagnostic());
        result
    }

    #[must_use]
    pub fn blocked_unsupported(plan: &Plan, diagnostic: Diagnostic) -> Self {
        let mut result = Self::from_plan(plan, ShardLoomExecutionStatus::BlockedUnsupported);
        result.diagnostics.push(diagnostic);
        result
    }

    #[must_use]
    pub fn executed(plan: &Plan) -> Self {
        Self::from_plan(plan, ShardLoomExecutionStatus::Executed)
    }

    #[must_use]
    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    pub fn set_provider_version_if_absent(&mut self, provider_version: Option<&str>) {
        if self.provider_version.is_none()
            && let Some(provider_version) =
                provider_version.filter(|value| !value.trim().is_empty())
        {
            self.provider_version = Some(provider_version.to_string());
        }
    }

    pub fn add_inline_artifact(&mut self, artifact: ShardLoomExecutionInlineArtifact) {
        self.inline_artifacts.push(artifact);
    }

    #[must_use]
    pub fn inline_artifact_ids(&self) -> Vec<String> {
        self.inline_artifacts
            .iter()
            .map(|artifact| artifact.artifact_id.clone())
            .collect()
    }

    #[must_use]
    pub fn evidence_slots(&self) -> Vec<ShardLoomExecutionEvidenceSlot> {
        vec![
            refs_slot(
                ExecutionEvidenceSlotKind::ResultRefs,
                &self.result_refs,
                matches!(self.status, ShardLoomExecutionStatus::Executed),
                "result refs are required for executed paths",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::ArtifactRefs,
                &self.artifact_refs,
                !matches!(self.status, ShardLoomExecutionStatus::ReportOnly),
                "provider reports must remain addressable as artifact refs",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::InlineArtifacts,
                &self.inline_artifact_ids(),
                matches!(self.status, ShardLoomExecutionStatus::Executed)
                    || !self.artifact_refs.is_empty(),
                "inline artifacts preserve report fields needed by typed envelopes",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::ExecutionCertificateRefs,
                &self.execution_certificate_refs,
                matches!(self.status, ShardLoomExecutionStatus::Executed),
                "execution certificates are required for executed claim-grade paths",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::NativeIoCertificateRefs,
                &self.native_io_certificate_refs,
                matches!(self.status, ShardLoomExecutionStatus::Executed),
                "Native I/O certificates are required when executed paths read or write data",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::MaterializationBoundaryRefs,
                &self.materialization_boundary_refs,
                self.materializing_transition_present(),
                "materializing representation transitions require boundary refs",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::ResidualBoundaryRefs,
                &self.residual_boundary_refs,
                false,
                "residual boundaries are present only when the plan declares them",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::RepresentationTransitions,
                &self.representation_transitions,
                matches!(self.status, ShardLoomExecutionStatus::Executed)
                    && !self.native_io_certificate_refs.is_empty(),
                "Native I/O evidence should name representation transitions",
            ),
            optional_slot(
                ExecutionEvidenceSlotKind::ProviderVersion,
                self.provider_version.as_deref(),
                self.execution_provider_kind.is_some() || self.provider_api_surface.is_some(),
                "provider dispatch surfaces must report a provider version",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::SourceRefs,
                &self.source_refs,
                self.source_refs_required(),
                "source-backed and Vortex primitive plans require source refs",
            ),
            refs_slot(
                ExecutionEvidenceSlotKind::SplitRefs,
                &self.split_refs,
                self.split_refs_required(),
                "source-backed and reader-backed plans require split refs",
            ),
            optional_slot(
                ExecutionEvidenceSlotKind::LifecycleStatus,
                Some(self.lifecycle_status.as_str()),
                true,
                "execution lifecycle status must be explicit",
            ),
            ShardLoomExecutionEvidenceSlot::new(
                ExecutionEvidenceSlotKind::FallbackStatus,
                ExecutionEvidenceSlotStatus::Present,
                vec![format!("attempted={}", self.fallback.attempted)],
                "fallback status is explicit on every execution result",
            ),
        ]
    }

    #[must_use]
    pub fn evidence_completeness_status(&self) -> &'static str {
        if matches!(self.status, ShardLoomExecutionStatus::ReportOnly) {
            "report_only"
        } else if self
            .evidence_slots()
            .iter()
            .any(|slot| slot.status.is_incomplete())
        {
            "evidence_incomplete"
        } else {
            "complete"
        }
    }

    #[must_use]
    pub fn to_output_envelope(
        &self,
        command: impl Into<String>,
        summary: impl Into<String>,
    ) -> OutputEnvelope {
        let mut envelope = OutputEnvelope::new(
            command,
            self.output_command_status(),
            summary,
            self.output_human_text(),
        );
        envelope.fallback = self.fallback.clone();
        envelope.diagnostics.extend(self.diagnostics.clone());
        self.attach_output_refs_and_artifacts(self.attach_output_fields(envelope))
    }

    fn output_command_status(&self) -> CommandStatus {
        match self.status {
            ShardLoomExecutionStatus::Executed | ShardLoomExecutionStatus::ReportOnly => {
                CommandStatus::Success
            }
            ShardLoomExecutionStatus::BlockedProviderDispatchRequired
            | ShardLoomExecutionStatus::BlockedUnsupported => CommandStatus::Unsupported,
        }
    }

    fn output_human_text(&self) -> String {
        format!(
            "top-level execution result\nplan: {}\nstatus: {}\nevidence: {}\nfallback attempted: {}",
            self.plan_id,
            self.status.as_str(),
            self.evidence_completeness_status(),
            self.fallback.attempted
        )
    }

    fn attach_output_fields(&self, envelope: OutputEnvelope) -> OutputEnvelope {
        envelope
            .with_result_field("plan_id", self.plan_id.clone())
            .with_result_field("plan_kind", self.plan_kind.clone())
            .with_result_field("engine_mode", self.engine_mode.clone())
            .with_result_field("execution_status", self.status.as_str())
            .with_result_field(
                "execution_provider_kind",
                self.execution_provider_kind
                    .map_or_else(|| "none".to_string(), |kind| kind.as_str().to_string()),
            )
            .with_result_field(
                "provider_api_surface",
                optional_string(self.provider_api_surface.as_deref()),
            )
            .with_result_field(
                "provider_version",
                optional_string(self.provider_version.as_deref()),
            )
            .with_result_field(
                "evidence_completeness_status",
                self.evidence_completeness_status(),
            )
            .with_result_field("source_refs", csv_or_none(&self.source_refs))
            .with_result_field("split_refs", csv_or_none(&self.split_refs))
            .with_result_field("result_refs", csv_or_none(&self.result_refs))
            .with_result_field("artifact_refs", csv_or_none(&self.artifact_refs))
            .with_result_field(
                "inline_artifact_ids",
                csv_or_none(&self.inline_artifact_ids()),
            )
            .with_result_field(
                "execution_certificate_refs",
                csv_or_none(&self.execution_certificate_refs),
            )
            .with_result_field(
                "native_io_certificate_refs",
                csv_or_none(&self.native_io_certificate_refs),
            )
            .with_result_field(
                "materialization_boundary_refs",
                csv_or_none(&self.materialization_boundary_refs),
            )
            .with_result_field(
                "residual_boundary_refs",
                csv_or_none(&self.residual_boundary_refs),
            )
            .with_result_field(
                "representation_transitions",
                csv_or_none(&self.representation_transitions),
            )
            .with_policy_field("fallback_attempted", bool_str(self.fallback.attempted))
            .with_policy_field(
                "fallback_execution_allowed",
                bool_str(self.fallback.allowed),
            )
            .with_policy_field("fallback_reason", self.fallback.reason.clone())
            .with_policy_field(
                "external_engine_invoked",
                bool_str(self.external_engine_invoked),
            )
            .with_lifecycle_field("lifecycle_status", self.lifecycle_status.clone())
            .with_lifecycle_field("execution_status", self.status.as_str())
            .with_lifecycle_field(
                "evidence_completeness_status",
                self.evidence_completeness_status(),
            )
            .with_capability_snapshot_field("plan_kind", self.plan_kind.clone())
            .with_capability_snapshot_field(
                "execution_provider_kind",
                self.execution_provider_kind
                    .map_or_else(|| "none".to_string(), |kind| kind.as_str().to_string()),
            )
            .with_capability_snapshot_field(
                "provider_api_surface",
                optional_string(self.provider_api_surface.as_deref()),
            )
            .with_capability_snapshot_field(
                "provider_version",
                optional_string(self.provider_version.as_deref()),
            )
    }

    fn attach_output_refs_and_artifacts(&self, mut envelope: OutputEnvelope) -> OutputEnvelope {
        for reference in &self.result_refs {
            envelope = envelope.with_result_ref(OutputTypedRef::new(
                reference.clone(),
                "execution_result",
                "available",
            ));
        }
        for reference in &self.artifact_refs {
            envelope = envelope.with_artifact_ref(OutputTypedRef::new(
                reference.clone(),
                "execution_artifact",
                "available",
            ));
        }
        for reference in &self.execution_certificate_refs {
            envelope = envelope.with_certificate(OutputTypedRef::new(
                reference.clone(),
                "execution_certificate",
                "available",
            ));
        }
        for reference in &self.native_io_certificate_refs {
            envelope = envelope.with_certificate(OutputTypedRef::new(
                reference.clone(),
                "native_io_certificate",
                "available",
            ));
        }
        for artifact in &self.inline_artifacts {
            envelope = envelope.with_artifact(artifact.to_output_artifact());
        }
        envelope.with_artifact(self.evidence_slots_artifact())
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.is_success()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    fn evidence_slots_artifact(&self) -> OutputTypedArtifact {
        let slots = self.evidence_slots();
        let slot_order = slots
            .iter()
            .map(|slot| slot.kind.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let mut artifact = OutputTypedArtifact::new(
            format!("{}.execution_evidence_slots", self.plan_id),
            "execution_evidence_slots",
            self.evidence_completeness_status(),
        )
        .with_field("evidence_slot_order", slot_order);
        for slot in slots {
            let prefix = format!("evidence_slot_{}", slot.kind.as_str());
            artifact = artifact
                .with_field(format!("{prefix}_status"), slot.status.as_str())
                .with_field(format!("{prefix}_refs"), csv_or_none(&slot.refs))
                .with_field(format!("{prefix}_detail"), slot.detail);
        }
        artifact
    }

    fn source_refs_required(&self) -> bool {
        matches!(
            self.plan_kind.as_str(),
            "vortex_primitive" | "source_backed_encoded" | "reader_backed_encoded"
        )
    }

    fn split_refs_required(&self) -> bool {
        matches!(
            self.plan_kind.as_str(),
            "source_backed_encoded" | "reader_backed_encoded"
        )
    }

    fn materializing_transition_present(&self) -> bool {
        self.representation_transitions.iter().any(|transition| {
            transition.contains("partially_decoded")
                || transition.contains("decoded_columnar")
                || transition.contains("materialized_rows")
        })
    }
}

fn refs_slot(
    kind: ExecutionEvidenceSlotKind,
    refs: &[String],
    required: bool,
    detail: &'static str,
) -> ShardLoomExecutionEvidenceSlot {
    let status = if refs.is_empty() {
        if required {
            ExecutionEvidenceSlotStatus::EvidenceIncomplete
        } else {
            ExecutionEvidenceSlotStatus::NotRequired
        }
    } else {
        ExecutionEvidenceSlotStatus::Present
    };
    ShardLoomExecutionEvidenceSlot::new(kind, status, refs.to_vec(), detail)
}

fn optional_slot(
    kind: ExecutionEvidenceSlotKind,
    value: Option<&str>,
    required: bool,
    detail: &'static str,
) -> ShardLoomExecutionEvidenceSlot {
    let refs = value
        .filter(|value| !value.trim().is_empty())
        .map_or_else(Vec::new, |value| vec![value.to_string()]);
    refs_slot(kind, &refs, required, detail)
}

fn csv_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(",")
    }
}

fn optional_string(value: Option<&str>) -> String {
    value
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| "none".to_string(), str::to_string)
}

const fn bool_str(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Provider-side top-level execution dispatch.
pub trait ShardLoomExecutionProvider {
    /// Execute a top-level plan through the provider.
    ///
    /// # Errors
    /// Returns an error when the provider cannot construct its typed execution result.
    fn execute_plan(&self, plan: &Plan) -> Result<ShardLoomExecutionResult>;
}

/// Return a simple system status for process validation.
#[must_use]
pub fn status() -> ExecStatus {
    ExecStatus {
        summary: "ShardLoom workspace initialized (native Vortex-first execution facade)"
            .to_string(),
    }
}

/// Execute a plan through the provider-neutral facade.
///
/// Provider-specific executable plans must be dispatched via
/// `execute_with_provider`. The provider-neutral path never returns no-op
/// success for executable plans.
///
/// # Errors
/// This provider-neutral facade currently constructs deterministic blocked
/// diagnostics and does not perform fallible IO or provider dispatch.
pub fn execute(plan: &Plan) -> Result<ShardLoomExecutionResult> {
    if matches!(plan.kind, PlanKind::ReportOnly(_)) {
        Ok(ShardLoomExecutionResult::report_only(plan))
    } else {
        Ok(ShardLoomExecutionResult::blocked_provider_dispatch_required(plan))
    }
}

/// Execute a plan through a concrete native provider.
///
/// # Errors
/// Returns provider errors from the selected execution provider.
pub fn execute_with_provider(
    plan: &Plan,
    provider: &dyn ShardLoomExecutionProvider,
) -> Result<ShardLoomExecutionResult> {
    provider.execute_plan(plan)
}

/// Fail explicitly for unsupported operations in the provider-neutral facade.
///
/// # Errors
/// Returns an error when the synthetic unsupported plan id cannot be constructed.
pub fn unsupported(operation: &str) -> Result<ShardLoomExecutionResult> {
    let plan = Plan::report_only(
        shardloom_plan::PlanId::new(format!("unsupported.{operation}"))?,
        shardloom_plan::ReportOnlyPlan::new("unsupported_operation"),
    );
    Ok(ShardLoomExecutionResult::blocked_unsupported(
        &plan,
        Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            operation,
            format!("unsupported execution path: {operation}; no fallback engines are enabled"),
            Some("Use a ShardLoom-native supported plan surface.".to_string()),
        ),
    ))
}

// Memory and spill planning/promotion surfaces; allocator/runtime spill remains
// evidence-gated unless a specific feature-gated helper says otherwise.
pub use memory::{
    MemoryAdmissionDecisionKind, MemoryAdmissionReport, MemoryBudget, MemoryOwner, MemoryPoolPlan,
    MemoryPoolSnapshot, MemoryPressureLevel, MemoryReservation, MemoryReservationId,
    MemoryReservationStatus, MemoryRuntimeHardeningGateEntry, MemoryRuntimeHardeningGateReport,
    MemoryRuntimeHardeningStatus, MemoryRuntimeHardeningSurface, OomSafetyPlan,
    OperatorMemoryClass, OperatorMemorySpillDeclaration, OperatorMemorySpillDeclarationReport,
    OperatorMemorySpillDeclarationStatus, SpillCompression, SpillDecision, SpillDecisionKind,
    SpillFileRef, SpillFileStatus, SpillFormat, SpillPartition, SpillPlan, SpillPlanStatus,
    SpillPolicy, SpillReport, plan_memory_runtime_hardening_gate,
    plan_operator_memory_spill_declarations,
};

// Recovery, retry, cancellation, cleanup, and commit promotion contracts.
pub use recovery::{
    AmbiguousCommitRecord, AttemptId, CancellationReason, CancellationRequest, CancellationScope,
    CancellationStatus, CleanupExecutionOption, CleanupRequirement, CleanupStatus,
    CleanupTargetKind, CommitExecutionPromotionGateEntry, CommitExecutionPromotionGateReport,
    CommitExecutionPromotionStatus, CommitExecutionPromotionSurface, CommitRecoveryState,
    FailureDomain, FailureKind, FailureRecord, FaultToleranceLevel, FaultTolerancePromotionArea,
    FaultTolerancePromotionGateEntry, FaultTolerancePromotionGateReport,
    FaultTolerancePromotionStatus, PartialOutputRecord, RecoveryAction, RecoveryActionKind,
    RecoveryPlan, RecoveryPlanStatus, RecoveryReport, RetryDecision, RetryDecisionKind,
    RetryEligibility, RetryPlan, ShardLoomCancellationExecutionGateEffect,
    ShardLoomCancellationExecutionGateMode, ShardLoomCancellationExecutionGateReport,
    ShardLoomCancellationExecutionGateRequest, ShardLoomCancellationExecutionGateSignal,
    ShardLoomCancellationExecutionGateStatus, ShardLoomCleanupExecutionEffect,
    ShardLoomCleanupExecutionMode, ShardLoomCleanupExecutionReport,
    ShardLoomCleanupExecutionRequest, ShardLoomCleanupExecutionStatus,
    ShardLoomRetryExecutionGateEffect, ShardLoomRetryExecutionGateMode,
    ShardLoomRetryExecutionGateReport, ShardLoomRetryExecutionGateRequest,
    ShardLoomRetryExecutionGateSignal, ShardLoomRetryExecutionGateStatus, TaskAttemptRecord,
    TaskAttemptStatus, cancellation_execution_gate_is_side_effect_free,
    cleanup_execution_plan_is_side_effect_free, plan_cancellation_execution_gate,
    plan_cleanup_execution, plan_commit_execution_promotion_gate,
    plan_fault_tolerance_promotion_gate, plan_retry_execution_gate,
    retry_execution_gate_is_side_effect_free,
};

// Adaptive sizing and bounded work-shaping planning surfaces.
pub use sizing::{
    AdaptiveSizer, AdaptiveSizingPolicy, ByteSize, CoalescingPolicy,
    DynamicRuntimePromotionGateEntry, DynamicRuntimePromotionGateReport,
    DynamicRuntimePromotionStatus, DynamicRuntimePromotionSurface, DynamicSizingFeedbackInput,
    DynamicSizingFeedbackMode, DynamicSizingFeedbackReport, DynamicSizingFeedbackStatus,
    DynamicWorkShapingReport, DynamicWorkShapingStatus, ParallelismLimit, ParallelismPlan,
    SizeEstimate, SizingFeedbackSignal, SizingFeedbackSignalKind, SizingInput, SizingPlan,
    TaskSizingDecision, TaskSizingDecisionKind, TaskSizingMode,
    plan_dynamic_runtime_promotion_gate, plan_dynamic_sizing_feedback, plan_dynamic_work_shaping,
};

// Streaming and zero-copy boundary planning surfaces; live execution is blocked.
pub use streaming::{
    BackpressurePlanInput, BackpressurePlanMode, BackpressurePlanReport, BackpressurePlanStatus,
    BackpressurePolicy, BoundaryInteropKind, BoundedMemoryPolicy, DataWorkLevel,
    EncodedBatchRepresentation, EncodedStreamingBatchPlanInput, EncodedStreamingBatchPlanReport,
    EncodedStreamingBatchPlanStatus, MaterializationBoundary, SinkRequirement, StreamingCapability,
    StreamingMode, StreamingOperator, StreamingOperatorKind, StreamingPlanSkeleton,
    StreamingPlanStatus, StreamingSink, StreamingSinkKind, StreamingSource, StreamingSourceKind,
    StreamingStage, ZeroCopyStatus, ZeroDecodeStatus, plan_backpressure,
    plan_encoded_streaming_batches,
};

pub use spill_lifecycle::*;

// Explicit spill-payload artifact helpers and report-only payload contracts.
pub use spill_payload::{
    SpillPayloadEffect, SpillPayloadFsFeatureStatus, SpillPayloadFsPlanMode,
    SpillPayloadFsPlanReport, SpillPayloadFsPlanStatus, SpillPayloadFsRef, SpillPayloadId,
    SpillPayloadMode, SpillPayloadPath, SpillPayloadPlanReport, SpillPayloadPlanRequest,
    SpillPayloadReadReport, SpillPayloadReadRequest, SpillPayloadRef, SpillPayloadRoundTripOption,
    SpillPayloadRoundTripReport, SpillPayloadRoundTripRequest, SpillPayloadStatus,
    SpillPayloadWriteOption, SpillPayloadWriteReport, SpillPayloadWriteRequest,
    SyntheticSpillPayload, plan_spill_payload, plan_spill_payload_filesystem_ref,
    read_spill_payload, roundtrip_spill_payload, spill_payload_fs_feature_enabled,
    spill_payload_plan_is_side_effect_free, write_spill_payload,
};

// Runtime task-graph planning surfaces; object-store/distributed task execution is blocked.
pub use runtime::{
    ByteRangeRequest, ObjectStoreKind, ObjectStoreRef, ReadPolicy, ResourceBudget, RetryPolicy,
    RuntimePlanSkeleton, RuntimePlanningStatus, SegmentTask, ShuffleRequirement, TaskGraph, TaskId,
    TaskKind, TaskStatus,
};

#[cfg(test)]
mod tests {
    use shardloom_plan::{Plan, PlanId, ReportOnlyPlan, build_vortex_count_all_plan};

    use super::{
        ExecutionEvidenceSlotKind, ExecutionEvidenceSlotStatus, ShardLoomExecutionInlineArtifact,
        ShardLoomExecutionResult, ShardLoomExecutionStatus, execute, status, unsupported,
    };

    #[test]
    fn reports_status() {
        assert!(status().summary.contains("initialized"));
    }

    #[test]
    fn executable_plan_requires_provider_dispatch() {
        let plan =
            build_vortex_count_all_plan("plan.count", "file://tmp/data.vortex").expect("plan");
        let result = execute(&plan).expect("execution result");
        assert_eq!(
            result.status,
            ShardLoomExecutionStatus::BlockedProviderDispatchRequired
        );
        assert_eq!(
            result.provider_api_surface.as_deref(),
            Some("vortex_local_primitive")
        );
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
        assert!(result.has_errors());
    }

    #[test]
    fn report_only_plan_is_not_noop_execution() {
        let plan = Plan::report_only(
            PlanId::new("plan.report").expect("plan id"),
            ReportOnlyPlan::new("architecture_spine"),
        );
        let result = execute(&plan).expect("execution result");
        assert_eq!(result.status, ShardLoomExecutionStatus::ReportOnly);
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
        assert!(result.result_refs.is_empty());
    }

    #[test]
    fn provider_required_result_marks_missing_evidence_slots() {
        let plan =
            build_vortex_count_all_plan("plan.count", "file://tmp/data.vortex").expect("plan");
        let result = execute(&plan).expect("execution result");
        let slots = result.evidence_slots();

        let provider_version = slots
            .iter()
            .find(|slot| slot.kind == ExecutionEvidenceSlotKind::ProviderVersion)
            .expect("provider version slot");
        assert_eq!(
            provider_version.status,
            ExecutionEvidenceSlotStatus::EvidenceIncomplete
        );
        assert_eq!(result.evidence_completeness_status(), "evidence_incomplete");
        let fallback = slots
            .iter()
            .find(|slot| slot.kind == ExecutionEvidenceSlotKind::FallbackStatus)
            .expect("fallback slot");
        assert_eq!(fallback.status, ExecutionEvidenceSlotStatus::Present);
        assert_eq!(fallback.refs, vec!["attempted=false".to_string()]);
    }

    #[test]
    fn typed_output_envelope_preserves_artifact_rich_execution_result() {
        let plan =
            build_vortex_count_all_plan("plan.count", "file://tmp/data.vortex").expect("plan");
        let mut result = ShardLoomExecutionResult::executed(&plan);
        result.provider_version = Some("provider-1".to_string());
        result.result_refs.push("result.rows".to_string());
        result
            .artifact_refs
            .push("artifact.provider-report".to_string());
        result
            .execution_certificate_refs
            .push("cert.execution".to_string());
        result
            .native_io_certificate_refs
            .push("cert.native-io".to_string());
        result
            .representation_transitions
            .push("vortex_encoded->vortex_encoded".to_string());
        result.add_inline_artifact(
            ShardLoomExecutionInlineArtifact::new(
                "artifact.provider-report",
                "provider_report",
                "available",
            )
            .with_field("provider_api_surface", "vortex_local_primitive")
            .with_field("fallback_attempted", "false"),
        );

        let envelope = result.to_output_envelope("top-level-exec", "executed");
        let json = envelope.to_json();

        assert!(json.contains("\"result_refs\":[{\"id\":\"result.rows\""));
        assert!(json.contains("\"artifact_refs\":[{\"id\":\"artifact.provider-report\""));
        assert!(json.contains("\"certificates\":[{\"id\":\"cert.execution\""));
        assert!(json.contains("\"id\":\"cert.native-io\""));
        assert!(json.contains("\"artifact_id\":\"artifact.provider-report\""));
        assert!(json.contains("\"artifact_kind\":\"execution_evidence_slots\""));
        assert!(json.contains("\"key\":\"provider_version\",\"value\":\"provider-1\""));
        assert!(json.contains("\"key\":\"fallback_attempted\",\"value\":\"false\""));
        assert_eq!(result.evidence_completeness_status(), "complete");
    }

    #[test]
    fn unsupported_fails_explicitly() {
        let result = unsupported("join").expect("unsupported result");
        assert_eq!(result.status, ShardLoomExecutionStatus::BlockedUnsupported);
        assert!(result.has_errors());
        assert!(!result.fallback_attempted());
    }
}
