//! Fault-tolerance, cancellation, retry, and recovery planning skeletons.
//!
//! This module is planning/reporting-only and intentionally performs no retry,
//! cancellation propagation, recovery execution, cleanup IO, or commit protocol IO.
#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_errors_doc,
    clippy::struct_excessive_bools
)]

use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
    OutputTarget, Result, ShardLoomError,
};
use std::fmt::Write as _;

use crate::{RetryPolicy, TaskId};

fn has_error_diagnostics(diagnostics: &[Diagnostic]) -> bool {
    diagnostics.iter().any(|d| {
        matches!(
            d.severity,
            DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
        )
    })
}

fn unsupported_diagnostic(feature: impl Into<String>, reason: impl Into<String>) -> Diagnostic {
    let feature = feature.into();
    let reason = reason.into();
    Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        feature,
        reason,
        Some(
            "Use supported planning/introspection commands; fallback execution remains disabled."
                .to_string(),
        ),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failure domain for native `ShardLoom` execution and effect boundaries.
pub enum FailureDomain {
    Planning,
    Metadata,
    VortexIo,
    ObjectStoreRead,
    ObjectStoreWrite,
    ExecutionTask,
    MemoryReservation,
    Spill,
    Shuffle,
    OutputTranslation,
    Commit,
    Cleanup,
    ExternalApi,
    ModelCall,
    EmbeddingGeneration,
    VectorSearch,
    CredentialAuth,
    UserCancellation,
    SystemCancellation,
    WorkerProcess,
    Unsupported,
}
impl FailureDomain {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Metadata => "metadata",
            Self::VortexIo => "vortex_io",
            Self::ObjectStoreRead => "object_store_read",
            Self::ObjectStoreWrite => "object_store_write",
            Self::ExecutionTask => "execution_task",
            Self::MemoryReservation => "memory_reservation",
            Self::Spill => "spill",
            Self::Shuffle => "shuffle",
            Self::OutputTranslation => "output_translation",
            Self::Commit => "commit",
            Self::Cleanup => "cleanup",
            Self::ExternalApi => "external_api",
            Self::ModelCall => "model_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
            Self::CredentialAuth => "credential_auth",
            Self::UserCancellation => "user_cancellation",
            Self::SystemCancellation => "system_cancellation",
            Self::WorkerProcess => "worker_process",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
    pub const fn is_external_effect(&self) -> bool {
        matches!(
            self,
            Self::ExternalApi | Self::ModelCall | Self::EmbeddingGeneration | Self::VectorSearch
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Failure kind metadata used for explicit retry/recovery planning only.
pub enum FailureKind {
    MissingInput,
    PermissionDenied,
    Timeout,
    RateLimited,
    PartialRead,
    PartialWrite,
    AmbiguousCommit,
    MemoryBudgetExceeded,
    SpillUnavailable,
    CleanupFailed,
    CancellationRequested,
    ExternalEffectUnsafeToRetry,
    UnsupportedFeature,
    NotImplemented,
    Unknown,
}
impl FailureKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MissingInput => "missing_input",
            Self::PermissionDenied => "permission_denied",
            Self::Timeout => "timeout",
            Self::RateLimited => "rate_limited",
            Self::PartialRead => "partial_read",
            Self::PartialWrite => "partial_write",
            Self::AmbiguousCommit => "ambiguous_commit",
            Self::MemoryBudgetExceeded => "memory_budget_exceeded",
            Self::SpillUnavailable => "spill_unavailable",
            Self::CleanupFailed => "cleanup_failed",
            Self::CancellationRequested => "cancellation_requested",
            Self::ExternalEffectUnsafeToRetry => "external_effect_unsafe_to_retry",
            Self::UnsupportedFeature => "unsupported_feature",
            Self::NotImplemented => "not_implemented",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_retryable_candidate(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::RateLimited | Self::PartialRead | Self::Unknown
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Validated task-attempt identifier for planning/reporting records.
pub struct AttemptId(String);
impl AttemptId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "attempt id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAttemptStatus {
    Planned,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    RetryScheduled,
    CleanupRequired,
    Unsupported,
}
impl TaskAttemptStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::RetryScheduled => "retry_scheduled",
            Self::CleanupRequired => "cleanup_required",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::Cancelled
                | Self::CleanupRequired
                | Self::Unsupported
        )
    }
    pub const fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::Failed | Self::CleanupRequired | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryEligibility {
    Retryable,
    NotRetryable,
    RequiresIdempotency,
    RequiresCleanup,
    Unknown,
}
impl RetryEligibility {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::NotRetryable => "not_retryable",
            Self::RequiresIdempotency => "requires_idempotency",
            Self::RequiresCleanup => "requires_cleanup",
            Self::Unknown => "unknown",
        }
    }
    pub const fn can_retry_now(&self) -> bool {
        matches!(self, Self::Retryable)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FailureRecord {
    pub domain: FailureDomain,
    pub kind: FailureKind,
    pub message: String,
    pub retry_eligibility: RetryEligibility,
    pub diagnostics: Vec<Diagnostic>,
}
impl FailureRecord {
    pub fn new(domain: FailureDomain, kind: FailureKind, message: impl Into<String>) -> Self {
        Self {
            domain,
            kind,
            message: message.into(),
            retry_eligibility: if kind.is_retryable_candidate() {
                RetryEligibility::Retryable
            } else {
                RetryEligibility::NotRetryable
            },
            diagnostics: vec![],
        }
    }
    pub fn with_retry_eligibility(mut self, retry_eligibility: RetryEligibility) -> Self {
        self.retry_eligibility = retry_eligibility;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "failure domain={} kind={} retry_eligibility={} message={} fallback execution: disabled",
            self.domain.as_str(),
            self.kind.as_str(),
            self.retry_eligibility.as_str(),
            self.message
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskAttemptRecord {
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub status: TaskAttemptStatus,
    pub failure: Option<FailureRecord>,
    pub output_files: Vec<String>,
    pub spill_files: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl TaskAttemptRecord {
    pub fn new(task_id: TaskId, attempt_id: AttemptId) -> Self {
        Self {
            task_id,
            attempt_id,
            status: TaskAttemptStatus::Planned,
            failure: None,
            output_files: vec![],
            spill_files: vec![],
            diagnostics: vec![],
        }
    }
    pub fn succeeded(mut self) -> Self {
        self.status = TaskAttemptStatus::Succeeded;
        self
    }
    pub fn failed(mut self, failure: FailureRecord) -> Self {
        self.status = TaskAttemptStatus::Failed;
        self.failure = Some(failure);
        self
    }
    pub fn cancelled(mut self, reason: impl Into<String>) -> Self {
        self.status = TaskAttemptStatus::Cancelled;
        self.failure = Some(FailureRecord::new(
            FailureDomain::UserCancellation,
            FailureKind::CancellationRequested,
            reason,
        ));
        self
    }
    pub fn add_output_file(&mut self, output_file: impl Into<String>) {
        self.output_files.push(output_file.into());
    }
    pub fn add_spill_file(&mut self, spill_file: impl Into<String>) {
        self.spill_files.push(spill_file.into());
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn requires_cleanup(&self) -> bool {
        self.status == TaskAttemptStatus::CleanupRequired
            || (matches!(
                self.status,
                TaskAttemptStatus::Failed | TaskAttemptStatus::Cancelled
            ) && (!self.output_files.is_empty() || !self.spill_files.is_empty()))
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_failure()
            || self.failure.as_ref().is_some_and(FailureRecord::has_errors)
            || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "task={} attempt={} status={} requires_cleanup={} fallback execution: disabled",
            self.task_id.as_str(),
            self.attempt_id.as_str(),
            self.status.as_str(),
            self.requires_cleanup()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecisionKind {
    RetryNow,
    RetryAfterCleanup,
    DoNotRetry,
    RequireIdempotencyKey,
    Unsupported,
}
impl RetryDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryNow => "retry_now",
            Self::RetryAfterCleanup => "retry_after_cleanup",
            Self::DoNotRetry => "do_not_retry",
            Self::RequireIdempotencyKey => "require_idempotency_key",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn will_retry(&self) -> bool {
        matches!(self, Self::RetryNow | Self::RetryAfterCleanup)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryDecision {
    pub kind: RetryDecisionKind,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl RetryDecision {
    pub fn retry_now(reason: impl Into<String>) -> Self {
        Self {
            kind: RetryDecisionKind::RetryNow,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn retry_after_cleanup(reason: impl Into<String>) -> Self {
        Self {
            kind: RetryDecisionKind::RetryAfterCleanup,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn do_not_retry(reason: impl Into<String>) -> Self {
        Self {
            kind: RetryDecisionKind::DoNotRetry,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn require_idempotency_key(reason: impl Into<String>) -> Self {
        Self {
            kind: RetryDecisionKind::RequireIdempotencyKey,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: RetryDecisionKind::Unsupported,
            reason: reason.into(),
            diagnostics: vec![unsupported_diagnostic(
                feature,
                "Retry decision unsupported in skeleton",
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn will_retry(&self) -> bool {
        self.kind.will_retry()
    }
    pub fn has_errors(&self) -> bool {
        self.kind == RetryDecisionKind::Unsupported || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "retry_decision={} will_retry={} reason={} fallback execution: disabled",
            self.kind.as_str(),
            self.will_retry(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryPlan {
    pub policy: RetryPolicy,
    pub attempt: TaskAttemptRecord,
    pub decision: RetryDecision,
    pub diagnostics: Vec<Diagnostic>,
}
impl RetryPlan {
    pub fn from_attempt(policy: RetryPolicy, attempt: TaskAttemptRecord) -> Self {
        let decision = if attempt.status == TaskAttemptStatus::Succeeded {
            RetryDecision::do_not_retry("attempt already succeeded")
        } else if attempt.requires_cleanup() {
            RetryDecision::retry_after_cleanup("cleanup required before retry")
        } else if let Some(f) = &attempt.failure {
            if f.retry_eligibility == RetryEligibility::Retryable && policy.max_attempts > 1 {
                RetryDecision::retry_now("failure retry-eligible and policy allows retries")
            } else if f.retry_eligibility == RetryEligibility::RequiresIdempotency {
                RetryDecision::require_idempotency_key("idempotency key required before retry")
            } else {
                RetryDecision::do_not_retry("failure is not retryable under current policy")
            }
        } else {
            RetryDecision::do_not_retry("no failure record present")
        };
        Self {
            policy,
            attempt,
            decision,
            diagnostics: vec![],
        }
    }
    pub fn unsupported(
        policy: RetryPolicy,
        attempt: TaskAttemptRecord,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut decision = RetryDecision::unsupported(feature, reason.into());
        decision.reason = "retry planning unsupported".to_string();
        Self {
            policy,
            attempt,
            decision,
            diagnostics: vec![],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.decision.has_errors() || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "retry_plan decision={} policy={} fallback execution: disabled",
            self.decision.kind.as_str(),
            self.policy.summary()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationScope {
    Query,
    Task,
    Scan,
    OutputWrite,
    ExternalEffect,
    SpillCleanup,
    Runtime,
}
impl CancellationScope {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Task => "task",
            Self::Scan => "scan",
            Self::OutputWrite => "output_write",
            Self::ExternalEffect => "external_effect",
            Self::SpillCleanup => "spill_cleanup",
            Self::Runtime => "runtime",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    UserRequested,
    Timeout,
    ResourceBudgetExceeded,
    DependencyFailed,
    Shutdown,
    Unsupported,
}
impl CancellationReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UserRequested => "user_requested",
            Self::Timeout => "timeout",
            Self::ResourceBudgetExceeded => "resource_budget_exceeded",
            Self::DependencyFailed => "dependency_failed",
            Self::Shutdown => "shutdown",
            Self::Unsupported => "unsupported",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationStatus {
    Requested,
    CooperativePending,
    Completed,
    Failed,
    Unsupported,
}
impl CancellationStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::CooperativePending => "cooperative_pending",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Unsupported)
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Failed | Self::Unsupported)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CancellationRequest {
    pub scope: CancellationScope,
    pub reason: CancellationReason,
    pub status: CancellationStatus,
    pub target: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl CancellationRequest {
    pub fn new(scope: CancellationScope, reason: CancellationReason) -> Self {
        Self {
            scope,
            reason,
            status: CancellationStatus::Requested,
            target: None,
            diagnostics: vec![],
        }
    }
    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }
    pub fn unsupported(scope: CancellationScope, reason: impl Into<String>) -> Self {
        Self {
            scope,
            reason: CancellationReason::Unsupported,
            status: CancellationStatus::Unsupported,
            target: None,
            diagnostics: vec![unsupported_diagnostic("cancellation", reason)],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error() || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "cancellation scope={} reason={} status={} fallback execution: disabled",
            self.scope.as_str(),
            self.reason.as_str(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryActionKind {
    RetryTask,
    CleanupTemporaryOutput,
    CleanupSpillFiles,
    ValidateCommit,
    MarkCommitAmbiguous,
    AbortWrite,
    RebuildManifest,
    ResumeFromSnapshot,
    ReportOnly,
    Unsupported,
}
impl RecoveryActionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryTask => "retry_task",
            Self::CleanupTemporaryOutput => "cleanup_temporary_output",
            Self::CleanupSpillFiles => "cleanup_spill_files",
            Self::ValidateCommit => "validate_commit",
            Self::MarkCommitAmbiguous => "mark_commit_ambiguous",
            Self::AbortWrite => "abort_write",
            Self::RebuildManifest => "rebuild_manifest",
            Self::ResumeFromSnapshot => "resume_from_snapshot",
            Self::ReportOnly => "report_only",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn requires_io(&self) -> bool {
        matches!(
            self,
            Self::CleanupTemporaryOutput
                | Self::CleanupSpillFiles
                | Self::ValidateCommit
                | Self::AbortWrite
                | Self::RebuildManifest
                | Self::ResumeFromSnapshot
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryAction {
    pub kind: RecoveryActionKind,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl RecoveryAction {
    pub fn new(kind: RecoveryActionKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: RecoveryActionKind::Unsupported,
            reason: reason.into(),
            diagnostics: vec![unsupported_diagnostic(
                feature,
                "Recovery action unsupported",
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.kind == RecoveryActionKind::Unsupported || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "recovery_action kind={} requires_io={} fallback execution: disabled",
            self.kind.as_str(),
            self.kind.requires_io()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupTargetKind {
    TemporaryOutput,
    SpillFile,
    PartialCommit,
    ManifestSidecar,
    Unknown,
}
impl CleanupTargetKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::TemporaryOutput => "temporary_output",
            Self::SpillFile => "spill_file",
            Self::PartialCommit => "partial_commit",
            Self::ManifestSidecar => "manifest_sidecar",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupStatus {
    NotRequired,
    Required,
    Planned,
    Completed,
    Failed,
    Unsupported,
}
impl CleanupStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Required => "required",
            Self::Planned => "planned",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Failed | Self::Unsupported)
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::NotRequired | Self::Completed | Self::Failed | Self::Unsupported
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CleanupRequirement {
    pub target_kind: CleanupTargetKind,
    pub target: String,
    pub status: CleanupStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl CleanupRequirement {
    fn validated_target(target: impl Into<String>) -> Result<String> {
        let target = target.into();
        if target.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "cleanup target must not be empty".to_string(),
            ));
        }
        Ok(target)
    }
    pub fn required(target_kind: CleanupTargetKind, target: impl Into<String>) -> Result<Self> {
        Ok(Self {
            target_kind,
            target: Self::validated_target(target)?,
            status: CleanupStatus::Required,
            diagnostics: vec![],
        })
    }
    pub fn not_required(target_kind: CleanupTargetKind, target: impl Into<String>) -> Result<Self> {
        Ok(Self {
            target_kind,
            target: Self::validated_target(target)?,
            status: CleanupStatus::NotRequired,
            diagnostics: vec![],
        })
    }
    pub fn unsupported(
        target_kind: CleanupTargetKind,
        target: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            target_kind,
            target: Self::validated_target(target)?,
            status: CleanupStatus::Unsupported,
            diagnostics: vec![unsupported_diagnostic("cleanup", reason)],
        })
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error() || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "cleanup target_kind={} target={} status={} fallback execution: disabled",
            self.target_kind.as_str(),
            self.target,
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartialOutputRecord {
    pub target: OutputTarget,
    pub files: Vec<String>,
    pub cleanup: Vec<CleanupRequirement>,
    pub diagnostics: Vec<Diagnostic>,
}
impl PartialOutputRecord {
    pub fn new(target: OutputTarget) -> Self {
        Self {
            target,
            files: vec![],
            cleanup: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_file(&mut self, file: impl Into<String>) {
        self.files.push(file.into());
    }
    pub fn add_cleanup_requirement(&mut self, cleanup: CleanupRequirement) {
        self.cleanup.push(cleanup);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn requires_cleanup(&self) -> bool {
        self.cleanup.iter().any(|c| {
            matches!(
                c.status,
                CleanupStatus::Required
                    | CleanupStatus::Planned
                    | CleanupStatus::Failed
                    | CleanupStatus::Unsupported
            )
        })
    }
    pub fn has_errors(&self) -> bool {
        self.cleanup.iter().any(CleanupRequirement::has_errors)
            || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "partial_output files={} requires_cleanup={} fallback execution: disabled",
            self.files.len(),
            self.requires_cleanup()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitRecoveryState {
    NotStarted,
    Planned,
    WritingTemporaryFiles,
    Validating,
    Committing,
    Committed,
    Failed,
    Ambiguous,
    Aborted,
    CleanupRequired,
    Unsupported,
}
impl CommitRecoveryState {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Planned => "planned",
            Self::WritingTemporaryFiles => "writing_temporary_files",
            Self::Validating => "validating",
            Self::Committing => "committing",
            Self::Committed => "committed",
            Self::Failed => "failed",
            Self::Ambiguous => "ambiguous",
            Self::Aborted => "aborted",
            Self::CleanupRequired => "cleanup_required",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Committed
                | Self::Failed
                | Self::Ambiguous
                | Self::Aborted
                | Self::CleanupRequired
                | Self::Unsupported
        )
    }
    pub const fn is_ambiguous(&self) -> bool {
        matches!(self, Self::Ambiguous)
    }
    pub const fn requires_cleanup(&self) -> bool {
        matches!(self, Self::Failed | Self::Ambiguous | Self::CleanupRequired)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AmbiguousCommitRecord {
    pub commit_id: String,
    pub state: CommitRecoveryState,
    pub output: Option<PartialOutputRecord>,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl AmbiguousCommitRecord {
    pub fn new(commit_id: impl Into<String>, reason: impl Into<String>) -> Result<Self> {
        let commit_id = commit_id.into();
        if commit_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "commit id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            commit_id,
            state: CommitRecoveryState::Ambiguous,
            output: None,
            reason: reason.into(),
            diagnostics: vec![],
        })
    }
    pub fn with_output(mut self, output: PartialOutputRecord) -> Self {
        self.output = Some(output);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn requires_cleanup(&self) -> bool {
        self.state.requires_cleanup()
            || self
                .output
                .as_ref()
                .is_some_and(PartialOutputRecord::requires_cleanup)
    }
    pub fn has_errors(&self) -> bool {
        self.state == CommitRecoveryState::Ambiguous || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "ambiguous_commit commit_id={} state={} fallback execution: disabled",
            self.commit_id,
            self.state.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultToleranceLevel {
    None,
    DiagnosticOnly,
    Retryable,
    Recoverable,
    Idempotent,
    TransactionalIfBackendSupports,
    Unsupported,
}
impl FaultToleranceLevel {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::DiagnosticOnly => "diagnostic_only",
            Self::Retryable => "retryable",
            Self::Recoverable => "recoverable",
            Self::Idempotent => "idempotent",
            Self::TransactionalIfBackendSupports => "transactional_if_backend_supports",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn supports_recovery(&self) -> bool {
        matches!(
            self,
            Self::Recoverable | Self::Idempotent | Self::TransactionalIfBackendSupports
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPlanStatus {
    Planned,
    DiagnosticOnly,
    RecoveryNotImplemented,
    Unsupported,
}
impl RecoveryPlanStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::DiagnosticOnly => "diagnostic_only",
            Self::RecoveryNotImplemented => "recovery_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::RecoveryNotImplemented | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Recovery planning skeleton; no recovery actions are executed by this type.
pub struct RecoveryPlan {
    pub status: RecoveryPlanStatus,
    pub fault_tolerance_level: FaultToleranceLevel,
    pub actions: Vec<RecoveryAction>,
    pub cleanup: Vec<CleanupRequirement>,
    pub retry_plan: Option<RetryPlan>,
    pub cancellation: Option<CancellationRequest>,
    pub ambiguous_commit: Option<AmbiguousCommitRecord>,
    pub diagnostics: Vec<Diagnostic>,
}
impl RecoveryPlan {
    pub fn diagnostic_only() -> Self {
        Self {
            status: RecoveryPlanStatus::DiagnosticOnly,
            fault_tolerance_level: FaultToleranceLevel::DiagnosticOnly,
            actions: vec![],
            cleanup: vec![],
            retry_plan: None,
            cancellation: None,
            ambiguous_commit: None,
            diagnostics: vec![],
        }
    }
    pub fn recovery_not_implemented(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut p = Self::diagnostic_only();
        p.status = RecoveryPlanStatus::RecoveryNotImplemented;
        p.diagnostics.push(unsupported_diagnostic(feature, reason));
        p
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut p = Self::diagnostic_only();
        p.status = RecoveryPlanStatus::Unsupported;
        p.fault_tolerance_level = FaultToleranceLevel::Unsupported;
        p.diagnostics.push(unsupported_diagnostic(feature, reason));
        p
    }
    pub fn add_action(&mut self, action: RecoveryAction) {
        self.actions.push(action);
    }
    pub fn add_cleanup(&mut self, cleanup: CleanupRequirement) {
        self.cleanup.push(cleanup);
    }
    pub fn with_retry_plan(mut self, retry_plan: RetryPlan) -> Self {
        self.retry_plan = Some(retry_plan);
        self
    }
    pub fn with_cancellation(mut self, cancellation: CancellationRequest) -> Self {
        self.cancellation = Some(cancellation);
        self
    }
    pub fn with_ambiguous_commit(mut self, ambiguous_commit: AmbiguousCommitRecord) -> Self {
        self.ambiguous_commit = Some(ambiguous_commit);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn requires_cleanup(&self) -> bool {
        self.cleanup.iter().any(|c| {
            matches!(
                c.status,
                CleanupStatus::Required
                    | CleanupStatus::Planned
                    | CleanupStatus::Failed
                    | CleanupStatus::Unsupported
            )
        }) || self
            .ambiguous_commit
            .as_ref()
            .is_some_and(AmbiguousCommitRecord::requires_cleanup)
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.actions.iter().any(RecoveryAction::has_errors)
            || self.cleanup.iter().any(CleanupRequirement::has_errors)
            || self.retry_plan.as_ref().is_some_and(RetryPlan::has_errors)
            || self
                .cancellation
                .as_ref()
                .is_some_and(CancellationRequest::has_errors)
            || self
                .ambiguous_commit
                .as_ref()
                .is_some_and(AmbiguousCommitRecord::has_errors)
            || has_error_diagnostics(&self.diagnostics)
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "Recovery plan status={} level={} actions={} cleanup={} fallback execution: disabled",
            self.status.as_str(),
            self.fault_tolerance_level.as_str(),
            self.actions.len(),
            self.cleanup.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Recovery reporting skeleton derived from plans without executing recovery.
pub struct RecoveryReport {
    pub status: RecoveryPlanStatus,
    pub actions_completed: usize,
    pub cleanup_completed: bool,
    pub diagnostics: Vec<Diagnostic>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultTolerancePromotionArea {
    RetryExecution,
    CancellationPropagation,
    CleanupExecution,
    AmbiguousCommitResolution,
    IdempotencyKeying,
    RecoveryExecution,
}
impl FaultTolerancePromotionArea {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryExecution => "retry_execution",
            Self::CancellationPropagation => "cancellation_propagation",
            Self::CleanupExecution => "cleanup_execution",
            Self::AmbiguousCommitResolution => "ambiguous_commit_resolution",
            Self::IdempotencyKeying => "idempotency_keying",
            Self::RecoveryExecution => "recovery_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultTolerancePromotionStatus {
    BlockedUntilCertified,
    ReportOnly,
    ExecutionReady,
}
impl FaultTolerancePromotionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BlockedUntilCertified => "blocked_until_certified",
            Self::ReportOnly => "report_only",
            Self::ExecutionReady => "execution_ready",
        }
    }
    pub const fn allows_execution(&self) -> bool {
        matches!(self, Self::ExecutionReady)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultTolerancePromotionGateEntry {
    pub area: FaultTolerancePromotionArea,
    pub status: FaultTolerancePromotionStatus,
    pub required_evidence: &'static str,
    pub requires_side_effect_boundary: bool,
    pub requires_commit_semantics: bool,
    pub execution_allowed: bool,
}
impl FaultTolerancePromotionGateEntry {
    pub const fn blocked(
        area: FaultTolerancePromotionArea,
        required_evidence: &'static str,
        requires_side_effect_boundary: bool,
        requires_commit_semantics: bool,
    ) -> Self {
        Self {
            area,
            status: FaultTolerancePromotionStatus::BlockedUntilCertified,
            required_evidence,
            requires_side_effect_boundary,
            requires_commit_semantics,
            execution_allowed: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultTolerancePromotionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<FaultTolerancePromotionGateEntry>,
    pub side_effect_boundaries_certified: bool,
    pub commit_semantics_certified: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub cg4_output_commit_evidence_required: bool,
    pub cg8_write_recovery_evidence_required: bool,
    pub cg10_object_store_evidence_required: bool,
    pub cg16_execution_certificate_evidence_required: bool,
    pub cg22_engine_mode_evidence_required: bool,
    pub request_validation_report_only: bool,
    pub cancellation_signal_required: bool,
    pub retry_policy_required: bool,
    pub checkpoint_plan_required: bool,
    pub cleanup_policy_required: bool,
    pub commit_semantics_required: bool,
    pub retry_execution_allowed: bool,
    pub cancellation_execution_allowed: bool,
    pub cleanup_execution_allowed: bool,
    pub checkpoint_write_allowed: bool,
    pub commit_execution_allowed: bool,
    pub ambiguous_commit_resolution_allowed: bool,
    pub idempotent_write_claim_allowed: bool,
    pub exactly_once_claim_allowed: bool,
    pub resumability_claim_allowed: bool,
    pub recovery_claim_allowed: bool,
    pub request_validation_performed: bool,
    pub cancellation_signal_consumed: bool,
    pub retry_execution_performed: bool,
    pub checkpoint_write_performed: bool,
    pub cleanup_execution_performed: bool,
    pub commit_execution_performed: bool,
    pub runtime_execution_performed: bool,
    pub object_store_io: bool,
    pub output_dataset_write: bool,
    pub external_effects_executed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl FaultTolerancePromotionGateReport {
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.fault_tolerance_promotion_gate.v1",
            report_id: "rfc0017.fault_tolerance_promotion_gate",
            entries: vec![
                FaultTolerancePromotionGateEntry::blocked(
                    FaultTolerancePromotionArea::RetryExecution,
                    "retry policy, attempt identity, cleanup proof, idempotency, and no external-effect duplication evidence",
                    true,
                    false,
                ),
                FaultTolerancePromotionGateEntry::blocked(
                    FaultTolerancePromotionArea::CancellationPropagation,
                    "cooperative cancellation checkpoints, cleanup requirements, effect-boundary policy, and certificate evidence",
                    true,
                    false,
                ),
                FaultTolerancePromotionGateEntry::blocked(
                    FaultTolerancePromotionArea::CleanupExecution,
                    "cleanup target manifest, destructive-operation policy, ownership proof, and post-cleanup audit evidence",
                    true,
                    true,
                ),
                FaultTolerancePromotionGateEntry::blocked(
                    FaultTolerancePromotionArea::AmbiguousCommitResolution,
                    "commit record, manifest pointer, backend atomicity, recovery decision, and audit evidence",
                    true,
                    true,
                ),
                FaultTolerancePromotionGateEntry::blocked(
                    FaultTolerancePromotionArea::IdempotencyKeying,
                    "stable run/task/write keys, duplicate detection, commit identity, and external-effect idempotency evidence",
                    true,
                    true,
                ),
                FaultTolerancePromotionGateEntry::blocked(
                    FaultTolerancePromotionArea::RecoveryExecution,
                    "CG-4, CG-8, CG-10, CG-16, CG-22, source/sink, certificate, and benchmark evidence",
                    true,
                    true,
                ),
            ],
            side_effect_boundaries_certified: false,
            commit_semantics_certified: false,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            cg4_output_commit_evidence_required: true,
            cg8_write_recovery_evidence_required: true,
            cg10_object_store_evidence_required: true,
            cg16_execution_certificate_evidence_required: true,
            cg22_engine_mode_evidence_required: true,
            request_validation_report_only: true,
            cancellation_signal_required: true,
            retry_policy_required: true,
            checkpoint_plan_required: true,
            cleanup_policy_required: true,
            commit_semantics_required: true,
            retry_execution_allowed: false,
            cancellation_execution_allowed: false,
            cleanup_execution_allowed: false,
            checkpoint_write_allowed: false,
            commit_execution_allowed: false,
            ambiguous_commit_resolution_allowed: false,
            idempotent_write_claim_allowed: false,
            exactly_once_claim_allowed: false,
            resumability_claim_allowed: false,
            recovery_claim_allowed: false,
            request_validation_performed: false,
            cancellation_signal_consumed: false,
            retry_execution_performed: false,
            checkpoint_write_performed: false,
            cleanup_execution_performed: false,
            commit_execution_performed: false,
            runtime_execution_performed: false,
            object_store_io: false,
            output_dataset_write: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }
    pub fn promotion_area_count(&self) -> usize {
        self.entries.len()
    }
    pub fn blocked_area_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status == FaultTolerancePromotionStatus::BlockedUntilCertified)
            .count()
    }
    pub fn execution_ready_area_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.execution_allowed || entry.status.allows_execution())
            .count()
    }
    pub fn area_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.area.as_str())
            .collect()
    }
    pub fn execution_gate_order(&self) -> Vec<&'static str> {
        vec![
            "request_validation",
            "cancellation_signal",
            "retry_allowed",
            "checkpoint_write",
            "cleanup_execution",
            "commit_execution",
        ]
    }
    pub fn execution_gate_blocker_order(&self) -> Vec<&'static str> {
        vec![
            "request_validation_policy",
            "cancellation_signal_policy",
            "retry_policy",
            "idempotency_key_contract",
            "checkpoint_plan",
            "cleanup_policy",
            "commit_semantics",
            "side_effect_boundary",
            "execution_certificate",
            "native_io_certificate",
            "no_fallback_evidence",
        ]
    }
    pub fn execution_gate_blocker_count(&self) -> usize {
        self.execution_gate_blocker_order().len()
    }
    pub fn execution_promotions_blocked(&self) -> bool {
        !self.retry_execution_allowed
            && !self.cancellation_execution_allowed
            && !self.cleanup_execution_allowed
            && !self.checkpoint_write_allowed
            && !self.commit_execution_allowed
            && !self.ambiguous_commit_resolution_allowed
            && !self.idempotent_write_claim_allowed
            && self.execution_ready_area_count() == 0
    }
    pub fn exactly_once_resumability_recovery_claims_blocked(&self) -> bool {
        !self.exactly_once_claim_allowed
            && !self.resumability_claim_allowed
            && !self.recovery_claim_allowed
    }
    pub fn side_effect_free(&self) -> bool {
        self.execution_promotions_blocked()
            && self.exactly_once_resumability_recovery_claims_blocked()
            && !self.request_validation_performed
            && !self.cancellation_signal_consumed
            && !self.retry_execution_performed
            && !self.checkpoint_write_performed
            && !self.cleanup_execution_performed
            && !self.commit_execution_performed
            && !self.runtime_execution_performed
            && !self.object_store_io
            && !self.output_dataset_write
            && !self.external_effects_executed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free() || has_error_diagnostics(&self.diagnostics)
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "fault tolerance promotion gate");
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "execution promotions blocked: {}",
            self.execution_promotions_blocked()
        );
        let _ = writeln!(
            out,
            "exactly-once/resumability/recovery claims blocked: {}",
            self.exactly_once_resumability_recovery_claims_blocked()
        );
        let _ = writeln!(
            out,
            "side-effect boundaries certified: {}",
            self.side_effect_boundaries_certified
        );
        let _ = writeln!(
            out,
            "commit semantics certified: {}",
            self.commit_semantics_certified
        );
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(out, "promotion areas:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] execution_allowed={}",
                entry.area.as_str(),
                entry.status.as_str(),
                entry.execution_allowed
            );
        }
        out
    }
}

pub fn plan_fault_tolerance_promotion_gate() -> FaultTolerancePromotionGateReport {
    FaultTolerancePromotionGateReport::planning_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitExecutionPromotionSurface {
    LocalCommittedManifestCopy,
    LocalCommittedManifestRollbackCleanup,
    LocalCommittedManifestRecoveryReplay,
    GeneralizedManifestSerialization,
    GeneralizedLocalSinkCommit,
    ObjectStoreCommit,
    TableCatalogCommit,
    LakehouseTransactionCommit,
    NativeSourceSinkCommit,
    FoundryDatasetTransactionCommit,
    UpstreamVortexWriteApiExecution,
    LiveHybridCheckpointCommit,
    OutputPayloadFidelityClaim,
}
impl CommitExecutionPromotionSurface {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalCommittedManifestCopy => "local_committed_manifest_copy",
            Self::LocalCommittedManifestRollbackCleanup => {
                "local_committed_manifest_rollback_cleanup"
            }
            Self::LocalCommittedManifestRecoveryReplay => {
                "local_committed_manifest_recovery_replay"
            }
            Self::GeneralizedManifestSerialization => "generalized_manifest_serialization",
            Self::GeneralizedLocalSinkCommit => "generalized_local_sink_commit",
            Self::ObjectStoreCommit => "object_store_commit",
            Self::TableCatalogCommit => "table_catalog_commit",
            Self::LakehouseTransactionCommit => "lakehouse_transaction_commit",
            Self::NativeSourceSinkCommit => "native_source_sink_commit",
            Self::FoundryDatasetTransactionCommit => "foundry_dataset_transaction_commit",
            Self::UpstreamVortexWriteApiExecution => "upstream_vortex_write_api_execution",
            Self::LiveHybridCheckpointCommit => "live_hybrid_checkpoint_commit",
            Self::OutputPayloadFidelityClaim => "output_payload_fidelity_claim",
        }
    }

    const fn diagnostic_code(self) -> DiagnosticCode {
        match self {
            Self::ObjectStoreCommit => DiagnosticCode::ObjectStoreUnsupported,
            Self::TableCatalogCommit | Self::LakehouseTransactionCommit => {
                DiagnosticCode::CommitNotAtomic
            }
            Self::OutputPayloadFidelityClaim => DiagnosticCode::MetadataLoss,
            _ => DiagnosticCode::NotImplemented,
        }
    }

    const fn diagnostic_category(self) -> DiagnosticCategory {
        match self {
            Self::ObjectStoreCommit => DiagnosticCategory::ObjectStore,
            Self::UpstreamVortexWriteApiExecution => DiagnosticCategory::VortexIo,
            Self::OutputPayloadFidelityClaim => DiagnosticCategory::MetadataLoss,
            _ => DiagnosticCategory::UnsupportedFeature,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitExecutionPromotionStatus {
    ExistingNarrowLocalPath,
    BlockedUntilCertified,
}
impl CommitExecutionPromotionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingNarrowLocalPath => "existing_narrow_local_path",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionPromotionGateEntry {
    pub surface: CommitExecutionPromotionSurface,
    pub status: CommitExecutionPromotionStatus,
    pub required_evidence: &'static str,
    pub existing_limited_local_path: bool,
    pub requires_execution_certificate: bool,
    pub requires_native_io_certificate: bool,
    pub requires_output_manifest: bool,
    pub requires_sink_requirement_report: bool,
    pub requires_materialization_fidelity_report: bool,
    pub requires_idempotency_key: bool,
    pub requires_recovery_rollback_proof: bool,
    pub requires_ambiguous_commit_diagnostics: bool,
    pub requires_backend_atomicity_policy: bool,
    pub requires_table_catalog_transaction_policy: bool,
    pub requires_foundry_transaction_context: bool,
    pub broader_execution_allowed: bool,
}
impl CommitExecutionPromotionGateEntry {
    const fn existing_limited(
        surface: CommitExecutionPromotionSurface,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: CommitExecutionPromotionStatus::ExistingNarrowLocalPath,
            required_evidence,
            existing_limited_local_path: true,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            requires_output_manifest: true,
            requires_sink_requirement_report: true,
            requires_materialization_fidelity_report: true,
            requires_idempotency_key: true,
            requires_recovery_rollback_proof: true,
            requires_ambiguous_commit_diagnostics: true,
            requires_backend_atomicity_policy: false,
            requires_table_catalog_transaction_policy: false,
            requires_foundry_transaction_context: false,
            broader_execution_allowed: false,
        }
    }

    const fn blocked(
        surface: CommitExecutionPromotionSurface,
        required_evidence: &'static str,
        requires_backend_atomicity_policy: bool,
        requires_table_catalog_transaction_policy: bool,
        requires_foundry_transaction_context: bool,
    ) -> Self {
        Self {
            surface,
            status: CommitExecutionPromotionStatus::BlockedUntilCertified,
            required_evidence,
            existing_limited_local_path: false,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            requires_output_manifest: true,
            requires_sink_requirement_report: true,
            requires_materialization_fidelity_report: true,
            requires_idempotency_key: true,
            requires_recovery_rollback_proof: true,
            requires_ambiguous_commit_diagnostics: true,
            requires_backend_atomicity_policy,
            requires_table_catalog_transaction_policy,
            requires_foundry_transaction_context,
            broader_execution_allowed: false,
        }
    }

    fn blocked_diagnostic(&self) -> Option<Diagnostic> {
        if self.status != CommitExecutionPromotionStatus::BlockedUntilCertified {
            return None;
        }
        Some(Diagnostic::new(
            self.surface.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.surface.diagnostic_category(),
            format!(
                "commit promotion surface {} is blocked until certified",
                self.surface.as_str()
            ),
            Some(self.surface.as_str().to_string()),
            Some(format!(
                "{} requires {} before runtime promotion.",
                self.surface.as_str(),
                self.required_evidence
            )),
            Some(
                "Keep the path report-only and attach the required evidence before enabling writes or claims."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

fn commit_execution_promotion_entries() -> Vec<CommitExecutionPromotionGateEntry> {
    vec![
        CommitExecutionPromotionGateEntry::existing_limited(
            CommitExecutionPromotionSurface::LocalCommittedManifestCopy,
            "existing feature-gated local finalized-manifest to committed-manifest copy evidence plus output manifest, payload, idempotency, rollback, certificate, and no-fallback linkage before any broader promotion",
        ),
        CommitExecutionPromotionGateEntry::existing_limited(
            CommitExecutionPromotionSurface::LocalCommittedManifestRollbackCleanup,
            "existing feature-gated local committed-manifest rollback cleanup evidence plus ownership, cleanup audit, idempotency, certificate, and no-fallback linkage before any broader promotion",
        ),
        CommitExecutionPromotionGateEntry::existing_limited(
            CommitExecutionPromotionSurface::LocalCommittedManifestRecoveryReplay,
            "existing local committed-manifest recovery replay smoke with sidecar digest, correctness, idempotency, certificate, and no-fallback linkage before any broader recovery promotion",
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::GeneralizedManifestSerialization,
            "generalized manifest schema, serialization roundtrip, compatibility/fidelity proof, idempotency identity, execution certificate, Native I/O certificate, and no-fallback evidence",
            false,
            false,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::GeneralizedLocalSinkCommit,
            "generalized sink requirement report, output manifest, materialization/fidelity report, idempotency key, rollback proof, execution certificate, and Native I/O certificate",
            false,
            false,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::ObjectStoreCommit,
            "object-store staging prefix, manifest pointer update, commit record, idempotency key, cleanup/recovery plan, backend atomicity policy, request evidence, and Native I/O certificate",
            true,
            false,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::TableCatalogCommit,
            "catalog/table transaction policy, snapshot identity, schema/delete/tombstone compatibility, commit/recovery evidence, and fidelity/materialization reports",
            true,
            true,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::LakehouseTransactionCommit,
            "lakehouse table-format transaction policy, manifest compatibility, catalog atomicity, delete/CDC semantics, recovery evidence, execution certificate, and Native I/O certificate",
            true,
            true,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::NativeSourceSinkCommit,
            "source/sink capability report, Native I/O certificate pair, residual/materialization boundary evidence, output manifest, commit identity, and recovery proof",
            true,
            false,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::FoundryDatasetTransactionCommit,
            "Foundry dataset transaction, branch/build context, staging/materialization evidence, certificate output policy, governance markings, and no external-compute fallback proof",
            true,
            true,
            true,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::UpstreamVortexWriteApiExecution,
            "approved upstream Vortex writer API contract, version pin, write-path certificate, output-payload fidelity report, rollback proof, and no-fallback evidence",
            false,
            false,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::LiveHybridCheckpointCommit,
            "engine-mode evidence, checkpoint identity, hot/warm/cold state manifest, freshness certificate, rollback/replay proof, and side-effect policy",
            true,
            true,
            false,
        ),
        CommitExecutionPromotionGateEntry::blocked(
            CommitExecutionPromotionSurface::OutputPayloadFidelityClaim,
            "payload schema/fidelity matrix, materialization/decode boundary proof, replay proof, compatibility loss diagnostics, execution certificate, Native I/O certificate, and workload-scoped evidence",
            false,
            false,
            false,
        ),
    ]
}

fn commit_execution_existing_report_refs() -> Vec<&'static str> {
    vec![
        "shardloom.vortex_staged_output.v1",
        "shardloom.vortex_manifest_finalization.v1",
        "shardloom.vortex_commit_marker.v1",
        "cg4.commit_execution_promotion_gate",
        "cg10.object_store_request_planner.aggregate",
        "shardloom.object_store_commit_protocol.v1",
        "shardloom.table_maintenance_execution_matrix.v1",
        "gar-runtime-impl-6d.local_table_commit_recovery_smoke",
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionPromotionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub entries: Vec<CommitExecutionPromotionGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub existing_local_commit_execution_present: bool,
    pub existing_local_rollback_execution_present: bool,
    pub existing_local_recovery_execution_present: bool,
    pub broader_commit_execution_allowed: bool,
    pub generalized_manifest_serialization_allowed: bool,
    pub generalized_local_sink_commit_allowed: bool,
    pub object_store_commit_execution_allowed: bool,
    pub table_catalog_commit_execution_allowed: bool,
    pub lakehouse_transaction_commit_execution_allowed: bool,
    pub native_source_sink_commit_execution_allowed: bool,
    pub foundry_dataset_commit_execution_allowed: bool,
    pub upstream_vortex_write_api_execution_allowed: bool,
    pub live_hybrid_checkpoint_commit_execution_allowed: bool,
    pub output_payload_fidelity_claim_allowed: bool,
    pub output_manifest_required: bool,
    pub sink_requirement_report_required: bool,
    pub materialization_fidelity_report_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub idempotency_key_required: bool,
    pub rollback_recovery_proof_required: bool,
    pub ambiguous_commit_diagnostics_required: bool,
    pub object_store_atomicity_policy_required: bool,
    pub table_catalog_transaction_policy_required: bool,
    pub credential_effect_policy_required: bool,
    pub upstream_vortex_write_api_policy_required: bool,
    pub deterministic_unsupported_diagnostics_ready: bool,
    pub runtime_execution_performed: bool,
    pub write_io: bool,
    pub object_store_io: bool,
    pub catalog_io: bool,
    pub manifest_write_io: bool,
    pub upstream_vortex_write_api_invoked: bool,
    pub external_effects_executed: bool,
    pub exactly_once_claim_allowed: bool,
    pub atomic_commit_claim_allowed: bool,
    pub recovery_claim_allowed: bool,
    pub lakehouse_claim_allowed: bool,
    pub production_output_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl CommitExecutionPromotionGateReport {
    pub fn planning_default() -> Self {
        let entries = commit_execution_promotion_entries();
        let diagnostics = entries
            .iter()
            .filter_map(CommitExecutionPromotionGateEntry::blocked_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.commit_execution_promotion_gate.v1",
            report_id: "cg4.commit_execution_promotion_gate",
            gar_id: "GAR-0028-A",
            support_status: "report_only_with_blocked_runtime_paths",
            claim_gate_status: "not_claim_grade",
            entries,
            existing_report_refs: commit_execution_existing_report_refs(),
            existing_local_commit_execution_present: true,
            existing_local_rollback_execution_present: true,
            existing_local_recovery_execution_present: true,
            broader_commit_execution_allowed: false,
            generalized_manifest_serialization_allowed: false,
            generalized_local_sink_commit_allowed: false,
            object_store_commit_execution_allowed: false,
            table_catalog_commit_execution_allowed: false,
            lakehouse_transaction_commit_execution_allowed: false,
            native_source_sink_commit_execution_allowed: false,
            foundry_dataset_commit_execution_allowed: false,
            upstream_vortex_write_api_execution_allowed: false,
            live_hybrid_checkpoint_commit_execution_allowed: false,
            output_payload_fidelity_claim_allowed: false,
            output_manifest_required: true,
            sink_requirement_report_required: true,
            materialization_fidelity_report_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            idempotency_key_required: true,
            rollback_recovery_proof_required: true,
            ambiguous_commit_diagnostics_required: true,
            object_store_atomicity_policy_required: true,
            table_catalog_transaction_policy_required: true,
            credential_effect_policy_required: true,
            upstream_vortex_write_api_policy_required: true,
            deterministic_unsupported_diagnostics_ready: true,
            runtime_execution_performed: false,
            write_io: false,
            object_store_io: false,
            catalog_io: false,
            manifest_write_io: false,
            upstream_vortex_write_api_invoked: false,
            external_effects_executed: false,
            exactly_once_claim_allowed: false,
            atomic_commit_claim_allowed: false,
            recovery_claim_allowed: false,
            lakehouse_claim_allowed: false,
            production_output_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    pub fn existing_limited_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.existing_limited_local_path)
            .count()
    }

    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status == CommitExecutionPromotionStatus::BlockedUntilCertified)
            .count()
    }

    pub fn broader_execution_ready_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.broader_execution_allowed)
            .count()
    }

    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    pub fn broader_execution_promotions_blocked(&self) -> bool {
        !self.broader_commit_execution_allowed
            && !self.generalized_manifest_serialization_allowed
            && !self.generalized_local_sink_commit_allowed
            && !self.object_store_commit_execution_allowed
            && !self.table_catalog_commit_execution_allowed
            && !self.lakehouse_transaction_commit_execution_allowed
            && !self.native_source_sink_commit_execution_allowed
            && !self.foundry_dataset_commit_execution_allowed
            && !self.upstream_vortex_write_api_execution_allowed
            && !self.live_hybrid_checkpoint_commit_execution_allowed
            && !self.output_payload_fidelity_claim_allowed
            && self.broader_execution_ready_surface_count() == 0
    }

    pub fn commit_claims_blocked(&self) -> bool {
        !self.exactly_once_claim_allowed
            && !self.atomic_commit_claim_allowed
            && !self.recovery_claim_allowed
            && !self.lakehouse_claim_allowed
            && !self.production_output_claim_allowed
            && !self.output_payload_fidelity_claim_allowed
    }

    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|diagnostic| {
                matches!(
                    diagnostic.code,
                    DiagnosticCode::NotImplemented
                        | DiagnosticCode::ObjectStoreUnsupported
                        | DiagnosticCode::CommitNotAtomic
                        | DiagnosticCode::MetadataLoss
                ) && diagnostic.severity == DiagnosticSeverity::Info
                    && !diagnostic.fallback.attempted
                    && !diagnostic.fallback.allowed
            })
            .count()
    }

    pub fn diagnostic_feature_order(&self) -> Vec<&str> {
        self.diagnostics
            .iter()
            .filter_map(|diagnostic| diagnostic.feature.as_deref())
            .collect()
    }

    pub fn unsupported_diagnostics_propagated(&self) -> bool {
        self.deterministic_unsupported_diagnostics_ready
            && self.unsupported_diagnostic_count() == self.blocked_surface_count()
            && self.diagnostic_feature_order()
                == self
                    .entries
                    .iter()
                    .filter(|entry| {
                        entry.status == CommitExecutionPromotionStatus::BlockedUntilCertified
                    })
                    .map(|entry| entry.surface.as_str())
                    .collect::<Vec<_>>()
    }

    pub fn side_effect_free(&self) -> bool {
        self.broader_execution_promotions_blocked()
            && self.commit_claims_blocked()
            && !self.runtime_execution_performed
            && !self.write_io
            && !self.object_store_io
            && !self.catalog_io
            && !self.manifest_write_io
            && !self.upstream_vortex_write_api_invoked
            && !self.external_effects_executed
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    pub fn has_errors(&self) -> bool {
        !self.side_effect_free() || has_error_diagnostics(&self.diagnostics)
    }

    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "commit execution promotion gate");
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "gar_id: {}", self.gar_id);
        let _ = writeln!(out, "support_status: {}", self.support_status);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(
            out,
            "unsupported diagnostics propagated: {}",
            self.unsupported_diagnostics_propagated()
        );
        let _ = writeln!(
            out,
            "broader commit execution allowed: {}",
            self.broader_commit_execution_allowed
        );
        let _ = writeln!(
            out,
            "existing local commit execution present: {}",
            self.existing_local_commit_execution_present
        );
        let _ = writeln!(
            out,
            "existing local rollback execution present: {}",
            self.existing_local_rollback_execution_present
        );
        let _ = writeln!(
            out,
            "existing local recovery execution present: {}",
            self.existing_local_recovery_execution_present
        );
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(out, "commit surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] broader_execution_allowed={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.broader_execution_allowed
            );
        }
        out
    }
}

pub fn plan_commit_execution_promotion_gate() -> CommitExecutionPromotionGateReport {
    CommitExecutionPromotionGateReport::planning_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRecoveryIntegrationStatus {
    Planned,
    CleanupNotRequired,
    CleanupRequired,
    RetryAllowedAfterCleanup,
    RetryBlocked,
    CancellationPlanned,
    BlockedByExternalEffect,
    BlockedByUnknownArtifact,
    Unsupported,
}
impl ShardLoomRecoveryIntegrationStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::CleanupNotRequired => "cleanup_not_required",
            Self::CleanupRequired => "cleanup_required",
            Self::RetryAllowedAfterCleanup => "retry_allowed_after_cleanup",
            Self::RetryBlocked => "retry_blocked",
            Self::CancellationPlanned => "cancellation_planned",
            Self::BlockedByExternalEffect => "blocked_by_external_effect",
            Self::BlockedByUnknownArtifact => "blocked_by_unknown_artifact",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::RetryBlocked
                | Self::BlockedByExternalEffect
                | Self::BlockedByUnknownArtifact
                | Self::Unsupported
        )
    }
    pub const fn requires_cleanup(&self) -> bool {
        matches!(
            self,
            Self::CleanupRequired | Self::RetryAllowedAfterCleanup | Self::BlockedByUnknownArtifact
        )
    }
    pub const fn allows_retry(&self) -> bool {
        matches!(self, Self::Planned | Self::RetryAllowedAfterCleanup)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRecoveryIntegrationMode {
    ReportOnly,
    CleanupPlanning,
    RetryPlanning,
    CancellationPlanning,
    Unsupported,
}
impl ShardLoomRecoveryIntegrationMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::CleanupPlanning => "cleanup_planning",
            Self::RetryPlanning => "retry_planning",
            Self::CancellationPlanning => "cancellation_planning",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn executes_cleanup(&self) -> bool {
        false
    }
    pub const fn executes_retry(&self) -> bool {
        false
    }
    pub const fn executes_cancellation(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryArtifactKind {
    SyntheticSpillPayload,
    SpillWorkspace,
    SpillMarker,
    TemporaryOutput,
    PartialOutput,
    Unknown,
}
impl RecoveryArtifactKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SyntheticSpillPayload => "synthetic_spill_payload",
            Self::SpillWorkspace => "spill_workspace",
            Self::SpillMarker => "spill_marker",
            Self::TemporaryOutput => "temporary_output",
            Self::PartialOutput => "partial_output",
            Self::Unknown => "unknown",
        }
    }
    pub const fn requires_cleanup(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryArtifactRef {
    pub kind: RecoveryArtifactKind,
    pub artifact_id: String,
    pub location_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl RecoveryArtifactRef {
    pub fn synthetic_spill_payload(payload_ref: &crate::SpillPayloadFsRef) -> Self {
        Self {
            kind: RecoveryArtifactKind::SyntheticSpillPayload,
            artifact_id: payload_ref.payload_ref().payload_id().as_str().to_string(),
            location_summary: Some(payload_ref.path_string()),
            diagnostics: vec![],
        }
    }
    pub fn spill_workspace(workspace_id: impl Into<String>, location: impl Into<String>) -> Self {
        let mut out = Self {
            kind: RecoveryArtifactKind::SpillWorkspace,
            artifact_id: workspace_id.into().trim().to_string(),
            location_summary: Some(location.into()),
            diagnostics: vec![],
        };
        if out.artifact_id.is_empty() {
            out.add_diagnostic(Diagnostic::invalid_input(
                "recovery_artifact_ref",
                "workspace id must not be empty",
                "set a non-empty workspace id",
            ));
        }
        out
    }
    pub fn unknown(artifact_id: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut out = Self {
            kind: RecoveryArtifactKind::Unknown,
            artifact_id: artifact_id.into().trim().to_string(),
            location_summary: None,
            diagnostics: vec![],
        };
        if out.artifact_id.is_empty() {
            out.artifact_id = "unknown-artifact".to_string();
            out.add_diagnostic(Diagnostic::invalid_input(
                "recovery_artifact_ref",
                "artifact id must not be empty",
                "set a non-empty artifact id",
            ));
        }
        out.add_diagnostic(unsupported_diagnostic("unknown_recovery_artifact", reason));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.kind == RecoveryArtifactKind::Unknown || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "kind={} artifact_id={}",
            self.kind.as_str(),
            self.artifact_id
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomRecoveryIntegrationRequest {
    pub attempt_id: Option<AttemptId>,
    pub bounded_spill_report_summary: Option<String>,
    pub artifacts: Vec<RecoveryArtifactRef>,
    pub retry_requested: bool,
    pub cancellation_requested: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl Default for ShardLoomRecoveryIntegrationRequest {
    fn default() -> Self {
        Self::new()
    }
}
impl ShardLoomRecoveryIntegrationRequest {
    pub fn new() -> Self {
        Self {
            attempt_id: None,
            bounded_spill_report_summary: None,
            artifacts: vec![],
            retry_requested: false,
            cancellation_requested: false,
            diagnostics: vec![],
        }
    }
    pub fn for_attempt(attempt_id: AttemptId) -> Self {
        let mut out = Self::new();
        out.attempt_id = Some(attempt_id);
        out
    }
    pub fn add_artifact(&mut self, artifact: RecoveryArtifactRef) {
        self.artifacts.push(artifact);
    }
    pub fn retry_requested(mut self, value: bool) -> Self {
        self.retry_requested = value;
        self
    }
    pub fn cancellation_requested(mut self, value: bool) -> Self {
        self.cancellation_requested = value;
        self
    }
    pub fn with_bounded_spill_report_summary(mut self, value: impl Into<String>) -> Self {
        self.bounded_spill_report_summary = Some(value.into());
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_option(&self, option: RecoveryIntegrationOption) -> bool {
        match option {
            RecoveryIntegrationOption::RetryRequested => self.retry_requested,
            RecoveryIntegrationOption::CancellationRequested => self.cancellation_requested,
        }
    }
    pub fn has_errors(&self) -> bool {
        self.artifacts.iter().any(RecoveryArtifactRef::has_errors)
            || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "attempt_present={} artifacts={} retry_requested={} cancellation_requested={}",
            self.attempt_id.is_some(),
            self.artifacts.len(),
            self.retry_requested,
            self.cancellation_requested
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryIntegrationOption {
    RetryRequested,
    CancellationRequested,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomRecoveryIntegrationReport {
    pub status: ShardLoomRecoveryIntegrationStatus,
    pub mode: ShardLoomRecoveryIntegrationMode,
    pub request: ShardLoomRecoveryIntegrationRequest,
    pub cleanup_requirements: Vec<CleanupRequirement>,
    pub retry_decision: Option<RetryDecision>,
    pub artifact_count: usize,
    pub cleanup_required_count: usize,
    pub unknown_artifact_count: usize,
    pub cleanup_execution: RecoveryExecutionState,
    pub retry_execution: RecoveryExecutionState,
    pub cancellation_execution: RecoveryExecutionState,
    pub external_effects_execution: RecoveryExecutionState,
    pub object_store_io: RecoveryExecutionState,
    pub output_dataset_write: RecoveryExecutionState,
    pub fallback_execution: FallbackExecutionState,
    pub diagnostics: Vec<Diagnostic>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryExecutionState {
    NotPerformed,
}
impl RecoveryExecutionState {
    pub const fn executed(self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackExecutionState {
    Disabled,
}
impl FallbackExecutionState {
    pub const fn allowed(self) -> bool {
        false
    }
}
impl ShardLoomRecoveryIntegrationReport {
    /// # Errors
    /// Returns an error if creating cleanup requirements fails.
    pub fn from_request(request: ShardLoomRecoveryIntegrationRequest) -> Result<Self> {
        let mut cleanup_requirements = Vec::new();
        let mut unknown_artifact_count = 0usize;
        for artifact in &request.artifacts {
            match artifact.kind {
                RecoveryArtifactKind::Unknown => unknown_artifact_count += 1,
                RecoveryArtifactKind::SyntheticSpillPayload | RecoveryArtifactKind::SpillMarker => {
                    cleanup_requirements.push(CleanupRequirement::required(
                        CleanupTargetKind::SpillFile,
                        artifact.artifact_id.clone(),
                    )?);
                }
                RecoveryArtifactKind::SpillWorkspace
                | RecoveryArtifactKind::TemporaryOutput
                | RecoveryArtifactKind::PartialOutput => {
                    cleanup_requirements.push(CleanupRequirement::required(
                        CleanupTargetKind::TemporaryOutput,
                        artifact.artifact_id.clone(),
                    )?);
                }
            }
        }
        let status;
        let mut mode = ShardLoomRecoveryIntegrationMode::ReportOnly;
        let mut retry_decision = None;
        if request.cancellation_requested {
            mode = ShardLoomRecoveryIntegrationMode::CancellationPlanning;
            status = ShardLoomRecoveryIntegrationStatus::CancellationPlanned;
        } else if unknown_artifact_count > 0 {
            status = ShardLoomRecoveryIntegrationStatus::BlockedByUnknownArtifact;
        } else if request.retry_requested && !cleanup_requirements.is_empty() {
            mode = ShardLoomRecoveryIntegrationMode::RetryPlanning;
            status = ShardLoomRecoveryIntegrationStatus::RetryAllowedAfterCleanup;
            retry_decision = Some(RetryDecision::retry_after_cleanup(
                "cleanup required before retry",
            ));
        } else if request.retry_requested {
            mode = ShardLoomRecoveryIntegrationMode::RetryPlanning;
            status = ShardLoomRecoveryIntegrationStatus::Planned;
            retry_decision = Some(RetryDecision::retry_now("no cleanup requirements found"));
        } else if !cleanup_requirements.is_empty() {
            mode = ShardLoomRecoveryIntegrationMode::CleanupPlanning;
            status = ShardLoomRecoveryIntegrationStatus::CleanupRequired;
        } else {
            status = ShardLoomRecoveryIntegrationStatus::CleanupNotRequired;
        }
        Ok(Self {
            status,
            mode,
            artifact_count: request.artifacts.len(),
            cleanup_required_count: cleanup_requirements.len(),
            unknown_artifact_count,
            cleanup_requirements,
            retry_decision,
            request,
            cleanup_execution: RecoveryExecutionState::NotPerformed,
            retry_execution: RecoveryExecutionState::NotPerformed,
            cancellation_execution: RecoveryExecutionState::NotPerformed,
            external_effects_execution: RecoveryExecutionState::NotPerformed,
            object_store_io: RecoveryExecutionState::NotPerformed,
            output_dataset_write: RecoveryExecutionState::NotPerformed,
            fallback_execution: FallbackExecutionState::Disabled,
            diagnostics: vec![],
        })
    }
    pub fn unsupported(
        request: ShardLoomRecoveryIntegrationRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: ShardLoomRecoveryIntegrationStatus::Unsupported,
            mode: ShardLoomRecoveryIntegrationMode::Unsupported,
            artifact_count: request.artifacts.len(),
            cleanup_required_count: 0,
            unknown_artifact_count: 0,
            cleanup_requirements: vec![],
            retry_decision: None,
            request,
            cleanup_execution: RecoveryExecutionState::NotPerformed,
            retry_execution: RecoveryExecutionState::NotPerformed,
            cancellation_execution: RecoveryExecutionState::NotPerformed,
            external_effects_execution: RecoveryExecutionState::NotPerformed,
            object_store_io: RecoveryExecutionState::NotPerformed,
            output_dataset_write: RecoveryExecutionState::NotPerformed,
            fallback_execution: FallbackExecutionState::Disabled,
            diagnostics: vec![],
        };
        out.add_diagnostic(unsupported_diagnostic(feature, reason));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self
                .cleanup_requirements
                .iter()
                .any(CleanupRequirement::has_errors)
            || self
                .retry_decision
                .as_ref()
                .is_some_and(RetryDecision::has_errors)
            || has_error_diagnostics(&self.diagnostics)
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.cleanup_execution.executed()
            && !self.retry_execution.executed()
            && !self.cancellation_execution.executed()
            && !self.external_effects_execution.executed()
            && !self.object_store_io.executed()
            && !self.output_dataset_write.executed()
            && !self.fallback_execution.allowed()
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "recovery integration status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        if let Some(attempt_id) = &self.request.attempt_id {
            let _ = writeln!(out, "attempt id: {}", attempt_id.as_str());
        }
        let _ = writeln!(out, "artifact count: {}", self.artifact_count);
        let _ = writeln!(
            out,
            "cleanup required count: {}",
            self.cleanup_required_count
        );
        let _ = writeln!(
            out,
            "unknown artifact count: {}",
            self.unknown_artifact_count
        );
        if let Some(retry_decision) = &self.retry_decision {
            let _ = writeln!(out, "retry decision: {}", retry_decision.kind.as_str());
        }
        let fallback_allowed = self.fallback_execution.allowed();
        let _ = write!(
            out,
            "cleanup executed: {}\nretry executed: {}\ncancellation executed: {}\nexternal effects executed: {}\nobject-store IO: {}\noutput dataset write: {}\nfallback execution allowed: {}",
            self.cleanup_execution.executed(),
            self.retry_execution.executed(),
            self.cancellation_execution.executed(),
            self.external_effects_execution.executed(),
            self.object_store_io.executed(),
            self.output_dataset_write.executed(),
            fallback_allowed
        );
        if !fallback_allowed {
            let _ = write!(out, "\nfallback execution: disabled");
        }
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {}", d.message);
            }
        }
        out
    }
}

/// # Errors
/// Returns an error when `ShardLoomRecoveryIntegrationReport` planning fails.
pub fn plan_recovery_integration(
    request: ShardLoomRecoveryIntegrationRequest,
) -> Result<ShardLoomRecoveryIntegrationReport> {
    ShardLoomRecoveryIntegrationReport::from_request(request)
}

pub fn recovery_integration_is_side_effect_free(
    report: &ShardLoomRecoveryIntegrationReport,
) -> bool {
    report.is_side_effect_free()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCleanupExecutionEffect {
    CleanupExecuted,
    RetryExecuted,
    CancellationExecuted,
    ExternalEffectExecuted,
    ObjectStoreIo,
    OutputDatasetWrite,
    FallbackExecution,
}
impl ShardLoomCleanupExecutionEffect {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CleanupExecuted => "cleanup_executed",
            Self::RetryExecuted => "retry_executed",
            Self::CancellationExecuted => "cancellation_executed",
            Self::ExternalEffectExecuted => "external_effect_executed",
            Self::ObjectStoreIo => "object_store_io",
            Self::OutputDatasetWrite => "output_dataset_write",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCleanupExecutionStatus {
    Planned,
    FeatureDisabled,
    CleanupRequired,
    CleanupNotRequired,
    CleanupWouldExecute,
    CleanupCompleted,
    BlockedByUnknownArtifact,
    BlockedByUnsupportedArtifact,
    BlockedByMissingArtifact,
    BlockedByPolicy,
    Unsupported,
}
impl ShardLoomCleanupExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::FeatureDisabled => "feature_disabled",
            Self::CleanupRequired => "cleanup_required",
            Self::CleanupNotRequired => "cleanup_not_required",
            Self::CleanupWouldExecute => "cleanup_would_execute",
            Self::CleanupCompleted => "cleanup_completed",
            Self::BlockedByUnknownArtifact => "blocked_by_unknown_artifact",
            Self::BlockedByUnsupportedArtifact => "blocked_by_unsupported_artifact",
            Self::BlockedByMissingArtifact => "blocked_by_missing_artifact",
            Self::BlockedByPolicy => "blocked_by_policy",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByUnknownArtifact
                | Self::BlockedByUnsupportedArtifact
                | Self::BlockedByMissingArtifact
                | Self::BlockedByPolicy
                | Self::Unsupported
        )
    }
    pub const fn cleanup_would_execute(&self) -> bool {
        matches!(self, Self::CleanupWouldExecute)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCleanupExecutionMode {
    ReportOnly,
    CleanupPlanOnly,
    SyntheticPayloadCleanup,
    Unsupported,
}
impl ShardLoomCleanupExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::CleanupPlanOnly => "cleanup_plan_only",
            Self::SyntheticPayloadCleanup => "synthetic_payload_cleanup",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn executes_cleanup(&self) -> bool {
        matches!(self, Self::SyntheticPayloadCleanup)
    }
    pub const fn executes_retry(&self) -> bool {
        false
    }
    pub const fn executes_cancellation(&self) -> bool {
        false
    }
    pub const fn touches_object_store(&self) -> bool {
        false
    }
    pub const fn writes_output_dataset(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupExecutionOption {
    AllowSyntheticPayloadCleanup,
}
impl CleanupExecutionOption {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AllowSyntheticPayloadCleanup => "allow_synthetic_payload_cleanup",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomCleanupExecutionRequest {
    pub artifact: RecoveryArtifactRef,
    pub synthetic_payload_ref: Option<crate::SpillPayloadFsRef>,
    pub options: Vec<CleanupExecutionOption>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomCleanupExecutionRequest {
    pub fn new(artifact: RecoveryArtifactRef) -> Self {
        Self {
            artifact,
            synthetic_payload_ref: None,
            options: vec![],
            diagnostics: vec![],
        }
    }
    pub fn synthetic_payload(
        artifact: RecoveryArtifactRef,
        fs_ref: crate::SpillPayloadFsRef,
    ) -> Self {
        Self::new(artifact).with_synthetic_payload_ref(fs_ref)
    }
    pub fn with_synthetic_payload_ref(mut self, fs_ref: crate::SpillPayloadFsRef) -> Self {
        self.synthetic_payload_ref = Some(fs_ref);
        self
    }
    pub fn allow_synthetic_payload_cleanup(mut self, value: bool) -> Self {
        self.options
            .retain(|option| *option != CleanupExecutionOption::AllowSyntheticPayloadCleanup);
        if value {
            self.options
                .push(CleanupExecutionOption::AllowSyntheticPayloadCleanup);
        }
        self
    }
    pub fn has_option(&self, option: CleanupExecutionOption) -> bool {
        self.options.contains(&option)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.artifact.has_errors() || has_error_diagnostics(&self.diagnostics)
    }
    pub fn summary(&self) -> String {
        format!(
            "{} options={}",
            self.artifact.summary(),
            self.options
                .iter()
                .map(CleanupExecutionOption::as_str)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomCleanupExecutionReport {
    pub status: ShardLoomCleanupExecutionStatus,
    pub mode: ShardLoomCleanupExecutionMode,
    pub request: ShardLoomCleanupExecutionRequest,
    pub effects_performed: Vec<ShardLoomCleanupExecutionEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomCleanupExecutionReport {
    /// # Errors
    /// Returns an error only if planning metadata cannot be derived from the request.
    pub fn from_request(request: ShardLoomCleanupExecutionRequest) -> Result<Self> {
        if let Some((status, reason)) = Self::validate_request_for_cleanup(&request) {
            return Ok(Self::blocked(request, status, reason));
        }
        Ok(Self::cleanup_would_execute(request))
    }
    fn validate_request_for_cleanup(
        request: &ShardLoomCleanupExecutionRequest,
    ) -> Option<(ShardLoomCleanupExecutionStatus, &'static str)> {
        match request.artifact.kind {
            RecoveryArtifactKind::Unknown => Some((
                ShardLoomCleanupExecutionStatus::BlockedByUnknownArtifact,
                "artifact kind is unknown",
            )),
            RecoveryArtifactKind::SyntheticSpillPayload => {
                if !request.has_option(CleanupExecutionOption::AllowSyntheticPayloadCleanup) {
                    Some((
                        ShardLoomCleanupExecutionStatus::BlockedByPolicy,
                        "synthetic payload cleanup must be explicitly enabled in planning options",
                    ))
                } else if request.synthetic_payload_ref.is_none() {
                    Some((
                        ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact,
                        "synthetic spill payload filesystem reference is required",
                    ))
                } else if request.artifact.artifact_id
                    != request
                        .synthetic_payload_ref
                        .as_ref()
                        .map(|fs_ref| fs_ref.payload_ref().payload_id().as_str().to_string())
                        .unwrap_or_default()
                {
                    Some((
                        ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact,
                        "synthetic spill payload filesystem reference payload id does not match artifact id",
                    ))
                } else {
                    None
                }
            }
            RecoveryArtifactKind::SpillWorkspace
            | RecoveryArtifactKind::SpillMarker
            | RecoveryArtifactKind::TemporaryOutput
            | RecoveryArtifactKind::PartialOutput => Some((
                ShardLoomCleanupExecutionStatus::BlockedByUnsupportedArtifact,
                "cleanup execution for this artifact kind is not implemented in this phase",
            )),
        }
    }
    pub fn planned(request: ShardLoomCleanupExecutionRequest) -> Self {
        Self {
            status: ShardLoomCleanupExecutionStatus::Planned,
            mode: ShardLoomCleanupExecutionMode::ReportOnly,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        }
    }
    pub fn cleanup_not_required(request: ShardLoomCleanupExecutionRequest) -> Self {
        Self {
            status: ShardLoomCleanupExecutionStatus::CleanupNotRequired,
            mode: ShardLoomCleanupExecutionMode::ReportOnly,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        }
    }
    pub fn cleanup_would_execute(request: ShardLoomCleanupExecutionRequest) -> Self {
        Self {
            status: ShardLoomCleanupExecutionStatus::CleanupWouldExecute,
            mode: ShardLoomCleanupExecutionMode::CleanupPlanOnly,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        }
    }
    pub fn cleanup_completed(request: ShardLoomCleanupExecutionRequest) -> Self {
        Self {
            status: ShardLoomCleanupExecutionStatus::CleanupCompleted,
            mode: ShardLoomCleanupExecutionMode::SyntheticPayloadCleanup,
            request,
            effects_performed: vec![ShardLoomCleanupExecutionEffect::CleanupExecuted],
            diagnostics: vec![],
        }
    }
    pub fn blocked(
        request: ShardLoomCleanupExecutionRequest,
        status: ShardLoomCleanupExecutionStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status,
            mode: ShardLoomCleanupExecutionMode::ReportOnly,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        };
        out.add_diagnostic(unsupported_diagnostic("cleanup_execution", reason));
        out
    }
    pub fn unsupported(
        request: ShardLoomCleanupExecutionRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: ShardLoomCleanupExecutionStatus::Unsupported,
            mode: ShardLoomCleanupExecutionMode::Unsupported,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        };
        out.add_diagnostic(unsupported_diagnostic(feature, reason));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || has_error_diagnostics(&self.diagnostics)
    }
    pub fn cleanup_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::CleanupExecuted)
    }
    pub fn retry_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::RetryExecuted)
    }
    pub fn cancellation_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::CancellationExecuted)
    }
    pub fn external_effects_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::ExternalEffectExecuted)
    }
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::ObjectStoreIo)
    }
    pub fn output_dataset_write(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::OutputDatasetWrite)
    }
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCleanupExecutionEffect::FallbackExecution)
    }
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "cleanup execution status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "artifact: {}", self.request.artifact.summary());
        let _ = writeln!(
            out,
            "synthetic payload ref present: {}",
            self.request.synthetic_payload_ref.is_some()
        );
        if let Some(fs_ref) = &self.request.synthetic_payload_ref {
            let _ = writeln!(out, "cleanup target summary: {}", fs_ref.summary());
        }
        let fallback_allowed = self.fallback_execution_allowed();
        let _ = write!(
            out,
            "cleanup executed: {}\nretry executed: {}\ncancellation executed: {}\nexternal effects executed: {}\nobject-store IO: {}\noutput dataset write: {}\nfallback execution allowed: {}",
            self.cleanup_executed(),
            self.retry_executed(),
            self.cancellation_executed(),
            self.external_effects_executed(),
            self.object_store_io(),
            self.output_dataset_write(),
            fallback_allowed
        );
        if !fallback_allowed {
            let _ = write!(out, "\nfallback execution: disabled");
        }
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "\ndiagnostics:");
            for diagnostic in &self.diagnostics {
                let _ = writeln!(out, "- {}", diagnostic.message);
            }
        }
        out
    }
    fn execute_synthetic_payload_cleanup(request: ShardLoomCleanupExecutionRequest) -> Self {
        #[cfg(feature = "spill-payload-fs")]
        {
            use std::fs;
            use std::path::Path;

            let Some(fs_ref) = request.synthetic_payload_ref.as_ref() else {
                return Self::blocked(
                    request,
                    ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact,
                    "synthetic spill payload filesystem reference is required",
                );
            };
            let target_path = Path::new(&fs_ref.path_string()).to_path_buf();
            if !target_path.exists() {
                return Self::blocked(
                    request,
                    ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact,
                    "synthetic spill payload file does not exist",
                );
            }
            if target_path.is_dir() {
                return Self::blocked(
                    request,
                    ShardLoomCleanupExecutionStatus::BlockedByUnsupportedArtifact,
                    "synthetic spill payload path is a directory",
                );
            }
            match fs::remove_file(&target_path) {
                Ok(()) => Self::cleanup_completed(request),
                Err(error) => Self::blocked(
                    request,
                    ShardLoomCleanupExecutionStatus::BlockedByUnsupportedArtifact,
                    format!(
                        "failed to remove synthetic spill payload file '{}': {error}",
                        target_path.to_string_lossy()
                    ),
                ),
            }
        }
        #[cfg(not(feature = "spill-payload-fs"))]
        {
            Self::blocked(
                request,
                ShardLoomCleanupExecutionStatus::FeatureDisabled,
                "synthetic spill payload cleanup execution requires the `spill-payload-fs` feature",
            )
        }
    }
}
/// # Errors
/// Returns an error when creating a `ShardLoomCleanupExecutionReport` from planning inputs fails.
pub fn plan_cleanup_execution(
    request: ShardLoomCleanupExecutionRequest,
) -> Result<ShardLoomCleanupExecutionReport> {
    ShardLoomCleanupExecutionReport::from_request(request)
}
/// # Errors
/// Returns an error when generating a `ShardLoomCleanupExecutionReport` fails.
pub fn execute_cleanup_plan(
    request: ShardLoomCleanupExecutionRequest,
) -> Result<ShardLoomCleanupExecutionReport> {
    let planned = ShardLoomCleanupExecutionReport::from_request(request)?;
    if planned.status != ShardLoomCleanupExecutionStatus::CleanupWouldExecute {
        return Ok(planned);
    }
    Ok(ShardLoomCleanupExecutionReport::execute_synthetic_payload_cleanup(planned.request))
}
pub fn cleanup_execution_plan_is_side_effect_free(
    report: &ShardLoomCleanupExecutionReport,
) -> bool {
    report.is_side_effect_free()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRetryCancellationStatus {
    Planned,
    RetryAllowed,
    RetryAllowedAfterCleanup,
    RetryBlockedByCleanup,
    RetryBlockedByUnknownArtifact,
    RetryBlockedByExternalEffect,
    RetryBlockedByPolicy,
    CancellationPlanned,
    CancellationBlocked,
    NotRequired,
    Unsupported,
}
impl ShardLoomRetryCancellationStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::RetryAllowed => "retry_allowed",
            Self::RetryAllowedAfterCleanup => "retry_allowed_after_cleanup",
            Self::RetryBlockedByCleanup => "retry_blocked_by_cleanup",
            Self::RetryBlockedByUnknownArtifact => "retry_blocked_by_unknown_artifact",
            Self::RetryBlockedByExternalEffect => "retry_blocked_by_external_effect",
            Self::RetryBlockedByPolicy => "retry_blocked_by_policy",
            Self::CancellationPlanned => "cancellation_planned",
            Self::CancellationBlocked => "cancellation_blocked",
            Self::NotRequired => "not_required",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::RetryBlockedByCleanup
                | Self::RetryBlockedByUnknownArtifact
                | Self::RetryBlockedByExternalEffect
                | Self::RetryBlockedByPolicy
                | Self::CancellationBlocked
                | Self::Unsupported
        )
    }
    pub const fn allows_retry(&self) -> bool {
        matches!(self, Self::RetryAllowed | Self::RetryAllowedAfterCleanup)
    }
    pub const fn requires_cleanup_before_retry(&self) -> bool {
        matches!(self, Self::RetryAllowedAfterCleanup)
    }
    pub const fn cancellation_requested(&self) -> bool {
        matches!(self, Self::CancellationPlanned)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRetryCancellationMode {
    ReportOnly,
    RetryPlanning,
    CancellationPlanning,
    RetryAndCancellationPlanning,
    Unsupported,
}
impl ShardLoomRetryCancellationMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::RetryPlanning => "retry_planning",
            Self::CancellationPlanning => "cancellation_planning",
            Self::RetryAndCancellationPlanning => "retry_and_cancellation_planning",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn executes_retry(&self) -> bool {
        false
    }
    pub const fn executes_cancellation(&self) -> bool {
        false
    }
    pub const fn executes_cleanup(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryCancellationOption {
    RetryRequested,
    CancellationRequested,
    ExternalEffectsPresent,
    CleanupAlreadyCompleted,
    AllowRetryAfterCleanup,
}
impl RetryCancellationOption {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryRequested => "retry_requested",
            Self::CancellationRequested => "cancellation_requested",
            Self::ExternalEffectsPresent => "external_effects_present",
            Self::CleanupAlreadyCompleted => "cleanup_already_completed",
            Self::AllowRetryAfterCleanup => "allow_retry_after_cleanup",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomRetryCancellationRequest {
    pub recovery_report: ShardLoomRecoveryIntegrationReport,
    pub attempt_id: Option<AttemptId>,
    pub options: Vec<RetryCancellationOption>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomRetryCancellationRequest {
    pub fn new(recovery_report: ShardLoomRecoveryIntegrationReport) -> Self {
        Self {
            attempt_id: recovery_report.request.attempt_id.clone(),
            recovery_report,
            options: vec![],
            diagnostics: vec![],
        }
    }
    pub fn for_attempt(
        recovery_report: ShardLoomRecoveryIntegrationReport,
        attempt_id: AttemptId,
    ) -> Self {
        let mut req = Self::new(recovery_report);
        req.attempt_id = Some(attempt_id);
        req
    }
    fn set_option(mut self, option: RetryCancellationOption, value: bool) -> Self {
        self.options.retain(|o| o != &option);
        if value {
            self.options.push(option);
        }
        self
    }
    pub fn retry_requested(self, value: bool) -> Self {
        self.set_option(RetryCancellationOption::RetryRequested, value)
    }
    pub fn cancellation_requested(self, value: bool) -> Self {
        self.set_option(RetryCancellationOption::CancellationRequested, value)
    }
    pub fn external_effects_present(self, value: bool) -> Self {
        self.set_option(RetryCancellationOption::ExternalEffectsPresent, value)
    }
    pub fn cleanup_already_completed(self, value: bool) -> Self {
        self.set_option(RetryCancellationOption::CleanupAlreadyCompleted, value)
    }
    pub fn allow_retry_after_cleanup(self, value: bool) -> Self {
        self.set_option(RetryCancellationOption::AllowRetryAfterCleanup, value)
    }
    pub fn has_option(&self, option: RetryCancellationOption) -> bool {
        self.options.contains(&option)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        has_error_diagnostics(&self.diagnostics) || self.recovery_report.has_errors()
    }
    pub fn summary(&self) -> String {
        format!(
            "attempt_present={} options={} retry_requested={} cancellation_requested={}",
            self.attempt_id.is_some(),
            self.options.len(),
            self.has_option(RetryCancellationOption::RetryRequested),
            self.has_option(RetryCancellationOption::CancellationRequested)
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomRetryCancellationReport {
    pub status: ShardLoomRetryCancellationStatus,
    pub mode: ShardLoomRetryCancellationMode,
    pub request: ShardLoomRetryCancellationRequest,
    pub retry_decision: Option<RetryDecision>,
    pub cancellation_request: Option<CancellationRequest>,
    pub cleanup_required_count: usize,
    pub unknown_artifact_count: usize,
    pub retry_allowed: bool,
    pub retry_requires_cleanup: bool,
    pub cancellation_requested: bool,
    pub retry_execution: RecoveryExecutionState,
    pub cancellation_execution: RecoveryExecutionState,
    pub cleanup_execution: RecoveryExecutionState,
    pub external_effects_execution: RecoveryExecutionState,
    pub object_store_io: RecoveryExecutionState,
    pub output_dataset_write: RecoveryExecutionState,
    pub fallback_execution: FallbackExecutionState,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomRetryCancellationReport {
    /// # Errors
    /// Returns an error when deriving retry/cancellation planning from the recovery report fails.
    pub fn from_request(request: ShardLoomRetryCancellationRequest) -> Result<Self> {
        let retry_requested = request.has_option(RetryCancellationOption::RetryRequested);
        let cancellation_requested =
            request.has_option(RetryCancellationOption::CancellationRequested);
        let external_effects = request.has_option(RetryCancellationOption::ExternalEffectsPresent);
        let cleanup_complete = request.has_option(RetryCancellationOption::CleanupAlreadyCompleted);
        let allow_after_cleanup =
            request.has_option(RetryCancellationOption::AllowRetryAfterCleanup);
        let cleanup_required_count = request.recovery_report.cleanup_required_count;
        let unknown_artifact_count = request.recovery_report.unknown_artifact_count;
        let retry_requires_cleanup =
            retry_requested && cleanup_required_count > 0 && !cleanup_complete;
        let mut status;
        let mut retry_allowed = false;
        let mut retry_decision = None;
        if retry_requested {
            if external_effects {
                status = ShardLoomRetryCancellationStatus::RetryBlockedByExternalEffect;
            } else if unknown_artifact_count > 0 {
                status = ShardLoomRetryCancellationStatus::RetryBlockedByUnknownArtifact;
            } else if retry_requires_cleanup && !allow_after_cleanup {
                status = ShardLoomRetryCancellationStatus::RetryBlockedByCleanup;
            } else if retry_requires_cleanup {
                status = ShardLoomRetryCancellationStatus::RetryAllowedAfterCleanup;
                retry_allowed = true;
                retry_decision = Some(RetryDecision::retry_after_cleanup(
                    "cleanup required before retry",
                ));
            } else {
                status = ShardLoomRetryCancellationStatus::RetryAllowed;
                retry_allowed = true;
                retry_decision = Some(RetryDecision::retry_now("retry allowed by planning policy"));
            }
        } else {
            status = ShardLoomRetryCancellationStatus::NotRequired;
        }
        let mut mode = if retry_requested {
            ShardLoomRetryCancellationMode::RetryPlanning
        } else {
            ShardLoomRetryCancellationMode::ReportOnly
        };
        let mut cancellation_request = None;
        if cancellation_requested {
            mode = if retry_requested {
                ShardLoomRetryCancellationMode::RetryAndCancellationPlanning
            } else {
                ShardLoomRetryCancellationMode::CancellationPlanning
            };
            cancellation_request = Some(CancellationRequest::new(
                CancellationScope::Task,
                CancellationReason::UserRequested,
            ));
            if !retry_requested {
                status = ShardLoomRetryCancellationStatus::CancellationPlanned;
            }
        }
        Ok(Self {
            status,
            mode,
            request,
            retry_decision,
            cancellation_request,
            cleanup_required_count,
            unknown_artifact_count,
            retry_allowed,
            retry_requires_cleanup,
            cancellation_requested,
            retry_execution: RecoveryExecutionState::NotPerformed,
            cancellation_execution: RecoveryExecutionState::NotPerformed,
            cleanup_execution: RecoveryExecutionState::NotPerformed,
            external_effects_execution: RecoveryExecutionState::NotPerformed,
            object_store_io: RecoveryExecutionState::NotPerformed,
            output_dataset_write: RecoveryExecutionState::NotPerformed,
            fallback_execution: FallbackExecutionState::Disabled,
            diagnostics: vec![],
        })
    }
    pub fn unsupported(
        request: ShardLoomRetryCancellationRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self {
            status: ShardLoomRetryCancellationStatus::Unsupported,
            mode: ShardLoomRetryCancellationMode::Unsupported,
            request,
            retry_decision: None,
            cancellation_request: None,
            cleanup_required_count: 0,
            unknown_artifact_count: 0,
            retry_allowed: false,
            retry_requires_cleanup: false,
            cancellation_requested: false,
            retry_execution: RecoveryExecutionState::NotPerformed,
            cancellation_execution: RecoveryExecutionState::NotPerformed,
            cleanup_execution: RecoveryExecutionState::NotPerformed,
            external_effects_execution: RecoveryExecutionState::NotPerformed,
            object_store_io: RecoveryExecutionState::NotPerformed,
            output_dataset_write: RecoveryExecutionState::NotPerformed,
            fallback_execution: FallbackExecutionState::Disabled,
            diagnostics: vec![],
        };
        r.add_diagnostic(unsupported_diagnostic(feature, reason));
        r
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self
                .retry_decision
                .as_ref()
                .is_some_and(RetryDecision::has_errors)
            || self
                .cancellation_request
                .as_ref()
                .is_some_and(CancellationRequest::has_errors)
            || has_error_diagnostics(&self.diagnostics)
            || has_error_diagnostics(&self.request.recovery_report.diagnostics)
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.cleanup_execution.executed()
            && !self.retry_execution.executed()
            && !self.cancellation_execution.executed()
            && !self.external_effects_execution.executed()
            && !self.object_store_io.executed()
            && !self.output_dataset_write.executed()
            && !self.fallback_execution.allowed()
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "retry/cancellation status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        if let Some(attempt) = &self.request.attempt_id {
            let _ = writeln!(out, "attempt id: {}", attempt.as_str());
        }
        let _ = writeln!(
            out,
            "retry requested: {}",
            self.request
                .has_option(RetryCancellationOption::RetryRequested)
        );
        let _ = writeln!(out, "retry allowed: {}", self.retry_allowed);
        let _ = writeln!(
            out,
            "retry requires cleanup: {}",
            self.retry_requires_cleanup
        );
        let _ = writeln!(
            out,
            "cleanup required count: {}",
            self.cleanup_required_count
        );
        let _ = writeln!(
            out,
            "unknown artifact count: {}",
            self.unknown_artifact_count
        );
        let _ = writeln!(
            out,
            "cancellation requested: {}",
            self.cancellation_requested
        );
        let _ = write!(
            out,
            "retry executed: {}\ncancellation executed: {}\ncleanup executed: {}\nexternal effects executed: {}\nobject-store IO: {}\noutput dataset write: {}\nfallback execution: disabled",
            self.retry_execution.executed(),
            self.cancellation_execution.executed(),
            self.cleanup_execution.executed(),
            self.external_effects_execution.executed(),
            self.object_store_io.executed(),
            self.output_dataset_write.executed()
        );
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {}", d.message);
            }
        }
        out
    }
}
/// # Errors
/// Returns an error when retry/cancellation planning cannot be derived from the request.
pub fn plan_retry_cancellation(
    request: ShardLoomRetryCancellationRequest,
) -> Result<ShardLoomRetryCancellationReport> {
    ShardLoomRetryCancellationReport::from_request(request)
}
pub fn retry_cancellation_plan_is_side_effect_free(
    report: &ShardLoomRetryCancellationReport,
) -> bool {
    report.is_side_effect_free()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRetryExecutionGateStatus {
    GateOpen,
    GateClosedCleanupRequired,
    GateClosedUnknownArtifact,
    GateClosedExternalEffect,
    GateClosedRetryNotRequested,
    GateClosedRetryNotAllowed,
    GateClosedCancellationRequested,
    GateClosedObjectStoreRecovery,
    GateClosedOutputRecovery,
    Unsupported,
}
impl ShardLoomRetryExecutionGateStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::GateOpen => "gate_open",
            Self::GateClosedCleanupRequired => "gate_closed_cleanup_required",
            Self::GateClosedUnknownArtifact => "gate_closed_unknown_artifact",
            Self::GateClosedExternalEffect => "gate_closed_external_effect",
            Self::GateClosedRetryNotRequested => "gate_closed_retry_not_requested",
            Self::GateClosedRetryNotAllowed => "gate_closed_retry_not_allowed",
            Self::GateClosedCancellationRequested => "gate_closed_cancellation_requested",
            Self::GateClosedObjectStoreRecovery => "gate_closed_object_store_recovery",
            Self::GateClosedOutputRecovery => "gate_closed_output_recovery",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::GateClosedCleanupRequired
                | Self::GateClosedUnknownArtifact
                | Self::GateClosedExternalEffect
                | Self::GateClosedRetryNotAllowed
                | Self::GateClosedCancellationRequested
                | Self::GateClosedObjectStoreRecovery
                | Self::GateClosedOutputRecovery
                | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn gate_open(&self) -> bool {
        matches!(self, Self::GateOpen)
    }
    #[must_use]
    pub const fn requires_cleanup(&self) -> bool {
        matches!(self, Self::GateClosedCleanupRequired)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRetryExecutionGateMode {
    ReportOnly,
    RetryGateOnly,
    Unsupported,
}
impl ShardLoomRetryExecutionGateMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::RetryGateOnly => "retry_gate_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_retry(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_cleanup(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_cancellation(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRetryExecutionGateSignal {
    RetryRequested,
    RetryAllowedByPlan,
    RetryRequiresCleanup,
    CleanupCompleted,
    UnknownArtifactPresent,
    ExternalEffectsPresent,
    ObjectStoreRecoveryRequired,
    OutputRecoveryRequired,
    CancellationRequested,
}
impl ShardLoomRetryExecutionGateSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryRequested => "retry_requested",
            Self::RetryAllowedByPlan => "retry_allowed_by_plan",
            Self::RetryRequiresCleanup => "retry_requires_cleanup",
            Self::CleanupCompleted => "cleanup_completed",
            Self::UnknownArtifactPresent => "unknown_artifact_present",
            Self::ExternalEffectsPresent => "external_effects_present",
            Self::ObjectStoreRecoveryRequired => "object_store_recovery_required",
            Self::OutputRecoveryRequired => "output_recovery_required",
            Self::CancellationRequested => "cancellation_requested",
        }
    }
    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::UnknownArtifactPresent
                | Self::ExternalEffectsPresent
                | Self::ObjectStoreRecoveryRequired
                | Self::OutputRecoveryRequired
                | Self::CancellationRequested
                | Self::RetryRequiresCleanup
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomRetryExecutionGateEffect {
    RetryExecuted,
    CleanupExecutedByGate,
    CancellationExecuted,
    ExternalEffectExecuted,
    ObjectStoreIo,
    OutputDatasetWrite,
    FallbackExecution,
}
impl ShardLoomRetryExecutionGateEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryExecuted => "retry_executed",
            Self::CleanupExecutedByGate => "cleanup_executed_by_gate",
            Self::CancellationExecuted => "cancellation_executed",
            Self::ExternalEffectExecuted => "external_effect_executed",
            Self::ObjectStoreIo => "object_store_io",
            Self::OutputDatasetWrite => "output_dataset_write",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomRetryExecutionGateRequest {
    pub signals: Vec<ShardLoomRetryExecutionGateSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomRetryExecutionGateRequest {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signals: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_signal(&mut self, signal: ShardLoomRetryExecutionGateSignal) {
        if !self.signals.contains(&signal) {
            self.signals.push(signal);
        }
    }
    fn set_signal(mut self, signal: ShardLoomRetryExecutionGateSignal, value: bool) -> Self {
        self.signals.retain(|existing| existing != &signal);
        if value {
            self.signals.push(signal);
        }
        self
    }
    pub fn retry_requested(self, value: bool) -> Self {
        self.set_signal(ShardLoomRetryExecutionGateSignal::RetryRequested, value)
    }
    pub fn retry_allowed_by_plan(self, value: bool) -> Self {
        self.set_signal(ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan, value)
    }
    pub fn retry_requires_cleanup(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomRetryExecutionGateSignal::RetryRequiresCleanup,
            value,
        )
    }
    pub fn cleanup_completed(self, value: bool) -> Self {
        self.set_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted, value)
    }
    pub fn unknown_artifact_present(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent,
            value,
        )
    }
    pub fn external_effects_present(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomRetryExecutionGateSignal::ExternalEffectsPresent,
            value,
        )
    }
    pub fn object_store_recovery_required(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomRetryExecutionGateSignal::ObjectStoreRecoveryRequired,
            value,
        )
    }
    pub fn output_recovery_required(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomRetryExecutionGateSignal::OutputRecoveryRequired,
            value,
        )
    }
    pub fn cancellation_requested(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomRetryExecutionGateSignal::CancellationRequested,
            value,
        )
    }
    #[must_use]
    pub fn has_signal(&self, signal: ShardLoomRetryExecutionGateSignal) -> bool {
        self.signals.contains(&signal)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        has_error_diagnostics(&self.diagnostics)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "signals={} retry_requested={} retry_allowed_by_plan={}",
            self.signals.len(),
            self.has_signal(ShardLoomRetryExecutionGateSignal::RetryRequested),
            self.has_signal(ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan)
        )
    }
}
impl Default for ShardLoomRetryExecutionGateRequest {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomRetryExecutionGateReport {
    pub status: ShardLoomRetryExecutionGateStatus,
    pub mode: ShardLoomRetryExecutionGateMode,
    pub request: ShardLoomRetryExecutionGateRequest,
    pub effects_performed: Vec<ShardLoomRetryExecutionGateEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomRetryExecutionGateReport {
    /// # Errors
    /// Returns an error when creating a `ShardLoomRetryExecutionGateReport` fails.
    pub fn from_request(request: ShardLoomRetryExecutionGateRequest) -> Result<Self> {
        let status = if request.has_signal(ShardLoomRetryExecutionGateSignal::CancellationRequested)
        {
            ShardLoomRetryExecutionGateStatus::GateClosedCancellationRequested
        } else if request.has_signal(ShardLoomRetryExecutionGateSignal::ExternalEffectsPresent) {
            ShardLoomRetryExecutionGateStatus::GateClosedExternalEffect
        } else if request.has_signal(ShardLoomRetryExecutionGateSignal::ObjectStoreRecoveryRequired)
        {
            ShardLoomRetryExecutionGateStatus::GateClosedObjectStoreRecovery
        } else if request.has_signal(ShardLoomRetryExecutionGateSignal::OutputRecoveryRequired) {
            ShardLoomRetryExecutionGateStatus::GateClosedOutputRecovery
        } else if request.has_signal(ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent) {
            ShardLoomRetryExecutionGateStatus::GateClosedUnknownArtifact
        } else if !request.has_signal(ShardLoomRetryExecutionGateSignal::RetryRequested) {
            ShardLoomRetryExecutionGateStatus::GateClosedRetryNotRequested
        } else if !request.has_signal(ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan) {
            ShardLoomRetryExecutionGateStatus::GateClosedRetryNotAllowed
        } else if request.has_signal(ShardLoomRetryExecutionGateSignal::RetryRequiresCleanup)
            && !request.has_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted)
        {
            ShardLoomRetryExecutionGateStatus::GateClosedCleanupRequired
        } else {
            ShardLoomRetryExecutionGateStatus::GateOpen
        };
        Ok(Self {
            status,
            mode: ShardLoomRetryExecutionGateMode::RetryGateOnly,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn unsupported(
        request: ShardLoomRetryExecutionGateRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: ShardLoomRetryExecutionGateStatus::Unsupported,
            mode: ShardLoomRetryExecutionGateMode::Unsupported,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        };
        report.add_diagnostic(unsupported_diagnostic(feature, reason));
        report
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || has_error_diagnostics(&self.diagnostics)
    }
    #[must_use]
    pub fn retry_requested(&self) -> bool {
        self.request
            .has_signal(ShardLoomRetryExecutionGateSignal::RetryRequested)
    }
    #[must_use]
    pub fn retry_allowed_by_plan(&self) -> bool {
        self.request
            .has_signal(ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan)
    }
    #[must_use]
    pub fn retry_gate_open(&self) -> bool {
        self.status.gate_open()
    }
    #[must_use]
    pub fn retry_requires_cleanup(&self) -> bool {
        self.request
            .has_signal(ShardLoomRetryExecutionGateSignal::RetryRequiresCleanup)
    }
    #[must_use]
    pub fn cleanup_completed(&self) -> bool {
        self.request
            .has_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted)
    }
    #[must_use]
    pub fn unknown_artifact_present(&self) -> bool {
        self.request
            .has_signal(ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent)
    }
    #[must_use]
    pub fn retry_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::RetryExecuted)
    }
    #[must_use]
    pub fn cleanup_executed_by_gate(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::CleanupExecutedByGate)
    }
    #[must_use]
    pub fn cancellation_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::CancellationExecuted)
    }
    #[must_use]
    pub fn external_effects_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::ExternalEffectExecuted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn output_dataset_write(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::OutputDatasetWrite)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomRetryExecutionGateEffect::FallbackExecution)
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "retry execution gate status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "retry requested: {}", self.retry_requested());
        let _ = writeln!(
            out,
            "retry allowed by plan: {}",
            self.retry_allowed_by_plan()
        );
        let _ = writeln!(out, "retry gate open: {}", self.retry_gate_open());
        let _ = writeln!(
            out,
            "retry requires cleanup: {}",
            self.retry_requires_cleanup()
        );
        let _ = writeln!(out, "cleanup completed: {}", self.cleanup_completed());
        let _ = writeln!(
            out,
            "unknown artifact present: {}",
            self.unknown_artifact_present()
        );
        let _ = write!(
            out,
            "retry executed: {}\ncleanup executed by gate: {}\ncancellation executed: {}\nexternal effects executed: {}\nobject-store IO: {}\noutput dataset write: {}\nfallback execution: disabled",
            self.retry_executed(),
            self.cleanup_executed_by_gate(),
            self.cancellation_executed(),
            self.external_effects_executed(),
            self.object_store_io(),
            self.output_dataset_write()
        );
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(out, "\ndiagnostics:");
            for diagnostic in self.request.diagnostics.iter().chain(&self.diagnostics) {
                let _ = writeln!(out, "- {}", diagnostic.message);
            }
        }
        out
    }
}
/// # Errors
/// Returns an error when creating a `ShardLoomRetryExecutionGateReport` from planning signals fails.
pub fn plan_retry_execution_gate(
    request: ShardLoomRetryExecutionGateRequest,
) -> Result<ShardLoomRetryExecutionGateReport> {
    ShardLoomRetryExecutionGateReport::from_request(request)
}
#[must_use]
pub fn retry_gate_request_from_retry_cancellation_report(
    report: &ShardLoomRetryCancellationReport,
) -> ShardLoomRetryExecutionGateRequest {
    let mut request = ShardLoomRetryExecutionGateRequest::new();
    if report
        .request
        .has_option(RetryCancellationOption::RetryRequested)
    {
        request.add_signal(ShardLoomRetryExecutionGateSignal::RetryRequested);
    }
    if report.retry_allowed {
        request.add_signal(ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan);
    }
    if report.retry_requires_cleanup {
        request.add_signal(ShardLoomRetryExecutionGateSignal::RetryRequiresCleanup);
    }
    if report.unknown_artifact_count > 0 {
        request.add_signal(ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent);
    }
    if report
        .request
        .has_option(RetryCancellationOption::ExternalEffectsPresent)
        || report.status == ShardLoomRetryCancellationStatus::RetryBlockedByExternalEffect
    {
        request.add_signal(ShardLoomRetryExecutionGateSignal::ExternalEffectsPresent);
    }
    if report.cancellation_requested {
        request.add_signal(ShardLoomRetryExecutionGateSignal::CancellationRequested);
    }
    request
        .diagnostics
        .extend(report.request.diagnostics.clone());
    request
        .diagnostics
        .extend(report.request.recovery_report.diagnostics.clone());
    request.diagnostics.extend(report.diagnostics.clone());
    request
}
#[must_use]
pub fn retry_gate_request_with_cleanup_report(
    mut request: ShardLoomRetryExecutionGateRequest,
    cleanup_report: &ShardLoomCleanupExecutionReport,
) -> ShardLoomRetryExecutionGateRequest {
    if cleanup_report.cleanup_executed() {
        request.add_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted);
    }
    if matches!(
        cleanup_report.status,
        ShardLoomCleanupExecutionStatus::BlockedByUnknownArtifact
            | ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact
    ) {
        request.add_signal(ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent);
    }
    if cleanup_report.object_store_io() {
        request.add_signal(ShardLoomRetryExecutionGateSignal::ObjectStoreRecoveryRequired);
    }
    if cleanup_report.output_dataset_write() {
        request.add_signal(ShardLoomRetryExecutionGateSignal::OutputRecoveryRequired);
    }
    request
        .diagnostics
        .extend(cleanup_report.request.diagnostics.clone());
    request
        .diagnostics
        .extend(cleanup_report.diagnostics.clone());
    request
}
#[must_use]
pub fn retry_gate_request_from_reports(
    retry_report: &ShardLoomRetryCancellationReport,
    cleanup_report: Option<&ShardLoomCleanupExecutionReport>,
) -> ShardLoomRetryExecutionGateRequest {
    let request = retry_gate_request_from_retry_cancellation_report(retry_report);
    if let Some(cleanup_report) = cleanup_report {
        return retry_gate_request_with_cleanup_report(request, cleanup_report);
    }
    request
}
/// # Errors
/// Returns an error when creating a `ShardLoomRetryExecutionGateReport` from derived report signals fails.
pub fn plan_retry_execution_gate_from_reports(
    retry_report: &ShardLoomRetryCancellationReport,
    cleanup_report: Option<&ShardLoomCleanupExecutionReport>,
) -> Result<ShardLoomRetryExecutionGateReport> {
    plan_retry_execution_gate(retry_gate_request_from_reports(
        retry_report,
        cleanup_report,
    ))
}
#[must_use]
pub fn retry_execution_gate_is_side_effect_free(
    report: &ShardLoomRetryExecutionGateReport,
) -> bool {
    report.is_side_effect_free()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCancellationExecutionGateStatus {
    GateOpen,
    GateClosedCancellationNotRequested,
    GateClosedCleanupRequired,
    GateClosedUnknownArtifact,
    GateClosedExternalEffect,
    GateClosedObjectStoreRecovery,
    GateClosedOutputRecovery,
    GateClosedRetryInProgress,
    Unsupported,
}
impl ShardLoomCancellationExecutionGateStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::GateOpen => "gate_open",
            Self::GateClosedCancellationNotRequested => "gate_closed_cancellation_not_requested",
            Self::GateClosedCleanupRequired => "gate_closed_cleanup_required",
            Self::GateClosedUnknownArtifact => "gate_closed_unknown_artifact",
            Self::GateClosedExternalEffect => "gate_closed_external_effect",
            Self::GateClosedObjectStoreRecovery => "gate_closed_object_store_recovery",
            Self::GateClosedOutputRecovery => "gate_closed_output_recovery",
            Self::GateClosedRetryInProgress => "gate_closed_retry_in_progress",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(
            self,
            Self::GateOpen | Self::GateClosedCancellationNotRequested
        )
    }
    #[must_use]
    pub const fn gate_open(&self) -> bool {
        matches!(self, Self::GateOpen)
    }
    #[must_use]
    pub const fn requires_cleanup(&self) -> bool {
        matches!(self, Self::GateClosedCleanupRequired)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCancellationExecutionGateMode {
    ReportOnly,
    CancellationGateOnly,
    Unsupported,
}
impl ShardLoomCancellationExecutionGateMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::CancellationGateOnly => "cancellation_gate_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_cancellation(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_retry(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_cleanup(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCancellationExecutionGateSignal {
    CancellationRequested,
    CleanupRequired,
    CleanupCompleted,
    UnknownArtifactPresent,
    ExternalEffectsPresent,
    ObjectStoreRecoveryRequired,
    OutputRecoveryRequired,
    RetryInProgress,
}
impl ShardLoomCancellationExecutionGateSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CancellationRequested => "cancellation_requested",
            Self::CleanupRequired => "cleanup_required",
            Self::CleanupCompleted => "cleanup_completed",
            Self::UnknownArtifactPresent => "unknown_artifact_present",
            Self::ExternalEffectsPresent => "external_effects_present",
            Self::ObjectStoreRecoveryRequired => "object_store_recovery_required",
            Self::OutputRecoveryRequired => "output_recovery_required",
            Self::RetryInProgress => "retry_in_progress",
        }
    }
    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::UnknownArtifactPresent
                | Self::ExternalEffectsPresent
                | Self::ObjectStoreRecoveryRequired
                | Self::OutputRecoveryRequired
                | Self::RetryInProgress
                | Self::CleanupRequired
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomCancellationExecutionGateEffect {
    CancellationExecuted,
    RetryExecuted,
    CleanupExecutedByGate,
    ExternalEffectExecuted,
    ObjectStoreIo,
    OutputDatasetWrite,
    FallbackExecution,
}
impl ShardLoomCancellationExecutionGateEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CancellationExecuted => "cancellation_executed",
            Self::RetryExecuted => "retry_executed",
            Self::CleanupExecutedByGate => "cleanup_executed_by_gate",
            Self::ExternalEffectExecuted => "external_effect_executed",
            Self::ObjectStoreIo => "object_store_io",
            Self::OutputDatasetWrite => "output_dataset_write",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomCancellationExecutionGateRequest {
    pub signals: Vec<ShardLoomCancellationExecutionGateSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomCancellationExecutionGateRequest {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signals: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_signal(&mut self, signal: ShardLoomCancellationExecutionGateSignal) {
        if !self.signals.contains(&signal) {
            self.signals.push(signal);
        }
    }
    fn set_signal(mut self, signal: ShardLoomCancellationExecutionGateSignal, value: bool) -> Self {
        self.signals.retain(|s| s != &signal);
        if value {
            self.signals.push(signal);
        }
        self
    }
    pub fn cancellation_requested(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::CancellationRequested,
            value,
        )
    }
    pub fn cleanup_required(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::CleanupRequired,
            value,
        )
    }
    pub fn cleanup_completed(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::CleanupCompleted,
            value,
        )
    }
    pub fn unknown_artifact_present(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent,
            value,
        )
    }
    pub fn external_effects_present(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::ExternalEffectsPresent,
            value,
        )
    }
    pub fn object_store_recovery_required(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::ObjectStoreRecoveryRequired,
            value,
        )
    }
    pub fn output_recovery_required(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::OutputRecoveryRequired,
            value,
        )
    }
    pub fn retry_in_progress(self, value: bool) -> Self {
        self.set_signal(
            ShardLoomCancellationExecutionGateSignal::RetryInProgress,
            value,
        )
    }
    #[must_use]
    pub fn has_signal(&self, signal: ShardLoomCancellationExecutionGateSignal) -> bool {
        self.signals.contains(&signal)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        has_error_diagnostics(&self.diagnostics)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "signals={} cancellation_requested={}",
            self.signals.len(),
            self.has_signal(ShardLoomCancellationExecutionGateSignal::CancellationRequested)
        )
    }
}
impl Default for ShardLoomCancellationExecutionGateRequest {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ShardLoomCancellationExecutionGateReport {
    pub status: ShardLoomCancellationExecutionGateStatus,
    pub mode: ShardLoomCancellationExecutionGateMode,
    pub request: ShardLoomCancellationExecutionGateRequest,
    pub effects_performed: Vec<ShardLoomCancellationExecutionGateEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ShardLoomCancellationExecutionGateReport {
    /// # Errors
    /// Returns an error when creating a `ShardLoomCancellationExecutionGateReport` fails.
    pub fn from_request(request: ShardLoomCancellationExecutionGateRequest) -> Result<Self> {
        let status = if !request
            .has_signal(ShardLoomCancellationExecutionGateSignal::CancellationRequested)
        {
            ShardLoomCancellationExecutionGateStatus::GateClosedCancellationNotRequested
        } else if request.has_signal(ShardLoomCancellationExecutionGateSignal::RetryInProgress) {
            ShardLoomCancellationExecutionGateStatus::GateClosedRetryInProgress
        } else if request
            .has_signal(ShardLoomCancellationExecutionGateSignal::ExternalEffectsPresent)
        {
            ShardLoomCancellationExecutionGateStatus::GateClosedExternalEffect
        } else if request
            .has_signal(ShardLoomCancellationExecutionGateSignal::ObjectStoreRecoveryRequired)
        {
            ShardLoomCancellationExecutionGateStatus::GateClosedObjectStoreRecovery
        } else if request
            .has_signal(ShardLoomCancellationExecutionGateSignal::OutputRecoveryRequired)
        {
            ShardLoomCancellationExecutionGateStatus::GateClosedOutputRecovery
        } else if request
            .has_signal(ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent)
        {
            ShardLoomCancellationExecutionGateStatus::GateClosedUnknownArtifact
        } else if request.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupRequired)
            && !request.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted)
        {
            ShardLoomCancellationExecutionGateStatus::GateClosedCleanupRequired
        } else {
            ShardLoomCancellationExecutionGateStatus::GateOpen
        };
        Ok(Self {
            status,
            mode: ShardLoomCancellationExecutionGateMode::CancellationGateOnly,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn unsupported(
        request: ShardLoomCancellationExecutionGateRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: ShardLoomCancellationExecutionGateStatus::Unsupported,
            mode: ShardLoomCancellationExecutionGateMode::Unsupported,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        };
        report.add_diagnostic(unsupported_diagnostic(feature, reason));
        report
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || has_error_diagnostics(&self.diagnostics)
    }
    #[must_use]
    pub fn cancellation_requested(&self) -> bool {
        self.request
            .has_signal(ShardLoomCancellationExecutionGateSignal::CancellationRequested)
    }
    #[must_use]
    pub fn cancellation_gate_open(&self) -> bool {
        self.status.gate_open()
    }
    #[must_use]
    pub fn cleanup_required(&self) -> bool {
        self.request
            .has_signal(ShardLoomCancellationExecutionGateSignal::CleanupRequired)
    }
    #[must_use]
    pub fn cleanup_completed(&self) -> bool {
        self.request
            .has_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted)
    }
    #[must_use]
    pub fn unknown_artifact_present(&self) -> bool {
        self.request
            .has_signal(ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent)
    }
    #[must_use]
    pub fn cancellation_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::CancellationExecuted)
    }
    #[must_use]
    pub fn retry_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::RetryExecuted)
    }
    #[must_use]
    pub fn cleanup_executed_by_gate(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::CleanupExecutedByGate)
    }
    #[must_use]
    pub fn external_effects_executed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::ExternalEffectExecuted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn output_dataset_write(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::OutputDatasetWrite)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&ShardLoomCancellationExecutionGateEffect::FallbackExecution)
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "cancellation execution gate status: {}",
            self.status.as_str()
        );
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            out,
            "cancellation requested: {}",
            self.cancellation_requested()
        );
        let _ = writeln!(
            out,
            "cancellation gate open: {}",
            self.cancellation_gate_open()
        );
        let _ = writeln!(out, "cleanup required: {}", self.cleanup_required());
        let _ = writeln!(out, "cleanup completed: {}", self.cleanup_completed());
        let _ = writeln!(
            out,
            "unknown artifact present: {}",
            self.unknown_artifact_present()
        );
        let _ = write!(
            out,
            "cancellation executed: {}
retry executed: {}
cleanup executed by gate: {}
external effects executed: {}
object-store IO: {}
output dataset write: {}
fallback execution: disabled",
            self.cancellation_executed(),
            self.retry_executed(),
            self.cleanup_executed_by_gate(),
            self.external_effects_executed(),
            self.object_store_io(),
            self.output_dataset_write()
        );
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(
                out,
                "
diagnostics:"
            );
            for diagnostic in self.request.diagnostics.iter().chain(&self.diagnostics) {
                let _ = writeln!(out, "- {}", diagnostic.message);
            }
        }
        out
    }
}
/// # Errors
/// Returns an error when creating a `ShardLoomCancellationExecutionGateReport` from planning signals fails.
pub fn plan_cancellation_execution_gate(
    request: ShardLoomCancellationExecutionGateRequest,
) -> Result<ShardLoomCancellationExecutionGateReport> {
    ShardLoomCancellationExecutionGateReport::from_request(request)
}
#[must_use]
pub fn cancellation_gate_request_from_retry_cancellation_report(
    report: &ShardLoomRetryCancellationReport,
) -> ShardLoomCancellationExecutionGateRequest {
    let mut request = ShardLoomCancellationExecutionGateRequest::new();
    if report.cancellation_requested {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::CancellationRequested);
    }
    if report.cleanup_required_count > 0 {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::CleanupRequired);
    }
    if report
        .request
        .has_option(RetryCancellationOption::CleanupAlreadyCompleted)
    {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted);
    }
    if report.unknown_artifact_count > 0 {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent);
    }
    if report
        .request
        .has_option(RetryCancellationOption::ExternalEffectsPresent)
        || report.status == ShardLoomRetryCancellationStatus::RetryBlockedByExternalEffect
    {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::ExternalEffectsPresent);
    }
    if report.object_store_io.executed() {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::ObjectStoreRecoveryRequired);
    }
    if report.output_dataset_write.executed() {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::OutputRecoveryRequired);
    }
    request
        .diagnostics
        .extend(report.request.diagnostics.clone());
    request
        .diagnostics
        .extend(report.request.recovery_report.diagnostics.clone());
    request.diagnostics.extend(report.diagnostics.clone());
    request
}
#[must_use]
pub fn cancellation_gate_request_with_cleanup_report(
    mut request: ShardLoomCancellationExecutionGateRequest,
    cleanup_report: &ShardLoomCleanupExecutionReport,
) -> ShardLoomCancellationExecutionGateRequest {
    if cleanup_report.cleanup_executed() {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted);
    }
    if matches!(
        cleanup_report.status,
        ShardLoomCleanupExecutionStatus::BlockedByUnknownArtifact
            | ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact
    ) {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent);
    }
    if cleanup_report.object_store_io() {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::ObjectStoreRecoveryRequired);
    }
    if cleanup_report.output_dataset_write() {
        request.add_signal(ShardLoomCancellationExecutionGateSignal::OutputRecoveryRequired);
    }
    request
        .diagnostics
        .extend(cleanup_report.request.diagnostics.clone());
    request
        .diagnostics
        .extend(cleanup_report.diagnostics.clone());
    request
}
#[must_use]
pub fn cancellation_gate_request_from_reports(
    retry_report: &ShardLoomRetryCancellationReport,
    cleanup_report: Option<&ShardLoomCleanupExecutionReport>,
) -> ShardLoomCancellationExecutionGateRequest {
    let request = cancellation_gate_request_from_retry_cancellation_report(retry_report);
    if let Some(cleanup_report) = cleanup_report {
        return cancellation_gate_request_with_cleanup_report(request, cleanup_report);
    }
    request
}
/// # Errors
/// Returns an error when creating a `ShardLoomCancellationExecutionGateReport` from derived report signals fails.
pub fn plan_cancellation_execution_gate_from_reports(
    retry_report: &ShardLoomRetryCancellationReport,
    cleanup_report: Option<&ShardLoomCleanupExecutionReport>,
) -> Result<ShardLoomCancellationExecutionGateReport> {
    plan_cancellation_execution_gate(cancellation_gate_request_from_reports(
        retry_report,
        cleanup_report,
    ))
}
#[must_use]
pub fn cancellation_execution_gate_is_side_effect_free(
    report: &ShardLoomCancellationExecutionGateReport,
) -> bool {
    report.is_side_effect_free()
}
impl RecoveryReport {
    pub fn not_run() -> Self {
        Self {
            status: RecoveryPlanStatus::DiagnosticOnly,
            actions_completed: 0,
            cleanup_completed: false,
            diagnostics: vec![],
            notes: vec![
                "Recovery execution not run; planning/reporting skeleton only.".to_string(),
            ],
        }
    }
    pub fn from_plan(plan: &RecoveryPlan) -> Self {
        Self {
            status: plan.status,
            actions_completed: 0,
            cleanup_completed: !plan.requires_cleanup(),
            diagnostics: plan.diagnostics.clone(),
            notes: vec!["No actions executed; this is a reporting skeleton.".to_string()],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn add_note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error() || has_error_diagnostics(&self.diagnostics)
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "Recovery report status={} actions_completed={} cleanup_completed={} fallback execution: disabled",
            self.status.as_str(),
            self.actions_completed,
            self.cleanup_completed
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{SpillPayloadFsRef, SpillPayloadId, SpillPayloadPath, SpillPayloadRef};

    use super::*;
    use shardloom_core::DatasetUri;
    fn task_id() -> TaskId {
        TaskId::new("task-1").expect("task")
    }
    fn output_target() -> OutputTarget {
        OutputTarget::from_uri(DatasetUri::new("file://tmp/out.vortex").expect("uri"))
    }
    fn spill_payload_fs_ref(payload_id: &str, workspace: &str) -> SpillPayloadFsRef {
        let payload_id = SpillPayloadId::new(payload_id).expect("payload id");
        let payload_ref = SpillPayloadRef::new(payload_id, workspace).expect("payload ref");
        let workspace_path = SpillPayloadPath::new(workspace).expect("workspace path");
        SpillPayloadFsRef::new(payload_ref, workspace_path)
    }
    #[test]
    fn attempt_id_rejects_empty_ids() {
        assert!(AttemptId::new("  ").is_err());
    }
    #[test]
    fn failure_domain_external_api_is_external_effect() {
        assert!(FailureDomain::ExternalApi.is_external_effect());
    }
    #[test]
    fn failure_kind_timeout_is_retryable_candidate() {
        assert!(FailureKind::Timeout.is_retryable_candidate());
    }
    #[test]
    fn failure_kind_ambiguous_commit_is_not_retryable_candidate() {
        assert!(!FailureKind::AmbiguousCommit.is_retryable_candidate());
    }
    #[test]
    fn task_attempt_status_failed_is_terminal_and_failure() {
        assert!(TaskAttemptStatus::Failed.is_terminal());
        assert!(TaskAttemptStatus::Failed.is_failure());
    }
    #[test]
    fn retry_eligibility_retryable_can_retry_now() {
        assert!(RetryEligibility::Retryable.can_retry_now());
    }
    #[test]
    fn failure_record_new_sets_retry_eligibility_from_failure_kind() {
        let r = FailureRecord::new(FailureDomain::ExecutionTask, FailureKind::Timeout, "x");
        assert_eq!(r.retry_eligibility, RetryEligibility::Retryable);
    }
    #[test]
    fn task_attempt_record_failed_has_errors() {
        let a = TaskAttemptRecord::new(task_id(), AttemptId::new("a1").expect("attempt")).failed(
            FailureRecord::new(FailureDomain::ExecutionTask, FailureKind::Unknown, "fail"),
        );
        assert!(a.has_errors());
    }
    #[test]
    fn task_attempt_record_failed_output_files_requires_cleanup() {
        let mut a =
            TaskAttemptRecord::new(task_id(), AttemptId::new("a1").expect("attempt")).failed(
                FailureRecord::new(FailureDomain::ExecutionTask, FailureKind::Unknown, "fail"),
            );
        a.add_output_file("f");
        assert!(a.requires_cleanup());
    }
    #[test]
    fn retry_decision_retry_now_will_retry() {
        assert!(RetryDecision::retry_now("x").will_retry());
    }
    #[test]
    fn retry_decision_do_not_retry_will_not_retry() {
        assert!(!RetryDecision::do_not_retry("x").will_retry());
    }
    #[test]
    fn retry_decision_unsupported_has_errors_and_fallback_attempted_false() {
        let d = RetryDecision::unsupported("x", "y");
        assert!(d.has_errors());
        assert!(!d.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn retry_plan_from_attempt_returns_do_not_retry_for_succeeded_attempt() {
        let a =
            TaskAttemptRecord::new(task_id(), AttemptId::new("a1").expect("attempt")).succeeded();
        let p = RetryPlan::from_attempt(RetryPolicy::default_read_retries(), a);
        assert_eq!(p.decision.kind, RetryDecisionKind::DoNotRetry);
    }
    #[test]
    fn retry_plan_from_attempt_returns_retry_after_cleanup_for_failed_attempt_with_cleanup() {
        let mut a =
            TaskAttemptRecord::new(task_id(), AttemptId::new("a1").expect("attempt")).failed(
                FailureRecord::new(FailureDomain::ExecutionTask, FailureKind::Unknown, "fail"),
            );
        a.add_spill_file("s");
        let p = RetryPlan::from_attempt(RetryPolicy::default_read_retries(), a);
        assert_eq!(p.decision.kind, RetryDecisionKind::RetryAfterCleanup);
    }
    #[test]
    fn cancellation_request_unsupported_has_errors() {
        assert!(CancellationRequest::unsupported(CancellationScope::Task, "x").has_errors());
    }
    #[test]
    fn recovery_action_kind_cleanup_temporary_output_requires_io() {
        assert!(RecoveryActionKind::CleanupTemporaryOutput.requires_io());
    }
    #[test]
    fn cleanup_requirement_rejects_empty_target() {
        assert!(CleanupRequirement::required(CleanupTargetKind::TemporaryOutput, " ").is_err());
    }
    #[test]
    fn cleanup_requirement_unsupported_has_errors() {
        assert!(
            CleanupRequirement::unsupported(CleanupTargetKind::Unknown, "x", "r")
                .expect("ok")
                .has_errors()
        );
    }
    #[test]
    fn partial_output_record_requires_cleanup_when_cleanup_required() {
        let mut p = PartialOutputRecord::new(output_target());
        p.add_cleanup_requirement(
            CleanupRequirement::required(CleanupTargetKind::TemporaryOutput, "tmp").expect("ok"),
        );
        assert!(p.requires_cleanup());
    }
    #[test]
    fn commit_recovery_state_ambiguous_is_ambiguous_and_requires_cleanup() {
        assert!(CommitRecoveryState::Ambiguous.is_ambiguous());
        assert!(CommitRecoveryState::Ambiguous.requires_cleanup());
    }
    #[test]
    fn ambiguous_commit_record_rejects_empty_commit_id() {
        assert!(AmbiguousCommitRecord::new(" ", "x").is_err());
    }
    #[test]
    fn fault_tolerance_level_recoverable_supports_recovery() {
        assert!(FaultToleranceLevel::Recoverable.supports_recovery());
    }
    #[test]
    fn recovery_plan_recovery_not_implemented_has_errors() {
        assert!(RecoveryPlan::recovery_not_implemented("x", "y").has_errors());
    }
    #[test]
    fn recovery_plan_human_text_includes_fallback_execution_disabled() {
        assert!(
            RecoveryPlan::diagnostic_only()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn recovery_report_from_plan_does_not_execute_actions_and_preserves_status() {
        let p = RecoveryPlan::recovery_not_implemented("x", "y");
        let r = RecoveryReport::from_plan(&p);
        assert_eq!(r.status, p.status);
        assert_eq!(r.actions_completed, 0);
    }
    #[test]
    fn fault_tolerance_promotion_gate_blocks_required_execution_areas() {
        let report = plan_fault_tolerance_promotion_gate();
        assert_eq!(report.promotion_area_count(), 6);
        assert_eq!(report.blocked_area_count(), 6);
        assert_eq!(report.execution_ready_area_count(), 0);
        assert_eq!(
            report.execution_gate_order(),
            vec![
                "request_validation",
                "cancellation_signal",
                "retry_allowed",
                "checkpoint_write",
                "cleanup_execution",
                "commit_execution"
            ]
        );
        assert_eq!(report.execution_gate_blocker_count(), 11);
        assert!(report.request_validation_report_only);
        assert!(report.cancellation_signal_required);
        assert!(report.retry_policy_required);
        assert!(report.checkpoint_plan_required);
        assert!(report.cleanup_policy_required);
        assert!(report.commit_semantics_required);
        assert!(
            report
                .area_order()
                .contains(&FaultTolerancePromotionArea::RetryExecution.as_str())
        );
        assert!(
            report
                .area_order()
                .contains(&FaultTolerancePromotionArea::RecoveryExecution.as_str())
        );
    }
    #[test]
    fn fault_tolerance_promotion_gate_preserves_claim_blockers_and_no_effects() {
        let report = plan_fault_tolerance_promotion_gate();
        assert!(report.execution_promotions_blocked());
        assert!(report.exactly_once_resumability_recovery_claims_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.retry_execution_allowed);
        assert!(!report.cancellation_execution_allowed);
        assert!(!report.cleanup_execution_allowed);
        assert!(!report.checkpoint_write_allowed);
        assert!(!report.commit_execution_allowed);
        assert!(!report.ambiguous_commit_resolution_allowed);
        assert!(!report.idempotent_write_claim_allowed);
        assert!(!report.exactly_once_claim_allowed);
        assert!(!report.resumability_claim_allowed);
        assert!(!report.recovery_claim_allowed);
        assert!(!report.request_validation_performed);
        assert!(!report.cancellation_signal_consumed);
        assert!(!report.retry_execution_performed);
        assert!(!report.checkpoint_write_performed);
        assert!(!report.cleanup_execution_performed);
        assert!(!report.commit_execution_performed);
        assert!(!report.object_store_io);
        assert!(!report.output_dataset_write);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn fault_tolerance_promotion_gate_decouples_certification_evidence_from_side_effects() {
        let mut report = plan_fault_tolerance_promotion_gate();
        report.side_effect_boundaries_certified = true;
        report.commit_semantics_certified = true;

        assert!(report.exactly_once_resumability_recovery_claims_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.runtime_execution_performed);
        assert!(!report.object_store_io);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_attempted);
    }
    #[test]
    fn commit_execution_promotion_gate_tracks_broader_surfaces() {
        let report = plan_commit_execution_promotion_gate();
        assert_eq!(report.surface_count(), 13);
        assert_eq!(report.existing_limited_surface_count(), 3);
        assert_eq!(report.blocked_surface_count(), 10);
        assert_eq!(report.broader_execution_ready_surface_count(), 0);
        assert_eq!(report.gar_id, "GAR-0028-A");
        assert_eq!(
            report.support_status,
            "report_only_with_blocked_runtime_paths"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(
            report
                .surface_order()
                .contains(&CommitExecutionPromotionSurface::LocalCommittedManifestCopy.as_str())
        );
        assert!(report.surface_order().contains(
            &CommitExecutionPromotionSurface::LocalCommittedManifestRecoveryReplay.as_str()
        ));
        assert!(
            report.surface_order().contains(
                &CommitExecutionPromotionSurface::GeneralizedManifestSerialization.as_str()
            )
        );
        assert!(
            report
                .surface_order()
                .contains(&CommitExecutionPromotionSurface::ObjectStoreCommit.as_str())
        );
        assert!(
            report
                .surface_order()
                .contains(&CommitExecutionPromotionSurface::LakehouseTransactionCommit.as_str())
        );
        assert!(
            report.surface_order().contains(
                &CommitExecutionPromotionSurface::FoundryDatasetTransactionCommit.as_str()
            )
        );
        assert!(
            report.surface_order().contains(
                &CommitExecutionPromotionSurface::UpstreamVortexWriteApiExecution.as_str()
            )
        );
        assert!(
            report
                .surface_order()
                .contains(&CommitExecutionPromotionSurface::OutputPayloadFidelityClaim.as_str())
        );
    }
    #[test]
    fn commit_execution_promotion_gate_blocks_broader_execution_claims_and_effects() {
        let report = plan_commit_execution_promotion_gate();
        assert!(report.existing_local_commit_execution_present);
        assert!(report.existing_local_rollback_execution_present);
        assert!(report.existing_local_recovery_execution_present);
        assert!(report.broader_execution_promotions_blocked());
        assert!(report.commit_claims_blocked());
        assert_eq!(report.diagnostics.len(), 10);
        assert_eq!(report.unsupported_diagnostic_count(), 10);
        assert!(report.unsupported_diagnostics_propagated());
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| diagnostic.severity == DiagnosticSeverity::Info
                    && !diagnostic.fallback.attempted
                    && !diagnostic.fallback.allowed)
        );
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.broader_commit_execution_allowed);
        assert!(!report.generalized_manifest_serialization_allowed);
        assert!(!report.generalized_local_sink_commit_allowed);
        assert!(!report.object_store_commit_execution_allowed);
        assert!(!report.table_catalog_commit_execution_allowed);
        assert!(!report.lakehouse_transaction_commit_execution_allowed);
        assert!(!report.native_source_sink_commit_execution_allowed);
        assert!(!report.foundry_dataset_commit_execution_allowed);
        assert!(!report.upstream_vortex_write_api_execution_allowed);
        assert!(!report.live_hybrid_checkpoint_commit_execution_allowed);
        assert!(!report.output_payload_fidelity_claim_allowed);
        assert!(report.output_manifest_required);
        assert!(report.sink_requirement_report_required);
        assert!(report.materialization_fidelity_report_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.idempotency_key_required);
        assert!(report.rollback_recovery_proof_required);
        assert!(report.ambiguous_commit_diagnostics_required);
        assert!(report.object_store_atomicity_policy_required);
        assert!(report.table_catalog_transaction_policy_required);
        assert!(report.credential_effect_policy_required);
        assert!(report.upstream_vortex_write_api_policy_required);
        assert!(!report.runtime_execution_performed);
        assert!(!report.write_io);
        assert!(!report.object_store_io);
        assert!(!report.catalog_io);
        assert!(!report.manifest_write_io);
        assert!(!report.upstream_vortex_write_api_invoked);
        assert!(!report.external_effects_executed);
        assert!(!report.external_engine_invoked);
        assert!(!report.exactly_once_claim_allowed);
        assert!(!report.atomic_commit_claim_allowed);
        assert!(!report.recovery_claim_allowed);
        assert!(!report.lakehouse_claim_allowed);
        assert!(!report.production_output_claim_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn recovery_integration_status_cleanup_required_requires_cleanup() {
        assert!(ShardLoomRecoveryIntegrationStatus::CleanupRequired.requires_cleanup());
    }
    #[test]
    fn recovery_integration_status_retry_blocked_is_error() {
        assert!(ShardLoomRecoveryIntegrationStatus::RetryBlocked.is_error());
    }
    #[test]
    fn recovery_integration_mode_report_only_executes_cleanup_false() {
        assert!(!ShardLoomRecoveryIntegrationMode::ReportOnly.executes_cleanup());
    }
    #[test]
    fn recovery_artifact_kind_synthetic_requires_cleanup() {
        assert!(RecoveryArtifactKind::SyntheticSpillPayload.requires_cleanup());
    }
    #[test]
    fn recovery_artifact_kind_unknown_blocks_and_has_no_direct_cleanup_requirement() {
        assert!(!RecoveryArtifactKind::Unknown.requires_cleanup());
    }
    #[test]
    fn recovery_artifact_ref_unknown_has_errors() {
        let artifact = RecoveryArtifactRef::unknown("a", "unknown kind");
        assert!(artifact.has_errors());
    }
    #[test]
    fn recovery_integration_request_new_has_no_errors() {
        assert!(!ShardLoomRecoveryIntegrationRequest::new().has_errors());
    }
    #[test]
    fn recovery_integration_request_retry_option_works() {
        let req = ShardLoomRecoveryIntegrationRequest::new().retry_requested(true);
        assert!(req.has_option(RecoveryIntegrationOption::RetryRequested));
    }
    #[test]
    fn recovery_integration_request_cancellation_option_works() {
        let req = ShardLoomRecoveryIntegrationRequest::new().cancellation_requested(true);
        assert!(req.has_option(RecoveryIntegrationOption::CancellationRequested));
    }
    #[test]
    fn recovery_integration_from_request_no_artifacts_is_cleanup_not_required_side_effect_free() {
        let report = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        assert_eq!(
            report.status,
            ShardLoomRecoveryIntegrationStatus::CleanupNotRequired
        );
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn recovery_integration_with_spill_artifact_returns_cleanup_or_retry_after_cleanup() {
        let mut req = ShardLoomRecoveryIntegrationRequest::new().retry_requested(true);
        req.add_artifact(RecoveryArtifactRef::spill_workspace("ws-1", "/tmp/ws-1"));
        let report = ShardLoomRecoveryIntegrationReport::from_request(req).expect("report");
        assert_eq!(
            report.status,
            ShardLoomRecoveryIntegrationStatus::RetryAllowedAfterCleanup
        );
    }
    #[test]
    fn recovery_integration_unknown_artifact_is_blocked() {
        let mut req = ShardLoomRecoveryIntegrationRequest::new();
        req.add_artifact(RecoveryArtifactRef::unknown("u1", "not classified"));
        let report = ShardLoomRecoveryIntegrationReport::from_request(req).expect("report");
        assert_eq!(
            report.status,
            ShardLoomRecoveryIntegrationStatus::BlockedByUnknownArtifact
        );
    }
    #[test]
    fn recovery_integration_cancellation_with_unknown_artifact_is_cancellation_planned() {
        let mut req = ShardLoomRecoveryIntegrationRequest::new().cancellation_requested(true);
        req.add_artifact(RecoveryArtifactRef::unknown("u1", "not classified"));
        let report = ShardLoomRecoveryIntegrationReport::from_request(req).expect("report");
        assert_eq!(
            report.status,
            ShardLoomRecoveryIntegrationStatus::CancellationPlanned
        );
        assert_eq!(
            report.mode,
            ShardLoomRecoveryIntegrationMode::CancellationPlanning
        );
        assert_eq!(report.unknown_artifact_count, 1);
    }
    #[test]
    fn recovery_integration_report_side_effect_flags_are_false() {
        let report = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        assert!(!report.cleanup_execution.executed());
        assert!(!report.retry_execution.executed());
        assert!(!report.cancellation_execution.executed());
        assert!(!report.object_store_io.executed());
        assert!(!report.output_dataset_write.executed());
        assert!(!report.fallback_execution.allowed());
    }
    #[test]
    fn recovery_integration_human_text_contains_expected_markers() {
        let mut report = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        report.add_diagnostic(Diagnostic::invalid_input("recovery", "diag-message", "fix"));
        let text = report.to_human_text();
        assert!(text.contains("fallback execution allowed: false"));
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("cleanup executed: false"));
        assert!(text.contains("diagnostics:"));
    }
    #[test]
    fn plan_recovery_integration_is_side_effect_free() {
        let report =
            plan_recovery_integration(ShardLoomRecoveryIntegrationRequest::new()).expect("report");
        assert!(recovery_integration_is_side_effect_free(&report));
    }
    #[test]
    fn retry_cancellation_status_retry_allowed_allows_retry() {
        assert!(ShardLoomRetryCancellationStatus::RetryAllowed.allows_retry());
    }
    #[test]
    fn retry_cancellation_status_retry_allowed_after_cleanup_requires_cleanup() {
        assert!(ShardLoomRetryCancellationStatus::RetryAllowedAfterCleanup.allows_retry());
        assert!(
            ShardLoomRetryCancellationStatus::RetryAllowedAfterCleanup
                .requires_cleanup_before_retry()
        );
    }
    #[test]
    fn retry_cancellation_status_blocked_states_are_errors() {
        assert!(ShardLoomRetryCancellationStatus::RetryBlockedByCleanup.is_error());
        assert!(ShardLoomRetryCancellationStatus::RetryBlockedByExternalEffect.is_error());
    }
    #[test]
    fn retry_cancellation_status_cancellation_planned_marks_requested() {
        assert!(ShardLoomRetryCancellationStatus::CancellationPlanned.cancellation_requested());
    }
    #[test]
    fn retry_cancellation_mode_report_only_executes_are_false() {
        assert!(!ShardLoomRetryCancellationMode::ReportOnly.executes_retry());
        assert!(!ShardLoomRetryCancellationMode::ReportOnly.executes_cleanup());
    }
    #[test]
    fn retry_cancellation_request_defaults_to_no_retry_or_cancel() {
        let report = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        let req = ShardLoomRetryCancellationRequest::new(report);
        assert!(!req.has_option(RetryCancellationOption::RetryRequested));
        assert!(!req.has_option(RetryCancellationOption::CancellationRequested));
    }
    #[test]
    fn retry_cancellation_request_options_work() {
        let report = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        let req = ShardLoomRetryCancellationRequest::new(report)
            .retry_requested(true)
            .cancellation_requested(true);
        assert!(req.has_option(RetryCancellationOption::RetryRequested));
        assert!(req.has_option(RetryCancellationOption::CancellationRequested));
    }
    #[test]
    fn retry_cancellation_from_request_blocks_external_effect_retry() {
        let base = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        let report = ShardLoomRetryCancellationReport::from_request(
            ShardLoomRetryCancellationRequest::new(base)
                .retry_requested(true)
                .external_effects_present(true),
        )
        .expect("report");
        assert_eq!(
            report.status,
            ShardLoomRetryCancellationStatus::RetryBlockedByExternalEffect
        );
        assert!(!report.retry_allowed);
    }
    #[test]
    fn retry_cancellation_from_request_retry_allowed_and_cleanup_variants() {
        let base = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("report");
        let allowed = ShardLoomRetryCancellationReport::from_request(
            ShardLoomRetryCancellationRequest::new(base.clone()).retry_requested(true),
        )
        .expect("report");
        assert_eq!(
            allowed.status,
            ShardLoomRetryCancellationStatus::RetryAllowed
        );
        let mut req = ShardLoomRecoveryIntegrationRequest::new();
        req.add_artifact(RecoveryArtifactRef::spill_workspace("ws-1", "/tmp/ws-1"));
        let with_cleanup = ShardLoomRecoveryIntegrationReport::from_request(req).expect("report");
        let blocked = ShardLoomRetryCancellationReport::from_request(
            ShardLoomRetryCancellationRequest::new(with_cleanup.clone()).retry_requested(true),
        )
        .expect("report");
        assert_eq!(
            blocked.status,
            ShardLoomRetryCancellationStatus::RetryBlockedByCleanup
        );
        let allowed_after_cleanup = ShardLoomRetryCancellationReport::from_request(
            ShardLoomRetryCancellationRequest::new(with_cleanup)
                .retry_requested(true)
                .allow_retry_after_cleanup(true),
        )
        .expect("report");
        assert_eq!(
            allowed_after_cleanup.status,
            ShardLoomRetryCancellationStatus::RetryAllowedAfterCleanup
        );
    }
    #[test]
    fn cleanup_execution_status_would_execute_and_error_behaviors() {
        assert!(ShardLoomCleanupExecutionStatus::CleanupWouldExecute.cleanup_would_execute());
        assert!(ShardLoomCleanupExecutionStatus::BlockedByUnknownArtifact.is_error());
    }
    #[test]
    fn cleanup_execution_mode_report_only_methods_are_false() {
        assert!(!ShardLoomCleanupExecutionMode::ReportOnly.executes_cleanup());
        assert!(!ShardLoomCleanupExecutionMode::ReportOnly.executes_retry());
        assert!(!ShardLoomCleanupExecutionMode::ReportOnly.touches_object_store());
    }
    #[test]
    fn cleanup_execution_request_option_defaults_and_setter_work() {
        let request = ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::unknown("a", "u"));
        assert!(!request.has_option(CleanupExecutionOption::AllowSyntheticPayloadCleanup));
        let request = request.allow_synthetic_payload_cleanup(true);
        assert!(request.has_option(CleanupExecutionOption::AllowSyntheticPayloadCleanup));
    }
    #[test]
    fn cleanup_execution_from_request_unknown_and_synthetic_policy() {
        let unknown = ShardLoomCleanupExecutionReport::from_request(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::unknown("u1", "unknown")),
        )
        .expect("report");
        assert_eq!(
            unknown.status,
            ShardLoomCleanupExecutionStatus::BlockedByUnknownArtifact
        );
        let payload_ref = spill_payload_fs_ref("p1", "/tmp/p1");
        let blocked =
            ShardLoomCleanupExecutionReport::from_request(ShardLoomCleanupExecutionRequest::new(
                RecoveryArtifactRef::synthetic_spill_payload(&payload_ref),
            ))
            .expect("report");
        assert_eq!(
            blocked.status,
            ShardLoomCleanupExecutionStatus::BlockedByPolicy
        );
        let missing_ref = ShardLoomCleanupExecutionReport::from_request(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &payload_ref,
            ))
            .allow_synthetic_payload_cleanup(true),
        )
        .expect("report");
        assert_eq!(
            missing_ref.status,
            ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact
        );
        let allowed = ShardLoomCleanupExecutionReport::from_request(
            ShardLoomCleanupExecutionRequest::synthetic_payload(
                RecoveryArtifactRef::synthetic_spill_payload(&payload_ref),
                payload_ref.clone(),
            )
            .allow_synthetic_payload_cleanup(true),
        )
        .expect("report");
        assert_eq!(
            allowed.status,
            ShardLoomCleanupExecutionStatus::CleanupWouldExecute
        );
    }
    #[test]
    fn cleanup_execution_from_request_blocks_mismatched_payload_reference() {
        let artifact_ref = spill_payload_fs_ref("artifact-a", "/tmp/p1");
        let mismatched_ref = spill_payload_fs_ref("payload-b", "/tmp/p1");
        let report = ShardLoomCleanupExecutionReport::from_request(
            ShardLoomCleanupExecutionRequest::synthetic_payload(
                RecoveryArtifactRef::synthetic_spill_payload(&artifact_ref),
                mismatched_ref,
            )
            .allow_synthetic_payload_cleanup(true),
        )
        .expect("report");
        assert_eq!(
            report.status,
            ShardLoomCleanupExecutionStatus::BlockedByMissingArtifact
        );
    }
    #[cfg(feature = "spill-payload-fs")]
    #[test]
    fn cleanup_execution_feature_removes_only_target_file() {
        use crate::{SpillPayloadWriteRequest, SyntheticSpillPayload};
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let workspace = std::env::temp_dir().join(format!("shardloom-cleanup-{unique}"));
        fs::create_dir_all(&workspace).expect("workspace");
        let workspace_str = workspace.to_string_lossy().into_owned();
        let fs_ref = spill_payload_fs_ref("p-cleanup", &workspace_str);
        let sibling = workspace.join("sibling.bin");
        fs::write(&sibling, b"sibling").expect("sibling");
        let payload = SyntheticSpillPayload::from_bytes(vec![1, 2, 3, 4]).expect("payload");
        let write_report = crate::write_spill_payload(
            SpillPayloadWriteRequest::new(fs_ref.clone(), payload)
                .create_workspace(true)
                .allow_overwrite(true),
        )
        .expect("write");
        assert!(write_report.payload_written());
        let target = workspace.join(fs_ref.file_name());
        assert!(target.exists());
        let report = execute_cleanup_plan(
            ShardLoomCleanupExecutionRequest::synthetic_payload(
                RecoveryArtifactRef::synthetic_spill_payload(&fs_ref),
                fs_ref.clone(),
            )
            .allow_synthetic_payload_cleanup(true),
        )
        .expect("report");
        assert_eq!(
            report.status,
            ShardLoomCleanupExecutionStatus::CleanupCompleted
        );
        assert!(report.cleanup_executed());
        assert!(!report.retry_executed());
        assert!(!report.cancellation_executed());
        assert!(!report.external_effects_executed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(!target.exists());
        assert!(sibling.exists());
        assert!(workspace.exists());
        let _ = fs::remove_file(&sibling);
        let _ = fs::remove_dir(&workspace);
    }
    #[test]
    fn cleanup_execution_report_side_effects_and_human_text() {
        let payload_ref = spill_payload_fs_ref("p1", "/tmp/p1");
        let mut report = ShardLoomCleanupExecutionReport::from_request(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &payload_ref,
            ))
            .allow_synthetic_payload_cleanup(true),
        )
        .expect("report");
        assert!(!report.cleanup_executed());
        assert!(!report.retry_executed());
        assert!(!report.cancellation_executed());
        assert!(!report.external_effects_executed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        report.add_diagnostic(unsupported_diagnostic("cleanup_test", "diag-message"));
        let text = report.to_human_text();
        assert!(text.contains("fallback execution allowed: false"));
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("cleanup executed: false"));
        assert!(text.contains("diagnostics:"));
        assert!(text.contains("- "));
    }
    #[test]
    fn cleanup_execution_report_human_text_reflects_effects() {
        let request =
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::unknown("u3", "unknown"));
        let mut report = ShardLoomCleanupExecutionReport::planned(request);
        report.effects_performed.extend([
            ShardLoomCleanupExecutionEffect::CleanupExecuted,
            ShardLoomCleanupExecutionEffect::RetryExecuted,
            ShardLoomCleanupExecutionEffect::CancellationExecuted,
            ShardLoomCleanupExecutionEffect::FallbackExecution,
        ]);
        assert!(report.cleanup_executed());
        assert!(report.retry_executed());
        assert!(report.cancellation_executed());
        assert!(report.fallback_execution_allowed());
        assert!(!report.is_side_effect_free());
        let text = report.to_human_text();
        assert!(text.contains("cleanup executed: true"));
        assert!(text.contains("retry executed: true"));
        assert!(text.contains("cancellation executed: true"));
        assert!(text.contains("fallback execution allowed: true"));
        assert!(!text.contains("fallback execution: disabled"));
    }
    #[test]
    fn cleanup_execution_report_normal_states_keep_fallback_disabled() {
        let planned = ShardLoomCleanupExecutionReport::planned(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::unknown("u4", "unknown")),
        );
        let planned_text = planned.to_human_text();
        assert!(!planned.fallback_execution_allowed());
        assert!(planned.is_side_effect_free());
        assert!(planned_text.contains("fallback execution allowed: false"));
        assert!(planned_text.contains("fallback execution: disabled"));

        let unsupported = ShardLoomCleanupExecutionReport::unsupported(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::unknown("u5", "unknown")),
            "cleanup_exec",
            "unsupported",
        );
        let unsupported_text = unsupported.to_human_text();
        assert!(!unsupported.fallback_execution_allowed());
        assert!(unsupported_text.contains("fallback execution allowed: false"));
        assert!(unsupported_text.contains("fallback execution: disabled"));
    }
    #[test]
    fn cleanup_execution_unsupported_and_helpers() {
        let unsupported = ShardLoomCleanupExecutionReport::unsupported(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::unknown("u2", "unknown")),
            "cleanup_exec",
            "unsupported",
        );
        assert!(unsupported.has_errors());
        assert!(!unsupported.fallback_execution_allowed());
        let report = plan_cleanup_execution(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &spill_payload_fs_ref("p2", "/tmp/p2"),
            ))
            .allow_synthetic_payload_cleanup(true),
        )
        .expect("report");
        assert!(cleanup_execution_plan_is_side_effect_free(&report));
    }

    #[test]
    fn retry_execution_gate_status_and_mode_basics() {
        assert!(ShardLoomRetryExecutionGateStatus::GateOpen.gate_open());
        assert!(ShardLoomRetryExecutionGateStatus::GateClosedCleanupRequired.is_error());
        assert!(ShardLoomRetryExecutionGateStatus::GateClosedCleanupRequired.requires_cleanup());
        assert!(!ShardLoomRetryExecutionGateMode::ReportOnly.executes_retry());
    }

    #[test]
    fn retry_execution_gate_request_builders() {
        let request = ShardLoomRetryExecutionGateRequest::new();
        assert!(request.signals.is_empty());
        let request = request.retry_requested(true).cleanup_completed(true);
        assert!(request.has_signal(ShardLoomRetryExecutionGateSignal::RetryRequested));
        assert!(request.has_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted));
    }

    #[test]
    fn retry_execution_gate_from_request_statuses() {
        let not_requested = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new(),
        )
        .expect("report");
        assert_eq!(
            not_requested.status,
            ShardLoomRetryExecutionGateStatus::GateClosedRetryNotRequested
        );
        assert!(!not_requested.has_errors());
        let not_allowed = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new().retry_requested(true),
        )
        .expect("report");
        assert_eq!(
            not_allowed.status,
            ShardLoomRetryExecutionGateStatus::GateClosedRetryNotAllowed
        );
        let cleanup_required = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .retry_requires_cleanup(true),
        )
        .expect("report");
        assert_eq!(
            cleanup_required.status,
            ShardLoomRetryExecutionGateStatus::GateClosedCleanupRequired
        );
    }

    #[test]
    fn retry_execution_gate_from_request_open_states() {
        let open_no_cleanup = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true),
        )
        .expect("report");
        assert!(open_no_cleanup.retry_gate_open());
        let open_cleanup_completed = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .retry_requires_cleanup(true)
                .cleanup_completed(true),
        )
        .expect("report");
        assert!(open_cleanup_completed.retry_gate_open());
    }

    #[test]
    fn retry_execution_gate_from_request_blocking_priority() {
        let unknown = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .unknown_artifact_present(true),
        )
        .expect("report");
        assert_eq!(
            unknown.status,
            ShardLoomRetryExecutionGateStatus::GateClosedUnknownArtifact
        );
        let external = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .external_effects_present(true),
        )
        .expect("report");
        assert_eq!(
            external.status,
            ShardLoomRetryExecutionGateStatus::GateClosedExternalEffect
        );
        let cancellation = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .cancellation_requested(true),
        )
        .expect("report");
        assert_eq!(
            cancellation.status,
            ShardLoomRetryExecutionGateStatus::GateClosedCancellationRequested
        );
    }

    #[test]
    fn retry_execution_gate_from_request_object_store_and_output_recovery() {
        let object_store = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .object_store_recovery_required(true),
        )
        .expect("report");
        assert_eq!(
            object_store.status,
            ShardLoomRetryExecutionGateStatus::GateClosedObjectStoreRecovery
        );
        let output = ShardLoomRetryExecutionGateReport::from_request(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true)
                .output_recovery_required(true),
        )
        .expect("report");
        assert_eq!(
            output.status,
            ShardLoomRetryExecutionGateStatus::GateClosedOutputRecovery
        );
    }

    #[test]
    fn retry_execution_gate_report_effect_defaults_and_human_text() {
        let mut request = ShardLoomRetryExecutionGateRequest::new()
            .retry_requested(true)
            .retry_allowed_by_plan(true);
        request.add_diagnostic(Diagnostic::no_fallback_execution("diag"));
        let report = ShardLoomRetryExecutionGateReport::from_request(request).expect("report");
        assert!(!report.retry_executed());
        assert!(!report.cleanup_executed_by_gate());
        assert!(!report.cancellation_executed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("retry executed: false"));
        assert!(text.contains("diag"));
    }

    #[test]
    fn retry_execution_gate_helpers_are_side_effect_free() {
        let report = plan_retry_execution_gate(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true),
        )
        .expect("report");
        assert!(retry_execution_gate_is_side_effect_free(&report));
    }

    fn retry_report_with_options(
        recovery_request: ShardLoomRecoveryIntegrationRequest,
        request: ShardLoomRetryCancellationRequest,
    ) -> ShardLoomRetryCancellationReport {
        let recovery_report =
            ShardLoomRecoveryIntegrationReport::from_request(recovery_request).expect("report");
        let request = ShardLoomRetryCancellationRequest {
            recovery_report,
            ..request
        };
        ShardLoomRetryCancellationReport::from_request(request).expect("report")
    }

    #[test]
    fn retry_gate_request_from_retry_report_maps_core_signals() {
        let report = retry_report_with_options(
            ShardLoomRecoveryIntegrationRequest::new(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true),
        );
        let request = retry_gate_request_from_retry_cancellation_report(&report);
        assert!(request.has_signal(ShardLoomRetryExecutionGateSignal::RetryRequested));
        assert!(request.has_signal(ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan));
    }

    #[test]
    fn retry_gate_request_from_retry_report_maps_cleanup_unknown_external_signals() {
        let mut cleanup_recovery = ShardLoomRecoveryIntegrationRequest::new();
        cleanup_recovery.add_artifact(RecoveryArtifactRef::spill_workspace("ws", "/tmp/ws"));
        let cleanup_report = retry_report_with_options(
            cleanup_recovery,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true)
            .allow_retry_after_cleanup(true),
        );
        let cleanup_request = retry_gate_request_from_retry_cancellation_report(&cleanup_report);
        assert!(
            cleanup_request.has_signal(ShardLoomRetryExecutionGateSignal::RetryRequiresCleanup)
        );

        let mut unknown_recovery = ShardLoomRecoveryIntegrationRequest::new();
        unknown_recovery.add_artifact(RecoveryArtifactRef::unknown("u1", "unknown"));
        let unknown_report = retry_report_with_options(
            unknown_recovery,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true),
        );
        let unknown_request = retry_gate_request_from_retry_cancellation_report(&unknown_report);
        assert!(
            unknown_request.has_signal(ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent)
        );

        let external_report = retry_report_with_options(
            ShardLoomRecoveryIntegrationRequest::new(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true)
            .external_effects_present(true),
        );
        let external_request = retry_gate_request_from_retry_cancellation_report(&external_report);
        assert!(
            external_request.has_signal(ShardLoomRetryExecutionGateSignal::ExternalEffectsPresent)
        );
    }

    #[test]
    fn retry_gate_request_with_cleanup_report_only_sets_cleanup_completed_when_executed() {
        let base = ShardLoomRetryExecutionGateRequest::new();
        let payload_ref = spill_payload_fs_ref("p1", "/tmp/p1");
        let executed = ShardLoomCleanupExecutionReport::cleanup_completed(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &payload_ref,
            )),
        );
        let with_exec = retry_gate_request_with_cleanup_report(base.clone(), &executed);
        assert!(with_exec.has_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted));

        let not_executed = ShardLoomCleanupExecutionReport::blocked(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &payload_ref,
            )),
            ShardLoomCleanupExecutionStatus::FeatureDisabled,
            "disabled",
        );
        let without_exec = retry_gate_request_with_cleanup_report(base, &not_executed);
        assert!(!without_exec.has_signal(ShardLoomRetryExecutionGateSignal::CleanupCompleted));
    }

    #[test]
    fn plan_retry_execution_gate_from_reports_enforces_expected_states() {
        let mut cleanup_needed = ShardLoomRecoveryIntegrationRequest::new();
        cleanup_needed.add_artifact(RecoveryArtifactRef::spill_workspace("ws2", "/tmp/ws2"));
        let retry_report = retry_report_with_options(
            cleanup_needed,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true)
            .allow_retry_after_cleanup(true),
        );
        let no_cleanup =
            plan_retry_execution_gate_from_reports(&retry_report, None).expect("report");
        assert_eq!(
            no_cleanup.status,
            ShardLoomRetryExecutionGateStatus::GateClosedCleanupRequired
        );

        let cleanup_not_run = ShardLoomCleanupExecutionReport::cleanup_would_execute(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &spill_payload_fs_ref("p2", "/tmp/p2"),
            )),
        );
        let blocked = plan_retry_execution_gate_from_reports(&retry_report, Some(&cleanup_not_run))
            .expect("report");
        assert_eq!(
            blocked.status,
            ShardLoomRetryExecutionGateStatus::GateClosedCleanupRequired
        );

        let cleanup_done = ShardLoomCleanupExecutionReport::cleanup_completed(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &spill_payload_fs_ref("p2", "/tmp/p2"),
            )),
        );
        let open = plan_retry_execution_gate_from_reports(&retry_report, Some(&cleanup_done))
            .expect("report");
        assert_eq!(open.status, ShardLoomRetryExecutionGateStatus::GateOpen);
        assert!(open.is_side_effect_free());
        assert!(!open.retry_executed());
        assert!(!open.cleanup_executed_by_gate());
        assert!(!open.cancellation_executed());
        assert!(!open.object_store_io());
        assert!(!open.output_dataset_write());
        assert!(!open.fallback_execution_allowed());
        let text = open.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
    }
}

#[cfg(test)]
mod cancellation_execution_gate_tests {
    use super::*;
    use crate::{SpillPayloadFsRef, SpillPayloadId, SpillPayloadPath, SpillPayloadRef};

    fn spill_payload_fs_ref(payload_id: &str, workspace: &str) -> SpillPayloadFsRef {
        let payload_id = SpillPayloadId::new(payload_id).expect("payload id");
        let payload_ref = SpillPayloadRef::new(payload_id, workspace).expect("payload ref");
        let workspace_path = SpillPayloadPath::new(workspace).expect("workspace path");
        SpillPayloadFsRef::new(payload_ref, workspace_path)
    }

    fn retry_report_with_options(
        recovery_request: ShardLoomRecoveryIntegrationRequest,
        request: ShardLoomRetryCancellationRequest,
    ) -> ShardLoomRetryCancellationReport {
        let recovery_report =
            ShardLoomRecoveryIntegrationReport::from_request(recovery_request).expect("report");
        let request = ShardLoomRetryCancellationRequest {
            recovery_report,
            ..request
        };
        ShardLoomRetryCancellationReport::from_request(request).expect("report")
    }

    #[test]
    fn cancellation_execution_gate_status_and_mode_basics() {
        assert!(ShardLoomCancellationExecutionGateStatus::GateOpen.gate_open());
        assert!(
            !ShardLoomCancellationExecutionGateStatus::GateClosedCancellationNotRequested
                .is_error()
        );
        assert!(ShardLoomCancellationExecutionGateStatus::GateClosedCleanupRequired.is_error());
        assert!(
            ShardLoomCancellationExecutionGateStatus::GateClosedCleanupRequired.requires_cleanup()
        );
        assert!(!ShardLoomCancellationExecutionGateMode::ReportOnly.executes_cancellation());
    }

    #[test]
    fn cancellation_execution_gate_request_builders_work() {
        let request = ShardLoomCancellationExecutionGateRequest::new();
        assert!(request.signals.is_empty());
        let request = request.cancellation_requested(true).cleanup_completed(true);
        assert!(
            request.has_signal(ShardLoomCancellationExecutionGateSignal::CancellationRequested)
        );
        assert!(request.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted));
    }

    #[test]
    fn cancellation_execution_gate_status_priority_and_open_cases() {
        let not_requested = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new().cleanup_required(true),
        )
        .expect("report");
        assert_eq!(
            not_requested.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedCancellationNotRequested
        );

        let cleanup_required = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .cleanup_required(true),
        )
        .expect("report");
        assert_eq!(
            cleanup_required.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedCleanupRequired
        );

        let open_no_cleanup = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new().cancellation_requested(true),
        )
        .expect("report");
        assert_eq!(
            open_no_cleanup.status,
            ShardLoomCancellationExecutionGateStatus::GateOpen
        );

        let open_cleanup_done = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .cleanup_required(true)
                .cleanup_completed(true),
        )
        .expect("report");
        assert_eq!(
            open_cleanup_done.status,
            ShardLoomCancellationExecutionGateStatus::GateOpen
        );
    }

    #[test]
    fn cancellation_execution_gate_blocking_signals_close_gate() {
        let unknown = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .unknown_artifact_present(true),
        )
        .expect("report");
        assert_eq!(
            unknown.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedUnknownArtifact
        );

        let external = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .external_effects_present(true),
        )
        .expect("report");
        assert_eq!(
            external.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedExternalEffect
        );

        let retry = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .retry_in_progress(true),
        )
        .expect("report");
        assert_eq!(
            retry.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedRetryInProgress
        );

        let object_store = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .object_store_recovery_required(true),
        )
        .expect("report");
        assert_eq!(
            object_store.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedObjectStoreRecovery
        );

        let output = ShardLoomCancellationExecutionGateReport::from_request(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .output_recovery_required(true),
        )
        .expect("report");
        assert_eq!(
            output.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedOutputRecovery
        );
    }

    #[test]
    fn cancellation_execution_gate_side_effect_fields_and_text() {
        let mut request =
            ShardLoomCancellationExecutionGateRequest::new().cancellation_requested(true);
        request.add_diagnostic(unsupported_diagnostic("cancel", "diag-message"));
        let report =
            ShardLoomCancellationExecutionGateReport::from_request(request).expect("report");
        assert!(!report.cancellation_executed());
        assert!(!report.retry_executed());
        assert!(!report.cleanup_executed_by_gate());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());

        let text = report.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("cancellation executed: false"));
        assert!(text.contains("diag-message"));

        let via_plan = plan_cancellation_execution_gate(
            ShardLoomCancellationExecutionGateRequest::new().cancellation_requested(true),
        )
        .expect("report");
        assert!(cancellation_execution_gate_is_side_effect_free(&via_plan));
    }

    #[test]
    fn cancellation_gate_request_from_retry_report_maps_expected_signals() {
        let requested = retry_report_with_options(
            ShardLoomRecoveryIntegrationRequest::new(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true),
        );
        let request = cancellation_gate_request_from_retry_cancellation_report(&requested);
        assert!(
            request.has_signal(ShardLoomCancellationExecutionGateSignal::CancellationRequested)
        );

        let mut cleanup_needed = ShardLoomRecoveryIntegrationRequest::new();
        cleanup_needed.add_artifact(RecoveryArtifactRef::spill_workspace("ws-c", "/tmp/ws-c"));
        let cleanup_report = retry_report_with_options(
            cleanup_needed.clone(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true)
            .cancellation_requested(true)
            .allow_retry_after_cleanup(true),
        );
        let cleanup_request =
            cancellation_gate_request_from_retry_cancellation_report(&cleanup_report);
        assert!(
            cleanup_request.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupRequired)
        );

        let cancellation_only_cleanup_report = retry_report_with_options(
            cleanup_needed,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true),
        );
        let cancellation_only_cleanup_request =
            cancellation_gate_request_from_retry_cancellation_report(
                &cancellation_only_cleanup_report,
            );
        assert!(
            cancellation_only_cleanup_request
                .has_signal(ShardLoomCancellationExecutionGateSignal::CleanupRequired)
        );

        let mut unknown = ShardLoomRecoveryIntegrationRequest::new();
        unknown.add_artifact(RecoveryArtifactRef::unknown("u-c", "unknown"));
        let unknown_report = retry_report_with_options(
            unknown,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true),
        );
        let unknown_request =
            cancellation_gate_request_from_retry_cancellation_report(&unknown_report);
        assert!(
            unknown_request
                .has_signal(ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent)
        );

        let external_report = retry_report_with_options(
            ShardLoomRecoveryIntegrationRequest::new(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true)
            .external_effects_present(true),
        );
        let external_request =
            cancellation_gate_request_from_retry_cancellation_report(&external_report);
        assert!(
            external_request
                .has_signal(ShardLoomCancellationExecutionGateSignal::ExternalEffectsPresent)
        );
    }

    #[test]
    fn cancellation_gate_request_from_retry_report_maps_cleanup_completed_signal() {
        let report = retry_report_with_options(
            ShardLoomRecoveryIntegrationRequest::new(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true)
            .cleanup_already_completed(true),
        );
        let request = cancellation_gate_request_from_retry_cancellation_report(&report);
        assert!(request.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted));
    }

    #[test]
    fn cancellation_gate_request_with_cleanup_report_sets_completion_only_when_executed() {
        let base = ShardLoomCancellationExecutionGateRequest::new();
        let payload_ref = spill_payload_fs_ref("p-c", "/tmp/p-c");
        let executed = ShardLoomCleanupExecutionReport::cleanup_completed(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &payload_ref,
            )),
        );
        let with_exec = cancellation_gate_request_with_cleanup_report(base.clone(), &executed);
        assert!(with_exec.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted));

        let not_executed = ShardLoomCleanupExecutionReport::blocked(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &payload_ref,
            )),
            ShardLoomCleanupExecutionStatus::FeatureDisabled,
            "disabled",
        );
        let without_exec = cancellation_gate_request_with_cleanup_report(base, &not_executed);
        assert!(
            !without_exec.has_signal(ShardLoomCancellationExecutionGateSignal::CleanupCompleted)
        );
    }

    #[test]
    fn plan_cancellation_execution_gate_from_reports_enforces_expected_states() {
        let mut cleanup_needed = ShardLoomRecoveryIntegrationRequest::new();
        cleanup_needed.add_artifact(RecoveryArtifactRef::spill_workspace("ws-cc", "/tmp/ws-cc"));
        let retry_report = retry_report_with_options(
            cleanup_needed,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .retry_requested(true)
            .cancellation_requested(true)
            .allow_retry_after_cleanup(true),
        );
        let no_cleanup =
            plan_cancellation_execution_gate_from_reports(&retry_report, None).expect("report");
        assert_eq!(
            no_cleanup.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedCleanupRequired
        );

        let cleanup_not_run = ShardLoomCleanupExecutionReport::cleanup_would_execute(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &spill_payload_fs_ref("p-cc", "/tmp/p-cc"),
            )),
        );
        let blocked =
            plan_cancellation_execution_gate_from_reports(&retry_report, Some(&cleanup_not_run))
                .expect("report");
        assert_eq!(
            blocked.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedCleanupRequired
        );

        let cleanup_done = ShardLoomCleanupExecutionReport::cleanup_completed(
            ShardLoomCleanupExecutionRequest::new(RecoveryArtifactRef::synthetic_spill_payload(
                &spill_payload_fs_ref("p-cc", "/tmp/p-cc"),
            )),
        );
        let open =
            plan_cancellation_execution_gate_from_reports(&retry_report, Some(&cleanup_done))
                .expect("report");
        assert_eq!(
            open.status,
            ShardLoomCancellationExecutionGateStatus::GateOpen
        );
        assert!(open.is_side_effect_free());
        assert!(!open.cancellation_executed());
        assert!(!open.retry_executed());
        assert!(!open.cleanup_executed_by_gate());
        assert!(!open.object_store_io());
        assert!(!open.output_dataset_write());
        assert!(!open.fallback_execution_allowed());
        let text = open.to_human_text();
        assert!(text.contains("fallback execution: disabled"));

        let mut unknown = ShardLoomRecoveryIntegrationRequest::new();
        unknown.add_artifact(RecoveryArtifactRef::unknown("u-cc", "unknown"));
        let unknown_retry_report = retry_report_with_options(
            unknown,
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true),
        );
        let unknown_closed = plan_cancellation_execution_gate_from_reports(
            &unknown_retry_report,
            Some(&cleanup_done),
        )
        .expect("report");
        assert_eq!(
            unknown_closed.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedUnknownArtifact
        );

        let external_report = retry_report_with_options(
            ShardLoomRecoveryIntegrationRequest::new(),
            ShardLoomRetryCancellationRequest::new(
                ShardLoomRecoveryIntegrationReport::from_request(
                    ShardLoomRecoveryIntegrationRequest::new(),
                )
                .expect("report"),
            )
            .cancellation_requested(true)
            .external_effects_present(true),
        );
        let external_closed =
            plan_cancellation_execution_gate_from_reports(&external_report, None).expect("report");
        assert_eq!(
            external_closed.status,
            ShardLoomCancellationExecutionGateStatus::GateClosedExternalEffect
        );
    }
}
