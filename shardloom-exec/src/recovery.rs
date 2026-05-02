//! Fault-tolerance, cancellation, retry, and recovery planning skeletons.
//!
//! This module is planning/reporting-only and intentionally performs no retry,
//! cancellation propagation, recovery execution, cleanup IO, or commit protocol IO.
#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_errors_doc
)]

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, OutputTarget, Result, ShardLoomError,
};

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
    use super::*;
    use shardloom_core::DatasetUri;
    fn task_id() -> TaskId {
        TaskId::new("task-1").expect("task")
    }
    fn output_target() -> OutputTarget {
        OutputTarget::from_uri(DatasetUri::new("file://tmp/out.vortex").expect("uri"))
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
}
