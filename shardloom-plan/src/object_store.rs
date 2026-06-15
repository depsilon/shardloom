//! Object-store range planning evidence.
//!
//! This module plans request shapes from already-declared manifest byte-range metadata.
//! It performs no object-store IO, no file reads, no data materialization, and no fallback execution.

use std::{collections::BTreeSet, fmt::Write as _};

use shardloom_core::{
    ByteRange, DatasetManifest, DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode,
    DiagnosticSeverity, FallbackStatus, FileDescriptor, FileRole, SegmentId, UriScheme,
};

/// Report-only policy for object-store range planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreRangePlanningPolicy {
    pub max_ranges_per_request: usize,
    pub max_request_bytes: u64,
    pub coalesce_adjacent_ranges: bool,
    pub max_coalesce_gap_bytes: u64,
}

impl Default for ObjectStoreRangePlanningPolicy {
    fn default() -> Self {
        Self {
            max_ranges_per_request: 8,
            max_request_bytes: 16 * 1024 * 1024,
            coalesce_adjacent_ranges: true,
            max_coalesce_gap_bytes: 4096,
        }
    }
}

/// Object-store range planning status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRangePlanningStatus {
    Planned,
    BlockedMissingByteRanges,
    BlockedInvalidRanges,
    BlockedRequestBudget,
    BlockedNonObjectStore,
    Unsupported,
}

impl ObjectStoreRangePlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::BlockedMissingByteRanges => "blocked_missing_byte_ranges",
            Self::BlockedInvalidRanges => "blocked_invalid_ranges",
            Self::BlockedRequestBudget => "blocked_request_budget",
            Self::BlockedNonObjectStore => "blocked_non_object_store",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned)
    }
}

/// Planned object-store range request shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreRangeRequest {
    pub uri: DatasetUri,
    pub segment_ids: Vec<SegmentId>,
    pub range: ByteRange,
    pub source_range_count: usize,
}

impl ObjectStoreRangeRequest {
    #[must_use]
    pub fn new(
        uri: DatasetUri,
        segment_id: SegmentId,
        range: ByteRange,
    ) -> ObjectStoreRangeRequest {
        Self {
            uri,
            segment_ids: vec![segment_id],
            range,
            source_range_count: 1,
        }
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> u64 {
        self.range.length
    }
}

/// Machine-readable CG-10 object-store range planning evidence.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRangePlanningReport {
    pub manifest: DatasetManifest,
    pub policy: ObjectStoreRangePlanningPolicy,
    pub status: ObjectStoreRangePlanningStatus,
    pub requests: Vec<ObjectStoreRangeRequest>,
    pub diagnostics: Vec<Diagnostic>,
    pub file_count: usize,
    pub segment_count: usize,
    pub object_store_file_count: usize,
    pub non_object_store_file_count: usize,
    pub ranged_segment_count: usize,
    pub missing_byte_range_segment_count: usize,
    pub invalid_range_count: usize,
    pub oversized_range_count: usize,
    pub planned_request_count: usize,
    pub planned_range_count: usize,
    pub coalesced_range_count: usize,
    pub estimated_request_bytes: u64,
    pub requires_byte_ranges: bool,
    pub requires_request_budget_review: bool,
    pub full_file_read_required: bool,
    pub full_file_read_allowed: bool,
    pub can_plan_without_io: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreRangePlanningReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.fallback_execution_allowed
            || self.object_store_io
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_range_plan(status={}, object_store_files={}, segments={}, planned_requests={}, planned_ranges={}, estimated_request_bytes={}, data_read=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.object_store_file_count,
            self.segment_count,
            self.planned_request_count,
            self.planned_range_count,
            self.estimated_request_bytes
        )
    }
}

/// Request coalescing status derived from object-store range planning evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRequestCoalescingStatus {
    Planned,
    NoCoalescingNeeded,
    BlockedByRangePlanning,
}

impl ObjectStoreRequestCoalescingStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::NoCoalescingNeeded => "no_coalescing_needed",
            Self::BlockedByRangePlanning => "blocked_by_range_planning",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::BlockedByRangePlanning)
    }
}

/// Request coalescing decision family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRequestCoalescingDecisionKind {
    CoalesceAdjacentRanges,
    KeepSeparate,
    Blocked,
}

impl ObjectStoreRequestCoalescingDecisionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CoalesceAdjacentRanges => "coalesce_adjacent_ranges",
            Self::KeepSeparate => "keep_separate",
            Self::Blocked => "blocked",
        }
    }
}

/// Report-only coalescing decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreRequestCoalescingDecision {
    pub kind: ObjectStoreRequestCoalescingDecisionKind,
    pub input_request_count: usize,
    pub output_request_count: usize,
    pub coalesced_range_count: usize,
    pub reason: String,
}

impl ObjectStoreRequestCoalescingDecision {
    #[must_use]
    pub fn new(
        kind: ObjectStoreRequestCoalescingDecisionKind,
        input_request_count: usize,
        output_request_count: usize,
        coalesced_range_count: usize,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            input_request_count,
            output_request_count,
            coalesced_range_count,
            reason: reason.into(),
        }
    }
}

/// Machine-readable CG-10 request coalescing evidence.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRequestCoalescingReport {
    pub uncoalesced_range_report: ObjectStoreRangePlanningReport,
    pub coalesced_range_report: ObjectStoreRangePlanningReport,
    pub status: ObjectStoreRequestCoalescingStatus,
    pub decisions: Vec<ObjectStoreRequestCoalescingDecision>,
    pub diagnostics: Vec<Diagnostic>,
    pub input_request_count: usize,
    pub output_request_count: usize,
    pub request_reduction_count: usize,
    pub input_range_count: usize,
    pub coalesced_range_count: usize,
    pub estimated_request_bytes_before: u64,
    pub estimated_request_bytes_after: u64,
    pub coalescing_applied: bool,
    pub can_plan_without_io: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreRequestCoalescingReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.fallback_execution_allowed
            || self.object_store_io
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_request_coalescing(status={}, input_requests={}, output_requests={}, request_reduction={}, coalesced_ranges={}, data_read=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.input_request_count,
            self.output_request_count,
            self.request_reduction_count,
            self.coalesced_range_count
        )
    }
}

/// Report-only policy for object-store distributed task-shape planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreDistributedSchedulingPolicy {
    pub max_requests_per_task: usize,
    pub max_task_count: usize,
}

impl Default for ObjectStoreDistributedSchedulingPolicy {
    fn default() -> Self {
        Self {
            max_requests_per_task: 4,
            max_task_count: 128,
        }
    }
}

impl ObjectStoreDistributedSchedulingPolicy {
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.max_requests_per_task > 0 && self.max_task_count > 0
    }
}

/// Object-store distributed scheduling planning status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreDistributedSchedulingStatus {
    Planned,
    BlockedByCoalescing,
    BlockedEmptyRequests,
    BlockedTaskBudget,
    BlockedInvalidPolicy,
}

impl ObjectStoreDistributedSchedulingStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::BlockedByCoalescing => "blocked_by_coalescing",
            Self::BlockedEmptyRequests => "blocked_empty_requests",
            Self::BlockedTaskBudget => "blocked_task_budget",
            Self::BlockedInvalidPolicy => "blocked_invalid_policy",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned)
    }
}

/// Planned distributed task shape derived from object-store request evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreDistributedTaskPlan {
    pub task_id: String,
    pub request_start_index: usize,
    pub request_count: usize,
    pub range_count: usize,
    pub uri_count: usize,
    pub estimated_request_bytes: u64,
    pub requires_retry_identity: bool,
    pub requires_checkpoint_record: bool,
    pub requires_idempotency_key: bool,
    pub task_execution_allowed: bool,
    pub object_store_io: bool,
    pub write_io: bool,
}

/// Machine-readable CG-10 object-store distributed scheduling evidence.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreDistributedSchedulingReport {
    pub coalescing_report: ObjectStoreRequestCoalescingReport,
    pub policy: ObjectStoreDistributedSchedulingPolicy,
    pub status: ObjectStoreDistributedSchedulingStatus,
    pub tasks: Vec<ObjectStoreDistributedTaskPlan>,
    pub diagnostics: Vec<Diagnostic>,
    pub input_request_count: usize,
    pub planned_task_count: usize,
    pub estimated_request_bytes: u64,
    pub requires_checkpoint_plan: bool,
    pub requires_retry_policy: bool,
    pub requires_idempotency_keys: bool,
    pub scheduler_execution_allowed: bool,
    pub coordinator_started: bool,
    pub worker_started: bool,
    pub task_execution_allowed: bool,
    pub can_plan_without_io: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreDistributedSchedulingReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.scheduler_execution_allowed
            || self.coordinator_started
            || self.worker_started
            || self.task_execution_allowed
            || self.object_store_io
            || self.write_io
            || self.fallback_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.scheduler_execution_allowed
            && !self.coordinator_started
            && !self.worker_started
            && !self.task_execution_allowed
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_distributed_scheduling(status={}, input_requests={}, planned_tasks={}, estimated_request_bytes={}, scheduler_execution=false, coordinator_started=false, worker_started=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.input_request_count,
            self.planned_task_count,
            self.estimated_request_bytes
        )
    }
}

/// Report-only input for object-store checkpoint/retry/idempotency readiness.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreCheckpointRetryInput {
    pub scheduling_report: ObjectStoreDistributedSchedulingReport,
    pub retry_policy_declared: bool,
    pub checkpoint_plan_declared: bool,
    pub idempotency_keys_declared: bool,
    pub attempt_record_declared: bool,
    pub cleanup_policy_declared: bool,
}

impl ObjectStoreCheckpointRetryInput {
    #[must_use]
    pub fn new(scheduling_report: ObjectStoreDistributedSchedulingReport) -> Self {
        Self {
            scheduling_report,
            retry_policy_declared: false,
            checkpoint_plan_declared: false,
            idempotency_keys_declared: false,
            attempt_record_declared: false,
            cleanup_policy_declared: false,
        }
    }

    #[must_use]
    pub fn with_retry_policy(mut self, value: bool) -> Self {
        self.retry_policy_declared = value;
        self
    }

    #[must_use]
    pub fn with_checkpoint_plan(mut self, value: bool) -> Self {
        self.checkpoint_plan_declared = value;
        self
    }

    #[must_use]
    pub fn with_idempotency_keys(mut self, value: bool) -> Self {
        self.idempotency_keys_declared = value;
        self
    }

    #[must_use]
    pub fn with_attempt_record(mut self, value: bool) -> Self {
        self.attempt_record_declared = value;
        self
    }

    #[must_use]
    pub fn with_cleanup_policy(mut self, value: bool) -> Self {
        self.cleanup_policy_declared = value;
        self
    }
}

/// Object-store checkpoint/retry/idempotency readiness status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreCheckpointRetryStatus {
    Ready,
    BlockedByScheduling,
    BlockedMissingRetryPolicy,
    BlockedMissingCheckpointPlan,
    BlockedMissingIdempotency,
    BlockedMissingAttemptRecord,
    BlockedMissingCleanupPolicy,
}

impl ObjectStoreCheckpointRetryStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::BlockedByScheduling => "blocked_by_scheduling",
            Self::BlockedMissingRetryPolicy => "blocked_missing_retry_policy",
            Self::BlockedMissingCheckpointPlan => "blocked_missing_checkpoint_plan",
            Self::BlockedMissingIdempotency => "blocked_missing_idempotency",
            Self::BlockedMissingAttemptRecord => "blocked_missing_attempt_record",
            Self::BlockedMissingCleanupPolicy => "blocked_missing_cleanup_policy",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Ready)
    }
}

/// Machine-readable CG-10 checkpoint/retry/idempotency planning evidence.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreCheckpointRetryReport {
    pub input: ObjectStoreCheckpointRetryInput,
    pub status: ObjectStoreCheckpointRetryStatus,
    pub diagnostics: Vec<Diagnostic>,
    pub task_count: usize,
    pub retryable_task_count: usize,
    pub planned_checkpoint_record_count: usize,
    pub planned_attempt_record_count: usize,
    pub requires_retry_policy: bool,
    pub requires_checkpoint_plan: bool,
    pub requires_idempotency_keys: bool,
    pub requires_attempt_records: bool,
    pub requires_cleanup_policy: bool,
    pub retry_execution_allowed: bool,
    pub checkpoint_write_allowed: bool,
    pub cleanup_execution_allowed: bool,
    pub coordinator_started: bool,
    pub worker_started: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreCheckpointRetryReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.retry_execution_allowed
            || self.checkpoint_write_allowed
            || self.cleanup_execution_allowed
            || self.coordinator_started
            || self.worker_started
            || self.object_store_io
            || self.write_io
            || self.fallback_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.retry_execution_allowed
            && !self.checkpoint_write_allowed
            && !self.cleanup_execution_allowed
            && !self.coordinator_started
            && !self.worker_started
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_checkpoint_retry(status={}, tasks={}, retryable_tasks={}, checkpoint_records={}, attempt_records={}, retry_execution=false, checkpoint_write=false, cleanup_execution=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.task_count,
            self.retryable_task_count,
            self.planned_checkpoint_record_count,
            self.planned_attempt_record_count
        )
    }
}

/// Report-only object-store commit protocol input.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreCommitProtocolInput {
    pub target_uri: DatasetUri,
    pub staging_prefix_declared: bool,
    pub manifest_pointer_update_declared: bool,
    pub commit_record_declared: bool,
    pub idempotency_key_declared: bool,
    pub cleanup_plan_declared: bool,
    pub provider_atomic_commit_declared: bool,
}

impl ObjectStoreCommitProtocolInput {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            staging_prefix_declared: false,
            manifest_pointer_update_declared: false,
            commit_record_declared: false,
            idempotency_key_declared: false,
            cleanup_plan_declared: false,
            provider_atomic_commit_declared: false,
        }
    }

    #[must_use]
    pub const fn with_staging_prefix(mut self, value: bool) -> Self {
        self.staging_prefix_declared = value;
        self
    }

    #[must_use]
    pub const fn with_manifest_pointer_update(mut self, value: bool) -> Self {
        self.manifest_pointer_update_declared = value;
        self
    }

    #[must_use]
    pub const fn with_commit_record(mut self, value: bool) -> Self {
        self.commit_record_declared = value;
        self
    }

    #[must_use]
    pub const fn with_idempotency_key(mut self, value: bool) -> Self {
        self.idempotency_key_declared = value;
        self
    }

    #[must_use]
    pub const fn with_cleanup_plan(mut self, value: bool) -> Self {
        self.cleanup_plan_declared = value;
        self
    }

    #[must_use]
    pub const fn with_provider_atomic_commit(mut self, value: bool) -> Self {
        self.provider_atomic_commit_declared = value;
        self
    }
}

/// Object-store commit protocol planning status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreCommitProtocolStatus {
    Ready,
    BlockedNonObjectStore,
    BlockedMissingStaging,
    BlockedMissingManifestPointer,
    BlockedMissingCommitRecord,
    BlockedMissingIdempotency,
    BlockedMissingCleanup,
    BlockedAtomicity,
}

impl ObjectStoreCommitProtocolStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::BlockedNonObjectStore => "blocked_non_object_store",
            Self::BlockedMissingStaging => "blocked_missing_staging",
            Self::BlockedMissingManifestPointer => "blocked_missing_manifest_pointer",
            Self::BlockedMissingCommitRecord => "blocked_missing_commit_record",
            Self::BlockedMissingIdempotency => "blocked_missing_idempotency",
            Self::BlockedMissingCleanup => "blocked_missing_cleanup",
            Self::BlockedAtomicity => "blocked_atomicity",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Ready)
    }
}

/// Machine-readable CG-10 object-store commit protocol planning evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreCommitProtocolReport {
    pub input: ObjectStoreCommitProtocolInput,
    pub status: ObjectStoreCommitProtocolStatus,
    pub diagnostics: Vec<Diagnostic>,
    pub object_store_target: bool,
    pub requires_staging_prefix: bool,
    pub requires_manifest_pointer_update: bool,
    pub requires_commit_record: bool,
    pub requires_idempotency_key: bool,
    pub requires_cleanup_plan: bool,
    pub requires_atomic_commit_evidence: bool,
    pub commit_execution_allowed: bool,
    pub can_plan_without_io: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreCommitProtocolReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.commit_execution_allowed
            || self.object_store_io
            || self.write_io
            || self.fallback_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.commit_execution_allowed
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_commit_protocol(status={}, object_store_target={}, commit_execution=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.object_store_target
        )
    }
}

/// Aggregate object-store request planner status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRequestPlannerStatus {
    Planned,
    BlockedByRangePlanning,
    BlockedByCoalescing,
    BlockedByScheduling,
    BlockedByReliability,
    BlockedByCommit,
    UnsafeSideEffect,
}

impl ObjectStoreRequestPlannerStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::BlockedByRangePlanning => "blocked_by_range_planning",
            Self::BlockedByCoalescing => "blocked_by_coalescing",
            Self::BlockedByScheduling => "blocked_by_scheduling",
            Self::BlockedByReliability => "blocked_by_reliability",
            Self::BlockedByCommit => "blocked_by_commit",
            Self::UnsafeSideEffect => "unsafe_side_effect",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned)
    }
}

/// Byte-range provider admission status for future object-store reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreByteRangeProviderGateStatus {
    BlockedUntilCertified,
}

impl ObjectStoreByteRangeProviderGateStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }
}

/// Report-only provider gate for future object-store byte-range reads.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreByteRangeProviderGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub status: ObjectStoreByteRangeProviderGateStatus,
    pub scope: &'static str,
    pub provider_family: &'static str,
    pub blocker_id: &'static str,
    pub required_evidence: &'static str,
    pub range_planning_evidence_present: bool,
    pub request_budget_policy_required: bool,
    pub provider_capability_policy_required: bool,
    pub credential_policy_required: bool,
    pub retry_policy_required: bool,
    pub idempotency_key_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub public_no_credential_fixture_profile_admitted: bool,
    pub public_no_credential_fixture_read_allowed: bool,
    pub public_no_credential_fixture_listing_allowed: bool,
    pub public_no_credential_fixture_cache_write_allowed: bool,
    pub live_provider_network_read_allowed: bool,
    pub range_read_execution_allowed: bool,
    pub full_file_read_allowed: bool,
    pub credential_resolution_allowed: bool,
    pub credentials_resolved: bool,
    pub retry_execution_allowed: bool,
    pub provider_probe: bool,
    pub network_probe: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl ObjectStoreByteRangeProviderGateReport {
    #[must_use]
    pub fn blocked_default(range_planning_evidence_present: bool) -> Self {
        Self {
            schema_version: "shardloom.object_store_byte_range_provider_gate.v1",
            report_id: "gar0008a.object_store_byte_range_provider_gate",
            status: ObjectStoreByteRangeProviderGateStatus::BlockedUntilCertified,
            scope: "s3_gcs_adls_byte_range_reads",
            provider_family: "s3,gcs,adls",
            blocker_id: "gar0008a.byte_range_provider_runtime_blocked",
            required_evidence: "provider_capability_policy,credential_effect_policy,request_budget_policy,retry_policy,idempotency_key_contract,execution_certificate,native_io_certificate,benchmark_evidence",
            range_planning_evidence_present,
            request_budget_policy_required: true,
            provider_capability_policy_required: true,
            credential_policy_required: true,
            retry_policy_required: true,
            idempotency_key_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            public_no_credential_fixture_profile_admitted: true,
            public_no_credential_fixture_read_allowed: true,
            public_no_credential_fixture_listing_allowed: true,
            public_no_credential_fixture_cache_write_allowed: false,
            live_provider_network_read_allowed: false,
            range_read_execution_allowed: false,
            full_file_read_allowed: false,
            credential_resolution_allowed: false,
            credentials_resolved: false,
            retry_execution_allowed: false,
            provider_probe: false,
            network_probe: false,
            data_read: false,
            object_store_io: false,
            write_io: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.range_read_execution_allowed
            && !self.full_file_read_allowed
            && !self.public_no_credential_fixture_cache_write_allowed
            && !self.live_provider_network_read_allowed
            && !self.credential_resolution_allowed
            && !self.credentials_resolved
            && !self.retry_execution_allowed
            && !self.provider_probe
            && !self.network_probe
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }
}

/// Machine-readable CG-10 aggregate request planner evidence.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRequestPlannerReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub status: ObjectStoreRequestPlannerStatus,
    pub range_report: ObjectStoreRangePlanningReport,
    pub coalescing_report: ObjectStoreRequestCoalescingReport,
    pub scheduling_report: ObjectStoreDistributedSchedulingReport,
    pub checkpoint_retry_report: ObjectStoreCheckpointRetryReport,
    pub commit_report: ObjectStoreCommitProtocolReport,
    pub byte_range_provider_gate: ObjectStoreByteRangeProviderGateReport,
    pub diagnostics: Vec<Diagnostic>,
    pub planned_surface_count: usize,
    pub blocked_surface_count: usize,
    pub planned_request_count: usize,
    pub coalesced_request_count: usize,
    pub planned_task_count: usize,
    pub retryable_task_count: usize,
    pub planned_checkpoint_record_count: usize,
    pub planned_attempt_record_count: usize,
    pub estimated_request_bytes: u64,
    pub requires_byte_ranges: bool,
    pub requires_request_budget_review: bool,
    pub requires_checkpoint_plan: bool,
    pub requires_retry_policy: bool,
    pub requires_idempotency_keys: bool,
    pub requires_attempt_records: bool,
    pub requires_cleanup_policy: bool,
    pub requires_atomic_commit_evidence: bool,
    pub full_file_read_allowed: bool,
    pub coordinator_started: bool,
    pub worker_started: bool,
    pub task_execution_allowed: bool,
    pub retry_execution_allowed: bool,
    pub checkpoint_write_allowed: bool,
    pub cleanup_execution_allowed: bool,
    pub commit_execution_allowed: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreRequestPlannerReport {
    #[must_use]
    pub fn surface_order() -> Vec<&'static str> {
        vec![
            "range_planning",
            "request_coalescing",
            "distributed_scheduling",
            "checkpoint_retry",
            "commit_protocol",
        ]
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || !self.side_effect_free()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.full_file_read_allowed
            && !self.coordinator_started
            && !self.worker_started
            && !self.task_execution_allowed
            && !self.retry_execution_allowed
            && !self.checkpoint_write_allowed
            && !self.cleanup_execution_allowed
            && !self.commit_execution_allowed
            && self.byte_range_provider_gate.side_effect_free()
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_request_planner(status={}, planned_surfaces={}, blocked_surfaces={}, planned_requests={}, coalesced_requests={}, planned_tasks={}, retryable_tasks={}, checkpoint_records={}, commit_status={}, byte_range_provider_gate={}, data_read=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.planned_surface_count,
            self.blocked_surface_count,
            self.planned_request_count,
            self.coalesced_request_count,
            self.planned_task_count,
            self.retryable_task_count,
            self.planned_checkpoint_record_count,
            self.commit_report.status.as_str(),
            self.byte_range_provider_gate.status.as_str(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRuntimePromotionSurface {
    RequestPlannerAggregate,
    ByteRangeProviderGate,
    RangeReadExecution,
    RequestCoalescingRuntime,
    DistributedCoordinatorStartup,
    DistributedWorkerStartup,
    DistributedTaskExecution,
    CheckpointWriteExecution,
    RetryExecution,
    CleanupExecution,
    ObjectStoreCommitExecution,
    PartitionDiscoveryRuntime,
    CatalogIntegrationRuntime,
    RemoteResultDeliveryRuntime,
    ProviderCredentialRuntime,
    BenchmarkCertificateCloseout,
}

impl ObjectStoreRuntimePromotionSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RequestPlannerAggregate => "request_planner_aggregate",
            Self::ByteRangeProviderGate => "byte_range_provider_gate",
            Self::RangeReadExecution => "range_read_execution",
            Self::RequestCoalescingRuntime => "request_coalescing_runtime",
            Self::DistributedCoordinatorStartup => "distributed_coordinator_startup",
            Self::DistributedWorkerStartup => "distributed_worker_startup",
            Self::DistributedTaskExecution => "distributed_task_execution",
            Self::CheckpointWriteExecution => "checkpoint_write_execution",
            Self::RetryExecution => "retry_execution",
            Self::CleanupExecution => "cleanup_execution",
            Self::ObjectStoreCommitExecution => "object_store_commit_execution",
            Self::PartitionDiscoveryRuntime => "partition_discovery_runtime",
            Self::CatalogIntegrationRuntime => "catalog_integration_runtime",
            Self::RemoteResultDeliveryRuntime => "remote_result_delivery_runtime",
            Self::ProviderCredentialRuntime => "provider_credential_runtime",
            Self::BenchmarkCertificateCloseout => "benchmark_certificate_closeout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRuntimePromotionStatus {
    ExistingReportOnlyEvidence,
    BlockedUntilCertified,
}

impl ObjectStoreRuntimePromotionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingReportOnlyEvidence => "existing_report_only_evidence",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }

    #[must_use]
    pub const fn is_existing_evidence(&self) -> bool {
        matches!(self, Self::ExistingReportOnlyEvidence)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRuntimePromotionRequirements {
    pub requires_range_planning: bool,
    pub requires_request_budget: bool,
    pub requires_scheduler_policy: bool,
    pub requires_checkpoint_plan: bool,
    pub requires_retry_policy: bool,
    pub requires_commit_atomicity: bool,
    pub requires_credential_policy: bool,
    pub requires_benchmark_evidence: bool,
}

impl ObjectStoreRuntimePromotionRequirements {
    pub const RANGE_READ: Self = Self {
        requires_range_planning: true,
        requires_request_budget: true,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: false,
        requires_retry_policy: false,
        requires_commit_atomicity: false,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const COALESCING_RUNTIME: Self = Self {
        requires_range_planning: true,
        requires_request_budget: true,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: false,
        requires_retry_policy: false,
        requires_commit_atomicity: false,
        requires_credential_policy: false,
        requires_benchmark_evidence: true,
    };

    pub const DISTRIBUTED_RUNTIME: Self = Self {
        requires_range_planning: true,
        requires_request_budget: true,
        requires_scheduler_policy: true,
        requires_checkpoint_plan: true,
        requires_retry_policy: true,
        requires_commit_atomicity: false,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const RELIABILITY_RUNTIME: Self = Self {
        requires_range_planning: true,
        requires_request_budget: true,
        requires_scheduler_policy: true,
        requires_checkpoint_plan: true,
        requires_retry_policy: true,
        requires_commit_atomicity: false,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const COMMIT_RUNTIME: Self = Self {
        requires_range_planning: false,
        requires_request_budget: false,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: true,
        requires_retry_policy: true,
        requires_commit_atomicity: true,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const PARTITION_DISCOVERY_RUNTIME: Self = Self {
        requires_range_planning: false,
        requires_request_budget: true,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: false,
        requires_retry_policy: true,
        requires_commit_atomicity: false,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const CATALOG_INTEGRATION_RUNTIME: Self = Self {
        requires_range_planning: false,
        requires_request_budget: true,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: false,
        requires_retry_policy: true,
        requires_commit_atomicity: false,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const REMOTE_RESULT_DELIVERY_RUNTIME: Self = Self {
        requires_range_planning: false,
        requires_request_budget: true,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: true,
        requires_retry_policy: true,
        requires_commit_atomicity: true,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };

    pub const CREDENTIAL_RUNTIME: Self = Self {
        requires_range_planning: false,
        requires_request_budget: false,
        requires_scheduler_policy: false,
        requires_checkpoint_plan: false,
        requires_retry_policy: false,
        requires_commit_atomicity: false,
        requires_credential_policy: true,
        requires_benchmark_evidence: false,
    };

    pub const BENCHMARK_CLOSEOUT: Self = Self {
        requires_range_planning: true,
        requires_request_budget: true,
        requires_scheduler_policy: true,
        requires_checkpoint_plan: true,
        requires_retry_policy: true,
        requires_commit_atomicity: true,
        requires_credential_policy: true,
        requires_benchmark_evidence: true,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRuntimePromotionGateEntry {
    pub surface: ObjectStoreRuntimePromotionSurface,
    pub status: ObjectStoreRuntimePromotionStatus,
    pub existing_report_ref: Option<&'static str>,
    pub requirements: ObjectStoreRuntimePromotionRequirements,
    pub runtime_allowed: bool,
    pub object_store_io_allowed: bool,
    pub write_io_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreRuntimePromotionGateEntry {
    #[must_use]
    pub const fn existing(
        surface: ObjectStoreRuntimePromotionSurface,
        existing_report_ref: &'static str,
    ) -> Self {
        Self {
            surface,
            status: ObjectStoreRuntimePromotionStatus::ExistingReportOnlyEvidence,
            existing_report_ref: Some(existing_report_ref),
            requirements: ObjectStoreRuntimePromotionRequirements {
                requires_range_planning: false,
                requires_request_budget: false,
                requires_scheduler_policy: false,
                requires_checkpoint_plan: false,
                requires_retry_policy: false,
                requires_commit_atomicity: false,
                requires_credential_policy: false,
                requires_benchmark_evidence: false,
            },
            runtime_allowed: false,
            object_store_io_allowed: false,
            write_io_allowed: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn blocked(
        surface: ObjectStoreRuntimePromotionSurface,
        requirements: ObjectStoreRuntimePromotionRequirements,
    ) -> Self {
        Self {
            surface,
            status: ObjectStoreRuntimePromotionStatus::BlockedUntilCertified,
            existing_report_ref: None,
            requirements,
            runtime_allowed: false,
            object_store_io_allowed: false,
            write_io_allowed: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_allowed
            && !self.object_store_io_allowed
            && !self.write_io_allowed
            && !self.fallback_execution_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRuntimeBlockerMatrixRow {
    pub action: &'static str,
    pub status: &'static str,
    pub diagnostic_code: DiagnosticCode,
    pub blocker_id: &'static str,
    pub required_evidence: &'static str,
    pub allowed: bool,
    pub coordinator_started: bool,
    pub worker_started: bool,
    pub task_executed: bool,
    pub checkpoint_written: bool,
    pub retry_attempted: bool,
    pub cleanup_executed: bool,
    pub commit_record_written: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl ObjectStoreRuntimeBlockerMatrixRow {
    #[must_use]
    pub const fn blocked(
        action: &'static str,
        blocker_id: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            action,
            status: "blocked_until_certified",
            diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
            blocker_id,
            required_evidence,
            allowed: false,
            coordinator_started: false,
            worker_started: false,
            task_executed: false,
            checkpoint_written: false,
            retry_attempted: false,
            cleanup_executed: false,
            commit_record_written: false,
            data_read: false,
            object_store_io: false,
            write_io: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.allowed
            && !self.coordinator_started
            && !self.worker_started
            && !self.task_executed
            && !self.checkpoint_written
            && !self.retry_attempted
            && !self.cleanup_executed
            && !self.commit_record_written
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn to_diagnostic(&self) -> Diagnostic {
        Diagnostic::new(
            self.diagnostic_code,
            DiagnosticSeverity::Info,
            DiagnosticCategory::ObjectStore,
            format!("object-store runtime action {} is blocked", self.action),
            Some(self.action.to_string()),
            Some(format!(
                "{} requires {} before runtime promotion.",
                self.blocker_id, self.required_evidence
            )),
            Some("Keep the path report-only until all required evidence is attached.".to_string()),
            FallbackStatus::disabled_by_policy(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRuntimePromotionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<ObjectStoreRuntimePromotionGateEntry>,
    pub byte_range_provider_gate: ObjectStoreByteRangeProviderGateReport,
    pub runtime_blocker_matrix: Vec<ObjectStoreRuntimeBlockerMatrixRow>,
    pub existing_report_refs: Vec<&'static str>,
    pub existing_request_planner_evidence_present: bool,
    pub existing_range_planning_evidence_present: bool,
    pub existing_coalescing_evidence_present: bool,
    pub existing_distributed_scheduling_evidence_present: bool,
    pub existing_checkpoint_retry_evidence_present: bool,
    pub existing_commit_protocol_evidence_present: bool,
    pub approved_real_backend_profile_declared: bool,
    pub approved_real_backend_profile_id: &'static str,
    pub approved_real_backend_profile_status: &'static str,
    pub approved_real_backend_required_evidence: &'static str,
    pub approved_real_backend_profile_required: bool,
    pub approved_real_backend_network_access_allowed: bool,
    pub approved_real_backend_credential_resolution_allowed: bool,
    pub approved_real_backend_read_allowed: bool,
    pub approved_real_backend_write_allowed: bool,
    pub production_object_store_native_io_certificate_present: bool,
    pub production_object_store_claim_allowed: bool,
    pub production_object_store_claim_gate_status: &'static str,
    pub production_object_store_blocker_id: &'static str,
    pub range_read_execution_allowed: bool,
    pub full_file_read_allowed: bool,
    pub request_coalescing_runtime_allowed: bool,
    pub coordinator_start_allowed: bool,
    pub worker_start_allowed: bool,
    pub task_execution_allowed: bool,
    pub retry_execution_allowed: bool,
    pub checkpoint_write_allowed: bool,
    pub cleanup_execution_allowed: bool,
    pub commit_execution_allowed: bool,
    pub credential_resolution_allowed: bool,
    pub object_store_io_allowed: bool,
    pub data_read_allowed: bool,
    pub write_io_allowed: bool,
    pub object_store_runtime_claim_allowed: bool,
    pub distributed_runtime_claim_allowed: bool,
    pub range_planning_evidence_required: bool,
    pub request_budget_policy_required: bool,
    pub provider_capability_policy_required: bool,
    pub credential_effect_policy_required: bool,
    pub scheduler_policy_required: bool,
    pub worker_identity_required: bool,
    pub checkpoint_plan_required: bool,
    pub retry_policy_required: bool,
    pub idempotency_keys_required: bool,
    pub attempt_records_required: bool,
    pub cleanup_policy_required: bool,
    pub atomic_commit_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl ObjectStoreRuntimePromotionGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        let runtime_blocker_matrix = object_store_runtime_blocker_matrix_rows();
        let diagnostics = runtime_blocker_matrix
            .iter()
            .map(ObjectStoreRuntimeBlockerMatrixRow::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.object_store_runtime_promotion_gate.v1",
            report_id: "cg10.object_store_runtime_promotion_gate",
            entries: object_store_runtime_promotion_entries(),
            byte_range_provider_gate: ObjectStoreByteRangeProviderGateReport::blocked_default(true),
            runtime_blocker_matrix,
            existing_report_refs: object_store_runtime_existing_report_refs(),
            existing_request_planner_evidence_present: true,
            existing_range_planning_evidence_present: true,
            existing_coalescing_evidence_present: true,
            existing_distributed_scheduling_evidence_present: true,
            existing_checkpoint_retry_evidence_present: true,
            existing_commit_protocol_evidence_present: true,
            approved_real_backend_profile_declared: false,
            approved_real_backend_profile_id: "not_declared",
            approved_real_backend_profile_status: "missing_approved_real_backend_profile",
            approved_real_backend_required_evidence: "approved_backend_id,credential_policy,redaction_policy,network_probe_policy,byte_range_read_certificate,write_commit_recovery_certificate,retry_backoff_policy,rate_limit_policy,bounded_streaming_evidence,benchmark_profile",
            approved_real_backend_profile_required: true,
            approved_real_backend_network_access_allowed: false,
            approved_real_backend_credential_resolution_allowed: false,
            approved_real_backend_read_allowed: false,
            approved_real_backend_write_allowed: false,
            production_object_store_native_io_certificate_present: false,
            production_object_store_claim_allowed: false,
            production_object_store_claim_gate_status: "not_claim_grade",
            production_object_store_blocker_id: "prod-ready-1b.approved_real_backend_profile_missing",
            range_read_execution_allowed: false,
            full_file_read_allowed: false,
            request_coalescing_runtime_allowed: false,
            coordinator_start_allowed: false,
            worker_start_allowed: false,
            task_execution_allowed: false,
            retry_execution_allowed: false,
            checkpoint_write_allowed: false,
            cleanup_execution_allowed: false,
            commit_execution_allowed: false,
            credential_resolution_allowed: false,
            object_store_io_allowed: false,
            data_read_allowed: false,
            write_io_allowed: false,
            object_store_runtime_claim_allowed: false,
            distributed_runtime_claim_allowed: false,
            range_planning_evidence_required: true,
            request_budget_policy_required: true,
            provider_capability_policy_required: true,
            credential_effect_policy_required: true,
            scheduler_policy_required: true,
            worker_identity_required: true,
            checkpoint_plan_required: true,
            retry_policy_required: true,
            idempotency_keys_required: true,
            attempt_records_required: true,
            cleanup_policy_required: true,
            atomic_commit_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn existing_evidence_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_existing_evidence())
            .count()
    }

    #[must_use]
    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.status,
                    ObjectStoreRuntimePromotionStatus::BlockedUntilCertified
                )
            })
            .count()
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.range_read_execution_allowed
            && !self.full_file_read_allowed
            && !self.request_coalescing_runtime_allowed
            && !self.coordinator_start_allowed
            && !self.worker_start_allowed
            && !self.task_execution_allowed
            && !self.retry_execution_allowed
            && !self.checkpoint_write_allowed
            && !self.cleanup_execution_allowed
            && !self.commit_execution_allowed
            && !self.credential_resolution_allowed
            && !self.approved_real_backend_network_access_allowed
            && !self.approved_real_backend_credential_resolution_allowed
            && !self.approved_real_backend_read_allowed
            && !self.approved_real_backend_write_allowed
            && !self.object_store_io_allowed
            && !self.data_read_allowed
            && !self.write_io_allowed
            && self
                .entries
                .iter()
                .all(ObjectStoreRuntimePromotionGateEntry::side_effect_free)
            && self.byte_range_provider_gate.side_effect_free()
            && self
                .runtime_blocker_matrix
                .iter()
                .all(ObjectStoreRuntimeBlockerMatrixRow::side_effect_free)
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        !self.object_store_runtime_claim_allowed
            && !self.distributed_runtime_claim_allowed
            && !self.production_object_store_claim_allowed
            && !self.production_object_store_native_io_certificate_present
            && !self.approved_real_backend_profile_declared
    }

    #[must_use]
    pub fn runtime_blocker_matrix_row_order(&self) -> Vec<&'static str> {
        self.runtime_blocker_matrix
            .iter()
            .map(|row| row.action)
            .collect()
    }

    #[must_use]
    pub fn runtime_blocker_matrix_diagnostic_count(&self) -> usize {
        self.runtime_blocker_matrix
            .iter()
            .filter(|row| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == row.diagnostic_code
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.category == DiagnosticCategory::ObjectStore
                        && diagnostic.feature.as_deref() == Some(row.action)
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn runtime_blocker_matrix_diagnostics_propagated(&self) -> bool {
        !self.runtime_blocker_matrix.is_empty()
            && self.runtime_blocker_matrix_diagnostic_count() == self.runtime_blocker_matrix.len()
    }

    #[must_use]
    pub fn runtime_blocker_matrix_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.runtime_blocker_matrix
            .iter()
            .map(|row| row.diagnostic_code.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_blocker_matrix_diagnostic_category_order(&self) -> Vec<&'static str> {
        self.runtime_blocker_matrix
            .iter()
            .map(|_| DiagnosticCategory::ObjectStore.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_blocker_matrix_diagnostic_severity_order(&self) -> Vec<&'static str> {
        self.runtime_blocker_matrix
            .iter()
            .map(|_| DiagnosticSeverity::Info.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_blocker_matrix_all_allowed_false(&self) -> bool {
        self.runtime_blocker_matrix.iter().all(|row| !row.allowed)
    }

    #[must_use]
    pub fn runtime_blocker_matrix_all_no_io(&self) -> bool {
        self.runtime_blocker_matrix
            .iter()
            .all(|row| !row.data_read && !row.object_store_io && !row.write_io)
    }

    #[must_use]
    pub fn runtime_blocker_matrix_all_no_fallback(&self) -> bool {
        self.runtime_blocker_matrix
            .iter()
            .all(|row| !row.fallback_attempted && !row.fallback_execution_allowed)
    }

    #[must_use]
    pub fn runtime_blocker_matrix_all_no_external_engine(&self) -> bool {
        self.runtime_blocker_matrix
            .iter()
            .all(|row| !row.external_engine_invoked)
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "existing report refs: {}",
            self.existing_report_refs.join(",")
        );
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(out, "claim blocked: {}", self.claim_blocked());
        let _ = writeln!(
            out,
            "byte-range provider gate: {}",
            self.byte_range_provider_gate.status.as_str()
        );
        let _ = writeln!(
            out,
            "runtime blocker matrix rows: {}",
            self.runtime_blocker_matrix.len()
        );
        let _ = writeln!(
            out,
            "runtime blocker diagnostics propagated: {}",
            self.runtime_blocker_matrix_diagnostics_propagated()
        );
        let _ = writeln!(
            out,
            "runtime blocker diagnostic count: {}",
            self.runtime_blocker_matrix_diagnostic_count()
        );
        let _ = writeln!(
            out,
            "runtime blocker diagnostic severities: {}",
            self.runtime_blocker_matrix_diagnostic_severity_order()
                .join(",")
        );
        let _ = writeln!(
            out,
            "runtime blocker all no external engine: {}",
            self.runtime_blocker_matrix_all_no_external_engine()
        );
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} runtime_allowed={} object_store_io_allowed={} write_io_allowed={} fallback_execution_allowed={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
                entry.runtime_allowed,
                entry.object_store_io_allowed,
                entry.write_io_allowed,
                entry.fallback_execution_allowed
            );
        }
        out
    }
}

fn object_store_runtime_promotion_entries() -> Vec<ObjectStoreRuntimePromotionGateEntry> {
    vec![
        ObjectStoreRuntimePromotionGateEntry::existing(
            ObjectStoreRuntimePromotionSurface::RequestPlannerAggregate,
            "cg10.object_store_request_planner.aggregate",
        ),
        ObjectStoreRuntimePromotionGateEntry::existing(
            ObjectStoreRuntimePromotionSurface::ByteRangeProviderGate,
            "gar0008a.object_store_byte_range_provider_gate",
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::RangeReadExecution,
            ObjectStoreRuntimePromotionRequirements::RANGE_READ,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::RequestCoalescingRuntime,
            ObjectStoreRuntimePromotionRequirements::COALESCING_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::DistributedCoordinatorStartup,
            ObjectStoreRuntimePromotionRequirements::DISTRIBUTED_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::DistributedWorkerStartup,
            ObjectStoreRuntimePromotionRequirements::DISTRIBUTED_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::DistributedTaskExecution,
            ObjectStoreRuntimePromotionRequirements::DISTRIBUTED_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::CheckpointWriteExecution,
            ObjectStoreRuntimePromotionRequirements::RELIABILITY_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::RetryExecution,
            ObjectStoreRuntimePromotionRequirements::RELIABILITY_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::CleanupExecution,
            ObjectStoreRuntimePromotionRequirements::RELIABILITY_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::ObjectStoreCommitExecution,
            ObjectStoreRuntimePromotionRequirements::COMMIT_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::PartitionDiscoveryRuntime,
            ObjectStoreRuntimePromotionRequirements::PARTITION_DISCOVERY_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::CatalogIntegrationRuntime,
            ObjectStoreRuntimePromotionRequirements::CATALOG_INTEGRATION_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::RemoteResultDeliveryRuntime,
            ObjectStoreRuntimePromotionRequirements::REMOTE_RESULT_DELIVERY_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::ProviderCredentialRuntime,
            ObjectStoreRuntimePromotionRequirements::CREDENTIAL_RUNTIME,
        ),
        ObjectStoreRuntimePromotionGateEntry::blocked(
            ObjectStoreRuntimePromotionSurface::BenchmarkCertificateCloseout,
            ObjectStoreRuntimePromotionRequirements::BENCHMARK_CLOSEOUT,
        ),
    ]
}

fn object_store_runtime_blocker_matrix_rows() -> Vec<ObjectStoreRuntimeBlockerMatrixRow> {
    vec![
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "coordinator_start",
            "gar0008b.coordinator_start_blocked",
            "scheduler_policy,coordinator_identity,execution_certificate,no_fallback_policy",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "worker_start",
            "gar0008b.worker_start_blocked",
            "worker_identity,scheduler_policy,execution_certificate,no_fallback_policy",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "task_execution",
            "gar0008b.task_execution_blocked",
            "task_plan,worker_identity,idempotency_key_contract,execution_certificate,native_io_certificate",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "checkpoint_write",
            "gar0008b.checkpoint_write_blocked",
            "checkpoint_plan,checkpoint_storage_policy,idempotency_key_contract,execution_certificate",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "retry_attempt",
            "gar0008b.retry_attempt_blocked",
            "retry_policy,retryable_failure_classes,attempt_records,idempotency_key_contract",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "cleanup_execution",
            "gar0008b.cleanup_execution_blocked",
            "cleanup_policy,attempt_records,idempotency_key_contract,execution_certificate",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "commit_record_write",
            "gar0008b.commit_record_write_blocked",
            "commit_record_schema,atomic_commit_evidence,cleanup_policy,idempotency_key_contract",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "partition_discovery",
            "gar0008b.partition_discovery_blocked",
            "partition_listing_policy,partition_schema_contract,credential_effect_policy,execution_certificate,native_io_certificate,no_fallback_policy",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "catalog_integration",
            "gar0008b.catalog_integration_blocked",
            "catalog_adapter_policy,catalog_auth_policy,snapshot_consistency_contract,execution_certificate,native_io_certificate,no_fallback_policy",
        ),
        ObjectStoreRuntimeBlockerMatrixRow::blocked(
            "remote_result_delivery",
            "gar0008b.remote_result_delivery_blocked",
            "remote_delivery_protocol,result_replay_policy,idempotency_key_contract,credential_effect_policy,execution_certificate,native_io_certificate,no_fallback_policy",
        ),
    ]
}

fn object_store_runtime_existing_report_refs() -> Vec<&'static str> {
    vec![
        "cg10.object_store_request_planner.aggregate",
        "gar0008a.object_store_byte_range_provider_gate",
        "shardloom.object_store_range_planning.v1",
        "shardloom.object_store_request_coalescing.v1",
        "shardloom.object_store_distributed_scheduling.v1",
        "shardloom.object_store_checkpoint_retry.v1",
        "shardloom.object_store_commit_protocol.v1",
    ]
}

#[must_use]
pub fn plan_object_store_runtime_promotion_gate() -> ObjectStoreRuntimePromotionGateReport {
    ObjectStoreRuntimePromotionGateReport::planning_default()
}

/// Plans object-store byte-range request shapes from declared manifest metadata only.
#[must_use]
pub fn plan_object_store_ranges(
    manifest: DatasetManifest,
    policy: ObjectStoreRangePlanningPolicy,
) -> ObjectStoreRangePlanningReport {
    let counts = ObjectStoreRangeCounts::from_manifest(&manifest, policy);
    let status = object_store_range_status(counts);
    let requests = if status == ObjectStoreRangePlanningStatus::Planned {
        coalesced_object_store_ranges(&manifest, policy)
    } else {
        Vec::new()
    };
    let estimated_request_bytes = requests
        .iter()
        .map(ObjectStoreRangeRequest::estimated_bytes)
        .sum();
    let planned_range_count = requests
        .iter()
        .map(|request| request.source_range_count)
        .sum();
    let diagnostics = object_store_range_diagnostics(counts, status);

    ObjectStoreRangePlanningReport {
        file_count: counts.files,
        segment_count: counts.segments,
        object_store_file_count: counts.object_store_files,
        non_object_store_file_count: counts.non_object_store_files,
        ranged_segment_count: counts.ranged_segments,
        missing_byte_range_segment_count: counts.missing_byte_range_segments,
        invalid_range_count: counts.invalid_ranges,
        oversized_range_count: counts.oversized_ranges,
        planned_request_count: requests.len(),
        planned_range_count,
        coalesced_range_count: planned_range_count.saturating_sub(requests.len()),
        estimated_request_bytes,
        requires_byte_ranges: counts.missing_byte_range_segments > 0,
        requires_request_budget_review: counts.oversized_ranges > 0,
        full_file_read_required: counts.missing_byte_range_segments > 0,
        full_file_read_allowed: false,
        can_plan_without_io: true,
        data_read: false,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        manifest,
        policy,
        status,
        requests,
        diagnostics,
    }
}

/// Plans object-store commit protocol readiness without object-store IO or writes.
#[must_use]
pub fn plan_object_store_commit_protocol(
    input: ObjectStoreCommitProtocolInput,
) -> ObjectStoreCommitProtocolReport {
    let status = object_store_commit_protocol_status(&input);
    let diagnostics = object_store_commit_protocol_diagnostics(&input, status);
    let object_store_target = is_object_store_uri(&input.target_uri);

    ObjectStoreCommitProtocolReport {
        requires_staging_prefix: !input.staging_prefix_declared,
        requires_manifest_pointer_update: !input.manifest_pointer_update_declared,
        requires_commit_record: !input.commit_record_declared,
        requires_idempotency_key: !input.idempotency_key_declared,
        requires_cleanup_plan: !input.cleanup_plan_declared,
        requires_atomic_commit_evidence: !input.provider_atomic_commit_declared,
        commit_execution_allowed: false,
        can_plan_without_io: true,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        input,
        status,
        diagnostics,
        object_store_target,
    }
}

/// Aggregates object-store request, scheduling, reliability, and commit evidence.
#[must_use]
pub fn plan_object_store_request_planner(
    range_report: ObjectStoreRangePlanningReport,
    coalescing_report: ObjectStoreRequestCoalescingReport,
    scheduling_report: ObjectStoreDistributedSchedulingReport,
    checkpoint_retry_report: ObjectStoreCheckpointRetryReport,
    commit_report: ObjectStoreCommitProtocolReport,
) -> ObjectStoreRequestPlannerReport {
    let status = object_store_request_planner_status(
        &range_report,
        &coalescing_report,
        &scheduling_report,
        &checkpoint_retry_report,
        &commit_report,
    );
    let diagnostics = object_store_request_planner_diagnostics(
        &range_report,
        &coalescing_report,
        &scheduling_report,
        &checkpoint_retry_report,
        &commit_report,
    );
    let planned_surface_count = [
        !range_report.has_errors(),
        !coalescing_report.has_errors(),
        !scheduling_report.has_errors(),
        !checkpoint_retry_report.has_errors(),
        !commit_report.has_errors(),
    ]
    .into_iter()
    .filter(|planned| *planned)
    .count();
    let blocked_surface_count =
        ObjectStoreRequestPlannerReport::surface_order().len() - planned_surface_count;
    let byte_range_provider_gate =
        ObjectStoreByteRangeProviderGateReport::blocked_default(!range_report.has_errors());

    ObjectStoreRequestPlannerReport {
        schema_version: "shardloom.object_store_request_planner.v1",
        report_id: "cg10.object_store_request_planner.aggregate",
        planned_surface_count,
        blocked_surface_count,
        planned_request_count: range_report.planned_request_count,
        coalesced_request_count: coalescing_report.output_request_count,
        planned_task_count: scheduling_report.planned_task_count,
        retryable_task_count: checkpoint_retry_report.retryable_task_count,
        planned_checkpoint_record_count: checkpoint_retry_report.planned_checkpoint_record_count,
        planned_attempt_record_count: checkpoint_retry_report.planned_attempt_record_count,
        estimated_request_bytes: scheduling_report.estimated_request_bytes,
        requires_byte_ranges: range_report.requires_byte_ranges,
        requires_request_budget_review: range_report.requires_request_budget_review,
        requires_checkpoint_plan: scheduling_report.requires_checkpoint_plan
            || checkpoint_retry_report.requires_checkpoint_plan,
        requires_retry_policy: scheduling_report.requires_retry_policy
            || checkpoint_retry_report.requires_retry_policy,
        requires_idempotency_keys: scheduling_report.requires_idempotency_keys
            || checkpoint_retry_report.requires_idempotency_keys
            || commit_report.requires_idempotency_key,
        requires_attempt_records: checkpoint_retry_report.requires_attempt_records,
        requires_cleanup_policy: checkpoint_retry_report.requires_cleanup_policy
            || commit_report.requires_cleanup_plan,
        requires_atomic_commit_evidence: commit_report.requires_atomic_commit_evidence,
        full_file_read_allowed: range_report.full_file_read_allowed,
        coordinator_started: scheduling_report.coordinator_started
            || checkpoint_retry_report.coordinator_started,
        worker_started: scheduling_report.worker_started || checkpoint_retry_report.worker_started,
        task_execution_allowed: scheduling_report.task_execution_allowed,
        retry_execution_allowed: checkpoint_retry_report.retry_execution_allowed,
        checkpoint_write_allowed: checkpoint_retry_report.checkpoint_write_allowed,
        cleanup_execution_allowed: checkpoint_retry_report.cleanup_execution_allowed,
        commit_execution_allowed: commit_report.commit_execution_allowed,
        data_read: range_report.data_read || coalescing_report.data_read,
        object_store_io: range_report.object_store_io
            || coalescing_report.object_store_io
            || scheduling_report.object_store_io
            || checkpoint_retry_report.object_store_io
            || commit_report.object_store_io,
        write_io: range_report.write_io
            || coalescing_report.write_io
            || scheduling_report.write_io
            || checkpoint_retry_report.write_io
            || commit_report.write_io,
        fallback_execution_allowed: range_report.fallback_execution_allowed
            || coalescing_report.fallback_execution_allowed
            || scheduling_report.fallback_execution_allowed
            || checkpoint_retry_report.fallback_execution_allowed
            || commit_report.fallback_execution_allowed,
        status,
        range_report,
        coalescing_report,
        scheduling_report,
        checkpoint_retry_report,
        commit_report,
        byte_range_provider_gate,
        diagnostics,
    }
}

fn object_store_request_planner_status(
    range_report: &ObjectStoreRangePlanningReport,
    coalescing_report: &ObjectStoreRequestCoalescingReport,
    scheduling_report: &ObjectStoreDistributedSchedulingReport,
    checkpoint_retry_report: &ObjectStoreCheckpointRetryReport,
    commit_report: &ObjectStoreCommitProtocolReport,
) -> ObjectStoreRequestPlannerStatus {
    if !range_report.side_effect_free()
        || !coalescing_report.side_effect_free()
        || !scheduling_report.side_effect_free()
        || !checkpoint_retry_report.side_effect_free()
        || !commit_report.side_effect_free()
    {
        ObjectStoreRequestPlannerStatus::UnsafeSideEffect
    } else if range_report.has_errors() {
        ObjectStoreRequestPlannerStatus::BlockedByRangePlanning
    } else if coalescing_report.has_errors() {
        ObjectStoreRequestPlannerStatus::BlockedByCoalescing
    } else if scheduling_report.has_errors() {
        ObjectStoreRequestPlannerStatus::BlockedByScheduling
    } else if checkpoint_retry_report.has_errors() {
        ObjectStoreRequestPlannerStatus::BlockedByReliability
    } else if commit_report.has_errors() {
        ObjectStoreRequestPlannerStatus::BlockedByCommit
    } else {
        ObjectStoreRequestPlannerStatus::Planned
    }
}

fn object_store_request_planner_diagnostics(
    range_report: &ObjectStoreRangePlanningReport,
    coalescing_report: &ObjectStoreRequestCoalescingReport,
    scheduling_report: &ObjectStoreDistributedSchedulingReport,
    checkpoint_retry_report: &ObjectStoreCheckpointRetryReport,
    commit_report: &ObjectStoreCommitProtocolReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    diagnostics.extend(range_report.diagnostics.clone());
    diagnostics.extend(coalescing_report.diagnostics.clone());
    diagnostics.extend(scheduling_report.diagnostics.clone());
    diagnostics.extend(checkpoint_retry_report.diagnostics.clone());
    diagnostics.extend(commit_report.diagnostics.clone());
    diagnostics
}

fn object_store_commit_protocol_status(
    input: &ObjectStoreCommitProtocolInput,
) -> ObjectStoreCommitProtocolStatus {
    if !is_object_store_uri(&input.target_uri) {
        ObjectStoreCommitProtocolStatus::BlockedNonObjectStore
    } else if !input.staging_prefix_declared {
        ObjectStoreCommitProtocolStatus::BlockedMissingStaging
    } else if !input.manifest_pointer_update_declared {
        ObjectStoreCommitProtocolStatus::BlockedMissingManifestPointer
    } else if !input.commit_record_declared {
        ObjectStoreCommitProtocolStatus::BlockedMissingCommitRecord
    } else if !input.idempotency_key_declared {
        ObjectStoreCommitProtocolStatus::BlockedMissingIdempotency
    } else if !input.cleanup_plan_declared {
        ObjectStoreCommitProtocolStatus::BlockedMissingCleanup
    } else if !input.provider_atomic_commit_declared {
        ObjectStoreCommitProtocolStatus::BlockedAtomicity
    } else {
        ObjectStoreCommitProtocolStatus::Ready
    }
}

fn object_store_commit_protocol_diagnostics(
    input: &ObjectStoreCommitProtocolInput,
    status: ObjectStoreCommitProtocolStatus,
) -> Vec<Diagnostic> {
    if status == ObjectStoreCommitProtocolStatus::Ready {
        return Vec::new();
    }
    let (code, feature, message, next) = match status {
        ObjectStoreCommitProtocolStatus::Ready => unreachable!("handled above"),
        ObjectStoreCommitProtocolStatus::BlockedNonObjectStore => (
            DiagnosticCode::ObjectStoreUnsupported,
            "object_store_target",
            "object-store commit protocol requires an S3, GCS, or ADLS target",
            "Use object-store targets only for object-store commit protocol planning.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingStaging => (
            DiagnosticCode::InvalidInput,
            "staging_prefix",
            "object-store commit protocol requires a declared staging prefix",
            "Declare a staging prefix before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingManifestPointer => (
            DiagnosticCode::InvalidInput,
            "manifest_pointer",
            "object-store commit protocol requires a manifest pointer update plan",
            "Declare manifest pointer update evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingCommitRecord => (
            DiagnosticCode::InvalidInput,
            "commit_record",
            "object-store commit protocol requires a commit record",
            "Declare commit record evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingIdempotency => (
            DiagnosticCode::InvalidInput,
            "idempotency_key",
            "object-store commit protocol requires an idempotency key",
            "Declare idempotency key evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingCleanup => (
            DiagnosticCode::InvalidInput,
            "cleanup_plan",
            "object-store commit protocol requires cleanup planning",
            "Declare cleanup evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedAtomicity => (
            DiagnosticCode::CommitNotAtomic,
            "atomic_commit",
            "object-store commit protocol requires atomicity evidence",
            "Declare provider atomicity or pointer-swap evidence before planning object-store commits.",
        ),
    };
    vec![Diagnostic::new(
        code,
        DiagnosticSeverity::Error,
        DiagnosticCategory::ObjectStore,
        message,
        Some(feature.to_string()),
        Some(format!(
            "Commit planning for {} is report-only and did not write to storage.",
            input.target_uri.as_str()
        )),
        Some(next.to_string()),
        FallbackStatus::disabled_by_policy(),
    )]
}

/// Plans request coalescing from object-store byte-range request-shape evidence only.
#[must_use]
pub fn plan_object_store_request_coalescing(
    manifest: DatasetManifest,
    policy: ObjectStoreRangePlanningPolicy,
) -> ObjectStoreRequestCoalescingReport {
    let uncoalesced_policy = ObjectStoreRangePlanningPolicy {
        coalesce_adjacent_ranges: false,
        ..policy
    };
    let coalesced_policy = ObjectStoreRangePlanningPolicy {
        coalesce_adjacent_ranges: true,
        ..policy
    };
    let uncoalesced_range_report = plan_object_store_ranges(manifest.clone(), uncoalesced_policy);
    let coalesced_range_report = plan_object_store_ranges(manifest, coalesced_policy);
    let status =
        object_store_request_coalescing_status(&uncoalesced_range_report, &coalesced_range_report);
    let input_request_count = uncoalesced_range_report.planned_request_count;
    let output_request_count = coalesced_range_report.planned_request_count;
    let request_reduction_count = input_request_count.saturating_sub(output_request_count);
    let coalesced_range_count = coalesced_range_report.coalesced_range_count;
    let decisions = object_store_request_coalescing_decisions(
        status,
        input_request_count,
        output_request_count,
        coalesced_range_count,
    );
    let diagnostics = if status.is_error() {
        coalesced_range_report.diagnostics.clone()
    } else {
        Vec::new()
    };

    ObjectStoreRequestCoalescingReport {
        input_range_count: uncoalesced_range_report.planned_range_count,
        estimated_request_bytes_before: uncoalesced_range_report.estimated_request_bytes,
        estimated_request_bytes_after: coalesced_range_report.estimated_request_bytes,
        coalescing_applied: status == ObjectStoreRequestCoalescingStatus::Planned,
        can_plan_without_io: true,
        data_read: false,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        uncoalesced_range_report,
        coalesced_range_report,
        status,
        decisions,
        diagnostics,
        input_request_count,
        output_request_count,
        request_reduction_count,
        coalesced_range_count,
    }
}

/// Plans distributed object-store task shapes without scheduling or executing tasks.
#[must_use]
pub fn plan_object_store_distributed_scheduling(
    coalescing_report: ObjectStoreRequestCoalescingReport,
    policy: ObjectStoreDistributedSchedulingPolicy,
) -> ObjectStoreDistributedSchedulingReport {
    let status = object_store_distributed_scheduling_status(&coalescing_report, policy);
    let tasks = if status == ObjectStoreDistributedSchedulingStatus::Planned {
        object_store_distributed_task_plans(
            &coalescing_report.coalesced_range_report.requests,
            policy,
        )
    } else {
        Vec::new()
    };
    let diagnostics = object_store_distributed_scheduling_diagnostics(
        &coalescing_report,
        status,
        policy,
        object_store_task_count(
            coalescing_report.output_request_count,
            policy.max_requests_per_task,
        ),
    );

    ObjectStoreDistributedSchedulingReport {
        input_request_count: coalescing_report.output_request_count,
        planned_task_count: tasks.len(),
        estimated_request_bytes: coalescing_report.estimated_request_bytes_after,
        requires_checkpoint_plan: status == ObjectStoreDistributedSchedulingStatus::Planned,
        requires_retry_policy: status == ObjectStoreDistributedSchedulingStatus::Planned,
        requires_idempotency_keys: status == ObjectStoreDistributedSchedulingStatus::Planned,
        scheduler_execution_allowed: false,
        coordinator_started: false,
        worker_started: false,
        task_execution_allowed: false,
        can_plan_without_io: true,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        coalescing_report,
        policy,
        status,
        tasks,
        diagnostics,
    }
}

fn object_store_distributed_scheduling_status(
    report: &ObjectStoreRequestCoalescingReport,
    policy: ObjectStoreDistributedSchedulingPolicy,
) -> ObjectStoreDistributedSchedulingStatus {
    if !policy.valid() {
        ObjectStoreDistributedSchedulingStatus::BlockedInvalidPolicy
    } else if report.has_errors() {
        ObjectStoreDistributedSchedulingStatus::BlockedByCoalescing
    } else if report.output_request_count == 0 {
        ObjectStoreDistributedSchedulingStatus::BlockedEmptyRequests
    } else if object_store_task_count(report.output_request_count, policy.max_requests_per_task)
        > policy.max_task_count
    {
        ObjectStoreDistributedSchedulingStatus::BlockedTaskBudget
    } else {
        ObjectStoreDistributedSchedulingStatus::Planned
    }
}

fn object_store_task_count(request_count: usize, max_requests_per_task: usize) -> usize {
    if request_count == 0 || max_requests_per_task == 0 {
        0
    } else {
        request_count.div_ceil(max_requests_per_task)
    }
}

fn object_store_distributed_task_plans(
    requests: &[ObjectStoreRangeRequest],
    policy: ObjectStoreDistributedSchedulingPolicy,
) -> Vec<ObjectStoreDistributedTaskPlan> {
    requests
        .chunks(policy.max_requests_per_task)
        .enumerate()
        .map(|(index, chunk)| {
            let request_start_index = index * policy.max_requests_per_task;
            let range_count = chunk.iter().map(|request| request.source_range_count).sum();
            let estimated_request_bytes = chunk
                .iter()
                .map(ObjectStoreRangeRequest::estimated_bytes)
                .sum();
            let uri_count = chunk
                .iter()
                .map(|request| request.uri.as_str())
                .collect::<BTreeSet<_>>()
                .len();

            ObjectStoreDistributedTaskPlan {
                task_id: format!("object-store-task-{index:04}"),
                request_start_index,
                request_count: chunk.len(),
                range_count,
                uri_count,
                estimated_request_bytes,
                requires_retry_identity: true,
                requires_checkpoint_record: true,
                requires_idempotency_key: true,
                task_execution_allowed: false,
                object_store_io: false,
                write_io: false,
            }
        })
        .collect()
}

fn object_store_distributed_scheduling_diagnostics(
    report: &ObjectStoreRequestCoalescingReport,
    status: ObjectStoreDistributedSchedulingStatus,
    policy: ObjectStoreDistributedSchedulingPolicy,
    planned_task_count: usize,
) -> Vec<Diagnostic> {
    match status {
        ObjectStoreDistributedSchedulingStatus::Planned => Vec::new(),
        ObjectStoreDistributedSchedulingStatus::BlockedByCoalescing => {
            if report.diagnostics.is_empty() {
                vec![object_store_scheduling_error(
                    DiagnosticCode::ObjectStoreUnsupported,
                    "coalescing_report",
                    "object-store distributed scheduling requires successful request coalescing evidence",
                    "Fix range/coalescing blockers before planning distributed task shapes.",
                )]
            } else {
                report.diagnostics.clone()
            }
        }
        ObjectStoreDistributedSchedulingStatus::BlockedEmptyRequests => {
            vec![object_store_scheduling_error(
                DiagnosticCode::InvalidInput,
                "object_store_requests",
                "object-store distributed scheduling requires at least one planned request",
                "Attach successful range/coalescing evidence before planning distributed task shapes.",
            )]
        }
        ObjectStoreDistributedSchedulingStatus::BlockedTaskBudget => {
            vec![object_store_scheduling_error(
                DiagnosticCode::ResourceBudgetExceeded,
                "task_budget",
                format!(
                    "{planned_task_count} planned tasks exceed the max task budget of {}",
                    policy.max_task_count
                ),
                "Raise the task budget explicitly or coalesce request shapes before scheduling.",
            )]
        }
        ObjectStoreDistributedSchedulingStatus::BlockedInvalidPolicy => {
            vec![object_store_scheduling_error(
                DiagnosticCode::InvalidInput,
                "scheduler_policy",
                "object-store distributed scheduling requires positive task policy limits",
                "Set max_requests_per_task and max_task_count above zero.",
            )]
        }
    }
}

fn object_store_scheduling_error(
    code: DiagnosticCode,
    feature: impl Into<String>,
    message: impl Into<String>,
    suggested_next_step: impl Into<String>,
) -> Diagnostic {
    Diagnostic::new(
        code,
        DiagnosticSeverity::Error,
        DiagnosticCategory::ObjectStore,
        message,
        Some(feature.into()),
        Some("Object-store distributed scheduling is report-only and did not start a coordinator, worker, or storage request.".to_string()),
        Some(suggested_next_step.into()),
        FallbackStatus::disabled_by_policy(),
    )
}

/// Plans object-store checkpoint/retry/idempotency readiness without executing retries or writes.
#[must_use]
pub fn plan_object_store_checkpoint_retry(
    input: ObjectStoreCheckpointRetryInput,
) -> ObjectStoreCheckpointRetryReport {
    let status = object_store_checkpoint_retry_status(&input);
    let task_count = input.scheduling_report.planned_task_count;
    let ready = status == ObjectStoreCheckpointRetryStatus::Ready;
    let diagnostics = object_store_checkpoint_retry_diagnostics(&input, status);

    ObjectStoreCheckpointRetryReport {
        task_count,
        retryable_task_count: if ready { task_count } else { 0 },
        planned_checkpoint_record_count: if ready { task_count } else { 0 },
        planned_attempt_record_count: if ready { task_count } else { 0 },
        requires_retry_policy: !input.retry_policy_declared,
        requires_checkpoint_plan: !input.checkpoint_plan_declared,
        requires_idempotency_keys: !input.idempotency_keys_declared,
        requires_attempt_records: !input.attempt_record_declared,
        requires_cleanup_policy: !input.cleanup_policy_declared,
        retry_execution_allowed: false,
        checkpoint_write_allowed: false,
        cleanup_execution_allowed: false,
        coordinator_started: false,
        worker_started: false,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        input,
        status,
        diagnostics,
    }
}

fn object_store_checkpoint_retry_status(
    input: &ObjectStoreCheckpointRetryInput,
) -> ObjectStoreCheckpointRetryStatus {
    if input.scheduling_report.has_errors() {
        ObjectStoreCheckpointRetryStatus::BlockedByScheduling
    } else if !input.retry_policy_declared {
        ObjectStoreCheckpointRetryStatus::BlockedMissingRetryPolicy
    } else if !input.checkpoint_plan_declared {
        ObjectStoreCheckpointRetryStatus::BlockedMissingCheckpointPlan
    } else if !input.idempotency_keys_declared {
        ObjectStoreCheckpointRetryStatus::BlockedMissingIdempotency
    } else if !input.attempt_record_declared {
        ObjectStoreCheckpointRetryStatus::BlockedMissingAttemptRecord
    } else if !input.cleanup_policy_declared {
        ObjectStoreCheckpointRetryStatus::BlockedMissingCleanupPolicy
    } else {
        ObjectStoreCheckpointRetryStatus::Ready
    }
}

fn object_store_checkpoint_retry_diagnostics(
    input: &ObjectStoreCheckpointRetryInput,
    status: ObjectStoreCheckpointRetryStatus,
) -> Vec<Diagnostic> {
    match status {
        ObjectStoreCheckpointRetryStatus::Ready => Vec::new(),
        ObjectStoreCheckpointRetryStatus::BlockedByScheduling => {
            if input.scheduling_report.diagnostics.is_empty() {
                vec![object_store_checkpoint_retry_error(
                    DiagnosticCode::ObjectStoreUnsupported,
                    "scheduling_report",
                    "object-store checkpoint/retry planning requires successful scheduling evidence",
                    "Fix distributed scheduling blockers before planning checkpoint/retry readiness.",
                )]
            } else {
                input.scheduling_report.diagnostics.clone()
            }
        }
        ObjectStoreCheckpointRetryStatus::BlockedMissingRetryPolicy => {
            vec![object_store_checkpoint_retry_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "retry_policy",
                "object-store checkpoint/retry planning requires a declared retry policy",
                "Declare retry limits and retryable failure classes before distributed execution.",
            )]
        }
        ObjectStoreCheckpointRetryStatus::BlockedMissingCheckpointPlan => {
            vec![object_store_checkpoint_retry_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "checkpoint_plan",
                "object-store checkpoint/retry planning requires a checkpoint plan",
                "Declare checkpoint record identity and storage behavior before distributed execution.",
            )]
        }
        ObjectStoreCheckpointRetryStatus::BlockedMissingIdempotency => {
            vec![object_store_checkpoint_retry_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "idempotency_keys",
                "object-store checkpoint/retry planning requires task idempotency keys",
                "Declare stable task idempotency keys before distributed execution.",
            )]
        }
        ObjectStoreCheckpointRetryStatus::BlockedMissingAttemptRecord => {
            vec![object_store_checkpoint_retry_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "attempt_record",
                "object-store checkpoint/retry planning requires attempt record evidence",
                "Declare attempt record identity before distributed execution.",
            )]
        }
        ObjectStoreCheckpointRetryStatus::BlockedMissingCleanupPolicy => {
            vec![object_store_checkpoint_retry_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "cleanup_policy",
                "object-store checkpoint/retry planning requires cleanup policy evidence",
                "Declare cleanup behavior for failed attempts before distributed execution.",
            )]
        }
    }
}

fn object_store_checkpoint_retry_error(
    code: DiagnosticCode,
    feature: impl Into<String>,
    message: impl Into<String>,
    suggested_next_step: impl Into<String>,
) -> Diagnostic {
    Diagnostic::new(
        code,
        DiagnosticSeverity::Error,
        DiagnosticCategory::ObjectStore,
        message,
        Some(feature.into()),
        Some("Object-store checkpoint/retry planning is report-only and did not execute retries, write checkpoints, or contact storage.".to_string()),
        Some(suggested_next_step.into()),
        FallbackStatus::disabled_by_policy(),
    )
}

fn object_store_request_coalescing_status(
    uncoalesced: &ObjectStoreRangePlanningReport,
    coalesced: &ObjectStoreRangePlanningReport,
) -> ObjectStoreRequestCoalescingStatus {
    if uncoalesced.has_errors() || coalesced.has_errors() {
        ObjectStoreRequestCoalescingStatus::BlockedByRangePlanning
    } else if uncoalesced.planned_request_count > coalesced.planned_request_count {
        ObjectStoreRequestCoalescingStatus::Planned
    } else {
        ObjectStoreRequestCoalescingStatus::NoCoalescingNeeded
    }
}

fn object_store_request_coalescing_decisions(
    status: ObjectStoreRequestCoalescingStatus,
    input_request_count: usize,
    output_request_count: usize,
    coalesced_range_count: usize,
) -> Vec<ObjectStoreRequestCoalescingDecision> {
    match status {
        ObjectStoreRequestCoalescingStatus::Planned => {
            vec![ObjectStoreRequestCoalescingDecision::new(
                ObjectStoreRequestCoalescingDecisionKind::CoalesceAdjacentRanges,
                input_request_count,
                output_request_count,
                coalesced_range_count,
                "adjacent byte ranges fit within the request coalescing policy",
            )]
        }
        ObjectStoreRequestCoalescingStatus::NoCoalescingNeeded => {
            vec![ObjectStoreRequestCoalescingDecision::new(
                ObjectStoreRequestCoalescingDecisionKind::KeepSeparate,
                input_request_count,
                output_request_count,
                0,
                "declared ranges are already separated by policy or only one request is needed",
            )]
        }
        ObjectStoreRequestCoalescingStatus::BlockedByRangePlanning => {
            vec![ObjectStoreRequestCoalescingDecision::new(
                ObjectStoreRequestCoalescingDecisionKind::Blocked,
                input_request_count,
                output_request_count,
                0,
                "range planning must succeed before request coalescing can be planned",
            )]
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ObjectStoreRangeCounts {
    files: usize,
    segments: usize,
    object_store_files: usize,
    non_object_store_files: usize,
    ranged_segments: usize,
    missing_byte_range_segments: usize,
    invalid_ranges: usize,
    oversized_ranges: usize,
}

impl ObjectStoreRangeCounts {
    fn from_manifest(manifest: &DatasetManifest, policy: ObjectStoreRangePlanningPolicy) -> Self {
        Self {
            files: manifest.file_count(),
            segments: manifest.segment_count(),
            object_store_files: manifest
                .files
                .iter()
                .filter(|file| {
                    is_object_store_range_input_file(file) && is_object_store_uri(&file.uri)
                })
                .count(),
            non_object_store_files: manifest
                .files
                .iter()
                .filter(|file| {
                    is_object_store_range_input_file(file) && !is_object_store_uri(&file.uri)
                })
                .count(),
            ranged_segments: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_range_input_file(&segment.file)
                        && is_object_store_uri(&segment.file.uri)
                        && segment.segment.has_byte_ranges()
                })
                .count(),
            missing_byte_range_segments: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_range_input_file(&segment.file)
                        && is_object_store_uri(&segment.file.uri)
                        && !segment.segment.has_byte_ranges()
                })
                .count(),
            invalid_ranges: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_range_input_file(&segment.file)
                        && is_object_store_uri(&segment.file.uri)
                })
                .flat_map(|segment| segment.segment.layout.byte_ranges.iter())
                .filter(|range| range.is_empty())
                .count(),
            oversized_ranges: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_range_input_file(&segment.file)
                        && is_object_store_uri(&segment.file.uri)
                })
                .flat_map(|segment| segment.segment.layout.byte_ranges.iter())
                .filter(|range| range.length > policy.max_request_bytes)
                .count(),
        }
    }
}

fn object_store_range_status(counts: ObjectStoreRangeCounts) -> ObjectStoreRangePlanningStatus {
    if counts.segments == 0 {
        ObjectStoreRangePlanningStatus::Unsupported
    } else if counts.object_store_files == 0 || counts.non_object_store_files > 0 {
        ObjectStoreRangePlanningStatus::BlockedNonObjectStore
    } else if counts.invalid_ranges > 0 {
        ObjectStoreRangePlanningStatus::BlockedInvalidRanges
    } else if counts.oversized_ranges > 0 {
        ObjectStoreRangePlanningStatus::BlockedRequestBudget
    } else if counts.missing_byte_range_segments > 0 {
        ObjectStoreRangePlanningStatus::BlockedMissingByteRanges
    } else {
        ObjectStoreRangePlanningStatus::Planned
    }
}

fn is_object_store_uri(uri: &DatasetUri) -> bool {
    matches!(
        uri.scheme(),
        UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls
    )
}

fn is_object_store_range_input_file(file: &FileDescriptor) -> bool {
    file.role == FileRole::NativeVortexData
}

fn coalesced_object_store_ranges(
    manifest: &DatasetManifest,
    policy: ObjectStoreRangePlanningPolicy,
) -> Vec<ObjectStoreRangeRequest> {
    let mut requests = manifest
        .segments
        .iter()
        .filter(|segment| {
            is_object_store_range_input_file(&segment.file)
                && is_object_store_uri(&segment.file.uri)
        })
        .flat_map(|segment| {
            segment
                .segment
                .layout
                .byte_ranges
                .iter()
                .copied()
                .map(|range| {
                    ObjectStoreRangeRequest::new(
                        segment.file.uri.clone(),
                        segment.segment.id.clone(),
                        range,
                    )
                })
        })
        .collect::<Vec<_>>();

    requests.sort_by(|left, right| {
        left.uri
            .as_str()
            .cmp(right.uri.as_str())
            .then(left.range.start.cmp(&right.range.start))
            .then(left.range.length.cmp(&right.range.length))
    });

    if !policy.coalesce_adjacent_ranges {
        return requests;
    }

    let mut coalesced: Vec<ObjectStoreRangeRequest> = Vec::new();
    for request in requests {
        if let Some(last) = coalesced.last_mut()
            && can_coalesce_ranges(last, &request, policy)
        {
            let start = last.range.start.min(request.range.start);
            let end = last
                .range
                .end_exclusive()
                .max(request.range.end_exclusive());
            last.range = ByteRange::new(start, end.saturating_sub(start));
            last.segment_ids.extend(request.segment_ids);
            last.source_range_count += request.source_range_count;
            continue;
        }
        coalesced.push(request);
    }
    coalesced
}

fn can_coalesce_ranges(
    left: &ObjectStoreRangeRequest,
    right: &ObjectStoreRangeRequest,
    policy: ObjectStoreRangePlanningPolicy,
) -> bool {
    if left.uri != right.uri || left.source_range_count >= policy.max_ranges_per_request {
        return false;
    }
    let left_end = left.range.end_exclusive();
    let right_end = right.range.end_exclusive();
    let gap = right.range.start.saturating_sub(left_end);
    let merged_length = right_end
        .max(left_end)
        .saturating_sub(left.range.start.min(right.range.start));
    gap <= policy.max_coalesce_gap_bytes && merged_length <= policy.max_request_bytes
}

fn object_store_range_diagnostics(
    counts: ObjectStoreRangeCounts,
    status: ObjectStoreRangePlanningStatus,
) -> Vec<Diagnostic> {
    match status {
        ObjectStoreRangePlanningStatus::Planned => Vec::new(),
        ObjectStoreRangePlanningStatus::Unsupported => vec![object_store_range_error(
            DiagnosticCode::InvalidInput,
            "manifest_segments",
            "object-store range planning requires at least one declared segment",
            "Attach manifest segment metadata with byte ranges before planning object-store requests.",
        )],
        ObjectStoreRangePlanningStatus::BlockedNonObjectStore => vec![object_store_range_error(
            DiagnosticCode::ObjectStoreUnsupported,
            "object_store_uri",
            "object-store range planning requires every declared input data file to use an object-store URI",
            "Declare only S3, GCS, or ADLS input data file URIs before object-store range planning.",
        )],
        ObjectStoreRangePlanningStatus::BlockedInvalidRanges => vec![object_store_range_error(
            DiagnosticCode::InvalidInput,
            "byte_ranges",
            format!(
                "{} invalid empty byte ranges were declared",
                counts.invalid_ranges
            ),
            "Remove empty byte ranges before planning object-store requests.",
        )],
        ObjectStoreRangePlanningStatus::BlockedRequestBudget => vec![object_store_range_error(
            DiagnosticCode::ResourceBudgetExceeded,
            "request_budget",
            format!(
                "{} byte ranges exceed the per-request byte budget",
                counts.oversized_ranges
            ),
            "Split oversized byte ranges or raise the planning budget explicitly.",
        )],
        ObjectStoreRangePlanningStatus::BlockedMissingByteRanges => {
            vec![object_store_range_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "byte_ranges",
                format!(
                    "{} object-store segments are missing byte ranges",
                    counts.missing_byte_range_segments
                ),
                "Attach segment byte-range metadata before object-store range planning.",
            )]
        }
    }
}

fn object_store_range_error(
    code: DiagnosticCode,
    feature: impl Into<String>,
    message: impl Into<String>,
    suggested_next_step: impl Into<String>,
) -> Diagnostic {
    Diagnostic::new(
        code,
        DiagnosticSeverity::Error,
        DiagnosticCategory::ObjectStore,
        message,
        Some(feature.into()),
        Some("Object-store range planning is report-only and did not read storage.".to_string()),
        Some(suggested_next_step.into()),
        FallbackStatus::disabled_by_policy(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ColumnRef, DatasetFormat, DatasetRef, EncodedSegment, EncodingKind, FileDescriptor,
        FileRole, LayoutKind, LogicalDType, ManifestId, ManifestSegment, Nullability,
        SegmentLayout, SegmentStats, SnapshotId, SnapshotRef,
    };

    fn manifest_with_uri(uri: &str, ranges: Vec<ByteRange>) -> DatasetManifest {
        let dataset_uri = DatasetUri::new(uri).expect("uri");
        let mut manifest = DatasetManifest::new(
            ManifestId::new("m").expect("manifest id"),
            DatasetRef::from_uri(dataset_uri.clone()).expect("dataset ref"),
            SnapshotRef::new(SnapshotId::new("s").expect("snapshot id")),
        );
        let file = FileDescriptor::new(
            dataset_uri,
            DatasetFormat::Vortex,
            FileRole::NativeVortexData,
        )
        .with_size_bytes(128 * 1024 * 1024);
        let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
        layout.byte_ranges = ranges;
        layout.physical_size_bytes = Some(8 * 1024 * 1024);
        let segment = EncodedSegment::new(
            SegmentId::new("s1").expect("segment id"),
            ColumnRef::new("c").expect("column"),
            LogicalDType::Int64,
            Nullability::Nullable,
            layout,
            SegmentStats::with_row_count(64_000),
        );
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(segment, file));
        manifest
    }

    #[test]
    fn plans_s3_ranges_without_io() {
        let manifest = manifest_with_uri(
            "s3://bucket/table.vortex",
            vec![ByteRange::new(0, 1024), ByteRange::new(2048, 1024)],
        );

        let report = plan_object_store_ranges(
            manifest,
            ObjectStoreRangePlanningPolicy {
                max_coalesce_gap_bytes: 2048,
                ..ObjectStoreRangePlanningPolicy::default()
            },
        );

        assert_eq!(report.status, ObjectStoreRangePlanningStatus::Planned);
        assert_eq!(report.object_store_file_count, 1);
        assert_eq!(report.planned_range_count, 2);
        assert_eq!(report.planned_request_count, 1);
        assert_eq!(report.coalesced_range_count, 1);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn missing_byte_ranges_block_planning() {
        let report = plan_object_store_ranges(
            manifest_with_uri("s3://bucket/table.vortex", Vec::new()),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedMissingByteRanges
        );
        assert!(report.requires_byte_ranges);
        assert!(report.full_file_read_required);
        assert!(!report.full_file_read_allowed);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    #[test]
    fn local_files_are_not_object_store_range_targets() {
        let report = plan_object_store_ranges(
            manifest_with_uri("file://tmp/table.vortex", vec![ByteRange::new(0, 1024)]),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedNonObjectStore
        );
        assert_eq!(report.object_store_file_count, 0);
        assert!(report.has_errors());
    }

    #[test]
    fn mixed_local_and_object_store_files_block_range_planning() {
        let mut manifest =
            manifest_with_uri("s3://bucket/table.vortex", vec![ByteRange::new(0, 1024)]);
        let local_uri = DatasetUri::new("file://tmp/table.vortex").expect("uri");
        let local_file =
            FileDescriptor::new(local_uri, DatasetFormat::Vortex, FileRole::NativeVortexData)
                .with_size_bytes(128 * 1024 * 1024);
        let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
        layout.byte_ranges = vec![ByteRange::new(1024, 1024)];
        layout.physical_size_bytes = Some(8 * 1024 * 1024);
        let local_segment = EncodedSegment::new(
            SegmentId::new("s2").expect("segment id"),
            ColumnRef::new("c").expect("column"),
            LogicalDType::Int64,
            Nullability::Nullable,
            layout,
            SegmentStats::with_row_count(64_000),
        );
        manifest.add_file(local_file.clone());
        manifest.add_segment(ManifestSegment::new(local_segment, local_file));

        let report = plan_object_store_ranges(manifest, ObjectStoreRangePlanningPolicy::default());

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedNonObjectStore
        );
        assert_eq!(report.object_store_file_count, 1);
        assert_eq!(report.non_object_store_file_count, 1);
        assert!(report.has_errors());
    }

    #[test]
    fn local_compatibility_outputs_do_not_block_object_store_range_planning() {
        let mut manifest =
            manifest_with_uri("s3://bucket/table.vortex", vec![ByteRange::new(0, 1024)]);
        let compatibility_uri = DatasetUri::new("file://tmp/export.parquet").expect("uri");
        manifest.add_file(FileDescriptor::new(
            compatibility_uri,
            DatasetFormat::Parquet,
            FileRole::CompatibilityOutput,
        ));

        let report = plan_object_store_ranges(manifest, ObjectStoreRangePlanningPolicy::default());

        assert_eq!(report.status, ObjectStoreRangePlanningStatus::Planned);
        assert_eq!(report.file_count, 2);
        assert_eq!(report.object_store_file_count, 1);
        assert_eq!(report.non_object_store_file_count, 0);
        assert_eq!(report.planned_range_count, 1);
        assert!(!report.has_errors());
    }

    #[test]
    fn invalid_empty_ranges_block_planning() {
        let report = plan_object_store_ranges(
            manifest_with_uri("s3://bucket/table.vortex", vec![ByteRange::new(0, 0)]),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedInvalidRanges
        );
        assert_eq!(report.invalid_range_count, 1);
        assert!(report.has_errors());
    }

    #[test]
    fn request_coalescing_reduces_adjacent_requests_without_io() {
        let report = plan_object_store_request_coalescing(
            manifest_with_uri(
                "s3://bucket/table.vortex",
                vec![ByteRange::new(0, 1024), ByteRange::new(2048, 1024)],
            ),
            ObjectStoreRangePlanningPolicy {
                max_coalesce_gap_bytes: 2048,
                ..ObjectStoreRangePlanningPolicy::default()
            },
        );

        assert_eq!(report.status, ObjectStoreRequestCoalescingStatus::Planned);
        assert_eq!(report.input_request_count, 2);
        assert_eq!(report.output_request_count, 1);
        assert_eq!(report.request_reduction_count, 1);
        assert!(report.coalescing_applied);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn request_coalescing_blocks_when_range_planning_blocks() {
        let report = plan_object_store_request_coalescing(
            manifest_with_uri("s3://bucket/table.vortex", Vec::new()),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRequestCoalescingStatus::BlockedByRangePlanning
        );
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    fn distributed_scheduling_report(
        ranges: Vec<ByteRange>,
        scheduling_policy: ObjectStoreDistributedSchedulingPolicy,
    ) -> ObjectStoreDistributedSchedulingReport {
        let coalescing_report = plan_object_store_request_coalescing(
            manifest_with_uri("s3://bucket/table.vortex", ranges),
            ObjectStoreRangePlanningPolicy {
                max_coalesce_gap_bytes: 0,
                ..ObjectStoreRangePlanningPolicy::default()
            },
        );
        plan_object_store_distributed_scheduling(coalescing_report, scheduling_policy)
    }

    #[test]
    fn distributed_scheduling_plans_task_shapes_without_io() {
        let report = distributed_scheduling_report(
            vec![
                ByteRange::new(0, 1024),
                ByteRange::new(8192, 1024),
                ByteRange::new(16_384, 1024),
            ],
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 2,
                max_task_count: 4,
            },
        );

        assert_eq!(
            report.status,
            ObjectStoreDistributedSchedulingStatus::Planned
        );
        assert_eq!(report.input_request_count, 3);
        assert_eq!(report.planned_task_count, 2);
        assert!(report.requires_checkpoint_plan);
        assert!(report.requires_retry_policy);
        assert!(report.requires_idempotency_keys);
        assert!(report.tasks.iter().all(|task| !task.task_execution_allowed));
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn distributed_scheduling_blocks_when_coalescing_blocks() {
        let coalescing_report = plan_object_store_request_coalescing(
            manifest_with_uri("s3://bucket/table.vortex", Vec::new()),
            ObjectStoreRangePlanningPolicy::default(),
        );
        let report = plan_object_store_distributed_scheduling(
            coalescing_report,
            ObjectStoreDistributedSchedulingPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreDistributedSchedulingStatus::BlockedByCoalescing
        );
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    #[test]
    fn distributed_scheduling_blocks_task_budget() {
        let report = distributed_scheduling_report(
            vec![
                ByteRange::new(0, 1024),
                ByteRange::new(8192, 1024),
                ByteRange::new(16_384, 1024),
            ],
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 2,
            },
        );

        assert_eq!(
            report.status,
            ObjectStoreDistributedSchedulingStatus::BlockedTaskBudget
        );
        assert_eq!(report.planned_task_count, 0);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    fn ready_checkpoint_retry_input() -> ObjectStoreCheckpointRetryInput {
        ObjectStoreCheckpointRetryInput::new(distributed_scheduling_report(
            vec![ByteRange::new(0, 1024), ByteRange::new(8192, 1024)],
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 4,
            },
        ))
        .with_retry_policy(true)
        .with_checkpoint_plan(true)
        .with_idempotency_keys(true)
        .with_attempt_record(true)
        .with_cleanup_policy(true)
    }

    #[test]
    fn checkpoint_retry_ready_is_report_only() {
        let report = plan_object_store_checkpoint_retry(ready_checkpoint_retry_input());

        assert_eq!(report.status, ObjectStoreCheckpointRetryStatus::Ready);
        assert_eq!(report.task_count, 2);
        assert_eq!(report.retryable_task_count, 2);
        assert_eq!(report.planned_checkpoint_record_count, 2);
        assert_eq!(report.planned_attempt_record_count, 2);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn checkpoint_retry_blocks_when_scheduling_blocks() {
        let scheduling_report = distributed_scheduling_report(
            vec![
                ByteRange::new(0, 1024),
                ByteRange::new(8192, 1024),
                ByteRange::new(16_384, 1024),
            ],
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 2,
            },
        );
        let report = plan_object_store_checkpoint_retry(
            ObjectStoreCheckpointRetryInput::new(scheduling_report)
                .with_retry_policy(true)
                .with_checkpoint_plan(true)
                .with_idempotency_keys(true)
                .with_attempt_record(true)
                .with_cleanup_policy(true),
        );

        assert_eq!(
            report.status,
            ObjectStoreCheckpointRetryStatus::BlockedByScheduling
        );
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    #[test]
    fn checkpoint_retry_blocks_missing_idempotency() {
        let input = ready_checkpoint_retry_input().with_idempotency_keys(false);
        let report = plan_object_store_checkpoint_retry(input);

        assert_eq!(
            report.status,
            ObjectStoreCheckpointRetryStatus::BlockedMissingIdempotency
        );
        assert!(report.requires_idempotency_keys);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    fn ready_commit_input() -> ObjectStoreCommitProtocolInput {
        ObjectStoreCommitProtocolInput::new(
            DatasetUri::new("s3://bucket/table/_commit").expect("uri"),
        )
        .with_staging_prefix(true)
        .with_manifest_pointer_update(true)
        .with_commit_record(true)
        .with_idempotency_key(true)
        .with_cleanup_plan(true)
        .with_provider_atomic_commit(true)
    }

    fn ready_request_planner_report() -> ObjectStoreRequestPlannerReport {
        let manifest = manifest_with_uri(
            "s3://bucket/table.vortex",
            vec![
                ByteRange::new(0, 1024),
                ByteRange::new(8192, 1024),
                ByteRange::new(16_384, 1024),
            ],
        );
        let range_policy = ObjectStoreRangePlanningPolicy {
            max_coalesce_gap_bytes: 0,
            ..ObjectStoreRangePlanningPolicy::default()
        };
        let range_report = plan_object_store_ranges(manifest.clone(), range_policy);
        let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
        let scheduling_report = plan_object_store_distributed_scheduling(
            coalescing_report.clone(),
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 4,
            },
        );
        let checkpoint_retry_report = plan_object_store_checkpoint_retry(
            ObjectStoreCheckpointRetryInput::new(scheduling_report.clone())
                .with_retry_policy(true)
                .with_checkpoint_plan(true)
                .with_idempotency_keys(true)
                .with_attempt_record(true)
                .with_cleanup_policy(true),
        );
        let commit_report = plan_object_store_commit_protocol(ready_commit_input());

        plan_object_store_request_planner(
            range_report,
            coalescing_report,
            scheduling_report,
            checkpoint_retry_report,
            commit_report,
        )
    }

    #[test]
    fn request_planner_aggregates_ready_surfaces_without_io() {
        let report = ready_request_planner_report();

        assert_eq!(report.status, ObjectStoreRequestPlannerStatus::Planned);
        assert_eq!(
            report.schema_version,
            "shardloom.object_store_request_planner.v1"
        );
        assert_eq!(report.planned_surface_count, 5);
        assert_eq!(report.blocked_surface_count, 0);
        assert_eq!(report.planned_request_count, 3);
        assert_eq!(report.coalesced_request_count, 3);
        assert_eq!(report.planned_task_count, 3);
        assert_eq!(report.retryable_task_count, 3);
        assert_eq!(report.planned_checkpoint_record_count, 3);
        assert!(report.requires_checkpoint_plan);
        assert!(report.requires_retry_policy);
        assert!(report.requires_idempotency_keys);
        assert!(!report.requires_atomic_commit_evidence);
        assert_eq!(
            report.byte_range_provider_gate.status,
            ObjectStoreByteRangeProviderGateStatus::BlockedUntilCertified
        );
        assert!(
            report
                .byte_range_provider_gate
                .range_planning_evidence_present
        );
        assert!(report.byte_range_provider_gate.credential_policy_required);
        assert!(report.byte_range_provider_gate.retry_policy_required);
        assert!(report.byte_range_provider_gate.idempotency_key_required);
        assert!(report.byte_range_provider_gate.side_effect_free());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert_eq!(
            ObjectStoreRequestPlannerReport::surface_order(),
            vec![
                "range_planning",
                "request_coalescing",
                "distributed_scheduling",
                "checkpoint_retry",
                "commit_protocol"
            ]
        );
    }

    #[test]
    fn request_planner_blocks_on_range_planning() {
        let manifest = manifest_with_uri("s3://bucket/table.vortex", Vec::new());
        let range_policy = ObjectStoreRangePlanningPolicy::default();
        let range_report = plan_object_store_ranges(manifest.clone(), range_policy);
        let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
        let scheduling_report = plan_object_store_distributed_scheduling(
            coalescing_report.clone(),
            ObjectStoreDistributedSchedulingPolicy::default(),
        );
        let checkpoint_retry_report = plan_object_store_checkpoint_retry(
            ObjectStoreCheckpointRetryInput::new(scheduling_report.clone())
                .with_retry_policy(true)
                .with_checkpoint_plan(true)
                .with_idempotency_keys(true)
                .with_attempt_record(true)
                .with_cleanup_policy(true),
        );
        let commit_report = plan_object_store_commit_protocol(ready_commit_input());

        let report = plan_object_store_request_planner(
            range_report,
            coalescing_report,
            scheduling_report,
            checkpoint_retry_report,
            commit_report,
        );

        assert_eq!(
            report.status,
            ObjectStoreRequestPlannerStatus::BlockedByRangePlanning
        );
        assert!(report.requires_byte_ranges);
        assert!(
            !report
                .byte_range_provider_gate
                .range_planning_evidence_present
        );
        assert!(report.byte_range_provider_gate.side_effect_free());
        assert_eq!(report.planned_surface_count, 1);
        assert_eq!(report.blocked_surface_count, 4);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    #[test]
    fn request_planner_blocks_on_commit_evidence() {
        let ready = ready_request_planner_report();
        let commit_report =
            plan_object_store_commit_protocol(ready_commit_input().with_idempotency_key(false));
        let report = plan_object_store_request_planner(
            ready.range_report,
            ready.coalescing_report,
            ready.scheduling_report,
            ready.checkpoint_retry_report,
            commit_report,
        );

        assert_eq!(
            report.status,
            ObjectStoreRequestPlannerStatus::BlockedByCommit
        );
        assert!(report.requires_idempotency_keys);
        assert_eq!(report.planned_surface_count, 4);
        assert_eq!(report.blocked_surface_count, 1);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    fn assert_real_backend_absence_fields(report: &ObjectStoreRuntimePromotionGateReport) {
        assert!(!report.approved_real_backend_profile_declared);
        assert_eq!(report.approved_real_backend_profile_id, "not_declared");
        assert_eq!(
            report.approved_real_backend_profile_status,
            "missing_approved_real_backend_profile"
        );
        assert!(report.approved_real_backend_profile_required);
        assert!(!report.approved_real_backend_network_access_allowed);
        assert!(!report.approved_real_backend_credential_resolution_allowed);
        assert!(!report.approved_real_backend_read_allowed);
        assert!(!report.approved_real_backend_write_allowed);
        assert!(!report.production_object_store_native_io_certificate_present);
        assert!(!report.production_object_store_claim_allowed);
        assert_eq!(
            report.production_object_store_claim_gate_status,
            "not_claim_grade"
        );
    }

    #[test]
    fn object_store_runtime_gate_keeps_execution_surfaces_blocked() {
        let report = plan_object_store_runtime_promotion_gate();

        assert_eq!(
            report.schema_version,
            "shardloom.object_store_runtime_promotion_gate.v1"
        );
        assert_eq!(report.report_id, "cg10.object_store_runtime_promotion_gate");
        assert_eq!(report.surface_count(), 16);
        assert_eq!(report.existing_evidence_surface_count(), 2);
        assert_eq!(report.blocked_surface_count(), 14);
        assert_eq!(
            report.surface_order(),
            vec![
                "request_planner_aggregate",
                "byte_range_provider_gate",
                "range_read_execution",
                "request_coalescing_runtime",
                "distributed_coordinator_startup",
                "distributed_worker_startup",
                "distributed_task_execution",
                "checkpoint_write_execution",
                "retry_execution",
                "cleanup_execution",
                "object_store_commit_execution",
                "partition_discovery_runtime",
                "catalog_integration_runtime",
                "remote_result_delivery_runtime",
                "provider_credential_runtime",
                "benchmark_certificate_closeout",
            ]
        );
        assert!(report.existing_request_planner_evidence_present);
        assert!(report.existing_range_planning_evidence_present);
        assert!(report.existing_coalescing_evidence_present);
        assert!(report.existing_distributed_scheduling_evidence_present);
        assert!(report.existing_checkpoint_retry_evidence_present);
        assert!(report.existing_commit_protocol_evidence_present);
        assert_real_backend_absence_fields(&report);
        assert_eq!(
            report.byte_range_provider_gate.report_id,
            "gar0008a.object_store_byte_range_provider_gate"
        );
        assert!(report.byte_range_provider_gate.side_effect_free());
        assert_eq!(
            report.runtime_blocker_matrix_row_order(),
            vec![
                "coordinator_start",
                "worker_start",
                "task_execution",
                "checkpoint_write",
                "retry_attempt",
                "cleanup_execution",
                "commit_record_write",
                "partition_discovery",
                "catalog_integration",
                "remote_result_delivery",
            ]
        );
        assert_eq!(
            report.runtime_blocker_matrix_diagnostic_count(),
            report.runtime_blocker_matrix.len()
        );
        assert!(report.runtime_blocker_matrix_diagnostics_propagated());
        assert_eq!(
            report.diagnostics.len(),
            report.runtime_blocker_matrix.len()
        );
        assert!(report.diagnostics.iter().all(|diagnostic| diagnostic.code
            == DiagnosticCode::ObjectStoreUnsupported
            && diagnostic.severity == DiagnosticSeverity::Info
            && diagnostic.category == DiagnosticCategory::ObjectStore
            && !diagnostic.fallback.attempted
            && !diagnostic.fallback.allowed));
        assert_eq!(
            report
                .diagnostics
                .iter()
                .filter_map(|diagnostic| diagnostic.feature.as_deref())
                .collect::<Vec<_>>(),
            report.runtime_blocker_matrix_row_order()
        );
        assert!(report.runtime_blocker_matrix_all_allowed_false());
        assert!(report.runtime_blocker_matrix_all_no_io());
        assert!(report.runtime_blocker_matrix_all_no_fallback());
        assert!(report.runtime_blocker_matrix_all_no_external_engine());
        assert!(
            report
                .runtime_blocker_matrix
                .iter()
                .all(ObjectStoreRuntimeBlockerMatrixRow::side_effect_free)
        );
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn object_store_runtime_gate_requires_evidence_before_runtime() {
        let report = plan_object_store_runtime_promotion_gate();

        assert!(report.range_planning_evidence_required);
        assert!(report.request_budget_policy_required);
        assert!(report.provider_capability_policy_required);
        assert!(report.credential_effect_policy_required);
        assert!(report.scheduler_policy_required);
        assert!(report.worker_identity_required);
        assert!(report.checkpoint_plan_required);
        assert!(report.retry_policy_required);
        assert!(report.idempotency_keys_required);
        assert!(report.attempt_records_required);
        assert!(report.cleanup_policy_required);
        assert!(report.atomic_commit_evidence_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.benchmark_evidence_required);
        assert!(
            report
                .byte_range_provider_gate
                .provider_capability_policy_required
        );
        assert!(report.byte_range_provider_gate.credential_policy_required);
        assert!(report.byte_range_provider_gate.retry_policy_required);
        assert!(report.byte_range_provider_gate.idempotency_key_required);
        assert!(
            report
                .byte_range_provider_gate
                .execution_certificate_required
        );
        assert!(
            report
                .byte_range_provider_gate
                .native_io_certificate_required
        );
        assert!(report.byte_range_provider_gate.benchmark_evidence_required);
        assert!(!report.range_read_execution_allowed);
        assert!(!report.full_file_read_allowed);
        assert!(!report.byte_range_provider_gate.range_read_execution_allowed);
        assert!(
            !report
                .byte_range_provider_gate
                .credential_resolution_allowed
        );
        assert!(!report.byte_range_provider_gate.provider_probe);
        assert!(!report.byte_range_provider_gate.network_probe);
        assert!(!report.byte_range_provider_gate.object_store_io);
        assert!(!report.coordinator_start_allowed);
        assert!(!report.worker_start_allowed);
        assert!(!report.task_execution_allowed);
        assert!(!report.retry_execution_allowed);
        assert!(!report.checkpoint_write_allowed);
        assert!(!report.cleanup_execution_allowed);
        assert!(!report.commit_execution_allowed);
        assert!(!report.object_store_io_allowed);
        assert!(!report.data_read_allowed);
        assert!(!report.write_io_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        let retry = report
            .runtime_blocker_matrix
            .iter()
            .find(|row| row.action == "retry_attempt")
            .expect("retry row");
        assert_eq!(
            retry.diagnostic_code,
            DiagnosticCode::ObjectStoreUnsupported
        );
        assert_eq!(
            retry.to_diagnostic().code,
            DiagnosticCode::ObjectStoreUnsupported
        );
        assert!(retry.required_evidence.contains("retry_policy"));
        assert!(!retry.retry_attempted);
        let commit = report
            .runtime_blocker_matrix
            .iter()
            .find(|row| row.action == "commit_record_write")
            .expect("commit row");
        assert!(commit.required_evidence.contains("commit_record_schema"));
        assert!(!commit.commit_record_written);
        let partition = report
            .runtime_blocker_matrix
            .iter()
            .find(|row| row.action == "partition_discovery")
            .expect("partition discovery row");
        assert!(
            partition
                .required_evidence
                .contains("partition_listing_policy")
        );
        assert!(!partition.object_store_io);
        let catalog = report
            .runtime_blocker_matrix
            .iter()
            .find(|row| row.action == "catalog_integration")
            .expect("catalog integration row");
        assert!(catalog.required_evidence.contains("catalog_adapter_policy"));
        assert!(!catalog.external_engine_invoked);
        let remote_delivery = report
            .runtime_blocker_matrix
            .iter()
            .find(|row| row.action == "remote_result_delivery")
            .expect("remote result delivery row");
        assert!(
            remote_delivery
                .required_evidence
                .contains("remote_delivery_protocol")
        );
        assert!(!remote_delivery.write_io);
    }

    #[test]
    fn object_store_commit_protocol_ready_is_report_only() {
        let report = plan_object_store_commit_protocol(ready_commit_input());

        assert_eq!(report.status, ObjectStoreCommitProtocolStatus::Ready);
        assert!(report.object_store_target);
        assert!(!report.commit_execution_allowed);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn object_store_commit_protocol_blocks_non_object_store() {
        let input = ObjectStoreCommitProtocolInput::new(
            DatasetUri::new("file://tmp/table/_commit").expect("uri"),
        )
        .with_staging_prefix(true)
        .with_manifest_pointer_update(true)
        .with_commit_record(true)
        .with_idempotency_key(true)
        .with_cleanup_plan(true)
        .with_provider_atomic_commit(true);
        let report = plan_object_store_commit_protocol(input);

        assert_eq!(
            report.status,
            ObjectStoreCommitProtocolStatus::BlockedNonObjectStore
        );
        assert!(!report.object_store_target);
        assert!(report.has_errors());
        assert_eq!(
            report.diagnostics[0].code,
            DiagnosticCode::ObjectStoreUnsupported
        );
    }

    #[test]
    fn object_store_commit_protocol_blocks_missing_idempotency() {
        let input = ready_commit_input().with_idempotency_key(false);
        let report = plan_object_store_commit_protocol(input);

        assert_eq!(
            report.status,
            ObjectStoreCommitProtocolStatus::BlockedMissingIdempotency
        );
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::InvalidInput);
        assert!(report.requires_idempotency_key);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    #[test]
    fn object_store_commit_protocol_atomicity_uses_atomicity_diagnostic_code() {
        let input = ready_commit_input().with_provider_atomic_commit(false);
        let report = plan_object_store_commit_protocol(input);

        assert_eq!(
            report.status,
            ObjectStoreCommitProtocolStatus::BlockedAtomicity
        );
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::CommitNotAtomic);
    }
}
