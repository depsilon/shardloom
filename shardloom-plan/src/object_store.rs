//! Object-store range planning evidence.
//!
//! This module plans request shapes from already-declared manifest byte-range metadata.
//! It performs no object-store IO, no file reads, no data materialization, and no fallback execution.

use std::collections::BTreeSet;

use shardloom_core::{
    ByteRange, DatasetManifest, DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode,
    DiagnosticSeverity, FallbackStatus, SegmentId, UriScheme,
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
    let (feature, message, next) = match status {
        ObjectStoreCommitProtocolStatus::Ready => unreachable!("handled above"),
        ObjectStoreCommitProtocolStatus::BlockedNonObjectStore => (
            "object_store_target",
            "object-store commit protocol requires an S3, GCS, or ADLS target",
            "Use object-store targets only for object-store commit protocol planning.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingStaging => (
            "staging_prefix",
            "object-store commit protocol requires a declared staging prefix",
            "Declare a staging prefix before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingManifestPointer => (
            "manifest_pointer",
            "object-store commit protocol requires a manifest pointer update plan",
            "Declare manifest pointer update evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingCommitRecord => (
            "commit_record",
            "object-store commit protocol requires a commit record",
            "Declare commit record evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingIdempotency => (
            "idempotency_key",
            "object-store commit protocol requires an idempotency key",
            "Declare idempotency key evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedMissingCleanup => (
            "cleanup_plan",
            "object-store commit protocol requires cleanup planning",
            "Declare cleanup evidence before planning object-store commits.",
        ),
        ObjectStoreCommitProtocolStatus::BlockedAtomicity => (
            "atomic_commit",
            "object-store commit protocol requires atomicity evidence",
            "Declare provider atomicity or pointer-swap evidence before planning object-store commits.",
        ),
    };
    vec![Diagnostic::new(
        DiagnosticCode::CommitNotAtomic,
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
                .filter(|file| is_object_store_uri(&file.uri))
                .count(),
            non_object_store_files: manifest
                .files
                .iter()
                .filter(|file| !is_object_store_uri(&file.uri))
                .count(),
            ranged_segments: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_uri(&segment.file.uri) && segment.segment.has_byte_ranges()
                })
                .count(),
            missing_byte_range_segments: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_uri(&segment.file.uri) && !segment.segment.has_byte_ranges()
                })
                .count(),
            invalid_ranges: manifest
                .segments
                .iter()
                .filter(|segment| is_object_store_uri(&segment.file.uri))
                .flat_map(|segment| segment.segment.layout.byte_ranges.iter())
                .filter(|range| range.is_empty())
                .count(),
            oversized_ranges: manifest
                .segments
                .iter()
                .filter(|segment| is_object_store_uri(&segment.file.uri))
                .flat_map(|segment| segment.segment.layout.byte_ranges.iter())
                .filter(|range| range.length > policy.max_request_bytes)
                .count(),
        }
    }
}

fn object_store_range_status(counts: ObjectStoreRangeCounts) -> ObjectStoreRangePlanningStatus {
    if counts.segments == 0 {
        ObjectStoreRangePlanningStatus::Unsupported
    } else if counts.object_store_files == 0 {
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

fn coalesced_object_store_ranges(
    manifest: &DatasetManifest,
    policy: ObjectStoreRangePlanningPolicy,
) -> Vec<ObjectStoreRangeRequest> {
    let mut requests = manifest
        .segments
        .iter()
        .filter(|segment| is_object_store_uri(&segment.file.uri))
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
        if let Some(last) = coalesced.last_mut() {
            if can_coalesce_ranges(last, &request, policy) {
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
            "no object-store input files were declared",
            "Declare S3, GCS, or ADLS file URIs before object-store range planning.",
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
    }

    #[test]
    fn object_store_commit_protocol_blocks_missing_idempotency() {
        let input = ready_commit_input().with_idempotency_key(false);
        let report = plan_object_store_commit_protocol(input);

        assert_eq!(
            report.status,
            ObjectStoreCommitProtocolStatus::BlockedMissingIdempotency
        );
        assert!(report.requires_idempotency_key);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }
}
