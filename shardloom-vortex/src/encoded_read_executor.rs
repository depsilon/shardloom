#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools,
    clippy::too_many_lines
)]

use std::fmt::Write as _;

#[cfg(feature = "vortex-encoded-read-spike")]
use shardloom_core::UriScheme;
use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId,
};
use shardloom_exec::TaskId;

use crate::{
    VortexEncodedCountDataPathApprovalReport, VortexEncodedReadApiBoundaryStatus,
    VortexEncodedReadCandidateKind, VortexEncodedReadReadinessReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadExecutorFeatureStatus {
    Disabled,
    Enabled,
    Unsupported,
}
impl VortexEncodedReadExecutorFeatureStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadExecutionStatus {
    FeatureDisabled,
    Ready,
    BlockedByReadiness,
    BlockedByMissingEstimate,
    BlockedByMissingByteRange,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedBySpillIo,
    BlockedByUnsupportedInput,
    NoEncodedReadCandidates,
    WouldExecuteEncodedRead,
    LocalScanEncodedCountExecuted,
    Unsupported,
}
impl VortexEncodedReadExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Ready => "ready",
            Self::BlockedByReadiness => "blocked_by_readiness",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByMissingByteRange => "blocked_by_missing_byte_range",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::BlockedByWriteIo => "blocked_by_write_io",
            Self::BlockedBySpillIo => "blocked_by_spill_io",
            Self::BlockedByUnsupportedInput => "blocked_by_unsupported_input",
            Self::NoEncodedReadCandidates => "no_encoded_read_candidates",
            Self::WouldExecuteEncodedRead => "would_execute_encoded_read",
            Self::LocalScanEncodedCountExecuted => "local_scan_encoded_count_executed",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByReadiness
                | Self::BlockedByMissingEstimate
                | Self::BlockedByMissingByteRange
                | Self::BlockedByDecodeRisk
                | Self::BlockedByMaterializationRisk
                | Self::BlockedByObjectStoreIo
                | Self::BlockedByWriteIo
                | Self::BlockedBySpillIo
                | Self::BlockedByUnsupportedInput
                | Self::Unsupported
        )
    }
    pub const fn would_execute_anything(&self) -> bool {
        matches!(
            self,
            Self::WouldExecuteEncodedRead | Self::LocalScanEncodedCountExecuted
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadExecutionMode {
    ReportOnly,
    EncodedReadContractOnly,
    LocalScanEncodedArrayLengthCount,
    Unsupported,
}
impl VortexEncodedReadExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::EncodedReadContractOnly => "encoded_read_contract_only",
            Self::LocalScanEncodedArrayLengthCount => "local_scan_encoded_array_length_count",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn reads_data(&self) -> bool {
        matches!(self, Self::LocalScanEncodedArrayLengthCount)
    }
    pub const fn decodes_data(&self) -> bool {
        false
    }
    pub const fn materializes_data(&self) -> bool {
        false
    }
    pub const fn writes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadExecutionDecisionKind {
    WouldExecuteEncodedRead,
    NoCandidate,
    MetadataOnlyNoRead,
    PrunedNoRead,
    BlockedMissingEstimate,
    BlockedMissingByteRange,
    BlockedDecodeRisk,
    BlockedMaterializationRisk,
    BlockedObjectStoreIo,
    BlockedWriteIo,
    BlockedSpillIo,
    BlockedUnsupported,
}
impl VortexEncodedReadExecutionDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::WouldExecuteEncodedRead => "would_execute_encoded_read",
            Self::NoCandidate => "no_candidate",
            Self::MetadataOnlyNoRead => "metadata_only_no_read",
            Self::PrunedNoRead => "pruned_no_read",
            Self::BlockedMissingEstimate => "blocked_missing_estimate",
            Self::BlockedMissingByteRange => "blocked_missing_byte_range",
            Self::BlockedDecodeRisk => "blocked_decode_risk",
            Self::BlockedMaterializationRisk => "blocked_materialization_risk",
            Self::BlockedObjectStoreIo => "blocked_object_store_io",
            Self::BlockedWriteIo => "blocked_write_io",
            Self::BlockedSpillIo => "blocked_spill_io",
            Self::BlockedUnsupported => "blocked_unsupported",
        }
    }
    pub const fn is_candidate(&self) -> bool {
        matches!(self, Self::WouldExecuteEncodedRead)
    }
    pub const fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::BlockedMissingEstimate
                | Self::BlockedMissingByteRange
                | Self::BlockedDecodeRisk
                | Self::BlockedMaterializationRisk
                | Self::BlockedObjectStoreIo
                | Self::BlockedWriteIo
                | Self::BlockedSpillIo
                | Self::BlockedUnsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadExecutionDecision {
    pub kind: VortexEncodedReadExecutionDecisionKind,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub split_id: Option<String>,
    pub readiness_candidate_kind: Option<String>,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadExecutionDecision {
    fn base(
        kind: VortexEncodedReadExecutionDecisionKind,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id,
            segment_id: None,
            split_id: None,
            readiness_candidate_kind: None,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn would_execute_encoded_read(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::WouldExecuteEncodedRead,
            task_id,
            reason,
        )
    }
    pub fn no_candidate(reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::NoCandidate,
            None,
            reason,
        )
    }
    pub fn metadata_only(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::MetadataOnlyNoRead,
            task_id,
            reason,
        )
    }
    pub fn pruned(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::PrunedNoRead,
            task_id,
            reason,
        )
    }
    pub fn blocked_missing_estimate(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedMissingEstimate,
            task_id,
            reason,
        )
    }
    pub fn blocked_missing_byte_range(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedMissingByteRange,
            task_id,
            reason,
        )
    }
    pub fn blocked_decode_risk(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedDecodeRisk,
            task_id,
            reason,
        )
    }
    pub fn blocked_materialization_risk(
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedMaterializationRisk,
            task_id,
            reason,
        )
    }
    pub fn blocked_object_store_io(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedObjectStoreIo,
            task_id,
            reason,
        )
    }
    pub fn blocked_write_io(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedWriteIo,
            task_id,
            reason,
        )
    }
    pub fn blocked_spill_io(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedSpillIo,
            task_id,
            reason,
        )
    }
    pub fn blocked_unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexEncodedReadExecutionDecisionKind::BlockedUnsupported,
            task_id,
            "unsupported encoded-read executor decision",
        );
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    pub fn with_split_id(mut self, split_id: impl Into<String>) -> Self {
        self.split_id = Some(split_id.into());
        self
    }
    pub fn with_readiness_candidate_kind(
        mut self,
        readiness_candidate_kind: impl Into<String>,
    ) -> Self {
        self.readiness_candidate_kind = Some(readiness_candidate_kind.into());
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub const fn is_candidate(&self) -> bool {
        self.kind.is_candidate()
    }
    pub const fn is_blocked(&self) -> bool {
        self.kind.is_blocked()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn summary(&self) -> String {
        format!(
            "encoded-read executor decision (contract only, no execution) kind={} reason={}",
            self.kind.as_str(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadExecutionInput {
    pub readiness_report: VortexEncodedReadReadinessReport,
    pub allow_encoded_read_execution: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadExecutionInput {
    pub fn new(readiness_report: VortexEncodedReadReadinessReport) -> Self {
        Self {
            readiness_report,
            allow_encoded_read_execution: false,
            diagnostics: vec![],
        }
    }
    pub const fn allow_encoded_read_execution(mut self, value: bool) -> Self {
        self.allow_encoded_read_execution = value;
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.readiness_report.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn summary(&self) -> String {
        format!(
            "encoded-read execution input allow_encoded_read_execution={}",
            self.allow_encoded_read_execution
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedReadExecutionReport {
    pub feature_status: VortexEncodedReadExecutorFeatureStatus,
    pub status: VortexEncodedReadExecutionStatus,
    pub mode: VortexEncodedReadExecutionMode,
    pub input: VortexEncodedReadExecutionInput,
    pub decisions: Vec<VortexEncodedReadExecutionDecision>,
    pub future_encoded_read_candidates: usize,
    pub would_execute_encoded_read_count: usize,
    pub blocked_count: usize,
    pub metadata_only_count: usize,
    pub pruned_count: usize,
    pub missing_estimate_count: usize,
    pub missing_byte_range_count: usize,
    pub decode_blocked_count: usize,
    pub materialization_blocked_count: usize,
    pub object_store_blocked_count: usize,
    pub write_blocked_count: usize,
    pub spill_blocked_count: usize,
    pub unsupported_blocked_count: usize,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub upstream_scan_called: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub arrays_read_count: usize,
    pub rows_counted: u64,
    pub count_result: Option<u64>,
    pub local_scan_target_uri: Option<DatasetUri>,
    pub local_scan_readiness_source_uri: Option<DatasetUri>,
    pub local_scan_source_uri_matches_target: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadExecutionReport {
    pub fn feature_disabled(input: VortexEncodedReadExecutionInput) -> Self {
        Self {
            feature_status: VortexEncodedReadExecutorFeatureStatus::Disabled,
            status: VortexEncodedReadExecutionStatus::FeatureDisabled,
            mode: VortexEncodedReadExecutionMode::ReportOnly,
            input,
            decisions: vec![],
            future_encoded_read_candidates: 0,
            would_execute_encoded_read_count: 0,
            blocked_count: 0,
            metadata_only_count: 0,
            pruned_count: 0,
            missing_estimate_count: 0,
            missing_byte_range_count: 0,
            decode_blocked_count: 0,
            materialization_blocked_count: 0,
            object_store_blocked_count: 0,
            write_blocked_count: 0,
            spill_blocked_count: 0,
            unsupported_blocked_count: 0,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            upstream_scan_called: false,
            row_read: false,
            arrow_converted: false,
            arrays_read_count: 0,
            rows_counted: 0,
            count_result: None,
            local_scan_target_uri: None,
            local_scan_readiness_source_uri: None,
            local_scan_source_uri_matches_target: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    /// # Errors
    /// Returns an error only if internal contract construction fails.
    pub fn from_input(input: VortexEncodedReadExecutionInput) -> Result<Self> {
        if !vortex_encoded_read_executor_feature_enabled() {
            return Ok(Self::feature_disabled(input));
        }
        let mut r = Self {
            feature_status: VortexEncodedReadExecutorFeatureStatus::Enabled,
            status: VortexEncodedReadExecutionStatus::Ready,
            mode: VortexEncodedReadExecutionMode::EncodedReadContractOnly,
            input,
            decisions: vec![],
            future_encoded_read_candidates: 0,
            would_execute_encoded_read_count: 0,
            blocked_count: 0,
            metadata_only_count: 0,
            pruned_count: 0,
            missing_estimate_count: 0,
            missing_byte_range_count: 0,
            decode_blocked_count: 0,
            materialization_blocked_count: 0,
            object_store_blocked_count: 0,
            write_blocked_count: 0,
            spill_blocked_count: 0,
            unsupported_blocked_count: 0,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            upstream_scan_called: false,
            row_read: false,
            arrow_converted: false,
            arrays_read_count: 0,
            rows_counted: 0,
            count_result: None,
            local_scan_target_uri: None,
            local_scan_readiness_source_uri: None,
            local_scan_source_uri_matches_target: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        for c in r.input.readiness_report.candidates.clone() {
            let mut d = match c.kind {
                VortexEncodedReadCandidateKind::EncodedReadCandidate => {
                    VortexEncodedReadExecutionDecision::would_execute_encoded_read(
                        c.task_id,
                        if r.input.allow_encoded_read_execution {
                            "candidate classified for future encoded-read execution"
                        } else {
                            "encoded-read execution is not enabled by input contract"
                        },
                    )
                }
                VortexEncodedReadCandidateKind::MetadataOnlyNoRead => {
                    VortexEncodedReadExecutionDecision::metadata_only(c.task_id, c.reason.clone())
                }
                VortexEncodedReadCandidateKind::PrunedNoRead => {
                    VortexEncodedReadExecutionDecision::pruned(c.task_id, c.reason.clone())
                }
                VortexEncodedReadCandidateKind::NeedsEstimate => {
                    VortexEncodedReadExecutionDecision::blocked_missing_estimate(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::NeedsByteRange => {
                    VortexEncodedReadExecutionDecision::blocked_missing_byte_range(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::WouldDecode => {
                    VortexEncodedReadExecutionDecision::blocked_decode_risk(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::WouldMaterialize => {
                    VortexEncodedReadExecutionDecision::blocked_materialization_risk(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::WouldUseObjectStore => {
                    VortexEncodedReadExecutionDecision::blocked_object_store_io(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::WouldWrite => {
                    VortexEncodedReadExecutionDecision::blocked_write_io(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::WouldSpill => {
                    VortexEncodedReadExecutionDecision::blocked_spill_io(
                        c.task_id,
                        c.reason.clone(),
                    )
                }
                VortexEncodedReadCandidateKind::Unsupported => {
                    VortexEncodedReadExecutionDecision::blocked_unsupported(
                        c.task_id,
                        "vortex_encoded_read_executor",
                        c.reason.clone(),
                    )
                }
            };
            if let Some(x) = c.segment_id {
                d = d.with_segment_id(x);
            }
            if let Some(x) = c.split_id {
                d = d.with_split_id(x);
            }
            d = d.with_readiness_candidate_kind(c.kind.as_str());
            for dg in c.diagnostics {
                d.add_diagnostic(dg);
            }
            r.add_decision(d);
        }
        r.diagnostics.extend(r.input.diagnostics.clone());
        r.diagnostics
            .extend(r.input.readiness_report.diagnostics.clone());
        r.recompute_counts();
        if r.decisions.is_empty() {
            r.add_decision(VortexEncodedReadExecutionDecision::no_candidate(
                "no encoded-read candidates",
            ));
            r.status = VortexEncodedReadExecutionStatus::NoEncodedReadCandidates;
        } else if r.blocked_count > 0 {
            r.status = if r.decode_blocked_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByDecodeRisk
            } else if r.materialization_blocked_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByMaterializationRisk
            } else if r.object_store_blocked_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByObjectStoreIo
            } else if r.write_blocked_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByWriteIo
            } else if r.spill_blocked_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedBySpillIo
            } else if r.unsupported_blocked_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput
            } else if r.missing_estimate_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByMissingEstimate
            } else if r.missing_byte_range_count > 0 {
                VortexEncodedReadExecutionStatus::BlockedByMissingByteRange
            } else {
                VortexEncodedReadExecutionStatus::BlockedByReadiness
            };
        } else if r.would_execute_encoded_read_count > 0 {
            r.status = VortexEncodedReadExecutionStatus::WouldExecuteEncodedRead;
        }
        Ok(r)
    }
    pub fn unsupported(
        input: VortexEncodedReadExecutionInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self::feature_disabled(input);
        r.feature_status = VortexEncodedReadExecutorFeatureStatus::Unsupported;
        r.status = VortexEncodedReadExecutionStatus::Unsupported;
        r.mode = VortexEncodedReadExecutionMode::Unsupported;
        r.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        r
    }
    pub fn add_decision(&mut self, d: VortexEncodedReadExecutionDecision) {
        self.decisions.push(d);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn recompute_counts(&mut self) {
        self.future_encoded_read_candidates =
            self.decisions.iter().filter(|d| d.is_candidate()).count();
        self.would_execute_encoded_read_count = self.future_encoded_read_candidates;
        self.blocked_count = self.decisions.iter().filter(|d| d.is_blocked()).count();
        self.metadata_only_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::MetadataOnlyNoRead
                )
            })
            .count();
        self.pruned_count = self
            .decisions
            .iter()
            .filter(|d| matches!(d.kind, VortexEncodedReadExecutionDecisionKind::PrunedNoRead))
            .count();
        self.missing_estimate_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedMissingEstimate
                )
            })
            .count();
        self.missing_byte_range_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedMissingByteRange
                )
            })
            .count();
        self.decode_blocked_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedDecodeRisk
                )
            })
            .count();
        self.materialization_blocked_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedMaterializationRisk
                )
            })
            .count();
        self.object_store_blocked_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedObjectStoreIo
                )
            })
            .count();
        self.write_blocked_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedWriteIo
                )
            })
            .count();
        self.spill_blocked_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedSpillIo
                )
            })
            .count();
        self.unsupported_blocked_count = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexEncodedReadExecutionDecisionKind::BlockedUnsupported
                )
            })
            .count();
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.input.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .decisions
                .iter()
                .any(VortexEncodedReadExecutionDecision::has_errors)
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.upstream_scan_called
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(o, "vortex encoded-read executor skeleton report");
        let _ = writeln!(o, "feature status: {}", self.feature_status.as_str());
        let _ = writeln!(o, "execution status: {}", self.status.as_str());
        let _ = writeln!(o, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            o,
            "future encoded-read candidates: {}",
            self.future_encoded_read_candidates
        );
        let _ = writeln!(
            o,
            "would execute encoded read count: {}",
            self.would_execute_encoded_read_count
        );
        let _ = writeln!(o, "blocked count: {}", self.blocked_count);
        let _ = writeln!(o, "missing estimate count: {}", self.missing_estimate_count);
        let _ = writeln!(
            o,
            "missing byte range count: {}",
            self.missing_byte_range_count
        );
        let _ = writeln!(o, "decode blocked count: {}", self.decode_blocked_count);
        let _ = writeln!(
            o,
            "materialization blocked count: {}",
            self.materialization_blocked_count
        );
        let _ = writeln!(
            o,
            "object-store blocked count: {}",
            self.object_store_blocked_count
        );
        let _ = writeln!(o, "write blocked count: {}", self.write_blocked_count);
        let _ = writeln!(o, "spill blocked count: {}", self.spill_blocked_count);
        let _ = writeln!(
            o,
            "unsupported blocked count: {}",
            self.unsupported_blocked_count
        );
        let _ = writeln!(o, "data read: {}", self.data_read);
        let _ = writeln!(o, "data decoded: {}", self.data_decoded);
        let _ = writeln!(o, "data materialized: {}", self.data_materialized);
        let _ = writeln!(o, "upstream scan called: {}", self.upstream_scan_called);
        let _ = writeln!(o, "row read: {}", self.row_read);
        let _ = writeln!(o, "Arrow converted: {}", self.arrow_converted);
        let _ = writeln!(o, "arrays read count: {}", self.arrays_read_count);
        let _ = writeln!(o, "rows counted: {}", self.rows_counted);
        let _ = writeln!(
            o,
            "count result: {}",
            self.count_result
                .map_or_else(|| "none".to_string(), |count| count.to_string())
        );
        let _ = writeln!(
            o,
            "local scan target URI: {}",
            self.local_scan_target_uri
                .as_ref()
                .map_or("<none>", DatasetUri::as_str)
        );
        let _ = writeln!(
            o,
            "local scan readiness source URI: {}",
            self.local_scan_readiness_source_uri
                .as_ref()
                .map_or("<none>", DatasetUri::as_str)
        );
        let _ = writeln!(
            o,
            "local scan source URI matches target: {}",
            self.local_scan_source_uri_matches_target
        );
        let _ = writeln!(o, "object-store IO: {}", self.object_store_io);
        let _ = writeln!(o, "write IO: {}", self.write_io);
        let _ = writeln!(o, "spill IO performed: {}", self.spill_io_performed);
        let _ = writeln!(
            o,
            "external effects executed: {}",
            self.external_effects_executed
        );
        let _ = writeln!(o, "fallback execution disabled");
        if !self.input.allow_encoded_read_execution {
            let _ = writeln!(o, "encoded-read execution is not enabled by input contract");
        }
        if !self.diagnostics.is_empty() {
            let _ = writeln!(o, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(o, "- [{}] {}", d.code.as_str(), d.message);
            }
        }
        o
    }
}

pub const fn vortex_encoded_read_executor_feature_enabled() -> bool {
    cfg!(feature = "vortex-encoded-read-executor")
}

#[must_use]
pub const fn vortex_encoded_read_spike_feature_enabled() -> bool {
    cfg!(feature = "vortex-encoded-read-spike")
}
/// # Errors
/// Returns an error if encoded-read contract report construction fails.
pub fn execute_vortex_encoded_read_contract(
    readiness_report: VortexEncodedReadReadinessReport,
) -> Result<VortexEncodedReadExecutionReport> {
    VortexEncodedReadExecutionReport::from_input(VortexEncodedReadExecutionInput::new(
        readiness_report,
    ))
}

/// Executes the Phase 8 `Vortex` encoded-read spike contract path.
///
/// # Errors
/// Returns an error only if internal report construction fails.
pub fn execute_vortex_encoded_read_spike(
    readiness_report: crate::VortexEncodedReadReadinessReport,
    api_boundary_report: crate::VortexEncodedReadApiBoundaryReport,
    probe_report: crate::VortexEncodedReadProbeReport,
) -> Result<VortexEncodedReadExecutionReport> {
    let api_boundary = api_boundary_report.clone();
    let probe = probe_report.clone();
    std::mem::drop((api_boundary_report, probe_report));
    let input = VortexEncodedReadExecutionInput::new(readiness_report);
    if !vortex_encoded_read_spike_feature_enabled() {
        return Ok(VortexEncodedReadExecutionReport::feature_disabled(input));
    }
    if probe.status.is_error() {
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(input);
        report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        report.status = VortexEncodedReadExecutionStatus::BlockedByReadiness;
        report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        report.diagnostics.extend(probe.diagnostics.clone());
        return Ok(report);
    }
    if !probe.status.allows_future_probe() || probe.counts.eligible_candidate_count == 0 {
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(input);
        report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        report.status = VortexEncodedReadExecutionStatus::NoEncodedReadCandidates;
        report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        return Ok(report);
    }
    if !probe
        .input
        .encoded_readiness_report
        .status
        .allows_future_encoded_read()
    {
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(input);
        report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        report.status = VortexEncodedReadExecutionStatus::BlockedByReadiness;
        report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        return Ok(report);
    }
    let api_blocked = api_boundary.has_errors()
        || !matches!(
            api_boundary.status,
            VortexEncodedReadApiBoundaryStatus::ContractReady
                | VortexEncodedReadApiBoundaryStatus::ContractPartiallyReady
        )
        || api_boundary.data_read_api_count > 0
        || api_boundary.decode_api_count > 0
        || api_boundary.materialization_api_count > 0
        || api_boundary.arrow_default_risk_count > 0
        || api_boundary.object_store_api_count > 0
        || api_boundary.write_api_count > 0
        || api_boundary.fallback_execution_allowed;
    if api_blocked {
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(input);
        report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        report.status = VortexEncodedReadExecutionStatus::BlockedByReadiness;
        report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "vortex_encoded_read_spike",
            "Encoded-read public API path is not yet safe without decode/materialization.",
            Some("Fallback attempted: false".to_string()),
        ));
        return Ok(report);
    }
    let mut report = VortexEncodedReadExecutionReport::from_input(input)?;
    report.status = VortexEncodedReadExecutionStatus::BlockedByReadiness;
    report.add_diagnostic(Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        "vortex_encoded_read_spike",
        "Encoded-read public API path is not yet safe without decode/materialization.",
        Some("Fallback attempted: false".to_string()),
    ));
    Ok(report)
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn local_vortex_scan_path(
    target_uri: &DatasetUri,
    report: &mut VortexEncodedReadExecutionReport,
) -> Option<std::path::PathBuf> {
    if !target_uri.looks_like_vortex() {
        report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
        report.add_diagnostic(Diagnostic::invalid_input(
            "vortex_local_scan_count",
            format!(
                "target is not a Vortex-native path: {}",
                target_uri.as_str()
            ),
            "provide a local `.vortex` target for the feature-gated scan/count proof",
        ));
        return None;
    }
    let path = match target_uri.scheme() {
        UriScheme::LocalPath => std::path::PathBuf::from(target_uri.as_str()),
        UriScheme::File => std::path::PathBuf::from(
            target_uri
                .as_str()
                .strip_prefix("file://")
                .unwrap_or_else(|| target_uri.as_str()),
        ),
        UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls => {
            report.status = VortexEncodedReadExecutionStatus::BlockedByObjectStoreIo;
            report.add_diagnostic(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_scan_count",
                format!(
                    "object-store targets are outside the local scan/count scope: {}",
                    target_uri.as_str()
                ),
                Some(
                    "Use a local `.vortex` target until object-store IO is explicitly phased."
                        .to_string(),
                ),
            ));
            return None;
        }
        UriScheme::Other => {
            report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
            report.add_diagnostic(Diagnostic::invalid_input(
                "vortex_local_scan_count",
                format!(
                    "unsupported target scheme for local scan/count: {}",
                    target_uri.as_str()
                ),
                "provide a local path or file:// `.vortex` target",
            ));
            return None;
        }
    };
    if !path.exists() {
        report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
        report.add_diagnostic(Diagnostic::invalid_input(
            "vortex_local_scan_count",
            format!("local Vortex path does not exist: {}", path.display()),
            "provide an existing local `.vortex` target path",
        ));
        return None;
    }
    Some(path)
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn block_local_scan_for_approval(
    report: &mut VortexEncodedReadExecutionReport,
    reason: impl Into<String>,
) {
    report.status = VortexEncodedReadExecutionStatus::BlockedByReadiness;
    report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
    report.add_diagnostic(Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        "vortex_local_scan_count",
        reason,
        Some("Fallback attempted: false".to_string()),
    ));
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn read_planning_source_uri(report: &crate::VortexReadPlanningReport) -> Option<&DatasetUri> {
    report
        .input
        .universal_input_plan
        .as_ref()
        .and_then(|plan| plan.source.uri.as_ref())
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn runtime_bridge_source_uri(report: &crate::VortexRuntimeBridgeReport) -> Option<&DatasetUri> {
    read_planning_source_uri(&report.input.read_planning_report)
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn adaptive_sizing_source_uri(report: &crate::VortexAdaptiveSizingReport) -> Option<&DatasetUri> {
    report
        .input
        .runtime_bridge_report
        .as_ref()
        .and_then(runtime_bridge_source_uri)
        .or_else(|| {
            report
                .input
                .read_planning_report
                .as_ref()
                .and_then(read_planning_source_uri)
        })
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn memory_bridge_source_uri(report: &crate::VortexMemoryBridgeReport) -> Option<&DatasetUri> {
    report
        .input
        .adaptive_sizing_report
        .as_ref()
        .and_then(adaptive_sizing_source_uri)
        .or_else(|| {
            report
                .input
                .runtime_bridge_report
                .as_ref()
                .and_then(runtime_bridge_source_uri)
        })
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn encoded_read_readiness_source_uri(
    report: &VortexEncodedReadReadinessReport,
) -> Option<&DatasetUri> {
    memory_bridge_source_uri(&report.input.scheduler_report.input.memory_bridge_report)
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn annotate_local_scan_source_evidence(
    report: &mut VortexEncodedReadExecutionReport,
    target_uri: &DatasetUri,
    readiness_report: &VortexEncodedReadReadinessReport,
) -> Option<DatasetUri> {
    let readiness_source_uri = encoded_read_readiness_source_uri(readiness_report).cloned();
    report.local_scan_target_uri = Some(target_uri.clone());
    report
        .local_scan_readiness_source_uri
        .clone_from(&readiness_source_uri);
    report.local_scan_source_uri_matches_target = readiness_source_uri.as_ref() == Some(target_uri);
    readiness_source_uri
}

/// Executes a feature-gated local `CountAll` by scanning Vortex arrays
/// and summing their lengths.
///
/// This private helper is intentionally narrower than the general encoded-read
/// API boundary. The public entry point below adds the encoded-count approval
/// gate before this scan path is reachable.
///
/// # Errors
/// Returns an error only if deterministic report construction fails.
#[cfg(feature = "vortex-encoded-read-spike")]
fn execute_vortex_count_all_from_local_scan_readiness_with_session<B>(
    readiness_report: &VortexEncodedReadReadinessReport,
    target_uri: &DatasetUri,
    runtime: &B,
    session: &vortex::session::VortexSession,
) -> Result<VortexEncodedReadExecutionReport>
where
    B: vortex::io::runtime::BlockingRuntime,
{
    use vortex::file::OpenOptionsSessionExt as _;

    let input = VortexEncodedReadExecutionInput::new(readiness_report.clone())
        .allow_encoded_read_execution(true);
    let mut report = VortexEncodedReadExecutionReport::from_input(input)?;
    annotate_local_scan_source_evidence(&mut report, target_uri, readiness_report);
    if !vortex_encoded_read_spike_feature_enabled() {
        return Ok(VortexEncodedReadExecutionReport::feature_disabled(
            report.input,
        ));
    }
    if !report
        .input
        .readiness_report
        .status
        .allows_future_encoded_read()
        || report.would_execute_encoded_read_count == 0
        || report.blocked_count > 0
        || report.input.has_errors()
    {
        block_local_scan_for_approval(
            &mut report,
            "local scan/count requires a readiness report approved for future encoded read",
        );
        return Ok(report);
    }
    let Some(path) = local_vortex_scan_path(target_uri, &mut report) else {
        report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        return Ok(report);
    };
    let file = match runtime.block_on(session.open_options().open_path(&path)) {
        Ok(file) => file,
        Err(error) => {
            report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
            report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
            report.add_diagnostic(Diagnostic::invalid_input(
                "vortex_local_scan_count",
                format!("failed to open local Vortex target for scan/count: {error}"),
                "provide an existing local `.vortex` target compatible with the pinned Vortex version",
            ));
            return Ok(report);
        }
    };
    let scan = match file.scan() {
        Ok(scan) => scan,
        Err(error) => {
            report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
            report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
            report.add_diagnostic(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_scan_count",
                format!("Vortex scan setup failed for local count: {error}"),
                Some("Fallback attempted: false".to_string()),
            ));
            return Ok(report);
        }
    };
    let iter = match scan.into_array_iter(runtime) {
        Ok(iter) => iter,
        Err(error) => {
            report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
            report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
            report.add_diagnostic(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_scan_count",
                format!("Vortex array iterator setup failed for local count: {error}"),
                Some("Fallback attempted: false".to_string()),
            ));
            return Ok(report);
        }
    };
    let mut arrays_read_count = 0usize;
    let mut rows_counted = 0u64;
    for array_result in iter {
        let array = match array_result {
            Ok(array) => array,
            Err(error) => {
                report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
                report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
                report.add_diagnostic(Diagnostic::unsupported(
                    DiagnosticCode::NotImplemented,
                    "vortex_local_scan_count",
                    format!("Vortex local scan failed while reading arrays: {error}"),
                    Some("Fallback attempted: false".to_string()),
                ));
                return Ok(report);
            }
        };
        let Ok(len) = u64::try_from(array.len()) else {
            report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
            report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
            report.add_diagnostic(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_scan_count",
                "Vortex array length does not fit in u64 for count result",
                Some("Fallback attempted: false".to_string()),
            ));
            return Ok(report);
        };
        arrays_read_count += 1;
        let Some(total) = rows_counted.checked_add(len) else {
            report.status = VortexEncodedReadExecutionStatus::BlockedByUnsupportedInput;
            report.mode = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
            report.add_diagnostic(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_scan_count",
                "Vortex local count overflowed u64",
                Some("Fallback attempted: false".to_string()),
            ));
            return Ok(report);
        };
        rows_counted = total;
    }
    report.status = VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted;
    report.mode = VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount;
    report.data_read = true;
    report.upstream_scan_called = true;
    report.arrays_read_count = arrays_read_count;
    report.rows_counted = rows_counted;
    report.count_result = Some(rows_counted);
    report.data_decoded = false;
    report.data_materialized = false;
    report.row_read = false;
    report.arrow_converted = false;
    report.object_store_io = false;
    report.write_io = false;
    report.spill_io_performed = false;
    report.external_effects_executed = false;
    report.fallback_execution_allowed = false;
    Ok(report)
}

/// Executes an approval-gated local `CountAll` by scanning Vortex arrays
/// and summing their lengths.
///
/// This path requires the existing encoded-count data-path approval report plus
/// encoded-read readiness for the same source URI. It stays local-path-only
/// and does not read rows, request decode/materialization, convert to `Arrow`,
/// touch object stores, write data, spill, invoke external baselines, or allow
/// fallback execution.
///
/// # Errors
/// Returns an error only if deterministic report construction fails.
#[cfg(feature = "vortex-encoded-read-spike")]
pub fn execute_vortex_count_all_from_local_scan_with_session<B>(
    approval_report: &VortexEncodedCountDataPathApprovalReport,
    readiness_report: &VortexEncodedReadReadinessReport,
    runtime: &B,
    session: &vortex::session::VortexSession,
) -> Result<VortexEncodedReadExecutionReport>
where
    B: vortex::io::runtime::BlockingRuntime,
{
    let target_uri = &approval_report
        .input
        .count_readiness_report
        .request
        .target_uri;
    let mut report = VortexEncodedReadExecutionReport::from_input(
        VortexEncodedReadExecutionInput::new(readiness_report.clone())
            .allow_encoded_read_execution(true),
    )?;
    let readiness_target_uri =
        annotate_local_scan_source_evidence(&mut report, target_uri, readiness_report);
    if !approval_report.approved()
        || approval_report.has_errors()
        || !approval_report.is_side_effect_free()
        || approval_report.fallback_execution_allowed
    {
        block_local_scan_for_approval(
            &mut report,
            "local scan/count requires an approved encoded-count data-path approval report",
        );
        return Ok(report);
    }
    let Some(readiness_target_uri) = readiness_target_uri else {
        block_local_scan_for_approval(
            &mut report,
            "local scan/count requires encoded-read readiness source URI evidence",
        );
        return Ok(report);
    };
    if &readiness_target_uri != target_uri {
        block_local_scan_for_approval(
            &mut report,
            format!(
                "local scan/count approval target URI '{}' does not match encoded-read readiness source URI '{}'",
                target_uri.as_str(),
                readiness_target_uri.as_str()
            ),
        );
        return Ok(report);
    }
    execute_vortex_count_all_from_local_scan_readiness_with_session(
        readiness_report,
        target_uri,
        runtime,
        session,
    )
}

/// Executes approved local `.vortex` `CountAll` by scanning Vortex arrays and
/// summing their lengths.
///
/// This convenience entry point owns the local Vortex runtime/session setup so
/// callers can use the narrow local-count execution boundary without depending
/// on upstream Vortex runtime types. It remains feature-gated, local-path only,
/// approval-gated, and source-match-gated.
///
/// # Errors
/// Returns an error only if deterministic report construction fails.
pub fn execute_vortex_count_all_from_approved_local_scan(
    approval_report: &VortexEncodedCountDataPathApprovalReport,
    readiness_report: &VortexEncodedReadReadinessReport,
) -> Result<VortexEncodedReadExecutionReport> {
    #[cfg(feature = "vortex-encoded-read-spike")]
    {
        use vortex::VortexSessionDefault as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        execute_vortex_count_all_from_local_scan_with_session(
            approval_report,
            readiness_report,
            &runtime,
            &session,
        )
    }
    #[cfg(not(feature = "vortex-encoded-read-spike"))]
    {
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(
            VortexEncodedReadExecutionInput::new(readiness_report.clone())
                .allow_encoded_read_execution(true),
        );
        report.local_scan_target_uri = Some(
            approval_report
                .input
                .count_readiness_report
                .request
                .target_uri
                .clone(),
        );
        Ok(report)
    }
}

pub fn vortex_encoded_read_execution_is_side_effect_free(
    report: &VortexEncodedReadExecutionReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "vortex-encoded-read-spike")]
    fn ready_readiness_for_uri(
        target_uri: shardloom_core::DatasetUri,
    ) -> VortexEncodedReadReadinessReport {
        use crate::{
            VortexSchedulerBridgeInput, VortexTaskSchedulingDecision,
            build_vortex_runtime_task_graph, plan_native_vortex_universal_input,
            plan_vortex_memory_safety, plan_vortex_read_from_universal_input,
            size_vortex_runtime_task_graph,
        };
        use shardloom_core::UniversalInputSource;
        use shardloom_exec::{AdaptiveSizingPolicy, ByteSize, MemoryBudget};

        let source = UniversalInputSource::from_dataset_uri(target_uri).expect("source");
        let input_plan = plan_native_vortex_universal_input(source).expect("input plan");
        let read_report = plan_vortex_read_from_universal_input(input_plan).expect("read plan");
        let runtime_report = build_vortex_runtime_task_graph(read_report).expect("runtime bridge");
        let sizing_report = size_vortex_runtime_task_graph(
            runtime_report,
            AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(1)),
        )
        .expect("sizing");
        let memory = plan_vortex_memory_safety(
            sizing_report,
            MemoryBudget::from_gib(1).expect("memory budget"),
        )
        .expect("memory bridge");
        let mut scheduler =
            crate::VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(memory))
                .expect("scheduler bridge");
        scheduler.decisions.clear();
        scheduler
            .decisions
            .push(VortexTaskSchedulingDecision::schedule_now(
                None,
                "local array-length count scan",
            ));
        scheduler.recompute_counts();
        VortexEncodedReadReadinessReport::from_scheduler_report(scheduler).expect("readiness")
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    fn approved_encoded_count_path_for_uri(
        target_uri: shardloom_core::DatasetUri,
    ) -> VortexEncodedCountDataPathApprovalReport {
        use crate::{
            VortexCountCandidateSource, VortexCountReadinessRequest,
            VortexEncodedCountDataPathApprovalInput, VortexEncodedReadApiBoundaryReport,
            VortexEncodedReadApiBoundaryStatus, plan_vortex_count_readiness,
        };

        let readiness = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(
                target_uri,
                VortexCountCandidateSource::EncodedDataPath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .count_primitive(true)
            .encoded_data_path_ready(true),
        )
        .expect("count readiness");
        let mut api = VortexEncodedReadApiBoundaryReport::default_deferred();
        api.status = VortexEncodedReadApiBoundaryStatus::ContractReady;
        api.execution_usable_count = 1;
        VortexEncodedCountDataPathApprovalReport::from_input(
            VortexEncodedCountDataPathApprovalInput::new(readiness, api),
        )
        .expect("approval")
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    fn blocked_encoded_count_path_for_uri(
        target_uri: shardloom_core::DatasetUri,
    ) -> VortexEncodedCountDataPathApprovalReport {
        use crate::{
            VortexCountCandidateSource, VortexCountReadinessRequest, plan_vortex_count_readiness,
            plan_vortex_encoded_count_data_path_approval, vortex_encoded_read_public_api_boundary,
        };

        let readiness = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(
                target_uri,
                VortexCountCandidateSource::EncodedDataPath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .count_primitive(true)
            .encoded_data_path_ready(true),
        )
        .expect("count readiness");
        plan_vortex_encoded_count_data_path_approval(
            readiness,
            vortex_encoded_read_public_api_boundary(),
        )
        .expect("approval")
    }

    #[test]
    fn mode_contract_only_no_data() {
        let m = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data() && !m.writes_data());
    }

    #[test]
    fn local_scan_count_mode_reads_data_only() {
        let m = VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount;
        assert!(m.reads_data());
        assert!(!m.decodes_data());
        assert!(!m.materializes_data());
        assert!(!m.writes_data());
    }

    #[test]
    fn decision_kind_checks() {
        assert!(VortexEncodedReadExecutionDecisionKind::WouldExecuteEncodedRead.is_candidate());
        assert!(VortexEncodedReadExecutionDecisionKind::BlockedDecodeRisk.is_blocked());
        assert!(VortexEncodedReadExecutionDecisionKind::BlockedMaterializationRisk.is_blocked());
    }

    #[test]
    fn blocked_unsupported_has_error() {
        let d = VortexEncodedReadExecutionDecision::blocked_unsupported(None, "f", "r");
        assert!(d.has_errors());
    }

    #[test]
    fn feature_flag_value_matches_cfg() {
        assert_eq!(
            vortex_encoded_read_executor_feature_enabled(),
            cfg!(feature = "vortex-encoded-read-executor")
        );
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    #[test]
    fn local_scan_counts_vortex_array_lengths() {
        use shardloom_core::DatasetUri;
        use vortex::VortexSessionDefault as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let target_uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");
        let approval = approved_encoded_count_path_for_uri(target_uri.clone());
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());

        let report = execute_vortex_count_all_from_local_scan_with_session(
            &approval,
            &ready_readiness_for_uri(target_uri),
            &runtime,
            &session,
        )
        .expect("local scan/count");

        assert_eq!(
            report.status,
            VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        );
        assert_eq!(
            report.mode,
            VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount
        );
        assert_eq!(report.count_result, Some(20_000));
        assert_eq!(report.rows_counted, 20_000);
        assert!(report.arrays_read_count > 0);
        assert!(report.data_read);
        assert!(report.upstream_scan_called);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert_eq!(
            report.local_scan_target_uri.as_ref(),
            Some(&approval.input.count_readiness_report.request.target_uri)
        );
        assert_eq!(
            report.local_scan_readiness_source_uri.as_ref(),
            report.local_scan_target_uri.as_ref()
        );
        assert!(report.local_scan_source_uri_matches_target);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.has_errors());
        assert!(!report.is_side_effect_free());
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    #[test]
    fn approved_local_scan_helper_owns_runtime_and_counts() {
        use shardloom_core::DatasetUri;

        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let target_uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");
        let approval = approved_encoded_count_path_for_uri(target_uri.clone());

        let report = execute_vortex_count_all_from_approved_local_scan(
            &approval,
            &ready_readiness_for_uri(target_uri),
        )
        .expect("approved local scan/count");

        assert_eq!(
            report.status,
            VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        );
        assert_eq!(report.count_result, Some(20_000));
        assert!(report.data_read);
        assert!(report.upstream_scan_called);
        assert!(report.local_scan_source_uri_matches_target);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.has_errors());
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    #[test]
    fn local_encoded_count_matches_correctness_manifest_reference_output() {
        use crate::{
            VortexEncodedCountPhysicalKernelStatus, VortexLocalExecutionStatus,
            VortexLocalExecutionValue, VortexQueryPrimitiveValue,
            evaluate_vortex_local_encoded_count_physical_kernel,
            execute_vortex_count_all_from_approved_local_scan_result,
            local_encoded_count_execution_certificate,
        };
        use shardloom_core::{
            CorrectnessValidationPlan, DatasetUri, ExecutionCertificateStatus, ExpectedOutcome,
        };

        let plan = CorrectnessValidationPlan::default_foundation_plan();
        let fixture = plan
            .fixtures
            .iter()
            .find(|fixture| fixture.id.as_str() == "vortex-local-encoded-count-u64-20000")
            .expect("encoded count fixture");
        let ExpectedOutcome::EncodedCount { count } = fixture.expected else {
            panic!("encoded count fixture must declare encoded count reference output");
        };
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join(fixture.source_ref.as_ref().expect("fixture source ref"));
        let target_uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");
        let approval = approved_encoded_count_path_for_uri(target_uri.clone());
        let encoded_report = execute_vortex_count_all_from_approved_local_scan(
            &approval,
            &ready_readiness_for_uri(target_uri),
        )
        .expect("approved local scan/count");
        let local_report =
            execute_vortex_count_all_from_approved_local_scan_result(&approval, &encoded_report)
                .expect("local encoded count bridge");

        assert_eq!(encoded_report.count_result, Some(count));
        assert_eq!(encoded_report.rows_counted, count);
        assert!(encoded_report.local_scan_source_uri_matches_target);
        assert!(encoded_report.data_read);
        assert!(!encoded_report.data_decoded);
        assert!(!encoded_report.data_materialized);
        assert!(!encoded_report.row_read);
        assert!(!encoded_report.arrow_converted);
        assert!(!encoded_report.object_store_io);
        assert!(!encoded_report.write_io);
        assert!(!encoded_report.spill_io_performed);
        assert!(!encoded_report.fallback_execution_allowed);
        assert_eq!(
            local_report.status,
            VortexLocalExecutionStatus::LocalEncodedCountExecuted
        );
        assert_eq!(
            local_report.value,
            VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(count))
        );
        assert!(local_report.tasks_executed);
        assert!(local_report.data_read);
        assert!(!local_report.data_decoded);
        assert!(!local_report.data_materialized);
        assert!(!local_report.object_store_io);
        assert!(!local_report.write_io);
        assert!(!local_report.spill_io_performed);
        assert!(!local_report.external_effects_executed);
        assert!(!local_report.fallback_execution_allowed);
        assert!(!local_report.has_errors());

        let certificate =
            local_encoded_count_execution_certificate(fixture, &encoded_report, &local_report)
                .expect("execution certificate");
        assert_eq!(certificate.status, ExecutionCertificateStatus::Certified);
        assert!(certificate.is_certified());
        assert_eq!(
            certificate.correctness_fixture_id.as_deref(),
            Some("vortex-local-encoded-count-u64-20000")
        );
        assert_eq!(certificate.expected_outcome, certificate.actual_outcome);
        assert!(certificate.data_read);
        assert!(!certificate.data_decoded);
        assert!(!certificate.data_materialized);
        assert!(!certificate.row_read);
        assert!(!certificate.arrow_converted);
        assert!(!certificate.object_store_io);
        assert!(!certificate.write_io);
        assert!(!certificate.spill_io_performed);
        assert!(!certificate.external_effects_executed);
        assert!(certificate.fallback_free());

        let physical_kernel = evaluate_vortex_local_encoded_count_physical_kernel(
            &encoded_report,
            &local_report,
            &certificate,
        );
        assert_eq!(
            physical_kernel.status,
            VortexEncodedCountPhysicalKernelStatus::EvaluatedEncodedNative
        );
        assert_eq!(physical_kernel.count_result, Some(count));
        assert_eq!(physical_kernel.rows_counted, count);
        assert!(physical_kernel.data_read);
        assert!(physical_kernel.upstream_scan_called);
        assert!(!physical_kernel.data_decoded);
        assert!(!physical_kernel.data_materialized);
        assert!(!physical_kernel.row_read);
        assert!(!physical_kernel.arrow_converted);
        assert!(!physical_kernel.object_store_io);
        assert!(!physical_kernel.write_io);
        assert!(!physical_kernel.spill_io_performed);
        assert!(!physical_kernel.fallback_attempted);
        assert!(!physical_kernel.fallback_execution_allowed);
        assert!(!physical_kernel.production_claim_allowed);
        assert!(physical_kernel.is_safe_native_kernel_evidence());
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    #[test]
    fn local_scan_rejects_object_store_target_without_io() {
        use shardloom_core::DatasetUri;
        use vortex::VortexSessionDefault as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let target_uri = DatasetUri::new("s3://bucket/data.vortex").expect("uri");
        let approval = approved_encoded_count_path_for_uri(target_uri.clone());
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());

        let report = execute_vortex_count_all_from_local_scan_with_session(
            &approval,
            &ready_readiness_for_uri(target_uri),
            &runtime,
            &session,
        )
        .expect("object store block report");

        assert_eq!(
            report.status,
            VortexEncodedReadExecutionStatus::BlockedByObjectStoreIo
        );
        assert!(!report.data_read);
        assert!(!report.upstream_scan_called);
        assert_eq!(
            report.local_scan_target_uri.as_ref(),
            Some(&approval.input.count_readiness_report.request.target_uri)
        );
        assert_eq!(
            report.local_scan_readiness_source_uri.as_ref(),
            report.local_scan_target_uri.as_ref()
        );
        assert!(report.local_scan_source_uri_matches_target);
        assert!(!report.object_store_io);
        assert!(!report.fallback_execution_allowed);
        assert!(report.has_errors());
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    #[test]
    fn local_scan_requires_encoded_count_approval() {
        use shardloom_core::DatasetUri;
        use vortex::VortexSessionDefault as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let target_uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");
        let approval = blocked_encoded_count_path_for_uri(target_uri.clone());
        assert!(!approval.approved());
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());

        let report = execute_vortex_count_all_from_local_scan_with_session(
            &approval,
            &ready_readiness_for_uri(target_uri),
            &runtime,
            &session,
        )
        .expect("approval block report");

        assert_eq!(
            report.status,
            VortexEncodedReadExecutionStatus::BlockedByReadiness
        );
        assert!(!report.data_read);
        assert!(!report.upstream_scan_called);
        assert_eq!(
            report.local_scan_target_uri.as_ref(),
            Some(&approval.input.count_readiness_report.request.target_uri)
        );
        assert_eq!(
            report.local_scan_readiness_source_uri.as_ref(),
            report.local_scan_target_uri.as_ref()
        );
        assert!(report.local_scan_source_uri_matches_target);
        assert!(!report.fallback_execution_allowed);
        assert!(report.has_errors());
    }

    #[cfg(feature = "vortex-encoded-read-spike")]
    #[test]
    fn local_scan_requires_matching_approval_and_readiness_target() {
        use shardloom_core::DatasetUri;
        use vortex::VortexSessionDefault as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let readiness_uri =
            DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("readiness uri");
        let approval_uri =
            DatasetUri::new(format!("file://{}", fixture_path.display())).expect("approval uri");
        let approval = approved_encoded_count_path_for_uri(approval_uri);
        let approval_uri = approval
            .input
            .count_readiness_report
            .request
            .target_uri
            .clone();
        assert!(approval.approved());
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());

        let report = execute_vortex_count_all_from_local_scan_with_session(
            &approval,
            &ready_readiness_for_uri(readiness_uri.clone()),
            &runtime,
            &session,
        )
        .expect("target mismatch report");

        assert_eq!(
            report.status,
            VortexEncodedReadExecutionStatus::BlockedByReadiness
        );
        assert!(!report.data_read);
        assert!(!report.upstream_scan_called);
        assert_eq!(report.local_scan_target_uri.as_ref(), Some(&approval_uri));
        assert_eq!(
            report.local_scan_readiness_source_uri.as_ref(),
            Some(&readiness_uri)
        );
        assert!(!report.local_scan_source_uri_matches_target);
        assert!(!report.fallback_execution_allowed);
        assert!(report.has_errors());
        assert!(
            report
                .diagnostics
                .iter()
                .any(|d| d.message.contains("does not match encoded-read readiness"))
        );
    }
}
