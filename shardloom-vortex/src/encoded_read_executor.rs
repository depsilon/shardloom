#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools,
    clippy::too_many_lines
)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId};
use shardloom_exec::TaskId;

use crate::{VortexEncodedReadCandidateKind, VortexEncodedReadReadinessReport};

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
        matches!(self, Self::WouldExecuteEncodedRead)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadExecutionMode {
    ReportOnly,
    EncodedReadContractOnly,
    Unsupported,
}
impl VortexEncodedReadExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::EncodedReadContractOnly => "encoded_read_contract_only",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn reads_data(&self) -> bool {
        false
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
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
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
            data_read: false,
            data_decoded: false,
            data_materialized: false,
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
            data_read: false,
            data_decoded: false,
            data_materialized: false,
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
            "data read: false\ndata decoded: false\ndata materialized: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nexternal effects executed: false\nfallback execution disabled"
        );
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
/// # Errors
/// Returns an error if encoded-read contract report construction fails.
pub fn execute_vortex_encoded_read_contract(
    readiness_report: VortexEncodedReadReadinessReport,
) -> Result<VortexEncodedReadExecutionReport> {
    VortexEncodedReadExecutionReport::from_input(VortexEncodedReadExecutionInput::new(
        readiness_report,
    ))
}
pub fn vortex_encoded_read_execution_is_side_effect_free(
    report: &VortexEncodedReadExecutionReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_contract_only_no_data() {
        let m = VortexEncodedReadExecutionMode::EncodedReadContractOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data() && !m.writes_data());
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
}
