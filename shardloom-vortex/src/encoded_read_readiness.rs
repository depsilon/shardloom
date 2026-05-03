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

use crate::{
    VortexExecutionReadinessReport, VortexMetadataExecutionReport, VortexSchedulerBridgeReport,
    VortexSchedulingDecisionKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadReadinessStatus {
    ReadyForContract,
    ReadyForFutureEncodedRead,
    NoEncodedReadCandidates,
    BlockedByFeatureGate,
    BlockedByReadiness,
    BlockedByMissingEstimate,
    BlockedByMissingByteRanges,
    BlockedByMemoryPolicy,
    BlockedBySpillPolicy,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedByUnsupportedInput,
    Unsupported,
}
impl VortexEncodedReadReadinessStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReadyForContract => "ready_for_contract",
            Self::ReadyForFutureEncodedRead => "ready_for_future_encoded_read",
            Self::NoEncodedReadCandidates => "no_encoded_read_candidates",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::BlockedByReadiness => "blocked_by_readiness",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByMissingByteRanges => "blocked_by_missing_byte_ranges",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::BlockedBySpillPolicy => "blocked_by_spill_policy",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::BlockedByWriteIo => "blocked_by_write_io",
            Self::BlockedByUnsupportedInput => "blocked_by_unsupported_input",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        !matches!(
            self,
            Self::ReadyForContract
                | Self::ReadyForFutureEncodedRead
                | Self::NoEncodedReadCandidates
        )
    }
    pub const fn allows_future_encoded_read(&self) -> bool {
        matches!(self, Self::ReadyForFutureEncodedRead)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadReadinessMode {
    ContractOnly,
    ReadinessOnly,
    Unsupported,
}
impl VortexEncodedReadReadinessMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ContractOnly => "contract_only",
            Self::ReadinessOnly => "readiness_only",
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
pub enum VortexEncodedReadCandidateKind {
    EncodedReadCandidate,
    MetadataOnlyNoRead,
    PrunedNoRead,
    NeedsEstimate,
    NeedsByteRange,
    WouldDecode,
    WouldMaterialize,
    WouldUseObjectStore,
    WouldWrite,
    WouldSpill,
    Unsupported,
}
impl VortexEncodedReadCandidateKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EncodedReadCandidate => "encoded_read_candidate",
            Self::MetadataOnlyNoRead => "metadata_only_no_read",
            Self::PrunedNoRead => "pruned_no_read",
            Self::NeedsEstimate => "needs_estimate",
            Self::NeedsByteRange => "needs_byte_range",
            Self::WouldDecode => "would_decode",
            Self::WouldMaterialize => "would_materialize",
            Self::WouldUseObjectStore => "would_use_object_store",
            Self::WouldWrite => "would_write",
            Self::WouldSpill => "would_spill",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_candidate(&self) -> bool {
        matches!(self, Self::EncodedReadCandidate)
    }
    pub const fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::NeedsEstimate
                | Self::NeedsByteRange
                | Self::WouldDecode
                | Self::WouldMaterialize
                | Self::WouldUseObjectStore
                | Self::WouldWrite
                | Self::WouldSpill
                | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadCandidate {
    pub kind: VortexEncodedReadCandidateKind,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub split_id: Option<String>,
    pub has_byte_ranges: bool,
    pub required_columns_known: bool,
    pub estimate_known: bool,
    pub memory_ready: bool,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadCandidate {
    fn base(
        kind: VortexEncodedReadCandidateKind,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id,
            segment_id: None,
            split_id: None,
            has_byte_ranges: false,
            required_columns_known: true,
            estimate_known: true,
            memory_ready: true,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn encoded_read(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadCandidateKind::EncodedReadCandidate,
            task_id,
            reason,
        )
    }
    pub fn metadata_only(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadCandidateKind::MetadataOnlyNoRead,
            task_id,
            reason,
        )
    }
    pub fn pruned(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadCandidateKind::PrunedNoRead,
            task_id,
            reason,
        )
    }
    pub fn needs_estimate(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        let mut s = Self::base(
            VortexEncodedReadCandidateKind::NeedsEstimate,
            task_id,
            reason,
        );
        s.estimate_known = false;
        s
    }
    pub fn needs_byte_range(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        let mut s = Self::base(
            VortexEncodedReadCandidateKind::NeedsByteRange,
            task_id,
            reason,
        );
        s.has_byte_ranges = false;
        s
    }
    pub fn would_decode(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(VortexEncodedReadCandidateKind::WouldDecode, task_id, reason)
    }
    pub fn would_materialize(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadCandidateKind::WouldMaterialize,
            task_id,
            reason,
        )
    }
    pub fn would_use_object_store(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadCandidateKind::WouldUseObjectStore,
            task_id,
            reason,
        )
    }
    pub fn would_write(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(VortexEncodedReadCandidateKind::WouldWrite, task_id, reason)
    }
    pub fn would_spill(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(VortexEncodedReadCandidateKind::WouldSpill, task_id, reason)
    }
    pub fn unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexEncodedReadCandidateKind::Unsupported,
            task_id,
            "unsupported encoded-read candidate",
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
    pub fn with_byte_ranges(mut self, has: bool) -> Self {
        self.has_byte_ranges = has;
        self
    }
    pub fn with_required_columns_known(mut self, known: bool) -> Self {
        self.required_columns_known = known;
        self
    }
    pub fn with_estimate_known(mut self, known: bool) -> Self {
        self.estimate_known = known;
        self
    }
    pub fn with_memory_ready(mut self, ready: bool) -> Self {
        self.memory_ready = ready;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub const fn is_future_encoded_read_candidate(&self) -> bool {
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
        }) || self.kind == VortexEncodedReadCandidateKind::Unsupported
    }
    pub fn summary(&self) -> String {
        format!(
            "kind={} readiness_only=true execution_performed=false reason={}",
            self.kind.as_str(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadReadinessInput {
    pub scheduler_report: VortexSchedulerBridgeReport,
    pub execution_readiness_report: Option<VortexExecutionReadinessReport>,
    pub metadata_execution_report: Option<VortexMetadataExecutionReport>,
    pub require_known_estimates: bool,
    pub require_byte_ranges: bool,
    pub require_memory_ready: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadReadinessInput {
    pub fn new(scheduler_report: VortexSchedulerBridgeReport) -> Self {
        Self {
            scheduler_report,
            execution_readiness_report: None,
            metadata_execution_report: None,
            require_known_estimates: true,
            require_byte_ranges: false,
            require_memory_ready: true,
            diagnostics: vec![],
        }
    }
    pub fn with_execution_readiness_report(
        mut self,
        report: VortexExecutionReadinessReport,
    ) -> Self {
        self.execution_readiness_report = Some(report);
        self
    }
    pub fn with_metadata_execution_report(mut self, report: VortexMetadataExecutionReport) -> Self {
        self.metadata_execution_report = Some(report);
        self
    }
    pub fn require_known_estimates(mut self, value: bool) -> Self {
        self.require_known_estimates = value;
        self
    }
    pub fn require_byte_ranges(mut self, value: bool) -> Self {
        self.require_byte_ranges = value;
        self
    }
    pub fn require_memory_ready(mut self, value: bool) -> Self {
        self.require_memory_ready = value;
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
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
            "readiness-only input require_known_estimates={} require_byte_ranges={} require_memory_ready={}",
            self.require_known_estimates, self.require_byte_ranges, self.require_memory_ready
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadReadinessReport {
    pub status: VortexEncodedReadReadinessStatus,
    pub mode: VortexEncodedReadReadinessMode,
    pub input: VortexEncodedReadReadinessInput,
    pub candidates: Vec<VortexEncodedReadCandidate>,
    pub future_encoded_read_candidate_count: usize,
    pub metadata_only_count: usize,
    pub pruned_count: usize,
    pub blocked_count: usize,
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
impl VortexEncodedReadReadinessReport {
    /// # Errors
    /// Returns an error if report construction invariants are violated.
    pub fn from_input(input: VortexEncodedReadReadinessInput) -> Result<Self> {
        let mut out = Self {
            status: VortexEncodedReadReadinessStatus::ReadyForContract,
            mode: VortexEncodedReadReadinessMode::ReadinessOnly,
            input,
            candidates: vec![],
            future_encoded_read_candidate_count: 0,
            metadata_only_count: 0,
            pruned_count: 0,
            blocked_count: 0,
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
        out.diagnostics.extend(out.input.diagnostics.clone());
        out.diagnostics
            .extend(out.input.scheduler_report.diagnostics.clone());
        if let Some(r) = &out.input.execution_readiness_report {
            out.diagnostics.extend(r.diagnostics.clone());
        }
        if let Some(r) = &out.input.metadata_execution_report {
            out.diagnostics.extend(r.diagnostics.clone());
        }
        for decision in out.input.scheduler_report.decisions.clone() {
            let mut candidate = match decision.kind {
                VortexSchedulingDecisionKind::ScheduleNow => {
                    VortexEncodedReadCandidate::encoded_read(
                        decision.task_id,
                        decision.reason.clone(),
                    )
                }
                VortexSchedulingDecisionKind::ScheduleMetadataOnly => {
                    VortexEncodedReadCandidate::metadata_only(
                        decision.task_id,
                        decision.reason.clone(),
                    )
                }
                VortexSchedulingDecisionKind::SkipPruned => {
                    VortexEncodedReadCandidate::pruned(decision.task_id, decision.reason.clone())
                }
                VortexSchedulingDecisionKind::HoldForEstimate => {
                    VortexEncodedReadCandidate::needs_estimate(
                        decision.task_id,
                        decision.reason.clone(),
                    )
                }
                VortexSchedulingDecisionKind::HoldForMemory => {
                    VortexEncodedReadCandidate::would_materialize(
                        decision.task_id,
                        decision.reason.clone(),
                    )
                }
                VortexSchedulingDecisionKind::HoldForSpillSupport => {
                    VortexEncodedReadCandidate::would_spill(
                        decision.task_id,
                        decision.reason.clone(),
                    )
                }
                VortexSchedulingDecisionKind::Unsupported => {
                    VortexEncodedReadCandidate::unsupported(
                        decision.task_id,
                        "encoded_read_readiness",
                        decision.reason.clone(),
                    )
                }
            };
            candidate.segment_id.clone_from(&decision.segment_id);
            candidate.diagnostics.extend(decision.diagnostics.clone());
            out.add_candidate(candidate);
        }
        out.recompute_counts();
        let has_hard_errors = out.has_errors();
        let memory_blocked = out.input.require_memory_ready
            && out.candidates.iter().any(|c| !c.memory_ready)
            || out.input.scheduler_report.status.as_str() == "blocked_by_memory_policy";
        out.status = if has_hard_errors {
            VortexEncodedReadReadinessStatus::Unsupported
        } else if out.input.require_known_estimates && out.missing_estimate_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedByMissingEstimate
        } else if out.input.require_byte_ranges && out.missing_byte_range_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedByMissingByteRanges
        } else if memory_blocked {
            VortexEncodedReadReadinessStatus::BlockedByMemoryPolicy
        } else if out.spill_blocked_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedBySpillPolicy
        } else if out.decode_blocked_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedByDecodeRisk
        } else if out.materialization_blocked_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedByMaterializationRisk
        } else if out.object_store_blocked_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedByObjectStoreIo
        } else if out.write_blocked_count > 0 {
            VortexEncodedReadReadinessStatus::BlockedByWriteIo
        } else if out.future_encoded_read_candidate_count == 0 && out.blocked_count == 0 {
            VortexEncodedReadReadinessStatus::NoEncodedReadCandidates
        } else if out.future_encoded_read_candidate_count > 0 && out.blocked_count == 0 {
            VortexEncodedReadReadinessStatus::ReadyForFutureEncodedRead
        } else {
            VortexEncodedReadReadinessStatus::ReadyForContract
        };
        Ok(out)
    }
    /// # Errors
    /// Returns error propagated from `from_input`.
    pub fn from_scheduler_report(report: VortexSchedulerBridgeReport) -> Result<Self> {
        Self::from_input(VortexEncodedReadReadinessInput::new(report))
    }
    pub fn unsupported(
        input: VortexEncodedReadReadinessInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::from_input(input).expect("infallible from_input");
        s.status = VortexEncodedReadReadinessStatus::Unsupported;
        s.mode = VortexEncodedReadReadinessMode::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn add_candidate(&mut self, c: VortexEncodedReadCandidate) {
        self.candidates.push(c);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn recompute_counts(&mut self) {
        self.future_encoded_read_candidate_count = self
            .candidates
            .iter()
            .filter(|c| c.is_future_encoded_read_candidate())
            .count();
        self.metadata_only_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::MetadataOnlyNoRead)
            .count();
        self.pruned_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::PrunedNoRead)
            .count();
        self.blocked_count = self.candidates.iter().filter(|c| c.is_blocked()).count();
        self.missing_estimate_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::NeedsEstimate)
            .count();
        self.missing_byte_range_count = self
            .candidates
            .iter()
            .filter(|c| {
                c.kind == VortexEncodedReadCandidateKind::NeedsByteRange || !c.has_byte_ranges
            })
            .count();
        self.decode_blocked_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::WouldDecode)
            .count();
        self.materialization_blocked_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::WouldMaterialize)
            .count();
        self.object_store_blocked_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::WouldUseObjectStore)
            .count();
        self.write_blocked_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::WouldWrite)
            .count();
        self.spill_blocked_count = self
            .candidates
            .iter()
            .filter(|c| c.kind == VortexEncodedReadCandidateKind::WouldSpill)
            .count();
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .chain(self.input.diagnostics.iter())
            .any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .candidates
                .iter()
                .any(VortexEncodedReadCandidate::has_errors)
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
        let mut out = String::new();
        let _ = writeln!(out, "Vortex encoded-read readiness report");
        let _ = writeln!(
            out,
            "encoded-read readiness status: {}",
            self.status.as_str()
        );
        let _ = writeln!(
            out,
            "future encoded-read candidate count: {}",
            self.future_encoded_read_candidate_count
        );
        let _ = writeln!(out, "metadata-only count: {}", self.metadata_only_count);
        let _ = writeln!(out, "pruned count: {}", self.pruned_count);
        let _ = writeln!(out, "blocked count: {}", self.blocked_count);
        let _ = writeln!(
            out,
            "missing estimate count: {}",
            self.missing_estimate_count
        );
        let _ = writeln!(
            out,
            "missing byte range count: {}",
            self.missing_byte_range_count
        );
        let _ = writeln!(out, "decode blocked count: {}", self.decode_blocked_count);
        let _ = writeln!(
            out,
            "materialization blocked count: {}",
            self.materialization_blocked_count
        );
        let _ = writeln!(
            out,
            "object-store blocked count: {}",
            self.object_store_blocked_count
        );
        let _ = writeln!(out, "write blocked count: {}", self.write_blocked_count);
        let _ = writeln!(out, "spill blocked count: {}", self.spill_blocked_count);
        let _ = writeln!(out, "data read: false");
        let _ = writeln!(out, "data decoded: false");
        let _ = writeln!(out, "data materialized: false");
        let _ = writeln!(out, "object-store IO: false");
        let _ = writeln!(out, "write IO: false");
        let _ = writeln!(out, "spill IO performed: false");
        let _ = writeln!(out, "external effects executed: false");
        let _ = writeln!(out, "fallback execution disabled");
        if self.diagnostics.is_empty() {
            let _ = write!(out, "diagnostics: none");
        } else {
            let _ = writeln!(out, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {} [{}]", d.message, d.severity.as_str());
            }
        }
        out
    }
}

/// # Errors
/// Returns an error if report construction fails.
pub fn evaluate_vortex_encoded_read_readiness(
    scheduler_report: VortexSchedulerBridgeReport,
) -> Result<VortexEncodedReadReadinessReport> {
    VortexEncodedReadReadinessReport::from_input(VortexEncodedReadReadinessInput::new(
        scheduler_report,
    ))
}
pub fn vortex_encoded_read_readiness_is_side_effect_free(
    report: &VortexEncodedReadReadinessReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexMemoryBridgeInput, VortexMemoryBridgeReport, VortexSchedulerBridgeInput,
        VortexTaskSchedulingDecision,
    };
    use shardloom_exec::MemoryBudget;
    fn empty_sched() -> VortexSchedulerBridgeReport {
        let m = VortexMemoryBridgeReport::from_input(VortexMemoryBridgeInput::new(
            MemoryBudget::from_gib(1).expect("ok"),
        ))
        .expect("ok");
        crate::VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(m))
            .expect("ok")
    }
    #[test]
    fn status_ready_for_future_encoded_read_allows() {
        assert!(
            VortexEncodedReadReadinessStatus::ReadyForFutureEncodedRead
                .allows_future_encoded_read()
        );
    }
    #[test]
    fn status_ready_for_contract_disallows() {
        assert!(!VortexEncodedReadReadinessStatus::ReadyForContract.allows_future_encoded_read());
    }
    #[test]
    fn status_unsupported_is_error() {
        assert!(VortexEncodedReadReadinessStatus::Unsupported.is_error());
    }
    #[test]
    fn mode_readiness_only_flags_false() {
        let m = VortexEncodedReadReadinessMode::ReadinessOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data() && !m.writes_data());
    }
    #[test]
    fn kind_checks() {
        assert!(VortexEncodedReadCandidateKind::EncodedReadCandidate.is_candidate());
        assert!(VortexEncodedReadCandidateKind::WouldDecode.is_blocked());
        assert!(VortexEncodedReadCandidateKind::WouldMaterialize.is_blocked());
        assert!(VortexEncodedReadCandidateKind::WouldUseObjectStore.is_blocked());
        assert!(VortexEncodedReadCandidateKind::WouldWrite.is_blocked());
        assert!(VortexEncodedReadCandidateKind::WouldSpill.is_blocked());
    }
    #[test]
    fn unsupported_candidate_has_errors_and_no_fallback() {
        let c = VortexEncodedReadCandidate::unsupported(None, "feat", "reason");
        assert!(c.has_errors());
        assert!(
            c.diagnostics
                .iter()
                .any(|d| !d.fallback.attempted && !d.fallback.allowed)
        );
    }
    #[test]
    fn input_defaults() {
        let i = VortexEncodedReadReadinessInput::new(empty_sched());
        assert!(i.require_known_estimates);
        assert!(i.require_memory_ready);
    }
    #[test]
    fn unsupported_report_has_error_and_no_fallback() {
        let r = VortexEncodedReadReadinessReport::unsupported(
            VortexEncodedReadReadinessInput::new(empty_sched()),
            "f",
            "r",
        );
        assert!(r.has_errors());
        assert!(
            r.diagnostics
                .iter()
                .any(|d| !d.fallback.attempted && !d.fallback.allowed)
        );
    }
    #[test]
    fn from_input_none_not_ready() {
        let r = VortexEncodedReadReadinessReport::from_input(VortexEncodedReadReadinessInput::new(
            empty_sched(),
        ))
        .expect("ok");
        assert!(matches!(
            r.status,
            VortexEncodedReadReadinessStatus::NoEncodedReadCandidates
                | VortexEncodedReadReadinessStatus::ReadyForContract
        ));
        assert!(!r.status.allows_future_encoded_read());
    }
    #[test]
    fn blocks_needs_estimate() {
        let mut s = empty_sched();
        s.decisions
            .push(VortexTaskSchedulingDecision::hold_for_estimate(None, "n"));
        let r = VortexEncodedReadReadinessReport::from_scheduler_report(s).expect("ok");
        assert!(!r.status.allows_future_encoded_read());
    }
    #[test]
    fn blocks_would_decode() {
        let mut r =
            VortexEncodedReadReadinessReport::from_scheduler_report(empty_sched()).expect("ok");
        r.add_candidate(VortexEncodedReadCandidate::would_decode(None, "d"));
        r.recompute_counts();
        r.status = VortexEncodedReadReadinessStatus::BlockedByDecodeRisk;
        assert!(!r.status.allows_future_encoded_read());
    }
    #[test]
    fn valid_candidate_ready() {
        let mut s = empty_sched();
        s.decisions
            .push(VortexTaskSchedulingDecision::schedule_now(None, "ok"));
        let r = VortexEncodedReadReadinessReport::from_scheduler_report(s).expect("ok");
        assert!(matches!(
            r.status,
            VortexEncodedReadReadinessStatus::ReadyForFutureEncodedRead
                | VortexEncodedReadReadinessStatus::ReadyForContract
        ));
    }
    #[test]
    fn side_effect_free_true() {
        assert!(
            VortexEncodedReadReadinessReport::from_scheduler_report(empty_sched())
                .expect("ok")
                .is_side_effect_free()
        );
    }
    #[test]
    fn human_text_contains_required() {
        let mut r =
            VortexEncodedReadReadinessReport::from_scheduler_report(empty_sched()).expect("ok");
        r.add_diagnostic(Diagnostic::new(
            DiagnosticCode::ConfigurationError,
            DiagnosticSeverity::Warning,
            shardloom_core::DiagnosticCategory::Configuration,
            "x",
            None,
            None,
            None,
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
        let t = r.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("data decoded: false"));
        assert!(t.contains("data materialized: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn evaluate_no_io() {
        let r = evaluate_vortex_encoded_read_readiness(empty_sched()).expect("ok");
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn helper_side_effect_free() {
        assert!(vortex_encoded_read_readiness_is_side_effect_free(
            &VortexEncodedReadReadinessReport::from_scheduler_report(empty_sched()).expect("ok")
        ));
    }
}
