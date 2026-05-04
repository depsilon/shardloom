use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId};
use shardloom_exec::TaskId;

use crate::{
    VortexEncodedReadApiBoundaryReport, VortexEncodedReadApiBoundaryStatus,
    VortexEncodedReadCandidate, VortexEncodedReadCandidateKind, VortexEncodedReadExecutionReport,
    VortexEncodedReadReadinessReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadProbeStatus {
    ProbePlanReady,
    ProbePartiallyReady,
    NoEligibleCandidates,
    BlockedByApiBoundary,
    BlockedByReadiness,
    BlockedByFeatureGate,
    BlockedByMissingEstimate,
    BlockedByMissingByteRange,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedBySpillIo,
    Unsupported,
}
impl VortexEncodedReadProbeStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProbePlanReady => "probe_plan_ready",
            Self::ProbePartiallyReady => "probe_partially_ready",
            Self::NoEligibleCandidates => "no_eligible_candidates",
            Self::BlockedByApiBoundary => "blocked_by_api_boundary",
            Self::BlockedByReadiness => "blocked_by_readiness",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByMissingByteRange => "blocked_by_missing_byte_range",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::BlockedByWriteIo => "blocked_by_write_io",
            Self::BlockedBySpillIo => "blocked_by_spill_io",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::ProbePlanReady | Self::ProbePartiallyReady | Self::NoEligibleCandidates
        )
    }
    #[must_use]
    pub const fn allows_future_probe(self) -> bool {
        matches!(self, Self::ProbePlanReady | Self::ProbePartiallyReady)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadProbeMode {
    ContractOnly,
    ProbePlanOnly,
    Unsupported,
}
impl VortexEncodedReadProbeMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ContractOnly => "contract_only",
            Self::ProbePlanOnly => "probe_plan_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn reads_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn decodes_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn materializes_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_data(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadProbeCandidateKind {
    EligibleFutureProbe,
    DeferredByApiBoundary,
    DeferredByReadiness,
    MissingEstimate,
    MissingByteRange,
    DecodeRisk,
    MaterializationRisk,
    ObjectStoreRisk,
    WriteRisk,
    SpillRisk,
    Unsupported,
}
impl VortexEncodedReadProbeCandidateKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EligibleFutureProbe => "eligible_future_probe",
            Self::DeferredByApiBoundary => "deferred_by_api_boundary",
            Self::DeferredByReadiness => "deferred_by_readiness",
            Self::MissingEstimate => "missing_estimate",
            Self::MissingByteRange => "missing_byte_range",
            Self::DecodeRisk => "decode_risk",
            Self::MaterializationRisk => "materialization_risk",
            Self::ObjectStoreRisk => "object_store_risk",
            Self::WriteRisk => "write_risk",
            Self::SpillRisk => "spill_risk",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_eligible(self) -> bool {
        matches!(self, Self::EligibleFutureProbe)
    }
    #[must_use]
    pub const fn is_blocked(self) -> bool {
        !matches!(
            self,
            Self::EligibleFutureProbe | Self::DeferredByApiBoundary | Self::DeferredByReadiness
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexProbeSideEffect {
    DataRead,
    DataDecoded,
    DataMaterialized,
    ObjectStoreIo,
    WriteIo,
    SpillIo,
    ExternalEffect,
    FallbackExecution,
}
impl VortexProbeSideEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DataRead => "data_read",
            Self::DataDecoded => "data_decoded",
            Self::DataMaterialized => "data_materialized",
            Self::ObjectStoreIo => "object_store_io",
            Self::WriteIo => "write_io",
            Self::SpillIo => "spill_io",
            Self::ExternalEffect => "external_effect",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexProbeRequirement {
    ApiContractReady,
    KnownEstimates,
    ByteRanges,
    MemoryReady,
}
impl VortexProbeRequirement {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApiContractReady => "api_contract_ready",
            Self::KnownEstimates => "known_estimates",
            Self::ByteRanges => "byte_ranges",
            Self::MemoryReady => "memory_ready",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadProbeCandidate {
    pub kind: VortexEncodedReadProbeCandidateKind,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub split_id: Option<String>,
    pub api_item_name: Option<String>,
    pub readiness_candidate_kind: Option<String>,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadProbeCandidate {
    fn base(
        kind: VortexEncodedReadProbeCandidateKind,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id,
            segment_id: None,
            split_id: None,
            api_item_name: None,
            readiness_candidate_kind: None,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn eligible_future_probe(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::EligibleFutureProbe,
            task_id,
            reason,
        )
    }
    pub fn deferred_by_api_boundary(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::DeferredByApiBoundary,
            task_id,
            reason,
        )
    }
    pub fn deferred_by_readiness(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::DeferredByReadiness,
            task_id,
            reason,
        )
    }
    pub fn missing_estimate(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::MissingEstimate,
            task_id,
            reason,
        )
    }
    pub fn missing_byte_range(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::MissingByteRange,
            task_id,
            reason,
        )
    }
    pub fn decode_risk(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::DecodeRisk,
            task_id,
            reason,
        )
    }
    pub fn materialization_risk(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::MaterializationRisk,
            task_id,
            reason,
        )
    }
    pub fn object_store_risk(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::ObjectStoreRisk,
            task_id,
            reason,
        )
    }
    pub fn write_risk(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::WriteRisk,
            task_id,
            reason,
        )
    }
    pub fn spill_risk(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexEncodedReadProbeCandidateKind::SpillRisk,
            task_id,
            reason,
        )
    }
    pub fn unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexEncodedReadProbeCandidateKind::Unsupported,
            task_id,
            reason,
        );
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            "unsupported probe candidate",
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    #[must_use]
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    #[must_use]
    pub fn with_split_id(mut self, split_id: impl Into<String>) -> Self {
        self.split_id = Some(split_id.into());
        self
    }
    #[must_use]
    pub fn with_api_item_name(mut self, v: impl Into<String>) -> Self {
        self.api_item_name = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_readiness_candidate_kind(mut self, v: impl Into<String>) -> Self {
        self.readiness_candidate_kind = Some(v.into());
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        self.kind.is_eligible()
    }
    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        self.kind.is_blocked()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("kind={} reason={}", self.kind.as_str(), self.reason)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadProbeInput {
    pub api_boundary_report: VortexEncodedReadApiBoundaryReport,
    pub encoded_readiness_report: VortexEncodedReadReadinessReport,
    pub encoded_execution_report: Option<VortexEncodedReadExecutionReport>,
    pub requirements: Vec<VortexProbeRequirement>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadProbeInput {
    #[must_use]
    pub fn new(
        api_boundary_report: VortexEncodedReadApiBoundaryReport,
        encoded_readiness_report: VortexEncodedReadReadinessReport,
    ) -> Self {
        Self {
            api_boundary_report,
            encoded_readiness_report,
            encoded_execution_report: None,
            requirements: vec![
                VortexProbeRequirement::ApiContractReady,
                VortexProbeRequirement::KnownEstimates,
                VortexProbeRequirement::MemoryReady,
            ],
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_encoded_execution_report(
        mut self,
        report: VortexEncodedReadExecutionReport,
    ) -> Self {
        self.encoded_execution_report = Some(report);
        self
    }
    #[must_use]
    pub fn require_api_contract_ready(mut self, value: bool) -> Self {
        set_req(
            &mut self.requirements,
            VortexProbeRequirement::ApiContractReady,
            value,
        );
        self
    }
    #[must_use]
    pub fn require_known_estimates(mut self, value: bool) -> Self {
        set_req(
            &mut self.requirements,
            VortexProbeRequirement::KnownEstimates,
            value,
        );
        self
    }
    #[must_use]
    pub fn require_byte_ranges(mut self, value: bool) -> Self {
        set_req(
            &mut self.requirements,
            VortexProbeRequirement::ByteRanges,
            value,
        );
        self
    }
    #[must_use]
    pub fn require_memory_ready(mut self, value: bool) -> Self {
        set_req(
            &mut self.requirements,
            VortexProbeRequirement::MemoryReady,
            value,
        );
        self
    }
    #[must_use]
    pub fn requires(&self, requirement: VortexProbeRequirement) -> bool {
        self.requirements.contains(&requirement)
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("requirements={}", self.requirements.len())
    }
}
fn set_req(reqs: &mut Vec<VortexProbeRequirement>, req: VortexProbeRequirement, enabled: bool) {
    if enabled {
        if !reqs.contains(&req) {
            reqs.push(req);
        }
    } else {
        reqs.retain(|r| *r != req);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexProbeCounts {
    pub eligible_candidate_count: usize,
    pub blocked_candidate_count: usize,
    pub deferred_candidate_count: usize,
    pub missing_estimate_count: usize,
    pub missing_byte_range_count: usize,
    pub decode_risk_count: usize,
    pub materialization_risk_count: usize,
    pub object_store_risk_count: usize,
    pub write_risk_count: usize,
    pub spill_risk_count: usize,
}
impl VortexProbeCounts {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            eligible_candidate_count: 0,
            blocked_candidate_count: 0,
            deferred_candidate_count: 0,
            missing_estimate_count: 0,
            missing_byte_range_count: 0,
            decode_risk_count: 0,
            materialization_risk_count: 0,
            object_store_risk_count: 0,
            write_risk_count: 0,
            spill_risk_count: 0,
        }
    }
    #[must_use]
    pub fn recompute(candidates: &[VortexEncodedReadProbeCandidate]) -> Self {
        let mut s = Self::empty();
        for c in candidates {
            match c.kind {
                VortexEncodedReadProbeCandidateKind::EligibleFutureProbe => {
                    s.eligible_candidate_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::DeferredByApiBoundary
                | VortexEncodedReadProbeCandidateKind::DeferredByReadiness => {
                    s.deferred_candidate_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::MissingEstimate => {
                    s.blocked_candidate_count += 1;
                    s.missing_estimate_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::MissingByteRange => {
                    s.blocked_candidate_count += 1;
                    s.missing_byte_range_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::DecodeRisk => {
                    s.blocked_candidate_count += 1;
                    s.decode_risk_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::MaterializationRisk => {
                    s.blocked_candidate_count += 1;
                    s.materialization_risk_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::ObjectStoreRisk => {
                    s.blocked_candidate_count += 1;
                    s.object_store_risk_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::WriteRisk => {
                    s.blocked_candidate_count += 1;
                    s.write_risk_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::SpillRisk => {
                    s.blocked_candidate_count += 1;
                    s.spill_risk_count += 1;
                }
                VortexEncodedReadProbeCandidateKind::Unsupported => s.blocked_candidate_count += 1,
            }
        }
        s
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedReadProbeReport {
    pub status: VortexEncodedReadProbeStatus,
    pub mode: VortexEncodedReadProbeMode,
    pub input: VortexEncodedReadProbeInput,
    pub candidates: Vec<VortexEncodedReadProbeCandidate>,
    pub counts: VortexProbeCounts,
    pub side_effects_performed: Vec<VortexProbeSideEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadProbeReport {
    /// # Errors
    /// Returns an error propagated from inner report validation.
    pub fn from_input(input: VortexEncodedReadProbeInput) -> Result<Self> {
        let mut s = Self {
            status: VortexEncodedReadProbeStatus::NoEligibleCandidates,
            mode: VortexEncodedReadProbeMode::ProbePlanOnly,
            input,
            candidates: vec![],
            counts: VortexProbeCounts::empty(),
            side_effects_performed: vec![],
            diagnostics: vec![],
        };
        s.diagnostics.extend(s.input.diagnostics.clone());
        s.diagnostics
            .extend(s.input.api_boundary_report.diagnostics.clone());
        s.diagnostics
            .extend(s.input.encoded_readiness_report.diagnostics.clone());
        let api_blocked = s.input.requires(VortexProbeRequirement::ApiContractReady)
            && (s.input.api_boundary_report.has_errors()
                || matches!(
                    s.input.api_boundary_report.status,
                    VortexEncodedReadApiBoundaryStatus::BlockedByRisk
                        | VortexEncodedReadApiBoundaryStatus::Unsupported
                ));
        for rc in &s.input.encoded_readiness_report.candidates {
            let pc = map_candidate(rc, &s.input, api_blocked);
            s.candidates.push(pc);
        }
        s.recompute_counts();
        s.status = if s
            .candidates
            .iter()
            .any(|c| matches!(c.kind, VortexEncodedReadProbeCandidateKind::Unsupported))
            || s.has_errors()
        {
            VortexEncodedReadProbeStatus::Unsupported
        } else if api_blocked {
            VortexEncodedReadProbeStatus::BlockedByApiBoundary
        } else if s.input.requires(VortexProbeRequirement::KnownEstimates)
            && s.counts.missing_estimate_count > 0
        {
            VortexEncodedReadProbeStatus::BlockedByMissingEstimate
        } else if s.input.requires(VortexProbeRequirement::ByteRanges)
            && s.counts.missing_byte_range_count > 0
        {
            VortexEncodedReadProbeStatus::BlockedByMissingByteRange
        } else if s.counts.decode_risk_count > 0 {
            VortexEncodedReadProbeStatus::BlockedByDecodeRisk
        } else if s.counts.materialization_risk_count > 0 {
            VortexEncodedReadProbeStatus::BlockedByMaterializationRisk
        } else if s.counts.object_store_risk_count > 0 {
            VortexEncodedReadProbeStatus::BlockedByObjectStoreIo
        } else if s.counts.write_risk_count > 0 {
            VortexEncodedReadProbeStatus::BlockedByWriteIo
        } else if s.counts.spill_risk_count > 0 {
            VortexEncodedReadProbeStatus::BlockedBySpillIo
        } else if s.counts.eligible_candidate_count == 0 {
            VortexEncodedReadProbeStatus::NoEligibleCandidates
        } else if s.counts.deferred_candidate_count > 0 {
            VortexEncodedReadProbeStatus::ProbePartiallyReady
        } else {
            VortexEncodedReadProbeStatus::ProbePlanReady
        };
        Ok(s)
    }
    /// # Errors
    pub fn from_reports(
        api_boundary_report: VortexEncodedReadApiBoundaryReport,
        encoded_readiness_report: VortexEncodedReadReadinessReport,
    ) -> Result<Self> {
        Self::from_input(VortexEncodedReadProbeInput::new(
            api_boundary_report,
            encoded_readiness_report,
        ))
    }
    #[must_use]
    pub fn unsupported(
        input: VortexEncodedReadProbeInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self {
            status: VortexEncodedReadProbeStatus::Unsupported,
            mode: VortexEncodedReadProbeMode::Unsupported,
            input,
            candidates: vec![],
            counts: VortexProbeCounts::empty(),
            side_effects_performed: vec![],
            diagnostics: vec![],
        };
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn add_candidate(&mut self, c: VortexEncodedReadProbeCandidate) {
        self.candidates.push(c);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn recompute_counts(&mut self) {
        self.counts = VortexProbeCounts::recompute(&self.candidates);
    }
    fn has_side_effect(&self, e: VortexProbeSideEffect) -> bool {
        self.side_effects_performed.contains(&e)
    }
    #[must_use]
    pub fn data_read(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::DataRead)
    }
    #[must_use]
    pub fn data_decoded(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::DataDecoded)
    }
    #[must_use]
    pub fn data_materialized(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::DataMaterialized)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn write_io(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::WriteIo)
    }
    #[must_use]
    pub fn spill_io_performed(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::SpillIo)
    }
    #[must_use]
    pub fn external_effects_executed(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::ExternalEffect)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.has_side_effect(VortexProbeSideEffect::FallbackExecution)
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .candidates
                .iter()
                .any(VortexEncodedReadProbeCandidate::has_errors)
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.side_effects_performed.is_empty()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "probe status: {}", self.status.as_str());
        let _ = writeln!(
            out,
            "eligible candidate count: {}",
            self.counts.eligible_candidate_count
        );
        let _ = writeln!(
            out,
            "blocked candidate count: {}",
            self.counts.blocked_candidate_count
        );
        let _ = writeln!(
            out,
            "deferred candidate count: {}",
            self.counts.deferred_candidate_count
        );
        let _ = writeln!(
            out,
            "missing estimate count: {}",
            self.counts.missing_estimate_count
        );
        let _ = writeln!(
            out,
            "missing byte range count: {}",
            self.counts.missing_byte_range_count
        );
        let _ = writeln!(out, "decode risk count: {}", self.counts.decode_risk_count);
        let _ = writeln!(
            out,
            "materialization risk count: {}",
            self.counts.materialization_risk_count
        );
        let _ = writeln!(
            out,
            "object-store risk count: {}",
            self.counts.object_store_risk_count
        );
        let _ = writeln!(out, "write risk count: {}", self.counts.write_risk_count);
        let _ = writeln!(out, "spill risk count: {}", self.counts.spill_risk_count);
        let _ = writeln!(out, "data read: {}", self.data_read());
        let _ = writeln!(out, "data decoded: {}", self.data_decoded());
        let _ = writeln!(out, "data materialized: {}", self.data_materialized());
        let _ = writeln!(out, "object-store IO: {}", self.object_store_io());
        let _ = writeln!(out, "write IO: {}", self.write_io());
        let _ = writeln!(out, "spill IO performed: {}", self.spill_io_performed());
        let _ = writeln!(
            out,
            "external effects executed: {}",
            self.external_effects_executed()
        );
        let _ = writeln!(out, "fallback execution disabled");
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {}", d.message);
            }
        }
        out
    }
}

fn map_candidate(
    rc: &VortexEncodedReadCandidate,
    input: &VortexEncodedReadProbeInput,
    api_blocked: bool,
) -> VortexEncodedReadProbeCandidate {
    let mut base = if api_blocked {
        VortexEncodedReadProbeCandidate::deferred_by_api_boundary(
            rc.task_id.clone(),
            rc.reason.clone(),
        )
    } else {
        match rc.kind {
            VortexEncodedReadCandidateKind::EncodedReadCandidate => {
                let estimate_ok =
                    !input.requires(VortexProbeRequirement::KnownEstimates) || rc.estimate_known;
                let mem_ok =
                    !input.requires(VortexProbeRequirement::MemoryReady) || rc.memory_ready;
                let br_ok =
                    !input.requires(VortexProbeRequirement::ByteRanges) || rc.has_byte_ranges;
                if estimate_ok
                    && mem_ok
                    && br_ok
                    && input.api_boundary_report.status.allows_future_probe()
                {
                    VortexEncodedReadProbeCandidate::eligible_future_probe(
                        rc.task_id.clone(),
                        rc.reason.clone(),
                    )
                } else if !estimate_ok {
                    VortexEncodedReadProbeCandidate::missing_estimate(
                        rc.task_id.clone(),
                        rc.reason.clone(),
                    )
                } else if !br_ok {
                    VortexEncodedReadProbeCandidate::missing_byte_range(
                        rc.task_id.clone(),
                        rc.reason.clone(),
                    )
                } else {
                    VortexEncodedReadProbeCandidate::deferred_by_readiness(
                        rc.task_id.clone(),
                        rc.reason.clone(),
                    )
                }
            }
            VortexEncodedReadCandidateKind::MetadataOnlyNoRead
            | VortexEncodedReadCandidateKind::PrunedNoRead => {
                VortexEncodedReadProbeCandidate::deferred_by_readiness(
                    rc.task_id.clone(),
                    rc.reason.clone(),
                )
            }
            VortexEncodedReadCandidateKind::NeedsEstimate => {
                VortexEncodedReadProbeCandidate::missing_estimate(
                    rc.task_id.clone(),
                    rc.reason.clone(),
                )
            }
            VortexEncodedReadCandidateKind::NeedsByteRange => {
                VortexEncodedReadProbeCandidate::missing_byte_range(
                    rc.task_id.clone(),
                    rc.reason.clone(),
                )
            }
            VortexEncodedReadCandidateKind::WouldDecode => {
                VortexEncodedReadProbeCandidate::decode_risk(rc.task_id.clone(), rc.reason.clone())
            }
            VortexEncodedReadCandidateKind::WouldMaterialize => {
                VortexEncodedReadProbeCandidate::materialization_risk(
                    rc.task_id.clone(),
                    rc.reason.clone(),
                )
            }
            VortexEncodedReadCandidateKind::WouldUseObjectStore => {
                VortexEncodedReadProbeCandidate::object_store_risk(
                    rc.task_id.clone(),
                    rc.reason.clone(),
                )
            }
            VortexEncodedReadCandidateKind::WouldWrite => {
                VortexEncodedReadProbeCandidate::write_risk(rc.task_id.clone(), rc.reason.clone())
            }
            VortexEncodedReadCandidateKind::WouldSpill => {
                VortexEncodedReadProbeCandidate::spill_risk(rc.task_id.clone(), rc.reason.clone())
            }
            VortexEncodedReadCandidateKind::Unsupported => {
                VortexEncodedReadProbeCandidate::unsupported(
                    rc.task_id.clone(),
                    "encoded_read_probe",
                    rc.reason.clone(),
                )
            }
        }
    };
    base.segment_id.clone_from(&rc.segment_id);
    base.split_id.clone_from(&rc.split_id);
    base.readiness_candidate_kind = Some(rc.kind.as_str().to_string());
    base.diagnostics.extend(rc.diagnostics.clone());
    base
}

/// # Errors
pub fn plan_vortex_encoded_read_probe(
    api_boundary_report: VortexEncodedReadApiBoundaryReport,
    encoded_readiness_report: VortexEncodedReadReadinessReport,
) -> Result<VortexEncodedReadProbeReport> {
    VortexEncodedReadProbeReport::from_reports(api_boundary_report, encoded_readiness_report)
}
#[must_use]
pub fn vortex_encoded_read_probe_is_side_effect_free(
    report: &VortexEncodedReadProbeReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexEncodedReadApiBoundaryReport, VortexEncodedReadApiBoundaryStatus,
        VortexMemoryBridgeInput, VortexMemoryBridgeReport, VortexSchedulerBridgeInput,
        VortexSchedulerBridgeReport,
    };
    use shardloom_exec::MemoryBudget;

    fn test_api_boundary_ready() -> VortexEncodedReadApiBoundaryReport {
        let mut r = VortexEncodedReadApiBoundaryReport::default_deferred();
        r.status = VortexEncodedReadApiBoundaryStatus::ContractReady;
        r
    }
    fn test_api_boundary_blocked() -> VortexEncodedReadApiBoundaryReport {
        let mut r = VortexEncodedReadApiBoundaryReport::default_deferred();
        r.status = VortexEncodedReadApiBoundaryStatus::BlockedByRisk;
        r
    }
    fn test_readiness_empty() -> VortexEncodedReadReadinessReport {
        let m = VortexMemoryBridgeReport::from_input(VortexMemoryBridgeInput::new(
            MemoryBudget::from_gib(1).expect("ok"),
        ))
        .expect("ok");
        let s = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(m))
            .expect("ok");
        VortexEncodedReadReadinessReport::from_scheduler_report(s).expect("ok")
    }
    fn test_readiness_with_candidate(
        kind: VortexEncodedReadCandidateKind,
    ) -> VortexEncodedReadReadinessReport {
        let mut r = test_readiness_empty();
        let c = match kind {
            VortexEncodedReadCandidateKind::EncodedReadCandidate => {
                VortexEncodedReadCandidate::encoded_read(None, "x")
            }
            VortexEncodedReadCandidateKind::MetadataOnlyNoRead => {
                VortexEncodedReadCandidate::metadata_only(None, "x")
            }
            VortexEncodedReadCandidateKind::PrunedNoRead => {
                VortexEncodedReadCandidate::pruned(None, "x")
            }
            VortexEncodedReadCandidateKind::NeedsEstimate => {
                VortexEncodedReadCandidate::needs_estimate(None, "x")
            }
            VortexEncodedReadCandidateKind::NeedsByteRange => {
                VortexEncodedReadCandidate::needs_byte_range(None, "x")
            }
            VortexEncodedReadCandidateKind::WouldDecode => {
                VortexEncodedReadCandidate::would_decode(None, "x")
            }
            VortexEncodedReadCandidateKind::WouldMaterialize => {
                VortexEncodedReadCandidate::would_materialize(None, "x")
            }
            VortexEncodedReadCandidateKind::WouldUseObjectStore => {
                VortexEncodedReadCandidate::would_use_object_store(None, "x")
            }
            VortexEncodedReadCandidateKind::WouldWrite => {
                VortexEncodedReadCandidate::would_write(None, "x")
            }
            VortexEncodedReadCandidateKind::WouldSpill => {
                VortexEncodedReadCandidate::would_spill(None, "x")
            }
            VortexEncodedReadCandidateKind::Unsupported => {
                VortexEncodedReadCandidate::unsupported(None, "f", "x")
            }
        };
        r.add_candidate(c);
        r.recompute_counts();
        r
    }

    #[test]
    fn probe_ready_allows_future_probe() {
        assert!(VortexEncodedReadProbeStatus::ProbePlanReady.allows_future_probe());
    }
    #[test]
    fn blocked_is_error() {
        assert!(VortexEncodedReadProbeStatus::BlockedByApiBoundary.is_error());
    }
    #[test]
    fn mode_flags_false() {
        let m = VortexEncodedReadProbeMode::ProbePlanOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data() && !m.writes_data());
    }
    #[test]
    fn kind_checks() {
        assert!(VortexEncodedReadProbeCandidateKind::EligibleFutureProbe.is_eligible());
        assert!(VortexEncodedReadProbeCandidateKind::DecodeRisk.is_blocked());
    }
    #[test]
    fn from_input_empty_no_eligible() {
        let r = VortexEncodedReadProbeReport::from_reports(
            test_api_boundary_ready(),
            test_readiness_empty(),
        )
        .expect("ok");
        assert_eq!(r.status, VortexEncodedReadProbeStatus::NoEligibleCandidates);
    }
    #[test]
    fn from_input_eligible_ready() {
        let r = VortexEncodedReadProbeReport::from_reports(
            test_api_boundary_ready(),
            test_readiness_with_candidate(VortexEncodedReadCandidateKind::EncodedReadCandidate),
        )
        .expect("ok");
        assert_eq!(r.status, VortexEncodedReadProbeStatus::ProbePlanReady);
    }
    #[test]
    fn from_input_decode_blocks() {
        let r = VortexEncodedReadProbeReport::from_reports(
            test_api_boundary_ready(),
            test_readiness_with_candidate(VortexEncodedReadCandidateKind::WouldDecode),
        )
        .expect("ok");
        assert_eq!(r.status, VortexEncodedReadProbeStatus::BlockedByDecodeRisk);
    }
    #[test]
    fn from_input_api_blocked() {
        let r = VortexEncodedReadProbeReport::from_reports(
            test_api_boundary_blocked(),
            test_readiness_empty(),
        )
        .expect("ok");
        assert!(matches!(
            r.status,
            VortexEncodedReadProbeStatus::BlockedByApiBoundary
                | VortexEncodedReadProbeStatus::Unsupported
        ));
    }
    #[test]
    fn text_has_flags() {
        let mut r = VortexEncodedReadProbeReport::from_reports(
            test_api_boundary_ready(),
            test_readiness_empty(),
        )
        .expect("ok");
        r.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "x",
            "y",
            None,
        ));
        let t = r.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("data read: false"));
        assert!(r.is_side_effect_free());
    }
}
