#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexMetadataSummaryReport, VortexQueryPrimitiveAnalysisReport, VortexQueryPrimitiveRequest,
    VortexQueryPrimitiveResult, VortexQueryPrimitiveStatus, VortexQueryPrimitiveValue,
    analyze_vortex_query_primitive_result, evaluate_vortex_query_primitive,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalExecutionStatus {
    Planned,
    MetadataExecuted,
    NoOpCompleted,
    NeedsEncodedRead,
    NeedsPredicateEvaluation,
    MissingMetadata,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedBySpillIo,
    BlockedByExternalEffect,
    Unsupported,
}
impl VortexLocalExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataExecuted => "metadata_executed",
            Self::NoOpCompleted => "no_op_completed",
            Self::NeedsEncodedRead => "needs_encoded_read",
            Self::NeedsPredicateEvaluation => "needs_predicate_evaluation",
            Self::MissingMetadata => "missing_metadata",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::BlockedByWriteIo => "blocked_by_write_io",
            Self::BlockedBySpillIo => "blocked_by_spill_io",
            Self::BlockedByExternalEffect => "blocked_by_external_effect",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByDecodeRisk
                | Self::BlockedByMaterializationRisk
                | Self::BlockedByObjectStoreIo
                | Self::BlockedByWriteIo
                | Self::BlockedBySpillIo
                | Self::BlockedByExternalEffect
                | Self::Unsupported
        )
    }
    pub const fn completed_without_data_read(&self) -> bool {
        matches!(self, Self::MetadataExecuted | Self::NoOpCompleted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalExecutionMode {
    MetadataOnly,
    NoOp,
    PlanOnly,
    Blocked,
    Unsupported,
}
impl VortexLocalExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::NoOp => "no_op",
            Self::PlanOnly => "plan_only",
            Self::Blocked => "blocked",
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
pub enum VortexLocalExecutionStepKind {
    EvaluateQueryPrimitive,
    AttachDecisionTrace,
    AttachWorkAvoidedReport,
    ReturnMetadataResult,
    DeferEncodedRead,
    BlockUnsafePath,
    Unsupported,
}
impl VortexLocalExecutionStepKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvaluateQueryPrimitive => "evaluate_query_primitive",
            Self::AttachDecisionTrace => "attach_decision_trace",
            Self::AttachWorkAvoidedReport => "attach_work_avoided_report",
            Self::ReturnMetadataResult => "return_metadata_result",
            Self::DeferEncodedRead => "defer_encoded_read",
            Self::BlockUnsafePath => "block_unsafe_path",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::ReturnMetadataResult)
    }
    pub const fn is_blocking(&self) -> bool {
        matches!(self, Self::BlockUnsafePath | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexLocalExecutionStep {
    pub kind: VortexLocalExecutionStepKind,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalExecutionStep {
    pub fn new(kind: VortexLocalExecutionStepKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let mut out = Self::new(VortexLocalExecutionStepKind::Unsupported, reason.clone());
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Requested local execution step is unsupported.",
            Some(reason),
        ));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
        format!("{}: {}", self.kind.as_str(), self.reason)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexLocalExecutionInput {
    pub request: VortexQueryPrimitiveRequest,
    pub metadata_summary: Option<VortexMetadataSummaryReport>,
    pub allow_encoded_read: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalExecutionInput {
    pub fn new(request: VortexQueryPrimitiveRequest) -> Self {
        Self {
            request,
            metadata_summary: None,
            allow_encoded_read: false,
            diagnostics: vec![],
        }
    }
    pub fn with_metadata_summary(mut self, metadata_summary: VortexMetadataSummaryReport) -> Self {
        self.metadata_summary = Some(metadata_summary);
        self
    }
    pub fn allow_encoded_read(mut self, value: bool) -> Self {
        self.allow_encoded_read = value;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
            "kind={} metadata_summary={} allow_encoded_read={} diagnostics={}",
            self.request.kind.as_str(),
            self.metadata_summary.is_some(),
            self.allow_encoded_read,
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexLocalExecutionValue {
    QueryPrimitive(VortexQueryPrimitiveValue),
    Deferred,
    Unknown,
}
impl VortexLocalExecutionValue {
    pub const fn is_known(&self) -> bool {
        matches!(
            self,
            Self::QueryPrimitive(
                VortexQueryPrimitiveValue::Count(_)
                    | VortexQueryPrimitiveValue::Boolean(_)
                    | VortexQueryPrimitiveValue::Text(_)
            )
        )
    }
    pub fn summary(&self) -> String {
        match self {
            Self::QueryPrimitive(v) => v.as_str(),
            Self::Deferred => "deferred".to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalExecutionReport {
    pub status: VortexLocalExecutionStatus,
    pub mode: VortexLocalExecutionMode,
    pub input: VortexLocalExecutionInput,
    pub primitive_result: Option<VortexQueryPrimitiveResult>,
    pub analysis_report: Option<VortexQueryPrimitiveAnalysisReport>,
    pub value: VortexLocalExecutionValue,
    pub steps: Vec<VortexLocalExecutionStep>,
    pub tasks_executed: bool,
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
impl VortexLocalExecutionReport {
    /// # Errors
    /// Returns an error if query primitive evaluation or analysis fails.
    pub fn from_input(input: VortexLocalExecutionInput) -> Result<Self> {
        let Some(summary) = input.metadata_summary.clone() else {
            return Ok(Self::missing_metadata(
                input,
                "metadata summary was not provided",
            ));
        };
        let result = evaluate_vortex_query_primitive(input.request.clone(), &summary)?;
        let analysis = analyze_vortex_query_primitive_result(result.clone());
        Ok(match result.status {
            VortexQueryPrimitiveStatus::MetadataAnswered => {
                Self::metadata_executed(input, result, analysis)
            }
            VortexQueryPrimitiveStatus::NeedsEncodedRead
            | VortexQueryPrimitiveStatus::NeedsEncodedPredicate => {
                Self::needs_encoded_read(input, result, analysis)
            }
            VortexQueryPrimitiveStatus::MissingMetadata => {
                Self::missing_metadata(input, "query primitive result reported missing metadata")
            }
            VortexQueryPrimitiveStatus::Unsupported => Self::unsupported(
                input,
                "vortex_local_execution",
                "query primitive status is unsupported for metadata-only local execution",
            ),
            _ => Self::unsupported(
                input,
                "vortex_local_execution",
                "query primitive status is not supported by local execution skeleton",
            ),
        })
    }
    pub fn metadata_executed(
        input: VortexLocalExecutionInput,
        primitive_result: VortexQueryPrimitiveResult,
        analysis_report: VortexQueryPrimitiveAnalysisReport,
    ) -> Self {
        let mut r = Self::base(
            VortexLocalExecutionStatus::MetadataExecuted,
            VortexLocalExecutionMode::MetadataOnly,
            input,
        );
        r.value = VortexLocalExecutionValue::QueryPrimitive(primitive_result.value.clone());
        r.primitive_result = Some(primitive_result);
        r.analysis_report = Some(analysis_report);
        r.add_step(VortexLocalExecutionStep::new(
            VortexLocalExecutionStepKind::EvaluateQueryPrimitive,
            "evaluated query primitive from metadata summary",
        ));
        r.add_step(VortexLocalExecutionStep::new(
            VortexLocalExecutionStepKind::AttachDecisionTrace,
            "attached decision trace from primitive analysis",
        ));
        r.add_step(VortexLocalExecutionStep::new(
            VortexLocalExecutionStepKind::AttachWorkAvoidedReport,
            "attached work avoided report from primitive analysis",
        ));
        r.add_step(VortexLocalExecutionStep::new(
            VortexLocalExecutionStepKind::ReturnMetadataResult,
            "returned metadata-only primitive result",
        ));
        r
    }
    pub fn needs_encoded_read(
        input: VortexLocalExecutionInput,
        primitive_result: VortexQueryPrimitiveResult,
        analysis_report: VortexQueryPrimitiveAnalysisReport,
    ) -> Self {
        let status = if matches!(
            primitive_result.status,
            VortexQueryPrimitiveStatus::NeedsEncodedPredicate
        ) {
            VortexLocalExecutionStatus::NeedsPredicateEvaluation
        } else {
            VortexLocalExecutionStatus::NeedsEncodedRead
        };
        let mut r = Self::base(status, VortexLocalExecutionMode::PlanOnly, input);
        r.value = VortexLocalExecutionValue::Deferred;
        r.primitive_result = Some(primitive_result);
        r.analysis_report = Some(analysis_report);
        r.add_step(VortexLocalExecutionStep::new(VortexLocalExecutionStepKind::DeferEncodedRead,"encoded read or encoded predicate evaluation is required and deferred by phase-10a skeleton"));
        r
    }
    pub fn missing_metadata(input: VortexLocalExecutionInput, reason: impl Into<String>) -> Self {
        let mut r = Self::base(
            VortexLocalExecutionStatus::MissingMetadata,
            VortexLocalExecutionMode::Blocked,
            input,
        );
        r.add_step(VortexLocalExecutionStep::new(
            VortexLocalExecutionStepKind::BlockUnsafePath,
            reason.into(),
        ));
        r
    }
    pub fn unsupported(
        input: VortexLocalExecutionInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self::base(
            VortexLocalExecutionStatus::Unsupported,
            VortexLocalExecutionMode::Unsupported,
            input,
        );
        let reason = reason.into();
        r.add_step(VortexLocalExecutionStep::unsupported(
            feature,
            reason.clone(),
        ));
        r.add_diagnostic(Diagnostic::unsupported(DiagnosticCode::NoFallbackExecution,"vortex_local_execution", "Local execution skeleton encountered unsupported behavior. Fallback execution was not attempted.", Some(reason)));
        r
    }
    fn base(
        status: VortexLocalExecutionStatus,
        mode: VortexLocalExecutionMode,
        input: VortexLocalExecutionInput,
    ) -> Self {
        Self {
            status,
            mode,
            input,
            primitive_result: None,
            analysis_report: None,
            value: VortexLocalExecutionValue::Unknown,
            steps: vec![],
            tasks_executed: false,
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
    pub fn add_step(&mut self, step: VortexLocalExecutionStep) {
        self.steps.push(step);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self.input.has_errors()
            || self.steps.iter().any(VortexLocalExecutionStep::has_errors)
            || self
                .primitive_result
                .as_ref()
                .is_some_and(VortexQueryPrimitiveResult::has_errors)
            || self
                .analysis_report
                .as_ref()
                .is_some_and(VortexQueryPrimitiveAnalysisReport::has_errors)
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.tasks_executed
            && !self.data_read
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
        let _ = writeln!(
            out,
            "Vortex local execution report\nstatus: {}\nmode: {}\nquery primitive kind: {}\nvalue: {}\nstep count: {}",
            self.status.as_str(),
            self.mode.as_str(),
            self.input.request.kind.as_str(),
            self.value.summary(),
            self.steps.len()
        );
        if let Some(analysis) = &self.analysis_report {
            let _ = writeln!(
                out,
                "decision trace count: {}",
                analysis.decision_trace.entry_count()
            );
            let _ = writeln!(
                out,
                "work avoided metric count: {}",
                analysis.work_avoided.metric_count()
            );
        }
        let _ = write!(
            out,
            "tasks executed: {}\ndata read: {}\ndata decoded: {}\ndata materialized: {}\nobject-store IO: {}\nwrite IO: {}\nspill IO: {}\nexternal effects executed: {}\nfallback execution: disabled",
            self.tasks_executed,
            self.data_read,
            self.data_decoded,
            self.data_materialized,
            self.object_store_io,
            self.write_io,
            self.spill_io_performed,
            self.external_effects_executed
        );
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {}", d.to_human_text());
            }
        }
        out
    }
}

/// Executes `Vortex` local query primitive skeleton path.
/// # Errors
/// Returns an error if primitive evaluation or analysis construction fails.
pub fn execute_vortex_local_query_primitive(
    request: VortexQueryPrimitiveRequest,
    metadata_summary: Option<VortexMetadataSummaryReport>,
) -> Result<VortexLocalExecutionReport> {
    let mut input = VortexLocalExecutionInput::new(request);
    if let Some(summary) = metadata_summary {
        input = input.with_metadata_summary(summary);
    }
    VortexLocalExecutionReport::from_input(input)
}

pub fn vortex_local_execution_is_side_effect_free(report: &VortexLocalExecutionReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VortexFileMetadataSummary, VortexMetadataSummaryStatus};
    use shardloom_core::DatasetUri;
    fn count_request() -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::count_all(DatasetUri::new("file://tmp/a.vortex").expect("uri"))
    }
    fn count_summary(rows: u64) -> VortexMetadataSummaryReport {
        VortexMetadataSummaryReport {
            status: VortexMetadataSummaryStatus::Summarized,
            summary: VortexFileMetadataSummary {
                row_count: Some(rows),
                ..VortexFileMetadataSummary::empty()
            },
            diagnostics: vec![],
        }
    }
    #[test]
    fn status_checks() {
        assert!(VortexLocalExecutionStatus::MetadataExecuted.completed_without_data_read());
        assert!(!VortexLocalExecutionStatus::NeedsEncodedRead.is_error());
        assert!(VortexLocalExecutionStatus::Unsupported.is_error());
    }
    #[test]
    fn mode_flags_false() {
        let m = VortexLocalExecutionMode::MetadataOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data() && !m.writes_data());
    }
    #[test]
    fn step_kind_flags() {
        assert!(VortexLocalExecutionStepKind::ReturnMetadataResult.is_completed());
        assert!(VortexLocalExecutionStepKind::BlockUnsafePath.is_blocking());
    }
    #[test]
    fn unsupported_step_has_error_and_no_fallback() {
        let s = VortexLocalExecutionStep::unsupported("x", "y");
        assert!(s.has_errors());
        assert!(s.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
    #[test]
    fn input_defaults() {
        assert!(!VortexLocalExecutionInput::new(count_request()).allow_encoded_read);
    }
    #[test]
    fn missing_metadata_is_side_effect_free() {
        let r =
            VortexLocalExecutionReport::from_input(VortexLocalExecutionInput::new(count_request()))
                .expect("ok");
        assert_eq!(r.status, VortexLocalExecutionStatus::MissingMetadata);
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn metadata_executed_count() {
        let r = VortexLocalExecutionReport::from_input(
            VortexLocalExecutionInput::new(count_request()).with_metadata_summary(count_summary(7)),
        )
        .expect("ok");
        assert_eq!(r.status, VortexLocalExecutionStatus::MetadataExecuted);
        assert_eq!(
            r.value,
            VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(7))
        );
        assert!(
            !r.tasks_executed
                && !r.data_read
                && !r.data_decoded
                && !r.data_materialized
                && !r.fallback_execution_allowed
        );
        let t = r.to_human_text();
        assert!(t.contains("fallback execution: disabled"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("decision trace count"));
        assert!(t.contains("work avoided metric count"));
    }
    #[test]
    fn needs_encoded_read_side_effect_free() {
        let req = VortexQueryPrimitiveRequest::project(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
            shardloom_plan::ProjectionRequest::all(),
        );
        let r = VortexLocalExecutionReport::from_input(
            VortexLocalExecutionInput::new(req).with_metadata_summary(count_summary(1)),
        )
        .expect("ok");
        assert!(matches!(
            r.status,
            VortexLocalExecutionStatus::NeedsEncodedRead
                | VortexLocalExecutionStatus::NeedsPredicateEvaluation
        ));
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn unsupported_has_errors_no_fallback() {
        let r = VortexLocalExecutionReport::unsupported(
            VortexLocalExecutionInput::new(count_request()),
            "x",
            "y",
        );
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn helper_exec_no_io() {
        let r = execute_vortex_local_query_primitive(count_request(), None).expect("ok");
        assert!(vortex_local_execution_is_side_effect_free(&r));
    }
}
