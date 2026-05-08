#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexCountCandidateSource, VortexCountReadinessReport, VortexCountReadinessStatus,
    VortexEncodedCountDataPathApprovalReport, VortexFilteredCountCandidateSource,
    VortexFilteredCountReadinessReport, VortexMetadataSummaryReport,
    VortexQueryPrimitiveAnalysisReport, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
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
            if input.allow_encoded_read && input.request.kind == VortexQueryPrimitiveKind::CountAll
            {
                let result = VortexQueryPrimitiveResult::needs_encoded_read(
                    input.request.clone(),
                    "metadata summary is unavailable and an encoded-data count candidate is approved",
                );
                let analysis = analyze_vortex_query_primitive_result(result.clone());
                return Ok(Self::needs_encoded_read(input, result, analysis));
            }
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

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedCountLocalGuardDiscoveryReport {
    pub schema_version: &'static str,
    pub guard_id: &'static str,
    pub accepted_approval_sources: Vec<&'static str>,
    pub local_execution_status: VortexLocalExecutionStatus,
    pub mode: VortexLocalExecutionMode,
    pub layout_row_count_path_accepted: bool,
    pub returns_count_result: bool,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
}
impl VortexEncodedCountLocalGuardDiscoveryReport {
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.vortex_encoded_count_local_guard.v1",
            guard_id: "cg2.1e-layout-approved-count-local-guard",
            accepted_approval_sources: vec![
                "execution_usable_public_api_boundary",
                "layout_row_count_approval",
            ],
            local_execution_status: VortexLocalExecutionStatus::NeedsEncodedRead,
            mode: VortexLocalExecutionMode::PlanOnly,
            layout_row_count_path_accepted: true,
            returns_count_result: false,
            tasks_executed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
        }
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.tasks_executed
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }
    pub fn accepted_approval_sources_text(&self) -> String {
        self.accepted_approval_sources.join(",")
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "Vortex encoded-count local guard discovery\nschema_version: {}\nguard_id: {}\naccepted approval sources: {}\nlocal execution status: {}\nmode: {}\nlayout row-count path accepted: {}\nreturns count result: {}\ndata read: {}\ndata decoded: {}\ndata materialized: {}\nobject-store IO: {}\nwrite IO: {}\nspill IO: {}\nfallback execution: disabled",
            self.schema_version,
            self.guard_id,
            self.accepted_approval_sources_text(),
            self.local_execution_status.as_str(),
            self.mode.as_str(),
            self.layout_row_count_path_accepted,
            self.returns_count_result,
            self.data_read,
            self.data_decoded,
            self.data_materialized,
            self.object_store_io,
            self.write_io,
            self.spill_io_performed
        )
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

/// Executes `CountAll` using the typed metadata summary carried by a metadata/footer invocation.
///
/// This bridge does not call scan/read-start APIs, read encoded data or rows,
/// decode/materialize values, convert to `Arrow`, perform object-store IO,
/// write data, or permit fallback execution.
///
/// # Errors
/// Returns an error if primitive evaluation or analysis construction fails.
pub fn execute_vortex_count_all_from_metadata_footer_invocation(
    invocation: &crate::VortexMetadataAsyncInvocationReport,
) -> Result<VortexLocalExecutionReport> {
    execute_vortex_local_query_primitive(
        VortexQueryPrimitiveRequest::count_all(
            invocation.boundary_report.request.target_uri.clone(),
        ),
        invocation.metadata_summary_report.clone(),
    )
}

/// Plans `CountAll` through an approved encoded-data candidate without reading encoded data.
///
/// The returned report is a deferred `NeedsEncodedRead` result. It does not call
/// scan/read-start APIs, traverse encoded data, read rows, decode/materialize
/// values, convert to `Arrow`, perform object-store IO, write data, or permit
/// fallback execution.
///
/// # Errors
/// Returns an error if primitive analysis construction fails.
pub fn execute_vortex_count_all_from_encoded_data_candidate(
    readiness: &VortexCountReadinessReport,
) -> Result<VortexLocalExecutionReport> {
    let request = VortexQueryPrimitiveRequest::count_all(readiness.request.target_uri.clone());
    let input = VortexLocalExecutionInput::new(request).allow_encoded_read(true);
    if readiness.status == VortexCountReadinessStatus::CountReady
        && readiness.request.candidate_source == VortexCountCandidateSource::EncodedDataPath
        && readiness.encoded_data_path_ready()
        && readiness.request.api_boundary_blockers.is_empty()
        && !readiness.has_errors()
    {
        return VortexLocalExecutionReport::from_input(input);
    }
    Ok(VortexLocalExecutionReport::unsupported(
        input,
        "vortex_count_encoded_candidate",
        "encoded-data count candidate is not ready",
    ))
}

/// Plans `CountAll` through an approved encoded-count data-path approval report.
///
/// The returned report is still a deferred `NeedsEncodedRead` result. This
/// helper exists so future encoded-count execution work has to pass the
/// explicit approval boundary before any local execution path can advance.
///
/// # Errors
/// Returns an error if primitive analysis construction fails.
pub fn execute_vortex_count_all_from_encoded_count_data_path_approval(
    approval: &VortexEncodedCountDataPathApprovalReport,
) -> Result<VortexLocalExecutionReport> {
    let request = VortexQueryPrimitiveRequest::count_all(
        approval
            .input
            .count_readiness_report
            .request
            .target_uri
            .clone(),
    );
    let input = VortexLocalExecutionInput::new(request).allow_encoded_read(true);
    if approval.approved() && approval.is_side_effect_free() && !approval.has_errors() {
        return VortexLocalExecutionReport::from_input(input);
    }
    Ok(VortexLocalExecutionReport::unsupported(
        input,
        "vortex_encoded_count_data_path_approval",
        "encoded-count data-path approval is not approved",
    ))
}

/// Executes metadata-proven `CountWhere` through the filtered-count readiness guard.
///
/// This path accepts only metadata predicate proof candidates. It may return a
/// metadata-only count result when segment metadata proves the predicate, or a
/// deferred `NeedsPredicateEvaluation` plan when metadata is inconclusive. It
/// does not read encoded data, read rows, decode/materialize values, convert to
/// `Arrow`, perform object-store IO, write data, or permit fallback execution.
///
/// # Errors
/// Returns an error if primitive analysis construction fails.
pub fn execute_vortex_count_where_from_filtered_count_metadata_proof(
    readiness: &VortexFilteredCountReadinessReport,
    request: VortexQueryPrimitiveRequest,
    metadata_summary: VortexMetadataSummaryReport,
) -> Result<VortexLocalExecutionReport> {
    let target_matches = request.source_uri.as_ref() == Some(&readiness.request.target_uri);
    let is_count_where = request.kind == VortexQueryPrimitiveKind::CountWhere;
    let input = VortexLocalExecutionInput::new(request).with_metadata_summary(metadata_summary);
    if readiness.filtered_count_ready()
        && readiness.request.candidate_source
            == VortexFilteredCountCandidateSource::MetadataPredicateProof
        && readiness.is_side_effect_free()
        && !readiness.has_errors()
        && is_count_where
        && target_matches
    {
        return VortexLocalExecutionReport::from_input(input);
    }
    Ok(VortexLocalExecutionReport::unsupported(
        input,
        "vortex_filtered_count_metadata_proof",
        "filtered-count metadata proof readiness is not approved for this CountWhere request",
    ))
}

pub fn vortex_local_execution_is_side_effect_free(report: &VortexLocalExecutionReport) -> bool {
    report.is_side_effect_free()
}

pub fn vortex_encoded_count_local_guard_discovery_report()
-> VortexEncodedCountLocalGuardDiscoveryReport {
    VortexEncodedCountLocalGuardDiscoveryReport::report_only()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexCountCandidateSource, VortexCountReadinessRequest,
        VortexEncodedCountDataPathApprovalInput, VortexEncodedReadApiBoundaryReport,
        VortexEncodedReadApiBoundaryStatus, VortexFileMetadataSummary,
        VortexFilteredCountCandidateSource, VortexFilteredCountReadinessRequest,
        VortexLayoutReaderDriverApprovalInput, VortexMetadataSummaryStatus,
        VortexSegmentMetadataSummary, plan_vortex_count_readiness,
        plan_vortex_encoded_count_data_path_approval,
        plan_vortex_encoded_count_data_path_approval_with_layout_driver,
        plan_vortex_filtered_count_readiness, plan_vortex_layout_reader_driver_approval,
        vortex_encoded_read_public_api_boundary,
    };
    use shardloom_core::{DatasetUri, PredicateExpr};
    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/a.vortex").expect("uri")
    }
    fn count_request() -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::count_all(uri())
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
    fn segmented_count_summary(rows: u64) -> VortexMetadataSummaryReport {
        let mut summary = VortexFileMetadataSummary::empty();
        summary.row_count = Some(rows);
        summary.add_segment(VortexSegmentMetadataSummary::unknown().with_row_count(rows));
        VortexMetadataSummaryReport {
            status: VortexMetadataSummaryStatus::Summarized,
            summary,
            diagnostics: vec![],
        }
    }
    fn encoded_count_ready_report() -> VortexCountReadinessReport {
        plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("readiness")
    }
    fn approved_layout_driver_report(
        api: VortexEncodedReadApiBoundaryReport,
    ) -> crate::VortexLayoutReaderDriverApprovalReport {
        plan_vortex_layout_reader_driver_approval(
            VortexLayoutReaderDriverApprovalInput::new(api)
                .local_fixture_only(true)
                .caller_session_allowed(true)
                .runtime_driver_start_allowed(true)
                .layout_row_count_only_intent(true)
                .scan_forbidden(true)
                .evaluation_forbidden(true)
                .data_read_forbidden(true)
                .decode_forbidden(true)
                .materialization_forbidden(true)
                .arrow_forbidden(true)
                .object_store_forbidden(true)
                .write_forbidden(true)
                .fallback_forbidden(true),
        )
        .expect("layout driver approval")
    }
    fn filtered_count_metadata_proof_ready_report() -> VortexFilteredCountReadinessReport {
        plan_vortex_filtered_count_readiness(
            VortexFilteredCountReadinessRequest::new(
                uri(),
                VortexFilteredCountCandidateSource::MetadataPredicateProof,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .metadata_footer_ready(true)
            .filtered_count_primitive(true)
            .predicate_provided(true)
            .predicate_metadata_proof_ready(true),
        )
        .expect("filtered count readiness")
    }
    fn filtered_count_encoded_predicate_ready_report() -> VortexFilteredCountReadinessReport {
        plan_vortex_filtered_count_readiness(
            VortexFilteredCountReadinessRequest::new(
                uri(),
                VortexFilteredCountCandidateSource::EncodedPredicatePath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .encoded_data_path_ready(true)
            .filtered_count_primitive(true)
            .predicate_provided(true),
        )
        .expect("filtered count readiness")
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

    #[test]
    fn encoded_data_candidate_defers_count_without_reading() {
        let readiness = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("readiness");

        let report =
            execute_vortex_count_all_from_encoded_data_candidate(&readiness).expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::NeedsEncodedRead);
        assert_eq!(report.mode, VortexLocalExecutionMode::PlanOnly);
        assert_eq!(report.value, VortexLocalExecutionValue::Deferred);
        assert!(report.input.allow_encoded_read);
        assert!(report.is_side_effect_free());
        assert!(!report.tasks_executed);
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn encoded_data_candidate_with_named_api_blocker_is_unsupported() {
        let mut request =
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true);
        request.add_api_boundary_blocker(
            "area=data_read name=ScanBuilder::into_array_stream status=forbidden_for_now",
        );
        let readiness = plan_vortex_count_readiness(request).expect("readiness");

        let report =
            execute_vortex_count_all_from_encoded_data_candidate(&readiness).expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn encoded_count_approval_guard_blocks_current_public_api_boundary() {
        let readiness = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("readiness");
        let approval = plan_vortex_encoded_count_data_path_approval(
            readiness,
            vortex_encoded_read_public_api_boundary(),
        )
        .expect("approval");

        let report = execute_vortex_count_all_from_encoded_count_data_path_approval(&approval)
            .expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn encoded_count_approval_guard_defers_when_future_boundary_is_approved() {
        let readiness = encoded_count_ready_report();
        let mut api = VortexEncodedReadApiBoundaryReport::default_deferred();
        api.status = VortexEncodedReadApiBoundaryStatus::ContractReady;
        api.execution_usable_count = 1;
        let approval = crate::VortexEncodedCountDataPathApprovalReport::from_input(
            VortexEncodedCountDataPathApprovalInput::new(readiness, api),
        )
        .expect("approval");
        assert!(approval.approved());

        let report = execute_vortex_count_all_from_encoded_count_data_path_approval(&approval)
            .expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::NeedsEncodedRead);
        assert_eq!(report.mode, VortexLocalExecutionMode::PlanOnly);
        assert!(report.input.allow_encoded_read);
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn encoded_count_approval_guard_defers_with_layout_row_count_approval() {
        let api = vortex_encoded_read_public_api_boundary();
        let layout = approved_layout_driver_report(api.clone());
        let approval = plan_vortex_encoded_count_data_path_approval_with_layout_driver(
            encoded_count_ready_report(),
            api,
            layout,
        )
        .expect("approval");
        assert!(approval.approved());
        assert!(approval.layout_row_count_path_approved);

        let report = execute_vortex_count_all_from_encoded_count_data_path_approval(&approval)
            .expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::NeedsEncodedRead);
        assert_eq!(report.mode, VortexLocalExecutionMode::PlanOnly);
        assert!(report.input.allow_encoded_read);
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn encoded_count_local_guard_discovery_remains_report_only() {
        let report = vortex_encoded_count_local_guard_discovery_report();

        assert_eq!(
            report.schema_version,
            "shardloom.vortex_encoded_count_local_guard.v1"
        );
        assert_eq!(
            report.local_execution_status,
            VortexLocalExecutionStatus::NeedsEncodedRead
        );
        assert_eq!(report.mode, VortexLocalExecutionMode::PlanOnly);
        assert!(report.layout_row_count_path_accepted);
        assert!(!report.returns_count_result);
        assert!(report.is_side_effect_free());
        assert!(!report.fallback_execution_allowed);
        assert_eq!(
            report.accepted_approval_sources_text(),
            "execution_usable_public_api_boundary,layout_row_count_approval"
        );
    }

    #[test]
    fn filtered_count_metadata_proof_guard_executes_metadata_count_where() {
        let request = VortexQueryPrimitiveRequest::count_where(uri(), PredicateExpr::AlwaysTrue);
        let report = execute_vortex_count_where_from_filtered_count_metadata_proof(
            &filtered_count_metadata_proof_ready_report(),
            request,
            segmented_count_summary(12),
        )
        .expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::MetadataExecuted);
        assert_eq!(report.mode, VortexLocalExecutionMode::MetadataOnly);
        assert!(report.value.is_known());
        match report.value {
            VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(v)) => {
                assert_eq!(v, 12);
            }
            other => panic!("expected count result, got {}", other.summary()),
        }
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn filtered_count_metadata_proof_guard_rejects_encoded_predicate_candidate() {
        let request = VortexQueryPrimitiveRequest::count_where(uri(), PredicateExpr::AlwaysTrue);
        let report = execute_vortex_count_where_from_filtered_count_metadata_proof(
            &filtered_count_encoded_predicate_ready_report(),
            request,
            segmented_count_summary(12),
        )
        .expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn encoded_data_candidate_helper_rejects_metadata_source() {
        let readiness = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .metadata_footer_ready(true),
        )
        .expect("readiness");

        let report =
            execute_vortex_count_all_from_encoded_data_candidate(&readiness).expect("execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn allow_encoded_read_without_count_candidate_does_not_widen_other_primitives() {
        let request =
            VortexQueryPrimitiveRequest::project(uri(), shardloom_plan::ProjectionRequest::all());
        let report = VortexLocalExecutionReport::from_input(
            VortexLocalExecutionInput::new(request).allow_encoded_read(true),
        )
        .expect("report");

        assert_eq!(report.status, VortexLocalExecutionStatus::MissingMetadata);
        assert!(report.is_side_effect_free());
    }

    #[cfg(feature = "vortex-file-io")]
    #[test]
    fn metadata_footer_invocation_executes_count_all_from_fixture_footer() {
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
        let fixture =
            crate::VortexEncodedReadFixtureRef::new(fixture_path.to_string_lossy().to_string())
                .expect("fixture ref");
        let boundary = crate::plan_vortex_metadata_async_boundary(
            crate::VortexMetadataAsyncBoundaryRequest::new(target_uri, fixture)
                .feature_gate_enabled(true)
                .local_fixture_ready(true)
                .runtime_boundary_approved(true)
                .async_session_allowed(true)
                .metadata_footer_only_intent(true),
        )
        .expect("boundary");
        assert!(boundary.boundary_ready());

        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let invocation = runtime
            .block_on(
                crate::invoke_vortex_metadata_footer_probe_with_session_async(
                    crate::VortexMetadataAsyncInvocationInput {
                        boundary,
                        session: &session,
                    },
                ),
            )
            .expect("invocation");

        let report = execute_vortex_count_all_from_metadata_footer_invocation(&invocation)
            .expect("local execution");

        assert_eq!(report.status, VortexLocalExecutionStatus::MetadataExecuted);
        assert_eq!(
            report.value,
            VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(20000))
        );
        assert!(report.value.is_known());
        assert!(report.is_side_effect_free());
        assert!(!report.tasks_executed);
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.fallback_execution_allowed);
        assert_eq!(
            report
                .input
                .metadata_summary
                .as_ref()
                .and_then(|summary| summary.summary.row_count),
            Some(20000)
        );
    }
}
