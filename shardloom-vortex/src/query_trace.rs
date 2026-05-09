#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexMetadataSummaryReport, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    VortexQueryPrimitiveResult, VortexQueryPrimitiveStatus, VortexQueryPrimitiveValue,
    evaluate_vortex_query_primitive,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryDecisionKind {
    MetadataAnswer,
    MetadataCount,
    MetadataPredicateProof,
    SegmentPruned,
    SegmentMayMatch,
    EncodedReadRequired,
    EncodedPredicateRequired,
    ProjectionRequired,
    MissingMetadata,
    MissingEstimate,
    DecodeAvoided,
    MaterializationAvoided,
    ObjectStoreAvoided,
    SpillAvoided,
    FallbackBlocked,
    Unsupported,
}
impl VortexQueryDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataAnswer => "metadata_answer",
            Self::MetadataCount => "metadata_count",
            Self::MetadataPredicateProof => "metadata_predicate_proof",
            Self::SegmentPruned => "segment_pruned",
            Self::SegmentMayMatch => "segment_may_match",
            Self::EncodedReadRequired => "encoded_read_required",
            Self::EncodedPredicateRequired => "encoded_predicate_required",
            Self::ProjectionRequired => "projection_required",
            Self::MissingMetadata => "missing_metadata",
            Self::MissingEstimate => "missing_estimate",
            Self::DecodeAvoided => "decode_avoided",
            Self::MaterializationAvoided => "materialization_avoided",
            Self::ObjectStoreAvoided => "object_store_avoided",
            Self::SpillAvoided => "spill_avoided",
            Self::FallbackBlocked => "fallback_blocked",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::MissingMetadata | Self::MissingEstimate | Self::Unsupported
        )
    }
    pub const fn is_work_avoidance(&self) -> bool {
        matches!(
            self,
            Self::DecodeAvoided
                | Self::MaterializationAvoided
                | Self::ObjectStoreAvoided
                | Self::SpillAvoided
                | Self::FallbackBlocked
                | Self::SegmentPruned
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexQueryDecisionTraceEntry {
    pub kind: VortexQueryDecisionKind,
    pub reason: String,
    pub evidence: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryDecisionTraceEntry {
    pub fn new(kind: VortexQueryDecisionKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
            evidence: None,
            diagnostics: vec![],
        }
    }
    pub fn with_evidence(mut self, evidence: impl Into<String>) -> Self {
        self.evidence = Some(evidence.into());
        self
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let mut out = Self::new(VortexQueryDecisionKind::Unsupported, reason.clone());
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Requested query primitive decision is unsupported.",
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
        let mut text = String::new();
        let _ = write!(text, "{}: {}", self.kind.as_str(), self.reason);
        if let Some(e) = &self.evidence {
            let _ = write!(text, " (evidence: {e})");
        }
        text
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexQueryDecisionTrace {
    pub entries: Vec<VortexQueryDecisionTraceEntry>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryDecisionTrace {
    pub fn empty() -> Self {
        Self {
            entries: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_entry(&mut self, entry: VortexQueryDecisionTraceEntry) {
        self.entries.push(entry);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
    pub fn blocking_count(&self) -> usize {
        self.entries.iter().filter(|e| e.kind.is_blocking()).count()
    }
    pub fn work_avoidance_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.kind.is_work_avoidance())
            .count()
    }
    pub fn has_errors(&self) -> bool {
        self.entries
            .iter()
            .any(VortexQueryDecisionTraceEntry::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "decision trace:");
        for entry in &self.entries {
            let _ = writeln!(text, "- {}", entry.summary());
            for d in &entry.diagnostics {
                let _ = writeln!(text, "  diagnostic: {} [{}]", d.message, d.code.as_str());
            }
        }
        for d in &self.diagnostics {
            let _ = writeln!(text, "diagnostic: {} [{}]", d.message, d.code.as_str());
        }
        text
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexWorkAvoidedMetricKind {
    SegmentsPruned,
    RowsNotScanned,
    BytesNotRead,
    ColumnsNotLoaded,
    DecodeAvoided,
    MaterializationAvoided,
    ObjectStoreRequestsAvoided,
    SpillAvoided,
    FallbackBlocked,
    Unknown,
}
impl VortexWorkAvoidedMetricKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SegmentsPruned => "segments_pruned",
            Self::RowsNotScanned => "rows_not_scanned",
            Self::BytesNotRead => "bytes_not_read",
            Self::ColumnsNotLoaded => "columns_not_loaded",
            Self::DecodeAvoided => "decode_avoided",
            Self::MaterializationAvoided => "materialization_avoided",
            Self::ObjectStoreRequestsAvoided => "object_store_requests_avoided",
            Self::SpillAvoided => "spill_avoided",
            Self::FallbackBlocked => "fallback_blocked",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_known_when_zero(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexWorkAvoidedValue {
    KnownU64(u64),
    KnownBool(bool),
    Unknown,
}
impl VortexWorkAvoidedValue {
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
    pub const fn as_u64(&self) -> Option<u64> {
        if let Self::KnownU64(v) = self {
            Some(*v)
        } else {
            None
        }
    }
    pub const fn as_bool(&self) -> Option<bool> {
        if let Self::KnownBool(v) = self {
            Some(*v)
        } else {
            None
        }
    }
    pub fn summary(&self) -> String {
        match self {
            Self::KnownU64(v) => v.to_string(),
            Self::KnownBool(v) => v.to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexWorkAvoidedMetric {
    pub kind: VortexWorkAvoidedMetricKind,
    pub value: VortexWorkAvoidedValue,
    pub reason: String,
}
impl VortexWorkAvoidedMetric {
    pub fn known_u64(
        kind: VortexWorkAvoidedMetricKind,
        value: u64,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            value: VortexWorkAvoidedValue::KnownU64(value),
            reason: reason.into(),
        }
    }
    pub fn known_bool(
        kind: VortexWorkAvoidedMetricKind,
        value: bool,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            value: VortexWorkAvoidedValue::KnownBool(value),
            reason: reason.into(),
        }
    }
    pub fn unknown(kind: VortexWorkAvoidedMetricKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            value: VortexWorkAvoidedValue::Unknown,
            reason: reason.into(),
        }
    }
    pub fn is_known(&self) -> bool {
        self.value.is_known()
    }
    pub fn summary(&self) -> String {
        format!(
            "{}={} ({})",
            self.kind.as_str(),
            self.value.summary(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexWorkAvoidedReport {
    pub metrics: Vec<VortexWorkAvoidedMetric>,
    pub diagnostics: Vec<Diagnostic>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
}
impl VortexWorkAvoidedReport {
    pub fn empty() -> Self {
        Self {
            metrics: vec![],
            diagnostics: vec![],
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
        }
    }
    pub fn add_metric(&mut self, metric: VortexWorkAvoidedMetric) {
        self.metrics.push(metric);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn metric_count(&self) -> usize {
        self.metrics.len()
    }
    pub fn metric(&self, kind: VortexWorkAvoidedMetricKind) -> Option<&VortexWorkAvoidedMetric> {
        self.metrics.iter().find(|metric| metric.kind == kind)
    }
    pub fn metric_value_summary(&self, kind: VortexWorkAvoidedMetricKind) -> String {
        self.metric(kind)
            .map_or_else(|| "none".to_string(), |metric| metric.value.summary())
    }
    pub fn metric_known_summary(&self, kind: VortexWorkAvoidedMetricKind) -> String {
        self.metric(kind)
            .is_some_and(VortexWorkAvoidedMetric::is_known)
            .to_string()
    }
    pub fn metric_reason_summary(&self, kind: VortexWorkAvoidedMetricKind) -> String {
        self.metric(kind)
            .map_or_else(|| "none".to_string(), |metric| metric.reason.clone())
    }
    pub fn known_metric_count(&self) -> usize {
        self.metrics.iter().filter(|m| m.is_known()).count()
    }
    pub fn unknown_metric_count(&self) -> usize {
        self.metrics.iter().filter(|m| !m.is_known()).count()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "work avoided:");
        for m in &self.metrics {
            let _ = writeln!(text, "- {}", m.summary());
        }
        let _ = writeln!(
            text,
            "fallback execution disabled: {}",
            !self.fallback_execution_allowed
        );
        text
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexQueryPrimitiveAnalysisReport {
    pub result: VortexQueryPrimitiveResult,
    pub decision_trace: VortexQueryDecisionTrace,
    pub work_avoided: VortexWorkAvoidedReport,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveAnalysisReport {
    pub fn from_result(result: VortexQueryPrimitiveResult) -> Self {
        Self {
            result,
            decision_trace: VortexQueryDecisionTrace::empty(),
            work_avoided: VortexWorkAvoidedReport::empty(),
            diagnostics: vec![],
        }
    }
    pub fn with_trace(mut self, decision_trace: VortexQueryDecisionTrace) -> Self {
        self.decision_trace = decision_trace;
        self
    }
    pub fn with_work_avoided(mut self, work_avoided: VortexWorkAvoidedReport) -> Self {
        self.work_avoided = work_avoided;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.result.has_errors()
            || self.decision_trace.has_errors()
            || self.work_avoided.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn is_side_effect_free(&self) -> bool {
        self.result.is_side_effect_free() && self.work_avoided.is_side_effect_free()
    }
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(
            text,
            "query primitive result:\n{}",
            self.result.to_human_text()
        );
        let _ = writeln!(text, "{}", self.decision_trace.to_human_text());
        let _ = writeln!(text, "{}", self.work_avoided.to_human_text());
        text
    }
}

#[allow(clippy::too_many_lines)]
pub fn analyze_vortex_query_primitive_result(
    result: VortexQueryPrimitiveResult,
) -> VortexQueryPrimitiveAnalysisReport {
    let mut trace = VortexQueryDecisionTrace::empty();
    match result.status {
        VortexQueryPrimitiveStatus::MetadataAnswered => {
            trace.add_entry(VortexQueryDecisionTraceEntry::new(
                VortexQueryDecisionKind::MetadataAnswer,
                "answered from metadata",
            ));
            if matches!(
                result.request.kind,
                VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere
            ) {
                trace.add_entry(VortexQueryDecisionTraceEntry::new(
                    VortexQueryDecisionKind::MetadataCount,
                    "count answer proven by metadata",
                ));
            }
        }
        VortexQueryPrimitiveStatus::NeedsEncodedRead => {
            trace.add_entry(VortexQueryDecisionTraceEntry::new(
                VortexQueryDecisionKind::EncodedReadRequired,
                "encoded read planning required",
            ));
        }
        VortexQueryPrimitiveStatus::NeedsEncodedPredicate => {
            trace.add_entry(VortexQueryDecisionTraceEntry::new(
                VortexQueryDecisionKind::EncodedPredicateRequired,
                "encoded predicate planning required",
            ));
        }
        VortexQueryPrimitiveStatus::NeedsProjection => {
            trace.add_entry(VortexQueryDecisionTraceEntry::new(
                VortexQueryDecisionKind::ProjectionRequired,
                "projection planning required",
            ));
        }
        VortexQueryPrimitiveStatus::MissingMetadata => {
            trace.add_entry(VortexQueryDecisionTraceEntry::new(
                VortexQueryDecisionKind::MissingMetadata,
                "metadata is insufficient",
            ));
        }
        VortexQueryPrimitiveStatus::Unsupported => {
            trace.add_entry(VortexQueryDecisionTraceEntry::unsupported(
                result.request.kind.as_str(),
                "unsupported query primitive status",
            ));
        }
        _ => {}
    }
    if !result.data_decoded {
        trace.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::DecodeAvoided,
            "no decode performed",
        ));
    }
    if !result.data_materialized {
        trace.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::MaterializationAvoided,
            "no materialization performed",
        ));
    }
    if !result.object_store_io {
        trace.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::ObjectStoreAvoided,
            "no object-store IO performed",
        ));
    }
    if !result.spill_io_performed {
        trace.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::SpillAvoided,
            "no spill IO performed",
        ));
    }
    if !result.fallback_execution_allowed {
        trace.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::FallbackBlocked,
            "fallback execution disabled by policy",
        ));
    }

    let mut work = VortexWorkAvoidedReport::empty();
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::DecodeAvoided,
        !result.data_decoded,
        "result indicates decode avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::MaterializationAvoided,
        !result.data_materialized,
        "result indicates materialization avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::ObjectStoreRequestsAvoided,
        !result.object_store_io,
        "result indicates object-store requests avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::SpillAvoided,
        !result.spill_io_performed,
        "result indicates spill avoided",
    ));
    work.add_metric(VortexWorkAvoidedMetric::known_bool(
        VortexWorkAvoidedMetricKind::FallbackBlocked,
        !result.fallback_execution_allowed,
        "fallback remains disabled",
    ));
    if matches!(result.status, VortexQueryPrimitiveStatus::MetadataAnswered)
        && matches!(result.request.kind, VortexQueryPrimitiveKind::CountAll)
    {
        if let VortexQueryPrimitiveValue::Count(v) = result.value {
            work.add_metric(VortexWorkAvoidedMetric::known_u64(
                VortexWorkAvoidedMetricKind::RowsNotScanned,
                v,
                "metadata count avoided row scans",
            ));
        }
    } else if matches!(result.request.kind, VortexQueryPrimitiveKind::CountWhere) {
        work.add_metric(VortexWorkAvoidedMetric::unknown(
            VortexWorkAvoidedMetricKind::RowsNotScanned,
            "count-where rows-not-scanned requires scanned-input cardinality metadata",
        ));
    }
    work.add_metric(VortexWorkAvoidedMetric::unknown(
        VortexWorkAvoidedMetricKind::SegmentsPruned,
        "segment prune count unavailable from current result",
    ));
    work.add_metric(VortexWorkAvoidedMetric::unknown(
        VortexWorkAvoidedMetricKind::BytesNotRead,
        "bytes not read are unknown without safe estimate",
    ));

    VortexQueryPrimitiveAnalysisReport::from_result(result)
        .with_trace(trace)
        .with_work_avoided(work)
}

/// Evaluates and analyzes a `Vortex` query primitive without performing data execution.
/// # Errors
/// Returns an error if primitive evaluation fails.
pub fn evaluate_vortex_query_primitive_with_analysis(
    request: VortexQueryPrimitiveRequest,
    summary: &VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveAnalysisReport> {
    let result = evaluate_vortex_query_primitive(request, summary)?;
    Ok(analyze_vortex_query_primitive_result(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::DatasetUri;
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/test.vortex").expect("uri")
    }
    #[test]
    fn decode_avoided_is_work_avoidance() {
        assert!(VortexQueryDecisionKind::DecodeAvoided.is_work_avoidance());
    }
    #[test]
    fn missing_metadata_is_blocking() {
        assert!(VortexQueryDecisionKind::MissingMetadata.is_blocking());
    }
    #[test]
    fn unsupported_entry_has_error_and_no_fallback() {
        let entry = VortexQueryDecisionTraceEntry::unsupported("feature", "reason");
        assert!(entry.has_errors());
        assert!(entry.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
    #[test]
    fn trace_counts() {
        let mut t = VortexQueryDecisionTrace::empty();
        t.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::MissingMetadata,
            "x",
        ));
        t.add_entry(VortexQueryDecisionTraceEntry::new(
            VortexQueryDecisionKind::DecodeAvoided,
            "x",
        ));
        assert_eq!(t.blocking_count(), 1);
        assert_eq!(t.work_avoidance_count(), 1);
    }
    #[test]
    fn known_u64_known() {
        assert!(VortexWorkAvoidedValue::KnownU64(1).is_known());
    }
    #[test]
    fn unknown_not_known() {
        assert!(!VortexWorkAvoidedValue::Unknown.is_known());
    }
    #[test]
    fn empty_report_side_effect_free() {
        assert!(VortexWorkAvoidedReport::empty().is_side_effect_free());
    }
    #[test]
    fn decode_metric_known() {
        let mut r = VortexWorkAvoidedReport::empty();
        r.add_metric(VortexWorkAvoidedMetric::known_bool(
            VortexWorkAvoidedMetricKind::DecodeAvoided,
            true,
            "x",
        ));
        assert_eq!(r.known_metric_count(), 1);
    }
    #[test]
    fn metric_lookup_summaries_are_stable_for_known_unknown_and_missing() {
        let mut r = VortexWorkAvoidedReport::empty();
        r.add_metric(VortexWorkAvoidedMetric::known_bool(
            VortexWorkAvoidedMetricKind::DecodeAvoided,
            true,
            "decode skipped",
        ));
        r.add_metric(VortexWorkAvoidedMetric::unknown(
            VortexWorkAvoidedMetricKind::BytesNotRead,
            "not safely estimated",
        ));

        assert_eq!(
            r.metric_value_summary(VortexWorkAvoidedMetricKind::DecodeAvoided),
            "true"
        );
        assert_eq!(
            r.metric_known_summary(VortexWorkAvoidedMetricKind::BytesNotRead),
            "false"
        );
        assert_eq!(
            r.metric_reason_summary(VortexWorkAvoidedMetricKind::BytesNotRead),
            "not safely estimated"
        );
        assert_eq!(
            r.metric_value_summary(VortexWorkAvoidedMetricKind::RowsNotScanned),
            "none"
        );
    }
    #[test]
    fn human_text_mentions_fallback_disabled() {
        assert!(
            VortexWorkAvoidedReport::empty()
                .to_human_text()
                .contains("fallback execution disabled")
        );
    }
    #[test]
    fn analyze_metadata_count_adds_entries_and_rows_not_scanned() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            crate::VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(10),
        );
        let report = analyze_vortex_query_primitive_result(result);
        assert!(
            report
                .decision_trace
                .entries
                .iter()
                .any(|e| e.kind == VortexQueryDecisionKind::MetadataAnswer)
        );
        assert!(
            report
                .decision_trace
                .entries
                .iter()
                .any(|e| e.kind == VortexQueryDecisionKind::MetadataCount)
        );
        assert!(
            report
                .work_avoided
                .metrics
                .iter()
                .any(|m| m.kind == VortexWorkAvoidedMetricKind::RowsNotScanned && m.is_known())
        );
    }
    #[test]
    fn analyze_needs_encoded_read_adds_entry() {
        let result = VortexQueryPrimitiveResult::needs_encoded_read(
            crate::VortexQueryPrimitiveRequest::project(
                uri(),
                shardloom_plan::ProjectionRequest::all(),
            ),
            "x",
        );
        let report = analyze_vortex_query_primitive_result(result);
        assert!(
            report
                .decision_trace
                .entries
                .iter()
                .any(|e| e.kind == VortexQueryDecisionKind::EncodedReadRequired)
        );
    }
    #[test]
    fn bytes_not_read_not_guessed() {
        let result = VortexQueryPrimitiveResult::needs_encoded_read(
            crate::VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
        );
        let report = analyze_vortex_query_primitive_result(result);
        assert!(
            report
                .work_avoided
                .metrics
                .iter()
                .any(|m| m.kind == VortexWorkAvoidedMetricKind::BytesNotRead && !m.is_known())
        );
    }
    #[test]
    fn evaluate_with_analysis_side_effect_free() {
        let req = crate::VortexQueryPrimitiveRequest::count_all(uri());
        let summary = crate::VortexMetadataSummaryReport::unsupported("x", "y");
        let report = evaluate_vortex_query_primitive_with_analysis(req, &summary).expect("ok");
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn analysis_human_text_sections_and_diags() {
        let mut result = VortexQueryPrimitiveResult::needs_encoded_read(
            crate::VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
        );
        result.add_diagnostic(Diagnostic::no_fallback_execution("no fallback"));
        let report = analyze_vortex_query_primitive_result(result);
        let text = report.to_human_text();
        assert!(text.contains("decision trace"));
        assert!(text.contains("work avoided"));
        assert!(text.contains("diagnostics"));
    }
}
