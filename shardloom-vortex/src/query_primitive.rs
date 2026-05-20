use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, PredicateExpr, Result,
};
use shardloom_plan::ProjectionRequest;

/// Query primitive kind for minimal `Vortex` planning in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveKind {
    CountAll,
    CountWhere,
    ProjectColumns,
    FilterPredicate,
    FilterAndProject,
    SimpleAggregate,
    Unsupported,
}
impl VortexQueryPrimitiveKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CountAll => "count_all",
            Self::CountWhere => "count_where",
            Self::ProjectColumns => "project_columns",
            Self::FilterPredicate => "filter_predicate",
            Self::FilterAndProject => "filter_and_project",
            Self::SimpleAggregate => "simple_aggregate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_data_read(&self) -> bool {
        !matches!(self, Self::CountAll | Self::CountWhere | Self::Unsupported)
    }
    #[must_use]
    pub const fn requires_decode(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn requires_materialization(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveMode {
    MetadataOnly,
    EncodedReadRequired,
    Deferred,
    Unsupported,
}
impl VortexQueryPrimitiveMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedReadRequired => "encoded_read_required",
            Self::Deferred => "deferred",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn reads_data(&self) -> bool {
        matches!(self, Self::EncodedReadRequired)
    }
    #[must_use]
    pub const fn decodes_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn materializes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveStatus {
    Planned,
    MetadataAnswered,
    NeedsEncodedRead,
    NeedsEncodedPredicate,
    NeedsProjection,
    MissingMetadata,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    Unsupported,
}
impl VortexQueryPrimitiveStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataAnswered => "metadata_answered",
            Self::NeedsEncodedRead => "needs_encoded_read",
            Self::NeedsEncodedPredicate => "needs_encoded_predicate",
            Self::NeedsProjection => "needs_projection",
            Self::MissingMetadata => "missing_metadata",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn has_result(&self) -> bool {
        matches!(self, Self::MetadataAnswered)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexQueryPrimitiveRequest {
    pub kind: VortexQueryPrimitiveKind,
    pub source_uri: Option<DatasetUri>,
    pub projection: ProjectionRequest,
    pub predicate: Option<PredicateExpr>,
    pub source_order_limit: Option<usize>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveRequest {
    #[must_use]
    pub fn count_all(uri: DatasetUri) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::CountAll,
            source_uri: Some(uri),
            projection: ProjectionRequest::all(),
            predicate: None,
            source_order_limit: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn count_where(uri: DatasetUri, predicate: PredicateExpr) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::CountWhere,
            source_uri: Some(uri),
            projection: ProjectionRequest::all(),
            predicate: Some(predicate),
            source_order_limit: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn project(uri: DatasetUri, projection: ProjectionRequest) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::ProjectColumns,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn filter(uri: DatasetUri, predicate: PredicateExpr) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::FilterPredicate,
            source_uri: Some(uri),
            projection: ProjectionRequest::all(),
            predicate: Some(predicate),
            source_order_limit: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn filter_and_project(
        uri: DatasetUri,
        predicate: PredicateExpr,
        projection: ProjectionRequest,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::FilterAndProject,
            source_uri: Some(uri),
            projection,
            predicate: Some(predicate),
            source_order_limit: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_source_order_limit(mut self, limit: usize) -> Self {
        self.source_order_limit = Some(limit);
        self
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut request = Self {
            kind: VortexQueryPrimitiveKind::Unsupported,
            source_uri: None,
            projection: ProjectionRequest::all(),
            predicate: None,
            source_order_limit: None,
            diagnostics: vec![],
        };
        request.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature.into(),
            "Requested query primitive is not supported for native `Vortex` execution.",
            Some(reason.into()),
        ));
        request
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
        format!(
            "kind={} uri={} projection={} predicate={} source_order_limit={} diagnostics={}",
            self.kind.as_str(),
            self.source_uri
                .as_ref()
                .map_or("<none>", DatasetUri::as_str),
            self.projection.summary(),
            self.predicate
                .as_ref()
                .map_or_else(|| "none".to_string(), PredicateExpr::summary),
            self.source_order_limit
                .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexQueryPrimitiveValue {
    Count(u64),
    Boolean(bool),
    Text(String),
    Unknown,
}
impl VortexQueryPrimitiveValue {
    #[must_use]
    pub fn as_str(&self) -> String {
        match self {
            Self::Count(v) => v.to_string(),
            Self::Boolean(v) => v.to_string(),
            Self::Text(v) => v.clone(),
            Self::Unknown => "unknown".to_string(),
        }
    }
    #[must_use]
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexQueryPrimitiveResult {
    pub status: VortexQueryPrimitiveStatus,
    pub mode: VortexQueryPrimitiveMode,
    pub request: VortexQueryPrimitiveRequest,
    pub value: VortexQueryPrimitiveValue,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveResult {
    #[must_use]
    pub fn to_analysis_report(self) -> crate::VortexQueryPrimitiveAnalysisReport {
        crate::analyze_vortex_query_primitive_result(self)
    }
    #[must_use]
    pub fn metadata_answered(
        request: VortexQueryPrimitiveRequest,
        value: VortexQueryPrimitiveValue,
    ) -> Self {
        Self {
            status: VortexQueryPrimitiveStatus::MetadataAnswered,
            mode: VortexQueryPrimitiveMode::MetadataOnly,
            request,
            value,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn needs_encoded_read(
        request: VortexQueryPrimitiveRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexQueryPrimitiveStatus::NeedsEncodedRead,
            mode: VortexQueryPrimitiveMode::EncodedReadRequired,
            request,
            value: VortexQueryPrimitiveValue::Unknown,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Warning,
            shardloom_core::DiagnosticCategory::Execution,
            "Encoded read is required for this primitive.",
            Some("vortex_query_primitive".to_string()),
            Some(reason.into()),
            Some(
                "Use metadata-only `CountAll` or wait for native encoded-read execution support."
                    .to_string(),
            ),
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
        out
    }
    #[must_use]
    pub fn missing_metadata(
        request: VortexQueryPrimitiveRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self::needs_encoded_read(request, reason);
        out.status = VortexQueryPrimitiveStatus::MissingMetadata;
        out.mode = VortexQueryPrimitiveMode::Deferred;
        out
    }
    #[must_use]
    pub fn unsupported(
        request: VortexQueryPrimitiveRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexQueryPrimitiveStatus::Unsupported,
            mode: VortexQueryPrimitiveMode::Unsupported,
            request,
            value: VortexQueryPrimitiveValue::Unknown,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature.into(),
            "Requested query primitive is unsupported for native execution.",
            Some(reason.into()),
        ));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "primitive: {}", self.request.kind.as_str());
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(text, "mode: {}", self.mode.as_str());
        if self.value.is_known() {
            let _ = writeln!(text, "value: {}", self.value.as_str());
        }
        let _ = writeln!(text, "data read: {}", self.data_read);
        let _ = writeln!(text, "data decoded: {}", self.data_decoded);
        let _ = writeln!(text, "data materialized: {}", self.data_materialized);
        let _ = writeln!(text, "object-store io: {}", self.object_store_io);
        let _ = writeln!(text, "write io: {}", self.write_io);
        let _ = writeln!(text, "spill io: {}", self.spill_io_performed);
        let _ = writeln!(
            text,
            "fallback execution disabled: {}",
            !self.fallback_execution_allowed
        );
        if !self.diagnostics.is_empty() {
            let _ = writeln!(text, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(text, "- {} [{}]", d.message, d.code.as_str());
            }
        }
        text
    }
}

/// Evaluates metadata-only `CountAll` using a `VortexMetadataSummaryReport`.
/// # Errors
/// Returns an error only if `ShardLoom` detects an internal overflow conversion issue.
pub fn evaluate_vortex_count_all_from_summary(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveResult> {
    if request.kind != VortexQueryPrimitiveKind::CountAll {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "count_all",
            "Only `CountAll` is supported by metadata-count evaluation.",
        ));
    }
    if let Some(v) = summary.summary.row_count {
        return Ok(VortexQueryPrimitiveResult::metadata_answered(
            request,
            VortexQueryPrimitiveValue::Count(v),
        ));
    }
    if summary.summary.segments.is_empty() {
        return Ok(VortexQueryPrimitiveResult::missing_metadata(
            request,
            "no segment metadata available for CountAll evaluation",
        ));
    }
    let mut total = 0_u64;
    let mut any = false;
    for seg in &summary.summary.segments {
        let Some(rows) = seg.row_count else {
            return Ok(VortexQueryPrimitiveResult::missing_metadata(
                request,
                "segment row_count is missing",
            ));
        };
        total = total.checked_add(rows).ok_or_else(|| {
            shardloom_core::ShardLoomError::InvalidOperation(
                "row count overflow while summing segment metadata".to_string(),
            )
        })?;
        any = true;
    }
    if any {
        Ok(VortexQueryPrimitiveResult::metadata_answered(
            request,
            VortexQueryPrimitiveValue::Count(total),
        ))
    } else {
        Ok(VortexQueryPrimitiveResult::missing_metadata(
            request,
            "file and segment row_count metadata are unavailable",
        ))
    }
}

/// Evaluates metadata-only `CountWhere` using a `VortexMetadataSummaryReport`.
///
/// # Errors
/// Returns an error only if `ShardLoom` detects an internal overflow conversion issue.
pub fn evaluate_vortex_count_where_from_summary(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveResult> {
    if request.kind != VortexQueryPrimitiveKind::CountWhere {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "count_where",
            "Only `CountWhere` is supported by metadata-filtered count evaluation.",
        ));
    }
    let Some(predicate) = request.predicate.as_ref() else {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "count_where",
            "missing `PredicateExpr` for `CountWhere` request",
        ));
    };
    if summary.summary.segments.is_empty() {
        return Ok(VortexQueryPrimitiveResult::missing_metadata(
            request,
            "no segment metadata available for CountWhere evaluation",
        ));
    }
    let mut total = 0_u64;
    for seg in &summary.summary.segments {
        match crate::prove_predicate_from_segment_stats(predicate, seg) {
            shardloom_core::PredicateProof::AlwaysFalse { .. } => {}
            shardloom_core::PredicateProof::AlwaysTrue { .. } => {
                let Some(rows) = seg.row_count else {
                    return Ok(VortexQueryPrimitiveResult::missing_metadata(
                        request,
                        "segment row_count is required for metadata-proven true predicate",
                    ));
                };
                total = total.checked_add(rows).ok_or_else(|| {
                    shardloom_core::ShardLoomError::InvalidOperation(
                        "row count overflow while summing metadata-filtered count".to_string(),
                    )
                })?;
            }
            shardloom_core::PredicateProof::MayMatch { reason }
            | shardloom_core::PredicateProof::Unknown { reason } => {
                let mut out = VortexQueryPrimitiveResult::needs_encoded_read(request, reason);
                out.status = VortexQueryPrimitiveStatus::NeedsEncodedPredicate;
                out.mode = VortexQueryPrimitiveMode::Deferred;
                return Ok(out);
            }
            shardloom_core::PredicateProof::Unsupported { reason } => {
                return Ok(VortexQueryPrimitiveResult::unsupported(
                    request,
                    "count_where",
                    reason,
                ));
            }
        }
    }
    Ok(VortexQueryPrimitiveResult::metadata_answered(
        request,
        VortexQueryPrimitiveValue::Count(total),
    ))
}

/// Plans encoded projection intent for `ProjectColumns`/`FilterAndProject`.
/// # Errors
/// Returns an error only if `ShardLoom` observes malformed internal metadata state.
pub fn plan_vortex_encoded_projection(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
    _probe_report: Option<&crate::VortexEncodedReadProbeReport>,
) -> Result<VortexQueryPrimitiveResult> {
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::ProjectColumns | VortexQueryPrimitiveKind::FilterAndProject
    ) {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "encoded_projection",
            "only `ProjectColumns` and `FilterAndProject` can plan encoded projection",
        ));
    }
    if request.projection.is_all() {
        return Ok(VortexQueryPrimitiveResult::needs_encoded_read(
            request,
            "projection=all requires encoded-read candidate planning",
        ));
    }
    let known_columns: std::collections::BTreeSet<&str> = summary
        .summary
        .segments
        .iter()
        .flat_map(|segment| segment.columns.iter())
        .filter_map(|column| {
            column
                .column
                .as_ref()
                .map(shardloom_core::ColumnRef::as_str)
        })
        .collect();
    if let ProjectionRequest::Columns(columns) = &request.projection {
        let missing: Vec<&str> = columns
            .iter()
            .map(shardloom_core::ColumnRef::as_str)
            .filter(|name| !known_columns.contains(*name))
            .collect();
        if !missing.is_empty() {
            let missing_text = missing.join(",");
            return Ok(VortexQueryPrimitiveResult::missing_metadata(
                request,
                format!("projection columns missing from metadata summary: {missing_text}"),
            ));
        }
    }
    let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
        request,
        "projection columns are metadata-known; encoded projection may be possible",
    );
    out.status = VortexQueryPrimitiveStatus::NeedsProjection;
    Ok(out)
}

/// Plans encoded predicate intent for `FilterPredicate`/`FilterAndProject`.
/// # Errors
/// Returns an error only if `ShardLoom` detects internal overflow while deriving metadata answers.
pub fn plan_vortex_encoded_predicate(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
    _probe_report: Option<&crate::VortexEncodedReadProbeReport>,
) -> Result<VortexQueryPrimitiveResult> {
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::FilterPredicate | VortexQueryPrimitiveKind::FilterAndProject
    ) {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "encoded_predicate",
            "only `FilterPredicate` and `FilterAndProject` can plan encoded predicate",
        ));
    }
    let Some(predicate) = request.predicate.as_ref() else {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "encoded_predicate",
            "missing `PredicateExpr` for filter request",
        ));
    };
    let mut saw_segment = false;
    let mut saw_inconclusive = false;
    for segment in &summary.summary.segments {
        saw_segment = true;
        match crate::prove_predicate_from_segment_stats(predicate, segment) {
            shardloom_core::PredicateProof::AlwaysFalse { .. } => {}
            shardloom_core::PredicateProof::AlwaysTrue { .. }
            | shardloom_core::PredicateProof::MayMatch { .. }
            | shardloom_core::PredicateProof::Unknown { .. } => saw_inconclusive = true,
            shardloom_core::PredicateProof::Unsupported { reason } => {
                return Ok(VortexQueryPrimitiveResult::unsupported(
                    request,
                    "encoded_predicate",
                    reason,
                ));
            }
        }
    }
    if saw_segment && !saw_inconclusive {
        return Ok(VortexQueryPrimitiveResult::metadata_answered(
            request,
            VortexQueryPrimitiveValue::Boolean(false),
        ));
    }
    let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
        request,
        "metadata proof is inconclusive; encoded predicate planning is required",
    );
    out.status = VortexQueryPrimitiveStatus::NeedsEncodedPredicate;
    out.mode = VortexQueryPrimitiveMode::Deferred;
    Ok(out)
}

/// Evaluates a minimal `Vortex` query primitive against metadata summary.
/// # Errors
/// Returns an error only if metadata count evaluation overflows while summing rows.
pub fn evaluate_vortex_query_primitive(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveResult> {
    match request.kind {
        VortexQueryPrimitiveKind::CountAll => {
            evaluate_vortex_count_all_from_summary(request, summary)
        }
        VortexQueryPrimitiveKind::CountWhere => {
            evaluate_vortex_count_where_from_summary(request, summary)
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            plan_vortex_encoded_projection(request, summary, None)
        }
        VortexQueryPrimitiveKind::FilterPredicate => {
            plan_vortex_encoded_predicate(request, summary, None)
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            let predicate_result = plan_vortex_encoded_predicate(request.clone(), summary, None)?;
            if predicate_result.has_errors()
                || matches!(
                    predicate_result.status,
                    VortexQueryPrimitiveStatus::MetadataAnswered
                        | VortexQueryPrimitiveStatus::MissingMetadata
                )
            {
                Ok(predicate_result)
            } else {
                plan_vortex_encoded_projection(request, summary, None)
            }
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            Ok(VortexQueryPrimitiveResult::unsupported(
                request,
                "simple_aggregate",
                "Only metadata `CountAll` is supported in this phase.",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{ColumnRef, DatasetUri, SegmentId, SegmentStats};
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/test.vortex").expect("uri")
    }
    fn empty_summary() -> crate::VortexMetadataSummaryReport {
        crate::VortexMetadataSummaryReport::unsupported("metadata_summary", "test fallback summary")
    }
    #[test]
    fn countall_no_read() {
        assert!(!VortexQueryPrimitiveKind::CountAll.requires_data_read());
    }
    #[test]
    fn countwhere_no_read() {
        assert!(!VortexQueryPrimitiveKind::CountWhere.requires_data_read());
    }
    #[test]
    fn project_may_read() {
        assert!(VortexQueryPrimitiveKind::ProjectColumns.requires_data_read());
    }
    #[test]
    fn metadata_mode_flags_false() {
        let m = VortexQueryPrimitiveMode::MetadataOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data());
    }
    #[test]
    fn status_meta_has_result() {
        assert!(VortexQueryPrimitiveStatus::MetadataAnswered.has_result());
    }
    #[test]
    fn unsupported_error() {
        assert!(VortexQueryPrimitiveStatus::Unsupported.is_error());
    }
    #[test]
    fn count_known() {
        assert!(VortexQueryPrimitiveValue::Count(7).is_known());
    }
    #[test]
    fn metadata_answer_side_effect_false() {
        let r = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(1),
        );
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn needs_read_side_effect_false() {
        let r = VortexQueryPrimitiveResult::needs_encoded_read(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
        );
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn unsupported_errors_no_fallback() {
        let r = VortexQueryPrimitiveResult::unsupported(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
            "y",
        );
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn eval_count_file_row_count() {
        let mut s = empty_summary();
        s.summary.row_count = Some(11);
        let out = evaluate_vortex_count_all_from_summary(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &s,
        )
        .expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(11));
    }
    #[test]
    fn eval_count_segments_sum() {
        let mut s = empty_summary();
        s.summary.segments = vec![
            crate::VortexSegmentMetadataSummary::unknown()
                .with_segment_id(SegmentId::new("s1").expect("id"))
                .with_row_count(2),
            crate::VortexSegmentMetadataSummary::unknown()
                .with_segment_id(SegmentId::new("s2").expect("id"))
                .with_row_count(3),
        ];
        let out = evaluate_vortex_count_all_from_summary(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &s,
        )
        .expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(5));
    }
    #[test]
    fn eval_count_missing_metadata() {
        let s = empty_summary();
        let out = evaluate_vortex_count_all_from_summary(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &s,
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::MissingMetadata);
    }
    #[test]
    fn eval_project_needs_read() {
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::project(uri(), ProjectionRequest::all()),
            &empty_summary(),
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::NeedsEncodedRead);
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_project_known_columns_needs_projection() {
        let mut s = empty_summary();
        let mut seg = crate::VortexSegmentMetadataSummary::unknown();
        seg.add_column(crate::VortexColumnMetadataSummary::new(
            ColumnRef::new("col1").expect("column"),
        ));
        s.summary.segments.push(seg);
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::project(
                uri(),
                ProjectionRequest::columns(vec![ColumnRef::new("col1").expect("column")]),
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::NeedsProjection);
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_filter_inconclusive_needs_encoded_predicate() {
        let mut s = empty_summary();
        s.summary
            .segments
            .push(crate::VortexSegmentMetadataSummary::unknown());
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::Compare {
                    column: ColumnRef::new("x").expect("column"),
                    op: shardloom_core::ComparisonOp::Eq,
                    value: shardloom_core::StatValue::Int64(7),
                },
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexQueryPrimitiveStatus::NeedsEncodedPredicate
        );
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_filter_without_segment_stats_needs_encoded_predicate() {
        let s = empty_summary();
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::Compare {
                    column: ColumnRef::new("x").expect("column"),
                    op: shardloom_core::ComparisonOp::Eq,
                    value: shardloom_core::StatValue::Int64(7),
                },
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexQueryPrimitiveStatus::NeedsEncodedPredicate
        );
        assert_eq!(out.value, VortexQueryPrimitiveValue::Unknown);
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_filter_all_false_metadata_answered() {
        let mut s = empty_summary();
        let mut stats = SegmentStats::unknown();
        stats.null_count = Some(0);
        s.summary.segments = vec![seg_with_stats(Some(4), stats)];
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::IsNull {
                    column: ColumnRef::new("x").expect("column"),
                },
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::MetadataAnswered);
        assert_eq!(out.value, VortexQueryPrimitiveValue::Boolean(false));
    }
    #[test]
    fn count_where_request_stores_predicate() {
        let req = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNull {
                column: ColumnRef::new("x").expect("column"),
            },
        );
        assert_eq!(req.kind, VortexQueryPrimitiveKind::CountWhere);
        assert!(req.predicate.is_some());
    }
    fn seg_with_stats(
        row_count: Option<u64>,
        stats: SegmentStats,
    ) -> crate::VortexSegmentMetadataSummary {
        let mut s = crate::VortexSegmentMetadataSummary::unknown();
        s.row_count = row_count;
        let mut c = crate::VortexColumnMetadataSummary::new(ColumnRef::new("x").expect("column"));
        c.stats = stats;
        s.add_column(c);
        s
    }
    #[test]
    fn eval_count_where_all_false_returns_zero() {
        let mut s = empty_summary();
        let mut stats = SegmentStats::unknown();
        stats.null_count = Some(0);
        s.summary.segments = vec![seg_with_stats(Some(9), stats)];
        let req = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNull {
                column: ColumnRef::new("x").expect("column"),
            },
        );
        let out = evaluate_vortex_count_where_from_summary(req, &s).expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(0));
    }
    #[test]
    fn eval_count_where_all_true_sums_rows() {
        let mut s = empty_summary();
        let mut stats_a = SegmentStats::unknown();
        stats_a.null_count = Some(0);
        let mut stats_b = SegmentStats::unknown();
        stats_b.null_count = Some(0);
        s.summary.segments = vec![
            seg_with_stats(Some(2), stats_a),
            seg_with_stats(Some(3), stats_b),
        ];
        let req = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").expect("column"),
            },
        );
        let out = evaluate_vortex_count_where_from_summary(req, &s).expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(5));
    }
    #[test]
    fn human_text_contains_flags() {
        let mut r = VortexQueryPrimitiveResult::needs_encoded_read(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
        );
        r.add_diagnostic(Diagnostic::no_fallback_execution("nope"));
        let t = r.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn side_effect_free_metadata_and_deferred() {
        let a = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(1),
        );
        let b = VortexQueryPrimitiveResult::missing_metadata(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "missing",
        );
        assert!(a.is_side_effect_free());
        assert!(b.is_side_effect_free());
    }
}
