//! Native scan request planning skeletons.
//!
//! Scan requests in this module are planning intents. They do not execute reads
//! by default and they never permit fallback engines.

use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, DatasetRef, Diagnostic, DiagnosticCode, DiagnosticSeverity, ExecutionState,
    MaterializationPolicy, PredicateExpr,
};

/// Requested projection for a dataset scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionRequest {
    All,
    Columns(Vec<ColumnRef>),
}

impl ProjectionRequest {
    #[must_use]
    pub fn all() -> Self {
        Self::All
    }

    #[must_use]
    pub fn columns(columns: Vec<ColumnRef>) -> Self {
        Self::Columns(columns)
    }

    #[must_use]
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    #[must_use]
    pub fn column_count(&self) -> Option<usize> {
        match self {
            Self::All => None,
            Self::Columns(columns) => Some(columns.len()),
        }
    }

    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::All => "all".to_string(),
            Self::Columns(columns) => format!("{} columns", columns.len()),
        }
    }
}

/// Scan behavior mode; execution is explicit and not default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanMode {
    MetadataOnly,
    PlanOnly,
    NativeExecute,
}

impl ScanMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::PlanOnly => "plan_only",
            Self::NativeExecute => "native_execute",
        }
    }
}

/// Native scan request domain object for plan/explain/estimate entry points.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanRequest {
    pub dataset: DatasetRef,
    pub projection: ProjectionRequest,
    pub predicate: Option<PredicateExpr>,
    pub mode: ScanMode,
    pub materialization_policy: MaterializationPolicy,
}

impl ScanRequest {
    #[must_use]
    pub fn new(dataset: DatasetRef) -> Self {
        Self {
            dataset,
            projection: ProjectionRequest::all(),
            predicate: None,
            mode: ScanMode::PlanOnly,
            materialization_policy: MaterializationPolicy::Late,
        }
    }
    #[must_use]
    pub fn with_projection(mut self, projection: ProjectionRequest) -> Self {
        self.projection = projection;
        self
    }
    #[must_use]
    pub fn with_predicate(mut self, predicate: PredicateExpr) -> Self {
        self.predicate = Some(predicate);
        self
    }
    #[must_use]
    pub fn with_mode(mut self, mode: ScanMode) -> Self {
        self.mode = mode;
        self
    }
    #[must_use]
    pub fn with_materialization_policy(mut self, policy: MaterializationPolicy) -> Self {
        self.materialization_policy = policy;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "dataset=[{}]; projection={}; predicate={}; mode={}; materialization_policy={}",
            self.dataset.summary(),
            self.projection.summary(),
            self.predicate
                .as_ref()
                .map_or_else(|| "none".to_string(), PredicateExpr::summary),
            self.mode.as_str(),
            self.materialization_policy.summary(),
        )
    }
    #[must_use]
    pub fn requires_execution(&self) -> bool {
        matches!(self.mode, ScanMode::NativeExecute)
    }
}

/// Planning result state for a scan request skeleton.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPlanningStatus {
    Planned,
    Unsupported,
    ExecutionNotImplemented,
}

impl ScanPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Unsupported => "unsupported",
            Self::ExecutionNotImplemented => "execution_not_implemented",
        }
    }
}

/// Scan planning skeleton with deterministic diagnostics and no fallback execution.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPlanSkeleton {
    pub request: ScanRequest,
    pub status: ScanPlanningStatus,
    pub execution_state: ExecutionState,
    pub diagnostics: Vec<Diagnostic>,
}

impl ScanPlanSkeleton {
    #[must_use]
    pub fn plan_only(request: ScanRequest) -> Self {
        Self {
            request,
            status: ScanPlanningStatus::Planned,
            execution_state: ExecutionState::MetadataOnly,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn execution_not_implemented(request: ScanRequest) -> Self {
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "native_scan_execution",
            "Native scan execution is not implemented yet.",
            Some(
                "Use scan plan, explain, or estimate modes until native execution lands."
                    .to_string(),
            ),
        );
        Self {
            request,
            status: ScanPlanningStatus::ExecutionNotImplemented,
            execution_state: ExecutionState::Unsupported,
            diagnostics: vec![diagnostic],
        }
    }

    #[must_use]
    pub fn unsupported(
        request: ScanRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Requested scan feature is not supported in the current planning skeleton.",
            Some(reason),
        );
        Self {
            request,
            status: ScanPlanningStatus::Unsupported,
            execution_state: ExecutionState::Unsupported,
            diagnostics: vec![diagnostic],
        }
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
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        out.push_str("ShardLoom scan plan skeleton\n");
        let _ = writeln!(out, "dataset: {}", self.request.dataset.summary());
        let _ = writeln!(out, "projection: {}", self.request.projection.summary());
        let _ = writeln!(out, "mode: {}", self.request.mode.as_str());
        let _ = writeln!(out, "status: {}", self.status.as_str());
        let _ = writeln!(out, "execution state: {}", self.execution_state.as_str());
        out.push_str("fallback execution: disabled\n");
        out.push_str("execution performed: false\n");
        if self.diagnostics.is_empty() {
            out.push_str("diagnostics: none\n");
        } else {
            out.push_str("diagnostics:\n");
            for diagnostic in &self.diagnostics {
                let _ = writeln!(out, "- {}", diagnostic.to_human_text());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::DatasetUri;

    fn test_dataset() -> DatasetRef {
        DatasetRef::from_uri(DatasetUri::new("file://tmp/table.vortex").expect("uri"))
            .expect("dataset")
    }

    #[test]
    fn projection_request_all_has_no_column_count() {
        assert_eq!(ProjectionRequest::all().column_count(), None);
    }
    #[test]
    fn projection_request_columns_has_correct_column_count() {
        let cols = vec![ColumnRef::new("a").expect("col")];
        assert_eq!(ProjectionRequest::columns(cols).column_count(), Some(1));
    }
    #[test]
    fn scan_request_defaults_to_plan_only() {
        assert_eq!(ScanRequest::new(test_dataset()).mode, ScanMode::PlanOnly);
    }
    #[test]
    fn scan_request_default_projection_is_all() {
        assert!(ScanRequest::new(test_dataset()).projection.is_all());
    }
    #[test]
    fn scan_request_requires_execution_is_true_only_for_native_execute() {
        assert!(!ScanRequest::new(test_dataset()).requires_execution());
        assert!(
            ScanRequest::new(test_dataset())
                .with_mode(ScanMode::NativeExecute)
                .requires_execution()
        );
    }
    #[test]
    fn scan_plan_skeleton_plan_only_is_not_an_error() {
        assert!(!ScanPlanSkeleton::plan_only(ScanRequest::new(test_dataset())).has_errors());
    }
    #[test]
    fn scan_plan_skeleton_execution_not_implemented_has_errors() {
        assert!(
            ScanPlanSkeleton::execution_not_implemented(
                ScanRequest::new(test_dataset()).with_mode(ScanMode::NativeExecute)
            )
            .has_errors()
        );
    }
    #[test]
    fn scan_plan_skeleton_unsupported_has_errors() {
        assert!(
            ScanPlanSkeleton::unsupported(ScanRequest::new(test_dataset()), "x", "y").has_errors()
        );
    }
    #[test]
    fn scan_plan_skeleton_human_text_includes_fallback_execution_disabled() {
        assert!(
            ScanPlanSkeleton::plan_only(ScanRequest::new(test_dataset()))
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
