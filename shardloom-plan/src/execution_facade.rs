//! Top-level execution plan facade.
//!
//! This module replaces the early one-variant planning skeleton with typed
//! plan variants for the Vortex-native surfaces that the rest of the workspace
//! already models. It remains a planning/admission contract: provider crates
//! own provider-specific execution and attach their reports as artifacts.

use shardloom_core::{
    ColumnRef, DatasetUri, Diagnostic, DiagnosticCode, EncodedSegment, EncodedValueBatch,
    ExecutionProviderKind, PredicateExpr, Result, ShardLoomError, UniversalInputSource,
};

use crate::plan_ir::PlanId;
use crate::scan::ProjectionRequest;

/// Current top-level local Vortex primitive operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexPrimitiveOperation {
    CountAll,
    CountWhere,
    FilterPredicate,
    ProjectColumns,
    FilterAndProject,
}

impl VortexPrimitiveOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CountAll => "count_all",
            Self::CountWhere => "count_where",
            Self::FilterPredicate => "filter_predicate",
            Self::ProjectColumns => "project_columns",
            Self::FilterAndProject => "filter_and_project",
        }
    }
}

/// Current top-level prepared/source-backed encoded operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedExecutionOperation {
    Filter,
    Projection,
    FilterAndProject,
}

impl EncodedExecutionOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Projection => "projection",
            Self::FilterAndProject => "filter_and_project",
        }
    }
}

/// Prepared encoded batch payload made available before top-level dispatch.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedEncodedBatch {
    pub segment: EncodedSegment,
    pub values: EncodedValueBatch,
}

impl PreparedEncodedBatch {
    #[must_use]
    pub const fn new(segment: EncodedSegment, values: EncodedValueBatch) -> Self {
        Self { segment, values }
    }

    #[must_use]
    pub fn row_count(&self) -> Option<u64> {
        self.values.row_count()
    }
}

/// Source-bound prepared batch payload with an explicit split reference.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceBackedPreparedEncodedBatch {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub batch: PreparedEncodedBatch,
}

impl SourceBackedPreparedEncodedBatch {
    /// # Errors
    /// Returns an error when `split_ref` is empty or whitespace only.
    pub fn new(
        source_uri: DatasetUri,
        split_ref: impl Into<String>,
        batch: PreparedEncodedBatch,
    ) -> Result<Self> {
        let split_ref = split_ref.into();
        if split_ref.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "split ref must not be empty".to_string(),
            ));
        }
        Ok(Self {
            source_uri,
            split_ref,
            batch,
        })
    }
}

/// Reader/split evidence required before reader-backed encoded plans can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderBackedSplitRef {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub provider_boundary_ref: String,
    pub provider_kind: ExecutionProviderKind,
    pub provider_api_surface: String,
    pub row_count: usize,
    pub dtype_summary: String,
    pub encoding_id: String,
    pub child_count: usize,
    pub buffer_count: usize,
    pub residual_boundary_ref: Option<String>,
}

impl ReaderBackedSplitRef {
    /// # Errors
    /// Returns an error when `split_ref`, `provider_boundary_ref`, or `provider_api_surface` is
    /// empty.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_uri: DatasetUri,
        split_ref: impl Into<String>,
        provider_boundary_ref: impl Into<String>,
        provider_kind: ExecutionProviderKind,
        provider_api_surface: impl Into<String>,
        row_count: usize,
        dtype_summary: impl Into<String>,
        encoding_id: impl Into<String>,
        child_count: usize,
        buffer_count: usize,
    ) -> Result<Self> {
        let split_ref = split_ref.into();
        let provider_boundary_ref = provider_boundary_ref.into();
        let provider_api_surface = provider_api_surface.into();
        if split_ref.trim().is_empty()
            || provider_boundary_ref.trim().is_empty()
            || provider_api_surface.trim().is_empty()
        {
            return Err(ShardLoomError::InvalidOperation(
                "reader-backed split refs and provider API surface must not be empty".to_string(),
            ));
        }
        Ok(Self {
            source_uri,
            split_ref,
            provider_boundary_ref,
            provider_kind,
            provider_api_surface,
            row_count,
            dtype_summary: dtype_summary.into(),
            encoding_id: encoding_id.into(),
            child_count,
            buffer_count,
            residual_boundary_ref: None,
        })
    }

    #[must_use]
    pub fn with_residual_boundary_ref(mut self, residual_boundary_ref: impl Into<String>) -> Self {
        self.residual_boundary_ref = Some(residual_boundary_ref.into());
        self
    }
}

/// Top-level local Vortex primitive plan.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPrimitivePlan {
    pub operation: VortexPrimitiveOperation,
    pub source_uri: DatasetUri,
    pub projection: ProjectionRequest,
    pub predicate: Option<PredicateExpr>,
    pub provider_kind: ExecutionProviderKind,
    pub provider_api_surface: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexPrimitivePlan {
    #[must_use]
    pub fn new(
        operation: VortexPrimitiveOperation,
        source_uri: DatasetUri,
        projection: ProjectionRequest,
        predicate: Option<PredicateExpr>,
    ) -> Self {
        Self {
            operation,
            source_uri,
            projection,
            predicate,
            provider_kind: ExecutionProviderKind::VortexScan,
            provider_api_surface: "vortex_local_primitive".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn count_all(source_uri: DatasetUri) -> Self {
        Self::new(
            VortexPrimitiveOperation::CountAll,
            source_uri,
            ProjectionRequest::all(),
            None,
        )
    }

    #[must_use]
    pub fn count_where(source_uri: DatasetUri, predicate: PredicateExpr) -> Self {
        Self::new(
            VortexPrimitiveOperation::CountWhere,
            source_uri,
            ProjectionRequest::all(),
            Some(predicate),
        )
    }

    #[must_use]
    pub fn filter(source_uri: DatasetUri, predicate: PredicateExpr) -> Self {
        Self::new(
            VortexPrimitiveOperation::FilterPredicate,
            source_uri,
            ProjectionRequest::all(),
            Some(predicate),
        )
    }

    #[must_use]
    pub fn project(source_uri: DatasetUri, projection: ProjectionRequest) -> Self {
        Self::new(
            VortexPrimitiveOperation::ProjectColumns,
            source_uri,
            projection,
            None,
        )
    }

    #[must_use]
    pub fn filter_and_project(
        source_uri: DatasetUri,
        predicate: PredicateExpr,
        projection: ProjectionRequest,
    ) -> Self {
        Self::new(
            VortexPrimitiveOperation::FilterAndProject,
            source_uri,
            projection,
            Some(predicate),
        )
    }

    #[must_use]
    pub fn source_refs(&self) -> Vec<String> {
        vec![self.source_uri.as_str().to_string()]
    }
}

/// Top-level prepared encoded plan.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedEncodedPlan {
    pub operation: EncodedExecutionOperation,
    pub predicate: Option<PredicateExpr>,
    pub requested_columns: Vec<ColumnRef>,
    pub filter_batches: Vec<PreparedEncodedBatch>,
    pub projection_batches: Vec<PreparedEncodedBatch>,
    pub provider_kind: ExecutionProviderKind,
    pub provider_api_surface: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl PreparedEncodedPlan {
    #[must_use]
    pub fn filter(predicate: PredicateExpr, filter_batches: Vec<PreparedEncodedBatch>) -> Self {
        Self {
            operation: EncodedExecutionOperation::Filter,
            predicate: Some(predicate),
            requested_columns: vec![],
            filter_batches,
            projection_batches: vec![],
            provider_kind: ExecutionProviderKind::VortexArrayKernel,
            provider_api_surface: "vortex_prepared_encoded_filter".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn projection(
        requested_columns: Vec<ColumnRef>,
        projection_batches: Vec<PreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::Projection,
            predicate: None,
            requested_columns,
            filter_batches: vec![],
            projection_batches,
            provider_kind: ExecutionProviderKind::VortexArrayKernel,
            provider_api_surface: "vortex_prepared_encoded_projection".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn filter_and_project(
        predicate: PredicateExpr,
        requested_columns: Vec<ColumnRef>,
        filter_batches: Vec<PreparedEncodedBatch>,
        projection_batches: Vec<PreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::FilterAndProject,
            predicate: Some(predicate),
            requested_columns,
            filter_batches,
            projection_batches,
            provider_kind: ExecutionProviderKind::VortexArrayKernel,
            provider_api_surface: "vortex_prepared_encoded_filter_project".to_string(),
            diagnostics: vec![],
        }
    }
}

/// Top-level source-backed encoded plan.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceBackedEncodedPlan {
    pub operation: EncodedExecutionOperation,
    pub source: UniversalInputSource,
    pub predicate: Option<PredicateExpr>,
    pub requested_columns: Vec<ColumnRef>,
    pub filter_batches: Vec<SourceBackedPreparedEncodedBatch>,
    pub projection_batches: Vec<SourceBackedPreparedEncodedBatch>,
    pub provider_kind: ExecutionProviderKind,
    pub provider_api_surface: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl SourceBackedEncodedPlan {
    #[must_use]
    pub fn filter(
        source: UniversalInputSource,
        predicate: PredicateExpr,
        filter_batches: Vec<SourceBackedPreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::Filter,
            source,
            predicate: Some(predicate),
            requested_columns: vec![],
            filter_batches,
            projection_batches: vec![],
            provider_kind: ExecutionProviderKind::VortexSource,
            provider_api_surface: "vortex_source_backed_encoded_filter".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn projection(
        source: UniversalInputSource,
        requested_columns: Vec<ColumnRef>,
        projection_batches: Vec<SourceBackedPreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::Projection,
            source,
            predicate: None,
            requested_columns,
            filter_batches: vec![],
            projection_batches,
            provider_kind: ExecutionProviderKind::VortexSource,
            provider_api_surface: "vortex_source_backed_encoded_projection".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn filter_and_project(
        source: UniversalInputSource,
        predicate: PredicateExpr,
        requested_columns: Vec<ColumnRef>,
        filter_batches: Vec<SourceBackedPreparedEncodedBatch>,
        projection_batches: Vec<SourceBackedPreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::FilterAndProject,
            source,
            predicate: Some(predicate),
            requested_columns,
            filter_batches,
            projection_batches,
            provider_kind: ExecutionProviderKind::VortexSource,
            provider_api_surface: "vortex_source_backed_encoded_filter_project".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn source_refs(&self) -> Vec<String> {
        self.source
            .uri
            .as_ref()
            .map_or_else(Vec::new, |uri| vec![uri.as_str().to_string()])
    }

    #[must_use]
    pub fn split_refs(&self) -> Vec<String> {
        self.filter_batches
            .iter()
            .map(|batch| batch.split_ref.clone())
            .chain(
                self.projection_batches
                    .iter()
                    .map(|batch| batch.split_ref.clone()),
            )
            .collect()
    }
}

/// Top-level reader-backed encoded plan.
#[derive(Debug, Clone, PartialEq)]
pub struct ReaderBackedEncodedPlan {
    pub operation: EncodedExecutionOperation,
    pub source: UniversalInputSource,
    pub reader_splits: Vec<ReaderBackedSplitRef>,
    pub predicate: Option<PredicateExpr>,
    pub requested_columns: Vec<ColumnRef>,
    pub filter_batches: Vec<SourceBackedPreparedEncodedBatch>,
    pub projection_batches: Vec<SourceBackedPreparedEncodedBatch>,
    pub provider_kind: ExecutionProviderKind,
    pub provider_api_surface: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl ReaderBackedEncodedPlan {
    #[must_use]
    pub fn filter(
        source: UniversalInputSource,
        reader_splits: Vec<ReaderBackedSplitRef>,
        predicate: PredicateExpr,
        filter_batches: Vec<SourceBackedPreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::Filter,
            source,
            reader_splits,
            predicate: Some(predicate),
            requested_columns: vec![],
            filter_batches,
            projection_batches: vec![],
            provider_kind: ExecutionProviderKind::VortexSource,
            provider_api_surface: "vortex_reader_backed_encoded_filter".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn projection(
        source: UniversalInputSource,
        reader_splits: Vec<ReaderBackedSplitRef>,
        requested_columns: Vec<ColumnRef>,
        projection_batches: Vec<SourceBackedPreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::Projection,
            source,
            reader_splits,
            predicate: None,
            requested_columns,
            filter_batches: vec![],
            projection_batches,
            provider_kind: ExecutionProviderKind::VortexSource,
            provider_api_surface: "vortex_reader_backed_encoded_projection".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn filter_and_project(
        source: UniversalInputSource,
        reader_splits: Vec<ReaderBackedSplitRef>,
        predicate: PredicateExpr,
        requested_columns: Vec<ColumnRef>,
        filter_batches: Vec<SourceBackedPreparedEncodedBatch>,
        projection_batches: Vec<SourceBackedPreparedEncodedBatch>,
    ) -> Self {
        Self {
            operation: EncodedExecutionOperation::FilterAndProject,
            source,
            reader_splits,
            predicate: Some(predicate),
            requested_columns,
            filter_batches,
            projection_batches,
            provider_kind: ExecutionProviderKind::VortexSource,
            provider_api_surface: "vortex_reader_backed_encoded_filter_project".to_string(),
            diagnostics: vec![],
        }
    }

    #[must_use]
    pub fn source_refs(&self) -> Vec<String> {
        self.source
            .uri
            .as_ref()
            .map_or_else(Vec::new, |uri| vec![uri.as_str().to_string()])
    }

    #[must_use]
    pub fn split_refs(&self) -> Vec<String> {
        self.reader_splits
            .iter()
            .map(|split| split.split_ref.clone())
            .collect()
    }

    #[must_use]
    pub fn residual_boundary_refs(&self) -> Vec<String> {
        self.reader_splits
            .iter()
            .filter_map(|split| split.residual_boundary_ref.clone())
            .collect()
    }
}

/// Explicit report-only top-level plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportOnlyPlan {
    pub report_kind: String,
    pub artifact_refs: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
}

impl ReportOnlyPlan {
    #[must_use]
    pub fn new(report_kind: impl Into<String>) -> Self {
        Self {
            report_kind: report_kind.into(),
            artifact_refs: vec![],
            diagnostics: vec![],
        }
    }
}

/// Top-level plan variants.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanKind {
    VortexPrimitive(VortexPrimitivePlan),
    PreparedEncoded(PreparedEncodedPlan),
    SourceBackedEncoded(SourceBackedEncodedPlan),
    ReaderBackedEncoded(ReaderBackedEncodedPlan),
    ReportOnly(ReportOnlyPlan),
}

impl PlanKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::VortexPrimitive(_) => "vortex_primitive",
            Self::PreparedEncoded(_) => "prepared_encoded",
            Self::SourceBackedEncoded(_) => "source_backed_encoded",
            Self::ReaderBackedEncoded(_) => "reader_backed_encoded",
            Self::ReportOnly(_) => "report_only",
        }
    }
}

/// Top-level execution plan description.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub id: PlanId,
    pub kind: PlanKind,
    pub diagnostics: Vec<Diagnostic>,
    pub fallback_attempted: bool,
}

impl Plan {
    #[must_use]
    pub fn new(id: PlanId, kind: PlanKind) -> Self {
        Self {
            id,
            kind,
            diagnostics: vec![],
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn vortex_primitive(id: PlanId, primitive: VortexPrimitivePlan) -> Self {
        Self::new(id, PlanKind::VortexPrimitive(primitive))
    }

    #[must_use]
    pub fn prepared_encoded(id: PlanId, prepared: PreparedEncodedPlan) -> Self {
        Self::new(id, PlanKind::PreparedEncoded(prepared))
    }

    #[must_use]
    pub fn source_backed_encoded(id: PlanId, source_backed: SourceBackedEncodedPlan) -> Self {
        Self::new(id, PlanKind::SourceBackedEncoded(source_backed))
    }

    #[must_use]
    pub fn reader_backed_encoded(id: PlanId, reader_backed: ReaderBackedEncodedPlan) -> Self {
        Self::new(id, PlanKind::ReaderBackedEncoded(reader_backed))
    }

    #[must_use]
    pub fn report_only(id: PlanId, report: ReportOnlyPlan) -> Self {
        Self::new(id, PlanKind::ReportOnly(report))
    }

    #[must_use]
    pub fn provider_kind(&self) -> Option<ExecutionProviderKind> {
        match &self.kind {
            PlanKind::VortexPrimitive(plan) => Some(plan.provider_kind),
            PlanKind::PreparedEncoded(plan) => Some(plan.provider_kind),
            PlanKind::SourceBackedEncoded(plan) => Some(plan.provider_kind),
            PlanKind::ReaderBackedEncoded(plan) => Some(plan.provider_kind),
            PlanKind::ReportOnly(_) => None,
        }
    }

    #[must_use]
    pub fn provider_api_surface(&self) -> Option<&str> {
        match &self.kind {
            PlanKind::VortexPrimitive(plan) => Some(plan.provider_api_surface.as_str()),
            PlanKind::PreparedEncoded(plan) => Some(plan.provider_api_surface.as_str()),
            PlanKind::SourceBackedEncoded(plan) => Some(plan.provider_api_surface.as_str()),
            PlanKind::ReaderBackedEncoded(plan) => Some(plan.provider_api_surface.as_str()),
            PlanKind::ReportOnly(_) => None,
        }
    }

    #[must_use]
    pub fn source_refs(&self) -> Vec<String> {
        match &self.kind {
            PlanKind::VortexPrimitive(plan) => plan.source_refs(),
            PlanKind::PreparedEncoded(_) | PlanKind::ReportOnly(_) => vec![],
            PlanKind::SourceBackedEncoded(plan) => plan.source_refs(),
            PlanKind::ReaderBackedEncoded(plan) => plan.source_refs(),
        }
    }

    #[must_use]
    pub fn split_refs(&self) -> Vec<String> {
        match &self.kind {
            PlanKind::VortexPrimitive(_)
            | PlanKind::PreparedEncoded(_)
            | PlanKind::ReportOnly(_) => {
                vec![]
            }
            PlanKind::SourceBackedEncoded(plan) => plan.split_refs(),
            PlanKind::ReaderBackedEncoded(plan) => plan.split_refs(),
        }
    }

    #[must_use]
    pub fn residual_boundary_refs(&self) -> Vec<String> {
        match &self.kind {
            PlanKind::ReaderBackedEncoded(plan) => plan.residual_boundary_refs(),
            PlanKind::VortexPrimitive(_)
            | PlanKind::PreparedEncoded(_)
            | PlanKind::SourceBackedEncoded(_)
            | PlanKind::ReportOnly(_) => vec![],
        }
    }

    #[must_use]
    pub fn provider_dispatch_required(&self) -> bool {
        !matches!(self.kind, PlanKind::ReportOnly(_))
    }

    #[must_use]
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        let mut diagnostics = self.diagnostics.clone();
        match &self.kind {
            PlanKind::VortexPrimitive(plan) => diagnostics.extend(plan.diagnostics.clone()),
            PlanKind::PreparedEncoded(plan) => diagnostics.extend(plan.diagnostics.clone()),
            PlanKind::SourceBackedEncoded(plan) => diagnostics.extend(plan.diagnostics.clone()),
            PlanKind::ReaderBackedEncoded(plan) => diagnostics.extend(plan.diagnostics.clone()),
            PlanKind::ReportOnly(plan) => diagnostics.extend(plan.diagnostics.clone()),
        }
        diagnostics
    }

    #[must_use]
    pub fn unsupported_diagnostic(&self) -> Diagnostic {
        Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            self.kind.as_str(),
            "Top-level execution requires an admitted ShardLoom/Vortex provider dispatch surface.",
            Some(
                "Use a provider-side dispatcher such as the Vortex top-level facade or keep the plan in report-only mode."
                    .to_string(),
            ),
        )
    }
}

/// Build a local Vortex primitive count-all plan.
///
/// # Errors
/// Returns an error when `plan_id` or `source_uri` is invalid.
pub fn build_vortex_count_all_plan(
    plan_id: impl Into<String>,
    source_uri: impl Into<String>,
) -> Result<Plan> {
    Ok(Plan::vortex_primitive(
        PlanId::new(plan_id)?,
        VortexPrimitivePlan::count_all(DatasetUri::new(source_uri)?),
    ))
}
